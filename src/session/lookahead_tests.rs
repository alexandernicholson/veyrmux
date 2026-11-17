use super::*;
use std::{future::poll_fn, time::Duration};
use tokio::time::timeout;

/// A finite wire with deterministic transport chunk boundaries and an explicit
/// gate. Counts only reads that actually consume bytes, not EOF or Pending.
struct CountingIo {
    wire: Vec<u8>,
    consumed: usize,
    boundaries: VecDeque<usize>,
    gate: Option<usize>,
    positive_reads: usize,
    pending_reads: usize,
    zero_capacity_reads: usize,
}

impl CountingIo {
    fn new(wire: Vec<u8>, boundaries: impl IntoIterator<Item = usize>) -> Self {
        Self {
            wire,
            consumed: 0,
            boundaries: boundaries.into_iter().collect(),
            gate: None,
            positive_reads: 0,
            pending_reads: 0,
            zero_capacity_reads: 0,
        }
    }
}

impl AsyncRead for CountingIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buffer.remaining() == 0 {
            this.zero_capacity_reads += 1;
            return Poll::Ready(Ok(()));
        }
        if this.gate == Some(this.consumed) {
            this.pending_reads += 1;
            return Poll::Pending;
        }
        while this
            .boundaries
            .front()
            .is_some_and(|end| *end <= this.consumed)
        {
            this.boundaries.pop_front();
        }
        let end = this.boundaries.front().copied().unwrap_or(this.wire.len());
        let end = end.min(this.gate.unwrap_or(this.wire.len()));
        let count = buffer.remaining().min(end - this.consumed);
        buffer.put_slice(&this.wire[this.consumed..this.consumed + count]);
        this.consumed += count;
        this.positive_reads += usize::from(count != 0);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for CountingIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn data(flags: u16, value: u32) -> Header {
    Header {
        kind: FrameType::Data,
        flags,
        stream_id: 1,
        value,
    }
}

fn wire_frame(wire: &mut Vec<u8>, header: Header, payload: &[u8]) {
    wire.extend_from_slice(&header.encode());
    wire.extend_from_slice(payload);
}

fn connection(
    wire: Vec<u8>,
    boundaries: impl IntoIterator<Item = usize>,
) -> Connection<CountingIo> {
    Connection::new(
        CountingIo::new(wire, boundaries),
        Config::default(),
        Mode::Server,
    )
}

async fn next_frame(
    connection: &mut Connection<CountingIo>,
) -> Result<Option<(Header, BytesMut)>, ConnectionError> {
    poll_fn(|cx| match connection.read_frame(cx) {
        Ok(poll) => poll.map(Ok),
        Err(error) => Poll::Ready(Err(error)),
    })
    .await
}

#[tokio::test]
async fn adjacent_data_frames_decode_fixed_wire_in_n_plus_one_positive_reads() {
    timeout(Duration::from_secs(3), async {
        // Literal Yamux bytes independently pin version, flags, lengths and order.
        let wire = b"\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00\x03abc\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x02de\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x01f";
        let expected = [(data(SYN, 3), &b"abc"[..]), (data(0, 2), &b"de"[..]), (data(FIN, 1), &b"f"[..])];
        let mut connection = connection(wire.to_vec(), []);
        let mut decoded_wire = Vec::new();
        for (header, payload) in expected {
            let (actual, body) = next_frame(&mut connection).await.unwrap().unwrap();
            assert_eq!(actual, header);
            assert_eq!(body.as_ref(), payload);
            wire_frame(&mut decoded_wire, actual, &body);
            assert_eq!(connection.body.capacity(), 0);
            connection.handle_frame(actual, body).unwrap();
        }
        assert_eq!(decoded_wire, wire);
        assert!(connection.io.positive_reads <= expected.len() + 1);
        assert_eq!(connection.io.positive_reads, 4);
        assert_eq!(connection.io.consumed, wire.len());
        assert!(next_frame(&mut connection).await.unwrap().is_none());
        assert_eq!(connection.io.zero_capacity_reads, 0);
    }).await.unwrap();
}

#[tokio::test]
async fn every_transport_rechunk_boundary_preserves_data_control_and_empty_frames() {
    timeout(Duration::from_secs(3), async {
        let expected = [
            (data(SYN, 3), &b"abc"[..]),
            (
                Header {
                    kind: FrameType::Ping,
                    flags: SYN,
                    stream_id: 0,
                    value: 17,
                },
                &b""[..],
            ),
            (data(0, 0), &b""[..]),
            (
                Header {
                    kind: FrameType::WindowUpdate,
                    flags: 0,
                    stream_id: 1,
                    value: 7,
                },
                &b""[..],
            ),
            (data(FIN, 5), &b"hello"[..]),
            (
                Header {
                    kind: FrameType::GoAway,
                    flags: 0,
                    stream_id: 0,
                    value: 0,
                },
                &b""[..],
            ),
        ];
        let mut wire = Vec::new();
        for (header, payload) in expected {
            wire_frame(&mut wire, header, payload);
        }
        // Every two-chunk split, plus every fixed chunk width including one byte.
        let splits = (1..wire.len()).map(|split| vec![split]);
        let widths = (1..=wire.len()).map(|width| (width..wire.len()).step_by(width).collect());
        for boundaries in splits.chain(widths) {
            let mut connection = connection(wire.clone(), boundaries);
            let mut decoded_wire = Vec::new();
            for (header, payload) in expected {
                let (actual, body) = next_frame(&mut connection).await.unwrap().unwrap();
                assert_eq!(actual, header);
                assert_eq!(body.as_ref(), payload);
                wire_frame(&mut decoded_wire, actual, &body);
                assert_eq!(connection.body.capacity(), 0);
                connection.handle_frame(actual, body).unwrap();
            }
            assert_eq!(decoded_wire, wire);
            assert!(next_frame(&mut connection).await.unwrap().is_none());
            assert_eq!(connection.io.consumed, wire.len());
            assert_eq!(connection.io.zero_capacity_reads, 0);
            let shared = lock(&connection.shared);
            let stream = shared.streams.get(&1).unwrap();
            assert_eq!(
                stream
                    .received
                    .iter()
                    .flat_map(|body| body.iter().copied())
                    .collect::<Vec<_>>(),
                b"abchello"
            );
            assert_eq!(stream.receive_credit, WINDOW - 8);
            assert_eq!(stream.send_credit, WINDOW + 7);
            assert!(stream.remote_fin);
            assert!(shared.remote_goaway);
            drop(shared);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn prefetched_eof_distinguishes_clean_boundary_partial_header_and_missing_payload() {
    timeout(Duration::from_secs(3), async {
        for prefetched in 0..=HEADER_LEN {
            let mut wire = Vec::new();
            wire_frame(&mut wire, data(SYN, 3), b"abc");
            wire.extend_from_slice(&data(0, 4).encode()[..prefetched]);
            let mut connection = connection(wire.clone(), []);
            let (header, body) = next_frame(&mut connection).await.unwrap().unwrap();
            assert_eq!(body.as_ref(), b"abc");
            connection.handle_frame(header, body).unwrap();
            assert_eq!(connection.header_bytes, prefetched);
            assert_eq!(connection.io.consumed, wire.len());
            let result = next_frame(&mut connection).await;
            match prefetched {
                0 => assert!(result.unwrap().is_none()),
                HEADER_LEN => assert!(matches!(
                    result,
                    Err(ConnectionError::Protocol("EOF inside frame payload"))
                )),
                _ => assert!(matches!(
                    result,
                    Err(ConnectionError::Protocol("EOF inside frame header"))
                )),
            }
            assert_eq!(connection.io.zero_capacity_reads, 0);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn complete_prefetched_empty_or_control_header_decodes_before_eof_without_body_allocation() {
    timeout(Duration::from_secs(3), async {
        for second in [
            data(0, 0),
            data(FIN, 0),
            data(RST, 0),
            Header {
                kind: FrameType::Ping,
                flags: ACK,
                stream_id: 0,
                value: 3,
            },
            Header {
                kind: FrameType::WindowUpdate,
                flags: 0,
                stream_id: 1,
                value: 3,
            },
            Header {
                kind: FrameType::GoAway,
                flags: 0,
                stream_id: 0,
                value: 0,
            },
        ] {
            let mut wire = Vec::new();
            wire_frame(&mut wire, data(SYN, 1), b"x");
            wire_frame(&mut wire, second, b"");
            let mut connection = connection(wire.clone(), []);
            let (header, body) = next_frame(&mut connection).await.unwrap().unwrap();
            connection.handle_frame(header, body).unwrap();
            assert_eq!(connection.header_bytes, HEADER_LEN);
            let reads = connection.io.positive_reads;
            let (actual, body) = next_frame(&mut connection).await.unwrap().unwrap();
            assert_eq!(actual, second);
            assert_eq!(body.capacity(), 0);
            assert_eq!(connection.body.capacity(), 0);
            assert_eq!(connection.io.positive_reads, reads);
            connection.handle_frame(actual, body).unwrap();
            assert!(next_frame(&mut connection).await.unwrap().is_none());
            assert_eq!(connection.io.zero_capacity_reads, 0);
        }
        // Standalone empty DATA must not consume even a following header.
        let mut wire = Vec::new();
        wire_frame(&mut wire, data(SYN, 0), b"");
        wire_frame(&mut wire, data(0, 1), b"x");
        let mut connection = connection(wire, []);
        let (_, body) = next_frame(&mut connection).await.unwrap().unwrap();
        assert_eq!(body.capacity(), 0);
        assert_eq!(connection.io.consumed, HEADER_LEN);
        assert_eq!(connection.header_bytes, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn payload_pending_keeps_single_ownership_and_resumes_with_only_header_slack() {
    timeout(Duration::from_secs(3), async {
        let payload = b"partial-frame-data";
        let mut wire = Vec::new();
        wire_frame(&mut wire, data(SYN, payload.len() as u32), payload);
        wire_frame(&mut wire, data(0, 4), b"next");
        let mut connection = connection(wire, []);
        connection.io.gate = Some(HEADER_LEN + 3);
        poll_fn(|cx| {
            assert!(matches!(connection.read_frame(cx), Ok(Poll::Pending)));
            Poll::Ready(())
        })
        .await;
        assert_eq!(connection.io.pending_reads, 1);
        assert_eq!(connection.body.as_ref(), &payload[..3]);
        assert!(connection.incoming.is_some());
        connection.io.gate = None;
        let (header, body) = next_frame(&mut connection).await.unwrap().unwrap();
        assert_eq!(body.as_ref(), payload);
        assert_eq!(connection.body.capacity(), 0);
        assert_eq!(connection.header_bytes, HEADER_LEN);
        assert_eq!(
            connection.io.consumed,
            HEADER_LEN + payload.len() + HEADER_LEN
        );
        connection.handle_frame(header, body).unwrap();
        let (_, body) = next_frame(&mut connection).await.unwrap().unwrap();
        assert_eq!(body.as_ref(), b"next");
        assert_eq!(connection.io.zero_capacity_reads, 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn complete_payload_is_delivered_without_waiting_for_lookahead() {
    timeout(Duration::from_secs(3), async {
        let mut wire = Vec::new();
        wire_frame(&mut wire, data(SYN, 4), b"done");
        let mut connection = connection(wire.clone(), []);
        connection.io.gate = Some(wire.len());
        let (_, body) = next_frame(&mut connection).await.unwrap().unwrap();
        assert_eq!(body.as_ref(), b"done");
        assert_eq!(connection.header_bytes, 0);
        assert_eq!(connection.io.pending_reads, 0);
        assert_eq!(connection.io.positive_reads, 2);
        assert_eq!(connection.io.consumed, wire.len());
        assert_eq!(connection.body.capacity(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_prefetched_header_is_rejected_before_any_second_body_byte_is_consumed() {
    timeout(Duration::from_secs(3), async {
        let ordinary = data(0, 4).encode();
        let mut version = ordinary;
        version[0] = 1;
        let mut kind = ordinary;
        kind[1] = 99;
        let mut flags = ordinary;
        flags[3] = 16;
        let mut scope = ordinary;
        scope[7] = 0;
        let mut parity = data(SYN, 4);
        parity.stream_id = 2;
        let invalid = [
            ("version", version, false),
            ("kind", kind, false),
            ("flags", flags, false),
            ("scope", scope, false),
            ("SYN parity", parity.encode(), false),
            ("SYN ACK", data(SYN | ACK, 4).encode(), false),
            ("SYN RST", data(SYN | RST, 4).encode(), false),
            ("duplicate SYN", data(SYN, 4).encode(), false),
            ("reset body", data(RST, 4).encode(), false),
            ("reset flags", data(RST | FIN, 0).encode(), false),
            ("frame limit", data(0, MAX_FRAME as u32 + 1).encode(), false),
            ("window limit", data(0, WINDOW + 1).encode(), false),
            ("receive credit", data(0, WINDOW - 2).encode(), false),
            ("after FIN", data(0, 4).encode(), true),
            ("duplicate FIN", data(FIN, 0).encode(), true),
        ];
        for (name, bad_header, fin) in invalid {
            let mut wire = Vec::new();
            wire_frame(&mut wire, data(SYN | if fin { FIN } else { 0 }, 3), b"abc");
            wire.extend_from_slice(&bad_header);
            wire.extend_from_slice(b"bad-body-must-not-be-read");
            let mut connection = connection(wire, []);
            let (header, body) = next_frame(&mut connection).await.unwrap().unwrap();
            assert_eq!(body.as_ref(), b"abc", "{name}");
            connection.handle_frame(header, body).unwrap();
            let boundary = HEADER_LEN + 3 + HEADER_LEN;
            assert_eq!(connection.io.consumed, boundary, "{name}");
            assert_eq!(connection.header_bytes, HEADER_LEN, "{name}");
            let result = next_frame(&mut connection).await;
            assert!(
                matches!(
                    result,
                    Err(ConnectionError::Protocol(_)) | Err(ConnectionError::Io(_))
                ),
                "{name}: {result:?}"
            );
            assert_eq!(connection.io.consumed, boundary, "{name}");
            assert_eq!(connection.io.positive_reads, 2, "{name}");
            assert_eq!(connection.body.capacity(), 0, "{name}");
            assert_eq!(connection.io.zero_capacity_reads, 0, "{name}");
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn every_partial_prefetched_header_resumes_after_pending() {
    timeout(Duration::from_secs(3), async {
        for prefetched in 1..HEADER_LEN {
            let mut wire = Vec::new();
            wire_frame(&mut wire, data(SYN, 3), b"abc");
            wire_frame(&mut wire, data(FIN, 4), b"next");
            let mut connection = connection(wire, []);
            connection.io.gate = Some(HEADER_LEN + 3 + prefetched);
            let (header, body) = next_frame(&mut connection).await.unwrap().unwrap();
            assert_eq!(body.as_ref(), b"abc");
            connection.handle_frame(header, body).unwrap();
            assert_eq!(connection.header_bytes, prefetched);
            poll_fn(|cx| {
                assert!(matches!(connection.read_frame(cx), Ok(Poll::Pending)));
                Poll::Ready(())
            })
            .await;
            assert_eq!(connection.io.pending_reads, 1);
            assert_eq!(connection.header_bytes, prefetched);
            assert_eq!(connection.body.capacity(), 0);
            connection.io.gate = None;
            let (header, body) = next_frame(&mut connection).await.unwrap().unwrap();
            assert_eq!(header, data(FIN, 4));
            assert_eq!(body.as_ref(), b"next");
            connection.handle_frame(header, body).unwrap();
            assert!(next_frame(&mut connection).await.unwrap().is_none());
            assert_eq!(connection.io.zero_capacity_reads, 0);
        }
    })
    .await
    .unwrap();
}
