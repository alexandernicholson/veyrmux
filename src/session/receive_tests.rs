use super::*;
use std::{future::poll_fn, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

#[tokio::test]
async fn completed_frame_does_not_leave_a_reclaimable_shared_tail() {
    timeout(Duration::from_secs(3), async {
        let (transport, mut peer) = tokio::io::duplex(64 * 1024);
        let mut connection = Connection::new(transport, Config::default(), Mode::Server);
        peer.write_all(
            &Header {
                kind: FrameType::Data,
                flags: SYN,
                stream_id: 1,
                value: 16384,
            }
            .encode(),
        )
        .await
        .unwrap();
        peer.write_all(&[0xa5; 16384]).await.unwrap();
        let (_, first) = poll_fn(|cx| match connection.read_frame(cx) {
            Ok(Poll::Ready(Some(frame))) => Poll::Ready(Ok::<_, ConnectionError>(frame)),
            Ok(Poll::Pending) => Poll::Pending,
            other => panic!("unexpected frame result: {other:?}"),
        })
        .await
        .unwrap();
        assert_eq!(first.len(), 16384);
        drop(first);
        peer.write_all(
            &Header {
                kind: FrameType::Data,
                flags: SYN,
                stream_id: 3,
                value: 1,
            }
            .encode(),
        )
        .await
        .unwrap();
        peer.write_all(b"x").await.unwrap();
        let (_, tiny) = poll_fn(|cx| match connection.read_frame(cx) {
            Ok(Poll::Ready(Some(frame))) => Poll::Ready(Ok::<_, ConnectionError>(frame)),
            Ok(Poll::Pending) => Poll::Pending,
            other => panic!("unexpected frame result: {other:?}"),
        })
        .await
        .unwrap();
        assert_eq!(tiny.as_ref(), b"x");
        assert_eq!(
            connection.body.capacity(),
            0,
            "tiny frame retained a larger prior backing allocation"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn receive_payload_ownership_survives_pending_without_truncation() {
    timeout(Duration::from_secs(3), async {
        let (transport, mut peer) = tokio::io::duplex(1024);
        let mut connection = Connection::new(transport, Config::default(), Mode::Server);
        let payload = b"partial-frame-data";
        peer.write_all(
            &Header {
                kind: FrameType::Data,
                flags: SYN,
                stream_id: 1,
                value: payload.len() as u32,
            }
            .encode(),
        )
        .await
        .unwrap();
        peer.write_all(&payload[..3]).await.unwrap();
        poll_fn(|cx| {
            assert!(matches!(connection.read_frame(cx), Ok(Poll::Pending)));
            Poll::Ready(())
        })
        .await;
        assert_eq!(connection.body.len(), 3);
        peer.write_all(&payload[3..]).await.unwrap();
        let (_, received) = poll_fn(|cx| match connection.read_frame(cx) {
            Ok(Poll::Ready(Some(frame))) => Poll::Ready(Ok::<_, ConnectionError>(frame)),
            Ok(Poll::Pending) => Poll::Pending,
            other => panic!("unexpected frame result: {other:?}"),
        })
        .await
        .unwrap();
        assert_eq!(received.as_ref(), payload);
        assert_eq!(connection.body.capacity(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tiny_frames_use_bounded_chunk_metadata_and_preserve_the_full_window() {
    timeout(Duration::from_secs(5), async {
        let (transport, _peer) = tokio::io::duplex(64);
        let mut connection = Connection::new(transport, Config::default(), Mode::Server);
        let expected: Vec<u8> = (0..WINDOW as usize).map(|n| (n % 251) as u8).collect();
        for (n, value) in expected.iter().enumerate() {
            connection
                .handle_frame(
                    Header {
                        kind: FrameType::Data,
                        flags: if n == 0 { SYN } else { 0 },
                        stream_id: 1,
                        value: 1,
                    },
                    BytesMut::from(std::slice::from_ref(value)),
                )
                .unwrap();
        }
        {
            let shared = lock(&connection.shared);
            let state = shared.streams.get(&1).unwrap();
            assert_eq!(state.receive_credit, 0);
            assert_eq!(state.received_bytes, WINDOW as usize);
            assert!(state.received.len() <= WINDOW as usize / RECEIVE_COALESCE_LIMIT);
            assert!(
                state.received.iter().map(BytesMut::capacity).sum::<usize>() <= 2 * WINDOW as usize
            );
            drop(shared);
        }
        let mut stream = Stream::new(connection.shared.clone(), 1);
        let mut received = vec![0; expected.len()];
        stream.read_exact(&mut received).await.unwrap();
        assert_eq!(received, expected);
        let shared = lock(&connection.shared);
        assert_eq!(shared.streams.get(&1).unwrap().received_bytes, 0);
        drop(shared);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn coalescing_after_partial_application_reads_preserves_byte_order() {
    timeout(Duration::from_secs(3), async {
        let (transport, _peer) = tokio::io::duplex(64);
        let mut connection = Connection::new(transport, Config::default(), Mode::Server);
        for value in 0..100u8 {
            connection
                .handle_frame(
                    Header {
                        kind: FrameType::Data,
                        flags: if value == 0 { SYN } else { 0 },
                        stream_id: 1,
                        value: 1,
                    },
                    BytesMut::from(&[value][..]),
                )
                .unwrap();
        }
        let mut stream = Stream::new(connection.shared.clone(), 1);
        let mut prefix = [0; 37];
        stream.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix.as_slice(), (0..37u8).collect::<Vec<_>>());
        for value in 100..155u8 {
            connection
                .handle_frame(
                    Header {
                        kind: FrameType::Data,
                        flags: 0,
                        stream_id: 1,
                        value: 1,
                    },
                    BytesMut::from(&[value][..]),
                )
                .unwrap();
        }
        let mut suffix = [0; 118];
        stream.read_exact(&mut suffix).await.unwrap();
        assert_eq!(suffix.as_slice(), (37..155u8).collect::<Vec<_>>());
    })
    .await
    .unwrap();
}
