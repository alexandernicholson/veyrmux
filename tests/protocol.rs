#![forbid(unsafe_code)]
mod support;

use std::{future::poll_fn, time::Duration};
use support::pair;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(10);

async fn exchange(reference_client: bool, reference_server: bool, streams: usize) {
    timeout(DEADLINE, async {
        let (client, mut server) = pair(reference_client, reference_server);
        let mut tasks = tokio::task::JoinSet::new();
        for n in 0..streams {
            let mut outgoing = client.open().await;
            // Above the exact 256 KiB boundary where broken window handling hangs.
            let length = 1024 * 1024 + 37 + n;
            let expected: Vec<u8> = (0..length)
                .map(|i| (i.wrapping_mul(31) ^ n) as u8)
                .collect();
            tasks.spawn(async move {
                let (mut reader, mut writer) = tokio::io::split(&mut outgoing);
                let mut received = Vec::with_capacity(expected.len());
                let sending = async {
                    writer.write_all(&expected).await.unwrap();
                    writer.shutdown().await.unwrap();
                };
                let receiving = async {
                    reader.read_to_end(&mut received).await.unwrap();
                };
                tokio::join!(sending, receiving);
                assert_eq!(received, expected, "stream {n} corrupted or truncated");
            });
            let incoming = server.accept().await;
            tasks.spawn(async move {
                let (mut reader, mut writer) = tokio::io::split(incoming);
                tokio::io::copy(&mut reader, &mut writer).await.unwrap();
                writer.shutdown().await.unwrap();
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("stream flow control, half-close, or driver deadlocked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_native_streams_preserve_bytes_above_receive_window() {
    exchange(false, false, 8).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yamux_client_can_exchange_large_streams_with_veyrmux_server() {
    exchange(true, false, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn veyrmux_client_can_exchange_large_streams_with_yamux_server() {
    exchange(false, true, 4).await;
}

#[tokio::test]
async fn peer_fin_is_eof_but_opposite_direction_still_writes() {
    timeout(DEADLINE, async {
        let (client, mut server) = pair(false, false);
        let mut outgoing = client.open().await;
        let mut incoming = server.accept().await;
        outgoing.write_all(b"request").await.unwrap();
        outgoing.shutdown().await.unwrap();
        let mut request = Vec::new();
        incoming.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request");
        incoming.write_all(b"response-after-eof").await.unwrap();
        incoming.shutdown().await.unwrap();
        let mut response = Vec::new();
        outgoing.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"response-after-eof");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropped_stream_resets_without_killing_the_connection() {
    timeout(DEADLINE, async {
        let (client, mut server) = pair(false, false);
        let outgoing = client.open().await;
        let mut incoming = server.accept().await;
        drop(outgoing);
        let mut byte = [0];
        assert_eq!(incoming.read(&mut byte).await.unwrap(), 0);
        assert!(incoming.write_all(b"reset stream").await.is_err());
        let mut next = client.open().await;
        let mut next_peer = server.accept().await;
        next.write_all(b"alive").await.unwrap();
        let mut received = [0; 5];
        next_peer.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"alive");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_connection_wakes_stream_readers_and_writers() {
    timeout(DEADLINE, async {
        let (client_io, _other) = tokio::io::duplex(64);
        let mut connection =
            veyrmux::Connection::new(client_io, veyrmux::Config::default(), veyrmux::Mode::Client);
        let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx))
            .await
            .unwrap();
        drop(connection);
        let mut byte = [0];
        let read = stream.read(&mut byte).await;
        assert!(matches!(read, Ok(0) | Err(_)));
        assert!(stream.write_all(b"dead connection").await.is_err());
    })
    .await
    .unwrap();
}

fn header(kind: u8, flags: u16, id: u32, value: u32) -> [u8; 12] {
    let mut wire = [0; 12];
    wire[1] = kind;
    wire[2..4].copy_from_slice(&flags.to_be_bytes());
    wire[4..8].copy_from_slice(&id.to_be_bytes());
    wire[8..12].copy_from_slice(&value.to_be_bytes());
    wire
}

#[tokio::test]
async fn peer_stream_ids_and_window_overflow_fail_closed() {
    for frames in [
        vec![header(1, 1, 2, 0)],                     // client must open odd IDs
        vec![header(1, 1, 1, 0), header(1, 1, 1, 0)], // reused SYN
        vec![header(1, 1, 1, 0), header(1, 0, 1, u32::MAX)],
    ] {
        timeout(DEADLINE, async {
            let (mut raw, server_io) = tokio::io::duplex(1024);
            let mut server = veyrmux::Connection::new(
                server_io,
                veyrmux::Config::default(),
                veyrmux::Mode::Server,
            );
            for frame in &frames {
                raw.write_all(frame).await.unwrap();
            }
            let mut accepted = Vec::new();
            loop {
                match poll_fn(|cx| server.poll_next_inbound(cx)).await {
                    Some(Err(_)) => break,
                    Some(Ok(stream)) => accepted.push(stream),
                    None => panic!("malformed peer was silently accepted"),
                }
            }
            assert!(
                accepted.len() < frames.len(),
                "every malformed frame was admitted"
            );
        })
        .await
        .expect("malformed-frame rejection did not complete");
    }
}

#[tokio::test]
async fn ping_is_echoed_with_ack_and_does_not_open_a_stream() {
    timeout(DEADLINE, async {
        let (mut raw, server_io) = tokio::io::duplex(1024);
        let mut server =
            veyrmux::Connection::new(server_io, veyrmux::Config::default(), veyrmux::Mode::Server);
        let task = tokio::spawn(async move { poll_fn(|cx| server.poll_next_inbound(cx)).await });
        raw.write_all(&header(2, 1, 0, 0x1234_abcd)).await.unwrap();
        let mut ack = [0; 12];
        raw.read_exact(&mut ack).await.unwrap();
        assert_eq!(ack, header(2, 2, 0, 0x1234_abcd));
        assert!(
            !task.is_finished(),
            "ping incorrectly returned an incoming stream"
        );
        task.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn data_cannot_exceed_the_advertised_receive_credit() {
    timeout(DEADLINE, async {
        let (mut raw, server_io) = tokio::io::duplex(1024 * 1024);
        let mut server =
            veyrmux::Connection::new(server_io, veyrmux::Config::default(), veyrmux::Mode::Server);
        raw.write_all(&header(0, 1, 1, 256 * 1024 + 1))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            raw.write_all(&vec![0x5a; 256 * 1024 + 1]).await.unwrap();
            raw
        });
        let mut streams = Vec::new();
        loop {
            match poll_fn(|cx| server.poll_next_inbound(cx)).await {
                Some(Err(_)) => break,
                Some(Ok(stream)) => streams.push(stream),
                None => panic!("oversized payload was not rejected"),
            }
        }
        assert!(
            streams.is_empty(),
            "peer was admitted before its credit was validated"
        );
        task.abort();
    })
    .await
    .unwrap();
}
