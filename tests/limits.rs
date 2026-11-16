#![forbid(unsafe_code)]
use std::{future::poll_fn, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use veyrmux::{Config, Connection, ConnectionError, Mode};

const DEADLINE: Duration = Duration::from_secs(3);

fn header(kind: u8, flags: u16, id: u32, value: u32) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[1] = kind;
    bytes[2..4].copy_from_slice(&flags.to_be_bytes());
    bytes[4..8].copy_from_slice(&id.to_be_bytes());
    bytes[8..12].copy_from_slice(&value.to_be_bytes());
    bytes
}

#[tokio::test]
async fn stream_capacity_is_reclaimed_on_drop_without_reusing_identifiers() {
    timeout(DEADLINE, async {
        let (transport, _peer) = tokio::io::duplex(1024);
        let mut config = Config::default();
        config.set_max_num_streams(1);
        let mut connection = Connection::new(transport, config, Mode::Client);
        let first = poll_fn(|cx| connection.poll_new_outbound(cx))
            .await
            .unwrap();
        assert_eq!(first.id().val(), 1);
        assert!(matches!(
            poll_fn(|cx| connection.poll_new_outbound(cx)).await,
            Err(ConnectionError::TooManyStreams)
        ));
        drop(first);
        for expected in (3..103).step_by(2) {
            let next = poll_fn(|cx| connection.poll_new_outbound(cx))
                .await
                .unwrap();
            assert_eq!(next.id().val(), expected);
            drop(next);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_limits_fail_before_creating_a_stream() {
    let mut no_streams = Config::default();
    no_streams.set_max_num_streams(0);
    let mut no_send_credit = Config::default();
    no_send_credit.set_max_send_window(256 * 1024 - 1);
    let mut no_frame_space = Config::default();
    no_frame_space.set_max_pending_bytes(12);
    for config in [no_streams, no_send_credit, no_frame_space] {
        let (transport, _peer) = tokio::io::duplex(128);
        let mut connection = Connection::new(transport, config, Mode::Client);
        assert!(matches!(
            poll_fn(|cx| connection.poll_new_outbound(cx)).await,
            Err(ConnectionError::InvalidConfig)
        ));
    }
}

#[tokio::test]
async fn aggregate_receive_budget_limits_admission() {
    let (transport, _peer) = tokio::io::duplex(128);
    let mut config = Config::default();
    config
        .set_max_num_streams(8)
        .set_max_connection_receive_window(Some(256 * 1024));
    let mut connection = Connection::new(transport, config, Mode::Client);
    let first = poll_fn(|cx| connection.poll_new_outbound(cx))
        .await
        .unwrap();
    assert!(matches!(
        poll_fn(|cx| connection.poll_new_outbound(cx)).await,
        Err(ConnectionError::TooManyStreams)
    ));
    drop(first);
    assert!(poll_fn(|cx| connection.poll_new_outbound(cx)).await.is_ok());
}

#[tokio::test]
async fn queued_byte_budget_backpressures_writes_until_driver_progress() {
    timeout(DEADLINE, async {
        let (transport, _peer) = tokio::io::duplex(32);
        let mut config = Config::default();
        config.set_max_pending_bytes(12 + 64);
        let mut connection = Connection::new(transport, config, Mode::Client);
        let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx))
            .await
            .unwrap();
        let count = stream.write(&[0x5a; 1024]).await.unwrap();
        assert_eq!(count, 64, "write exceeded configured queued frame capacity");
        assert!(
            timeout(Duration::from_millis(10), stream.write(b"blocked"))
                .await
                .is_err(),
            "full queue failed to backpressure producer"
        );
        drop(connection);
        assert!(
            stream.write(b"closed").await.is_err(),
            "blocked writer did not observe connection drop"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn partial_header_and_partial_payload_eof_are_errors() {
    for bytes in [
        header(1, 1, 1, 0)[..7].to_vec(),
        [header(0, 1, 1, 4).as_slice(), b"xy"].concat(),
    ] {
        timeout(DEADLINE, async {
            let (mut peer, transport) = tokio::io::duplex(1024);
            peer.write_all(&bytes).await.unwrap();
            peer.shutdown().await.unwrap();
            let mut connection = Connection::new(transport, Config::default(), Mode::Server);
            assert!(matches!(
                poll_fn(|cx| connection.poll_next_inbound(cx)).await,
                Some(Err(_))
            ));
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn goaway_rejects_new_opens_but_keeps_admitted_stream_usable() {
    timeout(DEADLINE, async {
        let (mut peer, transport) = tokio::io::duplex(1024);
        let mut connection = Connection::new(transport, Config::default(), Mode::Client);
        let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx)).await.unwrap();
        peer.write_all(&header(3, 0, 0, 0)).await.unwrap();
        poll_fn(|cx| {
            assert!(connection.poll_next_inbound(cx).is_pending());
            Poll::Ready(())
        }).await;
        assert!(matches!(poll_fn(|cx| connection.poll_new_outbound(cx)).await, Err(ConnectionError::Closed)));
        stream.write_all(b"alive").await.unwrap();
        let mut wire = [0; 29]; // opening window header + data header + five bytes
        tokio::select! {
            result = peer.read_exact(&mut wire) => { result.unwrap(); }
            _ = poll_fn(|cx| connection.poll_next_inbound(cx)) => panic!("admitted stream was closed by GOAWAY"),
        }
        assert_eq!(&wire[24..], b"alive");
    }).await.unwrap();
}
