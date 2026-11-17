#![forbid(unsafe_code)]
use std::{future::poll_fn, pin::Pin, task::Poll, time::Duration};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;
use veyrmux::{Config, Connection, Mode};

const WINDOW: usize = 256 * 1024;

#[tokio::test]
async fn configured_data_sizes_preserve_credit_backpressure_and_resume_on_grant() {
    timeout(Duration::from_secs(3), async {
        for chunk in [16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, WINDOW] {
            let (transport, mut peer) = tokio::io::duplex(2 * WINDOW);
            let mut config = Config::default();
            config.set_split_send_size(chunk);
            let mut connection = Connection::new(transport, config, Mode::Client);
            let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx))
                .await
                .unwrap();
            let payload = vec![0xa5; WINDOW + 1];
            let mut queued = 0;
            while queued < WINDOW {
                let written = stream.write(&payload).await.unwrap();
                assert_eq!(written, chunk.min(WINDOW - queued));
                queued += written;
            }
            poll_fn(|cx| {
                assert!(
                    Pin::new(&mut stream)
                        .poll_write(cx, b"blocked")
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            let mut update = [0; 12];
            update[1] = 1; // WINDOW_UPDATE
            update[3] = 2; // ACK of the opening DATA
            update[4..8].copy_from_slice(&stream.id().val().to_be_bytes());
            update[8..].copy_from_slice(&((WINDOW / 2) as u32).to_be_bytes());
            peer.write_all(&update).await.unwrap();
            let written = poll_fn(|cx| {
                assert!(connection.poll_next_inbound(cx).is_pending());
                Pin::new(&mut stream).poll_write(cx, &payload)
            })
            .await
            .unwrap();
            assert_eq!(written, chunk.min(WINDOW / 2));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn configured_data_size_is_still_limited_by_caller_and_queue_capacity() {
    timeout(Duration::from_secs(3), async {
        for (chunk, budget, caller) in [(1, 76, 64), (128 * 1024, 76, 1024), (128 * 1024, 1024, 7)]
        {
            let (transport, _peer) = tokio::io::duplex(1024);
            let mut config = Config::default();
            config
                .set_split_send_size(chunk)
                .set_max_pending_bytes(budget);
            let mut connection = Connection::new(transport, config, Mode::Client);
            let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx))
                .await
                .unwrap();
            let written = stream.write(&vec![0x5a; caller]).await.unwrap();
            assert_eq!(written, caller.min(chunk).min(budget - 12));
        }
    })
    .await
    .unwrap();
}
