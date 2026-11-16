#![forbid(unsafe_code)]
use std::{future::poll_fn, task::Poll, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

fn open_header(id: u32) -> [u8; 12] {
    let mut header = [0; 12];
    header[1] = 1;
    header[3] = 1;
    header[4..8].copy_from_slice(&id.to_be_bytes());
    header
}

#[tokio::test]
async fn independent_peer_streams_may_be_announced_out_of_identifier_order() {
    timeout(Duration::from_secs(3), async {
        let (mut peer, transport) = tokio::io::duplex(1024);
        peer.write_all(&open_header(3)).await.unwrap();
        peer.write_all(&open_header(1)).await.unwrap();
        let mut connection =
            veyrmux::Connection::new(transport, veyrmux::Config::default(), veyrmux::Mode::Server);
        let second_allocated = poll_fn(|cx| connection.poll_next_inbound(cx))
            .await
            .unwrap()
            .unwrap();
        let first_allocated = poll_fn(|cx| connection.poll_next_inbound(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_allocated.id().val(), 3);
        assert_eq!(first_allocated.id().val(), 1);
    })
    .await
    .unwrap();
}

#[cfg(feature = "futures-compat")]
#[tokio::test]
async fn futures_adapter_announces_only_the_stream_that_performs_io() {
    use tokio_util::compat::TokioAsyncReadCompatExt;
    timeout(Duration::from_secs(3), async {
        let (transport, mut peer) = tokio::io::duplex(1024);
        let mut connection = veyrmux::yamux_shim::Connection::new(transport.compat(), veyrmux::Config::default(), veyrmux::Mode::Client);
        let mut first = poll_fn(|cx| connection.poll_new_outbound(cx)).await.unwrap();
        let mut second = poll_fn(|cx| connection.poll_new_outbound(cx)).await.unwrap();
        assert_eq!(first.id().val(), 1);
        assert_eq!(second.id().val(), 3);
        poll_fn(|cx| {
            assert!(connection.poll_next_inbound(cx).is_pending());
            Poll::Ready(())
        }).await;
        let mut peek = [0];
        assert!(timeout(Duration::from_millis(10), peer.read(&mut peek)).await.is_err(), "opening idle streams unexpectedly emitted SYN");
        for (stream, id, payload) in [(&mut second, 3_u32, b"two"), (&mut first, 1, b"one")] {
            futures::io::AsyncWriteExt::write_all(stream, payload).await.unwrap();
            let mut wire = [0; 15];
            tokio::select! {
                result = peer.read_exact(&mut wire) => { result.unwrap(); }
                _ = poll_fn(|cx| connection.poll_next_inbound(cx)) => panic!("connection ended during a lazy stream open"),
            }
            assert_eq!(&wire[4..8], &id.to_be_bytes());
            assert_eq!(u16::from_be_bytes([wire[2], wire[3]]) & 1, 1);
            assert_eq!(&wire[12..], payload);
        }
    }).await.unwrap();
}
