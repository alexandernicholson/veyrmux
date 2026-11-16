#![forbid(unsafe_code)]
use std::{
    future::poll_fn,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::time::timeout;

#[derive(Default)]
struct Capture {
    chunks: Vec<Vec<u8>>,
    wire: Vec<u8>,
}

struct Socket {
    capture: Arc<Mutex<Capture>>,
    max_write: usize,
    alternate_pending: bool,
    pending: bool,
}
impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}
impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.pending {
            this.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.pending = this.alternate_pending;
        let count = bytes.len().min(this.max_write);
        let mut capture = this.capture.lock().unwrap();
        capture.chunks.push(bytes[..count].to_vec());
        capture.wire.extend_from_slice(&bytes[..count]);
        drop(capture);
        Poll::Ready(Ok(count))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn payload_from_wire(wire: &[u8]) -> Option<Vec<u8>> {
    let mut remaining = wire;
    let mut payload = Vec::new();
    while !remaining.is_empty() {
        let header = remaining.get(..12)?;
        let size = u32::from_be_bytes(header.get(8..12)?.try_into().ok()?) as usize;
        let body_len = if header[1] == 0 { size } else { 0 };
        let body = remaining.get(12..12 + body_len)?;
        payload.extend_from_slice(body);
        remaining = remaining.get(12 + body_len..)?;
    }
    Some(payload)
}

async fn capture_frame(max_write: usize, alternate_pending: bool) -> Arc<Mutex<Capture>> {
    let capture = Arc::new(Mutex::new(Capture::default()));
    let socket = Socket {
        capture: capture.clone(),
        max_write,
        alternate_pending,
        pending: false,
    };
    let mut connection =
        veyrmux::Connection::new(socket, veyrmux::Config::default(), veyrmux::Mode::Client);
    let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx))
        .await
        .unwrap();
    let payload = b"one-coalesced-frame";
    stream.write_all(payload).await.unwrap();
    timeout(
        Duration::from_secs(2),
        poll_fn(|cx| {
            let _not_inbound = connection.poll_next_inbound(cx);
            let snapshot = capture.lock().unwrap();
            if payload_from_wire(&snapshot.wire).as_deref() == Some(payload.as_slice()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .expect("partial socket writes stopped making progress");
    capture
}

#[tokio::test]
async fn data_header_and_payload_reach_the_writer_together() {
    let capture = capture_frame(usize::MAX, false).await;
    let capture = capture.lock().unwrap();
    assert_eq!(
        payload_from_wire(&capture.wire).unwrap(),
        b"one-coalesced-frame"
    );
    let boundaries: Vec<_> = capture
        .chunks
        .iter()
        .scan(0, |end, chunk| {
            let start = *end;
            *end += chunk.len();
            Some((start, *end))
        })
        .collect();
    let mut offset = 0;
    let mut data_frames = 0;
    while offset < capture.wire.len() {
        let header = &capture.wire[offset..offset + 12];
        let payload_len = if header[1] == 0 {
            u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize
        } else {
            0
        };
        if payload_len > 0 {
            data_frames += 1;
            assert!(
                boundaries
                    .iter()
                    .any(|&(start, end)| { start <= offset && end >= offset + 12 + payload_len }),
                "data frame header emitted separately"
            );
        }
        offset += 12 + payload_len;
    }
    assert!(data_frames > 0, "test never observed a data frame");
    drop(capture);
}

#[tokio::test]
async fn partial_writes_and_pending_preserve_frame_byte_order() {
    let capture = capture_frame(7, true).await;
    let capture = capture.lock().unwrap();
    assert!(
        capture.chunks.len() >= 4,
        "fake failed to force partial writes"
    );
    assert!(capture.chunks.iter().all(|chunk| chunk.len() <= 7));
    assert_eq!(
        payload_from_wire(&capture.wire).unwrap(),
        b"one-coalesced-frame"
    );
    drop(capture);
}
