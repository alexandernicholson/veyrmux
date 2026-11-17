#![forbid(unsafe_code)]
use std::{
    future::poll_fn,
    io::{self, IoSlice},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::time::timeout;

#[derive(Default)]
struct Capture {
    wire: Vec<u8>,
    vectors: Vec<(usize, usize)>,
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    Zero,
    Overreported,
}
struct Socket {
    capture: Arc<Mutex<Capture>>,
    limit: usize,
    alternate: bool,
    pending: bool,
    fault: Fault,
}
impl Socket {
    fn emit(&mut self, cx: &Context<'_>, buffers: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.pending = self.alternate;
        let offered: usize = buffers.iter().map(|b| b.len()).sum();
        match self.fault {
            Fault::Zero => return Poll::Ready(Ok(0)),
            Fault::Overreported => return Poll::Ready(Ok(offered + 1)),
            Fault::None => {}
        }
        let written = offered.min(self.limit);
        let mut remaining = written;
        let mut capture = self.capture.lock().unwrap();
        capture.vectors.push((buffers.len(), offered));
        for buffer in buffers {
            let count = remaining.min(buffer.len());
            capture.wire.extend_from_slice(&buffer[..count]);
            remaining -= count;
            if remaining == 0 {
                break;
            }
        }
        drop(capture);
        Poll::Ready(Ok(written))
    }
}
impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buffer: &mut ReadBuf<'_>,
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
        self.get_mut().emit(cx, &[IoSlice::new(bytes)])
    }
    fn is_write_vectored(&self) -> bool {
        true
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().emit(cx, buffers)
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
struct Fixture {
    connection: veyrmux::Connection<Socket>,
    _streams: Vec<veyrmux::Stream>,
    capture: Arc<Mutex<Capture>>,
    expected: Vec<u8>,
}
impl Fixture {
    async fn new(
        streams: usize,
        bytes: usize,
        limit: usize,
        alternate: bool,
        fault: Fault,
    ) -> Self {
        let capture = Arc::new(Mutex::new(Capture::default()));
        let socket = Socket {
            capture: capture.clone(),
            limit,
            alternate,
            pending: false,
            fault,
        };
        let mut config = veyrmux::Config::default();
        config.set_split_send_size(bytes.max(16 * 1024));
        let mut connection = veyrmux::Connection::new(socket, config, veyrmux::Mode::Client);
        let mut handles = Vec::new();
        let mut expected = Vec::new();
        for n in 0..streams {
            let mut stream = poll_fn(|cx| connection.poll_new_outbound(cx))
                .await
                .unwrap();
            let payload = vec![(n + 1) as u8; bytes];
            stream.write_all(&payload).await.unwrap();
            expected.extend_from_slice(&[0, 0, 0, 1]);
            expected.extend_from_slice(&stream.id().val().to_be_bytes());
            expected.extend_from_slice(&(bytes as u32).to_be_bytes());
            expected.extend_from_slice(&payload);
            handles.push(stream);
        }
        Self {
            connection,
            _streams: handles,
            capture,
            expected,
        }
    }
    async fn drain(&mut self) {
        timeout(
            Duration::from_secs(3),
            poll_fn(|cx| {
                if let Poll::Ready(result) = self.connection.poll_next_inbound(cx) {
                    panic!("unexpected connection event: {result:?}");
                }
                if self.capture.lock().unwrap().wire.len() >= self.expected.len() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("vectored write did not make progress");
    }
}

#[tokio::test]
async fn multiple_frames_use_one_write_without_reordering_bytes() {
    let mut fixture = Fixture::new(4, 19, usize::MAX, false, Fault::None).await;
    fixture.drain().await;
    let capture = fixture.capture.lock().unwrap();
    assert_eq!(capture.wire, fixture.expected);
    assert_eq!(capture.vectors, [(4, 4 * (12 + 19))]);
    drop(capture);
}

#[tokio::test]
async fn partial_vectored_writes_cross_headers_without_duplication_or_loss() {
    let mut fixture = Fixture::new(4, 19, 7, true, Fault::None).await;
    fixture.drain().await;
    let capture = fixture.capture.lock().unwrap();
    assert_eq!(capture.wire, fixture.expected);
    assert!(
        capture.vectors.len() > 4,
        "fake did not force partial progress"
    );
    assert!(capture.vectors.iter().any(|&(count, _)| count > 1));
    drop(capture);
}

#[tokio::test]
async fn descriptor_and_byte_limits_bound_each_write_batch() {
    for (streams, bytes) in [(32, 64), (6, 16384), (6, 128 * 1024), (1, 256 * 1024)] {
        let mut fixture = Fixture::new(streams, bytes, usize::MAX, false, Fault::None).await;
        fixture.drain().await;
        let capture = fixture.capture.lock().unwrap();
        assert_eq!(capture.wire, fixture.expected);
        assert!(capture.vectors.len() > 1);
        assert!(
            capture
                .vectors
                .iter()
                .all(|&(count, bytes)| count <= 16 && bytes <= 65536)
        );
        drop(capture);
    }
}

#[tokio::test]
async fn broken_vectored_writers_fail_only_the_connection() {
    for fault in [Fault::Zero, Fault::Overreported] {
        let mut fixture = Fixture::new(2, 19, usize::MAX, false, fault).await;
        let outcome = timeout(
            Duration::from_secs(3),
            poll_fn(|cx| fixture.connection.poll_next_inbound(cx)),
        )
        .await
        .unwrap();
        assert!(matches!(outcome, Some(Err(_))));
        assert!(fixture._streams[0].write_all(b"closed").await.is_err());
    }
}

#[tokio::test]
async fn partial_large_frames_preserve_bytes_and_do_not_finish_flush_early() {
    let mut fixture = Fixture::new(1, 128 * 1024, 65536, true, Fault::None).await;
    poll_fn(|cx| {
        assert!(fixture.connection.poll_next_inbound(cx).is_pending());
        assert!(
            Pin::new(&mut fixture._streams[0])
                .poll_flush(cx)
                .is_pending()
        );
        Poll::Ready(())
    })
    .await;
    assert_eq!(fixture.capture.lock().unwrap().wire.len(), 128 * 1024);
    fixture.drain().await;
    timeout(Duration::from_secs(3), fixture._streams[0].flush())
        .await
        .unwrap()
        .unwrap();
    let capture = fixture.capture.lock().unwrap();
    assert_eq!(capture.wire, fixture.expected);
    assert!(capture.vectors.iter().all(|&(_, bytes)| bytes <= 65536));
    drop(capture);
}
