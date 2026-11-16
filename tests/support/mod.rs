use std::{
    future::poll_fn,
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

pub enum TestStream {
    Native(veyrmux::Stream),
    Reference(Compat<yamux::Stream>),
}

impl AsyncRead for TestStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(s) => Pin::new(s).poll_read(cx, buf),
            Self::Reference(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for TestStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Native(s) => Pin::new(s).poll_write(cx, buf),
            Self::Reference(s) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(s) => Pin::new(s).poll_flush(cx),
            Self::Reference(s) => Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(s) => Pin::new(s).poll_shutdown(cx),
            Self::Reference(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

enum TestConnection {
    Native(veyrmux::Connection<DuplexStream>),
    Reference(Box<yamux::Connection<Compat<DuplexStream>>>),
}
impl TestConnection {
    fn poll_open(&mut self, cx: &mut Context<'_>) -> Poll<Result<TestStream, String>> {
        match self {
            Self::Native(conn) => conn
                .poll_new_outbound(cx)
                .map(|r| r.map(TestStream::Native).map_err(|e| e.to_string())),
            Self::Reference(conn) => conn.poll_new_outbound(cx).map(|r| {
                r.map(|s| TestStream::Reference(s.compat()))
                    .map_err(|e| e.to_string())
            }),
        }
    }
    fn poll_inbound(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<TestStream, String>>> {
        match self {
            Self::Native(conn) => conn
                .poll_next_inbound(cx)
                .map(|r| r.map(|s| s.map(TestStream::Native).map_err(|e| e.to_string()))),
            Self::Reference(conn) => conn.poll_next_inbound(cx).map(|r| {
                r.map(|s| {
                    s.map(|s| TestStream::Reference(s.compat()))
                        .map_err(|e| e.to_string())
                })
            }),
        }
    }
}

pub struct Peer {
    open: mpsc::Sender<oneshot::Sender<Result<TestStream, String>>>,
    incoming: mpsc::Receiver<TestStream>,
    driver: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub async fn open(&self) -> TestStream {
        let (tx, rx) = oneshot::channel();
        self.open.send(tx).await.unwrap();
        rx.await.unwrap().unwrap()
    }
    pub async fn accept(&mut self) -> TestStream {
        self.incoming.recv().await.unwrap()
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

fn drive(mut conn: TestConnection) -> Peer {
    let (open, mut commands) = mpsc::channel::<oneshot::Sender<Result<TestStream, String>>>(64);
    let (tx, incoming) = mpsc::channel(64);
    let driver = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(reply) = commands.recv() => {
                    let result = poll_fn(|cx| conn.poll_open(cx)).await;
                    let _receiver_closed = reply.send(result);
                }
                incoming = poll_fn(|cx| conn.poll_inbound(cx)) => {
                    match incoming {
                        Some(Ok(stream)) => if tx.send(stream).await.is_err() { return; },
                        Some(Err(_)) | None => return,
                    }
                }
            }
        }
    });
    Peer {
        open,
        incoming,
        driver,
    }
}

pub fn pair(reference_client: bool, reference_server: bool) -> (Peer, Peer) {
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = if reference_client {
        TestConnection::Reference(Box::new(yamux::Connection::new(
            client_io.compat(),
            yamux::Config::default(),
            yamux::Mode::Client,
        )))
    } else {
        TestConnection::Native(veyrmux::Connection::new(
            client_io,
            veyrmux::Config::default(),
            veyrmux::Mode::Client,
        ))
    };
    let server = if reference_server {
        TestConnection::Reference(Box::new(yamux::Connection::new(
            server_io.compat(),
            yamux::Config::default(),
            yamux::Mode::Server,
        )))
    } else {
        TestConnection::Native(veyrmux::Connection::new(
            server_io,
            veyrmux::Config::default(),
            veyrmux::Mode::Server,
        ))
    };
    (drive(client), drive(server))
}
