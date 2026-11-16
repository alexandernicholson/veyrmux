//! Adapter for consumers of the `futures_io`-based Yamux connection API.
//!
//! Framing, limits and errors use the native Veyrmux implementation. Like the
//! reference Futures API, an outbound stream is announced on its first I/O.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

pub use crate::{Config, ConnectionError, Mode, Role, StreamId};

/// A connection whose transport implements the Futures I/O traits.
pub struct Connection<T> {
    inner: crate::Connection<Compat<T>>,
}

impl<T: futures_io::AsyncRead + futures_io::AsyncWrite + Unpin> Connection<T> {
    /// Wrap a transport with the configured endpoint role and resource limits.
    pub fn new(io: T, mut config: Config, mode: Mode) -> Self {
        config.eager_open = false;
        Self {
            inner: crate::Connection::new(io.compat(), config, mode),
        }
    }

    /// Poll for an incoming stream while continuously advancing transport I/O.
    pub fn poll_next_inbound(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Stream, ConnectionError>>> {
        self.inner
            .poll_next_inbound(cx)
            .map(|item| item.map(|result| result.map(|inner| Stream { inner })))
    }

    /// Open a local stream. The connection must continue being driven afterward.
    pub fn poll_new_outbound(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Stream, ConnectionError>> {
        self.inner
            .poll_new_outbound(cx)
            .map(|result| result.map(|inner| Stream { inner }))
    }

    /// Send GoAway and perform the transport's graceful close sequence.
    pub fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectionError>> {
        self.inner.poll_close(cx)
    }
}

/// Futures I/O view of a native Veyrmux stream.
#[derive(Debug)]
pub struct Stream {
    inner: crate::Stream,
}

impl Stream {
    /// The stream's stable, wire-level identifier.
    pub const fn id(&self) -> StreamId {
        self.inner.id()
    }
}

impl futures_io::AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut buffer = ReadBuf::new(bytes);
        match Pin::new(&mut self.get_mut().inner).poll_read(cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(buffer.filled().len())),
        }
    }
}

impl futures_io::AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
