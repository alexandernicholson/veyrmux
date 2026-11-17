//! Application streams share only bounded queues and synchronous state locks.
use crate::frame::{FrameType, HEADER_LEN, Header, SYN};
use crate::session::{Shared, StreamId, WINDOW, lock, remember};
use bytes::Buf;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// One independently half-closable Yamux byte stream.
/// The owning connection must be continuously driven for I/O to progress.
/// Dropping an unfinished stream sends RST, without closing sibling streams.
pub struct Stream {
    shared: Arc<Mutex<Shared>>,
    id: u32,
}
impl Stream {
    pub(crate) const fn new(shared: Arc<Mutex<Shared>>, id: u32) -> Self {
        Self { shared, id }
    }
    /// The peer-visible stream identifier.
    pub const fn id(&self) -> StreamId {
        StreamId(self.id)
    }
}
impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if destination.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut shared = lock(&self.shared);
        let terminal = shared.terminal;
        let Some(stream) = shared.streams.get_mut(&self.id) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream no longer exists",
            )));
        };
        if stream.reset {
            return Poll::Ready(Ok(()));
        }
        if let Some(mut bytes) = stream.received.pop_front() {
            let length = destination.remaining().min(bytes.len());
            // length is bounded by both source and destination before slicing.
            destination.put_slice(&bytes[..length]);
            bytes.advance(length);
            if !bytes.is_empty() {
                stream.received.push_front(bytes);
            }
            stream.received_bytes -= length;
            stream.pending_credit += length as u32;
            let update = stream.pending_credit >= WINDOW / 2 && !stream.remote_fin;
            shared.finish_if_closed(self.id);
            if update {
                shared.schedule(self.id);
            }
            return Poll::Ready(Ok(()));
        }
        if stream.remote_fin {
            return Poll::Ready(Ok(()));
        }
        if let Some((kind, message)) = terminal {
            return Poll::Ready(Err(io::Error::new(kind, message)));
        }
        remember(&mut stream.read_waker, cx.waker());
        if stream.pending_flags & SYN != 0 {
            shared.schedule(self.id);
        }
        Poll::Pending
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut shared = lock(&self.shared);
        if let Some((kind, message)) = shared.terminal {
            return Poll::Ready(Err(io::Error::new(kind, message)));
        }
        let available = shared
            .config
            .pending_budget
            .saturating_sub(shared.queued_bytes)
            .saturating_sub(HEADER_LEN);
        let split_send_size = shared.config.split_send_size;
        let Some(stream) = shared.streams.get_mut(&self.id) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream no longer exists",
            )));
        };
        if stream.local_fin || stream.reset {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream write half is closed",
            )));
        }
        let length = data
            .len()
            .min(split_send_size)
            .min(stream.send_credit as usize)
            .min(available);
        if length == 0 {
            remember(&mut stream.write_waker, cx.waker());
            if available == 0 {
                shared.write_waiters.insert(self.id, cx.waker().clone());
            }
            shared.wake_driver();
            return Poll::Pending;
        }
        stream.send_credit -= length as u32;
        let flags = stream.pending_flags;
        stream.pending_flags = 0;
        if flags & SYN != 0 {
            stream.announced = true;
        }
        let sequence = shared.enqueue(
            Header {
                kind: FrameType::Data,
                flags,
                stream_id: self.id,
                value: length as u32,
            },
            &data[..length],
        );
        if let Some(stream) = shared.streams.get_mut(&self.id) {
            stream.last_sequence = sequence;
        }
        drop(shared);
        Poll::Ready(Ok(length))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut shared = lock(&self.shared);
        let Some(stream) = shared.streams.get(&self.id) else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream no longer exists",
            )));
        };
        if stream.reset {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "stream reset",
            )));
        }
        let pending_control = stream.pending_flags != 0 || stream.pending_fin;
        let target = if pending_control {
            u64::MAX
        } else {
            stream.last_sequence
        };
        if !pending_control && target <= shared.flushed_sequence {
            return Poll::Ready(Ok(()));
        }
        if let Some((kind, message)) = shared.terminal {
            return Poll::Ready(Err(io::Error::new(kind, message)));
        }
        shared
            .flush_waiters
            .insert(self.id, (target, cx.waker().clone()));
        if pending_control {
            shared.schedule(self.id);
        } else {
            shared.wake_driver();
        }
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        {
            let mut shared = lock(&self.shared);
            let terminal = shared.terminal;
            let flushed = shared.flushed_sequence;
            let Some(stream) = shared.streams.get_mut(&self.id) else {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stream no longer exists",
                )));
            };
            if stream
                .fin_sequence
                .is_some_and(|sequence| sequence <= flushed)
            {
                return Poll::Ready(Ok(()));
            }
            if let Some((kind, message)) = terminal {
                return Poll::Ready(Err(io::Error::new(kind, message)));
            }
            if stream.reset {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "stream reset",
                )));
            }
            if !stream.local_fin {
                stream.local_fin = true;
                stream.pending_fin = true;
                shared.finish_if_closed(self.id);
                shared.schedule(self.id);
            }
        }
        self.poll_flush(cx)
    }
}
impl Drop for Stream {
    fn drop(&mut self) {
        lock(&self.shared).remove(self.id);
    }
}
