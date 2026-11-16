//! Tokio-native connection driver and bounded shared stream state.
use crate::frame::{ACK, FIN, FrameType, HEADER_LEN, Header, MAX_FRAME, RST, SYN};
use crate::stream::Stream;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

pub const WINDOW: u32 = 256 * 1024;
const WRITE_FRAME: usize = 16 * 1024;
const POLL_BUDGET: usize = 32;
const MAX_WRITE_BATCH_FRAMES: usize = 16;
const MAX_WRITE_BATCH_BYTES: usize = 64 * 1024;
const SMALL_RECEIVE_FRAME: usize = 256;
const RECEIVE_COALESCE_LIMIT: usize = 4 * 1024;

/// Which side of the transport opens odd-numbered streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Initiator; opens odd stream identifiers.
    Client,
    /// Acceptor; opens even stream identifiers.
    Server,
}
/// Connection endpoint role.
pub type Role = Mode;

/// A nonzero Yamux stream identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StreamId(pub(crate) u32);
impl StreamId {
    /// The wire identifier.
    pub const fn val(self) -> u32 {
        self.0
    }
    /// The wire identifier.
    pub const fn get(self) -> u32 {
        self.0
    }
    /// Whether the client initiated this stream.
    pub const fn is_client(self) -> bool {
        self.0 & 1 != 0
    }
    /// Whether the server initiated this stream.
    pub const fn is_server(self) -> bool {
        self.0 & 1 == 0
    }
}
impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Memory and admission limits. Every stream advertises a fixed 256 KiB window.
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) max_streams: usize,
    receive_budget: Option<usize>,
    pub(crate) pending_budget: usize,
    max_send_window: usize,
    pub(crate) eager_open: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_streams: 512,
            receive_budget: None,
            pending_budget: 1024 * 1024,
            max_send_window: u32::MAX as usize,
            eager_open: true,
        }
    }
}
impl Config {
    /// Construct the default bounded configuration.
    pub fn new() -> Self {
        Self::default()
    }
    /// Limit simultaneously active streams, including streams awaiting their peer's FIN.
    pub const fn set_max_num_streams(&mut self, max: usize) -> &mut Self {
        self.max_streams = max;
        self
    }
    /// Bound receive-credit reservations across active streams.
    /// A new stream reserves 256 KiB; `None` disables this additional admission limit.
    pub const fn set_max_connection_receive_window(&mut self, max: Option<usize>) -> &mut Self {
        self.receive_budget = max;
        self
    }
    /// Bound aggregate queued wire bytes. A frame currently being written counts too.
    /// Stream-drop resets can temporarily add one header per previously active stream.
    pub const fn set_max_pending_bytes(&mut self, max: usize) -> &mut Self {
        self.pending_budget = max;
        self
    }
    /// Reject peer credit grants that exceed this per-stream send-credit limit.
    /// This must accommodate Yamux's mandatory initial 256 KiB credit.
    pub const fn set_max_send_window(&mut self, max: usize) -> &mut Self {
        self.max_send_window = max;
        self
    }
    fn stream_limit(&self) -> usize {
        self.receive_budget.map_or(self.max_streams, |budget| {
            self.max_streams.min(budget / WINDOW as usize)
        })
    }
    const fn valid(&self) -> bool {
        self.pending_budget > HEADER_LEN
            && self.max_streams != 0
            && self.max_send_window >= WINDOW as usize
    }
}

/// A terminal transport/protocol failure, or a rejected stream-open operation.
#[derive(Debug)]
pub enum ConnectionError {
    /// Underlying transport failure.
    Io(io::Error),
    /// Invalid Yamux frame or state transition.
    Protocol(&'static str),
    /// The connection no longer accepts new streams.
    Closed,
    /// The configured stream/receive-reservation limit is exhausted.
    TooManyStreams,
    /// Invalid local limits.
    InvalidConfig,
}
impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Protocol(message) => write!(f, "Yamux protocol error: {message}"),
            Self::Closed => f.write_str("multiplexer closed"),
            Self::TooManyStreams => f.write_str("multiplexer stream limit exhausted"),
            Self::InvalidConfig => f.write_str("invalid multiplexer limits"),
        }
    }
}
impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
impl From<io::Error> for ConnectionError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub struct StreamState {
    pub(crate) received: VecDeque<BytesMut>,
    pub(crate) received_bytes: usize,
    pub(crate) receive_credit: u32,
    pub(crate) pending_credit: u32,
    pub(crate) send_credit: u32,
    pub(crate) local_fin: bool,
    pub(crate) remote_fin: bool,
    pub(crate) reset: bool,
    pub(crate) pending_flags: u16,
    pub(crate) pending_fin: bool,
    pub(crate) control_queued: bool,
    pub(crate) announced: bool,
    acked: bool,
    counted: bool,
    pub(crate) read_waker: Option<Waker>,
    pub(crate) write_waker: Option<Waker>,
    pub(crate) last_sequence: u64,
    pub(crate) fin_sequence: Option<u64>,
}
impl StreamState {
    const fn new(outbound: bool) -> Self {
        Self {
            received: VecDeque::new(),
            received_bytes: 0,
            receive_credit: WINDOW,
            pending_credit: 0,
            send_credit: WINDOW,
            local_fin: false,
            remote_fin: false,
            reset: false,
            pending_flags: if outbound { SYN } else { ACK },
            pending_fin: false,
            control_queued: false,
            announced: !outbound,
            acked: !outbound,
            counted: true,
            read_waker: None,
            write_waker: None,
            last_sequence: 0,
            fin_sequence: None,
        }
    }
}

enum Wire {
    Control {
        header: [u8; HEADER_LEN],
        offset: usize,
    },
    Data(Bytes),
}
impl Wire {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Control { header, offset } => &header[*offset..],
            Self::Data(bytes) => bytes,
        }
    }
    fn advance(&mut self, length: usize) {
        // The transport result is checked against as_slice().len() first.
        match self {
            Self::Control { offset, .. } => *offset += length,
            Self::Data(bytes) => bytes.advance(length),
        }
    }
}
pub struct QueuedFrame {
    wire: Wire,
    sequence: u64,
}

pub struct Shared {
    pub(crate) streams: HashMap<u32, StreamState>,
    queue: VecDeque<QueuedFrame>,
    control: VecDeque<u32>,
    inbound: VecDeque<u32>,
    pub(crate) queued_bytes: usize,
    pub(crate) config: Config,
    pub(crate) active: usize,
    next_sequence: u64,
    pub(crate) flushed_sequence: u64,
    pub(crate) driver_waker: Option<Waker>,
    pub(crate) write_waiters: HashMap<u32, Waker>,
    pub(crate) flush_waiters: HashMap<u32, (u64, Waker)>,
    pub(crate) terminal: Option<(io::ErrorKind, &'static str)>,
    closing: bool,
    remote_goaway: bool,
}

pub fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    match shared.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub fn remember(slot: &mut Option<Waker>, waker: &Waker) {
    if slot.as_ref().is_none_or(|old| !old.will_wake(waker)) {
        *slot = Some(waker.clone());
    }
}

impl Shared {
    pub(crate) fn schedule(&mut self, id: u32) {
        if let Some(stream) = self.streams.get_mut(&id)
            && !stream.control_queued
        {
            stream.control_queued = true;
            self.control.push_back(id);
        }
        self.wake_driver();
    }
    pub(crate) fn wake_driver(&self) {
        if let Some(waker) = &self.driver_waker {
            waker.wake_by_ref();
        }
    }
    pub(crate) fn enqueue(&mut self, header: Header, data: &[u8]) -> u64 {
        let wire = if data.is_empty() {
            Wire::Control {
                header: header.encode(),
                offset: 0,
            }
        } else {
            let mut wire = BytesMut::with_capacity(HEADER_LEN + data.len());
            header.append_to(&mut wire);
            wire.extend_from_slice(data);
            Wire::Data(wire.freeze())
        };
        // Sequence exhaustion requires over 2 EiB of headers on one connection.
        self.next_sequence = self.next_sequence.saturating_add(1);
        let sequence = self.next_sequence;
        self.queued_bytes += wire.as_slice().len();
        self.queue.push_back(QueuedFrame { wire, sequence });
        self.wake_driver();
        sequence
    }
    pub(crate) fn finish_if_closed(&mut self, id: u32) {
        if let Some(stream) = self.streams.get_mut(&id)
            && stream.counted
            && (stream.reset
                || (stream.local_fin && stream.remote_fin && stream.received_bytes == 0))
        {
            stream.counted = false;
            self.active = self.active.saturating_sub(1);
        }
    }
    pub(crate) fn remove(&mut self, id: u32) {
        if let Some(stream) = self.streams.remove(&id) {
            if stream.counted {
                self.active = self.active.saturating_sub(1);
            }
            if stream.announced
                && !stream.reset
                && !(stream.local_fin && stream.remote_fin)
                && self.terminal.is_none()
            {
                self.enqueue(
                    Header {
                        kind: FrameType::Data,
                        flags: RST,
                        stream_id: id,
                        value: 0,
                    },
                    &[],
                );
            }
        }
        self.flush_waiters.remove(&id);
        self.write_waiters.remove(&id);
        // Lazy deletion is amortized, but cannot grow with unused open/drop churn.
        if self.control.len() > self.active.saturating_add(64) {
            let streams = &self.streams;
            self.control.retain(|queued| streams.contains_key(queued));
        }
        self.wake_driver();
    }
    pub(crate) fn materialize_controls(&mut self) {
        while self.config.pending_budget.saturating_sub(self.queued_bytes) >= HEADER_LEN {
            let Some(id) = self.control.pop_front() else {
                break;
            };
            let Some(stream) = self.streams.get_mut(&id) else {
                continue;
            };
            stream.control_queued = false;
            if stream.reset {
                continue;
            }
            let flags = stream.pending_flags | if stream.pending_fin { FIN } else { 0 };
            let credit = stream.pending_credit;
            if flags == 0 && credit == 0 {
                continue;
            }
            stream.pending_flags = 0;
            stream.pending_credit = 0;
            stream.receive_credit += credit;
            let fin = stream.pending_fin;
            stream.pending_fin = false;
            if flags & SYN != 0 {
                stream.announced = true;
            }
            let kind = if fin && credit == 0 {
                FrameType::Data
            } else {
                FrameType::WindowUpdate
            };
            let sequence = self.enqueue(
                Header {
                    kind,
                    flags,
                    stream_id: id,
                    value: credit,
                },
                &[],
            );
            if let Some(stream) = self.streams.get_mut(&id) {
                stream.last_sequence = sequence;
                if fin {
                    stream.fin_sequence = Some(sequence);
                }
            }
            if let Some((target, _)) = self.flush_waiters.get_mut(&id)
                && *target == u64::MAX
            {
                *target = sequence;
            }
        }
    }
    fn terminate(&mut self, kind: io::ErrorKind, message: &'static str) {
        if self.terminal.is_some() {
            return;
        }
        self.terminal = Some((kind, message));
        self.queue.clear();
        self.control.clear();
        self.inbound.clear();
        self.queued_bytes = 0;
        for stream in self.streams.values_mut() {
            if let Some(waker) = stream.read_waker.take() {
                waker.wake();
            }
            if let Some(waker) = stream.write_waker.take() {
                waker.wake();
            }
        }
        for (_, waker) in self.write_waiters.drain() {
            waker.wake();
        }
        for (_, (_, waker)) in self.flush_waiters.drain() {
            waker.wake();
        }
    }
}

/// Owns the transport. Continuously poll `poll_next_inbound` to drive all streams.
///
/// Dropping a stream resets its unfinished remote half; dropping the connection
/// wakes every stream with a transport-closed error. No lock crosses transport I/O.
pub struct Connection<T> {
    io: T,
    shared: Arc<Mutex<Shared>>,
    mode: Mode,
    next_outbound: Option<u32>,
    last_inbound: u32,
    current: VecDeque<QueuedFrame>,
    written_sequence: u64,
    transport_dirty: bool,
    header: [u8; HEADER_LEN],
    header_bytes: usize,
    incoming: Option<Header>,
    body: BytesMut,
    ended: bool,
    goaway_queued: bool,
}

impl<T> Connection<T> {
    /// Wrap a Tokio transport with a Yamux-compatible connection.
    pub fn new(io: T, config: Config, mode: Mode) -> Self {
        let shared = Shared {
            streams: HashMap::new(),
            queue: VecDeque::new(),
            control: VecDeque::new(),
            inbound: VecDeque::new(),
            queued_bytes: 0,
            config,
            active: 0,
            next_sequence: 0,
            flushed_sequence: 0,
            driver_waker: None,
            write_waiters: HashMap::new(),
            flush_waiters: HashMap::new(),
            terminal: None,
            closing: false,
            remote_goaway: false,
        };
        Self {
            io,
            shared: Arc::new(Mutex::new(shared)),
            mode,
            next_outbound: Some(if mode == Mode::Client { 1 } else { 2 }),
            last_inbound: 0,
            current: VecDeque::with_capacity(MAX_WRITE_BATCH_FRAMES),
            written_sequence: 0,
            transport_dirty: false,
            header: [0; HEADER_LEN],
            header_bytes: 0,
            incoming: None,
            body: BytesMut::new(),
            ended: false,
            goaway_queued: false,
        }
    }
    /// Open a stream without waiting for a round trip. The SYN can share its first data frame.
    pub fn poll_new_outbound(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Stream, ConnectionError>> {
        let mut shared = lock(&self.shared);
        if !shared.config.valid() {
            return Poll::Ready(Err(ConnectionError::InvalidConfig));
        }
        if self.ended || shared.closing || shared.remote_goaway || shared.terminal.is_some() {
            return Poll::Ready(Err(ConnectionError::Closed));
        }
        if shared.active >= shared.config.stream_limit() {
            return Poll::Ready(Err(ConnectionError::TooManyStreams));
        }
        let Some(id) = self.next_outbound else {
            return Poll::Ready(Err(ConnectionError::Closed));
        };
        self.next_outbound = id.checked_add(2);
        shared.streams.insert(id, StreamState::new(true));
        shared.active += 1;
        remember(&mut shared.driver_waker, cx.waker());
        // Native streams are announced eagerly for open-then-read protocols.
        // The Futures adapter preserves lazy announcement until first I/O.
        if shared.config.eager_open {
            shared.schedule(id);
        }
        drop(shared);
        Poll::Ready(Ok(Stream::new(self.shared.clone(), id)))
    }
    fn fail(&mut self, error: ConnectionError) -> ConnectionError {
        self.ended = true;
        let kind = match &error {
            ConnectionError::Io(error) => error.kind(),
            _ => io::ErrorKind::InvalidData,
        };
        lock(&self.shared).terminate(kind, "multiplexer connection failed");
        error
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    /// Drive transport I/O and yield the next remotely initiated stream.
    /// After GOAWAY, already admitted streams continue until both FINs arrive.
    pub fn poll_next_inbound(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Stream, ConnectionError>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if !lock(&self.shared).config.valid() {
            let error = self.fail(ConnectionError::InvalidConfig);
            return Poll::Ready(Some(Err(error)));
        }
        if let Err(error) = self.pump(cx) {
            let error = self.fail(error);
            return Poll::Ready(Some(Err(error)));
        }
        let mut shared = lock(&self.shared);
        while let Some(id) = shared.inbound.pop_front() {
            if shared.streams.contains_key(&id) {
                drop(shared);
                return Poll::Ready(Some(Ok(Stream::new(self.shared.clone(), id))));
            }
        }
        if shared.terminal.is_some() {
            drop(shared);
            self.ended = true;
            return Poll::Ready(None);
        }
        if shared.remote_goaway
            && shared.active == 0
            && shared.queued_bytes == 0
            && shared.flushed_sequence >= self.written_sequence
        {
            shared.terminate(
                io::ErrorKind::BrokenPipe,
                "multiplexer drained after GOAWAY",
            );
            drop(shared);
            self.ended = true;
            return Poll::Ready(None);
        }
        drop(shared);
        Poll::Pending
    }

    /// Send normal GOAWAY, drain active streams, flush, then shut down the transport.
    pub fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectionError>> {
        if self.ended {
            return Poll::Ready(Ok(()));
        }
        {
            let mut shared = lock(&self.shared);
            if !shared.config.valid() {
                return Poll::Ready(Err(ConnectionError::InvalidConfig));
            }
            shared.closing = true;
            if !self.goaway_queued {
                shared.enqueue(
                    Header {
                        kind: FrameType::GoAway,
                        flags: 0,
                        stream_id: 0,
                        value: 0,
                    },
                    &[],
                );
                drop(shared);
                self.goaway_queued = true;
            }
        }
        if let Err(error) = self.pump(cx) {
            let error = self.fail(error);
            return Poll::Ready(Err(error));
        }
        {
            let shared = lock(&self.shared);
            if let Some((kind, message)) = shared.terminal {
                self.ended = true;
                return Poll::Ready(Err(ConnectionError::Io(io::Error::new(kind, message))));
            }
            if shared.active != 0
                || shared.queued_bytes != 0
                || shared.flushed_sequence < self.written_sequence
            {
                return Poll::Pending;
            }
        }
        match Pin::new(&mut self.io).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                self.ended = true;
                lock(&self.shared).terminate(io::ErrorKind::BrokenPipe, "multiplexer closed");
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                let error = self.fail(error.into());
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn pump(&mut self, cx: &mut Context<'_>) -> Result<(), ConnectionError> {
        {
            let mut shared = lock(&self.shared);
            remember(&mut shared.driver_waker, cx.waker());
            shared.materialize_controls();
        }
        self.write_frames(cx)?;
        let mut exhausted = true;
        for _ in 0..POLL_BUDGET {
            match self.read_frame(cx)? {
                Poll::Ready(Some((header, body))) => self.handle_frame(header, body)?,
                Poll::Ready(None) => {
                    lock(&self.shared).terminate(
                        io::ErrorKind::UnexpectedEof,
                        "multiplexer transport reached EOF",
                    );
                    exhausted = false;
                    break;
                }
                Poll::Pending => {
                    exhausted = false;
                    break;
                }
            }
        }
        {
            let mut shared = lock(&self.shared);
            shared.materialize_controls();
        }
        self.write_frames(cx)?;
        if exhausted {
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    fn write_frames(&mut self, cx: &mut Context<'_>) -> Result<(), ConnectionError> {
        let mut exhausted = true;
        for _ in 0..POLL_BUDGET {
            if self.current.is_empty() {
                let mut shared = lock(&self.shared);
                for _ in 0..MAX_WRITE_BATCH_FRAMES {
                    let Some(frame) = shared.queue.pop_front() else {
                        break;
                    };
                    self.current.push_back(frame);
                }
                drop(shared);
            }
            let Some(first) = self.current.front() else {
                exhausted = false;
                break;
            };
            // Stack-only descriptors borrow queued frames for this one poll.
            // No lock or borrowed I/O slice survives the transport call.
            let (result, offered) = if self.current.len() > 1 && self.io.is_write_vectored() {
                let mut vectors = [IoSlice::new(&[]); MAX_WRITE_BATCH_FRAMES];
                let mut count = 0;
                let mut total = 0;
                for (slot, frame) in vectors.iter_mut().zip(&self.current) {
                    let bytes = frame.wire.as_slice();
                    if count != 0 && total + bytes.len() > MAX_WRITE_BATCH_BYTES {
                        break;
                    }
                    *slot = IoSlice::new(bytes);
                    total += bytes.len();
                    count += 1;
                }
                if total == 0 {
                    return Err(ConnectionError::Protocol("empty queued frame batch"));
                }
                (
                    Pin::new(&mut self.io).poll_write_vectored(cx, &vectors[..count]),
                    total,
                )
            } else {
                let bytes = first.wire.as_slice();
                (Pin::new(&mut self.io).poll_write(cx, bytes), bytes.len())
            };
            match result {
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "multiplexer transport wrote zero bytes",
                        )
                        .into());
                    }
                    if n > offered {
                        return Err(ConnectionError::Protocol(
                            "transport over-reported write length",
                        ));
                    }
                    self.transport_dirty = true;
                    let mut remaining = n;
                    while remaining != 0 {
                        let Some(frame) = self.current.front_mut() else {
                            return Err(ConnectionError::Protocol(
                                "missing partially written frame",
                            ));
                        };
                        let used = remaining.min(frame.wire.as_slice().len());
                        if used == 0 {
                            return Err(ConnectionError::Protocol("empty partially written frame"));
                        }
                        frame.wire.advance(used);
                        remaining -= used;
                        if frame.wire.as_slice().is_empty() {
                            self.written_sequence = frame.sequence;
                            let _completed = self.current.pop_front();
                        }
                    }
                    let mut shared = lock(&self.shared);
                    shared.queued_bytes = shared.queued_bytes.saturating_sub(n);
                    for (_, waker) in shared.write_waiters.drain() {
                        waker.wake();
                    }
                    drop(shared);
                }
                Poll::Ready(Err(error)) => return Err(error.into()),
                Poll::Pending => {
                    exhausted = false;
                    break;
                }
            }
        }
        let needs_flush =
            self.transport_dirty || lock(&self.shared).flushed_sequence < self.written_sequence;
        if needs_flush {
            match Pin::new(&mut self.io).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    self.transport_dirty = false;
                    let mut shared = lock(&self.shared);
                    shared.flushed_sequence = self.written_sequence;
                    let sequence = shared.flushed_sequence;
                    shared.flush_waiters.retain(|_, (target, waker)| {
                        if *target <= sequence {
                            waker.wake_by_ref();
                            false
                        } else {
                            true
                        }
                    });
                }
                Poll::Ready(Err(error)) => return Err(error.into()),
                Poll::Pending => {}
            }
        }
        if exhausted {
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    fn read_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<Poll<Option<(Header, BytesMut)>>, ConnectionError> {
        while self.incoming.is_none() {
            let mut read = ReadBuf::new(&mut self.header[self.header_bytes..]);
            match Pin::new(&mut self.io).poll_read(cx, &mut read) {
                Poll::Pending => return Ok(Poll::Pending),
                Poll::Ready(Err(error)) => return Err(error.into()),
                Poll::Ready(Ok(())) => {
                    let n = read.filled().len();
                    if n == 0 {
                        return if self.header_bytes == 0 {
                            Ok(Poll::Ready(None))
                        } else {
                            Err(ConnectionError::Protocol("EOF inside frame header"))
                        };
                    }
                    self.header_bytes += n;
                    if self.header_bytes != HEADER_LEN {
                        continue;
                    }
                    let header = Header::decode(&self.header)?;
                    self.header_bytes = 0;
                    if header.kind != FrameType::Data {
                        return Ok(Poll::Ready(Some((header, BytesMut::new()))));
                    }
                    if header.flags & SYN != 0 {
                        let peer_odd = self.mode == Mode::Server;
                        if (header.stream_id % 2 == 1) != peer_odd
                            || header.flags & (ACK | RST) != 0
                        {
                            return Err(ConnectionError::Protocol("invalid peer SYN"));
                        }
                    }
                    if header.flags & RST != 0 && (header.flags != RST || header.value != 0) {
                        return Err(ConnectionError::Protocol("invalid reset"));
                    }
                    let length = header.value as usize;
                    if length > MAX_FRAME || length > WINDOW as usize {
                        return Err(ConnectionError::Protocol(
                            "data frame exceeds receive window",
                        ));
                    }
                    {
                        let shared = lock(&self.shared);
                        if let Some(stream) = shared.streams.get(&header.stream_id) {
                            if header.value > stream.receive_credit {
                                return Err(ConnectionError::Protocol(
                                    "peer exceeded receive credit",
                                ));
                            }
                            if stream.remote_fin && header.value != 0 {
                                return Err(ConnectionError::Protocol("data after FIN"));
                            }
                        }
                    }
                    // A delivered payload must not share a reusable driver's allocation.
                    self.body = BytesMut::with_capacity(length);
                    self.incoming = Some(header);
                }
            }
        }
        let Some(expected) = self.incoming.as_ref().map(|header| header.value as usize) else {
            return Err(ConnectionError::Protocol("missing data header"));
        };
        while self.body.len() < expected {
            // Tokio's cancel-safe read_buf advances only initialized bytes.
            // Limiting spare capacity prevents reading into the following frame.
            let remaining = expected - self.body.len();
            let mut destination = (&mut self.body).limit(remaining);
            let read = self.io.read_buf(&mut destination);
            let mut read = std::pin::pin!(read);
            match read.as_mut().poll(cx) {
                Poll::Pending => return Ok(Poll::Pending),
                Poll::Ready(Err(error)) => return Err(error.into()),
                Poll::Ready(Ok(0)) => {
                    return Err(ConnectionError::Protocol("EOF inside frame payload"));
                }
                Poll::Ready(Ok(_)) => {}
            }
        }
        let Some(header) = self.incoming.take() else {
            return Err(ConnectionError::Protocol("missing data header"));
        };
        Ok(Poll::Ready(Some((header, std::mem::take(&mut self.body)))))
    }

    fn handle_frame(&mut self, header: Header, body: BytesMut) -> Result<(), ConnectionError> {
        if header.flags & !(SYN | ACK | FIN | RST) != 0 {
            return Err(ConnectionError::Protocol("unknown flags"));
        }
        let mut shared = lock(&self.shared);
        match header.kind {
            FrameType::Ping => {
                if header.stream_id != 0 || (header.flags != SYN && header.flags != ACK) {
                    return Err(ConnectionError::Protocol("invalid ping"));
                }
                if header.flags == SYN {
                    if shared.queued_bytes
                        > shared.config.pending_budget.saturating_add(64 * HEADER_LEN)
                    {
                        return Err(ConnectionError::Protocol(
                            "control response budget exhausted",
                        ));
                    }
                    shared.enqueue(
                        Header {
                            kind: FrameType::Ping,
                            flags: ACK,
                            stream_id: 0,
                            value: header.value,
                        },
                        &[],
                    );
                }
                return Ok(());
            }
            FrameType::GoAway => {
                if header.stream_id != 0 || header.flags != 0 || header.value > 2 {
                    return Err(ConnectionError::Protocol("invalid GOAWAY"));
                }
                if header.value != 0 {
                    return Err(ConnectionError::Protocol("peer sent error GOAWAY"));
                }
                shared.remote_goaway = true;
                return Ok(());
            }
            FrameType::Data | FrameType::WindowUpdate => {}
        }
        let id = header.stream_id;
        if id == 0 {
            return Err(ConnectionError::Protocol("zero stream identifier"));
        }
        if header.flags & SYN != 0 {
            let peer_odd = self.mode == Mode::Server;
            if (id % 2 == 1) != peer_odd
                || shared.streams.contains_key(&id)
                || header.flags & (ACK | RST) != 0
            {
                return Err(ConnectionError::Protocol(
                    "invalid peer SYN or duplicate active stream ID",
                ));
            }
            self.last_inbound = self.last_inbound.max(id);
            if shared.closing
                || shared.remote_goaway
                || shared.active >= shared.config.stream_limit()
                || shared.inbound.len() >= shared.config.stream_limit()
            {
                if shared.queued_bytes
                    > shared.config.pending_budget.saturating_add(64 * HEADER_LEN)
                {
                    return Err(ConnectionError::Protocol("reset response budget exhausted"));
                }
                shared.enqueue(
                    Header {
                        kind: FrameType::Data,
                        flags: RST,
                        stream_id: id,
                        value: 0,
                    },
                    &[],
                );
                return Ok(());
            }
            shared.streams.insert(id, StreamState::new(false));
            shared.active += 1;
            shared.inbound.push_back(id);
            shared.schedule(id);
        }
        let max_send_window = shared.config.max_send_window;
        let Some(stream) = shared.streams.get_mut(&id) else {
            let outbound_used = (id % 2 == 1) == (self.mode == Mode::Client)
                && self.next_outbound.is_none_or(|next| id < next);
            let inbound_used =
                (id % 2 == 1) == (self.mode == Mode::Server) && id <= self.last_inbound;
            if inbound_used || outbound_used {
                return Ok(());
            }
            return Err(ConnectionError::Protocol("frame for unknown stream"));
        };
        if header.flags & RST != 0 {
            if header.flags != RST || header.value != 0 {
                return Err(ConnectionError::Protocol("invalid reset"));
            }
            stream.reset = true;
            stream.received.clear();
            stream.received_bytes = 0;
            if let Some(waker) = stream.read_waker.take() {
                waker.wake();
            }
            if let Some(waker) = stream.write_waker.take() {
                waker.wake();
            }
            shared.finish_if_closed(id);
            if let Some((_, waker)) = shared.flush_waiters.remove(&id) {
                waker.wake();
            }
            return Ok(());
        }
        if stream.reset {
            return Ok(());
        }
        if header.flags & ACK != 0 {
            if stream.acked {
                return Err(ConnectionError::Protocol("duplicate or unsolicited ACK"));
            }
            stream.acked = true;
        }
        if header.kind == FrameType::WindowUpdate {
            let Some(credit) = stream.send_credit.checked_add(header.value) else {
                return Err(ConnectionError::Protocol("send credit overflow"));
            };
            if credit as usize > max_send_window {
                return Err(ConnectionError::Protocol(
                    "send credit exceeds configured maximum",
                ));
            }
            stream.send_credit = credit;
            if let Some(waker) = stream.write_waker.take() {
                waker.wake();
            }
        } else if !body.is_empty() {
            if stream.remote_fin {
                return Err(ConnectionError::Protocol("data after FIN"));
            }
            if header.value > stream.receive_credit {
                return Err(ConnectionError::Protocol("peer exceeded receive credit"));
            }
            stream.receive_credit -= header.value;
            stream.received_bytes += body.len();
            // Coalesce tiny payloads so frame metadata cannot dominate receive credit.
            // Large frames stay zero-copy between the driver and the stream queue.
            if body.len() <= SMALL_RECEIVE_FRAME
                && let Some(tail) = stream.received.back_mut()
                && tail.len() + body.len() <= RECEIVE_COALESCE_LIMIT
            {
                tail.extend_from_slice(&body);
            } else {
                stream.received.push_back(body);
            }
            if let Some(waker) = stream.read_waker.take() {
                waker.wake();
            }
        }
        if header.flags & FIN != 0 {
            if stream.remote_fin {
                return Err(ConnectionError::Protocol("duplicate FIN"));
            }
            stream.remote_fin = true;
            if let Some(waker) = stream.read_waker.take() {
                waker.wake();
            }
        }
        shared.finish_if_closed(id);
        drop(shared);
        Ok(())
    }
}
impl<T> Drop for Connection<T> {
    fn drop(&mut self) {
        lock(&self.shared).terminate(io::ErrorKind::BrokenPipe, "multiplexer driver dropped");
    }
}

pub const fn write_frame_limit() -> usize {
    WRITE_FRAME
}

#[cfg(test)]
mod receive_tests;
