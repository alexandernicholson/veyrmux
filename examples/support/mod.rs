//! Identical, byte-validated streaming echo workload for both multiplexers.
//! Connection setup and a 4 KiB warmup exchange are outside the reported time.
use std::{
    future::poll_fn,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{Barrier, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

pub const WINDOW: usize = 256 * 1024;
pub const STREAM_CAP: usize = 64;
pub const CHUNK: usize = 16 * 1024;
pub const INTERACTIVE_PAYLOAD: usize = 64;
// Exact distributions are bounded to 16 MiB of Duration samples across workers.
pub const MAX_LATENCY_SAMPLES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    Uniform,
    Mixed,
}

impl Scenario {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value {
            "uniform" => Ok(Self::Uniform),
            "mixed" => Ok(Self::Mixed),
            _ => Err(io::Error::other("scenario must be uniform or mixed")),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Engine {
    Veyrmux,
    Yamux,
}

impl Engine {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value {
            "veyrmux" => Ok(Self::Veyrmux),
            "yamux" => Ok(Self::Yamux),
            _ => Err(io::Error::other("engine must be veyrmux or yamux")),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Veyrmux => "veyrmux",
            Self::Yamux => "yamux",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Transport {
    Duplex,
    Tcp,
}

impl Transport {
    pub fn parse(value: &str) -> io::Result<Self> {
        match value {
            "duplex" => Ok(Self::Duplex),
            "tcp" => Ok(Self::Tcp),
            _ => Err(io::Error::other("transport must be duplex or tcp")),
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Duplex => "duplex",
            Self::Tcp => "tcp",
        }
    }
}

#[derive(Clone, Copy)]
pub struct Workload {
    pub engine: Engine,
    pub transport: Transport,
    pub payload: usize,
    pub streams: usize,
    pub rounds: usize,
    pub data_chunk: usize,
    pub caller_chunk: usize,
    pub scenario: Scenario,
}

impl Workload {
    pub fn validate(self) -> io::Result<()> {
        if self.streams == 0 || self.streams > STREAM_CAP || self.rounds == 0 {
            return Err(io::Error::other(
                "streams must be 1..=64 and rounds must be positive",
            ));
        }
        if self.payload == 0 {
            return Err(io::Error::other("payload must be positive"));
        }
        if !(1..=WINDOW).contains(&self.data_chunk) || !(1..=WINDOW).contains(&self.caller_chunk) {
            return Err(io::Error::other(
                "DATA and caller chunks must be 1..=262144",
            ));
        }
        if self.scenario == Scenario::Mixed && self.streams < 2 {
            return Err(io::Error::other(
                "mixed requires one bulk and at least one interactive stream",
            ));
        }
        if self.rounds > MAX_LATENCY_SAMPLES / self.streams {
            return Err(io::Error::other(
                "rounds exceed the per-worker exact latency sample budget",
            ));
        }
        let requests = self
            .streams
            .checked_mul(self.rounds)
            .ok_or_else(|| io::Error::other("request count overflows usize"))?;
        checked_payload_bytes(self.payload, requests)?;
        Ok(())
    }
}

fn checked_payload_bytes(payload: usize, requests: usize) -> io::Result<u64> {
    u64::try_from(payload)
        .ok()
        .and_then(|bytes| {
            u64::try_from(requests)
                .ok()
                .and_then(|count| bytes.checked_mul(count))
        })
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or_else(|| io::Error::other("payload byte count overflows u64"))
}

pub struct Distribution {
    pub payload_bytes: u64,
    pub latencies: Vec<Duration>,
}

impl Distribution {
    /// Nearest-rank percentile of successful open-to-echo-EOF exchanges.
    pub fn percentile_seconds(&self, percentile: usize) -> f64 {
        let rank = (self.latencies.len() as u128 * percentile as u128).div_ceil(100);
        self.latencies[(rank as usize).saturating_sub(1)].as_secs_f64()
    }
}

pub struct Measurement {
    pub elapsed: Duration,
    pub payload_bytes: u64,
    pub requests: usize,
    pub uniform: Option<Distribution>,
    pub interactive: Option<Distribution>,
    pub bulk: Option<Distribution>,
    pub interactive_requests_completed_during_bulk: usize,
    pub bulk_elapsed: Option<Duration>,
}

enum Stream {
    Native(veyrmux::Stream),
    Stock(Compat<yamux::Stream>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Stock(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Stock(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_flush(cx),
            Self::Stock(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Stock(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

enum Connection<I> {
    Native(veyrmux::Connection<I>),
    Stock(yamux::Connection<Compat<I>>),
}

impl<I: AsyncRead + AsyncWrite + Unpin> Connection<I> {
    fn new(io: I, engine: Engine, client: bool, data_chunk: usize) -> Self {
        match engine {
            Engine::Veyrmux => {
                let mut config = veyrmux::Config::default();
                config.set_max_num_streams(STREAM_CAP);
                config.set_max_connection_receive_window(Some(STREAM_CAP * WINDOW));
                config.set_split_send_size(data_chunk);
                Self::Native(veyrmux::Connection::new(
                    io,
                    config,
                    if client {
                        veyrmux::Mode::Client
                    } else {
                        veyrmux::Mode::Server
                    },
                ))
            }
            Engine::Yamux => {
                let mut config = yamux::Config::default();
                config.set_max_num_streams(STREAM_CAP);
                // Stock reserves the default credit for *every configured stream*.
                // No excess connection budget means auto-tuning cannot grow a window.
                config.set_max_connection_receive_window(Some(STREAM_CAP * WINDOW));
                config.set_split_send_size(data_chunk);
                Self::Stock(yamux::Connection::new(
                    io.compat(),
                    config,
                    if client {
                        yamux::Mode::Client
                    } else {
                        yamux::Mode::Server
                    },
                ))
            }
        }
    }

    fn poll_inbound(&mut self, cx: &mut Context<'_>) -> Poll<Option<io::Result<Stream>>> {
        match self {
            Self::Native(connection) => connection.poll_next_inbound(cx).map(|item| {
                item.map(|result| result.map(Stream::Native).map_err(io::Error::other))
            }),
            Self::Stock(connection) => connection.poll_next_inbound(cx).map(|item| {
                item.map(|result| {
                    result
                        .map(|stream| Stream::Stock(stream.compat()))
                        .map_err(io::Error::other)
                })
            }),
        }
    }

    fn poll_outbound(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Stream>> {
        match self {
            Self::Native(connection) => connection
                .poll_new_outbound(cx)
                .map(|result| result.map(Stream::Native).map_err(io::Error::other)),
            Self::Stock(connection) => connection.poll_new_outbound(cx).map(|result| {
                result
                    .map(|stream| Stream::Stock(stream.compat()))
                    .map_err(io::Error::other)
            }),
        }
    }
}

type OpenReply = oneshot::Sender<io::Result<Stream>>;

#[derive(Clone)]
struct Opener(mpsc::UnboundedSender<OpenReply>);

impl Opener {
    async fn open(&self) -> io::Result<Stream> {
        let (sender, receiver) = oneshot::channel();
        self.0
            .send(sender)
            .map_err(|_| io::Error::other("connection driver stopped"))?;
        receiver.await.map_err(io::Error::other)?
    }
}

// Aborting on drop also covers timeout/error cancellation; no detached driver survives a run.
struct Driver(JoinHandle<io::Result<()>>);

impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn drive<I>(
    mut connection: Connection<I>,
) -> (Opener, mpsc::UnboundedReceiver<io::Result<Stream>>, Driver)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (commands, mut requests) = mpsc::unbounded_channel::<OpenReply>();
    let (inbound, receiver) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut pending = None;
        loop {
            // Polling only outbound opens does not drive either engine. Always pump
            // inbound I/O in the same poll, including while an open is backpressured.
            let next = poll_fn(|cx| {
                if pending.is_none()
                    && let Poll::Ready(command) = requests.poll_recv(cx)
                {
                    match command {
                        Some(reply) => pending = Some(reply),
                        None => return Poll::Ready(Ok(None)),
                    }
                }
                match connection.poll_inbound(cx) {
                    Poll::Ready(Some(Ok(stream))) => return Poll::Ready(Ok(Some(stream))),
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                    Poll::Ready(None) => return Poll::Ready(Ok(None)),
                    Poll::Pending => {}
                }
                if pending.is_some()
                    && let Poll::Ready(result) = connection.poll_outbound(cx)
                {
                    if let Some(reply) = pending.take() {
                        let _undelivered = reply.send(result);
                    }
                    // Another queued command may already be ready.
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            })
            .await;
            match next {
                Ok(Some(stream)) => {
                    if inbound.send(Ok(stream)).is_err() {
                        return Err(io::Error::other("unexpected inbound stream"));
                    }
                }
                Ok(None) => return Ok(()),
                Err(error) => {
                    let _undelivered = inbound.send(Err(io::Error::other(error.to_string())));
                    return Err(error);
                }
            }
        }
    });
    (Opener(commands), receiver, Driver(task))
}

async fn echo(mut stream: Stream, mut scratch: Vec<u8>) -> io::Result<Vec<u8>> {
    loop {
        let count = stream.read(&mut scratch).await?;
        if count == 0 {
            stream.shutdown().await?;
            return Ok(scratch);
        }
        stream.write_all(&scratch[..count]).await?;
    }
}

async fn echo_uniform(mut stream: Stream, caller_chunk: usize) -> io::Result<()> {
    // One dynamic allocation per echo worker, reused for every chunk in its stream.
    // Keep scratch owned by the worker and drop it at completion, not at JoinSet reap.
    let mut scratch = vec![0_u8; caller_chunk];
    loop {
        let count = stream.read(&mut scratch).await?;
        if count == 0 {
            return stream.shutdown().await;
        }
        stream.write_all(&scratch[..count]).await?;
    }
}

async fn exchange(
    opener: &Opener,
    payload: usize,
    pattern: &[u8],
    scratch: &mut [u8],
) -> io::Result<Duration> {
    let started = Instant::now();
    let stream = opener.open().await?;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let write = async {
        let mut remaining = payload;
        while remaining != 0 {
            let count = remaining.min(pattern.len());
            writer.write_all(&pattern[..count]).await?;
            remaining -= count;
        }
        writer.shutdown().await
    };
    let read = async {
        let mut remaining = payload;
        while remaining != 0 {
            let count = remaining.min(scratch.len());
            reader.read_exact(&mut scratch[..count]).await?;
            if scratch[..count] != pattern[..count] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "echo payload differs from deterministic pattern",
                ));
            }
            remaining -= count;
        }
        if reader.read(&mut scratch[..1]).await? != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "echo has trailing bytes instead of EOF",
            ));
        }
        Ok(())
    };
    // Concurrent reads and writes are essential for payloads larger than a window.
    tokio::try_join!(write, read)?;
    Ok(started.elapsed())
}

pub async fn measure(workload: Workload) -> io::Result<Measurement> {
    workload.validate()?;
    tokio::time::timeout(Duration::from_secs(30), async {
        match workload.transport {
            Transport::Duplex => {
                let (client, server) = tokio::io::duplex(64 * 1024);
                run_pair(client, server, workload).await
            }
            Transport::Tcp => {
                let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
                let connect = TcpStream::connect(listener.local_addr()?);
                let (client, (server, _)) = tokio::try_join!(connect, listener.accept())?;
                client.set_nodelay(true)?;
                server.set_nodelay(true)?;
                run_pair(client, server, workload).await
            }
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "benchmark exceeded global 30 second timeout",
        )
    })?
}

async fn run_pair<I>(client: I, server: I, workload: Workload) -> io::Result<Measurement>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (opener, _client_inbound, _client_driver) = drive(Connection::new(
        client,
        workload.engine,
        true,
        workload.data_chunk,
    ));
    let (_server_opener, mut incoming, _server_driver) = drive(Connection::new(
        server,
        workload.engine,
        false,
        workload.data_chunk,
    ));
    // Both JoinSets abort their workers on error/cancel; Driver aborts the pumps.
    let (stop_server, mut stopped) = oneshot::channel();
    let mut servers = JoinSet::new();
    if workload.scenario == Scenario::Uniform {
        let requests = workload
            .streams
            .checked_mul(workload.rounds)
            .ok_or_else(|| io::Error::other("request count overflows usize"))?;
        servers.spawn(async move {
            let mut echoes = JoinSet::new();
            // Match the uniform receive/spawn/lazy-reap lifecycle, including warmup.
            for _ in 0..=requests {
                let stream = incoming
                    .recv()
                    .await
                    .ok_or_else(|| io::Error::other("server connection ended early"))??;
                echoes.spawn(echo_uniform(stream, workload.caller_chunk));
                while let Some(result) = echoes.try_join_next() {
                    result.map_err(io::Error::other)??;
                }
            }
            while let Some(result) = echoes.join_next().await {
                result.map_err(io::Error::other)??;
            }
            Ok::<(), io::Error>(())
        });
    } else {
        // Mixed work has no predetermined request count; retain its existing dispatcher.
        let mut buffers: Vec<_> = (0..workload.streams)
            .map(|_| vec![0_u8; workload.caller_chunk])
            .collect();
        servers.spawn(async move {
            let mut echoes = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    result = echoes.join_next(), if !echoes.is_empty() => {
                        let buffer = result.ok_or_else(|| io::Error::other("echo worker disappeared"))?
                            .map_err(io::Error::other)??;
                        buffers.push(buffer);
                    }
                    _ = &mut stopped => break,
                    stream = incoming.recv(), if !buffers.is_empty() => {
                        let stream = stream
                            .ok_or_else(|| io::Error::other("server connection ended early"))??;
                        let scratch = buffers.pop()
                            .ok_or_else(|| io::Error::other("echo scratch pool exhausted"))?;
                        echoes.spawn(echo(stream, scratch));
                    }
                }
            }
            while let Some(result) = echoes.join_next().await {
                result.map_err(io::Error::other)??;
            }
            Ok::<(), io::Error>(())
        });
    }

    let warm_pattern = vec![0x5a; workload.caller_chunk];
    let mut warm_scratch = vec![0_u8; workload.caller_chunk];
    exchange(&opener, 4096, &warm_pattern, &mut warm_scratch).await?;

    let start_gate = std::sync::Arc::new(Barrier::new(workload.streams + 1));
    let round_gate = std::sync::Arc::new(Barrier::new(workload.streams));
    let (bulk_done, _) = tokio::sync::watch::channel(false);
    let mut clients = JoinSet::new();
    for worker in 0..workload.streams {
        let opener = opener.clone();
        let start_gate = start_gate.clone();
        let round_gate = round_gate.clone();
        let bulk_done = bulk_done.clone();
        let completion = bulk_done.subscribe();
        let interactive = workload.scenario == Scenario::Mixed && worker != 0;
        let payload = if interactive {
            INTERACTIVE_PAYLOAD
        } else {
            workload.payload
        };
        // One pattern/scratch pair per client, reused across all fresh-stream requests.
        let pattern: Vec<u8> = (0..workload.caller_chunk)
            .map(|index| (index as u8).wrapping_mul(31).wrapping_add(worker as u8))
            .collect();
        let mut scratch = vec![0_u8; workload.caller_chunk];
        let sample_limit = MAX_LATENCY_SAMPLES / workload.streams;
        let mut latencies = Vec::with_capacity(if interactive {
            sample_limit
        } else {
            workload.rounds
        });
        let mut completed_during_bulk = 0_usize;
        clients.spawn(async move {
            start_gate.wait().await;
            let started = Instant::now();
            loop {
                if latencies.len() == sample_limit {
                    return Err(io::Error::other("exact latency sample budget exhausted"));
                }
                latencies.push(exchange(&opener, payload, &pattern, &mut scratch).await?);
                if workload.scenario == Scenario::Uniform {
                    round_gate.wait().await;
                    if latencies.len() == workload.rounds {
                        break;
                    }
                } else if interactive {
                    // Never cancel an in-flight request when the bulk worker finishes.
                    if *completion.borrow() {
                        break;
                    }
                    completed_during_bulk += 1;
                } else if latencies.len() == workload.rounds {
                    bulk_done.send_replace(true);
                    break;
                }
            }
            Ok::<_, io::Error>((
                interactive,
                completed_during_bulk,
                started.elapsed(),
                Distribution {
                    payload_bytes: checked_payload_bytes(payload, latencies.len())?,
                    latencies,
                },
            ))
        });
    }
    let mut result = Measurement {
        elapsed: Duration::ZERO,
        payload_bytes: 0,
        requests: 0,
        uniform: None,
        interactive: None,
        bulk: None,
        bulk_elapsed: None,
        interactive_requests_completed_during_bulk: 0,
    };
    let started = Instant::now();
    start_gate.wait().await;
    while let Some(client) = clients.join_next().await {
        let (interactive, overlap_count, elapsed, distribution) =
            client.map_err(io::Error::other)??;
        result.interactive_requests_completed_during_bulk += overlap_count;
        result.payload_bytes = result
            .payload_bytes
            .checked_add(distribution.payload_bytes)
            .ok_or_else(|| io::Error::other("actual payload byte count overflows u64"))?;
        result.requests = result
            .requests
            .checked_add(distribution.latencies.len())
            .ok_or_else(|| io::Error::other("actual request count overflows usize"))?;
        let class = if workload.scenario == Scenario::Uniform {
            &mut result.uniform
        } else if interactive {
            &mut result.interactive
        } else {
            result.bulk_elapsed = Some(elapsed);
            &mut result.bulk
        };
        if let Some(class) = class {
            class.payload_bytes = class
                .payload_bytes
                .checked_add(distribution.payload_bytes)
                .ok_or_else(|| io::Error::other("class byte count overflows u64"))?;
            class.latencies.extend(distribution.latencies);
        } else {
            *class = Some(distribution);
        }
    }
    let _undelivered = stop_server.send(());
    while let Some(server) = servers.join_next().await {
        server.map_err(io::Error::other)??;
    }
    result.elapsed = started.elapsed();
    for class in [
        &mut result.uniform,
        &mut result.interactive,
        &mut result.bulk,
    ]
    .into_iter()
    .flatten()
    {
        class.latencies.sort_unstable();
    }
    Ok(result)
}
