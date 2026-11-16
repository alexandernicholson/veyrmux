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
}

pub struct Measurement {
    pub elapsed: Duration,
    pub payload_bytes: u64,
    pub requests: usize,
    pub latencies: Vec<Duration>,
}

impl Measurement {
    /// Nearest-rank percentile of successful open-to-echo-EOF exchanges.
    pub fn percentile_seconds(&self, percentile: usize) -> f64 {
        let rank = (self.latencies.len() as u128 * percentile as u128).div_ceil(100);
        self.latencies[(rank as usize).saturating_sub(1)].as_secs_f64()
    }
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
    fn new(io: I, engine: Engine, client: bool) -> Self {
        match engine {
            Engine::Veyrmux => {
                let mut config = veyrmux::Config::default();
                config.set_max_num_streams(STREAM_CAP);
                config.set_max_connection_receive_window(Some(STREAM_CAP * WINDOW));
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
                config.set_split_send_size(CHUNK);
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

async fn echo(mut stream: Stream) -> io::Result<()> {
    let mut scratch = [0_u8; CHUNK];
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
    if workload.streams == 0 || workload.streams > STREAM_CAP || workload.rounds == 0 {
        return Err(io::Error::other(
            "streams must be 1..=64 and rounds must be positive",
        ));
    }
    let requests = workload
        .streams
        .checked_mul(workload.rounds)
        .ok_or_else(|| io::Error::other("request count overflows usize"))?;
    let payload_bytes = (workload.payload as u64)
        .checked_mul(requests as u64)
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or_else(|| io::Error::other("payload byte count overflows u64"))?;
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut result = match workload.transport {
            Transport::Duplex => {
                let (client, server) = tokio::io::duplex(64 * 1024);
                run_pair(client, server, workload, requests).await?
            }
            Transport::Tcp => {
                let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
                let connect = TcpStream::connect(listener.local_addr()?);
                let (client, (server, _)) = tokio::try_join!(connect, listener.accept())?;
                client.set_nodelay(true)?;
                server.set_nodelay(true)?;
                run_pair(client, server, workload, requests).await?
            }
        };
        result.payload_bytes = payload_bytes;
        Ok(result)
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "benchmark exceeded global 30 second timeout",
        )
    })?
}

async fn run_pair<I>(
    client: I,
    server: I,
    workload: Workload,
    requests: usize,
) -> io::Result<Measurement>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (opener, _client_inbound, _client_driver) =
        drive(Connection::new(client, workload.engine, true));
    let (_server_opener, mut incoming, _server_driver) =
        drive(Connection::new(server, workload.engine, false));
    // JoinSet aborts all echo workers if an error or timeout cancels this run.
    let mut servers = JoinSet::new();
    servers.spawn(async move {
        let mut echoes = JoinSet::new();
        for _ in 0..=requests {
            let stream = incoming
                .recv()
                .await
                .ok_or_else(|| io::Error::other("server connection ended early"))??;
            echoes.spawn(echo(stream));
            while let Some(result) = echoes.try_join_next() {
                result.map_err(io::Error::other)??;
            }
        }
        while let Some(result) = echoes.join_next().await {
            result.map_err(io::Error::other)??;
        }
        Ok::<(), io::Error>(())
    });

    let warm_pattern = [0x5a; CHUNK];
    let mut warm_scratch = [0_u8; CHUNK];
    exchange(&opener, 4096, &warm_pattern, &mut warm_scratch).await?;

    let start_gate = std::sync::Arc::new(Barrier::new(workload.streams + 1));
    let round_gate = std::sync::Arc::new(Barrier::new(workload.streams));
    let mut clients = JoinSet::new();
    for worker in 0..workload.streams {
        let opener = opener.clone();
        let start_gate = start_gate.clone();
        let round_gate = round_gate.clone();
        // One prefilled pattern and fixed scratch per worker, reused across rounds.
        let pattern: Vec<u8> = (0..CHUNK)
            .map(|index| (index as u8).wrapping_mul(31).wrapping_add(worker as u8))
            .collect();
        let mut scratch = vec![0_u8; CHUNK];
        let mut latencies = Vec::with_capacity(workload.rounds);
        clients.spawn(async move {
            start_gate.wait().await;
            for _ in 0..workload.rounds {
                latencies.push(exchange(&opener, workload.payload, &pattern, &mut scratch).await?);
                round_gate.wait().await;
            }
            Ok::<_, io::Error>(latencies)
        });
    }
    let mut latencies = Vec::with_capacity(requests);
    let started = Instant::now();
    start_gate.wait().await;
    while let Some(result) = clients.join_next().await {
        latencies.extend(result.map_err(io::Error::other)??);
    }
    while let Some(result) = servers.join_next().await {
        result.map_err(io::Error::other)??;
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    Ok(Measurement {
        elapsed,
        payload_bytes: 0,
        requests,
        latencies,
    })
}
