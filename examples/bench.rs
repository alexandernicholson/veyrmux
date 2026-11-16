#![forbid(unsafe_code)]
//! Run: cargo run --release --example bench -- veyrmux 1048576 8 16 duplex
mod support;

use std::io;
use support::{Engine, Transport, Workload, measure};

const USAGE: &str = "Usage: bench <veyrmux|yamux> <payload-bytes> <streams:1..64> <rounds> <duplex|tcp>\n\
Payload bytes are per stream, per direction. Each round opens fresh streams.\n\
Both engines use fixed 256 KiB receive windows, a 64-stream cap, and 16 KiB DATA chunks.\n\
Duplex has a bounded 64 KiB transport; TCP uses loopback with TCP_NODELAY on both endpoints.\n\
A 4 KiB echo warmup and transport/connection setup are excluded from elapsed time.\n\
Latency samples include stream open, concurrent request/echo, byte validation and echo EOF.\n\
Percentiles are nearest-rank across every successful exchange, not round averages.\n\
The whole run, including startup, must finish within 30 seconds.";

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() == 1 && matches!(args[0].as_str(), "--help" | "-h") {
        println!("{USAGE}");
        return Ok(());
    }
    if args.len() != 5 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE));
    }
    let workload = Workload {
        engine: Engine::parse(&args[0])?,
        payload: args[1]
            .parse()
            .map_err(|_| io::Error::other("payload-bytes must be a nonnegative integer"))?,
        streams: args[2]
            .parse()
            .map_err(|_| io::Error::other("streams must be an integer in 1..=64"))?,
        rounds: args[3]
            .parse()
            .map_err(|_| io::Error::other("rounds must be a positive integer"))?,
        transport: Transport::parse(&args[4])?,
    };
    let result = measure(workload).await?;
    let elapsed = result.elapsed.as_secs_f64();
    println!(
        concat!(
            "{{\"engine\":\"{}\",\"transport\":\"{}\",\"payload_bytes_per_stream_per_direction\":{},",
            "\"streams\":{},\"rounds\":{},\"receive_window_bytes\":{},\"stream_cap\":{},\"data_chunk_bytes\":{},",
            "\"elapsed_secs\":{:.9},\"payload_bytes\":{},\"payload_bytes_per_sec\":{:.3},\"requests\":{},\"requests_per_sec\":{:.3},",
            "\"latency_distribution\":\"all successful stream-open-to-byte-validated-echo-EOF exchanges; nearest-rank; seconds\",",
            "\"latency_samples\":{},\"latency_p50_secs\":{:.9},\"latency_p90_secs\":{:.9},\"latency_p99_secs\":{:.9},",
            "\"byte_accounting\":\"request plus echo payload; excludes frame headers\",",
            "\"runtime\":\"tokio-current-thread; both endpoints\",\"startup_included\":false,\"warmup_bytes\":4096}}"
        ),
        workload.engine.name(),
        workload.transport.name(),
        workload.payload,
        workload.streams,
        workload.rounds,
        support::WINDOW,
        support::STREAM_CAP,
        support::CHUNK,
        elapsed,
        result.payload_bytes,
        result.payload_bytes as f64 / elapsed,
        result.requests,
        result.requests as f64 / elapsed,
        result.latencies.len(),
        result.percentile_seconds(50),
        result.percentile_seconds(90),
        result.percentile_seconds(99),
    );
    Ok(())
}
