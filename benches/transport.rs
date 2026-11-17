#![forbid(unsafe_code)]
//! Criterion reports each round's bidirectional, byte-validated payload throughput.
//! A runtime is reused across samples. Each sample makes fresh connections, performs
//! an untimed warmup, then returns only the timed workload duration to Criterion.
//! Stream creation/FIN, validation and task coordination are included for both engines.
#[path = "../examples/support/mod.rs"]
pub mod support;

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use support::{CHUNK, Engine, Scenario, Transport, Workload};

fn transport(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create benchmark runtime");
    for transport in [Transport::Duplex, Transport::Tcp] {
        let mut group = c.benchmark_group(format!("transport/{}", transport.name()));
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(3));
        for (label, payload, streams, data_chunk, caller_chunk) in [
            ("latency-64B-1stream", 64, 1, CHUNK, CHUNK),
            ("bulk-1MiB-1stream", 1024 * 1024, 1, CHUNK, CHUNK),
            ("bulk-1MiB-8streams", 1024 * 1024, 8, CHUNK, CHUNK),
            ("bulk-1MiB-8streams-D32K-C32K", 1024 * 1024, 8, 32768, 32768),
            ("bulk-1MiB-8streams-D64K-C64K", 1024 * 1024, 8, 65536, 65536),
            (
                "bulk-1MiB-8streams-D128K-C128K",
                1024 * 1024,
                8,
                131072,
                131072,
            ),
            ("bulk-1MiB-8streams-D16K-C64K", 1024 * 1024, 8, CHUNK, 65536),
            ("bulk-1MiB-8streams-D64K-C16K", 1024 * 1024, 8, 65536, CHUNK),
        ] {
            group.throughput(Throughput::Bytes((2 * payload * streams) as u64));
            for engine in [Engine::Veyrmux, Engine::Yamux] {
                group.bench_function(BenchmarkId::new(label, engine.name()), |b| {
                    b.iter_custom(|rounds| {
                        let measurement = runtime
                            .block_on(support::measure(Workload {
                                engine,
                                transport,
                                payload,
                                streams,
                                data_chunk,
                                caller_chunk,
                                scenario: Scenario::Uniform,
                                rounds: usize::try_from(rounds)
                                    .expect("Criterion iteration count fits usize"),
                            }))
                            .expect(
                                "byte-validated transport workload must succeed within 30 seconds",
                            );
                        criterion::black_box(measurement.elapsed)
                    });
                });
            }
        }
        group.finish();
    }
}

criterion_group!(benches, transport);
criterion_main!(benches);
