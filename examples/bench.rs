#![forbid(unsafe_code)]
//! Run: cargo run --release --example bench -- veyrmux 1048576 8 16 duplex
mod support;

use std::io;
use support::{Distribution, Engine, Scenario, Transport, Workload, measure};

const USAGE: &str = "Usage: bench <veyrmux|yamux> <payload-bytes> <streams:1..64> <rounds> <duplex|tcp> [--data-chunk BYTES] [--caller-chunk BYTES] [--scenario uniform|mixed]\n\
Both chunks default to 16384 and must be 1..=262144; caller buffers are capped at the fixed 256 KiB receive window.\n\
Payload and rounds must be positive; rounds <= floor(1048576 / streams), bounding exact latency storage.\n\
Uniform: payload bytes per stream/direction; each round opens fresh streams with a round barrier.\n\
Mixed: streams >=2; one bulk client does rounds at payload bytes, all other clients repeatedly exchange 64 bytes until bulk completes, finishing in-flight exchanges.\n\
Mixed aborts rather than truncates if any worker exceeds floor(1048576 / streams) samples.\n\
Both engines use the requested DATA split, a fixed 256 KiB receive window and a 64-stream cap.\n\
Duplex has a bounded 64 KiB transport; TCP uses loopback with TCP_NODELAY on both endpoints.\n\
A 4 KiB echo warmup and transport/connection setup are excluded from elapsed time.\n\
Latency includes stream open, concurrent request/echo, byte validation and echo EOF; nearest-rank per class.\n\
Bulk goodput is request bytes (one direction) / bulk worker elapsed, excluding interactive work and drain.\n\
Both endpoints run on one current-thread runtime; these are local contention controls, not service latency.\n\
The whole run, including startup, must finish within 30 seconds.";

fn parse_workload(args: &[String]) -> io::Result<Workload> {
    if args.len() < 5 || !(args.len() - 5).is_multiple_of(2) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE));
    }
    let number = |value: &str| {
        value.parse::<usize>().map_err(|_| {
            io::Error::other("numeric arguments must be nonnegative integers fitting usize")
        })
    };
    let mut workload = Workload {
        engine: Engine::parse(&args[0])?,
        payload: number(&args[1])?,
        streams: number(&args[2])?,
        rounds: number(&args[3])?,
        transport: Transport::parse(&args[4])?,
        data_chunk: support::CHUNK,
        caller_chunk: support::CHUNK,
        scenario: Scenario::Uniform,
    };
    let mut seen = [false; 3];
    for flag in args[5..].as_chunks::<2>().0 {
        let index = match flag[0].as_str() {
            "--data-chunk" => {
                workload.data_chunk = number(&flag[1])?;
                0
            }
            "--caller-chunk" => {
                workload.caller_chunk = number(&flag[1])?;
                1
            }
            "--scenario" => {
                workload.scenario = Scenario::parse(&flag[1])?;
                2
            }
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, USAGE)),
        };
        if std::mem::replace(&mut seen[index], true) {
            return Err(io::Error::other("duplicate optional flag"));
        }
    }
    workload.validate()?;
    Ok(workload)
}

fn distribution_json(class: Option<&Distribution>) -> String {
    class.map_or_else(|| "null".to_owned(), |class| format!(
        concat!(
            "{{\"requests\":{},\"payload_bytes\":{},\"latency_samples\":{},",
            "\"latency_p50_secs\":{:.9},\"latency_p90_secs\":{:.9},\"latency_p99_secs\":{:.9}}}"
        ),
        class.latencies.len(), class.payload_bytes, class.latencies.len(),
        class.percentile_seconds(50), class.percentile_seconds(90), class.percentile_seconds(99),
    ))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() == 1 && matches!(args[0].as_str(), "--help" | "-h") {
        println!("{USAGE}");
        return Ok(());
    }
    let workload = parse_workload(&args)?;
    let result = measure(workload).await?;
    let elapsed = result.elapsed.as_secs_f64();
    println!(
        concat!(
            "{{\"engine\":\"{}\",\"transport\":\"{}\",\"payload_bytes_per_stream_per_direction\":{},",
            "\"streams\":{},\"rounds\":{},\"receive_window_bytes\":{},\"stream_cap\":{},\"data_chunk_bytes\":{},",
            "\"caller_chunk_bytes\":{},\"scenario\":\"{}\",\"interactive_payload_bytes_per_direction\":{},",
            "\"elapsed_secs\":{:.9},\"payload_bytes\":{},\"payload_bytes_per_sec\":{:.3},\"requests\":{},\"requests_per_sec\":{:.3},",
            "\"latency_distribution\":\"per-class successful stream-open-to-byte-validated-echo-EOF exchanges; nearest-rank; seconds; uniform is not interactive latency\",",
            "\"uniform\":{},\"interactive\":{},\"bulk\":{},",
            "\"interactive_requests_completed_during_bulk\":{},",
            "\"bulk_elapsed_secs\":{},\"bulk_goodput_bytes_per_sec\":{},",
            "\"byte_accounting\":\"actual successful request plus echo payload; excludes warmup and frame headers; bulk goodput counts request direction only\",",
            "\"runtime\":\"tokio-current-thread; both endpoints\",\"startup_included\":false,\"warmup_bytes\":4096}}"
        ),
        workload.engine.name(),
        workload.transport.name(),
        workload.payload,
        workload.streams,
        workload.rounds,
        support::WINDOW,
        support::STREAM_CAP,
        workload.data_chunk,
        workload.caller_chunk,
        workload.scenario.name(),
        support::INTERACTIVE_PAYLOAD,
        elapsed,
        result.payload_bytes,
        result.payload_bytes as f64 / elapsed,
        result.requests,
        result.requests as f64 / elapsed,
        distribution_json(result.uniform.as_ref()),
        distribution_json(result.interactive.as_ref()),
        distribution_json(result.bulk.as_ref()),
        result.interactive_requests_completed_during_bulk,
        result.bulk_elapsed.map_or_else(
            || "null".to_owned(),
            |elapsed| format!("{:.9}", elapsed.as_secs_f64())
        ),
        result.bulk.as_ref().zip(result.bulk_elapsed).map_or_else(
            || "null".to_owned(),
            |(bulk, elapsed)| format!(
                "{:.3}",
                (bulk.payload_bytes / 2) as f64 / elapsed.as_secs_f64()
            ),
        ),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Vec<String> {
        ["veyrmux", "1048576", "8", "2", "duplex"]
            .into_iter()
            .chain(extra.iter().copied())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn legacy_defaults_and_independent_chunks() {
        let defaults = parse_workload(&args(&[])).unwrap();
        assert_eq!((defaults.data_chunk, defaults.caller_chunk), (16384, 16384));
        assert_eq!(defaults.scenario, Scenario::Uniform);
        let sized = parse_workload(&args(&[
            "--caller-chunk",
            "7",
            "--scenario",
            "mixed",
            "--data-chunk",
            "65536",
        ]))
        .unwrap();
        assert_eq!((sized.data_chunk, sized.caller_chunk), (65536, 7));
        assert_eq!(sized.scenario, Scenario::Mixed);
    }

    #[test]
    fn rejects_ambiguous_or_incomplete_flags() {
        for flags in [
            vec!["--data-chunk"],
            vec!["--caller-chunk", "0"],
            vec!["--data-chunk", "262145"],
            vec!["--scenario", "other"],
            vec!["--unknown", "1"],
            vec!["--data-chunk", "1", "--data-chunk", "2"],
        ] {
            assert!(parse_workload(&args(&flags)).is_err(), "{flags:?}");
        }
    }
}

#[cfg(test)]
mod workload_tests {
    use super::support::*;
    use std::time::Duration;

    fn workload(engine: Engine, transport: Transport, scenario: Scenario) -> Workload {
        Workload {
            engine,
            transport,
            scenario,
            payload: WINDOW + 17,
            streams: 3,
            rounds: 2,
            data_chunk: WINDOW,
            caller_chunk: 65537,
        }
    }

    #[test]
    fn rejects_invalid_workloads_and_overflowing_accounting() {
        let valid = workload(Engine::Veyrmux, Transport::Duplex, Scenario::Uniform);
        valid.validate().unwrap();
        for invalid in [
            Workload {
                payload: 0,
                ..valid
            },
            Workload {
                streams: 0,
                ..valid
            },
            Workload {
                streams: STREAM_CAP + 1,
                ..valid
            },
            Workload { rounds: 0, ..valid },
            Workload {
                rounds: MAX_LATENCY_SAMPLES + 1,
                ..valid
            },
            Workload {
                data_chunk: 0,
                ..valid
            },
            Workload {
                data_chunk: WINDOW + 1,
                ..valid
            },
            Workload {
                caller_chunk: 0,
                ..valid
            },
            Workload {
                caller_chunk: WINDOW + 1,
                ..valid
            },
            Workload {
                scenario: Scenario::Mixed,
                streams: 1,
                ..valid
            },
        ] {
            assert!(invalid.validate().is_err());
        }
        assert!(
            Workload {
                payload: usize::MAX,
                ..valid
            }
            .validate()
            .is_err()
        );
        assert!(
            Workload {
                payload: 17,
                ..valid
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn nearest_rank_is_over_exchanges_not_round_averages() {
        let distribution = Distribution {
            payload_bytes: 0,
            latencies: [1, 2, 3, 4, 100]
                .into_iter()
                .map(Duration::from_secs)
                .collect(),
        };
        assert_eq!(distribution.percentile_seconds(50), 3.0);
        assert_eq!(distribution.percentile_seconds(90), 100.0);
        assert_eq!(distribution.percentile_seconds(99), 100.0);
    }

    #[tokio::test]
    async fn independent_chunks_validate_non_aligned_payloads_and_eof() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for engine in [Engine::Veyrmux, Engine::Yamux] {
                for transport in [Transport::Duplex, Transport::Tcp] {
                    for (payload, data_chunk, caller_chunk) in
                        [(131, WINDOW, 7), (WINDOW + 17, 16384, 65537), (67, 1, 1)]
                    {
                        let result = measure(Workload {
                            payload,
                            data_chunk,
                            caller_chunk,
                            ..workload(engine, transport, Scenario::Uniform)
                        })
                        .await
                        .unwrap();
                        assert_eq!(result.requests, 6);
                        assert_eq!(result.payload_bytes, 12 * payload as u64);
                        assert_eq!(result.uniform.as_ref().unwrap().latencies.len(), 6);
                        assert!(result.interactive.is_none());
                        assert!(result.bulk.is_none());
                    }
                }
            }
        })
        .await
        .expect("uniform chunk controls must not hang");
    }

    #[tokio::test]
    async fn mixed_overlaps_small_requests_and_accounts_for_graceful_drain() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for engine in [Engine::Veyrmux, Engine::Yamux] {
                for transport in [Transport::Duplex, Transport::Tcp] {
                    let workload = Workload {
                        payload: 1024 * 1024,
                        data_chunk: 32768,
                        caller_chunk: 16384,
                        ..workload(engine, transport, Scenario::Mixed)
                    };
                    let result = measure(workload).await.unwrap();
                    let small = result.interactive.as_ref().unwrap();
                    let bulk = result.bulk.as_ref().unwrap();
                    assert!(result.uniform.is_none());
                    assert_eq!(bulk.latencies.len(), workload.rounds);
                    assert_eq!(
                        bulk.payload_bytes,
                        2 * workload.payload as u64 * workload.rounds as u64
                    );
                    assert!(small.latencies.len() >= workload.streams - 1);
                    assert!(result.interactive_requests_completed_during_bulk > 0);
                    assert!(
                        result.interactive_requests_completed_during_bulk <= small.latencies.len()
                    );
                    assert_eq!(small.payload_bytes, 128 * small.latencies.len() as u64);
                    assert_eq!(
                        result.requests,
                        small.latencies.len() + bulk.latencies.len()
                    );
                    assert_eq!(
                        result.payload_bytes,
                        small.payload_bytes + bulk.payload_bytes
                    );
                    assert!(result.bulk_elapsed.unwrap() <= result.elapsed);
                    assert!(small.percentile_seconds(50) <= small.percentile_seconds(99));
                }
            }
        })
        .await
        .expect("mixed clients must finish in-flight exchanges without hanging");
    }
}
