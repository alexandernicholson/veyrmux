# Veyrmux

A safe Rust, Tokio-native implementation of the Yamux v0 multiplexing protocol.
Independent implementation; interoperates with existing Yamux peers over a
reliable, ordered byte transport.

## Design

- `#![forbid(unsafe_code)]` in the library. Tokio, Bytes and optional compatibility
  adapters expose the safe APIs used here; this is not a claim that dependencies
  contain no unsafe code internally.
- A continuously polled connection owns the transport. Logical streams implement
  Tokio `AsyncRead` and `AsyncWrite`.
- Data-frame headers and bodies are coalesced into contiguous writes.
- Bounded vectored writes group queued frames into at most 16 descriptors and
  64 KiB per call. Non-vectored transports retain the single-frame path.
- Receive storage is single-owner. Payload reads may prefetch at most the next
  12-byte header, without waiting for it or reading an unvalidated payload.
  Small payloads are coalesced into bounded per-stream chunks.
- Receive credit, active streams and pending send bytes have explicit limits.
  Reading data returns credit; reaching stream EOF does not close the opposite
  direction. Dropping an unfinished stream resets it.
- No background busy loop, altered wire format, or application-protocol parsing.

The wire format is specified by [HashiCorp Yamux](https://github.com/hashicorp/yamux/blob/master/spec.md):
12-byte big-endian headers, client-odd/server-even stream IDs, 256 KiB initial
stream credit, SYN/ACK/FIN/RST, window updates, ping and GoAway.

## API

Create `Connection::new(transport, config, Mode::Client)` or `Mode::Server`.
Continuously poll `poll_next_inbound` while any streams are in use, including
outbound-only sessions; this advances reads, writes and control frames.
`poll_new_outbound` opens a stream and `poll_close` closes a session.

`Config::set_split_send_size(bytes)` bounds outgoing DATA payloads independently
of receive credit. The default remains 16 KiB; valid limits are 1..=256 KiB.
Caller slices, available credit and the queued-byte budget can shorten a frame.
Invalid configuration is rejected when opening or driving the connection.

The optional `futures-compat` feature exposes the equivalent Futures I/O adapter
under `veyrmux::yamux_shim`. It exists for migration from the `yamux` crate; it
uses the same protocol implementation and limits, not another transport engine.
Native streams are announced eagerly. The Futures adapter preserves the
reference API's lazy announcement on first read, write, flush or close;
opening an idle stream does not itself notify the peer through that adapter.

`examples/bench.rs` and its support module contain complete connection-driver
and streaming-echo examples for native Veyrmux and reference Yamux.

## Verification

```sh
./scripts/preflight.sh
```

The stages may be run separately as `fmt`, `clippy`, and `test`. Nursery lints are
enabled. Protocol tests include bidirectional interoperability, concurrent streams
larger than the initial receive window, half-close, cancellation/reset recovery,
hostile headers/stream IDs and partial socket writes. Coordination waits have
timeouts.

## Benchmarks

```sh
cargo run --release --example bench -- veyrmux 1048576 8 16 tcp
cargo run --release --example bench -- yamux 1048576 8 16 tcp
./scripts/bench.sh 1048576 8 16 tcp
./scripts/bench.sh 1048576 8 256 tcp --data-chunk 65536 --caller-chunk 65536
cargo run --release --example bench -- veyrmux 1048576 8 256 tcp --scenario mixed
cargo bench --bench transport
```

Both engines use matching windows, DATA limits, caller buffers, stream counts
and payloads. DATA and caller chunks are independent options; both default to
16 KiB. `mixed` runs one bulk worker alongside the remaining 64-byte closed-loop
requesters and reports interactive latency separately from bulk goodput.
This is a fixed-concurrency contention control, not a fixed arrival-rate test.
The echo workload validates every byte and EOF; setup and warmup are excluded
from its internal elapsed time. Hyperfine separately measures complete process
runtime, including startup; those two measurements are not interchangeable.
Generated runs are stored in unique directories under `results/`.

`examples/simd_probe.rs` evaluates `fearless_simd`'s safe portable copy against
`copy_from_slice` with identical buffers. SIMD is a benchmark dependency, not a
runtime dependency whose performance is assumed. Payloads remain opaque.

### Current release measurements: Linux x86_64

Rust 1.100.0, release thin LTO, one Tokio current-thread runtime for both
endpoints. Reference: Yamux 0.13.10. Each process validates 4 GiB: 1 MiB request
and echo, eight streams, 256 rounds. Both DATA and caller chunks are 16 KiB.
TCP is loopback; these are not remote-service or network-capacity measurements.

The native before/after comparison isolates bounded header lookahead with the
same corrected harness. Two interleaved ABBA blocks, one warmup and three
measurements per block give 12 timed processes per native variant; the stock
reference has three. Times include process startup.

| Transport | Before, ms ± σ | After, ms ± σ | Yamux, ms ± σ | Native change |
|---|---:|---:|---:|---:|
| TCP loopback | 672.6 ± 34.5 | 620.8 ± 9.3 | 1113.8 ± 46.1 | −7.7% |
| Bounded in-memory duplex | 326.1 ± 4.5 | 318.7 ± 8.5 | 397.1 ± 2.7 | −2.3% |

Separate TCP counter executions recorded 44.1% fewer `recvfrom` calls.
Small-message and mixed interactive controls used 20 processes per condition.
No stable greater-than-5% p99 regression was observed; the small-duplex paired
interval remains wide, so this does not establish a strict latency bound.

Prefix packing plus queue refills was rejected: TCP slowed despite fewer writes,
and combining it with lookahead lost to lookahead alone. Matched 32/64/128 KiB
DATA/caller sweeps were slower for Veyrmux than 16 KiB; 128 KiB also worsened tail
controls. Defaults remain unchanged. Scalar and vectored offers still obey the
64 KiB cap when larger DATA sizes are explicitly configured.

Source/binary fingerprints, raw timings, per-class observations, counters,
rejected variants and uncertainty are in
[`results/linux-lookahead-sizing/`](results/linux-lookahead-sizing/).
The harness lifecycle correction is recorded separately, not credited to the
runtime algorithm. Cross-capture deltas against the previous baseline are in
[`summary.json`](results/linux-lookahead-sizing/summary.json); historical ratios
below must not be chained into a new release claim.

### Previous receive-storage baseline: Linux x86_64

Rust 1.100.0, release thin LTO, one Tokio current-thread runtime for both
endpoints. Reference: Yamux 0.13.10. One process validates 4 GiB of payload:
1 MiB request and echo per stream, eight streams, 256 rounds. Hyperfine uses
one warmup and three timed runs; process startup is included.

| Transport | Veyrmux mean ± σ | Yamux mean ± σ | Speedup |
|---|---:|---:|---:|
| TCP loopback | 695.0 ± 9.9 ms | 1265.6 ± 79.8 ms | 1.82× |
| Bounded in-memory duplex | 322.1 ± 8.4 ms | 400.2 ± 10.1 ms | 1.24× |

A separate TCP counter pair recorded 708.23 versus 1271.59 ms of task-clock
and 5.66 versus 11.61 billion instructions. Single counter pairs are not
confidence intervals. Exact parameters, source fingerprints, observations
and counters: [`results/linux-selected/`](results/linux-selected/).

That baseline uses independently owned receive buffers and bounded small-payload
coalescing, before header lookahead. Earlier measurements below predate that
storage design; their ratios must not be chained into a release claim.

### Bounded-write batching baseline: Linux x86_64

Rust 1.100.0, release profile with thin LTO, one Tokio current-thread runtime
driving both endpoints. Yamux reference: 0.13.10. Each process validates 4 GiB of
request-plus-echo payload: 1 MiB per direction, eight streams, 256 rounds.
Hyperfine uses one warmup and three measured runs; times include process startup.

| Transport | Veyrmux mean ± σ | Yamux mean ± σ | Speedup |
|---|---:|---:|---:|
| TCP loopback | 659.8 ± 26.8 ms | 1249.7 ± 21.4 ms | 1.89× |
| Bounded in-memory duplex | 329.5 ± 6.4 ms | 403.8 ± 8.4 ms | 1.23× |

A separate TCP CPU-counter pair recorded 656.83 versus 1178.86 ms of task-clock
and 5.27 versus 10.73 billion instructions. These single pairs are observations,
not confidence intervals. See [`results/linux-batching/`](results/linux-batching/).

This development stage selected bounded transport-write batching. Additional
experiments were rejected rather than accumulating unproven machinery:

- A bounded write-buffer cache plus receive-ownership changes regressed TCP
  throughput by 2.73% versus batching, for only a small duplex benefit.
- Notification coalescing measured effectively neutral, with overlapping ranges.
- Explicit SIMD copying was slower at the default 16 KiB frame size on both tested
  architectures. A 64 KiB Linux copy case improved, but this was not an end-to-end
  transport improvement; standard copying remains the runtime choice.

The initial standalone CPU profile prioritized copying, allocation and I/O call
count. Callchains were incomplete, so it does not establish precise attribution
of every copy. Hash-table and mutex replacements were not justified by that
profile. Sanitized measurements and rejected-experiment results are retained
under `results/`; raw profiler recordings are not included.

### Historical ARM64 baseline

macOS ARM64, Rust 1.96.1, one Tokio current-thread runtime driving both endpoints.
This predates vectored batching; it is retained as a development baseline, not
as a measurement of the selected implementation.
Yamux reference version: 0.13.10. The Criterion cases use ten samples, one-second
warmup and three-second measurement targets. Both directions validate the payload.

| Criterion workload | Veyrmux time/round | Yamux time/round |
|---|---:|---:|
| In-memory, 64 B, 1 stream | 5.25 µs | 5.54 µs |
| In-memory, 1 MiB, 1 stream | 232.50 µs | 254.24 µs |
| In-memory, 1 MiB, 8 streams | 1.956 ms | 2.176 ms |
| TCP loopback, 64 B, 1 stream | 24.15 µs | 23.64 µs |
| TCP loopback, 1 MiB, 1 stream | 601.09 µs | 791.87 µs |
| TCP loopback, 1 MiB, 8 streams | 4.477 ms | 5.842 ms |

These are workload-specific results, not a universal speed claim. The small TCP
case is slightly slower by point estimate, with overlapping confidence intervals.
The eight-stream bulk TCP case improves throughput by about 30.5%.

Hyperfine's separate 256-round bulk process measurements found a 1.33× TCP speedup
and a 1.21× in-memory speedup. Those include process startup and use the release
profile (thin LTO); Criterion excludes setup and uses the bench profile (full LTO).
Their absolute durations and relative improvements are not interchangeable.

Raw observations, confidence intervals, source fingerprints, parameters and
Hyperfine data are under [`results/`](results/). The SIMD copy probe was slower
than `copy_from_slice` for both 16 KiB and 64 KiB buffers on this machine, so
the runtime does not add a SIMD dependency.

## License

MIT. See [LICENSE](LICENSE).
