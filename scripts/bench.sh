#!/usr/bin/env bash
# End-to-end process timings include startup; each command also emits its own
# warmed, byte-validated transfer metrics. Never compare the two as the same metric.
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

usage() {
    printf 'Usage: %s [payload-bytes=1048576] [streams=8] [rounds=16] [duplex|tcp=duplex] [--data-chunk BYTES] [--caller-chunk BYTES] [--scenario uniform|mixed]\n' "$0"
    printf 'Chunks default to 16384; both must be 1..262144. Mixed needs >=2 streams; one bulk client plus continuous 64-byte clients.\n'
}
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi
POSITIONAL=()
DATA_CHUNK=16384
CALLER_CHUNK=16384
SCENARIO=uniform
SEEN_DATA=0
SEEN_CALLER=0
SEEN_SCENARIO=0
while (( $# )); do
    case "$1" in
        --data-chunk|--caller-chunk|--scenario)
            if (( $# < 2 )); then usage >&2; exit 2; fi
            case "$1" in
                --data-chunk)
                    if (( SEEN_DATA )); then usage >&2; exit 2; fi
                    DATA_CHUNK=$2; SEEN_DATA=1 ;;
                --caller-chunk)
                    if (( SEEN_CALLER )); then usage >&2; exit 2; fi
                    CALLER_CHUNK=$2; SEEN_CALLER=1 ;;
                --scenario)
                    if (( SEEN_SCENARIO )); then usage >&2; exit 2; fi
                    SCENARIO=$2; SEEN_SCENARIO=1 ;;
            esac
            shift 2 ;;
        --*) usage >&2; exit 2 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
if (( ${#POSITIONAL[@]} > 4 )); then usage >&2; exit 2; fi
PAYLOAD=${POSITIONAL[0]:-1048576}
STREAMS=${POSITIONAL[1]:-8}
ROUNDS=${POSITIONAL[2]:-16}
TRANSPORT=${POSITIONAL[3]:-duplex}
for value in "$PAYLOAD" "$STREAMS" "$ROUNDS" "$DATA_CHUNK" "$CALLER_CHUNK"; do
    if [[ ! $value =~ ^[0-9]+$ ]]; then
        printf 'Payload, streams, rounds and chunks must be decimal integers.\n' >&2
        exit 2
    fi
done
if [[ $TRANSPORT != duplex && $TRANSPORT != tcp ]] || [[ $SCENARIO != uniform && $SCENARIO != mixed ]]; then
    usage >&2
    exit 2
fi
for dependency in cargo hyperfine python3; do
    if ! command -v "$dependency" >/dev/null 2>&1; then
        printf 'Required command not found: %s (install locally before running).\n' "$dependency" >&2
        exit 1
    fi
done
python3 - "$PAYLOAD" "$STREAMS" "$ROUNDS" "$DATA_CHUNK" "$CALLER_CHUNK" "$SCENARIO" <<'PY'
import sys
payload, streams, rounds, data, caller = map(int, sys.argv[1:6])
scenario = sys.argv[6]
if not (0 < payload < 2**64 and 1 <= streams <= 64 and 0 < rounds <= 1048576 // streams
        and 1 <= data <= 262144 and 1 <= caller <= 262144
        and 2 * payload * streams * rounds < 2**64
        and (scenario != "mixed" or streams >= 2)):
    sys.exit("Invalid payload/streams/rounds/chunks or overflowing byte accounting; see --help.")
PY

cargo build --release --example bench
# cargo metadata respects a caller's configured target directory. No /tmp builds.
TARGET=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
BIN=$(python3 -c 'import os,sys; print("./" + os.path.relpath(sys.argv[1], sys.argv[2]))' "$TARGET/release/examples/bench" "$ROOT")
mkdir -p results
# mktemp creates a unique directory, never overwriting another run's artifacts.
RESULTS=$(mktemp -d "$ROOT/results/transport-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
printf 'payload=%s\nstreams=%s\nrounds=%s\ntransport=%s\ndata_chunk=%s\ncaller_chunk=%s\nscenario=%s\nreceive_window=262144\nruntime=tokio-current-thread; both endpoints\n' \
    "$PAYLOAD" "$STREAMS" "$ROUNDS" "$TRANSPORT" "$DATA_CHUNK" "$CALLER_CHUNK" "$SCENARIO" > "$RESULTS/parameters.txt"
rustc --version >> "$RESULTS/parameters.txt"
uname -srm >> "$RESULTS/parameters.txt"
printf 'RUSTFLAGS=%s\n' "${RUSTFLAGS:-}" >> "$RESULTS/parameters.txt"
python3 - >> "$RESULTS/parameters.txt" <<'PY'
import hashlib
from pathlib import Path
h = hashlib.sha256()
paths = [Path("Cargo.toml"), Path("Cargo.lock")]
paths += sorted(Path("src").rglob("*.rs"))
paths += sorted(Path("examples").rglob("*.rs"))
paths += sorted(Path("benches").rglob("*.rs"))
paths += [Path("scripts/bench.sh")]
for path in paths:
    h.update(str(path).encode())
    h.update(path.read_bytes())
print("source_sha256=" + h.hexdigest())
PY
# Quote the executable for hyperfine's command shell, including spaces in the path.
printf -v QUOTED_BIN '%q' "$BIN"
# Each executable enforces a 30-second deadline, including startup and warmup.
hyperfine --warmup 1 --runs 3 --show-output \
    --export-json "$RESULTS/hyperfine.json" \
    --command-name veyrmux "$QUOTED_BIN veyrmux $PAYLOAD $STREAMS $ROUNDS $TRANSPORT --data-chunk $DATA_CHUNK --caller-chunk $CALLER_CHUNK --scenario $SCENARIO" \
    --command-name yamux "$QUOTED_BIN yamux $PAYLOAD $STREAMS $ROUNDS $TRANSPORT --data-chunk $DATA_CHUNK --caller-chunk $CALLER_CHUNK --scenario $SCENARIO"
printf 'Results: %s\n' "$RESULTS"
