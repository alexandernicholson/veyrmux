#!/usr/bin/env bash
# End-to-end process timings include startup; each command also emits its own
# warmed, byte-validated transfer metrics. Never compare the two as the same metric.
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

usage() {
    printf 'Usage: %s [payload-bytes=1048576] [streams=8] [rounds=16] [duplex|tcp=duplex]\n' "$0"
}
if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi
if (( $# > 4 )); then
    usage >&2
    exit 2
fi
PAYLOAD=${1:-1048576}
STREAMS=${2:-8}
ROUNDS=${3:-16}
TRANSPORT=${4:-duplex}
for value in "$PAYLOAD" "$STREAMS" "$ROUNDS"; do
    if [[ ! $value =~ ^[0-9]+$ ]]; then
        printf 'Payload, streams, and rounds must be nonnegative decimal integers.\n' >&2
        exit 2
    fi
done
if [[ $TRANSPORT != duplex && $TRANSPORT != tcp ]]; then
    printf 'Transport must be duplex or tcp.\n' >&2
    exit 2
fi
for dependency in cargo hyperfine python3; do
    if ! command -v "$dependency" >/dev/null 2>&1; then
        printf 'Required command not found: %s (install locally before running).\n' "$dependency" >&2
        exit 1
    fi
done

cargo build --release --example bench
# cargo metadata respects a caller's configured target directory. No /tmp builds.
TARGET=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
BIN=$(python3 -c 'import os,sys; print("./" + os.path.relpath(sys.argv[1], sys.argv[2]))' "$TARGET/release/examples/bench" "$ROOT")
mkdir -p results
# mktemp creates a unique directory, never overwriting another run's artifacts.
RESULTS=$(mktemp -d "$ROOT/results/transport-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")
printf 'payload=%s\nstreams=%s\nrounds=%s\ntransport=%s\n' "$PAYLOAD" "$STREAMS" "$ROUNDS" "$TRANSPORT" > "$RESULTS/parameters.txt"
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
    --command-name veyrmux "$QUOTED_BIN veyrmux $PAYLOAD $STREAMS $ROUNDS $TRANSPORT" \
    --command-name yamux "$QUOTED_BIN yamux $PAYLOAD $STREAMS $ROUNDS $TRANSPORT"
printf 'Results: %s\n' "$RESULTS"
