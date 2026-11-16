#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Every stage may also be run separately, for machines with cold build caches.
stage=${1:-all}
case "$stage" in
  fmt) cargo fmt --all -- --check ;;
  clippy) cargo clippy --all-targets --all-features -- -D warnings -W clippy::nursery ;;
  test) cargo test --all-features ;;
  all)
    "$0" fmt
    "$0" clippy
    "$0" test
    ;;
  *) echo "usage: $0 [all|fmt|clippy|test]" >&2; exit 2 ;;
esac
