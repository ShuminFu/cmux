#!/usr/bin/env bash
# Format, lint, test, and Go-parity-check the Rust remote daemon
# (daemon/remote-rs). Mirrors what the remote-daemon-rs-tests CI job runs.
#
# Usage: scripts/run-remote-daemon-rs-checks.sh [--require-go] [--skip-parity]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CRATE="$ROOT/daemon/remote-rs"
MANIFEST="$CRATE/Cargo.toml"
REQUIRE_GO=0
SKIP_PARITY=0
for arg in "$@"; do
  case "$arg" in
    --require-go) REQUIRE_GO=1 ;;
    --skip-parity) SKIP_PARITY=1 ;;
    *)
      echo "usage: $0 [--require-go] [--skip-parity]" >&2
      exit 2
      ;;
  esac
done

export PATH="$HOME/.cargo/bin:$PATH"

echo "==> cargo fmt --check"
cargo fmt --manifest-path "$MANIFEST" --all -- --check

echo "==> cargo clippy --all-targets -- -D warnings"
cargo clippy --manifest-path "$MANIFEST" --all-targets --locked -- -D warnings

echo "==> cargo test"
cargo test --manifest-path "$MANIFEST" --locked

if [[ "$SKIP_PARITY" == "1" ]]; then
  echo "==> parity harness skipped (--skip-parity)"
  exit 0
fi

echo "==> Go/Rust parity harness"
PARITY_ARGS=()
if [[ "$REQUIRE_GO" == "1" ]]; then
  PARITY_ARGS+=(--require-go)
fi
cargo build --manifest-path "$MANIFEST" --locked
python3 "$CRATE/parity/run_parity.py" --rust-bin "$CRATE/target/debug/cmuxd-remote" "${PARITY_ARGS[@]}"
