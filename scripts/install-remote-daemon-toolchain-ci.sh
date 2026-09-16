#!/usr/bin/env bash
# Install everything needed to build and cross-compile daemon/remote
# (cmuxd-remote): the pinned Rust toolchain with the four release targets,
# zig, and cargo-zigbuild. Safe to re-run; skips what is already present.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DAEMON_ROOT="${REPO_ROOT}/daemon/remote"
CARGO_ZIGBUILD_VERSION="${CARGO_ZIGBUILD_VERSION:-0.23.4}"

export PATH="$HOME/.cargo/bin:$PATH"

if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain none
  export PATH="$HOME/.cargo/bin:$PATH"
fi
if [ -n "${GITHUB_PATH:-}" ]; then
  echo "$HOME/.cargo/bin" >> "$GITHUB_PATH"
fi

TOOLCHAIN="$(awk -F '"' '/^[[:space:]]*channel[[:space:]]*=/{print $2; exit}' "${DAEMON_ROOT}/rust-toolchain.toml")"
if [ -z "$TOOLCHAIN" ]; then
  echo "error: could not read channel from ${DAEMON_ROOT}/rust-toolchain.toml" >&2
  exit 1
fi
rustup toolchain install "$TOOLCHAIN" --profile minimal --component clippy,rustfmt
rustup target add --toolchain "$TOOLCHAIN" \
  aarch64-apple-darwin x86_64-apple-darwin \
  aarch64-unknown-linux-musl x86_64-unknown-linux-musl

# zig backs cargo-zigbuild's cross linkers (musl and Mach-O from any host).
"${SCRIPT_DIR}/install-zig-ci.sh"
if [ -n "${CMUX_ZIG:-}" ]; then
  export PATH="$(dirname "$CMUX_ZIG"):$PATH"
fi

if ! command -v cargo-zigbuild >/dev/null 2>&1 \
  || [ "$(cargo-zigbuild --version 2>/dev/null | awk '{print $2}')" != "$CARGO_ZIGBUILD_VERSION" ]; then
  rustup run "$TOOLCHAIN" cargo install --locked --force "cargo-zigbuild@${CARGO_ZIGBUILD_VERSION}"
fi

rustup run "$TOOLCHAIN" cargo --version
rustup run "$TOOLCHAIN" rustc --version
zig version
cargo-zigbuild --version
