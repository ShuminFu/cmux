#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/build_remote_daemon_release_assets.sh \
  --version <app-version> \
  --release-tag <tag> \
  --repo <owner/repo> \
  --output-dir <dir> \
  [--asset-suffix <suffix>]

Cross-compiles the Rust cmuxd-remote daemon (daemon/remote) with
cargo-zigbuild for the supported remote platforms and emits:
  cmuxd-remote-<os>-<arch>[-<suffix>]
  cmuxd-remote-checksums[-<suffix>].txt
  cmuxd-remote-manifest[-<suffix>].json

When --asset-suffix is provided, all output filenames and manifest download URLs
include the suffix, making each build's assets immutable (used by nightly builds
to avoid checksum mismatches when assets are overwritten by later builds).
EOF
}

VERSION=""
RELEASE_TAG=""
REPO=""
OUTPUT_DIR=""
ASSET_SUFFIX=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --release-tag)
      RELEASE_TAG="${2:-}"
      shift 2
      ;;
    --repo)
      REPO="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --asset-suffix)
      ASSET_SUFFIX="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$VERSION" || -z "$RELEASE_TAG" || -z "$REPO" || -z "$OUTPUT_DIR" ]]; then
  echo "error: --version, --release-tag, --repo, and --output-dir are required" >&2
  usage
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DAEMON_ROOT="${REPO_ROOT}/daemon/remote"
export PATH="$HOME/.cargo/bin:$PATH"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required to build cmuxd-remote release assets (see scripts/install-remote-daemon-toolchain-ci.sh)" >&2
  exit 1
fi
if ! command -v cargo-zigbuild >/dev/null 2>&1; then
  echo "error: cargo-zigbuild is required to cross-compile cmuxd-remote (see scripts/install-remote-daemon-toolchain-ci.sh)" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
rm -f "$OUTPUT_DIR"/cmuxd-remote-* "$OUTPUT_DIR"/cmuxd-remote-checksums.txt "$OUTPUT_DIR"/cmuxd-remote-manifest.json

SUFFIX_TAG=""
if [[ -n "$ASSET_SUFFIX" ]]; then
  SUFFIX_TAG="-${ASSET_SUFFIX}"
fi

CHECKSUMS_ASSET_NAME="cmuxd-remote-checksums${SUFFIX_TAG}.txt"
CHECKSUMS_PATH="${OUTPUT_DIR}/${CHECKSUMS_ASSET_NAME}"
MANIFEST_PATH="${OUTPUT_DIR}/cmuxd-remote-manifest${SUFFIX_TAG}.json"

# Asset names keep the historical <os>-<arch> spelling the app's embedded
# manifest and bootstrap probe rely on; the third column is the Rust target.
TARGETS=(
  "darwin arm64 aarch64-apple-darwin"
  "darwin amd64 x86_64-apple-darwin"
  "linux arm64 aarch64-unknown-linux-musl"
  "linux amd64 x86_64-unknown-linux-musl"
)

: > "$CHECKSUMS_PATH"
ENTRIES_FILE="$(mktemp "${TMPDIR:-/tmp}/cmuxd-remote-entries.XXXXXX")"
trap 'rm -f "$ENTRIES_FILE"' EXIT
: > "$ENTRIES_FILE"

# Each target gets its own target dir so the four builds can run in parallel
# without contending for cargo's build lock.
BUILD_ROOT="${CMUXD_REMOTE_BUILD_ROOT:-${DAEMON_ROOT}/target/release-assets}"
BUILD_PIDS=()
BUILD_LABELS=()
for target in "${TARGETS[@]}"; do
  read -r ASSET_OS ASSET_ARCH RUST_TARGET <<<"$target"
  (
    cd "$DAEMON_ROOT"
    CMUXD_REMOTE_VERSION="$VERSION" \
    cargo zigbuild --release --locked \
      --target "$RUST_TARGET" \
      --target-dir "${BUILD_ROOT}/${RUST_TARGET}"
  ) &
  BUILD_PIDS+=("$!")
  BUILD_LABELS+=("${ASSET_OS}/${ASSET_ARCH} (${RUST_TARGET})")
done

BUILD_FAILED=0
for index in "${!BUILD_PIDS[@]}"; do
  if ! wait "${BUILD_PIDS[$index]}"; then
    echo "error: cmuxd-remote build failed for ${BUILD_LABELS[$index]}" >&2
    BUILD_FAILED=1
  fi
done
if [[ "$BUILD_FAILED" -ne 0 ]]; then
  exit 1
fi

# Assemble checksums and manifest entries in stable target order after every
# parallel build succeeds. Parallel workers only write their own binary.
for target in "${TARGETS[@]}"; do
  read -r ASSET_OS ASSET_ARCH RUST_TARGET <<<"$target"
  ASSET_NAME="cmuxd-remote-${ASSET_OS}-${ASSET_ARCH}${SUFFIX_TAG}"
  OUTPUT_PATH="${OUTPUT_DIR}/${ASSET_NAME}"
  BUILD_PATH="${BUILD_ROOT}/${RUST_TARGET}/${RUST_TARGET}/release/cmuxd-remote"
  cp "$BUILD_PATH" "$OUTPUT_PATH"
  chmod 755 "$OUTPUT_PATH"
  SHA256="$(shasum -a 256 "$OUTPUT_PATH" | awk '{print $1}')"
  printf '%s  %s\n' "$SHA256" "$ASSET_NAME" >> "$CHECKSUMS_PATH"

  printf '%s\t%s\t%s\t%s\n' "$ASSET_OS" "$ASSET_ARCH" "$ASSET_NAME" "$SHA256" >> "$ENTRIES_FILE"
done

python3 "$SCRIPT_DIR/generate_remote_daemon_release_manifest.py" \
  "$VERSION" \
  "$RELEASE_TAG" \
  "$REPO" \
  "$CHECKSUMS_ASSET_NAME" \
  "$CHECKSUMS_PATH" \
  "$MANIFEST_PATH" \
  "$ENTRIES_FILE"

echo "Built cmuxd-remote assets in ${OUTPUT_DIR}"
