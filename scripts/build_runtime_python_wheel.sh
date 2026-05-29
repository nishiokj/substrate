#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DIST_DIR="${DIST_DIR:-$ROOT/dist}"
STAGE="$(mktemp -d)"

cleanup() {
  rm -rf "$STAGE"
}
trap cleanup EXIT

cargo build --release --bin substrate-runtime

mkdir -p "$DIST_DIR"
cp -R "$ROOT/packages/substrate-runtime-python" "$STAGE/substrate-runtime-python"
cp "$ROOT/target/release/substrate-runtime" \
  "$STAGE/substrate-runtime-python/src/substrate_runtime/bin/substrate-runtime"
chmod 755 "$STAGE/substrate-runtime-python/src/substrate_runtime/bin/substrate-runtime"

"$PYTHON_BIN" -m build --wheel --outdir "$DIST_DIR" "$STAGE/substrate-runtime-python"
