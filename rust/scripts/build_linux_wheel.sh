#!/usr/bin/env bash
# Builds a manylinux2014 x86_64 abi3 wheel (for Databricks clusters) in Docker,
# cross-compiling natively with zig. Output: target/wheels/ at the repo root.
# PIP_INDEX_URL / ~/.cargo/config.toml are passed through for package proxies.
set -euo pipefail

RUST_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$RUST_DIR/../target/wheels"
mkdir -p "$OUT_DIR"

CARGO_CONFIG_MOUNT=()
if [[ -f "$HOME/.cargo/config.toml" ]]; then
  CARGO_CONFIG_MOUNT=(-v "$HOME/.cargo/config.toml:/usr/local/cargo/config.toml:ro")
fi

docker run --rm \
  -v "$RUST_DIR":/src -v "$OUT_DIR":/out -v grib-cargo-cache:/usr/local/cargo/registry \
  "${CARGO_CONFIG_MOUNT[@]}" \
  -e PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.org/simple}" \
  -w /src rust:1-bookworm bash -euc '
    rustup target add x86_64-unknown-linux-gnu >/dev/null
    apt-get update -qq >/dev/null && apt-get install -y -qq python3-venv >/dev/null
    python3 -m venv /tmp/venv && /tmp/venv/bin/pip install -q "maturin>=1.9,<2" ziglang
    export PATH=/tmp/venv/bin:$PATH
    CARGO_TARGET_DIR=/tmp/target maturin build --release --zig \
      --target x86_64-unknown-linux-gnu --compatibility manylinux2014 -o /out
  '
