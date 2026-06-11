#!/usr/bin/env bash
set -euo pipefail

target="${CARGO_DIST_TARGET:?CARGO_DIST_TARGET must be set by cargo-dist}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/.." && pwd)"
out_dir="$repo_root/target/dist/mantissa"
release_dir="$repo_root/target/$target/release"

rustup target add "$target"

cargo build \
  --release \
  --target "$target" \
  --package mantissa-cli \
  --bin mantissa \
  --package mantissa-sandbox \
  --bin mantissa-sandbox-init

mkdir -p "$out_dir"
install -m 0755 "$release_dir/mantissa" "$out_dir/mantissa"
install -m 0755 "$release_dir/mantissa-sandbox-init" "$out_dir/mantissa-sandbox-init"
