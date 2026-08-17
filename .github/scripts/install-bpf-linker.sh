#!/usr/bin/env bash

set -euo pipefail

case "$(uname -m)" in
  x86_64) bpf_linker_arch="x86_64" ;;
  aarch64|arm64) bpf_linker_arch="aarch64" ;;
  *)
    echo "unsupported bpf-linker host architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

bpf_linker_version="v0.11.0"
bpf_linker_archive="bpf-linker-${bpf_linker_arch}-unknown-linux-musl.tar.zst"
bpf_linker_url="https://github.com/aya-rs/bpf-linker/releases/download/${bpf_linker_version}/${bpf_linker_archive}"
download_dir="$(mktemp -d)"
archive_path="${download_dir}/${bpf_linker_archive}"
install_dir="${HOME}/.local/bin"

# Remove only the temporary download created by this script.
cleanup() {
  rm -f "${archive_path}"
  rmdir "${download_dir}" 2>/dev/null || true
}
trap cleanup EXIT

curl --proto '=https' --tlsv1.2 -LsSf "${bpf_linker_url}" \
  --output "${archive_path}"
mkdir -p "${install_dir}"
tar -xpf "${archive_path}" -C "${install_dir}"
echo "${install_dir}" >> "${GITHUB_PATH:?GITHUB_PATH is not set}"
"${install_dir}/bpf-linker" --version
