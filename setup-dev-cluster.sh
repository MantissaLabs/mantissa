#!/usr/bin/env bash
set -euo pipefail

# Defaults (override with flags)
COUNT=2
REPO="${HOME}/dev/mantissa"   # host path to the mantissa repo (mounted at /mantissa)
ARCH="aarch64"
CPUS=8
MEM="16GiB"
DISK="100GiB"
SSH_BASE=7200
IMAGE_URL="https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-generic-arm64.qcow2"

usage() {
  cat >&2 <<USAGE
Usage: $0 [-n COUNT] [-r /abs/path/to/mantissa] [-P SSH_BASE] [-c CPUS] [-m MEM] [-d DISK]
Defaults: COUNT=2, REPO=\$HOME/dev/mantissa, SSH_BASE=7200, CPUS=8, MEM=16GiB, DISK=100GiB
Notes:
  - QEMU + aarch64 + mountType=9p.
  - Mounts "~" read-write, and mounts the repo at /mantissa inside each VM.
  - Enables shared VM <-> VM network (user-v2) so VMs can ping each other.
Examples:
  $0 -n 3
  $0 -n 3 -r /Users/you/dev/mantissa
USAGE
  exit 1
}

while getopts ":n:r:P:c:m:d:h" opt; do
  case $opt in
    n) COUNT="$OPTARG" ;;
    r) REPO="$OPTARG" ;;
    P) SSH_BASE="$OPTARG" ;;
    c) CPUS="$OPTARG" ;;
    m) MEM="$OPTARG" ;;
    d) DISK="$OPTARG" ;;
    h|*) usage ;;
  esac
done

if ! command -v limactl >/dev/null 2>&1; then
  echo "limactl not found. Install Lima first." >&2
  exit 1
fi

if [[ ! -d "$REPO" ]]; then
  echo "Repo path not found: $REPO" >&2
  exit 1
fi

start_vm() {
  local NAME="$1" SSHPORT="$2" TMPYAML
  TMPYAML="$(mktemp -t "${NAME}.yaml.XXXXXX")"

  {
    echo "# ${NAME}"
    echo "arch: \"${ARCH}\""
    echo "vmType: \"qemu\""
    echo "cpus: ${CPUS}"
    echo "memory: \"${MEM}\""
    echo "disk: \"${DISK}\""
    echo
    echo "images:"
    echo "  - location: \"${IMAGE_URL}\""
    echo "    arch: \"${ARCH}\""
    echo
    # Shared network so VMs can ping each other
    echo "networks:"
    echo "  - lima: user-v2"
    echo
    echo "mountType: \"9p\""
    echo "mounts:"
    echo "  - location: \"~\""
    echo "    writable: true"
    echo "  - location: \"${REPO}\""
    echo "    mountPoint: \"/mantissa\""
    echo "    writable: true"
    echo
    echo "ssh:"
    echo "  localPort: ${SSHPORT}"
    echo
    # Provision block (quoted heredoc -> no host-side $ expansion)
    cat <<'PROVISION_1'
provision:
  - mode: user
    script: |
      set -euxo pipefail
      sudo apt-get update && sudo apt-get upgrade -y
      sudo apt-get install -y build-essential curl git capnproto libcapnp-dev libssl-dev pkg-config iputils-ping
      # Rust toolchain
      if ! command -v rustup >/dev/null 2>&1; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
      fi
      # Ensure cargo in PATH for interactive shells
      if ! grep -q 'cargo/bin' ~/.bashrc; then
        echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.bashrc
      fi
      if ! grep -q 'cargo/bin' ~/.profile; then
        echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.profile
      fi
      # Mantissa dev bin and alias
      if ! grep -q 'MANTISSA_BIN=' ~/.bashrc; then
        echo 'export MANTISSA_BIN="/mantissa/target/debug"' >> ~/.bashrc
        echo 'export PATH="$PATH:$MANTISSA_BIN"' >> ~/.bashrc
        echo "alias mts='mantissa'" >> ~/.bashrc
      fi
      if ! grep -q 'MANTISSA_BIN=' ~/.profile; then
        echo 'export MANTISSA_BIN="/mantissa/target/debug"' >> ~/.profile
        echo 'export PATH="$PATH:$MANTISSA_BIN"' >> ~/.profile
        echo "alias mts='mantissa'" >> ~/.profile
      fi
PROVISION_1
    # Separate step to set hostname with ${NAME} (needs host-side expansion)
    echo "  - mode: user"
    echo "    script: |"
    echo "      set -euxo pipefail"
    echo "      if command -v hostnamectl >/dev/null 2>&1; then sudo hostnamectl set-hostname ${NAME}; fi"
  } > "${TMPYAML}"

  echo "Starting ${NAME} (SSH port ${SSHPORT})…"
  limactl start --name="${NAME}" "${TMPYAML}"
  rm -f "${TMPYAML}"
}

# Create and start N VMs
for i in $(seq 1 "${COUNT}"); do
  NAME="mantissa-${i}"
  SSHPORT=$((SSH_BASE + i))
  start_vm "${NAME}" "${SSHPORT}"
done

echo
echo "✅ ${COUNT} VM(s) up with shared network (user-v2)."
echo
echo "SSH from host:"
for i in $(seq 1 "${COUNT}"); do
  echo "  ssh -p $((SSH_BASE + i)) \$(whoami)@127.0.0.1   # mantissa-${i}"
done
echo
echo "Inside each VM (open a new shell so env/alias apply):"
echo "  cd /mantissa"
echo "  cargo build -p mantissa"
echo "  mts init    # alias for 'mantissa'"
echo
echo "VMs can reach each other via DNS and IP:"
echo "  ping -c1 lima-mantissa-2.internal     # from mantissa-1"
echo "  hostname -I                            # to see your VM's IP(s)"
echo
if [[ "${COUNT}" -ge 2 ]]; then
  cat <<'JOIN'
Join example:
  # On mantissa-2:
  mts token show
  # Copy the token

  # On mantissa-1 (use DNS name or IP):
  mts link --anchor lima-mantissa-2.internal:6578 --join-token <TOKEN>
  # or:
  mts link --anchor <IP_OF_MANTISSA_2>:6578 --join-token <TOKEN>
JOIN
fi

echo
echo "Stop & delete all later with:"
echo "  limactl stop $(printf 'mantissa-%s ' $(seq 1 ${COUNT}))"
echo "  limactl delete $(printf 'mantissa-%s ' $(seq 1 ${COUNT}))"
