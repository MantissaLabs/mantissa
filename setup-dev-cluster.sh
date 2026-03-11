#!/usr/bin/env bash
set -euo pipefail

# Defaults (override with flags)
COUNT=2
REPO="${HOME}/dev/mantissa"   # host path to the mantissa repo (mounted at /mantissa)
ARCH="aarch64"
CPUS=10
MEM="24GiB"
DISK="100GiB"
SSH_BASE=7200
IMAGE_URL="https://cloud.debian.org/images/cloud/bookworm/latest/debian-12-generic-arm64.qcow2"
CREATED_COUNT=0
SKIPPED_COUNT=0
LIMA_ENABLE_VZNAT="${LIMA_ENABLE_VZNAT:-0}"

usage() {
  cat >&2 <<USAGE
Usage: $0 [-n COUNT] [-r /abs/path/to/mantissa] [-P SSH_BASE] [-c CPUS] [-m MEM] [-d DISK]
Defaults: COUNT=2, REPO=\$HOME/dev/mantissa, SSH_BASE=7200, CPUS=10, MEM=24GiB, DISK=100GiB
Notes:
  - Prefers VZ + virtiofs on supported macOS hosts and falls back to QEMU + 9p otherwise.
  - Mounts "~" read-write, and mounts the repo at /mantissa inside each VM.
  - Enables shared VM <-> VM network (user-v2) so VMs can ping each other.
  - Set LIMA_ENABLE_VZNAT=1 to add a secondary vzNAT interface on supported macOS hosts.
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

# Returns success when the current host can use Lima's VZ + virtiofs stack.
#
# Lima documents VZ as supported on macOS 13.0+ and uses it by default on
# compatible macOS hosts. We keep the check local so the generated YAML stays
# valid on Linux and older macOS releases.
host_supports_vz_stack() {
  local PRODUCT_VERSION
  local MAJOR
  local MINOR
  local REST

  if [[ "$(uname -s)" != "Darwin" ]]; then
    return 1
  fi

  PRODUCT_VERSION="$(sw_vers -productVersion 2>/dev/null || true)"
  if [[ -z "${PRODUCT_VERSION}" ]]; then
    return 1
  fi

  MAJOR="${PRODUCT_VERSION%%.*}"
  MINOR=0
  REST=""
  if [[ "${PRODUCT_VERSION}" == *.* ]]; then
    REST="${PRODUCT_VERSION#*.}"
    MINOR="${REST%%.*}"
  fi

  if (( MAJOR > 13 )); then
    return 0
  fi
  if (( MAJOR == 13 && MINOR >= 0 )); then
    return 0
  fi
  return 1
}

# Writes the Lima instance YAML using the requested virtualization settings.
#
# user-v2 remains the primary network because Lima documents it as the
# multi-node path for VM-to-VM communication. vzNAT is optional because it
# changes routing behavior and should be an explicit choice.
write_vm_yaml() {
  local NAME
  local SSHPORT
  local VM_TYPE
  local MOUNT_TYPE
  local ENABLE_VZNAT
  local DEST

  NAME="$1"
  SSHPORT="$2"
  VM_TYPE="$3"
  MOUNT_TYPE="$4"
  ENABLE_VZNAT="$5"
  DEST="$6"

  cat > "${DEST}" <<EOF
# ${NAME}
arch: "${ARCH}"
vmType: "${VM_TYPE}"
cpus: ${CPUS}
memory: "${MEM}"
disk: "${DISK}"

images:
  - location: "${IMAGE_URL}"
    arch: "${ARCH}"

# Shared network so VMs can ping each other.
networks:
EOF

  if [[ "${ENABLE_VZNAT}" == "1" ]]; then
    printf '%s\n' '  - vzNAT: true' >> "${DEST}"
  fi

  cat >> "${DEST}" <<EOF
  - lima: user-v2

mountType: "${MOUNT_TYPE}"
mounts:
  - location: "~"
    writable: true
  - location: "${REPO}"
    mountPoint: "/mantissa"
    writable: true

ssh:
  localPort: ${SSHPORT}

EOF

  # Provision block (quoted heredoc -> no host-side $ expansion)
  cat >> "${DEST}" <<'PROVISION_1'
provision:
  - mode: user
    script: |
      set -euxo pipefail
      sudo apt-get update && sudo apt-get upgrade -y

      # Install docker
      sudo apt-get install -y ca-certificates curl build-essential git capnproto libcapnp-dev libssl-dev pkg-config iputils-ping linux-perf bpftool wireguard ripgrep htop
      sudo install -m 0755 -d /etc/apt/keyrings
      if [ ! -f /etc/apt/keyrings/docker.gpg ]; then
        curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
      fi
      sudo chmod a+r /etc/apt/keyrings/docker.gpg

      # Determine the apt codename for Docker's repository with sensible fallbacks.
      CODENAME=""
      if [ -r /etc/os-release ]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        CODENAME="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
      fi
      if [ -z "$CODENAME" ] && command -v lsb_release >/dev/null 2>&1; then
        CODENAME="$(lsb_release -cs)"
      fi
      if [ -z "$CODENAME" ]; then
        echo "Unable to determine OS codename for Docker repository." >&2
        exit 1
      fi

      DOCKER_SOURCE_LINE="deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian ${CODENAME} stable"
      if ! grep -Fxq "$DOCKER_SOURCE_LINE" /etc/apt/sources.list.d/docker.list 2>/dev/null; then
        printf '%s\n' "$DOCKER_SOURCE_LINE" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      fi
      sudo apt-get update
      sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

      # Add current user to docker group.
      if ! getent group docker >/dev/null; then
        sudo groupadd docker
      fi
      if ! id -nG "$USER" | tr ' ' '\n' | grep -qx docker; then
        sudo usermod -aG docker "$USER"
      fi

      if ! getent group mantissa >/dev/null; then
        sudo groupadd --system mantissa
      fi
      if ! id -nG "$USER" | tr ' ' '\n' | grep -qx mantissa; then
        sudo usermod -aG mantissa "$USER"
      fi
      sudo install -d -m 0750 -o root -g mantissa /var/lib/mantissa

      # Follow Docker post-install guidance: enable daemon
      sudo systemctl enable docker.service
      sudo systemctl start docker.service

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

      # Mantissa dev bin path for convenience
      if ! grep -q 'MANTISSA_BIN=' ~/.bashrc; then
        echo 'export MANTISSA_BIN="/mantissa/target/debug"' >> ~/.bashrc
        echo 'export PATH="$PATH:$MANTISSA_BIN"' >> ~/.bashrc
      fi
      if ! grep -q 'MANTISSA_BIN=' ~/.profile; then
        echo 'export MANTISSA_BIN="/mantissa/target/debug"' >> ~/.profile
        echo 'export PATH="$PATH:$MANTISSA_BIN"' >> ~/.profile
      fi

      # System-wide symlink so both the user and sudo see the freshly built binary.
      sudo ln -sfn /mantissa/target/debug/mantissa /usr/local/bin/mantissa

      if ! grep -q 'alias dockerclean=' ~/.bashrc; then
        echo "alias dockerclean='docker rm -f \$(docker ps -aq)'" >> ~/.bashrc
      fi
      if ! grep -q 'alias dockerclean=' ~/.profile; then
        echo "alias dockerclean='docker rm -f \$(docker ps -aq)'" >> ~/.profile
      fi

      if [ -f "$HOME/.cargo/env" ]; then
        # shellcheck disable=SC1090
        source "$HOME/.cargo/env"
      fi
      export PATH="$HOME/.cargo/bin:$PATH"

      rustup toolchain install nightly-aarch64-unknown-linux-gnu
      rustup component add rust-src --toolchain nightly-aarch64-unknown-linux-gnu

      if ! cargo install --list | grep -q '^flamegraph v'; then
        cargo install flamegraph
      fi
PROVISION_1

  # Separate step to set hostname with ${NAME} (needs host-side expansion)
  cat >> "${DEST}" <<EOF
  - mode: user
    script: |
      set -euxo pipefail
      if command -v hostnamectl >/dev/null 2>&1; then sudo hostnamectl set-hostname ${NAME}; fi
EOF
}

# Validates one generated Lima YAML before we ask Lima to create the instance.
#
# This catches unsupported config keys or incompatible combinations early and
# lets the script fall back to the conservative QEMU profile when needed.
validate_vm_yaml() {
  local YAML_PATH="$1"
  if ! limactl validate --help >/dev/null 2>&1; then
    echo "limactl validate is unavailable; skipping template validation." >&2
    return 1
  fi
  limactl validate --fill "${YAML_PATH}" >/dev/null
}

# Returns success when the Lima instance directory is already present.
# This keeps cluster setup idempotent when rerunning with a higher node count.
vm_exists() {
  local NAME="$1"
  [[ -d "${HOME}/.lima/${NAME}" ]]
}

# Creates and provisions a single Lima VM for cluster use.
# Existing instances are skipped so we only create missing nodes.
start_vm() {
  local NAME
  local SSHPORT
  local TMPYAML
  local VM_TYPE
  local MOUNT_TYPE
  local ENABLE_VZNAT

  NAME="$1"
  SSHPORT="$2"

  if vm_exists "${NAME}"; then
    echo "Skipping ${NAME}: instance already exists."
    SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
    return 0
  fi

  TMPYAML="$(mktemp -t "${NAME}.yaml.XXXXXX")"

  if host_supports_vz_stack; then
    VM_TYPE="vz"
    MOUNT_TYPE="virtiofs"
  else
    VM_TYPE="qemu"
    MOUNT_TYPE="9p"
  fi

  ENABLE_VZNAT=0
  if [[ "${VM_TYPE}" == "vz" && "${LIMA_ENABLE_VZNAT}" == "1" ]]; then
    ENABLE_VZNAT=1
  fi

  write_vm_yaml "${NAME}" "${SSHPORT}" "${VM_TYPE}" "${MOUNT_TYPE}" "${ENABLE_VZNAT}" "${TMPYAML}"
  if ! validate_vm_yaml "${TMPYAML}"; then
    if [[ "${VM_TYPE}" != "qemu" || "${MOUNT_TYPE}" != "9p" || "${ENABLE_VZNAT}" != "0" ]]; then
      echo "Preferred Lima config for ${NAME} failed validation; falling back to qemu + 9p + user-v2." >&2
      VM_TYPE="qemu"
      MOUNT_TYPE="9p"
      ENABLE_VZNAT=0
      write_vm_yaml "${NAME}" "${SSHPORT}" "${VM_TYPE}" "${MOUNT_TYPE}" "${ENABLE_VZNAT}" "${TMPYAML}"
      if ! validate_vm_yaml "${TMPYAML}"; then
        echo "Generated fallback Lima config for ${NAME} could not be validated." >&2
      fi
    fi
  fi

  echo "Starting ${NAME} (SSH port ${SSHPORT}, vmType=${VM_TYPE}, mountType=${MOUNT_TYPE}, vzNAT=${ENABLE_VZNAT})..."
  limactl start --name="${NAME}" "${TMPYAML}"
  CREATED_COUNT=$((CREATED_COUNT + 1))
  rm -f "${TMPYAML}"

  # Ensure any pre-existing SSH ControlMaster session is closed so future shells
  # pick up updated group membership (e.g. docker) without requiring a VM restart.
  local SSH_CONFIG="${HOME}/.lima/${NAME}/ssh.config"
  if [[ -f "${SSH_CONFIG}" ]]; then
    ssh -F "${SSH_CONFIG}" -O exit "lima-${NAME}" >/dev/null 2>&1 || true
  fi
}

# Ensure the first N VM slots exist; create only missing instances.
for i in $(seq 1 "${COUNT}"); do
  NAME="mantissa-${i}"
  SSHPORT=$((SSH_BASE + i))
  start_vm "${NAME}" "${SSHPORT}"
done

echo
echo "Requested ${COUNT} VM(s): created ${CREATED_COUNT}, already present ${SKIPPED_COUNT}."
echo
echo "SSH from host:"
for i in $(seq 1 "${COUNT}"); do
  echo "  ssh -p $((SSH_BASE + i)) \$(whoami)@127.0.0.1   # mantissa-${i}"
done
echo
echo "Inside each VM (open a new shell so env/alias apply):"
echo "  cd /mantissa"
echo "  cargo build -p mantissa"
echo "  sudo mantissa init"
echo
echo "VMs can reach each other via DNS and IP:"
echo "  ping -c1 lima-mantissa-2.internal     # from mantissa-1"
echo "  hostname -I                            # to see your VM's IP(s)"
echo
if [[ "${COUNT}" -ge 2 ]]; then
  cat <<'JOIN'
Join example:
  # On mantissa-2:
  sudo mantissa token show
  # Copy the token

  # On mantissa-1 (use DNS name or IP):
  sudo mantissa link --anchor lima-mantissa-2.internal:6578 --join-token <TOKEN>
  # or:
  sudo mantissa link --anchor <IP_OF_MANTISSA_2>:6578 --join-token <TOKEN>
JOIN
fi

echo
echo "Stop & delete all later with:"
echo "  limactl stop $(printf 'mantissa-%s ' $(seq 1 ${COUNT}))"
echo "  limactl delete $(printf 'mantissa-%s ' $(seq 1 ${COUNT}))"
