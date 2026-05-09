# Quickstart

This guide covers the two fastest ways to spin up a Mantissa cluster:

- Two nodes on a single machine (local development)
- A multi-VM cluster using the provided Lima script

## Prerequisites

- Rust 1.74+ installed via rustup.
- Cap'n Proto tooling (`capnp` plus headers such as `libcapnp-dev` on Debian/Ubuntu).
- Clang/LLVM toolchain when hacking on networking/eBPF components.
- Optional: Lima for multi-VM clusters (`setup-dev-cluster.sh`).

## Option A: Two Nodes on One Machine

1) Start the first node (blocking):

```bash
mantissa init
```

`mantissa init` prompts for the local master-key envelope passphrase when run
interactively. For non-interactive starts, use
`--master-key-passphrase-file <file>` or `--master-key-passphrase-fd <fd>`.
To keep the daemon in the background, run `mantissa init --detach`; it still
prompts when attached to a terminal. Use `mantissa status`, `mantissa logs -f`,
and `mantissa shutdown` for the local process lifecycle.

2) In a second terminal, fetch the join token:

```bash
mantissa token show
```

3) Join a second node on a different port:

```bash
mantissa join \
  --anchor 127.0.0.1:6578 \
  --join-token <TOKEN_FROM_STEP_2> \
  --listen 127.0.0.1:6580
```

4) Inspect the cluster:

```bash
mantissa nodes list
mantissa scheduler slots --details
```

5) (Optional) Create an overlay network used by the sample service manifest:

```bash
mantissa networks create \
  --name demo-overlay \
  --description "Overlay for demo-service" \
  --subnet 10.240.0.0/16
```

6) Deploy the sample service manifest:

```bash
mantissa services run examples/replicated_service.ron
```

`services run` follows deployment progress by default and exits when the
submitted service reaches `running`. Its progress view shows the service line
with task-template aggregates underneath. Use `--detach` to return immediately
after submission, then inspect progress later with `mantissa services list` or
`mantissa services rollout status <SERVICE_ID>`.

Stop each node with `Ctrl+C` when finished.

## Option B: Multi-VM Cluster with Lima

1) Provision VMs and mount the repo inside each guest:

```bash
./setup-dev-cluster.sh -n 2 -r $(pwd)
```

2) SSH into each VM (as printed by the script), then build once:

```bash
cd /mantissa
cargo build
```

3) On VM 1:

```bash
mantissa init
mantissa token show
```

Use the same interactive prompt or passphrase source options described above on
each VM. The passphrase protects each node's local master-key envelope; joined
nodes still receive the cluster master key from the anchor during `mantissa join`.

4) On VM 2, join the cluster:

```bash
mantissa join --anchor <vm1-ip>:6578 --join-token <TOKEN>
```

Use `mantissa nodes list` and `mantissa scheduler slots` to inspect cluster state.

## Next steps

- GPU setup: `docs/gpu-setup.md`
- Configuration and hot reload: `docs/configuration.md`
- Secrets in manifests: `docs/secrets.md`
- Service rollout strategy: `docs/service-rollouts.md`
- Large-cluster stress test: `docs/stress-test.md`
