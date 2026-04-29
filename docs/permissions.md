# Running with Elevated Privileges

Mantissa adjusts its data directories and Unix socket layout depending on whether it runs as root
or as an unprivileged user.

- Root: writes cluster state to `/var/lib/mantissa` and exposes its control socket under `/var/run`
  or `/run`.
- Unprivileged: uses `~/.mantissa` for state and prefers private runtime socket paths such as
  `$XDG_RUNTIME_DIR/mantissa/mantissa.sock`, `~/.mantissa/mantissa.sock`, or a per-user directory
  under the system temp directory.

## Trust Boundary

Mantissa currently uses a deliberately coarse trust model:

- Every joined node is a fully trusted cluster member. Do not join hosts that should only receive
  partial cluster authority.
- The local Unix socket is a cluster-admin control socket. A user that can connect to it can submit
  workloads, mutate services, networks, volumes, jobs, scheduling state, and secret metadata or
  values exposed by the local APIs.
- When the daemon runs as root, access to the socket is granted to root and the `mantissa` group.
  Treat membership in that group as cluster-admin access, similar to membership in Docker's local
  control-socket group.
- Mantissa does not currently provide read-only, deploy-only, or per-service scoped local
  capabilities. Use a separate cluster boundary when operators or workloads must not share the
  same administrative trust domain.

Do not expose the local Unix socket through TCP proxies, broad shared mounts, or world-writable
directories. The fallback socket path is intentionally placed below a private per-user directory
rather than directly under `/tmp`.

The state directory may be group-traversable for privileged local workflows, but
the Redb state database itself is owner-only (`0600` on Unix). It contains local
credentials and the cluster secret master key, so membership in the `mantissa`
group should not grant offline read access to `state.redb`.

To mimic Docker's developer workflow (build as your user, run the daemon with sudo), set up the
shared `mantissa` group once:

```bash
sudo groupadd --system mantissa            # no-op if it already exists
sudo usermod -aG mantissa "$USER"
sudo install -d -m 0750 -o root -g mantissa /var/lib/mantissa
```

The Lima provisioning script (`setup-dev-cluster.sh`) performs those steps automatically for VM
users.

When you want to run the daemon with elevated privileges from the repo build, create a system-wide
symlink:

```bash
cargo build -p mantissa
sudo ln -sfn "$(pwd)/target/debug/mantissa" /usr/local/bin/mantissa
```

With that in place, run privileged commands explicitly (`sudo mantissa init`, `sudo mantissa token show`, ...)
and drop `sudo` for the unprivileged client.
