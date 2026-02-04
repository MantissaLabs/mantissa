# Running with Elevated Privileges

Mantissa adjusts its data directories and Unix socket layout depending on whether it runs as root
or as an unprivileged user.

- Root: writes cluster state to `/var/lib/mantissa` and exposes its control socket under `/var/run`.
- Unprivileged: uses `~/.mantissa` for state and local sockets.

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
