# Secrets in Manifests

Service and agent manifests can populate container environment variables or
files with cluster secrets. Before deploying a manifest that references
secrets, seed them on a node that is already part of the cluster:

```bash
# Generate a random API token and store it
mantissa secrets create demo-api-token --value "$(openssl rand -hex 32)"

# Pipe a database password from stdin (no echo in history)
printf 'p@55w0rd!' | mantissa secrets create demo-db-password

# Import an existing PEM key (can be any binary payload)
mantissa secrets create demo-nginx-key <<'SECRET_EOF'
-----BEGIN PRIVATE KEY-----
...truncated key material...
-----END PRIVATE KEY-----
SECRET_EOF

mantissa secrets list
```

The bundled service manifest `examples/replicated_service.ron` shows how those
secrets are consumed:

```ron
(
    name: "demo-service",
    tasks: [
        (
            name: "echo",
            env: [
                (name: "DEMO_API_TOKEN", value: None, secret: Some((name: "demo-api-token", version: None))),
            ],
            secret_files: [
                (path: "/run/secrets/demo-database-password", secret: (name: "demo-db-password", version: None), mode: Some(0o440)),
            ],
            ...
        ),
        (
            name: "api",
            secret_files: [
                (path: "/etc/nginx/ssl/private_key", secret: (name: "demo-nginx-key", version: None), mode: Some(0o400)),
            ],
            ...
        ),
    ],
)
```

Secrets are resolved on the node that launches the task: environment variables receive the
plaintext value, and file projections mount a read-only bind of staged secret material inside the
container. Once the task stops or is rescheduled, Mantissa scrubs the temporary host-side staging
area.

## At-Rest Security Model

Mantissa encrypts stored secret values with the cluster secret master key before
replicating them through the control plane. The master key itself is stored as a
local envelope: Redb contains only encrypted master-key records plus metadata,
not the plaintext 32-byte key.

The first envelope provider is passphrase-backed. On `mantissa init`, Mantissa
prompts for the master-key passphrase when attached to a terminal. For
non-interactive starts, provide the passphrase through one of these sources:

```bash
mantissa init --master-key-passphrase-file /run/mantissa/master-key-passphrase
mantissa init --master-key-passphrase-fd 3
```

Do not pass the passphrase as a command-line value. Process arguments are easy
to expose through shell history, process listings, service-manager metadata, and
logs. File and file-descriptor sources still contain plaintext at read time, but
they keep it out of argv and can be backed by owner-protected files, tmpfs,
systemd credentials, named pipes, or secret-manager wrappers. On Unix, regular
passphrase files are rejected if they are readable by group or other users.

Every node may use a different local passphrase. The passphrase protects only
that node's at-rest master-key envelope; it is not the cluster master key. When
a node joins or receives a master-key rotation, the current cluster master key
is transferred in an authenticated envelope encrypted to the recipient node's
Noise static key, then re-wrapped locally with the recipient's passphrase.

Fresh standalone nodes create a temporary local v1 master key during bootstrap.
If the node joins an existing cluster, it may replace that temporary key with
the authenticated anchor key. If the node first serves its key to another peer,
that key is committed as the cluster key and later conflicting same-version
imports are rejected. This prevents split-key races where one node could give a
peer key A while concurrently adopting key B from another anchor.

The state database is still sensitive. A copied Redb file allows offline
passphrase guessing against the envelope, and a live privileged compromise of a
joined node can read decrypted key material from process memory. Use strong
generated passphrases, host disk encryption, and tight local administrative
access controls.

After creating the secrets, deploy the manifest and inspect the resulting tasks:

```bash
mantissa networks list
mantissa services run examples/replicated_service.ron
mantissa tasks list --state running
```

`services run` follows deployment progress by default, so a missing secret is
reported directly before the command exits.

If a secret is missing, the deployment fails fast with a descriptive error so you can seed it
before retrying.

Agent manifests use the same secret reference shape. The bundled
`examples/codex_agent_nono.ron` projects the OpenAI API key as a secret file
inside a sandboxed Codex session, and the example image entrypoint exports
`CODEX_API_KEY` from that file right before launching Codex:

```ron
(
    execution: (
        secret_files: [
            (
                path: "/run/secrets/codex-api-key",
                secret: (
                    name: "openai-api-key",
                    version: None,
                ),
                mode: Some(0o400),
                ownership: user(uid: 1000, gid: 1000),
                path_env_name: Some("CODEX_API_KEY_PATH"),
            ),
        ],
    ),
)
```

This keeps the secret out of Docker environment metadata while still satisfying
programs that require an environment variable at process start. The
`path_env_name` helper is the preferred production pattern for tools that
already support `*_FILE` or equivalent path-based configuration, because
Mantissa can point the process at the mounted file without projecting the
secret value into Docker env metadata at all.

A complete end-to-end flow for that example looks like this:

```bash
docker build -t mantissa/codex-sandbox:0.118.0 \
  examples/images/codex-sandbox
mantissa secrets create openai-api-key --value "$OPENAI_API_KEY"
mantissa agents run --file examples/codex_agent_nono.ron
mantissa agents inspect <SESSION_ID>
mantissa agents logs <SESSION_ID> -f
```
