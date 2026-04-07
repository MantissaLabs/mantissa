# Secrets in Manifests

Service and agent manifests can hydrate container environment variables or
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
    task_templates: [
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

After creating the secrets, deploy the manifest and inspect the resulting tasks:

```bash
mantissa networks list
mantissa services run examples/replicated_service.ron
mantissa services list
mantissa tasks list --state running
```

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
