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
`examples/codex_agent_nono.ron` injects `CODEX_API_KEY` into a sandboxed
Codex session through the agent execution environment:

```ron
(
    execution: (
        env: [
            (
                name: "CODEX_API_KEY",
                value: None,
                secret: Some((
                    name: "openai-api-key",
                    version: None,
                )),
            ),
        ],
    ),
)
```

A complete end-to-end flow for that example looks like this:

```bash
docker build -t mantissa/codex-sandbox:0.118.0 \
  examples/images/codex-sandbox
mantissa secrets create openai-api-key --value "$OPENAI_API_KEY"
mantissa agents run --file examples/codex_agent_nono.ron
mantissa agents inspect <SESSION_ID>
mantissa agents logs <SESSION_ID> -f
```
