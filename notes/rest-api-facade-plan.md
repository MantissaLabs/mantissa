# Local REST API Facade Plan

## Goal

Add a local, typed REST API facade for Mantissa that is convenient for local
programs, dashboards, and scripts while keeping Cap'n Proto as the real node and
client protocol.

This is not a public internet-facing cluster API. The first implementation
should be a local admin gateway over the existing daemon Unix socket, with the
same effective authority as any client that can connect to that socket.

## Non-Goals

- Do not generate REST code from Cap'n Proto.
- Do not expose peer bootstrap, gossip, anti-entropy sync, internal workload
  assignment, or scheduler lease mutation over REST.
- Do not make REST part of the node-to-node protocol.
- Do not promise Kubernetes-compatible semantics or public API stability.
- Do not add fine-grained RBAC in the first version.

## High-Level Shape

The recommended first implementation is a new workspace crate:

```text
crates/
  mantissa-rest/
    Cargo.toml
    src/
      lib.rs
      config.rs
      server.rs
      state.rs
      error.rs
      auth.rs
      client_worker.rs
      types/
        mod.rs
        agents.rs
        clusters.rs
        jobs.rs
        networks.rs
        nodes.rs
        scheduler.rs
        secrets.rs
        services.rs
        tasks.rs
        volumes.rs
      routes/
        mod.rs
        agents.rs
        clusters.rs
        jobs.rs
        networks.rs
        nodes.rs
        scheduler.rs
        secrets.rs
        services.rs
        tasks.rs
        volumes.rs
      stream/
        mod.rs
        task_logs.rs
        task_exec.rs
```

The dependency direction should be:

```text
local HTTP client
  -> mantissa-rest
    -> mantissa-client / mantissa-protocol
      -> local Unix socket
        -> daemon ClusterSession
```

`mantissa-rest` owns HTTP details: routing, path/query parsing, JSON types,
HTTP status codes, auth headers, CORS posture, and streaming choices.

`mantissa-client` should remain the reusable typed client/domain layer. If a
REST route needs an operation that only exists inline in the CLI today, add a
typed function to `mantissa-client` first, then call it from REST.

## Runtime Constraint: Cap'n Proto Local Worker

The current Cap'n Proto client path uses local tasks and `Rc`-backed RPC
capabilities. HTTP handlers should not directly store or await those clients in
Axum state.

Use a local client worker:

```text
Axum handler, Send future
  -> mpsc command + oneshot response
    -> client_worker on LocalSet
      -> mantissa-client / capnp RPC
```

This keeps HTTP handlers ordinary `Send` futures while all Cap'n Proto RPC work
runs on a single local task executor. It also creates one place to handle Unix
socket reconnects and Cap'n Proto session invalidation.

Initial worker behavior:

- Own a `mantissa_client::config::ClientConfig`.
- Accept typed commands such as `JobsList`, `ServicesInspect`, `TaskStart`.
- Open a local session lazily.
- Reconnect on transport/session failure.
- Return owned HTTP-ready Rust values, not Cap'n Proto readers.

Later worker behavior:

- Cache capability handles if that proves useful.
- Track basic per-command latency and failure metrics.
- Support a second backend for embedded daemon mode.

## Configuration

Add a REST config type in `mantissa-rest`:

```rust
pub struct RestConfig {
    pub bind_addr: std::net::SocketAddr,
    pub socket: Option<std::path::PathBuf>,
    pub auth: RestAuthConfig,
}
```

Defaults:

- Bind to `127.0.0.1:6579`.
- Use normal Mantissa Unix socket auto-discovery when `socket` is unset.
- Disable CORS by default.
- Reject non-loopback bind addresses unless auth is explicitly configured.

Auth policy:

- Treat access as cluster-admin.
- Support `Authorization: Bearer <token>`.
- Accept token from explicit config or `MANTISSA_REST_TOKEN`.
- Allow unauthenticated mode only behind an explicit dev flag.
- Do not expose secrets through browser-friendly CORS by default.

This keeps the security model simple and avoids accidentally turning the local
admin API into a remotely reachable control plane.

## Crate Dependencies

Add new dependencies only to `crates/mantissa-rest` first:

- `axum` for routing and extractors.
- `tower-http` only if needed for tracing or compression.
- `serde` and `serde_json` for JSON types.
- `tokio` for async runtime, channels, and local tasks.
- `uuid` with `serde` for public UUID strings.
- `anyhow` or `thiserror` for internal errors.
- `mantissa-client` and `mantissa-protocol`.

Do not add these dependencies to the root daemon until embedded REST serving is
actually needed.

## Public REST Data Rules

Do not serialize raw Cap'n Proto JSON shapes.

Use explicit REST-facing types with these conventions:

- IDs are UUID strings, not raw 16-byte arrays.
- Opaque binary data is base64 only when it truly must be exposed.
- Timestamps stay RFC3339 strings until the internal API provides typed time.
- Empty protocol strings that mean "unset" become `null` in JSON where useful.
- Enums are lowercase snake-case strings.
- Request types should use `deny_unknown_fields` once the surface stabilizes.

Example:

```json
{
  "id": "ab8d4db5-c4cb-4f18-a97d-9fbefb457163",
  "name": "api",
  "status": "running"
}
```

## Route Scope

Use `/v1` from the beginning. This does not promise external stability, but it
keeps route evolution explicit.

### Health

- `GET /healthz`
- `GET /v1/health`

These should validate that the gateway is running and can ping the local
Mantissa session.

### Nodes

- `GET /v1/nodes`
- `GET /v1/nodes/{node_id}`
- `POST /v1/nodes/{node_id}/drain`
- `POST /v1/nodes/{node_id}/resume`
- `DELETE /v1/nodes/{node_id}`

Do not expose join token rotation in the first REST pass unless there is a
specific local automation use case.

### Clusters

- `GET /v1/clusters/views`
- `GET /v1/clusters/current`
- `GET /v1/clusters/operations`
- `GET /v1/clusters/operations/{operation_id}`

Cluster split/merge can wait until the read-only cluster routes are stable.

### Services

- `GET /v1/services`
- `POST /v1/services`
- `GET /v1/services/{selector}`
- `GET /v1/services/{selector}/status`
- `DELETE /v1/services/{selector}`

The `POST /v1/services` body should use a JSON type aligned with the existing
service manifest model, not the raw Cap'n Proto `ServiceDeploySpec`.

### Jobs

- `GET /v1/jobs`
- `POST /v1/jobs`
- `GET /v1/jobs/{job_id}`
- `POST /v1/jobs/{job_id}/cancel`
- `DELETE /v1/jobs/{job_id}`

The submit body should mirror first-class job intent. Manifest-backed submission
can be added after raw JSON submission works.

### Agents

- `GET /v1/agents/sessions`
- `POST /v1/agents/sessions`
- `GET /v1/agents/sessions/{session_id}`
- `POST /v1/agents/sessions/{session_id}/input`
- `POST /v1/agents/sessions/{session_id}/cancel`
- `POST /v1/agents/sessions/{session_id}/close`
- `DELETE /v1/agents/sessions/{session_id}`

Agent logs and snapshots can follow the same streaming pattern as task logs.

### Tasks

- `GET /v1/tasks`
- `POST /v1/tasks`
- `GET /v1/tasks/{selector}`
- `POST /v1/tasks/{selector}/stop`
- `GET /v1/tasks/{selector}/logs`
- `POST /v1/tasks/{selector}/exec`

Use a streaming HTTP response or server-sent events for logs. Use WebSocket for
interactive attach and exec after basic logs work.

### Networks

- `GET /v1/networks`
- `POST /v1/networks`
- `GET /v1/networks/{network_id}`
- `GET /v1/networks/{network_id}/peers`
- `GET /v1/networks/{network_id}/attachments`
- `DELETE /v1/networks/{network_id}`

### Volumes

- `GET /v1/volumes`
- `POST /v1/volumes`
- `POST /v1/volumes/import`
- `GET /v1/volumes/{selector}`
- `GET /v1/volumes/{selector}/status`
- `DELETE /v1/volumes/{selector}`

### Secrets

- `GET /v1/secrets`
- `POST /v1/secrets`
- `PUT /v1/secrets/{name}`
- `GET /v1/secrets/{name}`
- `GET /v1/secrets/{name}/versions/{version_id}`
- `DELETE /v1/secrets/{name}`

Secrets expose sensitive values. Keep auth mandatory for these routes and do
not add permissive CORS.

### Scheduler

- `GET /v1/scheduler/summary`

Do not expose lease prepare, commit, or abort over REST initially. Those are
internal distributed scheduling primitives.

## Error Mapping

Create one `RestError` type implementing `IntoResponse`.

Initial mapping:

- `400 Bad Request`: invalid JSON, bad UUID, invalid query option.
- `401 Unauthorized`: missing or invalid bearer token.
- `403 Forbidden`: route disabled by config or non-loopback bind without auth.
- `404 Not Found`: missing resource when the client layer can identify it.
- `409 Conflict`: duplicate or conflicting desired state.
- `422 Unprocessable Entity`: valid JSON with invalid domain intent.
- `503 Service Unavailable`: local daemon socket missing, refused, or session
  revoked.
- `500 Internal Server Error`: unexpected gateway or conversion failure.

Cap'n Proto errors are currently mostly stringly typed. The first version can
classify obvious local socket failures through `ClientSocketError` and return
other protocol failures as `500` or `422` depending on context. A later pass can
improve `mantissa-client` domain errors.

## Implementation Phases

### Phase 0: Planning

Status: current step.

Outputs:

- This note.
- User ACK before implementation.

### Phase 1: Crate Skeleton And Server

Tasks:

- Add `crates/mantissa-rest` to the workspace.
- Add `Cargo.toml` and module skeleton.
- Add `RestConfig`, `RestAuthConfig`, `RestError`, and `AppState`.
- Add Axum router with `/healthz` and `/v1/health`.
- Add bearer-token middleware or extractor.
- Add the local Cap'n Proto `ClientWorker` skeleton.
- Add unit tests for auth and health error mapping.

Completion criteria:

- `cargo fmt --all` passes.
- `cargo clippy -p mantissa-rest --all-targets -- -D warnings` passes.
- `cargo test -p mantissa-rest` passes.
- Running the binary can bind to loopback and return health status.

### Phase 2: Read-Only Operator Endpoints

Start with low-risk read paths.

Tasks:

- Add types and routes for nodes list/info.
- Add jobs list/inspect.
- Add services list/inspect/status.
- Add networks list/inspect.
- Add volumes list/get/status.
- Add scheduler summary.
- Add missing typed read functions to `mantissa-client` when CLI-only code is
  currently doing protocol decoding inline.

Completion criteria:

- All read routes return owned JSON types.
- No handler returns or stores Cap'n Proto readers/builders.
- Route tests cover success and representative error cases.

### Phase 3: Mutating Resource Endpoints

Tasks:

- Add service deploy/delete.
- Add job submit/cancel/delete.
- Add task start/stop.
- Add network create/delete.
- Add volume create/import/delete.
- Add secret create/update/delete/get.
- Add node drain/resume/evict.
- Keep every route behind bearer auth.

Completion criteria:

- Mutating routes are deterministic and idempotent where the domain supports it.
- Domain validation happens before calling Cap'n Proto where reasonable.
- Errors map to stable JSON error bodies.

### Phase 4: Streaming

Tasks:

- Implement `GET /v1/tasks/{selector}/logs`.
- Bridge Cap'n Proto `TaskLogSink` into an HTTP body stream.
- Start with non-interactive log streaming before attach/exec.
- Add WebSocket support for `exec` only after log streaming is correct.
- Preserve backpressure through bounded channels.

Completion criteria:

- A log stream closes cleanly on task completion.
- Client disconnect cancels the Cap'n Proto sink bridge.
- Follow mode does not leak tasks after disconnect.

### Phase 5: CLI Or Daemon Integration

Pick one of these after the standalone gateway works:

Option A, separate binary:

- Add `mantissa-rest` binary.
- Run with `mantissa-rest serve`.
- Keep process lifecycle independent from the daemon.

Option B, daemon subcommand/listener:

- Add an optional REST listener to the main daemon.
- Reuse the `mantissa-rest` crate.
- Provide an embedded backend that receives local service clients directly.

Recommendation:

- Start with Option A.
- Add Option B only when there is a concrete operational reason.

### Phase 6: Documentation And Examples

Tasks:

- Add docs under `docs/rest-api.md`.
- Document trust boundary and loopback-only default.
- Document token configuration.
- Add curl examples for health, jobs, services, and logs.
- Document that REST is a local convenience facade over the Cap'n Proto local
  admin session.

Completion criteria:

- Docs explain what is and is not exposed.
- Docs include examples that can be run against a local daemon.

## Testing Strategy

Unit tests:

- JSON type serialization and deserialization.
- UUID and enum conversion.
- Auth extractor/middleware.
- Error response bodies.

Route tests:

- Use a mock worker for fast HTTP behavior tests.
- Avoid requiring a real daemon for simple route coverage.

Integration tests:

- Add root-level tests only when a route needs real daemon behavior.
- Use existing testkit/headless helpers where possible.
- Avoid arbitrary sleeps.
- Do not run multiple `cargo test` processes concurrently.

Manual checks:

- Start a local daemon.
- Start the REST gateway on loopback.
- `curl /healthz`.
- Submit/list/inspect/cancel one job.
- Deploy/list/inspect/delete one service.
- Stream logs from a short-lived task.

Required before marking implementation complete:

- `cargo fmt --all`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`

## First Implementation Slice After ACK

After ACK, implement only Phase 1 unless you explicitly ask for a larger first
slice.

Expected first code change:

- New `crates/mantissa-rest` workspace member.
- `/healthz` and `/v1/health`.
- Auth plumbing.
- Local client worker skeleton.
- Tests for auth/error behavior.

This creates the foundation without committing to the full endpoint surface in
one large change.

## Risks

Cap'n Proto client `Send` boundaries:

- Mitigation: keep RPC calls inside `ClientWorker` running on a LocalSet.

Stringly typed domain errors:

- Mitigation: start with conservative HTTP status mapping and improve
  `mantissa-client` typed errors incrementally.

REST type drift from CLI/domain behavior:

- Mitigation: add reusable typed functions to `mantissa-client` and share them
  between CLI and REST.

Accidental public exposure:

- Mitigation: loopback default, auth required for non-loopback, CORS disabled,
  explicit docs that this is cluster-admin.

Streaming lifecycle leaks:

- Mitigation: bounded channels, cancellation on client disconnect, focused
  tests for log stream shutdown.

## ACK Checkpoint

Implementation should not begin until the user ACKs this plan.

When ACKed, proceed with Phase 1 and keep changes scoped to the new crate and
the minimum workspace wiring needed to build it.
