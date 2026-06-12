# Local REST API Facade Completion Plan

## Goal

Finish the local, typed REST API facade so it is a credible local admin
convenience layer over the existing Cap'n Proto session.

The baseline crate, loopback gateway, auth, client worker, health routes, and
several resource routes already exist. This plan only tracks remaining work.
Do not reimplement completed foundation work unless a later step finds a bug.

REST remains a local cluster-admin interface. It is not a public internet API,
not a node-to-node protocol, and not a generated projection of Cap'n Proto.

## Ground Rules

- Keep Cap'n Proto as the real node and client protocol.
- Keep `mantissa-rest` as a manual, typed facade with explicit JSON types.
- Add reusable typed helpers to `mantissa-client` when REST needs behavior that
  is currently only available through CLI-local protocol code.
- Do not expose gossip, anti-entropy sync, internal workload mutation, or
  scheduler lease prepare/commit/abort over REST.
- Keep every `/v1` route authenticated except `/healthz`.
- Keep CORS disabled unless a concrete local-browser use case is accepted.
- Use "types" for REST JSON structs and request/response objects.
- When commits are requested, use the repository style from `master`: subject
  line plus wrapped explanatory paragraphs, with body lines no longer than 80
  columns.

## Current Missing Work

### Agents

The agent modules currently exist only as placeholders. Implement the REST
surface for first-class agent sessions and runs.

Routes:

- `GET /v1/agents/sessions`
- `POST /v1/agents/sessions`
- `GET /v1/agents/sessions/{session_id}`
- `GET /v1/agents/sessions/{session_id}/runs`
- `POST /v1/agents/sessions/{session_id}/input`
- `POST /v1/agents/sessions/{session_id}/cancel`
- `POST /v1/agents/sessions/{session_id}/close`
- `DELETE /v1/agents/sessions/{session_id}`

Tasks:

- Add REST-facing agent session and run types.
- Add request types for session submission and input submission.
- Add client worker commands for every agent operation.
- Add or reuse `mantissa-client` agent helpers rather than decoding protocol
  responses directly in route handlers.
- Decide whether agent session submission should accept the same manifest shape
  used by the CLI or a narrower first-class JSON request. Prefer reusing the
  manifest normalization path when it already exists.
- Map closed, active, missing, and invalid session states to stable REST errors.

Completion criteria:

- Agent REST routes cover the same public operations as `Agents` Cap'n Proto.
- Agent routes have unit tests for auth, validation, and not-found behavior.
- At least one integration test submits, inspects, sends input, closes, and
  deletes an agent session through REST.

### Integration Tests

There is no root-level REST integration suite. Add one that exercises HTTP
against a real local test node.

Tasks:

- Add `tests/rest_api.rs` or a small `tests/rest_api/` module tree.
- Reuse the existing `TestNode` harness.
- Start an Axum listener on an ephemeral loopback port, or call the router
  directly when a real TCP listener is not required.
- Use a real `ClientWorkerHandle` against the test node socket for at least the
  Cap'n Proto integration path.
- Keep route-only tests in `crates/mantissa-rest` for fast worker-mock checks.
- Avoid arbitrary sleeps; use existing convergence helpers.
- Do not run more than one `cargo test` process at a time.

Minimum integration scenarios:

- `/healthz` works without auth.
- `/v1/health` rejects missing auth and succeeds with a valid token.
- Nodes list returns the test node.
- Job submit/list/inspect/cancel/delete works for a small manifest.
- Service deploy/list/status/delete works for a small manifest.
- Task start/list/logs/stop works, including log stream closure.
- Network create/list/inspect/peers/attachments/delete works where attachments
  can be produced deterministically.
- Volume create/list/status/delete works for a managed local volume.
- Secret create/list/get/update/delete works and preserves base64 payload rules.
- Agent session lifecycle works once agent routes are implemented.

Completion criteria:

- The REST facade is exercised through HTTP and the real Cap'n Proto session.
- Integration tests are deterministic on the existing local test harness.
- `cargo test --test rest_api` passes independently.

### Task Attach And Exec

Task logs are implemented, but attach and exec are not exposed over REST.

Routes:

- `POST /v1/tasks/{selector}/exec`
- `POST /v1/tasks/{selector}/attach`

Tasks:

- Pick one streaming transport for interactive stdin/stdout. WebSocket is the
  most practical fit because it supports bidirectional byte streams.
- Define a small framed JSON protocol for stdin, stdout, stderr, console,
  terminal resize, close-input, exit status, and errors.
- Bridge Cap'n Proto `TaskAttachSession` and `TaskExecSession` to the HTTP
  stream without blocking the single client worker command loop.
- Preserve backpressure with bounded channels.
- Cancel the Cap'n Proto session when the HTTP client disconnects.
- Return exec exit status in a terminal frame.

Completion criteria:

- Non-interactive exec works for a command that exits.
- Interactive stdin forwarding works.
- Client disconnect releases the worker-local task.
- Tests cover frame encoding, close-input, exit status, and cancellation.

### Network Peer And Attachment Subroutes

Network inspect embeds peer summaries, but the protocol exposes peer and
attachment listings as distinct operations.

Routes:

- `GET /v1/networks/{network_id}/peers`
- `GET /v1/networks/{network_id}/attachments`

Tasks:

- Add REST-facing `NetworkAttachment` types.
- Add client worker commands for peer status and attachments.
- Add or reuse typed `mantissa-client` helpers for `Networks.peerStatus` and
  `Networks.attachments`.
- Keep `GET /v1/networks/{network_id}` as the compact inspect response.

Completion criteria:

- Peer route returns only peer rows.
- Attachment route returns task, node, assigned IP, MAC, state, traffic
  publication, and service ownership fields.
- Integration coverage verifies attachment rows after a networked workload.

### Cluster Operations And Admin Reads

Cluster view reads exist, but operation visibility is incomplete.

Routes:

- `GET /v1/clusters/operations`
- `GET /v1/clusters/operations/{operation_id}`
- `GET /v1/clusters/split-candidates`

Tasks:

- Confirm whether `mantissa-client` exposes operation listing. If not, add a
  typed helper around the existing topology capability or registry-backed
  protocol path.
- Add REST-facing cluster operation types with status, source view, target
  views, timestamps, and error detail when available.
- Add split-candidate types for local dashboard use.
- Leave split and merge mutation routes out until the read side is stable.

Completion criteria:

- REST can show active and retained cluster operations.
- A split or merge protocol integration test can observe the operation through
  REST.

### Node Admin Completeness

Drain, resume, and evict exist. Useful local admin reads and label mutation are
still missing.

Routes:

- `GET /v1/nodes/{node_id}/drain`
- `PUT /v1/nodes/{node_id}/labels`

Deferred routes, only if accepted later:

- `GET /v1/nodes/join-token`
- `POST /v1/nodes/join-token/rotate`

Tasks:

- Add a drain-status response type.
- Add a label update request that supports set, remove, and replace semantics.
- Reuse topology `getNodeDrainStatus` and `setNodeLabels`.
- Keep join-token routes out unless there is a concrete local automation need.

Completion criteria:

- Operators can see why a drain is blocked through REST.
- Label changes through REST are visible in node list output and scheduling
  constraints after convergence.

### REST Type Coverage

Some current request types are narrower than the underlying domain model. That
is acceptable for an MVP but incomplete for a practical facade.

Tasks:

- Expand standalone task start to support environment variables, secret files,
  networks, placement, liveness, isolation, admission, deadlines, graceful
  termination, and pre-stop commands when supported by the client layer.
- Audit service, job, and agent manifest request coverage against CLI manifest
  behavior.
- Add `deny_unknown_fields` to every stable request type.
- Normalize optional empty strings to `null` in response types where this makes
  the API clearer.
- Keep binary fields base64 encoded.

Completion criteria:

- REST can express the same common workload intent as CLI manifests.
- Missing fields are explicitly documented as unsupported rather than silently
  ignored.

### Error Classification

Many errors still flow through string-based operation failures. Improve this
before treating the API as dependable for automation.

Tasks:

- Add typed error categories to `mantissa-client` where domain operations can
  distinguish invalid input, missing resources, conflicts, and unavailable
  daemon/session failures.
- Map domain categories to `400`, `404`, `409`, `422`, `503`, or `500`.
- Keep error bodies stable:

```json
{
  "code": "not_found",
  "message": "task 'demo' not found"
}
```

- Add tests for representative errors from jobs, services, tasks, networks,
  volumes, secrets, agents, and nodes.

Completion criteria:

- Automation can make decisions from status code and `code`, not from message
  string parsing.
- Cap'n Proto transport failures consistently map to `503`.

### Documentation Accuracy

The REST docs must match what is actually exposed.

Tasks:

- Update `docs/rest-api.md` to include the new missing routes as they land.
- Add a clear "not implemented yet" section until the backlog is complete.
- Document agent lifecycle examples.
- Document task exec and attach framing.
- Document network peers and attachments.
- Document cluster operations and node drain status.
- Keep the local cluster-admin trust boundary explicit.

Completion criteria:

- Docs never claim a route exists before it is registered.
- Curl or WebSocket examples are runnable against a local daemon.

### Operational Readiness

The standalone gateway works, but it lacks the operational polish expected for
regular local use.

Tasks:

- Add graceful shutdown support to the standalone binary.
- Add request tracing with route, method, status, and latency fields.
- Add simple in-process metrics if there is an existing metrics pattern to
  follow. Otherwise defer metrics.
- Confirm non-loopback binding is rejected without auth in integration coverage.
- Add a development example for running the gateway next to a local daemon.
- Decide whether an embedded daemon listener is still needed. Keep it deferred
  unless a concrete deployment workflow requires it.

Completion criteria:

- The gateway shuts down cleanly on process termination.
- Request failures are visible in structured logs.
- Unsafe bind/auth combinations are covered by tests.

### Optional Schema Artifact

This is not required for the first complete local API, but it may be useful for
client generation.

Tasks:

- Decide whether to expose an OpenAPI document generated from the manual Axum
  route/types layer.
- If adopted, use the REST types as the source of truth rather than Cap'n Proto.
- Keep the artifact local-facade scoped and do not imply public API stability.

Completion criteria:

- OpenAPI generation does not introduce large dependencies into the daemon.
- The generated schema matches registered routes.

## Suggested Next Goal Phases

### Phase 1: Agents And Route Coverage

- Implement agent types, routes, worker commands, and client helpers.
- Add focused crate tests.
- Add at least one real REST integration test for agent lifecycle.
- Update docs to list agent routes.

### Phase 2: REST Integration Harness

- Build the root REST integration harness.
- Cover health, auth, nodes, jobs, services, tasks/logs, networks, volumes,
  secrets, and agents.
- Keep tests deterministic and scoped.

### Phase 3: Missing Read Routes

- Add network peers.
- Add network attachments.
- Add cluster operation reads.
- Add split-candidate reads if the client surface supports it cleanly.
- Add node drain status and labels.

### Phase 4: Interactive Task Streams

- Implement task exec.
- Implement task attach if the framing works cleanly after exec.
- Add stream lifecycle and disconnect tests.

### Phase 5: Type And Error Hardening

- Expand workload request coverage.
- Add typed client errors.
- Tighten REST error mapping and request validation.
- Add missing negative tests.

### Phase 6: Docs And Operations

- Correct `docs/rest-api.md` to match the final exposed surface.
- Add graceful shutdown and request tracing.
- Add optional OpenAPI only if it stays lean.

## Verification Before Completion

Run these before the next goal is marked complete:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
git diff --check
```

Also verify that no `cargo`, `rustc`, or test process is left running after the
work completes.

## ACK Checkpoint

This note is the backlog for the next REST completion goal. Do not start
implementation from this plan until the user explicitly ACKs the next goal.
