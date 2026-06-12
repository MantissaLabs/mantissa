# Local REST API Facade Completion Status

## Goal

Finish the local, typed REST API facade so it is a credible local admin
convenience layer over the existing Cap'n Proto session.

REST remains a local cluster-admin interface. It is not a public internet API,
not a node-to-node protocol, and not a generated projection of Cap'n Proto.

## Completed Scope

The REST facade now has a real local-session integration harness and covers the
main local admin surfaces that were missing from the first implementation:

- agent session lifecycle and run reads;
- authenticated health, node listing, and agent lifecycle integration tests;
- node drain status and label updates;
- network peer and attachment subroutes;
- cluster split-candidate reads and operation lookup by id;
- task attach and exec WebSocket transports with bounded worker bridges;
- docs that describe the registered route surface and stream framing;
- embedded daemon startup through `mantissa init --rest`;
- graceful standalone shutdown on Ctrl-C;
- request completion logs with method, path, status, and latency;
- stable JSON error envelopes for malformed request bodies and unknown fields.

## Route Surface

All `/v1` routes require REST auth. `/healthz` is intentionally unauthenticated.

Implemented groups:

- `GET /v1/health`
- `GET /v1/nodes`
- `GET /v1/nodes/{node_id}`
- `GET /v1/nodes/{node_id}/drain`
- `PUT /v1/nodes/{node_id}/labels`
- `POST /v1/nodes/{node_id}/drain`
- `POST /v1/nodes/{node_id}/resume`
- `DELETE /v1/nodes/{node_id}`
- `GET /v1/agents/sessions`
- `POST /v1/agents/sessions`
- `GET /v1/agents/sessions/{session_id}`
- `DELETE /v1/agents/sessions/{session_id}`
- `GET /v1/agents/sessions/{session_id}/runs`
- `POST /v1/agents/sessions/{session_id}/input`
- `POST /v1/agents/sessions/{session_id}/cancel`
- `POST /v1/agents/sessions/{session_id}/close`
- jobs, services, volumes, secrets, tasks, scheduler, and networks routes
  documented in `docs/rest-api.md`
- `GET /v1/tasks/{selector}/attach` as a WebSocket
- `GET /v1/tasks/{selector}/exec` as a WebSocket
- `GET /v1/clusters`
- `GET /v1/clusters/views`
- `GET /v1/clusters/current`
- `GET /v1/clusters/split-candidates`
- `GET /v1/clusters/{cluster_id}/split-candidates`
- `GET /v1/clusters/operations/{operation_id}`

## Deferred Items

These are intentionally not part of the completed local facade:

- public internet API guarantees;
- browser CORS policy;
- node-to-node gossip or anti-entropy internals;
- scheduler lease prepare, commit, or abort;
- peer bootstrap and join-token rotation routes;
- cluster operation listing without an operation id, because the current
  topology protocol exposes lookup but not a list operation;
- OpenAPI generation;
- fine-grained RBAC.

The remaining engineering gaps are narrower:

- workload start request types are still simpler than full CLI manifests;
- many domain errors still arrive as strings from client helpers and are mapped
  conservatively;
- the integration harness covers network peer and attachment routes after
  network creation, but not deterministic workload-produced attachment rows;
- WebSocket framing is tested at the bridge layer, while full end-to-end task
  attach and exec behavior remains covered by the existing Cap'n Proto protocol
  integration tests.

## Verification

Before marking the goal complete, run:

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
git diff --check
```

Also verify that no cargo, rustc, or test process is left running after the
work completes.
