# Local REST API

Mantissa exposes an optional local REST facade for programs that want ordinary
HTTP and JSON while the real node protocol remains Cap'n Proto.

The REST gateway is a local cluster-admin convenience API. It connects to the
same local daemon session as the CLI, so anyone who can reach it has the same
effective authority as a local Mantissa client.

## Trust Boundary

Default behavior is deliberately narrow:

- bind to `127.0.0.1:6579`;
- use the normal local daemon socket discovery path;
- require bearer auth with a daemon-owned local REST token;
- do not enable browser CORS.

Do not expose this listener directly to the public internet. If a program needs
remote access, put an authenticated local transport boundary in front of it
such as SSH forwarding, a private WireGuard link, or a host-local sidecar.

Secrets routes can return decrypted secret payloads as base64. Treat REST access
as cluster-admin access.

## Start The Daemon With REST

Start Mantissa with the embedded REST listener enabled:

```bash
mantissa init --detach --rest
```

The same setting can be environment-only:

```bash
MANTISSA_REST_ENABLED=true mantissa init --detach
```

The daemon generates and persists the REST token when REST support is enabled.
The token is local HTTP bearer auth for this facade. It is separate from the
join token used for cluster membership.

Show or rotate the token through the local client API:

```bash
mantissa rest token show
mantissa rest token rotate
```

For development, the standalone REST wrapper still exists and connects to an
already-running local daemon:

```bash
mantissa init --detach
cargo run -p mantissa-rest -- serve
```

Embedded REST can also be enabled with CLI flags:

```bash
mantissa init --detach \
  --rest \
  --rest-addr 127.0.0.1:6579
```

Configuration:

| Variable | Meaning |
| --- | --- |
| `MANTISSA_REST_ENABLED` | Start embedded REST from `mantissa init`. |
| `MANTISSA_REST_ADDR` | Bind address, default `127.0.0.1:6579`. |
| `MANTISSA_REST_SOCKET` | Optional daemon Unix socket path override. |

The equivalent `mantissa init` flags are `--rest` and `--rest-addr`.

Use this shell helper for examples:

```bash
TOKEN="$(mantissa rest token show)"
REST=http://127.0.0.1:6579
AUTH=(-H "Authorization: Bearer $TOKEN")
```

## Response Rules

REST response bodies use explicit JSON types, not raw Cap'n Proto JSON shapes.

- IDs are UUID strings.
- Enums and states are lowercase strings where practical.
- Binary payloads are base64.
- Empty optional protocol strings become JSON `null` where useful.
- Errors use a stable body:

```json
{
  "code": "bad_request",
  "message": "invalid task selector"
}
```

Malformed JSON, missing JSON content types, and unknown request fields return
this same error envelope.

Task log streaming uses newline-delimited JSON with content type
`application/x-ndjson`.

Task attach and exec use WebSocket JSON text frames. Binary WebSocket messages
sent by the client are treated as raw stdin bytes.

## Health

Check that the HTTP process is alive:

```bash
curl -sS "$REST/healthz"
```

Check that the gateway can authenticate and ping the local daemon:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/health"
```

## Nodes

List nodes:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/nodes"
curl -sS "${AUTH[@]}" "$REST/v1/nodes/$NODE_ID"
```

Inspect drain status, update labels, drain, resume, and evict a node:

```bash
NODE_ID=00000000-0000-0000-0000-000000000000

curl -sS "${AUTH[@]}" "$REST/v1/nodes/$NODE_ID/drain"

curl -sS -X PUT "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"labels":["role=worker","zone=west"],"remove":[],"replace":true}' \
  "$REST/v1/nodes/$NODE_ID/labels"

curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"reason":"maintenance","task_stop_timeout_secs":30}' \
  "$REST/v1/nodes/$NODE_ID/drain"

curl -sS -X POST "${AUTH[@]}" "$REST/v1/nodes/$NODE_ID/resume"
curl -sS -X DELETE "${AUTH[@]}" "$REST/v1/nodes/$NODE_ID"
```

## Clusters

List cluster lineages, raw views, and the local active view:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/clusters"
curl -sS "${AUTH[@]}" "$REST/v1/clusters/views"
curl -sS "${AUTH[@]}" "$REST/v1/clusters/current"
```

Show split candidates for the active view or a selected cluster lineage:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/clusters/split-candidates"
curl -sS "${AUTH[@]}" "$REST/v1/clusters/$CLUSTER_ID/split-candidates"
```

Fetch a known cluster operation:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/clusters/operations/$OPERATION_ID"
```

The protocol currently exposes operation lookup by id, not a retained operation
list route. REST mirrors that instead of inventing a registry read path.

## Jobs

Submit a finite job:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "manifest": {
      "name": "hello-job",
      "execution_platform": "oci",
      "isolation_mode": "standard",
      "execution": {
        "image": "alpine:3.20",
        "command": ["sh", "-lc", "echo hello from REST"],
        "resources": {
          "cpu_millis": 250,
          "memory_mb": 128
        }
      },
      "retry_policy": {
        "max_retries": 0,
        "backoff_secs": 2
      }
    }
  }' \
  "$REST/v1/jobs"
```

List and inspect jobs:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/jobs"
curl -sS "${AUTH[@]}" "$REST/v1/jobs/$JOB_ID"
```

Cancel or delete a job:

```bash
curl -sS -X POST "${AUTH[@]}" "$REST/v1/jobs/$JOB_ID/cancel"
curl -sS -X DELETE "${AUTH[@]}" "$REST/v1/jobs/$JOB_ID"
```

## Services

Deploy a service:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "manifest": {
      "name": "demo-service",
      "tasks": [
        {
          "name": "echo",
          "image": "alpine:3.20",
          "command": [
            "sh",
            "-lc",
            "while true; do echo demo-service; sleep 5; done"
          ],
          "replicas": 2,
          "resources": {
            "cpu_millis": 500,
            "memory_mb": 128
          }
        }
      ]
    }
  }' \
  "$REST/v1/services"
```

List, inspect, and stop services:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/services"
curl -sS "${AUTH[@]}" "$REST/v1/services/demo-service"
curl -sS "${AUTH[@]}" "$REST/v1/services/demo-service/status"
curl -sS -X DELETE "${AUTH[@]}" "$REST/v1/services/demo-service"
```

## Agents

Submit and inspect durable agent sessions:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "manifest": {
      "name": "demo-agent",
      "execution": {
        "image": "ghcr.io/mantissa/demo-agent:latest",
        "resources": {
          "cpu_millis": 250,
          "memory_mb": 128
        }
      }
    }
  }' \
  "$REST/v1/agents/sessions"

curl -sS "${AUTH[@]}" "$REST/v1/agents/sessions"
curl -sS "${AUTH[@]}" "$REST/v1/agents/sessions/$SESSION_ID"
curl -sS "${AUTH[@]}" "$REST/v1/agents/sessions/$SESSION_ID/runs"
```

Send input, cancel, close, or delete a session:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"input":"continue"}' \
  "$REST/v1/agents/sessions/$SESSION_ID/input"

curl -sS -X POST "${AUTH[@]}" \
  "$REST/v1/agents/sessions/$SESSION_ID/cancel"

curl -sS -X POST "${AUTH[@]}" \
  "$REST/v1/agents/sessions/$SESSION_ID/close"

curl -sS -X DELETE "${AUTH[@]}" \
  "$REST/v1/agents/sessions/$SESSION_ID"
```

## Tasks, Logs, Attach, And Exec

Start a standalone task:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sleepy",
    "image": "alpine:3.20",
    "command": ["sh", "-lc", "while true; do echo tick; sleep 1; done"],
    "cpu_millis": 250,
    "memory_bytes": 134217728
  }' \
  "$REST/v1/tasks"
```

Stream logs:

```bash
curl -N -sS "${AUTH[@]}" \
  "$REST/v1/tasks/sleepy/logs?follow=true&tail=10"
```

Each line is one JSON event:

```json
{"type":"frame","stream":"stdout","data_base64":"dGljaw0K"}
```

Decode `data_base64` to recover the original bytes. `stream` is one of
`stdout`, `stderr`, or `console`.

Attach and exec use WebSocket upgrade requests:

```bash
WS_AUTH="Authorization: Bearer $TOKEN"

websocat -H "$WS_AUTH" \
  "ws://127.0.0.1:6579/v1/tasks/sleepy/attach?stdin=true&stdout=true&stderr=true"

websocat -H "$WS_AUTH" \
  "ws://127.0.0.1:6579/v1/tasks/sleepy/exec?command=sh&command=-lc&command=id"
```

Client-to-server text frames:

```json
{"type":"input","data_base64":"ZWNobyBoaQo="}
```

```json
{"type":"close_input"}
```

Server-to-client text frames:

```json
{"type":"frame","stream":"stdout","data_base64":"aGkK"}
```

```json
{"type":"result","has_exit_code":true,"exit_code":0}
```

```json
{"type":"end"}
```

```json
{"type":"error","message":"task stream session is closed"}
```

Exec sockets close after both the output stream has ended and the result or
terminal error has been sent. Attach sockets close after the output stream ends.

Stop the task:

```bash
curl -sS -X POST "${AUTH[@]}" "$REST/v1/tasks/sleepy/stop"
```

## Networks, Volumes, And Secrets

Create a network:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"name":"demo-overlay","driver":"vxlan","subnet_cidr":"10.240.0.0/16"}' \
  "$REST/v1/networks"
```

Inspect network peers and workload attachments:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/networks"
curl -sS "${AUTH[@]}" "$REST/v1/networks/$NETWORK_ID"
curl -sS "${AUTH[@]}" "$REST/v1/networks/$NETWORK_ID/peers"
curl -sS "${AUTH[@]}" "$REST/v1/networks/$NETWORK_ID/attachments"
curl -sS -X DELETE "${AUTH[@]}" "$REST/v1/networks/$NETWORK_ID"
```

Create a managed local volume:

```bash
curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d '{"name":"demo-data","requested_bytes":1073741824}' \
  "$REST/v1/volumes"
```

Create a secret:

```bash
SECRET_VALUE="$(printf 's3cr3t' | base64)"

curl -sS "${AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"demo-token\",\"plaintext_base64\":\"$SECRET_VALUE\"}" \
  "$REST/v1/secrets"
```

Fetch the current secret version:

```bash
curl -sS "${AUTH[@]}" "$REST/v1/secrets/demo-token"
```

## Current Scope

Exposed now:

- health;
- nodes list/get/drain-status/labels/drain/resume/evict;
- clusters list/views/current/split-candidates/operation lookup;
- agents session and run lifecycle;
- jobs list/submit/get/cancel/delete;
- services list/deploy/get/status/delete;
- networks list/create/get/peers/attachments/delete;
- volumes list/create/import/get/status/delete;
- tasks list/start/get/logs/attach/exec/stop;
- secrets list/create/update/get/delete;
- scheduler summary.

Not exposed:

- node-to-node gossip or anti-entropy internals;
- scheduler lease prepare/commit/abort;
- peer bootstrap or join-token rotation;
- cluster operation listing without an operation id;
- public internet API guarantees;
- fine-grained RBAC.

REST is a convenience facade over the Cap'n Proto local admin session. Keep
Cap'n Proto as the internal protocol and add reusable typed client functions
when a new REST route needs behavior that is currently CLI-only.
