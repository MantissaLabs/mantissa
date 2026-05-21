# Workloads and Runtime Platforms

Mantissa schedules *workloads*. A workload is the generic schedulable unit in
the control plane, but that does not mean every user-facing concept collapsed
into one bland type. The point of the current structure is the opposite: it
separates lifecycle semantics from execution mechanics.

A direct task, a service replica, a job attempt, and an agent run can all be
scheduled through the same workload machinery while still meaning different
things to the controllers that own them. In the same way, a workload can ask
for OCI-style execution, a sandbox contract, or a MicroVM family without
forcing the rest of the system to treat every backend like Docker.

This document explains that split from two angles. First it describes the
conceptual model: what a task, service, job, agent session, and agent run are
supposed to mean. Then it maps those concepts onto the code layout so it is
clear where the responsibilities live.

## The Mental Model

Mantissa is easiest to understand when read as three layers stacked on top of
each other.

At the top sit the controller-specific semantics. That is where the system
decides whether something is a direct task, a service that should keep a
replica set alive, a job that should run to completion with retries, or an
agent session that may pause for input and later resume.

In the middle sits the shared workload layer. This is where the scheduler,
runtime orchestration, attachment repair, placement, and generic lifecycle
handling live. It is deliberately indifferent to whether a workload belongs to
a service rollout or to an agent controller.

At the bottom sit runtime backends. They answer the practical question of how a
workload instance is created, started, stopped, inspected, and attached to.

```mermaid
flowchart TB
    subgraph Semantics["Public and controller semantics"]
        Task["Task API"]
        Service["Service controller"]
        Job["Job controller"]
        Agent["Agent controller"]
    end

    subgraph Workloads["Shared workload layer"]
        TaskStart["TaskStartRequest"]
        Start["WorkloadStartRequest"]
        Exec["ExecutionSpec"]
        Manager["WorkloadManager"]
    end

    subgraph Runtimes["Runtime backends"]
        Trait["RuntimeBackend"]
        Docker["Docker runtime backend"]
        Memory["In-memory runtime backend"]
        Future["Future MicroVM backend"]
    end

    Task --> TaskStart
    TaskStart --> Start
    Service --> Start
    Job --> Start
    Agent --> Start
    Start --> Exec
    Exec --> Manager
    Manager --> Trait
    Trait --> Docker
    Trait --> Memory
    Trait -.-> Future
```

The important consequence is that names describe different axes:

| Term | What it answers |
| --- | --- |
| `Task`, `ServiceReplica`, `JobAttempt`, `AgentRun` | Which schedulable workload row this is |
| `ExecutionSpec` | How the schedulable execution should run |
| `ExecutionPlatform` | Which runtime family is requested |
| `RuntimeBackend` | Which local engine actually implements the request |

That is why Mantissa talks about `TaskStartRequest` at the public task
boundary but `WorkloadStartRequest` inside the shared layer. The task service
converts the task-shaped request into the generic workload request that is
also reused by services, jobs, and agents.

## Control-Plane Concepts

The control plane contains both durable controller records and schedulable
executions. Those are not the same thing, and most confusion comes from mixing
them together.

### Task

A task is the simplest case: a standalone execution requested directly by the
operator. It has no higher-level controller trying to maintain a replica count
or apply a retry budget. If the task exits, the next action is determined by
its own execution policy and by whatever the operator does next.

Internally, a direct task is represented as a workload with
`WorkloadKind::Task`, but the task name is intentionally kept at the public
surface. The `src/task` module exists as the task-facing compatibility and API
layer over the generic workload model.

### Service and Service Replica

A service is not itself one runtime instance. A service is the durable desired
state owned by the service controller: task templates, replica counts, rollout
strategy, readiness, dependency ordering, and traffic publication rules.

When the service controller wants work to run, it materializes service-owned
replicas. Those replicas are schedulable executions and therefore go through
the shared workload layer. They are not modeled as standalone tasks because
their lifecycle is owned by the service controller rather than by an operator
submitting a single direct task.

The `ServiceReplica` workload kind is therefore about ownership semantics, not
about a different execution mechanism. A service replica may use the same
runtime family and the same execution template as a direct task, but it is
reconciled as part of a service rollout.

At the shared workload-row level, this ownership is carried as one exclusive
owner value. A direct task has no owner. A service replica, job attempt, or
agent run stores exactly one owner variant, which keeps the shared workload
model from representing impossible combinations such as "service-owned and
job-owned at the same time".

### Job

A job is a controller-level record for finite work. It owns retry and
completion semantics above the runtime layer. A job may launch multiple
underlying workload attempts over time, but those attempts still reuse the
shared execution platform and are recorded as `WorkloadKind::JobAttempt`.

This is why a job is not simply another name for a service. A service wants to
keep a desired replica set alive and routable. A job wants to produce a
terminal success or failure. Those are different control-plane problems even if
both are implemented by creating lower-level workloads.

The job record links to those workload attempts through workload-oriented
fields such as `active_workload_id`, `last_workload_id`, and
`successful_workload_id`. Those identifiers point to shared workload rows, not
to a separate job-specific execution store.

For the operator-facing jobs surface, manifest format, and day-to-day job
commands, see [docs/jobs.md](/Users/abronan/hack/mantissa/docs/jobs.md).

### Agent Session and Agent Run

Agents are deliberately split in two.

An `AgentSession` is the durable control-plane object. It owns workspace
policy, tool policy, checkpoint policy, interaction policy, deployment
deadlines, queued input, and recent structured event history. It may exist while
consuming no runtime capacity at all.

An `AgentRun` is the schedulable execution slice created from that session. It
is the thing that actually turns into an underlying workload and then into a
runtime instance, and it is recorded in the shared workload layer as
`WorkloadKind::AgentRun`. This split lets Mantissa keep an idle session durable
without pinning compute, which is important for human-in-the-loop workflows.

Once scheduled, an agent run records the bound shared workload row through its
`workload_id`. The run remains the controller-owned record; the shared workload
row remains the generic schedulable execution beneath it.

### Summary Table

| Object | Role in the system | Does it directly consume runtime capacity? | Primary code |
| --- | --- | --- | --- |
| Task | Standalone user-submitted execution | Yes | `src/task`, `src/workload` |
| Service | Desired state for replicated long-lived work | No | `src/services` |
| Service replica | One schedulable execution owned by a service | Yes | `src/services`, `src/workload` |
| Job | Durable finite-work controller with retries | No by itself; it launches attempts | `src/jobs` |
| Agent session | Durable agent identity, policy, and event record | No | `src/agents` |
| Agent run | One schedulable execution slice of an agent session | Yes | `src/agents`, `src/workload` |

## The Shared Execution Shape

The shared execution shape lives in `ExecutionSpec`. This type answers
the narrow question, "if something is scheduled, how should it execute?" It is
not supposed to answer higher-level questions such as "when is the rollout
healthy?" or "how many times should a failed attempt be retried?"

That boundary matters because it prevents the scheduler and runtime layer from
accumulating controller-specific policy. A service replica, a job attempt, and
an agent run can all reuse the same execution template while differing
completely in how the higher-level controller reacts to success, failure, or
missing readiness.

The execution template carries runtime-local concerns such as image, command,
TTY behavior, CPU and memory requests, GPU count, restart policy, termination
grace period, pre-stop hooks, liveness probes, environment variables, secret
files, volume mounts, and networks.

By contrast, the following policy stays above the execution layer:

| Shared execution concerns | Controller-owned concerns |
| --- | --- |
| Image and command | Service replica counts and rollout strategy |
| CPU, memory, GPU, TTY | Service readiness and dependency ordering |
| Restart policy and liveness | Job completion and retry policy |
| Volumes, networks, env, secret files | Agent workspace, tools, checkpoints, and interaction rules |

This is the key reason the code now prefers `ExecutionSpec` over
copying similar launch fields into every controller-specific type.

## Execution Platforms, Isolation, and Backends

Mantissa uses four related ideas that are easy to conflate: execution
platform, isolation mode, runtime backend, and isolation profile.

An `ExecutionPlatform` is the cluster-visible family requested by a workload.
Today the shared model recognizes `oci` and `microvm`.

An `IsolationMode` is the higher-level isolation contract requested by a
workload. Today the shared model recognizes `standard` and `sandboxed`.

A `RuntimeBackend` is the local implementation that actually performs runtime
operations. It is responsible for create, start, stop, inspect, attach, log
streaming, exec, and event watching where supported.

An isolation profile is an optional named policy exposed to the scheduler and
the runtime support profile. It is not the same thing as a physical
platform.

### OCI, MicroVM, and Sandboxed Isolation

`oci` means container-style execution. The current production backend for that
family is Docker.

`microvm` means a MicroVM-style execution family. The model, planner, and
support profile can express it, but there is no production MicroVM backend in
the tree yet.

`sandboxed` is an isolation contract, not a third platform. A workload that
requests `ExecutionPlatform::Oci` with `IsolationMode::Sandboxed` is asking
for container-backed sandboxing. A workload that requests
`ExecutionPlatform::MicroVm` with `IsolationMode::Sandboxed` is asking for a
MicroVM-backed sandbox. Today only OCI-backed sandboxing is implemented.

That is why "sandbox" and "MicroVM" are not synonyms. A sandbox may be
implemented on top of OCI or on top of a MicroVM backend. The execution
platform expresses where the workload runs; the isolation mode expresses how
strongly isolated that execution should be; the backend expresses how that
request is actually fulfilled on a node.

### Capabilities and Support Profiles

Nodes advertise their runtime support with `RuntimeSupportProfile`. The profile
contains four kinds of information: which execution platforms a node
supports, which isolation modes it exposes, which named isolation profiles it
supports, and which optional runtime features are available.

Those feature flags matter because not every backend supports the same
interaction surface. A backend may support logs but not attach, or exec but not
lifecycle events. The workload manager therefore treats attach, exec, logs, and
lifecycle events as capabilities rather than as universal assumptions.

### Current Runtime Backend Matrix

The runtime model is broader than the set of backends currently implemented.
This table reflects what exists in the repository today.

| Backend | Status | Advertised execution platforms | Isolation modes | Isolation profiles | Exec | Interactive exec | Logs | Attach | Lifecycle events | Primary code |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `docker-standard` backend | Production backend | `oci` | `standard` | `default` | Yes | Yes | Yes | Yes | Yes | `src/runtime/oci/docker/` |
| `docker-sandboxed` backend | Production backend when `nono` helper is available on the host | `oci` | `sandboxed` | `oci-default`, `nono-default` | Yes | Yes | Yes | Yes | Yes | `src/runtime/oci/docker/` |
| In-memory runtime backend | Test and local harness backend | `oci` | `standard`, `sandboxed` | `default`, `oci-default` | Yes | Yes | Yes | No | No | `src/runtime/testing/in_memory.rs` |
| MicroVM backend | Not implemented yet | None in code today | None in code today | None in code today | N/A | N/A | N/A | N/A | N/A | Not present |

The important reading of this table is that support is advertised per backend,
not implied by the shared model. The presence of `ExecutionPlatform::MicroVm` in the
model means the scheduler and runtime APIs can express that family; it does not
mean a MicroVM engine is already wired into the runtime layer. It also means a
node only advertises `docker-sandboxed` when it can actually honor that
contract. If the `nono` helper cannot be resolved at startup, the node keeps
`docker-standard` and does not publish sandboxed OCI support.

### Sandboxed Agents with `nono`

The first real `nono` integration target is agent runs. An agent session can
request `ExecutionPlatform::Oci`, `IsolationMode::Sandboxed`, and the
`nono-default` isolation profile without changing the higher-level agent
controller model.

The split of responsibilities is deliberate:

| Agent controller still owns | Runtime sandbox now owns |
| --- | --- |
| Allowed tools, workspace policy, checkpoint policy, interaction rules | Filesystem grants, working directory, and network enforcement |

At launch time the workload manager translates the persisted agent policy into
`RuntimeSandboxPolicy`. In practice that means:

- `allow_network = false` becomes a blocked runtime network policy.
- `allow_write` widens access only where Mantissa intends it to: the working
  directory, writable mounts, `/tmp`, and `/var/tmp`.
- Secret files, workspace mounts, and checkpoint mounts become explicit
  filesystem grants instead of remaining env-only hints.

The sandboxed Docker backend keeps using normal Docker create, start, and exec
operations. It does not introduce a Docker runtime shim. Instead it bind-mounts
`mantissa-sandbox-init` into the container, passes the serialized
`RuntimeSandboxPolicy` through `MANTISSA_SANDBOX_POLICY`, and re-enters through
the same helper on later `docker exec` calls.

Helper discovery is intentionally simple. Mantissa looks for
`mantissa-sandbox-init` next to the main executable by default, and
`MANTISSA_SANDBOX_HELPER_PATH` overrides the host-side path when packaging or local
development needs something different. When you build Mantissa from the
repository root with a plain `cargo build`, the workspace default members now
emit both binaries into the shared `target/<profile>/` directory so the helper
is colocated with the daemon automatically.

## Networking Is Runtime-Neutral

The networking layer no longer assumes that every schedulable execution is "an
OCI container identified by a PID". Instead it consumes a runtime attachment target
published by the backend.

Today the shared runtime model supports three attachment target forms:

| Attachment target | Typical use |
| --- | --- |
| `NetworkNamespacePid` | OCI-style process namespace attachment |
| `NetworkNamespacePath` | Backends that expose a network namespace path directly |
| `TapDevice` | Backends that wire guest networking through a tap device, such as a MicroVM design |

This is the piece that lets OCI backends and future MicroVM-style backends share
the same attachment orchestration without pretending they expose identical
network primitives.

## How Scheduling Fits Together

The shared workload manager is where controller-specific requests become
schedulable executions and then runtime instances.

The general flow is:

```mermaid
sequenceDiagram
    participant API as API or controller
    participant WM as WorkloadManager
    participant SCH as Scheduler
    participant RT as RuntimeBackend
    participant NET as Network attachment layer

    API->>WM: submit WorkloadStartRequest
    WM->>SCH: choose node and reserve capacity
    WM->>RT: create_instance / start_instance
    RT-->>WM: RuntimeInfo and runtime id
    WM->>NET: provision runtime attachment target
    WM-->>API: controller-specific state update
```

The public surface may still speak in controller-specific terms. For example,
the task API sends a `TaskStartRequest`, and the task service converts it into
a `WorkloadStartRequest` before handing it to the shared workload manager. It
still returns a `TaskSpec` because the caller is creating a direct task. The
internal request shape is generic; the resulting durable record is exposed
through the task surface.

The service controller builds service-owned replica requests, the job
controller reserves and observes workload attempts, and the agent controller
creates runs from durable sessions. All of them eventually pass through the
same workload manager and runtime backend contract.

## Code Structure

The code layout follows the same conceptual split.

```mermaid
flowchart LR
    subgraph Surfaces["User and protocol surfaces"]
        Client["CLI and RPC surfaces"]
        Task["src/task"]
        Services["src/services"]
        Jobs["src/jobs"]
        Agents["src/agents"]
    end

    subgraph Shared["Shared workload layer"]
        Model["src/workload/model.rs"]
        Types["src/workload/types.rs"]
        Manager["src/workload/manager/"]
    end

    subgraph Runtime["Runtime abstraction and backends"]
        RuntimeTypes["src/runtime/types.rs"]
        Docker["src/runtime/oci/docker/"]
        Memory["src/runtime/testing/in_memory.rs"]
    end

    subgraph Support["Supporting subsystems"]
        Scheduler["src/scheduler"]
        Network["src/network"]
        Volumes["src/volumes"]
        Secrets["src/secrets"]
        Topology["src/topology"]
    end

    Client --> Task
    Client --> Services
    Client --> Jobs
    Client --> Agents
    Task --> Model
    Task --> Types
    Services --> Manager
    Jobs --> Manager
    Agents --> Manager
    Manager --> RuntimeTypes
    RuntimeTypes --> Docker
    RuntimeTypes --> Memory
    Manager --> Scheduler
    Manager --> Network
    Manager --> Volumes
    Manager --> Secrets
    Manager --> Topology
```

### `src/workload`

`src/workload/model.rs` defines the generic model and terminology: workload
kind, execution platform, isolation mode, workload identity, workload phases,
generic state filters,
and the shared durable workload structures.

`src/workload/types.rs` defines the shared execution-side types such as
`ExecutionSpec`, restart policy, and liveness probe.

`src/workload/manager/` is the shared orchestration engine. This is where
placement, reconciliation, runtime adoption, attachment repair, local runtime
inventory handling, public task start adaptation, and internal workload launch
requests are implemented.

### `src/task`

`src/task` is the task-facing compatibility surface. It exists so the `tasks`
API remains a first-class operator interface while the generic orchestration
core lives in `src/workload`.

The types in `src/task/types.rs` are standalone-task projections and helpers
over the generic workload model rather than a second independent task
orchestration stack.

### `src/services`

`src/services` owns service desired state, rollout progression, dependency
ordering, readiness, and traffic publication. It does not own the generic
runtime lifecycle. When it needs schedulable work to exist, it builds
service-owned replica start requests and hands them to the workload layer.

### `src/jobs`

`src/jobs` owns finite completion-oriented controller state. The job controller
tracks attempts, retry windows, and terminal success or failure. The actual
attempts still reuse the shared workload execution platform.

### `src/agents`

`src/agents` owns durable agent sessions and runs. It is intentionally
session-oriented: sessions are durable control-plane records, while runs are
the schedulable executions that consume capacity.

The current default for agent sessions and runs is `oci` plus
`sandboxed` isolation, which matches their need for stronger isolation and
explicit interaction policy. That default is a controller choice, not a
special case in the shared workload manager.

Agent manifests accept the same top-level `deployment` defaults as services and
jobs. The agent controller uses `progress_deadline_secs` while a run is queued
before workload launch and `healthy_deadline_secs` while the launched workload
is still bootstrapping. See
[docs/deployment-deadlines.md](/Users/abronan/hack/mantissa/docs/deployment-deadlines.md).

### `src/runtime`

`src/runtime/types.rs` defines the runtime-neutral contract: runtime create
requests, runtime info, capabilities, support profiles, runtime events, and
the `RuntimeBackend` trait itself.

`src/runtime/oci/docker/` contains the Docker-backed implementation of that
contract. The module is split by responsibility into runtime operations,
interactive attach and exec handling, image pull helpers, conversions, and
backend tests.

`src/runtime/testing/in_memory.rs` contains the in-memory backend used by the
test harness and local synthetic execution paths.

## Practical Terminology Guide

In everyday conversation inside this repository, the following shortcuts are
useful:

| If you mean... | Prefer this term |
| --- | --- |
| A standalone user-submitted execution | Task |
| The generic schedulable layer beneath tasks, replicas, jobs, and runs | Workload |
| One service-owned schedulable execution | Service replica |
| The durable finite-work controller record | Job |
| The durable agent record that can wait for input | Agent session |
| The schedulable execution created from an agent session | Agent run |
| The family requested by the workload (`oci` or `microvm`) | Execution platform |
| The requested isolation contract (`standard` or `sandboxed`) | Isolation mode |
| The local engine implementation | Runtime backend |
| An optional named isolation policy such as `oci-default` | Isolation profile |
| A higher-level isolation request that may be implemented by OCI or MicroVM backends | Sandboxed isolation |

The model becomes much easier to reason about once those terms are kept on
their own axes. A service replica can be task-shaped in execution terms
without being a direct task. An agent run can use sandboxed isolation
without the sandbox itself being the agent. A future MicroVM backend can
support sandboxed isolation without changing the job or service controller
logic.

That separation is the main organizing principle of the current workload and
runtime design.
