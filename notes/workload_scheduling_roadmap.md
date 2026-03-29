# Workload Scheduling Roadmap

## Summary

Mantissa should not solve agent sandbox scheduling by bolting `Agent*` fields
onto the current task model or by stretching `task::docker::ContainerManager`
across every future runtime.

The right cut is:

1. extract the shared execution model that already exists in duplicated form
   across tasks and services,
2. isolate Docker behind a generic runtime backend interface,
3. generalize the internal control plane around workloads,
4. keep existing regular task and service behavior intact on top of that
   workload layer,
5. then add first-class jobs and agent sessions as additional controllers,
6. and explicitly remove duplicated and container-only code as each milestone
   lands.

This roadmap is intentionally subtractive first. The first milestones are about
deleting duplication and renaming the right seams before adding jobs or agents.

## Execution Rules

1. Execute milestones strictly in order.
2. Before starting a milestone, update this document and set its status to
   `In progress`.
3. A milestone is not complete until all planned removals/refactors for that
   milestone are done, not just the additions.
4. At the end of every milestone, update this document with:
   - status,
   - date completed,
   - findings or scope adjustments,
   - validation results.
5. At the end of every milestone, run the full validation gate:
   - `cargo fmt --all`
   - `cargo clippy --all-targets -- -D warnings`
   - `cargo test`
6. Do not start the next milestone until the full validation gate passes.
7. Repository rules prohibit me from creating commits directly. At the end of
   each milestone I will stop, present the commit-ready diff summary, and give
   a proposed commit title/body that follows the repository rules for you to
   apply.

## Progress Tracking

Each milestone will be updated in place as work progresses. The expected shape
of the update is:

1. `Status`: `Pending`, `In progress`, or `Completed on <date>`.
2. `Implemented`: what was added or changed.
3. `Removed`: what duplicate, obsolete, or container-only code was deleted.
4. `Findings`: any design correction discovered while implementing the
   milestone.
5. `Validation completed`: the full validation gate results.
6. `Proposed commit`: the exact commit title/body I recommend for you to apply
   before I continue to the next milestone.

## Target Architecture

The end state should separate three concerns that are currently mixed together:

1. controller semantics:
   - regular standalone task,
   - service replica,
   - batch job,
   - agent session or agent run.
2. execution runtime:
   - OCI container runtime,
   - MicroVM runtime,
   - sandbox runtime profile.
3. isolation and policy:
   - network policy,
   - filesystem and workspace policy,
   - tool policy,
   - checkpoint policy,
   - interaction policy.

### Shared execution shape

The following fields should become one shared execution spec reused by tasks,
services, jobs, and agents:

1. runtime selection and runtime payload,
2. CPU, memory, and GPU requests,
3. `tty`,
4. `restart_policy`,
5. `termination_grace_period_secs`,
6. `pre_stop_command`,
7. local `liveness`,
8. `env`,
9. `secret_files`,
10. `volumes`,
11. `networks`.

The following must remain controller-specific and must not be pushed into the
shared execution spec:

1. service-only:
   - `replicas`,
   - `depends_on`,
   - `readiness`,
   - `public_port`,
   - rollout policy.
2. job-only:
   - completion policy,
   - retry/backoff policy,
   - result retention.
3. agent-only:
   - workspace policy,
   - tool policy,
   - checkpoint policy,
   - input wait/resume semantics,
   - model/session metadata.

## Refactor-First Removal And Sharing Targets

### Duplicate launch shape

These locations currently carry near-copies of the same launch shape and should
be collapsed onto one shared execution model:

1. `src/task/manager/mod.rs`
   - `TaskStartRequest`
2. `src/services/types.rs`
   - `ServiceTaskSpecValue`
3. `crates/client/src/services/manifest.rs`
   - `TaskSpec`
4. `src/services/manager.rs`
   - `make_replica_request()` field-by-field translation

### Duplicate restart/liveness codecs

These locations currently duplicate restart policy and liveness probe codec
logic and should be collapsed onto shared helpers:

1. `src/task/service.rs`
2. `src/services/service.rs`
3. `crates/client/src/services/deploy.rs`

The existing shared codec pattern in `src/task/capnp_codec.rs` for environment
variables, secret files, and volume mounts is the correct seed to expand.

### Duplicate in-memory runtimes

These locations currently carry near-identical in-memory runtime
implementations and should be replaced with a single shared runtime-testing
module:

1. `src/task/docker.rs`
2. `tests/common/testkit.rs`

### Container-only vocabulary to remove from generic layers

The following names should not remain in the generic workload/runtime layers by
the end of this roadmap:

1. `ContainerManager`
2. `ContainerCreateRequest`
3. `ContainerInfo`
4. `ContainerRuntimeEvent`
5. `local_containers`
6. `container_id` as generic instance identifier
7. `container_pid` as generic network attachment target
8. top-level `docker.host` as the only runtime config entry

## Milestone 1: Shared Execution Spec And Codec Cleanup

### Goal

Delete current duplication in launch shape, restart policy, and liveness probe
handling before introducing any new workload or agent concepts.

### Status

Completed on 2026-03-29.

### Scope

1. Add `src/workload/mod.rs`.
2. Add `src/workload/types.rs` with the shared execution-side model:
   - `WorkloadExecutionSpec`
   - `WorkloadRestartPolicy`
   - `WorkloadRestartPolicyKind`
   - `WorkloadLivenessProbe`
   - `WorkloadLivenessProbeKind`
3. Refactor `src/task/manager/mod.rs::TaskStartRequest` to carry or embed the
   shared execution spec instead of owning a full copy of those fields.
4. Refactor `src/services/types.rs::ServiceTaskSpecValue` to carry the shared
   execution spec plus service-only fields.
5. Expand `src/task/capnp_codec.rs` or replace it with
   `src/workload/capnp_codec.rs` so it owns shared encoding/decoding for:
   - environment variables,
   - secret files,
   - volume mounts,
   - restart policy,
   - local liveness probes.
6. Update `src/task/service.rs`, `src/services/service.rs`, and
   `crates/client/src/services/deploy.rs` to use the shared codec helpers.

### Removals And Refactors Required

1. Remove duplicate `write_liveness_probe()` and `read_liveness_probe()`
   helpers from:
   - `src/task/service.rs`
   - `src/services/service.rs`
   - `crates/client/src/services/deploy.rs`
2. Remove duplicate restart-policy encode/decode blocks from those same files.
3. Remove `ServiceTaskRestartPolicy` and `ServiceLivenessProbe` if they become
   exact aliases of the shared workload types. If service-specific wrappers are
   temporarily needed inside the milestone, delete them before marking the
   milestone done.
4. Remove the field-by-field service-to-task translation duplication where the
   service controller re-materializes the full runtime launch shape.

### Code Touchpoints

1. `src/workload/mod.rs` new
2. `src/workload/types.rs` new
3. `src/lib.rs`
4. `src/task/capnp_codec.rs` or `src/workload/capnp_codec.rs`
5. `src/task/types.rs`
6. `src/task/manager/mod.rs`
7. `src/task/service.rs`
8. `src/services/types.rs`
9. `src/services/service.rs`
10. `src/services/manager.rs`
11. `crates/client/src/services/manifest.rs`
12. `crates/client/src/services/deploy.rs`

### Exit Criteria

1. Tasks and service templates share one execution-side type.
2. Restart policy and local liveness codecs are centralized.
3. The service controller no longer owns an ad hoc full copy of task launch
   fields.
4. No duplicated liveness/restart codec functions remain in the task/service
   RPC layers.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Implemented

1. Added `src/workload/mod.rs`, `src/workload/types.rs`, and
   `src/workload/capnp_codec.rs`.
2. Added shared execution-side types:
   - `WorkloadExecutionSpec`
   - `WorkloadRestartPolicy`
   - `WorkloadRestartPolicyKind`
   - `WorkloadLivenessProbe`
   - `WorkloadLivenessProbeKind`
3. Replaced the old task/service duplication with shared execution composition:
   - `src/task/manager/mod.rs::TaskStartRequest` now carries
     `TaskExecutionSpec`
   - `src/services/types.rs::ServiceTaskSpecValue` now carries
     `WorkloadExecutionSpec<ServiceTaskNetworkRequirement>`
4. Centralized workload codec helpers in `src/workload/capnp_codec.rs` for:
   - env vars,
   - secret files,
   - volume mounts,
   - restart policies,
   - local liveness probes.
5. Switched task and service RPC code to the shared workload codec:
   - `src/task/service.rs`
   - `src/services/service.rs`
6. Removed service-to-task field rematerialization in
   `src/services/manager.rs::make_replica_request()` by mapping the shared
   execution spec directly.
7. Updated tests and helper builders so service templates and task starts now
   use the shared execution shape instead of the old flat field copy.

### Removed

1. Deleted `src/task/capnp_codec.rs` and moved its shared responsibilities into
   `src/workload/capnp_codec.rs`.
2. Removed duplicate restart/liveness codec helpers from:
   - `src/task/service.rs`
   - `src/services/service.rs`
3. Removed standalone task/service restart and liveness struct duplication by
   aliasing task/service types to the shared workload types in:
   - `src/task/types.rs`
   - `src/services/types.rs`
4. Removed the old flat launch-shape copy from `TaskStartRequest` and
   `ServiceTaskSpecValue`.

### Findings

1. The clean cut was to keep controller-only fields on the outer structs and
   move only runtime-local fields into `WorkloadExecutionSpec`. Trying to keep
   write-through compatibility on the old flat fields would have added more
   code than it removed.
2. `ServiceTaskSpecValue` needed an immutable deref to the execution spec for
   read-side reuse, but mutable call sites in tests were updated explicitly to
   `execution.*` instead of adding a compatibility `DerefMut`.
3. The client manifest layer did not need structural changes in this milestone.
   The useful cut here was server-side consolidation first, then test and RPC
   adoption. Client/runtime-neutral API changes belong in later milestones.

### Validation completed

1. `cargo fmt --all` passed.
2. `cargo clippy --all-targets -- -D warnings` passed.
3. `cargo test` passed.

### Proposed commit

Title:

`workload: extract shared execution spec`

Body:

`Move the duplicated task and service launch fields into a shared`
`WorkloadExecutionSpec and centralize the shared workload Cap'n Proto codec.`
`This removes the repeated restart-policy and liveness encoding logic and`
`lets service templates and direct task starts share the same execution-side`
`shape instead of carrying separate flat copies.`

`The service controller now maps replica launches from the shared execution`
`spec directly rather than reconstructing the full task launch shape field by`
`field. Tests and helper builders were updated to use the new execution`
`composition explicitly so the old duplicated launch model is no longer part`
`of the active code path.`

## Milestone 2: Runtime Backend Extraction And Test Runtime Unification

### Goal

Make Docker one backend behind a generic runtime interface and delete the
duplicated in-memory runtime.

### Status

Completed on 2026-03-29.

### Scope

1. Add `src/runtime/mod.rs`.
2. Add `src/runtime/types.rs` with generic runtime-side types:
   - `RuntimeBackend`
   - `RuntimeHandle`
   - `RuntimeInfo`
   - `RuntimeLogFrame`
   - `RuntimeLogStream`
   - `RuntimeExecResult`
   - `RuntimeCapabilities`
   - `RuntimeEvent`
3. Add `src/runtime/testing/in_memory.rs` with the shared in-memory backend.
4. Add `src/runtime/oci/mod.rs`.
5. Add `src/runtime/oci/docker.rs` and move Docker-specific code there.
6. Update all runtime consumers to depend on the new runtime interface:
   - task manager,
   - tests,
   - bootstrap,
   - headless runtime setup.

### Removals And Refactors Required

1. Remove the in-memory runtime implementation from `src/task/docker.rs`.
2. Remove the duplicated in-memory runtime implementation from
   `tests/common/testkit.rs`.
3. Remove Bollard types from generic runtime consumers. Bollard should remain
   inside Docker backend files only.
4. Rename container-named generic runtime types:
   - `ContainerLogFrame` -> `RuntimeLogFrame`
   - `ContainerExecResult` -> `RuntimeExecResult`
   - similar runtime-side names throughout manager/tests.
5. Delete `src/task/docker.rs` outright in this milestone if all remaining code
   can move cleanly into `src/runtime/oci/docker.rs`. If a temporary shim is
   needed for one milestone, it must be removed no later than Milestone 10.

### Code Touchpoints

1. `src/runtime/mod.rs` new
2. `src/runtime/types.rs` new
3. `src/runtime/testing/in_memory.rs` new
4. `src/runtime/oci/mod.rs` new
5. `src/runtime/oci/docker.rs` new
6. `src/lib.rs`
7. `src/task/docker.rs`
8. `src/task/mod.rs`
9. `src/task/manager/mod.rs`
10. `src/task/manager/launch.rs`
11. `src/task/manager/state.rs`
12. `src/task/manager/runtime.rs`
13. `src/server/bootstrap/runtime.rs`
14. `src/server/headless.rs`
15. `tests/common/testkit.rs`
16. `tests/task_exec.rs`
17. `tests/task_attach.rs`
18. `tests/task_logs.rs`
19. `tests/task_secrets.rs`
20. `src/task/manager/tests.rs`

### Exit Criteria

1. Docker lives entirely under `src/runtime/oci/`.
2. All generic consumers use the new runtime interface.
3. Only one in-memory runtime implementation exists.
4. No generic module outside the Docker backend depends on Bollard types.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Implemented

1. Added the generic runtime module tree:
   - `src/runtime/mod.rs`
   - `src/runtime/types.rs`
   - `src/runtime/oci/mod.rs`
   - `src/runtime/oci/docker.rs`
   - `src/runtime/testing/mod.rs`
   - `src/runtime/testing/in_memory.rs`
2. Added the runtime-neutral backend contract and payload types:
   - `RuntimeBackend`
   - `RuntimeError` and `RuntimeResult`
   - `RuntimeCreateRequest`
   - `RuntimeInfo`, `RuntimeStateInfo`, and `RuntimeConfigInfo`
   - `RuntimeLogFrame`, `RuntimeLogStream`, and `RuntimeExecResult`
   - `RuntimeCapabilities` and `RuntimeEvent`
3. Moved the Docker backend completely under `src/runtime/oci/docker.rs` and
   converted it to implement the generic runtime trait.
4. Updated generic runtime consumers to depend on `crate::runtime::types`:
   - `src/task/manager/mod.rs`
   - `src/task/manager/launch.rs`
   - `src/task/manager/local.rs`
   - `src/task/manager/runtime.rs`
   - `src/task/manager/state.rs`
   - `src/server/bootstrap/runtime.rs`
   - `src/server/headless.rs`
5. Switched runtime-facing manager logic from Docker-specific method names and
   inspect payloads to runtime-neutral instance methods and `RuntimeInfo`.
6. Consolidated all in-memory runtime usage onto the shared testing backend and
   updated integration/unit tests to implement or consume the new runtime trait:
   - `tests/common/testkit.rs`
   - `tests/task_exec.rs`
   - `tests/task_attach.rs`
   - `tests/task_logs.rs`
   - `tests/task_secrets.rs`
   - `tests/volumes.rs`
   - `tests/services.rs`
   - `tests/gossip.rs`
   - `tests/health.rs`
   - `tests/cluster_view_protocol.rs`
   - `src/task/manager/tests.rs`

### Removed

1. Deleted `src/task/docker.rs`.
2. Removed the duplicate in-memory runtime implementation from
   `tests/common/testkit.rs`.
3. Removed all remaining `task::docker::*` imports from runtime consumers and
   tests.
4. Removed generic-layer dependence on `bollard::service::ContainerInspectResponse`.
   Bollard inspect/list types now stay inside the Docker backend only.
5. Removed the last test assertions and helpers that still expected
   Docker-specific runtime error wording or error variants.

### Findings

1. `RuntimeInfo` needed to carry both lightweight list data and inspect-side
   state/config details so the generic task manager could stop depending on
   Docker inspect payloads without adding a second ad hoc runtime snapshot type.
2. The cleanest cut for event support was a declarative capability flag on the
   backend (`RuntimeCapabilities::lifecycle_events`) instead of a dedicated
   `supports_runtime_events()` helper.
3. Converting the runtime trait forced a useful cleanup in tests: most custom
   backends only needed `RuntimeInfo` plus a small amount of state mutation,
   not fabricated Bollard structs.

### Validation completed

1. `cargo fmt --all` passed.
2. `cargo clippy --all-targets -- -D warnings` passed.
3. `cargo test` passed.

### Proposed commit

Title:

`runtime: extract generic backend interface`

Body:

`Move the task runtime abstraction out of task::docker and into a new`
`runtime module with generic backend types, a shared in-memory backend,`
`and a Docker implementation under runtime/oci.`

`This removes the duplicate in-memory runtime, deletes the old`
`src/task/docker.rs` module, and cuts generic task manager code over to`
`runtime-neutral instance methods and RuntimeInfo snapshots instead of`
`Bollard inspect payloads.`

`Bootstrap, headless startup, task manager tests, and the integration`
`test backends now all depend on the same RuntimeBackend contract, so`
`Docker is just one backend implementation rather than the defining`
`shape of the runtime layer.`

## Milestone 3: Network Attachment Generalization

### Goal

Remove container-specific assumptions from runtime networking so OCI, MicroVM,
and sandbox runtimes can all participate without pretending to be Docker
containers.

### Status

Completed on 2026-03-29.

### Scope

1. Add a generic attachment target model in `src/network/attachment.rs`, for
   example:
   - netns PID
   - netns path
   - tap device or runtime-defined target
2. Extend `RuntimeInfo` so runtime backends can report:
   - network attachment target,
   - current running state,
   - current exposed network endpoints.
3. Update task/workload runtime attachment logic to use generic runtime info.
4. Update network attachment persistence and wire schema to refer to runtime
   instance identifiers rather than container identifiers.

### Removals And Refactors Required

1. Remove `container_id` from `NetworkAttachmentValue` and replace it with
   `instance_id`.
2. Remove `container_pid` from `AttachmentProvisioningRequest` and replace it
   with a runtime attachment target.
3. Remove generic manager logic that directly interprets Docker inspect network
   fields to find liveness targets.

### Code Touchpoints

1. `src/network/attachment.rs`
2. `src/network/attachment/linux.rs`
3. `src/network/types.rs`
4. `src/network/service.rs`
5. `crates/protocol/schema/network.capnp`
6. `src/task/manager/runtime.rs`
7. `src/task/manager/state.rs`
8. `src/runtime/types.rs`
9. `src/runtime/oci/docker.rs`
10. `src/runtime/testing/in_memory.rs`
11. `src/task/manager/tests.rs`
12. `crates/client/src/networks/types.rs`
13. `crates/client/src/networks/attachments.rs`

### Exit Criteria

1. Generic networking code no longer uses `container_id` or `container_pid`.
2. Runtime attachment provisioning is driven by generic runtime attachment
   targets.
3. Generic liveness and network-repair logic no longer depends on Docker
   inspect payloads directly.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Implemented

1. Added `RuntimeAttachmentTarget` to `src/runtime/types.rs` and extended
   `RuntimeInfo` so runtimes can publish a generic attachment target alongside
   running state and network endpoints.
2. Updated the OCI Docker backend in `src/runtime/oci/docker.rs` and the shared
   test backend in `src/runtime/testing/in_memory.rs` to publish attachment
   targets through `RuntimeInfo` instead of forcing the task manager to read
   Docker-specific inspect fields.
3. Refactored `src/network/attachment.rs` and
   `src/network/attachment/linux.rs` so attachment provisioning consumes a
   runtime-defined attachment target rather than a raw container PID.
4. Renamed network attachment persistence and wire fields from `container_id`
   to `instance_id` in:
   - `src/network/types.rs`
   - `src/network/service.rs`
   - `crates/protocol/schema/network.capnp`
   - `crates/client/src/networks/types.rs`
   - `crates/client/src/networks/attachments.rs`
5. Refactored `src/task/manager/runtime.rs` so attachment reconciliation,
   repair, and retry logic refresh the runtime attachment target through
   `inspect_instance()` before each retry instead of reading PID data directly.
6. Updated attachment-related manager tests in `src/task/manager/tests.rs` to
   exercise the new runtime attachment target flow and keep retry validation in
   place.

### Removed

1. Removed `container_pid` from `AttachmentProvisioningRequest`.
2. Removed `container_id` from generic network attachment persistence and
   client decoding, replacing it with `instance_id`.
3. Removed generic task-manager attachment setup logic that interpreted runtime
   inspect PID fields directly during provisioning retries.
4. Removed the last client-side attachment listing references to container-only
   terminology in the network attachment path.

### Findings

1. The attachment target belongs in the runtime layer, not the network layer:
   runtimes produce it, task reconciliation refreshes it, and the networking
   layer only consumes it.
2. The Linux provisioner currently supports `RuntimeAttachmentTarget::
   NetworkNamespacePid` and returns explicit errors for netns-path or tap-based
   targets. That is acceptable for this milestone because the generic contract
   now exists and future runtimes can add concrete provisioner support without
   reopening the task manager path.
3. `src/task/manager/state.rs` did not require direct edits. The existing
   boundary through `src/task/manager/runtime.rs` was already the right place
   to contain the generic attachment-target handoff.

### Validation Completed

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

All three commands passed on 2026-03-29.

### Proposed Commit

```text
network: generalize runtime attachment targets

Replace the container-specific attachment wiring path with a generic
runtime attachment target that is surfaced by runtime inspect results
and consumed by the networking layer.

This renames network attachment records from container_id to
instance_id, removes container_pid from attachment provisioning, and
teaches the task manager to refresh runtime attachment targets during
retry instead of reading Docker-shaped inspect state directly.

The Docker backend and shared in-memory runtime now publish attachment
targets through RuntimeInfo, while the client and network protocol use
instance terminology consistently for attachment listings.
```

## Milestone 4: Internal Workload Core

### Goal

Introduce one internal workload model that can represent regular tasks,
services, jobs, and agent sessions without immediately changing every user
surface.

### Status

Completed on 2026-03-29.

### Scope

1. Add `src/workload/model.rs` with:
   - `WorkloadKind`
   - `RuntimeClass`
   - `WorkloadPhase`
   - `WorkloadSpec`
   - `WorkloadStatus`
   - `WorkloadIdentity`
2. Add conversions between current task-facing types and workload-facing types.
3. Move new generic logic to depend on workload model instead of task-specific
   model.
4. Keep current `TaskSpec` and `TaskStatus` only as thin facades or projections
   for existing task-facing APIs during the transition.

### Removals And Refactors Required

1. Remove any new generic code that still depends on
   `task::container::ContainerState`.
2. Do not duplicate another full set of task-only manager or runtime types in
   parallel with workload types.
3. If temporary conversion shims are introduced in this milestone, mark them
   for deletion in Milestone 5 and delete them there before that milestone is
   complete.

### Code Touchpoints

1. `src/workload/model.rs` new
2. `src/workload/mod.rs`
3. `src/lib.rs`
4. `src/task/container.rs`
5. `src/task/types.rs`
6. `src/task/service.rs`
7. `src/task/manager/mod.rs`
8. `src/services/manager.rs`
9. `src/services/types.rs`
10. `src/task/causality.rs`
11. `src/workload/types.rs`
12. `src/workload/capnp_codec.rs`
13. `src/services/readiness.rs`
14. `src/services/slot_reconcile.rs`

### Exit Criteria

1. An internal workload model exists and is usable by new generic code.
2. New work no longer adds task-only structural types.
3. Existing task-facing APIs still behave the same externally.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Implemented

1. Added `src/workload/model.rs` with the internal workload-native core:
   - `WorkloadKind`
   - `RuntimeClass`
   - `WorkloadIdentity`
   - `WorkloadPhase`
   - `WorkloadSpec`
   - `WorkloadStatus`
   - `WorkloadEvent`
   - `WorkloadValue`
   - `WorkloadValueDraft`
   - workload-native env/secret/volume/service metadata types
2. Moved lifecycle ordering and replicated-value projection logic into the
   workload model:
   - causal comparison helpers
   - event replacement ordering
   - `spec_to_status`
   - `spec_to_value`
   - `value_to_spec`
   - `merge_status_into_value`
   - `merge_definition_into_value`
   - `select_best_workload_value`
3. Converted `src/task/container.rs` into a compatibility alias over
   `WorkloadPhase`.
4. Converted `src/task/types.rs` into task-facing aliases over the workload
   model plus the existing task-only filter helpers.
5. Converted `src/task/causality.rs` into a thin task-facing re-export layer
   over workload causal ordering.
6. Updated `src/task/manager/mod.rs` to consume workload-model projection and
   merge helpers instead of defining those task-shaped helpers inline.
7. Updated `src/workload/types.rs` and `src/workload/capnp_codec.rs` to depend
   on workload-native support types instead of importing task types back into
   the generic workload layer.
8. Updated generic service controller code to depend on workload lifecycle
   vocabulary instead of importing `task::container::ContainerState`:
   - `src/services/manager.rs`
   - `src/services/readiness.rs`
   - `src/services/slot_reconcile.rs`

### Removed

1. Removed the standalone lifecycle enum definition from `src/task/container.rs`
   and replaced it with a task-facing alias over `WorkloadPhase`.
2. Removed the duplicated structural task model definitions from
   `src/task/types.rs` and replaced them with task-facing aliases over the
   workload model.
3. Removed the duplicated causal ordering implementation from
   `src/task/causality.rs` and replaced it with a thin façade over the
   workload model.
4. Removed the task-manager-local copies of spec/status/value projection logic
   from `src/task/manager/mod.rs`.
5. Removed the reverse dependency from the generic workload layer back into
   task-only support types in `src/workload/types.rs` and
   `src/workload/capnp_codec.rs`.

### Findings

1. `TaskSpec`, `TaskStatus`, `TaskValue`, and related support types can be kept
   stable externally by making them aliases over workload-native structs
   instead of carrying a second full task-shaped definition in parallel.
2. Current service replicas can be represented without adding a new stored
   field by inferring `WorkloadKind::ServiceReplica` from the existing
   `service_metadata`; direct standalone tasks still map to
   `WorkloadKind::Task`.
3. Current task-facing runtime selection still maps cleanly to
   `RuntimeClass::Oci`. Jobs, agents, and alternate runtimes can extend that
   model later without reintroducing task-shaped persistence helpers.

### Validation Completed

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

All three commands passed on 2026-03-29.

### Proposed Commit

```text
workload: extract internal workload core model

Introduce a workload-native core model for lifecycle phase, identity,
spec, status, and replicated value handling, and move the shared
projection and causal-ordering logic onto that layer.

Task-facing types now project from the workload model instead of
redefining the same structures locally. The task container phase type
becomes a compatibility alias over WorkloadPhase, and the task manager
stops owning spec/status/value merge helpers that are really model
logic.

The generic workload layer no longer depends back on task-only support
types, and service controller code now imports workload lifecycle
vocabulary directly where it participates in generic orchestration.
```

## Milestone 5: Workload Manager Cutover Behind Existing Task Surface

### Goal

Move reconciliation and runtime orchestration onto a generic workload manager
while keeping the existing task CLI/RPC behavior intact.

### Status

Completed on 2026-03-29.

### Scope

1. Add `src/workload/manager/`.
2. Move generic reconciliation logic from `src/task/manager/` into the workload
   manager.
3. Keep `TaskManager` as a thin task-kind facade over `WorkloadManager` until
   all task-facing callers are cut over.
4. Keep `src/task/service.rs` as a task-kind RPC surface that projects into the
   workload manager.

### Removals And Refactors Required

1. Remove container-specific names from generic manager state:
   - `local_containers` -> `local_instances`
   - similar cache and helper names
2. Remove generic runtime event handling that parses deterministic container
   names to infer task identity. Use explicit runtime metadata instead.
3. Delete temporary workload conversion shims introduced in Milestone 4 once
   task facade methods use the workload manager directly.

### Code Touchpoints

1. `src/workload/manager/` new
2. `src/workload/mod.rs`
3. `src/lib.rs`
4. `src/task/manager/mod.rs`
5. `src/task/manager/launch.rs`
6. `src/task/manager/local.rs`
7. `src/task/manager/planner.rs`
8. `src/task/manager/runtime.rs`
9. `src/task/manager/state.rs`
10. `src/task/service.rs`
11. `src/server/bootstrap/runtime.rs`
12. `src/server/headless.rs`
13. `src/task/manager/tests.rs`

### Exit Criteria

1. Generic runtime/reconciliation logic lives under workload manager code.
2. Task-facing APIs are facades, not the core orchestration layer.
3. Generic manager code no longer carries container-only naming.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Completed

1. Moved the generic reconciliation/runtime orchestration implementation from
   `src/task/manager/` into `src/workload/manager/`:
   - `launch.rs`
   - `local.rs`
   - `planner.rs`
   - `remote_advisory.rs`
   - `reservation.rs`
   - `runtime.rs`
   - `secrets.rs`
   - `state.rs`
   - `tests.rs`
   - `volumes.rs`
2. Replaced `src/task/manager/mod.rs` with a thin compatibility façade that
   re-exports:
   - `WorkloadManager` as `TaskManager`
   - `WorkloadManagerConfig` as `TaskManagerConfig`
   - `WorkloadRuntimeConfig` as `TaskRuntimeConfig`
   - `WorkloadStartRequest` as `TaskStartRequest`
   - `WorkloadTrafficPublicationUpdate` as
     `TaskTrafficPublicationUpdate`
   - task-facing helper re-exports needed by existing callers
3. Removed the generic manager's dependency on deterministic instance names for
   ownership recovery by adding explicit runtime labels:
   - `RuntimeCreateRequest.labels`
   - `RuntimeInfo.labels`
   - Docker and in-memory runtimes now persist and report
     `mantissa.workload_id`
4. Updated runtime event ingestion and local runtime inventory reconciliation
   to identify workloads from runtime labels instead of parsing
   `mantissa-<uuid>` names.
5. Renamed generic in-memory manager state away from container-only vocabulary:
   - `local_containers` -> `local_instances`
   - related helper/cache names under `src/workload/manager/`
6. Cut the moved workload manager over to workload-owned model/types in
   production code instead of importing task definitions back into the generic
   layer:
   - `src/workload/manager/mod.rs`
   - `src/workload/manager/launch.rs`
   - `src/workload/manager/local.rs`
   - `src/workload/manager/planner.rs`
   - `src/workload/manager/runtime.rs`
   - `src/workload/manager/secrets.rs`
   - `src/workload/manager/state.rs`
   - `src/workload/manager/volumes.rs`
7. Added a shared workload-manager test helper change so synthetic runtime
   inventory rows include the same ownership label path used by real runtime
   launches.

### Removed

1. Deleted the old generic manager implementation files from
   `src/task/manager/`; the task module no longer owns the orchestration core.
2. Removed the runtime-event fallback that inferred task identity from
   deterministic Docker/container names.
3. Removed the generic manager's direct dependency on task-layer causal/type
   helpers in the moved workload-manager production code.

### Findings

1. The runtime label is the right boundary for generic ownership recovery. Once
   inventory and event streams use explicit metadata, instance names can remain
   a launch detail rather than part of the control-plane identity contract.
2. Keeping `task::manager` as a façade is enough to preserve the existing CLI,
   RPC, and service control-plane call sites while the core moves under
   `workload/manager`.
3. The only intentional task-facing dependency left inside the moved generic
   layer is task-surface compatibility where callers still use task-named
   filters or RPC codecs. The orchestration core no longer depends on the task
   layer for its model/state types.

### Validation Completed

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

All three commands passed on 2026-03-29.

### Proposed Commit

```text
workload: move manager core behind task facade

Move the generic runtime orchestration and reconciliation code out of
src/task/manager and into a new workload manager module, leaving the
task manager as a thin compatibility facade over workload-owned types.

The moved manager now tracks local instances instead of local
containers, and runtime ownership recovery no longer depends on
parsing deterministic instance names. Docker and the in-memory runtime
stamp mantissa.workload_id labels on launches and report them back
through runtime inventory and event streams so the generic manager can
reconcile explicit workload identity.

This also cuts the production workload manager over to workload-owned
model/types instead of importing task definitions back into the
generic layer, while preserving existing task-facing callers and test
coverage through the facade.
```

## Milestone 6: Planner And Node Capability Generalization

### Goal

Make scheduling runtime-aware so not every node is assumed to support every
runtime class or sandbox profile.

### Status

Completed on 2026-03-29.

### Scope

1. Extend node metadata and runtime health/registration to advertise:
   - supported runtime classes,
   - supported sandbox profiles,
   - runtime-specific feature flags.
2. Update planner input structures to carry runtime class and execution spec
   instead of only image/container metadata.
3. Update placement filters to exclude nodes that cannot satisfy runtime or
   sandbox requirements.

### Removals And Refactors Required

1. Remove image-only assumptions from planner intents and batch plans.
2. Remove `container_name` as a scheduler input. The runtime launch path should
   derive instance names after placement rather than the planner treating them
   as core scheduling data.
3. Remove any fallback logic that assumes all schedulable nodes are equivalent
   for every workload runtime.

### Code Touchpoints

1. `src/task/manager/planner.rs`
2. `src/workload/manager/` planned scheduler-facing files
3. `src/registry/mod.rs`
4. `src/topology/` runtime metadata propagation points
5. `src/node/mod.rs`
6. `src/server/bootstrap/runtime.rs`

### Exit Criteria

1. Planner inputs include runtime requirements.
2. Node filtering is runtime-aware.
3. Scheduling no longer assumes “any free node can run any workload”.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Completed

1. Added cluster-visible runtime capability metadata and propagated it through
   topology registration, join handling, gossip relay, peer storage, and
   registry lookups:
   - `src/runtime/types.rs`
   - `crates/protocol/schema/topology.capnp`
   - `src/topology/peers.rs`
   - `src/topology/types.rs`
   - `src/topology/mod.rs`
   - `src/topology/service.rs`
   - `src/server/service.rs`
   - `src/gossip/mod.rs`
   - `src/server/bootstrap/runtime.rs`
   - `src/registry/mod.rs`
2. Extended workload start requests and planner intents to carry runtime class,
   sandbox profile, and derived runtime feature requirements:
   - `src/workload/manager/mod.rs`
   - `src/workload/manager/planner.rs`
   - `src/task/service.rs`
   - `src/services/manager.rs`
   - `tests/task_secrets.rs`
   - `tests/volumes.rs`
3. Made planner placement runtime-aware:
   - local preassigned starts now fail when the local node cannot satisfy the
     requested runtime requirements,
   - remote digest hints only count intents a peer can actually host,
   - untargeted candidate allocation tracks runtime-incompatible peers and
     returns a structured runtime-requirements error instead of generic
     capacity failure.
4. Removed planner-owned instance naming from scheduling inputs. The planner no
   longer carries `instance_name`; local launch derives `mantissa-<uuid>` only
   after placement in `src/workload/manager/local.rs`.
5. Added runtime-aware test coverage:
   - planner unit coverage for digest hostability against runtime profiles in
     `src/workload/manager/planner.rs`
   - workload-manager coverage for fast-failing unsupported local runtime
     classes in `src/workload/manager/tests.rs`
6. Tightened scheduling retry classification so runtime requirement failures are
   not treated as transient convergence problems. Unsupported runtime classes
   now fail fast instead of exhausting the scheduling retry budget in
   `src/workload/manager/mod.rs`.
7. Refactored supporting code to satisfy the stricter lint gate introduced by
   the new metadata:
   - `RuntimeClass` now implements `std::str::FromStr` in
     `src/workload/model.rs`
   - `TopologyEvent::Join` boxes scheduling and runtime-support payloads in
     `src/topology/types.rs`
   - local placement prerequisites are grouped in
     `src/workload/manager/planner.rs` instead of extending
     `seed_local_plans(...)` argument count further.

### Removed

1. Removed the planner's dependency on precomputed instance names as scheduling
   data.
2. Removed the implicit assumption that every schedulable node can host every
   workload runtime. Placement now consults per-peer runtime support before a
   node is considered a valid candidate.
3. Removed the retry-path bug where runtime mismatches were treated like
   transient network or snapshot convergence errors.
4. Removed the ad hoc `RuntimeClass::from_str` helper in favor of the standard
   `FromStr` trait implementation.

### Findings

1. Runtime capability propagation touches more than the planner. The topology
   wire format, peer merge logic, join replay path, and registry cache all had
   to converge on the same runtime-support projection before scheduler filters
   could trust the data.
2. The test suite exposed a real behavior bug, not just missing coverage:
   runtime requirement failures were still being retried because the retry
   classifier treated every `SchedulingError` as transient. That had to be
   fixed before the milestone could be considered complete.
3. Adding runtime metadata to `TopologyEvent::Join` pushed the enum over the
   clippy size threshold. Boxing the scheduling and runtime-support payloads
   kept the event cheap to clone and relay without weakening the lint gate.

### Validation Completed

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

All three commands passed on 2026-03-29.

### Proposed Commit

```text
planner: make placement runtime aware

Propagate runtime support metadata through topology registration,
gossip, peer storage, and registry lookups so scheduler decisions no
longer assume every node can host every workload runtime.

This extends workload start requests and planner intents with runtime
class, sandbox profile, and derived feature requirements, removes
planner-owned instance naming, and teaches local and remote candidate
selection to reject runtime-incompatible nodes explicitly.

The scheduling retry path now treats runtime requirement failures as
non-transient so unsupported runtime classes fail fast instead of
burning the retry budget. Tests were added for runtime-aware digest
hostability and unsupported local runtime rejection.
```

## Milestone 7: Service Controller Cutover Onto Shared Workload Templates

### Goal

Make services launch workloads directly from shared execution templates instead
of translating service templates into task-start structs.

### Status

Completed on 2026-03-29.

### Scope

1. Update service template model to embed the shared execution spec fully.
2. Update `ServiceController` to launch workloads directly.
3. Keep service-only semantics where they belong:
   - readiness,
   - dependency ordering,
   - public port exposure,
   - rollout state.

### Removals And Refactors Required

1. Remove `make_replica_request()` and similar field-copy glue from
   `src/services/manager.rs`.
2. Remove task-specific runtime policy types from service types if still
   present.
3. Do not move readiness into the shared execution spec. Keep it service-only.

### Code Touchpoints

1. `src/services/types.rs`
2. `src/services/manager.rs`
3. `src/services/service.rs`
4. `src/services/rollout.rs`
5. `src/services/dependencies.rs`
6. `crates/client/src/services/manifest.rs`
7. `crates/client/src/services/deploy.rs`

### Exit Criteria

1. Services use the shared execution model directly.
2. The service controller no longer rebuilds task launch shape by hand.
3. Service-only behavior remains separate and explicit.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Completed

1. Moved service replica launch construction onto the shared service-template
   model in `src/services/types.rs`:
   - added `launch_execution()` to resolve service network requirements into
     launch-time network IDs
   - added `replica_start_request()` so one `ServiceTaskSpecValue` can produce
     its own `WorkloadStartRequest`
   - moved deterministic replica naming beside the template model instead of
     rebuilding it in the controller
2. Cut service rollout and reconciliation paths over to
   `WorkloadStartRequest` and `WorkloadTrafficPublicationUpdate` in:
   - `src/services/manager.rs`
   - `src/services/rollout.rs`
   - `src/services/slot_reconcile.rs`
3. Removed the controller-side field-copy glue from `src/services/manager.rs`:
   - deleted `make_replica_request()`
   - deleted `format_replica_name()`
   - deleted `short_id()`
4. Updated fallback and rollout helper APIs so they operate on workload start
   requests rather than task-start aliases:
   - `start_tasks_with_fallback(...)`
   - `build_start_requests(...)`
   - `build_missing_template_requests(...)`
   - `build_replacement_requests(...)`
   - `allow_untargeted_fallback(...)`
   - `requests_require_pinned_targets(...)`
5. Removed the now-unused `task_start_error_is_retryable` compatibility
   re-export from `src/task/manager/mod.rs` because the service controller now
   uses the workload-native helper directly.
6. Preserved service-only semantics in the service layer:
   - readiness stayed on `ServiceTaskSpecValue`
   - dependency ordering stayed in `src/services/dependencies.rs`
   - public port/protocol stayed in `src/services/types.rs`
   - rollout orchestration stayed in `src/services/rollout.rs`
7. Updated service-manager tests so request helpers and replica-launch
   assertions exercise the workload-native request path directly.

### Removed

1. Removed the last service-controller code path that rebuilt replica launch
   metadata field by field in `src/services/manager.rs`.
2. Removed the local compatibility re-export that only existed to support the
   old task-shaped service launch path.
3. Removed task-shaped request types from service rollout/fallback helper
   signatures; the controller now depends on workload-native start requests at
   that boundary.

### Findings

1. The service template model was already most of the way there. The real
   remaining duplication was not in manifest storage or RPC codecs, but in the
   controller seam where service templates were still being copied into
   task-shaped requests.
2. No client-side schema or manifest parsing changes were required for this
   milestone because the shared execution-spec embedding had already landed in
   earlier work. The remaining cutover was server-side orchestration.
3. The workload-manager API is still task-named (`start_tasks_batch`,
   `request_task_stop`, and similar), but the service controller now feeds it
   workload-native request objects. Renaming those APIs is a separate cleanup,
   not a blocker for shared template cutover.

### Validation Completed

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

All three commands passed on 2026-03-29.

### Proposed Commit

```text
services: launch replicas from shared workload templates

Move service replica request construction onto ServiceTaskSpecValue so
the service controller launches shared workload requests directly from
the embedded execution template instead of rebuilding the launch shape
inside services/manager.rs.

This removes make_replica_request and related naming glue, cuts
rollout and fallback helpers over to WorkloadStartRequest and
WorkloadTrafficPublicationUpdate, and drops the now-unused
task_start_error_is_retryable compatibility re-export.

Service-only behavior such as readiness, dependency ordering, public
port exposure, and rollout state remains in the service layer while
execution details stay shared through WorkloadExecutionSpec.
```

## Milestone 8: First-Class Jobs

### Goal

Add a finite workload controller for jobs without overloading regular tasks.

### Status

Completed on 2026-03-29.

### Scope

1. Add `src/jobs/`.
2. Define:
   - `JobSpec`
   - `JobStatus`
   - `JobCompletionPolicy`
   - `JobRetryPolicy`
   - `JobController`
3. Jobs should schedule shared execution specs through the workload manager and
   use the same runtime backend layer as regular tasks and services.

### Removals And Refactors Required

1. Do not add job-only completion or retry semantics to regular task specs.
2. Reuse workload manager/runtime/event infrastructure instead of cloning task
   orchestration code under a job namespace.

### Code Touchpoints

1. `src/jobs/` new
2. `src/lib.rs`
3. `src/workload/manager/`
4. `src/runtime/`
5. `src/store/` job persistence additions as required
6. `crates/protocol/schema/` job RPC schema additions as required
7. `crates/client/src/` job client commands as required

### Exit Criteria

1. Jobs are first-class and finite.
2. Jobs reuse shared execution, scheduling, and runtime layers.
3. No job-only fields leak into regular task or service template models.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Completed

1. Added the first-class jobs subsystem under `src/jobs/`:
   - `src/jobs/types.rs`
   - `src/jobs/registry.rs`
   - `src/jobs/manager.rs`
   - `src/jobs/service.rs`
2. Introduced a durable replicated job model that keeps finite-run semantics
   out of regular tasks:
   - `JobSpecValue`
   - `JobStatus`
   - `JobCompletionPolicy`
   - `JobRetryPolicy`
   - `JobEvent`
3. Added a dedicated replicated job store in `src/store/job_store.rs` and
   wired it through bootstrap, headless runtime setup, topology stores, sync
   stores, and server clients.
4. Added job gossip and sync plumbing so jobs replicate like other first-class
   control-plane objects:
   - `crates/protocol/schema/jobs.capnp`
   - `crates/protocol/schema/gossip.capnp`
   - `crates/protocol/schema/sync.capnp`
   - `src/gossip/mod.rs`
   - `src/sync/mod.rs`
   - `src/sync/delta.rs`
5. Added a jobs RPC capability and server/session exposure:
   - `crates/protocol/schema/server.capnp`
   - `src/server/session.rs`
   - `src/server/mod.rs`
   - `src/server/bootstrap/runtime.rs`
   - `src/server/headless.rs`
6. Added a `JobController` that owns completion and retry semantics while
   reusing the shared execution/runtime path:
   - jobs submit one shared `TaskExecutionSpec`
   - jobs launch attempt tasks through `TaskManager`
   - retry ownership stays in the job controller instead of leaking into task
     or service specs
7. Added client and CLI support for jobs:
   - `crates/client/src/jobs/`
   - `src/cli.rs`
   - `src/app.rs`
8. Added integration coverage for the new finite workload path in
   `tests/jobs.rs` and updated shared cluster-domain assertions in
   `tests/stress_large_cluster.rs`.

### Removed

1. Avoided adding job-only completion or retry fields to regular task specs,
   service templates, or the shared execution template.
2. Avoided cloning task orchestration code under `src/jobs/`; the job
   controller reuses workload execution, task start, runtime inspection, sync,
   and gossip infrastructure.
3. Removed duplicated CLI volume-mount parsing by extracting shared helpers
   into `crates/client/src/volumes/mod.rs` and cutting `tasks start` over to
   the shared path before adding `jobs run`.

### Findings

1. The correct split is controller semantics versus execution semantics. Jobs
   need their own durable status, retry, and completion policy, but their
   launch shape is still the shared workload execution template.
2. The cleanest first cut was to make jobs a finite controller over scheduled
   task attempts, not a new runtime type. That keeps this milestone aligned
   with the earlier workload/runtime split and avoids smuggling job policy into
   the generic runtime layer.
3. Adding a new replicated domain touched more shared bootstrap paths than the
   controller itself: gossip routing, sync-store registration, session
   capability exposure, topology wiring, and headless test setup were all
   required for a true first-class feature.

### Validation Completed

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

All three commands passed on 2026-03-29.

### Proposed Commit

```text
jobs: add first-class finite workload controller

Add a first-class jobs control-plane slice backed by a replicated job
store, a finite job controller, and a jobs RPC capability.

Jobs now have their own durable spec/status model and retry policy,
while still reusing the shared workload execution template, task
scheduler, runtime backend, gossip transport, sync domain wiring, and
headless bootstrap flow. This keeps finite-run semantics out of regular
tasks and service templates instead of overloading the task model.

The server/session capability graph, gossip schema, sync domain set,
client crate, CLI, and test harness now expose jobs end to end. The
client-side volume mount parsing used by `tasks start` was also shared
with `jobs run` rather than duplicated, and new integration tests cover
job submission, successful completion, and retry after failed task
termination.
```

## Milestone 9: Agent Sessions And Sandbox Scheduling

### Goal

Add agent scheduling without conflating agents with regular tasks.

### Status

Pending.

### Scope

1. Add `src/agents/`.
2. Define:
   - `AgentSessionSpec`
   - `AgentSessionStatus`
   - `AgentRunSpec`
   - `AgentController`
   - `AgentEvent`
   - structured control/event protocol
3. Agent sessions should own:
   - workspace policy,
   - tool policy,
   - checkpoint policy,
   - interaction policy.
4. Agent runs should be the schedulable executions that acquire compute and
   launch a sandbox runtime instance.
5. Add sandbox runtime class and sandbox profiles. Sandbox backends may be OCI,
   MicroVM, or both.

### Removals And Refactors Required

1. Do not add agent-only fields to regular task specs or service templates.
2. Do not treat stdout/stderr as the primary agent protocol. Logs remain useful
   but structured events are required.
3. Remove generic assumptions that every runtime supports:
   - `exec`,
   - `attach`,
   - `pre_stop_command`,
   - exec-based liveness probes.
   Those must become capability-driven.

### Code Touchpoints

1. `src/agents/` new
2. `src/lib.rs`
3. `src/runtime/`
4. `src/workload/manager/`
5. `src/services/manager.rs` if service-managed agents are desired later
6. `crates/protocol/schema/` new agent RPC schema
7. `crates/client/src/` new agent client commands
8. `src/volumes/` if workspace volume helpers are needed
9. `src/secrets/` if agent tool/session secret scoping is needed

### Exit Criteria

1. Agent sessions and runs are first-class.
2. Agents reuse workload scheduling and runtime layers.
3. Agent-specific state is not mixed into regular task/service models.
4. Sandbox capability and event model is explicit.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

## Milestone 10: Final Cleanup And Naming Reconciliation

### Goal

Delete temporary transition code and reconcile naming so generic layers no
longer read like Docker container orchestration internals.

### Status

Pending.

### Scope

1. Remove temporary task-to-workload conversion shims.
2. Remove leftover container-only names from generic runtime/workload layers.
3. Replace top-level runtime config with a runtime registry config model.
4. Consolidate tests into:
   - workload-core tests,
   - runtime contract tests,
   - controller-specific tests.
5. Update notes and docs to use the final workload/runtime terminology.

### Removals And Refactors Required

1. Delete temporary compatibility helpers introduced in earlier milestones.
2. Delete old container-named helper functions and state fields once generic
   replacements are in use.
3. Keep the `tasks` user surface only if it remains a real dedicated interface
   for `WorkloadKind::Task`; do not keep duplicate internal task orchestration
   code behind it.

### Code Touchpoints

1. `src/task/`
2. `src/workload/`
3. `src/runtime/`
4. `src/network/`
5. `src/server/bootstrap/runtime.rs`
6. `src/config.rs`
7. `tests/`
8. `notes/`

### Exit Criteria

1. No generic layer depends on Docker/container-specific names or types.
2. Duplicated transition code is gone.
3. Workload, runtime, jobs, services, and agents all sit on the same shared
   execution and scheduling core.

### Validation Gate

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`
