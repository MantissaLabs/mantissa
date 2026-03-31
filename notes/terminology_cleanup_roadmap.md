# Terminology Cleanup Roadmap

## Summary

The workload scheduling refactor fixed the architecture, but the repository
still carries a large amount of transitional terminology. The main problem is
not that one name is "wrong" in isolation. The problem is that the tree still
mixes four different vocabularies:

1. public standalone task APIs,
2. the shared workload substrate,
3. controller-specific concepts such as service task templates, job attempts,
   agent sessions, and agent runs,
4. runtime substrate names versus sandbox or isolation names.

That leaves readers needing historical context to understand simple code paths.
The goal of this roadmap is to hard-cut the remaining names so that the code
matches the model that already exists.

This is a cleanup roadmap, not a redesign roadmap. The plan assumes the
existing architecture stays intact:

1. `task` remains the standalone public execution surface,
2. `workload` remains the shared schedulable substrate,
3. services, jobs, and agents remain controller layers on top,
4. runtime substrate and isolation concepts stay separate.

## End State

At the end of this cleanup:

1. public task APIs use task-shaped names,
2. the shared substrate uses workload-shaped names internally,
3. service specs hold task templates, and launched replicas stay distinct from
   those task template definitions,
4. job and agent controller records are not mislabeled as workload kinds,
5. runtime substrate names are separated from sandbox or isolation names,
6. the only deliberate task-facing compatibility surface lives under `src/task`.

## Cleanup Rules

1. Use hard cutovers only. Do not keep dual names for compatibility.
2. Delete obsolete names in the same milestone that introduces the replacement.
3. Keep backend-specific container wording only inside Docker-specific code.
4. Prefer renaming the generic layer first when a task-shaped name leaks into a
   shared subsystem.
5. If a milestone uncovers a better abstraction than a straight rename, stop
   and update this note before proceeding.
6. Avoid owner-stutter names. A child type inside `ServiceSpec` should not be
   named `ServiceTemplate` when the parent already encodes the owner.

## Execution Rules

1. Execute milestones strictly in order.
2. Before starting a milestone, mark it `In progress` in this document.
3. A milestone is complete only when the old terminology has been removed, not
   when the new name merely exists beside it.
4. When a milestone completes, update this document with:
   - status,
   - completion date,
   - findings or scope adjustments,
   - validation results.
5. Validation gate after every milestone:
   - `cargo fmt --all`
   - `cargo clippy --all-targets -- -D warnings`
   - `cargo test`
6. Do not start the next milestone until the full validation gate passes.
7. I will not create commits directly. After each milestone I will stop and
   provide a commit title and body for you to apply.

## Current Mismatch Inventory

### 1. Public task APIs still expose internal workload naming

The task RPC and task client still ask users to send `WorkloadStartRequest`
even though the public surface is explicitly task-oriented.

Primary locations:

1. `crates/protocol/schema/task.capnp`
   - `start @0 (request :WorkloadStartRequest) -> (spec :TaskSpec)`
   - `startMany @3 (requests :List(WorkloadStartRequest)) -> (specs :List(TaskSpec))`
2. `src/task/service.rs`
   - task service codec reads and writes `WorkloadStartRequest`
3. `crates/client/src/tasks/start.rs`
   - task client request builder speaks in workload terms
4. `docs/workloads-and-runtimes.md`
   - currently has to explain why the task API accepts a workload request

### 2. Service specs still use confusing child-template terminology

The first service cleanup removed the old `tasks` ambiguity, but the selected
replacement still stutters. A `ServiceSpec` containing `ServiceTemplate*`
values is harder to read than a `ServiceSpec` containing `TaskTemplate*`
values. The next pass should keep the replica split while correcting the child
template names.

Primary locations:

1. `src/services/types.rs`
   - `ServiceSpecValue.templates`
   - `ServiceTemplateSpecValue`
   - `ServiceTemplateNetworkRequirement`
2. `src/services/service.rs`
   - service codec fields and comments use `ServiceTemplate`
3. `src/services/manager.rs`
   - deployment helpers accept `templates: Vec<ServiceTemplateSpecValue>`
4. `src/services/rollout.rs`
   - rollout bookkeeping mixes launched task IDs with `ServiceTemplate*`
     template definitions
5. `src/services/reconcile.rs`
6. `src/services/dependencies.rs`
7. `src/services/ownership.rs`
8. `src/services/ordering.rs`
9. `crates/client/src/services/manifest.rs`
   - `ServiceManifest.templates`
   - `ServiceTemplateSpec`
10. `crates/client/src/services/deploy.rs`
11. `crates/client/src/services/list.rs`
12. `crates/client/src/services/mod.rs`
13. `crates/protocol/schema/services.capnp`
   - `ServiceTemplate`
   - `ServiceTemplateNetwork`
   - `templates @... :List(ServiceTemplate)`
14. `src/network/discovery.rs`
15. `tests/services.rs`
16. `tests/discovery.rs`
17. `tests/stress_large_cluster.rs`
18. `examples/*.ron` service manifests
19. `docs/service-rollouts.md`
20. `docs/workloads-and-runtimes.md`

### 3. The shared execution template still says task

The execution spec is shared by tasks, services, jobs, and agents. The current
name still implies it belongs to tasks only.

Primary locations:

1. `src/workload/types.rs`
   - `WorkloadExecutionSpec<N>`
   - `TaskExecutionSpec = WorkloadExecutionSpec<Uuid>`
2. `src/services/types.rs`
3. `src/task/service.rs`
4. `src/jobs/types.rs`
5. `src/jobs/manager.rs`
6. `src/jobs/service.rs`
7. `src/agents/types.rs`
8. `src/agents/manager.rs`
9. `src/agents/service.rs`
10. `tests/services.rs`
11. `tests/jobs.rs`
12. `tests/agents.rs`
13. `tests/volumes.rs`

### 4. Internal replication and storage still say task

The generic replicated execution layer still uses task names in the store,
sync, gossip, and generic workload manager even when it is carrying
service-owned replicas and other shared workload rows.

Primary locations:

1. `src/store/task_store.rs`
   - `TaskStore`
   - `TaskTables`
   - `open_task_store`
   - table names `task_values`, `task_tombs`, `task_meta`
2. `src/sync/mod.rs`
   - `Domain::Tasks`
   - `SyncStores.tasks`
3. `src/sync/delta.rs`
   - `TaskStore`
   - `TaskValue`
   - `Domain::Tasks`
4. `crates/protocol/schema/gossip.capnp`
   - `using import "task.capnp".TaskEvent`
   - `task @3 :TaskEvent`
5. `src/workload/manager/mod.rs`
   - still imports workload types as `TaskEvent`, `TaskExecutionSpec`,
     `TaskStore`, `TaskValue`
6. `src/workload/manager/state.rs`
   - internal caches and helpers still use `task_values`
7. `src/workload/manager/runtime.rs`
8. `src/server/headless.rs`
9. `src/server/bootstrap/stores.rs`
10. `src/topology/mod.rs`
11. `src/network/controller.rs`
12. `src/network/discovery.rs`
13. `tests/discovery.rs`
14. `tests/stress_large_cluster.rs`

### 5. Workload ownership vocabulary is still mixed

`WorkloadKind` currently combines real schedulable workload rows and
controller-level records. Some of those variants are not backed by shared
workload rows at all.

Primary locations:

1. `src/workload/model.rs`
   - `WorkloadKind::{Task, ServiceReplica, Job, AgentSession, AgentRun}`
2. `src/workload/model.rs`
   - `infer_workload_kind()` effectively only distinguishes direct tasks and
     service replicas today
3. `src/jobs/manager.rs`
   - jobs launch shared workload rows but do not stamp explicit ownership
     semantics beyond job-local records
4. `src/agents/manager.rs`
   - agent runs are schedulable executions, but agent sessions are not
5. `docs/workloads-and-runtimes.md`
   - currently has to explain controller semantics around a workload enum that
     is broader than the underlying rows

### 6. Runtime substrate and sandbox naming are overloaded

The code needs to represent both the execution substrate and the isolation
contract, but the current model still uses `RuntimeClass::Sandbox` as if
sandbox were a peer of OCI and MicroVM.

Primary locations:

1. `src/workload/model.rs`
   - `RuntimeClass::{Oci, MicroVm, Sandbox}`
   - `sandbox_profile`
2. `src/runtime/types.rs`
   - `RuntimeSupportProfile.runtime_classes`
   - `RuntimeSupportProfile.sandbox_profiles`
3. `src/workload/manager/planner.rs`
   - scheduling intent and candidate filtering use `runtime_class` plus
     `sandbox_profile`
4. `src/registry/mod.rs`
5. `src/topology/peers.rs`
6. `src/topology/service.rs`
7. `src/agents/types.rs`
   - agent defaults currently request `RuntimeClass::Sandbox`
8. `src/agents/manager.rs`
9. `src/agents/service.rs`
10. `crates/protocol/schema/task.capnp`
11. `crates/protocol/schema/agents.capnp`
12. `crates/client/src/agents/submit.rs`
13. `crates/client/src/agents/list.rs`
14. `crates/client/src/agents/runs.rs`
15. `src/cli.rs`
16. `src/app.rs`
17. `docs/workloads-and-runtimes.md`

### 7. CLI help and docs still leak transitional wording

Some help strings and docs still say container when they mean execution image,
runtime instance, or workload execution.

Primary locations:

1. `src/cli.rs`
   - job and agent help strings
2. `src/app.rs`
3. `docs/workloads-and-runtimes.md`
4. `docs/distributed-scheduler.md`
5. `docs/repo-layout.md`
6. `docs/service-rollouts.md`
7. protocol comments in `task.capnp`, `services.capnp`, and `agents.capnp`

## Target Vocabulary

The roadmap should converge on the following names.

| Current name | Target name | Notes |
| --- | --- | --- |
| `WorkloadStartRequest` in public task RPC | `TaskStartRequest` | Keep `WorkloadStartRequest` internal to the shared workload manager |
| `ServiceTemplateSpecValue` | `TaskTemplateSpecValue` | `ServiceSpec` should hold task templates, not `ServiceTemplate`s |
| `ServiceTemplateNetworkRequirement` | `TaskTemplateNetworkRequirement` | Network requirement for one task template inside a service |
| `ServiceSpecValue.templates` | `ServiceSpecValue.task_templates` | Be explicit that these are child task templates |
| `ServiceSpecValue.task_ids` | `ServiceSpecValue.replica_ids` | These UUIDs identify service-owned replicas |
| manifest `ServiceTemplateSpec` | `TaskTemplateSpec` | Service manifest should describe task templates |
| manifest `templates` | `task_templates` | Match the `TaskTemplate` child concept directly |
| `ServiceTemplate` in `services.capnp` | `TaskTemplate` | Child template type inside `ServiceSpec` |
| `ServiceTemplateNetwork` in `services.capnp` | `TaskTemplateNetwork` | Task-template network requirement |
| `WorkloadExecutionSpec<N>` | `ExecutionSpec<N>` | Shared execution template |
| `TaskExecutionSpec` | `ResolvedExecutionSpec` | Concrete execution spec with resolved network identifiers |
| `TaskStore` | `WorkloadStore` | Internal replicated store of shared workload rows |
| `TaskEvent` in gossip and sync | `WorkloadEvent` | Internal shared workload event stream |
| `Domain::Tasks` | `Domain::Workloads` | Internal sync domain |
| `WorkloadKind::{Job, AgentSession}` | remove | Controller records are not workload rows |
| `RuntimeClass` | split into substrate plus isolation vocabulary | Runtime substrate and isolation are separate concepts |
| `sandbox_profile` | `isolation_profile` | Rename only after the runtime split lands |

## Milestone 1: Service Task Template Terminology

### Goal

Keep service-owned replica bookkeeping distinct from template definitions while
making the child template names read naturally inside `ServiceSpec`.

### Status

Done on 2026-03-31 after correcting the first cut's target vocabulary.

Findings and scope adjustments:

1. The structural split was correct, but the interim `ServiceTemplate*` /
   `templates` vocabulary was removed completely in favor of `TaskTemplate*` /
   `task_templates`.
2. The hard cut still needed to include service example manifests and operator
   docs, since the manifest field now really is `task_templates`.
3. Service controller APIs that accept child template vectors now use
   `task_templates` explicitly. Leaving `submit_deployment(..., templates)` in
   place would still have obscured the parent-child relationship.
4. `replica_ids` remains the correct launched-replica term and was preserved.
5. Helper names and comments that still say `task ids` continue to refer to
   launched task rows owned by the service, not to task-template definitions.

Validation:

1. `cargo fmt --all`
2. `cargo clippy --all-targets -- -D warnings`
3. `cargo test`

### Planned changes

1. Rename current `ServiceTemplateSpecValue` to `TaskTemplateSpecValue`.
2. Rename current `ServiceTemplateNetworkRequirement` to
   `TaskTemplateNetworkRequirement`.
3. Rename `ServiceSpecValue.templates` to `task_templates`.
4. Keep `ServiceSpecValue.replica_ids` as the launched-replica bookkeeping.
5. Rename client manifest `ServiceTemplateSpec` to `TaskTemplateSpec`.
6. Rename client manifest `templates` to `task_templates`.
7. Rename `ServiceTemplate` in `services.capnp` to `TaskTemplate`.
8. Rename `ServiceTemplateNetwork` in `services.capnp` to
   `TaskTemplateNetwork`.
9. Rename helper functions and comments so `task template` refers only to
   service child templates and `task ids` refers only to launched task rows.

### Old names to remove in this milestone

1. current `ServiceTemplateSpecValue`
2. current `ServiceTemplateNetworkRequirement`
3. current `ServiceSpecValue.templates`
4. current manifest `ServiceTemplateSpec`
5. current manifest `templates`
6. current `ServiceTemplate` in `services.capnp`
7. current `ServiceTemplateNetwork` in `services.capnp`

### Code touchpoints

1. `src/services/types.rs`
   - current `ServiceTemplateSpecValue`
   - current `ServiceTemplateNetworkRequirement`
   - current `ServiceSpecValue.templates`
2. `src/services/service.rs`
3. `src/services/manager.rs`
4. `src/services/rollout.rs`
5. `src/services/reconcile.rs`
6. `src/services/dependencies.rs`
7. `src/services/ownership.rs`
8. `src/services/ordering.rs`
9. `src/services/readiness.rs`
10. `src/services/slot_reconcile.rs`
11. `src/services/registry.rs`
12. `src/network/discovery.rs`
13. `crates/client/src/services/manifest.rs`
   - current `ServiceTemplateSpec`
   - current `ServiceManifest.templates`
14. `crates/client/src/services/deploy.rs`
15. `crates/client/src/services/list.rs`
16. `crates/client/src/services/mod.rs`
17. `crates/protocol/schema/services.capnp`
   - current `ServiceTemplate`
   - current `ServiceTemplateNetwork`
   - current `templates`
18. `tests/services.rs`
19. `tests/discovery.rs`
20. `tests/stress_large_cluster.rs`
21. `examples/service_discovery_demo.ron`
22. `examples/replicated_service.ron`
23. `examples/rolling_update.ron`
24. `examples/attach_busybox_shell.ron`
25. `examples/gpu_smoketest.ron`
26. `examples/postgresql_local_volume.ron`
27. `docs/service-rollouts.md`
28. `docs/gpu-setup.md`
29. `docs/secrets.md`
30. `docs/workloads-and-runtimes.md`

### Outcome

1. `ServiceSpecValue` now carries `task_templates` and `replica_ids`.
2. Service child template structs now use `TaskTemplate*` names throughout the
   controller, client manifest layer, tests, examples, docs, and services RPC
   schema.
3. Service list output now distinguishes `TASK TEMPLATES` from `REPLICAS`.
4. Remaining `task` wording in service code refers either to child task
   templates or to actual launched task rows, never both at once.

## Milestone 2: Public Task API Boundary Cleanup

### Goal

Keep `WorkloadStartRequest` internal to the shared workload manager and expose
task-shaped naming again at the public task API boundary.

### Status

Pending.

### Planned changes

1. Introduce a public `TaskStartRequest` schema in `task.capnp`.
2. Change task RPC methods from `WorkloadStartRequest` to `TaskStartRequest`.
3. Convert public `TaskStartRequest` to internal `WorkloadStartRequest` inside
   `src/task/service.rs`.
4. Rename client request builders and tests to task-shaped request naming.
5. Remove task-RPC comments that only exist to justify the current mismatch.

### Old names to remove in this milestone

1. public `WorkloadStartRequest` in task RPC method signatures
2. task client builders that directly expose `WorkloadStartRequest`
3. transitional task RPC comments explaining the mismatch

### Code touchpoints

1. `crates/protocol/schema/task.capnp`
2. `src/task/service.rs`
3. `crates/client/src/tasks/start.rs`
4. `crates/client/src/tasks/mod.rs`
5. `src/app.rs`
6. `src/cli.rs`
7. `tests/task_secrets.rs`
8. `tests/volumes.rs`
9. `docs/workloads-and-runtimes.md`

### Questions to resolve while implementing

1. Whether `TaskStartRequest` should be a fully task-shaped schema or a direct
   mirror of the shared execution fields with task-specific naming only at the
   boundary.
2. Whether `startMany` should stay as one bulk task API or be renamed at the
   same time for consistency.

## Milestone 3: Shared Execution Type Names

### Goal

Stop using task-specific names for the shared execution template reused by
tasks, services, jobs, and agents.

### Status

Pending.

### Planned changes

1. Rename `WorkloadExecutionSpec<N>` to `ExecutionSpec<N>`.
2. Rename `TaskExecutionSpec` to `ResolvedExecutionSpec`.
3. Update all service, task, job, and agent code to use the new names.
4. Rename helpers and comments that still call the shared execution shape a
   "task execution" when it is used more broadly.

### Old names to remove in this milestone

1. `TaskExecutionSpec`
2. `WorkloadExecutionSpec`
3. helper names that use task wording for the shared execution template

### Code touchpoints

1. `src/workload/types.rs`
2. `src/task/service.rs`
3. `src/services/types.rs`
4. `src/services/service.rs`
5. `src/services/manager.rs`
6. `src/jobs/types.rs`
7. `src/jobs/service.rs`
8. `src/jobs/manager.rs`
9. `src/agents/types.rs`
10. `src/agents/service.rs`
11. `src/agents/manager.rs`
12. `tests/services.rs`
13. `tests/jobs.rs`
14. `tests/agents.rs`
15. `tests/volumes.rs`
16. `docs/workloads-and-runtimes.md`

### Questions to resolve while implementing

1. Whether the resolved versus unresolved network-ID split should stay encoded
   through type aliases, or whether the cleanup should introduce distinct
   struct names beyond the planned rename.
2. Whether task-service codec helpers should move to `execution` naming in the
   same patch to avoid another pass later.

## Milestone 4: Internal Workload Replication Vocabulary

### Goal

Make the internal replicated execution layer speak in workload terms instead of
task terms.

### Status

Pending.

### Planned changes

1. Rename `TaskStore` to `WorkloadStore`.
2. Rename `TaskTables` to `WorkloadTables`.
3. Rename `open_task_store()` to `open_workload_store()`.
4. Rename table names from `task_*` to `workload_*`.
5. Rename the sync domain from `Tasks` to `Workloads`.
6. Rename the gossip payload branch from `task` to `workload`.
7. Rename generic workload manager aliases and caches from task-shaped names to
   workload-shaped names.
8. If needed, extract generic workload replication types from `task.capnp` into
   a dedicated `workload.capnp` so task RPC and workload replication stop
   sharing one schema namespace.

### Old names to remove in this milestone

1. `src/store/task_store.rs`
2. `TaskStore`
3. `TaskTables`
4. `open_task_store`
5. `task_values`, `task_tombs`, `task_meta`
6. `Domain::Tasks`
7. gossip branch `task`
8. generic-layer task aliases inside `src/workload/manager/*`

### Code touchpoints

1. `src/store/task_store.rs` to delete and replace with `src/store/workload_store.rs`
2. `src/store/mod.rs`
3. `src/sync/mod.rs`
4. `src/sync/delta.rs`
5. `src/workload/manager/mod.rs`
6. `src/workload/manager/state.rs`
7. `src/workload/manager/runtime.rs`
8. `src/workload/manager/local.rs`
9. `src/workload/manager/reservation.rs`
10. `src/workload/manager/tests.rs`
11. `src/server/headless.rs`
12. `src/server/bootstrap/stores.rs`
13. `src/server/bootstrap/runtime.rs`
14. `src/topology/mod.rs`
15. `src/topology/service.rs`
16. `src/network/controller.rs`
17. `src/network/discovery.rs`
18. `crates/protocol/schema/gossip.capnp`
19. `crates/protocol/schema/task.capnp`
20. `crates/protocol/schema/workload.capnp` if introduced
21. `src/task/types.rs`
22. `src/task/service.rs`
23. `tests/task_attach.rs`
24. `tests/task_exec.rs`
25. `tests/task_logs.rs`
26. `tests/task_secrets.rs`
27. `tests/discovery.rs`
28. `tests/stress_large_cluster.rs`

### Questions to resolve while implementing

1. Whether the cleanup should include persisted table names immediately. The
   current repository rules point toward yes, since this is a hard cutover.
2. Whether extracting `workload.capnp` is cleaner than keeping generic workload
   replication types inside `task.capnp`.

## Milestone 5: Workload Ownership Semantics

### Goal

Make `WorkloadKind` describe only real schedulable workload rows and remove
controller-level records from that enum.

### Status

Pending.

### Planned changes

1. Remove `Job` and `AgentSession` from `WorkloadKind`.
2. Decide whether `AgentRun` remains in `WorkloadKind` based on whether the
   shared workload row records explicit agent-run ownership.
3. Add explicit workload ownership metadata instead of inferring only direct
   task versus service replica from `service_metadata`.
4. Stamp job attempts and agent runs with explicit ownership when they launch
   shared workload rows.
5. Update docs and comments so controller records are not described as
   workload kinds.

### Old names to remove in this milestone

1. dead `WorkloadKind` variants that are not workload rows
2. inference logic that only understands direct tasks and service replicas if
   explicit ownership metadata replaces it
3. comments that describe job controller records or agent sessions as workload
   kinds

### Code touchpoints

1. `src/workload/model.rs`
2. `src/workload/manager/mod.rs`
3. `src/workload/manager/local.rs`
4. `src/workload/manager/planner.rs`
5. `src/task/types.rs`
6. `src/services/types.rs`
7. `src/jobs/types.rs`
8. `src/jobs/manager.rs`
9. `src/agents/types.rs`
10. `src/agents/manager.rs`
11. `crates/protocol/schema/task.capnp`
12. `docs/workloads-and-runtimes.md`

### Questions to resolve while implementing

1. Whether the final enum should still be called `WorkloadKind`, or whether a
   name like `WorkloadOwnerKind` better reflects the concept after the cleanup.
2. Whether direct task listing should remain the only public place where
   standalone task ownership is surfaced explicitly.

## Milestone 6: Runtime Substrate Versus Isolation Terms

### Goal

Split the current overloaded runtime model into substrate vocabulary and
isolation vocabulary.

### Status

Pending.

### Planned changes

1. Replace `RuntimeClass` with a substrate enum such as
   `ExecutionSubstrate::{Oci, MicroVm}`.
2. Introduce a separate isolation field or mode plus `isolation_profile`.
3. Change runtime support advertisement to expose:
   - supported substrates,
   - supported isolation profiles,
   - feature flags.
4. Update scheduler intent and placement filtering to use substrate plus
   isolation explicitly.
5. Update agent defaults to request explicit substrate plus isolation instead
   of `RuntimeClass::Sandbox`.
6. Update CLI and protocol names from `sandbox_profile` to `isolation_profile`.

### Old names to remove in this milestone

1. `RuntimeClass::Sandbox`
2. `sandbox_profile`
3. `runtime_classes` as the only way to talk about sandbox support
4. comments and help strings that treat sandbox as if it were a peer substrate

### Code touchpoints

1. `src/workload/model.rs`
2. `src/runtime/types.rs`
3. `src/registry/mod.rs`
4. `src/topology/peers.rs`
5. `src/topology/service.rs`
6. `src/workload/manager/mod.rs`
7. `src/workload/manager/planner.rs`
8. `src/workload/manager/launch.rs`
9. `src/services/types.rs`
10. `src/jobs/types.rs`
11. `src/jobs/manager.rs`
12. `src/agents/types.rs`
13. `src/agents/manager.rs`
14. `src/agents/service.rs`
15. `src/runtime/testing/in_memory.rs`
16. `src/runtime/oci/docker/runtime.rs`
17. `crates/protocol/schema/task.capnp`
18. `crates/protocol/schema/agents.capnp`
19. `crates/client/src/agents/submit.rs`
20. `crates/client/src/agents/list.rs`
21. `crates/client/src/agents/runs.rs`
22. `src/cli.rs`
23. `src/app.rs`
24. `tests/agents.rs`
25. `tests/jobs.rs`
26. `tests/services.rs`
27. `tests/task_exec.rs`
28. `tests/task_attach.rs`
29. `tests/task_logs.rs`
30. `tests/task_secrets.rs`
31. `tests/volumes.rs`
32. `docs/workloads-and-runtimes.md`
33. `docs/configuration.md`

### Questions to resolve while implementing

1. Whether the cleanest final pair is `ExecutionSubstrate` plus
   `IsolationMode`, or `RuntimeSubstrate` plus `IsolationProfile`.
2. Whether Docker-backed sandbox support should be modeled as
   `Oci + sandboxed` or via a more explicit isolation enum.

## Milestone 7: Final Public Wording Sweep

### Goal

Sweep the remaining comments, docs, CLI help, protocol comments, and tests so
the repository reads coherently after the structural renames land.

### Status

Pending.

### Planned changes

1. Update CLI help strings to use either generic execution wording or precise
   backend-specific wording.
2. Remove generic `container` wording outside Docker backend modules.
3. Update docs to match the final names from milestones 1 through 6.
4. Update protocol comments so the Cap'n Proto files read naturally without
   transitional explanations.
5. Rename tests and helper functions that still use task or container wording
   where they now mean workload, template, replica, or runtime instance.

### Old names to remove in this milestone

1. stale explanatory comments that justify transitional names
2. generic `container` wording outside backend-specific code
3. remaining "task template" wording
4. remaining store or sync comments that say task when they mean workload

### Code touchpoints

1. `src/cli.rs`
2. `src/app.rs`
3. `docs/workloads-and-runtimes.md`
4. `docs/distributed-scheduler.md`
5. `docs/repo-layout.md`
6. `docs/service-rollouts.md`
7. `crates/protocol/schema/task.capnp`
8. `crates/protocol/schema/services.capnp`
9. `crates/protocol/schema/agents.capnp`
10. selected tests under `tests/`

### Questions to resolve while implementing

1. Which CLI help strings should remain substrate-specific because they are
   intentionally describing OCI container images or Docker behavior.
2. Whether any docs need a separate terminology glossary after the cleanup, or
   whether the main architecture docs become clear enough without one.

## Recommended Execution Order

The milestones should be executed in this order:

1. Milestone 1: service task template terminology
2. Milestone 2: public task API boundary cleanup
3. Milestone 3: shared execution type names
4. Milestone 4: internal workload replication vocabulary
5. Milestone 5: workload ownership semantics
6. Milestone 6: runtime substrate versus isolation terms
7. Milestone 7: final public wording sweep

This order deliberately front-loads the lower-risk, high-signal naming fixes
and postpones the runtime substrate split until the rest of the repository is
already speaking in consistent controller and workload terms.

## Current Status

Milestone 1 is now fully complete with the corrected `TaskTemplate*` /
`task_templates` vocabulary and with `replica_ids` retained for launched
service-owned replicas. Milestone 2 is the next pending step.
