use super::deployment::{ServiceSlotCutover, task_cutover_was_cancelled};
use super::inventory::{ServiceReplicaSnapshot, TaskInventory};
use super::placement::{
    SlotTargetContext, build_placement_preference_inventory, compute_effective_slot_targets,
    is_local_volume_unavailable_error, mounted_local_volumes_require_pinned_target,
};
use super::state::{
    deploying_assignment_incomplete, deploying_missing_slot_is_unknown, expected_task_id_count,
    node_is_down, should_restart_missing_slot_immediately, task_age_allows_cleanup,
    task_age_allows_rebalance, task_state_healthy, task_state_rebalanceable,
};
use super::{
    SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS, SERVICE_SLOT_CUTOVER_TIMEOUT,
    SERVICE_SLOT_MISSING_GRACE_SECS, ServiceController,
};
use crate::services::ownership::{
    ReplicaSlot, SlotKey, build_replica_slots, select_generation_owner, select_slot_owner,
    select_task_owner,
};
use crate::services::types::{ServiceSpecValue, ServiceStatus};
use crate::workload::model::{WorkloadPhase, WorkloadSpec};
use anyhow::anyhow;
use mantissa_health::Status as HealthStatus;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

struct SlotReconcileEnv<'a> {
    inventory: &'a TaskInventory,
    health_snapshot: &'a HashMap<Uuid, HealthStatus>,
    slot_targets: &'a HashMap<SlotKey, Uuid>,
    service_degraded: bool,
    owns_missing_repair: bool,
    owns_rebalance: bool,
}

/// Immutable inputs shared by the repair and healthy reconciliation paths for one slot.
struct SlotReconcileContext<'a> {
    spec: &'a ServiceSpecValue,
    slot: &'a ReplicaSlot,
    assigned_task_id: Uuid,
    key: &'a SlotKey,
    desired_node: Uuid,
    preferred_node: Option<Uuid>,
    health_snapshot: &'a HashMap<Uuid, HealthStatus>,
    service_degraded: bool,
    owns_missing_repair: bool,
    owns_rebalance: bool,
}

/// Observed task details that require the assigned slot to be repaired or replaced.
#[derive(Clone, Copy)]
struct SlotRepairObservation<'a> {
    task: Option<&'a WorkloadSpec>,
    task_on_draining_node: bool,
    task_owner_unavailable: bool,
}

/// Classifies the assigned task before reconciliation performs any slot mutations.
enum SlotTaskDisposition<'a> {
    /// The assigned task cannot access a volume that exists only on its current node.
    ///
    /// Moving the task cannot repair this state because no other node can mount the volume.
    /// Reconciliation therefore marks the service as `VolumeUnavailable` and leaves the assigned
    /// task in place. The same task can return to `Running` if the volume becomes available again.
    PinnedVolumeUnavailable,

    /// The assigned task is missing or cannot continue serving this replica slot.
    ///
    /// This includes a task row that is not visible locally, a task in an unhealthy state, a task
    /// on a draining node, or a task whose owner has left or is down. The repair path first adopts
    /// any replacement that was already started for this slot. If none exists, the elected slot
    /// owner starts a replacement after the delay selected by `SlotRepairDelay`.
    RepairRequired(SlotRepairObservation<'a>),

    /// The assigned task is visible, healthy, and hosted by an available non-draining node.
    ///
    /// No failure repair is needed. Reconciliation makes sure the task is published for service
    /// traffic, stops replacement tasks that are no longer valid, and lets the generation owner
    /// move the task if the current placement no longer matches the computed slot target.
    Healthy(&'a WorkloadSpec),
}

/// Delay policy that must elapse before a slot repair may create a replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotRepairDelay {
    /// Start a replacement during this reconciliation pass without waiting for another scan.
    ///
    /// This is used only when the controller has direct evidence that waiting cannot make the
    /// current assignment usable: its node is draining, its owner has left or is down, its
    /// deployment target is down, its deployment task is terminal, or a steady-state cluster split
    /// deliberately removed its workload row.
    Immediate,

    /// Wait for a newly assigned deployment task to appear in the local workload inventory.
    ///
    /// The service assignment and the workload inventory are replicated separately. A slot owner
    /// can therefore receive a service row containing the assigned task ID before it receives the
    /// workload row for that task. The task may already be starting or running on its target node,
    /// only its workload row is missing from this node's snapshot. Starting a replacement at this
    /// point would create two tasks for one replica slot. We thus wait for a delay until we receive
    /// the workload row.
    DeploymentVisibilityGrace,

    /// Wait for a temporarily missing or unhealthy task to become visible and healthy again.
    ///
    /// This is the default when the controller has no direct evidence that the assignment is
    /// permanently lost. The first observation records a `SERVICE_SLOT_MISSING_GRACE_SECS`
    /// deadline. Later reconciliation passes replace the task only if the slot is still unhealthy
    /// after that deadline, observing a healthy task before then clears the deadline.
    MissingGrace,
}

/// Task and target identities involved in a start-first slot replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SlotReplacement {
    previous_task_id: Uuid,
    replacement_task_id: Uuid,
    replacement_node_id: Uuid,
}

/// One observed task whose handoff still applies to the current service slot assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SlotHandoffCandidate {
    task_id: Uuid,
    node_id: Uuid,
}

/// One unchanged healthy placement proposal waiting out its stability window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SlotRebalancePlan {
    task_id: Uuid,
    current_node_id: Uuid,
    desired_node_id: Uuid,
    view_revision: u64,
    pub(super) observed_at: Instant,
}

impl SlotRebalancePlan {
    /// Returns whether two observations describe the same proposed slot movement.
    fn describes_same_move(&self, other: &Self) -> bool {
        self.task_id == other.task_id
            && self.current_node_id == other.current_node_id
            && self.desired_node_id == other.desired_node_id
            && self.view_revision == other.view_revision
    }
}

/// Returns true when generic cleanup must leave a handoff to service-slot reconciliation.
fn handoff_requires_slot_reconciliation(spec: &ServiceSpecValue, task: &WorkloadSpec) -> bool {
    task.service_owner().is_some_and(|metadata| {
        service_metadata_handoff_requires_slot_reconciliation(spec.service_epoch, metadata)
    })
}

/// Returns true while handoff metadata is current or ahead of visible service state.
fn service_metadata_handoff_requires_slot_reconciliation(
    service_epoch: u64,
    metadata: &crate::workload::model::WorkloadServiceMetadata,
) -> bool {
    metadata.handoff.is_some() && metadata.service_epoch >= service_epoch
}

/// Chooses an observed handoff deterministically, preferring the current placement target.
fn select_slot_handoff_candidate(
    candidates: &[SlotHandoffCandidate],
    preferred_node: Option<Uuid>,
) -> Option<SlotHandoffCandidate> {
    candidates.iter().copied().min_by_key(|candidate| {
        (
            preferred_node.is_none_or(|node_id| node_id != candidate.node_id),
            candidate.task_id,
        )
    })
}

/// Returns the preferred slot target only while the target is still eligible.
///
/// Reconciliation computes placement targets from a point-in-time eligible-node snapshot, but drain
/// metadata can arrive before a slot actually starts its replacement. Re-checking live
/// schedulability here prevents an evacuation from aiming the fresh task back at a newly drained
/// node.
fn preferred_slot_node(
    desired_node: Uuid,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
    schedulable: bool,
) -> Option<Uuid> {
    if !schedulable || node_is_down(desired_node, health_snapshot) {
        None
    } else {
        Some(desired_node)
    }
}

/// Chooses the delay policy for one repair-required slot observation.
///
/// Deployment visibility takes precedence over split-prune hints because an assigned workload row
/// can still be in flight while its target remains healthy. Concrete placement failure, terminal
/// deployment state, and steady-state split pruning may bypass the normal missing-slot grace.
fn slot_repair_delay(
    status: ServiceStatus,
    task: Option<&WorkloadSpec>,
    desired_node: Uuid,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
    split_pruned: bool,
    task_on_draining_node: bool,
    task_owner_unavailable: bool,
) -> SlotRepairDelay {
    if deploying_missing_slot_is_unknown(status, task, desired_node, health_snapshot) {
        return SlotRepairDelay::DeploymentVisibilityGrace;
    }

    let deploying_absent_target_down = status == ServiceStatus::Deploying
        && task.is_none()
        && node_is_down(desired_node, health_snapshot);
    if task_on_draining_node
        || task_owner_unavailable
        || split_pruned
        || deploying_absent_target_down
        || should_restart_missing_slot_immediately(status, task)
    {
        SlotRepairDelay::Immediate
    } else {
        SlotRepairDelay::MissingGrace
    }
}

/// Returns true for cleanup stop errors that are expected during rapid view transitions.
fn service_cleanup_stop_error_is_transient(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        let detail = cause.to_string();
        detail.contains("unknown task")
            || detail.contains("no active session for peer")
            || detail.contains("cluster view mismatch")
            || detail.contains("peer session revoked")
    })
}

/// Returns true when the task owner cannot make progress for service reconciliation.
///
/// SWIM `Down` and explicit cluster leave both make remote workload stop impossible. Unknown
/// membership is left retryable so temporary peer metadata lag does not retire healthy tasks.
fn service_task_owner_unavailable_for_cleanup(
    node_id: Uuid,
    local_node_id: Uuid,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
    owner_active_cluster_member: Option<bool>,
) -> bool {
    if node_id == local_node_id {
        return false;
    }

    node_is_down(node_id, health_snapshot) || matches!(owner_active_cluster_member, Some(false))
}

impl ServiceController {
    /// Reconciles each replica slot owned by this node so rescheduling is distributed per-slot.
    pub(super) async fn reconcile_service(
        &self,
        spec: ServiceSpecValue,
        inventory: &TaskInventory,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        eligible_nodes: &[Uuid],
    ) -> anyhow::Result<()> {
        if eligible_nodes.is_empty() {
            return Ok(());
        }

        // During initial deployment submission, the service spec is broadcast as Deploying with
        // task templates populated but replica_ids still empty while launch is in-flight. Running the
        // normal cleanup/slot loops at that point causes false "excess task" stops and churn.
        if deploying_assignment_incomplete(&spec) {
            tracing::debug!(
                target: "services",
                service = %spec.service_name,
                expected_slots = expected_task_id_count(&spec),
                assigned_slots = spec.assigned_replica_count(),
                "skipping deploy reconciliation until task ids are fully assigned"
            );
            return Ok(());
        }

        let slots = build_replica_slots(&spec);
        let placement_nodes = self.placement_nodes_for(eligible_nodes);
        let preference_inventory =
            build_placement_preference_inventory(&self.workload_manager).await?;
        let slot_targets = compute_effective_slot_targets(&SlotTargetContext {
            service_name: &spec.service_name,
            service_id: spec.id,
            service_epoch: spec.service_epoch,
            task_templates: &spec.task_templates,
            eligible_nodes,
            placement_nodes: &placement_nodes,
            preference_inventory: &preference_inventory,
            network_registry: &self.network_registry,
            volume_registry: &self.volume_registry,
        })?;
        let desired_ids: HashSet<Uuid> = slots.iter().filter_map(|slot| slot.replica_id).collect();
        let service_tasks = inventory.service_task_snapshot(&spec.service_name, desired_ids);
        let service_degraded = slots.iter().any(|slot| {
            let Some(task_id) = slot.replica_id else {
                return true;
            };
            let Some(task) = inventory.by_id.get(&task_id) else {
                return true;
            };
            // `None` means the peer row has not converged here yet, so keep it retryable.
            // `Some(false)` is an explicit leave tombstone and must degrade the service.
            let owner_active_cluster_member = self
                .cluster_registry
                .peer_active_in_local_view(task.node_id);
            service_task_owner_unavailable_for_cleanup(
                task.node_id,
                self.local_node_id,
                health_snapshot,
                owner_active_cluster_member,
            ) || !task_state_healthy(&task.state)
        });

        if spec.status() == ServiceStatus::VolumeUnavailable && !service_degraded {
            self.restore_volume_available_service(&spec).await?;
        }

        self.reconcile_extra_tasks(&spec, &service_tasks, eligible_nodes, health_snapshot)
            .await;

        // Missing replicas remain sharded by slot so independent failures heal in parallel.
        // Healthy placement moves update the whole service assignment row, so one deterministic
        // generation owner performs those moves sequentially and avoids lost concurrent updates.
        let generation_owner = select_generation_owner(spec.id, spec.service_epoch, eligible_nodes);

        for slot in slots {
            let Some(task_id) = slot.replica_id else {
                tracing::warn!(
                    target: "services",
                    "service '{}' missing task id for template '{}' replica {}; skipping slot",
                    spec.service_name,
                    slot.template.name,
                    slot.replica
                );
                continue;
            };

            let Some(slot_owner) =
                select_slot_owner(spec.id, &slot.template.name, slot.replica, eligible_nodes)
            else {
                continue;
            };
            let owns_missing_repair = slot_owner == self.local_node_id;
            let owns_rebalance = generation_owner == Some(self.local_node_id);
            if !owns_missing_repair && !owns_rebalance {
                continue;
            }

            let key = SlotKey::new(spec.id, &slot.template.name, slot.replica);
            let Some(_guard) = self.try_begin_slot(&key).await else {
                continue;
            };

            if let Err(err) = self
                .reconcile_slot(
                    &spec,
                    &slot,
                    task_id,
                    SlotReconcileEnv {
                        inventory,
                        health_snapshot,
                        slot_targets: &slot_targets,
                        service_degraded,
                        owns_missing_repair,
                        owns_rebalance,
                    },
                    &key,
                )
                .await
            {
                if task_cutover_was_cancelled(&err) {
                    tracing::debug!(
                        target: "services",
                        "slot replacement for '{}' replica {} was canceled: {err}",
                        slot.template.name,
                        slot.replica
                    );
                } else {
                    tracing::warn!(
                        target: "services",
                        "slot reconciliation failed for '{}' replica {}: {err}",
                        slot.template.name,
                        slot.replica
                    );
                }
            }
        }

        Ok(())
    }

    /// Stops tasks no longer referenced by the service spec using deterministic cleanup ownership.
    async fn reconcile_extra_tasks(
        &self,
        spec: &ServiceSpecValue,
        service_tasks: &ServiceReplicaSnapshot<'_>,
        eligible_nodes: &[Uuid],
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) {
        let mut remote_cleanup_owners = BTreeSet::new();

        for task in service_tasks.observed_tasks() {
            if service_tasks.is_desired(task.id) {
                continue;
            }
            if !task_state_healthy(&task.state) {
                continue;
            }
            if !task_age_allows_cleanup(task, self.timing.cleanup_min_age) {
                continue;
            }
            if handoff_requires_slot_reconciliation(spec, task) {
                let Some(metadata) = task.service_owner() else {
                    continue;
                };
                if metadata.service_epoch == spec.service_epoch
                    && let Some(owner) =
                        select_generation_owner(spec.id, spec.service_epoch, eligible_nodes)
                    && owner != self.local_node_id
                    && task.node_id == self.local_node_id
                {
                    // Only the runtime owner advertises this row to the generation owner. Every
                    // node may eventually observe the same handoff through anti-entropy; letting
                    // every observer send this hint creates all-to-all workload-only sync traffic.
                    remote_cleanup_owners.insert(owner);
                }
                continue;
            }
            let owner_active_cluster_member = self
                .cluster_registry
                .peer_active_in_local_view(task.node_id);
            let runtime_owner_unavailable = service_task_owner_unavailable_for_cleanup(
                task.node_id,
                self.local_node_id,
                health_snapshot,
                owner_active_cluster_member,
            );
            let cleanup_owner = if task.node_id == self.local_node_id {
                // The runtime owner already has the workload row and can stop the task without
                // making another node pull the entire workload domain first.
                Some(self.local_node_id)
            } else if runtime_owner_unavailable {
                // A departed runtime owner cannot perform local cleanup. Elect one active node to
                // retire its stale row after anti-entropy makes that row visible there.
                select_task_owner(task.id, eligible_nodes)
            } else {
                None
            };

            let Some(cleanup_owner) = cleanup_owner else {
                continue;
            };
            if cleanup_owner != self.local_node_id {
                remote_cleanup_owners.insert(cleanup_owner);
                continue;
            }

            if let Err(err) = self.workload_manager.request_workload_stop(task.id).await {
                if service_cleanup_stop_error_is_transient(&err) {
                    tracing::debug!(
                        target: "services",
                        task = %task.id,
                        service = %spec.service_name,
                        "deferred excess task cleanup after transient stop failure: {err:#}"
                    );
                } else {
                    tracing::warn!(
                        target: "services",
                        "failed to stop excess task {} for '{}': {err:#}",
                        task.id,
                        spec.service_name
                    );
                }
                self.retire_unavailable_service_task_best_effort(
                    &spec.service_name,
                    task.id,
                    health_snapshot,
                )
                .await;
            }
        }

        for owner in remote_cleanup_owners {
            self.workload_manager
                .notify_workload_rows_available(owner)
                .await;
        }
    }

    /// Reconciles a single slot owned by this node, restarting or rebalancing as needed.
    async fn reconcile_slot(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task_id: Uuid,
        env: SlotReconcileEnv<'_>,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        let Some(desired_node) = env.slot_targets.get(key).copied() else {
            return Ok(());
        };

        let preferred_node = preferred_slot_node(
            desired_node,
            env.health_snapshot,
            self.cluster_registry.peer_schedulable(desired_node),
        );

        let requires_pinned_target = mounted_local_volumes_require_pinned_target(
            &self.volume_registry,
            &slot.template.volumes,
        )?;

        let task = env.inventory.by_id.get(&task_id);
        let disposition =
            match self.classify_slot_task(task, requires_pinned_target, env.health_snapshot) {
                SlotTaskDisposition::PinnedVolumeUnavailable => {
                    // Imported and other pinned local volumes recover in place once the node-local
                    // path returns. Promote the service immediately instead of waiting for a restart
                    // attempt to rediscover the same local volume error.
                    self.mark_service_volume_unavailable(spec).await?;
                    return Ok(());
                }
                disposition => disposition,
            };

        let context = SlotReconcileContext {
            spec,
            slot,
            assigned_task_id: task_id,
            key,
            desired_node,
            preferred_node,
            health_snapshot: env.health_snapshot,
            service_degraded: env.service_degraded,
            owns_missing_repair: env.owns_missing_repair,
            owns_rebalance: env.owns_rebalance,
        };
        let handoff_candidates = self
            .collect_slot_handoff_candidates(
                spec,
                slot,
                task_id,
                env.inventory,
                env.health_snapshot,
            )
            .await;

        match disposition {
            SlotTaskDisposition::RepairRequired(observation) => {
                self.reconcile_repair_required_slot(&context, observation, &handoff_candidates)
                    .await
            }
            SlotTaskDisposition::Healthy(task) => {
                self.reconcile_healthy_slot(&context, task, handoff_candidates)
                    .await
            }
            SlotTaskDisposition::PinnedVolumeUnavailable => Ok(()),
        }
    }

    /// Classifies the assigned task into the next slot reconciliation path.
    fn classify_slot_task<'a>(
        &self,
        task: Option<&'a WorkloadSpec>,
        requires_pinned_target: bool,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) -> SlotTaskDisposition<'a> {
        if matches!(
            task.map(|task| &task.state),
            Some(WorkloadPhase::VolumeUnavailable)
        ) && requires_pinned_target
        {
            return SlotTaskDisposition::PinnedVolumeUnavailable;
        }

        let task_on_draining_node = task
            .map(|task| self.node_drain_requested(task.node_id))
            .unwrap_or(false);

        let task_owner_unavailable = task
            .map(|task| {
                // Health Down and explicit Left both mean a remote stop RPC cannot make progress.
                // Unknown topology is intentionally not terminal so sync lag does not retire work.
                let owner_active_cluster_member = self
                    .cluster_registry
                    .peer_active_in_local_view(task.node_id);
                service_task_owner_unavailable_for_cleanup(
                    task.node_id,
                    self.local_node_id,
                    health_snapshot,
                    owner_active_cluster_member,
                )
            })
            .unwrap_or(false);

        let observation = SlotRepairObservation {
            task,
            task_on_draining_node,
            task_owner_unavailable,
        };

        match task {
            Some(task)
                if !task_on_draining_node
                    && !task_owner_unavailable
                    && task_state_healthy(&task.state) =>
            {
                SlotTaskDisposition::Healthy(task)
            }
            _ => SlotTaskDisposition::RepairRequired(observation),
        }
    }

    /// Repairs an absent, unhealthy, draining, or unavailable slot assignment.
    async fn reconcile_repair_required_slot(
        &self,
        context: &SlotReconcileContext<'_>,
        observation: SlotRepairObservation<'_>,
        handoff_candidates: &[SlotHandoffCandidate],
    ) -> anyhow::Result<()> {
        self.clear_rebalance_plan(context.key).await;

        if let Some(candidate) =
            select_slot_handoff_candidate(handoff_candidates, context.preferred_node)
        {
            if !context.owns_rebalance {
                return Ok(());
            }
            self.resume_slot_handoff(
                context.spec,
                context.slot,
                SlotReplacement {
                    previous_task_id: context.assigned_task_id,
                    replacement_task_id: candidate.task_id,
                    replacement_node_id: candidate.node_id,
                },
                handoff_candidates,
                context.health_snapshot,
                context.key,
            )
            .await?;
            return Ok(());
        }
        if !context.owns_missing_repair {
            return Ok(());
        }

        if observation.task_on_draining_node {
            tracing::debug!(
                target: "services",
                service = %context.spec.service_name,
                template = %context.slot.template.name,
                replica = context.slot.replica,
                task = %context.assigned_task_id,
                "slot task is assigned to a draining node; forcing evacuation"
            );
        }

        if observation.task_owner_unavailable {
            tracing::debug!(
                target: "services",
                service = %context.spec.service_name,
                template = %context.slot.template.name,
                replica = context.slot.replica,
                task = %context.assigned_task_id,
                "slot task owner is unavailable; forcing replacement"
            );
        }

        let split_pruned = observation.task.is_none()
            && self
                .reconcile_trigger
                .workload_was_split_pruned(context.assigned_task_id);

        let delay = slot_repair_delay(
            context.spec.status(),
            observation.task,
            context.desired_node,
            context.health_snapshot,
            split_pruned,
            observation.task_on_draining_node,
            observation.task_owner_unavailable,
        );

        if !self
            .slot_repair_delay_elapsed(context, observation.task, delay)
            .await
        {
            return Ok(());
        }

        self.start_slot_task(
            context.spec,
            context.slot,
            context.assigned_task_id,
            context.preferred_node,
            context.health_snapshot,
            context.key,
        )
        .await
    }

    /// Returns true once the selected repair delay has elapsed, logging exceptional bypasses.
    async fn slot_repair_delay_elapsed(
        &self,
        context: &SlotReconcileContext<'_>,
        task: Option<&WorkloadSpec>,
        delay: SlotRepairDelay,
    ) -> bool {
        match delay {
            SlotRepairDelay::Immediate => {
                if context.spec.status() == ServiceStatus::Deploying
                    && task.is_none()
                    && node_is_down(context.desired_node, context.health_snapshot)
                {
                    tracing::debug!(
                        target: "services",
                        service = %context.spec.service_name,
                        template = %context.slot.template.name,
                        replica = context.slot.replica,
                        task = %context.assigned_task_id,
                        target = %context.desired_node,
                        "deployment slot task is absent and its assigned target is down; allowing replacement without visibility grace"
                    );
                }
                true
            }
            SlotRepairDelay::DeploymentVisibilityGrace => {
                let elapsed = self
                    .slot_missing_elapsed_after(
                        context.key,
                        Duration::from_secs(SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS),
                    )
                    .await;

                if elapsed {
                    tracing::debug!(
                        target: "services",
                        service = %context.spec.service_name,
                        template = %context.slot.template.name,
                        replica = context.slot.replica,
                        task = %context.assigned_task_id,
                        target = %context.desired_node,
                        visibility_grace_secs = SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS,
                        "deployment visibility grace elapsed for absent slot task; allowing replacement"
                    );
                } else {
                    tracing::debug!(
                        target: "services",
                        service = %context.spec.service_name,
                        template = %context.slot.template.name,
                        replica = context.slot.replica,
                        task = %context.assigned_task_id,
                        target = %context.desired_node,
                        visibility_grace_secs = SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS,
                        "slot task row is not locally visible during deployment; waiting for direct assignment delivery or workload MST sync"
                    );
                }
                elapsed
            }
            SlotRepairDelay::MissingGrace => self.slot_missing_elapsed(context.key).await,
        }
    }

    /// Reconciles traffic, handoffs, and placement for one healthy assigned task.
    async fn reconcile_healthy_slot(
        &self,
        context: &SlotReconcileContext<'_>,
        task: &WorkloadSpec,
        handoff_candidates: Vec<SlotHandoffCandidate>,
    ) -> anyhow::Result<()> {
        self.clear_slot_missing(context.key).await;

        if context.spec.status() == ServiceStatus::Running
            && task_state_healthy(&task.state)
            && !node_is_down(task.node_id, context.health_snapshot)
        {
            self.publish_running_task_traffic_best_effort(&context.spec.service_name, task.id)
                .await;
        }

        // A visible candidate proves that a controller already started replacing this slot. Nodes
        // can temporarily calculate different targets from independent health snapshots, so a
        // target mismatch is not evidence that the candidate is stale. Complete one candidate
        // deterministically, preferring this observer's target. The guarded slot update chooses one
        // winner, and that assignment change gives every observer causal proof to stop the losers.
        if let Some(candidate) =
            select_slot_handoff_candidate(&handoff_candidates, Some(context.desired_node))
        {
            self.clear_rebalance_plan(context.key).await;
            if !context.owns_rebalance {
                return Ok(());
            }
            self.resume_slot_handoff(
                context.spec,
                context.slot,
                SlotReplacement {
                    previous_task_id: context.assigned_task_id,
                    replacement_task_id: candidate.task_id,
                    replacement_node_id: candidate.node_id,
                },
                &handoff_candidates,
                context.health_snapshot,
                context.key,
            )
            .await?;
            self.set_rebalance_cooldown(context.key).await;
            return Ok(());
        }

        if context.desired_node == task.node_id {
            self.clear_rebalance_plan(context.key).await;
            return Ok(());
        }

        // Missing slots are healed by their slot owners above. Healthy slots are moved only by the
        // generation owner because each cutover rewrites the shared service assignment row.
        let service_can_rebalance = context.spec.status() == ServiceStatus::Running
            && context.owns_rebalance
            && context.slot.template.replicas > 1
            && !context.service_degraded;

        let task_can_rebalance = task_state_rebalanceable(&task.state)
            && task_age_allows_rebalance(task, self.timing.rebalance_min_age);

        if !service_can_rebalance || !task_can_rebalance {
            self.clear_rebalance_plan(context.key).await;
            return Ok(());
        }

        let cooldown_elapsed = self.rebalance_allowed(context.key).await;
        let target_is_available = !node_is_down(context.desired_node, context.health_snapshot);

        if !cooldown_elapsed || !target_is_available {
            self.clear_rebalance_plan(context.key).await;
            return Ok(());
        }

        if !self
            .rebalance_plan_is_stable(context.key, task, context.desired_node)
            .await
        {
            return Ok(());
        }

        self.move_slot_task(
            context.spec,
            context.slot,
            task,
            context.desired_node,
            context.health_snapshot,
            context.key,
        )
        .await
    }

    /// Collects valid handoffs and retires only candidates proven obsolete by replicated state.
    async fn collect_slot_handoff_candidates(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        current_task_id: Uuid,
        inventory: &TaskInventory,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) -> Vec<SlotHandoffCandidate> {
        let mut candidates = Vec::new();
        for task in inventory.service_slot_tasks(
            &spec.service_name,
            spec.service_epoch,
            &slot.template.name,
            slot.replica,
        ) {
            if task.id == current_task_id || !task_state_healthy(&task.state) {
                continue;
            }
            let Some(metadata) = task.service_owner() else {
                continue;
            };
            let Some(handoff) = metadata.handoff.as_ref() else {
                continue;
            };
            if handoff.previous_task_id != current_task_id {
                self.abort_replacement_task_best_effort(
                    &spec.service_name,
                    task.id,
                    "handoff source no longer owns service slot",
                )
                .await;
                continue;
            }

            let owner_active_cluster_member = self
                .cluster_registry
                .peer_active_in_local_view(task.node_id);
            if self.node_drain_requested(task.node_id)
                || service_task_owner_unavailable_for_cleanup(
                    task.node_id,
                    self.local_node_id,
                    health_snapshot,
                    owner_active_cluster_member,
                )
            {
                self.abort_replacement_task_best_effort(
                    &spec.service_name,
                    task.id,
                    "handoff target is no longer available",
                )
                .await;
                continue;
            }

            candidates.push(SlotHandoffCandidate {
                task_id: task.id,
                node_id: task.node_id,
            });
        }
        candidates
    }

    /// Resumes one observed handoff and lets the service slot update settle concurrent candidates.
    async fn resume_slot_handoff(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        replacement: SlotReplacement,
        candidates: &[SlotHandoffCandidate],
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        if spec.status() == ServiceStatus::Running
            && let Err(err) = self
                .publish_task_traffic_for_cutover(
                    &spec.service_name,
                    replacement.replacement_task_id,
                    replacement.replacement_node_id,
                    SERVICE_SLOT_CUTOVER_TIMEOUT,
                )
                .await
        {
            self.abort_replacement_task_best_effort(
                &spec.service_name,
                replacement.replacement_task_id,
                "adopted replacement never became traffic-ready",
            )
            .await;
            return Err(err);
        }

        if self
            .settle_slot_replacement(
                spec,
                slot,
                replacement,
                health_snapshot,
                key,
                "adopted replacement could not claim service slot",
            )
            .await?
        {
            self.abort_slot_handoff_candidates(
                &spec.service_name,
                candidates,
                Some(replacement.replacement_task_id),
                "concurrent replacement lost service slot cutover",
            )
            .await;
        }
        Ok(())
    }

    /// Stops observed handoff candidates other than an optional accepted replacement.
    async fn abort_slot_handoff_candidates(
        &self,
        service_name: &str,
        candidates: &[SlotHandoffCandidate],
        accepted_task_id: Option<Uuid>,
        context: &str,
    ) {
        for candidate in candidates {
            if Some(candidate.task_id) == accepted_task_id {
                continue;
            }
            self.abort_replacement_task_best_effort(service_name, candidate.task_id, context)
                .await;
        }
    }

    /// Starts or restarts a replica slot on the preferred node, falling back only when safe.
    async fn start_slot_task(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task_id: Uuid,
        preferred_node: Option<Uuid>,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        let requires_pinned_target = mounted_local_volumes_require_pinned_target(
            &self.volume_registry,
            &slot.template.volumes,
        )?;
        if preferred_node.is_none() && requires_pinned_target {
            self.mark_service_volume_unavailable(spec).await?;
            return Ok(());
        }

        let replacement_task_id = Uuid::new_v4();
        if let Some(preferred_node) = preferred_node {
            let request = slot.template.replica_handoff_start_request(
                &spec.service_name,
                spec.service_epoch,
                slot.replica,
                task_id,
                replacement_task_id,
                Some(preferred_node),
            );

            match self
                .workload_manager
                .start_workloads_batch(vec![request])
                .await
            {
                Ok(specs) => {
                    if specs.len() != 1 {
                        tracing::warn!(
                            target: "services",
                            "unexpected start response for '{}' replica {}: expected 1, got {}",
                            slot.template.name,
                            slot.replica,
                            specs.len()
                        );
                    }
                    if spec.status() == ServiceStatus::Running
                        && let Err(err) = self
                            .publish_task_traffic_for_cutover(
                                &spec.service_name,
                                replacement_task_id,
                                preferred_node,
                                SERVICE_SLOT_CUTOVER_TIMEOUT,
                            )
                            .await
                    {
                        self.abort_replacement_task_best_effort(
                            &spec.service_name,
                            replacement_task_id,
                            "preferred replacement never became traffic-ready",
                        )
                        .await;
                        return Err(err);
                    }
                    if !self
                        .settle_slot_replacement(
                            spec,
                            slot,
                            SlotReplacement {
                                previous_task_id: task_id,
                                replacement_task_id,
                                replacement_node_id: preferred_node,
                            },
                            health_snapshot,
                            key,
                            "preferred replacement could not claim service slot",
                        )
                        .await?
                    {
                        return Ok(());
                    }
                    return Ok(());
                }
                Err(err) => {
                    if requires_pinned_target {
                        if is_local_volume_unavailable_error(&err) {
                            self.mark_service_volume_unavailable(spec).await?;
                            return Ok(());
                        }
                        return Err(err);
                    }
                    tracing::debug!(
                        target: "services",
                        "preferred placement failed for '{}' replica {} on {}: {err}",
                        slot.template.name,
                        slot.replica,
                        preferred_node
                    );
                }
            }
        }

        if requires_pinned_target {
            return Ok(());
        }

        let fallback = slot.template.replica_handoff_start_request(
            &spec.service_name,
            spec.service_epoch,
            slot.replica,
            task_id,
            replacement_task_id,
            None,
        );

        let fallback_specs = self
            .workload_manager
            .start_workloads_batch(vec![fallback])
            .await
            .map_err(|err| anyhow!("fallback placement failed: {err}"))?;
        if fallback_specs.len() != 1 {
            tracing::warn!(
                target: "services",
                "fallback placement mismatch for '{}' replica {}: expected 1, got {}",
                slot.template.name,
                slot.replica,
                fallback_specs.len()
            );
        }
        let fallback_node_id = fallback_specs
            .iter()
            .find(|fallback_spec| fallback_spec.id == replacement_task_id)
            .map(|fallback_spec| fallback_spec.node_id);
        let Some(fallback_node_id) = fallback_node_id else {
            self.abort_replacement_task_best_effort(
                &spec.service_name,
                replacement_task_id,
                "fallback placement returned no matching replacement",
            )
            .await;
            return Err(anyhow!(
                "fallback placement did not return replacement task {replacement_task_id}"
            ));
        };

        if spec.status() == ServiceStatus::Running
            && let Err(err) = self
                .publish_task_traffic_for_cutover(
                    &spec.service_name,
                    replacement_task_id,
                    fallback_node_id,
                    SERVICE_SLOT_CUTOVER_TIMEOUT,
                )
                .await
        {
            self.abort_replacement_task_best_effort(
                &spec.service_name,
                replacement_task_id,
                "fallback replacement never became traffic-ready",
            )
            .await;
            return Err(err);
        }
        if !self
            .settle_slot_replacement(
                spec,
                slot,
                SlotReplacement {
                    previous_task_id: task_id,
                    replacement_task_id,
                    replacement_node_id: fallback_node_id,
                },
                health_snapshot,
                key,
                "fallback replacement could not claim service slot",
            )
            .await?
        {
            return Ok(());
        }
        Ok(())
    }

    /// Marks the current service generation as blocked on node-local volume availability.
    async fn mark_service_volume_unavailable(&self, spec: &ServiceSpecValue) -> anyhow::Result<()> {
        let Some(mut current) = self.registry.get(spec.id)? else {
            return Ok(());
        };
        if current.manifest_id != spec.manifest_id {
            return Ok(());
        }
        if current.status() == ServiceStatus::VolumeUnavailable {
            return Ok(());
        }
        current.set_status(ServiceStatus::VolumeUnavailable);
        self.apply_upsert(current.clone()).await?;
        self.broadcast(crate::services::types::ServiceEvent::Upsert(current))
            .await?;
        Ok(())
    }

    /// Restores a service to `Running` once every desired task is healthy again.
    async fn restore_volume_available_service(
        &self,
        spec: &ServiceSpecValue,
    ) -> anyhow::Result<()> {
        let Some(mut current) = self.registry.get(spec.id)? else {
            return Ok(());
        };
        if current.manifest_id != spec.manifest_id {
            return Ok(());
        }
        if current.status() != ServiceStatus::VolumeUnavailable {
            return Ok(());
        }
        current.set_status(ServiceStatus::Running);
        self.apply_upsert(current.clone()).await?;
        self.broadcast(crate::services::types::ServiceEvent::Upsert(current))
            .await?;
        Ok(())
    }

    /// Moves a replica to the preferred node using a fresh-identity start-first handoff.
    ///
    /// Overlay addressing is derived from task id, so reusing the old task id across old and new
    /// placements makes both nodes advertise the same IP, MAC, and attachment identity during the
    /// handoff window. Start the replacement with a new task id, wait for it to become
    /// traffic-ready, then atomically switch the service slot to the new identity before
    /// withdrawing and stopping the superseded task.
    async fn move_slot_task(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task: &WorkloadSpec,
        preferred_node: Uuid,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        let replacement_task_id = Uuid::new_v4();
        let request = slot.template.replica_handoff_start_request(
            &spec.service_name,
            spec.service_epoch,
            slot.replica,
            task.id,
            replacement_task_id,
            Some(preferred_node),
        );

        self.workload_manager
            .start_workloads_batch(vec![request])
            .await
            .map_err(|err| {
                anyhow!(
                    "rebalance placement failed for '{}' replica {} on {}: {err}",
                    slot.template.name,
                    slot.replica,
                    preferred_node
                )
            })?;

        if let Err(err) = self
            .publish_task_traffic_for_cutover(
                &spec.service_name,
                replacement_task_id,
                preferred_node,
                SERVICE_SLOT_CUTOVER_TIMEOUT,
            )
            .await
        {
            self.abort_replacement_task_best_effort(
                &spec.service_name,
                replacement_task_id,
                "rebalance replacement never became traffic-ready",
            )
            .await;
            return Err(err);
        }
        if !self
            .settle_slot_replacement(
                spec,
                slot,
                SlotReplacement {
                    previous_task_id: task.id,
                    replacement_task_id,
                    replacement_node_id: preferred_node,
                },
                health_snapshot,
                key,
                "rebalance replacement could not claim service slot",
            )
            .await?
        {
            self.set_rebalance_cooldown(key).await;
            return Ok(());
        }
        self.set_rebalance_cooldown(key).await;

        tracing::debug!(
            target: "services",
            service = %spec.service_name,
            template = %slot.template.name,
            replica = slot.replica,
            old_task = %task.id,
            replacement_task = %replacement_task_id,
            previous_node = %task.node_id,
            preferred_node = %preferred_node,
            "rebalance replacement accepted and cut over to a fresh task identity"
        );

        Ok(())
    }

    /// Settles a start-first slot replacement and returns true when this replacement won.
    async fn settle_slot_replacement(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        replacement: SlotReplacement,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
        key: &SlotKey,
        abort_context: &str,
    ) -> anyhow::Result<bool> {
        let outcome = match self
            .swap_service_slot_task_id_for_cutover(
                spec.id,
                spec.manifest_id,
                &slot.template.name,
                slot.replica,
                replacement.previous_task_id,
                replacement.replacement_task_id,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                self.abort_replacement_task_best_effort(
                    &spec.service_name,
                    replacement.replacement_task_id,
                    abort_context,
                )
                .await;
                return Err(err);
            }
        };

        match outcome {
            ServiceSlotCutover::Applied => {
                self.clear_slot_missing(key).await;
                self.withdraw_and_stop_superseded_task_best_effort(
                    &spec.service_name,
                    replacement.previous_task_id,
                    health_snapshot,
                )
                .await;
                Ok(true)
            }
            ServiceSlotCutover::Stale {
                current_task_id,
                reason,
            } => {
                tracing::debug!(
                    target: "services",
                    service = %spec.service_name,
                    template = %slot.template.name,
                    replica = slot.replica,
                    previous_task = %replacement.previous_task_id,
                    replacement_task = %replacement.replacement_task_id,
                    current_task = ?current_task_id,
                    stale_reason = reason.as_str(),
                    "replacement lost service slot cutover race"
                );
                self.abort_replacement_task_best_effort(
                    &spec.service_name,
                    replacement.replacement_task_id,
                    "stale replacement lost service slot cutover race",
                )
                .await;
                self.clear_slot_missing(key).await;
                Ok(false)
            }
        }
    }

    /// Stops a failed replacement task that never became the desired slot owner.
    async fn abort_replacement_task_best_effort(
        &self,
        service_name: &str,
        replacement_task_id: Uuid,
        context: &str,
    ) {
        if let Err(err) = self
            .workload_manager
            .request_workload_stop(replacement_task_id)
            .await
        {
            if service_cleanup_stop_error_is_transient(&err) {
                tracing::debug!(
                    target: "services",
                    service = %service_name,
                    replacement_task = %replacement_task_id,
                    "{context}; replacement stop deferred after transient cleanup failure: {err:#}"
                );
            } else {
                tracing::warn!(
                    target: "services",
                    service = %service_name,
                    replacement_task = %replacement_task_id,
                    "{context}; failed to stop superseded replacement: {err:#}"
                );
            }
            let health_snapshot = self.health_monitor.snapshot();
            self.retire_unavailable_service_task_best_effort(
                service_name,
                replacement_task_id,
                &health_snapshot,
            )
            .await;
        }
    }

    /// Withdraws service traffic from the old task and requests stop after cutover succeeds.
    async fn withdraw_and_stop_superseded_task_best_effort(
        &self,
        service_name: &str,
        superseded_task_id: Uuid,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) {
        if let Err(err) = self
            .workload_manager
            .set_task_traffic_published(superseded_task_id, false)
            .await
        {
            tracing::warn!(
                target: "services",
                service = %service_name,
                task = %superseded_task_id,
                "failed to withdraw superseded task traffic after cutover: {err:#}"
            );
        }
        if let Err(err) = self
            .workload_manager
            .request_workload_stop(superseded_task_id)
            .await
        {
            if service_cleanup_stop_error_is_transient(&err) {
                tracing::debug!(
                    target: "services",
                    service = %service_name,
                    task = %superseded_task_id,
                    "deferred superseded task cleanup after transient stop failure: {err:#}"
                );
            } else {
                tracing::warn!(
                    target: "services",
                    service = %service_name,
                    task = %superseded_task_id,
                    "failed to stop superseded task after cutover: {err:#}"
                );
            }
            self.retire_unavailable_service_task_best_effort(
                service_name,
                superseded_task_id,
                health_snapshot,
            )
            .await;
        }
    }

    /// Retires one superseded service task directly when its original owner is unavailable.
    ///
    /// Fresh task identities keep service handoff correct, but the old workload row can still stay
    /// visible as `Running` if the original owner is down or has left the cluster. Narrow that case
    /// to service-owned workloads whose owner cannot accept remote cleanup so cluster-visible task
    /// listings converge on the replacement tasks.
    async fn retire_unavailable_service_task_best_effort(
        &self,
        service_name: &str,
        superseded_task_id: Uuid,
        health_snapshot: &HashMap<Uuid, HealthStatus>,
    ) {
        let spec = match self
            .workload_manager
            .inspect_workload(superseded_task_id)
            .await
        {
            Ok(spec) => spec,
            Err(err) => {
                if service_cleanup_stop_error_is_transient(&err) {
                    tracing::debug!(
                        target: "services",
                        service = %service_name,
                        task = %superseded_task_id,
                        "superseded task already absent during cleanup inspection: {err:#}"
                    );
                } else {
                    tracing::warn!(
                        target: "services",
                        service = %service_name,
                        task = %superseded_task_id,
                        "failed to inspect superseded task after stop failure: {err:#}"
                    );
                }
                return;
            }
        };
        if spec
            .owner
            .as_ref()
            .and_then(|owner| owner.as_service_replica())
            .is_none()
        {
            return;
        }
        let owner_active_cluster_member = self
            .cluster_registry
            .peer_active_in_local_view(spec.node_id);
        if !service_task_owner_unavailable_for_cleanup(
            spec.node_id,
            self.local_node_id,
            health_snapshot,
            owner_active_cluster_member,
        ) {
            return;
        }

        if let Err(err) = self
            .workload_manager
            .retire_unavailable_service_workload(
                superseded_task_id,
                format!(
                    "service controller retired superseded task after owner node {} became unavailable",
                    spec.node_id
                ),
            )
            .await
        {
            tracing::warn!(
                target: "services",
                service = %service_name,
                task = %superseded_task_id,
                node = %spec.node_id,
                "failed to retire superseded task on down node: {err:#}"
            );
        }
    }

    /// Claims a local in-flight marker so a slot is not reconciled concurrently.
    async fn try_begin_slot(&self, key: &SlotKey) -> Option<SlotGuard> {
        let mut guard = self.inflight_slots.lock().await;
        if guard.contains(key) {
            return None;
        }
        guard.insert(key.clone());
        Some(SlotGuard {
            key: key.clone(),
            inflight: self.inflight_slots.clone(),
        })
    }

    /// Records that a slot appears missing and returns true once the normal grace period elapses.
    async fn slot_missing_elapsed(&self, key: &SlotKey) -> bool {
        self.slot_missing_elapsed_after(key, Duration::from_secs(SERVICE_SLOT_MISSING_GRACE_SECS))
            .await
    }

    /// Records a missing-slot deadline and returns true once its requested grace elapses.
    ///
    /// Slot reconciliation has two different absence windows. Steady-state
    /// repair uses the short grace period, while an absent row during deployment
    /// waits longer because direct assignment and workload MST sync can lag the
    /// service spec assignment on large rollouts.
    async fn slot_missing_elapsed_after(&self, key: &SlotKey, grace: Duration) -> bool {
        let now = Instant::now();
        let mut guard = self.slot_missing_after.lock().await;
        match guard.get(key) {
            Some(deadline) => now >= *deadline,
            None => {
                guard.insert(key.clone(), now + grace);
                false
            }
        }
    }

    /// Clears any missing marker for a slot once its task is confirmed healthy.
    async fn clear_slot_missing(&self, key: &SlotKey) {
        let mut guard = self.slot_missing_after.lock().await;
        guard.remove(key);
    }

    /// Returns true when the slot is eligible for another rebalance attempt.
    async fn rebalance_allowed(&self, key: &SlotKey) -> bool {
        let now = Instant::now();
        let guard = self.slot_rebalance_after.lock().await;
        guard
            .get(key)
            .map(|deadline| now >= *deadline)
            .unwrap_or(true)
    }

    /// Sets a cooldown window to prevent repeated rebalance attempts for the same slot.
    async fn set_rebalance_cooldown(&self, key: &SlotKey) {
        let mut guard = self.slot_rebalance_after.lock().await;
        guard.insert(key.clone(), Instant::now() + self.timing.rebalance_cooldown);
    }

    /// Returns true after one exact healthy placement plan remains unchanged for its quiet window.
    async fn rebalance_plan_is_stable(
        &self,
        key: &SlotKey,
        task: &WorkloadSpec,
        desired_node_id: Uuid,
    ) -> bool {
        let now = Instant::now();
        let mut plans = self.slot_rebalance_plans.lock().await;
        let plan = SlotRebalancePlan {
            task_id: task.id,
            current_node_id: task.node_id,
            desired_node_id,
            view_revision: self.reconcile_trigger.current_view_revision(),
            observed_at: now,
        };
        match plans.get_mut(key) {
            Some(current) if current.describes_same_move(&plan) => {
                now.saturating_duration_since(current.observed_at)
                    >= self.timing.rebalance_plan_stability
            }
            Some(current) => {
                *current = plan;
                false
            }
            None => {
                plans.insert(key.clone(), plan);
                false
            }
        }
    }

    /// Drops a stale healthy-placement proposal when the slot no longer needs that exact move.
    async fn clear_rebalance_plan(&self, key: &SlotKey) {
        self.slot_rebalance_plans.lock().await.remove(key);
    }
}

/// Local guard that clears the in-flight marker for a slot on drop.
struct SlotGuard {
    key: SlotKey,
    inflight: Arc<AsyncMutex<HashSet<SlotKey>>>,
}

impl Drop for SlotGuard {
    /// Clears the in-flight marker when the guard is dropped.
    fn drop(&mut self) {
        let inflight = self.inflight.clone();
        let key = self.key.clone();
        tokio::task::spawn_local(async move {
            inflight.lock().await.remove(&key);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::model::WorkloadServiceMetadata;
    use anyhow::anyhow;

    /// Preferred placement keeps the deterministic slot target while the node is usable.
    #[test]
    fn preferred_slot_node_uses_schedulable_alive_target() {
        let node = Uuid::from_bytes([1u8; 16]);
        let health = HashMap::new();

        assert_eq!(preferred_slot_node(node, &health, true), Some(node));
    }

    /// Newly drained targets are ignored even if they came from an older placement snapshot.
    #[test]
    fn preferred_slot_node_rejects_unschedulable_target() {
        let node = Uuid::from_bytes([2u8; 16]);
        let health = HashMap::new();

        assert_eq!(preferred_slot_node(node, &health, false), None);
    }

    /// Down targets remain ineligible regardless of their last scheduling bit.
    #[test]
    fn preferred_slot_node_rejects_down_target() {
        let node = Uuid::from_bytes([3u8; 16]);
        let health = HashMap::from([(node, HealthStatus::Down)]);

        assert_eq!(preferred_slot_node(node, &health, true), None);
    }

    /// Absent deployment rows keep the long visibility grace even after split pruning.
    #[test]
    fn deployment_visibility_grace_precedes_split_pruning() {
        let node = Uuid::from_bytes([4u8; 16]);
        let health = HashMap::from([(node, HealthStatus::Alive)]);

        assert_eq!(
            slot_repair_delay(
                ServiceStatus::Deploying,
                None,
                node,
                &health,
                false,
                false,
                false,
            ),
            SlotRepairDelay::DeploymentVisibilityGrace
        );
        assert_eq!(
            slot_repair_delay(
                ServiceStatus::Deploying,
                None,
                node,
                &health,
                true,
                false,
                false,
            ),
            SlotRepairDelay::DeploymentVisibilityGrace
        );
    }

    /// A down deployment target proves that an absent assignment cannot still make progress.
    #[test]
    fn deployment_down_target_bypasses_visibility_grace() {
        let node = Uuid::from_bytes([5u8; 16]);
        let health = HashMap::from([(node, HealthStatus::Down)]);

        assert_eq!(
            slot_repair_delay(
                ServiceStatus::Deploying,
                None,
                node,
                &health,
                false,
                false,
                false,
            ),
            SlotRepairDelay::Immediate
        );
    }

    /// Steady-state absence keeps normal grace unless split evidence confirms local pruning.
    #[test]
    fn steady_state_split_pruning_bypasses_missing_grace() {
        let node = Uuid::from_bytes([6u8; 16]);
        let health = HashMap::new();

        assert_eq!(
            slot_repair_delay(
                ServiceStatus::Running,
                None,
                node,
                &health,
                false,
                false,
                false,
            ),
            SlotRepairDelay::MissingGrace
        );
        assert_eq!(
            slot_repair_delay(
                ServiceStatus::Running,
                None,
                node,
                &health,
                true,
                false,
                false,
            ),
            SlotRepairDelay::Immediate
        );
    }

    /// Current-generation handoffs remain owned by slot reconciliation instead of generic cleanup.
    #[test]
    fn current_handoff_requires_slot_reconciliation() {
        let metadata = WorkloadServiceMetadata::new("demo", "api", 2)
            .with_service_epoch(7)
            .with_handoff(Uuid::new_v4());

        assert!(service_metadata_handoff_requires_slot_reconciliation(
            7, &metadata
        ));
    }

    /// A workload row may arrive before its newer service row and must not be cleaned early.
    #[test]
    fn future_handoff_waits_for_service_state() {
        let metadata = WorkloadServiceMetadata::new("demo", "api", 2)
            .with_service_epoch(8)
            .with_handoff(Uuid::new_v4());

        assert!(service_metadata_handoff_requires_slot_reconciliation(
            7, &metadata
        ));
    }

    /// Superseded generations return to ordinary excess-task cleanup.
    #[test]
    fn old_handoff_does_not_block_generic_cleanup() {
        let metadata = WorkloadServiceMetadata::new("demo", "api", 2)
            .with_service_epoch(6)
            .with_handoff(Uuid::new_v4());

        assert!(!service_metadata_handoff_requires_slot_reconciliation(
            7, &metadata
        ));
    }

    /// Candidate selection prefers the converged placement target before UUID ordering.
    #[test]
    fn handoff_candidate_selection_prefers_target_node() {
        let preferred_node = Uuid::from_bytes([9u8; 16]);
        let other_node = Uuid::from_bytes([8u8; 16]);
        let smaller_task = Uuid::from_bytes([1u8; 16]);
        let preferred_task = Uuid::from_bytes([2u8; 16]);
        let candidates = [
            SlotHandoffCandidate {
                task_id: smaller_task,
                node_id: other_node,
            },
            SlotHandoffCandidate {
                task_id: preferred_task,
                node_id: preferred_node,
            },
        ];

        assert_eq!(
            select_slot_handoff_candidate(&candidates, Some(preferred_node)),
            Some(candidates[1])
        );
    }

    /// Equivalent candidates use task UUID ordering so every observer makes the same choice.
    #[test]
    fn handoff_candidate_selection_is_deterministic() {
        let node_id = Uuid::from_bytes([9u8; 16]);
        let smaller = SlotHandoffCandidate {
            task_id: Uuid::from_bytes([1u8; 16]),
            node_id,
        };
        let larger = SlotHandoffCandidate {
            task_id: Uuid::from_bytes([2u8; 16]),
            node_id,
        };

        assert_eq!(
            select_slot_handoff_candidate(&[larger, smaller], Some(node_id)),
            Some(smaller)
        );
    }

    /// A different local placement result still resumes a valid handoff on its healthy target.
    #[test]
    fn healthy_handoff_target_disagreement_is_non_destructive() {
        let candidate = SlotHandoffCandidate {
            task_id: Uuid::from_bytes([1u8; 16]),
            node_id: Uuid::from_bytes([2u8; 16]),
        };
        let observer_target = Uuid::from_bytes([3u8; 16]);

        assert_eq!(
            select_slot_handoff_candidate(&[candidate], Some(observer_target)),
            Some(candidate)
        );
    }

    /// Missing remote sessions are retryable cleanup noise during view transitions.
    #[test]
    fn service_cleanup_stop_error_treats_missing_session_as_transient() {
        let err = anyhow!("no active session for peer 00000000-0000-0000-0000-000000000001");

        assert!(service_cleanup_stop_error_is_transient(&err));
    }

    /// Revoked peer sessions are expected after clean leave revokes stale capabilities.
    #[test]
    fn service_cleanup_stop_error_treats_revoked_peer_session_as_transient() {
        let err = anyhow!(
            "failed to open workload service with peer 00000000-0000-0000-0000-000000000001: \
             Failed: remote exception: peer session revoked"
        );

        assert!(service_cleanup_stop_error_is_transient(&err));
    }

    /// Unrelated cleanup errors remain warnable so real storage failures still surface.
    #[test]
    fn service_cleanup_stop_error_keeps_unrelated_errors_warnable() {
        let err = anyhow!("permission denied while updating workload store");

        assert!(!service_cleanup_stop_error_is_transient(&err));
    }

    /// Generic remote stop failures remain warnable until their cause is known to be benign.
    #[test]
    fn service_cleanup_stop_error_keeps_generic_remote_stop_warnable() {
        let err = anyhow!("stop request failed on peer 00000000-0000-0000-0000-000000000001");

        assert!(!service_cleanup_stop_error_is_transient(&err));
    }

    /// Explicit left membership makes remote service cleanup impossible even without SWIM Down.
    #[test]
    fn service_task_owner_unavailable_for_cleanup_accepts_left_peer() {
        let local = Uuid::from_bytes([1u8; 16]);
        let peer = Uuid::from_bytes([2u8; 16]);
        let health = HashMap::new();

        assert!(service_task_owner_unavailable_for_cleanup(
            peer,
            local,
            &health,
            Some(false)
        ));
    }

    /// Missing peer metadata remains retryable because workload gossip can outrun peer sync.
    #[test]
    fn service_task_owner_unavailable_for_cleanup_keeps_unknown_peer_retryable() {
        let local = Uuid::from_bytes([1u8; 16]);
        let peer = Uuid::from_bytes([2u8; 16]);
        let health = HashMap::new();

        assert!(!service_task_owner_unavailable_for_cleanup(
            peer, local, &health, None
        ));
    }

    /// Active cluster membership keeps cleanup on the normal remote stop path.
    #[test]
    fn service_task_owner_unavailable_for_cleanup_keeps_active_peer_retryable() {
        let local = Uuid::from_bytes([1u8; 16]);
        let peer = Uuid::from_bytes([2u8; 16]);
        let health = HashMap::new();

        assert!(!service_task_owner_unavailable_for_cleanup(
            peer,
            local,
            &health,
            Some(true)
        ));
    }

    /// SWIM Down still retires stale service tasks when membership has not observed leave.
    #[test]
    fn service_task_owner_unavailable_for_cleanup_accepts_down_peer() {
        let local = Uuid::from_bytes([1u8; 16]);
        let peer = Uuid::from_bytes([2u8; 16]);
        let health = HashMap::from([(peer, HealthStatus::Down)]);

        assert!(service_task_owner_unavailable_for_cleanup(
            peer,
            local,
            &health,
            Some(true)
        ));
    }
}
