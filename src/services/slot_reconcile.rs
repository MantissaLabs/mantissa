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
    SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS, SERVICE_ENABLE_PROACTIVE_REBALANCE,
    SERVICE_SLOT_MISSING_GRACE_SECS, ServiceController,
};
use crate::services::ownership::{
    ReplicaSlot, SlotKey, build_replica_slots, select_slot_owner, select_task_owner,
};
use crate::services::types::{ServiceSpecValue, ServiceStatus};
use crate::workload::model::{WorkloadPhase, WorkloadSpec};
use anyhow::anyhow;
use mantissa_health::Status as HealthStatus;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

struct SlotReconcileEnv<'a> {
    inventory: &'a TaskInventory,
    health_snapshot: &'a HashMap<Uuid, HealthStatus>,
    slot_targets: &'a HashMap<SlotKey, Uuid>,
    service_degraded: bool,
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
            node_is_down(task.node_id, health_snapshot) || !task_state_healthy(&task.state)
        });

        if spec.status() == ServiceStatus::VolumeUnavailable && !service_degraded {
            self.restore_volume_available_service(&spec).await?;
        }

        self.reconcile_extra_tasks(&spec, &service_tasks, eligible_nodes, health_snapshot)
            .await;

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

            let Some(owner) =
                select_slot_owner(spec.id, &slot.template.name, slot.replica, eligible_nodes)
            else {
                continue;
            };

            if owner != self.local_node_id {
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
                    },
                    &key,
                )
                .await
            {
                tracing::warn!(
                    target: "services",
                    "slot reconciliation failed for '{}' replica {}: {err}",
                    slot.template.name,
                    slot.replica
                );
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
        for task in service_tasks.observed_tasks() {
            if service_tasks.is_desired(task.id) {
                continue;
            }
            if !task_state_healthy(&task.state) {
                continue;
            }
            if !task_age_allows_cleanup(task) {
                continue;
            }
            let Some(owner) = select_task_owner(task.id, eligible_nodes) else {
                continue;
            };
            if owner != self.local_node_id {
                // This node can see an extra service-owned row, but the deterministic cleanup
                // owner may not have learned that row yet because routine workload gossip is
                // suppressed for large deployments. Prioritize workload MST sync with that owner
                // so the next cleanup pass can observe and stop the same extra task without a
                // global gossip fallback.
                self.workload_manager
                    .prioritize_workload_sync_with_peer(owner);
                continue;
            }

            if let Err(err) = self.workload_manager.request_workload_stop(task.id).await {
                tracing::warn!(
                    target: "services",
                    "failed to stop excess task {} for '{}': {err}",
                    task.id,
                    spec.service_name
                );
                self.retire_down_superseded_service_task_best_effort(
                    &spec.service_name,
                    task.id,
                    health_snapshot,
                )
                .await;
            }
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
        if matches!(
            task.map(|task| &task.state),
            Some(WorkloadPhase::VolumeUnavailable)
        ) && requires_pinned_target
        {
            // Imported and other pinned local volumes recover in place once the node-local
            // path returns. Promote the service immediately instead of waiting for a restart
            // attempt to rediscover the same local volume error.
            self.mark_service_volume_unavailable(spec).await?;
            return Ok(());
        }
        let task_on_draining_node = task
            .map(|task| self.node_drain_requested(task.node_id))
            .unwrap_or(false);
        let missing = match task {
            None => true,
            Some(task) => {
                task_on_draining_node
                    || node_is_down(task.node_id, env.health_snapshot)
                    || !task_state_healthy(&task.state)
            }
        };

        if missing {
            if task_on_draining_node {
                tracing::debug!(
                    target: "services",
                    service = %spec.service_name,
                    template = %slot.template.name,
                    replica = slot.replica,
                    task = %task_id,
                    "slot task is assigned to a draining node; forcing evacuation"
                );
            }
            let deployment_visibility_elapsed = if deploying_missing_slot_is_unknown(
                spec.status(),
                task,
                desired_node,
                env.health_snapshot,
            ) {
                if !self
                    .slot_missing_elapsed_after(
                        key,
                        Duration::from_secs(SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS),
                    )
                    .await
                {
                    tracing::debug!(
                        target: "services",
                        service = %spec.service_name,
                        template = %slot.template.name,
                        replica = slot.replica,
                        task = %task_id,
                        target = %desired_node,
                        visibility_grace_secs = SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS,
                        "slot task row is not locally visible during deployment; waiting for direct assignment delivery or workload MST sync"
                    );
                    return Ok(());
                }

                tracing::debug!(
                    target: "services",
                    service = %spec.service_name,
                    template = %slot.template.name,
                    replica = slot.replica,
                    task = %task_id,
                    target = %desired_node,
                    visibility_grace_secs = SERVICE_DEPLOYING_SLOT_VISIBILITY_GRACE_SECS,
                    "deployment visibility grace elapsed for absent slot task; allowing replacement"
                );
                true
            } else {
                false
            };
            let deploying_absent_target_down = spec.status() == ServiceStatus::Deploying
                && task.is_none()
                && node_is_down(desired_node, env.health_snapshot);
            if deploying_absent_target_down {
                tracing::debug!(
                    target: "services",
                    service = %spec.service_name,
                    template = %slot.template.name,
                    replica = slot.replica,
                    task = %task_id,
                    target = %desired_node,
                    "deployment slot task is absent and its assigned target is down; allowing replacement without visibility grace"
                );
            }
            let restart_immediately = task_on_draining_node
                || deploying_absent_target_down
                || deployment_visibility_elapsed
                || should_restart_missing_slot_immediately(spec.status(), task);
            if restart_immediately || self.slot_missing_elapsed(key).await {
                self.start_slot_task(
                    spec,
                    slot,
                    task_id,
                    preferred_node,
                    env.health_snapshot,
                    key,
                )
                .await?;
            }
            return Ok(());
        }

        self.clear_slot_missing(key).await;

        let Some(task) = task else {
            return Ok(());
        };

        if spec.status() == ServiceStatus::Running
            && task_state_healthy(&task.state)
            && !node_is_down(task.node_id, env.health_snapshot)
        {
            self.publish_running_task_traffic_best_effort(&spec.service_name, task.id)
                .await;
        }

        // Deployment reconciliation should heal missing/failed slots, but avoid proactive
        // rebalancing until the service is fully running to prevent startup churn.
        if spec.status() != ServiceStatus::Running {
            return Ok(());
        }

        if !SERVICE_ENABLE_PROACTIVE_REBALANCE {
            return Ok(());
        }

        if slot.template.replicas <= 1 {
            return Ok(());
        }

        if env.service_degraded {
            return Ok(());
        }

        if !task_state_rebalanceable(&task.state) {
            return Ok(());
        }
        if !task_age_allows_rebalance(task, self.timing.rebalance_min_age) {
            return Ok(());
        }
        if !self.rebalance_allowed(key).await {
            return Ok(());
        }

        if node_is_down(desired_node, env.health_snapshot) {
            return Ok(());
        }

        if desired_node == task.node_id {
            return Ok(());
        }

        self.move_slot_task(spec, slot, task, desired_node, env.health_snapshot, key)
            .await?;

        Ok(())
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
            let request = slot.template.replica_start_request(
                &spec.service_name,
                spec.service_epoch,
                slot.replica,
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
                                Duration::from_secs(30),
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
                    if let Err(err) = self
                        .swap_service_slot_task_id_for_cutover(
                            spec.id,
                            spec.manifest_id,
                            &slot.template.name,
                            slot.replica,
                            task_id,
                            replacement_task_id,
                        )
                        .await
                    {
                        self.abort_replacement_task_best_effort(
                            &spec.service_name,
                            replacement_task_id,
                            "preferred replacement could not claim service slot",
                        )
                        .await;
                        return Err(err);
                    }
                    self.clear_slot_missing(key).await;
                    self.withdraw_and_stop_superseded_task_best_effort(
                        &spec.service_name,
                        task_id,
                        health_snapshot,
                    )
                    .await;
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

        let fallback = slot.template.replica_start_request(
            &spec.service_name,
            spec.service_epoch,
            slot.replica,
            replacement_task_id,
            None,
        );

        self.workload_manager
            .start_workloads_batch(vec![fallback])
            .await
            .map(|specs| {
                if specs.len() != 1 {
                    tracing::warn!(
                        target: "services",
                        "fallback placement mismatch for '{}' replica {}: expected 1, got {}",
                        slot.template.name,
                        slot.replica,
                        specs.len()
                    );
                }
            })
            .map_err(|err| anyhow!("fallback placement failed: {err}"))?;

        if spec.status() == ServiceStatus::Running
            && let Err(err) = self
                .publish_task_traffic_for_cutover(
                    &spec.service_name,
                    replacement_task_id,
                    Duration::from_secs(30),
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
        if let Err(err) = self
            .swap_service_slot_task_id_for_cutover(
                spec.id,
                spec.manifest_id,
                &slot.template.name,
                slot.replica,
                task_id,
                replacement_task_id,
            )
            .await
        {
            self.abort_replacement_task_best_effort(
                &spec.service_name,
                replacement_task_id,
                "fallback replacement could not claim service slot",
            )
            .await;
            return Err(err);
        }
        self.clear_slot_missing(key).await;
        self.withdraw_and_stop_superseded_task_best_effort(
            &spec.service_name,
            task_id,
            health_snapshot,
        )
        .await;
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
        let request = slot.template.replica_start_request(
            &spec.service_name,
            spec.service_epoch,
            slot.replica,
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
                Duration::from_secs(30),
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
        if let Err(err) = self
            .swap_service_slot_task_id_for_cutover(
                spec.id,
                spec.manifest_id,
                &slot.template.name,
                slot.replica,
                task.id,
                replacement_task_id,
            )
            .await
        {
            self.abort_replacement_task_best_effort(
                &spec.service_name,
                replacement_task_id,
                "rebalance replacement could not claim service slot",
            )
            .await;
            return Err(err);
        }
        self.withdraw_and_stop_superseded_task_best_effort(
            &spec.service_name,
            task.id,
            health_snapshot,
        )
        .await;
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
            tracing::warn!(
                target: "services",
                service = %service_name,
                replacement_task = %replacement_task_id,
                "{context}; failed to stop superseded replacement: {err:#}"
            );
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
            tracing::warn!(
                target: "services",
                service = %service_name,
                task = %superseded_task_id,
                "failed to stop superseded task after cutover: {err:#}"
            );
            self.retire_down_superseded_service_task_best_effort(
                service_name,
                superseded_task_id,
                health_snapshot,
            )
            .await;
        }
    }

    /// Retires one superseded service task directly when its original owner node is already down.
    ///
    /// Fresh task identities keep service handoff correct, but the old workload row can still stay
    /// visible as `Running` if the original owner is gone and the normal remote stop RPC cannot be
    /// delivered. Narrow that case to service-owned workloads on nodes already marked `Down` so
    /// cluster-visible task listings converge on the replacement tasks.
    async fn retire_down_superseded_service_task_best_effort(
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
                tracing::warn!(
                    target: "services",
                    service = %service_name,
                    task = %superseded_task_id,
                    "failed to inspect superseded task after stop failure: {err:#}"
                );
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
        if !node_is_down(spec.node_id, health_snapshot) {
            return;
        }

        if let Err(err) = self
            .workload_manager
            .retire_unavailable_service_workload(
                superseded_task_id,
                format!(
                    "service controller retired superseded task after owner node {} went down",
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

    /// Records that a slot appears missing and returns true once the requested grace elapses.
    ///
    /// Slot reconciliation has two different absence windows. Steady-state
    /// repair uses the short grace period, while an absent row during deployment
    /// waits longer because direct assignment and workload MST sync can lag the
    /// service spec assignment on large rollouts.
    async fn slot_missing_elapsed_after(&self, key: &SlotKey, grace: Duration) -> bool {
        let now = Instant::now();
        let mut guard = self.slot_missing_since.lock().await;
        match guard.get(key) {
            Some(started) => now.duration_since(*started) >= grace,
            None => {
                guard.insert(key.clone(), now);
                false
            }
        }
    }

    /// Clears any missing marker for a slot once its task is confirmed healthy.
    async fn clear_slot_missing(&self, key: &SlotKey) {
        let mut guard = self.slot_missing_since.lock().await;
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
}
