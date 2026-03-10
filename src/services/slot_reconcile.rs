use super::ownership::{
    ReplicaSlot, SlotKey, build_replica_slots, compute_slot_targets, select_slot_owner,
    select_task_owner,
};
use super::{
    SERVICE_ENABLE_PROACTIVE_REBALANCE, SERVICE_REBALANCE_COOLDOWN_SECS,
    SERVICE_SLOT_MISSING_GRACE_SECS, ServiceController, ServiceTaskSnapshot, TaskInventory,
    deploying_assignment_incomplete, expected_task_id_count, make_replica_request, node_is_down,
    should_restart_missing_slot_immediately, task_age_allows_cleanup, task_age_allows_rebalance,
    task_state_healthy, task_state_rebalanceable,
};
use crate::services::types::{ServiceSpecValue, ServiceStatus};
use crate::task::types::TaskSpec;
use anyhow::anyhow;
use health::Status as HealthStatus;
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
        // templates populated but task_ids still empty while launch is in-flight. Running the
        // normal cleanup/slot loops at that point causes false "excess task" stops and churn.
        if deploying_assignment_incomplete(&spec) {
            tracing::debug!(
                target: "services",
                service = %spec.service_name,
                expected_slots = expected_task_id_count(&spec),
                assigned_slots = spec.task_ids.len(),
                "skipping deploy reconciliation until task ids are fully assigned"
            );
            return Ok(());
        }

        let slots = build_replica_slots(&spec);
        let slot_targets = compute_slot_targets(spec.id, &spec.tasks, eligible_nodes);
        let desired_ids: HashSet<Uuid> = slots.iter().filter_map(|slot| slot.task_id).collect();
        let service_tasks = inventory.service_task_snapshot(&spec.service_name, desired_ids);
        let service_degraded = slots.iter().any(|slot| {
            let Some(task_id) = slot.task_id else {
                return true;
            };
            let Some(task) = inventory.by_id.get(&task_id) else {
                return true;
            };
            node_is_down(task.node_id, health_snapshot) || !task_state_healthy(&task.state)
        });

        self.reconcile_extra_tasks(&spec, &service_tasks, eligible_nodes)
            .await;

        for slot in slots {
            let Some(task_id) = slot.task_id else {
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
        service_tasks: &ServiceTaskSnapshot<'_>,
        eligible_nodes: &[Uuid],
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
                continue;
            }

            if let Err(err) = self.task_manager.request_task_stop(task.id).await {
                tracing::warn!(
                    target: "services",
                    "failed to stop excess task {} for '{}': {err}",
                    task.id,
                    spec.service_name
                );
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
        let preferred_node = if node_is_down(desired_node, env.health_snapshot) {
            None
        } else {
            Some(desired_node)
        };

        let task = env.inventory.by_id.get(&task_id);
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
            let restart_immediately = task_on_draining_node
                || should_restart_missing_slot_immediately(spec.status(), task);
            if restart_immediately || self.slot_missing_elapsed(key).await {
                self.start_slot_task(spec, slot, task_id, preferred_node, key)
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
        if !task_age_allows_rebalance(task) {
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

        self.move_slot_task(spec, slot, task, desired_node, key)
            .await?;

        Ok(())
    }

    /// Starts or restarts a replica slot on the preferred node, falling back if placement fails.
    async fn start_slot_task(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task_id: Uuid,
        preferred_node: Option<Uuid>,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        if let Some(preferred_node) = preferred_node {
            let request = make_replica_request(
                &spec.service_name,
                &slot.template,
                slot.replica,
                task_id,
                Some(preferred_node),
            );

            match self.task_manager.start_tasks_batch(vec![request]).await {
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
                    self.clear_slot_missing(key).await;
                    if spec.status() == ServiceStatus::Running {
                        self.publish_task_traffic_for_cutover(
                            &spec.service_name,
                            task_id,
                            Duration::from_secs(30),
                        )
                        .await?;
                    }
                    return Ok(());
                }
                Err(err) => {
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

        let fallback = make_replica_request(
            &spec.service_name,
            &slot.template,
            slot.replica,
            task_id,
            None,
        );

        self.task_manager
            .start_tasks_batch(vec![fallback])
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

        self.clear_slot_missing(key).await;
        if spec.status() == ServiceStatus::Running {
            self.publish_task_traffic_for_cutover(
                &spec.service_name,
                task_id,
                Duration::from_secs(30),
            )
            .await?;
        }
        Ok(())
    }

    /// Moves a replica to the preferred node using a start-first handoff to avoid downtime.
    ///
    /// The previous stop-then-start workflow could temporarily drop a healthy attachment entry
    /// when the replacement launch retried due network/readiness lag (for example when a node
    /// rejoins after being down and service scale changed while it was offline). Starting the
    /// preferred placement first keeps the slot represented in attachment state throughout the
    /// transition; stale local containers are then drained by task inventory reconciliation.
    async fn move_slot_task(
        &self,
        spec: &ServiceSpecValue,
        slot: &ReplicaSlot,
        task: &TaskSpec,
        preferred_node: Uuid,
        key: &SlotKey,
    ) -> anyhow::Result<()> {
        let request = make_replica_request(
            &spec.service_name,
            &slot.template,
            slot.replica,
            task.id,
            Some(preferred_node),
        );

        self.task_manager
            .start_tasks_batch(vec![request])
            .await
            .map_err(|err| {
                anyhow!(
                    "rebalance placement failed for '{}' replica {} on {}: {err}",
                    slot.template.name,
                    slot.replica,
                    preferred_node
                )
            })?;

        self.publish_task_traffic_for_cutover(&spec.service_name, task.id, Duration::from_secs(30))
            .await?;
        self.set_rebalance_cooldown(key).await;

        tracing::debug!(
            target: "services",
            service = %spec.service_name,
            template = %slot.template.name,
            replica = slot.replica,
            task = %task.id,
            previous_node = %task.node_id,
            preferred_node = %preferred_node,
            "rebalance replacement accepted; previous owner will drain stale local runtime"
        );

        Ok(())
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

    /// Records that a slot appears missing and returns true once the grace period elapses.
    async fn slot_missing_elapsed(&self, key: &SlotKey) -> bool {
        let now = Instant::now();
        let mut guard = self.slot_missing_since.lock().await;
        match guard.get(key) {
            Some(started) => {
                now.duration_since(*started) >= Duration::from_secs(SERVICE_SLOT_MISSING_GRACE_SECS)
            }
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
        guard.insert(
            key.clone(),
            Instant::now() + Duration::from_secs(SERVICE_REBALANCE_COOLDOWN_SECS),
        );
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
