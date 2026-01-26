use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Context;
use chrono::Utc;
use protocol::server::cluster_session;
use tracing::warn;
use uuid::Uuid;

use crate::scheduler::{SchedulerError, SlotId, SlotReservationRequest};
use crate::task::container::ContainerState;
use crate::task::service::read_spec;
use crate::task::types::{TaskEvent, TaskSpec};

use super::TaskManager;
use super::planner::{BatchStartPlan, RemoteStartPlan};

/// Error returned by slot reservation stages, signalling whether the caller should retry.
pub(super) enum ExecutionError {
    Retry(anyhow::Error),
    Fatal(anyhow::Error),
}

/// Tracks slot reservations that happened on a peer so they can be released on rollback.
pub(super) struct RemoteReservation {
    pub(super) slots: Vec<SlotId>,
    pub(super) version: u64,
}

fn is_scheduler_retryable_message(message: &str) -> bool {
    message.contains("snapshot mismatch")
        || message.contains("slots unavailable")
        || message.contains("unknown slots")
}

impl TaskManager {
    /// Releases a single slot via the scheduler, retrying on snapshot mismatches.
    pub(super) async fn release_slot(&self, slot_id: SlotId) -> Result<(), anyhow::Error> {
        const MAX_ATTEMPTS: usize = 10;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.scheduler.snapshot().await {
                Some(s) => s,
                None => return Err(anyhow::anyhow!("scheduler snapshot unavailable")),
            };

            match self.scheduler.free_slots(snapshot.version, [slot_id]).await {
                Ok(_) => return Ok(()),
                Err(SchedulerError::SnapshotMismatch { .. }) => continue,
                Err(SchedulerError::UnknownSlots { .. })
                | Err(SchedulerError::SlotsNotReserved { .. }) => return Ok(()),
                Err(err) => return Err(anyhow::anyhow!(err)),
            }
        }

        Err(anyhow::anyhow!(
            "failed to free scheduler slot after retries"
        ))
    }

    /// Reserves the local slots required by the batch, returning the list of new reservations.
    pub(super) async fn reserve_local_slots(
        &self,
        plans: &[BatchStartPlan],
        expected_version: u64,
    ) -> Result<Vec<SlotId>, ExecutionError> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let mut requests = Vec::new();
        let mut newly_reserved = Vec::new();

        for plan in plans {
            if plan.preassigned {
                continue;
            }

            for slot in &plan.slots {
                requests.push(SlotReservationRequest {
                    slot_id: slot.slot_id,
                    owner: self.local_node_id,
                    task_id: Some(plan.id),
                });
                newly_reserved.push(slot.slot_id);
            }
        }

        if requests.is_empty() {
            return Ok(Vec::new());
        }

        match self
            .scheduler
            .reserve_slots(expected_version, requests)
            .await
        {
            Ok(_) => Ok(newly_reserved),
            Err(err @ SchedulerError::SnapshotMismatch { .. })
            | Err(err @ SchedulerError::SlotsUnavailable { .. })
            | Err(err @ SchedulerError::UnknownSlots { .. }) => {
                Err(ExecutionError::Retry(anyhow::anyhow!(err)))
            }
            Err(err) => Err(ExecutionError::Fatal(anyhow::anyhow!(err))),
        }
    }

    /// Releases the provided local slots, logging but otherwise ignoring failures.
    pub(super) async fn release_local_slots(&self, slots: &[SlotId]) {
        let mut seen = HashSet::new();
        for slot_id in slots {
            if !seen.insert(*slot_id) {
                continue;
            }

            if let Err(err) = self.release_slot(*slot_id).await {
                warn!(
                    target: "task",
                    "failed to release local slot {slot_id}: {err}"
                );
            }
        }
    }

    /// Reserves slots on remote peers grouped per node, returning the reservations map.
    pub(super) async fn reserve_remote_slots(
        &self,
        plans: &[RemoteStartPlan],
    ) -> Result<HashMap<Uuid, RemoteReservation>, ExecutionError> {
        let mut reservations = HashMap::new();
        if plans.is_empty() {
            return Ok(reservations);
        }

        let mut grouped: HashMap<Uuid, Vec<&RemoteStartPlan>> = HashMap::new();
        for plan in plans {
            grouped.entry(plan.peer_id).or_default().push(plan);
        }

        for (peer_id, peer_plans) in grouped {
            let session = match self.remote_session(peer_id).await {
                Ok(session) => session,
                Err(err) => {
                    self.release_remote_slots(&reservations).await;
                    return Err(ExecutionError::Retry(err));
                }
            };

            let scheduler_client =
                match session.clone().get_scheduler_request().send().promise.await {
                    Ok(resp) => match resp.get() {
                        Ok(result) => match result.get_scheduler() {
                            Ok(client) => client,
                            Err(err) => {
                                self.release_remote_slots(&reservations).await;
                                return Err(ExecutionError::Retry(anyhow::anyhow!(
                                    err.to_string()
                                )));
                            }
                        },
                        Err(err) => {
                            self.release_remote_slots(&reservations).await;
                            return Err(ExecutionError::Retry(anyhow::anyhow!(err.to_string())));
                        }
                    },
                    Err(err) => {
                        self.release_remote_slots(&reservations).await;
                        return Err(ExecutionError::Retry(anyhow::anyhow!(err.to_string())));
                    }
                };

            let mut reserve_req = scheduler_client.reserve_slots_request();
            {
                let mut inner = reserve_req.get().init_request();
                let expected_version = peer_plans
                    .first()
                    .map(|plan| plan.scheduler_version)
                    .unwrap_or(0);
                inner.set_expected_version(expected_version);
                let total_slots: usize = peer_plans.iter().map(|plan| plan.slots.len()).sum();
                if total_slots == 0 {
                    return Err(ExecutionError::Fatal(anyhow::anyhow!(
                        "remote plan missing slot assignments"
                    )));
                }

                let mut intents_builder = inner.reborrow().init_intents(total_slots as u32);
                let mut intent_idx = 0u32;
                for plan in &peer_plans {
                    for slot in &plan.slots {
                        let mut entry = intents_builder.reborrow().get(intent_idx);
                        entry.set_slot_id(slot.slot_id);
                        entry.set_owner(plan.peer_id.as_bytes());
                        entry.set_task_id(plan.id.as_bytes());
                        intent_idx += 1;
                    }
                }
            }

            match reserve_req.send().promise.await {
                Ok(resp) => match resp.get() {
                    Ok(result) => match result.get_response() {
                        Ok(response) => {
                            let slots: Vec<SlotId> = peer_plans
                                .iter()
                                .flat_map(|plan| plan.slots.iter().map(|slot| slot.slot_id))
                                .collect();
                            let version = response.get_new_version();
                            reservations.insert(peer_id, RemoteReservation { slots, version });
                        }
                        Err(err) => {
                            let message = err.to_string();
                            self.release_remote_slots(&reservations).await;
                            if is_scheduler_retryable_message(&message) {
                                return Err(ExecutionError::Retry(anyhow::anyhow!(message)));
                            }
                            return Err(ExecutionError::Fatal(anyhow::anyhow!(message)));
                        }
                    },
                    Err(err) => {
                        let message = err.to_string();
                        self.release_remote_slots(&reservations).await;
                        if is_scheduler_retryable_message(&message) {
                            return Err(ExecutionError::Retry(anyhow::anyhow!(message)));
                        }
                        return Err(ExecutionError::Fatal(anyhow::anyhow!(message)));
                    }
                },
                Err(err) => {
                    let message = err.to_string();
                    self.release_remote_slots(&reservations).await;
                    if is_scheduler_retryable_message(&message) {
                        return Err(ExecutionError::Retry(anyhow::anyhow!(message)));
                    }
                    return Err(ExecutionError::Fatal(anyhow::anyhow!(message)));
                }
            }
        }

        Ok(reservations)
    }

    /// Releases remote reservations accumulated during previous stages.
    pub(super) async fn release_remote_slots(
        &self,
        reservations: &HashMap<Uuid, RemoteReservation>,
    ) {
        for (peer_id, reservation) in reservations {
            let session = match self.remote_session(*peer_id).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to reopen session with peer {peer_id} while releasing slots: {err}"
                    );
                    continue;
                }
            };

            let scheduler_client = match session
                .clone()
                .get_scheduler_request()
                .send()
                .promise
                .await
            {
                Ok(resp) => match resp.get() {
                    Ok(result) => match result.get_scheduler() {
                        Ok(client) => client,
                        Err(err) => {
                            warn!(
                                target: "task",
                                "failed to access scheduler for peer {peer_id} while releasing slots: {err}"
                            );
                            continue;
                        }
                    },
                    Err(err) => {
                        warn!(
                            target: "task",
                            "failed to obtain scheduler response for peer {peer_id}: {err}"
                        );
                        continue;
                    }
                },
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to send scheduler request to peer {peer_id}: {err}"
                    );
                    continue;
                }
            };

            let mut release_req = scheduler_client.release_slots_request();
            {
                let mut inner = release_req.get().init_request();
                inner.set_expected_version(reservation.version);
                let mut ids_builder = inner
                    .reborrow()
                    .init_slot_ids(reservation.slots.len() as u32);
                for (idx, slot_id) in reservation.slots.iter().enumerate() {
                    ids_builder.set(idx as u32, *slot_id);
                }
            }

            if let Err(err) = release_req.send().promise.await {
                warn!(
                    target: "task",
                    "failed to release slots on peer {peer_id}: {err}"
                );
            }
        }
    }

    pub(super) async fn remote_session(
        &self,
        peer_id: Uuid,
    ) -> Result<cluster_session::Client, anyhow::Error> {
        self.registry
            .session_for_peer(peer_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("no active session for peer {peer_id}"))
    }

    /// Requests a remote peer to stop a task so the owner updates state and broadcasts it.
    pub(super) async fn stop_remote_task(
        &self,
        spec: &TaskSpec,
    ) -> Result<TaskSpec, anyhow::Error> {
        if spec.node_id == self.local_node_id {
            return Err(anyhow::anyhow!(
                "remote stop invoked for local task {}",
                spec.id
            ));
        }

        let peer_id = spec.node_id;
        let session = self
            .remote_session(peer_id)
            .await
            .context(format!("no active session for peer {peer_id}"))?;
        let task_client = session
            .get_task_request()
            .send()
            .promise
            .await
            .context(format!("failed to open task service with peer {peer_id}"))?
            .get()
            .context(format!("invalid task response from peer {peer_id}"))?
            .get_task()
            .context(format!("missing task service for peer {peer_id}"))?;

        let mut stop_req = task_client.stop_request();
        {
            let mut request = stop_req.get().init_request();
            request.set_id(spec.id.as_bytes());
        }
        let response = stop_req
            .send()
            .promise
            .await
            .context(format!("stop request failed on peer {peer_id}"))?;
        let reader = response
            .get()
            .context(format!("invalid stop response from peer {peer_id}"))?
            .get_spec()
            .context(format!("missing task spec in stop response from peer {peer_id}"))?;

        read_spec(reader).map_err(|err| anyhow::anyhow!("failed to decode stop response: {err}"))
    }

    /// Requests remote peers to stop tasks so rollbacks do not leak running containers.
    pub(super) async fn signal_remote_stop(&self, specs: &[(usize, TaskSpec)]) {
        if specs.is_empty() {
            return;
        }

        for (_, spec) in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if matches!(spec.state, ContainerState::Stopped) {
                continue;
            }

            if let Err(err) = self.stop_remote_task(spec).await {
                warn!(
                    target: "task",
                    "failed to request remote stop for task {} on peer {}: {err}",
                    spec.id,
                    spec.node_id
                );
            }
        }
    }

    /// Creates task specs for remote placements and persists them locally.
    pub(super) async fn materialize_remote_specs(
        &self,
        plans: &[RemoteStartPlan],
    ) -> Result<Vec<(usize, TaskSpec)>, ExecutionError> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let mut results: Vec<(usize, TaskSpec)> = Vec::new();
        let mut persisted: Vec<TaskSpec> = Vec::new();

        for plan in plans {
            let slot_ids: Vec<SlotId> = plan.slots.iter().map(|slot| slot.slot_id).collect();
            if slot_ids.is_empty() {
                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                    "remote plan missing slot assignments"
                )));
            }

            let node_name = self
                .registry
                .peer_hostname(plan.peer_id)
                .unwrap_or_else(|| plan.peer_id.to_string());

            let spec = TaskSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Pending,
                created_at: Utc::now().to_rfc3339(),
                updated_at: Utc::now().to_rfc3339(),
                command: plan.command.clone(),
                node_id: plan.peer_id,
                node_name,
                slot_ids: slot_ids.clone(),
                slot_id: slot_ids.first().copied(),
                cpu_millis: plan.cpu_millis,
                memory_bytes: plan.memory_bytes,
                restart_policy: plan.restart_policy.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                networks: plan.networks.clone(),
                service_metadata: plan.service_metadata.clone(),
            };

            if let Err(err) = self.persist_spec(&spec).await {
                for rollback in &persisted {
                    if let Err(cleanup) = self.remove_spec(rollback.id).await {
                        warn!(
                            target: "task",
                            "failed to rollback remote task {} after error: {cleanup}",
                            rollback.id
                        );
                    }
                }
                let err = err.context(format!(
                    "failed to persist remote task spec {} ({})",
                    spec.name, spec.id
                ));
                return Err(ExecutionError::Fatal(err));
            }

            persisted.push(spec.clone());
            results.push((plan.index, spec));
        }

        for (_, spec) in &results {
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to enqueue task gossip for {}: {err}",
                    spec.name
                );
            }
        }

        Ok(results)
    }
}
