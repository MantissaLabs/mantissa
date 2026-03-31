use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Context;
use chrono::Utc;
use protocol::scheduling::{self, prepare_leases_response};
use protocol::server::cluster_session;
use tracing::warn;
use uuid::Uuid;

use crate::scheduler::digest::{SchedulerDigestValue, read_scheduler_digest};
use crate::scheduler::{GpuReservationRequest, SchedulerError, SlotId, SlotReservationRequest};
use crate::workload::model::{WorkloadEvent, WorkloadPhase, WorkloadSpec};
use crate::workload::service::read_spec;

use super::WorkloadManager;
use super::planner::{BatchStartPlan, PreparedRemoteStartPlan, RemoteStartPlan};

/// Error returned by slot reservation stages, signalling whether the caller should retry.
pub(super) enum ExecutionError {
    Retry(anyhow::Error),
    Fatal(anyhow::Error),
}

/// Tracks local reservations so they can be released on rollback.
pub(super) struct ReservedResources {
    pub(super) slots: Vec<SlotId>,
    pub(super) gpu_device_ids: Vec<String>,
}

/// Tracks prepared remote leases so they can be aborted on rollback.
pub(super) struct RemoteReservation {
    pub(super) leases: Vec<RemoteLeaseReservation>,
}

pub(super) struct RemoteLeaseReservation {
    pub(super) lease_id: Uuid,
    pub(super) task_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RemotePrepareRejectionReason {
    InsufficientResources,
    Uninitialized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RemotePrepareRejection {
    pub(super) reason: RemotePrepareRejectionReason,
    pub(super) digest: SchedulerDigestValue,
}

/// Maps one structured rejection reason into the human-readable task-manager retry message.
fn describe_remote_prepare_rejection(reason: RemotePrepareRejectionReason) -> &'static str {
    match reason {
        RemotePrepareRejectionReason::InsufficientResources => "insufficient resources",
        RemotePrepareRejectionReason::Uninitialized => "scheduler uninitialized",
    }
}

/// Decodes one structured prepare rejection returned by a remote scheduler RPC.
fn parse_prepare_rejection(
    reader: scheduling::prepare_leases_rejected::Reader<'_>,
) -> Result<RemotePrepareRejection, anyhow::Error> {
    let reason = match reader.get_reason()? {
        scheduling::PrepareLeasesRejectionReason::InsufficientResources => {
            RemotePrepareRejectionReason::InsufficientResources
        }
        scheduling::PrepareLeasesRejectionReason::Uninitialized => {
            RemotePrepareRejectionReason::Uninitialized
        }
    };
    let digest = read_scheduler_digest(reader.get_current_digest()?)?;
    Ok(RemotePrepareRejection { reason, digest })
}

fn parse_uuid(bytes: capnp::data::Reader<'_>) -> Result<Uuid, anyhow::Error> {
    if bytes.len() != 16 {
        return Err(anyhow::anyhow!("uuid fields must be 16 bytes"));
    }

    let mut raw = [0u8; 16];
    raw.copy_from_slice(bytes);
    Ok(Uuid::from_bytes(raw))
}

impl WorkloadManager {
    /// Applies one structured remote prepare rejection so the next shortlist uses fresher peer state.
    pub(super) async fn apply_remote_prepare_rejection(
        &self,
        peer_id: Uuid,
        rejection: RemotePrepareRejection,
    ) -> Result<(), anyhow::Error> {
        if rejection.digest.node_id != peer_id {
            return Err(anyhow::anyhow!(
                "peer {peer_id} returned scheduler digest for unexpected node {}",
                rejection.digest.node_id
            ));
        }

        self.core
            .scheduler
            .observe_scheduler_digest(rejection.digest)
            .await?;
        self.local_state
            .remote_prepare_feedback
            .record_retryable_failure(peer_id);
        Ok(())
    }

    /// Releases a single slot via the scheduler, retrying on snapshot mismatches.
    pub(super) async fn release_slot(&self, slot_id: SlotId) -> Result<(), anyhow::Error> {
        const MAX_ATTEMPTS: usize = 10;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.core.scheduler.snapshot().await {
                Some(s) => s,
                None => return Err(anyhow::anyhow!("scheduler snapshot unavailable")),
            };

            match self
                .core
                .scheduler
                .free_slots(snapshot.version, [slot_id])
                .await
            {
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

    /// Reserves the local slots and GPUs required by the batch, returning the new reservations.
    pub(super) async fn reserve_local_resources(
        &self,
        plans: &[BatchStartPlan],
        expected_version: u64,
    ) -> Result<ReservedResources, ExecutionError> {
        if plans.is_empty() {
            return Ok(ReservedResources {
                slots: Vec::new(),
                gpu_device_ids: Vec::new(),
            });
        }

        let mut slot_requests = Vec::new();
        let mut gpu_requests = Vec::new();
        let mut newly_reserved_slots = Vec::new();
        let mut newly_reserved_gpus = Vec::new();

        for plan in plans {
            if plan.preassigned {
                continue;
            }

            for slot in &plan.slots {
                slot_requests.push(SlotReservationRequest {
                    slot_id: slot.slot_id,
                    owner: self.local_node_id,
                    task_id: Some(plan.id),
                });
                newly_reserved_slots.push(slot.slot_id);
            }

            for device_id in &plan.gpu_device_ids {
                gpu_requests.push(GpuReservationRequest {
                    device_id: device_id.clone(),
                    owner: self.local_node_id,
                    task_id: Some(plan.id),
                });
                newly_reserved_gpus.push(device_id.clone());
            }
        }

        if slot_requests.is_empty() && gpu_requests.is_empty() {
            return Ok(ReservedResources {
                slots: Vec::new(),
                gpu_device_ids: Vec::new(),
            });
        }

        match self
            .core
            .scheduler
            .reserve_resources(expected_version, slot_requests, gpu_requests)
            .await
        {
            Ok(_) => Ok(ReservedResources {
                slots: newly_reserved_slots,
                gpu_device_ids: newly_reserved_gpus,
            }),
            Err(err @ SchedulerError::SnapshotMismatch { .. })
            | Err(err @ SchedulerError::SlotsUnavailable { .. })
            | Err(err @ SchedulerError::UnknownSlots { .. }) => {
                Err(ExecutionError::Retry(anyhow::anyhow!(err)))
            }
            Err(err @ SchedulerError::GpuDevicesUnavailable { .. })
            | Err(err @ SchedulerError::UnknownGpuDevices { .. }) => {
                Err(ExecutionError::Retry(anyhow::anyhow!(err)))
            }
            Err(err) => Err(ExecutionError::Fatal(anyhow::anyhow!(err))),
        }
    }

    /// Releases the provided local slots and GPUs, logging but otherwise ignoring failures.
    pub(super) async fn release_local_resources(&self, resources: &ReservedResources) {
        if resources.slots.is_empty() && resources.gpu_device_ids.is_empty() {
            return;
        }

        let mut slot_seen = HashSet::new();
        let mut slots = Vec::new();
        for slot_id in &resources.slots {
            if slot_seen.insert(*slot_id) {
                slots.push(*slot_id);
            }
        }

        let mut gpu_seen = HashSet::new();
        let mut gpu_device_ids = Vec::new();
        for device_id in &resources.gpu_device_ids {
            if gpu_seen.insert(device_id.as_str()) {
                gpu_device_ids.push(device_id.clone());
            }
        }

        const MAX_ATTEMPTS: usize = 10;

        for _ in 0..MAX_ATTEMPTS {
            let expected_version = match self.core.scheduler.snapshot().await {
                Some(snapshot) => snapshot.version,
                None => {
                    warn!(
                        target: "task",
                        "failed to release local resources; scheduler snapshot unavailable"
                    );
                    return;
                }
            };

            match self
                .core
                .scheduler
                .free_resources(expected_version, slots.clone(), gpu_device_ids.clone())
                .await
            {
                Ok(_) => return,
                Err(SchedulerError::SnapshotMismatch { .. }) => continue,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to release local resources: {err}"
                    );
                    return;
                }
            }
        }

        warn!(
            target: "task",
            "failed to release local resources after retries"
        );
    }

    /// Prepares remote leases grouped per target node and returns the rollback map.
    pub(super) async fn prepare_remote_leases(
        &self,
        plans: &[RemoteStartPlan],
    ) -> Result<
        (
            HashMap<Uuid, RemoteReservation>,
            Vec<PreparedRemoteStartPlan>,
        ),
        ExecutionError,
    > {
        let mut reservations = HashMap::new();
        let mut prepared_plans = Vec::new();
        if plans.is_empty() {
            return Ok((reservations, prepared_plans));
        }

        let mut grouped: HashMap<Uuid, Vec<&RemoteStartPlan>> = HashMap::new();
        for plan in plans {
            grouped.entry(plan.peer_id).or_default().push(plan);
        }

        for (peer_id, peer_plans) in grouped {
            let session = match self.remote_session(peer_id).await {
                Ok(session) => session,
                Err(err) => {
                    self.local_state
                        .remote_prepare_feedback
                        .record_retryable_failure(peer_id);
                    self.abort_remote_leases(&reservations).await;
                    return Err(ExecutionError::Retry(err));
                }
            };

            let scheduler_client =
                match session.clone().get_scheduler_request().send().promise.await {
                    Ok(resp) => match resp.get() {
                        Ok(result) => match result.get_scheduler() {
                            Ok(client) => client,
                            Err(err) => {
                                self.local_state
                                    .remote_prepare_feedback
                                    .record_retryable_failure(peer_id);
                                self.abort_remote_leases(&reservations).await;
                                return Err(ExecutionError::Retry(anyhow::anyhow!(
                                    err.to_string()
                                )));
                            }
                        },
                        Err(err) => {
                            self.local_state
                                .remote_prepare_feedback
                                .record_retryable_failure(peer_id);
                            self.abort_remote_leases(&reservations).await;
                            return Err(ExecutionError::Retry(anyhow::anyhow!(err.to_string())));
                        }
                    },
                    Err(err) => {
                        self.local_state
                            .remote_prepare_feedback
                            .record_retryable_failure(peer_id);
                        self.abort_remote_leases(&reservations).await;
                        return Err(ExecutionError::Retry(anyhow::anyhow!(err.to_string())));
                    }
                };

            let mut prepare_req = scheduler_client.prepare_leases_request();
            {
                let mut inner = prepare_req.get().init_request();
                inner.set_coordinator_node_id(self.local_node_id.as_bytes());
                inner.set_ttl_ms(30_000);
                let mut intents_builder = inner.reborrow().init_intents(peer_plans.len() as u32);
                for (idx, plan) in peer_plans.iter().enumerate() {
                    let mut entry = intents_builder.reborrow().get(idx as u32);
                    entry.set_task_id(plan.id.as_bytes());
                    entry.set_cpu_millis(plan.cpu_millis);
                    entry.set_memory_bytes(plan.memory_bytes);
                    entry.set_gpu_count(plan.gpu_count);
                }
            }

            match prepare_req.send().promise.await {
                Ok(resp) => match resp.get() {
                    Ok(result) => match result.get_response() {
                        Ok(response) => match response.which() {
                            Ok(prepare_leases_response::Prepared(Ok(leases))) => {
                                let mut bindings_by_task = HashMap::new();
                                for lease in leases.iter() {
                                    let lease_id = match lease.get_lease_id() {
                                        Ok(bytes) => match parse_uuid(bytes) {
                                            Ok(lease_id) => lease_id,
                                            Err(err) => {
                                                self.abort_remote_leases(&reservations).await;
                                                return Err(ExecutionError::Fatal(err));
                                            }
                                        },
                                        Err(err) => {
                                            self.abort_remote_leases(&reservations).await;
                                            return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                                err.to_string()
                                            )));
                                        }
                                    };
                                    let task_id = match lease.get_task_id() {
                                        Ok(bytes) => match parse_uuid(bytes) {
                                            Ok(task_id) => task_id,
                                            Err(err) => {
                                                self.abort_remote_leases(&reservations).await;
                                                return Err(ExecutionError::Fatal(err));
                                            }
                                        },
                                        Err(err) => {
                                            self.abort_remote_leases(&reservations).await;
                                            return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                                err.to_string()
                                            )));
                                        }
                                    };
                                    let expires_at_unix_ms = lease.get_expires_at_unix_ms();
                                    let slot_ids = match lease.get_slot_ids() {
                                        Ok(ids) => ids.iter().collect::<Vec<_>>(),
                                        Err(err) => {
                                            self.abort_remote_leases(&reservations).await;
                                            return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                                err.to_string()
                                            )));
                                        }
                                    };
                                    let gpu_devices = match lease.get_gpu_device_ids() {
                                        Ok(ids) => ids,
                                        Err(err) => {
                                            self.abort_remote_leases(&reservations).await;
                                            return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                                err.to_string()
                                            )));
                                        }
                                    };
                                    let mut gpu_device_ids =
                                        Vec::with_capacity(gpu_devices.len() as usize);
                                    for device_id in gpu_devices.iter() {
                                        let value = match device_id {
                                            Ok(value) => value,
                                            Err(err) => {
                                                self.abort_remote_leases(&reservations).await;
                                                return Err(ExecutionError::Fatal(
                                                    anyhow::anyhow!(err.to_string()),
                                                ));
                                            }
                                        };
                                        let value = match value.to_str() {
                                            Ok(value) => value,
                                            Err(err) => {
                                                self.abort_remote_leases(&reservations).await;
                                                return Err(ExecutionError::Fatal(
                                                    anyhow::anyhow!(err.to_string()),
                                                ));
                                            }
                                        };
                                        gpu_device_ids.push(value.to_string());
                                    }

                                    if bindings_by_task
                                        .insert(
                                            task_id,
                                            (
                                                lease_id,
                                                expires_at_unix_ms,
                                                slot_ids,
                                                gpu_device_ids,
                                            ),
                                        )
                                        .is_some()
                                    {
                                        self.abort_remote_leases(&reservations).await;
                                        return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                            "duplicate prepared binding returned for task {task_id}"
                                        )));
                                    }
                                }
                                let mut lease_reservations = Vec::new();
                                for plan in &peer_plans {
                                    let Some((
                                        lease_id,
                                        _expires_at_unix_ms,
                                        prepared_slot_ids,
                                        prepared_gpu_ids,
                                    )) = bindings_by_task.remove(&plan.id)
                                    else {
                                        self.abort_remote_leases(&reservations).await;
                                        return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                            "missing prepared binding for remote task {} on peer {}",
                                            plan.id,
                                            peer_id
                                        )));
                                    };

                                    if prepared_slot_ids.is_empty() {
                                        self.abort_remote_leases(&reservations).await;
                                        return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                            "prepared remote task {} on peer {} without slots",
                                            plan.id,
                                            peer_id
                                        )));
                                    }

                                    if prepared_gpu_ids.len() < plan.gpu_count as usize {
                                        self.abort_remote_leases(&reservations).await;
                                        return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                            "prepared remote task {} on peer {} returned only {} GPU(s) for request of {}",
                                            plan.id,
                                            peer_id,
                                            prepared_gpu_ids.len(),
                                            plan.gpu_count
                                        )));
                                    }

                                    lease_reservations.push(RemoteLeaseReservation {
                                        lease_id,
                                        task_id: plan.id,
                                    });
                                    prepared_plans.push(PreparedRemoteStartPlan {
                                        index: plan.index,
                                        id: plan.id,
                                        lease_id,
                                        lease_coordinator_node_id: self.local_node_id,
                                        name: plan.name.clone(),
                                        image: plan.image.clone(),
                                        execution_substrate: plan.execution_substrate,
                                        isolation_mode: plan.isolation_mode,
                                        isolation_profile: plan.isolation_profile.clone(),
                                        command: plan.command.clone(),
                                        tty: plan.tty,
                                        cpu_millis: plan.cpu_millis,
                                        memory_bytes: plan.memory_bytes,
                                        gpu_count: plan.gpu_count,
                                        slot_ids: prepared_slot_ids,
                                        gpu_device_ids: prepared_gpu_ids,
                                        peer_id: plan.peer_id,
                                        restart_policy: plan.restart_policy.clone(),
                                        termination_grace_period_secs: plan
                                            .termination_grace_period_secs,
                                        pre_stop_command: plan.pre_stop_command.clone(),
                                        liveness: plan.liveness.clone(),
                                        env: plan.env.clone(),
                                        secret_files: plan.secret_files.clone(),
                                        volumes: plan.volumes.clone(),
                                        networks: plan.networks.clone(),
                                        service_metadata: plan.service_metadata.clone(),
                                        job_metadata: plan.job_metadata.clone(),
                                        agent_run_metadata: plan.agent_run_metadata.clone(),
                                    });
                                }

                                if !bindings_by_task.is_empty() {
                                    self.abort_remote_leases(&reservations).await;
                                    return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                        "peer {peer_id} returned unexpected prepared bindings"
                                    )));
                                }

                                reservations.insert(
                                    peer_id,
                                    RemoteReservation {
                                        leases: lease_reservations,
                                    },
                                );
                                self.local_state
                                    .remote_prepare_feedback
                                    .clear_success(peer_id);
                            }
                            Ok(prepare_leases_response::Prepared(Err(err))) => {
                                self.abort_remote_leases(&reservations).await;
                                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                    err.to_string()
                                )));
                            }
                            Ok(prepare_leases_response::Rejected(Ok(rejected))) => {
                                let rejection = match parse_prepare_rejection(rejected) {
                                    Ok(rejection) => rejection,
                                    Err(err) => {
                                        self.abort_remote_leases(&reservations).await;
                                        return Err(ExecutionError::Fatal(err));
                                    }
                                };
                                let rejection_message = format!(
                                    "peer {peer_id} rejected lease prepare: {} (digest version {}, free slots {}, free cpu {}, free memory {}, free gpu {})",
                                    describe_remote_prepare_rejection(rejection.reason),
                                    rejection.digest.snapshot_version,
                                    rejection.digest.free_slot_count,
                                    rejection.digest.free_cpu_millis,
                                    rejection.digest.free_memory_bytes,
                                    rejection.digest.free_gpu_count,
                                );
                                self.abort_remote_leases(&reservations).await;
                                if let Err(err) = self
                                    .apply_remote_prepare_rejection(peer_id, rejection)
                                    .await
                                {
                                    return Err(ExecutionError::Fatal(err));
                                }
                                return Err(ExecutionError::Retry(anyhow::anyhow!(
                                    rejection_message
                                )));
                            }
                            Ok(prepare_leases_response::Rejected(Err(err))) => {
                                self.abort_remote_leases(&reservations).await;
                                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                    err.to_string()
                                )));
                            }
                            Err(err) => {
                                self.abort_remote_leases(&reservations).await;
                                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                                    err.to_string()
                                )));
                            }
                        },
                        Err(err) => {
                            self.abort_remote_leases(&reservations).await;
                            return Err(ExecutionError::Fatal(anyhow::anyhow!(err.to_string())));
                        }
                    },
                    Err(err) => {
                        self.abort_remote_leases(&reservations).await;
                        return Err(ExecutionError::Fatal(anyhow::anyhow!(err.to_string())));
                    }
                },
                Err(err) => {
                    self.abort_remote_leases(&reservations).await;
                    self.local_state
                        .remote_prepare_feedback
                        .record_retryable_failure(peer_id);
                    return Err(ExecutionError::Retry(anyhow::anyhow!(err.to_string())));
                }
            }
        }

        Ok((reservations, prepared_plans))
    }

    /// Aborts prepared remote leases accumulated during previous stages.
    pub(super) async fn abort_remote_leases(
        &self,
        reservations: &HashMap<Uuid, RemoteReservation>,
    ) {
        for (peer_id, reservation) in reservations {
            let session = match self.remote_session(*peer_id).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to reopen session with peer {peer_id} while aborting leases: {err}"
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
                                "failed to access scheduler for peer {peer_id} while aborting leases: {err}"
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

            let mut release_req = scheduler_client.abort_leases_request();
            {
                let mut inner = release_req.get().init_request();
                inner.set_coordinator_node_id(self.local_node_id.as_bytes());
                let mut intents = inner
                    .reborrow()
                    .init_intents(reservation.leases.len() as u32);
                for (idx, lease) in reservation.leases.iter().enumerate() {
                    let mut entry = intents.reborrow().get(idx as u32);
                    entry.set_lease_id(lease.lease_id.as_bytes());
                    entry.set_task_id(lease.task_id.as_bytes());
                }
            }

            if let Err(err) = release_req.send().promise.await {
                warn!(
                    target: "task",
                    "failed to abort leases on peer {peer_id}: {err}"
                );
            }
        }
    }

    pub(super) async fn remote_session(
        &self,
        peer_id: Uuid,
    ) -> Result<cluster_session::Client, anyhow::Error> {
        self.core
            .registry
            .session_for_peer(peer_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("no active session for peer {peer_id}"))
    }

    /// Requests a remote peer to stop a task so the owner updates state and broadcasts it.
    pub(super) async fn stop_remote_workload(
        &self,
        spec: &WorkloadSpec,
    ) -> Result<WorkloadSpec, anyhow::Error> {
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
            .get_workload_request()
            .send()
            .promise
            .await
            .context(format!(
                "failed to open workload service with peer {peer_id}"
            ))?
            .get()
            .context(format!("invalid workload response from peer {peer_id}"))?
            .get_workload()
            .context(format!("missing workload service for peer {peer_id}"))?;

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
            .context(format!(
                "missing task spec in stop response from peer {peer_id}"
            ))?;

        read_spec(reader).map_err(|err| anyhow::anyhow!("failed to decode stop response: {err}"))
    }

    /// Requests remote peers to stop tasks so rollbacks do not leak running runtime instances.
    pub(super) async fn signal_remote_stop(&self, specs: &[(usize, WorkloadSpec)]) {
        if specs.is_empty() {
            return;
        }

        for (_, spec) in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if matches!(spec.state, WorkloadPhase::Stopped) {
                continue;
            }

            if let Err(err) = self.stop_remote_workload(spec).await {
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
        plans: &[PreparedRemoteStartPlan],
    ) -> Result<Vec<(usize, WorkloadSpec)>, ExecutionError> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let mut results: Vec<(usize, WorkloadSpec)> = Vec::new();

        for plan in plans {
            let slot_ids = plan.slot_ids.clone();
            if slot_ids.is_empty() {
                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                    "remote plan missing slot assignments"
                )));
            }

            let node_name = self
                .core
                .registry
                .peer_hostname(plan.peer_id)
                .unwrap_or_else(|| plan.peer_id.to_string());
            let task_epoch = self
                .next_task_epoch_for_assignment(plan.id, plan.peer_id, &slot_ids)
                .await
                .map_err(ExecutionError::Fatal)?;

            let spec = WorkloadSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                execution_substrate: plan.execution_substrate,
                isolation_mode: plan.isolation_mode,
                isolation_profile: plan.isolation_profile.clone(),
                state: WorkloadPhase::Pending,
                phase_reason: None,
                phase_progress: None,
                created_at: Utc::now().to_rfc3339(),
                updated_at: Utc::now().to_rfc3339(),
                command: plan.command.clone(),
                tty: plan.tty,
                node_id: plan.peer_id,
                node_name,
                slot_ids: slot_ids.clone(),
                slot_id: slot_ids.first().copied(),
                cpu_millis: plan.cpu_millis,
                memory_bytes: plan.memory_bytes,
                gpu_count: plan.gpu_count,
                gpu_device_ids: plan.gpu_device_ids.clone(),
                restart_policy: plan.restart_policy.clone(),
                termination_grace_period_secs: plan.termination_grace_period_secs,
                pre_stop_command: plan.pre_stop_command.clone(),
                liveness: plan.liveness.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                volumes: plan.volumes.clone(),
                networks: plan.networks.clone(),
                service_metadata: plan.service_metadata.clone(),
                job_metadata: plan.job_metadata.clone(),
                agent_run_metadata: plan.agent_run_metadata.clone(),
                lease_id: Some(plan.lease_id),
                lease_coordinator_node_id: Some(plan.lease_coordinator_node_id),
                task_epoch,
                phase_version: 0,
                launch_attempt: 0,
                last_terminal_observed_launch: None,
            };
            results.push((plan.index, spec));
        }

        let specs: Vec<WorkloadSpec> = results.iter().map(|(_, spec)| spec.clone()).collect();
        if let Err(err) = self.persist_specs_batch(&specs).await {
            return Err(ExecutionError::Fatal(
                err.context("failed to persist remote task specs for batch"),
            ));
        }

        for (_, spec) in &results {
            if let Err(err) = self
                .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
                .await
            {
                warn!(
                    target: "task",
                    "failed to record workload gossip for {}: {err}",
                    spec.name
                );
            }
        }

        Ok(results)
    }
}
