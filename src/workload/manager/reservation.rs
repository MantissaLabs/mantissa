use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Instant;

use anyhow::Context;
use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use mantissa_protocol::scheduling::{self, prepare_leases_response, scheduler as scheduler_rpc};
use mantissa_protocol::server::cluster_session;
use mantissa_protocol::workload::workload as workload_rpc;
use tracing::warn;
use uuid::Uuid;

use crate::scheduler::digest::{SchedulerDigestValue, read_scheduler_digest};
use crate::scheduler::{
    ExactTaskLeaseIntent, GpuReservationRequest, PreparedTaskLease, PreparedTaskLeaseBatch,
    SchedulerError, SlotId, SlotReservationRequest,
};
use crate::workload::model::{WorkloadAdmissionState, WorkloadPhase, WorkloadSpec};
use crate::workload::service::{read_spec, write_spec};

use super::WorkloadManager;
use super::planner::{BatchStartPlan, PreparedRemoteStartPlan, RemoteStartPlan};

/// Default lifetime for prepared scheduler leases while a batch or group admission commits.
pub(super) const DEFAULT_PREPARED_LEASE_TTL_MS: u64 = 30_000;

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

/// Tracks remote group leases so they can be committed or aborted together.
pub(super) struct RemoteGroupReservation {
    pub(super) leases: Vec<PreparedTaskLease>,
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

/// Prepared binding returned by one remote scheduler for a single task.
#[derive(Clone)]
struct PreparedRemoteLeaseBinding {
    lease_id: Uuid,
    expires_at_unix_ms: u64,
    slot_ids: Vec<SlotId>,
    gpu_device_ids: Vec<String>,
}

/// Structured outcome of a remote prepare RPC after Cap'n Proto decoding.
enum RemotePrepareOutcome {
    Prepared(HashMap<Uuid, PreparedRemoteLeaseBinding>),
    Rejected(RemotePrepareRejection),
}

/// Deferred remote prepare failure kept while draining already-started peers.
enum RemotePrepareFailure {
    Execution(ExecutionError),
    Rejection {
        peer_id: Uuid,
        operation: &'static str,
        rejection: RemotePrepareRejection,
    },
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

/// Builds the retry detail for one structured prepare rejection from a remote scheduler.
fn remote_prepare_rejection_message(
    peer_id: Uuid,
    operation: &'static str,
    rejection: &RemotePrepareRejection,
) -> String {
    let reason = match rejection.reason {
        RemotePrepareRejectionReason::InsufficientResources => "not enough slots or resources",
        RemotePrepareRejectionReason::Uninitialized => "scheduler uninitialized",
    };

    format!(
        "peer {peer_id} rejected {operation}: {reason} (digest version {}, free slots {}, free cpu {}, free memory {}, free gpu {})",
        rejection.digest.snapshot_version,
        rejection.digest.free_slot_count,
        rejection.digest.free_cpu_millis,
        rejection.digest.free_memory_bytes,
        rejection.digest.free_gpu_count,
    )
}

/// Resolves configured remote admission concurrency, clamped to a non-zero value.
fn remote_admission_parallelism() -> usize {
    crate::config::replication_runtime_config()
        .remote_admission_parallelism
        .max(1)
}

/// Resolves configured remote assignment delivery concurrency, clamped to a non-zero value.
fn remote_assignment_parallelism() -> usize {
    crate::config::replication_runtime_config()
        .remote_assignment_parallelism
        .max(1)
}

/// Returns the bounded metrics label for one remote prepare peer result.
fn remote_prepare_result_label(
    result: &Result<RemotePrepareOutcome, ExecutionError>,
) -> &'static str {
    match result {
        Ok(RemotePrepareOutcome::Prepared(_)) => "prepared",
        Ok(RemotePrepareOutcome::Rejected(_)) => "rejected",
        Err(ExecutionError::Retry(_)) => "retry",
        Err(ExecutionError::Fatal(_)) => "fatal",
    }
}

/// Returns the bounded metrics label for one remote assignment delivery result.
fn remote_assignment_result_label(result: &Result<(), anyhow::Error>) -> &'static str {
    if result.is_ok() {
        "delivered"
    } else {
        "failed"
    }
}

/// Decodes one prepared lease row returned by a remote scheduler.
fn parse_prepared_remote_lease(
    reader: scheduling::prepared_lease::Reader<'_>,
) -> Result<(Uuid, PreparedRemoteLeaseBinding), anyhow::Error> {
    let lease_id = parse_uuid(reader.get_lease_id().context("read prepared lease id")?)
        .context("decode prepared lease id")?;
    let task_id = parse_uuid(reader.get_task_id().context("read prepared task id")?)
        .context("decode prepared task id")?;
    let slot_ids = reader
        .get_slot_ids()
        .context("read prepared slot ids")?
        .iter()
        .collect::<Vec<_>>();

    let gpu_devices = reader
        .get_gpu_device_ids()
        .context("read prepared GPU device ids")?;
    let mut gpu_device_ids = Vec::with_capacity(gpu_devices.len() as usize);
    for device_id in gpu_devices.iter() {
        let device_id = device_id.context("read prepared GPU device id")?;
        gpu_device_ids.push(
            device_id
                .to_str()
                .context("decode prepared GPU device id")?
                .to_string(),
        );
    }

    Ok((
        task_id,
        PreparedRemoteLeaseBinding {
            lease_id,
            expires_at_unix_ms: reader.get_expires_at_unix_ms(),
            slot_ids,
            gpu_device_ids,
        },
    ))
}

/// Decodes the remote scheduler prepare response into a small domain enum.
fn parse_prepare_response(
    response: scheduling::prepare_leases_response::Reader<'_>,
) -> Result<RemotePrepareOutcome, anyhow::Error> {
    match response
        .which()
        .context("read prepareLeases response variant")?
    {
        prepare_leases_response::Prepared(Ok(leases)) => {
            let mut bindings_by_task = HashMap::new();
            for lease in leases.iter() {
                let (task_id, binding) = parse_prepared_remote_lease(lease)?;
                if bindings_by_task.insert(task_id, binding).is_some() {
                    return Err(anyhow::anyhow!(
                        "duplicate prepared binding returned for task {task_id}"
                    ));
                }
            }
            Ok(RemotePrepareOutcome::Prepared(bindings_by_task))
        }
        prepare_leases_response::Prepared(Err(err)) => Err(anyhow::anyhow!(err.to_string())),
        prepare_leases_response::Rejected(Ok(rejected)) => Ok(RemotePrepareOutcome::Rejected(
            parse_prepare_rejection(rejected).context("decode prepare rejection")?,
        )),
        prepare_leases_response::Rejected(Err(err)) => Err(anyhow::anyhow!(err.to_string())),
    }
}

/// Converts decoded remote bindings into the exact lease rows required by group commit RPCs.
fn remote_group_reservation_from_bindings(
    bindings_by_task: &HashMap<Uuid, PreparedRemoteLeaseBinding>,
) -> RemoteGroupReservation {
    let mut leases = bindings_by_task
        .iter()
        .map(|(task_id, binding)| PreparedTaskLease {
            lease_id: binding.lease_id,
            task_id: *task_id,
            expires_at_unix_ms: binding.expires_at_unix_ms,
            slot_ids: binding.slot_ids.clone(),
            gpu_device_ids: binding.gpu_device_ids.clone(),
        })
        .collect::<Vec<_>>();
    leases.sort_by_key(|lease| lease.task_id);
    RemoteGroupReservation { leases }
}

/// Encodes prepared lease rows into a scheduler group commit request.
fn write_prepared_leases_for_request(
    mut builder: capnp::struct_list::Builder<scheduling::prepared_lease::Owned>,
    leases: &[PreparedTaskLease],
) {
    for (idx, lease) in leases.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_lease_id(lease.lease_id.as_bytes());
        entry.set_task_id(lease.task_id.as_bytes());
        entry.set_expires_at_unix_ms(lease.expires_at_unix_ms);

        let mut slot_ids = entry.reborrow().init_slot_ids(lease.slot_ids.len() as u32);
        for (slot_idx, slot_id) in lease.slot_ids.iter().enumerate() {
            slot_ids.set(slot_idx as u32, *slot_id);
        }

        let mut gpu_ids = entry
            .reborrow()
            .init_gpu_device_ids(lease.gpu_device_ids.len() as u32);
        for (gpu_idx, device_id) in lease.gpu_device_ids.iter().enumerate() {
            gpu_ids.set(gpu_idx as u32, device_id);
        }
    }
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
                    group_id: None,
                });
                newly_reserved_slots.push(slot.slot_id);
            }

            for device_id in &plan.gpu_device_ids {
                gpu_requests.push(GpuReservationRequest {
                    device_id: device_id.clone(),
                    owner: self.local_node_id,
                    task_id: Some(plan.id),
                    group_id: None,
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

    /// Prepares exact local leases for one gang admission group without making rows runnable.
    pub(super) async fn prepare_local_lease_group(
        &self,
        group_id: Uuid,
        plans: &[BatchStartPlan],
        expected_version: u64,
        ttl_ms: u64,
    ) -> Result<PreparedTaskLeaseBatch, ExecutionError> {
        if plans.is_empty() {
            return Ok(PreparedTaskLeaseBatch { leases: Vec::new() });
        }

        let mut intents = Vec::with_capacity(plans.len());
        for plan in plans {
            if plan.preassigned {
                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                    "gang admission does not support preassigned local slots for task {}",
                    plan.name
                )));
            }

            intents.push(ExactTaskLeaseIntent {
                task_id: plan.id,
                slot_ids: plan.slot_ids(),
                gpu_device_ids: plan.gpu_device_ids.clone(),
            });
        }

        match self
            .core
            .scheduler
            .prepare_exact_task_lease_group(
                expected_version,
                self.local_node_id,
                group_id,
                ttl_ms,
                intents,
            )
            .await
        {
            Ok(prepared) => Ok(prepared),
            Err(err @ SchedulerError::SnapshotMismatch { .. })
            | Err(err @ SchedulerError::SlotsUnavailable { .. })
            | Err(err @ SchedulerError::GpuDevicesUnavailable { .. })
            | Err(err @ SchedulerError::UnknownSlots { .. })
            | Err(err @ SchedulerError::UnknownGpuDevices { .. }) => {
                Err(ExecutionError::Retry(anyhow::anyhow!(err)))
            }
            Err(err) => Err(ExecutionError::Fatal(anyhow::anyhow!(err))),
        }
    }

    /// Commits every local prepared lease in one admission group.
    pub(super) async fn commit_local_lease_group(
        &self,
        group_id: Uuid,
        prepared: &PreparedTaskLeaseBatch,
    ) -> Result<(), ExecutionError> {
        if prepared.leases.is_empty() {
            return Ok(());
        }

        self.core
            .scheduler
            .commit_task_lease_group(group_id, self.local_node_id, &prepared.leases)
            .await
            .map(|_| ())
            .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err)))
    }

    /// Aborts every still-prepared local lease in one admission group.
    pub(super) async fn abort_local_lease_group(&self, group_id: Uuid) {
        if let Err(err) = self
            .core
            .scheduler
            .abort_task_lease_group(self.local_node_id, group_id)
            .await
        {
            warn!(
                target: "task",
                "failed to abort local lease group {group_id}: {err}"
            );
        }
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
        crate::observability::metrics::record_remote_prepare_batch(
            "incremental",
            grouped.len(),
            plans.len(),
        );

        let parallelism = remote_admission_parallelism();
        let batch_started = Instant::now();
        let mut first_failure = None;
        let mut peer_ids = grouped.keys().copied().collect::<Vec<_>>();
        peer_ids.sort_unstable();
        let mut pending_peers = peer_ids.into_iter();
        let mut inflight = FuturesUnordered::new();

        loop {
            while first_failure.is_none() && inflight.len() < parallelism {
                let Some(peer_id) = pending_peers.next() else {
                    break;
                };
                let peer_plans = &grouped[&peer_id];
                crate::observability::metrics::record_remote_prepare_peer(
                    "incremental",
                    peer_plans.len(),
                );
                inflight.push(self.prepare_incremental_remote_peer(
                    peer_id,
                    peer_plans,
                    batch_started,
                ));
            }

            let Some((peer_id, outcome)) = inflight.next().await else {
                break;
            };

            match outcome {
                Ok(RemotePrepareOutcome::Prepared(bindings_by_task)) => {
                    let peer_plans = &grouped[&peer_id];
                    let (reservation, peer_prepared_plans) = match self.build_prepared_remote_plans(
                        peer_id,
                        peer_plans,
                        bindings_by_task,
                    ) {
                        Ok(prepared) => prepared,
                        Err(err) => {
                            if first_failure.is_none() {
                                first_failure = Some(RemotePrepareFailure::Execution(
                                    ExecutionError::Fatal(err),
                                ));
                            }
                            continue;
                        }
                    };

                    reservations.insert(peer_id, reservation);
                    prepared_plans.extend(peer_prepared_plans);
                    self.local_state
                        .remote_prepare_feedback
                        .clear_success(peer_id);
                }
                Ok(RemotePrepareOutcome::Rejected(rejection)) => {
                    if first_failure.is_none() {
                        first_failure = Some(RemotePrepareFailure::Rejection {
                            peer_id,
                            operation: "lease prepare",
                            rejection,
                        });
                    }
                }
                Err(err) => {
                    if matches!(&err, ExecutionError::Retry(_)) {
                        self.local_state
                            .remote_prepare_feedback
                            .record_retryable_failure(peer_id);
                    }
                    if first_failure.is_none() {
                        first_failure = Some(RemotePrepareFailure::Execution(err));
                    }
                }
            }
        }

        if let Some(failure) = first_failure {
            self.abort_remote_leases(&reservations).await;
            self.finish_remote_prepare_failure(failure).await?;
        }

        Ok((reservations, prepared_plans))
    }

    /// Prepares remote leases for one gang admission group on every target node.
    pub(super) async fn prepare_remote_lease_group(
        &self,
        group_id: Uuid,
        plans: &[RemoteStartPlan],
        ttl_ms: u64,
    ) -> Result<
        (
            HashMap<Uuid, RemoteGroupReservation>,
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
        crate::observability::metrics::record_remote_prepare_batch(
            "gang",
            grouped.len(),
            plans.len(),
        );

        let parallelism = remote_admission_parallelism();
        let batch_started = Instant::now();
        let mut first_failure = None;
        let mut peer_ids = grouped.keys().copied().collect::<Vec<_>>();
        peer_ids.sort_unstable();
        let mut pending_peers = peer_ids.into_iter();
        let mut inflight = FuturesUnordered::new();

        loop {
            while first_failure.is_none() && inflight.len() < parallelism {
                let Some(peer_id) = pending_peers.next() else {
                    break;
                };
                let peer_plans = &grouped[&peer_id];
                crate::observability::metrics::record_remote_prepare_peer("gang", peer_plans.len());
                inflight.push(self.prepare_group_remote_peer(
                    peer_id,
                    group_id,
                    ttl_ms,
                    peer_plans,
                    batch_started,
                ));
            }

            let Some((peer_id, outcome)) = inflight.next().await else {
                break;
            };

            match outcome {
                Ok(RemotePrepareOutcome::Prepared(bindings_by_task)) => {
                    let peer_plans = &grouped[&peer_id];
                    let group_reservation =
                        remote_group_reservation_from_bindings(&bindings_by_task);
                    let (_, peer_prepared_plans) = match self.build_prepared_remote_plans(
                        peer_id,
                        peer_plans,
                        bindings_by_task,
                    ) {
                        Ok(prepared) => prepared,
                        Err(err) => {
                            if first_failure.is_none() {
                                first_failure = Some(RemotePrepareFailure::Execution(
                                    ExecutionError::Fatal(err),
                                ));
                            }
                            continue;
                        }
                    };

                    reservations.insert(peer_id, group_reservation);
                    prepared_plans.extend(peer_prepared_plans);
                    self.local_state
                        .remote_prepare_feedback
                        .clear_success(peer_id);
                }
                Ok(RemotePrepareOutcome::Rejected(rejection)) => {
                    if first_failure.is_none() {
                        first_failure = Some(RemotePrepareFailure::Rejection {
                            peer_id,
                            operation: "group lease prepare",
                            rejection,
                        });
                    }
                }
                Err(err) => {
                    if matches!(&err, ExecutionError::Retry(_)) {
                        self.local_state
                            .remote_prepare_feedback
                            .record_retryable_failure(peer_id);
                    }
                    if first_failure.is_none() {
                        first_failure = Some(RemotePrepareFailure::Execution(err));
                    }
                }
            }
        }

        if let Some(failure) = first_failure {
            self.abort_remote_lease_groups(group_id, &reservations)
                .await;
            self.finish_remote_prepare_failure(failure).await?;
        }

        Ok((reservations, prepared_plans))
    }

    /// Prepares one target peer through the normal remote-admission RPC path.
    async fn prepare_incremental_remote_peer(
        &self,
        peer_id: Uuid,
        peer_plans: &[&RemoteStartPlan],
        batch_started: Instant,
    ) -> (Uuid, Result<RemotePrepareOutcome, ExecutionError>) {
        crate::observability::metrics::record_remote_prepare_queue_delay(
            "incremental",
            batch_started.elapsed(),
        );
        let started = Instant::now();
        let result = match self.remote_scheduler_client(peer_id).await {
            Ok(scheduler_client) => {
                self.send_prepare_leases_request(&scheduler_client, peer_plans)
                    .await
            }
            Err(err) => Err(ExecutionError::Retry(err)),
        };
        crate::observability::metrics::record_remote_prepare_latency(
            "incremental",
            remote_prepare_result_label(&result),
            started.elapsed(),
        );
        (peer_id, result)
    }

    /// Prepares one target peer through the gang-admission group RPC path.
    async fn prepare_group_remote_peer(
        &self,
        peer_id: Uuid,
        group_id: Uuid,
        ttl_ms: u64,
        peer_plans: &[&RemoteStartPlan],
        batch_started: Instant,
    ) -> (Uuid, Result<RemotePrepareOutcome, ExecutionError>) {
        crate::observability::metrics::record_remote_prepare_queue_delay(
            "gang",
            batch_started.elapsed(),
        );
        let started = Instant::now();
        let result = match self.remote_scheduler_client(peer_id).await {
            Ok(scheduler_client) => {
                self.send_prepare_lease_group_request(
                    &scheduler_client,
                    group_id,
                    ttl_ms,
                    peer_plans,
                )
                .await
            }
            Err(err) => Err(ExecutionError::Retry(err)),
        };
        crate::observability::metrics::record_remote_prepare_latency(
            "gang",
            remote_prepare_result_label(&result),
            started.elapsed(),
        );
        (peer_id, result)
    }

    /// Applies the first remote prepare failure after already-started peers have drained.
    async fn finish_remote_prepare_failure(
        &self,
        failure: RemotePrepareFailure,
    ) -> Result<(), ExecutionError> {
        match failure {
            RemotePrepareFailure::Execution(err) => Err(err),
            RemotePrepareFailure::Rejection {
                peer_id,
                operation,
                rejection,
            } => {
                let rejection_message =
                    remote_prepare_rejection_message(peer_id, operation, &rejection);
                if let Err(err) = self
                    .apply_remote_prepare_rejection(peer_id, rejection)
                    .await
                {
                    return Err(ExecutionError::Fatal(err));
                }
                Err(ExecutionError::Retry(anyhow::anyhow!(rejection_message)))
            }
        }
    }

    /// Opens the remote scheduler capability for a peer before sending reservation requests.
    async fn remote_scheduler_client(
        &self,
        peer_id: Uuid,
    ) -> Result<scheduler_rpc::Client, anyhow::Error> {
        let session = self.remote_session(peer_id).await?;
        session
            .clone()
            .get_scheduler_request()
            .send()
            .promise
            .await
            .with_context(|| format!("failed to request scheduler service from peer {peer_id}"))?
            .get()
            .with_context(|| format!("invalid scheduler response from peer {peer_id}"))?
            .get_scheduler()
            .with_context(|| format!("missing scheduler service from peer {peer_id}"))
    }

    /// Sends one remote prepare RPC and classifies transport failures separately from decode bugs.
    async fn send_prepare_leases_request(
        &self,
        scheduler_client: &scheduler_rpc::Client,
        peer_plans: &[&RemoteStartPlan],
    ) -> Result<RemotePrepareOutcome, ExecutionError> {
        let mut prepare_req = scheduler_client.prepare_leases_request();
        {
            let mut inner = prepare_req.get().init_request();
            inner.set_coordinator_node_id(self.local_node_id.as_bytes());
            inner.set_ttl_ms(DEFAULT_PREPARED_LEASE_TTL_MS);
            let mut intents_builder = inner.reborrow().init_intents(peer_plans.len() as u32);
            for (idx, plan) in peer_plans.iter().enumerate() {
                let mut entry = intents_builder.reborrow().get(idx as u32);
                entry.set_task_id(plan.id.as_bytes());
                entry.set_cpu_millis(plan.cpu_millis);
                entry.set_memory_bytes(plan.memory_bytes);
                entry.set_gpu_count(plan.gpu_count);
            }
        }

        let response = prepare_req
            .send()
            .promise
            .await
            .map_err(|err| ExecutionError::Retry(anyhow::anyhow!(err.to_string())))?;
        let result = response
            .get()
            .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
        let response = result
            .get_response()
            .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
        parse_prepare_response(response).map_err(ExecutionError::Fatal)
    }

    /// Sends one remote group prepare RPC and decodes the target's admission result.
    async fn send_prepare_lease_group_request(
        &self,
        scheduler_client: &scheduler_rpc::Client,
        group_id: Uuid,
        ttl_ms: u64,
        peer_plans: &[&RemoteStartPlan],
    ) -> Result<RemotePrepareOutcome, ExecutionError> {
        let mut prepare_req = scheduler_client.prepare_lease_group_request();
        {
            let mut inner = prepare_req.get().init_request();
            inner.set_group_id(group_id.as_bytes());
            inner.set_coordinator_node_id(self.local_node_id.as_bytes());
            inner.set_ttl_ms(ttl_ms);
            let mut intents_builder = inner.reborrow().init_intents(peer_plans.len() as u32);
            for (idx, plan) in peer_plans.iter().enumerate() {
                let mut entry = intents_builder.reborrow().get(idx as u32);
                entry.set_task_id(plan.id.as_bytes());
                entry.set_cpu_millis(plan.cpu_millis);
                entry.set_memory_bytes(plan.memory_bytes);
                entry.set_gpu_count(plan.gpu_count);
            }
        }

        let response = prepare_req
            .send()
            .promise
            .await
            .map_err(|err| ExecutionError::Retry(anyhow::anyhow!(err.to_string())))?;
        let result = response
            .get()
            .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
        let response = result
            .get_response()
            .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
        parse_prepare_response(response).map_err(ExecutionError::Fatal)
    }

    /// Validates prepared bindings and converts them into launch plans plus rollback metadata.
    fn build_prepared_remote_plans(
        &self,
        peer_id: Uuid,
        peer_plans: &[&RemoteStartPlan],
        mut bindings_by_task: HashMap<Uuid, PreparedRemoteLeaseBinding>,
    ) -> Result<(RemoteReservation, Vec<PreparedRemoteStartPlan>), anyhow::Error> {
        let mut lease_reservations = Vec::new();
        let mut prepared_plans = Vec::new();

        for plan in peer_plans {
            let Some(binding) = bindings_by_task.remove(&plan.id) else {
                return Err(anyhow::anyhow!(
                    "missing prepared binding for remote task {} on peer {}",
                    plan.id,
                    peer_id
                ));
            };

            if binding.slot_ids.is_empty() {
                return Err(anyhow::anyhow!(
                    "prepared remote task {} on peer {} without slots",
                    plan.id,
                    peer_id
                ));
            }

            if binding.gpu_device_ids.len() < plan.gpu_count as usize {
                return Err(anyhow::anyhow!(
                    "prepared remote task {} on peer {} returned only {} GPU(s) for request of {}",
                    plan.id,
                    peer_id,
                    binding.gpu_device_ids.len(),
                    plan.gpu_count
                ));
            }

            lease_reservations.push(RemoteLeaseReservation {
                lease_id: binding.lease_id,
                task_id: plan.id,
            });
            prepared_plans.push(PreparedRemoteStartPlan {
                index: plan.index,
                id: plan.id,
                lease_id: binding.lease_id,
                lease_coordinator_node_id: self.local_node_id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                execution_platform: plan.execution_platform,
                isolation_mode: plan.isolation_mode,
                isolation_profile: plan.isolation_profile.clone(),
                command: plan.command.clone(),
                tty: plan.tty,
                cpu_millis: plan.cpu_millis,
                memory_bytes: plan.memory_bytes,
                gpu_count: plan.gpu_count,
                slot_ids: binding.slot_ids,
                gpu_device_ids: binding.gpu_device_ids,
                peer_id: plan.peer_id,
                restart_policy: plan.restart_policy.clone(),
                termination_grace_period_secs: plan.termination_grace_period_secs,
                pre_stop_command: plan.pre_stop_command.clone(),
                liveness: plan.liveness.clone(),
                env: plan.env.clone(),
                secret_files: plan.secret_files.clone(),
                volumes: plan.volumes.clone(),
                networks: plan.networks.clone(),
                ports: plan.ports.clone(),
                owner: plan.owner.clone(),
            });
        }

        if !bindings_by_task.is_empty() {
            return Err(anyhow::anyhow!(
                "peer {peer_id} returned unexpected prepared bindings"
            ));
        }

        Ok((
            RemoteReservation {
                leases: lease_reservations,
            },
            prepared_plans,
        ))
    }

    /// Aborts prepared remote leases accumulated during previous stages.
    pub(super) async fn abort_remote_leases(
        &self,
        reservations: &HashMap<Uuid, RemoteReservation>,
    ) {
        let lease_count: usize = reservations
            .values()
            .map(|reservation| reservation.leases.len())
            .sum();
        crate::observability::metrics::record_remote_prepare_abort(
            "incremental",
            reservations.len(),
            lease_count,
        );

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

    /// Commits every prepared remote group lease on its owning target node.
    pub(super) async fn commit_remote_lease_groups(
        &self,
        group_id: Uuid,
        reservations: &HashMap<Uuid, RemoteGroupReservation>,
    ) -> Result<(), ExecutionError> {
        let mut peer_ids = reservations.keys().copied().collect::<Vec<_>>();
        peer_ids.sort_unstable();
        for peer_id in peer_ids {
            let reservation = &reservations[&peer_id];
            let scheduler_client = self
                .remote_scheduler_client(peer_id)
                .await
                .map_err(ExecutionError::Retry)?;

            let mut commit_req = scheduler_client.commit_lease_group_request();
            {
                let mut inner = commit_req.get().init_request();
                inner.set_group_id(group_id.as_bytes());
                inner.set_coordinator_node_id(self.local_node_id.as_bytes());
                write_prepared_leases_for_request(
                    inner
                        .reborrow()
                        .init_prepared(reservation.leases.len() as u32),
                    &reservation.leases,
                );
            }

            let response = commit_req
                .send()
                .promise
                .await
                .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
            response
                .get()
                .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
        }

        Ok(())
    }

    /// Aborts every still-prepared remote group lease on its owning target node.
    pub(super) async fn abort_remote_lease_groups(
        &self,
        group_id: Uuid,
        reservations: &HashMap<Uuid, RemoteGroupReservation>,
    ) {
        let lease_count: usize = reservations
            .values()
            .map(|reservation| reservation.leases.len())
            .sum();
        crate::observability::metrics::record_remote_prepare_abort(
            "gang",
            reservations.len(),
            lease_count,
        );

        let mut peer_ids = reservations.keys().copied().collect::<Vec<_>>();
        peer_ids.sort_unstable();
        for peer_id in peer_ids {
            let scheduler_client = match self.remote_scheduler_client(peer_id).await {
                Ok(client) => client,
                Err(err) => {
                    warn!(
                        target: "task",
                        "failed to access scheduler for peer {peer_id} while aborting lease group {group_id}: {err}"
                    );
                    continue;
                }
            };

            let mut abort_req = scheduler_client.abort_lease_group_request();
            {
                let mut inner = abort_req.get().init_request();
                inner.set_group_id(group_id.as_bytes());
                inner.set_coordinator_node_id(self.local_node_id.as_bytes());
            }

            if let Err(err) = abort_req.send().promise.await {
                warn!(
                    target: "task",
                    "failed to abort lease group {group_id} on peer {peer_id}: {err}"
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

    /// Opens the remote workload capability for target-side assignment and stop control.
    async fn remote_workload_client(
        &self,
        peer_id: Uuid,
    ) -> Result<workload_rpc::Client, anyhow::Error> {
        let session = self
            .remote_session(peer_id)
            .await
            .context(format!("no active session for peer {peer_id}"))?;
        session
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
            .context(format!("missing workload service for peer {peer_id}"))
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
        let task_client = self.remote_workload_client(peer_id).await?;

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
        self.materialize_remote_specs_with_admission(plans, None, WorkloadAdmissionState::None)
            .await
    }

    /// Creates remote task specs with the requested admission barrier metadata.
    pub(super) async fn materialize_remote_specs_with_admission(
        &self,
        plans: &[PreparedRemoteStartPlan],
        admission_group_id: Option<Uuid>,
        admission_state: WorkloadAdmissionState,
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
                execution_platform: plan.execution_platform,
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
                ports: plan.ports.clone(),
                owner: plan.owner.clone(),
                lease_id: Some(plan.lease_id),
                lease_coordinator_node_id: Some(plan.lease_coordinator_node_id),
                admission_group_id,
                admission_state,
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

        self.deliver_remote_assignment_batches(&results).await;

        Ok(results)
    }

    /// Best-effort delivers owner-built assignment rows to their target nodes in per-node batches.
    ///
    /// The owner has already persisted the rows locally before this runs, so failed direct delivery
    /// leaves MST workload sync as the repair path instead of reintroducing global assignment
    /// gossip.
    async fn deliver_remote_assignment_batches(&self, results: &[(usize, WorkloadSpec)]) {
        let mut grouped: HashMap<Uuid, Vec<WorkloadSpec>> = HashMap::new();
        for (_, spec) in results {
            if spec.node_id == self.local_node_id {
                continue;
            }
            grouped.entry(spec.node_id).or_default().push(spec.clone());
        }

        let assignment_count: usize = grouped.values().map(Vec::len).sum();
        crate::observability::metrics::record_remote_assignment_batch(
            grouped.len(),
            assignment_count,
        );

        let parallelism = remote_assignment_parallelism();
        let batch_started = Instant::now();
        let mut peer_ids = grouped.keys().copied().collect::<Vec<_>>();
        peer_ids.sort_unstable();
        let mut pending_peers = peer_ids.into_iter();
        let mut inflight = FuturesUnordered::new();

        loop {
            while inflight.len() < parallelism {
                let Some(peer_id) = pending_peers.next() else {
                    break;
                };
                let specs = &grouped[&peer_id];
                crate::observability::metrics::record_remote_assignment_peer(specs.len());
                inflight.push(self.deliver_remote_assignment_peer(peer_id, specs, batch_started));
            }

            let Some((peer_id, assignments, result)) = inflight.next().await else {
                break;
            };
            if let Err(err) = result {
                warn!(
                    target: "task",
                    peer = %peer_id,
                    assignments,
                    "failed direct assignment delivery; workload MST sync will reconcile these persisted assignment rows if the target misses them: {err:#}"
                );
                // The owner already persisted these assignment rows locally.
                // Prioritize workload sync with the target so the target can
                // pull the missing rows through MST anti-entropy instead of
                // waiting for unrelated peer rotation.
                self.prioritize_workload_sync_with_peer(peer_id);
            }
        }
    }

    /// Sends one target-node assignment batch while recording bounded-window timing.
    async fn deliver_remote_assignment_peer(
        &self,
        peer_id: Uuid,
        specs: &[WorkloadSpec],
        batch_started: Instant,
    ) -> (Uuid, usize, Result<(), anyhow::Error>) {
        crate::observability::metrics::record_remote_assignment_queue_delay(
            batch_started.elapsed(),
        );
        let started = Instant::now();
        let result = self.deliver_remote_assignment_batch(peer_id, specs).await;
        crate::observability::metrics::record_remote_assignment_latency(
            remote_assignment_result_label(&result),
            started.elapsed(),
        );
        (peer_id, specs.len(), result)
    }

    /// Sends one target-node assignment batch through the workload RPC hot path.
    async fn deliver_remote_assignment_batch(
        &self,
        peer_id: Uuid,
        specs: &[WorkloadSpec],
    ) -> Result<(), anyhow::Error> {
        if specs.is_empty() {
            return Ok(());
        }

        let workload_client = self.remote_workload_client(peer_id).await?;
        let mut apply_req = workload_client.apply_assignments_request();
        {
            let mut request = apply_req.get().init_request();
            request.set_coordinator_node_id(self.local_node_id.as_bytes());
            request.set_target_node_id(peer_id.as_bytes());
            let mut spec_list = request.reborrow().init_specs(specs.len() as u32);
            for (idx, spec) in specs.iter().enumerate() {
                write_spec(spec_list.reborrow().get(idx as u32), spec);
            }
        }

        let response = apply_req
            .send()
            .promise
            .await
            .with_context(|| format!("assignment delivery RPC failed for peer {peer_id}"))?;
        let applied = response
            .get()
            .with_context(|| format!("invalid assignment delivery response from peer {peer_id}"))?
            .get_response()
            .with_context(|| format!("missing assignment delivery response from peer {peer_id}"))?
            .get_applied();
        if applied != specs.len() as u64 {
            return Err(anyhow::anyhow!(
                "peer {peer_id} accepted {applied} assignment(s), expected {}",
                specs.len()
            ));
        }

        Ok(())
    }
}
