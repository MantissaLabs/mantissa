use crate::gossip::Message;
use crate::registry::Registry;
use crate::scheduler::summary::SchedulerSlotState;
use crate::scheduler::{
    Scheduler, SchedulerError, SlotCapacity, SlotId, SlotReservationRequest, SlotState,
};
use crate::store::workload_store::WorkloadStore;
use crate::workload::container::ContainerState;
use crate::workload::docker::{ContainerError, ContainerManager};
use crate::workload::service::read_spec;
use crate::workload::types::{WorkloadEvent, WorkloadSpec, WorkloadStateFilter, WorkloadValue};
use anyhow::Context;
use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, warn};
use uuid::Uuid;

use protocol::server::cluster_session;
use rand::rng;
use rand::seq::SliceRandom;

#[derive(Clone)]
pub struct WorkloadManager {
    store: WorkloadStore,
    tx: Sender<Message>,
    rx: Receiver<Message>,
    seen_ids: Arc<AsyncMutex<HashSet<Uuid>>>,
    local_node_id: Uuid,
    local_node_name: String,
    scheduler: Rc<Scheduler>,
    container_manager: Arc<dyn ContainerManager + Send + Sync>,
    local_containers: Arc<AsyncMutex<HashMap<Uuid, String>>>,
    registry: Registry,
}

#[derive(Clone)]
pub struct ContainerStartRequest {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub id: Option<Uuid>,
    pub slot_id: Option<SlotId>,
}

struct BatchStartPlan {
    id: Uuid,
    name: String,
    image: String,
    command: Vec<String>,
    slot_id: SlotId,
    slot_capacity: SlotCapacity,
    container_name: String,
    container_id: Option<String>,
    created_at: DateTime<Utc>,
    index: usize,
    preassigned: bool,
}

#[derive(Clone)]
struct StartIntent {
    index: usize,
    id: Uuid,
    name: String,
    image: String,
    command: Vec<String>,
    cpu_millis: u64,
    memory_bytes: u64,
    preassigned_slot: Option<SlotId>,
}

#[derive(Clone)]
struct SlotChoice {
    slot_id: SlotId,
    capacity: SlotCapacity,
}

enum CandidateKind {
    Local,
    Remote { peer_id: Uuid },
}

struct NodeCandidate {
    kind: CandidateKind,
    version: u64,
    slots: Vec<SlotChoice>,
}

struct RemoteStartPlan {
    index: usize,
    id: Uuid,
    name: String,
    image: String,
    command: Vec<String>,
    cpu_millis: u64,
    memory_bytes: u64,
    slot_id: SlotId,
    peer_id: Uuid,
    scheduler_version: u64,
}

struct Assignment {
    local_version: u64,
    local: Vec<BatchStartPlan>,
    remote: Vec<RemoteStartPlan>,
}

enum ExecutionError {
    Retry(anyhow::Error),
    Fatal(anyhow::Error),
}

struct RemoteReservation {
    slots: Vec<SlotId>,
    version: u64,
}

fn is_scheduler_retryable_message(message: &str) -> bool {
    message.contains("snapshot mismatch")
        || message.contains("slots unavailable")
        || message.contains("unknown slots")
}

impl NodeCandidate {
    fn take_slot(&mut self, cpu_millis: u64, memory_bytes: u64) -> Option<SlotChoice> {
        if let Some(pos) = self.slots.iter().position(|slot| {
            slot.capacity.cpu_millis >= cpu_millis && slot.capacity.memory_bytes >= memory_bytes
        }) {
            Some(self.slots.remove(pos))
        } else {
            None
        }
    }
}

impl WorkloadManager {
    fn build_start_intents(requests: Vec<ContainerStartRequest>) -> Vec<StartIntent> {
        requests
            .into_iter()
            .enumerate()
            .map(|(index, request)| StartIntent {
                index,
                id: request.id.unwrap_or_else(Uuid::new_v4),
                name: request.name,
                image: request.image,
                command: request.command,
                cpu_millis: request.cpu_millis,
                memory_bytes: request.memory_bytes,
                preassigned_slot: request.slot_id,
            })
            .collect()
    }

    pub fn new(
        store: WorkloadStore,
        tx: Sender<Message>,
        rx: Receiver<Message>,
        local_node_id: Uuid,
        local_node_name: impl Into<String>,
        scheduler: Rc<Scheduler>,
        container_manager: Arc<dyn ContainerManager + Send + Sync>,
        registry: Registry,
    ) -> Self {
        Self {
            store,
            tx,
            rx,
            seen_ids: Arc::new(AsyncMutex::new(HashSet::new())),
            local_node_id,
            local_node_name: local_node_name.into(),
            scheduler,
            container_manager,
            local_containers: Arc::new(AsyncMutex::new(HashMap::new())),
            registry,
        }
    }

    async fn release_slot(&self, slot_id: SlotId) -> Result<(), anyhow::Error> {
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

    pub async fn start_container(
        &self,
        name: impl Into<String>,
        image: impl Into<String>,
        command: Vec<String>,
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Result<WorkloadSpec, anyhow::Error> {
        let request = ContainerStartRequest {
            name: name.into(),
            image: image.into(),
            command,
            cpu_millis,
            memory_bytes,
            id: None,
            slot_id: None,
        };

        let mut specs = self.start_containers_batch(vec![request]).await?;
        Ok(specs
            .pop()
            .expect("batch start with single request should yield one spec"))
    }

    pub async fn start_containers_batch(
        &self,
        requests: Vec<ContainerStartRequest>,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let intents = Self::build_start_intents(requests);

        const MAX_ATTEMPTS: usize = 5;
        let mut attempt = 0usize;

        while attempt < MAX_ATTEMPTS {
            attempt += 1;

            let assignment = match self.compute_assignment(&intents).await {
                Ok(plan) => plan,
                Err(err) => return Err(err.context("failed to compute scheduling plan")),
            };

            let local_version = assignment.local_version;
            let mut local_plans = assignment.local;
            let remote_plans = assignment.remote;

            let mut reserved_local_slots: Option<Vec<SlotId>> = None;
            let mut reserved_remote: HashMap<Uuid, RemoteReservation> = HashMap::new();

            match self.reserve_local_slots(&local_plans, local_version).await {
                Ok(slots) => {
                    if !slots.is_empty() {
                        reserved_local_slots = Some(slots);
                    }
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "workload",
                        "local reservation conflicted on attempt {attempt}: {err}"
                    );
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => return Err(err),
            }

            match self.reserve_remote_slots(&remote_plans).await {
                Ok(map) => {
                    reserved_remote = map;
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "workload",
                        "remote reservation conflicted on attempt {attempt}: {err}"
                    );
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    reserved_remote.clear();
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    reserved_remote.clear();
                    return Err(err);
                }
            }

            let remote_specs = match self.execute_remote_plans(&remote_plans).await {
                Ok(specs) => {
                    reserved_remote.clear();
                    specs
                }
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "workload",
                        "remote start conflicted on attempt {attempt}: {err}"
                    );
                    self.release_remote_slots(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    continue;
                }
                Err(ExecutionError::Fatal(err)) => {
                    self.release_remote_slots(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    return Err(err);
                }
            };

            match self.start_local_containers(&mut local_plans).await {
                Ok(local_specs) => {
                    let mut ordered: Vec<Option<WorkloadSpec>> = vec![None; intents.len()];

                    for (idx, spec) in remote_specs.into_iter().chain(local_specs.into_iter()) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<WorkloadSpec> = ordered
                        .into_iter()
                        .map(|spec| spec.expect("missing workload spec after execution"))
                        .collect();

                    self.broadcast_remote_specs(&specs).await;

                    return Ok(specs);
                }
                Err(err) => {
                    debug!(
                        target: "workload",
                        "local execution failed; attempting remote cleanup: {err}"
                    );
                    self.cleanup_remote_specs_from_specs(&remote_specs).await;
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    return Err(err);
                }
            }
        }

        Err(anyhow::anyhow!(
            "failed to schedule workloads after {MAX_ATTEMPTS} attempts"
        ))
    }

    async fn compute_assignment(
        &self,
        intents: &[StartIntent],
    ) -> Result<Assignment, anyhow::Error> {
        if intents.is_empty() {
            return Ok(Assignment {
                local_version: 0,
                local: Vec::new(),
                remote: Vec::new(),
            });
        }

        let snapshot = self
            .scheduler
            .snapshot()
            .await
            .ok_or_else(|| anyhow::anyhow!("scheduler snapshot unavailable"))?;

        let local_version = snapshot.version;

        let mut slot_lookup = HashMap::new();
        let mut available_local_slots = Vec::new();
        for slot in snapshot.slots.iter() {
            slot_lookup.insert(slot.slot_id, slot.clone());
            if matches!(slot.state, SlotState::Free) {
                available_local_slots.push(SlotChoice {
                    slot_id: slot.slot_id,
                    capacity: slot.capacity,
                });
            }
        }

        let mut local_plans = Vec::new();
        let mut remote_plans = Vec::new();
        let mut assigned = vec![false; intents.len()];
        let mut reserved_local_slots = HashSet::new();

        for intent in intents.iter() {
            if let Some(slot_id) = intent.preassigned_slot {
                let slot = slot_lookup
                    .get(&slot_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown preassigned slot {slot_id}"))?;

                if slot.capacity.cpu_millis < intent.cpu_millis
                    || slot.capacity.memory_bytes < intent.memory_bytes
                {
                    return Err(anyhow::anyhow!(
                        "preassigned slot {slot_id} cannot satisfy requested capacity"
                    ));
                }

                if let SlotState::Reserved(reservation) = &slot.state {
                    if reservation.owner != self.local_node_id {
                        return Err(anyhow::anyhow!(
                            "preassigned slot {slot_id} owned by different node"
                        ));
                    }

                    if let Some(workload_id) = reservation.workload_id {
                        if workload_id != intent.id {
                            return Err(anyhow::anyhow!(
                                "preassigned slot {slot_id} reserved for workload {workload_id}"
                            ));
                        }
                    }
                }

                reserved_local_slots.insert(slot_id);
                assigned[intent.index] = true;

                local_plans.push(BatchStartPlan {
                    id: intent.id,
                    name: intent.name.clone(),
                    image: intent.image.clone(),
                    command: intent.command.clone(),
                    slot_id,
                    slot_capacity: slot.capacity,
                    container_name: String::new(),
                    container_id: None,
                    created_at: Utc::now(),
                    index: intent.index,
                    preassigned: true,
                });
            }
        }

        if !reserved_local_slots.is_empty() {
            available_local_slots.retain(|slot| !reserved_local_slots.contains(&slot.slot_id));
        }

        let mut nodes = Vec::new();
        nodes.push(NodeCandidate {
            kind: CandidateKind::Local,
            version: local_version,
            slots: available_local_slots,
        });

        let mut remote_candidates = Vec::new();
        let peers = self.registry.known_peers()?;
        for peer_id in peers {
            if peer_id == self.local_node_id {
                continue;
            }

            let summary = match self.scheduler.fetch_remote_summary(peer_id, true).await {
                Ok(summary) => summary,
                Err(err) => {
                    debug!(
                        target: "workload",
                        "scheduler summary fetch failed for peer {peer_id}: {err}"
                    );
                    continue;
                }
            };

            let mut slots = Vec::new();
            for detail in summary.details.iter() {
                if !matches!(detail.state, SchedulerSlotState::Free) {
                    continue;
                }

                slots.push(SlotChoice {
                    slot_id: detail.slot_id,
                    capacity: SlotCapacity::new(detail.cpu_millis, detail.memory_bytes),
                });
            }

            if slots.is_empty() {
                continue;
            }

            remote_candidates.push(NodeCandidate {
                kind: CandidateKind::Remote { peer_id },
                version: summary.version,
                slots,
            });
        }

        let mut rng = rng();
        remote_candidates.shuffle(&mut rng);
        nodes.extend(remote_candidates);

        if nodes.is_empty() {
            if local_plans.len() == intents.len() {
                local_plans.sort_by_key(|plan| plan.index);
                return Ok(Assignment {
                    local_version,
                    local: local_plans,
                    remote: remote_plans,
                });
            }

            return Err(anyhow::anyhow!(
                "scheduler reservation failed: no available capacity across cluster"
            ));
        }

        let node_count = nodes.len();
        let mut cursor = 0usize;

        for intent in intents.iter() {
            if assigned[intent.index] {
                continue;
            }

            // Walk the candidate ring in round-robin order so replicas alternate across nodes.
            let mut allocated = None;
            let mut checked = 0usize;
            while checked < node_count {
                let idx = (cursor + checked) % node_count;
                if let Some(slot) = nodes[idx].take_slot(intent.cpu_millis, intent.memory_bytes) {
                    allocated = Some((idx, slot));
                    cursor = (idx + 1) % node_count;
                    break;
                }
                checked += 1;
            }

            let Some((node_idx, slot)) = allocated else {
                return Err(anyhow::anyhow!(
                    "scheduler reservation failed: insufficient capacity for batch"
                ));
            };

            match &nodes[node_idx].kind {
                CandidateKind::Local => {
                    local_plans.push(BatchStartPlan {
                        id: intent.id,
                        name: intent.name.clone(),
                        image: intent.image.clone(),
                        command: intent.command.clone(),
                        slot_id: slot.slot_id,
                        slot_capacity: slot.capacity,
                        container_name: String::new(),
                        container_id: None,
                        created_at: Utc::now(),
                        index: intent.index,
                        preassigned: false,
                    });
                }
                CandidateKind::Remote { peer_id } => {
                    remote_plans.push(RemoteStartPlan {
                        index: intent.index,
                        id: intent.id,
                        name: intent.name.clone(),
                        image: intent.image.clone(),
                        command: intent.command.clone(),
                        cpu_millis: intent.cpu_millis,
                        memory_bytes: intent.memory_bytes,
                        slot_id: slot.slot_id,
                        peer_id: *peer_id,
                        scheduler_version: nodes[node_idx].version,
                    });
                }
            }

            assigned[intent.index] = true;
        }

        local_plans.sort_by_key(|plan| plan.index);
        remote_plans.sort_by_key(|plan| plan.index);

        Ok(Assignment {
            local_version,
            local: local_plans,
            remote: remote_plans,
        })
    }

    async fn start_local_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<Vec<(usize, WorkloadSpec)>, anyhow::Error> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        for plan in plans.iter_mut() {
            plan.container_name = format!("mantissa-{}", plan.id);
        }

        if let Err(err) = self.launch_batch_containers(plans).await {
            self.cleanup_batch(plans).await;
            return Err(err);
        }

        match self.commit_batch(plans).await {
            Ok(specs) => {
                let ordered = plans
                    .iter()
                    .zip(specs.into_iter())
                    .map(|(plan, spec)| (plan.index, spec))
                    .collect();
                Ok(ordered)
            }
            Err(err) => {
                self.cleanup_batch(plans).await;
                Err(err)
            }
        }
    }

    async fn reserve_local_slots(
        &self,
        plans: &[BatchStartPlan],
        expected_version: u64,
    ) -> Result<Vec<SlotId>, ExecutionError> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let mut requests = Vec::with_capacity(plans.len());
        let mut newly_reserved = Vec::with_capacity(plans.len());

        for plan in plans {
            if plan.preassigned {
                continue;
            }

            requests.push(SlotReservationRequest {
                slot_id: plan.slot_id,
                owner: self.local_node_id,
                workload_id: Some(plan.id),
            });
            newly_reserved.push(plan.slot_id);
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

    async fn release_local_slots(&self, slots: &[SlotId]) {
        for slot_id in slots {
            if let Err(err) = self.release_slot(*slot_id).await {
                warn!(
                    target: "workload",
                    "failed to release local slot {slot_id}: {err}"
                );
            }
        }
    }

    async fn reserve_remote_slots(
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
                let mut intents_builder = inner.reborrow().init_intents(peer_plans.len() as u32);
                for (idx, plan) in peer_plans.iter().enumerate() {
                    let mut entry = intents_builder.reborrow().get(idx as u32);
                    entry.set_slot_id(plan.slot_id);
                    entry.set_owner(plan.peer_id.as_bytes());
                    entry.set_workload_id(plan.id.as_bytes());
                }
            }

            match reserve_req.send().promise.await {
                Ok(resp) => match resp.get() {
                    Ok(result) => match result.get_response() {
                        Ok(response) => {
                            let slots: Vec<SlotId> =
                                peer_plans.iter().map(|plan| plan.slot_id).collect();
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

    async fn release_remote_slots(&self, reservations: &HashMap<Uuid, RemoteReservation>) {
        for (peer_id, reservation) in reservations {
            let session = match self.remote_session(*peer_id).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(
                        target: "workload",
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
                                target: "workload",
                                "failed to access scheduler for peer {peer_id} while releasing slots: {err}"
                            );
                            continue;
                        }
                    },
                    Err(err) => {
                        warn!(
                            target: "workload",
                            "failed to obtain scheduler response for peer {peer_id}: {err}"
                        );
                        continue;
                    }
                },
                Err(err) => {
                    warn!(
                        target: "workload",
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
                    target: "workload",
                    "failed to release slots on peer {peer_id}: {err}"
                );
            }
        }
    }

    async fn remote_session(
        &self,
        peer_id: Uuid,
    ) -> Result<cluster_session::Client, anyhow::Error> {
        self.registry
            .session_for_peer(peer_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("no active session for peer {peer_id}"))
    }

    async fn cleanup_remote_specs_from_specs(&self, specs: &[(usize, WorkloadSpec)]) {
        if specs.is_empty() {
            return;
        }

        let mut by_peer: HashMap<Uuid, Vec<&WorkloadSpec>> = HashMap::new();
        for (_, spec) in specs.iter() {
            if spec.node_id == self.local_node_id {
                continue;
            }
            by_peer.entry(spec.node_id).or_default().push(spec);
        }

        for (peer_id, workloads) in by_peer.into_iter() {
            let session = match self.remote_session(peer_id).await {
                Ok(session) => session,
                Err(err) => {
                    warn!(
                        target: "workload",
                        "failed to reopen session with peer {peer_id} during cleanup: {err}"
                    );
                    continue;
                }
            };

            let workload_client = match session.get_workload_request().send().promise.await {
                Ok(resp) => match resp.get() {
                    Ok(result) => match result.get_workload() {
                        Ok(client) => client,
                        Err(err) => {
                            warn!(
                                target: "workload",
                                "failed to access workload capability for peer {peer_id}: {err}"
                            );
                            continue;
                        }
                    },
                    Err(err) => {
                        warn!(
                            target: "workload",
                            "failed to complete workload capability request for peer {peer_id}: {err}"
                        );
                        continue;
                    }
                },
                Err(err) => {
                    warn!(
                        target: "workload",
                        "failed to send workload capability request to peer {peer_id}: {err}"
                    );
                    continue;
                }
            };

            for spec in workloads {
                // Remote stop is best-effort; failures are logged but do not abort cleanup.
                let mut stop_req = workload_client.stop_request();
                {
                    let mut inner = stop_req.get().init_request();
                    inner.set_id(spec.id.as_bytes());
                }

                match stop_req.send().promise.await {
                    Ok(response) => {
                        if let Err(err) = response.get() {
                            warn!(
                                target: "workload",
                                "failed to stop remote workload {} on peer {peer_id}: {err}",
                                spec.id
                            );
                        }
                    }
                    Err(err) => {
                        warn!(
                            target: "workload",
                            "failed to stop remote workload {} on peer {peer_id}: {err}",
                            spec.id
                        );
                    }
                }
            }
        }
    }

    async fn execute_remote_plans(
        &self,
        plans: &[RemoteStartPlan],
    ) -> Result<Vec<(usize, WorkloadSpec)>, ExecutionError> {
        if plans.is_empty() {
            return Ok(Vec::new());
        }

        let mut by_peer: HashMap<Uuid, Vec<&RemoteStartPlan>> = HashMap::new();
        for plan in plans {
            by_peer.entry(plan.peer_id).or_default().push(plan);
        }

        let mut results = Vec::new();

        for (peer_id, peer_plans) in by_peer.into_iter() {
            let session = self
                .remote_session(peer_id)
                .await
                .map_err(|err| ExecutionError::Retry(err))?;

            let workload_client = session
                .get_workload_request()
                .send()
                .promise
                .await
                .map_err(|err| ExecutionError::Retry(anyhow::anyhow!(err.to_string())))?
                .get()
                .map_err(|err| ExecutionError::Retry(anyhow::anyhow!(err.to_string())))?
                .get_workload()
                .map_err(|err| ExecutionError::Retry(anyhow::anyhow!(err.to_string())))?;

            let mut start_req = workload_client.start_many_request();
            {
                let mut requests_builder = start_req.get().init_requests(peer_plans.len() as u32);
                for (idx, plan) in peer_plans.iter().enumerate() {
                    let mut entry = requests_builder.reborrow().get(idx as u32);
                    entry.set_name(&plan.name);
                    entry.set_image(&plan.image);
                    entry.set_cpu_millis(plan.cpu_millis);
                    entry.set_memory_bytes(plan.memory_bytes);
                    let encoded_slot_id = plan
                        .slot_id
                        .checked_add(1)
                        .expect("slot id overflow while encoding reservation");
                    entry.set_slot_id(encoded_slot_id);
                    entry.set_workload_id(plan.id.as_bytes());

                    let mut cmd_builder = entry.reborrow().init_command(plan.command.len() as u32);
                    for (arg_idx, arg) in plan.command.iter().enumerate() {
                        cmd_builder.set(arg_idx as u32, arg);
                    }
                }
            }

            let response = match start_req.send().promise.await {
                Ok(resp) => resp,
                Err(err) => {
                    let message = err.to_string();
                    if message.contains("scheduler reservation failed")
                        || message.contains("slots unavailable")
                    {
                        return Err(ExecutionError::Retry(anyhow::anyhow!(message)));
                    }

                    return Err(ExecutionError::Fatal(anyhow::anyhow!(message)));
                }
            };

            let reader = response
                .get()
                .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
            let specs_reader = reader
                .get_specs()
                .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;

            if specs_reader.len() as usize != peer_plans.len() {
                return Err(ExecutionError::Fatal(anyhow::anyhow!(
                    "remote peer {peer_id} returned {} specs but {} plans were sent",
                    specs_reader.len(),
                    peer_plans.len()
                )));
            }

            for (plan, spec_reader) in peer_plans.iter().zip(specs_reader.iter()) {
                let spec = read_spec(spec_reader)
                    .map_err(|err| ExecutionError::Fatal(anyhow::anyhow!(err.to_string())))?;
                results.push((plan.index, spec));
            }
        }

        Ok(results)
    }

    async fn launch_batch_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<(), anyhow::Error> {
        for plan in plans.iter_mut() {
            self.container_manager
                .pull_image(&plan.image)
                .await
                .with_context(|| format!("docker pull failed for image {}", plan.image))?;

            let container_id = self
                .container_manager
                .create_container(
                    &plan.container_name,
                    &plan.image,
                    if plan.command.is_empty() {
                        None
                    } else {
                        Some(plan.command.clone())
                    },
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .with_context(|| format!("docker create failed for workload {}", plan.name))?;

            plan.container_id = Some(container_id.clone());

            self.container_manager
                .start_container(&container_id)
                .await
                .with_context(|| format!("docker start failed for workload {}", plan.name))?;

            plan.created_at = Utc::now();
        }

        Ok(())
    }

    async fn commit_batch(
        &self,
        plans: &[BatchStartPlan],
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let mut specs = Vec::with_capacity(plans.len());
        let mut persisted: Vec<WorkloadSpec> = Vec::new();

        for plan in plans {
            let spec = WorkloadSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Running,
                created_at: plan.created_at.to_rfc3339(),
                command: plan.command.clone(),
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_id: Some(plan.slot_id),
                cpu_millis: plan.slot_capacity.cpu_millis,
                memory_bytes: plan.slot_capacity.memory_bytes,
            };

            if let Err(err) = self.persist_spec(&spec).await {
                for rollback in &persisted {
                    let _ = self.remove_spec(rollback.id).await;
                }
                return Err(err.context(format!("failed to persist workload spec {}", spec.name)));
            }

            persisted.push(spec.clone());
            specs.push(spec);
        }

        for spec in &specs {
            if let Err(err) = self
                .enqueue_gossip(WorkloadEvent::Upsert(spec.clone()))
                .await
            {
                warn!(
                    target: "workload",
                    "failed to enqueue workload gossip for {}: {err}",
                    spec.name
                );
            }
        }

        {
            let mut guard = self.local_containers.lock().await;
            for plan in plans {
                if let Some(container_id) = plan.container_id.as_ref() {
                    guard.insert(plan.id, container_id.clone());
                }
            }
        }

        Ok(specs)
    }

    async fn cleanup_batch(&self, plans: &[BatchStartPlan]) {
        for plan in plans {
            if let Some(container_id) = plan.container_id.as_ref() {
                if let Err(err) = self
                    .container_manager
                    .stop_container(container_id, Some(Duration::from_secs(10)))
                    .await
                {
                    warn!(
                        target: "workload",
                        "failed to stop container {container_id} for workload {}: {err}",
                        plan.id
                    );
                }

                if let Err(err) = self
                    .container_manager
                    .remove_container(container_id, true, true)
                    .await
                {
                    warn!(
                        target: "workload",
                        "failed to remove container {container_id} for workload {}: {err}",
                        plan.id
                    );
                }

                let mut guard = self.local_containers.lock().await;
                guard.remove(&plan.id);
            }

            if plan.slot_id != 0 {
                if let Err(err) = self.release_slot(plan.slot_id).await {
                    warn!(
                        target: "workload",
                        "failed to release slot {} during rollback: {err}",
                        plan.slot_id
                    );
                }
            }
        }

        self.cleanup_orphaned_slots().await;
    }

    /// Returns workload specifications filtered according to the provided list policy.
    pub async fn list_containers(
        &self,
        filter: &WorkloadStateFilter,
    ) -> Result<Vec<WorkloadSpec>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("workload store load_all failed: {e}"))?;

        let mut specs = Vec::with_capacity(actives.len());
        for (k, snap) in actives {
            let id = k.to_uuid();
            if let Some(value) = snap.as_slice().last() {
                let spec = value_to_spec(id, value.clone());
                if filter.accepts(&spec.state) {
                    specs.push(spec);
                }
            }
        }
        Ok(specs)
    }

    async fn persist_spec(&self, spec: &WorkloadSpec) -> Result<(), anyhow::Error> {
        let value = WorkloadValue::new(
            spec.id,
            spec.name.clone(),
            spec.image.clone(),
            spec.state.clone(),
            spec.created_at.clone(),
            spec.command.clone(),
            spec.node_id,
            spec.node_name.clone(),
            spec.slot_id,
            spec.cpu_millis,
            spec.memory_bytes,
        );

        self.store
            .upsert(&UuidKey::from(spec.id), value)
            .await
            .map_err(|e| anyhow::anyhow!("workload upsert failed: {e}"))
    }

    async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow::anyhow!("workload remove failed: {e}"))?;
        Ok(())
    }

    fn tx(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn broadcast_remote_specs(&self, specs: &[WorkloadSpec]) {
        for spec in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if let Err(err) = self
                .enqueue_gossip(WorkloadEvent::Upsert(spec.clone()))
                .await
            {
                warn!(
                    target: "workload",
                    "failed to relay workload {} from node {}: {err}",
                    spec.name,
                    spec.node_id
                );
            }
        }
    }

    async fn cleanup_orphaned_slots(&self) {
        const MAX_ATTEMPTS: usize = 5;

        for _ in 0..MAX_ATTEMPTS {
            let snapshot = match self.scheduler.snapshot().await {
                Some(snapshot) => snapshot,
                None => return,
            };

            let reserved: Vec<SlotId> = snapshot
                .slots
                .iter()
                .filter_map(|slot| match &slot.state {
                    SlotState::Reserved(reservation) if reservation.owner == self.local_node_id => {
                        Some(slot.slot_id)
                    }
                    _ => None,
                })
                .collect();

            if reserved.is_empty() {
                return;
            }

            let active = match self.collect_local_slot_ids().await {
                Ok(ids) => ids,
                Err(err) => {
                    warn!(
                        target: "workload",
                        "failed to collect active slots while cleaning orphans: {err}"
                    );
                    return;
                }
            };

            let to_free: Vec<SlotId> = reserved
                .into_iter()
                .filter(|slot_id| !active.contains(slot_id))
                .collect();

            if to_free.is_empty() {
                return;
            }

            match self
                .scheduler
                .free_slots(snapshot.version, to_free.clone())
                .await
            {
                Ok(_) => return,
                Err(SchedulerError::SnapshotMismatch { .. })
                | Err(SchedulerError::SlotsNotReserved { .. }) => continue,
                Err(err) => {
                    warn!(
                        target: "workload",
                        "failed to free orphaned slots {:?}: {err}",
                        to_free
                    );
                    return;
                }
            }
        }
    }

    async fn collect_local_slot_ids(&self) -> Result<HashSet<SlotId>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("workload store load_all failed: {e}"))?;

        let mut slots = HashSet::new();
        for (key, snapshot) in actives {
            let id = key.to_uuid();
            if let Some(value) = snapshot.as_slice().last() {
                if value.node_id == self.local_node_id {
                    if let Some(slot_id) = value.slot_id {
                        slots.insert(slot_id);
                    }
                }
            } else {
                let _ = self.remove_spec(id).await;
            }
        }

        Ok(slots)
    }

    async fn enqueue_gossip(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        let id = Uuid::new_v4();
        let message = Message::Workload { id, event };
        self.tx()
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue workload gossip: {e}"))
    }

    async fn load_spec(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("workload lookup failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("unknown workload {id}"))?;

        let value = snapshot
            .as_slice()
            .last()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("workload {id} has no value"))?;

        Ok(value_to_spec(id, value))
    }

    pub async fn workload_owned_locally(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        Ok(spec.node_id == self.local_node_id)
    }

    pub async fn stop_workload(&self, id: Uuid) -> Result<WorkloadSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        let node_name = spec.node_name.clone();

        if spec.node_id != self.local_node_id {
            return Err(anyhow::anyhow!(
                "workload {id} is assigned to node {node_name}",
            ));
        }

        if matches!(spec.state, ContainerState::Stopped) {
            return Ok(spec);
        }

        let identifier_entry = {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&id)
        };

        let (container_identifier, from_cache) = match identifier_entry {
            Some(value) => (value, true),
            None => (format!("mantissa-{id}"), false),
        };

        let mut updated = spec.clone();
        if !matches!(spec.state, ContainerState::Stopping) {
            updated.state = ContainerState::Stopping;
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(WorkloadEvent::Upsert(updated.clone()))
                .await?;
        }

        match self
            .container_manager
            .stop_container(&container_identifier, Some(Duration::from_secs(10)))
            .await
        {
            Ok(_) => {}
            Err(ContainerError::NotFound(_)) => {
                debug!(
                    target: "workload",
                    "container {container_identifier} not found while stopping workload {id}; cache_hit={from_cache}"
                );
            }
            Err(e) => {
                updated.state = spec.state;
                if updated.state != ContainerState::Stopping {
                    self.persist_spec(&updated).await?;
                    self.enqueue_gossip(WorkloadEvent::Upsert(updated.clone()))
                        .await?;
                }
                return Err(anyhow::anyhow!("docker stop failed: {e}"));
            }
        }

        if let Err(e) = self
            .container_manager
            .remove_container(&container_identifier, false, true)
            .await
        {
            match e {
                ContainerError::NotFound(_) => debug!(
                    target: "workload",
                    "container {container_identifier} already absent while removing workload {id}"
                ),
                other => warn!(
                    target: "workload",
                    "failed to remove container {container_identifier}: {other}"
                ),
            }
        }

        updated.state = ContainerState::Stopped;
        if let Some(slot_id) = spec.slot_id {
            self.release_slot(slot_id)
                .await
                .with_context(|| "scheduler release failed during stop".to_string())?;
            updated.slot_id = None;
            updated.cpu_millis = 0;
            updated.memory_bytes = 0;
        }

        self.persist_spec(&updated).await?;
        self.enqueue_gossip(WorkloadEvent::Upsert(updated.clone()))
            .await?;
        self.cleanup_orphaned_slots().await;
        Ok(updated)
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    pub async fn run(&mut self) {
        while let Ok(message) = self.rx.recv().await {
            match message {
                Message::Workload { id, event } => {
                    if !self.record_gossip_id(id).await {
                        continue;
                    }
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(target: "workload", "failed to handle workload event: {e}");
                    }
                }
                Message::Void { .. } => {}
                _ => {}
            }
        }
    }

    async fn handle_event(&self, event: WorkloadEvent) -> Result<(), anyhow::Error> {
        match event {
            WorkloadEvent::Upsert(spec) => {
                if spec.node_id == self.local_node_id && spec.state != ContainerState::Running {
                    self.local_containers.lock().await.remove(&spec.id);
                }
                self.persist_spec(&spec).await
            }
            WorkloadEvent::Remove { id } => self.remove_spec(id).await,
        }
    }
}

fn value_to_spec(id: Uuid, value: WorkloadValue) -> WorkloadSpec {
    WorkloadSpec {
        id,
        name: value.name,
        image: value.image,
        state: value.state,
        created_at: value.created_at,
        command: value.command,
        node_id: value.node_id,
        node_name: value.node_name,
        slot_id: value.slot_id,
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::registry::Registry;
    use crate::scheduler::SlotSpec;
    use crate::store::local_session_store::LocalSessionStore;
    use crate::store::peer_store::open_peers_store;
    use crate::store::scheduler_store::open_scheduler_store;
    use crate::store::workload_store::open_workload_store;
    use crate::workload::types::WorkloadStateKind;
    use ::health::{Config as HealthConfig, HealthMonitor};
    use async_channel::bounded;
    use async_trait::async_trait;
    use ed25519_dalek::SigningKey;
    use net::noise::NoiseKeys;
    use std::collections::HashMap;
    use std::rc::Rc;
    use tempfile::tempdir;

    #[derive(Clone, Default)]
    struct MockContainerManager {
        created: Arc<AsyncMutex<Vec<String>>>,
        stopped: Arc<AsyncMutex<Vec<String>>>,
    }

    #[async_trait]
    impl ContainerManager for MockContainerManager {
        async fn create_container(
            &self,
            _name: &str,
            _image: &str,
            _command: Option<Vec<String>>,
            _env_vars: Option<Vec<String>>,
            _ports: Option<HashMap<String, Vec<HashMap<String, String>>>>,
            _volumes: Option<Vec<String>>,
            _restart_policy: Option<crate::workload::docker::RestartPolicyConfig>,
        ) -> crate::workload::docker::ContainerResult<String> {
            let mut guard = self.created.lock().await;
            let id = format!("container-{}", guard.len());
            guard.push(id.clone());
            Ok(id)
        }

        async fn start_container(
            &self,
            _container_id: &str,
        ) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }

        async fn stop_container(
            &self,
            container_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> crate::workload::docker::ContainerResult<()> {
            self.stopped.lock().await.push(container_id.to_string());
            Ok(())
        }

        async fn restart_container(
            &self,
            _container_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }

        async fn remove_container(
            &self,
            _container_id: &str,
            _force: bool,
            _remove_volumes: bool,
        ) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }

        async fn list_containers(
            &self,
            _filters: Option<HashMap<String, Vec<String>>>,
        ) -> crate::workload::docker::ContainerResult<Vec<crate::workload::docker::ContainerInfo>>
        {
            Ok(Vec::new())
        }

        async fn inspect_container(
            &self,
            _container_id: &str,
        ) -> crate::workload::docker::ContainerResult<bollard::service::ContainerInspectResponse>
        {
            Err(crate::workload::docker::ContainerError::OperationFailed(
                "inspect unsupported in mock".into(),
            ))
        }

        async fn pull_image(&self, _image: &str) -> crate::workload::docker::ContainerResult<()> {
            Ok(())
        }
    }

    fn temp_db(prefix: &str) -> (Arc<redb::Database>, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(format!("{prefix}-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(path).expect("create db"));
        (db, dir)
    }

    async fn setup_manager() -> (WorkloadManager, Rc<Scheduler>, Arc<MockContainerManager>) {
        let actor = Uuid::new_v4();
        let (scheduler_db, _dir) = temp_db("scheduler");
        let scheduler_store =
            open_scheduler_store(scheduler_db.clone(), actor).expect("open scheduler store");
        scheduler_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild scheduler store");

        let (registry_db, _reg_dir) = temp_db("registry");
        let peers_store = open_peers_store(registry_db.clone(), actor).expect("open peers store");
        peers_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild peers store");

        let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
        let session_store =
            LocalSessionStore::open(registry_db.clone(), &noise_keys).expect("open sessions");

        let health_monitor = HealthMonitor::new(HealthConfig::default());

        let registry = Registry::new(
            peers_store,
            session_store,
            SigningKey::from_bytes(&[0xA5; 32]),
            actor,
            health_monitor,
        );

        let scheduler = Rc::new(
            Scheduler::new(scheduler_store, registry.clone(), actor).expect("scheduler init"),
        );
        scheduler
            .init_slots([
                SlotSpec::new(0, SlotCapacity::new(1_000, 1_024 * 1_024 * 1_024)),
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1_024 * 1_024)),
            ])
            .await
            .expect("init slots");

        let (workload_db, _wd) = temp_db("workload");
        let workload_store = open_workload_store(workload_db, actor).expect("open workload store");

        let mock_manager = Arc::new(MockContainerManager::default());
        let (tx, rx) = bounded(4);
        let container_manager: Arc<dyn ContainerManager + Send + Sync> = mock_manager.clone();
        let manager = WorkloadManager::new(
            workload_store,
            tx,
            rx,
            actor,
            "local-node",
            scheduler.clone(),
            container_manager,
            registry,
        );

        (manager, scheduler, mock_manager)
    }

    #[tokio::test]
    async fn start_container_reserves_slot_and_records_resources() {
        let (manager, scheduler, _cm) = setup_manager().await;

        let spec = manager
            .start_container(
                "svc",
                "image",
                vec!["--arg".into()],
                500,
                256 * 1_024 * 1_024,
            )
            .await
            .expect("start container");

        assert_eq!(spec.cpu_millis, 1_000);
        assert_eq!(spec.memory_bytes, 1_024 * 1_024 * 1_024);
        let slot_id = spec.slot_id.expect("slot assigned");

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let slot = snapshot
            .slots
            .iter()
            .find(|s| s.slot_id == slot_id)
            .expect("slot exists");
        assert!(matches!(slot.state, SlotState::Reserved(_)));
    }

    #[tokio::test]
    async fn stop_workload_releases_slot_and_clears_resources() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024)
            .await
            .expect("start container");

        let slot_id = spec.slot_id.expect("slot assigned");
        let stopped = manager.stop_workload(spec.id).await.expect("stop workload");

        assert!(matches!(stopped.state, ContainerState::Stopped));
        let stopped_containers = mock_cm.stopped.lock().await.clone();
        assert_eq!(stopped_containers, vec!["container-0".to_string()]);

        assert!(stopped.slot_id.is_none());
        assert_eq!(stopped.cpu_millis, 0);
        assert_eq!(stopped.memory_bytes, 0);

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let slot = snapshot
            .slots
            .iter()
            .find(|s| s.slot_id == slot_id)
            .expect("slot exists");
        assert!(matches!(slot.state, SlotState::Free));
    }

    #[tokio::test]
    async fn stop_workload_uses_container_name_when_cache_missing() {
        let (manager, _scheduler, mock_cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024)
            .await
            .expect("start container");

        {
            manager.local_containers.lock().await.clear();
        }

        {
            mock_cm.stopped.lock().await.clear();
        }

        manager
            .stop_workload(spec.id)
            .await
            .expect("stop workload with fallback");

        let expected = format!("mantissa-{}", spec.id);
        let stopped_containers = mock_cm.stopped.lock().await.clone();
        assert_eq!(stopped_containers, vec![expected]);
    }

    #[tokio::test]
    async fn list_containers_respects_filters() {
        let (manager, _scheduler, _mock_cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024)
            .await
            .expect("start container");

        let active = manager
            .list_containers(&WorkloadStateFilter::active_only())
            .await
            .expect("list active");
        assert_eq!(active.len(), 1);
        assert!(matches!(active[0].state, ContainerState::Running));

        manager.stop_workload(spec.id).await.expect("stop workload");

        let active_only = manager
            .list_containers(&WorkloadStateFilter::active_only())
            .await
            .expect("list active after stop");
        assert!(active_only.is_empty());

        let with_stopped = manager
            .list_containers(&WorkloadStateFilter::new([
                WorkloadStateKind::Pending,
                WorkloadStateKind::Creating,
                WorkloadStateKind::Running,
                WorkloadStateKind::Stopping,
                WorkloadStateKind::Stopped,
            ]))
            .await
            .expect("list active with stopped");
        assert_eq!(with_stopped.len(), 1);
        assert!(matches!(with_stopped[0].state, ContainerState::Stopped));

        let all = manager
            .list_containers(&WorkloadStateFilter::all())
            .await
            .expect("list all");

        let only_stopped = manager
            .list_containers(&WorkloadStateFilter::new([WorkloadStateKind::Stopped]))
            .await
            .expect("list stopped only");
        assert_eq!(only_stopped.len(), 1);
        assert!(matches!(only_stopped[0].state, ContainerState::Stopped));
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn start_container_fails_when_no_matching_slot() {
        let (manager, _scheduler, _cm) = setup_manager().await;

        let err = manager
            .start_container("svc", "image", vec![], 2_000, 512 * 1_024 * 1_024)
            .await
            .expect_err("reservation should fail");
        assert!(err.to_string().contains("scheduler reservation failed"));
    }

    #[tokio::test]
    async fn start_containers_batch_reserves_every_slot() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let specs = manager
            .start_containers_batch(vec![
                ContainerStartRequest {
                    name: "svc-a".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 400,
                    memory_bytes: 128 * 1_024 * 1_024,
                    id: None,
                    slot_id: None,
                },
                ContainerStartRequest {
                    name: "svc-b".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                    id: None,
                    slot_id: None,
                },
            ])
            .await
            .expect("batch start");

        assert_eq!(specs.len(), 2);

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let reserved = snapshot
            .slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
            .count();
        assert_eq!(reserved, 2);

        let created = mock_cm.created.lock().await.clone();
        assert_eq!(created.len(), 2);
    }

    #[tokio::test]
    async fn start_containers_batch_respects_existing_reservations() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let workload_id = Uuid::new_v4();
        let slot_id = 0;

        scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id,
                    owner: manager.local_node_id,
                    workload_id: Some(workload_id),
                }],
            )
            .await
            .expect("pre-reserve slot");

        let specs = manager
            .start_containers_batch(vec![ContainerStartRequest {
                name: "svc-pre".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                id: Some(workload_id),
                slot_id: Some(slot_id),
            }])
            .await
            .expect("start with pre-reserved slot");

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].slot_id, Some(slot_id));

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        assert_eq!(snapshot.version, 1);
        let reserved: Vec<_> = snapshot
            .slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
            .collect();
        assert_eq!(reserved.len(), 1);
        assert_eq!(reserved[0].slot_id, slot_id);
        match &reserved[0].state {
            SlotState::Reserved(reservation) => {
                assert_eq!(reservation.owner, manager.local_node_id);
                assert_eq!(reservation.workload_id, Some(workload_id));
            }
            _ => unreachable!("slot should be reserved"),
        }

        let created = mock_cm.created.lock().await.clone();
        assert_eq!(created.len(), 1);
    }

    #[tokio::test]
    async fn workload_owned_locally_detects_remote_entries() {
        let (manager, _scheduler, _mock_cm) = setup_manager().await;

        let local_spec = manager
            .start_container("local", "img", vec![], 200, 64 * 1_024 * 1_024)
            .await
            .expect("start local workload");

        assert!(
            manager
                .workload_owned_locally(local_spec.id)
                .await
                .expect("local ownership check")
        );

        let remote_id = Uuid::new_v4();
        let remote_value = WorkloadValue::new(
            remote_id,
            "remote",
            "img",
            ContainerState::Running,
            Utc::now().to_rfc3339(),
            vec![],
            Uuid::new_v4(),
            "remote-node",
            Some(1),
            100,
            64 * 1_024 * 1_024,
        );

        let store = manager.store.clone();
        store
            .upsert(&UuidKey::from(remote_id), remote_value)
            .await
            .expect("insert remote workload value");

        assert!(
            !manager
                .workload_owned_locally(remote_id)
                .await
                .expect("remote ownership check")
        );
    }

    #[tokio::test]
    async fn start_containers_batch_is_atomic_on_capacity_failure() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        manager
            .start_container("baseline", "img", vec![], 400, 128 * 1_024 * 1_024)
            .await
            .expect("pre-existing container");

        let created_before = mock_cm.created.lock().await.len();

        let err = manager
            .start_containers_batch(vec![
                ContainerStartRequest {
                    name: "svc-c".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                    id: None,
                    slot_id: None,
                },
                ContainerStartRequest {
                    name: "svc-d".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                    id: None,
                    slot_id: None,
                },
            ])
            .await
            .expect_err("batch should fail when capacity is insufficient");

        assert!(err.to_string().contains("scheduler reservation failed"));

        let created_after = mock_cm.created.lock().await.len();
        assert_eq!(created_before, created_after);

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let reserved = snapshot
            .slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
            .count();
        assert_eq!(reserved, 1);
    }
}
