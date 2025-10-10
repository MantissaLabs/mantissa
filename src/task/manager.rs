use crate::gossip::Message;
use crate::registry::Registry;
use crate::scheduler::summary::SchedulerSlotState;
use crate::scheduler::{
    Scheduler, SchedulerError, SchedulerSnapshot, SlotCapacity, SlotId, SlotReservationRequest,
    SlotState,
};
use crate::store::task_store::TaskStore;
use crate::task::container::ContainerState;
use crate::task::docker::{
    ContainerError, ContainerManager, ResourceLimits, RestartPolicyConfig, RestartPolicyType,
};
use crate::task::types::{
    TaskEvent, TaskRestartPolicy, TaskRestartPolicyKind, TaskSpec, TaskStateFilter, TaskValue,
};
use anyhow::Context;
use async_channel::{Receiver, Sender};
use bollard::errors::Error as BollardError;
use chrono::{DateTime, Utc};
use crdt_store::uuid_key::UuidKey;
use std::collections::{HashMap, HashSet, VecDeque};
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
pub struct TaskManager {
    store: TaskStore,
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
pub struct TaskStartRequest {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub id: Option<Uuid>,
    pub slot_ids: Vec<SlotId>,
    pub restart_policy: Option<TaskRestartPolicy>,
}

struct BatchStartPlan {
    id: Uuid,
    name: String,
    image: String,
    command: Vec<String>,
    slots: Vec<SlotChoice>,
    requested_cpu_millis: u64,
    requested_memory_bytes: u64,
    container_name: String,
    container_id: Option<String>,
    created_at: DateTime<Utc>,
    index: usize,
    preassigned: bool,
    restart_policy: Option<TaskRestartPolicy>,
}

impl BatchStartPlan {
    fn slot_ids(&self) -> Vec<SlotId> {
        let mut ids: Vec<SlotId> = self.slots.iter().map(|slot| slot.slot_id).collect();
        ids.sort_unstable();
        ids
    }
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
    preassigned_slots: Vec<SlotId>,
    restart_policy: Option<TaskRestartPolicy>,
}

#[derive(Clone)]
struct SlotChoice {
    slot_id: SlotId,
    capacity: SlotCapacity,
}

/// Identifies whether a scheduling candidate refers to this node or a remote peer.
#[derive(Clone)]
enum CandidateLocation {
    Local,
    Remote { peer_id: Uuid, version: u64 },
}

/// Wrapper for a node's free slots together with the metadata needed to build
/// either a local or remote plan once a slot is assigned.
struct Candidate {
    location: CandidateLocation,
    slots: Vec<SlotChoice>,
}

impl Candidate {
    /// Returns `Some` when the node has at least one usable slot; otherwise we
    /// drop the candidate early so later stages don't need to handle empties.
    fn new(location: CandidateLocation, slots: Vec<SlotChoice>) -> Option<Self> {
        if slots.is_empty() {
            None
        } else {
            Some(Self { location, slots })
        }
    }

    /// Attempts to reserve enough slots to satisfy the requested CPU and memory.
    /// A greedy selection is used: for each iteration we pick the slot that
    /// contributes the largest share of the remaining requirement. The
    /// allocation only succeeds when the accumulated capacity fully covers the
    /// requested resources.
    fn allocate(&mut self, cpu_millis: u64, memory_bytes: u64) -> Option<Vec<SlotChoice>> {
        if self.slots.is_empty() {
            return None;
        }

        // Zero-capacity requests still require a single slot to run the task so
        // we keep the previous behaviour.
        if cpu_millis == 0 && memory_bytes == 0 {
            let slot = self.slots.remove(0);
            return Some(vec![slot]);
        }

        let mut remaining_cpu = cpu_millis;
        let mut remaining_mem = memory_bytes;
        let mut selected_indices: Vec<usize> = Vec::new();
        let mut available_indices: Vec<usize> = (0..self.slots.len()).collect();

        while remaining_cpu > 0 || remaining_mem > 0 {
            if available_indices.is_empty() {
                return None;
            }

            let mut best_choice = None;
            let mut best_score = 0u128;

            for &idx in &available_indices {
                let slot = &self.slots[idx];
                let cpu_contrib = std::cmp::min(slot.capacity.cpu_millis, remaining_cpu);
                let mem_contrib = std::cmp::min(slot.capacity.memory_bytes, remaining_mem);
                let score = (cpu_contrib as u128) << 64 | mem_contrib as u128;

                if score > best_score {
                    best_score = score;
                    best_choice = Some(idx);
                }
            }

            let best_idx = best_choice?;

            let slot = &self.slots[best_idx];
            if slot.capacity.cpu_millis == 0 && slot.capacity.memory_bytes == 0 {
                // Slot contributes nothing; abort allocation to avoid infinite loop.
                return None;
            }

            selected_indices.push(best_idx);
            remaining_cpu = remaining_cpu.saturating_sub(slot.capacity.cpu_millis);
            remaining_mem = remaining_mem.saturating_sub(slot.capacity.memory_bytes);
            available_indices.retain(|&idx| idx != best_idx);
        }

        selected_indices.sort_unstable_by(|a, b| b.cmp(a));
        let mut allocated = Vec::with_capacity(selected_indices.len());
        for idx in selected_indices {
            allocated.push(self.slots.remove(idx));
        }
        allocated.reverse();
        Some(allocated)
    }

    fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

struct RemoteStartPlan {
    index: usize,
    id: Uuid,
    name: String,
    image: String,
    command: Vec<String>,
    cpu_millis: u64,
    memory_bytes: u64,
    slots: Vec<SlotChoice>,
    peer_id: Uuid,
    scheduler_version: u64,
    restart_policy: Option<TaskRestartPolicy>,
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

impl TaskManager {
    fn build_start_intents(requests: Vec<TaskStartRequest>) -> Vec<StartIntent> {
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
                preassigned_slots: request.slot_ids,
                restart_policy: request.restart_policy,
            })
            .collect()
    }

    pub fn new(
        store: TaskStore,
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
        restart_policy: Option<TaskRestartPolicy>,
    ) -> Result<TaskSpec, anyhow::Error> {
        let request = TaskStartRequest {
            name: name.into(),
            image: image.into(),
            command,
            cpu_millis,
            memory_bytes,
            id: None,
            slot_ids: Vec::new(),
            restart_policy,
        };

        let mut specs = self.start_tasks_batch(vec![request]).await?;
        Ok(specs
            .pop()
            .expect("batch start with single request should yield one spec"))
    }

    pub async fn start_tasks_batch(
        &self,
        requests: Vec<TaskStartRequest>,
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
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
                        target: "task",
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
                        target: "task",
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

            let remote_specs = match self.materialize_remote_specs(&remote_plans).await {
                Ok(specs) => specs,
                Err(ExecutionError::Retry(err)) => {
                    debug!(
                        target: "task",
                        "remote materialization conflicted on attempt {attempt}: {err}"
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
                    reserved_remote.clear();
                    let mut ordered: Vec<Option<TaskSpec>> = vec![None; intents.len()];

                    for (idx, spec) in remote_specs.into_iter().chain(local_specs.into_iter()) {
                        ordered[idx] = Some(spec);
                    }

                    let specs: Vec<TaskSpec> = ordered
                        .into_iter()
                        .map(|spec| spec.expect("missing task spec after execution"))
                        .collect();

                    self.broadcast_remote_specs(&specs).await;

                    return Ok(specs);
                }
                Err(err) => {
                    debug!(
                        target: "task",
                        "local execution failed; rolling back remote tasks: {err}"
                    );
                    self.signal_remote_stop(&remote_specs).await;
                    self.release_remote_slots(&reserved_remote).await;
                    reserved_remote.clear();
                    if let Some(slots) = reserved_local_slots.take() {
                        self.release_local_slots(&slots).await;
                    }
                    return Err(err);
                }
            }
        }

        Err(anyhow::anyhow!(
            "failed to schedule tasks after {MAX_ATTEMPTS} attempts"
        ))
    }

    /// Compute a placement plan for a batch of start intents.
    ///
    /// The pipeline is intentionally broken down into three steps for clarity:
    /// 1. Attach any pre-assigned local slots that came from the request (seed_local_plans).
    /// 2. Discover the candidates (local node + remote peers) that can satisfy the rest.
    /// 3. Walk the remaining intents in order and pick a slot from the candidate queue.
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
        let (mut assignment, remaining_intents, available_slots) =
            self.seed_local_plans(intents, &snapshot, local_version)?;

        if remaining_intents.is_empty() {
            assignment.local.sort_by_key(|plan| plan.index);
            return Ok(assignment);
        }

        let mut candidates = self.build_candidate_queue(available_slots).await?;
        if candidates.is_empty() {
            return Err(anyhow::anyhow!(
                "scheduler reservation failed: no available capacity across cluster"
            ));
        }

        self.allocate_remaining(&mut assignment, &mut candidates, remaining_intents)?;
        assignment.local.sort_by_key(|plan| plan.index);
        assignment.remote.sort_by_key(|plan| plan.index);
        Ok(assignment)
    }

    /// Prepare an initial `Assignment` containing all tasks that specified
    /// a concrete local slot. The remaining intents plus the list of free local
    /// slots are returned for the distributed placement step.
    fn seed_local_plans<'a>(
        &'a self,
        intents: &'a [StartIntent],
        snapshot: &SchedulerSnapshot,
        local_version: u64,
    ) -> Result<(Assignment, Vec<&'a StartIntent>, Vec<SlotChoice>), anyhow::Error> {
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
        for intent in intents.iter() {
            if intent.preassigned_slots.is_empty() {
                continue;
            }

            let mut seen = HashSet::new();
            let mut chosen_slots = Vec::new();

            for slot_id in intent.preassigned_slots.iter().copied() {
                if !seen.insert(slot_id) {
                    return Err(anyhow::anyhow!(
                        "duplicate preassigned slot {slot_id} for task {}",
                        intent.name
                    ));
                }

                let slot = slot_lookup
                    .get(&slot_id)
                    .ok_or_else(|| anyhow::anyhow!("unknown preassigned slot {slot_id}"))?;

                if let SlotState::Reserved(reservation) = &slot.state {
                    if reservation.owner != self.local_node_id {
                        return Err(anyhow::anyhow!(
                            "preassigned slot {slot_id} owned by different node"
                        ));
                    }

                    if let Some(task_id) = reservation.task_id {
                        if task_id != intent.id {
                            return Err(anyhow::anyhow!(
                                "preassigned slot {slot_id} reserved for task {task_id}"
                            ));
                        }
                    }
                }

                available_local_slots.retain(|slot| slot.slot_id != slot_id);
                chosen_slots.push(SlotChoice {
                    slot_id,
                    capacity: slot.capacity,
                });
            }

            if chosen_slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task '{}' must specify at least one preassigned slot",
                    intent.name
                ));
            }

            let total_cpu: u64 = chosen_slots
                .iter()
                .map(|slot| slot.capacity.cpu_millis)
                .sum();
            let total_mem: u64 = chosen_slots
                .iter()
                .map(|slot| slot.capacity.memory_bytes)
                .sum();

            if intent.cpu_millis > total_cpu || intent.memory_bytes > total_mem {
                return Err(anyhow::anyhow!(
                    "preassigned slots for task '{}' provide insufficient capacity",
                    intent.name
                ));
            }

            local_plans.push(BatchStartPlan {
                id: intent.id,
                name: intent.name.clone(),
                image: intent.image.clone(),
                command: intent.command.clone(),
                slots: chosen_slots,
                requested_cpu_millis: intent.cpu_millis,
                requested_memory_bytes: intent.memory_bytes,
                container_name: String::new(),
                container_id: None,
                created_at: Utc::now(),
                index: intent.index,
                preassigned: true,
                restart_policy: intent.restart_policy.clone(),
            });
        }

        let assignment = Assignment {
            local_version,
            local: local_plans,
            remote: Vec::new(),
        };
        let remaining_intents: Vec<&StartIntent> = intents
            .iter()
            .filter(|intent| intent.preassigned_slots.is_empty())
            .collect();

        Ok((assignment, remaining_intents, available_local_slots))
    }

    /// Build the round-robin candidate queue starting with the local node followed
    /// by a shuffled list of peers. Each candidate wraps the slots currently
    /// reported as free by the relevant scheduler snapshot.
    async fn build_candidate_queue(
        &self,
        local_slots: Vec<SlotChoice>,
    ) -> Result<VecDeque<Candidate>, anyhow::Error> {
        let mut queue = VecDeque::new();
        if let Some(local_candidate) = Candidate::new(CandidateLocation::Local, local_slots) {
            queue.push_back(local_candidate);
        }

        let peers = self.registry.known_peers()?;
        let mut remote_candidates = Vec::new();
        for peer_id in peers {
            if peer_id == self.local_node_id {
                continue;
            }

            let summary = match self.scheduler.fetch_remote_summary(peer_id, true).await {
                Ok(summary) => summary,
                Err(err) => {
                    debug!(
                        target: "task",
                        "scheduler summary fetch failed for peer {peer_id}: {err}"
                    );
                    continue;
                }
            };

            let slots: Vec<SlotChoice> = summary
                .details
                .iter()
                .filter(|detail| matches!(detail.state, SchedulerSlotState::Free))
                .map(|detail| SlotChoice {
                    slot_id: detail.slot_id,
                    capacity: SlotCapacity::new(detail.cpu_millis, detail.memory_bytes),
                })
                .collect();

            if let Some(candidate) = Candidate::new(
                CandidateLocation::Remote {
                    peer_id,
                    version: summary.version,
                },
                slots,
            ) {
                remote_candidates.push(candidate);
            }
        }

        let mut rng = rng();
        remote_candidates.shuffle(&mut rng);
        for candidate in remote_candidates {
            queue.push_back(candidate);
        }

        Ok(queue)
    }

    /// Allocate the remaining intents across the candidate queue. The queue is
    /// treated as a ring: after examining a candidate we move it to the back so
    /// subsequent intents see a rotated view, which naturally spreads replicas.
    fn allocate_remaining(
        &self,
        assignment: &mut Assignment,
        candidates: &mut VecDeque<Candidate>,
        intents: Vec<&StartIntent>,
    ) -> Result<(), anyhow::Error> {
        for intent in intents {
            let candidate_count = candidates.len();
            if candidate_count == 0 {
                return Err(anyhow::anyhow!(
                    "scheduler reservation failed: insufficient capacity for batch"
                ));
            }

            let mut allocated: Option<(CandidateLocation, Vec<SlotChoice>)> = None;
            for _ in 0..candidate_count {
                let mut candidate = candidates
                    .pop_front()
                    .expect("candidate deque should not be empty");

                if let Some(slots) = candidate.allocate(intent.cpu_millis, intent.memory_bytes) {
                    let location = candidate.location.clone();
                    if !candidate.is_empty() {
                        candidates.push_back(candidate);
                    }
                    allocated = Some((location, slots));
                    break;
                } else {
                    candidates.push_back(candidate);
                }
            }

            let Some((location, slots)) = allocated else {
                return Err(anyhow::anyhow!(
                    "scheduler reservation failed: insufficient capacity for batch"
                ));
            };

            match location {
                CandidateLocation::Local => {
                    assignment.local.push(BatchStartPlan {
                        id: intent.id,
                        name: intent.name.clone(),
                        image: intent.image.clone(),
                        command: intent.command.clone(),
                        slots,
                        requested_cpu_millis: intent.cpu_millis,
                        requested_memory_bytes: intent.memory_bytes,
                        container_name: String::new(),
                        container_id: None,
                        created_at: Utc::now(),
                        index: intent.index,
                        preassigned: false,
                        restart_policy: intent.restart_policy.clone(),
                    });
                }
                CandidateLocation::Remote { peer_id, version } => {
                    assignment.remote.push(RemoteStartPlan {
                        index: intent.index,
                        id: intent.id,
                        name: intent.name.clone(),
                        image: intent.image.clone(),
                        command: intent.command.clone(),
                        cpu_millis: intent.cpu_millis,
                        memory_bytes: intent.memory_bytes,
                        slots,
                        peer_id,
                        scheduler_version: version,
                        restart_policy: intent.restart_policy.clone(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn start_local_containers(
        &self,
        plans: &mut [BatchStartPlan],
    ) -> Result<Vec<(usize, TaskSpec)>, anyhow::Error> {
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

    async fn release_local_slots(&self, slots: &[SlotId]) {
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

    async fn release_remote_slots(&self, reservations: &HashMap<Uuid, RemoteReservation>) {
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

    async fn remote_session(
        &self,
        peer_id: Uuid,
    ) -> Result<cluster_session::Client, anyhow::Error> {
        self.registry
            .session_for_peer(peer_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("no active session for peer {peer_id}"))
    }

    async fn signal_remote_stop(&self, specs: &[(usize, TaskSpec)]) {
        if specs.is_empty() {
            return;
        }

        for (_, spec) in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if matches!(
                spec.state,
                ContainerState::Stopping | ContainerState::Stopped
            ) {
                continue;
            }

            let mut updated = spec.clone();
            updated.state = ContainerState::Stopping;

            if let Err(err) = self.persist_spec(&updated).await {
                warn!(
                    target: "task",
                    "failed to persist stopping state for remote task {}: {err}",
                    spec.id
                );
                continue;
            }

            if let Err(err) = self.enqueue_gossip(TaskEvent::Upsert(updated)).await {
                warn!(
                    target: "task",
                    "failed to broadcast stopping state for remote task {}: {err}",
                    spec.id
                );
            }
        }
    }

    async fn materialize_remote_specs(
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
                command: plan.command.clone(),
                node_id: plan.peer_id,
                node_name,
                slot_ids: slot_ids.clone(),
                slot_id: slot_ids.first().copied(),
                cpu_millis: plan.cpu_millis,
                memory_bytes: plan.memory_bytes,
                restart_policy: plan.restart_policy.clone(),
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
            if let Err(err) = self.enqueue_gossip(TaskEvent::Upsert(spec.clone())).await {
                warn!(
                    target: "task",
                    "failed to enqueue task gossip for {}: {err}",
                    spec.name
                );
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

            let restart_policy = plan
                .restart_policy
                .as_ref()
                .map(|policy| RestartPolicyConfig {
                    name: match policy.name {
                        TaskRestartPolicyKind::No => RestartPolicyType::No,
                        TaskRestartPolicyKind::Always => RestartPolicyType::Always,
                        TaskRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
                        TaskRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
                    },
                    max_retry_count: policy.max_retry_count,
                });

            let resource_limits = ResourceLimits::from_requests(
                plan.requested_cpu_millis,
                plan.requested_memory_bytes,
            );

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
                    restart_policy,
                    resource_limits,
                )
                .await
                .with_context(|| format!("docker create failed for task {}", plan.name))?;

            plan.container_id = Some(container_id.clone());

            self.container_manager
                .start_container(&container_id)
                .await
                .with_context(|| format!("docker start failed for task {}", plan.name))?;

            plan.created_at = Utc::now();
        }

        Ok(())
    }

    async fn commit_batch(&self, plans: &[BatchStartPlan]) -> Result<Vec<TaskSpec>, anyhow::Error> {
        let mut specs = Vec::with_capacity(plans.len());
        let mut persisted: Vec<TaskSpec> = Vec::new();

        for plan in plans {
            if plan.slots.is_empty() {
                return Err(anyhow::anyhow!(
                    "task {} has no slots assigned during commit",
                    plan.name
                ));
            }

            let slot_ids = plan.slot_ids();
            let slot_id = slot_ids.first().copied();
            let spec = TaskSpec {
                id: plan.id,
                name: plan.name.clone(),
                image: plan.image.clone(),
                state: ContainerState::Running,
                created_at: plan.created_at.to_rfc3339(),
                command: plan.command.clone(),
                node_id: self.local_node_id,
                node_name: self.local_node_name.clone(),
                slot_ids,
                slot_id,
                cpu_millis: plan.requested_cpu_millis,
                memory_bytes: plan.requested_memory_bytes,
                restart_policy: plan.restart_policy.clone(),
            };

            if let Err(err) = self.persist_spec(&spec).await {
                for rollback in &persisted {
                    let _ = self.remove_spec(rollback.id).await;
                }
                return Err(err.context(format!("failed to persist task spec {}", spec.name)));
            }

            persisted.push(spec.clone());
            specs.push(spec);
        }

        for spec in &specs {
            if let Err(err) = self.enqueue_gossip(TaskEvent::Upsert(spec.clone())).await {
                warn!(
                    target: "task",
                    "failed to enqueue task gossip for {}: {err}",
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
                        target: "task",
                        "failed to stop container {container_id} for task {}: {err}",
                        plan.id
                    );
                }

                if let Err(err) = self
                    .container_manager
                    .remove_container(container_id, true, true)
                    .await
                {
                    warn!(
                        target: "task",
                        "failed to remove container {container_id} for task {}: {err}",
                        plan.id
                    );
                }

                let mut guard = self.local_containers.lock().await;
                guard.remove(&plan.id);
            }

            for slot in &plan.slots {
                if let Err(err) = self.release_slot(slot.slot_id).await {
                    warn!(
                        target: "task",
                        "failed to release slot {} during rollback: {err}",
                        slot.slot_id
                    );
                }
            }
        }

        self.cleanup_orphaned_slots().await;
    }

    /// Returns task specifications filtered according to the provided list policy.
    pub async fn list_tasks(
        &self,
        filter: &TaskStateFilter,
    ) -> Result<Vec<TaskSpec>, anyhow::Error> {
        let (actives, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

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

    async fn persist_spec(&self, spec: &TaskSpec) -> Result<(), anyhow::Error> {
        let mut value = TaskValue::new(
            spec.id,
            spec.name.clone(),
            spec.image.clone(),
            spec.state.clone(),
            spec.created_at.clone(),
            spec.command.clone(),
            spec.node_id,
            spec.node_name.clone(),
            spec.slot_ids.clone(),
            spec.cpu_millis,
            spec.memory_bytes,
        );

        value.restart_policy = spec.restart_policy.clone();

        self.store
            .upsert(&UuidKey::from(spec.id), value)
            .await
            .map_err(|e| anyhow::anyhow!("task upsert failed: {e}"))
    }

    async fn remove_spec(&self, id: Uuid) -> Result<(), anyhow::Error> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow::anyhow!("task remove failed: {e}"))?;
        Ok(())
    }

    fn tx(&self) -> Sender<Message> {
        self.tx.clone()
    }

    async fn broadcast_remote_specs(&self, specs: &[TaskSpec]) {
        for spec in specs {
            if spec.node_id == self.local_node_id {
                continue;
            }

            if let Err(err) = self.enqueue_gossip(TaskEvent::Upsert(spec.clone())).await {
                warn!(
                    target: "task",
                    "failed to relay task {} from node {}: {err}",
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
                        target: "task",
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
                        target: "task",
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
            .map_err(|e| anyhow::anyhow!("task store load_all failed: {e}"))?;

        let mut slots = HashSet::new();
        for (key, snapshot) in actives {
            let id = key.to_uuid();
            if let Some(value) = snapshot.as_slice().last() {
                if value.node_id == self.local_node_id {
                    if value.slot_ids.is_empty() {
                        if let Some(slot_id) = value.slot_id {
                            slots.insert(slot_id);
                        }
                    } else {
                        for slot_id in &value.slot_ids {
                            slots.insert(*slot_id);
                        }
                    }
                }
            } else {
                let _ = self.remove_spec(id).await;
            }
        }

        Ok(slots)
    }

    async fn enqueue_gossip(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        let id = Uuid::new_v4();
        let message = Message::Task { id, event };
        self.tx()
            .send(message)
            .await
            .map_err(|e| anyhow::anyhow!("failed to enqueue task gossip: {e}"))
    }

    async fn perform_local_stop(&self, spec: TaskSpec) -> Result<TaskSpec, anyhow::Error> {
        if matches!(spec.state, ContainerState::Stopped) {
            return Ok(spec);
        }

        let id = spec.id;
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
            self.enqueue_gossip(TaskEvent::Upsert(updated.clone()))
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
                    target: "task",
                    "container {container_identifier} not found while stopping task {id}; cache_hit={from_cache}"
                );
            }
            Err(e) => {
                updated.state = spec.state;
                if updated.state != ContainerState::Stopping {
                    self.persist_spec(&updated).await?;
                    self.enqueue_gossip(TaskEvent::Upsert(updated.clone()))
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
                    target: "task",
                    "container {container_identifier} already absent while removing task {id}"
                ),
                other => warn!(
                    target: "task",
                    "failed to remove container {container_identifier}: {other}"
                ),
            }
        }

        updated.state = ContainerState::Stopped;
        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                self.release_slot(*slot_id)
                    .await
                    .with_context(|| "scheduler release failed during stop".to_string())?;
            }
            updated.slot_ids.clear();
            updated.slot_id = None;
            updated.cpu_millis = 0;
            updated.memory_bytes = 0;
        }

        self.persist_spec(&updated).await?;
        self.enqueue_gossip(TaskEvent::Upsert(updated.clone()))
            .await?;
        self.cleanup_orphaned_slots().await;
        Ok(updated)
    }

    async fn mark_task_failed(&self, mut spec: TaskSpec, error: anyhow::Error) -> anyhow::Error {
        let task_id = spec.id;
        warn!(
            target: "task",
            "marking task {} ({}) as failed: {error}",
            spec.name,
            task_id
        );

        {
            let mut guard = self.local_containers.lock().await;
            guard.remove(&task_id);
        }

        if !spec.slot_ids.is_empty() {
            for slot_id in &spec.slot_ids {
                if let Err(err) = self.release_slot(*slot_id).await {
                    warn!(
                        target: "task",
                        "failed to release slot {} after failure of {}: {err}",
                        slot_id,
                        task_id
                    );
                }
            }
            spec.slot_ids.clear();
            spec.slot_id = None;
        }

        spec.state = ContainerState::Failed;

        if let Err(err) = self.persist_spec(&spec).await {
            warn!(
                target: "task",
                "failed to persist failed state for task {}: {err}",
                task_id
            );
        } else if let Err(err) = self.enqueue_gossip(TaskEvent::Upsert(spec.clone())).await {
            warn!(
                target: "task",
                "failed to broadcast failed state for task {}: {err}",
                task_id
            );
        }

        self.cleanup_orphaned_slots().await;
        error
    }

    fn restart_policy_to_config(policy: &TaskRestartPolicy) -> RestartPolicyConfig {
        RestartPolicyConfig {
            name: match policy.name {
                TaskRestartPolicyKind::No => RestartPolicyType::No,
                TaskRestartPolicyKind::Always => RestartPolicyType::Always,
                TaskRestartPolicyKind::OnFailure => RestartPolicyType::OnFailure,
                TaskRestartPolicyKind::UnlessStopped => RestartPolicyType::UnlessStopped,
            },
            max_retry_count: policy.max_retry_count,
        }
    }

    async fn ensure_local_tracking(&self, spec: &TaskSpec) {
        let mut guard = self.local_containers.lock().await;
        guard
            .entry(spec.id)
            .or_insert_with(|| format!("mantissa-{}", spec.id));
    }

    async fn ensure_task_running(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        if matches!(spec.state, ContainerState::Running) {
            self.ensure_local_tracking(&spec).await;
            return Ok(());
        }

        if matches!(
            spec.state,
            ContainerState::Stopping | ContainerState::Stopped | ContainerState::Failed
        ) {
            return Ok(());
        }

        if spec.slot_ids.is_empty() {
            return Err(anyhow::anyhow!(
                "task {} ({}) missing scheduler slot assignments",
                spec.name,
                spec.id
            ));
        }

        let mut working = spec.clone();
        let task_name = working.name.clone();
        if !matches!(working.state, ContainerState::Creating) {
            working.state = ContainerState::Creating;
            if let Err(err) = self.persist_spec(&working).await {
                return Err(err);
            }
            if let Err(err) = self
                .enqueue_gossip(TaskEvent::Upsert(working.clone()))
                .await
            {
                warn!(
                    target: "task",
                    "failed to broadcast creating state for task {}: {err}",
                    working.id
                );
            }
        }

        if let Err(err) = self
            .container_manager
            .pull_image(&working.image)
            .await
            .with_context(|| format!("docker pull failed for image {}", working.image))
        {
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        let restart_policy = working
            .restart_policy
            .as_ref()
            .map(Self::restart_policy_to_config);

        let resource_limits =
            ResourceLimits::from_requests(working.cpu_millis, working.memory_bytes);

        let container_name = format!("mantissa-{}", working.id);

        let create_outcome = self
            .container_manager
            .create_container(
                &container_name,
                &working.image,
                if working.command.is_empty() {
                    None
                } else {
                    Some(working.command.clone())
                },
                None,
                None,
                None,
                restart_policy,
                resource_limits,
            )
            .await;

        let (container_id, created_fresh) = match create_outcome {
            Ok(id) => (id, true),
            Err(err) => {
                if is_name_conflict(&err) {
                    match self.resolve_existing_container_id(&container_name).await {
                        Ok(Some(existing_id)) => (existing_id, false),
                        Ok(None) => {
                            let err = self
                                .mark_task_failed(working, wrap_create_error(&task_name, err))
                                .await;
                            return Err(err);
                        }
                        Err(inspect_err) => {
                            let err = self
                                .mark_task_failed(
                                    working,
                                    wrap_existing_inspect_error(&task_name, inspect_err),
                                )
                                .await;
                            return Err(err);
                        }
                    }
                } else {
                    let err = self
                        .mark_task_failed(working, wrap_create_error(&task_name, err))
                        .await;
                    return Err(err);
                }
            }
        };

        match self.container_manager.start_container(&container_id).await {
            Ok(_) => {}
            Err(err) => {
                if container_already_running(&err) {
                    debug!(
                        target: "task",
                        "container {} already running while starting task {}",
                        container_id,
                        working.id
                    );
                } else {
                    if created_fresh {
                        if let Err(remove_err) = self
                            .container_manager
                            .remove_container(&container_id, true, true)
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to remove container {} after start failure: {remove_err}",
                                container_id
                            );
                        }
                    }
                    let err = self
                        .mark_task_failed(working, wrap_start_error(&task_name, err))
                        .await;
                    return Err(err);
                }
            }
        }

        {
            let mut guard = self.local_containers.lock().await;
            guard.insert(working.id, container_id.clone());
        }

        working.state = ContainerState::Running;
        working.created_at = Utc::now().to_rfc3339();
        working.node_id = self.local_node_id;
        working.node_name = self.local_node_name.clone();

        if let Err(err) = self.persist_spec(&working).await {
            warn!(
                target: "task",
                "failed to persist running state for task {}: {err}",
                working.id
            );
            if let Err(stop_err) = self
                .container_manager
                .stop_container(&container_id, Some(Duration::from_secs(10)))
                .await
            {
                warn!(
                    target: "task",
                    "failed to stop container {} during rollback: {stop_err}",
                    container_id
                );
            }
            if let Err(remove_err) = self
                .container_manager
                .remove_container(&container_id, true, true)
                .await
            {
                warn!(
                    target: "task",
                    "failed to remove container {} during rollback: {remove_err}",
                    container_id
                );
            }
            let err = err.context("task state commit failed after container launch");
            let err = self.mark_task_failed(working, err).await;
            return Err(err);
        }

        if let Err(err) = self
            .enqueue_gossip(TaskEvent::Upsert(working.clone()))
            .await
        {
            warn!(
                target: "task",
                "failed to enqueue task gossip for {}: {err}",
                working.name
            );
        }

        Ok(())
    }

    async fn resolve_existing_container_id(
        &self,
        container_name: &str,
    ) -> Result<Option<String>, ContainerError> {
        match self
            .container_manager
            .inspect_container(container_name)
            .await
        {
            Ok(info) => {
                let raw = info.id.unwrap_or_else(|| container_name.to_string());
                Ok(Some(raw.trim_start_matches('/').to_string()))
            }
            Err(ContainerError::NotFound(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn ensure_task_stopped(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        if matches!(spec.state, ContainerState::Stopped) {
            self.local_containers.lock().await.remove(&spec.id);
            return Ok(());
        }

        let has_container = {
            let guard = self.local_containers.lock().await;
            guard.contains_key(&spec.id)
        };

        if !has_container {
            return Ok(());
        }

        let _ = self.perform_local_stop(spec).await?;
        Ok(())
    }

    async fn reconcile_local_task(&self, spec: TaskSpec) -> Result<(), anyhow::Error> {
        match spec.state {
            ContainerState::Pending | ContainerState::Creating => {
                self.ensure_task_running(spec).await
            }
            ContainerState::Running => {
                self.ensure_local_tracking(&spec).await;
                Ok(())
            }
            ContainerState::Stopping | ContainerState::Stopped => {
                self.ensure_task_stopped(spec).await
            }
            ContainerState::Paused
            | ContainerState::Failed
            | ContainerState::Exited(_)
            | ContainerState::Unknown => {
                self.local_containers.lock().await.remove(&spec.id);
                Ok(())
            }
        }
    }

    async fn load_spec(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow::anyhow!("task lookup failed: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("unknown task {id}"))?;

        let value = snapshot
            .as_slice()
            .last()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("task {id} has no value"))?;

        Ok(value_to_spec(id, value))
    }

    pub async fn task_owned_locally(&self, id: Uuid) -> Result<bool, anyhow::Error> {
        let spec = self.load_spec(id).await?;
        Ok(spec.node_id == self.local_node_id)
    }

    pub async fn stop_task(&self, id: Uuid) -> Result<TaskSpec, anyhow::Error> {
        let spec = self.load_spec(id).await?;

        if spec.node_id != self.local_node_id {
            if matches!(
                spec.state,
                ContainerState::Stopping | ContainerState::Stopped
            ) {
                return Ok(spec);
            }

            let mut updated = spec.clone();
            updated.state = ContainerState::Stopping;
            self.persist_spec(&updated).await?;
            self.enqueue_gossip(TaskEvent::Upsert(updated.clone()))
                .await?;
            return Ok(updated);
        }

        self.perform_local_stop(spec).await
    }

    async fn record_gossip_id(&self, id: Uuid) -> bool {
        let mut guard = self.seen_ids.lock().await;
        guard.insert(id)
    }

    pub async fn run(&mut self) {
        while let Ok(message) = self.rx.recv().await {
            match message {
                Message::Task { id, event } => {
                    if !self.record_gossip_id(id).await {
                        continue;
                    }
                    if let Err(e) = self.handle_event(event).await {
                        tracing::error!(target: "task", "failed to handle task event: {e}");
                    }
                }
                Message::Void { .. } => {}
                _ => {}
            }
        }
    }

    async fn handle_event(&self, event: TaskEvent) -> Result<(), anyhow::Error> {
        match event {
            TaskEvent::Upsert(spec) => {
                let belongs = spec.node_id == self.local_node_id;
                self.persist_spec(&spec).await?;

                if belongs {
                    let manager = self.clone();
                    let spec_for_reconcile = spec.clone();
                    tokio::task::spawn_local(async move {
                        if let Err(err) = manager
                            .reconcile_local_task(spec_for_reconcile.clone())
                            .await
                        {
                            warn!(
                                target: "task",
                                "failed to reconcile task {}: {err}",
                                spec_for_reconcile.id
                            );
                        }
                    });
                } else if !matches!(spec.state, ContainerState::Running) {
                    self.local_containers.lock().await.remove(&spec.id);
                }

                Ok(())
            }
            TaskEvent::Remove { id } => {
                self.local_containers.lock().await.remove(&id);
                self.remove_spec(id).await
            }
        }
    }
}

fn wrap_create_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("docker create failed for task {}", task_name))
}

fn wrap_existing_inspect_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!(
        "failed to inspect existing container for task {} after name conflict",
        task_name
    ))
}

fn wrap_start_error(task_name: &str, err: ContainerError) -> anyhow::Error {
    anyhow::Error::new(err).context(format!("docker start failed for task {}", task_name))
}

fn is_name_conflict(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 409
    )
}

fn container_already_running(err: &ContainerError) -> bool {
    matches!(
        err,
        ContainerError::DockerAPI(BollardError::DockerResponseServerError { status_code, .. })
            if *status_code == 304
    )
}

fn value_to_spec(id: Uuid, value: TaskValue) -> TaskSpec {
    let mut slot_ids = value.slot_ids;
    if slot_ids.is_empty() {
        if let Some(slot_id) = value.slot_id {
            slot_ids.push(slot_id);
        }
    }
    let slot_id = slot_ids.first().copied();

    TaskSpec {
        id,
        name: value.name,
        image: value.image,
        state: value.state,
        created_at: value.created_at,
        command: value.command,
        node_id: value.node_id,
        node_name: value.node_name,
        slot_ids,
        slot_id,
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        restart_policy: value.restart_policy,
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
    use crate::store::task_store::open_task_store;
    use crate::task::types::{TaskStateKind, TaskValue};
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
        limits: Arc<AsyncMutex<Vec<crate::task::docker::ResourceLimits>>>,
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
            _restart_policy: Option<crate::task::docker::RestartPolicyConfig>,
            resource_limits: crate::task::docker::ResourceLimits,
        ) -> crate::task::docker::ContainerResult<String> {
            let mut guard = self.created.lock().await;
            let id = format!("container-{}", guard.len());
            guard.push(id.clone());
            self.limits.lock().await.push(resource_limits);
            Ok(id)
        }

        async fn start_container(
            &self,
            _container_id: &str,
        ) -> crate::task::docker::ContainerResult<()> {
            Ok(())
        }

        async fn stop_container(
            &self,
            container_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> crate::task::docker::ContainerResult<()> {
            self.stopped.lock().await.push(container_id.to_string());
            Ok(())
        }

        async fn restart_container(
            &self,
            _container_id: &str,
            _timeout: Option<std::time::Duration>,
        ) -> crate::task::docker::ContainerResult<()> {
            Ok(())
        }

        async fn remove_container(
            &self,
            _container_id: &str,
            _force: bool,
            _remove_volumes: bool,
        ) -> crate::task::docker::ContainerResult<()> {
            Ok(())
        }

        async fn list_containers(
            &self,
            _filters: Option<HashMap<String, Vec<String>>>,
        ) -> crate::task::docker::ContainerResult<Vec<crate::task::docker::ContainerInfo>> {
            Ok(Vec::new())
        }

        async fn inspect_container(
            &self,
            _container_id: &str,
        ) -> crate::task::docker::ContainerResult<bollard::service::ContainerInspectResponse>
        {
            Err(crate::task::docker::ContainerError::OperationFailed(
                "inspect unsupported in mock".into(),
            ))
        }

        async fn pull_image(&self, _image: &str) -> crate::task::docker::ContainerResult<()> {
            Ok(())
        }
    }

    fn temp_db(prefix: &str) -> (Arc<redb::Database>, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join(format!("{prefix}-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(path).expect("create db"));
        (db, dir)
    }

    async fn setup_manager() -> (TaskManager, Rc<Scheduler>, Arc<MockContainerManager>) {
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

        let (task_db, _wd) = temp_db("task");
        let task_store = open_task_store(task_db, actor).expect("open task store");

        let mock_manager = Arc::new(MockContainerManager::default());
        let (tx, rx) = bounded(4);
        let container_manager: Arc<dyn ContainerManager + Send + Sync> = mock_manager.clone();
        let manager = TaskManager::new(
            task_store,
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
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let spec = manager
            .start_container(
                "svc",
                "image",
                vec!["--arg".into()],
                500,
                256 * 1_024 * 1_024,
                None,
            )
            .await
            .expect("start container");

        assert_eq!(spec.cpu_millis, 500);
        assert_eq!(spec.memory_bytes, 256 * 1_024 * 1_024);
        assert_eq!(spec.slot_ids.len(), 1);
        let slot_id = *spec.slot_ids.first().expect("slot assigned");
        assert_eq!(spec.slot_id, Some(slot_id));

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let slot = snapshot
            .slots
            .iter()
            .find(|s| s.slot_id == slot_id)
            .expect("slot exists");
        assert!(matches!(slot.state, SlotState::Reserved(_)));

        let limits = mock_cm.limits.lock().await;
        let recorded = limits.last().expect("resource limits recorded");
        assert_eq!(recorded.memory_bytes, Some((256 * 1_024 * 1_024) as i64));
        assert_eq!(recorded.nano_cpus, Some(500_000_000));
        assert_eq!(recorded.cpu_shares, Some(512));
    }

    #[tokio::test]
    async fn start_container_reserves_multiple_slots_when_needed() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let spec = manager
            .start_container(
                "svc-multi",
                "image",
                vec![],
                1_500,
                1_536 * 1_024 * 1_024,
                None,
            )
            .await
            .expect("start container with multi-slot request");

        assert_eq!(spec.slot_ids.len(), 2, "expected two slots to be reserved");
        assert_eq!(spec.cpu_millis, 1_500);
        assert_eq!(spec.memory_bytes, 1_536 * 1_024 * 1_024);
        assert_eq!(spec.slot_id, spec.slot_ids.first().copied());

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        let reserved: Vec<_> = snapshot
            .slots
            .iter()
            .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
            .collect();
        assert_eq!(reserved.len(), 2);

        for slot in reserved {
            assert!(spec.slot_ids.contains(&slot.slot_id));
        }

        let limits = mock_cm.limits.lock().await;
        let recorded = limits.last().expect("resource limits recorded");
        assert_eq!(recorded.memory_bytes, Some((1_536 * 1_024 * 1_024) as i64));
        assert_eq!(recorded.nano_cpus, Some(1_500_000_000));
        assert_eq!(recorded.cpu_shares, Some(1_536));
    }

    #[tokio::test]
    async fn stop_task_releases_slot_and_clears_resources() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024, None)
            .await
            .expect("start container");

        let slot_id = *spec.slot_ids.first().expect("slot assigned");
        let stopped = manager.stop_task(spec.id).await.expect("stop task");

        assert!(matches!(stopped.state, ContainerState::Stopped));
        let stopped_containers = mock_cm.stopped.lock().await.clone();
        assert_eq!(stopped_containers, vec!["container-0".to_string()]);

        assert!(stopped.slot_ids.is_empty());
        assert_eq!(stopped.slot_id, None);
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
    async fn stop_task_uses_container_name_when_cache_missing() {
        let (manager, _scheduler, mock_cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024, None)
            .await
            .expect("start container");

        {
            manager.local_containers.lock().await.clear();
        }

        {
            mock_cm.stopped.lock().await.clear();
        }

        manager
            .stop_task(spec.id)
            .await
            .expect("stop task with fallback");

        let expected = format!("mantissa-{}", spec.id);
        let stopped_containers = mock_cm.stopped.lock().await.clone();
        assert_eq!(stopped_containers, vec![expected]);
    }

    #[tokio::test]
    async fn list_tasks_respects_filters() {
        let (manager, _scheduler, _mock_cm) = setup_manager().await;

        let spec = manager
            .start_container("svc", "image", vec![], 500, 256 * 1_024 * 1_024, None)
            .await
            .expect("start container");

        let active = manager
            .list_tasks(&TaskStateFilter::active_only())
            .await
            .expect("list active");
        assert_eq!(active.len(), 1);
        assert!(matches!(active[0].state, ContainerState::Running));

        manager.stop_task(spec.id).await.expect("stop task");

        let active_only = manager
            .list_tasks(&TaskStateFilter::active_only())
            .await
            .expect("list active after stop");
        assert!(active_only.is_empty());

        let with_stopped = manager
            .list_tasks(&TaskStateFilter::new([
                TaskStateKind::Pending,
                TaskStateKind::Creating,
                TaskStateKind::Running,
                TaskStateKind::Stopping,
                TaskStateKind::Stopped,
            ]))
            .await
            .expect("list active with stopped");
        assert_eq!(with_stopped.len(), 1);
        assert!(matches!(with_stopped[0].state, ContainerState::Stopped));

        let all = manager
            .list_tasks(&TaskStateFilter::all())
            .await
            .expect("list all");

        let only_stopped = manager
            .list_tasks(&TaskStateFilter::new([TaskStateKind::Stopped]))
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
            .start_container("svc", "image", vec![], 2_000, 512 * 1_024 * 1_024, None)
            .await
            .expect_err("reservation should fail");
        assert!(
            err.chain()
                .any(|cause| cause.to_string().contains("scheduler reservation failed"))
        );
    }

    #[tokio::test]
    async fn start_tasks_batch_reserves_every_slot() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let specs = manager
            .start_tasks_batch(vec![
                TaskStartRequest {
                    name: "svc-a".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 400,
                    memory_bytes: 128 * 1_024 * 1_024,
                    id: None,
                    slot_ids: Vec::new(),
                    restart_policy: None,
                },
                TaskStartRequest {
                    name: "svc-b".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                    id: None,
                    slot_ids: Vec::new(),
                    restart_policy: None,
                },
            ])
            .await
            .expect("batch start");

        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].cpu_millis, 400);
        assert_eq!(specs[1].cpu_millis, 200);
        assert_eq!(specs[0].memory_bytes, 128 * 1_024 * 1_024);
        assert_eq!(specs[1].memory_bytes, 64 * 1_024 * 1_024);
        assert_eq!(specs[0].slot_ids.len(), 1);
        assert_eq!(specs[1].slot_ids.len(), 1);
        assert_eq!(specs[0].slot_id, specs[0].slot_ids.first().copied());
        assert_eq!(specs[1].slot_id, specs[1].slot_ids.first().copied());

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
    async fn start_tasks_batch_respects_existing_reservations() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        let task_id = Uuid::new_v4();
        let slot_id = 0;

        scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id,
                    owner: manager.local_node_id,
                    task_id: Some(task_id),
                }],
            )
            .await
            .expect("pre-reserve slot");

        let specs = manager
            .start_tasks_batch(vec![TaskStartRequest {
                name: "svc-pre".into(),
                image: "img".into(),
                command: vec![],
                cpu_millis: 200,
                memory_bytes: 64 * 1_024 * 1_024,
                id: Some(task_id),
                slot_ids: vec![slot_id],
                restart_policy: None,
            }])
            .await
            .expect("start with pre-reserved slot");

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].slot_ids, vec![slot_id]);
        assert_eq!(specs[0].cpu_millis, 200);
        assert_eq!(specs[0].memory_bytes, 64 * 1_024 * 1_024);
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
                assert_eq!(reservation.task_id, Some(task_id));
            }
            _ => unreachable!("slot should be reserved"),
        }

        let created = mock_cm.created.lock().await.clone();
        assert_eq!(created.len(), 1);
    }

    #[tokio::test]
    async fn task_owned_locally_detects_remote_entries() {
        let (manager, _scheduler, _mock_cm) = setup_manager().await;

        let local_spec = manager
            .start_container("local", "img", vec![], 200, 64 * 1_024 * 1_024, None)
            .await
            .expect("start local task");

        assert!(
            manager
                .task_owned_locally(local_spec.id)
                .await
                .expect("local ownership check")
        );

        let remote_id = Uuid::new_v4();
        let remote_value = TaskValue::new(
            remote_id,
            "remote",
            "img",
            ContainerState::Running,
            Utc::now().to_rfc3339(),
            vec![],
            Uuid::new_v4(),
            "remote-node",
            vec![1],
            100,
            64 * 1_024 * 1_024,
        );

        let store = manager.store.clone();
        store
            .upsert(&UuidKey::from(remote_id), remote_value)
            .await
            .expect("insert remote task value");

        assert!(
            !manager
                .task_owned_locally(remote_id)
                .await
                .expect("remote ownership check")
        );
    }

    #[tokio::test]
    async fn start_tasks_batch_is_atomic_on_capacity_failure() {
        let (manager, scheduler, mock_cm) = setup_manager().await;

        manager
            .start_container("baseline", "img", vec![], 400, 128 * 1_024 * 1_024, None)
            .await
            .expect("pre-existing container");

        let created_before = mock_cm.created.lock().await.len();

        let err = manager
            .start_tasks_batch(vec![
                TaskStartRequest {
                    name: "svc-c".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                    id: None,
                    slot_ids: Vec::new(),
                    restart_policy: None,
                },
                TaskStartRequest {
                    name: "svc-d".into(),
                    image: "img".into(),
                    command: vec![],
                    cpu_millis: 200,
                    memory_bytes: 64 * 1_024 * 1_024,
                    id: None,
                    slot_ids: Vec::new(),
                    restart_policy: None,
                },
            ])
            .await
            .expect_err("batch should fail when capacity is insufficient");

        assert!(
            err.chain()
                .any(|cause| cause.to_string().contains("scheduler reservation failed"))
        );

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
