use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use rand::rng;
use rand::seq::SliceRandom;
use tracing::debug;
use uuid::Uuid;

use crate::scheduler::summary::SchedulerSlotState;
use crate::scheduler::{SchedulerSnapshot, SlotCapacity, SlotId, SlotState};
use crate::task::types::{TaskEnvironmentVariable, TaskRestartPolicy, TaskSecretFile};

use super::{TaskManager, TaskStartRequest};

/// Execution plan for a single local task launch, holding the target slots and container metadata.
#[derive(Clone)]
pub(super) struct BatchStartPlan {
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) command: Vec<String>,
    pub(super) slots: Vec<SlotChoice>,
    pub(super) requested_cpu_millis: u64,
    pub(super) requested_memory_bytes: u64,
    pub(super) container_name: String,
    pub(super) container_id: Option<String>,
    pub(super) created_at: DateTime<Utc>,
    pub(super) index: usize,
    pub(super) preassigned: bool,
    pub(super) restart_policy: Option<TaskRestartPolicy>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<TaskSecretFile>,
}

impl BatchStartPlan {
    /// Returns the slot identifiers that back this plan in sorted order for stable persistence.
    pub(super) fn slot_ids(&self) -> Vec<SlotId> {
        let mut ids: Vec<SlotId> = self.slots.iter().map(|slot| slot.slot_id).collect();
        ids.sort_unstable();
        ids
    }
}

#[derive(Clone)]
pub(super) struct StartIntent {
    pub(super) index: usize,
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) command: Vec<String>,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) preassigned_slots: Vec<SlotId>,
    pub(super) restart_policy: Option<TaskRestartPolicy>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<TaskSecretFile>,
}

#[derive(Clone)]
pub(super) struct SlotChoice {
    pub(super) slot_id: SlotId,
    pub(super) capacity: SlotCapacity,
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

#[derive(Clone)]
pub(super) struct RemoteStartPlan {
    pub(super) index: usize,
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) image: String,
    pub(super) command: Vec<String>,
    pub(super) cpu_millis: u64,
    pub(super) memory_bytes: u64,
    pub(super) slots: Vec<SlotChoice>,
    pub(super) peer_id: Uuid,
    pub(super) scheduler_version: u64,
    pub(super) restart_policy: Option<TaskRestartPolicy>,
    pub(super) env: Vec<TaskEnvironmentVariable>,
    pub(super) secret_files: Vec<TaskSecretFile>,
}

pub(super) struct Assignment {
    pub(super) local_version: u64,
    pub(super) local: Vec<BatchStartPlan>,
    pub(super) remote: Vec<RemoteStartPlan>,
}

impl TaskManager {
    /// Normalizes user requests into deterministic scheduling intents, applying IDs and defaults.
    pub(super) fn build_start_intents(requests: Vec<TaskStartRequest>) -> Vec<StartIntent> {
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
                env: request.env,
                secret_files: request.secret_files,
            })
            .collect()
    }

    /// Computes the full placement assignment for a batch of tasks across local and remote nodes.
    pub(super) async fn compute_assignment(
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
                env: intent.env.clone(),
                secret_files: intent.secret_files.clone(),
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
                        env: intent.env.clone(),
                        secret_files: intent.secret_files.clone(),
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
                        env: intent.env.clone(),
                        secret_files: intent.secret_files.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}
