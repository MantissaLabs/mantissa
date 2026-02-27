use crate::services::types::{ServiceSpecValue, ServiceTaskSpecValue};
use std::collections::HashMap;
use uuid::Uuid;

/// Unique identifier for a service replica slot used to coordinate per-slot reconciliation.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct SlotKey {
    service_id: Uuid,
    template: String,
    replica: u16,
}

impl SlotKey {
    /// Builds a slot key from a service and replica identity for local tracking.
    pub(super) fn new(service_id: Uuid, template: &str, replica: u16) -> Self {
        Self {
            service_id,
            template: template.to_string(),
            replica,
        }
    }
}

/// Desired service slot projection used by reconciliation loops.
#[derive(Clone, Debug)]
pub(super) struct ReplicaSlot {
    pub(super) template: ServiceTaskSpecValue,
    pub(super) replica: u16,
    pub(super) task_id: Option<Uuid>,
}

/// Expands the service spec into an ordered list of desired replica slots.
pub(super) fn build_replica_slots(spec: &ServiceSpecValue) -> Vec<ReplicaSlot> {
    let mut slots = Vec::new();
    let mut cursor = 0usize;

    for template in &spec.tasks {
        for replica in 1..=template.replicas {
            let task_id = spec.task_ids.get(cursor).copied();
            slots.push(ReplicaSlot {
                template: template.clone(),
                replica,
                task_id,
            });
            cursor += 1;
        }
    }

    slots
}

/// Computes deterministic target nodes for every replica slot to keep placement balanced.
pub(super) fn compute_slot_targets(
    service_id: Uuid,
    templates: &[ServiceTaskSpecValue],
    eligible_nodes: &[Uuid],
) -> HashMap<SlotKey, Uuid> {
    let mut targets = HashMap::new();
    if eligible_nodes.is_empty() {
        return targets;
    }

    let total_replicas: usize = templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum();
    let service_max = max_replicas_per_node(total_replicas, eligible_nodes.len());
    let mut template_caps: HashMap<String, usize> = HashMap::new();
    for template in templates {
        template_caps.insert(
            template.name.clone(),
            max_replicas_per_node(template.replicas as usize, eligible_nodes.len()),
        );
    }

    let mut slots: Vec<(ServiceTaskSpecValue, u16)> = Vec::new();
    for template in templates {
        for replica in 1..=template.replicas {
            slots.push((template.clone(), replica));
        }
    }
    slots.sort_by(|(left, left_replica), (right, right_replica)| {
        left.name
            .cmp(&right.name)
            .then(left_replica.cmp(right_replica))
    });

    let mut total_counts: HashMap<Uuid, usize> = HashMap::new();
    let mut template_counts: HashMap<(Uuid, String), usize> = HashMap::new();

    for (template, replica) in slots {
        let key = SlotKey::new(service_id, &template.name, replica);
        let ranked = rank_nodes_for_slot(service_id, &template.name, replica, eligible_nodes);
        let template_cap = template_caps
            .get(&template.name)
            .copied()
            .unwrap_or(service_max);

        // Prefer nodes that satisfy both template and service caps; relax template caps if needed.
        let mut chosen: Option<Uuid> = None;
        for node_id in &ranked {
            let total = total_counts.get(node_id).copied().unwrap_or(0);
            if total >= service_max {
                continue;
            }
            let template_key = (*node_id, template.name.clone());
            let template_total = template_counts.get(&template_key).copied().unwrap_or(0);
            if template_total >= template_cap {
                continue;
            }
            chosen = Some(*node_id);
            break;
        }

        if chosen.is_none() {
            for node_id in &ranked {
                let total = total_counts.get(node_id).copied().unwrap_or(0);
                if total < service_max {
                    chosen = Some(*node_id);
                    break;
                }
            }
        }

        let Some(node_id) = chosen.or_else(|| ranked.first().copied()) else {
            continue;
        };

        *total_counts.entry(node_id).or_insert(0) += 1;
        let template_key = (node_id, template.name.clone());
        *template_counts.entry(template_key).or_insert(0) += 1;
        targets.insert(key, node_id);
    }

    targets
}

/// Produces a stable ordering of candidate nodes for a replica slot using rendezvous hashing.
pub(super) fn rank_nodes_for_slot(
    service_id: Uuid,
    template: &str,
    replica: u16,
    candidates: &[Uuid],
) -> Vec<Uuid> {
    let mut scored: Vec<(Uuid, u128)> = candidates
        .iter()
        .map(|node_id| {
            (
                *node_id,
                rendezvous_score(service_id, template, replica, *node_id),
            )
        })
        .collect();
    scored.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    scored.into_iter().map(|(node_id, _)| node_id).collect()
}

/// Computes the maximum number of replicas a node should hold for even distribution.
fn max_replicas_per_node(replicas: usize, node_count: usize) -> usize {
    if node_count == 0 {
        return 0;
    }
    replicas.div_ceil(node_count)
}

/// Computes the rendezvous hash score for a node given a replica identity.
fn rendezvous_score(service_id: Uuid, template: &str, replica: u16, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(service_id.as_bytes());
    hasher.update(template.as_bytes());
    hasher.update(&replica.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Selects the deterministic owner node for a replica slot so rescheduling is distributed.
pub(super) fn select_slot_owner(
    service_id: Uuid,
    template: &str,
    replica: u16,
    candidates: &[Uuid],
) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = slot_owner_score(service_id, template, replica, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => {
                best = Some((*node_id, score));
            }
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Picks the cleanup owner for an extra task so only one node prunes it.
pub(super) fn select_task_owner(task_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = task_owner_score(task_id, *node_id);
        match best {
            None => best = Some((*node_id, score)),
            Some((_, best_score)) if score > best_score => {
                best = Some((*node_id, score));
            }
            _ => {}
        }
    }
    best.map(|(node_id, _)| node_id)
}

/// Computes the rendezvous score for slot ownership selection.
fn slot_owner_score(service_id: Uuid, template: &str, replica: u16, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"owner");
    hasher.update(service_id.as_bytes());
    hasher.update(template.as_bytes());
    hasher.update(&replica.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Computes the rendezvous score used to choose the cleanup owner for extra tasks.
fn task_owner_score(task_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"cleanup");
    hasher.update(task_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}
