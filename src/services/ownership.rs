use crate::scheduler::placement::{
    PlacementNode, PlacementPreferenceCounts, PlacementPreferenceInventory, PlacementStrategy,
    ServicePlacementPreference, compare_placement_preference_counts,
};
use crate::services::types::{ServiceSpecValue, TaskTemplateSpecValue};
use anyhow::anyhow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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
    pub(super) template: TaskTemplateSpecValue,
    pub(super) replica: u16,
    pub(super) replica_id: Option<Uuid>,
}

/// Deterministic target-node shard assigned to one replaceable coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ServiceDeploymentShard {
    pub(super) shard_index: usize,
    pub(super) coordinator_node_id: Uuid,
    pub(super) target_node_ids: Vec<Uuid>,
}

/// Expands the service spec into an ordered list of desired replica slots.
pub(super) fn build_replica_slots(spec: &ServiceSpecValue) -> Vec<ReplicaSlot> {
    let mut slots = Vec::new();
    let mut cursor = 0usize;

    for template in &spec.task_templates {
        for replica in 1..=template.replicas {
            let replica_id = spec.replica_ids.get(cursor).copied();
            slots.push(ReplicaSlot {
                template: template.clone(),
                replica,
                replica_id,
            });
            cursor += 1;
        }
    }

    slots
}

/// Computes deterministic target nodes for every replica slot using the default spread strategy.
#[cfg(test)]
pub(super) fn compute_slot_targets(
    service_id: Uuid,
    task_templates: &[TaskTemplateSpecValue],
    eligible_nodes: &[Uuid],
) -> HashMap<SlotKey, Uuid> {
    compute_slot_targets_with_placement(
        service_id,
        "service",
        task_templates,
        eligible_nodes,
        &[],
        &PlacementPreferenceInventory::default(),
    )
    .unwrap_or_default()
}

/// Computes deterministic target nodes while honoring each template's hard placement policy.
pub(super) fn compute_slot_targets_with_placement(
    service_id: Uuid,
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    eligible_nodes: &[Uuid],
    placement_nodes: &[PlacementNode],
    existing_preferences: &PlacementPreferenceInventory,
) -> anyhow::Result<HashMap<SlotKey, Uuid>> {
    let mut targets = HashMap::new();
    if eligible_nodes.is_empty() {
        return Ok(targets);
    }
    let eligible_node_ids: HashSet<Uuid> = eligible_nodes.iter().copied().collect();

    let total_replicas: usize = task_templates
        .iter()
        .map(|template| template.replicas as usize)
        .sum();
    let service_max = max_replicas_per_node(total_replicas, eligible_nodes.len());
    let mut template_caps: HashMap<String, usize> = HashMap::new();
    for template in task_templates {
        template_caps.insert(
            template.name.clone(),
            max_replicas_per_node(template.replicas as usize, eligible_nodes.len()),
        );
    }

    let mut slots: Vec<(TaskTemplateSpecValue, u16)> = Vec::new();
    let mut template_candidates: HashMap<String, Vec<Uuid>> = HashMap::new();
    for template in task_templates {
        let candidates = if template.placement().is_unconstrained() || placement_nodes.is_empty() {
            eligible_nodes.to_vec()
        } else {
            placement_nodes
                .iter()
                .filter(|node| {
                    eligible_node_ids.contains(&node.node_id) && template.placement().matches(node)
                })
                .map(|node| node.node_id)
                .collect()
        };
        if candidates.is_empty() {
            return Err(anyhow!(
                "task template '{}' placement constraints exclude every eligible node",
                template.name
            ));
        }
        template_candidates.insert(template.name.clone(), candidates);
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
    let mut preference_inventory = existing_preferences.clone();

    for (template, replica) in slots {
        let key = SlotKey::new(service_id, &template.name, replica);
        let candidates = template_candidates
            .get(&template.name)
            .map(Vec::as_slice)
            .unwrap_or(eligible_nodes);
        let ranked = rank_nodes_for_slot(service_id, &template.name, replica, candidates);
        let strategy = template.placement().strategy;
        let context = SlotTargetingContext {
            service_name,
            template_name: &template.name,
            preferences: template.placement_preferences(),
            total_counts: &total_counts,
            template_counts: &template_counts,
            preference_inventory: &preference_inventory,
        };
        let spread_limits = SpreadLimits {
            service_max,
            template_cap: template_caps
                .get(&template.name)
                .copied()
                .unwrap_or(service_max),
        };
        let Some(node_id) = choose_slot_target(&context, strategy, &ranked, spread_limits)
            .or_else(|| ranked.first().copied())
        else {
            continue;
        };

        *total_counts.entry(node_id).or_insert(0) += 1;
        let template_key = (node_id, template.name.clone());
        *template_counts.entry(template_key).or_insert(0) += 1;
        preference_inventory.record_service_replica(node_id, service_name, &template.name);
        targets.insert(key, node_id);
    }

    Ok(targets)
}

/// Chooses one deterministic target node for a slot using the template's ranking strategy.
fn choose_slot_target(
    context: &SlotTargetingContext<'_>,
    strategy: PlacementStrategy,
    ranked: &[Uuid],
    spread_limits: SpreadLimits,
) -> Option<Uuid> {
    match strategy {
        PlacementStrategy::Spread => choose_spread_slot_target(context, ranked, spread_limits),
        PlacementStrategy::Binpack => choose_binpack_slot_target(context, ranked),
    }
}

/// Ranked metadata snapshot for one candidate node while choosing a service replica slot.
#[derive(Clone, Copy, Debug)]
struct SlotTargetCandidate {
    node_id: Uuid,
    rank_idx: usize,
    total_count: usize,
    template_count: usize,
    preference_counts: PlacementPreferenceCounts,
}

/// Shared immutable inputs reused while evaluating one replica slot target.
struct SlotTargetingContext<'a> {
    service_name: &'a str,
    template_name: &'a str,
    preferences: &'a [ServicePlacementPreference],
    total_counts: &'a HashMap<Uuid, usize>,
    template_counts: &'a HashMap<(Uuid, String), usize>,
    preference_inventory: &'a PlacementPreferenceInventory,
}

/// Spread-specific balancing bounds for one candidate set.
#[derive(Clone, Copy)]
struct SpreadLimits {
    service_max: usize,
    template_cap: usize,
}

/// Builds the candidate metadata needed by spread and binpack slot selection.
fn slot_target_candidate(
    context: &SlotTargetingContext<'_>,
    node_id: Uuid,
    rank_idx: usize,
) -> SlotTargetCandidate {
    SlotTargetCandidate {
        node_id,
        rank_idx,
        total_count: context.total_counts.get(&node_id).copied().unwrap_or(0),
        template_count: context
            .template_counts
            .get(&(node_id, context.template_name.to_string()))
            .copied()
            .unwrap_or(0),
        preference_counts: context.preference_inventory.counts_for(
            node_id,
            context.service_name,
            context.template_name,
        ),
    }
}

/// Compares two candidate snapshots according to the declared soft placement preferences.
fn preference_ordering(
    preferences: &[ServicePlacementPreference],
    left: SlotTargetCandidate,
    right: SlotTargetCandidate,
) -> Ordering {
    compare_placement_preference_counts(
        preferences,
        left.preference_counts,
        right.preference_counts,
    )
}

/// Returns true when the new spread candidate is preferable to the current best candidate.
///
/// Spread first honors explicit soft preferences, then keeps the service-wide replica count low,
/// then keeps the template-local count low, and finally falls back to rendezvous rank to keep
/// ownership deterministic.
fn spread_prefers_candidate(
    preferences: &[ServicePlacementPreference],
    candidate: SlotTargetCandidate,
    best: SlotTargetCandidate,
) -> bool {
    let preference_cmp = preference_ordering(preferences, candidate, best);
    if preference_cmp != Ordering::Equal {
        return preference_cmp.is_gt();
    }

    let spreads_service_more_evenly = candidate.total_count < best.total_count;
    if spreads_service_more_evenly {
        return true;
    }
    let has_same_service_load = candidate.total_count == best.total_count;
    if !has_same_service_load {
        return false;
    }

    let spreads_template_more_evenly = candidate.template_count < best.template_count;
    if spreads_template_more_evenly {
        return true;
    }
    let has_same_template_load = candidate.template_count == best.template_count;
    if !has_same_template_load {
        return false;
    }

    candidate.rank_idx < best.rank_idx
}

/// Returns true when the new binpack candidate is preferable to the current best candidate.
///
/// Binpack still lets explicit soft preferences win first. Once two candidates are equally
/// preferred, it chooses the node that is already the fullest for this service, then for this
/// template, and finally uses rendezvous rank as the deterministic tie-breaker.
fn binpack_prefers_candidate(
    preferences: &[ServicePlacementPreference],
    candidate: SlotTargetCandidate,
    best: SlotTargetCandidate,
) -> bool {
    let preference_cmp = preference_ordering(preferences, candidate, best);
    if preference_cmp != Ordering::Equal {
        return preference_cmp.is_gt();
    }

    let packs_more_service_replicas = candidate.total_count > best.total_count;
    if packs_more_service_replicas {
        return true;
    }
    let has_same_service_load = candidate.total_count == best.total_count;
    if !has_same_service_load {
        return false;
    }

    let packs_more_template_replicas = candidate.template_count > best.template_count;
    if packs_more_template_replicas {
        return true;
    }
    let has_same_template_load = candidate.template_count == best.template_count;
    if !has_same_template_load {
        return false;
    }

    candidate.rank_idx < best.rank_idx
}

/// Chooses one slot target while keeping both service-wide and template-local replica counts even.
///
/// Spread tries to keep the overall service balanced first, while also avoiding
/// concentrating too many replicas from the same template onto one node. When
/// those two goals conflict, service-wide balance wins because it affects the
/// entire deployment shape rather than a single template slice.
fn choose_spread_slot_target(
    context: &SlotTargetingContext<'_>,
    ranked: &[Uuid],
    limits: SpreadLimits,
) -> Option<Uuid> {
    if !context.preferences.is_empty() {
        // Explicit soft preferences should outrank spread's balancing caps. The spread
        // comparator still uses service/template load as a secondary signal, so this path
        // keeps the deployment stable without forcing operators to switch strategies just
        // to co-locate or separate replicas deliberately.
        let mut best: Option<SlotTargetCandidate> = None;

        for (rank_idx, node_id) in ranked.iter().copied().enumerate() {
            let candidate = slot_target_candidate(context, node_id, rank_idx);

            match best {
                None => best = Some(candidate),
                Some(current_best)
                    if spread_prefers_candidate(context.preferences, candidate, current_best) =>
                {
                    best = Some(candidate);
                }
                _ => {}
            }
        }

        return best.map(|candidate| candidate.node_id);
    }

    let mut best_with_template_capacity: Option<SlotTargetCandidate> = None;
    let mut best_with_service_capacity: Option<SlotTargetCandidate> = None;

    for (rank_idx, node_id) in ranked.iter().copied().enumerate() {
        let candidate = slot_target_candidate(context, node_id, rank_idx);
        let service_has_capacity = candidate.total_count < limits.service_max;
        let template_has_capacity = candidate.template_count < limits.template_cap;

        if service_has_capacity {
            match best_with_service_capacity {
                None => best_with_service_capacity = Some(candidate),
                Some(best) if spread_prefers_candidate(context.preferences, candidate, best) => {
                    best_with_service_capacity = Some(candidate);
                }
                _ => {}
            }
        }

        if service_has_capacity && template_has_capacity {
            match best_with_template_capacity {
                None => best_with_template_capacity = Some(candidate),
                Some(best) if spread_prefers_candidate(context.preferences, candidate, best) => {
                    best_with_template_capacity = Some(candidate);
                }
                _ => {}
            }
        }
    }

    best_with_template_capacity
        .or(best_with_service_capacity)
        .map(|candidate| candidate.node_id)
}

/// Chooses one slot target by reusing the fullest matching node before opening a new node.
///
/// Binpack intentionally inverts spread's balancing goal. It prefers the node
/// that already carries the most replicas for this service, then the most
/// replicas for this template, and only falls back to rendezvous rank when the
/// current packing level is identical.
fn choose_binpack_slot_target(context: &SlotTargetingContext<'_>, ranked: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<SlotTargetCandidate> = None;

    for (rank_idx, node_id) in ranked.iter().copied().enumerate() {
        let candidate = slot_target_candidate(context, node_id, rank_idx);

        match best {
            None => best = Some(candidate),
            Some(current_best)
                if binpack_prefers_candidate(context.preferences, candidate, current_best) =>
            {
                best = Some(candidate);
            }
            _ => {}
        }
    }

    best.map(|candidate| candidate.node_id)
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

/// Selects the deterministic owner for one service generation so only one node executes rollout.
pub(crate) fn select_generation_owner(
    service_id: Uuid,
    service_epoch: u64,
    candidates: &[Uuid],
) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = generation_owner_score(service_id, service_epoch, *node_id);
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

/// Builds deterministic target-node shards for one service deployment generation.
///
/// The generation owner uses this as the only supported sharding shape for
/// large deployments: target nodes are partitioned once, and each partition is
/// assigned to a replaceable coordinator selected by rendezvous hashing. The
/// function is intentionally pure so every owner or backup can recompute the
/// same shard plan from the service generation and active eligible view.
pub(super) fn build_service_deployment_shards(
    service_id: Uuid,
    service_epoch: u64,
    eligible_nodes: &[Uuid],
    target_node_ids: &[Uuid],
    max_targets_per_shard: usize,
) -> Vec<ServiceDeploymentShard> {
    if max_targets_per_shard == 0 || eligible_nodes.is_empty() || target_node_ids.is_empty() {
        return Vec::new();
    }

    let mut eligible = eligible_nodes.to_vec();
    eligible.sort_unstable();
    eligible.dedup();
    if eligible.is_empty() {
        return Vec::new();
    }

    let mut targets = target_node_ids.to_vec();
    targets.sort_unstable();
    targets.dedup();

    targets
        .chunks(max_targets_per_shard)
        .enumerate()
        .filter_map(|(shard_index, target_chunk)| {
            let coordinator_node_id = select_shard_coordinator(
                service_id,
                service_epoch,
                shard_index,
                target_chunk,
                &eligible,
            )?;
            Some(ServiceDeploymentShard {
                shard_index,
                coordinator_node_id,
                target_node_ids: target_chunk.to_vec(),
            })
        })
        .collect()
}

/// Selects the deterministic coordinator for one deployment shard.
///
/// A coordinator inside the shard target set is preferred so the shard can
/// often handle one of its own target batches locally. If every shard target is
/// unavailable in the current eligible view, the function falls back to the
/// broader eligible set so the generation can still be delegated.
fn select_shard_coordinator(
    service_id: Uuid,
    service_epoch: u64,
    shard_index: usize,
    target_node_ids: &[Uuid],
    eligible_nodes: &[Uuid],
) -> Option<Uuid> {
    let eligible: HashSet<Uuid> = eligible_nodes.iter().copied().collect();
    let mut candidates = target_node_ids
        .iter()
        .copied()
        .filter(|node_id| eligible.contains(node_id))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        candidates.extend_from_slice(eligible_nodes);
    }
    candidates.sort_unstable();
    candidates.dedup();

    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = shard_coordinator_score(service_id, service_epoch, shard_index, node_id);
        match best {
            None => best = Some((node_id, score)),
            Some((_, best_score)) if score > best_score => {
                best = Some((node_id, score));
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

/// Computes the rendezvous score used to choose one rollout owner for a service generation.
fn generation_owner_score(service_id: Uuid, service_epoch: u64, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"generation");
    hasher.update(service_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Computes the rendezvous score used to choose one deployment shard coordinator.
fn shard_coordinator_score(
    service_id: Uuid,
    service_epoch: u64,
    shard_index: usize,
    node_id: Uuid,
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"deployment-shard");
    hasher.update(service_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());
    hasher.update(&shard_index.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::{SlotKey, compute_slot_targets_with_placement};
    use crate::scheduler::placement::{
        PlacementConstraint, PlacementConstraintSelector, PlacementNode, PlacementPolicy,
        PlacementPreferenceInventory, PlacementStrategy, ServicePlacementPreference,
    };
    use crate::services::types::TaskTemplateSpecValue;
    use crate::topology::peers::PeerLabel;
    use crate::workload::types::ExecutionSpec;
    use uuid::Uuid;

    /// Hard placement constraints should restrict deterministic slot targeting to matching nodes.
    #[test]
    fn slot_targets_honor_template_placement_constraints() {
        let service_id = Uuid::new_v4();
        let east = Uuid::new_v4();
        let west = Uuid::new_v4();
        let template = template_with_constraints(
            "backend",
            1,
            vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("topology.zone"),
                    "west",
                )
                .expect("west label constraint"),
            ],
        );
        let targets = compute_slot_targets_with_placement(
            service_id,
            "demo-service",
            &[template],
            &[east, west],
            &[placement_node(east, "east"), placement_node(west, "west")],
            &PlacementPreferenceInventory::default(),
        )
        .expect("placement-filtered slot targets should build");

        assert_eq!(
            targets.get(&SlotKey::new(service_id, "backend", 1)),
            Some(&west)
        );
    }

    /// Deterministic slot targeting should fail fast when no eligible node satisfies a template.
    #[test]
    fn slot_targets_reject_unsatisfied_template_constraints() {
        let service_id = Uuid::new_v4();
        let east = Uuid::new_v4();
        let template = template_with_constraints(
            "backend",
            1,
            vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("topology.zone"),
                    "west",
                )
                .expect("west label constraint"),
            ],
        );
        let err = compute_slot_targets_with_placement(
            service_id,
            "demo-service",
            &[template],
            &[east],
            &[placement_node(east, "east")],
            &PlacementPreferenceInventory::default(),
        )
        .expect_err("unsatisfied placement constraints should fail");

        assert!(
            err.to_string().contains("exclude every eligible node"),
            "unexpected error: {err:#}"
        );
    }

    /// Builds one task template with the provided hard placement constraints for ownership tests.
    fn template_with_constraints(
        name: &str,
        replicas: u16,
        constraints: Vec<PlacementConstraint>,
    ) -> TaskTemplateSpecValue {
        TaskTemplateSpecValue {
            name: name.to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/example/backend:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
                placement: PlacementPolicy {
                    constraints,
                    strategy: PlacementStrategy::Spread,
                },
            },
            placement_preferences: vec![ServicePlacementPreference::ServiceAffinity],
            depends_on: Vec::new(),
            replicas,
            readiness: None,
            public_port: None,
            public_protocol: None,
        }
    }

    /// Builds one scheduler-visible node record carrying a topology zone label for tests.
    fn placement_node(node_id: Uuid, zone: &str) -> PlacementNode {
        PlacementNode::new(
            node_id,
            format!("worker-{zone}"),
            "10.0.0.1:7000",
            "linux",
            "amd64",
            vec![PeerLabel {
                key: "topology.zone".to_string(),
                value: zone.to_string(),
            }],
        )
    }
}
