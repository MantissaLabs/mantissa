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

/// A bounded group of nodes whose tasks will be batched together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ServiceDeploymentGroup {
    /// Nodes grouped before their tasks are divided into bounded requests.
    pub(super) target_node_ids: Vec<Uuid>,
}

/// Stores the nodes allowed to coordinate one deployment.
///
/// Coordinator selection runs once per final task batch, but the eligible nodes
/// do not change between batches. This type builds the membership set only once.
pub(super) struct DeploymentCoordinatorSelector<'a> {
    /// Nodes currently allowed to send start requests for this deployment.
    eligible_nodes: &'a [Uuid],
    /// Set used to quickly test whether a node may coordinate.
    eligible_node_ids: HashSet<Uuid>,
}

impl<'a> DeploymentCoordinatorSelector<'a> {
    /// Builds the membership lookup shared by all task batches in one launch.
    pub(super) fn new(eligible_nodes: &'a [Uuid]) -> Self {
        Self {
            eligible_nodes,
            eligible_node_ids: eligible_nodes.iter().copied().collect(),
        }
    }

    /// Chooses the coordinator for one task batch.
    ///
    /// It first considers eligible nodes in `target_node_ids`. Such a coordinator
    /// can start its own tasks locally. If that list has no eligible node, the
    /// method chooses from all eligible nodes.
    pub(super) fn select(
        &self,
        service_id: Uuid,
        service_epoch: u64,
        batch_index: usize,
        target_node_ids: &[Uuid],
    ) -> Option<Uuid> {
        select_highest_scoring_coordinator(
            service_id,
            service_epoch,
            batch_index,
            target_node_ids
                .iter()
                .copied()
                .filter(|node_id| self.eligible_node_ids.contains(node_id)),
        )
        .or_else(|| {
            select_highest_scoring_coordinator(
                service_id,
                service_epoch,
                batch_index,
                self.eligible_nodes.iter().copied(),
            )
        })
    }
}

/// Expands the service spec into an ordered list of desired replica slots.
pub(super) fn build_replica_slots(spec: &ServiceSpecValue) -> Vec<ReplicaSlot> {
    let mut slots = Vec::new();
    let mut cursor = 0usize;

    for template in &spec.task_templates {
        for replica in 1..=template.replicas {
            let replica_id = spec.assigned_replica_id(cursor);
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

    // Slot planning only reads templates. Borrow them instead of cloning the
    // complete execution spec once for every replica.
    let mut slots: Vec<(&TaskTemplateSpecValue, u16)> = Vec::new();
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
            slots.push((template, replica));
        }
    }
    slots.sort_by(|(left, left_replica), (right, right_replica)| {
        left.name
            .cmp(&right.name)
            .then(left_replica.cmp(right_replica))
    });

    let mut total_counts: HashMap<Uuid, usize> = HashMap::new();
    // Template names come from `task_templates` and remain valid throughout
    // planning. Borrow them so candidate checks do not allocate new Strings.
    let mut template_counts: HashMap<(Uuid, &str), usize> = HashMap::new();
    let mut preference_inventory = existing_preferences.clone();

    for (template, replica) in slots {
        let key = SlotKey::new(service_id, &template.name, replica);
        let candidates = template_candidates
            .get(&template.name)
            .map(Vec::as_slice)
            .unwrap_or(eligible_nodes);
        let strategy = template.placement().strategy;
        let context = SlotTargetingContext {
            service_id,
            service_name,
            template_name: &template.name,
            replica,
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
        let Some(node_id) = choose_slot_target(&context, strategy, candidates, spread_limits)
        else {
            continue;
        };

        *total_counts.entry(node_id).or_insert(0) += 1;
        let template_key = (node_id, template.name.as_str());
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
    candidates: &[Uuid],
    spread_limits: SpreadLimits,
) -> Option<Uuid> {
    match strategy {
        PlacementStrategy::Spread => choose_spread_slot_target(context, candidates, spread_limits),
        PlacementStrategy::Binpack => choose_binpack_slot_target(context, candidates),
    }
}

/// Ranked metadata snapshot for one candidate node while choosing a service replica slot.
#[derive(Clone, Copy, Debug)]
struct SlotTargetCandidate {
    node_id: Uuid,
    rendezvous_score: u128,
    total_count: usize,
    template_count: usize,
    preference_counts: PlacementPreferenceCounts,
}

/// Shared immutable inputs reused while evaluating one replica slot target.
struct SlotTargetingContext<'a> {
    service_id: Uuid,
    service_name: &'a str,
    template_name: &'a str,
    replica: u16,
    preferences: &'a [ServicePlacementPreference],
    total_counts: &'a HashMap<Uuid, usize>,
    template_counts: &'a HashMap<(Uuid, &'a str), usize>,
    preference_inventory: &'a PlacementPreferenceInventory,
}

/// Spread-specific balancing bounds for one candidate set.
#[derive(Clone, Copy)]
struct SpreadLimits {
    service_max: usize,
    template_cap: usize,
}

/// Builds the candidate metadata needed by spread and binpack slot selection.
fn slot_target_candidate(context: &SlotTargetingContext<'_>, node_id: Uuid) -> SlotTargetCandidate {
    SlotTargetCandidate {
        node_id,
        rendezvous_score: rendezvous_score(
            context.service_id,
            context.template_name,
            context.replica,
            node_id,
        ),
        total_count: context.total_counts.get(&node_id).copied().unwrap_or(0),
        template_count: context
            .template_counts
            .get(&(node_id, context.template_name))
            .copied()
            .unwrap_or(0),
        preference_counts: if context.preferences.is_empty() {
            PlacementPreferenceCounts::default()
        } else {
            context.preference_inventory.counts_for(
                node_id,
                context.service_name,
                context.template_name,
            )
        },
    }
}

/// Returns true when rendezvous hashing ranks the new candidate first.
///
/// The lower UUID resolves the unlikely case where two nodes have the same
/// score. This preserves the order previously produced by sorting every node.
fn rendezvous_prefers_candidate(candidate: SlotTargetCandidate, best: SlotTargetCandidate) -> bool {
    candidate.rendezvous_score > best.rendezvous_score
        || (candidate.rendezvous_score == best.rendezvous_score && candidate.node_id < best.node_id)
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

    rendezvous_prefers_candidate(candidate, best)
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

    rendezvous_prefers_candidate(candidate, best)
}

/// Chooses one slot target while keeping both service-wide and template-local replica counts even.
///
/// Spread tries to keep the overall service balanced first, while also avoiding
/// concentrating too many replicas from the same template onto one node. When
/// those two goals conflict, service-wide balance wins because it affects the
/// entire deployment shape rather than a single template slice.
fn choose_spread_slot_target(
    context: &SlotTargetingContext<'_>,
    candidates: &[Uuid],
    limits: SpreadLimits,
) -> Option<Uuid> {
    if !context.preferences.is_empty() {
        // Explicit soft preferences should outrank spread's balancing caps. The spread
        // comparator still uses service/template load as a secondary signal, so this path
        // keeps the deployment stable without forcing operators to switch strategies just
        // to co-locate or separate replicas deliberately.
        let mut best: Option<SlotTargetCandidate> = None;

        for node_id in candidates.iter().copied() {
            let candidate = slot_target_candidate(context, node_id);

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
    let mut best_without_capacity: Option<SlotTargetCandidate> = None;

    for node_id in candidates.iter().copied() {
        let candidate = slot_target_candidate(context, node_id);
        let service_has_capacity = candidate.total_count < limits.service_max;
        let template_has_capacity = candidate.template_count < limits.template_cap;

        match best_without_capacity {
            None => best_without_capacity = Some(candidate),
            Some(best) if rendezvous_prefers_candidate(candidate, best) => {
                best_without_capacity = Some(candidate);
            }
            _ => {}
        }

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
        // Hard constraints can leave every candidate above the preferred
        // spread cap. A valid constrained target is better than no target.
        .or(best_without_capacity)
        .map(|candidate| candidate.node_id)
}

/// Chooses one slot target by reusing the fullest matching node before opening a new node.
///
/// Binpack intentionally inverts spread's balancing goal. It prefers the node
/// that already carries the most replicas for this service, then the most
/// replicas for this template, and only falls back to rendezvous rank when the
/// current packing level is identical.
fn choose_binpack_slot_target(
    context: &SlotTargetingContext<'_>,
    candidates: &[Uuid],
) -> Option<Uuid> {
    let mut best: Option<SlotTargetCandidate> = None;

    for node_id in candidates.iter().copied() {
        let candidate = slot_target_candidate(context, node_id);

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

/// Selects the deterministic autoscale owner for one service.
///
/// Autoscale ownership intentionally excludes the service epoch so the owner
/// remains stable across the generation updates produced by scale decisions.
pub(crate) fn select_autoscale_owner(service_id: Uuid, candidates: &[Uuid]) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = autoscale_owner_score(service_id, *node_id);
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

/// Divides target nodes into groups of at most `max_nodes_per_group` nodes.
///
/// Unsorted input is normalized so group membership does not depend on input
/// order. Already sorted production input avoids another sort. This step does
/// not choose coordinators because task limits may split a group later.
pub(super) fn build_service_deployment_groups(
    target_node_ids: &[Uuid],
    max_nodes_per_group: usize,
) -> Vec<ServiceDeploymentGroup> {
    if max_nodes_per_group == 0 || target_node_ids.is_empty() {
        return Vec::new();
    }

    let mut targets = target_node_ids.to_vec();
    if !targets.is_sorted() {
        targets.sort_unstable();
    }
    targets.dedup();

    targets
        .chunks(max_nodes_per_group)
        .map(|group_nodes| ServiceDeploymentGroup {
            target_node_ids: group_nodes.to_vec(),
        })
        .collect()
}

/// Returns the candidate with the highest rendezvous score.
///
/// A score collision is unlikely but possible. Choosing the lower UUID on a tie
/// makes the result independent of input order without sorting the candidates.
fn select_highest_scoring_coordinator(
    service_id: Uuid,
    service_epoch: u64,
    batch_index: usize,
    candidates: impl IntoIterator<Item = Uuid>,
) -> Option<Uuid> {
    let mut best: Option<(Uuid, u128)> = None;
    for node_id in candidates {
        let score = deployment_coordinator_score(service_id, service_epoch, batch_index, node_id);
        match best {
            None => best = Some((node_id, score)),
            Some((best_node_id, best_score))
                if score > best_score || (score == best_score && node_id < best_node_id) =>
            {
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

/// Computes the rendezvous score used to choose one autoscale owner for a service.
fn autoscale_owner_score(service_id: Uuid, node_id: Uuid) -> u128 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"autoscale");
    hasher.update(service_id.as_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

/// Computes the rendezvous score used to choose a deployment coordinator.
fn deployment_coordinator_score(
    service_id: Uuid,
    service_epoch: u64,
    batch_index: usize,
    node_id: Uuid,
) -> u128 {
    let mut hasher = blake3::Hasher::new();
    // This label is part of the hash input. Changing it would reassign groups.
    hasher.update(b"deployment-shard");
    hasher.update(service_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());
    hasher.update(&batch_index.to_le_bytes());
    hasher.update(node_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    u128::from_le_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        SlotKey, SlotTargetCandidate, compute_slot_targets_with_placement,
        rendezvous_prefers_candidate, select_autoscale_owner,
    };
    use crate::scheduler::placement::{
        PlacementConstraint, PlacementConstraintSelector, PlacementNode, PlacementPolicy,
        PlacementPreferenceInventory, PlacementStrategy, ServicePlacementPreference,
    };
    use crate::services::types::TaskTemplateSpecValue;
    use crate::topology::peers::PeerLabel;
    use crate::workload::types::ExecutionSpec;
    use uuid::Uuid;

    /// Autoscale owner selection should be deterministic and independent from service epochs.
    #[test]
    fn autoscale_owner_selection_is_stable_for_service() {
        let service_id = Uuid::new_v4();
        let mut candidates = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let first = select_autoscale_owner(service_id, &candidates).expect("owner");
        candidates.reverse();
        let second = select_autoscale_owner(service_id, &candidates).expect("owner");

        assert_eq!(first, second);
    }

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

    /// Binpack should keep the same result when candidate input order changes.
    #[test]
    fn binpack_slot_targets_are_deterministic() {
        let service_id = Uuid::from_u128(42);
        let candidates = (1u128..=4).map(Uuid::from_u128).collect::<Vec<_>>();
        let mut reversed = candidates.clone();
        reversed.reverse();
        let mut template = template_with_constraints("backend", 8, Vec::new());
        template.placement_preferences.clear();
        template.execution.placement.strategy = PlacementStrategy::Binpack;

        let targets = compute_slot_targets_with_placement(
            service_id,
            "demo-service",
            &[template.clone()],
            &candidates,
            &[],
            &PlacementPreferenceInventory::default(),
        )
        .expect("binpack targets");
        let reversed_targets = compute_slot_targets_with_placement(
            service_id,
            "demo-service",
            &[template],
            &reversed,
            &[],
            &PlacementPreferenceInventory::default(),
        )
        .expect("reversed binpack targets");

        assert_eq!(targets, reversed_targets);
        let first_target = targets.values().next().copied().expect("target");
        assert!(targets.values().all(|node_id| *node_id == first_target));
    }

    /// Service affinity should remain more important than rendezvous score.
    #[test]
    fn slot_targets_honor_service_affinity() {
        let service_id = Uuid::from_u128(43);
        let other = Uuid::from_u128(1);
        let preferred = Uuid::from_u128(2);
        let template = template_with_constraints("backend", 1, Vec::new());
        let mut inventory = PlacementPreferenceInventory::default();
        inventory.record_service_replica(preferred, "demo-service", "other-template");

        let targets = compute_slot_targets_with_placement(
            service_id,
            "demo-service",
            &[template],
            &[other, preferred],
            &[],
            &inventory,
        )
        .expect("affinity targets");

        assert_eq!(
            targets.get(&SlotKey::new(service_id, "backend", 1)),
            Some(&preferred)
        );
    }

    /// Hard constraints should still place replicas after the spread cap is full.
    #[test]
    fn constrained_slot_targets_fall_back_after_spread_cap() {
        let service_id = Uuid::from_u128(44);
        let selected = Uuid::from_u128(1);
        let other_nodes = [Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4)];
        let mut candidates = vec![selected];
        candidates.extend(other_nodes);
        let mut placement_nodes = vec![placement_node(selected, "west")];
        placement_nodes.extend(
            other_nodes
                .into_iter()
                .map(|node_id| placement_node(node_id, "east")),
        );
        let mut template = template_with_constraints(
            "backend",
            2,
            vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("topology.zone"),
                    "west",
                )
                .expect("west label constraint"),
            ],
        );
        template.placement_preferences.clear();

        let targets = compute_slot_targets_with_placement(
            service_id,
            "demo-service",
            &[template],
            &candidates,
            &placement_nodes,
            &PlacementPreferenceInventory::default(),
        )
        .expect("constrained targets");

        assert_eq!(targets.len(), 2);
        assert!(targets.values().all(|node_id| *node_id == selected));
    }

    /// Equal rendezvous scores should retain the old lower-UUID tie-breaker.
    #[test]
    fn rendezvous_score_ties_choose_lower_uuid() {
        let candidate = SlotTargetCandidate {
            node_id: Uuid::from_u128(1),
            rendezvous_score: 7,
            total_count: 0,
            template_count: 0,
            preference_counts: Default::default(),
        };
        let best = SlotTargetCandidate {
            node_id: Uuid::from_u128(2),
            ..candidate
        };

        assert!(rendezvous_prefers_candidate(candidate, best));
        assert!(!rendezvous_prefers_candidate(best, candidate));
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
            autoscale: None,
            depends_on: Vec::new(),
            replicas,
            readiness: None,
            public_port: None,
            public_protocol: None,
            public_ingress: Default::default(),
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
