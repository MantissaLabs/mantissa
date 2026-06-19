use super::*;
use crate::network::types::{NetworkDriver, NetworkServiceDependencyRequirement};

/// Immutable inputs used to derive deterministic service slot targets.
pub(super) struct SlotTargetContext<'a> {
    pub(super) service_name: &'a str,
    pub(super) service_id: Uuid,
    pub(super) service_epoch: u64,
    pub(super) task_templates: &'a [TaskTemplateSpecValue],
    pub(super) eligible_nodes: &'a [Uuid],
    pub(super) placement_nodes: &'a [PlacementNode],
    pub(super) preference_inventory: &'a PlacementPreferenceInventory,
    pub(super) network_registry: &'a NetworkRegistry,
    pub(super) volume_registry: &'a VolumeRegistry,
}

/// Builds the individual workload start requests for every replica defined in the service manifest.
pub(super) fn build_start_requests(
    context: SlotTargetContext<'_>,
) -> anyhow::Result<Vec<WorkloadStartRequest>> {
    let slot_targets = compute_effective_slot_targets(&context)?;
    let mut requests = Vec::new();
    for template in context.task_templates {
        for replica_idx in 0..template.replicas {
            let replica_number = replica_idx + 1;
            let desired_id = crate::services::types::derive_service_replica_id(
                context.service_id,
                context.service_epoch,
                &template.name,
                replica_number,
            );
            let key = SlotKey::new(context.service_id, &template.name, replica_number);
            let target_node = slot_targets.get(&key).copied();
            let mut request = template.replica_start_request(
                context.service_name,
                context.service_epoch,
                replica_number,
                desired_id,
                target_node,
            );
            request.dependency_requirements = dependency_requirements_for_template(
                context.service_name,
                template,
                context.task_templates,
            );
            requests.push(request);
        }
    }
    Ok(requests)
}

/// Builds workload start requests only for replicas still missing from the current manifest.
pub(super) fn build_missing_template_requests(
    service_name: &str,
    service_id: Uuid,
    service_epoch: u64,
    template: &TaskTemplateSpecValue,
    task_templates: &[TaskTemplateSpecValue],
    assignments: &BTreeMap<(String, u16), Uuid>,
    slot_targets: &HashMap<SlotKey, Uuid>,
) -> Vec<WorkloadStartRequest> {
    let mut requests = Vec::new();
    for replica in 1..=template.replicas {
        if assignments.contains_key(&(template.name.clone(), replica)) {
            continue;
        }

        let desired_id = crate::services::types::derive_service_replica_id(
            service_id,
            service_epoch,
            &template.name,
            replica,
        );
        let key = SlotKey::new(service_id, &template.name, replica);
        let target_node = slot_targets.get(&key).copied();
        let mut request = template.replica_start_request(
            service_name,
            service_epoch,
            replica,
            desired_id,
            target_node,
        );
        request.dependency_requirements =
            dependency_requirements_for_template(service_name, template, task_templates);
        requests.push(request);
    }
    requests
}

/// Builds target-admission dependency checks for upstream templates that share a network.
fn dependency_requirements_for_template(
    service_name: &str,
    template: &TaskTemplateSpecValue,
    task_templates: &[TaskTemplateSpecValue],
) -> Vec<NetworkServiceDependencyRequirement> {
    if template.depends_on.is_empty() || template.execution.networks.is_empty() {
        return Vec::new();
    }

    let downstream_networks = template
        .execution
        .networks
        .iter()
        .map(|network| network.network_id)
        .collect::<HashSet<_>>();
    let templates_by_name = task_templates
        .iter()
        .map(|candidate| (candidate.name.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    let mut requirements = Vec::new();
    for dependency_name in &template.depends_on {
        let Some(dependency) = templates_by_name.get(dependency_name.as_str()) else {
            continue;
        };
        for network in &dependency.execution.networks {
            if downstream_networks.contains(&network.network_id) {
                requirements.push(NetworkServiceDependencyRequirement {
                    network_id: network.network_id,
                    service_name: service_name.to_string(),
                    template_name: dependency.name.clone(),
                });
            }
        }
    }

    requirements.sort_by(|left, right| {
        left.network_id
            .cmp(&right.network_id)
            .then_with(|| left.service_name.cmp(&right.service_name))
            .then_with(|| left.template_name.cmp(&right.template_name))
    });
    requirements.dedup();
    requirements
}

/// Computes effective slot targets after applying any hard local-volume locality overrides.
pub(super) fn compute_effective_slot_targets(
    context: &SlotTargetContext<'_>,
) -> anyhow::Result<HashMap<SlotKey, Uuid>> {
    let mut targets = compute_slot_targets_with_placement(
        context.service_id,
        context.service_name,
        context.task_templates,
        context.eligible_nodes,
        context.placement_nodes,
        context.preference_inventory,
    )?;
    let mut hard_targets: HashMap<SlotKey, Uuid> = HashMap::new();
    for template in context.task_templates {
        let Some(target_node) =
            resolve_template_volume_target(context.volume_registry, &template.volumes)?
        else {
            continue;
        };
        for replica in 1..=template.replicas {
            let key = SlotKey::new(context.service_id, &template.name, replica);
            hard_targets.insert(key.clone(), target_node);
            targets.insert(key, target_node);
        }
    }
    apply_bridge_dependency_targets(context, &hard_targets, &mut targets)?;
    Ok(targets)
}

/// Co-locate dependent templates when their dependency edge relies on a node-local bridge network.
///
/// Bridge networks do not provide cross-node reachability. If a downstream template depends on an
/// upstream template over the same bridge network, every downstream replica must be pinned to a node
/// that also hosts one upstream replica. Conflicts with hard volume locality or placement
/// constraints fail deployment instead of silently producing unreachable service DNS answers.
fn apply_bridge_dependency_targets(
    context: &SlotTargetContext<'_>,
    hard_targets: &HashMap<SlotKey, Uuid>,
    targets: &mut HashMap<SlotKey, Uuid>,
) -> anyhow::Result<()> {
    let templates_by_name: HashMap<&str, &TaskTemplateSpecValue> = context
        .task_templates
        .iter()
        .map(|template| (template.name.as_str(), template))
        .collect();
    let mut bridge_targets: HashMap<SlotKey, Uuid> = HashMap::new();

    for _ in 0..context.task_templates.len().max(1) {
        let mut changed = false;
        for template in context.task_templates {
            for dependency_name in &template.depends_on {
                let Some(dependency) = templates_by_name.get(dependency_name.as_str()).copied()
                else {
                    continue;
                };
                if !templates_share_bridge_network(template, dependency, context.network_registry)?
                {
                    continue;
                }
                if dependency.replicas == 0 && template.replicas > 0 {
                    return Err(anyhow!(
                        "service '{}' template '{}' depends on bridge-local template '{}' but the dependency has no replicas",
                        context.service_name,
                        template.name,
                        dependency.name
                    ));
                }

                for replica in 1..=template.replicas {
                    let dependency_replica = ((replica - 1) % dependency.replicas) + 1;
                    let dependency_key =
                        SlotKey::new(context.service_id, &dependency.name, dependency_replica);
                    let Some(target_node) = targets.get(&dependency_key).copied() else {
                        return Err(anyhow!(
                            "service '{}' template '{}' depends on bridge-local template '{}' but dependency replica {} has no target node",
                            context.service_name,
                            template.name,
                            dependency.name,
                            dependency_replica
                        ));
                    };

                    if !template_can_run_on_node(
                        template,
                        target_node,
                        context.eligible_nodes,
                        context.placement_nodes,
                    ) {
                        return Err(anyhow!(
                            "service '{}' template '{}' depends on bridge-local template '{}' but placement constraints exclude dependency node {}",
                            context.service_name,
                            template.name,
                            dependency.name,
                            target_node
                        ));
                    }

                    let key = SlotKey::new(context.service_id, &template.name, replica);
                    if let Some(hard_target) = hard_targets.get(&key)
                        && *hard_target != target_node
                    {
                        return Err(anyhow!(
                            "service '{}' template '{}' replica {} cannot be co-located with bridge-local dependency '{}' because a local volume pins it to node {} while the dependency is on node {}",
                            context.service_name,
                            template.name,
                            replica,
                            dependency.name,
                            hard_target,
                            target_node
                        ));
                    }
                    if let Some(existing_bridge_target) = bridge_targets.get(&key)
                        && *existing_bridge_target != target_node
                    {
                        return Err(anyhow!(
                            "service '{}' template '{}' replica {} has bridge-local dependencies on different nodes",
                            context.service_name,
                            template.name,
                            replica
                        ));
                    }

                    bridge_targets.insert(key.clone(), target_node);
                    if targets.get(&key).copied() != Some(target_node) {
                        targets.insert(key, target_node);
                        changed = true;
                    }
                }
            }
        }

        if !changed {
            return Ok(());
        }
    }

    Ok(())
}

/// Return whether two templates share at least one node-local bridge network.
fn templates_share_bridge_network(
    left: &TaskTemplateSpecValue,
    right: &TaskTemplateSpecValue,
    network_registry: &NetworkRegistry,
) -> anyhow::Result<bool> {
    let right_networks: HashSet<Uuid> = right.required_network_ids().into_iter().collect();
    for network_id in left.required_network_ids() {
        if !right_networks.contains(&network_id) {
            continue;
        }
        let Some(spec) = network_registry.get_spec(network_id)? else {
            continue;
        };
        if matches!(spec.driver, NetworkDriver::Bridge) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Return whether a bridge co-location target also satisfies the template's placement policy.
fn template_can_run_on_node(
    template: &TaskTemplateSpecValue,
    node_id: Uuid,
    eligible_nodes: &[Uuid],
    placement_nodes: &[PlacementNode],
) -> bool {
    if !eligible_nodes.contains(&node_id) {
        return false;
    }
    if template.placement().is_unconstrained() || placement_nodes.is_empty() {
        return true;
    }
    placement_nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .is_some_and(|node| template.placement().matches(node))
}

/// Builds the active service-replica inventory used by soft affinity and anti-affinity hints.
///
/// Only non-terminal workloads are counted so stale history does not bias future placement.
pub(super) async fn build_placement_preference_inventory(
    workload_manager: &WorkloadManager,
) -> anyhow::Result<PlacementPreferenceInventory> {
    let active_filter = TaskStateFilter::active_only();
    let workloads = workload_manager.list_workloads(&active_filter).await?;
    let mut inventory = PlacementPreferenceInventory::default();

    for workload in workloads {
        let Some(owner) = workload.service_owner() else {
            continue;
        };
        inventory.record_service_replica(workload.node_id, &owner.service_name, &owner.template);
    }

    Ok(inventory)
}

/// Resolves one hard target node for a template when all mounted local volumes are already bound.
fn resolve_template_volume_target(
    volume_registry: &VolumeRegistry,
    mounts: &[WorkloadVolumeMount],
) -> anyhow::Result<Option<Uuid>> {
    let mut bound_node: Option<Uuid> = None;
    for mount in mounts {
        let spec = volume_registry.get_spec(mount.volume_id)?.ok_or_else(|| {
            anyhow!(
                "unknown volume '{}' ({})",
                mount.volume_name,
                mount.volume_id
            )
        })?;
        let Some(node_id) = spec.bound_node_id else {
            continue;
        };
        match bound_node {
            Some(current) if current != node_id => {
                return Err(anyhow!(
                    "mounted volumes are bound to different nodes for one task template"
                ));
            }
            None => bound_node = Some(node_id),
            _ => {}
        }
    }
    Ok(bound_node)
}

/// Returns true when the mount list includes a bound node-local volume that cannot safely fall back.
pub(super) fn mounted_local_volumes_require_pinned_target(
    volume_registry: &VolumeRegistry,
    mounts: &[WorkloadVolumeMount],
) -> anyhow::Result<bool> {
    for mount in mounts {
        let spec = volume_registry.get_spec(mount.volume_id)?.ok_or_else(|| {
            anyhow!(
                "unknown volume '{}' ({})",
                mount.volume_name,
                mount.volume_id
            )
        })?;
        if spec.bound_node_id.is_some() && matches!(spec.driver, VolumeDriver::Local(_)) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Returns true when any task request in the batch must preserve its explicit node target.
pub(super) fn requests_require_pinned_targets(
    volume_registry: &VolumeRegistry,
    network_registry: &NetworkRegistry,
    requests: &[WorkloadStartRequest],
) -> anyhow::Result<bool> {
    for request in requests {
        if mounted_local_volumes_require_pinned_target(volume_registry, &request.volumes)? {
            return Ok(true);
        }
        if request.target_node.is_some()
            && request
                .networks
                .iter()
                .any(|network_id| network_is_node_local(network_registry, *network_id))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Return true when a known network id is backed by a node-local driver.
fn network_is_node_local(network_registry: &NetworkRegistry, network_id: Uuid) -> bool {
    matches!(
        network_registry.get_spec(network_id),
        Ok(Some(spec)) if spec.driver.is_node_local()
    )
}

/// Returns true when the error chain represents a recoverable node-local volume availability issue.
pub(super) fn is_local_volume_unavailable_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|cause| cause.is::<LocalVolumeAccessError>())
}

/// Collects the sorted set of nodes that remain eligible for service placement.
pub(super) fn build_eligible_nodes<I>(
    local_node_id: Uuid,
    local_schedulable: bool,
    local_down: bool,
    peer_states: I,
) -> Vec<Uuid>
where
    I: IntoIterator<Item = (Uuid, bool, bool)>,
{
    let mut nodes: BTreeSet<Uuid> = BTreeSet::new();
    if local_schedulable && !local_down {
        nodes.insert(local_node_id);
    }

    for (peer_id, schedulable, down) in peer_states {
        if schedulable && !down {
            nodes.insert(peer_id);
        }
    }

    nodes.into_iter().collect()
}

/// Returns whether a targeted rollout batch may safely drop its node targets on fallback.
///
/// Multi-node targeted batches encode deterministic spread decisions. Dropping every target after
/// one transient scheduling miss can collapse a balanced scale-out onto fewer nodes and leave the
/// repair work to a later rebalance loop. Only batches that point at zero or one distinct target
/// should use the untargeted fallback path.
pub(super) fn allow_untargeted_fallback(requests: &[WorkloadStartRequest]) -> bool {
    requests
        .iter()
        .filter_map(|request| request.target_node)
        .collect::<HashSet<_>>()
        .len()
        <= 1
}
