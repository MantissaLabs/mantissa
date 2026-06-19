use crate::gossip::Message;
use crate::network::controller::NetworkController;
use crate::network::defaults::{
    CidrOverlapIndex, DefaultNetworkIpFamily, default_bpf_programs_for_driver,
    default_network_ip_family, default_network_subnet_with_conflict_check,
};
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkDriver, NetworkEvent, NetworkPeerStateValue, NetworkRealizationPolicy, NetworkSpecDraft,
    NetworkSpecUpdate, NetworkSpecValue, NetworkStatus,
};
use crate::registry::Registry;
use crate::workload::manager::WorkloadStartRequest;
use anyhow::{Result, anyhow};
use async_channel::Sender;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// Address family requested for a manifest-declared network dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadNetworkIpFamily {
    Default,
    Ipv4,
    Ipv6,
}

/// Network dependency declared by a first-class workload submission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkloadNetworkRequirement {
    pub name: String,
    pub driver: NetworkDriver,
    pub ip_family: WorkloadNetworkIpFamily,
    pub realization: Option<NetworkRealizationPolicy>,
}

/// Shared network prerequisite handler used before first-class workload placement.
#[derive(Clone)]
pub struct WorkloadNetworkPrerequisites {
    network_registry: NetworkRegistry,
    network_controller: NetworkController,
    cluster_registry: Registry,
    gossip_tx: Sender<Message>,
}

/// Readiness phase selected by workload network prerequisite gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NetworkReadinessPurpose {
    Admission,
    TargetRealization,
}

impl WorkloadNetworkPrerequisites {
    /// Builds the prerequisite handler from the replicated network and cluster controllers.
    pub fn new(
        network_registry: NetworkRegistry,
        network_controller: NetworkController,
        cluster_registry: Registry,
        gossip_tx: Sender<Message>,
    ) -> Self {
        Self {
            network_registry,
            network_controller,
            cluster_registry,
            gossip_tx,
        }
    }

    /// Ensures every network declared by a workload submit request exists before admission.
    pub async fn ensure_required_networks(
        &self,
        context: &str,
        required_networks: &[WorkloadNetworkRequirement],
    ) -> Result<()> {
        let required = normalize_required_networks(context, required_networks)?;
        if required.is_empty() {
            return Ok(());
        }

        let existing = self.network_registry.list_specs()?;
        let existing_by_name: HashMap<String, NetworkSpecValue> = existing
            .iter()
            .cloned()
            .map(|spec| (spec.name.clone(), spec))
            .collect();
        let mut known_subnets = CidrOverlapIndex::new();
        for spec in existing.iter().filter(|spec| !spec.is_deleted()) {
            let _ = known_subnets.insert_cidr(&spec.subnet_cidr);
        }

        for requested in required {
            if let Some(existing) = existing_by_name.get(&requested.name)
                && !existing.is_deleted()
            {
                validate_existing_required_network(context, existing, &requested)?;
                continue;
            }

            let mut spec = build_required_network_spec(&requested, &known_subnets)?;
            if let Some(mut deleted) = existing_by_name.get(&requested.name).cloned()
                && deleted.is_deleted()
            {
                deleted.reset_for_recreate(NetworkSpecUpdate {
                    description: spec.description.clone(),
                    driver: spec.driver,
                    subnet_cidr: spec.subnet_cidr.clone(),
                    vni: spec.vni,
                    mtu: spec.mtu,
                    sealed: spec.sealed,
                    realization: spec.realization,
                    bpf_programs: spec.bpf_programs.clone(),
                });
                spec = deleted;
            }

            spec.set_status(NetworkStatus::Ready);
            self.network_registry.upsert_spec(spec.clone()).await?;
            self.gossip_tx
                .send(Message::Network {
                    id: Uuid::new_v4(),
                    event: NetworkEvent::Upsert(spec.clone()),
                })
                .await
                .map_err(|err| anyhow!("failed to broadcast network upsert: {err}"))?;
            if spec.realizes_on_all_nodes() {
                self.network_controller.schedule_spec_change(spec.id).await;
            }
            known_subnets
                .insert_cidr(&spec.subnet_cidr)
                .map_err(|err| anyhow!("network subnet index update failed: {err}"))?;
            tracing::info!(
                target: "workload",
                "network '{}' auto-provisioned for {context} with id {}",
                spec.name,
                spec.id
            );
        }

        Ok(())
    }

    /// Builds a human-readable blocker when required networks are not admissible for placement.
    ///
    /// For on-demand networks this only requires an observed Ready spec. Target-side scheduler
    /// admission is responsible for realizing the local dataplane before any task is accepted.
    pub fn admission_readiness_detail(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Result<Option<String>> {
        self.readiness_detail(requests, NetworkReadinessPurpose::Admission)
    }

    /// Builds a human-readable blocker when selected target nodes lack realized network dataplane.
    ///
    /// This is stricter than placement admission: every targeted request must have a Ready peer row
    /// for the exact selected node, including on-demand networks.
    pub fn target_realization_readiness_detail(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Result<Option<String>> {
        self.readiness_detail(requests, NetworkReadinessPurpose::TargetRealization)
    }

    /// Builds the status detail for either pre-demand admission or post-demand realization checks.
    fn readiness_detail(
        &self,
        requests: &[WorkloadStartRequest],
        purpose: NetworkReadinessPurpose,
    ) -> Result<Option<String>> {
        let mut blockers = BTreeSet::new();
        for request in requests {
            for network_id in &request.networks {
                match request.target_node {
                    Some(node_id) => {
                        if !self.network_satisfies_target(*network_id, node_id, purpose)? {
                            blockers.insert(format!(
                                "network '{}' {} on node '{}'",
                                self.network_label(*network_id)?,
                                match purpose {
                                    NetworkReadinessPurpose::Admission => "not admissible",
                                    NetworkReadinessPurpose::TargetRealization => "not ready",
                                },
                                self.node_label(node_id)
                            ));
                        }
                    }
                    None => match purpose {
                        NetworkReadinessPurpose::Admission => {
                            if !self.network_admissible_on_any_peer(*network_id)? {
                                blockers.insert(format!(
                                    "network '{}' has no ready schedulable peer",
                                    self.network_label(*network_id)?
                                ));
                            }
                        }
                        NetworkReadinessPurpose::TargetRealization => {
                            blockers.insert(format!(
                                "network '{}' has no selected target node",
                                self.network_label(*network_id)?
                            ));
                        }
                    },
                }
            }
        }

        if blockers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(format!(
                "{}: {}",
                match purpose {
                    NetworkReadinessPurpose::Admission => "waiting for network readiness",
                    NetworkReadinessPurpose::TargetRealization => "waiting for network realization",
                },
                format_network_readiness_blockers(&blockers)
            )))
        }
    }

    /// Returns true when the requested target node satisfies the selected readiness purpose.
    fn network_satisfies_target(
        &self,
        network_id: Uuid,
        node_id: Uuid,
        purpose: NetworkReadinessPurpose,
    ) -> Result<bool> {
        let Some(spec) = self.network_registry.get_spec(network_id)? else {
            return Ok(false);
        };

        let peer_state = self.network_registry.get_peer_state(network_id, node_id)?;
        Ok(match purpose {
            NetworkReadinessPurpose::Admission => {
                Self::network_state_admissible_for_target(&spec, peer_state.as_ref())
            }
            NetworkReadinessPurpose::TargetRealization => {
                Self::network_state_realized_for_target(&spec, peer_state.as_ref())
            }
        })
    }

    /// Returns true once any schedulable peer can admit workloads for the requested network.
    fn network_admissible_on_any_peer(&self, network_id: Uuid) -> Result<bool> {
        let Some(spec) = self.network_registry.get_spec(network_id)? else {
            return Ok(false);
        };
        if !Self::network_spec_admissible_for_workload(&spec) {
            return Ok(false);
        }
        if spec.realization == NetworkRealizationPolicy::OnDemand {
            return Ok(true);
        }
        for state in self.network_registry.list_peer_states(Some(network_id))? {
            if Self::network_peer_state_ready(&state)
                && self.cluster_registry.peer_schedulable(state.peer_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns true when a spec is available enough to enter workload placement.
    fn network_spec_admissible_for_workload(spec: &NetworkSpecValue) -> bool {
        !spec.is_deleted() && spec.status == NetworkStatus::Ready
    }

    /// Returns true when a target node can enter placement for the requested network.
    fn network_state_admissible_for_target(
        spec: &NetworkSpecValue,
        peer_state: Option<&NetworkPeerStateValue>,
    ) -> bool {
        if !Self::network_spec_admissible_for_workload(spec) {
            return false;
        }
        if spec.realization == NetworkRealizationPolicy::OnDemand {
            return true;
        }

        peer_state.is_some_and(Self::network_peer_state_ready)
    }

    /// Returns true when a target node has fully realized the requested network.
    fn network_state_realized_for_target(
        spec: &NetworkSpecValue,
        peer_state: Option<&NetworkPeerStateValue>,
    ) -> bool {
        Self::network_spec_admissible_for_workload(spec)
            && peer_state.is_some_and(Self::network_peer_state_ready)
    }

    /// Returns true when a replicated peer row reports successful network reconciliation.
    fn network_peer_state_ready(peer_state: &NetworkPeerStateValue) -> bool {
        peer_state.error.is_none() && peer_state.state.is_ready()
    }

    /// Renders one network id as a stable operator-facing label for status details.
    fn network_label(&self, network_id: Uuid) -> Result<String> {
        Ok(self
            .network_registry
            .get_spec(network_id)?
            .map(|spec| spec.name)
            .unwrap_or_else(|| short_uuid(network_id)))
    }

    /// Renders one node id as a compact hostname-or-id label for status details.
    fn node_label(&self, node_id: Uuid) -> String {
        self.cluster_registry
            .peer_hostname(node_id)
            .map(|hostname| hostname.trim().to_string())
            .filter(|hostname| !hostname.is_empty())
            .unwrap_or_else(|| short_uuid(node_id))
    }
}

/// Validates that a named network already in the registry satisfies the workload request.
fn validate_existing_required_network(
    context: &str,
    existing: &NetworkSpecValue,
    requested: &WorkloadNetworkRequirement,
) -> Result<()> {
    if existing.status == NetworkStatus::Deleting {
        return Err(anyhow!(
            "{context} requests network '{}' but the existing network is deleting",
            requested.name
        ));
    }
    if existing.driver != requested.driver {
        return Err(anyhow!(
            "{context} requests network '{}' with driver {:?} but existing network uses {:?}",
            requested.name,
            requested.driver,
            existing.driver
        ));
    }
    if let Some(realization) = requested.realization
        && existing.realization != realization
    {
        return Err(anyhow!(
            "{context} requests network '{}' with realization {} but existing network uses {}",
            requested.name,
            realization,
            existing.realization
        ));
    }
    Ok(())
}

/// Builds the replicated network spec used for manifest-side auto-provisioning.
fn build_required_network_spec(
    requested: &WorkloadNetworkRequirement,
    known_subnets: &CidrOverlapIndex,
) -> Result<NetworkSpecValue> {
    let family = default_subnet_family_for_requirement(requested.ip_family);
    let bpf_programs = default_bpf_programs_for_driver(requested.driver);
    let subnet_cidr =
        default_network_subnet_with_conflict_check(&requested.name, family, |candidate| {
            known_subnets.overlaps_cidr(candidate)
        })
        .ok_or_else(|| {
            anyhow!(
                "failed to auto-provision network '{}': no default subnet is available",
                requested.name
            )
        })?;

    let realization = requested
        .realization
        .unwrap_or_else(crate::config::network_realization_default);

    Ok(NetworkSpecValue::new_with_realization(
        NetworkSpecDraft {
            name: requested.name.clone(),
            description: String::new(),
            driver: requested.driver,
            subnet_cidr,
            vni: 0,
            mtu: 0,
            sealed: false,
            bpf_programs,
        },
        realization,
    ))
}

/// Deduplicates required networks while rejecting conflicting driver or family requests.
fn normalize_required_networks(
    context: &str,
    required_networks: &[WorkloadNetworkRequirement],
) -> Result<Vec<WorkloadNetworkRequirement>> {
    let mut normalized: BTreeMap<String, WorkloadNetworkRequirement> = BTreeMap::new();
    for network in required_networks {
        let name = network.name.trim();
        if name.is_empty() {
            continue;
        }

        if let Some(existing) = normalized.get_mut(name) {
            if existing.driver != network.driver {
                return Err(anyhow!(
                    "{context} requests network '{}' with conflicting drivers",
                    name
                ));
            }
            match (existing.ip_family, network.ip_family) {
                (WorkloadNetworkIpFamily::Ipv4, WorkloadNetworkIpFamily::Ipv6)
                | (WorkloadNetworkIpFamily::Ipv6, WorkloadNetworkIpFamily::Ipv4) => {
                    return Err(anyhow!(
                        "{context} requests network '{}' with conflicting IP families",
                        name
                    ));
                }
                (WorkloadNetworkIpFamily::Default, explicit)
                    if explicit != WorkloadNetworkIpFamily::Default =>
                {
                    existing.ip_family = explicit;
                }
                _ => {}
            }
            match (existing.realization, network.realization) {
                (Some(left), Some(right)) if left != right => {
                    return Err(anyhow!(
                        "{context} requests network '{}' with conflicting realization policies",
                        name
                    ));
                }
                (None, Some(policy)) => {
                    existing.realization = Some(policy);
                }
                _ => {}
            }
            continue;
        }

        normalized.insert(
            name.to_string(),
            WorkloadNetworkRequirement {
                name: name.to_string(),
                driver: network.driver,
                ip_family: network.ip_family,
                realization: network.realization,
            },
        );
    }

    Ok(normalized.into_values().collect())
}

/// Maps a workload network family request to the concrete family used for default subnet choice.
fn default_subnet_family_for_requirement(
    family: WorkloadNetworkIpFamily,
) -> DefaultNetworkIpFamily {
    match family {
        WorkloadNetworkIpFamily::Default => default_network_ip_family(),
        WorkloadNetworkIpFamily::Ipv4 => DefaultNetworkIpFamily::Ipv4,
        WorkloadNetworkIpFamily::Ipv6 => DefaultNetworkIpFamily::Ipv6,
    }
}

/// Formats a bounded list of network readiness blockers for status details.
fn format_network_readiness_blockers(blockers: &BTreeSet<String>) -> String {
    let mut parts = Vec::new();
    for blocker in blockers.iter().take(3) {
        parts.push(blocker.clone());
    }
    if blockers.len() > parts.len() {
        let remaining = blockers.len() - parts.len();
        parts.push(format!("{remaining} more blocker(s)"));
    }
    parts.join("; ")
}

/// Returns a short stable id for operator-facing status details.
fn short_uuid(id: Uuid) -> String {
    id.to_string().chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::types::NetworkPeerState;

    /// Builds a network spec in the requested realization policy and lifecycle status.
    fn readiness_test_spec(
        realization: NetworkRealizationPolicy,
        status: NetworkStatus,
    ) -> NetworkSpecValue {
        let mut spec = NetworkSpecValue::new_with_realization(
            NetworkSpecDraft {
                name: format!("network-{realization}"),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.41.0.0/24".to_string(),
                vni: 0,
                mtu: 0,
                sealed: false,
                bpf_programs: Vec::new(),
            },
            realization,
        );
        spec.set_status(status);
        spec
    }

    #[test]
    /// Required network normalization rejects driver conflicts for the same network name.
    fn normalize_required_networks_rejects_conflicting_drivers() {
        let err = normalize_required_networks(
            "job submission",
            &[
                WorkloadNetworkRequirement {
                    name: "shared".to_string(),
                    driver: NetworkDriver::Vxlan,
                    ip_family: WorkloadNetworkIpFamily::Default,
                    realization: None,
                },
                WorkloadNetworkRequirement {
                    name: "shared".to_string(),
                    driver: NetworkDriver::Bridge,
                    ip_family: WorkloadNetworkIpFamily::Default,
                    realization: None,
                },
            ],
        )
        .expect_err("conflicting drivers should fail");

        assert!(
            err.to_string().contains("conflicting drivers"),
            "unexpected error: {err}"
        );
    }

    #[test]
    /// Required network normalization rejects realization conflicts for the same network name.
    fn normalize_required_networks_rejects_conflicting_realization() {
        let err = normalize_required_networks(
            "service deployment",
            &[
                WorkloadNetworkRequirement {
                    name: "shared".to_string(),
                    driver: NetworkDriver::Vxlan,
                    ip_family: WorkloadNetworkIpFamily::Default,
                    realization: Some(NetworkRealizationPolicy::AllNodes),
                },
                WorkloadNetworkRequirement {
                    name: "shared".to_string(),
                    driver: NetworkDriver::Vxlan,
                    ip_family: WorkloadNetworkIpFamily::Default,
                    realization: Some(NetworkRealizationPolicy::OnDemand),
                },
            ],
        )
        .expect_err("conflicting realization policies should fail");

        assert!(
            err.to_string().contains("conflicting realization"),
            "unexpected error: {err}"
        );
    }

    #[test]
    /// On-demand admission only needs a Ready spec because target admission creates demand.
    fn readiness_admission_allows_on_demand_ready_spec_without_peer_state() {
        let spec = readiness_test_spec(NetworkRealizationPolicy::OnDemand, NetworkStatus::Ready);

        assert!(
            WorkloadNetworkPrerequisites::network_state_admissible_for_target(&spec, None),
            "on-demand placement admission should not require a pre-existing peer row"
        );
        assert!(
            !WorkloadNetworkPrerequisites::network_state_realized_for_target(&spec, None),
            "target realization still requires an exact-node ready peer row"
        );
    }

    #[test]
    /// All-nodes admission still requires the selected target node to be already ready.
    fn readiness_admission_blocks_all_nodes_spec_without_peer_state() {
        let spec = readiness_test_spec(NetworkRealizationPolicy::AllNodes, NetworkStatus::Ready);

        assert!(
            !WorkloadNetworkPrerequisites::network_state_admissible_for_target(&spec, None),
            "all-nodes placement admission must keep waiting for the target peer row"
        );
        assert!(
            !WorkloadNetworkPrerequisites::network_state_realized_for_target(&spec, None),
            "realization cannot pass without an exact-node peer row"
        );
    }

    #[test]
    /// Ready peer rows satisfy both admission and realization for the exact target node.
    fn readiness_realization_accepts_ready_peer_state() {
        let spec = readiness_test_spec(NetworkRealizationPolicy::OnDemand, NetworkStatus::Ready);
        let peer_state = NetworkPeerStateValue::new(
            spec.id,
            Uuid::new_v4(),
            "node-a",
            NetworkPeerState::Ready,
            None,
        );

        assert!(
            WorkloadNetworkPrerequisites::network_state_admissible_for_target(
                &spec,
                Some(&peer_state)
            ),
            "ready on-demand specs remain admissible"
        );
        assert!(
            WorkloadNetworkPrerequisites::network_state_realized_for_target(
                &spec,
                Some(&peer_state)
            ),
            "ready peer row should satisfy exact-node realization"
        );
    }

    #[test]
    /// Non-ready specs block both placement admission and target realization.
    fn readiness_blocks_pending_spec_even_with_ready_peer_state() {
        let spec = readiness_test_spec(NetworkRealizationPolicy::OnDemand, NetworkStatus::Pending);
        let peer_state = NetworkPeerStateValue::new(
            spec.id,
            Uuid::new_v4(),
            "node-a",
            NetworkPeerState::Ready,
            None,
        );

        assert!(
            !WorkloadNetworkPrerequisites::network_state_admissible_for_target(
                &spec,
                Some(&peer_state)
            ),
            "pending specs must not enter placement admission"
        );
        assert!(
            !WorkloadNetworkPrerequisites::network_state_realized_for_target(
                &spec,
                Some(&peer_state)
            ),
            "pending specs must not be treated as target-realized"
        );
    }

    #[test]
    /// Peer rows carrying an error are not treated as realized even if the state is Ready.
    fn readiness_blocks_peer_state_with_error() {
        let peer_state = NetworkPeerStateValue::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            "node-a",
            NetworkPeerState::Ready,
            Some("failed after readiness".to_string()),
        );

        assert!(
            !WorkloadNetworkPrerequisites::network_peer_state_ready(&peer_state),
            "peer errors must keep readiness predicates blocked"
        );
    }
}
