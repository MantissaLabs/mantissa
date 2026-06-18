use crate::gossip::Message;
use crate::network::controller::NetworkController;
use crate::network::defaults::{
    CidrOverlapIndex, DefaultNetworkIpFamily, default_bpf_programs_for_driver,
    default_network_ip_family, default_network_subnet_with_conflict_check,
};
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkDriver, NetworkEvent, NetworkRealizationPolicy, NetworkSpecDraft, NetworkSpecUpdate,
    NetworkSpecValue, NetworkStatus,
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

    /// Builds a human-readable blocker when target nodes lack required network readiness.
    pub fn launch_readiness_detail(
        &self,
        requests: &[WorkloadStartRequest],
    ) -> Result<Option<String>> {
        let mut blockers = BTreeSet::new();
        for request in requests {
            for network_id in &request.networks {
                match request.target_node {
                    Some(node_id) => {
                        if !self.network_ready_on_node(*network_id, node_id)? {
                            blockers.insert(format!(
                                "network '{}' not ready on node '{}'",
                                self.network_label(*network_id)?,
                                self.node_label(node_id)
                            ));
                        }
                    }
                    None => {
                        if !self.network_ready_on_any_peer(*network_id)? {
                            blockers.insert(format!(
                                "network '{}' has no ready schedulable peer",
                                self.network_label(*network_id)?
                            ));
                        }
                    }
                }
            }
        }

        if blockers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(format!(
                "waiting for network readiness: {}",
                format_network_readiness_blockers(&blockers)
            )))
        }
    }

    /// Returns true once the given peer has reconciled the requested network locally.
    fn network_ready_on_node(&self, network_id: Uuid, node_id: Uuid) -> Result<bool> {
        let Some(spec) = self.network_registry.get_spec(network_id)? else {
            return Ok(false);
        };
        if spec.is_deleted() || spec.status != NetworkStatus::Ready {
            return Ok(false);
        }
        if spec.realization == NetworkRealizationPolicy::OnDemand {
            // Lazy networks are made ready by scheduler/runtime admission on the selected node.
            return Ok(true);
        }
        Ok(self
            .network_registry
            .get_peer_state(network_id, node_id)?
            .is_some_and(|state| state.state.is_ready()))
    }

    /// Returns true once any schedulable peer can host workloads for the requested network.
    fn network_ready_on_any_peer(&self, network_id: Uuid) -> Result<bool> {
        let Some(spec) = self.network_registry.get_spec(network_id)? else {
            return Ok(false);
        };
        if spec.is_deleted() || spec.status != NetworkStatus::Ready {
            return Ok(false);
        }
        if spec.realization == NetworkRealizationPolicy::OnDemand {
            // Lazy networks are made ready by scheduler/runtime admission on the selected node.
            return Ok(true);
        }
        for state in self.network_registry.list_peer_states(Some(network_id))? {
            if state.state.is_ready() && self.cluster_registry.peer_schedulable(state.peer_id) {
                return Ok(true);
            }
        }
        Ok(false)
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
}
