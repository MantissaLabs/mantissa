use crate::config;
use crate::gossip::Message;
use crate::ip_family::{IpFamily, infer_default_ip_family};
use crate::network::bpf::overlay_bpf_program_specs;
use crate::network::controller::NetworkController;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkDriver, NetworkEvent, NetworkSpecDraft, NetworkSpecUpdate, NetworkSpecValue,
    NetworkStatus,
};
use crate::registry::Registry;
use crate::workload::manager::WorkloadStartRequest;
use anyhow::{Result, anyhow};
use async_channel::Sender;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// IPv4 prefix used by deterministic auto-provisioned manifest networks.
const DEFAULT_NETWORK_SUBNET_PREFIX_V4: u8 = 20;
/// Number of non-overlapping `/20` candidates inside the default IPv4 `10.0.0.0/8` range.
const DEFAULT_NETWORK_SUBNET_CANDIDATES_V4: u32 = 1 << 12;
/// IPv6 prefix used by deterministic auto-provisioned manifest networks.
const DEFAULT_NETWORK_SUBNET_PREFIX_V6: u8 = 64;
/// Number of deterministic IPv6 ULA subnet candidates probed before falling back to the first.
const DEFAULT_NETWORK_SUBNET_CANDIDATES_V6: u32 = 1 << 16;

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
        let mut known_subnets: BTreeSet<String> = existing
            .iter()
            .filter(|spec| !spec.is_deleted())
            .map(|spec| spec.subnet_cidr.clone())
            .collect();

        for requested in required {
            if let Some(existing) = existing_by_name.get(&requested.name)
                && !existing.is_deleted()
            {
                validate_existing_required_network(context, existing, &requested)?;
                continue;
            }

            let mut spec = build_required_network_spec(&requested, &known_subnets);
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
                    bpf_programs: spec.bpf_programs.clone(),
                });
                spec = deleted;
            }

            spec.set_status(NetworkStatus::Pending);
            self.network_registry.upsert_spec(spec.clone()).await?;
            self.gossip_tx
                .send(Message::Network {
                    id: Uuid::new_v4(),
                    event: NetworkEvent::Upsert(spec.clone()),
                })
                .await
                .map_err(|err| anyhow!("failed to broadcast network upsert: {err}"))?;
            self.network_controller.schedule_spec_change(spec.id).await;
            known_subnets.insert(spec.subnet_cidr.clone());
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
        if spec.is_deleted() {
            return Ok(false);
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
        if spec.is_deleted() {
            return Ok(false);
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
    Ok(())
}

/// Builds the replicated network spec used for manifest-side auto-provisioning.
fn build_required_network_spec(
    requested: &WorkloadNetworkRequirement,
    known_subnets: &BTreeSet<String>,
) -> NetworkSpecValue {
    let family = match requested.ip_family {
        WorkloadNetworkIpFamily::Ipv4 => WorkloadNetworkIpFamily::Ipv4,
        WorkloadNetworkIpFamily::Ipv6 => WorkloadNetworkIpFamily::Ipv6,
        WorkloadNetworkIpFamily::Default => default_required_network_family(),
    };
    let bpf_programs = match requested.driver {
        NetworkDriver::Vxlan => overlay_bpf_program_specs(),
        NetworkDriver::Bridge => Vec::new(),
    };

    NetworkSpecValue::new(NetworkSpecDraft {
        name: requested.name.clone(),
        description: String::new(),
        driver: requested.driver,
        subnet_cidr: default_required_network_subnet(
            &requested.name,
            known_subnets.iter().map(String::as_str),
            family,
        ),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs,
    })
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
            continue;
        }

        normalized.insert(
            name.to_string(),
            WorkloadNetworkRequirement {
                name: name.to_string(),
                driver: network.driver,
                ip_family: network.ip_family,
            },
        );
    }

    Ok(normalized.into_values().collect())
}

/// Resolves the daemon's default network IP family for server-side auto-provisioning.
fn default_required_network_family() -> WorkloadNetworkIpFamily {
    let (has_ipv4, has_ipv6) = crate::node::address::detect_local_ip_families();
    match infer_default_ip_family(
        config::nodeport_ip(),
        config::advertise_addr().as_deref(),
        config::default_ip_family_policy(),
        has_ipv4,
        has_ipv6,
    ) {
        IpFamily::Ipv4 => WorkloadNetworkIpFamily::Ipv4,
        IpFamily::Ipv6 => WorkloadNetworkIpFamily::Ipv6,
    }
}

/// Computes a deterministic default subnet for an auto-provisioned manifest network.
fn default_required_network_subnet<I, S>(
    name: &str,
    existing_subnets: I,
    family: WorkloadNetworkIpFamily,
) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let used: BTreeSet<String> = existing_subnets
        .into_iter()
        .map(|subnet| subnet.as_ref().trim().to_string())
        .collect();
    let hash = default_required_network_subnet_hash(name);
    let candidates = default_required_network_subnet_candidate_count(family);

    for offset in 0..candidates {
        let candidate = default_required_network_subnet_candidate(hash, offset, family);
        if !used.contains(&candidate) {
            return candidate;
        }
    }

    default_required_network_subnet_candidate(hash, 0, family)
}

/// Hashes a network name into a stable default-subnet selection seed.
fn default_required_network_subnet_hash(name: &str) -> u32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&digest.as_bytes()[..4]);
    u32::from_le_bytes(bytes)
}

/// Returns the number of deterministic subnet candidates in the requested family.
fn default_required_network_subnet_candidate_count(family: WorkloadNetworkIpFamily) -> u32 {
    match family {
        WorkloadNetworkIpFamily::Default | WorkloadNetworkIpFamily::Ipv4 => {
            DEFAULT_NETWORK_SUBNET_CANDIDATES_V4
        }
        WorkloadNetworkIpFamily::Ipv6 => DEFAULT_NETWORK_SUBNET_CANDIDATES_V6,
    }
}

/// Converts a deterministic subnet candidate offset into a concrete CIDR string.
fn default_required_network_subnet_candidate(
    hash: u32,
    offset: u32,
    family: WorkloadNetworkIpFamily,
) -> String {
    match family {
        WorkloadNetworkIpFamily::Default | WorkloadNetworkIpFamily::Ipv4 => {
            default_required_network_subnet_candidate_v4(hash, offset)
        }
        WorkloadNetworkIpFamily::Ipv6 => default_required_network_subnet_candidate_v6(hash, offset),
    }
}

/// Converts one candidate offset into a unique `10.0.0.0/8` `/20` subnet.
fn default_required_network_subnet_candidate_v4(hash: u32, offset: u32) -> String {
    let seed = hash & (DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let bucket = seed.wrapping_add(offset) & (DEFAULT_NETWORK_SUBNET_CANDIDATES_V4 - 1);
    let second_octet = (bucket >> 4) as u8;
    let third_octet = ((bucket & 0x0f) << 4) as u8;
    format!("10.{second_octet}.{third_octet}.0/{DEFAULT_NETWORK_SUBNET_PREFIX_V4}")
}

/// Converts one candidate offset into a unique `fd42::/16` `/64` subnet.
fn default_required_network_subnet_candidate_v6(hash: u32, offset: u32) -> String {
    let group = (hash >> 16) as u16;
    let seed = hash as u16;
    let bucket = seed.wrapping_add(offset as u16);
    format!("fd42:{group:04x}:{bucket:04x}::/{DEFAULT_NETWORK_SUBNET_PREFIX_V6}")
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
                },
                WorkloadNetworkRequirement {
                    name: "shared".to_string(),
                    driver: NetworkDriver::Bridge,
                    ip_family: WorkloadNetworkIpFamily::Default,
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
    /// Default-subnet selection probes away from an already used IPv4 candidate.
    fn default_required_network_subnet_skips_used_ipv4_candidate() {
        let initial = default_required_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            WorkloadNetworkIpFamily::Ipv4,
        );
        let resolved = default_required_network_subnet(
            "alpha",
            [initial.as_str()],
            WorkloadNetworkIpFamily::Ipv4,
        );

        assert_ne!(initial, resolved);
        assert!(resolved.ends_with("/20"));
    }

    #[test]
    /// Default-subnet selection probes away from an already used IPv6 candidate.
    fn default_required_network_subnet_skips_used_ipv6_candidate() {
        let initial = default_required_network_subnet(
            "alpha",
            std::iter::empty::<&str>(),
            WorkloadNetworkIpFamily::Ipv6,
        );
        let resolved = default_required_network_subnet(
            "alpha",
            [initial.as_str()],
            WorkloadNetworkIpFamily::Ipv6,
        );

        assert_ne!(initial, resolved);
        assert!(resolved.starts_with("fd42:"));
        assert!(resolved.ends_with("/64"));
    }
}
