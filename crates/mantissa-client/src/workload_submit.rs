use crate::config::ClientConfig;
use crate::config::NetworkIpFamily;
use crate::networks;
use crate::output;
use crate::volumes;
use anyhow::{Result, anyhow};
use blake3::Hasher;
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use uuid::Uuid;

const NETWORK_PROVISION_POLL_INTERVAL: Duration = Duration::from_millis(250);
const NETWORK_PROVISION_TIMEOUT: Duration = Duration::from_secs(30);

/// Driver families accepted by shared manifest-side volume provisioning helpers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeclaredVolumeDriverKind {
    LocalManaged,
    LocalImportedPath,
    External,
}

/// One manifest-facing volume label normalized for shared provisioning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredVolumeLabel {
    pub key: String,
    pub value: String,
}

/// One manifest-declared volume normalized for shared provisioning helpers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredVolumeSpec {
    pub name: String,
    pub driver_kind: DeclaredVolumeDriverKind,
    pub local_ownership: Option<volumes::LocalVolumeOwnership>,
    pub access_mode: volumes::VolumeAccessMode,
    pub binding_mode: volumes::VolumeBindingMode,
    pub reclaim_policy: volumes::VolumeReclaimPolicy,
    pub capacity_mb: Option<u64>,
    pub labels: Vec<DeclaredVolumeLabel>,
}

/// Resolved volume identity returned after manifest-side auto-provisioning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedDeclaredVolume {
    pub volume_id: Uuid,
    pub volume_name: String,
}

/// One top-level manifest network declaration used to override auto-created network defaults.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ManifestNetworkSpec {
    pub name: String,
    #[serde(default, deserialize_with = "deserialize_optional_network_driver")]
    pub driver: Option<networks::NetworkDriver>,
    #[serde(default, deserialize_with = "deserialize_optional_network_ip_family")]
    pub ip_family: Option<NetworkIpFamily>,
}

/// One normalized network request returned after manifest-side network resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestedNetworkSpec {
    pub name: String,
    pub driver: networks::NetworkDriver,
    pub ip_family: Option<NetworkIpFamily>,
}

/// One manifest-required network that must be schedulable before workload submission.
#[derive(Clone, Debug)]
struct NetworkProvisionTarget {
    id: Uuid,
    name: String,
}

/// Last observed readiness projection for one network during manifest auto-provisioning.
#[derive(Clone, Debug, PartialEq, Eq)]
struct NetworkReadinessObservation {
    status: networks::NetworkStatus,
    peer_count: u32,
    ready_peers: u32,
    error_peers: u32,
    first_error: Option<String>,
}

impl NetworkReadinessObservation {
    /// Build a scheduler-facing readiness snapshot from a network inspect response.
    fn from_inspect(info: &networks::NetworkInspect) -> Self {
        let peer_count = u32::try_from(info.peers.len()).map_or(u32::MAX, |value| value);
        let ready_peers = u32::try_from(
            info.peers
                .iter()
                .filter(|peer| peer.state == networks::NetworkPeerState::Ready)
                .count(),
        )
        .map_or(u32::MAX, |value| value);
        let error_peers = u32::try_from(
            info.peers
                .iter()
                .filter(|peer| peer.state == networks::NetworkPeerState::Error)
                .count(),
        )
        .map_or(u32::MAX, |value| value);
        let first_error = info
            .peers
            .iter()
            .find_map(|peer| peer.error.as_ref().filter(|error| !error.trim().is_empty()))
            .cloned();

        Self {
            status: info.spec.status,
            peer_count,
            ready_peers,
            error_peers,
            first_error,
        }
    }

    /// Return true once at least one peer can satisfy scheduler network readiness.
    fn is_schedulable(&self) -> bool {
        self.ready_peers > 0
            && !matches!(
                self.status,
                networks::NetworkStatus::Deleting | networks::NetworkStatus::Deleted
            )
    }

    /// Return a terminal readiness error when every observed peer has failed reconciliation.
    fn terminal_error(&self) -> Option<String> {
        if matches!(self.status, networks::NetworkStatus::Deleting) {
            return Some("network is deleting".to_string());
        }
        if matches!(self.status, networks::NetworkStatus::Deleted) {
            return Some("network is deleted".to_string());
        }
        if self.peer_count > 0 && self.ready_peers == 0 && self.error_peers == self.peer_count {
            return Some(self.first_error.clone().unwrap_or_else(|| {
                "all peers reported network reconciliation errors".to_string()
            }));
        }
        None
    }

    /// Render a compact status summary for prerequisite wait output and timeout errors.
    fn summary(&self) -> String {
        if self.peer_count == 0 {
            return format!("status {}, no peer state reported", self.status);
        }
        format!(
            "status {}, ready peers {}/{}",
            self.status, self.ready_peers, self.peer_count
        )
    }
}

/// Transport protocol for one manifest-declared node-local host port binding.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ManifestPortProtocol {
    #[default]
    Tcp,
    Udp,
}

/// Static node-local host port binding shared by service and job manifests.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ManifestPortBinding {
    pub name: String,
    pub target: u16,
    pub host: u16,
    #[serde(default = "default_host_port_ip")]
    pub host_ip: String,
    #[serde(default)]
    pub protocol: ManifestPortProtocol,
}

/// Derive the canonical network UUID from the manifest-facing network name.
pub fn compute_network_id(name: &str) -> Uuid {
    let mut hasher = Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Deserialize one optional manifest network driver while accepting bare `vxlan` / `bridge` syntax.
fn deserialize_optional_network_driver<'de, D>(
    deserializer: D,
) -> Result<Option<networks::NetworkDriver>, D::Error>
where
    D: Deserializer<'de>,
{
    networks::NetworkDriver::deserialize(deserializer).map(Some)
}

/// Deserialize one optional manifest network family while accepting bare `ipv4` / `ipv6` syntax.
fn deserialize_optional_network_ip_family<'de, D>(
    deserializer: D,
) -> Result<Option<NetworkIpFamily>, D::Error>
where
    D: Deserializer<'de>,
{
    NetworkIpFamily::deserialize(deserializer).map(Some)
}

/// Validate one set of top-level manifest network declarations before submission.
pub fn validate_declared_networks(
    declared_networks: &[ManifestNetworkSpec],
    context: &str,
) -> Result<()> {
    let mut seen = HashSet::new();
    for network in declared_networks {
        let trimmed = network.name.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("{context} declares a network with an empty name"));
        }
        if !seen.insert(trimmed.to_string()) {
            return Err(anyhow!(
                "{context} declares network '{}' multiple times",
                trimmed
            ));
        }
    }
    Ok(())
}

/// Resolve manifest network references against optional top-level family overrides.
pub fn resolve_requested_networks<I, S>(
    referenced_networks: I,
    declared_networks: &[ManifestNetworkSpec],
    context: &str,
) -> Result<Vec<RequestedNetworkSpec>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    validate_declared_networks(declared_networks, context)?;

    let mut overrides = HashMap::new();
    for network in declared_networks {
        overrides.insert(
            network.name.trim().to_string(),
            (
                network.driver.unwrap_or(networks::NetworkDriver::Vxlan),
                network.ip_family,
            ),
        );
    }

    let mut requested = Vec::new();
    let mut seen = HashSet::new();
    for network in referenced_networks {
        let trimmed = network.as_ref().trim();
        if trimmed.is_empty() {
            continue;
        }
        if seen.insert(trimmed.to_string()) {
            let (driver, ip_family) = overrides
                .get(trimmed)
                .copied()
                .unwrap_or((networks::NetworkDriver::Vxlan, None));
            requested.push(RequestedNetworkSpec {
                name: trimmed.to_string(),
                driver,
                ip_family,
            });
        }
    }

    Ok(requested)
}

/// Validate one manifest host port list before submitting it to the coordinator.
pub fn validate_manifest_ports(ports: &[ManifestPortBinding], context: &str) -> Result<()> {
    let mut names = HashSet::new();
    let mut requests = Vec::new();
    for port in ports {
        let name = port.name.trim();
        if name.is_empty() {
            return Err(anyhow!("{context} declares a port with an empty name"));
        }
        if !names.insert(name.to_string()) {
            return Err(anyhow!("{context} declares port '{}' multiple times", name));
        }
        if port.target == 0 {
            return Err(anyhow!(
                "{context} port '{}' must set target to a non-zero container port",
                name
            ));
        }
        if port.host == 0 {
            return Err(anyhow!(
                "{context} port '{}' must set host to a non-zero static host port",
                name
            ));
        }
        let host_ip = port.host_ip.trim().parse::<IpAddr>().map_err(|_| {
            anyhow!(
                "{context} port '{}' has invalid host_ip '{}'",
                name,
                port.host_ip
            )
        })?;
        let request = ManifestHostPortRequest {
            name: name.to_string(),
            host_ip,
            host: port.host,
            protocol: port.protocol,
        };
        if let Some(existing) = requests
            .iter()
            .find(|existing| manifest_host_ports_conflict(existing, &request))
        {
            return Err(anyhow!(
                "{context} ports '{}' and '{}' both reserve {}/{}",
                existing.name,
                request.name,
                request.host,
                manifest_port_protocol_label(request.protocol)
            ));
        }
        requests.push(request);
    }
    Ok(())
}

/// Return the default host bind address for node-local host ports.
fn default_host_port_ip() -> String {
    "0.0.0.0".to_string()
}

/// Normalized host port request used for duplicate and wildcard conflict detection.
struct ManifestHostPortRequest {
    name: String,
    host_ip: IpAddr,
    host: u16,
    protocol: ManifestPortProtocol,
}

/// Return true when two host port requests would contend for the same local socket.
fn manifest_host_ports_conflict(
    left: &ManifestHostPortRequest,
    right: &ManifestHostPortRequest,
) -> bool {
    left.host == right.host
        && left.protocol == right.protocol
        && same_ip_family(left.host_ip, right.host_ip)
        && (left.host_ip == right.host_ip
            || left.host_ip.is_unspecified()
            || right.host_ip.is_unspecified())
}

/// Return true when two IP addresses belong to the same address family.
fn same_ip_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

/// Render one manifest host port protocol for operator-facing validation errors.
fn manifest_port_protocol_label(protocol: ManifestPortProtocol) -> &'static str {
    match protocol {
        ManifestPortProtocol::Tcp => "tcp",
        ManifestPortProtocol::Udp => "udp",
    }
}

/// Ensure every referenced job or agent manifest network exists before workload submission.
pub async fn ensure_named_networks(
    cfg: &ClientConfig,
    required_networks: impl IntoIterator<Item = RequestedNetworkSpec>,
) -> Result<()> {
    let mut required = Vec::new();
    let mut seen = HashMap::new();
    for network in required_networks {
        let trimmed = network.name.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((existing_driver, existing_family)) = seen.get(trimmed).copied() {
            if existing_driver != network.driver {
                return Err(anyhow!(
                    "manifest requests network '{}' with conflicting drivers",
                    trimmed
                ));
            }
            if let (Some(existing_family), Some(requested_family)) =
                (existing_family, network.ip_family)
                && existing_family != requested_family
            {
                return Err(anyhow!(
                    "manifest requests network '{}' with conflicting IP families",
                    trimmed
                ));
            }
            continue;
        }
        seen.insert(trimmed.to_string(), (network.driver, network.ip_family));
        required.push(RequestedNetworkSpec {
            name: trimmed.to_string(),
            driver: network.driver,
            ip_family: network.ip_family,
        });
    }

    if required.is_empty() {
        return Ok(());
    }

    let existing = networks::list_raw(cfg).await?;
    let existing_by_name: HashMap<String, networks::NetworkSummary> = existing
        .iter()
        .cloned()
        .map(|net| (net.name.clone(), net))
        .collect();
    let mut known_subnets: HashSet<String> =
        existing.iter().map(|net| net.subnet_cidr.clone()).collect();
    let mut targets = Vec::with_capacity(required.len());

    for requested in required {
        if let Some(existing) = existing_by_name.get(&requested.name) {
            validate_existing_manifest_network(existing, &requested)?;
            targets.push(NetworkProvisionTarget {
                id: existing.id,
                name: existing.name.clone(),
            });
            continue;
        }

        let family = requested.ip_family.unwrap_or(cfg.default_network_ip_family);
        let request = networks::default_network_create_request_for_driver(
            requested.name.clone(),
            known_subnets.iter().map(String::as_str),
            family,
            requested.driver,
        );
        match networks::create_raw(cfg, &request).await {
            Ok(network_id) => {
                output::emit_line(format!(
                    "network '{}' created with id {network_id} (auto-provisioned)",
                    requested.name
                ));
                known_subnets.insert(request.subnet_cidr.clone());
                targets.push(NetworkProvisionTarget {
                    id: network_id,
                    name: requested.name.clone(),
                });
            }
            Err(error) => {
                let fallback = networks::list_raw(cfg).await?;
                if let Some(existing) = fallback.iter().find(|net| net.name == requested.name) {
                    validate_existing_manifest_network(existing, &requested)?;
                    let network_name = &requested.name;
                    eprintln!(
                        "warning: auto-provision for network '{network_name}' failed but it already exists: {error}"
                    );
                    targets.push(NetworkProvisionTarget {
                        id: existing.id,
                        name: existing.name.clone(),
                    });
                    continue;
                }
                return Err(error);
            }
        }
    }

    wait_for_manifest_network_readiness(cfg, &targets).await?;
    Ok(())
}

/// Validate that an existing network can satisfy one manifest network request.
fn validate_existing_manifest_network(
    existing: &networks::NetworkSummary,
    requested: &RequestedNetworkSpec,
) -> Result<()> {
    if existing.driver != requested.driver {
        return Err(anyhow!(
            "manifest requests network '{}' with driver {} but an existing network uses driver {}",
            requested.name,
            requested.driver,
            existing.driver
        ));
    }
    Ok(())
}

/// Wait until all manifest-required networks have at least one scheduler-eligible peer.
async fn wait_for_manifest_network_readiness(
    cfg: &ClientConfig,
    targets: &[NetworkProvisionTarget],
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }

    let deadline = Instant::now() + NETWORK_PROVISION_TIMEOUT;
    let mut pending = targets.to_vec();
    let mut announced = HashSet::new();
    let mut last_observed = HashMap::new();

    loop {
        let mut remaining = Vec::new();
        for target in pending {
            let info = networks::inspect_raw(cfg, target.id).await?;
            let observed = NetworkReadinessObservation::from_inspect(&info);
            if observed.is_schedulable() {
                if announced.remove(&target.id) {
                    output::emit_line(format!(
                        "network '{}' ready ({}/{})",
                        target.name, observed.ready_peers, observed.peer_count
                    ));
                }
                continue;
            }

            if let Some(error) = observed.terminal_error() {
                return Err(anyhow!(
                    "network '{}' ({}) failed to become ready: {error}",
                    target.name,
                    target.id
                ));
            }

            if announced.insert(target.id) {
                output::emit_line(format!(
                    "network '{}' waiting for readiness ({})",
                    target.name,
                    observed.summary()
                ));
            }
            last_observed.insert(target.id, observed);
            remaining.push(target);
        }

        if remaining.is_empty() {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for manifest networks to become ready after {}: {}",
                format_network_wait_duration(NETWORK_PROVISION_TIMEOUT),
                render_network_wait_timeout(&remaining, &last_observed)
            ));
        }

        pending = remaining;
        sleep(NETWORK_PROVISION_POLL_INTERVAL).await;
    }
}

/// Render the last observed state for every network that did not become schedulable in time.
fn render_network_wait_timeout(
    targets: &[NetworkProvisionTarget],
    observations: &HashMap<Uuid, NetworkReadinessObservation>,
) -> String {
    targets
        .iter()
        .map(|target| {
            let summary = observations
                .get(&target.id)
                .map(NetworkReadinessObservation::summary)
                .unwrap_or_else(|| "no status observed".to_string());
            format!("{} ({}) {}", target.name, target.id, summary)
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Format one fixed network readiness timeout for operator-facing error messages.
fn format_network_wait_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs > 0 {
        return format!("{secs}s");
    }
    format!("{}ms", duration.as_millis())
}

/// Ensure every declared manifest volume exists as a cluster volume object.
pub async fn ensure_declared_volumes(
    cfg: &ClientConfig,
    declared_volumes: &[DeclaredVolumeSpec],
) -> Result<HashMap<String, ResolvedDeclaredVolume>> {
    if declared_volumes.is_empty() {
        return Ok(HashMap::new());
    }

    let existing = volumes::list_raw(cfg).await?;
    let existing_by_name: HashMap<String, volumes::VolumeSummary> = existing
        .into_iter()
        .map(|volume| (volume.name.clone(), volume))
        .collect();

    let mut resolved = HashMap::new();
    for volume in declared_volumes {
        match volume.driver_kind {
            DeclaredVolumeDriverKind::LocalManaged => {}
            DeclaredVolumeDriverKind::LocalImportedPath => {
                return Err(anyhow!(
                    "manifest volume '{}' cannot use imported_path; import host paths ahead of submission through `mantissa volumes import`",
                    volume.name
                ));
            }
            DeclaredVolumeDriverKind::External => {
                return Err(anyhow!(
                    "manifest volume '{}' cannot use an external driver yet",
                    volume.name
                ));
            }
        }

        let spec = if let Some(existing) = existing_by_name.get(&volume.name) {
            validate_declared_volume_compatibility(existing, volume)?;
            volumes::inspect_raw(cfg, &volume.name).await?.spec
        } else {
            volumes::create_raw(
                cfg,
                &volumes::VolumeCreateRequest {
                    name: volume.name.clone(),
                    ownership: volume.local_ownership.clone().unwrap_or_default(),
                    binding_mode: volume.binding_mode,
                    reclaim_policy: volume.reclaim_policy,
                    requested_bytes: volume
                        .capacity_mb
                        .map(|value| value.saturating_mul(1_048_576)),
                    labels: volume
                        .labels
                        .iter()
                        .map(|label| volumes::VolumeLabel {
                            key: label.key.clone(),
                            value: label.value.clone(),
                        })
                        .collect(),
                    node_selector: None,
                },
            )
            .await?
        };

        resolved.insert(
            volume.name.clone(),
            ResolvedDeclaredVolume {
                volume_id: spec.id,
                volume_name: spec.name,
            },
        );
    }

    Ok(resolved)
}

/// Validates that one existing cluster volume matches one manifest declaration.
fn validate_declared_volume_compatibility(
    existing: &volumes::VolumeSummary,
    declared: &DeclaredVolumeSpec,
) -> Result<()> {
    match (&existing.driver, declared.driver_kind) {
        (volumes::VolumeDriver::LocalManaged, DeclaredVolumeDriverKind::LocalManaged) => {}
        (
            volumes::VolumeDriver::LocalImportedPath(_),
            DeclaredVolumeDriverKind::LocalImportedPath,
        ) => {}
        (volumes::VolumeDriver::External { .. }, DeclaredVolumeDriverKind::External) => {}
        _ => {
            return Err(anyhow!(
                "existing volume '{}' does not match the manifest driver/source kind",
                declared.name
            ));
        }
    }

    if existing.access_mode != declared.access_mode {
        return Err(anyhow!(
            "existing volume '{}' does not match the manifest access_mode",
            declared.name
        ));
    }

    if existing.local_ownership != declared.local_ownership {
        return Err(anyhow!(
            "existing volume '{}' does not match the manifest local ownership policy",
            declared.name
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ready network status alone must not pass the manifest prerequisite gate.
    #[test]
    fn network_readiness_requires_ready_peer() {
        let observed = NetworkReadinessObservation {
            status: networks::NetworkStatus::Ready,
            peer_count: 1,
            ready_peers: 0,
            error_peers: 0,
            first_error: None,
        };

        assert!(!observed.is_schedulable());

        let observed = NetworkReadinessObservation {
            ready_peers: 1,
            ..observed
        };

        assert!(observed.is_schedulable());
    }

    /// A network whose observed peers all errored should surface the platform failure immediately.
    #[test]
    fn network_readiness_reports_terminal_peer_errors() {
        let observed = NetworkReadinessObservation {
            status: networks::NetworkStatus::Provisioning,
            peer_count: 1,
            ready_peers: 0,
            error_peers: 1,
            first_error: Some("failed to attach bpf program".to_string()),
        };

        assert_eq!(
            observed.terminal_error().as_deref(),
            Some("failed to attach bpf program")
        );
    }

    /// Timeout details should include each pending network name and last observed readiness state.
    #[test]
    fn network_wait_timeout_renders_last_observed_state() {
        let network_id = Uuid::new_v4();
        let targets = vec![NetworkProvisionTarget {
            id: network_id,
            name: "frontend".to_string(),
        }];
        let mut observations = HashMap::new();
        observations.insert(
            network_id,
            NetworkReadinessObservation {
                status: networks::NetworkStatus::Pending,
                peer_count: 1,
                ready_peers: 0,
                error_peers: 0,
                first_error: None,
            },
        );

        let rendered = render_network_wait_timeout(&targets, &observations);

        assert!(rendered.contains("frontend"));
        assert!(rendered.contains("ready peers 0/1"));
    }
}
