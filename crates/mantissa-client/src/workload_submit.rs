use crate::config::ClientConfig;
use crate::config::NetworkIpFamily;
use crate::networks;
use crate::volumes;
use anyhow::{Result, anyhow};
use blake3::Hasher;
use serde::{Deserialize, Deserializer};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use uuid::Uuid;

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

/// Candidate ranking mode applied after hard placement filters pass.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlacementStrategy {
    #[default]
    Spread,
    Binpack,
}

/// Typed scheduler-visible field used by one hard placement constraint.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlacementConstraintSelector {
    NodeId,
    NodeHostname,
    NodeIp,
    NodeAddress,
    NodePlatformOs,
    NodePlatformArch,
    NodeLabel { key: String },
}

impl PlacementConstraintSelector {
    /// Builds one typed node-label selector for manifest authors constructing constraints in code.
    pub fn node_label(key: impl Into<String>) -> Self {
        Self::NodeLabel { key: key.into() }
    }

    /// Returns the stable operator-facing selector key for diagnostics and validation errors.
    fn render_key(&self) -> String {
        match self {
            Self::NodeId => "node.id".to_string(),
            Self::NodeHostname => "node.hostname".to_string(),
            Self::NodeIp => "node.ip".to_string(),
            Self::NodeAddress => "node.address".to_string(),
            Self::NodePlatformOs => "node.platform.os".to_string(),
            Self::NodePlatformArch => "node.platform.arch".to_string(),
            Self::NodeLabel { key } => format!("node.labels.{key}"),
        }
    }
}

/// Supported comparison operators for hard placement constraints.
#[derive(Debug, Deserialize, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlacementConstraintOperator {
    #[default]
    Eq,
    Ne,
}

/// One hard placement predicate interpreted against a candidate node.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct PlacementConstraint {
    pub selector: PlacementConstraintSelector,
    #[serde(default)]
    pub operator: PlacementConstraintOperator,
    pub value: String,
}

impl PlacementConstraint {
    /// Builds one typed equality constraint for callers constructing manifests in code.
    pub fn eq(selector: PlacementConstraintSelector, value: impl Into<String>) -> Self {
        Self {
            selector,
            operator: PlacementConstraintOperator::Eq,
            value: value.into(),
        }
    }

    /// Builds one typed inequality constraint for callers constructing manifests in code.
    pub fn ne(selector: PlacementConstraintSelector, value: impl Into<String>) -> Self {
        Self {
            selector,
            operator: PlacementConstraintOperator::Ne,
            value: value.into(),
        }
    }
}

/// Generic workload placement policy shared by services, jobs, and agents.
#[derive(Debug, Default, Deserialize, Clone, PartialEq, Eq)]
pub struct PlacementSpec {
    #[serde(default)]
    pub constraints: Vec<PlacementConstraint>,
    #[serde(default)]
    pub strategy: PlacementStrategy,
}

/// Admission behavior requested by a manifest-level workload controller.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadAdmissionMode {
    /// Batch-aware placement without a strict all-or-nothing admission barrier.
    #[default]
    Incremental,
    /// Strict grouped admission for controllers that launch multiple workloads together.
    Gang,
}

/// Shared manifest-side admission policy for controller-owned workload groups.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkloadAdmissionPolicy {
    #[serde(default)]
    pub mode: WorkloadAdmissionMode,
}

impl Default for WorkloadAdmissionPolicy {
    fn default() -> Self {
        Self {
            mode: WorkloadAdmissionMode::Incremental,
        }
    }
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

/// Validate one generic workload placement policy before submission.
pub fn validate_placement(policy: &PlacementSpec, context: &str) -> Result<()> {
    validate_placement_constraints(&policy.constraints, context)
}

/// Validate one hard placement constraint list before submission.
pub fn validate_placement_constraints(
    constraints: &[PlacementConstraint],
    context: &str,
) -> Result<()> {
    for constraint in constraints {
        validate_constraint(constraint).map_err(|message| {
            anyhow!("{context} defines an invalid placement constraint: {message}")
        })?;
    }

    Ok(())
}

/// Performs lightweight local validation for one typed placement constraint.
fn validate_constraint(constraint: &PlacementConstraint) -> std::result::Result<(), String> {
    let selector_key = constraint.selector.render_key();
    let value = constraint.value.trim();
    if value.is_empty() {
        return Err(format!(
            "constraint for selector '{}' must include a non-empty value",
            selector_key
        ));
    }

    match &constraint.selector {
        PlacementConstraintSelector::NodeLabel { key } if key.trim().is_empty() => {
            return Err("node_label selector requires a non-empty key".to_string());
        }
        PlacementConstraintSelector::NodeIp if !is_valid_ip_or_cidr(value) => {
            return Err(format!(
                "selector '{}' requires an IP address or CIDR value, got '{}'",
                selector_key, constraint.value
            ));
        }
        _ => {}
    }

    Ok(())
}

/// Returns true when the provided string encodes either one IP address or one CIDR prefix.
fn is_valid_ip_or_cidr(value: &str) -> bool {
    if value.parse::<IpAddr>().is_ok() {
        return true;
    }

    parse_cidr(value).is_some()
}

/// Parses one CIDR string into a network IP and prefix length.
fn parse_cidr(value: &str) -> Option<(IpAddr, u8)> {
    let (network_text, prefix_text) = value.split_once('/')?;
    let network = network_text.parse::<IpAddr>().ok()?;
    let prefix = prefix_text.parse::<u8>().ok()?;
    let max_prefix = match network {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };

    (prefix <= max_prefix).then_some((network, prefix))
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

/// Ensure every declared manifest volume exists as a cluster volume object.
pub async fn ensure_declared_volumes(
    cfg: &ClientConfig,
    declared_volumes: &[DeclaredVolumeSpec],
) -> Result<HashMap<String, ResolvedDeclaredVolume>> {
    if declared_volumes.is_empty() {
        return Ok(HashMap::new());
    }

    let existing = volumes::list(cfg).await?;
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
            volumes::inspect(cfg, &volume.name).await?.spec
        } else {
            volumes::create_with_request(
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
