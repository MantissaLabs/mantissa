use anyhow::Result;
use std::net::IpAddr;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use crate::network::addressing::resolve_advertise_ip;

const NODEPORT_PROTO_TCP: u8 = 6;
const NODEPORT_PROTO_UDP: u8 = 17;
/// Default max entry count for one pinned NodePort VIP publication map.
#[cfg(test)]
const NODEPORT_VIP_CAPACITY: usize = crate::config::DEFAULT_NODEPORT_VIP_CAPACITY;
/// Default max entry count for one pinned NodePort forward or reverse flow map.
#[cfg(test)]
const NODEPORT_FLOW_CAPACITY: usize = crate::config::DEFAULT_NODEPORT_FLOW_CAPACITY;
/// Default max entry count for one pinned NodePort host-access attachment map.
#[cfg(test)]
const NODEPORT_HOST_CAPACITY: usize = crate::config::DEFAULT_NODEPORT_HOST_CAPACITY;
/// Keep the userspace readers aligned with the ingress drop-reason map size in the tc ingress program.
const NODEPORT_INGRESS_DROP_REASON_COUNT: usize = 6;
/// Keep the userspace readers aligned with the shared NodePort flow-event map size in the tc programs.
const NODEPORT_FLOW_EVENT_COUNT: usize = 5;

const NODEPORT_FLOW_CREATE_INDEX: usize = 0;
const NODEPORT_FLOW_CLEAR_INDEX: usize = 1;
const NODEPORT_REVERSE_MISS_INDEX: usize = 2;
const NODEPORT_INVALID_TRANSITION_INDEX: usize = 3;
const NODEPORT_RETURN_BYPASS_INDEX: usize = 4;

/// Capacity limits for the pinned NodePort maps that back publication, host-access SNAT, and
/// public conntrack state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct NodePortMapCapacities {
    vip: usize,
    host: usize,
    flow: usize,
}

impl NodePortMapCapacities {
    /// Resolve the current NodePort map-capacity configuration from the global config snapshot.
    fn from_config() -> Self {
        Self {
            vip: crate::config::nodeport_vip_capacity(),
            host: crate::config::nodeport_host_capacity(),
            flow: crate::config::nodeport_flow_capacity(),
        }
    }

    /// Convert the configured VIP-map capacity into Aya's `u32` max-entry type.
    fn vip_u32(self) -> Result<u32> {
        checked_map_capacity("network.nodeport.vip_capacity", self.vip)
    }

    /// Convert the configured host-access map capacity into Aya's `u32` max-entry type.
    fn host_u32(self) -> Result<u32> {
        checked_map_capacity("network.nodeport.host_capacity", self.host)
    }

    /// Convert the configured public flow-map capacity into Aya's `u32` max-entry type.
    fn flow_u32(self) -> Result<u32> {
        checked_map_capacity("network.nodeport.flow_capacity", self.flow)
    }
}

/// Declarative nodeport mapping that connects an external port to an overlay VIP.
#[derive(Clone, Debug)]
pub struct NodePortMapping {
    pub port: u16,
    pub vip: IpAddr,
    pub vip_port: u16,
    pub protocol: NodePortProtocol,
}

/// Supported nodeport transport protocols for VIP exposure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum NodePortProtocol {
    Tcp,
    Udp,
}

impl NodePortProtocol {
    /// Convert the nodeport protocol to the IP protocol number used in L4 headers.
    pub fn number(self) -> u8 {
        match self {
            NodePortProtocol::Tcp => NODEPORT_PROTO_TCP,
            NodePortProtocol::Udp => NODEPORT_PROTO_UDP,
        }
    }
}

/// Runtime lifecycle for the local nodeport dataplane manager.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodePortRuntimeState {
    Disabled,
    Pending,
    Ready,
    Degraded,
}

/// Aggregated packet counters for packets that matched the published NodePort dataplane path.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodePortPacketCounters {
    pub packets: u64,
    pub bytes: u64,
    pub drops: u64,
}

/// Breakdown of the ingress drop paths currently tracked by the NodePort tc program.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodePortIngressDropReasons {
    pub invalid_ipv4_headers: u64,
    pub invalid_l4_headers: u64,
    pub missing_host_entries: u64,
    pub nat_insert_failures: u64,
    pub rewrite_failures: u64,
    pub fragmented_ipv4_packets: u64,
}

/// Aggregated flow diagnostics for the shared NodePort conntrack caches.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodePortFlowDiagnostics {
    pub ipv4_flow_pairs: usize,
    pub ipv6_flow_pairs: usize,
    pub flow_creates: u64,
    pub flow_clears: u64,
    pub estimated_flow_evictions: u64,
    pub reverse_misses: u64,
    pub invalid_conntrack_transitions: u64,
    pub return_path_bypass_packets: u64,
}

/// Why the current NodePort publication identity was chosen.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodePortIdentitySource {
    NodePortIp,
    AdvertiseAddr,
    InterfaceAddress,
    Autodetect,
}

impl NodePortIdentitySource {
    /// Render one stable source label for diagnostics and operator-facing status output.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodePortIp => "nodeport_ip",
            Self::AdvertiseAddr => "advertise_addr",
            Self::InterfaceAddress => "iface_address",
            Self::Autodetect => "autodetect",
        }
    }
}

impl std::fmt::Display for NodePortIdentitySource {
    /// Render the chosen NodePort identity source as stable text for logs and RPC output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Snapshot of node-local nodeport capability and resolved external identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePortStatus {
    pub desired_enabled: bool,
    pub state: NodePortRuntimeState,
    pub source_mode: crate::config::NodePortSourceMode,
    pub identity_source: Option<NodePortIdentitySource>,
    pub resolved_iface: Option<String>,
    pub resolved_node_ip: Option<IpAddr>,
    pub active_networks: usize,
    pub active_ports: usize,
    pub active_host_networks: usize,
    pub vip_capacity: usize,
    pub host_capacity: usize,
    pub flow_capacity: usize,
    pub ingress_stats: Option<NodePortPacketCounters>,
    pub ingress_drop_reasons: Option<NodePortIngressDropReasons>,
    pub egress_stats: Option<NodePortPacketCounters>,
    pub flow_diagnostics: Option<NodePortFlowDiagnostics>,
    pub last_error: Option<String>,
    pub stats_error: Option<String>,
}

impl std::fmt::Display for NodePortRuntimeState {
    /// Render a stable, human-readable runtime state label for diagnostics and RPC output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodePortRuntimeState::Disabled => f.write_str("disabled"),
            NodePortRuntimeState::Pending => f.write_str("pending"),
            NodePortRuntimeState::Ready => f.write_str("ready"),
            NodePortRuntimeState::Degraded => f.write_str("degraded"),
        }
    }
}

/// Pick the configured NodePort IP identity, preferring the explicit `network.nodeport.ip`
/// override over the advertise address when both exist.
fn configured_node_ip_from_sources(
    configured_node_ip: Option<IpAddr>,
    advertise_addr: Option<&str>,
) -> Option<IpAddr> {
    configured_node_ip.or_else(|| advertise_addr.and_then(resolve_advertise_ip))
}

/// Identify which explicit configuration source currently supplies the NodePort publication IP.
fn configured_node_ip_source(
    configured_node_ip: Option<IpAddr>,
    advertise_addr: Option<&str>,
) -> Option<NodePortIdentitySource> {
    if configured_node_ip.is_some() {
        return Some(NodePortIdentitySource::NodePortIp);
    }
    if advertise_addr.and_then(resolve_advertise_ip).is_some() {
        return Some(NodePortIdentitySource::AdvertiseAddr);
    }
    None
}

/// Convert one configured NodePort map capacity into the `u32` value expected by Aya before the
/// kernel creates the pinned map.
fn checked_map_capacity(name: &str, value: usize) -> Result<u32> {
    u32::try_from(value)
        .map_err(|_| anyhow::anyhow!("configured {name} exceeds the kernel map size limit"))
}

/// Estimate how many tracked NodePort flow pairs were evicted from the LRU maps by comparing the
/// total successful flow creations against explicit clears and the current forward-map occupancy.
fn estimated_flow_evictions(
    flow_creates: u64,
    flow_clears: u64,
    ipv4_flow_pairs: usize,
    ipv6_flow_pairs: usize,
) -> u64 {
    let active_pairs = (ipv4_flow_pairs as u64).saturating_add(ipv6_flow_pairs as u64);
    flow_creates.saturating_sub(flow_clears.saturating_add(active_pairs))
}

/// Project the active-public-network count after one NodePort sync applies.
fn projected_active_networks_after_sync(
    current_active_networks: usize,
    had_ports: bool,
    wants_mappings: bool,
) -> usize {
    match (had_ports, wants_mappings) {
        (true, false) => current_active_networks.saturating_sub(1),
        (false, true) => current_active_networks + 1,
        _ => current_active_networks,
    }
}

/// Return the first fixed-capacity violation that would make one NodePort sync unsafe to apply.
fn nodeport_capacity_error(
    projected_active_ports: usize,
    projected_active_networks: usize,
    capacities: NodePortMapCapacities,
) -> Option<String> {
    if projected_active_ports > capacities.vip {
        return Some(format!(
            "nodeport VIP capacity exceeded: requested {projected_active_ports} active ports, limit {}",
            capacities.vip
        ));
    }
    if projected_active_networks > capacities.host {
        return Some(format!(
            "nodeport host-access capacity exceeded: requested {projected_active_networks} active public networks, limit {}",
            capacities.host
        ));
    }
    None
}

impl std::fmt::Display for NodePortProtocol {
    /// Render a stable, human-readable protocol label for logs and diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodePortProtocol::Tcp => f.write_str("tcp"),
            NodePortProtocol::Udp => f.write_str("udp"),
        }
    }
}

/// Maintain host-NIC nodeport programs and their associated dataplane maps.
#[derive(Clone)]
pub struct NodePortManager {
    inner: Arc<AsyncMutex<PlatformNodePortManager>>,
}

impl Default for NodePortManager {
    fn default() -> Self {
        Self::new()
    }
}

impl NodePortManager {
    /// Build a nodeport manager using environment configuration for external interfaces.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(PlatformNodePortManager::new())),
        }
    }

    /// Synchronize nodeport mappings for a specific network so external traffic can reach VIPs.
    pub async fn sync_ports(&self, network_id: Uuid, entries: &[NodePortMapping]) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.sync_ports(network_id, entries).await
    }

    /// Return the current node-local nodeport runtime status for diagnostics.
    pub async fn status(&self) -> NodePortStatus {
        let guard = self.inner.lock().await;
        guard.status()
    }
}

#[cfg(not(target_os = "linux"))]
struct PlatformNodePortManager;

#[cfg(not(target_os = "linux"))]
impl PlatformNodePortManager {
    /// Create a disabled manager on unsupported platforms so callers can stay portable.
    fn new() -> Self {
        Self
    }

    /// Ignore sync requests on unsupported platforms.
    async fn sync_ports(&mut self, _network_id: Uuid, _entries: &[NodePortMapping]) -> Result<()> {
        Ok(())
    }

    /// Return a disabled runtime snapshot on unsupported platforms.
    fn status(&self) -> NodePortStatus {
        let capacities = NodePortMapCapacities::from_config();
        NodePortStatus {
            desired_enabled: false,
            state: NodePortRuntimeState::Disabled,
            source_mode: crate::config::nodeport_source_mode(),
            identity_source: None,
            resolved_iface: None,
            resolved_node_ip: None,
            active_networks: 0,
            active_ports: 0,
            active_host_networks: 0,
            vip_capacity: capacities.vip,
            host_capacity: capacities.host,
            flow_capacity: capacities.flow,
            ingress_stats: None,
            ingress_drop_reasons: None,
            egress_stats: None,
            flow_diagnostics: None,
            last_error: Some("nodeport is only available on linux".to_string()),
            stats_error: None,
        }
    }
}

mod platform;

#[cfg(target_os = "linux")]
use self::platform::PlatformNodePortManager;

#[cfg(test)]
mod tests;
