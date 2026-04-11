use anyhow::Result;
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

const NODEPORT_PROTO_TCP: u8 = 6;
const NODEPORT_PROTO_UDP: u8 = 17;
/// Keep the userspace capacity checks aligned with the pinned VIP map size in the tc ingress program.
const NODEPORT_VIP_CAPACITY: usize = 1024;
/// Keep the userspace capacity checks aligned with the pinned NAT flow maps in the tc programs.
const NODEPORT_FLOW_CAPACITY: usize = 2048;
/// Keep the userspace capacity checks aligned with the pinned host-access map size in the tc ingress program.
const NODEPORT_HOST_CAPACITY: usize = 256;

/// Declarative nodeport mapping that connects an external port to an overlay VIP.
#[derive(Clone, Debug)]
pub struct NodePortMapping {
    pub port: u16,
    pub vip: Ipv4Addr,
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

/// Aggregated packet counters read from the pinned NodePort eBPF stats maps.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NodePortPacketCounters {
    pub packets: u64,
    pub bytes: u64,
    pub drops: u64,
}

/// Snapshot of node-local nodeport capability and resolved external identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePortStatus {
    pub desired_enabled: bool,
    pub state: NodePortRuntimeState,
    pub resolved_iface: Option<String>,
    pub resolved_node_ip: Option<Ipv4Addr>,
    pub active_networks: usize,
    pub active_ports: usize,
    pub active_host_networks: usize,
    pub vip_capacity: usize,
    pub host_capacity: usize,
    pub flow_capacity: usize,
    pub ingress_stats: Option<NodePortPacketCounters>,
    pub egress_stats: Option<NodePortPacketCounters>,
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

/// # Description:
///
/// Resolve an IPv4 address from one operator-configured advertise address when
/// the address is expressed as a literal socket or a resolvable hostname.
fn resolve_advertise_ipv4(addr: &str) -> Option<Ipv4Addr> {
    addr.to_socket_addrs()
        .ok()?
        .find_map(|socket| match socket.ip() {
            IpAddr::V4(ip) => Some(ip),
            IpAddr::V6(_) => None,
        })
}

/// # Description:
///
/// Pick the configured NodePort IPv4 identity, preferring the explicit
/// `network.nodeport.ip` override over the advertise address when both exist.
fn configured_node_ip_from_sources(
    configured_node_ip: Option<Ipv4Addr>,
    advertise_addr: Option<&str>,
) -> Option<Ipv4Addr> {
    configured_node_ip.or_else(|| advertise_addr.and_then(resolve_advertise_ipv4))
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
) -> Option<String> {
    if projected_active_ports > NODEPORT_VIP_CAPACITY {
        return Some(format!(
            "nodeport VIP capacity exceeded: requested {projected_active_ports} active ports, limit {NODEPORT_VIP_CAPACITY}"
        ));
    }
    if projected_active_networks > NODEPORT_HOST_CAPACITY {
        return Some(format!(
            "nodeport host-access capacity exceeded: requested {projected_active_networks} active public networks, limit {NODEPORT_HOST_CAPACITY}"
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
        NodePortStatus {
            desired_enabled: false,
            state: NodePortRuntimeState::Disabled,
            resolved_iface: None,
            resolved_node_ip: None,
            active_networks: 0,
            active_ports: 0,
            active_host_networks: 0,
            vip_capacity: NODEPORT_VIP_CAPACITY,
            host_capacity: NODEPORT_HOST_CAPACITY,
            flow_capacity: NODEPORT_FLOW_CAPACITY,
            ingress_stats: None,
            egress_stats: None,
            last_error: Some("nodeport is only available on linux".to_string()),
            stats_error: None,
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{
        NODEPORT_FLOW_CAPACITY, NODEPORT_HOST_CAPACITY, NODEPORT_VIP_CAPACITY, NodePortMapping,
        NodePortPacketCounters, NodePortProtocol, NodePortRuntimeState, NodePortStatus,
        configured_node_ip_from_sources, nodeport_capacity_error,
        projected_active_networks_after_sync,
    };
    use crate::config;
    use crate::network::attachment::host_access_host_iface_name;
    use crate::network::wireguard::MANTISSA_WIREGUARD_IFNAME;
    use anyhow::{Context, Result, anyhow};
    use aya::Pod;
    use aya::maps::{Map, MapData, PerCpuArray};
    use aya::programs::ProgramError;
    use aya::programs::tc::{
        SchedClassifier, TcAttachType, qdisc_add_clsact, qdisc_detach_program,
    };
    use aya::{Ebpf, EbpfLoader};
    use futures::TryStreamExt;
    use libc::if_nametoindex;
    use nix::mount::{MsFlags, mount};
    use nix::sys::statfs::{BPF_FS_MAGIC, statfs};
    use rtnetlink::new_connection;
    use rtnetlink::packet_route::address::AddressAttribute;
    use rtnetlink::packet_route::link::{LinkAttribute, LinkFlags};
    use std::collections::{HashMap, HashSet};
    use std::env;
    use std::ffi::CString;
    use std::fs;
    use std::io;
    use std::mem;
    use std::net::{IpAddr, Ipv4Addr};
    use std::os::fd::{AsFd, AsRawFd};
    use std::path::{Path, PathBuf};
    use tracing::{debug, info, warn};
    use uuid::Uuid;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct NodePortKey {
        port: u16,
        proto: u8,
        _pad: u8,
    }
    unsafe impl Pod for NodePortKey {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct NodePortEntry {
        vip: u32,
        vip_port: u16,
        _pad: u16,
        overlay_ifindex: u32,
        node_ip: u32,
    }
    unsafe impl Pod for NodePortEntry {}

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct NodePortHost {
        mac: [u8; 6],
        _pad: u16,
        host_ip: u32,
    }
    unsafe impl Pod for NodePortHost {}
    unsafe impl Pod for NodePortPacketCounters {}

    /// Uniquely identify a nodeport binding by port and transport protocol.
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct NodePortSelector {
        port: u16,
        protocol: NodePortProtocol,
    }

    impl NodePortSelector {
        /// Build a selector for nodeport ownership and deduplication.
        fn new(port: u16, protocol: NodePortProtocol) -> Self {
            Self { port, protocol }
        }
    }

    struct NodePortAttachment {
        _ingress: Ebpf,
        egress: Ebpf,
    }

    /// Linux implementation that loads nodeport tc programs and keeps their maps synchronized.
    pub(super) struct PlatformNodePortManager {
        desired_enabled: bool,
        configured_iface: Option<String>,
        configured_node_ip: Option<Ipv4Addr>,
        configured_advertise_addr: Option<String>,
        iface: Option<String>,
        node_ip: Option<Ipv4Addr>,
        attached_iface: Option<String>,
        attached_node_ip: Option<Ipv4Addr>,
        attachment: Option<NodePortAttachment>,
        ports_by_network: HashMap<Uuid, HashSet<NodePortSelector>>,
        port_owner: HashMap<NodePortSelector, Uuid>,
        host_ingress_attached: HashSet<Uuid>,
        host_ingress_ifindex: HashMap<Uuid, u32>,
        runtime_state: NodePortRuntimeState,
        last_error: Option<String>,
    }

    impl PlatformNodePortManager {
        /// Capture nodeport configuration from the global config for later attachment.
        pub(super) fn new() -> Self {
            let configured_iface = config::nodeport_iface();
            let configured_node_ip = config::nodeport_ip();
            let configured_advertise_addr = config::advertise_addr();
            let mut desired_enabled = config::nodeport_enabled();
            let initial_error = if desired_enabled && !config::bpf_attach_enabled() {
                debug!(
                    target: "network",
                    "nodeport disabled because bpf attachment is disabled"
                );
                desired_enabled = false;
                Some("nodeport disabled because bpf attachment is disabled".to_string())
            } else {
                None
            };

            let mut manager = Self {
                desired_enabled,
                configured_iface: configured_iface.clone(),
                configured_node_ip,
                configured_advertise_addr,
                iface: configured_iface,
                node_ip: configured_node_ip,
                attached_iface: None,
                attached_node_ip: None,
                attachment: None,
                ports_by_network: HashMap::new(),
                port_owner: HashMap::new(),
                host_ingress_attached: HashSet::new(),
                host_ingress_ifindex: HashMap::new(),
                runtime_state: if desired_enabled {
                    NodePortRuntimeState::Pending
                } else {
                    NodePortRuntimeState::Disabled
                },
                last_error: None,
            };

            manager.set_runtime_status(
                manager.runtime_state,
                initial_error,
                "nodeport runtime initialized",
            );
            manager
        }

        /// Sync the nodeport map to match the declared mappings for a network.
        pub(super) async fn sync_ports(
            &mut self,
            network_id: Uuid,
            entries: &[NodePortMapping],
        ) -> Result<()> {
            let wants_mappings = !entries.is_empty();
            if !self.desired_enabled {
                self.set_runtime_status(
                    NodePortRuntimeState::Disabled,
                    self.last_error.clone(),
                    "nodeport runtime disabled",
                );
                if wants_mappings {
                    return Err(anyhow!(
                        "{}",
                        self.last_error
                            .clone()
                            .unwrap_or_else(|| "nodeport runtime disabled".to_string())
                    ));
                }
                return Ok(());
            }
            let had_ports = self
                .ports_by_network
                .get(&network_id)
                .map(|ports| !ports.is_empty())
                .unwrap_or(false);
            if entries.is_empty() && !had_ports {
                return Ok(());
            }
            let desired_ports = self.collect_desired_ports(network_id, entries)?;
            self.ensure_sync_capacity(network_id, had_ports, &desired_ports)?;
            if !self.ensure_runtime_capable().await? {
                if wants_mappings {
                    return Err(anyhow!(
                        "{}",
                        self.last_error
                            .clone()
                            .unwrap_or_else(|| { "nodeport runtime preflight failed".to_string() })
                    ));
                }
                return Ok(());
            }
            if let Err(err) = self.ensure_attached().await {
                self.degrade_runtime(
                    format!("nodeport attach failed: {err:#}"),
                    "nodeport runtime degraded",
                );
                if wants_mappings {
                    return Err(anyhow!(
                        "{}",
                        self.last_error
                            .clone()
                            .unwrap_or_else(|| format!("nodeport attach failed: {err:#}"))
                    ));
                }
                return Ok(());
            }
            if wants_mappings && let Err(err) = self.ensure_host_ingress(network_id).await {
                self.degrade_runtime(
                    format!("nodeport host-access attach failed for network {network_id}: {err:#}"),
                    "nodeport runtime degraded",
                );
                return Err(anyhow!(
                    "{}",
                    self.last_error.clone().unwrap_or_else(|| {
                        format!(
                            "nodeport host-access attach failed for network {network_id}: {err:#}"
                        )
                    })
                ));
            }
            self.set_runtime_status(NodePortRuntimeState::Ready, None, "nodeport runtime ready");

            let node_ip = self
                .node_ip
                .ok_or_else(|| anyhow!("nodeport node_ip missing"))?;
            let overlay_ifindex_opt = if entries.is_empty() {
                None
            } else {
                Some(overlay_ifindex(network_id)?)
            };
            let base = map_pin_dir()?;
            let vip_map = open_map(&base, "NODEPORT_VIPS").context("open NODEPORT_VIPS map")?;
            let vip_fd = vip_map.fd().as_fd().as_raw_fd();
            if let Some(overlay_ifindex) = overlay_ifindex_opt {
                let host_mac = host_access_mac(network_id).await?;
                let host_ip = host_access_ip(network_id).await?;
                let host_map =
                    open_map(&base, "NODEPORT_HOST").context("open NODEPORT_HOST map")?;
                let host_fd = host_map.fd().as_fd().as_raw_fd();
                let value = NodePortHost {
                    mac: host_mac,
                    _pad: 0,
                    host_ip: u32::from_ne_bytes(host_ip.octets()),
                };
                update_elem(host_fd, &overlay_ifindex, &value)
                    .context("program nodeport host attachment")?;
            } else if had_ports
                && let Ok(overlay_ifindex) = overlay_ifindex(network_id)
                && let Ok(host_map) = open_map(&base, "NODEPORT_HOST")
            {
                let host_fd = host_map.fd().as_fd().as_raw_fd();
                let _ = delete_elem(host_fd, &overlay_ifindex);
            }
            let overlay_index = if entries.is_empty() {
                0
            } else {
                overlay_ifindex_opt.ok_or_else(|| anyhow!("nodeport overlay ifindex missing"))?
            };

            for entry in entries {
                let selector = NodePortSelector::new(entry.port, entry.protocol);
                let key = NodePortKey {
                    port: entry.port.to_be(),
                    proto: entry.protocol.number(),
                    _pad: 0,
                };
                let value = NodePortEntry {
                    vip: u32::from_ne_bytes(entry.vip.octets()),
                    vip_port: entry.vip_port.to_be(),
                    _pad: 0,
                    overlay_ifindex: overlay_index,
                    node_ip: u32::from_ne_bytes(node_ip.octets()),
                };
                update_elem(vip_fd, &key, &value)
                    .with_context(|| format!("program nodeport {}", entry.port))?;
                self.port_owner.insert(selector, network_id);
            }

            let known = self.ports_by_network.entry(network_id).or_default().clone();
            for selector in known.difference(&desired_ports) {
                let key = NodePortKey {
                    port: selector.port.to_be(),
                    proto: selector.protocol.number(),
                    _pad: 0,
                };
                delete_elem(vip_fd, &key)
                    .with_context(|| format!("remove nodeport {}", selector.port))?;
                self.port_owner.remove(selector);
            }
            self.ports_by_network.insert(network_id, desired_ports);
            if entries.is_empty() {
                self.host_ingress_attached.remove(&network_id);
                self.host_ingress_ifindex.remove(&network_id);
            }
            if self.port_owner.is_empty() {
                self.detach_if_idle().await?;
            }
            Ok(())
        }

        /// Collect the desired port selectors for one network and reject duplicate or conflicting claims.
        fn collect_desired_ports(
            &self,
            network_id: Uuid,
            entries: &[NodePortMapping],
        ) -> Result<HashSet<NodePortSelector>> {
            let mut desired_ports = HashSet::new();
            for entry in entries {
                let selector = NodePortSelector::new(entry.port, entry.protocol);
                if !desired_ports.insert(selector) {
                    return Err(anyhow!(
                        "nodeport {} {} is declared more than once for network {}",
                        entry.port,
                        entry.protocol,
                        network_id
                    ));
                }
                if let Some(owner) = self.port_owner.get(&selector)
                    && *owner != network_id
                {
                    return Err(anyhow!(
                        "nodeport {} {} is already owned by network {}",
                        entry.port,
                        entry.protocol,
                        owner
                    ));
                }
            }
            Ok(desired_ports)
        }

        /// Fail fast when the requested sync would exceed the fixed NodePort map capacities.
        fn ensure_sync_capacity(
            &mut self,
            network_id: Uuid,
            had_ports: bool,
            desired_ports: &HashSet<NodePortSelector>,
        ) -> Result<()> {
            let current_ports = self
                .ports_by_network
                .get(&network_id)
                .map(HashSet::len)
                .unwrap_or(0);
            let projected_active_ports =
                self.port_owner.len() - current_ports + desired_ports.len();
            let current_active_networks = self
                .ports_by_network
                .values()
                .filter(|ports| !ports.is_empty())
                .count();
            let projected_active_networks = projected_active_networks_after_sync(
                current_active_networks,
                had_ports,
                !desired_ports.is_empty(),
            );
            if let Some(error) =
                nodeport_capacity_error(projected_active_ports, projected_active_networks)
            {
                self.degrade_runtime(error.clone(), "nodeport runtime degraded");
                return Err(anyhow!(error));
            }

            Ok(())
        }

        /// Return the current runtime snapshot for diagnostics and future status plumbing.
        pub(super) fn status(&self) -> NodePortStatus {
            let mut status = self.status_snapshot();
            if self.attachment.is_none() {
                return status;
            }

            match self.read_dataplane_counters() {
                Ok((ingress, egress)) => {
                    status.ingress_stats = Some(ingress);
                    status.egress_stats = Some(egress);
                }
                Err(err) => {
                    status.stats_error = Some(err.to_string());
                }
            }

            status
        }

        /// Build one status snapshot from the manager's current resolved identity and state.
        fn status_snapshot(&self) -> NodePortStatus {
            let active_networks = self
                .ports_by_network
                .values()
                .filter(|ports| !ports.is_empty())
                .count();
            NodePortStatus {
                desired_enabled: self.desired_enabled,
                state: self.runtime_state,
                resolved_iface: self.iface.clone(),
                resolved_node_ip: self.node_ip,
                active_networks,
                active_ports: self.port_owner.len(),
                active_host_networks: self.host_ingress_attached.len(),
                vip_capacity: NODEPORT_VIP_CAPACITY,
                host_capacity: NODEPORT_HOST_CAPACITY,
                flow_capacity: NODEPORT_FLOW_CAPACITY,
                ingress_stats: None,
                egress_stats: None,
                last_error: self.last_error.clone(),
                stats_error: None,
            }
        }

        /// Read and aggregate the ingress and egress packet counters from the pinned NodePort stats maps.
        fn read_dataplane_counters(
            &self,
        ) -> Result<(NodePortPacketCounters, NodePortPacketCounters)> {
            Ok((
                read_counter_map("NODEPORT_TC_INGRESS_STATS")?,
                read_counter_map("NODEPORT_TC_EGRESS_STATS")?,
            ))
        }

        /// Record one runtime state transition and log it only when the visible snapshot changed.
        fn set_runtime_status(
            &mut self,
            state: NodePortRuntimeState,
            last_error: Option<String>,
            message: &'static str,
        ) {
            let previous = self.status_snapshot();
            self.runtime_state = state;
            self.last_error = last_error;
            let current = self.status_snapshot();
            if current == previous {
                return;
            }

            match current.state {
                NodePortRuntimeState::Disabled | NodePortRuntimeState::Pending => {
                    debug!(
                        target: "network",
                        desired_enabled = current.desired_enabled,
                        state = ?current.state,
                        iface = ?current.resolved_iface,
                        node_ip = ?current.resolved_node_ip,
                        last_error = ?current.last_error,
                        active_networks = current.active_networks,
                        active_ports = current.active_ports,
                        "{message}"
                    );
                }
                NodePortRuntimeState::Ready => {
                    info!(
                        target: "network",
                        desired_enabled = current.desired_enabled,
                        state = ?current.state,
                        iface = ?current.resolved_iface,
                        node_ip = ?current.resolved_node_ip,
                        active_networks = current.active_networks,
                        active_ports = current.active_ports,
                        "{message}"
                    );
                }
                NodePortRuntimeState::Degraded => {
                    warn!(
                        target: "network",
                        desired_enabled = current.desired_enabled,
                        state = ?current.state,
                        iface = ?current.resolved_iface,
                        node_ip = ?current.resolved_node_ip,
                        last_error = ?current.last_error,
                        active_networks = current.active_networks,
                        active_ports = current.active_ports,
                        "{message}"
                    );
                }
            }
        }

        /// Record one runtime degradation while preserving the current desired config and identity.
        fn degrade_runtime(&mut self, error: impl Into<String>, message: &'static str) {
            self.set_runtime_status(NodePortRuntimeState::Degraded, Some(error.into()), message);
        }

        /// Re-resolve the runtime iface and node IP from explicit config before any fallback.
        async fn refresh_runtime_identity(&mut self) -> Result<()> {
            self.iface = self.configured_iface.clone();
            self.node_ip = configured_node_ip_from_sources(
                self.configured_node_ip,
                self.configured_advertise_addr.as_deref(),
            );

            if let Some(iface) = self.iface.clone() {
                if self.node_ip.is_none() {
                    self.node_ip = detect_iface_ip(&iface).await?;
                }
                return Ok(());
            }

            if let Some(node_ip) = self.node_ip {
                self.iface = detect_iface_for_ip(node_ip).await?;
                return Ok(());
            }

            if let Some((iface, ip)) = detect_default_iface().await? {
                self.iface = Some(iface);
                self.node_ip = Some(ip);
            }

            Ok(())
        }

        /// Check whether nodeport has enough local capability to attempt program attachment.
        async fn ensure_runtime_capable(&mut self) -> Result<bool> {
            self.refresh_runtime_identity().await?;

            let Some(iface) = self.iface.clone() else {
                self.degrade_runtime(
                    "nodeport interface missing; set network.nodeport.iface or configure a reachable advertise address",
                    "nodeport runtime degraded",
                );
                return Ok(false);
            };
            if self.node_ip.is_none() {
                self.degrade_runtime(
                    format!(
                        "nodeport IPv4 address missing for interface {iface}; set network.nodeport.ip or a concrete advertise address"
                    ),
                    "nodeport runtime degraded",
                );
                return Ok(false);
            }

            if let Err(err) = ifindex(&iface) {
                self.degrade_runtime(
                    format!("nodeport interface {iface} is unavailable: {err:#}"),
                    "nodeport runtime degraded",
                );
                return Ok(false);
            }

            let resolver = ArtifactResolver::new();
            for name in ["nodeport_tc_ingress", "nodeport_tc_egress"] {
                if let Err(err) = resolver.resolve(name) {
                    self.degrade_runtime(
                        format!("nodeport artifact {name} missing: {err:#}"),
                        "nodeport runtime degraded",
                    );
                    return Ok(false);
                }
            }

            if let Err(err) = map_pin_dir() {
                self.degrade_runtime(
                    format!("nodeport bpffs setup failed: {err:#}"),
                    "nodeport runtime degraded",
                );
                return Ok(false);
            }

            self.set_runtime_status(
                if self.attachment.is_some() {
                    NodePortRuntimeState::Ready
                } else {
                    NodePortRuntimeState::Pending
                },
                None,
                "nodeport runtime preflight passed",
            );
            Ok(true)
        }

        /// Attach nodeport programs to the configured external interface if not already attached.
        async fn ensure_attached(&mut self) -> Result<()> {
            let Some(iface) = self.iface.clone() else {
                return Err(anyhow!("nodeport interface missing after preflight"));
            };
            let Some(node_ip) = self.node_ip else {
                return Err(anyhow!("nodeport IP missing after preflight"));
            };
            if self.attachment.is_some()
                && self.attached_iface.as_deref() == Some(iface.as_str())
                && self.attached_node_ip == Some(node_ip)
            {
                return Ok(());
            }

            if self.attachment.is_some() {
                self.detach_attachment().await?;
            }
            let ifindex = ifindex(&iface)?;
            ensure_clsact(&iface)?;

            let map_root = map_pin_dir()?;
            if let Err(err) = reset_nodeport_maps(&map_root) {
                warn!(
                    target: "network",
                    "nodeport map reset failed (continuing): {err:#}"
                );
            }

            let mut ingress =
                load_program("nodeport_tc_ingress").context("load nodeport ingress")?;
            let mut egress = load_program("nodeport_tc_egress").context("load nodeport egress")?;

            attach_tc(
                &mut ingress,
                &iface,
                TcAttachType::Ingress,
                "nodeport_tc_ingress",
            )
            .context("attach nodeport ingress tc")?;
            attach_tc(
                &mut egress,
                &iface,
                TcAttachType::Egress,
                "nodeport_tc_egress",
            )
            .context("attach nodeport egress tc")?;
            if let Err(err) = ensure_clsact("lo") {
                warn!(
                    target: "network",
                    "unable to enable nodeport on loopback: {err:#}"
                );
            } else if let Err(err) = attach_tc(
                &mut ingress,
                "lo",
                TcAttachType::Ingress,
                "nodeport_tc_ingress",
            ) {
                warn!(
                    target: "network",
                    "unable to attach nodeport ingress on loopback: {err:#}"
                );
            }

            info!(
                target: "network",
                iface = %iface,
                ifindex,
                node_ip = %node_ip,
                "nodeport tc programs attached"
            );

            self.attachment = Some(NodePortAttachment {
                _ingress: ingress,
                egress,
            });
            self.attached_iface = Some(iface.clone());
            self.attached_node_ip = Some(node_ip);
            Ok(())
        }

        /// Detach one active attachment so nodeport can reattach to a new external identity.
        async fn detach_attachment(&mut self) -> Result<()> {
            let Some(iface) = self.attached_iface.clone() else {
                self.attachment = None;
                self.attached_node_ip = None;
                return Ok(());
            };
            let Some(_attachment) = self.attachment.take() else {
                self.attached_iface = None;
                self.attached_node_ip = None;
                return Ok(());
            };

            detach_tc(&iface, TcAttachType::Ingress, "nodeport_tc_ingress")?;
            detach_tc(&iface, TcAttachType::Egress, "nodeport_tc_egress")?;
            let _ = detach_tc("lo", TcAttachType::Ingress, "nodeport_tc_ingress");

            self.host_ingress_attached.clear();
            self.host_ingress_ifindex.clear();
            self.attached_iface = None;
            self.attached_node_ip = None;
            Ok(())
        }

        /// Detach nodeport programs when no mappings remain to avoid side effects on loopback.
        async fn detach_if_idle(&mut self) -> Result<()> {
            if self.attachment.is_none() {
                return Ok(());
            }

            self.detach_attachment().await?;
            self.set_runtime_status(
                NodePortRuntimeState::Pending,
                None,
                "nodeport runtime detached while idle",
            );
            Ok(())
        }

        /// Attach nodeport SNAT handling to the host-access interface for a network.
        async fn ensure_host_ingress(&mut self, network_id: Uuid) -> Result<()> {
            let Some(attachment) = self.attachment.as_mut() else {
                return Ok(());
            };

            let iface = host_access_host_iface_name(network_id);
            let ifindex = match ifindex(&iface) {
                Ok(index) => index,
                Err(err) => {
                    self.host_ingress_attached.remove(&network_id);
                    self.host_ingress_ifindex.remove(&network_id);
                    return Err(err).with_context(|| {
                        format!("nodeport host-access interface {iface} is unavailable")
                    });
                }
            };

            if self.host_ingress_attached.contains(&network_id)
                && self.host_ingress_ifindex.get(&network_id) == Some(&ifindex)
            {
                return Ok(());
            }

            ensure_clsact(&iface)?;
            configure_host_access_sysctls(&iface)
                .with_context(|| format!("configure nodeport host-access sysctls on {iface}"))?;
            attach_tc(
                &mut attachment.egress,
                &iface,
                TcAttachType::Ingress,
                "nodeport_tc_egress",
            )
            .context("attach nodeport host-access tc")?;

            info!(
                target: "network",
                iface = %iface,
                ifindex,
                "nodeport host-access tc program attached"
            );
            self.host_ingress_attached.insert(network_id);
            self.host_ingress_ifindex.insert(network_id, ifindex);
            Ok(())
        }
    }

    /// Extract a stable interface name from link attributes, falling back to `ifindex<N>`.
    fn link_name_from_attrs(attributes: &[LinkAttribute], index: u32) -> String {
        attributes
            .iter()
            .find_map(|attr| match attr {
                LinkAttribute::IfName(name) => Some(name.clone()),
                _ => None,
            })
            .unwrap_or_else(|| format!("ifindex{index}"))
    }

    /// Return true when any address attribute carries the provided IPv4 address.
    fn address_attrs_contain_ipv4(attributes: &[AddressAttribute], needle: Ipv4Addr) -> bool {
        attributes.iter().any(|attr| match attr {
            AddressAttribute::Address(addr) | AddressAttribute::Local(addr) => {
                matches!(addr, IpAddr::V4(ip) if *ip == needle)
            }
            _ => false,
        })
    }

    /// Resolve the first IPv4 address found in one netlink address attribute list.
    fn first_ipv4_from_address_attrs(attributes: &[AddressAttribute]) -> Option<Ipv4Addr> {
        attributes.iter().find_map(|attr| match attr {
            AddressAttribute::Address(addr) | AddressAttribute::Local(addr) => match addr {
                IpAddr::V4(ip) => Some(*ip),
                _ => None,
            },
            _ => None,
        })
    }

    /// Find the interface that owns the provided IPv4 address.
    async fn detect_iface_for_ip(node_ip: Ipv4Addr) -> Result<Option<String>> {
        let (conn, handle, _) =
            new_connection().context("open rtnetlink connection for nodeport iface lookup")?;
        tokio::spawn(conn);

        let mut link_stream = handle.link().get().execute();
        while let Some(link) = link_stream
            .try_next()
            .await
            .context("enumerate links for nodeport iface lookup")?
        {
            let index = link.header.index;
            let name = link_name_from_attrs(&link.attributes, index);

            let flags = link.header.flags;
            if !flags.contains(LinkFlags::Up) || flags.contains(LinkFlags::Loopback) {
                continue;
            }
            if name == MANTISSA_WIREGUARD_IFNAME {
                continue;
            }

            let mut addr_stream = handle
                .address()
                .get()
                .set_link_index_filter(index)
                .execute();

            while let Some(msg) = addr_stream
                .try_next()
                .await
                .context("enumerate nodeport iface addresses")?
            {
                if address_attrs_contain_ipv4(&msg.attributes, node_ip) {
                    return Ok(Some(name.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Resolve an IPv4 address assigned to a specific interface name.
    async fn detect_iface_ip(iface: &str) -> Result<Option<Ipv4Addr>> {
        let (conn, handle, _) =
            new_connection().context("open rtnetlink connection for nodeport iface ip lookup")?;
        tokio::spawn(conn);

        let Some(link) = handle
            .link()
            .get()
            .match_name(iface.to_string())
            .execute()
            .try_next()
            .await
            .context("fetch nodeport interface link")?
        else {
            return Ok(None);
        };

        let mut addr_stream = handle
            .address()
            .get()
            .set_link_index_filter(link.header.index)
            .execute();

        while let Some(msg) = addr_stream
            .try_next()
            .await
            .context("enumerate nodeport interface addresses")?
        {
            if let Some(ip) = first_ipv4_from_address_attrs(&msg.attributes) {
                return Ok(Some(ip));
            }
        }

        Ok(None)
    }

    /// Pick the first up, non-loopback interface that has an IPv4 address.
    async fn detect_default_iface() -> Result<Option<(String, Ipv4Addr)>> {
        let (conn, handle, _) =
            new_connection().context("open rtnetlink connection for nodeport autodetect")?;
        tokio::spawn(conn);

        let mut link_stream = handle.link().get().execute();
        while let Some(link) = link_stream
            .try_next()
            .await
            .context("enumerate links for nodeport autodetect")?
        {
            let index = link.header.index;
            let name = link_name_from_attrs(&link.attributes, index);

            let flags = link.header.flags;
            if !flags.contains(LinkFlags::Up) || flags.contains(LinkFlags::Loopback) {
                continue;
            }
            if name == MANTISSA_WIREGUARD_IFNAME {
                continue;
            }

            let mut addr_stream = handle
                .address()
                .get()
                .set_link_index_filter(index)
                .execute();

            while let Some(msg) = addr_stream
                .try_next()
                .await
                .context("enumerate nodeport autodetect addresses")?
            {
                if let Some(ip) = first_ipv4_from_address_attrs(&msg.attributes) {
                    return Ok(Some((name.clone(), ip)));
                }
            }
        }

        Ok(None)
    }

    /// Resolve a kernel ifindex for a given interface name so tc programs can attach.
    fn ifindex(ifname: &str) -> Result<u32> {
        let cstr = CString::new(ifname).context("convert interface name")?;
        let idx = unsafe { if_nametoindex(cstr.as_ptr()) };
        if idx == 0 {
            return Err(anyhow!("interface {ifname} not found"));
        }
        Ok(idx)
    }

    /// Look up the host-access ifindex to redirect nodeport traffic into the dataplane.
    fn overlay_ifindex(network_id: Uuid) -> Result<u32> {
        let ifname = host_access_host_iface_name(network_id);
        ifindex(&ifname).with_context(|| format!("lookup host access {ifname}"))
    }

    /// Resolve the host-access interface MAC so loopback redirects can set a valid source MAC.
    async fn host_access_mac(network_id: Uuid) -> Result<[u8; 6]> {
        let ifname = host_access_host_iface_name(network_id);
        let (conn, handle, _) =
            new_connection().context("open rtnetlink connection for nodeport mac lookup")?;
        tokio::spawn(conn);

        let Some(link) = handle
            .link()
            .get()
            .match_name(ifname.clone())
            .execute()
            .try_next()
            .await
            .context("fetch nodeport host access link")?
        else {
            return Err(anyhow!("host access interface {ifname} not found"));
        };

        let addr = link
            .attributes
            .iter()
            .find_map(|attr| match attr {
                LinkAttribute::Address(bytes) => Some(bytes.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("host access interface {ifname} missing mac"))?;

        if addr.len() != 6 {
            return Err(anyhow!(
                "host access interface {ifname} returned invalid mac length {}",
                addr.len()
            ));
        }

        let mut mac = [0u8; 6];
        mac.copy_from_slice(&addr);
        Ok(mac)
    }

    /// Resolve the host-access interface IPv4 address to use for nodeport SNAT.
    async fn host_access_ip(network_id: Uuid) -> Result<Ipv4Addr> {
        let ifname = host_access_host_iface_name(network_id);
        let (conn, handle, _) =
            new_connection().context("open rtnetlink connection for nodeport ip lookup")?;
        tokio::spawn(conn);

        let Some(link) = handle
            .link()
            .get()
            .match_name(ifname.clone())
            .execute()
            .try_next()
            .await
            .context("fetch nodeport host access link for ip")?
        else {
            return Err(anyhow!("host access interface {ifname} not found"));
        };

        let mut addr_stream = handle
            .address()
            .get()
            .set_link_index_filter(link.header.index)
            .execute();

        while let Some(msg) = addr_stream
            .try_next()
            .await
            .context("enumerate host access addresses")?
        {
            if let Some(ip) = first_ipv4_from_address_attrs(&msg.attributes) {
                return Ok(ip);
            }
        }

        Err(anyhow!(
            "host access interface {ifname} missing IPv4 address"
        ))
    }

    /// Load a tc program from the local BPF artifact directory.
    fn load_program(name: &str) -> Result<Ebpf> {
        let resolver = ArtifactResolver::new();
        let path = resolver
            .resolve(name)
            .with_context(|| format!("resolve nodeport artifact {name}"))?;
        let map_pin_path = map_pin_dir()?;
        let bpf = EbpfLoader::new()
            .map_pin_path(&map_pin_path)
            .load_file(path)
            .context("load nodeport ebpf")?;
        Ok(bpf)
    }

    /// Ensure a clsact qdisc exists so tc programs can attach on an interface.
    fn ensure_clsact(iface: &str) -> Result<()> {
        match qdisc_add_clsact(iface) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            Err(err) => Err(anyhow!("ensure clsact qdisc on {iface}: {err}")),
        }
    }

    /// Set host-access sysctls so local hairpin replies are accepted by the kernel.
    fn configure_host_access_sysctls(iface: &str) -> Result<()> {
        // Hairpin replies arrive on the host-access veth with a local source, so we must
        // explicitly accept local sources and disable strict reverse-path filtering there.
        write_ipv4_sysctl(iface, "accept_local", "1").context("set nodeport accept_local")?;
        write_ipv4_sysctl(iface, "rp_filter", "0").context("disable nodeport rp_filter")?;
        Ok(())
    }

    /// Write a per-interface IPv4 sysctl to allow nodeport hairpin responses.
    fn write_ipv4_sysctl(iface: &str, key: &str, value: &str) -> Result<()> {
        let path = Path::new("/proc/sys/net/ipv4/conf").join(iface).join(key);
        // The sysctl path expects newline-terminated values.
        fs::write(&path, format!("{value}\n"))
            .with_context(|| format!("write sysctl {}", path.display()))?;
        Ok(())
    }

    /// Attach a tc program to the provided interface, tolerating existing attachments.
    fn attach_tc(
        bpf: &mut Ebpf,
        iface: &str,
        attach_type: TcAttachType,
        program_name: &str,
    ) -> Result<()> {
        let program = bpf
            .program_mut(program_name)
            .ok_or_else(|| anyhow!("nodeport program missing"))?;
        let tc: &mut SchedClassifier = program.try_into()?;
        match tc.load() {
            Ok(()) => {}
            Err(ProgramError::AlreadyLoaded) => {}
            Err(err) => return Err(err.into()),
        }
        match tc.attach(iface, attach_type) {
            Ok(_) => {}
            Err(ProgramError::AlreadyAttached) => {}
            Err(err) => return Err(err.into()),
        }
        Ok(())
    }

    /// Detach a tc program from the provided interface, tolerating missing attachments.
    fn detach_tc(iface: &str, attach_type: TcAttachType, program_name: &str) -> Result<()> {
        let mut candidates = vec![program_name.to_string()];
        let truncated: String = program_name.chars().take(15).collect();
        if truncated != program_name {
            candidates.push(truncated);
        }

        for name in candidates {
            match qdisc_detach_program(iface, attach_type, &name) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }

    struct ArtifactResolver {
        search_roots: Vec<PathBuf>,
    }

    impl ArtifactResolver {
        /// Build a resolver using the same search roots as the core BPF loader.
        fn new() -> Self {
            let mut roots = Vec::new();
            if let Some(dir) = config::bpf_artifact_dir() {
                roots.push(dir);
            }
            if let Ok(pwd) = env::current_dir() {
                roots.push(pwd.join("target/bpf"));
                roots.push(pwd.join("assets/bpf"));
            }
            Self {
                search_roots: roots,
            }
        }

        /// Find a compiled BPF object for the requested program name.
        fn resolve(&self, name: &str) -> Result<PathBuf> {
            for candidate in self.candidates(name) {
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
            Err(anyhow!(
                "unable to locate nodeport artifact '{}' (searched {:?})",
                name,
                self.search_roots
            ))
        }

        /// Enumerate candidate paths for a BPF program artifact.
        fn candidates(&self, name: &str) -> Vec<PathBuf> {
            let mut out = Vec::new();
            let path = PathBuf::from(name);
            if path.is_absolute() || name.contains(std::path::MAIN_SEPARATOR) {
                out.push(path.clone());
                if path.extension().is_none() {
                    out.push(path.with_extension("bpf.o"));
                }
                return dedup(out);
            }

            for root in &self.search_roots {
                out.push(root.join(name));
                out.push(root.join(format!("{name}.bpf.o")));
                out.push(root.join(format!("{name}.o")));
            }
            dedup(out)
        }
    }

    /// Deduplicate candidate artifact paths while preserving order.
    fn dedup(paths: Vec<PathBuf>) -> Vec<PathBuf> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for path in paths {
            if seen.insert(path.clone()) {
                out.push(path);
            }
        }
        out
    }

    /// Return the nodeport map pin directory and ensure it exists.
    fn map_pin_dir() -> Result<PathBuf> {
        ensure_bpffs().context("prepare bpffs mount")?;
        let path = PathBuf::from("/sys/fs/bpf/mantissa/nodeport");
        fs::create_dir_all(&path)
            .with_context(|| format!("create nodeport map directory {}", path.display()))?;
        Ok(path)
    }

    /// Read one pinned per-CPU stats map and aggregate its counters across every CPU slot.
    fn read_counter_map(name: &str) -> Result<NodePortPacketCounters> {
        let base = map_pin_dir()?;
        let map = open_map(&base, name).with_context(|| format!("open {name}"))?;
        let array = PerCpuArray::<_, NodePortPacketCounters>::try_from(Map::PerCpuArray(map))
            .with_context(|| format!("interpret {name} as per-cpu stats array"))?;
        let values = array
            .get(&0, 0)
            .with_context(|| format!("read counter slot from {name}"))?;

        let mut counters = NodePortPacketCounters::default();
        for value in values.iter().copied() {
            counters.packets += value.packets;
            counters.bytes += value.bytes;
            counters.drops += value.drops;
        }

        Ok(counters)
    }

    /// Remove pinned nodeport maps so new layouts can be loaded atomically.
    fn reset_nodeport_maps(root: &Path) -> Result<()> {
        let maps = [
            "NODEPORT_FWD",
            "NODEPORT_REV",
            "NODEPORT_VIPS",
            "NODEPORT_HOST",
        ];

        for name in maps {
            let path = root.join(name);
            if path.exists() {
                fs::remove_file(&path)
                    .with_context(|| format!("remove nodeport map {}", path.display()))?;
            }
        }

        Ok(())
    }

    /// Update a pinned BPF map entry with the provided key/value pair.
    fn update_elem<K: Pod, V: Pod>(fd: i32, key: &K, val: &V) -> Result<()> {
        const BPF_MAP_UPDATE_ELEM: libc::c_uint = 2;

        #[repr(C)]
        struct BpfAttrUpsert {
            map_fd: u32,
            _pad: u32,
            key: u64,
            value: u64,
            flags: u64,
        }

        let mut attr = BpfAttrUpsert {
            map_fd: fd as u32,
            _pad: 0,
            key: key as *const _ as u64,
            value: val as *const _ as u64,
            flags: 0,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_UPDATE_ELEM,
                &mut attr as *mut _,
                mem::size_of::<BpfAttrUpsert>(),
            )
        };
        if ret < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }

    /// Delete a pinned BPF map entry when it is no longer needed.
    fn delete_elem<K: Pod>(fd: i32, key: &K) -> Result<()> {
        const BPF_MAP_DELETE_ELEM: libc::c_uint = 3;

        #[repr(C)]
        struct BpfAttrDelete {
            map_fd: u32,
            _pad: u32,
            key: u64,
        }

        let mut attr = BpfAttrDelete {
            map_fd: fd as u32,
            _pad: 0,
            key: key as *const _ as u64,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_DELETE_ELEM,
                &mut attr as *mut _,
                mem::size_of::<BpfAttrDelete>(),
            )
        };
        if ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(err.into());
        }
        Ok(())
    }

    /// Open a pinned map by name using the same search order as the core BPF loader.
    fn open_map(base: &Path, name: &str) -> Result<MapData> {
        let candidates = [
            base.join(name),
            base.join("tc").join("globals").join(name),
            Path::new("/sys/fs/bpf/tc/globals").join(name),
        ];

        for candidate in candidates {
            if let Ok(map) = MapData::from_pin(&candidate) {
                return Ok(map);
            }
        }

        Err(anyhow!("map {name} not found in expected pin locations"))
    }

    /// Ensure the bpffs filesystem is mounted so pinned maps can be accessed.
    fn ensure_bpffs() -> Result<()> {
        let mountpoint = Path::new("/sys/fs/bpf");
        if !mountpoint.exists() {
            fs::create_dir_all(mountpoint).context("create /sys/fs/bpf")?;
        }

        if is_bpffs(mountpoint) {
            return Ok(());
        }

        mount(
            Some("bpffs"),
            mountpoint,
            Some("bpf"),
            MsFlags::empty(),
            None::<&str>,
        )
        .context("mount bpffs")
    }

    /// Check whether the provided path is backed by bpffs.
    fn is_bpffs(path: &Path) -> bool {
        statfs(path)
            .map(|s| s.filesystem_type() == BPF_FS_MAGIC)
            .unwrap_or(false)
    }
}

#[cfg(target_os = "linux")]
use platform::PlatformNodePortManager;

#[cfg(test)]
mod tests {
    use super::{
        NODEPORT_FLOW_CAPACITY, NODEPORT_HOST_CAPACITY, NODEPORT_VIP_CAPACITY,
        NodePortPacketCounters, NodePortRuntimeState, NodePortStatus,
        configured_node_ip_from_sources, nodeport_capacity_error,
        projected_active_networks_after_sync, resolve_advertise_ipv4,
    };
    use std::net::Ipv4Addr;

    #[test]
    fn resolve_advertise_ipv4_accepts_literal_ipv4_socket() {
        assert_eq!(
            resolve_advertise_ipv4("192.168.10.4:6578"),
            Some(Ipv4Addr::new(192, 168, 10, 4))
        );
    }

    #[test]
    fn resolve_advertise_ipv4_ignores_ipv6_only_socket() {
        assert_eq!(resolve_advertise_ipv4("[::1]:6578"), None);
    }

    #[test]
    fn configured_node_ip_prefers_explicit_override() {
        let configured = Some(Ipv4Addr::new(10, 20, 30, 40));
        let advertise = Some("192.168.10.4:6578");

        assert_eq!(
            configured_node_ip_from_sources(configured, advertise),
            Some(Ipv4Addr::new(10, 20, 30, 40))
        );
    }

    #[test]
    fn configured_node_ip_uses_advertise_addr_when_override_absent() {
        assert_eq!(
            configured_node_ip_from_sources(None, Some("192.168.10.4:6578")),
            Some(Ipv4Addr::new(192, 168, 10, 4))
        );
    }

    #[test]
    fn nodeport_status_tracks_active_counts() {
        let status = NodePortStatus {
            desired_enabled: true,
            state: NodePortRuntimeState::Pending,
            resolved_iface: Some("eth0".to_string()),
            resolved_node_ip: Some(Ipv4Addr::new(10, 0, 0, 4)),
            active_networks: 2,
            active_ports: 3,
            active_host_networks: 2,
            vip_capacity: NODEPORT_VIP_CAPACITY,
            host_capacity: NODEPORT_HOST_CAPACITY,
            flow_capacity: NODEPORT_FLOW_CAPACITY,
            ingress_stats: Some(NodePortPacketCounters {
                packets: 10,
                bytes: 2048,
                drops: 1,
            }),
            egress_stats: None,
            last_error: None,
            stats_error: None,
        };

        assert_eq!(status.active_networks, 2);
        assert_eq!(status.active_ports, 3);
        assert_eq!(status.active_host_networks, 2);
        assert_eq!(status.vip_capacity, NODEPORT_VIP_CAPACITY);
        assert_eq!(
            status.ingress_stats,
            Some(NodePortPacketCounters {
                packets: 10,
                bytes: 2048,
                drops: 1,
            })
        );
        assert_eq!(status.state, NodePortRuntimeState::Pending);
    }

    #[test]
    fn projected_active_networks_adds_new_public_network() {
        assert_eq!(projected_active_networks_after_sync(3, false, true), 4);
    }

    #[test]
    fn projected_active_networks_removes_empty_public_network() {
        assert_eq!(projected_active_networks_after_sync(3, true, false), 2);
    }

    #[test]
    fn nodeport_capacity_error_reports_vip_limit() {
        let error = nodeport_capacity_error(NODEPORT_VIP_CAPACITY + 1, 1)
            .expect("expected vip capacity error");
        assert!(error.contains("VIP capacity exceeded"));
    }

    #[test]
    fn nodeport_capacity_error_reports_host_limit() {
        let error = nodeport_capacity_error(1, NODEPORT_HOST_CAPACITY + 1)
            .expect("expected host capacity error");
        assert!(error.contains("host-access capacity exceeded"));
    }
}
