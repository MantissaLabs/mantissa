use super::{
    NODEPORT_FLOW_CLEAR_INDEX, NODEPORT_FLOW_CREATE_INDEX, NODEPORT_FLOW_EVENT_COUNT,
    NODEPORT_INGRESS_DROP_REASON_COUNT, NODEPORT_INVALID_TRANSITION_INDEX,
    NODEPORT_RETURN_BYPASS_INDEX, NODEPORT_REVERSE_MISS_INDEX, NodePortFlowDiagnostics,
    NodePortIdentitySource, NodePortIngressDropReasons, NodePortMapCapacities, NodePortMapping,
    NodePortPacketCounters, NodePortProtocol, NodePortRuntimeState, NodePortStatus,
    configured_node_ip_from_sources, configured_node_ip_source, estimated_flow_evictions,
    nodeport_capacity_error, projected_active_networks_after_sync, resolve_advertise_ip,
};
use crate::config;
use crate::ip_family::{IpFamily, infer_default_ip_family};
use crate::network::attachment::host_access_host_iface_name;
use crate::network::wireguard::MANTISSA_WIREGUARD_IFNAME;
use anyhow::{Context, Result, anyhow};
use aya::Pod;
use aya::maps::{Map, MapData, PerCpuArray};
use aya::programs::ProgramError;
use aya::programs::tc::{SchedClassifier, TcAttachType, qdisc_add_clsact, qdisc_detach_program};
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
use std::net::IpAddr;
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
struct NodePortEntry6 {
    vip: [u8; 16],
    vip_port: u16,
    _pad: u16,
    overlay_ifindex: u32,
    node_ip: [u8; 16],
}
unsafe impl Pod for NodePortEntry6 {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct NodePortReturnKey {
    pub(super) vip: u32,
    pub(super) vip_port: u16,
    pub(super) proto: u8,
    pub(super) _pad: u8,
}
unsafe impl Pod for NodePortReturnKey {}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(super) struct NodePortReturnKey6 {
    pub(super) vip: [u8; 16],
    pub(super) vip_port: u16,
    pub(super) proto: u8,
    pub(super) _pad: u8,
}
unsafe impl Pod for NodePortReturnKey6 {}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NodePortHost {
    mac: [u8; 6],
    tcp_mss: u16,
    host_ip: u32,
}
unsafe impl Pod for NodePortHost {}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NodePortHost6 {
    mac: [u8; 6],
    tcp_mss: u16,
    host_ip: [u8; 16],
}
unsafe impl Pod for NodePortHost6 {}
unsafe impl Pod for NodePortPacketCounters {}

/// Userspace mirror of the IPv4 flow key layout pinned by the NodePort tc programs.
///
/// The cleanup path iterates pinned flow maps directly, so these fields must stay byte-for-
/// byte aligned with `network_ebpf::lb::Flow4`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Flow4 {
    src: u32,
    dst: u32,
    src_port: u16,
    dst_port: u16,
    proto: u8,
    pad: u8,
    padding: [u8; 2],
}
unsafe impl Pod for Flow4 {}

/// Userspace mirror of the IPv6 flow key layout pinned by the NodePort tc programs.
///
/// Keeping the key layout local lets the manager clear stale IPv6 flow state without reaching
/// across crate boundaries for userspace-only map iteration code.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Flow6 {
    src: [u8; 16],
    dst: [u8; 16],
    src_port: u16,
    dst_port: u16,
    proto: u8,
    padding: [u8; 3],
}
unsafe impl Pod for Flow6 {}

/// Userspace mirror of the shared conntrack metadata stored next to NodePort flow entries.
///
/// Cleanup logic only needs this to preserve the exact value layout while it looks up pinned
/// NAT entries and filters them by their published selector.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct ConntrackMetadata {
    last_seen_ns: u64,
    protocol: u8,
    state: u8,
    flags: u8,
    _pad: [u8; 5],
}
unsafe impl Pod for ConntrackMetadata {}

/// Userspace mirror of one cached IPv4 NodePort NAT entry.
///
/// The manager uses this to distinguish multiple public selectors that intentionally target
/// the same VIP and service port without flushing each other's flow cache entries.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NodePortNat {
    node_ip: u32,
    node_port: u16,
    _pad: u16,
    client_ip: u32,
    conntrack: ConntrackMetadata,
}
unsafe impl Pod for NodePortNat {}

/// Userspace mirror of one cached IPv6 NodePort NAT entry.
///
/// IPv6 cleanup follows the same exact selector matching rules as IPv4, so it keeps a local
/// map-value mirror instead of branching on ad-hoc byte slices.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct NodePortNat6 {
    node_ip: [u8; 16],
    node_port: u16,
    _pad: [u8; 2],
    client_ip: [u8; 16],
    conntrack: ConntrackMetadata,
}
unsafe impl Pod for NodePortNat6 {}

/// Pinned map names for one NodePort address family.
struct NodePortMapNames {
    vip_map: &'static str,
    return_map: &'static str,
    host_map: &'static str,
    forward_map: &'static str,
    reverse_map: &'static str,
}

/// Pinned map names used by the IPv4 NodePort dataplane programs.
const IPV4_MAPS: NodePortMapNames = NodePortMapNames {
    vip_map: "NODEPORT_VIPS",
    return_map: "NODEPORT_RETURNS",
    host_map: "NODEPORT_HOST",
    forward_map: "NODEPORT_FWD",
    reverse_map: "NODEPORT_REV",
};

/// Pinned map names used by the IPv6 NodePort dataplane programs.
const IPV6_MAPS: NodePortMapNames = NodePortMapNames {
    vip_map: "NODEPORT_VIPS_V6",
    return_map: "NODEPORT_RETURNS_V6",
    host_map: "NODEPORT_HOST_V6",
    forward_map: "NODEPORT_FWD_V6",
    reverse_map: "NODEPORT_REV_V6",
};
/// Combined IPv4 and TCP header size used when deriving a safe MSS clamp.
const IPV4_TCP_HEADER_BYTES: u32 = 40;
/// Combined IPv6 and TCP header size used when deriving a safe MSS clamp.
const IPV6_TCP_HEADER_BYTES: u32 = 60;

/// Address family selector shared by interface discovery and map programming helpers.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum NodePortIpFamily {
    Ipv4,
    Ipv6,
}

impl NodePortIpFamily {
    /// Return the map family that matches one concrete IP address.
    fn from_ip(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }

    /// Render a stable address-family label for diagnostics and operator guidance.
    fn label(self) -> &'static str {
        match self {
            Self::Ipv4 => "IPv4",
            Self::Ipv6 => "IPv6",
        }
    }
}

/// Uniquely identify a nodeport binding by port and transport protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct NodePortSelector {
    port: u16,
    protocol: NodePortProtocol,
}

impl NodePortSelector {
    /// Build a selector for nodeport ownership and deduplication.
    pub(super) fn new(port: u16, protocol: NodePortProtocol) -> Self {
        Self { port, protocol }
    }
}

/// Fully resolved dataplane mapping programmed for one public selector on one refresh tick.
///
/// Keeping the resolved VIP, target port, external node IP, and overlay attachment together
/// lets the manager detect when one selector changed meaningfully enough that stale flow state
/// must be purged before fresh traffic can reuse the old cache entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NodePortPublishedMapping {
    vip: IpAddr,
    vip_port: u16,
    node_ip: IpAddr,
    overlay_ifindex: u32,
}

impl NodePortPublishedMapping {
    /// Capture one resolved NodePort dataplane mapping after family-specific identity
    /// selection completed.
    pub(super) fn new(vip: IpAddr, vip_port: u16, node_ip: IpAddr, overlay_ifindex: u32) -> Self {
        Self {
            vip,
            vip_port,
            node_ip,
            overlay_ifindex,
        }
    }
}

/// Return the previously programmed selector mappings that need flow cleanup on this sync.
///
/// A selector becomes stale when it disappears entirely or when its resolved dataplane target
/// changes in any way that would make the old cached translation incorrect for future packets.
pub(super) fn stale_nodeport_mappings(
    previous: &HashMap<NodePortSelector, NodePortPublishedMapping>,
    desired: &HashMap<NodePortSelector, NodePortPublishedMapping>,
) -> Vec<(NodePortSelector, NodePortPublishedMapping)> {
    previous
        .iter()
        .filter_map(|(selector, existing)| match desired.get(selector) {
            Some(next) if next == existing => None,
            _ => Some((*selector, *existing)),
        })
        .collect()
}

/// Return the overlay host-access ifindices that are no longer referenced after one sync.
///
/// NodePort host maps are keyed by overlay ifindex, so removing stale indices prevents a dead
/// host-access attachment from continuing to advertise a routable SNAT source.
pub(super) fn stale_overlay_ifindices(
    previous: &HashMap<NodePortSelector, NodePortPublishedMapping>,
    desired: &HashMap<NodePortSelector, NodePortPublishedMapping>,
) -> Vec<u32> {
    let previous_indices: HashSet<u32> = previous
        .values()
        .map(|mapping| mapping.overlay_ifindex)
        .collect();
    let desired_indices: HashSet<u32> = desired
        .values()
        .map(|mapping| mapping.overlay_ifindex)
        .collect();
    previous_indices
        .difference(&desired_indices)
        .copied()
        .collect()
}

/// Return the published VIP/service-port tuples that should count as real NodePort return-path
/// candidates for one sync snapshot.
///
/// The tc egress hook is attached to both the external interface and every host-access
/// interface, so it sees unrelated traffic as well as real NodePort replies. Keeping one
/// family-specific candidate set in bpffs lets the dataplane distinguish "ordinary traffic
/// that passed through the hook" from "published return packet missing conntrack state"
/// without scanning the publication map by value at packet time.
pub(super) fn nodeport_return_keys(
    mappings: &HashMap<NodePortSelector, NodePortPublishedMapping>,
) -> (HashSet<NodePortReturnKey>, HashSet<NodePortReturnKey6>) {
    let mut ipv4 = HashSet::new();
    let mut ipv6 = HashSet::new();

    for (selector, mapping) in mappings {
        match mapping.vip {
            IpAddr::V4(vip) => {
                ipv4.insert(NodePortReturnKey {
                    vip: u32::from_ne_bytes(vip.octets()),
                    vip_port: mapping.vip_port.to_be(),
                    proto: selector.protocol.number(),
                    _pad: 0,
                });
            }
            IpAddr::V6(vip) => {
                ipv6.insert(NodePortReturnKey6 {
                    vip: vip.octets(),
                    vip_port: mapping.vip_port.to_be(),
                    proto: selector.protocol.number(),
                    _pad: 0,
                });
            }
        }
    }

    (ipv4, ipv6)
}

struct NodePortAttachment {
    _ingress: Ebpf,
    egress: Ebpf,
}

/// Linux implementation that loads nodeport tc programs and keeps their maps synchronized.
pub(super) struct PlatformNodePortManager {
    desired_enabled: bool,
    source_mode: config::NodePortSourceMode,
    configured_iface: Option<String>,
    configured_node_ip: Option<IpAddr>,
    configured_advertise_addr: Option<String>,
    identity_source: Option<NodePortIdentitySource>,
    iface: Option<String>,
    node_ip: Option<IpAddr>,
    attached_iface: Option<String>,
    attached_node_ip: Option<IpAddr>,
    attachment: Option<NodePortAttachment>,
    ports_by_network: HashMap<Uuid, HashMap<NodePortSelector, NodePortPublishedMapping>>,
    port_owner: HashMap<NodePortSelector, Uuid>,
    host_ingress_attached: HashSet<Uuid>,
    host_ingress_ifindex: HashMap<Uuid, u32>,
    capacities: NodePortMapCapacities,
    userspace_flow_clears: u64,
    runtime_state: NodePortRuntimeState,
    last_error: Option<String>,
}

impl PlatformNodePortManager {
    /// Capture nodeport configuration from the global config for later attachment.
    pub(super) fn new() -> Self {
        let source_mode = config::nodeport_source_mode();
        let configured_iface = config::nodeport_iface();
        let configured_node_ip = config::nodeport_ip();
        let configured_advertise_addr = config::advertise_addr();
        let capacities = NodePortMapCapacities::from_config();
        let mut desired_enabled = config::nodeport_enabled();
        let initial_error = if desired_enabled
            && source_mode != config::NodePortSourceMode::SnatHostAccess
        {
            debug!(
                target: "network",
                source_mode = %source_mode,
                "nodeport disabled because the configured source mode is not implemented"
            );
            desired_enabled = false;
            Some(format!(
                "nodeport disabled because network.nodeport.source_mode '{}' is not supported yet",
                source_mode
            ))
        } else if desired_enabled && !config::bpf_attach_enabled() {
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
            source_mode,
            configured_iface: configured_iface.clone(),
            configured_node_ip,
            configured_advertise_addr,
            identity_source: None,
            iface: configured_iface,
            node_ip: configured_node_ip,
            attached_iface: None,
            attached_node_ip: None,
            attachment: None,
            ports_by_network: HashMap::new(),
            port_owner: HashMap::new(),
            host_ingress_attached: HashSet::new(),
            host_ingress_ifindex: HashMap::new(),
            capacities,
            userspace_flow_clears: 0,
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
                    format!("nodeport host-access attach failed for network {network_id}: {err:#}")
                })
            ));
        }
        self.set_runtime_status(NodePortRuntimeState::Ready, None, "nodeport runtime ready");

        let overlay_ifindex_opt = if entries.is_empty() {
            None
        } else {
            Some(overlay_ifindex(network_id)?)
        };
        let base = map_pin_dir()?;
        let ipv4_vip_map = open_map(&base, IPV4_MAPS.vip_map).context("open NODEPORT_VIPS map")?;
        let ipv6_vip_map =
            open_map(&base, IPV6_MAPS.vip_map).context("open NODEPORT_VIPS_V6 map")?;
        let ipv4_return_map =
            open_map(&base, IPV4_MAPS.return_map).context("open NODEPORT_RETURNS map")?;
        let ipv6_return_map =
            open_map(&base, IPV6_MAPS.return_map).context("open NODEPORT_RETURNS_V6 map")?;
        let ipv4_host_map =
            open_map(&base, IPV4_MAPS.host_map).context("open NODEPORT_HOST map")?;
        let ipv6_host_map =
            open_map(&base, IPV6_MAPS.host_map).context("open NODEPORT_HOST_V6 map")?;
        let ipv4_vip_fd = ipv4_vip_map.fd().as_fd().as_raw_fd();
        let ipv6_vip_fd = ipv6_vip_map.fd().as_fd().as_raw_fd();
        let ipv4_return_fd = ipv4_return_map.fd().as_fd().as_raw_fd();
        let ipv6_return_fd = ipv6_return_map.fd().as_fd().as_raw_fd();
        let ipv4_host_fd = ipv4_host_map.fd().as_fd().as_raw_fd();
        let ipv6_host_fd = ipv6_host_map.fd().as_fd().as_raw_fd();
        let previous_mappings = self
            .ports_by_network
            .get(&network_id)
            .cloned()
            .unwrap_or_default();
        let mut resolved_node_ips = HashMap::new();
        let overlay_index = if entries.is_empty() {
            0
        } else {
            overlay_ifindex_opt.ok_or_else(|| anyhow!("nodeport overlay ifindex missing"))?
        };
        let desired_mappings = self
            .collect_programmed_mappings(entries, overlay_index, &mut resolved_node_ips)
            .await?;
        let stale_mappings = stale_nodeport_mappings(&previous_mappings, &desired_mappings);
        let (previous_return_keys_v4, previous_return_keys_v6) =
            nodeport_return_keys(&previous_mappings);
        let (desired_return_keys_v4, desired_return_keys_v6) =
            nodeport_return_keys(&desired_mappings);
        for (selector, _) in &stale_mappings {
            let key = NodePortKey {
                port: selector.port.to_be(),
                proto: selector.protocol.number(),
                _pad: 0,
            };
            // Remove stale selectors from both families before clearing flow state so packets
            // cannot recreate the old translation while this sync is still converging.
            let _ = delete_elem(ipv4_vip_fd, &key);
            let _ = delete_elem(ipv6_vip_fd, &key);
        }
        for key in previous_return_keys_v4.difference(&desired_return_keys_v4) {
            let _ = delete_elem(ipv4_return_fd, key);
        }
        for key in previous_return_keys_v6.difference(&desired_return_keys_v6) {
            let _ = delete_elem(ipv6_return_fd, key);
        }
        let cleared_pairs = clear_stale_nodeport_flows(&base, stale_mappings.as_slice())
            .context("clear stale nodeport flows")?;
        self.userspace_flow_clears = self.userspace_flow_clears.saturating_add(cleared_pairs);

        if let Some(overlay_ifindex) = overlay_ifindex_opt {
            let host_mac = host_access_mac(network_id).await?;
            let host_mtu = host_access_mtu(network_id).await?;
            let mut programmed_ipv4_host = false;
            let mut programmed_ipv6_host = false;

            for family in unique_nodeport_families(entries)
                .into_iter()
                .map(|(family, _)| family)
            {
                let host_ip = host_access_ip(network_id, family).await?;
                let tcp_mss = tcp_mss_for_family(host_mtu, family).with_context(|| {
                    format!("derive {family:?} NodePort TCP MSS from host-access MTU {host_mtu}")
                })?;

                match host_ip {
                    IpAddr::V4(host_ip) => {
                        let value = NodePortHost {
                            mac: host_mac,
                            tcp_mss,
                            host_ip: u32::from_ne_bytes(host_ip.octets()),
                        };
                        update_elem(ipv4_host_fd, &overlay_ifindex, &value)
                            .context("program IPv4 nodeport host attachment")?;
                        programmed_ipv4_host = true;
                    }
                    IpAddr::V6(host_ip) => {
                        let value = NodePortHost6 {
                            mac: host_mac,
                            tcp_mss,
                            host_ip: host_ip.octets(),
                        };
                        update_elem(ipv6_host_fd, &overlay_ifindex, &value)
                            .context("program IPv6 nodeport host attachment")?;
                        programmed_ipv6_host = true;
                    }
                }
            }

            if !programmed_ipv4_host {
                let _ = delete_elem(ipv4_host_fd, &overlay_ifindex);
            }
            if !programmed_ipv6_host {
                let _ = delete_elem(ipv6_host_fd, &overlay_ifindex);
            }
        }
        for overlay_ifindex in stale_overlay_ifindices(&previous_mappings, &desired_mappings) {
            let _ = delete_elem(ipv4_host_fd, &overlay_ifindex);
            let _ = delete_elem(ipv6_host_fd, &overlay_ifindex);
        }

        for key in &desired_return_keys_v4 {
            update_elem(ipv4_return_fd, key, &1u8)
                .context("program IPv4 nodeport return candidate")?;
        }
        for key in &desired_return_keys_v6 {
            update_elem(ipv6_return_fd, key, &1u8)
                .context("program IPv6 nodeport return candidate")?;
        }

        for (selector, mapping) in &desired_mappings {
            let key = NodePortKey {
                port: selector.port.to_be(),
                proto: selector.protocol.number(),
                _pad: 0,
            };
            match (mapping.vip, mapping.node_ip) {
                (IpAddr::V4(vip), IpAddr::V4(node_ip)) => {
                    let value = NodePortEntry {
                        vip: u32::from_ne_bytes(vip.octets()),
                        vip_port: mapping.vip_port.to_be(),
                        _pad: 0,
                        overlay_ifindex: mapping.overlay_ifindex,
                        node_ip: u32::from_ne_bytes(node_ip.octets()),
                    };
                    update_elem(ipv4_vip_fd, &key, &value)
                        .with_context(|| format!("program IPv4 nodeport {}", selector.port))?;
                    let _ = delete_elem(ipv6_vip_fd, &key);
                }
                (IpAddr::V6(vip), IpAddr::V6(node_ip)) => {
                    let value = NodePortEntry6 {
                        vip: vip.octets(),
                        vip_port: mapping.vip_port.to_be(),
                        _pad: 0,
                        overlay_ifindex: mapping.overlay_ifindex,
                        node_ip: node_ip.octets(),
                    };
                    update_elem(ipv6_vip_fd, &key, &value)
                        .with_context(|| format!("program IPv6 nodeport {}", selector.port))?;
                    let _ = delete_elem(ipv4_vip_fd, &key);
                }
                (vip, node_ip) => {
                    let error = format!(
                        "nodeport resolved an invalid {} publication identity {node_ip} for VIP {vip}; configure network.nodeport.ip explicitly for the correct family",
                        NodePortIpFamily::from_ip(node_ip).label(),
                    );
                    self.degrade_runtime(error.clone(), "nodeport runtime degraded");
                    return Err(anyhow!(error));
                }
            }
            self.port_owner.insert(*selector, network_id);
        }

        let known: HashSet<NodePortSelector> = previous_mappings.keys().copied().collect();
        for selector in known.difference(&desired_ports) {
            self.port_owner.remove(selector);
        }
        if desired_mappings.is_empty() {
            self.ports_by_network.remove(&network_id);
        } else {
            self.ports_by_network.insert(network_id, desired_mappings);
        }
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

    /// Resolve the fully programmed mapping state for one sync so cleanup and programming can
    /// reason about the same selector snapshot.
    ///
    /// The sync loop first materializes the desired dataplane view, then removes stale state,
    /// and only after that publishes fresh selectors. Keeping the resolved mappings in one
    /// structure avoids recomputing family-specific node identities in each phase.
    async fn collect_programmed_mappings(
        &mut self,
        entries: &[NodePortMapping],
        overlay_ifindex: u32,
        resolved_node_ips: &mut HashMap<NodePortIpFamily, IpAddr>,
    ) -> Result<HashMap<NodePortSelector, NodePortPublishedMapping>> {
        let mut desired = HashMap::new();
        for entry in entries {
            let family = NodePortIpFamily::from_ip(entry.vip);
            let node_ip = if let Some(node_ip) = resolved_node_ips.get(&family).copied() {
                node_ip
            } else {
                let node_ip = self
                    .resolve_public_node_ip_for_family(family, entry.vip)
                    .await?;
                resolved_node_ips.insert(family, node_ip);
                node_ip
            };
            let selector = NodePortSelector::new(entry.port, entry.protocol);
            desired.insert(
                selector,
                NodePortPublishedMapping::new(entry.vip, entry.vip_port, node_ip, overlay_ifindex),
            );
        }
        Ok(desired)
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
            .map(HashMap::len)
            .unwrap_or(0);
        let projected_active_ports = self.port_owner.len() - current_ports + desired_ports.len();
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
        if let Some(error) = nodeport_capacity_error(
            projected_active_ports,
            projected_active_networks,
            self.capacities,
        ) {
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
                return status;
            }
        }

        match self.read_ingress_drop_reasons() {
            Ok(reasons) => {
                status.ingress_drop_reasons = Some(reasons);
            }
            Err(err) => {
                status.stats_error = Some(err.to_string());
                return status;
            }
        }

        match self.read_flow_diagnostics() {
            Ok(diagnostics) => {
                status.flow_diagnostics = Some(diagnostics);
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
            source_mode: self.source_mode,
            identity_source: self.identity_source,
            resolved_iface: self.iface.clone(),
            resolved_node_ip: self.node_ip,
            active_networks,
            active_ports: self.port_owner.len(),
            active_host_networks: self.host_ingress_attached.len(),
            vip_capacity: self.capacities.vip,
            host_capacity: self.capacities.host,
            flow_capacity: self.capacities.flow,
            ingress_stats: None,
            ingress_drop_reasons: None,
            egress_stats: None,
            flow_diagnostics: None,
            last_error: self.last_error.clone(),
            stats_error: None,
        }
    }

    /// Read and aggregate the matched ingress and egress NodePort packet counters from the pinned stats maps.
    fn read_dataplane_counters(&self) -> Result<(NodePortPacketCounters, NodePortPacketCounters)> {
        Ok((
            read_counter_map("NODEPORT_TC_INGRESS_STATS")?,
            read_counter_map("NODEPORT_TC_EGRESS_STATS")?,
        ))
    }

    /// Read the per-reason ingress drop counters from the pinned NodePort diagnostics map.
    fn read_ingress_drop_reasons(&self) -> Result<NodePortIngressDropReasons> {
        let values = read_u64_percpu_array(
            "NODEPORT_TC_INGRESS_DROP_REASONS",
            NODEPORT_INGRESS_DROP_REASON_COUNT,
        )?;
        Ok(NodePortIngressDropReasons {
            invalid_ipv4_headers: values[0],
            invalid_l4_headers: values[1],
            missing_host_entries: values[2],
            nat_insert_failures: values[3],
            rewrite_failures: values[4],
            fragmented_ipv4_packets: values[5],
        })
    }

    /// Read the shared NodePort flow diagnostics and derive an eviction estimate from current
    /// forward-map occupancy plus cumulative lifecycle counters.
    ///
    /// The return-path hook also sees ordinary traffic on the external and host-access
    /// interfaces, so the diagnostics distinguish candidate reverse misses from packets that
    /// simply bypassed NodePort accounting on those shared hooks.
    fn read_flow_diagnostics(&self) -> Result<NodePortFlowDiagnostics> {
        let values = read_u64_percpu_array("NODEPORT_TC_FLOW_EVENTS", NODEPORT_FLOW_EVENT_COUNT)?;
        let ipv4_flow_pairs = count_pinned_map_entries::<Flow4>(IPV4_MAPS.forward_map)?;
        let ipv6_flow_pairs = count_pinned_map_entries::<Flow6>(IPV6_MAPS.forward_map)?;
        let flow_creates = values[NODEPORT_FLOW_CREATE_INDEX];
        let flow_clears =
            values[NODEPORT_FLOW_CLEAR_INDEX].saturating_add(self.userspace_flow_clears);

        Ok(NodePortFlowDiagnostics {
            ipv4_flow_pairs,
            ipv6_flow_pairs,
            flow_creates,
            flow_clears,
            estimated_flow_evictions: estimated_flow_evictions(
                flow_creates,
                flow_clears,
                ipv4_flow_pairs,
                ipv6_flow_pairs,
            ),
            reverse_misses: values[NODEPORT_REVERSE_MISS_INDEX],
            invalid_conntrack_transitions: values[NODEPORT_INVALID_TRANSITION_INDEX],
            return_path_bypass_packets: values[NODEPORT_RETURN_BYPASS_INDEX],
        })
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
                    identity_source = ?current.identity_source,
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
                    identity_source = ?current.identity_source,
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
                    identity_source = ?current.identity_source,
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

    /// Resolve the concrete node publication address for one requested VIP family.
    ///
    /// `network.nodeport.ip` is treated as a hard publication override. If it is configured in
    /// the wrong family, we surface that directly instead of silently switching to another
    /// address. `advertise_addr` is softer: it can still anchor interface selection while we
    /// pick a different family-specific address from the same interface when available.
    async fn resolve_public_node_ip_for_family(
        &mut self,
        family: NodePortIpFamily,
        vip: IpAddr,
    ) -> Result<IpAddr> {
        let Some(iface) = self.iface.clone() else {
            let error = if let Some(node_ip) = self.node_ip {
                format!(
                    "nodeport could not find an interface that owns publication address {node_ip}; set network.nodeport.iface explicitly or assign that address to a usable interface"
                )
            } else {
                "nodeport interface missing; set network.nodeport.iface, configure network.nodeport.ip / network.advertise_addr, or assign a usable address to one up non-loopback interface".to_string()
            };
            self.degrade_runtime(error.clone(), "nodeport runtime degraded");
            return Err(anyhow!(error));
        };

        if let Some(configured_ip) = self.configured_node_ip {
            if NodePortIpFamily::from_ip(configured_ip) == family {
                if let Err(err) =
                    ensure_iface_owns_ip(&iface, configured_ip, "network.nodeport.ip").await
                {
                    let error = err.to_string();
                    self.degrade_runtime(error.clone(), "nodeport runtime degraded");
                    return Err(anyhow!(error));
                }
                return Ok(configured_ip);
            }

            let error = format!(
                "configured network.nodeport.ip {configured_ip} is {} but published VIP {vip} requires {}; set network.nodeport.ip to a usable {} address or remove the override",
                NodePortIpFamily::from_ip(configured_ip).label(),
                family.label(),
                family.label(),
            );
            self.degrade_runtime(error.clone(), "nodeport runtime degraded");
            return Err(anyhow!(error));
        }

        if let Some(configured_ip) = self
            .configured_advertise_addr
            .as_deref()
            .and_then(resolve_advertise_ip)
            .filter(|ip| NodePortIpFamily::from_ip(*ip) == family)
        {
            if let Err(err) =
                ensure_iface_owns_ip(&iface, configured_ip, "network.advertise_addr").await
            {
                let error = err.to_string();
                self.degrade_runtime(error.clone(), "nodeport runtime degraded");
                return Err(anyhow!(error));
            }
            return Ok(configured_ip);
        }

        if let Some(node_ip) = detect_iface_ip(&iface, Some(family)).await?
            && NodePortIpFamily::from_ip(node_ip) == family
        {
            return Ok(node_ip);
        }

        let error = match family {
            NodePortIpFamily::Ipv4 => format!(
                "nodeport interface {iface} has no usable IPv4 address for published VIP {vip}; assign an IPv4 address or configure network.nodeport.ip"
            ),
            NodePortIpFamily::Ipv6 => format!(
                "nodeport interface {iface} has no usable IPv6 address for published VIP {vip}; link-local IPv6 addresses cannot be used for public NodePort, assign a global or ULA IPv6 address or configure network.nodeport.ip"
            ),
        };
        self.degrade_runtime(error.clone(), "nodeport runtime degraded");
        Err(anyhow!(error))
    }

    /// Resolve the preferred family used when nodeport autodetect must choose between IPv4 and IPv6.
    fn preferred_runtime_family(&self) -> NodePortIpFamily {
        let (has_ipv4, has_ipv6) = crate::node::address::detect_outbound_ip_families();
        match infer_default_ip_family(
            self.configured_node_ip,
            self.configured_advertise_addr.as_deref(),
            config::default_ip_family_policy(),
            has_ipv4,
            has_ipv6,
        ) {
            IpFamily::Ipv4 => NodePortIpFamily::Ipv4,
            IpFamily::Ipv6 => NodePortIpFamily::Ipv6,
        }
    }

    /// Re-resolve the runtime iface and node IP from explicit config before any fallback.
    async fn refresh_runtime_identity(&mut self) -> Result<()> {
        self.iface = self.configured_iface.clone();
        self.node_ip = configured_node_ip_from_sources(
            self.configured_node_ip,
            self.configured_advertise_addr.as_deref(),
        );
        self.identity_source = configured_node_ip_source(
            self.configured_node_ip,
            self.configured_advertise_addr.as_deref(),
        );
        let preferred_family = Some(self.preferred_runtime_family());

        if let Some(iface) = self.iface.clone() {
            if self.node_ip.is_none() {
                self.node_ip = detect_iface_ip(&iface, preferred_family).await?;
                if self.node_ip.is_some() {
                    self.identity_source = Some(NodePortIdentitySource::InterfaceAddress);
                }
            }
            return Ok(());
        }

        if let Some(node_ip) = self.node_ip {
            self.iface = detect_iface_for_ip(node_ip).await?;
            return Ok(());
        }

        if let Some((iface, ip)) = detect_default_iface(preferred_family).await? {
            self.iface = Some(iface);
            self.node_ip = Some(ip);
            self.identity_source = Some(NodePortIdentitySource::Autodetect);
        }

        Ok(())
    }

    /// Check whether nodeport has enough local capability to attempt program attachment.
    async fn ensure_runtime_capable(&mut self) -> Result<bool> {
        self.refresh_runtime_identity().await?;

        let Some(iface) = self.iface.clone() else {
            let error = if let Some(node_ip) = self.node_ip {
                format!(
                    "nodeport could not find an interface that owns publication address {node_ip}; set network.nodeport.iface explicitly or assign that address to a usable interface"
                )
            } else {
                "nodeport interface missing; set network.nodeport.iface, configure network.nodeport.ip / network.advertise_addr, or assign a usable address to one up non-loopback interface".to_string()
            };
            self.degrade_runtime(error, "nodeport runtime degraded");
            return Ok(false);
        };
        let Some(node_ip) = self.node_ip else {
            self.degrade_runtime(
                    format!(
                        "nodeport address missing for interface {iface}; set network.nodeport.ip, set a concrete network.advertise_addr, or assign a usable address directly to the interface"
                    ),
                    "nodeport runtime degraded",
                );
            return Ok(false);
        };

        if let Err(err) = ifindex(&iface) {
            self.degrade_runtime(
                format!("nodeport interface {iface} is unavailable: {err:#}"),
                "nodeport runtime degraded",
            );
            return Ok(false);
        }

        if let Err(err) = ensure_iface_owns_ip(
            &iface,
            node_ip,
            match self.identity_source {
                Some(NodePortIdentitySource::NodePortIp) => "network.nodeport.ip",
                Some(NodePortIdentitySource::AdvertiseAddr) => "network.advertise_addr",
                Some(NodePortIdentitySource::InterfaceAddress) => "network.nodeport.iface",
                Some(NodePortIdentitySource::Autodetect) => "nodeport autodetect",
                None => "nodeport identity",
            },
        )
        .await
        {
            self.degrade_runtime(
                format!("nodeport publication identity is inconsistent: {err:#}"),
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
        self.userspace_flow_clears = 0;

        let mut ingress = load_program("nodeport_tc_ingress", self.capacities)
            .context("load nodeport ingress")?;
        let mut egress =
            load_program("nodeport_tc_egress", self.capacities).context("load nodeport egress")?;

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

/// Return true when any address attribute carries the provided IP address.
fn address_attrs_contain_ip(attributes: &[AddressAttribute], needle: IpAddr) -> bool {
    attributes.iter().any(|attr| match attr {
        AddressAttribute::Address(addr) | AddressAttribute::Local(addr) => *addr == needle,
        _ => false,
    })
}

/// Check whether one specific interface currently owns the provided publication IP.
///
/// This path is stricter than generic autodetect because it validates an explicit operator
/// choice in place, including loopback addresses used by the privileged test harness.
async fn iface_owns_ip(iface: &str, node_ip: IpAddr) -> Result<bool> {
    let (conn, handle, _) = new_connection()
        .context("open rtnetlink connection for nodeport interface address validation")?;
    tokio::spawn(conn);

    let Some(link) = handle
        .link()
        .get()
        .match_name(iface.to_string())
        .execute()
        .try_next()
        .await
        .context("fetch nodeport interface for address validation")?
    else {
        return Ok(false);
    };

    let mut addr_stream = handle
        .address()
        .get()
        .set_link_index_filter(link.header.index)
        .execute();

    while let Some(msg) = addr_stream
        .try_next()
        .await
        .context("enumerate nodeport interface addresses for validation")?
    {
        if address_attrs_contain_ip(&msg.attributes, node_ip) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Filter out addresses that should never become a published NodePort identity.
fn is_usable_nodeport_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => !ip.is_unspecified(),
        IpAddr::V6(ip) => !ip.is_unspecified() && !ip.is_unicast_link_local(),
    }
}

/// Resolve the first usable address in the requested family order from one link-address list.
fn first_ip_from_address_attrs(
    attributes: &[AddressAttribute],
    preferred_family: Option<NodePortIpFamily>,
) -> Option<IpAddr> {
    let mut ipv4 = None;
    let mut ipv6 = None;

    for attr in attributes {
        let Some(candidate) = (match attr {
            AddressAttribute::Address(addr) | AddressAttribute::Local(addr) => Some(*addr),
            _ => None,
        }) else {
            continue;
        };
        if !is_usable_nodeport_ip(candidate) {
            continue;
        }
        match candidate {
            IpAddr::V4(ip) if ipv4.is_none() => ipv4 = Some(IpAddr::V4(ip)),
            IpAddr::V6(ip) if ipv6.is_none() => ipv6 = Some(IpAddr::V6(ip)),
            _ => {}
        }
    }

    match preferred_family {
        Some(NodePortIpFamily::Ipv4) => ipv4.or(ipv6),
        Some(NodePortIpFamily::Ipv6) => ipv6.or(ipv4),
        None => ipv4.or(ipv6),
    }
}

/// Find the interface that owns the provided NodePort address.
async fn detect_iface_for_ip(node_ip: IpAddr) -> Result<Option<String>> {
    let (conn, handle, _) =
        new_connection().context("open rtnetlink connection for nodeport iface lookup")?;
    tokio::spawn(conn);
    let allow_loopback = match node_ip {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_loopback(),
    };

    let mut link_stream = handle.link().get().execute();
    while let Some(link) = link_stream
        .try_next()
        .await
        .context("enumerate links for nodeport iface lookup")?
    {
        let index = link.header.index;
        let name = link_name_from_attrs(&link.attributes, index);

        let flags = link.header.flags;
        if !flags.contains(LinkFlags::Up)
            || (!allow_loopback && flags.contains(LinkFlags::Loopback))
        {
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
            if address_attrs_contain_ip(&msg.attributes, node_ip) {
                return Ok(Some(name.clone()));
            }
        }
    }

    Ok(None)
}

/// Confirm that the selected publication interface currently owns the requested NodePort IP.
///
/// Explicit `network.nodeport.ip` and advertise-derived identities are only safe when the
/// chosen attach interface actually carries that address. Rejecting mismatches here prevents
/// NodePort from attaching to one interface while publishing an address that belongs to
/// another interface or is not assigned locally at all.
async fn ensure_iface_owns_ip(iface: &str, node_ip: IpAddr, source: &str) -> Result<()> {
    if iface_owns_ip(iface, node_ip).await? {
        return Ok(());
    }

    match detect_iface_for_ip(node_ip).await? {
        Some(owner) => Err(anyhow!(
            "{source} resolved {node_ip}, but interface {iface} does not own that address; it belongs to {owner}"
        )),
        None => Err(anyhow!(
            "{source} resolved {node_ip}, but that address is not assigned to interface {iface}"
        )),
    }
}

/// Return one sample VIP per requested publication family so shared per-family state can be
/// resolved only once during a NodePort sync.
fn unique_nodeport_families(entries: &[NodePortMapping]) -> Vec<(NodePortIpFamily, IpAddr)> {
    let mut families = Vec::new();
    for entry in entries {
        let family = NodePortIpFamily::from_ip(entry.vip);
        if families
            .iter()
            .any(|(existing_family, _)| *existing_family == family)
        {
            continue;
        }
        families.push((family, entry.vip));
    }
    families
}

/// Resolve one usable NodePort address assigned to a specific interface.
async fn detect_iface_ip(
    iface: &str,
    preferred_family: Option<NodePortIpFamily>,
) -> Result<Option<IpAddr>> {
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
        if let Some(ip) = first_ip_from_address_attrs(&msg.attributes, preferred_family) {
            return Ok(Some(ip));
        }
    }

    Ok(None)
}

/// Pick the first up, non-loopback interface that has a usable NodePort address.
///
/// The preferred family comes from the same default-family policy used by auto-created
/// networks, so IPv6-only hosts resolve an IPv6 identity automatically while dual-stack
/// hosts keep the existing IPv4-first behavior unless the operator explicitly prefers IPv6.
async fn detect_default_iface(
    preferred_family: Option<NodePortIpFamily>,
) -> Result<Option<(String, IpAddr)>> {
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
            if let Some(ip) = first_ip_from_address_attrs(&msg.attributes, preferred_family) {
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

/// Resolve the host-access interface address to use for NodePort SNAT in the matching family.
async fn host_access_ip(network_id: Uuid, family: NodePortIpFamily) -> Result<IpAddr> {
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
        if let Some(ip) = first_ip_from_address_attrs(&msg.attributes, Some(family)) {
            return Ok(ip);
        }
    }

    let family_label = match family {
        NodePortIpFamily::Ipv4 => "IPv4",
        NodePortIpFamily::Ipv6 => "IPv6",
    };
    Err(anyhow!(
        "host access interface {ifname} missing {family_label} address"
    ))
}

/// Resolve the configured MTU on one host-access interface so NodePort can clamp TCP MSS.
///
/// The public dataplane rewrites traffic into the overlay through the host-access veth, so
/// its MTU is the authoritative bound for the initial SYN MSS the backend should observe.
async fn host_access_mtu(network_id: Uuid) -> Result<u32> {
    let ifname = host_access_host_iface_name(network_id);
    let (conn, handle, _) =
        new_connection().context("open rtnetlink connection for nodeport MTU lookup")?;
    tokio::spawn(conn);

    let Some(link) = handle
        .link()
        .get()
        .match_name(ifname.clone())
        .execute()
        .try_next()
        .await
        .context("fetch nodeport host access link for MTU")?
    else {
        return Err(anyhow!("host access interface {ifname} not found"));
    };

    link.attributes
        .iter()
        .find_map(|attr| match attr {
            LinkAttribute::Mtu(mtu) => Some(*mtu),
            _ => None,
        })
        .ok_or_else(|| anyhow!("host access interface {ifname} missing MTU"))
}

/// Convert one host-access MTU into the per-family TCP MSS ceiling programmed into BPF maps.
///
/// Userspace computes this once during reconciliation so the tc programs only need to read a
/// scalar MSS value from their host map before clamping SYN options.
fn tcp_mss_for_family(mtu: u32, family: NodePortIpFamily) -> Result<u16> {
    let value = match family {
        NodePortIpFamily::Ipv4 => ipv4_tcp_mss_from_mtu(mtu),
        NodePortIpFamily::Ipv6 => ipv6_tcp_mss_from_mtu(mtu),
    };
    value.ok_or_else(|| anyhow!("MTU {mtu} is too small for {family:?} TCP"))
}

/// Convert one IPv4 MTU into the largest TCP MSS that still fits on that link.
fn ipv4_tcp_mss_from_mtu(mtu: u32) -> Option<u16> {
    mtu.checked_sub(IPV4_TCP_HEADER_BYTES)
        .and_then(|mss| u16::try_from(mss).ok())
}

/// Convert one IPv6 MTU into the largest TCP MSS that still fits on that link.
fn ipv6_tcp_mss_from_mtu(mtu: u32) -> Option<u16> {
    mtu.checked_sub(IPV6_TCP_HEADER_BYTES)
        .and_then(|mss| u16::try_from(mss).ok())
}

/// # Description:
///
/// Apply the configured NodePort map capacities before Aya creates or reuses the pinned tc
/// maps that back public publication and conntrack state.
fn configure_nodeport_loader_capacities(
    loader: &mut EbpfLoader<'_>,
    capacities: NodePortMapCapacities,
) -> Result<()> {
    let vip_capacity = capacities.vip_u32()?;
    let host_capacity = capacities.host_u32()?;
    let flow_capacity = capacities.flow_u32()?;

    for map_name in [
        IPV4_MAPS.vip_map,
        IPV4_MAPS.return_map,
        IPV6_MAPS.vip_map,
        IPV6_MAPS.return_map,
    ] {
        loader.set_max_entries(map_name, vip_capacity);
    }
    for map_name in [IPV4_MAPS.host_map, IPV6_MAPS.host_map] {
        loader.set_max_entries(map_name, host_capacity);
    }
    for map_name in [
        IPV4_MAPS.forward_map,
        IPV4_MAPS.reverse_map,
        IPV6_MAPS.forward_map,
        IPV6_MAPS.reverse_map,
    ] {
        loader.set_max_entries(map_name, flow_capacity);
    }

    Ok(())
}

/// Load a tc program from the local BPF artifact directory.
fn load_program(name: &str, capacities: NodePortMapCapacities) -> Result<Ebpf> {
    let resolver = ArtifactResolver::new();
    let path = resolver
        .resolve(name)
        .with_context(|| format!("resolve nodeport artifact {name}"))?;
    let map_pin_path = map_pin_dir()?;
    let mut loader = EbpfLoader::new();
    loader.map_pin_path(&map_pin_path);
    configure_nodeport_loader_capacities(&mut loader, capacities)
        .context("configure nodeport bpf map capacities")?;
    let bpf = loader.load_file(path).context("load nodeport ebpf")?;
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
    // Loopback-backed NodePort tests and local curls can route 127/8 over the host-access
    // veth after eBPF rewrites, which Linux rejects unless route_localnet is enabled on the
    // receiving interface.
    write_ipv4_sysctl(iface, "route_localnet", "1").context("set nodeport route_localnet")?;
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

/// Read one pinned per-CPU `u64` array and aggregate each index across every CPU slot.
fn read_u64_percpu_array(name: &str, entries: usize) -> Result<Vec<u64>> {
    let base = map_pin_dir()?;
    let map = open_map(&base, name).with_context(|| format!("open {name}"))?;
    let array = PerCpuArray::<_, u64>::try_from(Map::PerCpuArray(map))
        .with_context(|| format!("interpret {name} as per-cpu u64 array"))?;
    let mut totals = vec![0u64; entries];
    for (index, total) in totals.iter_mut().enumerate() {
        let values = array
            .get(&(index as u32), 0)
            .with_context(|| format!("read counter slot {index} from {name}"))?;
        for value in values.iter().copied() {
            *total += value;
        }
    }
    Ok(totals)
}

/// Count the number of live entries in one pinned BPF map by walking its keys through the
/// kernel `get_next_key` interface.
fn count_pinned_map_entries<K>(name: &str) -> Result<usize>
where
    K: Pod + Copy + Default,
{
    let base = map_pin_dir()?;
    let map = open_map(&base, name).with_context(|| format!("open {name}"))?;
    count_map_keys::<K>(map.fd().as_fd().as_raw_fd())
}

/// Clear every stale NodePort flow mapping identified during one selector sync.
///
/// The sync path removes stale selectors from the public VIP maps before calling this helper,
/// which guarantees new packets cannot recreate the old translation while flow cleanup walks
/// the pinned forward and reverse caches.
fn clear_stale_nodeport_flows(
    base: &Path,
    stale: &[(NodePortSelector, NodePortPublishedMapping)],
) -> Result<u64> {
    let mut cleared_pairs = 0u64;
    for (selector, mapping) in stale {
        cleared_pairs =
            cleared_pairs.saturating_add(clear_nodeport_flows(base, *selector, *mapping)?);
    }
    Ok(cleared_pairs)
}

/// Clear one stale selector's flow cache from the family that owns its VIP.
///
/// Selector cleanup is family-local because IPv4 and IPv6 NodePort use separate pinned map
/// sets. Matching on the resolved mapping keeps the cleanup rules aligned with the current map
/// layout instead of depending on stringly-typed pin names at call sites.
fn clear_nodeport_flows(
    base: &Path,
    selector: NodePortSelector,
    mapping: NodePortPublishedMapping,
) -> Result<u64> {
    match (mapping.vip, mapping.node_ip) {
        (IpAddr::V4(vip), IpAddr::V4(node_ip)) => {
            clear_nodeport_flows_v4(base, selector, vip, mapping.vip_port, node_ip)
        }
        (IpAddr::V6(vip), IpAddr::V6(node_ip)) => {
            clear_nodeport_flows_v6(base, selector, vip, mapping.vip_port, node_ip)
        }
        (vip, node_ip) => Err(anyhow!(
            "nodeport stale mapping used mixed IP families: vip={vip} node_ip={node_ip}"
        )),
    }
}

/// Clear stale IPv4 forward and reverse flow entries for one published selector.
///
/// Both directions must be purged together so no reply packet can keep using a public mapping
/// that the control plane has already retired or repointed.
fn clear_nodeport_flows_v4(
    base: &Path,
    selector: NodePortSelector,
    vip: std::net::Ipv4Addr,
    vip_port: u16,
    node_ip: std::net::Ipv4Addr,
) -> Result<u64> {
    let mut cleared_pairs = 0u64;
    if let Ok(fwd_map) = open_map(base, IPV4_MAPS.forward_map) {
        cleared_pairs = cleared_pairs.saturating_add(clear_nodeport_forward_flows_v4(
            fwd_map.fd().as_fd().as_raw_fd(),
            selector,
            vip,
            vip_port,
            node_ip,
        )?);
    }
    if let Ok(rev_map) = open_map(base, IPV4_MAPS.reverse_map) {
        clear_nodeport_reverse_flows_v4(
            rev_map.fd().as_fd().as_raw_fd(),
            selector,
            vip,
            vip_port,
            node_ip,
        )?;
    }
    Ok(cleared_pairs)
}

/// Clear stale IPv6 forward and reverse flow entries for one published selector.
///
/// IPv6 cleanup follows the same selector-level contract as IPv4 so dual-stack services do not
/// need different churn semantics during public mapping updates.
fn clear_nodeport_flows_v6(
    base: &Path,
    selector: NodePortSelector,
    vip: std::net::Ipv6Addr,
    vip_port: u16,
    node_ip: std::net::Ipv6Addr,
) -> Result<u64> {
    let mut cleared_pairs = 0u64;
    if let Ok(fwd_map) = open_map(base, IPV6_MAPS.forward_map) {
        cleared_pairs = cleared_pairs.saturating_add(clear_nodeport_forward_flows_v6(
            fwd_map.fd().as_fd().as_raw_fd(),
            selector,
            vip,
            vip_port,
            node_ip,
        )?);
    }
    if let Ok(rev_map) = open_map(base, IPV6_MAPS.reverse_map) {
        clear_nodeport_reverse_flows_v6(
            rev_map.fd().as_fd().as_raw_fd(),
            selector,
            vip,
            vip_port,
            node_ip,
        )?;
    }
    Ok(cleared_pairs)
}

/// Remove IPv4 forward flows that still point at one stale public selector.
///
/// Forward flow keys are shared by every packet that was already translated into the overlay
/// VIP tuple, so cleanup also checks the cached public port and node IP before deleting an
/// entry. That avoids clearing unrelated selectors that intentionally share a VIP target.
fn clear_nodeport_forward_flows_v4(
    fd: std::os::fd::RawFd,
    selector: NodePortSelector,
    vip: std::net::Ipv4Addr,
    vip_port: u16,
    node_ip: std::net::Ipv4Addr,
) -> Result<u64> {
    let mut cleared_pairs = 0u64;
    visit_map_keys::<Flow4, _>(fd, |next| {
        let matches_selector = lookup_elem::<Flow4, NodePortNat>(fd, &next)?
            .map(|entry| {
                next.dst == u32::from_ne_bytes(vip.octets())
                    && next.dst_port == vip_port.to_be()
                    && next.proto == selector.protocol.number()
                    && entry.node_ip == u32::from_ne_bytes(node_ip.octets())
                    && entry.node_port == selector.port.to_be()
            })
            .unwrap_or(false);
        if matches_selector {
            cleared_pairs += 1;
        }
        Ok(matches_selector)
    })?;
    Ok(cleared_pairs)
}

/// Remove IPv4 reverse flows that still restore one stale public selector.
///
/// Reverse keys already carry the VIP tuple, but the cached NAT value still disambiguates
/// multiple public ports that intentionally share the same service target.
fn clear_nodeport_reverse_flows_v4(
    fd: std::os::fd::RawFd,
    selector: NodePortSelector,
    vip: std::net::Ipv4Addr,
    vip_port: u16,
    node_ip: std::net::Ipv4Addr,
) -> Result<()> {
    visit_map_keys::<Flow4, _>(fd, |next| {
        let matches_selector = lookup_elem::<Flow4, NodePortNat>(fd, &next)?
            .map(|entry| {
                next.src == u32::from_ne_bytes(vip.octets())
                    && next.src_port == vip_port.to_be()
                    && next.proto == selector.protocol.number()
                    && entry.node_ip == u32::from_ne_bytes(node_ip.octets())
                    && entry.node_port == selector.port.to_be()
            })
            .unwrap_or(false);
        Ok(matches_selector)
    })
}

/// Remove IPv6 forward flows that still point at one stale public selector.
///
/// Matching the cached public selector in the NAT value keeps cleanup precise even when more
/// than one external port targets the same IPv6 VIP and service port.
fn clear_nodeport_forward_flows_v6(
    fd: std::os::fd::RawFd,
    selector: NodePortSelector,
    vip: std::net::Ipv6Addr,
    vip_port: u16,
    node_ip: std::net::Ipv6Addr,
) -> Result<u64> {
    let mut cleared_pairs = 0u64;
    visit_map_keys::<Flow6, _>(fd, |next| {
        let matches_selector = lookup_elem::<Flow6, NodePortNat6>(fd, &next)?
            .map(|entry| {
                next.dst == vip.octets()
                    && next.dst_port == vip_port.to_be()
                    && next.proto == selector.protocol.number()
                    && entry.node_ip == node_ip.octets()
                    && entry.node_port == selector.port.to_be()
            })
            .unwrap_or(false);
        if matches_selector {
            cleared_pairs += 1;
        }
        Ok(matches_selector)
    })?;
    Ok(cleared_pairs)
}

/// Remove IPv6 reverse flows that still restore one stale public selector.
///
/// The reverse direction uses the same selector match as the forward map so the two caches
/// retire together and leave no half-stale return path behind.
fn clear_nodeport_reverse_flows_v6(
    fd: std::os::fd::RawFd,
    selector: NodePortSelector,
    vip: std::net::Ipv6Addr,
    vip_port: u16,
    node_ip: std::net::Ipv6Addr,
) -> Result<()> {
    visit_map_keys::<Flow6, _>(fd, |next| {
        let matches_selector = lookup_elem::<Flow6, NodePortNat6>(fd, &next)?
            .map(|entry| {
                next.src == vip.octets()
                    && next.src_port == vip_port.to_be()
                    && next.proto == selector.protocol.number()
                    && entry.node_ip == node_ip.octets()
                    && entry.node_port == selector.port.to_be()
            })
            .unwrap_or(false);
        Ok(matches_selector)
    })
}

/// Iterate every key in one pinned BPF map so stale entries can be deleted in place.
///
/// Resetting the cursor after a delete mirrors the kernel's `get_next_key` contract and keeps
/// cleanup correct even when adjacent entries are removed during the same scan.
fn visit_map_keys<K, F>(fd: std::os::fd::RawFd, mut visitor: F) -> Result<()>
where
    K: Pod + Copy + Default,
    F: FnMut(K) -> Result<bool>,
{
    // `BPF_MAP_GET_NEXT_KEY` walks pinned maps without needing Aya map wrappers.
    const BPF_MAP_GET_NEXT_KEY: libc::c_uint = 4;

    #[repr(C)]
    struct BpfAttrKeyIter {
        map_fd: u32,
        _pad: u32,
        key: u64,
        next_key: u64,
    }

    let mut cursor: Option<K> = None;
    loop {
        let mut next = K::default();
        let mut iter = BpfAttrKeyIter {
            map_fd: fd as u32,
            _pad: 0,
            key: cursor
                .as_ref()
                .map(|key| key as *const _ as u64)
                .unwrap_or(0),
            next_key: &mut next as *mut _ as u64,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_GET_NEXT_KEY,
                &mut iter as *mut _,
                mem::size_of::<BpfAttrKeyIter>(),
            )
        };
        if ret < 0 {
            break;
        }

        if visitor(next)? {
            let _ = delete_elem(fd, &next);
            cursor = None;
        } else {
            cursor = Some(next);
        }
    }

    Ok(())
}

/// Count the current number of keys in one pinned BPF map without mutating its contents.
fn count_map_keys<K>(fd: std::os::fd::RawFd) -> Result<usize>
where
    K: Pod + Copy + Default,
{
    let mut count = 0usize;
    visit_map_keys::<K, _>(fd, |_next| {
        count = count.saturating_add(1);
        Ok(false)
    })?;
    Ok(count)
}

/// Remove pinned nodeport maps so new layouts can be loaded atomically.
fn reset_nodeport_maps(root: &Path) -> Result<()> {
    let maps = [
        "NODEPORT_FWD",
        "NODEPORT_FWD_V6",
        "NODEPORT_REV",
        "NODEPORT_REV_V6",
        "NODEPORT_VIPS",
        "NODEPORT_VIPS_V6",
        "NODEPORT_RETURNS",
        "NODEPORT_RETURNS_V6",
        "NODEPORT_HOST",
        "NODEPORT_HOST_V6",
        "NODEPORT_TC_INGRESS_STATS",
        "NODEPORT_TC_INGRESS_DROP_REASONS",
        "NODEPORT_TC_EGRESS_STATS",
        "NODEPORT_TC_FLOW_EVENTS",
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
    // `BPF_MAP_UPDATE_ELEM` lets NodePort publish VIP, host, and flow entries atomically.
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
    // `BPF_MAP_DELETE_ELEM` removes stale selectors and cached reverse-flow state.
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

/// Look up one pinned BPF map entry by key, returning `None` when the entry is absent.
///
/// Selector cleanup uses direct lookups while iterating flow keys so it can distinguish
/// multiple public selectors that intentionally reuse the same VIP target tuple.
fn lookup_elem<K: Pod, V: Pod + Default>(fd: i32, key: &K) -> Result<Option<V>> {
    // `BPF_MAP_LOOKUP_ELEM` reads reverse-flow metadata before deciding whether to delete a key.
    const BPF_MAP_LOOKUP_ELEM: libc::c_uint = 1;

    #[repr(C)]
    struct BpfAttrLookup {
        map_fd: u32,
        _pad: u32,
        key: u64,
        value: u64,
    }

    let mut value = V::default();
    let mut attr = BpfAttrLookup {
        map_fd: fd as u32,
        _pad: 0,
        key: key as *const _ as u64,
        value: &mut value as *mut _ as u64,
    };

    let ret = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            BPF_MAP_LOOKUP_ELEM,
            &mut attr as *mut _,
            mem::size_of::<BpfAttrLookup>(),
        )
    };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ENOENT) {
            return Ok(None);
        }
        return Err(err.into());
    }
    Ok(Some(value))
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
