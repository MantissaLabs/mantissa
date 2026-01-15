use anyhow::Result;
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

const NODEPORT_PROTO_TCP: u8 = 6;
const NODEPORT_PROTO_UDP: u8 = 17;

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

impl NodePortManager {
    /// Build a nodeport manager using environment configuration for external interfaces.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AsyncMutex::new(PlatformNodePortManager::new())),
        }
    }

    /// Synchronize nodeport mappings for a specific network so external traffic can reach VIPs.
    pub async fn sync_ports(
        &self,
        network_id: Uuid,
        entries: &[NodePortMapping],
    ) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.sync_ports(network_id, entries).await
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
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{NodePortMapping, NodePortProtocol};
    use crate::network::attachment::host_access_host_iface_name;
    use crate::network::wireguard::MANTISSA_WIREGUARD_IFNAME;
    use crate::node::address::compute_advertise_ip;
    use anyhow::{Context, Result, anyhow};
    use aya::Pod;
    use aya::maps::MapData;
    use aya::programs::ProgramError;
    use aya::programs::tc::{SchedClassifier, TcAttachType, qdisc_add_clsact};
    use aya::{Ebpf, EbpfLoader};
    use futures::TryStreamExt;
    use libc::if_nametoindex;
    use nix::mount::{MsFlags, mount};
    use nix::sys::statfs::{BPF_FS_MAGIC, statfs};
    use rtnetlink::packet_route::address::AddressAttribute;
    use rtnetlink::packet_route::link::{LinkAttribute, LinkFlags};
    use rtnetlink::new_connection;
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
        enabled: bool,
        iface: Option<String>,
        node_ip: Option<Ipv4Addr>,
        attachment: Option<NodePortAttachment>,
        ports_by_network: HashMap<Uuid, HashSet<NodePortSelector>>,
        port_owner: HashMap<NodePortSelector, Uuid>,
        host_ingress_attached: HashSet<Uuid>,
    }

    impl PlatformNodePortManager {
        /// Capture nodeport configuration from the environment for later attachment.
        pub(super) fn new() -> Self {
            let iface = env::var("MANTISSA_NODEPORT_IFACE").ok();
            let node_ip = env::var("MANTISSA_NODEPORT_IP")
                .ok()
                .and_then(|val| val.parse::<Ipv4Addr>().ok())
                .or_else(|| compute_advertise_ip(None, None).ok().and_then(|ip| match ip {
                    IpAddr::V4(v4) => Some(v4),
                    _ => None,
                }));
            let enabled = env::var_os("MANTISSA_BPF_NO_ATTACH").is_none()
                && env::var_os("MANTISSA_SKIP_BPF").is_none();

            if enabled {
                info!(
                    target: "network",
                    iface = ?iface,
                    node_ip = ?node_ip,
                    "nodeport external load balancer enabled"
                );
                if iface.is_none() || node_ip.is_none() {
                    debug!(
                        target: "network",
                        iface = ?iface,
                        node_ip = ?node_ip,
                        "nodeport will auto-detect missing interface settings"
                    );
                }
            } else {
                debug!(
                    target: "network",
                    iface = ?iface,
                    node_ip = ?node_ip,
                    "nodeport external load balancer disabled"
                );
            }

            Self {
                enabled,
                iface,
                node_ip,
                attachment: None,
                ports_by_network: HashMap::new(),
                port_owner: HashMap::new(),
                host_ingress_attached: HashSet::new(),
            }
        }

        /// Sync the nodeport map to match the declared mappings for a network.
        pub(super) async fn sync_ports(
            &mut self,
            network_id: Uuid,
            entries: &[NodePortMapping],
        ) -> Result<()> {
            if !self.enabled {
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
            self.ensure_attached().await?;
            if !self.enabled {
                return Ok(());
            }
            if !entries.is_empty() {
                self.ensure_host_ingress(network_id).await?;
            }

            let node_ip = self
                .node_ip
                .ok_or_else(|| anyhow!("nodeport node_ip missing"))?;
            let overlay_ifindex_opt = if entries.is_empty() {
                None
            } else {
                Some(overlay_ifindex(network_id)?)
            };
            let mut desired_ports = HashSet::new();
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
            } else if had_ports {
                if let Ok(overlay_ifindex) = overlay_ifindex(network_id) {
                    if let Ok(host_map) = open_map(&base, "NODEPORT_HOST") {
                        let host_fd = host_map.fd().as_fd().as_raw_fd();
                        let _ = delete_elem(host_fd, &overlay_ifindex);
                    }
                }
            }
            let overlay_index = if entries.is_empty() {
                0
            } else {
                overlay_ifindex_opt.ok_or_else(|| anyhow!("nodeport overlay ifindex missing"))?
            };

            for entry in entries {
                let selector = NodePortSelector::new(entry.port, entry.protocol);
                desired_ports.insert(selector);
                if let Some(owner) = self.port_owner.get(&selector)
                    && *owner != network_id
                {
                    warn!(
                        target: "network",
                        port = entry.port,
                        protocol = %entry.protocol,
                        existing = %owner,
                        requested = %network_id,
                        "nodeport conflict; keeping existing owner"
                    );
                    continue;
                }

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

            let known = self
                .ports_by_network
                .entry(network_id)
                .or_default()
                .clone();
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
            Ok(())
        }

        /// Attach nodeport programs to the configured external interface if not already attached.
        async fn ensure_attached(&mut self) -> Result<()> {
            if self.attachment.is_some() {
                return Ok(());
            }
            if self.iface.is_none() || self.node_ip.is_none() {
                if let Err(err) = self.autodetect_iface().await {
                    warn!(
                        target: "network",
                        "nodeport interface autodetection failed: {err:#}"
                    );
                }
            }

            let Some(iface) = self.iface.clone() else {
                warn!(
                    target: "network",
                    "nodeport interface missing; disable nodeport (set MANTISSA_NODEPORT_IFACE to override)"
                );
                self.enabled = false;
                return Ok(());
            };
            let Some(node_ip) = self.node_ip else {
                warn!(
                    target: "network",
                    iface = %iface,
                    "nodeport IP missing; disable nodeport (set MANTISSA_NODEPORT_IP to override)"
                );
                self.enabled = false;
                return Ok(());
            };
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
            let mut egress =
                load_program("nodeport_tc_egress").context("load nodeport egress")?;

            attach_tc(&mut ingress, &iface, TcAttachType::Ingress, "nodeport_tc_ingress")
                .context("attach nodeport ingress tc")?;
            attach_tc(&mut egress, &iface, TcAttachType::Egress, "nodeport_tc_egress")
                .context("attach nodeport egress tc")?;
            if let Err(err) = ensure_clsact("lo") {
                warn!(
                    target: "network",
                    "unable to enable nodeport on loopback: {err:#}"
                );
            } else if let Err(err) =
                attach_tc(&mut ingress, "lo", TcAttachType::Ingress, "nodeport_tc_ingress")
            {
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
            Ok(())
        }

        /// Attach nodeport SNAT handling to the host-access interface for a network.
        async fn ensure_host_ingress(&mut self, network_id: Uuid) -> Result<()> {
            if self.host_ingress_attached.contains(&network_id) {
                return Ok(());
            }
            let Some(attachment) = self.attachment.as_mut() else {
                return Ok(());
            };

            let iface = host_access_host_iface_name(network_id);
            let ifindex = match ifindex(&iface) {
                Ok(index) => index,
                Err(err) => {
                    debug!(
                        target: "network",
                        iface = %iface,
                        "nodeport host-access interface not ready: {err:#}"
                    );
                    return Ok(());
                }
            };

            ensure_clsact(&iface)?;
            if let Err(err) = configure_host_access_sysctls(&iface) {
                warn!(
                    target: "network",
                    iface = %iface,
                    "nodeport host-access sysctls could not be applied: {err:#}"
                );
            }
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
            Ok(())
        }

        /// Attempt to find a usable interface when one is not configured explicitly.
        async fn autodetect_iface(&mut self) -> Result<()> {
            if self.iface.is_some() && self.node_ip.is_some() {
                return Ok(());
            }

            if let Some(iface) = self.iface.clone() {
                if self.node_ip.is_none() {
                    match detect_iface_ip(&iface).await? {
                        Some(ip) => {
                            info!(
                                target: "network",
                                iface = %iface,
                                node_ip = %ip,
                                "nodeport selected IP on configured interface"
                            );
                            self.node_ip = Some(ip);
                        }
                        None => {
                            warn!(
                                target: "network",
                                iface = %iface,
                                "nodeport could not find IPv4 address for configured interface"
                            );
                        }
                    }
                    return Ok(());
                }
            }

            if let Some(node_ip) = self.node_ip {
                if let Some(iface) = detect_iface_for_ip(node_ip).await? {
                    info!(
                        target: "network",
                        iface = %iface,
                        node_ip = %node_ip,
                        "nodeport selected interface matching advertise IP"
                    );
                    self.iface = Some(iface);
                    return Ok(());
                }
                warn!(
                    target: "network",
                    node_ip = %node_ip,
                    "nodeport could not find interface matching node IP"
                );
                return Ok(());
            }

            if let Some((iface, ip)) = detect_default_iface().await? {
                info!(
                    target: "network",
                    iface = %iface,
                    node_ip = %ip,
                    "nodeport selected default interface"
                );
                self.iface = Some(iface);
                self.node_ip = Some(ip);
            }

            Ok(())
        }
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
            let name = link
                .attributes
                .iter()
                .find_map(|attr| match attr {
                    LinkAttribute::IfName(name) => Some(name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| format!("ifindex{index}"));

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
                for attr in msg.attributes.iter() {
                    if let AddressAttribute::Address(addr) | AddressAttribute::Local(addr) = attr {
                        if let IpAddr::V4(ip) = *addr {
                            if ip == node_ip {
                                return Ok(Some(name.clone()));
                            }
                        }
                    }
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
            for attr in msg.attributes.iter() {
                if let AddressAttribute::Address(addr) | AddressAttribute::Local(addr) = attr {
                    if let IpAddr::V4(ip) = *addr {
                        return Ok(Some(ip));
                    }
                }
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
            let name = link
                .attributes
                .iter()
                .find_map(|attr| match attr {
                    LinkAttribute::IfName(name) => Some(name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| format!("ifindex{index}"));

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
                for attr in msg.attributes.iter() {
                    if let AddressAttribute::Address(addr) | AddressAttribute::Local(addr) = attr {
                        if let IpAddr::V4(ip) = *addr {
                            return Ok(Some((name.clone(), ip)));
                        }
                    }
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
            for attr in msg.attributes.iter() {
                if let AddressAttribute::Address(addr) | AddressAttribute::Local(addr) = attr {
                    if let IpAddr::V4(ip) = *addr {
                        return Ok(ip);
                    }
                }
            }
        }

        Err(anyhow!("host access interface {ifname} missing IPv4 address"))
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
        write_ipv4_sysctl(iface, "accept_local", "1")
            .context("set nodeport accept_local")?;
        write_ipv4_sysctl(iface, "rp_filter", "0")
            .context("disable nodeport rp_filter")?;
        Ok(())
    }

    /// Write a per-interface IPv4 sysctl to allow nodeport hairpin responses.
    fn write_ipv4_sysctl(iface: &str, key: &str, value: &str) -> Result<()> {
        let path = Path::new("/proc/sys/net/ipv4/conf")
            .join(iface)
            .join(key);
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

    struct ArtifactResolver {
        search_roots: Vec<PathBuf>,
    }

    impl ArtifactResolver {
        /// Build a resolver using the same search roots as the core BPF loader.
        fn new() -> Self {
            let mut roots = Vec::new();
            if let Some(dir) = env::var_os("MANTISSA_BPF_DIR") {
                roots.push(PathBuf::from(dir));
            }
            if let Ok(pwd) = env::current_dir() {
                roots.push(pwd.join("target/bpf"));
                roots.push(pwd.join("assets/bpf"));
            }
            Self { search_roots: roots }
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
