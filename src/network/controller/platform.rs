#[cfg(target_os = "linux")]
mod linux {
    use super::super::{
        LINK_STATE_SETTLE_ATTEMPTS, LINK_STATE_SETTLE_DELAY, LINK_STATE_UPDATE_RETRIES,
        NetworkPlan, VXLAN_PORT, collect_orphaned_network_suffixes, format_mac,
        is_managed_overlay_link_name,
    };
    use crate::config;
    use crate::ip_family::DefaultIpFamilyPolicy;
    use crate::network::addressing::resolve_advertise_ip;
    use crate::network::attachment::{host_access_host_iface_name, host_access_peer_iface_name};
    use crate::network::wireguard::MANTISSA_WIREGUARD_IFNAME;
    use anyhow::{Context, Result, anyhow};
    use etherparse::{ArpHardwareId, ArpOperation, ArpPacket, EtherType, PacketBuilder};
    use futures::TryStreamExt;
    use libc;
    use netlink_packet_core::{DefaultNla, Nla};
    use netlink_packet_utils::nla::{NLA_ALIGNTO, NLA_F_NESTED, NLA_HEADER_SIZE, NlaBuffer};
    use rtnetlink::packet_route::AddressFamily;
    use rtnetlink::packet_route::address::AddressAttribute;
    use rtnetlink::packet_route::link::{
        InfoBridgePort, InfoData, InfoKind, InfoPortData, InfoVxlan, LinkAttribute, LinkFlags,
        LinkHeader, LinkInfo, LinkProtoInfoBridge,
    };
    use rtnetlink::packet_route::route::{RouteAddress, RouteAttribute, RouteHeader};
    use rtnetlink::{
        AddressMessageBuilder, Error as RtnetlinkError, Handle, LinkBridge, LinkMessageBuilder,
        LinkUnspec, LinkVeth, LinkVxlan, RouteMessageBuilder, new_connection,
    };
    use std::fs;
    use std::mem;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    use std::process::Command;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tracing::{debug, info, warn};

    #[derive(Clone, Debug)]
    struct ResolvedUnderlay {
        index: u32,
        name: String,
        ip: IpAddr,
        mtu: u32,
    }

    #[derive(Clone)]
    pub struct NetworkProvisioner {
        handle: Option<Handle>,
        underlay: Arc<AsyncMutex<Option<ResolvedUnderlay>>>,
    }

    /// Link attribute containing bridge-port protocol information.
    const IFLA_PROTINFO: u16 = 12;
    /// Bridge-port attribute controlling whether frames can egress back out their ingress port.
    const BRPORT_ATTR_HAIRPIN_MODE: u16 = 4;
    /// Bridge-port attribute controlling MAC learning on the VXLAN port.
    const BRPORT_ATTR_LEARNING: u16 = 8;
    /// Bridge-port attribute controlling unknown unicast flooding.
    const BRPORT_ATTR_UNICAST_FLOOD: u16 = 9;
    /// Bridge-port attribute controlling neighbour suppression.
    const BRPORT_ATTR_NEIGH_SUPPRESS: u16 = 32;
    /// Bridge-port attribute controlling multicast flooding.
    const BRPORT_ATTR_MULTICAST_FLOOD: u16 = 27;
    /// Bridge-port attribute controlling broadcast flooding.
    const BRPORT_ATTR_BROADCAST_FLOOD: u16 = 30;

    /// Convert a resolver address into the rtnetlink address family used by interface rows.
    fn address_family(ip: IpAddr) -> AddressFamily {
        match ip {
            IpAddr::V4(_) => AddressFamily::Inet,
            IpAddr::V6(_) => AddressFamily::Inet6,
        }
    }

    /// Return the VXLAN encapsulation overhead required by one underlay IP family.
    ///
    /// Linux derives the default VXLAN device MTU from the lower-device MTU minus the UDP/IP
    /// encapsulation size. Mantissa mirrors that rule centrally so all overlay interfaces share
    /// the same effective MTU instead of relying on kernel-side failures after creation.
    fn vxlan_underlay_overhead(ip: IpAddr) -> u32 {
        match ip {
            IpAddr::V4(_) => 50,
            IpAddr::V6(_) => 70,
        }
    }

    /// Clamp one requested overlay MTU to the ceiling supported by the resolved underlay.
    ///
    /// This keeps the bridge and VXLAN interfaces aligned with the selected transport path even
    /// when the replicated network spec asks for a larger MTU than the current underlay can carry.
    fn clamp_overlay_mtu(requested_mtu: u32, underlay: &ResolvedUnderlay) -> Result<u32> {
        let overhead = vxlan_underlay_overhead(underlay.ip);
        let ceiling = underlay.mtu.checked_sub(overhead).ok_or_else(|| {
            anyhow!(
                "underlay {} (idx {}) mtu {} is too small for vxlan over {} (overhead {})",
                underlay.name,
                underlay.index,
                underlay.mtu,
                underlay.ip,
                overhead
            )
        })?;
        if ceiling == 0 {
            anyhow::bail!(
                "underlay {} (idx {}) leaves no payload mtu for vxlan encapsulation",
                underlay.name,
                underlay.index
            );
        }
        Ok(requested_mtu.min(ceiling))
    }

    /// Return the preferred default-route family order for plaintext underlay discovery.
    ///
    /// An explicit advertise address takes precedence. Otherwise we follow the configured default
    /// IP-family policy and still fall back to the opposite family if the preferred table has no
    /// usable default route.
    fn preferred_route_families(advertise_ip: Option<IpAddr>) -> [AddressFamily; 2] {
        match advertise_ip {
            Some(IpAddr::V6(_)) => [AddressFamily::Inet6, AddressFamily::Inet],
            Some(IpAddr::V4(_)) => [AddressFamily::Inet, AddressFamily::Inet6],
            None => match config::default_ip_family_policy() {
                DefaultIpFamilyPolicy::Ipv6 => [AddressFamily::Inet6, AddressFamily::Inet],
                _ => [AddressFamily::Inet, AddressFamily::Inet6],
            },
        }
    }

    impl NetworkProvisioner {
        /// Returns a kernel-backed network provisioner when the current node is allowed to touch
        /// host interfaces, or a stub provisioner otherwise.
        pub fn new() -> Result<Self> {
            if !config::kernel_network_provisioning_enabled() {
                debug!(
                    target: "network",
                    "kernel network provisioning disabled by config; using stub network provisioner"
                );
                return Ok(Self::unavailable());
            }
            if unsafe { libc::geteuid() } != 0 {
                debug!(
                    target: "network",
                    "running unprivileged; using stub network provisioner"
                );
                return Ok(Self::unavailable());
            }
            Self::ensure_vxlan_module().context("load vxlan kernel module")?;

            match new_connection() {
                Ok((connection, handle, _)) => {
                    tokio::spawn(connection);
                    Ok(Self {
                        handle: Some(handle),
                        underlay: Arc::new(AsyncMutex::new(None)),
                    })
                }
                Err(err) => {
                    debug!(
                        target: "network",
                        "failed to open rtnetlink connection for network provisioner: {err}"
                    );
                    Ok(Self::unavailable())
                }
            }
        }

        /// Returns a provisioning stub for environments without kernel networking access.
        pub fn unavailable() -> Self {
            Self {
                handle: None,
                underlay: Arc::new(AsyncMutex::new(None)),
            }
        }

        /// Report whether this host can provision kernel interfaces and therefore bind resolver IPs.
        pub fn supports_resolver_bind(&self) -> bool {
            self.handle.is_some()
        }

        /// Delete any leaked per-network links whose suffix no longer corresponds to a live network.
        ///
        /// Crashed runs or externally interrupted tests can leave `mnhost-*`, `mnhp-*`, `mvx-*`,
        /// and `mnt-br-*` interfaces behind. Those host-access links install connected overlay
        /// routes, so a single orphan is enough to hijack host-originated health probes away from
        /// the live overlay and make service discovery think every backend is unhealthy.
        pub async fn cleanup_orphaned_network_links(
            &self,
            desired: &std::collections::HashSet<uuid::Uuid>,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let links = self
                .list_link_indices()
                .await
                .context("list kernel links for orphan cleanup")?;
            let orphaned_suffixes = collect_orphaned_network_suffixes(desired, links.keys());
            if orphaned_suffixes.is_empty() {
                return Ok(());
            }

            for suffix in orphaned_suffixes {
                for link_name in [
                    format!("mnhost-{suffix}"),
                    format!("mnhp-{suffix}"),
                    format!("mvx-{suffix}"),
                    format!("mnt-br-{suffix}"),
                ] {
                    let Some(index) = links.get(&link_name).copied() else {
                        continue;
                    };
                    self.delete_link_if_present(handle, index, &link_name)
                        .await
                        .with_context(|| format!("delete orphaned link {link_name}"))?;
                }
            }

            Ok(())
        }

        /// Returns whether any deterministic kernel link for this overlay still exists.
        ///
        /// A restarted process can lose the in-memory active-network marker while the host still
        /// has links from the previous process. Deleted-network cleanup uses this check to avoid
        /// skipping teardown in that state.
        pub async fn network_links_exist(&self, plan: &NetworkPlan) -> Result<bool> {
            let (host_ifname, peer_ifname) = Self::host_access_ifnames(plan.network_id);
            Ok(self.find_link(&host_ifname).await?.is_some()
                || self.find_link(&peer_ifname).await?.is_some()
                || self.find_link(&plan.vxlan_name).await?.is_some()
                || self.find_link(&plan.bridge_name).await?.is_some())
        }

        /// Return the rtnetlink handle when kernel provisioning is available.
        fn handle(&self) -> Option<&Handle> {
            self.handle.as_ref()
        }

        /// Snapshot current link names to indices so cleanup can remove leaked interfaces in one pass.
        async fn list_link_indices(&self) -> Result<std::collections::HashMap<String, u32>> {
            let Some(handle) = self.handle() else {
                return Ok(std::collections::HashMap::new());
            };

            let mut links = std::collections::HashMap::new();
            let mut stream = handle.link().get().execute();
            while let Some(message) = stream.try_next().await.context("list links")? {
                let Some(name) = message
                    .attributes
                    .iter()
                    .find_map(|attribute| match attribute {
                        LinkAttribute::IfName(name) => Some(name.clone()),
                        _ => None,
                    })
                else {
                    continue;
                };
                links.insert(name, message.header.index);
            }

            Ok(links)
        }

        /// Delete one link index while tolerating races where a parent deletion already removed it.
        async fn delete_link_if_present(
            &self,
            handle: &Handle,
            link_index: u32,
            link_name: &str,
        ) -> Result<()> {
            match handle.link().del(link_index).execute().await {
                Ok(()) => Ok(()),
                Err(RtnetlinkError::NetlinkError(msg)) => {
                    let errno = msg.raw_code().abs();
                    if errno == libc::ENOENT || errno == libc::ENODEV {
                        debug!(
                            target: "network",
                            link = link_name,
                            errno,
                            "orphan cleanup raced with link deletion; continuing"
                        );
                        Ok(())
                    } else {
                        Err(RtnetlinkError::NetlinkError(msg))
                            .with_context(|| format!("delete link {link_name}"))
                    }
                }
                Err(err) => Err(err).with_context(|| format!("delete link {link_name}")),
            }
        }

        /// Delete one link and wait until the kernel no longer resolves the link name.
        ///
        /// Recovery paths recreate interfaces with the same deterministic name. Waiting for the
        /// original link to disappear closes the race where rtnetlink accepts the delete but the
        /// next lookup still resolves to the stale kernel object for a short window.
        async fn delete_link_and_wait_absent(
            &self,
            handle: &Handle,
            link_index: u32,
            link_name: &str,
        ) -> Result<()> {
            self.delete_link_if_present(handle, link_index, link_name)
                .await?;

            for _ in 0..20 {
                if self.find_link(link_name).await?.is_none() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }

            let remaining = self.find_link(link_name).await?;
            Err(anyhow!(
                "link {link_name} still present after delete request; remaining_index={remaining:?}"
            ))
        }

        /// Compute stable interface names for the per-network host access veth pair.
        ///
        /// This veth is used to inject host-originated traffic into the overlay bridge as a
        /// *bridge port ingress*, so the tc-ingress eBPF programs (ARP responder + DNAT) see the
        /// packets just like container traffic.
        fn host_access_ifnames(network_id: uuid::Uuid) -> (String, String) {
            (
                host_access_host_iface_name(network_id),
                host_access_peer_iface_name(network_id),
            )
        }

        /// Ensure the host has a dedicated veth pair wired into the overlay bridge.
        ///
        /// The host side remains L3 (keeps IP addresses/routes), while the peer is enslaved to the
        /// bridge so packets traverse the same dataplane as workload veth devices.
        async fn ensure_host_access_veth(
            &self,
            network_id: uuid::Uuid,
            bridge_index: u32,
            host_mac: Option<[u8; 6]>,
        ) -> Result<(u32, u32)> {
            let Some(handle) = self.handle() else {
                return Ok((0, 0));
            };

            let (host_ifname, peer_ifname) = Self::host_access_ifnames(network_id);
            let host_existing = self.find_link(&host_ifname).await?;
            let peer_existing = self.find_link(&peer_ifname).await?;

            let (host_index, peer_index) = match (host_existing, peer_existing) {
                (Some(host_index), Some(peer_index)) => (host_index, peer_index),
                (Some(host_index), None) => {
                    warn!(
                        target: "network",
                        network = %network_id,
                        host_if = %host_ifname,
                        "host access veth peer missing; recreating veth pair"
                    );
                    handle
                        .link()
                        .del(host_index)
                        .execute()
                        .await
                        .with_context(|| {
                            format!("delete orphaned host access interface {host_ifname}")
                        })?;
                    self.create_host_access_veth(handle, &host_ifname, &peer_ifname)
                        .await?;
                    let host_index = self
                        .find_link(&host_ifname)
                        .await?
                        .context("host access interface missing after recreation")?;
                    let peer_index = self
                        .find_link(&peer_ifname)
                        .await?
                        .context("host access peer missing after recreation")?;
                    (host_index, peer_index)
                }
                (None, Some(peer_index)) => {
                    warn!(
                        target: "network",
                        network = %network_id,
                        peer_if = %peer_ifname,
                        "host access veth host missing; recreating veth pair"
                    );
                    handle
                        .link()
                        .del(peer_index)
                        .execute()
                        .await
                        .with_context(|| {
                            format!("delete orphaned host access peer interface {peer_ifname}")
                        })?;
                    self.create_host_access_veth(handle, &host_ifname, &peer_ifname)
                        .await?;
                    let host_index = self
                        .find_link(&host_ifname)
                        .await?
                        .context("host access interface missing after recreation")?;
                    let peer_index = self
                        .find_link(&peer_ifname)
                        .await?
                        .context("host access peer missing after recreation")?;
                    (host_index, peer_index)
                }
                (None, None) => {
                    self.create_host_access_veth(handle, &host_ifname, &peer_ifname)
                        .await?;
                    let host_index = self
                        .find_link(&host_ifname)
                        .await?
                        .context("host access interface missing after creation")?;
                    let peer_index = self
                        .find_link(&peer_ifname)
                        .await?
                        .context("host access peer missing after creation")?;
                    (host_index, peer_index)
                }
            };

            if let Some(mac) = host_mac {
                self.ensure_link_mac(host_index, mac, &host_ifname)
                    .await
                    .with_context(|| {
                        format!(
                            "ensure host access mac {} on {} (idx {})",
                            format_mac(mac),
                            host_ifname,
                            host_index
                        )
                    })?;
            }

            self.attach_master(peer_index, bridge_index)
                .await
                .with_context(|| {
                    format!(
                        "attach host access peer {} (idx {}) to bridge (idx {})",
                        peer_ifname, peer_index, bridge_index
                    )
                })?;

            self.configure_bridge_hairpin(peer_index, &peer_ifname)
                .await
                .with_context(|| {
                    format!(
                        "enable hairpin mode on host access peer {} (idx {})",
                        peer_ifname, peer_index
                    )
                })?;

            Ok((host_index, peer_index))
        }

        /// Create the host access veth pair that connects the host namespace to the overlay bridge.
        async fn create_host_access_veth(
            &self,
            handle: &Handle,
            host_ifname: &str,
            peer_ifname: &str,
        ) -> Result<()> {
            handle
                .link()
                .add(LinkVeth::new(host_ifname, peer_ifname).build())
                .execute()
                .await
                .with_context(|| {
                    format!("create host access veth {host_ifname}<->{peer_ifname}")
                })?;
            Ok(())
        }

        /// Ensure the VXLAN bridge port is attached, configured, and usable for dataplane traffic.
        ///
        /// Normal reconciles reuse an existing `mvx-*` device when possible, but stale local
        /// cleanup can occasionally leave a link object that still resolves by name while
        /// rejecting bridge attach or MTU programming. When that happens, recreate the VXLAN
        /// device once and reapply the bridge-port wiring so the rest of the network reconcile
        /// can proceed deterministically.
        async fn ensure_vxlan_bridge_port(
            &self,
            plan: &NetworkPlan,
            bridge_index: u32,
        ) -> Result<u32> {
            let handle = self
                .handle()
                .ok_or_else(|| anyhow!("rtnetlink handle unavailable"))?;
            let mut vxlan_index = self
                .ensure_vxlan(plan)
                .await
                .with_context(|| format!("ensure vxlan interface {}", plan.vxlan_name))?;

            for attempt in 0..=1 {
                match self.attach_master(vxlan_index, bridge_index).await {
                    Ok(()) => {}
                    Err(err) if attempt == 0 => {
                        warn!(
                            target: "network",
                            vxlan = %plan.vxlan_name,
                            vxlan_index,
                            bridge = %plan.bridge_name,
                            bridge_index,
                            error = %err,
                            "vxlan bridge attach hit stale local state; recreating link once"
                        );
                        self.delete_link_and_wait_absent(handle, vxlan_index, &plan.vxlan_name)
                            .await
                            .with_context(|| {
                                format!(
                                    "delete stale vxlan {} (idx {}) after bridge attach failure",
                                    plan.vxlan_name, vxlan_index
                                )
                            })?;
                        vxlan_index = self.ensure_vxlan(plan).await.with_context(|| {
                            format!(
                                "recreate vxlan interface {} after bridge attach failure",
                                plan.vxlan_name
                            )
                        })?;
                        continue;
                    }
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "attach vxlan {} (idx {}) to bridge {} (idx {})",
                                plan.vxlan_name, vxlan_index, plan.bridge_name, bridge_index
                            )
                        });
                    }
                }

                self.configure_bridge_port(vxlan_index, bridge_index, &plan.vxlan_name)
                    .await
                    .with_context(|| {
                        format!(
                            "configure bridge port for vxlan {} (idx {}) on bridge {} (idx {})",
                            plan.vxlan_name, vxlan_index, plan.bridge_name, bridge_index
                        )
                    })?;

                self.set_up(vxlan_index).await.with_context(|| {
                    format!("bring link {} (idx {}) up", plan.vxlan_name, vxlan_index)
                })?;

                if plan.mtu == 0 {
                    return Ok(vxlan_index);
                }

                match self.set_mtu(vxlan_index, plan.mtu).await {
                    Ok(()) => return Ok(vxlan_index),
                    Err(err) if attempt == 0 => {
                        warn!(
                            target: "network",
                            vxlan = %plan.vxlan_name,
                            vxlan_index,
                            mtu = plan.mtu,
                            error = %err,
                            "vxlan mtu update hit stale local state; recreating link once"
                        );
                        self.delete_link_and_wait_absent(handle, vxlan_index, &plan.vxlan_name)
                            .await
                            .with_context(|| {
                                format!(
                                    "delete stale vxlan {} (idx {}) after mtu failure",
                                    plan.vxlan_name, vxlan_index
                                )
                            })?;
                        vxlan_index = self.ensure_vxlan(plan).await.with_context(|| {
                            format!(
                                "recreate vxlan interface {} after mtu failure",
                                plan.vxlan_name
                            )
                        })?;
                    }
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "set mtu {} on vxlan {} (idx {})",
                                plan.mtu, plan.vxlan_name, vxlan_index
                            )
                        });
                    }
                }
            }

            Err(anyhow!(
                "vxlan {} did not reach a usable state after one recreate attempt",
                plan.vxlan_name
            ))
        }

        /// Ensure bridge, VXLAN, host-access veth, addresses, and MTUs match one network plan.
        pub async fn ensure_network(&self, plan: &NetworkPlan) -> Result<()> {
            if self.handle.is_none() {
                debug!(
                    target: "network",
                    network = %plan.network_id,
                    vxlan = %plan.vxlan_name,
                    bridge = %plan.bridge_name,
                    "skipping network provisioning; rtnetlink unavailable"
                );
                return Ok(());
            }

            debug!(
                target: "network",
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                driver = ?plan.driver,
                vni = plan.vni,
                mtu = plan.mtu,
                "provisioner: ensuring kernel interfaces"
            );
            let bridge_index = self
                .ensure_bridge(plan)
                .await
                .with_context(|| format!("ensure bridge {}", plan.bridge_name))?;
            debug!(
                target: "network",
                bridge = %plan.bridge_name,
                bridge_index,
                "provisioner: bridge interface ready"
            );

            if plan.uses_vxlan() {
                let vxlan_index = self
                    .ensure_vxlan_bridge_port(plan, bridge_index)
                    .await
                    .with_context(|| {
                        format!(
                            "ensure vxlan {} is attached and configured on bridge {}",
                            plan.vxlan_name, plan.bridge_name
                        )
                    })?;
                debug!(
                    target: "network",
                    vxlan = %plan.vxlan_name,
                    vxlan_index,
                    "provisioner: vxlan interface ready"
                );
            }

            let host_access = if plan.resolver_ip.is_some() && plan.subnet_prefix.is_some() {
                Some(
                    self.ensure_host_access_veth(
                        plan.network_id,
                        bridge_index,
                        plan.host_access_mac,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "ensure host access veth for network {} on bridge {} (idx {})",
                            plan.network_id, plan.bridge_name, bridge_index
                        )
                    })?,
                )
            } else {
                None
            };

            self.set_up(bridge_index).await.with_context(|| {
                format!("bring link {} (idx {}) up", plan.bridge_name, bridge_index)
            })?;
            if let Some((host_index, peer_index)) = host_access {
                let (host_ifname, peer_ifname) = Self::host_access_ifnames(plan.network_id);
                self.set_up(peer_index).await.with_context(|| {
                    format!("bring link {} (idx {}) up", peer_ifname, peer_index)
                })?;
                self.set_up(host_index).await.with_context(|| {
                    format!("bring link {} (idx {}) up", host_ifname, host_index)
                })?;
            }

            if plan.mtu > 0 {
                self.set_mtu(bridge_index, plan.mtu)
                    .await
                    .with_context(|| {
                        format!(
                            "set mtu {} on bridge {} (idx {})",
                            plan.mtu, plan.bridge_name, bridge_index
                        )
                    })?;
                if let Some((host_index, peer_index)) = host_access {
                    let (host_ifname, peer_ifname) = Self::host_access_ifnames(plan.network_id);
                    self.set_mtu(peer_index, plan.mtu).await.with_context(|| {
                        format!(
                            "set mtu {} on host access peer {} (idx {})",
                            plan.mtu, peer_ifname, peer_index
                        )
                    })?;
                    self.set_mtu(host_index, plan.mtu).await.with_context(|| {
                        format!(
                            "set mtu {} on host access link {} (idx {})",
                            plan.mtu, host_ifname, host_index
                        )
                    })?;
                }
            }

            if let (Some(ip), Some(prefix)) = (plan.resolver_ip, plan.subnet_prefix) {
                let Some((host_index, _peer_index)) = host_access else {
                    return Err(anyhow!(
                        "host access veth missing despite resolver address being configured"
                    ));
                };
                let (host_ifname, _peer_ifname) = Self::host_access_ifnames(plan.network_id);
                for iface in [&plan.bridge_name, &host_ifname] {
                    if let Err(err) = self.configure_address_ownership_tuning(iface, ip) {
                        debug!(
                            target: "network",
                            iface,
                            ip = %ip,
                            "failed to apply address ownership tuning (continuing): {err:#}"
                        );
                    }
                }

                // Older deployments assigned the resolver address to the bridge device. That makes
                // host-originated overlay traffic (including VIP flows) bypass tc-ingress and
                // therefore miss VIP neighbor handling plus DNAT. Move the IP to the host-access
                // veth so locally originated traffic enters through the bridge port path.
                self.remove_interface_address(bridge_index, ip, prefix, &plan.bridge_name)
                    .await
                    .with_context(|| {
                        format!(
                            "remove resolver address {ip}/{prefix} from bridge {} (idx {})",
                            plan.bridge_name, bridge_index
                        )
                    })?;

                self.remove_stale_interface_addresses(host_index, ip, prefix, &host_ifname)
                    .await
                    .with_context(|| {
                        format!(
                            "remove stale resolver addresses from host access {} (idx {})",
                            host_ifname, host_index
                        )
                    })?;

                self.ensure_interface_address(host_index, ip, prefix, &host_ifname)
                    .await
                    .with_context(|| {
                        format!(
                            "assign resolver address {ip}/{prefix} to host access {} (idx {})",
                            host_ifname, host_index
                        )
                    })?;
                if let Some(mac) = plan.host_access_mac
                    && let Err(err) = self
                        .announce_host_access_ip(host_index, ip, mac, &host_ifname)
                        .await
                {
                    debug!(
                        target: "network",
                        network = %plan.network_id,
                        iface = %host_ifname,
                        ip = %ip,
                        "failed to announce host access ip (continuing): {err:#}"
                    );
                }
            }

            debug!(
                target: "network",
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                driver = ?plan.driver,
                "provisioner: kernel interfaces ensured"
            );
            Ok(())
        }

        /// Ensure an interface owns the resolver address for the network, replacing stale state.
        ///
        /// Mantissa uses this to place the per-network resolver address on the interface that
        /// should own the connected route for the overlay subnet.
        async fn ensure_interface_address(
            &self,
            link_index: u32,
            ip: IpAddr,
            prefix: u8,
            link_name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };
            handle
                .address()
                .add(link_index, ip, prefix)
                .replace()
                .execute()
                .await
                .with_context(|| format!("assign resolver {ip}/{prefix} on {link_name}"))
        }

        /// Remove stale addresses from a dedicated host-access interface while preserving the active one.
        ///
        /// Split/merge transitions can move a network onto a new resolver address while reusing
        /// the same `mnhost-*` link name. We must delete old addresses first so the kernel does
        /// not keep multiple connected prefixes on the interface and pick an unexpected source IP
        /// for overlay traffic and health probes.
        async fn remove_stale_interface_addresses(
            &self,
            link_index: u32,
            keep_ip: IpAddr,
            keep_prefix: u8,
            link_name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };
            let family = address_family(keep_ip);

            let mut stream = handle
                .address()
                .get()
                .set_link_index_filter(link_index)
                .execute();

            while let Some(message) = stream
                .try_next()
                .await
                .context("list interface addresses")?
            {
                if message.header.family != family {
                    continue;
                }

                let mut address: Option<IpAddr> = None;
                for attr in message.attributes.iter() {
                    match attr {
                        AddressAttribute::Address(ip) | AddressAttribute::Local(ip) => {
                            address = Some(*ip);
                            break;
                        }
                        _ => {}
                    }
                }

                let Some(ip) = address else {
                    continue;
                };
                let prefix = message.header.prefix_len;
                if ip == keep_ip && prefix == keep_prefix {
                    continue;
                }

                self.remove_interface_address(link_index, ip, prefix, link_name)
                    .await
                    .with_context(|| {
                        format!("remove stale resolver {ip}/{prefix} from {link_name}")
                    })?;
            }

            Ok(())
        }

        /// Apply per-family address ownership tuning on interfaces that can own overlay resolver IPs.
        ///
        /// IPv4 needs ARP flux mitigation because the bridge and host-access links share one L2
        /// domain. IPv6 does not use ARP, but freshly assigned resolver addresses still start in
        /// the kernel's duplicate-address-detection state. Disable DAD on these synthetic
        /// host-access addresses so the embedded DNS listener can bind immediately instead of
        /// racing the tentative window on every network reconciliation.
        fn configure_address_ownership_tuning(&self, iface: &str, ip: IpAddr) -> Result<()> {
            if matches!(ip, IpAddr::V6(_)) {
                Self::write_sysctl_value(
                    &format!("/proc/sys/net/ipv6/conf/{iface}/accept_dad"),
                    "0",
                )?;
                return Ok(());
            }
            Self::write_sysctl_value(&format!("/proc/sys/net/ipv4/conf/{iface}/arp_ignore"), "1")?;
            Self::write_sysctl_value(
                &format!("/proc/sys/net/ipv4/conf/{iface}/arp_announce"),
                "2",
            )?;
            Ok(())
        }

        /// Write a sysctl value via /proc so interface-specific network tuning can be set.
        fn write_sysctl_value(path: &str, value: &str) -> Result<()> {
            fs::write(path, value).with_context(|| format!("write sysctl {path}"))
        }

        /// Broadcast an announcement for the host-access IP so peers refresh stale neighbor entries.
        ///
        /// The current implementation emits gratuitous ARP for IPv4. IPv6 neighbors converge via
        /// normal discovery and permanent host-side entries in the first phase of IPv6 support.
        async fn announce_host_access_ip(
            &self,
            host_index: u32,
            ip: IpAddr,
            mac: [u8; 6],
            link_name: &str,
        ) -> Result<()> {
            let IpAddr::V4(ip) = ip else {
                let _ = (host_index, mac, link_name);
                return Ok(());
            };
            let frame = Self::build_arp_announcement_frame(mac, ip)
                .with_context(|| format!("build arp announcement for {link_name}"))?;

            let fd = unsafe {
                libc::socket(
                    libc::AF_PACKET,
                    libc::SOCK_RAW,
                    (libc::ETH_P_ARP as u16).to_be() as i32,
                )
            };
            if fd < 0 {
                return Err(std::io::Error::last_os_error())
                    .with_context(|| format!("open raw socket for {link_name}"));
            }

            let mut addr: libc::sockaddr_ll = unsafe { mem::zeroed() };
            addr.sll_family = libc::AF_PACKET as u16;
            addr.sll_protocol = (libc::ETH_P_ARP as u16).to_be();
            addr.sll_ifindex = host_index as i32;
            addr.sll_halen = 6;
            addr.sll_addr[..6].copy_from_slice(&[0xff; 6]);

            let sent = unsafe {
                libc::sendto(
                    fd,
                    frame.as_ptr().cast::<libc::c_void>(),
                    frame.len(),
                    0,
                    &addr as *const _ as *const libc::sockaddr,
                    mem::size_of::<libc::sockaddr_ll>() as u32,
                )
            };

            let close_result = unsafe { libc::close(fd) };

            if sent < 0 {
                return Err(std::io::Error::last_os_error())
                    .with_context(|| format!("send arp announcement on {link_name}"));
            }
            if close_result < 0 {
                return Err(std::io::Error::last_os_error())
                    .with_context(|| format!("close raw socket for {link_name}"));
            }

            Ok(())
        }

        /// Builds a broadcast ARP announcement frame for the provided IPv4 address.
        ///
        /// This packet advertises the host-access IP with the correct MAC so peers update
        /// their neighbor caches immediately after a reschedule.
        fn build_arp_announcement_frame(mac: [u8; 6], ip: Ipv4Addr) -> Result<Vec<u8>> {
            let broadcast = [0xffu8; 6];
            let arp = ArpPacket::new(
                ArpHardwareId::ETHERNET,
                EtherType::IPV4,
                ArpOperation::REQUEST,
                &mac,
                &ip.octets(),
                &[0u8; 6],
                &ip.octets(),
            )?;

            let builder = PacketBuilder::ethernet2(mac, broadcast).arp(arp);
            let mut frame = Vec::with_capacity(builder.size());
            builder.write(&mut frame)?;
            Ok(frame)
        }

        /// Remove the specified address from the provided link if present.
        ///
        /// This enables safe migrations where the resolver IP used to live on the bridge device
        /// but now should move onto the host-access veth so host traffic hits tc-ingress programs.
        async fn remove_interface_address(
            &self,
            link_index: u32,
            ip: IpAddr,
            prefix: u8,
            link_name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let msg = match ip {
                IpAddr::V4(ip) => AddressMessageBuilder::<Ipv4Addr>::new()
                    .index(link_index)
                    .address(ip, prefix)
                    .build(),
                IpAddr::V6(ip) => AddressMessageBuilder::<Ipv6Addr>::new()
                    .index(link_index)
                    .address(ip, prefix)
                    .build(),
            };
            match handle.address().del(msg).execute().await {
                Ok(()) => Ok(()),
                Err(RtnetlinkError::NetlinkError(msg)) => {
                    let raw = msg.raw_code();
                    let errno = raw.abs();
                    if errno == libc::ENOENT || errno == libc::EADDRNOTAVAIL {
                        debug!(
                            target: "network",
                            link = link_name,
                            ip = %ip,
                            prefix,
                            errno,
                            raw_code = raw,
                            "address already absent while removing; ignoring"
                        );
                        Ok(())
                    } else {
                        Err(RtnetlinkError::NetlinkError(msg)).with_context(|| {
                            format!("remove resolver {ip}/{prefix} from {link_name}")
                        })
                    }
                }
                Err(err) => Err(err)
                    .with_context(|| format!("remove resolver {ip}/{prefix} from {link_name}")),
            }
        }

        /// Delete the local kernel interfaces that implement one overlay network.
        pub async fn teardown_network(&self, plan: &NetworkPlan) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let (host_ifname, _peer_ifname) = Self::host_access_ifnames(plan.network_id);
            if let Some(index) = self.find_link(&host_ifname).await? {
                handle
                    .link()
                    .del(index)
                    .execute()
                    .await
                    .with_context(|| format!("delete host access {}", host_ifname))?;
            }

            if let Some(index) = self.find_link(&plan.vxlan_name).await? {
                handle
                    .link()
                    .del(index)
                    .execute()
                    .await
                    .with_context(|| format!("delete vxlan {}", plan.vxlan_name))?;
            }

            if let Some(index) = self.find_link(&plan.bridge_name).await? {
                handle
                    .link()
                    .del(index)
                    .execute()
                    .await
                    .with_context(|| format!("delete bridge {}", plan.bridge_name))?;
            }

            Ok(())
        }

        /// Create or validate the deterministic VXLAN interface for one network plan.
        async fn ensure_vxlan(&self, plan: &NetworkPlan) -> Result<u32> {
            let handle = self
                .handle()
                .ok_or_else(|| anyhow!("rtnetlink handle unavailable"))?;

            if let Some(index) = self.find_link(&plan.vxlan_name).await? {
                let mut recreate = false;

                if let Some(forced_underlay) = plan.underlay_iface.as_deref() {
                    match self.find_link(forced_underlay).await? {
                        Some(forced_index) => {
                            let current = self.link_lower_index(index).await?;
                            if current != Some(forced_index) {
                                warn!(
                                    target: "network",
                                    vxlan = %plan.vxlan_name,
                                    vxlan_index = index,
                                    current_underlay = ?current,
                                    desired_underlay = forced_underlay,
                                    desired_underlay_index = forced_index,
                                    "vxlan underlay changed; recreating interface"
                                );
                                recreate = true;
                            }
                        }
                        None => {
                            warn!(
                                target: "network",
                                vxlan = %plan.vxlan_name,
                                vxlan_index = index,
                                desired_underlay = forced_underlay,
                                "requested vxlan underlay interface missing; reusing existing vxlan"
                            );
                        }
                    }
                } else if let Some(wg_index) = self.find_link(MANTISSA_WIREGUARD_IFNAME).await? {
                    let current = self.link_lower_index(index).await?;
                    if current == Some(wg_index) {
                        warn!(
                            target: "network",
                            vxlan = %plan.vxlan_name,
                            vxlan_index = index,
                            "wireguard underlay no longer requested; recreating vxlan on detected underlay"
                        );
                        recreate = true;
                    }
                }

                if recreate {
                    self.delete_link_and_wait_absent(handle, index, &plan.vxlan_name)
                        .await
                        .with_context(|| {
                            format!("delete vxlan {} (idx {})", plan.vxlan_name, index)
                        })?;
                } else {
                    if let Err(err) = self.configure_existing_vxlan(index, &plan.vxlan_name).await {
                        warn!(
                            target: "network",
                            vxlan = %plan.vxlan_name,
                            error = %err,
                            "failed to update vxlan configuration while reusing interface"
                        );
                    }
                    debug!(
                        target: "network",
                        vxlan = %plan.vxlan_name,
                        vxlan_index = index,
                        "provisioner: reusing existing vxlan interface"
                    );
                    return Ok(index);
                }
            }

            let mut last_error: Option<anyhow::Error> = None;

            for attempt in 0..=1 {
                let resolved_underlay = if let (Some(ifname), Some(ip)) =
                    (plan.underlay_iface.as_deref(), plan.underlay_ip)
                {
                    match self.explicit_underlay_by_name(ifname, ip).await {
                        Ok(info) => info,
                        Err(err) => {
                            warn!(
                                target: "network",
                                attempt,
                                underlay = ifname,
                                error = %err,
                                "requested explicit underlay is unavailable; refreshing detected underlay"
                            );
                            let mut guard = self.underlay.lock().await;
                            *guard = None;
                            drop(guard);
                            self.underlay_info()
                                .await
                                .context("resolve detected underlay for vxlan")?
                        }
                    }
                } else {
                    self.underlay_info()
                        .await
                        .context("resolve underlay interface for vxlan")?
                };

                let underlay_index = resolved_underlay.index;
                let underlay_ip = resolved_underlay.ip;
                let underlay_name = resolved_underlay.name;

                info!(
                    target: "network",
                    attempt,
                    "creating vxlan {} (vni {}) on underlay {} (index {}, ip {})",
                    plan.vxlan_name,
                    plan.vni,
                    underlay_name,
                    underlay_index,
                    underlay_ip
                );

                let builder = {
                    let base = LinkVxlan::new(&plan.vxlan_name, plan.vni)
                        .dev(underlay_index)
                        .learning(false)
                        .proxy(false)
                        .rsc(true)
                        .l2miss(false)
                        .l3miss(false)
                        .port(VXLAN_PORT)
                        .link(underlay_index);
                    match underlay_ip {
                        IpAddr::V4(ip) => base.local(ip),
                        IpAddr::V6(ip) => base.local6(ip),
                    }
                };

                match handle.link().add(builder.build()).execute().await {
                    Ok(()) => {
                        let index = self
                            .find_link(&plan.vxlan_name)
                            .await?
                            .context("vxlan interface missing after creation")?;
                        debug!(
                            target: "network",
                            attempt,
                            vxlan = %plan.vxlan_name,
                            index,
                            underlay = underlay_name,
                            underlay_index,
                            "vxlan interface provisioned"
                        );
                        if let Err(err) =
                            self.configure_existing_vxlan(index, &plan.vxlan_name).await
                        {
                            warn!(
                                target: "network",
                                vxlan = %plan.vxlan_name,
                                error = %err,
                                "failed to apply vxlan configuration after creation"
                            );
                        }
                        return Ok(index);
                    }
                    Err(err) => {
                        let (raw_code, errno) = match &err {
                            RtnetlinkError::NetlinkError(msg) => {
                                let raw = msg.raw_code();
                                (raw, raw.abs())
                            }
                            _ => (0, 0),
                        };
                        let errno_name = if errno != 0 {
                            std::io::Error::from_raw_os_error(errno).to_string()
                        } else {
                            "unknown".into()
                        };

                        let inventory = match self.collect_link_inventory().await {
                            Ok(entries) if !entries.is_empty() => entries.join("; "),
                            Ok(_) => "<no interfaces enumerated>".into(),
                            Err(inv_err) => format!("failed to enumerate interfaces: {inv_err:#}"),
                        };

                        let mut message = format!(
                            "failed to create vxlan {} (vni {}) on underlay {} (idx {}, ip {}): kernel returned {} ({errno_name}); available links [{}]",
                            plan.vxlan_name,
                            plan.vni,
                            underlay_name,
                            underlay_index,
                            underlay_ip,
                            errno,
                            inventory
                        );
                        if raw_code != errno {
                            message.push_str(&format!(" raw_code={raw_code}"));
                        }

                        warn!(
                            target: "network",
                            attempt,
                            vxlan = %plan.vxlan_name,
                            vni = plan.vni,
                            underlay = %underlay_name,
                            underlay_index,
                            errno,
                            errno_name = %errno_name,
                            raw_code,
                            available_links = %inventory,
                            error = %err,
                            message = %message
                        );

                        if attempt == 0 && errno == libc::ENODEV {
                            warn!(
                                target: "network",
                                attempt,
                                underlay = %underlay_name,
                                underlay_index,
                                "vxlan creation returned ENODEV; refreshing underlay cache and retrying"
                            );
                            let mut guard = self.underlay.lock().await;
                            *guard = None;
                            last_error = Some(anyhow!(message));
                            continue;
                        }

                        return Err(anyhow!(message));
                    }
                }
            }

            Err(last_error.unwrap_or_else(|| anyhow!("vxlan creation failed after retries")))
        }

        /// Reapply VXLAN runtime flags on a reused interface so stale settings do not linger.
        async fn configure_existing_vxlan(&self, index: u32, name: &str) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let request = LinkMessageBuilder::<LinkVxlan>::new_with_info_kind(InfoKind::Vxlan)
                .index(index)
                .learning(false)
                .proxy(false)
                .rsc(true)
                .l2miss(false)
                .l3miss(false)
                .build();

            handle
                .link()
                .set(request)
                .execute()
                .await
                .with_context(|| format!("configure vxlan {} (idx {})", name, index))
        }

        /// Configure VXLAN bridge-port attributes used by overlay forwarding.
        async fn configure_bridge_port(
            &self,
            vxlan_index: u32,
            _bridge_index: u32,
            name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            // Encode the bridge proto info attributes manually so we can set the
            // NLA_F_NESTED flag on IFLA_PROTINFO. The kernel rejects the update
            // if the payload is not marked nested.
            let payload = {
                let proto_attrs = [
                    // Disable hairpin on the VXLAN port to avoid BUM traffic looping back into
                    // the overlay; we enable hairpin selectively on host-access veths instead.
                    LinkProtoInfoBridge::Other(DefaultNla::new(BRPORT_ATTR_HAIRPIN_MODE, vec![0])),
                    LinkProtoInfoBridge::Other(DefaultNla::new(BRPORT_ATTR_LEARNING, vec![0])),
                    LinkProtoInfoBridge::Other(DefaultNla::new(
                        BRPORT_ATTR_NEIGH_SUPPRESS,
                        vec![0],
                    )),
                    LinkProtoInfoBridge::Other(DefaultNla::new(BRPORT_ATTR_UNICAST_FLOOD, vec![1])),
                    LinkProtoInfoBridge::Other(DefaultNla::new(
                        BRPORT_ATTR_MULTICAST_FLOOD,
                        vec![1],
                    )),
                    LinkProtoInfoBridge::Other(DefaultNla::new(
                        BRPORT_ATTR_BROADCAST_FLOOD,
                        vec![1],
                    )),
                ];

                let mut buf: Vec<u8> = Vec::with_capacity(64);
                for attr in &proto_attrs {
                    let value_len = attr.value_len();
                    let attr_len = (NLA_HEADER_SIZE + value_len) as u16;
                    let align = NLA_ALIGNTO;
                    let aligned_len = ((attr_len as usize) + align - 1) & !(align - 1);
                    let start = buf.len();
                    buf.resize(start + aligned_len, 0);
                    {
                        let mut nla_buf = NlaBuffer::new(&mut buf[start..start + aligned_len]);
                        nla_buf.set_kind(attr.kind());
                        nla_buf.set_length(attr_len);
                        attr.emit_value(nla_buf.value_mut());
                    }
                }
                buf
            };

            let request = LinkMessageBuilder::<LinkUnspec>::default()
                .set_header(LinkHeader {
                    interface_family: AddressFamily::Bridge,
                    index: vxlan_index,
                    ..Default::default()
                })
                .name(name.to_string())
                .append_extra_attribute(LinkAttribute::Other(DefaultNla::new(
                    IFLA_PROTINFO | NLA_F_NESTED,
                    payload,
                )))
                .build();

            handle
                .link()
                .set(request)
                .execute()
                .await
                .with_context(|| {
                    format!(
                        "configure bridge port attributes for vxlan {} (idx {})",
                        name, vxlan_index
                    )
                })
                .map(|_| ())?;

            if let Err(err) = self.log_bridge_port_state(vxlan_index, name).await {
                debug!(
                    target: "network",
                    vxlan = %name,
                    error = %err,
                    "[bridge-config] failed to inspect bridge port after applying settings"
                );
            }

            Ok(())
        }

        /// Enable hairpin mode on a bridge port so frames may egress back out the ingress port.
        ///
        /// Mantissa's VIP ARP responder synthesizes replies by rewriting inbound ARP requests on
        /// tc-ingress. Hairpin mode is required so those replies can be sent back to the original
        /// ingress port (containers, vxlan, or the host access veth peer).
        async fn configure_bridge_hairpin(&self, port_index: u32, name: &str) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let payload = {
                let proto_attrs = [LinkProtoInfoBridge::Other(DefaultNla::new(
                    BRPORT_ATTR_HAIRPIN_MODE,
                    vec![1],
                ))];

                let mut buf: Vec<u8> = Vec::with_capacity(32);
                for attr in &proto_attrs {
                    let value_len = attr.value_len();
                    let attr_len = (NLA_HEADER_SIZE + value_len) as u16;
                    let align = NLA_ALIGNTO;
                    let aligned_len = ((attr_len as usize) + align - 1) & !(align - 1);
                    let start = buf.len();
                    buf.resize(start + aligned_len, 0);
                    {
                        let mut nla_buf = NlaBuffer::new(&mut buf[start..start + aligned_len]);
                        nla_buf.set_kind(attr.kind());
                        nla_buf.set_length(attr_len);
                        attr.emit_value(nla_buf.value_mut());
                    }
                }
                buf
            };

            let request = LinkMessageBuilder::<LinkUnspec>::default()
                .set_header(LinkHeader {
                    interface_family: AddressFamily::Bridge,
                    index: port_index,
                    ..Default::default()
                })
                .name(name.to_string())
                .append_extra_attribute(LinkAttribute::Other(DefaultNla::new(
                    IFLA_PROTINFO | NLA_F_NESTED,
                    payload,
                )))
                .build();

            handle.link().set(request).execute().await.with_context(|| {
                format!("enable hairpin mode on bridge port {name} (idx {port_index})")
            })
        }

        /// Log the effective bridge-port attributes after configuration for diagnostics.
        async fn log_bridge_port_state(&self, index: u32, name: &str) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let mut stream = handle.link().get().match_index(index).execute();
            while let Some(msg) = stream.try_next().await? {
                let mut hairpin = None;
                let mut learning = None;
                let mut neigh_suppress = None;
                let mut unicast_flood = None;
                let mut multicast_flood = None;
                let mut broadcast_flood = None;

                for attr in &msg.attributes {
                    if let LinkAttribute::LinkInfo(infos) = attr {
                        for info in infos {
                            if let LinkInfo::PortData(InfoPortData::BridgePort(entries)) = info {
                                for entry in entries {
                                    match entry {
                                        InfoBridgePort::HairpinMode(value) => {
                                            hairpin = Some(*value)
                                        }
                                        InfoBridgePort::Learning(value) => learning = Some(*value),
                                        InfoBridgePort::NeighSupress(value) => {
                                            neigh_suppress = Some(*value)
                                        }
                                        InfoBridgePort::UnicastFlood(value) => {
                                            unicast_flood = Some(*value)
                                        }
                                        InfoBridgePort::MulticastFlood(value) => {
                                            multicast_flood = Some(*value)
                                        }
                                        InfoBridgePort::BroadcastFlood(value) => {
                                            broadcast_flood = Some(*value)
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }

                debug!(
                    target: "network",
                    vxlan = %name,
                    hairpin = ?hairpin,
                    learning = ?learning,
                    neigh_suppress = ?neigh_suppress,
                    unicast_flood = ?unicast_flood,
                    multicast_flood = ?multicast_flood,
                    broadcast_flood = ?broadcast_flood,
                    "[bridge-config] bridge port state after configuration"
                );
            }

            Ok(())
        }

        /// Create or reuse the deterministic Linux bridge for one overlay network.
        async fn ensure_bridge(&self, plan: &NetworkPlan) -> Result<u32> {
            if let Some(index) = self.find_link(&plan.bridge_name).await? {
                debug!(
                    target: "network",
                    bridge = %plan.bridge_name,
                    bridge_index = index,
                    "provisioner: reusing existing bridge"
                );
                return Ok(index);
            }

            debug!(
                target: "network",
                bridge = %plan.bridge_name,
                "provisioner: creating bridge"
            );

            let handle = self
                .handle()
                .ok_or_else(|| anyhow!("rtnetlink handle unavailable"))?;

            handle
                .link()
                .add(LinkBridge::new(&plan.bridge_name).build())
                .execute()
                .await
                .with_context(|| format!("create bridge {}", plan.bridge_name))?;

            let index = self
                .find_link(&plan.bridge_name)
                .await?
                .context("bridge interface missing after creation")?;
            Ok(index)
        }

        /// Bring one interface up by index and tolerate already-up state.
        async fn set_up(&self, index: u32) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let name = self
                .link_name(index)
                .await
                .context("resolve link name before bringing link up")?
                .unwrap_or_else(|| format!("ifindex{index}"));

            debug!(
                target: "network",
                link = %name,
                link_index = index,
                "provisioner: bringing link up"
            );

            handle
                .link()
                .set(LinkUnspec::new_with_index(index).up().build())
                .execute()
                .await
                .with_context(|| format!("bring link {name} (index {index}) up"))?;

            debug!(
                target: "network",
                link = %name,
                link_index = index,
                "provisioner: link is up"
            );
            Ok(())
        }

        /// Set one link MTU while tolerating already-converged kernel state.
        ///
        /// Network reconciles can revisit the same deterministic interface while the kernel is
        /// still finishing a previous MTU update. Re-reading the observed MTU before and after a
        /// netlink error keeps the helper idempotent and prevents transient kernel timing from
        /// being misclassified as a stale-link failure.
        async fn set_mtu(&self, index: u32, mtu: u32) -> Result<()> {
            if mtu == 0 {
                return Ok(());
            }

            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let name = self
                .link_name(index)
                .await
                .context("resolve link name before setting mtu")?
                .unwrap_or_else(|| format!("ifindex{index}"));
            let current_mtu = self.link_mtu(index).await?;
            if current_mtu == Some(mtu) {
                debug!(
                    target: "network",
                    link = %name,
                    link_index = index,
                    mtu,
                    "provisioner: link already has desired mtu"
                );
                return Ok(());
            }
            debug!(
                target: "network",
                link = %name,
                link_index = index,
                mtu,
                "provisioner: updating mtu"
            );
            for retry in 0..LINK_STATE_UPDATE_RETRIES {
                match handle
                    .link()
                    .set(LinkUnspec::new_with_index(index).mtu(mtu).build())
                    .execute()
                    .await
                {
                    Ok(()) => {
                        debug!(
                            target: "network",
                            link = %name,
                            link_index = index,
                            mtu,
                            "provisioner: mtu updated"
                        );
                        return Ok(());
                    }
                    Err(err) => {
                        let observed_mtu = self.wait_for_link_mtu(index, mtu).await?;
                        if observed_mtu == Some(mtu) {
                            debug!(
                                target: "network",
                                link = %name,
                                link_index = index,
                                mtu,
                                "provisioner: mtu update already converged after netlink error"
                            );
                            return Ok(());
                        }

                        if retry + 1 < LINK_STATE_UPDATE_RETRIES {
                            debug!(
                                target: "network",
                                link = %name,
                                link_index = index,
                                mtu,
                                retry,
                                observed_mtu = ?observed_mtu,
                                error = %err,
                                "provisioner: retrying mtu update after transient mismatch"
                            );
                            continue;
                        }

                        return Err(err).with_context(|| match (current_mtu, observed_mtu) {
                                (Some(current), Some(observed)) => format!(
                                    "set mtu {mtu} on link {name} (index {index}); current_mtu={current}; observed_mtu_after_error={observed}"
                                ),
                                (Some(current), None) => format!(
                                    "set mtu {mtu} on link {name} (index {index}); current_mtu={current}; observed_mtu_after_error=none"
                                ),
                                (None, Some(observed)) => format!(
                                    "set mtu {mtu} on link {name} (index {index}); observed_mtu_after_error={observed}"
                                ),
                                (None, None) => {
                                    format!("set mtu {mtu} on link {name} (index {index})")
                                }
                            });
                    }
                }
            }

            Ok(())
        }

        /// Wait briefly for one link to report the requested MTU after a netlink update.
        ///
        /// Some kernels acknowledge MTU changes later than the original rtnetlink request path.
        /// A short settle window lets the provisioner distinguish "the update converged anyway"
        /// from a real MTU programming failure without adding an unbounded retry loop.
        async fn wait_for_link_mtu(&self, index: u32, desired_mtu: u32) -> Result<Option<u32>> {
            let mut observed = self.link_mtu(index).await?;
            if observed == Some(desired_mtu) {
                return Ok(observed);
            }

            for _ in 0..LINK_STATE_SETTLE_ATTEMPTS {
                tokio::time::sleep(LINK_STATE_SETTLE_DELAY).await;
                observed = self.link_mtu(index).await?;
                if observed == Some(desired_mtu) {
                    return Ok(observed);
                }
            }

            Ok(observed)
        }

        /// Ensure a link advertises the requested MAC address for deterministic forwarding.
        ///
        /// This keeps the host-access interface stable across reconciles so peer FDB entries can
        /// target a consistent MAC and avoid unknown-unicast flooding.
        async fn ensure_link_mac(&self, index: u32, mac: [u8; 6], name: &str) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let current = self.link_address(index).await?;
            if current.as_deref() == Some(&mac[..]) {
                return Ok(());
            }

            debug!(
                target: "network",
                link = %name,
                link_index = index,
                desired = %format_mac(mac),
                "provisioner: updating link mac"
            );

            handle
                .link()
                .set(
                    LinkUnspec::new_with_index(index)
                        .address(mac.to_vec())
                        .build(),
                )
                .execute()
                .await
                .with_context(|| {
                    format!("set mac {} on link {name} (index {index})", format_mac(mac))
                })?;

            Ok(())
        }

        /// Attach one link to the requested bridge, treating already-converged kernel state as success.
        ///
        /// The network controller can reconcile a reused VXLAN or host-access port immediately
        /// after the kernel has already applied the same bridge enslave request. In that case the
        /// rtnetlink update can still report a transient busy error even though the link now
        /// belongs to the desired bridge. Re-reading the current controller after a failed update
        /// keeps the helper idempotent without hiding genuine attachment conflicts.
        async fn attach_master(&self, link_index: u32, master_index: u32) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let link_name = self
                .link_name(link_index)
                .await
                .context("resolve link name before attaching to bridge")?
                .unwrap_or_else(|| format!("ifindex{link_index}"));
            let master_name = self
                .link_name(master_index)
                .await
                .context("resolve bridge name before attaching interface")?
                .unwrap_or_else(|| format!("ifindex{master_index}"));
            let current_master = self.link_controller_index(link_index).await?;

            if current_master == Some(master_index) {
                debug!(
                    target: "network",
                    link = %link_name,
                    link_index,
                    bridge = %master_name,
                    bridge_index = master_index,
                    "provisioner: link already attached to bridge"
                );
                return Ok(());
            }

            debug!(
                target: "network",
                link = %link_name,
                link_index,
                bridge = %master_name,
                bridge_index = master_index,
                "provisioner: attaching link to bridge"
            );
            for retry in 0..LINK_STATE_UPDATE_RETRIES {
                match handle
                    .link()
                    .set(
                        LinkUnspec::new_with_index(link_index)
                            .controller(master_index)
                            .build(),
                    )
                    .execute()
                    .await
                {
                    Ok(()) => {
                        debug!(
                            target: "network",
                            link = %link_name,
                            link_index,
                            bridge = %master_name,
                            bridge_index = master_index,
                            "provisioner: link attached to bridge"
                        );
                        return Ok(());
                    }
                    Err(err) => {
                        let observed_master = self
                            .wait_for_link_controller(link_index, master_index)
                            .await?;
                        if observed_master == Some(master_index) {
                            debug!(
                                target: "network",
                                link = %link_name,
                                link_index,
                                bridge = %master_name,
                                bridge_index = master_index,
                                "provisioner: bridge attach already converged after netlink error"
                            );
                            return Ok(());
                        }

                        if retry + 1 < LINK_STATE_UPDATE_RETRIES {
                            debug!(
                                target: "network",
                                link = %link_name,
                                link_index,
                                bridge = %master_name,
                                bridge_index = master_index,
                                retry,
                                observed_master = ?observed_master,
                                error = %err,
                                "provisioner: retrying bridge attach after transient mismatch"
                            );
                            continue;
                        }

                        return Err(err).with_context(|| match (current_master, observed_master) {
                                (Some(current), Some(observed)) => format!(
                                    "attach link {link_name} (index {link_index}) to bridge {master_name} (index {master_index}); current_master={current}; observed_master_after_error={observed}"
                                ),
                                (Some(current), None) => format!(
                                    "attach link {link_name} (index {link_index}) to bridge {master_name} (index {master_index}); current_master={current}; observed_master_after_error=none"
                                ),
                                (None, Some(observed)) => format!(
                                    "attach link {link_name} (index {link_index}) to bridge {master_name} (index {master_index}); observed_master_after_error={observed}"
                                ),
                                (None, None) => format!(
                                    "attach link {link_name} (index {link_index}) to bridge {master_name} (index {master_index})"
                                ),
                            });
                    }
                }
            }

            Ok(())
        }

        /// Wait briefly for one link to report the requested bridge controller after attach.
        ///
        /// Bridge enslave requests can converge slightly after the original rtnetlink call
        /// returns. A short bounded wait avoids treating that convergence lag as a hard bridge
        /// attach failure while still surfacing persistent mismatches promptly.
        async fn wait_for_link_controller(
            &self,
            index: u32,
            desired_controller: u32,
        ) -> Result<Option<u32>> {
            let mut observed = self.link_controller_index(index).await?;
            if observed == Some(desired_controller) {
                return Ok(observed);
            }

            for _ in 0..LINK_STATE_SETTLE_ATTEMPTS {
                tokio::time::sleep(LINK_STATE_SETTLE_DELAY).await;
                observed = self.link_controller_index(index).await?;
                if observed == Some(desired_controller) {
                    return Ok(observed);
                }
            }

            Ok(observed)
        }

        /// Return the current bridge/controller link index for one interface when present.
        ///
        /// Bridge attachment is idempotent in the healthy case. Reading the current controller
        /// lets the provisioner skip a redundant bridge-enslave request and distinguish a link
        /// that is already attached to the desired bridge from one that is busy on stale state.
        async fn link_controller_index(&self, index: u32) -> Result<Option<u32>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut links = handle.link().get().match_index(index).execute();
            while let Some(link) = links.try_next().await? {
                for nla in link.attributes.into_iter() {
                    if let LinkAttribute::Controller(controller) = nla {
                        return Ok(Some(controller));
                    }
                }
            }
            Ok(None)
        }

        /// Return the current MTU for one link when the kernel still resolves the interface.
        ///
        /// MTU updates can converge slightly ahead of the corresponding rtnetlink response. The
        /// provisioner uses this snapshot to keep MTU programming idempotent and to surface the
        /// observed kernel state in any remaining error paths.
        async fn link_mtu(&self, index: u32) -> Result<Option<u32>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut links = handle.link().get().match_index(index).execute();
            while let Some(link) = links.try_next().await? {
                for nla in link.attributes.into_iter() {
                    if let LinkAttribute::Mtu(mtu) = nla {
                        return Ok(Some(mtu));
                    }
                }
            }
            Ok(None)
        }

        /// Resolve the kernel interface index for the provided link name.
        ///
        /// This is used by higher-level controllers to detect when interfaces have been recreated
        /// (e.g. underlay changes) so they can invalidate any cached forwarding state.
        pub async fn link_index(&self, name: &str) -> Result<Option<u32>> {
            self.find_link(name).await
        }

        /// Resolve one interface name to its current ifindex.
        async fn find_link(&self, name: &str) -> Result<Option<u32>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut stream = handle.link().get().match_name(name.to_string()).execute();

            match stream.try_next().await {
                Ok(Some(link)) => Ok(Some(link.header.index)),
                Ok(None) => Ok(None),
                Err(RtnetlinkError::NetlinkError(msg)) => {
                    let raw = msg.raw_code();
                    let errno = raw.abs();
                    if errno == libc::ENODEV || errno == libc::ENOENT {
                        debug!(
                            target: "network",
                            link = name,
                            errno,
                            raw_code = raw,
                            "link lookup returned ENODEV/ENOENT; treating as absent"
                        );
                        Ok(None)
                    } else {
                        Err(RtnetlinkError::NetlinkError(msg).into())
                    }
                }
                Err(err) => Err(err.into()),
            }
        }

        /// Resolve the underlay interface currently used for plaintext VXLAN creation.
        ///
        /// The provisioner caches the last good underlay choice, but it validates that cache on
        /// every use so transient Mantissa-managed interfaces or kernel index reuse cannot pin the
        /// controller to another overlay network's local plumbing.
        async fn underlay_info(&self) -> Result<ResolvedUnderlay> {
            let cached = {
                let guard = self.underlay.lock().await;
                guard.clone()
            };

            if let Some(info) = cached
                && let Some(validated) = self.validate_cached_underlay(&info).await?
            {
                debug!(
                    target: "network",
                    underlay = %validated.name,
                    underlay_index = validated.index,
                    underlay_ip = %validated.ip,
                    underlay_mtu = validated.mtu,
                    "provisioner: reusing cached underlay interface"
                );
                let mut guard = self.underlay.lock().await;
                *guard = Some(validated.clone());
                return Ok(validated);
            }

            let info = self.detect_underlay_info().await?;
            {
                let mut guard = self.underlay.lock().await;
                *guard = Some(info.clone());
            }
            info!(
                target: "network",
                underlay = %info.name,
                underlay_index = info.index,
                underlay_ip = %info.ip,
                underlay_mtu = info.mtu,
                "provisioner: detected underlay interface"
            );
            Ok(info)
        }

        /// Resolve the effective underlay interface for plaintext VXLAN traffic.
        ///
        /// Mantissa prefers the local interface that owns the configured advertise address so the
        /// overlay uses the same source IP the node publishes to peers. When no explicit local
        /// advertise address is available, the controller falls back to the kernel's default route
        /// and only then scans interfaces, always excluding Mantissa-managed overlay links.
        async fn detect_underlay_info(&self) -> Result<ResolvedUnderlay> {
            let advertise_ip = config::advertise_addr()
                .as_deref()
                .and_then(resolve_advertise_ip);

            if let Some(ip) = advertise_ip {
                if let Some(info) = self.local_underlay_for_ip(ip).await? {
                    info!(
                        target: "network",
                        underlay = %info.name,
                        underlay_index = info.index,
                        underlay_ip = %info.ip,
                        underlay_mtu = info.mtu,
                        "selected underlay from locally owned advertise address"
                    );
                    return Ok(info);
                }

                warn!(
                    target: "network",
                    advertise_ip = %ip,
                    "configured advertise address does not belong to a local usable interface; falling back to route-based underlay detection"
                );
            }

            for family in preferred_route_families(advertise_ip) {
                if let Some(info) = self.default_route_underlay(family).await? {
                    info!(
                        target: "network",
                        underlay = %info.name,
                        underlay_index = info.index,
                        underlay_ip = %info.ip,
                        underlay_mtu = info.mtu,
                        family = ?family,
                        "selected underlay from default route"
                    );
                    return Ok(info);
                }
            }

            self.scan_underlay_candidates().await
        }

        /// Resolve one explicit interface/IP pair into a complete underlay snapshot.
        ///
        /// This ensures the provisioner carries the lower-device MTU alongside the chosen source
        /// IP so later MTU clamping can be derived from one consistent kernel snapshot.
        async fn resolved_underlay(
            &self,
            index: u32,
            name: String,
            ip: IpAddr,
        ) -> Result<Option<ResolvedUnderlay>> {
            let mtu = self.link_mtu(index).await?.ok_or_else(|| {
                anyhow!("link {name} (index {index}) disappeared before its mtu could be read")
            })?;
            Ok(Some(ResolvedUnderlay {
                index,
                name,
                ip,
                mtu,
            }))
        }

        /// Validate one cached underlay choice against the current kernel link and address state.
        ///
        /// The cache remains valid only while the same interface still owns the same local source
        /// IP. Any name, index, or address drift forces a fresh underlay detection pass.
        async fn validate_cached_underlay(
            &self,
            cached: &ResolvedUnderlay,
        ) -> Result<Option<ResolvedUnderlay>> {
            let Some(info) = self.local_underlay_for_ip(cached.ip).await? else {
                return Ok(None);
            };
            if info.name != cached.name {
                return Ok(None);
            }
            Ok(Some(info))
        }

        /// Resolve the local usable interface that currently owns one specific IP address.
        ///
        /// This is the most deterministic underlay source because it ties VXLAN creation directly
        /// to the address the node advertises or otherwise expects peers to reach.
        async fn local_underlay_for_ip(&self, ip: IpAddr) -> Result<Option<ResolvedUnderlay>> {
            let Some(handle) = self.handle() else {
                return Err(anyhow!("rtnetlink handle unavailable"));
            };

            let family = address_family(ip);
            let mut links = handle.link().get().execute();
            while let Some(link) = links.try_next().await.context("enumerate link devices")? {
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
                if !flags.contains(LinkFlags::Up) {
                    continue;
                }
                if name == MANTISSA_WIREGUARD_IFNAME || is_managed_overlay_link_name(&name) {
                    continue;
                }
                if !flags.contains(LinkFlags::Loopback) && ip.is_loopback() {
                    continue;
                }
                if self.interface_has_ip(index, family, ip).await? {
                    return self.resolved_underlay(index, name, ip).await;
                }
            }
            Ok(None)
        }

        /// Return whether one interface currently owns the provided local address.
        ///
        /// Matching against the explicit address avoids guessing the source IP on interfaces that
        /// carry several addresses from the same family.
        async fn interface_has_ip(
            &self,
            index: u32,
            family: AddressFamily,
            desired_ip: IpAddr,
        ) -> Result<bool> {
            let Some(handle) = self.handle() else {
                return Ok(false);
            };

            let mut addresses = handle
                .address()
                .get()
                .set_link_index_filter(index)
                .execute();
            while let Some(message) = addresses.try_next().await.context("enumerate addresses")? {
                if message.header.family != family {
                    continue;
                }
                for attribute in message.attributes {
                    if let AddressAttribute::Address(ip) | AddressAttribute::Local(ip) = attribute
                        && ip == desired_ip
                    {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }

        /// Resolve the kernel-selected default-route interface for one IP family.
        ///
        /// Route-driven lookup is stable across unrelated local interfaces because it follows the
        /// same kernel output-interface decision the host would use for plain underlay traffic.
        async fn default_route_underlay(
            &self,
            family: AddressFamily,
        ) -> Result<Option<ResolvedUnderlay>> {
            let Some(handle) = self.handle() else {
                return Err(anyhow!("rtnetlink handle unavailable"));
            };

            match family {
                AddressFamily::Inet => {
                    let mut routes = handle
                        .route()
                        .get(RouteMessageBuilder::<Ipv4Addr>::new().build())
                        .execute();
                    while let Some(route) = routes.try_next().await.context("list ipv4 routes")? {
                        if let Some(info) = self
                            .route_underlay_from_message(route.header, route.attributes)
                            .await?
                        {
                            return Ok(Some(info));
                        }
                    }
                }
                AddressFamily::Inet6 => {
                    let mut routes = handle
                        .route()
                        .get(RouteMessageBuilder::<Ipv6Addr>::new().build())
                        .execute();
                    while let Some(route) = routes.try_next().await.context("list ipv6 routes")? {
                        if let Some(info) = self
                            .route_underlay_from_message(route.header, route.attributes)
                            .await?
                        {
                            return Ok(Some(info));
                        }
                    }
                }
                _ => {}
            }

            Ok(None)
        }

        /// Convert one default-route row into a usable underlay snapshot when possible.
        ///
        /// Only main-table default routes qualify here. If the route exposes a preferred source
        /// address we use it directly; otherwise we fall back to the first usable interface address
        /// in the same family.
        async fn route_underlay_from_message(
            &self,
            header: RouteHeader,
            attributes: Vec<RouteAttribute>,
        ) -> Result<Option<ResolvedUnderlay>> {
            let mut table = u32::from(header.table);
            let mut output_ifindex = None;
            let mut destination_present = false;
            let mut preferred_source = None;

            for attribute in attributes {
                match attribute {
                    RouteAttribute::Oif(index) => output_ifindex = Some(index),
                    RouteAttribute::Destination(_) => destination_present = true,
                    RouteAttribute::Table(route_table) => table = route_table,
                    RouteAttribute::PrefSource(RouteAddress::Inet(ip)) => {
                        preferred_source = Some(IpAddr::V4(ip));
                    }
                    RouteAttribute::PrefSource(RouteAddress::Inet6(ip)) => {
                        preferred_source = Some(IpAddr::V6(ip));
                    }
                    _ => {}
                }
            }

            if table != u32::from(RouteHeader::RT_TABLE_MAIN)
                || header.destination_prefix_length != 0
                || destination_present
            {
                return Ok(None);
            }

            let Some(index) = output_ifindex else {
                return Ok(None);
            };
            self.underlay_from_interface_index(index, preferred_source)
                .await
        }

        /// Resolve one interface index into a usable underlay snapshot.
        ///
        /// This filters out down, WireGuard-managed, and Mantissa-managed overlay links before
        /// returning the lower-device name, source IP, and current link MTU.
        async fn underlay_from_interface_index(
            &self,
            index: u32,
            preferred_ip: Option<IpAddr>,
        ) -> Result<Option<ResolvedUnderlay>> {
            let Some(handle) = self.handle() else {
                return Err(anyhow!("rtnetlink handle unavailable"));
            };

            let mut links = handle.link().get().match_index(index).execute();
            let Some(link) = links.try_next().await.context("resolve route interface")? else {
                return Ok(None);
            };
            let name = link
                .attributes
                .iter()
                .find_map(|attr| match attr {
                    LinkAttribute::IfName(name) => Some(name.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| format!("ifindex{index}"));
            let flags = link.header.flags;
            if !flags.contains(LinkFlags::Up)
                || name == MANTISSA_WIREGUARD_IFNAME
                || is_managed_overlay_link_name(&name)
            {
                return Ok(None);
            }

            let ip = match preferred_ip {
                Some(ip) => ip,
                None => {
                    let family = match flags.contains(LinkFlags::Loopback) {
                        true => AddressFamily::Inet,
                        false => AddressFamily::Unspec,
                    };
                    let Some(ip) = self.interface_primary_ip(index, family).await? else {
                        return Ok(None);
                    };
                    ip
                }
            };

            self.resolved_underlay(index, name, ip).await
        }

        /// Pick one usable source IP from an interface, optionally constraining the family.
        ///
        /// The route lookup prefers the route's own preferred source. This helper only runs when
        /// the kernel did not provide one, in which case we choose the first stable non-link-local
        /// address available on the output interface.
        async fn interface_primary_ip(
            &self,
            index: u32,
            family: AddressFamily,
        ) -> Result<Option<IpAddr>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut addresses = handle
                .address()
                .get()
                .set_link_index_filter(index)
                .execute();
            let mut ipv6_candidate = None;
            while let Some(message) = addresses.try_next().await.context("enumerate addresses")? {
                if family != AddressFamily::Unspec && message.header.family != family {
                    continue;
                }
                for attribute in message.attributes {
                    if let AddressAttribute::Address(ip) | AddressAttribute::Local(ip) = attribute {
                        if ip.is_loopback()
                            || matches!(ip, IpAddr::V6(addr) if addr.is_unicast_link_local())
                        {
                            continue;
                        }
                        match ip {
                            IpAddr::V4(_) => return Ok(Some(ip)),
                            IpAddr::V6(_) => {
                                if ipv6_candidate.is_none() {
                                    ipv6_candidate = Some(ip);
                                }
                            }
                        }
                    }
                }
            }
            Ok(ipv6_candidate)
        }

        /// Scan links as a last-resort fallback when no explicit advertise or default route wins.
        ///
        /// This keeps the old best-effort behavior for unusual hosts without routing metadata, but
        /// it now refuses loopback, WireGuard, and Mantissa-managed overlay interfaces so the
        /// controller cannot bootstrap a new VXLAN device from another overlay's transient state.
        async fn scan_underlay_candidates(&self) -> Result<ResolvedUnderlay> {
            let Some(handle) = self.handle() else {
                return Err(anyhow!("rtnetlink handle unavailable"));
            };

            let mut link_stream = handle.link().get().execute();
            while let Some(link) = link_stream
                .try_next()
                .await
                .context("enumerate link devices via rtnetlink")?
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
                if !flags.contains(LinkFlags::Up) {
                    continue;
                }
                if flags.contains(LinkFlags::Loopback)
                    || name == MANTISSA_WIREGUARD_IFNAME
                    || is_managed_overlay_link_name(&name)
                {
                    continue;
                }

                let Some(ip) = self
                    .interface_primary_ip(index, AddressFamily::Unspec)
                    .await?
                else {
                    continue;
                };
                if let Some(info) = self.resolved_underlay(index, name, ip).await? {
                    info!(
                        target: "network",
                        underlay = %info.name,
                        underlay_index = info.index,
                        underlay_ip = %info.ip,
                        underlay_mtu = info.mtu,
                        "selected underlay from fallback link scan"
                    );
                    return Ok(info);
                }
            }

            Err(anyhow!(
                "unable to locate a usable local interface for vxlan underlay"
            ))
        }

        /// Resolve and apply the effective underlay contract for one network plan.
        ///
        /// The controller keeps the replicated spec as the operator intent, but every reconcile
        /// still needs one concrete local underlay interface and one effective MTU derived from
        /// the host's current routing state before it touches kernel links.
        pub async fn apply_plan_underlay_constraints(&self, plan: &mut NetworkPlan) -> Result<()> {
            if !plan.uses_vxlan() {
                return Ok(());
            }
            if self.handle.is_none() {
                return Ok(());
            }

            let underlay = if let (Some(ifname), Some(ip)) =
                (plan.underlay_iface.as_deref(), plan.underlay_ip)
            {
                self.explicit_underlay_by_name(ifname, ip).await?
            } else {
                self.underlay_info().await?
            };

            let requested_mtu = plan.mtu;
            let effective_mtu = clamp_overlay_mtu(requested_mtu, &underlay)?;
            if effective_mtu != requested_mtu {
                warn!(
                    target: "network",
                    network = %plan.network_id,
                    requested_mtu,
                    effective_mtu,
                    underlay = %underlay.name,
                    underlay_index = underlay.index,
                    underlay_ip = %underlay.ip,
                    underlay_mtu = underlay.mtu,
                    "clamping overlay mtu to the selected underlay ceiling"
                );
                plan.mtu = effective_mtu;
            }

            plan.underlay_iface = Some(underlay.name);
            plan.underlay_ip = Some(underlay.ip);
            Ok(())
        }

        /// Resolve one explicitly requested underlay interface into a usable local snapshot.
        ///
        /// WireGuard underlay selection feeds an exact interface and tunnel IP into the network
        /// plan, so reconcile must honor that explicit lower-device choice instead of rerunning the
        /// plaintext autodetection filters.
        async fn explicit_underlay_by_name(
            &self,
            ifname: &str,
            ip: IpAddr,
        ) -> Result<ResolvedUnderlay> {
            let Some(index) = self.find_link(ifname).await? else {
                anyhow::bail!("explicit underlay interface {ifname} is missing");
            };
            if !self.interface_has_ip(index, address_family(ip), ip).await? {
                anyhow::bail!(
                    "explicit underlay interface {ifname} no longer owns local address {ip}"
                );
            }
            self.resolved_underlay(index, ifname.to_string(), ip)
                .await?
                .ok_or_else(|| anyhow!("explicit underlay interface {ifname} disappeared"))
        }

        /// Resolve one ifindex back to its interface name.
        async fn link_name(&self, index: u32) -> Result<Option<String>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut links = handle.link().get().match_index(index).execute();
            while let Some(link) = links.try_next().await? {
                for nla in link.attributes.into_iter() {
                    if let LinkAttribute::IfName(name) = nla {
                        return Ok(Some(name));
                    }
                }
            }
            Ok(None)
        }

        /// Resolve the current MAC address for a link so MAC updates remain idempotent.
        ///
        /// The provisioning loop uses this to skip redundant `ip link set address` operations
        /// once the host-access veth has the desired deterministic address.
        async fn link_address(&self, index: u32) -> Result<Option<Vec<u8>>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut links = handle.link().get().match_index(index).execute();
            while let Some(link) = links.try_next().await? {
                for nla in link.attributes.into_iter() {
                    if let LinkAttribute::Address(addr) = nla {
                        return Ok(Some(addr));
                    }
                }
            }
            Ok(None)
        }

        /// Return the "lower" (underlay) link index for the provided interface, when available.
        ///
        /// Mantissa needs to detect the underlay device used by an existing VXLAN interface so
        /// we can decide whether it must be recreated (for example when switching the overlay
        /// underlay from plaintext to WireGuard).
        ///
        /// Important: the VXLAN underlay link is stored in `IFLA_INFO_DATA` as
        /// `IFLA_VXLAN_LINK` (parsed here as `InfoData::Vxlan(..)/InfoVxlan::Link(..)`).
        /// `LinkMessageBuilder::link()` / `IFLA_LINK` is *not* reliable for VXLAN devices.
        async fn link_lower_index(&self, index: u32) -> Result<Option<u32>> {
            let Some(handle) = self.handle() else {
                return Ok(None);
            };

            let mut links = handle.link().get().match_index(index).execute();
            while let Some(link) = links.try_next().await? {
                for nla in link.attributes.into_iter() {
                    match nla {
                        LinkAttribute::LinkInfo(infos) => {
                            for info in infos {
                                if let LinkInfo::Data(InfoData::Vxlan(entries)) = info {
                                    for entry in entries {
                                        if let InfoVxlan::Link(lower) = entry {
                                            return Ok(Some(lower));
                                        }
                                    }
                                }
                            }
                        }
                        LinkAttribute::Link(lower) => {
                            return Ok(Some(lower));
                        }
                        _ => {}
                    }
                }
            }
            Ok(None)
        }

        /// Collect a compact link inventory string list used in VXLAN creation diagnostics.
        async fn collect_link_inventory(&self) -> Result<Vec<String>> {
            let mut entries = Vec::new();
            let Some(handle) = self.handle() else {
                return Ok(entries);
            };

            let mut stream = handle.link().get().execute();
            while let Some(link) = stream.try_next().await? {
                let index = link.header.index;
                let mut name = format!("ifindex{index}");
                let mut master: Option<u32> = None;
                let mut lower: Option<u32> = None;
                for attr in link.attributes.iter() {
                    match attr {
                        LinkAttribute::IfName(ifname) => name = ifname.clone(),
                        LinkAttribute::Controller(idx) => master = Some(*idx),
                        LinkAttribute::Link(idx) => lower = Some(*idx),
                        _ => {}
                    }
                }
                let flags = format!("{:?}", link.header.flags);
                entries.push(format!(
                    "idx={} name={} flags={} master={:?} link={:?}",
                    index, name, flags, master, lower
                ));
            }
            Ok(entries)
        }

        /// Best-effort load of the Linux VXLAN module before creating VXLAN devices.
        fn ensure_vxlan_module() -> Result<()> {
            match Command::new("modprobe").arg("vxlan").status() {
                Ok(status) if status.success() => Ok(()),
                Ok(status) => {
                    if unsafe { libc::geteuid() } != 0 {
                        warn!(
                            target: "network",
                            "modprobe vxlan failed with status {status}; ignoring because process is not root"
                        );
                        Ok(())
                    } else {
                        Err(anyhow!("modprobe vxlan exited with status {status}"))
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    if unsafe { libc::geteuid() } != 0 {
                        warn!(
                            target: "network",
                            "modprobe not available; skipping vxlan load because process is not root"
                        );
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "modprobe binary not found; ensure the vxlan module is available"
                        ))
                    }
                }
                Err(err) => {
                    if unsafe { libc::geteuid() } != 0 {
                        warn!(
                            target: "network",
                            "modprobe vxlan failed ({err}); ignoring because process is not root"
                        );
                        Ok(())
                    } else {
                        Err(err.into())
                    }
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
pub(super) use linux::NetworkProvisioner;

#[cfg(not(target_os = "linux"))]
mod stub {
    use super::super::NetworkPlan;
    use anyhow::Result;
    use tracing::info;

    #[derive(Clone, Default)]
    pub struct NetworkProvisioner;

    impl NetworkProvisioner {
        /// Build the no-op provisioner used on unsupported platforms.
        pub fn new() -> Result<Self> {
            Ok(Self)
        }

        /// Report whether this platform can provision kernel interfaces and therefore bind resolver IPs.
        pub fn supports_resolver_bind(&self) -> bool {
            false
        }

        /// Unsupported platforms never create kernel overlay links, so there is nothing to clean.
        pub async fn cleanup_orphaned_network_links(
            &self,
            _desired: &std::collections::HashSet<uuid::Uuid>,
        ) -> Result<()> {
            Ok(())
        }

        /// Unsupported platforms never create deterministic overlay links.
        pub async fn network_links_exist(&self, _plan: &NetworkPlan) -> Result<bool> {
            Ok(false)
        }

        /// Return `None` on unsupported platforms, since no kernel interfaces are created.
        pub async fn link_index(&self, _name: &str) -> Result<Option<u32>> {
            Ok(None)
        }

        /// Unsupported platforms do not derive kernel underlay constraints for overlay links.
        pub async fn apply_plan_underlay_constraints(&self, _plan: &mut NetworkPlan) -> Result<()> {
            Ok(())
        }

        /// Log successful no-op provisioning so non-Linux tests can exercise control-plane flow.
        pub async fn ensure_network(&self, plan: &NetworkPlan) -> Result<()> {
            info!(
                target: "network",
                "network provisioning is not supported on this platform, marking '{}' ({:?}) ready without kernel changes",
                plan.bridge_name,
                plan.driver
            );
            Ok(())
        }

        /// Unsupported platforms do not create kernel links, so teardown is a no-op.
        pub async fn teardown_network(&self, _plan: &NetworkPlan) -> Result<()> {
            Ok(())
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) use stub::NetworkProvisioner;
