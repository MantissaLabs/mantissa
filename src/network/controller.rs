use crate::network::attachment::PlatformAttachmentProvisioner;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkAttachmentState, NetworkPeerState, NetworkPeerStateValue, NetworkSpecValue,
    NetworkStatus,
};
use crate::registry::Registry;
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use uuid::Uuid;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const DEFAULT_MTU: u32 = 1450;
#[cfg(target_os = "linux")]
const VXLAN_PORT: u16 = 4789;

#[derive(Clone)]
pub struct NetworkController {
    inner: Arc<NetworkControllerInner>,
}

struct NetworkControllerInner {
    registry: NetworkRegistry,
    node_id: Uuid,
    node_name: String,
    cluster_registry: Registry,
    provisioner: platform::NetworkProvisioner,
    active_networks: AsyncMutex<HashSet<Uuid>>,
    remote_fdb: AsyncMutex<HashMap<Uuid, HashMap<String, IpAddr>>>,
    flood_entries: AsyncMutex<HashMap<Uuid, HashSet<IpAddr>>>,
    attachment: PlatformAttachmentProvisioner,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug)]
struct NetworkPlan {
    network_id: Uuid,
    vxlan_name: String,
    bridge_name: String,
    vni: u32,
    mtu: u32,
}

impl NetworkController {
    pub fn new(
        registry: NetworkRegistry,
        cluster_registry: Registry,
        node_id: Uuid,
        node_name: String,
    ) -> Result<Self> {
        let provisioner = platform::NetworkProvisioner::new()?;
        let attachment = PlatformAttachmentProvisioner::new().unwrap_or_else(|err| {
            warn!(target: "network", "failed to initialize attachment provisioner: {err}");
            PlatformAttachmentProvisioner::default()
        });

        Ok(Self {
            inner: Arc::new(NetworkControllerInner {
                registry,
                node_id,
                node_name,
                cluster_registry,
                provisioner,
                active_networks: AsyncMutex::new(HashSet::new()),
                remote_fdb: AsyncMutex::new(HashMap::new()),
                flood_entries: AsyncMutex::new(HashMap::new()),
                attachment,
            }),
        })
    }

    /// Spawn the reconciliation loop on the current local executor.
    pub fn spawn(&self) {
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.run().await;
        });
    }

    async fn run(&self) {
        loop {
            if let Err(err) = self.reconcile_once().await {
                warn!(target: "network", "network reconciliation failed: {err:#}");
            }
            sleep(RECONCILE_INTERVAL).await;
        }
    }

    async fn reconcile_once(&self) -> Result<()> {
        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specifications")?;

        let mut desired: HashSet<Uuid> = HashSet::with_capacity(specs.len());
        for spec in specs {
            if spec.is_deleted() {
                if let Err(err) = self.teardown_deleted_network(&spec).await {
                    warn!(
                        target: "network",
                        "failed to process deleted network {} ({}): {err:#}",
                        spec.name,
                        spec.id
                    );
                }
                continue;
            }

            desired.insert(spec.id);
            if let Err(err) = self.reconcile_network(spec.clone()).await {
                warn!(
                    target: "network",
                    "failed to reconcile network {} ({}): {err:#}",
                    spec.name,
                    spec.id
                );
                self.update_peer_state_error(spec.id, err.to_string())
                    .await?;
            }
        }

        self.teardown_removed_networks(&desired).await?;
        Ok(())
    }

    async fn reconcile_network(&self, mut spec: NetworkSpecValue) -> Result<()> {
        let (plan, spec_changed) = self.prepare_plan(&mut spec)?;
        if spec_changed {
            self.inner
                .registry
                .upsert_spec(spec.clone())
                .await
                .context("persist network spec update")?;
        }

        info!("blablabla2");

        info!(
            target: "network",
            network_id = %plan.network_id,
            node_id = %self.inner.node_id,
            node = %self.inner.node_name,
            vxlan = %plan.vxlan_name,
            bridge = %plan.bridge_name,
            vni = plan.vni,
            mtu = plan.mtu,
            "ensuring network resources"
        );
        self.inner
            .provisioner
            .ensure_network(&plan)
            .await
            .with_context(|| format!("ensure network {}", plan.network_id))?;
        info!(
            target: "network",
            network_id = %plan.network_id,
            vxlan = %plan.vxlan_name,
            bridge = %plan.bridge_name,
            "network resources ensured"
        );

        self.mark_peer_ready(plan.network_id).await?;

        if spec.status != NetworkStatus::Ready {
            let mut updated_spec = spec.clone();
            updated_spec.set_status(NetworkStatus::Ready);
            self.inner
                .registry
                .upsert_spec(updated_spec)
                .await
                .context("update network status to ready")?;
        }

        self.reconcile_remote_forwarding(&plan).await?;

        let mut active = self.inner.active_networks.lock().await;
        active.insert(plan.network_id);
        Ok(())
    }

    async fn teardown_removed_networks(&self, desired: &HashSet<Uuid>) -> Result<()> {
        let mut active = self.inner.active_networks.lock().await;
        let stale: Vec<Uuid> = active
            .iter()
            .cloned()
            .filter(|id| !desired.contains(id))
            .collect();

        for id in stale {
            let plan = NetworkPlan::from_id(id);
            if let Err(err) = self.inner.provisioner.teardown_network(&plan).await {
                warn!(
                    target: "network",
                    "failed to tear down network {}: {err:#}",
                    id
                );
            }

            self.cleanup_network_state(id)
                .await
                .context("cleanup network state for deleted network")?;
            active.remove(&id);
        }

        Ok(())
    }

    async fn teardown_deleted_network(&self, spec: &NetworkSpecValue) -> Result<()> {
        let plan = NetworkPlan::from_id(spec.id);
        if let Err(err) = self.inner.provisioner.teardown_network(&plan).await {
            warn!(
                target: "network",
                "failed to tear down deleted network {}: {err:#}",
                spec.id
            );
        }

        self.cleanup_network_state(spec.id)
            .await
            .context("cleanup network state for deleted spec")?;

        let mut active = self.inner.active_networks.lock().await;
        active.remove(&spec.id);
        Ok(())
    }

    async fn cleanup_network_state(&self, network_id: Uuid) -> Result<()> {
        self.inner
            .registry
            .remove_peer_states_for_network(network_id)
            .await
            .context("remove peer state for network")?;

        self.inner
            .registry
            .remove_attachments_for_network(network_id)
            .await
            .context("remove attachments for network")?;

        {
            let mut guard = self.inner.remote_fdb.lock().await;
            guard.remove(&network_id);
        }

        {
            let mut guard = self.inner.flood_entries.lock().await;
            guard.remove(&network_id);
        }

        Ok(())
    }

    fn prepare_plan(&self, spec: &mut NetworkSpecValue) -> Result<(NetworkPlan, bool)> {
        let mut changed = false;

        // Normalize defaults to keep the CRDT state consistent across nodes.
        if spec.vni == 0 {
            let computed_vni = compute_deterministic_vni(spec.id);
            spec.vni = computed_vni;
            changed = true;
        }

        if spec.mtu == 0 {
            spec.mtu = DEFAULT_MTU;
            changed = true;
        }

        spec.bpf_programs.sort();

        let suffix = short_id(spec.id);
        let plan = NetworkPlan {
            network_id: spec.id,
            vxlan_name: format!("mvx-{suffix}"),
            bridge_name: format!("mnt-br-{suffix}"),
            vni: spec.vni,
            mtu: spec.mtu,
        };

        Ok((plan, changed))
    }

    async fn mark_peer_ready(&self, network_id: Uuid) -> Result<()> {
        let mut state = NetworkPeerStateValue::new(
            network_id,
            self.inner.node_id,
            self.inner.node_name.clone(),
            NetworkPeerState::Ready,
            None,
        );
        state.touch();

        self.inner
            .registry
            .upsert_peer_state(state)
            .await
            .context("persist peer state ready")
    }

    async fn update_peer_state_error(&self, network_id: Uuid, message: String) -> Result<()> {
        let mut state = NetworkPeerStateValue::new(
            network_id,
            self.inner.node_id,
            self.inner.node_name.clone(),
            NetworkPeerState::Error,
            Some(message.clone()),
        );
        state.touch();

        self.inner
            .registry
            .upsert_peer_state(state)
            .await
            .context("persist peer state error")
    }

    async fn reconcile_remote_forwarding(&self, plan: &NetworkPlan) -> Result<()> {
        let attachments = self
            .inner
            .registry
            .list_attachments(Some(plan.network_id))
            .context("list attachments for forwarding")?;

        let mut desired: HashMap<String, IpAddr> = HashMap::new();
        let mut flood_targets: HashMap<IpAddr, usize> = HashMap::new();

        for attachment in attachments {
            if attachment.node_id == self.inner.node_id {
                continue;
            }

            if !matches!(attachment.state, NetworkAttachmentState::Ready) {
                continue;
            }

            let mac = match attachment.mac.as_ref() {
                Some(mac) if !mac.is_empty() => mac.clone(),
                _ => continue,
            };

            let peer_ip = match self.peer_ip_for_node(attachment.node_id) {
                Some(ip) => ip,
                None => continue,
            };

            desired.insert(mac, peer_ip);
            *flood_targets.entry(peer_ip).or_insert(0) += 1;
        }

        {
            let mut guard = self.inner.remote_fdb.lock().await;
            let entry = guard.entry(plan.network_id).or_default();

            // Apply desired entries
            for (mac, ip) in &desired {
                let needs_update = entry
                    .get(mac)
                    .map(|existing| existing != ip)
                    .unwrap_or(true);

                if needs_update {
                    self.inner
                        .attachment
                        .ensure_remote_fdb(&plan.vxlan_name, mac, *ip)
                        .await
                        .with_context(|| {
                            format!(
                                "ensure remote fdb entry for mac {mac} to {ip} on {}",
                                plan.vxlan_name
                            )
                        })?;
                }

                entry.insert(mac.clone(), *ip);
            }

            let stale: Vec<(String, IpAddr)> = entry
                .iter()
                .filter(|(mac, ip)| desired.get(*mac).map_or(true, |want| want != *ip))
                .map(|(mac, ip)| (mac.clone(), *ip))
                .collect();

            for (mac, ip) in stale {
                if let Err(err) = self
                    .inner
                    .attachment
                    .remove_remote_fdb(&plan.vxlan_name, &mac, ip)
                    .await
                {
                    warn!(
                        target: "network",
                        "failed to remove stale fdb entry for mac {mac} dst {ip}: {err}"
                    );
                }
                entry.remove(&mac);
            }
        }

        let mut flood_guard = self.inner.flood_entries.lock().await;
        let flood_entry = flood_guard.entry(plan.network_id).or_default();

        for ip in flood_targets.keys() {
            if flood_entry.insert(*ip) {
                self.inner
                    .attachment
                    .ensure_flood_entry(&plan.vxlan_name, *ip)
                    .await
                    .with_context(|| {
                        format!(
                            "ensure broadcast forwarding for {} towards {}",
                            plan.vxlan_name, ip
                        )
                    })?;
            }
        }

        let obsolete: Vec<IpAddr> = flood_entry
            .iter()
            .copied()
            .filter(|ip| !flood_targets.contains_key(ip))
            .collect();

        for ip in obsolete {
            if let Err(err) = self
                .inner
                .attachment
                .remove_flood_entry(&plan.vxlan_name, ip)
                .await
            {
                warn!(
                    target: "network",
                    "failed to remove broadcast forwarding for {} towards {}: {err}",
                    plan.vxlan_name,
                    ip
                );
            }
            flood_entry.remove(&ip);
        }

        Ok(())
    }

    fn peer_ip_for_node(&self, peer_id: Uuid) -> Option<IpAddr> {
        if peer_id == self.inner.node_id {
            return None;
        }

        let address = self.inner.cluster_registry.peer_address(peer_id)?;
        match address.parse::<SocketAddr>() {
            Ok(sock) => Some(sock.ip()),
            Err(err) => {
                warn!(
                    target: "network",
                    "failed to parse peer address '{address}' for {peer_id}: {err}"
                );
                None
            }
        }
    }
}

impl NetworkPlan {
    fn from_id(network_id: Uuid) -> Self {
        let suffix = short_id(network_id);
        Self {
            network_id,
            vxlan_name: format!("mvx-{suffix}"),
            bridge_name: format!("mnt-br-{suffix}"),
            vni: compute_deterministic_vni(network_id),
            mtu: DEFAULT_MTU,
        }
    }
}

fn short_id(id: Uuid) -> String {
    let hex = id.simple().to_string();
    hex.chars().take(8).collect()
}

fn compute_deterministic_vni(network_id: Uuid) -> u32 {
    let bytes = network_id.as_u128();
    let vni = (bytes & 0x00FF_FFFF) as u32;
    // Reserved VNIs are 0 and 16777215; clamp to safe range.
    let vni = if vni == 0 { 1 } else { vni };
    // VXLAN VNI is 24 bits; ensure we stay in range.
    vni & 0x00FF_FFFF
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{NetworkPlan, VXLAN_PORT};
    use anyhow::{Context, Result, anyhow};
    use futures::TryStreamExt;
    use libc;
    use rtnetlink::packet_route::address::AddressAttribute;
    use rtnetlink::packet_route::link::{LinkAttribute, LinkFlags};
    use rtnetlink::{
        Error as RtnetlinkError, Handle, LinkBridge, LinkUnspec, LinkVxlan, new_connection,
    };
    use std::io;
    use std::net::IpAddr;
    use std::process::Command;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tracing::{debug, info, warn};

    #[derive(Clone)]
    pub struct NetworkProvisioner {
        handle: Handle,
        underlay: Arc<AsyncMutex<Option<(u32, IpAddr)>>>,
    }

    impl NetworkProvisioner {
        pub fn new() -> Result<Self> {
            Self::ensure_vxlan_module().context("load vxlan kernel module")?;

            let (connection, handle, _) =
                new_connection().context("failed to open rtnetlink connection")?;

            tokio::spawn(connection);

            Ok(Self {
                handle,
                underlay: Arc::new(AsyncMutex::new(None)),
            })
        }

        pub async fn ensure_network(&self, plan: &NetworkPlan) -> Result<()> {
            info!(
                target: "network",
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                vni = plan.vni,
                mtu = plan.mtu,
                "provisioner: ensuring kernel interfaces"
            );
            let vxlan_index = self
                .ensure_vxlan(plan)
                .await
                .with_context(|| format!("ensure vxlan interface {}", plan.vxlan_name))?;
            info!(
                target: "network",
                vxlan = %plan.vxlan_name,
                vxlan_index,
                "provisioner: vxlan interface ready"
            );

            let bridge_index = self
                .ensure_bridge(plan)
                .await
                .with_context(|| format!("ensure bridge {}", plan.bridge_name))?;
            info!(
                target: "network",
                bridge = %plan.bridge_name,
                bridge_index,
                "provisioner: bridge interface ready"
            );

            self.attach_master(vxlan_index, bridge_index)
                .await
                .with_context(|| {
                    format!(
                        "attach vxlan {} (idx {}) to bridge {} (idx {})",
                        plan.vxlan_name, vxlan_index, plan.bridge_name, bridge_index
                    )
                })?;

            self.set_up(vxlan_index).await.with_context(|| {
                format!("bring link {} (idx {}) up", plan.vxlan_name, vxlan_index)
            })?;
            self.set_up(bridge_index).await.with_context(|| {
                format!("bring link {} (idx {}) up", plan.bridge_name, bridge_index)
            })?;

            if plan.mtu > 0 {
                self.set_mtu(vxlan_index, plan.mtu).await.with_context(|| {
                    format!(
                        "set mtu {} on vxlan {} (idx {})",
                        plan.mtu, plan.vxlan_name, vxlan_index
                    )
                })?;
                self.set_mtu(bridge_index, plan.mtu)
                    .await
                    .with_context(|| {
                        format!(
                            "set mtu {} on bridge {} (idx {})",
                            plan.mtu, plan.bridge_name, bridge_index
                        )
                    })?;
            }

            info!(
                target: "network",
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                "provisioner: kernel interfaces ensured"
            );
            Ok(())
        }

        pub async fn teardown_network(&self, plan: &NetworkPlan) -> Result<()> {
            if let Some(index) = self.find_link(&plan.vxlan_name).await? {
                self.handle
                    .link()
                    .del(index)
                    .execute()
                    .await
                    .with_context(|| format!("delete vxlan {}", plan.vxlan_name))?;
            }

            if let Some(index) = self.find_link(&plan.bridge_name).await? {
                self.handle
                    .link()
                    .del(index)
                    .execute()
                    .await
                    .with_context(|| format!("delete bridge {}", plan.bridge_name))?;
            }

            Ok(())
        }

        async fn ensure_vxlan(&self, plan: &NetworkPlan) -> Result<u32> {
            if let Some(index) = self.find_link(&plan.vxlan_name).await? {
                info!(
                    target: "network",
                    vxlan = %plan.vxlan_name,
                    vxlan_index = index,
                    "provisioner: reusing existing vxlan interface"
                );
                return Ok(index);
            }

            let mut last_error: Option<anyhow::Error> = None;

            for attempt in 0..=1 {
                debug!("LOL3");
                let (underlay_index, underlay_ip) = self
                    .underlay_info()
                    .await
                    .context("resolve underlay interface for vxlan")?;

                let underlay_name = match self.link_name(underlay_index).await {
                    Ok(Some(name)) => name,
                    Ok(None) => {
                        warn!(
                            target: "network",
                            underlay_index,
                            attempt,
                            "underlay index missing while preparing vxlan; will fall back to numeric name"
                        );
                        format!("ifindex{underlay_index}")
                    }
                    Err(err) => {
                        warn!(
                            target: "network",
                            underlay_index,
                            attempt,
                            error = %err,
                            "failed to resolve underlay name before vxlan creation; falling back to numeric name"
                        );
                        format!("ifindex{underlay_index}")
                    }
                };

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
                        .learning(true)
                        .port(VXLAN_PORT)
                        .link(underlay_index);
                    match underlay_ip {
                        IpAddr::V4(ip) => base.local(ip),
                        IpAddr::V6(ip) => base.local6(ip),
                    }
                };

                match self.handle.link().add(builder.build()).execute().await {
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

        async fn ensure_bridge(&self, plan: &NetworkPlan) -> Result<u32> {
            if let Some(index) = self.find_link(&plan.bridge_name).await? {
                info!(
                    target: "network",
                    bridge = %plan.bridge_name,
                    bridge_index = index,
                    "provisioner: reusing existing bridge"
                );
                return Ok(index);
            }

            info!(
                target: "network",
                bridge = %plan.bridge_name,
                "provisioner: creating bridge"
            );
            self.handle
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

        async fn set_up(&self, index: u32) -> Result<()> {
            let name = self
                .link_name(index)
                .await
                .context("resolve link name before bringing link up")?
                .unwrap_or_else(|| format!("ifindex{index}"));
            info!(
                target: "network",
                link = %name,
                link_index = index,
                "provisioner: bringing link up"
            );
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(index).up().build())
                .execute()
                .await
                .with_context(|| format!("bring link {name} (index {index}) up"))?;
            info!(
                target: "network",
                link = %name,
                link_index = index,
                "provisioner: link is up"
            );
            Ok(())
        }

        async fn set_mtu(&self, index: u32, mtu: u32) -> Result<()> {
            if mtu == 0 {
                return Ok(());
            }
            let name = self
                .link_name(index)
                .await
                .context("resolve link name before setting mtu")?
                .unwrap_or_else(|| format!("ifindex{index}"));
            info!(
                target: "network",
                link = %name,
                link_index = index,
                mtu,
                "provisioner: updating mtu"
            );
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(index).mtu(mtu).build())
                .execute()
                .await
                .with_context(|| format!("set mtu {mtu} on link {name} (index {index})"))?;
            info!(
                target: "network",
                link = %name,
                link_index = index,
                mtu,
                "provisioner: mtu updated"
            );
            Ok(())
        }

        async fn attach_master(&self, link_index: u32, master_index: u32) -> Result<()> {
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

            info!(
                target: "network",
                link = %link_name,
                link_index,
                bridge = %master_name,
                bridge_index = master_index,
                "provisioner: attaching link to bridge"
            );
            self.handle
                .link()
                .set(
                    LinkUnspec::new_with_index(link_index)
                        .controller(master_index)
                        .build(),
                )
                .execute()
                .await
                .with_context(|| {
                    format!(
                        "attach link {link_name} (index {link_index}) to bridge {master_name} (index {master_index})"
                    )
                })?;
            info!(
                target: "network",
                link = %link_name,
                link_index,
                bridge = %master_name,
                bridge_index = master_index,
                "provisioner: link attached to bridge"
            );
            Ok(())
        }

        async fn find_link(&self, name: &str) -> Result<Option<u32>> {
            let mut stream = self
                .handle
                .link()
                .get()
                .match_name(name.to_string())
                .execute();

            loop {
                match stream.try_next().await {
                    Ok(Some(link)) => return Ok(Some(link.header.index)),
                    Ok(None) => break,
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
                            return Ok(None);
                        }
                        return Err(RtnetlinkError::NetlinkError(msg).into());
                    }
                    Err(err) => return Err(err.into()),
                }
            }

            Ok(None)
        }

        async fn underlay_info(&self) -> Result<(u32, IpAddr)> {
            let cached = {
                let guard = self.underlay.lock().await;
                *guard
            };

            if let Some(info) = cached {
                let name = self
                    .link_name(info.0)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| format!("ifindex{}", info.0));
                debug!(
                    target: "network",
                    underlay = %name,
                    underlay_index = info.0,
                    underlay_ip = %info.1,
                    "provisioner: reusing cached underlay interface"
                );
                return Ok(info);
            }

            let info = self.detect_underlay_info().await?;
            {
                let mut guard = self.underlay.lock().await;
                *guard = Some(info);
            }
            let name = self
                .link_name(info.0)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| format!("ifindex{}", info.0));
            info!(
                target: "network",
                underlay = %name,
                underlay_index = info.0,
                underlay_ip = %info.1,
                "provisioner: detected underlay interface"
            );
            Ok(info)
        }

        async fn detect_underlay_info(&self) -> Result<(u32, IpAddr)> {
            // Walk all links and choose the first non-loopback interface that is up
            // and has an assigned IP address. Prefer IPv4 addresses but fall back to
            // IPv6 if needed.
            let mut link_stream = self.handle.link().get().execute();

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
                    warn!(
                        target: "network",
                        "skipping underlay candidate {} (index {}) because it is down",
                        name,
                        index
                    );
                    continue;
                }
                if flags.contains(LinkFlags::Loopback) {
                    warn!(
                        target: "network",
                        "skipping underlay candidate {} (index {}) because it is loopback",
                        name,
                        index
                    );
                    continue;
                }

                let mut addr_stream = self
                    .handle
                    .address()
                    .get()
                    .set_link_index_filter(index)
                    .execute();

                let mut ipv6_candidate: Option<IpAddr> = None;

                while let Some(msg) = addr_stream
                    .try_next()
                    .await
                    .context("enumerate interface addresses via rtnetlink")?
                {
                    for attr in msg.attributes.iter() {
                        if let AddressAttribute::Address(addr) | AddressAttribute::Local(addr) =
                            attr
                        {
                            let ip = addr.clone();
                            if ip.is_loopback() {
                                continue;
                            }

                            match ip {
                                IpAddr::V4(_) => {
                                    info!(
                                        target: "network",
                                        "selected underlay interface {name} (index {index}) with address {ip}"
                                    );
                                    return Ok((index, ip));
                                }
                                IpAddr::V6(_) => {
                                    if ipv6_candidate.is_none() {
                                        ipv6_candidate = Some(ip);
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some(ip) = ipv6_candidate {
                    info!(
                        target: "network",
                        "selected underlay interface {name} (index {index}) with address {ip}"
                    );
                    return Ok((index, ip));
                }

                warn!(
                    target: "network",
                    "no usable addresses found on underlay candidate {name} (index {index}), continuing"
                );
            }

            Err(anyhow!(
                "unable to locate a non-loopback interface with an IP address for vxlan underlay"
            ))
        }

        async fn link_name(&self, index: u32) -> Result<Option<String>> {
            let mut links = self.handle.link().get().match_index(index).execute();
            while let Some(link) = links.try_next().await? {
                for nla in link.attributes.into_iter() {
                    if let LinkAttribute::IfName(name) = nla {
                        return Ok(Some(name));
                    }
                }
            }
            Ok(None)
        }

        async fn collect_link_inventory(&self) -> Result<Vec<String>> {
            let mut entries = Vec::new();
            let mut stream = self.handle.link().get().execute();
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

        fn ensure_vxlan_module() -> Result<()> {
            match Command::new("modprobe").arg("vxlan").status() {
                Ok(status) if status.success() => Ok(()),
                Ok(status) => Err(anyhow!("modprobe vxlan exited with status {status}")),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(anyhow!(
                    "modprobe binary not found; ensure the vxlan module is available"
                )),
                Err(err) => Err(err.into()),
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::NetworkPlan;
    use anyhow::Result;
    use tracing::info;

    #[derive(Clone, Default)]
    pub struct NetworkProvisioner;

    impl NetworkProvisioner {
        pub fn new() -> Result<Self> {
            Ok(Self)
        }

        pub async fn ensure_network(&self, plan: &NetworkPlan) -> Result<()> {
            info!(
                target: "network",
                "network provisioning is not supported on this platform, marking '{}' ready without kernel changes",
                plan.vxlan_name
            );
            Ok(())
        }

        pub async fn teardown_network(&self, _plan: &NetworkPlan) -> Result<()> {
            Ok(())
        }
    }
}
