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
use tracing::warn;
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

        self.inner
            .provisioner
            .ensure_network(&plan)
            .await
            .with_context(|| format!("ensure network {}", plan.network_id))?;

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
    use anyhow::{Context, Result};
    use futures::TryStreamExt;
    use rtnetlink::{Handle, LinkBridge, LinkUnspec, LinkVxlan, new_connection};

    #[derive(Clone)]
    pub struct NetworkProvisioner {
        handle: Handle,
    }

    impl NetworkProvisioner {
        pub fn new() -> Result<Self> {
            let (connection, handle, _) =
                new_connection().context("failed to open rtnetlink connection")?;

            tokio::spawn(connection);

            Ok(Self { handle })
        }

        pub async fn ensure_network(&self, plan: &NetworkPlan) -> Result<()> {
            let vxlan_index = self.ensure_vxlan(plan).await?;
            let bridge_index = self.ensure_bridge(plan).await?;
            self.attach_master(vxlan_index, bridge_index).await?;
            self.set_up(vxlan_index).await?;
            self.set_up(bridge_index).await?;
            self.set_mtu(vxlan_index, plan.mtu).await?;
            self.set_mtu(bridge_index, plan.mtu).await?;
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
                return Ok(index);
            }

            self.handle
                .link()
                .add(
                    LinkVxlan::new(&plan.vxlan_name, plan.vni)
                        .learning(true)
                        .port(VXLAN_PORT)
                        .build(),
                )
                .execute()
                .await
                .with_context(|| format!("create vxlan {}", plan.vxlan_name))?;

            let index = self
                .find_link(&plan.vxlan_name)
                .await?
                .context("vxlan interface missing after creation")?;
            Ok(index)
        }

        async fn ensure_bridge(&self, plan: &NetworkPlan) -> Result<u32> {
            if let Some(index) = self.find_link(&plan.bridge_name).await? {
                return Ok(index);
            }

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
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(index).up().build())
                .execute()
                .await
                .with_context(|| format!("bring link {index} up"))?;
            Ok(())
        }

        async fn set_mtu(&self, index: u32, mtu: u32) -> Result<()> {
            if mtu == 0 {
                return Ok(());
            }
            self.handle
                .link()
                .set(LinkUnspec::new_with_index(index).mtu(mtu).build())
                .execute()
                .await
                .with_context(|| format!("set mtu {mtu} on link {index}"))?;
            Ok(())
        }

        async fn attach_master(&self, link_index: u32, master_index: u32) -> Result<()> {
            if let Err(err) = self
                .handle
                .link()
                .set(
                    LinkUnspec::new_with_index(link_index)
                        .controller(master_index)
                        .build(),
                )
                .execute()
                .await
            {
                tracing::warn!(
                    target: "network",
                    "failed to attach link {link_index} to bridge {master_index}: {err:#}"
                );
            }
            Ok(())
        }

        async fn find_link(&self, name: &str) -> Result<Option<u32>> {
            let mut links = self
                .handle
                .link()
                .get()
                .match_name(name.to_string())
                .execute();

            while let Some(link) = links.try_next().await? {
                return Ok(Some(link.header.index));
            }
            Ok(None)
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
