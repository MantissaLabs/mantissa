use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkPeerState, NetworkPeerStateValue, NetworkSpecValue, NetworkStatus,
};
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{Duration, sleep};
use tracing::warn;
use uuid::Uuid;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MTU: u32 = 1450;
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
    provisioner: platform::NetworkProvisioner,
    active_networks: AsyncMutex<HashSet<Uuid>>,
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
    pub fn new(registry: NetworkRegistry, node_id: Uuid, node_name: String) -> Result<Self> {
        let provisioner = platform::NetworkProvisioner::new()?;
        Ok(Self {
            inner: Arc::new(NetworkControllerInner {
                registry,
                node_id,
                node_name,
                provisioner,
                active_networks: AsyncMutex::new(HashSet::new()),
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

            self.inner
                .registry
                .remove_peer_states_for_network(id)
                .await
                .context("remove peer state for deleted network")?;

            self.inner
                .registry
                .remove_attachments_for_network(id)
                .await
                .context("remove attachments for deleted network")?;

            active.remove(&id);
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
            vxlan_name: format!("mnt-vxlan-{suffix}"),
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
}

impl NetworkPlan {
    fn from_id(network_id: Uuid) -> Self {
        let suffix = short_id(network_id);
        Self {
            network_id,
            vxlan_name: format!("mnt-vxlan-{suffix}"),
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
    use rtnetlink::{Handle, new_connection};

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
                .add()
                .vxlan(plan.vxlan_name.clone(), plan.vni)
                .learning(1)
                .port(VXLAN_PORT)
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
                .add()
                .bridge(plan.bridge_name.clone())
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
                .set(index)
                .up()
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
                .set(index)
                .mtu(mtu)
                .execute()
                .await
                .with_context(|| format!("set mtu {mtu} on link {index}"))?;
            Ok(())
        }

        async fn attach_master(&self, link_index: u32, master_index: u32) -> Result<()> {
            if let Err(err) = self
                .handle
                .link()
                .set(link_index)
                .master(master_index)
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
