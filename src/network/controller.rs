use crate::config;
use crate::gossip::Message;
use crate::network::allocator::{parse_ipv4_cidr, resolver_ipv4_address};
use crate::network::attachment::PlatformAttachmentProvisioner;
use crate::network::bpf::{NetworkBpfManager, NetworkInterfaceContext};
use crate::network::discovery::ServiceDiscovery;
use crate::network::events::ForwardingEvent;
use crate::network::nodeport::NodePortManager;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    BpfAttachPoint, BpfProgramSpec, NetworkAttachmentState, NetworkEvent, NetworkPeerState,
    NetworkPeerStateValue, NetworkSpecValue, NetworkStatus,
};
use crate::network::wireguard::{self, WireGuardUnderlayState};
use crate::registry::Registry;
use crate::services::registry::ServiceRegistry;
use crate::store::workload_store::WorkloadStore;
use anyhow::{Context, Result};
use async_channel::Sender;
#[cfg(target_os = "linux")]
use aya::{programs::ProgramError, sys::SyscallError};
use blake3::Hasher;
use std::collections::{HashMap, HashSet};
use std::future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc::UnboundedReceiver};
use tokio::time::Duration;
use tracing::{debug, warn};
use uuid::Uuid;

/// Periodic reconciliation interval for drift detection when no events are pending.
const RECONCILE_DRIFT_INTERVAL: Duration = Duration::from_secs(60);
/// Frequency to check for attachment updates that require forwarding refresh.
const ATTACHMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const DEFAULT_MTU: u32 = 1450;
#[cfg(target_os = "linux")]
const VXLAN_PORT: u16 = 4789;
const WIREGUARD_RECONCILE_DEBOUNCE: Duration = Duration::from_secs(1);
const WIREGUARD_RECONCILE_RETRY_LIMIT: usize = 3;

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
    bpf: NetworkBpfManager,
    discovery: ServiceDiscovery,
    active_networks: AsyncMutex<HashSet<Uuid>>,
    vxlan_ifindex: AsyncMutex<HashMap<Uuid, u32>>,
    remote_fdb: AsyncMutex<HashMap<Uuid, HashMap<String, IpAddr>>>,
    flood_entries: AsyncMutex<HashMap<Uuid, HashSet<IpAddr>>>,
    attachment: PlatformAttachmentProvisioner,
    pending_forwarding: AsyncMutex<HashSet<Uuid>>,
    forwarding_events: AsyncMutex<Option<UnboundedReceiver<ForwardingEvent>>>,
    pending_specs: AsyncMutex<HashSet<Uuid>>,
    wireguard: AsyncMutex<WireGuardUnderlayState>,
    wireguard_last_reconcile: AsyncMutex<Option<std::time::Instant>>,
    wireguard_retry_scheduled: AsyncMutex<bool>,
    attachments_root: AsyncMutex<Option<String>>,
    attachment_sync_notify: Option<Arc<Notify>>,
    wake: Notify,
    gossip_tx: Sender<Message>,
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug)]
struct NetworkPlan {
    network_id: Uuid,
    vxlan_name: String,
    bridge_name: String,
    vni: u32,
    mtu: u32,
    resolver_ipv4: Option<Ipv4Addr>,
    subnet_prefix: Option<u8>,
    underlay_iface: Option<String>,
    underlay_ip: Option<IpAddr>,
    /// Deterministic host-access MAC used for static FDB programming when resolver networking is enabled.
    host_access_mac: Option<[u8; 6]>,
}

#[cfg(target_os = "linux")]
fn default_bpf_programs() -> Vec<BpfProgramSpec> {
    if !config::bpf_attach_enabled() {
        return Vec::new();
    }

    vec![
        BpfProgramSpec::with_attach_point("vxlan_xdp", BpfAttachPoint::VxlanXdp),
        BpfProgramSpec::with_attach_point("bridge_xdp", BpfAttachPoint::BridgeXdp),
        BpfProgramSpec::with_attach_point("bridge_tc_ingress", BpfAttachPoint::BridgeTcIngress),
        BpfProgramSpec::with_attach_point("bridge_tc_egress", BpfAttachPoint::BridgeTcEgress),
    ]
}

#[cfg(not(target_os = "linux"))]
fn default_bpf_programs() -> Vec<BpfProgramSpec> {
    Vec::new()
}

impl NetworkController {
    #[allow(clippy::arc_with_non_send_sync, clippy::too_many_arguments)]
    pub fn new(
        registry: NetworkRegistry,
        cluster_registry: Registry,
        workload_store: WorkloadStore,
        service_registry: ServiceRegistry,
        node_id: Uuid,
        node_name: String,
        gossip_tx: Sender<Message>,
        forwarding_events: Option<UnboundedReceiver<ForwardingEvent>>,
        attachment_sync_notify: Option<Arc<Notify>>,
    ) -> Result<Self> {
        let provisioner = platform::NetworkProvisioner::new()?;
        let attachment = PlatformAttachmentProvisioner::new().unwrap_or_else(|err| {
            warn!(target: "network", "failed to initialize attachment provisioner: {err}");
            PlatformAttachmentProvisioner::unavailable()
        });
        let bpf = NetworkBpfManager::new().unwrap_or_else(|err| {
            warn!(target: "network", "failed to initialize bpf manager: {err:#}");
            NetworkBpfManager::unavailable()
        });

        let discovery = ServiceDiscovery::new(
            registry.clone(),
            workload_store,
            service_registry,
            bpf.clone(),
            cluster_registry.health_monitor(),
        );

        Ok(Self {
            inner: Arc::new(NetworkControllerInner {
                registry,
                node_id,
                node_name,
                cluster_registry,
                provisioner,
                bpf,
                discovery,
                active_networks: AsyncMutex::new(HashSet::new()),
                vxlan_ifindex: AsyncMutex::new(HashMap::new()),
                remote_fdb: AsyncMutex::new(HashMap::new()),
                flood_entries: AsyncMutex::new(HashMap::new()),
                attachment,
                pending_forwarding: AsyncMutex::new(HashSet::new()),
                forwarding_events: AsyncMutex::new(forwarding_events),
                pending_specs: AsyncMutex::new(HashSet::new()),
                wireguard: AsyncMutex::new(WireGuardUnderlayState::default()),
                wireguard_last_reconcile: AsyncMutex::new(None),
                wireguard_retry_scheduled: AsyncMutex::new(false),
                attachments_root: AsyncMutex::new(None),
                attachment_sync_notify,
                wake: Notify::new(),
                gossip_tx,
            }),
        })
    }

    /// Spawn the reconciliation loop on the current local executor.
    pub fn spawn(&self) {
        self.spawn_forwarding_listener();
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.run().await;
        });
    }

    /// Request an immediate reconcile for the provided network identifier.
    pub async fn schedule_spec_change(&self, network_id: Uuid) {
        let mut guard = self.inner.pending_specs.lock().await;
        let inserted = guard.insert(network_id);
        drop(guard);
        if inserted {
            self.inner.wake.notify_one();
        }
    }

    /// Return the local NodePort manager used by network discovery and public-service publication.
    pub fn nodeport_manager(&self) -> NodePortManager {
        self.inner.discovery.nodeport_manager()
    }

    async fn send_event(&self, event: NetworkEvent) {
        let tx = self.inner.gossip_tx.clone();
        if let Err(err) = tx
            .send(Message::Network {
                id: Uuid::new_v4(),
                event,
            })
            .await
        {
            warn!(target: "network", "failed to broadcast network gossip: {err}");
        }
    }

    /// List every non-deleted network that currently expects a local dataplane reconcile.
    ///
    /// WireGuard failures are global to the node, but deployment gating still happens per network
    /// through `NetworkPeerState`. This helper gives the controller the network identifiers that
    /// must be marked `Error` when encrypted underlay setup is blocked.
    fn active_network_ids(&self) -> Result<Vec<Uuid>> {
        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specs for wireguard scope")?;
        Ok(specs
            .into_iter()
            .filter(|spec| !spec.is_deleted())
            .map(|spec| spec.id)
            .collect())
    }

    /// Snapshot every currently visible cluster peer except the local node.
    ///
    /// This acts as a bootstrap fallback while network peer readiness is still converging and the
    /// scoped WireGuard set has not been derived from shared network state yet.
    fn visible_cluster_peer_ids(&self) -> Result<HashSet<Uuid>> {
        let peers = self
            .inner
            .cluster_registry
            .peer_values_snapshot()
            .context("load cluster peers for wireguard bootstrap")?;
        Ok(peers
            .into_iter()
            .map(|(peer_id, _)| peer_id)
            .filter(|peer_id| *peer_id != self.inner.node_id)
            .collect())
    }

    /// Compute the remote peer set that must participate in the encrypted VXLAN underlay.
    ///
    /// Once network peer readiness has converged, the steady-state scope comes from shared Ready
    /// networks. During bootstrap we fall back to visible cluster peers so multi-node encrypted
    /// networks can establish WireGuard before any node advertises itself Ready.
    fn desired_wireguard_peers(&self) -> Result<HashSet<Uuid>> {
        let scoped = self
            .inner
            .registry
            .wireguard_scope_peers(self.inner.node_id)
            .context("derive scoped wireguard peers")?;
        if !config::wireguard_enabled() || !scoped.is_empty() {
            return Ok(scoped);
        }

        let active_network_ids = self.active_network_ids()?;
        if active_network_ids.is_empty() {
            return Ok(scoped);
        }

        let bootstrap = self.visible_cluster_peer_ids()?;
        if !bootstrap.is_empty() {
            debug!(
                target: "network",
                peers = bootstrap.len(),
                networks = active_network_ids.len(),
                "bootstrapping wireguard peer scope from visible cluster membership until network readiness converges"
            );
        }
        Ok(bootstrap)
    }

    /// Reset cached underlay state and mark active networks as blocked by WireGuard.
    ///
    /// The controller uses peer-state `Error` rows to keep scheduling and deployment from routing
    /// traffic onto plaintext or partially configured underlays.
    async fn fail_wireguard_reconcile(&self, message: &str) -> Result<()> {
        {
            let mut guard = self.inner.wireguard.lock().await;
            *guard = WireGuardUnderlayState::default();
        }

        for network_id in self.active_network_ids()? {
            self.update_peer_state_error(network_id, message.to_string())
                .await?;
        }

        Ok(())
    }

    /// Reconcile the mandatory WireGuard underlay used to encrypt VXLAN traffic.
    ///
    /// When WireGuard is enabled, a network may not advance to `Ready` until its scoped encrypted
    /// underlay is available. Any failure here is surfaced through per-network error state instead
    /// of silently falling back to plaintext.
    async fn reconcile_wireguard_underlay(&self) -> Result<bool> {
        // WireGuard provisioning is expensive (it rewrites interface + routes). The main network
        // reconciliation loop can invoke this helper multiple times per tick (pending specs,
        // pending forwarding, periodic sweep). Debounce here so we only touch kernel WireGuard once
        // per short window.
        let now = std::time::Instant::now();
        {
            let mut guard = self.inner.wireguard_last_reconcile.lock().await;
            if let Some(last) = *guard
                && let Some(remaining) =
                    WIREGUARD_RECONCILE_DEBOUNCE.checked_sub(now.saturating_duration_since(last))
            {
                self.schedule_wireguard_retry(remaining).await;
                return Ok(false);
            }
            *guard = Some(now);
        }

        let desired_peer_ids = match self.desired_wireguard_peers() {
            Ok(peers) => peers,
            Err(err) => {
                let message = format!("failed to derive mandatory wireguard peer scope: {err:#}");
                self.fail_wireguard_reconcile(&message).await?;
                return Err(err.context("derive mandatory wireguard peer scope"));
            }
        };

        let previous = { self.inner.wireguard.lock().await.clone() };
        match wireguard::ensure_wireguard_underlay(
            &self.inner.cluster_registry,
            self.inner.node_id,
            &desired_peer_ids,
            Some(previous),
        )
        .await
        {
            Ok(state) => {
                let mut guard = self.inner.wireguard.lock().await;
                let changed = guard.underlay_active != state.underlay_active
                    || guard.required_peer_count != state.required_peer_count
                    || guard.tunnel_ip != state.tunnel_ip
                    || guard.ifname != state.ifname
                    || guard.configured_peer_ids != state.configured_peer_ids;
                if changed {
                    debug!(
                        target: "network",
                        underlay_active = state.underlay_active,
                        required_peers = state.required_peer_count,
                        ifname = %state.ifname,
                        tunnel_ip = ?state.tunnel_ip,
                        peers = state.configured_peer_ids.len(),
                        "wireguard underlay state updated"
                    );
                }
                *guard = state;
                Ok(changed)
            }
            Err(err) => {
                warn!(
                    target: "network",
                    "wireguard underlay reconcile failed; blocking encrypted network provisioning: {err:#}"
                );
                let message = format!("wireguard underlay reconcile failed: {err:#}");
                self.fail_wireguard_reconcile(&message).await?;
                Err(err.context("reconcile mandatory wireguard underlay"))
            }
        }
    }

    /// Requeue network reconciliation after the WireGuard debounce window elapses.
    ///
    /// Peer metadata can become ready immediately after a failed underlay reconcile. Without a
    /// delayed requeue, the controller would keep the stale underlay state until the slow drift
    /// sweep fires, which unnecessarily delays network readiness by up to one minute. We keep the
    /// retry bounded so persistent kernel/configuration failures still fall back to the normal
    /// periodic sweep instead of spinning forever.
    async fn schedule_wireguard_retry(&self, delay: Duration) {
        let mut guard = self.inner.wireguard_retry_scheduled.lock().await;
        if *guard {
            return;
        }
        *guard = true;
        drop(guard);

        let controller = self.clone();
        tokio::task::spawn_local(async move {
            let mut next_delay = delay;
            for _ in 0..WIREGUARD_RECONCILE_RETRY_LIMIT {
                tokio::time::sleep(next_delay).await;

                if let Err(err) = controller.schedule_active_network_reconcile().await {
                    warn!(
                        target: "network",
                        "failed to requeue network reconcile after wireguard debounce: {err:#}"
                    );
                    break;
                }

                let state = { controller.inner.wireguard.lock().await.clone() };
                if state.underlay_active || state.required_peer_count == 0 {
                    break;
                }

                next_delay = WIREGUARD_RECONCILE_DEBOUNCE;
            }

            let mut guard = controller.inner.wireguard_retry_scheduled.lock().await;
            *guard = false;
        });
    }

    /// Queue every active network for a full reconcile.
    ///
    /// WireGuard underlay transitions can change the required VXLAN device shape without any
    /// accompanying spec update. Scheduling active networks here forces an immediate interface
    /// rebuild instead of waiting for the slow drift sweep to notice the mismatch.
    async fn schedule_active_network_reconcile(&self) -> Result<()> {
        let active = self.active_network_ids()?;
        if active.is_empty() {
            return Ok(());
        }

        let mut pending = self.inner.pending_specs.lock().await;
        let mut inserted = false;
        for network_id in active {
            inserted |= pending.insert(network_id);
        }
        drop(pending);

        if inserted {
            self.inner.wake.notify_one();
        }

        Ok(())
    }

    fn spawn_forwarding_listener(&self) {
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.forwarding_event_loop().await;
        });
    }

    /// Run the event-driven reconciliation loop with a slow drift sweep for external changes.
    async fn run(&self) {
        if let Err(err) = self.reconcile_pending_forwarding().await {
            warn!(
                target: "network",
                "pending forwarding reconcile failed on startup: {err:#}"
            );
        }
        if let Err(err) = self.reconcile_pending_specs().await {
            warn!(
                target: "network",
                "pending spec reconcile failed on startup: {err:#}"
            );
        }

        let mut interval = tokio::time::interval(RECONCILE_DRIFT_INTERVAL);
        let mut attachment_refresh = tokio::time::interval(ATTACHMENT_REFRESH_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = self.reconcile_once().await {
                        warn!(target: "network", "network reconciliation failed: {err:#}");
                    }
                }
                _ = attachment_refresh.tick() => {
                    if let Err(err) = self.refresh_forwarding_from_attachments().await {
                        warn!(
                            target: "network",
                            "attachment forwarding refresh failed: {err:#}"
                        );
                    }
                }
                _ = async {
                    if let Some(notify) = self.inner.attachment_sync_notify.as_ref() {
                        notify.notified().await;
                    } else {
                        future::pending::<()>().await;
                    }
                } => {
                    // Anti-entropy can apply remote attachment rows long before the periodic
                    // attachment refresh would notice them. Refresh forwarding immediately so
                    // first traffic to those newly replicated backends does not wait on the
                    // slow poll cadence.
                    if let Err(err) = self.refresh_forwarding_from_attachments().await {
                        warn!(
                            target: "network",
                            "attachment forwarding refresh after sync failed: {err:#}"
                        );
                    }
                }
                _ = self.inner.wake.notified() => {
                    if let Err(err) = self.reconcile_pending_forwarding().await {
                        warn!(
                            target: "network",
                            "pending forwarding reconcile failed: {err:#}"
                        );
                    }
                    if let Err(err) = self.reconcile_pending_specs().await {
                        warn!(target: "network", "pending spec reconcile failed: {err:#}");
                    }
                }
            }
        }
    }

    async fn reconcile_pending_forwarding(&self) -> Result<()> {
        let pending: Vec<Uuid> = {
            let mut guard = self.inner.pending_forwarding.lock().await;
            guard.drain().collect()
        };
        if pending.is_empty() {
            return Ok(());
        }

        let _ = self.reconcile_wireguard_underlay().await?;

        for network_id in pending {
            let spec_opt = self.inner.registry.get_spec(network_id)?;
            let Some(spec) = spec_opt else {
                continue;
            };

            if let Err(err) = self.reconcile_network(spec).await {
                warn!(
                    target: "network",
                    network = %network_id,
                    "event-triggered network reconcile failed: {err:#}"
                );
                self.update_peer_state_error(network_id, err.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn reconcile_pending_specs(&self) -> Result<()> {
        let queued: Vec<Uuid> = {
            let mut guard = self.inner.pending_specs.lock().await;
            guard.drain().collect()
        };
        if queued.is_empty() {
            return Ok(());
        }

        let _ = self.reconcile_wireguard_underlay().await?;

        for network_id in queued {
            match self.inner.registry.get_spec(network_id) {
                Ok(Some(spec)) => {
                    if spec.is_deleted() {
                        if let Err(err) = self.teardown_deleted_network(&spec).await {
                            warn!(
                                target: "network",
                                network = %network_id,
                                "teardown after gossip failed: {err:#}"
                            );
                        }
                    } else if let Err(err) = self.reconcile_network(spec.clone()).await {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "immediate reconcile failed: {err:#}"
                        );
                        self.update_peer_state_error(network_id, err.to_string())
                            .await?;
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(
                        target: "network",
                        network = %network_id,
                        "failed to load spec for immediate reconcile: {err:#}"
                    );
                }
            }
        }

        Ok(())
    }

    async fn refresh_forwarding_from_attachments(&self) -> Result<()> {
        let root = self
            .inner
            .registry
            .attachments_root_hex()
            .await
            .context("load attachment root hash")?;

        let mut guard = self.inner.attachments_root.lock().await;
        if guard.as_deref() == Some(root.as_str()) {
            return Ok(());
        }
        *guard = Some(root);
        drop(guard);

        if self.reconcile_wireguard_underlay().await? {
            debug!(
                target: "network",
                "wireguard underlay changed during attachment refresh; scheduling full network reconcile"
            );
            self.schedule_active_network_reconcile().await?;
            return Ok(());
        }

        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specs for attachment forwarding refresh")?;

        for mut spec in specs {
            if spec.is_deleted() {
                continue;
            }
            let (mut plan, _) = self.prepare_plan(&mut spec)?;
            self.apply_wireguard_overrides(&mut plan).await?;
            if let Err(err) = self.reconcile_remote_forwarding(&plan).await {
                warn!(
                    target: "network",
                    network = %plan.network_id,
                    "attachment-triggered forwarding reconcile failed: {err:#}"
                );
            }
        }

        Ok(())
    }

    async fn forwarding_event_loop(&self) {
        let receiver = {
            let mut guard = self.inner.forwarding_events.lock().await;
            guard.take()
        };

        let Some(mut receiver) = receiver else {
            return;
        };

        while let Some(event) = receiver.recv().await {
            match event {
                ForwardingEvent::AttachmentReady { network_id } => {
                    // Mark the network for a targeted reconcile so remote FDB entries
                    // are refreshed immediately after the attachment finished configuring.
                    let mut guard = self.inner.pending_forwarding.lock().await;
                    let inserted = guard.insert(network_id);
                    drop(guard);
                    if inserted {
                        self.inner.wake.notify_one();
                    }
                }
            }
        }
    }

    async fn reconcile_once(&self) -> Result<()> {
        let _ = self.reconcile_wireguard_underlay().await?;

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
        let (mut plan, spec_changed) = self.prepare_plan(&mut spec)?;
        self.apply_wireguard_overrides(&mut plan).await?;
        if spec_changed {
            self.inner
                .registry
                .upsert_spec(spec.clone())
                .await
                .context("persist network spec update")?;
            self.send_event(NetworkEvent::Upsert(spec.clone())).await;
        }

        let interface_ctx: NetworkInterfaceContext = (&plan).into();
        let mut retried_after_bpf_conflict = false;
        loop {
            debug!(
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
            self.observe_vxlan_ifindex(&plan).await;
            debug!(
                target: "network",
                network_id = %plan.network_id,
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                "network resources ensured"
            );

            match self.inner.bpf.ensure_network(&spec, &interface_ctx).await {
                Ok(()) => break,
                Err(err) => {
                    if retried_after_bpf_conflict || !Self::is_bpf_link_conflict(&err) {
                        return Err(err.context(format!(
                            "ensure bpf programs for network {}",
                            plan.network_id
                        )));
                    }

                    retried_after_bpf_conflict = true;
                    warn!(
                        target: "network",
                        network = %plan.network_id,
                        bridge = %plan.bridge_name,
                        vxlan = %plan.vxlan_name,
                        "stale eBPF attachments detected, rebuilding interfaces"
                    );

                    if let Err(teardown_err) = self.inner.bpf.teardown_network(&interface_ctx).await
                    {
                        warn!(
                            target: "network",
                            network = %plan.network_id,
                            "failed to detach bpf programs after conflict: {teardown_err:#}"
                        );
                    }

                    if let Err(teardown_err) = self.inner.provisioner.teardown_network(&plan).await
                    {
                        warn!(
                            target: "network",
                            network = %plan.network_id,
                            "failed to teardown network after bpf conflict: {teardown_err:#}"
                        );
                    }

                    continue;
                }
            }
        }

        if let Err(err) = self
            .inner
            .discovery
            .ensure_network(&spec, plan.resolver_ipv4)
            .await
        {
            warn!(
                target: "network",
                network = %plan.network_id,
                "failed to ensure service discovery: {err:#}"
            );
        }

        self.mark_peer_ready(plan.network_id).await?;

        if spec.status != NetworkStatus::Ready {
            let mut updated_spec = spec.clone();
            updated_spec.set_status(NetworkStatus::Ready);
            self.inner
                .registry
                .upsert_spec(updated_spec.clone())
                .await
                .context("update network status to ready")?;
            self.send_event(NetworkEvent::Upsert(updated_spec)).await;
        }

        self.reconcile_remote_forwarding(&plan).await?;

        let mut active = self.inner.active_networks.lock().await;
        active.insert(plan.network_id);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn is_bpf_link_conflict(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            if let Some(sys) = cause.downcast_ref::<SyscallError>() {
                return Self::is_link_create_conflict(sys);
            }
            if let Some(ProgramError::SyscallError(sys)) = cause.downcast_ref::<ProgramError>() {
                return Self::is_link_create_conflict(sys);
            }
            false
        })
    }

    #[cfg(target_os = "linux")]
    fn is_link_create_conflict(sys: &SyscallError) -> bool {
        sys.call == "bpf_link_create"
            && matches!(sys.io_error.raw_os_error(), Some(code) if code == libc::EEXIST)
    }

    #[cfg(not(target_os = "linux"))]
    fn is_bpf_link_conflict(_err: &anyhow::Error) -> bool {
        false
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
            let interface_ctx: NetworkInterfaceContext = (&plan).into();
            if let Err(err) = self.inner.bpf.teardown_network(&interface_ctx).await {
                warn!(
                    target: "network",
                    network = %id,
                    "failed to tear down bpf programs: {err:#}"
                );
            }
            if let Err(err) = self.inner.provisioner.teardown_network(&plan).await {
                warn!(
                    target: "network",
                    "failed to tear down network {}: {err:#}",
                    id
                );
            }
            if let Err(err) = self.inner.discovery.teardown_network(id).await {
                warn!(
                    target: "network",
                    network = %id,
                    "failed to tear down discovery service: {err:#}"
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
        let has_active = {
            let active = self.inner.active_networks.lock().await;
            active.contains(&spec.id)
        };

        let has_peers = !self
            .inner
            .registry
            .list_peer_states(Some(spec.id))?
            .is_empty();

        let has_attachments = !self
            .inner
            .registry
            .list_attachments(Some(spec.id))?
            .is_empty();

        let should_teardown = has_active || has_peers || has_attachments;

        if !should_teardown {
            return Ok(());
        }

        let plan = NetworkPlan::from_id(spec.id);
        let interface_ctx: NetworkInterfaceContext = (&plan).into();
        if let Err(err) = self.inner.bpf.teardown_network(&interface_ctx).await {
            warn!(
                target: "network",
                network = %spec.id,
                "failed to tear down bpf programs for deleted network: {err:#}"
            );
        }
        if let Err(err) = self.inner.provisioner.teardown_network(&plan).await {
            warn!(
                target: "network",
                "failed to tear down deleted network {}: {err:#}",
                spec.id
            );
        }
        if let Err(err) = self.inner.discovery.teardown_network(spec.id).await {
            warn!(
                target: "network",
                network = %spec.id,
                "failed to tear down discovery service for deleted network: {err:#}"
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
            .remove_attachments_for_network(network_id)
            .await
            .context("remove attachments for network")?;

        let peer_states = self
            .inner
            .registry
            .list_peer_states(Some(network_id))
            .context("list peer states for cleanup")?;

        for state in peer_states {
            let _ = self.inner.registry.remove_peer_state(state.id).await;
            self.send_event(NetworkEvent::PeerRemove(state.id)).await;
        }

        {
            let mut guard = self.inner.remote_fdb.lock().await;
            guard.remove(&network_id);
        }

        {
            let mut guard = self.inner.flood_entries.lock().await;
            guard.remove(&network_id);
        }

        {
            let mut guard = self.inner.vxlan_ifindex.lock().await;
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

        changed |= Self::ensure_default_bpf_programs(&mut spec.bpf_programs);
        spec.bpf_programs.sort();

        let resolver_ipv4 = match resolver_ipv4_address(spec, self.inner.node_id) {
            Ok(ip) => Some(ip),
            Err(err) => {
                warn!(
                    target: "network",
                    network = %spec.id,
                    "failed to compute resolver address: {err}"
                );
                None
            }
        };

        let subnet_prefix = match parse_ipv4_cidr(&spec.subnet_cidr) {
            Ok((_, prefix)) => Some(prefix),
            Err(err) => {
                warn!(
                    target: "network",
                    network = %spec.id,
                    subnet = %spec.subnet_cidr,
                    "failed to parse subnet for resolver configuration: {err}"
                );
                None
            }
        };

        let host_access_mac = if resolver_ipv4.is_some() && subnet_prefix.is_some() {
            Some(host_access_mac(spec.id, self.inner.node_id))
        } else {
            None
        };

        let suffix = short_id(spec.id);
        let plan = NetworkPlan {
            network_id: spec.id,
            vxlan_name: format!("mvx-{suffix}"),
            bridge_name: format!("mnt-br-{suffix}"),
            vni: spec.vni,
            mtu: spec.mtu,
            resolver_ipv4,
            subnet_prefix,
            underlay_iface: None,
            underlay_ip: None,
            host_access_mac,
        };

        Ok((plan, changed))
    }

    /// Apply runtime wireguard decisions to the network plan.
    ///
    /// This adjusts MTU and VXLAN underlay selection without mutating the replicated network spec.
    async fn apply_wireguard_overrides(&self, plan: &mut NetworkPlan) -> Result<()> {
        let state = { self.inner.wireguard.lock().await.clone() };
        if config::wireguard_enabled() && state.required_peer_count > 0 && !state.underlay_active {
            anyhow::bail!(
                "wireguard underlay required for {} scoped peers but is not ready yet",
                state.required_peer_count
            );
        }

        if !state.underlay_active {
            return Ok(());
        }

        if let Some(underlay_ip) = state.tunnel_ip {
            plan.underlay_iface = Some(state.ifname);
            plan.underlay_ip = Some(underlay_ip);
            plan.mtu = plan.mtu.min(wireguard::MANTISSA_WIREGUARD_VXLAN_MTU);
        }

        Ok(())
    }

    /// Track the current VXLAN interface index for the network and clear forwarding caches when
    /// the interface is recreated.
    ///
    /// Mantissa keeps in-memory maps of remote FDB and flood targets to avoid repeating netlink
    /// work. When the VXLAN device is deleted/recreated (e.g. underlay switch, bpf conflict
    /// recovery), the kernel state is lost and these caches must be invalidated.
    async fn observe_vxlan_ifindex(&self, plan: &NetworkPlan) {
        let current = match self.inner.provisioner.link_index(&plan.vxlan_name).await {
            Ok(index) => index,
            Err(err) => {
                warn!(
                    target: "network",
                    network = %plan.network_id,
                    vxlan = %plan.vxlan_name,
                    "failed to resolve vxlan ifindex; clearing forwarding caches defensively: {err:#}"
                );
                self.clear_forwarding_caches(plan.network_id).await;
                let mut guard = self.inner.vxlan_ifindex.lock().await;
                guard.remove(&plan.network_id);
                return;
            }
        };

        let mut changed = false;
        {
            let mut guard = self.inner.vxlan_ifindex.lock().await;
            let prev = match current {
                Some(index) => guard.insert(plan.network_id, index),
                None => guard.remove(&plan.network_id),
            };

            if let (Some(prev), Some(now)) = (prev, current) {
                changed = prev != now;
            } else if prev.is_some() && current.is_none() {
                changed = true;
            }
        }

        if changed {
            debug!(
                target: "network",
                network = %plan.network_id,
                vxlan = %plan.vxlan_name,
                "vxlan interface changed; clearing forwarding caches"
            );
            self.clear_forwarding_caches(plan.network_id).await;
        }
    }

    /// Remove in-memory remote forwarding caches for the provided network.
    async fn clear_forwarding_caches(&self, network_id: Uuid) {
        {
            let mut guard = self.inner.remote_fdb.lock().await;
            guard.remove(&network_id);
        }
        {
            let mut guard = self.inner.flood_entries.lock().await;
            guard.remove(&network_id);
        }
    }

    /// Guarantee the dataplane programs required for VIP load-balancing are present with the
    /// correct attach points so LB maps are always created and pinned.
    fn ensure_default_bpf_programs(programs: &mut Vec<BpfProgramSpec>) -> bool {
        let mut changed = false;
        let defaults = default_bpf_programs();
        if defaults.is_empty() {
            return false;
        }

        for default in defaults {
            match programs.iter_mut().find(|p| p.name == default.name) {
                Some(existing) => {
                    if existing.attach_point != default.attach_point {
                        existing.attach_point = default.attach_point;
                        changed = true;
                    }
                }
                None => {
                    programs.push(default);
                    changed = true;
                }
            }
        }

        programs.sort();
        programs.dedup();
        changed
    }

    async fn mark_peer_ready(&self, network_id: Uuid) -> Result<()> {
        if let Some(existing) = self
            .inner
            .registry
            .get_peer_state(network_id, self.inner.node_id)?
            && existing.state == NetworkPeerState::Ready
            && existing.error.is_none()
        {
            return Ok(());
        }

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
            .upsert_peer_state(state.clone())
            .await
            .context("persist peer state ready")?;

        self.send_event(NetworkEvent::PeerUpsert(state)).await;
        Ok(())
    }

    async fn update_peer_state_error(&self, network_id: Uuid, message: String) -> Result<()> {
        if let Some(existing) = self
            .inner
            .registry
            .get_peer_state(network_id, self.inner.node_id)?
            && existing.state == NetworkPeerState::Error
            && existing.error.as_deref() == Some(message.as_str())
        {
            return Ok(());
        }

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
            .upsert_peer_state(state.clone())
            .await
            .context("persist peer state error")?;
        self.send_event(NetworkEvent::PeerUpsert(state)).await;
        Ok(())
    }

    async fn reconcile_remote_forwarding(&self, plan: &NetworkPlan) -> Result<()> {
        let attachments = self
            .inner
            .registry
            .list_attachments(Some(plan.network_id))
            .context("list attachments for forwarding")?;

        let mut desired: HashMap<String, IpAddr> = HashMap::new();
        let mut flood_targets: HashMap<IpAddr, usize> = HashMap::new();
        let mut peer_ip_cache: HashMap<Uuid, Option<IpAddr>> = HashMap::new();

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

            let peer_ip = match self
                .peer_ip_for_node_cached(attachment.node_id, &mut peer_ip_cache)
                .await
            {
                Some(ip) => ip,
                None => continue,
            };

            desired.insert(mac, peer_ip);
            *flood_targets.entry(peer_ip).or_insert(0) += 1;
        }

        if plan.host_access_mac.is_some() {
            let peer_states = self
                .inner
                .registry
                .list_peer_states(Some(plan.network_id))
                .context("list peer states for host access forwarding")?;

            for state in peer_states {
                if state.peer_id == self.inner.node_id {
                    continue;
                }

                if state.state != NetworkPeerState::Ready {
                    continue;
                }

                let peer_ip = match self
                    .peer_ip_for_node_cached(state.peer_id, &mut peer_ip_cache)
                    .await
                {
                    Some(ip) => ip,
                    None => continue,
                };

                // Add deterministic host-access MACs so return traffic to resolver-originated
                // flows stays unicast instead of flooding across every VXLAN peer.
                let mac = format_mac(host_access_mac(plan.network_id, state.peer_id));
                match desired.get(&mac) {
                    Some(existing) if existing == &peer_ip => continue,
                    Some(existing) => {
                        warn!(
                            target: "network",
                            network = %plan.network_id,
                            mac,
                            existing = %existing,
                            candidate = %peer_ip,
                            "host access mac collides with existing forwarding entry"
                        );
                        continue;
                    }
                    None => {}
                }

                desired.insert(mac, peer_ip);
            }
        }

        const FLOOD_MAC: &str = "00:00:00:00:00:00";

        // Reconcile from kernel truth so split/merge churn cannot leave stale FDB entries behind
        // when in-memory caches are dropped (process restart, interface recreation).
        let observed = self
            .inner
            .attachment
            .list_remote_fdb(&plan.vxlan_name)
            .await
            .with_context(|| format!("list remote fdb entries on {}", plan.vxlan_name))?;

        let mut observed_unicast: HashMap<String, IpAddr> = HashMap::new();
        let mut observed_unicast_entries: Vec<(String, IpAddr)> = Vec::new();
        let mut observed_flood: HashSet<IpAddr> = HashSet::new();
        for (mac, dst) in observed {
            if mac == FLOOD_MAC {
                observed_flood.insert(dst);
                continue;
            }
            observed_unicast.insert(mac.clone(), dst);
            observed_unicast_entries.push((mac, dst));
        }

        for (mac, ip) in &desired {
            if observed_unicast.get(mac) == Some(ip) {
                continue;
            }

            let installed = self
                .inner
                .attachment
                .ensure_remote_fdb(&plan.vxlan_name, mac, *ip)
                .await
                .with_context(|| {
                    format!(
                        "ensure remote fdb entry for mac {mac} to {ip} on {}",
                        plan.vxlan_name
                    )
                })?;

            if !installed {
                debug!(
                    target: "network",
                    vxlan = %plan.vxlan_name,
                    mac,
                    dst = %ip,
                    "deferring remote fdb entry; kernel reported unsupported"
                );
            }
        }

        let stale_unicast: Vec<(String, IpAddr)> = observed_unicast_entries
            .into_iter()
            .filter(|(mac, ip)| desired.get(mac) != Some(ip))
            .collect();
        for (mac, ip) in stale_unicast {
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
        }

        for ip in flood_targets.keys() {
            if observed_flood.contains(ip) {
                continue;
            }

            let installed = self
                .inner
                .attachment
                .ensure_flood_entry(&plan.vxlan_name, *ip)
                .await
                .with_context(|| {
                    format!(
                        "ensure broadcast forwarding for {} towards {}",
                        plan.vxlan_name, ip
                    )
                })?;

            if !installed {
                debug!(
                    target: "network",
                    vxlan = %plan.vxlan_name,
                    dst = %ip,
                    "deferring flood entry; kernel reported unsupported"
                );
            }
        }

        let obsolete_flood: Vec<IpAddr> = observed_flood
            .into_iter()
            .filter(|ip| !flood_targets.contains_key(ip))
            .collect();
        for ip in obsolete_flood {
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
        }

        {
            let mut guard = self.inner.remote_fdb.lock().await;
            guard.insert(plan.network_id, desired);
        }
        {
            let mut guard = self.inner.flood_entries.lock().await;
            let entry = guard.entry(plan.network_id).or_default();
            entry.clear();
            entry.extend(flood_targets.into_keys());
        }

        Ok(())
    }

    /// Resolve and memoize one peer underlay destination during a reconcile pass.
    ///
    /// Reconciliation loops frequently reference the same peer across attachment and host-access
    /// paths; caching avoids repeated registry lookups and address parsing within one pass.
    async fn peer_ip_for_node_cached(
        &self,
        peer_id: Uuid,
        cache: &mut HashMap<Uuid, Option<IpAddr>>,
    ) -> Option<IpAddr> {
        if let Some(cached) = cache.get(&peer_id) {
            return *cached;
        }

        let resolved = self.peer_ip_for_node(peer_id).await;
        cache.insert(peer_id, resolved);
        resolved
    }

    /// Resolve the VXLAN underlay destination address to reach `peer_id`.
    ///
    /// When WireGuard underlay is active, we only route peers inside the scoped WireGuard set to
    /// their deterministic tunnel IPv6 addresses. Any peer outside that set is skipped instead of
    /// falling back to the plaintext address because the local VXLAN device is already pinned to
    /// the WireGuard interface.
    async fn peer_ip_for_node(&self, peer_id: Uuid) -> Option<IpAddr> {
        if peer_id == self.inner.node_id {
            return None;
        }

        let state = { self.inner.wireguard.lock().await.clone() };
        if state.underlay_active {
            if !state.configured_peer_ids.contains(&peer_id) {
                debug!(
                    target: "network",
                    peer = %peer_id,
                    "wireguard underlay active but peer is outside the scoped wireguard set; skipping forwarding entry"
                );
                return None;
            }
            if self
                .inner
                .cluster_registry
                .peer_wireguard(peer_id)
                .is_none()
            {
                warn!(
                    target: "network",
                    peer = %peer_id,
                    "wireguard underlay active but peer is missing wireguard metadata; skipping forwarding entry"
                );
                return None;
            }
            return Some(IpAddr::V6(net::wireguard::wireguard_tunnel_ipv6(peer_id)));
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::NetworkController;
    use anyhow::Context;
    use aya::{programs::ProgramError, sys::SyscallError};

    fn make_syscall_error() -> SyscallError {
        SyscallError {
            call: "bpf_link_create",
            io_error: std::io::Error::from_raw_os_error(libc::EEXIST),
        }
    }

    #[test]
    fn detects_syscall_conflict_directly() {
        let err = Err::<(), _>(make_syscall_error())
            .context("attach xdp")
            .unwrap_err();
        assert!(
            NetworkController::is_bpf_link_conflict(&err),
            "expected syscall conflict to be detected"
        );
    }

    #[test]
    fn detects_syscall_conflict_wrapped_in_program_error() {
        let program_err: ProgramError = make_syscall_error().into();
        let err = Err::<(), _>(program_err).context("attach xdp").unwrap_err();
        assert!(
            NetworkController::is_bpf_link_conflict(&err),
            "expected program error conflict to be detected"
        );
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
            resolver_ipv4: None,
            subnet_prefix: None,
            underlay_iface: None,
            underlay_ip: None,
            host_access_mac: None,
        }
    }
}

impl From<&NetworkPlan> for NetworkInterfaceContext {
    fn from(plan: &NetworkPlan) -> Self {
        NetworkInterfaceContext::new(
            plan.network_id,
            plan.bridge_name.clone(),
            plan.vxlan_name.clone(),
        )
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

/// Derive a stable host-access MAC for a node/network pair so peers can program static FDB entries.
///
/// The MAC is locally administered and unicast, avoiding conflicts with hardware addresses while
/// providing a deterministic value for control-plane reconciliation.
fn host_access_mac(network_id: Uuid, node_id: Uuid) -> [u8; 6] {
    let digest = {
        let mut hasher = Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(node_id.as_bytes());
        hasher.update(b"host-access-mac");
        hasher.finalize()
    };

    let mut mac = [0u8; 6];
    mac[0] = 0x02;
    mac[1..].copy_from_slice(&digest.as_bytes()[0..5]);
    mac
}

/// Format a MAC address as a lowercase, colon-delimited string for netlink programming.
fn format_mac(mac: [u8; 6]) -> String {
    format!(
        "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
        mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
    )
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{NetworkPlan, VXLAN_PORT, format_mac};
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
    use rtnetlink::{
        AddressMessageBuilder, Error as RtnetlinkError, Handle, LinkBridge, LinkMessageBuilder,
        LinkUnspec, LinkVeth, LinkVxlan, new_connection,
    };
    use std::fs;
    use std::mem;
    use std::net::{IpAddr, Ipv4Addr};
    use std::process::Command;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tracing::{debug, info, warn};

    #[derive(Clone)]
    pub struct NetworkProvisioner {
        handle: Option<Handle>,
        underlay: Arc<AsyncMutex<Option<(u32, IpAddr)>>>,
    }

    const IFLA_PROTINFO: u16 = 12;
    const BRPORT_ATTR_HAIRPIN_MODE: u16 = 4;
    const BRPORT_ATTR_LEARNING: u16 = 8;
    const BRPORT_ATTR_UNICAST_FLOOD: u16 = 9;
    const BRPORT_ATTR_NEIGH_SUPPRESS: u16 = 32;
    const BRPORT_ATTR_MULTICAST_FLOOD: u16 = 27;
    const BRPORT_ATTR_BROADCAST_FLOOD: u16 = 30;

    impl NetworkProvisioner {
        pub fn new() -> Result<Self> {
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

        fn handle(&self) -> Option<&Handle> {
            self.handle.as_ref()
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
                vni = plan.vni,
                mtu = plan.mtu,
                "provisioner: ensuring kernel interfaces"
            );
            let vxlan_index = self
                .ensure_vxlan(plan)
                .await
                .with_context(|| format!("ensure vxlan interface {}", plan.vxlan_name))?;
            debug!(
                target: "network",
                vxlan = %plan.vxlan_name,
                vxlan_index,
                "provisioner: vxlan interface ready"
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

            self.attach_master(vxlan_index, bridge_index)
                .await
                .with_context(|| {
                    format!(
                        "attach vxlan {} (idx {}) to bridge {} (idx {})",
                        plan.vxlan_name, vxlan_index, plan.bridge_name, bridge_index
                    )
                })?;

            self.configure_bridge_port(vxlan_index, bridge_index, &plan.vxlan_name)
                .await
                .with_context(|| {
                    format!(
                        "configure bridge port for vxlan {} (idx {}) on bridge {} (idx {})",
                        plan.vxlan_name, vxlan_index, plan.bridge_name, bridge_index
                    )
                })?;

            let host_access = if plan.resolver_ipv4.is_some() && plan.subnet_prefix.is_some() {
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

            self.set_up(vxlan_index).await.with_context(|| {
                format!("bring link {} (idx {}) up", plan.vxlan_name, vxlan_index)
            })?;
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
                if let Err(err) = self.configure_arp_tuning(&plan.bridge_name) {
                    debug!(
                        target: "network",
                        iface = %plan.bridge_name,
                        "failed to apply bridge arp tuning (continuing): {err:#}"
                    );
                }
                if let Err(err) = self.configure_arp_tuning(&peer_ifname) {
                    debug!(
                        target: "network",
                        iface = %peer_ifname,
                        "failed to apply host-access peer arp tuning (continuing): {err:#}"
                    );
                }
                if let Err(err) = self.configure_arp_tuning(&host_ifname) {
                    debug!(
                        target: "network",
                        iface = %host_ifname,
                        "failed to apply host-access arp tuning (continuing): {err:#}"
                    );
                }
            }

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

            if let (Some(ip), Some(prefix)) = (plan.resolver_ipv4, plan.subnet_prefix) {
                let Some((host_index, _peer_index)) = host_access else {
                    return Err(anyhow!(
                        "host access veth missing despite resolver address being configured"
                    ));
                };
                let (host_ifname, _peer_ifname) = Self::host_access_ifnames(plan.network_id);

                // Older deployments assigned the resolver address to the bridge device. That makes
                // host-originated overlay traffic (including VIP flows) bypass tc-ingress and
                // therefore miss ARP + DNAT handling. Move the IP to the host-access veth.
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
                "provisioner: kernel interfaces ensured"
            );
            Ok(())
        }

        /// Ensure an IPv4 address exists on the provided link using a replace operation.
        ///
        /// Mantissa uses this to place the per-network resolver address on the interface that
        /// should own the connected route for the overlay subnet.
        async fn ensure_interface_address(
            &self,
            link_index: u32,
            ip: Ipv4Addr,
            prefix: u8,
            link_name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };
            handle
                .address()
                .add(link_index, IpAddr::V4(ip), prefix)
                .replace()
                .execute()
                .await
                .with_context(|| format!("assign resolver {ip}/{prefix} on {link_name}"))
        }

        /// Remove stale IPv4 addresses from a dedicated host-access interface.
        ///
        /// Split/merge transitions can move a network onto a new resolver address while reusing
        /// the same `mnhost-*` link name. We must delete old addresses first so the kernel does
        /// not keep multiple /16s on the interface and pick an unexpected source IP for overlay
        /// ARP + health probe traffic.
        async fn remove_stale_interface_addresses(
            &self,
            link_index: u32,
            keep_ip: Ipv4Addr,
            keep_prefix: u8,
            link_name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

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
                if message.header.family != AddressFamily::Inet {
                    continue;
                }

                let mut address: Option<Ipv4Addr> = None;
                for attr in message.attributes.iter() {
                    match attr {
                        AddressAttribute::Address(IpAddr::V4(ip))
                        | AddressAttribute::Local(IpAddr::V4(ip)) => {
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

        /// Apply ARP flux mitigation to an interface so it does not answer for foreign IPs.
        ///
        /// We use this on the overlay bridge and host-access veth to ensure ARP replies come from
        /// the interface that actually owns the address, preventing peers from caching the bridge
        /// MAC for host-access IPs.
        fn configure_arp_tuning(&self, iface: &str) -> Result<()> {
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

        /// Broadcast an ARP announcement for the host-access IP so peers refresh stale
        /// neighbor entries after the resolver address moves off the bridge and onto the
        /// host veth.
        async fn announce_host_access_ip(
            &self,
            host_index: u32,
            ip: Ipv4Addr,
            mac: [u8; 6],
            link_name: &str,
        ) -> Result<()> {
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

        /// Remove the specified IPv4 address from the provided link if present.
        ///
        /// This enables safe migrations where the resolver IP used to live on the bridge device
        /// but now should move onto the host-access veth so host traffic hits tc-ingress programs.
        async fn remove_interface_address(
            &self,
            link_index: u32,
            ip: Ipv4Addr,
            prefix: u8,
            link_name: &str,
        ) -> Result<()> {
            let Some(handle) = self.handle() else {
                return Ok(());
            };

            let msg = AddressMessageBuilder::<Ipv4Addr>::new()
                .index(link_index)
                .address(ip, prefix)
                .build();
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
                    handle.link().del(index).execute().await.with_context(|| {
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
                let mut forced_underlay_name: Option<String> = None;
                let (underlay_index, underlay_ip) = if let (Some(ifname), Some(ip)) =
                    (plan.underlay_iface.as_deref(), plan.underlay_ip)
                {
                    forced_underlay_name = Some(ifname.to_string());
                    match self.find_link(ifname).await? {
                        Some(index) => (index, ip),
                        None => {
                            warn!(
                                target: "network",
                                attempt,
                                underlay = ifname,
                                "requested wireguard underlay interface missing; falling back to detected underlay"
                            );
                            forced_underlay_name = None;
                            self.underlay_info()
                                .await
                                .context("resolve underlay interface for vxlan")?
                        }
                    }
                } else {
                    self.underlay_info()
                        .await
                        .context("resolve underlay interface for vxlan")?
                };

                let underlay_name = match forced_underlay_name {
                    Some(name) => name,
                    None => match self.link_name(underlay_index).await {
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
                    },
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
            debug!(
                target: "network",
                link = %name,
                link_index = index,
                mtu,
                "provisioner: updating mtu"
            );
            handle
                .link()
                .set(LinkUnspec::new_with_index(index).mtu(mtu).build())
                .execute()
                .await
                .with_context(|| format!("set mtu {mtu} on link {name} (index {index})"))?;
            debug!(
                target: "network",
                link = %name,
                link_index = index,
                mtu,
                "provisioner: mtu updated"
            );
            Ok(())
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

            debug!(
                target: "network",
                link = %link_name,
                link_index,
                bridge = %master_name,
                bridge_index = master_index,
                "provisioner: attaching link to bridge"
            );
            handle
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
            debug!(
                target: "network",
                link = %link_name,
                link_index,
                bridge = %master_name,
                bridge_index = master_index,
                "provisioner: link attached to bridge"
            );
            Ok(())
        }

        /// Resolve the kernel interface index for the provided link name.
        ///
        /// This is used by higher-level controllers to detect when interfaces have been recreated
        /// (e.g. underlay changes) so they can invalidate any cached forwarding state.
        pub async fn link_index(&self, name: &str) -> Result<Option<u32>> {
            self.find_link(name).await
        }

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
                if name == MANTISSA_WIREGUARD_IFNAME {
                    debug!(
                        target: "network",
                        "skipping underlay candidate {name} (index {index}) because it is managed by wireguard"
                    );
                    continue;
                }

                let mut addr_stream = handle
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
                            let ip = *addr;
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

        /// Return `None` on unsupported platforms, since no kernel interfaces are created.
        pub async fn link_index(&self, _name: &str) -> Result<Option<u32>> {
            Ok(None)
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
