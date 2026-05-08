use crate::config;
use crate::gossip::Message;
use crate::network::allocator::{parse_overlay_cidr, resolver_ip_address};
use crate::network::attachment::{PlatformAttachmentProvisioner, host_iface_name};
use crate::network::bpf::{NetworkBpfManager, NetworkInterfaceContext, overlay_bpf_program_specs};
use crate::network::defaults::merge_default_bpf_programs;
use crate::network::discovery::ServiceDiscovery;
use crate::network::events::ForwardingEvent;
use crate::network::naming::{
    collect_orphaned_network_suffixes, is_managed_overlay_link_name, managed_interface_suffix,
};
use crate::network::nodeport::NodePortManager;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    BpfProgramSpec, NetworkAttachmentState, NetworkDriver, NetworkEvent, NetworkPeerState,
    NetworkPeerStateValue, NetworkSpecValue, NetworkStatus,
};
use crate::network::wireguard::{self, WireGuardUnderlayState};
use crate::registry::Registry;
use crate::services::registry::ServiceRegistry;
use crate::store::replicated::workloads::WorkloadStore;
use anyhow::{Context, Result, anyhow};
use async_channel::Sender;
#[cfg(target_os = "linux")]
use aya::{programs::ProgramError, sys::SyscallError};
use std::collections::{HashMap, HashSet};
use std::future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc::UnboundedReceiver};
use tokio::time::Duration;
use tracing::{debug, warn};
use uuid::Uuid;

/// Periodic reconciliation interval for drift detection when no events are pending.
const RECONCILE_DRIFT_INTERVAL: Duration = Duration::from_secs(60);
/// Frequency to check for attachment updates that require forwarding refresh.
const ATTACHMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// Retry interval for service-discovery startup after local interface programming.
const DISCOVERY_RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// Number of short retries before discovery startup falls back to the normal drift sweep.
const DISCOVERY_RETRY_ATTEMPTS: usize = 10;
/// Default overlay MTU used when a network spec omits an explicit MTU.
pub(crate) const DEFAULT_MTU: u32 = 1450;
/// Default MTU for node-local bridge networks when the operator omits an explicit value.
pub(crate) const DEFAULT_BRIDGE_MTU: u32 = 1500;
#[cfg(target_os = "linux")]
/// UDP destination port used by Linux VXLAN devices created for Mantissa overlays.
const VXLAN_PORT: u16 = 4789;
/// Minimum interval between expensive WireGuard interface reconciles.
const WIREGUARD_RECONCILE_DEBOUNCE: Duration = Duration::from_secs(1);
/// Number of short retries scheduled after WireGuard reconciliation is debounce-skipped.
const WIREGUARD_RECONCILE_RETRY_LIMIT: usize = 3;
/// Number of checks used when waiting for netlink link state to converge.
const LINK_STATE_SETTLE_ATTEMPTS: usize = 10;
/// Delay between netlink link-state convergence checks.
const LINK_STATE_SETTLE_DELAY: Duration = Duration::from_millis(20);
/// Number of netlink update retries after a transient link-state failure.
const LINK_STATE_UPDATE_RETRIES: usize = 2;

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

/// Construction inputs for one network controller instance.
///
/// The controller owns several long-lived stores and channels. Grouping those dependencies keeps
/// startup readable and avoids a constructor whose positional arguments are hard to audit.
pub struct NetworkControllerInit {
    pub registry: NetworkRegistry,
    pub cluster_registry: Registry,
    pub workload_store: WorkloadStore,
    pub service_registry: ServiceRegistry,
    pub node_id: Uuid,
    pub node_name: String,
    pub gossip_tx: Sender<Message>,
    pub forwarding_events: Option<UnboundedReceiver<ForwardingEvent>>,
    pub attachment_sync_notify: Option<Arc<Notify>>,
}

#[cfg(target_os = "linux")]
/// Return the canonical eBPF program bundle when dataplane attachment is enabled on Linux.
fn default_bpf_programs() -> Vec<BpfProgramSpec> {
    if !config::bpf_attach_enabled() {
        return Vec::new();
    }

    overlay_bpf_program_specs()
}

#[cfg(not(target_os = "linux"))]
/// Return no default eBPF programs on unsupported platforms.
fn default_bpf_programs() -> Vec<BpfProgramSpec> {
    Vec::new()
}

impl NetworkController {
    #[allow(clippy::arc_with_non_send_sync)]
    /// Construct the network controller and all local platform adapters used by reconciliation.
    pub fn new(init: NetworkControllerInit) -> Result<Self> {
        let NetworkControllerInit {
            registry,
            cluster_registry,
            workload_store,
            service_registry,
            node_id,
            node_name,
            gossip_tx,
            forwarding_events,
            attachment_sync_notify,
        } = init;
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
            node_id,
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

    /// Spawn the long-running controller tasks on the current local executor.
    ///
    /// Bootstrap keeps the returned join handles so headless restart tests can
    /// abort the network controller cleanly before starting a replacement node
    /// with the same persisted state.
    pub fn spawn(&self) -> Vec<tokio::task::JoinHandle<()>> {
        let mut tasks = Vec::with_capacity(2);
        tasks.push(self.spawn_forwarding_listener());
        let controller = self.clone();
        tasks.push(tokio::task::spawn_local(async move {
            controller.run().await;
        }));
        tasks
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

    /// Refresh discovery-derived VIP and NodePort publication for one network immediately.
    pub async fn refresh_publication(&self, network_id: Uuid) {
        if let Err(err) = self.inner.discovery.refresh_network(network_id).await {
            warn!(
                target: "network",
                network = %network_id,
                "publication refresh failed: {err:#}"
            );
        }
    }

    /// Return the local NodePort manager used by network discovery and public-service publication.
    pub fn nodeport_manager(&self) -> NodePortManager {
        self.inner.discovery.nodeport_manager()
    }

    /// Publish a network event onto the gossip plane so peers converge replicated network state.
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

    /// List every active VXLAN network that needs encrypted underlay reconciliation.
    fn active_vxlan_network_ids(&self) -> Result<Vec<Uuid>> {
        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specs for vxlan scope")?;
        Ok(specs
            .into_iter()
            .filter(|spec| !spec.is_deleted() && spec.driver.requires_wireguard_underlay())
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

        let active_network_ids = self.active_vxlan_network_ids()?;
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

        for network_id in self.active_vxlan_network_ids()? {
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
                crate::observability::metrics::set_wireguard_underlay(
                    state.underlay_active,
                    state.required_peer_count,
                    state.configured_peer_ids.len(),
                );
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
                crate::observability::metrics::record_network_reconcile_failure("startup_state");
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

    /// Spawn the attachment-forwarding event listener that reacts to workload-network changes.
    fn spawn_forwarding_listener(&self) -> tokio::task::JoinHandle<()> {
        let controller = self.clone();
        tokio::task::spawn_local(async move {
            controller.forwarding_event_loop().await;
        })
    }

    /// Tear down discovery-owned per-network resources before a headless restart.
    ///
    /// The headless restart harness reuses the same node identity and durable
    /// state in one process. Stopping discovery listeners explicitly here keeps
    /// the replacement node from racing stale DNS sockets or NodePort
    /// publication left behind by the previous runtime instance.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner
            .discovery
            .shutdown()
            .await
            .context("shut down service discovery before headless restart")
    }

    /// Queue every persisted non-deleted network for one startup reconcile.
    ///
    /// The controller rebuilds local interfaces, discovery listeners, and NodePort publication
    /// from durable replicated state after daemon restart. Enqueuing the known specs explicitly
    /// keeps that recovery path deterministic instead of relying on the slow drift sweep.
    async fn queue_startup_spec_reconcile(&self) -> Result<usize> {
        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specs for startup reconcile")?;

        let mut pending = self.inner.pending_specs.lock().await;
        let mut inserted = 0usize;
        for spec in specs {
            if spec.is_deleted() {
                continue;
            }
            if pending.insert(spec.id) {
                inserted = inserted.saturating_add(1);
            }
        }
        Ok(inserted)
    }

    /// Mark every persisted local network as `Configuring` during startup recovery.
    ///
    /// Peer readiness is durable replicated state, so a daemon restart can otherwise leave a
    /// stale local `Ready` row visible to the rest of the cluster until reconciliation runs. That
    /// would let discovery keep routing traffic to attachments whose local bridge, BPF, or
    /// runtime-facing network path has not been rebuilt yet.
    async fn mark_startup_networks_configuring(&self) -> Result<usize> {
        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specs for startup peer-state demotion")?;

        let mut updated = 0usize;
        for spec in specs {
            if spec.is_deleted() {
                continue;
            }

            if self.mark_peer_configuring(spec.id).await? {
                updated = updated.saturating_add(1);
            }
        }

        Ok(updated)
    }

    /// Run the event-driven reconciliation loop with a slow drift sweep for external changes.
    async fn run(&self) {
        match self.mark_startup_networks_configuring().await {
            Ok(updated) if updated > 0 => {
                debug!(
                    target: "network",
                    updated,
                    "marked persisted local networks as configuring during startup recovery"
                );
            }
            Ok(_) => {}
            Err(err) => {
                crate::observability::metrics::record_network_reconcile_failure("startup_specs");
                warn!(
                    target: "network",
                    "failed to demote persisted peer readiness on startup: {err:#}"
                );
            }
        }

        match self.queue_startup_spec_reconcile().await {
            Ok(queued) if queued > 0 => {
                debug!(
                    target: "network",
                    queued,
                    "queued persisted network specs for startup reconcile"
                );
            }
            Ok(_) => {}
            Err(err) => {
                crate::observability::metrics::record_network_reconcile_failure("wireguard");
                warn!(
                    target: "network",
                    "failed to queue persisted network specs on startup: {err:#}"
                );
            }
        }

        if let Err(err) = self.reconcile_pending_forwarding().await {
            crate::observability::metrics::record_network_reconcile_failure("startup_forwarding");
            warn!(
                target: "network",
                "pending forwarding reconcile failed on startup: {err:#}"
            );
        }
        if let Err(err) = self.reconcile_pending_specs().await {
            crate::observability::metrics::record_network_reconcile_failure(
                "startup_pending_specs",
            );
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
                        crate::observability::metrics::record_network_reconcile_failure("drift");
                        warn!(target: "network", "network reconciliation failed: {err:#}");
                    }
                }
                _ = attachment_refresh.tick() => {
                    if let Err(err) = self.refresh_forwarding_from_attachments().await {
                        crate::observability::metrics::record_network_reconcile_failure("attachment_refresh");
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
                        crate::observability::metrics::record_network_reconcile_failure("attachment_sync");
                        warn!(
                            target: "network",
                            "attachment forwarding refresh after sync failed: {err:#}"
                        );
                    }
                }
                _ = self.inner.wake.notified() => {
                    if let Err(err) = self.reconcile_pending_forwarding().await {
                        crate::observability::metrics::record_network_reconcile_failure("pending_forwarding");
                        warn!(
                            target: "network",
                            "pending forwarding reconcile failed: {err:#}"
                        );
                    }
                    if let Err(err) = self.reconcile_pending_specs().await {
                        crate::observability::metrics::record_network_reconcile_failure("pending_specs");
                        warn!(target: "network", "pending spec reconcile failed: {err:#}");
                    }
                }
            }
        }
    }

    /// Reconcile forwarding state for networks queued by attachment or peer-state changes.
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
                self.update_peer_state_error(network_id, format!("{err:#}"))
                    .await?;
            }
        }

        Ok(())
    }

    /// Reconcile network specs that were explicitly queued by config, gossip, or startup.
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
                        self.update_peer_state_error(network_id, format!("{err:#}"))
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

    /// Refresh remote forwarding when the attachment store root changes through anti-entropy.
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
            if spec.driver.supports_remote_forwarding() {
                self.apply_wireguard_overrides(&mut plan).await?;
                self.inner
                    .provisioner
                    .apply_plan_underlay_constraints(&mut plan)
                    .await?;
                if let Err(err) = self.reconcile_remote_forwarding(&plan).await {
                    warn!(
                        target: "network",
                        network = %plan.network_id,
                        "attachment-triggered forwarding reconcile failed: {err:#}"
                    );
                }
            } else {
                self.clear_forwarding_caches(plan.network_id).await;
                self.refresh_publication(plan.network_id).await;
            }
        }

        Ok(())
    }

    /// Consume workload attachment events and queue the affected network for forwarding reconcile.
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
                    // Discovery-derived VIP and NodePort publication depend on attachment
                    // readiness as well as the publication bit. Refresh immediately so a service
                    // whose publication intent arrived before the attachment became Ready does not
                    // wait for the periodic discovery tick before exposing the backend.
                    self.refresh_publication(network_id).await;
                }
                ForwardingEvent::TrafficPublicationChanged { network_id } => {
                    // Refresh discovery-derived VIP and NodePort publication immediately after a
                    // service attachment becomes publishable or is withdrawn, so backend
                    // eligibility and operator-facing public endpoint status do not wait for the
                    // background discovery tick.
                    self.refresh_publication(network_id).await;
                }
            }
        }
    }

    /// Run one full drift reconcile across all known network specs and stale active networks.
    async fn reconcile_once(&self) -> Result<()> {
        let _ = self.reconcile_wireguard_underlay().await?;

        let specs = self
            .inner
            .registry
            .list_specs()
            .context("list network specifications")?;

        let desired: HashSet<Uuid> = specs
            .iter()
            .filter(|spec| !spec.is_deleted())
            .map(|spec| spec.id)
            .collect();

        if let Err(err) = self
            .inner
            .provisioner
            .cleanup_orphaned_network_links(&desired)
            .await
        {
            warn!(
                target: "network",
                "failed to clean orphaned kernel network interfaces: {err:#}"
            );
        }

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

            if let Err(err) = self.reconcile_network(spec.clone()).await {
                warn!(
                    target: "network",
                    "failed to reconcile network {} ({}): {err:#}",
                    spec.name,
                    spec.id
                );
                self.update_peer_state_error(spec.id, format!("{err:#}"))
                    .await?;
            }
        }

        self.teardown_removed_networks(&desired).await?;
        Ok(())
    }

    /// Reconcile one active network from replicated spec through local dataplane readiness.
    async fn reconcile_network(&self, mut spec: NetworkSpecValue) -> Result<()> {
        let (mut plan, spec_changed) = self.prepare_plan(&mut spec)?;
        if spec.driver.requires_wireguard_underlay() {
            self.apply_wireguard_overrides(&mut plan).await?;
            self.inner
                .provisioner
                .apply_plan_underlay_constraints(&mut plan)
                .await?;
        }
        if spec_changed {
            self.inner
                .registry
                .upsert_spec(spec.clone())
                .await
                .context("persist network spec update")?;
            self.send_event(NetworkEvent::Upsert(spec.clone())).await;
        }

        let interface_ctx = self.build_interface_context(&plan)?;
        let mut retried_after_bpf_conflict = false;
        loop {
            debug!(
                target: "network",
                network_id = %plan.network_id,
                node_id = %self.inner.node_id,
                node = %self.inner.node_name,
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                driver = ?plan.driver,
                vni = plan.vni,
                mtu = plan.mtu,
                "ensuring network resources"
            );
            self.inner
                .provisioner
                .ensure_network(&plan)
                .await
                .with_context(|| format!("ensure network {}", plan.network_id))?;
            if plan.uses_vxlan() {
                self.observe_vxlan_ifindex(&plan).await;
            }
            debug!(
                target: "network",
                network_id = %plan.network_id,
                vxlan = %plan.vxlan_name,
                bridge = %plan.bridge_name,
                driver = ?plan.driver,
                "network resources ensured"
            );

            if self
                .inner
                .bpf
                .requires_reload(&spec, &interface_ctx)
                .await
                .context("determine whether bpf reconcile requires local reload")?
            {
                self.prepare_for_dataplane_rebuild(plan.network_id).await?;
            }

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
                    self.prepare_for_dataplane_rebuild(plan.network_id).await?;
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

        if self.inner.provisioner.supports_resolver_bind() {
            if let Err(err) = self.ensure_service_discovery(&spec, plan.resolver_ip).await {
                warn!(
                    target: "network",
                    network = %plan.network_id,
                    "failed to ensure service discovery: {err:#}"
                );
            } else if let Err(err) = self.inner.discovery.refresh_network(plan.network_id).await {
                warn!(
                    target: "network",
                    network = %plan.network_id,
                    "failed to refresh service discovery state after startup: {err:#}"
                );
            }
        } else {
            if let Err(err) = self.inner.discovery.teardown_network(spec.id).await {
                warn!(
                    target: "network",
                    network = %plan.network_id,
                    "failed to clear service discovery while network provisioner is unavailable: {err:#}"
                );
            }
            debug!(
                target: "network",
                network = %plan.network_id,
                "skipping service discovery because resolver addresses cannot be bound without kernel networking"
            );
        }

        if spec.driver.supports_remote_forwarding() {
            self.reconcile_remote_forwarding(&plan).await?;
        } else {
            self.clear_forwarding_caches(plan.network_id).await;
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

        self.refresh_publication(plan.network_id).await;

        let mut active = self.inner.active_networks.lock().await;
        active.insert(plan.network_id);
        Ok(())
    }

    /// Start or rebuild the per-network discovery listener with a short local retry window.
    ///
    /// Immediately after the controller creates or repairs the host-access interface, the kernel
    /// can briefly report the resolver address as not yet bindable. Retrying here keeps restart
    /// recovery on the fast path instead of waiting for the slow drift sweep to retry discovery a
    /// full minute later.
    async fn ensure_service_discovery(
        &self,
        spec: &NetworkSpecValue,
        resolver_ip: Option<IpAddr>,
    ) -> Result<()> {
        let mut last_error = None;
        for attempt in 0..DISCOVERY_RETRY_ATTEMPTS {
            match self.inner.discovery.ensure_network(spec, resolver_ip).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    last_error = Some(err);
                    if attempt + 1 < DISCOVERY_RETRY_ATTEMPTS {
                        tokio::time::sleep(DISCOVERY_RETRY_INTERVAL).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("discovery retry loop did not run")))
    }

    /// Build the current bridge-port attachment context for one network.
    ///
    /// Bridge tc programs must be attached to every local bridge-facing port that can carry
    /// service-VIP traffic: the VXLAN device, the host-access peer, and any local `mnth-*`
    /// task-attachment veths currently present for the network.
    fn build_interface_context(&self, plan: &NetworkPlan) -> Result<NetworkInterfaceContext> {
        let attachment_ifnames = self
            .inner
            .registry
            .list_attachments(Some(plan.network_id))?
            .into_iter()
            .map(|attachment| host_iface_name(attachment.id));

        Ok(NetworkInterfaceContext::from(plan).with_attachment_host_ifnames(attachment_ifnames))
    }

    #[cfg(target_os = "linux")]
    /// Detect stale eBPF link conflicts that can be recovered by rebuilding local dataplane state.
    fn is_bpf_link_conflict(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            if let Some(sys) = cause.downcast_ref::<SyscallError>() {
                return Self::is_stale_bpf_attach_conflict(sys);
            }
            if let Some(ProgramError::SyscallError(sys)) = cause.downcast_ref::<ProgramError>() {
                return Self::is_stale_bpf_attach_conflict(sys);
            }
            false
        })
    }

    #[cfg(target_os = "linux")]
    /// Classify Linux syscall failures that mean a previous BPF attachment is still present.
    fn is_stale_bpf_attach_conflict(sys: &SyscallError) -> bool {
        match sys.io_error.raw_os_error() {
            Some(code) if code == libc::EEXIST => sys.call == "bpf_link_create",
            // Restarted daemons can hit EBUSY while reattaching XDP to interfaces that still
            // carry the previous process' stale hook. Treat that as the same stale-attachment
            // class so reconciliation rebuilds the dataplane instead of leaving the network in
            // error until a manual detach happens.
            Some(code) if code == libc::EBUSY => true,
            _ => false,
        }
    }

    #[cfg(not(target_os = "linux"))]
    /// Non-Linux platforms never run eBPF attachment recovery.
    fn is_bpf_link_conflict(_err: &anyhow::Error) -> bool {
        false
    }

    /// Tear down local runtime state for networks no longer present in the active spec set.
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
            // Stop the discovery loop before detaching dataplane state so periodic VIP refreshes
            // cannot race teardown and try to heal maps that are intentionally being removed.
            if let Err(err) = self.inner.discovery.teardown_network(id).await {
                warn!(
                    target: "network",
                    network = %id,
                    "failed to tear down discovery service: {err:#}"
                );
            }
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

            self.cleanup_network_state(id)
                .await
                .context("cleanup network state for deleted network")?;
            active.remove(&id);
        }

        Ok(())
    }

    /// Tear down local runtime and replicated rows for a network whose spec is tombstoned.
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
        // Stop the discovery loop before detaching dataplane state so periodic VIP refreshes
        // cannot race teardown and try to heal maps that are intentionally being removed.
        if let Err(err) = self.inner.discovery.teardown_network(spec.id).await {
            warn!(
                target: "network",
                network = %spec.id,
                "failed to tear down discovery service for deleted network: {err:#}"
            );
        }
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

        self.cleanup_network_state(spec.id)
            .await
            .context("cleanup network state for deleted spec")?;

        let mut active = self.inner.active_networks.lock().await;
        active.remove(&spec.id);
        Ok(())
    }

    /// Remove replicated attachment, peer, and in-memory forwarding state for a deleted network.
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

    /// Normalize one network spec and derive the local deterministic dataplane plan.
    fn prepare_plan(&self, spec: &mut NetworkSpecValue) -> Result<(NetworkPlan, bool)> {
        let mut changed = false;

        // Normalize defaults to keep the CRDT state consistent across nodes.
        if spec.driver == NetworkDriver::Vxlan && spec.vni == 0 {
            let computed_vni = compute_deterministic_vni(spec.id);
            spec.vni = computed_vni;
            changed = true;
        } else if spec.driver == NetworkDriver::Bridge && spec.vni != 0 {
            spec.vni = 0;
            changed = true;
        }

        if spec.mtu == 0 {
            spec.mtu = match spec.driver {
                NetworkDriver::Vxlan => DEFAULT_MTU,
                NetworkDriver::Bridge => DEFAULT_BRIDGE_MTU,
            };
            changed = true;
        }

        if spec.driver == NetworkDriver::Vxlan {
            changed |= Self::ensure_default_bpf_programs(&mut spec.bpf_programs);
        } else if !spec.bpf_programs.is_empty() {
            spec.bpf_programs.clear();
            changed = true;
        }
        spec.bpf_programs.sort();

        let resolver_ip = match resolver_ip_address(spec, self.inner.node_id) {
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

        let subnet_prefix = match parse_overlay_cidr(&spec.subnet_cidr) {
            Ok(subnet) => Some(subnet.prefix),
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

        let host_access_mac = if resolver_ip.is_some() && subnet_prefix.is_some() {
            Some(host_access_mac(spec.id, self.inner.node_id))
        } else {
            None
        };

        let suffix = managed_interface_suffix(spec.id);
        let plan = NetworkPlan {
            network_id: spec.id,
            driver: spec.driver,
            vxlan_name: format!("mvx-{suffix}"),
            bridge_name: format!("mnt-br-{suffix}"),
            vni: spec.vni,
            mtu: spec.mtu,
            resolver_ip,
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

    /// Guarantee each required dataplane attach point has a declared program.
    fn ensure_default_bpf_programs(programs: &mut Vec<BpfProgramSpec>) -> bool {
        let defaults = default_bpf_programs();
        if defaults.is_empty() {
            return false;
        }

        let original = std::mem::take(programs);
        let merged = merge_default_bpf_programs(defaults, original.clone());
        let changed = original != merged;
        *programs = merged;
        changed
    }

    /// Mark the local peer ready after network, BPF, discovery, and forwarding have converged.
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

    /// Persist the local peer as `Configuring` so discovery can withdraw local backends.
    ///
    /// The controller uses this before startup recovery and before destructive dataplane reloads.
    /// Returning `true` means a replicated state change was emitted; `false` means the peer was
    /// already in the desired `Configuring` state without an error payload.
    async fn mark_peer_configuring(&self, network_id: Uuid) -> Result<bool> {
        if let Some(existing) = self
            .inner
            .registry
            .get_peer_state(network_id, self.inner.node_id)?
            && existing.state == NetworkPeerState::Configuring
            && existing.error.is_none()
        {
            return Ok(false);
        }

        let mut state = NetworkPeerStateValue::new(
            network_id,
            self.inner.node_id,
            self.inner.node_name.clone(),
            NetworkPeerState::Configuring,
            None,
        );
        state.touch();

        self.inner
            .registry
            .upsert_peer_state(state.clone())
            .await
            .context("persist peer state configuring")?;
        self.send_event(NetworkEvent::PeerUpsert(state)).await;
        Ok(true)
    }

    /// Withdraw local routability before rebuilding bridge, BPF, or discovery state.
    ///
    /// A destructive dataplane rebuild can temporarily invalidate local VIP and NodePort routing
    /// even while attachment rows still exist. Marking the peer as `Configuring` first makes
    /// discovery stop admitting local backends, and the explicit refresh updates the local
    /// nodeport / VIP view immediately instead of waiting for the next background tick.
    async fn prepare_for_dataplane_rebuild(&self, network_id: Uuid) -> Result<()> {
        let _ = self.mark_peer_configuring(network_id).await?;
        self.refresh_publication(network_id).await;
        Ok(())
    }

    /// Persist a local peer error so scheduling and discovery stop using this network path.
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

    /// Reconcile VXLAN FDB entries for remote task and host-access MAC forwarding.
    async fn reconcile_remote_forwarding(&self, plan: &NetworkPlan) -> Result<()> {
        if !plan.driver.supports_remote_forwarding() {
            self.clear_forwarding_caches(plan.network_id).await;
            return Ok(());
        }

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

        // Linux uses the all-zero MAC in VXLAN FDB entries to represent flood targets.
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
            return Some(IpAddr::V6(mantissa_net::wireguard::wireguard_tunnel_ipv6(
                peer_id,
            )));
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

mod plan;
mod platform;
#[cfg(all(test, target_os = "linux"))]
mod tests;

use self::plan::{NetworkPlan, compute_deterministic_vni, format_mac, host_access_mac};
