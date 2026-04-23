use crate::network::allocator::{OverlayIpFamily, parse_overlay_cidr};
use crate::network::attachment::{bridge_name, host_access_host_iface_name, vxlan_name};
use crate::network::bpf::{NetworkBpfManager, NetworkInterfaceContext};
use crate::network::lb::{BackendAddress, BpfLoadBalancer};
use crate::network::nodeport::{NodePortManager, NodePortMapping, NodePortProtocol};
use crate::network::registry::NetworkRegistry;
use crate::network::types::{NetworkAttachmentState, NetworkSpecValue};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServicePortProtocol, ServiceReadinessProbe, ServiceReadinessProbeKind, ServiceSpecValue,
    ServiceStatus,
};
use crate::store::workload_store::WorkloadStore;
use crate::workload::model::WorkloadPhase;
use crate::workload::model::{WorkloadValue, select_best_workload_value};
use ::health::{HealthMonitor, Status as HealthStatus};
use anyhow::{Context, Result, bail};
use blake3::Hasher;
use crdt_store::uuid_key::UuidKey;
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, info, warn};
use uuid::Uuid;

mod dns;
mod health;
mod vip;

use self::dns::spawn_dns_server;
use self::health::{
    BackendHealth, evaluate_backend_health, filter_cached_backends, nodeport_protocol,
    normalize_backend_selection, reconcile_service_health_cache,
};
#[cfg(test)]
use self::health::{
    HealthEntry, HealthState, readiness_recheck_after, select_backends_for_active_probe,
};
use self::vip::{
    apply_public_endpoint_observations, reconcile_host_vip_neighbors, sync_service_vip_for_backends,
};

const SERVICE_ZONE_SUFFIX: &str = "svc.mantissa";
const SERVICE_TTL_SECS: u32 = 5;
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
// Keep cached health around for roughly one DNS TTL to avoid stale blackholes.
#[cfg(test)]
const HEALTH_CACHE_STALE_AFTER: Duration = Duration::from_secs(SERVICE_TTL_SECS as u64);
/// Bound how many already-healthy endpoints discovery rechecks on one refresh tick.
///
/// Unknown and unhealthy endpoints are always reprobed as soon as they become eligible because
/// readiness-enabled services must not route traffic to them until they pass. This limit only
/// applies to steady-state spot-checking of endpoints that are already known healthy.
const MAX_HEALTHY_READINESS_RECHECKS_PER_REFRESH: usize = 2;
/// Slow down steady-state rechecks for backends that are already known healthy.
///
/// Once a backend is admitted into discovery, local liveness and peer health cover the common hard
/// failure cases. Discovery readiness only needs to spot softer "temporarily not ready" conditions,
/// so healthy endpoints are spot-checked on a much slower cadence.
const HEALTHY_READINESS_RECHECK_FLOOR: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ServiceDiscovery {
    registry: NetworkRegistry,
    workloads: WorkloadStore,
    services: ServiceRegistry,
    bpf: NetworkBpfManager,
    health_monitor: Arc<HealthMonitor>,
    servers: Arc<AsyncMutex<HashMap<Uuid, DnsServerHandle>>>,
    load_balancer: Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: Arc<AsyncMutex<BackendHealth>>,
    dns_port: u16,
    bpf_lb: BpfLoadBalancer,
    nodeport: NodePortManager,
    missing_lb_maps: Arc<AsyncMutex<HashSet<Uuid>>>,
}

struct DnsServerHandle {
    resolver_ip: IpAddr,
    runtime: DiscoveryRuntime,
    shutdown: Option<watch::Sender<bool>>,
    task: JoinHandle<()>,
}

/// Shared per-network discovery state used by DNS queries and background refreshes.
///
/// Discovery serves one overlay network through two concurrent paths: DNS query handling and the
/// periodic refresh loop. Bundling the stable network-scoped dependencies here keeps those paths
/// on one source of truth and avoids long helper argument lists.
#[derive(Clone)]
struct DiscoveryRuntime {
    registry: NetworkRegistry,
    workloads: WorkloadStore,
    services: ServiceRegistry,
    bpf: NetworkBpfManager,
    network_id: Uuid,
    network_name: String,
    load_balancer: Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: Arc<AsyncMutex<BackendHealth>>,
    health_monitor: Arc<HealthMonitor>,
    backend_catalog: Arc<AsyncMutex<NetworkBackendCatalog>>,
    bpf_lb: BpfLoadBalancer,
    nodeport: NodePortManager,
    missing_lb_maps: Arc<AsyncMutex<HashSet<Uuid>>>,
}

/// Cached backend-resolution metadata for one network, invalidated by store generations and
/// health state changes.
struct NetworkBackendCatalog {
    attachment_generation: u64,
    workload_generation: u64,
    service_generation: u64,
    peer_generation: u64,
    health_fingerprint: u64,
    services: HashMap<String, ServiceBackendCatalogEntry>,
}

impl Default for NetworkBackendCatalog {
    /// Build one backend catalog that always forces an initial refresh.
    ///
    /// Store change clocks restart from zero when a node boots from persisted
    /// state. Using impossible sentinel generations here guarantees the first
    /// discovery refresh rebuilds the backend catalog instead of incorrectly
    /// treating an empty catalog as already current.
    fn default() -> Self {
        Self {
            attachment_generation: u64::MAX,
            workload_generation: u64::MAX,
            service_generation: u64::MAX,
            peer_generation: u64::MAX,
            health_fingerprint: u64::MAX,
            services: HashMap::new(),
        }
    }
}

/// One service entry in the backend catalog.
#[derive(Clone)]
struct ServiceBackendCatalogEntry {
    service_id: Uuid,
    template_name: String,
    service_name: String,
    candidates: Vec<BackendAddress>,
    readiness: Option<ServiceReadinessProbe>,
    expose_to_host: bool,
    public_port: Option<u16>,
    public_target_port: Option<u16>,
    public_protocols: Vec<NodePortProtocol>,
}

/// Refresh result for one discoverable service label, including public endpoint status.
struct ServiceRefreshResult {
    nodeport_mappings: Vec<NodePortMapping>,
    public_endpoint: Option<PublicEndpointObservation>,
    host_vip: Option<IpAddr>,
}

/// Observed public endpoint state for one template during the current refresh tick.
#[derive(Clone, Debug)]
struct PublicEndpointObservation {
    service_id: Uuid,
    template_name: String,
    port: u16,
    detail: Option<String>,
}

impl ServiceDiscovery {
    /// Build service discovery with the default DNS bind port (53).
    pub fn new(
        registry: NetworkRegistry,
        workloads: WorkloadStore,
        services: ServiceRegistry,
        bpf: NetworkBpfManager,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self::new_with_dns_port(registry, workloads, services, bpf, health_monitor, 53)
    }

    /// Build service discovery with an explicit DNS bind port.
    ///
    /// Tests use this to run DNS flows unprivileged on high ports while production keeps 53.
    pub fn new_with_dns_port(
        registry: NetworkRegistry,
        workloads: WorkloadStore,
        services: ServiceRegistry,
        bpf: NetworkBpfManager,
        health_monitor: Arc<HealthMonitor>,
        dns_port: u16,
    ) -> Self {
        Self {
            registry,
            workloads,
            services,
            bpf,
            health_monitor,
            servers: Arc::new(AsyncMutex::new(HashMap::new())),
            load_balancer: Arc::new(AsyncMutex::new(ServiceLoadBalancer::default())),
            health: Arc::new(AsyncMutex::new(BackendHealth::default())),
            dns_port,
            bpf_lb: BpfLoadBalancer::new(),
            nodeport: NodePortManager::new(),
            missing_lb_maps: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    /// Return the shared NodePort manager used by discovery so other local diagnostics can inspect it.
    pub fn nodeport_manager(&self) -> NodePortManager {
        self.nodeport.clone()
    }

    /// Build the network-scoped runtime bundle shared by one resolver socket and refresh loop.
    fn build_runtime(
        &self,
        network_id: Uuid,
        network_name: String,
        backend_catalog: Arc<AsyncMutex<NetworkBackendCatalog>>,
    ) -> DiscoveryRuntime {
        DiscoveryRuntime {
            registry: self.registry.clone(),
            workloads: self.workloads.clone(),
            services: self.services.clone(),
            bpf: self.bpf.clone(),
            network_id,
            network_name,
            load_balancer: self.load_balancer.clone(),
            health: self.health.clone(),
            health_monitor: self.health_monitor.clone(),
            backend_catalog,
            bpf_lb: self.bpf_lb.clone(),
            nodeport: self.nodeport.clone(),
            missing_lb_maps: self.missing_lb_maps.clone(),
        }
    }

    pub async fn ensure_network(
        &self,
        spec: &NetworkSpecValue,
        resolver_ip: Option<IpAddr>,
    ) -> Result<()> {
        let Some(resolver_ip) = resolver_ip else {
            self.teardown_network(spec.id).await?;
            return Ok(());
        };

        {
            let guard = self.servers.lock().await;
            if let Some(existing) = guard.get(&spec.id)
                && existing.resolver_ip == resolver_ip
                && !existing.task.is_finished()
            {
                return Ok(());
            }
        }

        self.teardown_network(spec.id).await?;

        let backend_catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
        let runtime = self.build_runtime(spec.id, spec.name.clone(), backend_catalog);
        let server = spawn_dns_server(runtime, resolver_ip, self.dns_port).await?;

        let mut guard = self.servers.lock().await;
        guard.insert(spec.id, server);
        Ok(())
    }

    /// Refresh one network's current discovery-derived dataplane state immediately.
    ///
    /// Network reconciliation calls this after (re)starting the per-network
    /// listener so public publication and VIP programming can be rebuilt from
    /// the latest durable stores without waiting for the next background tick.
    pub async fn refresh_network(&self, network_id: Uuid) -> Result<()> {
        let runtime = {
            let guard = self.servers.lock().await;
            guard.get(&network_id).map(|server| server.runtime.clone())
        };
        let Some(runtime) = runtime else {
            return Ok(());
        };

        refresh_network_services(&runtime).await
    }

    pub async fn teardown_network(&self, network_id: Uuid) -> Result<()> {
        let handle = {
            let mut guard = self.servers.lock().await;
            guard.remove(&network_id)
        };

        if let Err(err) = self.nodeport.sync_ports(network_id, &[]).await {
            warn!(
                target: "network",
                network = %network_id,
                "failed to clear nodeport mappings during teardown: {err:#}"
            );
        }

        if let Some(mut handle) = handle {
            if let Some(tx) = handle.shutdown.take() {
                let _ = tx.send(true);
            }
            handle.task.await.with_context(|| {
                format!("wait for service discovery listener shutdown for network {network_id}")
            })?;
        }

        Ok(())
    }

    /// Stop every active discovery listener and withdraw its local publication state.
    ///
    /// Headless restart tests reuse one process, so discovery needs an explicit
    /// shutdown path that releases resolver sockets and clears NodePort
    /// mappings before a replacement runtime starts from the same persisted
    /// state.
    pub async fn shutdown(&self) -> Result<()> {
        let network_ids = {
            let guard = self.servers.lock().await;
            guard.keys().copied().collect::<Vec<_>>()
        };

        for network_id in network_ids {
            self.teardown_network(network_id).await?;
        }

        Ok(())
    }
}

/// Resolves service backends while one backend-catalog refresh is in progress.
struct ServiceBackendResolver<'a> {
    runtime: &'a DiscoveryRuntime,
    template_index: &'a HashMap<Uuid, (String, String)>,
    health_snapshot: &'a HashMap<Uuid, HealthStatus>,
}

impl ServiceBackendResolver<'_> {
    /// Resolve the currently routable backend attachment set for one service template name.
    async fn resolve(&self, service_name: &str) -> Result<Vec<BackendAddress>> {
        let expected_family = self
            .runtime
            .registry
            .get_spec(self.runtime.network_id)?
            .map(|spec| parse_overlay_cidr(&spec.subnet_cidr))
            .transpose()?
            .map(|subnet| subnet.family);
        let ready_peers: HashSet<Uuid> = self
            .runtime
            .registry
            .list_peer_states(Some(self.runtime.network_id))?
            .into_iter()
            .filter(|state| state.state.is_ready())
            .map(|state| state.peer_id)
            .collect();
        let attachments = self
            .runtime
            .registry
            .list_attachments(Some(self.runtime.network_id))
            .context("list attachments for discovery")?;
        let mut cache: HashMap<Uuid, Option<WorkloadValue>> = HashMap::new();
        let mut results = Vec::new();

        tracing::trace!(
            target: "network",
            network = %self.runtime.network_id,
            service = %service_name,
            attachments = attachments.len(),
            "resolving service backends"
        );

        for attachment in attachments {
            if !ready_peers.contains(&attachment.node_id) {
                tracing::trace!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    node = %attachment.node_id,
                    "skipping attachment on node whose network peer state is not ready"
                );
                continue;
            }
            if matches!(
                self.health_snapshot.get(&attachment.node_id),
                Some(HealthStatus::Down)
            ) {
                tracing::trace!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    node = %attachment.node_id,
                    "skipping attachment on down node"
                );
                continue;
            }
            if attachment.state != NetworkAttachmentState::Ready {
                tracing::trace!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    state = ?attachment.state,
                    "skipping attachment not ready"
                );
                continue;
            }
            if !attachment.traffic_published {
                tracing::trace!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    "skipping attachment not published for traffic"
                );
                continue;
            }
            let Some(ip_text) = &attachment.assigned_ip else {
                tracing::debug!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    "skipping attachment without ip"
                );
                continue;
            };
            let Some(mac_text) = &attachment.mac else {
                tracing::debug!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    "skipping attachment without mac"
                );
                continue;
            };
            let task_entry = cache
                .entry(attachment.task_id)
                .or_insert_with(|| load_task(&self.runtime.workloads, attachment.task_id));
            let task = match task_entry.as_ref() {
                Some(task) => task,
                None => {
                    tracing::debug!(
                        target: "network",
                        network = %self.runtime.network_id,
                        attachment = %attachment.id,
                        task = %attachment.task_id,
                        "skipping attachment; task record missing"
                    );
                    continue;
                }
            };

            let mut template_match = false;
            if let Some(template) = attachment.template_name.as_deref() {
                template_match |= template.eq_ignore_ascii_case(service_name);
            }
            if let Some(service) = attachment.service_name.as_deref() {
                template_match |= service.eq_ignore_ascii_case(service_name);
            }
            if let Some(meta) = task.service_owner() {
                template_match |= meta.template.eq_ignore_ascii_case(service_name);
            }
            template_match |= task.name.eq_ignore_ascii_case(service_name);
            if let Some((_, template)) = self.template_index.get(&attachment.task_id) {
                template_match |= template.eq_ignore_ascii_case(service_name);
            }

            if !template_match {
                tracing::trace!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    task = %attachment.task_id,
                    template = %attachment.template_name.clone().unwrap_or_default(),
                    service = %service_name,
                    "skipping attachment; template mismatch"
                );
                continue;
            }

            if task.node_id != attachment.node_id {
                if attachment_is_newer_than_task(&attachment, task) {
                    tracing::debug!(
                        target: "network",
                        network = %self.runtime.network_id,
                        attachment = %attachment.id,
                        task = %task.id,
                        expected_node = %task.node_id,
                        actual_node = %attachment.node_id,
                        "keeping attachment; task record appears stale"
                    );
                } else {
                    tracing::debug!(
                        target: "network",
                        network = %self.runtime.network_id,
                        attachment = %attachment.id,
                        task = %task.id,
                        expected_node = %task.node_id,
                        actual_node = %attachment.node_id,
                        "skipping attachment; task moved to another node"
                    );
                    continue;
                }
            }
            if task.state != WorkloadPhase::Running {
                tracing::debug!(
                    target: "network",
                    network = %self.runtime.network_id,
                    attachment = %attachment.id,
                    task = %task.id,
                    state = ?task.state,
                    "skipping attachment; task not running"
                );
                continue;
            }
            let ip_addr = match ip_text.parse::<IpAddr>() {
                Ok(addr) => addr,
                Err(err) => {
                    warn!(
                        target: "network",
                        network = %self.runtime.network_id,
                        task = %attachment.task_id,
                        "invalid attachment ip {}: {err}",
                        ip_text
                    );
                    continue;
                }
            };
            if let Some(expected_family) = expected_family
                && ip_family(ip_addr) != expected_family
            {
                warn!(
                    target: "network",
                    network = %self.runtime.network_id,
                    task = %attachment.task_id,
                    expected_family = ?expected_family,
                    actual_ip = %ip_addr,
                    "attachment ip family does not match overlay subnet"
                );
                continue;
            }
            let mac = match parse_mac(mac_text) {
                Ok(mac) => mac,
                Err(err) => {
                    warn!(
                        target: "network",
                        network = %self.runtime.network_id,
                        task = %attachment.task_id,
                        "invalid attachment mac {}: {err}",
                        mac_text
                    );
                    continue;
                }
            };
            results.push(BackendAddress { ip: ip_addr, mac });
        }

        tracing::trace!(
            target: "network",
            network = %self.runtime.network_id,
            service = %service_name,
            backends = results.len(),
            "resolved service backends"
        );

        Ok(results)
    }
}

/// Load the most relevant task value so discovery follows the current scheduling decision.
fn load_task(workloads: &WorkloadStore, id: Uuid) -> Option<WorkloadValue> {
    let key = UuidKey::from(id);
    let snapshot = workloads.get_snapshot(&key).ok()??;
    select_best_workload_value(snapshot.as_slice())
}

/// Decide whether an attachment should be trusted over a stale task record during convergence.
fn attachment_is_newer_than_task(
    attachment: &crate::network::types::NetworkAttachmentValue,
    task: &WorkloadValue,
) -> bool {
    let attachment_ts = attachment_revision_timestamp(attachment);
    let task_ts = task_revision_timestamp(task);

    match (attachment_ts, task_ts) {
        (Some(attachment_ts), Some(task_ts)) => attachment_ts >= task_ts,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

/// Extract a comparable revision timestamp from an attachment to resolve reschedule races.
fn attachment_revision_timestamp(
    attachment: &crate::network::types::NetworkAttachmentValue,
) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_rfc3339(attachment.task_updated_at.as_deref())
        .or_else(|| parse_rfc3339(Some(&attachment.updated_at)))
        .or_else(|| parse_rfc3339(Some(&attachment.created_at)))
}

/// Extract a comparable revision timestamp from a task to detect stale task records.
fn task_revision_timestamp(task: &WorkloadValue) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_rfc3339(Some(&task.updated_at)).or_else(|| parse_rfc3339(Some(&task.created_at)))
}

/// Parse RFC3339 timestamps from optional string inputs for revision comparisons.
fn parse_rfc3339(raw: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    let raw = raw?;
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .ok()
}

/// Sort backend endpoints deterministically so LB programming is stable across refreshes.
fn sort_backends(backends: &mut [BackendAddress]) {
    backends.sort_by(|a, b| compare_ip_addrs(a.ip, b.ip).then_with(|| a.mac.cmp(&b.mac)));
}

/// Return the overlay family associated with a concrete attachment or resolver address.
fn ip_family(ip: IpAddr) -> OverlayIpFamily {
    match ip {
        IpAddr::V4(_) => OverlayIpFamily::Ipv4,
        IpAddr::V6(_) => OverlayIpFamily::Ipv6,
    }
}

/// Keep backend ordering stable across nodes by comparing family first and octets second.
fn compare_ip_addrs(left: IpAddr, right: IpAddr) -> std::cmp::Ordering {
    match (left, right) {
        (IpAddr::V4(left), IpAddr::V4(right)) => left.octets().cmp(&right.octets()),
        (IpAddr::V6(left), IpAddr::V6(right)) => left.octets().cmp(&right.octets()),
        (IpAddr::V4(_), IpAddr::V6(_)) => std::cmp::Ordering::Less,
        (IpAddr::V6(_), IpAddr::V4(_)) => std::cmp::Ordering::Greater,
    }
}

fn build_task_template_index(specs: &[ServiceSpecValue]) -> HashMap<Uuid, (String, String)> {
    let mut index = HashMap::new();
    for spec in specs {
        let mut ids = spec.replica_ids.iter();
        for template in &spec.task_templates {
            for _ in 0..template.replicas {
                let Some(task_id) = ids.next() else { break };
                index.insert(*task_id, (spec.service_name.clone(), template.name.clone()));
            }
        }
    }
    index
}

fn compute_service_vip(
    registry: &NetworkRegistry,
    network_id: Uuid,
    service_name: &str,
    backends: &[BackendAddress],
) -> Result<Option<(IpAddr, [u8; 6])>> {
    let Some(spec) = registry.get_spec(network_id)? else {
        return Ok(None);
    };
    let subnet = match parse_overlay_cidr(&spec.subnet_cidr) {
        Ok(subnet) => subnet,
        Err(_) => return Ok(None),
    };
    let address_bits = match subnet.family {
        OverlayIpFamily::Ipv4 => 32u8,
        OverlayIpFamily::Ipv6 => 128u8,
    };
    let base_ip = match subnet.base_ip {
        IpAddr::V4(ip) => u32::from(ip) as u128,
        IpAddr::V6(ip) => u128::from(ip),
    };
    let host_bits = address_bits.saturating_sub(subnet.prefix);
    if host_bits < 4 {
        return Ok(None);
    }

    let digest = {
        let mut hasher = Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(service_name.as_bytes());
        hasher.finalize()
    };
    let mut slot_seed = [0u8; 16];
    slot_seed.copy_from_slice(&digest.as_bytes()[..16]);
    let slot_seed = u128::from_le_bytes(slot_seed);

    // Constrain VIPs to the even offsets of the overlay to avoid collisions with per-node resolver
    // addresses, which always occupy the odd slots (offsets 1, 3, 5, ...).
    let max_hosts: u128 = match (subnet.family, host_bits) {
        (OverlayIpFamily::Ipv4, 32) => u32::MAX as u128 + 1,
        (OverlayIpFamily::Ipv6, 128) => return Ok(None),
        _ => 1u128 << host_bits,
    };
    let available_even = max_hosts.saturating_sub(16) / 2;
    if available_even == 0 {
        return Ok(None);
    }

    let backend_ips: std::collections::HashSet<u128> = backends
        .iter()
        .filter_map(|backend| match (subnet.family, backend.ip) {
            (OverlayIpFamily::Ipv4, IpAddr::V4(ip)) => Some(u32::from(ip) as u128),
            (OverlayIpFamily::Ipv6, IpAddr::V6(ip)) => Some(u128::from(ip)),
            _ => None,
        })
        .collect();
    if backend_ips.len() != backends.len() {
        return Ok(None);
    }

    let mut slot = (slot_seed % available_even) * 2 + 8;
    let probe_budget = usize::try_from(available_even.min(16)).unwrap_or(16);
    for _ in 0..probe_budget {
        let candidate = base_ip.saturating_add(slot);
        if !backend_ips.contains(&candidate) {
            let vip = match subnet.family {
                OverlayIpFamily::Ipv4 => IpAddr::V4(Ipv4Addr::from(candidate as u32)),
                OverlayIpFamily::Ipv6 => IpAddr::V6(Ipv6Addr::from(candidate)),
            };

            let mut mac = [0u8; 6];
            mac[0] = 0x02;
            mac[1..].copy_from_slice(&digest.as_bytes()[4..9]);

            return Ok(Some((vip, mac)));
        }

        // Walk forward to the next even slot if we collided with an existing backend.
        slot = slot.wrapping_add(2) % (available_even * 2);
        if slot < 8 {
            slot = 8;
        }
    }

    Ok(None)
}

fn parse_mac(text: &str) -> Result<[u8; 6], String> {
    let parts: Vec<&str> = text.split(':').collect();
    if parts.len() != 6 {
        return Err("wrong number of octets".to_string());
    }
    let mut mac = [0u8; 6];
    for (idx, part) in parts.iter().enumerate() {
        mac[idx] = u8::from_str_radix(part, 16).map_err(|err| err.to_string())?;
    }
    Ok(mac)
}

#[derive(Default)]
struct ServiceLoadBalancer {
    cursors: HashMap<(Uuid, String), usize>,
}

impl ServiceLoadBalancer {
    /// Track one per-service cursor offset so successive DNS responses rotate their primary backend.
    ///
    /// Normal service DNS should prefer the stable service VIP whenever dataplane programming
    /// succeeds. This cursor remains as the backend-only fallback path for environments where VIP
    /// programming is unavailable and Mantissa still has to expose attachment addresses directly.
    fn next_offset(&mut self, network_id: Uuid, service_name: &str, backend_count: usize) -> usize {
        if backend_count == 0 {
            return 0;
        }
        let key = (network_id, service_name.to_ascii_lowercase());
        let cursor = self.cursors.entry(key).or_insert(0);
        let offset = *cursor % backend_count;
        *cursor = cursor.wrapping_add(1);
        offset
    }
}

/// Rotate the ordered list of backend addresses so the requested offset becomes the first entry.
fn rotate_addresses(mut addresses: Vec<IpAddr>, offset: usize) -> Vec<IpAddr> {
    if addresses.is_empty() {
        return addresses;
    }
    let shift = offset % addresses.len();
    addresses.rotate_left(shift);
    addresses
}

async fn refresh_backend_catalog_if_needed(
    runtime: &DiscoveryRuntime,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
) -> Result<()> {
    let attachment_generation = runtime.registry.attachment_change_clock();
    let workload_generation = runtime.workloads.change_clock();
    let service_generation = runtime.services.change_clock();
    let peer_generation = runtime.registry.peer_change_clock();
    let health_fingerprint = health_snapshot_fingerprint(health_snapshot);

    let previous_services = {
        let guard = runtime.backend_catalog.lock().await;
        if guard.attachment_generation == attachment_generation
            && guard.workload_generation == workload_generation
            && guard.service_generation == service_generation
            && guard.peer_generation == peer_generation
            && guard.health_fingerprint == health_fingerprint
        {
            return Ok(());
        }
        guard.services.clone()
    };

    let service_specs = runtime
        .services
        .list()
        .context("load service specs for backend catalog refresh")?;
    let template_index = build_task_template_index(&service_specs);
    let resolver = ServiceBackendResolver {
        runtime,
        template_index: &template_index,
        health_snapshot,
    };
    let mut next_services = HashMap::new();
    for spec in &service_specs {
        for template in &spec.task_templates {
            if !template
                .networks
                .iter()
                .any(|net| net.network_id == runtime.network_id)
            {
                continue;
            }

            let service_name = template.name.clone();
            let candidates = resolver.resolve(&service_name).await?;
            let public_port = template.public_port();
            let public_target_port = template.public_target_port();
            let public_protocols = if public_port.is_some() {
                template
                    .public_protocols()
                    .into_iter()
                    .map(nodeport_protocol)
                    .collect()
            } else {
                Vec::new()
            };
            let service_key = service_name.to_ascii_lowercase();

            next_services.insert(
                service_key,
                ServiceBackendCatalogEntry {
                    service_id: spec.id,
                    template_name: template.name.clone(),
                    service_name,
                    candidates,
                    readiness: template.readiness().cloned(),
                    expose_to_host: public_port.is_some(),
                    public_port,
                    public_target_port,
                    public_protocols,
                },
            );
        }
    }

    {
        let mut guard = runtime.health.lock().await;
        let mut service_keys: HashSet<String> =
            previous_services.keys().cloned().collect::<HashSet<_>>();
        service_keys.extend(next_services.keys().cloned());
        for service_key in service_keys {
            reconcile_service_health_cache(
                &mut guard,
                runtime.network_id,
                &service_key,
                previous_services.get(&service_key),
                next_services.get(&service_key),
            );
        }
    }

    let mut guard = runtime.backend_catalog.lock().await;
    guard.attachment_generation = attachment_generation;
    guard.workload_generation = workload_generation;
    guard.service_generation = service_generation;
    guard.peer_generation = peer_generation;
    guard.health_fingerprint = health_fingerprint;
    guard.services = next_services;
    Ok(())
}

/// Hash a peer-health snapshot into a stable fingerprint so backend cache invalidation can react
/// to node liveness changes that affect candidate filtering.
fn health_snapshot_fingerprint(snapshot: &HashMap<Uuid, HealthStatus>) -> u64 {
    let mut entries: Vec<(Uuid, HealthStatus)> = snapshot.iter().map(|(k, v)| (*k, *v)).collect();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut hasher = Hasher::new();
    for (peer_id, status) in entries {
        hasher.update(peer_id.as_bytes());
        hasher.update(&[health_status_discriminant(status)]);
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

/// Encode health status as a compact numeric discriminant for cache fingerprinting.
fn health_status_discriminant(status: HealthStatus) -> u8 {
    match status {
        HealthStatus::Unknown => 0,
        HealthStatus::Alive => 1,
        HealthStatus::Suspect => 2,
        HealthStatus::Down => 3,
        HealthStatus::Degraded => 4,
    }
}

/// Periodically refresh health and BPF state for all services attached to a specific network so
/// dataplane programming keeps up with container restarts even when no DNS queries arrive.
async fn refresh_network_services(runtime: &DiscoveryRuntime) -> Result<()> {
    let health_snapshot = runtime.health_monitor.snapshot();
    refresh_backend_catalog_if_needed(runtime, &health_snapshot).await?;
    let entries: Vec<ServiceBackendCatalogEntry> = {
        let guard = runtime.backend_catalog.lock().await;
        guard.services.values().cloned().collect()
    };
    let mut nodeport_entries = Vec::new();
    let mut public_endpoint_observations = Vec::new();
    let mut host_vips = HashSet::new();

    for entry in entries {
        let result = refresh_single_service(runtime, &entry).await?;
        nodeport_entries.extend(result.nodeport_mappings);
        if let Some(observation) = result.public_endpoint {
            public_endpoint_observations.push(observation);
        }
        if let Some(vip) = result.host_vip {
            host_vips.insert(vip);
        }
    }

    if let Err(err) = runtime
        .nodeport
        .sync_ports(runtime.network_id, &nodeport_entries)
        .await
    {
        for observation in &mut public_endpoint_observations {
            if observation.detail.is_none() {
                observation.detail = Some(format!(
                    "template '{}' public port {} could not publish NodePort: {err:#}",
                    observation.template_name, observation.port
                ));
            }
        }
        warn!(
            target: "network",
            network = %runtime.network_id,
            "failed to sync public nodeport mappings: {err:#}"
        );
    }
    if let Err(err) = reconcile_host_vip_neighbors(runtime.network_id, &host_vips).await {
        warn!(
            target: "network",
            network = %runtime.network_id,
            "failed to reconcile host vip neighbours: {err:#}"
        );
    }
    apply_public_endpoint_observations(&runtime.services, public_endpoint_observations.as_slice())
        .await?;

    Ok(())
}

/// Refresh healthy backend list and VIP programming for a single service so clients connected via
/// the VIP can fail over even if they do not issue new DNS lookups.
///
/// Returns nodeport mappings when the service is marked public so external listeners can be
/// reconciled by the caller.
async fn refresh_single_service(
    runtime: &DiscoveryRuntime,
    service: &ServiceBackendCatalogEntry,
) -> Result<ServiceRefreshResult> {
    let service_name = service.service_name.as_str();
    let candidates = service.candidates.clone();
    let mut backends = evaluate_backend_health(
        &runtime.health,
        &runtime.registry,
        runtime.network_id,
        service_name,
        candidates.clone(),
        service.readiness.clone(),
    )
    .await;

    backends = normalize_backend_selection(
        runtime.network_id,
        service_name,
        candidates,
        backends,
        service.readiness.is_some(),
        "refresh",
    );

    if backends.is_empty() {
        let _ = sync_service_vip_for_backends(runtime, service_name, &[], service.expose_to_host)
            .await?;
        return Ok(ServiceRefreshResult {
            nodeport_mappings: Vec::new(),
            public_endpoint: service.public_port.map(|port| PublicEndpointObservation {
                service_id: service.service_id,
                template_name: service.template_name.clone(),
                port,
                detail: Some(format!(
                    "template '{}' public port {} has no healthy backends",
                    service.template_name, port
                )),
            }),
            // Only keep the host VIP neighbour alive while at least one backend is published.
            host_vip: None,
        });
    }

    if let Some((vip, programmed)) =
        sync_service_vip_for_backends(runtime, service_name, &backends, service.expose_to_host)
            .await?
        && let Some(port) = service.public_port
    {
        let vip_port = service.public_target_port.unwrap_or(port);
        if !programmed {
            return Ok(ServiceRefreshResult {
                nodeport_mappings: Vec::new(),
                public_endpoint: Some(PublicEndpointObservation {
                    service_id: service.service_id,
                    template_name: service.template_name.clone(),
                    port,
                    detail: Some(format!(
                        "template '{}' public port {} lost VIP programming; internal discovery is still available",
                        service.template_name, port
                    )),
                }),
                host_vip: service.expose_to_host.then_some(vip),
            });
        }

        let mut mappings = Vec::new();
        for protocol in service.public_protocols.clone() {
            mappings.push(NodePortMapping {
                port,
                vip,
                vip_port,
                protocol,
            });
        }
        return Ok(ServiceRefreshResult {
            nodeport_mappings: mappings,
            public_endpoint: Some(PublicEndpointObservation {
                service_id: service.service_id,
                template_name: service.template_name.clone(),
                port,
                detail: None,
            }),
            host_vip: service.expose_to_host.then_some(vip),
        });
    }

    Ok(ServiceRefreshResult {
        nodeport_mappings: Vec::new(),
        public_endpoint: service.public_port.map(|port| PublicEndpointObservation {
            service_id: service.service_id,
            template_name: service.template_name.clone(),
            port,
            detail: Some(format!(
                "template '{}' public port {} could not assign a stable service VIP",
                service.template_name, port
            )),
        }),
        host_vip: None,
    })
}

#[cfg(test)]
mod tests;
