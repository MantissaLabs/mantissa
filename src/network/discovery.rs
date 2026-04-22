use crate::network::allocator::{OverlayIpFamily, parse_overlay_cidr};
use crate::network::attachment::{bridge_name, host_access_host_iface_name, vxlan_name};
use crate::network::bpf::{NetworkBpfManager, NetworkInterfaceContext};
use crate::network::lb::{BackendAddress, BpfLoadBalancer};
use crate::network::nodeport::{NodePortManager, NodePortMapping, NodePortProtocol};
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    BpfAttachPoint, BpfProgramSpec, NetworkAttachmentState, NetworkSpecValue,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{
    ServicePortProtocol, ServiceReadinessProbe, ServiceReadinessProbeKind, ServiceSpecValue,
    ServiceStatus,
};
use crate::store::workload_store::WorkloadStore;
use crate::workload::model::WorkloadPhase;
use crate::workload::model::{WorkloadValue, select_best_workload_value};
use anyhow::{Context, Result, bail};
use blake3::Hasher;
use crdt_store::uuid_key::UuidKey;
use health::{HealthMonitor, Status as HealthStatus};
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

const SERVICE_ZONE_SUFFIX: &str = "svc.mantissa";
const SERVICE_TTL_SECS: u32 = 5;
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);
// Keep cached health around for roughly one DNS TTL to avoid stale blackholes.
const HEALTH_CACHE_STALE_AFTER: Duration = Duration::from_secs(SERVICE_TTL_SECS as u64);
/// Bound how many active readiness probes one node performs per service on one refresh tick.
///
/// Discovery already filters hard failures through node health, running-task state, and published
/// attachment state. Readiness probing remains useful for "alive but should not receive traffic"
/// cases, but sampling a small subset per refresh keeps the steady-state cost proportional to
/// service churn rather than cluster size.
const MAX_READINESS_PROBES_PER_REFRESH: usize = 2;
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
    backend_catalog: Arc<AsyncMutex<NetworkBackendCatalog>>,
    shutdown: Option<watch::Sender<bool>>,
    task: JoinHandle<()>,
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

        let server = spawn_dns_server(
            self.registry.clone(),
            self.workloads.clone(),
            self.services.clone(),
            self.bpf.clone(),
            spec.id,
            spec.name.clone(),
            resolver_ip,
            self.load_balancer.clone(),
            self.health.clone(),
            self.dns_port,
            self.bpf_lb.clone(),
            self.nodeport.clone(),
            self.missing_lb_maps.clone(),
            self.health_monitor.clone(),
        )
        .await?;

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
        let backend_catalog = {
            let guard = self.servers.lock().await;
            guard
                .get(&network_id)
                .map(|server| server.backend_catalog.clone())
        };
        let Some(backend_catalog) = backend_catalog else {
            return Ok(());
        };

        refresh_network_services(
            &self.registry,
            &self.workloads,
            &self.services,
            &self.bpf,
            network_id,
            &self.health,
            &self.bpf_lb,
            &self.nodeport,
            &self.missing_lb_maps,
            &self.health_monitor,
            &backend_catalog,
        )
        .await
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

#[allow(clippy::too_many_arguments)]
async fn spawn_dns_server(
    registry: NetworkRegistry,
    workloads: WorkloadStore,
    services: ServiceRegistry,
    bpf: NetworkBpfManager,
    network_id: Uuid,
    network_name: String,
    resolver_ip: IpAddr,
    load_balancer: Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: Arc<AsyncMutex<BackendHealth>>,
    dns_port: u16,
    bpf_lb: BpfLoadBalancer,
    nodeport: NodePortManager,
    missing_lb_maps: Arc<AsyncMutex<HashSet<Uuid>>>,
    health_monitor: Arc<HealthMonitor>,
) -> Result<DnsServerHandle> {
    let bind_addr = SocketAddr::new(resolver_ip, dns_port);
    let socket = UdpSocket::bind(bind_addr)
        .await
        .with_context(|| format!("bind resolver socket {bind_addr}"))?;
    info!(
        target: "network",
        network = %network_id,
        resolver = %resolver_ip,
        "started service discovery listener"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task_registry = registry.clone();
    let service_registry = services.clone();
    let backend_catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
    let lb_manager = bpf_lb.clone();
    let bpf_manager = bpf.clone();
    let lb_missing = missing_lb_maps.clone();
    let refresh_health_monitor = health_monitor.clone();
    if let Err(err) = refresh_network_services(
        &task_registry,
        &workloads,
        &service_registry,
        &bpf_manager,
        network_id,
        &health,
        &lb_manager,
        &nodeport,
        &lb_missing,
        &refresh_health_monitor,
        &backend_catalog,
    )
    .await
    {
        warn!(
            target: "network",
            network = %network_id,
            "initial service discovery refresh failed: {err:#}"
        );
    }
    let mut refresh_shutdown = shutdown_rx.clone();
    let refresh_task_registry = task_registry.clone();
    let refresh_workloads = workloads.clone();
    let refresh_service_registry = service_registry.clone();
    let refresh_bpf_manager = bpf_manager.clone();
    let refresh_health = health.clone();
    let refresh_lb_manager = lb_manager.clone();
    let refresh_nodeport = nodeport.clone();
    let refresh_lb_missing = lb_missing.clone();
    let refresh_health_monitor = health_monitor.clone();
    let refresh_backend_catalog = backend_catalog.clone();
    let refresh_task = tokio::spawn(async move {
        let mut refresh = time::interval(REFRESH_INTERVAL);
        loop {
            tokio::select! {
                _ = refresh_shutdown.changed() => {
                    if *refresh_shutdown.borrow() {
                        break;
                    }
                }
                _ = refresh.tick() => {
                    if let Err(err) = refresh_network_services(
                        &refresh_task_registry,
                        &refresh_workloads,
                        &refresh_service_registry,
                        &refresh_bpf_manager,
                        network_id,
                        &refresh_health,
                        &refresh_lb_manager,
                        &refresh_nodeport,
                        &refresh_lb_missing,
                        &refresh_health_monitor,
                        &refresh_backend_catalog,
                    ).await {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "service discovery refresh failed: {err:#}"
                        );
                    }
                }
            }
        }
    });

    let mut dns_shutdown = shutdown_rx.clone();
    let dns_task_registry = task_registry.clone();
    let dns_workloads = workloads.clone();
    let dns_service_registry = service_registry.clone();
    let dns_bpf_manager = bpf_manager.clone();
    let dns_load_balancer = load_balancer.clone();
    let dns_health = health.clone();
    let dns_lb_manager = lb_manager.clone();
    let dns_lb_missing = lb_missing.clone();
    let dns_health_monitor = health_monitor.clone();
    let dns_backend_catalog = backend_catalog.clone();
    let dns_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 2048];
        loop {
            tokio::select! {
                _ = dns_shutdown.changed() => {
                    if *dns_shutdown.borrow() {
                        break;
                    }
                }
                result = socket.recv_from(&mut buf) => {
                    match result {
                        Ok((len, peer)) => {
                            if let Err(err) = handle_datagram(
                                &socket,
                                &buf[..len],
                                peer,
                                &dns_task_registry,
                            &dns_workloads,
                                &dns_service_registry,
                                &dns_bpf_manager,
                                network_id,
                                &network_name,
                                &dns_load_balancer,
                                &dns_health,
                                &dns_lb_manager,
                                &dns_lb_missing,
                                &dns_health_monitor,
                                &dns_backend_catalog,
                            ).await {
                                warn!(
                                    target: "network",
                                    network = %network_id,
                                    "service discovery failed to handle udp datagram: {err:#}"
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                target: "network",
                                network = %network_id,
                                "service discovery socket recv failed: {err}"
                            );
                        }
                    }
                }
            }
        }
        info!(
            target: "network",
            network = %network_id,
            "service discovery listener stopped"
        );
    });

    let server = tokio::spawn(async move {
        let _ = refresh_task.await;
        let _ = dns_task.await;
    });

    Ok(DnsServerHandle {
        resolver_ip,
        backend_catalog,
        shutdown: Some(shutdown_tx),
        task: server,
    })
}

#[allow(clippy::too_many_arguments)]
async fn handle_datagram(
    socket: &UdpSocket,
    payload: &[u8],
    peer: SocketAddr,
    registry: &NetworkRegistry,
    workloads: &WorkloadStore,
    services: &ServiceRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    network_name: &str,
    load_balancer: &Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: &Arc<AsyncMutex<BackendHealth>>,
    bpf_lb: &BpfLoadBalancer,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
    health_monitor: &Arc<HealthMonitor>,
    backend_catalog: &Arc<AsyncMutex<NetworkBackendCatalog>>,
) -> Result<()> {
    let request = match Message::from_vec(payload) {
        Ok(message) => message,
        Err(err) => {
            debug!(
                target: "network",
                network = %network_id,
                "discarding malformed dns query: {err}"
            );
            return Ok(());
        }
    };

    let query_names: Vec<String> = request
        .queries()
        .iter()
        .map(|q| q.name().to_string())
        .collect();
    debug!(
        target: "network",
        network = %network_id,
        peer = %peer,
        ?query_names,
        "received dns query"
    );

    let mut response = Message::new();
    response.set_id(request.id());
    response.set_message_type(MessageType::Response);
    response.set_op_code(request.op_code());
    response.set_recursion_desired(request.recursion_desired());
    response.set_recursion_available(false);
    response.set_authoritative(true);

    for query in request.queries() {
        response.add_query(query.clone());
    }

    let mut answers_added = false;
    let mut total_answer_records = 0usize;
    let mut saw_nxdomain = false;
    let mut saw_nodata = false;
    let mut saw_notimp = false;

    let health_snapshot = health_monitor.snapshot();
    if let Err(err) = refresh_backend_catalog_if_needed(
        backend_catalog,
        registry,
        workloads,
        services,
        network_id,
        &health_snapshot,
    )
    .await
    {
        warn!(
            target: "network",
            network = %network_id,
            "failed to refresh backend catalog while answering dns query: {err:#}"
        );
    }

    for query in request.queries() {
        match answer_query(
            query,
            registry,
            bpf,
            network_id,
            network_name,
            load_balancer,
            health,
            bpf_lb,
            lb_missing,
            backend_catalog,
        )
        .await?
        {
            LookupOutcome::Records(records) => {
                for record in records {
                    response.add_answer(record);
                    answers_added = true;
                    total_answer_records += 1;
                }
                debug!(
                    target: "network",
                    network = %network_id,
                    peer = %peer,
                    name = %query.name(),
                    answers = total_answer_records,
                    "dns answered with records"
                );
            }
            LookupOutcome::NxDomain => saw_nxdomain = true,
            LookupOutcome::NoData => saw_nodata = true,
            LookupOutcome::NotImplemented => saw_notimp = true,
        }
    }

    let code = if answers_added || saw_nodata {
        ResponseCode::NoError
    } else if saw_notimp {
        ResponseCode::NotImp
    } else if saw_nxdomain {
        ResponseCode::NXDomain
    } else {
        ResponseCode::ServFail
    };
    response.set_response_code(code);

    let bytes = response.to_vec().context("encode dns response")?;
    socket
        .send_to(&bytes, peer)
        .await
        .with_context(|| format!("send dns response to {}", peer))?;
    Ok(())
}

enum LookupOutcome {
    Records(Vec<Record>),
    NxDomain,
    NoData,
    NotImplemented,
}

#[allow(clippy::too_many_arguments)]
async fn answer_query(
    query: &Query,
    registry: &NetworkRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    network_name: &str,
    load_balancer: &Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: &Arc<AsyncMutex<BackendHealth>>,
    bpf_lb: &BpfLoadBalancer,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
    backend_catalog: &Arc<AsyncMutex<NetworkBackendCatalog>>,
) -> Result<LookupOutcome> {
    if query.query_type() != RecordType::A && query.query_type() != RecordType::AAAA {
        return Ok(LookupOutcome::NotImplemented);
    }

    let expected_record_type = overlay_dns_record_type(registry, network_id)?;
    if query.query_type() != expected_record_type {
        return Ok(LookupOutcome::NoData);
    }

    let Some(service_name) = extract_service_label(query.name(), network_name) else {
        return Ok(LookupOutcome::NxDomain);
    };

    let catalog_entry = {
        let guard = backend_catalog.lock().await;
        guard
            .services
            .get(&service_name.to_ascii_lowercase())
            .cloned()
    };
    let Some(catalog_entry) = catalog_entry else {
        return Ok(LookupOutcome::NxDomain);
    };

    let candidates = catalog_entry.candidates.clone();
    let mut backends = if catalog_entry.readiness.is_some() {
        let guard = health.lock().await;
        filter_cached_backends(&guard, network_id, &service_name, candidates.clone())
    } else {
        candidates.clone()
    };
    tracing::trace!(
        target: "network",
        network = %network_id,
        service = %service_name,
        candidate_backends = candidates.len(),
        healthy_backends = backends.len(),
        "post-health backends"
    );
    backends = normalize_backend_selection(
        network_id,
        &service_name,
        candidates,
        backends,
        catalog_entry.readiness.is_some(),
        "dns",
    );

    if backends.is_empty() {
        let _ = sync_service_vip_for_backends(
            bpf_lb,
            bpf,
            lb_missing,
            registry,
            network_id,
            &service_name,
            &[],
            catalog_entry.expose_to_host,
        )
        .await?;
        return Ok(LookupOutcome::NxDomain);
    }
    if let Some((vip, programmed)) = sync_service_vip_for_backends(
        bpf_lb,
        bpf,
        lb_missing,
        registry,
        network_id,
        &service_name,
        &backends,
        catalog_entry.expose_to_host,
    )
    .await?
        && programmed
    {
        // Service names should resolve to one stable VIP whenever the dataplane is available so
        // clients do not depend on backend-record ordering for load-balancing.
        return Ok(LookupOutcome::Records(vec![address_record(
            query.name(),
            vip,
        )]));
    }

    let offset = {
        let mut picker = load_balancer.lock().await;
        picker.next_offset(network_id, &service_name, backends.len())
    };
    let records = rotate_addresses(
        backends
            .iter()
            .map(|backend| backend.ip)
            .collect::<Vec<IpAddr>>(),
        offset,
    )
    .into_iter()
    .map(|addr| address_record(query.name(), addr))
    .collect();

    Ok(LookupOutcome::Records(records))
}

/// Resolve which DNS record family one overlay network should answer for service names.
fn overlay_dns_record_type(registry: &NetworkRegistry, network_id: Uuid) -> Result<RecordType> {
    let Some(spec) = registry.get_spec(network_id)? else {
        bail!("network {network_id} is missing while resolving service records");
    };
    let subnet = parse_overlay_cidr(&spec.subnet_cidr)?;
    Ok(match subnet.family {
        OverlayIpFamily::Ipv4 => RecordType::A,
        OverlayIpFamily::Ipv6 => RecordType::AAAA,
    })
}

/// Build one DNS address record matching the concrete IP family being published.
fn address_record(name: &Name, addr: IpAddr) -> Record {
    match addr {
        IpAddr::V4(addr) => {
            Record::from_rdata(name.clone(), SERVICE_TTL_SECS, RData::A(addr.into()))
        }
        IpAddr::V6(addr) => {
            Record::from_rdata(name.clone(), SERVICE_TTL_SECS, RData::AAAA(addr.into()))
        }
    }
}

fn extract_service_label(name: &Name, network_name: &str) -> Option<String> {
    let mut labels = Vec::new();
    for raw in name.iter() {
        let lower = raw.to_ascii_lowercase();
        let label = match String::from_utf8(lower) {
            Ok(text) => text,
            Err(_) => return None,
        };
        labels.push(label);
    }
    let suffix_labels: Vec<&str> = SERVICE_ZONE_SUFFIX.split('.').collect();
    if labels.len() != suffix_labels.len() + 2 {
        return None;
    }
    for expected in suffix_labels.iter().rev() {
        if labels.pop()?.as_str() != *expected {
            return None;
        }
    }
    let network_label = labels.pop()?;
    if network_label != network_name.to_ascii_lowercase() {
        return None;
    }
    labels.pop()
}

async fn resolve_service_backends(
    registry: &NetworkRegistry,
    workloads: &WorkloadStore,
    template_index: &HashMap<Uuid, (String, String)>,
    network_id: Uuid,
    service_name: &str,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
) -> Result<Vec<BackendAddress>> {
    let expected_family = registry
        .get_spec(network_id)?
        .map(|spec| parse_overlay_cidr(&spec.subnet_cidr))
        .transpose()?
        .map(|subnet| subnet.family);
    let ready_peers: HashSet<Uuid> = registry
        .list_peer_states(Some(network_id))?
        .into_iter()
        .filter(|state| state.state.is_ready())
        .map(|state| state.peer_id)
        .collect();
    let attachments = registry
        .list_attachments(Some(network_id))
        .context("list attachments for discovery")?;
    let mut cache: HashMap<Uuid, Option<WorkloadValue>> = HashMap::new();
    let mut results = Vec::new();

    tracing::trace!(
        target: "network",
        network = %network_id,
        service = %service_name,
        attachments = attachments.len(),
        "resolving service backends"
    );

    for attachment in attachments {
        if !ready_peers.contains(&attachment.node_id) {
            tracing::trace!(
                target: "network",
                network = %network_id,
                attachment = %attachment.id,
                node = %attachment.node_id,
                "skipping attachment on node whose network peer state is not ready"
            );
            continue;
        }
        if matches!(
            health_snapshot.get(&attachment.node_id),
            Some(HealthStatus::Down)
        ) {
            tracing::trace!(
                target: "network",
                network = %network_id,
                attachment = %attachment.id,
                node = %attachment.node_id,
                "skipping attachment on down node"
            );
            continue;
        }
        if attachment.state != NetworkAttachmentState::Ready {
            tracing::trace!(
                target: "network",
                network = %network_id,
                attachment = %attachment.id,
                state = ?attachment.state,
                "skipping attachment not ready"
            );
            continue;
        }
        if !attachment.traffic_published {
            tracing::trace!(
                target: "network",
                network = %network_id,
                attachment = %attachment.id,
                "skipping attachment not published for traffic"
            );
            continue;
        }
        let Some(ip_text) = &attachment.assigned_ip else {
            tracing::debug!(
                target: "network",
                network = %network_id,
                attachment = %attachment.id,
                "skipping attachment without ip"
            );
            continue;
        };
        let Some(mac_text) = &attachment.mac else {
            tracing::debug!(
                target: "network",
                network = %network_id,
                attachment = %attachment.id,
                "skipping attachment without mac"
            );
            continue;
        };
        let task_entry = cache
            .entry(attachment.task_id)
            .or_insert_with(|| load_task(workloads, attachment.task_id));
        let task = match task_entry.as_ref() {
            Some(task) => task,
            None => {
                tracing::debug!(
                    target: "network",
                    network = %network_id,
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
        if let Some((_, template)) = template_index.get(&attachment.task_id) {
            template_match |= template.eq_ignore_ascii_case(service_name);
        }

        if !template_match {
            tracing::trace!(
                target: "network",
                network = %network_id,
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
                    network = %network_id,
                    attachment = %attachment.id,
                    task = %task.id,
                    expected_node = %task.node_id,
                    actual_node = %attachment.node_id,
                    "keeping attachment; task record appears stale"
                );
            } else {
                tracing::debug!(
                    target: "network",
                    network = %network_id,
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
                network = %network_id,
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
                    network = %network_id,
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
                network = %network_id,
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
                    network = %network_id,
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
        network = %network_id,
        service = %service_name,
        backends = results.len(),
        "resolved service backends"
    );

    Ok(results)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum HealthState {
    Unknown,
    Healthy,
    Unhealthy,
}

#[derive(Default)]
struct BackendHealth {
    statuses: HashMap<(Uuid, String), HashMap<IpAddr, HealthEntry>>,
}

impl BackendHealth {
    /// Returns the cached readiness entry for one backend, if discovery has probed it before.
    fn get_entry(
        &self,
        network_id: Uuid,
        service_name: &str,
        backend: IpAddr,
    ) -> Option<HealthEntry> {
        let key = (network_id, service_name.to_ascii_lowercase());
        self.statuses
            .get(&key)
            .and_then(|svc| svc.get(&backend).copied())
    }

    /// Persists the latest readiness observation for one backend.
    fn set_entry(
        &mut self,
        network_id: Uuid,
        service_name: &str,
        backend: IpAddr,
        entry_state: HealthEntry,
    ) {
        let key = (network_id, service_name.to_ascii_lowercase());
        let entry = self.statuses.entry(key).or_default();
        entry.insert(backend, entry_state);
    }
}

#[derive(Clone, Copy)]
struct HealthEntry {
    state: HealthState,
    checked_at: Instant,
    consecutive_failures: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ProbePriority {
    Unknown = 0,
    Unhealthy = 1,
    Healthy = 2,
}

/// Computes the next cached backend readiness state after one active probe attempt.
fn next_health_entry(
    previous: Option<HealthEntry>,
    probe: &ServiceReadinessProbe,
    probe_ok: bool,
) -> HealthEntry {
    let checked_at = Instant::now();
    if probe_ok {
        return HealthEntry {
            state: HealthState::Healthy,
            checked_at,
            consecutive_failures: 0,
        };
    }

    let failures = previous
        .map(|entry| entry.consecutive_failures)
        .unwrap_or_default()
        .saturating_add(1);
    let previous_state = previous
        .map(|entry| entry.state)
        .unwrap_or(HealthState::Unknown);
    let state = match previous_state {
        HealthState::Healthy if failures < probe.failure_threshold() => HealthState::Healthy,
        _ if failures >= probe.failure_threshold() => HealthState::Unhealthy,
        _ => HealthState::Unknown,
    };

    HealthEntry {
        state,
        checked_at,
        consecutive_failures: failures,
    }
}

/// Returns how long discovery should trust the cached readiness state before actively probing again.
///
/// Healthy backends are rechecked much more slowly than unknown or unhealthy ones so steady-state
/// clusters do not keep probing every replica on every node.
fn readiness_recheck_after(entry: Option<HealthEntry>, probe: &ServiceReadinessProbe) -> Duration {
    match entry
        .map(|value| value.state)
        .unwrap_or(HealthState::Unknown)
    {
        HealthState::Healthy => probe.interval().max(HEALTHY_READINESS_RECHECK_FLOOR),
        HealthState::Unknown | HealthState::Unhealthy => probe.interval(),
    }
}

/// Classifies which stale backends discovery should actively probe first on one refresh cycle.
///
/// Unknown and unhealthy backends are checked ahead of healthy ones so discovery converges quickly
/// on new or recovering replicas while steady-state healthy replicas are only spot-checked.
fn probe_priority(entry: Option<HealthEntry>) -> ProbePriority {
    match entry
        .map(|value| value.state)
        .unwrap_or(HealthState::Unknown)
    {
        HealthState::Unknown => ProbePriority::Unknown,
        HealthState::Unhealthy => ProbePriority::Unhealthy,
        HealthState::Healthy => ProbePriority::Healthy,
    }
}

/// Selects the bounded subset of backends that should receive active readiness probes on this tick.
///
/// The selection prefers stale unknown and unhealthy backends, then rotates through stale healthy
/// backends by oldest observation first. This keeps readiness traffic bounded without pinning the
/// same healthy replicas forever.
fn select_backends_for_active_probe(
    health: &BackendHealth,
    network_id: Uuid,
    service_name: &str,
    backends: &[BackendAddress],
    probe: &ServiceReadinessProbe,
) -> Vec<BackendAddress> {
    let now = Instant::now();
    let mut candidates = Vec::new();
    for backend in backends {
        let entry = health.get_entry(network_id, service_name, backend.ip);
        let recheck_after = readiness_recheck_after(entry, probe);
        let checked_at = entry.map(|value| value.checked_at);
        let is_stale = checked_at
            .map(|instant| now.saturating_duration_since(instant) >= recheck_after)
            .unwrap_or(true);
        if !is_stale {
            continue;
        }

        candidates.push((
            probe_priority(entry),
            checked_at,
            backend.ip,
            backend.clone(),
        ));
    }

    candidates.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    candidates
        .into_iter()
        .take(MAX_READINESS_PROBES_PER_REFRESH)
        .map(|(_, _, _, backend)| backend)
        .collect()
}

/// Filter candidate backends using cached health without performing probes.
///
/// This keeps DNS responses fast and avoids blocking on synchronous checks.
fn filter_cached_backends(
    health: &BackendHealth,
    network_id: Uuid,
    service_name: &str,
    backends: Vec<BackendAddress>,
) -> Vec<BackendAddress> {
    let now = Instant::now();
    let mut preferred = Vec::with_capacity(backends.len());
    let mut stale_unhealthy = Vec::with_capacity(backends.len());
    for backend in backends {
        let entry = health.get_entry(network_id, service_name, backend.ip);
        match entry {
            None => preferred.push(backend),
            Some(entry) => {
                let is_stale =
                    now.saturating_duration_since(entry.checked_at) >= HEALTH_CACHE_STALE_AFTER;
                match entry.state {
                    HealthState::Healthy => preferred.push(backend),
                    HealthState::Unknown if entry.consecutive_failures == 0 => {
                        preferred.push(backend)
                    }
                    HealthState::Unknown => {}
                    HealthState::Unhealthy => {
                        // Keep stale unhealthy endpoints out of normal DNS responses whenever at
                        // least one non-unhealthy endpoint exists. This avoids periodic latency
                        // spikes where clients connect-timeout on a dead backend before falling
                        // back to another A record.
                        if is_stale {
                            stale_unhealthy.push(backend);
                        }
                    }
                }
            }
        }
    }

    if preferred.is_empty() {
        stale_unhealthy
    } else {
        preferred
    }
}

/// Normalize one backend-selection result with shared fallback and stable ordering.
///
/// If health checks are disabled for the service and the selected set is empty, discovery falls
/// back to all ready/running endpoints derived from attachment records so the service remains
/// reachable without active probing.
///
/// Both DNS answering and periodic refresh use this helper to keep selection behavior in lockstep.
fn normalize_backend_selection(
    network_id: Uuid,
    service_name: &str,
    candidates: Vec<BackendAddress>,
    mut selected: Vec<BackendAddress>,
    health_checks_enabled: bool,
    source: &'static str,
) -> Vec<BackendAddress> {
    if selected.is_empty() && !candidates.is_empty() && !health_checks_enabled {
        warn!(
            target: "network",
            network = %network_id,
            service = %service_name,
            candidates = candidates.len(),
            source,
            "no healthy backends; falling back to candidate attachments"
        );
        selected = candidates;
    }
    sort_backends(&mut selected);
    selected
}

/// Actively probe a bounded subset of candidate backends so cached readiness and dataplane MACs
/// stay fresh without turning steady-state discovery into a full cluster-wide sweep.
async fn evaluate_backend_health(
    health: &Arc<AsyncMutex<BackendHealth>>,
    registry: &NetworkRegistry,
    network_id: Uuid,
    service_name: &str,
    backends: Vec<BackendAddress>,
    probe: Option<ServiceReadinessProbe>,
) -> Vec<BackendAddress> {
    // Readiness probing is opt-in. When no probe is provided we keep current behavior and return
    // all backends derived from ready attachments.
    let Some(probe) = probe else { return backends };

    let mut refreshed_macs = HashMap::new();
    let active_probes = {
        let guard = health.lock().await;
        select_backends_for_active_probe(&guard, network_id, service_name, &backends, &probe)
    };
    for backend in active_probes {
        let entry = {
            let guard = health.lock().await;
            guard.get_entry(network_id, service_name, backend.ip)
        };
        let previous_state = entry
            .map(|value| value.state)
            .unwrap_or(HealthState::Unknown);
        let probe_ok = probe_backend(&backend.ip, &probe).await;
        let refreshed_mac = if probe_ok {
            refresh_backend_mac(registry, network_id, backend.ip).await
        } else {
            None
        };
        let next_entry = next_health_entry(entry, &probe, probe_ok);
        let mut guard = health.lock().await;
        guard.set_entry(network_id, service_name, backend.ip, next_entry);
        if !matches!(next_entry.state, HealthState::Healthy) {
            tracing::debug!(
                target: "network",
                network = %network_id,
                service = %service_name,
                backend = %backend.ip,
                state = ?previous_state,
                failures = next_entry.consecutive_failures,
                threshold = probe.failure_threshold(),
                "backend failed readiness probe"
            );
        }

        if let Some(mac) = refreshed_mac {
            refreshed_macs.insert(backend.ip, mac);
        }
    }

    let backends = backends
        .into_iter()
        .map(|mut backend| {
            if let Some(mac) = refreshed_macs.get(&backend.ip) {
                backend.mac = *mac;
            }
            backend
        })
        .collect();
    let guard = health.lock().await;
    filter_cached_backends(&guard, network_id, service_name, backends)
}

/// Refresh the MAC address stored for a backend by querying kernel neighbour tables and persisting
/// the new value into the attachment registry so downstream dataplane programming uses the right
/// L2 destination.
async fn refresh_backend_mac(
    registry: &NetworkRegistry,
    network_id: Uuid,
    ip: IpAddr,
) -> Option<[u8; 6]> {
    let mac = resolve_neighbor_mac(network_id, ip).await?;

    if let Ok(attachments) = registry.list_attachments(Some(network_id)) {
        for mut attachment in attachments {
            if attachment.assigned_ip.as_deref() == Some(&ip.to_string()) {
                let formatted = format!(
                    "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
                );
                if attachment.mac.as_deref() == Some(formatted.as_str()) {
                    break;
                }
                attachment.mac = Some(formatted);
                let _ = registry.upsert_attachment(attachment).await;
                break;
            }
        }
    }

    Some(mac)
}

/// Look up the current MAC associated with a backend IP by scanning neighbour entries on the
/// overlay bridge and VXLAN devices so we can recover when containers are restarted out of band.
#[cfg(target_os = "linux")]
async fn resolve_neighbor_mac(network_id: Uuid, ip: IpAddr) -> Option<[u8; 6]> {
    use futures::TryStreamExt;

    let (conn, handle, _) = match rtnetlink::new_connection() {
        Ok(parts) => parts,
        Err(_) => return None,
    };
    tokio::spawn(conn);

    let bridge = bridge_name(network_id);
    let bridge_index = match handle
        .link()
        .get()
        .match_name(bridge.clone())
        .execute()
        .try_next()
        .await
    {
        Ok(Some(msg)) => Some(msg.header.index),
        _ => None,
    };

    let vxlan = vxlan_name(network_id);
    let vxlan_index = match handle
        .link()
        .get()
        .match_name(vxlan.clone())
        .execute()
        .try_next()
        .await
    {
        Ok(Some(msg)) => Some(msg.header.index),
        _ => None,
    };

    if bridge_index.is_none() && vxlan_index.is_none() {
        return None;
    }

    let mut neighs = handle.neighbours().get().execute();
    while let Ok(Some(msg)) = neighs.try_next().await {
        let on_bridge = bridge_index
            .map(|idx| idx == msg.header.ifindex)
            .unwrap_or(false);
        let on_vxlan = vxlan_index
            .map(|idx| idx == msg.header.ifindex)
            .unwrap_or(false);
        if !on_bridge && !on_vxlan {
            continue;
        }

        let mut found_ip = false;
        let mut found_mac: Option<[u8; 6]> = None;
        for nla in &msg.attributes {
            use rtnetlink::packet_route::neighbour::{NeighbourAddress, NeighbourAttribute};
            match nla {
                NeighbourAttribute::Destination(NeighbourAddress::Inet(addr))
                    if ip == IpAddr::V4(*addr) =>
                {
                    found_ip = true;
                }
                NeighbourAttribute::Destination(NeighbourAddress::Inet6(addr))
                    if ip == IpAddr::V6(*addr) =>
                {
                    found_ip = true;
                }
                NeighbourAttribute::LinkLocalAddress(ll) if ll.len() == 6 => {
                    let mut mac = [0u8; 6];
                    mac.copy_from_slice(ll);
                    found_mac = Some(mac);
                }
                _ => {}
            }
        }

        if found_ip && let Some(mac) = found_mac {
            return Some(mac);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
async fn resolve_neighbor_mac(_network_id: Uuid, _ip: IpAddr) -> Option<[u8; 6]> {
    None
}

async fn probe_backend(ip: &IpAddr, probe: &ServiceReadinessProbe) -> bool {
    match probe.kind {
        ServiceReadinessProbeKind::Http => {
            probe_backend_http(
                ip,
                probe.port,
                probe.http_path().unwrap_or("/"),
                probe.timeout(),
            )
            .await
        }
        ServiceReadinessProbeKind::Tcp => probe_backend_tcp(ip, probe.port, probe.timeout()).await,
    }
}

/// Convert a service protocol descriptor into a nodeport transport selector.
fn nodeport_protocol(protocol: ServicePortProtocol) -> NodePortProtocol {
    match protocol {
        ServicePortProtocol::Tcp => NodePortProtocol::Tcp,
        ServicePortProtocol::Udp => NodePortProtocol::Udp,
        // TcpUdp is expanded into both entries by TaskTemplateSpecValue::public_protocols.
        ServicePortProtocol::TcpUdp => NodePortProtocol::Tcp,
    }
}

async fn probe_backend_tcp(ip: &IpAddr, port: u16, timeout: Duration) -> bool {
    let addr = SocketAddr::new(*ip, port);
    matches!(
        tokio::time::timeout(timeout, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

async fn probe_backend_http(ip: &IpAddr, port: u16, path: &str, timeout: Duration) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = SocketAddr::new(*ip, port);
    let path = if path.is_empty() { "/" } else { path };
    let mut stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        _ => return false,
    };

    let host = http_host_literal(*ip);
    let request = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n");
    if tokio::time::timeout(timeout, stream.write_all(request.as_bytes()))
        .await
        .is_err()
    {
        return false;
    }

    let mut buf = [0u8; 128];
    match tokio::time::timeout(timeout, stream.read(&mut buf)).await {
        Ok(Ok(n)) if n > 0 => {
            let prefix = &buf[..n];
            prefix.starts_with(b"HTTP/1.1 2") || prefix.starts_with(b"HTTP/1.0 2")
        }
        _ => false,
    }
}

/// Render IP literals for HTTP Host headers, bracketing IPv6 addresses as required by URI syntax.
fn http_host_literal(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

/// Refresh the per-network backend catalog when any upstream generation (attachments, tasks,
/// services) or peer-health snapshot has changed.
///
/// This centralizes backend resolution so both DNS answers and periodic refresh reuse the same
/// computed candidate set.
async fn refresh_backend_catalog_if_needed(
    backend_catalog: &Arc<AsyncMutex<NetworkBackendCatalog>>,
    registry: &NetworkRegistry,
    workloads: &WorkloadStore,
    services: &ServiceRegistry,
    network_id: Uuid,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
) -> Result<()> {
    let attachment_generation = registry.attachment_change_clock();
    let workload_generation = workloads.change_clock();
    let service_generation = services.change_clock();
    let peer_generation = registry.peer_change_clock();
    let health_fingerprint = health_snapshot_fingerprint(health_snapshot);

    {
        let guard = backend_catalog.lock().await;
        if guard.attachment_generation == attachment_generation
            && guard.workload_generation == workload_generation
            && guard.service_generation == service_generation
            && guard.peer_generation == peer_generation
            && guard.health_fingerprint == health_fingerprint
        {
            return Ok(());
        }
    }

    let service_specs = services
        .list()
        .context("load service specs for backend catalog refresh")?;
    let template_index = build_task_template_index(&service_specs);
    let mut next_services = HashMap::new();
    for spec in &service_specs {
        for template in &spec.task_templates {
            if !template
                .networks
                .iter()
                .any(|net| net.network_id == network_id)
            {
                continue;
            }

            let service_name = template.name.clone();
            let candidates = resolve_service_backends(
                registry,
                workloads,
                &template_index,
                network_id,
                &service_name,
                health_snapshot,
            )
            .await?;
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

    let mut guard = backend_catalog.lock().await;
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
#[expect(
    clippy::too_many_arguments,
    reason = "network refresh keeps one explicit private control-plane context"
)]
async fn refresh_network_services(
    registry: &NetworkRegistry,
    workloads: &WorkloadStore,
    services: &ServiceRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    health: &Arc<AsyncMutex<BackendHealth>>,
    bpf_lb: &BpfLoadBalancer,
    nodeport: &NodePortManager,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
    health_monitor: &Arc<HealthMonitor>,
    backend_catalog: &Arc<AsyncMutex<NetworkBackendCatalog>>,
) -> Result<()> {
    let health_snapshot = health_monitor.snapshot();
    refresh_backend_catalog_if_needed(
        backend_catalog,
        registry,
        workloads,
        services,
        network_id,
        &health_snapshot,
    )
    .await?;
    let entries: Vec<ServiceBackendCatalogEntry> = {
        let guard = backend_catalog.lock().await;
        guard.services.values().cloned().collect()
    };
    let mut nodeport_entries = Vec::new();
    let mut public_endpoint_observations = Vec::new();
    let mut host_vips = HashSet::new();

    for entry in entries {
        let result = refresh_single_service(
            registry, bpf, network_id, &entry, health, bpf_lb, lb_missing,
        )
        .await?;
        nodeport_entries.extend(result.nodeport_mappings);
        if let Some(observation) = result.public_endpoint {
            public_endpoint_observations.push(observation);
        }
        if let Some(vip) = result.host_vip {
            host_vips.insert(vip);
        }
    }

    if let Err(err) = nodeport.sync_ports(network_id, &nodeport_entries).await {
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
            network = %network_id,
            "failed to sync public nodeport mappings: {err:#}"
        );
    }
    if let Err(err) = reconcile_host_vip_neighbors(network_id, &host_vips).await {
        warn!(
            target: "network",
            network = %network_id,
            "failed to reconcile host vip neighbours: {err:#}"
        );
    }
    apply_public_endpoint_observations(services, public_endpoint_observations.as_slice()).await?;

    Ok(())
}

/// Refresh healthy backend list and VIP programming for a single service so clients connected via
/// the VIP can fail over even if they do not issue new DNS lookups.
///
/// Returns nodeport mappings when the service is marked public so external listeners can be
/// reconciled by the caller.
async fn refresh_single_service(
    registry: &NetworkRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    service: &ServiceBackendCatalogEntry,
    health: &Arc<AsyncMutex<BackendHealth>>,
    bpf_lb: &BpfLoadBalancer,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
) -> Result<ServiceRefreshResult> {
    let service_name = service.service_name.as_str();
    let candidates = service.candidates.clone();
    let mut backends = evaluate_backend_health(
        health,
        registry,
        network_id,
        service_name,
        candidates.clone(),
        service.readiness.clone(),
    )
    .await;

    backends = normalize_backend_selection(
        network_id,
        service_name,
        candidates,
        backends,
        service.readiness.is_some(),
        "refresh",
    );

    if backends.is_empty() {
        let _ = sync_service_vip_for_backends(
            bpf_lb,
            bpf,
            lb_missing,
            registry,
            network_id,
            service_name,
            &[],
            service.expose_to_host,
        )
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

    if let Some((vip, programmed)) = sync_service_vip_for_backends(
        bpf_lb,
        bpf,
        lb_missing,
        registry,
        network_id,
        service_name,
        &backends,
        service.expose_to_host,
    )
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

/// Persists the current public endpoint outcome back into the replicated service row.
///
/// Public ingress is a user-visible contract. When NodePort or VIP programming degrades, the
/// owning service should expose that explicitly instead of silently presenting internal DNS
/// fallback as success.
async fn apply_public_endpoint_observations(
    services: &ServiceRegistry,
    observations: &[PublicEndpointObservation],
) -> Result<()> {
    let mut issues_by_service: HashMap<Uuid, Vec<String>> = HashMap::new();
    let mut service_ids = HashSet::new();
    for observation in observations {
        service_ids.insert(observation.service_id);
        let issues = issues_by_service.entry(observation.service_id).or_default();
        if let Some(detail) = observation.detail.as_ref() {
            issues.push(detail.clone());
        }
    }

    for service_id in service_ids {
        let Some(mut spec) = services.get(service_id)? else {
            continue;
        };
        if spec.status() != ServiceStatus::Running {
            continue;
        }

        let next_detail = issues_by_service
            .get(&service_id)
            .filter(|issues| !issues.is_empty())
            .map(|issues| summarize_public_endpoint_issues(issues));
        let current_detail = spec.public_endpoint_detail().map(str::to_string);

        if current_detail == next_detail {
            continue;
        }
        if next_detail.is_none() && spec.public_endpoint_detail().is_none() {
            continue;
        }

        spec.set_public_endpoint_detail(next_detail);
        services
            .upsert(spec)
            .await
            .context("persist public endpoint service detail")?;
    }

    Ok(())
}

/// Compresses one service's public endpoint issues into the single lifecycle detail field.
fn summarize_public_endpoint_issues(issues: &[String]) -> String {
    let Some(first) = issues.first() else {
        return String::new();
    };
    if issues.len() == 1 {
        return first.clone();
    }
    format!(
        "{first}; +{} more public endpoint issue(s)",
        issues.len() - 1
    )
}

/// Compute and synchronize one service VIP for the provided backend set.
///
/// Returns the selected VIP and whether dataplane programming succeeded.
#[allow(clippy::too_many_arguments)]
async fn sync_service_vip_for_backends(
    bpf_lb: &BpfLoadBalancer,
    bpf: &NetworkBpfManager,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
    registry: &NetworkRegistry,
    network_id: Uuid,
    service_name: &str,
    backends: &[BackendAddress],
    expose_to_host: bool,
) -> Result<Option<(IpAddr, bool)>> {
    let Some((vip, mac)) = compute_service_vip(registry, network_id, service_name, backends)?
    else {
        return Ok(None);
    };
    let programmed = program_service_vip(
        bpf_lb,
        bpf,
        lb_missing,
        registry,
        network_id,
        service_name,
        vip,
        mac,
        backends,
        expose_to_host,
    )
    .await;
    Ok(Some((vip, programmed)))
}

/// Attempt to synchronize VIP metadata into the eBPF maps if they are available, returning whether
/// the dataplane was programmed successfully. Missing maps are warned once per network.
#[expect(
    clippy::too_many_arguments,
    reason = "vip programming needs the private dataplane and service metadata bundle"
)]
async fn program_service_vip(
    bpf_lb: &BpfLoadBalancer,
    bpf: &NetworkBpfManager,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
    registry: &NetworkRegistry,
    network_id: Uuid,
    service_name: &str,
    vip: IpAddr,
    vip_mac: [u8; 6],
    backends: &[BackendAddress],
    expose_to_host: bool,
) -> bool {
    match bpf_lb.sync_vip(network_id, vip, vip_mac, backends) {
        Ok(()) => {
            if expose_to_host
                && let Err(err) = ensure_host_vip_neighbor(network_id, vip, vip_mac).await
            {
                debug!(
                    target: "network",
                    network = %network_id,
                    service = %service_name,
                    vip = %vip,
                    "failed to program host neighbour for vip (continuing): {err:#}"
                );
            }
            let mut guard = lb_missing.lock().await;
            guard.remove(&network_id);
            true
        }
        Err(err) => {
            // Attempt to heal the maps by re-ensuring BPF programs, then retry once.
            let healed = heal_lb_maps(bpf, registry, network_id).await;
            if healed.is_ok() && bpf_lb.sync_vip(network_id, vip, vip_mac, backends).is_ok() {
                if expose_to_host
                    && let Err(err) = ensure_host_vip_neighbor(network_id, vip, vip_mac).await
                {
                    debug!(
                        target: "network",
                        network = %network_id,
                        service = %service_name,
                        vip = %vip,
                        "failed to program host neighbour for vip after healing (continuing): {err:#}"
                    );
                }
                let mut guard = lb_missing.lock().await;
                guard.remove(&network_id);
                return true;
            }

            let mut guard = lb_missing.lock().await;
            if guard.insert(network_id) {
                warn!(
                    target: "network",
                    network = %network_id,
                    service = %service_name,
                    "failed to sync bpf vip for service; falling back to dns round robin: {err:#}"
                );
            }
            false
        }
    }
}

/// Ensure the local host has a stable neighbour entry for a service VIP.
///
/// Host-originated traffic enters the overlay via a dedicated `mnhost-*` interface. Without an
/// ARP reply, the host neighbour table can remain in `FAILED` and prevent `curl` from reaching
/// the VIP. Programming a permanent neighbour entry ties the VIP to the deterministic VIP MAC so
/// packets reach the bridge tc-ingress load balancer immediately.
async fn ensure_host_vip_neighbor(network_id: Uuid, vip: IpAddr, vip_mac: [u8; 6]) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (network_id, vip, vip_mac);
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        use futures::TryStreamExt;
        use rtnetlink::packet_route::neighbour::NeighbourState;

        let (conn, handle, _) =
            rtnetlink::new_connection().context("open rtnetlink connection for vip neighbour")?;
        tokio::spawn(conn);

        let host_ifname = host_access_host_iface_name(network_id);
        let host_index = match handle
            .link()
            .get()
            .match_name(host_ifname.clone())
            .execute()
            .try_next()
            .await
        {
            Ok(Some(msg)) => msg.header.index,
            Ok(None) => {
                debug!(
                    target: "network",
                    network = %network_id,
                    iface = %host_ifname,
                    "host access interface missing while programming vip neighbour"
                );
                return Ok(());
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("lookup host access interface {host_ifname} for vip neighbour")
                });
            }
        };

        handle
            .neighbours()
            .add(host_index, vip)
            .link_local_address(&vip_mac)
            .state(NeighbourState::Permanent)
            .replace()
            .execute()
            .await
            .with_context(|| format!("program vip neighbour entry for {vip} on {host_ifname}"))?;

        Ok(())
    }
}

/// Remove stale permanent host VIP neighbours that no longer belong to any published service.
async fn reconcile_host_vip_neighbors(
    network_id: Uuid,
    desired_vips: &HashSet<IpAddr>,
) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (network_id, desired_vips);
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        use futures::{StreamExt, TryStreamExt};
        use rtnetlink::packet_core::{NLM_F_ACK, NLM_F_REQUEST, NetlinkMessage, NetlinkPayload};
        use rtnetlink::packet_route::neighbour::{
            NeighbourAddress, NeighbourAttribute, NeighbourMessage, NeighbourState,
        };
        use rtnetlink::packet_route::{AddressFamily, RouteNetlinkMessage};

        let (conn, handle, _) = rtnetlink::new_connection()
            .context("open rtnetlink connection for vip neighbour gc")?;
        tokio::spawn(conn);

        let host_ifname = host_access_host_iface_name(network_id);
        let host_index = match handle
            .link()
            .get()
            .match_name(host_ifname.clone())
            .execute()
            .try_next()
            .await
        {
            Ok(Some(msg)) => msg.header.index,
            Ok(None) => return Ok(()),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("lookup host access interface {host_ifname} for vip neighbour gc")
                });
            }
        };

        let mut stale_vips = Vec::new();
        let mut neighs = handle.neighbours().get().execute();
        while let Ok(Some(msg)) = neighs.try_next().await {
            if msg.header.ifindex != host_index || msg.header.state != NeighbourState::Permanent {
                continue;
            }

            let vip = msg.attributes.iter().find_map(|attr| match attr {
                NeighbourAttribute::Destination(NeighbourAddress::Inet(v4)) => {
                    Some(IpAddr::V4(*v4))
                }
                NeighbourAttribute::Destination(NeighbourAddress::Inet6(v6)) => {
                    Some(IpAddr::V6(*v6))
                }
                _ => None,
            });
            if let Some(vip) = vip
                && !desired_vips.contains(&vip)
            {
                stale_vips.push(vip);
            }
        }

        for vip in stale_vips {
            let mut message = NeighbourMessage::default();
            message.header.family = match vip {
                IpAddr::V4(_) => AddressFamily::Inet,
                IpAddr::V6(_) => AddressFamily::Inet6,
            };
            message.header.ifindex = host_index;
            let destination = match vip {
                IpAddr::V4(vip) => NeighbourAddress::Inet(vip),
                IpAddr::V6(vip) => NeighbourAddress::Inet6(vip),
            };
            message
                .attributes
                .push(NeighbourAttribute::Destination(destination));

            let mut request = NetlinkMessage::from(RouteNetlinkMessage::DelNeighbour(message));
            request.header.flags = NLM_F_REQUEST | NLM_F_ACK;
            let mut responses = handle.clone().request(request).with_context(|| {
                format!("submit vip neighbour delete for {vip} on {host_ifname}")
            })?;
            while let Some(message) = responses.next().await {
                if let NetlinkPayload::Error(err) = message.payload {
                    return Err(rtnetlink::Error::NetlinkError(err)).with_context(|| {
                        format!("delete stale host vip neighbour {vip} on {host_ifname}")
                    });
                }
            }
        }

        Ok(())
    }
}

/// Reconcile BPF programs for a network when VIP map access fails so pinned maps can be recreated.
async fn heal_lb_maps(
    bpf: &NetworkBpfManager,
    registry: &NetworkRegistry,
    network_id: Uuid,
) -> Result<()> {
    let Some(spec) = registry.get_spec(network_id)? else {
        bail!("network spec {network_id} missing while healing LB maps");
    };
    let mut attach_spec = spec.clone();
    if attach_spec.bpf_programs.is_empty() {
        attach_spec.bpf_programs = default_bpf_programs();
    }

    let attachment_ifnames = registry
        .list_attachments(Some(network_id))?
        .into_iter()
        .map(|attachment| crate::network::attachment::host_iface_name(attachment.id));
    let interfaces =
        NetworkInterfaceContext::new(network_id, bridge_name(network_id), vxlan_name(network_id))
            .with_attachment_host_ifnames(attachment_ifnames);
    bpf.ensure_network(&attach_spec, &interfaces).await
}

fn default_bpf_programs() -> Vec<BpfProgramSpec> {
    vec![
        BpfProgramSpec::with_attach_point("vxlan_xdp", BpfAttachPoint::VxlanXdp),
        BpfProgramSpec::with_attach_point("bridge_xdp", BpfAttachPoint::BridgeXdp),
        BpfProgramSpec::with_attach_point("bridge_tc_ingress", BpfAttachPoint::BridgeTcIngress),
        BpfProgramSpec::with_attach_point("bridge_tc_egress", BpfAttachPoint::BridgeTcEgress),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::registry::NetworkRegistry;
    use crate::network::types::{
        NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver,
        NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecValue,
    };
    use crate::services::registry::ServiceRegistry;
    use crate::services::types::{
        ServiceSpecValue, TaskTemplateNetworkRequirement, TaskTemplateSpecValue,
    };
    use crate::store::network_store::{
        open_network_attachment_store, open_network_peer_store, open_network_spec_store,
    };
    use crate::store::service_store::open_service_store;
    use crate::store::workload_store::{WorkloadStore, open_workload_store};
    use crate::workload::model::{
        WorkloadOwner, WorkloadPhase, WorkloadServiceMetadata, WorkloadValue, WorkloadValueDraft,
    };
    use crate::workload::types::ExecutionSpec;
    use crdt_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn backend(ip: [u8; 4], mac: [u8; 6]) -> BackendAddress {
        BackendAddress {
            ip: IpAddr::V4(Ipv4Addr::from(ip)),
            mac,
        }
    }

    #[test]
    fn filter_cached_backends_excludes_stale_unhealthy_when_alternative_exists() {
        let network_id = Uuid::new_v4();
        let service = "backend";
        let unhealthy_ip = Ipv4Addr::new(10, 42, 1, 10);
        let healthy_ip = Ipv4Addr::new(10, 42, 1, 11);

        let mut health = BackendHealth::default();
        let key = (network_id, service.to_string());
        health.statuses.entry(key).or_default().insert(
            IpAddr::V4(unhealthy_ip),
            HealthEntry {
                state: HealthState::Unhealthy,
                checked_at: Instant::now() - HEALTH_CACHE_STALE_AFTER - Duration::from_secs(1),
                consecutive_failures: 1,
            },
        );

        let filtered = filter_cached_backends(
            &health,
            network_id,
            service,
            vec![
                backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1]),
                backend([10, 42, 1, 11], [0x02, 0, 0, 0, 0, 2]),
            ],
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].ip, IpAddr::V4(healthy_ip));
    }

    #[test]
    fn filter_cached_backends_keeps_stale_unhealthy_when_only_choice() {
        let network_id = Uuid::new_v4();
        let service = "backend";
        let unhealthy_ip = Ipv4Addr::new(10, 42, 1, 10);

        let mut health = BackendHealth::default();
        let key = (network_id, service.to_string());
        health.statuses.entry(key).or_default().insert(
            IpAddr::V4(unhealthy_ip),
            HealthEntry {
                state: HealthState::Unhealthy,
                checked_at: Instant::now() - HEALTH_CACHE_STALE_AFTER - Duration::from_secs(1),
                consecutive_failures: 1,
            },
        );

        let filtered = filter_cached_backends(
            &health,
            network_id,
            service,
            vec![backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1])],
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].ip, IpAddr::V4(unhealthy_ip));
    }

    #[test]
    fn readiness_recheck_after_slows_healthy_backends() {
        let probe = ServiceReadinessProbe {
            kind: ServiceReadinessProbeKind::Http,
            port: 8_000,
            path: Some("/healthz".to_string()),
            interval_ms: 2_000,
            timeout_ms: 300,
            failure_threshold: 1,
        };
        let healthy = Some(HealthEntry {
            state: HealthState::Healthy,
            checked_at: Instant::now(),
            consecutive_failures: 0,
        });
        let unhealthy = Some(HealthEntry {
            state: HealthState::Unhealthy,
            checked_at: Instant::now(),
            consecutive_failures: 1,
        });

        assert_eq!(
            readiness_recheck_after(healthy, &probe),
            HEALTHY_READINESS_RECHECK_FLOOR
        );
        assert_eq!(readiness_recheck_after(unhealthy, &probe), probe.interval());
        assert_eq!(readiness_recheck_after(None, &probe), probe.interval());
    }

    #[test]
    fn select_backends_for_active_probe_prioritizes_unknown_and_unhealthy() {
        let network_id = Uuid::new_v4();
        let service = "backend";
        let probe = ServiceReadinessProbe {
            kind: ServiceReadinessProbeKind::Http,
            port: 8_000,
            path: Some("/healthz".to_string()),
            interval_ms: 2_000,
            timeout_ms: 300,
            failure_threshold: 1,
        };
        let mut health = BackendHealth::default();
        let key = (network_id, service.to_string());
        health.statuses.entry(key).or_default().insert(
            IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)),
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now()
                    - HEALTHY_READINESS_RECHECK_FLOOR
                    - Duration::from_secs(5),
                consecutive_failures: 0,
            },
        );
        health
            .statuses
            .get_mut(&(network_id, service.to_string()))
            .expect("service health entry")
            .insert(
                IpAddr::V4(Ipv4Addr::new(10, 42, 1, 12)),
                HealthEntry {
                    state: HealthState::Unhealthy,
                    checked_at: Instant::now() - probe.interval() - Duration::from_millis(1),
                    consecutive_failures: 1,
                },
            );

        let selected = select_backends_for_active_probe(
            &health,
            network_id,
            service,
            &[
                backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1]),
                backend([10, 42, 1, 11], [0x02, 0, 0, 0, 0, 2]),
                backend([10, 42, 1, 12], [0x02, 0, 0, 0, 0, 3]),
            ],
            &probe,
        );

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)));
        assert_eq!(selected[1].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 12)));
    }

    #[test]
    fn select_backends_for_active_probe_rotates_oldest_healthy_entries() {
        let network_id = Uuid::new_v4();
        let service = "backend";
        let probe = ServiceReadinessProbe {
            kind: ServiceReadinessProbeKind::Http,
            port: 8_000,
            path: Some("/healthz".to_string()),
            interval_ms: 2_000,
            timeout_ms: 300,
            failure_threshold: 1,
        };
        let mut health = BackendHealth::default();
        let key = (network_id, service.to_string());
        let mut entries = HashMap::new();
        entries.insert(
            IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)),
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now()
                    - HEALTHY_READINESS_RECHECK_FLOOR
                    - Duration::from_secs(30),
                consecutive_failures: 0,
            },
        );
        entries.insert(
            IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)),
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now()
                    - HEALTHY_READINESS_RECHECK_FLOOR
                    - Duration::from_secs(20),
                consecutive_failures: 0,
            },
        );
        entries.insert(
            IpAddr::V4(Ipv4Addr::new(10, 42, 1, 12)),
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now()
                    - HEALTHY_READINESS_RECHECK_FLOOR
                    - Duration::from_secs(10),
                consecutive_failures: 0,
            },
        );
        entries.insert(
            IpAddr::V4(Ipv4Addr::new(10, 42, 1, 13)),
            HealthEntry {
                state: HealthState::Healthy,
                checked_at: Instant::now(),
                consecutive_failures: 0,
            },
        );
        health.statuses.insert(key, entries);

        let selected = select_backends_for_active_probe(
            &health,
            network_id,
            service,
            &[
                backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1]),
                backend([10, 42, 1, 11], [0x02, 0, 0, 0, 0, 2]),
                backend([10, 42, 1, 12], [0x02, 0, 0, 0, 0, 3]),
                backend([10, 42, 1, 13], [0x02, 0, 0, 0, 0, 4]),
            ],
            &probe,
        );

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)));
        assert_eq!(selected[1].ip, IpAddr::V4(Ipv4Addr::new(10, 42, 1, 11)));
    }

    struct CatalogHarness {
        registry: NetworkRegistry,
        workloads: WorkloadStore,
        services: ServiceRegistry,
        network: NetworkSpecValue,
    }

    /// Creates isolated stores backing one discovery catalog test harness.
    async fn setup_catalog_harness() -> CatalogHarness {
        let actor = Uuid::new_v4();

        let network_dir = tempdir().expect("network tempdir");
        let network_path = network_dir
            .path()
            .join(format!("network-{}.redb", Uuid::new_v4()));
        let network_db = Arc::new(redb::Database::create(network_path).expect("create network db"));
        let spec_store =
            open_network_spec_store(network_db.clone(), actor).expect("open network spec store");
        spec_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network spec store");
        let peer_store =
            open_network_peer_store(network_db.clone(), actor).expect("open network peer store");
        peer_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network peer store");
        let attachment_store = open_network_attachment_store(network_db, actor)
            .expect("open network attachment store");
        attachment_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild network attachment store");
        let registry = NetworkRegistry::new(spec_store, peer_store, attachment_store);

        let task_dir = tempdir().expect("task tempdir");
        let task_path = task_dir
            .path()
            .join(format!("task-{}.redb", Uuid::new_v4()));
        let task_db = Arc::new(redb::Database::create(task_path).expect("create task db"));
        let workloads = open_workload_store(task_db, actor).expect("open workload store");
        workloads
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild workload store");

        let service_dir = tempdir().expect("service tempdir");
        let service_path = service_dir
            .path()
            .join(format!("service-{}.redb", Uuid::new_v4()));
        let service_db = Arc::new(redb::Database::create(service_path).expect("create service db"));
        let service_store = open_service_store(service_db, actor).expect("open service store");
        service_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild service store");
        let services = ServiceRegistry::new(service_store);

        let network = NetworkSpecValue::new(NetworkSpecDraft {
            name: format!("catalog-net-{}", Uuid::new_v4()),
            description: "discovery catalog test".to_string(),
            driver: NetworkDriver::Vxlan,
            subnet_cidr: "10.88.0.0/16".to_string(),
            vni: 4242,
            mtu: 1350,
            sealed: false,
            bpf_programs: Vec::new(),
        });
        registry
            .upsert_spec(network.clone())
            .await
            .expect("upsert network spec");

        CatalogHarness {
            registry,
            workloads,
            services,
            network,
        }
    }

    /// Builds one running task value used by catalog invalidation tests.
    fn catalog_task(
        task_id: Uuid,
        node_id: Uuid,
        service_name: &str,
        network_id: Uuid,
    ) -> WorkloadValue {
        let now = chrono::Utc::now().to_rfc3339();
        WorkloadValue::new(WorkloadValueDraft {
            id: task_id,
            name: "backend".to_string(),
            image: "hashicorp/http-echo:1.0.0".to_string(),
            execution_platform: crate::workload::model::ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            state: WorkloadPhase::Running,
            phase_reason: None,
            phase_progress: None,
            created_at: now.clone(),
            updated_at: now,
            command: Vec::new(),
            tty: false,
            node_id,
            node_name: format!("node-{node_id}"),
            slot_ids: vec![1, 2],
            networks: vec![network_id],
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            termination_grace_period_secs: None,
            pre_stop_command: None,
            liveness: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                service_name,
                "backend",
            ))),
            lease_id: None,
            lease_coordinator_node_id: None,
            task_epoch: 0,
            phase_version: 0,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        })
    }

    /// Builds one ready attachment row for the provided task/backend pair.
    fn catalog_attachment(
        task_id: Uuid,
        node_id: Uuid,
        network_id: Uuid,
        backend_ip: Ipv4Addr,
        service_name: &str,
    ) -> NetworkAttachmentValue {
        NetworkAttachmentValue::new(NetworkAttachmentDraft {
            id: crate::network::types::compute_network_attachment_id(task_id, network_id),
            task_id,
            node_id,
            instance_id: format!("container-{task_id}"),
            network_id,
            task_updated_at: Some(chrono::Utc::now().to_rfc3339()),
            requested_ip: Some(backend_ip.to_string()),
            assigned_ip: Some(backend_ip.to_string()),
            mac: Some("02:aa:bb:cc:dd:01".to_string()),
            state: NetworkAttachmentState::Ready,
            error: None,
            traffic_published: true,
            service_name: Some(service_name.to_string()),
            template_name: Some("backend".to_string()),
        })
    }

    /// Writes one task template that maps the backend name to the provided network.
    async fn upsert_catalog_service(
        services: &ServiceRegistry,
        service_name: &str,
        network_id: Uuid,
        task_ids: Vec<Uuid>,
    ) {
        upsert_catalog_service_with_readiness(services, service_name, network_id, task_ids, None)
            .await;
    }

    /// Writes one task template with optional backend readiness metadata for catalog tests.
    async fn upsert_catalog_service_with_readiness(
        services: &ServiceRegistry,
        service_name: &str,
        network_id: Uuid,
        replica_ids: Vec<Uuid>,
        readiness: Option<ServiceReadinessProbe>,
    ) {
        upsert_catalog_service_with_public_port(
            services,
            service_name,
            network_id,
            replica_ids,
            readiness,
            None,
        )
        .await;
    }

    /// Writes one task template with optional readiness and public NodePort metadata.
    async fn upsert_catalog_service_with_public_port(
        services: &ServiceRegistry,
        service_name: &str,
        network_id: Uuid,
        replica_ids: Vec<Uuid>,
        readiness: Option<ServiceReadinessProbe>,
        public_port: Option<u16>,
    ) {
        let service = ServiceSpecValue::new(
            Uuid::new_v4(),
            "catalog-test-manifest",
            service_name,
            vec![TaskTemplateSpecValue {
                name: "backend".to_string(),
                execution: ExecutionSpec {
                    image: "hashicorp/http-echo:1.0.0".to_string(),
                    command: Vec::new(),
                    tty: false,
                    cpu_millis: 100,
                    memory_bytes: 64 * 1024 * 1024,
                    gpu_count: 0,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    liveness: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: vec![TaskTemplateNetworkRequirement::new("default", network_id)],
                    placement: Default::default(),
                },
                depends_on: Vec::new(),
                replicas: replica_ids.len() as u16,
                readiness,
                public_port,
                public_protocol: None,
            }],
            replica_ids,
        );
        services
            .upsert(service)
            .await
            .expect("upsert catalog service");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_catalog_refresh_invalidates_on_task_change_clock() {
        let harness = setup_catalog_harness().await;
        let service_name = "backend-service";
        let node_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        upsert_catalog_service(
            &harness.services,
            service_name,
            harness.network.id,
            vec![task_id],
        )
        .await;

        harness
            .workloads
            .upsert(
                &UuidKey::from(task_id),
                catalog_task(task_id, node_id, service_name, harness.network.id),
            )
            .await
            .expect("upsert running task");
        harness
            .registry
            .upsert_attachment(catalog_attachment(
                task_id,
                node_id,
                harness.network.id,
                Ipv4Addr::new(10, 88, 1, 10),
                service_name,
            ))
            .await
            .expect("upsert ready attachment");
        harness
            .registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                harness.network.id,
                node_id,
                "backend-node",
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert ready peer state");

        let catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
        let mut health = HashMap::new();
        health.insert(node_id, HealthStatus::Alive);
        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.workloads,
            &harness.services,
            harness.network.id,
            &health,
        )
        .await
        .expect("initial catalog refresh");
        let initial_workload_generation = { catalog.lock().await.workload_generation };
        let initial_candidates = {
            let guard = catalog.lock().await;
            guard
                .services
                .get("backend")
                .map(|entry| entry.candidates.len())
                .unwrap_or_default()
        };
        assert_eq!(initial_candidates, 1);

        let mut stopped = catalog_task(task_id, node_id, service_name, harness.network.id);
        stopped.state = WorkloadPhase::Stopped;
        stopped.updated_at = chrono::Utc::now().to_rfc3339();
        harness
            .workloads
            .upsert(&UuidKey::from(task_id), stopped)
            .await
            .expect("upsert stopped task");

        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.workloads,
            &harness.services,
            harness.network.id,
            &health,
        )
        .await
        .expect("refresh after task change");

        let guard = catalog.lock().await;
        assert!(
            guard.workload_generation > initial_workload_generation,
            "workload generation must advance after task upsert"
        );
        assert_eq!(
            guard
                .services
                .get("backend")
                .map(|entry| entry.candidates.len())
                .unwrap_or_default(),
            0
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_catalog_refresh_invalidates_on_peer_change_clock() {
        let harness = setup_catalog_harness().await;
        let service_name = "backend-service";
        let node_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        upsert_catalog_service(
            &harness.services,
            service_name,
            harness.network.id,
            vec![task_id],
        )
        .await;

        harness
            .workloads
            .upsert(
                &UuidKey::from(task_id),
                catalog_task(task_id, node_id, service_name, harness.network.id),
            )
            .await
            .expect("upsert running task");
        harness
            .registry
            .upsert_attachment(catalog_attachment(
                task_id,
                node_id,
                harness.network.id,
                Ipv4Addr::new(10, 88, 1, 10),
                service_name,
            ))
            .await
            .expect("upsert ready attachment");
        harness
            .registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                harness.network.id,
                node_id,
                "backend-node",
                NetworkPeerState::Configuring,
                None,
            ))
            .await
            .expect("upsert configuring peer state");

        let catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
        let mut health = HashMap::new();
        health.insert(node_id, HealthStatus::Alive);
        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.workloads,
            &harness.services,
            harness.network.id,
            &health,
        )
        .await
        .expect("initial catalog refresh");

        let initial_peer_generation = { catalog.lock().await.peer_generation };
        let initial_candidates = {
            let guard = catalog.lock().await;
            guard
                .services
                .get("backend")
                .map(|entry| entry.candidates.len())
                .unwrap_or_default()
        };
        assert_eq!(
            initial_candidates, 0,
            "attachments on non-ready peers must be excluded from discovery"
        );

        harness
            .registry
            .upsert_peer_state(NetworkPeerStateValue::new(
                harness.network.id,
                node_id,
                "backend-node",
                NetworkPeerState::Ready,
                None,
            ))
            .await
            .expect("upsert ready peer state");

        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.workloads,
            &harness.services,
            harness.network.id,
            &health,
        )
        .await
        .expect("refresh after peer-state change");

        let guard = catalog.lock().await;
        assert!(
            guard.peer_generation > initial_peer_generation,
            "peer generation must advance after peer-state upsert"
        );
        assert_eq!(
            guard
                .services
                .get("backend")
                .map(|entry| entry.candidates.len())
                .unwrap_or_default(),
            1,
            "ready peer state should re-admit the backend into discovery"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_catalog_refresh_requires_service_readiness_opt_in() {
        let harness = setup_catalog_harness().await;
        upsert_catalog_service(
            &harness.services,
            "backend-service",
            harness.network.id,
            vec![Uuid::new_v4()],
        )
        .await;

        let catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.workloads,
            &harness.services,
            harness.network.id,
            &HashMap::new(),
        )
        .await
        .expect("refresh catalog without explicit service readiness");

        let guard = catalog.lock().await;
        let entry = guard
            .services
            .get("backend")
            .expect("catalog entry for backend");
        assert!(entry.readiness.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_catalog_refresh_keeps_explicit_readiness_probe() {
        let harness = setup_catalog_harness().await;
        upsert_catalog_service_with_readiness(
            &harness.services,
            "backend-service",
            harness.network.id,
            vec![Uuid::new_v4()],
            Some(ServiceReadinessProbe {
                kind: ServiceReadinessProbeKind::Http,
                port: 8_000,
                path: Some("/healthz".to_string()),
                interval_ms: 2_000,
                timeout_ms: 300,
                failure_threshold: 2,
            }),
        )
        .await;

        let catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.workloads,
            &harness.services,
            harness.network.id,
            &HashMap::new(),
        )
        .await
        .expect("refresh catalog with explicit readiness probe");

        let guard = catalog.lock().await;
        let entry = guard
            .services
            .get("backend")
            .expect("catalog entry for backend");
        let readiness = entry.readiness.as_ref().expect("readiness probe");
        assert_eq!(readiness.kind, ServiceReadinessProbeKind::Http);
        assert_eq!(readiness.port, 8_000);
        assert_eq!(readiness.path.as_deref(), Some("/healthz"));
        assert_eq!(readiness.failure_threshold, 2);
    }

    /// Public endpoint observations should set and later clear the replicated service detail.
    #[tokio::test(flavor = "current_thread")]
    async fn public_endpoint_observations_update_running_service_detail() {
        let harness = setup_catalog_harness().await;
        upsert_catalog_service_with_public_port(
            &harness.services,
            "public-service",
            harness.network.id,
            vec![Uuid::new_v4()],
            None,
            Some(443),
        )
        .await;

        let service = harness
            .services
            .list()
            .expect("list services")
            .into_iter()
            .find(|spec| spec.service_name == "public-service")
            .expect("public service");

        apply_public_endpoint_observations(
            &harness.services,
            &[PublicEndpointObservation {
                service_id: service.id,
                template_name: "backend".to_string(),
                port: 443,
                detail: Some("template 'backend' public port 443 has no healthy backends".into()),
            }],
        )
        .await
        .expect("persist degraded detail");

        let degraded = harness
            .services
            .get(service.id)
            .expect("load degraded service")
            .expect("service row");
        assert_eq!(
            degraded.public_endpoint_detail(),
            Some("template 'backend' public port 443 has no healthy backends")
        );

        apply_public_endpoint_observations(
            &harness.services,
            &[PublicEndpointObservation {
                service_id: service.id,
                template_name: "backend".to_string(),
                port: 443,
                detail: None,
            }],
        )
        .await
        .expect("clear degraded detail");

        let recovered = harness
            .services
            .get(service.id)
            .expect("load recovered service")
            .expect("service row");
        assert!(recovered.public_endpoint_detail().is_none());
    }

    #[test]
    fn health_snapshot_fingerprint_is_order_stable_and_status_sensitive() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();

        let mut first = HashMap::new();
        first.insert(a, HealthStatus::Alive);
        first.insert(b, HealthStatus::Down);

        let mut reordered = HashMap::new();
        reordered.insert(b, HealthStatus::Down);
        reordered.insert(a, HealthStatus::Alive);

        assert_eq!(
            health_snapshot_fingerprint(&first),
            health_snapshot_fingerprint(&reordered),
            "fingerprint must not depend on insertion order"
        );

        reordered.insert(a, HealthStatus::Suspect);
        assert_ne!(
            health_snapshot_fingerprint(&first),
            health_snapshot_fingerprint(&reordered),
            "fingerprint must change when peer health changes"
        );
    }
}
