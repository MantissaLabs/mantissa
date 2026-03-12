use crate::config;
use crate::network::allocator::parse_ipv4_cidr;
use crate::network::attachment::{bridge_name, host_access_host_iface_name, vxlan_name};
use crate::network::bpf::{NetworkBpfManager, NetworkInterfaceContext};
use crate::network::lb::{BackendAddress, BpfLoadBalancer};
use crate::network::nodeport::{NodePortManager, NodePortMapping, NodePortProtocol};
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    BpfAttachPoint, BpfProgramSpec, NetworkAttachmentState, NetworkSpecValue,
};
use crate::services::registry::ServiceRegistry;
use crate::services::types::{ServicePortProtocol, ServiceSpecValue};
use crate::store::task_store::TaskStore;
use crate::task::container::ContainerState;
use crate::task::manager::select_best_task_value;
use crate::task::types::TaskValue;
use anyhow::{Context, Result, bail};
use blake3::Hasher;
use crdt_store::uuid_key::UuidKey;
use health::{HealthMonitor, Status as HealthStatus};
use hickory_proto::op::{Message, MessageType, Query, ResponseCode};
use hickory_proto::rr::{Name, RData, Record, RecordType};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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
// Recheck healthy backends periodically so we refresh MACs without probing on every tick.
const HEALTH_HEALTHY_RECHECK: Duration = Duration::from_secs(2);
// Back off unhealthy probes to reduce repeated timeouts while an endpoint is down.
const HEALTH_UNHEALTHY_RECHECK: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ServiceDiscovery {
    registry: NetworkRegistry,
    tasks: TaskStore,
    services: ServiceRegistry,
    bpf: NetworkBpfManager,
    health_monitor: Arc<HealthMonitor>,
    servers: Arc<AsyncMutex<HashMap<Uuid, DnsServerHandle>>>,
    load_balancer: Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: Arc<AsyncMutex<BackendHealth>>,
    dns_port: u16,
    health_port: Option<u16>,
    health_timeout: Duration,
    bpf_lb: BpfLoadBalancer,
    nodeport: NodePortManager,
    missing_lb_maps: Arc<AsyncMutex<HashSet<Uuid>>>,
}

struct DnsServerHandle {
    resolver_ip: Ipv4Addr,
    shutdown: Option<watch::Sender<bool>>,
    task: JoinHandle<()>,
}

/// Cached backend-resolution metadata for one network, invalidated by store generations and
/// health state changes.
#[derive(Default)]
struct NetworkBackendCatalog {
    attachment_generation: u64,
    task_generation: u64,
    service_generation: u64,
    health_fingerprint: u64,
    services: HashMap<String, ServiceBackendCatalogEntry>,
}

/// One service entry in the backend catalog.
#[derive(Clone)]
struct ServiceBackendCatalogEntry {
    service_name: String,
    candidates: Vec<BackendAddress>,
    health_port: Option<u16>,
    health_path: Option<String>,
    expose_to_host: bool,
    public_port: Option<u16>,
    public_protocols: Vec<NodePortProtocol>,
}

impl ServiceDiscovery {
    /// Build service discovery with the default DNS bind port (53).
    pub fn new(
        registry: NetworkRegistry,
        tasks: TaskStore,
        services: ServiceRegistry,
        bpf: NetworkBpfManager,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self::new_with_dns_port(registry, tasks, services, bpf, health_monitor, 53)
    }

    /// Build service discovery with an explicit DNS bind port.
    ///
    /// Tests use this to run DNS flows unprivileged on high ports while production keeps 53.
    pub fn new_with_dns_port(
        registry: NetworkRegistry,
        tasks: TaskStore,
        services: ServiceRegistry,
        bpf: NetworkBpfManager,
        health_monitor: Arc<HealthMonitor>,
        dns_port: u16,
    ) -> Self {
        let health_port = config::discovery_health_port();
        Self {
            registry,
            tasks,
            services,
            bpf,
            health_monitor,
            servers: Arc::new(AsyncMutex::new(HashMap::new())),
            load_balancer: Arc::new(AsyncMutex::new(ServiceLoadBalancer::default())),
            health: Arc::new(AsyncMutex::new(BackendHealth::default())),
            dns_port,
            health_port,
            health_timeout: Duration::from_millis(300),
            bpf_lb: BpfLoadBalancer::new(),
            nodeport: NodePortManager::new(),
            missing_lb_maps: Arc::new(AsyncMutex::new(HashSet::new())),
        }
    }

    pub async fn ensure_network(
        &self,
        spec: &NetworkSpecValue,
        resolver_ip: Option<Ipv4Addr>,
    ) -> Result<()> {
        let Some(resolver_ip) = resolver_ip else {
            self.teardown_network(spec.id).await?;
            return Ok(());
        };

        {
            let guard = self.servers.lock().await;
            if let Some(existing) = guard.get(&spec.id)
                && existing.resolver_ip == resolver_ip
            {
                return Ok(());
            }
        }

        self.teardown_network(spec.id).await?;

        let server = spawn_dns_server(
            self.registry.clone(),
            self.tasks.clone(),
            self.services.clone(),
            self.bpf.clone(),
            spec.id,
            spec.name.clone(),
            resolver_ip,
            self.load_balancer.clone(),
            self.health.clone(),
            self.dns_port,
            self.health_port,
            self.health_timeout,
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
            tokio::spawn(async move {
                if let Err(err) = handle.task.await {
                    warn!(
                        target: "network",
                        network = %network_id,
                        "service discovery loop exited with error: {err:#}"
                    );
                }
            });
        }

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_dns_server(
    registry: NetworkRegistry,
    tasks: TaskStore,
    services: ServiceRegistry,
    bpf: NetworkBpfManager,
    network_id: Uuid,
    network_name: String,
    resolver_ip: Ipv4Addr,
    load_balancer: Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: Arc<AsyncMutex<BackendHealth>>,
    dns_port: u16,
    health_port: Option<u16>,
    health_timeout: Duration,
    bpf_lb: BpfLoadBalancer,
    nodeport: NodePortManager,
    missing_lb_maps: Arc<AsyncMutex<HashSet<Uuid>>>,
    health_monitor: Arc<HealthMonitor>,
) -> Result<DnsServerHandle> {
    let bind_addr = SocketAddr::new(IpAddr::V4(resolver_ip), dns_port);
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
        &tasks,
        &service_registry,
        &bpf_manager,
        network_id,
        &health,
        health_port,
        health_timeout,
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
    let refresh_tasks = tasks.clone();
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
                        &refresh_tasks,
                        &refresh_service_registry,
                        &refresh_bpf_manager,
                        network_id,
                        &refresh_health,
                        health_port,
                        health_timeout,
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
    let dns_tasks = tasks.clone();
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
                                &dns_tasks,
                                &dns_service_registry,
                                &dns_bpf_manager,
                                network_id,
                                &network_name,
                                &dns_load_balancer,
                                &dns_health,
                                health_port,
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
    tasks: &TaskStore,
    services: &ServiceRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    network_name: &str,
    load_balancer: &Arc<AsyncMutex<ServiceLoadBalancer>>,
    health: &Arc<AsyncMutex<BackendHealth>>,
    health_port: Option<u16>,
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
        tasks,
        services,
        network_id,
        &health_snapshot,
        health_port,
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
    if query.query_type() == RecordType::AAAA {
        return Ok(LookupOutcome::NoData);
    }
    if query.query_type() != RecordType::A {
        return Ok(LookupOutcome::NotImplemented);
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
    let mut backends = if catalog_entry.health_port.is_some() {
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
        catalog_entry.health_port.is_some(),
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
    let mut records = Vec::new();
    let offset = {
        let mut picker = load_balancer.lock().await;
        picker.next_offset(network_id, &service_name, backends.len())
    };
    let addresses = rotate_addresses(
        backends
            .iter()
            .map(|backend| backend.ip)
            .collect::<Vec<Ipv4Addr>>(),
        offset,
    );

    records.extend(addresses.into_iter().map(|addr| {
        Record::from_rdata(
            query.name().clone(),
            SERVICE_TTL_SECS,
            RData::A(addr.into()),
        )
    }));

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
        records.push(Record::from_rdata(
            query.name().clone(),
            SERVICE_TTL_SECS,
            RData::A(vip.into()),
        ));
    }

    Ok(LookupOutcome::Records(records))
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
    tasks: &TaskStore,
    template_index: &HashMap<Uuid, (String, String)>,
    network_id: Uuid,
    service_name: &str,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
) -> Result<Vec<BackendAddress>> {
    let attachments = registry
        .list_attachments(Some(network_id))
        .context("list attachments for discovery")?;
    let mut cache: HashMap<Uuid, Option<TaskValue>> = HashMap::new();
    let mut results = Vec::new();

    tracing::trace!(
        target: "network",
        network = %network_id,
        service = %service_name,
        attachments = attachments.len(),
        "resolving service backends"
    );

    for attachment in attachments {
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
            .or_insert_with(|| load_task(tasks, attachment.task_id));
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
        if let Some(meta) = task.service_metadata.as_ref() {
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
        if task.state != ContainerState::Running {
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
        let ip_addr = match ip_text.parse::<Ipv4Addr>() {
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
fn load_task(tasks: &TaskStore, id: Uuid) -> Option<TaskValue> {
    let key = UuidKey::from(id);
    let snapshot = tasks.get_snapshot(&key).ok()??;
    select_best_task_value(snapshot.as_slice())
}

/// Decide whether an attachment should be trusted over a stale task record during convergence.
fn attachment_is_newer_than_task(
    attachment: &crate::network::types::NetworkAttachmentValue,
    task: &TaskValue,
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
fn task_revision_timestamp(task: &TaskValue) -> Option<chrono::DateTime<chrono::Utc>> {
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
    backends.sort_by(|a, b| {
        let a_ip = u32::from(a.ip);
        let b_ip = u32::from(b.ip);
        a_ip.cmp(&b_ip).then_with(|| a.mac.cmp(&b.mac))
    });
}

fn build_task_template_index(specs: &[ServiceSpecValue]) -> HashMap<Uuid, (String, String)> {
    let mut index = HashMap::new();
    for spec in specs {
        let mut ids = spec.task_ids.iter();
        for template in &spec.tasks {
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
) -> Result<Option<(Ipv4Addr, [u8; 6])>> {
    let Some(spec) = registry.get_spec(network_id)? else {
        return Ok(None);
    };
    let Ok((base_ip, prefix)) = parse_ipv4_cidr(&spec.subnet_cidr) else {
        return Ok(None);
    };

    let host_bits = 32u8.saturating_sub(prefix);
    if host_bits < 4 {
        return Ok(None);
    }

    let digest = {
        let mut hasher = Hasher::new();
        hasher.update(network_id.as_bytes());
        hasher.update(service_name.as_bytes());
        hasher.finalize()
    };

    let mut slot_seed = [0u8; 4];
    slot_seed.copy_from_slice(&digest.as_bytes()[..4]);

    // Constrain VIPs to the even offsets of the overlay to avoid collisions with per-node resolver
    // addresses, which always occupy the odd slots (offsets 1, 3, 5, ...).
    let available_even = (1u64 << host_bits).saturating_sub(16) / 2;
    if available_even == 0 {
        return Ok(None);
    }

    let backend_ips: std::collections::HashSet<u32> = backends
        .iter()
        .map(|backend| u32::from(backend.ip))
        .collect();

    let mut slot = (u32::from_le_bytes(slot_seed) % available_even as u32) * 2 + 8;
    for _ in 0..available_even.min(16) as usize {
        let candidate = u32::from(base_ip).saturating_add(slot);
        if !backend_ips.contains(&candidate) {
            let vip = Ipv4Addr::from(candidate);

            let mut mac = [0u8; 6];
            mac[0] = 0x02;
            mac[1..].copy_from_slice(&digest.as_bytes()[4..9]);

            return Ok(Some((vip, mac)));
        }

        // Walk forward to the next even slot if we collided with an existing backend.
        slot = slot.wrapping_add(2) % (available_even as u32 * 2);
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
    /// Track per-service cursor offsets so DNS answers expose different primaries and downstream
    /// clients that always pick the first A record can still fan out across replicas.
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
fn rotate_addresses(mut addresses: Vec<Ipv4Addr>, offset: usize) -> Vec<Ipv4Addr> {
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
    statuses: HashMap<(Uuid, String), HashMap<Ipv4Addr, HealthEntry>>,
}

impl BackendHealth {
    fn get_entry(
        &self,
        network_id: Uuid,
        service_name: &str,
        backend: Ipv4Addr,
    ) -> Option<HealthEntry> {
        let key = (network_id, service_name.to_ascii_lowercase());
        self.statuses
            .get(&key)
            .and_then(|svc| svc.get(&backend).copied())
    }

    /// Select only backends currently marked healthy (or unknown) to prepare for active probing.
    fn set_health(
        &mut self,
        network_id: Uuid,
        service_name: &str,
        backend: Ipv4Addr,
        state: HealthState,
    ) {
        let key = (network_id, service_name.to_ascii_lowercase());
        let entry = self.statuses.entry(key).or_default();
        entry.insert(
            backend,
            HealthEntry {
                state,
                checked_at: Instant::now(),
            },
        );
    }
}

#[derive(Clone, Copy)]
struct HealthEntry {
    state: HealthState,
    checked_at: Instant,
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
                    HealthState::Healthy | HealthState::Unknown => preferred.push(backend),
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

/// Actively probe candidate backends so the cached health state and dataplane MACs stay fresh.
#[expect(
    clippy::too_many_arguments,
    reason = "health probing carries one private service refresh context"
)]
async fn evaluate_backend_health(
    health: &Arc<AsyncMutex<BackendHealth>>,
    registry: &NetworkRegistry,
    network_id: Uuid,
    service_name: &str,
    backends: Vec<BackendAddress>,
    port: Option<u16>,
    http_path: Option<String>,
    timeout: Duration,
) -> Vec<BackendAddress> {
    // Health probing is opt-in. When no port is provided we keep current behavior and return all backends.
    let Some(port) = port else { return backends };

    let mut healthy = Vec::with_capacity(backends.len());
    for backend in backends {
        let entry = {
            let guard = health.lock().await;
            guard.get_entry(network_id, service_name, backend.ip)
        };
        let now = Instant::now();
        let state = entry.map(|e| e.state).unwrap_or(HealthState::Unknown);
        let checked_at = entry.map(|e| e.checked_at).unwrap_or(now);
        let recheck_after = match state {
            HealthState::Healthy => HEALTH_HEALTHY_RECHECK,
            HealthState::Unknown => Duration::from_secs(0),
            HealthState::Unhealthy => HEALTH_UNHEALTHY_RECHECK,
        };
        let is_stale = now.saturating_duration_since(checked_at) >= recheck_after;

        // Respect cached health until the recheck interval expires.
        if matches!(state, HealthState::Healthy) && !is_stale {
            healthy.push(backend);
            continue;
        }
        if matches!(state, HealthState::Unhealthy) && !is_stale {
            continue;
        }

        let mut backend = backend;
        let probe_ok = probe_backend(&backend.ip, port, http_path.as_deref(), timeout).await;
        let refreshed_mac = if probe_ok {
            refresh_backend_mac(registry, network_id, backend.ip).await
        } else {
            None
        };
        let mut guard = health.lock().await;
        if probe_ok {
            if let Some(mac) = refreshed_mac {
                backend.mac = mac;
            }
            guard.set_health(network_id, service_name, backend.ip, HealthState::Healthy);
            healthy.push(backend);
        } else {
            guard.set_health(network_id, service_name, backend.ip, HealthState::Unhealthy);
            tracing::debug!(
                target: "network",
                network = %network_id,
                service = %service_name,
                backend = %backend.ip,
                state = ?state,
                stale = is_stale,
                "backend failed health check"
            );
        }
    }

    healthy
}

/// Refresh the MAC address stored for a backend by querying kernel neighbour tables and persisting
/// the new value into the attachment registry so downstream dataplane programming uses the right
/// L2 destination.
async fn refresh_backend_mac(
    registry: &NetworkRegistry,
    network_id: Uuid,
    ip: Ipv4Addr,
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
async fn resolve_neighbor_mac(network_id: Uuid, ip: Ipv4Addr) -> Option<[u8; 6]> {
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
                NeighbourAttribute::Destination(NeighbourAddress::Inet(addr)) if *addr == ip => {
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
async fn resolve_neighbor_mac(_network_id: Uuid, _ip: Ipv4Addr) -> Option<[u8; 6]> {
    None
}

async fn probe_backend(
    ip: &Ipv4Addr,
    port: u16,
    http_path: Option<&str>,
    timeout: Duration,
) -> bool {
    if let Some(path) = http_path
        && probe_backend_http(ip, port, path, timeout).await
    {
        return true;
    }
    probe_backend_tcp(ip, port, timeout).await
}

fn service_health_port(service_specs: &[ServiceSpecValue], service_name: &str) -> Option<u16> {
    for spec in service_specs {
        for task in &spec.tasks {
            if task.name.eq_ignore_ascii_case(service_name) {
                if let Some(port) = task.health_port() {
                    return Some(port);
                }
                for env in &task.env {
                    if env.name.eq_ignore_ascii_case("MANTISSA_HEALTH_PORT")
                        && let Some(val) = env.value.as_deref()
                        && let Ok(port) = val.parse::<u16>()
                    {
                        return Some(port);
                    }
                }
                return None;
            }
        }
    }
    None
}

/// Resolve the public nodeport requested by a service template, if any.
fn service_public_port(service_specs: &[ServiceSpecValue], service_name: &str) -> Option<u16> {
    for spec in service_specs {
        for task in &spec.tasks {
            if task.name.eq_ignore_ascii_case(service_name) {
                return task.public_port();
            }
        }
    }
    None
}

/// Resolve the transport protocols to expose when a service declares a public port.
fn service_public_protocols(
    service_specs: &[ServiceSpecValue],
    service_name: &str,
) -> Vec<NodePortProtocol> {
    for spec in service_specs {
        for task in &spec.tasks {
            if task.name.eq_ignore_ascii_case(service_name) {
                return task
                    .public_protocols()
                    .into_iter()
                    .map(nodeport_protocol)
                    .collect();
            }
        }
    }
    vec![NodePortProtocol::Tcp]
}

/// Convert a service protocol descriptor into a nodeport transport selector.
fn nodeport_protocol(protocol: ServicePortProtocol) -> NodePortProtocol {
    match protocol {
        ServicePortProtocol::Tcp => NodePortProtocol::Tcp,
        ServicePortProtocol::Udp => NodePortProtocol::Udp,
        // TcpUdp is expanded into both entries by ServiceTaskSpecValue::public_protocols.
        ServicePortProtocol::TcpUdp => NodePortProtocol::Tcp,
    }
}

fn service_health_path(service_specs: &[ServiceSpecValue], service_name: &str) -> Option<String> {
    for spec in service_specs {
        for task in &spec.tasks {
            if task.name.eq_ignore_ascii_case(service_name) {
                if let Some(cmd) = task.health_command()
                    && let Some(first) = cmd.first()
                {
                    return Some(first.clone());
                }
                for env in &task.env {
                    if env.name.eq_ignore_ascii_case("MANTISSA_HEALTH_PATH")
                        && let Some(val) = env.value.clone()
                    {
                        return Some(val);
                    }
                }
            }
        }
    }
    None
}

async fn probe_backend_tcp(ip: &Ipv4Addr, port: u16, timeout: Duration) -> bool {
    let addr = SocketAddr::new(IpAddr::V4(*ip), port);
    matches!(
        tokio::time::timeout(timeout, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

async fn probe_backend_http(ip: &Ipv4Addr, port: u16, path: &str, timeout: Duration) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let addr = SocketAddr::new(IpAddr::V4(*ip), port);
    let path = if path.is_empty() { "/" } else { path };
    let mut stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        _ => return false,
    };

    let request = format!("GET {path} HTTP/1.0\r\nHost: {ip}\r\n\r\n");
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

/// Refresh the per-network backend catalog when any upstream generation (attachments, tasks,
/// services) or peer-health snapshot has changed.
///
/// This centralizes backend resolution so both DNS answers and periodic refresh reuse the same
/// computed candidate set.
async fn refresh_backend_catalog_if_needed(
    backend_catalog: &Arc<AsyncMutex<NetworkBackendCatalog>>,
    registry: &NetworkRegistry,
    tasks: &TaskStore,
    services: &ServiceRegistry,
    network_id: Uuid,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
    default_health_port: Option<u16>,
) -> Result<()> {
    let attachment_generation = registry.attachment_change_clock();
    let task_generation = tasks.change_clock();
    let service_generation = services.change_clock();
    let health_fingerprint = health_snapshot_fingerprint(health_snapshot);

    {
        let guard = backend_catalog.lock().await;
        if guard.attachment_generation == attachment_generation
            && guard.task_generation == task_generation
            && guard.service_generation == service_generation
            && guard.health_fingerprint == health_fingerprint
        {
            return Ok(());
        }
    }

    let service_specs = services
        .list()
        .context("load service specs for backend catalog refresh")?;
    let template_index = build_task_template_index(&service_specs);
    let service_names = services_for_network(&service_specs, network_id);
    let mut next_services = HashMap::with_capacity(service_names.len());
    for service_name in service_names {
        let candidates = resolve_service_backends(
            registry,
            tasks,
            &template_index,
            network_id,
            &service_name,
            health_snapshot,
        )
        .await?;
        let health_port = default_health_port
            .or_else(|| service_health_port(&service_specs, &service_name))
            .and_then(|port| if port == 0 { None } else { Some(port) });
        let public_port = service_public_port(&service_specs, &service_name);
        let public_protocols = if public_port.is_some() {
            service_public_protocols(&service_specs, &service_name)
        } else {
            Vec::new()
        };
        let health_path = service_health_path(&service_specs, &service_name);
        let expose_to_host = service_is_public(&service_specs, network_id, &service_name);
        let service_key = service_name.to_ascii_lowercase();

        next_services.insert(
            service_key,
            ServiceBackendCatalogEntry {
                service_name,
                candidates,
                health_port,
                health_path,
                expose_to_host,
                public_port,
                public_protocols,
            },
        );
    }

    let mut guard = backend_catalog.lock().await;
    guard.attachment_generation = attachment_generation;
    guard.task_generation = task_generation;
    guard.service_generation = service_generation;
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
    tasks: &TaskStore,
    services: &ServiceRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    health: &Arc<AsyncMutex<BackendHealth>>,
    health_port: Option<u16>,
    health_timeout: Duration,
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
        tasks,
        services,
        network_id,
        &health_snapshot,
        health_port,
    )
    .await?;
    let entries: Vec<ServiceBackendCatalogEntry> = {
        let guard = backend_catalog.lock().await;
        guard.services.values().cloned().collect()
    };
    let mut nodeport_entries = Vec::new();

    for entry in entries {
        let mappings = refresh_single_service(
            registry,
            bpf,
            network_id,
            &entry,
            health,
            health_timeout,
            bpf_lb,
            lb_missing,
        )
        .await?;
        nodeport_entries.extend(mappings);
    }

    nodeport
        .sync_ports(network_id, &nodeport_entries)
        .await
        .context("sync nodeport mappings")?;

    Ok(())
}

/// Determine all service labels that attach to the provided network based on declared task network
/// requirements so we can refresh health and load balancer state without waiting on DNS requests.
fn services_for_network(service_specs: &[ServiceSpecValue], network_id: Uuid) -> HashSet<String> {
    let mut services = HashSet::new();
    for spec in service_specs {
        for task in &spec.tasks {
            if task.networks.iter().any(|net| net.network_id == network_id) {
                services.insert(task.name.clone());
            }
        }
    }
    services
}

/// Refresh healthy backend list and VIP programming for a single service so clients connected via
/// the VIP can fail over even if they do not issue new DNS lookups.
///
/// Returns nodeport mappings when the service is marked public so external listeners can be
/// reconciled by the caller.
#[expect(
    clippy::too_many_arguments,
    reason = "single-service refresh threads one explicit private control-plane context"
)]
async fn refresh_single_service(
    registry: &NetworkRegistry,
    bpf: &NetworkBpfManager,
    network_id: Uuid,
    service: &ServiceBackendCatalogEntry,
    health: &Arc<AsyncMutex<BackendHealth>>,
    health_timeout: Duration,
    bpf_lb: &BpfLoadBalancer,
    lb_missing: &Arc<AsyncMutex<HashSet<Uuid>>>,
) -> Result<Vec<NodePortMapping>> {
    let service_name = service.service_name.as_str();
    let candidates = service.candidates.clone();
    let mut backends = evaluate_backend_health(
        health,
        registry,
        network_id,
        service_name,
        candidates.clone(),
        service.health_port,
        service.health_path.clone(),
        health_timeout,
    )
    .await;

    backends = normalize_backend_selection(
        network_id,
        service_name,
        candidates,
        backends,
        service.health_port.is_some(),
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
        return Ok(Vec::new());
    }

    if let Some((vip, _)) = sync_service_vip_for_backends(
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
        let mut mappings = Vec::new();
        for protocol in service.public_protocols.clone() {
            mappings.push(NodePortMapping {
                port,
                vip,
                vip_port: port,
                protocol,
            });
        }
        return Ok(mappings);
    }

    Ok(Vec::new())
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
) -> Result<Option<(Ipv4Addr, bool)>> {
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
    vip: Ipv4Addr,
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

/// Return true if a service template declares a public port for the given network.
///
/// This is used to decide whether Mantissa should proactively program host neighbour entries for
/// VIPs, enabling `curl http://<vip>:<port>` from the node without relying on ARP synthesis.
fn service_is_public(
    service_specs: &[ServiceSpecValue],
    network_id: Uuid,
    service_name: &str,
) -> bool {
    service_specs.iter().any(|spec| {
        spec.tasks.iter().any(|task| {
            task.name.eq_ignore_ascii_case(service_name)
                && task.public_port().is_some()
                && task.networks.iter().any(|net| net.network_id == network_id)
        })
    })
}

/// Ensure the local host has a stable neighbour entry for a service VIP.
///
/// Host-originated traffic enters the overlay via a dedicated `mnhost-*` interface. Without an
/// ARP reply, the host neighbour table can remain in `FAILED` and prevent `curl` from reaching
/// the VIP. Programming a permanent neighbour entry ties the VIP to the deterministic VIP MAC so
/// packets reach the bridge tc-ingress load balancer immediately.
async fn ensure_host_vip_neighbor(network_id: Uuid, vip: Ipv4Addr, vip_mac: [u8; 6]) -> Result<()> {
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
            .add(host_index, IpAddr::V4(vip))
            .link_local_address(&vip_mac)
            .state(NeighbourState::Permanent)
            .replace()
            .execute()
            .await
            .with_context(|| format!("program vip neighbour entry for {vip} on {host_ifname}"))?;

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

    let interfaces =
        NetworkInterfaceContext::new(network_id, bridge_name(network_id), vxlan_name(network_id));
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
        NetworkSpecDraft, NetworkSpecValue,
    };
    use crate::services::registry::ServiceRegistry;
    use crate::services::types::{
        ServiceSpecValue, ServiceTaskNetworkRequirement, ServiceTaskSpecValue,
    };
    use crate::store::network_store::{
        open_network_attachment_store, open_network_peer_store, open_network_spec_store,
    };
    use crate::store::service_store::open_service_store;
    use crate::store::task_store::{TaskStore, open_task_store};
    use crate::task::container::ContainerState;
    use crate::task::types::{TaskServiceMetadata, TaskValue, TaskValueDraft};
    use crdt_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn backend(ip: [u8; 4], mac: [u8; 6]) -> BackendAddress {
        BackendAddress {
            ip: Ipv4Addr::from(ip),
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
            unhealthy_ip,
            HealthEntry {
                state: HealthState::Unhealthy,
                checked_at: Instant::now() - HEALTH_CACHE_STALE_AFTER - Duration::from_secs(1),
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
        assert_eq!(filtered[0].ip, healthy_ip);
    }

    #[test]
    fn filter_cached_backends_keeps_stale_unhealthy_when_only_choice() {
        let network_id = Uuid::new_v4();
        let service = "backend";
        let unhealthy_ip = Ipv4Addr::new(10, 42, 1, 10);

        let mut health = BackendHealth::default();
        let key = (network_id, service.to_string());
        health.statuses.entry(key).or_default().insert(
            unhealthy_ip,
            HealthEntry {
                state: HealthState::Unhealthy,
                checked_at: Instant::now() - HEALTH_CACHE_STALE_AFTER - Duration::from_secs(1),
            },
        );

        let filtered = filter_cached_backends(
            &health,
            network_id,
            service,
            vec![backend([10, 42, 1, 10], [0x02, 0, 0, 0, 0, 1])],
        );

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].ip, unhealthy_ip);
    }

    struct CatalogHarness {
        registry: NetworkRegistry,
        tasks: TaskStore,
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
        let tasks = open_task_store(task_db, actor).expect("open task store");
        tasks
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild task store");

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
            tasks,
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
    ) -> TaskValue {
        let now = chrono::Utc::now().to_rfc3339();
        TaskValue::new(TaskValueDraft {
            id: task_id,
            name: "backend".to_string(),
            image: "hashicorp/http-echo:1.0.0".to_string(),
            state: ContainerState::Running,
            phase_reason: None,
            phase_progress: None,
            created_at: now.clone(),
            updated_at: now,
            command: Vec::new(),
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
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            service_metadata: Some(TaskServiceMetadata::new(service_name, "backend")),
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
            container_id: format!("container-{task_id}"),
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

    /// Writes one service template that maps the backend name to the provided network.
    async fn upsert_catalog_service(
        services: &ServiceRegistry,
        service_name: &str,
        network_id: Uuid,
        task_ids: Vec<Uuid>,
    ) {
        let service = ServiceSpecValue::new(
            Uuid::new_v4(),
            "catalog-test-manifest",
            service_name,
            vec![ServiceTaskSpecValue {
                name: "backend".to_string(),
                image: "hashicorp/http-echo:1.0.0".to_string(),
                command: Vec::new(),
                depends_on: Vec::new(),
                replicas: task_ids.len() as u16,
                cpu_millis: 100,
                memory_bytes: 64 * 1024 * 1024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: vec![ServiceTaskNetworkRequirement::new("default", network_id)],
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
            task_ids,
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
            .tasks
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

        let catalog = Arc::new(AsyncMutex::new(NetworkBackendCatalog::default()));
        let mut health = HashMap::new();
        health.insert(node_id, HealthStatus::Alive);
        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.tasks,
            &harness.services,
            harness.network.id,
            &health,
            None,
        )
        .await
        .expect("initial catalog refresh");
        let initial_task_generation = { catalog.lock().await.task_generation };
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
        stopped.state = ContainerState::Stopped;
        stopped.updated_at = chrono::Utc::now().to_rfc3339();
        harness
            .tasks
            .upsert(&UuidKey::from(task_id), stopped)
            .await
            .expect("upsert stopped task");

        refresh_backend_catalog_if_needed(
            &catalog,
            &harness.registry,
            &harness.tasks,
            &harness.services,
            harness.network.id,
            &health,
            None,
        )
        .await
        .expect("refresh after task change");

        let guard = catalog.lock().await;
        assert!(
            guard.task_generation > initial_task_generation,
            "task generation must advance after task upsert"
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
