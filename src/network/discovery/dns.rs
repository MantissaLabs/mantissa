use super::*;

/// Spawn the per-network DNS listener and its refresh loop.
///
/// The socket answers service queries for one overlay network, while the sibling interval task
/// keeps backend selection, VIP programming, and NodePort publication current.
#[allow(clippy::too_many_arguments)]
pub(super) async fn spawn_dns_server(
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

/// Decode one DNS datagram, ensure backend state is fresh, and write the reply.
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
        health,
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

/// Resolve one DNS query against the cached per-network service catalog.
///
/// If the eBPF VIP dataplane is available, service names resolve to one stable VIP. Otherwise
/// discovery rotates backend records in userspace so service reachability does not depend on
/// dataplane availability.
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
