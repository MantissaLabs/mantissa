use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) enum HealthState {
    Unknown,
    Healthy,
    Unhealthy,
}

#[derive(Default)]
pub(super) struct BackendHealth {
    pub(super) statuses: HashMap<(Uuid, String), HashMap<IpAddr, HealthEntry>>,
}

impl BackendHealth {
    /// Returns the cached readiness entry for one backend, if discovery has probed it before.
    pub(super) fn get_entry(
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
    pub(super) fn set_entry(
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

    /// Drop every cached readiness observation for one service after discovery no longer trusts it.
    pub(super) fn clear_service(&mut self, network_id: Uuid, service_name: &str) {
        let key = (network_id, service_name.to_ascii_lowercase());
        self.statuses.remove(&key);
    }

    /// Retain cached readiness only for backends whose identity is still unchanged and admitted.
    pub(super) fn retain_service_backends(
        &mut self,
        network_id: Uuid,
        service_name: &str,
        retained_backends: &HashSet<IpAddr>,
    ) {
        let key = (network_id, service_name.to_ascii_lowercase());
        let mut remove_service = false;
        if let Some(entries) = self.statuses.get_mut(&key) {
            entries.retain(|ip, _| retained_backends.contains(ip));
            remove_service = entries.is_empty();
        }
        if remove_service {
            self.statuses.remove(&key);
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct HealthEntry {
    pub(super) state: HealthState,
    pub(super) checked_at: Instant,
    pub(super) consecutive_failures: u32,
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
pub(super) fn readiness_recheck_after(
    entry: Option<HealthEntry>,
    probe: &ServiceReadinessProbe,
) -> Duration {
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

/// Selects the backends that should receive active readiness probes on this tick.
///
/// Unknown and unhealthy backends are always reprobed once stale so readiness remains the sole
/// authority for routing eligibility. Only already-healthy endpoints are rate-limited for
/// steady-state spot-checking.
pub(super) fn select_backends_for_active_probe(
    health: &BackendHealth,
    network_id: Uuid,
    service_name: &str,
    backends: &[BackendAddress],
    probe: &ServiceReadinessProbe,
) -> Vec<BackendAddress> {
    let now = Instant::now();
    let mut eager = Vec::new();
    let mut healthy = Vec::new();
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

        match probe_priority(entry) {
            ProbePriority::Unknown | ProbePriority::Unhealthy => {
                eager.push((probe_priority(entry), backend.ip, backend.clone()));
            }
            ProbePriority::Healthy => {
                if let Some(checked_at) = checked_at {
                    healthy.push((checked_at, backend.ip, backend.clone()));
                } else {
                    eager.push((ProbePriority::Unknown, backend.ip, backend.clone()));
                }
            }
        }
    }

    eager.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    healthy.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut selected: Vec<BackendAddress> =
        eager.into_iter().map(|(_, _, backend)| backend).collect();
    selected.extend(
        healthy
            .into_iter()
            .take(MAX_HEALTHY_READINESS_RECHECKS_PER_REFRESH)
            .map(|(_, _, backend)| backend),
    );
    selected
}

/// Filter candidate backends using cached health without performing probes.
///
/// Readiness-enabled services only admit endpoints that have already passed their readiness probe.
/// Unknown and unhealthy endpoints remain unroutable until active probing marks them healthy.
pub(super) fn filter_cached_backends(
    health: &BackendHealth,
    network_id: Uuid,
    service_name: &str,
    backends: Vec<BackendAddress>,
) -> Vec<BackendAddress> {
    let mut preferred = Vec::with_capacity(backends.len());
    for backend in backends {
        let entry = health.get_entry(network_id, service_name, backend.ip);
        match entry {
            None => {}
            Some(entry) => match entry.state {
                HealthState::Healthy => preferred.push(backend),
                HealthState::Unknown | HealthState::Unhealthy => {}
            },
        }
    }

    preferred
}

/// Normalize one backend set into a stable identity used to detect candidate-set changes.
fn canonical_backend_identity(backends: &[BackendAddress]) -> Vec<(IpAddr, [u8; 6])> {
    let mut entries: Vec<(IpAddr, [u8; 6])> = backends
        .iter()
        .map(|backend| (backend.ip, backend.mac))
        .collect();
    entries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    entries
}

/// Reconcile one service health cache entry against the latest derived backend catalog.
///
/// Readiness remains authoritative for routing, but topology changes should only drop cached
/// health for backends whose identity disappeared or changed. Surviving healthy backends stay
/// routable so peer churn does not create avoidable service gaps.
pub(super) fn reconcile_service_health_cache(
    health: &mut BackendHealth,
    network_id: Uuid,
    service_name: &str,
    previous: Option<&ServiceBackendCatalogEntry>,
    next: Option<&ServiceBackendCatalogEntry>,
) {
    match (previous, next) {
        (None, None) => {}
        (Some(previous), None) => {
            if previous.readiness.is_some() {
                health.clear_service(network_id, service_name);
            }
        }
        (None, Some(next)) => {
            if next.readiness.is_some() {
                health.clear_service(network_id, service_name);
            }
        }
        (Some(previous), Some(next)) => {
            if previous.readiness != next.readiness {
                health.clear_service(network_id, service_name);
                return;
            }
            if next.readiness.is_none() {
                return;
            }

            let previous_candidates = canonical_backend_identity(&previous.candidates);
            let next_candidates = canonical_backend_identity(&next.candidates);
            if previous_candidates == next_candidates {
                return;
            }

            let previous_map: HashMap<IpAddr, [u8; 6]> = previous_candidates.into_iter().collect();
            let retained_backends: HashSet<IpAddr> = next_candidates
                .into_iter()
                .filter_map(|(ip, mac)| (previous_map.get(&ip).copied() == Some(mac)).then_some(ip))
                .collect();
            health.retain_service_backends(network_id, service_name, &retained_backends);
        }
    }
}

/// Normalize one backend-selection result with shared fallback and stable ordering.
///
/// If health checks are disabled for the service and the selected set is empty, discovery falls
/// back to all ready/running endpoints derived from attachment records so the service remains
/// reachable without active probing.
///
/// Both DNS answering and periodic refresh use this helper to keep selection behavior in lockstep.
pub(super) fn normalize_backend_selection(
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
pub(super) async fn evaluate_backend_health(
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

    for ifindex in [bridge_index, vxlan_index].into_iter().flatten() {
        if let Some(mac) = resolve_neighbor_mac_on_link(&handle, ifindex, ip).await {
            return Some(mac);
        }
    }
    None
}

/// Resolve one backend MAC by draining a single interface-scoped neighbour dump.
///
/// The stream must be consumed to completion before returning. Dropping an active rtnetlink dump
/// after the first matching neighbour leaves the connection task with kernel responses that no
/// request handle is still receiving, which `netlink-proto` reports as
/// `failed to forward response back to the handle`.
#[cfg(target_os = "linux")]
async fn resolve_neighbor_mac_on_link(
    handle: &rtnetlink::Handle,
    ifindex: u32,
    ip: IpAddr,
) -> Option<[u8; 6]> {
    use futures::TryStreamExt;
    use rtnetlink::IpVersion;

    let ip_version = match ip {
        IpAddr::V4(_) => IpVersion::V4,
        IpAddr::V6(_) => IpVersion::V6,
    };
    let mut request = handle.neighbours().get().set_family(ip_version);
    request.message_mut().header.ifindex = ifindex;

    let mut found_mac = None;
    let mut neighs = request.execute();
    while let Ok(Some(msg)) = neighs.try_next().await {
        if msg.header.ifindex != ifindex {
            continue;
        }

        let mut found_ip = false;
        let mut message_mac: Option<[u8; 6]> = None;
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
                    message_mac = Some(mac);
                }
                _ => {}
            }
        }

        if found_ip && let Some(mac) = message_mac {
            found_mac = Some(mac);
        }
    }
    found_mac
}

#[cfg(not(target_os = "linux"))]
/// Return no neighbour MAC on unsupported platforms because no kernel overlay bridge exists.
async fn resolve_neighbor_mac(_network_id: Uuid, _ip: IpAddr) -> Option<[u8; 6]> {
    None
}

/// Dispatch one backend readiness probe according to the service's configured probe kind.
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
pub(super) fn nodeport_protocol(protocol: ServicePortProtocol) -> NodePortProtocol {
    match protocol {
        ServicePortProtocol::Tcp => NodePortProtocol::Tcp,
        ServicePortProtocol::Udp => NodePortProtocol::Udp,
        // TcpUdp is expanded into both entries by TaskTemplateSpecValue::public_protocols.
        ServicePortProtocol::TcpUdp => NodePortProtocol::Tcp,
    }
}

/// Probe backend TCP readiness by attempting to establish one connection within the timeout.
async fn probe_backend_tcp(ip: &IpAddr, port: u16, timeout: Duration) -> bool {
    let addr = SocketAddr::new(*ip, port);
    matches!(
        tokio::time::timeout(timeout, TcpStream::connect(addr)).await,
        Ok(Ok(_))
    )
}

/// Probe backend HTTP readiness by issuing a minimal HTTP/1.0 GET and accepting 2xx responses.
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
