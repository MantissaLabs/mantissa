use super::*;
use crate::network::bpf::overlay_bpf_program_specs;

/// Persist the latest public-endpoint outcome back into the replicated service rows.
///
/// Discovery owns public VIP and NodePort publication, so service rows should expose degraded
/// publication explicitly instead of silently appearing healthy while only internal DNS works.
pub(super) async fn apply_public_endpoint_observations(
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
pub(super) async fn sync_service_vip_for_backends(
    runtime: &DiscoveryRuntime,
    discovery_name: &str,
    backends: &[BackendAddress],
    expose_to_host: bool,
) -> Result<Option<(IpAddr, bool)>> {
    let Some((vip, mac)) = compute_service_vip(
        &runtime.registry,
        runtime.network_id,
        discovery_name,
        backends,
    )?
    else {
        return Ok(None);
    };
    let programmed =
        program_service_vip(runtime, discovery_name, vip, mac, backends, expose_to_host).await;
    Ok(Some((vip, programmed)))
}

/// Attempt to synchronize VIP metadata into the eBPF maps if they are available, returning whether
/// the dataplane was programmed successfully. Missing maps are warned once per network.
async fn program_service_vip(
    runtime: &DiscoveryRuntime,
    discovery_name: &str,
    vip: IpAddr,
    vip_mac: [u8; 6],
    backends: &[BackendAddress],
    expose_to_host: bool,
) -> bool {
    match runtime
        .bpf_lb
        .sync_vip(runtime.network_id, vip, vip_mac, backends)
    {
        Ok(()) => {
            if expose_to_host
                && let Err(err) = ensure_host_vip_neighbor(runtime.network_id, vip, vip_mac).await
            {
                debug!(
                    target: "network",
                    network = %runtime.network_id,
                    service = %discovery_name,
                    vip = %vip,
                    "failed to program host neighbour for vip (continuing): {err:#}"
                );
            }
            let mut guard = runtime.missing_lb_maps.lock().await;
            guard.remove(&runtime.network_id);
            true
        }
        Err(err) => {
            // Attempt to heal the maps by re-ensuring BPF programs, then retry once.
            let healed = heal_lb_maps(&runtime.bpf, &runtime.registry, runtime.network_id).await;
            if healed.is_ok()
                && runtime
                    .bpf_lb
                    .sync_vip(runtime.network_id, vip, vip_mac, backends)
                    .is_ok()
            {
                if expose_to_host
                    && let Err(err) =
                        ensure_host_vip_neighbor(runtime.network_id, vip, vip_mac).await
                {
                    debug!(
                        target: "network",
                        network = %runtime.network_id,
                        service = %discovery_name,
                        vip = %vip,
                        "failed to program host neighbour for vip after healing (continuing): {err:#}"
                    );
                }
                let mut guard = runtime.missing_lb_maps.lock().await;
                guard.remove(&runtime.network_id);
                return true;
            }

            let mut guard = runtime.missing_lb_maps.lock().await;
            if guard.insert(runtime.network_id) {
                warn!(
                    target: "network",
                    network = %runtime.network_id,
                    service = %discovery_name,
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
pub(super) async fn reconcile_host_vip_neighbors(
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
        attach_spec.bpf_programs = overlay_bpf_program_specs();
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
