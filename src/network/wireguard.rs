use crate::config;
use crate::registry::Registry;
use crate::topology::peers::{PeerValue, WireGuardPeerValue};
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Name of the kernel WireGuard interface managed by Mantissa.
///
/// We keep this stable so all nodes converge on the same underlay device without requiring
/// any user configuration.
pub const MANTISSA_WIREGUARD_IFNAME: &str = "mnwg0";

/// Default WireGuard MTU used by Mantissa for encrypted underlay traffic.
///
/// The kernel default is typically 1420; we set this explicitly to make MTU interactions with
/// VXLAN deterministic across nodes.
pub const MANTISSA_WIREGUARD_MTU: u32 = 1420;

/// Recommended overlay MTU when VXLAN runs over the WireGuard underlay.
///
/// When Mantissa runs VXLAN over the WireGuard tunnel we use IPv6 tunnel addresses
/// (`fd42:6d61:6e74:6973::/64`). VXLAN over IPv6 has ~70 bytes of overhead, so the safe overlay
/// MTU is:
/// - `WireGuard MTU (1420) - VXLAN/UDP/IPv6 overhead (70) = 1350`
pub const MANTISSA_WIREGUARD_VXLAN_MTU: u32 = MANTISSA_WIREGUARD_MTU - 70;

/// Periodic forced reconfiguration interval to correct external drift.
const WIREGUARD_FORCE_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// UDP destination port used by Mantissa VXLAN devices.
///
/// We keep this local to the WireGuard module so we can punch firewall holes without depending
/// on private constants from the network controller.
const MANTISSA_VXLAN_UDP_PORT: u16 = 4789;

/// Snapshot of the current WireGuard underlay readiness as seen by the network controller.
#[derive(Clone, Debug, Default)]
pub struct WireGuardUnderlayState {
    /// Whether the controller should use WireGuard for the VXLAN underlay on this node.
    ///
    /// Once a node has successfully activated WireGuard for VXLAN, this may remain true even when
    /// the current scoped peer set is empty. Keeping the selected underlay sticky avoids expensive
    /// plaintext/WireGuard VXLAN rebuilds during lazy peer-scope churn.
    pub underlay_active: bool,

    /// Number of remote peers that currently require an encrypted VXLAN underlay on this node.
    ///
    /// When this is non-zero, plaintext fallback is not allowed. The network controller uses this
    /// to block network readiness until the scoped WireGuard peer set is fully configured.
    pub required_peer_count: usize,

    /// The WireGuard interface name (stable).
    pub ifname: String,

    /// The local tunnel IP address used as the VXLAN underlay source/destination.
    pub tunnel_ip: Option<IpAddr>,

    /// Hash of the last WireGuard interface configuration applied by Mantissa.
    pub config_hash: Option<u64>,

    /// Timestamp of the last successful WireGuard configuration apply.
    pub last_configured_at: Option<Instant>,

    /// Remote peers currently programmed on the local WireGuard interface.
    ///
    /// The controller uses this scoped set to decide which remote VXLAN peers can safely route
    /// over the tunnel. Any peer outside this set must be skipped while the local VXLAN devices are
    /// pinned to the WireGuard underlay interface.
    pub configured_peer_ids: HashSet<Uuid>,
}

/// Snapshot the per-peer configuration fields that affect the kernel WireGuard interface.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct PeerConfigFingerprint {
    peer_id: Uuid,
    public_key: [u8; 32],
    endpoint: String,
    allowed_ip: Ipv6Addr,
    keepalive: u16,
}

/// Pure summary of the remote peers this node should configure on its WireGuard interface.
///
/// The controller passes a desired peer scope derived from shared participating networks. This
/// planner then intersects that scope with the visible peer metadata snapshot so view-scoped
/// exclusions do not block local convergence.
struct WireGuardPeerPlan {
    peer_configs: Vec<PeerConfigFingerprint>,
    desired_peer_count: usize,
    all_desired_peers_advertised: bool,
    all_desired_peers_enabled: bool,
}

/// Local inputs required to reconcile the Mantissa-managed kernel WireGuard interface.
///
/// Keeping the node-local values together makes the main reconcile path read as:
/// load local identity -> compute remote peer plan -> apply/publish/prune.
#[cfg(target_os = "linux")]
struct LocalWireGuardConfig {
    ifname: String,
    keys: mantissa_net::wireguard::WireGuardKeys,
    listen_port: u16,
    tunnel_v6: Ipv6Addr,
    tunnel_ip: IpAddr,
    prefer_underlay: bool,
}

/// Return the inert underlay state used when WireGuard is disabled or unsupported on this host.
fn inactive_wireguard_state() -> WireGuardUnderlayState {
    WireGuardUnderlayState {
        underlay_active: false,
        required_peer_count: 0,
        ifname: MANTISSA_WIREGUARD_IFNAME.to_string(),
        tunnel_ip: None,
        config_hash: None,
        last_configured_at: None,
        configured_peer_ids: HashSet::new(),
    }
}

/// Compute a stable hash for the WireGuard interface configuration so we only reconfigure when needed.
fn compute_wireguard_config_hash(
    listen_port: u16,
    tunnel_ip: IpAddr,
    peers: &[PeerConfigFingerprint],
) -> u64 {
    let mut hasher = DefaultHasher::new();
    MANTISSA_WIREGUARD_IFNAME.hash(&mut hasher);
    MANTISSA_WIREGUARD_MTU.hash(&mut hasher);
    listen_port.hash(&mut hasher);
    tunnel_ip.hash(&mut hasher);
    peers.hash(&mut hasher);
    hasher.finish()
}

/// Decide whether the WireGuard interface should be reconfigured to reduce churn while correcting drift.
fn should_reconfigure_wireguard(
    previous: Option<&WireGuardUnderlayState>,
    config_hash: u64,
    now: Instant,
) -> bool {
    let Some(previous) = previous else {
        return true;
    };

    if previous.config_hash != Some(config_hash) {
        return true;
    }

    let Some(last) = previous.last_configured_at else {
        return true;
    };

    now.saturating_duration_since(last) >= WIREGUARD_FORCE_REFRESH_INTERVAL
}

/// Load the persisted preference for using WireGuard as the VXLAN underlay.
///
/// Preference read failures should not disable networking, so this helper logs and falls back to
/// plaintext underlay selection.
#[cfg(target_os = "linux")]
fn load_wireguard_underlay_preference_or_default() -> bool {
    match mantissa_net::wireguard::load_wireguard_underlay_preference() {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(
                target: "network",
                "failed to read wireguard underlay preference; defaulting to plaintext: {err}"
            );
            false
        }
    }
}

/// Load the local WireGuard identity, port, and deterministic tunnel address for this node.
///
/// These inputs are stable across reconciliations and are shared by every later step.
#[cfg(target_os = "linux")]
fn load_local_wireguard_config(self_id: Uuid) -> Result<LocalWireGuardConfig> {
    let keys_path = mantissa_net::wireguard::resolve_wireguard_key_path()
        .context("resolve wireguard key path")?;
    let keys = mantissa_net::wireguard::load_or_generate_wireguard_keys(keys_path)
        .context("load wireguard keys")?;
    let listen_port =
        mantissa_net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
            None,
            config::wireguard_port_override(),
        )
        .context("load wireguard listen port")?;
    let tunnel_v6 = mantissa_net::wireguard::wireguard_tunnel_ipv6(self_id);

    Ok(LocalWireGuardConfig {
        ifname: MANTISSA_WIREGUARD_IFNAME.to_string(),
        keys,
        listen_port,
        tunnel_v6,
        tunnel_ip: IpAddr::V6(tunnel_v6),
        prefer_underlay: load_wireguard_underlay_preference_or_default(),
    })
}

/// Build the scoped peer configuration plan for the current node.
///
/// Only peers present in both the caller-provided desired scope and the local peer snapshot are
/// considered. This keeps split-view exclusions from blocking encryption for the peers that remain
/// visible in the local controller scope.
fn build_wireguard_peer_plan(
    peers_snapshot: &[(Uuid, PeerValue)],
    self_id: Uuid,
    desired_peer_ids: &HashSet<Uuid>,
) -> WireGuardPeerPlan {
    let mut peer_configs = Vec::new();
    let mut desired_peer_count = 0usize;
    let mut all_desired_peers_advertised = true;
    let mut all_desired_peers_enabled = true;

    for (peer_id, peer_value) in peers_snapshot {
        if *peer_id == self_id || !desired_peer_ids.contains(peer_id) {
            continue;
        }

        desired_peer_count += 1;
        let Some(wg) = peer_value.wireguard.as_ref() else {
            all_desired_peers_advertised = false;
            all_desired_peers_enabled = false;
            continue;
        };

        if !wg.enabled {
            all_desired_peers_enabled = false;
        }

        let endpoint = match build_wireguard_endpoint(&peer_value.address, wg.port) {
            Some(endpoint) => endpoint,
            None => {
                all_desired_peers_advertised = false;
                all_desired_peers_enabled = false;
                continue;
            }
        };

        peer_configs.push(PeerConfigFingerprint {
            peer_id: *peer_id,
            public_key: wg.public_key,
            endpoint,
            allowed_ip: mantissa_net::wireguard::wireguard_tunnel_ipv6(*peer_id),
            keepalive: 25,
        });
    }

    peer_configs.sort_by_key(|peer| peer.peer_id);

    WireGuardPeerPlan {
        peer_configs,
        desired_peer_count,
        all_desired_peers_advertised,
        all_desired_peers_enabled,
    }
}

/// Load the peer snapshot and build the scoped remote configuration plan for this node.
///
/// The desired peer scope comes from the network controller and already excludes unrelated
/// cluster members. This helper only intersects that scope with the current peer metadata.
#[cfg(target_os = "linux")]
fn load_wireguard_peer_plan(
    registry: &Registry,
    self_id: Uuid,
    desired_peer_ids: &HashSet<Uuid>,
) -> Result<WireGuardPeerPlan> {
    let peers_snapshot = registry
        .peer_values_snapshot()
        .context("load peers snapshot for wireguard")?;
    Ok(build_wireguard_peer_plan(
        &peers_snapshot,
        self_id,
        desired_peer_ids,
    ))
}

/// Convert the scoped peer plan into kernel WireGuard peer objects.
///
/// Keeping the protocol-facing peer fingerprint separate from the kernel object creation keeps
/// the planner pure and the kernel apply code focused on translation.
#[cfg(target_os = "linux")]
fn build_kernel_wireguard_peers(
    peer_plan: &WireGuardPeerPlan,
) -> Result<Vec<defguard_wireguard_rs::host::Peer>> {
    let mut peers = Vec::with_capacity(peer_plan.peer_configs.len());
    for peer_config in &peer_plan.peer_configs {
        let mut peer = defguard_wireguard_rs::host::Peer::new(
            defguard_wireguard_rs::key::Key::new(peer_config.public_key),
        );
        peer.set_allowed_ips(vec![defguard_wireguard_rs::net::IpAddrMask::host(
            IpAddr::V6(peer_config.allowed_ip),
        )]);
        peer.persistent_keepalive_interval = Some(peer_config.keepalive);
        peer.set_endpoint(&peer_config.endpoint)
            .with_context(|| format!("set wireguard endpoint for peer {}", peer_config.peer_id))?;
        peers.push(peer);
    }
    Ok(peers)
}

/// Apply the desired WireGuard interface and peer routing state to the kernel.
///
/// This is the expensive part of reconciliation, so callers should only invoke it after the
/// configuration hash says a refresh is necessary.
#[cfg(target_os = "linux")]
async fn apply_wireguard_interface_config(
    local: &LocalWireGuardConfig,
    peer_plan: &WireGuardPeerPlan,
) -> Result<()> {
    let peers = build_kernel_wireguard_peers(peer_plan)?;
    let interface_config = defguard_wireguard_rs::InterfaceConfiguration {
        name: local.ifname.clone(),
        prvkey: BASE64_STANDARD.encode(local.keys.to_private_bytes()),
        addresses: vec![defguard_wireguard_rs::net::IpAddrMask::host(
            local.tunnel_ip,
        )],
        port: local.listen_port,
        peers: peers.clone(),
        mtu: Some(MANTISSA_WIREGUARD_MTU),
    };
    let ifname_for_blocking = local.ifname.clone();

    tokio::task::spawn_blocking(move || -> Result<()> {
        use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};

        let mut wgapi =
            WGApi::<Kernel>::new(ifname_for_blocking).context("create WGApi<Kernel>")?;
        wgapi
            .create_interface()
            .context("create wireguard interface")?;
        wgapi
            .configure_interface(&interface_config)
            .context("configure wireguard interface")?;
        wgapi
            .configure_peer_routing(&peers)
            .context("configure wireguard peer routing")?;
        ensure_vxlan_firewall_accept(&interface_config.name)?;
        Ok(())
    })
    .await
    .context("wireguard configuration task panicked")??;

    Ok(())
}

/// Reconcile the kernel WireGuard device when the computed configuration differs from the last apply.
///
/// The returned hash/timestamp pair is persisted in controller state so later reconciliations can
/// skip expensive rewrites while still forcing a periodic refresh for external drift.
#[cfg(target_os = "linux")]
async fn reconcile_wireguard_kernel_state(
    local: &LocalWireGuardConfig,
    peer_plan: &WireGuardPeerPlan,
    previous: Option<&WireGuardUnderlayState>,
    now: Instant,
) -> Result<(u64, Option<Instant>)> {
    let config_hash =
        compute_wireguard_config_hash(local.listen_port, local.tunnel_ip, &peer_plan.peer_configs);

    if should_reconfigure_wireguard(previous, config_hash, now) {
        apply_wireguard_interface_config(local, peer_plan).await?;
        return Ok((config_hash, Some(now)));
    }

    Ok((
        config_hash,
        previous.and_then(|state| state.last_configured_at),
    ))
}

/// Publish this node's WireGuard advertisement back into the Peers CRDT.
///
/// Publication happens after the local kernel device is configured so remote peers never see an
/// enabled advertisement for a node that still lacks its interface.
#[cfg(target_os = "linux")]
async fn publish_local_wireguard_state(
    registry: &Registry,
    local: &LocalWireGuardConfig,
) -> Result<()> {
    registry
        .upsert_self_wireguard(WireGuardPeerValue {
            public_key: local.keys.public_bytes(),
            port: local.listen_port,
            enabled: true,
        })
        .await
        .context("publish self wireguard state")
}

/// Report whether the scoped peer set is ready for VXLAN traffic to switch onto the tunnel.
///
/// Nodes with no shared encrypted peers keep the underlay inactive. Otherwise every scoped peer
/// must advertise WireGuard metadata and mark itself enabled.
#[cfg(target_os = "linux")]
fn scoped_wireguard_peers_ready(peer_plan: &WireGuardPeerPlan) -> bool {
    peer_plan.desired_peer_count == 0
        || (peer_plan.all_desired_peers_advertised && peer_plan.all_desired_peers_enabled)
}

/// Log when a persisted underlay preference exists but the scoped peer set is not ready yet.
///
/// This makes delayed encryption activation easier to understand without burying the main
/// reconcile flow in logging-specific conditionals.
#[cfg(target_os = "linux")]
fn log_unready_wireguard_preference(
    prefer_underlay: bool,
    peer_plan: &WireGuardPeerPlan,
    ready: bool,
) {
    if prefer_underlay && peer_plan.desired_peer_count > 0 && !ready {
        tracing::debug!(
            target: "network",
            peers = peer_plan.desired_peer_count,
            "wireguard underlay preference set but scoped peers are not ready yet; blocking vxlan underlay activation"
        );
    }
}

/// Persist the underlay preference once every scoped peer is ready for encrypted VXLAN.
///
/// Mantissa only flips this sticky preference after the local node is configured and every remote
/// peer in scope has published an enabled advertisement.
#[cfg(target_os = "linux")]
fn persist_wireguard_underlay_preference_if_ready(
    prefer_underlay: bool,
    published: bool,
    peer_plan: &WireGuardPeerPlan,
    ready: bool,
) {
    if published
        && ready
        && peer_plan.desired_peer_count > 0
        && !prefer_underlay
        && let Err(err) = mantissa_net::wireguard::persist_wireguard_underlay_preference(true)
    {
        tracing::warn!(
            target: "network",
            "failed to persist wireguard underlay preference; may not survive restarts: {err}"
        );
    }
}

/// Prune stale tunnel routes that no longer belong to the local node or its scoped peers.
///
/// Route cleanup is best-effort because stale peer routes should not block overlay reconciliation.
#[cfg(target_os = "linux")]
async fn prune_wireguard_tunnel_routes(
    local: &LocalWireGuardConfig,
    peer_plan: &WireGuardPeerPlan,
) {
    let mut desired_tunnel_routes = HashSet::with_capacity(peer_plan.peer_configs.len() + 1);
    desired_tunnel_routes.insert(local.tunnel_v6);
    desired_tunnel_routes.extend(peer_plan.peer_configs.iter().map(|peer| peer.allowed_ip));

    if let Err(err) = prune_stale_wireguard_routes(&local.ifname, &desired_tunnel_routes).await {
        tracing::warn!(
            target: "network",
            ifname = local.ifname,
            "failed to prune stale wireguard tunnel routes: {err:#}"
        );
    }
}

/// Inputs used to derive one controller-facing WireGuard underlay snapshot.
#[cfg(target_os = "linux")]
struct WireGuardUnderlayStateInput<'a> {
    ifname: String,
    tunnel_ip: IpAddr,
    config_hash: u64,
    last_configured_at: Option<Instant>,
    published: bool,
    peer_plan: &'a WireGuardPeerPlan,
    scoped_ready_for_encryption: bool,
    prefer_underlay: bool,
}

/// Build the controller-facing underlay state snapshot from the latest reconcile results.
#[cfg(target_os = "linux")]
fn build_wireguard_underlay_state(
    input: WireGuardUnderlayStateInput<'_>,
) -> WireGuardUnderlayState {
    WireGuardUnderlayState {
        underlay_active: input.published
            && input.scoped_ready_for_encryption
            && (input.peer_plan.desired_peer_count > 0 || input.prefer_underlay),
        required_peer_count: input.peer_plan.desired_peer_count,
        ifname: input.ifname,
        tunnel_ip: Some(input.tunnel_ip),
        config_hash: Some(input.config_hash),
        last_configured_at: input.last_configured_at,
        configured_peer_ids: input
            .peer_plan
            .peer_configs
            .iter()
            .map(|peer| peer.peer_id)
            .collect(),
    }
}

/// Ensure the Mantissa-managed WireGuard underlay is configured on this node and return the
/// current underlay state decision.
///
/// This function is called by the network controller reconciliation loop. It is designed to be:
/// - **Mandatory when scoped peers exist**: once a node must exchange VXLAN traffic with remote
///   peers, failure to prepare WireGuard aborts network readiness instead of falling back to
///   plaintext.
/// - **Idempotent**: repeated calls converge to the same kernel configuration.
/// - **Self-contained**: requires no external `wg` tooling and uses the Peers CRDT to discover
///   peer keys and endpoints for nodes that currently share a participating network with the local
///   node.
#[cfg(target_os = "linux")]
pub async fn ensure_wireguard_underlay(
    registry: &Registry,
    self_id: Uuid,
    desired_peer_ids: &HashSet<Uuid>,
    previous: Option<WireGuardUnderlayState>,
) -> Result<WireGuardUnderlayState> {
    if !config::wireguard_enabled() {
        return Ok(inactive_wireguard_state());
    }

    let wireguard_required = !desired_peer_ids.is_empty();
    if unsafe { libc::geteuid() } != 0 {
        if wireguard_required {
            anyhow::bail!(
                "wireguard underlay requires root privileges while encrypted networks are active"
            );
        }
        return Ok(inactive_wireguard_state());
    }

    let now = Instant::now();
    let local = load_local_wireguard_config(self_id)?;
    let peer_plan = load_wireguard_peer_plan(registry, self_id, desired_peer_ids)?;
    let (config_hash, last_configured_at) =
        reconcile_wireguard_kernel_state(&local, &peer_plan, previous.as_ref(), now).await?;
    let published = match publish_local_wireguard_state(registry, &local).await {
        Ok(()) => true,
        Err(err) if wireguard_required => return Err(err),
        Err(err) => {
            tracing::warn!(
                target: "network",
                "wireguard configured but could not publish self enabled state yet: {err:#}"
            );
            false
        }
    };

    // Only switch the VXLAN underlay once every scoped peer has successfully configured its own
    // WireGuard interface. This keeps unrelated cluster members out of the readiness gate while
    // still preventing one side of an actively shared network from switching too early.
    let scoped_ready_for_encryption = scoped_wireguard_peers_ready(&peer_plan);
    log_unready_wireguard_preference(
        local.prefer_underlay,
        &peer_plan,
        scoped_ready_for_encryption,
    );
    persist_wireguard_underlay_preference_if_ready(
        local.prefer_underlay,
        published,
        &peer_plan,
        scoped_ready_for_encryption,
    );
    prune_wireguard_tunnel_routes(&local, &peer_plan).await;

    Ok(build_wireguard_underlay_state(
        WireGuardUnderlayStateInput {
            ifname: local.ifname,
            tunnel_ip: local.tunnel_ip,
            config_hash,
            last_configured_at,
            published,
            peer_plan: &peer_plan,
            scoped_ready_for_encryption,
            prefer_underlay: local.prefer_underlay,
        },
    ))
}

/// Non-Linux builds do not provision the kernel underlay. They always fall back to plaintext.
#[cfg(not(target_os = "linux"))]
pub async fn ensure_wireguard_underlay(
    _registry: &Registry,
    _self_id: Uuid,
    _desired_peer_ids: &HashSet<Uuid>,
    _previous: Option<WireGuardUnderlayState>,
) -> Result<WireGuardUnderlayState> {
    Ok(WireGuardUnderlayState::default())
}

/// Build a WireGuard endpoint string ("host:port" or "[v6]:port") from a peer's advertised
/// address and WireGuard listen port.
///
/// When `listen_port` is `0` we default to reusing the port embedded in the advertised address.
/// This keeps the WireGuard underlay "zero-config" in the common case where nodes already expose
/// a reachable control-plane port to each other.
///
/// Returns `None` when the address is not compatible with an IP underlay (e.g. in-process
/// transports used in tests).
fn build_wireguard_endpoint(advertise: &str, listen_port: u16) -> Option<String> {
    if advertise.starts_with("inproc://") || advertise.starts_with("unix://") {
        return None;
    }

    if let Ok(sa) = advertise.parse::<SocketAddr>() {
        let port = if listen_port == 0 {
            sa.port()
        } else {
            listen_port
        };
        return Some(match sa.ip() {
            IpAddr::V4(ip) => format!("{ip}:{port}"),
            IpAddr::V6(ip) => format!("[{ip}]:{port}"),
        });
    }

    let (host, advertised_port) = advertise
        .rsplit_once(':')
        .map(|(host, port)| (host, port.parse::<u16>().ok()))
        .unwrap_or((advertise, None));
    let port = if listen_port == 0 {
        advertised_port.unwrap_or(mantissa_net::wireguard::DEFAULT_WIREGUARD_LISTEN_PORT)
    } else {
        listen_port
    };

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(match ip {
            IpAddr::V4(ip) => format!("{ip}:{port}"),
            IpAddr::V6(ip) => format!("[{ip}]:{port}"),
        });
    }

    Some(format!("{host}:{port}"))
}

/// Ensure the host firewall admits VXLAN-over-WireGuard traffic.
///
/// When Mantissa runs VXLAN over the WireGuard underlay, the kernel sends/receives VXLAN packets
/// as UDP/IPv6 (dst port 4789) on the `mnwg0` interface. Some environments default-drop IPv6
/// traffic via ip6tables/ufw, which would allow WireGuard handshakes (UDP/IPv4 on the physical
/// interface) but silently drop the encapsulated VXLAN packets. That failure mode looks like:
/// - `wg show` appears healthy
/// - overlay service discovery / health probes time out across nodes
///
/// We add a minimal INPUT/OUTPUT allow rule for UDP/4789 on the WireGuard interface as part of
/// mandatory encrypted underlay setup. When firewall management is enabled, failure to install
/// these rules aborts WireGuard readiness so Mantissa never silently falls back to a broken or
/// plaintext dataplane.
#[cfg(target_os = "linux")]
fn ensure_vxlan_firewall_accept(ifname: &str) -> Result<()> {
    if !config::wireguard_manage_firewall() {
        return Ok(());
    }

    let port = MANTISSA_VXLAN_UDP_PORT.to_string();
    let spec = [
        "-i",
        ifname,
        "-p",
        "udp",
        "--dport",
        port.as_str(),
        "-j",
        "ACCEPT",
    ];

    // INPUT chain: admit VXLAN packets arriving from the tunnel.
    if !ip6tables_has_rule("INPUT", &spec) {
        ip6tables_insert_rule("INPUT", &spec).with_context(|| {
            format!("add ip6tables INPUT rule for VXLAN over wireguard on {ifname}")
        })?;
        tracing::debug!(
            target: "network",
            ifname,
            port = MANTISSA_VXLAN_UDP_PORT,
            "installed ip6tables INPUT accept rule for VXLAN over wireguard"
        );
    }

    // OUTPUT chain: admit locally generated VXLAN packets egressing the tunnel (usually already
    // allowed, but some hardened hosts drop output by default).
    let output_spec = [
        "-o",
        ifname,
        "-p",
        "udp",
        "--sport",
        port.as_str(),
        "-j",
        "ACCEPT",
    ];
    if !ip6tables_has_rule("OUTPUT", &output_spec) {
        ip6tables_insert_rule("OUTPUT", &output_spec).with_context(|| {
            format!("add ip6tables OUTPUT rule for VXLAN over wireguard on {ifname}")
        })?;
        tracing::debug!(
            target: "network",
            ifname,
            port = MANTISSA_VXLAN_UDP_PORT,
            "installed ip6tables OUTPUT accept rule for VXLAN over wireguard"
        );
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
/// Treat VXLAN firewall setup as a no-op on non-Linux builds.
///
/// The production WireGuard overlay is Linux-only, but keeping this stub lets the rest of the crate
/// compile on macOS for control-plane development and unprivileged tests.
fn ensure_vxlan_firewall_accept(_ifname: &str) -> Result<()> {
    Ok(())
}

/// Remove stale `/128` tunnel routes from the WireGuard interface.
///
/// Kernel route helpers can leave old peer routes behind after cluster membership churn. Keeping
/// those stale routes can blackhole VXLAN traffic whenever stale forwarding entries still point at
/// retired tunnel addresses, so we prune every route outside the current self + peer set.
#[cfg(target_os = "linux")]
async fn prune_stale_wireguard_routes(
    ifname: &str,
    desired_routes: &HashSet<Ipv6Addr>,
) -> Result<()> {
    use futures::TryStreamExt;
    use rtnetlink::packet_route::route::{RouteAddress, RouteAttribute, RouteHeader};
    use rtnetlink::{RouteMessageBuilder, new_connection};

    let (connection, handle, _) =
        new_connection().context("open rtnetlink connection for wireguard route pruning")?;
    tokio::spawn(connection);

    // Rtnetlink link lookups are dump streams. Keep the first match but drain the
    // stream to completion so the connection task has a receiver for every kernel
    // response and does not emit "failed to forward response back to the handle".
    let mut links = handle.link().get().match_name(ifname.to_string()).execute();
    let mut ifindex = None;
    while let Some(link) = links
        .try_next()
        .await
        .with_context(|| format!("lookup link index for {ifname}"))?
    {
        if ifindex.is_none() {
            ifindex = Some(link.header.index);
        }
    }
    let ifindex = ifindex
        .ok_or_else(|| anyhow::anyhow!("link {ifname} missing while pruning wireguard routes"))?;

    let mut routes = handle
        .route()
        .get(RouteMessageBuilder::<Ipv6Addr>::new().build())
        .execute();

    let mut stale_routes = Vec::new();
    while let Some(route) = routes
        .try_next()
        .await
        .with_context(|| format!("list ipv6 routes on {ifname}"))?
    {
        let mut table = u32::from(route.header.table);
        let prefix_len = route.header.destination_prefix_length;
        let mut route_ifindex = None;
        let mut route_ip = None;

        for attribute in route.attributes {
            match attribute {
                RouteAttribute::Oif(index) => route_ifindex = Some(index),
                RouteAttribute::Destination(RouteAddress::Inet6(ip)) => route_ip = Some(ip),
                RouteAttribute::Table(route_table) => table = route_table,
                _ => {}
            }
        }

        // Match `ip -6 route show dev <ifname>` semantics: main table and this output interface.
        if table != u32::from(RouteHeader::RT_TABLE_MAIN) || route_ifindex != Some(ifindex) {
            continue;
        }

        if prefix_len != 128 {
            continue;
        }

        let Some(route_ip) = route_ip else {
            continue;
        };
        if !is_mantissa_tunnel_ipv6(route_ip) || desired_routes.contains(&route_ip) {
            continue;
        }
        stale_routes.push(route_ip);
    }

    for route_ip in stale_routes {
        let delete = RouteMessageBuilder::<Ipv6Addr>::new()
            .destination_prefix(route_ip, 128)
            .output_interface(ifindex)
            .build();

        if let Err(err) = handle.route().del(delete).execute().await {
            match err {
                rtnetlink::Error::NetlinkError(message) => {
                    let errno = message.raw_code().abs();
                    if errno != libc::ENOENT && errno != libc::ESRCH {
                        let cidr = format!("{route_ip}/128");
                        return Err(rtnetlink::Error::NetlinkError(message)).with_context(|| {
                            format!("delete stale ipv6 route {cidr} on {ifname}")
                        });
                    }
                }
                other => {
                    let cidr = format!("{route_ip}/128");
                    return Err(other)
                        .with_context(|| format!("delete stale ipv6 route {cidr} on {ifname}"));
                }
            }
        }
    }

    Ok(())
}

/// Check whether an IPv6 address belongs to Mantissa's deterministic tunnel prefix.
#[cfg(target_os = "linux")]
fn is_mantissa_tunnel_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    segments[0] == 0xfd42 && segments[1] == 0x6d61 && segments[2] == 0x6e74 && segments[3] == 0x6973
}

#[cfg(target_os = "linux")]
/// Return whether the filter table already contains the requested IPv6 firewall rule.
///
/// Firewall checks are best-effort because a missing client or rule should drive insertion rather
/// than fail the whole WireGuard reconciliation path before any route state is updated.
fn ip6tables_has_rule(chain: &str, spec: &[&str]) -> bool {
    let rule = spec.join(" ");
    let ip6t = match iptables::new(true) {
        Ok(client) => client,
        Err(err) => {
            tracing::debug!(
                target: "network",
                chain,
                rule = %rule,
                error = %err,
                "ip6tables check failed while creating client"
            );
            return false;
        }
    };

    match ip6t.exists("filter", chain, &rule) {
        Ok(exists) => exists,
        Err(err) => {
            tracing::debug!(
                target: "network",
                chain,
                rule = %rule,
                error = %err,
                "ip6tables check failed"
            );
            false
        }
    }
}

#[cfg(target_os = "linux")]
/// Insert one IPv6 firewall rule at the top of the requested filter chain.
///
/// WireGuard reconciliation uses this narrowly for VXLAN-over-WireGuard accept rules so the overlay
/// can recover after host firewall state is flushed or after a fresh node bootstrap.
fn ip6tables_insert_rule(chain: &str, spec: &[&str]) -> std::io::Result<()> {
    let rule = spec.join(" ");
    let ip6t = iptables::new(true).map_err(|err| {
        std::io::Error::other(format!("failed to create ip6tables client: {err}"))
    })?;
    ip6t.insert("filter", chain, &rule, 1)
        .map_err(|err| std::io::Error::other(format!("ip6tables insert failed: {err}")))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::{
        WireGuardPeerPlan, WireGuardUnderlayStateInput, build_wireguard_peer_plan,
        build_wireguard_underlay_state, scoped_wireguard_peers_ready,
    };
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::topology::peers::{
        PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue,
    };
    use std::collections::HashSet;
    use uuid::Uuid;

    /// Build one minimal peer value for scoped WireGuard planning tests.
    fn test_peer_value(address: &str, wireguard: Option<WireGuardPeerValue>) -> PeerValue {
        PeerValue {
            address: address.to_string(),
            hostname: "peer".to_string(),
            platform_os: String::new(),
            platform_arch: String::new(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard,
            runtime_support: RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState::schedulable_default(Uuid::nil()),
            readiness: Default::default(),
            labels: crate::topology::peers::PeerLabelState::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: PeerMembership::active(1),
        }
    }

    /// Collect peer identifiers from the plan for assertion readability.
    fn planned_peer_ids(plan: &WireGuardPeerPlan) -> Vec<Uuid> {
        plan.peer_configs.iter().map(|peer| peer.peer_id).collect()
    }

    /// Ensure the planner only includes peers inside the caller-provided scope.
    #[test]
    fn scoped_plan_ignores_unrelated_peers() {
        let self_id = Uuid::new_v4();
        let scoped_peer = Uuid::new_v4();
        let unrelated_peer = Uuid::new_v4();
        let desired = HashSet::from([scoped_peer]);
        let peers = vec![
            (
                scoped_peer,
                test_peer_value(
                    "10.0.0.2:51820",
                    Some(WireGuardPeerValue {
                        public_key: [7u8; 32],
                        port: 0,
                        enabled: true,
                    }),
                ),
            ),
            (
                unrelated_peer,
                test_peer_value(
                    "10.0.0.3:51820",
                    Some(WireGuardPeerValue {
                        public_key: [8u8; 32],
                        port: 0,
                        enabled: true,
                    }),
                ),
            ),
        ];

        let plan = build_wireguard_peer_plan(&peers, self_id, &desired);

        assert_eq!(plan.desired_peer_count, 1);
        assert_eq!(planned_peer_ids(&plan), vec![scoped_peer]);
        assert!(plan.all_desired_peers_advertised);
        assert!(plan.all_desired_peers_enabled);
    }

    /// Ensure unrelated peers do not block scoped readiness, while a scoped peer without metadata
    /// still does.
    #[test]
    fn scoped_plan_only_gates_on_selected_peers() {
        let self_id = Uuid::new_v4();
        let ready_peer = Uuid::new_v4();
        let missing_peer = Uuid::new_v4();
        let unrelated_peer = Uuid::new_v4();
        let desired = HashSet::from([ready_peer, missing_peer]);
        let peers = vec![
            (
                ready_peer,
                test_peer_value(
                    "10.0.0.2:51820",
                    Some(WireGuardPeerValue {
                        public_key: [7u8; 32],
                        port: 0,
                        enabled: true,
                    }),
                ),
            ),
            (missing_peer, test_peer_value("inproc://peer", None)),
            (
                unrelated_peer,
                test_peer_value(
                    "inproc://peer",
                    Some(WireGuardPeerValue {
                        public_key: [9u8; 32],
                        port: 0,
                        enabled: true,
                    }),
                ),
            ),
        ];

        let plan = build_wireguard_peer_plan(&peers, self_id, &desired);

        assert_eq!(plan.desired_peer_count, 2);
        assert_eq!(planned_peer_ids(&plan), vec![ready_peer]);
        assert!(!plan.all_desired_peers_advertised);
        assert!(!plan.all_desired_peers_enabled);
    }

    /// Keep WireGuard selected after activation even when the current scoped peer set is empty.
    #[test]
    fn underlay_state_honors_persisted_wireguard_preference_without_peers() {
        let plan = WireGuardPeerPlan {
            peer_configs: Vec::new(),
            desired_peer_count: 0,
            all_desired_peers_advertised: true,
            all_desired_peers_enabled: true,
        };

        let state = build_wireguard_underlay_state(WireGuardUnderlayStateInput {
            ifname: super::MANTISSA_WIREGUARD_IFNAME.to_string(),
            tunnel_ip: "fd42:6d61:6e74:6973::1"
                .parse()
                .expect("valid test tunnel ip"),
            config_hash: 7,
            last_configured_at: None,
            published: true,
            peer_plan: &plan,
            scoped_ready_for_encryption: scoped_wireguard_peers_ready(&plan),
            prefer_underlay: true,
        });

        assert!(
            state.underlay_active,
            "a persisted WireGuard underlay preference should prevent fallback to plaintext"
        );
        assert_eq!(state.required_peer_count, 0);
        assert!(state.configured_peer_ids.is_empty());
    }
}
