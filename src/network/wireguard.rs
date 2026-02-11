use crate::config;
use crate::registry::Registry;
use crate::topology::peers::WireGuardPeerValue;
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::process::Command;
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
    pub underlay_active: bool,

    /// The WireGuard interface name (stable).
    pub ifname: String,

    /// The local tunnel IP address used as the VXLAN underlay source/destination.
    pub tunnel_ip: Option<IpAddr>,

    /// Hash of the last WireGuard interface configuration applied by Mantissa.
    pub config_hash: Option<u64>,

    /// Timestamp of the last successful WireGuard configuration apply.
    pub last_configured_at: Option<Instant>,
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

/// Ensure the Mantissa-managed WireGuard underlay is configured on this node and return the
/// current underlay state decision.
///
/// This function is called by the network controller reconciliation loop. It is designed to be:
/// - **Best-effort**: failures do not stop the overlay; Mantissa falls back to the plaintext
///   underlay.
/// - **Idempotent**: repeated calls converge to the same kernel configuration.
/// - **Self-contained**: requires no external `wg` tooling and uses the Peers CRDT to discover
///   peer keys and endpoints.
#[cfg(target_os = "linux")]
pub async fn ensure_wireguard_underlay(
    registry: &Registry,
    self_id: Uuid,
    previous: Option<WireGuardUnderlayState>,
) -> Result<WireGuardUnderlayState> {
    if !config::wireguard_enabled() {
        return Ok(WireGuardUnderlayState {
            underlay_active: false,
            ifname: MANTISSA_WIREGUARD_IFNAME.to_string(),
            tunnel_ip: None,
            config_hash: None,
            last_configured_at: None,
        });
    }

    if unsafe { libc::geteuid() } != 0 {
        return Ok(WireGuardUnderlayState {
            underlay_active: false,
            ifname: MANTISSA_WIREGUARD_IFNAME.to_string(),
            tunnel_ip: None,
            config_hash: None,
            last_configured_at: None,
        });
    }

    let now = Instant::now();
    let prefer_underlay = match net::wireguard::load_wireguard_underlay_preference() {
        Ok(value) => value,
        Err(err) => {
            tracing::warn!(
                target: "network",
                "failed to read wireguard underlay preference; defaulting to plaintext: {err}"
            );
            false
        }
    };

    let keys_path =
        net::wireguard::resolve_wireguard_key_path().context("resolve wireguard key path")?;
    let keys = net::wireguard::load_or_generate_wireguard_keys(keys_path)
        .context("load wireguard keys")?;

    let listen_port =
        net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
            None,
            config::wireguard_port_override(),
        )
        .context("load wireguard listen port")?;

    let tunnel_v6 = net::wireguard::wireguard_tunnel_ipv6(self_id);
    let tunnel_ip = IpAddr::V6(tunnel_v6);

    let peers_snapshot = registry
        .peer_values_snapshot()
        .context("load peers snapshot for wireguard")?;

    let mut peer_configs = Vec::new();
    let mut peer_count = 0usize;
    let mut all_peers_advertised = true;
    let mut all_peers_enabled = true;

    for (peer_id, peer_value) in peers_snapshot {
        if peer_id == self_id {
            continue;
        }

        peer_count += 1;
        let Some(wg) = peer_value.wireguard else {
            all_peers_advertised = false;
            all_peers_enabled = false;
            continue;
        };

        if !wg.enabled {
            all_peers_enabled = false;
        }

        let endpoint = match build_wireguard_endpoint(&peer_value.address, wg.port) {
            Some(ep) => ep,
            None => {
                all_peers_advertised = false;
                all_peers_enabled = false;
                continue;
            }
        };

        peer_configs.push(PeerConfigFingerprint {
            peer_id,
            public_key: wg.public_key,
            endpoint,
            allowed_ip: net::wireguard::wireguard_tunnel_ipv6(peer_id),
            keepalive: 25,
        });
    }

    peer_configs.sort_by_key(|peer| peer.peer_id);

    let config_hash = compute_wireguard_config_hash(listen_port, tunnel_ip, &peer_configs);
    let should_configure = should_reconfigure_wireguard(previous.as_ref(), config_hash, now);

    let ifname = MANTISSA_WIREGUARD_IFNAME.to_string();

    let last_configured_at = if should_configure {
        let mut peers = Vec::with_capacity(peer_configs.len());
        for peer_config in &peer_configs {
            let mut peer = defguard_wireguard_rs::host::Peer::new(
                defguard_wireguard_rs::key::Key::new(peer_config.public_key),
            );
            peer.set_allowed_ips(vec![defguard_wireguard_rs::net::IpAddrMask::host(
                IpAddr::V6(peer_config.allowed_ip),
            )]);
            peer.persistent_keepalive_interval = Some(peer_config.keepalive);
            peer.set_endpoint(&peer_config.endpoint).with_context(|| {
                format!("set wireguard endpoint for peer {}", peer_config.peer_id)
            })?;
            peers.push(peer);
        }

        let prvkey_b64 = BASE64_STANDARD.encode(keys.to_private_bytes());
        let interface_config = defguard_wireguard_rs::InterfaceConfiguration {
            name: ifname.clone(),
            prvkey: prvkey_b64,
            addresses: vec![defguard_wireguard_rs::net::IpAddrMask::host(tunnel_ip)],
            port: listen_port,
            peers: peers.clone(),
            mtu: Some(MANTISSA_WIREGUARD_MTU),
        };

        let ifname_for_blocking = ifname.clone();
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
            ensure_vxlan_firewall_accept(&interface_config.name);
            Ok(())
        })
        .await
        .context("wireguard configuration task panicked")??;
        Some(now)
    } else {
        previous.as_ref().and_then(|state| state.last_configured_at)
    };

    let published = match registry
        .upsert_self_wireguard(WireGuardPeerValue {
            public_key: keys.public_bytes(),
            port: listen_port,
            enabled: true,
        })
        .await
    {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                target: "network",
                "wireguard configured but could not publish self enabled state yet: {err}"
            );
            false
        }
    };

    // Only switch the VXLAN underlay once every peer has successfully configured its own
    // WireGuard interface (enabled = true). This avoids one node switching early and breaking
    // cross-node overlay traffic during cluster bootstrap or rolling restarts.
    let cluster_ready_for_encryption =
        peer_count == 0 || (all_peers_advertised && all_peers_enabled);

    if prefer_underlay && peer_count > 0 && !cluster_ready_for_encryption {
        tracing::debug!(
            target: "network",
            "wireguard underlay preference set but cluster not ready yet; keeping plaintext underlay"
        );
    }

    if published && cluster_ready_for_encryption && peer_count > 0 && !prefer_underlay {
        if let Err(err) = net::wireguard::persist_wireguard_underlay_preference(true) {
            tracing::warn!(
                target: "network",
                "failed to persist wireguard underlay preference; may not survive restarts: {err}"
            );
        }
    }

    let mut desired_tunnel_routes = HashSet::with_capacity(peer_configs.len() + 1);
    desired_tunnel_routes.insert(tunnel_v6);
    desired_tunnel_routes.extend(peer_configs.iter().map(|peer| peer.allowed_ip));
    if let Err(err) = prune_stale_wireguard_routes(&ifname, &desired_tunnel_routes) {
        tracing::warn!(
            target: "network",
            ifname,
            "failed to prune stale wireguard tunnel routes: {err:#}"
        );
    }

    Ok(WireGuardUnderlayState {
        underlay_active: published && cluster_ready_for_encryption,
        ifname,
        tunnel_ip: Some(tunnel_ip),
        config_hash: Some(config_hash),
        last_configured_at,
    })
}

/// Non-Linux builds do not provision the kernel underlay. They always fall back to plaintext.
#[cfg(not(target_os = "linux"))]
pub async fn ensure_wireguard_underlay(
    _registry: &Registry,
    _self_id: Uuid,
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
        advertised_port.unwrap_or(net::wireguard::DEFAULT_WIREGUARD_LISTEN_PORT)
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
/// We add a minimal INPUT/OUTPUT allow rule for UDP/4789 on the WireGuard interface as a
/// best-effort step. Failures are logged and do not block networking setup.
#[cfg(target_os = "linux")]
fn ensure_vxlan_firewall_accept(ifname: &str) {
    if !config::wireguard_manage_firewall() {
        return;
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
        if let Err(err) = ip6tables_insert_rule("INPUT", &spec) {
            tracing::debug!(
                target: "network",
                ifname,
                error = %err,
                "failed to add ip6tables INPUT rule for VXLAN over wireguard"
            );
        } else {
            tracing::debug!(
                target: "network",
                ifname,
                port = MANTISSA_VXLAN_UDP_PORT,
                "installed ip6tables INPUT accept rule for VXLAN over wireguard"
            );
        }
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
        if let Err(err) = ip6tables_insert_rule("OUTPUT", &output_spec) {
            tracing::debug!(
                target: "network",
                ifname,
                error = %err,
                "failed to add ip6tables OUTPUT rule for VXLAN over wireguard"
            );
        } else {
            tracing::debug!(
                target: "network",
                ifname,
                port = MANTISSA_VXLAN_UDP_PORT,
                "installed ip6tables OUTPUT accept rule for VXLAN over wireguard"
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn ensure_vxlan_firewall_accept(_ifname: &str) {}

/// Remove stale `/128` tunnel routes from the WireGuard interface.
///
/// Kernel route helpers can leave old peer routes behind after cluster membership churn. Keeping
/// those stale routes can blackhole VXLAN traffic whenever stale forwarding entries still point at
/// retired tunnel addresses, so we prune every route outside the current self + peer set.
#[cfg(target_os = "linux")]
fn prune_stale_wireguard_routes(ifname: &str, desired_routes: &HashSet<Ipv6Addr>) -> Result<()> {
    let output = Command::new("ip")
        .arg("-6")
        .arg("route")
        .arg("show")
        .arg("dev")
        .arg(ifname)
        .output()
        .with_context(|| format!("list ipv6 routes on {ifname}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!(
            "ip -6 route show dev {ifname} failed (status {:?}): {}",
            output.status.code(),
            stderr.trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        let Some(prefix) = line.split_whitespace().next() else {
            continue;
        };
        let (ip_text, prefix_len) = if let Some((ip_text, prefix_len)) = prefix.split_once('/') {
            (ip_text, prefix_len.parse::<u8>().ok())
        } else {
            // `ip route` may omit `/128` for host routes; treat bare IPv6 addresses as /128.
            (prefix, Some(128))
        };
        if prefix_len != Some(128) {
            continue;
        }

        let Ok(route_ip) = ip_text.parse::<Ipv6Addr>() else {
            continue;
        };
        if !is_mantissa_tunnel_ipv6(route_ip) || desired_routes.contains(&route_ip) {
            continue;
        }

        let cidr = format!("{route_ip}/128");
        let delete = Command::new("ip")
            .arg("-6")
            .arg("route")
            .arg("del")
            .arg(cidr.as_str())
            .arg("dev")
            .arg(ifname)
            .output()
            .with_context(|| format!("delete stale ipv6 route {cidr} on {ifname}"))?;
        if !delete.status.success() {
            let stderr = String::from_utf8_lossy(&delete.stderr);
            if !stderr.contains("No such process") {
                return Err(anyhow::anyhow!(
                    "ip -6 route del {cidr} dev {ifname} failed (status {:?}): {}",
                    delete.status.code(),
                    stderr.trim()
                ));
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
fn ip6tables_has_rule(chain: &str, spec: &[&str]) -> bool {
    let output = match Command::new("ip6tables")
        .arg("-C")
        .arg(chain)
        .args(spec)
        .output()
    {
        Ok(output) => output,
        Err(err) => {
            tracing::debug!(
                target: "network",
                chain,
                error = %err,
                "ip6tables check failed (command missing or not permitted)"
            );
            return false;
        }
    };

    if output.status.success() {
        return true;
    }

    // `ip6tables -C` exits with status 1 when the rule does not exist. It also prints a
    // human-oriented message on stderr ("Bad rule ...") which we intentionally suppress by
    // capturing command output.
    match output.status.code() {
        Some(1) => false,
        code => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::debug!(
                target: "network",
                chain,
                exit_code = ?code,
                stderr = %stderr.trim(),
                "ip6tables check returned an unexpected status"
            );
            false
        }
    }
}

#[cfg(target_os = "linux")]
fn ip6tables_insert_rule(chain: &str, spec: &[&str]) -> std::io::Result<()> {
    let output = Command::new("ip6tables")
        .arg("-I")
        .arg(chain)
        .args(spec)
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!(
                "ip6tables returned status {:?}: {}",
                output.status.code(),
                stderr.trim()
            ),
        ))
    }
}
