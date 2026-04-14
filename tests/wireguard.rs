#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::convergence::wait_until;
use common::privileged_networking::{
    PrivilegedTestGuard, command_stdout, create_privileged_network, create_privileged_node,
    delete_privileged_network, link_exists, privileged_headless_config,
    privileged_network_interfaces, privileged_networking_enabled, privileged_test_network,
    privileged_test_subnet,
};
use crdt_store::uuid_key::UuidKey;
use futures::TryStreamExt;
use mantissa::network::types::{NetworkPeerState, NetworkStatus};
use mantissa::network::wireguard::{MANTISSA_WIREGUARD_IFNAME, MANTISSA_WIREGUARD_VXLAN_MTU};
use mantissa::runtime::types::RuntimeSupportProfile;
use mantissa::server::headless::{HeadlessKeys, HeadlessNode};
use mantissa::topology::peers::{
    PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue,
};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

const REMOTE_RPC_ADDR: &str = "192.0.2.10:61234";
const REMOTE_WIREGUARD_PORT: u16 = 61235;
const VXLAN_UDP_PORT: u16 = 4789;

/// Restores the host firewall state after the WireGuard manage-firewall smoke test completes.
struct WireGuardFirewallRestoreGuard {
    input_rule: String,
    output_rule: String,
    input_was_present: bool,
    output_was_present: bool,
}

impl Drop for WireGuardFirewallRestoreGuard {
    /// Restore the original ip6tables rule presence snapshot for the managed WireGuard VXLAN rules.
    fn drop(&mut self) {
        set_ip6tables_rule_presence("INPUT", &self.input_rule, self.input_was_present);
        set_ip6tables_rule_presence("OUTPUT", &self.output_rule, self.output_was_present);
    }
}

/// Build one minimal peer row that is sufficient for WireGuard underlay planning.
fn test_peer_value(address: &str, wireguard: WireGuardPeerValue) -> PeerValue {
    PeerValue {
        address: address.to_string(),
        hostname: "remote-peer".to_string(),
        platform_os: "linux".to_string(),
        platform_arch: "amd64".to_string(),
        noise_static_pub: [1u8; 32],
        signing_pub: [2u8; 32],
        identity_sig: vec![3u8; 64],
        wireguard: Some(wireguard),
        scheduling: PeerSchedulingState::schedulable_default(Uuid::nil()),
        labels: PeerLabelState::default(),
        runtime_support: RuntimeSupportProfile::default(),
        root_schema: mantissa::cluster::RootSchemaInfo::default(),
        membership: PeerMembership::active(1),
    }
}

/// Upsert one synthetic remote peer advertisement and trigger a local network reconcile.
async fn upsert_remote_wireguard_peer(
    node: &HeadlessNode,
    network_id: Uuid,
    peer_id: Uuid,
    enabled: bool,
) {
    node.peers
        .upsert(
            &UuidKey::from(peer_id),
            test_peer_value(
                REMOTE_RPC_ADDR,
                WireGuardPeerValue {
                    public_key: [7u8; 32],
                    port: REMOTE_WIREGUARD_PORT,
                    enabled,
                },
            ),
        )
        .await
        .expect("upsert synthetic remote WireGuard peer");
    node.network_controller
        .schedule_spec_change(network_id)
        .await;
}

/// Best-effort delete one kernel link by name through rtnetlink.
async fn delete_link_if_exists(ifname: &str) {
    let Ok((conn, handle, _)) = rtnetlink::new_connection() else {
        return;
    };
    tokio::spawn(conn);

    let Ok(Some(link)) = handle
        .link()
        .get()
        .match_name(ifname.to_string())
        .execute()
        .try_next()
        .await
    else {
        return;
    };

    let _ = handle.link().del(link.header.index).execute().await;
}

/// Delete the Mantissa-managed WireGuard link if a previous failed test left it behind.
async fn cleanup_wireguard_interface() {
    delete_link_if_exists(MANTISSA_WIREGUARD_IFNAME).await;
}

/// Require that the current host can create kernel WireGuard interfaces.
///
/// When the privileged networking suite is explicitly enabled, WireGuard being unavailable is a
/// real test-environment failure and must not silently degrade into an `ok` result.
async fn require_wireguard_kernel_support() {
    // Linux interface names are capped at 15 visible bytes, so keep the probe short.
    const PROBE_IFNAME: &str = "mnwg-probe";

    delete_link_if_exists(PROBE_IFNAME).await;

    let create = tokio::task::spawn_blocking(|| {
        use defguard_wireguard_rs::{Kernel, WGApi, WireguardInterfaceApi};

        let mut wgapi = WGApi::<Kernel>::new(PROBE_IFNAME).map_err(|err| err.to_string())?;
        wgapi.create_interface().map_err(|err| err.to_string())
    })
    .await
    .expect("probe WireGuard kernel support task should not panic");
    if let Err(err) = create {
        panic!(
            "privileged WireGuard tests require kernel WireGuard interface creation via \
             defguard_wireguard_rs, but the probe failed: {err}"
        );
    }

    delete_link_if_exists(PROBE_IFNAME).await;
}

/// Build the INPUT chain rule Mantissa manages for VXLAN-over-WireGuard traffic.
fn wireguard_input_firewall_rule(ifname: &str) -> String {
    format!("-i {ifname} -p udp --dport {VXLAN_UDP_PORT} -j ACCEPT")
}

/// Build the OUTPUT chain rule Mantissa manages for VXLAN-over-WireGuard traffic.
fn wireguard_output_firewall_rule(ifname: &str) -> String {
    format!("-o {ifname} -p udp --sport {VXLAN_UDP_PORT} -j ACCEPT")
}

/// Return whether one ip6tables filter rule currently exists.
fn ip6tables_rule_exists(chain: &str, rule: &str) -> bool {
    let client = iptables::new(true)
        .unwrap_or_else(|err| panic!("create ip6tables client for {chain} '{rule}': {err}"));
    client
        .exists("filter", chain, rule)
        .unwrap_or_else(|err| panic!("check ip6tables {chain} rule '{rule}': {err}"))
}

/// Ensure one ip6tables filter rule is either present or absent so tests can restore host state.
fn set_ip6tables_rule_presence(chain: &str, rule: &str, present: bool) {
    let client = iptables::new(true)
        .unwrap_or_else(|err| panic!("create ip6tables client for {chain} '{rule}': {err}"));
    let exists = client
        .exists("filter", chain, rule)
        .unwrap_or_else(|err| panic!("check ip6tables {chain} rule '{rule}': {err}"));
    if present && !exists {
        client
            .insert("filter", chain, rule, 1)
            .unwrap_or_else(|err| panic!("insert ip6tables {chain} rule '{rule}': {err}"));
    } else if !present && exists {
        client
            .delete("filter", chain, rule)
            .unwrap_or_else(|err| panic!("delete ip6tables {chain} rule '{rule}': {err}"));
    }
}

/// Wait until the local node publishes its WireGuard advertisement and the managed link exists.
async fn wait_for_local_wireguard_publication(
    node: &HeadlessNode,
    network_id: Uuid,
    timeout: Duration,
) -> bool {
    wait_until(timeout, Duration::from_millis(100), || async {
        matches!(
            node.network_registry.get_spec(network_id),
            Ok(Some(spec)) if spec.status == NetworkStatus::Ready
        ) && node.registry.peer_wireguard(node.id).is_some()
            && link_exists(MANTISSA_WIREGUARD_IFNAME)
    })
    .await
}

local_test!(wireguard_scoped_peer_gate_blocks_until_peer_enabled, {
    if !privileged_networking_enabled("WireGuard") {
        return;
    }

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = true;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    require_wireguard_kernel_support().await;
    cleanup_wireguard_interface().await;

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = privileged_test_network(
        "wireguard-test",
        "privileged wireguard integration test network",
        &subnet,
        1450,
        Vec::new(),
    );
    node.network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert privileged WireGuard test network");

    let remote_peer_id = Uuid::new_v4();
    upsert_remote_wireguard_peer(&node, network.id, remote_peer_id, false).await;

    let blocked = wait_until(
        Duration::from_secs(30),
        Duration::from_millis(100),
        || async {
            matches!(
                node.network_registry.get_peer_state(network.id, node.id),
                Ok(Some(state))
                    if state.state == NetworkPeerState::Error
                        && state
                            .error
                            .as_deref()
                            .is_some_and(|error| error.contains("wireguard underlay required"))
            )
        },
    )
    .await;
    if !blocked {
        let spec = node
            .network_registry
            .get_spec(network.id)
            .expect("load wireguard network after blocked wait");
        let peer_state = node
            .network_registry
            .get_peer_state(network.id, node.id)
            .expect("load local peer state after blocked wait");
        let visible_peers = node
            .registry
            .peer_values_snapshot()
            .expect("load visible peers after blocked wait");
        panic!(
            "network should stay blocked until the scoped WireGuard peer marks itself enabled; spec={spec:?}; peer_state={peer_state:?}; visible_peers={visible_peers:?}"
        );
    }

    let blocked_spec = node
        .network_registry
        .get_spec(network.id)
        .expect("load blocked network spec")
        .expect("blocked network spec should remain present");
    assert_eq!(
        blocked_spec.status,
        NetworkStatus::Pending,
        "blocked WireGuard network should not reach ready while the scoped peer is disabled"
    );

    upsert_remote_wireguard_peer(&node, network.id, remote_peer_id, true).await;

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                matches!(
                    node.network_registry.get_spec(network.id),
                    Ok(Some(spec)) if spec.status == NetworkStatus::Ready
                ) && matches!(
                    node.network_registry.get_peer_state(network.id, node.id),
                    Ok(Some(state)) if state.state == NetworkPeerState::Ready
                )
            }
        )
        .await,
        "network should become ready once the scoped WireGuard peer is enabled"
    );

    let local_wireguard = node
        .registry
        .peer_wireguard(node.id)
        .expect("local peer should publish its WireGuard state");
    assert!(
        local_wireguard.enabled,
        "local peer should only advertise enabled WireGuard state after configuring mnwg0"
    );

    let local_tunnel = net::wireguard::wireguard_tunnel_ipv6(node.id);
    let remote_tunnel = net::wireguard::wireguard_tunnel_ipv6(remote_peer_id);
    let [vxlan_ifname, ..] = privileged_network_interfaces(network.id);

    let underlay_details = command_stdout(
        "ip",
        &["-d", "link", "show", "dev", MANTISSA_WIREGUARD_IFNAME],
    );
    assert!(
        underlay_details.contains("wireguard"),
        "mnwg0 should be provisioned as a WireGuard link: {underlay_details}"
    );

    let underlay_addr = command_stdout(
        "ip",
        &["-6", "addr", "show", "dev", MANTISSA_WIREGUARD_IFNAME],
    );
    assert!(
        underlay_addr.contains(&local_tunnel.to_string()),
        "mnwg0 should carry the deterministic local tunnel address {local_tunnel}: {underlay_addr}"
    );

    let underlay_routes = command_stdout(
        "ip",
        &["-6", "route", "show", "dev", MANTISSA_WIREGUARD_IFNAME],
    );
    assert!(
        underlay_routes.contains(&remote_tunnel.to_string()),
        "mnwg0 should keep a route for the scoped remote peer tunnel {remote_tunnel}: {underlay_routes}"
    );

    let vxlan_details = command_stdout("ip", &["-d", "link", "show", "dev", &vxlan_ifname]);
    assert!(
        vxlan_details.contains(&format!("dev {MANTISSA_WIREGUARD_IFNAME}")),
        "vxlan device should switch onto the WireGuard underlay once the scoped peer is ready: {vxlan_details}"
    );
    assert!(
        vxlan_details.contains(&format!("local {local_tunnel}")),
        "vxlan device should use the local WireGuard tunnel address as its underlay source: {vxlan_details}"
    );

    let vxlan_link = command_stdout("ip", &["link", "show", "dev", &vxlan_ifname]);
    assert!(
        vxlan_link.contains(&format!("mtu {}", MANTISSA_WIREGUARD_VXLAN_MTU)),
        "vxlan MTU should be clamped for WireGuard encapsulation: {vxlan_link}"
    );

    delete_privileged_network(&node, network.id).await;
    cleanup_wireguard_interface().await;
});

local_test!(wireguard_disabled_keeps_plaintext_overlay_path, {
    if !privileged_networking_enabled("WireGuard") {
        return;
    }

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    require_wireguard_kernel_support().await;
    cleanup_wireguard_interface().await;

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "wireguard-plaintext",
            "privileged plaintext wireguard test network",
            &subnet,
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;

    let [vxlan_ifname, ..] = privileged_network_interfaces(network.id);
    let vxlan_details = command_stdout("ip", &["-d", "link", "show", "dev", &vxlan_ifname]);
    let vxlan_link = command_stdout("ip", &["link", "show", "dev", &vxlan_ifname]);

    assert!(
        !link_exists(MANTISSA_WIREGUARD_IFNAME),
        "wireguard disabled should keep mnwg0 absent on the host"
    );
    assert!(
        node.registry.peer_wireguard(node.id).is_none(),
        "wireguard disabled should not publish a local WireGuard advertisement"
    );
    assert!(
        !vxlan_details.contains(&format!("dev {MANTISSA_WIREGUARD_IFNAME}")),
        "vxlan should stay on the plaintext underlay when wireguard is disabled: {vxlan_details}"
    );
    assert!(
        vxlan_link.contains("mtu 1450"),
        "plaintext vxlan should keep the configured MTU instead of the WireGuard clamp: {vxlan_link}"
    );

    delete_privileged_network(&node, network.id).await;
    cleanup_wireguard_interface().await;
});

local_test!(wireguard_restart_reuses_persisted_identity, {
    if !privileged_networking_enabled("WireGuard") {
        return;
    }

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = true;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    require_wireguard_kernel_support().await;
    cleanup_wireguard_interface().await;

    let temp_dir = tempdir().expect("create tempdir for persisted wireguard test database");
    let db_path = temp_dir.path().join("wireguard-restart.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create persisted redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(net::noise::NoiseKeys::from_private_bytes([0x71; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x91; 32]);
    let network = privileged_test_network(
        "wireguard-restart",
        "privileged wireguard restart persistence network",
        &privileged_test_subnet(),
        1450,
        Vec::new(),
    );

    let node = HeadlessNode::new_with(
        db.clone(),
        self_id,
        HeadlessKeys::new(noise_keys.clone(), signing.clone()),
        privileged_headless_config(),
    )
    .await
    .expect("start persisted WireGuard node");

    node.network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert persisted WireGuard network");
    node.network_controller
        .schedule_spec_change(network.id)
        .await;

    let published_first =
        wait_for_local_wireguard_publication(&node, network.id, Duration::from_secs(60)).await;
    if !published_first {
        let spec = node
            .network_registry
            .get_spec(network.id)
            .expect("load persisted wireguard network after first publication wait");
        let peer_state = node
            .network_registry
            .get_peer_state(network.id, node.id)
            .expect("load persisted wireguard peer state after first publication wait");
        let published_self = node.registry.peer_wireguard(node.id);
        panic!(
            "first start should publish the local WireGuard identity and create mnwg0; spec={spec:?}; peer_state={peer_state:?}; published_self={published_self:?}; mnwg0_exists={}",
            link_exists(MANTISSA_WIREGUARD_IFNAME)
        );
    }

    let published_before = node
        .registry
        .peer_wireguard(node.id)
        .expect("first start should publish local WireGuard metadata");
    let key_path =
        net::wireguard::resolve_wireguard_key_path().expect("resolve persisted WireGuard key path");
    let port_path = net::wireguard::resolve_wireguard_port_path()
        .expect("resolve persisted WireGuard port path");
    let key_bytes_before =
        std::fs::read(&key_path).expect("read persisted WireGuard key material before restart");
    let port_before =
        std::fs::read_to_string(&port_path).expect("read persisted WireGuard port before restart");

    node.shutdown()
        .await
        .expect("shut down first WireGuard node");

    let restarted = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        privileged_headless_config(),
    )
    .await
    .expect("restart persisted WireGuard node");

    let published_restarted =
        wait_for_local_wireguard_publication(&restarted, network.id, Duration::from_secs(60)).await;
    if !published_restarted {
        let spec = restarted
            .network_registry
            .get_spec(network.id)
            .expect("load persisted wireguard network after restart publication wait");
        let peer_state = restarted
            .network_registry
            .get_peer_state(network.id, restarted.id)
            .expect("load persisted wireguard peer state after restart publication wait");
        let published_self = restarted.registry.peer_wireguard(restarted.id);
        panic!(
            "restart should reuse the persisted WireGuard identity and recreate mnwg0; spec={spec:?}; peer_state={peer_state:?}; published_self={published_self:?}; mnwg0_exists={}",
            link_exists(MANTISSA_WIREGUARD_IFNAME)
        );
    }

    let published_after = restarted
        .registry
        .peer_wireguard(restarted.id)
        .expect("restart should republish local WireGuard metadata");
    let key_bytes_after =
        std::fs::read(&key_path).expect("read persisted WireGuard key material after restart");
    let port_after =
        std::fs::read_to_string(&port_path).expect("read persisted WireGuard port after restart");

    assert_eq!(
        published_after, published_before,
        "restarting with the same state dir and node identity should reuse the advertised WireGuard endpoint"
    );
    assert_eq!(
        key_bytes_after, key_bytes_before,
        "wireguard key material should persist unchanged across restart"
    );
    assert_eq!(
        port_after, port_before,
        "wireguard listen port should persist unchanged across restart"
    );

    let tunnel_addr = net::wireguard::wireguard_tunnel_ipv6(self_id);
    let underlay_addr = command_stdout(
        "ip",
        &["-6", "addr", "show", "dev", MANTISSA_WIREGUARD_IFNAME],
    );
    assert!(
        underlay_addr.contains(&tunnel_addr.to_string()),
        "restarted mnwg0 should keep the deterministic local tunnel address {tunnel_addr}: {underlay_addr}"
    );

    delete_privileged_network(&restarted, network.id).await;
    cleanup_wireguard_interface().await;
});

local_test!(wireguard_manage_firewall_installs_vxlan_rules, {
    if !privileged_networking_enabled("WireGuard") {
        return;
    }

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = true;
        config.network.wireguard.manage_firewall = true;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });
    require_wireguard_kernel_support().await;
    cleanup_wireguard_interface().await;

    let input_rule = wireguard_input_firewall_rule(MANTISSA_WIREGUARD_IFNAME);
    let output_rule = wireguard_output_firewall_rule(MANTISSA_WIREGUARD_IFNAME);
    let input_was_present = ip6tables_rule_exists("INPUT", &input_rule);
    let output_was_present = ip6tables_rule_exists("OUTPUT", &output_rule);
    let _firewall_restore = WireGuardFirewallRestoreGuard {
        input_rule: input_rule.clone(),
        output_rule: output_rule.clone(),
        input_was_present,
        output_was_present,
    };

    // Remove any pre-existing Mantissa-specific rules first so this test proves the managed
    // firewall path installs them when WireGuard becomes ready.
    set_ip6tables_rule_presence("INPUT", &input_rule, false);
    set_ip6tables_rule_presence("OUTPUT", &output_rule, false);

    let node = create_privileged_node().await;
    let subnet = privileged_test_subnet();
    let network = privileged_test_network(
        "wireguard-firewall",
        "privileged wireguard firewall smoke test network",
        &subnet,
        1450,
        Vec::new(),
    );
    node.network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert privileged WireGuard firewall network");

    let remote_peer_id = Uuid::new_v4();
    upsert_remote_wireguard_peer(&node, network.id, remote_peer_id, true).await;

    assert!(
        wait_until(
            Duration::from_secs(60),
            Duration::from_millis(100),
            || async {
                matches!(
                    node.network_registry.get_spec(network.id),
                    Ok(Some(spec)) if spec.status == NetworkStatus::Ready
                ) && node.registry.peer_wireguard(node.id).is_some()
                    && link_exists(MANTISSA_WIREGUARD_IFNAME)
                    && ip6tables_rule_exists("INPUT", &input_rule)
                    && ip6tables_rule_exists("OUTPUT", &output_rule)
            }
        )
        .await,
        "wireguard manage_firewall should install the vxlan INPUT and OUTPUT allow rules"
    );

    delete_privileged_network(&node, network.id).await;
    cleanup_wireguard_interface().await;
});
