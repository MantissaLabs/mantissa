#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::convergence::wait_until;
use common::privileged_networking::{
    PrivilegedTestGuard, command_stdout, create_privileged_network, create_privileged_node,
    delete_privileged_network, force_cleanup_privileged_network_links, link_exists,
    privileged_headless_config, privileged_network_interfaces, privileged_networking_enabled,
    privileged_test_network,
};
use crdt_store::uuid_key::UuidKey;
use mantissa::network::types::{NetworkPeerState, NetworkStatus};
use mantissa::network::wireguard::{MANTISSA_WIREGUARD_IFNAME, MANTISSA_WIREGUARD_VXLAN_MTU};
use mantissa::runtime::types::RuntimeSupportProfile;
use mantissa::server::headless::{HeadlessKeys, HeadlessNode};
use mantissa::topology::peers::{
    PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue,
};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use uuid::Uuid;

const REMOTE_RPC_ADDR: &str = "192.0.2.10:61234";
const REMOTE_WIREGUARD_PORT: u16 = 61235;

/// Build one minimal peer row that is sufficient for WireGuard underlay planning.
fn test_peer_value(address: &str, wireguard: WireGuardPeerValue) -> PeerValue {
    PeerValue {
        address: address.to_string(),
        hostname: "remote-peer".to_string(),
        noise_static_pub: [1u8; 32],
        signing_pub: [2u8; 32],
        identity_sig: vec![3u8; 64],
        wireguard: Some(wireguard),
        scheduling: PeerSchedulingState::schedulable_default(Uuid::nil()),
        labels: PeerLabelState::default(),
        runtime_support: RuntimeSupportProfile::default(),
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

/// Delete the Mantissa-managed WireGuard link if a previous failed test left it behind.
fn cleanup_wireguard_interface() {
    let _ = Command::new("ip")
        .args(["link", "delete", "dev", MANTISSA_WIREGUARD_IFNAME])
        .output();
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

    cleanup_wireguard_interface();

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = true;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let network = privileged_test_network(
        "wireguard-test",
        "privileged wireguard integration test network",
        "10.45.0.0/24",
        1450,
        Vec::new(),
    );
    node.network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert privileged WireGuard test network");

    let remote_peer_id = Uuid::new_v4();
    upsert_remote_wireguard_peer(&node, network.id, remote_peer_id, false).await;

    assert!(
        wait_until(
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
            }
        )
        .await,
        "network should stay blocked until the scoped WireGuard peer marks itself enabled"
    );

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
    force_cleanup_privileged_network_links(network.id).await;
    cleanup_wireguard_interface();
});

local_test!(wireguard_disabled_keeps_plaintext_overlay_path, {
    if !privileged_networking_enabled("WireGuard") {
        return;
    }

    cleanup_wireguard_interface();

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "wireguard-plaintext",
            "privileged plaintext wireguard test network",
            "10.49.0.0/24",
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
    force_cleanup_privileged_network_links(network.id).await;
    cleanup_wireguard_interface();
});

local_test!(wireguard_restart_reuses_persisted_identity, {
    if !privileged_networking_enabled("WireGuard") {
        return;
    }

    cleanup_wireguard_interface();

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = true;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = false;
        config.network.bpf.artifact_dir = None;
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let temp_dir = tempdir().expect("create tempdir for persisted wireguard test database");
    let db_path = temp_dir.path().join("wireguard-restart.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("create persisted redb"));
    let self_id = Uuid::new_v4();
    let noise_keys = Arc::new(net::noise::NoiseKeys::from_private_bytes([0x71; 32]));
    let signing = ed25519_dalek::SigningKey::from_bytes(&[0x91; 32]);
    let network = privileged_test_network(
        "wireguard-restart",
        "privileged wireguard restart persistence network",
        "10.50.0.0/24",
        1450,
        Vec::new(),
    );

    let mut node = HeadlessNode::new_with(
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

    assert!(
        wait_for_local_wireguard_publication(&node, network.id, Duration::from_secs(60)).await,
        "first start should publish the local WireGuard identity and create mnwg0"
    );

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

    node.stop().await.expect("stop first WireGuard node");
    drop(node);

    let restarted = HeadlessNode::new_with(
        db,
        self_id,
        HeadlessKeys::new(noise_keys, signing),
        privileged_headless_config(),
    )
    .await
    .expect("restart persisted WireGuard node");

    assert!(
        wait_for_local_wireguard_publication(&restarted, network.id, Duration::from_secs(60)).await,
        "restart should reuse the persisted WireGuard identity and recreate mnwg0"
    );

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
    force_cleanup_privileged_network_links(network.id).await;
    cleanup_wireguard_interface();
});
