#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::privileged_networking::{
    PrivilegedTestGuard, command_stdout, create_privileged_network, create_privileged_node,
    delete_privileged_network, force_cleanup_privileged_network_links, link_exists,
    privileged_artifact_dir, privileged_network_interfaces, privileged_test_network,
};
use mantissa::network::types::NetworkStatus;
use std::path::PathBuf;
use uuid::Uuid;

/// Resolve the compiled overlay dataplane artifacts for the privileged eBPF validation lane.
fn privileged_ebpf_artifact_dir() -> Option<PathBuf> {
    privileged_artifact_dir(
        "eBPF overlay",
        &[
            "vxlan_xdp.bpf.o",
            "bridge_xdp.bpf.o",
            "bridge_tc_ingress.bpf.o",
            "bridge_tc_egress.bpf.o",
        ],
    )
}

/// Return whether the detailed `ip link` output reports an attached XDP program.
fn has_xdp_attachment(details: &str) -> bool {
    details.contains("prog/xdp") || details.contains("xdp id")
}

/// Return the bpffs directory where one network pins its load-balancer maps.
fn pinned_lb_map_dir(network_id: Uuid) -> PathBuf {
    PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string())
}

/// Assert that the standard pinned load-balancer maps are reachable for one network.
fn assert_lb_maps_present(network_id: Uuid) {
    let map_dir = pinned_lb_map_dir(network_id);
    for map_name in ["LB_VIPS", "LB_BACKENDS", "LB_FWD", "LB_REV"] {
        let pinned = map_dir.join(map_name);
        assert!(
            pinned.exists(),
            "load-balancer map {map_name} should be pinned at {}",
            pinned.display()
        );
    }
}

local_test!(ebpf_overlay_attaches_programs_and_tears_down_cleanly, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let network = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-test",
            "privileged ebpf overlay integration test network",
            "10.46.0.0/24",
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;

    let [vxlan_ifname, bridge_ifname, host_peer_ifname, _host_ifname] =
        privileged_network_interfaces(network.id);

    let vxlan_details = command_stdout("ip", &["-d", "link", "show", "dev", &vxlan_ifname]);
    assert!(
        has_xdp_attachment(&vxlan_details),
        "vxlan interface should carry the xdp program: {vxlan_details}"
    );

    let bridge_details = command_stdout("ip", &["-d", "link", "show", "dev", &bridge_ifname]);
    assert!(
        has_xdp_attachment(&bridge_details),
        "bridge interface should carry the xdp program: {bridge_details}"
    );

    let ingress_filters = command_stdout(
        "tc",
        &["filter", "show", "dev", &host_peer_ifname, "ingress"],
    );
    assert!(
        ingress_filters.contains("bpf"),
        "host-access ingress qdisc should carry the bridge tc ingress program: {ingress_filters}"
    );

    let egress_filters = command_stdout(
        "tc",
        &["filter", "show", "dev", &host_peer_ifname, "egress"],
    );
    assert!(
        egress_filters.contains("bpf"),
        "host-access egress qdisc should carry the bridge tc egress program: {egress_filters}"
    );

    assert_lb_maps_present(network.id);

    delete_privileged_network(&node, network.id).await;
    force_cleanup_privileged_network_links(network.id).await;
});

local_test!(ebpf_overlay_multiple_networks_attach_and_cleanup_cleanly, {
    let Some(artifact_dir) = privileged_ebpf_artifact_dir() else {
        return;
    };

    let _config = PrivilegedTestGuard::apply(|config| {
        config.network.wireguard.enabled = false;
        config.network.wireguard.manage_firewall = false;
        config.network.bpf.attach = true;
        config.network.bpf.artifact_dir = Some(artifact_dir.display().to_string());
        config.network.nodeport.enabled = false;
        config.network.advertise_addr = Some("127.0.0.1:6578".to_string());
    });

    let node = create_privileged_node().await;
    let network_a = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-test-a",
            "privileged ebpf multi-network test A",
            "10.47.0.0/24",
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;
    let network_b = create_privileged_network(
        &node,
        privileged_test_network(
            "ebpf-test-b",
            "privileged ebpf multi-network test B",
            "10.48.0.0/24",
            1450,
            Vec::new(),
        ),
        NetworkStatus::Ready,
    )
    .await;

    let interfaces_a = privileged_network_interfaces(network_a.id);
    let interfaces_b = privileged_network_interfaces(network_b.id);
    for iface in &interfaces_a {
        assert!(
            !interfaces_b.contains(iface),
            "independent overlay networks should get distinct kernel link names: {interfaces_a:?} vs {interfaces_b:?}"
        );
        assert!(
            link_exists(iface),
            "network A interface should exist after attach: {iface}"
        );
    }
    for iface in &interfaces_b {
        assert!(
            link_exists(iface),
            "network B interface should exist after attach: {iface}"
        );
    }

    assert_lb_maps_present(network_a.id);
    assert_lb_maps_present(network_b.id);

    delete_privileged_network(&node, network_a.id).await;
    force_cleanup_privileged_network_links(network_a.id).await;

    for iface in &interfaces_a {
        assert!(
            !link_exists(iface),
            "deleting network A should remove its kernel links: {iface}"
        );
    }
    for iface in &interfaces_b {
        assert!(
            link_exists(iface),
            "deleting network A should not tear down network B links: {iface}"
        );
    }
    assert_lb_maps_present(network_b.id);

    let [vxlan_ifname, _bridge_ifname, host_peer_ifname, _host_ifname] = interfaces_b.clone();
    let vxlan_details = command_stdout("ip", &["-d", "link", "show", "dev", &vxlan_ifname]);
    assert!(
        has_xdp_attachment(&vxlan_details),
        "network B should keep its xdp attachment after network A is deleted: {vxlan_details}"
    );
    let ingress_filters = command_stdout(
        "tc",
        &["filter", "show", "dev", &host_peer_ifname, "ingress"],
    );
    assert!(
        ingress_filters.contains("bpf"),
        "network B should keep its ingress tc program after network A is deleted: {ingress_filters}"
    );

    delete_privileged_network(&node, network_b.id).await;
    force_cleanup_privileged_network_links(network_b.id).await;
});
