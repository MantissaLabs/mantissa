#![cfg(target_os = "linux")]

#[macro_use]
mod common;

use common::privileged_networking::{
    PrivilegedTestGuard, command_stdout, create_privileged_network, create_privileged_node,
    delete_privileged_network, force_cleanup_privileged_network_links, privileged_artifact_dir,
    privileged_network_interfaces, privileged_test_network,
};
use mantissa::network::types::NetworkStatus;
use std::path::PathBuf;

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

    let map_dir = PathBuf::from("/sys/fs/bpf/mantissa").join(network.id.to_string());
    for map_name in ["LB_VIPS", "LB_BACKENDS", "LB_FWD", "LB_REV"] {
        let pinned = map_dir.join(map_name);
        assert!(
            pinned.exists(),
            "load-balancer map {map_name} should be pinned at {}",
            pinned.display()
        );
    }

    delete_privileged_network(&node, network.id).await;
    force_cleanup_privileged_network_links(network.id).await;
});
