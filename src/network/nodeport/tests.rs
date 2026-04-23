#[cfg(target_os = "linux")]
use super::platform::{
    NodePortPublishedMapping, NodePortReturnKey, NodePortReturnKey6, NodePortSelector,
    nodeport_return_keys, stale_nodeport_mappings, stale_overlay_ifindices,
};
use super::{
    NODEPORT_FLOW_CAPACITY, NODEPORT_HOST_CAPACITY, NODEPORT_VIP_CAPACITY, NodePortFlowDiagnostics,
    NodePortIdentitySource, NodePortMapCapacities, NodePortPacketCounters, NodePortProtocol,
    NodePortRuntimeState, NodePortStatus, configured_node_ip_from_sources,
    configured_node_ip_source, estimated_flow_evictions, nodeport_capacity_error,
    projected_active_networks_after_sync, resolve_advertise_ip,
};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[test]
#[cfg(target_os = "linux")]
fn stale_nodeport_mappings_include_removed_and_changed_selectors() {
    let selector_unchanged = NodePortSelector::new(18080, NodePortProtocol::Tcp);
    let selector_changed = NodePortSelector::new(18081, NodePortProtocol::Udp);
    let selector_removed = NodePortSelector::new(18082, NodePortProtocol::Udp);
    let unchanged = NodePortPublishedMapping::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10)),
        8080,
        IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
        42,
    );
    let changed_old = NodePortPublishedMapping::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 11)),
        5353,
        IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
        42,
    );
    let changed_new = NodePortPublishedMapping::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 12)),
        5353,
        IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
        42,
    );
    let removed = NodePortPublishedMapping::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 13)),
        9000,
        IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
        42,
    );

    let previous = HashMap::from([
        (selector_unchanged, unchanged),
        (selector_changed, changed_old),
        (selector_removed, removed),
    ]);
    let desired = HashMap::from([
        (selector_unchanged, unchanged),
        (selector_changed, changed_new),
    ]);

    let stale = stale_nodeport_mappings(&previous, &desired);
    assert_eq!(stale.len(), 2);
    assert!(stale.contains(&(selector_changed, changed_old)));
    assert!(stale.contains(&(selector_removed, removed)));
}

#[test]
#[cfg(target_os = "linux")]
fn stale_overlay_ifindices_only_report_detached_indices() {
    let selector_a = NodePortSelector::new(18080, NodePortProtocol::Tcp);
    let selector_b = NodePortSelector::new(18081, NodePortProtocol::Udp);
    let previous = HashMap::from([
        (
            selector_a,
            NodePortPublishedMapping::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10)),
                8080,
                IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
                41,
            ),
        ),
        (
            selector_b,
            NodePortPublishedMapping::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, 11)),
                5353,
                IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
                42,
            ),
        ),
    ]);
    let desired = HashMap::from([(
        selector_b,
        NodePortPublishedMapping::new(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 11)),
            5353,
            IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
            42,
        ),
    )]);

    assert_eq!(stale_overlay_ifindices(&previous, &desired), vec![41]);
}

#[test]
#[cfg(target_os = "linux")]
fn nodeport_return_keys_dedupe_shared_vip_targets() {
    let tcp_a = NodePortSelector::new(18080, NodePortProtocol::Tcp);
    let tcp_b = NodePortSelector::new(18081, NodePortProtocol::Tcp);
    let udp = NodePortSelector::new(18082, NodePortProtocol::Udp);
    let ipv6 = NodePortSelector::new(18083, NodePortProtocol::Tcp);
    let shared_ipv4 = NodePortPublishedMapping::new(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 10)),
        8080,
        IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)),
        42,
    );
    let shared_ipv6 = NodePortPublishedMapping::new(
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        8080,
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        42,
    );

    let mappings = HashMap::from([
        (tcp_a, shared_ipv4),
        (tcp_b, shared_ipv4),
        (udp, shared_ipv4),
        (ipv6, shared_ipv6),
    ]);

    let (ipv4, ipv6_keys) = nodeport_return_keys(&mappings);
    assert_eq!(
        ipv4,
        std::collections::HashSet::from([
            NodePortReturnKey {
                vip: u32::from_ne_bytes([10, 0, 0, 10]),
                vip_port: 8080u16.to_be(),
                proto: NodePortProtocol::Tcp.number(),
                _pad: 0,
            },
            NodePortReturnKey {
                vip: u32::from_ne_bytes([10, 0, 0, 10]),
                vip_port: 8080u16.to_be(),
                proto: NodePortProtocol::Udp.number(),
                _pad: 0,
            },
        ])
    );
    assert_eq!(
        ipv6_keys,
        std::collections::HashSet::from([NodePortReturnKey6 {
            vip: Ipv6Addr::LOCALHOST.octets(),
            vip_port: 8080u16.to_be(),
            proto: NodePortProtocol::Tcp.number(),
            _pad: 0,
        }])
    );
}
#[test]
fn resolve_advertise_ipv4_accepts_literal_ipv4_socket() {
    assert_eq!(
        resolve_advertise_ip("192.168.10.4:6578"),
        Some(IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)))
    );
}

#[test]
fn resolve_advertise_ipv4_ignores_ipv6_only_socket() {
    assert_eq!(
        resolve_advertise_ip("[::1]:6578"),
        Some(IpAddr::V6(Ipv6Addr::LOCALHOST))
    );
}

#[test]
fn configured_node_ip_prefers_explicit_override() {
    let configured = Some(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)));
    let advertise = Some("192.168.10.4:6578");

    assert_eq!(
        configured_node_ip_from_sources(configured, advertise),
        Some(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)))
    );
}

#[test]
fn configured_node_ip_uses_advertise_addr_when_override_absent() {
    assert_eq!(
        configured_node_ip_from_sources(None, Some("192.168.10.4:6578")),
        Some(IpAddr::V4(Ipv4Addr::new(192, 168, 10, 4)))
    );
}

#[test]
fn configured_node_ip_source_reports_explicit_override() {
    assert_eq!(
        configured_node_ip_source(
            Some(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40))),
            Some("192.168.10.4:6578")
        ),
        Some(NodePortIdentitySource::NodePortIp)
    );
}

#[test]
fn configured_node_ip_source_reports_advertise_addr_fallback() {
    assert_eq!(
        configured_node_ip_source(None, Some("192.168.10.4:6578")),
        Some(NodePortIdentitySource::AdvertiseAddr)
    );
}

#[test]
fn nodeport_status_tracks_active_counts() {
    let status = NodePortStatus {
        desired_enabled: true,
        state: NodePortRuntimeState::Pending,
        source_mode: crate::config::NodePortSourceMode::SnatHostAccess,
        identity_source: Some(NodePortIdentitySource::NodePortIp),
        resolved_iface: Some("eth0".to_string()),
        resolved_node_ip: Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4))),
        active_networks: 2,
        active_ports: 3,
        active_host_networks: 2,
        vip_capacity: NODEPORT_VIP_CAPACITY,
        host_capacity: NODEPORT_HOST_CAPACITY,
        flow_capacity: NODEPORT_FLOW_CAPACITY,
        ingress_stats: Some(NodePortPacketCounters {
            packets: 10,
            bytes: 2048,
            drops: 1,
        }),
        ingress_drop_reasons: None,
        egress_stats: None,
        flow_diagnostics: Some(NodePortFlowDiagnostics {
            ipv4_flow_pairs: 2,
            ipv6_flow_pairs: 1,
            flow_creates: 5,
            flow_clears: 1,
            estimated_flow_evictions: 1,
            reverse_misses: 2,
            invalid_conntrack_transitions: 1,
            return_path_bypass_packets: 3,
        }),
        last_error: None,
        stats_error: None,
    };

    assert_eq!(status.active_networks, 2);
    assert_eq!(status.active_ports, 3);
    assert_eq!(status.active_host_networks, 2);
    assert_eq!(
        status.source_mode,
        crate::config::NodePortSourceMode::SnatHostAccess
    );
    assert_eq!(
        status.identity_source,
        Some(NodePortIdentitySource::NodePortIp)
    );
    assert_eq!(status.vip_capacity, NODEPORT_VIP_CAPACITY);
    assert_eq!(
        status.flow_diagnostics,
        Some(NodePortFlowDiagnostics {
            ipv4_flow_pairs: 2,
            ipv6_flow_pairs: 1,
            flow_creates: 5,
            flow_clears: 1,
            estimated_flow_evictions: 1,
            reverse_misses: 2,
            invalid_conntrack_transitions: 1,
            return_path_bypass_packets: 3,
        })
    );
    assert_eq!(
        status.ingress_stats,
        Some(NodePortPacketCounters {
            packets: 10,
            bytes: 2048,
            drops: 1,
        })
    );
    assert_eq!(status.state, NodePortRuntimeState::Pending);
}

#[test]
fn projected_active_networks_adds_new_public_network() {
    assert_eq!(projected_active_networks_after_sync(3, false, true), 4);
}

#[test]
fn projected_active_networks_removes_empty_public_network() {
    assert_eq!(projected_active_networks_after_sync(3, true, false), 2);
}

#[test]
fn estimated_flow_evictions_tracks_lru_pressure() {
    assert_eq!(estimated_flow_evictions(2, 0, 1, 0), 1);
    assert_eq!(estimated_flow_evictions(5, 3, 1, 1), 0);
}

#[test]
fn nodeport_capacity_error_reports_vip_limit() {
    let error = nodeport_capacity_error(
        NODEPORT_VIP_CAPACITY + 1,
        1,
        NodePortMapCapacities {
            vip: NODEPORT_VIP_CAPACITY,
            host: NODEPORT_HOST_CAPACITY,
            flow: NODEPORT_FLOW_CAPACITY,
        },
    )
    .expect("expected vip capacity error");
    assert!(error.contains("VIP capacity exceeded"));
}

#[test]
fn nodeport_capacity_error_reports_host_limit() {
    let error = nodeport_capacity_error(
        1,
        NODEPORT_HOST_CAPACITY + 1,
        NodePortMapCapacities {
            vip: NODEPORT_VIP_CAPACITY,
            host: NODEPORT_HOST_CAPACITY,
            flow: NODEPORT_FLOW_CAPACITY,
        },
    )
    .expect("expected host capacity error");
    assert!(error.contains("host-access capacity exceeded"));
}
