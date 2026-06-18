use super::{NetworkController, collect_orphaned_network_suffixes, is_managed_overlay_link_name};
use crate::ingress::types::IngressPoolSpecDraft;
use crate::network::types::{
    NetworkDriver, NetworkRealizationPolicy, NetworkSpecDraft, NetworkSpecValue, NetworkStatus,
};
use crate::runtime::types::RuntimeSupportProfile;
use crate::scheduler::placement::{
    PlacementConstraint, PlacementConstraintSelector, PlacementNode, PlacementPolicy,
    PlacementStrategy,
};
use crate::services::types::{
    PublicIngressPolicy, ServiceSpecValue, ServiceStatus, TaskTemplateNetworkRequirement,
    TaskTemplateSpecValue,
};
use crate::topology::peers::{
    NodeReadiness, PeerLabel, PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue,
};
use crate::workload::types::ExecutionSpec;
use anyhow::Context;
use aya::{programs::ProgramError, sys::SyscallError};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

fn make_syscall_error(errno: i32) -> SyscallError {
    SyscallError {
        call: "bpf_link_create",
        io_error: std::io::Error::from_raw_os_error(errno),
    }
}

fn test_network_spec(name: &str, realization: NetworkRealizationPolicy) -> NetworkSpecValue {
    NetworkSpecValue::new_with_realization(
        NetworkSpecDraft {
            name: name.to_string(),
            description: String::new(),
            driver: NetworkDriver::Vxlan,
            subnet_cidr: "10.42.0.0/24".to_string(),
            vni: 0,
            mtu: 0,
            sealed: false,
            bpf_programs: Vec::new(),
        },
        realization,
    )
}

fn test_ingress_pool(name: &str, min_nodes: u16, max_nodes: Option<u16>) -> IngressPoolSpecDraft {
    IngressPoolSpecDraft {
        name: name.to_string(),
        min_nodes,
        max_nodes,
        placement: PlacementPolicy {
            constraints: vec![
                PlacementConstraint::eq(
                    PlacementConstraintSelector::node_label("mantissa.io/ingress"),
                    name,
                )
                .expect("valid ingress label constraint"),
            ],
            strategy: PlacementStrategy::Spread,
        },
        spread_by: None,
    }
}

fn test_placement_node(node_id: Uuid, hostname: &str, ingress_pool: &str) -> PlacementNode {
    PlacementNode::new(
        node_id,
        hostname,
        format!("10.0.0.{}:6578", hostname.trim_start_matches("node-")),
        "linux",
        "x86_64",
        vec![PeerLabel {
            key: "mantissa.io/ingress".to_string(),
            value: ingress_pool.to_string(),
        }],
    )
}

fn test_peer_value(
    peer_id: Uuid,
    schedulable: bool,
    ready: bool,
    active: bool,
) -> (Uuid, PeerValue) {
    let mut scheduling = PeerSchedulingState::schedulable_default(peer_id);
    scheduling.schedulable = schedulable;
    (
        peer_id,
        PeerValue {
            address: format!("inproc://{peer_id}"),
            hostname: format!("node-{peer_id}"),
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: None,
            scheduling,
            readiness: if ready {
                NodeReadiness::ready(peer_id, 1)
            } else {
                NodeReadiness::syncing(peer_id, 1)
            },
            labels: PeerLabelState::default(),
            runtime_support: RuntimeSupportProfile::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: if active {
                PeerMembership::active(1)
            } else {
                PeerMembership::left(2)
            },
        },
    )
}

fn service_with_ingress_pool(network_id: Uuid, pool: &str) -> ServiceSpecValue {
    ServiceSpecValue::new(
        Uuid::new_v4(),
        "ingress-demo",
        "ingress-demo",
        vec![TaskTemplateSpecValue {
            name: "api".to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/api:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 100,
                memory_bytes: 128 * 1024 * 1024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: vec![TaskTemplateNetworkRequirement::new("frontend", network_id)],
                ports: Vec::new(),
                placement: PlacementPolicy::default(),
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: Some(8080),
            public_protocol: None,
            public_ingress: PublicIngressPolicy::IngressPool {
                pool: pool.to_string(),
            },
            placement_preferences: Vec::new(),
            autoscale: None,
        }],
        Vec::new(),
    )
}

#[test]
fn all_nodes_network_specs_have_synthetic_local_realization_demand() {
    let spec = test_network_spec("all-nodes", NetworkRealizationPolicy::AllNodes);
    let local_demand = HashSet::new();

    assert!(
        NetworkController::spec_has_local_realization_demand(&spec, &local_demand),
        "all_nodes specs should be locally demanded even without attachments"
    );
}

#[test]
fn on_demand_network_specs_require_explicit_local_realization_demand() {
    let spec = test_network_spec("on-demand", NetworkRealizationPolicy::OnDemand);
    let empty_demand = HashSet::new();
    assert!(
        !NetworkController::spec_has_local_realization_demand(&spec, &empty_demand),
        "on_demand specs without local references must stay cold"
    );

    let local_demand = HashSet::from([spec.id]);
    assert!(
        NetworkController::spec_has_local_realization_demand(&spec, &local_demand),
        "on_demand specs should realize when local workload or ingress demand exists"
    );
}

#[test]
fn successful_reconcile_promotes_pending_spec_status_to_ready() {
    let mut spec = test_network_spec("pending", NetworkRealizationPolicy::AllNodes);

    assert!(NetworkController::promote_spec_status_after_reconcile(
        &mut spec
    ));
    assert_eq!(spec.status, NetworkStatus::Ready);
}

#[test]
fn successful_reconcile_preserves_non_pending_spec_status() {
    let mut ready = test_network_spec("ready", NetworkRealizationPolicy::AllNodes);
    ready.set_status(NetworkStatus::Ready);
    let mut deleted = test_network_spec("deleted", NetworkRealizationPolicy::AllNodes);
    deleted.mark_deleted();

    assert!(!NetworkController::promote_spec_status_after_reconcile(
        &mut ready
    ));
    assert_eq!(ready.status, NetworkStatus::Ready);
    assert!(!NetworkController::promote_spec_status_after_reconcile(
        &mut deleted
    ));
    assert_eq!(deleted.status, NetworkStatus::Deleted);
}

#[test]
fn all_nodes_wireguard_scope_includes_visible_ready_peers() {
    let local_node_id = Uuid::new_v4();
    let ready_peer = Uuid::new_v4();
    let syncing_peer = Uuid::new_v4();
    let drained_peer = Uuid::new_v4();
    let left_peer = Uuid::new_v4();
    let all_nodes = test_network_spec("all-nodes", NetworkRealizationPolicy::AllNodes);
    let on_demand = test_network_spec("on-demand", NetworkRealizationPolicy::OnDemand);
    let peers = vec![
        test_peer_value(local_node_id, true, true, true),
        test_peer_value(ready_peer, true, true, true),
        test_peer_value(syncing_peer, true, false, true),
        test_peer_value(drained_peer, false, true, true),
        test_peer_value(left_peer, true, true, false),
    ];

    let scope =
        NetworkController::all_nodes_wireguard_scope_peers(&[all_nodes], &peers, local_node_id);

    assert_eq!(scope, HashSet::from([ready_peer, drained_peer]));
    assert!(
        NetworkController::all_nodes_wireguard_scope_peers(&[on_demand], &peers, local_node_id)
            .is_empty()
    );
}

#[test]
fn ingress_pool_network_demand_includes_ready_selected_local_node() {
    let network_id = Uuid::new_v4();
    let local_node = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let service = service_with_ingress_pool(network_id, "public-web");
    let pool = test_ingress_pool("public-web", 1, Some(1))
        .into_value()
        .expect("valid ingress pool");
    let pool_networks = NetworkController::referenced_ingress_pool_networks(&[service]);
    let pools = HashMap::from([("public-web".to_string(), pool)]);
    let candidates = vec![
        test_placement_node(local_node, "node-1", "public-web"),
        test_placement_node(remote_node, "node-2", "public-web"),
    ];

    let demand = NetworkController::collect_ingress_pool_network_demand(
        &pool_networks,
        &pools,
        &candidates,
        local_node,
    );

    assert_eq!(
        demand,
        HashSet::from([network_id]),
        "selected ready ingress pool nodes should demand the service network"
    );
}

#[test]
fn ingress_pool_network_demand_excludes_unselected_local_node() {
    let network_id = Uuid::new_v4();
    let local_node = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let service = service_with_ingress_pool(network_id, "public-web");
    let pool = test_ingress_pool("public-web", 1, Some(1))
        .into_value()
        .expect("valid ingress pool");
    let pool_networks = NetworkController::referenced_ingress_pool_networks(&[service]);
    let pools = HashMap::from([("public-web".to_string(), pool)]);
    let candidates = vec![
        test_placement_node(remote_node, "node-1", "public-web"),
        test_placement_node(local_node, "node-2", "public-web"),
    ];

    let demand = NetworkController::collect_ingress_pool_network_demand(
        &pool_networks,
        &pools,
        &candidates,
        local_node,
    );

    assert!(
        demand.is_empty(),
        "unselected ingress pool nodes should not realize the service network"
    );
}

#[test]
fn ingress_pool_network_demand_waits_for_pool_readiness() {
    let network_id = Uuid::new_v4();
    let local_node = Uuid::new_v4();
    let service = service_with_ingress_pool(network_id, "public-web");
    let pool = test_ingress_pool("public-web", 2, None)
        .into_value()
        .expect("valid ingress pool");
    let pool_networks = NetworkController::referenced_ingress_pool_networks(&[service]);
    let pools = HashMap::from([("public-web".to_string(), pool)]);
    let candidates = vec![test_placement_node(local_node, "node-1", "public-web")];

    let demand = NetworkController::collect_ingress_pool_network_demand(
        &pool_networks,
        &pools,
        &candidates,
        local_node,
    );

    assert!(
        demand.is_empty(),
        "underfilled ingress pools should not realize service networks"
    );
}

#[test]
fn ingress_pool_network_demand_ignores_stopped_services() {
    let network_id = Uuid::new_v4();
    let mut service = service_with_ingress_pool(network_id, "public-web");
    service.set_status(ServiceStatus::Stopped);

    let pool_networks = NetworkController::referenced_ingress_pool_networks(&[service]);

    assert!(
        pool_networks.is_empty(),
        "stopped services should not keep ingress pool networks realized"
    );
}

#[test]
fn detects_syscall_conflict_directly() {
    let err = Err::<(), _>(make_syscall_error(libc::EEXIST))
        .context("attach xdp")
        .unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected syscall conflict to be detected"
    );
}

#[test]
fn detects_syscall_conflict_wrapped_in_program_error() {
    let program_err: ProgramError = make_syscall_error(libc::EEXIST).into();
    let err = Err::<(), _>(program_err).context("attach xdp").unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected program error conflict to be detected"
    );
}

#[test]
fn detects_xdp_busy_conflict_directly() {
    let err = Err::<(), _>(make_syscall_error(libc::EBUSY))
        .context("attach xdp")
        .unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected xdp busy conflict to be detected"
    );
}

#[test]
fn detects_xdp_busy_conflict_wrapped_in_program_error() {
    let program_err: ProgramError = make_syscall_error(libc::EBUSY).into();
    let err = Err::<(), _>(program_err).context("attach xdp").unwrap_err();
    assert!(
        NetworkController::is_bpf_link_conflict(&err),
        "expected wrapped xdp busy conflict to be detected"
    );
}

#[test]
fn collects_only_orphaned_managed_network_suffixes() {
    let live =
        Uuid::parse_str("21523dac-bdaa-6cf5-359f-57139c6464a8").expect("valid live network id");
    let desired = HashSet::from([live]);
    let suffixes = collect_orphaned_network_suffixes(
        &desired,
        [
            "mnhost-21523dac",
            "mnhp-21523dac",
            "mvx-21523dac",
            "mnt-br-21523dac",
            "mnhost-b3d339cd",
            "mnt-br-b3d339cd",
            "mvx-b3d339cd",
            "mnhp-b3d339cd",
            "docker0",
            "mnhost-nothexzz",
        ],
    );

    assert_eq!(
        suffixes,
        vec!["b3d339cd".to_string()],
        "only managed suffixes that are absent from desired network ids should be collected"
    );
}

#[test]
fn identifies_all_managed_overlay_link_names() {
    assert!(is_managed_overlay_link_name("mvx-21523dac"));
    assert!(is_managed_overlay_link_name("mnt-br-21523dac"));
    assert!(is_managed_overlay_link_name("mnhost-21523dac"));
    assert!(is_managed_overlay_link_name("mnhp-21523dac"));
    assert!(is_managed_overlay_link_name("mnth-21523dac"));
    assert!(is_managed_overlay_link_name("mntc-21523dac"));
    assert!(!is_managed_overlay_link_name("eth0"));
    assert!(!is_managed_overlay_link_name("docker0"));
}
