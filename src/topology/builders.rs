use crate::cluster::operations::SplitNodeCandidate;
use crate::cluster::{ClusterId, ClusterViewId, RootSchemaInfo};
use crate::node::id::set_node_id;
use crate::runtime::types::RuntimeSupportProfile;
use crate::topology::peers::{
    NodeReadiness, PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue,
    WireGuardPeerValue, write_peer,
};
use mantissa_protocol::gossip::gossip_message;
use mantissa_protocol::server;
use mantissa_protocol::topology::{
    PeerMembershipState as CapnpPeerMembershipState, cluster_view_summary, node_drain_status,
    node_info as node_info_capnp, peer as peer_capnp, split_candidate, topology_event,
};
use uuid::Uuid;

use super::TopologyEvent;

/// Join registration payload sent to the anchor and reused for local self-row restoration.
#[derive(Clone)]
pub(super) struct JoinPayload {
    pub(super) id: Uuid,
    pub(super) hostname: String,
    pub(super) advertise_addr: String,
    pub(super) platform_os: String,
    pub(super) platform_arch: String,
    pub(super) incarnation: u64,
    pub(super) server_handle: server::Client,
    pub(super) public_key: [u8; 32],
    pub(super) signing_key: [u8; 32],
    pub(super) identity_sig: [u8; 64],
    pub(super) wireguard: Option<WireGuardPeerValue>,
    pub(super) scheduling: PeerSchedulingState,
    pub(super) readiness: NodeReadiness,
    pub(super) labels: PeerLabelState,
    pub(super) runtime_support: RuntimeSupportProfile,
    pub(super) root_schema: RootSchemaInfo,
}

/// Internal drain state used while deriving the operator-facing drain status response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DrainStatusState {
    Open,
    Fenced,
    Draining,
    Drained,
    Blocked,
}

impl DrainStatusState {
    /// Converts the internal drain state into the Cap'n Proto enum used by RPC clients.
    pub(super) fn as_capnp(self) -> mantissa_protocol::topology::NodeDrainState {
        match self {
            DrainStatusState::Open => mantissa_protocol::topology::NodeDrainState::Open,
            DrainStatusState::Fenced => mantissa_protocol::topology::NodeDrainState::Fenced,
            DrainStatusState::Draining => mantissa_protocol::topology::NodeDrainState::Draining,
            DrainStatusState::Drained => mantissa_protocol::topology::NodeDrainState::Drained,
            DrainStatusState::Blocked => mantissa_protocol::topology::NodeDrainState::Blocked,
        }
    }
}

/// Derived drain-status snapshot detached from the response builder surface.
#[derive(Clone, Debug)]
pub(super) struct NodeDrainStatusSnapshot {
    pub(super) node_id: Uuid,
    pub(super) schedulable: bool,
    pub(super) drain_requested: bool,
    pub(super) task_stop_timeout_secs: Option<u32>,
    pub(super) state: DrainStatusState,
    pub(super) remaining_service_tasks: u32,
    pub(super) blocking_standalone_tasks: u32,
    pub(super) remaining_reserved_slots: u32,
    pub(super) remaining_reserved_gpus: u32,
    pub(super) scheduler_summary_known: bool,
    pub(super) reason: Option<String>,
    pub(super) message: String,
    pub(super) last_scheduling_error: Option<String>,
}

/// Prepared list row for one visible peer after filtering and live status derivation.
#[derive(Clone, Debug)]
pub(super) struct ListedNodeRow {
    pub(super) id: Uuid,
    pub(super) value: PeerValue,
    pub(super) health: mantissa_protocol::health::NodeStatus,
    pub(super) drain_state: mantissa_protocol::topology::NodeDrainState,
}

/// Prepared split-candidate row after attaching health and best-known view metadata.
#[derive(Clone, Debug)]
pub(super) struct SplitCandidateRow {
    pub(super) candidate: SplitNodeCandidate,
    pub(super) health: mantissa_protocol::health::NodeStatus,
    pub(super) active_cluster_view: ClusterViewId,
}

/// Prepared cluster-view summary row detached from the Cap'n Proto builder surface.
#[derive(Clone, Debug)]
pub(super) struct ClusterViewSummaryRow {
    pub(super) view: ClusterViewId,
    pub(super) node_count: u32,
    pub(super) local_active: bool,
    pub(super) cluster_name: Option<String>,
}

/// Converts one scheduling snapshot into the conservative drain state used on wire snapshots.
///
/// This helper is used when the caller only knows the persisted scheduling fence and has not
/// derived a live drain-progress view.
pub(super) fn drain_state_from_scheduling(
    scheduling: &PeerSchedulingState,
) -> mantissa_protocol::topology::NodeDrainState {
    if scheduling.schedulable {
        mantissa_protocol::topology::NodeDrainState::Open
    } else {
        mantissa_protocol::topology::NodeDrainState::Fenced
    }
}

/// Writes the scheduling-related `Peer` fields shared by join, list, and gossip payloads.
pub(super) fn write_scheduling_fields_to_peer(
    mut peer: peer_capnp::Builder<'_>,
    scheduling: &PeerSchedulingState,
) {
    peer.set_schedulable(scheduling.schedulable);
    peer.set_drain_requested(scheduling.drain_requested);
    peer.set_drain_task_stop_timeout_secs(scheduling.drain_task_stop_timeout_secs.unwrap_or(0));
    peer.set_scheduling_updated_at_unix_ms(scheduling.updated_at_unix_ms);
    peer.set_scheduling_actor_node_id(scheduling.actor_node_id.as_bytes());
    if let Some(reason) = scheduling.reason.as_deref() {
        peer.set_scheduling_reason(reason);
    }
}

/// Writes replicated node labels into the topology `Peer` builder.
pub(super) fn write_labels_to_peer(mut peer: peer_capnp::Builder<'_>, labels: &PeerLabelState) {
    let mut entries = peer.reborrow().init_labels(labels.labels.len() as u32);
    for (idx, label) in labels.labels.iter().enumerate() {
        entries.set(idx as u32, label.format_assignment());
    }
    peer.set_labels_updated_at_unix_ms(labels.updated_at_unix_ms);
    peer.set_labels_actor_node_id(labels.actor_node_id.as_bytes());
}

/// Writes one join payload into the topology `NodeInfo` request sent to the anchor.
pub(super) fn write_join_payload_to_node_info(
    mut info: node_info_capnp::Builder<'_>,
    payload: &JoinPayload,
    cluster_view: ClusterViewId,
) {
    set_node_id(info.reborrow().init_id(), &payload.id);
    cluster_view.write_capnp(info.reborrow().init_active_cluster_view());
    info.set_handle(payload.server_handle.clone());
    let peer = join_payload_peer_value(payload);
    write_peer(info.reborrow().init_peer(), &peer);
    info.set_drain_state(drain_state_from_scheduling(&payload.scheduling));
    info.set_readiness_state(payload.readiness.state.as_capnp());
}

/// Builds the peer projection carried by one join payload.
fn join_payload_peer_value(payload: &JoinPayload) -> PeerValue {
    PeerValue {
        address: payload.advertise_addr.clone(),
        hostname: payload.hostname.clone(),
        platform_os: payload.platform_os.clone(),
        platform_arch: payload.platform_arch.clone(),
        noise_static_pub: payload.public_key,
        signing_pub: payload.signing_key,
        identity_sig: payload.identity_sig.to_vec(),
        wireguard: payload.wireguard.clone(),
        scheduling: payload.scheduling.clone(),
        readiness: payload.readiness.clone(),
        labels: payload.labels.clone(),
        runtime_support: payload.runtime_support.clone(),
        root_schema: payload.root_schema,
        membership: PeerMembership::active(payload.incarnation),
    }
}

/// Writes one prepared node-list row into the `list` RPC response.
pub(super) fn write_listed_node_row(
    mut node: node_info_capnp::Builder<'_>,
    row: &ListedNodeRow,
    cluster_view: ClusterViewId,
) {
    set_node_id(node.reborrow().init_id(), &row.id);
    cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
    write_peer(node.reborrow().init_peer(), &row.value);
    node.set_drain_state(row.drain_state);
    node.set_readiness_state(row.value.readiness.state.as_capnp());
    node.set_health(row.health);
}

/// Writes one prepared split-candidate row into the planning RPC response.
pub(super) fn write_split_candidate_row(
    mut row: split_candidate::Builder<'_>,
    candidate: &SplitCandidateRow,
) {
    set_node_id(row.reborrow().init_node_id(), &candidate.candidate.node_id);
    row.set_hostname(&candidate.candidate.hostname);
    row.set_addr(&candidate.candidate.address);
    row.set_wireguard_enabled(candidate.candidate.wireguard_enabled);
    row.set_health(candidate.health);
    candidate
        .active_cluster_view
        .write_capnp(row.reborrow().init_active_cluster_view());

    if let Some(cpu_vendor) = candidate.candidate.cpu_vendor.as_deref() {
        row.set_cpu_vendor(cpu_vendor);
    }
    if let Some(cpu_brand) = candidate.candidate.cpu_brand.as_deref() {
        row.set_cpu_brand(cpu_brand);
    }
    row.set_cpu_logical(candidate.candidate.cpu_logical.unwrap_or_default());
    row.set_cpu_cores(candidate.candidate.cpu_cores.unwrap_or_default());
    row.set_memory_total_kb(candidate.candidate.memory_total_kb.unwrap_or_default());

    if let Some(gpu_vendor) = candidate.candidate.gpu_vendor.as_deref() {
        row.set_gpu_vendor(gpu_vendor);
    }
    row.set_gpu_count(candidate.candidate.gpu_count.unwrap_or_default());

    let mut gpu_models = row
        .reborrow()
        .init_gpu_models(candidate.candidate.gpu_models.len() as u32);
    for (gpu_idx, model) in candidate.candidate.gpu_models.iter().enumerate() {
        gpu_models.set(gpu_idx as u32, model);
    }

    let mut labels = row
        .reborrow()
        .init_labels(candidate.candidate.labels.len() as u32);
    for (label_idx, label) in candidate.candidate.labels.iter().enumerate() {
        labels.set(label_idx as u32, label.format_assignment());
    }
}

/// Writes one derived drain-status snapshot into the RPC response payload.
pub(super) fn write_node_drain_status(
    mut builder: node_drain_status::Builder<'_>,
    status: &NodeDrainStatusSnapshot,
) {
    set_node_id(builder.reborrow().init_node_id(), &status.node_id);
    builder.set_schedulable(status.schedulable);
    builder.set_drain_requested(status.drain_requested);
    builder.set_task_stop_timeout_secs(status.task_stop_timeout_secs.unwrap_or(0));
    builder.set_state(status.state.as_capnp());
    builder.set_remaining_service_tasks(status.remaining_service_tasks);
    builder.set_blocking_standalone_tasks(status.blocking_standalone_tasks);
    builder.set_remaining_reserved_slots(status.remaining_reserved_slots);
    builder.set_remaining_reserved_gpus(status.remaining_reserved_gpus);
    builder.set_scheduler_summary_known(status.scheduler_summary_known);
    builder.set_reason(status.reason.as_deref().unwrap_or_default());
    builder.set_message(&status.message);
    builder.set_last_scheduling_error(status.last_scheduling_error.as_deref().unwrap_or_default());
}

/// Writes one cluster-view summary row after the discovery logic has already resolved counts.
pub(super) fn write_cluster_view_summary_row(
    mut row: cluster_view_summary::Builder<'_>,
    summary: &ClusterViewSummaryRow,
) {
    summary.view.write_capnp(row.reborrow().init_view());
    row.set_node_count(summary.node_count);
    row.set_local_active(summary.local_active);
    if let Some(name) = summary.cluster_name.as_deref() {
        row.set_cluster_name(name);
    }
}

/// Initializes the topology event node shared by membership-like gossip message variants.
fn init_topology_event_node<'a>(
    msg: gossip_message::Builder<'a>,
    event_type: topology_event::EventType,
    node_id: &Uuid,
    cluster_view: ClusterViewId,
) -> node_info_capnp::Builder<'a> {
    let mut topo = msg.init_topology();
    topo.set_event(event_type);
    let mut node = topo.init_node();
    set_node_id(node.reborrow().init_id(), node_id);
    cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
    node
}

/// Writes one join event into a gossip message builder.
fn write_join_event(
    msg: gossip_message::Builder<'_>,
    event: &TopologyEvent,
    cluster_view: ClusterViewId,
) {
    let TopologyEvent::Join {
        id,
        hostname,
        address,
        platform_os,
        platform_arch,
        root_hash,
        incarnation,
        client,
        noise_static_pub,
        signing_pub,
        identity_sig,
        wireguard,
        scheduling,
        readiness,
        labels,
        runtime_support,
        root_schema,
    } = event
    else {
        unreachable!("write_join_event must only be called with join events");
    };

    let mut node = init_topology_event_node(msg, topology_event::EventType::Add, id, cluster_view);
    node.set_root_hash(root_hash);
    let peer = PeerValue {
        address: address.clone(),
        hostname: hostname.clone(),
        platform_os: platform_os.clone(),
        platform_arch: platform_arch.clone(),
        noise_static_pub: noise_static_pub.to_bytes(),
        signing_pub: signing_pub.to_bytes(),
        identity_sig: identity_sig.clone(),
        wireguard: wireguard.clone(),
        scheduling: scheduling.as_ref().clone(),
        readiness: readiness.as_ref().clone(),
        labels: labels.as_ref().clone(),
        runtime_support: runtime_support.as_ref().clone(),
        root_schema: *root_schema,
        membership: PeerMembership::active(*incarnation),
    };
    write_peer(node.reborrow().init_peer(), &peer);

    if let Some(client) = client.as_ref() {
        // Only embed our own handle; forwarding a capability learned from another peer
        // can't be re-exported on this connection safely.
        // Set the handle as a Cap'n Proto client only when available locally.
        node.set_handle(client.clone());
    }
}

/// Writes one membership transition event into a gossip message builder.
fn write_membership_event(
    msg: gossip_message::Builder<'_>,
    event_type: topology_event::EventType,
    id: &Uuid,
    incarnation: u64,
    cluster_view: ClusterViewId,
) {
    let mut node = init_topology_event_node(msg, event_type, id, cluster_view);
    let mut peer = node.reborrow().init_peer();
    peer.set_membership_incarnation(incarnation);
    peer.set_membership_state(if matches!(event_type, topology_event::EventType::Remove) {
        CapnpPeerMembershipState::Left
    } else {
        CapnpPeerMembershipState::Active
    });
}

/// Writes one cluster-name update event into a gossip message builder.
fn write_cluster_name_updated_event(
    msg: gossip_message::Builder<'_>,
    cluster_id: &ClusterId,
    name: &str,
    updated_at_unix_ms: u64,
    actor_node_id: &Uuid,
) {
    let mut topo = msg.init_topology();
    topo.set_event(topology_event::EventType::ClusterNameUpdated);
    topo.reborrow()
        .init_cluster_id()
        .set_value(cluster_id.as_bytes());
    topo.set_cluster_name(name);
    topo.set_updated_at_unix_ms(updated_at_unix_ms);
    set_node_id(topo.init_actor_node_id(), actor_node_id);
}

/// Writes one cluster-wide metadata availability hint into a gossip message builder.
fn write_cluster_metadata_changed_event(
    msg: gossip_message::Builder<'_>,
    operation_id: &Uuid,
    source_node_id: &Uuid,
) {
    let mut topo = msg.init_topology();
    topo.set_event(topology_event::EventType::ClusterMetadataChanged);
    topo.set_operation_id(operation_id.as_bytes());
    topo.set_metadata_source_node_id(source_node_id.as_bytes());
}

/// Writes one scheduling-update event into a gossip message builder.
fn write_node_scheduling_updated_event(
    msg: gossip_message::Builder<'_>,
    id: &Uuid,
    scheduling: &PeerSchedulingState,
    cluster_view: ClusterViewId,
) {
    let mut node = init_topology_event_node(
        msg,
        topology_event::EventType::NodeSchedulingUpdated,
        id,
        cluster_view,
    );
    write_scheduling_fields_to_peer(node.reborrow().init_peer(), scheduling);
}

/// Writes one readiness-update event into a gossip message builder.
fn write_node_readiness_updated_event(
    msg: gossip_message::Builder<'_>,
    id: &Uuid,
    readiness: &NodeReadiness,
    cluster_view: ClusterViewId,
) {
    let mut node = init_topology_event_node(
        msg,
        topology_event::EventType::NodeReadinessUpdated,
        id,
        cluster_view,
    );
    let mut peer = node.reborrow().init_peer();
    peer.set_readiness_state(readiness.state.as_capnp());
    peer.set_readiness_updated_at_unix_ms(readiness.updated_at_unix_ms);
    peer.set_readiness_actor_node_id(readiness.actor_node_id.as_bytes());
}

/// Writes one label-update event into a gossip message builder.
fn write_node_labels_updated_event(
    msg: gossip_message::Builder<'_>,
    id: &Uuid,
    labels: &PeerLabelState,
    cluster_view: ClusterViewId,
) {
    let mut node = init_topology_event_node(
        msg,
        topology_event::EventType::NodeLabelsUpdated,
        id,
        cluster_view,
    );
    write_labels_to_peer(node.reborrow().init_peer(), labels);
}

/// Writes one topology event into the outbound gossip message list.
pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &TopologyEvent,
    cluster_view: ClusterViewId,
) {
    let msg = list.reborrow().get(index);

    match event {
        TopologyEvent::Join { .. } => write_join_event(msg, event, cluster_view),
        TopologyEvent::Leave { id, incarnation } => write_membership_event(
            msg,
            topology_event::EventType::Remove,
            id,
            *incarnation,
            cluster_view,
        ),
        TopologyEvent::Alive { id, incarnation } => write_membership_event(
            msg,
            topology_event::EventType::Alive,
            id,
            *incarnation,
            cluster_view,
        ),
        TopologyEvent::Suspect { id, incarnation } => write_membership_event(
            msg,
            topology_event::EventType::Suspect,
            id,
            *incarnation,
            cluster_view,
        ),
        TopologyEvent::Down { id, incarnation } => write_membership_event(
            msg,
            topology_event::EventType::Down,
            id,
            *incarnation,
            cluster_view,
        ),
        TopologyEvent::ClusterNameUpdated {
            cluster_id,
            name,
            updated_at_unix_ms,
            actor_node_id,
        } => write_cluster_name_updated_event(
            msg,
            cluster_id,
            name,
            *updated_at_unix_ms,
            actor_node_id,
        ),
        TopologyEvent::ClusterMetadataChanged {
            operation_id,
            source_node_id,
        } => write_cluster_metadata_changed_event(msg, operation_id, source_node_id),
        TopologyEvent::NodeSchedulingUpdated { id, scheduling } => {
            write_node_scheduling_updated_event(msg, id, scheduling, cluster_view)
        }
        TopologyEvent::NodeReadinessUpdated { id, readiness } => {
            write_node_readiness_updated_event(msg, id, readiness, cluster_view)
        }
        TopologyEvent::NodeLabelsUpdated { id, labels } => {
            write_node_labels_updated_event(msg, id, labels, cluster_view)
        }
    }
}
