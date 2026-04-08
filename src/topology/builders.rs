use crate::cluster::operations::SplitNodeCandidate;
use crate::cluster::{ClusterId, ClusterViewId};
use crate::node::id::set_node_id;
use crate::runtime::types::RuntimeSupportProfile;
use crate::topology::peers::{PeerSchedulingState, PeerValue, WireGuardPeerValue};
use protocol::gossip::gossip_message;
use protocol::server;
use protocol::topology::{
    cluster_view_summary, node_drain_status, node_info as node_info_capnp, split_candidate,
    topology_event,
};
use uuid::Uuid;

use super::TopologyEvent;

/// Join registration payload sent to the anchor and reused for local self-row restoration.
#[derive(Clone)]
pub(in crate::topology) struct JoinPayload {
    pub(in crate::topology) id: Uuid,
    pub(in crate::topology) hostname: String,
    pub(in crate::topology) advertise_addr: String,
    pub(in crate::topology) incarnation: u64,
    pub(in crate::topology) server_handle: server::Client,
    pub(in crate::topology) public_key: [u8; 32],
    pub(in crate::topology) signing_key: [u8; 32],
    pub(in crate::topology) identity_sig: [u8; 64],
    pub(in crate::topology) wireguard: Option<WireGuardPeerValue>,
    pub(in crate::topology) scheduling: PeerSchedulingState,
    pub(in crate::topology) runtime_support: RuntimeSupportProfile,
}

/// Internal drain state used while deriving the operator-facing drain status response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::topology) enum DrainStatusState {
    Open,
    Fenced,
    Draining,
    Drained,
    Blocked,
}

impl DrainStatusState {
    /// Converts the internal drain state into the Cap'n Proto enum used by RPC clients.
    pub(in crate::topology) fn as_capnp(self) -> protocol::topology::NodeDrainState {
        match self {
            DrainStatusState::Open => protocol::topology::NodeDrainState::Open,
            DrainStatusState::Fenced => protocol::topology::NodeDrainState::Fenced,
            DrainStatusState::Draining => protocol::topology::NodeDrainState::Draining,
            DrainStatusState::Drained => protocol::topology::NodeDrainState::Drained,
            DrainStatusState::Blocked => protocol::topology::NodeDrainState::Blocked,
        }
    }
}

/// Derived drain-status snapshot detached from the response builder surface.
#[derive(Clone, Debug)]
pub(in crate::topology) struct NodeDrainStatusSnapshot {
    pub(in crate::topology) node_id: Uuid,
    pub(in crate::topology) schedulable: bool,
    pub(in crate::topology) drain_requested: bool,
    pub(in crate::topology) task_stop_timeout_secs: Option<u32>,
    pub(in crate::topology) state: DrainStatusState,
    pub(in crate::topology) remaining_service_tasks: u32,
    pub(in crate::topology) blocking_standalone_tasks: u32,
    pub(in crate::topology) remaining_reserved_slots: u32,
    pub(in crate::topology) remaining_reserved_gpus: u32,
    pub(in crate::topology) scheduler_summary_known: bool,
    pub(in crate::topology) reason: Option<String>,
    pub(in crate::topology) message: String,
    pub(in crate::topology) last_scheduling_error: Option<String>,
}

/// Prepared list row for one visible peer after filtering and live status derivation.
#[derive(Clone, Debug)]
pub(in crate::topology) struct ListedNodeRow {
    pub(in crate::topology) id: Uuid,
    pub(in crate::topology) value: PeerValue,
    pub(in crate::topology) health: protocol::health::NodeStatus,
    pub(in crate::topology) drain_state: protocol::topology::NodeDrainState,
}

/// Prepared split-candidate row after attaching health and best-known view metadata.
#[derive(Clone, Debug)]
pub(in crate::topology) struct SplitCandidateRow {
    pub(in crate::topology) candidate: SplitNodeCandidate,
    pub(in crate::topology) health: protocol::health::NodeStatus,
    pub(in crate::topology) active_cluster_view: ClusterViewId,
}

/// Prepared cluster-view summary row detached from the Cap'n Proto builder surface.
#[derive(Clone, Debug)]
pub(in crate::topology) struct ClusterViewSummaryRow {
    pub(in crate::topology) view: ClusterViewId,
    pub(in crate::topology) node_count: u32,
    pub(in crate::topology) local_active: bool,
    pub(in crate::topology) cluster_name: Option<String>,
}

/// Converts one scheduling snapshot into the conservative drain state used on wire snapshots.
///
/// This helper is used when the caller only knows the persisted scheduling fence and has not
/// derived a live drain-progress view.
pub(in crate::topology) fn drain_state_from_scheduling(
    scheduling: &PeerSchedulingState,
) -> protocol::topology::NodeDrainState {
    if scheduling.schedulable {
        protocol::topology::NodeDrainState::Open
    } else {
        protocol::topology::NodeDrainState::Fenced
    }
}

/// Writes one runtime support profile into the topology `NodeInfo` builder.
pub(in crate::topology) fn write_runtime_support_to_node_info(
    mut info: node_info_capnp::Builder<'_>,
    runtime_support: &RuntimeSupportProfile,
) {
    let mut execution_platforms = info
        .reborrow()
        .init_execution_platforms(runtime_support.execution_platforms.len() as u32);
    for (idx, execution_platform) in runtime_support.execution_platforms.iter().enumerate() {
        execution_platforms.set(idx as u32, execution_platform.as_str());
    }

    let mut isolation_modes = info
        .reborrow()
        .init_isolation_modes(runtime_support.isolation_modes.len() as u32);
    for (idx, isolation_mode) in runtime_support.isolation_modes.iter().enumerate() {
        isolation_modes.set(idx as u32, isolation_mode.as_str());
    }

    let mut isolation_profiles = info
        .reborrow()
        .init_isolation_profiles(runtime_support.isolation_profiles.len() as u32);
    for (idx, isolation_profile) in runtime_support.isolation_profiles.iter().enumerate() {
        isolation_profiles.set(idx as u32, isolation_profile);
    }

    let mut feature_flags = info
        .reborrow()
        .init_runtime_feature_flags(runtime_support.feature_flags.len() as u32);
    for (idx, feature_flag) in runtime_support.feature_flags.iter().enumerate() {
        feature_flags.set(idx as u32, feature_flag);
    }
}

/// Writes the scheduling-related `NodeInfo` fields shared by join, list, and gossip payloads.
pub(in crate::topology) fn write_scheduling_fields_to_node_info(
    mut info: node_info_capnp::Builder<'_>,
    scheduling: &PeerSchedulingState,
) {
    info.set_schedulable(scheduling.schedulable);
    info.set_drain_requested(scheduling.drain_requested);
    info.set_drain_task_stop_timeout_secs(scheduling.drain_task_stop_timeout_secs.unwrap_or(0));
    info.set_scheduling_updated_at_unix_ms(scheduling.updated_at_unix_ms);
    set_node_id(
        info.reborrow().init_scheduling_actor_node_id(),
        &scheduling.actor_node_id,
    );
    if let Some(reason) = scheduling.reason.as_deref() {
        info.set_scheduling_reason(reason);
    }
}

/// Writes the optional WireGuard endpoint fields carried by one peer snapshot.
pub(in crate::topology) fn write_wireguard_to_node_info(
    mut info: node_info_capnp::Builder<'_>,
    wireguard: Option<&WireGuardPeerValue>,
) {
    if let Some(wg) = wireguard {
        info.set_wireguard_public_key(&wg.public_key);
        info.set_wireguard_port(wg.port);
        info.set_wireguard_enabled(wg.enabled);
    }
}

/// Writes one join payload into the topology `NodeInfo` request sent to the anchor.
pub(in crate::topology) fn write_join_payload_to_node_info(
    mut info: node_info_capnp::Builder<'_>,
    payload: &JoinPayload,
    cluster_view: ClusterViewId,
) {
    set_node_id(info.reborrow().init_id(), &payload.id);
    cluster_view.write_capnp(info.reborrow().init_active_cluster_view());
    info.set_hostname(&payload.hostname);
    info.set_addr(&payload.advertise_addr);
    info.set_handle(payload.server_handle.clone());
    info.set_public_key(&payload.public_key);
    info.set_signing_key(&payload.signing_key);
    info.set_identity_sig(&payload.identity_sig);
    info.set_incarnation(payload.incarnation);
    write_scheduling_fields_to_node_info(info.reborrow(), &payload.scheduling);
    info.set_drain_state(drain_state_from_scheduling(&payload.scheduling));
    write_runtime_support_to_node_info(info.reborrow(), &payload.runtime_support);
    write_wireguard_to_node_info(info.reborrow(), payload.wireguard.as_ref());
}

/// Writes one prepared node-list row into the `list` RPC response.
pub(in crate::topology) fn write_listed_node_row(
    mut node: node_info_capnp::Builder<'_>,
    row: &ListedNodeRow,
    cluster_view: ClusterViewId,
) {
    set_node_id(node.reborrow().init_id(), &row.id);
    cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
    node.set_addr(&row.value.address);
    node.set_hostname(&row.value.hostname);
    node.set_public_key(&row.value.noise_static_pub);
    node.set_signing_key(&row.value.signing_pub);
    write_scheduling_fields_to_node_info(node.reborrow(), &row.value.scheduling);
    node.set_drain_state(row.drain_state);
    write_runtime_support_to_node_info(node.reborrow(), &row.value.runtime_support);
    write_wireguard_to_node_info(node.reborrow(), row.value.wireguard.as_ref());
    node.set_health(row.health);
}

/// Writes one prepared split-candidate row into the planning RPC response.
pub(in crate::topology) fn write_split_candidate_row(
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
}

/// Writes one derived drain-status snapshot into the RPC response payload.
pub(in crate::topology) fn write_node_drain_status(
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
pub(in crate::topology) fn write_cluster_view_summary_row(
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
        root_hash,
        incarnation,
        client,
        noise_static_pub,
        signing_pub,
        identity_sig,
        wireguard,
        scheduling,
        runtime_support,
    } = event
    else {
        unreachable!("write_join_event must only be called with join events");
    };

    let mut node = init_topology_event_node(msg, topology_event::EventType::Add, id, cluster_view);
    node.set_hostname(hostname);
    node.set_addr(address);
    node.set_root_hash(root_hash);
    node.set_public_key(&noise_static_pub.to_bytes());
    node.set_signing_key(&signing_pub.to_bytes());
    node.set_identity_sig(identity_sig);
    node.set_incarnation(*incarnation);
    write_scheduling_fields_to_node_info(node.reborrow(), scheduling.as_ref());
    write_runtime_support_to_node_info(node.reborrow(), runtime_support.as_ref());
    write_wireguard_to_node_info(node.reborrow(), wireguard.as_ref());

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
    node.set_incarnation(incarnation);
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
    write_scheduling_fields_to_node_info(node.reborrow(), scheduling);
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
        TopologyEvent::NodeSchedulingUpdated { id, scheduling } => {
            write_node_scheduling_updated_event(msg, id, scheduling, cluster_view)
        }
    }
}
