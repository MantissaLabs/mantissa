//! # Gossip
//!
//! This module handles the cluster gossip backbone. It contains the
//! implementation of the gossip server, which spreads informations to
//! other nodes in the cluster based on events and updates to be applied.
//!

use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::dedupe::BoundedSeenCache;
use crate::network::service::{read_network_event, write_network_event};
use crate::network::types::NetworkEvent;
use crate::secrets::service::{read_secret_event, write_secret_event};
use crate::secrets::types::SecretEvent;
use crate::services::service::{read_service_event, write_service_event};
use crate::services::types::ServiceEvent;
use crate::task::container::ContainerState;
use crate::task::service as task_service;
use crate::task::types::{TaskEvent, TaskSpec};
use crate::topology;
use crate::topology::TopologyEvent;
use crate::topology::peer_provider::PeerProvider;
use async_channel::{Receiver, Sender, TrySendError};
use async_trait::async_trait;
use capnp::Error;
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use protocol::gossip;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::gossip::gossip_message::Which::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use topology::PeerHandle;
use tracing::{debug, error, warn};
use uuid::Uuid;

#[async_trait(?Send)]
pub trait GossipContext: PeerProvider {
    /// Returns the currently active cluster view used for observability tags.
    fn active_cluster_view(&self) -> ClusterViewId {
        ClusterViewId::legacy_default()
    }

    fn local_peer_id(&self) -> Uuid;

    async fn gossip_client_for(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error>;

    /// Returns peers for the global metadata gossip plane.
    ///
    /// By default this reuses the view-scoped peer list so non-topology callers do
    /// not need to implement a second provider path.
    async fn get_peers_unscoped(&self) -> Vec<PeerHandle> {
        self.get_peers().await
    }

    /// Resolves a gossip capability without enforcing active-view session matching.
    ///
    /// The default implementation keeps existing behavior, but topology can override
    /// this to route selected metadata events across split view boundaries.
    async fn gossip_client_for_unscoped(
        &self,
        peer: &PeerHandle,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        self.gossip_client_for(peer).await
    }

    async fn invalidate_peer_capabilities(&self, peer: &PeerHandle) {
        let _ = peer;
    }
}

#[derive(Clone)]
pub enum Message {
    Void { id: Uuid },
    Topology { id: Uuid, event: TopologyEvent },
    Task { id: Uuid, event: TaskEvent },
    Service { id: Uuid, event: ServiceEvent },
    Network { id: Uuid, event: NetworkEvent },
    Secret { id: Uuid, event: SecretEvent },
    // Scheduling(SchedulingEvent),
}

impl Message {
    pub fn id(&self) -> Uuid {
        match self {
            Message::Void { id }
            | Message::Topology { id, .. }
            | Message::Task { id, .. }
            | Message::Service { id, .. }
            | Message::Network { id, .. }
            | Message::Secret { id, .. } => *id,
        }
    }
}

/// Gossip transport plane selector.
///
/// `ViewScoped` carries regular control-plane events that must stay inside the active
/// view boundary. `GlobalMetadata` carries low-rate metadata that is allowed to cross
/// split view boundaries (currently cluster lineage names).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GossipPlane {
    ViewScoped,
    GlobalMetadata,
}

impl GossipPlane {
    /// Returns a stable telemetry label for the plane.
    fn as_str(self) -> &'static str {
        match self {
            Self::ViewScoped => "view_scoped",
            Self::GlobalMetadata => "global_metadata",
        }
    }

    /// Returns true when this plane may cross view boundaries.
    fn allows_cross_view(self) -> bool {
        matches!(self, Self::GlobalMetadata)
    }
}

/// Selects the gossip plane for one outbound message.
fn gossip_plane_for_message(message: &Message) -> GossipPlane {
    match message {
        Message::Topology {
            event: TopologyEvent::ClusterNameUpdated { .. },
            ..
        } => GossipPlane::GlobalMetadata,
        _ => GossipPlane::ViewScoped,
    }
}

/// Selects the gossip plane for one inbound wire message.
fn gossip_plane_for_wire_message(message: gossip::gossip_message::Reader<'_>) -> GossipPlane {
    match message.which() {
        Ok(Topology(Ok(reader))) => match reader.get_event() {
            Ok(protocol::topology::topology_event::EventType::ClusterNameUpdated) => {
                GossipPlane::GlobalMetadata
            }
            _ => GossipPlane::ViewScoped,
        },
        _ => GossipPlane::ViewScoped,
    }
}

/// Returns whether one inbound message should be enqueued for relay.
///
/// Global metadata events are always relayed regardless of the generic relay env flag
/// so cluster lineage names converge quickly without enabling high-volume relay for all
/// task/service update traffic.
fn should_relay_inbound_message(relay_inbound: bool, message: &Message) -> bool {
    relay_inbound || gossip_plane_for_message(message).allows_cross_view()
}

pub const DEFAULT_FANOUT: usize = 5;
/// Max number of gossip messages sent in a single RPC request.
const MAX_GOSSIP_BATCH_MESSAGES: usize = 32;
/// Default max message count per outbound gossip RPC call.
const DEFAULT_GOSSIP_RPC_BATCH_MAX: usize = MAX_GOSSIP_BATCH_MESSAGES;
/// Default number of peer sends allowed concurrently within one outbound dispatch batch.
const DEFAULT_GOSSIP_SEND_PARALLELISM: usize = 1;
/// Maximum number of gossip identifiers retained for ingress deduplication.
const GOSSIP_DEDUPE_MAX_ENTRIES: usize = 100_000;
/// Time window used to suppress duplicate gossip identifiers.
const GOSSIP_DEDUPE_TTL: Duration = Duration::from_secs(10 * 60);
/// Process-wide counter tracking how many outbound task gossip updates were coalesced.
static GOSSIP_COALESCED_TASK_UPDATES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Coalesces one pending outbound gossip batch by task identifier.
///
/// This keeps at most one task lifecycle update per task id for each flush tick, selecting the
/// causally newest state so we reduce launch-phase chatter and avoid propagating stale transitions.
fn coalesce_pending_messages(pending: Vec<Message>) -> (Vec<Message>, usize) {
    let mut coalesced = Vec::with_capacity(pending.len());
    let mut task_positions: HashMap<Uuid, usize> = HashMap::new();
    let mut dropped = 0usize;

    for message in pending {
        let Some(task_id) = task_message_task_id(&message) else {
            coalesced.push(message);
            continue;
        };

        if let Some(position) = task_positions.get(&task_id).copied() {
            if should_replace_task_message(&coalesced[position], &message) {
                coalesced[position] = message;
            }
            dropped = dropped.saturating_add(1);
            continue;
        }

        task_positions.insert(task_id, coalesced.len());
        coalesced.push(message);
    }

    (coalesced, dropped)
}

/// Returns the logical task identifier for one gossip message when it carries a task event.
fn task_message_task_id(message: &Message) -> Option<Uuid> {
    match message {
        Message::Task { event, .. } => Some(match event {
            TaskEvent::Upsert(spec) => spec.id,
            TaskEvent::Remove { id } => *id,
        }),
        _ => None,
    }
}

/// Returns true when the candidate task message should replace the currently retained one.
fn should_replace_task_message(current: &Message, candidate: &Message) -> bool {
    let (
        Message::Task {
            event: current_event,
            ..
        },
        Message::Task {
            event: candidate_event,
            ..
        },
    ) = (current, candidate)
    else {
        return false;
    };

    match (current_event, candidate_event) {
        (TaskEvent::Remove { .. }, TaskEvent::Upsert(_)) => false,
        (_, TaskEvent::Remove { .. }) => true,
        (TaskEvent::Upsert(current_spec), TaskEvent::Upsert(candidate_spec)) => {
            should_accept_task_spec(current_spec, candidate_spec)
        }
    }
}

/// Returns true when one candidate task specification is causally newer than the current one.
fn should_accept_task_spec(current: &TaskSpec, candidate: &TaskSpec) -> bool {
    compare_task_spec_causality(current, candidate).is_gt()
}

/// Compares two task specifications by causal ordering used for outbound gossip coalescing.
fn compare_task_spec_causality(current: &TaskSpec, candidate: &TaskSpec) -> Ordering {
    match candidate.task_epoch.cmp(&current.task_epoch) {
        Ordering::Equal => {}
        order => return order,
    }
    match candidate.phase_version.cmp(&current.phase_version) {
        Ordering::Equal => {}
        order => return order,
    }

    match (
        parse_task_timestamp(&current.updated_at, &current.created_at),
        parse_task_timestamp(&candidate.updated_at, &candidate.created_at),
    ) {
        (Some(current_ts), Some(candidate_ts)) => {
            if candidate_ts > current_ts {
                return Ordering::Greater;
            } else if candidate_ts < current_ts {
                return Ordering::Less;
            }
        }
        (None, Some(_)) => return Ordering::Greater,
        (Some(_), None) => return Ordering::Less,
        (None, None) => {}
    }

    let current_rank = task_state_rank(&current.state);
    let candidate_rank = task_state_rank(&candidate.state);
    match candidate_rank.cmp(&current_rank) {
        Ordering::Equal => candidate.node_id.cmp(&current.node_id),
        order => order,
    }
}

/// Parses the freshest available timestamp from one task spec for causal comparisons.
fn parse_task_timestamp(updated_at: &str, created_at: &str) -> Option<DateTime<Utc>> {
    parse_timestamp(updated_at).or_else(|| parse_timestamp(created_at))
}

/// Parses one RFC3339 timestamp into UTC.
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Ranks task states by lifecycle progression when epoch/version/timestamp are tied.
fn task_state_rank(state: &ContainerState) -> u8 {
    match state {
        ContainerState::Running => 6,
        ContainerState::Creating => 5,
        ContainerState::Pulling => 5,
        ContainerState::Pending => 4,
        ContainerState::Stopping => 3,
        ContainerState::Stopped => 2,
        ContainerState::Paused => 1,
        ContainerState::Failed | ContainerState::Exited(_) | ContainerState::Unknown => 0,
    }
}

/// Reads the optional outbound gossip RPC batch cap from the environment.
fn gossip_rpc_batch_max_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GOSSIP_RPC_BATCH_MAX")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Reads the optional maximum concurrent gossip send count from the environment.
fn gossip_send_parallelism_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GOSSIP_SEND_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Reads the optional per-peer gossip send timeout (milliseconds) from the environment.
fn gossip_send_timeout_from_env() -> Option<Duration> {
    std::env::var("MANTISSA_GOSSIP_SEND_TIMEOUT_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
}

/// Reads whether inbound gossip should be relayed into the outbound queue.
///
/// Disabled by default to avoid amplifying high-volume task update streams.
fn gossip_relay_inbound_from_env() -> bool {
    std::env::var("MANTISSA_GOSSIP_RELAY_INBOUND")
        .ok()
        .map(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

/// Derives one deterministic cursor seed from the local peer id.
///
/// This de-correlates rotating fanout windows across nodes so they do not all target
/// the same peer subset on each gossip tick.
fn fanout_cursor_seed(peer_id: Uuid) -> usize {
    let bytes = peer_id.as_bytes();
    let mut lower = [0u8; 8];
    lower.copy_from_slice(&bytes[..8]);
    let mut upper = [0u8; 8];
    upper.copy_from_slice(&bytes[8..]);
    (u64::from_le_bytes(lower) ^ u64::from_le_bytes(upper)) as usize
}

/// Shared handle type used by ingress and outbound gossip loops for deduplication.
pub(crate) type DedupeStateHandle = Arc<AsyncMutex<GossipDedupeState>>;

/// Process-local gossip dedupe state tied to the currently active cluster view.
#[derive(Debug)]
pub(crate) struct GossipDedupeState {
    last_active_view: ClusterViewId,
    seen: BoundedSeenCache,
}

impl GossipDedupeState {
    /// Builds one dedupe state initialized for the provided active cluster view.
    fn new(active_view: ClusterViewId) -> Self {
        Self {
            last_active_view: active_view,
            seen: BoundedSeenCache::new(GOSSIP_DEDUPE_MAX_ENTRIES, GOSSIP_DEDUPE_TTL),
        }
    }

    /// Rotates the dedupe cache whenever the active cluster view changes.
    fn rotate_if_view_changed(&mut self, active_view: ClusterViewId) {
        if self.last_active_view == active_view {
            return;
        }
        self.last_active_view = active_view;
        self.seen = BoundedSeenCache::new(GOSSIP_DEDUPE_MAX_ENTRIES, GOSSIP_DEDUPE_TTL);
    }

    /// Records one inbound gossip identifier and returns true only when it is new.
    fn record_inbound(&mut self, active_view: ClusterViewId, id: Uuid) -> bool {
        self.rotate_if_view_changed(active_view);
        self.seen.record(id)
    }

    /// Records one locally-originated identifier so echoed copies are suppressed.
    fn record_outbound(&mut self, active_view: ClusterViewId, id: Uuid) {
        self.rotate_if_view_changed(active_view);
        let _ = self.seen.record(id);
    }
}

/// Represents the gossip server.
pub struct Gossip {
    pub chans: Channels,
    pub cluster_view: ClusterViewState,
    dedupe_state: DedupeStateHandle,
}

pub struct Channels {
    pub topology_events: Sender<Message>,
    pub task_events: Sender<Message>,
    pub service_events: Sender<Message>,
    pub network_events: Sender<Message>,
    pub secret_events: Sender<Message>,
    /// Shared outbound queue so newly received gossip can be forwarded to additional peers.
    pub outbound_events: Sender<Message>,
    // scheduling_events: Sender<SchedulingEvent>,
}

impl Gossip {
    /// Creates a gossip server with one shared dedupe state for ingress and egress loops.
    pub fn new(chans: Channels, cluster_view: ClusterViewState) -> Self {
        let active_view = cluster_view.active_view();
        Self {
            chans,
            cluster_view,
            dedupe_state: Arc::new(AsyncMutex::new(GossipDedupeState::new(active_view))),
        }
    }

    /// Returns the dedupe state handle so the outbound loop can pre-register local ids.
    pub(crate) fn dedupe_state_handle(&self) -> DedupeStateHandle {
        self.dedupe_state.clone()
    }
}

impl gossip::Server for Gossip {
    async fn gossip(
        self: Rc<Self>,
        params: gossip::GossipParams,
        _results: gossip::GossipResults,
    ) -> Result<(), Error> {
        let topo_tx = self.chans.topology_events.clone();
        let task_tx = self.chans.task_events.clone();
        let service_tx = self.chans.service_events.clone();
        let network_tx = self.chans.network_events.clone();
        let secret_tx = self.chans.secret_events.clone();
        let outbound_tx = self.chans.outbound_events.clone();
        let relay_inbound = gossip_relay_inbound_from_env();

        let params_reader = params
            .get()
            .map_err(|e| Error::failed(format!("failed to read gossip params: {e}")))?;
        let messages = params_reader
            .get_messages()
            .map_err(|e| Error::failed(format!("failed to read gossip messages: {e}")))?;
        let message_list = messages
            .get_messages()
            .map_err(|e| Error::failed(format!("failed to read gossip message list: {e}")))?;

        for msg in message_list.iter() {
            let id = match msg.get_id() {
                Ok(data) => {
                    let bytes = data.to_owned();
                    match <[u8; 16]>::try_from(bytes.as_slice()) {
                        Ok(arr) => Uuid::from_bytes(arr),
                        Err(_) => {
                            eprintln!("Invalid gossip id length");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Missing gossip id: {e}");
                    continue;
                }
            };
            let message_view = match msg.get_view() {
                Ok(view) => match ClusterViewId::from_capnp(view) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        debug!(
                            target: "gossip",
                            gossip_id = %id,
                            "dropping gossip message with invalid cluster view: {err}"
                        );
                        continue;
                    }
                },
                Err(_) => {
                    debug!(
                        target: "gossip",
                        gossip_id = %id,
                        "dropping gossip message without cluster view"
                    );
                    continue;
                }
            };
            let gossip_plane = gossip_plane_for_wire_message(msg.reborrow());
            let active_view = self.cluster_view.active_view();
            if !gossip_plane.allows_cross_view() && message_view != active_view {
                debug!(
                    target: "gossip",
                    gossip_id = %id,
                    message_view = %message_view,
                    active_view = %active_view,
                    gossip_plane = gossip_plane.as_str(),
                    "dropping gossip message for non-active cluster view"
                );
                continue;
            }
            {
                let mut dedupe = self.dedupe_state.lock().await;
                if !dedupe.record_inbound(active_view, id) {
                    debug!(
                        target: "gossip",
                        gossip_id = %id,
                        view = %active_view,
                        "dropping duplicate gossip message"
                    );
                    continue;
                }
            }
            let which = msg.reborrow().which().expect("failed to read variant");
            let message_type = match &which {
                Void(_) => "void",
                Topology(_) => "topology",
                Task(_) => "task",
                Service(_) => "service",
                Network(_) => "network",
                Secret(_) => "secret",
            };
            debug!(
                target: "gossip",
                gossip_id = %id,
                view = %message_view,
                gossip_plane = gossip_plane.as_str(),
                message_type = message_type,
                "received gossip message"
            );

            match which {
                Void(_) => {
                    let message = Message::Void { id };
                    if should_relay_inbound_message(relay_inbound, &message) {
                        forward_inbound_message(&outbound_tx, message_for_forwarding(&message));
                    }
                    let _ = topo_tx.send(message).await;
                }
                Topology(Ok(reader)) => match topology::read_topology_event(reader) {
                    Ok(event) => {
                        let message = Message::Topology { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(&outbound_tx, message_for_forwarding(&message));
                        }
                        topo_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't sent event to topology: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert topology event: {e}"),
                },
                Topology(Err(e)) => {
                    eprintln!("Error reading topology: {e}");
                }
                Task(Ok(reader)) => match task_service::read_event(reader) {
                    Ok(event) => {
                        let message = Message::Task { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(&outbound_tx, message_for_forwarding(&message));
                        }
                        task_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to task: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert task event: {e}"),
                },
                Task(Err(e)) => {
                    eprintln!("Error reading task: {e}");
                }
                Service(Ok(reader)) => match read_service_event(reader) {
                    Ok(event) => {
                        let message = Message::Service { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(&outbound_tx, message_for_forwarding(&message));
                        }
                        service_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to services: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert service event: {e}"),
                },
                Service(Err(e)) => {
                    eprintln!("Error reading service: {e}");
                }
                Network(Ok(reader)) => match read_network_event(reader) {
                    Ok(event) => {
                        let message = Message::Network { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(&outbound_tx, message_for_forwarding(&message));
                        }
                        network_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to networks: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert network event: {e}"),
                },
                Network(Err(e)) => {
                    eprintln!("Error reading network: {e}");
                }
                Secret(Ok(reader)) => match read_secret_event(reader) {
                    Ok(event) => {
                        let message = Message::Secret { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(&outbound_tx, message_for_forwarding(&message));
                        }
                        secret_tx.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to secrets: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert secret event: {e}"),
                },
                Secret(Err(e)) => {
                    eprintln!("Error reading secret: {e}");
                }
            }
        }
        Ok(())
    }
}

// This method receives messages to gossip to neighbors in the network.
pub(crate) async fn start<C>(
    event_rx: Receiver<Message>,
    context: C,
    dedupe_state: DedupeStateHandle,
    fanout: Option<usize>,
    tick: Duration,
) where
    C: GossipContext,
{
    use tokio::time::interval;
    let mut ticker = interval(tick);
    let mut buffer: Vec<Message> = Vec::new();
    let rpc_batch_max = gossip_rpc_batch_max_from_env(DEFAULT_GOSSIP_RPC_BATCH_MAX);
    let send_parallelism = gossip_send_parallelism_from_env(DEFAULT_GOSSIP_SEND_PARALLELISM);
    let send_timeout = gossip_send_timeout_from_env();
    let mut fanout_cursor = fanout_cursor_seed(context.local_peer_id());

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let pending = std::mem::take(&mut buffer);

                if pending.is_empty() {
                    // Idle ticks no longer emit synthetic void gossip messages. Health probing
                    // runs on a separate loop, and anti-entropy sync already guarantees
                    // convergence without per-tick heartbeat payloads.
                    buffer = pending;
                    continue;
                }

                let before_count = pending.len();
                let (pending, coalesced_task_updates) = coalesce_pending_messages(pending);
                if coalesced_task_updates > 0 {
                    let total_coalesced_task_updates = GOSSIP_COALESCED_TASK_UPDATES_TOTAL
                        .fetch_add(coalesced_task_updates as u64, AtomicOrdering::Relaxed)
                        + coalesced_task_updates as u64;
                    debug!(
                        target: "diag.gossip.coalesce",
                        before_count,
                        after_count = pending.len(),
                        coalesced_task_updates,
                        total_coalesced_task_updates,
                        "coalesced outbound task gossip updates"
                    );
                    if should_emit_diag_sample(total_coalesced_task_updates) {
                        warn!(
                            target: "diag.gossip.coalesce",
                            before_count,
                            after_count = pending.len(),
                            coalesced_task_updates,
                            total_coalesced_task_updates,
                            "coalesced outbound task gossip updates sampled"
                        );
                    }
                }

                let (view_scoped, global_metadata) = split_messages_by_plane(pending);
                dispatch_gossip_plane(
                    &context,
                    view_scoped,
                    GossipPlane::ViewScoped,
                    fanout,
                    &mut fanout_cursor,
                    rpc_batch_max,
                    send_parallelism,
                    send_timeout,
                )
                .await;
                dispatch_gossip_plane(
                    &context,
                    global_metadata,
                    GossipPlane::GlobalMetadata,
                    fanout,
                    &mut fanout_cursor,
                    rpc_batch_max,
                    send_parallelism,
                    send_timeout,
                )
                .await;
                buffer = Vec::new();
            }

            Ok(msg) = event_rx.recv() => {
                let active_view = context.active_cluster_view();
                let mut dedupe = dedupe_state.lock().await;
                dedupe.record_outbound(active_view, msg.id());
                buffer.push(msg);
            }

            // channel closed
            else => break,
        }
    }
}

/// Partitions pending outbound messages by gossip plane.
///
/// This keeps control-plane traffic inside the active view while allowing selected
/// low-rate metadata updates to use the global metadata plane.
fn split_messages_by_plane(pending: Vec<Message>) -> (Vec<Message>, Vec<Message>) {
    let mut view_scoped = Vec::new();
    let mut global_metadata = Vec::new();

    for message in pending {
        match gossip_plane_for_message(&message) {
            GossipPlane::ViewScoped => view_scoped.push(message),
            GossipPlane::GlobalMetadata => global_metadata.push(message),
        }
    }

    (view_scoped, global_metadata)
}

/// Dispatches one plane-specific outbound gossip batch to the selected peers.
///
/// The plane controls both peer selection (view-scoped vs unscoped) and capability
/// resolution strategy, while preserving the same bounded fanout and chunking behavior.
async fn dispatch_gossip_plane<C>(
    context: &C,
    pending: Vec<Message>,
    plane: GossipPlane,
    fanout: Option<usize>,
    fanout_cursor: &mut usize,
    rpc_batch_max: usize,
    send_parallelism: usize,
    send_timeout: Option<Duration>,
) where
    C: GossipContext + ?Sized,
{
    if pending.is_empty() {
        return;
    }

    let peers = match plane {
        GossipPlane::ViewScoped => match fanout {
            Some(0) => context.get_peers().await,
            Some(n) => fanout_sample(context, n, fanout_cursor).await,
            None => fanout_sample(context, DEFAULT_FANOUT, fanout_cursor).await,
        },
        GossipPlane::GlobalMetadata => {
            let peer_population = context.get_peers_unscoped().await;
            match fanout {
                Some(0) => peer_population,
                Some(n) => select_fanout_window(peer_population, n, fanout_cursor),
                None => select_fanout_window(peer_population, DEFAULT_FANOUT, fanout_cursor),
            }
        }
    };
    let self_id = context.local_peer_id();
    let cluster_view = context.active_cluster_view();

    debug!(
        target: "gossip",
        cluster_view = %cluster_view,
        gossip_plane = plane.as_str(),
        peer_count = peers.len(),
        message_count = pending.len(),
        "gossip tick dispatch"
    );

    let total_batches = pending.len().div_ceil(MAX_GOSSIP_BATCH_MESSAGES);
    for (batch_idx, batch) in pending.chunks(MAX_GOSSIP_BATCH_MESSAGES).enumerate() {
        debug!(
            target: "gossip",
            cluster_view = %cluster_view,
            gossip_plane = plane.as_str(),
            batch = batch_idx + 1,
            total_batches,
            message_count = batch.len(),
            "gossip chunk dispatch"
        );

        let mut inflight = FuturesUnordered::new();
        for peer in peers.iter() {
            if peer.id == self_id {
                continue;
            }

            // Filter out messages that describe the peer itself so we never hand
            // its exported capability back to the same connection.
            let outbound: Vec<Message> = batch
                .iter()
                .filter(|msg| !message_targets_peer(msg, peer.id))
                .cloned()
                .collect();

            if outbound.is_empty() {
                continue;
            }
            inflight.push(send_gossip_to_peer(
                outbound,
                peer,
                context,
                rpc_batch_max,
                send_timeout,
                cluster_view,
                plane,
            ));
            if inflight.len() >= send_parallelism {
                let _ = inflight.next().await;
            }
        }
        while inflight.next().await.is_some() {}
    }
}

/// Best-effort forwards one newly received gossip message into the outbound queue.
///
/// This converts the gossip path into bounded epidemic forwarding while preserving
/// backpressure safety: when the queue is saturated we drop the relay and rely on sync.
fn forward_inbound_message(outbound_tx: &Sender<Message>, message: Option<Message>) {
    let Some(message) = message else {
        return;
    };
    let message_id = message.id();
    match outbound_tx.try_send(message) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            debug!(
                target: "gossip",
                gossip_id = %message_id,
                "dropping inbound gossip relay due full outbound queue"
            );
        }
        Err(TrySendError::Closed(_)) => {
            warn!(
                target: "gossip",
                gossip_id = %message_id,
                "failed to relay inbound gossip because outbound queue is closed"
            );
        }
    }
}

/// Returns the message shape that should be forwarded to peers for one inbound gossip event.
///
/// Topology join events intentionally drop imported `client` capabilities before relay to avoid
/// re-exporting non-local Cap'n Proto handles through intermediate peers.
fn message_for_forwarding(message: &Message) -> Option<Message> {
    match message {
        Message::Void { .. } => None,
        Message::Topology { id, event } => {
            let forwarded_event = match event {
                TopologyEvent::Join {
                    id: peer_id,
                    hostname,
                    address,
                    root_hash,
                    incarnation,
                    client: _,
                    noise_static_pub,
                    signing_pub,
                    identity_sig,
                    wireguard,
                } => TopologyEvent::Join {
                    id: *peer_id,
                    hostname: hostname.clone(),
                    address: address.clone(),
                    root_hash: root_hash.clone(),
                    incarnation: *incarnation,
                    client: None,
                    noise_static_pub: *noise_static_pub,
                    signing_pub: signing_pub.clone(),
                    identity_sig: identity_sig.clone(),
                    wireguard: wireguard.clone(),
                },
                other => other.clone(),
            };
            Some(Message::Topology {
                id: *id,
                event: forwarded_event,
            })
        }
        _ => Some(message.clone()),
    }
}

/// Sends one outbound gossip payload to one peer with RPC chunking and timeout guards.
///
/// The timeout is applied per RPC chunk so one stalled peer cannot indefinitely delay
/// the dispatch loop for the rest of the selected fanout peers.
async fn send_gossip_to_peer<C>(
    outbound: Vec<Message>,
    peer: &PeerHandle,
    context: &C,
    rpc_batch_max: usize,
    send_timeout: Option<Duration>,
    cluster_view: ClusterViewId,
    plane: GossipPlane,
) where
    C: GossipContext + ?Sized,
{
    for outbound_batch in outbound.chunks(rpc_batch_max) {
        let send_result = if let Some(timeout) = send_timeout {
            match tokio::time::timeout(timeout, send_gossip(outbound_batch, peer, context, plane))
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    warn!(
                        target: "diag.gossip.send",
                        cluster_view = %cluster_view,
                        gossip_plane = plane.as_str(),
                        peer = %peer.id,
                        addr = %peer.address,
                        message_count = outbound_batch.len(),
                        timeout_ms = timeout.as_millis() as u64,
                        "gossip send timed out"
                    );
                    break;
                }
            }
        } else {
            send_gossip(outbound_batch, peer, context, plane).await
        };

        match send_result {
            Ok(()) => {}
            Err(e) => {
                error!("Gossip to {} failed: {:?}", peer.address, e);
                warn!(
                    target: "diag.gossip.send",
                    cluster_view = %cluster_view,
                    gossip_plane = plane.as_str(),
                    peer = %peer.id,
                    addr = %peer.address,
                    message_count = outbound_batch.len(),
                    disconnected = is_disconnected_capnp(&e),
                    error = %e,
                    "gossip send failed"
                );
                break;
            }
        }
    }
}

async fn send_gossip<C>(
    messages: &[Message],
    peer: &PeerHandle,
    ctx: &C,
    plane: GossipPlane,
) -> Result<(), capnp::Error>
where
    C: GossipContext + ?Sized,
{
    if messages.is_empty() {
        return Ok(());
    }
    let cluster_view = ctx.active_cluster_view();

    let gossip_cap = match plane {
        GossipPlane::ViewScoped => ctx.gossip_client_for(peer).await?,
        GossipPlane::GlobalMetadata => ctx.gossip_client_for_unscoped(peer).await?,
    };
    let Some(gossip_cap) = gossip_cap else {
        return Ok(());
    };

    let mut req = gossip_cap.gossip_request();
    let message_count = messages.len() as u32;
    let list = req.get().init_messages();
    let mut msgs = list.init_messages(message_count);

    for (idx, msg) in messages.iter().enumerate() {
        let mut builder = msgs.reborrow().get(idx as u32);
        builder.set_id(msg.id().as_bytes());
        cluster_view.write_capnp(builder.reborrow().init_view());

        match msg {
            Message::Void { .. } => {
                builder.init_void();
            }
            Message::Topology { event, .. } => {
                topology::add_event(&mut msgs, idx as u32, event, cluster_view);
            }
            Message::Task { event, .. } => {
                task_service::add_event(&mut msgs, idx as u32, event);
            }
            Message::Service { event, .. } => {
                let service_builder = builder.init_service();
                write_service_event(service_builder, event)?;
            }
            Message::Network { event, .. } => {
                let network_builder = builder.init_network();
                write_network_event(network_builder, event)?;
            }
            Message::Secret { event, .. } => {
                let secret_builder = builder.init_secret();
                write_secret_event(secret_builder, event)?;
            }
        }
    }

    match req.send().promise.await {
        Ok(_) => {
            debug!(
                target: "gossip",
                cluster_view = %cluster_view,
                gossip_plane = plane.as_str(),
                peer = %peer.id,
                message_count = messages.len(),
                "gossip batch delivered"
            );
            Ok(())
        }
        Err(err) => {
            ctx.invalidate_peer_capabilities(peer).await;
            Err(err)
        }
    }
}

/// # Description:
///
/// Returns true when one Cap'n Proto error corresponds to a remote disconnect.
fn is_disconnected_capnp(error: &capnp::Error) -> bool {
    let text = error.to_string();
    text.contains("Disconnected") || text.contains("disconnected")
}

/// Returns true when one telemetry counter sample should emit a diagnostic log.
fn should_emit_diag_sample(count: u64) -> bool {
    count <= 3 || count.is_power_of_two() || count % 100 == 0
}

// Return true when the gossip message is about the provided peer identifier.
fn message_targets_peer(message: &Message, peer_id: Uuid) -> bool {
    match message {
        Message::Void { .. } => false,
        Message::Topology { event, .. } => match event {
            TopologyEvent::Join { id, .. }
            | TopologyEvent::Leave { id }
            | TopologyEvent::Alive { id, .. }
            | TopologyEvent::Suspect { id, .. }
            | TopologyEvent::Down { id, .. } => *id == peer_id,
            TopologyEvent::ClusterNameUpdated { .. } => false,
        },
        // Task updates replicate to every peer regardless of assignment so keep them.
        Message::Task { .. } => false,
        Message::Service { .. } => false,
        Message::Network { .. } => false,
        Message::Secret { .. } => false,
    }
}

/// Selects a deterministic fanout window from known peers using one rotating cursor.
///
/// This keeps fanout low while guaranteeing eventual coverage of all peers without relying
/// on random sampling luck.
pub async fn fanout_sample<P>(provider: &P, fanout: usize, cursor: &mut usize) -> Vec<PeerHandle>
where
    P: PeerProvider + ?Sized,
{
    let peers = provider.get_peers().await;
    select_fanout_window(peers, fanout, cursor)
}

/// Selects one deterministic fanout window from an already-materialized peer list.
///
/// The selection is stable for a given cursor value and rotates on each call so all
/// peers are eventually selected without randomized shuffles.
fn select_fanout_window(
    mut peers: Vec<PeerHandle>,
    fanout: usize,
    cursor: &mut usize,
) -> Vec<PeerHandle> {
    if peers.is_empty() {
        *cursor = 0;
        return peers;
    }

    peers.sort_by(|a, b| a.id.cmp(&b.id));
    let target = if fanout == 0 {
        peers.len()
    } else {
        fanout.min(peers.len())
    };

    if target >= peers.len() {
        *cursor = 0;
        return peers;
    }

    let start = *cursor % peers.len();
    let mut selected = Vec::with_capacity(target);
    for offset in 0..target {
        selected.push(peers[(start + offset) % peers.len()].clone());
    }
    *cursor = (start + target) % peers.len();
    selected
}

#[cfg(test)]
mod tests {
    use super::{
        GossipDedupeState, Message, coalesce_pending_messages, fanout_sample,
        message_for_forwarding,
    };
    use crate::cluster::{ClusterId, ClusterViewId};
    use crate::task::container::ContainerState;
    use crate::task::types::{TaskEvent, TaskSpec};
    use crate::topology::PeerHandle;
    use crate::topology::TopologyEvent;
    use crate::topology::peer_provider::PeerProvider;
    use async_trait::async_trait;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::collections::HashSet;
    use uuid::Uuid;
    use x25519_dalek::PublicKey;

    #[derive(Clone)]
    struct StaticPeerProvider {
        peers: Vec<PeerHandle>,
    }

    #[async_trait(?Send)]
    impl PeerProvider for StaticPeerProvider {
        /// Returns one fixed peer list for deterministic fanout sampling tests.
        async fn get_peers(&self) -> Vec<PeerHandle> {
            self.peers.clone()
        }
    }

    /// Duplicate message ids should be rejected while the active view is unchanged.
    #[test]
    fn dedupe_state_rejects_duplicate_in_same_view() {
        let view = ClusterViewId::legacy_default();
        let id = Uuid::new_v4();
        let mut dedupe = GossipDedupeState::new(view);

        assert!(dedupe.record_inbound(view, id));
        assert!(!dedupe.record_inbound(view, id));
    }

    /// Outbound ids should be pre-registered so echoed inbound copies are dropped.
    #[test]
    fn dedupe_state_preseeds_outbound_ids() {
        let view = ClusterViewId::legacy_default();
        let id = Uuid::new_v4();
        let mut dedupe = GossipDedupeState::new(view);

        dedupe.record_outbound(view, id);
        assert!(!dedupe.record_inbound(view, id));
    }

    /// Switching active views should rotate the cache and accept ids again in the new view.
    #[test]
    fn dedupe_state_rotates_on_view_change() {
        let legacy_view = ClusterViewId::legacy_default();
        let next_view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1);
        let id = Uuid::new_v4();
        let mut dedupe = GossipDedupeState::new(legacy_view);

        assert!(dedupe.record_inbound(legacy_view, id));
        assert!(!dedupe.record_inbound(legacy_view, id));
        assert!(dedupe.record_inbound(next_view, id));
    }

    /// Rotating fanout sampling should cover all peers over successive ticks.
    #[tokio::test]
    async fn fanout_sample_rotates_across_population() {
        let mut expected = HashSet::new();
        let mut peers = Vec::new();
        for idx in 0..5 {
            let id = Uuid::new_v4();
            expected.insert(id);
            peers.push(PeerHandle {
                id,
                hostname: format!("peer-{idx}"),
                address: format!("127.0.0.1:{}", 5000 + idx),
                root_hash: String::new(),
                noise_static_pub: PublicKey::from([idx as u8; 32]),
            });
        }
        let provider = StaticPeerProvider { peers };

        let mut cursor = 0usize;
        let mut seen = HashSet::new();
        for _ in 0..3 {
            let selected = fanout_sample(&provider, 2, &mut cursor).await;
            for peer in selected {
                seen.insert(peer.id);
            }
        }

        assert_eq!(seen, expected);
    }

    /// Relayed topology join events should never carry imported server capabilities.
    #[test]
    fn message_for_forwarding_strips_join_client_capability() {
        let message = Message::Topology {
            id: Uuid::new_v4(),
            event: TopologyEvent::Join {
                id: Uuid::new_v4(),
                hostname: "peer-a".to_string(),
                address: "127.0.0.1:1234".to_string(),
                root_hash: String::new(),
                incarnation: 1,
                client: None,
                noise_static_pub: PublicKey::from([7u8; 32]),
                signing_pub: Box::new(
                    ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]).verifying_key(),
                ),
                identity_sig: vec![0u8; 64],
                wireguard: None,
            },
        };

        let forwarded = message_for_forwarding(&message).expect("forwarded message");
        match forwarded {
            Message::Topology {
                event: TopologyEvent::Join { client, .. },
                ..
            } => assert!(client.is_none()),
            _ => panic!("unexpected forwarded message variant"),
        }
    }

    /// Task coalescing should keep the causally newest upsert and drop stale updates.
    #[test]
    fn coalesce_pending_messages_keeps_newest_task_upsert() {
        let task_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let now = Utc::now();

        let newer = TaskSpec {
            id: task_id,
            name: "task".to_string(),
            image: "img".to_string(),
            state: ContainerState::Running,
            phase_reason: None,
            phase_progress: None,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
            command: Vec::new(),
            node_id,
            node_name: "node".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            task_epoch: 4,
            phase_version: 7,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };

        let stale = TaskSpec {
            id: task_id,
            name: "task".to_string(),
            image: "img".to_string(),
            state: ContainerState::Pending,
            phase_reason: None,
            phase_progress: None,
            created_at: now.to_rfc3339(),
            updated_at: (now + ChronoDuration::seconds(60)).to_rfc3339(),
            command: Vec::new(),
            node_id,
            node_name: "node".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            task_epoch: 4,
            phase_version: 6,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };

        let pending = vec![
            Message::Task {
                id: Uuid::new_v4(),
                event: TaskEvent::Upsert(Box::new(newer.clone())),
            },
            Message::Task {
                id: Uuid::new_v4(),
                event: TaskEvent::Upsert(Box::new(stale)),
            },
        ];

        let (coalesced, dropped) = coalesce_pending_messages(pending);
        assert_eq!(dropped, 1);
        assert_eq!(coalesced.len(), 1);
        match &coalesced[0] {
            Message::Task {
                event: TaskEvent::Upsert(spec),
                ..
            } => {
                assert_eq!(spec.state, ContainerState::Running);
                assert_eq!(spec.phase_version, 7);
            }
            _ => panic!("unexpected coalesced message variant"),
        }
    }

    /// Task coalescing should keep one remove event for a task and drop intermediate upserts.
    #[test]
    fn coalesce_pending_messages_prefers_task_remove() {
        let task_id = Uuid::new_v4();
        let now = Utc::now();
        let upsert = TaskSpec {
            id: task_id,
            name: "task".to_string(),
            image: "img".to_string(),
            state: ContainerState::Stopping,
            phase_reason: None,
            phase_progress: None,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
            command: Vec::new(),
            node_id: Uuid::new_v4(),
            node_name: "node".to_string(),
            slot_ids: vec![1],
            slot_id: Some(1),
            cpu_millis: 100,
            memory_bytes: 64 * 1024 * 1024,
            gpu_count: 0,
            gpu_device_ids: Vec::new(),
            restart_policy: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            networks: Vec::new(),
            service_metadata: None,
            task_epoch: 2,
            phase_version: 9,
            launch_attempt: 0,
            last_terminal_observed_launch: None,
        };

        let pending = vec![
            Message::Task {
                id: Uuid::new_v4(),
                event: TaskEvent::Upsert(Box::new(upsert)),
            },
            Message::Task {
                id: Uuid::new_v4(),
                event: TaskEvent::Remove { id: task_id },
            },
        ];

        let (coalesced, dropped) = coalesce_pending_messages(pending);
        assert_eq!(dropped, 1);
        assert_eq!(coalesced.len(), 1);
        assert!(matches!(
            coalesced[0],
            Message::Task {
                event: TaskEvent::Remove { .. },
                ..
            }
        ));
    }

    /// Burst task lifecycle chatter should collapse to one causally newest task update.
    #[test]
    fn coalesce_pending_messages_collapses_many_task_phase_updates() {
        let task_id = Uuid::new_v4();
        let node_id = Uuid::new_v4();
        let now = Utc::now();
        let mut pending = Vec::new();

        for phase_version in 1..=16u64 {
            let state = if phase_version >= 16 {
                ContainerState::Running
            } else if phase_version >= 11 {
                ContainerState::Creating
            } else if phase_version >= 6 {
                ContainerState::Pulling
            } else {
                ContainerState::Pending
            };

            let spec = TaskSpec {
                id: task_id,
                name: "task".to_string(),
                image: "img".to_string(),
                state,
                phase_reason: None,
                phase_progress: None,
                created_at: now.to_rfc3339(),
                updated_at: (now + ChronoDuration::seconds(phase_version as i64)).to_rfc3339(),
                command: Vec::new(),
                node_id,
                node_name: "node".to_string(),
                slot_ids: vec![1],
                slot_id: Some(1),
                cpu_millis: 100,
                memory_bytes: 64 * 1024 * 1024,
                gpu_count: 0,
                gpu_device_ids: Vec::new(),
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                service_metadata: None,
                task_epoch: 2,
                phase_version,
                launch_attempt: 0,
                last_terminal_observed_launch: None,
            };
            pending.push(Message::Task {
                id: Uuid::new_v4(),
                event: TaskEvent::Upsert(Box::new(spec)),
            });
        }

        pending.push(Message::Void { id: Uuid::new_v4() });

        let (coalesced, dropped) = coalesce_pending_messages(pending);
        assert_eq!(dropped, 15);
        assert_eq!(coalesced.len(), 2);

        let newest_task = coalesced
            .iter()
            .find_map(|message| match message {
                Message::Task {
                    event: TaskEvent::Upsert(spec),
                    ..
                } => Some(spec),
                _ => None,
            })
            .expect("coalesced batch should keep one task upsert");

        assert_eq!(newest_task.phase_version, 16);
        assert_eq!(newest_task.state, ContainerState::Running);
    }
}
