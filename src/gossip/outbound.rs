use super::plane::{GossipPlane, gossip_plane_for_message};
use super::{DedupeStateHandle, GossipContext, Message};
use crate::agents::service::write_agent_event;
use crate::cluster::ClusterViewId;
use crate::jobs::service::write_job_event;
use crate::network::service::write_network_event;
use crate::scheduler::digest::{
    scheduler_digest_event_node_id, should_replace_scheduler_digest_event,
    write_scheduler_digest_event,
};
use crate::secrets::service::write_secret_event;
use crate::services::service::write_service_event;
use crate::timing::jittered_interval;
use crate::topology;
use crate::topology::PeerHandle;
use crate::topology::TopologyEvent;
use crate::topology::peer_provider::PeerProvider;
use crate::volumes::service::write_volume_event;
use crate::workload::model::{should_replace_workload_event, workload_event_id};
use crate::workload::service as workload_service;
use async_channel::Receiver;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;
use tracing::{debug, error, warn};
use uuid::Uuid;

pub const DEFAULT_FANOUT: usize = 5;
/// Default max messages processed in one outbound dispatch slice per gossip tick.
///
/// This stays separate from the per-RPC cap so one tick can batch enough work to
/// amortize transport encryption while still keeping memory and fanout loops bounded.
const DEFAULT_GOSSIP_DISPATCH_BATCH_MAX: usize = 128;
/// Default max message count per outbound gossip RPC call.
const DEFAULT_GOSSIP_RPC_BATCH_MAX: usize = DEFAULT_GOSSIP_DISPATCH_BATCH_MAX;
/// Default number of peer sends allowed concurrently within one outbound dispatch batch.
const DEFAULT_GOSSIP_SEND_PARALLELISM: usize = 1;
/// Process-wide counter tracking how many outbound workload gossip updates were coalesced.
static GOSSIP_COALESCED_TASK_UPDATES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Coalesces one pending outbound gossip batch by task id and digest node id.
///
/// This keeps at most one task lifecycle update per task id and one scheduler digest event per
/// node for each flush tick, selecting the causally newest state so we reduce burst chatter and
/// avoid propagating stale shortlist metadata.
pub(super) fn coalesce_pending_messages(pending: Vec<Message>) -> (Vec<Message>, usize) {
    let mut coalesced = Vec::with_capacity(pending.len());
    let mut task_positions: HashMap<Uuid, usize> = HashMap::new();
    let mut scheduler_digest_positions: HashMap<Uuid, usize> = HashMap::new();
    let mut dropped = 0usize;

    for message in pending {
        if let Some(task_id) = workload_message_id(&message) {
            if let Some(position) = task_positions.get(&task_id).copied() {
                if should_replace_workload_message(&coalesced[position], &message) {
                    coalesced[position] = message;
                }
                dropped = dropped.saturating_add(1);
                continue;
            }

            task_positions.insert(task_id, coalesced.len());
            coalesced.push(message);
            continue;
        }

        if let Some(node_id) = scheduler_digest_message_node_id(&message) {
            if let Some(position) = scheduler_digest_positions.get(&node_id).copied() {
                if should_replace_scheduler_digest_message(&coalesced[position], &message) {
                    coalesced[position] = message;
                }
                dropped = dropped.saturating_add(1);
                continue;
            }

            scheduler_digest_positions.insert(node_id, coalesced.len());
            coalesced.push(message);
            continue;
        }

        coalesced.push(message);
    }

    (coalesced, dropped)
}

/// Returns the logical workload identifier for one gossip message when it
/// carries a workload event.
fn workload_message_id(message: &Message) -> Option<Uuid> {
    match message {
        Message::Workload { event, .. } => Some(workload_event_id(event)),
        _ => None,
    }
}

/// Returns the logical node identifier for one scheduler digest gossip message.
fn scheduler_digest_message_node_id(message: &Message) -> Option<Uuid> {
    match message {
        Message::SchedulerDigest { event, .. } => Some(scheduler_digest_event_node_id(event)),
        _ => None,
    }
}

/// Returns true when the candidate workload message should replace the
/// currently retained one.
fn should_replace_workload_message(current: &Message, candidate: &Message) -> bool {
    let (
        Message::Workload {
            event: current_event,
            ..
        },
        Message::Workload {
            event: candidate_event,
            ..
        },
    ) = (current, candidate)
    else {
        return false;
    };

    should_replace_workload_event(current_event, candidate_event)
}

/// Returns true when the candidate scheduler digest message should replace the retained one.
fn should_replace_scheduler_digest_message(current: &Message, candidate: &Message) -> bool {
    let (
        Message::SchedulerDigest {
            event: current_event,
            ..
        },
        Message::SchedulerDigest {
            event: candidate_event,
            ..
        },
    ) = (current, candidate)
    else {
        return false;
    };

    should_replace_scheduler_digest_event(current_event, candidate_event)
}

/// Reads the optional outbound gossip dispatch slice cap from the environment.
fn gossip_dispatch_batch_max_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GOSSIP_DISPATCH_BATCH_MAX")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
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

/// Receives locally-produced messages and periodically gossips them to selected peers.
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
    let dispatch_batch_max = gossip_dispatch_batch_max_from_env(DEFAULT_GOSSIP_DISPATCH_BATCH_MAX);
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
                    ticker.reset_after(jittered_interval(tick));
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
                        "coalesced outbound workload gossip updates"
                    );
                    if should_emit_diag_sample(total_coalesced_task_updates) {
                        warn!(
                            target: "diag.gossip.coalesce",
                            before_count,
                            after_count = pending.len(),
                            coalesced_task_updates,
                            total_coalesced_task_updates,
                            "coalesced outbound workload gossip updates sampled"
                        );
                    }
                }

                let (view_scoped, global_metadata) = split_messages_by_plane(pending);
                let options = DispatchOptions {
                    fanout,
                    dispatch_batch_max,
                    rpc_batch_max,
                    send_parallelism,
                    send_timeout,
                };
                dispatch_gossip_plane(
                    &context,
                    view_scoped,
                    GossipPlane::ViewScoped,
                    &mut fanout_cursor,
                    options,
                )
                .await;
                dispatch_gossip_plane(
                    &context,
                    global_metadata,
                    GossipPlane::GlobalMetadata,
                    &mut fanout_cursor,
                    options,
                )
                .await;
                ticker.reset_after(jittered_interval(tick));
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

#[derive(Clone, Copy)]
struct DispatchOptions {
    fanout: Option<usize>,
    dispatch_batch_max: usize,
    rpc_batch_max: usize,
    send_parallelism: usize,
    send_timeout: Option<Duration>,
}

/// Dispatches one plane-specific outbound gossip batch to the selected peers.
///
/// The plane controls both peer selection (view-scoped vs unscoped) and capability
/// resolution strategy, while preserving the same bounded fanout and chunking behavior.
async fn dispatch_gossip_plane<C>(
    context: &C,
    pending: Vec<Message>,
    plane: GossipPlane,
    fanout_cursor: &mut usize,
    options: DispatchOptions,
) where
    C: GossipContext + ?Sized,
{
    if pending.is_empty() {
        return;
    }

    let peers = match plane {
        GossipPlane::ViewScoped => match options.fanout {
            Some(0) => context.get_peers().await,
            Some(n) => select_fanout_window(context.get_warm_peers(n).await, n, fanout_cursor),
            None => select_fanout_window(
                context.get_warm_peers(DEFAULT_FANOUT).await,
                DEFAULT_FANOUT,
                fanout_cursor,
            ),
        },
        GossipPlane::GlobalMetadata => {
            let peer_population = context.get_peers_unscoped().await;
            match options.fanout {
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

    let total_batches = pending.len().div_ceil(options.dispatch_batch_max);
    for (batch_idx, batch) in pending.chunks(options.dispatch_batch_max).enumerate() {
        debug!(
            target: "gossip",
            cluster_view = %cluster_view,
            gossip_plane = plane.as_str(),
            batch = batch_idx + 1,
            total_batches,
            message_count = batch.len(),
            dispatch_batch_max = options.dispatch_batch_max,
            rpc_batch_max = options.rpc_batch_max,
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
                options.rpc_batch_max,
                options.send_timeout,
                cluster_view,
                plane,
            ));
            if inflight.len() >= options.send_parallelism {
                let _ = inflight.next().await;
            }
        }
        while inflight.next().await.is_some() {}
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
            Message::Workload { event, .. } => {
                workload_service::add_event(&mut msgs, idx as u32, event);
            }
            Message::Job { event, .. } => {
                let job_builder = builder.init_job();
                write_job_event(job_builder, event.as_ref())?;
            }
            Message::Agent { event, .. } => {
                let agent_builder = builder.init_agent();
                write_agent_event(agent_builder, event.as_ref())?;
            }
            Message::Service { event, .. } => {
                let service_builder = builder.init_service();
                write_service_event(service_builder, event.as_ref())?;
            }
            Message::Network { event, .. } => {
                let network_builder = builder.init_network();
                write_network_event(network_builder, event)?;
            }
            Message::Secret { event, .. } => {
                let secret_builder = builder.init_secret();
                write_secret_event(secret_builder, event)?;
            }
            Message::Volume { event, .. } => {
                let volume_builder = builder.init_volume();
                write_volume_event(volume_builder, event)?;
            }
            Message::SchedulerDigest { event, .. } => {
                let digest_builder = builder.init_scheduler_digest();
                write_scheduler_digest_event(digest_builder, event)?;
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
    count <= 3 || count.is_power_of_two() || count.is_multiple_of(100)
}

// Return true when the gossip message is about the provided peer identifier.
fn message_targets_peer(message: &Message, peer_id: Uuid) -> bool {
    match message {
        Message::Void { .. } => false,
        Message::Topology { event, .. } => match event {
            TopologyEvent::Join { id, .. }
            | TopologyEvent::Leave { id, .. }
            | TopologyEvent::Alive { id, .. }
            | TopologyEvent::Suspect { id, .. }
            | TopologyEvent::Down { id, .. }
            | TopologyEvent::NodeSchedulingUpdated { id, .. }
            | TopologyEvent::NodeLabelsUpdated { id, .. } => *id == peer_id,
            TopologyEvent::ClusterNameUpdated { .. } => false,
        },
        // Task updates replicate to every peer regardless of assignment so keep them.
        Message::Workload { .. } => false,
        Message::Job { .. } => false,
        Message::Agent { .. } => false,
        Message::Service { .. } => false,
        Message::Network { .. } => false,
        Message::Secret { .. } => false,
        Message::Volume { .. } => false,
        Message::SchedulerDigest { event, .. } => scheduler_digest_event_node_id(event) == peer_id,
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
