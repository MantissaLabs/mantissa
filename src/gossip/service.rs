use super::dedupe::GossipDedupeState;
use super::plane::{gossip_plane_for_wire_message, should_relay_inbound_message};
use super::{DedupeStateHandle, Message};
use crate::agents::service::read_agent_event;
use crate::cluster::{ClusterViewId, ClusterViewState};
use crate::jobs::service::read_job_event;
use crate::network::service::read_network_event;
use crate::scheduler::digest::read_scheduler_digest_event;
use crate::secrets::service::read_secret_event;
use crate::services::service::read_service_event;
use crate::store::secret_master_key_store::read_secret_master_key_sync_record;
use crate::topology;
use crate::topology::TopologyEvent;
use crate::volumes::service::read_volume_event;
use crate::workload::service as workload_service;
use async_channel::{Sender, TrySendError};
use capnp::Error;
use mantissa_protocol::gossip;
use mantissa_protocol::gossip::gossip_message::Which::*;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, warn};
use uuid::Uuid;

/// Represents the gossip RPC ingress service.
pub struct Gossip {
    pub chans: Channels,
    pub cluster_view: ClusterViewState,
    dedupe_state: DedupeStateHandle,
}

/// Sender routes used to fan inbound gossip into domain-specific actors.
pub struct Channels {
    pub topology_events: Sender<Message>,
    pub workload_events: Sender<Message>,
    pub job_events: Sender<Message>,
    pub agent_events: Sender<Message>,
    pub service_events: Sender<Message>,
    pub network_events: Sender<Message>,
    pub secret_events: Sender<Message>,
    pub secret_master_key_events: Sender<Message>,
    pub volume_events: Sender<Message>,
    pub scheduler_digest_events: Sender<Message>,
    /// Shared outbound queue so newly received gossip can be forwarded to additional peers.
    pub outbound_events: Sender<Message>,
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
                            crate::observability::metrics::record_gossip_drop("invalid_id");
                            eprintln!("Invalid gossip id length");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    crate::observability::metrics::record_gossip_drop("missing_id");
                    eprintln!("Missing gossip id: {e}");
                    continue;
                }
            };
            let message_view = match msg.get_view() {
                Ok(view) => match ClusterViewId::from_capnp(view) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        crate::observability::metrics::record_gossip_drop("invalid_view");
                        debug!(
                            target: "gossip",
                            gossip_id = %id,
                            "dropping gossip message with invalid cluster view: {err}"
                        );
                        continue;
                    }
                },
                Err(_) => {
                    crate::observability::metrics::record_gossip_drop("missing_view");
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
                crate::observability::metrics::record_gossip_drop("wrong_view");
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
                    crate::observability::metrics::record_gossip_drop("duplicate");
                    debug!(
                        target: "gossip",
                        gossip_id = %id,
                        view = %active_view,
                        "dropping duplicate gossip message"
                    );
                    continue;
                }
            }
            let which = match msg.reborrow().which() {
                Ok(which) => which,
                Err(err) => {
                    crate::observability::metrics::record_gossip_drop("unreadable_variant");
                    debug!(
                        target: "gossip",
                        gossip_id = %id,
                        "dropping gossip message with unreadable variant: {err}"
                    );
                    continue;
                }
            };
            let message_type = match &which {
                Void(_) => "void",
                Topology(_) => "topology",
                Workload(_) => "workload",
                Job(_) => "job",
                Agent(_) => "agent",
                Service(_) => "service",
                Network(_) => "network",
                Secret(_) => "secret",
                SecretMasterKey(_) => "secret_master_key",
                Volume(_) => "volume",
                SchedulerDigest(_) => "scheduler_digest",
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
                        forward_inbound_message(
                            &self.chans.outbound_events,
                            message_for_forwarding(&message),
                        );
                    }
                    let _ = self.chans.topology_events.send(message).await;
                }
                Topology(Ok(reader)) => match topology::read_topology_event(reader) {
                    Ok(event) => {
                        let message = Message::Topology { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans
                            .topology_events
                            .send(message)
                            .await
                            .map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't sent event to topology: {e}"
                                ))
                            })?;
                    }
                    Err(e) => eprintln!("Failed to convert topology event: {e}"),
                },
                Topology(Err(e)) => {
                    eprintln!("Error reading topology: {e}");
                }
                Workload(Ok(reader)) => match workload_service::read_event(reader) {
                    Ok(event) => {
                        let message = Message::Workload { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans
                            .workload_events
                            .send(message)
                            .await
                            .map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't send event to workload: {e}"
                                ))
                            })?;
                    }
                    Err(e) => eprintln!("Failed to convert workload event: {e}"),
                },
                Workload(Err(e)) => {
                    eprintln!("Error reading workload: {e}");
                }
                Job(Ok(reader)) => match read_job_event(reader) {
                    Ok(event) => {
                        let message = Message::Job {
                            id,
                            event: Box::new(event),
                        };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans.job_events.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to jobs: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert job event: {e}"),
                },
                Job(Err(e)) => {
                    eprintln!("Error reading job: {e}");
                }
                Agent(Ok(reader)) => match read_agent_event(reader) {
                    Ok(event) => {
                        let message = Message::Agent {
                            id,
                            event: Box::new(event),
                        };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans.agent_events.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to agents: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert agent event: {e}"),
                },
                Agent(Err(e)) => {
                    eprintln!("Error reading agent: {e}");
                }
                Service(Ok(reader)) => match read_service_event(reader) {
                    Ok(event) => {
                        let message = Message::Service {
                            id,
                            event: Box::new(event),
                        };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans.service_events.send(message).await.map_err(|e| {
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
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans.network_events.send(message).await.map_err(|e| {
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
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans.secret_events.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to secrets: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert secret event: {e}"),
                },
                Secret(Err(e)) => {
                    eprintln!("Error reading secret: {e}");
                }
                SecretMasterKey(Ok(reader)) => match read_secret_master_key_sync_record(reader) {
                    Ok(record) => {
                        let message = Message::SecretMasterKey { id, record };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans
                            .secret_master_key_events
                            .send(message)
                            .await
                            .map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't send event to secret master keys: {e}"
                                ))
                            })?;
                    }
                    Err(e) => eprintln!("Failed to convert secret master-key event: {e}"),
                },
                SecretMasterKey(Err(e)) => {
                    eprintln!("Error reading secret master key: {e}");
                }
                Volume(Ok(reader)) => match read_volume_event(reader) {
                    Ok(event) => {
                        let message = Message::Volume { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans.volume_events.send(message).await.map_err(|e| {
                            capnp::Error::failed(format!("Couldn't send event to volumes: {e}"))
                        })?;
                    }
                    Err(e) => eprintln!("Failed to convert volume event: {e}"),
                },
                Volume(Err(e)) => {
                    eprintln!("Error reading volume: {e}");
                }
                SchedulerDigest(Ok(reader)) => match read_scheduler_digest_event(reader) {
                    Ok(event) => {
                        let message = Message::SchedulerDigest { id, event };
                        if should_relay_inbound_message(relay_inbound, &message) {
                            forward_inbound_message(
                                &self.chans.outbound_events,
                                message_for_forwarding(&message),
                            );
                        }
                        self.chans
                            .scheduler_digest_events
                            .send(message)
                            .await
                            .map_err(|e| {
                                capnp::Error::failed(format!(
                                    "Couldn't send event to scheduler digests: {e}"
                                ))
                            })?;
                    }
                    Err(e) => eprintln!("Failed to convert scheduler digest event: {e}"),
                },
                SchedulerDigest(Err(e)) => {
                    eprintln!("Error reading scheduler digest: {e}");
                }
            }
        }
        Ok(())
    }
}

/// Reads whether inbound gossip should be relayed into the outbound queue.
///
/// Disabled by default to avoid amplifying high-volume workload update streams.
fn gossip_relay_inbound_from_env() -> bool {
    std::env::var("MANTISSA_GOSSIP_RELAY_INBOUND")
        .ok()
        .map(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
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
            crate::observability::metrics::record_gossip_drop("relay_queue_full");
            debug!(
                target: "gossip",
                gossip_id = %message_id,
                "dropping inbound gossip relay due full outbound queue"
            );
        }
        Err(TrySendError::Closed(_)) => {
            crate::observability::metrics::record_gossip_drop("relay_queue_closed");
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
pub(super) fn message_for_forwarding(message: &Message) -> Option<Message> {
    match message {
        Message::Void { .. } => None,
        Message::Topology { id, event } => {
            let forwarded_event = match event {
                TopologyEvent::Join {
                    id: peer_id,
                    hostname,
                    address,
                    platform_os,
                    platform_arch,
                    root_hash,
                    incarnation,
                    client: _,
                    noise_static_pub,
                    signing_pub,
                    identity_sig,
                    wireguard,
                    scheduling,
                    labels,
                    runtime_support,
                    root_schema,
                } => TopologyEvent::Join {
                    id: *peer_id,
                    hostname: hostname.clone(),
                    address: address.clone(),
                    platform_os: platform_os.clone(),
                    platform_arch: platform_arch.clone(),
                    root_hash: root_hash.clone(),
                    incarnation: *incarnation,
                    client: None,
                    noise_static_pub: *noise_static_pub,
                    signing_pub: signing_pub.clone(),
                    identity_sig: identity_sig.clone(),
                    wireguard: wireguard.clone(),
                    scheduling: scheduling.clone(),
                    labels: labels.clone(),
                    runtime_support: runtime_support.clone(),
                    root_schema: *root_schema,
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
