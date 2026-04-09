use super::builders::{
    ClusterViewSummaryRow, JoinPayload, ListedNodeRow, SplitCandidateRow, add_event,
    drain_state_from_scheduling, write_cluster_view_summary_row, write_join_payload_to_node_info,
    write_listed_node_row, write_node_drain_status, write_split_candidate_row,
};
use super::{Topology, TopologyEvent};
use crate::cluster::operations::{ClusterOperationKind, ClusterOperationStage};
use crate::cluster::{ClusterId, ClusterViewId};
use crate::config;
use crate::node::address::extract_port;
use crate::node::id::read_node_id;
use crate::node::identity::pubkey_from_slice;
use crate::runtime::types::RuntimeSupportProfile;
use crate::server::credential::ClusterCredential;
use crate::store::local::{LocalCredentialStore, LocalSessionStore, MasterKeyRecord};
use crate::store::peer_store::PeersStore;
use crate::sync::SyncTraceContext;
use crate::topology::health::status_to_node_status;
use crate::topology::peers::{
    PeerLabel, PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue,
    labels_from_node_info, runtime_support_from_node_info,
};
use capnp::Error;
use capnp::data;
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::VerifyingKey;
use protocol::server::{self, cluster_session};
use protocol::topology::{topology, topology_event};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use tracing::{info, warn};
use uuid::Uuid;

use super::cluster_operations::SplitOperationBuildInput;

struct JoinInputs {
    anchor: String,
    join_token: String,
}

impl JoinInputs {
    fn from_params(params: topology::JoinParams) -> Result<Self, Error> {
        let request = params.get()?.get_request()?;
        let anchor = request
            .get_anchor()?
            .to_string()
            .expect("expected anchor address");
        let join_token = request
            .get_join_token()?
            .to_string()
            .expect("expected join token");

        Ok(Self { anchor, join_token })
    }
}

/// Builds the durable peer-store row that represents one local join payload.
///
/// This keeps the join path consistent across the local self-row restore and the
/// anchor-side registration flow.
fn peer_value_from_join_payload(payload: &JoinPayload) -> PeerValue {
    PeerValue {
        address: payload.advertise_addr.clone(),
        hostname: payload.hostname.clone(),
        noise_static_pub: payload.public_key,
        signing_pub: payload.signing_key,
        identity_sig: payload.identity_sig.to_vec(),
        wireguard: payload.wireguard.clone(),
        scheduling: payload.scheduling.clone(),
        labels: payload.labels.clone(),
        runtime_support: payload.runtime_support.clone(),
        membership: PeerMembership::active(payload.incarnation),
    }
}

/// Builds the local peer row to restore after join without clobbering newer runtime metadata.
///
/// The join path intentionally starts from a conservative self-row snapshot captured before the
/// node has necessarily finished publishing dynamic local state like WireGuard readiness. When
/// reasserting the self row after bootstrap sync, preserve any better local runtime metadata that
/// already won the local MVReg register.
fn restored_local_peer_value(current: Option<&PeerValue>, mut restored: PeerValue) -> PeerValue {
    if let Some(current) = current {
        restored.wireguard =
            WireGuardPeerValue::preferred(current.wireguard.as_ref(), restored.wireguard.as_ref());
        restored.scheduling = PeerSchedulingState::merge(&restored.scheduling, &current.scheduling);
        restored.labels = PeerLabelState::merge(&restored.labels, &current.labels);
        restored.runtime_support = RuntimeSupportProfile::preferred(
            Some(&restored.runtime_support),
            Some(&current.runtime_support),
        )
        .unwrap_or_default();
    }
    restored
}

struct JoinResponse {
    peer_id: Uuid,
    peer_value: PeerValue,
    peer_incarnation: u64,
    ticket: Vec<u8>,
    credential: Vec<u8>,
    session: cluster_session::Client,
}

impl Topology {
    fn build_join_payload(&self) -> Result<JoinPayload, Error> {
        let server_handle = self
            .get_server_handle()
            .ok_or_else(|| Error::failed("server handle not set".into()))?;

        let advertise_addr = self
            .compute_advertise_addr()
            .map_err(|e| Error::failed(format!("failed to compute advertise addr: {e}")))?;
        let preferred_wireguard_port = extract_port(&advertise_addr).ok();

        let hostname = self
            .local
            .node
            .system_info
            .info
            .hostname
            .clone()
            .ok_or_else(|| Error::failed("hostname not set".into()))?;

        let wireguard = if !config::wireguard_enabled() || !net::paths::running_as_root() {
            None
        } else {
            match net::wireguard::resolve_wireguard_key_path()
                .and_then(net::wireguard::load_or_generate_wireguard_keys)
            {
                Ok(keys) => {
                    match net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
                        preferred_wireguard_port,
                        config::wireguard_port_override(),
                    ) {
                        Ok(port) => Some(WireGuardPeerValue {
                            public_key: keys.public_bytes(),
                            port,
                            enabled: false,
                        }),
                        Err(err) => {
                            tracing::warn!(
                                target: "topology",
                                "failed to resolve WireGuard listen port: {err}"
                            );
                            None
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(target: "topology", "failed to load WireGuard keys: {err}");
                    None
                }
            }
        };

        Ok(JoinPayload {
            id: self.local.node.id,
            hostname,
            advertise_addr,
            incarnation: self.swim_local_incarnation(),
            server_handle,
            public_key: self.local.public_key.to_bytes(),
            signing_key: self.local.signing_key.verifying_key().to_bytes(),
            identity_sig: crate::node::identity::sign_peer_identity(
                &self.local.signing_key,
                &self.local.node.id,
                &self.local.public_key.to_bytes(),
                &self.local.signing_key.verifying_key().to_bytes(),
            ),
            wireguard,
            scheduling: self.current_scheduling_state(),
            labels: self.current_label_state(),
            runtime_support: self.local.runtime_support.clone(),
        })
    }

    async fn register_with_anchor(
        client: server::Client,
        payload: &JoinPayload,
        cluster_view: ClusterViewId,
        join_token: &str,
    ) -> Result<JoinResponse, Error> {
        let mut request = client.register_node_request();
        write_join_payload_to_node_info(request.get().init_info(), payload, cluster_view);
        request.get().set_token(join_token);

        let response = request.send().promise.await?;
        let resp = response.get()?;

        let session = resp.get_session()?;
        let ticket = resp.get_ticket()?.to_vec();
        let credential = resp.get_credential()?.to_vec();
        let node_info = resp.get_node_info()?;
        let peer_id = read_node_id(node_info.get_id()?)?;
        let peer_incarnation = node_info.get_incarnation();
        let peer_value = PeerValue::from_node_info(peer_id, node_info)?;

        Ok(JoinResponse {
            peer_id,
            peer_value,
            peer_incarnation,
            ticket,
            credential,
            session,
        })
    }

    async fn persist_join_state(
        peers: &PeersStore,
        local_sessions: &LocalSessionStore,
        local_creds: &LocalCredentialStore,
        peer_id: Uuid,
        peer_value: &PeerValue,
        ticket: &[u8],
        credential: &[u8],
    ) -> Result<(), Error> {
        if let Err(e) = peers
            .upsert(&UuidKey::from(peer_id), peer_value.clone())
            .await
        {
            log::warn!(target: "topology", "join: upsert of anchor NodeInfo failed: {e}");
        }

        local_sessions
            .put(peer_id, ticket)
            .map_err(|e| Error::failed(format!("ticket persist failed: {e}")))?;

        local_creds
            .put(peer_id, credential)
            .map_err(|e| Error::failed(format!("credential persist failed: {e}")))?;

        Ok(())
    }

    /// Restores this node's own peer row after a successful join so rejoin does not depend on
    /// remote gossip to clear the self tombstone created by `leave()`.
    async fn persist_local_join_payload(&self, payload: &JoinPayload) -> Result<(), Error> {
        let current = self.deps.registry.peer_value_unscoped(payload.id);
        let local_peer_value =
            restored_local_peer_value(current.as_ref(), peer_value_from_join_payload(payload));
        self.stores
            .peers
            .upsert(&UuidKey::from(payload.id), local_peer_value)
            .await
            .map_err(|err| Error::failed(format!("local join upsert failed: {err}")))?;
        self.swim_record_join(payload.id, payload.incarnation);
        Ok(())
    }

    /// Retrieves and installs the cluster master key returned by the anchor during join.
    async fn install_master_key_from_anchor(
        &self,
        session: cluster_session::Client,
    ) -> Result<(), Error> {
        let request = session.get_secrets_request();
        let response = request.send().promise.await?;
        let secrets_client = response.get()?.get_secrets()?;

        let mk_request = secrets_client.get_master_key_request();
        let mk_response = mk_request.send().promise.await?;
        let envelope = mk_response.get()?.get_envelope()?;

        let version = envelope.get_version();
        let key_bytes = envelope.get_key()?;
        if key_bytes.len() != 32 {
            return Err(Error::failed(
                "anchor provided master key with invalid length".to_string(),
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(key_bytes);

        let record = MasterKeyRecord::new(version, key)
            .map_err(|e| Error::failed(format!("invalid master key payload: {e}")))?;

        self.stores
            .secret_master_store
            .import_current(&record)
            .map_err(|e| Error::failed(format!("failed to persist master key: {e}")))?;

        {
            let guard = self.stores.secret_keyring.write().await;
            guard.install_current(record);
        }

        Ok(())
    }

    /// Broadcasts one topology event immediately to the currently known active peers.
    async fn broadcast_topology_event_now(&self, event: &TopologyEvent) {
        let Some(snapshot) = self.peer_snapshot().await else {
            return;
        };
        let cluster_view = self.active_cluster_view();

        for entry in snapshot.entries.iter() {
            if entry.peer_id == self.local.node.id {
                continue;
            }

            let gossip_cap = match self
                .deps
                .registry
                .gossip_client_for(entry.peer_id, cluster_view)
                .await
            {
                Ok(Some(cap)) => cap,
                Ok(None) => continue,
                Err(err) => {
                    warn!(
                        target: "topology",
                        peer_id = %entry.peer_id,
                        "leave: failed to resolve gossip capability for immediate broadcast: {err}"
                    );
                    continue;
                }
            };

            let mut request = gossip_cap.gossip_request();
            let list = request.get().init_messages();
            let mut messages = list.init_messages(1);
            let mut message = messages.reborrow().get(0);
            message.set_id(Uuid::new_v4().as_bytes());
            cluster_view.write_capnp(message.reborrow().init_view());
            add_event(&mut messages, 0, event, cluster_view);

            if let Err(err) = request.send().promise.await {
                warn!(
                    target: "topology",
                    peer_id = %entry.peer_id,
                    "leave: immediate topology broadcast failed: {err}"
                );
            }
        }
    }
}

impl topology::Server for Topology {
    /// Join the cluster and adds our client handle to the `Memberlist`
    /// Returns an instance of `Membership` to the caller to track its
    /// status.
    async fn join(
        self: Rc<Self>,
        params: topology::JoinParams,
        mut _results: topology::JoinResults,
    ) -> Result<(), Error> {
        let payload = self.build_join_payload()?;

        let self_addr = self.local.advertise.configured().to_string();

        let inputs = JoinInputs::from_params(params)?;

        if inputs.anchor == self_addr {
            return Err(capnp::Error::failed("cannot join own address".to_string()));
        }

        let noise_keys = self.deps.registry.noise_keys();
        let client = client::connection::get_client_secure_join_with_keys(
            &inputs.anchor,
            &inputs.join_token,
            noise_keys.as_ref(),
        )
        .await
        .map_err(|e| {
            let mut msg = e.to_string();
            if let Some(stripped) = msg.strip_prefix("Failed: ") {
                msg = stripped.to_string();
            }
            Error::failed(format!(
                "could not connect to anchor {}: {msg}",
                inputs.anchor
            ))
        })?;
        let anchor_handle = client.clone();

        let response = Topology::register_with_anchor(
            client,
            &payload,
            self.active_cluster_view(),
            &inputs.join_token,
        )
        .await?;

        let JoinResponse {
            peer_id,
            peer_value,
            peer_incarnation,
            ticket,
            credential,
            session,
        } = response;

        self.persist_local_join_payload(&payload).await?;

        Topology::persist_join_state(
            &self.stores.peers,
            &self.stores.local_sessions,
            &self.stores.local_credential_store,
            peer_id,
            &peer_value,
            &ticket,
            &credential,
        )
        .await?;

        self.install_master_key_from_anchor(session.clone()).await?;

        ClusterCredential::from_bytes_verified(&credential).map_err(Error::failed)?;

        self.swim_record_join(peer_id, peer_incarnation);

        self.attach_handle_only(peer_id, anchor_handle).await;

        let sync_cap = {
            let req = session.get_sync_request();
            let resp = req.send().promise.await?;
            resp.get()?.get_sync()?
        };

        let sync_trace = SyncTraceContext::peer(peer_id, peer_value.address.clone(), "join");
        tokio::task::spawn_local({
            let topology = self.clone();
            let cluster_view = self.active_cluster_view();
            let trace = sync_trace;
            let payload = payload.clone();
            async move {
                // Bootstrap immediately from the anchor session so the join path does not wait
                // for the next periodic tick before the new node has a converged view.
                topology
                    .deps
                    .sync
                    .sync_all_domains(sync_cap, cluster_view, Some(trace))
                    .await;

                // A successful rejoin must end with the local node's own peer row restored even
                // if the bootstrap sync observed a stale leave tombstone from another peer.
                if let Err(err) = topology.persist_local_join_payload(&payload).await {
                    warn!(target: "topology", "join: failed to restore local peer row after bootstrap sync: {err}");
                }
                if let Err(err) = topology.publish_local_cluster_node_count().await {
                    warn!(
                        target: "cluster_view",
                        "join: failed to publish local cluster node count after bootstrap sync: {err}"
                    );
                }
            }
        });

        self.ensure_cluster_background_tasks();
        self.sync_once_now();

        Ok(())
    }

    /// Leave the cluster: tombstone *this node* in its local Peers store and
    /// trigger an immediate sync so peers learn about the removal quickly.
    async fn leave(
        self: Rc<Self>,
        _params: topology::LeaveParams,
        _results: topology::LeaveResults,
    ) -> Result<(), capnp::Error> {
        if !self.runtime.sync.is_running() {
            return Err(capnp::Error::failed("node is not part of a cluster".into()));
        }

        let self_id = self.local.node.id;
        let leave_incarnation = self.swim_advance_local_incarnation();
        self.mark_peer_left(self_id, leave_incarnation)
            .await
            .map_err(|e| capnp::Error::failed(format!("leave: mark-left failed: {e}")))?;
        let leave_event = TopologyEvent::Leave {
            id: self_id,
            incarnation: leave_incarnation,
        };
        self.broadcast_topology_event_now(&leave_event).await;
        self.gossip_topology_event(leave_event).await?;

        // Stop all background peer contact before clearing local auth state so the
        // node becomes quiescent immediately after the leave broadcast completes.
        self.stop_cluster_background_tasks();
        self.deps.registry.clear().await;
        self.clear_local_cluster_auth_state();

        Ok(())
    }

    /// List members of the network. Returns a list of nodes with their
    /// relevant information.
    async fn list(
        self: Rc<Self>,
        _params: topology::ListParams,
        mut results: topology::ListResults,
    ) -> Result<(), Error> {
        info!(target: "topology", "Listing nodes");

        let peers = self.stores.peers.clone();
        let health_snapshot = self.deps.health_monitor.snapshot();

        let (actives, _) = peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let local_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let mut scoped_nodes = Vec::<ListedNodeRow>::with_capacity(actives.len());

        for (k, snap) in actives.into_iter() {
            let id = k.to_uuid();
            if excluded_peers.contains(&id) {
                continue;
            }
            let candidate_view = if id == self.local.node.id {
                Some(local_view)
            } else {
                Some(self.best_known_peer_view(id).await.unwrap_or(local_view))
            };
            if candidate_view != Some(local_view) {
                continue;
            }

            // Map health snapshot to NodeStatus.
            let health_status = health_snapshot
                .get(&id)
                .cloned()
                .unwrap_or(::health::Status::Unknown);
            let node_status = status_to_node_status(health_status);
            let Some(value) = PeerValue::select(snap.as_slice()).filter(|value| value.is_active())
            else {
                continue;
            };
            let drain_state = if value.scheduling.drain_requested {
                self.build_node_drain_status(id).await?.state.as_capnp()
            } else {
                drain_state_from_scheduling(&value.scheduling)
            };
            scoped_nodes.push(ListedNodeRow {
                id,
                value,
                health: node_status,
                drain_state,
            });
        }

        scoped_nodes.sort_by_key(|row| row.id);
        let list_builder = results.get().init_nodes();
        let mut node_list = list_builder.init_nodes(scoped_nodes.len() as u32);
        for (index, row) in scoped_nodes.into_iter().enumerate() {
            write_listed_node_row(node_list.reborrow().get(index as u32), &row, local_view);
        }

        Ok(())
    }

    /// Returns the current join token for other nodes to use
    /// to join the cluster from this node.
    async fn show_token(
        self: Rc<Self>,
        _params: topology::ShowTokenParams,
        mut results: topology::ShowTokenResults,
    ) -> Result<(), Error> {
        let token = self.stores.token_store.current_token().await;
        results.get().set_token(&token);
        Ok(())
    }

    /// Rotates the token used to join the cluster.
    async fn rotate_token(
        self: Rc<Self>,
        _params: topology::RotateTokenParams,
        mut results: topology::RotateTokenResults,
    ) -> Result<(), Error> {
        let new_token = self.stores.token_store.rotate_and_persist().await?;
        results.get().set_token(&new_token);
        Ok(())
    }

    /// Returns the local active cluster view. This is currently a single legacy default view.
    async fn get_cluster_view(
        self: Rc<Self>,
        _params: topology::GetClusterViewParams,
        mut results: topology::GetClusterViewResults,
    ) -> Result<(), capnp::Error> {
        self.active_cluster_view()
            .write_capnp(results.get().init_view());
        Ok(())
    }

    /// Lists split candidates for one source view with host details used by interactive planners.
    async fn list_split_candidates(
        self: Rc<Self>,
        params: topology::ListSplitCandidatesParams,
        mut results: topology::ListSplitCandidatesResults,
    ) -> Result<(), capnp::Error> {
        let source_view = ClusterViewId::from_capnp(params.get()?.get_source_view()?)
            .map_err(capnp::Error::failed)?;
        let local_view = self.active_cluster_view();
        if source_view != local_view {
            return Err(capnp::Error::failed(format!(
                "split candidates source view must equal local active view {local_view}"
            )));
        }

        let candidates = self.collect_split_node_candidates(source_view).await?;
        let health_snapshot = self.deps.health_monitor.snapshot();
        let mut rows = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let active_cluster_view = self
                .best_known_peer_view(candidate.node_id)
                .await
                .unwrap_or(local_view);
            rows.push(SplitCandidateRow {
                health: status_to_node_status(
                    health_snapshot
                        .get(&candidate.node_id)
                        .cloned()
                        .unwrap_or(::health::Status::Unknown),
                ),
                active_cluster_view,
                candidate,
            });
        }

        let mut list = results.get().init_nodes(rows.len() as u32);
        for (idx, row) in rows.iter().enumerate() {
            write_split_candidate_row(list.reborrow().get(idx as u32), row);
        }

        Ok(())
    }

    /// Persists one friendly cluster lineage name locally, then gossips the update to peers.
    async fn set_cluster_name(
        self: Rc<Self>,
        params: topology::SetClusterNameParams,
        _results: topology::SetClusterNameResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let cluster_id = Self::cluster_id_from_capnp(request.get_cluster_id()?)?;
        let name = request.get_name()?.to_string()?;
        let updated_at_unix_ms = Self::now_unix_ms();
        let actor_node_id = self.local.node.id;
        let changed = self
            .apply_cluster_name_update(cluster_id, &name, updated_at_unix_ms, actor_node_id)
            .await?;
        if !changed {
            return Ok(());
        }

        self.gossip_topology_event(TopologyEvent::ClusterNameUpdated {
            cluster_id,
            name: name.trim().to_string(),
            updated_at_unix_ms,
            actor_node_id,
        })
        .await?;
        Ok(())
    }

    /// Accepts one relayed cluster-name payload and applies conflict-resolved local persistence only.
    async fn submit_cluster_name(
        self: Rc<Self>,
        params: topology::SubmitClusterNameParams,
        _results: topology::SubmitClusterNameResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let cluster_id = Self::cluster_id_from_capnp(request.get_cluster_id()?)?;
        let name = request.get_name()?.to_string()?;
        let updated_at_unix_ms = request.get_updated_at_unix_ms();
        let actor_node_id = read_node_id(request.get_actor_node_id()?)?;
        let _ = self
            .apply_cluster_name_update(cluster_id, &name, updated_at_unix_ms, actor_node_id)
            .await?;
        Ok(())
    }

    /// Marks one node unschedulable and gossips the maintenance fence cluster-wide.
    async fn drain_node(
        self: Rc<Self>,
        params: topology::DrainNodeParams,
        _results: topology::DrainNodeResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let node_id = read_node_id(request.get_node_id()?)?;
        let reason = request.get_reason()?.to_string()?;
        let drain_task_stop_timeout_secs = match request.get_task_stop_timeout_secs() {
            0 => None,
            value => Some(value),
        };
        self.validate_node_drain_request(node_id)?;
        let scheduling = PeerSchedulingState {
            schedulable: false,
            drain_requested: true,
            updated_at_unix_ms: Topology::now_unix_ms(),
            actor_node_id: self.local.node.id,
            reason: {
                let trimmed = reason.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            },
            drain_task_stop_timeout_secs,
        };
        let changed = self
            .apply_peer_scheduling_update(node_id, scheduling.clone())
            .await?;
        if changed {
            self.gossip_topology_event(TopologyEvent::NodeSchedulingUpdated {
                id: node_id,
                scheduling,
            })
            .await?;
        }
        Ok(())
    }

    /// Clears one node maintenance fence so schedulers may place new work on it again.
    async fn resume_node(
        self: Rc<Self>,
        params: topology::ResumeNodeParams,
        _results: topology::ResumeNodeResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let node_id = read_node_id(request.get_node_id()?)?;
        let scheduling = PeerSchedulingState {
            schedulable: true,
            drain_requested: false,
            updated_at_unix_ms: Topology::now_unix_ms(),
            actor_node_id: self.local.node.id,
            reason: None,
            drain_task_stop_timeout_secs: None,
        };
        let changed = self
            .apply_peer_scheduling_update(node_id, scheduling.clone())
            .await?;
        if changed {
            self.gossip_topology_event(TopologyEvent::NodeSchedulingUpdated {
                id: node_id,
                scheduling,
            })
            .await?;
        }
        Ok(())
    }

    /// Applies operator-managed labels to one node and relays the converged update through gossip.
    async fn set_node_labels(
        self: Rc<Self>,
        params: topology::SetNodeLabelsParams,
        _results: topology::SetNodeLabelsResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let node_id = read_node_id(request.get_node_id()?)?;
        let replace = request.get_replace();
        let Some(current) = self.deps.registry.peer_value_unscoped(node_id) else {
            return Err(capnp::Error::failed(format!(
                "node '{}' not found",
                node_id
            )));
        };

        let labels_reader = request.get_labels()?;
        let remove_reader = request.get_remove_keys()?;
        if !replace && labels_reader.is_empty() && remove_reader.is_empty() {
            return Err(capnp::Error::failed(
                "label update requires at least one label assignment or removal".to_string(),
            ));
        }

        let mut labels = if replace {
            BTreeMap::new()
        } else {
            current
                .labels
                .labels
                .into_iter()
                .map(|label| (label.key, label.value))
                .collect::<BTreeMap<_, _>>()
        };

        for raw in labels_reader.iter() {
            let parsed =
                PeerLabel::parse_assignment(raw?.to_str()?).map_err(capnp::Error::failed)?;
            labels.insert(parsed.key, parsed.value);
        }

        for raw in remove_reader.iter() {
            let key = raw?.to_str()?.trim().to_string();
            if key.is_empty() {
                return Err(capnp::Error::failed(
                    "label remove key must not be empty".to_string(),
                ));
            }
            labels.remove(&key);
        }

        let next = PeerLabelState::new(
            labels
                .into_iter()
                .map(|(key, value)| PeerLabel { key, value })
                .collect(),
            Topology::now_unix_ms(),
            self.local.node.id,
        );
        let changed = self.apply_peer_labels_update(node_id, next.clone()).await?;
        if changed {
            self.gossip_topology_event(TopologyEvent::NodeLabelsUpdated {
                id: node_id,
                labels: next,
            })
            .await?;
        }
        Ok(())
    }

    /// Returns a derived drain progress snapshot for one node so operators can wait safely.
    async fn get_node_drain_status(
        self: Rc<Self>,
        params: topology::GetNodeDrainStatusParams,
        mut results: topology::GetNodeDrainStatusResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let node_id = read_node_id(request.get_node_id()?)?;
        let status = self.build_node_drain_status(node_id).await?;
        write_node_drain_status(results.get().init_status(), &status);
        Ok(())
    }

    /// Registers a merge operation intent and stores it durably for later orchestration stages.
    async fn merge_clusters(
        self: Rc<Self>,
        params: topology::MergeClustersParams,
        mut results: topology::MergeClustersResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_no_active_cluster_operation("start merge operation")?;

        let req = params.get()?.get_req()?;
        let source_view =
            ClusterViewId::from_capnp(req.get_source_view()?).map_err(capnp::Error::failed)?;
        let destination_view =
            ClusterViewId::from_capnp(req.get_destination_view()?).map_err(capnp::Error::failed)?;
        let dry_run = req.get_dry_run();
        let merge_service_policy = Self::merge_service_policy_from_capnp(req.get_service_policy()?);
        let active_view = self.active_cluster_view();

        if source_view == destination_view {
            return Err(capnp::Error::failed(
                "merge request source and destination view must differ".into(),
            ));
        }
        if source_view != active_view && destination_view != active_view {
            return Err(capnp::Error::failed(format!(
                "merge request must include local active view {active_view}"
            )));
        }

        let operation = self.build_merge_operation_record(
            source_view,
            destination_view,
            dry_run,
            merge_service_policy,
        );
        self.persist_and_dispatch_operation(&operation).await?;

        info!(
            target: "cluster_view",
            operation_id = %operation.id,
            source_view = %source_view,
            destination_view = %destination_view,
            merge_service_policy = ?operation.merge_service_policy,
            dry_run,
            stage = ?operation.stage,
            "merge operation accepted"
        );

        operation.write_capnp(results.get().init_op());
        Ok(())
    }

    /// Registers a split operation intent and stores derived target views durably.
    async fn split_cluster(
        self: Rc<Self>,
        params: topology::SplitClusterParams,
        mut results: topology::SplitClusterResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_no_active_cluster_operation("start split operation")?;

        let req = params.get()?.get_req()?;
        let source_view =
            ClusterViewId::from_capnp(req.get_source_view()?).map_err(capnp::Error::failed)?;
        let dry_run = req.get_dry_run();
        let split_service_policy = Self::split_service_policy_from_capnp(req.get_service_policy()?);
        let split_network_policy = Self::split_network_policy_from_capnp(req.get_network_policy()?);
        let active_view = self.active_cluster_view();
        if source_view != active_view {
            return Err(capnp::Error::failed(format!(
                "split request source view must equal local active view {active_view}"
            )));
        }

        let targets = req.get_targets()?;
        let (target_specs, target_views, detail_targets) =
            self.parse_split_target_specs(source_view, targets)?;

        let split_assignments = self
            .build_split_assignments(source_view, &target_specs)
            .await?;
        let operation = self.build_split_operation_record(SplitOperationBuildInput {
            source_view,
            dry_run,
            split_service_policy,
            split_network_policy,
            target_specs: &target_specs,
            target_views,
            detail_targets,
            split_assignments,
        });
        self.persist_and_dispatch_operation(&operation).await?;

        info!(
            target: "cluster_view",
            operation_id = %operation.id,
            source_view = %source_view,
            target_count = operation.target_views.len(),
            split_service_policy = ?operation.split_service_policy,
            split_network_policy = ?operation.split_network_policy,
            dry_run,
            stage = ?operation.stage,
            "split operation accepted"
        );

        operation.write_capnp(results.get().init_op());
        Ok(())
    }

    /// Accepts a relayed operation record and triggers local progression when appropriate.
    async fn submit_cluster_operation(
        self: Rc<Self>,
        params: topology::SubmitClusterOperationParams,
        _results: topology::SubmitClusterOperationResults,
    ) -> Result<(), capnp::Error> {
        let reader = params.get()?;
        let operation_id = Self::operation_id_from_data(reader.get_id()?)?;
        let payload = reader.get_payload()?;
        self.accept_submitted_cluster_operation(operation_id, payload)
            .await
    }

    /// Returns the most recent locally persisted operation record for the requested operation id.
    async fn get_cluster_operation(
        self: Rc<Self>,
        params: topology::GetClusterOperationParams,
        mut results: topology::GetClusterOperationResults,
    ) -> Result<(), capnp::Error> {
        let id = Self::operation_id_from_data(params.get()?.get_id()?)?;
        let operation = self
            .load_cluster_operation(id)?
            .ok_or_else(|| capnp::Error::failed(format!("cluster operation not found: {id}")))?;
        operation.write_capnp(results.get().init_op());
        Ok(())
    }

    /// Lists known cluster views and best-effort member counts.
    async fn list_cluster_views(
        self: Rc<Self>,
        _params: topology::ListClusterViewsParams,
        mut results: topology::ListClusterViewsResults,
    ) -> Result<(), capnp::Error> {
        let local_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let operations = self.load_cluster_operations()?;
        let local_node_count = self.local_cluster_view_member_count().await?;
        let mut retired_views = HashSet::<ClusterViewId>::new();
        for operation in operations.iter() {
            if operation.kind != ClusterOperationKind::Merge
                || operation.dry_run
                || operation.stage == ClusterOperationStage::Aborted
            {
                continue;
            }
            if !matches!(
                operation.stage,
                ClusterOperationStage::Committed | ClusterOperationStage::Finalized
            ) {
                continue;
            }
            for source_view in operation.source_views.iter().copied() {
                retired_views.insert(source_view);
            }
        }

        let mut counts = HashMap::<ClusterViewId, u32>::new();
        counts.insert(local_view, local_node_count);

        let (actives, _) = self
            .stores
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        for (key, snapshot) in actives {
            let peer_id = key.to_uuid();
            if peer_id == self.local.node.id {
                continue;
            }
            if excluded_peers.contains(&peer_id) {
                continue;
            }
            let Some(_selected) =
                PeerValue::select(snapshot.as_slice()).filter(|value| value.is_active())
            else {
                continue;
            };

            // When no cached session is available yet, treat the peer as part of the
            // local active view until a concrete remote view is observed.
            let view = self
                .best_known_peer_view(peer_id)
                .await
                .unwrap_or(local_view);
            if view == local_view {
                continue;
            }
            if retired_views.contains(&view) {
                continue;
            }
            let entry = counts.entry(view).or_insert(0);
            *entry = entry.saturating_add(1);
        }

        // Preserve split sibling discoverability even after peer pruning removes direct sessions.
        // This keeps merge UX simple because both split target clusters stay listable from either side.
        for operation in operations.iter() {
            if operation.kind != ClusterOperationKind::Split
                || operation.dry_run
                || operation.stage == ClusterOperationStage::Aborted
            {
                continue;
            }
            if !operation.target_views.contains(&local_view) {
                continue;
            }

            let mut inferred_target_counts = vec![0u32; operation.target_views.len()];
            for assignment in operation.split_assignments.iter() {
                if let Some(slot) = inferred_target_counts.get_mut(assignment.target_index) {
                    *slot = slot.saturating_add(1);
                }
            }

            for (idx, view) in operation.target_views.iter().copied().enumerate() {
                if retired_views.contains(&view) {
                    continue;
                }
                if view == local_view {
                    continue;
                }
                let inferred_count = inferred_target_counts.get(idx).copied().unwrap_or_default();
                if inferred_count == 0 {
                    continue;
                }
                let entry = counts.entry(view).or_insert(0);
                if *entry < inferred_count {
                    *entry = inferred_count;
                }
            }
        }

        let cluster_metadata_rows = self
            .stores
            .cluster_view_store
            .list_cluster_metadata()
            .map_err(|err| capnp::Error::failed(err.to_string()))?;
        let cluster_names = cluster_metadata_rows
            .iter()
            .filter_map(|(cluster_id, record)| {
                record
                    .name
                    .as_ref()
                    .map(|name| (*cluster_id, name.name.clone()))
            })
            .collect::<HashMap<_, _>>();
        let cluster_node_counts = cluster_metadata_rows
            .into_iter()
            .filter_map(|(cluster_id, record)| {
                record
                    .node_count
                    .map(|node_count| (cluster_id, node_count.node_count))
            })
            .collect::<HashMap<_, _>>();

        let mut rows = counts
            .into_iter()
            .filter_map(|(view, node_count)| {
                let resolved_count = if view == local_view {
                    node_count
                } else {
                    cluster_node_counts
                        .get(&view.cluster_id)
                        .copied()
                        .unwrap_or(node_count)
                };
                if resolved_count == 0 || (view != local_view && retired_views.contains(&view)) {
                    return None;
                }
                Some(ClusterViewSummaryRow {
                    view,
                    node_count: resolved_count,
                    local_active: view == local_view,
                    cluster_name: cluster_names.get(&view.cluster_id).cloned(),
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.view
                .cluster_id
                .as_bytes()
                .cmp(right.view.cluster_id.as_bytes())
                .then(left.view.epoch.cmp(&right.view.epoch))
        });

        let mut list = results.get().init_views(rows.len() as u32);
        for (idx, row) in rows.iter().enumerate() {
            write_cluster_view_summary_row(list.reborrow().get(idx as u32), row);
        }

        Ok(())
    }
}

fn verifying_key_from_data(d: data::Reader<'_>) -> Result<VerifyingKey, capnp::Error> {
    let arr: [u8; 32] = d
        .try_into()
        .map_err(|_| capnp::Error::failed("ed25519 pubkey must be 32 bytes".to_string()))?;

    VerifyingKey::from_bytes(&arr).map_err(|e| capnp::Error::failed(e.to_string()))
}

fn cluster_id_from_topology_event(
    reader: protocol::topology::cluster_id::Reader<'_>,
) -> Result<ClusterId, capnp::Error> {
    let value = reader.get_value()?;
    if value.len() != 16 {
        return Err(capnp::Error::failed(format!(
            "cluster id must be exactly 16 bytes, got {}",
            value.len()
        )));
    }

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(value);
    Ok(ClusterId::from_bytes(bytes))
}

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let event = match reader.get_event()? {
        EventType::Add => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            let pubkey =
                pubkey_from_slice(node.get_public_key()?).expect("Failed to parse public key");
            let signing_pub = verifying_key_from_data(node.get_signing_key()?)?;
            let identity_sig = node.get_identity_sig()?;
            if identity_sig.is_empty() {
                return Err(capnp::Error::failed(
                    "identitySig must be set for peer identity verification".into(),
                ));
            }
            if identity_sig.len() != 64 {
                return Err(capnp::Error::failed(
                    "identitySig must be exactly 64 bytes".into(),
                ));
            }
            let wg_pk_bytes = node.get_wireguard_public_key()?;
            let wireguard = if wg_pk_bytes.is_empty() {
                None
            } else {
                if wg_pk_bytes.len() != 32 {
                    return Err(capnp::Error::failed(
                        "wireguardPublicKey must be exactly 32 bytes".into(),
                    ));
                }
                let mut public_key = [0u8; 32];
                public_key.copy_from_slice(wg_pk_bytes);

                Some(WireGuardPeerValue {
                    public_key,
                    port: node.get_wireguard_port(),
                    enabled: node.get_wireguard_enabled(),
                })
            };
            let client = if node.has_handle() {
                Some(node.get_handle()?)
            } else {
                None
            };
            let scheduling = PeerSchedulingState::from_node_info(
                id,
                node.get_schedulable(),
                node.get_drain_requested(),
                node.get_scheduling_updated_at_unix_ms(),
                {
                    let actor = node.get_scheduling_actor_node_id()?;
                    let bytes = actor.get_bytes()?;
                    if bytes.is_empty() {
                        None
                    } else {
                        Some(
                            Uuid::from_slice(bytes)
                                .map_err(|err| capnp::Error::failed(err.to_string()))?,
                        )
                    }
                },
                Some(node.get_scheduling_reason()?.to_string()?),
                match node.get_drain_task_stop_timeout_secs() {
                    0 => None,
                    value => Some(value),
                },
            );

            TopologyEvent::Join {
                id,
                hostname: node.get_hostname()?.to_str()?.to_string(),
                address: node.get_addr()?.to_str()?.to_string(),
                root_hash: node.get_root_hash()?.to_str()?.to_string(),
                incarnation: node.get_incarnation(),
                client,
                noise_static_pub: pubkey,
                signing_pub: Box::new(signing_pub),
                identity_sig: identity_sig.to_vec(),
                wireguard,
                scheduling: Box::new(scheduling),
                labels: Box::new(labels_from_node_info(node)?),
                runtime_support: Box::new(runtime_support_from_node_info(node)?),
            }
        }
        EventType::Remove => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Leave {
                id,
                incarnation: node.get_incarnation(),
            }
        }
        EventType::Alive => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Alive {
                id,
                incarnation: node.get_incarnation(),
            }
        }
        EventType::Suspect => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Suspect {
                id,
                incarnation: node.get_incarnation(),
            }
        }
        EventType::Down => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Down {
                id,
                incarnation: node.get_incarnation(),
            }
        }
        EventType::ClusterNameUpdated => TopologyEvent::ClusterNameUpdated {
            cluster_id: cluster_id_from_topology_event(reader.get_cluster_id()?)?,
            name: reader.get_cluster_name()?.to_string()?,
            updated_at_unix_ms: reader.get_updated_at_unix_ms(),
            actor_node_id: read_node_id(reader.get_actor_node_id()?)?,
        },
        EventType::NodeSchedulingUpdated => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::NodeSchedulingUpdated {
                id,
                scheduling: PeerSchedulingState::from_node_info(
                    id,
                    node.get_schedulable(),
                    node.get_drain_requested(),
                    node.get_scheduling_updated_at_unix_ms(),
                    {
                        let actor = node.get_scheduling_actor_node_id()?;
                        let bytes = actor.get_bytes()?;
                        if bytes.is_empty() {
                            None
                        } else {
                            Some(
                                Uuid::from_slice(bytes)
                                    .map_err(|err| capnp::Error::failed(err.to_string()))?,
                            )
                        }
                    },
                    Some(node.get_scheduling_reason()?.to_string()?),
                    match node.get_drain_task_stop_timeout_secs() {
                        0 => None,
                        value => Some(value),
                    },
                ),
            }
        }
        EventType::NodeLabelsUpdated => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::NodeLabelsUpdated {
                id,
                labels: labels_from_node_info(node)?,
            }
        }
    };

    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::restored_local_peer_value;
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::topology::peers::{
        PeerLabel, PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue,
        WireGuardPeerValue,
    };
    use uuid::Uuid;

    /// Build one synthetic self row matching the conservative join-time snapshot.
    fn test_join_peer_value() -> PeerValue {
        let node_id = Uuid::from_bytes([9u8; 16]);
        PeerValue {
            address: "127.0.0.1:7000".to_string(),
            hostname: "node-a".to_string(),
            noise_static_pub: [1u8; 32],
            signing_pub: [2u8; 32],
            identity_sig: vec![3u8; 64],
            wireguard: Some(WireGuardPeerValue {
                public_key: [4u8; 32],
                port: 7777,
                enabled: false,
            }),
            runtime_support: RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState {
                schedulable: true,
                drain_requested: false,
                updated_at_unix_ms: 10,
                actor_node_id: node_id,
                reason: None,
                drain_task_stop_timeout_secs: None,
            },
            labels: PeerLabelState::default(),
            membership: PeerMembership::active(10),
        }
    }

    /// Restoring the self row should preserve a newer locally published WireGuard advertisement.
    #[test]
    fn restored_local_peer_value_keeps_newer_wireguard_state() {
        let payload = test_join_peer_value();
        let mut current = payload.clone();
        current.wireguard = Some(WireGuardPeerValue {
            public_key: [4u8; 32],
            port: 7777,
            enabled: true,
        });

        let restored = restored_local_peer_value(Some(&current), payload.clone());

        assert_eq!(restored.address, payload.address);
        assert!(restored.wireguard.expect("wireguard state").enabled);
    }

    /// Restoring the self row should preserve later local scheduling updates over stale join data.
    #[test]
    fn restored_local_peer_value_keeps_newer_scheduling_state() {
        let node_id = Uuid::from_bytes([9u8; 16]);
        let payload = test_join_peer_value();
        let current = PeerValue {
            address: "127.0.0.1:7999".to_string(),
            hostname: "node-newer".to_string(),
            noise_static_pub: [8u8; 32],
            signing_pub: [7u8; 32],
            identity_sig: vec![6u8; 64],
            wireguard: None,
            runtime_support: RuntimeSupportProfile::default(),
            scheduling: PeerSchedulingState {
                schedulable: false,
                drain_requested: true,
                updated_at_unix_ms: 20,
                actor_node_id: node_id,
                reason: Some("maintenance".to_string()),
                drain_task_stop_timeout_secs: Some(30),
            },
            labels: PeerLabelState::default(),
            membership: PeerMembership::active(20),
        };

        let restored = restored_local_peer_value(Some(&current), payload.clone());

        assert_eq!(restored.address, payload.address);
        assert_eq!(restored.hostname, payload.hostname);
        assert!(!restored.scheduling.schedulable);
        assert!(restored.scheduling.drain_requested);
        assert_eq!(restored.scheduling.reason.as_deref(), Some("maintenance"));
        assert_eq!(restored.scheduling.drain_task_stop_timeout_secs, Some(30));
    }

    /// Restoring the self row should preserve later local label updates over stale join data.
    #[test]
    fn restored_local_peer_value_keeps_newer_labels() {
        let node_id = Uuid::from_bytes([9u8; 16]);
        let payload = test_join_peer_value();
        let mut current = payload.clone();
        current.labels = PeerLabelState::new(
            vec![PeerLabel {
                key: "topology.zone".to_string(),
                value: "west".to_string(),
            }],
            30,
            node_id,
        );

        let restored = restored_local_peer_value(Some(&current), payload);

        assert_eq!(restored.labels.get("topology.zone"), Some("west"));
    }
}
