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
use crate::runtime::types::RuntimeSupportProfile;
use crate::secrets::master_key::reconciler::SecretMasterKeyReconciler;
use crate::secrets::master_key::replication::SecretMasterKeyGrantRecipient;
use crate::server::credential::ClusterCredential;
use crate::store::local::{LocalCredentialStore, LocalSessionStore};
use crate::store::peer_store::PeersStore;
use crate::store::secret_master_key_store::{
    SecretMasterKeyCurrent, SecretMasterKeySyncRecord, current_for_scope, current_row_id,
    read_secret_master_key_sync_record, upsert_record,
};
use crate::sync::SyncTraceContext;
use crate::topology::health::status_to_node_status;
use crate::topology::peers::{
    PeerLabel, PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue,
    labels_from_peer,
};
use capnp::Error;
use ed25519_dalek::VerifyingKey;
use mantissa_protocol::server::{self, cluster_session};
use mantissa_protocol::topology::{topology, topology_event};
use mantissa_store::uuid_key::UuidKey;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use tracing::{info, warn};
use uuid::Uuid;
use x25519_dalek::PublicKey;

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
            .map_err(|error| Error::failed(format!("invalid join anchor text: {error}")))?;
        let join_token = request
            .get_join_token()?
            .to_string()
            .map_err(|error| Error::failed(format!("invalid join token text: {error}")))?;

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
        platform_os: payload.platform_os.clone(),
        platform_arch: payload.platform_arch.clone(),
        noise_static_pub: payload.public_key,
        signing_pub: payload.signing_key,
        identity_sig: payload.identity_sig.to_vec(),
        wireguard: payload.wireguard.clone(),
        scheduling: payload.scheduling.clone(),
        labels: payload.labels.clone(),
        runtime_support: payload.runtime_support.clone(),
        root_schema: payload.root_schema,
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
        if restored.platform_os.is_empty() && !current.platform_os.is_empty() {
            restored.platform_os = current.platform_os.clone();
        }
        if restored.platform_arch.is_empty() && !current.platform_arch.is_empty() {
            restored.platform_arch = current.platform_arch.clone();
        }
        restored.wireguard =
            WireGuardPeerValue::preferred(current.wireguard.as_ref(), restored.wireguard.as_ref());
        restored.scheduling = PeerSchedulingState::merge(&restored.scheduling, &current.scheduling);
        restored.labels = PeerLabelState::merge(&restored.labels, &current.labels);
        restored.root_schema =
            crate::cluster::RootSchemaInfo::merge(restored.root_schema, current.root_schema);
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
    ticket_expires_at_unix_secs: Option<u64>,
    credential: Vec<u8>,
    session: cluster_session::Client,
    master_key_records: Vec<SecretMasterKeySyncRecord>,
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

        let wireguard = if !config::wireguard_enabled() || !mantissa_net::paths::running_as_root() {
            None
        } else {
            match mantissa_net::wireguard::resolve_wireguard_key_path()
                .and_then(mantissa_net::wireguard::load_or_generate_wireguard_keys)
            {
                Ok(keys) => {
                    match mantissa_net::wireguard::load_or_choose_wireguard_listen_port_with_preferred_and_override(
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
            platform_os: std::env::consts::OS.to_string(),
            platform_arch: std::env::consts::ARCH.to_string(),
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
            root_schema: self.root_schema_info(),
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
        let resp = response.get()?.get_response()?;

        let session = resp.get_session()?;
        let ticket = resp.get_ticket()?.to_vec();
        let ticket_expires_at_unix_secs = match resp.get_ticket_expires_at_unix_secs() {
            0 => None,
            expires_at => Some(expires_at),
        };
        let credential = resp.get_credential()?.to_vec();
        let node_info = resp.get_node_info()?;
        let peer_id = read_node_id(node_info.get_id()?)?;
        let peer_value = PeerValue::from_node_info(peer_id, node_info)?;
        let peer_incarnation = peer_value.membership.incarnation;
        let records_reader = resp.get_master_key_records()?;
        let mut master_key_records = Vec::with_capacity(records_reader.len() as usize);
        for record in records_reader.iter() {
            master_key_records.push(read_secret_master_key_sync_record(record)?);
        }

        Ok(JoinResponse {
            peer_id,
            peer_value,
            peer_incarnation,
            ticket,
            ticket_expires_at_unix_secs,
            credential,
            session,
            master_key_records,
        })
    }

    /// Persists the anchor peer row and renewable join credentials returned by a successful join.
    async fn persist_join_state(
        peers: &PeersStore,
        local_sessions: &LocalSessionStore,
        local_creds: &LocalCredentialStore,
        response: &JoinResponse,
    ) -> Result<(), Error> {
        if let Err(e) = peers
            .upsert(
                &UuidKey::from(response.peer_id),
                response.peer_value.clone(),
            )
            .await
        {
            log::warn!(target: "topology", "join: upsert of anchor NodeInfo failed: {e}");
        }

        local_sessions
            .put_with_meta(
                response.peer_id,
                &response.ticket,
                response.ticket_expires_at_unix_secs,
                None,
            )
            .map_err(|e| Error::failed(format!("ticket persist failed: {e}")))?;

        local_creds
            .put(response.peer_id, &response.credential)
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

    /// Publishes the current master-key rows for a node accepted by this anchor.
    pub(crate) async fn publish_master_key_grants_for_joiner(
        &self,
        joiner_id: Uuid,
        joiner_noise_static_pub: [u8; 32],
    ) -> Result<Vec<SecretMasterKeySyncRecord>, Error> {
        let recipient = SecretMasterKeyGrantRecipient {
            node_id: joiner_id,
            noise_static_pub: joiner_noise_static_pub,
        };

        // Keep the keyring read lock until the current row and grants are durable. Otherwise a
        // rotation or replicated-current adoption could switch the local current key between
        // "which key did this anchor grant?" and "which current key did the joiner adopt?".
        // Join must only grant the cached current key here: loading all historical keys unwraps
        // every local envelope and moves production Argon2 work back onto the registerNode path.
        let keyring = self.stores.secret_keyring.read().await;
        let current = keyring
            .current_record()
            .map_err(|err| Error::failed(format!("load active master key: {err}")))?;
        self.stores
            .secret_master_store
            .commit_current_for_replication(current.key_id())
            .map_err(|err| Error::failed(format!("commit join master key grant: {err}")))?;
        self.stores
            .secret_master_key_publisher
            .publish_current_key_returning_records(&current, &[recipient])
            .await
            .map_err(|err| Error::failed(format!("publish join master-key grants: {err}")))
    }

    /// Applies registerNode master-key rows and adopts the cluster current before join returns.
    async fn adopt_join_master_key_records(
        &self,
        records: &[SecretMasterKeySyncRecord],
    ) -> Result<(), Error> {
        let cluster_view = self.active_cluster_view();
        for record in records {
            upsert_record(&self.stores.secret_master_keys, record.clone())
                .await
                .map_err(|err| Error::failed(format!("seed joined master-key row: {err}")))?;
        }

        let reconciler = SecretMasterKeyReconciler::new(
            self.local.node.id,
            self.deps.registry.noise_keys(),
            self.deps.registry.clone(),
            self.stores.secret_master_keys.clone(),
            self.stores.secret_master_store.clone(),
            self.stores.secret_keyring.clone(),
            self.local.cluster_view.clone(),
        );
        let report = reconciler
            .reconcile_active_view()
            .await
            .map_err(|err| Error::failed(format!("reconcile joined master key: {err:#}")))?;
        if report.current_waiting_for_descriptor || report.current_waiting_for_key {
            return Err(Error::failed(
                "joined master key is not yet available from replicated grants".into(),
            ));
        }

        let replicated_current = current_for_scope(&self.stores.secret_master_keys, cluster_view)
            .map_err(|err| Error::failed(format!("load replicated master-key current: {err}")))?
            .ok_or_else(|| Error::failed("anchor did not publish a master-key current".into()))?;
        self.ensure_join_current_is_unambiguous(cluster_view, &replicated_current)?;
        let local_current = self
            .stores
            .secret_master_store
            .current()
            .map_err(|err| Error::failed(format!("load local master key: {err}")))?;
        if local_current.key_id() != replicated_current.key_id {
            return Err(Error::failed(format!(
                "joined master key {} was not adopted locally",
                replicated_current.key_id
            )));
        }

        Ok(())
    }

    /// Rejects a join when independent current rows are visible for the joined view.
    fn ensure_join_current_is_unambiguous(
        &self,
        cluster_view: ClusterViewId,
        adopted: &SecretMasterKeyCurrent,
    ) -> Result<(), Error> {
        let snapshot = self
            .stores
            .secret_master_keys
            .get_snapshot(&UuidKey::from(current_row_id(cluster_view)))
            .map_err(|err| Error::failed(format!("load joined master-key currents: {err}")))?;
        let Some(snapshot) = snapshot else {
            return Ok(());
        };

        for record in snapshot.as_slice() {
            let SecretMasterKeySyncRecord::Current(candidate) = record else {
                continue;
            };
            if join_current_conflicts(candidate, adopted) {
                return Err(Error::failed(format!(
                    "conflicting master-key current {} observed while joining {}",
                    candidate.key_id, cluster_view
                )));
            }
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
                        "topology: failed to resolve gossip capability for immediate broadcast: {err}"
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
                    "topology: immediate topology broadcast failed: {err}"
                );
            }
        }
    }
}

/// Returns true when two current rows represent unrelated key identities for join.
fn join_current_conflicts(
    candidate: &SecretMasterKeyCurrent,
    adopted: &SecretMasterKeyCurrent,
) -> bool {
    if candidate.key_id == adopted.key_id {
        return false;
    }
    if candidate.created_by_operation_id.is_some() || adopted.created_by_operation_id.is_some() {
        return false;
    }
    !join_currents_share_lineage(candidate, adopted)
}

/// Checks lineage only from the metadata embedded in the visible current rows.
fn join_currents_share_lineage(
    left: &SecretMasterKeyCurrent,
    right: &SecretMasterKeyCurrent,
) -> bool {
    left.parent_key_ids.contains(&right.key_id)
        || right.parent_key_ids.contains(&left.key_id)
        || left
            .parent_key_ids
            .iter()
            .any(|parent| right.parent_key_ids.contains(parent))
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
        let client = mantissa_client::connection::get_client_secure_join_with_keys(
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

        let root_schema_version = super::sync::negotiated_sync_root_schema_version(
            self.root_schema_info(),
            response.peer_value.root_schema,
        )
        .ok_or_else(|| {
            Error::failed("anchor and joiner do not share a compatible root schema version".into())
        })?;

        self.persist_local_join_payload(&payload).await?;

        Topology::persist_join_state(
            &self.stores.peers,
            &self.stores.local_sessions,
            &self.stores.local_credential_store,
            &response,
        )
        .await?;

        ClusterCredential::from_bytes_verified(&response.credential).map_err(Error::failed)?;

        self.swim_record_join(response.peer_id, response.peer_incarnation);

        self.attach_handle_only(response.peer_id, anchor_handle)
            .await;

        let sync_cap = {
            let req = response.session.get_sync_request();
            let resp = req.send().promise.await?;
            resp.get()?.get_sync()?
        };

        self.adopt_join_master_key_records(&response.master_key_records)
            .await?;

        let sync_trace = SyncTraceContext::peer(
            response.peer_id,
            response.peer_value.address.clone(),
            "join",
        );
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
                    .sync_all_domains(sync_cap, cluster_view, root_schema_version, Some(trace))
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

    /// Evicts one stale peer identity from the cluster by publishing a newer left membership.
    async fn evict_node(
        self: Rc<Self>,
        params: topology::EvictNodeParams,
        _results: topology::EvictNodeResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?;
        let node_id = read_node_id(request.get_node_id()?)?;
        if node_id == self.local.node.id {
            return Err(capnp::Error::failed(
                "cannot evict the local node; use `mantissa leave` instead".into(),
            ));
        }

        let Some(membership) = self.peer_membership_unscoped(node_id)? else {
            return Err(capnp::Error::failed(format!("node '{node_id}' not found")));
        };
        let evict_incarnation = if membership.is_active() {
            let incarnation = membership.incarnation.checked_add(1).ok_or_else(|| {
                capnp::Error::failed(format!("node '{node_id}' is already at max incarnation"))
            })?;
            self.mark_peer_left(node_id, incarnation)
                .await
                .map_err(|e| capnp::Error::failed(format!("evict: mark-left failed: {e}")))?;
            incarnation
        } else {
            membership.incarnation
        };
        let evict_event = TopologyEvent::Leave {
            id: node_id,
            incarnation: evict_incarnation,
        };
        self.broadcast_topology_event_now(&evict_event).await;
        self.gossip_topology_event(evict_event).await?;

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
            .load_all_regs()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let local_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let mut scoped_nodes = Vec::<ListedNodeRow>::with_capacity(actives.len());

        for (k, reg) in actives.into_iter() {
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
                .unwrap_or(::mantissa_health::Status::Unknown);
            let node_status = status_to_node_status(health_status);
            let Some(value) = PeerValue::select_reg(&reg).filter(|value| value.is_active()) else {
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
                        .unwrap_or(::mantissa_health::Status::Unknown),
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
            .load_all_regs()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        for (key, reg) in actives {
            let peer_id = key.to_uuid();
            if peer_id == self.local.node.id {
                continue;
            }
            if excluded_peers.contains(&peer_id) {
                continue;
            }
            let Some(_selected) = PeerValue::select_reg(&reg).filter(|value| value.is_active())
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

fn read_optional_uuid_data(
    data: capnp::data::Reader<'_>,
    field_name: &str,
) -> Result<Option<Uuid>, capnp::Error> {
    if data.is_empty() {
        return Ok(None);
    }
    if data.len() != 16 {
        return Err(capnp::Error::failed(format!(
            "{field_name} must be empty or exactly 16 bytes"
        )));
    }

    Uuid::from_slice(data)
        .map(Some)
        .map_err(|err| capnp::Error::failed(err.to_string()))
}

fn cluster_id_from_topology_event(
    reader: mantissa_protocol::topology::cluster_id::Reader<'_>,
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
            let peer = PeerValue::from_node_info(id, node)?;
            let signing_pub = VerifyingKey::from_bytes(&peer.signing_pub)
                .map_err(|err| capnp::Error::failed(err.to_string()))?;
            let client = if node.has_handle() {
                Some(node.get_handle()?)
            } else {
                None
            };

            TopologyEvent::Join {
                id,
                hostname: peer.hostname.clone(),
                address: peer.address.clone(),
                platform_os: peer.platform_os.clone(),
                platform_arch: peer.platform_arch.clone(),
                root_hash: node.get_root_hash()?.to_str()?.to_string(),
                incarnation: peer.membership.incarnation,
                client,
                noise_static_pub: PublicKey::from(peer.noise_static_pub),
                signing_pub: Box::new(signing_pub),
                identity_sig: peer.identity_sig,
                wireguard: peer.wireguard,
                scheduling: Box::new(peer.scheduling),
                labels: Box::new(peer.labels),
                runtime_support: Box::new(peer.runtime_support),
                root_schema: peer.root_schema,
            }
        }
        EventType::Remove => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Leave {
                id,
                incarnation: node.get_peer()?.get_membership_incarnation(),
            }
        }
        EventType::Alive => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Alive {
                id,
                incarnation: node.get_peer()?.get_membership_incarnation(),
            }
        }
        EventType::Suspect => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Suspect {
                id,
                incarnation: node.get_peer()?.get_membership_incarnation(),
            }
        }
        EventType::Down => {
            let node = reader.get_node()?;
            let id = read_node_id(node.get_id()?)?;
            TopologyEvent::Down {
                id,
                incarnation: node.get_peer()?.get_membership_incarnation(),
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
            let peer = node.get_peer()?;
            TopologyEvent::NodeSchedulingUpdated {
                id,
                scheduling: PeerSchedulingState::from_node_info(
                    id,
                    peer.get_schedulable(),
                    peer.get_drain_requested(),
                    peer.get_scheduling_updated_at_unix_ms(),
                    read_optional_uuid_data(
                        peer.get_scheduling_actor_node_id()?,
                        "schedulingActorNodeId",
                    )?,
                    Some(peer.get_scheduling_reason()?.to_string()?),
                    match peer.get_drain_task_stop_timeout_secs() {
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
                labels: labels_from_peer(node.get_peer()?)?,
            }
        }
    };

    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::{join_current_conflicts, restored_local_peer_value};
    use crate::cluster::ClusterViewId;
    use crate::runtime::types::RuntimeSupportProfile;
    use crate::store::secret_master_key_store::SecretMasterKeyCurrent;
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
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
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
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: PeerMembership::active(10),
        }
    }

    /// Build one synthetic master-key current row for join conflict tests.
    fn master_key_current(key_id: Uuid, parent_key_ids: Vec<Uuid>) -> SecretMasterKeyCurrent {
        SecretMasterKeyCurrent {
            scope_view: ClusterViewId::legacy_default(),
            key_id,
            generation: parent_key_ids.len().saturating_add(1) as u64,
            created_by_operation_id: None,
            parent_key_ids,
        }
    }

    /// Independent initial current rows should make join fail instead of picking by key id.
    #[test]
    fn join_current_conflict_detects_unrelated_initial_keys() {
        let left = master_key_current(Uuid::from_u128(1), Vec::new());
        let right = master_key_current(Uuid::from_u128(2), Vec::new());

        assert!(join_current_conflicts(&left, &right));
        assert!(join_current_conflicts(&right, &left));
    }

    /// Parent-child current rows are normal rotation convergence, not a join conflict.
    #[test]
    fn join_current_conflict_allows_lineage() {
        let parent_id = Uuid::from_u128(1);
        let child_id = Uuid::from_u128(2);
        let parent = master_key_current(parent_id, Vec::new());
        let child = master_key_current(child_id, vec![parent_id]);

        assert!(!join_current_conflicts(&parent, &child));
        assert!(!join_current_conflicts(&child, &parent));
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
            platform_os: "linux".to_string(),
            platform_arch: "amd64".to_string(),
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
            root_schema: crate::cluster::RootSchemaInfo::default(),
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
