use super::{Topology, types::TopologyEvent};
use crate::cluster::coordinator::ClusterTransitionCoordinator;
use crate::cluster::participant::{ClusterParticipantReport, ClusterTransitionParticipant};
use crate::cluster::transition::ClusterTransition;
use crate::cluster::{ClusterId, ClusterViewId};
use crate::config;
use crate::node::address::extract_port;
use crate::node::id::{read_node_id, set_node_id};
use crate::node::identity::pubkey_from_slice;
use crate::server::credential::ClusterCredential;
use crate::services::types::ServiceStatus;
use crate::store::local_credential_store::LocalCredentialStore;
use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::store::secret_master_store::MasterKeyRecord;
use crate::sync::delta::{SyncStores, SyncTraceContext, sync_all_domains};
use crate::topology::health::status_to_node_status;
use crate::topology::operation::{
    ClusterOperationKind, ClusterOperationRecord, ClusterOperationStage, MergeServicePolicy,
    SplitNetworkPolicy, SplitServicePolicy,
};
use crate::topology::peers::{PeerValue, WireGuardPeerValue};
use async_trait::async_trait;
use capnp::Error;
use capnp::data;
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::VerifyingKey;
use protocol::gossip::gossip_message;
use protocol::server::{self, cluster_session};
use protocol::topology::{topology, topology_event};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use tracing::{info, warn};
use uuid::Uuid;

/// Prefix used when commit-time active-view precondition checks reject stale operations.
const COMMIT_PRECONDITION_FAILURE_PREFIX: &str = "cluster operation commit precondition failed";
/// Number of finalized/aborted operation rows retained durably before old rows are garbage-collected.
const CLUSTER_OPERATION_FINALIZED_RETENTION_COUNT: usize = 512;

#[path = "assignment.rs"]
mod assignment;
#[path = "operation_progress.rs"]
mod operation_progress;
#[path = "split_selector.rs"]
mod split_selector;
use split_selector::{SplitSelectorClauseSpec, SplitTargetSpec};

#[derive(Clone)]
struct JoinPayload {
    id: Uuid,
    hostname: String,
    advertise_addr: String,
    incarnation: u64,
    server_handle: server::Client,
    public_key: [u8; 32],
    signing_key: [u8; 32],
    identity_sig: [u8; 64],
    wireguard: Option<WireGuardPeerValue>,
}

struct JoinInputs {
    anchor: String,
    join_token: String,
}

impl JoinInputs {
    fn from_params(params: topology::JoinParams) -> Result<Self, Error> {
        let request = params.get()?.get_link()?;
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

struct JoinResponse {
    peer_id: Uuid,
    peer_value: PeerValue,
    peer_incarnation: u64,
    ticket: Vec<u8>,
    credential: Vec<u8>,
    session: cluster_session::Client,
}

struct PeerScopeParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for PeerScopeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "peer_scope"
    }

    /// Applies split/merge peer-scope side effects so control-plane sessions match the local view.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split() {
            let local_target_index = transition.local_split_target_index.ok_or_else(|| {
                capnp::Error::failed(format!(
                    "split transition {} missing local target index",
                    transition.operation_id
                ))
            })?;

            if !transition
                .retained_node_ids
                .contains(&self.topology.node.id)
            {
                return Err(capnp::Error::failed(format!(
                    "split operation {} local target {} does not retain local node {}",
                    transition.operation_id, local_target_index, self.topology.node.id
                )));
            }

            let mut evicted = transition
                .evicted_node_ids
                .iter()
                .copied()
                .collect::<Vec<_>>();
            evicted.sort_unstable();

            let mut removed_sessions = 0usize;
            let mut removed_credentials = 0usize;
            for peer_id in evicted.iter().copied() {
                match self.topology.local_sessions.remove(peer_id) {
                    Ok(()) => removed_sessions = removed_sessions.saturating_add(1),
                    Err(err) => {
                        warn!(
                            target: "cluster_view",
                            operation_id = %transition.operation_id,
                            peer_id = %peer_id,
                            "failed to remove local session ticket during split prune: {err}"
                        );
                    }
                }

                match self.topology.local_credential_store.remove(peer_id) {
                    Ok(()) => removed_credentials = removed_credentials.saturating_add(1),
                    Err(err) => {
                        warn!(
                            target: "cluster_view",
                            operation_id = %transition.operation_id,
                            peer_id = %peer_id,
                            "failed to remove local credential during split prune: {err}"
                        );
                    }
                }

                self.topology.registry.remove_peer(peer_id).await;
            }

            self.topology
                .set_excluded_peers(transition.evicted_node_ids.clone())
                .await;
            self.topology
                .registry
                .set_excluded_peers(transition.evicted_node_ids.clone());

            report = report
                .add_detail("local_target_index", local_target_index.to_string())
                .add_detail(
                    "retained_count",
                    transition.retained_node_ids.len().to_string(),
                )
                .add_detail(
                    "evicted_count",
                    transition.evicted_node_ids.len().to_string(),
                )
                .add_detail("removed_sessions", removed_sessions.to_string())
                .add_detail("removed_credentials", removed_credentials.to_string());
            return Ok(report);
        }

        if transition.is_merge() {
            self.topology.set_excluded_peers(HashSet::new()).await;
            self.topology.registry.set_excluded_peers(HashSet::new());
            report = report.add_detail("excluded_peers_reset", "true");
        }

        Ok(report)
    }
}

struct SplitTaskRuntimeParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for SplitTaskRuntimeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "split_task_runtime"
    }

    /// Prunes out-of-scope task runtime rows when split policy requests service partitioning.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split()
            && transition.split_service_policy == SplitServicePolicy::Partitioned
        {
            let removed = self
                .topology
                .prune_split_task_runtime_state(&transition.evicted_node_ids)
                .await?;
            report = report.add_detail("removed_tasks", removed.to_string());
        }
        Ok(report)
    }
}

struct SplitNetworkRuntimeParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for SplitNetworkRuntimeParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "split_network_runtime"
    }

    /// Prunes out-of-scope network runtime rows when split policy requests network isolation.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_split() && transition.split_network_policy == SplitNetworkPolicy::Isolate {
            let (removed_peer_states, removed_attachments) = self
                .topology
                .prune_split_network_runtime_state(&transition.evicted_node_ids)
                .await?;
            report = report
                .add_detail(
                    "removed_network_peer_states",
                    removed_peer_states.to_string(),
                )
                .add_detail(
                    "removed_network_attachments",
                    removed_attachments.to_string(),
                );
        }
        Ok(report)
    }
}

struct MergeServiceParticipant {
    topology: Topology,
}

#[async_trait(?Send)]
impl ClusterTransitionParticipant for MergeServiceParticipant {
    /// Returns the participant identifier used by transition diagnostics.
    fn name(&self) -> &'static str {
        "merge_services"
    }

    /// Nudges running services after merge so replicas can rebalance across the unified view.
    async fn on_commit(
        &self,
        transition: &ClusterTransition,
    ) -> Result<ClusterParticipantReport, capnp::Error> {
        let mut report = ClusterParticipantReport::new(self.name());
        if transition.is_merge() && transition.merge_service_policy == MergeServicePolicy::Rebalance
        {
            let nudged = self
                .topology
                .nudge_running_services_for_merge_rebalance()
                .await?;
            report = report.add_detail("nudged_services", nudged.to_string());
        }
        Ok(report)
    }
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
            id: self.node.id,
            hostname,
            advertise_addr,
            incarnation: self.swim_local_incarnation(),
            server_handle,
            public_key: self.public_key.to_bytes(),
            signing_key: self.signing_key.verifying_key().to_bytes(),
            identity_sig: crate::node::identity::sign_peer_identity(
                &self.signing_key,
                &self.node.id,
                &self.public_key.to_bytes(),
                &self.signing_key.verifying_key().to_bytes(),
            ),
            wireguard,
        })
    }

    async fn register_with_anchor(
        client: server::Client,
        payload: &JoinPayload,
        cluster_view: ClusterViewId,
        join_token: &str,
    ) -> Result<JoinResponse, Error> {
        let mut request = client.register_node_request();

        let mut info = request.get().init_info();
        set_node_id(info.reborrow().init_id(), &payload.id);
        cluster_view.write_capnp(info.reborrow().init_active_cluster_view());
        info.set_hostname(&payload.hostname);
        info.set_addr(&payload.advertise_addr);
        info.set_handle(payload.server_handle.clone());
        info.set_public_key(&payload.public_key);
        info.set_signing_key(&payload.signing_key);
        info.set_identity_sig(&payload.identity_sig);
        info.set_incarnation(payload.incarnation);
        if let Some(wg) = payload.wireguard.as_ref() {
            info.set_wireguard_public_key(&wg.public_key);
            info.set_wireguard_port(wg.port);
            info.set_wireguard_enabled(wg.enabled);
        }

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

        self.secret_master_store
            .import_current(&record)
            .map_err(|e| Error::failed(format!("failed to persist master key: {e}")))?;

        {
            let guard = self.secret_keyring.write().await;
            guard.install_current(record);
        }

        Ok(())
    }

    /// Resolves the split target index selected for the local node in a split operation.
    fn local_split_target_index(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<usize, capnp::Error> {
        operation
            .split_assignments
            .iter()
            .find(|assignment| assignment.node_id == self.node.id)
            .map(|assignment| assignment.target_index)
            .ok_or_else(|| {
                capnp::Error::failed(format!(
                    "split operation {} has no assignment for local node {}",
                    operation.id, self.node.id
                ))
            })
    }

    /// Resolves the target view this node should activate when committing the operation.
    fn local_target_view_for_operation(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<ClusterViewId, capnp::Error> {
        match operation.kind {
            ClusterOperationKind::Merge => operation.target_views.first().copied(),
            ClusterOperationKind::Split => operation
                .target_views
                .get(self.local_split_target_index(operation)?)
                .copied(),
        }
        .ok_or_else(|| capnp::Error::failed("operation has no target views for commit".to_string()))
    }

    /// Builds a canonical local transition snapshot from one durable operation record.
    fn transition_for_operation(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<ClusterTransition, capnp::Error> {
        let (actives, _) = self
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let known_peers = actives
            .into_iter()
            .map(|(key, _)| key.to_uuid())
            .filter(|peer_id| *peer_id != self.node.id)
            .collect::<HashSet<_>>();
        ClusterTransition::from_operation(operation, self.node.id, &known_peers)
    }

    /// Runs all registered transition participants for commit-time side effects.
    async fn run_transition_commit_hooks(
        &self,
        transition: &ClusterTransition,
    ) -> Result<Vec<ClusterParticipantReport>, capnp::Error> {
        let coordinator = ClusterTransitionCoordinator::new(vec![
            Box::new(PeerScopeParticipant {
                topology: self.clone(),
            }),
            Box::new(SplitTaskRuntimeParticipant {
                topology: self.clone(),
            }),
            Box::new(SplitNetworkRuntimeParticipant {
                topology: self.clone(),
            }),
            Box::new(MergeServiceParticipant {
                topology: self.clone(),
            }),
        ]);
        coordinator.on_commit(transition).await
    }

    /// Converts the merge request policy from the Cap'n Proto enum into local durable policy state.
    fn merge_service_policy_from_capnp(
        policy: protocol::topology::MergeServicePolicy,
    ) -> MergeServicePolicy {
        match policy {
            protocol::topology::MergeServicePolicy::Rebalance => MergeServicePolicy::Rebalance,
            protocol::topology::MergeServicePolicy::Preserve => MergeServicePolicy::Preserve,
        }
    }

    /// Converts the split request service policy into local durable policy state.
    fn split_service_policy_from_capnp(
        policy: protocol::topology::SplitServicePolicy,
    ) -> SplitServicePolicy {
        match policy {
            protocol::topology::SplitServicePolicy::Partitioned => SplitServicePolicy::Partitioned,
            protocol::topology::SplitServicePolicy::Preserve => SplitServicePolicy::Preserve,
        }
    }

    /// Converts the split request network policy into local durable policy state.
    fn split_network_policy_from_capnp(
        policy: protocol::topology::SplitNetworkPolicy,
    ) -> SplitNetworkPolicy {
        match policy {
            protocol::topology::SplitNetworkPolicy::Isolate => SplitNetworkPolicy::Isolate,
            protocol::topology::SplitNetworkPolicy::Preserve => SplitNetworkPolicy::Preserve,
        }
    }

    /// Removes out-of-scope task runtime rows after split so each partition reconciles services locally.
    async fn prune_split_task_runtime_state(
        &self,
        evicted: &HashSet<Uuid>,
    ) -> Result<usize, capnp::Error> {
        let (actives, _) = self
            .tasks
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut removed = 0usize;
        for (key, snapshot) in actives {
            let Some(task) = snapshot.as_slice().last() else {
                continue;
            };
            if !evicted.contains(&task.node_id) {
                continue;
            }

            // Split pruning is view-scoped, not a global delete. Purge locally so merge/sync
            // can repopulate rows from the other partition.
            self.tasks
                .purge_local(&UuidKey::from(key.to_uuid()))
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            removed = removed.saturating_add(1);
        }

        Ok(removed)
    }

    /// Removes out-of-scope overlay peer/attachment rows after split to isolate data-plane state.
    async fn prune_split_network_runtime_state(
        &self,
        evicted: &HashSet<Uuid>,
    ) -> Result<(usize, usize), capnp::Error> {
        let (peer_rows, _) = self
            .network_peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut removed_peer_states = 0usize;
        for (key, snapshot) in peer_rows {
            let Some(peer_state) = snapshot.as_slice().last() else {
                continue;
            };
            if !evicted.contains(&peer_state.peer_id) {
                continue;
            }

            // Keep split prune reversible: do not leave durable tombstones that block merge replay.
            self.network_peers
                .purge_local(&UuidKey::from(key.to_uuid()))
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            removed_peer_states = removed_peer_states.saturating_add(1);
        }

        let (attachment_rows, _) = self
            .network_attachments
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut removed_attachments = 0usize;
        for (key, snapshot) in attachment_rows {
            let Some(attachment) = snapshot.as_slice().last() else {
                continue;
            };
            if !evicted.contains(&attachment.node_id) {
                continue;
            }

            // Keep split prune reversible: do not leave durable tombstones that block merge replay.
            self.network_attachments
                .purge_local(&UuidKey::from(key.to_uuid()))
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            removed_attachments = removed_attachments.saturating_add(1);
        }

        Ok((removed_peer_states, removed_attachments))
    }

    /// Touches active service specs after merge so controllers promptly rebalance replicas cluster-wide.
    async fn nudge_running_services_for_merge_rebalance(&self) -> Result<usize, capnp::Error> {
        let (actives, _) = self
            .services
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let mut updated = 0usize;
        for (key, snapshot) in actives {
            let Some(current) = snapshot.as_slice().last().cloned() else {
                continue;
            };
            if !matches!(
                current.status,
                ServiceStatus::Running | ServiceStatus::Deploying
            ) {
                continue;
            }

            let mut next = current.clone();
            next.touch();
            self.services
                .upsert(&UuidKey::from(key.to_uuid()), next)
                .await
                .map_err(|e| capnp::Error::failed(e.to_string()))?;
            updated = updated.saturating_add(1);
        }

        Ok(updated)
    }

    /// Reads the cluster view currently bound to a session for operation relay validation.
    async fn session_cluster_view(
        session: &cluster_session::Client,
    ) -> Result<ClusterViewId, capnp::Error> {
        let request = session.get_cluster_view_request();
        let response = request.send().promise.await?;
        ClusterViewId::from_capnp(response.get()?.get_view()?).map_err(capnp::Error::failed)
    }

    /// Resolves the best-known cluster view for one peer session, if available.
    async fn best_known_peer_view(&self, peer_id: Uuid) -> Option<ClusterViewId> {
        if peer_id == self.node.id {
            return Some(self.active_cluster_view());
        }

        // Keep list/split introspection side-effect free: do not force session bootstrap
        // from read-only view probes.
        let session = self.registry.cached_session_for(peer_id).await?;
        Self::session_cluster_view(&session).await.ok()
    }

    /// Best-effort relay of one operation record to peers in the operation's relay scope.
    async fn broadcast_cluster_operation(
        &self,
        operation: &ClusterOperationRecord,
    ) -> Result<usize, capnp::Error> {
        let relay_views = match operation.kind {
            ClusterOperationKind::Split => {
                let source_view = operation.source_views.first().copied().ok_or_else(|| {
                    capnp::Error::failed("split operation missing source view".to_string())
                })?;
                HashSet::from([source_view])
            }
            ClusterOperationKind::Merge => {
                let source_view = operation.source_views.first().copied().ok_or_else(|| {
                    capnp::Error::failed("merge operation missing source view".to_string())
                })?;
                let mut views = HashSet::from([source_view]);
                for target in operation.target_views.iter().copied() {
                    views.insert(target);
                }
                views
            }
        };
        let snapshot = match self.peer_snapshot().await {
            Some(snapshot) => snapshot,
            None => return Ok(0),
        };
        let payload =
            bincode::serialize(operation).map_err(|e| capnp::Error::failed(e.to_string()))?;
        let mut relayed = 0usize;

        for entry in snapshot.entries.iter() {
            let peer_id = entry.peer_id;
            if peer_id == self.node.id {
                continue;
            }

            let session = if operation.kind == ClusterOperationKind::Merge {
                self.registry.session_for_peer_unscoped(peer_id).await
            } else {
                self.registry.session_for_peer(peer_id).await
            };
            let Some(session) = session else {
                continue;
            };
            let peer_view = match Self::session_cluster_view(&session).await {
                Ok(view) => view,
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        peer_id = %peer_id,
                        "failed to read peer session view for operation relay: {err}"
                    );
                    continue;
                }
            };
            if !relay_views.contains(&peer_view) {
                continue;
            }

            let topology = session
                .get_topology_request()
                .send()
                .pipeline
                .get_topology();
            let mut relay = topology.submit_cluster_operation_request();
            relay.get().set_id(operation.id.as_bytes());
            relay.get().set_payload(&payload);
            match relay.send().promise.await {
                Ok(_) => {
                    relayed = relayed.saturating_add(1);
                }
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        operation_id = %operation.id,
                        peer_id = %peer_id,
                        "failed to relay cluster operation: {err}"
                    );
                }
            }
        }

        if relayed > 0 {
            info!(
                target: "cluster_view",
                operation_id = %operation.id,
                relayed,
                relay_view_count = relay_views.len(),
                "relayed cluster operation to peers"
            );
        }

        Ok(relayed)
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

        let self_addr = self.networking.configured().to_string();

        let inputs = JoinInputs::from_params(params)?;

        if inputs.anchor == self_addr {
            return Err(capnp::Error::failed("cannot join own address".to_string()));
        }

        let noise_keys = self.registry.noise_keys();
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

        Topology::persist_join_state(
            &self.peers,
            &self.local_sessions,
            &self.local_credential_store,
            peer_id,
            &peer_value,
            &ticket,
            &credential,
        )
        .await?;

        self.install_master_key_from_anchor(session.clone()).await?;

        ClusterCredential::from_bytes_verified(&credential).map_err(Error::failed)?;

        self.swim_note_join(peer_id, peer_incarnation).await;

        self.attach_handle_only(peer_id, anchor_handle).await;

        let sync_cap = {
            let req = session.get_sync_request();
            let resp = req.send().promise.await?;
            resp.get()?.get_sync()?
        };

        let sync_stores = SyncStores {
            peers: self.peers.clone(),
            tasks: self.tasks.clone(),
            services: self.services.clone(),
            secrets: self.secrets.clone(),
            networks: self.networks.clone(),
            network_peers: self.network_peers.clone(),
            network_attachments: self.network_attachments.clone(),
        };

        let sync_trace = SyncTraceContext::peer(peer_id, peer_value.address.clone(), "join");
        tokio::task::spawn_local({
            let stores = sync_stores;
            let cluster_view = self.active_cluster_view();
            let trace = sync_trace;
            async move {
                sync_all_domains(stores, sync_cap, cluster_view, Some(trace)).await;
            }
        });

        self.ensure_periodic_sync();
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
        if !self.sync.is_running() {
            return Err(capnp::Error::failed("node is not part of a cluster".into()));
        }

        let self_id = self.node.id;

        self.peers
            .remove(&UuidKey::from(self_id))
            .await
            .map_err(|e| capnp::Error::failed(format!("leave: tombstone failed: {e}")))?;

        self.registry.clear().await;

        // Stop the loop so this node is quiescent and can rejoin elsewhere
        self.stop_periodic_sync();

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

        let peers = self.peers.clone();
        let health_snapshot = self.health_monitor.snapshot();

        let (actives, _) = peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let local_view = self.active_cluster_view();
        let excluded_peers = self.excluded_peers_snapshot().await;
        let mut scoped_nodes =
            Vec::<(Uuid, Option<PeerValue>, protocol::health::NodeStatus)>::with_capacity(
                actives.len(),
            );

        for (k, snap) in actives.into_iter() {
            let id = k.to_uuid();
            if excluded_peers.contains(&id) {
                continue;
            }
            let candidate_view = if id == self.node.id {
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
            scoped_nodes.push((id, snap.as_slice().last().cloned(), node_status));
        }

        scoped_nodes.sort_by_key(|(id, _, _)| *id);
        let list_builder = results.get().init_nodes();
        let mut node_list = list_builder.init_nodes(scoped_nodes.len() as u32);
        for (index, (id, value, node_status)) in scoped_nodes.into_iter().enumerate() {
            let mut node = node_list.reborrow().get(index as u32);
            set_node_id(node.reborrow().init_id(), &id);
            local_view.write_capnp(node.reborrow().init_active_cluster_view());

            if let Some(val) = value {
                node.set_addr(&val.address);
                node.set_hostname(&val.hostname);
                node.set_public_key(&val.noise_static_pub);
                node.set_signing_key(&val.signing_pub);
                if let Some(wg) = val.wireguard.as_ref() {
                    node.set_wireguard_public_key(&wg.public_key);
                    node.set_wireguard_port(wg.port);
                    node.set_wireguard_enabled(wg.enabled);
                }
            }
            node.set_health(node_status);
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
        let token = self.token_store.current_token().await;
        results.get().set_token(&token);
        Ok(())
    }

    /// Rotates the token used to join the cluster.
    async fn rotate_token(
        self: Rc<Self>,
        _params: topology::RotateTokenParams,
        mut results: topology::RotateTokenResults,
    ) -> Result<(), Error> {
        let new_token = self.token_store.rotate_and_persist().await?;
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
        let health_snapshot = self.health_monitor.snapshot();
        let mut list = results.get().init_nodes(candidates.len() as u32);
        for (idx, candidate) in candidates.into_iter().enumerate() {
            let mut row = list.reborrow().get(idx as u32);
            set_node_id(row.reborrow().init_node_id(), &candidate.node_id);
            row.set_hostname(&candidate.hostname);
            row.set_addr(&candidate.address);
            row.set_wireguard_enabled(candidate.wireguard_enabled);
            row.set_health(status_to_node_status(
                health_snapshot
                    .get(&candidate.node_id)
                    .cloned()
                    .unwrap_or(::health::Status::Unknown),
            ));

            let view = self
                .best_known_peer_view(candidate.node_id)
                .await
                .unwrap_or(local_view);
            view.write_capnp(row.reborrow().init_active_cluster_view());

            if let Some(cpu_vendor) = candidate.cpu_vendor.as_deref() {
                row.set_cpu_vendor(cpu_vendor);
            }
            if let Some(cpu_brand) = candidate.cpu_brand.as_deref() {
                row.set_cpu_brand(cpu_brand);
            }
            row.set_cpu_logical(candidate.cpu_logical.unwrap_or_default());
            row.set_cpu_cores(candidate.cpu_cores.unwrap_or_default());
            row.set_memory_total_kb(candidate.memory_total_kb.unwrap_or_default());

            if let Some(gpu_vendor) = candidate.gpu_vendor.as_deref() {
                row.set_gpu_vendor(gpu_vendor);
            }
            row.set_gpu_count(candidate.gpu_count.unwrap_or_default());

            let mut gpu_models = row
                .reborrow()
                .init_gpu_models(candidate.gpu_models.len() as u32);
            for (gpu_idx, model) in candidate.gpu_models.iter().enumerate() {
                gpu_models.set(gpu_idx as u32, model);
            }
        }

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

        let operation = ClusterOperationRecord {
            id: Uuid::new_v4(),
            kind: ClusterOperationKind::Merge,
            stage: ClusterOperationStage::Proposed,
            dry_run,
            source_views: vec![source_view],
            target_views: vec![destination_view],
            split_assignments: Vec::new(),
            split_service_policy: SplitServicePolicy::default(),
            split_network_policy: SplitNetworkPolicy::default(),
            merge_service_policy,
            updated_at_unix_ms: Self::now_unix_ms(),
            details: format!(
                "merge proposed: source={source_view}, destination={destination_view}, dry_run={dry_run}, service_policy={merge_service_policy:?}"
            ),
        };
        self.persist_cluster_operation(&operation)?;
        if !dry_run {
            let _ = self.broadcast_cluster_operation(&operation).await?;
        }
        self.trigger_operation_progress(operation.id, dry_run);

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
        if targets.is_empty() {
            return Err(capnp::Error::failed(
                "split request requires at least one target".into(),
            ));
        }

        let mut seen_names = HashSet::<String>::new();
        let mut target_specs = Vec::with_capacity(targets.len() as usize);
        let mut target_views = Vec::with_capacity(targets.len() as usize);
        let mut detail_targets = Vec::with_capacity(targets.len() as usize);

        for idx in 0..targets.len() {
            let target = targets.get(idx);
            let name = target.get_name()?.to_string()?;
            if name.trim().is_empty() {
                return Err(capnp::Error::failed(
                    "split target name must not be empty".into(),
                ));
            }
            if !seen_names.insert(name.clone()) {
                return Err(capnp::Error::failed(format!(
                    "duplicate split target name: {name}"
                )));
            }

            let selector = target.get_selector()?;
            let clauses_reader = selector.get_clauses()?;
            let explicit_nodes_reader = selector.get_explicit_nodes()?;
            let clause_count = clauses_reader.len();
            let explicit_count = explicit_nodes_reader.len();
            let mut clauses = Vec::with_capacity(clause_count as usize);
            for clause_index in 0..clauses_reader.len() {
                let clause = clauses_reader.get(clause_index);
                let key = clause.get_key()?.to_string()?;
                if key.trim().is_empty() {
                    return Err(capnp::Error::failed(
                        "split selector clause key must not be empty".into(),
                    ));
                }

                clauses.push(SplitSelectorClauseSpec {
                    key,
                    op: clause.get_op()?,
                    value: clause.get_value()?.to_string()?,
                });
            }

            let mut explicit_nodes = HashSet::with_capacity(explicit_count as usize);
            for node_index in 0..explicit_nodes_reader.len() {
                let node_id = read_node_id(explicit_nodes_reader.get(node_index))?;
                if !explicit_nodes.insert(node_id) {
                    return Err(capnp::Error::failed(format!(
                        "split target '{name}' contains duplicate explicit node {node_id}"
                    )));
                }
            }

            let mut hasher = Sha256::new();
            hasher.update(source_view.cluster_id.as_bytes());
            hasher.update(source_view.epoch.to_le_bytes());
            hasher.update(name.as_bytes());
            let digest = hasher.finalize();
            let mut cluster_bytes = [0u8; 16];
            cluster_bytes.copy_from_slice(&digest[..16]);
            let target_cluster = ClusterId::from_bytes(cluster_bytes);
            let view = ClusterViewId::new(target_cluster, source_view.epoch.saturating_add(1));
            target_views.push(view);
            target_specs.push(SplitTargetSpec {
                name: name.clone(),
                clauses,
                explicit_nodes,
            });
            detail_targets.push(format!(
                "{name}(clauses={clause_count}, explicit_nodes={explicit_count}, view={view})"
            ));
        }

        let split_assignments = self
            .build_split_assignments(source_view, &target_specs)
            .await?;
        let mut assignments_per_target = vec![0usize; target_views.len()];
        for assignment in &split_assignments {
            if let Some(slot) = assignments_per_target.get_mut(assignment.target_index) {
                *slot = slot.saturating_add(1);
            }
        }
        let assignment_detail = target_specs
            .iter()
            .enumerate()
            .map(|(idx, target)| format!("{}={}", target.name, assignments_per_target[idx]))
            .collect::<Vec<_>>()
            .join(", ");

        let operation = ClusterOperationRecord {
            id: Uuid::new_v4(),
            kind: ClusterOperationKind::Split,
            stage: ClusterOperationStage::Proposed,
            dry_run,
            source_views: vec![source_view],
            target_views: target_views.clone(),
            split_assignments,
            split_service_policy,
            split_network_policy,
            merge_service_policy: MergeServicePolicy::default(),
            updated_at_unix_ms: Self::now_unix_ms(),
            details: format!(
                "split proposed: source={source_view}, dry_run={dry_run}, service_policy={split_service_policy:?}, network_policy={split_network_policy:?}, targets=[{}], assignments=[{}]",
                detail_targets.join(", "),
                assignment_detail
            ),
        };
        self.persist_cluster_operation(&operation)?;
        if !dry_run {
            let _ = self.broadcast_cluster_operation(&operation).await?;
        }
        self.trigger_operation_progress(operation.id, dry_run);

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
        let mut incoming: ClusterOperationRecord =
            bincode::deserialize(payload).map_err(|e| capnp::Error::failed(e.to_string()))?;
        if incoming.updated_at_unix_ms == 0 {
            incoming.updated_at_unix_ms = Self::now_unix_ms();
        }
        if incoming.id != operation_id {
            return Err(capnp::Error::failed(format!(
                "relayed operation id mismatch: envelope={operation_id}, payload={}",
                incoming.id
            )));
        }
        if let Some(active) = self.active_cluster_operation()? {
            if active.id != operation_id {
                return Err(capnp::Error::failed(format!(
                    "cannot accept operation {operation_id} while operation {} ({:?}/{:?}) is in progress",
                    active.id, active.kind, active.stage
                )));
            }
        }

        let merged = match self.load_cluster_operation(operation_id)? {
            Some(current)
                if Self::stage_rank(current.stage) >= Self::stage_rank(incoming.stage) =>
            {
                current
            }
            _ => {
                self.persist_cluster_operation(&incoming)?;
                incoming
            }
        };

        if merged.dry_run {
            return Ok(());
        }

        match merged.stage {
            ClusterOperationStage::Proposed
            | ClusterOperationStage::Prepared
            | ClusterOperationStage::Committed => {
                self.trigger_operation_progress(merged.id, false);
            }
            ClusterOperationStage::Finalized => {
                let target = self.local_target_view_for_operation(&merged)?;
                if merged.kind == ClusterOperationKind::Merge
                    || self.active_cluster_view() != target
                {
                    if let Err(err) = self.apply_committed_operation_side_effects(&merged).await {
                        if Self::is_commit_precondition_failure(&err) {
                            warn!(
                                target: "cluster_view",
                                operation_id = %merged.id,
                                "skipped finalized operation side effects due to commit precondition mismatch: {err}"
                            );
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
            ClusterOperationStage::Aborted => {}
        }

        let _ = self.garbage_collect_cluster_operations()?;

        Ok(())
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
        counts.insert(local_view, 1);

        let (actives, _) = self
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;
        for (key, _snapshot) in actives {
            let peer_id = key.to_uuid();
            if peer_id == self.node.id {
                continue;
            }
            if excluded_peers.contains(&peer_id) {
                continue;
            }

            // When no cached session is available yet, treat the peer as part of the
            // local active view until a concrete remote view is observed.
            let view = self
                .best_known_peer_view(peer_id)
                .await
                .unwrap_or(local_view);
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

        let mut rows = counts
            .into_iter()
            .filter(|(view, node_count)| {
                *node_count > 0 && (*view == local_view || !retired_views.contains(view))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|(left, _), (right, _)| {
            left.cluster_id
                .as_bytes()
                .cmp(right.cluster_id.as_bytes())
                .then(left.epoch.cmp(&right.epoch))
        });

        let mut list = results.get().init_views(rows.len() as u32);
        for (idx, (view, node_count)) in rows.into_iter().enumerate() {
            let mut row = list.reborrow().get(idx as u32);
            view.write_capnp(row.reborrow().init_view());
            row.set_node_count(node_count);
            row.set_local_active(view == local_view);
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

pub fn read_topology_event(reader: topology_event::Reader) -> Result<TopologyEvent, capnp::Error> {
    use topology_event::EventType;

    let node = reader.get_node()?;
    let id = read_node_id(node.get_id()?)?;
    let event = match reader.get_event()? {
        EventType::Add => {
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
            }
        }
        EventType::Remove => TopologyEvent::Leave { id },
        EventType::Alive => TopologyEvent::Alive {
            id,
            incarnation: node.get_incarnation(),
        },
        EventType::Suspect => TopologyEvent::Suspect {
            id,
            incarnation: node.get_incarnation(),
        },
        EventType::Down => TopologyEvent::Down {
            id,
            incarnation: node.get_incarnation(),
        },
    };

    Ok(event)
}

pub fn add_event(
    list: &mut capnp::struct_list::Builder<gossip_message::Owned>,
    index: u32,
    event: &TopologyEvent,
    cluster_view: ClusterViewId,
) {
    let msg = list.reborrow().get(index);

    match event {
        TopologyEvent::Join {
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
        } => {
            let mut topo = msg.init_topology();

            topo.set_event(topology_event::EventType::Add);
            let mut node = topo.init_node();

            set_node_id(node.reborrow().init_id(), id);
            cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
            node.set_hostname(hostname);
            node.set_addr(address);
            node.set_root_hash(root_hash);
            node.set_public_key(&noise_static_pub.to_bytes());
            node.set_signing_key(&signing_pub.to_bytes());
            node.set_identity_sig(identity_sig);
            node.set_incarnation(*incarnation);
            if let Some(wg) = wireguard.as_ref() {
                node.set_wireguard_public_key(&wg.public_key);
                node.set_wireguard_port(wg.port);
                node.set_wireguard_enabled(wg.enabled);
            }

            if let Some(client) = client {
                // Only embed our own handle; forwarding a capability learned from another peer
                // can’t be re-exported on this connection safely.
                // Set the handle as a Cap’n Proto client only when available locally.
                node.set_handle(client.clone());
            }
        }

        TopologyEvent::Leave { id } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Remove);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), id);
            cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
        }

        TopologyEvent::Alive { id, incarnation } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Alive);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), id);
            cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
            node.set_incarnation(*incarnation);
        }

        TopologyEvent::Suspect { id, incarnation } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Suspect);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), id);
            cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
            node.set_incarnation(*incarnation);
        }

        TopologyEvent::Down { id, incarnation } => {
            let mut topo = msg.init_topology();
            topo.set_event(topology_event::EventType::Down);
            let mut node = topo.init_node();
            set_node_id(node.reborrow().init_id(), id);
            cluster_view.write_capnp(node.reborrow().init_active_cluster_view());
            node.set_incarnation(*incarnation);
        }
    }
}
