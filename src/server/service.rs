use super::Server;
use crate::cluster::ClusterViewId;
use crate::crypto::rand;
use crate::node::id;
use crate::node::identity::{pubkey_from_slice, verify_peer_identity};
use crate::server::credential::ClusterCredential;
use crate::topology::TopologyEvent;
use crate::topology::peers::{
    PeerMembership, PeerSchedulingState, PeerValue, WireGuardPeerValue, labels_from_node_info,
    runtime_support_from_node_info,
};
use std::rc::Rc;
use tracing::{debug, warn};
use x25519_dalek::PublicKey;

/// Typed join request assembled from the Cap'n Proto payload.
///
/// `register_node()` now operates on this parsed form so validation,
/// persistence, response generation, and gossip publication are easier to
/// follow than when they were interleaved in one large RPC method.
struct JoinRequest {
    join_token: String,
    joiner_id: uuid::Uuid,
    active_view: ClusterViewId,
    root_hash: String,
    server_handle: protocol::server::server::Client,
    peer: PeerValue,
    noise_static_pub: PublicKey,
    signing_vk: ed25519_dalek::VerifyingKey,
    identity_sig: Vec<u8>,
    incarnation: u64,
}

impl JoinRequest {
    /// Parses and validates the static fields of a join request.
    ///
    /// This converts the wire format into strongly typed Rust values before the
    /// server mutates topology state or issues any server session handle.
    fn from_params(params: protocol::server::RegisterNodeParams) -> Result<Self, capnp::Error> {
        let params = params.get()?;
        let info = params.get_info()?;
        let joiner_id = id::read_node_id(info.get_id()?)?;
        let join_token = params.get_token()?.to_string()?;
        let server_handle = info.get_handle()?;
        let active_view = ClusterViewId::from_capnp(info.get_active_cluster_view()?)
            .map_err(capnp::Error::failed)?;

        let hostname = info.get_hostname()?.to_string()?;
        let address = info.get_addr()?.to_string()?;
        let root_hash = info
            .get_root_hash()
            .ok()
            .and_then(|hash| hash.to_str().ok())
            .unwrap_or_default()
            .to_string();

        let noise_static_pub = pubkey_from_slice(info.get_public_key()?)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let signing_vk = Self::parse_signing_verifying_key(info.get_signing_key()?)?;
        let identity_sig = Self::parse_identity_signature(info.get_identity_sig()?)?;
        verify_peer_identity(
            &signing_vk,
            &joiner_id,
            &noise_static_pub.to_bytes(),
            &identity_sig,
        )
        .map_err(|error| capnp::Error::failed(error.to_string()))?;

        let peer = PeerValue {
            address,
            hostname,
            platform_os: info.get_platform_os()?.to_string()?,
            platform_arch: info.get_platform_arch()?.to_string()?,
            noise_static_pub: noise_static_pub.to_bytes(),
            signing_pub: signing_vk.to_bytes(),
            identity_sig: identity_sig.clone(),
            wireguard: Self::parse_wireguard(
                info.get_wireguard_public_key()?,
                info.get_wireguard_port(),
                info.get_wireguard_enabled(),
            )?,
            scheduling: PeerSchedulingState::from_node_info(
                joiner_id,
                info.get_schedulable(),
                info.get_drain_requested(),
                info.get_scheduling_updated_at_unix_ms(),
                Self::parse_scheduling_actor(info.get_scheduling_actor_node_id()?.get_bytes()?)?,
                Some(info.get_scheduling_reason()?.to_string()?),
                match info.get_drain_task_stop_timeout_secs() {
                    0 => None,
                    value => Some(value),
                },
            ),
            labels: labels_from_node_info(info)?,
            runtime_support: runtime_support_from_node_info(info)?,
            membership: PeerMembership::active(info.get_incarnation()),
        };

        Ok(Self {
            join_token,
            joiner_id,
            active_view,
            root_hash,
            server_handle,
            peer,
            noise_static_pub,
            signing_vk,
            identity_sig,
            incarnation: info.get_incarnation(),
        })
    }

    /// Parses the peer signing key advertised in the join request.
    ///
    /// Join validation uses the verifying key to bind the peer id to its noise
    /// key before the node is accepted into topology state.
    fn parse_signing_verifying_key(
        bytes: &[u8],
    ) -> Result<ed25519_dalek::VerifyingKey, capnp::Error> {
        let key_bytes: [u8; 32] = bytes.try_into().map_err(|_| {
            capnp::Error::failed("signing key must be exactly 32 bytes".to_string())
        })?;
        ed25519_dalek::VerifyingKey::from_bytes(&key_bytes)
            .map_err(|error| capnp::Error::failed(error.to_string()))
    }

    /// Parses and validates the peer identity signature.
    ///
    /// This keeps the join parser responsible for static wire validation rather
    /// than leaving signature shape checks spread across the RPC method.
    fn parse_identity_signature(bytes: &[u8]) -> Result<Vec<u8>, capnp::Error> {
        if bytes.is_empty() {
            return Err(capnp::Error::failed(
                "identitySig must be set for peer identity verification".to_string(),
            ));
        }
        if bytes.len() != 64 {
            return Err(capnp::Error::failed(
                "identitySig must be exactly 64 bytes".to_string(),
            ));
        }
        Ok(bytes.to_vec())
    }

    /// Parses the optional WireGuard peer payload from the join request.
    ///
    /// The wire format allows the key to be absent, so the parser keeps that
    /// optionality localized here instead of in the main RPC flow.
    fn parse_wireguard(
        bytes: &[u8],
        port: u16,
        enabled: bool,
    ) -> Result<Option<WireGuardPeerValue>, capnp::Error> {
        if bytes.is_empty() {
            return Ok(None);
        }
        if bytes.len() != 32 {
            return Err(capnp::Error::failed(
                "wireguardPublicKey must be exactly 32 bytes".to_string(),
            ));
        }

        let mut public_key = [0u8; 32];
        public_key.copy_from_slice(bytes);
        Ok(Some(WireGuardPeerValue {
            public_key,
            port,
            enabled,
        }))
    }

    /// Parses the optional scheduling actor UUID carried in `NodeInfo`.
    ///
    /// Scheduling metadata belongs to the peer value, but the conversion from
    /// Cap'n Proto bytes to `Uuid` is clearer when kept near the join parser.
    fn parse_scheduling_actor(bytes: &[u8]) -> Result<Option<uuid::Uuid>, capnp::Error> {
        if bytes.is_empty() {
            Ok(None)
        } else {
            uuid::Uuid::from_slice(bytes)
                .map(Some)
                .map_err(|error| capnp::Error::failed(error.to_string()))
        }
    }

    /// Converts the parsed join request into the relayed topology event.
    ///
    /// Registering the peer and gossiping its join now share one authoritative
    /// parsed representation instead of rebuilding the event ad hoc.
    fn to_topology_event(&self) -> TopologyEvent {
        TopologyEvent::Join {
            id: self.joiner_id,
            hostname: self.peer.hostname.clone(),
            address: self.peer.address.clone(),
            platform_os: self.peer.platform_os.clone(),
            platform_arch: self.peer.platform_arch.clone(),
            root_hash: self.root_hash.clone(),
            incarnation: self.incarnation,
            client: Some(self.server_handle.clone()),
            noise_static_pub: self.noise_static_pub,
            signing_pub: Box::new(self.signing_vk),
            identity_sig: self.identity_sig.clone(),
            wireguard: self.peer.wireguard.clone(),
            scheduling: Box::new(self.peer.scheduling.clone()),
            labels: Box::new(self.peer.labels.clone()),
            runtime_support: Box::new(self.peer.runtime_support.clone()),
        }
    }
}

/// Cluster session handle returned to a newly joined node.
///
/// Grouping the issued ticket, credential, and session capability together
/// keeps response population separate from issuance logic.
struct ClusterSession {
    ticket: Vec<u8>,
    credential: Vec<u8>,
    session: protocol::server::cluster_session::Client,
}

impl Server {
    /// Validates the parsed join request against current local server state.
    ///
    /// This is the gate between static request parsing and any topology or
    /// session side effects.
    async fn validate_join_request(&self, request: &JoinRequest) -> Result<(), capnp::Error> {
        if !self.auth.join_tokens.matches(&request.join_token).await {
            return Err(capnp::Error::failed("invalid join token".to_string()));
        }

        self.topology.ensure_join_allowed()?;

        if request.joiner_id == self.identity.id {
            return Err(capnp::Error::failed("cannot join self".to_string()));
        }

        let active_view = self.topology.active_cluster_view();
        if request.active_view != active_view {
            return Err(capnp::Error::failed(format!(
                "register_node view mismatch: joiner {}, local {active_view}",
                request.active_view
            )));
        }

        let exists = self
            .topology
            .peer_exists(request.joiner_id)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        if exists {
            return Err(capnp::Error::failed("node already registered".to_string()));
        }

        Ok(())
    }

    /// Persists the peer join into topology state.
    ///
    /// Registration and SWIM state updates are grouped here so the main RPC
    /// method does not mix topology mutation with response generation.
    async fn register_join_request(&self, request: &JoinRequest) -> Result<(), capnp::Error> {
        self.topology
            .register_peer(
                request.joiner_id,
                &request.peer,
                Some(request.server_handle.clone()),
            )
            .await
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        self.topology
            .swim_record_join(request.joiner_id, request.incarnation);
        match self.topology.publish_local_cluster_node_count().await {
            Ok(true) => self.topology.sync_once_now(),
            Ok(false) => {}
            Err(err) => {
                warn!(
                    target: "cluster_view",
                    node_id = %request.joiner_id,
                    "failed to publish cluster node count after join: {err}"
                );
            }
        }
        Ok(())
    }

    /// Issues the ticket, credential, and cluster session capability for a joiner.
    ///
    /// Keeping this logic together makes the register flow read as "validate,
    /// persist, issue, respond, gossip" instead of one interleaved procedure.
    fn issue_join_session(&self, joiner_id: uuid::Uuid) -> Result<ClusterSession, capnp::Error> {
        let ticket = self
            .auth
            .sessions
            .issue_ticket(joiner_id)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let nonce = rand::try_nonce16().map_err(|error| capnp::Error::failed(error.to_string()))?;

        const TTL_SECS: u64 = 3600;
        let credential =
            ClusterCredential::sign(&self.identity.signing_key, joiner_id, TTL_SECS, nonce)
                .to_bytes()
                .map_err(capnp::Error::failed)?;

        Ok(ClusterSession {
            ticket,
            credential,
            session: self.sessions.new_client(),
        })
    }

    /// Populates the join response with a cluster session handle and local node
    /// info.
    ///
    /// This keeps the Cap'n Proto writer manipulation out of the main control
    /// flow and ensures all successful joins return the same response shape.
    fn write_join_response(
        &self,
        results: &mut protocol::server::RegisterNodeResults,
        cluster_session: &ClusterSession,
    ) -> Result<(), capnp::Error> {
        let mut out = results.get();
        out.set_session(cluster_session.session.clone());
        out.set_ticket(&cluster_session.ticket);

        let node_info = out.reborrow().init_node_info();
        self.topology.populate_self_node_info(node_info);
        out.set_credential(&cluster_session.credential);
        Ok(())
    }

    /// Ensures background cluster participation loops are running after the first successful join.
    ///
    /// Joining a second node is the earliest point where background sync starts
    /// to matter, so the server kicks it off asynchronously here.
    fn ensure_cluster_background_tasks_after_join(&self) {
        let topology = self.topology.clone();
        tokio::task::spawn_local(async move {
            topology.ensure_cluster_background_tasks();
        });
    }

    /// Rejects remote cluster-control RPCs once the local node has left the cluster.
    ///
    /// A left node may stay online for local CLI access, but it must not mint
    /// fresh peer sessions or accept new joins into a cluster it no longer
    /// participates in.
    fn ensure_local_cluster_membership_active(&self) -> Result<(), capnp::Error> {
        if self
            .topology
            .peer_exists(self.identity.id)
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        {
            Ok(())
        } else {
            Err(capnp::Error::failed(
                "node is not an active cluster member".to_string(),
            ))
        }
    }
}

impl protocol::server::Server for Server {
    async fn register_node(
        self: Rc<Self>,
        params: protocol::server::RegisterNodeParams,
        mut results: protocol::server::RegisterNodeResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        self.ensure_local_cluster_membership_active()?;

        let join_request = JoinRequest::from_params(params)?;
        self.validate_join_request(&join_request).await?;
        self.register_join_request(&join_request).await?;

        let cluster_session = self.issue_join_session(join_request.joiner_id)?;
        self.ensure_cluster_background_tasks_after_join();
        self.write_join_response(&mut results, &cluster_session)?;

        self.topology
            .gossip_topology_event(join_request.to_topology_event())
            .await?;
        Ok(())
    }

    async fn get_session(
        self: Rc<Self>,
        params: protocol::server::GetSessionParams,
        mut results: protocol::server::GetSessionResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        self.ensure_local_cluster_membership_active()?;

        let ticket = params.get()?.get_ticket()?;
        let Some(peer_id) = self
            .auth
            .sessions
            .lookup(ticket)
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        else {
            return Err(capnp::Error::failed("unknown session ticket".to_string()));
        };

        if !self
            .topology
            .peer_exists(peer_id)
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        {
            return Err(capnp::Error::failed("peer not registered".to_string()));
        }

        results.get().set_session(self.sessions.new_client());
        Ok(())
    }

    async fn get_with_credential(
        self: Rc<Self>,
        params: protocol::server::GetWithCredentialParams,
        mut results: protocol::server::GetWithCredentialResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        self.ensure_local_cluster_membership_active()?;

        let cred_bytes = params.get()?.get_credential()?;
        let cred =
            ClusterCredential::from_bytes_verified(cred_bytes).map_err(capnp::Error::failed)?;

        if !self
            .topology
            .peer_exists(cred.subject)
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        {
            return Err(capnp::Error::failed(
                "peer not registered on this node".to_string(),
            ));
        }

        if let Some(expected_vk) = self.topology.signing_vk_for(cred.subject) {
            if expected_vk != cred.issuer {
                debug!(target: "server", subject=%cred.subject, "issuer mismatch for");
                return Err(capnp::Error::failed(
                    "issuer mismatch for subject".to_string(),
                ));
            }
        } else {
            debug!(target: "server", subject=%cred.subject, "issuer unknown (not yet synced)");
            return Err(capnp::Error::failed(
                "issuer unknown (not yet synced)".to_string(),
            ));
        }

        debug!(target: "server", "Peer {} authenticated", cred.subject);

        let ticket = self
            .auth
            .sessions
            .issue_ticket(cred.subject)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;

        let mut out = results.get();
        out.set_session(self.sessions.new_client());
        out.set_ticket(&ticket);

        let node_info = out.reborrow().init_node_info();
        self.topology.populate_self_node_info(node_info);

        Ok(())
    }
}
