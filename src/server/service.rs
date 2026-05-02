use super::Server;
use crate::cluster::ClusterViewId;
use crate::crypto::rand;
use crate::node::id;
use crate::server::credential::ClusterCredential;
use crate::topology::TopologyEvent;
use crate::topology::peers::PeerValue;
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
    server_handle: mantissa_protocol::server::server::Client,
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
    fn from_params(
        params: mantissa_protocol::server::RegisterNodeParams,
    ) -> Result<Self, capnp::Error> {
        let params = params.get()?;
        let info = params.get_info()?;
        let joiner_id = id::read_node_id(info.get_id()?)?;
        let join_token = params.get_token()?.to_string()?;
        let server_handle = info.get_handle()?;
        let active_view = ClusterViewId::from_capnp(info.get_active_cluster_view()?)
            .map_err(capnp::Error::failed)?;

        let root_hash = info
            .get_root_hash()
            .ok()
            .and_then(|hash| hash.to_str().ok())
            .unwrap_or_default()
            .to_string();

        let peer = PeerValue::from_node_info(joiner_id, info)?;
        let noise_static_pub = PublicKey::from(peer.noise_static_pub);
        let signing_vk = ed25519_dalek::VerifyingKey::from_bytes(&peer.signing_pub)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;
        let identity_sig = peer.identity_sig.clone();
        let incarnation = peer.membership.incarnation;

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
            incarnation,
        })
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
            root_schema: self.peer.root_schema,
        }
    }
}

/// Cluster session handle returned to a newly joined node.
///
/// Grouping the issued ticket, credential, and session capability together
/// keeps response population separate from issuance logic.
struct ClusterSession {
    ticket: Vec<u8>,
    ticket_expires_at_unix_secs: u64,
    credential: Vec<u8>,
    session: mantissa_protocol::server::cluster_session::Client,
}

impl Server {
    /// Validates the parsed join request against current local server state.
    ///
    /// This is the gate between static request parsing and any topology or
    /// session side effects.
    async fn validate_join_request(&self, request: &JoinRequest) -> Result<(), capnp::Error> {
        if !self.auth.join_tokens.matches(&request.join_token).await {
            crate::observability::metrics::record_auth_failure("join", "invalid_token");
            return Err(capnp::Error::failed("invalid join token".to_string()));
        }

        if let Err(error) = self.topology.ensure_join_allowed() {
            crate::observability::metrics::record_auth_failure("join", "not_allowed");
            return Err(error);
        }

        if request.joiner_id == self.identity.id {
            crate::observability::metrics::record_auth_failure("join", "self_join");
            return Err(capnp::Error::failed("cannot join self".to_string()));
        }

        let active_view = self.topology.active_cluster_view();
        if request.active_view != active_view {
            crate::observability::metrics::record_auth_failure("join", "view_mismatch");
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
            crate::observability::metrics::record_auth_failure("join", "already_registered");
            return Err(capnp::Error::failed("node already registered".to_string()));
        }

        if crate::cluster::RootSchemaInfo::highest_common_version(
            self.topology.root_schema_info(),
            request.peer.root_schema,
        )
        .is_none()
        {
            crate::observability::metrics::record_auth_failure("join", "root_schema_mismatch");
            return Err(capnp::Error::failed(
                "register_node root schema mismatch: no compatible version overlap".to_string(),
            ));
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
        let issued_ticket = self
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
            session: self
                .sessions
                .new_peer_client(joiner_id, issued_ticket.ticket.clone()),
            ticket: issued_ticket.ticket,
            ticket_expires_at_unix_secs: issued_ticket.expires_at_unix_secs,
            credential,
        })
    }

    /// Populates the join response with a cluster session handle and local node
    /// info.
    ///
    /// This keeps the Cap'n Proto writer manipulation out of the main control
    /// flow and ensures all successful joins return the same response shape.
    fn write_join_response(
        &self,
        results: &mut mantissa_protocol::server::RegisterNodeResults,
        cluster_session: &ClusterSession,
    ) -> Result<(), capnp::Error> {
        let mut out = results.get();
        out.set_session(cluster_session.session.clone());
        out.set_ticket(&cluster_session.ticket);
        out.set_ticket_expires_at_unix_secs(cluster_session.ticket_expires_at_unix_secs);

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

impl mantissa_protocol::server::Server for Server {
    async fn register_node(
        self: Rc<Self>,
        params: mantissa_protocol::server::RegisterNodeParams,
        mut results: mantissa_protocol::server::RegisterNodeResults,
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
        params: mantissa_protocol::server::GetSessionParams,
        mut results: mantissa_protocol::server::GetSessionResults,
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
            crate::observability::metrics::record_auth_failure("session_ticket", "unknown");
            return Err(capnp::Error::failed("unknown session ticket".to_string()));
        };

        if !self
            .topology
            .peer_exists(peer_id)
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        {
            crate::observability::metrics::record_auth_failure(
                "session_ticket",
                "peer_not_registered",
            );
            return Err(capnp::Error::failed("peer not registered".to_string()));
        }
        results
            .get()
            .set_session(self.sessions.new_peer_client(peer_id, ticket.to_vec()));
        Ok(())
    }

    async fn get_with_credential(
        self: Rc<Self>,
        params: mantissa_protocol::server::GetWithCredentialParams,
        mut results: mantissa_protocol::server::GetWithCredentialResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;
        self.ensure_local_cluster_membership_active()?;

        let cred_bytes = params.get()?.get_credential()?;
        let cred = match ClusterCredential::from_bytes_verified(cred_bytes) {
            Ok(credential) => credential,
            Err(error) => {
                crate::observability::metrics::record_auth_failure("credential", "invalid");
                return Err(capnp::Error::failed(error));
            }
        };

        if !self
            .topology
            .peer_exists(cred.subject)
            .map_err(|error| capnp::Error::failed(error.to_string()))?
        {
            crate::observability::metrics::record_auth_failure("credential", "peer_not_registered");
            return Err(capnp::Error::failed(
                "peer not registered on this node".to_string(),
            ));
        }
        if let Some(expected_vk) = self.topology.signing_vk_for(cred.subject) {
            if expected_vk != cred.issuer {
                crate::observability::metrics::record_auth_failure("credential", "issuer_mismatch");
                debug!(target: "server", subject=%cred.subject, "issuer mismatch for");
                return Err(capnp::Error::failed(
                    "issuer mismatch for subject".to_string(),
                ));
            }
        } else {
            crate::observability::metrics::record_auth_failure("credential", "issuer_unknown");
            debug!(target: "server", subject=%cred.subject, "issuer unknown (not yet synced)");
            return Err(capnp::Error::failed(
                "issuer unknown (not yet synced)".to_string(),
            ));
        }

        debug!(target: "server", "Peer {} authenticated", cred.subject);

        let issued_ticket = self
            .auth
            .sessions
            .issue_ticket(cred.subject)
            .map_err(|error| capnp::Error::failed(error.to_string()))?;

        let mut out = results.get();
        out.set_session(
            self.sessions
                .new_peer_client(cred.subject, issued_ticket.ticket.clone()),
        );
        out.set_ticket(&issued_ticket.ticket);
        out.set_ticket_expires_at_unix_secs(issued_ticket.expires_at_unix_secs);

        let node_info = out.reborrow().init_node_info();
        self.topology.populate_self_node_info(node_info);

        Ok(())
    }
}
