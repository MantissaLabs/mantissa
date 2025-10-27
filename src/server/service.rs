use super::Server;
use crate::crypto::rand;
use crate::node::id;
use crate::node::identity::pubkey_from_slice;
use crate::server::credential::ClusterCredential;
use crate::topology::TopologyEvent;
use crate::topology::peers::PeerValue;
use tracing::debug;

impl protocol::server::Server for Server {
    async fn register_node(
        &self,
        params: protocol::server::RegisterNodeParams,
        mut results: protocol::server::RegisterNodeResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        let p = params.get()?;
        let info = p.get_info()?;
        let token = p.get_token()?.to_string()?;
        let handle = info.get_handle()?;

        // Join token check.
        if !self.stores.token_store.matches(&token).await {
            return Err(capnp::Error::failed("invalid join token".to_string()));
        }

        let joiner_id = id::read_node_id(info.get_id()?)?;
        if joiner_id == self.id {
            return Err(capnp::Error::failed("cannot join self".to_string()));
        }

        // Already registered?
        let exists = self
            .topology
            .peer_exists(joiner_id)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        if exists {
            return Err(capnp::Error::failed("node already registered".to_string()));
        }

        // Upsert peer into store (MST will update)
        let hostname = info.get_hostname()?.to_string()?;
        let address = info.get_addr()?.to_string()?;

        let root_hash = info
            .get_root_hash()
            .ok()
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default()
            .to_string();

        let public_key = info.get_public_key()?;
        let pubkey = pubkey_from_slice(public_key).expect("expect valid public key");

        let sk_vec = info.get_signing_key()?.to_vec();
        let sk_arr: [u8; 32] = sk_vec.as_slice().try_into().map_err(|_| {
            capnp::Error::failed("signing key must be exactly 32 bytes".to_string())
        })?;

        let signing_vk = ed25519_dalek::VerifyingKey::from_bytes(&sk_arr)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let signing_pub = signing_vk.to_bytes();

        let peer = PeerValue {
            address,
            hostname,
            noise_static_pub: pubkey.to_bytes(),
            signing_pub,
        };

        self.topology
            .register_peer(joiner_id, &peer, Some(handle.clone()))
            .await
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        // Issue session ticket.
        let ticket = self
            .stores
            .session_store
            .issue_ticket(joiner_id)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let nonce = rand::try_nonce16().map_err(|e| capnp::Error::failed(e.to_string()))?;

        const TTL_SECS: u64 = 3600; // 1 hour (tune it)
        let cred = ClusterCredential::sign(&self.signing_key, joiner_id, TTL_SECS, nonce);
        let cred_bytes = cred.to_bytes().map_err(capnp::Error::failed)?;
        let session_client = self.new_session_client();

        // Ensure the periodic sync loop is running on this node as soon as we have a cluster
        // at least two nodes.
        {
            let topo = self.topology.clone();
            tokio::task::spawn_local(async move {
                topo.ensure_periodic_sync();
            });
        }

        let mut out = results.get();
        out.set_session(session_client);
        out.set_ticket(&ticket);

        // Include our NodeInfo so the joiner can immediately insert to its store.
        // Fast propagation of our info means we can get a session to the joiner fast.
        let ni = out.reborrow().init_node_info();
        self.topology.populate_self_node_info(ni);
        out.set_credential(&cred_bytes);

        // Gossip event to other peers.
        let join_event = TopologyEvent::Join {
            id: joiner_id,
            hostname: peer.hostname.clone(),
            address: peer.address.clone(),
            root_hash,
            client: Some(handle.clone()),
            noise_static_pub: pubkey,
            signing_pub: Box::new(signing_vk),
        };

        self.topology.gossip_topology_event(join_event).await?;

        Ok(())
    }

    async fn get_session(
        &self,
        params: protocol::server::GetSessionParams,
        mut results: protocol::server::GetSessionResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        let ticket = params.get()?.get_ticket()?;
        let Some(peer_id) = self
            .stores
            .session_store
            .lookup(ticket)
            .map_err(|e| capnp::Error::failed(e.to_string()))?
        else {
            return Err(capnp::Error::failed("unknown session ticket".to_string()));
        };

        if !self
            .topology
            .peer_exists(peer_id)
            .map_err(|e| capnp::Error::failed(e.to_string()))?
        {
            return Err(capnp::Error::failed("peer not registered".to_string()));
        }

        let session_client = self.new_session_client();
        results.get().set_session(session_client);
        Ok(())
    }

    async fn get_with_credential(
        &self,
        params: protocol::server::GetWithCredentialParams,
        mut results: protocol::server::GetWithCredentialResults,
    ) -> Result<(), capnp::Error> {
        self.ensure_online()?;

        // Parse + Verify the signed blob
        let cred_bytes = params.get()?.get_credential()?;
        let cred =
            ClusterCredential::from_bytes_verified(cred_bytes).map_err(capnp::Error::failed)?;

        // We must already know the subject as a registered peer
        if !self
            .topology
            .peer_exists(cred.subject)
            .map_err(|e| capnp::Error::failed(e.to_string()))?
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
            // Likely not yet synced, reject for now and the next sync tick will succeed.
            debug!(target: "server", subject=%cred.subject, "issuer unknown (not yet synced)");
            return Err(capnp::Error::failed(
                "issuer unknown (not yet synced)".to_string(),
            ));
        }

        debug!(target: "server", "Peer {} authenticated", cred.subject);

        // Mint a fresh ticket for the subject
        let ticket = self
            .stores
            .session_store
            .issue_ticket(cred.subject)
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        // Return session + ticket + our peer id (so caller can persist)
        let session_client = self.new_session_client();

        let mut out = results.get();
        out.set_session(session_client);
        out.set_ticket(&ticket);

        // Include our NodeInfo so the caller can upsert immediately.
        let ni = out.reborrow().init_node_info();
        self.topology.populate_self_node_info(ni);

        Ok(())
    }
}
