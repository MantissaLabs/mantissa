use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::topology::peers::PeerValue;
use ::health::HealthMonitor;
use ed25519_dalek::SigningKey;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::health;
use protocol::server::{self, cluster_session};
use protocol::sync;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::error;
use uuid::Uuid;

/// Internal map storing server handles per peer.
type HandleMap = Arc<RwLock<HashMap<Uuid, server::Client>>>;

#[derive(Clone)]
pub struct Registry {
    handles: HandleMap,
    sessions: LocalSessionStore,
    peers: PeersStore,
    signing_key: Arc<AsyncMutex<SigningKey>>,
    node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
}

#[derive(Clone, Copy)]
enum SessionStrategy {
    TicketOnly,
    TicketThenCredential,
}

impl Registry {
    pub fn new(
        peers: PeersStore,
        sessions: LocalSessionStore,
        signing_key: SigningKey,
        node_id: Uuid,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            handles: Arc::new(RwLock::new(HashMap::new())),
            sessions,
            peers,
            signing_key: Arc::new(AsyncMutex::new(signing_key)),
            node_id,
            health_monitor,
        }
    }

    pub async fn register_peer_handle(&self, id: Uuid, handle: server::Client) {
        self.handles.write().await.insert(id, handle);
    }

    pub async fn attach_handle_only(&self, id: Uuid, handle: server::Client) {
        self.handles.write().await.insert(id, handle);
    }

    pub async fn remove_peer(&self, id: Uuid) {
        self.handles.write().await.remove(&id);
    }

    pub async fn clear(&self) {
        self.handles.write().await.clear();
    }

    pub async fn server_handle_for(&self, peer_id: Uuid) -> Option<server::Client> {
        let guard = self.handles.read().await;
        guard.get(&peer_id).cloned()
    }

    pub async fn refresh_peer_handle(&self, peer_id: Uuid) -> Option<server::Client> {
        let peer = self.peer_latest_value(peer_id)?;

        let addr = peer.address.clone();

        {
            let mut guard = self.handles.write().await;
            guard.remove(&peer_id);
        }

        match Self::connect_to_peer(&addr).await {
            Ok(client) => {
                let mut guard = self.handles.write().await;
                guard.insert(peer_id, client.clone());
                Some(client)
            }
            Err(e) => {
                error!(target: "connect", "reconnect {addr} failed: {e}");
                None
            }
        }
    }

    pub async fn session_for_peer(&self, peer_id: Uuid) -> Option<cluster_session::Client> {
        if let Some(client) = self.server_handle_for(peer_id).await {
            if let Some(session) = self
                .session_for_strategy(&client, peer_id, SessionStrategy::TicketThenCredential)
                .await
            {
                return Some(session);
            }
        }

        let refreshed = self.refresh_peer_handle(peer_id).await?;

        self.session_for_strategy(&refreshed, peer_id, SessionStrategy::TicketThenCredential)
            .await
    }

    pub async fn scheduler_session_via_handle(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        self.session_for_strategy(client, peer_id, SessionStrategy::TicketThenCredential)
            .await
    }

    pub async fn connect_known_peers(&self, allow_credentials: bool) -> Result<(), capnp::Error> {
        let (actives, _tombs) = self
            .peers
            .load_all()
            .map_err(|e| capnp::Error::failed(e.to_string()))?;

        let strategy = if allow_credentials {
            SessionStrategy::TicketThenCredential
        } else {
            SessionStrategy::TicketOnly
        };

        for (k, snap) in actives {
            let peer_id = k.to_uuid();

            if peer_id == self.node_id {
                continue;
            }

            if self.handles.read().await.contains_key(&peer_id) {
                continue;
            }

            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address.clone();

            let client = match Self::connect_to_peer(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    error!(target: "connect", "dial {addr} failed: {e}");
                    continue;
                }
            };

            let Some(session) = self.session_for_strategy(&client, peer_id, strategy).await else {
                if !allow_credentials {
                    error!(target: "connect", "no ticket and no signing key; skipping {addr}");
                }
                continue;
            };

            self.handles.write().await.insert(peer_id, client.clone());

            let _ = session.ping_request().send().promise.await.map(|_| {
                self.health_monitor.observe_seen(peer_id);
            });
        }

        Ok(())
    }

    pub async fn resume_sessions_on_boot(&self, local_addr: &str) {
        println!("Resuming sessions with peers...");

        let mut addr_map = HashMap::<Uuid, String>::new();
        if let Ok((actives, _tombs)) = self.peers.load_all() {
            for (k, snap) in actives {
                let id = k.to_uuid();

                if id == self.node_id {
                    continue;
                }

                if let Some(val) = snap.as_slice().last().cloned() {
                    if val.address == local_addr {
                        continue;
                    }
                    addr_map.insert(id, val.address);
                }
            }
        }

        let entries = match self.sessions.list() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("resume: cannot list local session tickets: {e}");
                return;
            }
        };

        for (peer_id, ticket) in entries {
            let Some(addr) = addr_map.get(&peer_id) else {
                eprintln!("resume: peer {peer_id} has no known address; skipping");
                continue;
            };

            match Self::connect_to_peer(addr).await {
                Ok(client) => {
                    let mut req = client.get_session_request();
                    req.get().set_ticket(&ticket);
                    match req.send().promise.await {
                        Ok(resp) => match resp.get().and_then(|r| r.get_session()) {
                            Ok(session) => {
                                self.attach_handle_only(peer_id, client.clone()).await;
                                let _ = session.ping_request().send().promise.await.map(|_| {
                                    self.health_monitor.observe_seen(peer_id);
                                });

                                println!("Session established with peer {peer_id} @ {addr}");
                            }
                            Err(e) => eprintln!("resume: decode failed for {peer_id}: {e}"),
                        },
                        Err(e) => {
                            eprintln!("resume: get_session RPC failed for {peer_id} @ {addr}: {e}")
                        }
                    }
                }
                Err(e) => eprintln!("resume: connect to {addr} failed for {peer_id}: {e}"),
            }
        }
    }

    pub async fn fetch_sync_capability(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<sync::Client>, capnp::Error> {
        let Some(session) = self.session_for_peer(peer_id).await else {
            return Ok(None);
        };

        let req = session.get_sync_request();
        let resp = req.send().promise.await?;
        let sync_cap = resp.get()?.get_sync()?;
        Ok(Some(sync_cap))
    }

    pub async fn fetch_health_capability(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<health::health::Client>, capnp::Error> {
        let client = match self.refresh_peer_handle(peer_id).await {
            Some(handle) => handle,
            None => return Ok(None),
        };

        let Some(session) = self
            .session_for_strategy(&client, peer_id, SessionStrategy::TicketThenCredential)
            .await
        else {
            return Ok(None);
        };

        let req = session.get_capabilities_request();
        let resp = req.send().promise.await?;
        let caps = resp.get()?.get_caps()?;
        caps.get_health().map(Some)
    }

    pub async fn gossip_client_for(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        let Some(session) = self.session_for_peer(peer_id).await else {
            return Ok(None);
        };

        let req = session.get_gossip_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_gossip().map(Some)
    }

    async fn session_for_strategy(
        &self,
        client: &server::Client,
        peer_id: Uuid,
        strategy: SessionStrategy,
    ) -> Option<cluster_session::Client> {
        let mut session = self.session_via_ticket(client, peer_id).await;

        if session.is_none() && matches!(strategy, SessionStrategy::TicketThenCredential) {
            session = self.session_via_credential(client, peer_id).await;
        }

        session
    }

    async fn session_via_ticket(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let ticket = match self.sessions.get(peer_id) {
            Ok(Some(t)) => t,
            _ => return None,
        };

        let mut req = client.get_session_request();
        req.get().set_ticket(&ticket);
        match req.send().promise.await {
            Ok(resp) => match resp.get() {
                Ok(r) => r.get_session().ok(),
                Err(e) => {
                    error!(target: "sync", "get_session response error: {e}");
                    None
                }
            },
            Err(e) => {
                error!(target: "sync", "get_session failed: {e}");
                None
            }
        }
    }

    async fn session_via_credential(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        let cred_bytes = {
            let sk_guard = self.signing_key.lock().await;
            let cred = crate::server::credential::ClusterCredential::sign(
                &sk_guard,
                self.node_id,
                3600,
                crate::crypto::rand::nonce16(),
            );
            match cred.to_bytes() {
                Ok(b) => b,
                Err(e) => {
                    error!(target: "sync", "credential serialize failed: {e}");
                    return None;
                }
            }
        };

        let mut req = client.get_with_credential_request();
        req.get().set_credential(&cred_bytes);

        match req.send().promise.await {
            Ok(resp) => {
                let r = match resp.get() {
                    Ok(r) => r,
                    Err(e) => {
                        error!(target: "sync", "getWithCredential response error: {e}");
                        return None;
                    }
                };

                if let Ok(ni) = r.get_node_info() {
                    if let Ok(v) = PeerValue::from_node_info(ni) {
                        if let Err(e) = self
                            .peers
                            .upsert(&crdt_store::uuid_key::UuidKey::from(peer_id), v)
                            .await
                        {
                            error!(target: "sync", "upsert nodeInfo failed for {peer_id}: {e}");
                        }
                    }
                }

                if let Err(e) = self.sessions.put(peer_id, r.get_ticket().ok()?) {
                    error!(target: "sync", "ticket persist failed for {peer_id}: {e}");
                }

                r.get_session().ok()
            }
            Err(e) => {
                error!(target: "sync", "getWithCredential failed: {e}");
                None
            }
        }
    }

    fn peer_latest_value(&self, peer_id: Uuid) -> Option<PeerValue> {
        let (actives, _) = self.peers.load_all().ok()?;
        actives
            .into_iter()
            .find(|(k, _)| k.to_uuid() == peer_id)
            .and_then(|(_, snap)| snap.as_slice().last().cloned())
    }

    async fn connect_to_peer(addr: &str) -> Result<server::Client, String> {
        client::connection::get_client_secure(addr)
            .await
            .map_err(|e| e.to_string())
    }
}
