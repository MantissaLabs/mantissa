use crate::store::local_session_store::LocalSessionStore;
use crate::store::peer_store::PeersStore;
use crate::topology::peers::{PeerValue, WireGuardPeerValue};
use ::health::HealthMonitor;
use anyhow::{Result as AnyResult, anyhow};
use crdt_store::uuid_key::UuidKey;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use protocol::gossip::gossip::Client as GossipClient;
use protocol::health;
use protocol::server::{self, cluster_session};
use protocol::sync;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tracing::error;
use uuid::Uuid;

type PeerEntry = Arc<AsyncMutex<PeerState>>;
type CapabilityMap = Arc<RwLock<HashMap<Uuid, PeerEntry>>>;

#[derive(Default)]
struct PeerState {
    server: Option<server::Client>,
    session: Option<cluster_session::Client>,
    sync: Option<sync::Client>,
    health: Option<health::health::Client>,
    gossip: Option<GossipClient>,
}

impl PeerState {
    fn clear_capabilities(&mut self) {
        self.sync = None;
        self.health = None;
        self.gossip = None;
    }

    fn clear_session(&mut self) {
        self.session = None;
        self.clear_capabilities();
    }

    fn replace_server(&mut self, server: server::Client) {
        self.server = Some(server);
        self.clear_session();
    }

    fn replace_session(&mut self, session: cluster_session::Client) {
        self.session = Some(session);
        self.clear_capabilities();
    }

    fn clear_all(&mut self) {
        self.server = None;
        self.clear_session();
    }
}

#[derive(Clone)]
pub struct Registry {
    cache: CapabilityMap,
    sessions: LocalSessionStore,
    peers: PeersStore,
    signing_key: Arc<AsyncMutex<SigningKey>>,
    noise_keys: Arc<NoiseKeys>,
    node_id: Uuid,
    health_monitor: Arc<HealthMonitor>,
}

#[derive(Clone, Copy)]
enum SessionStrategy {
    TicketOnly,
    TicketThenCredential,
}

impl Registry {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(
        peers: PeersStore,
        sessions: LocalSessionStore,
        signing_key: SigningKey,
        noise_keys: Arc<NoiseKeys>,
        node_id: Uuid,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            sessions,
            peers,
            signing_key: Arc::new(AsyncMutex::new(signing_key)),
            noise_keys,
            node_id,
            health_monitor,
        }
    }

    pub fn noise_keys(&self) -> Arc<NoiseKeys> {
        self.noise_keys.clone()
    }

    pub async fn register_peer_handle(&self, id: Uuid, handle: server::Client) {
        let entry = self.ensure_entry(id).await;
        let mut state = entry.lock().await;
        state.replace_server(handle);
    }

    pub async fn attach_handle_only(&self, id: Uuid, handle: server::Client) {
        let entry = self.ensure_entry(id).await;
        let mut state = entry.lock().await;
        state.replace_server(handle);
    }

    pub async fn remove_peer(&self, id: Uuid) {
        self.cache.write().await.remove(&id);
    }

    pub async fn clear(&self) {
        self.cache.write().await.clear();
    }

    /// Clears any cached capabilities for `peer_id`, forcing a full refresh on next access.
    pub async fn invalidate_peer_capabilities(&self, peer_id: Uuid) {
        if let Some(entry) = self.entry_if_present(peer_id).await {
            self.invalidate_peer(peer_id, &entry).await;
        }
    }

    pub async fn server_handle_for(&self, peer_id: Uuid) -> Option<server::Client> {
        let entry = {
            let guard = self.cache.read().await;
            guard.get(&peer_id).cloned()
        }?;

        let state = entry.lock().await;
        state.server.clone()
    }

    pub async fn refresh_peer_handle(&self, peer_id: Uuid) -> Option<server::Client> {
        let peer = self.peer_latest_value(peer_id)?;
        let addr = peer.address.clone();

        match self.connect_to_peer(&addr, &peer.noise_static_pub).await {
            Ok(client) => {
                let entry = self.ensure_entry(peer_id).await;
                let mut state = entry.lock().await;
                state.replace_server(client.clone());
                Some(client)
            }
            Err(e) => {
                error!(target: "connect", "reconnect {addr} failed: {e}");
                None
            }
        }
    }

    pub fn known_peers(&self) -> AnyResult<Vec<Uuid>> {
        let (actives, _) = self
            .peers
            .load_all()
            .map_err(|e| anyhow!("failed to load peer store: {e}"))?;

        let mut ids = Vec::new();
        for (key, snapshot) in actives {
            let peer_id = key.to_uuid();
            if peer_id == self.node_id {
                continue;
            }

            if snapshot.as_slice().last().is_some() {
                ids.push(peer_id);
            }
        }

        Ok(ids)
    }

    /// Returns the last recorded hostname for the provided `peer_id`, if available.
    pub fn peer_hostname(&self, peer_id: Uuid) -> Option<String> {
        self.peer_latest_value(peer_id)
            .map(|value| value.hostname.clone())
    }

    pub fn peer_address(&self, peer_id: Uuid) -> Option<String> {
        self.peer_latest_value(peer_id)
            .map(|value| value.address.clone())
    }

    /// Returns the last recorded WireGuard underlay configuration for the provided `peer_id`, if
    /// available.
    pub fn peer_wireguard(&self, peer_id: Uuid) -> Option<WireGuardPeerValue> {
        self.peer_latest_value(peer_id)
            .and_then(|value| value.wireguard)
    }

    /// Returns a shared handle to the cluster health monitor.
    pub fn health_monitor(&self) -> Arc<HealthMonitor> {
        self.health_monitor.clone()
    }

    /// Returns a best-effort snapshot of the latest `PeerValue` for every active peer.
    ///
    /// This is used by subsystems (like networking) that need to reconcile state based on peer
    /// metadata without repeatedly scanning the store for each individual peer.
    pub fn peer_values_snapshot(&self) -> AnyResult<Vec<(Uuid, PeerValue)>> {
        let (actives, _) = self
            .peers
            .load_all()
            .map_err(|e| anyhow!("failed to load peer store: {e}"))?;

        let mut out = Vec::with_capacity(actives.len());
        for (key, snapshot) in actives {
            if let Some(value) = Self::select_peer_value(snapshot.as_slice()) {
                out.push((key.to_uuid(), value));
            }
        }
        Ok(out)
    }

    /// Updates the local node's advertised WireGuard state in the peers store.
    ///
    /// This allows the data plane (network controller) to mark WireGuard as ready once the kernel
    /// interface has been provisioned, enabling other nodes to safely switch the VXLAN underlay
    /// to the encrypted tunnel.
    pub async fn upsert_self_wireguard(&self, wireguard: WireGuardPeerValue) -> AnyResult<()> {
        let Some(mut current) = self.peer_latest_value(self.node_id) else {
            return Err(anyhow!("self peer value not yet available"));
        };

        current.wireguard = Some(wireguard);
        self.peers
            .upsert(&UuidKey::from(self.node_id), current)
            .await
            .map_err(|e| anyhow!("failed to upsert self peer wireguard state: {e}"))?;
        Ok(())
    }

    pub async fn session_for_peer(&self, peer_id: Uuid) -> Option<cluster_session::Client> {
        let entry = self.ensure_entry(peer_id).await;
        self.ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
            .await
    }

    pub async fn scheduler_session_via_handle(
        &self,
        client: &server::Client,
        peer_id: Uuid,
    ) -> Option<cluster_session::Client> {
        if let Some(entry) = self.entry_if_present(peer_id).await {
            if let Some(session) = self.cached_session(&entry).await {
                return Some(session);
            }
        }

        let session = self
            .session_for_strategy(client, peer_id, SessionStrategy::TicketThenCredential)
            .await?;

        self.store_session(peer_id, session.clone()).await;
        Some(session)
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

            if self.server_handle_for(peer_id).await.is_some() {
                continue;
            }

            let Some(val) = snap.as_slice().last().cloned() else {
                continue;
            };
            let addr = val.address.clone();

            let client = match self.connect_to_peer(&addr, &val.noise_static_pub).await {
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

            self.register_peer_handle(peer_id, client.clone()).await;
            self.store_session(peer_id, session.clone()).await;

            let _ = session.ping_request().send().promise.await.map(|_| {
                self.health_monitor.observe_seen(peer_id);
            });
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub async fn resume_sessions_on_boot(&self, local_addr: &str) {
        println!("Resuming sessions with peers...");

        let mut addr_map = HashMap::<Uuid, (String, [u8; 32])>::new();
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
                    addr_map.insert(id, (val.address, val.noise_static_pub));
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
            let Some((addr, static_pub)) = addr_map.get(&peer_id) else {
                eprintln!("resume: peer {peer_id} has no known address; skipping");
                continue;
            };

            match self.connect_to_peer(addr, static_pub).await {
                Ok(client) => {
                    let mut req = client.get_session_request();
                    req.get().set_ticket(&ticket);
                    match req.send().promise.await {
                        Ok(resp) => match resp.get().and_then(|r| r.get_session()) {
                            Ok(session) => {
                                self.attach_handle_only(peer_id, client.clone()).await;
                                self.store_session(peer_id, session.clone()).await;
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
        let entry = self.ensure_entry(peer_id).await;

        if let Some(sync_cap) = {
            let state = entry.lock().await;
            state.sync.clone()
        } {
            return Ok(Some(sync_cap));
        }

        let Some(session) = self
            .ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
            .await
        else {
            return Ok(None);
        };

        match Self::fetch_sync_from_session(&session).await {
            Ok(sync_cap) => {
                let mut state = entry.lock().await;
                state.sync = Some(sync_cap.clone());
                Ok(Some(sync_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;

                let Some(session) = self
                    .ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
                    .await
                else {
                    return Err(err);
                };

                let sync_cap = Self::fetch_sync_from_session(&session).await?;
                let mut state = entry.lock().await;
                state.sync = Some(sync_cap.clone());
                Ok(Some(sync_cap))
            }
        }
    }

    pub async fn fetch_health_capability(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<health::health::Client>, capnp::Error> {
        let entry = self.ensure_entry(peer_id).await;

        if let Some(health_cap) = {
            let state = entry.lock().await;
            state.health.clone()
        } {
            return Ok(Some(health_cap));
        }

        let Some(session) = self
            .ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
            .await
        else {
            return Ok(None);
        };

        match Self::fetch_health_from_session(&session).await {
            Ok(health_cap) => {
                let mut state = entry.lock().await;
                state.health = Some(health_cap.clone());
                Ok(Some(health_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;

                let Some(session) = self
                    .ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
                    .await
                else {
                    return Err(err);
                };

                let health_cap = Self::fetch_health_from_session(&session).await?;
                let mut state = entry.lock().await;
                state.health = Some(health_cap.clone());
                Ok(Some(health_cap))
            }
        }
    }

    pub async fn gossip_client_for(
        &self,
        peer_id: Uuid,
    ) -> Result<Option<GossipClient>, capnp::Error> {
        let entry = self.ensure_entry(peer_id).await;

        if let Some(gossip_cap) = {
            let state = entry.lock().await;
            state.gossip.clone()
        } {
            return Ok(Some(gossip_cap));
        }

        let Some(session) = self
            .ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
            .await
        else {
            return Ok(None);
        };

        match Self::fetch_gossip_from_session(&session).await {
            Ok(gossip_cap) => {
                let mut state = entry.lock().await;
                state.gossip = Some(gossip_cap.clone());
                Ok(Some(gossip_cap))
            }
            Err(err) => {
                self.invalidate_peer(peer_id, &entry).await;

                let Some(session) = self
                    .ensure_session(peer_id, &entry, SessionStrategy::TicketThenCredential)
                    .await
                else {
                    return Err(err);
                };

                let gossip_cap = Self::fetch_gossip_from_session(&session).await?;
                let mut state = entry.lock().await;
                state.gossip = Some(gossip_cap.clone());
                Ok(Some(gossip_cap))
            }
        }
    }

    /// Returns the cached capability entry for `peer_id` if one already exists.
    async fn entry_if_present(&self, peer_id: Uuid) -> Option<PeerEntry> {
        let guard = self.cache.read().await;
        guard.get(&peer_id).cloned()
    }

    /// Ensures a capability entry exists for `peer_id`, creating one if necessary.
    #[allow(clippy::arc_with_non_send_sync)]
    async fn ensure_entry(&self, peer_id: Uuid) -> PeerEntry {
        let mut guard = self.cache.write().await;
        guard
            .entry(peer_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(PeerState::default())))
            .clone()
    }

    /// Returns the cached ClusterSession for the given peer entry, if present.
    async fn cached_session(&self, entry: &PeerEntry) -> Option<cluster_session::Client> {
        let state = entry.lock().await;
        state.session.clone()
    }

    /// Stores a freshly obtained ClusterSession for `peer_id` and clears derived capability caches.
    async fn store_session(&self, peer_id: Uuid, session: cluster_session::Client) {
        let entry = self.ensure_entry(peer_id).await;
        let mut state = entry.lock().await;
        state.replace_session(session);
    }

    /// Guarantees a ClusterSession for `peer_id`, reconnecting as needed with the supplied strategy.
    async fn ensure_session(
        &self,
        peer_id: Uuid,
        entry: &PeerEntry,
        strategy: SessionStrategy,
    ) -> Option<cluster_session::Client> {
        if let Some(session) = self.cached_session(entry).await {
            return Some(session);
        }

        if let Some(server) = {
            let state = entry.lock().await;
            state.server.clone()
        } {
            if let Some(session) = self.session_for_strategy(&server, peer_id, strategy).await {
                let mut state = entry.lock().await;
                state.replace_session(session.clone());
                return Some(session);
            }
        }

        let refreshed = self.refresh_peer_handle(peer_id).await?;
        let session = self
            .session_for_strategy(&refreshed, peer_id, strategy)
            .await?;

        let mut state = entry.lock().await;
        state.replace_session(session.clone());
        Some(session)
    }

    /// Clears the cached capability tree for the peer so the next call rebuilds it from scratch.
    async fn invalidate_peer(&self, _peer_id: Uuid, entry: &PeerEntry) {
        let mut state = entry.lock().await;
        state.clear_all();
    }

    /// Fetches the Sync capability from an existing session.
    async fn fetch_sync_from_session(
        session: &cluster_session::Client,
    ) -> Result<sync::Client, capnp::Error> {
        let req = session.get_sync_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_sync()
    }

    /// Fetches the Health capability by expanding the session capabilities set.
    async fn fetch_health_from_session(
        session: &cluster_session::Client,
    ) -> Result<health::health::Client, capnp::Error> {
        let req = session.get_capabilities_request();
        let resp = req.send().promise.await?;
        let caps = resp.get()?.get_caps()?;
        caps.get_health()
    }

    /// Fetches the Gossip capability from the cached session.
    async fn fetch_gossip_from_session(
        session: &cluster_session::Client,
    ) -> Result<GossipClient, capnp::Error> {
        let req = session.get_gossip_request();
        let resp = req.send().promise.await?;
        resp.get()?.get_gossip()
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
                    if let Ok(v) = PeerValue::from_node_info(peer_id, ni) {
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

    /// Select the "best" peer value from an MVReg snapshot.
    ///
    /// Peers are stored as a multi-value register to tolerate concurrent writes during cluster
    /// joins/sync. For the networking stack we want a single, stable view of a peer that prefers
    /// values with more complete metadata (e.g. WireGuard configuration and enabled state) instead
    /// of relying on the arbitrary ordering of concurrent register entries.
    fn select_peer_value(values: &[PeerValue]) -> Option<PeerValue> {
        fn is_nonzero_key(key: &[u8; 32]) -> bool {
            key.iter().any(|b| *b != 0)
        }

        fn rank_wireguard(wg: &WireGuardPeerValue) -> (bool, bool, bool, u16, [u8; 32]) {
            (
                wg.enabled,
                is_nonzero_key(&wg.public_key),
                wg.port != 0,
                wg.port,
                wg.public_key,
            )
        }

        if values.is_empty() {
            return None;
        }

        let mut address: Option<&str> = None;
        let mut hostname: Option<&str> = None;
        let mut noise_static_pub: Option<[u8; 32]> = None;
        let mut signing_pub: Option<[u8; 32]> = None;
        let mut identity_sig: Option<Vec<u8>> = None;
        let mut wireguard: Option<WireGuardPeerValue> = None;

        for value in values {
            if !value.address.is_empty() {
                address = match address {
                    None => Some(value.address.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.address.as_str())),
                };
            }

            if !value.hostname.is_empty() {
                hostname = match hostname {
                    None => Some(value.hostname.as_str()),
                    Some(current) => Some(std::cmp::max(current, value.hostname.as_str())),
                };
            }

            noise_static_pub = match noise_static_pub {
                None => Some(value.noise_static_pub),
                Some(current) => Some(std::cmp::max(current, value.noise_static_pub)),
            };

            signing_pub = match signing_pub {
                None => Some(value.signing_pub),
                Some(current) => Some(std::cmp::max(current, value.signing_pub)),
            };

            if value.identity_sig.len() == 64 {
                identity_sig = match identity_sig {
                    None => Some(value.identity_sig.clone()),
                    Some(current) => Some(std::cmp::max(current, value.identity_sig.clone())),
                };
            }

            if let Some(candidate) = value.wireguard.as_ref() {
                wireguard = match wireguard.as_ref() {
                    None => Some(candidate.clone()),
                    Some(current) => {
                        if rank_wireguard(candidate) > rank_wireguard(current) {
                            Some(candidate.clone())
                        } else {
                            Some(current.clone())
                        }
                    }
                };
            }
        }

        Some(PeerValue {
            address: address.unwrap_or_default().to_string(),
            hostname: hostname.unwrap_or_default().to_string(),
            noise_static_pub: noise_static_pub.unwrap_or_default(),
            signing_pub: signing_pub.unwrap_or_default(),
            identity_sig: identity_sig.unwrap_or_default(),
            wireguard,
        })
    }

    fn peer_latest_value(&self, peer_id: Uuid) -> Option<PeerValue> {
        let (actives, _) = self.peers.load_all().ok()?;
        actives
            .into_iter()
            .find(|(k, _)| k.to_uuid() == peer_id)
            .and_then(|(_, snap)| Self::select_peer_value(snap.as_slice()))
    }

    /// Dial a peer over authenticated Noise using the current join token.
    /// This enforces cluster membership for all inter-node RPC traffic.
    async fn connect_to_peer(
        &self,
        addr: &str,
        peer_static: &[u8; 32],
    ) -> Result<server::Client, String> {
        client::connection::get_client_secure_peer_with_keys(
            addr,
            peer_static,
            self.noise_keys.as_ref(),
        )
            .await
            .map_err(|e| e.to_string())
    }
}
