use super::*;

impl Topology {
    /// Clears locally cached peer authentication material after this node leaves a cluster.
    pub(super) fn clear_local_cluster_auth_state(&self) {
        if let Err(err) = self.stores.local_sessions.clear() {
            warn!(
                target: "topology",
                "leave: failed to clear local session tickets: {err}"
            );
        }
        if let Err(err) = self.stores.local_credential_store.clear() {
            warn!(
                target: "topology",
                "leave: failed to clear local credentials: {err}"
            );
        }
    }

    /// Applies one scheduling-state update to the peer store using deterministic convergence.
    pub(super) async fn apply_peer_scheduling_update(
        &self,
        node_id: Uuid,
        scheduling: PeerSchedulingState,
    ) -> Result<bool, capnp::Error> {
        let Some(mut current) = self.deps.registry.peer_value_unscoped(node_id) else {
            return Err(capnp::Error::failed(format!(
                "node '{}' not found",
                node_id
            )));
        };

        let merged = PeerSchedulingState::merge(&current.scheduling, &scheduling);
        if current.scheduling == merged {
            return Ok(false);
        }

        current.scheduling = merged;
        self.stores
            .peers
            .upsert(&UuidKey::from(node_id), current)
            .await
            .map_err(|err| {
                capnp::Error::failed(format!(
                    "failed to persist scheduling update for node '{}': {err}",
                    node_id
                ))
            })?;
        Ok(true)
    }

    /// Applies one node-label update to the peer store using deterministic convergence.
    pub(super) async fn apply_peer_labels_update(
        &self,
        node_id: Uuid,
        labels: crate::topology::peers::PeerLabelState,
    ) -> Result<bool, capnp::Error> {
        let Some(mut current) = self.deps.registry.peer_value_unscoped(node_id) else {
            return Err(capnp::Error::failed(format!(
                "node '{}' not found",
                node_id
            )));
        };

        let merged = crate::topology::peers::PeerLabelState::merge(&current.labels, &labels);
        if current.labels == merged {
            return Ok(false);
        }

        current.labels = merged;
        self.stores
            .peers
            .upsert(&UuidKey::from(node_id), current)
            .await
            .map_err(|err| {
                capnp::Error::failed(format!(
                    "failed to persist label update for node '{}': {err}",
                    node_id
                ))
            })?;
        Ok(true)
    }

    /// Restores the peers MST from durable storage after process startup.
    #[allow(dead_code)]
    pub async fn restore_peers(&self) -> std::io::Result<()> {
        self.stores
            .peers
            .rebuild_mst_from_disk()
            .await
            .map_err(Into::into)
    }

    /// Persists one peer row and registers or invalidates its corresponding transport handle.
    pub async fn register_peer(
        &self,
        id: Uuid,
        val: &PeerValue,
        handle: Option<server::Client>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.stores
            .peers
            .upsert(&UuidKey::from(id), val.clone())
            .await?;
        match handle {
            Some(handle) => {
                self.deps.registry.register_peer_handle(id, handle).await;
            }
            None => {
                // If the gossip message did not carry a usable handle, clear any stale capability
                // cache so later connection attempts fall back to dialing the advertised address.
                self.deps.registry.invalidate_peer_capabilities(id).await;
            }
        }
        Ok(())
    }

    /// Returns the converged membership state for one peer without active-member filtering.
    pub(super) fn peer_membership_unscoped(
        &self,
        id: Uuid,
    ) -> Result<Option<PeerMembership>, capnp::Error> {
        let reg = self
            .stores
            .peers
            .get_reg(&UuidKey::from(id))
            .map_err(|err| {
                capnp::Error::failed(format!("failed to load peer membership for '{id}': {err}"))
            })?;
        Ok(reg
            .as_ref()
            .and_then(PeerValue::select_reg)
            .map(|value| value.membership))
    }

    /// Marks one peer as left without tombstoning the reusable identity row.
    pub async fn mark_peer_left(
        &self,
        id: Uuid,
        incarnation: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current = self.deps.registry.peer_value_unscoped(id);
        let stale_against_current = current
            .as_ref()
            .map(|value| {
                value.membership.incarnation > incarnation
                    || (value.membership.incarnation == incarnation && value.membership.is_active())
            })
            .unwrap_or(false);
        if stale_against_current {
            return Ok(());
        }

        let mut value = current.unwrap_or_else(|| PeerValue {
            address: String::new(),
            hostname: String::new(),
            platform_os: String::new(),
            platform_arch: String::new(),
            noise_static_pub: [0u8; 32],
            signing_pub: [0u8; 32],
            identity_sig: Vec::new(),
            wireguard: None,
            scheduling: PeerSchedulingState::schedulable_default(id),
            labels: crate::topology::peers::PeerLabelState::default(),
            runtime_support: RuntimeSupportProfile::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: PeerMembership::left(incarnation),
        });
        value.membership = PeerMembership::left(incarnation);
        self.stores.peers.upsert(&UuidKey::from(id), value).await?;
        if let Err(err) = self.stores.session_auth.revoke_by_peer(id) {
            warn!(
                target: "topology",
                peer_id = %id,
                "failed to revoke server session ticket for left peer: {err}"
            );
        }
        if let Err(err) = self.stores.local_sessions.remove(id) {
            warn!(
                target: "topology",
                peer_id = %id,
                "failed to remove local session ticket for left peer: {err}"
            );
        }
        if let Err(err) = self.stores.local_credential_store.remove(id) {
            warn!(
                target: "topology",
                peer_id = %id,
                "failed to remove local credential for left peer: {err}"
            );
        }
        self.deps.registry.remove_peer(id).await;
        self.deps.health_monitor.remove_peer(id);
        if id != self.local.node.id {
            match self.publish_local_cluster_node_count().await {
                Ok(true) => self.sync_once_now(),
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        target: "cluster_view",
                        peer_id = %id,
                        "failed to publish cluster node count after leave event: {err}"
                    );
                }
            }
        }
        Ok(())
    }

    /// Return true if the peer `id` currently exists as an active member.
    pub fn peer_exists(&self, id: Uuid) -> io::Result<bool> {
        let reg = self
            .stores
            .peers
            .get_reg(&UuidKey::from(id))
            .map_err(io::Error::other)?;
        Ok(reg
            .as_ref()
            .and_then(PeerValue::select_reg)
            .map(|value| value.is_active())
            .unwrap_or(false))
    }

    /// Removes one peer row and clears all runtime state associated with that peer.
    pub async fn remove_peer(&self, id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(e) = self.stores.peers.remove(&UuidKey::from(id)).await {
            eprintln!("Could not remove peer: {e}");
        }
        self.deps.registry.remove_peer(id).await;
        self.deps.health_monitor.remove_peer(id);
        Ok(())
    }

    /// Only attach a server handle (no upsert). Useful on session resume.
    pub async fn attach_handle_only(&self, id: Uuid, handle: server::Client) {
        self.deps.registry.attach_handle_only(id, handle).await;
    }

    /// Best-effort resume of sessions stored locally (tickets) after restart.
    /// For each stored (peer, ticket):
    ///  - look up the peer's current address from the persisted peers store,
    ///  - connect securely to the peer's Server,
    ///  - call getSession(ticket) to obtain a ClusterSession,
    ///  - attach the server handle so higher-level code can use it.
    #[allow(dead_code)]
    pub async fn resume_sessions_on_boot(&self) {
        self.deps
            .registry
            .resume_sessions_on_boot(self.local.advertise.configured())
            .await;
    }

    /// Connect to known peers and open a ClusterSession with each.
    /// - Try local ticket via `getSession`.
    /// - If no ticket (or it fails) and `signing_key` is provided,
    ///   mint a short-lived ClusterCredential and call `getWithCredential`.
    /// - On success, register the refreshed `Server` handle via the capability
    ///   registry and persist any new ticket returned.
    pub async fn connect_known_peers(
        &self,
        signing_key: Option<&SigningKey>,
    ) -> Result<(), capnp::Error> {
        let allow_credentials = signing_key.is_some();
        self.deps
            .registry
            .connect_known_peers(allow_credentials)
            .await
    }

    /// Return the stored ed25519 verifying key for `peer_id` if we have it locally.
    /// This is used to verify self-signed short-lived credentials in getWithCredential.
    pub fn signing_vk_for(&self, peer_id: Uuid) -> Option<VerifyingKey> {
        let (actives, _tombs) = self.stores.peers.load_all_regs().ok()?;

        let reg = actives.into_iter().find(|(k, _)| k.to_uuid() == peer_id)?.1;
        let last = PeerValue::select_reg(&reg).filter(|value| value.is_active())?;

        let arr: [u8; 32] = last.signing_pub.as_slice().try_into().ok()?;
        VerifyingKey::from_bytes(&arr).ok()
    }
}
