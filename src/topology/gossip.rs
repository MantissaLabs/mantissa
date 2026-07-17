use super::*;

impl Topology {
    /// Queues one topology event onto the outbound gossip channel.
    pub async fn gossip_topology_event(&self, event: TopologyEvent) -> Result<(), capnp::Error> {
        let id = Uuid::new_v4();
        self.runtime
            .gossip
            .send(Message::Topology { id, event })
            .await
    }

    /// Verifies a peer identity signature and enforces signing-key pinning for existing ids.
    async fn verify_peer_identity_event(
        &self,
        peer_id: Uuid,
        noise_static_pub: &x25519_dalek::PublicKey,
        signing_pub: &VerifyingKey,
        identity_sig: &[u8],
    ) -> Result<(), String> {
        if identity_sig.is_empty() {
            return Err("identitySig is required for peer identity verification".to_string());
        }
        if identity_sig.len() != 64 {
            return Err("identitySig must be exactly 64 bytes".to_string());
        }

        crate::node::identity::verify_peer_identity(
            signing_pub,
            &peer_id,
            &noise_static_pub.to_bytes(),
            identity_sig,
        )
        .map_err(|e| e.to_string())?;

        if let Some(snapshot) = self.peer_snapshot().await
            && let Some(entry) = snapshot
                .entries
                .iter()
                .find(|entry| entry.peer_id == peer_id)
            && entry.value.signing_pub != signing_pub.to_bytes()
        {
            return Err("peer signing key does not match existing record".to_string());
        }

        Ok(())
    }

    /// Processes inbound topology gossip and applies accepted events to local state.
    pub async fn run(&self) {
        loop {
            match self.runtime.gossip.recv().await {
                Ok(Message::Void { .. }) => {}
                Ok(Message::Job { .. }) => {}
                Ok(Message::Agent { .. }) => {}
                Ok(Message::Volume { .. }) => {}
                Ok(Message::SchedulerDigest { .. }) => {}
                Ok(Message::SecretMasterKey { .. }) => {}
                Ok(Message::Topology { id, event }) => {
                    match event {
                        TopologyEvent::Join {
                            id,
                            ref address,
                            ref hostname,
                            ref platform_os,
                            ref platform_arch,
                            root_hash: _,
                            incarnation,
                            ref client,
                            ref noise_static_pub,
                            ref signing_pub,
                            ref identity_sig,
                            ref wireguard,
                            ref scheduling,
                            ref readiness,
                            ref labels,
                            ref runtime_support,
                            root_schema,
                        } => {
                            info!(target: "topology", "Node joined: {id} at {address}");

                            if let Err(e) = self
                                .verify_peer_identity_event(
                                    id,
                                    noise_static_pub,
                                    signing_pub,
                                    identity_sig,
                                )
                                .await
                            {
                                warn!(target: "topology", "rejecting peer {id}: {e}");
                                continue;
                            }

                            let v = PeerValue {
                                address: address.clone(),
                                hostname: hostname.clone(),
                                platform_os: platform_os.clone(),
                                platform_arch: platform_arch.clone(),
                                noise_static_pub: noise_static_pub.to_bytes(),
                                signing_pub: signing_pub.to_bytes(),
                                identity_sig: identity_sig.clone(),
                                wireguard: wireguard.clone(),
                                scheduling: scheduling.as_ref().clone(),
                                readiness: readiness.as_ref().clone(),
                                labels: labels.as_ref().clone(),
                                runtime_support: runtime_support.as_ref().clone(),
                                root_schema,
                                membership: PeerMembership::active(incarnation),
                            };

                            if let Err(e) = self.register_peer(id, &v, client.clone()).await {
                                error!("Failed to register peer: {e}");
                                continue;
                            }
                            self.swim_record_join(id, incarnation);
                            if let Err(err) = self.publish_local_cluster_node_count().await {
                                warn!(
                                    target: "cluster_view",
                                    peer_id = %id,
                                    "failed to publish cluster node count after join event: {err}"
                                );
                            }
                            self.sync_once_now();
                        }

                        TopologyEvent::Leave { id, incarnation } => {
                            match self.mark_peer_left(id, incarnation).await {
                                Ok(true) => {
                                    info!(target: "topology", "Node left: {id}");
                                }
                                Ok(false) => {
                                    tracing::debug!(
                                        target: "topology",
                                        peer_id = %id,
                                        incarnation,
                                        "ignored duplicate or stale leave event"
                                    );
                                }
                                Err(e) => {
                                    error!("Failed to remove peer: {e}");
                                    continue;
                                }
                            }
                        }

                        TopologyEvent::Alive { id, incarnation } => {
                            self.handle_alive_event(id, incarnation).await;
                        }

                        TopologyEvent::Suspect { id, incarnation } => {
                            self.handle_suspect_event(id, incarnation).await;
                        }

                        TopologyEvent::Down { id, incarnation } => {
                            self.handle_down_event(id, incarnation).await;
                        }

                        TopologyEvent::ClusterNameUpdated {
                            cluster_id,
                            ref name,
                            updated_at_unix_ms,
                            actor_node_id,
                        } => {
                            let trimmed = name.trim();
                            if trimmed.is_empty() {
                                warn!(
                                    target: "cluster_view",
                                    cluster_id = %cluster_id,
                                    actor_node_id = %actor_node_id,
                                    "ignoring empty cluster name gossip update"
                                );
                                continue;
                            }

                            let record = ClusterNameRecord {
                                name: trimmed.to_string(),
                                updated_at_unix_ms,
                                actor_node_id,
                            };
                            if let Err(err) =
                                self.upsert_cluster_name_record(cluster_id, &record).await
                            {
                                warn!(
                                    target: "cluster_view",
                                    cluster_id = %cluster_id,
                                    actor_node_id = %actor_node_id,
                                    "failed to apply gossiped cluster name update: {err}"
                                );
                                continue;
                            }
                        }
                        TopologyEvent::ClusterMetadataChanged {
                            operation_id,
                            source_node_id,
                        } => {
                            debug!(
                                target: "cluster_view",
                                %operation_id,
                                %source_node_id,
                                "received cluster-wide metadata availability hint"
                            );
                            self.sync_metadata_from_peer_now(source_node_id);
                        }
                        TopologyEvent::NodeSchedulingUpdated { id, ref scheduling } => {
                            if let Err(err) = self
                                .apply_peer_scheduling_update(id, scheduling.clone())
                                .await
                            {
                                warn!(
                                    target: "topology",
                                    node_id = %id,
                                    "failed to apply gossiped scheduling update: {err}"
                                );
                                continue;
                            }
                        }
                        TopologyEvent::NodeReadinessUpdated { id, ref readiness } => {
                            if let Err(err) = self
                                .apply_peer_readiness_update(id, readiness.clone())
                                .await
                            {
                                warn!(
                                    target: "topology",
                                    node_id = %id,
                                    "failed to apply gossiped readiness update: {err}"
                                );
                                continue;
                            }
                        }
                        TopologyEvent::NodeLabelsUpdated { id, ref labels } => {
                            if let Err(err) =
                                self.apply_peer_labels_update(id, labels.clone()).await
                            {
                                warn!(
                                    target: "topology",
                                    node_id = %id,
                                    "failed to apply gossiped label update: {err}"
                                );
                                continue;
                            }
                        }
                    }

                    let event_clone = match event.clone() {
                        TopologyEvent::Join {
                            id,
                            hostname,
                            address,
                            platform_os,
                            platform_arch,
                            root_hash,
                            incarnation,
                            client,
                            noise_static_pub,
                            signing_pub,
                            identity_sig,
                            wireguard,
                            scheduling,
                            readiness,
                            labels,
                            runtime_support,
                            root_schema,
                        } => {
                            let client = if id == self.local.node.id {
                                client
                            } else {
                                None
                            };
                            TopologyEvent::Join {
                                id,
                                hostname,
                                address,
                                platform_os,
                                platform_arch,
                                root_hash,
                                incarnation,
                                client,
                                noise_static_pub,
                                signing_pub,
                                identity_sig,
                                wireguard,
                                scheduling,
                                readiness,
                                labels,
                                runtime_support,
                                root_schema,
                            }
                        }
                        evt => evt,
                    };

                    if let Err(e) = self
                        .runtime
                        .gossip
                        .send(Message::Topology {
                            id,
                            event: event_clone,
                        })
                        .await
                    {
                        error!("Failed to forward gossip event: {e}");
                    }
                }
                Ok(Message::Workload { .. })
                | Ok(Message::Service { .. })
                | Ok(Message::Network { .. })
                | Ok(Message::Secret { .. }) => {}
                Err(async_channel::RecvError) => {
                    debug!("topology channel closed!");
                    break;
                }
            }
        }
    }
}
