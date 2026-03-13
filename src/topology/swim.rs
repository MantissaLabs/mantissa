use super::{Topology, TopologyEvent};
use crate::cluster::ClusterViewId;
use health::{Action as HealthAction, SwimEvent as HealthSwimEvent};
use std::time::Duration;
use tracing::debug;
use uuid::Uuid;

impl Topology {
    /// # Description:
    ///
    /// Returns the local SWIM incarnation used when refuting stale suspect/down rumors.
    pub fn swim_local_incarnation(&self) -> u64 {
        self.health_monitor.local_incarnation()
    }

    /// # Description:
    ///
    /// Records that a peer joined the membership and seeds detector state as alive.
    pub fn swim_record_join(&self, id: Uuid, incarnation: u64) {
        self.health_monitor.record_join(id, incarnation);
    }

    /// # Description:
    ///
    /// Applies an `alive` SWIM update and refreshes local detector state for the subject peer.
    pub(super) async fn handle_alive_event(&self, id: Uuid, incarnation: u64) {
        self.apply_health_actions(self.health_monitor.handle_alive_event(id, incarnation))
            .await;
    }

    /// # Description:
    ///
    /// Applies a `suspect` SWIM update, or emits an immediate alive refutation when we are the target.
    pub(super) async fn handle_suspect_event(&self, id: Uuid, incarnation: u64) {
        self.apply_health_actions(self.health_monitor.handle_suspect_event(
            id,
            incarnation,
            self.runtime_health.down_after,
        ))
        .await;
    }

    /// # Description:
    ///
    /// Applies a `down` SWIM update, or emits an immediate alive refutation when we are the target.
    pub(super) async fn handle_down_event(&self, id: Uuid, incarnation: u64) {
        self.apply_health_actions(self.health_monitor.handle_down_event(id, incarnation))
            .await;
    }

    /// # Description:
    ///
    /// Executes one SWIM probe cycle:
    ///  - picks one target peer,
    ///  - performs direct ping and optional indirect probes,
    ///  - transitions local suspicion/down state, and
    ///  - gossips liveness transitions.
    pub async fn health_probe_tick(&self) {
        let candidates = self.swim_probe_candidates().await;
        if candidates.is_empty() {
            return;
        }

        let cluster_view = self.active_cluster_view();
        let timeout = self.runtime_health.probe_timeout;
        let Some(target_index) = self.health_monitor.next_probe_index(candidates.len()) else {
            return;
        };
        let target = candidates[target_index];

        let direct_ok = self
            .probe_peer_direct(target, cluster_view, timeout)
            .await
            .unwrap_or(false);
        let indirect_ok = if direct_ok {
            true
        } else {
            self.probe_peer_indirect(target, &candidates, cluster_view, timeout)
                .await
        };

        if indirect_ok {
            if let Some(event) = self.health_monitor.record_probe_success(target) {
                self.apply_health_actions(vec![HealthAction::Gossip(event)])
                    .await;
            }
        } else {
            self.apply_health_actions(self.health_monitor.record_probe_failure(
                target,
                self.runtime_health.suspect_after,
                self.runtime_health.down_after,
            ))
            .await;
        }

        self.apply_health_actions(self.health_monitor.expire_suspicions())
            .await;
    }

    /// # Description:
    ///
    /// Returns probe-eligible peers, excluding the local node and view-scoped excluded peers.
    async fn swim_probe_candidates(&self) -> Vec<Uuid> {
        let snapshot = match self.peer_snapshot().await {
            Some(snapshot) => snapshot,
            None => return Vec::new(),
        };
        let excluded = self.excluded_peers_snapshot().await;

        snapshot
            .entries
            .iter()
            .filter_map(|entry| {
                if entry.peer_id == self.node.id || excluded.contains(&entry.peer_id) {
                    None
                } else {
                    Some(entry.peer_id)
                }
            })
            .collect()
    }

    /// # Description:
    ///
    /// Performs a direct health ping to `peer_id` within `timeout`.
    async fn probe_peer_direct(
        &self,
        peer_id: Uuid,
        cluster_view: ClusterViewId,
        timeout: Duration,
    ) -> Result<bool, capnp::Error> {
        let Some(health_cap) = self
            .registry
            .fetch_health_capability(peer_id, cluster_view)
            .await?
        else {
            return Ok(false);
        };

        let ping = async {
            let req = health_cap.ping_request();
            req.send().promise.await
        };

        match tokio::time::timeout(timeout, ping).await {
            Ok(Ok(_)) => Ok(true),
            Ok(Err(err)) => {
                debug!(target: "health", peer = %peer_id, "direct ping failed: {err}");
                self.registry.invalidate_peer_capabilities(peer_id).await;
                Ok(false)
            }
            Err(_) => {
                debug!(target: "health", peer = %peer_id, "direct ping timed out");
                self.registry.invalidate_peer_capabilities(peer_id).await;
                Ok(false)
            }
        }
    }

    /// # Description:
    ///
    /// Executes one direct health probe for another node, used by remote indirect probe requests.
    pub async fn health_indirect_ping(&self, target_id: Uuid, timeout: Duration) -> bool {
        self.probe_peer_direct(target_id, self.active_cluster_view(), timeout)
            .await
            .unwrap_or(false)
    }

    /// # Description:
    ///
    /// Executes SWIM indirect probing by asking helper peers to ping the target on our behalf.
    async fn probe_peer_indirect(
        &self,
        target_id: Uuid,
        candidates: &[Uuid],
        cluster_view: ClusterViewId,
        timeout: Duration,
    ) -> bool {
        let helper_population = candidates
            .iter()
            .filter(|peer_id| **peer_id != target_id)
            .count();
        if helper_population == 0 {
            return false;
        }

        // Lifeguard-style scaling: grow helper fanout logarithmically with membership while
        // keeping an operator-provided floor via `health.probe_fanout` and configured bounds.
        let adaptive_floor = (helper_population.max(1)).ilog2() as usize + 1;
        let adaptive_floor = adaptive_floor.clamp(
            self.runtime_health.indirect_fanout_min,
            self.runtime_health.indirect_fanout_max,
        );
        let helper_budget = self
            .runtime_health
            .probe_fanout
            .max(adaptive_floor)
            .min(self.runtime_health.indirect_fanout_max)
            .min(helper_population);

        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
        let mut helpers = candidates
            .iter()
            .copied()
            .filter(|peer_id| *peer_id != target_id)
            .collect::<Vec<_>>();
        use ::rand::prelude::SliceRandom;
        let mut rng = ::rand::rng();
        helpers.shuffle(&mut rng);

        for helper_id in helpers.into_iter().take(helper_budget) {
            let helper_cap = match self
                .registry
                .fetch_health_capability(helper_id, cluster_view)
                .await
            {
                Ok(Some(cap)) => cap,
                Ok(None) => continue,
                Err(err) => {
                    debug!(target: "health", helper = %helper_id, "indirect helper unavailable: {err}");
                    continue;
                }
            };

            let probe = async {
                let mut req = helper_cap.indirect_ping_request();
                {
                    let mut payload = req.get();
                    payload.set_target_id(target_id.as_bytes());
                    payload.set_timeout_ms(timeout_ms);
                }
                let resp = req.send().promise.await?;
                let reader = resp.get()?;
                Ok::<bool, capnp::Error>(reader.get_ok())
            };

            match tokio::time::timeout(timeout, probe).await {
                Ok(Ok(true)) => return true,
                Ok(Ok(false)) => {}
                Ok(Err(err)) => {
                    debug!(target: "health", helper = %helper_id, "indirect ping failed: {err}");
                }
                Err(_) => {
                    debug!(target: "health", helper = %helper_id, "indirect ping timed out");
                }
            }
        }

        false
    }

    /// # Description:
    ///
    /// Applies the side effects requested by the shared detector, keeping topology transport-specific.
    async fn apply_health_actions(&self, actions: Vec<HealthAction>) {
        for action in actions {
            match action {
                HealthAction::Gossip(event) => {
                    let _ = self
                        .gossip_topology_event(Self::topology_event_from_health(event))
                        .await;
                }
                HealthAction::InvalidatePeer(peer_id) => {
                    self.registry.invalidate_peer_capabilities(peer_id).await;
                }
            }
        }
    }

    /// # Description:
    ///
    /// Translates detector gossip into topology events so transport encoding stays outside the crate.
    fn topology_event_from_health(event: HealthSwimEvent) -> TopologyEvent {
        match event {
            HealthSwimEvent::Alive { id, incarnation } => TopologyEvent::Alive { id, incarnation },
            HealthSwimEvent::Suspect { id, incarnation } => {
                TopologyEvent::Suspect { id, incarnation }
            }
            HealthSwimEvent::Down { id, incarnation } => TopologyEvent::Down { id, incarnation },
        }
    }
}
