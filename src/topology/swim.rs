use super::{Topology, TopologyEvent, lock_or_recover};
use crate::cluster::ClusterViewId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;
use tracing::debug;
use uuid::Uuid;

#[derive(Clone, Copy, Debug)]
pub(super) struct SwimPeerState {
    /// Highest incarnation observed for this peer.
    pub(super) incarnation: u64,
    /// Locally selected health status for this peer.
    pub(super) status: ::health::Status,
    /// Timestamp of the first consecutive probe failure for suspicion gating.
    pub(super) first_failed_at: Option<Instant>,
    /// Deadline at which suspect should transition to down if no refutation arrives.
    pub(super) suspect_deadline: Option<Instant>,
}

impl Default for SwimPeerState {
    /// # Description:
    ///
    /// Creates the baseline SWIM state for a peer before any liveness signal is observed.
    fn default() -> Self {
        Self {
            incarnation: 0,
            status: ::health::Status::Unknown,
            first_failed_at: None,
            suspect_deadline: None,
        }
    }
}

#[derive(Clone)]
pub(super) struct SwimState {
    /// Per-peer SWIM liveness state keyed by node identifier.
    pub(super) peers: Arc<AsyncMutex<HashMap<Uuid, SwimPeerState>>>,
    /// Round-robin cursor used to select one probe target per tick.
    pub(super) probe_cursor: Arc<Mutex<usize>>,
    /// Local node incarnation used to refute remote suspect/down rumors.
    pub(super) local_incarnation: Arc<AtomicU64>,
}

impl SwimState {
    /// # Description:
    ///
    /// Creates SWIM runtime state and seeds the local incarnation counter.
    pub(super) fn new(local_id: Uuid) -> Self {
        let bytes = local_id.as_u128() as usize;
        let boot_incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(1)
            .max(1);
        Self {
            peers: Arc::new(AsyncMutex::new(HashMap::new())),
            probe_cursor: Arc::new(Mutex::new(bytes)),
            local_incarnation: Arc::new(AtomicU64::new(boot_incarnation)),
        }
    }
}

/// # Description:
///
/// Computes an ordering rank for SWIM statuses when incarnation numbers are equal.
fn swim_status_rank(status: ::health::Status) -> u8 {
    match status {
        ::health::Status::Unknown => 0,
        ::health::Status::Alive => 1,
        ::health::Status::Degraded => 1,
        ::health::Status::Suspect => 2,
        ::health::Status::Down => 3,
    }
}

impl Topology {
    /// # Description:
    ///
    /// Returns the local SWIM incarnation used when refuting stale suspect/down rumors.
    pub fn swim_local_incarnation(&self) -> u64 {
        self.swim.local_incarnation.load(Ordering::SeqCst)
    }

    /// # Description:
    ///
    /// Records that a peer joined the membership and seeds SWIM state as alive.
    pub async fn swim_record_join(&self, id: Uuid, incarnation: u64) {
        let mut states = self.swim.peers.lock().await;
        let state = states.entry(id).or_default();
        state.incarnation = state.incarnation.max(incarnation);
        state.status = ::health::Status::Alive;
        state.first_failed_at = None;
        state.suspect_deadline = None;
        drop(states);
        self.health_monitor.set_status(id, ::health::Status::Alive);
    }

    /// # Description:
    ///
    /// Applies an `alive` SWIM update and refreshes local status/metadata for the subject peer.
    pub(super) async fn handle_alive_event(&self, id: Uuid, incarnation: u64) {
        if id == self.node.id {
            if incarnation > self.swim_local_incarnation() {
                self.swim
                    .local_incarnation
                    .store(incarnation, Ordering::SeqCst);
            }
            self.swim_record_join(id, self.swim_local_incarnation())
                .await;
            return;
        }
        self.apply_remote_swim_update(id, incarnation, ::health::Status::Alive)
            .await;
    }

    /// # Description:
    ///
    /// Applies a `suspect` SWIM update, or emits an immediate alive refutation when we are the target.
    pub(super) async fn handle_suspect_event(&self, id: Uuid, incarnation: u64) {
        if id == self.node.id {
            if let Some(next) = self.refute_self_suspicion(incarnation).await {
                let _ = self
                    .gossip_topology_event(TopologyEvent::Alive {
                        id: self.node.id,
                        incarnation: next,
                    })
                    .await;
            }
            return;
        }
        self.apply_remote_swim_update(id, incarnation, ::health::Status::Suspect)
            .await;
    }

    /// # Description:
    ///
    /// Applies a `down` SWIM update, or emits an immediate alive refutation when we are the target.
    pub(super) async fn handle_down_event(&self, id: Uuid, incarnation: u64) {
        if id == self.node.id {
            if let Some(next) = self.refute_self_suspicion(incarnation).await {
                let _ = self
                    .gossip_topology_event(TopologyEvent::Alive {
                        id: self.node.id,
                        incarnation: next,
                    })
                    .await;
            }
            return;
        }
        self.apply_remote_swim_update(id, incarnation, ::health::Status::Down)
            .await;
    }

    /// # Description:
    ///
    /// Applies one remote SWIM status update with incarnation ordering and same-incarnation precedence.
    async fn apply_remote_swim_update(&self, id: Uuid, incarnation: u64, status: ::health::Status) {
        let now = Instant::now();
        let mut states = self.swim.peers.lock().await;
        let state = states.entry(id).or_default();
        if incarnation < state.incarnation {
            return;
        }

        let should_apply = if incarnation > state.incarnation {
            true
        } else {
            swim_status_rank(status) > swim_status_rank(state.status)
        };

        if !should_apply {
            return;
        }

        state.incarnation = incarnation;
        state.status = status;
        state.first_failed_at = None;
        state.suspect_deadline = if matches!(status, ::health::Status::Suspect) {
            Some(now + self.runtime_health.down_after)
        } else {
            None
        };
        drop(states);

        self.health_monitor.set_status(id, status);
        if matches!(status, ::health::Status::Down) {
            self.registry.invalidate_peer_capabilities(id).await;
        }
    }

    /// # Description:
    ///
    /// Increments local incarnation when a remote suspect/down rumor targets this node.
    async fn refute_self_suspicion(&self, observed_incarnation: u64) -> Option<u64> {
        let current = self.swim_local_incarnation();
        if observed_incarnation < current {
            return None;
        }
        let next = observed_incarnation.saturating_add(1);
        self.swim.local_incarnation.store(next, Ordering::SeqCst);
        self.swim_record_join(self.node.id, next).await;
        Some(next)
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
        let target = {
            let mut cursor = lock_or_recover(&self.swim.probe_cursor, "topology.swim_probe_cursor");
            let idx = *cursor % candidates.len();
            *cursor = (*cursor + 1) % candidates.len();
            candidates[idx]
        };

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
            if let Some(incarnation) = self.swim_record_probe_success(target).await {
                let _ = self
                    .gossip_topology_event(TopologyEvent::Alive {
                        id: target,
                        incarnation,
                    })
                    .await;
            }
        } else if let Some(event) = self.swim_record_probe_failure(target).await {
            let _ = self.gossip_topology_event(event).await;
        }

        for event in self.swim_expire_suspicions().await {
            let _ = self.gossip_topology_event(event).await;
        }
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
    /// Clears local failure counters after a successful probe and returns incarnation when
    /// an alive transition should be gossiped.
    async fn swim_record_probe_success(&self, peer_id: Uuid) -> Option<u64> {
        let mut states = self.swim.peers.lock().await;
        let state = states.entry(peer_id).or_default();
        let previous = state.status;
        if state.incarnation == 0 {
            state.incarnation = 1;
        }
        state.status = ::health::Status::Alive;
        state.first_failed_at = None;
        state.suspect_deadline = None;
        let incarnation = state.incarnation;
        drop(states);

        self.health_monitor
            .set_status(peer_id, ::health::Status::Alive);
        if previous != ::health::Status::Alive {
            Some(incarnation)
        } else {
            None
        }
    }

    /// # Description:
    ///
    /// Records one failed probe and emits suspect/down gossip events when thresholds are crossed.
    async fn swim_record_probe_failure(&self, peer_id: Uuid) -> Option<TopologyEvent> {
        let now = Instant::now();
        let mut states = self.swim.peers.lock().await;
        let state = states.entry(peer_id).or_default();
        if state.incarnation == 0 {
            state.incarnation = 1;
        }
        if state.status == ::health::Status::Down {
            return None;
        }

        if state.first_failed_at.is_none() {
            state.first_failed_at = Some(now);
            return None;
        }

        if now.duration_since(state.first_failed_at.unwrap_or(now))
            < self.runtime_health.suspect_after
        {
            return None;
        }

        if state.status != ::health::Status::Suspect {
            state.status = ::health::Status::Suspect;
            state.suspect_deadline = Some(now + self.runtime_health.down_after);
            let incarnation = state.incarnation;
            drop(states);
            self.health_monitor
                .set_status(peer_id, ::health::Status::Suspect);
            return Some(TopologyEvent::Suspect {
                id: peer_id,
                incarnation,
            });
        }

        if state
            .suspect_deadline
            .map(|deadline| now >= deadline)
            .unwrap_or(false)
        {
            state.status = ::health::Status::Down;
            state.first_failed_at = None;
            state.suspect_deadline = None;
            let incarnation = state.incarnation;
            drop(states);
            self.health_monitor
                .set_status(peer_id, ::health::Status::Down);
            self.registry.invalidate_peer_capabilities(peer_id).await;
            return Some(TopologyEvent::Down {
                id: peer_id,
                incarnation,
            });
        }

        None
    }

    /// # Description:
    ///
    /// Converts expired suspect entries to down and returns the gossip events to disseminate.
    async fn swim_expire_suspicions(&self) -> Vec<TopologyEvent> {
        let now = Instant::now();
        let mut to_down = Vec::new();
        {
            let mut states = self.swim.peers.lock().await;
            for (peer_id, state) in states.iter_mut() {
                if state.status != ::health::Status::Suspect {
                    continue;
                }
                if state
                    .suspect_deadline
                    .map(|deadline| now >= deadline)
                    .unwrap_or(false)
                {
                    state.status = ::health::Status::Down;
                    state.first_failed_at = None;
                    state.suspect_deadline = None;
                    state.incarnation = state.incarnation.max(1);
                    to_down.push((*peer_id, state.incarnation));
                }
            }
        }

        let mut events = Vec::with_capacity(to_down.len());
        for (peer_id, incarnation) in to_down {
            self.health_monitor
                .set_status(peer_id, ::health::Status::Down);
            self.registry.invalidate_peer_capabilities(peer_id).await;
            events.push(TopologyEvent::Down {
                id: peer_id,
                incarnation,
            });
        }
        events
    }
}
