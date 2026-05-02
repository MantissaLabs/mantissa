#![cfg_attr(test, allow(clippy::unwrap_used))]

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Unknown,
    Alive,
    Suspect,
    Down,
    Degraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwimEvent {
    Alive { id: Uuid, incarnation: u64 },
    Suspect { id: Uuid, incarnation: u64 },
    Down { id: Uuid, incarnation: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    Gossip(SwimEvent),
    InvalidatePeer(Uuid),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerState {
    incarnation: u64,
    status: Status,
    first_failed_at: Option<Instant>,
    suspect_deadline: Option<Instant>,
}

impl Default for PeerState {
    /// # Description:
    ///
    /// Creates the baseline SWIM state for a peer before any liveness signal is observed.
    fn default() -> Self {
        Self {
            incarnation: 0,
            status: Status::Unknown,
            first_failed_at: None,
            suspect_deadline: None,
        }
    }
}

pub struct HealthMonitor {
    local_id: Uuid,
    peers: Mutex<HashMap<Uuid, PeerState>>,
    probe_cursor: Mutex<usize>,
    local_incarnation: AtomicU64,
}

/// # Description:
///
/// Computes an ordering rank for SWIM statuses when incarnation numbers are equal.
fn swim_status_rank(status: Status) -> u8 {
    match status {
        Status::Unknown => 0,
        Status::Alive => 1,
        Status::Degraded => 1,
        Status::Suspect => 2,
        Status::Down => 3,
    }
}

impl HealthMonitor {
    /// # Description:
    ///
    /// Creates one detector seeded with the local node identifier and a monotonic incarnation.
    pub fn new(local_id: Uuid) -> Arc<Self> {
        let boot_incarnation = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(1)
            .max(1);

        Arc::new(Self {
            local_id,
            peers: Mutex::new(HashMap::new()),
            probe_cursor: Mutex::new(local_id.as_u128() as usize),
            local_incarnation: AtomicU64::new(boot_incarnation),
        })
    }

    /// # Description:
    ///
    /// Returns the current local incarnation used to refute stale suspect/down rumors.
    pub fn local_incarnation(&self) -> u64 {
        self.local_incarnation.load(Ordering::SeqCst)
    }

    /// Advances the local incarnation for one explicit membership transition.
    pub fn advance_local_incarnation(&self) -> u64 {
        let next = self
            .local_incarnation
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);

        let mut peers = self.peers.lock();
        let state = peers.entry(self.local_id).or_default();
        state.incarnation = state.incarnation.max(next);
        next
    }

    /// # Description:
    ///
    /// Chooses the next probe index for one candidate list using a deterministic round-robin cursor.
    pub fn next_probe_index(&self, candidates_len: usize) -> Option<usize> {
        if candidates_len == 0 {
            return None;
        }

        let mut cursor = self.probe_cursor.lock();
        let index = *cursor % candidates_len;
        *cursor = (*cursor + 1) % candidates_len;
        Some(index)
    }

    /// # Description:
    ///
    /// Seeds peer detector state when membership has already established the peer as present.
    pub fn record_join(&self, id: Uuid, incarnation: u64) {
        let known_incarnation = if id == self.local_id {
            let next = self.local_incarnation().max(incarnation);
            self.local_incarnation.store(next, Ordering::SeqCst);
            next
        } else {
            incarnation
        };

        let mut peers = self.peers.lock();
        let state = peers.entry(id).or_default();
        state.incarnation = state.incarnation.max(known_incarnation);
        state.status = Status::Alive;
        state.first_failed_at = None;
        state.suspect_deadline = None;
    }

    /// # Description:
    ///
    /// Records a successful passive liveness observation outside the active probe loop.
    pub fn record_observation(&self, id: Uuid) -> Option<SwimEvent> {
        self.record_success(id, false)
    }

    /// # Description:
    ///
    /// Applies one incoming `alive` SWIM update with incarnation ordering and self-heal handling.
    pub fn handle_alive_event(&self, id: Uuid, incarnation: u64) -> Vec<Action> {
        if id == self.local_id {
            if incarnation > self.local_incarnation() {
                self.local_incarnation.store(incarnation, Ordering::SeqCst);
            }
            self.record_join(id, self.local_incarnation());
            return Vec::new();
        }

        self.apply_remote_update(id, incarnation, Status::Alive, None);
        Vec::new()
    }

    /// # Description:
    ///
    /// Applies one incoming `suspect` SWIM update or emits an alive refutation for the local node.
    pub fn handle_suspect_event(
        &self,
        id: Uuid,
        incarnation: u64,
        down_after: Duration,
    ) -> Vec<Action> {
        if id == self.local_id {
            return self
                .refute_self_suspicion(incarnation)
                .map(|next| {
                    Action::Gossip(SwimEvent::Alive {
                        id: self.local_id,
                        incarnation: next,
                    })
                })
                .into_iter()
                .collect();
        }

        self.apply_remote_update(id, incarnation, Status::Suspect, Some(down_after));
        Vec::new()
    }

    /// # Description:
    ///
    /// Applies one incoming `down` SWIM update or emits an alive refutation for the local node.
    pub fn handle_down_event(&self, id: Uuid, incarnation: u64) -> Vec<Action> {
        if id == self.local_id {
            return self
                .refute_self_suspicion(incarnation)
                .map(|next| {
                    Action::Gossip(SwimEvent::Alive {
                        id: self.local_id,
                        incarnation: next,
                    })
                })
                .into_iter()
                .collect();
        }

        if self.apply_remote_update(id, incarnation, Status::Down, None) {
            vec![Action::InvalidatePeer(id)]
        } else {
            Vec::new()
        }
    }

    /// # Description:
    ///
    /// Records one successful active probe and emits an alive rumor when status changed locally.
    pub fn record_probe_success(&self, peer_id: Uuid) -> Option<SwimEvent> {
        self.record_success(peer_id, true)
    }

    /// # Description:
    ///
    /// Records one failed active probe and emits suspect/down actions when thresholds are crossed.
    pub fn record_probe_failure(
        &self,
        peer_id: Uuid,
        suspect_after: Duration,
        down_after: Duration,
    ) -> Vec<Action> {
        let now = Instant::now();
        let mut peers = self.peers.lock();
        let state = peers.entry(peer_id).or_default();
        if state.incarnation == 0 {
            state.incarnation = 1;
        }
        if state.status == Status::Down {
            return Vec::new();
        }

        if state.first_failed_at.is_none() {
            state.first_failed_at = Some(now);
            return Vec::new();
        }

        if now.duration_since(state.first_failed_at.unwrap_or(now)) < suspect_after {
            return Vec::new();
        }

        if state.status != Status::Suspect {
            state.status = Status::Suspect;
            state.suspect_deadline = Some(now + down_after);
            return vec![Action::Gossip(SwimEvent::Suspect {
                id: peer_id,
                incarnation: state.incarnation,
            })];
        }

        if state
            .suspect_deadline
            .map(|deadline| now >= deadline)
            .unwrap_or(false)
        {
            state.status = Status::Down;
            state.first_failed_at = None;
            state.suspect_deadline = None;
            return vec![
                Action::InvalidatePeer(peer_id),
                Action::Gossip(SwimEvent::Down {
                    id: peer_id,
                    incarnation: state.incarnation,
                }),
            ];
        }

        Vec::new()
    }

    /// # Description:
    ///
    /// Converts expired suspect entries to down and returns the invalidate/gossip actions to emit.
    pub fn expire_suspicions(&self) -> Vec<Action> {
        let now = Instant::now();
        let mut to_down = Vec::new();
        {
            let mut peers = self.peers.lock();
            for (peer_id, state) in peers.iter_mut() {
                if state.status != Status::Suspect {
                    continue;
                }
                if state
                    .suspect_deadline
                    .map(|deadline| now >= deadline)
                    .unwrap_or(false)
                {
                    state.status = Status::Down;
                    state.first_failed_at = None;
                    state.suspect_deadline = None;
                    state.incarnation = state.incarnation.max(1);
                    to_down.push((*peer_id, state.incarnation));
                }
            }
        }

        let mut actions = Vec::with_capacity(to_down.len() * 2);
        for (peer_id, incarnation) in to_down {
            actions.push(Action::InvalidatePeer(peer_id));
            actions.push(Action::Gossip(SwimEvent::Down {
                id: peer_id,
                incarnation,
            }));
        }
        actions
    }

    /// # Description:
    ///
    /// Forgets one peer from the local detector, causing subsequent lookups to resolve to unknown.
    pub fn remove_peer(&self, id: Uuid) {
        self.peers.lock().remove(&id);
    }

    /// # Description:
    ///
    /// Returns the last locally selected health status for one peer.
    pub fn status(&self, id: Uuid) -> Status {
        self.peers
            .lock()
            .get(&id)
            .map(|state| state.status)
            .unwrap_or(Status::Unknown)
    }

    /// # Description:
    ///
    /// Clones the current peer-health view for consumers that need a stable point-in-time snapshot.
    pub fn snapshot(&self) -> HashMap<Uuid, Status> {
        self.peers
            .lock()
            .iter()
            .map(|(id, state)| (*id, state.status))
            .collect()
    }

    /// # Description:
    ///
    /// Applies one remote SWIM update with incarnation ordering and same-incarnation precedence.
    fn apply_remote_update(
        &self,
        id: Uuid,
        incarnation: u64,
        status: Status,
        down_after: Option<Duration>,
    ) -> bool {
        let now = Instant::now();
        let mut peers = self.peers.lock();
        let state = peers.entry(id).or_default();
        if incarnation < state.incarnation {
            return false;
        }

        let should_apply = if incarnation > state.incarnation {
            true
        } else {
            swim_status_rank(status) > swim_status_rank(state.status)
        };

        if !should_apply {
            return false;
        }

        state.incarnation = incarnation;
        state.status = status;
        state.first_failed_at = None;
        state.suspect_deadline = if matches!(status, Status::Suspect) {
            down_after.map(|delay| now + delay)
        } else {
            None
        };

        true
    }

    /// # Description:
    ///
    /// Increments the local incarnation when a remote suspect/down rumor targets this node.
    fn refute_self_suspicion(&self, observed_incarnation: u64) -> Option<u64> {
        let current = self.local_incarnation();
        if observed_incarnation < current {
            return None;
        }

        let next = observed_incarnation.saturating_add(1);
        self.local_incarnation.store(next, Ordering::SeqCst);
        self.record_join(self.local_id, next);
        Some(next)
    }

    /// # Description:
    ///
    /// Clears suspicion timers after one successful observation and optionally emits an alive rumor.
    fn record_success(&self, id: Uuid, emit_gossip: bool) -> Option<SwimEvent> {
        let mut peers = self.peers.lock();
        let state = peers.entry(id).or_default();
        let previous = state.status;
        if state.incarnation == 0 {
            state.incarnation = if id == self.local_id {
                self.local_incarnation()
            } else {
                1
            };
        }
        if id == self.local_id {
            state.incarnation = state.incarnation.max(self.local_incarnation());
        }
        state.status = Status::Alive;
        state.first_failed_at = None;
        state.suspect_deadline = None;

        if emit_gossip && previous != Status::Alive {
            Some(SwimEvent::Alive {
                id,
                incarnation: state.incarnation,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Action, HealthMonitor, Status, SwimEvent};
    use std::time::Duration;
    use uuid::Uuid;

    /// # Description:
    ///
    /// Ensures two failed observations promote to suspect once the suspicion threshold is met.
    #[test]
    fn probe_failures_escalate_to_suspect() {
        let peer_id = Uuid::new_v4();
        let monitor = HealthMonitor::new(Uuid::new_v4());

        assert!(
            monitor
                .record_probe_failure(peer_id, Duration::ZERO, Duration::from_secs(1))
                .is_empty()
        );

        let actions = monitor.record_probe_failure(peer_id, Duration::ZERO, Duration::ZERO);
        assert_eq!(
            actions,
            vec![Action::Gossip(SwimEvent::Suspect {
                id: peer_id,
                incarnation: 1,
            })]
        );
        assert_eq!(monitor.status(peer_id), Status::Suspect);
    }

    /// # Description:
    ///
    /// Ensures expired suspect entries promote to down and request capability invalidation.
    #[test]
    fn suspect_entries_expire_to_down() {
        let peer_id = Uuid::new_v4();
        let monitor = HealthMonitor::new(Uuid::new_v4());

        let _ = monitor.record_probe_failure(peer_id, Duration::ZERO, Duration::ZERO);
        let _ = monitor.record_probe_failure(peer_id, Duration::ZERO, Duration::ZERO);

        let actions = monitor.expire_suspicions();
        assert_eq!(
            actions,
            vec![
                Action::InvalidatePeer(peer_id),
                Action::Gossip(SwimEvent::Down {
                    id: peer_id,
                    incarnation: 1,
                }),
            ]
        );
        assert_eq!(monitor.status(peer_id), Status::Down);
    }

    /// # Description:
    ///
    /// Ensures remote self-suspicion is refuted by incrementing the local incarnation.
    #[test]
    fn self_suspicion_is_refuted() {
        let local_id = Uuid::new_v4();
        let monitor = HealthMonitor::new(local_id);
        let current = monitor.local_incarnation();

        let actions = monitor.handle_suspect_event(local_id, current, Duration::from_secs(1));
        assert_eq!(
            actions,
            vec![Action::Gossip(SwimEvent::Alive {
                id: local_id,
                incarnation: current + 1,
            })]
        );
        assert_eq!(monitor.local_incarnation(), current + 1);
        assert_eq!(monitor.status(local_id), Status::Alive);
    }

    /// # Description:
    ///
    /// Ensures passive observations clear local suspicion without maintaining a second status map.
    #[test]
    fn passive_observation_restores_alive() {
        let peer_id = Uuid::new_v4();
        let monitor = HealthMonitor::new(Uuid::new_v4());

        let _ = monitor.record_probe_failure(peer_id, Duration::ZERO, Duration::from_secs(10));
        let _ = monitor.record_probe_failure(peer_id, Duration::ZERO, Duration::from_secs(10));
        assert_eq!(monitor.status(peer_id), Status::Suspect);

        assert!(monitor.record_observation(peer_id).is_none());
        assert_eq!(monitor.status(peer_id), Status::Alive);
    }
}
