use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Bounded in-memory dedupe cache for recently seen message identifiers.
///
/// The cache uses two controls:
/// - `ttl`: entries older than this duration are expired lazily on insert.
/// - `max_entries`: hard upper bound to cap memory when traffic remains hot.
#[derive(Debug)]
pub struct BoundedSeenCache {
    max_entries: usize,
    ttl: Duration,
    seen: HashSet<Uuid>,
    order: VecDeque<(Uuid, Instant)>,
}

impl BoundedSeenCache {
    /// Constructs one cache with explicit TTL and capacity bounds.
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            max_entries: max_entries.max(1),
            ttl,
            seen: HashSet::with_capacity(max_entries.max(1)),
            order: VecDeque::with_capacity(max_entries.max(1)),
        }
    }

    /// Records one identifier and returns true only when it was not recently seen.
    ///
    /// This call also runs lazy expiration and capacity eviction so memory stays bounded.
    pub fn record(&mut self, id: Uuid) -> bool {
        let now = Instant::now();
        self.evict_expired(now);
        if self.seen.contains(&id) {
            return false;
        }

        self.seen.insert(id);
        self.order.push_back((id, now));
        self.evict_over_capacity();
        true
    }

    /// Evicts entries that exceed the TTL bound.
    fn evict_expired(&mut self, now: Instant) {
        while let Some((id, seen_at)) = self.order.front().copied() {
            if now.duration_since(seen_at) < self.ttl {
                break;
            }
            self.order.pop_front();
            self.seen.remove(&id);
        }
    }

    /// Evicts oldest entries until the cache respects the configured capacity limit.
    fn evict_over_capacity(&mut self) {
        while self.seen.len() > self.max_entries {
            let Some((id, _)) = self.order.pop_front() else {
                break;
            };
            self.seen.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedSeenCache;
    use std::time::Duration;
    use uuid::Uuid;

    /// Duplicate identifiers should only be accepted once while still in cache.
    #[test]
    fn dedupe_rejects_recent_duplicate() {
        let id = Uuid::new_v4();
        let mut cache = BoundedSeenCache::new(16, Duration::from_secs(60));
        assert!(cache.record(id));
        assert!(!cache.record(id));
    }

    /// Cache capacity bounds should evict oldest entries and allow re-accepting them later.
    #[test]
    fn dedupe_evicts_oldest_over_capacity() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();

        let mut cache = BoundedSeenCache::new(2, Duration::from_secs(60));
        assert!(cache.record(a));
        assert!(cache.record(b));
        assert!(cache.record(c));
        assert!(cache.record(a));
    }
}
