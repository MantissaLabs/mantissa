#![no_main]

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use mantissa::dedupe::BoundedSeenCache;
use uuid::Uuid;

const MAX_OPS: usize = 256;

#[derive(Arbitrary, Debug)]
struct DedupeInput {
    capacity: u8,
    ops: Vec<DedupeOp>,
}

#[derive(Arbitrary, Debug)]
enum DedupeOp {
    SmallId { id: u8 },
    RawId { bytes: [u8; 16] },
}

fuzz_target!(|input: DedupeInput| {
    input.assert_long_ttl_matches_model();
    input.assert_zero_ttl_accepts_replays();
});

impl DedupeInput {
    /// Checks cache acceptance and capacity eviction against a simple reference model.
    fn assert_long_ttl_matches_model(&self) {
        let capacity = usize::from(self.capacity).max(1);
        let mut cache = BoundedSeenCache::new(capacity, Duration::from_secs(60));
        let mut model = DedupeModel::new(capacity);

        for op in self.ops.iter().take(MAX_OPS) {
            let id = op.uuid();
            assert_eq!(cache.record(id), model.record(id));
        }
    }

    /// Checks the zero-TTL edge where every previous entry expires before the next record.
    fn assert_zero_ttl_accepts_replays(&self) {
        let capacity = usize::from(self.capacity).max(1);
        let mut cache = BoundedSeenCache::new(capacity, Duration::ZERO);

        for op in self.ops.iter().take(MAX_OPS) {
            assert!(cache.record(op.uuid()));
        }
    }
}

impl DedupeOp {
    /// Builds a stable UUID from one generated operation.
    fn uuid(&self) -> Uuid {
        match self {
            Self::SmallId { id } => {
                let mut bytes = [0u8; 16];
                bytes[15] = *id;
                Uuid::from_bytes(bytes)
            }
            Self::RawId { bytes } => Uuid::from_bytes(*bytes),
        }
    }
}

struct DedupeModel {
    capacity: usize,
    seen: HashSet<Uuid>,
    order: VecDeque<Uuid>,
}

impl DedupeModel {
    /// Builds the long-TTL reference model for `BoundedSeenCache`.
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            seen: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    /// Records one identifier and returns the expected cache acceptance result.
    fn record(&mut self, id: Uuid) -> bool {
        if self.seen.contains(&id) {
            return false;
        }

        self.seen.insert(id);
        self.order.push_back(id);
        while self.seen.len() > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.seen.remove(&oldest);
        }
        true
    }
}
