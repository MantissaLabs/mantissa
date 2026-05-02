//! Store-local garbage collection policy and report types.
//!
//! Distributed safety signals are supplied by callers. The generic store only
//! needs the resulting barrier and policy knobs so it can prune durable rows
//! without knowing anything about peer membership.

/// Barrier proving active peers have matched one domain root at a schema version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcBarrier {
    /// Oldest local equal-root observation across the active peer set.
    pub safe_observed_before_unix_ms: u64,
    /// Number of peers covered by this barrier, including the local node.
    pub active_peer_count: usize,
    /// Root schema version used for the equality observations.
    pub root_schema_version: u32,
}

/// Store-local GC policy knobs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreGcPolicy {
    /// Minimum local age a tombstone must reach before it can be pruned.
    pub tombstone_min_retention_ms: u64,
    /// Maximum tombstone index rows to process in one GC pass.
    pub tombstone_batch_limit: usize,
    /// Maximum register rows to inspect in one future MVReg compaction pass.
    pub mvreg_batch_limit: usize,
    /// Maximum concurrent MVReg values to retain, when compaction is enabled.
    pub mvreg_max_values: Option<usize>,
}

impl Default for StoreGcPolicy {
    /// Builds a conservative policy that does no work until limits are set.
    fn default() -> Self {
        Self {
            tombstone_min_retention_ms: 0,
            tombstone_batch_limit: 0,
            mvreg_batch_limit: 0,
            mvreg_max_values: None,
        }
    }
}

/// Store-local GC counters returned by one maintenance pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreGcReport {
    /// Tombstone age-index rows considered before hitting cutoff or batch limit.
    pub tombstones_scanned: usize,
    /// Primary tombstone rows removed from the replicated store.
    pub tombstones_pruned: usize,
    /// Register rows inspected by future MVReg compaction.
    pub registers_scanned: usize,
    /// Register rows rewritten by future MVReg compaction.
    pub registers_compacted: usize,
}
