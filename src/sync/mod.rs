//! Mantissa's view-scoped anti-entropy protocol.
//!
//! Sync intentionally runs in three phases so large clusters do not ship full snapshots on every
//! reconciliation pass:
//! 1. `get_roots_for_view` compares cheap MST roots per replicated domain.
//! 2. `get_ranges_for_view` narrows mismatches down to page digest ranges.
//! 3. `open_delta_for_view` filters mismatched pages by row digest before streaming fragments.
//!
//! All sync traffic is scoped to an explicit `ClusterViewId` so anti-entropy stays inside one
//! control-plane lineage.

mod encoding;

pub mod delta;
pub mod gc_progress;
mod service;

pub use crate::store::replicated::registry::{
    REPLICATED_DOMAINS as ALL_DOMAINS, ReplicatedStoreRegistry as SyncStores,
};
pub use delta::{SyncRunner, SyncTraceContext};
pub use gc_progress::SyncGcProgress;
pub use mantissa_store::gc::GcBarrier;
pub use service::SyncService;

/// Replicated domains that must continue converging across split view boundaries.
///
/// These rows describe cluster lineage and the key material needed to enter a
/// target view. Workload and service state remain view-scoped.
pub const CLUSTER_WIDE_DOMAINS: [mantissa_protocol::sync::Domain; 4] = [
    mantissa_protocol::sync::Domain::ClusterViews,
    // Membership remains globally discoverable even though exclusions fence ordinary peer
    // traffic after a split. It also defines the participant set for the key-GC frontier.
    mantissa_protocol::sync::Domain::Peers,
    // Transfer transition key prerequisites before the intent that authorizes local cutover. A
    // failed partial sync may leave harmless unused keys, but never an operation with keys that
    // were skipped merely because the same stream disconnected first.
    mantissa_protocol::sync::Domain::SecretMasterKeys,
    mantissa_protocol::sync::Domain::ClusterOperations,
];

/// Returns whether a replicated domain belongs to the cluster-wide metadata plane.
pub fn is_cluster_wide_domain(domain: mantissa_protocol::sync::Domain) -> bool {
    CLUSTER_WIDE_DOMAINS.contains(&domain)
}

/// Number of replicated domains exposed through view-scoped sync RPCs.
pub const VIEW_SCOPED_DOMAIN_COUNT: usize = ALL_DOMAINS.len();
/// Default max entries per streamed delta chunk.
pub const DEFAULT_DELTA_CHUNK_MAX: usize = 8192;
/// Default approximate payload target per streamed delta chunk.
pub const DEFAULT_DELTA_CHUNK_TARGET_BYTES: usize = 1024 * 1024;

/// Reads the max register/tombstone entries per streamed delta chunk.
///
/// Keeping this configurable helps scale anti-entropy payload sizing without rebuilding.
fn delta_chunk_max() -> usize {
    std::env::var("MANTISSA_SYNC_DELTA_CHUNK_MAX")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DELTA_CHUNK_MAX)
}

/// Reads the approximate payload target per streamed delta chunk.
///
/// Chunk sizing stays approximate because the sender already has the encoded entries on hand
/// and only needs a stable batching signal, not byte-perfect Cap'n Proto accounting.
fn delta_chunk_target_bytes() -> usize {
    std::env::var("MANTISSA_SYNC_DELTA_CHUNK_TARGET_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DELTA_CHUNK_TARGET_BYTES)
}

/// Normalizes storage/runtime errors into Cap'n Proto failures for RPC propagation.
fn to_capnp<E: std::fmt::Display>(e: E) -> capnp::Error {
    capnp::Error::failed(e.to_string())
}
