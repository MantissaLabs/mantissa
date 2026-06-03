use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result as AnyhowResult;
use arc_swap::ArcSwapOption;
use mantissa_store::uuid_key::UuidKey;
use parking_lot::RwLock as SyncRwLock;
use tracing::warn;
use uuid::Uuid;

use crate::registry::Registry;
use crate::store::replicated::scheduler::SchedulerStore;

use self::digest::{
    ObservedSchedulerDigest, SchedulerDigestPublisher, SchedulerDigestRegistry,
    SchedulerDigestValue,
};
use self::state::SchedulerState;

mod codec;
mod leases;
mod remote;
mod reservations;
mod resources;
mod state;
mod types;

pub mod digest;
pub mod placement;
pub mod service;
pub mod summary;

pub use self::types::*;

/// Returns the current Unix time in milliseconds for scheduler lease bookkeeping.
fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Scheduler maintains a local in-memory view of slots together with a CRDT-backed snapshot
/// that is ready to be gossiped to other peers.
pub struct Scheduler {
    store: SchedulerStore,
    store_key: UuidKey,
    state: Arc<ArcSwapOption<SchedulerState>>, // stores Option<Arc<SchedulerState>>
    registry: Registry,
    digest_publisher: SyncRwLock<Option<SchedulerDigestPublisher>>,
    digest_registry: SyncRwLock<Option<SchedulerDigestRegistry>>,
}

impl Scheduler {
    /// Creates one scheduler handle backed by the replicated store row for `resource_id`.
    pub fn new(
        store: SchedulerStore,
        registry: Registry,
        resource_id: Uuid,
    ) -> Result<Self, SchedulerError> {
        let store_key = UuidKey::from(resource_id);
        let existing_snapshot = store
            .get_snapshot(&store_key)?
            .and_then(|snap| snap.as_slice().last().cloned());

        let initial_state =
            existing_snapshot.map(|snapshot| Arc::new(SchedulerState::new(snapshot)));
        let state = Arc::new(ArcSwapOption::new(initial_state));

        Ok(Self {
            store,
            store_key,
            state,
            registry,
            digest_publisher: SyncRwLock::new(None),
            digest_registry: SyncRwLock::new(None),
        })
    }

    /// Returns the next scheduler snapshot version without panicking on counter exhaustion.
    fn next_snapshot_version(snapshot: &SchedulerSnapshot) -> Result<u64, SchedulerError> {
        snapshot
            .version
            .checked_add(1)
            .ok_or_else(|| SchedulerError::SnapshotVersionOverflow {
                snapshot: snapshot.clone(),
            })
    }

    /// Attaches the scheduler digest publisher used to replicate shortlist metadata.
    pub fn set_digest_publisher(&self, publisher: SchedulerDigestPublisher) {
        *self.digest_publisher.write() = Some(publisher);
    }

    /// Attaches the scheduler digest registry used by the planner for shortlist reads.
    pub fn set_digest_registry(&self, registry: SchedulerDigestRegistry) {
        *self.digest_registry.write() = Some(registry);
    }

    /// Returns the latest canonical scheduler digest rows replicated for shortlist selection.
    pub fn scheduler_digests(&self) -> AnyhowResult<Vec<SchedulerDigestValue>> {
        let registry = self.digest_registry.read().clone();

        let Some(registry) = registry else {
            return Ok(Vec::new());
        };

        registry.list()
    }

    /// Returns the latest canonical scheduler digests together with local ingest timestamps.
    pub fn observed_scheduler_digests(&self) -> AnyhowResult<Vec<ObservedSchedulerDigest>> {
        let registry = self.digest_registry.read().clone();

        let Some(registry) = registry else {
            return Ok(Vec::new());
        };

        registry.list_observed()
    }

    /// Upserts one observed remote scheduler digest into the local replicated digest cache.
    pub async fn observe_scheduler_digest(&self, digest: SchedulerDigestValue) -> AnyhowResult<()> {
        let registry = self.digest_registry.read().clone();

        let Some(registry) = registry else {
            return Ok(());
        };

        registry.upsert(digest).await
    }

    /// Returns the latest in-memory scheduler snapshot, if the scheduler has been initialized.
    pub async fn snapshot(&self) -> Option<SchedulerSnapshot> {
        self.state
            .load_full()
            .as_ref()
            .map(|state| state.snapshot.clone())
    }

    /// Publishes one compact digest for the provided snapshot when the publisher is configured.
    async fn publish_digest_from_snapshot(&self, snapshot: &SchedulerSnapshot) {
        let publisher = self.digest_publisher.read().clone();

        let Some(publisher) = publisher else {
            return;
        };

        if let Err(err) = publisher.publish_from_snapshot(snapshot).await {
            warn!(
                target: "scheduler",
                node_id = %self.store_key.to_uuid(),
                version = snapshot.version,
                "failed to publish scheduler digest: {err:#}"
            );
        }
    }

    /// Republishes the current scheduler snapshot through the attached digest publisher.
    ///
    /// Bootstrap uses this after wiring the digest publisher and registry onto an
    /// already-initialized scheduler so the initial capacity digest is visible to
    /// the local planner and to remote peers before any placements are attempted.
    pub async fn publish_current_digest(&self) {
        if let Some(snapshot) = self.snapshot().await {
            self.publish_digest_from_snapshot(&snapshot).await;
        }
    }
}

#[cfg(test)]
mod tests;
