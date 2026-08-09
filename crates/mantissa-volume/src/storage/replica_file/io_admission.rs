//! Locally applied volume state and per-request I/O admission.

use std::collections::{BTreeMap, btree_map};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use arc_swap::ArcSwap;
use mantissa_raft::ApplyContext;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedMutexGuard};

use crate::catalog::ReplicaKey;
use crate::control_state::{
    DataControlState, ReplacementGrant, VolumeControlState, VolumeDisposition, WriterGrant,
};
use crate::{
    DriverSessionId, FenceEpoch, RecoveryId, ReplacementId, VolumeDescriptor, VolumeNodeId,
};

#[cfg(test)]
mod tests;

const ADMISSION_DISABLED: u8 = 0;
const ADMISSION_ENABLED: u8 = 1;
const ADMISSION_RETIRED: u8 = 2;

/// One locally durable committed control state exposed to data request gates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedVolumeState {
    /// Raft position saved atomically with this applied state.
    pub applied: ApplyContext,

    /// Immutable volume generation descriptor.
    pub descriptor: VolumeDescriptor,

    /// Monotonic compare-and-set revision.
    pub revision: u64,

    /// Whether the generation is live or retained.
    pub disposition: VolumeDisposition,

    /// Current data fence, copy set, writer, and recovery grant.
    pub data: DataControlState,

    /// Current inactive-copy replacement grant, when any.
    pub replacement: Option<ReplacementGrant>,
}

impl AppliedVolumeState {
    /// Projects initialized committed state into its read-only data-path form.
    fn from_state(applied: ApplyContext, state: &VolumeControlState) -> Option<Self> {
        Some(Self {
            applied,
            descriptor: state.descriptor()?.clone(),
            revision: state.revision(),
            disposition: state.disposition(),
            data: state.data()?.clone(),
            replacement: state.replacement(),
        })
    }

    /// Reconstructs the bounded control state model for local level reconciliation.
    pub fn control_state(
        &self,
    ) -> Result<VolumeControlState, crate::control_state::VolumeControlStateInvariantError> {
        VolumeControlState::from_parts(
            Some(self.descriptor.clone()),
            self.revision,
            self.disposition,
            Some(self.data.clone()),
            self.replacement,
        )
    }
}

/// Lock-free current control state cell retained while its local replica exists.
pub struct AppliedVolumeStateCell {
    current: ArcSwap<AppliedVolumeState>,
}

impl AppliedVolumeStateCell {
    /// Creates one cell from durable initialized control state.
    fn new(applied_state: AppliedVolumeState) -> Self {
        Self {
            current: ArcSwap::from_pointee(applied_state),
        }
    }

    /// Loads one immutable control-state snapshot without contacting Raft.
    #[must_use]
    pub fn load(&self) -> Arc<AppliedVolumeState> {
        self.current.load_full()
    }

    /// Publishes newer durable applied state after monotonic validation.
    fn publish(
        &self,
        applied_state: AppliedVolumeState,
    ) -> Result<(), AppliedVolumeStatePublicationError> {
        let current = self.current.load_full();
        if applied_state.applied.index() < current.applied.index() {
            return Err(AppliedVolumeStatePublicationError::StaleAppliedIndex {
                current: current.applied.index(),
                proposed: applied_state.applied.index(),
            });
        }
        if applied_state.applied.index() == current.applied.index() {
            if applied_state == *current {
                return Ok(());
            }
            return Err(
                AppliedVolumeStatePublicationError::ConflictingAppliedEntry {
                    index: applied_state.applied.index(),
                },
            );
        }
        if applied_state.revision < current.revision
            || applied_state.data.fence < current.data.fence
            || applied_state.descriptor != current.descriptor
        {
            return Err(AppliedVolumeStatePublicationError::ControlStateRegressed);
        }
        self.current.store(Arc::new(applied_state));
        Ok(())
    }
}

/// Node-wide current control state for all locally known volume generations.
#[derive(Default)]
pub struct AppliedVolumeStateRegistry {
    entries: RwLock<BTreeMap<ReplicaKey, Arc<AppliedVolumeStateCell>>>,
}

impl AppliedVolumeStateRegistry {
    /// Creates an empty registry populated during local startup recovery.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publishes state only after its caller has durably saved the same entry.
    pub fn publish(
        &self,
        applied: ApplyContext,
        state: &VolumeControlState,
    ) -> Result<Option<Arc<AppliedVolumeStateCell>>, AppliedVolumeStatePublicationError> {
        state
            .validate()
            .map_err(|_| AppliedVolumeStatePublicationError::InvalidControlState)?;
        let Some(applied_state) = AppliedVolumeState::from_state(applied, state) else {
            return Ok(None);
        };
        let key = ReplicaKey::from(&applied_state.descriptor);
        let mut entries = self.entries.write();
        match entries.entry(key) {
            btree_map::Entry::Vacant(entry) => {
                let cell = Arc::new(AppliedVolumeStateCell::new(applied_state));
                entry.insert(Arc::clone(&cell));
                Ok(Some(cell))
            }
            btree_map::Entry::Occupied(entry) => {
                let cell = Arc::clone(entry.get());
                cell.publish(applied_state)?;
                Ok(Some(cell))
            }
        }
    }

    /// Returns one current per-generation cell without retaining the map lock.
    #[must_use]
    pub fn cell(&self, key: ReplicaKey) -> Option<Arc<AppliedVolumeStateCell>> {
        self.entries.read().get(&key).cloned()
    }

    /// Returns the number of locally retained generation applied-state cells.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns whether no local generation state has been recovered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Removes stale applied state after durable local revocation exists.
    ///
    /// The caller must first persist a local deleting or retiring catalog row.
    /// Terminal desired deletion and committed membership exclusion are both
    /// fail-closed facts even if this cached cell still contains older live
    /// applied state.
    pub fn remove_locally_revoked(
        &self,
        key: ReplicaKey,
        admission: &Arc<FenceAdmission>,
    ) -> Result<(), AppliedVolumeStateRemovalError> {
        let mut entries = self.entries.write();
        let cell = entries
            .get(&key)
            .ok_or(AppliedVolumeStateRemovalError::NotFound)?;
        if !Arc::ptr_eq(cell, &admission.applied_volume_state) {
            return Err(AppliedVolumeStateRemovalError::WrongAdmissionGate);
        }
        admission.retire();
        if !admission.in_flight.lock().is_empty() {
            return Err(AppliedVolumeStateRemovalError::RequestsInFlight);
        }
        entries.remove(&key);
        Ok(())
    }
}

/// Permission category checked against the complete current grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoRequestKind {
    /// Normal block traffic from the exact current writer session.
    Foreground,

    /// Durable installation of the exact current writer fence.
    InstallFence,

    /// Stable range read from the committed recovery source.
    RecoverySource(RecoveryId),

    /// Range mutation or sync on a committed recovery target.
    RecoveryTarget(RecoveryId),

    /// Stable or online range read from the replacement source.
    ReplacementSource(ReplacementId),

    /// Range mutation or sync on the inactive replacement target.
    ReplacementTarget(ReplacementId),
}

/// Complete identity rechecked for every request on an authenticated stream.
#[derive(Clone, Debug)]
pub struct IoAdmissionRequest<'a> {
    /// Descriptor carried by the action being admitted.
    pub descriptor: &'a VolumeDescriptor,

    /// Fence carried by the already-open connection and request.
    pub fence: FenceEpoch,

    /// Driver or maintenance session fixed in the connection header.
    pub session_id: DriverSessionId,

    /// Node authenticated by the underlying Noise transport.
    pub authenticated_peer: VolumeNodeId,

    /// Exact capability needed by this request action.
    pub kind: IoRequestKind,
}

/// Counts work already admitted under current and recently draining fences.
pub struct FenceAdmission {
    applied_volume_state: Arc<AppliedVolumeStateCell>,
    local_node: VolumeNodeId,
    local_state: AtomicU8,
    in_flight: Mutex<BTreeMap<FenceEpoch, usize>>,
    drained: Notify,
    install_lane: Arc<AsyncMutex<()>>,
}

impl FenceAdmission {
    /// Creates a fail-closed local gate around recovered committed control state.
    #[must_use]
    pub fn new(
        applied_volume_state: Arc<AppliedVolumeStateCell>,
        local_node: VolumeNodeId,
    ) -> Arc<Self> {
        Arc::new(Self {
            applied_volume_state,
            local_node,
            local_state: AtomicU8::new(ADMISSION_DISABLED),
            in_flight: Mutex::new(BTreeMap::new()),
            drained: Notify::new(),
            install_lane: Arc::new(AsyncMutex::new(())),
        })
    }

    /// Enables serving only for the exact durable entry inspected by a caller.
    pub fn enable_for(&self, applied_index: u64) -> Result<(), IoAdmissionError> {
        if self.local_state.load(Ordering::Acquire) == ADMISSION_RETIRED {
            return Err(IoAdmissionError::Retired);
        }
        let applied_state = self.applied_volume_state.load();
        if applied_state.applied.index() != applied_index
            || !local_node_belongs(&applied_state, self.local_node)
            || applied_state.disposition != VolumeDisposition::Live
        {
            return Err(IoAdmissionError::StaleEnable);
        }
        match self.local_state.compare_exchange(
            ADMISSION_DISABLED,
            ADMISSION_ENABLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(ADMISSION_ENABLED) => {}
            Err(ADMISSION_RETIRED) => return Err(IoAdmissionError::Retired),
            Err(_) => return Err(IoAdmissionError::LocallyDisabled),
        }
        // Close the publication race: if control-state changed after validation,
        // disable again and require the reconciler to inspect the new entry.
        if self.applied_volume_state.load().applied.index() != applied_index {
            let _ = self.local_state.compare_exchange(
                ADMISSION_ENABLED,
                ADMISSION_DISABLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            return Err(IoAdmissionError::StaleEnable);
        }
        match self.local_state.load(Ordering::Acquire) {
            ADMISSION_ENABLED => Ok(()),
            ADMISSION_RETIRED => Err(IoAdmissionError::Retired),
            _ => Err(IoAdmissionError::LocallyDisabled),
        }
    }

    /// Fails closed immediately without waiting for already admitted work.
    pub fn disable(&self) {
        let _ = self.local_state.compare_exchange(
            ADMISSION_ENABLED,
            ADMISSION_DISABLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.drained.notify_waiters();
    }

    /// Returns whether local admission is currently enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.local_state.load(Ordering::Acquire) == ADMISSION_ENABLED
    }

    /// Admits one request at a linearized counter increment and revalidation.
    pub fn admit(
        self: &Arc<Self>,
        request: &IoAdmissionRequest<'_>,
    ) -> Result<FencePermit, IoAdmissionError> {
        if self.local_state.load(Ordering::Acquire) != ADMISSION_ENABLED {
            return Err(IoAdmissionError::LocallyDisabled);
        }
        validate_request(&self.applied_volume_state.load(), self.local_node, request)?;

        let mut in_flight = self.in_flight.lock();
        if self.local_state.load(Ordering::Acquire) != ADMISSION_ENABLED {
            return Err(IoAdmissionError::LocallyDisabled);
        }
        validate_request(&self.applied_volume_state.load(), self.local_node, request)?;
        let count = in_flight.entry(request.fence).or_default();
        *count = count
            .checked_add(1)
            .ok_or(IoAdmissionError::RequestCountExhausted)?;

        // The increment is the admission linearization point. Publication or
        // local disable before it is visible to this final validation and is
        // rolled back. A change after it must observe and drain this permit.
        let final_result = if self.local_state.load(Ordering::Acquire) == ADMISSION_ENABLED {
            validate_request(&self.applied_volume_state.load(), self.local_node, request)
        } else {
            Err(IoAdmissionError::LocallyDisabled)
        };
        if let Err(error) = final_result {
            decrement_count(&mut in_flight, request.fence);
            drop(in_flight);
            self.drained.notify_waiters();
            return Err(error);
        }
        drop(in_flight);
        Ok(FencePermit {
            lease: Arc::new(FencePermitLease {
                admission: Arc::clone(self),
                fence: request.fence,
            }),
        })
    }

    /// Waits cancellation-safely until every older-fence request is terminal.
    pub async fn wait_for_older_than(&self, fence: FenceEpoch) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if !self
                .in_flight
                .lock()
                .range(..fence)
                .any(|(_, count)| *count != 0)
            {
                return;
            }
            drained.await;
        }
    }

    /// Serializes one fence install after every earlier-fence request drains.
    pub async fn prepare_fence_install(
        self: &Arc<Self>,
        fence: FenceEpoch,
    ) -> Result<FenceInstallGuard, IoAdmissionError> {
        let lane = Arc::clone(&self.install_lane).lock_owned().await;
        self.wait_for_older_than(fence).await;
        if self.local_state.load(Ordering::Acquire) != ADMISSION_ENABLED {
            return Err(IoAdmissionError::LocallyDisabled);
        }
        let applied_state = self.applied_volume_state.load();
        if applied_state.disposition != VolumeDisposition::Live {
            return Err(IoAdmissionError::VolumeNotLive);
        }
        if applied_state.data.fence != fence {
            return Err(IoAdmissionError::StaleFence {
                current: applied_state.data.fence,
                requested: fence,
            });
        }
        if !applied_state.data.copies.contains(&self.local_node) {
            return Err(IoAdmissionError::Unauthorized);
        }
        Ok(FenceInstallGuard { _lane: lane })
    }

    /// Returns current non-zero permit counts for metrics and tests.
    #[must_use]
    pub fn in_flight(&self) -> BTreeMap<FenceEpoch, usize> {
        self.in_flight.lock().clone()
    }

    /// Returns the immutable applied-state cell used by this local gate.
    #[must_use]
    pub fn applied_volume_state(&self) -> &Arc<AppliedVolumeStateCell> {
        &self.applied_volume_state
    }

    /// Permanently prevents this local gate from being enabled again.
    fn retire(&self) {
        self.local_state.store(ADMISSION_RETIRED, Ordering::Release);
        self.drained.notify_waiters();
    }
}

/// Exclusive local file-control ownership for one validated fence install.
pub struct FenceInstallGuard {
    _lane: OwnedMutexGuard<()>,
}

/// Keeps one admitted request visible until its actual disk work is terminal.
#[derive(Clone)]
pub struct FencePermit {
    lease: Arc<FencePermitLease>,
}

/// Shared lease released only after disk work and its response owner finish.
struct FencePermitLease {
    admission: Arc<FenceAdmission>,
    fence: FenceEpoch,
}

impl FencePermit {
    /// Returns the exact fence under which this work was admitted.
    #[must_use]
    pub fn fence(&self) -> FenceEpoch {
        self.lease.fence
    }
}

impl Drop for FencePermitLease {
    /// Releases one admission after every owner of its shared lease is gone.
    fn drop(&mut self) {
        let mut in_flight = self.admission.in_flight.lock();
        decrement_count(&mut in_flight, self.fence);
        drop(in_flight);
        self.admission.drained.notify_waiters();
    }
}

/// Removes one count and its fence key once no work remains.
fn decrement_count(in_flight: &mut BTreeMap<FenceEpoch, usize>, fence: FenceEpoch) {
    let remove = if let Some(count) = in_flight.get_mut(&fence) {
        *count = count.saturating_sub(1);
        *count == 0
    } else {
        false
    };
    if remove {
        in_flight.remove(&fence);
    }
}

/// Returns whether current control state recognizes this local replica at all.
fn local_node_belongs(applied_state: &AppliedVolumeState, local_node: VolumeNodeId) -> bool {
    applied_state.data.copies.contains(&local_node)
        || applied_state
            .replacement
            .is_some_and(|replacement| replacement.new_node_id == local_node)
}

/// Validates one action against applied state loaded for this exact request.
fn validate_request(
    applied_state: &AppliedVolumeState,
    local_node: VolumeNodeId,
    request: &IoAdmissionRequest<'_>,
) -> Result<(), IoAdmissionError> {
    if &applied_state.descriptor != request.descriptor {
        return Err(IoAdmissionError::WrongDescriptor);
    }
    if applied_state.disposition != VolumeDisposition::Live {
        return Err(IoAdmissionError::VolumeNotLive);
    }
    if applied_state.data.fence != request.fence {
        return Err(IoAdmissionError::StaleFence {
            current: applied_state.data.fence,
            requested: request.fence,
        });
    }
    let allowed = match request.kind {
        IoRequestKind::Foreground | IoRequestKind::InstallFence => {
            applied_state.data.copies.contains(&local_node)
                && applied_state.data.writer
                    == Some(WriterGrant {
                        node_id: request.authenticated_peer,
                        session_id: request.session_id,
                    })
        }
        IoRequestKind::RecoverySource(id) => {
            applied_state
                .data
                .recovery
                .as_ref()
                .is_some_and(|recovery| {
                    recovery.id == id
                        && recovery.coordinator_node_id == request.authenticated_peer
                        && recovery.source_node_id == local_node
                })
        }
        IoRequestKind::RecoveryTarget(id) => {
            applied_state
                .data
                .recovery
                .as_ref()
                .is_some_and(|recovery| {
                    recovery.id == id
                        && recovery.coordinator_node_id == request.authenticated_peer
                        && recovery.target_node_ids.contains(&local_node)
                })
        }
        IoRequestKind::ReplacementSource(id) => {
            applied_state.replacement.is_some_and(|replacement| {
                replacement.id == id
                    && replacement.coordinator_node_id == request.authenticated_peer
                    && replacement.source_node_id == local_node
            })
        }
        IoRequestKind::ReplacementTarget(id) => {
            applied_state.replacement.is_some_and(|replacement| {
                replacement.id == id
                    && replacement.coordinator_node_id == request.authenticated_peer
                    && replacement.new_node_id == local_node
            })
        }
    };
    if allowed {
        Ok(())
    } else {
        Err(IoAdmissionError::Unauthorized)
    }
}

/// Rejects non-monotonic or conflicting local control state publication.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AppliedVolumeStatePublicationError {
    /// The supplied state does not satisfy bounded control state invariants.
    #[error("cannot publish invalid volume control state")]
    InvalidControlState,

    /// A lower Raft log index cannot replace a newer locally durable entry.
    #[error("applied-state log index regressed from {current} to {proposed}")]
    StaleAppliedIndex {
        /// Current locally published log index.
        current: u64,

        /// Older index that publication attempted to install.
        proposed: u64,
    },

    /// One log index cannot contain two different application states.
    #[error("applied state differs at already published log index {index}")]
    ConflictingAppliedEntry {
        /// Conflicting applied log index.
        index: u64,
    },

    /// Revision, fence, or immutable descriptor regressed at a newer index.
    #[error("newer applied entry regresses volume control state")]
    ControlStateRegressed,
}

/// Rejects premature or mismatched applied-state cell garbage collection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AppliedVolumeStateRemovalError {
    /// No applied-state cell exists for the requested local generation.
    #[error("local volume control state cell was not found")]
    NotFound,

    /// The supplied admission gate belongs to another applied-state cell.
    #[error("local volume control state removal used the wrong admission gate")]
    WrongAdmissionGate,

    /// Previously admitted data work has not reached terminal completion.
    #[error("local volume control state still has requests in flight")]
    RequestsInFlight,
}

/// Rejects data work that lacks exact current and local control state.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum IoAdmissionError {
    /// Local quarantine, removal, or shutdown disabled this gate.
    #[error("local replica data admission is disabled")]
    LocallyDisabled,

    /// A removed generation gate can never be enabled again.
    #[error("local replica data admission is permanently retired")]
    Retired,

    /// The reconciler attempted to enable an obsolete applied entry.
    #[error("local replica cannot enable stale or inapplicable applied state")]
    StaleEnable,

    /// The request names another volume or destructive generation.
    #[error("replica data request descriptor does not match applied state")]
    WrongDescriptor,

    /// Retained generations cannot admit data requests.
    #[error("volume generation is not live")]
    VolumeNotLive,

    /// The request fence is no longer the locally applied fence.
    #[error("replica data fence {requested} is stale; current fence is {current}")]
    StaleFence {
        /// Current locally applied data fence.
        current: FenceEpoch,

        /// Fence carried by the rejected request.
        requested: FenceEpoch,
    },

    /// Peer, session, local role, or maintenance grant does not match.
    #[error("replica data request is not authorized by current control state")]
    Unauthorized,

    /// No larger in-memory admitted request count can be represented.
    #[error("replica data admitted request count is exhausted")]
    RequestCountExhausted,
}
