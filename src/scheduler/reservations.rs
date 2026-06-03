use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use super::state::{SchedulerState, ptr_eq_option};
use super::{
    GpuDeviceReservation, GpuDeviceState, GpuReservationRequest, Scheduler, SchedulerError,
    SchedulerSnapshot, SlotId, SlotReservation, SlotReservationRequest, SlotState, current_unix_ms,
};

impl Scheduler {
    /// Reserves slots only (legacy path) by delegating to `reserve_resources`.
    #[allow(dead_code)]
    pub async fn reserve_slots(
        &self,
        expected_version: u64,
        requests: Vec<SlotReservationRequest>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        self.reserve_resources(expected_version, requests, Vec::new())
            .await
    }

    /// Reserves slots and GPU devices in a single optimistic transaction.
    pub async fn reserve_resources(
        &self,
        expected_version: u64,
        slot_requests: Vec<SlotReservationRequest>,
        gpu_requests: Vec<GpuReservationRequest>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if slot_requests.is_empty() && gpu_requests.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        // Reservations mutate the shared snapshot, so we retry until our compare-and-swap (CAS)
        // succeeds. Each iteration works against an immutable view of the current scheduler
        // state which guarantees consistent validation while preventing write tearing.
        loop {
            // Snapshot the current scheduler state. CAS means the pointer we read can go stale, so
            // everything below must be prepared to restart if another writer wins the race.
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            // Callers pass the version they observed; we only proceed if nothing changed in the
            // meantime. This enforces optimistic concurrency semantics for the scheduler API.
            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            Self::clear_expired_leases(&mut new_snapshot, current_unix_ms());

            // Track the validation outcome using deterministic sets so callers receive stable
            // ordering without the extra sort/dedup passes we previously needed.
            let mut slot_seen = HashSet::with_capacity(slot_requests.len());
            let mut slot_duplicates = BTreeSet::new();
            let mut slot_unknown = BTreeSet::new();
            let mut slot_conflicts = BTreeSet::new();

            for req in &slot_requests {
                // Reject duplicate requests first.
                if !slot_seen.insert(req.slot_id) {
                    slot_duplicates.insert(req.slot_id);
                    continue;
                }

                match current.slot_index.get(&req.slot_id) {
                    Some(&idx) => {
                        if !matches!(new_snapshot.slots[idx].state, SlotState::Free) {
                            slot_conflicts.insert(req.slot_id);
                        }
                    }
                    None => {
                        slot_unknown.insert(req.slot_id);
                    }
                }
            }

            if !slot_duplicates.is_empty() {
                return Err(SchedulerError::DuplicateSlots {
                    duplicates: slot_duplicates.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !slot_unknown.is_empty() {
                return Err(SchedulerError::UnknownSlots {
                    unknown: slot_unknown.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !slot_conflicts.is_empty() {
                return Err(SchedulerError::SlotsUnavailable {
                    conflicts: slot_conflicts.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut gpu_seen = HashSet::with_capacity(gpu_requests.len());
            let mut gpu_duplicates = BTreeSet::new();
            let mut gpu_unknown = BTreeSet::new();
            let mut gpu_conflicts = BTreeSet::new();

            for req in &gpu_requests {
                if !gpu_seen.insert(req.device_id.clone()) {
                    gpu_duplicates.insert(req.device_id.clone());
                    continue;
                }

                match current.gpu_index.get(&req.device_id) {
                    Some(&idx) => {
                        if !matches!(new_snapshot.gpu_devices[idx].state, GpuDeviceState::Free) {
                            gpu_conflicts.insert(req.device_id.clone());
                        }
                    }
                    None => {
                        gpu_unknown.insert(req.device_id.clone());
                    }
                }
            }

            if !gpu_duplicates.is_empty() {
                return Err(SchedulerError::DuplicateGpuDevices {
                    duplicates: gpu_duplicates.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !gpu_unknown.is_empty() {
                return Err(SchedulerError::UnknownGpuDevices {
                    unknown: gpu_unknown.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !gpu_conflicts.is_empty() {
                return Err(SchedulerError::GpuDevicesUnavailable {
                    conflicts: gpu_conflicts.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            for req in &slot_requests {
                let idx = current.slot_index[&req.slot_id];
                new_snapshot.slots[idx].state = SlotState::Reserved(SlotReservation {
                    owner: req.owner,
                    task_id: req.task_id,
                    group_id: req.group_id,
                });
            }
            for req in &gpu_requests {
                let idx = current.gpu_index[&req.device_id];
                new_snapshot.gpu_devices[idx].state =
                    GpuDeviceState::Reserved(GpuDeviceReservation {
                        owner: req.owner,
                        task_id: req.task_id,
                        group_id: req.group_id,
                    });
            }

            // Monotonic versioning gives downstream consumers a simple way to detect updates and
            // mirrors the MVReg behaviour in the backing store.
            new_snapshot.version = Self::next_snapshot_version(&new_snapshot)?;

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));

            // Attempt the CAS publication. A mismatch signals that another thread beat us, so we
            // restart the loop with the freshest pointer.
            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));

            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                // Durable persistence failed; roll back the published state so readers continue
                // to observe the pre-update snapshot.
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }

    /// Releases slots only (legacy path) by delegating to `free_resources`.
    pub async fn free_slots<I>(
        &self,
        expected_version: u64,
        slots: I,
    ) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotId>,
    {
        let slot_ids: Vec<SlotId> = slots.into_iter().collect();
        self.free_resources(expected_version, slot_ids, Vec::new())
            .await
    }

    /// Releases slots and GPU devices in a single optimistic transaction.
    pub async fn free_resources(
        &self,
        expected_version: u64,
        slots: Vec<SlotId>,
        gpu_device_ids: Vec<String>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        let slot_ids: BTreeSet<SlotId> = slots.into_iter().collect();
        let gpu_ids: BTreeSet<String> = gpu_device_ids.into_iter().collect();

        if slot_ids.is_empty() && gpu_ids.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        // Retry loop mirroring `reserve_resources` but toggling states back to free.
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            Self::clear_expired_leases(&mut new_snapshot, current_unix_ms());

            let mut unknown_slots = Vec::new();
            let mut not_reserved_slots = Vec::new();
            for slot_id in &slot_ids {
                let Some(&idx) = current.slot_index.get(slot_id) else {
                    unknown_slots.push(*slot_id);
                    continue;
                };

                if matches!(new_snapshot.slots[idx].state, SlotState::Free) {
                    not_reserved_slots.push(*slot_id);
                }
            }

            if !unknown_slots.is_empty() {
                return Err(SchedulerError::UnknownSlots {
                    unknown: unknown_slots,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !not_reserved_slots.is_empty() {
                return Err(SchedulerError::SlotsNotReserved {
                    slots: not_reserved_slots,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut unknown_gpus = Vec::new();
            let mut not_reserved_gpus = Vec::new();
            for device_id in &gpu_ids {
                let Some(&idx) = current.gpu_index.get(device_id) else {
                    unknown_gpus.push(device_id.clone());
                    continue;
                };

                if matches!(new_snapshot.gpu_devices[idx].state, GpuDeviceState::Free) {
                    not_reserved_gpus.push(device_id.clone());
                }
            }

            if !unknown_gpus.is_empty() {
                return Err(SchedulerError::UnknownGpuDevices {
                    unknown: unknown_gpus,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !not_reserved_gpus.is_empty() {
                return Err(SchedulerError::GpuDevicesNotReserved {
                    devices: not_reserved_gpus,
                    snapshot: current.snapshot.clone(),
                });
            }

            for slot_id in &slot_ids {
                let idx = current.slot_index[slot_id];
                new_snapshot.slots[idx].state = SlotState::Free;
            }
            for device_id in &gpu_ids {
                let idx = current.gpu_index[device_id];
                new_snapshot.gpu_devices[idx].state = GpuDeviceState::Free;
            }

            new_snapshot.version = Self::next_snapshot_version(&new_snapshot)?;

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));

            let prev = self
                .state
                .compare_and_swap(&current_opt, Some(new_state_arc.clone()));

            if !ptr_eq_option(&prev, &current_opt) {
                continue;
            }

            if let Err(e) = self
                .store
                .upsert(&self.store_key, new_snapshot.clone())
                .await
            {
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            return Ok(new_snapshot);
        }
    }
}
