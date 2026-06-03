use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use uuid::Uuid;

use super::state::{SchedulerState, ptr_eq_option};
use super::{
    AbortTaskLeaseIntent, ExactTaskLeaseIntent, GpuDeviceReservation, GpuDeviceState,
    LeaseReservation, PreparedTaskLease, PreparedTaskLeaseBatch, Scheduler, SchedulerError,
    SchedulerSnapshot, SlotId, SlotReservation, SlotState, TaskLeaseIntent, current_unix_ms,
};

impl Scheduler {
    /// Releases every prepared lease whose expiry is at or before `now_unix_ms`.
    pub(super) fn clear_expired_leases(
        snapshot: &mut SchedulerSnapshot,
        now_unix_ms: u64,
    ) -> Vec<Uuid> {
        let mut expired = BTreeSet::new();

        for slot in &mut snapshot.slots {
            if let SlotState::Leased(lease) = &slot.state
                && lease.expires_at_unix_ms <= now_unix_ms
            {
                expired.insert(lease.lease_id);
                slot.state = SlotState::Free;
            }
        }

        for device in &mut snapshot.gpu_devices {
            if let GpuDeviceState::Leased(lease) = &device.state
                && lease.expires_at_unix_ms <= now_unix_ms
            {
                expired.insert(lease.lease_id);
                device.state = GpuDeviceState::Free;
            }
        }

        expired.into_iter().collect()
    }

    /// Releases uncommitted leases for one admission group before replacing its prepare attempt.
    fn clear_prepared_lease_group(snapshot: &mut SchedulerSnapshot, group_id: Uuid) {
        for slot in &mut snapshot.slots {
            if matches!(
                &slot.state,
                SlotState::Leased(LeaseReservation {
                    group_id: Some(existing),
                    ..
                }) if *existing == group_id
            ) {
                slot.state = SlotState::Free;
            }
        }

        for device in &mut snapshot.gpu_devices {
            if matches!(
                &device.state,
                GpuDeviceState::Leased(LeaseReservation {
                    group_id: Some(existing),
                    ..
                }) if *existing == group_id
            ) {
                device.state = GpuDeviceState::Free;
            }
        }
    }

    /// Selects the free slot indices visible after expired prepared leases have been reclaimed.
    fn free_slot_indices(snapshot: &SchedulerSnapshot) -> Vec<usize> {
        snapshot
            .slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| matches!(slot.state, SlotState::Free).then_some(idx))
            .collect()
    }

    /// Selects the free GPU indices visible after expired prepared leases have been reclaimed.
    fn free_gpu_indices(snapshot: &SchedulerSnapshot) -> Vec<usize> {
        snapshot
            .gpu_devices
            .iter()
            .enumerate()
            .filter_map(|(idx, device)| matches!(device.state, GpuDeviceState::Free).then_some(idx))
            .collect()
    }

    /// Selects exact free slot indices that satisfy one resource-vector request.
    fn select_slot_indices(
        snapshot: &SchedulerSnapshot,
        available_indices: &[usize],
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Option<Vec<usize>> {
        if available_indices.is_empty() {
            return None;
        }

        if cpu_millis == 0 && memory_bytes == 0 {
            return Some(vec![available_indices[0]]);
        }

        let mut remaining_cpu = cpu_millis;
        let mut remaining_memory = memory_bytes;
        let mut selected = Vec::new();
        let mut candidates = available_indices.to_vec();

        while remaining_cpu > 0 || remaining_memory > 0 {
            let mut best_choice = None;
            let mut best_score = 0u128;

            for &idx in &candidates {
                let slot = &snapshot.slots[idx];
                let cpu_contrib = std::cmp::min(slot.capacity.cpu_millis, remaining_cpu);
                let memory_contrib = std::cmp::min(slot.capacity.memory_bytes, remaining_memory);
                let score = ((cpu_contrib as u128) << 64) | memory_contrib as u128;

                if score > best_score {
                    best_score = score;
                    best_choice = Some(idx);
                }
            }

            let best_idx = best_choice?;
            if best_score == 0 {
                return None;
            }

            let slot = &snapshot.slots[best_idx];
            selected.push(best_idx);
            remaining_cpu = remaining_cpu.saturating_sub(slot.capacity.cpu_millis);
            remaining_memory = remaining_memory.saturating_sub(slot.capacity.memory_bytes);
            candidates.retain(|idx| *idx != best_idx);
        }

        Some(selected)
    }

    /// Selects exact free GPU indices that satisfy one GPU count request.
    fn select_gpu_indices(available_indices: &[usize], gpu_count: u32) -> Option<Vec<usize>> {
        if gpu_count == 0 {
            return Some(Vec::new());
        }

        let required = gpu_count as usize;
        if available_indices.len() < required {
            return None;
        }

        Some(available_indices.iter().copied().take(required).collect())
    }

    /// Prepares an admission-group lease for exact local slot and GPU bindings.
    pub async fn prepare_exact_task_lease_group(
        &self,
        expected_version: u64,
        coordinator_node_id: Uuid,
        group_id: Uuid,
        ttl_ms: u64,
        intents: Vec<ExactTaskLeaseIntent>,
    ) -> Result<PreparedTaskLeaseBatch, SchedulerError> {
        if intents.is_empty() {
            return Ok(PreparedTaskLeaseBatch { leases: Vec::new() });
        }

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
            let now_unix_ms = current_unix_ms();
            Self::clear_expired_leases(&mut new_snapshot, now_unix_ms);
            Self::clear_prepared_lease_group(&mut new_snapshot, group_id);

            let slot_capacity = intents.iter().map(|intent| intent.slot_ids.len()).sum();
            let gpu_capacity = intents
                .iter()
                .map(|intent| intent.gpu_device_ids.len())
                .sum();
            let mut slot_seen = HashSet::with_capacity(slot_capacity);
            let mut slot_duplicates = BTreeSet::new();
            let mut slot_unknown = BTreeSet::new();
            let mut slot_conflicts = BTreeSet::new();
            let mut gpu_seen = HashSet::with_capacity(gpu_capacity);
            let mut gpu_duplicates = BTreeSet::new();
            let mut gpu_unknown = BTreeSet::new();
            let mut gpu_conflicts = BTreeSet::new();

            for intent in &intents {
                for slot_id in &intent.slot_ids {
                    if !slot_seen.insert(*slot_id) {
                        slot_duplicates.insert(*slot_id);
                        continue;
                    }

                    match current.slot_index.get(slot_id) {
                        Some(&idx) => {
                            if !matches!(new_snapshot.slots[idx].state, SlotState::Free) {
                                slot_conflicts.insert(*slot_id);
                            }
                        }
                        None => {
                            slot_unknown.insert(*slot_id);
                        }
                    }
                }

                for device_id in &intent.gpu_device_ids {
                    if !gpu_seen.insert(device_id.clone()) {
                        gpu_duplicates.insert(device_id.clone());
                        continue;
                    }

                    match current.gpu_index.get(device_id) {
                        Some(&idx) => {
                            if !matches!(new_snapshot.gpu_devices[idx].state, GpuDeviceState::Free)
                            {
                                gpu_conflicts.insert(device_id.clone());
                            }
                        }
                        None => {
                            gpu_unknown.insert(device_id.clone());
                        }
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

            let expires_at_unix_ms = now_unix_ms.saturating_add(ttl_ms);
            let mut leases = Vec::with_capacity(intents.len());
            for intent in &intents {
                let lease_id = Uuid::new_v4();
                let reservation = LeaseReservation {
                    lease_id,
                    coordinator_node_id,
                    task_id: intent.task_id,
                    expires_at_unix_ms,
                    group_id: Some(group_id),
                };

                let mut slot_ids = intent.slot_ids.clone();
                slot_ids.sort_unstable();
                for slot_id in &slot_ids {
                    let idx = current.slot_index[slot_id];
                    new_snapshot.slots[idx].state = SlotState::Leased(reservation.clone());
                }

                let mut gpu_device_ids = intent.gpu_device_ids.clone();
                gpu_device_ids.sort();
                for device_id in &gpu_device_ids {
                    let idx = current.gpu_index[device_id];
                    new_snapshot.gpu_devices[idx].state =
                        GpuDeviceState::Leased(reservation.clone());
                }

                leases.push(PreparedTaskLease {
                    lease_id,
                    task_id: intent.task_id,
                    expires_at_unix_ms,
                    slot_ids,
                    gpu_device_ids,
                });
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
            return Ok(PreparedTaskLeaseBatch { leases });
        }
    }

    /// Prepares one batch of short-lived resource leases by choosing exact local bindings atomically.
    pub async fn prepare_task_leases(
        &self,
        coordinator_node_id: Uuid,
        ttl_ms: u64,
        intents: Vec<TaskLeaseIntent>,
    ) -> Result<PreparedTaskLeaseBatch, SchedulerError> {
        self.prepare_task_leases_with_group(coordinator_node_id, None, ttl_ms, intents)
            .await
    }

    /// Prepares one grouped batch of resource leases for a future all-or-nothing admission commit.
    pub async fn prepare_task_lease_group(
        &self,
        coordinator_node_id: Uuid,
        group_id: Uuid,
        ttl_ms: u64,
        intents: Vec<TaskLeaseIntent>,
    ) -> Result<PreparedTaskLeaseBatch, SchedulerError> {
        self.prepare_task_leases_with_group(coordinator_node_id, Some(group_id), ttl_ms, intents)
            .await
    }

    /// Shared prepare implementation used by both task-local and grouped lease paths.
    async fn prepare_task_leases_with_group(
        &self,
        coordinator_node_id: Uuid,
        group_id: Option<Uuid>,
        ttl_ms: u64,
        intents: Vec<TaskLeaseIntent>,
    ) -> Result<PreparedTaskLeaseBatch, SchedulerError> {
        if intents.is_empty() {
            crate::observability::metrics::record_scheduler_prepare("success", "empty");
            return Ok(PreparedTaskLeaseBatch { leases: Vec::new() });
        }

        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => {
                    let error = SchedulerError::Uninitialized;
                    crate::observability::metrics::record_scheduler_prepare(
                        "failure",
                        crate::observability::metrics::scheduler_error_reason(&error),
                    );
                    return Err(error);
                }
            };
            let current = current_arc.as_ref();

            let mut new_snapshot = current.snapshot.clone();
            let now_unix_ms = current_unix_ms();
            Self::clear_expired_leases(&mut new_snapshot, now_unix_ms);
            if let Some(group_id) = group_id {
                Self::clear_prepared_lease_group(&mut new_snapshot, group_id);
            }
            let expires_at_unix_ms = now_unix_ms.saturating_add(ttl_ms);
            let mut free_slot_indices = Self::free_slot_indices(&new_snapshot);
            let mut free_gpu_indices = Self::free_gpu_indices(&new_snapshot);
            let mut leases = Vec::with_capacity(intents.len());
            let mut failed_tasks = Vec::new();

            for intent in &intents {
                let Some(slot_indices) = Self::select_slot_indices(
                    &new_snapshot,
                    &free_slot_indices,
                    intent.cpu_millis,
                    intent.memory_bytes,
                ) else {
                    failed_tasks.push(intent.task_id);
                    let error = SchedulerError::InsufficientResources {
                        task_ids: failed_tasks,
                        snapshot: current.snapshot.clone(),
                    };
                    crate::observability::metrics::record_scheduler_prepare(
                        "failure",
                        crate::observability::metrics::scheduler_error_reason(&error),
                    );
                    return Err(error);
                };

                let Some(gpu_indices) =
                    Self::select_gpu_indices(&free_gpu_indices, intent.gpu_count)
                else {
                    failed_tasks.push(intent.task_id);
                    let error = SchedulerError::InsufficientResources {
                        task_ids: failed_tasks,
                        snapshot: current.snapshot.clone(),
                    };
                    crate::observability::metrics::record_scheduler_prepare(
                        "failure",
                        crate::observability::metrics::scheduler_error_reason(&error),
                    );
                    return Err(error);
                };

                let slot_index_set: HashSet<usize> = slot_indices.iter().copied().collect();
                let gpu_index_set: HashSet<usize> = gpu_indices.iter().copied().collect();
                let lease_id = Uuid::new_v4();
                let reservation = LeaseReservation {
                    lease_id,
                    coordinator_node_id,
                    task_id: intent.task_id,
                    expires_at_unix_ms,
                    group_id,
                };

                let mut slot_ids = Vec::with_capacity(slot_indices.len());
                for idx in &slot_indices {
                    let slot = &mut new_snapshot.slots[*idx];
                    slot.state = SlotState::Leased(reservation.clone());
                    slot_ids.push(slot.slot_id);
                }
                slot_ids.sort_unstable();

                let mut gpu_device_ids = Vec::with_capacity(gpu_indices.len());
                for idx in &gpu_indices {
                    let device = &mut new_snapshot.gpu_devices[*idx];
                    device.state = GpuDeviceState::Leased(reservation.clone());
                    gpu_device_ids.push(device.device_id.clone());
                }
                gpu_device_ids.sort();

                free_slot_indices.retain(|idx| !slot_index_set.contains(idx));
                free_gpu_indices.retain(|idx| !gpu_index_set.contains(idx));
                leases.push(PreparedTaskLease {
                    lease_id,
                    task_id: intent.task_id,
                    expires_at_unix_ms,
                    slot_ids,
                    gpu_device_ids,
                });
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
                let error = SchedulerError::Store(e);
                crate::observability::metrics::record_scheduler_prepare(
                    "failure",
                    crate::observability::metrics::scheduler_error_reason(&error),
                );
                return Err(error);
            }

            self.publish_digest_from_snapshot(&new_snapshot).await;
            crate::observability::metrics::record_scheduler_prepare("success", "ok");
            return Ok(PreparedTaskLeaseBatch { leases });
        }
    }

    /// Commits one prepared lease into a durable task reservation on the local node.
    pub async fn commit_task_lease(
        &self,
        lease_id: Uuid,
        coordinator_node_id: Uuid,
        task_id: Uuid,
        expected_slot_ids: &[SlotId],
        expected_gpu_device_ids: &[String],
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let now_unix_ms = current_unix_ms();

            let Some(allocation) = current.lease_index.get(&lease_id).cloned() else {
                return Err(SchedulerError::UnknownLeases {
                    lease_ids: vec![lease_id],
                    snapshot: current.snapshot.clone(),
                });
            };

            if allocation.expires_at_unix_ms <= now_unix_ms {
                return Err(SchedulerError::ExpiredLeases {
                    lease_ids: vec![lease_id],
                    snapshot: current.snapshot.clone(),
                });
            }

            if let Some(group_id) = allocation.group_id {
                return Err(SchedulerError::LeaseGroupMismatch {
                    group_id,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut expected_slots = expected_slot_ids.to_vec();
            expected_slots.sort_unstable();
            let mut expected_gpus = expected_gpu_device_ids.to_vec();
            expected_gpus.sort();

            if allocation.coordinator_node_id != coordinator_node_id
                || allocation.task_id != task_id
                || allocation.slot_ids != expected_slots
                || allocation.gpu_device_ids != expected_gpus
            {
                return Err(SchedulerError::LeaseMismatch {
                    lease_id,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            for slot_id in &allocation.slot_ids {
                let idx = current.slot_index[slot_id];
                new_snapshot.slots[idx].state = SlotState::Reserved(SlotReservation {
                    owner: self.store_key.to_uuid(),
                    task_id: Some(task_id),
                    group_id: None,
                });
            }
            for device_id in &allocation.gpu_device_ids {
                let idx = current.gpu_index[device_id];
                new_snapshot.gpu_devices[idx].state =
                    GpuDeviceState::Reserved(GpuDeviceReservation {
                        owner: self.store_key.to_uuid(),
                        task_id: Some(task_id),
                        group_id: None,
                    });
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

    /// Commits every prepared lease in a group into durable grouped reservations atomically.
    pub async fn commit_task_lease_group(
        &self,
        group_id: Uuid,
        coordinator_node_id: Uuid,
        prepared_leases: &[PreparedTaskLease],
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let now_unix_ms = current_unix_ms();

            let mut expected_by_lease = HashMap::with_capacity(prepared_leases.len());
            for lease in prepared_leases {
                if expected_by_lease.insert(lease.lease_id, lease).is_some() {
                    return Err(SchedulerError::LeaseGroupMismatch {
                        group_id,
                        snapshot: current.snapshot.clone(),
                    });
                }
            }

            let actual_lease_ids = current
                .lease_index
                .iter()
                .filter_map(|(lease_id, allocation)| {
                    (allocation.group_id == Some(group_id)).then_some(*lease_id)
                })
                .collect::<BTreeSet<_>>();

            if actual_lease_ids.is_empty() {
                return Err(SchedulerError::UnknownLeaseGroup {
                    group_id,
                    snapshot: current.snapshot.clone(),
                });
            }

            let expected_lease_ids = expected_by_lease.keys().copied().collect::<BTreeSet<_>>();
            if actual_lease_ids != expected_lease_ids {
                return Err(SchedulerError::LeaseGroupMismatch {
                    group_id,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut expired = Vec::new();
            for lease_id in &actual_lease_ids {
                let allocation = &current.lease_index[lease_id];
                if allocation.coordinator_node_id != coordinator_node_id {
                    return Err(SchedulerError::LeaseGroupMismatch {
                        group_id,
                        snapshot: current.snapshot.clone(),
                    });
                }
                if allocation.expires_at_unix_ms <= now_unix_ms {
                    expired.push(*lease_id);
                }
            }
            if !expired.is_empty() {
                return Err(SchedulerError::ExpiredLeases {
                    lease_ids: expired,
                    snapshot: current.snapshot.clone(),
                });
            }

            for lease_id in &actual_lease_ids {
                let allocation = &current.lease_index[lease_id];
                let expected = expected_by_lease[lease_id];
                let mut expected_slots = expected.slot_ids.clone();
                expected_slots.sort_unstable();
                let mut expected_gpus = expected.gpu_device_ids.clone();
                expected_gpus.sort();

                if allocation.task_id != expected.task_id
                    || allocation.slot_ids != expected_slots
                    || allocation.gpu_device_ids != expected_gpus
                {
                    return Err(SchedulerError::LeaseMismatch {
                        lease_id: *lease_id,
                        snapshot: current.snapshot.clone(),
                    });
                }
            }

            let mut new_snapshot = current.snapshot.clone();
            for lease_id in &actual_lease_ids {
                let allocation = &current.lease_index[lease_id];
                for slot_id in &allocation.slot_ids {
                    let idx = current.slot_index[slot_id];
                    new_snapshot.slots[idx].state = SlotState::Reserved(SlotReservation {
                        owner: self.store_key.to_uuid(),
                        task_id: Some(allocation.task_id),
                        group_id: Some(group_id),
                    });
                }
                for device_id in &allocation.gpu_device_ids {
                    let idx = current.gpu_index[device_id];
                    new_snapshot.gpu_devices[idx].state =
                        GpuDeviceState::Reserved(GpuDeviceReservation {
                            owner: self.store_key.to_uuid(),
                            task_id: Some(allocation.task_id),
                            group_id: Some(group_id),
                        });
                }
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

    /// Aborts prepared leases, releasing any still-leased capacity for the provided coordinator.
    pub async fn abort_task_leases(
        &self,
        coordinator_node_id: Uuid,
        intents: Vec<AbortTaskLeaseIntent>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if intents.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let mut new_snapshot = current.snapshot.clone();
            let mut changed = false;

            for intent in &intents {
                let Some(allocation) = current.lease_index.get(&intent.lease_id) else {
                    continue;
                };
                if allocation.coordinator_node_id != coordinator_node_id
                    || allocation.task_id != intent.task_id
                {
                    continue;
                }

                for slot_id in &allocation.slot_ids {
                    let idx = current.slot_index[slot_id];
                    if matches!(
                        new_snapshot.slots[idx].state,
                        SlotState::Leased(LeaseReservation { lease_id, .. }) if lease_id == intent.lease_id
                    ) {
                        new_snapshot.slots[idx].state = SlotState::Free;
                        changed = true;
                    }
                }
                for device_id in &allocation.gpu_device_ids {
                    let idx = current.gpu_index[device_id];
                    if matches!(
                        new_snapshot.gpu_devices[idx].state,
                        GpuDeviceState::Leased(LeaseReservation { lease_id, .. }) if lease_id == intent.lease_id
                    ) {
                        new_snapshot.gpu_devices[idx].state = GpuDeviceState::Free;
                        changed = true;
                    }
                }
            }

            if !changed {
                return Ok(current.snapshot.clone());
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

    /// Aborts every prepared lease or unpublished committed reservation in one group.
    pub async fn abort_task_lease_group(
        &self,
        coordinator_node_id: Uuid,
        group_id: Uuid,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let mut new_snapshot = current.snapshot.clone();
            let mut changed = false;

            let lease_ids = current
                .lease_index
                .iter()
                .filter_map(|(lease_id, allocation)| {
                    (allocation.group_id == Some(group_id)
                        && allocation.coordinator_node_id == coordinator_node_id)
                        .then_some(*lease_id)
                })
                .collect::<Vec<_>>();

            for lease_id in &lease_ids {
                let allocation = &current.lease_index[lease_id];
                for slot_id in &allocation.slot_ids {
                    let idx = current.slot_index[slot_id];
                    if matches!(
                        new_snapshot.slots[idx].state,
                        SlotState::Leased(LeaseReservation { lease_id: current_lease_id, .. })
                            if current_lease_id == *lease_id
                    ) {
                        new_snapshot.slots[idx].state = SlotState::Free;
                        changed = true;
                    }
                }
                for device_id in &allocation.gpu_device_ids {
                    let idx = current.gpu_index[device_id];
                    if matches!(
                        new_snapshot.gpu_devices[idx].state,
                        GpuDeviceState::Leased(LeaseReservation {
                            lease_id: current_lease_id,
                            ..
                        }) if current_lease_id == *lease_id
                    ) {
                        new_snapshot.gpu_devices[idx].state = GpuDeviceState::Free;
                        changed = true;
                    }
                }
            }

            let scheduler_owner = self.store_key.to_uuid();
            for slot in &mut new_snapshot.slots {
                if matches!(
                    &slot.state,
                    SlotState::Reserved(SlotReservation {
                        owner,
                        group_id: Some(reservation_group_id),
                        ..
                    }) if *owner == scheduler_owner && *reservation_group_id == group_id
                ) {
                    slot.state = SlotState::Free;
                    changed = true;
                }
            }
            for device in &mut new_snapshot.gpu_devices {
                if matches!(
                    &device.state,
                    GpuDeviceState::Reserved(GpuDeviceReservation {
                        owner,
                        group_id: Some(reservation_group_id),
                        ..
                    }) if *owner == scheduler_owner && *reservation_group_id == group_id
                ) {
                    device.state = GpuDeviceState::Free;
                    changed = true;
                }
            }

            if !changed {
                return Ok(current.snapshot.clone());
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

    /// Reclaims expired prepared leases so leaked scheduler capacity becomes visible again.
    pub async fn reap_expired_leases(&self) -> Result<Vec<Uuid>, SchedulerError> {
        loop {
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();
            let mut new_snapshot = current.snapshot.clone();
            let expired = Self::clear_expired_leases(&mut new_snapshot, current_unix_ms());

            if expired.is_empty() {
                return Ok(Vec::new());
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
            crate::observability::metrics::record_scheduler_expired_leases_reaped(expired.len());
            return Ok(expired);
        }
    }
}
