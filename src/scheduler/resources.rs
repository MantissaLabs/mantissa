use std::collections::HashSet;
use std::sync::Arc;

use tracing::warn;

use crate::gpu::{GpuDeviceOverrideAction, gpu_device_override_for, read_gpu_device_overrides};

use super::state::{SchedulerState, ptr_eq_option};
use super::{
    GpuDevice, GpuDeviceSpec, GpuDeviceState, ResourceSlot, Scheduler, SchedulerError,
    SchedulerSnapshot, SlotCapacity, SlotSpec, SlotState,
};

impl Scheduler {
    /// Initializes slot-only schedulers (legacy path) by delegating to `init_resources`.
    #[allow(dead_code)]
    pub async fn init_slots<I>(&self, slots: I) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotSpec>,
    {
        self.init_resources(slots, Vec::new()).await
    }

    /// Initializes scheduler resources (slots and GPU devices) from the provided specs.
    pub async fn init_resources<I, G>(
        &self,
        slots: I,
        gpu_devices: G,
    ) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotSpec>,
        G: IntoIterator<Item = GpuDeviceSpec>,
    {
        let current = self.state.load_full();
        if let Some(current) = current.as_ref() {
            return Err(SchedulerError::AlreadyInitialized {
                snapshot: current.snapshot.clone(),
            });
        }

        let mut specs: Vec<SlotSpec> = slots.into_iter().collect();
        specs.sort_by_key(|spec| spec.slot_id);
        specs.dedup_by(|a, b| a.slot_id == b.slot_id);

        let slots: Vec<ResourceSlot> = specs
            .into_iter()
            .map(|spec| ResourceSlot {
                slot_id: spec.slot_id,
                capacity: spec.capacity,
                state: SlotState::Free,
            })
            .collect();

        let gpu_devices = Self::materialize_gpu_devices(gpu_devices);

        let snapshot = SchedulerSnapshot {
            version: 0,
            slots,
            gpu_devices,
        };

        let state_arc = Arc::new(SchedulerState::new(snapshot.clone()));

        let prev = self
            .state
            .compare_and_swap(&None::<Arc<SchedulerState>>, Some(state_arc.clone()));

        if let Some(existing) = prev.as_ref() {
            // Another thread won the race to initialise the scheduler; reuse its snapshot.
            return Err(SchedulerError::AlreadyInitialized {
                snapshot: existing.snapshot.clone(),
            });
        }

        if let Err(e) = self.store.upsert(&self.store_key, snapshot.clone()).await {
            let _ = self.state.compare_and_swap(&Some(state_arc.clone()), None);
            return Err(SchedulerError::Store(e));
        }

        self.publish_digest_from_snapshot(&snapshot).await;
        Ok(snapshot)
    }

    /// Normalizes GPU specs into snapshot entries with stable ordering.
    fn materialize_gpu_devices<I>(gpu_devices: I) -> Vec<GpuDevice>
    where
        I: IntoIterator<Item = GpuDeviceSpec>,
    {
        let mut gpu_specs: Vec<GpuDeviceSpec> = gpu_devices.into_iter().collect();
        gpu_specs.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        gpu_specs.dedup_by(|a, b| a.device_id == b.device_id);

        gpu_specs
            .into_iter()
            .map(|spec| GpuDevice {
                device_id: spec.device_id,
                index: spec.index,
                uuid: spec.uuid,
                pci_bus_id: spec.pci_bus_id,
                name: spec.name,
                memory_total_bytes: spec.memory_total_bytes,
                state: GpuDeviceState::Free,
            })
            .collect()
    }

    /// Derives the initial slot specifications from allocatable node resources after reserve.
    ///
    /// Bootstrap subtracts a small per-node CPU and memory reserve before materializing slots so
    /// control-plane and system work retain headroom even when user workloads fill the scheduler.
    pub fn derive_slot_specs(
        node: &crate::node::Node,
        runtime_config: crate::config::RuntimeSchedulerConfig,
    ) -> Vec<SlotSpec> {
        let info = &node.system_info.info;

        let logical_cpus = info
            .cpu_info
            .as_ref()
            .map(|cpu| cpu.num_logical_cpus.max(1) as u64)
            .unwrap_or(1);

        let total_memory = info.mem_info.as_ref().map(|mem| mem.total).unwrap_or(0);
        let total_cpu_millis = logical_cpus.saturating_mul(1_000);
        let allocatable_cpu_millis =
            total_cpu_millis.saturating_sub(runtime_config.reserved_cpu_millis);
        let allocatable_memory = total_memory.saturating_sub(runtime_config.reserved_memory_bytes);
        let target_slot_cpu_millis = runtime_config.target_slot_cpu_millis.max(1);
        let target_slot_memory_bytes = runtime_config.target_slot_memory_bytes.max(1);
        let max_slots = runtime_config
            .max_slots
            .clamp(1, crate::config::SCHEDULER_MAX_SLOT_COUNT);

        let cpu_slot_count = if allocatable_cpu_millis > 0 {
            allocatable_cpu_millis
                .div_ceil(target_slot_cpu_millis)
                .max(1)
        } else {
            0
        };
        let memory_slot_count = if allocatable_memory > 0 {
            allocatable_memory.div_ceil(target_slot_memory_bytes).max(1)
        } else {
            0
        };

        let mut slot_count = cpu_slot_count.max(memory_slot_count);
        if slot_count == 0 {
            return Vec::new();
        }

        slot_count = slot_count.min(max_slots);
        if allocatable_cpu_millis > 0 {
            slot_count = slot_count.min(allocatable_cpu_millis);
        }
        if allocatable_memory > 0 {
            slot_count = slot_count.min(allocatable_memory);
        }
        if slot_count == 0 {
            return Vec::new();
        }

        let mut specs = Vec::with_capacity(slot_count as usize);
        for slot_idx in 0..slot_count {
            specs.push(SlotSpec::new(
                slot_idx,
                SlotCapacity::new(
                    Self::split_even_capacity(allocatable_cpu_millis, slot_idx, slot_count),
                    Self::split_even_capacity(allocatable_memory, slot_idx, slot_count),
                    0,
                ),
            ));
        }

        specs
    }

    /// Splits one scalar resource into nearly equal per-slot chunks while preserving the total.
    fn split_even_capacity(total: u64, slot_idx: u64, slot_count: u64) -> u64 {
        if total == 0 || slot_count == 0 {
            return 0;
        }

        let base = total / slot_count;
        let remainder = total % slot_count;
        if slot_idx < remainder {
            base.saturating_add(1)
        } else {
            base
        }
    }

    /// Derives GPU device specs from node inventory so GPUs can be reserved independently of slots.
    pub fn derive_gpu_specs(node: &crate::node::Node) -> Vec<GpuDeviceSpec> {
        let info = &node.system_info.info;
        let Some(gpu_info) = info.gpu_info.as_ref() else {
            return Vec::new();
        };

        let overrides = read_gpu_device_overrides();
        let mut specs: Vec<GpuDeviceSpec> = Vec::new();
        let mut seen_device_ids = HashSet::new();
        for device in &gpu_info.devices {
            let uuid = device
                .uuid
                .as_ref()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            let mut override_device_id: Option<String> = None;
            if let Some(entry) = gpu_device_override_for(
                uuid.as_deref(),
                device.pci_bus_id.as_deref(),
                device.index,
                &overrides,
            ) {
                match &entry.action {
                    GpuDeviceOverrideAction::Disable => {
                        continue;
                    }
                    GpuDeviceOverrideAction::OverrideId(id) => {
                        if id.trim().is_empty() {
                            warn!(
                                target: "scheduler",
                                "gpu override for device index {} uses an empty id; ignoring device",
                                device.index
                            );
                            continue;
                        }
                        override_device_id = Some(id.clone());
                    }
                }
            }

            let device_id = match override_device_id.or_else(|| uuid.clone()) {
                Some(id) => id,
                None => {
                    // Skip devices without a UUID unless an override supplied an ID.
                    continue;
                }
            };

            if !seen_device_ids.insert(device_id.clone()) {
                warn!(
                    target: "scheduler",
                    "duplicate gpu device id '{device_id}' detected; skipping device index {}",
                    device.index
                );
                continue;
            }
            specs.push(GpuDeviceSpec::new(
                device_id,
                device.index,
                uuid,
                device.pci_bus_id.clone(),
                device.name.clone(),
                device.memory_total_bytes,
            ));
        }

        specs.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        specs
    }

    /// Initializes the scheduler for the provided node using allocatable capacity after reserve.
    ///
    /// Returning the active snapshot either way keeps bootstrap callers on one consistent view
    /// whether initialization happened in this process or a previous one already persisted it.
    pub async fn initialize_with_node(
        &self,
        node: &crate::node::Node,
        runtime_config: crate::config::RuntimeSchedulerConfig,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if let Some(snapshot) = self.snapshot().await {
            if snapshot.gpu_devices.is_empty() {
                let gpu_specs = Self::derive_gpu_specs(node);
                if !gpu_specs.is_empty()
                    && let Ok(updated) =
                        self.populate_gpu_devices(snapshot.version, gpu_specs).await
                {
                    return Ok(updated);
                }
            }
            self.publish_digest_from_snapshot(&snapshot).await;
            return Ok(snapshot);
        }

        match self
            .init_resources(
                Self::derive_slot_specs(node, runtime_config),
                Self::derive_gpu_specs(node),
            )
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(SchedulerError::AlreadyInitialized { snapshot }) => {
                self.publish_digest_from_snapshot(&snapshot).await;
                Ok(snapshot)
            }
            Err(err) => Err(err),
        }
    }

    /// Populates GPU devices in the snapshot when upgrading from a slot-only scheduler.
    async fn populate_gpu_devices(
        &self,
        expected_version: u64,
        gpu_devices: Vec<GpuDeviceSpec>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if gpu_devices.is_empty() {
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

            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !current.snapshot.gpu_devices.is_empty() {
                return Ok(current.snapshot.clone());
            }

            let mut new_snapshot = current.snapshot.clone();
            new_snapshot.gpu_devices = Self::materialize_gpu_devices(gpu_devices.clone());
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
