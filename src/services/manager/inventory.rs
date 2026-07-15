use super::*;
use crate::services::types::{compute_service_id, derive_service_replica_id};
use crate::workload::model::WorkloadServiceMetadata;

/// Derives the compact inventory key shared by metadata and service-slot lookups.
fn service_replica_inventory_key(metadata: &WorkloadServiceMetadata) -> Uuid {
    derive_service_replica_id(
        compute_service_id(&metadata.service_name),
        metadata.service_epoch,
        &metadata.template,
        metadata.replica,
    )
}

#[derive(Clone, Debug)]
pub(super) struct TaskInventory {
    pub(super) by_id: HashMap<Uuid, WorkloadSpec>,
    pub(super) by_service: HashMap<String, Vec<Uuid>>,
    by_service_slot: HashMap<Uuid, Vec<Uuid>>,
}

impl TaskInventory {
    /// Builds a task inventory snapshot for service-level reconciliation checks.
    pub(super) fn from_specs(specs: Vec<WorkloadSpec>) -> Self {
        let mut by_id = HashMap::with_capacity(specs.len());
        let mut by_service: HashMap<String, Vec<Uuid>> = HashMap::new();
        let mut by_service_slot: HashMap<Uuid, Vec<Uuid>> = HashMap::new();

        for spec in specs {
            let task_id = spec.id;
            if let Some(meta) = spec.service_owner() {
                by_service
                    .entry(meta.service_name.clone())
                    .or_default()
                    .push(task_id);
                by_service_slot
                    .entry(service_replica_inventory_key(meta))
                    .or_default()
                    .push(task_id);
            }
            by_id.insert(task_id, spec);
        }

        for task_ids in by_service_slot.values_mut() {
            task_ids.sort_unstable();
        }

        Self {
            by_id,
            by_service,
            by_service_slot,
        }
    }

    /// Iterates tasks observed for one exact service replica slot and generation.
    pub(super) fn service_slot_tasks(
        &self,
        service_name: &str,
        service_epoch: u64,
        template: &str,
        replica: u16,
    ) -> impl Iterator<Item = &WorkloadSpec> {
        let key = derive_service_replica_id(
            compute_service_id(service_name),
            service_epoch,
            template,
            replica,
        );
        self.by_service_slot
            .get(&key)
            .into_iter()
            .flat_map(|task_ids| task_ids.iter())
            .filter_map(|task_id| self.by_id.get(task_id))
    }

    /// Builds a reusable, service-scoped task view combining desired and observed task ids.
    pub(super) fn service_task_snapshot<'a>(
        &'a self,
        service_name: &'a str,
        desired_ids: HashSet<Uuid>,
    ) -> ServiceReplicaSnapshot<'a> {
        ServiceReplicaSnapshot {
            inventory: self,
            service_name,
            desired_ids,
        }
    }
}

/// Lightweight service-scoped task view used by reconcile and stop paths.
pub(super) struct ServiceReplicaSnapshot<'a> {
    inventory: &'a TaskInventory,
    service_name: &'a str,
    desired_ids: HashSet<Uuid>,
}

impl ServiceReplicaSnapshot<'_> {
    /// Returns true when the task id is still assigned to a desired service replica slot.
    pub(super) fn is_desired(&self, task_id: Uuid) -> bool {
        self.desired_ids.contains(&task_id)
    }

    /// Iterates all currently observed tasks that advertise this service metadata.
    pub(super) fn observed_tasks(&self) -> impl Iterator<Item = &WorkloadSpec> {
        self.inventory
            .by_service
            .get(self.service_name)
            .into_iter()
            .flat_map(|task_ids| task_ids.iter())
            .filter_map(|task_id| self.inventory.by_id.get(task_id))
    }

    /// Returns the union of desired and observed task ids used for stop/drain workflows.
    pub(super) fn all_known_task_ids(&self) -> HashSet<Uuid> {
        let mut task_ids = self.desired_ids.clone();
        if let Some(observed) = self.inventory.by_service.get(self.service_name) {
            task_ids.extend(observed.iter().copied());
        }
        task_ids
    }
}
