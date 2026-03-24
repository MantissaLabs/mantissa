use std::rc::Rc;

use protocol::scheduling::scheduler;
use uuid::Uuid;

use super::summary::SchedulerSummary;
use super::{Scheduler, TaskResourceReservationIntent};

pub struct SchedulerService {
    scheduler: Rc<Scheduler>,
    node_id: Uuid,
    node_name: String,
}

impl SchedulerService {
    pub fn new(scheduler: Rc<Scheduler>, node_id: Uuid, node_name: String) -> Self {
        Self {
            scheduler,
            node_id,
            node_name,
        }
    }

    fn parse_uuid(bytes: capnp::data::Reader<'_>) -> Result<Uuid, capnp::Error> {
        if bytes.len() != 16 {
            return Err(capnp::Error::failed("UUID fields must be 16 bytes".into()));
        }

        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Ok(Uuid::from_bytes(arr))
    }
}

impl scheduler::Server for SchedulerService {
    async fn summary(
        self: Rc<Self>,
        params: scheduler::SummaryParams,
        mut results: scheduler::SummaryResults,
    ) -> Result<(), capnp::Error> {
        let node_id = self.node_id;

        let req = params.get()?;
        let inner = req.get_request()?;
        let include_details = inner.get_include_details();
        let peer_bytes = inner.get_peer_id()?;

        let target_peer = if peer_bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(peer_bytes);
            let peer = Uuid::from_bytes(arr);
            if peer == node_id { None } else { Some(peer) }
        } else {
            None
        };

        let summary = if let Some(peer_id) = target_peer {
            self.scheduler
                .fetch_remote_summary(peer_id, include_details)
                .await?
        } else {
            let snapshot = self.scheduler.snapshot().await;
            SchedulerSummary::from_snapshot(
                node_id,
                &self.node_name,
                snapshot.as_ref(),
                include_details,
            )
        };

        let mut builder = results.get().init_summary();
        summary.write_to_builder(&mut builder)?;
        Ok(())
    }

    async fn reserve_resources(
        self: Rc<Self>,
        params: scheduler::ReserveResourcesParams,
        mut results: scheduler::ReserveResourcesResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let expected_version = request.get_expected_version();
        let intents = request.get_intents()?;

        let mut reservations = Vec::with_capacity(intents.len() as usize);
        for intent in intents.iter() {
            let task_id = Self::parse_uuid(intent.get_task_id()?)?;
            reservations.push(TaskResourceReservationIntent {
                task_id,
                cpu_millis: intent.get_cpu_millis(),
                memory_bytes: intent.get_memory_bytes(),
                gpu_count: intent.get_gpu_count(),
            });
        }

        let prepared = self
            .scheduler
            .reserve_task_resources(expected_version, reservations)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        let mut response = results.get().init_response();
        response.set_new_version(prepared.new_version);
        let mut bindings = response
            .reborrow()
            .init_bindings(prepared.bindings.len() as u32);
        for (idx, binding) in prepared.bindings.iter().enumerate() {
            let mut entry = bindings.reborrow().get(idx as u32);
            entry.set_task_id(binding.task_id.as_bytes());
            let mut slot_ids = entry
                .reborrow()
                .init_slot_ids(binding.slot_ids.len() as u32);
            for (slot_idx, slot_id) in binding.slot_ids.iter().enumerate() {
                slot_ids.set(slot_idx as u32, *slot_id);
            }

            let mut gpu_ids = entry
                .reborrow()
                .init_gpu_device_ids(binding.gpu_device_ids.len() as u32);
            for (gpu_idx, device_id) in binding.gpu_device_ids.iter().enumerate() {
                gpu_ids.set(gpu_idx as u32, device_id);
            }
        }

        Ok(())
    }

    async fn release_slots(
        self: Rc<Self>,
        params: scheduler::ReleaseSlotsParams,
        mut results: scheduler::ReleaseSlotsResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let expected_version = request.get_expected_version();

        let ids_reader = request.get_slot_ids()?;
        let mut slot_ids = Vec::with_capacity(ids_reader.len() as usize);
        for slot_id in ids_reader.iter() {
            slot_ids.push(slot_id);
        }

        let gpu_reader = request.get_gpu_device_ids()?;
        let mut gpu_device_ids = Vec::with_capacity(gpu_reader.len() as usize);
        for device_id in gpu_reader.iter() {
            gpu_device_ids.push(device_id?.to_str()?.to_string());
        }

        let snapshot = self
            .scheduler
            .free_resources(expected_version, slot_ids, gpu_device_ids)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        results
            .get()
            .init_response()
            .set_new_version(snapshot.version);

        Ok(())
    }
}
