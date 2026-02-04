use std::rc::Rc;

use protocol::scheduling::scheduler;
use uuid::Uuid;

use super::summary::SchedulerSummary;
use super::{GpuReservationRequest, Scheduler, SlotReservationRequest};

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

    async fn reserve_slots(
        self: Rc<Self>,
        params: scheduler::ReserveSlotsParams,
        mut results: scheduler::ReserveSlotsResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let expected_version = request.get_expected_version();
        let intents = request.get_intents()?;
        let gpu_intents = request.get_gpu_intents()?;

        let mut reservations = Vec::with_capacity(intents.len() as usize);
        for intent in intents.iter() {
            let slot_id = intent.get_slot_id();
            let owner = Self::parse_uuid(intent.get_owner()?)?;

            let task_id = {
                let bytes = intent.get_task_id()?;
                if bytes.len() == 16 {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                } else {
                    None
                }
            };

            reservations.push(SlotReservationRequest {
                slot_id,
                owner,
                task_id,
            });
        }

        let mut gpu_reservations = Vec::with_capacity(gpu_intents.len() as usize);
        for intent in gpu_intents.iter() {
            let device_id = intent.get_device_id()?.to_str()?.to_string();
            let owner = Self::parse_uuid(intent.get_owner()?)?;

            let task_id = {
                let bytes = intent.get_task_id()?;
                if bytes.len() == 16 {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(bytes);
                    Some(Uuid::from_bytes(arr))
                } else {
                    None
                }
            };

            gpu_reservations.push(GpuReservationRequest {
                device_id,
                owner,
                task_id,
            });
        }

        let snapshot = self
            .scheduler
            .reserve_resources(expected_version, reservations, gpu_reservations)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        results
            .get()
            .init_response()
            .set_new_version(snapshot.version);

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
