use std::rc::Rc;

use protocol::scheduling::scheduler;
use uuid::Uuid;

use super::summary::SchedulerSummary;
use super::{AbortTaskLeaseIntent, Scheduler, TaskLeaseIntent};

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

    async fn prepare_leases(
        self: Rc<Self>,
        params: scheduler::PrepareLeasesParams,
        mut results: scheduler::PrepareLeasesResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let coordinator_node_id = Self::parse_uuid(request.get_coordinator_node_id()?)?;
        let expected_version = request.get_expected_version();
        let ttl_ms = request.get_ttl_ms();
        let intents = request.get_intents()?;

        let mut reservations = Vec::with_capacity(intents.len() as usize);
        for intent in intents.iter() {
            let task_id = Self::parse_uuid(intent.get_task_id()?)?;
            reservations.push(TaskLeaseIntent {
                task_id,
                cpu_millis: intent.get_cpu_millis(),
                memory_bytes: intent.get_memory_bytes(),
                gpu_count: intent.get_gpu_count(),
            });
        }

        let prepared = self
            .scheduler
            .prepare_task_leases(coordinator_node_id, expected_version, ttl_ms, reservations)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        let mut response = results.get().init_response();
        response.set_new_version(prepared.new_version);
        let mut leases = response
            .reborrow()
            .init_leases(prepared.leases.len() as u32);
        for (idx, lease) in prepared.leases.iter().enumerate() {
            let mut entry = leases.reborrow().get(idx as u32);
            entry.set_lease_id(lease.lease_id.as_bytes());
            entry.set_task_id(lease.task_id.as_bytes());
            entry.set_expires_at_unix_ms(lease.expires_at_unix_ms);
            let mut slot_ids = entry.reborrow().init_slot_ids(lease.slot_ids.len() as u32);
            for (slot_idx, slot_id) in lease.slot_ids.iter().enumerate() {
                slot_ids.set(slot_idx as u32, *slot_id);
            }

            let mut gpu_ids = entry
                .reborrow()
                .init_gpu_device_ids(lease.gpu_device_ids.len() as u32);
            for (gpu_idx, device_id) in lease.gpu_device_ids.iter().enumerate() {
                gpu_ids.set(gpu_idx as u32, device_id);
            }
        }

        Ok(())
    }

    async fn abort_leases(
        self: Rc<Self>,
        params: scheduler::AbortLeasesParams,
        _results: scheduler::AbortLeasesResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let coordinator_node_id = Self::parse_uuid(request.get_coordinator_node_id()?)?;
        let intents = request.get_intents()?;

        let mut aborts = Vec::with_capacity(intents.len() as usize);
        for intent in intents.iter() {
            aborts.push(AbortTaskLeaseIntent {
                lease_id: Self::parse_uuid(intent.get_lease_id()?)?,
                task_id: Self::parse_uuid(intent.get_task_id()?)?,
            });
        }

        self.scheduler
            .abort_task_leases(coordinator_node_id, aborts)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        Ok(())
    }
}
