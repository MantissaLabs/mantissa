use std::rc::Rc;

use capnp::capability::Promise;
use protocol::scheduling::scheduler;
use uuid::Uuid;

use super::summary::SchedulerSummary;
use super::{Scheduler, SlotReservationRequest};

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
    fn summary(
        &mut self,
        params: scheduler::SummaryParams,
        mut results: scheduler::SummaryResults,
    ) -> Promise<(), capnp::Error> {
        let scheduler = self.scheduler.clone();
        let node_id = self.node_id;
        let node_name = self.node_name.clone();

        Promise::from_future(async move {
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
                scheduler
                    .fetch_remote_summary(peer_id, include_details)
                    .await?
            } else {
                let snapshot = scheduler.snapshot().await;
                SchedulerSummary::from_snapshot(
                    node_id,
                    &node_name,
                    snapshot.as_ref(),
                    include_details,
                )
            };

            let mut builder = results.get().init_summary();
            summary.write_to_builder(&mut builder)?;
            Ok(())
        })
    }

    fn reserve_slots(
        &mut self,
        params: scheduler::ReserveSlotsParams,
        mut results: scheduler::ReserveSlotsResults,
    ) -> Promise<(), capnp::Error> {
        let scheduler = self.scheduler.clone();

        Promise::from_future(async move {
            let request = params.get()?.get_request()?;
            let expected_version = request.get_expected_version();
            let intents = request.get_intents()?;

            let mut reservations = Vec::with_capacity(intents.len() as usize);
            for intent in intents.iter() {
                let slot_id = intent.get_slot_id();
                let owner = Self::parse_uuid(intent.get_owner()?)?;

                let workload_id = {
                    let bytes = intent.get_workload_id()?;
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
                    workload_id,
                });
            }

            let snapshot = scheduler
                .reserve_slots(expected_version, reservations)
                .await
                .map_err(|err| capnp::Error::failed(err.to_string()))?;

            results
                .get()
                .init_response()
                .set_new_version(snapshot.version);

            Ok(())
        })
    }
}
