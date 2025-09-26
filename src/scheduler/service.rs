use std::rc::Rc;

use capnp::capability::Promise;
use protocol::scheduling::scheduler;
use uuid::Uuid;

use super::Scheduler;
use super::summary::SchedulerSummary;

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
}
