use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use mantissa_protocol::scheduling::{self, scheduler};
use uuid::Uuid;

use super::digest::{SchedulerDigestValue, write_scheduler_digest};
use super::summary::SchedulerSummary;
use super::{
    AbortTaskLeaseIntent, PreparedTaskLease, PreparedTaskLeaseBatch, Scheduler, SchedulerError,
    TaskLeaseIntent,
};

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

    /// Builds one zero-capacity digest for rejections emitted before the scheduler is initialized.
    fn empty_prepare_rejection_digest(&self) -> SchedulerDigestValue {
        SchedulerDigestValue {
            node_id: self.node_id,
            snapshot_version: 0,
            updated_at_unix_ms: current_unix_ms(),
            free_slot_count: 0,
            free_cpu_millis: 0,
            free_memory_bytes: 0,
            largest_free_slot_cpu_millis: 0,
            largest_free_slot_memory_bytes: 0,
            free_gpu_count: 0,
            gpu_runtime_ready: false,
        }
    }

    /// Maps one scheduler-side prepare failure into a structured wire rejection when possible.
    fn prepare_rejection_from_error(
        &self,
        err: SchedulerError,
    ) -> Result<
        (
            scheduling::PrepareLeasesRejectionReason,
            SchedulerDigestValue,
        ),
        SchedulerError,
    > {
        match err {
            SchedulerError::InsufficientResources { snapshot, .. } => Ok((
                scheduling::PrepareLeasesRejectionReason::InsufficientResources,
                SchedulerDigestValue::from_snapshot(self.node_id, &snapshot),
            )),
            SchedulerError::Uninitialized => Ok((
                scheduling::PrepareLeasesRejectionReason::Uninitialized,
                self.empty_prepare_rejection_digest(),
            )),
            other => Err(other),
        }
    }

    /// Writes one successful prepared-lease batch into the wire response payload.
    fn write_prepared_leases(
        prepared: &PreparedTaskLeaseBatch,
        mut response: scheduling::prepare_leases_response::Builder<'_>,
    ) {
        let mut leases = response
            .reborrow()
            .init_prepared(prepared.leases.len() as u32);
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
    }

    /// Writes one structured prepare rejection so callers can refresh local digest state immediately.
    fn write_prepare_rejection(
        reason: scheduling::PrepareLeasesRejectionReason,
        digest: &SchedulerDigestValue,
        mut response: scheduling::prepare_leases_response::Builder<'_>,
    ) {
        let mut rejected = response.reborrow().init_rejected();
        rejected.set_reason(reason);
        write_scheduler_digest(rejected.reborrow().init_current_digest(), digest);
    }

    /// Decodes one list of prepared leases supplied by a remote group commit request.
    fn read_prepared_leases(
        leases: scheduling::prepared_lease::Reader<'_>,
    ) -> Result<PreparedTaskLease, capnp::Error> {
        let lease_id = Self::parse_uuid(leases.get_lease_id()?)?;
        let task_id = Self::parse_uuid(leases.get_task_id()?)?;
        let slot_ids = leases.get_slot_ids()?.iter().collect::<Vec<_>>();
        let devices = leases.get_gpu_device_ids()?;
        let mut gpu_device_ids = Vec::with_capacity(devices.len() as usize);
        for device in devices.iter() {
            gpu_device_ids.push(device?.to_str()?.to_string());
        }

        Ok(PreparedTaskLease {
            lease_id,
            task_id,
            expires_at_unix_ms: leases.get_expires_at_unix_ms(),
            slot_ids,
            gpu_device_ids,
        })
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

        let mut response = results.get().init_response();
        match self
            .scheduler
            .prepare_task_leases(coordinator_node_id, ttl_ms, reservations)
            .await
        {
            Ok(prepared) => Self::write_prepared_leases(&prepared, response.reborrow()),
            Err(err) => match self.prepare_rejection_from_error(err) {
                Ok((reason, digest)) => {
                    Self::write_prepare_rejection(reason, &digest, response.reborrow());
                }
                Err(err) => return Err(capnp::Error::failed(err.to_string())),
            },
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

    async fn prepare_lease_group(
        self: Rc<Self>,
        params: scheduler::PrepareLeaseGroupParams,
        mut results: scheduler::PrepareLeaseGroupResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let group_id = Self::parse_uuid(request.get_group_id()?)?;
        let coordinator_node_id = Self::parse_uuid(request.get_coordinator_node_id()?)?;
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

        let mut response = results.get().init_response();
        match self
            .scheduler
            .prepare_task_lease_group(coordinator_node_id, group_id, ttl_ms, reservations)
            .await
        {
            Ok(prepared) => Self::write_prepared_leases(&prepared, response.reborrow()),
            Err(err) => match self.prepare_rejection_from_error(err) {
                Ok((reason, digest)) => {
                    Self::write_prepare_rejection(reason, &digest, response.reborrow());
                }
                Err(err) => return Err(capnp::Error::failed(err.to_string())),
            },
        }

        Ok(())
    }

    async fn commit_lease_group(
        self: Rc<Self>,
        params: scheduler::CommitLeaseGroupParams,
        _results: scheduler::CommitLeaseGroupResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let group_id = Self::parse_uuid(request.get_group_id()?)?;
        let coordinator_node_id = Self::parse_uuid(request.get_coordinator_node_id()?)?;
        let prepared_reader = request.get_prepared()?;
        let mut prepared = Vec::with_capacity(prepared_reader.len() as usize);
        for lease in prepared_reader.iter() {
            prepared.push(Self::read_prepared_leases(lease)?);
        }

        self.scheduler
            .commit_task_lease_group(group_id, coordinator_node_id, &prepared)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        Ok(())
    }

    async fn abort_lease_group(
        self: Rc<Self>,
        params: scheduler::AbortLeaseGroupParams,
        _results: scheduler::AbortLeaseGroupResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let group_id = Self::parse_uuid(request.get_group_id()?)?;
        let coordinator_node_id = Self::parse_uuid(request.get_coordinator_node_id()?)?;

        self.scheduler
            .abort_task_lease_group(coordinator_node_id, group_id)
            .await
            .map_err(|err| capnp::Error::failed(err.to_string()))?;

        Ok(())
    }
}

/// Returns the current Unix timestamp in milliseconds for rejection digest timestamps.
fn current_unix_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::SchedulerService;
    use crate::registry::Registry;
    use crate::scheduler::digest::read_scheduler_digest;
    use crate::scheduler::{Scheduler, SlotCapacity, SlotSpec, SlotState};
    use crate::store::local::LocalSessionStore;
    use crate::store::replicated::peers::open_peers_store;
    use crate::store::replicated::scheduler::open_scheduler_store;
    use ::mantissa_health::HealthMonitor;
    use capnp_rpc::new_client as capnp_new_client;
    use ed25519_dalek::SigningKey;
    use mantissa_net::noise::NoiseKeys;
    use mantissa_protocol::scheduling::{self, scheduler};
    use std::rc::Rc;
    use std::sync::Arc;
    use tempfile::{TempDir, tempdir};
    use uuid::Uuid;

    const TEST_PREPARED_LEASE_TTL_MS: u64 = 30_000;

    /// Builds one isolated scheduler and local RPC client for scheduler service tests.
    async fn make_scheduler_client() -> (scheduler::Client, Rc<Scheduler>, Uuid, TempDir) {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("scheduler-service-test-{}.redb", Uuid::new_v4()));
        let db = Arc::new(redb::Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();

        let scheduler_store = open_scheduler_store(db.clone(), actor).expect("open store");
        scheduler_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild scheduler store");

        let peers_store = open_peers_store(db.clone(), actor).expect("open peers store");
        peers_store
            .rebuild_mst_from_disk()
            .await
            .expect("rebuild peers store");

        let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
        let session_store =
            LocalSessionStore::open(db.clone(), &noise_keys).expect("open local session store");
        let registry = Registry::new(
            peers_store,
            session_store,
            SigningKey::from_bytes(&[0xA5; 32]),
            Arc::new(noise_keys),
            actor,
            HealthMonitor::new(actor),
        );

        let scheduler =
            Rc::new(Scheduler::new(scheduler_store, registry, actor).expect("scheduler"));
        let service = SchedulerService::new(scheduler.clone(), actor, "node-a".to_string());
        let client: scheduler::Client = capnp_new_client(service);
        (client, scheduler, actor, dir)
    }

    /// prepareLeases should reject uninitialized schedulers with a zero-capacity digest payload.
    #[tokio::test]
    async fn prepare_leases_returns_structured_uninitialized_rejection() {
        let (client, _scheduler, node_id, _dir) = make_scheduler_client().await;
        let mut request = client.prepare_leases_request();
        {
            let mut inner = request.get().init_request();
            inner.set_coordinator_node_id(Uuid::new_v4().as_bytes());
            inner.set_ttl_ms(TEST_PREPARED_LEASE_TTL_MS);
            let mut intents = inner.reborrow().init_intents(1);
            let mut intent = intents.reborrow().get(0);
            intent.set_task_id(Uuid::new_v4().as_bytes());
            intent.set_cpu_millis(500);
            intent.set_memory_bytes(512 * 1024 * 1024);
            intent.set_gpu_count(0);
        }

        let response = request.send().promise.await.expect("call prepareLeases");
        let payload = response
            .get()
            .expect("prepareLeases response")
            .get_response()
            .expect("prepareLeases payload");

        match payload.which().expect("prepareLeases variant") {
            scheduling::prepare_leases_response::Rejected(Ok(rejected)) => {
                assert_eq!(
                    rejected.get_reason().expect("rejection reason"),
                    scheduling::PrepareLeasesRejectionReason::Uninitialized
                );
                let digest =
                    read_scheduler_digest(rejected.get_current_digest().expect("rejection digest"))
                        .expect("decode rejection digest");
                assert_eq!(digest.node_id, node_id);
                assert_eq!(digest.snapshot_version, 0);
                assert_eq!(digest.free_slot_count, 0);
                assert_eq!(digest.free_cpu_millis, 0);
                assert_eq!(digest.free_memory_bytes, 0);
                assert_eq!(digest.free_gpu_count, 0);
            }
            _ => panic!("prepareLeases should reject uninitialized schedulers"),
        }
    }

    /// prepareLeases should return the current digest when a batch is rejected for capacity.
    #[tokio::test]
    async fn prepare_leases_returns_structured_capacity_rejection_with_digest() {
        let (client, scheduler, node_id, _dir) = make_scheduler_client().await;
        scheduler
            .init_slots([SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )])
            .await
            .expect("init scheduler");

        let mut request = client.prepare_leases_request();
        {
            let mut inner = request.get().init_request();
            inner.set_coordinator_node_id(Uuid::new_v4().as_bytes());
            inner.set_ttl_ms(TEST_PREPARED_LEASE_TTL_MS);
            let mut intents = inner.reborrow().init_intents(1);
            let mut intent = intents.reborrow().get(0);
            intent.set_task_id(Uuid::new_v4().as_bytes());
            intent.set_cpu_millis(1_500);
            intent.set_memory_bytes(1536 * 1024 * 1024);
            intent.set_gpu_count(0);
        }

        let response = request.send().promise.await.expect("call prepareLeases");
        let payload = response
            .get()
            .expect("prepareLeases response")
            .get_response()
            .expect("prepareLeases payload");

        match payload.which().expect("prepareLeases variant") {
            scheduling::prepare_leases_response::Rejected(Ok(rejected)) => {
                assert_eq!(
                    rejected.get_reason().expect("rejection reason"),
                    scheduling::PrepareLeasesRejectionReason::InsufficientResources
                );
                let digest =
                    read_scheduler_digest(rejected.get_current_digest().expect("rejection digest"))
                        .expect("decode rejection digest");
                assert_eq!(digest.node_id, node_id);
                assert_eq!(digest.snapshot_version, 0);
                assert_eq!(digest.free_slot_count, 1);
                assert_eq!(digest.free_cpu_millis, 500);
                assert_eq!(digest.free_memory_bytes, 512 * 1024 * 1024);
                assert_eq!(digest.free_gpu_count, 0);
            }
            _ => panic!("prepareLeases should reject oversized batches"),
        }
    }

    /// Group lease RPCs should prepare and commit all target-side resources with the group id.
    #[tokio::test]
    async fn lease_group_rpc_prepares_and_commits_group_reservations() {
        let (client, scheduler, coordinator, _dir) = make_scheduler_client().await;
        scheduler
            .init_slots([SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )])
            .await
            .expect("init scheduler");

        let group_id = Uuid::new_v4();
        let task_id = Uuid::new_v4();
        let mut prepare = client.prepare_lease_group_request();
        {
            let mut inner = prepare.get().init_request();
            inner.set_group_id(group_id.as_bytes());
            inner.set_coordinator_node_id(coordinator.as_bytes());
            inner.set_ttl_ms(TEST_PREPARED_LEASE_TTL_MS);
            let mut intents = inner.reborrow().init_intents(1);
            let mut intent = intents.reborrow().get(0);
            intent.set_task_id(task_id.as_bytes());
            intent.set_cpu_millis(100);
            intent.set_memory_bytes(64 * 1024 * 1024);
            intent.set_gpu_count(0);
        }

        let prepare_response = prepare
            .send()
            .promise
            .await
            .expect("call prepareLeaseGroup");
        let payload = prepare_response
            .get()
            .expect("prepareLeaseGroup response")
            .get_response()
            .expect("prepareLeaseGroup payload");
        let prepared = match payload.which().expect("prepareLeaseGroup variant") {
            scheduling::prepare_leases_response::Prepared(Ok(leases)) => leases,
            _ => panic!("prepareLeaseGroup should prepare the request"),
        };
        assert_eq!(prepared.len(), 1);

        let mut commit = client.commit_lease_group_request();
        {
            let mut inner = commit.get().init_request();
            inner.set_group_id(group_id.as_bytes());
            inner.set_coordinator_node_id(coordinator.as_bytes());
            let mut prepared_builder = inner.reborrow().init_prepared(1);
            let mut lease = prepared_builder.reborrow().get(0);
            let prepared_lease = prepared.get(0);
            lease.set_lease_id(prepared_lease.get_lease_id().expect("lease id"));
            lease.set_task_id(prepared_lease.get_task_id().expect("task id"));
            lease.set_expires_at_unix_ms(prepared_lease.get_expires_at_unix_ms());
            let prepared_slots = prepared_lease.get_slot_ids().expect("slot ids");
            let mut slot_ids = lease.reborrow().init_slot_ids(prepared_slots.len());
            for (idx, slot_id) in prepared_slots.iter().enumerate() {
                slot_ids.set(idx as u32, slot_id);
            }
            lease.reborrow().init_gpu_device_ids(0);
        }
        commit.send().promise.await.expect("call commitLeaseGroup");

        let snapshot = scheduler.snapshot().await.expect("snapshot");
        match &snapshot.slots[0].state {
            SlotState::Reserved(reservation) => {
                assert_eq!(reservation.task_id, Some(task_id));
                assert_eq!(reservation.group_id, Some(group_id));
            }
            other => panic!("expected group reservation, got {other:?}"),
        }
    }
}
