#![no_main]

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use libfuzzer_sys::fuzz_target;
use mantissa::registry::Registry;
use mantissa::scheduler::{
    AbortTaskLeaseIntent, ExactTaskLeaseIntent, GpuDeviceSpec, GpuDeviceState,
    GpuReservationRequest, LeaseReservation, PreparedTaskLease, ResourceSlot, Scheduler,
    SchedulerSnapshot, SlotCapacity, SlotReservationRequest, SlotSpec, SlotState,
    TaskLeaseIntent,
};
use mantissa::store::local::LocalSessionStore;
use mantissa::store::replicated::peers::open_peers_store;
use mantissa::store::replicated::scheduler::open_scheduler_store;
use mantissa_health::HealthMonitor;
use mantissa_net::noise::NoiseKeys;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_OPS: usize = 32;
const SLOT_IDS: [u64; 4] = [0, 1, 2, 3];
const GPU_IDS: [&str; 2] = ["gpu-0", "gpu-1"];
const LEASE_TTL_MS: u64 = 60_000;

fuzz_target!(|data: &[u8]| {
    let input = SchedulerInput::from_bytes(data);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Tokio runtime should build for scheduler fuzzing");
    runtime.block_on(async {
        input.run().await;
    });
});

#[derive(Debug)]
struct SchedulerInput {
    seed: [u8; 16],
    other_seed: [u8; 16],
    ops: Vec<Operation>,
}

#[derive(Clone, Copy, Debug)]
struct Operation {
    tag: u8,
    a: u8,
    b: u8,
    c: u8,
}

#[derive(Clone, Debug)]
struct LeaseBatch {
    coordinator: Uuid,
    group_id: Option<Uuid>,
    leases: Vec<PreparedTaskLease>,
}

impl SchedulerInput {
    /// Maps arbitrary bytes into a bounded scheduler operation sequence.
    fn from_bytes(data: &[u8]) -> Self {
        let mut ops = Vec::new();
        for chunk in data.chunks_exact(4).take(MAX_OPS) {
            ops.push(Operation {
                tag: chunk[0],
                a: chunk[1],
                b: chunk[2],
                c: chunk[3],
            });
        }

        if ops.is_empty() {
            ops.push(Operation {
                tag: 0,
                a: 0,
                b: 0,
                c: 0,
            });
        }

        Self {
            seed: fixed_bytes(data, 0),
            other_seed: fixed_bytes(data, 16),
            ops,
        }
    }

    /// Drives one Redb-backed scheduler through generated resource operations.
    async fn run(&self) {
        let actor = self.uuid(0);
        let (scheduler, _dir) = make_scheduler(actor).await;
        let mut latest_prepared: Option<LeaseBatch> = None;
        let mut latest_group: Option<LeaseBatch> = None;

        let initial = scheduler
            .init_resources(slot_specs(), gpu_specs())
            .await
            .expect("generated scheduler should initialize");
        assert_snapshot_invariants(&initial);

        for (idx, op) in self.ops.iter().copied().enumerate() {
            match op.tag % 12 {
                0 => {
                    self.apply_reserve(&scheduler, op, idx).await;
                }
                1 => {
                    self.apply_free(&scheduler, op).await;
                }
                2 => {
                    let coordinator = self.uuid(20 + idx as u8);
                    let result = checked_mutation(&scheduler, |scheduler, _before| async move {
                        scheduler
                            .prepare_task_leases(
                                coordinator,
                                LEASE_TTL_MS,
                                vec![self.task_lease_intent(op, idx)],
                            )
                            .await
                            .map(|batch| batch.leases)
                    })
                    .await;
                    if let Ok(leases) = result
                        && !leases.is_empty()
                    {
                        latest_prepared = Some(LeaseBatch {
                            coordinator,
                            group_id: None,
                            leases,
                        });
                    }
                }
                3 => {
                    self.apply_commit_one(&scheduler, op, &mut latest_prepared)
                        .await;
                }
                4 => {
                    self.apply_abort_prepared(&scheduler, op, &mut latest_prepared)
                        .await;
                }
                5 => {
                    let coordinator = self.uuid(40 + idx as u8);
                    let group_id = self.uuid(80 + idx as u8);
                    let result = checked_mutation(&scheduler, |scheduler, _before| async move {
                        scheduler
                            .prepare_task_lease_group(
                                coordinator,
                                group_id,
                                LEASE_TTL_MS,
                                vec![self.task_lease_intent(op, idx)],
                            )
                            .await
                            .map(|batch| batch.leases)
                    })
                    .await;
                    if let Ok(leases) = result
                        && !leases.is_empty()
                    {
                        latest_group = Some(LeaseBatch {
                            coordinator,
                            group_id: Some(group_id),
                            leases,
                        });
                    }
                }
                6 => {
                    self.apply_commit_group(&scheduler, op, &mut latest_group)
                        .await;
                }
                7 => {
                    self.apply_abort_group(&scheduler, &mut latest_group).await;
                }
                8 => {
                    self.apply_exact_group(&scheduler, op, idx, &mut latest_group)
                        .await;
                }
                9 => {
                    checked_mutation(&scheduler, |scheduler, _before| async move {
                        scheduler.reap_expired_leases().await.map(|_| ())
                    })
                    .await
                    .ok();
                }
                10 => {
                    checked_mutation(&scheduler, |scheduler, _before| async move {
                        scheduler.init_resources(slot_specs(), gpu_specs()).await.map(|_| ())
                    })
                    .await
                    .ok();
                }
                _ => {
                    self.apply_stale_reserve(&scheduler, op, idx).await;
                }
            }
        }

        let final_snapshot = scheduler
            .snapshot()
            .await
            .expect("initialized scheduler should keep a snapshot");
        assert_snapshot_invariants(&final_snapshot);
    }

    /// Applies one direct reserve operation and checks failure atomicity.
    async fn apply_reserve(&self, scheduler: &Rc<Scheduler>, op: Operation, idx: usize) {
        checked_mutation(scheduler, |scheduler, before| async move {
            scheduler
                .reserve_resources(
                    expected_version(before, op),
                    slot_requests(self, op, idx),
                    gpu_requests(self, op, idx),
                )
                .await
                .map(|_| ())
        })
        .await
        .ok();
    }

    /// Applies one direct free operation and checks failure atomicity.
    async fn apply_free(&self, scheduler: &Rc<Scheduler>, op: Operation) {
        checked_mutation(scheduler, |scheduler, before| async move {
            scheduler
                .free_resources(
                    expected_version(before, op),
                    vec![slot_id(op.a)],
                    vec![gpu_id(op.b).to_string()],
                )
                .await
                .map(|_| ())
        })
        .await
        .ok();
    }

    /// Applies one stale-version reserve that should fail without mutation.
    async fn apply_stale_reserve(&self, scheduler: &Rc<Scheduler>, op: Operation, idx: usize) {
        checked_mutation(scheduler, |scheduler, before| async move {
            scheduler
                .reserve_resources(
                    before.version.wrapping_add(1),
                    slot_requests(self, op, idx),
                    gpu_requests(self, op, idx),
                )
                .await
                .map(|_| ())
        })
        .await
        .ok();
    }

    /// Commits one prepared ungrouped lease, sometimes with perturbed expectations.
    async fn apply_commit_one(
        &self,
        scheduler: &Rc<Scheduler>,
        op: Operation,
        latest_prepared: &mut Option<LeaseBatch>,
    ) {
        let Some(batch) = latest_prepared.clone() else {
            return;
        };
        let Some(lease) = batch.leases.first().cloned() else {
            *latest_prepared = None;
            return;
        };
        let mut slot_ids = lease.slot_ids.clone();
        let mut gpu_ids = lease.gpu_device_ids.clone();
        if op.b & 0b0000_0001 != 0 {
            slot_ids.push(99);
        }
        if op.b & 0b0000_0010 != 0 {
            gpu_ids.push("gpu-unknown".to_string());
        }

        let result = checked_mutation(scheduler, |scheduler, _before| async move {
            scheduler
                .commit_task_lease(
                    lease.lease_id,
                    maybe_wrong_uuid(batch.coordinator, op.b, 0b0000_0100),
                    maybe_wrong_uuid(lease.task_id, op.b, 0b0000_1000),
                    &slot_ids,
                    &gpu_ids,
                )
                .await
                .map(|_| ())
        })
        .await;
        if result.is_ok() {
            *latest_prepared = None;
        }
    }

    /// Aborts the latest ungrouped lease batch.
    async fn apply_abort_prepared(
        &self,
        scheduler: &Rc<Scheduler>,
        op: Operation,
        latest_prepared: &mut Option<LeaseBatch>,
    ) {
        let Some(batch) = latest_prepared.clone() else {
            return;
        };
        let intents = batch
            .leases
            .iter()
            .map(|lease| AbortTaskLeaseIntent {
                lease_id: lease.lease_id,
                task_id: maybe_wrong_uuid(lease.task_id, op.b, 0b0000_0001),
            })
            .collect::<Vec<_>>();
        let result = checked_mutation(scheduler, |scheduler, _before| async move {
            scheduler
                .abort_task_leases(
                    maybe_wrong_uuid(batch.coordinator, op.b, 0b0000_0010),
                    intents,
                )
                .await
                .map(|_| ())
        })
        .await;
        if result.is_ok() {
            *latest_prepared = None;
        }
    }

    /// Commits one prepared group, sometimes with truncated expectations.
    async fn apply_commit_group(
        &self,
        scheduler: &Rc<Scheduler>,
        op: Operation,
        latest_group: &mut Option<LeaseBatch>,
    ) {
        let Some(batch) = latest_group.clone() else {
            return;
        };
        let Some(group_id) = batch.group_id else {
            return;
        };
        let mut leases = batch.leases.clone();
        if op.b & 0b0000_0001 != 0 && !leases.is_empty() {
            leases.pop();
        }
        if op.b & 0b0000_0010 != 0
            && let Some(first) = leases.first().cloned()
        {
            leases.push(first);
        }

        let result = checked_mutation(scheduler, |scheduler, _before| async move {
            scheduler
                .commit_task_lease_group(
                    group_id,
                    maybe_wrong_uuid(batch.coordinator, op.b, 0b0000_0100),
                    &leases,
                )
                .await
                .map(|_| ())
        })
        .await;
        if result.is_ok() {
            *latest_group = None;
        }
    }

    /// Aborts the latest prepared or committed group.
    async fn apply_abort_group(
        &self,
        scheduler: &Rc<Scheduler>,
        latest_group: &mut Option<LeaseBatch>,
    ) {
        let Some(batch) = latest_group.clone() else {
            return;
        };
        let Some(group_id) = batch.group_id else {
            return;
        };
        checked_mutation(scheduler, |scheduler, _before| async move {
            scheduler
                .abort_task_lease_group(batch.coordinator, group_id)
                .await
                .map(|_| ())
        })
        .await
        .ok();
        *latest_group = None;
    }

    /// Exercises exact group lease preparation with known, duplicate, and unknown resources.
    async fn apply_exact_group(
        &self,
        scheduler: &Rc<Scheduler>,
        op: Operation,
        idx: usize,
        latest_group: &mut Option<LeaseBatch>,
    ) {
        let coordinator = self.uuid(100 + idx as u8);
        let group_id = self.uuid(120 + idx as u8);
        let mut slot_ids = vec![slot_id(op.a)];
        let mut gpu_ids = Vec::new();
        if op.b & 0b0000_0001 != 0 {
            slot_ids.push(slot_ids[0]);
        }
        if op.b & 0b0000_0010 != 0 {
            slot_ids.push(999);
        }
        if op.b & 0b0000_0100 != 0 {
            gpu_ids.push(gpu_id(op.c).to_string());
        }
        if op.b & 0b0000_1000 != 0 {
            gpu_ids.push("gpu-unknown".to_string());
        }

        let intent = ExactTaskLeaseIntent {
            task_id: self.uuid(140 + idx as u8),
            slot_ids,
            gpu_device_ids: gpu_ids,
        };
        let result = checked_mutation(scheduler, |scheduler, before| async move {
            scheduler
                .prepare_exact_task_lease_group(
                    expected_version(before, op),
                    coordinator,
                    group_id,
                    LEASE_TTL_MS,
                    vec![intent],
                )
                .await
                .map(|batch| batch.leases)
        })
        .await;
        if let Ok(leases) = result
            && !leases.is_empty()
        {
            *latest_group = Some(LeaseBatch {
                coordinator,
                group_id: Some(group_id),
                leases,
            });
        }
    }

    /// Builds a vector lease intent from generated operation bytes.
    fn task_lease_intent(&self, op: Operation, idx: usize) -> TaskLeaseIntent {
        let cpu_millis = match op.a % 4 {
            0 => 0,
            1 => 250,
            2 => 750,
            _ => 1_500,
        };
        let memory_bytes = match op.b % 4 {
            0 => 0,
            1 => 128 * 1024 * 1024,
            2 => 512 * 1024 * 1024,
            _ => 2 * 1024 * 1024 * 1024,
        };
        let gpu_count = u32::from(op.c % 3);

        TaskLeaseIntent {
            task_id: self.uuid(160 + idx as u8),
            cpu_millis,
            memory_bytes,
            gpu_count,
        }
    }

    /// Returns a deterministic UUID derived from the fuzzed seeds.
    fn uuid(&self, salt: u8) -> Uuid {
        let mut bytes = if salt.is_multiple_of(2) {
            self.seed
        } else {
            self.other_seed
        };
        bytes[0] ^= salt;
        Uuid::from_bytes(bytes)
    }
}

/// Runs one scheduler mutation and checks success invariants or failure atomicity.
async fn checked_mutation<F, Fut, T>(scheduler: &Rc<Scheduler>, action: F) -> Result<T, ()>
where
    F: FnOnce(Rc<Scheduler>, SchedulerSnapshot) -> Fut,
    Fut: std::future::Future<Output = Result<T, mantissa::scheduler::SchedulerError>>,
{
    let before = scheduler
        .snapshot()
        .await
        .expect("initialized scheduler should expose a snapshot");
    assert_snapshot_invariants(&before);

    match action(scheduler.clone(), before.clone()).await {
        Ok(value) => {
            let after = scheduler
                .snapshot()
                .await
                .expect("initialized scheduler should expose a snapshot after mutation");
            assert_snapshot_invariants(&after);
            if after == before {
                assert_eq!(after.version, before.version);
            } else {
                assert!(
                    after.version > before.version,
                    "mutated scheduler snapshots must advance versions"
                );
            }
            Ok(value)
        }
        Err(_) => {
            let after = scheduler
                .snapshot()
                .await
                .expect("initialized scheduler should expose a snapshot after failure");
            assert_eq!(after, before, "failed scheduler mutations must be atomic");
            Err(())
        }
    }
}

/// Builds one temporary scheduler backed by Redb and the production store adapters.
async fn make_scheduler(actor: Uuid) -> (Rc<Scheduler>, TempDir) {
    let dir = tempfile::tempdir().expect("scheduler fuzz tempdir should be created");
    let db_path = dir.path().join("scheduler-fuzz.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("scheduler fuzz db should open"));

    let scheduler_store = open_scheduler_store(db.clone(), actor).expect("scheduler store opens");
    scheduler_store
        .rebuild_mst_from_disk()
        .await
        .expect("scheduler store rebuilds");

    let peers_store = open_peers_store(db.clone(), actor).expect("peers store opens");
    peers_store
        .rebuild_mst_from_disk()
        .await
        .expect("peers store rebuilds");

    let noise_keys = NoiseKeys::from_private_bytes([0x11; 32]);
    let session_store =
        LocalSessionStore::open(db, &noise_keys).expect("local session store opens");
    let health_monitor = HealthMonitor::new(actor);
    let registry = Registry::new(
        peers_store,
        session_store,
        SigningKey::from_bytes(&[0xA5; 32]),
        Arc::new(noise_keys),
        actor,
        health_monitor,
    );
    let scheduler = Scheduler::new(scheduler_store, registry, actor).expect("scheduler opens");

    (Rc::new(scheduler), dir)
}

/// Builds a small deterministic slot inventory.
fn slot_specs() -> Vec<SlotSpec> {
    vec![
        SlotSpec::new(0, SlotCapacity::new(500, 256 * 1024 * 1024, 0)),
        SlotSpec::new(1, SlotCapacity::new(1_000, 512 * 1024 * 1024, 0)),
        SlotSpec::new(2, SlotCapacity::new(1_500, 1024 * 1024 * 1024, 0)),
        SlotSpec::new(3, SlotCapacity::new(2_000, 2 * 1024 * 1024 * 1024, 0)),
    ]
}

/// Builds a small deterministic GPU inventory.
fn gpu_specs() -> Vec<GpuDeviceSpec> {
    GPU_IDS
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            GpuDeviceSpec::new(
                *id,
                idx as u32,
                Some(format!("GPU-{idx}")),
                Some(format!("0000:0{idx}:00.0")),
                format!("Fuzz GPU {idx}"),
                16 * 1024 * 1024 * 1024,
            )
        })
        .collect()
}

/// Builds generated slot reservation requests.
fn slot_requests(input: &SchedulerInput, op: Operation, idx: usize) -> Vec<SlotReservationRequest> {
    let mut requests = vec![SlotReservationRequest {
        slot_id: slot_id(op.a),
        owner: input.uuid(180 + idx as u8),
        task_id: Some(input.uuid(181 + idx as u8)),
        group_id: (op.b & 0b0001_0000 != 0).then(|| input.uuid(182 + idx as u8)),
    }];
    if op.b & 0b0000_0001 != 0 {
        requests.push(requests[0].clone());
    }
    if op.b & 0b0000_0010 != 0 {
        requests.push(SlotReservationRequest {
            slot_id: 999,
            owner: input.uuid(183 + idx as u8),
            task_id: None,
            group_id: None,
        });
    }
    requests
}

/// Builds generated GPU reservation requests.
fn gpu_requests(input: &SchedulerInput, op: Operation, idx: usize) -> Vec<GpuReservationRequest> {
    if op.b & 0b0000_0100 == 0 {
        return Vec::new();
    }

    let mut requests = vec![GpuReservationRequest {
        device_id: gpu_id(op.c).to_string(),
        owner: input.uuid(190 + idx as u8),
        task_id: Some(input.uuid(191 + idx as u8)),
        group_id: (op.b & 0b0010_0000 != 0).then(|| input.uuid(192 + idx as u8)),
    }];
    if op.b & 0b0000_1000 != 0 {
        requests.push(requests[0].clone());
    }
    if op.b & 0b0100_0000 != 0 {
        requests.push(GpuReservationRequest {
            device_id: "gpu-unknown".to_string(),
            owner: input.uuid(193 + idx as u8),
            task_id: None,
            group_id: None,
        });
    }
    requests
}

/// Returns the expected version, sometimes intentionally stale.
fn expected_version(snapshot: SchedulerSnapshot, op: Operation) -> u64 {
    if op.b & 0b1000_0000 != 0 {
        snapshot.version.wrapping_add(1)
    } else {
        snapshot.version
    }
}

/// Picks one known slot id from generated bytes.
fn slot_id(value: u8) -> u64 {
    SLOT_IDS[value as usize % SLOT_IDS.len()]
}

/// Picks one known GPU id from generated bytes.
fn gpu_id(value: u8) -> &'static str {
    GPU_IDS[value as usize % GPU_IDS.len()]
}

/// Optionally perturbs a UUID to drive mismatch paths.
fn maybe_wrong_uuid(value: Uuid, flags: u8, bit: u8) -> Uuid {
    if flags & bit == 0 {
        return value;
    }

    let mut bytes = *value.as_bytes();
    bytes[0] ^= bit;
    Uuid::from_bytes(bytes)
}

/// Copies a fixed-width little-endian lane out of arbitrary input bytes.
fn fixed_bytes<const N: usize>(data: &[u8], offset: usize) -> [u8; N] {
    let mut bytes = [0u8; N];
    if offset < data.len() {
        let len = (data.len() - offset).min(N);
        bytes[..len].copy_from_slice(&data[offset..offset + len]);
    }
    bytes
}

/// Verifies one scheduler snapshot cannot encode duplicate or inconsistent ownership.
fn assert_snapshot_invariants(snapshot: &SchedulerSnapshot) {
    assert_unique_sorted_slots(&snapshot.slots);
    assert_unique_sorted_gpus(snapshot);
    assert_lease_consistency(snapshot);
}

/// Verifies slot ids remain unique and sorted.
fn assert_unique_sorted_slots(slots: &[ResourceSlot]) {
    let mut seen = BTreeSet::new();
    let mut previous = None;
    for slot in slots {
        assert!(seen.insert(slot.slot_id), "duplicate scheduler slot id");
        if let Some(previous) = previous {
            assert!(previous < slot.slot_id, "scheduler slots should stay sorted");
        }
        previous = Some(slot.slot_id);
    }
}

/// Verifies GPU ids remain unique and sorted.
fn assert_unique_sorted_gpus(snapshot: &SchedulerSnapshot) {
    let mut seen = BTreeSet::new();
    let mut previous: Option<&str> = None;
    for device in &snapshot.gpu_devices {
        assert!(
            seen.insert(device.device_id.as_str()),
            "duplicate scheduler GPU id"
        );
        if let Some(previous) = previous {
            assert!(
                previous < device.device_id.as_str(),
                "scheduler GPU devices should stay sorted"
            );
        }
        previous = Some(device.device_id.as_str());
    }
}

/// Verifies all resources for the same prepared lease agree on lease metadata.
fn assert_lease_consistency(snapshot: &SchedulerSnapshot) {
    let mut leases: BTreeMap<Uuid, LeaseReservation> = BTreeMap::new();
    for slot in &snapshot.slots {
        if let SlotState::Leased(lease) = &slot.state {
            assert_consistent_lease(&mut leases, lease);
        }
    }
    for device in &snapshot.gpu_devices {
        if let GpuDeviceState::Leased(lease) = &device.state {
            assert_consistent_lease(&mut leases, lease);
        }
    }
}

/// Records or compares one lease metadata row.
fn assert_consistent_lease(leases: &mut BTreeMap<Uuid, LeaseReservation>, lease: &LeaseReservation) {
    if let Some(existing) = leases.insert(lease.lease_id, lease.clone()) {
        assert_eq!(existing.coordinator_node_id, lease.coordinator_node_id);
        assert_eq!(existing.task_id, lease.task_id);
        assert_eq!(existing.expires_at_unix_ms, lease.expires_at_unix_ms);
        assert_eq!(existing.group_id, lease.group_id);
    }
}
