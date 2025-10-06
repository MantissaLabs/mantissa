use std::collections::{BTreeSet, HashMap, HashSet};

use arc_swap::ArcSwapOption;
use crdt_store::uuid_key::UuidKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::registry::Registry;
use crate::store::scheduler_store::SchedulerStore;

use self::summary::SchedulerSummary;

pub mod service;
pub mod summary;

pub type SlotId = u64;

/// Reservation details attached to a slot when it is taken.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SlotReservation {
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}

/// Current state of a slot inside the scheduler snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SlotState {
    Free,
    Reserved(SlotReservation),
}

/// Capacity assigned to a slot. Values are expressed in milli-CPUs and bytes so we can represent
/// fractional CPU shares and precise memory allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SlotCapacity {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

impl SlotCapacity {
    pub const fn new(cpu_millis: u64, memory_bytes: u64) -> Self {
        Self {
            cpu_millis,
            memory_bytes,
        }
    }
}

/// Slot entry stored inside the CRDT snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ResourceSlot {
    pub slot_id: SlotId,
    pub capacity: SlotCapacity,
    pub state: SlotState,
}

/// Full scheduler snapshot persisted in the MVReg-backed store.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub version: u64,
    pub slots: Vec<ResourceSlot>,
}

/// Definition used during initialisation to map node resources to scheduler slots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotSpec {
    pub slot_id: SlotId,
    pub capacity: SlotCapacity,
}

impl SlotSpec {
    pub const fn new(slot_id: SlotId, capacity: SlotCapacity) -> Self {
        Self { slot_id, capacity }
    }
}

/// Reservation intent provided by callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotReservationRequest {
    pub slot_id: SlotId,
    pub owner: Uuid,
    pub task_id: Option<Uuid>,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("scheduler store error: {0}")]
    Store(#[from] Box<crdt_store::error::Error>),

    #[error("scheduler already initialised")]
    AlreadyInitialized { snapshot: SchedulerSnapshot },

    #[error("scheduler not initialised")]
    Uninitialized,

    #[error("snapshot mismatch (expected {expected_version}, current {current_version})")]
    SnapshotMismatch {
        expected_version: u64,
        current_version: u64,
        snapshot: SchedulerSnapshot,
    },

    #[error("duplicate slot ids in request: {duplicates:?}")]
    DuplicateSlots {
        duplicates: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("unknown slots in request: {unknown:?}")]
    UnknownSlots {
        unknown: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("slots unavailable: {conflicts:?}")]
    SlotsUnavailable {
        conflicts: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },

    #[error("slots not reserved: {slots:?}")]
    SlotsNotReserved {
        slots: Vec<SlotId>,
        snapshot: SchedulerSnapshot,
    },
}

#[derive(Clone)]
struct SchedulerState {
    snapshot: SchedulerSnapshot,
    index: HashMap<SlotId, usize>,
}

impl SchedulerState {
    fn new(snapshot: SchedulerSnapshot) -> Self {
        let index = Self::build_index(&snapshot.slots);
        Self { snapshot, index }
    }

    fn build_index(slots: &[ResourceSlot]) -> HashMap<SlotId, usize> {
        let mut index = HashMap::with_capacity(slots.len());
        for (pos, slot) in slots.iter().enumerate() {
            index.insert(slot.slot_id, pos);
        }
        index
    }
}

/// Scheduler maintains a local in-memory view of slots together with a CRDT-backed snapshot
/// that is ready to be gossiped to other peers.
pub struct Scheduler {
    store: SchedulerStore,
    store_key: UuidKey,
    state: Arc<ArcSwapOption<SchedulerState>>, // stores Option<Arc<SchedulerState>>
    registry: Registry,
}

fn ptr_eq_option(a: &Option<Arc<SchedulerState>>, b: &Option<Arc<SchedulerState>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}

impl Scheduler {
    pub fn new(
        store: SchedulerStore,
        registry: Registry,
        resource_id: Uuid,
    ) -> Result<Self, SchedulerError> {
        let store_key = UuidKey::from(resource_id);
        let existing_snapshot = store
            .get_snapshot(&store_key)?
            .and_then(|snap| snap.as_slice().last().cloned());

        let initial_state =
            existing_snapshot.map(|snapshot| Arc::new(SchedulerState::new(snapshot)));
        let state = Arc::new(ArcSwapOption::new(initial_state));

        Ok(Self {
            store,
            store_key,
            state,
            registry,
        })
    }

    pub async fn init_slots<I>(&self, slots: I) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotSpec>,
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

        let snapshot = SchedulerSnapshot { version: 0, slots };

        let state_arc = Arc::new(SchedulerState::new(snapshot.clone()));

        let prev = self
            .state
            .compare_and_swap(&None::<Arc<SchedulerState>>, Some(state_arc.clone()));

        if prev.is_some() {
            // Another thread won the race to initialise the scheduler; reuse its snapshot.
            let snapshot = prev.as_ref().map(|state| state.snapshot.clone()).unwrap();
            return Err(SchedulerError::AlreadyInitialized { snapshot });
        }

        if let Err(e) = self.store.upsert(&self.store_key, snapshot.clone()).await {
            let _ = self.state.compare_and_swap(&Some(state_arc.clone()), None);
            return Err(SchedulerError::Store(e));
        }

        Ok(snapshot)
    }

    pub async fn snapshot(&self) -> Option<SchedulerSnapshot> {
        self.state
            .load_full()
            .as_ref()
            .map(|state| state.snapshot.clone())
    }

    pub async fn reserve_slots(
        &self,
        expected_version: u64,
        requests: Vec<SlotReservationRequest>,
    ) -> Result<SchedulerSnapshot, SchedulerError> {
        if requests.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        // Reservations mutate the shared snapshot, so we retry until our compare-and-swap (CAS)
        // succeeds. Each iteration works against an immutable view of the current scheduler
        // state which guarantees consistent validation while preventing write tearing.
        loop {
            // Snapshot the current scheduler state. CAS means the pointer we read can go stale, so
            // everything below must be prepared to restart if another writer wins the race.
            let current_opt = self.state.load_full();
            let current_arc = match current_opt.as_ref() {
                Some(state) => state.clone(),
                None => return Err(SchedulerError::Uninitialized),
            };
            let current = current_arc.as_ref();

            // Callers pass the version they observed; we only proceed if nothing changed in the
            // meantime. This enforces optimistic concurrency semantics for the scheduler API.
            if current.snapshot.version != expected_version {
                return Err(SchedulerError::SnapshotMismatch {
                    expected_version,
                    current_version: current.snapshot.version,
                    snapshot: current.snapshot.clone(),
                });
            }

            // Track the validation outcome using deterministic sets so callers receive stable
            // ordering without the extra sort/dedup passes we previously needed.
            let mut seen = HashSet::with_capacity(requests.len());
            let mut duplicates = BTreeSet::new();
            let mut unknown = BTreeSet::new();
            let mut conflicts = BTreeSet::new();

            for req in &requests {
                // Reject duplicate requests first.
                if !seen.insert(req.slot_id) {
                    duplicates.insert(req.slot_id);
                    continue;
                }

                match current.index.get(&req.slot_id) {
                    Some(&idx) => {
                        if !matches!(current.snapshot.slots[idx].state, SlotState::Free) {
                            conflicts.insert(req.slot_id);
                        }
                    }
                    None => {
                        unknown.insert(req.slot_id);
                    }
                }
            }

            if !duplicates.is_empty() {
                return Err(SchedulerError::DuplicateSlots {
                    duplicates: duplicates.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !unknown.is_empty() {
                return Err(SchedulerError::UnknownSlots {
                    unknown: unknown.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            if !conflicts.is_empty() {
                return Err(SchedulerError::SlotsUnavailable {
                    conflicts: conflicts.into_iter().collect(),
                    snapshot: current.snapshot.clone(),
                });
            }

            // Clone the snapshot so we can safely mutate a private copy while readers continue to
            // observe the old data. We only publish the new snapshot once every validation passes.
            let mut new_snapshot = current.snapshot.clone();
            for req in &requests {
                let idx = current.index[&req.slot_id];
                new_snapshot.slots[idx].state = SlotState::Reserved(SlotReservation {
                    owner: req.owner,
                    task_id: req.task_id,
                });
            }

            // Monotonic versioning gives downstream consumers a simple way to detect updates and
            // mirrors the MVReg behaviour in the backing store.
            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

            let new_state_arc = Arc::new(SchedulerState::new(new_snapshot.clone()));

            // Attempt the CAS publication. A mismatch signals that another thread beat us, so we
            // restart the loop with the freshest pointer.
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
                // Durable persistence failed; roll back the published state so readers continue
                // to observe the pre-update snapshot.
                let _ = self
                    .state
                    .compare_and_swap(&Some(new_state_arc.clone()), current_opt.clone());
                return Err(SchedulerError::Store(e));
            }

            return Ok(new_snapshot);
        }
    }

    async fn fetch_remote_summary_via_handle(
        registry: &Registry,
        client: &protocol::server::Client,
        peer_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        let session = registry
            .scheduler_session_via_handle(client, peer_id)
            .await
            .ok_or_else(|| {
                capnp::Error::failed(format!(
                    "unable to open scheduler session with peer {peer_id}"
                ))
            })?;

        let scheduler_client = session
            .get_scheduler_request()
            .send()
            .promise
            .await?
            .get()?
            .get_scheduler()?;

        let mut summary_req = scheduler_client.summary_request();
        {
            let mut inner = summary_req.get().init_request();
            inner.set_peer_id(&[]);
            inner.set_include_details(include_details);
        }

        let response = summary_req.send().promise.await?;
        let reader = response.get()?.get_summary()?;

        SchedulerSummary::from_reader(reader)
    }

    pub async fn fetch_remote_summary(
        &self,
        peer_id: Uuid,
        include_details: bool,
    ) -> Result<SchedulerSummary, capnp::Error> {
        let self_id = self.store_key.to_uuid();

        if peer_id == self_id {
            return Err(capnp::Error::failed(
                "peer id references local node for scheduler summary".into(),
            ));
        }

        let mut client = match self.registry.server_handle_for(peer_id).await {
            Some(handle) => handle,
            None => self
                .registry
                .refresh_peer_handle(peer_id)
                .await
                .ok_or_else(|| {
                    capnp::Error::failed(format!("no handle available for peer {peer_id}"))
                })?,
        };

        for attempt in 0..=1 {
            match Self::fetch_remote_summary_via_handle(
                &self.registry,
                &client,
                peer_id,
                include_details,
            )
            .await
            {
                Ok(summary) => return Ok(summary),
                Err(err) => {
                    if attempt == 1 {
                        return Err(err);
                    }

                    client = match self.registry.refresh_peer_handle(peer_id).await {
                        Some(new_client) => new_client,
                        None => return Err(err),
                    };
                }
            }
        }

        unreachable!("retry loop bounded to two iterations");
    }

    pub async fn free_slots<I>(
        &self,
        expected_version: u64,
        slots: I,
    ) -> Result<SchedulerSnapshot, SchedulerError>
    where
        I: IntoIterator<Item = SlotId>,
    {
        let slot_ids: BTreeSet<SlotId> = slots.into_iter().collect();
        if slot_ids.is_empty() {
            return self
                .state
                .load_full()
                .as_ref()
                .ok_or(SchedulerError::Uninitialized)
                .map(|state| state.snapshot.clone());
        }

        // Retry loop mirroring `reserve_slots` but toggling slot states back to free.
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

            let mut unknown = Vec::new();
            let mut not_reserved = Vec::new();
            for slot_id in &slot_ids {
                let Some(&idx) = current.index.get(slot_id) else {
                    unknown.push(*slot_id);
                    continue;
                };

                if matches!(current.snapshot.slots[idx].state, SlotState::Free) {
                    not_reserved.push(*slot_id);
                }
            }

            if !unknown.is_empty() {
                return Err(SchedulerError::UnknownSlots {
                    unknown,
                    snapshot: current.snapshot.clone(),
                });
            }

            if !not_reserved.is_empty() {
                return Err(SchedulerError::SlotsNotReserved {
                    slots: not_reserved,
                    snapshot: current.snapshot.clone(),
                });
            }

            let mut new_snapshot = current.snapshot.clone();
            for slot_id in &slot_ids {
                let idx = current.index[slot_id];
                new_snapshot.slots[idx].state = SlotState::Free;
            }

            new_snapshot.version = new_snapshot
                .version
                .checked_add(1)
                .expect("scheduler snapshot version overflow");

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

            return Ok(new_snapshot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::store::local_session_store::LocalSessionStore;
    use crate::store::peer_store::open_peers_store;
    use crate::store::scheduler_store::open_scheduler_store;
    use ::health::{Config as HealthConfig, HealthMonitor};
    use ed25519_dalek::SigningKey;
    use net::noise::NoiseKeys;
    use tempfile::tempdir;

    async fn make_scheduler() -> (Scheduler, tempfile::TempDir) {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("scheduler-test-{}.redb", Uuid::new_v4()));
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

        let health_monitor = HealthMonitor::new(HealthConfig::default());

        let registry = Registry::new(
            peers_store,
            session_store,
            SigningKey::from_bytes(&[0xA5; 32]),
            actor,
            health_monitor,
        );

        let scheduler = Scheduler::new(scheduler_store, registry, actor).expect("scheduler init");

        (scheduler, dir)
    }

    #[tokio::test]
    async fn init_slots_sets_free_state() {
        let (scheduler, _dir) = make_scheduler().await;
        let snapshot = scheduler
            .init_slots([
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024)),
                SlotSpec::new(3, SlotCapacity::new(1000, 1024 * 1024 * 1024)),
            ])
            .await
            .unwrap();
        assert_eq!(snapshot.version, 0);
        assert_eq!(snapshot.slots.len(), 3);
        assert!(
            snapshot
                .slots
                .iter()
                .all(|slot| matches!(slot.state, SlotState::Free))
        );
        assert_eq!(snapshot.slots[0].capacity.cpu_millis, 500);
        assert_eq!(snapshot.slots[0].capacity.memory_bytes, 512 * 1024 * 1024);

        let Some(current) = scheduler.snapshot().await else {
            panic!("missing snapshot");
        };
        assert_eq!(current.version, 0);
    }

    #[tokio::test]
    async fn reserve_slots_marks_slots() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([
                SlotSpec::new(10, SlotCapacity::new(1000, 1024 * 1024 * 1024)),
                SlotSpec::new(20, SlotCapacity::new(500, 512 * 1024 * 1024)),
            ])
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        let task = Uuid::new_v4();
        let snapshot = scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id: 10,
                    owner,
                    task_id: Some(task),
                }],
            )
            .await
            .unwrap();

        assert_eq!(snapshot.version, 1);
        let slot10 = snapshot
            .slots
            .iter()
            .find(|slot| slot.slot_id == 10)
            .expect("slot 10");
        match &slot10.state {
            SlotState::Reserved(res) => {
                assert_eq!(res.owner, owner);
                assert_eq!(res.task_id, Some(task));
            }
            _ => panic!("slot 10 not reserved"),
        }
    }

    #[tokio::test]
    async fn reserve_slots_conflict_returns_error() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                1,
                SlotCapacity::new(1000, 1024 * 1024 * 1024),
            )])
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id: 1,
                    owner,
                    task_id: None,
                }],
            )
            .await
            .unwrap();

        let err = scheduler
            .reserve_slots(
                1,
                vec![SlotReservationRequest {
                    slot_id: 1,
                    owner: Uuid::new_v4(),
                    task_id: None,
                }],
            )
            .await
            .expect_err("conflict expected");

        match err {
            SchedulerError::SlotsUnavailable {
                conflicts,
                snapshot,
            } => {
                assert_eq!(conflicts, vec![1]);
                assert_eq!(snapshot.version, 1);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let current = scheduler.snapshot().await.unwrap();
        assert_eq!(current.version, 1);
        assert!(matches!(current.slots[0].state, SlotState::Reserved(_)));
    }

    #[tokio::test]
    async fn free_slots_releases_reservations() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                5,
                SlotCapacity::new(1000, 1024 * 1024 * 1024),
            )])
            .await
            .unwrap();

        let owner = Uuid::new_v4();
        scheduler
            .reserve_slots(
                0,
                vec![SlotReservationRequest {
                    slot_id: 5,
                    owner,
                    task_id: None,
                }],
            )
            .await
            .unwrap();

        let snapshot = scheduler.free_slots(1, [5]).await.unwrap();
        assert_eq!(snapshot.version, 2);
        assert!(matches!(snapshot.slots[0].state, SlotState::Free));
    }

    #[tokio::test]
    async fn free_slots_unknown_slot_errors() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([SlotSpec::new(
                5,
                SlotCapacity::new(1000, 1024 * 1024 * 1024),
            )])
            .await
            .unwrap();

        let err = scheduler.free_slots(0, [9]).await.expect_err("unknown");
        match err {
            SchedulerError::UnknownSlots { unknown, .. } => {
                assert_eq!(unknown, vec![9]);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn reserve_slots_version_mismatch() {
        let (scheduler, _dir) = make_scheduler().await;
        scheduler
            .init_slots([
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024)),
            ])
            .await
            .unwrap();

        let err = scheduler
            .reserve_slots(
                5,
                vec![SlotReservationRequest {
                    slot_id: 1,
                    owner: Uuid::new_v4(),
                    task_id: None,
                }],
            )
            .await
            .expect_err("version mismatch");

        match err {
            SchedulerError::SnapshotMismatch {
                expected_version,
                current_version,
                ..
            } => {
                assert_eq!(expected_version, 5);
                assert_eq!(current_version, 0);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
