use super::*;

use mantissa_store::codec::StoreValueCodec;

use std::sync::Arc;
use std::time::Duration;

use crate::config::RuntimeSchedulerConfig;
use crate::node::info::{Cpu, Memory};
use crate::store::local::LocalSessionStore;
use crate::store::replicated::peers::open_peers_store;
use crate::store::replicated::scheduler::open_scheduler_store;
use ::mantissa_health::HealthMonitor;
use ed25519_dalek::SigningKey;
use mantissa_net::noise::NoiseKeys;
use tempfile::tempdir;

/// Builds one synthetic node with fixed CPU and memory capacity so slot
/// derivation tests can assert allocatable scheduler output precisely.
fn make_test_node(logical_cpus: i32, memory_bytes: u64) -> crate::node::Node {
    let mut node = crate::node::Node::default();
    node.system_info.info.cpu_info = Some(Cpu {
        vendor: None,
        brand: None,
        codename: None,
        frequency: None,
        num_cores: logical_cpus,
        num_logical_cpus: logical_cpus,
        total_logical_cpus: Some(logical_cpus),
        l1_data_cache: None,
        l1_instruction_cache: None,
        l2_cache: None,
        l3_cache: None,
    });
    node.system_info.info.mem_info = Some(Memory {
        total: memory_bytes,
        free: memory_bytes,
        available: memory_bytes,
        used: 0,
        swap_total: 0,
        swap_used: 0,
        swap_free: 0,
    });
    node
}

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

    let health_monitor = HealthMonitor::new(actor);

    let registry = Registry::new(
        peers_store,
        session_store,
        SigningKey::from_bytes(&[0xA5; 32]),
        Arc::new(noise_keys),
        actor,
        health_monitor,
    );

    let scheduler = Scheduler::new(scheduler_store, registry, actor).expect("scheduler init");

    (scheduler, dir)
}

/// Builds one scheduler snapshot that exercises free, leased, and reserved resources.
fn sample_scheduler_snapshot() -> SchedulerSnapshot {
    let lease = LeaseReservation {
        lease_id: Uuid::new_v4(),
        coordinator_node_id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        expires_at_unix_ms: 1_776_000_000_001,
        group_id: Some(Uuid::new_v4()),
    };
    let reservation_owner = Uuid::new_v4();
    let reserved_task = Uuid::new_v4();

    SchedulerSnapshot {
        version: 9,
        slots: vec![
            ResourceSlot {
                slot_id: 1,
                capacity: SlotCapacity::new(500, 256 * 1024 * 1024, 0),
                state: SlotState::Free,
            },
            ResourceSlot {
                slot_id: 2,
                capacity: SlotCapacity::new(1_000, 512 * 1024 * 1024, 0),
                state: SlotState::Leased(lease.clone()),
            },
            ResourceSlot {
                slot_id: 3,
                capacity: SlotCapacity::new(2_000, 1024 * 1024 * 1024, 1),
                state: SlotState::Reserved(SlotReservation {
                    owner: reservation_owner,
                    task_id: Some(reserved_task),
                    group_id: Some(Uuid::new_v4()),
                }),
            },
        ],
        gpu_devices: vec![
            GpuDevice {
                device_id: "gpu-a".to_string(),
                index: 0,
                uuid: Some("GPU-a".to_string()),
                pci_bus_id: Some("0000:01:00.0".to_string()),
                name: "Test GPU A".to_string(),
                memory_total_bytes: 16 * 1024 * 1024 * 1024,
                state: GpuDeviceState::Leased(lease),
            },
            GpuDevice {
                device_id: "gpu-b".to_string(),
                index: 1,
                uuid: None,
                pci_bus_id: None,
                name: "Test GPU B".to_string(),
                memory_total_bytes: 24 * 1024 * 1024 * 1024,
                state: GpuDeviceState::Reserved(GpuDeviceReservation {
                    owner: reservation_owner,
                    task_id: None,
                    group_id: Some(Uuid::new_v4()),
                }),
            },
            GpuDevice {
                device_id: "gpu-c".to_string(),
                index: 2,
                uuid: Some("GPU-c".to_string()),
                pci_bus_id: Some("0000:03:00.0".to_string()),
                name: "Test GPU C".to_string(),
                memory_total_bytes: 32 * 1024 * 1024 * 1024,
                state: GpuDeviceState::Free,
            },
        ],
    }
}

/// Scheduler snapshots should round-trip through the Cap'n Proto store-value codec.
#[test]
fn store_value_codec_roundtrips_scheduler_snapshot() {
    let snapshot = sample_scheduler_snapshot();
    let encoded = snapshot
        .encode_store_value()
        .expect("encode scheduler snapshot");
    let decoded =
        SchedulerSnapshot::decode_store_value(&encoded).expect("decode scheduler snapshot");

    assert_eq!(decoded, snapshot);
}

/// Corrupt snapshots that encode oversized resource lists should fail before allocation.
#[test]
fn store_value_codec_rejects_oversized_scheduler_snapshot() {
    let oversized_snapshot = [
        0x02, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
        0xff, 0x02, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac,
        0xff, 0xff, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x45, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xea, 0xac, 0xac,
        0xac, 0xac, 0xac, 0xa4, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0x5a, 0xac, 0xac,
        0xac, 0xac, 0xac, 0xac, 0xac, 0xea, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xac, 0xea,
        0xac, 0xe5,
    ];

    let error = SchedulerSnapshot::decode_store_value(&oversized_snapshot)
        .expect_err("oversized scheduler snapshot should be rejected");

    assert!(error.to_string().contains("scheduler snapshot slots"));
}

/// Reopening the scheduler store should decode Cap'n Proto MVReg rows from Redb.
#[tokio::test]
async fn scheduler_store_reopens_capnp_rows() {
    let dir = tempdir().expect("tempdir");
    let db_path = dir
        .path()
        .join(format!("scheduler-reopen-{}.redb", Uuid::new_v4()));
    let db = Arc::new(redb::Database::create(db_path).expect("create db"));
    let actor = Uuid::new_v4();
    let key = UuidKey::from(actor);
    let snapshot = sample_scheduler_snapshot();

    {
        let store = open_scheduler_store(db.clone(), actor).expect("open scheduler store");
        store
            .upsert(&key, snapshot.clone())
            .await
            .expect("upsert scheduler snapshot");
    }

    let store = open_scheduler_store(db, actor).expect("reopen scheduler store");
    store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild scheduler MST");
    let reopened = store
        .get_snapshot(&key)
        .expect("lookup scheduler snapshot")
        .expect("scheduler snapshot present");

    assert_eq!(reopened.as_slice(), &[snapshot]);
}

#[test]
fn derive_slot_specs_applies_scheduler_reserve() {
    let node = make_test_node(4, 512 * 1024 * 1024);
    let specs = Scheduler::derive_slot_specs(
        &node,
        RuntimeSchedulerConfig {
            reserved_cpu_millis: 500,
            reserved_memory_bytes: 128 * 1024 * 1024,
        },
    );

    assert_eq!(specs.len(), 3);
    assert_eq!(
        specs
            .iter()
            .map(|slot| slot.capacity.cpu_millis)
            .sum::<u64>(),
        3_500
    );
    assert_eq!(
        specs
            .iter()
            .map(|slot| slot.capacity.memory_bytes)
            .sum::<u64>(),
        384 * 1024 * 1024
    );
    assert!(specs.iter().all(|slot| slot.capacity.memory_bytes > 0));
}

#[test]
fn derive_slot_specs_returns_empty_when_reserve_consumes_capacity() {
    let node = make_test_node(1, 128 * 1024 * 1024);
    let specs = Scheduler::derive_slot_specs(
        &node,
        RuntimeSchedulerConfig {
            reserved_cpu_millis: 2_000,
            reserved_memory_bytes: 256 * 1024 * 1024,
        },
    );

    assert!(specs.is_empty());
}

#[tokio::test]
async fn init_slots_sets_free_state() {
    let (scheduler, _dir) = make_scheduler().await;
    let snapshot = scheduler
        .init_slots([
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(3, SlotCapacity::new(1000, 1024 * 1024 * 1024, 0)),
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
            SlotSpec::new(10, SlotCapacity::new(1000, 1024 * 1024 * 1024, 0)),
            SlotSpec::new(20, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
        ])
        .await
        .unwrap();

    let owner = Uuid::new_v4();
    let task = Uuid::new_v4();
    let group = Uuid::new_v4();
    let snapshot = scheduler
        .reserve_slots(
            0,
            vec![SlotReservationRequest {
                slot_id: 10,
                owner,
                task_id: Some(task),
                group_id: Some(group),
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
            assert_eq!(res.group_id, Some(group));
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
            SlotCapacity::new(1000, 1024 * 1024 * 1024, 0),
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
                group_id: None,
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
                group_id: None,
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
            SlotCapacity::new(1000, 1024 * 1024 * 1024, 0),
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
                group_id: None,
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
            SlotCapacity::new(1000, 1024 * 1024 * 1024, 0),
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
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
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
                group_id: None,
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

#[tokio::test]
async fn prepare_task_leases_prepares_exact_bindings() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(3, SlotCapacity::new(1_000, 1024 * 1024 * 1024, 0)),
        ])
        .await
        .unwrap();

    let task_a = Uuid::new_v4();
    let task_b = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_leases(
            Uuid::new_v4(),
            30_000,
            vec![
                TaskLeaseIntent {
                    task_id: task_a,
                    cpu_millis: 1_500,
                    memory_bytes: 1536 * 1024 * 1024,
                    gpu_count: 0,
                },
                TaskLeaseIntent {
                    task_id: task_b,
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
            ],
        )
        .await
        .unwrap();

    assert_eq!(prepared.leases.len(), 2);
    assert_eq!(prepared.leases[0].task_id, task_a);
    assert_eq!(prepared.leases[0].slot_ids, vec![1, 3]);
    assert_eq!(prepared.leases[1].task_id, task_b);
    assert_eq!(prepared.leases[1].slot_ids, vec![2]);

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 1);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Leased(_)))
    );
}

#[tokio::test]
async fn prepare_task_leases_is_atomic_on_failure() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
        ])
        .await
        .unwrap();

    let err = scheduler
        .prepare_task_leases(
            Uuid::new_v4(),
            30_000,
            vec![
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 1_500,
                    memory_bytes: 1536 * 1024 * 1024,
                    gpu_count: 0,
                },
            ],
        )
        .await
        .expect_err("batch should fail atomically");

    match err {
        SchedulerError::InsufficientResources { snapshot, .. } => {
            assert_eq!(snapshot.version, 0);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 0);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
}

#[tokio::test]
async fn prepare_task_leases_returns_gpu_bindings() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [
                GpuDeviceSpec::new(
                    "gpu-a",
                    0,
                    Some("gpu-a".to_string()),
                    Some("0000:01:00.0".to_string()),
                    "GPU A",
                    16 * 1024 * 1024 * 1024,
                ),
                GpuDeviceSpec::new(
                    "gpu-b",
                    1,
                    Some("gpu-b".to_string()),
                    Some("0000:02:00.0".to_string()),
                    "GPU B",
                    16 * 1024 * 1024 * 1024,
                ),
            ],
        )
        .await
        .unwrap();

    let task_id = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_leases(
            Uuid::new_v4(),
            30_000,
            vec![TaskLeaseIntent {
                task_id,
                cpu_millis: 100,
                memory_bytes: 128 * 1024 * 1024,
                gpu_count: 2,
            }],
        )
        .await
        .unwrap();

    assert_eq!(prepared.leases.len(), 1);
    assert_eq!(prepared.leases[0].task_id, task_id);
    assert_eq!(prepared.leases[0].slot_ids, vec![1]);
    assert_eq!(
        prepared.leases[0].gpu_device_ids,
        vec!["gpu-a".to_string(), "gpu-b".to_string()]
    );

    let snapshot = scheduler.snapshot().await.unwrap();
    assert!(
        snapshot
            .gpu_devices
            .iter()
            .all(|device| matches!(device.state, GpuDeviceState::Leased(_)))
    );
}

#[tokio::test]
async fn prepare_task_leases_rejects_missing_resource_request() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 512 * 1024 * 1024, 0),
        )])
        .await
        .unwrap();

    let err = scheduler
        .prepare_task_leases(
            Uuid::new_v4(),
            30_000,
            vec![TaskLeaseIntent {
                task_id: Uuid::new_v4(),
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
            }],
        )
        .await
        .expect_err("zero CPU and memory request must fail");

    assert!(
        matches!(err, SchedulerError::InsufficientResources { .. }),
        "unexpected error: {err:?}"
    );
    let snapshot = scheduler.snapshot().await.unwrap();
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
}

#[tokio::test]
async fn commit_task_lease_promotes_resources_to_reserved() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_leases(
            coordinator,
            30_000,
            vec![TaskLeaseIntent {
                task_id,
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 1,
            }],
        )
        .await
        .unwrap();
    let lease = &prepared.leases[0];

    let snapshot = scheduler
        .commit_task_lease(
            lease.lease_id,
            coordinator,
            task_id,
            &lease.slot_ids,
            &lease.gpu_device_ids,
        )
        .await
        .unwrap();

    assert_eq!(snapshot.version, 2);
    assert!(snapshot.slots.iter().all(|slot| matches!(
        &slot.state,
        SlotState::Reserved(SlotReservation {
            owner,
            task_id: Some(owner_task_id),
            ..
        }) if *owner == scheduler.store_key.to_uuid() && *owner_task_id == task_id
    )));
    assert!(snapshot.gpu_devices.iter().all(|device| matches!(
        &device.state,
        GpuDeviceState::Reserved(GpuDeviceReservation {
            owner,
            task_id: Some(owner_task_id),
            ..
        }) if *owner == scheduler.store_key.to_uuid() && *owner_task_id == task_id
    )));
}

#[tokio::test]
async fn abort_task_leases_releases_prepared_resources() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_leases(
            coordinator,
            30_000,
            vec![TaskLeaseIntent {
                task_id,
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 1,
            }],
        )
        .await
        .unwrap();
    let lease = &prepared.leases[0];

    let snapshot = scheduler
        .abort_task_leases(
            coordinator,
            vec![AbortTaskLeaseIntent {
                lease_id: lease.lease_id,
                task_id,
            }],
        )
        .await
        .unwrap();

    assert_eq!(snapshot.version, 2);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
    assert!(
        snapshot
            .gpu_devices
            .iter()
            .all(|device| matches!(device.state, GpuDeviceState::Free))
    );
}

#[tokio::test]
async fn prepare_task_lease_group_consumes_capacity() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
        ])
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
            ],
        )
        .await
        .unwrap();

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 1);
    assert!(snapshot.slots.iter().all(|slot| matches!(
        &slot.state,
        SlotState::Leased(LeaseReservation {
            coordinator_node_id,
            group_id: Some(lease_group_id),
            ..
        }) if *coordinator_node_id == coordinator && *lease_group_id == group_id
    )));

    let err = scheduler
        .prepare_task_leases(
            Uuid::new_v4(),
            30_000,
            vec![TaskLeaseIntent {
                task_id: Uuid::new_v4(),
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 0,
            }],
        )
        .await
        .expect_err("group leases should consume capacity");

    match err {
        SchedulerError::InsufficientResources { snapshot, .. } => {
            assert_eq!(snapshot.version, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn prepare_task_lease_group_replaces_stale_same_group_leases() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    let first_task = Uuid::new_v4();
    let first = scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![TaskLeaseIntent {
                task_id: first_task,
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 1,
            }],
        )
        .await
        .unwrap();

    let second_task = Uuid::new_v4();
    let second = scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![TaskLeaseIntent {
                task_id: second_task,
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 1,
            }],
        )
        .await
        .unwrap();

    assert_ne!(first.leases[0].lease_id, second.leases[0].lease_id);
    assert_eq!(second.leases[0].task_id, second_task);
    assert_eq!(second.leases[0].gpu_device_ids, vec!["gpu-a".to_string()]);

    let err = scheduler
        .commit_task_lease_group(group_id, coordinator, &first.leases)
        .await
        .expect_err("superseded group leases must not commit");
    match err {
        SchedulerError::LeaseGroupMismatch { snapshot, .. } => {
            assert_eq!(snapshot.version, 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let snapshot = scheduler
        .commit_task_lease_group(group_id, coordinator, &second.leases)
        .await
        .unwrap();
    assert_eq!(snapshot.version, 3);
    assert!(matches!(
        &snapshot.slots[0].state,
        SlotState::Reserved(SlotReservation {
            task_id: Some(task_id),
            group_id: Some(reservation_group_id),
            ..
        }) if *task_id == second_task && *reservation_group_id == group_id
    ));
    assert!(matches!(
        &snapshot.gpu_devices[0].state,
        GpuDeviceState::Reserved(GpuDeviceReservation {
            task_id: Some(task_id),
            group_id: Some(reservation_group_id),
            ..
        }) if *task_id == second_task && *reservation_group_id == group_id
    ));
}

#[tokio::test]
async fn prepare_exact_task_lease_group_replaces_stale_same_group_leases() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    let first_task = Uuid::new_v4();
    let first = scheduler
        .prepare_exact_task_lease_group(
            0,
            coordinator,
            group_id,
            30_000,
            vec![ExactTaskLeaseIntent {
                task_id: first_task,
                slot_ids: vec![1],
                gpu_device_ids: vec!["gpu-a".to_string()],
            }],
        )
        .await
        .unwrap();

    let second_task = Uuid::new_v4();
    let second = scheduler
        .prepare_exact_task_lease_group(
            1,
            coordinator,
            group_id,
            30_000,
            vec![ExactTaskLeaseIntent {
                task_id: second_task,
                slot_ids: vec![1],
                gpu_device_ids: vec!["gpu-a".to_string()],
            }],
        )
        .await
        .unwrap();

    assert_ne!(first.leases[0].lease_id, second.leases[0].lease_id);
    assert_eq!(second.leases[0].task_id, second_task);
    assert_eq!(second.leases[0].gpu_device_ids, vec!["gpu-a".to_string()]);

    let snapshot = scheduler
        .commit_task_lease_group(group_id, coordinator, &second.leases)
        .await
        .unwrap();
    assert_eq!(snapshot.version, 3);
    assert!(matches!(
        &snapshot.slots[0].state,
        SlotState::Reserved(SlotReservation {
            task_id: Some(task_id),
            group_id: Some(reservation_group_id),
            ..
        }) if *task_id == second_task && *reservation_group_id == group_id
    ));
    assert!(matches!(
        &snapshot.gpu_devices[0].state,
        GpuDeviceState::Reserved(GpuDeviceReservation {
            task_id: Some(task_id),
            group_id: Some(reservation_group_id),
            ..
        }) if *task_id == second_task && *reservation_group_id == group_id
    ));
}

#[tokio::test]
async fn abort_task_lease_group_releases_all_resources() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![TaskLeaseIntent {
                task_id: Uuid::new_v4(),
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 1,
            }],
        )
        .await
        .unwrap();

    let snapshot = scheduler
        .abort_task_lease_group(coordinator, group_id)
        .await
        .unwrap();

    assert_eq!(snapshot.version, 2);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
    assert!(
        snapshot
            .gpu_devices
            .iter()
            .all(|device| matches!(device.state, GpuDeviceState::Free))
    );
}

#[tokio::test]
async fn abort_task_lease_group_releases_committed_group_resources() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [SlotSpec::new(
                1,
                SlotCapacity::new(500, 512 * 1024 * 1024, 0),
            )],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![TaskLeaseIntent {
                task_id: Uuid::new_v4(),
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 1,
            }],
        )
        .await
        .unwrap();
    scheduler
        .commit_task_lease_group(group_id, coordinator, &prepared.leases)
        .await
        .unwrap();

    let snapshot = scheduler
        .abort_task_lease_group(coordinator, group_id)
        .await
        .unwrap();

    assert_eq!(snapshot.version, 3);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
    assert!(
        snapshot
            .gpu_devices
            .iter()
            .all(|device| matches!(device.state, GpuDeviceState::Free))
    );
}

#[tokio::test]
async fn commit_task_lease_group_promotes_resources_to_group_reservations() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_resources(
            [
                SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
                SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            ],
            [GpuDeviceSpec::new(
                "gpu-a",
                0,
                Some("gpu-a".to_string()),
                Some("0000:01:00.0".to_string()),
                "GPU A",
                16 * 1024 * 1024 * 1024,
            )],
        )
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    let cpu_task = Uuid::new_v4();
    let gpu_task = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![
                TaskLeaseIntent {
                    task_id: cpu_task,
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
                TaskLeaseIntent {
                    task_id: gpu_task,
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 1,
                },
            ],
        )
        .await
        .unwrap();

    let snapshot = scheduler
        .commit_task_lease_group(group_id, coordinator, &prepared.leases)
        .await
        .unwrap();

    assert_eq!(snapshot.version, 2);
    assert!(snapshot.slots.iter().all(|slot| matches!(
        &slot.state,
        SlotState::Reserved(SlotReservation {
            owner,
            task_id: Some(task_id),
            group_id: Some(reservation_group_id),
        }) if *owner == scheduler.store_key.to_uuid()
            && *reservation_group_id == group_id
            && (*task_id == cpu_task || *task_id == gpu_task)
    )));
    assert!(snapshot.gpu_devices.iter().all(|device| matches!(
        &device.state,
        GpuDeviceState::Reserved(GpuDeviceReservation {
            owner,
            task_id: Some(task_id),
            group_id: Some(reservation_group_id),
        }) if *owner == scheduler.store_key.to_uuid()
            && *reservation_group_id == group_id
            && *task_id == gpu_task
    )));
}

#[tokio::test]
async fn commit_task_lease_group_rejects_partial_group() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
        ])
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
            ],
        )
        .await
        .unwrap();

    let err = scheduler
        .commit_task_lease_group(group_id, coordinator, &prepared.leases[..1])
        .await
        .expect_err("partial group commit should fail");

    match err {
        SchedulerError::LeaseGroupMismatch {
            group_id: failed_group_id,
            snapshot,
        } => {
            assert_eq!(failed_group_id, group_id);
            assert_eq!(snapshot.version, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 1);
    assert!(snapshot.slots.iter().all(|slot| matches!(
        &slot.state,
        SlotState::Leased(LeaseReservation {
            group_id: Some(lease_group_id),
            ..
        }) if *lease_group_id == group_id
    )));
}

#[tokio::test]
async fn reap_expired_leases_releases_prepared_group() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([
            SlotSpec::new(1, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
            SlotSpec::new(2, SlotCapacity::new(500, 512 * 1024 * 1024, 0)),
        ])
        .await
        .unwrap();

    let prepared = scheduler
        .prepare_task_lease_group(
            Uuid::new_v4(),
            Uuid::new_v4(),
            0,
            vec![
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
                TaskLeaseIntent {
                    task_id: Uuid::new_v4(),
                    cpu_millis: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    gpu_count: 0,
                },
            ],
        )
        .await
        .unwrap();

    let expired = scheduler.reap_expired_leases().await.unwrap();
    let expired = expired
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let expected = prepared
        .leases
        .iter()
        .map(|lease| lease.lease_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(expired, expected);

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 2);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
}

#[tokio::test]
async fn reap_expired_leases_keeps_committed_group_reservations() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([SlotSpec::new(
            1,
            SlotCapacity::new(500, 512 * 1024 * 1024, 0),
        )])
        .await
        .unwrap();

    let coordinator = Uuid::new_v4();
    let group_id = Uuid::new_v4();
    let task_id = Uuid::new_v4();
    let prepared = scheduler
        .prepare_task_lease_group(
            coordinator,
            group_id,
            30_000,
            vec![TaskLeaseIntent {
                task_id,
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 0,
            }],
        )
        .await
        .unwrap();
    scheduler
        .commit_task_lease_group(group_id, coordinator, &prepared.leases)
        .await
        .unwrap();

    let expired = scheduler.reap_expired_leases().await.unwrap();
    assert!(expired.is_empty());

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 2);
    assert!(matches!(
        &snapshot.slots[0].state,
        SlotState::Reserved(SlotReservation {
            owner,
            task_id: Some(reserved_task_id),
            group_id: Some(reservation_group_id),
        }) if *owner == scheduler.store_key.to_uuid()
            && *reserved_task_id == task_id
            && *reservation_group_id == group_id
    ));
}

#[tokio::test]
async fn reap_expired_leases_releases_stale_capacity() {
    let (scheduler, _dir) = make_scheduler().await;
    scheduler
        .init_slots([SlotSpec::new(
            1,
            SlotCapacity::new(500, 512 * 1024 * 1024, 0),
        )])
        .await
        .unwrap();

    let prepared = scheduler
        .prepare_task_leases(
            Uuid::new_v4(),
            1,
            vec![TaskLeaseIntent {
                task_id: Uuid::new_v4(),
                cpu_millis: 500,
                memory_bytes: 512 * 1024 * 1024,
                gpu_count: 0,
            }],
        )
        .await
        .unwrap();
    let lease = &prepared.leases[0];

    tokio::time::sleep(Duration::from_millis(5)).await;

    let expired = scheduler.reap_expired_leases().await.unwrap();
    assert_eq!(expired, vec![lease.lease_id]);

    let snapshot = scheduler.snapshot().await.unwrap();
    assert_eq!(snapshot.version, 2);
    assert!(
        snapshot
            .slots
            .iter()
            .all(|slot| matches!(slot.state, SlotState::Free))
    );
}
