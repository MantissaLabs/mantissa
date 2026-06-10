#![no_main]

use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use mantissa::cluster::SUPPORTED_ROOT_SCHEMA_VERSION;
use mantissa::store::replicated::agents::open_agent_store;
use mantissa::store::replicated::cluster_views::ClusterViewDomainStoreInner;
use mantissa::store::replicated::jobs::open_job_store;
use mantissa::store::replicated::networks::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use mantissa::store::replicated::peers::open_peers_store;
use mantissa::store::replicated::registry::{
    EncodedRegisters, EncodedTombstones, REPLICATED_DOMAINS, ReplicatedStoreHandles,
    ReplicatedStoreRegistry, replicated_store_registry,
};
use mantissa::store::replicated::scheduler_digests::open_scheduler_digest_store;
use mantissa::store::replicated::secret_key_sync::open_secret_master_key_store;
use mantissa::store::replicated::secrets::open_secret_store;
use mantissa::store::replicated::services::open_service_store;
use mantissa::store::replicated::volumes::{open_volume_node_store, open_volume_spec_store};
use mantissa::store::replicated::workloads::open_workload_store;
use mantissa_protocol::sync::Domain;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_OPS: usize = 16;
const MAX_ROWS: usize = 8;
const MAX_KEY_BYTES: usize = 32;
const MAX_REGISTER_BYTES: usize = 512;
const MAX_ACTOR_BYTES: usize = 32;

#[derive(Arbitrary, Debug)]
struct DeltaInput {
    actor: [u8; 16],
    ops: Vec<DeltaOp>,
}

#[derive(Arbitrary, Debug)]
enum DeltaOp {
    Apply {
        domain: u8,
        registers: Vec<RegisterRow>,
        tombstones: Vec<TombstoneRow>,
    },
    Empty {
        domain: u8,
    },
    Rebuild {
        root_schema_seed: u8,
    },
}

#[derive(Arbitrary, Debug)]
struct RegisterRow {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct TombstoneRow {
    key: Vec<u8>,
    sequence: u64,
    origin_actor: Vec<u8>,
}

fuzz_target!(|input: DeltaInput| {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Tokio runtime should build for replicated delta fuzzing");

    runtime.block_on(async {
        run(input).await;
    });
});

/// Applies generated opaque sync deltas across replicated domains.
async fn run(input: DeltaInput) {
    let actor = Uuid::from_bytes(input.actor);
    let (_dir, registry) = open_registry(actor).await;

    for op in input.ops.iter().take(MAX_OPS) {
        match op {
            DeltaOp::Apply {
                domain,
                registers,
                tombstones,
            } => {
                apply_delta(
                    &registry,
                    domain_from_byte(*domain),
                    bounded_registers(registers),
                    bounded_tombstones(tombstones),
                )
                .await;
            }
            DeltaOp::Empty { domain } => {
                apply_delta(&registry, domain_from_byte(*domain), Vec::new(), Vec::new()).await;
            }
            DeltaOp::Rebuild { root_schema_seed } => {
                let version = if root_schema_seed & 1 == 0 {
                    SUPPORTED_ROOT_SCHEMA_VERSION
                } else {
                    SUPPORTED_ROOT_SCHEMA_VERSION.saturating_add(1)
                };
                let roots_before = domain_roots(&registry).await;
                let result = registry
                    .rebuild_msts_for_root_schema_version(version)
                    .await;
                if result.is_err() {
                    assert_eq!(domain_roots(&registry).await, roots_before);
                }
            }
        }
    }
}

/// Applies one delta and checks decode failure atomicity or successful replay idempotence.
async fn apply_delta(
    registry: &ReplicatedStoreRegistry,
    domain: Domain,
    registers: EncodedRegisters,
    tombstones: EncodedTombstones,
) {
    let entry = registry
        .require(domain)
        .expect("generated domain should be registered");
    let before = entry
        .store
        .root_digest_at_version(SUPPORTED_ROOT_SCHEMA_VERSION)
        .await
        .expect("domain root should load before delta");

    let result = entry
        .store
        .apply_delta_encoded(registers.clone(), tombstones.clone())
        .await;
    let after = entry
        .store
        .root_digest_at_version(SUPPORTED_ROOT_SCHEMA_VERSION)
        .await
        .expect("domain root should load after delta");

    if result.is_err() {
        assert_eq!(after, before, "failed delta must not mutate domain root");
        return;
    }

    entry
        .store
        .apply_delta_encoded(registers, tombstones)
        .await
        .expect("accepted delta should replay");
    let replayed = entry
        .store
        .root_digest_at_version(SUPPORTED_ROOT_SCHEMA_VERSION)
        .await
        .expect("domain root should load after replay");
    assert_eq!(replayed, after, "accepted delta replay must be idempotent");
}

/// Opens one full replicated-store registry backed by a temporary Redb database.
async fn open_registry(actor: Uuid) -> (TempDir, ReplicatedStoreRegistry) {
    let dir = tempfile::tempdir().expect("replicated delta fuzz tempdir should be created");
    let db_path = dir.path().join("replicated-delta.redb");
    let db = Arc::new(redb::Database::create(db_path).expect("fuzz database should open"));

    let handles = ReplicatedStoreHandles {
        peers: open_peers_store(db.clone(), actor).expect("open peers store"),
        workloads: open_workload_store(db.clone(), actor).expect("open workloads store"),
        services: open_service_store(db.clone(), actor).expect("open services store"),
        jobs: open_job_store(db.clone(), actor).expect("open jobs store"),
        agents: open_agent_store(db.clone(), actor).expect("open agents store"),
        secrets: open_secret_store(db.clone(), actor).expect("open secrets store"),
        secret_master_keys: open_secret_master_key_store(db.clone(), actor)
            .expect("open secret master key store"),
        networks: open_network_spec_store(db.clone(), actor).expect("open networks store"),
        network_peers: open_network_peer_store(db.clone(), actor)
            .expect("open network peers store"),
        network_attachments: open_network_attachment_store(db.clone(), actor)
            .expect("open network attachments store"),
        cluster_views: Arc::new(
            ClusterViewDomainStoreInner::open(db.clone(), actor)
                .expect("open cluster view domain store"),
        ),
        volumes: open_volume_spec_store(db.clone(), actor).expect("open volume specs store"),
        volume_nodes: open_volume_node_store(db.clone(), actor).expect("open volume nodes store"),
        scheduler_digests: open_scheduler_digest_store(db, actor)
            .expect("open scheduler digest store"),
    };
    let registry = replicated_store_registry(handles);
    registry
        .rebuild_msts_for_root_schema_version(SUPPORTED_ROOT_SCHEMA_VERSION)
        .await
        .expect("empty replicated stores should rebuild");
    (dir, registry)
}

/// Reads every replicated domain root in canonical order.
async fn domain_roots(registry: &ReplicatedStoreRegistry) -> Vec<[u8; 16]> {
    let mut roots = Vec::with_capacity(REPLICATED_DOMAINS.len());
    for domain in REPLICATED_DOMAINS {
        roots.push(
            registry
                .root_digest_at_version(domain, SUPPORTED_ROOT_SCHEMA_VERSION)
                .await
                .expect("domain root should load"),
        );
    }
    roots
}

/// Picks one replicated sync domain from a generated byte.
fn domain_from_byte(value: u8) -> Domain {
    REPLICATED_DOMAINS[usize::from(value) % REPLICATED_DOMAINS.len()]
}

/// Bounds generated opaque register rows.
fn bounded_registers(rows: &[RegisterRow]) -> EncodedRegisters {
    rows.iter()
        .take(MAX_ROWS)
        .map(|row| {
            (
                bounded_bytes(&row.key, MAX_KEY_BYTES),
                bounded_bytes(&row.value, MAX_REGISTER_BYTES),
            )
        })
        .collect()
}

/// Bounds generated opaque tombstone rows.
fn bounded_tombstones(rows: &[TombstoneRow]) -> EncodedTombstones {
    rows.iter()
        .take(MAX_ROWS)
        .map(|row| {
            (
                bounded_bytes(&row.key, MAX_KEY_BYTES),
                row.sequence,
                bounded_bytes(&row.origin_actor, MAX_ACTOR_BYTES),
            )
        })
        .collect()
}

/// Returns at most `limit` bytes from a generated field.
fn bounded_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
    bytes.iter().copied().take(limit).collect()
}
