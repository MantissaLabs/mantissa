#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use mantissa_store::TableSet;
use mantissa_store::adapter::StoreMvRegAdapterSorted;
use mantissa_store::codec::TombstoneRecord;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::{CrdtMstStore, PageDigestRange};
use mantissa_store::uuid_key::UuidKey;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;

const MAX_OPS: usize = 64;
const MAX_BATCH_ENTRIES: usize = 16;
const MAX_VALUE_BYTES: usize = 64;
const KEY_SPACE: u8 = 32;

type Adapter = StoreMvRegAdapterSorted<UuidKey, String, Uuid>;
type Store = CrdtMstStore<Adapter, XXHash128, FuzzTables>;

struct FuzzTables;

impl TableSet for FuzzTables {
    const VALUES: &'static str = "values";
    const TOMBS: &'static str = "tombs";
    const TOMBS_BY_OBSERVED: &'static str = "tombs_by_observed";
    const META: &'static str = "meta";
}

#[derive(Arbitrary, Debug)]
struct MstStoreInput {
    ops: Vec<StoreOp>,
    sync_split: u8,
}

#[derive(Arbitrary, Debug)]
enum StoreOp {
    Upsert {
        key: u8,
        value: Vec<u8>,
    },
    UpsertMany {
        entries: Vec<GeneratedEntry>,
    },
    Remove {
        key: u8,
    },
    ApplyRemoteTombstone {
        key: u8,
        sequence: u64,
        origin_actor: [u8; 16],
    },
    Rebuild,
    SyncMirror,
}

#[derive(Arbitrary, Debug)]
struct GeneratedEntry {
    key: u8,
    value: Vec<u8>,
}

fuzz_target!(|input: MstStoreInput| {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Tokio runtime should build for MST store fuzzing");

    runtime.block_on(async {
        run_sequence(input).await;
    });
});

/// Drives bounded store operations and checks durable/MST sync invariants.
async fn run_sequence(input: MstStoreInput) {
    let actor = Uuid::from_u128(1);
    let (_source_dir, source_db, source) = open_store(actor);
    let (_replay_dir, _replay_db, replay) = open_store(actor);
    let (_mirror_dir, _mirror_db, mirror) = open_store(Uuid::from_u128(2));

    for op in input.ops.iter().take(MAX_OPS) {
        apply_store_op(&source, op).await;
        apply_store_op(&replay, op).await;

        if matches!(op, StoreOp::Rebuild) {
            assert_rebuild_preserves_root(&source).await;
            assert_rebuild_preserves_root(&replay).await;
        }

        if matches!(op, StoreOp::SyncMirror) {
            sync_full_delta_update_mst(&source, &mirror).await;
            assert_roots_equal(&source, &mirror).await;
            assert_store_has_no_active_tombstone_overlap(&mirror);
        }
    }

    assert_rebuild_preserves_root(&source).await;
    assert_rebuild_preserves_root(&replay).await;
    assert_reopened_rebuild_preserves_root(&source, source_db, actor).await;
    assert_roots_equal(&source, &replay).await;
    assert_page_ranges_are_deterministic(&source).await;
    assert_store_has_no_active_tombstone_overlap(&source);

    sync_full_delta_update_mst(&source, &mirror).await;
    assert_roots_equal(&source, &mirror).await;

    let (_session_dir, _session_db, session_mirror) = open_store(Uuid::from_u128(3));
    sync_full_delta_session(&source, &session_mirror, usize::from(input.sync_split)).await;
    assert_roots_equal(&source, &session_mirror).await;
    assert_store_has_no_active_tombstone_overlap(&session_mirror);
}

/// Applies one generated operation to a store.
async fn apply_store_op(store: &Store, op: &StoreOp) {
    match op {
        StoreOp::Upsert { key, value } => {
            store
                .upsert(&key_from_byte(*key), bounded_string(value))
                .await
                .expect("bounded store upsert should succeed");
        }
        StoreOp::UpsertMany { entries } => {
            let bounded = entries
                .iter()
                .take(MAX_BATCH_ENTRIES)
                .map(|entry| (key_from_byte(entry.key), bounded_string(&entry.value)))
                .collect::<Vec<_>>();
            store
                .upsert_many(bounded)
                .await
                .expect("bounded store batch upsert should succeed");
        }
        StoreOp::Remove { key } => {
            store
                .remove(&key_from_byte(*key))
                .await
                .expect("bounded store remove should succeed");
        }
        StoreOp::ApplyRemoteTombstone {
            key,
            sequence,
            origin_actor,
        } => {
            let sequence = sequence.saturating_add(1);
            let tombstone = TombstoneRecord::new(sequence, origin_actor.to_vec(), 0);
            store
                .apply_delta_chunk_update_mst(Vec::new(), vec![(key_from_byte(*key), tombstone)])
                .await
                .expect("bounded remote tombstone should apply");
        }
        StoreOp::Rebuild => {
            store
                .rebuild_mst_from_disk()
                .await
                .expect("store rebuild should succeed");
        }
        StoreOp::SyncMirror => {}
    }

    assert_store_has_no_active_tombstone_overlap(store);
}

/// Verifies rebuilding the in-memory MST from durable rows preserves the root.
async fn assert_rebuild_preserves_root(store: &Store) {
    let before = store.root_hex().await;
    store
        .rebuild_mst_from_disk()
        .await
        .expect("store rebuild should succeed");
    assert_eq!(store.root_hex().await, before);
    assert_eq!(store.root_digest().await.len(), 16);
}

/// Verifies a reopened store can rebuild the same root from shared durable rows.
async fn assert_reopened_rebuild_preserves_root(
    source: &Store,
    db: Arc<redb::Database>,
    actor: Uuid,
) {
    let expected = source.root_hex().await;
    let reopened = Store::open(db, actor).expect("reopened store should initialize");
    reopened
        .rebuild_mst_from_disk()
        .await
        .expect("reopened store rebuild should succeed");
    assert_eq!(reopened.root_hex().await, expected);
}

/// Verifies two stores expose the same current MST root.
async fn assert_roots_equal(left: &Store, right: &Store) {
    assert_eq!(left.root_hex().await, right.root_hex().await);
    assert_eq!(left.root_digest().await, right.root_digest().await);
}

/// Exports all source ranges and applies them through incremental MST updates.
async fn sync_full_delta_update_mst(source: &Store, target: &Store) {
    let ranges = source
        .page_range_summary()
        .await
        .expect("source page ranges should load");
    assert_page_ranges(&ranges);

    let (regs, tombs) = source
        .export_page_ranges_delta(&duplicated_ranges(&ranges))
        .expect("source delta export should succeed");
    assert_encoded_register_delta_is_well_formed(source, regs.clone());

    target
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .expect("full source delta should apply incrementally");
}

/// Exports all source ranges and applies them through a rebuild-on-commit session.
async fn sync_full_delta_session(source: &Store, target: &Store, split_seed: usize) {
    let ranges = source
        .page_range_summary()
        .await
        .expect("source page ranges should load");
    let (regs, tombs) = source
        .export_page_ranges_delta(&ranges)
        .expect("source delta export should succeed");

    let reg_split = split_seed % (regs.len().saturating_add(1));
    let tomb_split = split_seed % (tombs.len().saturating_add(1));
    let first_regs = regs[..reg_split].to_vec();
    let second_regs = regs[reg_split..].to_vec();
    let first_tombs = tombs[..tomb_split].to_vec();
    let second_tombs = tombs[tomb_split..].to_vec();

    let session = target.begin_delta_apply().await;
    session
        .apply_chunk(first_regs, first_tombs)
        .expect("first streamed delta chunk should apply");
    session
        .apply_chunk(second_regs, second_tombs)
        .expect("second streamed delta chunk should apply");
    session
        .commit()
        .await
        .expect("streamed delta session should rebuild target MST");
}

/// Verifies page range summaries are stable and structurally bounded.
async fn assert_page_ranges_are_deterministic(store: &Store) {
    let first = store
        .page_range_summary()
        .await
        .expect("first page range summary should load");
    let second = store
        .page_range_summary()
        .await
        .expect("second page range summary should load");
    assert_eq!(first, second);
    assert_page_ranges(&first);
}

/// Verifies no logical key has both an active register and a tombstone row.
fn assert_store_has_no_active_tombstone_overlap(store: &Store) {
    let (actives, tombs) = store.load_all().expect("store rows should load");
    let active_keys = actives
        .into_iter()
        .map(|(key, _)| key.as_ref().to_vec())
        .collect::<BTreeSet<_>>();

    for (key, _) in tombs {
        assert!(
            !active_keys.contains(key.as_ref()),
            "store key is both active and tombstoned"
        );
    }
}

/// Verifies exported register rows use stable UUID keys and non-empty row bytes.
fn assert_encoded_register_delta_is_well_formed(
    source: &Store,
    regs: Vec<(UuidKey, mantissa_store::mvreg::MvReg<String, Uuid>)>,
) {
    let encoded = source
        .encode_register_delta(regs)
        .expect("register delta encoding should succeed");
    for (key, reg) in encoded {
        assert_eq!(key.len(), 16);
        assert!(!reg.is_empty());
    }
}

/// Verifies page ranges have sorted key bounds and 128-bit hashes.
fn assert_page_ranges(ranges: &[PageDigestRange]) {
    for range in ranges {
        assert_eq!(range.start.len(), 16);
        assert_eq!(range.end.len(), 16);
        assert_eq!(range.hash.len(), 16);
        assert!(range.start <= range.end);
    }

    for pair in ranges.windows(2) {
        assert!(pair[0].start <= pair[1].start);
    }
}

/// Returns ranges with deliberate duplicates to exercise export deduplication.
fn duplicated_ranges(ranges: &[PageDigestRange]) -> Vec<PageDigestRange> {
    let mut duplicated = ranges.to_vec();
    if let Some(first) = ranges.first() {
        duplicated.push(first.clone());
    }
    duplicated
}

/// Opens one isolated Redb-backed fuzz store.
fn open_store(actor: Uuid) -> (TempDir, Arc<redb::Database>, Store) {
    let dir = tempfile::tempdir().expect("store fuzz tempdir should be created");
    let path = dir.path().join("mst.redb");
    let db = Arc::new(redb::Database::create(path).expect("store fuzz database should be created"));
    let store = Store::open(db.clone(), actor).expect("store fuzz database should open");
    (dir, db, store)
}

/// Maps arbitrary key bytes into a small deterministic UUID key space.
fn key_from_byte(byte: u8) -> UuidKey {
    let mut bytes = [0u8; 16];
    bytes[15] = byte % KEY_SPACE;
    UuidKey::try_from(&bytes[..]).expect("generated UUID key should be valid")
}

/// Returns a bounded ASCII value string from arbitrary bytes.
fn bounded_string(bytes: &[u8]) -> String {
    let bytes = bytes
        .iter()
        .copied()
        .take(MAX_VALUE_BYTES)
        .collect::<Vec<_>>();
    if bytes.is_empty() {
        return "v".to_string();
    }

    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}
