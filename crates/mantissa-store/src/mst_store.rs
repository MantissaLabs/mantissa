//! CRDT + Merkle Search Tree backed store with tombstones.
//!
//! - Durable per-key CRDT registers in redb.
//! - Durable tombstones for deletions.
//! - In-memory Merkle Search Tree (MST) over (Key, Entry<Snapshot>).
//!
//! This module exposes fast range-based delta export/import primitives to power
//! anti-entropy sync between peers.
//!
//! Tombstones have two durable representations:
//! - `T::tombs()` is the primary `key -> TombstoneRecord` lookup used by sync,
//!   merge, rebuild, and range export.
//! - `T::tombs_by_observed()` is a secondary age index keyed by
//!   `observed_at_unix_ms || key`, used later by GC to scan old tombstones
//!   without walking every deleted key.
//!
//! Any code path that creates, replaces, or removes a tombstone must keep those
//! two tables in sync before updating the in-memory MST.

// base64 used only in debug helpers/tests; prefer fully-qualified calls to avoid unused imports.
use merkle_search_tree::digest::Hasher as MstHasher;
use merkle_search_tree::{MerkleSearchTree, builder::Builder};
use redb::{ReadableDatabase, ReadableTable, Table};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{hash::Hash, io, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

use crate::adapter::RegAdapter;
use crate::codec::{TombstoneRecord, decode_tombstone_row, encode_tombstone_row};
use crate::error::Error;
use crate::gc::{GcBarrier, StoreGcPolicy, StoreGcReport};
use crate::table_set::TableSet;

/// Default semantic root-schema version used until the caller commits a newer one.
const DEFAULT_ROOT_SCHEMA_VERSION: u32 = 1;

/// Prefix for per-origin tombstone prune frontier entries in the metadata table.
const TOMB_PRUNED_META_PREFIX: &str = "tomb_pruned/";

/// Value stored in each MST leaf.
///
/// Active leaves carry a CRDT snapshot. Deleted leaves carry the tombstone sequence and
/// origin actor so two same-sequence tombstones from different actors do not hash as equal
/// before their metadata has converged.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry<S> {
    Active(S),
    Deleted { ts: u64, origin_actor: Vec<u8> },
}

/// List of `(Key, Snapshot)` pairs.
pub type Snapshots<K, S> = Vec<(K, S)>;

/// List of `(Key, Reg)` pairs.
pub type Registers<K, R> = Vec<(K, R)>;

/// List of `(Key, tombstone metadata)` pairs.
pub type Tombstones<K> = Vec<(K, TombstoneRecord)>;

/// List of `(origin actor bytes, highest safely pruned tombstone sequence)` pairs.
pub type TombstonePruneFrontiers = Vec<(Vec<u8>, u64)>;

/// Tuple of `(Snapshots, Tombstones)` returned by bulk loaders.
pub type SnapshotsAndTombs<K, S> = (Snapshots<K, S>, Tombstones<K>);

/// Tuple of `(Registers, Tombstones)` returned by delta exporters.
pub type RegistersAndTombs<K, R> = (Registers<K, R>, Tombstones<K>);

/// Candidate tombstone age-index row selected for one GC pass.
struct TombstoneGcCandidate {
    index_key: Vec<u8>,
    key_bytes: Vec<u8>,
}

/// Candidate register row selected for one compaction pass.
struct RegisterCompactionCandidate<C: RegAdapter> {
    key_bytes: Vec<u8>,
    original_reg_bytes: Vec<u8>,
    key: C::Key,
    reg: C::Reg,
    snapshot: C::Snapshot,
}

/// In-memory Merkle Search Tree keyed by CRDT register keys for a given adapter.
type InMemoryMerkleSearchTree<C, H> =
    MerkleSearchTree<<C as RegAdapter>::Key, Entry<<C as RegAdapter>::Snapshot>, H>;

/// Shared handle to the in-memory MST guarded by a Tokio `RwLock`.
type SharedInMemoryMerkleSearchTree<C, H> = Arc<RwLock<InMemoryMerkleSearchTree<C, H>>>;

// Canonical hashing: tag byte + payload in a fixed-endian encoding.
// IMPORTANT: The hashing of snapshots must be stable/canonical.
impl<S> Hash for Entry<S>
where
    S: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Entry::Active(s) => {
                state.write_u8(0);
                s.hash(state);
            }
            Entry::Deleted { ts, origin_actor } => {
                state.write_u8(1);
                state.write_u64(*ts);
                origin_actor.hash(state);
            }
        }
    }
}

/// Summary of an MST page: the inclusive [start, end] key bounds (raw bytes) and the page digest.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PageDigestRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub hash: Vec<u8>,
}

#[inline]
fn into_err<E: Into<Error>>(e: E) -> Box<Error> {
    Box::new(e.into())
}

/// Returns the current wall-clock time as Unix milliseconds for local GC metadata.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Builds the age-index key for one tombstone row.
///
/// Big-endian time keeps Redb range scans in chronological order. The raw key suffix makes
/// the index unique even when many tombstones are observed in the same millisecond.
fn tombstone_observed_index_key(key_bytes: &[u8], record: &TombstoneRecord) -> Vec<u8> {
    let mut out = Vec::with_capacity(std::mem::size_of::<u64>() + key_bytes.len());
    out.extend_from_slice(&record.observed_at_unix_ms.to_be_bytes());
    out.extend_from_slice(key_bytes);
    out
}

/// Splits an observed-time index key into `(observed_at_unix_ms, raw key bytes)`.
fn tombstone_observed_index_parts(index_key: &[u8]) -> crate::Result<(u64, &[u8])> {
    let timestamp_width = std::mem::size_of::<u64>();
    if index_key.len() < timestamp_width {
        return Err(Box::new(Error::Other(format!(
            "tombstone observed index key is too short: {} bytes",
            index_key.len()
        ))));
    }

    let mut timestamp = [0u8; 8];
    timestamp.copy_from_slice(&index_key[..timestamp_width]);
    Ok((u64::from_be_bytes(timestamp), &index_key[timestamp_width..]))
}

/// Builds the metadata key for one origin actor's tombstone prune frontier.
///
/// The frontier records the highest origin-local tombstone sequence this node has
/// deliberately forgotten. Future inbound tombstones at or below that sequence can be
/// ignored because accepting them would recreate GCed delete markers.
fn tombstone_prune_frontier_key(origin_actor: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut key = String::with_capacity(TOMB_PRUNED_META_PREFIX.len() + origin_actor.len() * 2);
    key.push_str(TOMB_PRUNED_META_PREFIX);
    for byte in origin_actor {
        key.push(char::from(HEX[(byte >> 4) as usize]));
        key.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    key
}

/// Decodes one hexadecimal actor nibble stored in a prune-frontier metadata key.
fn decode_actor_hex_nibble(raw: u8) -> crate::Result<u8> {
    match raw {
        b'0'..=b'9' => Ok(raw - b'0'),
        b'a'..=b'f' => Ok(raw - b'a' + 10),
        b'A'..=b'F' => Ok(raw - b'A' + 10),
        _ => Err(Box::new(Error::Other(format!(
            "invalid tombstone prune frontier actor hex byte: {raw}"
        )))),
    }
}

/// Extracts origin actor bytes from one prune-frontier metadata key.
fn tombstone_prune_frontier_origin_actor(meta_key: &str) -> crate::Result<Option<Vec<u8>>> {
    let Some(encoded_actor) = meta_key.strip_prefix(TOMB_PRUNED_META_PREFIX) else {
        return Ok(None);
    };
    if encoded_actor.len() % 2 != 0 {
        return Err(Box::new(Error::Other(format!(
            "invalid tombstone prune frontier actor length: {}",
            encoded_actor.len()
        ))));
    }

    let mut actor = Vec::with_capacity(encoded_actor.len() / 2);
    for pair in encoded_actor.as_bytes().chunks_exact(2) {
        let high = decode_actor_hex_nibble(pair[0])?;
        let low = decode_actor_hex_nibble(pair[1])?;
        actor.push((high << 4) | low);
    }
    Ok(Some(actor))
}

/// Advances one prune frontier inside an existing Redb write transaction.
fn advance_tombstone_prune_frontier_in_meta(
    meta: &mut Table<'_, &'static str, u64>,
    origin_actor: &[u8],
    sequence: u64,
) -> crate::Result<()> {
    if sequence == 0 {
        return Ok(());
    }

    let frontier_key = tombstone_prune_frontier_key(origin_actor);
    let current = meta
        .get(frontier_key.as_str())
        .map_err(into_err)?
        .map(|row| row.value())
        .unwrap_or(0);

    if sequence > current {
        meta.insert(frontier_key.as_str(), &sequence)
            .map_err(into_err)?;
    }

    Ok(())
}

/// CRDT + MST store. Parameterized by:
/// - `C`: a register adapter (MVReg, ORSWOT, etc.)
/// - `H`: the MST hasher
/// - `T`: the redb table set (values/tombs/meta)
pub struct CrdtMstStore<C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]> + std::fmt::Debug,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    db: Arc<redb::Database>,
    actor: C::Actor,
    mst: SharedInMemoryMerkleSearchTree<C, H>,
    // Serializes each durable mutation with the MST update/rebuild that makes
    // the anti-entropy root reflect that durable state. Redb already serializes
    // write transactions; this gate covers the post-commit in-memory index step.
    mutation_gate: Mutex<()>,
    root_schema_version: AtomicU32,
    change_clock: AtomicU64,
    preserve_local_tombs: bool,
    _tables: std::marker::PhantomData<T>,
}

impl<C, H, T> CrdtMstStore<C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]> + std::fmt::Debug,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    /// Create a builder to customize MST options (e.g., hasher).
    pub fn builder(db: Arc<redb::Database>, actor: C::Actor) -> StoreBuilder<C, H, T> {
        StoreBuilder::<C, H, T> {
            db,
            actor,
            hasher: None,
            preserve_local_tombs: false,
            _tables: std::marker::PhantomData,
        }
    }

    /// Start a delta-apply session. Apply one or more chunks, then call `commit()`.
    ///
    /// The session holds the store mutation gate for its full lifetime because
    /// chunk application mutates Redb without updating the MST until commit.
    /// Without that gate, another writer could rebuild or patch the MST from an
    /// interleaved durable state and leave the anti-entropy root stale.
    pub async fn begin_delta_apply(&self) -> DeltaApplySession<'_, C, H, T> {
        let mutation = self.mutation_gate.lock().await;
        DeltaApplySession {
            store: self,
            finalize: FinalizeStrategy::Rebuild,
            _mutation: mutation,
        }
    }

    /// Open (or initialize) the underlying tables and create an empty in-memory MST.
    pub fn open(db: Arc<redb::Database>, actor: C::Actor) -> crate::Result<Self> {
        Self::builder(db, actor).build()
    }

    /// Return whether a register value exists for key `k`.
    pub fn exists(&self, k: &C::Key) -> crate::Result<bool> {
        let r = self.db.begin_read().map_err(into_err)?;
        let t = r.open_table(T::values()).map_err(into_err)?;
        Ok(t.get(Self::encode_key(k).as_slice())
            .map_err(into_err)?
            .is_some())
    }

    /// Return whether a tombstone currently exists for key `k`.
    pub fn has_tombstone(&self, k: &C::Key) -> crate::Result<bool> {
        let r = self.db.begin_read().map_err(into_err)?;
        let t = r.open_table(T::tombs()).map_err(into_err)?;
        Ok(t.get(Self::encode_key(k).as_slice())
            .map_err(into_err)?
            .is_some())
    }

    /// Returns the highest tombstone sequence deliberately forgotten for an origin actor.
    ///
    /// GC advances this frontier after it has purged durable tombstones from one
    /// origin. Future inbound tombstones at or below the frontier are stale and
    /// must not recreate the deleted rows.
    pub fn tombstone_prune_frontier(&self, origin_actor: &[u8]) -> crate::Result<u64> {
        let r = self.db.begin_read().map_err(into_err)?;
        let meta = r.open_table(T::meta()).map_err(into_err)?;
        let frontier_key = tombstone_prune_frontier_key(origin_actor);
        Ok(meta
            .get(frontier_key.as_str())
            .map_err(into_err)?
            .map(|row| row.value())
            .unwrap_or(0))
    }

    /// Loads all durable tombstone prune frontiers for sync with slower peers.
    pub fn load_tombstone_prune_frontiers(&self) -> crate::Result<TombstonePruneFrontiers> {
        let r = self.db.begin_read().map_err(into_err)?;
        let meta = r.open_table(T::meta()).map_err(into_err)?;
        let mut frontiers = Vec::new();
        for row in meta.iter().map_err(into_err)? {
            let (key, sequence) = row.map_err(into_err)?;
            let Some(origin_actor) = tombstone_prune_frontier_origin_actor(key.value())? else {
                continue;
            };
            let sequence = sequence.value();
            if sequence > 0 {
                frontiers.push((origin_actor, sequence));
            }
        }
        Ok(frontiers)
    }

    /// Applies peer prune frontiers and drops local tombstones proven obsolete by them.
    ///
    /// A peer only advertises a frontier after its own barrier allowed pruning, so adopting
    /// that frontier lets nodes that missed the equal-root pruning window catch up without
    /// reintroducing already-forgotten delete markers.
    pub async fn apply_tombstone_prune_frontiers(
        &self,
        frontiers: TombstonePruneFrontiers,
    ) -> crate::Result<usize> {
        let mut frontier_by_origin: HashMap<Vec<u8>, u64> = HashMap::new();
        for (origin_actor, sequence) in frontiers {
            if sequence == 0 {
                continue;
            }
            frontier_by_origin
                .entry(origin_actor)
                .and_modify(|current| *current = (*current).max(sequence))
                .or_insert(sequence);
        }
        if frontier_by_origin.is_empty() {
            return Ok(0);
        }

        let _mutation = self.mutation_gate.lock().await;
        let candidates = {
            let r = self.db.begin_read().map_err(into_err)?;
            let tombs = r.open_table(T::tombs()).map_err(into_err)?;
            let mut candidates = Vec::new();
            for row in tombs.iter().map_err(into_err)? {
                let (key, tombstone) = row.map_err(into_err)?;
                let record = Self::decode_tombstone(tombstone.value())?;
                if frontier_by_origin
                    .get(&record.origin_actor)
                    .is_some_and(|frontier| record.sequence <= *frontier)
                {
                    candidates.push(key.value().to_vec());
                }
            }
            candidates
        };

        let w = self.db.begin_write().map_err(into_err)?;
        let mut pruned = 0usize;
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let mut meta = w.open_table(T::meta()).map_err(into_err)?;

            for key_bytes in &candidates {
                let record = {
                    let Some(row) = tombs.get(key_bytes.as_slice()).map_err(into_err)? else {
                        continue;
                    };
                    Self::decode_tombstone(row.value())?
                };
                let should_prune = frontier_by_origin
                    .get(&record.origin_actor)
                    .is_some_and(|frontier| record.sequence <= *frontier);
                if !should_prune {
                    continue;
                }

                let index_key = tombstone_observed_index_key(key_bytes.as_slice(), &record);
                let _ = tombs.remove(key_bytes.as_slice()).map_err(into_err)?;
                let _ = tombs_by_observed
                    .remove(index_key.as_slice())
                    .map_err(into_err)?;
                pruned = pruned.saturating_add(1);
            }

            for (origin_actor, sequence) in &frontier_by_origin {
                advance_tombstone_prune_frontier_in_meta(&mut meta, origin_actor, *sequence)?;
            }
        }
        w.commit().map_err(into_err)?;

        if pruned > 0 {
            self.rebuild_mst_from_disk_unlocked().await?;
            self.bump_change_clock();
        }

        Ok(pruned)
    }

    /// Advances the tombstone prune frontier for one origin actor.
    ///
    /// The frontier is monotonic because lowering it could let an older delete
    /// marker re-enter the store after GC already decided that sequence was safe
    /// to forget.
    pub fn advance_tombstone_prune_frontier(
        &self,
        origin_actor: &[u8],
        sequence: u64,
    ) -> crate::Result<()> {
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut meta = w.open_table(T::meta()).map_err(into_err)?;
            advance_tombstone_prune_frontier_in_meta(&mut meta, origin_actor, sequence)?;
        }
        w.commit().map_err(into_err)?;
        Ok(())
    }

    #[inline]
    fn encode_reg(r: &C::Reg) -> crate::Result<Vec<u8>> {
        C::encode_reg(r)
    }

    #[inline]
    fn decode_reg(bytes: &[u8]) -> crate::Result<C::Reg> {
        C::decode_reg(bytes)
    }

    #[inline]
    fn encode_key(k: &C::Key) -> Vec<u8> {
        C::key_to_bytes(k)
    }

    #[inline]
    fn decode_key(bytes: &[u8]) -> crate::Result<C::Key> {
        C::key_from_bytes(bytes).map_err(into_err)
    }

    #[inline]
    fn encode_tombstone(record: &TombstoneRecord) -> crate::Result<Vec<u8>> {
        encode_tombstone_row(record)
    }

    #[inline]
    fn decode_tombstone(bytes: &[u8]) -> crate::Result<TombstoneRecord> {
        decode_tombstone_row(bytes)
    }

    #[inline]
    fn local_tombstone(&self, sequence: u64) -> TombstoneRecord {
        TombstoneRecord::new(sequence, C::actor_to_bytes(&self.actor), now_unix_ms())
    }

    /// Builds the exact deleted leaf inserted into the MST for one tombstone.
    ///
    /// The MST does not store full tombstone metadata, only the fields that affect
    /// convergence and root calculation. `observed_at_unix_ms` is intentionally excluded
    /// because it is local GC metadata and would make roots differ across replicas.
    #[inline]
    fn deleted_entry(record: &TombstoneRecord) -> Entry<C::Snapshot> {
        Entry::Deleted {
            ts: record.sequence,
            origin_actor: record.origin_actor.clone(),
        }
    }

    /// Fills missing local observation time for a tombstone received from the wire.
    ///
    /// Sync carries origin sequence and actor, but not the receiver's observation timestamp.
    /// The timestamp is local-only GC metadata, so each node stamps inbound tombstones when
    /// it first prepares them for durable storage.
    fn normalize_incoming_tombstone(
        mut record: TombstoneRecord,
        observed_at_unix_ms: u64,
    ) -> TombstoneRecord {
        if record.observed_at_unix_ms == 0 {
            record.observed_at_unix_ms = observed_at_unix_ms;
        }
        record
    }

    /// Selects the durable tombstone that should represent one deleted key.
    ///
    /// Sequence is the primary monotonic delete generation. Origin actor is only a stable
    /// tie-breaker for the rare case where two actors create same-sequence tombstones for
    /// the same key before those deletes converge.
    fn merge_tombstone_records(
        current: Option<TombstoneRecord>,
        incoming: TombstoneRecord,
    ) -> TombstoneRecord {
        match current {
            Some(current) if !incoming.dominates(&current) => current,
            _ => incoming,
        }
    }

    /// Returns the semantic root-schema version represented by the in-memory MST.
    #[inline]
    fn current_root_schema_version(&self) -> u32 {
        self.root_schema_version.load(Ordering::Acquire)
    }

    /// Builds one snapshot using the current in-memory MST's semantic version.
    #[inline]
    fn snapshot_reg_for_current_version(&self, reg: &C::Reg) -> C::Snapshot {
        C::snapshot_reg_at_version(reg, self.current_root_schema_version())
    }

    /// Rebuilds an ephemeral MST from durable storage for the requested semantic version.
    fn build_tree_from_disk_at_version(
        &self,
        root_schema_version: u32,
    ) -> crate::Result<InMemoryMerkleSearchTree<C, H>> {
        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let tombs = r.open_table(T::tombs()).map_err(into_err)?;

        // Values and tombstones live in separate Redb tables. Rebuild each side
        // independently, sort by logical key, then insert into the fresh MST so
        // startup recovery is deterministic regardless of table iteration order.
        let mut actives: Vec<(C::Key, C::Snapshot)> = {
            let mut out = Vec::new();
            let mut it = values.iter().map_err(into_err)?;
            while let Some(Ok((k_guard, v_guard))) = it.next() {
                let key = Self::decode_key(k_guard.value())?;
                let reg = Self::decode_reg(v_guard.value())?;
                out.push((key, C::snapshot_reg_at_version(&reg, root_schema_version)));
            }
            out
        };
        actives.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));

        // The primary tombstone table carries full metadata; only the sequence
        // and origin actor participate in the MST leaf hash. The observed-at
        // timestamp is kept out of roots because it is node-local GC state.
        let mut tomb_list: Tombstones<C::Key> = {
            let mut out = Vec::new();
            let mut it = tombs.iter().map_err(into_err)?;
            while let Some(Ok((k_guard, tomb_guard))) = it.next() {
                out.push((
                    Self::decode_key(k_guard.value())?,
                    Self::decode_tombstone(tomb_guard.value())?,
                ));
            }
            out
        };
        tomb_list.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));

        let mut tree = Builder::default().with_hasher(H::default()).build();
        for (k, snap) in actives {
            tree.upsert(k, &Entry::Active(snap));
        }
        for (k, tombstone) in tomb_list {
            tree.upsert(k, &Self::deleted_entry(&tombstone));
        }

        Ok(tree)
    }

    /// Rebuilds the in-memory MST from durable registers and tombstones.
    ///
    /// This private helper assumes the caller already holds `mutation_gate`.
    /// Keeping it separate lets write paths rebuild after their Redb commit
    /// without re-entering the same async mutex.
    async fn rebuild_mst_from_disk_unlocked(&self) -> crate::Result<()> {
        let tree = self.build_tree_from_disk_at_version(self.current_root_schema_version())?;
        *self.mst.write().await = tree;
        Ok(())
    }

    /// Rebuild the in-memory MST from durable registers + tombstones.
    pub async fn rebuild_mst_from_disk(&self) -> crate::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        self.rebuild_mst_from_disk_unlocked().await
    }

    /// Rebuilds the in-memory MST for one semantic root schema version.
    ///
    /// This private helper assumes the caller already holds `mutation_gate`.
    /// It updates the schema marker only after the replacement tree has been
    /// built, so concurrent writers cannot project new leaves into the wrong
    /// in-memory root.
    async fn rebuild_mst_from_disk_at_version_unlocked(
        &self,
        root_schema_version: u32,
    ) -> crate::Result<()> {
        let tree = self.build_tree_from_disk_at_version(root_schema_version)?;
        *self.mst.write().await = tree;
        self.root_schema_version
            .store(root_schema_version, Ordering::Release);
        Ok(())
    }

    /// Rebuilds the in-memory MST using the requested semantic root-schema version.
    pub async fn rebuild_mst_from_disk_at_version(
        &self,
        root_schema_version: u32,
    ) -> crate::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        self.rebuild_mst_from_disk_at_version_unlocked(root_schema_version)
            .await
    }

    /// Replace the current in-memory MST with one built from given entries.
    pub async fn rebuild_mst<Ia, It>(&self, actives: Ia, tombs: It)
    where
        Ia: IntoIterator<Item = (C::Key, C::Snapshot)>,
        It: IntoIterator<Item = (C::Key, TombstoneRecord)>,
    {
        let _mutation = self.mutation_gate.lock().await;
        let mut t = Builder::default().with_hasher(H::default()).build();
        for (k, s) in actives {
            t.upsert(k, &Entry::Active(s));
        }
        for (k, tombstone) in tombs {
            t.upsert(k, &Self::deleted_entry(&tombstone));
        }
        *self.mst.write().await = t;
    }

    /// Hex root of the MST (useful for anti-entropy quick checks).
    pub async fn root_hex(&self) -> String {
        let mut t = self.mst.write().await;
        t.root_hash().to_string()
    }

    /// Binary root digest of the MST (16 bytes for XXH3-128).
    pub async fn root_digest(&self) -> [u8; 16] {
        let mut t = self.mst.write().await;
        let d = t.root_hash();
        let mut out = [0u8; 16];
        out.copy_from_slice(d.as_ref());
        out
    }

    /// Binary root digest of the MST for one semantic root-schema version.
    pub async fn root_digest_at_version(
        &self,
        root_schema_version: u32,
    ) -> crate::Result<[u8; 16]> {
        if root_schema_version == self.current_root_schema_version() {
            return Ok(self.root_digest().await);
        }

        let mut tree = self.build_tree_from_disk_at_version(root_schema_version)?;
        let digest = tree.root_hash();
        let mut out = [0u8; 16];
        out.copy_from_slice(digest.as_ref());
        Ok(out)
    }

    /// Advance the internal monotonic change counter after a successful write.
    #[inline]
    fn bump_change_clock(&self) {
        self.change_clock.fetch_add(1, Ordering::Release);
    }

    /// Expose the current change counter so callers can detect when cached views are stale.
    pub fn change_clock(&self) -> u64 {
        self.change_clock.load(Ordering::Acquire)
    }

    /// Garbage-collects tombstones proven safe by an external sync barrier.
    ///
    /// The store owns only the local mechanics: scanning the age index, removing
    /// eligible primary/index rows, advancing the per-origin prune frontier, and
    /// refreshing the in-memory MST. The caller is responsible for passing a
    /// barrier built from active peer/root-equality state.
    pub async fn garbage_collect_tombstones(
        &self,
        policy: &StoreGcPolicy,
        barrier: GcBarrier,
        now_unix_ms: u64,
    ) -> crate::Result<StoreGcReport> {
        let mut report = StoreGcReport::default();
        if policy.tombstone_batch_limit == 0 {
            return Ok(report);
        }
        let _mutation = self.mutation_gate.lock().await;
        if barrier.root_schema_version != self.current_root_schema_version() {
            return Err(Box::new(Error::Other(format!(
                "tombstone GC barrier root schema {} does not match current store root schema {}",
                barrier.root_schema_version,
                self.current_root_schema_version()
            ))));
        }

        let retention_cutoff = now_unix_ms.saturating_sub(policy.tombstone_min_retention_ms);
        let eligible_before_unix_ms = barrier.safe_observed_before_unix_ms.min(retention_cutoff);
        let candidates = self.collect_tombstone_gc_candidates(
            eligible_before_unix_ms,
            policy.tombstone_batch_limit,
            &mut report,
        )?;
        if candidates.is_empty() {
            return Ok(report);
        }

        report.tombstones_pruned =
            self.prune_tombstone_gc_candidates(&candidates, eligible_before_unix_ms)?;
        if report.tombstones_pruned > 0 {
            // The MST crate currently supports upsert but not delete. Rebuild
            // once per GC batch instead of trying to emulate removal with a
            // sentinel leaf, which would keep deleted keys in the root.
            self.rebuild_mst_from_disk_unlocked().await?;
            self.bump_change_clock();
        }

        Ok(report)
    }

    /// Compacts register rows according to the adapter's opt-in policy.
    ///
    /// This store-level pass owns only the generic mechanics: scan value rows,
    /// ask the adapter whether each decoded register should be rewritten, commit
    /// bounded rewrites, and update the in-memory MST leaves. Domain-specific
    /// decisions about which MVReg values can be dropped belong in
    /// `RegAdapter::compact_reg`.
    pub async fn compact_registers(&self, policy: &StoreGcPolicy) -> crate::Result<StoreGcReport> {
        let mut report = StoreGcReport::default();
        let Some(max_values) = policy.mvreg_max_values else {
            return Ok(report);
        };
        if max_values == 0 || policy.mvreg_batch_limit == 0 {
            return Ok(report);
        }

        let _mutation = self.mutation_gate.lock().await;
        let candidates = self.collect_register_compaction_candidates(
            max_values,
            policy.mvreg_batch_limit,
            &mut report,
        )?;
        if candidates.is_empty() {
            return Ok(report);
        }

        let applied = self.write_register_compaction_candidates(&candidates)?;
        if applied.is_empty() {
            return Ok(report);
        }

        report.registers_compacted = applied.len();
        let mut tree = self.mst.write().await;
        for index in applied {
            let candidate = &candidates[index];
            tree.upsert(
                candidate.key.clone(),
                &Entry::Active(candidate.snapshot.clone()),
            );
        }
        self.bump_change_clock();

        Ok(report)
    }

    /// Collects tombstone index rows older than the exclusive GC cutoff.
    ///
    /// The age index is ordered by big-endian observed time, so scanning can stop
    /// as soon as the first non-eligible row is reached.
    fn collect_tombstone_gc_candidates(
        &self,
        eligible_before_unix_ms: u64,
        batch_limit: usize,
        report: &mut StoreGcReport,
    ) -> crate::Result<Vec<TombstoneGcCandidate>> {
        let r = self.db.begin_read().map_err(into_err)?;
        let tombs_by_observed = r.open_table(T::tombs_by_observed()).map_err(into_err)?;
        let mut candidates = Vec::with_capacity(batch_limit.min(1024));
        let mut it = tombs_by_observed.iter().map_err(into_err)?;

        while candidates.len() < batch_limit {
            let Some(row) = it.next() else {
                break;
            };
            let (index_key_guard, _) = row.map_err(into_err)?;
            let index_key = index_key_guard.value();
            let (observed_at_unix_ms, key_bytes) = tombstone_observed_index_parts(index_key)?;
            if observed_at_unix_ms >= eligible_before_unix_ms {
                break;
            }

            report.tombstones_scanned = report.tombstones_scanned.saturating_add(1);
            candidates.push(TombstoneGcCandidate {
                index_key: index_key.to_vec(),
                key_bytes: key_bytes.to_vec(),
            });
        }

        Ok(candidates)
    }

    /// Collects value rows whose adapter wants to compact their decoded register.
    ///
    /// The batch limit bounds rows inspected, not just rows rewritten. This keeps
    /// a pass predictable even when most domains use the default no-op adapter
    /// hook or most registers are already within policy.
    fn collect_register_compaction_candidates(
        &self,
        max_values: usize,
        batch_limit: usize,
        report: &mut StoreGcReport,
    ) -> crate::Result<Vec<RegisterCompactionCandidate<C>>> {
        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let mut candidates = Vec::new();
        let mut it = values.iter().map_err(into_err)?;

        while report.registers_scanned < batch_limit {
            let Some(row) = it.next() else {
                break;
            };
            let (key_guard, reg_guard) = row.map_err(into_err)?;
            let key_bytes = key_guard.value().to_vec();
            let original_reg_bytes = reg_guard.value().to_vec();
            let key = Self::decode_key(&key_bytes)?;
            let reg = Self::decode_reg(&original_reg_bytes)?;
            report.registers_scanned = report.registers_scanned.saturating_add(1);

            let Some(compacted) = C::compact_reg(reg, max_values)? else {
                continue;
            };
            let snapshot = self.snapshot_reg_for_current_version(&compacted);
            candidates.push(RegisterCompactionCandidate {
                key_bytes,
                original_reg_bytes,
                key,
                reg: compacted,
                snapshot,
            });
        }

        Ok(candidates)
    }

    /// Writes compacted register candidates whose source rows have not changed.
    ///
    /// Compaction scans through a read transaction and rewrites later in a write
    /// transaction. Verifying the original row bytes avoids overwriting a newer
    /// upsert or sync merge that committed between those two phases.
    fn write_register_compaction_candidates(
        &self,
        candidates: &[RegisterCompactionCandidate<C>],
    ) -> crate::Result<Vec<usize>> {
        let w = self.db.begin_write().map_err(into_err)?;
        let mut applied = Vec::new();
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            for (index, candidate) in candidates.iter().enumerate() {
                let current_matches = match values
                    .get(candidate.key_bytes.as_slice())
                    .map_err(into_err)?
                {
                    Some(current) => current.value() == candidate.original_reg_bytes.as_slice(),
                    None => false,
                };
                if !current_matches {
                    continue;
                }

                let encoded = Self::encode_reg(&candidate.reg)?;
                values
                    .insert(candidate.key_bytes.as_slice(), encoded.as_slice())
                    .map_err(into_err)?;
                applied.push(index);
            }
        }
        w.commit().map_err(into_err)?;

        Ok(applied)
    }

    /// Removes selected tombstones and advances prune frontiers in one transaction.
    ///
    /// Each candidate is verified against the primary tombstone row before
    /// deletion. If the primary row is missing or now points at a different
    /// observed-time index key, only the stale secondary index row is removed.
    fn prune_tombstone_gc_candidates(
        &self,
        candidates: &[TombstoneGcCandidate],
        eligible_before_unix_ms: u64,
    ) -> crate::Result<usize> {
        let w = self.db.begin_write().map_err(into_err)?;
        let mut pruned = 0usize;
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let mut meta = w.open_table(T::meta()).map_err(into_err)?;

            for candidate in candidates {
                let current = match tombs
                    .get(candidate.key_bytes.as_slice())
                    .map_err(into_err)?
                {
                    Some(row) => Some(Self::decode_tombstone(row.value())?),
                    None => None,
                };

                let Some(record) = current else {
                    let _ = tombs_by_observed
                        .remove(candidate.index_key.as_slice())
                        .map_err(into_err)?;
                    continue;
                };

                let expected_index_key =
                    tombstone_observed_index_key(candidate.key_bytes.as_slice(), &record);
                if expected_index_key != candidate.index_key
                    || record.observed_at_unix_ms >= eligible_before_unix_ms
                {
                    let _ = tombs_by_observed
                        .remove(candidate.index_key.as_slice())
                        .map_err(into_err)?;
                    continue;
                }

                let _ = tombs
                    .remove(candidate.key_bytes.as_slice())
                    .map_err(into_err)?;
                let _ = tombs_by_observed
                    .remove(candidate.index_key.as_slice())
                    .map_err(into_err)?;
                advance_tombstone_prune_frontier_in_meta(
                    &mut meta,
                    &record.origin_actor,
                    record.sequence,
                )?;
                pruned = pruned.saturating_add(1);
            }
        }
        w.commit().map_err(into_err)?;
        Ok(pruned)
    }

    /// Insert or update value for key `k`.
    pub async fn upsert(&self, k: &C::Key, v: C::Value) -> crate::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        let w = self.db.begin_write().map_err(into_err)?;
        let kb = Self::encode_key(k);
        let new_reg = {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let current = match values.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            };
            let new_reg = C::upsert_reg(current, &self.actor, v);
            values
                .insert(kb.as_slice(), Self::encode_reg(&new_reg)?.as_slice())
                .map_err(into_err)?;
            new_reg
        };

        {
            // A value row makes any existing tombstone obsolete for this key. Clear both
            // tombstone tables inside the same write transaction that observed the latest
            // register so a stale read cannot overwrite a concurrent local update.
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let existing = match tombs.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_tombstone(row.value())?),
                None => None,
            };

            if let Some(record) = existing {
                let index_key = tombstone_observed_index_key(kb.as_slice(), &record);
                let _ = tombs_by_observed
                    .remove(index_key.as_slice())
                    .map_err(into_err)?;
            }

            let _ = tombs.remove(kb.as_slice()).map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        // Reflect in MST after the durable commit. Readers that need a fully current
        // semantic view should use the durable tables; the MST is the anti-entropy
        // index and is refreshed immediately after each successful write.
        let mut t = self.mst.write().await;
        let snap = self.snapshot_reg_for_current_version(&new_reg);
        t.upsert(k.clone(), &Entry::Active(snap));

        self.bump_change_clock();

        Ok(())
    }

    /// Inserts or updates a batch of key/value pairs in a single durable transaction.
    ///
    /// The last value for a duplicated key in the provided iterator wins.
    pub async fn upsert_many<I>(&self, entries: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = (C::Key, C::Value)>,
    {
        let mut requested: HashMap<C::Key, C::Value> = HashMap::new();
        for (key, value) in entries {
            requested.insert(key, value);
        }
        if requested.is_empty() {
            return Ok(());
        }

        let _mutation = self.mutation_gate.lock().await;
        let mut merged: Vec<(C::Key, C::Reg)> = Vec::with_capacity(requested.len());
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            for (key, value) in requested {
                let kb = Self::encode_key(&key);
                let current = match values.get(kb.as_slice()).map_err(into_err)? {
                    Some(row) => Some(Self::decode_reg(row.value())?),
                    None => None,
                };
                let reg = C::upsert_reg(current, &self.actor, value);
                values
                    .insert(kb.as_slice(), Self::encode_reg(&reg)?.as_slice())
                    .map_err(into_err)?;

                // Batch upserts have the same tombstone-clearing semantics as a
                // single upsert. The merge happens in this write transaction so
                // each key observes the latest durable register before writing.
                let existing = match tombs.get(kb.as_slice()).map_err(into_err)? {
                    Some(row) => Some(Self::decode_tombstone(row.value())?),
                    None => None,
                };

                if let Some(record) = existing {
                    let index_key = tombstone_observed_index_key(kb.as_slice(), &record);
                    let _ = tombs_by_observed
                        .remove(index_key.as_slice())
                        .map_err(into_err)?;
                }

                let _ = tombs.remove(kb.as_slice()).map_err(into_err)?;
                merged.push((key, reg));
            }
        }
        w.commit().map_err(into_err)?;

        let mut tree = self.mst.write().await;
        for (key, reg) in &merged {
            let snap = self.snapshot_reg_for_current_version(reg);
            tree.upsert(key.clone(), &Entry::Active(snap));
        }

        self.bump_change_clock();
        Ok(())
    }

    /// Remove key and persist a tombstone with a monotonic sequence.
    pub async fn remove(&self, k: &C::Key) -> crate::Result<u64> {
        let _mutation = self.mutation_gate.lock().await;
        // First check if tomb already exists; if so, do NOT allocate a new seq.
        //
        // Delete operations are idempotent per key. Re-removing an already deleted
        // key must keep the original tombstone metadata so peers do not observe a
        // fresh delete generation every time a caller retries.
        let (already_tombstoned, needs_value_drop) = {
            let r = self.db.begin_read().map_err(into_err)?;
            let tombstones = r.open_table(T::tombs()).map_err(into_err)?;
            let values = r.open_table(T::values()).map_err(into_err)?;

            let kb = Self::encode_key(k);
            let tombstone = match tombstones.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_tombstone(row.value())?),
                None => None,
            };
            let value_exists = values.get(kb.as_slice()).map_err(into_err)?.is_some();

            (tombstone, value_exists)
        };

        if let Some(tombstone) = already_tombstoned {
            // Ensure value row is gone and MST reflects the *existing* monotonic ts.
            let w = self.db.begin_write().map_err(into_err)?;
            if needs_value_drop {
                let mut values = w.open_table(T::values()).map_err(into_err)?;
                let _ = values
                    .remove(Self::encode_key(k).as_slice())
                    .map_err(into_err)?;
            }
            w.commit().map_err(into_err)?;

            let mut t = self.mst.write().await;
            t.upsert(k.clone(), &Self::deleted_entry(&tombstone));
            self.bump_change_clock();
            return Ok(tombstone.sequence);
        }

        // No tombstone yet: allocate a new actor-local sequence and persist.
        //
        // `tomb_seq` is local to this store actor. It is paired with origin_actor
        // in TombstoneRecord so later GC/prune logic can reason about exactly
        // which actor generated a delete marker.
        let w = self.db.begin_write().map_err(into_err)?;

        let sequence = {
            let mut meta = w.open_table(T::meta()).map_err(into_err)?;
            let next = match meta.get("tomb_seq").map_err(into_err)? {
                Some(g) => g.value().saturating_add(1),
                None => 1,
            };
            meta.insert("tomb_seq", &next).map_err(into_err)?;
            next
        };

        let tombstone = self.local_tombstone(sequence);
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let kb = Self::encode_key(k);

            // The primary row is what sync, rebuild, and range export read.
            tombs
                .insert(
                    kb.as_slice(),
                    Self::encode_tombstone(&tombstone)?.as_slice(),
                )
                .map_err(into_err)?;

            // The secondary row is intentionally value-less; its key is the data
            // needed by future GC scans to find old tombstones in time order.
            let index_key = tombstone_observed_index_key(kb.as_slice(), &tombstone);

            tombs_by_observed
                .insert(index_key.as_slice(), &[] as &[u8])
                .map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Self::deleted_entry(&tombstone));
        self.bump_change_clock();
        Ok(sequence)
    }

    /// Purge a key locally without writing a tombstone so remote replicas can repopulate it.
    ///
    /// This is intended for recovery/testing scenarios where a local store is missing entries
    /// and should accept the next sync payload, not for user-facing delete operations.
    pub async fn purge_local(&self, k: &C::Key) -> crate::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let kb = Self::encode_key(k);

            // Purge is not a replicated delete. It removes local state so the
            // next anti-entropy pass can repopulate the key. That means both
            // the primary tombstone and the age index must be removed locally.
            let existing = match tombs.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_tombstone(row.value())?),
                None => None,
            };

            if let Some(record) = existing {
                let index_key = tombstone_observed_index_key(kb.as_slice(), &record);
                let _ = tombs_by_observed
                    .remove(index_key.as_slice())
                    .map_err(into_err)?;
            }
            let _ = tombs.remove(kb.as_slice()).map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        self.rebuild_mst_from_disk_unlocked().await?;
        self.bump_change_clock();
        Ok(())
    }

    /// Merge a remote register for key `k` into durable state and MST, clearing any local tombstone.
    pub async fn merge_register(&self, k: &C::Key, incoming: &C::Reg) -> crate::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        let w = self.db.begin_write().map_err(into_err)?;
        let kb = Self::encode_key(k);
        let merged = {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let current = match values.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            };
            let merged = C::merge_regs(current, incoming.clone());
            values
                .insert(kb.as_slice(), Self::encode_reg(&merged)?.as_slice())
                .map_err(into_err)?;
            merged
        };

        {
            // Accepting a register means the key is live again locally, so any tombstone
            // metadata for the key has to be removed from both tombstone tables in the
            // same transaction that observed and merged the latest durable register.
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let existing = match tombs.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_tombstone(row.value())?),
                None => None,
            };

            if let Some(record) = existing {
                let index_key = tombstone_observed_index_key(kb.as_slice(), &record);
                let _ = tombs_by_observed
                    .remove(index_key.as_slice())
                    .map_err(into_err)?;
            }
            let _ = tombs.remove(kb.as_slice()).map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        let mut t = self.mst.write().await;
        let snap = self.snapshot_reg_for_current_version(&merged);
        t.upsert(k.clone(), &Entry::Active(snap));
        self.bump_change_clock();
        Ok(())
    }

    /// Apply an inbound tombstone (idempotent, monotonic).
    pub async fn apply_tombstone(&self, k: &C::Key, ts: u64) -> io::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        // This scalar helper is kept for local/test callers that only have a
        // tombstone sequence. Sync delta application uses TombstoneRecord directly
        // so it can preserve the remote origin actor.
        let incoming = self.local_tombstone(ts);
        let next_tombstone = {
            let r = self.db.begin_read().map_err(into_err)?;
            let meta = r.open_table(T::meta()).map_err(into_err)?;
            let tombs = r.open_table(T::tombs()).map_err(into_err)?;
            let kb = Self::encode_key(k);
            let frontier_key = tombstone_prune_frontier_key(&incoming.origin_actor);
            let pruned_sequence = meta
                .get(frontier_key.as_str())
                .map_err(into_err)?
                .map(|row| row.value())
                .unwrap_or(0);

            // If GC has already forgotten this origin sequence, do not recreate
            // a tombstone row or an MST leaf for it. A frontier of zero means no
            // pruning has happened for that origin yet.
            if pruned_sequence > 0 && incoming.sequence <= pruned_sequence {
                return Ok(());
            }

            // Keep whichever tombstone is newer by sequence, with origin actor as
            // a deterministic tie-breaker. This prevents a stale delete from
            // downgrading a newer local tombstone.
            let current = match tombs.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_tombstone(row.value())?),
                None => None,
            };

            Self::merge_tombstone_records(current, incoming)
        };

        let w = self.db.begin_write().map_err(into_err)?;
        let kb = Self::encode_key(k);
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let _ = values.remove(kb.as_slice()).map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;

            // Replacing a tombstone requires removing the previous age-index row
            // first. The timestamp is part of the old payload, not derivable from
            // the key or from the incoming record.
            let old = match tombs.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_tombstone(row.value())?),
                None => None,
            };

            if let Some(record) = old {
                let index_key = tombstone_observed_index_key(kb.as_slice(), &record);
                let _ = tombs_by_observed
                    .remove(index_key.as_slice())
                    .map_err(into_err)?;
            }

            // Write both tombstone representations together so crashes cannot
            // leave the primary table and GC index logically out of step after commit.
            tombs
                .insert(
                    kb.as_slice(),
                    Self::encode_tombstone(&next_tombstone)?.as_slice(),
                )
                .map_err(into_err)?;

            let index_key = tombstone_observed_index_key(kb.as_slice(), &next_tombstone);
            tombs_by_observed
                .insert(index_key.as_slice(), &[] as &[u8])
                .map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Self::deleted_entry(&next_tombstone));
        self.bump_change_clock();
        Ok(())
    }

    /// Apply one streamed delta chunk (register merges + tombstones) without updating the MST.
    ///
    /// This is used only by `DeltaApplySession`, which holds `mutation_gate`
    /// until `commit()` rebuilds the MST from disk. Callers that need one-shot
    /// delta application should use `apply_delta_chunk_update_mst()` instead.
    fn apply_delta_chunk(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tombs: Tombstones<C::Key>,
    ) -> io::Result<()> {
        let (merged_regs, merged_tombs) =
            self.write_delta_chunk_merged_latest(regs, tombs, false)?;
        let had_changes = !merged_regs.is_empty() || !merged_tombs.is_empty();

        if had_changes {
            self.bump_change_clock();
        }

        Ok(())
    }

    /// Apply one streamed delta chunk and update the in-memory MST incrementally.
    /// This avoids a full rebuild at the end of the stream when chunks are small.
    pub async fn apply_delta_chunk_update_mst(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tombs: Tombstones<C::Key>,
    ) -> io::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        let (merged_regs, merged_tombs) =
            self.write_delta_chunk_merged_latest(regs, tombs, true)?;
        let had_changes = !merged_regs.is_empty() || !merged_tombs.is_empty();

        // Reflect in MST in the same logical order: regs then tombs.
        {
            let mut t = self.mst.write().await;
            for (k, reg) in &merged_regs {
                let snap = self.snapshot_reg_for_current_version(reg);
                t.upsert(k.clone(), &Entry::Active(snap));
            }
            for (k, tombstone) in merged_tombs {
                t.upsert(k.clone(), &Self::deleted_entry(&tombstone));
            }
        }

        if had_changes {
            self.bump_change_clock();
        }

        Ok(())
    }

    /// Merges and writes one inbound delta chunk against the latest durable rows.
    ///
    /// Delta application is a read-merge-write operation. The merge must happen
    /// inside the Redb write transaction, not in an earlier read transaction,
    /// otherwise a stale sync payload prepared before local MVReg compaction can
    /// commit after that compaction and overwrite the compacted register.
    fn write_delta_chunk_merged_latest(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tombs: Tombstones<C::Key>,
        refresh_existing_tombs_for_mst: bool,
    ) -> io::Result<RegistersAndTombs<C::Key, C::Reg>> {
        let tomb_keys_in_chunk: HashSet<C::Key> = if self.preserve_local_tombs && !regs.is_empty() {
            tombs.iter().map(|(key, _)| key.clone()).collect()
        } else {
            HashSet::new()
        };
        let observed_at_unix_ms = now_unix_ms();
        let mut merged_regs = Vec::with_capacity(regs.len());
        let mut merged_tombs = Vec::with_capacity(tombs.len());

        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let mut tomb_table = w.open_table(T::tombs()).map_err(into_err)?;
            let mut tombs_by_observed = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
            let meta = w.open_table(T::meta()).map_err(into_err)?;

            for (key, incoming) in regs {
                let key_bytes = Self::encode_key(&key);
                // For preserve-local-tomb domains, an inbound register cannot
                // clear a local tombstone unless this same chunk also carries the
                // tombstone for that key. The check is performed inside the write
                // transaction so a concurrent local delete cannot be missed.
                let skip_due_to_local_tomb =
                    self.preserve_local_tombs && !tomb_keys_in_chunk.contains(&key) && {
                        tomb_table
                            .get(key_bytes.as_slice())
                            .map_err(into_err)?
                            .is_some()
                    };

                if skip_due_to_local_tomb {
                    continue;
                }

                let current = match values.get(key_bytes.as_slice()).map_err(into_err)? {
                    Some(row) => Some(Self::decode_reg(row.value())?),
                    None => None,
                };
                let merged = C::merge_regs(current, incoming);
                values
                    .insert(key_bytes.as_slice(), Self::encode_reg(&merged)?.as_slice())
                    .map_err(into_err)?;

                let existing = match tomb_table.get(key_bytes.as_slice()).map_err(into_err)? {
                    Some(row) => Some(Self::decode_tombstone(row.value())?),
                    None => None,
                };

                if let Some(record) = existing {
                    let index_key = tombstone_observed_index_key(key_bytes.as_slice(), &record);
                    let _ = tombs_by_observed
                        .remove(index_key.as_slice())
                        .map_err(into_err)?;
                }
                let _ = tomb_table.remove(key_bytes.as_slice()).map_err(into_err)?;

                merged_regs.push((key, merged));
            }

            for (key, incoming) in tombs {
                let frontier_key = tombstone_prune_frontier_key(&incoming.origin_actor);
                let pruned_sequence = meta
                    .get(frontier_key.as_str())
                    .map_err(into_err)?
                    .map(|row| row.value())
                    .unwrap_or(0);

                // A tombstone at or below the per-origin prune frontier was already
                // deliberately forgotten by local GC. Dropping it here prevents an
                // old peer from reintroducing that delete marker during anti-entropy.
                if pruned_sequence > 0 && incoming.sequence <= pruned_sequence {
                    continue;
                }

                let key_bytes = Self::encode_key(&key);
                let current_tomb = match tomb_table.get(key_bytes.as_slice()).map_err(into_err)? {
                    Some(row) => Some(Self::decode_tombstone(row.value())?),
                    None => None,
                };
                let value_exists = values
                    .get(key_bytes.as_slice())
                    .map_err(into_err)?
                    .is_some();

                // Wire tombstones leave observed_at_unix_ms unset so each receiver
                // can index them by local observation time. Existing local
                // tombstones keep their original timestamp when they win the merge.
                let incoming = Self::normalize_incoming_tombstone(incoming, observed_at_unix_ms);
                let next_tombstone = Self::merge_tombstone_records(current_tomb.clone(), incoming);

                // Skip pure no-ops unless the caller needs the existing tombstone
                // returned to refresh the MST incrementally. The durable state
                // already contains the winning tombstone and there is no live value.
                if current_tomb.as_ref() == Some(&next_tombstone)
                    && !value_exists
                    && !refresh_existing_tombs_for_mst
                {
                    continue;
                }

                if let Some(record) = current_tomb {
                    let index_key = tombstone_observed_index_key(key_bytes.as_slice(), &record);
                    let _ = tombs_by_observed
                        .remove(index_key.as_slice())
                        .map_err(into_err)?;
                }

                tomb_table
                    .insert(
                        key_bytes.as_slice(),
                        Self::encode_tombstone(&next_tombstone)?.as_slice(),
                    )
                    .map_err(into_err)?;
                let index_key = tombstone_observed_index_key(key_bytes.as_slice(), &next_tombstone);
                tombs_by_observed
                    .insert(index_key.as_slice(), &[] as &[u8])
                    .map_err(into_err)?;
                let _ = values.remove(key_bytes.as_slice()).map_err(into_err)?;

                merged_tombs.push((key, next_tombstone));
            }
        }
        w.commit().map_err(into_err)?;

        Ok((merged_regs, merged_tombs))
    }

    /// Rebuild MST once after a sequence of `apply_delta_chunk()` calls.
    pub async fn finalize_after_stream(&self) -> crate::Result<()> {
        let _mutation = self.mutation_gate.lock().await;
        self.rebuild_mst_from_disk_unlocked().await
    }

    /// Dump durable (key, snapshot) and (key, tombstone).
    pub fn load_all(&self) -> crate::Result<SnapshotsAndTombs<C::Key, C::Snapshot>> {
        let mut actives = Vec::new();
        let mut tombs = Vec::new();
        self.load_all_into(&mut actives, &mut tombs)?;
        Ok((actives, tombs))
    }

    /// Dump durable raw registers and tombstones.
    ///
    /// This is reserved for callers that need access to full concurrent values
    /// instead of the MST snapshot projection derived from them.
    pub fn load_all_regs(&self) -> crate::Result<RegistersAndTombs<C::Key, C::Reg>> {
        let mut actives = Vec::new();
        let mut tombs = Vec::new();
        self.load_all_regs_into(&mut actives, &mut tombs)?;
        Ok((actives, tombs))
    }

    /// Populate caller-provided buffers with all durable snapshots and tombstones so hot loops can reuse allocations.
    pub fn load_all_into(
        &self,
        actives: &mut Vec<(C::Key, C::Snapshot)>,
        tombs: &mut Vec<(C::Key, TombstoneRecord)>,
    ) -> crate::Result<()> {
        actives.clear();
        tombs.clear();

        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let tomb_table = r.open_table(T::tombs()).map_err(into_err)?;

        {
            let mut it = values.iter().map_err(into_err)?;
            while let Some(Ok((k, v))) = it.next() {
                let key = Self::decode_key(k.value())?;
                let reg = Self::decode_reg(v.value())?;
                actives.push((
                    key,
                    C::snapshot_reg_at_version(&reg, self.current_root_schema_version()),
                ));
            }
        }

        {
            let mut it = tomb_table.iter().map_err(into_err)?;
            while let Some(Ok((k, tombstone))) = it.next() {
                tombs.push((
                    Self::decode_key(k.value())?,
                    Self::decode_tombstone(tombstone.value())?,
                ));
            }
        }

        Ok(())
    }

    /// Populate caller-provided buffers with all raw registers and tombstones.
    ///
    /// Peer metadata uses this path so operational-only fields can remain
    /// available to topology code without becoming part of the MST snapshot
    /// hash contract.
    pub fn load_all_regs_into(
        &self,
        actives: &mut Vec<(C::Key, C::Reg)>,
        tombs: &mut Vec<(C::Key, TombstoneRecord)>,
    ) -> crate::Result<()> {
        actives.clear();
        tombs.clear();

        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let tomb_table = r.open_table(T::tombs()).map_err(into_err)?;

        {
            let mut it = values.iter().map_err(into_err)?;
            while let Some(Ok((k, v))) = it.next() {
                let key = Self::decode_key(k.value())?;
                let reg = Self::decode_reg(v.value())?;
                actives.push((key, reg));
            }
        }

        {
            let mut it = tomb_table.iter().map_err(into_err)?;
            while let Some(Ok((k, tombstone))) = it.next() {
                tombs.push((
                    Self::decode_key(k.value())?,
                    Self::decode_tombstone(tombstone.value())?,
                ));
            }
        }

        Ok(())
    }

    /// Visit all snapshots using a single read transaction without building a Vec.
    pub fn for_each_snapshot<F>(&self, mut f: F) -> crate::Result<()>
    where
        F: FnMut(C::Key, C::Snapshot),
    {
        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let mut it = values.iter().map_err(into_err)?;
        while let Some(Ok((k, v))) = it.next() {
            let key = Self::decode_key(k.value())?;
            let reg = Self::decode_reg(v.value())?;
            f(
                key,
                C::snapshot_reg_at_version(&reg, self.current_root_schema_version()),
            );
        }
        Ok(())
    }

    /// Visit all tombstones using a single read transaction without building a Vec.
    ///
    /// This visits primary tombstone rows, not the observed-time index. Callers get
    /// full metadata so later maintenance code can inspect sequence, origin actor,
    /// and local observation time without doing a second lookup.
    pub fn for_each_tombstone<F>(&self, mut f: F) -> crate::Result<()>
    where
        F: FnMut(C::Key, TombstoneRecord),
    {
        let r = self.db.begin_read().map_err(into_err)?;
        let tombs = r.open_table(T::tombs()).map_err(into_err)?;
        let mut it = tombs.iter().map_err(into_err)?;
        while let Some(Ok((k, tombstone))) = it.next() {
            f(
                Self::decode_key(k.value())?,
                Self::decode_tombstone(tombstone.value())?,
            );
        }
        Ok(())
    }

    /// Read and return the current snapshot for key `k`, if present.
    pub fn get_snapshot(&self, k: &C::Key) -> crate::Result<Option<C::Snapshot>> {
        let r = self.db.begin_read().map_err(into_err)?;
        let t = r.open_table(T::values()).map_err(into_err)?;
        let kb = Self::encode_key(k);
        match t.get(kb.as_slice()).map_err(into_err)? {
            Some(v) => {
                let reg = Self::decode_reg(v.value())?;
                Ok(Some(C::snapshot_reg_at_version(
                    &reg,
                    self.current_root_schema_version(),
                )))
            }
            None => Ok(None),
        }
    }

    /// Reads the current raw register for key `k`, if present.
    ///
    /// This is the escape hatch used by peer metadata code that must inspect
    /// values omitted from the MST snapshot projection.
    pub fn get_reg(&self, k: &C::Key) -> crate::Result<Option<C::Reg>> {
        let r = self.db.begin_read().map_err(into_err)?;
        let t = r.open_table(T::values()).map_err(into_err)?;
        let kb = Self::encode_key(k);
        match t.get(kb.as_slice()).map_err(into_err)? {
            Some(v) => Ok(Some(Self::decode_reg(v.value())?)),
            None => Ok(None),
        }
    }

    /// Return page summaries (inclusive [start,end] + digest) for the current MST.
    pub async fn page_range_summary(&self) -> crate::Result<Vec<PageDigestRange>> {
        let mut t = self.mst.write().await;
        // Ensure root is computed
        let _ = t.root_hash();
        let prs = t.serialise_page_ranges().unwrap_or_default();

        let out: Vec<PageDigestRange> = prs
            .into_iter()
            .map(|pr| PageDigestRange {
                start: C::key_to_bytes(pr.start()),
                end: C::key_to_bytes(pr.end()),
                hash: pr.hash().as_ref().to_vec(),
            })
            .collect();
        Ok(out)
    }

    /// Return page summaries for one semantic root-schema version.
    pub async fn page_range_summary_at_version(
        &self,
        root_schema_version: u32,
    ) -> crate::Result<Vec<PageDigestRange>> {
        if root_schema_version == self.current_root_schema_version() {
            return self.page_range_summary().await;
        }

        let mut tree = self.build_tree_from_disk_at_version(root_schema_version)?;
        let _ = tree.root_hash();
        let prs = tree.serialise_page_ranges().unwrap_or_default();

        let out: Vec<PageDigestRange> = prs
            .into_iter()
            .map(|pr| PageDigestRange {
                start: C::key_to_bytes(pr.start()),
                end: C::key_to_bytes(pr.end()),
                hash: pr.hash().as_ref().to_vec(),
            })
            .collect();
        Ok(out)
    }

    /// Optimized delta export for requested ranges:
    /// For each requested [start,end], include all values/tombstones whose raw-key bytes
    /// are within that inclusive range.
    pub fn export_page_ranges_delta(
        &self,
        want: &[PageDigestRange],
    ) -> crate::Result<RegistersAndTombs<C::Key, C::Reg>> {
        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let tombstones = r.open_table(T::tombs()).map_err(into_err)?;

        let mut registers_out: Vec<(C::Key, C::Reg)> = Vec::new();
        let mut tombstones_out: Tombstones<C::Key> = Vec::new();

        // Deduplicate across overlapping ranges with raw key bytes
        let mut seen_regs: HashSet<Vec<u8>> = HashSet::new();
        let mut seen_tombs: HashSet<Vec<u8>> = HashSet::new();

        for r in want {
            let start = r.start.as_slice();
            let end = r.end.as_slice();

            // Registers in [start, end]
            {
                let mut it = values.range(start..=end).map_err(into_err)?;
                while let Some(Ok((k_g, v_g))) = it.next() {
                    let k_bytes = k_g.value();
                    if !seen_regs.insert(k_bytes.to_vec()) {
                        continue;
                    }
                    let key = Self::decode_key(k_bytes)?;
                    let reg = Self::decode_reg(v_g.value())?;
                    registers_out.push((key, reg));
                }
            }

            // Tombstones in [start, end].
            //
            // Export from the primary tombstone table so the receiver gets the
            // complete TombstoneRecord. The observed-time index is deliberately
            // local-only and is never streamed to peers.
            {
                let mut it = tombstones.range(start..=end).map_err(into_err)?;
                while let Some(Ok((k_g, tombstone_g))) = it.next() {
                    let k_bytes = k_g.value();
                    if !seen_tombs.insert(k_bytes.to_vec()) {
                        continue;
                    }
                    let key = Self::decode_key(k_bytes)?;
                    let tombstone = Self::decode_tombstone(tombstone_g.value())?;
                    tombstones_out.push((key, tombstone));
                }
            }
        }

        Ok((registers_out, tombstones_out))
    }

    /// Encodes exported register rows into the opaque sync payload representation.
    pub fn encode_register_delta(
        &self,
        regs: Registers<C::Key, C::Reg>,
    ) -> crate::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::with_capacity(regs.len());
        for (key, reg) in regs {
            out.push((Self::encode_key(&key), Self::encode_reg(&reg)?));
        }
        Ok(out)
    }

    // Debug helpers (feature-gated). When disabled, they are cheap no-ops.
    pub async fn debug_dump_root(&self, label: &str) {
        if std::env::var_os("CRDT_STORE_DEBUG_DUMP").is_some() {
            let hex = self.root_hex().await;
            debug!(target: "merkle search tree", "{label}: root={hex}");
        } else {
            let _ = label;
        }
    }

    pub async fn debug_dump_ranges(&self, label: &str, limit: usize) {
        if std::env::var_os("CRDT_STORE_DEBUG_DUMP").is_some() {
            let mut t = self.mst.write().await;
            let _ = t.root_hash();
            let prs = t.serialise_page_ranges().unwrap_or_default();

            debug!(target: "merkle search tree", "{label}: {} ranges", prs.len());
            for (i, pr) in prs.iter().take(limit).enumerate() {
                let s = C::key_to_bytes(pr.start());
                let e = C::key_to_bytes(pr.end());
                let h = pr.hash().as_ref();
                debug!(
                    target: "merkle search tree",
                    "  [{:03}] start={:02X?} end={:02X?} hash={:02X?}",
                    i,
                    &s[..std::cmp::min(6, s.len())],
                    &e[..std::cmp::min(6, e.len())],
                    &h[..std::cmp::min(6, h.len())],
                );
            }
        } else {
            let _ = (label, limit);
        }
    }

    /// Print the exact bytes we hash per leaf Entry (canonical). Debug only.
    #[allow(dead_code)]
    pub fn debug_dump_leaf_bytes_from_store(&self) -> io::Result<()> {
        if std::env::var_os("CRDT_STORE_DEBUG_DUMP").is_some() {
            let (actives, tombs) = self.load_all()?;

            println!("[LEAVES] actives:");
            for (k, snap) in actives {
                let mut sink = HashBytes::default();
                Entry::Active(snap).hash(&mut sink);
                use base64::Engine as _;
                println!(
                    "  key={:?} bytes(base64)={}",
                    k.as_ref(),
                    base64::engine::general_purpose::STANDARD.encode(sink.as_slice()),
                );
            }

            println!("[LEAVES] tombstones:");
            for (k, tombstone) in tombs {
                let mut sink = HashBytes::default();
                let e: Entry<C::Snapshot> = Self::deleted_entry(&tombstone);
                e.hash(&mut sink);
                use base64::Engine as _;
                println!(
                    "  key={:?} bytes(base64)={}",
                    k.as_ref(),
                    base64::engine::general_purpose::STANDARD.encode(sink.as_slice()),
                );
            }
            Ok(())
        } else {
            Ok(())
        }
    }
}

/// Builder for `CrdtMstStore` to customize MST options (hasher selection, future knobs).
pub struct StoreBuilder<C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]> + std::fmt::Debug,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    db: Arc<redb::Database>,
    actor: C::Actor,
    hasher: Option<H>,
    preserve_local_tombs: bool,
    _tables: std::marker::PhantomData<T>,
}

impl<C, H, T> StoreBuilder<C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]> + std::fmt::Debug,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    /// Override the hasher used by the MST.
    pub fn with_hasher(mut self, h: H) -> Self {
        self.hasher = Some(h);
        self
    }

    /// Configure whether locally authored tombstones remain authoritative unless the incoming delta explicitly includes the same tomb entry.
    ///
    /// Enable this only for domains where recreating a value goes through a higher-level path that clears the tombstone first (e.g., secrets with immediate gossip).
    /// For most stores we expect anti-entropy to repopulate accidentally deleted rows, so this flag should stay `false`.
    pub fn with_preserve_local_tombs(mut self, enabled: bool) -> Self {
        self.preserve_local_tombs = enabled;
        self
    }

    /// Build and open the store.
    pub fn build(self) -> crate::Result<CrdtMstStore<C, H, T>> {
        // Ensure tables exist
        let w = self.db.begin_write().map_err(into_err)?;
        let _ = w.open_table(T::values()).map_err(into_err)?;
        let _ = w.open_table(T::tombs()).map_err(into_err)?;
        let _ = w.open_table(T::tombs_by_observed()).map_err(into_err)?;
        let _ = w.open_table(T::meta()).map_err(into_err)?;
        w.commit().map_err(into_err)?;

        let hasher = self.hasher.unwrap_or_default();
        let mst = Arc::new(RwLock::new(Builder::default().with_hasher(hasher).build()));
        Ok(CrdtMstStore {
            db: self.db,
            actor: self.actor,
            mst,
            mutation_gate: Mutex::new(()),
            root_schema_version: AtomicU32::new(DEFAULT_ROOT_SCHEMA_VERSION),
            change_clock: AtomicU64::new(1),
            preserve_local_tombs: self.preserve_local_tombs,
            _tables: std::marker::PhantomData,
        })
    }
}

// RangeIndex removed in favor of direct Redb range-scans in export_page_ranges_delta.

/// A simple guard to apply multiple delta chunks, then finalize once.
pub struct DeltaApplySession<'a, C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]> + std::fmt::Debug,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    store: &'a CrdtMstStore<C, H, T>,
    finalize: FinalizeStrategy,
    _mutation: tokio::sync::MutexGuard<'a, ()>,
}

impl<'a, C, H, T> DeltaApplySession<'a, C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]> + std::fmt::Debug,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    /// Apply one chunk of registers and tombstones (batched write).
    pub fn apply_chunk(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tombs: Tombstones<C::Key>,
    ) -> crate::Result<()> {
        self.store.apply_delta_chunk(regs, tombs).map_err(into_err)
    }

    /// Finalize once after all chunks (rebuild MST from disk).
    pub async fn commit(self) -> crate::Result<()> {
        match self.finalize {
            FinalizeStrategy::Rebuild => self.store.rebuild_mst_from_disk_unlocked().await,
            FinalizeStrategy::NoOp => Ok(()),
        }
    }

    /// Choose commit() finalize behavior. Default is Rebuild.
    pub fn with_finalize_strategy(mut self, s: FinalizeStrategy) -> Self {
        self.finalize = s;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FinalizeStrategy {
    Rebuild,
    NoOp,
}

/// Compute what we want from `remote` that is either missing locally or has a different digest.
pub fn compute_want_from_have(
    remote: &[PageDigestRange],
    local: &[PageDigestRange],
) -> Vec<PageDigestRange> {
    let mut idx: HashMap<(&[u8], &[u8]), &[u8]> = HashMap::with_capacity(local.len());
    for r in local {
        idx.insert((r.start.as_slice(), r.end.as_slice()), r.hash.as_slice());
    }

    let mut out = Vec::with_capacity(remote.len().min(1024));
    for r in remote {
        match idx.get(&(r.start.as_slice(), r.end.as_slice())) {
            None => out.push(r.clone()),
            Some(h) if *h != r.hash.as_slice() => out.push(r.clone()),
            _ => {}
        }
    }
    out
}

// HashBytes: collects the byte stream produced by `T: Hash` for debug hashing.
#[derive(Default, Clone)]
struct HashBytes(Vec<u8>);
impl HashBytes {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}
impl std::hash::Hasher for HashBytes {
    fn write(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
    fn finish(&self) -> u64 {
        // Only used for debug dumping; value irrelevant.
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{RegAdapter, StoreMvRegAdapterSorted};
    use crate::codec::{MvRegStoreCodec, StoreRegisterCodec, StoreValueCodec, TombstoneRecord};
    use crate::gc::{GcBarrier, StoreGcPolicy, StoreGcReport};
    use crate::hash::XXHash128;
    use crate::mvreg::{MvReg, MvRegEntry, MvRegSnapshot, VectorClock};
    use crate::uuid_key::UuidKey;
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    // Use the production hasher from the crate for tests

    // ------- Minimal TableSet for tests -------
    struct TestTables;
    impl TableSet for TestTables {
        const VALUES: &'static str = "values";
        const TOMBS: &'static str = "tombs";
        const TOMBS_BY_OBSERVED: &'static str = "tombs_by_observed";
        const META: &'static str = "meta";
    }

    type Adapter = StoreMvRegAdapterSorted<UuidKey, String, Uuid>;

    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct VersionedValue {
        name: String,
        alias: String,
    }

    impl StoreValueCodec for VersionedValue {
        fn encode_store_value(&self) -> crate::Result<Vec<u8>> {
            let name = self.name.as_bytes();
            let alias = self.alias.as_bytes();
            let mut encoded = Vec::with_capacity(8 + name.len() + alias.len());
            encoded.extend_from_slice(&(name.len() as u32).to_le_bytes());
            encoded.extend_from_slice(name);
            encoded.extend_from_slice(&(alias.len() as u32).to_le_bytes());
            encoded.extend_from_slice(alias);
            Ok(encoded)
        }

        fn decode_store_value(bytes: &[u8]) -> crate::Result<Self> {
            let (name, rest) = read_string_field(bytes, "name")?;
            let (alias, rest) = read_string_field(rest, "alias")?;
            if !rest.is_empty() {
                return Err(Box::new(Error::Other(
                    "versioned value payload has trailing bytes".to_string(),
                )));
            }
            Ok(Self { name, alias })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct VersionedProjection {
        name: String,
        alias: Option<String>,
    }

    struct VersionedAdapter;

    impl RegAdapter for VersionedAdapter {
        type Key = UuidKey;
        type Actor = Uuid;
        type Reg = MvReg<VersionedValue, Uuid>;
        type Value = VersionedValue;
        type Snapshot = MvRegSnapshot<VersionedProjection>;

        fn upsert_reg(
            current: Option<Self::Reg>,
            actor: &Self::Actor,
            v: Self::Value,
        ) -> Self::Reg {
            let mut reg = current.unwrap_or_default();
            reg.write(*actor, v);
            reg
        }

        fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
            Self::snapshot_reg_at_version(reg, 1)
        }

        fn snapshot_reg_at_version(reg: &Self::Reg, root_schema_version: u32) -> Self::Snapshot {
            let values = reg
                .read_values()
                .into_iter()
                .map(|value| VersionedProjection {
                    name: value.name,
                    alias: (root_schema_version >= 2).then_some(value.alias),
                })
                .collect::<Vec<_>>();
            MvRegSnapshot::from_unsorted(values)
        }

        fn key_to_bytes(k: &Self::Key) -> Vec<u8> {
            k.as_ref().to_vec()
        }

        fn key_from_bytes(b: &[u8]) -> std::io::Result<Self::Key> {
            UuidKey::try_from(b).map_err(Into::into)
        }

        fn actor_to_bytes(actor: &Self::Actor) -> Vec<u8> {
            actor.as_bytes().to_vec()
        }

        fn actor_from_bytes(bytes: &[u8]) -> std::io::Result<Self::Actor> {
            Uuid::from_slice(bytes).map_err(|error| std::io::Error::other(error.to_string()))
        }

        fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>> {
            MvRegStoreCodec::<VersionedValue, Uuid>::encode_store_reg(reg)
        }

        fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg> {
            MvRegStoreCodec::<VersionedValue, Uuid>::decode_store_reg(bytes)
        }

        fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg {
            match current {
                Some(mut current) => {
                    current.merge(incoming);
                    current
                }
                None => incoming,
            }
        }
    }

    struct CompactingAdapter;

    impl RegAdapter for CompactingAdapter {
        type Key = UuidKey;
        type Actor = Uuid;
        type Reg = MvReg<String, Uuid>;
        type Value = String;
        type Snapshot = MvRegSnapshot<String>;

        fn upsert_reg(
            current: Option<Self::Reg>,
            actor: &Self::Actor,
            v: Self::Value,
        ) -> Self::Reg {
            let mut reg = current.unwrap_or_default();
            reg.write(*actor, v);
            reg
        }

        fn snapshot_reg(reg: &Self::Reg) -> Self::Snapshot {
            reg.snapshot()
        }

        fn key_to_bytes(k: &Self::Key) -> Vec<u8> {
            k.as_ref().to_vec()
        }

        fn key_from_bytes(b: &[u8]) -> std::io::Result<Self::Key> {
            UuidKey::try_from(b).map_err(Into::into)
        }

        fn actor_to_bytes(actor: &Self::Actor) -> Vec<u8> {
            actor.as_bytes().to_vec()
        }

        fn actor_from_bytes(bytes: &[u8]) -> std::io::Result<Self::Actor> {
            Uuid::from_slice(bytes).map_err(|error| std::io::Error::other(error.to_string()))
        }

        fn encode_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>> {
            MvRegStoreCodec::<String, Uuid>::encode_store_reg(reg)
        }

        fn decode_reg(bytes: &[u8]) -> crate::Result<Self::Reg> {
            MvRegStoreCodec::<String, Uuid>::decode_store_reg(bytes)
        }

        fn compact_reg(mut reg: Self::Reg, max_values: usize) -> crate::Result<Option<Self::Reg>> {
            Ok(reg
                .compact_with(max_values, |entry| entry.value().clone())
                .then_some(reg))
        }

        fn merge_regs(current: Option<Self::Reg>, incoming: Self::Reg) -> Self::Reg {
            match current {
                Some(mut current) => {
                    current.merge(incoming);
                    current
                }
                None => incoming,
            }
        }
    }

    fn temp_db() -> (TempDir, Arc<redb::Database>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mst.redb");
        (dir, Arc::new(redb::Database::create(path).unwrap()))
    }

    fn key(n: u8) -> UuidKey {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        UuidKey::try_from(&bytes[..]).unwrap()
    }

    fn actor(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    fn clock(actor: Uuid, counter: u64) -> VectorClock<Uuid> {
        let mut clock = VectorClock::new();
        clock.apply(actor, counter);
        clock
    }

    fn reg_entry(actor: Uuid, counter: u64, value: &str) -> MvRegEntry<String, Uuid> {
        MvRegEntry::new(clock(actor, counter), value.to_string())
    }

    fn concurrent_reg(values: &[(u128, u64, &str)]) -> MvReg<String, Uuid> {
        let entries = values
            .iter()
            .map(|(actor_id, counter, value)| reg_entry(actor(*actor_id), *counter, value))
            .collect::<Vec<_>>();
        MvReg::from_entries(entries)
    }

    fn tombstone_gc_policy(retention_ms: u64, batch_limit: usize) -> StoreGcPolicy {
        StoreGcPolicy {
            tombstone_min_retention_ms: retention_ms,
            tombstone_batch_limit: batch_limit,
            mvreg_batch_limit: 0,
            mvreg_max_values: None,
        }
    }

    fn mvreg_compaction_policy(max_values: usize, batch_limit: usize) -> StoreGcPolicy {
        StoreGcPolicy {
            tombstone_min_retention_ms: 0,
            tombstone_batch_limit: 0,
            mvreg_batch_limit: batch_limit,
            mvreg_max_values: Some(max_values),
        }
    }

    fn tombstone_gc_barrier(safe_observed_before_unix_ms: u64) -> GcBarrier {
        GcBarrier {
            safe_observed_before_unix_ms,
            active_peer_count: 1,
            root_schema_version: DEFAULT_ROOT_SCHEMA_VERSION,
        }
    }

    fn read_string_field<'a>(bytes: &'a [u8], field: &str) -> crate::Result<(String, &'a [u8])> {
        if bytes.len() < 4 {
            return Err(Box::new(Error::Other(format!(
                "versioned value {field} length is missing"
            ))));
        }
        let len = u32::from_le_bytes(bytes[0..4].try_into().expect("length width")) as usize;
        let end = 4usize.saturating_add(len);
        if bytes.len() < end {
            return Err(Box::new(Error::Other(format!(
                "versioned value {field} is truncated"
            ))));
        }
        let value = String::from_utf8(bytes[4..end].to_vec()).map_err(|error| {
            Box::new(Error::Other(format!(
                "versioned value {field} is not valid UTF-8: {error}"
            )))
        })?;
        Ok((value, &bytes[end..]))
    }

    #[tokio::test]
    async fn upsert_and_remove_and_rebuild() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        // Upsert a few keys
        for n in 1..=3u8 {
            store.upsert(&key(n), format!("v{n}")).await.unwrap();
        }

        // Remove one key
        let k = key(2);
        let ts = store.remove(&k).await.unwrap();
        assert!(ts > 0);
        assert!(!store.exists(&k).unwrap());

        // Tombstone reflected in MST root
        let root = store.root_hex().await;
        assert!(!root.is_empty());
    }

    #[tokio::test]
    async fn root_digest_matches_hex() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        // Mutate so we have a non-empty root
        store.upsert(&key(1), "v1".into()).await.unwrap();
        store.rebuild_mst_from_disk().await.unwrap();

        let s = store.root_hex().await; // string representation from MST
        let d = store.root_digest().await; // raw bytes
        // The MerkleSearchTree's root to_string() is base64 over the raw digest bytes.
        use base64::Engine as _;
        let expect = base64::engine::general_purpose::STANDARD.encode(d);
        assert_eq!(s, expect);
    }

    #[tokio::test]
    async fn get_snapshot_returns_current_value() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        let k = key(9);
        store.upsert(&k, "alpha".into()).await.unwrap();
        let snap = store.get_snapshot(&k).unwrap().unwrap();
        assert_eq!(snap.as_slice(), &[String::from("alpha")]);

        // Update MVReg by same actor; snapshot should be replaced with the latest value
        store.upsert(&k, "beta".into()).await.unwrap();
        let snap2 = store.get_snapshot(&k).unwrap().unwrap();
        assert_eq!(snap2.as_slice(), &[String::from("beta")]);
    }

    #[tokio::test]
    async fn merge_register_clears_tombstone() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        let k = key(1);
        store.apply_tombstone(&k, 42).await.unwrap();
        assert!(!store.exists(&k).unwrap());

        let reg = {
            let current = None;
            <Adapter as RegAdapter>::upsert_reg(current, &actor(1), "hello".to_string())
        };
        store.merge_register(&k, &reg).await.unwrap();

        assert!(store.exists(&k).unwrap());
        let (_actives, tombs) = store.load_all().unwrap();
        assert!(tombs.is_empty());
    }

    #[tokio::test]
    async fn export_page_ranges_delta_optimized() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        // upsert keys 1..=5; tombstone key 3
        for n in 1..=5u8 {
            let k = key(n);
            store.upsert(&k, format!("v{n}")).await.unwrap();
        }
        store.remove(&key(3)).await.unwrap();

        // Build MST so ranges exist
        store.rebuild_mst_from_disk().await.unwrap();

        // Ask for ranges that cover keys 2..4
        let want = vec![PageDigestRange {
            start: <Adapter as RegAdapter>::key_to_bytes(&key(2)),
            end: <Adapter as RegAdapter>::key_to_bytes(&key(4)),
            hash: vec![],
        }];

        let (regs, tombs) = store.export_page_ranges_delta(&want).unwrap();
        let mut reg_keys: Vec<_> = regs.into_iter().map(|(k, _)| k).collect();
        let mut tmb_keys: Vec<_> = tombs.into_iter().map(|(k, _)| k).collect();
        reg_keys.sort();
        tmb_keys.sort();

        assert_eq!(reg_keys, vec![key(2), key(4)]);
        assert_eq!(tmb_keys, vec![key(3)]);
    }

    #[test]
    fn want_computation() {
        let r = |s: u8, e: u8, h: u8| PageDigestRange {
            start: vec![s],
            end: vec![e],
            hash: vec![h],
        };
        let remote = vec![r(1, 2, 9), r(3, 4, 8), r(5, 6, 7)];
        let local = vec![r(1, 2, 9), r(3, 4, 0) /* diff hash */];

        let want = compute_want_from_have(&remote, &local);
        assert_eq!(want.len(), 2);
        assert!(want.iter().any(|w| w.start == vec![3] && w.end == vec![4]));
        assert!(want.iter().any(|w| w.start == vec![5] && w.end == vec![6]));
    }

    #[tokio::test]
    async fn apply_tombstone_uses_monotonic_ts_in_mst() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let k = key(1);

        // First a higher remote ts arrives
        store.apply_tombstone(&k, 100).await.unwrap();
        let root_after_high = store.root_hex().await;
        // Then a stale lower ts arrives
        store.apply_tombstone(&k, 10).await.unwrap();
        let root_after_lower = store.root_hex().await;

        // Disk must hold 100, and MST leaf must also be 100 (not 10)
        let (_, tombs) = store.load_all().unwrap();
        assert_eq!(tombs[0].1.sequence, 100);
        assert_eq!(root_after_high, root_after_lower);
        store.apply_tombstone(&k, 10).await.unwrap();
        let root_after = store.root_hex().await;
        assert_eq!(root_after_lower, root_after);
    }

    #[test]
    fn tombstone_prune_frontier_advances_monotonically() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(2));

        assert_eq!(store.tombstone_prune_frontier(&origin).unwrap(), 0);

        store.advance_tombstone_prune_frontier(&origin, 9).unwrap();
        store.advance_tombstone_prune_frontier(&origin, 3).unwrap();

        assert_eq!(store.tombstone_prune_frontier(&origin).unwrap(), 9);
    }

    #[tokio::test]
    async fn apply_tombstone_ignores_pruned_local_sequence() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(1));
        let k = key(1);

        store.advance_tombstone_prune_frontier(&origin, 10).unwrap();
        let change_clock_before = store.change_clock();

        store.apply_tombstone(&k, 10).await.unwrap();

        assert!(!store.has_tombstone(&k).unwrap());
        assert_eq!(store.change_clock(), change_clock_before);

        store.apply_tombstone(&k, 11).await.unwrap();
        let (_actives, tombs) = store.load_all().unwrap();

        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0].0, k);
        assert_eq!(tombs[0].1.sequence, 11);
        assert_eq!(tombs[0].1.origin_actor, origin);
    }

    #[tokio::test]
    async fn delta_tombstone_ignores_pruned_remote_sequence() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(2));
        let k = key(1);

        store.advance_tombstone_prune_frontier(&origin, 7).unwrap();

        let stale_tombstone = TombstoneRecord::new(7, origin.clone(), 0);
        store
            .apply_delta_chunk_update_mst(Vec::new(), vec![(k, stale_tombstone)])
            .await
            .unwrap();

        assert!(!store.has_tombstone(&k).unwrap());

        let fresh_tombstone = TombstoneRecord::new(8, origin.clone(), 0);
        store
            .apply_delta_chunk_update_mst(Vec::new(), vec![(k, fresh_tombstone)])
            .await
            .unwrap();
        let (_actives, tombs) = store.load_all().unwrap();

        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0].0, k);
        assert_eq!(tombs[0].1.sequence, 8);
        assert_eq!(tombs[0].1.origin_actor, origin);
        assert!(tombs[0].1.observed_at_unix_ms > 0);
    }

    #[tokio::test]
    async fn tombstone_gc_prunes_eligible_rows_and_advances_frontier() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(2));

        store
            .apply_delta_chunk_update_mst(
                Vec::new(),
                vec![
                    (key(1), TombstoneRecord::new(1, origin.clone(), 10)),
                    (key(2), TombstoneRecord::new(2, origin.clone(), 20)),
                    (key(3), TombstoneRecord::new(3, origin.clone(), 50)),
                ],
            )
            .await
            .unwrap();
        let root_before = store.root_hex().await;

        let report = store
            .garbage_collect_tombstones(&tombstone_gc_policy(0, 10), tombstone_gc_barrier(30), 100)
            .await
            .unwrap();
        let root_after = store.root_hex().await;
        let (_actives, mut tombs) = store.load_all().unwrap();
        tombs.sort_by_key(|(key, _)| *key);

        assert_eq!(
            report,
            StoreGcReport {
                tombstones_scanned: 2,
                tombstones_pruned: 2,
                registers_scanned: 0,
                registers_compacted: 0,
            }
        );
        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0].0, key(3));
        assert_eq!(tombs[0].1.sequence, 3);
        assert_eq!(store.tombstone_prune_frontier(&origin).unwrap(), 2);
        assert_ne!(root_before, root_after);

        store.rebuild_mst_from_disk().await.unwrap();
        assert_eq!(root_after, store.root_hex().await);

        let stale_tombstone = TombstoneRecord::new(2, origin.clone(), 0);
        store
            .apply_delta_chunk_update_mst(Vec::new(), vec![(key(1), stale_tombstone)])
            .await
            .unwrap();
        assert!(!store.has_tombstone(&key(1)).unwrap());

        let fresh_tombstone = TombstoneRecord::new(4, origin.clone(), 0);
        store
            .apply_delta_chunk_update_mst(Vec::new(), vec![(key(1), fresh_tombstone)])
            .await
            .unwrap();
        assert!(store.has_tombstone(&key(1)).unwrap());
    }

    #[tokio::test]
    async fn applying_remote_prune_frontier_prunes_matching_local_tombstones() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(2));

        store
            .apply_delta_chunk_update_mst(
                Vec::new(),
                vec![
                    (key(1), TombstoneRecord::new(1, origin.clone(), 10)),
                    (key(2), TombstoneRecord::new(2, origin.clone(), 20)),
                    (key(3), TombstoneRecord::new(3, origin.clone(), 30)),
                ],
            )
            .await
            .unwrap();
        let root_before = store.root_hex().await;

        let pruned = store
            .apply_tombstone_prune_frontiers(vec![(origin.clone(), 2)])
            .await
            .unwrap();
        let root_after = store.root_hex().await;
        let (_actives, mut tombs) = store.load_all().unwrap();
        tombs.sort_by_key(|(key, _)| *key);

        assert_eq!(pruned, 2);
        assert_eq!(store.tombstone_prune_frontier(&origin).unwrap(), 2);
        assert_eq!(
            store.load_tombstone_prune_frontiers().unwrap(),
            vec![(origin.clone(), 2)]
        );
        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0].0, key(3));
        assert_ne!(root_before, root_after);

        store
            .apply_delta_chunk_update_mst(
                Vec::new(),
                vec![(key(1), TombstoneRecord::new(2, origin.clone(), 0))],
            )
            .await
            .unwrap();
        assert!(!store.has_tombstone(&key(1)).unwrap());
    }

    #[tokio::test]
    async fn tombstone_gc_respects_retention_and_barrier_boundary() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(2));

        store
            .apply_delta_chunk_update_mst(
                Vec::new(),
                vec![
                    (key(1), TombstoneRecord::new(1, origin.clone(), 10)),
                    (key(2), TombstoneRecord::new(2, origin.clone(), 20)),
                    (key(3), TombstoneRecord::new(3, origin.clone(), 90)),
                ],
            )
            .await
            .unwrap();

        let report = store
            .garbage_collect_tombstones(&tombstone_gc_policy(10, 10), tombstone_gc_barrier(20), 100)
            .await
            .unwrap();
        let (_actives, mut tombs) = store.load_all().unwrap();
        tombs.sort_by_key(|(key, _)| *key);

        assert_eq!(report.tombstones_scanned, 1);
        assert_eq!(report.tombstones_pruned, 1);
        assert_eq!(
            tombs.into_iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec![key(2), key(3)]
        );
        assert_eq!(store.tombstone_prune_frontier(&origin).unwrap(), 1);
    }

    #[tokio::test]
    async fn tombstone_gc_honors_batch_limit() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let origin = <Adapter as RegAdapter>::actor_to_bytes(&actor(2));

        store
            .apply_delta_chunk_update_mst(
                Vec::new(),
                vec![
                    (key(1), TombstoneRecord::new(1, origin.clone(), 10)),
                    (key(2), TombstoneRecord::new(2, origin.clone(), 11)),
                    (key(3), TombstoneRecord::new(3, origin.clone(), 12)),
                ],
            )
            .await
            .unwrap();

        let report = store
            .garbage_collect_tombstones(&tombstone_gc_policy(0, 2), tombstone_gc_barrier(100), 100)
            .await
            .unwrap();
        let (_actives, mut tombs) = store.load_all().unwrap();
        tombs.sort_by_key(|(key, _)| *key);

        assert_eq!(report.tombstones_scanned, 2);
        assert_eq!(report.tombstones_pruned, 2);
        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0].0, key(3));
        assert_eq!(store.tombstone_prune_frontier(&origin).unwrap(), 2);
    }

    #[tokio::test]
    async fn register_compaction_rewrites_opt_in_adapter_and_updates_root() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<CompactingAdapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let compacted_key = key(1);

        store
            .apply_delta_chunk_update_mst(
                vec![(
                    compacted_key,
                    concurrent_reg(&[(1, 1, "old"), (2, 1, "winner")]),
                )],
                Vec::new(),
            )
            .await
            .unwrap();

        let report = store
            .compact_registers(&mvreg_compaction_policy(1, 10))
            .await
            .unwrap();
        let root_after = store.root_hex().await;
        let reg = store.get_reg(&compacted_key).unwrap().unwrap();

        assert_eq!(
            report,
            StoreGcReport {
                tombstones_scanned: 0,
                tombstones_pruned: 0,
                registers_scanned: 1,
                registers_compacted: 1,
            }
        );
        assert_eq!(reg.read_values(), vec!["winner".to_string()]);

        store.rebuild_mst_from_disk().await.unwrap();
        assert_eq!(root_after, store.root_hex().await);
    }

    #[tokio::test]
    async fn register_compaction_absorbs_stale_values() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<CompactingAdapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();
        let compacted_key = key(1);

        store
            .apply_delta_chunk_update_mst(
                vec![(
                    compacted_key,
                    concurrent_reg(&[(1, 1, "old"), (2, 1, "winner")]),
                )],
                Vec::new(),
            )
            .await
            .unwrap();
        store
            .compact_registers(&mvreg_compaction_policy(1, 10))
            .await
            .unwrap();

        store
            .apply_delta_chunk_update_mst(
                vec![(compacted_key, concurrent_reg(&[(1, 1, "old")]))],
                Vec::new(),
            )
            .await
            .unwrap();

        let reg = store.get_reg(&compacted_key).unwrap().unwrap();
        assert_eq!(reg.read_values(), vec!["winner".to_string()]);
    }

    #[tokio::test]
    async fn register_compaction_honors_batch_limit() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<CompactingAdapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        store
            .apply_delta_chunk_update_mst(
                vec![
                    (key(1), concurrent_reg(&[(1, 1, "a"), (2, 1, "z")])),
                    (key(2), concurrent_reg(&[(1, 1, "b"), (2, 1, "y")])),
                ],
                Vec::new(),
            )
            .await
            .unwrap();

        let report = store
            .compact_registers(&mvreg_compaction_policy(1, 1))
            .await
            .unwrap();
        let first = store.get_reg(&key(1)).unwrap().unwrap();
        let second = store.get_reg(&key(2)).unwrap().unwrap();

        assert_eq!(report.registers_scanned, 1);
        assert_eq!(report.registers_compacted, 1);
        assert_eq!(first.read_values(), vec!["z".to_string()]);
        assert_eq!(second.read_values().len(), 2);
    }

    #[tokio::test]
    async fn register_compaction_defaults_to_adapter_noop() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        store
            .apply_delta_chunk_update_mst(
                vec![(key(1), concurrent_reg(&[(1, 1, "a"), (2, 1, "b")]))],
                Vec::new(),
            )
            .await
            .unwrap();

        let report = store
            .compact_registers(&mvreg_compaction_policy(1, 10))
            .await
            .unwrap();
        let reg = store.get_reg(&key(1)).unwrap().unwrap();

        assert_eq!(report.registers_scanned, 1);
        assert_eq!(report.registers_compacted, 0);
        assert_eq!(reg.read_values().len(), 2);
    }

    #[tokio::test]
    async fn apply_delta_chunk_update_mst_keeps_newer_local_tombstone() {
        let (_local_dir, local_db) = temp_db();
        let (_remote_dir, remote_db) = temp_db();
        let local: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(local_db, actor(1)).unwrap();
        let remote: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(remote_db, actor(2)).unwrap();
        let tomb_key = key(1);

        // Give the local store a newer tombstone sequence than the remote store.
        local.remove(&key(9)).await.unwrap();
        local.remove(&tomb_key).await.unwrap();
        remote.remove(&tomb_key).await.unwrap();

        let root_before = local.root_hex().await;
        let remote_ranges = remote.page_range_summary().await.unwrap();
        let (regs, tombs) = remote.export_page_ranges_delta(&remote_ranges).unwrap();

        local
            .apply_delta_chunk_update_mst(regs, tombs)
            .await
            .unwrap();

        let (_, local_tombs) = local.load_all().unwrap();
        let local_ts = local_tombs
            .into_iter()
            .find(|(key, _)| *key == tomb_key)
            .map(|(_, tombstone)| tombstone.sequence)
            .expect("local tombstone present");
        let root_after = local.root_hex().await;

        assert_eq!(local_ts, 2);
        assert_eq!(root_before, root_after);
    }

    #[tokio::test]
    async fn export_overlap_ranges_are_deduped() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        for n in 1..=5u8 {
            store.upsert(&key(n), format!("v{n}")).await.unwrap();
        }
        store.remove(&key(3)).await.unwrap();
        store.rebuild_mst_from_disk().await.unwrap();

        // Two overlapping wants: [2,4] and [3,5]
        let want = vec![
            PageDigestRange {
                start: <Adapter as RegAdapter>::key_to_bytes(&key(2)),
                end: <Adapter as RegAdapter>::key_to_bytes(&key(4)),
                hash: vec![],
            },
            PageDigestRange {
                start: <Adapter as RegAdapter>::key_to_bytes(&key(3)),
                end: <Adapter as RegAdapter>::key_to_bytes(&key(5)),
                hash: vec![],
            },
        ];

        let (regs, tombs) = store.export_page_ranges_delta(&want).unwrap();
        let mut reg_keys: Vec<_> = regs.into_iter().map(|(k, _)| k).collect();
        let mut tmb_keys: Vec<_> = tombs.into_iter().map(|(k, _)| k).collect();
        reg_keys.sort();
        tmb_keys.sort();

        assert_eq!(reg_keys, vec![key(2), key(4), key(5)]);
        assert_eq!(tmb_keys, vec![key(3)]);
    }

    #[tokio::test]
    async fn delta_apply_session_commit_rebuilds_once() {
        let (_src_dir, db_src) = temp_db();
        let (_dst_dir, db_dst) = temp_db();
        let src: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db_src, actor(1)).unwrap();
        let dst: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db_dst, actor(2)).unwrap();

        // Populate source
        for n in 1..=4u8 {
            src.upsert(&key(n), format!("v{n}")).await.unwrap();
        }
        src.remove(&key(3)).await.unwrap();
        src.rebuild_mst_from_disk().await.unwrap();

        // Export everything from source
        let want = src.page_range_summary().await.unwrap();
        let (regs, tombs) = src.export_page_ranges_delta(&want).unwrap();

        // Apply via session and commit
        let sess = dst.begin_delta_apply().await;
        sess.apply_chunk(regs, tombs).unwrap();
        sess.commit().await.unwrap();

        // Roots match after commit
        assert_eq!(src.root_hex().await, dst.root_hex().await);
    }

    #[tokio::test]
    async fn apply_delta_chunk_update_mst_keeps_tree_fresh() {
        let (_dir, db) = temp_db();
        let src: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db.clone(), actor(1)).unwrap();
        let dst: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(2)).unwrap();

        src.upsert(&key(1), "x".into()).await.unwrap();
        src.upsert(&key(2), "y".into()).await.unwrap();
        src.remove(&key(1)).await.unwrap();
        src.rebuild_mst_from_disk().await.unwrap();

        let want = src.page_range_summary().await.unwrap();
        let (regs, tombs) = src.export_page_ranges_delta(&want).unwrap();

        // Apply and update MST incrementally
        dst.apply_delta_chunk_update_mst(regs, tombs).await.unwrap();

        // No finalize call; roots should already match
        assert_eq!(src.root_hex().await, dst.root_hex().await);
    }

    #[tokio::test]
    async fn root_schema_versions_change_snapshot_projection_without_losing_raw_registers() {
        let (_dir, db) = temp_db();
        let store: CrdtMstStore<VersionedAdapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, actor(1)).unwrap();

        let k = key(7);
        store
            .upsert(
                &k,
                VersionedValue {
                    name: "alpha".to_string(),
                    alias: "v2-visible".to_string(),
                },
            )
            .await
            .unwrap();

        let snapshot_v1 = store.get_snapshot(&k).unwrap().unwrap();
        assert_eq!(
            snapshot_v1.as_slice(),
            &[VersionedProjection {
                name: "alpha".to_string(),
                alias: None,
            }]
        );

        let root_v1 = store.root_digest_at_version(1).await.unwrap();
        assert_eq!(store.current_root_schema_version(), 1);
        assert_eq!(store.root_digest().await, root_v1);

        let root_v2 = store.root_digest_at_version(2).await.unwrap();
        assert_ne!(root_v1, root_v2);

        let ranges_v1 = store.page_range_summary_at_version(1).await.unwrap();
        let ranges_v2 = store.page_range_summary_at_version(2).await.unwrap();
        assert_ne!(ranges_v1, ranges_v2);

        assert_eq!(store.current_root_schema_version(), 1);
        assert_eq!(store.root_digest().await, root_v1);
        let snapshot_after_non_current_reads = store.get_snapshot(&k).unwrap().unwrap();
        assert_eq!(snapshot_after_non_current_reads, snapshot_v1);

        let raw_reg = store.get_reg(&k).unwrap().unwrap();
        assert_eq!(
            raw_reg.read_values(),
            vec![VersionedValue {
                name: "alpha".to_string(),
                alias: "v2-visible".to_string(),
            }]
        );

        store.rebuild_mst_from_disk_at_version(2).await.unwrap();
        assert_eq!(store.current_root_schema_version(), 2);
        assert_eq!(store.root_digest().await, root_v2);

        let snapshot_v2 = store.get_snapshot(&k).unwrap().unwrap();
        assert_eq!(
            snapshot_v2.as_slice(),
            &[VersionedProjection {
                name: "alpha".to_string(),
                alias: Some("v2-visible".to_string()),
            }]
        );
    }
}
