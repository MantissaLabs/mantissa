//! CRDT + Merkle Search Tree backed store with tombstones.
//!
//! - Durable per-key CRDT registers in redb.
//! - Durable tombstones for deletions.
//! - In-memory Merkle Search Tree (MST) over (Key, Entry<Snapshot>).
//!
//! This module exposes fast range-based delta export/import primitives to power
//! anti-entropy sync between peers.

// base64 used only in debug helpers/tests; prefer fully-qualified calls to avoid unused imports.
use merkle_search_tree::digest::Hasher as MstHasher;
use merkle_search_tree::{MerkleSearchTree, builder::Builder};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{hash::Hash, io, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

use crate::adapter::RegAdapter;
use crate::error::Error;
use crate::table_set::TableSet;

/// Value stored in each MST leaf.
/// Active leaves carry a CRDT snapshot; Deleted leaves carry a tombstone sequence.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry<S> {
    Active(S),
    Deleted { ts: u64 },
}

/// List of `(Key, Snapshot)` pairs.
pub type Snapshots<K, S> = Vec<(K, S)>;

/// List of `(Key, Reg)` pairs.
pub type Registers<K, R> = Vec<(K, R)>;

/// List of `(Key, tombstone_ts)` pairs.
pub type Tombstones<K> = Vec<(K, u64)>;

/// Tuple of `(Snapshots, Tombstones)` returned by bulk loaders.
pub type SnapshotsAndTombs<K, S> = (Snapshots<K, S>, Tombstones<K>);

/// Tuple of `(Registers, Tombstones)` returned by delta exporters.
pub type RegistersAndTombs<K, R> = (Registers<K, R>, Tombstones<K>);

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
            Entry::Deleted { ts } => {
                state.write_u8(1);
                state.write_u64(*ts);
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
    pub fn begin_delta_apply(&self) -> DeltaApplySession<'_, C, H, T> {
        DeltaApplySession {
            store: self,
            finalize: FinalizeStrategy::Rebuild,
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

    #[inline]
    fn encode_reg(r: &C::Reg) -> crate::Result<Vec<u8>> {
        bincode::serialize(r).map_err(into_err)
    }

    #[inline]
    fn decode_reg(bytes: &[u8]) -> crate::Result<C::Reg> {
        bincode::deserialize(bytes).map_err(into_err)
    }

    #[inline]
    fn encode_key(k: &C::Key) -> Vec<u8> {
        C::key_to_bytes(k)
    }

    #[inline]
    fn decode_key(bytes: &[u8]) -> crate::Result<C::Key> {
        C::key_from_bytes(bytes).map_err(into_err)
    }

    /// Rebuild the in-memory MST from durable registers + tombstones.
    pub async fn rebuild_mst_from_disk(&self) -> crate::Result<()> {
        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let tombs = r.open_table(T::tombs()).map_err(into_err)?;

        // Collect snapshots
        let mut actives: Vec<(C::Key, C::Snapshot)> = {
            let mut out = Vec::new();
            let mut it = values.iter().map_err(into_err)?;
            while let Some(Ok((k_guard, v_guard))) = it.next() {
                let key = Self::decode_key(k_guard.value())?;
                let reg = Self::decode_reg(v_guard.value())?;
                out.push((key, C::snapshot_reg(&reg)));
            }
            out
        };
        actives.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));

        let mut tomb_list: Vec<(C::Key, u64)> = {
            let mut out = Vec::new();
            let mut it = tombs.iter().map_err(into_err)?;
            while let Some(Ok((k_guard, ts_guard))) = it.next() {
                out.push((Self::decode_key(k_guard.value())?, ts_guard.value()));
            }
            out
        };
        tomb_list.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));

        // Rebuild
        let mut tree = Builder::default().with_hasher(H::default()).build();
        for (k, snap) in actives {
            tree.upsert(k, &Entry::Active(snap));
        }
        for (k, ts) in tomb_list {
            tree.upsert(k, &Entry::Deleted { ts });
        }

        *self.mst.write().await = tree;
        Ok(())
    }

    /// Replace the current in-memory MST with one built from given entries.
    pub async fn rebuild_mst<Ia, It>(&self, actives: Ia, tombs: It)
    where
        Ia: IntoIterator<Item = (C::Key, C::Snapshot)>,
        It: IntoIterator<Item = (C::Key, u64)>,
    {
        let mut t = Builder::default().with_hasher(H::default()).build();
        for (k, s) in actives {
            t.upsert(k, &Entry::Active(s));
        }
        for (k, ts) in tombs {
            t.upsert(k, &Entry::Deleted { ts });
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

    /// Advance the internal monotonic change counter after a successful write.
    #[inline]
    fn bump_change_clock(&self) {
        self.change_clock.fetch_add(1, Ordering::Release);
    }

    /// Expose the current change counter so callers can detect when cached views are stale.
    pub fn change_clock(&self) -> u64 {
        self.change_clock.load(Ordering::Acquire)
    }

    /// Insert or update value for key `k`.
    pub async fn upsert(&self, k: &C::Key, v: C::Value) -> crate::Result<()> {
        // Load current register (if any)
        let current = {
            let r = self.db.begin_read().map_err(into_err)?;
            let t = r.open_table(T::values()).map_err(into_err)?;
            match t.get(Self::encode_key(k).as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            }
        };

        let new_reg = C::upsert_reg(current, &self.actor, v);
        let snap = C::snapshot_reg(&new_reg);

        // Persist: write register + clear tombstone
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            values
                .insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(&new_reg)?.as_slice(),
                )
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let _ = tombs
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        // Reflect in MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Active(snap));

        self.bump_change_clock();

        Ok(())
    }

    /// Remove key and persist a tombstone with a monotonic sequence.
    pub async fn remove(&self, k: &C::Key) -> crate::Result<u64> {
        // First check if tomb already exists; if so, do NOT allocate a new seq.
        let (already_tombstoned, needs_value_drop) = {
            let r = self.db.begin_read().map_err(into_err)?;
            let tombstones = r.open_table(T::tombs()).map_err(into_err)?;
            let values = r.open_table(T::values()).map_err(into_err)?;

            let kb = Self::encode_key(k);
            let tomb_ts = tombstones
                .get(kb.as_slice())
                .map_err(into_err)?
                .map(|g| g.value());
            let value_exists = values.get(kb.as_slice()).map_err(into_err)?.is_some();

            (tomb_ts, value_exists)
        };

        if let Some(ts) = already_tombstoned {
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
            t.upsert(k.clone(), &Entry::Deleted { ts });
            self.bump_change_clock();
            return Ok(ts);
        }

        // No tombstone yet: allocate a new sequence and persist.
        let w = self.db.begin_write().map_err(into_err)?;
        let ts = {
            let mut meta = w.open_table(T::meta()).map_err(into_err)?;
            let next = match meta.get("tomb_seq").map_err(into_err)? {
                Some(g) => g.value().saturating_add(1),
                None => 1,
            };
            meta.insert("tomb_seq", &next).map_err(into_err)?;
            next
        };
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            tombs
                .insert(Self::encode_key(k).as_slice(), &ts)
                .map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Deleted { ts });
        self.bump_change_clock();
        Ok(ts)
    }

    /// Purge a key locally without writing a tombstone so remote replicas can repopulate it.
    ///
    /// This is intended for recovery/testing scenarios where a local store is missing entries
    /// and should accept the next sync payload, not for user-facing delete operations.
    pub async fn purge_local(&self, k: &C::Key) -> crate::Result<()> {
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let _ = tombs
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        self.rebuild_mst_from_disk().await?;
        self.bump_change_clock();
        Ok(())
    }

    /// Merge a remote register for key `k` into durable state and MST, clearing any local tombstone.
    pub async fn merge_register(&self, k: &C::Key, incoming: &C::Reg) -> crate::Result<()> {
        // Read current reg (if any)
        let current = {
            let r = self.db.begin_read().map_err(into_err)?;
            let t = r.open_table(T::values()).map_err(into_err)?;
            match t.get(Self::encode_key(k).as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            }
        };

        // Merge and persist
        let merged = C::merge_regs(current, incoming.clone());
        let snap = C::snapshot_reg(&merged);

        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            values
                .insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(&merged)?.as_slice(),
                )
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let _ = tombs
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        // Update MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Active(snap));
        self.bump_change_clock();
        Ok(())
    }

    /// Apply an inbound tombstone (idempotent, monotonic).
    pub async fn apply_tombstone(&self, k: &C::Key, ts: u64) -> io::Result<()> {
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_err)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_err)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_err)?;
            let kb = Self::encode_key(k);
            let next_ts = match tombs.get(kb.as_slice()).map_err(into_err)? {
                Some(g) => g.value().max(ts),
                None => ts,
            };
            tombs.insert(kb.as_slice(), &next_ts).map_err(into_err)?;
        }
        w.commit().map_err(into_err)?;

        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Deleted { ts });
        self.bump_change_clock();
        Ok(())
    }

    /// Apply one streamed delta chunk (register merges + tombstones). Batches in a single write.
    pub fn apply_delta_chunk(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tombs: Tombstones<C::Key>,
    ) -> io::Result<()> {
        // Prepare merged registers by reading current values once.
        let merged_regs = self.prepare_merged_registers(regs, &tombs)?;

        // Track whether anything was written so we only advance the change clock when needed.
        let had_changes = !merged_regs.is_empty() || !tombs.is_empty();

        // Single write transaction for everything:
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut tv = w.open_table(T::values()).map_err(into_err)?;
            let mut tt = w.open_table(T::tombs()).map_err(into_err)?;

            // Apply merged registers (and clear any tombstone)
            for (k, reg) in &merged_regs {
                tv.insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(reg)?.as_slice(),
                )
                .map_err(into_err)?;
                let _ = tt
                    .remove(Self::encode_key(k).as_slice())
                    .map_err(into_err)?;
            }

            // Apply tombstones (and remove register rows)
            for (k, ts) in &tombs {
                tt.insert(Self::encode_key(k).as_slice(), ts)
                    .map_err(into_err)?;
                let _ = tv
                    .remove(Self::encode_key(k).as_slice())
                    .map_err(into_err)?;
            }
        }
        w.commit().map_err(into_err)?;

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
        // Prepare merged registers by reading current values once.
        let merged_regs = self.prepare_merged_registers(regs, &tombs)?;

        let had_changes = !merged_regs.is_empty() || !tombs.is_empty();

        // Single write transaction for everything:
        let w = self.db.begin_write().map_err(into_err)?;
        {
            let mut tv = w.open_table(T::values()).map_err(into_err)?;
            let mut tt = w.open_table(T::tombs()).map_err(into_err)?;

            for (k, reg) in &merged_regs {
                tv.insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(reg)?.as_slice(),
                )
                .map_err(into_err)?;
                let _ = tt
                    .remove(Self::encode_key(k).as_slice())
                    .map_err(into_err)?;
            }

            for (k, ts) in &tombs {
                tt.insert(Self::encode_key(k).as_slice(), ts)
                    .map_err(into_err)?;
                let _ = tv
                    .remove(Self::encode_key(k).as_slice())
                    .map_err(into_err)?;
            }
        }
        w.commit().map_err(into_err)?;

        // Reflect in MST in the same logical order: regs then tombs.
        {
            let mut t = self.mst.write().await;
            for (k, reg) in &merged_regs {
                let snap = C::snapshot_reg(reg);
                t.upsert(k.clone(), &Entry::Active(snap));
            }
            for (k, ts) in tombs {
                t.upsert(k.clone(), &Entry::Deleted { ts });
            }
        }

        if had_changes {
            self.bump_change_clock();
        }

        Ok(())
    }

    /// Prepare merged registers honoring the configured tombstone strategy.
    fn prepare_merged_registers(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tombs: &Tombstones<C::Key>,
    ) -> io::Result<Registers<C::Key, C::Reg>> {
        if !self.preserve_local_tombs {
            return self.merge_registers_unconditional(regs);
        }

        let tomb_keys_in_chunk: HashSet<C::Key> = if regs.is_empty() {
            HashSet::new()
        } else {
            tombs.iter().map(|(k, _)| k.clone()).collect()
        };
        self.merge_registers_guarding_local_tombs(regs, tomb_keys_in_chunk)
    }

    /// Merge incoming registers while ensuring local tombstones remain authoritative unless
    /// the current chunk also carries a tomb for the same key.
    fn merge_registers_guarding_local_tombs(
        &self,
        regs: Registers<C::Key, C::Reg>,
        tomb_keys_in_chunk: HashSet<C::Key>,
    ) -> io::Result<Registers<C::Key, C::Reg>> {
        if regs.is_empty() {
            return Ok(Vec::new());
        }

        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;
        let tomb_table = r.open_table(T::tombs()).map_err(into_err)?;

        let mut out = Vec::with_capacity(regs.len());
        for (k, incoming) in regs {
            let kb = Self::encode_key(&k);
            let skip_due_to_local_tomb = if tomb_keys_in_chunk.contains(&k) {
                false
            } else {
                tomb_table.get(kb.as_slice()).map_err(into_err)?.is_some()
            };

            if skip_due_to_local_tomb {
                continue;
            }

            let current = match values.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            };
            out.push((k, C::merge_regs(current, incoming)));
        }

        Ok(out)
    }

    /// Merge incoming registers without inspecting tombstone state.
    fn merge_registers_unconditional(
        &self,
        regs: Registers<C::Key, C::Reg>,
    ) -> io::Result<Registers<C::Key, C::Reg>> {
        if regs.is_empty() {
            return Ok(Vec::new());
        }

        let r = self.db.begin_read().map_err(into_err)?;
        let values = r.open_table(T::values()).map_err(into_err)?;

        let mut out = Vec::with_capacity(regs.len());
        for (k, incoming) in regs {
            let kb = Self::encode_key(&k);
            let current = match values.get(kb.as_slice()).map_err(into_err)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            };
            out.push((k, C::merge_regs(current, incoming)));
        }

        Ok(out)
    }

    /// Rebuild MST once after a sequence of `apply_delta_chunk()` calls.
    pub async fn finalize_after_stream(&self) -> crate::Result<()> {
        self.rebuild_mst_from_disk().await
    }

    /// Dump durable (key, snapshot) and (key, tombstone).
    pub fn load_all(&self) -> crate::Result<SnapshotsAndTombs<C::Key, C::Snapshot>> {
        let mut actives = Vec::new();
        let mut tombs = Vec::new();
        self.load_all_into(&mut actives, &mut tombs)?;
        Ok((actives, tombs))
    }

    /// Populate caller-provided buffers with all durable snapshots and tombstones so hot loops can reuse allocations.
    pub fn load_all_into(
        &self,
        actives: &mut Vec<(C::Key, C::Snapshot)>,
        tombs: &mut Vec<(C::Key, u64)>,
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
                actives.push((key, C::snapshot_reg(&reg)));
            }
        }

        {
            let mut it = tomb_table.iter().map_err(into_err)?;
            while let Some(Ok((k, ts))) = it.next() {
                tombs.push((Self::decode_key(k.value())?, ts.value()));
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
            f(key, C::snapshot_reg(&reg));
        }
        Ok(())
    }

    /// Visit all tombstones using a single read transaction without building a Vec.
    pub fn for_each_tombstone<F>(&self, mut f: F) -> crate::Result<()>
    where
        F: FnMut(C::Key, u64),
    {
        let r = self.db.begin_read().map_err(into_err)?;
        let tombs = r.open_table(T::tombs()).map_err(into_err)?;
        let mut it = tombs.iter().map_err(into_err)?;
        while let Some(Ok((k, ts))) = it.next() {
            f(Self::decode_key(k.value())?, ts.value());
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
                Ok(Some(C::snapshot_reg(&reg)))
            }
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
        let mut tombstones_out: Vec<(C::Key, u64)> = Vec::new();

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

            // Tombstones in [start, end]
            {
                let mut it = tombstones.range(start..=end).map_err(into_err)?;
                while let Some(Ok((k_g, ts_g))) = it.next() {
                    let k_bytes = k_g.value();
                    if !seen_tombs.insert(k_bytes.to_vec()) {
                        continue;
                    }
                    let key = Self::decode_key(k_bytes)?;
                    tombstones_out.push((key, ts_g.value()));
                }
            }
        }

        Ok((registers_out, tombstones_out))
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
            for (k, ts) in tombs {
                let mut sink = HashBytes::default();
                let e: Entry<C::Snapshot> = Entry::Deleted { ts };
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
        let _ = w.open_table(T::meta()).map_err(into_err)?;
        w.commit().map_err(into_err)?;

        let hasher = self.hasher.unwrap_or_default();
        let mst = Arc::new(RwLock::new(Builder::default().with_hasher(hasher).build()));
        Ok(CrdtMstStore {
            db: self.db,
            actor: self.actor,
            mst,
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
            FinalizeStrategy::Rebuild => self.store.finalize_after_stream().await,
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
    // Map (start,end) → hash
    let mut idx: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::with_capacity(local.len());
    for r in local {
        idx.insert((r.start.clone(), r.end.clone()), r.hash.clone());
    }

    let mut out = Vec::with_capacity(remote.len().min(1024));
    for r in remote {
        match idx.get(&(r.start.clone(), r.end.clone())) {
            None => out.push(r.clone()),
            Some(h) if h.as_slice() != r.hash.as_slice() => out.push(r.clone()),
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
    use crate::adapter::{MvRegAdapterSorted, RegAdapter};
    use crate::hash::XXHash128;
    use crate::uuid_key::UuidKey;
    use std::sync::Arc;
    use uuid::Uuid;

    // Use the production hasher from the crate for tests

    // ------- Minimal TableSet for tests -------
    struct TestTables;
    impl TableSet for TestTables {
        const VALUES: &'static str = "values";
        const TOMBS: &'static str = "tombs";
        const META: &'static str = "meta";
    }

    type Adapter = MvRegAdapterSorted<UuidKey, String, u8>;

    fn temp_db() -> Arc<redb::Database> {
        let path = std::env::temp_dir().join(format!("mst-test-{}.redb", Uuid::new_v4()));
        Arc::new(redb::Database::create(path).unwrap())
    }

    fn key(n: u8) -> UuidKey {
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        UuidKey::try_from(&bytes[..]).unwrap()
    }

    #[tokio::test]
    async fn upsert_and_remove_and_rebuild() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();

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
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();

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
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();

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
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();

        let k = key(1);
        store.apply_tombstone(&k, 42).await.unwrap();
        assert!(!store.exists(&k).unwrap());

        let reg = {
            let current = None;
            <Adapter as RegAdapter>::upsert_reg(current, &1u8, "hello".to_string())
        };
        store.merge_register(&k, &reg).await.unwrap();

        assert!(store.exists(&k).unwrap());
        let (_actives, tombs) = store.load_all().unwrap();
        assert!(tombs.is_empty());
    }

    #[tokio::test]
    async fn export_page_ranges_delta_optimized() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();

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
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();
        let k = key(1);

        // First a higher remote ts arrives
        store.apply_tombstone(&k, 100).await.unwrap();
        // Then a stale lower ts arrives
        store.apply_tombstone(&k, 10).await.unwrap();

        // Disk must hold 100, and MST leaf must also be 100 (not 10)
        let (_, tombs) = store.load_all().unwrap();
        assert_eq!(tombs[0].1, 100);
        let root_before = store.root_hex().await;
        store.apply_tombstone(&k, 10).await.unwrap();
        let root_after = store.root_hex().await;
        assert_eq!(root_before, root_after);
    }

    #[tokio::test]
    async fn export_overlap_ranges_are_deduped() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 1u8).unwrap();

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
        let db_src = temp_db();
        let db_dst = temp_db();
        let src: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db_src, 1u8).unwrap();
        let dst: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db_dst, 2u8).unwrap();

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
        let sess = dst.begin_delta_apply();
        sess.apply_chunk(regs, tombs).unwrap();
        sess.commit().await.unwrap();

        // Roots match after commit
        assert_eq!(src.root_hex().await, dst.root_hex().await);
    }

    #[tokio::test]
    async fn apply_delta_chunk_update_mst_keeps_tree_fresh() {
        let db = temp_db();
        let src: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db.clone(), 1u8).unwrap();
        let dst: CrdtMstStore<Adapter, XXHash128, TestTables> =
            CrdtMstStore::open(db, 2u8).unwrap();

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
}
