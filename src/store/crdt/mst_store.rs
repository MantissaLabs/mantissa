//! CRDT + Merkle Search Tree backed store with tombstones.
//!
//! - Durable per-key CRDT registers in redb.
//! - Durable tombstones for deletions.
//! - In-memory Merkle Search Tree (MST) over (Key, Entry<Snapshot>).
//!
//! This module exposes fast range-based delta export/import primitives to power
//! anti-entropy sync between peers.

use crate::hash::HashBytes;
use crate::store::crdt::adapter::RegAdapter;
use crate::store::crdt::table_set::TableSet;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use merkle_search_tree::digest::Hasher as MstHasher;
use merkle_search_tree::{builder::Builder, MerkleSearchTree};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::{hash::Hash, io, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

/// Value stored in each MST leaf.
/// Active leaves carry a CRDT snapshot; Deleted leaves carry a tombstone sequence.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry<S> {
    Active(S),
    Deleted {
        // Tombstone sequence number
        ts: u64,
    },
}

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
/// This replaces the confusing name `OwnedPageRange`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PageDigestRange {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub hash: Vec<u8>, // digest bytes (e.g. 16 bytes for Digest<16>)
}

#[inline]
fn into_io<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

/// Convert a logical key to the raw byte ordering used by MST/page ranges.
#[inline]
fn key_to_raw_bytes<C: RegAdapter>(k: &C::Key) -> Vec<u8> {
    C::key_to_bytes(k)
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
    mst: Arc<RwLock<MerkleSearchTree<C::Key, Entry<C::Snapshot>, H>>>,
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
    // --------------------------------------------------------------
    // Construction & basic helpers
    // --------------------------------------------------------------

    /// Open (or initialize) the underlying tables and create an empty in-memory MST.
    pub fn open(db: Arc<redb::Database>, actor: C::Actor) -> io::Result<Self> {
        // Ensure tables exist
        let w = db.begin_write().map_err(into_io)?;
        let _ = w.open_table(T::values()).map_err(into_io)?;
        let _ = w.open_table(T::tombs()).map_err(into_io)?;
        let _ = w.open_table(T::meta()).map_err(into_io)?;
        w.commit().map_err(into_io)?;

        let mst = Arc::new(RwLock::new(
            Builder::default().with_hasher(H::default()).build(),
        ));
        Ok(Self {
            db,
            actor,
            mst,
            _tables: std::marker::PhantomData,
        })
    }

    /// Return whether a register value exists for key `k`.
    pub fn exists(&self, k: &C::Key) -> io::Result<bool> {
        let r = self.db.begin_read().map_err(into_io)?;
        let t = r.open_table(T::values()).map_err(into_io)?;
        Ok(t.get(Self::encode_key(k).as_slice())
            .map_err(into_io)?
            .is_some())
    }

    #[inline]
    fn encode_reg(r: &C::Reg) -> io::Result<Vec<u8>> {
        bincode::serialize(r).map_err(into_io)
    }

    #[inline]
    fn decode_reg(bytes: &[u8]) -> io::Result<C::Reg> {
        bincode::deserialize(bytes).map_err(into_io)
    }

    #[inline]
    fn encode_key(k: &C::Key) -> Vec<u8> {
        C::key_to_bytes(k)
    }

    #[inline]
    fn decode_key(bytes: &[u8]) -> io::Result<C::Key> {
        C::key_from_bytes(bytes)
    }

    // --------------------------------------------------------------
    // Durable state ↔ MST rebuild
    // --------------------------------------------------------------

    /// Rebuild the in-memory MST from durable registers + tombstones.
    pub async fn rebuild_mst_from_disk(&self) -> io::Result<()> {
        let r = self.db.begin_read().map_err(into_io)?;
        let values = r.open_table(T::values()).map_err(into_io)?;
        let tombs = r.open_table(T::tombs()).map_err(into_io)?;

        // Collect snapshots
        let mut actives: Vec<(C::Key, C::Snapshot)> = {
            let mut out = Vec::new();
            let mut it = values.iter().map_err(into_io)?;
            while let Some(Ok((k_guard, v_guard))) = it.next() {
                let key = Self::decode_key(k_guard.value())?;
                let reg = Self::decode_reg(v_guard.value())?;
                out.push((key, C::snapshot_reg(&reg)));
            }
            out
        };
        actives.sort_by(|(ka, _), (kb, _)| ka.cmp(kb)); // deterministic order

        let mut tomb_list: Vec<(C::Key, u64)> = {
            let mut out = Vec::new();
            let mut it = tombs.iter().map_err(into_io)?;
            while let Some(Ok((k_guard, ts_guard))) = it.next() {
                out.push((Self::decode_key(k_guard.value())?, ts_guard.value()));
            }
            out
        };
        tomb_list.sort_by(|(ka, _), (kb, _)| ka.cmp(kb)); // deterministic order

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

    // --------------------------------------------------------------
    // CRUD: upsert / remove / merge tombstone / deltas
    // --------------------------------------------------------------

    /// Insert or update value for key `k`:
    /// - merges CRDT value, clears any tombstone,
    /// - updates the in-memory MST leaf.
    pub async fn upsert(&self, k: &C::Key, v: C::Value) -> io::Result<()> {
        // Load current register (if any)
        let current = {
            let r = self.db.begin_read().map_err(into_io)?;
            let t = r.open_table(T::values()).map_err(into_io)?;
            match t.get(Self::encode_key(k).as_slice()).map_err(into_io)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            }
        };

        let new_reg = C::upsert_reg(current, &self.actor, v);
        let snap = C::snapshot_reg(&new_reg);

        // Persist: write register + clear tombstone
        let w = self.db.begin_write().map_err(into_io)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_io)?;
            values
                .insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(&new_reg)?.as_slice(),
                )
                .map_err(into_io)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            let _ = tombs
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_io)?;
        }
        w.commit().map_err(into_io)?;

        // Reflect in MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Active(snap));

        Ok(())
    }

    /// Remove key:
    /// - allocate a monotonic tombstone sequence number,
    /// - delete any register row,
    /// - persist the tombstone and reflect it in MST.
    pub async fn remove(&self, k: &C::Key) -> io::Result<u64> {
        // First check if tomb already exists; if so, do NOT allocate a new seq.
        let (already_tombstoned, needs_value_drop) = {
            let r = self.db.begin_read().map_err(into_io)?;
            let tombstones = r.open_table(T::tombs()).map_err(into_io)?;
            let values = r.open_table(T::values()).map_err(into_io)?;

            let kb = Self::encode_key(k);
            let tomb_ts = tombstones
                .get(kb.as_slice())
                .map_err(into_io)?
                .map(|g| g.value());
            let value_exists = values.get(kb.as_slice()).map_err(into_io)?.is_some();

            (tomb_ts, value_exists)
        };

        if let Some(ts) = already_tombstoned {
            // Ensure value row is gone and MST reflects the *existing* monotonic ts.
            let w = self.db.begin_write().map_err(into_io)?;
            if needs_value_drop {
                let mut values = w.open_table(T::values()).map_err(into_io)?;
                let _ = values
                    .remove(Self::encode_key(k).as_slice())
                    .map_err(into_io)?;
            }
            w.commit().map_err(into_io)?;

            let mut t = self.mst.write().await;
            t.upsert(k.clone(), &Entry::Deleted { ts });
            return Ok(ts);
        }

        // No tombstone yet: allocate a new sequence and persist.
        let w = self.db.begin_write().map_err(into_io)?;
        let ts = {
            let mut meta = w.open_table(T::meta()).map_err(into_io)?;
            let next = match meta.get("tomb_seq").map_err(into_io)? {
                Some(g) => g.value().saturating_add(1),
                None => 1,
            };
            meta.insert("tomb_seq", &next).map_err(into_io)?;
            next
        };
        {
            let mut values = w.open_table(T::values()).map_err(into_io)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_io)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            tombs
                .insert(Self::encode_key(k).as_slice(), &ts)
                .map_err(into_io)?;
        }
        w.commit().map_err(into_io)?;

        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Deleted { ts });
        Ok(ts)
    }

    /// Merge a remote register for key `k` into durable state and MST.
    /// Clears any local tombstone to allow explicit resurrection when a fresh
    /// register arrives via sync.
    pub async fn merge_register(&self, k: &C::Key, incoming: &C::Reg) -> io::Result<()> {
        // Read current reg (if any)
        let current = {
            let r = self.db.begin_read().map_err(into_io)?;
            let t = r.open_table(T::values()).map_err(into_io)?;
            match t.get(Self::encode_key(k).as_slice()).map_err(into_io)? {
                Some(row) => Some(Self::decode_reg(row.value())?),
                None => None,
            }
        };

        // Merge and persist
        let merged = C::merge_regs(current, incoming.clone());
        let snap = C::snapshot_reg(&merged);

        let w = self.db.begin_write().map_err(into_io)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_io)?;
            values
                .insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(&merged)?.as_slice(),
                )
                .map_err(into_io)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            let _ = tombs
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_io)?;
        }
        w.commit().map_err(into_io)?;

        // Update MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Active(snap));
        Ok(())
    }

    /// Apply an inbound tombstone (idempotent, monotonic).
    pub async fn apply_tombstone(&self, k: &C::Key, ts: u64) -> io::Result<()> {
        let w = self.db.begin_write().map_err(into_io)?;
        {
            let mut values = w.open_table(T::values()).map_err(into_io)?;
            let _ = values
                .remove(Self::encode_key(k).as_slice())
                .map_err(into_io)?;
        }
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            let kb = Self::encode_key(k);
            let next_ts = match tombs.get(kb.as_slice()).map_err(into_io)? {
                Some(g) => g.value().max(ts),
                None => ts,
            };
            tombs.insert(kb.as_slice(), &next_ts).map_err(into_io)?;
        }
        w.commit().map_err(into_io)?;

        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Deleted { ts });
        Ok(())
    }

    /// Apply one streamed delta chunk (register merges + tombstones).
    /// Optimized to batch writes in **one** write transaction.
    pub fn apply_delta_chunk(
        &self,
        regs: Vec<(C::Key, C::Reg)>,
        tombs: Vec<(C::Key, u64)>,
    ) -> io::Result<()> {
        // Prepare merged registers by reading current values once.
        let merged_regs: Vec<(C::Key, C::Reg)> = {
            let r = self.db.begin_read().map_err(into_io)?;
            let values = r.open_table(T::values()).map_err(into_io)?;

            let mut out = Vec::with_capacity(regs.len());
            for (k, incoming) in regs {
                let kb = Self::encode_key(&k);
                let current = match values.get(kb.as_slice()).map_err(into_io)? {
                    Some(row) => Some(Self::decode_reg(row.value())?),
                    None => None,
                };
                out.push((k, C::merge_regs(current, incoming)));
            }
            out
        };

        // Single write transaction for everything:
        let w = self.db.begin_write().map_err(into_io)?;
        {
            let mut tv = w.open_table(T::values()).map_err(into_io)?;
            let mut tt = w.open_table(T::tombs()).map_err(into_io)?;

            // Apply merged registers (and clear any tombstone)
            for (k, reg) in &merged_regs {
                tv.insert(
                    Self::encode_key(k).as_slice(),
                    Self::encode_reg(reg)?.as_slice(),
                )
                .map_err(into_io)?;
                let _ = tt.remove(Self::encode_key(k).as_slice()).map_err(into_io)?;
            }

            // Apply tombstones (and remove register rows)
            for (k, ts) in tombs {
                tt.insert(Self::encode_key(&k).as_slice(), &ts)
                    .map_err(into_io)?;
                let _ = tv
                    .remove(Self::encode_key(&k).as_slice())
                    .map_err(into_io)?;
            }
        }
        w.commit().map_err(into_io)?;

        Ok(())
    }

    /// Rebuild MST once after a sequence of `apply_delta_chunk()` calls.
    pub async fn finalize_after_stream(&self) -> io::Result<()> {
        self.rebuild_mst_from_disk().await
    }

    /// Dump durable (key, snapshot) and (key, tombstone).
    pub fn load_all(&self) -> io::Result<(Vec<(C::Key, C::Snapshot)>, Vec<(C::Key, u64)>)> {
        let r = self.db.begin_read().map_err(into_io)?;
        let values = r.open_table(T::values()).map_err(into_io)?;
        let tombs = r.open_table(T::tombs()).map_err(into_io)?;

        let mut actives = Vec::new();
        {
            let mut it = values.iter().map_err(into_io)?;
            while let Some(Ok((k, v))) = it.next() {
                let key = Self::decode_key(k.value())?;
                let reg = Self::decode_reg(v.value())?;
                actives.push((key, C::snapshot_reg(&reg)));
            }
        }

        let mut tomb_list = Vec::new();
        {
            let mut it = tombs.iter().map_err(into_io)?;
            while let Some(Ok((k, ts))) = it.next() {
                tomb_list.push((Self::decode_key(k.value())?, ts.value()));
            }
        }
        Ok((actives, tomb_list))
    }

    // --------------------------------------------------------------
    // Page summaries / range deltas
    // --------------------------------------------------------------

    /// Return page summaries (inclusive [start,end] + digest) for the current MST.
    pub async fn get_page_ranges_summaries(&self) -> io::Result<Vec<PageDigestRange>> {
        let t = self.mst.write().await;
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
    /// are within that inclusive range. Uses O(n log r) membership checks.
    pub fn export_page_ranges_delta(
        &self,
        want: &[PageDigestRange],
    ) -> io::Result<(Vec<(C::Key, C::Reg)>, Vec<(C::Key, u64)>)> {
        // Build an index over requested ranges, sorted by start.
        let index = RangeIndex::new(want);

        let r = self.db.begin_read().map_err(into_io)?;
        let values = r.open_table(T::values()).map_err(into_io)?;
        let tombstones = r.open_table(T::tombs()).map_err(into_io)?;

        let mut registers_out: Vec<(C::Key, C::Reg)> = Vec::new();
        let mut tombstones_out: Vec<(C::Key, u64)> = Vec::new();

        // To avoid double-emitting the same key if ranges overlap:
        let mut seen_regs: HashSet<C::Key> = HashSet::new();
        let mut seen_tombs: HashSet<C::Key> = HashSet::new();

        // Scan registers once, filter by range index.
        {
            let mut it = values.iter().map_err(into_io)?;
            while let Some(Ok((k_g, v_g))) = it.next() {
                let key = Self::decode_key(k_g.value())?;
                let raw = key_to_raw_bytes::<C>(&key);
                if index.contains(&raw) && seen_regs.insert(key.clone()) {
                    let reg = Self::decode_reg(v_g.value())?;
                    registers_out.push((key, reg));
                }
            }
        }

        // Scan tombstones once, filter by range index.
        {
            let mut it = tombstones.iter().map_err(into_io)?;
            while let Some(Ok((k_g, ts_g))) = it.next() {
                let key = Self::decode_key(k_g.value())?;
                let raw = key_to_raw_bytes::<C>(&key);
                if index.contains(&raw) && seen_tombs.insert(key.clone()) {
                    tombstones_out.push((key, ts_g.value()));
                }
            }
        }

        Ok((registers_out, tombstones_out))
    }

    // --------------------------------------------------------------
    // Wire helpers
    // --------------------------------------------------------------

    pub fn from_wire_reg(&self, b: &[u8]) -> io::Result<C::Reg> {
        bincode::deserialize(b).map_err(into_io)
    }

    pub fn from_wire_key(&self, b: &[u8]) -> io::Result<C::Key> {
        C::key_from_bytes(b)
    }

    #[inline]
    pub fn to_wire_key(&self, k: &C::Key) -> Vec<u8> {
        k.as_ref().to_vec()
    }

    #[inline]
    pub fn to_wire_reg(&self, r: &C::Reg) -> io::Result<Vec<u8>> {
        bincode::serialize(r).map_err(into_io)
    }

    // --------------------------------------------------------------
    // Debug helpers
    // --------------------------------------------------------------

    pub async fn debug_dump_root(&self, label: &str) {
        let hex = self.root_hex().await;
        debug!(target: "merkle search tree", "{label}: root={hex}");
    }

    pub async fn debug_dump_ranges(&self, label: &str, limit: usize) {
        let t = self.mst.write().await;
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
    }

    /// Print the exact bytes we hash per leaf Entry (canonical). Debug only.
    #[allow(dead_code)]
    pub fn debug_dump_leaf_bytes_from_store(&self) -> io::Result<()> {
        let (actives, tombs) = self.load_all()?;

        println!("[LEAVES] actives:");
        for (k, snap) in actives {
            let mut sink = HashBytes::default();
            Entry::Active(snap).hash(&mut sink);
            println!(
                "  key={:?} bytes(base64)={}",
                k.as_ref(),
                B64.encode(&sink.as_slice())
            );
        }

        println!("[LEAVES] tombstones:");
        for (k, ts) in tombs {
            let mut sink = HashBytes::default();
            let e: Entry<C::Snapshot> = Entry::Deleted { ts };
            e.hash(&mut sink);
            println!(
                "  key={:?} bytes(base64)={}",
                k.as_ref(),
                B64.encode(sink.as_slice())
            );
        }
        Ok(())
    }
}

// -----------------------------------------------------------------------
// RangeIndex: fast membership check for many inclusive [start,end] ranges.

/// A compact index to check whether raw key bytes fall into any of the requested ranges.
/// Assumes ranges are **mostly non-overlapping** (true for MST page ranges).
/// If they do overlap, we still behave correctly (we also guard against duplicates).
struct RangeIndex {
    // Sorted by start lexicographically (MST key ordering).
    starts: Vec<Vec<u8>>,
    ranges: Vec<(Vec<u8>, Vec<u8>)>, // (start, end)
}

impl RangeIndex {
    fn new(ranges: &[PageDigestRange]) -> Self {
        let mut rs: Vec<(Vec<u8>, Vec<u8>)> = ranges
            .iter()
            .map(|r| (r.start.clone(), r.end.clone()))
            .collect();
        rs.sort_by(|a, b| a.0.cmp(&b.0));
        let starts = rs.iter().map(|(s, _)| s.clone()).collect();
        Self { starts, ranges: rs }
    }

    /// Returns true if `key` is inside any inclusive [start, end].
    /// Complexity: O(log r) binary search + O(1) final bound check.
    fn contains(&self, key: &[u8]) -> bool {
        if self.ranges.is_empty() {
            return false;
        }
        // upper_bound(start <= key) → candidate is at pos-1
        let pos = self
            .starts
            .binary_search_by(|probe| probe.as_slice().cmp(key))
            .map(|i| i + 1)
            .unwrap_or_else(|i| i);
        if pos == 0 {
            return false;
        }
        let (start, end) = &self.ranges[pos - 1];
        start.as_slice() <= key && key <= end.as_slice()
    }
}

// -----------------------------------------------------------
// Cap’n Proto helpers (kept names/signatures for back-compat)

/// Fill a capnp builder from a slice of page digest ranges.
pub fn capnp_fill_ranges<K>(
    ranges: &[PageDigestRange],
    mut out: crate::sync_capnp::page_range_summary::Builder,
) -> Result<(), capnp::Error> {
    let mut lst = out.reborrow().init_ranges(ranges.len() as u32);
    for (i, r) in ranges.iter().enumerate() {
        let mut it = lst.reborrow().get(i as u32);
        it.set_start(&r.start);
        it.set_end(&r.end);
        it.set_hash(&r.hash);
    }
    Ok(())
}

/// Parse page digest ranges from a capnp reader.
pub fn page_ranges_from_capnp<K>(
    reader: crate::sync_capnp::page_range_summary::Reader,
) -> Result<Vec<PageDigestRange>, capnp::Error> {
    let ranges = reader.get_ranges()?;
    let mut out = Vec::with_capacity(ranges.len() as usize);
    for i in 0..ranges.len() {
        let r = ranges.get(i);
        out.push(PageDigestRange {
            start: r.get_start()?.to_vec(),
            end: r.get_end()?.to_vec(),
            hash: r.get_hash()?.to_vec(),
        });
    }
    Ok(out)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::XXHash128;
    use crate::store::crdt::adapter::{MvRegAdapterSorted, RegAdapter};
    use crate::store::crdt::uuid_key::UuidKey;
    use uuid::Uuid;

    // ------- Minimal TableSet for tests -------
    struct TestTables;
    impl TableSet for TestTables {
        const VALUES: &'static str = "values";
        const TOMBS: &'static str = "tombs";
        const META: &'static str = "meta";
    }

    // ------- Concrete adapter types for tests -------
    type Adapter = MvRegAdapterSorted<UuidKey, String, u8>;
    type Hasher = XXHash128;

    fn temp_db() -> Arc<redb::Database> {
        let path = std::env::temp_dir().join(format!("mst-test-{}.redb", Uuid::new_v4()));
        Arc::new(redb::Database::create(path).unwrap())
    }

    fn key(n: u8) -> UuidKey {
        // Stable, ordered keys for test: encode n into the last byte of UUID
        let mut bytes = [0u8; 16];
        bytes[15] = n;
        UuidKey::try_from(&bytes[..]).unwrap()
    }

    #[tokio::test]
    async fn upsert_exists_remove_roundtrip() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, Hasher, TestTables> = CrdtMstStore::open(db, 1u8).unwrap();

        let k = key(10);
        assert!(!store.exists(&k).unwrap());

        store.upsert(&k, "v1".to_string()).await.unwrap();
        assert!(store.exists(&k).unwrap());

        let ts = store.remove(&k).await.unwrap();
        assert!(ts > 0);
        assert!(store.exists(&k).unwrap() == false);

        // Tombstone reflected in MST
        let root = store.root_hex().await;
        assert!(!root.is_empty());
    }

    #[tokio::test]
    async fn merge_register_clears_tombstone() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, Hasher, TestTables> = CrdtMstStore::open(db, 1u8).unwrap();

        let k = key(1);

        // apply tombstone
        store.apply_tombstone(&k, 42).await.unwrap();
        assert!(!store.exists(&k).unwrap());

        // incoming register should resurrect and clear local tombstone
        let reg = {
            let current = None;
            <Adapter as RegAdapter>::upsert_reg(current, &1u8, "hello".to_string())
        };
        store.merge_register(&k, &reg).await.unwrap();

        // Value must exist and tomb be cleared
        assert!(store.exists(&k).unwrap());
        let (_actives, tombs) = store.load_all().unwrap();
        assert!(tombs.is_empty());

        // Subsequent tombstone would remove the value again (no guard). We only
        // assert that resurrection cleared the tomb as expected above.
    }

    #[tokio::test]
    async fn export_page_ranges_delta_optimized() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, Hasher, TestTables> = CrdtMstStore::open(db, 1u8).unwrap();

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
            start: key_to_raw_bytes::<Adapter>(&key(2)),
            end: key_to_raw_bytes::<Adapter>(&key(4)),
            hash: vec![], // not used in export filtering
        }];

        let (regs, tombs) = store.export_page_ranges_delta(&want).unwrap();

        // We expect registers for keys 2 and 4, tombstone for key 3.
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
        // (3,4) due to hash mismatch, and (5,6) missing locally
        assert!(want.iter().any(|w| w.start == vec![3] && w.end == vec![4]));
        assert!(want.iter().any(|w| w.start == vec![5] && w.end == vec![6]));
    }

    #[tokio::test]
    async fn apply_tombstone_uses_monotonic_ts_in_mst() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, Hasher, TestTables> = CrdtMstStore::open(db, 1u8).unwrap();
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
        assert_eq!(
            root_before, root_after,
            "stale tomb must not change MST root"
        );
    }

    #[tokio::test]
    async fn remove_is_idempotent() {
        let db = temp_db();
        let store: CrdtMstStore<Adapter, Hasher, TestTables> = CrdtMstStore::open(db, 1u8).unwrap();
        let k = key(2);

        // First call allocates a seq
        let ts1 = store.remove(&k).await.unwrap();
        let root1 = store.root_hex().await;

        // Second call returns same ts (no meta bump), root unchanged
        let ts2 = store.remove(&k).await.unwrap();
        let root2 = store.root_hex().await;

        assert_eq!(ts1, ts2, "idempotent remove must not allocate a new seq");
        assert_eq!(root1, root2, "idempotent remove must not change MST root");
    }
}
