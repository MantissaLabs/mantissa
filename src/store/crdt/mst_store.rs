use crate::hash::HashBytes;
use crate::store::crdt::adapter::RegAdapter;
use crate::store::crdt::table_set::TableSet;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use bincode;
use merkle_search_tree::digest::Hasher as MstHasher;
use merkle_search_tree::{builder::Builder, MerkleSearchTree};
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::{hash::Hash, io, sync::Arc};
use tokio::sync::RwLock;

/// Leaf value for MST.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Entry<S> {
    Active(S),
    Deleted { ts: u64 },
}

// Canonical hashing: tag byte + payload in a fixed-endian encoding
impl<S> Hash for Entry<S>
where
    S: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Entry::Active(s) => {
                state.write_u8(0);
                s.hash(state); // uses the canonical impl above for MvRegSnapshot<T>
            }
            Entry::Deleted { ts } => {
                state.write_u8(1);
                state.write_u64(*ts);
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OwnedPageRange {
    start: Vec<u8>,
    end: Vec<u8>,
    hash: Vec<u8>, // 16 bytes for Digest<16>, but Vec<u8> keeps it generic
}

#[inline]
fn into_io<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

// convert dec_key(K) → raw key bytes (the same shape MST uses in ranges)
fn key_raw_bytes<C: RegAdapter>(k: &C::Key) -> Vec<u8> {
    C::key_to_bytes(k) // your adapter’s K → &[u8] → Vec<u8>
}

/// Generic CRDT + MST store.
/// - Durable per-key CRDT registers in redb.
/// - Durable tombstones (for removes).
/// - In-memory MST over (Key, Entry<Snapshot>).
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
    db: std::sync::Arc<redb::Database>,
    actor: C::Actor,
    mst: Arc<RwLock<MerkleSearchTree<C::Key, Entry<C::Snapshot>, H>>>,
    _tables: PhantomData<T>,
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
    pub fn open(db: std::sync::Arc<redb::Database>, actor: C::Actor) -> std::io::Result<Self> {
        // Use &*db to call redb APIs as before
        let w = db.begin_write().map_err(into_io)?;
        let _ = w.open_table(T::values()).map_err(into_io)?;
        let _ = w.open_table(T::tombs()).map_err(into_io)?;
        let _ = w.open_table(T::meta()).map_err(into_io)?;
        w.commit().map_err(into_io)?;

        let mst = std::sync::Arc::new(tokio::sync::RwLock::new(
            merkle_search_tree::builder::Builder::default()
                .with_hasher(H::default())
                .build(),
        ));

        Ok(Self {
            db,
            actor,
            mst,
            _tables: std::marker::PhantomData,
        })
    }

    pub fn exists(&self, k: &C::Key) -> std::io::Result<bool> {
        let r = self.db.begin_read().map_err(into_io)?;
        let t = r.open_table(T::values()).map_err(into_io)?;
        let kb = Self::enc_key(k);
        Ok(t.get(kb.as_slice()).map_err(into_io)?.is_some())
    }

    #[inline]
    fn enc_reg(r: &C::Reg) -> io::Result<Vec<u8>> {
        bincode::serialize(r).map_err(into_io)
    }

    #[inline]
    fn dec_reg(bytes: &[u8]) -> io::Result<C::Reg> {
        bincode::deserialize(bytes).map_err(into_io)
    }

    /// Rebuild the in-memory MST from durable registers + tombstones.
    pub async fn rebuild_mst_from_disk(&self) -> io::Result<()> {
        let r = self.db.begin_read().map_err(into_io)?;
        let values = r.open_table(T::values()).map_err(into_io)?;
        let tombs = r.open_table(T::tombs()).map_err(into_io)?;

        let mut actives: Vec<(C::Key, C::Snapshot)> = Vec::new();
        {
            let mut it = values.iter().map_err(into_io)?;
            while let Some(Ok((k_guard, v_guard))) = it.next() {
                let k = Self::dec_key(k_guard.value())?;
                let reg = Self::dec_reg(v_guard.value())?;
                let snap = C::snapshot_reg(&reg);
                actives.push((k, snap));
            }
        }

        // sort by key to lock the insertion order
        actives.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));

        let mut tomb_list: Vec<(C::Key, u64)> = Vec::new();
        {
            let mut it = tombs.iter().map_err(into_io)?;
            while let Some(Ok((k_guard, ts_guard))) = it.next() {
                let k = Self::dec_key(k_guard.value())?;
                let ts = ts_guard.value();
                tomb_list.push((k, ts));
            }
        }

        // also sort tombstones by key
        tomb_list.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));

        let builder = Builder::default().with_hasher(H::default());
        let mut tree = builder.build();
        for (k, s) in actives {
            tree.upsert(k, &Entry::Active(s));
        }
        for (k, ts) in tomb_list {
            tree.upsert(k, &Entry::Deleted { ts });
        }

        *self.mst.write().await = tree;
        Ok(())
    }

    /// Insert/update a value for key `k`. Writes register + clears any tombstone, then updates MST.
    pub async fn upsert(&self, k: &C::Key, v: C::Value) -> io::Result<()> {
        // Read current register
        let current: Option<C::Reg> = {
            let r = self.db.begin_read().map_err(into_io)?;
            let t = r.open_table(T::values()).map_err(into_io)?;
            let kb = Self::enc_key(k);
            if let Some(row) = t.get(kb.as_slice()).map_err(into_io)? {
                Some(Self::dec_reg(row.value())?)
            } else {
                None
            }
        };

        // Compute new register + snapshot
        let new_reg = C::upsert_reg(current, &self.actor, v);
        let snap = C::snapshot_reg(&new_reg);

        // Write register, clear tombstone
        {
            let w = self.db.begin_write().map_err(into_io)?;
            {
                let mut values = w.open_table(T::values()).map_err(into_io)?;
                let kb = Self::enc_key(k);
                let rb = Self::enc_reg(&new_reg)?;
                values
                    .insert(kb.as_slice(), rb.as_slice())
                    .map_err(into_io)?;
            }
            {
                let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
                let kb = Self::enc_key(k);
                let _ = tombs.remove(kb.as_slice()).map_err(into_io)?;
            }
            w.commit().map_err(into_io)?;
        }

        // Update MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Active(snap));
        Ok(())
    }

    /// Remove key (write tombstone + delete value), returns tombstone seq.
    pub async fn remove(&self, k: &C::Key) -> io::Result<u64> {
        let w = self.db.begin_write().map_err(into_io)?;

        let ts = {
            let mut meta = w.open_table(T::meta()).map_err(into_io)?;
            let next = match meta.get("tomb_seq").map_err(into_io)? {
                Some(g) => g.value().saturating_add(1), // value(): u64 (Copy) — no '*'
                None => 1,
            };
            meta.insert("tomb_seq", &next).map_err(into_io)?;
            next
        };

        // delete the register row.
        {
            let mut values = w.open_table(T::values()).map_err(into_io)?;
            let kb = Self::enc_key(k);
            let _ = values.remove(kb.as_slice()).map_err(into_io)?;
        }

        // write the tombstone.
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            let kb = Self::enc_key(k);
            tombs.insert(kb.as_slice(), &ts).map_err(into_io)?;
        }

        w.commit().map_err(into_io)?;

        // update MST in-memory
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Deleted { ts });

        Ok(ts)
    }

    /// Dump durable (key, snapshot) and (key, tombstone) — useful for anti-entropy or external rebuilds.
    pub fn load_all(&self) -> io::Result<(Vec<(C::Key, C::Snapshot)>, Vec<(C::Key, u64)>)> {
        let r = self.db.begin_read().map_err(into_io)?;
        let values = r.open_table(T::values()).map_err(into_io)?;
        let tombs = r.open_table(T::tombs()).map_err(into_io)?;

        let mut actives = Vec::new();
        {
            let mut it = values.iter().map_err(into_io)?;
            while let Some(Ok((k, v))) = it.next() {
                let key = Self::dec_key(k.value())?;
                let reg = Self::dec_reg(v.value())?;
                let snap = C::snapshot_reg(&reg);
                actives.push((key, snap));
            }
        }
        let mut tomb_list = Vec::new();
        {
            let mut it = tombs.iter().map_err(into_io)?;
            while let Some(Ok((k, ts))) = it.next() {
                tomb_list.push((Self::dec_key(k.value())?, ts.value()));
            }
        }
        Ok((actives, tomb_list))
    }

    /// Replace the in-memory MST (e.g., after applying remote diffs).
    /// This is usually done incrementally, but this method exists if
    /// we want to rebuild the entire MST for other reasons.
    pub async fn rebuild_mst<Ia, It>(&self, actives: Ia, tombs: It)
    where
        Ia: IntoIterator<Item = (C::Key, C::Snapshot)>,
        It: IntoIterator<Item = (C::Key, u64)>,
    {
        let builder = Builder::default().with_hasher(H::default());
        let mut t = builder.build();
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
        let mut t = self.mst.write().await; // root_hash needs &mut internally
        t.root_hash().to_string()
    }

    /// Receiver: apply one chunk (merge + persist), *no* MST rebuild here.
    /// Call `finalize_after_stream()` once the stream has finished.
    pub fn apply_delta_chunk(
        &self,
        regs: Vec<(C::Key, C::Reg)>,
        tombs: Vec<(C::Key, u64)>,
    ) -> io::Result<()> {
        // Merge & persist registers
        for (k, incoming) in regs {
            let current = {
                let r = self.db.begin_read().map_err(into_io)?;
                let t = r.open_table(T::values()).map_err(into_io)?;
                let kb = Self::enc_key(&k);
                if let Some(row) = t.get(kb.as_slice()).map_err(into_io)? {
                    Some(Self::dec_reg(row.value())?)
                } else {
                    None
                }
            };

            let merged = C::merge_regs(current, incoming);

            let mut w = self.db.begin_write().map_err(into_io)?;
            {
                let mut tv = w.open_table(T::values()).map_err(into_io)?;
                let kb = Self::enc_key(&k);
                let rb = Self::enc_reg(&merged)?;
                tv.insert(kb.as_slice(), rb.as_slice()).map_err(into_io)?;
            }
            {
                let mut tt = w.open_table(T::tombs()).map_err(into_io)?;
                let kb = Self::enc_key(&k);
                let _ = tt.remove(kb.as_slice()).map_err(into_io)?;
            }
            w.commit().map_err(into_io)?;
        }

        // Persist tombstones (and optionally remove value rows to save space)
        for (k, ts_val) in tombs {
            let w = self.db.begin_write().map_err(into_io)?;
            {
                let mut tt = w.open_table(T::tombs()).map_err(into_io)?;
                let kb = Self::enc_key(&k);
                tt.insert(kb.as_slice(), &ts_val).map_err(into_io)?;
            }
            {
                let mut tv = w.open_table(T::values()).map_err(into_io)?;
                let kb = Self::enc_key(&k);
                let _ = tv.remove(kb.as_slice()).map_err(into_io)?;
            }
            w.commit().map_err(into_io)?;
        }

        Ok(())
    }

    /// Rebuild the in-memory MST once after all chunks have been applied.
    pub async fn finalize_after_stream(&self) -> io::Result<()> {
        self.rebuild_mst_from_disk().await
    }

    pub async fn mst_ranges_owned(&self) -> std::io::Result<Vec<OwnedPageRange>> {
        let t = self.mst.write().await;

        // Option<Vec<PageRange<'_, K>>> → Vec<...>
        let prs = t.serialise_page_ranges().unwrap_or_default();

        let out: Vec<OwnedPageRange> = prs
            .into_iter()
            .map(|pr| OwnedPageRange {
                start: C::key_to_bytes(pr.start()),
                end: C::key_to_bytes(pr.end()),
                // PageDigest → &[u8] → Vec<u8>
                hash: pr.hash().as_ref().to_vec(),
            })
            .collect();

        Ok(out)
    }

    /// Export exact delta for requested owned ranges:
    /// For each (start,end) page range, include all values/tombstones whose *raw-key bytes*
    /// are within [start, end] inclusive.
    pub fn export_delta_for_owned(
        &self,
        want: &[OwnedPageRange],
    ) -> io::Result<(Vec<(C::Key, C::Reg)>, Vec<(C::Key, u64)>)> {
        let r = self.db.begin_read().map_err(into_io)?;
        let t_vals = r.open_table(T::values()).map_err(into_io)?;
        let t_tmbs = r.open_table(T::tombs()).map_err(into_io)?;

        // Gather (K,Reg) and (K,ts) for all requested ranges
        let mut regs_out: Vec<(C::Key, C::Reg)> = Vec::new();
        let mut tmbs_out: Vec<(C::Key, u64)> = Vec::new();

        // Pre-load everything once (O(n)); fine for now, we can optimize later
        let mut all_vals: Vec<(C::Key, C::Reg, Vec<u8>)> = Vec::new();
        {
            let mut it = t_vals.iter().map_err(into_io)?;
            while let Some(Ok((k_g, v_g))) = it.next() {
                let k = Self::dec_key(k_g.value())?;
                let raw = key_raw_bytes::<C>(&k);
                let reg = Self::dec_reg(v_g.value())?;
                all_vals.push((k, reg, raw));
            }
        }
        let mut all_tmbs: Vec<(C::Key, u64, Vec<u8>)> = Vec::new();
        {
            let mut it = t_tmbs.iter().map_err(into_io)?;
            while let Some(Ok((k_g, ts_g))) = it.next() {
                let k = Self::dec_key(k_g.value())?;
                let raw = key_raw_bytes::<C>(&k);
                let ts = ts_g.value();
                all_tmbs.push((k, ts, raw));
            }
        }

        // For each wanted owned range, pick matching keys
        for wr in want {
            let start = wr.start.as_slice();
            let end = wr.end.as_slice();

            for (k, reg, raw) in all_vals.iter() {
                if raw.as_slice() >= start && raw.as_slice() <= end {
                    regs_out.push((k.clone(), reg.clone()));
                }
            }
            for (k, ts, raw) in all_tmbs.iter() {
                if raw.as_slice() >= start && raw.as_slice() <= end {
                    tmbs_out.push((k.clone(), *ts));
                }
            }
        }

        Ok((regs_out, tmbs_out))
    }

    /// Wire → full register.
    pub fn from_wire_reg(&self, b: &[u8]) -> io::Result<C::Reg> {
        bincode::deserialize(b).map_err(into_io)
    }

    #[inline]
    fn enc_key(k: &C::Key) -> Vec<u8> {
        C::key_to_bytes(k)
    }

    #[inline]
    fn dec_key(bytes: &[u8]) -> io::Result<C::Key> {
        C::key_from_bytes(bytes)
    }

    pub fn from_wire_key(&self, b: &[u8]) -> io::Result<C::Key> {
        C::key_from_bytes(b)
    }

    pub async fn debug_dump_root(&self, label: &str) {
        let hex = self.root_hex().await;
        println!("[MST] {label}: root={hex}");
    }

    pub async fn debug_dump_ranges(&self, label: &str, limit: usize) {
        let t = self.mst.write().await;
        let prs = t.serialise_page_ranges().unwrap_or_default();
        println!("[MST] {label}: {} ranges", prs.len());
        for (i, pr) in prs.iter().take(limit).enumerate() {
            // public accessors vary by version; adapt if needed
            let s = C::key_to_bytes(pr.start());
            let e = C::key_to_bytes(pr.end());
            let h = pr.hash().as_ref();
            println!(
                "  [{:03}] start={:02X?} end={:02X?} hash={:02X?}",
                i,
                &s[..std::cmp::min(6, s.len())],
                &e[..std::cmp::min(6, e.len())],
                &h[..std::cmp::min(6, h.len())],
            );
        }
    }

    pub async fn merge_register(&self, k: &C::Key, incoming: &C::Reg) -> std::io::Result<()> {
        // Read current
        let current: Option<C::Reg> = {
            let r = self.db.begin_read().map_err(into_io)?;
            let t = r.open_table(T::values()).map_err(into_io)?;
            let kb = Self::enc_key(k);
            if let Some(row) = t.get(kb.as_slice()).map_err(into_io)? {
                Some(Self::dec_reg(row.value())?)
            } else {
                None
            }
        };

        // Merge (owned)
        let merged = C::merge_regs(current, incoming.clone());
        let snap = C::snapshot_reg(&merged);

        // Write back + clear tombstone
        {
            let w = self.db.begin_write().map_err(into_io)?;
            {
                let mut values = w.open_table(T::values()).map_err(into_io)?;
                let kb = Self::enc_key(k);
                let rb = Self::enc_reg(&merged)?;
                values
                    .insert(kb.as_slice(), rb.as_slice())
                    .map_err(into_io)?;
            }
            {
                let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
                let kb = Self::enc_key(k);
                let _ = tombs.remove(kb.as_slice()).map_err(into_io)?;
            }
            w.commit().map_err(into_io)?;
        }

        // Update MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Active(snap));
        Ok(())
    }

    #[inline]
    pub fn to_wire_key(&self, k: &C::Key) -> Vec<u8> {
        k.as_ref().to_vec()
    }

    #[inline]
    pub fn key_from_wire(&self, b: &[u8]) -> io::Result<C::Key>
    where
        for<'a> C::Key: TryFrom<&'a [u8]>,
        for<'a> <C::Key as TryFrom<&'a [u8]>>::Error: std::fmt::Display,
    {
        C::Key::try_from(b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    #[inline]
    pub fn to_wire_reg(&self, r: &C::Reg) -> io::Result<Vec<u8>> {
        bincode::serialize(r).map_err(into_io)
    }

    #[inline]
    pub fn reg_from_wire(&self, bytes: &[u8]) -> io::Result<C::Reg> {
        bincode::deserialize(bytes).map_err(into_io)
    }

    /// Apply an inbound tombstone (idempotent, monotonic).
    pub async fn apply_tombstone(&self, k: &C::Key, ts: u64) -> io::Result<()> {
        // write/remove in redb
        let w = self.db.begin_write().map_err(into_io)?;
        {
            // delete any register row for this key
            let mut values = w.open_table(T::values()).map_err(into_io)?;
            let kb = Self::enc_key(k);
            let _ = values.remove(kb.as_slice()).map_err(into_io)?;
        }
        {
            // upsert tombstone with max(existing, ts)
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            let kb = Self::enc_key(k);
            let next_ts = match tombs.get(kb.as_slice()).map_err(into_io)? {
                Some(g) => g.value().max(ts),
                None => ts,
            };
            tombs.insert(kb.as_slice(), &next_ts).map_err(into_io)?;
        }
        w.commit().map_err(into_io)?;

        // reflect in MST
        let mut t = self.mst.write().await;
        t.upsert(k.clone(), &Entry::Deleted { ts });
        Ok(())
    }

    /// Print the exact byte stream we hash per leaf Entry (canonical).
    pub fn debug_dump_leaf_bytes_from_store(&self) -> io::Result<()> {
        let (actives, tombs) = self.load_all()?; // (Vec<(Key, Snapshot)>, Vec<(Key, u64)>)

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

            // Tell the compiler which Entry<S> we mean:
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

    /// Print MST page ranges + hashes (what serialise_page_ranges() sees).
    pub async fn debug_dump_mst_ranges(&self) -> io::Result<()> {
        let t = self.mst.write().await;
        let Some(ranges) = t.serialise_page_ranges() else {
            println!("[MST] ranges: <empty>");
            return Ok(());
        };

        println!("[MST] ranges: {}", ranges.len());
        for (i, pr) in ranges.iter().enumerate() {
            println!(
                "  [{:03}] start={:?} end={:?} hash(base64)={}",
                i,
                pr.start(),
                pr.end(),
                B64.encode(pr.hash().as_ref())
            );
        }
        Ok(())
    }
}

// fill ranges into capnp (Summary) from owned
pub fn capnp_fill_ranges<K>(
    owned: &[OwnedPageRange],
    mut out: crate::sync_capnp::page_range_summary::Builder,
) -> Result<(), capnp::Error> {
    let mut lst = out.reborrow().init_ranges(owned.len() as u32);
    for (i, r) in owned.iter().enumerate() {
        let mut it = lst.reborrow().get(i as u32);
        it.set_start(&r.start);
        it.set_end(&r.end);
        it.set_hash(&r.hash); // Data/bytes
    }
    Ok(())
}

// parse owned ranges from capnp reader
pub fn owned_ranges_from_capnp<K>(
    reader: crate::sync_capnp::page_range_summary::Reader,
) -> Result<Vec<OwnedPageRange>, capnp::Error> {
    let ranges = reader.get_ranges()?;
    let mut out = Vec::with_capacity(ranges.len() as usize);
    for i in 0..ranges.len() {
        let r = ranges.get(i);
        out.push(OwnedPageRange {
            start: r.get_start()?.to_vec(),
            end: r.get_end()?.to_vec(),
            hash: r.get_hash()?.to_vec(),
        });
    }
    Ok(out)
}

/// remote = B’s ranges, local = A’s ranges
/// Return remote ranges that A is missing or whose hash differs.
pub fn compute_want_from_owned(
    remote: &[OwnedPageRange],
    local: &[OwnedPageRange],
) -> Vec<OwnedPageRange> {
    // (start,end) → hash
    let mut idx: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::with_capacity(local.len());
    for r in local {
        idx.insert((r.start.clone(), r.end.clone()), r.hash.clone());
    }

    let mut out = Vec::new();
    out.reserve(remote.len().min(1024));

    for r in remote {
        match idx.get(&(r.start.clone(), r.end.clone())) {
            None => out.push(r.clone()),
            Some(h) if h.as_slice() != r.hash.as_slice() => out.push(r.clone()),
            _ => {}
        }
    }
    out
}
