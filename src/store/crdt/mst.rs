use crate::store::crdt::adapter::RegAdapter;
use crate::store::crdt::table_set::TableSet;
use merkle_search_tree::digest::Hasher as MstHasher;
use merkle_search_tree::{builder::Builder, MerkleSearchTree};
use redb::{Database, ReadableTable};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::{hash::Hash, io, sync::Arc};
use tokio::sync::RwLock;

/// Leaf value for MST.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Entry<S> {
    Active(S),
    Deleted { ts: u64 },
}

#[inline]
fn into_io<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

/// Generic CRDT + MST store.
/// - Durable per-key CRDT registers in redb.
/// - Durable tombstones (for removes).
/// - In-memory MST over (Key, Entry<Snapshot>).
pub struct CrdtMstStore<C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]>,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    db: Database,
    actor: C::Actor,
    mst: Arc<RwLock<MerkleSearchTree<C::Key, Entry<C::Snapshot>, H>>>,
    _tables: PhantomData<T>,
}

impl<C, H, T> CrdtMstStore<C, H, T>
where
    C: RegAdapter,
    C::Key: AsRef<[u8]>,
    H: MstHasher<16, C::Key>
        + MstHasher<16, Entry<C::Snapshot>>
        + Default
        + Clone
        + Send
        + Sync
        + 'static,
    T: TableSet,
{
    pub fn open(db: Database, actor: C::Actor) -> io::Result<Self> {
        // Ensure (or create) the domain tables
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
            _tables: PhantomData,
        })
    }

    pub fn exists(&self, k: &C::Key) -> std::io::Result<bool> {
        let r = self.db.begin_read().map_err(into_io)?;
        let t = r.open_table(T::values()).map_err(into_io)?;
        let kb = Self::enc_key(k)?;
        Ok(t.get(kb.as_slice()).map_err(into_io)?.is_some())
    }

    #[inline]
    fn enc_key(k: &C::Key) -> io::Result<Vec<u8>> {
        bincode::serialize(k).map_err(into_io)
    }

    #[inline]
    fn dec_key(bytes: &[u8]) -> io::Result<C::Key> {
        bincode::deserialize(bytes).map_err(into_io)
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

        let mut tomb_list: Vec<(C::Key, u64)> = Vec::new();
        {
            let mut it = tombs.iter().map_err(into_io)?;
            while let Some(Ok((k_guard, ts_guard))) = it.next() {
                let k = Self::dec_key(k_guard.value())?;
                let ts = ts_guard.value();
                tomb_list.push((k, ts));
            }
        }

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
            let kb = Self::enc_key(k)?;
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
            let mut w = self.db.begin_write().map_err(into_io)?;
            {
                let mut values = w.open_table(T::values()).map_err(into_io)?;
                let kb = Self::enc_key(k)?;
                let rb = Self::enc_reg(&new_reg)?;
                values
                    .insert(kb.as_slice(), rb.as_slice())
                    .map_err(into_io)?;
            }
            {
                let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
                let kb = Self::enc_key(k)?;
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
        let mut w = self.db.begin_write().map_err(into_io)?;

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
            let kb = Self::enc_key(k)?;
            let _ = values.remove(kb.as_slice()).map_err(into_io)?;
        }

        // write the tombstone.
        {
            let mut tombs = w.open_table(T::tombs()).map_err(into_io)?;
            let kb = Self::enc_key(k)?;
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
}
