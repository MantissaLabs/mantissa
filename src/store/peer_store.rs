use crate::store::local::LocalNodeInfo;
use crate::store::Store;
use crate::topology::peers::types::{NodeId, PeerValue};
use redb::{Database, ReadableTable, TableDefinition};
use std::{fs, io, path::PathBuf, sync::Arc};
use uuid::Uuid;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peers");
const TOMBS: TableDefinition<&str, &[u8]> = TableDefinition::new("tombstones");

#[derive(Clone)]
pub struct RedbStore {
    db: Arc<Database>,
}

impl RedbStore {
    pub fn open_or_create(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let db = if path.exists() {
            Database::open(path).map_err(into_io)?
        } else {
            Database::create(path).map_err(into_io)?
        };
        {
            let w = db.begin_write().map_err(into_io)?;
            w.open_table(PEERS).map_err(into_io)?;
            w.open_table(META).map_err(into_io)?;
            w.open_table(TOMBS).map_err(into_io)?;
            w.commit().map_err(into_io)?;
        }
        Ok(Self { db: Arc::new(db) })
    }

    #[inline]
    fn serialize_value(val: &PeerValue) -> io::Result<Vec<u8>> {
        bincode::serialize(val).map_err(into_io)
    }
    #[inline]
    fn deserialize_value(bytes: &[u8]) -> io::Result<PeerValue> {
        bincode::deserialize(bytes).map_err(into_io)
    }
}

fn into_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

#[async_trait::async_trait]
impl Store for RedbStore {
    async fn load_peers(&self) -> io::Result<Vec<(NodeId, PeerValue)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtxn = db.begin_read().map_err(into_io)?;
            let table = rtxn.open_table(PEERS).map_err(into_io)?;

            let mut out = Vec::new();
            let iter = table.iter().map_err(into_io)?;
            for res in iter {
                let (k, v) = res.map_err(into_io)?;
                let id = uuid::Uuid::parse_str(k.value())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                let val = RedbStore::deserialize_value(v.value())?;
                out.push((id, val));
            }
            Ok::<_, io::Error>(out)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("join error: {e}")))?
    }

    async fn upsert_peer(&self, id: NodeId, val: &PeerValue) -> io::Result<()> {
        let db = self.db.clone();
        let key = id.to_string();
        let bytes = Self::serialize_value(val)?;
        tokio::task::spawn_blocking(move || {
            let wtxn = db.begin_write().map_err(into_io)?;

            {
                // table borrow limited to this block
                let mut table = wtxn.open_table(PEERS).map_err(into_io)?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(into_io)?;
                // table drops here
            }

            wtxn.commit().map_err(into_io)?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("join error: {e}")))?
    }

    async fn remove_peer(&self, id: NodeId) -> io::Result<()> {
        let db = self.db.clone();
        let key = id.to_string();
        tokio::task::spawn_blocking(move || {
            let wtxn = db.begin_write().map_err(into_io)?;

            {
                // table borrow limited to this block
                let mut table = wtxn.open_table(PEERS).map_err(into_io)?;
                let _ = table.remove(key.as_str()).map_err(into_io)?;
                // table drops here
            }

            wtxn.commit().map_err(into_io)?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("join error: {e}")))?
    }

    async fn load_or_create_node_id(&self) -> io::Result<Uuid> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtxn = db.begin_read().map_err(into_io)?;
            if let Ok(t) = rtxn.open_table(META) {
                if let Ok(Some(v)) = t.get("node_id") {
                    let s = v.value();
                    if s.len() == 16 {
                        let mut arr = [0u8; 16];
                        arr.copy_from_slice(s);
                        return Ok(Uuid::from_bytes(arr));
                    }
                }
            }
            drop(rtxn);

            let new_id = uuid::Uuid::now_v7();
            let mut wtxn = db.begin_write().map_err(into_io)?;
            {
                let mut t = wtxn.open_table(META).map_err(into_io)?;
                let id_slice: &[u8] = new_id.as_bytes();
                t.insert("node_id", id_slice).map_err(into_io)?;
            }
            wtxn.commit().map_err(into_io)?;
            Ok::<_, std::io::Error>(new_id)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("join error: {e}")))?
    }

    async fn load_local_node(&self) -> io::Result<Option<LocalNodeInfo>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let rtxn = db.begin_read().map_err(into_io)?;
            let t = rtxn.open_table(META).map_err(into_io)?;
            if let Some(v) = t.get("self_info").map_err(into_io)? {
                let info: LocalNodeInfo = bincode::deserialize(v.value()).map_err(into_io)?;
                return Ok(Some(info));
            }
            Ok(None)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("join error: {e}")))?
    }

    async fn store_local_node(&self, info: &LocalNodeInfo) -> io::Result<()> {
        let db = self.db.clone();
        let bytes = bincode::serialize(info).map_err(into_io)?;
        tokio::task::spawn_blocking(move || {
            let wtxn = db.begin_write().map_err(into_io)?;
            {
                let mut t = wtxn.open_table(META).map_err(into_io)?;
                t.insert("self_info", bytes.as_slice()).map_err(into_io)?;
            }
            wtxn.commit().map_err(into_io)?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("join error: {e}")))?
    }

    async fn store_tombstone(&self, id: Uuid) -> std::io::Result<u64> {
        let db = self.db.clone();
        let key = id.to_string();

        tokio::task::spawn_blocking(move || {
            let wtxn = db.begin_write().map_err(into_io)?;

            // Read current tomb_seq in its own scope (drop guard & table)
            let next: u64 = {
                let meta = wtxn.open_table(META).map_err(into_io)?;
                // AccessGuard borrows `meta`, keep it in a tighter scope.
                let current = {
                    let maybe = meta.get("tomb_seq").map_err(into_io)?;
                    if let Some(guard) = maybe {
                        let bytes = guard.value();
                        let mut arr = [0u8; 8];
                        if bytes.len() == 8 {
                            arr.copy_from_slice(bytes);
                        }
                        u64::from_be_bytes(arr)
                    } else {
                        0
                    }
                };
                current.saturating_add(1)
            };

            let next_bytes = next.to_be_bytes();

            //  Write bumped tomb_seq (fresh table handle, no outstanding borrows)
            {
                let mut meta = wtxn.open_table(META).map_err(into_io)?;
                meta.insert("tomb_seq", &next_bytes[..]).map_err(into_io)?;
            }

            // Write (id -> ts) tombstone ----
            {
                let mut tombs = wtxn.open_table(TOMBS).map_err(into_io)?;
                tombs
                    .insert(key.as_str(), &next_bytes[..])
                    .map_err(into_io)?;
            }

            wtxn.commit().map_err(into_io)?;
            Ok::<_, std::io::Error>(next)
        })
        .await
        .map_err(|e| into_io(format!("join error: {e}")))?
    }

    async fn remove_tombstone(&self, id: Uuid) -> io::Result<()> {
        let db = self.db.clone();
        let key = id.to_string();
        tokio::task::spawn_blocking(move || {
            let w = db.begin_write().map_err(into_io)?;
            {
                let mut t = w.open_table(TOMBS).map_err(into_io)?;
                let _ = t.remove(key.as_str()).map_err(into_io)?;
            }
            w.commit().map_err(into_io)?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|e| into_io(format!("join error: {e}")))?
    }

    async fn load_tombstones(&self) -> io::Result<Vec<(Uuid, u64)>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let r = db.begin_read().map_err(into_io)?;
            let t = r.open_table(TOMBS).map_err(into_io)?;
            let mut out = Vec::new();
            for kv in t.iter().map_err(into_io)? {
                let (k, v) = kv.map_err(into_io)?;
                let id = Uuid::parse_str(k.value())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                let bytes = v.value();
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(bytes);
                    out.push((id, u64::from_be_bytes(arr)));
                }
            }
            Ok::<_, io::Error>(out)
        })
        .await
        .map_err(|e| into_io(format!("join error: {e}")))?
    }
}
