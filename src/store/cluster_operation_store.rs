use redb::{Database, TableDefinition};
use std::io;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table storing cluster operation payloads by operation UUID.
const T_CLUSTER_OPERATIONS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("cluster_operations");

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// Durable key/value store for serialized cluster operation records.
#[derive(Clone)]
pub struct ClusterOperationStore {
    db: Arc<Database>,
}

impl ClusterOperationStore {
    /// Opens the operation table and returns a handle used by topology orchestration paths.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let w = db.begin_write().map_err(ioerr)?;
        {
            let _ = w.open_table(T_CLUSTER_OPERATIONS).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(Self { db })
    }

    /// Persists a serialized operation payload for the provided operation identifier.
    pub fn put(&self, id: Uuid, payload: &[u8]) -> io::Result<()> {
        let w = self.db.begin_write().map_err(ioerr)?;
        {
            let mut table = w.open_table(T_CLUSTER_OPERATIONS).map_err(ioerr)?;
            table.insert(*id.as_bytes(), payload).map_err(ioerr)?;
        }
        w.commit().map_err(ioerr)?;
        Ok(())
    }

    /// Loads a serialized operation payload by identifier, if present.
    pub fn get(&self, id: Uuid) -> io::Result<Option<Vec<u8>>> {
        let r = self.db.begin_read().map_err(ioerr)?;
        let table = r.open_table(T_CLUSTER_OPERATIONS).map_err(ioerr)?;
        let value = table
            .get(*id.as_bytes())
            .map_err(ioerr)?
            .map(|guard| guard.value().to_vec());
        Ok(value)
    }
}
