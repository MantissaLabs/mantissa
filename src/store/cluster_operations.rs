use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, ReadableTable, TableDefinition};
use std::io;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table storing cluster operation payloads by operation UUID.
const T_CLUSTER_OPERATIONS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("cluster_operations");

/// Durable key/value store for serialized cluster operation records.
#[derive(Clone)]
pub struct ClusterOperationStore {
    db: Arc<Database>,
}

impl ClusterOperationStore {
    /// Opens the operation table and returns a handle used by topology orchestration paths.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db })
    }

    /// Persists a serialized operation payload for the provided operation identifier.
    pub fn put(&self, id: Uuid, payload: &[u8]) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            table.insert(*id.as_bytes(), payload).map_err(into_io)?;
            Ok(())
        })
    }

    /// Loads a serialized operation payload by identifier, if present.
    pub fn get(&self, id: Uuid) -> io::Result<Option<Vec<u8>>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            let value = table
                .get(*id.as_bytes())
                .map_err(into_io)?
                .map(|guard| guard.value().to_vec());
            Ok(value)
        })
    }

    /// Lists all serialized operation payloads currently present in the store.
    pub fn list(&self) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            let mut out = Vec::new();

            for entry in table.iter().map_err(into_io)? {
                let (key, value) = entry.map_err(into_io)?;
                out.push((Uuid::from_bytes(key.value()), value.value().to_vec()));
            }

            Ok(out)
        })
    }

    /// Deletes multiple operation payloads atomically and returns how many rows were removed.
    pub fn delete_many(&self, ids: &[Uuid]) -> io::Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }

        with_write_tx(&self.db, |tx| {
            let mut removed = 0usize;
            let mut table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            for id in ids {
                if table.remove(*id.as_bytes()).map_err(into_io)?.is_some() {
                    removed = removed.saturating_add(1);
                }
            }
            Ok(removed)
        })
    }
}
