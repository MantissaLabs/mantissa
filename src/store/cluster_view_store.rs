use crate::cluster::ClusterViewId;
use redb::{Database, TableDefinition};
use std::io;
use std::sync::Arc;

/// Redb table storing one persisted active cluster view record.
const T_ACTIVE_CLUSTER_VIEW: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("active_cluster_view");

/// Stable key used for the single active-view row.
const ACTIVE_VIEW_KEY: &str = "active";

#[inline]
fn ioerr<E: std::error::Error>(err: E) -> io::Error {
    io::Error::other(err.to_string())
}

/// Durable store for the local node's active cluster view.
#[derive(Clone)]
pub struct ClusterViewStore {
    db: Arc<Database>,
}

impl ClusterViewStore {
    /// Opens the active-view table and returns a handle used by topology bootstrap and commits.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let write_transaction = db.begin_write().map_err(ioerr)?;
        {
            let _ = write_transaction
                .open_table(T_ACTIVE_CLUSTER_VIEW)
                .map_err(ioerr)?;
        }
        write_transaction.commit().map_err(ioerr)?;
        Ok(Self { db })
    }

    /// Loads the persisted active cluster view, if one has been stored.
    pub fn read_active_view(&self) -> io::Result<Option<ClusterViewId>> {
        let read_transaction = self.db.begin_read().map_err(ioerr)?;
        let table = read_transaction
            .open_table(T_ACTIVE_CLUSTER_VIEW)
            .map_err(ioerr)?;
        let payload = table.get(ACTIVE_VIEW_KEY).map_err(ioerr)?;
        let Some(payload) = payload else {
            return Ok(None);
        };

        let view: ClusterViewId = bincode::deserialize(payload.value())
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Some(view))
    }

    /// Persists the provided active cluster view atomically.
    pub fn write_active_view(&self, view: ClusterViewId) -> io::Result<()> {
        let payload = bincode::serialize(&view).map_err(|err| io::Error::other(err.to_string()))?;
        let write_transaction = self.db.begin_write().map_err(ioerr)?;
        {
            let mut table = write_transaction
                .open_table(T_ACTIVE_CLUSTER_VIEW)
                .map_err(ioerr)?;
            table
                .insert(ACTIVE_VIEW_KEY, payload.as_slice())
                .map_err(ioerr)?;
        }
        write_transaction.commit().map_err(ioerr)?;
        Ok(())
    }
}
