use crate::cluster::ClusterViewId;
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, TableDefinition};
use std::io;
use std::sync::Arc;

/// Redb table storing one persisted active cluster view record.
const T_ACTIVE_CLUSTER_VIEW: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("active_cluster_view");

/// Stable key used for the single active-view row.
const ACTIVE_VIEW_KEY: &str = "active";

/// Durable store for the local node's active cluster view.
#[derive(Clone)]
pub struct ClusterViewStore {
    db: Arc<Database>,
}

impl ClusterViewStore {
    /// Opens the active-view table and returns a handle used by topology bootstrap and commits.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db })
    }

    /// Loads the persisted active cluster view, if one has been stored.
    pub fn read_active_view(&self) -> io::Result<Option<ClusterViewId>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            let payload = table.get(ACTIVE_VIEW_KEY).map_err(into_io)?;
            let Some(payload) = payload else {
                return Ok(None);
            };

            let view: ClusterViewId = bincode::deserialize(payload.value()).map_err(into_io)?;
            Ok(Some(view))
        })
    }

    /// Persists the provided active cluster view atomically.
    pub fn write_active_view(&self, view: ClusterViewId) -> io::Result<()> {
        let payload = bincode::serialize(&view).map_err(into_io)?;
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            table
                .insert(ACTIVE_VIEW_KEY, payload.as_slice())
                .map_err(into_io)?;
            Ok(())
        })
    }
}
