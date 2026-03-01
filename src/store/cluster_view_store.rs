use crate::cluster::{ClusterId, ClusterViewId};
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;
use uuid::Uuid;

/// Redb table storing one persisted active cluster view record.
const T_ACTIVE_CLUSTER_VIEW: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("active_cluster_view");
/// Redb table storing friendly cluster lineage names keyed by cluster id bytes.
const T_CLUSTER_NAMES: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("cluster_names");

/// Stable key used for the single active-view row.
const ACTIVE_VIEW_KEY: &str = "active";

/// Conflict-resolved cluster lineage name record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterNameRecord {
    pub name: String,
    pub updated_at_unix_ms: u64,
    pub actor_node_id: Uuid,
}

impl ClusterNameRecord {
    /// Returns whether this record should replace `current` in deterministic conflict resolution.
    ///
    /// This keeps cluster-name convergence stable across peers by ordering writes by:
    /// `updated_at_unix_ms`, then `actor_node_id`, then the name text.
    fn supersedes(&self, current: &Self) -> bool {
        self.precedence_key() > current.precedence_key()
    }

    /// Builds the ordering key used for deterministic cluster-name conflict resolution.
    fn precedence_key(&self) -> (u64, Uuid, &str) {
        (
            self.updated_at_unix_ms,
            self.actor_node_id,
            self.name.as_str(),
        )
    }
}

/// Durable store for the local node's active cluster view.
#[derive(Clone)]
pub struct ClusterViewStore {
    db: Arc<Database>,
}

impl ClusterViewStore {
    /// Decodes one serialized cluster-name payload into the durable record representation.
    fn decode_cluster_name_record(payload: &[u8]) -> io::Result<ClusterNameRecord> {
        bincode::deserialize(payload).map_err(into_io)
    }

    /// Opens the active-view table and returns a handle used by topology bootstrap and commits.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            let _ = tx.open_table(T_CLUSTER_NAMES).map_err(into_io)?;
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

    /// Applies one conflict-resolved cluster name update and reports whether the row changed.
    pub fn upsert_cluster_name(
        &self,
        cluster_id: ClusterId,
        incoming: &ClusterNameRecord,
    ) -> io::Result<bool> {
        let payload = bincode::serialize(incoming).map_err(into_io)?;
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_CLUSTER_NAMES).map_err(into_io)?;
            let should_replace = {
                let current = table.get(*cluster_id.as_bytes()).map_err(into_io)?;
                match current {
                    None => true,
                    Some(existing) => {
                        let existing = Self::decode_cluster_name_record(existing.value())?;
                        incoming.supersedes(&existing)
                    }
                }
            };

            if !should_replace {
                return Ok(false);
            }

            table
                .insert(*cluster_id.as_bytes(), payload.as_slice())
                .map_err(into_io)?;
            Ok(true)
        })
    }

    /// Lists all persisted cluster lineage names as `(cluster_id, record)` tuples.
    pub fn list_cluster_names(&self) -> io::Result<Vec<(ClusterId, ClusterNameRecord)>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_CLUSTER_NAMES).map_err(into_io)?;
            let mut out = Vec::new();
            for row in table.iter().map_err(into_io)? {
                let (cluster_key, payload) = row.map_err(into_io)?;
                let record = Self::decode_cluster_name_record(payload.value())?;
                out.push((ClusterId::from_bytes(cluster_key.value()), record));
            }

            out.sort_by(|(left_id, _), (right_id, _)| left_id.cmp(right_id));
            Ok(out)
        })
    }
}
