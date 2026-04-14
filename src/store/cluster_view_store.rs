use crate::cluster::{ClusterId, ClusterViewId};
use crate::store::open::open_arc_store;
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::codec;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::mvreg::MvRegSnapshot;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

/// Redb table storing one persisted active cluster view record.
const T_ACTIVE_CLUSTER_VIEW: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("active_cluster_view");

/// Stable key used for the single active-view row.
const ACTIVE_VIEW_KEY: &str = "active";

/// Cluster-view domain tables replicated through anti-entropy.
pub struct ClusterViewDomainTables;

impl TableSet for ClusterViewDomainTables {
    const VALUES: &'static str = "cluster_view_values";
    const TOMBS: &'static str = "cluster_view_tombs";
    const META: &'static str = "cluster_view_meta";
}

/// Specialized MST/CRDT store for replicated cluster-view lineage metadata.
pub type ClusterViewDomainStoreInner = CrdtMstStore<
    MvRegAdapterSorted<UuidKey, ClusterViewMetadataRecord, Uuid>,
    XXHash128,
    ClusterViewDomainTables,
>;

/// Shared handle to the cluster-view metadata domain store.
pub type ClusterViewDomainStore = Arc<ClusterViewDomainStoreInner>;

/// Conflict-resolved cluster lineage name record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
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

/// Conflict-resolved cluster lineage node-count record.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClusterNodeCountRecord {
    pub node_count: u32,
    pub updated_at_unix_ms: u64,
    pub actor_node_id: Uuid,
}

impl ClusterNodeCountRecord {
    /// Returns whether this record should replace `current` in deterministic conflict resolution.
    fn supersedes(&self, current: &Self) -> bool {
        self.precedence_key() > current.precedence_key()
    }

    /// Builds the ordering key used for deterministic node-count conflict resolution.
    fn precedence_key(&self) -> (u64, Uuid, u32) {
        (self.updated_at_unix_ms, self.actor_node_id, self.node_count)
    }
}

/// Replicated cluster lineage metadata carried through the `cluster_views` sync domain.
///
/// Each field uses its own last-writer metadata so future cluster-level metadata can be added
/// without introducing one monolithic precedence order for unrelated fields.
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClusterViewMetadataRecord {
    #[serde(default)]
    pub name: Option<ClusterNameRecord>,
    #[serde(default)]
    pub node_count: Option<ClusterNodeCountRecord>,
}

impl ClusterViewMetadataRecord {
    /// Returns true when this metadata row carries no fields.
    fn is_empty(&self) -> bool {
        self.name.is_none() && self.node_count.is_none()
    }

    /// Merges two metadata rows by resolving each field independently.
    fn merge(left: &Self, right: &Self) -> Self {
        let name = match (&left.name, &right.name) {
            (Some(left), Some(right)) => {
                if right.supersedes(left) {
                    Some(right.clone())
                } else {
                    Some(left.clone())
                }
            }
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (None, None) => None,
        };
        let node_count = match (&left.node_count, &right.node_count) {
            (Some(left), Some(right)) => {
                if right.supersedes(left) {
                    Some(right.clone())
                } else {
                    Some(left.clone())
                }
            }
            (Some(left), None) => Some(left.clone()),
            (None, Some(right)) => Some(right.clone()),
            (None, None) => None,
        };
        Self { name, node_count }
    }

    /// Builds one merged metadata winner from a raw MVReg snapshot.
    fn winner(snapshot: &MvRegSnapshot<Self>) -> Option<Self> {
        let mut merged = None::<Self>;
        for value in snapshot.as_slice() {
            merged = Some(match merged {
                Some(current) => Self::merge(&current, value),
                None => value.clone(),
            });
        }
        merged.filter(|record| !record.is_empty())
    }
}

/// Durable store for local active-view state and replicated cluster-view lineage metadata.
#[derive(Clone)]
pub struct ClusterViewStore {
    db: Arc<Database>,
    cluster_view_domain: ClusterViewDomainStore,
}

impl ClusterViewStore {
    /// Opens local and replicated cluster-view storage with one actor id for CRDT writes.
    pub fn new(db: Arc<Database>, actor_node_id: Uuid) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            Ok(())
        })?;

        let cluster_view_domain = open_arc_store(db.clone(), actor_node_id, |db, actor| {
            ClusterViewDomainStoreInner::builder(db, actor)
                .with_preserve_local_tombs(true)
                .build()
        })?;

        Ok(Self {
            db,
            cluster_view_domain,
        })
    }

    /// Rebuilds the in-memory MST for the replicated cluster-view metadata domain.
    pub async fn rebuild_cluster_view_domain_mst(&self) -> io::Result<()> {
        match self.cluster_view_domain.rebuild_mst_from_disk().await {
            Ok(()) => Ok(()),
            Err(err) => {
                warn!(
                    target: "cluster_view",
                    "failed to rebuild cluster-view metadata MST, purging stale local metadata: {err}"
                );
                self.purge_cluster_view_domain_data()?;
                self.cluster_view_domain
                    .rebuild_mst_from_disk()
                    .await
                    .map_err(io::Error::other)
            }
        }
    }

    /// Purges all replicated cluster-view metadata rows from local storage.
    ///
    /// This provides the hard-cutover path for metadata schema changes: names can be rehydrated
    /// from durable cluster operations and node counts are republished from live membership state.
    fn purge_cluster_view_domain_data(&self) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            {
                let mut values = tx
                    .open_table(ClusterViewDomainTables::values())
                    .map_err(into_io)?;
                let keys = values
                    .iter()
                    .map_err(into_io)?
                    .map(|entry| entry.map(|(key, _)| key.value().to_vec()).map_err(into_io))
                    .collect::<io::Result<Vec<_>>>()?;
                for key in keys {
                    let _ = values.remove(key.as_slice()).map_err(into_io)?;
                }
            }
            {
                let mut tombs = tx
                    .open_table(ClusterViewDomainTables::tombs())
                    .map_err(into_io)?;
                let keys = tombs
                    .iter()
                    .map_err(into_io)?
                    .map(|entry| entry.map(|(key, _)| key.value().to_vec()).map_err(into_io))
                    .collect::<io::Result<Vec<_>>>()?;
                for key in keys {
                    let _ = tombs.remove(key.as_slice()).map_err(into_io)?;
                }
            }
            {
                let mut meta = tx
                    .open_table(ClusterViewDomainTables::meta())
                    .map_err(into_io)?;
                let keys = meta
                    .iter()
                    .map_err(into_io)?
                    .map(|entry| {
                        entry
                            .map(|(key, _)| key.value().to_string())
                            .map_err(into_io)
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                for key in keys {
                    let _ = meta.remove(key.as_str()).map_err(into_io)?;
                }
            }
            Ok(())
        })
    }

    /// Returns the replicated cluster-view metadata domain handle for sync anti-entropy.
    pub fn cluster_view_domain_store(&self) -> ClusterViewDomainStore {
        self.cluster_view_domain.clone()
    }

    /// Loads the persisted active cluster view, if one has been stored.
    pub fn read_active_view(&self) -> io::Result<Option<ClusterViewId>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            let payload = table.get(ACTIVE_VIEW_KEY).map_err(into_io)?;
            let Some(payload) = payload else {
                return Ok(None);
            };

            let view: ClusterViewId = codec::decode(payload.value()).map_err(into_io)?;
            Ok(Some(view))
        })
    }

    /// Persists the provided active cluster view atomically.
    pub fn write_active_view(&self, view: ClusterViewId) -> io::Result<()> {
        let payload = codec::encode(&view).map_err(into_io)?;
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_ACTIVE_CLUSTER_VIEW).map_err(into_io)?;
            table
                .insert(ACTIVE_VIEW_KEY, payload.as_slice())
                .map_err(into_io)?;
            Ok(())
        })
    }

    /// Reads the deterministic winning metadata currently visible for one cluster lineage.
    fn winning_cluster_metadata_for(
        &self,
        cluster_id: ClusterId,
    ) -> io::Result<Option<ClusterViewMetadataRecord>> {
        let key = UuidKey::from(cluster_id.to_uuid());
        let snapshot = self
            .cluster_view_domain
            .get_snapshot(&key)
            .map_err(io::Error::other)?;
        Ok(snapshot
            .as_ref()
            .and_then(ClusterViewMetadataRecord::winner))
    }

    /// Applies one conflict-resolved cluster name update and reports whether the row changed.
    pub async fn upsert_cluster_name(
        &self,
        cluster_id: ClusterId,
        incoming: &ClusterNameRecord,
    ) -> io::Result<bool> {
        let current = self
            .winning_cluster_metadata_for(cluster_id)?
            .unwrap_or_default();
        if let Some(existing) = current.name.as_ref()
            && !incoming.supersedes(existing)
        {
            return Ok(false);
        }

        let key = UuidKey::from(cluster_id.to_uuid());
        self.cluster_view_domain
            .upsert(
                &key,
                ClusterViewMetadataRecord {
                    name: Some(incoming.clone()),
                    node_count: current.node_count,
                },
            )
            .await
            .map_err(io::Error::other)?;
        Ok(true)
    }

    /// Applies one conflict-resolved cluster lineage node-count update and reports whether the row changed.
    pub async fn upsert_cluster_node_count(
        &self,
        cluster_id: ClusterId,
        incoming: &ClusterNodeCountRecord,
    ) -> io::Result<bool> {
        let current = self
            .winning_cluster_metadata_for(cluster_id)?
            .unwrap_or_default();
        if let Some(existing) = current.node_count.as_ref()
            && !incoming.supersedes(existing)
        {
            return Ok(false);
        }

        let key = UuidKey::from(cluster_id.to_uuid());
        self.cluster_view_domain
            .upsert(
                &key,
                ClusterViewMetadataRecord {
                    name: current.name,
                    node_count: Some(incoming.clone()),
                },
            )
            .await
            .map_err(io::Error::other)?;
        Ok(true)
    }

    /// Reads the deterministic winning node-count record currently visible for one cluster lineage.
    pub fn winning_cluster_node_count_for(
        &self,
        cluster_id: ClusterId,
    ) -> io::Result<Option<ClusterNodeCountRecord>> {
        Ok(self
            .winning_cluster_metadata_for(cluster_id)?
            .and_then(|record| record.node_count))
    }

    /// Lists all persisted cluster lineage metadata rows as `(cluster_id, record)` tuples.
    pub fn list_cluster_metadata(&self) -> io::Result<Vec<(ClusterId, ClusterViewMetadataRecord)>> {
        let (actives, _tombs) = self
            .cluster_view_domain
            .load_all()
            .map_err(io::Error::other)?;
        let mut out = Vec::with_capacity(actives.len());

        for (cluster_key, snapshot) in actives {
            let Some(record) = ClusterViewMetadataRecord::winner(&snapshot) else {
                continue;
            };
            out.push((ClusterId::from_uuid(cluster_key.to_uuid()), record));
        }

        out.sort_by(|(left_id, _), (right_id, _)| left_id.cmp(right_id));
        Ok(out)
    }

    /// Lists all persisted cluster lineage names as `(cluster_id, record)` tuples.
    pub fn list_cluster_names(&self) -> io::Result<Vec<(ClusterId, ClusterNameRecord)>> {
        Ok(self
            .list_cluster_metadata()?
            .into_iter()
            .filter_map(|(cluster_id, record)| record.name.map(|name| (cluster_id, name)))
            .collect())
    }
}
