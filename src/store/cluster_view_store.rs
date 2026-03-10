use crate::cluster::{ClusterId, ClusterViewId};
use crate::store::open::open_arc_store;
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use crdt_store::adapter::MvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::mvreg::MvRegSnapshot;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;
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
    MvRegAdapterSorted<UuidKey, ClusterNameRecord, Uuid>,
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

    /// Selects the deterministic winner from one MVReg snapshot of name records.
    fn winner(snapshot: &MvRegSnapshot<Self>) -> Option<Self> {
        snapshot
            .as_slice()
            .iter()
            .cloned()
            .max_by(|left, right| left.precedence_key().cmp(&right.precedence_key()))
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
        self.cluster_view_domain
            .rebuild_mst_from_disk()
            .await
            .map_err(io::Error::other)
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

    /// Reads the deterministic winning record currently visible for one cluster lineage.
    fn winning_cluster_name_for(
        &self,
        cluster_id: ClusterId,
    ) -> io::Result<Option<ClusterNameRecord>> {
        let key = UuidKey::from(cluster_id.to_uuid());
        let snapshot = self
            .cluster_view_domain
            .get_snapshot(&key)
            .map_err(io::Error::other)?;
        Ok(snapshot.as_ref().and_then(ClusterNameRecord::winner))
    }

    /// Applies one conflict-resolved cluster name update and reports whether the row changed.
    pub async fn upsert_cluster_name(
        &self,
        cluster_id: ClusterId,
        incoming: &ClusterNameRecord,
    ) -> io::Result<bool> {
        if let Some(existing) = self.winning_cluster_name_for(cluster_id)?
            && !incoming.supersedes(&existing)
        {
            return Ok(false);
        }

        let key = UuidKey::from(cluster_id.to_uuid());
        self.cluster_view_domain
            .upsert(&key, incoming.clone())
            .await
            .map_err(io::Error::other)?;
        Ok(true)
    }

    /// Lists all persisted cluster lineage names as `(cluster_id, record)` tuples.
    pub fn list_cluster_names(&self) -> io::Result<Vec<(ClusterId, ClusterNameRecord)>> {
        let (actives, _tombs) = self
            .cluster_view_domain
            .load_all()
            .map_err(io::Error::other)?;
        let mut out = Vec::with_capacity(actives.len());

        for (cluster_key, snapshot) in actives {
            let Some(record) = ClusterNameRecord::winner(&snapshot) else {
                continue;
            };
            out.push((ClusterId::from_uuid(cluster_key.to_uuid()), record));
        }

        out.sort_by(|(left_id, _), (right_id, _)| left_id.cmp(right_id));
        Ok(out)
    }
}
