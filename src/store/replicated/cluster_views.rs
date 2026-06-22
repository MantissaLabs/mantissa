use crate::cluster::{ClusterId, ClusterViewId};
use crate::store::replicated::open::open_arc_store;
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use mantissa_protocol::topology::{
    cluster_name_record, cluster_node_count_record, cluster_view_id, cluster_view_metadata_record,
};
use mantissa_store::adapter::StoreMvRegAdapterSorted;
use mantissa_store::codec::StoreValueCodec;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::MvRegSnapshot;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::io;
use std::io::Cursor;
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
    const TOMBS_BY_OBSERVED: &'static str = "cluster_view_tombs_by_observed";
    const META: &'static str = "cluster_view_meta";
}

/// Specialized MST/CRDT store for replicated cluster-view lineage metadata.
pub type ClusterViewDomainStoreInner = CrdtMstStore<
    StoreMvRegAdapterSorted<UuidKey, ClusterViewMetadataRecord, Uuid>,
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
    pub source_view: ClusterViewId,
    pub updated_at_unix_ms: u64,
    pub actor_node_id: Uuid,
    pub membership_generation: u64,
}

impl ClusterNodeCountRecord {
    /// Returns whether this record was computed by the cluster lineage it summarizes.
    fn authoritative_for(&self, cluster_id: ClusterId) -> bool {
        self.source_view.cluster_id == cluster_id
    }

    /// Returns whether this record should replace `current` for the target lineage.
    fn supersedes_for(&self, cluster_id: ClusterId, current: &Self) -> bool {
        self.precedence_key(cluster_id) > current.precedence_key(cluster_id)
    }

    /// Builds the ordering key used for deterministic node-count conflict resolution.
    ///
    /// The authority bit prevents metadata from another split view from replacing the count
    /// computed by the lineage being summarized. The actor-local membership generation is ordered
    /// after the actor id on purpose: it only breaks ties between observations from the same node.
    fn precedence_key(&self, cluster_id: ClusterId) -> (bool, u64, u64, Uuid, u64, u32) {
        (
            self.authoritative_for(cluster_id),
            self.source_view.epoch,
            self.updated_at_unix_ms,
            self.actor_node_id,
            self.membership_generation,
            self.node_count,
        )
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
    fn merge_for(cluster_id: ClusterId, left: &Self, right: &Self) -> Self {
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
                if right.supersedes_for(cluster_id, left) {
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
    ///
    /// Non-authoritative node-count rows can still be present after raw sync, but they are hidden
    /// from the resolved view instead of being allowed to seed an incorrect cluster size.
    fn winner_for(cluster_id: ClusterId, snapshot: &MvRegSnapshot<Self>) -> Option<Self> {
        let mut merged = None::<Self>;
        for value in snapshot.as_slice() {
            merged = Some(match merged {
                Some(current) => Self::merge_for(cluster_id, &current, value),
                None => value.clone(),
            });
        }
        merged
            .map(|mut record| {
                if record
                    .node_count
                    .as_ref()
                    .is_some_and(|count| !count.authoritative_for(cluster_id))
                {
                    record.node_count = None;
                }
                record
            })
            .filter(|record| !record.is_empty())
    }
}

impl StoreValueCodec for ClusterViewMetadataRecord {
    /// Encodes one cluster-view metadata record as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_cluster_view_metadata_record(
            message.init_root::<cluster_view_metadata_record::Builder<'_>>(),
            self,
        );
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one cluster-view metadata record from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(cluster_view_store_codec_error)?;
        let record = reader
            .get_root::<cluster_view_metadata_record::Reader<'_>>()
            .map_err(cluster_view_store_codec_error)?;
        read_cluster_view_metadata_record(record).map_err(cluster_view_store_codec_error)
    }
}

/// Encodes one cluster-view metadata row into the store schema.
fn write_cluster_view_metadata_record(
    mut builder: cluster_view_metadata_record::Builder<'_>,
    record: &ClusterViewMetadataRecord,
) {
    if let Some(name) = record.name.as_ref() {
        write_cluster_name_record(builder.reborrow().init_name(), name);
    }
    if let Some(node_count) = record.node_count.as_ref() {
        write_cluster_node_count_record(builder.reborrow().init_node_count(), node_count);
    }
}

/// Decodes one cluster-view metadata row from the store schema.
fn read_cluster_view_metadata_record(
    reader: cluster_view_metadata_record::Reader<'_>,
) -> Result<ClusterViewMetadataRecord, capnp::Error> {
    let name = if reader.has_name() {
        Some(read_cluster_name_record(reader.get_name()?)?)
    } else {
        None
    };
    let node_count = if reader.has_node_count() {
        Some(read_cluster_node_count_record(reader.get_node_count()?)?)
    } else {
        None
    };
    Ok(ClusterViewMetadataRecord { name, node_count })
}

/// Encodes one cluster name record into the store schema.
fn write_cluster_name_record(
    mut builder: cluster_name_record::Builder<'_>,
    record: &ClusterNameRecord,
) {
    builder.set_name(&record.name);
    builder.set_updated_at_unix_ms(record.updated_at_unix_ms);
    builder.set_actor_node_id(record.actor_node_id.as_bytes());
}

/// Decodes one cluster name record from the store schema.
fn read_cluster_name_record(
    reader: cluster_name_record::Reader<'_>,
) -> Result<ClusterNameRecord, capnp::Error> {
    Ok(ClusterNameRecord {
        name: reader.get_name()?.to_str()?.to_string(),
        updated_at_unix_ms: reader.get_updated_at_unix_ms(),
        actor_node_id: read_uuid_data(reader.get_actor_node_id()?, "cluster name actor node id")?,
    })
}

/// Encodes one cluster node-count record into the store schema.
fn write_cluster_node_count_record(
    mut builder: cluster_node_count_record::Builder<'_>,
    record: &ClusterNodeCountRecord,
) {
    builder.set_node_count(record.node_count);
    builder.set_updated_at_unix_ms(record.updated_at_unix_ms);
    builder.set_actor_node_id(record.actor_node_id.as_bytes());
    record
        .source_view
        .write_capnp(builder.reborrow().init_source_view());
    builder.set_membership_generation(record.membership_generation);
}

/// Decodes one cluster node-count record from the store schema.
fn read_cluster_node_count_record(
    reader: cluster_node_count_record::Reader<'_>,
) -> Result<ClusterNodeCountRecord, capnp::Error> {
    Ok(ClusterNodeCountRecord {
        node_count: reader.get_node_count(),
        source_view: ClusterViewId::from_capnp(reader.get_source_view()?)
            .map_err(capnp::Error::failed)?,
        updated_at_unix_ms: reader.get_updated_at_unix_ms(),
        actor_node_id: read_uuid_data(
            reader.get_actor_node_id()?,
            "cluster node-count actor node id",
        )?,
        membership_generation: reader.get_membership_generation(),
    })
}

/// Encodes one active cluster view into its local singleton store row.
fn encode_active_cluster_view(view: ClusterViewId) -> io::Result<Vec<u8>> {
    let mut message = capnp::message::Builder::new_default();
    view.write_capnp(message.init_root::<cluster_view_id::Builder<'_>>());
    Ok(capnp::serialize::write_message_to_words(&message))
}

/// Decodes one active cluster view from its local singleton store row.
fn decode_active_cluster_view(bytes: &[u8]) -> io::Result<ClusterViewId> {
    let mut cursor = Cursor::new(bytes);
    let reader = capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
        .map_err(into_io)?;
    let view = reader
        .get_root::<cluster_view_id::Reader<'_>>()
        .map_err(into_io)?;
    ClusterViewId::from_capnp(view).map_err(io::Error::other)
}

/// Decodes one required UUID from a store `Data` field.
fn read_uuid_data(data: capnp::data::Reader<'_>, field: &str) -> Result<Uuid, capnp::Error> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| capnp::Error::failed(format!("invalid {field}: expected 16-byte UUID")))?;
    Ok(Uuid::from_bytes(slice))
}

/// Converts cluster-view store-codec errors into the CRDT store error type.
fn cluster_view_store_codec_error<E: std::fmt::Display>(
    error: E,
) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "cluster-view store codec error: {error}"
    )))
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
    /// This provides the hard-cutover path for metadata schema changes: names can be restored
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

            let view = decode_active_cluster_view(payload.value())?;
            Ok(Some(view))
        })
    }

    /// Persists the provided active cluster view atomically.
    pub fn write_active_view(&self, view: ClusterViewId) -> io::Result<()> {
        let payload = encode_active_cluster_view(view)?;
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
            .and_then(|snapshot| ClusterViewMetadataRecord::winner_for(cluster_id, snapshot)))
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
        if !incoming.authoritative_for(cluster_id) {
            return Ok(false);
        }

        let current = self
            .winning_cluster_metadata_for(cluster_id)?
            .unwrap_or_default();
        if let Some(existing) = current.node_count.as_ref()
            && !incoming.supersedes_for(cluster_id, existing)
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
            let cluster_id = ClusterId::from_uuid(cluster_key.to_uuid());
            let Some(record) = ClusterViewMetadataRecord::winner_for(cluster_id, &snapshot) else {
                continue;
            };
            out.push((cluster_id, record));
        }

        out.sort_by_key(|(view_id, _)| *view_id);
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

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_store::codec::StoreValueCodec;
    use mantissa_store::uuid_key::UuidKey;
    use tempfile::tempdir;

    /// Builds one authoritative node-count record for a test cluster lineage.
    fn count_record(
        view: ClusterViewId,
        node_count: u32,
        updated_at_unix_ms: u64,
        actor_node_id: Uuid,
        membership_generation: u64,
    ) -> ClusterNodeCountRecord {
        ClusterNodeCountRecord {
            node_count,
            source_view: view,
            updated_at_unix_ms,
            actor_node_id,
            membership_generation,
        }
    }

    /// Builds one metadata row that exercises every cluster-view store field.
    fn sample_metadata_record() -> ClusterViewMetadataRecord {
        let cluster_id = ClusterId::from_uuid(Uuid::new_v4());
        let source_view = ClusterViewId::new(cluster_id, 7);
        ClusterViewMetadataRecord {
            name: Some(ClusterNameRecord {
                name: "production".to_string(),
                updated_at_unix_ms: 1_776_000_000_001,
                actor_node_id: Uuid::new_v4(),
            }),
            node_count: Some(count_record(
                source_view,
                7,
                1_776_000_000_002,
                Uuid::new_v4(),
                42,
            )),
        }
    }

    /// Cluster-view metadata should round-trip through the Cap'n Proto store-value codec.
    #[test]
    fn store_value_codec_roundtrips_cluster_view_metadata() {
        let record = sample_metadata_record();
        let encoded = record
            .encode_store_value()
            .expect("encode cluster-view metadata");
        let decoded = ClusterViewMetadataRecord::decode_store_value(&encoded)
            .expect("decode cluster-view metadata");
        assert_eq!(decoded, record);

        let name_only = ClusterViewMetadataRecord {
            node_count: None,
            ..record.clone()
        };
        let encoded = name_only
            .encode_store_value()
            .expect("encode name-only metadata");
        let decoded = ClusterViewMetadataRecord::decode_store_value(&encoded)
            .expect("decode name-only metadata");
        assert_eq!(decoded, name_only);

        let empty = ClusterViewMetadataRecord::default();
        let encoded = empty.encode_store_value().expect("encode empty metadata");
        let decoded =
            ClusterViewMetadataRecord::decode_store_value(&encoded).expect("decode empty metadata");
        assert_eq!(decoded, empty);
    }

    /// The local active-view singleton should use the existing Cap'n Proto view id schema.
    #[test]
    fn active_cluster_view_codec_roundtrips_view_ids() {
        let view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 42);
        let encoded = encode_active_cluster_view(view).expect("encode active cluster view");
        let decoded = decode_active_cluster_view(&encoded).expect("decode active cluster view");
        assert_eq!(decoded, view);
    }

    /// Reopening the cluster-view store should decode Cap'n Proto rows from Redb.
    #[tokio::test]
    async fn cluster_view_store_reopens_capnp_rows() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("cluster-view-reopen-{}.redb", Uuid::new_v4()));
        let db = Arc::new(Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let cluster_id = ClusterId::from_uuid(Uuid::new_v4());
        let active_view = ClusterViewId::new(cluster_id, 3);
        let name = ClusterNameRecord {
            name: "edge".to_string(),
            updated_at_unix_ms: 1_776_000_001_001,
            actor_node_id: actor,
        };
        let count = count_record(active_view, 5, 1_776_000_001_002, actor, 17);

        {
            let store = ClusterViewStore::new(db.clone(), actor).expect("open cluster-view store");
            store
                .write_active_view(active_view)
                .expect("write active view");
            store
                .upsert_cluster_name(cluster_id, &name)
                .await
                .expect("upsert cluster name");
            store
                .upsert_cluster_node_count(cluster_id, &count)
                .await
                .expect("upsert node count");
        }

        let store = ClusterViewStore::new(db, actor).expect("reopen cluster-view store");
        store
            .rebuild_cluster_view_domain_mst()
            .await
            .expect("rebuild cluster-view metadata MST");

        assert_eq!(
            store.read_active_view().expect("read active view"),
            Some(active_view)
        );
        let metadata = store
            .list_cluster_metadata()
            .expect("list cluster metadata");
        assert_eq!(
            metadata,
            vec![(
                cluster_id,
                ClusterViewMetadataRecord {
                    name: Some(name),
                    node_count: Some(count),
                },
            )]
        );
    }

    /// Same-count updates should still advance the conflict-resolution fence when newer.
    #[tokio::test]
    async fn cluster_node_count_upsert_keeps_newer_observation_as_fence() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("cluster-view-count-{}.redb", Uuid::new_v4()));
        let db = Arc::new(Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let cluster_id = ClusterId::from_uuid(Uuid::new_v4());
        let source_view = ClusterViewId::new(cluster_id, 1);
        let store = ClusterViewStore::new(db, actor).expect("open cluster-view store");

        let first = count_record(source_view, 3, 10, actor, 1);
        let refreshed = count_record(source_view, 3, 20, Uuid::new_v4(), 2);
        let stale_arrival = count_record(source_view, 2, 15, Uuid::new_v4(), 2);
        let changed = count_record(source_view, 2, 30, actor, 3);

        assert!(
            store
                .upsert_cluster_node_count(cluster_id, &first)
                .await
                .expect("insert first count")
        );
        assert!(
            store
                .upsert_cluster_node_count(cluster_id, &refreshed)
                .await
                .expect("refresh same count")
        );
        assert_eq!(
            store
                .winning_cluster_node_count_for(cluster_id)
                .expect("read refreshed winning count"),
            Some(refreshed.clone())
        );

        assert!(
            !store
                .upsert_cluster_node_count(cluster_id, &stale_arrival)
                .await
                .expect("reject stale count")
        );
        assert_eq!(
            store
                .winning_cluster_node_count_for(cluster_id)
                .expect("read winning count after stale arrival"),
            Some(refreshed)
        );

        assert!(
            store
                .upsert_cluster_node_count(cluster_id, &changed)
                .await
                .expect("replace changed count")
        );
        assert_eq!(
            store
                .winning_cluster_node_count_for(cluster_id)
                .expect("read changed winning count"),
            Some(changed)
        );
    }

    /// Node-count metadata from the summarized cluster lineage should beat newer outside guesses.
    #[tokio::test]
    async fn authoritative_cluster_node_count_wins_over_cross_view_record() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("cluster-view-authority-{}.redb", Uuid::new_v4()));
        let db = Arc::new(Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let cluster_id = ClusterId::from_uuid(Uuid::new_v4());
        let authoritative_view = ClusterViewId::new(cluster_id, 1);
        let outsider_view = ClusterViewId::new(ClusterId::from_uuid(Uuid::new_v4()), 1);
        let store = ClusterViewStore::new(db, actor).expect("open cluster-view store");

        let authoritative = count_record(authoritative_view, 1, 10, actor, 1);
        let outsider = count_record(outsider_view, 2, 20, Uuid::new_v4(), 1);

        store
            .cluster_view_domain
            .upsert(
                &UuidKey::from(cluster_id.to_uuid()),
                ClusterViewMetadataRecord {
                    name: None,
                    node_count: Some(outsider),
                },
            )
            .await
            .expect("insert outside count through raw metadata domain");
        assert_eq!(
            store
                .winning_cluster_node_count_for(cluster_id)
                .expect("outside count should be ignored"),
            None
        );
        assert!(
            store
                .upsert_cluster_node_count(cluster_id, &authoritative)
                .await
                .expect("authoritative count replaces outside count")
        );
        assert_eq!(
            store
                .winning_cluster_node_count_for(cluster_id)
                .expect("read authoritative winning count"),
            Some(authoritative)
        );
    }

    /// Same-node count changes in the same millisecond should still move forward.
    #[tokio::test]
    async fn actor_membership_generation_breaks_same_timestamp_count_ties() {
        let dir = tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join(format!("cluster-view-generation-{}.redb", Uuid::new_v4()));
        let db = Arc::new(Database::create(db_path).expect("create db"));
        let actor = Uuid::new_v4();
        let cluster_id = ClusterId::from_uuid(Uuid::new_v4());
        let source_view = ClusterViewId::new(cluster_id, 1);
        let store = ClusterViewStore::new(db, actor).expect("open cluster-view store");

        let stale = count_record(source_view, 2, 10, actor, 1);
        let fresh = count_record(source_view, 1, 10, actor, 2);

        assert!(
            store
                .upsert_cluster_node_count(cluster_id, &stale)
                .await
                .expect("insert stale count")
        );
        assert!(
            store
                .upsert_cluster_node_count(cluster_id, &fresh)
                .await
                .expect("fresh generation replaces stale same-timestamp count")
        );
        assert_eq!(
            store
                .winning_cluster_node_count_for(cluster_id)
                .expect("read fresh winning count"),
            Some(fresh)
        );
    }
}
