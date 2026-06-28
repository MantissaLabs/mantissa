use crate::cluster::operations::ClusterOperationRecord;
use crate::store::replicated::cluster_operations::{
    ClusterOperationDomainStore, open_cluster_operation_domain_store,
};
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use mantissa_store::mvreg::MvRegSnapshot;
use mantissa_store::uuid_key::UuidKey;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::HashMap;
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
    replicated: Option<ClusterOperationDomainStore>,
}

impl ClusterOperationStore {
    /// Opens the local operation table without a replicated ledger mirror.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        Self::open(db, None)
    }

    /// Opens the local operation table with a replicated ledger mirror for anti-entropy.
    pub fn new_replicated(db: Arc<Database>, actor: Uuid) -> io::Result<Self> {
        let replicated = open_cluster_operation_domain_store(db.clone(), actor)?;
        Self::open(db, Some(replicated))
    }

    /// Opens the operation table and returns a handle used by topology orchestration paths.
    fn open(
        db: Arc<Database>,
        replicated: Option<ClusterOperationDomainStore>,
    ) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            Ok(())
        })?;
        Ok(Self { db, replicated })
    }

    /// Returns the replicated operation ledger handle used by global metadata sync.
    pub fn replicated_domain_store(&self) -> Option<ClusterOperationDomainStore> {
        self.replicated.clone()
    }

    /// Rebuilds the replicated operation ledger MST from durable rows, when enabled.
    pub async fn rebuild_replicated_mst(&self) -> io::Result<()> {
        let Some(replicated) = self.replicated.as_ref() else {
            return Ok(());
        };
        replicated
            .rebuild_mst_from_disk()
            .await
            .map_err(io::Error::other)
    }

    /// Persists a serialized operation payload for the provided operation identifier.
    pub fn put(&self, id: Uuid, payload: &[u8]) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            table.insert(*id.as_bytes(), payload).map_err(into_io)?;
            Ok(())
        })
    }

    /// Persists an operation record locally and mirrors non-dry-run rows into the replicated ledger.
    pub async fn put_record(
        &self,
        operation: &ClusterOperationRecord,
        payload: &[u8],
    ) -> io::Result<()> {
        self.put(operation.id, payload)?;
        if !operation.dry_run
            && let Some(replicated) = self.replicated.as_ref()
        {
            replicated
                .upsert(&UuidKey::from(operation.id), operation.clone())
                .await
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    /// Loads a serialized operation payload by identifier, if present.
    pub fn get(&self, id: Uuid) -> io::Result<Option<Vec<u8>>> {
        let local = with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            let value = table
                .get(*id.as_bytes())
                .map_err(into_io)?
                .map(|guard| guard.value().to_vec());
            Ok(value)
        })?;
        let replicated = self.replicated_record(id)?;
        Ok(select_encoded_operation(local, replicated))
    }

    /// Lists all serialized operation payloads currently present in the store.
    pub fn list(&self) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        let local = with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_CLUSTER_OPERATIONS).map_err(into_io)?;
            let mut out = Vec::new();

            for entry in table.iter().map_err(into_io)? {
                let (key, value) = entry.map_err(into_io)?;
                out.push((Uuid::from_bytes(key.value()), value.value().to_vec()));
            }

            Ok(out)
        })?;
        self.merge_replicated_records(local)
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

    /// Loads the winning replicated row for one operation id, if the mirror is enabled.
    fn replicated_record(&self, id: Uuid) -> io::Result<Option<ClusterOperationRecord>> {
        let Some(replicated) = self.replicated.as_ref() else {
            return Ok(None);
        };
        let snapshot = replicated
            .get_snapshot(&UuidKey::from(id))
            .map_err(io::Error::other)?;
        Ok(snapshot.and_then(select_replicated_operation))
    }

    /// Merges local operation rows with replicated winners for callers that scan all operations.
    fn merge_replicated_records(
        &self,
        local: Vec<(Uuid, Vec<u8>)>,
    ) -> io::Result<Vec<(Uuid, Vec<u8>)>> {
        let Some(replicated) = self.replicated.as_ref() else {
            return Ok(local);
        };

        let mut rows = HashMap::<Uuid, Vec<u8>>::new();
        for (id, payload) in local {
            rows.insert(id, payload);
        }

        let (snapshots, _) = replicated.load_all().map_err(io::Error::other)?;
        for (key, snapshot) in snapshots {
            let id = key.to_uuid();
            let Some(replicated_operation) = select_replicated_operation(snapshot) else {
                continue;
            };
            if let Some(selected) =
                select_encoded_operation(rows.remove(&id), Some(replicated_operation))
            {
                rows.insert(id, selected);
            }
        }

        Ok(rows.into_iter().collect())
    }
}

/// Selects the winning replicated operation row from a merged MV-register snapshot.
fn select_replicated_operation(
    snapshot: MvRegSnapshot<ClusterOperationRecord>,
) -> Option<ClusterOperationRecord> {
    snapshot.as_slice().iter().cloned().max_by(|left, right| {
        left.stage
            .rank()
            .cmp(&right.stage.rank())
            .then(left.updated_at_unix_ms.cmp(&right.updated_at_unix_ms))
            .then(left.id.cmp(&right.id))
            .then(left.details.cmp(&right.details))
    })
}

/// Selects the encoded operation row that should be visible for one operation id.
fn select_encoded_operation(
    local: Option<Vec<u8>>,
    replicated: Option<ClusterOperationRecord>,
) -> Option<Vec<u8>> {
    match (local, replicated) {
        (Some(local), Some(replicated)) => {
            let Ok(local_record) = ClusterOperationRecord::decode_capnp(&local) else {
                return Some(local);
            };
            if replicated.supersedes(&local_record) {
                replicated.encode_capnp().ok()
            } else {
                Some(local)
            }
        }
        (Some(local), None) => Some(local),
        (None, Some(replicated)) => replicated.encode_capnp().ok(),
        (None, None) => None,
    }
}
