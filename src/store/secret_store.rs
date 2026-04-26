use crate::secrets::types::SecretValue;
use crate::store::open::open_arc_store;
use crdt_store::adapter::StoreMvRegAdapterSorted;
use crdt_store::hash::XXHash128;
use crdt_store::mst_store::CrdtMstStore;
use crdt_store::table_set::TableSet;
use crdt_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

pub struct SecretTables;

impl TableSet for SecretTables {
    const VALUES: &'static str = "secret_values";
    const TOMBS: &'static str = "secret_tombs";
    const META: &'static str = "secret_meta";
}

pub type SecretStoreInner =
    CrdtMstStore<StoreMvRegAdapterSorted<UuidKey, SecretValue, Uuid>, XXHash128, SecretTables>;

pub type SecretStore = Arc<SecretStoreInner>;

/// Opens the replicated secret store backed by Redb.
pub fn open_secret_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<SecretStore> {
    open_arc_store(db, actor, |db, actor| {
        SecretStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}
