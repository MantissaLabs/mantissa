use crate::secrets::types::SecretValue;
use crate::store::replicated::open::open_arc_store;
use mantissa_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::MvRegEntry;
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::sync::Arc;
use uuid::Uuid;

pub struct SecretTables;

impl TableSet for SecretTables {
    const VALUES: &'static str = "secret_values";
    const TOMBS: &'static str = "secret_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "secret_tombs_by_observed";
    const META: &'static str = "secret_meta";
}

/// Secret compaction ranker that preserves the registry's current canonical snapshot order.
pub struct SecretCompactionRank;

impl MvRegCompactionRanker<SecretValue, Uuid> for SecretCompactionRank {
    type Rank = SecretValue;

    /// Ranks one secret by its full deterministic value ordering.
    fn rank(entry: &MvRegEntry<SecretValue, Uuid>) -> Self::Rank {
        entry.value().clone()
    }
}

/// Store adapter for secret registers with domain-aware compaction enabled.
pub type SecretRegAdapter =
    CompactingStoreMvRegAdapterSorted<UuidKey, SecretValue, Uuid, SecretCompactionRank>;

pub type SecretStoreInner = CrdtMstStore<SecretRegAdapter, XXHash128, SecretTables>;

pub type SecretStore = Arc<SecretStoreInner>;

/// Opens the replicated secret store backed by Redb.
pub fn open_secret_store(db: Arc<redb::Database>, actor: Uuid) -> std::io::Result<SecretStore> {
    open_arc_store(db, actor, |db, actor| {
        SecretStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}
