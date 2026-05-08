use crate::cluster::ClusterViewId;
use crate::secrets::master_key::envelope::{
    MASTER_KEY_SIZE, MasterKeyDescriptor, MasterKeyTransfer, read_master_key_descriptor,
    write_master_key_descriptor,
};
use crate::store::replicated::open::open_arc_store;
use capnp::Error;
use mantissa_protocol::secrets::{
    secret_master_key_current, secret_master_key_grant, secret_master_key_sync_record,
};
use mantissa_store::adapter::{CompactingStoreMvRegAdapterSorted, MvRegCompactionRanker};
use mantissa_store::codec::StoreValueCodec;
use mantissa_store::hash::XXHash128;
use mantissa_store::mst_store::CrdtMstStore;
use mantissa_store::mvreg::{MvRegEntry, MvRegSnapshot};
use mantissa_store::table_set::TableSet;
use mantissa_store::uuid_key::UuidKey;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use uuid::Uuid;

/// Domain separator for deterministic descriptor row ids.
const DESCRIPTOR_ROW_ID_PREFIX: &[u8] = b"mantissa.secret-master.descriptor.v1";
/// Domain separator for deterministic per-recipient grant row ids.
const GRANT_ROW_ID_PREFIX: &[u8] = b"mantissa.secret-master.grant.v1";
/// Domain separator for deterministic per-scope current pointer row ids.
const CURRENT_ROW_ID_PREFIX: &[u8] = b"mantissa.secret-master.current.v1";

/// Replicated encrypted grant addressed to one recipient node.
pub type SecretMasterKeyGrant = MasterKeyTransfer;

/// Replicated current-key pointer for one cluster-view scope.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SecretMasterKeyCurrent {
    pub scope_view: ClusterViewId,
    pub key_id: Uuid,
    pub generation: u64,
    pub created_by_operation_id: Option<Uuid>,
    pub parent_key_ids: Vec<Uuid>,
}

/// One row stored in the replicated secret-master-key sync domain.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SecretMasterKeySyncRecord {
    Descriptor(MasterKeyDescriptor),
    Grant(SecretMasterKeyGrant),
    Current(SecretMasterKeyCurrent),
}

/// Replicated master-key grant domain tables.
pub struct SecretMasterKeyTables;

impl TableSet for SecretMasterKeyTables {
    const VALUES: &'static str = "secret_master_key_values";
    const TOMBS: &'static str = "secret_master_key_tombs";
    const TOMBS_BY_OBSERVED: &'static str = "secret_master_key_tombs_by_observed";
    const META: &'static str = "secret_master_key_meta";
}

/// Compaction ranker for master-key sync rows.
///
/// Descriptor and grant rows should normally be single-writer facts. Current
/// rows can legitimately have concurrent siblings, so their rank mirrors the
/// deterministic current selection rule used by `current_for_scope`.
pub struct SecretMasterKeyCompactionRank;

impl MvRegCompactionRanker<SecretMasterKeySyncRecord, Uuid> for SecretMasterKeyCompactionRank {
    type Rank = (u8, u8, u64, Uuid, SecretMasterKeySyncRecord);

    /// Ranks one visible master-key row for deterministic MVReg compaction.
    fn rank(entry: &MvRegEntry<SecretMasterKeySyncRecord, Uuid>) -> Self::Rank {
        match entry.value() {
            SecretMasterKeySyncRecord::Descriptor(descriptor) => (
                0,
                0,
                descriptor.generation,
                descriptor.key_id,
                entry.value().clone(),
            ),
            SecretMasterKeySyncRecord::Grant(grant) => (
                1,
                0,
                grant.descriptor.generation,
                grant.descriptor.key_id,
                entry.value().clone(),
            ),
            SecretMasterKeySyncRecord::Current(current) => (
                2,
                u8::from(current.created_by_operation_id.is_some()),
                current.generation,
                current.key_id,
                entry.value().clone(),
            ),
        }
    }
}

/// Store adapter for replicated secret-master-key rows.
pub type SecretMasterKeyRegAdapter = CompactingStoreMvRegAdapterSorted<
    UuidKey,
    SecretMasterKeySyncRecord,
    Uuid,
    SecretMasterKeyCompactionRank,
>;

/// MST-backed CRDT store for replicated master-key descriptors, grants, and current pointers.
pub type SecretMasterKeyStoreInner =
    CrdtMstStore<SecretMasterKeyRegAdapter, XXHash128, SecretMasterKeyTables>;

/// Shared replicated secret-master-key store handle.
pub type SecretMasterKeyStore = Arc<SecretMasterKeyStoreInner>;

/// Opens the replicated secret-master-key store backed by Redb.
pub fn open_secret_master_key_store(
    db: Arc<redb::Database>,
    actor: Uuid,
) -> io::Result<SecretMasterKeyStore> {
    open_arc_store(db, actor, |db, actor| {
        SecretMasterKeyStoreInner::builder(db, actor)
            .with_preserve_local_tombs(true)
            .build()
    })
}

/// Upserts one master-key descriptor row using its deterministic row id.
pub async fn upsert_descriptor(
    store: &SecretMasterKeyStoreInner,
    descriptor: MasterKeyDescriptor,
) -> mantissa_store::Result<()> {
    store
        .upsert(
            &UuidKey::from(descriptor_row_id(descriptor.key_id)),
            SecretMasterKeySyncRecord::Descriptor(descriptor),
        )
        .await
}

/// Upserts one recipient grant row using its deterministic row id.
pub async fn upsert_grant(
    store: &SecretMasterKeyStoreInner,
    grant: SecretMasterKeyGrant,
) -> mantissa_store::Result<()> {
    store
        .upsert(
            &UuidKey::from(grant_row_id(
                grant.descriptor.key_id,
                grant.recipient_node_id,
            )),
            SecretMasterKeySyncRecord::Grant(grant),
        )
        .await
}

/// Upserts one current-key pointer row using its deterministic scope row id.
pub async fn upsert_current(
    store: &SecretMasterKeyStoreInner,
    current: SecretMasterKeyCurrent,
) -> mantissa_store::Result<()> {
    store
        .upsert(
            &UuidKey::from(current_row_id(current.scope_view)),
            SecretMasterKeySyncRecord::Current(current),
        )
        .await
}

/// Upserts one replicated master-key row using the row id implied by its variant.
pub async fn upsert_record(
    store: &SecretMasterKeyStoreInner,
    record: SecretMasterKeySyncRecord,
) -> mantissa_store::Result<()> {
    match record {
        SecretMasterKeySyncRecord::Descriptor(descriptor) => {
            upsert_descriptor(store, descriptor).await
        }
        SecretMasterKeySyncRecord::Grant(grant) => upsert_grant(store, grant).await,
        SecretMasterKeySyncRecord::Current(current) => upsert_current(store, current).await,
    }
}

/// Reads the deterministic current-key winner for one scope, if any row exists.
pub fn current_for_scope(
    store: &SecretMasterKeyStoreInner,
    scope_view: ClusterViewId,
) -> mantissa_store::Result<Option<SecretMasterKeyCurrent>> {
    let snapshot = match store.get_snapshot(&UuidKey::from(current_row_id(scope_view)))? {
        Some(snapshot) => snapshot,
        None => return Ok(None),
    };
    Ok(current_from_snapshot(&snapshot))
}

/// Builds the replicated current pointer that corresponds to one descriptor.
pub fn current_from_descriptor(descriptor: &MasterKeyDescriptor) -> SecretMasterKeyCurrent {
    SecretMasterKeyCurrent {
        scope_view: descriptor.scope_view,
        key_id: descriptor.key_id,
        generation: descriptor.generation,
        created_by_operation_id: descriptor.created_by_operation_id,
        parent_key_ids: descriptor.parent_key_ids.clone(),
    }
}

impl StoreValueCodec for SecretMasterKeySyncRecord {
    /// Encodes one replicated master-key row as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut builder = message.init_root::<secret_master_key_sync_record::Builder<'_>>();
            write_secret_master_key_sync_record(builder.reborrow(), self);
        }
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Decodes one replicated master-key row from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(secret_master_key_store_codec_error)?;
        let record = reader
            .get_root::<secret_master_key_sync_record::Reader<'_>>()
            .map_err(secret_master_key_store_codec_error)?;
        read_secret_master_key_sync_record(record).map_err(secret_master_key_store_codec_error)
    }
}

/// Builds the deterministic descriptor row id for one master-key id.
pub fn descriptor_row_id(key_id: Uuid) -> Uuid {
    row_uuid(DESCRIPTOR_ROW_ID_PREFIX, [key_id.as_bytes().as_slice()])
}

/// Builds the deterministic grant row id for one key and recipient.
pub fn grant_row_id(key_id: Uuid, recipient_node_id: Uuid) -> Uuid {
    row_uuid(
        GRANT_ROW_ID_PREFIX,
        [
            key_id.as_bytes().as_slice(),
            recipient_node_id.as_bytes().as_slice(),
        ],
    )
}

/// Builds the deterministic current pointer row id for one key scope.
pub fn current_row_id(scope_view: ClusterViewId) -> Uuid {
    let mut epoch = [0u8; 8];
    epoch.copy_from_slice(&scope_view.epoch.to_be_bytes());
    row_uuid(
        CURRENT_ROW_ID_PREFIX,
        [
            scope_view.cluster_id.as_bytes().as_slice(),
            epoch.as_slice(),
        ],
    )
}

/// Selects one current row from a visible MVReg snapshot.
fn current_from_snapshot(
    snapshot: &MvRegSnapshot<SecretMasterKeySyncRecord>,
) -> Option<SecretMasterKeyCurrent> {
    snapshot
        .as_slice()
        .iter()
        .filter_map(|record| match record {
            SecretMasterKeySyncRecord::Current(current) => Some(current),
            _ => None,
        })
        .fold(None, |winner, candidate| match winner {
            Some(winner) if !current_supersedes(candidate, &winner) => Some(winner),
            _ => Some(candidate.clone()),
        })
}

/// Returns true when `candidate` should be the active current row over `current`.
fn current_supersedes(
    candidate: &SecretMasterKeyCurrent,
    current: &SecretMasterKeyCurrent,
) -> bool {
    // First honor explicit ancestry. Split/merge currents are operation-created,
    // but a later normal rotation descended from them must still advance the
    // scope current instead of being pinned behind the operation row forever.
    if current_rows_share_lineage(candidate, current) && candidate.generation != current.generation
    {
        return candidate.generation > current.generation;
    }

    match (
        candidate.created_by_operation_id.is_some(),
        current.created_by_operation_id.is_some(),
    ) {
        (true, false) => return true,
        (false, true) => return false,
        _ => {}
    }

    (candidate.generation, candidate.key_id) > (current.generation, current.key_id)
}

/// Checks the lineage information available directly on two current rows.
///
/// This is intentionally local to the current row. Full ancestry walks need
/// descriptor lookup and belong in the reconciler; the store only needs a
/// deterministic winner for concurrent visible current-pointer values.
fn current_rows_share_lineage(
    left: &SecretMasterKeyCurrent,
    right: &SecretMasterKeyCurrent,
) -> bool {
    left.key_id == right.key_id
        || left.parent_key_ids.contains(&right.key_id)
        || right.parent_key_ids.contains(&left.key_id)
        || left
            .parent_key_ids
            .iter()
            .any(|parent| right.parent_key_ids.contains(parent))
}

/// Encodes one replicated master-key row into the wire/store schema.
pub(crate) fn write_secret_master_key_sync_record(
    mut builder: secret_master_key_sync_record::Builder<'_>,
    record: &SecretMasterKeySyncRecord,
) {
    match record {
        SecretMasterKeySyncRecord::Descriptor(descriptor) => {
            write_master_key_descriptor(builder.reborrow().init_descriptor(), descriptor);
        }
        SecretMasterKeySyncRecord::Grant(grant) => {
            write_secret_master_key_grant(builder.reborrow().init_grant(), grant);
        }
        SecretMasterKeySyncRecord::Current(current) => {
            write_secret_master_key_current(builder.reborrow().init_current(), current);
        }
    }
}

/// Decodes one replicated master-key row from the wire/store schema.
pub(crate) fn read_secret_master_key_sync_record(
    reader: secret_master_key_sync_record::Reader<'_>,
) -> Result<SecretMasterKeySyncRecord, Error> {
    match reader.which()? {
        secret_master_key_sync_record::Which::Descriptor(Ok(descriptor)) => {
            Ok(SecretMasterKeySyncRecord::Descriptor(
                read_master_key_descriptor(descriptor)
                    .map_err(|error| Error::failed(error.to_string()))?,
            ))
        }
        secret_master_key_sync_record::Which::Descriptor(Err(error)) => Err(error),
        secret_master_key_sync_record::Which::Grant(Ok(grant)) => Ok(
            SecretMasterKeySyncRecord::Grant(read_secret_master_key_grant(grant)?),
        ),
        secret_master_key_sync_record::Which::Grant(Err(error)) => Err(error),
        secret_master_key_sync_record::Which::Current(Ok(current)) => Ok(
            SecretMasterKeySyncRecord::Current(read_secret_master_key_current(current)?),
        ),
        secret_master_key_sync_record::Which::Current(Err(error)) => Err(error),
    }
}

/// Encodes one recipient-specific encrypted grant.
fn write_secret_master_key_grant(
    mut builder: secret_master_key_grant::Builder<'_>,
    grant: &SecretMasterKeyGrant,
) {
    write_master_key_descriptor(builder.reborrow().init_descriptor(), &grant.descriptor);
    builder.set_sender_node_id(grant.sender_node_id.as_bytes());
    builder.set_recipient_node_id(grant.recipient_node_id.as_bytes());
    builder.set_transfer_public_key(&grant.transfer_public_key);
    builder.set_recipient_noise_static_pub(&grant.recipient_noise_static_pub);
    builder.set_nonce(&grant.nonce);
    builder.set_ciphertext(&grant.ciphertext);
    builder.set_sender_noise_static_pub(&grant.sender_noise_static_pub);
}

/// Decodes one recipient-specific encrypted grant.
fn read_secret_master_key_grant(
    reader: secret_master_key_grant::Reader<'_>,
) -> Result<SecretMasterKeyGrant, Error> {
    Ok(MasterKeyTransfer {
        descriptor: read_master_key_descriptor(reader.get_descriptor()?)
            .map_err(|error| Error::failed(error.to_string()))?,
        sender_node_id: read_uuid(reader.get_sender_node_id()?)?,
        recipient_node_id: read_uuid(reader.get_recipient_node_id()?)?,
        transfer_public_key: read_fixed_data::<MASTER_KEY_SIZE>(
            reader.get_transfer_public_key()?,
            "master key grant transfer public key",
        )?,
        recipient_noise_static_pub: read_fixed_data::<MASTER_KEY_SIZE>(
            reader.get_recipient_noise_static_pub()?,
            "master key grant recipient noise key",
        )?,
        nonce: read_fixed_data::<24>(reader.get_nonce()?, "master key grant nonce")?,
        ciphertext: reader.get_ciphertext()?.to_vec(),
        sender_noise_static_pub: read_fixed_data::<MASTER_KEY_SIZE>(
            reader.get_sender_noise_static_pub()?,
            "master key grant sender noise key",
        )?,
    })
}

/// Encodes one current-key pointer row.
fn write_secret_master_key_current(
    mut builder: secret_master_key_current::Builder<'_>,
    current: &SecretMasterKeyCurrent,
) {
    current
        .scope_view
        .write_capnp(builder.reborrow().init_scope_view());
    builder.set_key_id(current.key_id.as_bytes());
    builder.set_generation(current.generation);
    if let Some(operation_id) = current.created_by_operation_id {
        builder.set_created_by_operation_id(operation_id.as_bytes());
    } else {
        builder.set_created_by_operation_id(&[]);
    }
    let mut parents = builder
        .reborrow()
        .init_parent_key_ids(current.parent_key_ids.len() as u32);
    for (idx, parent) in current.parent_key_ids.iter().enumerate() {
        parents.set(idx as u32, parent.as_bytes());
    }
}

/// Decodes one current-key pointer row.
fn read_secret_master_key_current(
    reader: secret_master_key_current::Reader<'_>,
) -> Result<SecretMasterKeyCurrent, Error> {
    let operation_id = {
        let raw = reader.get_created_by_operation_id()?;
        if raw.is_empty() {
            None
        } else {
            Some(read_uuid(raw)?)
        }
    };
    let parent_reader = reader.get_parent_key_ids()?;
    let mut parent_key_ids = Vec::with_capacity(parent_reader.len() as usize);
    for parent in parent_reader.iter() {
        parent_key_ids.push(read_uuid(parent?)?);
    }

    Ok(SecretMasterKeyCurrent {
        scope_view: ClusterViewId::from_capnp(reader.get_scope_view()?).map_err(Error::failed)?,
        key_id: read_uuid(reader.get_key_id()?)?,
        generation: reader.get_generation(),
        created_by_operation_id: operation_id,
        parent_key_ids,
    })
}

/// Derives a deterministic row UUID from a domain separator and fixed binary parts.
fn row_uuid<const N: usize>(prefix: &[u8], parts: [&[u8]; N]) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix);
    for part in parts {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Reads a required UUID from a Cap'n Proto data field.
fn read_uuid(data: capnp::data::Reader<'_>) -> Result<Uuid, Error> {
    let bytes = read_fixed_data::<16>(data, "uuid")?;
    Ok(Uuid::from_bytes(bytes))
}

/// Reads a fixed-width data field with a field-specific error.
fn read_fixed_data<const N: usize>(
    data: capnp::data::Reader<'_>,
    field: &str,
) -> Result<[u8; N], Error> {
    if data.len() != N {
        return Err(Error::failed(format!(
            "invalid {field}: expected {N} bytes, got {}",
            data.len()
        )));
    }
    let mut out = [0u8; N];
    out.copy_from_slice(data);
    Ok(out)
}

/// Converts master-key store-codec errors into the CRDT store error type.
fn secret_master_key_store_codec_error<E: std::fmt::Display>(
    error: E,
) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "secret master key store codec error: {error}"
    )))
}

#[cfg(test)]
mod tests {
    use super::{
        SecretMasterKeyCurrent, SecretMasterKeyGrant, SecretMasterKeySyncRecord, current_for_scope,
        current_row_id, current_supersedes, descriptor_row_id, grant_row_id,
        open_secret_master_key_store, upsert_current, upsert_descriptor, upsert_grant,
    };
    use crate::cluster::ClusterViewId;
    use crate::secrets::master_key::envelope::{MASTER_KEY_SIZE, MasterKeyDescriptor};
    use mantissa_store::codec::StoreValueCodec;
    use mantissa_store::uuid_key::UuidKey;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Builds one deterministic descriptor for replicated-store tests.
    fn descriptor(key_id: Uuid, generation: u64) -> MasterKeyDescriptor {
        MasterKeyDescriptor {
            key_id,
            generation,
            scope_view: ClusterViewId::legacy_default(),
            origin_view: ClusterViewId::legacy_default(),
            created_by_node_id: Uuid::from_u128(10),
            created_by_operation_id: None,
            parent_key_ids: Vec::new(),
            created_at_unix_secs: 42,
        }
    }

    /// Builds one deterministic encrypted grant payload for codec tests.
    fn grant(key_id: Uuid, recipient_node_id: Uuid) -> SecretMasterKeyGrant {
        SecretMasterKeyGrant {
            descriptor: descriptor(key_id, 1),
            sender_node_id: Uuid::from_u128(11),
            recipient_node_id,
            sender_noise_static_pub: [1u8; MASTER_KEY_SIZE],
            transfer_public_key: [2u8; MASTER_KEY_SIZE],
            recipient_noise_static_pub: [3u8; MASTER_KEY_SIZE],
            nonce: [4u8; 24],
            ciphertext: vec![5u8; 48],
        }
    }

    /// Builds one deterministic current pointer for selection tests.
    fn current(key_id: Uuid, generation: u64) -> SecretMasterKeyCurrent {
        SecretMasterKeyCurrent {
            scope_view: ClusterViewId::legacy_default(),
            key_id,
            generation,
            created_by_operation_id: None,
            parent_key_ids: Vec::new(),
        }
    }

    /// Opens a temporary replicated master-key store.
    async fn temp_store(actor: Uuid) -> (tempfile::TempDir, super::SecretMasterKeyStore) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("secret-master-keys.redb");
        let db = Arc::new(redb::Database::create(path).expect("create redb"));
        let store = open_secret_master_key_store(db, actor).expect("open store");
        store.rebuild_mst_from_disk().await.expect("rebuild store");
        (dir, store)
    }

    /// Store codec should roundtrip every master-key sync row kind.
    #[test]
    fn store_value_codec_roundtrips_master_key_rows() {
        let key_id = Uuid::from_u128(100);
        let rows = vec![
            SecretMasterKeySyncRecord::Descriptor(descriptor(key_id, 1)),
            SecretMasterKeySyncRecord::Grant(grant(key_id, Uuid::from_u128(101))),
            SecretMasterKeySyncRecord::Current(current(key_id, 1)),
        ];

        for row in rows {
            let encoded = row.encode_store_value().expect("encode row");
            let decoded =
                SecretMasterKeySyncRecord::decode_store_value(&encoded).expect("decode row");
            assert_eq!(decoded, row);
        }
    }

    /// Deterministic row ids must be stable and separated by row kind.
    #[test]
    fn row_ids_are_stable_and_distinct() {
        let key_id = Uuid::from_u128(200);
        let recipient = Uuid::from_u128(201);
        let view = ClusterViewId::legacy_default();

        assert_eq!(descriptor_row_id(key_id), descriptor_row_id(key_id));
        assert_eq!(
            grant_row_id(key_id, recipient),
            grant_row_id(key_id, recipient)
        );
        assert_eq!(current_row_id(view), current_row_id(view));
        assert_ne!(descriptor_row_id(key_id), grant_row_id(key_id, recipient));
        assert_ne!(descriptor_row_id(key_id), current_row_id(view));
        assert_ne!(grant_row_id(key_id, recipient), current_row_id(view));
    }

    /// Current row precedence should be deterministic across insertion orders.
    #[test]
    fn current_row_conflict_policy_is_deterministic() {
        let parent = Uuid::from_u128(300);
        let left_id = Uuid::from_u128(301);
        let right_id = Uuid::from_u128(302);
        let mut left = current(left_id, 2);
        left.parent_key_ids = vec![parent];
        let mut right = current(right_id, 2);
        right.parent_key_ids = vec![parent];

        assert!(current_supersedes(&right, &left));
        assert!(!current_supersedes(&left, &right));

        let mut merge = current(Uuid::from_u128(303), 1);
        merge.created_by_operation_id = Some(Uuid::from_u128(304));
        assert!(current_supersedes(&merge, &right));
    }

    /// A normal rotation descended from a transition-created key must become current.
    #[test]
    fn descendant_rotation_supersedes_operation_current() {
        let operation_key = Uuid::from_u128(310);
        let rotated_key = Uuid::from_u128(311);
        let mut operation_current = current(operation_key, 2);
        operation_current.created_by_operation_id = Some(Uuid::from_u128(312));
        let mut rotated_current = current(rotated_key, 3);
        rotated_current.parent_key_ids = vec![operation_key];

        assert!(current_supersedes(&rotated_current, &operation_current));
        assert!(!current_supersedes(&operation_current, &rotated_current));
    }

    /// Replicated master-key rows should converge through normal MST delta exchange.
    #[tokio::test]
    async fn master_key_store_roots_converge_after_delta() {
        let (_left_dir, left) = temp_store(Uuid::from_u128(401)).await;
        let (_right_dir, right) = temp_store(Uuid::from_u128(402)).await;
        let key_id = Uuid::from_u128(403);
        let recipient = Uuid::from_u128(404);
        let current = current(key_id, 1);

        upsert_descriptor(&left, descriptor(key_id, 1))
            .await
            .expect("upsert descriptor");
        upsert_grant(&left, grant(key_id, recipient))
            .await
            .expect("upsert grant");
        upsert_current(&left, current.clone())
            .await
            .expect("upsert current");

        let ranges = left.page_range_summary().await.expect("left ranges");
        let (registers, tombstones) = left
            .export_page_ranges_delta(&ranges)
            .expect("export left delta");
        right
            .apply_delta_chunk_update_mst(registers, tombstones)
            .await
            .expect("apply delta");

        assert_eq!(left.root_digest().await, right.root_digest().await);
        assert_eq!(
            current_for_scope(&right, ClusterViewId::legacy_default()).expect("read current"),
            Some(current)
        );
        let grant_snapshot = right
            .get_snapshot(&UuidKey::from(grant_row_id(key_id, recipient)))
            .expect("read grant snapshot")
            .expect("grant should exist");
        assert_eq!(grant_snapshot.as_slice().len(), 1);
    }
}
