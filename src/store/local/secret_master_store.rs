use crate::cluster::ClusterViewId;
use crate::secrets::master_key_protector::{
    MasterKeyDescriptor, MasterKeyPlaintext, MasterKeyProtectorHandle, WrappedMasterKeyRecord,
};
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use parking_lot::{Mutex, MutexGuard};
use redb::{Database, TableDefinition};
use std::cmp::Ordering;
use std::{io, sync::Arc};
use uuid::Uuid;

const T_MASTER_KEY_ENVELOPES: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("secret_master_key_envelopes");
const T_MASTER_META: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("secret_master_meta");
/// Metadata key pointing at the locally active master-key id.
const CURRENT_KEY_ID_KEY: &str = "current_key_id";
/// Metadata key marking whether the initial key is still a replaceable join bootstrap key.
const BOOTSTRAP_PENDING_KEY: &str = "bootstrap_pending";
/// Compact boolean marker for Redb metadata values.
const META_TRUE: &[u8] = b"1";
/// Compact boolean marker for Redb metadata values.
const META_FALSE: &[u8] = b"0";

/// Immutable plaintext snapshot of one master key after local envelope unwrap.
#[derive(Debug, Clone)]
pub struct MasterKeyRecord {
    pub descriptor: MasterKeyDescriptor,
    pub key: MasterKeyPlaintext,
}

impl MasterKeyRecord {
    /// Creates a record from raw parts after validating required descriptor fields.
    pub fn new(descriptor: MasterKeyDescriptor, key: MasterKeyPlaintext) -> io::Result<Self> {
        if descriptor.generation == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secret master key generation must be positive",
            ));
        }
        Ok(Self { descriptor, key })
    }

    /// Returns the globally unique master-key id.
    pub fn key_id(&self) -> Uuid {
        self.descriptor.key_id
    }

    /// Returns the display generation carried by this key descriptor.
    pub fn generation(&self) -> u64 {
        self.descriptor.generation
    }
}

/// Durable storage for locally wrapped cluster secret master keys.
#[derive(Clone)]
pub struct SecretMasterStore {
    db: Arc<Database>,
    protector: MasterKeyProtectorHandle,
    // Serializes the local cluster-key decision. A fresh node starts with a
    // temporary key so secrets can work before it joins. The first cluster
    // action must choose one outcome: adopt an anchor key, serve its own key as
    // an anchor, or rotate. Without this lock, node 1 could serve temporary key
    // A to node 3 while concurrently joining node 2 and adopting key B.
    policy_lock: Arc<Mutex<()>>,
}

impl SecretMasterStore {
    /// Opens or creates the wrapped secret master key tables.
    pub fn new(db: Arc<Database>, protector: MasterKeyProtectorHandle) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            let _ = tx.open_table(T_MASTER_META).map_err(into_io)?;
            Ok(())
        })?;

        Ok(Self {
            db,
            protector,
            policy_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Ensures a current wrapped master key exists using legacy default node metadata.
    pub fn ensure_current(&self) -> io::Result<MasterKeyRecord> {
        self.ensure_current_for_node(ClusterViewId::legacy_default(), Uuid::nil())
    }

    /// Ensures a current wrapped master key exists, generating one for the local node when missing.
    pub fn ensure_current_for_node(
        &self,
        scope_view: ClusterViewId,
        created_by_node_id: Uuid,
    ) -> io::Result<MasterKeyRecord> {
        let _guard = self.policy_guard();
        if let Some(existing) = self.load_current()? {
            return Ok(existing);
        }

        let generated = MasterKeyPlaintext::generate()?;
        let descriptor = MasterKeyDescriptor::initial(scope_view, created_by_node_id)?;
        self.persist_record(&descriptor, &generated, true, Some(true))?;
        MasterKeyRecord::new(descriptor, generated)
    }

    /// Returns the currently active master key record.
    pub fn current(&self) -> io::Result<MasterKeyRecord> {
        self.load_current()?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "secret master key missing"))
    }

    /// Fetches a master key by globally unique key id.
    pub fn load_key(&self, key_id: Uuid) -> io::Result<Option<MasterKeyRecord>> {
        let Some(wrapped) = self.load_wrapped_key(key_id)? else {
            return Ok(None);
        };
        let key = self.protector.unwrap(&wrapped)?;
        MasterKeyRecord::new(wrapped.descriptor, key).map(Some)
    }

    /// Returns true when a wrapped envelope exists locally for `key_id`.
    pub fn contains_key(&self, key_id: Uuid) -> io::Result<bool> {
        self.load_wrapped_key(key_id).map(|record| record.is_some())
    }

    /// Imports an externally provided key without changing the local current pointer.
    ///
    /// The reconciler uses this for historical grants so ciphertext encrypted
    /// under older keys stays readable while replicated current metadata
    /// remains the only authority for current-key selection.
    pub fn import_key(&self, record: &MasterKeyRecord) -> io::Result<()> {
        let _guard = self.policy_guard();
        self.persist_record(&record.descriptor, &record.key, false, None)
    }

    /// Imports an externally provided key and makes it the active key.
    pub fn import_current(&self, record: &MasterKeyRecord) -> io::Result<()> {
        self.import_current_with_policy(record, false)
    }

    /// Activates a key selected by replicated current-key metadata.
    ///
    /// This intentionally does not apply direct-transfer generation policy:
    /// once the replicated `current` row has converged, it is the source of
    /// truth. The key must still match any local envelope already stored for
    /// the same key id.
    pub fn activate_current(&self, record: &MasterKeyRecord) -> io::Result<()> {
        let _guard = self.policy_guard();
        self.persist_record(&record.descriptor, &record.key, true, Some(false))
    }

    /// Imports the anchor key during join without reopening the transfer/adoption race.
    ///
    /// A fresh node may either adopt an anchor key or serve its bootstrap key
    /// to another joiner. Once `commit_current_for_transfer` has served that
    /// bootstrap key as cluster-forming material, this join path must reject a
    /// different anchor key. Otherwise node 1 could give key A to node 3 while
    /// concurrently joining node 2 and adopting key B for itself.
    pub fn import_join_current(&self, record: &MasterKeyRecord) -> io::Result<()> {
        self.import_current_with_policy(record, true)
    }

    /// Commits the cached current key as a cluster key before exporting it.
    ///
    /// Serving a transfer makes this node an anchor for the recipient. If the
    /// current key is still the local bootstrap key, that key stops being
    /// replaceable before the transfer is encrypted. This intentionally makes
    /// concurrent "join another anchor" attempts fail instead of allowing this
    /// node to give one peer key A and then silently switch itself to key B.
    pub fn commit_current_for_transfer(&self, expected_key_id: Uuid) -> io::Result<()> {
        let _guard = self.policy_guard();
        let current_key_id = self
            .current_key_id()?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "secret master key missing"))?;
        if current_key_id != expected_key_id {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "secret master key changed before transfer",
            ));
        }
        self.set_bootstrap_pending(false)?;
        Ok(())
    }

    /// Generates, persists, and activates a fresh key scoped to `scope_view`.
    pub fn rotate(
        &self,
        scope_view: ClusterViewId,
        created_by_node_id: Uuid,
    ) -> io::Result<MasterKeyRecord> {
        let _guard = self.policy_guard();
        let parent = match self.load_current()? {
            Some(parent) => parent,
            None => {
                let generated = MasterKeyPlaintext::generate()?;
                let descriptor = MasterKeyDescriptor::initial(scope_view, created_by_node_id)?;
                self.persist_record(&descriptor, &generated, true, Some(true))?;
                return MasterKeyRecord::new(descriptor, generated);
            }
        };

        let descriptor =
            MasterKeyDescriptor::child(&parent.descriptor, scope_view, created_by_node_id, None)?;
        let key = MasterKeyPlaintext::generate()?;
        self.persist_record(&descriptor, &key, true, Some(false))?;
        MasterKeyRecord::new(descriptor, key)
    }

    /// Locks local master-key policy decisions for this daemon instance.
    fn policy_guard(&self) -> MutexGuard<'_, ()> {
        self.policy_lock.lock()
    }

    /// Loads the currently active master key record if one has been persisted.
    fn load_current(&self) -> io::Result<Option<MasterKeyRecord>> {
        self.current_key_id()?
            .map(|key_id| {
                self.load_key(key_id)?.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "secret master key envelope missing for recorded key id",
                    )
                })
            })
            .transpose()
    }

    /// Imports an external master key while enforcing generation and replay policy.
    fn import_current_with_policy(
        &self,
        record: &MasterKeyRecord,
        allow_bootstrap_replacement: bool,
    ) -> io::Result<()> {
        let _guard = self.policy_guard();
        if let Some(current_key_id) = self.current_key_id()? {
            let current = self.load_key(current_key_id)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "secret master key envelope missing for recorded key id",
                )
            })?;

            match record.generation().cmp(&current.generation()) {
                Ordering::Less => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "stale secret master key transfer rejected",
                    ));
                }
                Ordering::Equal => {
                    if allow_bootstrap_replacement && self.bootstrap_replacement_pending()? {
                        self.persist_record(&record.descriptor, &record.key, true, Some(false))?;
                        if current_key_id != record.key_id() {
                            self.remove_key(current_key_id)?;
                        }
                        return Ok(());
                    }
                    if current.descriptor == record.descriptor && current.key == record.key {
                        self.set_bootstrap_pending(false)?;
                        return Ok(());
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "conflicting secret master key transfer rejected",
                    ));
                }
                Ordering::Greater => {}
            }
        }

        self.persist_record(&record.descriptor, &record.key, true, Some(false))?;
        Ok(())
    }

    /// Loads the locally active key id.
    fn current_key_id(&self) -> io::Result<Option<Uuid>> {
        with_read_tx(&self.db, |tx| {
            let meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            let Some(raw) = meta.get(CURRENT_KEY_ID_KEY).map_err(into_io)? else {
                return Ok(None);
            };
            uuid_from_bytes(raw.value()).map(Some)
        })
    }

    /// Returns true while the local key may be replaced by an authenticated join anchor key.
    fn bootstrap_replacement_pending(&self) -> io::Result<bool> {
        with_read_tx(&self.db, |tx| {
            let meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            let Some(raw) = meta.get(BOOTSTRAP_PENDING_KEY).map_err(into_io)? else {
                return Ok(None);
            };
            Ok(Some(raw.value() == META_TRUE))
        })
        .map(|marker| marker.unwrap_or(false))
    }

    /// Updates whether the current local key is still a replaceable bootstrap key.
    fn set_bootstrap_pending(&self, pending: bool) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            let value = if pending { META_TRUE } else { META_FALSE };
            meta.insert(BOOTSTRAP_PENDING_KEY, value).map_err(into_io)?;
            Ok(())
        })
    }

    /// Removes one non-current envelope after its bootstrap replacement succeeds.
    fn remove_key(&self, key_id: Uuid) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut envelopes = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            envelopes
                .remove(key_id.as_bytes().as_slice())
                .map_err(into_io)?;
            Ok(())
        })
    }

    /// Loads the wrapped envelope associated with `key_id` if one exists.
    fn load_wrapped_key(&self, key_id: Uuid) -> io::Result<Option<WrappedMasterKeyRecord>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            let Some(raw_envelope) = table.get(key_id.as_bytes().as_slice()).map_err(into_io)?
            else {
                return Ok(None);
            };
            WrappedMasterKeyRecord::decode(raw_envelope.value()).map(Some)
        })
    }

    /// Persists `key` as an envelope and optionally advances current/bootstrap metadata.
    fn persist_record(
        &self,
        descriptor: &MasterKeyDescriptor,
        key: &MasterKeyPlaintext,
        make_current: bool,
        bootstrap_pending: Option<bool>,
    ) -> io::Result<()> {
        if descriptor.generation == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secret master key generation must be positive",
            ));
        }

        if let Some(existing) = self.load_key(descriptor.key_id)? {
            if existing.descriptor == *descriptor && existing.key == *key {
                self.update_metadata(descriptor.key_id, make_current, bootstrap_pending)?;
                return Ok(());
            }
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "conflicting secret master key transfer rejected",
            ));
        }

        let wrapped = self.protector.wrap(descriptor.clone(), key)?;
        let encoded = wrapped.encode()?;

        with_write_tx(&self.db, |tx| {
            let mut envelopes = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            envelopes
                .insert(descriptor.key_id.as_bytes().as_slice(), encoded.as_slice())
                .map_err(into_io)?;

            let mut meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            if make_current {
                meta.insert(CURRENT_KEY_ID_KEY, descriptor.key_id.as_bytes().as_slice())
                    .map_err(into_io)?;
            }
            if let Some(bootstrap_pending) = bootstrap_pending {
                let bootstrap_value = if bootstrap_pending {
                    META_TRUE
                } else {
                    META_FALSE
                };
                meta.insert(BOOTSTRAP_PENDING_KEY, bootstrap_value)
                    .map_err(into_io)?;
            }
            Ok(())
        })
    }

    /// Updates metadata for an already stored key without rewriting the envelope.
    fn update_metadata(
        &self,
        key_id: Uuid,
        make_current: bool,
        bootstrap_pending: Option<bool>,
    ) -> io::Result<()> {
        with_write_tx(&self.db, |tx| {
            let mut meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            if make_current {
                meta.insert(CURRENT_KEY_ID_KEY, key_id.as_bytes().as_slice())
                    .map_err(into_io)?;
            }
            if let Some(bootstrap_pending) = bootstrap_pending {
                let value = if bootstrap_pending {
                    META_TRUE
                } else {
                    META_FALSE
                };
                meta.insert(BOOTSTRAP_PENDING_KEY, value).map_err(into_io)?;
            }
            Ok(())
        })
    }
}

/// Decodes a UUID from stored metadata bytes.
fn uuid_from_bytes(bytes: &[u8]) -> io::Result<Uuid> {
    if bytes.len() != 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "secret master key metadata corrupted",
        ));
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(bytes);
    Ok(Uuid::from_bytes(out))
}

#[cfg(test)]
mod tests {
    use super::{MasterKeyRecord, SecretMasterStore};
    use crate::cluster::ClusterViewId;
    use crate::secrets::master_key_protector::{
        MasterKeyCipherSuite, MasterKeyDescriptor, MasterKeyPlaintext, MasterKeyProtector,
        PassphraseMasterKeyProtector, WrappedMasterKeyRecord,
    };
    use redb::Database;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use uuid::Uuid;

    /// Builds the low-cost passphrase protector shared by local store tests.
    fn test_protector() -> crate::secrets::master_key_protector::MasterKeyProtectorHandle {
        Arc::new(PassphraseMasterKeyProtector::for_test().expect("protector"))
    }

    /// Builds a descriptor with a unique key id for import-policy tests.
    fn descriptor(generation: u64) -> MasterKeyDescriptor {
        MasterKeyDescriptor {
            key_id: Uuid::new_v4(),
            generation,
            scope_view: ClusterViewId::legacy_default(),
            origin_view: ClusterViewId::legacy_default(),
            created_by_node_id: Uuid::new_v4(),
            created_by_operation_id: None,
            parent_key_ids: Vec::new(),
            created_at_unix_secs: 1,
        }
    }

    #[derive(Default)]
    struct CountingProtector {
        unwraps: AtomicUsize,
    }

    impl CountingProtector {
        /// Returns how many envelope unwraps this test protector performed.
        fn unwrap_count(&self) -> usize {
            self.unwraps.load(Ordering::SeqCst)
        }
    }

    impl MasterKeyProtector for CountingProtector {
        /// Returns the test provider id stored in fake envelopes.
        fn provider(&self) -> &'static str {
            "counting-test"
        }

        /// Stores plaintext bytes as ciphertext so tests can count unwraps without a KDF.
        fn wrap(
            &self,
            descriptor: MasterKeyDescriptor,
            plaintext: &MasterKeyPlaintext,
        ) -> io::Result<WrappedMasterKeyRecord> {
            Ok(WrappedMasterKeyRecord {
                schema_version: 1,
                descriptor,
                provider: self.provider().to_string(),
                provider_key_id: "local".to_string(),
                cipher_suite: MasterKeyCipherSuite::XChaCha20Poly1305,
                nonce: Vec::new(),
                ciphertext: plaintext.as_bytes().to_vec(),
                created_at_unix_secs: 0,
                provider_metadata: Vec::new(),
            })
        }

        /// Returns the fake plaintext while incrementing the unwrap counter.
        fn unwrap(&self, record: &WrappedMasterKeyRecord) -> io::Result<MasterKeyPlaintext> {
            self.unwraps.fetch_add(1, Ordering::SeqCst);
            MasterKeyPlaintext::from_slice(&record.ciphertext)
        }
    }

    #[test]
    fn ensure_current_generates_and_persists_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let protector = test_protector();
        let store = SecretMasterStore::new(db.clone(), protector.clone()).expect("open store");

        let first = store
            .ensure_current_for_node(ClusterViewId::legacy_default(), Uuid::new_v4())
            .expect("ensure master key");
        assert_eq!(first.generation(), 1);
        assert_eq!(first.key.as_bytes().len(), 32);

        let again = store.ensure_current().expect("reuse master key");
        assert_eq!(first.key_id(), again.key_id());
        assert_eq!(first.key, again.key);

        let reopened = SecretMasterStore::new(db, protector).expect("reopen store");
        let current = reopened.current().expect("load master key");
        assert_eq!(current.key_id(), again.key_id());
        assert_eq!(current.key, again.key);
    }

    #[test]
    fn rotate_advances_generation_and_keeps_historical_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let base = store.ensure_current().expect("ensure master key");
        let rotated = store
            .rotate(ClusterViewId::legacy_default(), Uuid::new_v4())
            .expect("rotate master key");

        assert_eq!(rotated.generation(), base.generation() + 1);
        assert_ne!(rotated.key_id(), base.key_id());
        assert_ne!(rotated.key, base.key);
        assert_eq!(
            store
                .load_key(base.key_id())
                .expect("load old key")
                .expect("old key exists")
                .key,
            base.key
        );
    }

    #[test]
    fn import_key_preserves_current_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let current = store.ensure_current().expect("ensure master key");
        let historical = MasterKeyRecord::new(
            descriptor(current.generation() + 1),
            MasterKeyPlaintext::generate().expect("historical key"),
        )
        .expect("historical record");

        store
            .import_key(&historical)
            .expect("import historical key");

        assert_eq!(store.current().expect("current").key_id(), current.key_id());
        assert!(
            store
                .contains_key(historical.key_id())
                .expect("contains key")
        );
        assert_eq!(
            store
                .load_key(historical.key_id())
                .expect("load imported key")
                .expect("imported key exists")
                .key,
            historical.key
        );
    }

    #[test]
    fn activate_current_uses_replicated_current_authority() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let original = store.ensure_current().expect("ensure master key");
        let replicated_current = MasterKeyRecord::new(
            descriptor(original.generation()),
            MasterKeyPlaintext::generate().expect("replicated key"),
        )
        .expect("replicated record");

        store
            .import_key(&replicated_current)
            .expect("import replicated key");
        store
            .activate_current(&replicated_current)
            .expect("activate replicated key");

        assert_eq!(
            store.current().expect("current").key_id(),
            replicated_current.key_id()
        );
        assert!(
            store
                .load_key(original.key_id())
                .expect("load original key")
                .is_some(),
            "replicated current adoption must not delete historical keys"
        );
    }

    #[test]
    fn import_current_rejects_stale_and_conflicting_generation() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let base = MasterKeyRecord::new(
            descriptor(2),
            MasterKeyPlaintext::generate().expect("base key"),
        )
        .expect("base record");
        store.import_current(&base).expect("import base");

        let stale = MasterKeyRecord::new(
            descriptor(1),
            MasterKeyPlaintext::generate().expect("stale key"),
        )
        .expect("stale record");
        let stale_err = store
            .import_current(&stale)
            .expect_err("stale transfer must fail");
        assert_eq!(stale_err.kind(), io::ErrorKind::PermissionDenied);

        let conflicting = MasterKeyRecord::new(
            descriptor(2),
            MasterKeyPlaintext::generate().expect("conflicting key"),
        )
        .expect("conflicting record");
        let conflict_err = store
            .import_current(&conflicting)
            .expect_err("same generation conflict must fail outside bootstrap");
        assert_eq!(conflict_err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn join_import_replaces_only_the_initial_bootstrap_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        let anchor = MasterKeyRecord::new(
            descriptor(bootstrap.generation()),
            MasterKeyPlaintext::generate().expect("anchor key"),
        )
        .expect("anchor record");
        store
            .import_join_current(&anchor)
            .expect("join import replaces bootstrap key");
        assert_eq!(store.current().expect("current").key_id(), anchor.key_id());
        store
            .import_join_current(&anchor)
            .expect("same join key import should be idempotent");
        assert!(
            store
                .load_key(bootstrap.key_id())
                .expect("bootstrap lookup")
                .is_none()
        );

        let other = MasterKeyRecord::new(
            descriptor(anchor.generation()),
            MasterKeyPlaintext::generate().expect("other key"),
        )
        .expect("other record");
        let err = store
            .import_join_current(&other)
            .expect_err("adopted join key must not be replaced by another join key");
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(store.current().expect("current").key_id(), anchor.key_id());
    }

    #[test]
    fn failed_join_import_preserves_bootstrap_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        let conflicting = MasterKeyRecord::new(
            bootstrap.descriptor.clone(),
            MasterKeyPlaintext::generate().expect("conflicting key"),
        )
        .expect("conflicting record");

        let err = store
            .import_join_current(&conflicting)
            .expect_err("conflicting bootstrap replacement must fail");
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);

        let current = store.current().expect("current bootstrap remains");
        assert_eq!(current.key_id(), bootstrap.key_id());
        assert_eq!(current.key, bootstrap.key);
    }

    #[test]
    fn transfer_export_commits_bootstrap_against_join_replacement() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        store
            .commit_current_for_transfer(bootstrap.key_id())
            .expect("commit current key for export");
        store
            .import_join_current(&bootstrap)
            .expect("same committed key should remain idempotent");

        let anchor = MasterKeyRecord::new(
            descriptor(bootstrap.generation()),
            MasterKeyPlaintext::generate().expect("anchor key"),
        )
        .expect("anchor record");
        let err = store
            .import_join_current(&anchor)
            .expect_err("served bootstrap key must not be replaced through join");
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);

        let current = store
            .current()
            .expect("current bootstrap remains committed");
        assert_eq!(current.key_id(), bootstrap.key_id());
        assert_eq!(current.key, bootstrap.key);
    }

    #[test]
    fn transfer_export_commit_does_not_unwrap_cached_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let protector = Arc::new(CountingProtector::default());
        let store = SecretMasterStore::new(db, protector.clone()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        store
            .commit_current_for_transfer(bootstrap.key_id())
            .expect("commit current key for export");

        assert_eq!(
            protector.unwrap_count(),
            0,
            "export policy commit should not unwrap the local envelope"
        );
    }

    #[test]
    fn persisted_store_does_not_contain_plaintext_master_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(&db_path).unwrap());
        let store = SecretMasterStore::new(db.clone(), test_protector()).expect("open store");

        let record = store.ensure_current().expect("ensure master key");
        drop(store);
        drop(db);

        let bytes = std::fs::read(&db_path).expect("read redb");
        assert!(
            !bytes
                .windows(record.key.as_bytes().len())
                .any(|window| window == &record.key.as_bytes()[..]),
            "wrapped master key store must not contain the raw master key"
        );
    }
}
