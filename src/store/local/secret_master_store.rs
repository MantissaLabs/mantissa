use crate::secrets::master_key_protector::{
    MasterKeyPlaintext, MasterKeyProtectorHandle, WrappedMasterKeyRecord,
};
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use parking_lot::{Mutex, MutexGuard};
use redb::{Database, TableDefinition};
use std::cmp::Ordering;
use std::{io, sync::Arc};

const T_MASTER_KEY_ENVELOPES: TableDefinition<u64, &'static [u8]> =
    TableDefinition::new("secret_master_key_envelopes");
const T_MASTER_META: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("secret_master_meta");
/// Metadata key pointing at the locally active master-key version.
const CURRENT_VERSION_KEY: &str = "current_version";
/// Metadata key marking whether the initial v1 key is still a join bootstrap key.
const BOOTSTRAP_PENDING_KEY: &str = "bootstrap_pending";
/// Compact boolean marker for Redb metadata values.
const META_TRUE: &[u8] = b"1";
/// Compact boolean marker for Redb metadata values.
const META_FALSE: &[u8] = b"0";

/// Immutable plaintext snapshot of one master key version after local envelope unwrap.
#[derive(Debug, Clone)]
pub struct MasterKeyRecord {
    pub version: u64,
    pub key: MasterKeyPlaintext,
}

impl MasterKeyRecord {
    /// Creates a record from raw parts after validating the version value.
    pub fn new(version: u64, key: MasterKeyPlaintext) -> io::Result<Self> {
        if version == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secret master key version must be positive",
            ));
        }
        Ok(Self { version, key })
    }
}

/// Durable storage for locally wrapped cluster secret master keys.
#[derive(Clone)]
pub struct SecretMasterStore {
    db: Arc<Database>,
    protector: MasterKeyProtectorHandle,
    // Serializes the local cluster-key decision. A fresh node starts with a
    // temporary v1 key so secrets can work before it joins. The first cluster
    // action must choose one outcome: adopt an anchor key, serve its own key as
    // an anchor, or rotate. Without this lock, node 1 could serve its temporary
    // key to node 3 while concurrently joining node 2 and adopting node 2's
    // key, leaving node 3 in a different secret domain.
    policy_lock: Arc<Mutex<()>>,
}

impl SecretMasterStore {
    /// Opens (or creates) the wrapped secret master key tables without generating a key.
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

    /// Ensures a current wrapped master key exists, generating and persisting one when missing.
    pub fn ensure_current(&self) -> io::Result<MasterKeyRecord> {
        let _guard = self.policy_guard();
        if let Some(existing) = self.load_current()? {
            return Ok(existing);
        }

        let generated = MasterKeyPlaintext::generate()?;
        self.persist_new_version(1, &generated, true)?;

        MasterKeyRecord::new(1, generated)
    }

    /// Returns the currently active master key record.
    pub fn current(&self) -> io::Result<MasterKeyRecord> {
        self.load_current()?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "secret master key missing"))
    }

    /// Fetches the master key record associated with `version` if it exists.
    ///
    /// Historical versions remain available so peers can continue decrypting
    /// data during cluster-wide convergence after a rotation. Once all nodes
    /// have installed the newer key, older entries can be garbage-collected
    /// separately.
    pub fn load_version(&self, version: u64) -> io::Result<Option<MasterKeyRecord>> {
        let Some(wrapped) = self.load_wrapped_version(version)? else {
            return Ok(None);
        };
        let key = self.protector.unwrap(&wrapped)?;
        MasterKeyRecord::new(version, key).map(Some)
    }

    /// Imports an externally provided master key as the active version.
    pub fn import_current(&self, record: &MasterKeyRecord) -> io::Result<()> {
        self.import_current_with_policy(record, false)
    }

    /// Imports the anchor's cluster master key while replacing a local bootstrap key if needed.
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
    ///
    /// The caller supplies the version it already has cached in the keyring.
    /// That avoids an envelope unwrap on the hot join path while still using
    /// this store lock as the authority for the cluster-key policy decision.
    pub fn commit_current_for_transfer(&self, expected_version: u64) -> io::Result<()> {
        let _guard = self.policy_guard();
        let current_version = self
            .current_version()?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "secret master key missing"))?;
        if current_version != expected_version {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "secret master key changed before transfer",
            ));
        }
        self.set_bootstrap_pending(false)?;
        Ok(())
    }

    /// Imports an external master key while enforcing version and replay policy.
    fn import_current_with_policy(
        &self,
        record: &MasterKeyRecord,
        allow_bootstrap_replacement: bool,
    ) -> io::Result<()> {
        let _guard = self.policy_guard();
        if let Some(current_version) = self.current_version()? {
            match record.version.cmp(&current_version) {
                Ordering::Less => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "stale secret master key transfer rejected",
                    ));
                }
                Ordering::Equal => {
                    if allow_bootstrap_replacement
                        && current_version == 1
                        && self.bootstrap_replacement_pending()?
                    {
                        self.persist_new_version(record.version, &record.key, false)?;
                        return Ok(());
                    }
                    let current = self.load_version(current_version)?.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "secret master key envelope missing for recorded version",
                        )
                    })?;
                    if current.key == record.key {
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

        self.persist_new_version(record.version, &record.key, false)?;
        Ok(())
    }

    /// Generates, persists, and returns a rotated master key with the next sequential version.
    pub fn rotate(&self) -> io::Result<MasterKeyRecord> {
        let _guard = self.policy_guard();
        let current_version = match self.current_version()? {
            Some(v) => v,
            None => {
                let generated = MasterKeyPlaintext::generate()?;
                self.persist_new_version(1, &generated, true)?;
                return MasterKeyRecord::new(1, generated);
            }
        };

        let next_version = current_version
            .checked_add(1)
            .ok_or_else(|| io::Error::other("master key version overflow"))?;
        let new_key = MasterKeyPlaintext::generate()?;
        self.persist_new_version(next_version, &new_key, false)?;
        MasterKeyRecord::new(next_version, new_key)
    }

    /// Locks local master-key policy changes so adoption, serving, and rotation cannot race.
    ///
    /// Redb transactions keep each individual write atomic, but the policy
    /// decision spans reads, unwraps, comparisons, and writes. Holding this
    /// process-local lock makes those multi-step decisions mutually exclusive
    /// for the single daemon instance that owns the local state directory.
    fn policy_guard(&self) -> MutexGuard<'_, ()> {
        self.policy_lock.lock()
    }

    /// Loads the currently active master key record if one has been persisted.
    fn load_current(&self) -> io::Result<Option<MasterKeyRecord>> {
        with_read_tx(&self.db, |tx| {
            let meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            let version_bytes = match meta.get(CURRENT_VERSION_KEY).map_err(into_io)? {
                Some(raw) => raw.value().to_vec(),
                None => return Ok(None),
            };
            if version_bytes.len() != 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "secret master key metadata corrupted",
                ));
            }
            let mut version_buf = [0u8; 8];
            version_buf.copy_from_slice(&version_bytes);
            let version = u64::from_be_bytes(version_buf);

            Ok(Some(version))
        })?
        .map(|version| {
            self.load_version(version)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "secret master key envelope missing for recorded version",
                )
            })
        })
        .transpose()
    }

    /// Reads the version pointer from metadata, returning `None` when uninitialized.
    fn current_version(&self) -> io::Result<Option<u64>> {
        with_read_tx(&self.db, |tx| {
            let meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            let Some(raw) = meta.get(CURRENT_VERSION_KEY).map_err(into_io)? else {
                return Ok(None);
            };
            let bytes = raw.value();
            if bytes.len() != 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "secret master key metadata corrupted",
                ));
            }
            let mut buf = [0u8; 8];
            buf.copy_from_slice(bytes);
            Ok(Some(u64::from_be_bytes(buf)))
        })
    }

    /// Returns true while the local v1 key may be replaced by an authenticated join anchor key.
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

    /// Loads the wrapped envelope associated with `version` if one has been persisted.
    fn load_wrapped_version(&self, version: u64) -> io::Result<Option<WrappedMasterKeyRecord>> {
        with_read_tx(&self.db, |tx| {
            let table = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            let Some(raw_envelope) = table.get(version).map_err(into_io)? else {
                return Ok(None);
            };
            WrappedMasterKeyRecord::decode(raw_envelope.value()).map(Some)
        })
    }

    /// Persists `key` as an envelope under `version` and advances the metadata pointer atomically.
    fn persist_new_version(
        &self,
        version: u64,
        key: &MasterKeyPlaintext,
        bootstrap_pending: bool,
    ) -> io::Result<()> {
        if version == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secret master key version must be positive",
            ));
        }

        let mut version_bytes = [0u8; 8];
        version_bytes.copy_from_slice(&version.to_be_bytes());
        let wrapped = self.protector.wrap(version, key)?;
        let encoded = wrapped.encode()?;

        with_write_tx(&self.db, |tx| {
            let mut envelopes = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            envelopes
                .insert(version, encoded.as_slice())
                .map_err(into_io)?;

            let mut meta = tx.open_table(T_MASTER_META).map_err(into_io)?;
            meta.insert(CURRENT_VERSION_KEY, version_bytes.as_slice())
                .map_err(into_io)?;
            let bootstrap_value = if bootstrap_pending {
                META_TRUE
            } else {
                META_FALSE
            };
            meta.insert(BOOTSTRAP_PENDING_KEY, bootstrap_value)
                .map_err(into_io)?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{MasterKeyRecord, SecretMasterStore};
    use crate::secrets::master_key_protector::{
        MasterKeyCipherSuite, MasterKeyPlaintext, MasterKeyProtector, PassphraseMasterKeyProtector,
        WrappedMasterKeyRecord,
    };
    use redb::Database;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    fn test_protector() -> crate::secrets::master_key_protector::MasterKeyProtectorHandle {
        Arc::new(PassphraseMasterKeyProtector::for_test().expect("protector"))
    }

    #[derive(Default)]
    struct CountingProtector {
        wraps: AtomicUsize,
        unwraps: AtomicUsize,
    }

    impl CountingProtector {
        fn unwrap_count(&self) -> usize {
            self.unwraps.load(Ordering::SeqCst)
        }
    }

    impl MasterKeyProtector for CountingProtector {
        fn provider(&self) -> &'static str {
            "counting-test"
        }

        fn wrap(
            &self,
            version: u64,
            plaintext: &MasterKeyPlaintext,
        ) -> io::Result<WrappedMasterKeyRecord> {
            self.wraps.fetch_add(1, Ordering::SeqCst);
            Ok(WrappedMasterKeyRecord {
                schema_version: 1,
                master_key_version: version,
                provider: self.provider().to_string(),
                provider_key_id: "local".to_string(),
                cipher_suite: MasterKeyCipherSuite::XChaCha20Poly1305,
                nonce: Vec::new(),
                ciphertext: plaintext.as_bytes().to_vec(),
                created_at_unix_secs: 0,
                provider_metadata: Vec::new(),
            })
        }

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

        let first = store.ensure_current().expect("ensure master key");
        assert_eq!(first.version, 1);
        assert_eq!(first.key.as_bytes().len(), 32);

        let again = store.ensure_current().expect("reuse master key");
        assert_eq!(first.version, again.version);
        assert_eq!(first.key, again.key);

        let reopened = SecretMasterStore::new(db, protector).expect("reopen store");
        let current = reopened.current().expect("load master key");
        assert_eq!(current.version, again.version);
        assert_eq!(current.key, again.key);
    }

    #[test]
    fn rotate_advances_version_and_changes_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let base = store.ensure_current().expect("ensure master key");
        let rotated = store.rotate().expect("rotate master key");

        assert_eq!(rotated.version, base.version + 1);
        assert_ne!(rotated.key, base.key);
    }

    #[test]
    fn import_current_rejects_stale_and_conflicting_versions() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let base = store.ensure_current().expect("ensure master key");
        let rotated = store.rotate().expect("rotate master key");

        let stale_err = store
            .import_current(&base)
            .expect_err("stale import must fail");
        assert_eq!(stale_err.kind(), std::io::ErrorKind::PermissionDenied);

        let conflicting = MasterKeyRecord::new(
            rotated.version,
            MasterKeyPlaintext::generate().expect("conflicting key"),
        )
        .expect("conflicting record");
        let conflict_err = store
            .import_current(&conflicting)
            .expect_err("same-version conflict must fail");
        assert_eq!(conflict_err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn join_import_replaces_only_the_initial_bootstrap_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        let anchor = MasterKeyRecord::new(
            bootstrap.version,
            MasterKeyPlaintext::generate().expect("anchor key"),
        )
        .expect("anchor record");
        store
            .import_join_current(&anchor)
            .expect("join import replaces bootstrap key");
        assert_eq!(store.current().expect("current").key, anchor.key);

        let other = MasterKeyRecord::new(
            anchor.version,
            MasterKeyPlaintext::generate().expect("other key"),
        )
        .expect("other record");
        let err = store
            .import_join_current(&other)
            .expect_err("adopted cluster key must not be replaced again");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn transfer_export_commits_the_initial_bootstrap_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db, test_protector()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        store
            .commit_current_for_transfer(bootstrap.version)
            .expect("commit current key for export");

        let anchor = MasterKeyRecord::new(
            bootstrap.version,
            MasterKeyPlaintext::generate().expect("anchor key"),
        )
        .expect("anchor record");
        let err = store
            .import_join_current(&anchor)
            .expect_err("served bootstrap key must be committed");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
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
            .commit_current_for_transfer(bootstrap.version)
            .expect("commit current key for export");

        assert_eq!(
            protector.unwrap_count(),
            0,
            "export policy commit should not unwrap the local envelope"
        );
    }

    #[test]
    fn join_import_replaces_bootstrap_key_without_unwrapping_local_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let protector = Arc::new(CountingProtector::default());
        let store = SecretMasterStore::new(db, protector.clone()).expect("open store");

        let bootstrap = store.ensure_current().expect("ensure master key");
        let anchor = MasterKeyRecord::new(
            bootstrap.version,
            MasterKeyPlaintext::generate().expect("anchor key"),
        )
        .expect("anchor record");
        store
            .import_join_current(&anchor)
            .expect("join import replaces bootstrap key");

        assert_eq!(
            protector.unwrap_count(),
            0,
            "bootstrap replacement should not unwrap the discarded local envelope"
        );
        assert_eq!(store.current().expect("current").key, anchor.key);
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
