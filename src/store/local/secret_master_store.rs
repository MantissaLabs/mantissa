use crate::secrets::master_key_protector::{
    MasterKeyPlaintext, MasterKeyProtectorHandle, WrappedMasterKeyRecord,
};
use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, TableDefinition};
use std::cmp::Ordering;
use std::{io, sync::Arc};

const T_MASTER_KEY_ENVELOPES: TableDefinition<u64, &'static [u8]> =
    TableDefinition::new("secret_master_key_envelopes");
const T_MASTER_META: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("secret_master_meta");
const CURRENT_VERSION_KEY: &str = "current_version";

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
}

impl SecretMasterStore {
    /// Opens (or creates) the wrapped secret master key tables without generating a key.
    pub fn new(db: Arc<Database>, protector: MasterKeyProtectorHandle) -> io::Result<Self> {
        with_write_tx(&db, |tx| {
            let _ = tx.open_table(T_MASTER_KEY_ENVELOPES).map_err(into_io)?;
            let _ = tx.open_table(T_MASTER_META).map_err(into_io)?;
            Ok(())
        })?;

        Ok(Self { db, protector })
    }

    /// Ensures a current wrapped master key exists, generating and persisting one when missing.
    pub fn ensure_current(&self) -> io::Result<MasterKeyRecord> {
        if let Some(existing) = self.load_current()? {
            return Ok(existing);
        }

        let generated = MasterKeyPlaintext::generate()?;
        self.persist_new_version(1, &generated)?;

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
        if let Some(current_version) = self.current_version()? {
            match record.version.cmp(&current_version) {
                Ordering::Less => {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "stale secret master key transfer rejected",
                    ));
                }
                Ordering::Equal => {
                    let current = self.load_version(current_version)?.ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "secret master key envelope missing for recorded version",
                        )
                    })?;
                    if current.key != record.key {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "conflicting secret master key transfer rejected",
                        ));
                    }
                    return Ok(());
                }
                Ordering::Greater => {}
            }
        }

        self.persist_new_version(record.version, &record.key)?;
        Ok(())
    }

    /// Generates, persists, and returns a rotated master key with the next sequential version.
    pub fn rotate(&self) -> io::Result<MasterKeyRecord> {
        let current_version = match self.current_version()? {
            Some(v) => v,
            None => return self.ensure_current(),
        };

        let next_version = current_version
            .checked_add(1)
            .ok_or_else(|| io::Error::other("master key version overflow"))?;
        let new_key = MasterKeyPlaintext::generate()?;
        self.persist_new_version(next_version, &new_key)?;
        MasterKeyRecord::new(next_version, new_key)
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
    fn persist_new_version(&self, version: u64, key: &MasterKeyPlaintext) -> io::Result<()> {
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
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{MasterKeyRecord, SecretMasterStore};
    use crate::secrets::master_key_protector::{MasterKeyPlaintext, PassphraseMasterKeyProtector};
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn test_protector() -> crate::secrets::master_key_protector::MasterKeyProtectorHandle {
        Arc::new(PassphraseMasterKeyProtector::for_test(Uuid::new_v4()).expect("protector"))
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
