use crate::secrets::crypto::SecretKeyring;
use redb::{Database, TableDefinition};
use std::{io, sync::Arc};

const T_MASTER_KEYS: TableDefinition<u64, &'static [u8]> =
    TableDefinition::new("secret_master_keys");
const T_MASTER_META: TableDefinition<&'static str, &'static [u8]> =
    TableDefinition::new("secret_master_meta");
const CURRENT_VERSION_KEY: &str = "current_version";

#[inline]
fn ioerr<E: std::error::Error>(err: E) -> io::Error {
    io::Error::other(err.to_string())
}

/// Immutable snapshot of a master key version stored in the durable key vault.
#[derive(Debug, Clone, Copy)]
pub struct MasterKeyRecord {
    pub version: u64,
    pub key: [u8; 32],
}

impl MasterKeyRecord {
    /// Creates a record from raw parts after validating the version value.
    pub fn new(version: u64, key: [u8; 32]) -> io::Result<Self> {
        if version == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secret master key version must be positive",
            ));
        }
        Ok(Self { version, key })
    }
}

/// Durable storage for the cluster-wide secret encryption root key.
#[derive(Clone)]
pub struct SecretMasterStore {
    db: Arc<Database>,
}

impl SecretMasterStore {
    /// Opens (or creates) the secret master key tables without generating a key.
    pub fn new(db: Arc<Database>) -> io::Result<Self> {
        let write_tx = db.begin_write().map_err(ioerr)?;
        {
            let _ = write_tx.open_table(T_MASTER_KEYS).map_err(ioerr)?;
            let _ = write_tx.open_table(T_MASTER_META).map_err(ioerr)?;
        }
        write_tx.commit().map_err(ioerr)?;

        Ok(Self { db })
    }

    /// Ensures a current master key exists, generating and persisting one when missing.
    pub fn ensure_current(&self) -> io::Result<MasterKeyRecord> {
        if let Some(existing) = self.load_current()? {
            return Ok(existing);
        }

        let generated = SecretKeyring::generate_master_key()?;
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
        let read_tx = self.db.begin_read().map_err(ioerr)?;
        let table = read_tx.open_table(T_MASTER_KEYS).map_err(ioerr)?;
        let Some(raw_key) = table.get(version).map_err(ioerr)? else {
            return Ok(None);
        };
        let bytes = raw_key.value();
        if bytes.len() != 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "secret master key length invalid",
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(bytes);
        MasterKeyRecord::new(version, key).map(Some)
    }

    /// Imports an externally provided master key as the active version.
    pub fn import_current(&self, record: &MasterKeyRecord) -> io::Result<()> {
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
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "master key version overflow"))?;
        let new_key = SecretKeyring::generate_master_key()?;
        self.persist_new_version(next_version, &new_key)?;
        MasterKeyRecord::new(next_version, new_key)
    }

    /// Loads the currently active master key record if one has been persisted.
    fn load_current(&self) -> io::Result<Option<MasterKeyRecord>> {
        let read_tx = self.db.begin_read().map_err(ioerr)?;
        let meta = read_tx.open_table(T_MASTER_META).map_err(ioerr)?;
        let version_bytes = match meta.get(CURRENT_VERSION_KEY).map_err(ioerr)? {
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

        let table = read_tx.open_table(T_MASTER_KEYS).map_err(ioerr)?;
        let Some(raw_key) = table.get(version).map_err(ioerr)? else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "secret master key missing for recorded version",
            ));
        };

        let bytes = raw_key.value();
        if bytes.len() != 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "secret master key length invalid",
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(bytes);
        MasterKeyRecord::new(version, key).map(Some)
    }

    /// Reads the version pointer from metadata, returning `None` when uninitialized.
    fn current_version(&self) -> io::Result<Option<u64>> {
        let read_tx = self.db.begin_read().map_err(ioerr)?;
        let meta = read_tx.open_table(T_MASTER_META).map_err(ioerr)?;
        let Some(raw) = meta.get(CURRENT_VERSION_KEY).map_err(ioerr)? else {
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
    }

    /// Persists `key` under `version` and advances the metadata pointer atomically.
    fn persist_new_version(&self, version: u64, key: &[u8; 32]) -> io::Result<()> {
        if version == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "secret master key version must be positive",
            ));
        }

        let mut version_bytes = [0u8; 8];
        version_bytes.copy_from_slice(&version.to_be_bytes());

        let write_tx = self.db.begin_write().map_err(ioerr)?;
        {
            let mut keys = write_tx.open_table(T_MASTER_KEYS).map_err(ioerr)?;
            keys.insert(version, key.as_slice()).map_err(ioerr)?;

            let mut meta = write_tx.open_table(T_MASTER_META).map_err(ioerr)?;
            meta.insert(CURRENT_VERSION_KEY, version_bytes.as_slice())
                .map_err(ioerr)?;
        }
        write_tx.commit().map_err(ioerr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SecretMasterStore;
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn ensure_current_generates_and_persists_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db.clone()).expect("open store");

        let first = store.ensure_current().expect("ensure master key");
        assert_eq!(first.version, 1);
        assert_eq!(first.key.len(), 32);

        let again = store.ensure_current().expect("reuse master key");
        assert_eq!(first.version, again.version);
        assert_eq!(first.key, again.key);

        let reopened = SecretMasterStore::new(db).expect("reopen store");
        let current = reopened.current().expect("load master key");
        assert_eq!(current.version, again.version);
        assert_eq!(current.key, again.key);
    }

    #[test]
    fn rotate_advances_version_and_changes_key() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db).expect("open store");

        let base = store.ensure_current().expect("ensure master key");
        let rotated = store.rotate().expect("rotate master key");

        assert_eq!(rotated.version, base.version + 1);
        assert_ne!(rotated.key, base.key);
    }
}
