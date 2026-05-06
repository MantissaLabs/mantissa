use crate::secrets::master_key_protector::{
    MASTER_KEY_SIZE, MasterKeyDescriptor, MasterKeyPlaintext,
};
use crate::secrets::types::SecretCiphertext;
use crate::store::local::{MasterKeyRecord, SecretMasterStore};
use blake3::Hash;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use uuid::Uuid;

const AAD_PREFIX: &[u8] = b"mantissa.secret.v1";

/// In-memory key material used to encrypt and decrypt secret payloads.
#[derive(Clone)]
pub struct SecretKeyring {
    inner: Arc<Inner>,
}

struct Inner {
    master_store: SecretMasterStore,
    cache: RwLock<HashMap<Uuid, MasterKeyPlaintext>>,
    current_descriptor: RwLock<MasterKeyDescriptor>,
}

impl SecretKeyring {
    /// Constructs a keyring bound to the provided master key store and active record.
    pub fn new(master_store: SecretMasterStore, active: MasterKeyRecord) -> Self {
        let mut cache = HashMap::new();
        cache.insert(active.key_id(), active.key);
        let inner = Inner {
            master_store,
            cache: RwLock::new(cache),
            current_descriptor: RwLock::new(active.descriptor),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Returns the current encrypted master key id.
    pub fn current_key_id(&self) -> Uuid {
        self.inner.current_descriptor.read().key_id
    }

    /// Returns the cached active master key record for transfer operations.
    ///
    /// The active key should always be in memory after bootstrap or rotation.
    /// The store fallback is retained for defensive recovery, but the normal
    /// join path avoids another passphrase KDF by cloning the cached key.
    pub fn current_record(&self) -> io::Result<MasterKeyRecord> {
        let descriptor = self.inner.current_descriptor.read().clone();
        {
            let cache = self.inner.cache.read();
            if let Some(key) = cache.get(&descriptor.key_id) {
                return MasterKeyRecord::new(descriptor, key.clone());
            }
        }

        let record = self
            .inner
            .master_store
            .load_key(descriptor.key_id)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "master key missing"))?;

        let mut cache = self.inner.cache.write();
        let entry = cache.entry(record.key_id()).or_insert(record.key);
        MasterKeyRecord::new(record.descriptor, entry.clone())
    }

    /// Installs `record` as the new active master key while caching its material.
    pub fn install_current(&self, record: &MasterKeyRecord) {
        // Keep previous keys in memory and on disk. Rotation rewraps secrets to
        // the new key, but a node can still read older replicated ciphertext
        // while the cluster converges or while historical versions are retained.
        {
            let mut cache = self.inner.cache.write();
            cache.insert(record.key_id(), record.key.clone());
        }
        *self.inner.current_descriptor.write() = record.descriptor.clone();
    }

    /// Caches one non-current master key for future decrypts by key id.
    pub fn cache_key(&self, record: &MasterKeyRecord) {
        let mut cache = self.inner.cache.write();
        cache.insert(record.key_id(), record.key.clone());
    }

    /// Rebuilds a master key record from cached key material and replicated metadata.
    ///
    /// Reconciliation imports a replicated grant by wrapping it locally and
    /// caching the plaintext in the same pass. When that grant is also the
    /// replicated current key, adoption can use this method instead of loading
    /// the freshly persisted envelope and paying a second passphrase KDF.
    pub fn cached_record(
        &self,
        descriptor: &MasterKeyDescriptor,
    ) -> io::Result<Option<MasterKeyRecord>> {
        let cache = self.inner.cache.read();
        cache
            .get(&descriptor.key_id)
            .cloned()
            .map(|key| MasterKeyRecord::new(descriptor.clone(), key))
            .transpose()
    }

    /// Borrows one master key by id for immediate cryptographic use.
    fn with_master_key<R>(
        &self,
        key_id: Uuid,
        f: impl FnOnce(&[u8; MASTER_KEY_SIZE]) -> io::Result<R>,
    ) -> io::Result<R> {
        {
            let cache = self.inner.cache.read();
            if let Some(key) = cache.get(&key_id) {
                return f(key.as_bytes());
            }
        }

        let record = self
            .inner
            .master_store
            .load_key(key_id)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "master key missing"))?;

        let mut cache = self.inner.cache.write();
        let entry = cache.entry(record.key_id()).or_insert(record.key);
        f(entry.as_bytes())
    }

    /// Encrypts `plaintext` for the provided secret/version identifiers.
    pub fn encrypt(
        &self,
        secret_id: Uuid,
        version_id: Uuid,
        plaintext: &[u8],
    ) -> io::Result<SecretCiphertext> {
        let nonce = Self::random_nonce()?;
        let descriptor = self.inner.current_descriptor.read().clone();
        let aad = Self::aad(
            secret_id,
            version_id,
            descriptor.key_id,
            descriptor.generation,
        );
        let ciphertext = self.with_master_key(descriptor.key_id, |master_key| {
            let aead = ChaCha20Poly1305::new(Key::from_slice(master_key));
            aead.encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| io::Error::other("secret encryption failed"))
        })?;

        let digest = Self::digest_bytes(blake3::hash(plaintext));

        Ok(SecretCiphertext {
            master_key_id: descriptor.key_id,
            master_key_generation: descriptor.generation,
            nonce,
            ciphertext,
            digest,
        })
    }

    /// Decrypts an encrypted payload and verifies the recorded digest.
    pub fn decrypt(
        &self,
        secret_id: Uuid,
        version_id: Uuid,
        envelope: &SecretCiphertext,
    ) -> io::Result<Vec<u8>> {
        let aad = Self::aad(
            secret_id,
            version_id,
            envelope.master_key_id,
            envelope.master_key_generation,
        );
        let plaintext = self.with_master_key(envelope.master_key_id, |master_key| {
            let aead = ChaCha20Poly1305::new(Key::from_slice(master_key));
            aead.decrypt(
                Nonce::from_slice(&envelope.nonce),
                Payload {
                    msg: envelope.ciphertext.as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| io::Error::new(io::ErrorKind::PermissionDenied, "secret decrypt failed"))
        })?;

        let digest = blake3::hash(&plaintext);
        if Self::digest_bytes(digest) != envelope.digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "secret digest mismatch after decrypt",
            ));
        }

        Ok(plaintext)
    }

    /// Generates one random nonce for ChaCha20-Poly1305 secret payloads.
    fn random_nonce() -> io::Result<[u8; 12]> {
        let mut nonce = [0u8; 12];
        getrandom::getrandom(&mut nonce)?;
        Ok(nonce)
    }

    /// Builds AAD that binds ciphertext to both the secret and the selected master key.
    fn aad(
        secret_id: Uuid,
        version_id: Uuid,
        master_key_id: Uuid,
        master_key_generation: u64,
    ) -> Vec<u8> {
        let mut aad = Vec::with_capacity(AAD_PREFIX.len() + 56);
        aad.extend_from_slice(AAD_PREFIX);
        aad.extend_from_slice(secret_id.as_bytes());
        aad.extend_from_slice(version_id.as_bytes());
        aad.extend_from_slice(master_key_id.as_bytes());
        aad.extend_from_slice(&master_key_generation.to_be_bytes());
        aad
    }

    /// Converts a Blake3 digest into the fixed-size digest field used in secret ciphertext.
    fn digest_bytes(hash: Hash) -> [u8; MASTER_KEY_SIZE] {
        let mut out = [0u8; MASTER_KEY_SIZE];
        out.copy_from_slice(hash.as_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::SecretKeyring;
    use crate::secrets::master_key_protector::PassphraseMasterKeyProtector;
    use crate::store::local::SecretMasterStore;
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    /// Builds a temporary wrapped master-key store for crypto tests.
    fn temp_store() -> (SecretMasterStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let protector = Arc::new(PassphraseMasterKeyProtector::for_test().unwrap());
        let store = SecretMasterStore::new(db, protector).expect("open store");
        (store, dir)
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let (store, _dir) = temp_store();
        let record = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store, record);
        let secret_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();

        let ciphertext = keyring
            .encrypt(secret_id, version_id, b"super-secret")
            .expect("encrypt");
        let plaintext = keyring
            .decrypt(secret_id, version_id, &ciphertext)
            .expect("decrypt");

        assert_eq!(plaintext, b"super-secret");
    }

    #[test]
    fn decrypt_rejects_wrong_secret_id() {
        let (store, _dir) = temp_store();
        let record = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store, record);
        let version_id = Uuid::new_v4();
        let ciphertext = keyring
            .encrypt(Uuid::new_v4(), version_id, b"super-secret")
            .expect("encrypt");

        assert!(
            keyring
                .decrypt(Uuid::new_v4(), version_id, &ciphertext)
                .is_err()
        );
    }

    #[test]
    fn decrypt_rejects_tampered_master_key_identity() {
        let (store, _dir) = temp_store();
        let record = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store, record);
        let secret_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let mut ciphertext = keyring
            .encrypt(secret_id, version_id, b"super-secret")
            .expect("encrypt");
        ciphertext.master_key_generation = ciphertext.master_key_generation.saturating_add(1);

        assert!(keyring.decrypt(secret_id, version_id, &ciphertext).is_err());
    }

    #[test]
    fn decrypt_uses_historical_key_after_rotation() {
        let (store, _dir) = temp_store();
        let initial = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store.clone(), initial);
        let secret_id = Uuid::new_v4();
        let old_version_id = Uuid::new_v4();
        let new_version_id = Uuid::new_v4();

        let cipher_old = keyring
            .encrypt(secret_id, old_version_id, b"old")
            .expect("encrypt old");
        let initial_key_id = cipher_old.master_key_id;
        let initial_generation = cipher_old.master_key_generation;

        let rotated = store
            .rotate(
                crate::cluster::ClusterViewId::legacy_default(),
                Uuid::new_v4(),
            )
            .expect("rotate");
        keyring.install_current(&rotated);

        let cipher_new = keyring
            .encrypt(secret_id, new_version_id, b"new")
            .expect("encrypt new");

        assert_eq!(cipher_old.master_key_id, initial_key_id);
        assert_eq!(cipher_old.master_key_generation, initial_generation);
        assert_eq!(cipher_new.master_key_id, rotated.key_id());
        assert_eq!(cipher_new.master_key_generation, rotated.generation());
        assert_eq!(
            keyring
                .decrypt(secret_id, old_version_id, &cipher_old)
                .expect("decrypt old"),
            b"old"
        );
        assert_eq!(
            keyring
                .decrypt(secret_id, new_version_id, &cipher_new)
                .expect("decrypt new"),
            b"new"
        );
    }
}
