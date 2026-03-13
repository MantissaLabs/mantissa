use crate::secrets::types::SecretCiphertext;
use crate::store::local::{MasterKeyRecord, SecretMasterStore};
use blake3::Hash;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use uuid::Uuid;

const AAD_PREFIX: &[u8] = b"mantissa.secret.v1";
const MASTER_KEY_SIZE: usize = 32;

/// In-memory key material used to encrypt and decrypt secret payloads.
#[derive(Clone)]
pub struct SecretKeyring {
    inner: Arc<Inner>,
}

struct Inner {
    master_store: SecretMasterStore,
    cache: RwLock<HashMap<u64, [u8; MASTER_KEY_SIZE]>>,
    current_version: AtomicU64,
}

impl SecretKeyring {
    /// Constructs a keyring bound to the provided master key store and active record.
    pub fn new(master_store: SecretMasterStore, active: MasterKeyRecord) -> Self {
        let mut cache = HashMap::new();
        cache.insert(active.version, active.key);
        let inner = Inner {
            master_store,
            cache: RwLock::new(cache),
            current_version: AtomicU64::new(active.version),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Generates a fresh cryptographically random master key suitable for cluster-wide use.
    pub fn generate_master_key() -> io::Result<[u8; MASTER_KEY_SIZE]> {
        let mut master_key = [0u8; MASTER_KEY_SIZE];
        getrandom::getrandom(&mut master_key)?;
        Ok(master_key)
    }

    /// Returns the current encrypted master key version identifier.
    pub fn current_version(&self) -> u64 {
        self.inner.current_version.load(Ordering::SeqCst)
    }

    /// Installs `record` as the new active master key while caching its material.
    pub fn install_current(&self, record: MasterKeyRecord) {
        // NOTE: we intentionally preserve older key versions in the cache/store so peers can
        // still decrypt ciphertext that was encrypted before they applied the new version.
        // Rotation re-wraps secrets with `record.version`, but remote nodes might serve reads
        // against the previous version until they receive the broadcast.
        {
            let mut cache = self.inner.cache.write().expect("poisoned master cache");
            cache.insert(record.version, record.key);
        }
        self.inner
            .current_version
            .store(record.version, Ordering::SeqCst);
    }

    fn master_key_for(&self, version: u64) -> io::Result<[u8; MASTER_KEY_SIZE]> {
        {
            let cache = self.inner.cache.read().expect("poisoned master cache");
            if let Some(key) = cache.get(&version) {
                return Ok(*key);
            }
        }

        let record = self
            .inner
            .master_store
            .load_version(version)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "master key version missing"))?;

        let mut cache = self.inner.cache.write().expect("poisoned master cache");
        let entry = cache.entry(record.version).or_insert(record.key);
        Ok(*entry)
    }

    /// Encrypts `plaintext` for the provided secret/version identifiers.
    pub fn encrypt(
        &self,
        secret_id: Uuid,
        version_id: Uuid,
        plaintext: &[u8],
    ) -> io::Result<SecretCiphertext> {
        let nonce = Self::random_nonce()?;
        let version = self.current_version();
        let master_key = self.master_key_for(version)?;
        let aead = ChaCha20Poly1305::new(Key::from_slice(&master_key));
        let aad = Self::aad(secret_id, version_id);
        let ciphertext = aead
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| io::Error::other("secret encryption failed"))?;

        let digest = Self::digest_bytes(blake3::hash(plaintext));

        Ok(SecretCiphertext {
            master_key_version: version,
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
        let master_key = self.master_key_for(envelope.master_key_version)?;
        let aead = ChaCha20Poly1305::new(Key::from_slice(&master_key));
        let aad = Self::aad(secret_id, version_id);
        let plaintext = aead
            .decrypt(
                Nonce::from_slice(&envelope.nonce),
                Payload {
                    msg: envelope.ciphertext.as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|_| {
                io::Error::new(io::ErrorKind::PermissionDenied, "secret decrypt failed")
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

    fn random_nonce() -> io::Result<[u8; 12]> {
        let mut nonce = [0u8; 12];
        getrandom::getrandom(&mut nonce)?;
        Ok(nonce)
    }

    fn aad(secret_id: Uuid, version_id: Uuid) -> Vec<u8> {
        let mut aad = Vec::with_capacity(AAD_PREFIX.len() + 32 + 32);
        aad.extend_from_slice(AAD_PREFIX);
        aad.extend_from_slice(secret_id.as_bytes());
        aad.extend_from_slice(version_id.as_bytes());
        aad
    }

    fn digest_bytes(hash: Hash) -> [u8; MASTER_KEY_SIZE] {
        let mut out = [0u8; MASTER_KEY_SIZE];
        out.copy_from_slice(hash.as_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::SecretKeyring;
    use crate::store::local::SecretMasterStore;
    use redb::Database;
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn temp_store() -> (SecretMasterStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("state.redb");
        let db = Arc::new(Database::create(db_path).unwrap());
        let store = SecretMasterStore::new(db).expect("open store");
        (store, dir)
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let (store, _dir) = temp_store();
        let record = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store.clone(), record);
        let secret_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let plaintext = b"cluster db password";

        let cipher = keyring
            .encrypt(secret_id, version_id, plaintext)
            .expect("encrypt");
        let recovered = keyring
            .decrypt(secret_id, version_id, &cipher)
            .expect("decrypt");
        assert_eq!(plaintext.as_ref(), recovered);
    }

    #[test]
    fn detect_digest_mismatch() {
        let (store, _dir) = temp_store();
        let record = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store.clone(), record);
        let secret_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let plaintext = b"mutable secret";

        let mut cipher = keyring
            .encrypt(secret_id, version_id, plaintext)
            .expect("encrypt");
        cipher.digest[0] ^= 0xff;

        let err = keyring
            .decrypt(secret_id, version_id, &cipher)
            .expect_err("digest mismatch must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn installing_new_master_keeps_previous_material_available() {
        let (store, _dir) = temp_store();
        let initial = store.ensure_current().expect("ensure master");
        let keyring = SecretKeyring::new(store.clone(), initial);

        let secret_id = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let payload = b"rotation payload";

        let cipher_old = keyring
            .encrypt(secret_id, version_id, payload)
            .expect("encrypt with initial key");
        assert_eq!(cipher_old.master_key_version, initial.version);

        let rotated = store.rotate().expect("rotate master key");
        keyring.install_current(rotated);

        let cipher_new = keyring
            .encrypt(secret_id, version_id, payload)
            .expect("encrypt with rotated key");
        assert_eq!(cipher_new.master_key_version, rotated.version);

        let plain_old = keyring
            .decrypt(secret_id, version_id, &cipher_old)
            .expect("decrypt with legacy key");
        assert_eq!(plain_old.as_slice(), payload);
    }
}
