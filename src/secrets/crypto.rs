use crate::secrets::types::SecretCiphertext;
use blake3::Hash;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use hkdf::Hkdf;
use sha2::Sha256;
use std::io;
use uuid::Uuid;

const MASTER_KDF_SALT: &[u8] = b"mantissa.secret.master.salt.v1";
const MASTER_KDF_INFO: &[u8] = b"mantissa/secret/master/key";
const AAD_PREFIX: &[u8] = b"mantissa.secret.v1";

/// In-memory key material used to encrypt and decrypt secret payloads.
#[derive(Clone)]
pub struct SecretKeyring {
    master_key: [u8; 32],
}

impl SecretKeyring {
    /// Derives a deterministic 256-bit master key from the current join token.
    pub fn derive_from_token(token: &str) -> io::Result<Self> {
        if token.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "join token cannot be empty when deriving secret keyring",
            ));
        }

        let hk = Hkdf::<Sha256>::new(Some(MASTER_KDF_SALT), token.as_bytes());
        let mut master_key = [0u8; 32];
        hk.expand(MASTER_KDF_INFO, &mut master_key)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "hkdf expand failed"))?;
        Ok(Self { master_key })
    }

    /// Returns the raw master key bytes (used for tests or advanced integrations).
    pub fn master_key(&self) -> &[u8; 32] {
        &self.master_key
    }

    /// Encrypts `plaintext` for the provided secret/version identifiers.
    pub fn encrypt(
        &self,
        secret_id: Uuid,
        version_id: Uuid,
        plaintext: &[u8],
    ) -> io::Result<SecretCiphertext> {
        let nonce = Self::random_nonce()?;
        let aead = ChaCha20Poly1305::new(Key::from_slice(&self.master_key));
        let aad = Self::aad(secret_id, version_id);
        let ciphertext = aead
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "secret encryption failed"))?;

        let digest = Self::digest_bytes(blake3::hash(plaintext));

        Ok(SecretCiphertext {
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
        let aead = ChaCha20Poly1305::new(Key::from_slice(&self.master_key));
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

    fn digest_bytes(hash: Hash) -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(hash.as_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::SecretKeyring;
    use uuid::Uuid;

    #[test]
    fn derive_from_token_is_deterministic() {
        let a = SecretKeyring::derive_from_token("MNTISA-1-abc234").unwrap();
        let b = SecretKeyring::derive_from_token("MNTISA-1-abc234").unwrap();
        let c = SecretKeyring::derive_from_token("MNTISA-1-different").unwrap();

        assert_eq!(a.master_key(), b.master_key());
        assert_ne!(a.master_key(), c.master_key());
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let keyring = SecretKeyring::derive_from_token("MNTISA-1-super-secret").unwrap();
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
        let keyring = SecretKeyring::derive_from_token("MNTISA-1-super-secret").unwrap();
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
}
