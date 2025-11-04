use blake3::Hash;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

/// Metadata associated with a secret that does not disclose its plaintext.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretMetadata {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

/// Authenticated ciphertext envelope for a single secret version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretCiphertext {
    /// Identifier of the master key version used for encryption.
    pub master_key_version: u64,
    /// Random nonce used for ChaCha20-Poly1305.
    pub nonce: [u8; 12],
    /// AEAD ciphertext bytes (contains the Poly1305 tag).
    #[serde(with = "serde_bytes")]
    pub ciphertext: Vec<u8>,
    /// Blake3 digest of the plaintext to detect tampering once decrypted.
    pub digest: [u8; 32],
}

impl SecretCiphertext {
    /// Returns the Blake3 digest recorded when encrypting the plaintext.
    #[allow(dead_code)]
    pub fn digest(&self) -> Hash {
        Hash::from_bytes(self.digest)
    }
}

/// Versioned payload for a secret, including authorship and timestamps.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretVersion {
    pub version_id: Uuid,
    pub ciphertext: SecretCiphertext,
    pub created_at: String,
    #[serde(default)]
    pub created_by: Option<Uuid>,
    pub master_key_version: u64,
}

impl SecretVersion {
    /// Builds a new version record from encrypted material and metadata.
    pub fn new(
        version_id: Uuid,
        ciphertext: SecretCiphertext,
        created_at: impl Into<String>,
        created_by: Option<Uuid>,
        master_key_version: u64,
    ) -> Self {
        Self {
            version_id,
            ciphertext,
            created_at: created_at.into(),
            created_by,
            master_key_version,
        }
    }
}

/// Durable CRDT value representing the latest state of a secret.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SecretValue {
    pub id: Uuid,
    pub name: String,
    #[serde(default)]
    pub metadata: SecretMetadata,
    pub created_at: String,
    pub updated_at: String,
    pub current_version: SecretVersion,
}

impl SecretValue {
    /// Constructs a new secret value with deterministic identifier derived from `name`.
    pub fn new(
        name: impl Into<String>,
        metadata: SecretMetadata,
        created_at: impl Into<String>,
        version: SecretVersion,
    ) -> Self {
        let name = name.into();
        let id = compute_secret_id(&name);
        let created_at = created_at.into();
        Self {
            id,
            name,
            metadata,
            created_at: created_at.clone(),
            updated_at: created_at,
            current_version: version,
        }
    }

    /// Updates the `updated_at` timestamp to reflect a new logical modification.
    pub fn touch(&mut self, timestamp: impl Into<String>) {
        self.updated_at = timestamp.into();
    }

    /// Replaces the active secret version and updates the modification timestamp.
    pub fn set_version(&mut self, version: SecretVersion, timestamp: impl Into<String>) {
        self.current_version = version;
        self.touch(timestamp);
    }

    /// Returns a reference to the currently active version.
    #[allow(dead_code)]
    pub fn version(&self) -> &SecretVersion {
        &self.current_version
    }
}

/// Gossip event describing how the secret registry should change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SecretEvent {
    Upsert(Box<SecretValue>),
    Remove(Uuid),
}

/// Computes a deterministic secret identifier from its logical name.
pub fn compute_secret_id(name: &str) -> Uuid {
    let digest = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::{SecretCiphertext, SecretMetadata, SecretValue, SecretVersion, compute_secret_id};
    use chrono::Utc;

    #[test]
    fn compute_secret_id_stable() {
        let a = compute_secret_id("db-password");
        let b = compute_secret_id("db-password");
        let c = compute_secret_id("api-key");

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn secret_value_tracks_updates() {
        let metadata = SecretMetadata::default();
        let ciphertext = SecretCiphertext {
            master_key_version: 1,
            nonce: [0u8; 12],
            ciphertext: vec![1, 2, 3],
            digest: [9u8; 32],
        };
        let version = SecretVersion::new(
            uuid::Uuid::new_v4(),
            ciphertext,
            Utc::now().to_rfc3339(),
            None,
            1,
        );

        let created_at = "2024-01-01T00:00:00Z".to_string();
        let mut value = SecretValue::new(
            "db-password",
            metadata.clone(),
            created_at.clone(),
            version.clone(),
        );

        assert_eq!(value.created_at, created_at);
        assert_eq!(value.updated_at, created_at);
        assert_eq!(value.metadata, metadata);
        assert_eq!(value.version().version_id, version.version_id);

        let new_version = SecretVersion::new(
            uuid::Uuid::new_v4(),
            SecretCiphertext {
                master_key_version: 2,
                nonce: [1u8; 12],
                ciphertext: vec![4, 5, 6],
                digest: [8u8; 32],
            },
            "2024-02-02T00:00:00Z",
            Some(uuid::Uuid::new_v4()),
            2,
        );
        value.set_version(new_version.clone(), "2024-02-02T00:00:00Z");

        assert_eq!(value.updated_at, "2024-02-02T00:00:00Z");
        assert_eq!(value.version().version_id, new_version.version_id);
    }
}
