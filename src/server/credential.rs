use crdt_store::codec;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Domain separation tag for the signed payload.
const DOMAIN: &[u8] = b"mantissa/cluster-cred/v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterCredential {
    /// Who signed this credential (their ed25519 verifying key).
    #[serde(with = "serde_ed25519::verifying_key")]
    pub issuer: VerifyingKey,

    /// The subject of the credential (the peer this is for).
    pub subject: Uuid,

    /// Expiry (unix seconds). Credential is invalid after this time.
    pub not_after: u64,

    /// Per-credential random to make blobs unique and replay-resistant.
    pub nonce: [u8; 16],

    /// Signature by `issuer` over `message(issuer, subject, not_after, nonce)`.
    #[serde(with = "serde_ed25519::signature")]
    pub sig: Signature,
}

impl ClusterCredential {
    /// Build canonical bytes to be signed/verified.
    fn message(issuer: &VerifyingKey, subject: &Uuid, not_after: u64, nonce: &[u8; 16]) -> Vec<u8> {
        let mut out = Vec::with_capacity(DOMAIN.len() + 32 + 16 + 8 + 16);
        out.extend_from_slice(DOMAIN);
        out.extend_from_slice(&issuer.to_bytes());
        out.extend_from_slice(subject.as_bytes());
        out.extend_from_slice(&not_after.to_le_bytes());
        out.extend_from_slice(nonce);
        out
    }

    /// Sign a fresh credential with `issuer_sk`.
    /// `ttl_secs`: validity window; `nonce`: random 16 bytes.
    pub fn sign(issuer_sk: &SigningKey, subject: Uuid, ttl_secs: u64, nonce: [u8; 16]) -> Self {
        let issuer = issuer_sk.verifying_key();
        let not_after = now_secs() + ttl_secs;
        let msg = Self::message(&issuer, &subject, not_after, &nonce);
        let sig = issuer_sk.sign(&msg);
        Self {
            issuer,
            subject,
            not_after,
            nonce,
            sig,
        }
    }

    /// Verify signature + expiry. Returns `Ok(())` if valid.
    pub fn verify(&self) -> Result<(), String> {
        if now_secs() > self.not_after {
            return Err("credential expired".to_string());
        }
        let msg = Self::message(&self.issuer, &self.subject, self.not_after, &self.nonce);
        self.issuer
            .verify(&msg, &self.sig)
            .map_err(|e| e.to_string())
    }

    /// Serialize to bytes (bincode).
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        codec::encode(self).map_err(|e| e.to_string())
    }

    /// Parse from bytes then verify.
    pub fn from_bytes_verified(b: &[u8]) -> Result<Self, String> {
        let cred: Self = codec::decode(b).map_err(|e| e.to_string())?;
        cred.verify()?;
        Ok(cred)
    }
}

fn now_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_secs(),
        Err(err) => {
            tracing::warn!("system clock error for credential verification: {err}");
            0
        }
    }
}

/// Minimal serde helpers to encode/decode ed25519 types as raw bytes.
/// Keeps the struct strongly typed while on-the-wire remains compact.
mod serde_ed25519 {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer, de};

    pub mod verifying_key {
        use super::*;
        pub fn serialize<S>(vk: &VerifyingKey, s: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            s.serialize_bytes(&vk.to_bytes())
        }
        pub fn deserialize<'de, D>(d: D) -> Result<VerifyingKey, D::Error>
        where
            D: Deserializer<'de>,
        {
            let bytes: Vec<u8> = Deserialize::deserialize(d)?;
            let arr: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| de::Error::custom("verifying key must be 32 bytes"))?;
            VerifyingKey::from_bytes(&arr).map_err(de::Error::custom)
        }
    }

    pub mod signature {
        use super::*;
        pub fn serialize<S>(sig: &Signature, s: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            s.serialize_bytes(&sig.to_bytes())
        }
        pub fn deserialize<'de, D>(d: D) -> Result<Signature, D::Error>
        where
            D: Deserializer<'de>,
        {
            let bytes: Vec<u8> = Deserialize::deserialize(d)?;
            Signature::from_slice(&bytes).map_err(de::Error::custom)
        }
    }
}
