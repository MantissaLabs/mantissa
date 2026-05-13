use crate::node::id::{read_node_id, set_node_id};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::io::Cursor;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Domain separation tag for the signed payload.
const DOMAIN: &[u8] = b"mantissa/cluster-cred/v1";

#[derive(Clone, Debug)]
pub struct ClusterCredential {
    /// Who signed this credential (their ed25519 verifying key).
    pub issuer: VerifyingKey,

    /// The subject of the credential (the peer this is for).
    pub subject: Uuid,

    /// Expiry (unix seconds). Credential is invalid after this time.
    pub not_after: u64,

    /// Per-credential random to make blobs unique and replay-resistant.
    pub nonce: [u8; 16],

    /// Signature by `issuer` over `message(issuer, subject, not_after, nonce)`.
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
        if now_secs() >= self.not_after {
            return Err("credential expired".to_string());
        }
        let msg = Self::message(&self.issuer, &self.subject, self.not_after, &self.nonce);
        self.issuer
            .verify(&msg, &self.sig)
            .map_err(|e| e.to_string())
    }

    /// Serializes this credential into its stable Cap'n Proto payload.
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let mut message = capnp::message::Builder::new_default();
        let mut builder =
            message.init_root::<mantissa_protocol::server::cluster_credential::Builder<'_>>();
        builder.set_issuer(&self.issuer.to_bytes());
        set_node_id(builder.reborrow().init_subject(), &self.subject);
        builder.set_not_after_unix_secs(self.not_after);
        builder.set_nonce(&self.nonce);
        builder.set_signature(&self.sig.to_bytes());
        Ok(capnp::serialize::write_message_to_words(&message))
    }

    /// Parses one Cap'n Proto payload from bytes and then verifies it.
    pub fn from_bytes_verified(b: &[u8]) -> Result<Self, String> {
        let cred = Self::from_capnp_bytes(b)?;
        cred.verify()?;
        Ok(cred)
    }

    /// Decodes one credential from its stable Cap'n Proto payload.
    fn from_capnp_bytes(bytes: &[u8]) -> Result<Self, String> {
        let mut cursor = Cursor::new(bytes);
        let reader =
            capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
                .map_err(|e| e.to_string())?;
        let credential = reader
            .get_root::<mantissa_protocol::server::cluster_credential::Reader<'_>>()
            .map_err(|e| e.to_string())?;
        Self::from_capnp(credential)
    }

    /// Decodes one credential from a Cap'n Proto reader.
    fn from_capnp(
        reader: mantissa_protocol::server::cluster_credential::Reader<'_>,
    ) -> Result<Self, String> {
        let issuer = read_fixed_bytes::<32>(reader.get_issuer().map_err(|e| e.to_string())?)
            .and_then(|bytes| VerifyingKey::from_bytes(&bytes).map_err(|e| e.to_string()))?;
        let subject = read_node_id(reader.get_subject().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let nonce = read_fixed_bytes::<16>(reader.get_nonce().map_err(|e| e.to_string())?)?;
        let sig = Signature::from_slice(reader.get_signature().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;

        Ok(Self {
            issuer,
            subject,
            not_after: reader.get_not_after_unix_secs(),
            nonce,
            sig,
        })
    }
}

/// Reads a fixed-width Cap'n Proto data field into an array.
fn read_fixed_bytes<const N: usize>(bytes: &[u8]) -> Result<[u8; N], String> {
    bytes
        .try_into()
        .map_err(|_| format!("expected {N} bytes, got {}", bytes.len()))
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
