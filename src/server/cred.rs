use crate::noise::NoiseKeys;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::io;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[inline]
fn ioerr<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

const CRED_AUD: &[u8] = b"mantissa/session/bootstrap/v1";
const CRED_SALT: &[u8] = b"mantissa.issuer.v1";
const CRED_INFO: &[u8] = b"ed25519-issuer-key";
pub const CRED_TTL_SECS: u64 = 10 * 60; // 10 minutes

#[derive(Serialize, Deserialize)]
struct CredPayload {
    sub: [u8; 16], // peer uuid
    exp: u64,      // unix seconds
    aud: Vec<u8>,  // audience tag
}

/// Deterministically derive an Ed25519 SigningKey from the Noise static private key.
/// Good enough for now, later we could store a true cluster issuer key.
fn derive_issuer(noise: &NoiseKeys) -> Result<SigningKey, io::Error> {
    let sk = noise.to_private_bytes(); // [u8;32]
    let hk = Hkdf::<Sha256>::new(Some(CRED_SALT), &sk);
    let mut seed = [0u8; SECRET_KEY_LENGTH];
    hk.expand(CRED_INFO, &mut seed)
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "hkdf expand"))?;
    Ok(SigningKey::from_bytes(&seed))
}

#[inline]
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Issue a short-lived credential for `peer_id` using the local issuer (this node).
pub fn issue_credential(
    noise: &NoiseKeys,
    peer_id: uuid::Uuid,
    ttl_secs: u64,
) -> Result<Vec<u8>, io::Error> {
    let sk = derive_issuer(noise)?;
    let payload = CredPayload {
        sub: *peer_id.as_bytes(),
        exp: now_secs() + ttl_secs,
        aud: CRED_AUD.to_vec(),
    };
    let buf = bincode::serialize(&payload).map_err(ioerr)?;
    let sig: Signature = sk.sign(&buf);
    // blob = len(payload u32 LE) || payload || sig(64)
    let mut out = Vec::with_capacity(4 + buf.len() + 64);
    out.extend_from_slice(&(buf.len() as u32).to_le_bytes());
    out.extend_from_slice(&buf);
    out.extend_from_slice(&sig.to_bytes());
    Ok(out)
}

/// Verify a presented credential (local verification).
/// Returns `peer_id` on success.
pub fn verify_credential(noise: &NoiseKeys, blob: &[u8]) -> Result<uuid::Uuid, io::Error> {
    if blob.len() < 4 + 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "credential too small",
        ));
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&blob[..4]);
    let plen = u32::from_le_bytes(len_bytes) as usize;
    if blob.len() != 4 + plen + 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "credential format",
        ));
    }
    let payload_bytes = &blob[4..4 + plen];
    let sig_bytes = &blob[4 + plen..];
    let sk = derive_issuer(noise)?; // to get verifying key deterministically
    let vk: VerifyingKey = sk.verifying_key();

    let sig = Signature::from_bytes(
        sig_bytes
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "sig size"))?,
    );
    vk.verify(payload_bytes, &sig)
        .map_err(|_| io::Error::new(io::ErrorKind::PermissionDenied, "invalid credential sig"))?;

    let payload: CredPayload = bincode::deserialize(payload_bytes).map_err(ioerr)?;
    if payload.aud != CRED_AUD {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "invalid audience",
        ));
    }
    if now_secs() > payload.exp {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "credential expired",
        ));
    }
    Ok(uuid::Uuid::from_bytes(payload.sub))
}
