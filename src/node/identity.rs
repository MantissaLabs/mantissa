use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use std::convert::TryInto;
use uuid::Uuid;
use x25519_dalek::PublicKey;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PeerId(pub [u8; 32]);

#[allow(dead_code)]
pub fn peer_id_from_public(pk: &x25519_dalek::PublicKey) -> PeerId {
    let h = blake3::hash(pk.as_bytes());
    PeerId(*h.as_bytes())
}

const PEER_IDENTITY_DOMAIN: &[u8] = b"MANTISSA|peer-identity|v1";

/// Build the canonical byte payload for peer identity signatures.
/// This binds the node id to both public keys to prevent gossip tampering.
pub fn peer_identity_payload(
    node_id: &Uuid,
    noise_static_pub: &[u8; 32],
    signing_pub: &[u8; 32],
) -> Vec<u8> {
    // Layout is fixed and versioned so signatures can't be replayed across protocols.
    let mut out = Vec::with_capacity(
        PEER_IDENTITY_DOMAIN.len()
            + node_id.as_bytes().len()
            + noise_static_pub.len()
            + signing_pub.len(),
    );
    out.extend_from_slice(PEER_IDENTITY_DOMAIN);
    out.extend_from_slice(node_id.as_bytes());
    out.extend_from_slice(noise_static_pub);
    out.extend_from_slice(signing_pub);
    out
}

/// Sign the peer identity payload using the local signing key.
/// This produces a 64-byte Ed25519 signature suitable for NodeInfo.identitySig.
pub fn sign_peer_identity(
    signing_key: &SigningKey,
    node_id: &Uuid,
    noise_static_pub: &[u8; 32],
    signing_pub: &[u8; 32],
) -> [u8; 64] {
    let payload = peer_identity_payload(node_id, noise_static_pub, signing_pub);
    let sig: Signature = signing_key.sign(&payload);
    sig.to_bytes()
}

/// Verify a peer identity signature against the provided signing key.
/// This rejects missing or malformed signatures to prevent identity substitution.
pub fn verify_peer_identity(
    signing_pub: &VerifyingKey,
    node_id: &Uuid,
    noise_static_pub: &[u8; 32],
    identity_sig: &[u8],
) -> Result<(), &'static str> {
    let sig =
        Signature::from_slice(identity_sig).map_err(|_| "identity signature must be 64 bytes")?;
    let signing_pub_bytes = signing_pub.to_bytes();
    let payload = peer_identity_payload(node_id, noise_static_pub, &signing_pub_bytes);
    signing_pub
        .verify_strict(&payload, &sig)
        .map_err(|_| "invalid peer identity signature")
}

pub fn pubkey_from_slice(bytes: &[u8]) -> Result<PublicKey, &'static str> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "x25519 public key must be exactly 32 bytes")?;
    Ok(PublicKey::from(arr))
}
