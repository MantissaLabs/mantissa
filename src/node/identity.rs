use std::convert::TryInto;
use x25519_dalek::PublicKey;

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PeerId(pub [u8; 32]);

#[allow(dead_code)]
pub fn peer_id_from_public(pk: &x25519_dalek::PublicKey) -> PeerId {
    let h = blake3::hash(pk.as_bytes());
    PeerId(*h.as_bytes())
}

pub fn pubkey_from_slice(bytes: &[u8]) -> Result<PublicKey, &'static str> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "x25519 public key must be exactly 32 bytes")?;
    Ok(PublicKey::from(arr))
}
