#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PeerId(pub [u8; 32]);

pub fn peer_id_from_public(pk: &x25519_dalek::PublicKey) -> PeerId {
    let h = blake3::hash(pk.as_bytes());
    PeerId(*h.as_bytes())
}
