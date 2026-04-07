use std::fmt;

use uuid::Uuid;
use x25519_dalek::PublicKey;

#[derive(Clone)]
pub struct PeerHandle {
    pub id: Uuid,
    pub hostname: String,
    pub address: String,
    pub root_hash: String,
    pub noise_static_pub: PublicKey,
}

impl fmt::Debug for PeerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Don’t print the capability; show useful fields only.
        f.debug_struct("PeerHandle")
            .field("id", &self.id)
            .field("hostname", &self.hostname)
            .field("address", &self.address)
            .field("root_hash", &self.root_hash)
            .field(
                "noise_static_pub_len",
                &self.noise_static_pub.to_bytes().len(),
            )
            .finish()
    }
}
