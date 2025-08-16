pub type NodeId = uuid::Uuid;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct PeerValue {
    pub address: String,
    pub hostname: String,
    pub noise_static_pub: [u8; 32],
}
