use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalNodeInfo {
    pub id: Uuid,
    pub hostname: String,
    pub address: String,
    pub noise_static_pub: [u8; 32],
}
