use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UuidKey([u8; 16]);

impl From<Uuid> for UuidKey {
    fn from(u: Uuid) -> Self {
        Self(*u.as_bytes())
    }
}

impl AsRef<[u8]> for UuidKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Ord for UuidKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}
impl PartialOrd for UuidKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Hash for UuidKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}
