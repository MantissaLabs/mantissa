use serde::{Deserialize, Serialize};
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
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.0.cmp(&o.0)
    }
}

impl PartialOrd for UuidKey {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl std::hash::Hash for UuidKey {
    fn hash<H: std::hash::Hasher>(&self, st: &mut H) {
        self.0.hash(st)
    }
}

impl UuidKey {
    pub fn to_uuid(self) -> Uuid {
        Uuid::from_bytes(self.0)
    }
}
