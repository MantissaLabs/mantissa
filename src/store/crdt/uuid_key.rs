use crate::store::crdt::key::KeyFromSlice;
use serde::{Deserialize, Serialize};
use std::io;
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

impl<'a> TryFrom<&'a [u8]> for UuidKey {
    type Error = io::Error;
    fn try_from(b: &'a [u8]) -> io::Result<Self> {
        if b.len() != 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UuidKey: expected 16 bytes",
            ));
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(b);
        Ok(UuidKey(arr))
    }
}

impl UuidKey {
    pub fn from_bytes(b: [u8; 16]) -> Self {
        Self(b)
    }

    pub fn to_uuid(self) -> Uuid {
        Uuid::from_bytes(self.0)
    }
}

impl KeyFromSlice for UuidKey {
    fn from_slice(b: &[u8]) -> Self {
        let arr: [u8; 16] = b.try_into().expect("UuidKey requires 16 bytes");
        Self(arr)
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
