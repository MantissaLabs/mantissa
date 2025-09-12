use protocol::topology::node_info::Reader as NodeInfo;
use capnp::Error as CapnpError;
use uuid::Uuid;

/// Generate a **time-ordered** UUID (v7).
#[inline]
pub fn new_node_id_v7() -> Uuid {
    Uuid::now_v7()
}

/// Convert UUID -> 16 bytes (big-endian, canonical)
#[inline]
pub fn uuid_to_bytes(u: &Uuid) -> [u8; 16] {
    *u.as_bytes()
}

/// Convert 16 bytes -> UUID
#[inline]
pub fn uuid_from_bytes(b: &[u8]) -> Result<Uuid, String> {
    Uuid::from_slice(b).map_err(|e| e.to_string())
}

/// Sort key: interpret the 16 bytes as a big-endian u128.
/// (For UUIDv7, this preserves chronological order.)
#[inline]
pub fn uuid_sort_key(u: &Uuid) -> u128 {
    u128::from_be_bytes(*u.as_bytes())
}

/// Set NodeId bytes into a builder (validates length via UUID first if you want).
#[inline]
pub fn set_node_id(mut id_builder: protocol::node::node_id::Builder, id: &Uuid) {
    id_builder.set_bytes(&uuid_to_bytes(id));
}

/// Read NodeId (bytes) from a reader and return a Uuid.
#[inline]
pub fn read_node_id(id_reader: protocol::node::node_id::Reader) -> Result<Uuid, CapnpError> {
    let bytes = id_reader.get_bytes()?; // &[u8]
    uuid_from_bytes(bytes).map_err(|e| CapnpError::failed(e))
}

/// Map a NodeInfo reader to a sortable u128 key (UUID bytes, big-endian).
/// On any parse error, returns u128::MAX to push the row to the end.
#[inline]
pub fn id_sort_key_uuid_bytes(n: &NodeInfo) -> u128 {
    match n
        .get_id()
        .and_then(|id| id.get_bytes())
        .ok()
        .and_then(|b| Uuid::from_slice(b).ok())
    {
        Some(u) => u128::from_be_bytes(*u.as_bytes()),
        None => u128::MAX,
    }
}

/// Pretty string for the UUID in NodeInfo (for printing)
#[inline]
pub fn id_string(n: &NodeInfo) -> Result<String, CapnpError> {
    let bytes = n.get_id()?.get_bytes()?;
    let u = Uuid::from_slice(bytes).map_err(|e| CapnpError::failed(e.to_string()))?;
    Ok(u.to_string())
}
