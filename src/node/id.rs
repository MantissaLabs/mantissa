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

/// Set NodeId bytes into a builder (validates length via UUID first if you want).
#[inline]
pub fn set_node_id(mut id_builder: mantissa_protocol::node::node_id::Builder, id: &Uuid) {
    id_builder.set_bytes(&uuid_to_bytes(id));
}

/// Read NodeId (bytes) from a reader and return a Uuid.
#[inline]
pub fn read_node_id(
    id_reader: mantissa_protocol::node::node_id::Reader,
) -> Result<Uuid, CapnpError> {
    let bytes = id_reader.get_bytes()?; // &[u8]
    uuid_from_bytes(bytes).map_err(CapnpError::failed)
}
