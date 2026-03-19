use anyhow::Result;
use capnp::Error as CapnpError;
use uuid::Uuid;

pub fn uuid_from_data(data: capnp::data::Reader) -> Result<Uuid, CapnpError> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| CapnpError::failed("invalid uuid".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

pub fn uuid_to_string(data: capnp::data::Reader) -> Result<String, CapnpError> {
    Ok(uuid_from_data(data)?.to_string())
}

pub fn uuid_short(data: capnp::data::Reader) -> Result<String, CapnpError> {
    let uuid = uuid_from_data(data)?;
    Ok(uuid
        .to_string()
        .split('-')
        .next()
        .unwrap_or_default()
        .to_string())
}
