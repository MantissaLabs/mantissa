use anyhow::Result;
use capnp::Error as CapnpError;
use mantissa_protocol::task::TaskLogStream;
use std::io::{self, Write};
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

/// Writes one streamed task output frame to stdout or stderr without reformatting the payload.
pub(super) fn write_frame(stream: TaskLogStream, bytes: &[u8]) -> Result<(), CapnpError> {
    match stream {
        TaskLogStream::Stdout | TaskLogStream::Console => {
            let mut stdout = io::stdout();
            stdout
                .write_all(bytes)
                .map_err(|err| CapnpError::failed(err.to_string()))?;
            stdout
                .flush()
                .map_err(|err| CapnpError::failed(err.to_string()))?;
        }
        TaskLogStream::Stderr => {
            let mut stderr = io::stderr();
            stderr
                .write_all(bytes)
                .map_err(|err| CapnpError::failed(err.to_string()))?;
            stderr
                .flush()
                .map_err(|err| CapnpError::failed(err.to_string()))?;
        }
    }

    Ok(())
}
