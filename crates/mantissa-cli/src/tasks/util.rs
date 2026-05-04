use capnp::Error as CapnpError;
use mantissa_protocol::task::TaskLogStream;
use std::io::{self, Write};

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
