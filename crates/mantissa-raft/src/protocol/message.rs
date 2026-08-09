use super::{ProtocolError, ProtocolLimits};

/// Holds either a zero-copy aligned message or an owned aligned copy.
pub(crate) enum MessageReader<'a> {
    Borrowed(capnp::message::Reader<capnp::serialize::BufferSegments<&'a [u8]>>),
    Owned(capnp::message::Reader<capnp::serialize::OwnedSegments>),
}

impl MessageReader<'_> {
    /// Reads the message root as one generated Cap'n Proto type.
    pub(crate) fn root<'a, T>(&'a self) -> Result<T, capnp::Error>
    where
        T: capnp::traits::FromPointerReader<'a>,
    {
        match self {
            Self::Borrowed(message) => message.get_root(),
            Self::Owned(message) => message.get_root(),
        }
    }
}

/// Reads one bounded message without assuming its input address is aligned.
pub(crate) fn read_message(
    bytes: &[u8],
    limits: ProtocolLimits,
) -> Result<MessageReader<'_>, ProtocolError> {
    check_input_size(bytes, limits)?;

    let mut remaining = bytes;
    let message = if (bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<capnp::Word>()) {
        MessageReader::Borrowed(capnp::serialize::read_message_from_flat_slice(
            &mut remaining,
            limits.reader_options(),
        )?)
    } else {
        MessageReader::Owned(capnp::serialize::read_message(
            &mut remaining,
            limits.reader_options(),
        )?)
    };
    check_no_trailing_bytes(remaining)?;
    Ok(message)
}

/// Rejects an input before Cap'n Proto reads its segment table.
fn check_input_size(bytes: &[u8], limits: ProtocolLimits) -> Result<(), ProtocolError> {
    if bytes.len() > limits.max_message_bytes() {
        return Err(ProtocolError::MessageTooLarge {
            actual: bytes.len(),
            maximum: limits.max_message_bytes(),
        });
    }
    Ok(())
}

/// Rejects output that exceeded the limit while it was being built.
pub(crate) fn check_output_size(
    bytes: Vec<u8>,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, ProtocolError> {
    if bytes.len() > limits.max_message_bytes() {
        return Err(ProtocolError::MessageTooLarge {
            actual: bytes.len(),
            maximum: limits.max_message_bytes(),
        });
    }
    Ok(bytes)
}

/// Rejects bytes after the single expected Cap'n Proto message.
fn check_no_trailing_bytes(bytes: &[u8]) -> Result<(), ProtocolError> {
    if !bytes.is_empty() {
        return Err(ProtocolError::TrailingBytes {
            remaining: bytes.len(),
        });
    }
    Ok(())
}
