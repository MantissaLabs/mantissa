use serde::{Serialize, de::DeserializeOwned};

use crate::error::Error;

/// Prefix tag used to identify Mantissa's versioned binary envelope.
const VERSIONED_PAYLOAD_TAG: &[u8; 8] = b"mntsbin\0";
/// Current binary payload envelope version.
const VERSIONED_PAYLOAD_VERSION: u8 = 1;

/// Encodes one value into Mantissa's versioned binary envelope.
///
/// The envelope gives every persisted or relayed blob a self-describing header
/// while keeping the existing bincode payload format underneath. Future
/// releases can add new envelope versions without overloading one raw byte
/// stream format for multiple protocol eras.
///
/// The envelope version is not a schema migration by itself. Any persisted
/// struct shape that changes still needs an explicit decode path for the older
/// shape before this function can safely write the newer shape.
pub fn encode<T>(value: &T) -> crate::Result<Vec<u8>>
where
    T: Serialize,
{
    let payload = bincode::serialize(value).map_err(Error::from)?;
    let mut out = Vec::with_capacity(VERSIONED_PAYLOAD_TAG.len() + 1 + payload.len());
    out.extend_from_slice(VERSIONED_PAYLOAD_TAG);
    out.push(VERSIONED_PAYLOAD_VERSION);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decodes one value from Mantissa's versioned binary envelope.
///
/// This is a hard cutover codec: callers must provide the Mantissa envelope.
/// From this change onward, all persisted and relayed opaque payloads are
/// expected to use this format so upgrade behavior remains easy to reason
/// about.
///
/// Bincode still defines the inner payload semantics. That means simple older
/// readers may ignore trailing bytes in some top-level cases, but newer readers
/// do not automatically recover missing fields from older payloads.
pub fn decode<T>(bytes: &[u8]) -> crate::Result<T>
where
    T: DeserializeOwned,
{
    let payload = decode_versioned_payload(bytes)?;
    bincode::deserialize(payload)
        .map_err(Error::from)
        .map_err(Box::new)
}

/// Returns the inner payload slice when `bytes` use Mantissa's versioned envelope.
fn decode_versioned_payload(bytes: &[u8]) -> crate::Result<&[u8]> {
    if !bytes.starts_with(VERSIONED_PAYLOAD_TAG) {
        return Err(Box::new(Error::Other(
            "binary payload missing Mantissa envelope header".to_string(),
        )));
    }

    if bytes.len() <= VERSIONED_PAYLOAD_TAG.len() {
        return Err(Box::new(Error::Other(
            "versioned binary payload missing envelope version".to_string(),
        )));
    }

    let version = bytes[VERSIONED_PAYLOAD_TAG.len()];
    if version != VERSIONED_PAYLOAD_VERSION {
        return Err(Box::new(Error::Other(format!(
            "unsupported binary payload envelope version {version}"
        ))));
    }

    Ok(&bytes[VERSIONED_PAYLOAD_TAG.len() + 1..])
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    /// Test payload used to prove the codec preserves round trips.
    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct SamplePayload {
        value: u64,
    }

    /// Evolved payload shape used by narrow inner-bincode behavior tests.
    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct EvolvedPayload {
        value: u64,
        #[serde(default)]
        enabled: bool,
    }

    /// Versioned payloads must round-trip through the codec without losing bytes.
    #[test]
    fn codec_roundtrips_versioned_payloads() {
        let payload = SamplePayload { value: 7 };

        let encoded = super::encode(&payload).expect("encode payload");
        let decoded: SamplePayload = super::decode(&encoded).expect("decode payload");

        assert_eq!(decoded, payload);
    }

    /// Decoding must reject payloads that bypass the versioned Mantissa envelope.
    #[test]
    fn codec_rejects_unversioned_payloads() {
        let payload = SamplePayload { value: 42 };
        let encoded = bincode::serialize(&payload).expect("encode raw payload");

        let error = super::decode::<SamplePayload>(&encoded).expect_err("reject raw payload");
        assert!(
            error
                .to_string()
                .contains("binary payload missing Mantissa envelope header"),
            "unexpected error: {error}"
        );
    }

    /// Decoding must reject envelopes with versions this binary does not understand.
    #[test]
    fn codec_rejects_unsupported_envelope_versions() {
        let payload = SamplePayload { value: 42 };
        let mut encoded = super::encode(&payload).expect("encode payload");
        encoded[super::VERSIONED_PAYLOAD_TAG.len()] =
            super::VERSIONED_PAYLOAD_VERSION.saturating_add(1);

        let error =
            super::decode::<SamplePayload>(&encoded).expect_err("reject future envelope version");
        assert!(
            error
                .to_string()
                .contains("unsupported binary payload envelope version"),
            "unexpected error: {error}"
        );
    }

    /// Older top-level structs can ignore trailing bincode bytes in this narrow case.
    #[test]
    fn codec_allows_simple_older_reader_to_ignore_trailing_payload_bytes() {
        let encoded = super::encode(&EvolvedPayload {
            value: 19,
            enabled: true,
        })
        .expect("encode evolved payload");

        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        struct OlderPayload {
            value: u64,
        }

        let decoded: OlderPayload = super::decode(&encoded).expect("decode older payload");

        assert_eq!(decoded, OlderPayload { value: 19 });
    }

    /// Older envelope payloads still need explicit version-aware migration when
    /// newer structs add fields.
    #[test]
    fn codec_rejects_old_payloads_with_newer_structs_without_migration() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        struct OlderPayload {
            value: u64,
        }

        let encoded = super::encode(&OlderPayload { value: 11 }).expect("encode older payload");

        let error = super::decode::<EvolvedPayload>(&encoded)
            .expect_err("older payload should require migration");
        assert!(
            error.to_string().contains("unexpected end of file"),
            "unexpected error: {error}"
        );
    }
}
