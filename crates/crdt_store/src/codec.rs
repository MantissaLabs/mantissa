use serde::{Serialize, de::DeserializeOwned};

use crate::error::Error;

/// Magic prefix used to identify Mantissa's versioned binary envelope.
const VERSIONED_PAYLOAD_MAGIC: &[u8; 8] = b"mntsbin\0";
/// Current binary payload envelope version.
const VERSIONED_PAYLOAD_VERSION: u8 = 1;

/// Encodes one value into Mantissa's versioned binary envelope.
///
/// The envelope gives every persisted or relayed blob a self-describing header
/// while keeping the existing bincode payload format underneath. Future
/// releases can add new envelope versions without overloading one raw byte
/// stream format for multiple protocol eras.
///
/// Append-only payload evolution still relies on serde defaults for newly added
/// fields because bincode itself does not carry schema information.
pub fn encode<T>(value: &T) -> crate::Result<Vec<u8>>
where
    T: Serialize,
{
    let payload = bincode::serialize(value).map_err(Error::from)?;
    let mut out = Vec::with_capacity(VERSIONED_PAYLOAD_MAGIC.len() + 1 + payload.len());
    out.extend_from_slice(VERSIONED_PAYLOAD_MAGIC);
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
/// Append-only payload evolution still relies on serde defaults for newly
/// added fields, and older readers still benefit from bincode tolerating
/// trailing bytes inside the envelope payload.
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
    if !bytes.starts_with(VERSIONED_PAYLOAD_MAGIC) {
        return Err(Box::new(Error::Other(
            "binary payload missing Mantissa envelope header".to_string(),
        )));
    }

    if bytes.len() <= VERSIONED_PAYLOAD_MAGIC.len() {
        return Err(Box::new(Error::Other(
            "versioned binary payload missing envelope version".to_string(),
        )));
    }

    let version = bytes[VERSIONED_PAYLOAD_MAGIC.len()];
    if version != VERSIONED_PAYLOAD_VERSION {
        return Err(Box::new(Error::Other(format!(
            "unsupported binary payload envelope version {version}"
        ))));
    }

    Ok(&bytes[VERSIONED_PAYLOAD_MAGIC.len() + 1..])
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    /// Test payload used to prove the codec preserves round trips.
    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct SamplePayload {
        value: u64,
    }

    /// Evolved payload shape used to prove append-only fields can be defaulted.
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

    /// Additive field evolution must continue to work inside the versioned envelope.
    #[test]
    fn codec_reads_new_payloads_with_older_structs() {
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

    /// Newer structs must accept older envelope payloads when added fields default cleanly.
    #[test]
    fn codec_reads_old_payloads_with_newer_structs() {
        #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
        struct OlderPayload {
            value: u64,
        }

        let encoded = super::encode(&OlderPayload { value: 11 }).expect("encode older payload");

        let decoded: EvolvedPayload = super::decode(&encoded).expect("decode newer payload");

        assert_eq!(
            decoded,
            EvolvedPayload {
                value: 11,
                enabled: false,
            }
        );
    }
}
