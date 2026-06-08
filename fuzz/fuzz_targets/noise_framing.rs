#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa_net::noise::{derive_psk_from_token, read_framed_len, write_framed};
use std::io::ErrorKind;
use tokio::io::AsyncWriteExt;

const MAX_PAYLOAD_BYTES: usize = 8 * 1024;
const MAX_RAW_FRAME_BYTES: usize = 8 * 1024;
const MAX_TOKEN_BYTES: usize = 1024;
const OVERSIZED_FRAME_LEN: usize = (u16::MAX as usize) + 1;
static OVERSIZED_FRAME: [u8; OVERSIZED_FRAME_LEN] = [0; OVERSIZED_FRAME_LEN];

#[derive(Arbitrary, Debug)]
struct NoiseFramingInput {
    payload: Vec<u8>,
    raw_frame: Vec<u8>,
    token_bytes: Vec<u8>,
    check_oversized_write: bool,
}

fuzz_target!(|data: &[u8]| {
    assert_token_psk_derivation(data);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("Tokio runtime should build for noise framing fuzzing");

    runtime.block_on(async {
        assert_raw_frame_reader_does_not_panic(data).await;

        let mut unstructured = Unstructured::new(data);
        let Ok(input) = NoiseFramingInput::arbitrary(&mut unstructured) else {
            return;
        };

        assert_token_psk_derivation(&input.token_bytes);
        assert_valid_frame_roundtrips(&input.payload).await;
        assert_raw_frame_reader_does_not_panic(&input.raw_frame).await;

        if input.check_oversized_write {
            assert_oversized_frame_is_rejected().await;
        }
    });
});

/// Verifies token PSK derivation rejects empty tokens and stays deterministic.
fn assert_token_psk_derivation(token_bytes: &[u8]) {
    let token_bytes = bounded_bytes(token_bytes, MAX_TOKEN_BYTES);
    let token = String::from_utf8_lossy(&token_bytes);
    let derived = derive_psk_from_token(token.as_ref());

    if token.is_empty() {
        assert_eq!(
            derived
                .expect_err("empty join token should be rejected")
                .kind(),
            ErrorKind::InvalidInput
        );
        return;
    }

    let psk = derived.expect("non-empty join token should derive a PSK");
    let repeated = derive_psk_from_token(token.as_ref())
        .expect("same non-empty join token should derive a PSK twice");
    assert_eq!(psk, repeated);
}

/// Verifies a bounded valid frame survives write and read framing unchanged.
async fn assert_valid_frame_roundtrips(payload: &[u8]) {
    let payload = bounded_bytes(payload, MAX_PAYLOAD_BYTES);
    let capacity = payload.len() + 3;
    let (mut writer, mut reader) = tokio::io::duplex(capacity);

    write_framed(&mut writer, &payload)
        .await
        .expect("bounded noise frame should write");

    let mut decoded = Vec::new();
    let decoded_len = read_framed_len(&mut reader, &mut decoded)
        .await
        .expect("freshly written noise frame should read");

    assert_eq!(decoded_len, payload.len());
    assert_eq!(&decoded[..decoded_len], payload.as_slice());
}

/// Exercises the public frame reader with arbitrary length-prefixed bytes.
async fn assert_raw_frame_reader_does_not_panic(raw_frame: &[u8]) {
    let raw_frame = bounded_bytes(raw_frame, MAX_RAW_FRAME_BYTES);
    let capacity = raw_frame.len() + 1;
    let (mut writer, mut reader) = tokio::io::duplex(capacity);

    writer
        .write_all(&raw_frame)
        .await
        .expect("bounded raw frame should fit the in-memory transport");
    drop(writer);

    let mut decoded = Vec::new();
    if let Ok(decoded_len) = read_framed_len(&mut reader, &mut decoded).await {
        assert!(raw_frame.len() >= 2);

        let declared_len = usize::from(u16::from_be_bytes([raw_frame[0], raw_frame[1]]));
        assert_eq!(decoded_len, declared_len);
        assert_eq!(&decoded[..decoded_len], &raw_frame[2..2 + declared_len]);
    }
}

/// Verifies oversized handshake frames fail before writing to the transport.
async fn assert_oversized_frame_is_rejected() {
    let mut sink = tokio::io::sink();
    let err = write_framed(&mut sink, &OVERSIZED_FRAME)
        .await
        .expect_err("oversized noise frame should be rejected");
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

/// Returns at most `limit` bytes from generated input.
fn bounded_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
    bytes.iter().copied().take(limit).collect()
}
