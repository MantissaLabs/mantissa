#![no_main]

use std::net::SocketAddr;

use libfuzzer_sys::fuzz_target;
use mantissa::agents;
use mantissa::jobs;
use mantissa::node::address::extract_port;
use mantissa::token::is_valid_format;

const MAX_TEXT_BYTES: usize = 512;
const TOKEN_PREFIX: &str = "MNTISA-1-";
const BASE32_LOWER_NOPAD: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

fuzz_target!(|data: &[u8]| {
    let input = ParserInput::from_bytes(data);
    input.assert_token_contract();
    input.assert_port_extraction_contract();
    input.assert_timestamp_contract();
    input.assert_optional_text_contract();
});

#[derive(Debug)]
struct ParserInput {
    raw: String,
    other: String,
    number: u64,
}

impl ParserInput {
    /// Maps arbitrary bytes into bounded parser inputs without rejecting short slices.
    fn from_bytes(data: &[u8]) -> Self {
        let raw_len = data.len().min(MAX_TEXT_BYTES);
        let raw = String::from_utf8_lossy(&data[..raw_len]).to_string();
        let other = String::from_utf8_lossy(&data[raw_len..data.len().min(MAX_TEXT_BYTES * 2)])
            .to_string();
        let number = u64::from_le_bytes(fixed_bytes(data, MAX_TEXT_BYTES * 2));

        Self { raw, other, number }
    }

    /// Verifies the public token checker accepts exactly the documented token alphabet.
    fn assert_token_contract(&self) {
        assert_eq!(is_valid_format(&self.raw), expected_token_format(&self.raw));

        let valid = valid_token_from_text(&self.raw);
        assert!(is_valid_format(&valid));
        assert_eq!(is_valid_format(&valid), expected_token_format(&valid));

        assert!(!is_valid_format(TOKEN_PREFIX));
        assert!(!is_valid_format(&format!("mntisa-1-{}", valid_body(&self.raw))));
        assert!(!is_valid_format(&format!("{TOKEN_PREFIX}{}", "ABC234")));
        assert!(!is_valid_format(&format!("{TOKEN_PREFIX}{}=", valid_body(&self.raw))));
    }

    /// Verifies node address port extraction follows literal and last-colon forms.
    fn assert_port_extraction_contract(&self) {
        match (extract_port(&self.raw), expected_extract_port(&self.raw)) {
            (Ok(actual), Some(expected)) => assert_eq!(actual, expected),
            (Err(_), None) => {}
            (actual, expected) => panic!("port extraction mismatch: {actual:?} != {expected:?}"),
        }

        let port = (self.number % u64::from(u16::MAX)) as u16;
        let host = token("host", &self.raw);
        assert_eq!(
            extract_port(&format!("{host}:{port}")).expect("generated host port should parse"),
            port
        );
        assert_eq!(
            extract_port(&format!("[fd42::1]:{port}"))
                .expect("generated bracketed IPv6 port should parse"),
            port
        );
        assert_eq!(
            extract_port(&format!("fd42::1:{port}"))
                .expect("generated fallback IPv6-like port should parse"),
            port
        );
    }

    /// Verifies job and agent timestamp parsers agree on shared RFC3339 input.
    fn assert_timestamp_contract(&self) {
        let job = jobs::types::parse_timestamp(&self.raw);
        let agent = agents::types::parse_timestamp(&self.raw);
        assert_eq!(job, agent);

        let second = self.number % 60;
        let valid = format!("2026-03-25T00:00:{second:02}Z");
        assert!(jobs::types::parse_timestamp(&valid).is_some());
        assert!(agents::types::parse_timestamp(&valid).is_some());
    }

    /// Verifies optional human-facing text normalizers trim and drop empty strings.
    fn assert_optional_text_contract(&self) {
        let expected = expected_optional_text(&self.other);
        assert_eq!(
            jobs::types::normalize_detail(Some(self.other.clone())),
            expected
        );
        assert_eq!(
            agents::types::normalize_optional_text(Some(self.other.clone())),
            expected
        );
        assert_eq!(jobs::types::normalize_detail(None), None);
        assert_eq!(agents::types::normalize_optional_text(None), None);
    }
}

/// Copies a fixed-width little-endian lane out of arbitrary input bytes.
fn fixed_bytes<const N: usize>(data: &[u8], offset: usize) -> [u8; N] {
    let mut bytes = [0u8; N];
    if offset < data.len() {
        let len = (data.len() - offset).min(N);
        bytes[..len].copy_from_slice(&data[offset..offset + len]);
    }
    bytes
}

/// Implements the documented join-token format independently of the production helper.
fn expected_token_format(token: &str) -> bool {
    let Some(rest) = token.strip_prefix(TOKEN_PREFIX) else {
        return false;
    };

    !rest.is_empty()
        && rest
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || (b'2'..=b'7').contains(&byte))
}

/// Builds one syntactically valid join token from arbitrary text.
fn valid_token_from_text(text: &str) -> String {
    format!("{TOKEN_PREFIX}{}", valid_body(text))
}

/// Maps arbitrary text bytes into lowercase base32 without padding.
fn valid_body(text: &str) -> String {
    let mut body = String::new();
    for byte in text.bytes().take(96) {
        body.push(char::from(BASE32_LOWER_NOPAD[byte as usize % BASE32_LOWER_NOPAD.len()]));
    }
    if body.is_empty() {
        body.push('a');
    }
    body
}

/// Mirrors the documented node address port extraction contract.
fn expected_extract_port(raw: &str) -> Option<u16> {
    if let Ok(socket_addr) = raw.parse::<SocketAddr>() {
        return Some(socket_addr.port());
    }

    raw.rsplit_once(':')
        .and_then(|(_, tail)| tail.parse::<u16>().ok())
}

/// Returns the shared trimmed optional-text contract.
fn expected_optional_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Converts arbitrary input into a host label that remains valid in a host:port pair.
fn token(prefix: &str, raw: &str) -> String {
    let mut value = String::from(prefix);
    for byte in raw.bytes().take(64) {
        let ch = match byte % 37 {
            0..=25 => char::from(b'a' + byte % 26),
            26..=35 => char::from(b'0' + byte % 10),
            _ => '-',
        };
        value.push(ch);
    }
    value
}
