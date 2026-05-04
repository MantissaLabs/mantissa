use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use std::io::{self, Read};

pub mod create;
pub mod delete;
pub mod list;
pub mod rotate;
pub mod show;
pub mod update;

pub use create::create;
pub use delete::delete;
pub use list::list;
pub use rotate::rotate_master_key;
pub use show::show;
pub use update::update;

/// Parses `KEY=VALUE` labels from CLI arguments into normalized key/value pairs.
pub(super) fn parse_secret_labels(labels: &[String]) -> Result<Vec<(String, String)>> {
    let mut pairs = Vec::with_capacity(labels.len());
    for raw in labels {
        let mut parts = raw.splitn(2, '=');
        let key = parts.next().unwrap_or_default().trim().to_string();
        let value = parts
            .next()
            .ok_or_else(|| anyhow!("invalid label '{}': expected KEY=VALUE", raw))?
            .trim()
            .to_string();

        if key.is_empty() {
            return Err(anyhow!("label key cannot be empty in '{}'", raw));
        }

        pairs.push((key, value));
    }
    Ok(pairs)
}

/// Resolves a secret plaintext payload from `--value` or falls back to stdin when omitted.
pub(super) fn resolve_secret_plaintext(value: Option<String>) -> Result<Vec<u8>> {
    if let Some(val) = value {
        return Ok(val.into_bytes());
    }

    let mut buffer = Vec::new();
    io::stdin()
        .read_to_end(&mut buffer)
        .context("failed to read secret value from stdin")?;

    while buffer.ends_with(b"\n") || buffer.ends_with(b"\r") {
        buffer.pop();
    }

    if buffer.is_empty() {
        Err(anyhow!(
            "secret value is empty; pass --value or provide data on stdin"
        ))
    } else {
        Ok(buffer)
    }
}

/// Renders plaintext as UTF-8 when possible, otherwise emits a base64-prefixed representation.
pub(super) fn display_secret_plaintext(data: &[u8]) -> String {
    match std::str::from_utf8(data) {
        Ok(text) => text.to_string(),
        Err(_) => format!("base64:{}", BASE64_STANDARD.encode(data)),
    }
}
