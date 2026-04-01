use anyhow::{Result, anyhow};

/// Default execution substrate used by raw job submissions and manifest parsing.
pub const DEFAULT_EXECUTION_SUBSTRATE: &str = "oci";

/// Default isolation mode used by raw job submissions and manifest parsing.
pub const DEFAULT_ISOLATION_MODE: &str = "standard";

/// Normalizes one execution substrate string into the stable jobs API identifier.
pub fn normalize_execution_substrate(raw: &str) -> Result<String> {
    let value = raw.trim().to_ascii_lowercase();
    match value.as_str() {
        "oci" | "microvm" => Ok(value),
        _ => Err(anyhow!(
            "invalid execution substrate '{raw}'; expected 'oci' or 'microvm'"
        )),
    }
}

/// Normalizes one isolation mode string into the stable jobs API identifier.
pub fn normalize_isolation_mode(raw: &str) -> Result<String> {
    let value = raw.trim().to_ascii_lowercase();
    match value.as_str() {
        "standard" | "sandboxed" => Ok(value),
        _ => Err(anyhow!(
            "invalid isolation mode '{raw}'; expected 'standard' or 'sandboxed'"
        )),
    }
}

/// Normalizes one optional isolation profile so empty values do not leak into the jobs API.
pub fn normalize_isolation_profile(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}
