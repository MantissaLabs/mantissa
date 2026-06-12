use mantissa_client::host_ports::HostPortView;
use serde::Serialize;

/// REST-facing host-port binding shared by jobs and services.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HostPort {
    pub name: String,
    pub target_port: u16,
    pub host_port: u16,
    pub host_ip: String,
    pub protocol: String,
}

impl From<HostPortView> for HostPort {
    /// Converts a client host-port view into the REST JSON shape.
    fn from(value: HostPortView) -> Self {
        Self {
            name: value.name,
            target_port: value.target_port,
            host_port: value.host_port,
            host_ip: value.host_ip,
            protocol: value.protocol.as_str().to_string(),
        }
    }
}

/// Converts Rust debug enum labels into lowercase snake-case strings.
pub(crate) fn debug_variant_label(value: impl std::fmt::Debug) -> String {
    camel_to_snake(&format!("{value:?}"))
}

/// Converts a CamelCase variant label into snake_case.
fn camel_to_snake(value: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    enum Example {
        VolumeUnavailable,
    }

    #[test]
    fn debug_variant_label_returns_snake_case() {
        assert_eq!(
            debug_variant_label(Example::VolumeUnavailable),
            "volume_unavailable"
        );
    }
}
