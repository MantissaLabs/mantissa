use super::runtime::BootstrapOptions;
use crate::config;
use crate::secrets::master_key::envelope::{PassphraseKdfParams, SecretPassphrase};

/// Selects the passphrase KDF profile for daemon master-key envelopes.
fn daemon_master_key_kdf_params() -> PassphraseKdfParams {
    #[cfg(any(test, debug_assertions))]
    {
        if let Ok(raw) = std::env::var("MANTISSA_TEST_MASTER_KEY_KDF") {
            if let Some(params) = parse_test_master_key_kdf_profile(&raw) {
                return params;
            }

            tracing::warn!(
                target: "server",
                "ignoring unsupported MANTISSA_TEST_MASTER_KEY_KDF profile '{raw}'"
            );
        }
    }

    PassphraseKdfParams::production()
}

/// Parses hidden test-only daemon KDF profiles used by subprocess harnesses.
#[cfg(any(test, debug_assertions))]
fn parse_test_master_key_kdf_profile(raw: &str) -> Option<PassphraseKdfParams> {
    match raw.trim() {
        "fast" | "test" => Some(PassphraseKdfParams::test()),
        "production" | "prod" | "default" => Some(PassphraseKdfParams::production()),
        _ => None,
    }
}

/// Builds the runtime options used by the production daemon entrypoint.
///
/// Headless startup constructs the same option type directly so both code paths
/// converge on one shared bootstrap pipeline.
pub(super) fn daemon_bootstrap_options(
    advertise_override: Option<String>,
    master_key_passphrase: SecretPassphrase,
) -> BootstrapOptions {
    let replication = config::replication_runtime_config();
    BootstrapOptions {
        gossip_channel_capacity: replication.gossip_channel_capacity,
        gossip_fanout: replication.gossip_fanout,
        sync_tick: Some(replication.sync_tick),
        sync_fanout: Some(replication.sync_fanout),
        workload_repair_fanout: Some(replication.workload_repair_fanout),
        global_metadata_sync_tick: Some(replication.global_metadata_sync_tick),
        global_metadata_sync_fanout: Some(replication.global_metadata_sync_fanout),
        gossip_tick: Some(replication.gossip_tick),
        advertise_override,
        master_key_passphrase: Some(master_key_passphrase),
        master_key_kdf_params: daemon_master_key_kdf_params(),
        ..BootstrapOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{PassphraseKdfParams, parse_test_master_key_kdf_profile};

    /// Hidden daemon KDF profile parsing should be strict and deterministic.
    #[test]
    fn parses_test_master_key_kdf_profiles() {
        assert_eq!(
            parse_test_master_key_kdf_profile("fast"),
            Some(PassphraseKdfParams::test())
        );
        assert_eq!(
            parse_test_master_key_kdf_profile("production"),
            Some(PassphraseKdfParams::production())
        );
        assert_eq!(parse_test_master_key_kdf_profile(""), None);
        assert_eq!(parse_test_master_key_kdf_profile("fastest"), None);
    }
}
