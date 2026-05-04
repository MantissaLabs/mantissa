use super::runtime::BootstrapOptions;
use crate::config;
use crate::secrets::master_key_protector::SecretPassphrase;

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
        ..BootstrapOptions::default()
    }
}
