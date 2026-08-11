use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use mantissa_raft::durable_log::{LogLimitSettings, LogLimits};
use mantissa_raft::protocol::{ProtocolLimitSettings, ProtocolLimits};
use mantissa_raft::runtime::{RuntimeLimitSettings, RuntimeLimits};
use mantissa_raft::transport::{TransportLimitSettings, TransportLimits};
use mantissa_volume::driver::{DriverLimitSettings, DriverLimits, UblkQueueSettings};
use mantissa_volume::fs::ext4;
use mantissa_volume::storage::replica_file::ReplicaFileSettings;
use mantissa_volume::storage::replica_file::connection::ReplicaDataServerSettings;
use mantissa_volume::storage::replica_file::data_path::FixedReplicaPathSettings;
use mantissa_volume::storage::replica_file::wire::ReplicaDataLimits;
use mantissa_volume::storage_format::{CHANGED_REGION_BYTES, REPLICA_DATA_BLOCK_BYTES};
use openraft::{Config as RaftConfig, SnapshotPolicy};

use super::{ReplicatedVolumeConfig, validate_absolute_path};

const MIN_REPLACEMENT_MEMBERSHIP_NODES: u32 = 4;

/// Replicated-volume settings converted once into the types used at runtime.
pub(crate) struct CheckedReplicatedVolumeConfig {
    pub(crate) pool_path: PathBuf,
    pub(crate) catalog_path: Option<PathBuf>,
    pub(crate) fs: ext4::Settings,
    pub(crate) listen_address: SocketAddr,
    pub(crate) advertise_address: SocketAddr,
    pub(crate) operation_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) stop_io_timeout: Duration,
    pub(crate) max_saved_replicas: usize,
    pub(crate) runtime_limits: RuntimeLimits,
    pub(crate) protocol_limits: ProtocolLimits,
    pub(crate) transport_limits: TransportLimits,
    pub(crate) log_limits: LogLimits,
    pub(crate) max_state_bytes: usize,
    pub(crate) driver_limits: DriverLimits,
    pub(crate) replica_file_settings: ReplicaFileSettings,
    pub(crate) replica_data_limits: ReplicaDataLimits,
    pub(crate) replica_data_server_settings: ReplicaDataServerSettings,
    pub(crate) fixed_path_settings: FixedReplicaPathSettings,
    pub(crate) replica_file_worker_threads: usize,
    pub(crate) replica_file_queue_capacity: usize,
    pub(crate) raft_config: RaftConfig,
    pub(crate) repair_failure_grace: Duration,
    pub(crate) max_repair_chunk_bytes: usize,
    pub(crate) max_parallel_repairs: usize,
    pub(crate) max_repair_bytes_per_second: u64,
}

impl CheckedReplicatedVolumeConfig {
    /// Checks and converts every setting before any storage resource opens.
    pub(super) fn new(config: &ReplicatedVolumeConfig) -> Result<Self> {
        validate_paths(config)?;
        let fs = ext4::Settings::new(ext4::Options {
            mount_root: PathBuf::from(&config.filesystem.mount_root),
            wipefs_path: PathBuf::from(&config.filesystem.wipefs_path),
            mkfs_ext4_path: PathBuf::from(&config.filesystem.mkfs_ext4_path),
            features: config.filesystem.features.clone(),
            inode_size_bytes: config.filesystem.inode_size_bytes,
            bytes_per_inode: config.filesystem.bytes_per_inode,
            reserved_space_percent: config.filesystem.reserved_space_percent,
            extended_options: config.filesystem.extended_options.clone(),
            mount_options: config.filesystem.mount_options.clone(),
        })
        .context("invalid replicated-volume ext4 settings")?;

        let listen_address = config
            .listen_address
            .parse::<SocketAddr>()
            .with_context(|| {
                format!(
                    "storage.replicated_volumes.listen_address must be a socket address (got '{}')",
                    config.listen_address
                )
            })?;
        if listen_address.port() == 0 {
            anyhow::bail!("storage.replicated_volumes.listen_address port must not be zero");
        }
        let advertise_address = config
            .advertise_address
            .parse::<SocketAddr>()
            .with_context(|| {
                format!(
                    "storage.replicated_volumes.advertise_address must be a socket address \
                     (got '{}')",
                    config.advertise_address
                )
            })?;
        if advertise_address.ip().is_unspecified() || advertise_address.port() == 0 {
            anyhow::bail!(
                "storage.replicated_volumes.advertise_address must use a specific IP and non-zero port"
            );
        }
        validate_plain_limits(config)?;

        if config.protocol_limits.max_membership_nodes < MIN_REPLACEMENT_MEMBERSHIP_NODES {
            anyhow::bail!(
                "storage.replicated_volumes.protocol_limits.max_membership_nodes must be at least \
                 {MIN_REPLACEMENT_MEMBERSHIP_NODES} so three stable voters and one replacement \
                 voter fit during joint consensus"
            );
        }

        let runtime_limits = RuntimeLimits::new(RuntimeLimitSettings {
            max_saved_groups: config.runtime_limits.max_saved_groups,
            max_active_groups: config.runtime_limits.max_active_groups,
            max_parallel_starts: config.runtime_limits.max_parallel_starts,
            max_background_jobs: config.runtime_limits.max_background_jobs,
        })
        .context("invalid replicated-volume group limits")?;
        let protocol_limits = ProtocolLimits::new(ProtocolLimitSettings {
            max_message_bytes: config.protocol_limits.max_message_bytes,
            max_entry_bytes: config.protocol_limits.max_entry_bytes,
            max_append_entries: config.protocol_limits.max_append_entries,
            max_membership_nodes: config.protocol_limits.max_membership_nodes,
            max_traversal_bytes: config.protocol_limits.max_traversal_bytes,
            max_nesting_levels: config.protocol_limits.max_nesting_levels,
        })
        .context("invalid replicated-volume protocol limits")?;
        let transport_limits = TransportLimits::new(TransportLimitSettings {
            max_connections: config.transport_limits.max_connections,
            max_queued_calls: config.transport_limits.max_queued_calls,
            max_queued_bytes: config.transport_limits.max_queued_bytes,
            reserved_vote_and_heartbeat_queue_bytes: config
                .transport_limits
                .reserved_vote_and_heartbeat_queue_bytes,
            max_calls_per_peer: config.transport_limits.max_calls_per_peer,
            reserved_vote_and_heartbeat_calls_per_peer: config
                .transport_limits
                .reserved_vote_and_heartbeat_calls_per_peer,
            max_snapshot_chunk_bytes: config.transport_limits.max_snapshot_chunk_bytes,
            connect_timeout: Duration::from_millis(config.transport_limits.connect_timeout_ms),
            handshake_timeout: Duration::from_millis(config.transport_limits.handshake_timeout_ms),
            call_timeout: Duration::from_millis(config.transport_limits.call_timeout_ms),
            queue_timeout: Duration::from_millis(config.transport_limits.queue_timeout_ms),
            reconnect_delay: Duration::from_millis(config.transport_limits.reconnect_delay_ms),
        })
        .context("invalid replicated-volume transport limits")?;
        let log_limits = LogLimits::new(LogLimitSettings {
            max_frame_bytes: config.log_limits.max_frame_bytes,
            max_segment_bytes: config.log_limits.max_segment_bytes,
        })
        .context("invalid replicated-volume Raft log limits")?;
        if config.data_store_limits.worker_threads == 0 {
            anyhow::bail!("replicated-volume file-worker thread count must be greater than zero");
        }
        if config.data_store_limits.max_queued_operations == 0 {
            anyhow::bail!("replicated-volume file-worker queue must allow at least one operation");
        }
        let replica_file_settings = ReplicaFileSettings::new(
            CHANGED_REGION_BYTES,
            config.driver_limits.max_batch_changes,
            config.driver_limits.max_batch_bytes,
            config.driver_limits.max_pending_requests,
        )
        .context("invalid replicated-volume fixed-file limits")?;
        let replica_data_limits = ReplicaDataLimits::new(
            protocol_limits.max_message_bytes(),
            4 << 10,
            replica_file_settings,
        )
        .context("invalid replicated-volume data message limits")?;
        validate_repair_limits(config, protocol_limits)?;
        let driver_limits = DriverLimits::new(DriverLimitSettings {
            queues: UblkQueueSettings {
                queue_count: config.driver_limits.queue_count,
                queue_depth: config.driver_limits.queue_depth,
                max_request_bytes: config.driver_limits.max_request_bytes,
                memory_limit_bytes: config.driver_limits.max_queue_buffer_bytes,
            },
            max_pending_requests: config.driver_limits.max_pending_requests,
            max_pending_buffer_bytes: config.driver_limits.max_pending_buffer_bytes,
        })
        .context("invalid replicated-volume driver limits")?;
        validate_batch_limits(config)?;

        let replica_data_server_settings = ReplicaDataServerSettings::new(
            config.driver_limits.max_pending_requests,
            transport_limits.call_timeout(),
        )?;
        let fixed_path_settings = FixedReplicaPathSettings::new(
            replica_file_settings,
            config.driver_limits.max_pending_requests,
            config.driver_limits.max_pending_buffer_bytes,
            config.data_store_limits.worker_threads,
            transport_limits.call_timeout(),
        )?
        .with_combine_delay(Duration::from_micros(
            config.driver_limits.max_batch_delay_us,
        ));
        let raft_config = raft_config(config, protocol_limits)?;
        let shutdown_timeout = Duration::from_millis(config.shutdown_timeout_ms);

        Ok(Self {
            pool_path: PathBuf::from(&config.pool_path),
            catalog_path: config.catalog_path.as_deref().map(PathBuf::from),
            fs,
            listen_address,
            advertise_address,
            operation_timeout: Duration::from_millis(config.operation_timeout_ms),
            shutdown_timeout,
            stop_io_timeout: Duration::from_millis(
                config
                    .election_timeout_max_ms
                    .min((config.shutdown_timeout_ms / 2).max(1)),
            ),
            max_saved_replicas: config.max_saved_replicas,
            runtime_limits,
            protocol_limits,
            transport_limits,
            log_limits,
            max_state_bytes: config.state_limits.max_state_bytes,
            driver_limits,
            replica_file_settings,
            replica_data_limits,
            replica_data_server_settings,
            fixed_path_settings,
            replica_file_worker_threads: config.data_store_limits.worker_threads,
            replica_file_queue_capacity: config.data_store_limits.max_queued_operations,
            raft_config,
            repair_failure_grace: Duration::from_millis(config.repair_limits.failure_grace_ms),
            max_repair_chunk_bytes: config.repair_limits.max_chunk_bytes,
            max_parallel_repairs: config.repair_limits.max_parallel_repairs,
            max_repair_bytes_per_second: config.repair_limits.max_bytes_per_second,
        })
    }
}

/// Checks storage paths that are not owned by a lower-level component.
fn validate_paths(config: &ReplicatedVolumeConfig) -> Result<()> {
    validate_absolute_path("storage.replicated_volumes.pool_path", &config.pool_path)?;
    if let Some(catalog_path) = config.catalog_path.as_deref() {
        validate_absolute_path("storage.replicated_volumes.catalog_path", catalog_path)?;
    }
    Ok(())
}

/// Checks scalar limits that have no dedicated checked type.
fn validate_plain_limits(config: &ReplicatedVolumeConfig) -> Result<()> {
    if config.startup_timeout_ms == 0
        || config.shutdown_timeout_ms == 0
        || config.operation_timeout_ms == 0
    {
        anyhow::bail!(
            "replicated-volume startup, shutdown, and operation timeouts must be greater than zero"
        );
    }
    if config.max_saved_replicas == 0 {
        anyhow::bail!("storage.replicated_volumes.max_saved_replicas must be greater than zero");
    }
    if config.state_limits.max_state_bytes == 0 {
        anyhow::bail!("replicated-volume state byte limit must be greater than zero");
    }
    if config.heartbeat_interval_ms == 0
        || config.election_timeout_min_ms <= config.heartbeat_interval_ms
        || config.election_timeout_min_ms >= config.election_timeout_max_ms
    {
        anyhow::bail!(
            "replicated-volume election timeouts must increase from heartbeat to minimum to maximum"
        );
    }
    Ok(())
}

/// Checks repair limits against the data block and message sizes.
fn validate_repair_limits(
    config: &ReplicatedVolumeConfig,
    protocol_limits: ProtocolLimits,
) -> Result<()> {
    if config.repair_limits.failure_grace_ms == 0
        || config.repair_limits.max_parallel_repairs == 0
        || config.repair_limits.max_chunk_bytes == 0
        || config.repair_limits.max_bytes_per_second == 0
    {
        anyhow::bail!("replicated-volume repair limits must be greater than zero");
    }
    if config.repair_limits.max_chunk_bytes > protocol_limits.max_message_bytes() / 2 {
        anyhow::bail!(
            "replicated-volume repair chunks must fit inside the Cap'n Proto message limit"
        );
    }
    if !config
        .repair_limits
        .max_chunk_bytes
        .is_multiple_of(REPLICA_DATA_BLOCK_BYTES as usize)
    {
        anyhow::bail!("replicated-volume repair chunks must be aligned to data blocks");
    }
    Ok(())
}

/// Checks batching limits that span the driver and fixed-file data path.
fn validate_batch_limits(config: &ReplicatedVolumeConfig) -> Result<()> {
    if config.driver_limits.max_batch_changes == 0 || config.driver_limits.max_batch_bytes == 0 {
        anyhow::bail!("replicated-volume block batch count and bytes must be greater than zero");
    }
    if config.driver_limits.max_batch_changes > config.driver_limits.max_pending_requests {
        anyhow::bail!("replicated-volume block batch count must fit the pending request limit");
    }
    if config.driver_limits.max_batch_bytes > config.driver_limits.max_pending_buffer_bytes {
        anyhow::bail!("replicated-volume block batch bytes must fit the pending buffer limit");
    }
    if config.driver_limits.max_request_bytes as usize > config.driver_limits.max_batch_bytes {
        anyhow::bail!("replicated-volume batches must hold one complete driver request");
    }
    let data_block_bytes = mantissa_volume::VolumeBlockSizes::supported()
        .data_block()
        .bytes() as usize;
    let minimum_batch_bytes = config
        .driver_limits
        .max_batch_changes
        .checked_mul(data_block_bytes)
        .context("replicated-volume block batch size overflow")?;
    if minimum_batch_bytes > config.driver_limits.max_batch_bytes {
        anyhow::bail!("replicated-volume block batch bytes must hold one block per batch entry");
    }
    Ok(())
}

/// Builds and checks the OpenRaft timing and snapshot settings.
fn raft_config(
    config: &ReplicatedVolumeConfig,
    protocol_limits: ProtocolLimits,
) -> Result<RaftConfig> {
    if config.log_limits.snapshot_after_entries == 0 {
        anyhow::bail!("replicated-volume snapshot entry limit must be greater than zero");
    }
    let mut raft_config = RaftConfig {
        cluster_name: "mantissa replicated volume".to_string(),
        election_timeout_min: config.election_timeout_min_ms,
        election_timeout_max: config.election_timeout_max_ms,
        heartbeat_interval: config.heartbeat_interval_ms,
        max_payload_entries: u64::from(protocol_limits.max_append_entries()),
        snapshot_policy: SnapshotPolicy::LogsSinceLast(config.log_limits.snapshot_after_entries),
        snapshot_max_chunk_size: config.transport_limits.max_snapshot_chunk_bytes as u64,
        max_in_snapshot_log_to_keep: config.log_limits.snapshot_after_entries,
        purge_batch_size: config.log_limits.snapshot_after_entries,
        ..RaftConfig::default()
    };
    raft_config.enable_tick = true;
    raft_config.enable_heartbeat = true;
    raft_config.enable_elect = true;
    raft_config.validate().context("invalid volume Raft timing")
}
