use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mantissa_net::paths::ensure_state_dir;
use mantissa_store::gc::StoreGcPolicy;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::ip_family::DefaultIpFamilyPolicy;
use crate::network::types::NetworkRealizationPolicy;
use crate::volumes::local::ensure_local_volume_root;

mod replicated_volume;

pub(crate) use replicated_volume::CheckedReplicatedVolumeConfig;

/// Maximum scheduler slot count supported by the local scheduler snapshot codec.
pub const SCHEDULER_MAX_SLOT_COUNT: u64 = 65_536;

/// # Description:
///
/// Root configuration container loaded from the Mantissa RON config file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub runtimes: RuntimeRegistryConfig,
    #[serde(default)]
    pub gpu: GpuConfig,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub replication: ReplicationConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

/// # Description:
///
/// Storage subsystem configuration shared by runtime components that persist local state.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StorageConfig {
    #[serde(default)]
    pub local_volume_root: Option<String>,
    #[serde(default)]
    pub local_volume_enforce_capacity: bool,
    #[serde(default)]
    pub replicated_volumes: Option<ReplicatedVolumeConfig>,
    #[serde(default)]
    pub gc: StoreGcConfig,
}

/// Daemon settings that override the built-in replicated-volume settings.
///
/// A node without this section uses local paths, detects its storage address,
/// and starts replicated volumes when the host supports them.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeConfig {
    pub pool_path: String,
    pub catalog_path: Option<String>,
    pub listen_address: String,
    pub advertise_address: String,
    pub startup_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub max_saved_replicas: usize,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub runtime_limits: ReplicatedVolumeRuntimeLimits,
    pub protocol_limits: ReplicatedVolumeProtocolLimits,
    pub transport_limits: ReplicatedVolumeTransportLimits,
    pub log_limits: ReplicatedVolumeLogLimits,
    pub data_store_limits: ReplicatedVolumeDataStoreLimits,
    pub state_limits: ReplicatedVolumeStateLimits,
    pub repair_limits: ReplicatedVolumeRepairLimits,
    pub driver_limits: ReplicatedVolumeDriverLimits,
    pub filesystem: ReplicatedVolumeFilesystemSettings,
}

/// Limits for saved and running Raft groups on one node.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeRuntimeLimits {
    pub max_saved_groups: usize,
    pub max_active_groups: usize,
    pub max_parallel_starts: usize,
    pub max_background_jobs: usize,
}

/// Limits applied while encoding and reading Raft Cap'n Proto messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeProtocolLimits {
    pub max_message_bytes: usize,
    pub max_entry_bytes: usize,
    pub max_append_entries: u32,
    pub max_membership_nodes: u32,
    pub max_traversal_bytes: usize,
    pub max_nesting_levels: u32,
}

/// Connection, queue, size, and time limits for the shared Raft transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeTransportLimits {
    pub max_connections: usize,
    pub max_queued_calls: usize,
    pub max_queued_bytes: usize,
    pub reserved_vote_and_heartbeat_queue_bytes: usize,
    pub max_calls_per_peer: usize,
    pub reserved_vote_and_heartbeat_calls_per_peer: usize,
    pub max_snapshot_chunk_bytes: usize,
    pub connect_timeout_ms: u64,
    pub handshake_timeout_ms: u64,
    pub call_timeout_ms: u64,
    pub queue_timeout_ms: u64,
    pub reconnect_delay_ms: u64,
}

/// Size limits for encrypted Raft log frames and files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeLogLimits {
    pub max_frame_bytes: usize,
    pub max_segment_bytes: u64,
    pub snapshot_after_entries: u64,
}

/// Worker limits for fixed replica data files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeDataStoreLimits {
    pub worker_threads: usize,
    pub max_queued_operations: usize,
}

/// Defensive encoded-size limit for replicated-volume control state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeStateLimits {
    pub max_state_bytes: usize,
}

/// Failure delay, concurrency, request, and bandwidth limits for replica repair.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeRepairLimits {
    pub failure_grace_ms: u64,
    pub max_parallel_repairs: usize,
    pub max_chunk_bytes: usize,
    pub max_bytes_per_second: u64,
}

/// ublk queue and copied-request memory limits for one attached volume.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeDriverLimits {
    pub queue_count: u16,
    pub queue_depth: u16,
    pub max_request_bytes: u32,
    pub max_queue_buffer_bytes: u64,
    pub max_pending_requests: usize,
    pub max_pending_buffer_bytes: usize,
    pub max_batch_changes: usize,
    pub max_batch_bytes: usize,
    pub max_batch_delay_us: u64,
}

/// Exact ext4 tools, layout choices, and mount settings.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ReplicatedVolumeFilesystemSettings {
    pub mount_root: String,
    pub wipefs_path: String,
    pub mkfs_ext4_path: String,
    pub features: Vec<String>,
    pub inode_size_bytes: u16,
    pub bytes_per_inode: u32,
    pub reserved_space_percent: u8,
    pub extended_options: Vec<String>,
    pub mount_options: Vec<String>,
}

impl Default for ReplicatedVolumeRuntimeLimits {
    /// Returns conservative limits for saved and active volume groups.
    fn default() -> Self {
        Self {
            max_saved_groups: 100,
            max_active_groups: 16,
            max_parallel_starts: 4,
            max_background_jobs: 2,
        }
    }
}

impl Default for ReplicatedVolumeProtocolLimits {
    /// Returns message limits that keep one write well below one Raft message.
    fn default() -> Self {
        Self {
            max_message_bytes: 8 << 20,
            max_entry_bytes: 2 << 20,
            max_append_entries: 16,
            max_membership_nodes: 8,
            max_traversal_bytes: 16 << 20,
            max_nesting_levels: 32,
        }
    }
}

impl Default for ReplicatedVolumeTransportLimits {
    /// Returns bounded connection, queue, and timeout settings.
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_queued_calls: 128,
            max_queued_bytes: 32 << 20,
            reserved_vote_and_heartbeat_queue_bytes: 1 << 20,
            max_calls_per_peer: 8,
            reserved_vote_and_heartbeat_calls_per_peer: 2,
            max_snapshot_chunk_bytes: 1 << 20,
            connect_timeout_ms: 3_000,
            handshake_timeout_ms: 3_000,
            call_timeout_ms: 30_000,
            queue_timeout_ms: 3_000,
            reconnect_delay_ms: 500,
        }
    }
}

impl Default for ReplicatedVolumeLogLimits {
    /// Returns file and frame limits for each volume's Raft log.
    fn default() -> Self {
        Self {
            max_frame_bytes: 3 << 20,
            max_segment_bytes: 64 << 20,
            snapshot_after_entries: 4_096,
        }
    }
}

impl Default for ReplicatedVolumeDataStoreLimits {
    /// Returns bounded node-wide fixed-file worker limits.
    fn default() -> Self {
        Self {
            worker_threads: 8,
            max_queued_operations: 64,
        }
    }
}

impl Default for ReplicatedVolumeStateLimits {
    /// Returns the bounded control-state snapshot limit for one volume group.
    fn default() -> Self {
        Self {
            max_state_bytes: 2 << 20,
        }
    }
}

impl Default for ReplicatedVolumeRepairLimits {
    /// Returns bounded repair work and a shared bandwidth limit.
    fn default() -> Self {
        Self {
            failure_grace_ms: 30_000,
            max_parallel_repairs: 2,
            max_chunk_bytes: 1 << 20,
            max_bytes_per_second: 64 << 20,
        }
    }
}

impl Default for ReplicatedVolumeDriverLimits {
    /// Returns ublk queue settings with a bounded memory cost per volume.
    fn default() -> Self {
        Self {
            queue_count: 2,
            queue_depth: 32,
            max_request_bytes: 128 << 10,
            max_queue_buffer_bytes: 8 << 20,
            max_pending_requests: 64,
            max_pending_buffer_bytes: 1 << 20,
            max_batch_changes: 64,
            max_batch_bytes: 1 << 20,
            max_batch_delay_us: 250,
        }
    }
}

impl ReplicatedVolumeConfig {
    /// Builds the settings used when the operator provides no storage section.
    pub(crate) fn automatic(advertise_override: Option<&str>) -> Result<Self> {
        let state_dir = ensure_state_dir().context("prepare the Mantissa state directory")?;
        let pool_path = state_dir.join("replicas");
        fs::create_dir_all(&pool_path).with_context(|| {
            format!(
                "create the default replicated-volume pool {}",
                pool_path.display()
            )
        })?;

        let advertise_ip = automatic_replicated_volume_ip(advertise_override)?;
        let listen_ip = match advertise_ip {
            IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        };
        let wipefs_path = find_replicated_volume_tool(
            "wipefs",
            &[
                "/usr/sbin/wipefs",
                "/sbin/wipefs",
                "/usr/bin/wipefs",
                "/bin/wipefs",
            ],
        )?;
        let mkfs_ext4_path = find_replicated_volume_tool(
            "mkfs.ext4",
            &[
                "/usr/sbin/mkfs.ext4",
                "/sbin/mkfs.ext4",
                "/usr/bin/mkfs.ext4",
                "/bin/mkfs.ext4",
            ],
        )?;

        Ok(Self::with_defaults(
            pool_path,
            state_dir.join("replicated-volumes.redb"),
            state_dir.join("volume-mounts"),
            SocketAddr::new(listen_ip, 7578),
            SocketAddr::new(advertise_ip, 7578),
            wipefs_path,
            mkfs_ext4_path,
        ))
    }

    /// Fills paths and addresses around the built-in limits and ext4 profile.
    fn with_defaults(
        pool_path: PathBuf,
        catalog_path: PathBuf,
        mount_root: PathBuf,
        listen_address: SocketAddr,
        advertise_address: SocketAddr,
        wipefs_path: PathBuf,
        mkfs_ext4_path: PathBuf,
    ) -> Self {
        Self {
            pool_path: pool_path.display().to_string(),
            catalog_path: Some(catalog_path.display().to_string()),
            listen_address: listen_address.to_string(),
            advertise_address: advertise_address.to_string(),
            startup_timeout_ms: 30_000,
            shutdown_timeout_ms: 60_000,
            operation_timeout_ms: 60_000,
            max_saved_replicas: 100,
            heartbeat_interval_ms: 2_000,
            election_timeout_min_ms: 8_000,
            election_timeout_max_ms: 12_000,
            runtime_limits: ReplicatedVolumeRuntimeLimits::default(),
            protocol_limits: ReplicatedVolumeProtocolLimits::default(),
            transport_limits: ReplicatedVolumeTransportLimits::default(),
            log_limits: ReplicatedVolumeLogLimits::default(),
            data_store_limits: ReplicatedVolumeDataStoreLimits::default(),
            state_limits: ReplicatedVolumeStateLimits::default(),
            repair_limits: ReplicatedVolumeRepairLimits::default(),
            driver_limits: ReplicatedVolumeDriverLimits::default(),
            filesystem: ReplicatedVolumeFilesystemSettings {
                mount_root: mount_root.display().to_string(),
                wipefs_path: wipefs_path.display().to_string(),
                mkfs_ext4_path: mkfs_ext4_path.display().to_string(),
                features: vec![
                    "has_journal".to_string(),
                    "extent".to_string(),
                    "filetype".to_string(),
                    "64bit".to_string(),
                    "metadata_csum".to_string(),
                ],
                inode_size_bytes: 256,
                bytes_per_inode: 16_384,
                reserved_space_percent: 0,
                extended_options: vec![
                    // New replicated volumes already read as zero. Discarding
                    // the full capacity would create needless Raft writes.
                    "nodiscard".to_string(),
                    // Let ext4 finish zeroing unused metadata after the first
                    // mount instead of blocking workload startup.
                    "lazy_itable_init=1".to_string(),
                    "lazy_journal_init=1".to_string(),
                ],
                mount_options: vec!["noatime".to_string()],
            },
        }
    }

    /// Checks and converts every replicated-volume setting.
    pub(crate) fn checked(&self) -> Result<CheckedReplicatedVolumeConfig> {
        CheckedReplicatedVolumeConfig::new(self)
    }

    /// Checks every replicated-volume setting without opening local resources.
    pub(crate) fn validate(&self) -> Result<()> {
        self.checked().map(drop)
    }
}

/// Uses an explicit daemon address when possible, then checks local interfaces.
fn automatic_replicated_volume_ip(advertise_override: Option<&str>) -> Result<IpAddr> {
    if let Some(address) = advertise_override
        .and_then(|address| address.parse::<SocketAddr>().ok())
        .filter(|address| !address.ip().is_unspecified())
    {
        return Ok(address.ip());
    }

    let preferred_family = match default_ip_family_policy() {
        DefaultIpFamilyPolicy::Ipv4 => Some(crate::ip_family::IpFamily::Ipv4),
        DefaultIpFamilyPolicy::Ipv6 => Some(crate::ip_family::IpFamily::Ipv6),
        DefaultIpFamilyPolicy::Auto => None,
    };
    crate::node::address::compute_advertise_ip(None, None, preferred_family)
        .context("find a local address for replicated volumes")
}

/// Finds one required system tool at a known absolute path.
fn find_replicated_volume_tool(name: &str, candidates: &[&str]) -> Result<PathBuf> {
    for candidate in candidates {
        let path = PathBuf::from(candidate);
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o111 == 0 {
            continue;
        }
        return Ok(path);
    }

    anyhow::bail!(
        "replicated volumes need {name}; checked {}",
        candidates.join(", ")
    )
}

/// Checks one required absolute filesystem path.
fn validate_absolute_path(name: &str, path: &str) -> Result<()> {
    if path.trim().is_empty() {
        anyhow::bail!("{name} cannot be empty");
    }
    if !Path::new(path).is_absolute() {
        anyhow::bail!("{name} must be an absolute path");
    }
    Ok(())
}

/// # Description:
///
/// Storage garbage collection policy used by the background maintenance loop.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct StoreGcConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_store_gc_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_store_gc_tombstone_min_retention_ms")]
    pub tombstone_min_retention_ms: u64,
    #[serde(default = "default_store_gc_tombstone_batch_limit")]
    pub tombstone_batch_limit: usize,
    #[serde(default)]
    pub mvreg_batch_limit: usize,
    #[serde(default)]
    pub mvreg_max_values: Option<usize>,
    #[serde(default = "default_store_gc_stale_peer_rejoin_after_ms")]
    pub stale_peer_rejoin_after_ms: u64,
}

impl Default for StoreGcConfig {
    /// # Description:
    ///
    /// Returns conservative store GC defaults for bounded production sweeps.
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: default_store_gc_interval_ms(),
            tombstone_min_retention_ms: default_store_gc_tombstone_min_retention_ms(),
            tombstone_batch_limit: default_store_gc_tombstone_batch_limit(),
            mvreg_batch_limit: 0,
            mvreg_max_values: None,
            stale_peer_rejoin_after_ms: default_store_gc_stale_peer_rejoin_after_ms(),
        }
    }
}

/// # Description:
///
/// Runtime-friendly store GC settings after scalar config values are normalized.
#[derive(Clone, Debug)]
pub struct RuntimeStoreGcConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub stale_peer_rejoin_after: Duration,
    pub policy: StoreGcPolicy,
}

impl StoreGcConfig {
    /// # Description:
    ///
    /// Converts persisted storage GC settings into runtime durations and store policy knobs.
    fn as_runtime(&self) -> RuntimeStoreGcConfig {
        RuntimeStoreGcConfig {
            enabled: self.enabled,
            interval: Duration::from_millis(self.interval_ms),
            stale_peer_rejoin_after: Duration::from_millis(self.stale_peer_rejoin_after_ms),
            policy: StoreGcPolicy {
                tombstone_min_retention_ms: self.tombstone_min_retention_ms,
                tombstone_batch_limit: self.tombstone_batch_limit,
                mvreg_batch_limit: self.mvreg_batch_limit,
                mvreg_max_values: self.mvreg_max_values,
            },
        }
    }
}

/// # Description:
///
/// Security-sensitive local credential and bearer-ticket policy.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct SecurityConfig {
    #[serde(default = "default_session_ticket_ttl_secs")]
    pub session_ticket_ttl_secs: u64,
}

impl Default for SecurityConfig {
    /// # Description:
    ///
    /// Returns conservative defaults for durable session tickets.
    fn default() -> Self {
        Self {
            session_ticket_ttl_secs: default_session_ticket_ttl_secs(),
        }
    }
}

/// # Description:
///
/// Observability subsystem configuration for local production telemetry.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObservabilityConfig {
    #[serde(default)]
    pub metrics: MetricsConfig,
}

/// # Description:
///
/// Prometheus/OpenMetrics exporter and low-cost sampler configuration.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_metrics_sample_interval_ms")]
    pub sample_interval_ms: u64,
    #[serde(default = "default_metrics_state_db_sample_interval_ms")]
    pub state_db_sample_interval_ms: u64,
}

impl Default for MetricsConfig {
    /// # Description:
    ///
    /// Keeps metrics disabled until operators explicitly enable the local scrape endpoint.
    fn default() -> Self {
        Self {
            enabled: false,
            listen_addr: default_metrics_listen_addr(),
            sample_interval_ms: default_metrics_sample_interval_ms(),
            state_db_sample_interval_ms: default_metrics_state_db_sample_interval_ms(),
        }
    }
}

/// # Description:
///
/// Runtime metrics configuration with parsed addresses and intervals.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeMetricsConfig {
    pub enabled: bool,
    pub listen_addr: SocketAddr,
    pub sample_interval: Duration,
    pub state_db_sample_interval: Duration,
}

impl MetricsConfig {
    /// # Description:
    ///
    /// Converts persisted metrics settings into runtime values after validation.
    fn as_runtime(&self) -> Result<RuntimeMetricsConfig> {
        let listen_addr = if self.enabled {
            self.listen_addr.parse().with_context(|| {
                format!(
                    "observability.metrics.listen_addr must be a socket address (got '{}')",
                    self.listen_addr
                )
            })?
        } else {
            default_metrics_socket_addr()
        };

        Ok(RuntimeMetricsConfig {
            enabled: self.enabled,
            listen_addr,
            sample_interval: Duration::from_millis(self.sample_interval_ms),
            state_db_sample_interval: Duration::from_millis(self.state_db_sample_interval_ms),
        })
    }
}

/// # Description:
///
/// Network subsystem configuration for WireGuard, BPF, and nodeport.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NetworkConfig {
    #[serde(default)]
    pub advertise_addr: Option<String>,
    #[serde(default)]
    pub default_ip_family: DefaultIpFamilyPolicy,
    #[serde(default)]
    pub realization_default: NetworkRealizationPolicy,
    #[serde(default = "default_true")]
    pub provision_kernel_interfaces: bool,
    #[serde(default)]
    pub wireguard: WireguardConfig,
    #[serde(default)]
    pub bpf: BpfConfig,
    #[serde(default)]
    pub nodeport: NodePortConfig,
}

impl Default for NetworkConfig {
    /// # Description:
    ///
    /// Returns the default network configuration used when no config is supplied.
    fn default() -> Self {
        Self {
            advertise_addr: None,
            default_ip_family: DefaultIpFamilyPolicy::Auto,
            realization_default: NetworkRealizationPolicy::AllNodes,
            provision_kernel_interfaces: true,
            wireguard: WireguardConfig::default(),
            bpf: BpfConfig::default(),
            nodeport: NodePortConfig::default(),
        }
    }
}

/// # Description:
///
/// WireGuard underlay configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WireguardConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default = "default_true")]
    pub manage_firewall: bool,
}

impl Default for WireguardConfig {
    /// # Description:
    ///
    /// Returns the default WireGuard configuration used when no config is supplied.
    fn default() -> Self {
        Self {
            enabled: true,
            port: None,
            manage_firewall: true,
        }
    }
}

/// # Description:
///
/// eBPF attachment configuration for the network dataplane.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BpfConfig {
    #[serde(default = "default_true")]
    pub attach: bool,
    #[serde(default)]
    pub artifact_dir: Option<String>,
    #[serde(default = "default_bpf_overlay_flow_capacity")]
    pub overlay_flow_capacity: usize,
}

impl Default for BpfConfig {
    /// # Description:
    ///
    /// Returns the default BPF configuration used when no config is supplied.
    fn default() -> Self {
        Self {
            attach: true,
            artifact_dir: None,
            overlay_flow_capacity: default_bpf_overlay_flow_capacity(),
        }
    }
}

/// # Description:
///
/// Nodeport exposure settings for the external load balancer.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NodePortConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub iface: Option<String>,
    #[serde(default)]
    pub ip: Option<String>,
    #[serde(default)]
    pub source_mode: NodePortSourceMode,
    #[serde(default = "default_nodeport_vip_capacity")]
    pub vip_capacity: usize,
    #[serde(default = "default_nodeport_host_capacity")]
    pub host_capacity: usize,
    #[serde(default = "default_nodeport_flow_capacity")]
    pub flow_capacity: usize,
}

impl Default for NodePortConfig {
    /// # Description:
    ///
    /// Returns the default nodeport configuration used when no config is supplied.
    fn default() -> Self {
        Self {
            enabled: true,
            iface: None,
            ip: None,
            source_mode: NodePortSourceMode::default(),
            vip_capacity: default_nodeport_vip_capacity(),
            host_capacity: default_nodeport_host_capacity(),
            flow_capacity: default_nodeport_flow_capacity(),
        }
    }
}

/// # Description:
///
/// Source-address contract applied to published NodePort traffic before it enters the overlay.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodePortSourceMode {
    #[default]
    SnatHostAccess,
    PreserveClient,
}

impl NodePortSourceMode {
    /// # Description:
    ///
    /// Render one stable label for config diagnostics, CLI output, and operator-facing docs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SnatHostAccess => "snat_host_access",
            Self::PreserveClient => "preserve_client",
        }
    }
}

impl std::fmt::Display for NodePortSourceMode {
    /// # Description:
    ///
    /// Render one stable text representation of the configured NodePort source-address contract.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// # Description:
///
/// Cluster SWIM probing and liveness threshold configuration.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct HealthConfig {
    #[serde(default = "default_health_probe_fanout")]
    pub probe_fanout: usize,
    #[serde(default = "default_health_probe_interval_ms")]
    pub probe_interval_ms: u64,
    #[serde(default = "default_health_probe_timeout_ms")]
    pub probe_timeout_ms: u64,
    #[serde(default = "default_health_suspect_after_ms")]
    pub suspect_after_ms: u64,
    #[serde(default = "default_health_down_after_ms")]
    pub down_after_ms: u64,
    #[serde(default = "default_health_indirect_fanout_min")]
    pub indirect_fanout_min: usize,
    #[serde(default = "default_health_indirect_fanout_max")]
    pub indirect_fanout_max: usize,
}

impl Default for HealthConfig {
    /// # Description:
    ///
    /// Returns baseline peer health settings tuned for small cluster defaults.
    fn default() -> Self {
        Self {
            probe_fanout: default_health_probe_fanout(),
            probe_interval_ms: default_health_probe_interval_ms(),
            probe_timeout_ms: default_health_probe_timeout_ms(),
            suspect_after_ms: default_health_suspect_after_ms(),
            down_after_ms: default_health_down_after_ms(),
            indirect_fanout_min: default_health_indirect_fanout_min(),
            indirect_fanout_max: default_health_indirect_fanout_max(),
        }
    }
}

/// # Description:
///
/// Runtime-friendly health settings after converting persisted millisecond values to durations.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeHealthConfig {
    pub probe_fanout: usize,
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub suspect_after: Duration,
    pub down_after: Duration,
    pub indirect_fanout_min: usize,
    pub indirect_fanout_max: usize,
}

impl HealthConfig {
    /// # Description:
    ///
    /// Converts persisted scalar health settings into strongly typed runtime durations.
    fn as_runtime(&self) -> RuntimeHealthConfig {
        RuntimeHealthConfig {
            probe_fanout: self.probe_fanout,
            probe_interval: Duration::from_millis(self.probe_interval_ms),
            probe_timeout: Duration::from_millis(self.probe_timeout_ms),
            suspect_after: Duration::from_millis(self.suspect_after_ms),
            down_after: Duration::from_millis(self.down_after_ms),
            indirect_fanout_min: self.indirect_fanout_min,
            indirect_fanout_max: self.indirect_fanout_max,
        }
    }
}

/// # Description:
///
/// Runtime registry configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RuntimeRegistryConfig {
    #[serde(default)]
    pub oci: OciRuntimeConfig,
}

/// # Description:
///
/// OCI runtime configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct OciRuntimeConfig {
    #[serde(default)]
    pub host: Option<String>,
}

/// # Description:
///
/// GPU scheduling configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct GpuConfig {
    #[serde(default)]
    pub device_overrides: Option<String>,
}

/// # Description:
///
/// Scheduler capacity reserves kept back from workload placement on each node.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct SchedulerConfig {
    #[serde(default = "default_scheduler_reserved_cpu_millis")]
    pub reserved_cpu_millis: u64,
    #[serde(default = "default_scheduler_reserved_memory_bytes")]
    pub reserved_memory_bytes: u64,
    #[serde(default = "default_scheduler_target_slot_cpu_millis")]
    pub target_slot_cpu_millis: u64,
    #[serde(default = "default_scheduler_target_slot_memory_bytes")]
    pub target_slot_memory_bytes: u64,
    #[serde(default = "default_scheduler_max_slots")]
    pub max_slots: u64,
}

impl Default for SchedulerConfig {
    /// # Description:
    ///
    /// Returns baseline scheduler reserves so Mantissa keeps modest local headroom by default.
    fn default() -> Self {
        Self {
            reserved_cpu_millis: default_scheduler_reserved_cpu_millis(),
            reserved_memory_bytes: default_scheduler_reserved_memory_bytes(),
            target_slot_cpu_millis: default_scheduler_target_slot_cpu_millis(),
            target_slot_memory_bytes: default_scheduler_target_slot_memory_bytes(),
            max_slots: default_scheduler_max_slots(),
        }
    }
}

/// # Description:
///
/// Runtime-friendly scheduler reserve settings used during slot derivation.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeSchedulerConfig {
    pub reserved_cpu_millis: u64,
    pub reserved_memory_bytes: u64,
    pub target_slot_cpu_millis: u64,
    pub target_slot_memory_bytes: u64,
    pub max_slots: u64,
}

impl SchedulerConfig {
    /// # Description:
    ///
    /// Converts persisted scheduler reserve settings into the runtime form used by bootstrap.
    fn as_runtime(&self) -> RuntimeSchedulerConfig {
        RuntimeSchedulerConfig {
            reserved_cpu_millis: self.reserved_cpu_millis,
            reserved_memory_bytes: self.reserved_memory_bytes,
            target_slot_cpu_millis: self.target_slot_cpu_millis,
            target_slot_memory_bytes: self.target_slot_memory_bytes,
            max_slots: self.max_slots,
        }
    }
}

/// # Description:
///
/// Control-plane gossip and anti-entropy tuning for one Mantissa node.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicationConfig {
    #[serde(default = "default_replication_gossip_channel_capacity")]
    pub gossip_channel_capacity: usize,
    #[serde(default = "default_replication_gossip_fanout")]
    pub gossip_fanout: usize,
    #[serde(default = "default_replication_gossip_tick_ms")]
    pub gossip_tick_ms: u64,
    #[serde(default = "default_replication_sync_tick_ms")]
    pub sync_tick_ms: u64,
    #[serde(default = "default_replication_sync_fanout")]
    pub sync_fanout: usize,
    #[serde(default = "default_replication_global_metadata_sync_tick_ms")]
    pub global_metadata_sync_tick_ms: u64,
    #[serde(default = "default_replication_global_metadata_sync_fanout")]
    pub global_metadata_sync_fanout: usize,
    #[serde(default = "default_replication_workload_repair_fanout")]
    pub workload_repair_fanout: usize,
    #[serde(default = "default_replication_remote_admission_parallelism")]
    pub remote_admission_parallelism: usize,
    #[serde(default = "default_replication_remote_assignment_parallelism")]
    pub remote_assignment_parallelism: usize,
    #[serde(default = "default_replication_service_shard_target_threshold")]
    pub service_shard_target_threshold: usize,
    #[serde(default = "default_replication_service_shard_target_size")]
    pub service_shard_target_size: usize,
    #[serde(default = "default_replication_service_shard_task_target_size")]
    pub service_shard_task_target_size: usize,
    #[serde(default = "default_replication_service_shard_parallelism")]
    pub service_shard_parallelism: usize,
}

impl Default for ReplicationConfig {
    /// # Description:
    ///
    /// Returns the default replication settings used when no config is supplied.
    fn default() -> Self {
        Self {
            gossip_channel_capacity: default_replication_gossip_channel_capacity(),
            gossip_fanout: default_replication_gossip_fanout(),
            gossip_tick_ms: default_replication_gossip_tick_ms(),
            sync_tick_ms: default_replication_sync_tick_ms(),
            sync_fanout: default_replication_sync_fanout(),
            global_metadata_sync_tick_ms: default_replication_global_metadata_sync_tick_ms(),
            global_metadata_sync_fanout: default_replication_global_metadata_sync_fanout(),
            workload_repair_fanout: default_replication_workload_repair_fanout(),
            remote_admission_parallelism: default_replication_remote_admission_parallelism(),
            remote_assignment_parallelism: default_replication_remote_assignment_parallelism(),
            service_shard_target_threshold: default_replication_service_shard_target_threshold(),
            service_shard_target_size: default_replication_service_shard_target_size(),
            service_shard_task_target_size: default_replication_service_shard_task_target_size(),
            service_shard_parallelism: default_replication_service_shard_parallelism(),
        }
    }
}

/// # Description:
///
/// Runtime-friendly replication settings after converting persisted millisecond
/// values to durations.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeReplicationConfig {
    pub gossip_channel_capacity: usize,
    pub gossip_fanout: usize,
    pub gossip_tick: Duration,
    pub sync_tick: Duration,
    pub sync_fanout: usize,
    pub global_metadata_sync_tick: Duration,
    pub global_metadata_sync_fanout: usize,
    pub workload_repair_fanout: usize,
    pub remote_admission_parallelism: usize,
    pub remote_assignment_parallelism: usize,
    pub service_shard_target_threshold: usize,
    pub service_shard_target_size: usize,
    pub service_shard_task_target_size: usize,
    pub service_shard_parallelism: usize,
}

impl ReplicationConfig {
    /// # Description:
    ///
    /// Converts persisted replication settings into strongly typed runtime
    /// durations and counters.
    fn as_runtime(&self) -> RuntimeReplicationConfig {
        RuntimeReplicationConfig {
            gossip_channel_capacity: self.gossip_channel_capacity,
            gossip_fanout: self.gossip_fanout,
            gossip_tick: Duration::from_millis(self.gossip_tick_ms),
            sync_tick: Duration::from_millis(self.sync_tick_ms),
            sync_fanout: self.sync_fanout,
            global_metadata_sync_tick: Duration::from_millis(self.global_metadata_sync_tick_ms),
            global_metadata_sync_fanout: self.global_metadata_sync_fanout,
            workload_repair_fanout: self.workload_repair_fanout,
            remote_admission_parallelism: self.remote_admission_parallelism,
            remote_assignment_parallelism: self.remote_assignment_parallelism,
            service_shard_target_threshold: self.service_shard_target_threshold,
            service_shard_target_size: self.service_shard_target_size,
            service_shard_task_target_size: self.service_shard_task_target_size,
            service_shard_parallelism: self.service_shard_parallelism,
        }
    }
}

static GLOBAL_CONFIG: Lazy<RwLock<Config>> = Lazy::new(|| RwLock::new(Config::default()));
static GLOBAL_SOURCE: Lazy<RwLock<ConfigSource>> =
    Lazy::new(|| RwLock::new(ConfigSource::default()));
static GLOBAL_LOADED: AtomicBool = AtomicBool::new(false);

/// # Description:
///
/// Default max entry count for one overlay VIP forward or reverse flow map.
pub const DEFAULT_BPF_OVERLAY_FLOW_CAPACITY: usize = 1024;

/// # Description:
///
/// Default max entry count for one NodePort VIP publication map.
pub const DEFAULT_NODEPORT_VIP_CAPACITY: usize = 1024;

/// # Description:
///
/// Default max entry count for one NodePort forward or reverse flow map.
pub const DEFAULT_NODEPORT_FLOW_CAPACITY: usize = 2048;

/// # Description:
///
/// Default max entry count for one NodePort host-access attachment map.
pub const DEFAULT_NODEPORT_HOST_CAPACITY: usize = 256;

/// # Description:
///
/// Default interval between store GC maintenance passes.
pub const DEFAULT_STORE_GC_INTERVAL_MS: u64 = 60_000;

/// # Description:
///
/// Default minimum local tombstone age before GC can prune it.
pub const DEFAULT_STORE_GC_TOMBSTONE_MIN_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// # Description:
///
/// Default bearer session ticket lifetime used by remote and local session stores.
pub const DEFAULT_SESSION_TICKET_TTL_SECS: u64 = 24 * 60 * 60;

/// # Description:
///
/// Default maximum tombstone rows pruned from one domain in one GC pass.
pub const DEFAULT_STORE_GC_TOMBSTONE_BATCH_LIMIT: usize = 1024;

/// # Description:
///
/// Default local Prometheus/OpenMetrics scrape address for enabled metrics.
pub const DEFAULT_METRICS_LISTEN_ADDR: &str = "127.0.0.1:9600";

/// # Description:
///
/// Default interval for cheap runtime metrics sampling.
pub const DEFAULT_METRICS_SAMPLE_INTERVAL_MS: u64 = 10_000;

/// # Description:
///
/// Default interval for filesystem-backed state database size sampling.
pub const DEFAULT_METRICS_STATE_DB_SAMPLE_INTERVAL_MS: u64 = 60_000;

/// # Description:
///
/// Tracks the origin metadata for the current configuration snapshot.
#[derive(Clone, Debug, Default)]
pub struct ConfigSource {
    pub path: Option<PathBuf>,
    pub env_overrides: bool,
}

/// # Description:
///
/// Load a configuration file (when available), apply environment overrides, and
/// return the config alongside its source metadata.
pub fn load_config_with_source(path: Option<&Path>) -> Result<(Config, ConfigSource)> {
    let mut source = ConfigSource::default();
    let mut config = if let Some(path) = path {
        let loaded = load_config_from_path(path)?;
        source.path = Some(path.to_path_buf());
        loaded
    } else if let Some(path) = find_default_config_path() {
        let loaded = load_config_from_path(&path)?;
        source.path = Some(path);
        loaded
    } else {
        Config::default()
    };

    source.env_overrides = config.apply_env_overrides();
    config.validate()?;
    Ok((config, source))
}

/// # Description:
///
/// Replace the global configuration snapshot with the supplied config.
/// # Description:
///
/// Replace the global configuration and record its origin metadata.
pub fn set_global_config_with_source(config: Config, source: ConfigSource) {
    let mut guard = GLOBAL_CONFIG.write();
    *guard = config;

    let mut source_guard = GLOBAL_SOURCE.write();
    *source_guard = source;
    GLOBAL_LOADED.store(true, Ordering::Release);
}

/// # Description:
///
/// Return a cloned snapshot of the current global configuration.
pub fn global_config() -> Config {
    ensure_config_loaded();
    let guard = GLOBAL_CONFIG.read();
    guard.clone()
}

/// # Description:
///
/// Return a snapshot of the metadata describing where the current config came from.
pub fn global_config_source() -> ConfigSource {
    ensure_config_loaded();
    let guard = GLOBAL_SOURCE.read();
    guard.clone()
}

/// # Description:
///
/// Resolve a configured nodeport IP address, if any, into an IP value.
pub fn nodeport_ip() -> Option<IpAddr> {
    let config = global_config();
    let raw = config.network.nodeport.ip?;
    raw.parse::<IpAddr>().ok()
}

/// # Description:
///
/// Resolve the configured nodeport interface name, if provided.
pub fn nodeport_iface() -> Option<String> {
    global_config().network.nodeport.iface
}

/// # Description:
///
/// Resolve the configured NodePort source-address mode that governs what backends observe.
pub fn nodeport_source_mode() -> NodePortSourceMode {
    global_config().network.nodeport.source_mode
}

/// # Description:
///
/// Resolve the configured nodeport enabled flag.
pub fn nodeport_enabled() -> bool {
    global_config().network.nodeport.enabled
}

/// # Description:
///
/// Resolve the operator-configured advertise address, if one has been supplied.
pub fn advertise_addr() -> Option<String> {
    global_config().network.advertise_addr
}

/// # Description:
///
/// Resolve the configured default IP-family policy used by auto-created networks and best-effort
/// node identity autodetection.
pub fn default_ip_family_policy() -> DefaultIpFamilyPolicy {
    global_config().network.default_ip_family
}

/// # Description:
///
/// Resolve the default realization policy applied when creating networks without an explicit one.
pub fn network_realization_default() -> NetworkRealizationPolicy {
    global_config().network.realization_default
}

/// # Description:
///
/// Resolve whether Mantissa should provision kernel network interfaces on this node.
pub fn kernel_network_provisioning_enabled() -> bool {
    global_config().network.provision_kernel_interfaces
}

/// # Description:
///
/// Resolve the configured BPF attachment toggle.
pub fn bpf_attach_enabled() -> bool {
    global_config().network.bpf.attach
}

/// # Description:
///
/// Resolve the configured BPF artifact directory, if provided.
pub fn bpf_artifact_dir() -> Option<PathBuf> {
    global_config().network.bpf.artifact_dir.map(PathBuf::from)
}

/// # Description:
///
/// Resolve the configured overlay flow-map capacity used when bridge tc programs pin their
/// shared conntrack state.
pub fn bpf_overlay_flow_capacity() -> usize {
    global_config().network.bpf.overlay_flow_capacity
}

/// # Description:
///
/// Resolve the configured NodePort VIP-map capacity used when publishing external selectors.
pub fn nodeport_vip_capacity() -> usize {
    global_config().network.nodeport.vip_capacity
}

/// # Description:
///
/// Resolve the configured NodePort host-access map capacity used for per-network SNAT state.
pub fn nodeport_host_capacity() -> usize {
    global_config().network.nodeport.host_capacity
}

/// # Description:
///
/// Resolve the configured NodePort flow-map capacity used by the public ingress conntrack cache.
pub fn nodeport_flow_capacity() -> usize {
    global_config().network.nodeport.flow_capacity
}

/// # Description:
///
/// Resolve the scheduler reserve configuration used while deriving allocatable capacity.
pub fn scheduler_runtime_config() -> RuntimeSchedulerConfig {
    global_config().scheduler.as_runtime()
}

/// # Description:
///
/// Resolve the peer-health runtime configuration used by liveness probing loops.
pub fn health_runtime_config() -> RuntimeHealthConfig {
    global_config().health.as_runtime()
}

/// # Description:
///
/// Resolve the control-plane replication runtime configuration used by gossip
/// and anti-entropy startup paths.
pub fn replication_runtime_config() -> RuntimeReplicationConfig {
    global_config().replication.as_runtime()
}

/// # Description:
///
/// Resolve the store GC runtime configuration used by the maintenance runner.
pub fn store_gc_runtime_config() -> RuntimeStoreGcConfig {
    global_config().storage.gc.as_runtime()
}

/// # Description:
///
/// Resolve the maximum lifetime for durable cluster session tickets.
pub fn session_ticket_ttl_secs() -> u64 {
    global_config().security.session_ticket_ttl_secs
}

/// # Description:
///
/// Resolve the configured metrics exporter and sampler settings.
pub fn metrics_runtime_config() -> Result<RuntimeMetricsConfig> {
    global_config().observability.metrics.as_runtime()
}

/// # Description:
///
/// Resolve whether WireGuard underlay is enabled on this node.
pub fn wireguard_enabled() -> bool {
    global_config().network.wireguard.enabled
}

/// # Description:
///
/// Resolve the WireGuard listen port override, if configured.
pub fn wireguard_port_override() -> Option<u16> {
    global_config().network.wireguard.port
}

/// # Description:
///
/// Resolve whether Mantissa should manage the WireGuard firewall rules.
pub fn wireguard_manage_firewall() -> bool {
    global_config().network.wireguard.manage_firewall
}

/// # Description:
///
/// Resolve the OCI runtime host override, if configured.
pub fn oci_runtime_host() -> Option<String> {
    global_config().runtimes.oci.host
}

/// # Description:
///
/// Resolve the configured GPU device override string, if present.
pub fn gpu_device_overrides() -> Option<String> {
    global_config().gpu.device_overrides
}

/// # Description:
///
/// Resolve the local on-disk root used to materialize managed volume directories on this node.
pub fn local_volume_root() -> Result<PathBuf> {
    let configured = global_config()
        .storage
        .local_volume_root
        .map(|path| PathBuf::from(path.trim()))
        .filter(|path| !path.as_os_str().is_empty());
    let root = configured.unwrap_or(ensure_state_dir()?.join("volumes"));
    ensure_local_volume_root(&root)?;
    Ok(root)
}

/// # Description:
///
/// Return whether requested local-volume capacity should be enforced as a runtime cutoff.
pub fn local_volume_enforce_capacity() -> bool {
    global_config().storage.local_volume_enforce_capacity
}

/// Returns the operator's replicated-volume settings, if they were supplied.
pub fn replicated_volume_config() -> Option<ReplicatedVolumeConfig> {
    global_config().storage.replicated_volumes
}

/// # Description:
///
/// Render a config snapshot as pretty-printed RON for diagnostics.
pub fn render_config_ron(config: &Config) -> Result<String> {
    let pretty = ron::ser::PrettyConfig::default();
    ron::ser::to_string_pretty(config, pretty).context("failed to serialize config to RON")
}

/// # Description:
///
/// Start a background watcher that reloads configuration when the config file changes.
pub fn spawn_config_watcher() -> Option<std::thread::JoinHandle<()>> {
    let source = global_config_source();
    let path = source.path?;
    match start_config_watch_thread(path) {
        Ok(handle) => Some(handle),
        Err(err) => {
            warn!(target: "config", "failed to spawn config watcher thread: {err}");
            None
        }
    }
}

/// # Description:
///
/// Validate one operator-supplied advertise address before startup publishes it to peers.
fn validate_advertise_addr(raw: &str) -> Result<()> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("network.advertise_addr cannot be empty");
    }

    let _ = crate::node::address::extract_port(trimmed).with_context(|| {
        format!("network.advertise_addr must include a concrete port (got '{trimmed}')")
    })?;

    if let Ok(socket_addr) = trimmed.parse::<SocketAddr>()
        && socket_addr.ip().is_unspecified()
    {
        anyhow::bail!("network.advertise_addr must not use an unspecified IP (got '{trimmed}')");
    }

    Ok(())
}

/// # Description:
///
/// Validate one configured eBPF map capacity so the loader can program it directly into the
/// kernel before map creation.
fn validate_map_capacity(name: &str, value: usize) -> Result<()> {
    if value == 0 {
        anyhow::bail!("{name} must be greater than zero");
    }

    if value > u32::MAX as usize {
        anyhow::bail!("{name} must be less than or equal to {}", u32::MAX);
    }

    Ok(())
}

/// # Description:
///
/// Parse one NodePort source-mode label from environment overrides or operator-facing diagnostics.
fn parse_nodeport_source_mode(raw: &str) -> Option<NodePortSourceMode> {
    match raw.trim() {
        "snat_host_access" => Some(NodePortSourceMode::SnatHostAccess),
        "preserve_client" => Some(NodePortSourceMode::PreserveClient),
        _ => None,
    }
}

/// # Description:
///
/// Return a default true value for serde defaults.
fn default_true() -> bool {
    true
}

/// # Description:
///
/// Returns the default interval between store GC maintenance passes.
fn default_store_gc_interval_ms() -> u64 {
    DEFAULT_STORE_GC_INTERVAL_MS
}

/// # Description:
///
/// Returns the default tombstone retention window for replicated store GC.
fn default_store_gc_tombstone_min_retention_ms() -> u64 {
    DEFAULT_STORE_GC_TOMBSTONE_MIN_RETENTION_MS
}

/// # Description:
///
/// Returns the default bounded tombstone batch size for one store GC pass.
fn default_store_gc_tombstone_batch_limit() -> usize {
    DEFAULT_STORE_GC_TOMBSTONE_BATCH_LIMIT
}

/// # Description:
///
/// Returns the default offline-peer guard window paired with tombstone retention.
fn default_store_gc_stale_peer_rejoin_after_ms() -> u64 {
    DEFAULT_STORE_GC_TOMBSTONE_MIN_RETENTION_MS
}

/// # Description:
///
/// Returns the default durable session ticket lifetime in seconds.
fn default_session_ticket_ttl_secs() -> u64 {
    DEFAULT_SESSION_TICKET_TTL_SECS
}

/// # Description:
///
/// Returns the default local metrics scrape listener.
fn default_metrics_listen_addr() -> String {
    DEFAULT_METRICS_LISTEN_ADDR.to_string()
}

/// # Description:
///
/// Returns the parsed default metrics listener without relying on fallible parsing.
fn default_metrics_socket_addr() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 9600))
}

/// # Description:
///
/// Returns the default cheap metrics sampler interval.
fn default_metrics_sample_interval_ms() -> u64 {
    DEFAULT_METRICS_SAMPLE_INTERVAL_MS
}

/// # Description:
///
/// Returns the default slow state database size sampler interval.
fn default_metrics_state_db_sample_interval_ms() -> u64 {
    DEFAULT_METRICS_STATE_DB_SAMPLE_INTERVAL_MS
}

/// # Description:
///
/// Returns the default overlay conntrack flow-map capacity.
fn default_bpf_overlay_flow_capacity() -> usize {
    DEFAULT_BPF_OVERLAY_FLOW_CAPACITY
}

/// # Description:
///
/// Returns the default NodePort VIP-map capacity.
fn default_nodeport_vip_capacity() -> usize {
    DEFAULT_NODEPORT_VIP_CAPACITY
}

/// # Description:
///
/// Returns the default NodePort host-access map capacity.
fn default_nodeport_host_capacity() -> usize {
    DEFAULT_NODEPORT_HOST_CAPACITY
}

/// # Description:
///
/// Returns the default NodePort public flow-map capacity.
fn default_nodeport_flow_capacity() -> usize {
    DEFAULT_NODEPORT_FLOW_CAPACITY
}

/// # Description:
///
/// Returns the default SWIM operator floor for indirect helper fanout.
fn default_health_probe_fanout() -> usize {
    5
}

/// # Description:
///
/// Returns the default interval between SWIM probe passes.
fn default_health_probe_interval_ms() -> u64 {
    1_000
}

/// # Description:
///
/// Returns the default timeout budget for one SWIM ping.
fn default_health_probe_timeout_ms() -> u64 {
    1_000
}

/// # Description:
///
/// Returns the default suspect threshold before SWIM escalates to suspect.
fn default_health_suspect_after_ms() -> u64 {
    2_000
}

/// # Description:
///
/// Returns the default down threshold after SWIM suspicion is raised.
fn default_health_down_after_ms() -> u64 {
    6_000
}

/// # Description:
///
/// Returns the minimum adaptive helper fanout for SWIM indirect probes.
fn default_health_indirect_fanout_min() -> usize {
    3
}

/// # Description:
///
/// Returns the maximum adaptive helper fanout for SWIM indirect probes.
fn default_health_indirect_fanout_max() -> usize {
    32
}

/// # Description:
///
/// Returns the default CPU reserve kept for Mantissa and system activity on each node.
fn default_scheduler_reserved_cpu_millis() -> u64 {
    250
}

/// # Description:
///
/// Returns the default memory reserve kept for Mantissa and system activity on each node.
fn default_scheduler_reserved_memory_bytes() -> u64 {
    256 * 1024 * 1024
}

/// # Description:
///
/// Returns the default target CPU granularity used while deriving scheduler slots.
fn default_scheduler_target_slot_cpu_millis() -> u64 {
    100
}

/// # Description:
///
/// Returns the default target memory granularity used while deriving scheduler slots.
fn default_scheduler_target_slot_memory_bytes() -> u64 {
    128 * 1024 * 1024
}

/// # Description:
///
/// Returns the default operator slot cap used while deriving scheduler slots.
fn default_scheduler_max_slots() -> u64 {
    SCHEDULER_MAX_SLOT_COUNT
}

/// # Description:
///
/// Returns the default shared gossip channel capacity used by the daemon path.
fn default_replication_gossip_channel_capacity() -> usize {
    128
}

/// # Description:
///
/// Returns the default outbound gossip fanout.
fn default_replication_gossip_fanout() -> usize {
    5
}

/// # Description:
///
/// Returns the default gossip dispatch tick interval in milliseconds.
fn default_replication_gossip_tick_ms() -> u64 {
    1_000
}

/// # Description:
///
/// Returns the default periodic sync tick interval in milliseconds.
fn default_replication_sync_tick_ms() -> u64 {
    5_000
}

/// # Description:
///
/// Returns the default number of peers sampled by the main sync loop.
fn default_replication_sync_fanout() -> usize {
    8
}

/// # Description:
///
/// Returns the default cross-view metadata sync tick interval in milliseconds.
fn default_replication_global_metadata_sync_tick_ms() -> u64 {
    5_000
}

/// # Description:
///
/// Returns the default cross-view metadata sync fanout.
fn default_replication_global_metadata_sync_fanout() -> usize {
    8
}

/// # Description:
///
/// Returns the default deterministic workload-repair fanout.
fn default_replication_workload_repair_fanout() -> usize {
    1
}

/// # Description:
///
/// Returns the default owner-side concurrency for remote scheduler admission.
fn default_replication_remote_admission_parallelism() -> usize {
    8
}

/// # Description:
///
/// Returns the default owner-side concurrency for remote assignment delivery.
fn default_replication_remote_assignment_parallelism() -> usize {
    16
}

/// # Description:
///
/// Returns the default target-peer count above which service deployment uses shard planning.
fn default_replication_service_shard_target_threshold() -> usize {
    256
}

/// # Description:
///
/// Returns the default maximum number of target peers assigned to one deployment shard.
fn default_replication_service_shard_target_size() -> usize {
    128
}

/// # Description:
///
/// Returns the default maximum number of replica starts carried by one deployment shard RPC.
fn default_replication_service_shard_task_target_size() -> usize {
    128
}

/// # Description:
///
/// Returns the default maximum number of service shard coordinators contacted in parallel.
fn default_replication_service_shard_parallelism() -> usize {
    16
}

/// # Description:
///
/// Ensure the global configuration has been loaded at least once.
fn ensure_config_loaded() {
    if GLOBAL_LOADED.load(Ordering::Acquire) {
        return;
    }

    if GLOBAL_LOADED
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    let mut config = Config::default();
    let env_overrides = config.apply_env_overrides();
    if let Err(err) = config.validate() {
        warn!(
            target: "config",
            "default config validation failed: {err}"
        );
    }
    let source = ConfigSource {
        path: None,
        env_overrides,
    };
    let mut guard = GLOBAL_CONFIG.write();
    *guard = config;

    let mut source_guard = GLOBAL_SOURCE.write();
    *source_guard = source;
}

/// # Description:
///
/// Load and parse a RON config file from the provided path.
fn load_config_from_path(path: &Path) -> Result<Config> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let config: Config = ron::from_str(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))?;
    Ok(config)
}

/// # Description:
///
/// Locate the first available config file in the default search paths.
fn find_default_config_path() -> Option<PathBuf> {
    let mut paths = Vec::new();
    paths.push(PathBuf::from("/etc/mantissa/config.ron"));
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        paths.push(home.join(".config/mantissa/config.ron"));
        paths.push(home.join(".mantissa/config.ron"));
    }
    if let Ok(cwd) = std::env::current_dir() {
        paths.push(cwd.join("mantissa.ron"));
    }

    paths.into_iter().find(|path| path.exists())
}

impl Config {
    /// # Description:
    ///
    /// Apply environment variable overrides for legacy compatibility.
    fn apply_env_overrides(&mut self) -> bool {
        let mut applied = false;
        if std::env::var_os("MANTISSA_WIREGUARD_DISABLE").is_some() {
            self.network.wireguard.enabled = false;
            applied = true;
        }

        if let Ok(raw) = std::env::var("MANTISSA_WIREGUARD_PORT") {
            applied = true;
            match raw.trim().parse::<u16>() {
                Ok(port) if port > 0 => self.network.wireguard.port = Some(port),
                _ => warn!(
                    target: "config",
                    "ignoring invalid MANTISSA_WIREGUARD_PORT '{raw}'"
                ),
            }
        }

        if std::env::var_os("MANTISSA_WIREGUARD_NO_FIREWALL").is_some() {
            self.network.wireguard.manage_firewall = false;
            applied = true;
        }

        if std::env::var_os("MANTISSA_BPF_NO_ATTACH").is_some()
            || std::env::var_os("MANTISSA_SKIP_BPF").is_some()
        {
            self.network.bpf.attach = false;
            self.network.nodeport.enabled = false;
            applied = true;
        }

        if let Ok(dir) = std::env::var("MANTISSA_BPF_DIR") {
            applied = true;
            let dir = dir.trim();
            if !dir.is_empty() {
                self.network.bpf.artifact_dir = Some(dir.to_string());
            }
        }

        applied |= apply_positive_usize_env_override(
            "MANTISSA_BPF_OVERLAY_FLOW_CAPACITY",
            &mut self.network.bpf.overlay_flow_capacity,
        );

        if let Ok(iface) = std::env::var("MANTISSA_NODEPORT_IFACE") {
            applied = true;
            let iface = iface.trim();
            if !iface.is_empty() {
                self.network.nodeport.iface = Some(iface.to_string());
            }
        }

        if let Ok(ip) = std::env::var("MANTISSA_NODEPORT_IP") {
            applied = true;
            let ip = ip.trim();
            if !ip.is_empty() {
                self.network.nodeport.ip = Some(ip.to_string());
            }
        }

        if let Ok(raw) = std::env::var("MANTISSA_NODEPORT_SOURCE_MODE") {
            applied = true;
            match parse_nodeport_source_mode(raw.trim()) {
                Some(mode) => self.network.nodeport.source_mode = mode,
                None => warn!("ignoring invalid MANTISSA_NODEPORT_SOURCE_MODE '{raw}'"),
            }
        }

        applied |= apply_positive_usize_env_override(
            "MANTISSA_NODEPORT_VIP_CAPACITY",
            &mut self.network.nodeport.vip_capacity,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_NODEPORT_HOST_CAPACITY",
            &mut self.network.nodeport.host_capacity,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_NODEPORT_FLOW_CAPACITY",
            &mut self.network.nodeport.flow_capacity,
        );

        if let Ok(addr) = std::env::var("MANTISSA_ADVERTISE_ADDR") {
            applied = true;
            let addr = addr.trim();
            if !addr.is_empty() {
                self.network.advertise_addr = Some(addr.to_string());
            }
        }
        if let Ok(raw) = std::env::var("MANTISSA_DEFAULT_IP_FAMILY") {
            applied = true;
            match raw.trim() {
                "auto" => self.network.default_ip_family = DefaultIpFamilyPolicy::Auto,
                "ipv4" => self.network.default_ip_family = DefaultIpFamilyPolicy::Ipv4,
                "ipv6" => self.network.default_ip_family = DefaultIpFamilyPolicy::Ipv6,
                other => warn!("ignoring invalid MANTISSA_DEFAULT_IP_FAMILY '{other}'"),
            }
        }

        if let Ok(host) = std::env::var("MANTISSA_RUNTIME_OCI_HOST") {
            applied = true;
            let host = host.trim();
            if !host.is_empty() {
                self.runtimes.oci.host = Some(host.to_string());
            }
        }

        if let Ok(raw) = std::env::var("MANTISSA_GPU_DEVICE_OVERRIDES") {
            applied = true;
            let raw = raw.trim();
            if !raw.is_empty() {
                self.gpu.device_overrides = Some(raw.to_string());
            }
        }

        if let Ok(path) = std::env::var("MANTISSA_LOCAL_VOLUME_ROOT") {
            applied = true;
            let path = path.trim();
            if !path.is_empty() {
                self.storage.local_volume_root = Some(path.to_string());
            }
        }

        if std::env::var_os("MANTISSA_LOCAL_VOLUME_ENFORCE_CAPACITY").is_some() {
            applied = true;
            self.storage.local_volume_enforce_capacity = true;
        }

        applied |= apply_positive_u64_env_override(
            "MANTISSA_SESSION_TICKET_TTL_SECS",
            &mut self.security.session_ticket_ttl_secs,
        );

        if std::env::var_os("MANTISSA_METRICS_ENABLE").is_some() {
            applied = true;
            self.observability.metrics.enabled = true;
        }

        if let Ok(addr) = std::env::var("MANTISSA_METRICS_LISTEN_ADDR") {
            applied = true;
            let addr = addr.trim();
            if !addr.is_empty() {
                self.observability.metrics.listen_addr = addr.to_string();
            }
        }

        applied |= apply_positive_u64_env_override(
            "MANTISSA_METRICS_SAMPLE_INTERVAL_MS",
            &mut self.observability.metrics.sample_interval_ms,
        );
        applied |= apply_positive_u64_env_override(
            "MANTISSA_METRICS_STATE_DB_SAMPLE_INTERVAL_MS",
            &mut self.observability.metrics.state_db_sample_interval_ms,
        );

        applied |= apply_u64_env_override(
            "MANTISSA_SCHEDULER_RESERVED_CPU_MILLIS",
            &mut self.scheduler.reserved_cpu_millis,
        );
        applied |= apply_u64_env_override(
            "MANTISSA_SCHEDULER_RESERVED_MEMORY_BYTES",
            &mut self.scheduler.reserved_memory_bytes,
        );
        applied |= apply_positive_u64_env_override(
            "MANTISSA_SCHEDULER_TARGET_SLOT_CPU_MILLIS",
            &mut self.scheduler.target_slot_cpu_millis,
        );
        applied |= apply_positive_u64_env_override(
            "MANTISSA_SCHEDULER_TARGET_SLOT_MEMORY_BYTES",
            &mut self.scheduler.target_slot_memory_bytes,
        );
        applied |= apply_positive_u64_env_override(
            "MANTISSA_SCHEDULER_MAX_SLOTS",
            &mut self.scheduler.max_slots,
        );

        applied |= apply_positive_usize_env_override(
            "MANTISSA_GOSSIP_CHANNEL_CAPACITY",
            &mut self.replication.gossip_channel_capacity,
        );
        applied |= apply_usize_env_override(
            "MANTISSA_GOSSIP_FANOUT",
            &mut self.replication.gossip_fanout,
        );
        applied |= apply_positive_u64_env_override(
            "MANTISSA_GOSSIP_TICK_MS",
            &mut self.replication.gossip_tick_ms,
        );
        applied |= apply_positive_u64_env_override(
            "MANTISSA_SYNC_TICK_MS",
            &mut self.replication.sync_tick_ms,
        );
        applied |=
            apply_usize_env_override("MANTISSA_SYNC_FANOUT", &mut self.replication.sync_fanout);
        applied |= apply_positive_u64_env_override(
            "MANTISSA_GLOBAL_METADATA_SYNC_TICK_MS",
            &mut self.replication.global_metadata_sync_tick_ms,
        );
        applied |= apply_usize_env_override(
            "MANTISSA_GLOBAL_METADATA_SYNC_FANOUT",
            &mut self.replication.global_metadata_sync_fanout,
        );
        applied |= apply_usize_env_override(
            "MANTISSA_WORKLOAD_REPAIR_FANOUT",
            &mut self.replication.workload_repair_fanout,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_REMOTE_ADMISSION_PARALLELISM",
            &mut self.replication.remote_admission_parallelism,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_REMOTE_ASSIGNMENT_PARALLELISM",
            &mut self.replication.remote_assignment_parallelism,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_SERVICE_SHARD_TARGET_THRESHOLD",
            &mut self.replication.service_shard_target_threshold,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_SERVICE_SHARD_TARGET_SIZE",
            &mut self.replication.service_shard_target_size,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_SERVICE_SHARD_TASK_TARGET_SIZE",
            &mut self.replication.service_shard_task_target_size,
        );
        applied |= apply_positive_usize_env_override(
            "MANTISSA_SERVICE_SHARD_PARALLELISM",
            &mut self.replication.service_shard_parallelism,
        );

        applied
    }

    /// # Description:
    ///
    /// Validate configuration values so runtime components receive sane inputs.
    pub fn validate(&self) -> Result<()> {
        if let Some(port) = self.network.wireguard.port
            && port == 0
        {
            anyhow::bail!("network.wireguard.port must be non-zero");
        }

        validate_map_capacity(
            "network.bpf.overlay_flow_capacity",
            self.network.bpf.overlay_flow_capacity,
        )?;
        validate_map_capacity(
            "network.nodeport.vip_capacity",
            self.network.nodeport.vip_capacity,
        )?;
        validate_map_capacity(
            "network.nodeport.host_capacity",
            self.network.nodeport.host_capacity,
        )?;
        validate_map_capacity(
            "network.nodeport.flow_capacity",
            self.network.nodeport.flow_capacity,
        )?;

        if let Some(ref ip) = self.network.nodeport.ip
            && ip.parse::<IpAddr>().is_err()
        {
            anyhow::bail!("network.nodeport.ip must be a valid IP address (got '{ip}')");
        }

        if let Some(ref iface) = self.network.nodeport.iface
            && iface.trim().is_empty()
        {
            anyhow::bail!("network.nodeport.iface cannot be empty");
        }

        if self.network.nodeport.source_mode == NodePortSourceMode::PreserveClient {
            anyhow::bail!(
                "network.nodeport.source_mode 'preserve_client' is not supported yet; use 'snat_host_access'"
            );
        }

        if let Some(ref addr) = self.network.advertise_addr {
            validate_advertise_addr(addr)?;
        }

        if let Some(ref host) = self.runtimes.oci.host
            && host.trim().is_empty()
        {
            anyhow::bail!("runtimes.oci.host cannot be empty");
        }

        if let Some(ref overrides) = self.gpu.device_overrides
            && overrides.trim().is_empty()
        {
            anyhow::bail!("gpu.device_overrides cannot be empty");
        }

        if let Some(ref path) = self.storage.local_volume_root {
            if path.trim().is_empty() {
                anyhow::bail!("storage.local_volume_root cannot be empty");
            }
            if !Path::new(path).is_absolute() {
                anyhow::bail!("storage.local_volume_root must be an absolute path");
            }
        }

        if let Some(replicated_volumes) = self.storage.replicated_volumes.as_ref() {
            replicated_volumes.validate()?;
        }

        if self.storage.gc.interval_ms == 0 {
            anyhow::bail!("storage.gc.interval_ms must be greater than zero");
        }

        if self.storage.gc.tombstone_min_retention_ms == 0 {
            anyhow::bail!("storage.gc.tombstone_min_retention_ms must be greater than zero");
        }

        if self.storage.gc.tombstone_batch_limit == 0 {
            anyhow::bail!("storage.gc.tombstone_batch_limit must be greater than zero");
        }

        if let Some(max_values) = self.storage.gc.mvreg_max_values
            && max_values == 0
        {
            anyhow::bail!("storage.gc.mvreg_max_values must be greater than zero when set");
        }

        if self.storage.gc.mvreg_max_values.is_some() && self.storage.gc.mvreg_batch_limit == 0 {
            anyhow::bail!(
                "storage.gc.mvreg_batch_limit must be greater than zero when MVReg compaction is enabled"
            );
        }

        if self.storage.gc.stale_peer_rejoin_after_ms == 0 {
            anyhow::bail!("storage.gc.stale_peer_rejoin_after_ms must be greater than zero");
        }

        if self.storage.gc.stale_peer_rejoin_after_ms > self.storage.gc.tombstone_min_retention_ms {
            anyhow::bail!(
                "storage.gc.stale_peer_rejoin_after_ms must be less than or equal to storage.gc.tombstone_min_retention_ms"
            );
        }

        if self.security.session_ticket_ttl_secs == 0 {
            anyhow::bail!("security.session_ticket_ttl_secs must be greater than zero");
        }

        if self.observability.metrics.enabled {
            self.observability
                .metrics
                .listen_addr
                .parse::<SocketAddr>()
                .with_context(|| {
                    format!(
                        "observability.metrics.listen_addr must be a socket address (got '{}')",
                        self.observability.metrics.listen_addr
                    )
                })?;
        }

        if self.observability.metrics.sample_interval_ms == 0 {
            anyhow::bail!("observability.metrics.sample_interval_ms must be greater than zero");
        }

        if self.observability.metrics.state_db_sample_interval_ms == 0 {
            anyhow::bail!(
                "observability.metrics.state_db_sample_interval_ms must be greater than zero"
            );
        }

        if self.scheduler.target_slot_cpu_millis == 0 {
            anyhow::bail!("scheduler.target_slot_cpu_millis must be greater than zero");
        }

        if self.scheduler.target_slot_memory_bytes == 0 {
            anyhow::bail!("scheduler.target_slot_memory_bytes must be greater than zero");
        }

        if self.scheduler.max_slots == 0 {
            anyhow::bail!("scheduler.max_slots must be greater than zero");
        }

        if self.scheduler.max_slots > SCHEDULER_MAX_SLOT_COUNT {
            anyhow::bail!(
                "scheduler.max_slots must be less than or equal to {SCHEDULER_MAX_SLOT_COUNT}"
            );
        }

        if self.health.probe_fanout == 0 {
            anyhow::bail!("health.probe_fanout must be greater than zero");
        }

        if self.health.probe_interval_ms == 0 {
            anyhow::bail!("health.probe_interval_ms must be greater than zero");
        }

        if self.health.probe_timeout_ms == 0 {
            anyhow::bail!("health.probe_timeout_ms must be greater than zero");
        }

        if self.health.suspect_after_ms == 0 {
            anyhow::bail!("health.suspect_after_ms must be greater than zero");
        }

        if self.health.down_after_ms == 0 {
            anyhow::bail!("health.down_after_ms must be greater than zero");
        }

        if self.health.down_after_ms <= self.health.suspect_after_ms {
            anyhow::bail!("health.down_after_ms must be greater than health.suspect_after_ms");
        }

        if self.health.indirect_fanout_min == 0 {
            anyhow::bail!("health.indirect_fanout_min must be greater than zero");
        }

        if self.health.indirect_fanout_max == 0 {
            anyhow::bail!("health.indirect_fanout_max must be greater than zero");
        }

        if self.health.indirect_fanout_max < self.health.indirect_fanout_min {
            anyhow::bail!(
                "health.indirect_fanout_max must be greater than or equal to health.indirect_fanout_min"
            );
        }

        if self.network.nodeport.enabled && !self.network.bpf.attach {
            anyhow::bail!("network.nodeport.enabled requires network.bpf.attach to be true");
        }

        if self.replication.gossip_channel_capacity == 0 {
            anyhow::bail!("replication.gossip_channel_capacity must be greater than zero");
        }

        if self.replication.gossip_tick_ms == 0 {
            anyhow::bail!("replication.gossip_tick_ms must be greater than zero");
        }

        if self.replication.sync_tick_ms == 0 {
            anyhow::bail!("replication.sync_tick_ms must be greater than zero");
        }

        if self.replication.global_metadata_sync_tick_ms == 0 {
            anyhow::bail!("replication.global_metadata_sync_tick_ms must be greater than zero");
        }

        if self.replication.remote_admission_parallelism == 0 {
            anyhow::bail!("replication.remote_admission_parallelism must be greater than zero");
        }

        if self.replication.remote_assignment_parallelism == 0 {
            anyhow::bail!("replication.remote_assignment_parallelism must be greater than zero");
        }

        if self.replication.service_shard_target_threshold == 0 {
            anyhow::bail!("replication.service_shard_target_threshold must be greater than zero");
        }

        if self.replication.service_shard_target_size == 0 {
            anyhow::bail!("replication.service_shard_target_size must be greater than zero");
        }

        if self.replication.service_shard_task_target_size == 0 {
            anyhow::bail!("replication.service_shard_task_target_size must be greater than zero");
        }

        if self.replication.service_shard_parallelism == 0 {
            anyhow::bail!("replication.service_shard_parallelism must be greater than zero");
        }

        Ok(())
    }
}

/// # Description:
///
/// Applies one non-negative `usize` environment override into the provided
/// destination, logging invalid values and returning whether an override was
/// attempted.
fn apply_usize_env_override(name: &str, dest: &mut usize) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };

    match raw.trim().parse::<usize>() {
        Ok(value) => *dest = value,
        Err(_) => warn!(target: "config", "ignoring invalid {name} '{raw}'"),
    }

    true
}

/// # Description:
///
/// Applies one strictly positive `usize` environment override into the
/// provided destination, logging invalid values and returning whether an
/// override was attempted.
fn apply_positive_usize_env_override(name: &str, dest: &mut usize) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };

    match raw.trim().parse::<usize>() {
        Ok(value) if value > 0 => *dest = value,
        _ => warn!(target: "config", "ignoring invalid {name} '{raw}'"),
    }

    true
}

/// # Description:
///
/// Applies one non-negative `u64` environment override into the provided
/// destination, logging invalid values and returning whether an override was
/// attempted.
fn apply_u64_env_override(name: &str, dest: &mut u64) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };

    match raw.trim().parse::<u64>() {
        Ok(value) => *dest = value,
        Err(_) => warn!(target: "config", "ignoring invalid {name} '{raw}'"),
    }

    true
}

/// # Description:
///
/// Applies one strictly positive `u64` environment override into the provided
/// destination, logging invalid values and returning whether an override was
/// attempted.
fn apply_positive_u64_env_override(name: &str, dest: &mut u64) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };

    match raw.trim().parse::<u64>() {
        Ok(value) if value > 0 => *dest = value,
        _ => warn!(target: "config", "ignoring invalid {name} '{raw}'"),
    }

    true
}

/// # Description:
///
/// Start a watcher thread for the provided config path and reload on changes.
fn start_config_watch_thread(path: PathBuf) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("mantissa-config-watch".to_string())
        .spawn(move || {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher = match RecommendedWatcher::new(tx, notify::Config::default()) {
                Ok(watcher) => watcher,
                Err(err) => {
                    warn!(target: "config", "failed to create config watcher: {err}");
                    return;
                }
            };

            if let Err(err) = watcher.watch(&path, RecursiveMode::NonRecursive) {
                warn!(
                    target: "config",
                    path = %path.display(),
                    "failed to watch config path: {err}"
                );
                return;
            }

            let mut last_reload = Instant::now()
                .checked_sub(Duration::from_secs(5))
                .unwrap_or_else(Instant::now);

            loop {
                let event = match rx.recv() {
                    Ok(Ok(event)) => event,
                    Ok(Err(err)) => {
                        warn!(target: "config", "config watcher error: {err}");
                        continue;
                    }
                    Err(err) => {
                        warn!(target: "config", "config watcher channel closed: {err}");
                        break;
                    }
                };

                if !should_reload_for_event(&event.kind) {
                    continue;
                }

                if last_reload.elapsed() < Duration::from_millis(200) {
                    continue;
                }
                last_reload = Instant::now();

                match load_config_with_source(Some(&path)) {
                    Ok((new_config, new_source)) => {
                        let previous = global_config();
                        let restart_required = restart_required_changes(&previous, &new_config);
                        if !restart_required.is_empty() {
                            warn!(
                                target: "config",
                                "config change requires restart to fully apply: {}",
                                restart_required.join(", ")
                            );
                        }
                        set_global_config_with_source(new_config, new_source);
                    }
                    Err(err) => {
                        warn!(
                            target: "config",
                            path = %path.display(),
                            "failed to reload config: {err}"
                        );
                    }
                }
            }
        })
}

/// # Description:
///
/// Decide whether a notify event should trigger a reload.
fn should_reload_for_event(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_) | EventKind::Any
    )
}

/// # Description:
///
/// Returns the list of config fields that require a restart to fully apply.
fn restart_required_changes(old: &Config, new: &Config) -> Vec<String> {
    let mut changes = Vec::new();

    if old.runtimes.oci.host != new.runtimes.oci.host {
        changes.push("runtimes.oci.host".to_string());
    }

    if old.gpu.device_overrides != new.gpu.device_overrides {
        changes.push("gpu.device_overrides".to_string());
    }

    if old.storage.local_volume_root != new.storage.local_volume_root {
        changes.push("storage.local_volume_root".to_string());
    }

    if old.storage.local_volume_enforce_capacity != new.storage.local_volume_enforce_capacity {
        changes.push("storage.local_volume_enforce_capacity".to_string());
    }

    if old.storage.replicated_volumes != new.storage.replicated_volumes {
        changes.push("storage.replicated_volumes".to_string());
    }

    if old.storage.gc != new.storage.gc {
        changes.push("storage.gc".to_string());
    }

    if old.security.session_ticket_ttl_secs != new.security.session_ticket_ttl_secs {
        changes.push("security.session_ticket_ttl_secs".to_string());
    }

    if old.observability.metrics != new.observability.metrics {
        changes.push("observability.metrics".to_string());
    }

    if old.scheduler.reserved_cpu_millis != new.scheduler.reserved_cpu_millis {
        changes.push("scheduler.reserved_cpu_millis".to_string());
    }

    if old.scheduler.reserved_memory_bytes != new.scheduler.reserved_memory_bytes {
        changes.push("scheduler.reserved_memory_bytes".to_string());
    }

    if old.scheduler.target_slot_cpu_millis != new.scheduler.target_slot_cpu_millis {
        changes.push("scheduler.target_slot_cpu_millis".to_string());
    }

    if old.scheduler.target_slot_memory_bytes != new.scheduler.target_slot_memory_bytes {
        changes.push("scheduler.target_slot_memory_bytes".to_string());
    }

    if old.scheduler.max_slots != new.scheduler.max_slots {
        changes.push("scheduler.max_slots".to_string());
    }

    if old.network.nodeport.enabled != new.network.nodeport.enabled {
        changes.push("network.nodeport.enabled".to_string());
    }

    if old.network.nodeport.iface != new.network.nodeport.iface {
        changes.push("network.nodeport.iface".to_string());
    }

    if old.network.nodeport.ip != new.network.nodeport.ip {
        changes.push("network.nodeport.ip".to_string());
    }

    if old.network.nodeport.source_mode != new.network.nodeport.source_mode {
        changes.push("network.nodeport.source_mode".to_string());
    }

    if old.network.nodeport.vip_capacity != new.network.nodeport.vip_capacity {
        changes.push("network.nodeport.vip_capacity".to_string());
    }

    if old.network.nodeport.host_capacity != new.network.nodeport.host_capacity {
        changes.push("network.nodeport.host_capacity".to_string());
    }

    if old.network.nodeport.flow_capacity != new.network.nodeport.flow_capacity {
        changes.push("network.nodeport.flow_capacity".to_string());
    }

    if old.network.advertise_addr != new.network.advertise_addr {
        changes.push("network.advertise_addr".to_string());
    }

    if old.network.default_ip_family != new.network.default_ip_family {
        changes.push("network.default_ip_family".to_string());
    }

    if old.network.provision_kernel_interfaces != new.network.provision_kernel_interfaces {
        changes.push("network.provision_kernel_interfaces".to_string());
    }

    if old.network.bpf.attach != new.network.bpf.attach {
        changes.push("network.bpf.attach".to_string());
    }

    if old.network.bpf.artifact_dir != new.network.bpf.artifact_dir {
        changes.push("network.bpf.artifact_dir".to_string());
    }

    if old.network.bpf.overlay_flow_capacity != new.network.bpf.overlay_flow_capacity {
        changes.push("network.bpf.overlay_flow_capacity".to_string());
    }

    if old.network.wireguard.port != new.network.wireguard.port {
        changes.push("network.wireguard.port".to_string());
    }

    if old.health.probe_fanout != new.health.probe_fanout {
        changes.push("health.probe_fanout".to_string());
    }

    if old.health.probe_interval_ms != new.health.probe_interval_ms {
        changes.push("health.probe_interval_ms".to_string());
    }

    if old.health.probe_timeout_ms != new.health.probe_timeout_ms {
        changes.push("health.probe_timeout_ms".to_string());
    }

    if old.health.suspect_after_ms != new.health.suspect_after_ms {
        changes.push("health.suspect_after_ms".to_string());
    }

    if old.health.down_after_ms != new.health.down_after_ms {
        changes.push("health.down_after_ms".to_string());
    }

    if old.health.indirect_fanout_min != new.health.indirect_fanout_min {
        changes.push("health.indirect_fanout_min".to_string());
    }

    if old.health.indirect_fanout_max != new.health.indirect_fanout_max {
        changes.push("health.indirect_fanout_max".to_string());
    }

    changes
}

#[cfg(test)]
pub(crate) mod test_support {
    use parking_lot::{Mutex, MutexGuard};
    use std::sync::OnceLock;

    /// Returns the process-wide unit-test lock for environment and global-config mutations.
    pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::MutexGuard;
    use std::ffi::OsString;

    /// Scoped process environment overrides guarded by the unit-test serialization lock.
    struct EnvOverrideSet {
        previous: Vec<(&'static str, Option<OsString>)>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvOverrideSet {
        /// Applies temporary environment overrides while excluding concurrent env mutation tests.
        fn set(overrides: &[(&'static str, &'static str)]) -> Self {
            let lock = crate::config::test_support::env_lock();
            let previous = overrides
                .iter()
                .map(|(key, _)| (*key, std::env::var_os(key)))
                .collect();

            for (key, value) in overrides {
                unsafe {
                    std::env::set_var(key, value);
                }
            }

            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvOverrideSet {
        /// Restores every environment variable that was changed by this scoped override.
        fn drop(&mut self) {
            for (key, previous) in self.previous.iter().rev() {
                unsafe {
                    match previous {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }

    /// Builds one complete valid storage config from the built-in settings.
    fn replicated_volume_config() -> ReplicatedVolumeConfig {
        ReplicatedVolumeConfig::with_defaults(
            PathBuf::from("/var/lib/mantissa/replicas"),
            PathBuf::from("/var/lib/mantissa/replicated-volumes.redb"),
            PathBuf::from("/var/lib/mantissa/volume-mounts"),
            "0.0.0.0:7578"
                .parse()
                .expect("test listen address should parse"),
            "10.0.0.8:7578"
                .parse()
                .expect("test advertise address should parse"),
            PathBuf::from("/usr/sbin/wipefs"),
            PathBuf::from("/usr/sbin/mkfs.ext4"),
        )
    }

    #[test]
    fn defaults_validate() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn replicated_volume_defaults_are_valid_and_use_the_storage_port() {
        let storage = replicated_volume_config();
        assert!(storage.validate().is_ok());
        assert_eq!(storage.listen_address, "0.0.0.0:7578");
        assert_eq!(storage.advertise_address, "10.0.0.8:7578");
        assert_eq!(storage.heartbeat_interval_ms, 2_000);
        assert_eq!(storage.election_timeout_min_ms, 8_000);
        assert_eq!(storage.election_timeout_max_ms, 12_000);
        assert_eq!(storage.log_limits.snapshot_after_entries, 4_096);
        assert_eq!(storage.data_store_limits.worker_threads, 8);
        assert_eq!(storage.data_store_limits.max_queued_operations, 64);
    }

    #[test]
    fn replicated_volume_runtime_uses_the_checked_config() {
        let storage = replicated_volume_config();
        let checked = storage.checked().expect("check replicated-volume config");

        assert_eq!(checked.pool_path, PathBuf::from(&storage.pool_path));
        assert_eq!(
            checked.catalog_path,
            storage.catalog_path.as_deref().map(PathBuf::from)
        );
        assert_eq!(
            checked.operation_timeout,
            Duration::from_millis(storage.operation_timeout_ms)
        );
        assert_eq!(
            checked.runtime_limits.max_saved_groups(),
            storage.runtime_limits.max_saved_groups
        );
        assert_eq!(
            checked.protocol_limits.max_message_bytes(),
            storage.protocol_limits.max_message_bytes
        );
        assert_eq!(
            checked.max_state_bytes,
            storage.state_limits.max_state_bytes
        );
        assert_eq!(
            checked.driver_limits.max_pending_requests(),
            storage.driver_limits.max_pending_requests
        );
        assert_eq!(
            checked.repair_failure_grace,
            Duration::from_millis(storage.repair_limits.failure_grace_ms)
        );
        assert_eq!(
            checked.raft_config.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(storage.log_limits.snapshot_after_entries)
        );
        assert_eq!(
            checked.raft_config.max_in_snapshot_log_to_keep,
            storage.log_limits.snapshot_after_entries
        );
        assert_eq!(
            checked.raft_config.purge_batch_size,
            storage.log_limits.snapshot_after_entries
        );
    }

    #[test]
    fn replicated_volume_address_uses_the_daemon_ip() {
        assert_eq!(
            automatic_replicated_volume_ip(Some("10.20.30.40:6578"))
                .expect("explicit daemon IP should be accepted"),
            "10.20.30.40"
                .parse::<IpAddr>()
                .expect("test IP should parse")
        );
    }

    #[test]
    fn replicated_volume_config_checks_all_required_values() {
        let mut config = Config::default();
        config.storage.replicated_volumes = Some(replicated_volume_config());
        assert!(config.validate().is_ok());

        let storage = config
            .storage
            .replicated_volumes
            .as_mut()
            .expect("replicated storage config should exist");
        storage.advertise_address = "0.0.0.0:7578".to_string();
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.operation_timeout_ms = 0;
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.log_limits.snapshot_after_entries = 0;
        config.storage.replicated_volumes = Some(storage);
        let error = config
            .validate()
            .expect_err("zero snapshot entry threshold must be rejected");
        assert!(
            format!("{error:#}").contains("snapshot entry limit"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn replicated_volume_driver_limits_fit_the_data_path() {
        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.driver_limits.max_pending_requests = 0;
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.protocol_limits.max_message_bytes = storage.driver_limits.max_batch_bytes;
        storage.protocol_limits.max_entry_bytes = storage.driver_limits.max_batch_bytes / 2;
        config.storage.replicated_volumes = Some(storage);
        let error = config
            .validate()
            .expect_err("a data message needs room beyond its block payload");
        assert!(
            format!("{error:#}").contains("replica write requests need"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn replicated_volume_membership_limit_fits_joint_replacement() {
        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.protocol_limits.max_membership_nodes = 3;
        config.storage.replicated_volumes = Some(storage);

        let error = config
            .validate()
            .expect_err("three stable voters plus a replacement must fit");
        assert!(
            format!("{error:#}").contains("at least 4"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn replicated_volume_fixed_file_limits_are_checked() {
        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.data_store_limits.worker_threads = 0;
        config.storage.replicated_volumes = Some(storage);
        let error = config
            .validate()
            .expect_err("zero fixed-file workers must be rejected");
        assert!(
            format!("{error:#}").contains("file-worker thread count"),
            "unexpected validation error: {error:#}"
        );

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.data_store_limits.max_queued_operations = 0;
        config.storage.replicated_volumes = Some(storage);
        let error = config
            .validate()
            .expect_err("an empty fixed-file queue must be rejected");
        assert!(
            format!("{error:#}").contains("file-worker queue"),
            "unexpected validation error: {error:#}"
        );
    }

    #[test]
    fn replicated_volume_filesystem_requires_explicit_safe_choices() {
        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.filesystem.extended_options[1] = "lazy_itable_init=0".to_string();
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_ok());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage
            .filesystem
            .extended_options
            .retain(|option| option != "lazy_itable_init=1");
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage
            .filesystem
            .extended_options
            .push("lazy_itable_init=1".to_string());
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage
            .filesystem
            .extended_options
            .retain(|option| option != "nodiscard");
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage
            .filesystem
            .extended_options
            .push("discard".to_string());
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());

        let mut config = Config::default();
        let mut storage = replicated_volume_config();
        storage.filesystem.mount_options.push("ro".to_string());
        config.storage.replicated_volumes = Some(storage);
        assert!(config.validate().is_err());
    }

    #[test]
    fn storage_gc_runtime_config_converts_policy() {
        let mut config = Config::default();
        config.storage.gc.interval_ms = 5_000;
        config.storage.gc.tombstone_min_retention_ms = 10_000;
        config.storage.gc.tombstone_batch_limit = 17;
        config.storage.gc.mvreg_batch_limit = 9;
        config.storage.gc.mvreg_max_values = Some(3);
        config.storage.gc.stale_peer_rejoin_after_ms = 7_000;

        let runtime = config.storage.gc.as_runtime();
        assert!(runtime.enabled);
        assert_eq!(runtime.interval, Duration::from_millis(5_000));
        assert_eq!(
            runtime.stale_peer_rejoin_after,
            Duration::from_millis(7_000)
        );
        assert_eq!(runtime.policy.tombstone_min_retention_ms, 10_000);
        assert_eq!(runtime.policy.tombstone_batch_limit, 17);
        assert_eq!(runtime.policy.mvreg_batch_limit, 9);
        assert_eq!(runtime.policy.mvreg_max_values, Some(3));
    }

    #[test]
    fn rejects_invalid_storage_gc_config() {
        let mut config = Config::default();
        config.storage.gc.interval_ms = 0;
        assert!(config.validate().is_err());

        config = Config::default();
        config.storage.gc.tombstone_min_retention_ms = 0;
        assert!(config.validate().is_err());

        config = Config::default();
        config.storage.gc.tombstone_batch_limit = 0;
        assert!(config.validate().is_err());

        config = Config::default();
        config.storage.gc.mvreg_max_values = Some(0);
        assert!(config.validate().is_err());

        config = Config::default();
        config.storage.gc.mvreg_max_values = Some(2);
        config.storage.gc.mvreg_batch_limit = 0;
        assert!(config.validate().is_err());

        config = Config::default();
        config.storage.gc.stale_peer_rejoin_after_ms = config
            .storage
            .gc
            .tombstone_min_retention_ms
            .saturating_add(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_session_ticket_ttl() {
        let mut config = Config::default();
        config.security.session_ticket_ttl_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_metrics_config() {
        let mut config = Config::default();
        config.observability.metrics.enabled = true;
        config.observability.metrics.listen_addr = "127.0.0.1".to_string();
        assert!(config.validate().is_err());

        config = Config::default();
        config.observability.metrics.sample_interval_ms = 0;
        assert!(config.validate().is_err());

        config = Config::default();
        config.observability.metrics.state_db_sample_interval_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn disabled_metrics_ignore_invalid_runtime_listener() {
        let mut config = Config::default();
        config.observability.metrics.listen_addr = "not-a-socket".to_string();

        assert!(config.validate().is_ok());
        let runtime = config
            .observability
            .metrics
            .as_runtime()
            .expect("disabled metrics runtime config");

        assert!(!runtime.enabled);
        assert_eq!(runtime.listen_addr, default_metrics_socket_addr());
    }

    #[test]
    fn rejects_invalid_wireguard_port() {
        let mut config = Config::default();
        config.network.wireguard.port = Some(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_nodeport_ip() {
        let mut config = Config::default();
        config.network.nodeport.ip = Some("not-an-ip".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_empty_nodeport_iface() {
        let mut config = Config::default();
        config.network.nodeport.iface = Some("   ".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unsupported_nodeport_source_mode() {
        let mut config = Config::default();
        config.network.nodeport.source_mode = NodePortSourceMode::PreserveClient;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_bpf_map_capacity() {
        let mut config = Config::default();
        config.network.bpf.overlay_flow_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_nodeport_map_capacity() {
        let mut config = Config::default();
        config.network.nodeport.vip_capacity = 0;
        assert!(config.validate().is_err());

        config.network.nodeport.vip_capacity = DEFAULT_NODEPORT_VIP_CAPACITY;
        config.network.nodeport.host_capacity = 0;
        assert!(config.validate().is_err());

        config.network.nodeport.host_capacity = DEFAULT_NODEPORT_HOST_CAPACITY;
        config.network.nodeport.flow_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn rejects_oversized_bpf_map_capacity() {
        let mut config = Config::default();
        config.network.nodeport.flow_capacity = (u32::MAX as usize).saturating_add(1);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_advertise_addr() {
        let mut config = Config::default();
        config.network.advertise_addr = Some("10.0.0.10".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn default_ip_family_defaults_to_auto() {
        let config = Config::default();
        assert_eq!(
            config.network.default_ip_family,
            DefaultIpFamilyPolicy::Auto
        );
    }

    #[test]
    fn rejects_unspecified_socket_advertise_addr() {
        let mut config = Config::default();
        config.network.advertise_addr = Some("0.0.0.0:6578".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_nodeport_without_bpf() {
        let mut config = Config::default();
        config.network.nodeport.enabled = true;
        config.network.bpf.attach = false;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_health_probe_fanout() {
        let mut config = Config::default();
        config.health.probe_fanout = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_health_threshold_ordering() {
        let mut config = Config::default();
        config.health.suspect_after_ms = 2_000;
        config.health.down_after_ms = 2_000;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_health_indirect_fanout_ordering() {
        let mut config = Config::default();
        config.health.indirect_fanout_min = 8;
        config.health.indirect_fanout_max = 4;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_replication_capacity() {
        let mut config = Config::default();
        config.replication.gossip_channel_capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_replication_tick() {
        let mut config = Config::default();
        config.replication.sync_tick_ms = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_remote_admission_parallelism() {
        let mut config = Config::default();
        config.replication.remote_admission_parallelism = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_remote_assignment_parallelism() {
        let mut config = Config::default();
        config.replication.remote_assignment_parallelism = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_scheduler_slot_sizing() {
        let mut config = Config::default();
        config.scheduler.target_slot_cpu_millis = 0;
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.scheduler.target_slot_memory_bytes = 0;
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.scheduler.max_slots = 0;
        assert!(config.validate().is_err());

        let mut config = Config::default();
        config.scheduler.max_slots = SCHEDULER_MAX_SLOT_COUNT + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_service_shard_threshold() {
        let mut config = Config::default();
        config.replication.service_shard_target_threshold = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_service_shard_target_size() {
        let mut config = Config::default();
        config.replication.service_shard_target_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_service_shard_task_target_size() {
        let mut config = Config::default();
        config.replication.service_shard_task_target_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_service_shard_parallelism() {
        let mut config = Config::default();
        config.replication.service_shard_parallelism = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn env_overrides_apply_and_validate() {
        let _env = EnvOverrideSet::set(&[
            ("MANTISSA_WIREGUARD_DISABLE", "1"),
            ("MANTISSA_WIREGUARD_PORT", "51820"),
            ("MANTISSA_BPF_NO_ATTACH", "1"),
            ("MANTISSA_BPF_OVERLAY_FLOW_CAPACITY", "4096"),
            ("MANTISSA_NODEPORT_IFACE", "eth0"),
            ("MANTISSA_NODEPORT_SOURCE_MODE", "snat_host_access"),
            ("MANTISSA_NODEPORT_VIP_CAPACITY", "2048"),
            ("MANTISSA_NODEPORT_HOST_CAPACITY", "512"),
            ("MANTISSA_NODEPORT_FLOW_CAPACITY", "8192"),
            ("MANTISSA_ADVERTISE_ADDR", "node-1.example.com:6578"),
            ("MANTISSA_DEFAULT_IP_FAMILY", "ipv6"),
            ("MANTISSA_RUNTIME_OCI_HOST", "unix:///var/run/docker.sock"),
            ("MANTISSA_GPU_DEVICE_OVERRIDES", "uuid:GPU-abc=id:GPU-abc"),
            ("MANTISSA_LOCAL_VOLUME_ENFORCE_CAPACITY", "1"),
            ("MANTISSA_SESSION_TICKET_TTL_SECS", "7200"),
            ("MANTISSA_METRICS_ENABLE", "1"),
            ("MANTISSA_METRICS_LISTEN_ADDR", "127.0.0.1:19600"),
            ("MANTISSA_METRICS_SAMPLE_INTERVAL_MS", "15000"),
            ("MANTISSA_METRICS_STATE_DB_SAMPLE_INTERVAL_MS", "75000"),
            ("MANTISSA_SCHEDULER_RESERVED_CPU_MILLIS", "750"),
            ("MANTISSA_SCHEDULER_RESERVED_MEMORY_BYTES", "134217728"),
            ("MANTISSA_SCHEDULER_TARGET_SLOT_CPU_MILLIS", "125"),
            ("MANTISSA_SCHEDULER_TARGET_SLOT_MEMORY_BYTES", "67108864"),
            ("MANTISSA_SCHEDULER_MAX_SLOTS", "8192"),
            ("MANTISSA_GOSSIP_CHANNEL_CAPACITY", "256"),
            ("MANTISSA_GOSSIP_FANOUT", "7"),
            ("MANTISSA_GOSSIP_TICK_MS", "200"),
            ("MANTISSA_SYNC_TICK_MS", "300"),
            ("MANTISSA_SYNC_FANOUT", "9"),
            ("MANTISSA_GLOBAL_METADATA_SYNC_TICK_MS", "400"),
            ("MANTISSA_GLOBAL_METADATA_SYNC_FANOUT", "11"),
            ("MANTISSA_WORKLOAD_REPAIR_FANOUT", "3"),
            ("MANTISSA_REMOTE_ADMISSION_PARALLELISM", "12"),
            ("MANTISSA_REMOTE_ASSIGNMENT_PARALLELISM", "24"),
            ("MANTISSA_SERVICE_SHARD_TARGET_THRESHOLD", "64"),
            ("MANTISSA_SERVICE_SHARD_TARGET_SIZE", "16"),
            ("MANTISSA_SERVICE_SHARD_TASK_TARGET_SIZE", "32"),
            ("MANTISSA_SERVICE_SHARD_PARALLELISM", "8"),
        ]);

        let mut config = Config::default();
        let applied = config.apply_env_overrides();
        assert!(applied);
        assert!(!config.network.wireguard.enabled);
        assert_eq!(config.network.wireguard.port, Some(51820));
        assert!(!config.network.bpf.attach);
        assert!(!config.network.nodeport.enabled);
        assert_eq!(config.network.bpf.overlay_flow_capacity, 4096);
        assert_eq!(config.network.nodeport.iface.as_deref(), Some("eth0"));
        assert_eq!(
            config.network.nodeport.source_mode,
            NodePortSourceMode::SnatHostAccess
        );
        assert_eq!(config.network.nodeport.vip_capacity, 2048);
        assert_eq!(config.network.nodeport.host_capacity, 512);
        assert_eq!(config.network.nodeport.flow_capacity, 8192);
        assert_eq!(
            config.network.advertise_addr.as_deref(),
            Some("node-1.example.com:6578")
        );
        assert_eq!(
            config.network.default_ip_family,
            DefaultIpFamilyPolicy::Ipv6
        );
        assert_eq!(
            config.runtimes.oci.host.as_deref(),
            Some("unix:///var/run/docker.sock")
        );
        assert_eq!(
            config.gpu.device_overrides.as_deref(),
            Some("uuid:GPU-abc=id:GPU-abc")
        );
        assert!(config.storage.local_volume_enforce_capacity);
        assert_eq!(config.security.session_ticket_ttl_secs, 7200);
        assert!(config.observability.metrics.enabled);
        assert_eq!(
            config.observability.metrics.listen_addr.as_str(),
            "127.0.0.1:19600"
        );
        assert_eq!(config.observability.metrics.sample_interval_ms, 15_000);
        assert_eq!(
            config.observability.metrics.state_db_sample_interval_ms,
            75_000
        );
        assert_eq!(config.scheduler.reserved_cpu_millis, 750);
        assert_eq!(config.scheduler.reserved_memory_bytes, 134_217_728);
        assert_eq!(config.scheduler.target_slot_cpu_millis, 125);
        assert_eq!(config.scheduler.target_slot_memory_bytes, 67_108_864);
        assert_eq!(config.scheduler.max_slots, 8_192);
        assert_eq!(config.replication.gossip_channel_capacity, 256);
        assert_eq!(config.replication.gossip_fanout, 7);
        assert_eq!(config.replication.gossip_tick_ms, 200);
        assert_eq!(config.replication.sync_tick_ms, 300);
        assert_eq!(config.replication.sync_fanout, 9);
        assert_eq!(config.replication.global_metadata_sync_tick_ms, 400);
        assert_eq!(config.replication.global_metadata_sync_fanout, 11);
        assert_eq!(config.replication.workload_repair_fanout, 3);
        assert_eq!(config.replication.remote_admission_parallelism, 12);
        assert_eq!(config.replication.remote_assignment_parallelism, 24);
        assert_eq!(config.replication.service_shard_target_threshold, 64);
        assert_eq!(config.replication.service_shard_target_size, 16);
        assert_eq!(config.replication.service_shard_task_target_size, 32);
        assert_eq!(config.replication.service_shard_parallelism, 8);
    }

    #[test]
    fn scheduler_reserve_changes_require_restart() {
        let old = Config::default();
        let mut new = Config::default();
        new.scheduler.reserved_cpu_millis = old.scheduler.reserved_cpu_millis.saturating_add(250);
        new.scheduler.reserved_memory_bytes = old
            .scheduler
            .reserved_memory_bytes
            .saturating_add(128 * 1024 * 1024);
        new.scheduler.target_slot_cpu_millis =
            old.scheduler.target_slot_cpu_millis.saturating_add(25);
        new.scheduler.target_slot_memory_bytes = old
            .scheduler
            .target_slot_memory_bytes
            .saturating_add(64 * 1024 * 1024);
        new.scheduler.max_slots = old.scheduler.max_slots.saturating_sub(1);

        let changes = restart_required_changes(&old, &new);
        assert!(
            changes
                .iter()
                .any(|change| change == "scheduler.reserved_cpu_millis")
        );
        assert!(
            changes
                .iter()
                .any(|change| change == "scheduler.reserved_memory_bytes")
        );
        assert!(
            changes
                .iter()
                .any(|change| change == "scheduler.target_slot_cpu_millis")
        );
        assert!(
            changes
                .iter()
                .any(|change| change == "scheduler.target_slot_memory_bytes")
        );
        assert!(changes.iter().any(|change| change == "scheduler.max_slots"));
    }

    #[test]
    fn advertise_addr_change_requires_restart() {
        let old = Config::default();
        let mut new = Config::default();
        new.network.advertise_addr = Some("node-1.example.com:6578".to_string());

        let changes = restart_required_changes(&old, &new);
        assert!(
            changes
                .iter()
                .any(|change| change == "network.advertise_addr")
        );
    }

    #[test]
    fn default_ip_family_change_requires_restart() {
        let old = Config::default();
        let mut new = Config::default();
        new.network.default_ip_family = DefaultIpFamilyPolicy::Ipv6;

        let changes = restart_required_changes(&old, &new);
        assert!(
            changes
                .iter()
                .any(|change| change == "network.default_ip_family")
        );
    }

    #[test]
    fn bpf_and_nodeport_capacity_changes_require_restart() {
        let old = Config::default();
        let mut new = Config::default();
        new.network.bpf.overlay_flow_capacity =
            old.network.bpf.overlay_flow_capacity.saturating_add(1024);
        new.network.nodeport.vip_capacity = old.network.nodeport.vip_capacity.saturating_add(128);
        new.network.nodeport.host_capacity = old.network.nodeport.host_capacity.saturating_add(64);
        new.network.nodeport.flow_capacity =
            old.network.nodeport.flow_capacity.saturating_add(1024);

        let changes = restart_required_changes(&old, &new);
        assert!(
            changes
                .iter()
                .any(|change| change == "network.bpf.overlay_flow_capacity")
        );
        assert!(
            changes
                .iter()
                .any(|change| change == "network.nodeport.vip_capacity")
        );
        assert!(
            changes
                .iter()
                .any(|change| change == "network.nodeport.host_capacity")
        );
        assert!(
            changes
                .iter()
                .any(|change| change == "network.nodeport.flow_capacity")
        );
    }

    #[test]
    fn nodeport_source_mode_change_requires_restart() {
        let old = Config::default();
        let mut new = Config::default();
        new.network.nodeport.source_mode = NodePortSourceMode::PreserveClient;

        let changes = restart_required_changes(&old, &new);
        assert!(
            changes
                .iter()
                .any(|change| change == "network.nodeport.source_mode")
        );
    }
}
