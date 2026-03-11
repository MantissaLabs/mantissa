use std::fs;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::{
    RwLock,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use net::paths::ensure_state_dir;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// # Description:
///
/// Root configuration container loaded from the Mantissa RON config file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub docker: DockerConfig,
    #[serde(default)]
    pub gpu: GpuConfig,
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
}

/// # Description:
///
/// Network subsystem configuration for WireGuard, BPF, and nodeport.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NetworkConfig {
    #[serde(default)]
    pub wireguard: WireguardConfig,
    #[serde(default)]
    pub bpf: BpfConfig,
    #[serde(default)]
    pub nodeport: NodePortConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
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
}

impl Default for BpfConfig {
    /// # Description:
    ///
    /// Returns the default BPF configuration used when no config is supplied.
    fn default() -> Self {
        Self {
            attach: true,
            artifact_dir: None,
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
        }
    }
}

/// # Description:
///
/// Service discovery and health probing configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DiscoveryConfig {
    #[serde(default)]
    pub health_port: Option<u16>,
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
/// Docker runtime configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DockerConfig {
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

static GLOBAL_CONFIG: Lazy<RwLock<Config>> = Lazy::new(|| RwLock::new(Config::default()));
static GLOBAL_SOURCE: Lazy<RwLock<ConfigSource>> =
    Lazy::new(|| RwLock::new(ConfigSource::default()));
static GLOBAL_LOADED: AtomicBool = AtomicBool::new(false);

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
    let mut guard = GLOBAL_CONFIG.write().expect("global config lock poisoned");
    *guard = config;

    let mut source_guard = GLOBAL_SOURCE
        .write()
        .expect("global config source lock poisoned");
    *source_guard = source;
    GLOBAL_LOADED.store(true, Ordering::Release);
}

/// # Description:
///
/// Return a cloned snapshot of the current global configuration.
pub fn global_config() -> Config {
    ensure_config_loaded();
    let guard = GLOBAL_CONFIG.read().expect("global config lock poisoned");
    guard.clone()
}

/// # Description:
///
/// Return a snapshot of the metadata describing where the current config came from.
pub fn global_config_source() -> ConfigSource {
    ensure_config_loaded();
    let guard = GLOBAL_SOURCE
        .read()
        .expect("global config source lock poisoned");
    guard.clone()
}

/// # Description:
///
/// Resolve a configured nodeport IP address, if any, into an IPv4 value.
pub fn nodeport_ip() -> Option<Ipv4Addr> {
    let config = global_config();
    let raw = config.network.nodeport.ip?;
    raw.parse::<Ipv4Addr>().ok()
}

/// # Description:
///
/// Resolve the configured nodeport interface name, if provided.
pub fn nodeport_iface() -> Option<String> {
    global_config().network.nodeport.iface
}

/// # Description:
///
/// Resolve the configured nodeport enabled flag.
pub fn nodeport_enabled() -> bool {
    global_config().network.nodeport.enabled
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
/// Resolve the configured service discovery health port, if provided.
pub fn discovery_health_port() -> Option<u16> {
    global_config().network.discovery.health_port
}

/// # Description:
///
/// Resolve the peer-health runtime configuration used by liveness probing loops.
pub fn health_runtime_config() -> RuntimeHealthConfig {
    global_config().health.as_runtime()
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
/// Resolve the Docker host override, if configured.
pub fn docker_host() -> Option<String> {
    global_config().docker.host
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
    fs::create_dir_all(&root)
        .with_context(|| format!("failed to create local volume root {}", root.display()))?;
    Ok(root)
}

/// # Description:
///
/// Return whether requested local-volume capacity should be enforced as a runtime cutoff.
pub fn local_volume_enforce_capacity() -> bool {
    global_config().storage.local_volume_enforce_capacity
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
    Some(start_config_watch_thread(path))
}

/// # Description:
///
/// Return a default true value for serde defaults.
fn default_true() -> bool {
    true
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
    let mut guard = GLOBAL_CONFIG.write().expect("global config lock poisoned");
    *guard = config;

    let mut source_guard = GLOBAL_SOURCE
        .write()
        .expect("global config source lock poisoned");
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

        if let Ok(raw) = std::env::var("MANTISSA_LB_HEALTH_PORT") {
            applied = true;
            match raw.trim().parse::<u16>() {
                Ok(port) if port > 0 => self.network.discovery.health_port = Some(port),
                _ => warn!(
                    target: "config",
                    "ignoring invalid MANTISSA_LB_HEALTH_PORT '{raw}'"
                ),
            }
        }

        if let Ok(host) = std::env::var("MANTISSA_DOCKER_HOST") {
            applied = true;
            let host = host.trim();
            if !host.is_empty() {
                self.docker.host = Some(host.to_string());
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

        if let Some(port) = self.network.discovery.health_port
            && port == 0
        {
            anyhow::bail!("network.discovery.health_port must be non-zero");
        }

        if let Some(ref ip) = self.network.nodeport.ip
            && ip.parse::<Ipv4Addr>().is_err()
        {
            anyhow::bail!("network.nodeport.ip must be a valid IPv4 address (got '{ip}')");
        }

        if let Some(ref host) = self.docker.host
            && host.trim().is_empty()
        {
            anyhow::bail!("docker.host cannot be empty");
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

        Ok(())
    }
}

/// # Description:
///
/// Start a watcher thread for the provided config path and reload on changes.
fn start_config_watch_thread(path: PathBuf) -> std::thread::JoinHandle<()> {
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
        .expect("failed to spawn config watcher thread")
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

    if old.docker.host != new.docker.host {
        changes.push("docker.host".to_string());
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

    if old.network.nodeport.enabled != new.network.nodeport.enabled {
        changes.push("network.nodeport.enabled".to_string());
    }

    if old.network.nodeport.iface != new.network.nodeport.iface {
        changes.push("network.nodeport.iface".to_string());
    }

    if old.network.nodeport.ip != new.network.nodeport.ip {
        changes.push("network.nodeport.ip".to_string());
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
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_invalid_wireguard_port() {
        let mut config = Config::default();
        config.network.wireguard.port = Some(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_health_port() {
        let mut config = Config::default();
        config.network.discovery.health_port = Some(0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_invalid_nodeport_ip() {
        let mut config = Config::default();
        config.network.nodeport.ip = Some("not-an-ip".to_string());
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
    fn env_overrides_apply_and_validate() {
        unsafe {
            std::env::set_var("MANTISSA_WIREGUARD_DISABLE", "1");
            std::env::set_var("MANTISSA_WIREGUARD_PORT", "51820");
            std::env::set_var("MANTISSA_BPF_NO_ATTACH", "1");
            std::env::set_var("MANTISSA_NODEPORT_IFACE", "eth0");
            std::env::set_var("MANTISSA_LB_HEALTH_PORT", "30080");
            std::env::set_var("MANTISSA_DOCKER_HOST", "unix:///var/run/docker.sock");
            std::env::set_var("MANTISSA_GPU_DEVICE_OVERRIDES", "uuid:GPU-abc=id:GPU-abc");
            std::env::set_var("MANTISSA_LOCAL_VOLUME_ENFORCE_CAPACITY", "1");
        }

        let mut config = Config::default();
        let applied = config.apply_env_overrides();
        assert!(applied);
        assert!(!config.network.wireguard.enabled);
        assert_eq!(config.network.wireguard.port, Some(51820));
        assert!(!config.network.bpf.attach);
        assert!(!config.network.nodeport.enabled);
        assert_eq!(config.network.nodeport.iface.as_deref(), Some("eth0"));
        assert_eq!(config.network.discovery.health_port, Some(30080));
        assert_eq!(
            config.docker.host.as_deref(),
            Some("unix:///var/run/docker.sock")
        );
        assert_eq!(
            config.gpu.device_overrides.as_deref(),
            Some("uuid:GPU-abc=id:GPU-abc")
        );
        assert!(config.storage.local_volume_enforce_capacity);

        unsafe {
            std::env::remove_var("MANTISSA_WIREGUARD_DISABLE");
            std::env::remove_var("MANTISSA_WIREGUARD_PORT");
            std::env::remove_var("MANTISSA_BPF_NO_ATTACH");
            std::env::remove_var("MANTISSA_NODEPORT_IFACE");
            std::env::remove_var("MANTISSA_LB_HEALTH_PORT");
            std::env::remove_var("MANTISSA_DOCKER_HOST");
            std::env::remove_var("MANTISSA_GPU_DEVICE_OVERRIDES");
            std::env::remove_var("MANTISSA_LOCAL_VOLUME_ENFORCE_CAPACITY");
        }
    }
}
