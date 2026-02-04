use std::fs;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// # Description:
///
/// Root configuration container loaded from the Mantissa RON config file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub docker: DockerConfig,
    #[serde(default)]
    pub gpu: GpuConfig,
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
    let mut guard = GLOBAL_CONFIG
        .write()
        .expect("global config lock poisoned");
    *guard = config;

    let mut source_guard = GLOBAL_SOURCE
        .write()
        .expect("global config source lock poisoned");
    *source_guard = source;
}

/// # Description:
///
/// Return a cloned snapshot of the current global configuration.
pub fn global_config() -> Config {
    let guard = GLOBAL_CONFIG
        .read()
        .expect("global config lock poisoned");
    guard.clone()
}

/// # Description:
///
/// Return a snapshot of the metadata describing where the current config came from.
pub fn global_config_source() -> ConfigSource {
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
    global_config()
        .network
        .bpf
        .artifact_dir
        .map(PathBuf::from)
}

/// # Description:
///
/// Resolve the configured service discovery health port, if provided.
pub fn discovery_health_port() -> Option<u16> {
    global_config().network.discovery.health_port
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
/// Render a config snapshot as pretty-printed RON for diagnostics.
pub fn render_config_ron(config: &Config) -> Result<String> {
    let pretty = ron::ser::PrettyConfig::default();
    ron::ser::to_string_pretty(config, pretty)
        .context("failed to serialize config to RON")
}

/// # Description:
///
/// Return a default true value for serde defaults.
fn default_true() -> bool {
    true
}

/// # Description:
///
/// Load and parse a RON config file from the provided path.
fn load_config_from_path(path: &Path) -> Result<Config> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    let config: Config =
        ron::from_str(&raw).with_context(|| format!("failed to parse config {}", path.display()))?;
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

        applied
    }

    /// # Description:
    ///
    /// Validate configuration values so runtime components receive sane inputs.
    pub fn validate(&self) -> Result<()> {
        if let Some(port) = self.network.wireguard.port {
            if port == 0 {
                anyhow::bail!("network.wireguard.port must be non-zero");
            }
        }

        if let Some(port) = self.network.discovery.health_port {
            if port == 0 {
                anyhow::bail!("network.discovery.health_port must be non-zero");
            }
        }

        if let Some(ref ip) = self.network.nodeport.ip {
            if ip.parse::<Ipv4Addr>().is_err() {
                anyhow::bail!(
                    "network.nodeport.ip must be a valid IPv4 address (got '{ip}')"
                );
            }
        }

        if let Some(ref host) = self.docker.host {
            if host.trim().is_empty() {
                anyhow::bail!("docker.host cannot be empty");
            }
        }

        if let Some(ref overrides) = self.gpu.device_overrides {
            if overrides.trim().is_empty() {
                anyhow::bail!("gpu.device_overrides cannot be empty");
            }
        }

        if self.network.nodeport.enabled && !self.network.bpf.attach {
            anyhow::bail!(
                "network.nodeport.enabled requires network.bpf.attach to be true"
            );
        }

        Ok(())
    }
}
