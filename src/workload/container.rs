use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap; // Use BTreeMap instead of HashMap
use std::hash::Hash;

/// Represents the type of runtime for a workload
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RuntimeType {
    Docker,
    Containerd,
    FirecrackerVM,
    GVisor,
    Kata,
    Custom(String),
}

/// Represents the current state of a container or MicroVM
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContainerState {
    Pending,
    Creating,
    Running,
    Stopping,
    Stopped,
    Failed,
    Exited(i32), // Exit code
    Unknown,
}

/// Isolation level for the workload
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IsolationType {
    Container, // Standard container isolation
    VM,        // Full VM isolation
    Hybrid,    // Hybrid approaches like Kata
}

/// Resource limits and requests
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Resources {
    pub cpu_cores: f32,              // Number of CPU cores (can be fractional)
    pub cpu_shares: Option<u32>,     // CPU shares (relative weight)
    pub memory_mb: u32,              // Memory in MB
    pub memory_swap_mb: Option<u32>, // Swap memory in MB
    pub disk_quota_mb: Option<u32>,  // Disk quota in MB
    pub iops_limits: Option<u32>,    // IO operations per second limits
    pub cpuset: Option<String>,      // CPU set constraints (e.g., "0-3,5")
}

// Implement Hash manually for Resources since f32 doesn't implement Hash
impl Hash for Resources {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Convert f32 to bits for hashing
        let cpu_bits = self.cpu_cores.to_bits();
        cpu_bits.hash(state);
        self.cpu_shares.hash(state);
        self.memory_mb.hash(state);
        self.memory_swap_mb.hash(state);
        self.disk_quota_mb.hash(state);
        self.iops_limits.hash(state);
        self.cpuset.hash(state);
    }
}

impl Eq for Resources {}

/// Network configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct NetworkConfig {
    pub mode: String,                 // bridge, host, none, etc.
    pub ip_address: Option<String>,   // Static IP, if assigned
    pub ports: Vec<PortMapping>,      // Port mappings
    pub dns: Vec<String>,             // DNS servers
    pub dns_search: Vec<String>,      // DNS search domains
    pub hostname: Option<String>,     // Custom hostname
    pub mac_address: Option<String>,  // MAC address if custom
    pub network_aliases: Vec<String>, // Network aliases
    pub networks: Vec<String>,        // Names of networks to join
}

/// Port mapping configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PortMapping {
    pub container_port: u16,
    pub host_port: Option<u16>,
    pub protocol: String,        // tcp, udp
    pub host_ip: Option<String>, // Binding address
}

/// Volume mount configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct VolumeMount {
    pub source: String,             // Host path or volume name
    pub target: String,             // Mount path in container
    pub readonly: bool,             // Read-only mount
    pub mount_type: String,         // bind, volume, tmpfs
    pub mount_options: Vec<String>, // Mount options
}

/// Health check configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HealthCheck {
    pub command: Vec<String>,      // Command to run
    pub interval_seconds: u32,     // Interval between checks
    pub timeout_seconds: u32,      // Timeout for each check
    pub retries: u32,              // Number of retries
    pub start_period_seconds: u32, // Grace period
}

// Implement Hash for HealthCheck
impl Hash for HealthCheck {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.command.hash(state);
        self.interval_seconds.hash(state);
        self.timeout_seconds.hash(state);
        self.retries.hash(state);
        self.start_period_seconds.hash(state);
    }
}

impl Eq for HealthCheck {}

/// Logging configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    pub driver: String,                    // json-file, syslog, etc.
    pub max_size: String,                  // Max size of logs (e.g., "10m")
    pub max_files: Option<u32>,            // Max number of log files
    pub options: BTreeMap<String, String>, // Driver-specific options
}

// Implement Hash for LogConfig
impl Hash for LogConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.driver.hash(state);
        self.max_size.hash(state);
        self.max_files.hash(state);

        // Sort keys for deterministic hashing
        let mut sorted_keys: Vec<&String> = self.options.keys().collect();
        sorted_keys.sort();

        for key in sorted_keys {
            key.hash(state);
            self.options.get(key).unwrap().hash(state);
        }
    }
}

/// Restart policy
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RestartPolicy {
    No,
    Always,
    OnFailure(u32), // Max retry count
    UnlessStopped,
}

/// Security options
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecurityOptions {
    pub privileged: bool,                 // Run in privileged mode
    pub cap_add: Vec<String>,             // Added capabilities
    pub cap_drop: Vec<String>,            // Dropped capabilities
    pub read_only_root_fs: bool,          // Read-only root filesystem
    pub security_opt: Vec<String>,        // Security options
    pub apparmor_profile: Option<String>, // AppArmor profile
    pub seccomp_profile: Option<String>,  // Seccomp profile
    pub no_new_privileges: bool,          // Disable new privileges
}

// Implement Hash for SecurityOptions
impl Hash for SecurityOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.privileged.hash(state);
        self.cap_add.hash(state);
        self.cap_drop.hash(state);
        self.read_only_root_fs.hash(state);
        self.security_opt.hash(state);
        self.apparmor_profile.hash(state);
        self.seccomp_profile.hash(state);
        self.no_new_privileges.hash(state);
    }
}

/// MicroVM specific configuration (for Firecracker etc.)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MicroVMConfig {
    pub kernel_path: Option<String>,         // Path to kernel
    pub kernel_args: Option<String>,         // Kernel boot arguments
    pub rootfs_path: String,                 // Path to rootfs
    pub vm_memory_mb: u32,                   // VM memory in MB
    pub vcpu_count: u32,                     // Number of vCPUs
    pub boot_source_id: Option<String>,      // Boot source ID
    pub jailer_config: Option<JailerConfig>, // Firecracker jailer config
}

// Implement Hash for MicroVMConfig
impl Hash for MicroVMConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kernel_path.hash(state);
        self.kernel_args.hash(state);
        self.rootfs_path.hash(state);
        self.vm_memory_mb.hash(state);
        self.vcpu_count.hash(state);
        self.boot_source_id.hash(state);
        self.jailer_config.hash(state);
    }
}

impl Eq for MicroVMConfig {}

/// Jailer configuration for Firecracker
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JailerConfig {
    pub gid: u32,                // GID to drop privileges to
    pub uid: u32,                // UID to drop privileges to
    pub id: String,              // Jailer ID
    pub numa_node: Option<u32>,  // NUMA node
    pub chroot_base_dir: String, // Chroot base directory
}

/// The main Container struct representing any workload (container or VM)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Container {
    // Core identity
    pub id: String,                            // Unique container/VM ID
    pub name: String,                          // Human-readable name
    pub namespace: String,                     // Organizational namespace
    pub labels: BTreeMap<String, String>,      // Metadata labels
    pub annotations: BTreeMap<String, String>, // Annotations

    // Runtime configuration
    pub runtime_type: RuntimeType,     // What runtime to use
    pub isolation_type: IsolationType, // Container vs VM isolation
    pub image: String,                 // Container image or VM image
    pub image_pull_policy: String,     // Always, IfNotPresent, Never

    // Command and Environment
    pub command: Vec<String>,          // Command to run
    pub args: Vec<String>,             // Command arguments
    pub working_dir: Option<String>,   // Working directory
    pub env: BTreeMap<String, String>, // Environment variables
    pub env_from: Vec<String>,         // Environment from sources

    // Resource management
    pub resources: Resources, // Resource limits and requests

    // Network configuration
    pub network: NetworkConfig, // Network settings

    // Storage
    pub volumes: Vec<VolumeMount>, // Volume mounts

    // State
    pub state: ContainerState,              // Current state
    pub exit_code: Option<i32>,             // Exit code if terminated
    pub created_at: Option<DateTime<Utc>>,  // Creation timestamp
    pub started_at: Option<DateTime<Utc>>,  // Start timestamp
    pub finished_at: Option<DateTime<Utc>>, // Finish timestamp

    // Health and monitoring
    pub health_check: Option<HealthCheck>, // Health check config
    pub log_config: LogConfig,             // Logging configuration

    // Reliability
    pub restart_policy: RestartPolicy, // Restart policy

    // Security
    pub security: SecurityOptions, // Security options

    // Specialized configs
    pub microvm_config: Option<MicroVMConfig>, // MicroVM specific config

    // Orchestration metadata
    pub node_id: Option<String>, // Current node running this container
    pub owner_id: String,        // Owner node ID
    pub priority: u32,           // Scheduling priority
    pub version: u64,            // Version counter

    // Distribution and CRDT tracking
    pub last_updated_by: Option<String>, // Last node that updated this
    pub last_updated_at: DateTime<Utc>,  // Last update timestamp
}

// Implement Hash for Container - only use ID to determine identity
impl Hash for Container {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // For CRDT purposes, we only need to hash the ID
        // This makes containers with the same ID equal for Orswot operations
        self.id.hash(state);
    }
}

impl Eq for Container {}

impl PartialEq<str> for Container {
    fn eq(&self, other: &str) -> bool {
        self.id == other
    }
}

/// Node implementation with Orswot
use crdts::Orswot;

/// Represents a node in the distributed system
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub hostname: String,
    pub ip_address: String,
    pub labels: BTreeMap<String, String>,
    pub resources: NodeResources,
    pub status: NodeStatus,

    // Track containers using Orswot CRDT
    #[serde(skip)] // Skip serialization of the CRDT directly
    pub containers: Orswot<Container, String>, // Actor is node_id
}

/// Node resources
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NodeResources {
    pub cpu_cores: u32,
    pub memory_mb: u64,
    pub disk_space_mb: u64,
    pub available_cpu: f32,
    pub available_memory_mb: u64,
    pub available_disk_mb: u64,
}

// Manual implementation of Hash for NodeResources to handle f32
impl Hash for NodeResources {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cpu_cores.hash(state);
        self.memory_mb.hash(state);
        self.disk_space_mb.hash(state);

        // Convert f32 to bits for hashing
        let available_cpu_bits = self.available_cpu.to_bits();
        available_cpu_bits.hash(state);

        self.available_memory_mb.hash(state);
        self.available_disk_mb.hash(state);
    }
}

/// Node status
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeStatus {
    Ready,
    NotReady,
    Maintenance,
    Draining,
    Offline,
}

impl Node {
    /// Create a new node
    pub fn new(id: String, hostname: String, ip_address: String) -> Self {
        Node {
            id: id.clone(),
            hostname,
            ip_address,
            labels: BTreeMap::new(),
            resources: NodeResources {
                cpu_cores: 0,
                memory_mb: 0,
                disk_space_mb: 0,
                available_cpu: 0.0,
                available_memory_mb: 0,
                available_disk_mb: 0,
            },
            status: NodeStatus::NotReady,
            containers: Orswot::new(),
        }
    }

    /// Add a container to this node
    pub fn add_container(&mut self, container: Container) {
        // TODO
    }

    /// Remove a container from this node
    pub fn remove_container(&mut self, container_id: &str) {
        // TODO
    }

    /// Merge container state from another node
    pub fn merge_container_state(&mut self, other_containers: &Orswot<Container, String>) {
        // TODO
    }

    /// Get all containers
    pub fn get_containers(&self) -> Vec<Container> {
        Vec::new()
    }

    /// Get container by ID
    pub fn get_container(&self, container_id: &str) -> Option<Container> {
        None
    }
}
