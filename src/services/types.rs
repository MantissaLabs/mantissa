use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::task::types::{TaskEnvironmentVariable, TaskSecretFile};

/// Value stored in the replicated service store describing desired service state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceSpecValue {
    pub id: Uuid,
    pub manifest_id: Uuid,
    pub manifest_name: String,
    pub service_name: String,
    pub tasks: Vec<ServiceTaskSpecValue>,
    pub task_ids: Vec<Uuid>,
    pub updated_at: String,
    #[serde(default)]
    pub status: ServiceStatus,
}

impl ServiceSpecValue {
    pub fn new(
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
        task_ids: Vec<Uuid>,
    ) -> Self {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        let id = compute_service_id(&service_name);

        Self {
            id,
            manifest_id,
            manifest_name,
            service_name,
            tasks,
            task_ids,
            updated_at: current_timestamp(),
            status: ServiceStatus::Running,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    pub fn status(&self) -> ServiceStatus {
        self.status
    }

    pub fn set_status(&mut self, status: ServiceStatus) {
        self.status = status;
        self.touch();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceTaskSpecValue {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub replicas: u16,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    #[serde(default)]
    pub restart_policy: Option<ServiceTaskRestartPolicy>,
    #[serde(default)]
    pub env: Vec<TaskEnvironmentVariable>,
    #[serde(default)]
    pub secret_files: Vec<TaskSecretFile>,
    #[serde(default)]
    pub networks: Vec<ServiceTaskNetworkRequirement>,
    #[serde(default)]
    pub health_port: Option<u16>,
    #[serde(default)]
    pub health_command: Option<Vec<String>>,
    #[serde(default)]
    pub public_port: Option<u16>,
    #[serde(default)]
    pub public_protocol: Option<ServicePortProtocol>,
}

/// Supported transport protocols for publicly exposed service ports.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ServicePortProtocol {
    Tcp,
    Udp,
    TcpUdp,
}

impl Default for ServicePortProtocol {
    fn default() -> Self {
        ServicePortProtocol::Tcp
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceTaskNetworkRequirement {
    pub name: String,
    pub network_id: Uuid,
}

impl ServiceTaskNetworkRequirement {
    pub fn new(name: impl Into<String>, network_id: Uuid) -> Self {
        Self {
            name: name.into(),
            network_id,
        }
    }
}

impl ServiceTaskSpecValue {
    pub fn required_network_ids(&self) -> Vec<Uuid> {
        self.networks
            .iter()
            .map(|network| network.network_id)
            .collect()
    }

    pub fn health_port(&self) -> Option<u16> {
        self.health_port
    }

    pub fn health_command(&self) -> Option<&[String]> {
        self.health_command.as_deref()
    }

    /// Return the port that should be reachable from the host via the network VIP, if one was
    /// declared in the service manifest.
    pub fn public_port(&self) -> Option<u16> {
        self.public_port
    }

    /// Return the public protocols to expose for the declared nodeport.
    ///
    /// The default remains TCP-only to match historical behavior unless the manifest opts in
    /// to UDP or both protocols.
    pub fn public_protocols(&self) -> Vec<ServicePortProtocol> {
        match self.public_protocol.unwrap_or_default() {
            ServicePortProtocol::Tcp => vec![ServicePortProtocol::Tcp],
            ServicePortProtocol::Udp => vec![ServicePortProtocol::Udp],
            ServicePortProtocol::TcpUdp => vec![ServicePortProtocol::Tcp, ServicePortProtocol::Udp],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceTaskRestartPolicy {
    pub name: ServiceTaskRestartPolicyKind,
    #[serde(default)]
    pub max_retry_count: Option<i32>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ServiceTaskRestartPolicyKind {
    No,
    Always,
    OnFailure,
    UnlessStopped,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceEvent {
    Upsert(ServiceSpecValue),
    Remove(ServiceSpecValue),
}

#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceStatus {
    Deploying,
    #[default]
    Running,
    Stopping,
    Stopped,
    Failed,
}

fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}

pub fn compute_service_id(service_name: &str) -> Uuid {
    let digest = blake3::hash(service_name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::compute_service_id;

    #[test]
    fn service_id_deterministic() {
        let first = compute_service_id("alpha-web");
        let second = compute_service_id("alpha-web");
        assert_eq!(first, second);

        let other = compute_service_id("beta-web");
        assert_ne!(first, other);
    }
}
