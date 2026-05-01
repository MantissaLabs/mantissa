use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::time::Duration;
use uuid::Uuid;

use crate::scheduler::placement::PlacementPolicy;
use crate::workload::manager::WorkloadStartRequest;
use crate::workload::model::{
    ExecutionPlatform, IsolationMode, WorkloadOwner, WorkloadServiceMetadata,
};
use crate::workload::types::{ExecutionSpec, ResolvedExecutionSpec};
pub use crate::workload::types::{
    WorkloadLivenessProbe as ServiceLivenessProbe,
    WorkloadLivenessProbeKind as ServiceLivenessProbeKind,
    WorkloadRestartPolicy as TaskTemplateRestartPolicy,
    WorkloadRestartPolicyKind as TaskTemplateRestartPolicyKind,
};

/// Prefix used when the service lifecycle detail is specifically describing public endpoint state.
pub const SERVICE_PUBLIC_ENDPOINT_DETAIL_PREFIX: &str = "public endpoint: ";

/// Value stored in the replicated service store describing desired service state.
///
/// A service is a controller-level object that owns rollout, readiness, and desired replica
/// count semantics. The individual schedulable executions it creates are service-owned workload
/// replicas, not standalone tasks.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceSpecValue {
    pub id: Uuid,
    pub manifest_id: Uuid,
    pub manifest_name: String,
    pub service_name: String,
    pub task_templates: Vec<TaskTemplateSpecValue>,
    pub replica_ids: Vec<Uuid>,
    pub updated_at: String,
    #[serde(default)]
    pub update_strategy: ServiceUpdateStrategy,
    #[serde(default)]
    pub service_epoch: u64,
    #[serde(default)]
    pub phase_version: u64,
    #[serde(default)]
    pub rollout: ServiceRolloutState,
    #[serde(default)]
    pub status: ServiceStatus,
    #[serde(default)]
    pub status_detail: Option<String>,
    #[serde(default)]
    pub previous_generation: Option<ServicePreviousGeneration>,
    #[serde(default)]
    pub reschedule_lock: Option<ServiceRescheduleLock>,
}

impl ServiceSpecValue {
    /// Builds one replicated service spec value with default lifecycle metadata.
    pub fn new(
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        task_templates: Vec<TaskTemplateSpecValue>,
        replica_ids: Vec<Uuid>,
    ) -> Self {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        let id = compute_service_id(&service_name);

        Self {
            id,
            manifest_id,
            manifest_name,
            service_name,
            task_templates,
            replica_ids,
            updated_at: current_timestamp(),
            update_strategy: ServiceUpdateStrategy::default(),
            service_epoch: 0,
            phase_version: 0,
            rollout: ServiceRolloutState::default(),
            status: ServiceStatus::Running,
            status_detail: None,
            previous_generation: None,
            reschedule_lock: None,
        }
    }

    /// Refreshes the logical update timestamp after one in-memory mutation.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Starts one new deployment generation and resets per-generation phase ordering.
    pub fn start_new_generation(&mut self) {
        self.service_epoch = self.service_epoch.saturating_add(1);
        self.phase_version = 0;
        self.touch();
    }

    /// Returns the current coarse lifecycle status for callers that only need the enum state.
    pub fn status(&self) -> ServiceStatus {
        self.status
    }

    /// Updates the coarse lifecycle status and clears any detail attached to the previous state.
    pub fn set_status(&mut self, status: ServiceStatus) {
        if self.status != status || self.status_detail.is_some() {
            self.phase_version = self.phase_version.saturating_add(1);
        }
        self.status = status;
        self.status_detail = None;
        self.touch();
    }

    /// Updates the human-readable lifecycle detail shown while a service stays in one status.
    pub fn set_status_detail(&mut self, detail: Option<String>) {
        let detail = detail.and_then(|detail| {
            let trimmed = detail.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });
        if self.status_detail != detail {
            self.phase_version = self.phase_version.saturating_add(1);
        }
        self.status_detail = detail;
        self.touch();
    }

    /// Returns the public-endpoint specific lifecycle detail without its stable display prefix.
    pub fn public_endpoint_detail(&self) -> Option<&str> {
        self.status_detail
            .as_deref()
            .and_then(|detail| detail.strip_prefix(SERVICE_PUBLIC_ENDPOINT_DETAIL_PREFIX))
            .map(str::trim)
            .filter(|detail| !detail.is_empty())
    }

    /// Updates only the public-endpoint lifecycle detail while preserving unrelated status text.
    pub fn set_public_endpoint_detail(&mut self, detail: Option<String>) {
        let detail = detail.and_then(|detail| {
            let trimmed = detail.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });

        match detail {
            Some(detail) => {
                self.set_status_detail(Some(format!(
                    "{SERVICE_PUBLIC_ENDPOINT_DETAIL_PREFIX}{detail}"
                )));
            }
            None => {
                if self.public_endpoint_detail().is_some() {
                    self.set_status_detail(None);
                }
            }
        }
    }

    /// Updates rollout progress metadata and advances causal ordering when values change.
    pub fn set_rollout(&mut self, rollout: ServiceRolloutState) {
        if self.rollout != rollout {
            self.phase_version = self.phase_version.saturating_add(1);
        }
        self.rollout = rollout;
        self.touch();
    }
}

/// Snapshot of the prior service generation kept long enough for deterministic owner adoption.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServicePreviousGeneration {
    pub manifest_id: Uuid,
    pub manifest_name: String,
    pub task_templates: Vec<TaskTemplateSpecValue>,
    pub replica_ids: Vec<Uuid>,
    #[serde(default)]
    pub update_strategy: ServiceUpdateStrategy,
    #[serde(default)]
    pub service_epoch: u64,
    #[serde(default)]
    pub status: ServiceStatus,
}

impl ServicePreviousGeneration {
    /// Captures the previous service generation so another node can adopt the rollout later.
    pub fn from_service(spec: &ServiceSpecValue) -> Self {
        Self {
            manifest_id: spec.manifest_id,
            manifest_name: spec.manifest_name.clone(),
            task_templates: spec.task_templates.clone(),
            replica_ids: spec.replica_ids.clone(),
            update_strategy: spec.update_strategy.clone(),
            service_epoch: spec.service_epoch,
            status: spec.status,
        }
    }

    /// Rebuilds one in-memory service spec from the persisted prior-generation rollout context.
    pub fn to_service_spec(
        &self,
        service_id: Uuid,
        service_name: impl Into<String>,
    ) -> ServiceSpecValue {
        let mut spec = ServiceSpecValue::new(
            self.manifest_id,
            self.manifest_name.clone(),
            service_name,
            self.task_templates.clone(),
            self.replica_ids.clone(),
        );
        spec.id = service_id;
        spec.update_strategy = self.update_strategy.clone();
        spec.service_epoch = self.service_epoch;
        spec.status = self.status;
        spec
    }
}

#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceUpdateStrategyMode {
    #[default]
    Rolling,
}

#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRolloutOrder {
    #[default]
    StartFirst,
    StopFirst,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceRollingUpdatePolicy {
    pub parallelism: u16,
    pub order: ServiceRolloutOrder,
    pub startup_timeout_secs: u32,
    pub monitor_secs: u32,
    pub max_failures: u16,
    pub auto_rollback: bool,
}

impl Default for ServiceRollingUpdatePolicy {
    fn default() -> Self {
        Self {
            parallelism: 1,
            order: ServiceRolloutOrder::StartFirst,
            startup_timeout_secs: 600,
            monitor_secs: 1,
            max_failures: 1,
            auto_rollback: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ServiceUpdateStrategy {
    #[serde(default)]
    pub mode: ServiceUpdateStrategyMode,
    #[serde(default)]
    pub rolling: ServiceRollingUpdatePolicy,
}

#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRolloutPhase {
    #[default]
    Idle,
    RollingForward,
    RollingBack,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceRolloutState {
    #[serde(default)]
    pub phase: ServiceRolloutPhase,
    #[serde(default)]
    pub total_steps: u32,
    #[serde(default)]
    pub completed_steps: u32,
    #[serde(default)]
    pub failed_steps: u32,
    #[serde(default)]
    pub max_failures: u16,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl Default for ServiceRolloutState {
    fn default() -> Self {
        Self {
            phase: ServiceRolloutPhase::Idle,
            total_steps: 0,
            completed_steps: 0,
            failed_steps: 0,
            max_failures: 0,
            last_error: None,
        }
    }
}

/// Default readiness probe interval in milliseconds.
fn default_readiness_interval_ms() -> u64 {
    2_000
}

/// Default readiness probe timeout in milliseconds.
fn default_readiness_timeout_ms() -> u64 {
    300
}

/// Default readiness failure threshold before a backend is removed from service.
fn default_readiness_failure_threshold() -> u32 {
    1
}

/// Transport style used by distributed readiness probing.
#[derive(
    Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceReadinessProbeKind {
    #[default]
    Http,
    Tcp,
}

/// Declarative readiness probe consumed by service discovery to admit or remove backends.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceReadinessProbe {
    #[serde(default)]
    pub kind: ServiceReadinessProbeKind,
    pub port: u16,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_readiness_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_readiness_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_readiness_failure_threshold")]
    pub failure_threshold: u32,
}

impl ServiceReadinessProbe {
    /// Returns the effective readiness probe period used by discovery refresh and DNS filtering.
    pub fn interval(&self) -> Duration {
        Duration::from_millis(self.interval_ms)
    }

    /// Returns the maximum probe runtime used for one readiness check attempt.
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Returns the HTTP path to probe when HTTP readiness is selected.
    pub fn http_path(&self) -> Option<&str> {
        match self.kind {
            ServiceReadinessProbeKind::Http => Some(self.path.as_deref().unwrap_or("/")),
            ServiceReadinessProbeKind::Tcp => None,
        }
    }

    /// Returns the normalized failure threshold, never allowing a zero threshold.
    pub fn failure_threshold(&self) -> u32 {
        self.failure_threshold.max(1)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskTemplateSpecValue {
    /// Template-local name used to identify one replica set within the service.
    pub name: String,
    /// Shared execution/runtime template reused by every replica of this task template.
    pub execution: ExecutionSpec<TaskTemplateNetworkRequirement>,
    /// Template names within the same service that must be ready before this template starts.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Desired replica count for this template.
    pub replicas: u16,
    #[serde(default)]
    pub readiness: Option<ServiceReadinessProbe>,
    #[serde(default)]
    pub public_port: Option<u16>,
    #[serde(default)]
    pub public_protocol: Option<ServicePortProtocol>,
}

/// Supported transport protocols for publicly exposed service ports.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ServicePortProtocol {
    #[default]
    Tcp,
    Udp,
    TcpUdp,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskTemplateNetworkRequirement {
    pub name: String,
    pub network_id: Uuid,
}

impl TaskTemplateNetworkRequirement {
    pub fn new(name: impl Into<String>, network_id: Uuid) -> Self {
        Self {
            name: name.into(),
            network_id,
        }
    }
}

impl TaskTemplateSpecValue {
    /// Returns the distributed readiness probe, if the template declares one.
    pub fn readiness(&self) -> Option<&ServiceReadinessProbe> {
        self.readiness.as_ref()
    }

    /// Returns the local liveness probe, if the template declares one.
    pub fn liveness(&self) -> Option<&ServiceLivenessProbe> {
        self.execution.liveness.as_ref()
    }

    /// Builds the resolved execution spec by replacing service network
    /// requirements with concrete network ids.
    pub fn resolved_execution(&self) -> ResolvedExecutionSpec {
        self.execution.map_networks(|network| network.network_id)
    }

    /// Returns the scheduler placement policy declared for this template.
    pub fn placement(&self) -> &PlacementPolicy {
        &self.execution.placement
    }

    pub fn required_network_ids(&self) -> Vec<Uuid> {
        self.execution
            .networks
            .iter()
            .map(|network| network.network_id)
            .collect()
    }

    /// Builds one workload start request for a specific service replica.
    ///
    /// The resulting workload is still launched through the shared workload manager, but the
    /// attached owner marks it as `WorkloadKind::ServiceReplica` rather than a standalone direct
    /// task.
    pub fn replica_start_request(
        &self,
        service_name: &str,
        replica: u16,
        desired_id: Uuid,
        target_node: Option<Uuid>,
    ) -> WorkloadStartRequest {
        WorkloadStartRequest {
            name: format_replica_name(service_name, &self.name, replica, desired_id),
            execution: self.resolved_execution(),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(desired_id),
            slot_ids: Vec::new(),
            owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
                service_name,
                &self.name,
            ))),
            target_node,
        }
    }

    /// Return the port that should be reachable from the host via the network VIP, if one was
    /// declared in the service manifest.
    pub fn public_port(&self) -> Option<u16> {
        self.public_port
    }

    /// Returns the backend port public ingress should target for this template.
    ///
    /// Mantissa publishes `node_ip:public_port`, but the overlay VIP dataplane still needs the
    /// workload's actual listen port. We infer that from the readiness probe first because it is
    /// the controller-level signal used to admit healthy backends into discovery. When readiness
    /// is absent, TCP/HTTP liveness still provides a concrete network port. As a final fallback we
    /// reuse `public_port`, which preserves the original behavior for services that publish and
    /// listen on the same numeric port.
    pub fn public_target_port(&self) -> Option<u16> {
        self.readiness()
            .map(|probe| probe.port)
            .filter(|port| *port != 0)
            .or_else(|| {
                self.liveness().and_then(|probe| match probe.kind {
                    ServiceLivenessProbeKind::Http | ServiceLivenessProbeKind::Tcp => {
                        (probe.port != 0).then_some(probe.port)
                    }
                    ServiceLivenessProbeKind::Exec => None,
                })
            })
            .or(self.public_port())
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

/// Formats one stable service replica workload name from the template and desired identifier.
fn format_replica_name(service_name: &str, template_name: &str, replica: u16, id: Uuid) -> String {
    let suffix = short_id(&id);
    format!("{service_name}-{template_name}-{replica}-{suffix}")
}

/// Produces a compact identifier fragment for replica names while preserving readability.
fn short_id(id: &Uuid) -> String {
    let raw = id.as_simple().to_string();
    raw[..8].to_string()
}

impl Deref for TaskTemplateSpecValue {
    type Target = ExecutionSpec<TaskTemplateNetworkRequirement>;

    /// Exposes the shared execution fields so service callers can keep using task-like accessors.
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
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
    VolumeUnavailable,
    #[default]
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceRescheduleLock {
    pub holder_id: Uuid,
    pub holder_name: String,
    pub token: Uuid,
    pub issued_at: String,
    pub expires_at: String,
    pub reason: ServiceRescheduleReason,
}

impl ServiceRescheduleLock {
    /// Creates a new reschedule lock with the provided metadata to coordinate service reconciliation.
    pub fn new(
        holder_id: Uuid,
        holder_name: impl Into<String>,
        token: Uuid,
        issued_at: String,
        expires_at: String,
        reason: ServiceRescheduleReason,
    ) -> Self {
        Self {
            holder_id,
            holder_name: holder_name.into(),
            token,
            issued_at,
            expires_at,
            reason,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRescheduleReason {
    MissingReplicas,
    ExcessReplicas,
    Drift,
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
    use super::{
        SERVICE_PUBLIC_ENDPOINT_DETAIL_PREFIX, ServiceSpecValue, TaskTemplateSpecValue,
        compute_service_id,
    };
    use crate::workload::types::ExecutionSpec;
    use uuid::Uuid;

    #[test]
    fn service_id_deterministic() {
        let first = compute_service_id("alpha-web");
        let second = compute_service_id("alpha-web");
        assert_eq!(first, second);

        let other = compute_service_id("beta-web");
        assert_ne!(first, other);
    }

    /// Public-endpoint detail helpers should round-trip the prefixed lifecycle text cleanly.
    #[test]
    fn public_endpoint_detail_round_trips_through_status_detail() {
        let mut spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "manifest",
            "demo-service",
            vec![TaskTemplateSpecValue {
                name: "web".into(),
                execution: ExecutionSpec {
                    image: "ghcr.io/demo/web:latest".into(),
                    command: Vec::new(),
                    tty: false,
                    cpu_millis: 0,
                    memory_bytes: 0,
                    gpu_count: 0,
                    restart_policy: None,
                    termination_grace_period_secs: None,
                    pre_stop_command: None,
                    liveness: None,
                    env: Vec::new(),
                    secret_files: Vec::new(),
                    volumes: Vec::new(),
                    networks: Vec::new(),
                    ports: Vec::new(),
                    placement: Default::default(),
                },
                depends_on: Vec::new(),
                replicas: 1,
                readiness: None,
                public_port: Some(443),
                public_protocol: None,
            }],
            Vec::new(),
        );

        spec.set_public_endpoint_detail(Some("template 'web' public port 443 is degraded".into()));
        assert_eq!(
            spec.public_endpoint_detail(),
            Some("template 'web' public port 443 is degraded")
        );
        assert_eq!(
            spec.status_detail.as_deref(),
            Some("public endpoint: template 'web' public port 443 is degraded")
        );

        spec.set_public_endpoint_detail(None);
        assert!(spec.public_endpoint_detail().is_none());
        assert!(spec.status_detail.is_none());

        spec.status_detail = Some(format!(
            "{SERVICE_PUBLIC_ENDPOINT_DETAIL_PREFIX}template 'web' public port 443 is ready"
        ));
        assert_eq!(
            spec.public_endpoint_detail(),
            Some("template 'web' public port 443 is ready")
        );
    }
}
