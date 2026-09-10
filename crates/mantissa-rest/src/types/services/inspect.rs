use super::{
    PublicIngressPolicy, ServiceReplicaAssignment, ServiceRollout, ServiceSummary,
    ServiceTaskProgress, TaskTemplate, TaskTemplateAutoscalePolicy,
};
use crate::types::{common::HostPort, volumes::FilesystemOwnership};
use mantissa_client::services::{
    list::ServiceRow, manifest as config, rollout::classify_rollout_outcome,
};
use mantissa_protocol::{services as protocol, volumes::filesystem_ownership, workload};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

/// Complete service configuration and progress from one daemon status snapshot.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceDetail {
    pub id: String,
    pub service_id: String,
    pub manifest_id: String,
    pub manifest_name: String,
    pub service_name: String,
    pub updated_at: String,
    pub service_epoch: u64,
    pub status: String,
    pub status_detail: Option<String>,
    pub rollout: ServiceRolloutDetail,
    pub replica_ids: Vec<String>,
    pub replica_assignments: Vec<ServiceReplicaAssignment>,
    pub replica_count: usize,
    pub admission: Option<config::WorkloadAdmissionPolicy>,
    pub deployment: Option<config::ServiceDeploymentPolicy>,
    pub update: Option<config::ServiceUpdateStrategy>,
    pub previous_generation: Option<ServicePreviousGeneration>,
    pub rescheduling: Option<ServiceRescheduling>,
    pub task_templates: Vec<ServiceTaskTemplate>,
    pub task_progress: Vec<ServiceTaskProgress>,
}

impl ServiceDetail {
    /// Uses the shared summary decoder while retaining the configuration omitted from lists.
    pub fn from_snapshot(
        snapshot: protocol::service_status_snapshot::Reader<'_>,
    ) -> capnp::Result<Self> {
        let service = snapshot.get_service()?;
        let row = ServiceRow::from_snapshot(snapshot)?;
        let outcome = classify_rollout_outcome(&row).replace('-', "_");
        let summary = ServiceSummary::from(row);

        let task_templates = service
            .get_task_templates()?
            .iter()
            .zip(summary.task_templates)
            .map(|(template, summary)| ServiceTaskTemplate::from_reader(template, summary))
            .collect::<capnp::Result<Vec<_>>>()?;

        let admission = if service.has_admission_policy() {
            Some(config::WorkloadAdmissionPolicy {
                mode: match service.get_admission_policy()?.get_mode()? {
                    workload::AdmissionMode::Incremental => {
                        config::WorkloadAdmissionMode::Incremental
                    }
                    workload::AdmissionMode::Gang => config::WorkloadAdmissionMode::Gang,
                },
            })
        } else {
            None
        };

        let deployment = if service.has_deployment_policy() {
            let policy = service.get_deployment_policy()?;
            Some(config::ServiceDeploymentPolicy {
                progress_deadline_secs: policy.get_progress_deadline_secs(),
                healthy_deadline_secs: policy.get_healthy_deadline_secs(),
                min_healthy_secs: policy.get_min_healthy_secs(),
            })
        } else {
            None
        };

        let update = if service.has_update_strategy() {
            let strategy = service.get_update_strategy()?;
            let rolling = strategy.get_rolling()?;
            Some(config::ServiceUpdateStrategy {
                mode: match strategy.get_mode()? {
                    protocol::UpdateStrategyMode::Rolling => {
                        config::ServiceUpdateStrategyMode::Rolling
                    }
                },
                rolling: config::RollingUpdatePolicy {
                    parallelism: rolling.get_parallelism(),
                    order: match rolling.get_order()? {
                        protocol::RolloutOrder::StartFirst => config::RolloutOrder::StartFirst,
                        protocol::RolloutOrder::StopFirst => config::RolloutOrder::StopFirst,
                    },
                    max_failures: rolling.get_max_failures(),
                    auto_rollback: rolling.get_auto_rollback(),
                },
            })
        } else {
            None
        };

        let previous_generation = if service.has_previous_generation() {
            let previous = service.get_previous_generation()?;
            Some(ServicePreviousGeneration {
                manifest_id: uuid_to_string(previous.get_manifest_id()?)?,
                service_epoch: previous.get_service_epoch(),
            })
        } else {
            None
        };

        let rescheduling = if service.has_reschedule_lock() {
            let lock = service.get_reschedule_lock()?;
            Some(ServiceRescheduling {
                holder_id: uuid_to_string(lock.get_holder_id()?)?,
                holder_name: lock.get_holder_name()?.to_str()?.to_string(),
                issued_at: lock.get_issued_at()?.to_str()?.to_string(),
                expires_at: lock.get_expires_at()?.to_str()?.to_string(),
                reason: match lock.get_reason()? {
                    protocol::RescheduleReason::MissingReplicas => {
                        ServiceRescheduleReason::MissingReplicas
                    }
                    protocol::RescheduleReason::ExcessReplicas => {
                        ServiceRescheduleReason::ExcessReplicas
                    }
                    protocol::RescheduleReason::Drift => ServiceRescheduleReason::Drift,
                },
            })
        } else {
            None
        };

        Ok(Self {
            id: summary.id,
            service_id: summary.service_id,
            manifest_id: summary.manifest_id,
            manifest_name: service.get_manifest_name()?.to_str()?.to_string(),
            service_name: summary.service_name,
            updated_at: summary.updated_at,
            service_epoch: summary.service_epoch,
            status: summary.status,
            status_detail: summary.status_detail,
            rollout: ServiceRolloutDetail {
                outcome,
                state: summary.rollout,
            },
            replica_ids: summary.replica_ids,
            replica_assignments: summary.replica_assignments,
            replica_count: summary.replica_count,
            admission,
            deployment,
            update,
            previous_generation,
            rescheduling,
            task_templates,
            task_progress: summary.task_progress,
        })
    }
}

/// Adds the CLI's derived outcome to the existing rollout counters and diagnostics.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceRolloutDetail {
    /// One of stable, in_progress, blocked, rolled_back, or failed.
    pub outcome: String,
    #[serde(flatten)]
    pub state: ServiceRollout,
}

/// Identifies the generation retained for rollback without repeating its full configuration.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServicePreviousGeneration {
    pub manifest_id: String,
    pub service_epoch: u64,
}

/// Describes active rescheduling without exposing the internal lock token.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceRescheduling {
    pub holder_id: String,
    pub holder_name: String,
    pub issued_at: String,
    pub expires_at: String,
    pub reason: ServiceRescheduleReason,
}

/// Stable values for the cause of an active rescheduling operation.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceRescheduleReason {
    MissingReplicas,
    ExcessReplicas,
    Drift,
}

/// Full desired settings for each replica, with exact resource and timing units.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceTaskTemplate {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub depends_on: Vec<String>,
    pub replicas: u16,
    pub resources: ServiceTaskResources,
    pub tty: bool,
    pub restart_policy: Option<config::TaskTemplateRestartPolicy>,
    /// Null means the runtime default is used.
    pub termination_grace_period_secs: Option<u32>,
    pub pre_stop_command: Vec<String>,
    pub autoscale: Option<TaskTemplateAutoscalePolicy>,
    pub placement: config::PlacementSpec,
    pub readiness: Option<config::ReadinessProbe>,
    pub liveness: Option<config::LivenessProbe>,
    pub ports: Vec<HostPort>,
    pub public_port: Option<u16>,
    pub public_protocol: Option<ServicePublicProtocol>,
    pub public_ingress: Option<PublicIngressPolicy>,
    pub networks: Vec<ServiceTaskNetwork>,
    pub volumes: Vec<ServiceVolumeMount>,
    pub env: Vec<config::EnvironmentVariable>,
    pub secret_files: Vec<ServiceSecretFile>,
}

impl ServiceTaskTemplate {
    /// Decodes optional settings explicitly and never reads decrypted secret contents.
    fn from_reader(
        template: protocol::task_template::Reader<'_>,
        summary: TaskTemplate,
    ) -> capnp::Result<Self> {
        let restart_policy = if template.has_restart_policy() {
            let policy = template.get_restart_policy()?;
            Some(config::TaskTemplateRestartPolicy {
                name: match policy.get_name()? {
                    protocol::RestartPolicyName::No => config::RestartPolicyName::No,
                    protocol::RestartPolicyName::Always => config::RestartPolicyName::Always,
                    protocol::RestartPolicyName::OnFailure => config::RestartPolicyName::OnFailure,
                    protocol::RestartPolicyName::UnlessStopped => {
                        config::RestartPolicyName::UnlessStopped
                    }
                },
                max_retry_count: policy.get_max_retry_count().try_into().ok(),
            })
        } else {
            None
        };

        let public_protocol = if summary.public_port.is_some() {
            Some(match template.get_public_protocol()? {
                protocol::PublicProtocol::Tcp => ServicePublicProtocol::Tcp,
                protocol::PublicProtocol::Udp => ServicePublicProtocol::Udp,
                protocol::PublicProtocol::TcpUdp => ServicePublicProtocol::TcpUdp,
            })
        } else {
            None
        };

        let networks = template
            .get_networks()?
            .iter()
            .map(|network| {
                Ok(ServiceTaskNetwork {
                    name: network.get_name()?.to_str()?.to_string(),
                    network_id: uuid_to_string(network.get_network_id()?)?,
                })
            })
            .collect::<capnp::Result<Vec<_>>>()?;

        let volumes = template
            .get_volumes()?
            .iter()
            .map(|mount| {
                Ok(ServiceVolumeMount {
                    volume_name: mount.get_volume_name()?.to_str()?.to_string(),
                    volume_id: uuid_to_string(mount.get_volume_id()?)?,
                    target: mount.get_target()?.to_str()?.to_string(),
                    read_only: mount.get_read_only(),
                })
            })
            .collect::<capnp::Result<Vec<_>>>()?;

        let env = template
            .get_env()?
            .iter()
            .map(|variable| {
                // A secret reference takes precedence even if a literal value is also present on the wire.
                let (value, secret) = if variable.has_secret() {
                    (None, Some(secret_reference(variable.get_secret()?)?))
                } else {
                    (Some(variable.get_value()?.to_str()?.to_string()), None)
                };

                Ok(config::EnvironmentVariable {
                    name: variable.get_name()?.to_str()?.to_string(),
                    value,
                    secret,
                })
            })
            .collect::<capnp::Result<Vec<_>>>()?;

        let secret_files = template
            .get_secret_files()?
            .iter()
            .map(|file| {
                let ownership = match file.get_ownership()?.which()? {
                    filesystem_ownership::Which::Daemon(()) => FilesystemOwnership {
                        kind: "daemon".into(),
                        uid: None,
                        gid: None,
                    },
                    filesystem_ownership::Which::User(user) => {
                        let user = user?;
                        FilesystemOwnership {
                            kind: "user".into(),
                            uid: Some(user.get_uid()),
                            gid: Some(user.get_gid()),
                        }
                    }
                    filesystem_ownership::Which::FsGroup(group) => FilesystemOwnership {
                        kind: "fs_group".into(),
                        uid: None,
                        gid: Some(group?.get_gid()),
                    },
                };

                let path_env_name = file.get_path_env_name()?.to_str()?;
                Ok(ServiceSecretFile {
                    path: file.get_path()?.to_str()?.to_string(),
                    secret: secret_reference(file.get_secret()?)?,
                    mode: (file.get_mode() != 0).then_some(file.get_mode()),
                    ownership,
                    path_env_name: (!path_env_name.is_empty()).then(|| path_env_name.to_string()),
                })
            })
            .collect::<capnp::Result<Vec<_>>>()?;

        let grace = template.get_termination_grace_period_secs();
        Ok(Self {
            name: summary.name,
            image: summary.image,
            command: summary.command,
            depends_on: text_list(template.get_depends_on()?)?,
            replicas: summary.replicas,
            resources: ServiceTaskResources {
                cpu_millis: template.get_cpu_millis(),
                memory_bytes: template.get_memory_bytes(),
                gpu_count: template.get_gpu_count(),
            },
            tty: template.get_tty(),
            restart_policy,
            termination_grace_period_secs: (grace != 0).then_some(grace),
            pre_stop_command: text_list(template.get_pre_stop_command()?)?,
            autoscale: summary.autoscale,
            placement: placement(template)?,
            readiness: readiness(template)?,
            liveness: liveness(template)?,
            ports: summary.ports,
            public_port: summary.public_port,
            public_protocol,
            public_ingress: summary.public_port.map(|_| summary.public_ingress),
            networks,
            volumes,
            env,
            secret_files,
        })
    }
}

/// Resource requests for a single replica, without display rounding or unit conversion.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceTaskResources {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub gpu_count: u32,
}

/// Transport protocols served by a template's public port.
#[derive(Clone, Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServicePublicProtocol {
    Tcp,
    Udp,
    TcpUdp,
}

/// Keeps both the manifest alias and the resolved network identity available to consumers.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceTaskNetwork {
    pub name: String,
    pub network_id: String,
}

/// Resolves a named volume mount without requiring a separate volume lookup.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceVolumeMount {
    pub volume_name: String,
    pub volume_id: String,
    pub target: String,
    pub read_only: bool,
}

/// Describes a projected secret file while keeping its contents out of inspection responses.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct ServiceSecretFile {
    pub path: String,
    pub secret: config::SecretReference,
    /// Numeric Unix permissions; null means the secret policy default.
    pub mode: Option<u32>,
    pub ownership: FilesystemOwnership,
    pub path_env_name: Option<String>,
}

/// Keeps selector fields and operators structured using the same types as deployment requests.
fn placement(
    template: protocol::task_template::Reader<'_>,
) -> capnp::Result<config::PlacementSpec> {
    let policy = template.get_placement()?;
    let constraints = policy
        .get_constraints()?
        .iter()
        .map(|constraint| {
            use workload::placement_constraint_selector::Which;

            let selector = match constraint.get_selector()?.which()? {
                Which::NodeId(()) => config::PlacementConstraintSelector::NodeId,
                Which::NodeHostname(()) => config::PlacementConstraintSelector::NodeHostname,
                Which::NodeIp(()) => config::PlacementConstraintSelector::NodeIp,
                Which::NodeAddress(()) => config::PlacementConstraintSelector::NodeAddress,
                Which::NodePlatformOs(()) => config::PlacementConstraintSelector::NodePlatformOs,
                Which::NodePlatformArch(()) => {
                    config::PlacementConstraintSelector::NodePlatformArch
                }
                Which::NodeLabel(key) => config::PlacementConstraintSelector::NodeLabel {
                    key: key?.to_str()?.to_string(),
                },
            };

            Ok(config::PlacementConstraint {
                selector,
                operator: match constraint.get_operator()? {
                    workload::PlacementConstraintOperator::Eq => {
                        config::PlacementConstraintOperator::Eq
                    }
                    workload::PlacementConstraintOperator::Ne => {
                        config::PlacementConstraintOperator::Ne
                    }
                },
                value: constraint.get_value()?.to_str()?.to_string(),
            })
        })
        .collect::<capnp::Result<Vec<_>>>()?;

    let preferences = template
        .get_service_placement_preferences()?
        .iter()
        .map(|preference| {
            Ok(match preference? {
                protocol::ServicePlacementPreference::ServiceAffinity => {
                    config::ServicePlacementPreference::ServiceAffinity
                }
                protocol::ServicePlacementPreference::ServiceAntiAffinity => {
                    config::ServicePlacementPreference::ServiceAntiAffinity
                }
                protocol::ServicePlacementPreference::TaskAffinity => {
                    config::ServicePlacementPreference::TaskAffinity
                }
                protocol::ServicePlacementPreference::TaskAntiAffinity => {
                    config::ServicePlacementPreference::TaskAntiAffinity
                }
            })
        })
        .collect::<capnp::Result<Vec<_>>>()?;

    Ok(config::PlacementSpec {
        strategy: match policy.get_strategy()? {
            workload::PlacementStrategy::Spread => config::PlacementStrategy::Spread,
            workload::PlacementStrategy::Binpack => config::PlacementStrategy::Binpack,
        },
        constraints,
        preferences,
    })
}

/// Preserves all readiness thresholds and distinguishes an absent probe from a TCP probe.
fn readiness(
    template: protocol::task_template::Reader<'_>,
) -> capnp::Result<Option<config::ReadinessProbe>> {
    if !template.has_readiness() {
        return Ok(None);
    }

    let probe = template.get_readiness()?;
    let (kind, path) = match probe.get_kind()? {
        protocol::ReadinessProbeKind::Http => (
            config::ReadinessKind::Http,
            Some(probe.get_path()?.to_str()?.to_string()),
        ),
        protocol::ReadinessProbeKind::Tcp => (config::ReadinessKind::Tcp, None),
    };

    Ok(Some(config::ReadinessProbe {
        kind,
        port: probe.get_port(),
        path,
        interval_ms: probe.get_interval_ms(),
        timeout_ms: probe.get_timeout_ms(),
        failure_threshold: probe.get_failure_threshold(),
    }))
}

/// Preserves command argument boundaries and exact liveness timing for API consumers.
fn liveness(
    template: protocol::task_template::Reader<'_>,
) -> capnp::Result<Option<config::LivenessProbe>> {
    if !template.has_liveness() {
        return Ok(None);
    }

    let probe = template.get_liveness()?;
    let (kind, path) = match probe.get_kind()? {
        protocol::LivenessProbeKind::Exec => (config::LivenessKind::Exec, None),
        protocol::LivenessProbeKind::Http => (
            config::LivenessKind::Http,
            Some(probe.get_path()?.to_str()?.to_string()),
        ),
        protocol::LivenessProbeKind::Tcp => (config::LivenessKind::Tcp, None),
    };

    Ok(Some(config::LivenessProbe {
        kind,
        command: text_list(probe.get_command()?)?,
        port: probe.get_port(),
        path,
        interval_ms: probe.get_interval_ms(),
        timeout_ms: probe.get_timeout_ms(),
        failure_threshold: probe.get_failure_threshold(),
        start_period_ms: probe.get_start_period_ms(),
    }))
}

/// Represents latest-version references with null instead of an artificial version identifier.
fn secret_reference(
    secret: workload::secret_ref::Reader<'_>,
) -> capnp::Result<config::SecretReference> {
    let version = secret.get_version_id()?;
    Ok(config::SecretReference {
        name: secret.get_name()?.to_str()?.to_string(),
        version: if version.is_empty() {
            None
        } else {
            Some(uuid_to_string(version)?)
        },
    })
}

/// Retains exact text and argument boundaries when converting Cap'n Proto lists to JSON arrays.
fn text_list(values: capnp::text_list::Reader<'_>) -> capnp::Result<Vec<String>> {
    values
        .iter()
        .map(|value| Ok(value?.to_str()?.to_string()))
        .collect()
}

/// Requires full UUIDs so inspection references can be used in other REST requests.
fn uuid_to_string(bytes: &[u8]) -> capnp::Result<String> {
    Uuid::from_slice(bytes)
        .map(|id| id.to_string())
        .map_err(|error| capnp::Error::failed(error.to_string()))
}
