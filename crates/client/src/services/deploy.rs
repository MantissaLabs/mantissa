use super::manifest::{
    EnvironmentVariable, LivenessKind, LivenessProbe, ReadinessKind, ReadinessProbe,
    RestartPolicyName, RolloutOrder, SecretFileProjection, SecretReference, ServiceManifest,
    ServiceUpdateStrategy, ServiceUpdateStrategyMode, TaskTemplateSpec, VolumeMount,
};
use crate::config::ClientConfig;
use crate::connection;
use crate::networks;
use crate::volumes;
use anyhow::{Context, Result, anyhow};
use blake3::Hasher;
use capnp::{Error as CapnpError, struct_list};
use protocol::services::task_template;
use protocol::task::{environment_var, secret_file, secret_ref, volume_mount};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// Identifies the asynchronous deployment issued against the cluster so callers can poll status.
#[derive(Debug, Clone)]
pub struct ServiceDeploymentHandle {
    pub service_id: Uuid,
    pub manifest_id: Uuid,
}

/// Simplified deploy outcomes surfaced by the services RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeployOutcome {
    Accepted,
    Unchanged,
}

#[derive(Clone)]
struct ResolvedManifestVolume {
    volume_id: Uuid,
    volume_name: String,
}

/// Derive the canonical network UUID from the manifest-facing network name.
fn compute_network_id(name: &str) -> Uuid {
    let mut hasher = Hasher::new();
    hasher.update(name.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Ensure every network referenced by the manifest exists so scheduling can attach task templates reliably.
async fn ensure_manifest_networks(cfg: &ClientConfig, manifest: &ServiceManifest) -> Result<()> {
    let mut required = Vec::new();
    let mut seen = HashSet::new();
    for template in &manifest.task_templates {
        for network in &template.networks {
            let trimmed = network.trim();
            if trimmed.is_empty() {
                continue;
            }
            if seen.insert(trimmed.to_string()) {
                required.push(trimmed.to_string());
            }
        }
    }

    if required.is_empty() {
        return Ok(());
    }

    let existing = networks::list_raw(cfg).await?;
    let existing_names: HashSet<String> = existing.into_iter().map(|net| net.name).collect();

    for name in required {
        if existing_names.contains(&name) {
            continue;
        }

        let request = networks::default_network_create_request(name.clone());
        match networks::create_raw(cfg, &request).await {
            Ok(network_id) => {
                println!("network '{name}' created with id {network_id} (auto-provisioned)");
            }
            Err(err) => {
                // Re-list to handle races where another actor created the network concurrently.
                let fallback = networks::list_raw(cfg).await?;
                if fallback.iter().any(|net| net.name == name) {
                    eprintln!(
                        "warning: auto-provision for network '{name}' failed but it already exists: {err}"
                    );
                    continue;
                }
                return Err(err);
            }
        }
    }

    Ok(())
}

fn write_secret_reference(
    mut builder: secret_ref::Builder<'_>,
    reference: &SecretReference,
    context: &str,
) -> Result<()> {
    builder.set_name(&reference.name);
    if let Some(version) = &reference.version {
        let uuid = Uuid::parse_str(version)
            .with_context(|| format!("invalid secret version '{version}' for {context}"))?;
        builder.set_version_id(uuid.as_bytes());
    } else {
        builder.set_version_id(&[]);
    }
    Ok(())
}

/// Ensures manifest-declared volumes exist as cluster objects and returns their resolved identities.
async fn ensure_manifest_volumes(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
) -> Result<HashMap<String, ResolvedManifestVolume>> {
    if manifest.volumes.is_empty() {
        return Ok(HashMap::new());
    }

    let existing = volumes::list_raw(cfg).await?;
    let existing_by_name: HashMap<String, volumes::VolumeSummary> = existing
        .into_iter()
        .map(|volume| (volume.name.clone(), volume))
        .collect();

    let mut resolved = HashMap::new();
    for volume in &manifest.volumes {
        match &volume.driver {
            super::manifest::VolumeDriver::Local(local) => match &local.source {
                super::manifest::LocalVolumeSource::Managed => {}
                super::manifest::LocalVolumeSource::ImportedPath(_) => {
                    return Err(anyhow!(
                        "service manifest volume '{}' cannot use imported_path; import host paths ahead of time with `mantissa volumes import`",
                        volume.name
                    ));
                }
            },
            super::manifest::VolumeDriver::External(_) => {
                return Err(anyhow!(
                    "service manifest volume '{}' cannot use an external driver yet",
                    volume.name
                ));
            }
        }

        let spec = if let Some(existing) = existing_by_name.get(&volume.name) {
            validate_manifest_volume_compatibility(existing, volume)?;
            volumes::inspect_raw(cfg, &volume.name).await?.spec
        } else {
            volumes::create_raw(
                cfg,
                &volumes::VolumeCreateRequest {
                    name: volume.name.clone(),
                    binding_mode: match volume.binding_mode {
                        super::manifest::VolumeBindingMode::Immediate => {
                            volumes::VolumeBindingMode::Immediate
                        }
                        super::manifest::VolumeBindingMode::WaitForFirstConsumer => {
                            volumes::VolumeBindingMode::WaitForFirstConsumer
                        }
                    },
                    reclaim_policy: match volume.reclaim_policy {
                        super::manifest::VolumeReclaimPolicy::Retain => {
                            volumes::VolumeReclaimPolicy::Retain
                        }
                        super::manifest::VolumeReclaimPolicy::Delete => {
                            volumes::VolumeReclaimPolicy::Delete
                        }
                    },
                    requested_bytes: volume
                        .capacity_mb
                        .map(|value| value.saturating_mul(1_048_576)),
                    labels: volume
                        .labels
                        .iter()
                        .map(|label| volumes::VolumeLabel {
                            key: label.key.clone(),
                            value: label.value.clone(),
                        })
                        .collect(),
                    node_selector: None,
                },
            )
            .await?
        };

        resolved.insert(
            volume.name.clone(),
            ResolvedManifestVolume {
                volume_id: spec.id,
                volume_name: spec.name,
            },
        );
    }

    Ok(resolved)
}

/// Validates that one existing cluster volume matches the immutable fields from the manifest.
fn validate_manifest_volume_compatibility(
    existing: &volumes::VolumeSummary,
    declared: &super::manifest::VolumeSpec,
) -> Result<()> {
    match (&existing.driver, &declared.driver) {
        (volumes::VolumeDriver::LocalManaged, super::manifest::VolumeDriver::Local(local))
            if matches!(local.source, super::manifest::LocalVolumeSource::Managed) => {}
        (
            volumes::VolumeDriver::LocalImportedPath(_),
            super::manifest::VolumeDriver::Local(super::manifest::LocalVolumeSpec {
                source: super::manifest::LocalVolumeSource::ImportedPath(_),
            }),
        ) => {}
        _ => {
            return Err(anyhow!(
                "existing volume '{}' does not match the manifest driver/source kind",
                declared.name
            ));
        }
    }

    let declared_access = match declared.access_mode {
        super::manifest::VolumeAccessMode::ReadWriteOnce => {
            volumes::VolumeAccessMode::ReadWriteOnce
        }
    };
    if existing.access_mode != declared_access {
        return Err(anyhow!(
            "existing volume '{}' does not match the manifest access_mode",
            declared.name
        ));
    }

    Ok(())
}

fn write_update_strategy(
    mut builder: protocol::services::update_strategy::Builder<'_>,
    strategy: &ServiceUpdateStrategy,
) {
    let mode = match strategy.mode {
        ServiceUpdateStrategyMode::Rolling => protocol::services::UpdateStrategyMode::Rolling,
    };
    builder.set_mode(mode);

    let mut rolling = builder.reborrow().init_rolling();
    rolling.set_parallelism(strategy.rolling.parallelism);
    let order = match strategy.rolling.order {
        RolloutOrder::StartFirst => protocol::services::RolloutOrder::StartFirst,
        RolloutOrder::StopFirst => protocol::services::RolloutOrder::StopFirst,
    };
    rolling.set_order(order);
    rolling.set_startup_timeout_secs(strategy.rolling.startup_timeout_secs);
    rolling.set_monitor_secs(strategy.rolling.monitor_secs);
    rolling.set_max_failures(strategy.rolling.max_failures);
    rolling.set_auto_rollback(strategy.rolling.auto_rollback);
}

fn write_env_vars(
    builder: &mut struct_list::Builder<environment_var::Owned>,
    vars: &[EnvironmentVariable],
    template_name: &str,
) -> Result<()> {
    for (idx, var) in vars.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_name(&var.name);
        if let Some(value) = &var.value {
            entry.set_value(value);
        }
        if let Some(secret) = &var.secret {
            let secret_builder = entry.reborrow().init_secret();
            let context = format!(
                "template '{}' environment '{}': secret",
                template_name, var.name
            );
            write_secret_reference(secret_builder, secret, &context)?;
        }
    }
    Ok(())
}

fn write_secret_files(
    builder: &mut struct_list::Builder<secret_file::Owned>,
    files: &[SecretFileProjection],
    template_name: &str,
) -> Result<()> {
    for (idx, file) in files.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_path(&file.path);
        let secret_builder = entry.reborrow().init_secret();
        let context = format!(
            "template '{}' secret file '{}': secret",
            template_name, file.path
        );
        write_secret_reference(secret_builder, &file.secret, &context)?;
        entry.set_mode(file.mode.unwrap_or(0));
    }
    Ok(())
}

/// Submits a service manifest to the local coordinator, returning immediately with the service id.
pub async fn deploy_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
) -> Result<ServiceDeploymentHandle> {
    let manifest_id = Uuid::new_v4();
    ensure_manifest_networks(cfg, manifest).await?;
    let resolved_volumes = ensure_manifest_volumes(cfg, manifest).await?;

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let mut deploy = services.deploy_request();

    {
        let mut spec = deploy.get().init_spec();
        spec.set_manifest_id(manifest_id.as_bytes());
        spec.set_manifest_name(&manifest.name);
        spec.set_service_name(&manifest.name);
        write_update_strategy(spec.reborrow().init_update_strategy(), &manifest.update);

        let mut templates_builder = spec
            .reborrow()
            .init_task_templates(manifest.task_templates.len() as u32);
        for (idx, template) in manifest.task_templates.iter().enumerate() {
            write_task_template(
                templates_builder.reborrow().get(idx as u32),
                template,
                &resolved_volumes,
            )?;
        }
    }

    let response = deploy.send().promise.await.map_err(map_deploy_rpc_error)?;
    let reader = response
        .get()
        .context("failed to read deployment response")?;
    let id_bytes = reader
        .get_service_id()
        .context("deployment response missing service id")?
        .to_owned();

    if id_bytes.len() != 16 {
        return Err(anyhow!(
            "deployment response contained invalid service id length {}",
            id_bytes.len()
        ));
    }

    let service_id = Uuid::from_slice(&id_bytes)
        .context("failed to decode service id from deployment response")?;

    let outcome = parse_deploy_outcome(reader.get_outcome()?);
    let detail = reader
        .get_detail()
        .ok()
        .and_then(|text| text.to_str().ok())
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);

    match outcome {
        DeployOutcome::Accepted => {
            println!(
                "service '{}' accepted with id {}",
                manifest.name, service_id
            );
            println!(
                "deployment is running in the background; track it with `mantissa services list` or stop it with `mantissa services stop {service_id}`"
            );
        }
        DeployOutcome::Unchanged => {
            if let Some(detail) = detail {
                println!(
                    "service '{}' unchanged (id {}): {}",
                    manifest.name, service_id, detail
                );
            } else {
                println!(
                    "service '{}' unchanged (id {}): already deployed at desired spec",
                    manifest.name, service_id
                );
            }
        }
    }

    Ok(ServiceDeploymentHandle {
        service_id,
        manifest_id,
    })
}

/// Writes one manifest task template into the Cap'n Proto builder for submission.
fn write_task_template(
    mut builder: task_template::Builder<'_>,
    template: &TaskTemplateSpec,
    resolved_volumes: &HashMap<String, ResolvedManifestVolume>,
) -> Result<()> {
    builder.set_name(&template.name);
    builder.set_image(&template.image);
    builder.set_replicas(template.replicas);
    builder.set_cpu_millis(template.resources.cpu_millis);
    builder.set_memory_bytes(template.resources.memory_bytes());
    builder.set_gpu_count(template.resources.gpu_count);
    builder.set_termination_grace_period_secs(template.termination_grace_period_secs.unwrap_or(0));
    let pre_stop = template.pre_stop_command.as_deref().unwrap_or(&[]);
    let mut pre_stop_builder = builder
        .reborrow()
        .init_pre_stop_command(pre_stop.len() as u32);
    for (idx, arg) in pre_stop.iter().enumerate() {
        pre_stop_builder.set(idx as u32, arg);
    }

    let mut cmd_builder = builder
        .reborrow()
        .init_command(template.command.len() as u32);
    for (idx, arg) in template.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }

    let mut depends_on_builder = builder
        .reborrow()
        .init_depends_on(template.depends_on.len() as u32);
    for (idx, dependency) in template.depends_on.iter().enumerate() {
        depends_on_builder.set(idx as u32, dependency);
    }

    if let Some(policy) = &template.restart_policy {
        let mut policy_builder = builder.reborrow().init_restart_policy();
        let name = match policy.name {
            RestartPolicyName::No => protocol::services::RestartPolicyName::No,
            RestartPolicyName::Always => protocol::services::RestartPolicyName::Always,
            RestartPolicyName::OnFailure => protocol::services::RestartPolicyName::OnFailure,
            RestartPolicyName::UnlessStopped => {
                protocol::services::RestartPolicyName::UnlessStopped
            }
        };
        policy_builder.set_name(name);
        policy_builder.set_max_retry_count(policy.max_retry_count.map_or(-1, |value| {
            i32::try_from(value).expect("validated restart policy bound")
        }));
    }

    let mut env_builder = builder.reborrow().init_env(template.env.len() as u32);
    write_env_vars(&mut env_builder, &template.env, &template.name)?;

    let mut networks_builder = builder
        .reborrow()
        .init_networks(template.networks.len() as u32);
    for (idx, network) in template.networks.iter().enumerate() {
        let trimmed = network.trim();
        let mut network_builder = networks_builder.reborrow().get(idx as u32);
        network_builder.set_name(trimmed);
        network_builder.set_network_id(compute_network_id(trimmed).as_bytes());
    }

    if let Some(readiness) = template.readiness.as_ref() {
        let builder = builder.reborrow().init_readiness();
        write_readiness_probe(builder, readiness);
    }
    if let Some(liveness) = template.liveness.as_ref() {
        let builder = builder.reborrow().init_liveness();
        write_liveness_probe(builder, liveness);
    }

    builder.set_public_port(template.public_port.unwrap_or(0));
    builder.set_tty(template.tty);

    let mut files_builder = builder
        .reborrow()
        .init_secret_files(template.secret_files.len() as u32);
    write_secret_files(&mut files_builder, &template.secret_files, &template.name)?;
    let mut volume_builder = builder
        .reborrow()
        .init_volumes(template.volumes.len() as u32);
    write_volume_mounts(
        &mut volume_builder,
        &template.name,
        &template.volumes,
        resolved_volumes,
    )?;

    Ok(())
}

/// Writes one readiness probe into the service deployment payload.
fn write_readiness_probe(
    mut builder: protocol::services::readiness_probe::Builder<'_>,
    probe: &ReadinessProbe,
) {
    let kind = match probe.kind {
        ReadinessKind::Http => protocol::services::ReadinessProbeKind::Http,
        ReadinessKind::Tcp => protocol::services::ReadinessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
}

/// Writes one local liveness probe into the service deployment payload.
fn write_liveness_probe(
    mut builder: protocol::services::liveness_probe::Builder<'_>,
    probe: &LivenessProbe,
) {
    let kind = match probe.kind {
        LivenessKind::Exec => protocol::services::LivenessProbeKind::Exec,
        LivenessKind::Http => protocol::services::LivenessProbeKind::Http,
        LivenessKind::Tcp => protocol::services::LivenessProbeKind::Tcp,
    };
    builder.set_kind(kind);
    let mut command_builder = builder.reborrow().init_command(probe.command.len() as u32);
    for (idx, arg) in probe.command.iter().enumerate() {
        command_builder.set(idx as u32, arg);
    }
    builder.set_port(probe.port);
    builder.set_path(probe.path.as_deref().unwrap_or(""));
    builder.set_interval_ms(probe.interval_ms);
    builder.set_timeout_ms(probe.timeout_ms);
    builder.set_failure_threshold(probe.failure_threshold);
    builder.set_start_period_ms(probe.start_period_ms);
}

/// Writes resolved named volume mounts into the task-template builder for deployment.
fn write_volume_mounts(
    builder: &mut struct_list::Builder<volume_mount::Owned>,
    template_name: &str,
    mounts: &[VolumeMount],
    resolved_volumes: &HashMap<String, ResolvedManifestVolume>,
) -> Result<()> {
    for (idx, mount) in mounts.iter().enumerate() {
        let source = mount.source.trim();
        let resolved = resolved_volumes.get(source).ok_or_else(|| {
            anyhow!(
                "template '{}' references unresolved volume '{}'",
                template_name,
                mount.source
            )
        })?;
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_volume_id(resolved.volume_id.as_bytes());
        entry.set_volume_name(&resolved.volume_name);
        entry.set_target(&mount.target);
        entry.set_read_only(mount.read_only);
    }
    Ok(())
}

/// Maps transport-level Cap'n Proto failures to user-facing deploy errors.
fn map_deploy_rpc_error(err: CapnpError) -> anyhow::Error {
    let text = err.to_string();
    if let Some((_, message)) = text.split_once("remote exception: ") {
        return anyhow!("service deployment rejected: {}", message.trim());
    }
    anyhow!("service deployment request failed: {text}")
}

/// Converts protocol deploy outcomes into a compact client-side representation.
fn parse_deploy_outcome(outcome: protocol::services::DeployOutcome) -> DeployOutcome {
    match outcome {
        protocol::services::DeployOutcome::Accepted => DeployOutcome::Accepted,
        protocol::services::DeployOutcome::Unchanged => DeployOutcome::Unchanged,
    }
}
