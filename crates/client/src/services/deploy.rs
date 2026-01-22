use super::manifest::{
    EnvironmentVariable, RestartPolicyName, SecretFileProjection, SecretReference, ServiceManifest,
    TaskSpec,
};
use crate::config::ClientConfig;
use crate::connection;
use crate::networks;
use anyhow::{Context, Result, anyhow};
use capnp::struct_list;
use protocol::services::task_template;
use protocol::task::{environment_var, secret_file, secret_ref};
use std::collections::HashSet;
use uuid::Uuid;

/// Identifies the asynchronous deployment issued against the cluster so callers can poll status.
#[derive(Debug, Clone)]
pub struct ServiceDeploymentHandle {
    pub service_id: Uuid,
    pub manifest_id: Uuid,
}

/// Ensure every network referenced by the manifest exists so scheduling can attach tasks reliably.
async fn ensure_manifest_networks(cfg: &ClientConfig, manifest: &ServiceManifest) -> Result<()> {
    let mut required = Vec::new();
    let mut seen = HashSet::new();
    for task in &manifest.tasks {
        for network in &task.networks {
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

    let existing = networks::list(cfg).await?;
    let existing_names: HashSet<String> = existing.into_iter().map(|net| net.name).collect();

    for name in required {
        if existing_names.contains(&name) {
            continue;
        }

        let request = networks::default_network_create_request(name.clone());
        match networks::create(cfg, &request).await {
            Ok(network_id) => {
                println!("network '{name}' created with id {network_id} (auto-provisioned)");
            }
            Err(err) => {
                // Re-list to handle races where another actor created the network concurrently.
                let fallback = networks::list(cfg).await?;
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

fn write_env_vars(
    builder: &mut struct_list::Builder<environment_var::Owned>,
    vars: &[EnvironmentVariable],
    task_name: &str,
) -> Result<()> {
    for (idx, var) in vars.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_name(&var.name);
        if let Some(value) = &var.value {
            entry.set_value(value);
        }
        if let Some(secret) = &var.secret {
            let secret_builder = entry.reborrow().init_secret();
            let context = format!("task '{}' environment '{}': secret", task_name, var.name);
            write_secret_reference(secret_builder, secret, &context)?;
        }
    }
    Ok(())
}

fn write_secret_files(
    builder: &mut struct_list::Builder<secret_file::Owned>,
    files: &[SecretFileProjection],
    task_name: &str,
) -> Result<()> {
    for (idx, file) in files.iter().enumerate() {
        let mut entry = builder.reborrow().get(idx as u32);
        entry.set_path(&file.path);
        let secret_builder = entry.reborrow().init_secret();
        let context = format!("task '{}' secret file '{}': secret", task_name, file.path);
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

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();
    let mut deploy = services.deploy_request();

    {
        let mut spec = deploy.get().init_spec();
        spec.set_manifest_id(manifest_id.as_bytes());
        spec.set_manifest_name(&manifest.name);
        spec.set_service_name(&manifest.name);

        let mut tasks_builder = spec.reborrow().init_tasks(manifest.tasks.len() as u32);
        for (idx, task) in manifest.tasks.iter().enumerate() {
            write_task(tasks_builder.reborrow().get(idx as u32), task)?;
        }
    }

    let response = deploy
        .send()
        .promise
        .await
        .context("service deployment request failed")?;
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

    println!(
        "service '{}' accepted with id {}",
        manifest.name, service_id
    );
    println!(
        "deployment is running in the background; track it with `mantissa services list` or stop it with `mantissa services stop {service_id}`"
    );

    Ok(ServiceDeploymentHandle {
        service_id,
        manifest_id,
    })
}

/// Writes a manifest task specification into the Cap'n Proto builder for submission.
fn write_task(mut builder: task_template::Builder<'_>, task: &TaskSpec) -> Result<()> {
    builder.set_name(&task.name);
    builder.set_image(&task.image);
    builder.set_replicas(task.replicas);
    builder.set_cpu_millis(task.resources.cpu_millis);
    builder.set_memory_bytes(task.resources.memory_bytes());

    let mut cmd_builder = builder.reborrow().init_command(task.command.len() as u32);
    for (idx, arg) in task.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }

    if let Some(policy) = &task.restart_policy {
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

    let mut env_builder = builder.reborrow().init_env(task.env.len() as u32);
    write_env_vars(&mut env_builder, &task.env, &task.name)?;

    let mut networks_builder = builder.reborrow().init_networks(task.networks.len() as u32);
    for (idx, network) in task.networks.iter().enumerate() {
        networks_builder.set(idx as u32, network.trim());
    }

    builder.set_health_port(task.health_port.unwrap_or(0));
    let mut health_builder = builder.reborrow().init_health_command(
        task.health_command
            .as_ref()
            .map(|cmd| cmd.len() as u32)
            .unwrap_or(0),
    );
    if let Some(cmd) = &task.health_command {
        for (idx, arg) in cmd.iter().enumerate() {
            health_builder.set(idx as u32, arg);
        }
    }

    builder.set_public_port(task.public_port.unwrap_or(0));

    let mut files_builder = builder
        .reborrow()
        .init_secret_files(task.secret_files.len() as u32);
    write_secret_files(&mut files_builder, &task.secret_files, &task.name)?;

    Ok(())
}
