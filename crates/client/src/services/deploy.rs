use super::manifest::{RestartPolicyName, ServiceManifest, TaskSpec};
use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use protocol::services::task_template;
use uuid::Uuid;

/// Identifies the asynchronous deployment issued against the cluster so callers can poll status.
#[derive(Debug, Clone)]
pub struct ServiceDeploymentHandle {
    pub service_id: Uuid,
    pub manifest_id: Uuid,
}

/// Submits a service manifest to the local coordinator, returning immediately with the service id.
pub async fn deploy_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
) -> Result<ServiceDeploymentHandle> {
    let manifest_id = Uuid::new_v4();

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
            write_task(tasks_builder.reborrow().get(idx as u32), task);
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
        "deployment is running in the background; track it with `mantissa services list` or stop it with `mantissa services stop {}`",
        service_id
    );

    Ok(ServiceDeploymentHandle {
        service_id,
        manifest_id,
    })
}

/// Writes a manifest task specification into the Cap'n Proto builder for submission.
fn write_task(mut builder: task_template::Builder<'_>, task: &TaskSpec) {
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
}
