use crate::config::ClientConfig;
use crate::connection;
use crate::services::deploy::ReplicaStart;
use crate::services::manifest::{ServiceManifest, TaskSpec};
use anyhow::{Context, Result};
use protocol::services::{services, task_template};
use std::collections::HashMap;
use uuid::Uuid;

pub async fn register_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
    manifest_id: Uuid,
    replicas: &[ReplicaStart],
) -> Result<()> {
    if manifest.tasks.is_empty() {
        return Ok(());
    }

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services_client: services::Client = request.send().pipeline.get_services();
    let mut upsert = services_client.upsert_request();

    {
        let mut specs_builder = upsert.get().init_specs(1);
        let mut tasks_by_task: HashMap<&str, Vec<Uuid>> = HashMap::new();
        for replica in replicas {
            let id = Uuid::parse_str(&replica.task.id)
                .with_context(|| format!("invalid task id {}", replica.task.id))?;
            tasks_by_task
                .entry(replica.task_name.as_str())
                .or_default()
                .push(id);
        }

        let mut entry = specs_builder.reborrow().get(0);
        entry.set_manifest_id(manifest_id.as_bytes());
        entry.set_manifest_name(&manifest.name);
        entry.set_service_name(&manifest.name);

        let mut tasks_builder = entry.reborrow().init_tasks(manifest.tasks.len() as u32);
        for (idx, task) in manifest.tasks.iter().enumerate() {
            write_task(tasks_builder.reborrow().get(idx as u32), task);
        }

        let mut task_ids: Vec<Uuid> = manifest
            .tasks
            .iter()
            .flat_map(|task| tasks_by_task.remove(task.name.as_str()).unwrap_or_default())
            .collect();
        for (_, ids) in tasks_by_task.into_iter() {
            task_ids.extend(ids);
        }
        let mut task_builder = entry.reborrow().init_task_ids(task_ids.len() as u32);
        for (idx, wid) in task_ids.iter().enumerate() {
            task_builder.set(idx as u32, wid.as_bytes());
        }
    }

    upsert.send().promise.await?;
    Ok(())
}

/// Return true when the local service registry already has `service_name`.
pub async fn service_exists(cfg: &ClientConfig, service_name: &str) -> Result<bool> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services_client: services::Client = request.send().pipeline.get_services();

    let response = services_client
        .list_request()
        .send()
        .promise
        .await
        .context("failed to query registered services")?;
    let reader = response.get()?;
    let specs = reader.get_services()?;

    for spec in specs.iter() {
        if spec.get_service_name()?.to_str()? == service_name {
            return Ok(true);
        }
    }

    Ok(false)
}

fn write_task(mut builder: task_template::Builder<'_>, task: &TaskSpec) {
    builder.set_name(&task.name);
    builder.set_image(&task.image);
    builder.set_replicas(task.replicas);

    let mut cmd_builder = builder.reborrow().init_command(task.command.len() as u32);
    for (idx, arg) in task.command.iter().enumerate() {
        cmd_builder.set(idx as u32, arg);
    }
}
