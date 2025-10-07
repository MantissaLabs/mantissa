use super::manifest::ServiceManifest;
use super::run::{StartedTask, TaskStartParams, run_many};
use super::state::{register_manifest, service_exists};
use crate::config::ClientConfig;
use crate::tasks;
use anyhow::{Context, Result, anyhow};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ReplicaStart {
    pub task_name: String,
    pub replica_number: u16,
    pub task: StartedTask,
}

pub async fn deploy_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
) -> Result<Vec<ReplicaStart>> {
    if service_exists(cfg, &manifest.name)
        .await
        .context("failed to check existing services")?
    {
        return Err(anyhow!(
            "service '{}' already exists; stop it before deploying again",
            manifest.name
        ));
    }

    let mut requests = Vec::new();
    let mut layout = Vec::new();

    for task in &manifest.tasks {
        let base_name = format!("{}-{}", manifest.name, task.name);

        for replica_idx in 0..task.replicas {
            let replica_number = replica_idx + 1;
            let task_name = if task.replicas > 1 {
                format!("{base_name}-{replica_number}")
            } else {
                base_name.clone()
            };

            requests.push(TaskStartParams {
                name: task_name,
                image: task.image.clone(),
                command: task.command.clone(),
                cpu_millis: task.resources.cpu_millis,
                memory_bytes: task.resources.memory_bytes(),
            });
            layout.push((task.name.clone(), replica_number));
        }
    }

    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let tasks = run_many(cfg, requests)
        .await
        .context("failed to start service replicas")?;

    if tasks.len() != layout.len() {
        return Err(anyhow!(
            "task batch returned {} replicas but {} were requested",
            tasks.len(),
            layout.len()
        ));
    }

    let replicas: Vec<ReplicaStart> = tasks
        .into_iter()
        .zip(layout.into_iter())
        .map(|(task, (task_name, replica_number))| ReplicaStart {
            task_name,
            replica_number,
            task,
        })
        .collect();

    let manifest_id = Uuid::new_v4();
    if let Err(err) = register_manifest(cfg, manifest, manifest_id, &replicas).await {
        for replica in &replicas {
            if let Err(stop_err) = tasks::stop(cfg, &replica.task.id).await {
                eprintln!(
                    "failed to stop task {} after service deployment error: {stop_err}",
                    replica.task.id
                );
            }
        }
        return Err(err.context("failed to register service manifest"));
    }

    Ok(replicas)
}

pub fn render_summary(manifest: &ServiceManifest, replicas: &[ReplicaStart]) -> Result<String> {
    if replicas.is_empty() {
        return Ok("no tasks started".to_string());
    }

    let mut rows: Vec<&ReplicaStart> = replicas.iter().collect();
    rows.sort_by(|a, b| {
        a.task_name
            .cmp(&b.task_name)
            .then_with(|| a.replica_number.cmp(&b.replica_number))
            .then_with(|| a.task.name.cmp(&b.task.name))
    });

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SERVICE\tTASK\tREPLICA\tNAME\tID\tIMAGE\tCOMMAND\tNODE\tSTATUS"
    )?;

    for row in rows {
        let command = if row.task.command.is_empty() {
            "-".to_string()
        } else {
            row.task.command.join(" ")
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            manifest.name,
            row.task_name,
            row.replica_number,
            row.task.name,
            row.task.id,
            row.task.image,
            command,
            row.task.node,
            row.task.state,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;

    let mut summary = String::new();
    summary.push_str(&format!("service '{}' deployed\n", manifest.name));
    summary.push_str(&output);

    Ok(summary)
}
