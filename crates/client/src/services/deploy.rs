use super::manifest::ServiceManifest;
use super::run::{StartedWorkload, WorkloadStartParams, run_many};
use crate::config::ClientConfig;
use anyhow::{Context, Result, anyhow};
use std::io::Write;
use tabwriter::TabWriter;

#[derive(Debug, Clone)]
pub struct ReplicaStart {
    pub service_name: String,
    pub replica_number: u16,
    pub workload: StartedWorkload,
}

pub async fn deploy_manifest(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
) -> Result<Vec<ReplicaStart>> {
    let mut requests = Vec::new();
    let mut layout = Vec::new();

    for service in &manifest.services {
        let base_name = if let Some(prefix) = manifest.name.as_ref() {
            format!("{prefix}-{}", service.name)
        } else {
            service.name.clone()
        };

        for replica_idx in 0..service.replicas {
            let replica_number = replica_idx + 1;
            let workload_name = if service.replicas > 1 {
                format!("{base_name}-{replica_number}")
            } else {
                base_name.clone()
            };

            requests.push(WorkloadStartParams {
                name: workload_name,
                image: service.image.clone(),
                command: service.command.clone(),
                cpu_millis: 0,
                memory_bytes: 0,
            });
            layout.push((service.name.clone(), replica_number));
        }
    }

    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let workloads = run_many(cfg, requests)
        .await
        .context("failed to start service replicas")?;

    if workloads.len() != layout.len() {
        return Err(anyhow!(
            "workload batch returned {} replicas but {} were requested",
            workloads.len(),
            layout.len()
        ));
    }

    let replicas = workloads
        .into_iter()
        .zip(layout.into_iter())
        .map(|(workload, (service_name, replica_number))| ReplicaStart {
            service_name,
            replica_number,
            workload,
        })
        .collect();

    Ok(replicas)
}

pub fn render_summary(manifest: &ServiceManifest, replicas: &[ReplicaStart]) -> Result<String> {
    if replicas.is_empty() {
        return Ok("no workloads started".to_string());
    }

    let mut rows: Vec<&ReplicaStart> = replicas.iter().collect();
    rows.sort_by(|a, b| {
        a.service_name
            .cmp(&b.service_name)
            .then_with(|| a.replica_number.cmp(&b.replica_number))
            .then_with(|| a.workload.name.cmp(&b.workload.name))
    });

    let mut tw = TabWriter::new(Vec::new());
    writeln!(
        &mut tw,
        "SERVICE\tREPLICA\tWORKLOAD\tID\tIMAGE\tCOMMAND\tNODE\tSTATUS"
    )?;

    for row in rows {
        let command = if row.workload.command.is_empty() {
            "-".to_string()
        } else {
            row.workload.command.join(" ")
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.service_name,
            row.replica_number,
            row.workload.name,
            row.workload.id,
            row.workload.image,
            command,
            row.workload.node,
            row.workload.state,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;

    let mut summary = String::new();
    if let Some(name) = manifest.name.as_ref() {
        summary.push_str(&format!("manifest '{name}' deployed\n"));
    } else {
        summary.push_str("service manifest deployed\n");
    }
    summary.push_str(&output);

    Ok(summary)
}
