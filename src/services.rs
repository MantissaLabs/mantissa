use crate::service_manifest::{ServiceManifest, ServiceSpec};
use anyhow::{Context, Result};
use client::config::ClientConfig;
use client::services::{StartedWorkload, run};
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
    let mut replicas = Vec::new();

    for service in &manifest.services {
        replicas.extend(start_service(cfg, manifest, service).await?);
    }

    Ok(replicas)
}

async fn start_service(
    cfg: &ClientConfig,
    manifest: &ServiceManifest,
    service: &ServiceSpec,
) -> Result<Vec<ReplicaStart>> {
    let mut replicas = Vec::with_capacity(service.replicas as usize);
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

        let workload = run(cfg, &workload_name, &service.image, &service.command)
            .await
            .with_context(|| {
                format!(
                    "failed to start replica {replica_number} of service '{}'",
                    service.name
                )
            })?;

        replicas.push(ReplicaStart {
            service_name: service.name.clone(),
            replica_number,
            workload,
        });
    }

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
