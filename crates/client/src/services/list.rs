use crate::config::ClientConfig;
use crate::connection;
use crate::tasks::uuid_to_string;
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::services::{ServiceStatus as ProtoServiceStatus, service_spec, task_template};
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;
    let request = client.get_services_request();
    let services = request.send().pipeline.get_services();

    let response = services.list_request().send().promise.await?;
    let reader = response.get()?;
    let specs = reader.get_services()?;

    let mut rows = Vec::with_capacity(specs.len() as usize);
    for spec in specs.iter() {
        rows.push(ServiceRow::from_reader(spec)?);
    }

    if rows.is_empty() {
        println!("no services registered");
        return Ok(());
    }

    rows.sort_by(|a, b| a.service_name.cmp(&b.service_name));

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "SERVICE\tSTATUS\tTASKS\tTASK IDS\tUPDATED\tID")?;

    for row in rows {
        let tasks_summary = if row.tasks.is_empty() {
            "-".to_string()
        } else {
            row.tasks
                .iter()
                .map(|task| format!("{} ({}x)", task.name, task.replicas))
                .collect::<Vec<_>>()
                .join(", ")
        };

        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}",
            row.service_name,
            row.status.to_string(),
            tasks_summary,
            row.task_ids.len(),
            row.updated_at,
            row.id,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    println!("{output}");

    Ok(())
}

#[derive(Clone, Debug)]
pub struct ServiceRow {
    pub id: String,
    pub service_name: String,
    pub tasks: Vec<ServiceTaskRow>,
    pub updated_at: String,
    pub task_ids: Vec<Uuid>,
    pub status: ServiceStatusRow,
}

impl ServiceRow {
    pub fn from_reader(spec: service_spec::Reader<'_>) -> Result<Self, CapnpError> {
        let id = uuid_to_string(spec.get_id()?)?;
        let service_name = spec.get_service_name()?.to_str()?.to_string();

        let mut tasks = Vec::new();
        for tmpl in spec.get_tasks()?.iter() {
            tasks.push(ServiceTaskRow::from_reader(tmpl)?);
        }

        let mut task_ids = Vec::new();
        for wid in spec.get_task_ids()?.iter() {
            let data = wid?.to_owned();
            if data.len() != 16 {
                return Err(CapnpError::failed("invalid task uuid length".to_string()));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&data);
            task_ids.push(Uuid::from_bytes(bytes));
        }

        Ok(Self {
            id,
            service_name,
            tasks,
            updated_at: spec.get_updated_at()?.to_str()?.to_string(),
            task_ids,
            status: ServiceStatusRow::from_proto(spec.get_status()?),
        })
    }
}

#[derive(Clone, Debug)]
pub struct ServiceTaskRow {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub replicas: u16,
}

impl ServiceTaskRow {
    fn from_reader(reader: task_template::Reader<'_>) -> Result<Self, CapnpError> {
        let mut command = Vec::new();
        for arg in reader.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        Ok(Self {
            name: reader.get_name()?.to_str()?.to_string(),
            image: reader.get_image()?.to_str()?.to_string(),
            command,
            replicas: reader.get_replicas(),
        })
    }
}

#[derive(Clone, Debug)]
pub enum ServiceStatusRow {
    Deploying,
    Running,
    Stopping,
    Stopped,
}

impl ServiceStatusRow {
    fn from_proto(status: ProtoServiceStatus) -> Self {
        match status {
            ProtoServiceStatus::Deploying => Self::Deploying,
            ProtoServiceStatus::Running => Self::Running,
            ProtoServiceStatus::Stopping => Self::Stopping,
            ProtoServiceStatus::Stopped => Self::Stopped,
        }
    }
}

impl std::fmt::Display for ServiceStatusRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            ServiceStatusRow::Deploying => "deploying",
            ServiceStatusRow::Running => "running",
            ServiceStatusRow::Stopping => "stopping",
            ServiceStatusRow::Stopped => "stopped",
        };
        write!(f, "{}", label)
    }
}
