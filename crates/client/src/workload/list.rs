use crate::config::ClientConfig;
use crate::connection;
use crate::workload::{uuid_short, uuid_to_string};
use anyhow::Result;
use capnp::Error as CapnpError;
use protocol::workload::workload_spec;
use std::io::Write;
use tabwriter::TabWriter;

pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_workload_request();
    let workload = request.send().pipeline.get_workload();
    let request = workload.list_request();

    let response = request.send().promise.await?;
    let workloads = response.get()?.get_workloads()?;

    let mut specs: Vec<WorkloadRow> = Vec::new();
    for spec in workloads.iter() {
        specs.push(WorkloadRow::from_reader(spec)?);
    }

    specs.sort_by(|a, b| a.name.cmp(&b.name));

    if specs.is_empty() {
        println!("no workloads found");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tNAME\tIMAGE\tCOMMAND\tNODE\tSTATUS\tCREATED")?;

    for spec in specs {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            spec.id, spec.name, spec.image, spec.command, spec.node, spec.state, spec.created_at,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    println!("{output}");

    Ok(())
}

struct WorkloadRow {
    id: String,
    name: String,
    image: String,
    command: String,
    node: String,
    state: String,
    created_at: String,
}

impl WorkloadRow {
    fn from_reader(spec: workload_spec::Reader) -> Result<Self, CapnpError> {
        let id = uuid_to_string(spec.get_id()?)?;
        let name = spec.get_name()?.to_str()?.to_string();
        let image = spec.get_image()?.to_str()?.to_string();
        let state = spec.get_state()?.to_str()?.to_string();
        let created_at = spec.get_created_at()?.to_str()?.to_string();
        let node_name = spec.get_node_name()?.to_str()?.to_string();
        let node_id = uuid_short(spec.get_node_id()?)?;

        let mut command = Vec::new();
        for arg in spec.get_command()?.iter() {
            command.push(arg?.to_str()?.to_string());
        }

        let node = if node_name.is_empty() {
            node_id
        } else {
            format!("{node_name} ({node_id})")
        };

        Ok(Self {
            id,
            name,
            image,
            command: if command.is_empty() {
                "-".to_string()
            } else {
                command.join(" ")
            },
            node,
            state,
            created_at,
        })
    }
}
