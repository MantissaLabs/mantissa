use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::Result;
use protocol::topology::node_info::Reader as NodeInfo;
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

pub async fn list(cfg: &ClientConfig) -> Result<()> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.list_request();

    let response = request.send().promise.await?;

    let reader = response.get()?.get_nodes()?;
    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "ID\tHOSTNAME\tENDPOINT\tSTATUS")?;

    let mut list: Vec<NodeInfo> = reader.get_nodes()?.iter().collect();
    list.sort_by_key(id_sort_key_uuid_bytes);

    for n in &list {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{:?}",
            id_string(n)?,
            n.get_hostname()?.to_str()?,
            n.get_addr()?.to_str()?,
            n.get_health()?,
        )?;
    }

    tw.flush()?;
    let output = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(output);

    Ok(())
}

#[inline]
fn id_sort_key_uuid_bytes(n: &NodeInfo) -> u128 {
    match n
        .get_id()
        .and_then(|id| id.get_bytes())
        .ok()
        .and_then(|b| Uuid::from_slice(b).ok())
    {
        Some(u) => u128::from_be_bytes(*u.as_bytes()),
        None => u128::MAX,
    }
}

#[inline]
fn id_string(n: &NodeInfo) -> anyhow::Result<String> {
    let bytes = n.get_id()?.get_bytes()?;
    let u = Uuid::from_slice(bytes).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(u.to_string())
}
