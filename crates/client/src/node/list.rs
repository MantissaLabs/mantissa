use crate::config::ClientConfig;
use crate::connection;
use crate::output;
use anyhow::Result;
use protocol::topology::{NodeDrainState, node_info::Reader as NodeInfo, peer::Reader as PeerInfo};
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
    writeln!(
        &mut tw,
        "ID\tHOSTNAME\tENDPOINT\tHEALTH\tSCHED\tDRAIN\tLABELS\tREASON"
    )?;

    let mut list: Vec<NodeInfo> = reader.get_nodes()?.iter().collect();
    list.sort_by_key(id_sort_key_uuid_bytes);

    for n in &list {
        let peer = n.get_peer()?;
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{:?}\t{}\t{}\t{}\t{}",
            id_string(n)?,
            peer.get_hostname()?.to_str()?,
            peer.get_address()?.to_str()?,
            n.get_health()?,
            sched_label(&peer),
            drain_label(n)?,
            labels_label(&peer)?,
            reason_label(&peer)?,
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

#[inline]
fn sched_label(peer: &PeerInfo) -> &'static str {
    if peer.get_schedulable() {
        "open"
    } else {
        "fenced"
    }
}

#[inline]
fn drain_label(n: &NodeInfo) -> anyhow::Result<&'static str> {
    Ok(match n.get_drain_state()? {
        NodeDrainState::Open | NodeDrainState::Fenced => "-",
        NodeDrainState::Draining => "draining",
        NodeDrainState::Drained => "drained",
        NodeDrainState::Blocked => "blocked",
    })
}

#[inline]
fn reason_label(peer: &PeerInfo) -> anyhow::Result<String> {
    let reason = peer.get_scheduling_reason()?.to_str()?.trim().to_string();
    if reason.is_empty() {
        Ok("-".to_string())
    } else {
        Ok(reason)
    }
}

#[inline]
fn labels_label(peer: &PeerInfo) -> anyhow::Result<String> {
    let labels = peer.get_labels()?;
    if labels.is_empty() {
        return Ok("-".to_string());
    }

    let mut out = Vec::with_capacity(labels.len() as usize);
    for label in labels.iter() {
        let text = label?.to_str()?.trim().to_string();
        if !text.is_empty() {
            out.push(text);
        }
    }

    if out.is_empty() {
        Ok("-".to_string())
    } else {
        Ok(out.join(","))
    }
}
