use crate::config::ClientConfig;
use crate::connection;
use anyhow::Result;
use mantissa_protocol::topology::{
    NodeDrainState, NodeReadinessState, node_info::Reader as NodeInfo, peer::Reader as PeerInfo,
};
use uuid::Uuid;

/// One node entry returned by the topology list RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeListEntry {
    pub id: Uuid,
    pub hostname: String,
    pub endpoint: String,
    pub health: String,
    pub readiness: NodeReadinessState,
    pub schedulable: bool,
    pub drain_state: NodeDrainState,
    pub labels: Vec<String>,
    pub scheduling_reason: Option<String>,
}

impl NodeListEntry {
    /// Decodes one topology node payload into an owned client result.
    fn from_reader(reader: NodeInfo) -> Result<Self> {
        let peer = reader.get_peer()?;
        Ok(Self {
            id: id_from_node(&reader)?,
            hostname: peer.get_hostname()?.to_str()?.to_string(),
            endpoint: peer.get_address()?.to_str()?.to_string(),
            health: format!("{:?}", reader.get_health()?),
            readiness: reader.get_readiness_state()?,
            schedulable: peer.get_schedulable(),
            drain_state: reader.get_drain_state()?,
            labels: labels_from_peer(&peer)?,
            scheduling_reason: optional_peer_reason(&peer)?,
        })
    }
}

/// Loads the current topology node list from the local coordinator.
pub async fn list(cfg: &ClientConfig) -> Result<Vec<NodeListEntry>> {
    let client = connection::get_local_session(cfg).await?;

    let request = client.get_topology_request();
    let topology = request.send().pipeline.get_topology();
    let request = topology.list_request();

    let response = request.send().promise.await?;
    let reader = response.get()?.get_nodes()?;
    let nodes = reader.get_nodes()?;

    let mut entries = Vec::with_capacity(nodes.len() as usize);
    for node in nodes.iter() {
        entries.push(NodeListEntry::from_reader(node)?);
    }

    Ok(entries)
}

/// Decodes one node UUID from the topology payload.
fn id_from_node(node: &NodeInfo) -> Result<Uuid> {
    let bytes = node.get_id()?.get_bytes()?;
    Uuid::from_slice(bytes).map_err(|err| anyhow::anyhow!(err.to_string()))
}

/// Decodes peer labels into an owned stable vector.
fn labels_from_peer(peer: &PeerInfo) -> Result<Vec<String>> {
    let labels = peer.get_labels()?;
    let mut out = Vec::with_capacity(labels.len() as usize);
    for label in labels.iter() {
        let text = label?.to_str()?.trim().to_string();
        if !text.is_empty() {
            out.push(text);
        }
    }
    Ok(out)
}

/// Decodes one optional scheduling reason from the peer payload.
fn optional_peer_reason(peer: &PeerInfo) -> Result<Option<String>> {
    let reason = peer.get_scheduling_reason()?.to_str()?.trim().to_string();
    Ok((!reason.is_empty()).then_some(reason))
}
