use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use protocol::topology;
use std::fmt;
use uuid::Uuid;

/// Parsed cluster view identifier used by client-side cluster orchestration calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ClusterViewSpec {
    pub cluster_id: Uuid,
    pub epoch: u64,
}

impl ClusterViewSpec {
    /// Encodes this view into a Cap'n Proto builder for topology RPC requests.
    pub(crate) fn write_capnp(self, mut builder: topology::cluster_view_id::Builder<'_>) {
        builder
            .reborrow()
            .init_cluster_id()
            .set_value(self.cluster_id.as_bytes());
        builder.set_epoch(self.epoch);
    }

    /// Decodes one view from a topology Cap'n Proto response payload.
    pub(crate) fn from_capnp(reader: topology::cluster_view_id::Reader<'_>) -> Result<Self> {
        let cluster_bytes = reader
            .get_cluster_id()
            .context("cluster view missing cluster id")?
            .get_value()
            .context("cluster view missing cluster id bytes")?
            .to_vec();
        if cluster_bytes.len() != 16 {
            return Err(anyhow!(
                "cluster view contained invalid cluster id length {}",
                cluster_bytes.len()
            ));
        }

        let cluster_id = Uuid::from_slice(&cluster_bytes).context("invalid cluster id bytes")?;
        Ok(Self {
            cluster_id,
            epoch: reader.get_epoch(),
        })
    }
}

impl fmt::Display for ClusterViewSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.cluster_id, self.epoch)
    }
}

/// Human-friendly summary returned for merge/split operation submissions.
#[derive(Clone, Debug)]
pub struct ClusterOperationSummary {
    pub id: Uuid,
    pub kind: String,
    pub stage: String,
    pub source_views: Vec<ClusterViewSpec>,
    pub target_views: Vec<ClusterViewSpec>,
    pub details: String,
}

impl ClusterOperationSummary {
    /// Converts a topology `ClusterOperation` reader into a client-facing summary.
    pub(crate) fn from_capnp(reader: topology::cluster_operation::Reader<'_>) -> Result<Self> {
        let id = reader.get_id().context("operation id missing")?.to_vec();
        if id.len() != 16 {
            return Err(anyhow!("operation id must be 16 bytes, got {}", id.len()));
        }

        let mut source_views = Vec::new();
        let sources = reader
            .get_source_views()
            .context("operation source views missing")?;
        for idx in 0..sources.len() {
            source_views.push(ClusterViewSpec::from_capnp(sources.get(idx))?);
        }

        let mut target_views = Vec::new();
        let targets = reader
            .get_target_views()
            .context("operation target views missing")?;
        for idx in 0..targets.len() {
            target_views.push(ClusterViewSpec::from_capnp(targets.get(idx))?);
        }

        Ok(Self {
            id: Uuid::from_slice(&id).context("invalid operation id bytes")?,
            kind: format!(
                "{:?}",
                reader
                    .get_kind()
                    .context("operation kind missing from response")?
            ),
            stage: format!(
                "{:?}",
                reader
                    .get_stage()
                    .context("operation stage missing from response")?
            ),
            source_views,
            target_views,
            details: reader
                .get_details()
                .context("operation details missing")?
                .to_string()
                .context("operation details invalid utf8")?,
        })
    }
}

/// Returns the topology capability from the local session for cluster orchestration RPCs.
pub(crate) async fn topology_capability(cfg: &ClientConfig) -> Result<topology::Client> {
    let session = connection::get_local_session(cfg).await?;
    Ok(session
        .get_topology_request()
        .send()
        .pipeline
        .get_topology())
}

/// Parses a cluster UUID from CLI input and emits a contextual error on malformed values.
pub(crate) fn parse_cluster_id(input: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(input).with_context(|| format!("invalid {field}: {input}"))
}
