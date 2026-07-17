use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use mantissa_protocol::topology;
use std::fmt;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const CLUSTER_OPERATION_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Parsed cluster view identifier used by client-side cluster orchestration calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ClusterViewSpec {
    pub cluster_id: Uuid,
    pub epoch: u64,
}

impl ClusterViewSpec {
    /// Encodes this view into a Cap'n Proto builder for topology RPC requests.
    pub(super) fn write_capnp(self, mut builder: topology::cluster_view_id::Builder<'_>) {
        builder
            .reborrow()
            .init_cluster_id()
            .set_value(self.cluster_id.as_bytes());
        builder.set_epoch(self.epoch);
    }

    /// Decodes one view from a topology Cap'n Proto response payload.
    pub(super) fn from_capnp(reader: topology::cluster_view_id::Reader<'_>) -> Result<Self> {
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
    pub stage: ClusterOperationStage,
    pub dry_run: bool,
    pub dependency_operation_ids: Vec<Uuid>,
    pub source_views: Vec<ClusterViewSpec>,
    pub target_views: Vec<ClusterViewSpec>,
    pub target_cluster_names: Vec<String>,
    pub split_assignments: Vec<ClusterSplitAssignment>,
    pub split_service_policy: String,
    pub split_network_policy: String,
    pub merge_service_policy: String,
    pub updated_at_unix_ms: u64,
    pub details: String,
}

/// Client-facing lifecycle stage for one durable split or merge operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClusterOperationStage {
    Proposed,
    Prepared,
    Committed,
    Finalized,
    Aborted,
}

impl ClusterOperationStage {
    /// Decodes the protocol stage without exposing generated Cap'n Proto types to callers.
    fn from_capnp(value: topology::ClusterOperationStage) -> Self {
        match value {
            topology::ClusterOperationStage::Proposed => Self::Proposed,
            topology::ClusterOperationStage::Prepared => Self::Prepared,
            topology::ClusterOperationStage::Committed => Self::Committed,
            topology::ClusterOperationStage::Finalized => Self::Finalized,
            topology::ClusterOperationStage::Aborted => Self::Aborted,
        }
    }

    /// Returns whether the operation will make no further lifecycle progress.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Finalized | Self::Aborted)
    }
}

impl fmt::Display for ClusterOperationStage {
    /// Renders the stage using the protocol's operator-facing names.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Deterministic assignment of one node to one split target.
#[derive(Clone, Debug)]
pub struct ClusterSplitAssignment {
    pub node_id: Uuid,
    pub target_index: u64,
}

impl ClusterOperationSummary {
    /// Converts a topology `ClusterOperation` reader into a client-facing summary.
    pub(super) fn from_capnp(reader: topology::cluster_operation::Reader<'_>) -> Result<Self> {
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

        let dependencies = reader
            .get_dependency_operation_ids()
            .context("operation dependencies missing")?;
        let mut dependency_operation_ids = Vec::with_capacity(dependencies.len() as usize);
        for dependency in dependencies.iter() {
            dependency_operation_ids.push(
                Uuid::from_slice(dependency.context("operation dependency missing")?)
                    .context("invalid operation dependency id bytes")?,
            );
        }

        let mut target_views = Vec::new();
        let targets = reader
            .get_target_views()
            .context("operation target views missing")?;
        for idx in 0..targets.len() {
            target_views.push(ClusterViewSpec::from_capnp(targets.get(idx))?);
        }

        let target_names = reader
            .get_target_cluster_names()
            .context("operation target cluster names missing")?;
        let mut target_cluster_names = Vec::with_capacity(target_names.len() as usize);
        for idx in 0..target_names.len() {
            target_cluster_names.push(
                target_names
                    .get(idx)
                    .context("operation target cluster name missing")?
                    .to_string()
                    .context("operation target cluster name invalid utf8")?,
            );
        }

        let assignments = reader
            .get_split_assignments()
            .context("operation split assignments missing")?;
        let mut split_assignments = Vec::with_capacity(assignments.len() as usize);
        for idx in 0..assignments.len() {
            let assignment = assignments.get(idx);
            let node_bytes = assignment
                .get_node_id()
                .context("split assignment missing node id")?
                .get_bytes()
                .context("split assignment missing node id bytes")?
                .to_vec();
            if node_bytes.len() != 16 {
                return Err(anyhow!(
                    "split assignment contained invalid node id length {}",
                    node_bytes.len()
                ));
            }
            split_assignments.push(ClusterSplitAssignment {
                node_id: Uuid::from_slice(&node_bytes)
                    .context("invalid split assignment node id bytes")?,
                target_index: assignment.get_target_index(),
            });
        }

        Ok(Self {
            id: Uuid::from_slice(&id).context("invalid operation id bytes")?,
            kind: format!(
                "{:?}",
                reader
                    .get_kind()
                    .context("operation kind missing from response")?
            ),
            stage: ClusterOperationStage::from_capnp(
                reader
                    .get_stage()
                    .context("operation stage missing from response")?,
            ),
            dry_run: reader.get_dry_run(),
            dependency_operation_ids,
            source_views,
            target_views,
            target_cluster_names,
            split_assignments,
            split_service_policy: format!(
                "{:?}",
                reader
                    .get_split_service_policy()
                    .context("operation split service policy missing from response")?
            ),
            split_network_policy: format!(
                "{:?}",
                reader
                    .get_split_network_policy()
                    .context("operation split network policy missing from response")?
            ),
            merge_service_policy: format!(
                "{:?}",
                reader
                    .get_merge_service_policy()
                    .context("operation merge service policy missing from response")?
            ),
            updated_at_unix_ms: reader.get_updated_at_unix_ms(),
            details: reader
                .get_details()
                .context("operation details missing")?
                .to_string()
                .context("operation details invalid utf8")?,
        })
    }
}

/// Fetches the latest locally known state for one cluster operation.
pub async fn get_cluster_operation(
    cfg: &ClientConfig,
    operation_id: &str,
) -> Result<ClusterOperationSummary> {
    let operation_id = Uuid::parse_str(operation_id)
        .with_context(|| format!("invalid cluster operation id: {operation_id}"))?;
    let topology = topology_capability(cfg).await?;
    get_cluster_operation_from_topology(&topology, operation_id).await
}

/// Waits until one durable operation reaches Finalized or Aborted on the local daemon.
///
/// This only controls client observation. It never times out, aborts, or otherwise mutates the
/// eventually convergent operation after submission.
pub async fn wait_for_cluster_operation(
    cfg: &ClientConfig,
    operation_id: Uuid,
) -> Result<ClusterOperationSummary> {
    let topology = topology_capability(cfg).await?;
    loop {
        let operation = get_cluster_operation_from_topology(&topology, operation_id).await?;
        if operation.dry_run || operation.stage.is_terminal() {
            return Ok(operation);
        }
        sleep(CLUSTER_OPERATION_POLL_INTERVAL).await;
    }
}

/// Fetches one operation using an already-open topology capability for efficient polling.
async fn get_cluster_operation_from_topology(
    topology: &topology::Client,
    operation_id: Uuid,
) -> Result<ClusterOperationSummary> {
    let mut request = topology.get_cluster_operation_request();
    request.get().set_id(operation_id.as_bytes());

    let response = request
        .send()
        .promise
        .await
        .context("getClusterOperation RPC failed")?;
    let op = response
        .get()
        .context("failed to read getClusterOperation response")?
        .get_op()
        .context("getClusterOperation response missing operation")?;
    ClusterOperationSummary::from_capnp(op)
}

/// Returns the topology capability from the local session for cluster orchestration RPCs.
pub(super) async fn topology_capability(cfg: &ClientConfig) -> Result<topology::Client> {
    let session = connection::get_local_session(cfg).await?;
    Ok(session
        .get_topology_request()
        .send()
        .pipeline
        .get_topology())
}

/// Parses a cluster UUID from CLI input and emits a contextual error on malformed values.
pub(super) fn parse_cluster_id(input: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(input).with_context(|| format!("invalid {field}: {input}"))
}
