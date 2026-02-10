use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use protocol::topology;
use std::fmt;
use uuid::Uuid;

/// Parsed cluster view selector used by CLI merge/split commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClusterViewSpec {
    pub cluster_id: Uuid,
    pub epoch: u64,
}

impl ClusterViewSpec {
    /// Parses a textual cluster view in the form `CLUSTER_UUID@EPOCH` or `CLUSTER_UUID`.
    pub fn parse(input: &str) -> Result<Self> {
        let (cluster_raw, epoch_raw) = match input.split_once('@') {
            Some((cluster, epoch)) => (cluster, Some(epoch)),
            None => (input, None),
        };

        let cluster_id =
            Uuid::parse_str(cluster_raw).with_context(|| format!("invalid cluster id: {input}"))?;
        let epoch = match epoch_raw {
            Some(raw) => raw
                .parse::<u64>()
                .with_context(|| format!("invalid cluster epoch in view: {input}"))?,
            None => 0,
        };

        Ok(Self { cluster_id, epoch })
    }

    /// Encodes this view spec into a Cap'n Proto cluster view builder.
    fn write_capnp(self, mut builder: topology::cluster_view_id::Builder<'_>) {
        builder
            .reborrow()
            .init_cluster_id()
            .set_value(self.cluster_id.as_bytes());
        builder.set_epoch(self.epoch);
    }

    /// Decodes this view spec from a Cap'n Proto cluster view reader.
    fn from_capnp(reader: topology::cluster_view_id::Reader<'_>) -> Result<Self> {
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
    fn from_capnp(reader: topology::cluster_operation::Reader<'_>) -> Result<Self> {
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

/// Submits a merge operation request to the local topology API.
pub async fn merge(
    cfg: &ClientConfig,
    source_view: &str,
    destination_view: &str,
    dry_run: bool,
) -> Result<ClusterOperationSummary> {
    let source = ClusterViewSpec::parse(source_view)?;
    let destination = ClusterViewSpec::parse(destination_view)?;

    let session = connection::get_local_session(cfg).await?;
    let topology_cap = session
        .get_topology_request()
        .send()
        .pipeline
        .get_topology();
    let mut request = topology_cap.merge_clusters_request();
    {
        let mut req = request.get().init_req();
        source.write_capnp(req.reborrow().init_source_view());
        destination.write_capnp(req.reborrow().init_destination_view());
        req.set_dry_run(dry_run);
    }

    let response = request
        .send()
        .promise
        .await
        .context("mergeClusters RPC failed")?;
    let op = response
        .get()
        .context("failed to read mergeClusters response")?
        .get_op()
        .context("mergeClusters response missing operation")?;
    ClusterOperationSummary::from_capnp(op)
}

/// Submits a split operation request to the local topology API.
pub async fn split(
    cfg: &ClientConfig,
    source_view: &str,
    targets: &[String],
    dry_run: bool,
) -> Result<ClusterOperationSummary> {
    if targets.is_empty() {
        return Err(anyhow!("split requires at least one target name"));
    }

    let source = ClusterViewSpec::parse(source_view)?;

    let session = connection::get_local_session(cfg).await?;
    let topology_cap = session
        .get_topology_request()
        .send()
        .pipeline
        .get_topology();
    let mut request = topology_cap.split_cluster_request();
    {
        let mut req = request.get().init_req();
        source.write_capnp(req.reborrow().init_source_view());

        let mut target_list = req.reborrow().init_targets(targets.len() as u32);
        for (idx, name) in targets.iter().enumerate() {
            let mut target = target_list.reborrow().get(idx as u32);
            target.set_name(name);
            let mut selector = target.reborrow().init_selector();
            selector.reborrow().init_clauses(0);
            selector.reborrow().init_explicit_nodes(0);
        }
        req.set_dry_run(dry_run);
    }

    let response = request
        .send()
        .promise
        .await
        .context("splitCluster RPC failed")?;
    let op = response
        .get()
        .context("failed to read splitCluster response")?
        .get_op()
        .context("splitCluster response missing operation")?;
    ClusterOperationSummary::from_capnp(op)
}
