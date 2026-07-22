use crate::config::ClientConfig;
use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use super::list::{list_cluster_views, resolve_view_from_summaries};
use super::operations::{
    ClusterOperationSummary, ClusterViewSpec, parse_cluster_id, topology_capability,
};

/// Merge-time service behavior policy exposed by the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeServicePolicy {
    /// Trigger service reconciliation after merge so replicas can rebalance cluster-wide.
    Rebalance,
    /// Preserve current service placement without extra merge-time reconciliation hints.
    Preserve,
}

impl MergeServicePolicy {
    /// Encodes this policy into the topology RPC enum.
    fn to_capnp(self) -> mantissa_protocol::topology::MergeServicePolicy {
        match self {
            Self::Rebalance => mantissa_protocol::topology::MergeServicePolicy::Rebalance,
            Self::Preserve => mantissa_protocol::topology::MergeServicePolicy::Preserve,
        }
    }
}

/// Submits a merge using cluster lineage ids and an optional list of required operations.
///
/// Pass an empty dependency slice for an independent merge. When this merge consumes clusters
/// that earlier operations are still changing, pass those operation ids so every node applies
/// the earlier changes before this merge.
pub async fn merge_by_cluster_id(
    cfg: &ClientConfig,
    source_cluster_id: &str,
    destination_cluster_id: &str,
    dry_run: bool,
    service_policy: MergeServicePolicy,
    dependency_operation_ids: &[Uuid],
) -> Result<ClusterOperationSummary> {
    let source_cluster = parse_cluster_id(source_cluster_id, "source cluster id")?;
    let destination_cluster = parse_cluster_id(destination_cluster_id, "destination cluster id")?;
    if source_cluster == destination_cluster {
        return Err(anyhow!(
            "merge requires two different cluster ids; both were {source_cluster}"
        ));
    }

    let summaries = list_cluster_views(cfg).await?;
    let source_view = resolve_view_from_summaries(&summaries, source_cluster)?;
    let destination_view = resolve_view_from_summaries(&summaries, destination_cluster)?;
    submit_merge_request(
        cfg,
        source_view,
        destination_view,
        dry_run,
        service_policy,
        dependency_operation_ids,
    )
    .await
}

/// Sends a merge request with the earlier operations that must finish before it can start.
async fn submit_merge_request(
    cfg: &ClientConfig,
    source_view: ClusterViewSpec,
    destination_view: ClusterViewSpec,
    dry_run: bool,
    service_policy: MergeServicePolicy,
    dependency_operation_ids: &[Uuid],
) -> Result<ClusterOperationSummary> {
    let topology = topology_capability(cfg).await?;
    let mut request = topology.merge_clusters_request();
    {
        let mut req = request.get().init_req();
        req.set_operation_id(Uuid::new_v4().as_bytes());
        let mut dependencies = req
            .reborrow()
            .init_dependency_operation_ids(dependency_operation_ids.len() as u32);
        for (index, operation_id) in dependency_operation_ids.iter().enumerate() {
            dependencies.set(index as u32, operation_id.as_bytes());
        }
        source_view.write_capnp(req.reborrow().init_source_view());
        destination_view.write_capnp(req.reborrow().init_destination_view());
        req.set_dry_run(dry_run);
        req.set_service_policy(service_policy.to_capnp());
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
