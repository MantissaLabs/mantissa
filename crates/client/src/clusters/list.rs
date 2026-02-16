use crate::config::ClientConfig;
use crate::output;
use anyhow::{Context, Result, anyhow};
use std::collections::BTreeMap;
use std::io::Write;
use tabwriter::TabWriter;
use uuid::Uuid;

use super::operations::{ClusterViewSpec, parse_cluster_id, topology_capability};

/// One row returned by the cluster listing API.
#[derive(Clone, Debug)]
pub struct ClusterViewSummary {
    pub view: ClusterViewSpec,
    pub node_count: u32,
    pub local_active: bool,
}

/// Cluster lineage summary exposed by the CLI.
#[derive(Clone, Debug)]
pub struct ClusterSummary {
    pub cluster_id: Uuid,
    pub epoch: u64,
    pub node_count: u32,
    pub local_active: bool,
}

/// Node candidate row returned for interactive split planning.
#[derive(Clone, Debug)]
pub struct SplitCandidate {
    pub node_id: Uuid,
    pub hostname: String,
    pub address: String,
    pub health: String,
    pub active_view: ClusterViewSpec,
    pub cpu_vendor: Option<String>,
    pub cpu_brand: Option<String>,
    pub cpu_logical: Option<u64>,
    pub cpu_cores: Option<u64>,
    pub memory_total_kb: Option<u64>,
    pub gpu_vendor: Option<String>,
    pub gpu_count: Option<u64>,
    pub gpu_models: Vec<String>,
    pub wireguard_enabled: bool,
}

/// Snapshot used by interactive split planners to display candidate nodes.
#[derive(Clone, Debug)]
pub struct SplitCandidateList {
    pub source_view: ClusterViewSpec,
    pub candidates: Vec<SplitCandidate>,
}

/// Picks the most recent known view for a given cluster lineage identifier.
pub(crate) fn resolve_view_from_summaries(
    summaries: &[ClusterViewSummary],
    cluster_id: Uuid,
) -> Result<ClusterViewSpec> {
    summaries
        .iter()
        .filter(|summary| summary.view.cluster_id == cluster_id)
        .max_by_key(|summary| {
            (
                if summary.local_active { 1u8 } else { 0u8 },
                summary.node_count,
                summary.view.epoch,
            )
        })
        .map(|summary| summary.view)
        .ok_or_else(|| anyhow!("cluster {cluster_id} is not known on this node"))
}

/// Aggregates view rows into one deterministic summary per cluster lineage id.
fn aggregate_cluster_summaries(view_rows: &[ClusterViewSummary]) -> Vec<ClusterSummary> {
    let mut grouped = BTreeMap::<Uuid, Vec<&ClusterViewSummary>>::new();
    for row in view_rows {
        grouped.entry(row.view.cluster_id).or_default().push(row);
    }

    let mut clusters = Vec::with_capacity(grouped.len());
    for (cluster_id, rows) in grouped {
        let local_active = rows.iter().any(|row| row.local_active);
        let selected = rows
            .iter()
            .copied()
            .max_by_key(|row| {
                (
                    if row.local_active { 1u8 } else { 0u8 },
                    row.node_count,
                    row.view.epoch,
                )
            })
            .expect("grouped rows are never empty");

        clusters.push(ClusterSummary {
            cluster_id,
            epoch: selected.view.epoch,
            node_count: selected.node_count,
            local_active,
        });
    }

    clusters
}

/// Queries the local node for all known cluster views and member counts.
pub async fn list_cluster_views(cfg: &ClientConfig) -> Result<Vec<ClusterViewSummary>> {
    let topology = topology_capability(cfg).await?;
    let response = topology
        .list_cluster_views_request()
        .send()
        .promise
        .await
        .context("listClusterViews RPC failed")?;
    let rows = response
        .get()
        .context("failed to read listClusterViews response")?
        .get_views()
        .context("listClusterViews response missing views")?;

    let mut out = Vec::with_capacity(rows.len() as usize);
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        out.push(ClusterViewSummary {
            view: ClusterViewSpec::from_capnp(row.get_view()?)?,
            node_count: row.get_node_count(),
            local_active: row.get_local_active(),
        });
    }

    out.sort_by(|left, right| {
        left.view
            .cluster_id
            .cmp(&right.view.cluster_id)
            .then(left.view.epoch.cmp(&right.view.epoch))
    });
    Ok(out)
}

/// Queries the local node for cluster lineages and renders a concise table for CLI output.
pub async fn list_clusters(cfg: &ClientConfig) -> Result<()> {
    let views = list_cluster_views(cfg).await?;
    let summaries = aggregate_cluster_summaries(&views);
    if summaries.is_empty() {
        output::emit_line("no clusters known");
        return Ok(());
    }

    let mut tw = TabWriter::new(Vec::new());
    writeln!(&mut tw, "CLUSTER_ID\tEPOCH\tNODES\tACTIVE_ON_THIS_NODE")?;
    for summary in summaries {
        writeln!(
            &mut tw,
            "{}\t{}\t{}\t{}",
            summary.cluster_id,
            summary.epoch,
            summary.node_count,
            if summary.local_active { "yes" } else { "no" }
        )?;
    }

    tw.flush()?;
    let rendered = String::from_utf8(tw.into_inner()?)?;
    output::emit_block(rendered);
    Ok(())
}

/// Returns the currently active cluster view on the local node.
pub async fn active_cluster_view(cfg: &ClientConfig) -> Result<ClusterViewSpec> {
    let topology = topology_capability(cfg).await?;
    let response = topology
        .get_cluster_view_request()
        .send()
        .promise
        .await
        .context("getClusterView RPC failed")?;
    let view = response
        .get()
        .context("failed to read getClusterView response")?
        .get_view()
        .context("getClusterView response missing view")?;
    ClusterViewSpec::from_capnp(view)
}

/// Resolves a cluster lineage id into the latest known active cluster view.
pub async fn resolve_cluster_view_by_cluster_id(
    cfg: &ClientConfig,
    cluster_id: Uuid,
) -> Result<ClusterViewSpec> {
    let summaries = list_cluster_views(cfg).await?;
    resolve_view_from_summaries(&summaries, cluster_id)
}

/// Resolves the split source view from either an explicit cluster id or the local active view.
async fn resolve_source_view(
    cfg: &ClientConfig,
    source_cluster_id: Option<&str>,
) -> Result<ClusterViewSpec> {
    match source_cluster_id {
        Some(cluster_id) => {
            let cluster_id = parse_cluster_id(cluster_id, "cluster id")?;
            resolve_cluster_view_by_cluster_id(cfg, cluster_id).await
        }
        None => active_cluster_view(cfg).await,
    }
}

/// Converts empty strings returned by Cap'n Proto into `None` for optional display fields.
fn text_or_none(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Lists split candidates for one source cluster so interactive UIs can present rich node details.
pub async fn list_split_candidates(
    cfg: &ClientConfig,
    source_cluster_id: Option<&str>,
) -> Result<SplitCandidateList> {
    let source_view = resolve_source_view(cfg, source_cluster_id).await?;
    let topology = topology_capability(cfg).await?;
    let mut request = topology.list_split_candidates_request();
    source_view.write_capnp(request.get().init_source_view());

    let response = request
        .send()
        .promise
        .await
        .context("listSplitCandidates RPC failed")?;
    let rows = response
        .get()
        .context("failed to read listSplitCandidates response")?
        .get_nodes()
        .context("listSplitCandidates response missing nodes")?;

    let mut candidates = Vec::with_capacity(rows.len() as usize);
    for idx in 0..rows.len() {
        let row = rows.get(idx);
        let node_bytes = row
            .get_node_id()
            .context("split candidate missing node id")?
            .get_bytes()
            .context("split candidate missing node id bytes")?
            .to_vec();
        if node_bytes.len() != 16 {
            return Err(anyhow!(
                "split candidate contained invalid node id length {}",
                node_bytes.len()
            ));
        }

        let node_id = Uuid::from_slice(&node_bytes).context("invalid split candidate node id")?;
        let hostname = row
            .get_hostname()
            .context("split candidate missing hostname")?
            .to_string()
            .context("split candidate hostname invalid utf8")?;
        let address = row
            .get_addr()
            .context("split candidate missing address")?
            .to_string()
            .context("split candidate address invalid utf8")?;
        let health = format!(
            "{:?}",
            row.get_health().context("split candidate missing health")?
        );
        let active_view = ClusterViewSpec::from_capnp(
            row.get_active_cluster_view()
                .context("split candidate missing active cluster view")?,
        )?;

        let cpu_vendor = text_or_none(
            row.get_cpu_vendor()
                .context("split candidate missing cpu vendor")?
                .to_string()
                .context("split candidate cpu vendor invalid utf8")?,
        );
        let cpu_brand = text_or_none(
            row.get_cpu_brand()
                .context("split candidate missing cpu brand")?
                .to_string()
                .context("split candidate cpu brand invalid utf8")?,
        );
        let gpu_vendor = text_or_none(
            row.get_gpu_vendor()
                .context("split candidate missing gpu vendor")?
                .to_string()
                .context("split candidate gpu vendor invalid utf8")?,
        );

        let gpu_models_reader = row
            .get_gpu_models()
            .context("split candidate missing gpu models")?;
        let mut gpu_models = Vec::with_capacity(gpu_models_reader.len() as usize);
        for model_idx in 0..gpu_models_reader.len() {
            let model = gpu_models_reader
                .get(model_idx)
                .context("split candidate gpu model missing")?
                .to_string()
                .context("split candidate gpu model invalid utf8")?;
            if !model.trim().is_empty() {
                gpu_models.push(model);
            }
        }

        candidates.push(SplitCandidate {
            node_id,
            hostname,
            address,
            health,
            active_view,
            cpu_vendor,
            cpu_brand,
            cpu_logical: {
                let value = row.get_cpu_logical();
                if value == 0 { None } else { Some(value) }
            },
            cpu_cores: {
                let value = row.get_cpu_cores();
                if value == 0 { None } else { Some(value) }
            },
            memory_total_kb: {
                let value = row.get_memory_total_kb();
                if value == 0 { None } else { Some(value) }
            },
            gpu_vendor,
            gpu_count: {
                let value = row.get_gpu_count();
                if value == 0 { None } else { Some(value) }
            },
            gpu_models,
            wireguard_enabled: row.get_wireguard_enabled(),
        });
    }

    candidates.sort_by(|left, right| {
        left.hostname
            .cmp(&right.hostname)
            .then(left.node_id.cmp(&right.node_id))
    });

    Ok(SplitCandidateList {
        source_view,
        candidates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds one synthetic view row for deterministic resolver and aggregation tests.
    fn view_row(
        cluster_id: Uuid,
        epoch: u64,
        node_count: u32,
        local_active: bool,
    ) -> ClusterViewSummary {
        ClusterViewSummary {
            view: ClusterViewSpec { cluster_id, epoch },
            node_count,
            local_active,
        }
    }

    #[test]
    fn resolve_view_prefers_local_active_row() {
        let cluster = Uuid::from_u128(0xA0);
        let rows = vec![
            view_row(cluster, 4, 5, false),
            view_row(cluster, 3, 0, true),
            view_row(cluster, 2, 1, false),
        ];

        let resolved = resolve_view_from_summaries(&rows, cluster).expect("resolve cluster view");
        assert_eq!(
            resolved.epoch, 3,
            "resolver should choose the local-active row for this lineage"
        );
    }

    #[test]
    fn resolve_view_prefers_populated_rows_over_empty_future_rows() {
        let cluster = Uuid::from_u128(0xB0);
        let rows = vec![
            view_row(cluster, 10, 0, false),
            view_row(cluster, 8, 4, false),
        ];

        let resolved = resolve_view_from_summaries(&rows, cluster).expect("resolve cluster view");
        assert_eq!(
            resolved.epoch, 8,
            "resolver should avoid choosing empty operation-only views"
        );
    }

    #[test]
    fn aggregate_cluster_summaries_returns_one_row_per_cluster() {
        let cluster_a = Uuid::from_u128(0xC0);
        let cluster_b = Uuid::from_u128(0xD0);
        let rows = vec![
            view_row(cluster_a, 2, 0, false),
            view_row(cluster_a, 1, 3, false),
            view_row(cluster_b, 7, 1, true),
        ];

        let summaries = aggregate_cluster_summaries(&rows);
        assert_eq!(
            summaries.len(),
            2,
            "aggregator should collapse multiple views into one row per cluster"
        );

        let summary_a = summaries
            .iter()
            .find(|summary| summary.cluster_id == cluster_a)
            .expect("cluster a summary");
        assert_eq!(
            summary_a.epoch, 1,
            "cluster A should use the populated view"
        );
        assert_eq!(
            summary_a.node_count, 3,
            "cluster A should keep selected node count"
        );
        assert!(
            !summary_a.local_active,
            "cluster A should not be local-active"
        );

        let summary_b = summaries
            .iter()
            .find(|summary| summary.cluster_id == cluster_b)
            .expect("cluster b summary");
        assert_eq!(
            summary_b.epoch, 7,
            "cluster B should keep its selected epoch"
        );
        assert_eq!(summary_b.node_count, 1, "cluster B node count mismatch");
        assert!(summary_b.local_active, "cluster B should be local-active");
    }
}
