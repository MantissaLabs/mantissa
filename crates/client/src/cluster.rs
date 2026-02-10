use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Context, Result, anyhow};
use protocol::topology;
use protocol::topology::split_selector_clause::Operator as SplitOperator;
use std::collections::{BTreeMap, HashSet};
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
    fn write_capnp(self, mut builder: topology::cluster_view_id::Builder<'_>) {
        builder
            .reborrow()
            .init_cluster_id()
            .set_value(self.cluster_id.as_bytes());
        builder.set_epoch(self.epoch);
    }

    /// Decodes one view from a topology Cap'n Proto response payload.
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

/// Operator-friendly split filters supported by the simplified CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitFilterKind {
    GpuVendor,
    GpuModel,
    CpuVendor,
    CpuBrand,
    GpuCount,
    CpuCores,
    CpuLogical,
    MemoryTotalKb,
    MemoryTotalBytes,
}

impl SplitFilterKind {
    /// Maps the CLI filter selector to the backend split selector key.
    fn selector_key(self) -> &'static str {
        match self {
            Self::GpuVendor => "resources.gpu.vendor",
            Self::GpuModel => "resources.gpu.model",
            Self::CpuVendor => "resources.cpu.vendor",
            Self::CpuBrand => "resources.cpu.brand",
            Self::GpuCount => "resources.gpu.count",
            Self::CpuCores => "resources.cpu.cores",
            Self::CpuLogical => "resources.cpu.logical",
            Self::MemoryTotalKb => "resources.memory.total_kb",
            Self::MemoryTotalBytes => "resources.memory.total_bytes",
        }
    }

    /// Returns whether this filter expects unsigned integer values.
    fn expects_numeric_value(self) -> bool {
        matches!(
            self,
            Self::GpuCount
                | Self::CpuCores
                | Self::CpuLogical
                | Self::MemoryTotalKb
                | Self::MemoryTotalBytes
        )
    }

    /// Returns a stable target-name prefix used to build split partition names.
    fn target_prefix(self) -> &'static str {
        match self {
            Self::GpuVendor => "gpu-vendor",
            Self::GpuModel => "gpu-model",
            Self::CpuVendor => "cpu-vendor",
            Self::CpuBrand => "cpu-brand",
            Self::GpuCount => "gpu-count",
            Self::CpuCores => "cpu-cores",
            Self::CpuLogical => "cpu-logical",
            Self::MemoryTotalKb => "memory-kb",
            Self::MemoryTotalBytes => "memory-bytes",
        }
    }
}

#[derive(Clone, Debug)]
struct SplitClauseSpec {
    key: String,
    op: SplitOperator,
    value: String,
}

#[derive(Clone, Debug)]
struct SplitTargetSpec {
    name: String,
    clauses: Vec<SplitClauseSpec>,
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

/// Returns the topology capability from the local session for cluster orchestration RPCs.
async fn topology_capability(cfg: &ClientConfig) -> Result<topology::Client> {
    let session = connection::get_local_session(cfg).await?;
    Ok(session
        .get_topology_request()
        .send()
        .pipeline
        .get_topology())
}

/// Parses a cluster UUID from CLI input and emits a contextual error on malformed values.
fn parse_cluster_id(input: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(input).with_context(|| format!("invalid {field}: {input}"))
}

/// Sanitizes a split filter value into a deterministic partition-name suffix.
fn slugify_split_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_dash = false;
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' {
            if last_dash {
                continue;
            }
            last_dash = true;
        } else {
            last_dash = false;
        }
        out.push(mapped);
    }
    out.trim_matches('-').to_string()
}

/// Appends a unique partition name into `seen`, adding a stable numeric suffix when needed.
fn reserve_unique_name(seen: &mut HashSet<String>, preferred: String) -> String {
    if !seen.contains(&preferred) {
        seen.insert(preferred.clone());
        return preferred;
    }

    let base = preferred;
    let mut suffix = 2u32;
    loop {
        let candidate = format!("{base}-{suffix}");
        if !seen.contains(&candidate) {
            seen.insert(candidate.clone());
            return candidate;
        }
        suffix = suffix.saturating_add(1);
    }
}

/// Normalizes and validates CLI-provided split filter values before request construction.
fn normalize_split_values(values: &[String], expects_numeric: bool) -> Result<Vec<String>> {
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::<String>::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }

        if expects_numeric && trimmed.parse::<u64>().is_err() {
            return Err(anyhow!(
                "split filter value '{trimmed}' must be an unsigned integer for this selector"
            ));
        }

        let dedupe_key = trimmed.to_ascii_lowercase();
        if seen.insert(dedupe_key) {
            normalized.push(trimmed.to_string());
        }
    }

    if normalized.is_empty() {
        return Err(anyhow!(
            "split requires at least one non-empty filter value"
        ));
    }

    Ok(normalized)
}

/// Picks the most recent known view for a given cluster lineage identifier.
fn resolve_view_from_summaries(
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

/// Queries the local node for cluster lineages without exposing raw per-view rows.
pub async fn list_clusters(cfg: &ClientConfig) -> Result<Vec<ClusterSummary>> {
    let views = list_cluster_views(cfg).await?;
    Ok(aggregate_cluster_summaries(&views))
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

/// Submits a merge request using cluster lineage identifiers instead of raw view ids.
pub async fn merge_by_cluster_id(
    cfg: &ClientConfig,
    source_cluster_id: &str,
    destination_cluster_id: &str,
    dry_run: bool,
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
    submit_merge_request(cfg, source_view, destination_view, dry_run).await
}

/// Submits a split request derived from a simple filter and value list.
pub async fn split_by_filter(
    cfg: &ClientConfig,
    source_cluster_id: Option<&str>,
    filter: SplitFilterKind,
    values: &[String],
    remainder_name: &str,
    dry_run: bool,
) -> Result<ClusterOperationSummary> {
    let source_view = match source_cluster_id {
        Some(cluster_id) => {
            let cluster_id = parse_cluster_id(cluster_id, "cluster id")?;
            resolve_cluster_view_by_cluster_id(cfg, cluster_id).await?
        }
        None => active_cluster_view(cfg).await?,
    };
    let selector_key = filter.selector_key();
    let value_list = normalize_split_values(values, filter.expects_numeric_value())?;

    let mut targets = Vec::with_capacity(value_list.len() + 1);
    let mut names = HashSet::<String>::new();
    for value in value_list {
        let suffix = slugify_split_value(&value);
        let preferred = if suffix.is_empty() {
            format!("{}-value", filter.target_prefix())
        } else {
            format!("{}-{suffix}", filter.target_prefix())
        };
        let name = reserve_unique_name(&mut names, preferred);
        targets.push(SplitTargetSpec {
            name,
            clauses: vec![SplitClauseSpec {
                key: selector_key.to_string(),
                op: SplitOperator::Eq,
                value,
            }],
        });
    }

    let fallback_trimmed = remainder_name.trim();
    let fallback_name = if fallback_trimmed.is_empty() {
        "other".to_string()
    } else {
        fallback_trimmed.to_string()
    };
    let fallback_name = reserve_unique_name(&mut names, fallback_name);
    targets.push(SplitTargetSpec {
        name: fallback_name,
        clauses: Vec::new(),
    });

    submit_split_request(cfg, source_view, &targets, dry_run).await
}

/// Sends a merge request to topology using resolved source and destination views.
async fn submit_merge_request(
    cfg: &ClientConfig,
    source_view: ClusterViewSpec,
    destination_view: ClusterViewSpec,
    dry_run: bool,
) -> Result<ClusterOperationSummary> {
    let topology = topology_capability(cfg).await?;
    let mut request = topology.merge_clusters_request();
    {
        let mut req = request.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());
        destination_view.write_capnp(req.reborrow().init_destination_view());
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

/// Sends a split request to topology using resolved source view and expanded targets.
async fn submit_split_request(
    cfg: &ClientConfig,
    source_view: ClusterViewSpec,
    targets: &[SplitTargetSpec],
    dry_run: bool,
) -> Result<ClusterOperationSummary> {
    if targets.is_empty() {
        return Err(anyhow!("split requires at least one target"));
    }

    let topology = topology_capability(cfg).await?;
    let mut request = topology.split_cluster_request();
    {
        let mut req = request.get().init_req();
        source_view.write_capnp(req.reborrow().init_source_view());

        let mut target_list = req.reborrow().init_targets(targets.len() as u32);
        for (idx, target_spec) in targets.iter().enumerate() {
            let mut target = target_list.reborrow().get(idx as u32);
            target.set_name(&target_spec.name);
            let mut selector = target.reborrow().init_selector();
            let mut clauses = selector
                .reborrow()
                .init_clauses(target_spec.clauses.len() as u32);
            for (clause_idx, clause_spec) in target_spec.clauses.iter().enumerate() {
                let mut clause = clauses.reborrow().get(clause_idx as u32);
                clause.set_key(&clause_spec.key);
                clause.set_op(clause_spec.op);
                clause.set_value(&clause_spec.value);
            }
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
