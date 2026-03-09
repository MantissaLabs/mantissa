use crate::config::ClientConfig;
use crate::output;
use anyhow::{Context, Result, anyhow};
use protocol::topology::split_selector_clause::Operator as SplitOperator;
use std::collections::HashSet;
use ui::split_interactive::{
    SplitCandidate as UiSplitCandidate, SplitCandidateList as UiSplitCandidateList,
    run_split_planner,
};
use uuid::Uuid;

use super::list::{
    SplitCandidate as ClientSplitCandidate, SplitCandidateList as ClientSplitCandidateList,
    active_cluster_view, list_split_candidates, resolve_cluster_view_by_cluster_id,
};
use super::operations::{
    ClusterOperationSummary, ClusterViewSpec, emit_operation_summary, parse_cluster_id,
    topology_capability,
};

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

/// Split-time service behavior policy exposed by the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitServicePolicy {
    /// Keep services active per resulting partition by pruning out-of-scope task runtime rows.
    Partitioned,
    /// Preserve service/task runtime rows without split-time pruning.
    Preserve,
}

impl SplitServicePolicy {
    /// Encodes this policy into the topology RPC enum.
    fn to_capnp(self) -> protocol::topology::SplitServicePolicy {
        match self {
            Self::Partitioned => protocol::topology::SplitServicePolicy::Partitioned,
            Self::Preserve => protocol::topology::SplitServicePolicy::Preserve,
        }
    }
}

/// Split-time network behavior policy exposed by the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitNetworkPolicy {
    /// Isolate overlays per partition by pruning out-of-scope network runtime rows.
    Isolate,
    /// Preserve network peer/attachment rows without split-time pruning.
    Preserve,
}

impl SplitNetworkPolicy {
    /// Encodes this policy into the topology RPC enum.
    fn to_capnp(self) -> protocol::topology::SplitNetworkPolicy {
        match self {
            Self::Isolate => protocol::topology::SplitNetworkPolicy::Isolate,
            Self::Preserve => protocol::topology::SplitNetworkPolicy::Preserve,
        }
    }
}

/// Input payload for `split` so callers can delegate interactive/filtered split orchestration.
#[derive(Clone, Debug)]
pub struct SplitCommandRequest {
    pub source_cluster_id: Option<String>,
    pub interactive: bool,
    pub filter_per_gpu: Vec<String>,
    pub filter: Option<SplitFilterKind>,
    pub values: Vec<String>,
    pub remainder_name: String,
    pub left_name: String,
    pub right_name: String,
    pub dry_run: bool,
    pub service_policy: SplitServicePolicy,
    pub network_policy: SplitNetworkPolicy,
}

/// Expanded split selector clause used to populate topology split targets.
#[derive(Clone, Debug)]
struct SplitClauseSpec {
    key: String,
    op: SplitOperator,
    value: String,
}

/// Split target representation consumed by split request encoding.
#[derive(Clone, Debug)]
struct SplitTargetSpec {
    name: String,
    clauses: Vec<SplitClauseSpec>,
    explicit_nodes: Vec<Uuid>,
}

/// One explicit split target name and node set produced by interactive group assignment.
#[derive(Clone, Debug)]
pub struct ExplicitSplitTarget {
    pub name: String,
    pub node_ids: Vec<Uuid>,
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

/// Convert one client split candidate into a UI-facing candidate without coupling crates.
fn to_ui_candidate(candidate: ClientSplitCandidate) -> UiSplitCandidate {
    UiSplitCandidate {
        node_id: candidate.node_id,
        hostname: candidate.hostname,
        address: candidate.address,
        health: candidate.health,
        active_view: candidate.active_view.to_string(),
        cpu_vendor: candidate.cpu_vendor,
        cpu_brand: candidate.cpu_brand,
        cpu_logical: candidate.cpu_logical,
        cpu_cores: candidate.cpu_cores,
        memory_total_kb: candidate.memory_total_kb,
        gpu_vendor: candidate.gpu_vendor,
        gpu_count: candidate.gpu_count,
        gpu_models: candidate.gpu_models,
        wireguard_enabled: candidate.wireguard_enabled,
    }
}

/// Convert one split-candidate payload from client types to UI-local types.
fn to_ui_payload(payload: ClientSplitCandidateList) -> UiSplitCandidateList {
    let source_view = format!(
        "{} ({})",
        payload.source_view.cluster_id, payload.source_view.epoch
    );
    let candidates = payload
        .candidates
        .into_iter()
        .map(to_ui_candidate)
        .collect();
    UiSplitCandidateList {
        source_view,
        candidates,
    }
}

/// Resolve split mode and execute either interactive-node assignment or filter-based splitting.
pub async fn split(cfg: &ClientConfig, request: &SplitCommandRequest) -> Result<()> {
    if request.interactive {
        let payload = list_split_candidates(cfg, request.source_cluster_id.as_deref()).await?;
        if payload.candidates.is_empty() {
            return Err(anyhow!("no split candidates found in the selected cluster"));
        }

        let initial_group_names = vec![request.left_name.clone(), request.right_name.clone()];
        let selection = run_split_planner(to_ui_payload(payload), &initial_group_names)?;
        if selection.cancelled {
            output::emit_line("split cancelled");
            return Ok(());
        }

        let targets = selection
            .targets
            .into_iter()
            .map(|target| ExplicitSplitTarget {
                name: target.name,
                node_ids: target.node_ids,
            })
            .collect::<Vec<_>>();

        return split_by_explicit_targets(
            cfg,
            request.source_cluster_id.as_deref(),
            &targets,
            request.dry_run,
            request.service_policy,
            request.network_policy,
        )
        .await;
    }

    let (filter, values) = if !request.filter_per_gpu.is_empty() {
        (SplitFilterKind::GpuVendor, request.filter_per_gpu.clone())
    } else {
        let filter = request.filter.ok_or_else(|| anyhow!("--by is required"))?;
        (filter, request.values.clone())
    };

    split_by_filter(
        cfg,
        request.source_cluster_id.as_deref(),
        filter,
        &values,
        &request.remainder_name,
        request.dry_run,
        request.service_policy,
        request.network_policy,
    )
    .await
}

/// Submits a split request derived from a simple filter and value list.
#[allow(clippy::too_many_arguments)]
pub async fn split_by_filter(
    cfg: &ClientConfig,
    source_cluster_id: Option<&str>,
    filter: SplitFilterKind,
    values: &[String],
    remainder_name: &str,
    dry_run: bool,
    service_policy: SplitServicePolicy,
    network_policy: SplitNetworkPolicy,
) -> Result<()> {
    let source_view = resolve_source_view(cfg, source_cluster_id).await?;
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
            explicit_nodes: Vec::new(),
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
        explicit_nodes: Vec::new(),
    });

    let summary = submit_split_request(
        cfg,
        source_view,
        &targets,
        dry_run,
        service_policy,
        network_policy,
    )
    .await?;
    emit_operation_summary(&summary);
    Ok(())
}

/// Submits a split request from explicit named-target assignments selected by interactive tooling.
pub async fn split_by_explicit_targets(
    cfg: &ClientConfig,
    source_cluster_id: Option<&str>,
    targets: &[ExplicitSplitTarget],
    dry_run: bool,
    service_policy: SplitServicePolicy,
    network_policy: SplitNetworkPolicy,
) -> Result<()> {
    if targets.len() < 2 {
        return Err(anyhow!(
            "interactive split requires at least two named targets"
        ));
    }

    let source_view = resolve_source_view(cfg, source_cluster_id).await?;
    let mut names_seen = HashSet::<String>::with_capacity(targets.len());
    let total_node_count = targets
        .iter()
        .map(|target| target.node_ids.len())
        .sum::<usize>();
    let mut node_seen = HashSet::<Uuid>::with_capacity(total_node_count);
    let mut prepared_targets = Vec::with_capacity(targets.len());

    for target in targets {
        let name = target.name.trim();
        if name.is_empty() {
            return Err(anyhow!("split target name must not be empty"));
        }
        if !names_seen.insert(name.to_string()) {
            return Err(anyhow!("duplicate split target name '{name}'"));
        }
        if target.node_ids.is_empty() {
            return Err(anyhow!(
                "split target '{name}' must include at least one node"
            ));
        }

        let mut unique_nodes = Vec::with_capacity(target.node_ids.len());
        let mut target_seen = HashSet::<Uuid>::with_capacity(target.node_ids.len());
        for node_id in target.node_ids.iter().copied() {
            if !target_seen.insert(node_id) {
                continue;
            }
            if !node_seen.insert(node_id) {
                return Err(anyhow!(
                    "node {node_id} is assigned multiple times across split targets"
                ));
            }
            unique_nodes.push(node_id);
        }

        if unique_nodes.is_empty() {
            return Err(anyhow!(
                "split target '{name}' must include at least one unique node"
            ));
        }

        prepared_targets.push(SplitTargetSpec {
            name: name.to_string(),
            clauses: Vec::new(),
            explicit_nodes: unique_nodes,
        });
    }

    let summary = submit_split_request(
        cfg,
        source_view,
        &prepared_targets,
        dry_run,
        service_policy,
        network_policy,
    )
    .await?;
    emit_operation_summary(&summary);
    Ok(())
}

/// Submits a split request from exactly two explicit target partitions.
#[allow(clippy::too_many_arguments)]
pub async fn split_by_explicit_nodes(
    cfg: &ClientConfig,
    source_cluster_id: Option<&str>,
    left_name: &str,
    right_name: &str,
    left_nodes: &[Uuid],
    right_nodes: &[Uuid],
    dry_run: bool,
    service_policy: SplitServicePolicy,
    network_policy: SplitNetworkPolicy,
) -> Result<()> {
    let targets = vec![
        ExplicitSplitTarget {
            name: left_name.to_string(),
            node_ids: left_nodes.to_vec(),
        },
        ExplicitSplitTarget {
            name: right_name.to_string(),
            node_ids: right_nodes.to_vec(),
        },
    ];

    split_by_explicit_targets(
        cfg,
        source_cluster_id,
        &targets,
        dry_run,
        service_policy,
        network_policy,
    )
    .await
}

/// Sends a split request to topology using resolved source view and expanded targets.
async fn submit_split_request(
    cfg: &ClientConfig,
    source_view: ClusterViewSpec,
    targets: &[SplitTargetSpec],
    dry_run: bool,
    service_policy: SplitServicePolicy,
    network_policy: SplitNetworkPolicy,
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
            let mut explicit_nodes = selector
                .reborrow()
                .init_explicit_nodes(target_spec.explicit_nodes.len() as u32);
            for (node_idx, node_id) in target_spec.explicit_nodes.iter().enumerate() {
                explicit_nodes
                    .reborrow()
                    .get(node_idx as u32)
                    .set_bytes(node_id.as_bytes());
            }
        }
        req.set_dry_run(dry_run);
        req.set_service_policy(service_policy.to_capnp());
        req.set_network_policy(network_policy.to_capnp());
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
