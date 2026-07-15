use crate::output;
use anyhow::{Result, anyhow};
use mantissa_client::clusters::{
    ExplicitSplitTarget, SplitCandidate, SplitCandidateList, SplitCommandRequest,
};
use mantissa_client::config::ClientConfig;
use mantissa_ui::split_interactive::{
    SplitCandidate as UiSplitCandidate, SplitCandidateList as UiSplitCandidateList,
    run_split_planner,
};

use super::operations::emit_operation_result;

/// Converts one client split candidate into a UI-facing candidate without coupling the client to UI.
fn to_ui_candidate(candidate: SplitCandidate) -> UiSplitCandidate {
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
        labels: candidate.labels,
    }
}

/// Converts one split-candidate payload from client types to UI-local types.
fn to_ui_payload(payload: SplitCandidateList) -> UiSplitCandidateList {
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

/// Resolves split mode and executes interactive or non-interactive split orchestration.
pub async fn split(cfg: &ClientConfig, request: &SplitCommandRequest, wait: bool) -> Result<()> {
    if request.interactive {
        let payload = mantissa_client::clusters::list_split_candidates(
            cfg,
            request.source_cluster_id.as_deref(),
        )
        .await?;
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

        let summary = mantissa_client::clusters::split_by_explicit_targets(
            cfg,
            request.source_cluster_id.as_deref(),
            &targets,
            request.dry_run,
            request.service_policy,
            request.network_policy,
        )
        .await?;
        return emit_operation_result(cfg, summary, wait).await;
    }

    let summary = mantissa_client::clusters::split(cfg, request).await?;
    emit_operation_result(cfg, summary, wait).await
}
