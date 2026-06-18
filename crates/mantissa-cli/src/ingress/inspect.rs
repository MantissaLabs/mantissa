use crate::output;
use anyhow::Result;
use mantissa_client::config::ClientConfig;
use mantissa_client::ingress::{
    IngressPlacementConstraint, IngressPlacementConstraintOperator,
    IngressPlacementConstraintSelector, IngressPlacementSpec, IngressPlacementStrategy,
    IngressPoolSpec,
};

/// Fetches one ingress pool and renders its full replicated spec.
pub async fn inspect(cfg: &ClientConfig, name: &str) -> Result<()> {
    let pool = mantissa_client::ingress::inspect(cfg, name).await?;
    render_pool(&pool);
    Ok(())
}

/// Renders a pool spec in a stable multi-line diagnostic view.
fn render_pool(pool: &IngressPoolSpec) {
    output::emit_line(format!("ingress pool {} ({})", pool.name, pool.id));
    output::emit_line(format!("  min_nodes: {}", pool.min_nodes));
    output::emit_line(format!("  max_nodes: {}", pool.max_nodes_label()));
    output::emit_line(format!("  generation: {}", pool.generation));
    output::emit_line(format!("  created: {}", pool.created_at));
    output::emit_line(format!("  updated: {}", pool.updated_at));
    if let Some(spread_by) = &pool.spread_by {
        output::emit_line(format!("  spread_by: {spread_by}"));
    }
    render_placement(&pool.placement);
}

/// Renders the scheduler placement policy attached to an ingress pool.
fn render_placement(placement: &IngressPlacementSpec) {
    output::emit_line(format!(
        "  placement_strategy: {}",
        placement_strategy_label(placement.strategy)
    ));
    if placement.constraints.is_empty() {
        output::emit_line("  placement_constraints: none");
        return;
    }

    output::emit_line("  placement_constraints:");
    for constraint in &placement.constraints {
        output::emit_line(format!("    - {}", render_constraint(constraint)));
    }
}

/// Renders one placement constraint in the same compact form used in diagnostics.
fn render_constraint(constraint: &IngressPlacementConstraint) -> String {
    format!(
        "{} {} {}",
        selector_label(&constraint.selector),
        operator_label(constraint.operator),
        constraint.value
    )
}

/// Returns the stable selector label for one placement constraint.
fn selector_label(selector: &IngressPlacementConstraintSelector) -> String {
    match selector {
        IngressPlacementConstraintSelector::NodeId => "node.id".to_string(),
        IngressPlacementConstraintSelector::NodeHostname => "node.hostname".to_string(),
        IngressPlacementConstraintSelector::NodeIp => "node.ip".to_string(),
        IngressPlacementConstraintSelector::NodeAddress => "node.address".to_string(),
        IngressPlacementConstraintSelector::NodePlatformOs => "node.platform.os".to_string(),
        IngressPlacementConstraintSelector::NodePlatformArch => "node.platform.arch".to_string(),
        IngressPlacementConstraintSelector::NodeLabel { key } => format!("node.labels.{key}"),
    }
}

/// Returns the stable operator label for one placement constraint.
fn operator_label(operator: IngressPlacementConstraintOperator) -> &'static str {
    match operator {
        IngressPlacementConstraintOperator::Eq => "==",
        IngressPlacementConstraintOperator::Ne => "!=",
    }
}

/// Returns the stable strategy label for one placement policy.
fn placement_strategy_label(strategy: IngressPlacementStrategy) -> &'static str {
    match strategy {
        IngressPlacementStrategy::Spread => "spread",
        IngressPlacementStrategy::Binpack => "binpack",
    }
}
