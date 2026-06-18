use mantissa_client::ingress::{
    IngressEndpoint as ClientIngressEndpoint, IngressEndpointFilter as ClientIngressEndpointFilter,
    IngressPlacementConstraint, IngressPlacementConstraintOperator,
    IngressPlacementConstraintSelector, IngressPlacementStrategy,
    IngressPoolManifest as ClientIngressPoolManifest, IngressPoolSpec as ClientIngressPoolSpec,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// REST request body for applying one ingress pool.
pub type IngressPoolApplyRequest = ClientIngressPoolManifest;

/// REST response returned after deleting an ingress pool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
pub struct IngressPoolDeleteResponse {
    pub deleted: usize,
}

/// REST-facing ingress-pool specification.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct IngressPoolSpec {
    pub id: String,
    pub name: String,
    pub min_nodes: u16,
    pub max_nodes: Option<u16>,
    pub placement_strategy: String,
    pub placement_constraints: Vec<String>,
    pub spread_by: Option<String>,
    pub generation: u64,
    pub created_at: String,
    pub updated_at: String,
}

impl From<ClientIngressPoolSpec> for IngressPoolSpec {
    /// Converts the client pool spec into a REST JSON shape.
    fn from(value: ClientIngressPoolSpec) -> Self {
        Self {
            id: value.id.to_string(),
            name: value.name,
            min_nodes: value.min_nodes,
            max_nodes: value.max_nodes,
            placement_strategy: placement_strategy_label(value.placement.strategy).to_string(),
            placement_constraints: value
                .placement
                .constraints
                .iter()
                .map(render_constraint)
                .collect(),
            spread_by: value.spread_by.map(|spread_by| spread_by.to_string()),
            generation: value.generation,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

/// Query parameters for listing ingress endpoint target rows.
#[derive(Clone, Debug, Default, Deserialize, IntoParams, ToSchema)]
pub struct IngressEndpointQuery {
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub template: Option<String>,
    #[serde(default)]
    pub pool: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub ready: bool,
}

impl From<IngressEndpointQuery> for ClientIngressEndpointFilter {
    /// Converts REST query parameters into the shared client endpoint filter.
    fn from(value: IngressEndpointQuery) -> Self {
        Self {
            service: value.service,
            template: value.template,
            pool: value.pool,
            port: value.port,
            ready_only: value.ready,
        }
    }
}

/// REST-facing public endpoint target row.
#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct IngressEndpoint {
    pub service_id: String,
    pub service_name: Option<String>,
    pub template_name: String,
    pub network_id: String,
    pub node_id: String,
    pub node_ip: Option<String>,
    pub public_port: u16,
    pub protocol: String,
    pub ingress_mode: String,
    pub ingress_pool: Option<String>,
    pub ready: bool,
    pub generation: u64,
    pub detail: Option<String>,
}

impl From<ClientIngressEndpoint> for IngressEndpoint {
    /// Converts the client endpoint row into the REST JSON shape.
    fn from(value: ClientIngressEndpoint) -> Self {
        Self {
            service_id: value.service_id.to_string(),
            service_name: value.service_name,
            template_name: value.template_name,
            network_id: value.network_id.to_string(),
            node_id: value.node_id.to_string(),
            node_ip: value.node_ip,
            public_port: value.public_port,
            protocol: value.protocol,
            ingress_mode: value.ingress_mode,
            ingress_pool: value.ingress_pool,
            ready: value.ready,
            generation: value.generation,
            detail: value.detail,
        }
    }
}

/// Renders one placement constraint in a stable operator-facing expression.
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
