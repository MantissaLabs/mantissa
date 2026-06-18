use crate::workload_submit::{PlacementConstraint, PlacementConstraintSelector, PlacementSpec};
use crate::workload_wire::{read_placement_policy, write_placement_policy};
use anyhow::{Context, Result, anyhow};
use capnp::Error as CapnpError;
use mantissa_protocol::ingress::{
    ingress_endpoint, ingress_pool_apply_spec, ingress_pool_spec, ingress_pool_spread_key,
};
use serde::Deserialize;
use std::fmt;
use std::fs;
use std::path::Path;
use uuid::Uuid;

pub use crate::workload_submit::{
    PlacementConstraint as IngressPlacementConstraint,
    PlacementConstraintOperator as IngressPlacementConstraintOperator,
    PlacementConstraintSelector as IngressPlacementConstraintSelector,
    PlacementSpec as IngressPlacementSpec, PlacementStrategy as IngressPlacementStrategy,
};

/// Optional spread dimension used while selecting bounded ingress nodes.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum IngressPoolSpreadKey {
    NodeLabel { key: String },
}

impl fmt::Display for IngressPoolSpreadKey {
    /// Renders the spread key as a compact operator-facing label.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NodeLabel { key } => write!(f, "node_label({key})"),
        }
    }
}

/// RON manifest used by `mantissa ingress apply`.
#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct IngressPoolManifest {
    pub name: String,
    pub min_nodes: u16,
    #[serde(default)]
    pub max_nodes: Option<u16>,
    #[serde(default)]
    pub placement: PlacementSpec,
    #[serde(default)]
    pub spread_by: Option<IngressPoolSpreadKey>,
}

impl IngressPoolManifest {
    /// Loads one ingress-pool manifest from a RON file path.
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read ingress manifest {}", path.display()))?;
        ron::de::from_str(&raw)
            .with_context(|| format!("failed to parse ingress manifest {}", path.display()))
    }

    /// Performs lightweight client-side validation before sending an apply request.
    pub fn validate(&self) -> Result<()> {
        let name = self.name.trim();
        if name.is_empty() {
            return Err(anyhow!("ingress pool name cannot be empty"));
        }
        if self.min_nodes == 0 {
            return Err(anyhow!(
                "ingress pool '{name}' must set min_nodes to a non-zero value"
            ));
        }
        if let Some(max_nodes) = self.max_nodes {
            if max_nodes == 0 {
                return Err(anyhow!(
                    "ingress pool '{name}' must set max_nodes to a non-zero value when provided"
                ));
            }
            if max_nodes < self.min_nodes {
                return Err(anyhow!(
                    "ingress pool '{name}' max_nodes must be greater than or equal to min_nodes"
                ));
            }
        }
        validate_placement(&self.placement, "ingress pool")?;
        if let Some(IngressPoolSpreadKey::NodeLabel { key }) = &self.spread_by
            && key.trim().is_empty()
        {
            return Err(anyhow!(
                "ingress pool '{name}' spread_by node_label key cannot be empty"
            ));
        }
        Ok(())
    }

    /// Encodes this manifest into the ingress-pool apply request schema.
    pub fn write_apply_spec(&self, mut builder: ingress_pool_apply_spec::Builder<'_>) {
        builder.set_name(self.name.trim());
        builder.set_min_nodes(self.min_nodes);
        builder.set_max_nodes(self.max_nodes.unwrap_or(0));
        write_placement_policy(builder.reborrow().init_placement(), &self.placement);
        write_spread_key(builder.reborrow().init_spread_by(), self.spread_by.as_ref());
    }
}

/// Replicated ingress-pool spec returned by the daemon.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressPoolSpec {
    pub id: Uuid,
    pub name: String,
    pub min_nodes: u16,
    pub max_nodes: Option<u16>,
    pub placement: PlacementSpec,
    pub spread_by: Option<IngressPoolSpreadKey>,
    pub generation: u64,
    pub created_at: String,
    pub updated_at: String,
}

impl IngressPoolSpec {
    /// Decodes one ingress-pool spec from the protocol reader.
    pub fn from_reader(reader: ingress_pool_spec::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            id: read_uuid(reader.get_id()?, "ingress pool id")?,
            name: reader.get_name()?.to_str()?.to_string(),
            min_nodes: reader.get_min_nodes(),
            max_nodes: match reader.get_max_nodes() {
                0 => None,
                value => Some(value),
            },
            placement: read_placement_policy(reader.get_placement()?)?,
            spread_by: read_spread_key(reader.get_spread_by()?)?,
            generation: reader.get_generation(),
            created_at: reader.get_created_at()?.to_str()?.to_string(),
            updated_at: reader.get_updated_at()?.to_str()?.to_string(),
        })
    }

    /// Renders the max-node bound in the same compact form used by CLI tables.
    pub fn max_nodes_label(&self) -> String {
        self.max_nodes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unbounded".to_string())
    }
}

/// Filter accepted by `mantissa ingress endpoints`.
#[derive(Clone, Debug, Default)]
pub struct IngressEndpointFilter {
    pub service: Option<String>,
    pub template: Option<String>,
    pub pool: Option<String>,
    pub port: Option<u16>,
    pub ready_only: bool,
}

impl IngressEndpointFilter {
    /// Writes the endpoint filter into the protocol request shape.
    pub fn write_filter(
        &self,
        mut builder: mantissa_protocol::ingress::ingress_endpoint_filter::Builder<'_>,
    ) {
        builder.set_service(self.service.as_deref().unwrap_or("").trim());
        builder.set_template(self.template.as_deref().unwrap_or("").trim());
        builder.set_pool(self.pool.as_deref().unwrap_or("").trim());
        builder.set_port(self.port.unwrap_or(0));
        builder.set_ready_only(self.ready_only);
    }
}

/// Public endpoint row returned by the ingress endpoint listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressEndpoint {
    pub service_id: Uuid,
    pub service_name: Option<String>,
    pub template_name: String,
    pub network_id: Uuid,
    pub node_id: Uuid,
    pub node_ip: Option<String>,
    pub public_port: u16,
    pub protocol: String,
    pub ingress_mode: String,
    pub ingress_pool: Option<String>,
    pub ready: bool,
    pub generation: u64,
    pub detail: Option<String>,
}

impl IngressEndpoint {
    /// Decodes one public endpoint row from the protocol reader.
    pub fn from_reader(reader: ingress_endpoint::Reader<'_>) -> Result<Self, CapnpError> {
        Ok(Self {
            service_id: read_uuid(reader.get_service_id()?, "service id")?,
            service_name: optional_text(reader.get_service_name()?),
            template_name: reader.get_template_name()?.to_str()?.to_string(),
            network_id: read_uuid(reader.get_network_id()?, "network id")?,
            node_id: read_uuid(reader.get_node_id()?, "node id")?,
            node_ip: optional_text(reader.get_node_ip()?),
            public_port: reader.get_public_port(),
            protocol: reader.get_protocol()?.to_str()?.to_string(),
            ingress_mode: reader.get_ingress_mode()?.to_str()?.to_string(),
            ingress_pool: optional_text(reader.get_ingress_pool()?),
            ready: reader.get_ready(),
            generation: reader.get_generation(),
            detail: optional_text(reader.get_detail()?),
        })
    }
}

/// Writes an optional ingress-pool spread key into the protocol schema.
fn write_spread_key(
    mut builder: ingress_pool_spread_key::Builder<'_>,
    spread_by: Option<&IngressPoolSpreadKey>,
) {
    match spread_by {
        Some(IngressPoolSpreadKey::NodeLabel { key }) => builder.set_node_label(key.trim()),
        None => builder.set_none(()),
    }
}

/// Decodes an optional ingress-pool spread key from the protocol schema.
fn read_spread_key(
    reader: ingress_pool_spread_key::Reader<'_>,
) -> Result<Option<IngressPoolSpreadKey>, CapnpError> {
    match reader.which() {
        Ok(ingress_pool_spread_key::Which::NodeLabel(Ok(key))) => {
            Ok(Some(IngressPoolSpreadKey::NodeLabel {
                key: key.to_str()?.to_string(),
            }))
        }
        Ok(ingress_pool_spread_key::Which::NodeLabel(Err(error))) => Err(error),
        _ => Ok(None),
    }
}

/// Validates one placement policy using the shared manifest rules.
fn validate_placement(policy: &PlacementSpec, context: &str) -> Result<()> {
    for constraint in &policy.constraints {
        validate_constraint(constraint).map_err(|message| {
            anyhow!("{context} defines an invalid placement constraint: {message}")
        })?;
    }
    Ok(())
}

/// Performs lightweight validation for one typed placement constraint.
fn validate_constraint(constraint: &PlacementConstraint) -> std::result::Result<(), String> {
    let selector_key = render_selector_key(&constraint.selector);
    let value = constraint.value.trim();
    if value.is_empty() {
        return Err(format!(
            "constraint for selector '{}' must include a non-empty value",
            selector_key
        ));
    }
    if let PlacementConstraintSelector::NodeLabel { key } = &constraint.selector
        && key.trim().is_empty()
    {
        return Err("node_label selector requires a non-empty key".to_string());
    }
    Ok(())
}

/// Renders one placement selector key for diagnostics.
fn render_selector_key(selector: &PlacementConstraintSelector) -> String {
    match selector {
        PlacementConstraintSelector::NodeId => "node.id".to_string(),
        PlacementConstraintSelector::NodeHostname => "node.hostname".to_string(),
        PlacementConstraintSelector::NodeIp => "node.ip".to_string(),
        PlacementConstraintSelector::NodeAddress => "node.address".to_string(),
        PlacementConstraintSelector::NodePlatformOs => "node.platform.os".to_string(),
        PlacementConstraintSelector::NodePlatformArch => "node.platform.arch".to_string(),
        PlacementConstraintSelector::NodeLabel { key } => format!("node.labels.{key}"),
    }
}

/// Decodes a 16-byte UUID field from a protocol response.
fn read_uuid(bytes: capnp::data::Reader<'_>, field: &str) -> Result<Uuid, CapnpError> {
    let raw = bytes.to_owned();
    if raw.len() != 16 {
        return Err(CapnpError::failed(format!(
            "{field} must be 16 bytes, got {}",
            raw.len()
        )));
    }
    Uuid::from_slice(&raw).map_err(|error| CapnpError::failed(error.to_string()))
}

/// Converts one text field into an optional owned string after trimming.
fn optional_text(text: capnp::text::Reader<'_>) -> Option<String> {
    let trimmed = text.to_str().ok()?.trim().to_string();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantissa_protocol::ingress::{ingress_endpoint_filter, ingress_pool_spec};

    #[test]
    fn ron_manifest_parses_flat_ingress_pool() {
        let raw = r#"
            (
                name: "public-web",
                min_nodes: 3,
                max_nodes: Some(12),
                placement: (
                    constraints: [
                        (
                            selector: node_label(key: "mantissa.io/ingress"),
                            operator: eq,
                            value: "public-web",
                        ),
                    ],
                    strategy: spread,
                ),
                spread_by: Some(node_label(key: "topology.zone")),
            )
        "#;

        let manifest: IngressPoolManifest = ron::de::from_str(raw).expect("parse ingress pool");

        assert_eq!(manifest.name, "public-web");
        assert_eq!(manifest.min_nodes, 3);
        assert_eq!(manifest.max_nodes, Some(12));
        assert_eq!(
            manifest.spread_by,
            Some(IngressPoolSpreadKey::NodeLabel {
                key: "topology.zone".to_string()
            })
        );
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn pool_spec_decoder_preserves_placement_and_spread_key() {
        let pool_id = Uuid::new_v4();
        let mut message = capnp::message::Builder::new_default();
        {
            let mut builder = message.init_root::<ingress_pool_spec::Builder<'_>>();
            builder.set_id(pool_id.as_bytes());
            builder.set_name("public-web");
            builder.set_min_nodes(2);
            builder.set_max_nodes(4);
            write_placement_policy(
                builder.reborrow().init_placement(),
                &PlacementSpec {
                    constraints: vec![PlacementConstraint::eq(
                        PlacementConstraintSelector::node_label("mantissa.io/ingress"),
                        "public-web",
                    )],
                    strategy: IngressPlacementStrategy::Binpack,
                },
            );
            let mut spread_by = builder.reborrow().init_spread_by();
            spread_by.set_node_label("topology.zone");
            builder.set_generation(7);
            builder.set_created_at("2026-01-01T00:00:00Z");
            builder.set_updated_at("2026-01-01T00:00:01Z");
        }

        let reader = message
            .get_root::<ingress_pool_spec::Builder<'_>>()
            .expect("read pool")
            .into_reader();
        let decoded = IngressPoolSpec::from_reader(reader).expect("decode pool");

        assert_eq!(decoded.id, pool_id);
        assert_eq!(decoded.name, "public-web");
        assert_eq!(decoded.max_nodes, Some(4));
        assert_eq!(decoded.placement.constraints.len(), 1);
        assert_eq!(
            decoded.placement.strategy,
            IngressPlacementStrategy::Binpack
        );
        assert_eq!(
            decoded.spread_by,
            Some(IngressPoolSpreadKey::NodeLabel {
                key: "topology.zone".to_string()
            })
        );
    }

    #[test]
    fn endpoint_filter_writer_trims_optional_fields() {
        let filter = IngressEndpointFilter {
            service: Some(" web ".to_string()),
            template: Some(" api ".to_string()),
            pool: Some(" edge ".to_string()),
            port: Some(8080),
            ready_only: true,
        };
        let mut message = capnp::message::Builder::new_default();
        filter.write_filter(message.init_root::<ingress_endpoint_filter::Builder<'_>>());
        let reader = message
            .get_root::<ingress_endpoint_filter::Builder<'_>>()
            .expect("read filter")
            .into_reader();

        assert_eq!(reader.get_service().unwrap().to_str().unwrap(), "web");
        assert_eq!(reader.get_template().unwrap().to_str().unwrap(), "api");
        assert_eq!(reader.get_pool().unwrap().to_str().unwrap(), "edge");
        assert_eq!(reader.get_port(), 8080);
        assert!(reader.get_ready_only());
    }
}
