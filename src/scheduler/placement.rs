use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::topology::peers::PeerLabel;

/// Placement policy attached to one schedulable workload template.
///
/// Constraints are evaluated as a conjunction: every constraint must match the candidate node.
/// Strategy selection controls how matching candidates are ranked once they pass hard filters.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementPolicy {
    #[serde(default)]
    pub constraints: Vec<PlacementConstraint>,
    #[serde(default)]
    pub preferences: Vec<PlacementPreference>,
    #[serde(default)]
    pub strategy: PlacementStrategy,
}

impl PlacementPolicy {
    /// Returns true when this policy does not carry any hard candidate filter.
    pub fn is_unconstrained(&self) -> bool {
        self.constraints.is_empty()
    }

    /// Returns true when the provided node satisfies every configured hard constraint.
    pub fn matches(&self, node: &PlacementNode) -> bool {
        self.constraints
            .iter()
            .all(|constraint| constraint.matches(node))
    }

    /// Renders every hard constraint back into a stable operator-facing expression string.
    pub fn rendered_constraints(&self) -> Vec<String> {
        self.constraints
            .iter()
            .map(PlacementConstraint::render_expression)
            .collect()
    }
}

/// Best-effort scheduler hints evaluated after hard constraints pass.
///
/// These preferences currently rely on service ownership metadata, so they are most useful for
/// service-managed workloads whose replicas already advertise stable `(service, template)` labels.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlacementPreference {
    /// Prefer nodes that already run replicas from the same service.
    ServiceAffinity,
    /// Prefer nodes that currently run fewer replicas from the same service.
    ServiceAntiAffinity,
    /// Prefer nodes that already run replicas from the same task template.
    TaskAffinity,
    /// Prefer nodes that currently run fewer replicas from the same task template.
    TaskAntiAffinity,
}

/// Candidate ranking mode applied after hard placement filters pass.
#[derive(
    Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum PlacementStrategy {
    /// Prefer even task distribution across the eligible candidate set.
    #[default]
    Spread,
    /// Prefer reusing the fullest matching node before expanding onto more peers.
    Binpack,
}

/// One hard placement predicate interpreted against a candidate node.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementConstraint {
    pub key: String,
    pub op: PlacementConstraintOperator,
    pub value: String,
}

impl PlacementConstraint {
    /// Parses one Swarm-style constraint expression such as `node.hostname == worker-a`.
    pub fn parse_expression(raw: &str) -> Result<Self, PlacementConstraintParseError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(PlacementConstraintParseError::EmptyExpression);
        }

        let (operator, parts) = if let Some(parts) = trimmed.split_once("!=") {
            (PlacementConstraintOperator::Ne, parts)
        } else if let Some(parts) = trimmed.split_once("==") {
            (PlacementConstraintOperator::Eq, parts)
        } else {
            return Err(PlacementConstraintParseError::MissingOperator {
                expression: trimmed.to_string(),
            });
        };

        let key = parts.0.trim();
        let value = parts.1.trim();
        if key.is_empty() {
            return Err(PlacementConstraintParseError::EmptyKey {
                expression: trimmed.to_string(),
            });
        }
        if value.is_empty() {
            return Err(PlacementConstraintParseError::EmptyValue {
                expression: trimmed.to_string(),
            });
        }

        Self::new(key.to_string(), operator, value.to_string())
    }

    /// Builds one validated placement constraint from its already split components.
    pub fn new(
        key: String,
        op: PlacementConstraintOperator,
        value: String,
    ) -> Result<Self, PlacementConstraintParseError> {
        let key = key.trim().to_string();
        let value = value.trim().to_string();
        let selector = PlacementSelector::parse(&key)?;
        selector.validate_value(&value)?;

        Ok(Self { key, op, value })
    }

    /// Renders this constraint into the stable Swarm-style operator string.
    pub fn render_expression(&self) -> String {
        format!("{} {} {}", self.key, self.op.as_str(), self.value)
    }

    /// Returns true when this single hard predicate matches the candidate node.
    pub fn matches(&self, node: &PlacementNode) -> bool {
        let Ok(selector) = PlacementSelector::parse(&self.key) else {
            return false;
        };
        let matched = selector.matches(node, &self.value);
        match self.op {
            PlacementConstraintOperator::Eq => matched,
            PlacementConstraintOperator::Ne => !matched,
        }
    }
}

/// Supported comparison operators for placement constraints.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PlacementConstraintOperator {
    Eq,
    Ne,
}

impl PlacementConstraintOperator {
    /// Returns the textual operator used by Swarm-style constraint expressions.
    pub const fn as_str(self) -> &'static str {
        match self {
            PlacementConstraintOperator::Eq => "==",
            PlacementConstraintOperator::Ne => "!=",
        }
    }
}

/// Scheduler-visible node metadata used while evaluating hard placement filters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlacementNode {
    pub node_id: Uuid,
    pub hostname: String,
    pub address: String,
    pub platform_os: String,
    pub platform_arch: String,
    pub labels: Vec<PeerLabel>,
}

impl PlacementNode {
    /// Builds one candidate node metadata record from converged cluster state.
    pub fn new(
        node_id: Uuid,
        hostname: impl Into<String>,
        address: impl Into<String>,
        platform_os: impl Into<String>,
        platform_arch: impl Into<String>,
        labels: Vec<PeerLabel>,
    ) -> Self {
        Self {
            node_id,
            hostname: hostname.into(),
            address: address.into(),
            platform_os: platform_os.into(),
            platform_arch: platform_arch.into(),
            labels,
        }
    }

    /// Returns the value stored for the provided node label key, if any.
    pub fn label_value(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|label| label.key == key)
            .map(|label| label.value.as_str())
    }

    /// Returns the advertised node IP when the cluster address encodes a socket endpoint.
    pub fn ip_addr(&self) -> Option<IpAddr> {
        if let Ok(socket) = self.address.parse::<SocketAddr>() {
            return Some(socket.ip());
        }

        self.address.parse::<IpAddr>().ok()
    }
}

/// Placement-constraint parse failures surfaced during manifest and RPC validation.
#[derive(Debug, Error)]
pub enum PlacementConstraintParseError {
    #[error("placement constraint must not be empty")]
    EmptyExpression,

    #[error("placement constraint '{expression}' must use either '==' or '!='")]
    MissingOperator { expression: String },

    #[error("placement constraint '{expression}' must include a non-empty key")]
    EmptyKey { expression: String },

    #[error("placement constraint '{expression}' must include a non-empty value")]
    EmptyValue { expression: String },

    #[error(
        "unsupported placement constraint key '{key}'; supported keys are node.id, node.hostname, node.ip, node.address, node.platform.os, node.platform.arch, and node.labels.<key>"
    )]
    UnsupportedKey { key: String },

    #[error("placement constraint key '{key}' requires an IP address or CIDR value, got '{value}'")]
    InvalidIpValue { key: String, value: String },
}

/// One normalized selector extracted from a placement-constraint key.
enum PlacementSelector {
    Id,
    Hostname,
    Ip,
    Address,
    PlatformOs,
    PlatformArch,
    Label(String),
}

impl PlacementSelector {
    /// Parses one placement-selector key into its normalized internal representation.
    fn parse(raw: &str) -> Result<Self, PlacementConstraintParseError> {
        if let Some(key) = raw
            .strip_prefix("node.labels.")
            .or_else(|| raw.strip_prefix("labels."))
        {
            if key.trim().is_empty() {
                return Err(PlacementConstraintParseError::UnsupportedKey {
                    key: raw.to_string(),
                });
            }
            return Ok(Self::Label(key.to_string()));
        }

        match raw {
            "node.id" => Ok(Self::Id),
            "node.hostname" => Ok(Self::Hostname),
            "node.ip" => Ok(Self::Ip),
            "node.address" => Ok(Self::Address),
            "node.platform.os" => Ok(Self::PlatformOs),
            "node.platform.arch" => Ok(Self::PlatformArch),
            _ => Err(PlacementConstraintParseError::UnsupportedKey {
                key: raw.to_string(),
            }),
        }
    }

    /// Validates the operand value for this selector before the constraint is accepted.
    fn validate_value(&self, value: &str) -> Result<(), PlacementConstraintParseError> {
        if matches!(self, Self::Ip) && !is_valid_ip_or_cidr(value) {
            return Err(PlacementConstraintParseError::InvalidIpValue {
                key: "node.ip".to_string(),
                value: value.to_string(),
            });
        }

        Ok(())
    }

    /// Returns true when the candidate node matches this selector/value pair.
    fn matches(&self, node: &PlacementNode, expected: &str) -> bool {
        match self {
            Self::Id => node.node_id.to_string() == expected,
            Self::Hostname => node.hostname == expected,
            Self::Address => node.address == expected,
            Self::PlatformOs => platform_os_matches_value(&node.platform_os, expected),
            Self::PlatformArch => platform_arch_matches_value(&node.platform_arch, expected),
            Self::Label(key) => node.label_value(key) == Some(expected),
            Self::Ip => node
                .ip_addr()
                .map(|actual| ip_matches_value(actual, expected))
                .unwrap_or(false),
        }
    }
}

/// Per-node service-placement counts used to evaluate soft affinity preferences deterministically.
#[derive(Clone, Debug, Default)]
pub struct PlacementPreferenceInventory {
    service_counts: HashMap<Uuid, HashMap<String, usize>>,
    template_counts: HashMap<Uuid, HashMap<String, HashMap<String, usize>>>,
}

impl PlacementPreferenceInventory {
    /// Records one visible service replica on the provided node so future placements can score it.
    pub fn record_service_replica(
        &mut self,
        node_id: Uuid,
        service_name: &str,
        template_name: &str,
    ) {
        *self
            .service_counts
            .entry(node_id)
            .or_default()
            .entry(service_name.to_string())
            .or_insert(0) += 1;
        *self
            .template_counts
            .entry(node_id)
            .or_default()
            .entry(service_name.to_string())
            .or_default()
            .entry(template_name.to_string())
            .or_insert(0) += 1;
    }

    /// Returns the visible same-service and same-template replica counts for one candidate node.
    pub fn counts_for(
        &self,
        node_id: Uuid,
        service_name: &str,
        template_name: &str,
    ) -> PlacementPreferenceCounts {
        PlacementPreferenceCounts {
            same_service_count: self
                .service_counts
                .get(&node_id)
                .and_then(|counts| counts.get(service_name))
                .copied()
                .unwrap_or(0),
            same_template_count: self
                .template_counts
                .get(&node_id)
                .and_then(|services| services.get(service_name))
                .and_then(|templates| templates.get(template_name))
                .copied()
                .unwrap_or(0),
        }
    }
}

/// Preference-relevant replica counts already visible on one candidate node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacementPreferenceCounts {
    pub same_service_count: usize,
    pub same_template_count: usize,
}

impl PlacementPreference {
    /// Returns the per-node count this preference reads when comparing two candidates.
    fn relevant_count(self, counts: PlacementPreferenceCounts) -> usize {
        match self {
            PlacementPreference::ServiceAffinity | PlacementPreference::ServiceAntiAffinity => {
                counts.same_service_count
            }
            PlacementPreference::TaskAffinity | PlacementPreference::TaskAntiAffinity => {
                counts.same_template_count
            }
        }
    }

    /// Compares two candidate count snapshots according to this individual preference.
    fn compare_counts(
        self,
        left: PlacementPreferenceCounts,
        right: PlacementPreferenceCounts,
    ) -> Ordering {
        let left_count = self.relevant_count(left);
        let right_count = self.relevant_count(right);

        match self {
            PlacementPreference::ServiceAffinity | PlacementPreference::TaskAffinity => {
                left_count.cmp(&right_count)
            }
            PlacementPreference::ServiceAntiAffinity | PlacementPreference::TaskAntiAffinity => {
                right_count.cmp(&left_count)
            }
        }
    }
}

/// Compares two candidate preference snapshots using the policy's declared preference order.
///
/// The first preference that distinguishes the candidates wins, which keeps operator intent
/// explicit and easy to reason about when multiple soft hints are present.
pub fn compare_placement_preference_counts(
    preferences: &[PlacementPreference],
    left: PlacementPreferenceCounts,
    right: PlacementPreferenceCounts,
) -> Ordering {
    for preference in preferences {
        let ordering = preference.compare_counts(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

/// Returns true when the text encodes either one concrete IP address or one CIDR prefix.
fn is_valid_ip_or_cidr(value: &str) -> bool {
    if value.parse::<IpAddr>().is_ok() {
        return true;
    }

    parse_cidr(value).is_some()
}

/// Returns true when the candidate IP matches either the concrete address or CIDR operand.
fn ip_matches_value(actual: IpAddr, expected: &str) -> bool {
    if let Ok(parsed) = expected.parse::<IpAddr>() {
        return parsed == actual;
    }

    parse_cidr(expected)
        .map(|(network, prefix)| ip_in_cidr(actual, network, prefix))
        .unwrap_or(false)
}

/// Parses one CIDR string into its base IP and prefix length.
fn parse_cidr(value: &str) -> Option<(IpAddr, u8)> {
    let (network_text, prefix_text) = value.split_once('/')?;
    let network = network_text.parse::<IpAddr>().ok()?;
    let prefix = prefix_text.parse::<u8>().ok()?;

    let max_prefix = match network {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    (prefix <= max_prefix).then_some((network, prefix))
}

/// Returns true when the IP falls inside the provided CIDR prefix.
fn ip_in_cidr(actual: IpAddr, network: IpAddr, prefix: u8) -> bool {
    match (actual, network) {
        (IpAddr::V4(actual), IpAddr::V4(network)) => {
            let actual = u32::from(actual);
            let network = u32::from(network);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            (actual & mask) == (network & mask)
        }
        (IpAddr::V6(actual), IpAddr::V6(network)) => {
            let actual = u128::from(actual);
            let network = u128::from(network);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            (actual & mask) == (network & mask)
        }
        _ => false,
    }
}

/// Returns true when the candidate platform OS matches the requested operand after alias folding.
fn platform_os_matches_value(actual: &str, expected: &str) -> bool {
    normalize_platform_os(actual) == normalize_platform_os(expected)
}

/// Returns true when the candidate platform architecture matches the requested operand alias.
fn platform_arch_matches_value(actual: &str, expected: &str) -> bool {
    normalize_platform_arch(actual) == normalize_platform_arch(expected)
}

/// Folds common platform OS aliases into one stable scheduler comparison key.
fn normalize_platform_os(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "macos" | "osx" => "darwin".to_string(),
        other => other.to_string(),
    }
}

/// Folds common architecture aliases into one stable scheduler comparison key.
fn normalize_platform_arch(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "x86_64" | "x64" => "amd64".to_string(),
        "aarch64" => "arm64".to_string(),
        "x86" | "i386" => "386".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PlacementConstraint, PlacementConstraintOperator, PlacementNode, PlacementPolicy,
        PlacementPreference, PlacementPreferenceCounts, PlacementStrategy,
        compare_placement_preference_counts,
    };
    use crate::topology::peers::PeerLabel;
    use std::cmp::Ordering;
    use uuid::Uuid;

    /// Constraint parsing should accept the Swarm-style equality operator.
    #[test]
    fn parses_equality_constraint_expression() {
        let parsed = PlacementConstraint::parse_expression("node.labels.zone == west")
            .expect("placement constraint should parse");

        assert_eq!(parsed.key, "node.labels.zone");
        assert_eq!(parsed.op, PlacementConstraintOperator::Eq);
        assert_eq!(parsed.value, "west");
        assert_eq!(
            parsed.render_expression(),
            "node.labels.zone == west".to_string()
        );
    }

    /// Constraint matching should support node labels and exact hostname predicates.
    #[test]
    fn policy_matches_node_labels_and_hostname() {
        let node = PlacementNode::new(
            Uuid::new_v4(),
            "worker-west-1",
            "10.0.0.22:7000",
            "linux",
            "amd64",
            vec![PeerLabel {
                key: "zone".into(),
                value: "west".into(),
            }],
        );
        let policy = PlacementPolicy {
            constraints: vec![
                PlacementConstraint::parse_expression("node.hostname == worker-west-1")
                    .expect("hostname constraint"),
                PlacementConstraint::parse_expression("node.labels.zone == west")
                    .expect("label constraint"),
            ],
            preferences: Vec::new(),
            strategy: PlacementStrategy::Spread,
        };

        assert!(policy.matches(&node));
    }

    /// IP constraints should support CIDR prefixes so address ranges can be targeted cleanly.
    #[test]
    fn policy_matches_node_ip_cidr() {
        let node = PlacementNode::new(
            Uuid::new_v4(),
            "worker-a",
            "10.42.7.9:7000",
            "linux",
            "amd64",
            Vec::new(),
        );
        let policy = PlacementPolicy {
            constraints: vec![
                PlacementConstraint::parse_expression("node.ip == 10.42.0.0/16")
                    .expect("cidr constraint"),
            ],
            preferences: Vec::new(),
            strategy: PlacementStrategy::Spread,
        };

        assert!(policy.matches(&node));
    }

    /// Invalid IP operands should be rejected during parsing instead of failing later at match time.
    #[test]
    fn rejects_invalid_ip_constraint_values() {
        let error = PlacementConstraint::parse_expression("node.ip == definitely-not-an-ip")
            .expect_err("invalid node.ip value must fail");

        assert!(error.to_string().contains("node.ip"));
    }

    /// Platform selectors should accept common OS and architecture aliases used by operators.
    #[test]
    fn policy_matches_platform_aliases() {
        let node = PlacementNode::new(
            Uuid::new_v4(),
            "worker-a",
            "10.42.7.9:7000",
            "macos",
            "x86_64",
            Vec::new(),
        );
        let policy = PlacementPolicy {
            constraints: vec![
                PlacementConstraint::parse_expression("node.platform.os == darwin")
                    .expect("platform os constraint"),
                PlacementConstraint::parse_expression("node.platform.arch == amd64")
                    .expect("platform arch constraint"),
            ],
            preferences: Vec::new(),
            strategy: PlacementStrategy::Spread,
        };

        assert!(policy.matches(&node));
    }

    /// Preference comparison should follow the declared list order so operators can break ties.
    #[test]
    fn preference_comparison_respects_declared_order() {
        let left = PlacementPreferenceCounts {
            same_service_count: 1,
            same_template_count: 3,
        };
        let right = PlacementPreferenceCounts {
            same_service_count: 2,
            same_template_count: 0,
        };

        let ordering = compare_placement_preference_counts(
            &[
                PlacementPreference::TaskAffinity,
                PlacementPreference::ServiceAntiAffinity,
            ],
            left,
            right,
        );

        assert_eq!(ordering, Ordering::Greater);
    }
}
