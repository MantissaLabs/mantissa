use crate::scheduler::placement::{PlacementNode, PlacementPolicy};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use uuid::Uuid;

/// Optional dimension used to spread selected ingress nodes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum IngressPoolSpreadKey {
    NodeLabel { key: String },
}

impl IngressPoolSpreadKey {
    /// Builds one normalized node-label spread key.
    pub fn node_label(key: impl Into<String>) -> Result<Self, String> {
        let key = key.into().trim().to_string();
        if key.is_empty() {
            return Err("ingress pool spread_by node_label key cannot be empty".to_string());
        }
        Ok(Self::NodeLabel { key })
    }

    /// Returns the grouping value for one placement node.
    pub fn value_for_node<'a>(&'a self, node: &'a PlacementNode) -> &'a str {
        match self {
            Self::NodeLabel { key } => node.label_value(key).unwrap_or(""),
        }
    }
}

/// Replicated desired-state row for one public ingress pool.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IngressPoolSpecValue {
    pub id: Uuid,
    pub name: String,
    pub min_nodes: u16,
    pub max_nodes: Option<u16>,
    pub placement: PlacementPolicy,
    pub spread_by: Option<IngressPoolSpreadKey>,
    pub generation: u64,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub deleted: bool,
}

impl IngressPoolSpecValue {
    /// Creates one replicated ingress pool spec from validated draft intent.
    pub fn from_draft(draft: IngressPoolSpecDraft) -> Result<Self, String> {
        draft.into_value()
    }

    /// Returns whether the selected node count satisfies the pool's lower bound.
    pub fn is_ready_with_selected_count(&self, selected_count: usize) -> bool {
        selected_count >= usize::from(self.min_nodes)
    }

    /// Returns whether this replicated row is a delete marker.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// Refresh the update timestamp after a spec lifecycle mutation.
    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }

    /// Convert this spec into a delete marker that wins over stale active rows.
    pub fn mark_deleted(&mut self) {
        if self.deleted {
            return;
        }
        self.deleted = true;
        self.generation = self.generation.saturating_add(1);
        self.touch();
    }
}

/// Draft input used to create or replace one ingress pool spec.
#[derive(Clone, Debug)]
pub struct IngressPoolSpecDraft {
    pub name: String,
    pub min_nodes: u16,
    pub max_nodes: Option<u16>,
    pub placement: PlacementPolicy,
    pub spread_by: Option<IngressPoolSpreadKey>,
}

impl IngressPoolSpecDraft {
    /// Validates and materializes one ingress pool spec value.
    pub fn into_value(self) -> Result<IngressPoolSpecValue, String> {
        let name = self.name.trim().to_string();
        if name.is_empty() {
            return Err("ingress pool name cannot be empty".to_string());
        }
        if self.min_nodes == 0 {
            return Err(format!(
                "ingress pool '{name}' must set min_nodes to a non-zero value"
            ));
        }
        if let Some(max_nodes) = self.max_nodes {
            if max_nodes == 0 {
                return Err(format!(
                    "ingress pool '{name}' must set max_nodes to a non-zero value when provided"
                ));
            }
            if max_nodes < self.min_nodes {
                return Err(format!(
                    "ingress pool '{name}' max_nodes must be greater than or equal to min_nodes"
                ));
            }
        }

        let now = current_timestamp();
        Ok(IngressPoolSpecValue {
            id: compute_ingress_pool_id(&name),
            name,
            min_nodes: self.min_nodes,
            max_nodes: self.max_nodes,
            placement: self.placement,
            spread_by: self.spread_by,
            generation: 1,
            created_at: now.clone(),
            updated_at: now,
            deleted: false,
        })
    }
}

/// Stable selected node data derived from a pool and current cluster candidates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressPoolSelectedNode {
    pub node_id: Uuid,
    pub hostname: String,
    pub address: String,
}

impl IngressPoolSelectedNode {
    /// Copies the operator-facing candidate node fields needed by ingress endpoint views.
    fn from_placement_node(node: &PlacementNode) -> Self {
        Self {
            node_id: node.node_id,
            hostname: node.hostname.clone(),
            address: node.address.clone(),
        }
    }
}

/// Derived selection snapshot for one ingress pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngressPoolSelection {
    pub pool_id: Uuid,
    pub pool_name: String,
    pub min_nodes: u16,
    pub max_nodes: Option<u16>,
    pub eligible_count: usize,
    pub selected_nodes: Vec<IngressPoolSelectedNode>,
}

impl IngressPoolSelection {
    /// Returns true when the selected set satisfies the pool's readiness bound.
    pub fn is_ready(&self) -> bool {
        self.selected_nodes.len() >= usize::from(self.min_nodes)
    }
}

/// Selects the bounded ingress-node set for one pool from scheduler-visible candidates.
pub fn select_ingress_pool_nodes(
    pool: &IngressPoolSpecValue,
    candidates: &[PlacementNode],
) -> IngressPoolSelection {
    let mut eligible = candidates
        .iter()
        .filter(|node| pool.placement.matches(node))
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| {
        left.hostname
            .cmp(&right.hostname)
            .then(left.node_id.cmp(&right.node_id))
    });

    let limit = pool
        .max_nodes
        .map(usize::from)
        .unwrap_or(eligible.len())
        .min(eligible.len());
    let selected = if let Some(spread_by) = pool.spread_by.as_ref() {
        select_spread_nodes(&eligible, spread_by, limit)
    } else {
        eligible
            .iter()
            .take(limit)
            .map(|node| IngressPoolSelectedNode::from_placement_node(node))
            .collect()
    };

    IngressPoolSelection {
        pool_id: pool.id,
        pool_name: pool.name.clone(),
        min_nodes: pool.min_nodes,
        max_nodes: pool.max_nodes,
        eligible_count: eligible.len(),
        selected_nodes: selected,
    }
}

/// Selects nodes round-robin across spread buckets until the limit is satisfied.
fn select_spread_nodes(
    eligible: &[&PlacementNode],
    spread_by: &IngressPoolSpreadKey,
    limit: usize,
) -> Vec<IngressPoolSelectedNode> {
    let mut buckets: BTreeMap<&str, VecDeque<&PlacementNode>> = BTreeMap::new();
    for node in eligible {
        buckets
            .entry(spread_by.value_for_node(node))
            .or_default()
            .push_back(*node);
    }
    for nodes in buckets.values_mut() {
        let mut sorted = nodes.drain(..).collect::<Vec<_>>();
        sorted.sort_by(|left, right| {
            left.hostname
                .cmp(&right.hostname)
                .then(left.node_id.cmp(&right.node_id))
        });
        nodes.extend(sorted);
    }

    let mut selected = Vec::with_capacity(limit);
    while selected.len() < limit {
        let mut advanced = false;
        for nodes in buckets.values_mut() {
            if selected.len() >= limit {
                break;
            }
            if let Some(node) = nodes.pop_front() {
                selected.push(IngressPoolSelectedNode::from_placement_node(node));
                advanced = true;
            }
        }
        if !advanced {
            break;
        }
    }
    selected
}

/// Computes one stable ingress pool identifier from the pool name.
pub fn compute_ingress_pool_id(name: &str) -> Uuid {
    let digest = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// Returns the current timestamp in the store's stable string format.
pub fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}
