use crate::ingress::types::{
    IngressPoolSelection, IngressPoolSpecValue, compute_ingress_pool_id, select_ingress_pool_nodes,
};
use crate::scheduler::placement::PlacementNode;
use crate::store::replicated::ingress::IngressPoolStore;
use anyhow::{Result, anyhow};
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Ordering;
use std::collections::HashSet;
use uuid::Uuid;

/// Ergonomic access layer over the replicated ingress pool store.
#[derive(Clone)]
pub struct IngressPoolRegistry {
    store: IngressPoolStore,
}

impl IngressPoolRegistry {
    /// Builds the registry from the underlying ingress pool store.
    pub fn new(store: IngressPoolStore) -> Self {
        Self { store }
    }

    /// Upserts one ingress pool specification into the replicated store.
    pub async fn upsert(&self, value: IngressPoolSpecValue) -> Result<()> {
        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|error| anyhow!("ingress pool upsert failed: {error}"))
    }

    /// Removes one ingress pool specification from the replicated store.
    pub async fn remove(&self, id: Uuid) -> Result<()> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|error| anyhow!("ingress pool remove failed: {error}"))?;
        Ok(())
    }

    /// Reads the canonical ingress pool specification for one identifier.
    pub fn get(&self, id: Uuid) -> Result<Option<IngressPoolSpecValue>> {
        let snapshot = self
            .store
            .get_snapshot(&UuidKey::from(id))
            .map_err(|error| anyhow!("ingress pool lookup failed: {error}"))?;
        Ok(snapshot.and_then(|snap| select_best_ingress_pool(snap.as_slice())))
    }

    /// Reads the canonical ingress pool specification for one pool name.
    pub fn get_by_name(&self, name: &str) -> Result<Option<IngressPoolSpecValue>> {
        self.get(compute_ingress_pool_id(name))
    }

    /// Lists canonical ingress pool specifications sorted by name.
    pub fn list(&self) -> Result<Vec<IngressPoolSpecValue>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|error| anyhow!("ingress pool load_all failed: {error}"))?;

        let mut seen = HashSet::new();
        let mut specs = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = select_best_ingress_pool(snapshot.as_slice())
                && seen.insert(id)
            {
                specs.push(value);
            }
        }

        specs.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(specs)
    }

    /// Derives the current selected node set for one ingress pool and candidate set.
    pub fn select_nodes(
        &self,
        pool: &IngressPoolSpecValue,
        candidates: &[PlacementNode],
    ) -> IngressPoolSelection {
        select_ingress_pool_nodes(pool, candidates)
    }
}

/// Selects the canonical MVReg winner for one ingress pool specification row.
pub fn select_best_ingress_pool(values: &[IngressPoolSpecValue]) -> Option<IngressPoolSpecValue> {
    let mut best: Option<&IngressPoolSpecValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if compare_ingress_pools(value, current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Compares two concurrent ingress pool specs to choose a deterministic canonical value.
fn compare_ingress_pools(left: &IngressPoolSpecValue, right: &IngressPoolSpecValue) -> Ordering {
    left.generation
        .cmp(&right.generation)
        .then(left.updated_at.cmp(&right.updated_at))
        .then(left.name.cmp(&right.name))
        .then(left.min_nodes.cmp(&right.min_nodes))
        .then(left.max_nodes.cmp(&right.max_nodes))
        .then(left.placement.cmp(&right.placement))
        .then(left.spread_by.cmp(&right.spread_by))
        .then(left.created_at.cmp(&right.created_at))
        .then(left.id.cmp(&right.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::types::{
        IngressPoolSpecDraft, IngressPoolSpreadKey, select_ingress_pool_nodes,
    };
    use crate::scheduler::placement::{
        PlacementConstraint, PlacementConstraintSelector, PlacementPolicy, PlacementStrategy,
    };
    use crate::store::replicated::ingress::open_ingress_pool_store;
    use crate::topology::peers::PeerLabel;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Builds one placement node with an ingress label and zone for selection tests.
    fn node(name: &str, ingress: &str, zone: &str) -> PlacementNode {
        PlacementNode::new(
            Uuid::new_v4(),
            name,
            format!("10.0.0.{}:6578", name.trim_start_matches("node-")),
            "linux",
            "x86_64",
            vec![
                PeerLabel {
                    key: "mantissa.io/ingress".to_string(),
                    value: ingress.to_string(),
                },
                PeerLabel {
                    key: "topology.zone".to_string(),
                    value: zone.to_string(),
                },
            ],
        )
    }

    /// Builds one valid pool spec for registry and selection tests.
    fn pool(name: &str, min_nodes: u16, max_nodes: Option<u16>) -> IngressPoolSpecValue {
        IngressPoolSpecValue::from_draft(IngressPoolSpecDraft {
            name: name.to_string(),
            min_nodes,
            max_nodes,
            placement: PlacementPolicy {
                constraints: vec![
                    PlacementConstraint::eq(
                        PlacementConstraintSelector::node_label("mantissa.io/ingress"),
                        name,
                    )
                    .expect("valid ingress constraint"),
                ],
                strategy: PlacementStrategy::Spread,
            },
            spread_by: Some(
                IngressPoolSpreadKey::node_label("topology.zone").expect("valid spread key"),
            ),
        })
        .expect("valid ingress pool")
    }

    #[tokio::test]
    async fn registry_round_trips_ingress_pool_specs() {
        let dir = tempdir().expect("tempdir");
        let db = redb::Database::create(dir.path().join("ingress.redb")).expect("create db");
        let store =
            open_ingress_pool_store(Arc::new(db), Uuid::new_v4()).expect("open ingress pool store");
        let registry = IngressPoolRegistry::new(store);
        let spec = pool("public-web", 2, Some(4));

        registry.upsert(spec.clone()).await.expect("upsert pool");

        assert_eq!(
            registry
                .get_by_name("public-web")
                .expect("lookup pool")
                .expect("pool present"),
            spec
        );
        assert_eq!(registry.list().expect("list pools"), vec![spec]);
    }

    #[test]
    fn ingress_pool_selection_spreads_across_label_values_before_filling_bucket() {
        let pool = pool("public-web", 2, Some(3));
        let candidates = vec![
            node("node-1", "public-web", "zone-a"),
            node("node-2", "public-web", "zone-a"),
            node("node-3", "public-web", "zone-b"),
            node("node-4", "public-web", "zone-c"),
            node("node-5", "private", "zone-c"),
        ];

        let selection = select_ingress_pool_nodes(&pool, &candidates);

        assert!(selection.is_ready());
        assert_eq!(selection.eligible_count, 4);
        assert_eq!(selection.selected_nodes.len(), 3);
        assert_eq!(
            selection
                .selected_nodes
                .iter()
                .map(|node| node.hostname.as_str())
                .collect::<Vec<_>>(),
            vec!["node-1", "node-3", "node-4"]
        );
    }
}
