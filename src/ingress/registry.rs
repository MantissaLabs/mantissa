use crate::ingress::types::{
    IngressPoolSelection, IngressPoolSpecValue, compute_ingress_pool_id, select_ingress_pool_nodes,
};
use crate::scheduler::placement::PlacementNode;
use crate::store::replicated::ingress::IngressPoolStore;
use anyhow::{Result, anyhow};
use mantissa_store::uuid_key::UuidKey;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

/// Ergonomic access layer over the replicated ingress pool store.
#[derive(Clone)]
pub struct IngressPoolRegistry {
    store: IngressPoolStore,
    change_notify: Arc<Notify>,
}

impl IngressPoolRegistry {
    /// Builds the registry from the underlying ingress pool store.
    pub fn new(store: IngressPoolStore) -> Self {
        Self {
            store,
            change_notify: Arc::new(Notify::new()),
        }
    }

    /// Upserts one ingress pool specification into the replicated store.
    pub async fn upsert(&self, value: IngressPoolSpecValue) -> Result<()> {
        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|error| anyhow!("ingress pool upsert failed: {error}"))?;
        self.change_notify.notify_one();
        Ok(())
    }

    /// Removes one ingress pool specification from the replicated store.
    pub async fn remove(&self, id: Uuid) -> Result<()> {
        let mut value = self
            .get_including_deleted(id)?
            .ok_or_else(|| anyhow!("ingress pool '{id}' not found for delete marker"))?;
        if value.is_deleted() {
            return Ok(());
        }
        value.mark_deleted();
        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|error| anyhow!("ingress pool remove failed: {error}"))?;
        self.change_notify.notify_one();
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

    /// Reads the canonical ingress pool row, including delete markers.
    pub fn get_including_deleted(&self, id: Uuid) -> Result<Option<IngressPoolSpecValue>> {
        let snapshot = self
            .store
            .get_snapshot(&UuidKey::from(id))
            .map_err(|error| anyhow!("ingress pool lookup failed: {error}"))?;
        Ok(snapshot.and_then(|snap| select_canonical_ingress_pool(snap.as_slice())))
    }

    /// Reads the canonical ingress pool specification for one pool name.
    pub fn get_by_name(&self, name: &str) -> Result<Option<IngressPoolSpecValue>> {
        self.get(compute_ingress_pool_id(name))
    }

    /// Reads the canonical ingress pool row for one pool name, including delete markers.
    pub fn get_by_name_including_deleted(
        &self,
        name: &str,
    ) -> Result<Option<IngressPoolSpecValue>> {
        self.get_including_deleted(compute_ingress_pool_id(name))
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

    /// Returns the ingress pool store change clock for derived-view invalidation.
    pub fn change_clock(&self) -> u64 {
        self.store.change_clock()
    }

    /// Returns a node-local notifier that fires after direct ingress-pool writes.
    pub fn change_notifier(&self) -> Arc<Notify> {
        self.change_notify.clone()
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
    select_canonical_ingress_pool(values).filter(|value| !value.is_deleted())
}

/// Selects the canonical MVReg winner, including delete markers.
fn select_canonical_ingress_pool(values: &[IngressPoolSpecValue]) -> Option<IngressPoolSpecValue> {
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
///
/// Deletes must outrank every active concurrent value. A later explicit apply
/// still recreates the pool because its MVReg write observes and dominates the
/// delete marker instead of merely competing with it.
fn compare_ingress_pools(left: &IngressPoolSpecValue, right: &IngressPoolSpecValue) -> Ordering {
    left.deleted
        .cmp(&right.deleted)
        .then(left.generation.cmp(&right.generation))
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
    use crate::store::replicated::ingress::{IngressPoolStore, open_ingress_pool_store};
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

    /// Opens one isolated ingress pool store for a specific CRDT actor.
    fn temp_store(actor: Uuid) -> (tempfile::TempDir, IngressPoolStore) {
        let dir = tempdir().expect("tempdir");
        let db = redb::Database::create(dir.path().join("ingress.redb")).expect("create db");
        let store = open_ingress_pool_store(Arc::new(db), actor).expect("open ingress pool store");
        (dir, store)
    }

    /// Replicates all raw ingress pool registers and tombstones from one store to another.
    async fn replicate_ingress_pool_store(source: &IngressPoolStore, target: &IngressPoolStore) {
        let (registers, tombstones) = source
            .load_all_regs()
            .expect("load ingress pool store rows");
        target
            .apply_delta_chunk_update_mst(registers, tombstones)
            .await
            .expect("apply ingress pool store delta");
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

    #[tokio::test]
    async fn deleted_pool_wins_over_stale_replicated_active_register() {
        let (_dir_a, store_a) = temp_store(Uuid::new_v4());
        let (_dir_b, store_b) = temp_store(Uuid::new_v4());
        let registry_a = IngressPoolRegistry::new(store_a.clone());
        let registry_b = IngressPoolRegistry::new(store_b.clone());
        let spec = pool("public-web", 1, Some(1));

        registry_a.upsert(spec.clone()).await.expect("upsert pool");
        replicate_ingress_pool_store(&store_a, &store_b).await;

        let mut stale_active = spec.clone();
        stale_active.generation = stale_active.generation.saturating_add(10);
        stale_active.touch();
        registry_b
            .upsert(stale_active)
            .await
            .expect("upsert concurrent active pool");
        registry_a.remove(spec.id).await.expect("delete pool");

        replicate_ingress_pool_store(&store_b, &store_a).await;
        replicate_ingress_pool_store(&store_a, &store_b).await;

        assert!(
            registry_a
                .get_by_name("public-web")
                .expect("lookup deleted pool on A")
                .is_none(),
            "deleted pool should hide a stale higher-generation active register on A"
        );
        assert!(
            registry_b
                .get_by_name("public-web")
                .expect("lookup deleted pool on B")
                .is_none(),
            "deleted pool should hide a stale higher-generation active register on B"
        );
        assert!(
            registry_a.list().expect("list pools on A").is_empty(),
            "deleted pool should not appear in A list"
        );
        assert!(
            registry_b.list().expect("list pools on B").is_empty(),
            "deleted pool should not appear in B list"
        );
    }

    #[tokio::test]
    async fn apply_after_observed_delete_recreates_pool() {
        let (_dir, store) = temp_store(Uuid::new_v4());
        let registry = IngressPoolRegistry::new(store);
        let spec = pool("public-web", 1, Some(1));

        registry.upsert(spec.clone()).await.expect("upsert pool");
        registry.remove(spec.id).await.expect("delete pool");

        assert!(
            registry
                .get_by_name("public-web")
                .expect("lookup deleted pool")
                .is_none(),
            "deleted pool should be hidden from active lookups"
        );

        let deleted = registry
            .get_by_name_including_deleted("public-web")
            .expect("lookup delete marker")
            .expect("delete marker should remain visible to apply path");
        assert!(deleted.is_deleted());

        let mut recreated = pool("public-web", 2, Some(3));
        recreated.id = deleted.id;
        recreated.generation = deleted.generation.saturating_add(1);
        recreated.touch();
        registry
            .upsert(recreated.clone())
            .await
            .expect("recreate pool after observing delete");

        assert_eq!(
            registry
                .get_by_name("public-web")
                .expect("lookup recreated pool")
                .expect("recreated pool should be active"),
            recreated
        );
        assert_eq!(
            registry.list().expect("list recreated pool"),
            vec![recreated]
        );
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
