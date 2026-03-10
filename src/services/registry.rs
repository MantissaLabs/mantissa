use crate::services::ordering::compare_service_specs;
use crate::services::types::{ServiceSpecValue, compute_service_id};
use crate::store::service_store::ServiceStore;
use anyhow::{Result, anyhow};
use crdt_store::uuid_key::UuidKey;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone)]
pub struct ServiceRegistry {
    store: ServiceStore,
}

impl ServiceRegistry {
    pub fn new(store: ServiceStore) -> Self {
        Self { store }
    }

    /// Returns the underlying store change clock so callers can invalidate cached projections.
    pub fn change_clock(&self) -> u64 {
        self.store.change_clock()
    }

    pub async fn upsert(&self, value: ServiceSpecValue) -> Result<()> {
        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("service upsert failed: {e}"))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn remove_by_id(&self, id: Uuid) -> Result<()> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("service remove failed: {e}"))?;
        Ok(())
    }

    pub fn get(&self, id: Uuid) -> Result<Option<ServiceSpecValue>> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow!("service lookup failed: {e}"))?;

        Ok(snapshot.and_then(|snap| select_best_service_spec(snap.as_slice())))
    }

    pub fn list(&self) -> Result<Vec<ServiceSpecValue>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow!("service store load_all failed: {e}"))?;

        let mut seen = HashSet::new();
        let mut values = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = select_best_service_spec(snapshot.as_slice())
                && seen.insert(id)
            {
                values.push(value);
            }
        }

        values.sort_by(|a, b| a.service_name.cmp(&b.service_name));

        Ok(values)
    }

    #[allow(dead_code)]
    pub fn compute_id(&self, service_name: &str) -> Uuid {
        compute_service_id(service_name)
    }
}

/// Picks the canonical service spec from concurrent MVReg versions based on status and timestamp.
fn select_best_service_spec(values: &[ServiceSpecValue]) -> Option<ServiceSpecValue> {
    let mut best: Option<&ServiceSpecValue> = None;
    for value in values {
        match best {
            None => best = Some(value),
            Some(current) => {
                if compare_service_specs(value, current).is_gt() {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::ServiceTaskSpecValue;
    use crate::services::types::{ServiceRolloutState, ServiceStatus};
    use crate::store::service_store::open_service_store;
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use redb::Database;
    use std::cmp::Ordering;
    use tempfile::tempdir;

    fn temp_store() -> ServiceStore {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("services.redb");
        let db = std::sync::Arc::new(Database::create(path).expect("create db"));
        open_service_store(db.clone(), Uuid::new_v4()).expect("open service store")
    }

    #[tokio::test]
    async fn upsert_and_list_services() {
        let store = temp_store();
        let registry = ServiceRegistry::new(store);

        let manifest_id = Uuid::new_v4();
        let spec = ServiceSpecValue::new(
            manifest_id,
            "demo-manifest",
            "demo-service",
            vec![ServiceTaskSpecValue {
                name: "web".into(),
                image: "ghcr.io/demo/web:latest".into(),
                command: vec!["--port".into(), "8080".into()],
                replicas: 2,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
            vec![Uuid::new_v4()],
        );

        registry.upsert(spec.clone()).await.expect("upsert");

        let fetched = registry.get(spec.id).expect("get").expect("value");
        assert_eq!(fetched.tasks.len(), 1);
        assert_eq!(fetched.tasks[0].image, "ghcr.io/demo/web:latest");

        let listed = registry.list().expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].service_name, "demo-service");
        assert_eq!(listed[0].tasks.len(), 1);
        assert_eq!(listed[0].tasks[0].name, "web");

        // Update same service with new manifest id (should overwrite)
        let mut updated = ServiceSpecValue::new(
            Uuid::new_v4(),
            "demo-manifest",
            "demo-service",
            vec![ServiceTaskSpecValue {
                name: "web".into(),
                image: "ghcr.io/demo/web:v2".into(),
                command: vec![],
                replicas: 3,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
            vec![Uuid::new_v4(), Uuid::new_v4()],
        );
        updated.service_epoch = spec.service_epoch.saturating_add(1);
        registry.upsert(updated.clone()).await.expect("upsert");

        let listed = registry.list().expect("list again");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].tasks[0].image, "ghcr.io/demo/web:v2");
        assert_eq!(listed[0].tasks[0].replicas, 3);
    }

    /// Builds a service value with explicit lifecycle metadata for preference tests.
    fn build_service_value(
        manifest_id: Uuid,
        status: ServiceStatus,
        updated_at: DateTime<Utc>,
        task_ids: Vec<Uuid>,
    ) -> ServiceSpecValue {
        let tasks = vec![ServiceTaskSpecValue {
            name: "api".into(),
            image: "ghcr.io/demo/api:latest".into(),
            command: Vec::new(),
            replicas: 1,
            cpu_millis: 0,
            memory_bytes: 0,
            gpu_count: 0,
            restart_policy: None,
            termination_grace_period_secs: None,
            pre_stop_command: None,
            env: Vec::new(),
            secret_files: Vec::new(),
            volumes: Vec::new(),
            networks: Vec::new(),
            health_port: None,
            health_command: None,
            public_port: None,
            public_protocol: None,
        }];

        let mut value = ServiceSpecValue::new(manifest_id, "manifest", "svc", tasks, task_ids);
        value.status = status;
        value.updated_at = updated_at.to_rfc3339();
        value
    }

    /// Ensures stopped services do not prefer stale cross-manifest running candidates.
    #[test]
    fn stopped_rejects_manifest_mismatch_running_candidate() {
        let now = Utc::now();
        let mut current =
            build_service_value(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        current.service_epoch = 2;
        let mut candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + ChronoDuration::seconds(3),
            vec![Uuid::new_v4()],
        );
        candidate.service_epoch = 3;

        assert_eq!(compare_service_specs(&candidate, &current), Ordering::Less);
    }

    /// Ensures stopped services only accept manifest-mismatch Deploying bootstrap candidates.
    #[test]
    fn stopped_accepts_manifest_mismatch_deploying_bootstrap_candidate() {
        let now = Utc::now();
        let mut current =
            build_service_value(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        current.service_epoch = 4;
        let mut candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            Vec::new(),
        );
        candidate.service_epoch = 5;

        assert_eq!(
            compare_service_specs(&candidate, &current),
            Ordering::Greater
        );
    }

    /// Ensures stopped services reject manifest-mismatch Deploying candidates with task ids.
    #[test]
    fn stopped_rejects_manifest_mismatch_deploying_prefilled_candidate() {
        let now = Utc::now();
        let mut current =
            build_service_value(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        current.service_epoch = 6;
        let mut candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            vec![Uuid::new_v4()],
        );
        candidate.service_epoch = 7;

        assert_eq!(compare_service_specs(&candidate, &current), Ordering::Less);
    }

    /// Ensures plain prior-generation running values do not override a fresh deploying intent.
    #[test]
    fn deploying_rejects_previous_generation_running_without_rollout_history() {
        let now = Utc::now();
        let mut current = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            Vec::new(),
        );
        current.service_epoch = 12;
        let mut candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + ChronoDuration::seconds(4),
            vec![Uuid::new_v4()],
        );
        candidate.service_epoch = 11;

        assert_eq!(compare_service_specs(&candidate, &current), Ordering::Less);
    }

    /// Ensures stale prior-generation failed values cannot block a fresh deploy bootstrap.
    #[test]
    fn deploying_rejects_previous_generation_failed_rollout_history_when_stale() {
        let now = Utc::now();
        let mut current = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            Vec::new(),
        );
        current.service_epoch = 22;

        let mut candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Failed,
            now,
            vec![Uuid::new_v4()],
        );
        candidate.service_epoch = 21;
        candidate.rollout = ServiceRolloutState {
            total_steps: 1,
            completed_steps: 0,
            failed_steps: 1,
            max_failures: 1,
            last_error: Some("older failed generation".into()),
            ..ServiceRolloutState::default()
        };

        assert_eq!(compare_service_specs(&candidate, &current), Ordering::Less);
    }

    /// Ensures explicit rollback completions beat the immediately newer deploying generation.
    #[test]
    fn deploying_prefers_previous_generation_running_rollback_candidate() {
        let now = Utc::now();
        let mut current = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            Vec::new(),
        );
        current.service_epoch = 12;

        let mut candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + ChronoDuration::seconds(4),
            vec![Uuid::new_v4()],
        );
        candidate.service_epoch = 11;
        candidate.rollout = ServiceRolloutState {
            total_steps: 1,
            completed_steps: 1,
            failed_steps: 1,
            max_failures: 1,
            last_error: Some("redeploy failed".into()),
            ..ServiceRolloutState::default()
        };

        assert_eq!(
            compare_service_specs(&candidate, &current),
            Ordering::Greater
        );
    }
}
