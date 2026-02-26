use crate::services::types::{ServiceSpecValue, compute_service_id};
use crate::store::service_store::ServiceStore;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use crdt_store::uuid_key::UuidKey;
use std::cmp::Ordering;
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
            if let Some(value) = select_best_service_spec(snapshot.as_slice()) {
                if seen.insert(id) {
                    values.push(value);
                }
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
                if should_prefer_candidate(current, value) {
                    best = Some(value);
                }
            }
        }
    }
    best.cloned()
}

/// Decides whether the candidate spec should replace the current selection.
fn should_prefer_candidate(current: &ServiceSpecValue, candidate: &ServiceSpecValue) -> bool {
    if current.manifest_id == candidate.manifest_id {
        let current_rank = status_rank(current.status);
        let candidate_rank = status_rank(candidate.status);

        match candidate_rank.cmp(&current_rank) {
            Ordering::Less => return false,
            Ordering::Equal => {
                if let (Some(current_ts), Some(candidate_ts)) = (
                    parse_timestamp(&current.updated_at),
                    parse_timestamp(&candidate.updated_at),
                ) {
                    return candidate_ts > current_ts;
                }
                return false;
            }
            Ordering::Greater => return true,
        }
    } else {
        return should_prefer_manifest_mismatch(current, candidate);
    }
}

/// Resolves selection when concurrent values carry different deployment manifests.
///
/// This mirrors service-controller gating so stale cross-generation updates cannot resurrect a
/// stopped service unless they represent a fresh Deploying bootstrap.
fn should_prefer_manifest_mismatch(
    current: &ServiceSpecValue,
    candidate: &ServiceSpecValue,
) -> bool {
    let (Some(current_ts), Some(candidate_ts)) = (
        parse_timestamp(&current.updated_at),
        parse_timestamp(&candidate.updated_at),
    ) else {
        return false;
    };

    if candidate_ts <= current_ts {
        return false;
    }

    use crate::services::types::ServiceStatus;

    match current.status {
        ServiceStatus::Stopping => false,
        ServiceStatus::Stopped | ServiceStatus::Failed => {
            candidate.status == ServiceStatus::Deploying && candidate.task_ids.is_empty()
        }
        ServiceStatus::Deploying | ServiceStatus::Running => {
            matches!(
                candidate.status,
                ServiceStatus::Deploying | ServiceStatus::Running
            )
        }
    }
}

/// Parses RFC3339 timestamps for service state comparisons.
fn parse_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

/// Ranks service status values for deterministic selection ordering.
fn status_rank(status: crate::services::types::ServiceStatus) -> u8 {
    use crate::services::types::ServiceStatus::{Deploying, Failed, Running, Stopped, Stopping};
    match status {
        Deploying | Failed => 0,
        Running => 1,
        Stopping => 2,
        Stopped => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::ServiceStatus;
    use crate::services::types::ServiceTaskSpecValue;
    use crate::store::service_store::open_service_store;
    use chrono::Duration as ChronoDuration;
    use redb::Database;
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
                env: Vec::new(),
                secret_files: Vec::new(),
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
        let updated = ServiceSpecValue::new(
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
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
                health_port: None,
                health_command: None,
                public_port: None,
                public_protocol: None,
            }],
            vec![Uuid::new_v4(), Uuid::new_v4()],
        );
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
            env: Vec::new(),
            secret_files: Vec::new(),
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
        let current = build_service_value(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        let candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Running,
            now + ChronoDuration::seconds(3),
            vec![Uuid::new_v4()],
        );

        assert!(!should_prefer_candidate(&current, &candidate));
    }

    /// Ensures stopped services only accept manifest-mismatch Deploying bootstrap candidates.
    #[test]
    fn stopped_accepts_manifest_mismatch_deploying_bootstrap_candidate() {
        let now = Utc::now();
        let current = build_service_value(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        let candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            Vec::new(),
        );

        assert!(should_prefer_candidate(&current, &candidate));
    }

    /// Ensures stopped services reject manifest-mismatch Deploying candidates with task ids.
    #[test]
    fn stopped_rejects_manifest_mismatch_deploying_prefilled_candidate() {
        let now = Utc::now();
        let current = build_service_value(Uuid::new_v4(), ServiceStatus::Stopped, now, Vec::new());
        let candidate = build_service_value(
            Uuid::new_v4(),
            ServiceStatus::Deploying,
            now + ChronoDuration::seconds(3),
            vec![Uuid::new_v4()],
        );

        assert!(!should_prefer_candidate(&current, &candidate));
    }
}
