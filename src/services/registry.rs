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

    pub async fn upsert(&self, mut value: ServiceSpecValue) -> Result<()> {
        // ensure timestamp reflects last update
        value.touch();

        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("service upsert failed: {e}"))?;
        Ok(())
    }

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

        Ok(snapshot.and_then(|snap| snap.as_slice().last().cloned()))
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
            if let Some(value) = snapshot.as_slice().last().cloned() {
                if seen.insert(id) {
                    values.push(value);
                }
            }
        }

        values.sort_by(|a, b| a.service_name.cmp(&b.service_name));

        Ok(values)
    }

    pub fn compute_id(&self, service_name: &str) -> Uuid {
        compute_service_id(service_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::ServiceTaskSpecValue;
    use crate::store::service_store::open_service_store;
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
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
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
                restart_policy: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                networks: Vec::new(),
            }],
            vec![Uuid::new_v4(), Uuid::new_v4()],
        );
        registry.upsert(updated.clone()).await.expect("upsert");

        let listed = registry.list().expect("list again");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].tasks[0].image, "ghcr.io/demo/web:v2");
        assert_eq!(listed[0].tasks[0].replicas, 3);
    }
}
