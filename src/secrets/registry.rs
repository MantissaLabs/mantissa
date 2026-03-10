use crate::secrets::types::{SecretValue, compute_secret_id};
use crate::store::secret_store::SecretStore;
use anyhow::{Result, anyhow};
use crdt_store::uuid_key::UuidKey;
use std::collections::HashSet;
use uuid::Uuid;

#[derive(Clone)]
pub struct SecretRegistry {
    store: SecretStore,
}

impl SecretRegistry {
    pub fn new(store: SecretStore) -> Self {
        Self { store }
    }

    pub async fn upsert(&self, value: SecretValue) -> Result<()> {
        self.store
            .upsert(&UuidKey::from(value.id), value)
            .await
            .map_err(|e| anyhow!("secret upsert failed: {e}"))
    }

    pub async fn remove(&self, id: Uuid) -> Result<()> {
        self.store
            .remove(&UuidKey::from(id))
            .await
            .map_err(|e| anyhow!("secret remove failed: {e}"))?;
        Ok(())
    }

    pub fn get(&self, id: Uuid) -> Result<Option<SecretValue>> {
        let key = UuidKey::from(id);
        let snapshot = self
            .store
            .get_snapshot(&key)
            .map_err(|e| anyhow!("secret lookup failed: {e}"))?;

        Ok(snapshot.and_then(|snap| snap.as_slice().last().cloned()))
    }

    pub fn get_by_name(&self, name: &str) -> Result<Option<SecretValue>> {
        let id = compute_secret_id(name);
        self.get(id)
    }

    pub fn list(&self) -> Result<Vec<SecretValue>> {
        let (entries, _) = self
            .store
            .load_all()
            .map_err(|e| anyhow!("secret store load_all failed: {e}"))?;

        let mut seen = HashSet::new();
        let mut values = Vec::with_capacity(entries.len());
        for (key, snapshot) in entries {
            let id = key.to_uuid();
            if let Some(value) = snapshot.as_slice().last().cloned()
                && seen.insert(id)
            {
                values.push(value);
            }
        }

        values.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(values)
    }
}
