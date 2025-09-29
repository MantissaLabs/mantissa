use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Value stored in the replicated service store describing desired service state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceSpecValue {
    pub id: Uuid,
    pub manifest_id: Uuid,
    pub manifest_name: String,
    pub service_name: String,
    pub tasks: Vec<ServiceTaskSpecValue>,
    pub workload_ids: Vec<Uuid>,
    pub updated_at: String,
}

impl ServiceSpecValue {
    pub fn new(
        manifest_id: Uuid,
        manifest_name: impl Into<String>,
        service_name: impl Into<String>,
        tasks: Vec<ServiceTaskSpecValue>,
        workload_ids: Vec<Uuid>,
    ) -> Self {
        let manifest_name = manifest_name.into();
        let service_name = service_name.into();
        let id = compute_service_id(&service_name);

        Self {
            id,
            manifest_id,
            manifest_name,
            service_name,
            tasks,
            workload_ids,
            updated_at: current_timestamp(),
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = current_timestamp();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServiceTaskSpecValue {
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub replicas: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceEvent {
    Upsert(ServiceSpecValue),
    Remove { id: Uuid },
}

fn current_timestamp() -> String {
    Utc::now().to_rfc3339()
}

pub fn compute_service_id(service_name: &str) -> Uuid {
    let digest = blake3::hash(service_name.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::compute_service_id;

    #[test]
    fn service_id_deterministic() {
        let first = compute_service_id("alpha-web");
        let second = compute_service_id("alpha-web");
        assert_eq!(first, second);

        let other = compute_service_id("beta-web");
        assert_ne!(first, other);
    }
}
