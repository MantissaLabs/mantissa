use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::workload::container::ContainerState;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkloadEvent {
    Upsert(WorkloadSpec),
    Remove { id: Uuid },
}
