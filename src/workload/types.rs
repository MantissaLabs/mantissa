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
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkloadEvent {
    Upsert(WorkloadSpec),
    Remove { id: Uuid },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct WorkloadValue {
    pub id: Uuid,
    pub name: String,
    pub image: String,
    pub state: ContainerState,
    pub created_at: String,
    pub command: Vec<String>,
    pub node_id: Uuid,
    pub node_name: String,
    pub slot_id: Option<u64>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

impl WorkloadValue {
    pub fn new(
        id: Uuid,
        name: impl Into<String>,
        image: impl Into<String>,
        state: ContainerState,
        created_at: impl Into<String>,
        command: Vec<String>,
        node_id: Uuid,
        node_name: impl Into<String>,
        slot_id: Option<u64>,
        cpu_millis: u64,
        memory_bytes: u64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            image: image.into(),
            state,
            created_at: created_at.into(),
            command,
            node_id,
            node_name: node_name.into(),
            slot_id,
            cpu_millis,
            memory_bytes,
        }
    }
}
