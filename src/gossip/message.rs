use crate::agents::types::AgentEvent;
use crate::jobs::types::JobEvent;
use crate::network::types::NetworkEvent;
use crate::scheduler::digest::SchedulerDigestEvent;
use crate::secrets::types::SecretEvent;
use crate::services::types::ServiceEvent;
use crate::store::replicated::secret_key_sync::SecretMasterKeySyncRecord;
use crate::topology::TopologyEvent;
use crate::volumes::types::VolumeEvent;
use crate::workload::model::WorkloadEvent;
use uuid::Uuid;

#[derive(Clone)]
pub enum Message {
    Void {
        id: Uuid,
    },
    Topology {
        id: Uuid,
        event: TopologyEvent,
    },
    Workload {
        id: Uuid,
        event: WorkloadEvent,
    },
    Job {
        id: Uuid,
        event: Box<JobEvent>,
    },
    Agent {
        id: Uuid,
        event: Box<AgentEvent>,
    },
    Service {
        id: Uuid,
        event: Box<ServiceEvent>,
    },
    Network {
        id: Uuid,
        event: NetworkEvent,
    },
    Secret {
        id: Uuid,
        event: SecretEvent,
    },
    Volume {
        id: Uuid,
        event: VolumeEvent,
    },
    SchedulerDigest {
        id: Uuid,
        event: SchedulerDigestEvent,
    },
    SecretMasterKey {
        id: Uuid,
        record: SecretMasterKeySyncRecord,
    },
}

impl Message {
    /// Returns the stable gossip identifier attached to this message.
    pub fn id(&self) -> Uuid {
        match self {
            Message::Void { id }
            | Message::Topology { id, .. }
            | Message::Workload { id, .. }
            | Message::Job { id, .. }
            | Message::Agent { id, .. }
            | Message::Service { id, .. }
            | Message::Network { id, .. }
            | Message::Secret { id, .. }
            | Message::Volume { id, .. }
            | Message::SchedulerDigest { id, .. }
            | Message::SecretMasterKey { id, .. } => *id,
        }
    }
}
