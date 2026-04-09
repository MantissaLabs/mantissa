use std::rc::Rc;
use std::sync::Arc;

use ::health::HealthMonitor;

use crate::config;
use crate::network::registry::NetworkRegistry;
use crate::registry::Registry;
use crate::scheduler::Scheduler;
use crate::services::ServiceRegistry;
use crate::sync::SyncRunner;
use crate::volumes::VolumeRegistry;
use crate::workload::WorkloadRegistry;

/// Runtime collaborators used by topology but owned outside its durable stores.
#[derive(Clone)]
pub(crate) struct TopologyDependencies {
    pub(crate) registry: Registry,
    pub(crate) network_registry: NetworkRegistry,
    pub(crate) workload_registry: WorkloadRegistry,
    pub(crate) service_registry: ServiceRegistry,
    pub(crate) volume_registry: VolumeRegistry,
    pub(crate) scheduler: Rc<Scheduler>,
    pub(crate) sync: SyncRunner,
    pub(crate) health_monitor: Arc<HealthMonitor>,
    pub(crate) runtime_health: config::RuntimeHealthConfig,
}
