use std::rc::Rc;
use std::sync::Arc;

use ::health::HealthMonitor;

use crate::config;
use crate::registry::Registry;
use crate::scheduler::Scheduler;
use crate::sync::SyncRunner;

/// Runtime collaborators used by topology but owned outside its durable stores.
#[derive(Clone)]
pub(crate) struct TopologyDependencies {
    pub(crate) registry: Registry,
    pub(crate) scheduler: Rc<Scheduler>,
    pub(crate) sync: SyncRunner,
    pub(crate) health_monitor: Arc<HealthMonitor>,
    pub(crate) runtime_health: config::RuntimeHealthConfig,
}
