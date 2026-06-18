#[macro_use]
mod common;

#[path = "services/autoscale.rs"]
mod autoscale;
#[path = "services/dependencies.rs"]
mod dependencies;
#[path = "services/deployment.rs"]
mod deployment;
#[path = "services/drain.rs"]
mod drain;
#[path = "services/gang.rs"]
mod gang;
#[path = "services/network_realization.rs"]
mod network_realization;
#[path = "services/partition.rs"]
mod partition;
#[path = "services/placement.rs"]
mod placement;
#[path = "services/redeploy.rs"]
mod redeploy;
#[path = "services/sharding.rs"]
mod sharding;
#[path = "services/stop.rs"]
mod stop;
#[path = "services/support.rs"]
mod support;
#[path = "services/volumes.rs"]
mod volumes;
