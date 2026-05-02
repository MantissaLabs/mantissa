#[macro_use]
mod common;

#[path = "services/dependencies.rs"]
mod dependencies;
#[path = "services/deployment.rs"]
mod deployment;
#[path = "services/drain.rs"]
mod drain;
#[path = "services/partition.rs"]
mod partition;
#[path = "services/placement.rs"]
mod placement;
#[path = "services/redeploy.rs"]
mod redeploy;
#[path = "services/stop.rs"]
mod stop;
#[path = "services/support.rs"]
mod support;
#[path = "services/volumes.rs"]
mod volumes;
