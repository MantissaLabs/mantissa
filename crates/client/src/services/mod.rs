pub mod deploy;
pub mod list;
pub mod manifest;
pub mod rollout;
pub mod run;
pub mod stop;

pub use deploy::{ServiceDeploymentHandle, deploy_manifest};
pub use list::list;
pub use manifest::{ServiceManifest, TaskSpec, load_manifest_from_path};
pub use rollout::status as rollout_status;
pub use run::{StartedTask, TaskStartParams, run, run_many};
pub use stop::stop;
