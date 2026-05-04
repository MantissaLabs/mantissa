pub mod deploy;
pub mod list;
pub mod manifest;
pub mod rollout;
pub mod stop;

pub use deploy::{ServiceDeployOutcome, ServiceDeploymentHandle, deploy_manifest};
pub use list::list;
pub use manifest::{ServiceManifest, TaskTemplateSpec, load_manifest_from_path};
pub use rollout::status as rollout_status;
pub use stop::stop;
