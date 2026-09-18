pub mod deploy;
pub mod exec;
pub mod inspect;
pub mod list;
pub mod logs;
pub mod manifest;
pub mod rollout;
pub mod stop;
pub mod tasks;

pub use deploy::{ServiceDeployOutcome, ServiceDeploymentHandle, deploy_manifest};
pub use exec::{ServiceExecTask, select_exec_task};
pub use inspect::{ServiceInspection, inspect};
pub use list::list;
pub use logs::{ServiceLogEvent, ServiceLogSource, ServiceLogStream, logs};
pub use manifest::{ServiceManifest, TaskTemplateSpec, load_manifest_from_path};
pub use rollout::status as rollout_status;
pub use stop::stop;
pub use tasks::{ServiceTaskRow, tasks};
