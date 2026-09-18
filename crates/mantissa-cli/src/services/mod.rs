pub mod deploy;
pub mod exec;
pub mod inspect;
pub mod list;
pub mod logs;
pub mod rollout;
pub mod stop;
pub mod tasks;
mod wait;

pub use deploy::{ServiceRunOptions, run_manifest};
pub use exec::exec;
pub use inspect::inspect;
pub use list::list;
pub use logs::logs;
pub use mantissa_client::services::load_manifest_from_path;
pub use rollout::status as rollout_status;
pub use stop::stop;
pub use tasks::tasks;
