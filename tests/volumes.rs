#[macro_use]
mod common;

#[path = "volumes/creation.rs"]
mod creation;
#[path = "volumes/deletion.rs"]
mod deletion;
#[path = "volumes/local.rs"]
mod local;
#[path = "volumes/postgres.rs"]
mod postgres;
#[path = "volumes/recovery.rs"]
mod recovery;
#[path = "volumes/restart.rs"]
mod restart;
#[path = "volumes/support.rs"]
mod support;
#[path = "volumes/sync.rs"]
mod sync;
