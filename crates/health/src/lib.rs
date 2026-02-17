#![cfg_attr(test, allow(clippy::unwrap_used))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::warn;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Unknown,
    Alive,
    Suspect,
    Down,
    Degraded,
}

pub struct HealthMonitor {
    status: Mutex<HashMap<Uuid, Status>>,
}

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> std::sync::MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(err) => {
            warn!("{name} mutex poisoned: {err}");
            err.into_inner()
        }
    }
}

impl HealthMonitor {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(HashMap::new()),
        })
    }

    pub fn observe_seen(&self, id: Uuid) {
        lock_or_recover(&self.status, "health.status").insert(id, Status::Alive);
    }

    pub fn set_status(&self, id: Uuid, s: Status) {
        lock_or_recover(&self.status, "health.status").insert(id, s);
    }

    pub fn status(&self, id: Uuid) -> Status {
        self.status
            .lock()
            .unwrap_or_else(|err| {
                warn!("health.status mutex poisoned: {err}");
                err.into_inner()
            })
            .get(&id)
            .cloned()
            .unwrap_or(Status::Unknown)
    }

    pub fn snapshot(&self) -> HashMap<Uuid, Status> {
        lock_or_recover(&self.status, "health.status").clone()
    }
}
