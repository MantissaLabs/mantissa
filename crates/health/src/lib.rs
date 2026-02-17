#![cfg_attr(test, allow(clippy::unwrap_used))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
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

#[derive(Clone, Debug)]
pub struct Config {
    pub tick: Duration,
    pub suspect_after: Duration,
    pub down_after: Duration,
    pub degrade_grace: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            tick: Duration::from_millis(250),
            suspect_after: Duration::from_secs(2),
            down_after: Duration::from_secs(6),
            degrade_grace: Duration::from_secs(3),
        }
    }
}

pub struct HealthMonitor {
    cfg: Config,
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
    pub fn new(cfg: Config) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            status: Mutex::new(HashMap::new()),
        })
    }

    pub fn start(self: &Arc<Self>) -> JoinHandle<()> {
        let me = self.clone();
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(me.cfg.tick);
            loop {
                ticker.tick().await;
            }
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
