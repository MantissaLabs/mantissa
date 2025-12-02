#![cfg_attr(test, allow(clippy::unwrap_used))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
    last_seen: Mutex<HashMap<Uuid, Instant>>,
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
            last_seen: Mutex::new(HashMap::new()),
            status: Mutex::new(HashMap::new()),
        })
    }

    pub fn start(self: &Arc<Self>) -> JoinHandle<()> {
        let me = self.clone();
        tokio::task::spawn_local(async move {
            let mut ticker = tokio::time::interval(me.cfg.tick);
            loop {
                ticker.tick().await;
                me.recompute();
            }
        })
    }

    pub fn observe_seen(&self, id: Uuid) {
        let now = Instant::now();
        lock_or_recover(&self.last_seen, "health.last_seen").insert(id, now);
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

    fn recompute(&self) {
        let now = Instant::now();
        let mut status = lock_or_recover(&self.status, "health.status");
        let seen = lock_or_recover(&self.last_seen, "health.last_seen");

        for (id, last) in seen.iter() {
            let elapsed = now.saturating_duration_since(*last);
            let next = if elapsed <= self.cfg.suspect_after {
                Status::Alive
            } else if elapsed <= self.cfg.down_after {
                Status::Suspect
            } else {
                Status::Down
            };
            status.insert(*id, next);
        }
    }
}
