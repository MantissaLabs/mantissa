use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
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
        self.last_seen.lock().unwrap().insert(id, now);
        self.status.lock().unwrap().insert(id, Status::Alive);
    }

    pub fn set_status(&self, id: Uuid, s: Status) {
        self.status.lock().unwrap().insert(id, s);
    }

    pub fn status(&self, id: Uuid) -> Status {
        self.status
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .unwrap_or(Status::Unknown)
    }

    pub fn snapshot(&self) -> HashMap<Uuid, Status> {
        self.status.lock().unwrap().clone()
    }

    fn recompute(&self) {
        let now = Instant::now();
        let mut status = self.status.lock().unwrap();
        let seen = self.last_seen.lock().unwrap();

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
