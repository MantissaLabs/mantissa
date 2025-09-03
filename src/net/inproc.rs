#![cfg(any(test, feature = "testkit"))]

use std::{cell::RefCell, collections::HashMap};

use crate::includes::server_capnp;

// Thread-local capnp capability registry.
// Works with tests that run on tokio::task::LocalSet (single-thread).
thread_local! {
    static REGISTRY: RefCell<HashMap<String, server_capnp::server::Client>> =
        RefCell::new(HashMap::new());
}

pub fn register(name: impl Into<String>, client: server_capnp::server::Client) {
    REGISTRY.with(|r| {
        r.borrow_mut().insert(name.into(), client);
    });
}

pub fn get(name: &str) -> Option<server_capnp::server::Client> {
    REGISTRY.with(|r| r.borrow().get(name).cloned())
}
