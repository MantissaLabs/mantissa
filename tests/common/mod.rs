#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_macros)]

use std::{path::PathBuf, sync::Arc};

pub fn temp_db_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

pub fn temp_db(path: &PathBuf) -> Arc<redb::Database> {
    Arc::new(redb::Database::create(path).expect("create redb"))
}

pub fn fixed_noise_keys(byte: u8) -> net::noise::NoiseKeys {
    // Deterministic Noise keypair for tests
    net::noise::NoiseKeys::from_private_bytes([byte; 32])
}

#[macro_use]
pub mod macros;
pub mod convergence;
#[cfg(target_os = "linux")]
pub mod privileged_networking;
pub mod testkit;
