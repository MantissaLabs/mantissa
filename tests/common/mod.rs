use std::{path::PathBuf, sync::Arc};

pub fn temp_db_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

pub fn temp_db(path: &PathBuf) -> Arc<redb::Database> {
    Arc::new(redb::Database::create(path).expect("create redb"))
}

pub fn fixed_noise_keys(byte: u8) -> mantissa::noise::NoiseKeys {
    // Deterministic Noise keypair for tests
    mantissa::noise::NoiseKeys::from_private_bytes([byte; 32])
}
