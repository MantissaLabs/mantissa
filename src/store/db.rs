use std::path::PathBuf;

use anyhow::{Context, Result};
use redb::{Database, TableDefinition};

pub const REGISTERS: TableDefinition<&str, &[u8]> = TableDefinition::new("registers");

pub fn init_database(base_path: PathBuf) -> Result<Database> {
    let mut db_path = base_path;
    db_path.push(".mantissa");

    std::fs::create_dir_all(&db_path).context("Failed to create .mantissa directory")?;

    db_path.push("mantissa.redb");

    let db = Database::create(&db_path)
        .with_context(|| format!("Failed to create database at {:?}", db_path))?;

    Ok(db)
}
