use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, ReadableTable, TableDefinition};
use std::io;
use uuid::Uuid;

const T_LOCAL: TableDefinition<&'static str, &'static str> = TableDefinition::new("local");
const ROOT_SCHEMA_GENERATION_KEY: &str = "root_schema_generation";

// create tables if missing
fn create_local_tables(db: &Database) -> io::Result<()> {
    with_write_tx(db, |tx| {
        let _ = tx.open_table(T_LOCAL).map_err(into_io)?;
        Ok(())
    })
}

pub fn load_or_create_node_id(db: &Database) -> io::Result<Uuid> {
    create_local_tables(db)?;

    // Try reading first.
    if let Some(existing) = with_read_tx(db, |tx| {
        let table = tx.open_table(T_LOCAL).map_err(into_io)?;
        let node_id = table
            .get("node_id")
            .map_err(into_io)?
            .and_then(|guard| Uuid::parse_str(guard.value()).ok());
        Ok(node_id)
    })? {
        return Ok(existing);
    }

    // Create a new UUIDv7 and persist as text
    let id = Uuid::now_v7();
    let id_str = id.to_string();

    with_write_tx(db, |tx| {
        let mut table = tx.open_table(T_LOCAL).map_err(into_io)?;
        table.insert("node_id", id_str.as_str()).map_err(into_io)?;
        Ok(())
    })?;

    Ok(id)
}

/// Advances and returns the durable root-schema publication generation for this node.
pub fn next_root_schema_publication_generation(db: &Database) -> io::Result<u64> {
    create_local_tables(db)?;

    with_write_tx(db, |tx| {
        let mut table = tx.open_table(T_LOCAL).map_err(into_io)?;
        let current = table
            .get(ROOT_SCHEMA_GENERATION_KEY)
            .map_err(into_io)?
            .and_then(|guard| guard.value().parse::<u64>().ok())
            .unwrap_or_default();
        let next = current.saturating_add(1).max(1);
        let next_str = next.to_string();
        table
            .insert(ROOT_SCHEMA_GENERATION_KEY, next_str.as_str())
            .map_err(into_io)?;
        Ok(next)
    })
}

#[cfg(test)]
mod tests {
    use super::next_root_schema_publication_generation;

    /// Root-schema publication generation must advance durably across store opens.
    #[test]
    fn root_schema_publication_generation_persists_and_increments() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("state.redb");

        {
            let db = redb::Database::create(&db_path).expect("create redb");
            assert_eq!(
                next_root_schema_publication_generation(&db).expect("first generation"),
                1
            );
        }

        let db = redb::Database::create(&db_path).expect("reopen redb");
        assert_eq!(
            next_root_schema_publication_generation(&db).expect("second generation"),
            2
        );
    }
}
