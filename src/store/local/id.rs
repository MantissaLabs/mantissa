use crate::store::tx::{into_io, with_read_tx, with_write_tx};
use redb::{Database, TableDefinition};
use std::io;
use uuid::Uuid;

const T_LOCAL: TableDefinition<&'static str, &'static str> = TableDefinition::new("local");

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
