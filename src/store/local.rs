use redb::{Database, TableDefinition};
use std::io;
use uuid::Uuid;

const T_LOCAL: TableDefinition<&'static str, &'static str> = TableDefinition::new("local");

fn into_io<E: std::error::Error>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

// create tables if missing
fn create_local_tables(db: &Database) -> io::Result<()> {
    let w = db.begin_write().map_err(into_io)?;
    let _ = w.open_table(T_LOCAL).map_err(into_io)?;
    w.commit().map_err(into_io)?;
    Ok(())
}

pub fn load_or_create_node_id(db: &Database) -> io::Result<Uuid> {
    create_local_tables(db)?;

    // Try reading first.
    {
        let r = db.begin_read().map_err(into_io)?;
        let t = r.open_table(T_LOCAL).map_err(into_io)?;
        if let Some(g) = t.get("node_id").map_err(into_io)? {
            let s: &str = g.value(); // borrowed from DB page
            if let Ok(id) = Uuid::parse_str(s) {
                return Ok(id);
            }
        }
    }

    // Create a new UUIDv7 and persist as text
    let id = Uuid::now_v7();
    let id_str = id.to_string();

    let w = db.begin_write().map_err(into_io)?;
    {
        let mut t = w.open_table(T_LOCAL).map_err(into_io)?;
        t.insert("node_id", id_str.as_str()).map_err(into_io)?;
    }
    w.commit().map_err(into_io)?;

    Ok(id)
}
