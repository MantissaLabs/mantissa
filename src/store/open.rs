use std::io;
use std::sync::Arc;
use uuid::Uuid;

/// Opens one actor-scoped CRDT store and wraps the inner store in `Arc`.
///
/// This centralizes the common `open -> Arc::new` pattern across store wrappers
/// to keep each store module focused on table/type declarations.
pub fn open_arc_store<Store, Open>(
    db: Arc<redb::Database>,
    actor: Uuid,
    open: Open,
) -> io::Result<Arc<Store>>
where
    Open: FnOnce(Arc<redb::Database>, Uuid) -> Result<Store, Box<mantissa_store::error::Error>>,
{
    open(db, actor).map(Arc::new).map_err(io::Error::other)
}
