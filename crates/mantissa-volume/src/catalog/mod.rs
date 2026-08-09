//! Durable local replica records and storage-pool accounting.
//!
//! Each replica holds its full logical data size, two checked headers, and a
//! bounded changed-region log. Repair writes directly into reserved replica
//! space and does not require another temporary copy.

mod error;
mod model;
mod pool;
mod protocol;
mod store;

pub use error::{CatalogError, PoolError};
pub use model::{
    InvalidLocalReplicaOrigin, InvalidSavedVolumeMount, LocalAttachmentRecord, LocalReplicaOrigin,
    LocalReplicaRetirement, PoolSpaceState, PoolStatus, ReplicaHealth, ReplicaKey, ReplicaRecord,
    ReplicaState, ReservedSpace, SavedFilesystemFormat, SavedMountState, SavedUblkDevice,
    SavedVolumeMount,
};
pub use pool::{PoolFilesystem, ReplicaPool};
pub use protocol::CatalogProtocolError;
pub use store::ReplicaCatalog;
