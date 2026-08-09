use std::sync::Arc;

use capnp::message::{Builder, ReaderOptions};
use mantissa_protocol::volumes::local_volume_control_state;
use mantissa_raft::ApplyContext;
use parking_lot::Mutex;
use redb::{ReadableTable, TableDefinition};
use thiserror::Error;

use crate::catalog::ReplicaKey;
use crate::control_state::VolumeControlState;
use crate::protocol::{ProtocolError, decode_control_state};
use crate::state_machine::VolumeStorage;
use crate::storage::replica_file::io_admission::{
    AppliedVolumeStatePublicationError, AppliedVolumeStateRegistry,
};

const CONTROL_STATE_FORMAT_VERSION: u16 = 1;
const CONTROL_STATE_DIGEST_DOMAIN: &[u8] = b"mantissa volume control state v1";
const CONTROL_STATES: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_volume_control_states");

/// Control state and applied position shared with reconcilers and health checks.
struct SharedVolumeControlState {
    state: VolumeControlState,
    applied: Option<ApplyContext>,
}

/// Durable bounded control-state owner used by the refactored Raft application.
pub struct VolumeControlStateStore {
    database: Arc<redb::Database>,
    key: ReplicaKey,
    shared: Arc<Mutex<SharedVolumeControlState>>,
    saved_state: Option<Vec<u8>>,
    maximum_state_bytes: usize,
    applied_volume_states: Arc<AppliedVolumeStateRegistry>,
}

impl VolumeControlStateStore {
    /// Opens format-v1 control state and publishes recovered durable state.
    pub fn new(
        database: Arc<redb::Database>,
        key: ReplicaKey,
        maximum_state_bytes: usize,
        applied_volume_states: Arc<AppliedVolumeStateRegistry>,
    ) -> Result<(Self, VolumeControlStateReader, Option<ApplyContext>), VolumeControlStateStoreError>
    {
        let saved = load_control_state(&database, key, maximum_state_bytes)?;
        let (saved_state, state, applied) = match saved {
            Some(saved) => {
                let state = decode_control_state(&saved.state, maximum_state_bytes)?;
                validate_control_state_key(key, &state)?;
                applied_volume_states.publish(saved.applied, &state)?;
                (Some(saved.state), state, Some(saved.applied))
            }
            None => (None, VolumeControlState::default(), None),
        };
        let shared = Arc::new(Mutex::new(SharedVolumeControlState { state, applied }));
        let reader = VolumeControlStateReader {
            shared: Arc::clone(&shared),
        };
        Ok((
            Self {
                database,
                key,
                shared,
                saved_state,
                maximum_state_bytes,
                applied_volume_states,
            },
            reader,
            applied,
        ))
    }
}

impl VolumeStorage for VolumeControlStateStore {
    type Error = VolumeControlStateStoreError;

    /// Returns the exact control-state bytes saved with the latest local apply.
    fn saved_state(&self) -> Option<&[u8]> {
        self.saved_state.as_deref()
    }

    /// Returns the defensive volume control-state byte limit.
    fn max_state_bytes(&self) -> usize {
        self.maximum_state_bytes
    }

    /// Durably saves then publishes one committed control state atomically ordered.
    async fn apply(&mut self, context: ApplyContext, state: &[u8]) -> Result<(), Self::Error> {
        let next_state = decode_control_state(state, self.maximum_state_bytes)?;
        validate_control_state_key(self.key, &next_state)?;
        let database = Arc::clone(&self.database);
        let key = self.key;
        let saved_state = state.to_vec();
        let state_for_disk = saved_state.clone();
        let maximum_state_bytes = self.maximum_state_bytes;
        tokio::task::spawn_blocking(move || {
            save_control_state(
                &database,
                key,
                context,
                &state_for_disk,
                maximum_state_bytes,
            )
        })
        .await
        .map_err(VolumeControlStateStoreError::ApplyTask)??;
        self.applied_volume_states.publish(context, &next_state)?;
        *self.shared.lock() = SharedVolumeControlState {
            state: next_state,
            applied: Some(context),
        };
        self.saved_state = Some(saved_state);
        Ok(())
    }
}

/// Binds initialized control state to the exact durable Raft group row.
fn validate_control_state_key(
    expected: ReplicaKey,
    state: &VolumeControlState,
) -> Result<(), VolumeControlStateStoreError> {
    let Some(descriptor) = state.descriptor() else {
        return Ok(());
    };
    let actual = ReplicaKey::from(descriptor);
    if actual != expected {
        return Err(VolumeControlStateStoreError::WrongReplicaKey { expected, actual });
    }
    Ok(())
}

/// Read handle for the latest locally durable bounded control state.
#[derive(Clone)]
pub struct VolumeControlStateReader {
    shared: Arc<Mutex<SharedVolumeControlState>>,
}

impl VolumeControlStateReader {
    /// Returns a copy of current locally applied state.
    #[must_use]
    pub fn state(&self) -> VolumeControlState {
        self.shared.lock().state.clone()
    }

    /// Returns the application log entry saved with current control state.
    #[must_use]
    pub fn applied(&self) -> Option<ApplyContext> {
        self.shared.lock().applied
    }
}

/// Removes saved application state after the matching replica is deleted.
pub fn remove_control_state(
    database: &redb::Database,
    key: ReplicaKey,
) -> Result<(), VolumeControlStateStoreError> {
    let key = encode_key(key);
    let write = database.begin_write()?;
    let mut table = write.open_table(CONTROL_STATES)?;
    table.remove(key.as_slice())?;
    drop(table);
    write.commit()?;
    Ok(())
}

/// Returns the applied entry saved with one optional control-state row.
pub fn control_state_applied(
    database: &redb::Database,
    key: ReplicaKey,
    maximum_state_bytes: usize,
) -> Result<Option<ApplyContext>, VolumeControlStateStoreError> {
    Ok(load_control_state(database, key, maximum_state_bytes)?.map(|saved| saved.applied))
}

/// One checked row read from Redb.
struct SavedControlState {
    applied: ApplyContext,
    state: Vec<u8>,
}

/// Opens the table and reads one optional control-state row.
fn load_control_state(
    database: &redb::Database,
    key: ReplicaKey,
    maximum_state_bytes: usize,
) -> Result<Option<SavedControlState>, VolumeControlStateStoreError> {
    let key_bytes = encode_key(key);
    let write = database.begin_write()?;
    let table = write.open_table(CONTROL_STATES)?;
    let saved = table
        .get(key_bytes.as_slice())?
        .map(|value| value.value().to_vec());
    drop(table);
    write.commit()?;
    saved
        .map(|bytes| decode_saved_control_state(key, &bytes, maximum_state_bytes))
        .transpose()
}

/// Replaces one control-state row in a durable Redb transaction.
fn save_control_state(
    database: &redb::Database,
    key: ReplicaKey,
    applied: ApplyContext,
    state: &[u8],
    maximum_state_bytes: usize,
) -> Result<(), VolumeControlStateStoreError> {
    if state.len() > maximum_state_bytes {
        return Err(VolumeControlStateStoreError::StateTooLarge {
            actual: state.len(),
            maximum: maximum_state_bytes,
        });
    }
    let key_bytes = encode_key(key);
    let digest = control_state_digest(&key_bytes, applied, state);
    let mut message = Builder::new_default();
    let mut root = message.init_root::<local_volume_control_state::Builder<'_>>();
    root.set_format_version(CONTROL_STATE_FORMAT_VERSION);
    root.set_applied_term(applied.term());
    root.set_applied_log_index(applied.index());
    root.set_state(state);
    root.set_digest(&digest);
    let encoded = capnp::serialize::write_message_to_words(&message);

    let write = database.begin_write()?;
    let mut table = write.open_table(CONTROL_STATES)?;
    table.insert(key_bytes.as_slice(), encoded.as_slice())?;
    drop(table);
    write.commit()?;
    Ok(())
}

/// Decodes and verifies one control-state row before it reaches Raft.
fn decode_saved_control_state(
    key: ReplicaKey,
    bytes: &[u8],
    maximum_state_bytes: usize,
) -> Result<SavedControlState, VolumeControlStateStoreError> {
    let maximum_record_bytes = maximum_state_bytes.saturating_add(1024);
    if bytes.len() > maximum_record_bytes {
        return Err(VolumeControlStateStoreError::StateTooLarge {
            actual: bytes.len(),
            maximum: maximum_record_bytes,
        });
    }
    let mut options = ReaderOptions::new();
    options.nesting_limit(8);
    options.traversal_limit_in_words(Some(
        bytes
            .len()
            .saturating_add(7)
            .saturating_div(8)
            .saturating_mul(4),
    ));
    let mut remaining = bytes;
    let message = capnp::serialize::read_message_from_flat_slice(&mut remaining, options)?;
    if !remaining.is_empty() {
        return Err(VolumeControlStateStoreError::TrailingBytes);
    }
    let root = message.get_root::<local_volume_control_state::Reader<'_>>()?;
    if root.get_format_version() != CONTROL_STATE_FORMAT_VERSION {
        return Err(VolumeControlStateStoreError::UnsupportedFormat(
            root.get_format_version(),
        ));
    }
    let state = root.get_state()?.to_vec();
    if state.is_empty() {
        return Err(VolumeControlStateStoreError::EmptyState);
    }
    if state.len() > maximum_state_bytes {
        return Err(VolumeControlStateStoreError::StateTooLarge {
            actual: state.len(),
            maximum: maximum_state_bytes,
        });
    }
    let applied = ApplyContext::new(root.get_applied_term(), root.get_applied_log_index());
    let digest: [u8; 32] = root
        .get_digest()?
        .try_into()
        .map_err(|_| VolumeControlStateStoreError::InvalidDigestLength)?;
    let expected = control_state_digest(&encode_key(key), applied, &state);
    if digest != expected {
        return Err(VolumeControlStateStoreError::DigestMismatch);
    }
    Ok(SavedControlState { applied, state })
}

/// Binds saved state to its exact volume generation and applied log entry.
fn control_state_digest(key: &[u8; 24], applied: ApplyContext, state: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONTROL_STATE_DIGEST_DOMAIN);
    hasher.update(key);
    hasher.update(&applied.term().to_le_bytes());
    hasher.update(&applied.index().to_le_bytes());
    hasher.update(&(state.len() as u64).to_le_bytes());
    hasher.update(state);
    *hasher.finalize().as_bytes()
}

/// Encodes the stable volume UUID and generation used as the Redb key.
fn encode_key(key: ReplicaKey) -> [u8; 24] {
    let mut encoded = [0_u8; 24];
    encoded[..16].copy_from_slice(key.volume_id().as_bytes());
    encoded[16..].copy_from_slice(&key.generation().get().to_be_bytes());
    encoded
}

/// Reports why local control state could not be opened or saved.
#[derive(Debug, Error)]
pub enum VolumeControlStateStoreError {
    /// Initialized control state belongs to another volume generation.
    #[error("volume control state key {actual:?} differs from local group {expected:?}")]
    WrongReplicaKey {
        /// Durable group whose row is being opened or changed.
        expected: ReplicaKey,

        /// Group derived from the control state descriptor.
        actual: ReplicaKey,
    },

    /// Redb could not start a transaction.
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),

    /// Redb could not open the control-state table.
    #[error(transparent)]
    Table(#[from] redb::TableError),

    /// Redb could not read or update a row.
    #[error(transparent)]
    Storage(#[from] redb::StorageError),

    /// Redb could not durably commit a state change.
    #[error(transparent)]
    Commit(#[from] redb::CommitError),

    /// Cap'n Proto could not decode the saved record.
    #[error(transparent)]
    Capnp(#[from] capnp::Error),

    /// The state machine produced or loaded invalid volume state.
    #[error(transparent)]
    State(#[from] ProtocolError),

    /// Durable control state could not be monotonically published in memory.
    #[error(transparent)]
    AppliedVolumeStatePublication(#[from] AppliedVolumeStatePublicationError),

    /// A control state exceeded its explicit limit.
    #[error("volume control state is {actual} bytes; maximum is {maximum}")]
    StateTooLarge { actual: usize, maximum: usize },

    /// The row uses a format this build does not support.
    #[error("unsupported volume control-state format {0}")]
    UnsupportedFormat(u16),

    /// One encoded record contains unused bytes after its message.
    #[error("volume control-state record contains trailing bytes")]
    TrailingBytes,

    /// A saved application state must contain one complete message.
    #[error("volume control-state record contains no application state")]
    EmptyState,

    /// The saved digest is not exactly 32 bytes.
    #[error("volume control-state digest must contain exactly 32 bytes")]
    InvalidDigestLength,

    /// The saved state or its identity was changed after commit.
    #[error("volume control-state digest does not match its contents")]
    DigestMismatch,

    /// The blocking Redb task stopped before returning its result.
    #[error("volume control-state task stopped during a committed apply")]
    ApplyTask(#[source] tokio::task::JoinError),
}

#[cfg(test)]
mod control_state_tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use mantissa_raft::ApplyContext;
    use uuid::Uuid;

    use super::{VolumeControlStateStore, VolumeControlStateStoreError};
    use crate::catalog::ReplicaKey;
    use crate::control_state::{InitializeVolume, VolumeCommand, VolumeControlState};
    use crate::protocol::encode_control_state;
    use crate::state_machine::VolumeStorage;
    use crate::storage::replica_file::io_admission::AppliedVolumeStateRegistry;
    use crate::{VolumeBlockSizes, VolumeDescriptor, VolumeGeneration, VolumeId, VolumeNodeId};

    /// Returns one deterministic node identity for persisted control state.
    fn node(value: u128) -> VolumeNodeId {
        VolumeNodeId::new(Uuid::from_u128(value)).expect("test node ID must be valid")
    }

    /// Returns one checked descriptor for the persisted generation.
    fn descriptor() -> VolumeDescriptor {
        VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(10)).expect("test volume ID must be valid"),
            VolumeGeneration::new(2).expect("test generation must be valid"),
            64 << 20,
            VolumeBlockSizes::supported(),
        )
        .expect("test descriptor must be valid")
    }

    /// Durable apply publishes control state and restart reconstructs the registry.
    #[tokio::test]
    async fn control_state_storage_publishes_only_durable_state_and_recovers_it() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join("control-state.redb");
        let database = Arc::new(
            redb::Database::create(&database_path).expect("create control state test database"),
        );
        let descriptor = descriptor();
        let key = ReplicaKey::from(&descriptor);
        let registry = Arc::new(AppliedVolumeStateRegistry::new());
        let (mut storage, reader, applied) = VolumeControlStateStore::new(
            Arc::clone(&database),
            key,
            64 << 10,
            Arc::clone(&registry),
        )
        .expect("open pristine control-state storage");
        assert_eq!(applied, None);
        assert!(registry.is_empty());

        let state = VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor: descriptor.clone(),
                initial_copies: BTreeSet::from([node(1), node(2), node(3)]),
            }))
            .state;
        let encoded =
            encode_control_state(&state, 64 << 10).expect("initialized control state must encode");
        VolumeStorage::apply(&mut storage, ApplyContext::new(3, 7), &encoded)
            .await
            .expect("control state apply must persist and publish");
        assert_eq!(reader.state(), state);
        assert_eq!(reader.applied(), Some(ApplyContext::new(3, 7)));
        assert_eq!(
            registry
                .cell(key)
                .expect("applied state cell must exist")
                .load()
                .applied,
            ApplyContext::new(3, 7)
        );
        drop(storage);
        drop(reader);
        drop(registry);
        drop(database);

        let reopened_database = Arc::new(
            redb::Database::open(database_path).expect("reopen control state test database"),
        );
        let reopened_registry = Arc::new(AppliedVolumeStateRegistry::new());
        let (_storage, reader, applied) = VolumeControlStateStore::new(
            reopened_database,
            key,
            64 << 10,
            Arc::clone(&reopened_registry),
        )
        .expect("reopen saved control-state storage");
        assert_eq!(applied, Some(ApplyContext::new(3, 7)));
        assert_eq!(reader.state(), state);
        assert_eq!(
            reopened_registry
                .cell(key)
                .expect("startup must republish saved control state")
                .load()
                .applied,
            ApplyContext::new(3, 7)
        );
    }

    /// A durable group never saves or publishes another descriptor's control state.
    #[tokio::test]
    async fn control_state_storage_rejects_a_mismatched_replica_key() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database = Arc::new(
            redb::Database::create(directory.path().join("control-state.redb"))
                .expect("create control state test database"),
        );
        let expected_descriptor = descriptor();
        let key = ReplicaKey::from(&expected_descriptor);
        let wrong_descriptor = VolumeDescriptor::new(
            VolumeId::new(Uuid::from_u128(11)).expect("test volume ID must be valid"),
            expected_descriptor.generation(),
            expected_descriptor.capacity().bytes(),
            VolumeBlockSizes::supported(),
        )
        .expect("mismatched descriptor must be valid");
        let wrong_key = ReplicaKey::from(&wrong_descriptor);
        let wrong_state = VolumeControlState::default()
            .evaluate(&VolumeCommand::Initialize(InitializeVolume {
                descriptor: wrong_descriptor,
                initial_copies: BTreeSet::from([node(1), node(2), node(3)]),
            }))
            .state;
        let encoded =
            encode_control_state(&wrong_state, 64 << 10).expect("control state must encode");
        let registry = Arc::new(AppliedVolumeStateRegistry::new());
        let (mut storage, reader, _) = VolumeControlStateStore::new(
            Arc::clone(&database),
            key,
            64 << 10,
            Arc::clone(&registry),
        )
        .expect("open pristine control-state storage");

        let error = VolumeStorage::apply(&mut storage, ApplyContext::new(1, 1), &encoded)
            .await
            .expect_err("mismatched control state must fail before persistence");
        assert!(matches!(
            error,
            VolumeControlStateStoreError::WrongReplicaKey { expected, actual }
                if expected == key && actual == wrong_key
        ));
        assert_eq!(reader.state(), VolumeControlState::default());
        assert_eq!(reader.applied(), None);
        assert!(registry.is_empty());

        super::save_control_state(&database, key, ApplyContext::new(1, 1), &encoded, 64 << 10)
            .expect("save deliberately mismatched control state row");
        let reopened = VolumeControlStateStore::new(
            Arc::clone(&database),
            key,
            64 << 10,
            Arc::new(AppliedVolumeStateRegistry::new()),
        );
        assert!(matches!(
            reopened,
            Err(VolumeControlStateStoreError::WrongReplicaKey { expected, actual })
                if expected == key && actual == wrong_key
        ));
    }
}
