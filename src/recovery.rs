use crate::store::path::open_state_database;
use crate::store::peer_store::open_peers_store;
use anyhow::{Context, Result, bail};
use crdt_store::uuid_key::UuidKey;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

const STATE_DB_FILE: &str = "state.redb";
const LOCAL_NODE_ID_KEY: &str = "node_id";
const ROOT_SCHEMA_GENERATION_KEY: &str = "root_schema_generation";
const IDENTITY_FILE_NAMES: &[&str] = &[
    "noise.key",
    "ed25519.key",
    "wireguard.key",
    "wireguard.port",
];

const T_LOCAL: TableDefinition<&'static str, &'static str> = TableDefinition::new("local");
const T_LOCAL_SESSIONS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_tickets_local");
const T_LOCAL_CREDS: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("session_credentials_local");
const T_SERVER_TICKETS: TableDefinition<&'static [u8], &'static [u8]> =
    TableDefinition::new("session_ticket_records");
const T_SERVER_REVERSE: TableDefinition<[u8; 16], &'static [u8]> =
    TableDefinition::new("peer_to_session_ticket");

/// Convert one Redb error into the identity reset I/O surface.
fn into_io<E: std::error::Error>(error: E) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

/// Options for preparing one copied state directory to start as a new node identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetIdentityOptions {
    pub state_dir: Option<PathBuf>,
}

/// Summary for one Redb table touched by identity reset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetTableReport {
    name: &'static str,
    removed_rows: usize,
}

/// Operator-facing summary of one identity reset pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResetIdentityReport {
    state_dir: PathBuf,
    db_path: PathBuf,
    previous_node_id: Option<Uuid>,
    local_peer_row_purged: bool,
    present_identity_files: Vec<PathBuf>,
    absent_identity_files: Vec<PathBuf>,
    removed_local_records: usize,
    cleared_tables: Vec<ResetTableReport>,
}

impl fmt::Display for ResetIdentityReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "init: reset local node identity in {}",
            self.state_dir.display()
        )?;
        writeln!(f, "state database: {}", self.db_path.display())?;
        match self.previous_node_id {
            Some(node_id) => {
                writeln!(f, "previous node id: {node_id}")?;
                writeln!(
                    f,
                    "previous local peer row purged: {}",
                    self.local_peer_row_purged
                )?;
            }
            None => {
                writeln!(f, "previous node id: none")?;
            }
        }

        if self.present_identity_files.is_empty() {
            writeln!(f, "identity files: none present")?;
        } else {
            writeln!(f, "identity files removed:")?;
            for path in &self.present_identity_files {
                writeln!(f, "  - {}", path.display())?;
            }
        }
        if !self.absent_identity_files.is_empty() {
            writeln!(
                f,
                "identity files already absent: {}",
                self.absent_identity_files.len()
            )?;
        }

        writeln!(
            f,
            "local identity records removed: {}",
            self.removed_local_records
        )?;

        for table in &self.cleared_tables {
            writeln!(f, "{} rows removed: {}", table.name, table.removed_rows)?;
        }

        writeln!(
            f,
            "next: join an active cluster with `mantissa join --anchor <addr> --join-token <token>`"
        )?;

        Ok(())
    }
}

/// Prepare a copied state directory to start as a distinct node.
pub async fn reset_identity(options: ResetIdentityOptions) -> Result<ResetIdentityReport> {
    let state_dir = match options.state_dir {
        Some(path) => path,
        None => net::paths::resolve_state_dir_path().context("resolve default state directory")?,
    };

    if !state_dir.exists() {
        bail!("state directory does not exist: {}", state_dir.display());
    }
    if !state_dir.is_dir() {
        bail!("state path is not a directory: {}", state_dir.display());
    }

    let db_path = state_dir.join(STATE_DB_FILE);
    if !db_path.exists() {
        bail!("state database does not exist: {}", db_path.display());
    }

    let (present_identity_files, absent_identity_files) = identity_file_status(&state_dir);
    for path in &present_identity_files {
        fs::remove_file(path)
            .with_context(|| format!("remove identity file {}", path.display()))?;
    }

    let db =
        Arc::new(open_state_database(&db_path).context("open state database for identity reset")?);
    let previous_node_id = read_local_node_id(&db).context("read previous local node id")?;
    let local_peer_row_purged = purge_previous_local_peer(&db, previous_node_id)
        .await
        .context("purge previous local peer row")?;
    let removed_local_records =
        reset_local_identity_records(&db).context("reset local identity records")?;
    let cleared_tables = vec![
        clear_uuid_keyed_table(&db, T_LOCAL_SESSIONS, "session_tickets_local")
            .context("clear local session tickets")?,
        clear_uuid_keyed_table(&db, T_LOCAL_CREDS, "session_credentials_local")
            .context("clear local credentials")?,
        clear_bytes_keyed_table(&db, T_SERVER_TICKETS, "session_ticket_records")
            .context("clear server session tickets")?,
        clear_uuid_keyed_table(&db, T_SERVER_REVERSE, "peer_to_session_ticket")
            .context("clear server session reverse index")?,
    ];

    Ok(ResetIdentityReport {
        state_dir,
        db_path,
        previous_node_id,
        local_peer_row_purged,
        present_identity_files,
        absent_identity_files,
        removed_local_records,
        cleared_tables,
    })
}

/// Classify identity-bearing files that are present or absent in `state_dir`.
fn identity_file_status(state_dir: &Path) -> (Vec<PathBuf>, Vec<PathBuf>) {
    let mut present = Vec::new();
    let mut absent = Vec::new();

    for name in IDENTITY_FILE_NAMES {
        let path = state_dir.join(name);
        if path.exists() {
            present.push(path);
        } else {
            absent.push(path);
        }
    }

    (present, absent)
}

/// Read the node id that belongs to the copied state before identity reset clears it.
fn read_local_node_id(db: &Database) -> std::io::Result<Option<Uuid>> {
    let r = db.begin_read().map_err(into_io)?;
    let table = match r.open_table(T_LOCAL) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(error) => return Err(into_io(error)),
    };
    let Some(raw) = table.get(LOCAL_NODE_ID_KEY).map_err(into_io)? else {
        return Ok(None);
    };

    Uuid::parse_str(raw.value())
        .map(Some)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))
}

/// Locally forget the copied node's old peer row without publishing a CRDT tombstone.
async fn purge_previous_local_peer(
    db: &Arc<Database>,
    previous_node_id: Option<Uuid>,
) -> Result<bool> {
    let Some(previous_node_id) = previous_node_id else {
        return Ok(false);
    };

    let key = UuidKey::from(previous_node_id);
    let peers = open_peers_store(db.clone(), previous_node_id).context("open peer store")?;
    let existed = peers
        .exists(&key)
        .context("check previous local peer row")?
        || peers
            .has_tombstone(&key)
            .context("check previous local peer tombstone")?;
    peers
        .purge_local(&key)
        .await
        .context("purge previous local peer row")?;
    Ok(existed)
}

/// Remove local identity rows that must not survive clone-based restore.
fn reset_local_identity_records(db: &Database) -> std::io::Result<usize> {
    let w = db.begin_write().map_err(into_io)?;
    let mut removed = 0usize;
    {
        let mut table = w.open_table(T_LOCAL).map_err(into_io)?;
        for key in [LOCAL_NODE_ID_KEY, ROOT_SCHEMA_GENERATION_KEY] {
            if table.remove(key).map_err(into_io)?.is_some() {
                removed = removed.saturating_add(1);
            }
        }
    }
    w.commit().map_err(into_io)?;
    Ok(removed)
}

/// Remove every row from a table keyed by peer UUID bytes.
fn clear_uuid_keyed_table(
    db: &Database,
    table_def: TableDefinition<[u8; 16], &'static [u8]>,
    name: &'static str,
) -> std::io::Result<ResetTableReport> {
    let w = db.begin_write().map_err(into_io)?;
    let mut removed = 0usize;
    {
        let mut table = w.open_table(table_def).map_err(into_io)?;
        let keys = {
            let mut keys = Vec::new();
            for entry in table.iter().map_err(into_io)? {
                let (key, _) = entry.map_err(into_io)?;
                keys.push(key.value());
            }
            keys
        };

        for key in keys {
            if table.remove(key).map_err(into_io)?.is_some() {
                removed = removed.saturating_add(1);
            }
        }
    }
    w.commit().map_err(into_io)?;
    Ok(ResetTableReport {
        name,
        removed_rows: removed,
    })
}

/// Remove every row from a table keyed by variable-length ticket bytes.
fn clear_bytes_keyed_table(
    db: &Database,
    table_def: TableDefinition<&'static [u8], &'static [u8]>,
    name: &'static str,
) -> std::io::Result<ResetTableReport> {
    let w = db.begin_write().map_err(into_io)?;
    let mut removed = 0usize;
    {
        let mut table = w.open_table(table_def).map_err(into_io)?;
        let keys = {
            let mut keys = Vec::new();
            for entry in table.iter().map_err(into_io)? {
                let (key, _) = entry.map_err(into_io)?;
                keys.push(key.value().to_vec());
            }
            keys
        };

        for key in keys {
            if table.remove(key.as_slice()).map_err(into_io)?.is_some() {
                removed = removed.saturating_add(1);
            }
        }
    }
    w.commit().map_err(into_io)?;
    Ok(ResetTableReport {
        name,
        removed_rows: removed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::local::{LocalTokenStore, SecretMasterStore, load_or_create_node_id};
    use crate::topology::peers::{PeerLabelState, PeerMembership, PeerSchedulingState, PeerValue};
    use redb::ReadableDatabase;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Build one deterministic peer row for reset fixtures.
    fn peer_value(node_id: Uuid, address: &str, byte: u8) -> PeerValue {
        PeerValue {
            address: address.to_string(),
            hostname: format!("node-{byte}"),
            platform_os: "linux".to_string(),
            platform_arch: "aarch64".to_string(),
            noise_static_pub: [byte; 32],
            signing_pub: [byte.saturating_add(1); 32],
            identity_sig: vec![byte.saturating_add(2); 64],
            wireguard: None,
            scheduling: PeerSchedulingState::schedulable_default(node_id),
            labels: PeerLabelState::default(),
            runtime_support: crate::runtime::types::RuntimeSupportProfile::default(),
            root_schema: crate::cluster::RootSchemaInfo::default(),
            membership: PeerMembership::active(1),
        }
    }

    /// Write representative clone-sensitive rows into the recovery tables.
    fn seed_recovery_state(db: &Database) {
        let w = db.begin_write().expect("write tx");
        {
            let mut local = w.open_table(T_LOCAL).expect("local");
            local
                .insert(ROOT_SCHEMA_GENERATION_KEY, "7")
                .expect("root generation");
        }
        {
            let mut sessions = w.open_table(T_LOCAL_SESSIONS).expect("sessions");
            sessions
                .insert([0x11; 16], b"sealed-ticket".as_slice())
                .expect("session");
        }
        {
            let mut creds = w.open_table(T_LOCAL_CREDS).expect("creds");
            creds
                .insert([0x22; 16], b"credential".as_slice())
                .expect("credential");
        }
        {
            let mut tickets = w.open_table(T_SERVER_TICKETS).expect("tickets");
            let ticket_record = [0x33u8; 32];
            tickets
                .insert(b"ticket".as_slice(), ticket_record.as_slice())
                .expect("ticket");
        }
        {
            let mut reverse = w.open_table(T_SERVER_REVERSE).expect("reverse");
            reverse
                .insert([0x44; 16], b"ticket".as_slice())
                .expect("reverse");
        }
        w.commit().expect("commit");
    }

    /// Count rows in one UUID-keyed recovery table.
    fn count_uuid_table(
        db: &Database,
        table_def: TableDefinition<[u8; 16], &'static [u8]>,
    ) -> usize {
        let r = db.begin_read().expect("read tx");
        let table = r.open_table(table_def).expect("open table");
        table.iter().expect("iter").count()
    }

    /// Count rows in one ticket-keyed recovery table.
    fn count_bytes_table(
        db: &Database,
        table_def: TableDefinition<&'static [u8], &'static [u8]>,
    ) -> usize {
        let r = db.begin_read().expect("read tx");
        let table = r.open_table(table_def).expect("open table");
        table.iter().expect("iter").count()
    }

    #[tokio::test]
    async fn reset_identity_removes_local_identity_and_preserves_cluster_secrets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let state_dir = dir.path();
        let db_path = state_dir.join(STATE_DB_FILE);
        let remote_id = Uuid::new_v4();
        let original_id;

        {
            let db = Arc::new(redb::Database::create(&db_path).expect("create db"));
            original_id = load_or_create_node_id(&db).expect("node id");
            let token_store = LocalTokenStore::new(db.clone()).expect("token store");
            token_store.write("MNTISA-1-abc234").expect("write token");
            let master_store = SecretMasterStore::new(db.clone()).expect("master store");
            let master = master_store.ensure_current().expect("master key");
            let peers = open_peers_store(db.clone(), original_id).expect("open peers");
            peers
                .upsert(
                    &UuidKey::from(original_id),
                    peer_value(original_id, "10.0.0.1:6578", 0x11),
                )
                .await
                .expect("write local peer");
            peers
                .upsert(
                    &UuidKey::from(remote_id),
                    peer_value(remote_id, "10.0.0.2:6578", 0x22),
                )
                .await
                .expect("write remote peer");
            seed_recovery_state(&db);
            assert_ne!(original_id, Uuid::nil());
            assert_eq!(master.version, 1);
        }

        for name in IDENTITY_FILE_NAMES {
            fs::write(state_dir.join(name), [0x55; 32]).expect("write identity file");
        }

        let report = reset_identity(ResetIdentityOptions {
            state_dir: Some(state_dir.to_path_buf()),
        })
        .await
        .expect("reset identity");

        assert_eq!(report.previous_node_id, Some(original_id));
        assert!(report.local_peer_row_purged);
        assert_eq!(
            report.present_identity_files.len(),
            IDENTITY_FILE_NAMES.len()
        );
        assert_eq!(report.removed_local_records, 2);
        for path in &report.present_identity_files {
            assert!(!path.exists(), "identity file should be removed: {path:?}");
        }

        let db = Arc::new(redb::Database::open(&db_path).expect("open db"));
        let new_id = load_or_create_node_id(&db).expect("new node id");
        assert_ne!(new_id, Uuid::nil());
        assert_ne!(new_id, original_id);

        let token_store = LocalTokenStore::new(db.clone()).expect("token store");
        assert_eq!(
            token_store.read().expect("token").as_deref(),
            Some("MNTISA-1-abc234")
        );
        let master_store = SecretMasterStore::new(db.clone()).expect("master store");
        assert_eq!(master_store.current().expect("master").version, 1);

        assert_eq!(count_uuid_table(&db, T_LOCAL_SESSIONS), 0);
        assert_eq!(count_uuid_table(&db, T_LOCAL_CREDS), 0);
        assert_eq!(count_bytes_table(&db, T_SERVER_TICKETS), 0);
        assert_eq!(count_uuid_table(&db, T_SERVER_REVERSE), 0);

        let peers = open_peers_store(db.clone(), new_id).expect("reopen peers");
        let old_key = UuidKey::from(original_id);
        assert!(
            !peers.exists(&old_key).expect("check old peer value"),
            "old local peer value must be purged without becoming a remote peer"
        );
        assert!(
            !peers.has_tombstone(&old_key).expect("check old peer tomb"),
            "identity reset must not publish a tombstone for the copied source peer"
        );
        assert!(
            peers
                .exists(&UuidKey::from(remote_id))
                .expect("check remote peer value"),
            "other peer rows from the copied store should be preserved"
        );
    }
}
