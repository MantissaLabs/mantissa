use std::collections::BTreeSet;
use std::sync::Arc;

use mantissa_raft::catalog::{CatalogError, GroupActivation, GroupCatalog, GroupIdAdapter};
use mantissa_raft::protocol::{NodeIdAdapter, ProtocolLimitSettings, ProtocolLimits};
use openraft::{CommittedLeaderId, EmptyNode, LogId, Membership, StoredMembership, Vote};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use tempfile::TempDir;
use thiserror::Error;

const KIB: usize = 1024;
const TEST_GROUP_LIMIT: usize = 64;
const RAW_GROUPS: TableDefinition<'static, &'static [u8], &'static [u8]> =
    TableDefinition::new("mantissa_raft_groups");

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestGroupId(u128);

#[derive(Clone, Copy, Debug, Default)]
struct TestGroupIdAdapter;

impl GroupIdAdapter<TestGroupId> for TestGroupIdAdapter {
    type Error = IdError;

    /// Writes a fixed-width group ID.
    fn write(
        &self,
        mut builder: mantissa_protocol::raft::raft_group_id::Builder<'_>,
        group_id: &TestGroupId,
    ) -> Result<(), Self::Error> {
        builder.set_value(&group_id.0.to_be_bytes());
        Ok(())
    }

    /// Reads a group ID only from its fixed-width representation.
    fn read(
        &self,
        reader: mantissa_protocol::raft::raft_group_id::Reader<'_>,
    ) -> Result<TestGroupId, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 16] = bytes.try_into().map_err(|_| IdError::InvalidLength {
            expected: 16,
            actual: bytes.len(),
        })?;
        Ok(TestGroupId(u128::from_be_bytes(bytes)))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct U64NodeIdAdapter;

impl NodeIdAdapter<u64> for U64NodeIdAdapter {
    type Error = IdError;

    /// Writes a fixed-width member ID.
    fn write(
        &self,
        mut builder: mantissa_protocol::raft::node_id::Builder<'_>,
        node_id: &u64,
    ) -> Result<(), Self::Error> {
        builder.set_value(&node_id.to_be_bytes());
        Ok(())
    }

    /// Reads a member ID only from its fixed-width representation.
    fn read(
        &self,
        reader: mantissa_protocol::raft::node_id::Reader<'_>,
    ) -> Result<u64, Self::Error> {
        let bytes = reader.get_value()?;
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| IdError::InvalidLength {
            expected: 8,
            actual: bytes.len(),
        })?;
        Ok(u64::from_be_bytes(bytes))
    }
}

#[derive(Debug, Error)]
enum IdError {
    #[error("could not read ID")]
    Capnp(#[from] capnp::Error),

    #[error("ID must contain {expected} bytes, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
}

type TestCatalog = GroupCatalog<TestGroupId, u64, TestGroupIdAdapter, U64NodeIdAdapter>;

/// Returns strict test-only protocol limits.
fn limits() -> ProtocolLimits {
    ProtocolLimits::new(ProtocolLimitSettings {
        max_message_bytes: 64 * KIB,
        max_entry_bytes: 32 * KIB,
        max_append_entries: 8,
        max_membership_nodes: 8,
        max_traversal_bytes: 128 * KIB,
        max_nesting_levels: 32,
    })
    .expect("test protocol limits must be valid")
}

/// Creates one shared Redb database for a catalog test.
fn database(directory: &TempDir) -> Arc<redb::Database> {
    Arc::new(
        redb::Database::create(directory.path().join("raft.redb"))
            .expect("create test Redb database"),
    )
}

/// Opens the test catalog on one shared Redb database.
fn open_catalog(database: Arc<redb::Database>) -> TestCatalog {
    GroupCatalog::open(database, TestGroupIdAdapter, U64NodeIdAdapter, limits())
        .expect("open test Raft catalog")
}

/// Creates a log ID with a preserved leader member ID.
fn log_id(term: u64, member_id: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, member_id), index)
}

/// Creates one stored membership at the requested log position.
fn stored_membership(
    index: u64,
    voters: &[u64],
    members: &[u64],
) -> StoredMembership<u64, EmptyNode> {
    let voters = voters.iter().copied().collect::<BTreeSet<_>>();
    let members = members.iter().copied().collect::<BTreeSet<_>>();
    StoredMembership::new(
        Some(log_id(7, 2, index)),
        Membership::new(vec![voters], members),
    )
}

/// Reads every raw key and value from the private test database.
fn raw_rows(database: &redb::Database) -> Vec<(Vec<u8>, Vec<u8>)> {
    let read = database.begin_read().expect("start raw read");
    let table = read.open_table(RAW_GROUPS).expect("open raw table");
    table
        .iter()
        .expect("iterate raw table")
        .map(|row| {
            let (key, value) = row.expect("read raw row");
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect()
}

/// Replaces one raw durable row to simulate damaged local storage.
fn replace_raw_row(database: &redb::Database, key: &[u8], value: &[u8]) {
    let write = database.begin_write().expect("start raw write");
    {
        let mut table = write.open_table(RAW_GROUPS).expect("open raw table");
        table.insert(key, value).expect("replace raw row");
    }
    write.commit().expect("commit raw row");
}

#[test]
fn vote_membership_and_activation_survive_restart() {
    let directory = TempDir::new().expect("create test directory");
    let group_id = TestGroupId(11);
    let vote = Vote::new_committed(7, 2);
    let membership = stored_membership(9, &[1, 2, 3], &[1, 2, 3, 4]);
    let applied_log_id = log_id(7, 2, 10);

    let database = database(&directory);
    let catalog = open_catalog(Arc::clone(&database));
    catalog
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create group");
    catalog.save_vote(&group_id, &vote).expect("save vote");
    catalog
        .save_membership(&group_id, &membership)
        .expect("save membership");
    catalog
        .save_applied_log_id(&group_id, &applied_log_id)
        .expect("save applied log ID");
    catalog
        .set_activation(&group_id, GroupActivation::Active)
        .expect("activate group");
    drop(catalog);
    drop(database);

    let database = Arc::new(
        redb::Database::open(directory.path().join("raft.redb")).expect("reopen test database"),
    );
    let catalog = open_catalog(database);
    let records = catalog
        .discover_groups(TEST_GROUP_LIMIT)
        .expect("discover durable groups");
    assert_eq!(1, records.len());
    let record = &records[0];
    assert_eq!(&group_id, record.group_id());
    assert_eq!(GroupActivation::Active, record.activation());
    assert_eq!(Some(&vote), record.vote());
    assert_eq!(Some(&membership), record.membership());
    assert_eq!(Some(&applied_log_id), record.applied_log_id());
}

#[test]
fn ensuring_an_existing_group_does_not_reset_it() {
    let directory = TempDir::new().expect("create test directory");
    let catalog = open_catalog(database(&directory));
    let group_id = TestGroupId(12);
    let vote = Vote::new_committed(4, 1);

    catalog
        .ensure_group(&group_id, GroupActivation::Active)
        .expect("create active group");
    catalog.save_vote(&group_id, &vote).expect("save vote");
    let existing = catalog
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("load existing group");

    assert_eq!(GroupActivation::Active, existing.activation());
    assert_eq!(Some(&vote), existing.vote());
}

#[test]
fn bounded_group_admission_preserves_restartable_catalog_size() {
    let directory = TempDir::new().expect("create test directory");
    let catalog = open_catalog(database(&directory));
    let first = TestGroupId(120);
    let second = TestGroupId(121);

    catalog
        .ensure_group_bounded(&first, GroupActivation::Active, 1)
        .expect("create first bounded group");
    catalog
        .ensure_group_bounded(&first, GroupActivation::Inactive, 1)
        .expect("retry existing bounded group");
    let error = catalog
        .ensure_group_bounded(&second, GroupActivation::Inactive, 1)
        .expect_err("second bounded group must be rejected");

    assert!(matches!(
        error,
        CatalogError::TooManyGroups {
            actual: 2,
            maximum: 1
        }
    ));
    assert_eq!(catalog.group_count().expect("count groups"), 1);
    assert_eq!(
        catalog
            .discover_groups(1)
            .expect("bounded catalog remains restartable")
            .len(),
        1
    );
}

#[test]
fn only_an_inactive_group_can_be_removed() {
    let directory = TempDir::new().expect("create test directory");
    let catalog = open_catalog(database(&directory));
    let group_id = TestGroupId(13);
    catalog
        .ensure_group(&group_id, GroupActivation::Active)
        .expect("create active group");

    assert!(matches!(
        catalog.remove_inactive_group(&group_id),
        Err(CatalogError::GroupActive)
    ));
    catalog
        .set_activation(&group_id, GroupActivation::Inactive)
        .expect("stop test group");
    assert!(
        catalog
            .remove_inactive_group(&group_id)
            .expect("remove stopped group")
    );
    assert!(
        !catalog
            .remove_inactive_group(&group_id)
            .expect("repeat stopped group removal")
    );
    assert!(
        catalog
            .group(&group_id)
            .expect("read removed group")
            .is_none()
    );
}

#[test]
fn ensuring_many_groups_does_not_reset_existing_rows() {
    let directory = TempDir::new().expect("create test directory");
    let catalog = open_catalog(database(&directory));
    let existing = TestGroupId(17);
    let vote = Vote::new_committed(4, 2);
    catalog
        .ensure_group(&existing, GroupActivation::Active)
        .expect("create existing group");
    catalog.save_vote(&existing, &vote).expect("save vote");

    catalog
        .ensure_groups([
            (existing, GroupActivation::Inactive),
            (TestGroupId(18), GroupActivation::Inactive),
        ])
        .expect("ensure group batch");

    let record = catalog
        .group(&existing)
        .expect("read existing group")
        .expect("existing group must remain");
    assert_eq!(GroupActivation::Active, record.activation());
    assert_eq!(Some(&vote), record.vote());
}

#[test]
fn stale_and_conflicting_updates_are_rejected() {
    let directory = TempDir::new().expect("create test directory");
    let catalog = open_catalog(database(&directory));
    let group_id = TestGroupId(13);
    catalog
        .ensure_group(&group_id, GroupActivation::Inactive)
        .expect("create group");

    catalog
        .save_vote(&group_id, &Vote::new_committed(8, 2))
        .expect("save current vote");
    let error = catalog
        .save_vote(&group_id, &Vote::new_committed(7, 2))
        .expect_err("a stale vote must fail");
    assert!(matches!(error, CatalogError::StaleVote));

    let current = stored_membership(10, &[1, 2, 3], &[1, 2, 3]);
    catalog
        .save_membership(&group_id, &current)
        .expect("save current membership");
    let stale = stored_membership(9, &[1, 2, 3], &[1, 2, 3]);
    let error = catalog
        .save_membership(&group_id, &stale)
        .expect_err("a stale membership must fail");
    assert!(matches!(error, CatalogError::StaleMembership));

    let conflict = stored_membership(10, &[1, 2], &[1, 2, 3]);
    let error = catalog
        .save_membership(&group_id, &conflict)
        .expect_err("a conflicting membership must fail");
    assert!(matches!(error, CatalogError::ConflictingMembership));

    let applied = log_id(8, 2, 12);
    catalog
        .save_applied_log_id(&group_id, &applied)
        .expect("save current applied log ID");
    let error = catalog
        .save_applied_log_id(&group_id, &log_id(8, 2, 11))
        .expect_err("an older applied index must fail");
    assert!(matches!(error, CatalogError::StaleAppliedLogId));
    let error = catalog
        .save_applied_log_id(&group_id, &log_id(9, 3, 12))
        .expect_err("a conflicting applied log ID must fail");
    assert!(matches!(error, CatalogError::ConflictingAppliedLogId));
    let error = catalog
        .save_applied_log_id(&group_id, &log_id(7, 2, 13))
        .expect_err("an applied term cannot move backwards");
    assert!(matches!(error, CatalogError::StaleAppliedLogId));

    let stored = catalog
        .group(&group_id)
        .expect("read group")
        .expect("group must exist");
    assert_eq!(Some(&current), stored.membership());
    assert_eq!(Some(&applied), stored.applied_log_id());
}

#[test]
fn truncated_catalog_record_fails_discovery() {
    let directory = TempDir::new().expect("create test directory");
    let database = database(&directory);
    let catalog = open_catalog(Arc::clone(&database));
    catalog
        .ensure_group(&TestGroupId(14), GroupActivation::Inactive)
        .expect("create group");

    let mut rows = raw_rows(&database);
    let (key, mut value) = rows.pop().expect("one raw group row");
    value.truncate(value.len() - 8);
    replace_raw_row(&database, &key, &value);

    let error = catalog
        .discover_groups(TEST_GROUP_LIMIT)
        .expect_err("a truncated record must fail");
    assert!(matches!(error, CatalogError::Protocol(_)));
}

#[test]
fn mismatched_catalog_key_and_group_id_fail_discovery() {
    let directory = TempDir::new().expect("create test directory");
    let database = database(&directory);
    let catalog = open_catalog(Arc::clone(&database));
    catalog
        .ensure_group(&TestGroupId(15), GroupActivation::Inactive)
        .expect("create first group");
    catalog
        .ensure_group(&TestGroupId(16), GroupActivation::Inactive)
        .expect("create second group");

    let rows = raw_rows(&database);
    let (first_key, _) = &rows[0];
    let (_, second_value) = &rows[1];
    replace_raw_row(&database, first_key, second_value);

    let error = catalog
        .discover_groups(TEST_GROUP_LIMIT)
        .expect_err("a mismatched identity must fail");
    assert!(matches!(error, CatalogError::GroupIdentityMismatch));
}

#[test]
fn discovery_limit_is_checked_before_record_allocation() {
    let directory = TempDir::new().expect("create test directory");
    let catalog = open_catalog(database(&directory));
    for value in 20..23 {
        catalog
            .ensure_group(&TestGroupId(value), GroupActivation::Inactive)
            .expect("create idle group");
    }

    let error = catalog
        .discover_groups(2)
        .expect_err("an undersized discovery limit must fail");
    assert!(matches!(
        error,
        CatalogError::TooManyGroups {
            actual: 3,
            maximum: 2
        }
    ));
}

#[test]
fn idle_groups_share_one_database_handle_and_start_no_runtime() {
    let directory = TempDir::new().expect("create test directory");
    let database = database(&directory);
    let catalog = open_catalog(Arc::clone(&database));
    for value in 100..132 {
        catalog
            .ensure_group(&TestGroupId(value), GroupActivation::Inactive)
            .expect("create idle group");
    }

    let records = catalog
        .discover_groups(TEST_GROUP_LIMIT)
        .expect("discover idle groups");
    assert_eq!(32, records.len());
    assert!(
        records
            .iter()
            .all(|record| record.activation() == GroupActivation::Inactive
                && record.vote().is_none()
                && record.membership().is_none())
    );
    assert_eq!(2, Arc::strong_count(&database));
}
