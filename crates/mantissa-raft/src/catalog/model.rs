use std::error::Error;

use mantissa_protocol::raft::raft_group_id;
use openraft::{EmptyNode, LogId, NodeId, StoredMembership, Vote};

/// Converts an application-defined Raft group ID without choosing its format.
pub trait GroupIdAdapter<GID>: Send + Sync {
    /// Error returned when a group ID cannot be converted.
    type Error: Error + Send + Sync + 'static;

    /// Writes one group ID directly into its Cap'n Proto field.
    fn write(&self, builder: raft_group_id::Builder<'_>, group_id: &GID)
    -> Result<(), Self::Error>;

    /// Reads and checks one owned group ID.
    fn read(&self, reader: raft_group_id::Reader<'_>) -> Result<GID, Self::Error>;
}

/// Decides whether startup should leave a discovered group idle or start it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupActivation {
    /// Keep the group durable but do not create its runtime.
    #[default]
    Inactive,

    /// Start the group before serving requests that can reach it.
    Active,
}

/// Durable identity and startup state for one local Raft group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupRecord<GID, NID>
where
    NID: NodeId,
{
    group_id: GID,
    activation: GroupActivation,
    vote: Option<Vote<NID>>,
    membership: Option<StoredMembership<NID, EmptyNode>>,
    applied_log_id: Option<LogId<NID>>,
}

impl<GID, NID> GroupRecord<GID, NID>
where
    NID: NodeId,
{
    /// Creates the first durable record for one group.
    pub(super) fn new(group_id: GID, activation: GroupActivation) -> Self {
        Self {
            group_id,
            activation,
            vote: None,
            membership: None,
            applied_log_id: None,
        }
    }

    /// Rebuilds one checked record read from the catalog.
    pub(crate) fn from_stored_parts(
        group_id: GID,
        activation: GroupActivation,
        vote: Option<Vote<NID>>,
        membership: Option<StoredMembership<NID, EmptyNode>>,
        applied_log_id: Option<LogId<NID>>,
    ) -> Self {
        Self {
            group_id,
            activation,
            vote,
            membership,
            applied_log_id,
        }
    }

    /// Returns the application-defined group ID.
    #[must_use]
    pub const fn group_id(&self) -> &GID {
        &self.group_id
    }

    /// Returns whether startup should leave this group idle or start it.
    #[must_use]
    pub const fn activation(&self) -> GroupActivation {
        self.activation
    }

    /// Returns the latest vote saved by this group.
    #[must_use]
    pub const fn vote(&self) -> Option<&Vote<NID>> {
        self.vote.as_ref()
    }

    /// Returns the latest membership and its log position.
    #[must_use]
    pub const fn membership(&self) -> Option<&StoredMembership<NID, EmptyNode>> {
        self.membership.as_ref()
    }

    /// Returns the newest entry applied by the local state machine.
    #[must_use]
    pub const fn applied_log_id(&self) -> Option<&LogId<NID>> {
        self.applied_log_id.as_ref()
    }

    /// Replaces the startup activation state.
    pub(super) fn set_activation(&mut self, activation: GroupActivation) {
        self.activation = activation;
    }

    /// Replaces the latest saved vote.
    pub(super) fn set_vote(&mut self, vote: Vote<NID>) {
        self.vote = Some(vote);
    }

    /// Replaces the latest stored membership.
    pub(super) fn set_membership(&mut self, membership: StoredMembership<NID, EmptyNode>) {
        self.membership = Some(membership);
    }

    /// Replaces the newest applied log ID.
    pub(super) fn set_applied_log_id(&mut self, log_id: LogId<NID>) {
        self.applied_log_id = Some(log_id);
    }
}
