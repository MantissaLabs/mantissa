use openraft::{NodeId, RaftMetrics, ServerState};

/// Stable runtime states exposed by the Mantissa Raft boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupState {
    /// The node receives replicated data but does not vote.
    Learner,

    /// The node follows the current leader.
    Follower,

    /// The node is campaigning for leadership.
    Candidate,

    /// The node is the current leader.
    Leader,

    /// The node is shutting down.
    Shutdown,
}

/// Small typed snapshot of the metrics needed by group users.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupMetrics<NID>
where
    NID: NodeId,
{
    /// Identity of the local group member.
    pub node_id: NID,

    /// Current Raft term.
    pub term: u64,

    /// Current local runtime state.
    pub state: GroupState,

    /// Leader currently known by this node.
    pub leader: Option<NID>,

    /// Highest log index stored by this node.
    pub last_log_index: Option<u64>,

    /// Highest log index applied by this node.
    pub last_applied_index: Option<u64>,

    /// Highest log index included in the current snapshot.
    pub snapshot_index: Option<u64>,
}

impl<NID> GroupMetrics<NID>
where
    NID: NodeId,
{
    /// Converts OpenRaft's complete metrics into the stable public subset.
    pub(crate) fn from_openraft<N>(metrics: &RaftMetrics<NID, N>) -> Self
    where
        N: openraft::Node,
    {
        let state = match metrics.state {
            ServerState::Learner => GroupState::Learner,
            ServerState::Follower => GroupState::Follower,
            ServerState::Candidate => GroupState::Candidate,
            ServerState::Leader => GroupState::Leader,
            ServerState::Shutdown => GroupState::Shutdown,
        };

        Self {
            node_id: metrics.id.clone(),
            term: metrics.current_term,
            state,
            leader: metrics.current_leader.clone(),
            last_log_index: metrics.last_log_index,
            last_applied_index: metrics.last_applied.as_ref().map(|log_id| log_id.index),
            snapshot_index: metrics.snapshot.as_ref().map(|log_id| log_id.index),
        }
    }
}
