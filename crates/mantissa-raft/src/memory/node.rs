use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::{ChangeMembers, Config, LogId, Raft, ServerState, SnapshotPolicy};

use crate::{ApplicationStateMachine, EntryResponse, RaftApplication, TypeConfig};

use super::error::{GroupError, WaitError};
use super::log_store::MemoryLogStore;
use super::metrics::GroupMetrics;
use super::network::InProcessNetwork;
use super::state_machine::MemoryStateMachine;
use crate::state_machine_shutdown::StateMachineShutdown;

/// Result returned after a typed application command is committed and applied.
#[derive(Debug, Eq, PartialEq)]
pub struct WriteResult<NID, R>
where
    NID: openraft::NodeId,
{
    /// Committed Raft log ID assigned to the command.
    pub log_id: LogId<NID>,

    /// Deterministic response produced by the application state machine.
    pub response: R,
}

/// One non-durable member of an in-process Raft group.
///
/// The memory log requires cloneable commands because each read returns owned
/// entries. This constraint belongs only to this non-durable runtime.
///
/// Call [`Self::shutdown`] to join OpenRaft's tasks and remove the member from
/// its in-process network.
#[must_use = "an in-memory Raft node must be shut down explicitly"]
pub struct InMemoryNode<A>
where
    A: RaftApplication,
{
    node_id: A::NodeId,
    raft: Raft<TypeConfig<A>>,
    network: InProcessNetwork<A>,
    state_machine_shutdown: Arc<StateMachineShutdown>,
}

struct LeadershipMoveSettings<A>
where
    A: RaftApplication,
{
    nodes: Vec<Raft<TypeConfig<A>>>,
}

impl<A> LeadershipMoveSettings<A>
where
    A: RaftApplication,
{
    /// Selects one voter for the next election and pauses the old leader.
    fn choose_target(
        nodes: Vec<(A::NodeId, Raft<TypeConfig<A>>)>,
        source: &A::NodeId,
        target: &A::NodeId,
    ) -> Self {
        let mut saved = Vec::with_capacity(nodes.len());
        for (node_id, raft) in nodes {
            raft.runtime_config().elect(&node_id == target);
            if &node_id == source {
                raft.runtime_config().heartbeat(false);
            }
            saved.push(raft);
        }
        Self { nodes: saved }
    }
}

impl<A> Drop for LeadershipMoveSettings<A>
where
    A: RaftApplication,
{
    /// Restores the settings supplied when each in-memory member was started.
    fn drop(&mut self) {
        for raft in &self.nodes {
            raft.runtime_config().elect(raft.config().enable_elect);
            raft.runtime_config()
                .heartbeat(raft.config().enable_heartbeat);
        }
    }
}

impl<A> InMemoryNode<A>
where
    A: RaftApplication,
    A::Command: Clone,
{
    /// Starts one empty group member with typed in-memory storage.
    pub async fn start<S>(
        node_id: A::NodeId,
        config: Config,
        network: InProcessNetwork<A>,
        state_machine: S,
    ) -> Result<Self, GroupError<A::NodeId, A::Node>>
    where
        S: ApplicationStateMachine<A>,
    {
        if config.snapshot_policy != SnapshotPolicy::Never {
            return Err(GroupError::SnapshotsEnabled);
        }

        let config = config
            .validate()
            .map_err(GroupError::InvalidConfiguration)?;
        let factory = network.factory(node_id.clone());
        let log_store = MemoryLogStore::new();
        let (state_machine_shutdown, shutdown_owner) = StateMachineShutdown::pair();
        let state_machine = MemoryStateMachine::new(state_machine, shutdown_owner);
        let raft = Raft::new(
            node_id.clone(),
            Arc::new(config),
            factory,
            log_store,
            state_machine,
        )
        .await
        .map_err(GroupError::Start)?;

        if !network.register(node_id.clone(), raft.clone()) {
            let shutdown_result = raft.shutdown().await.map_err(GroupError::Shutdown);
            state_machine_shutdown.wait().await;
            shutdown_result?;
            return Err(GroupError::NodeAlreadyRegistered(node_id));
        }

        Ok(Self {
            node_id,
            raft,
            network,
            state_machine_shutdown,
        })
    }

    /// Returns this member's stable identity.
    #[must_use]
    pub fn node_id(&self) -> &A::NodeId {
        &self.node_id
    }

    /// Initializes a new empty group with its complete initial voter set.
    pub async fn initialize(
        &self,
        members: BTreeMap<A::NodeId, A::Node>,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        self.raft
            .initialize(members)
            .await
            .map_err(GroupError::Initialize)
    }

    /// Adds one non-voting member and waits until it has the leader's log.
    pub async fn add_learner(
        &self,
        node_id: A::NodeId,
        node: A::Node,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map(|_| ())
            .map_err(GroupError::Write)
    }

    /// Replaces the voter set and waits for the membership change to commit.
    pub async fn set_voters(
        &self,
        voters: BTreeSet<A::NodeId>,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        self.raft
            .change_membership(voters, false)
            .await
            .map(|_| ())
            .map_err(GroupError::Write)
    }

    /// Removes one non-voting member without changing the current voter set.
    pub async fn remove_learner(
        &self,
        node_id: A::NodeId,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        self.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
            .await
            .map(|_| ())
            .map_err(GroupError::Write)
    }

    /// Commits one typed command and returns its application response.
    pub async fn write(
        &self,
        command: A::Command,
    ) -> Result<WriteResult<A::NodeId, A::Response>, GroupError<A::NodeId, A::Node>> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(GroupError::Write)?;

        match response.data {
            EntryResponse::Application(application_response) => Ok(WriteResult {
                log_id: response.log_id,
                response: application_response,
            }),
            EntryResponse::Internal => Err(GroupError::UnexpectedInternalResponse(response.log_id)),
        }
    }

    /// Confirms this member can serve a current read.
    ///
    /// The check contacts a quorum, confirms this member is still the leader,
    /// and waits until its local state machine has applied the read point.
    pub async fn confirm_leadership_for_read(
        &self,
        timeout: Duration,
    ) -> Result<Option<LogId<A::NodeId>>, GroupError<A::NodeId, A::Node>> {
        tokio::time::timeout(timeout, self.raft.ensure_linearizable())
            .await
            .map_err(|_| GroupError::ConfirmLeadershipTimeout { timeout })?
            .map_err(GroupError::CheckLeadership)
    }

    /// Moves leadership to one caught-up voter through a normal Raft election.
    ///
    /// The old leader first confirms a quorum and waits until the chosen voter
    /// has its latest log. It then stops heartbeats and permits only that voter
    /// to begin the next election. Normal settings are restored on success or
    /// failure.
    pub async fn move_leadership_to(
        &self,
        target: A::NodeId,
        timeout: Duration,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        if target == self.node_id {
            return Err(GroupError::LeaderTargetIsLocal);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::time::timeout(timeout, self.raft.ensure_linearizable())
            .await
            .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })?
            .map_err(GroupError::CheckLeadership)?;

        self.wait_for_replication(&target, deadline, timeout)
            .await?;
        let nodes = self.network.registered_nodes();
        let target_raft = nodes
            .iter()
            .find(|(node_id, _)| node_id == &target)
            .map(|(_, raft)| raft.clone())
            .ok_or(GroupError::LeaderTargetNotRunning)?;
        let _move_settings =
            LeadershipMoveSettings::choose_target(nodes.clone(), &self.node_id, &target);

        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(GroupError::MoveLeadershipTimeout { timeout })?;
        tokio::time::timeout(remaining, target_raft.trigger().elect())
            .await
            .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })?
            .map_err(GroupError::MoveLeadership)?;
        for (_, raft) in &nodes {
            Self::wait_for_selected_leader(raft, &target, deadline, timeout).await?;
        }

        let leader_count = nodes
            .iter()
            .filter(|(_, raft)| {
                let metrics = raft.metrics();
                let metrics = metrics.borrow();
                metrics.state == ServerState::Leader
                    && metrics.current_leader.as_ref() == Some(&target)
            })
            .count();
        if leader_count != 1 {
            return Err(GroupError::MoveLeadershipTimeout { timeout });
        }
        Ok(())
    }

    /// Returns the latest typed metrics without waiting for a change.
    #[must_use]
    pub fn metrics(&self) -> GroupMetrics<A::NodeId> {
        let receiver = self.raft.metrics();
        GroupMetrics::from_openraft(&receiver.borrow())
    }

    /// Waits until this member observes any current leader.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<A::NodeId, WaitError> {
        let metrics = self
            .wait_for_metrics(timeout, "a current leader", |metrics| {
                metrics.leader.is_some()
            })
            .await?;

        metrics
            .leader
            .ok_or(WaitError::InconsistentMetrics("a current leader"))
    }

    /// Waits until this member observes a leader other than `previous`.
    pub async fn wait_for_leader_change(
        &self,
        previous: &A::NodeId,
        timeout: Duration,
    ) -> Result<A::NodeId, WaitError> {
        let metrics = self
            .wait_for_metrics(timeout, "a different leader", |metrics| {
                metrics
                    .leader
                    .as_ref()
                    .is_some_and(|leader| leader != previous)
            })
            .await?;

        metrics
            .leader
            .ok_or(WaitError::InconsistentMetrics("a different leader"))
    }

    /// Waits until the state machine has applied at least `log_index`.
    pub async fn wait_for_applied(
        &self,
        log_index: u64,
        timeout: Duration,
    ) -> Result<GroupMetrics<A::NodeId>, WaitError> {
        self.wait_for_metrics(timeout, "the requested applied log index", |metrics| {
            metrics
                .last_applied_index
                .is_some_and(|applied| applied >= log_index)
        })
        .await
    }

    /// Stops this member, joins OpenRaft's tasks, and unregisters its RPC handle.
    pub async fn shutdown(&self) -> Result<(), GroupError<A::NodeId, A::Node>> {
        self.network.unregister(&self.node_id);
        let result = self.raft.shutdown().await.map_err(GroupError::Shutdown);
        self.state_machine_shutdown.wait().await;
        result
    }

    /// Waits until the chosen voter stores every log held by this leader.
    async fn wait_for_replication(
        &self,
        target: &A::NodeId,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        let mut receiver = self.raft.metrics();
        let wait = async {
            loop {
                let caught_up = {
                    let metrics = receiver.borrow_and_update();
                    if metrics.current_leader.as_ref() != Some(&self.node_id) {
                        return Err(GroupError::LostLeadership);
                    }
                    if !metrics
                        .membership_config
                        .voter_ids()
                        .any(|node_id| &node_id == target)
                    {
                        return Err(GroupError::LeaderTargetNotVoter);
                    }
                    let last_log_index = metrics.last_log_index;
                    metrics
                        .replication
                        .as_ref()
                        .and_then(|replication| replication.get(target))
                        .is_some_and(|matched| {
                            matched.as_ref().map(|log_id| log_id.index) >= last_log_index
                        })
                };
                if caught_up {
                    return Ok(());
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })?;
            }
        };
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(GroupError::MoveLeadershipTimeout { timeout })?;
        tokio::time::timeout(remaining, wait)
            .await
            .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })?
    }

    /// Waits until one member observes the requested current leader.
    async fn wait_for_selected_leader(
        raft: &Raft<TypeConfig<A>>,
        target: &A::NodeId,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<(), GroupError<A::NodeId, A::Node>> {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(GroupError::MoveLeadershipTimeout { timeout })?;
        raft.wait(Some(remaining))
            .current_leader(target.clone(), "move leadership")
            .await
            .map(|_| ())
            .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })
    }

    /// Waits on OpenRaft's watch channel without polling or arbitrary sleeps.
    async fn wait_for_metrics<F>(
        &self,
        timeout: Duration,
        condition_name: &'static str,
        mut condition: F,
    ) -> Result<GroupMetrics<A::NodeId>, WaitError>
    where
        F: FnMut(&GroupMetrics<A::NodeId>) -> bool,
    {
        let mut receiver = self.raft.metrics();
        let wait = async {
            loop {
                let metrics = {
                    let current = receiver.borrow_and_update();
                    GroupMetrics::from_openraft(&current)
                };

                if condition(&metrics) {
                    return Ok(metrics);
                }

                receiver
                    .changed()
                    .await
                    .map_err(|_| WaitError::MetricsClosed(condition_name))?;
            }
        };

        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| WaitError::Timeout {
                condition: condition_name,
                timeout,
            })?
    }
}

impl<A> Drop for InMemoryNode<A>
where
    A: RaftApplication,
{
    /// Removes the RPC registration if a caller drops a node without joining it.
    fn drop(&mut self) {
        self.network.unregister(&self.node_id);
    }
}

impl<A> crate::runtime::RunningGroup for InMemoryNode<A>
where
    A: RaftApplication,
    A::Command: Clone,
{
    type Error = GroupError<A::NodeId, A::Node>;

    /// Stops this in-memory member through the shared runtime boundary.
    async fn shutdown(&self) -> Result<(), Self::Error> {
        InMemoryNode::shutdown(self).await
    }
}
