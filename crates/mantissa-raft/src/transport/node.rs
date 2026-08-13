use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::{ChangeMembers, Config, EmptyNode, LogId, Raft, ServerState};

use crate::catalog::{CatalogError, GroupCatalog};
use crate::durable::{DurableLogShutdown, DurableLogStore, DurableStateMachine};
use crate::durable_log::EncryptedLog;
use crate::memory::{GroupError, GroupMetrics, WaitError, WriteResult};
use crate::runtime::RunningGroup;
use crate::{DurableApplicationStateMachine, EntryResponse, RaftApplication, TypeConfig};

use super::{IncomingSnapshots, RaftPeerDirectory, TcpTransport, TransportError};
use crate::catalog::GroupIdAdapter;
use crate::protocol::{ApplicationCommandAdapter, NodeIdAdapter};
use crate::state_machine_shutdown::StateMachineShutdown;

/// Restores normal heartbeats after one planned leader move.
struct PausedHeartbeats<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    raft: Raft<TypeConfig<A>>,
}

impl<A> PausedHeartbeats<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Stops the old leader's empty heartbeats while the target starts an election.
    fn new(raft: Raft<TypeConfig<A>>) -> Self {
        raft.runtime_config().heartbeat(false);
        Self { raft }
    }
}

impl<A> Drop for PausedHeartbeats<A>
where
    A: RaftApplication<Node = EmptyNode>,
{
    /// Restores the heartbeat setting selected when this member started.
    fn drop(&mut self) {
        self.raft
            .runtime_config()
            .heartbeat(self.raft.config().enable_heartbeat);
    }
}

/// One Raft member using authenticated TCP for group traffic.
#[must_use = "a TCP Raft node must be shut down explicitly"]
pub struct TcpNode<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    GID: Ord,
    D: RaftPeerDirectory<A::NodeId>,
{
    node_id: A::NodeId,
    group_id: GID,
    raft: Raft<TypeConfig<A>>,
    transport: Arc<TcpTransport<A, GID, G, N, C, D>>,
    leadership: tokio::sync::Mutex<()>,
    state_machine_shutdown: Arc<StateMachineShutdown>,
    durable_log_shutdown: Arc<DurableLogShutdown>,
}

impl<A, GID, G, N, C, D> TcpNode<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Starts one member from an opened encrypted log and saved application.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_stored<S>(
        node_id: A::NodeId,
        group_id: GID,
        config: Config,
        transport: Arc<TcpTransport<A, GID, G, N, C, D>>,
        catalog: GroupCatalog<GID, A::NodeId, G, N>,
        log: EncryptedLog<GID, A::NodeId, G, N>,
        commands: Arc<C>,
        state_machine: S,
        last_applied: Option<LogId<A::NodeId>>,
        incoming_snapshots: Option<Arc<dyn IncomingSnapshots<A>>>,
    ) -> Result<Self, TcpNodeError<A::NodeId, EmptyNode>>
    where
        S: DurableApplicationStateMachine<A>,
        G: Clone,
        N: Clone,
    {
        let config = config
            .validate()
            .map_err(GroupError::InvalidConfiguration)
            .map_err(TcpNodeError::Group)?;
        let record = catalog
            .group(&group_id)?
            .ok_or(CatalogError::GroupNotFound)?;
        let membership = record.membership().cloned().unwrap_or_default();
        let network = transport.network_factory(group_id.clone(), node_id.clone());
        let (log_store, durable_log_shutdown) =
            DurableLogStore::<A, _, _, _, _>::new(group_id.clone(), catalog.clone(), log, commands);
        let (state_machine_shutdown, shutdown_owner) = StateMachineShutdown::pair();
        let state_machine = DurableStateMachine::<A, _, _, _, _>::new(
            state_machine,
            group_id.clone(),
            catalog,
            last_applied,
            membership,
            shutdown_owner,
        );
        let raft = match Raft::new(
            node_id.clone(),
            Arc::new(config),
            network,
            log_store,
            state_machine,
        )
        .await
        {
            Ok(raft) => raft,
            Err(error) => {
                state_machine_shutdown.wait().await;
                durable_log_shutdown.wait().await;
                return Err(TcpNodeError::Group(GroupError::Start(error)));
            }
        };
        if let Err(error) =
            transport.register_group(group_id.clone(), raft.clone(), incoming_snapshots)
        {
            let _ = raft.shutdown().await;
            state_machine_shutdown.wait().await;
            durable_log_shutdown.wait().await;
            return Err(TcpNodeError::Transport(error));
        }
        Ok(Self {
            node_id,
            group_id,
            raft,
            transport,
            leadership: tokio::sync::Mutex::new(()),
            state_machine_shutdown,
            durable_log_shutdown,
        })
    }

    /// Returns this member's stable identity.
    #[must_use]
    pub const fn node_id(&self) -> &A::NodeId {
        &self.node_id
    }

    /// Initializes a new group with its complete initial voter set.
    pub async fn initialize(
        &self,
        members: BTreeMap<A::NodeId, EmptyNode>,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        self.raft
            .initialize(members)
            .await
            .map_err(GroupError::Initialize)
            .map_err(TcpNodeError::Group)
    }

    /// Adds one non-voting member and waits until it has the leader's log.
    pub async fn add_learner(
        &self,
        node_id: A::NodeId,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        self.raft
            .add_learner(node_id, EmptyNode {}, true)
            .await
            .map(|_| ())
            .map_err(GroupError::Write)
            .map_err(TcpNodeError::Group)
    }

    /// Replaces the voter set and waits for the membership change to commit.
    pub async fn set_voters(
        &self,
        voters: BTreeSet<A::NodeId>,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        self.raft
            .change_membership(voters, false)
            .await
            .map(|_| ())
            .map_err(GroupError::Write)
            .map_err(TcpNodeError::Group)
    }

    /// Removes one non-voting member without changing the current voter set.
    pub async fn remove_learner(
        &self,
        node_id: A::NodeId,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        self.raft
            .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
            .await
            .map(|_| ())
            .map_err(GroupError::Write)
            .map_err(TcpNodeError::Group)
    }

    /// Commits one typed command and waits for local application.
    pub async fn write(
        &self,
        command: A::Command,
    ) -> Result<WriteResult<A::NodeId, A::Response>, TcpNodeError<A::NodeId, EmptyNode>> {
        let response = self
            .raft
            .client_write(command)
            .await
            .map_err(GroupError::Write)
            .map_err(TcpNodeError::Group)?;
        match response.data {
            EntryResponse::Application(application_response) => Ok(WriteResult {
                log_id: response.log_id,
                response: application_response,
            }),
            EntryResponse::Internal => Err(TcpNodeError::Group(
                GroupError::UnexpectedInternalResponse(response.log_id),
            )),
        }
    }

    /// Confirms this member can serve a current read.
    pub async fn confirm_leadership_for_read(
        &self,
        timeout: Duration,
    ) -> Result<Option<LogId<A::NodeId>>, TcpNodeError<A::NodeId, EmptyNode>> {
        tokio::time::timeout(timeout, self.raft.ensure_linearizable())
            .await
            .map_err(|_| TcpNodeError::Group(GroupError::ConfirmLeadershipTimeout { timeout }))?
            .map_err(GroupError::CheckLeadership)
            .map_err(TcpNodeError::Group)
    }

    /// Repeats a temporary failed quorum check until this caller's deadline.
    async fn confirm_leadership_until(
        &self,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        loop {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(GroupError::ElectionTimeout { timeout })
                .map_err(TcpNodeError::Group)?;
            match self.confirm_leadership_for_read(remaining).await {
                Ok(_) => return Ok(()),
                Err(TcpNodeError::Group(GroupError::CheckLeadership(error)))
                    if matches!(
                        error.api_error(),
                        Some(openraft::error::CheckIsLeaderError::QuorumNotEnough(_))
                    ) => {}
                Err(error) => return Err(error),
            }

            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(GroupError::ElectionTimeout { timeout })
                .map_err(TcpNodeError::Group)?;
            let retry_delay = Duration::from_millis(self.raft.config().heartbeat_interval.max(1));
            tokio::time::sleep(retry_delay.min(remaining)).await;
        }
    }

    /// Returns the local applied index while this leader's quorum lease is valid.
    ///
    /// A follower cannot elect another leader before this lease expires, so a
    /// caller may read local applied state at this index without sending a new
    /// heartbeat for every read. `None` requires a normal quorum check.
    pub fn safe_local_read_index(&self) -> Option<u64> {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow();
        let lease_is_valid = metrics.state == ServerState::Leader
            && metrics.current_leader.as_ref() == Some(&self.node_id)
            && metrics
                .millis_since_quorum_ack
                .is_some_and(|age| age < self.raft.config().election_timeout_max);
        if lease_is_valid {
            metrics.last_applied.as_ref().map(|log_id| log_id.index)
        } else {
            None
        }
    }

    /// Waits for a leader and safely moves leadership to this caught-up voter.
    pub async fn become_leader(
        &self,
        timeout: Duration,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let _leadership = tokio::time::timeout(timeout, self.leadership.lock())
            .await
            .map_err(|_| GroupError::ElectionTimeout { timeout })
            .map_err(TcpNodeError::Group)?;
        loop {
            let current = self.metrics();
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(GroupError::ElectionTimeout { timeout })
                .map_err(TcpNodeError::Group)?;
            let leader = match current.leader {
                Some(leader) => leader,
                None => self
                    .wait_for_leader(remaining)
                    .await
                    .map_err(|_| TcpNodeError::Group(GroupError::ElectionTimeout { timeout }))?,
            };
            if leader != self.node_id {
                let remaining = deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .ok_or(GroupError::ElectionTimeout { timeout })
                    .map_err(TcpNodeError::Group)?;
                tokio::time::timeout(
                    remaining,
                    self.transport
                        .request_leadership(self.group_id.clone(), leader),
                )
                .await
                .map_err(|_| GroupError::ElectionTimeout { timeout })?
                .map_err(TcpNodeError::Transport)?;
            }
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(GroupError::ElectionTimeout { timeout })
                .map_err(TcpNodeError::Group)?;
            self.wait_for_metrics(remaining, "this member to become leader", |metrics| {
                metrics.state == crate::memory::GroupState::Leader
                    && metrics.leader.as_ref() == Some(&self.node_id)
            })
            .await
            .map_err(|_| TcpNodeError::Group(GroupError::ElectionTimeout { timeout }))?;
            match self.confirm_leadership_until(deadline, timeout).await {
                Ok(()) => return Ok(()),
                Err(TcpNodeError::Group(GroupError::CheckLeadership(error)))
                    if matches!(
                        error.api_error(),
                        Some(openraft::error::CheckIsLeaderError::ForwardToLeader(_))
                    ) =>
                {
                    // A restarted member can briefly read its saved vote before
                    // receiving the newer leader's vote. Let metrics catch up,
                    // then repeat the normal transfer instead of failing the
                    // caller on that expected startup race.
                    let remaining = deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .ok_or(GroupError::ElectionTimeout { timeout })
                        .map_err(TcpNodeError::Group)?;
                    let retry_delay =
                        Duration::from_millis(self.raft.config().heartbeat_interval.max(1));
                    tokio::time::sleep(retry_delay.min(remaining)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Moves leadership to one voter after it has the leader's complete log.
    pub async fn move_leadership_to(
        &self,
        target: A::NodeId,
        timeout: Duration,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        if target == self.node_id {
            return Err(TcpNodeError::Group(GroupError::LeaderTargetIsLocal));
        }
        let deadline = tokio::time::Instant::now() + timeout;
        let _leadership = tokio::time::timeout(timeout, self.leadership.lock())
            .await
            .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })
            .map_err(TcpNodeError::Group)?;
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(GroupError::MoveLeadershipTimeout { timeout })?;
        tokio::time::timeout(remaining, self.raft.ensure_linearizable())
            .await
            .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })?
            .map_err(GroupError::CheckLeadership)
            .map_err(TcpNodeError::Group)?;
        self.wait_for_replication(&target, deadline, timeout)
            .await?;

        let _paused_heartbeats = PausedHeartbeats::new(self.raft.clone());
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(GroupError::MoveLeadershipTimeout { timeout })?;
        tokio::time::timeout(
            remaining,
            self.transport
                .start_election(self.group_id.clone(), target.clone()),
        )
        .await
        .map_err(|_| GroupError::MoveLeadershipTimeout { timeout })??;
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or(GroupError::MoveLeadershipTimeout { timeout })?;
        self.wait_for_metrics(remaining, "the selected leader", |metrics| {
            metrics.leader.as_ref() == Some(&target)
                && metrics.state != crate::memory::GroupState::Leader
        })
        .await
        .map(|_| ())
        .map_err(|_| TcpNodeError::Group(GroupError::MoveLeadershipTimeout { timeout }))
    }

    /// Returns the latest typed metrics without waiting for a change.
    #[must_use]
    pub fn metrics(&self) -> GroupMetrics<A::NodeId> {
        let receiver = self.raft.metrics();
        GroupMetrics::from_openraft(&receiver.borrow())
    }

    /// Returns the voter set from this member's current membership.
    pub fn voter_ids(&self) -> BTreeSet<A::NodeId> {
        let receiver = self.raft.metrics();
        receiver.borrow().membership_config.voter_ids().collect()
    }

    /// Returns all members in the current membership, including learners.
    pub fn member_ids(&self) -> BTreeSet<A::NodeId> {
        let receiver = self.raft.metrics();
        receiver
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .map(|(node_id, _)| node_id.clone())
            .collect()
    }

    /// Returns whether the current membership contains more than one voter configuration.
    pub fn membership_is_joint(&self) -> bool {
        let receiver = self.raft.metrics();
        receiver
            .borrow()
            .membership_config
            .membership()
            .get_joint_config()
            .len()
            != 1
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

    /// Waits until a snapshot includes at least the requested log index.
    pub async fn wait_for_snapshot(
        &self,
        log_index: u64,
        timeout: Duration,
    ) -> Result<GroupMetrics<A::NodeId>, WaitError> {
        self.wait_for_metrics(timeout, "the requested snapshot log index", |metrics| {
            metrics
                .snapshot_index
                .is_some_and(|snapshot| snapshot >= log_index)
        })
        .await
    }

    /// Waits until one normal, non-joint voter set exactly matches the request.
    pub async fn wait_for_voters(
        &self,
        voters: &BTreeSet<A::NodeId>,
        timeout: Duration,
    ) -> Result<GroupMetrics<A::NodeId>, WaitError> {
        let mut receiver = self.raft.metrics();
        let wait = async {
            loop {
                let matches = {
                    let current = receiver.borrow_and_update();
                    let membership = current.membership_config.membership();
                    let current_voters = membership.voter_ids().collect::<BTreeSet<_>>();
                    membership.get_joint_config().len() == 1 && current_voters == *voters
                };
                if matches {
                    return Ok(GroupMetrics::from_openraft(&receiver.borrow()));
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| WaitError::MetricsClosed("the requested voter set"))?;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| WaitError::Timeout {
                condition: "the requested voter set",
                timeout,
            })?
    }

    /// Stops this member and removes its inbound RPC route.
    pub async fn shutdown(&self) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
        self.transport.unregister_group(&self.group_id);
        let result = self
            .raft
            .shutdown()
            .await
            .map_err(GroupError::Shutdown)
            .map_err(TcpNodeError::Group);
        self.state_machine_shutdown.wait().await;
        self.durable_log_shutdown.wait().await;
        result
    }

    /// Waits on OpenRaft's watch channel without polling.
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

    /// Waits until the selected voter stores every log held by this leader.
    async fn wait_for_replication(
        &self,
        target: &A::NodeId,
        deadline: tokio::time::Instant,
        timeout: Duration,
    ) -> Result<(), TcpNodeError<A::NodeId, EmptyNode>> {
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
            .map_err(TcpNodeError::Group)
    }
}

impl<A, GID, G, N, C, D> Drop for TcpNode<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    GID: Ord,
    D: RaftPeerDirectory<A::NodeId>,
{
    /// Removes inbound routing if shutdown was skipped.
    fn drop(&mut self) {
        self.transport.shared_unregister_group(&self.group_id);
    }
}

impl<A, GID, G, N, C, D> RunningGroup for TcpNode<A, GID, G, N, C, D>
where
    A: RaftApplication<Node = EmptyNode>,
    A::Command: Clone,
    GID: Clone + Ord + Send + Sync + 'static,
    G: GroupIdAdapter<GID> + 'static,
    N: NodeIdAdapter<A::NodeId> + 'static,
    C: ApplicationCommandAdapter<A::Command> + 'static,
    D: RaftPeerDirectory<A::NodeId>,
{
    type Error = TcpNodeError<A::NodeId, EmptyNode>;

    /// Stops the Raft member and removes its inbound transport route.
    async fn shutdown(&self) -> Result<(), Self::Error> {
        TcpNode::shutdown(self).await
    }
}

/// Reports whether node startup or transport registration failed.
#[derive(Debug, thiserror::Error)]
pub enum TcpNodeError<NID, N>
where
    NID: openraft::NodeId,
    N: openraft::Node,
{
    /// OpenRaft could not start or operate the member.
    #[error(transparent)]
    Group(#[from] GroupError<NID, N>),

    /// The authenticated TCP transport rejected registration.
    #[error(transparent)]
    Transport(#[from] TransportError),

    /// The durable group row could not be read.
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}
