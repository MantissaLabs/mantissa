//! Raft state, proposals, and membership changes for one replicated volume.

use super::{
    ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT, Arc, BTreeSet, Context, LeaderVolumeGroupState,
    PEER_WAKE_ATTEMPT_TIMEOUT, RECONCILE_RAFT_ATTEMPT_TIMEOUT, RaftMembershipSnapshot,
    ReplacementMembershipGoal, ReplicaKey, ReplicatedVolumeRuntime, Result, RuntimeError,
    TransportError, Uuid, VolumeCommand, VolumeCommandResponse, VolumeControlState,
    VolumeDisposition, debug, group, split_blocks_command, stale_replacement_learners,
    validate_replacement_rollback_voters,
};

impl ReplicatedVolumeRuntime {
    /// Returns the shared membership lock for one volume without retaining idle entries.
    pub(super) fn volume_membership_lock(&self, key: ReplicaKey) -> Arc<tokio::sync::RwLock<()>> {
        self.volume_membership_locks.for_volume(key)
    }

    /// Holds one volume's membership work after a final durable split check.
    pub(super) async fn lock_raft_membership_change(
        &self,
        key: ReplicaKey,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<()>> {
        let change = self.volume_membership_lock(key).read_owned().await;
        self.membership_change_blocker
            .require_membership_changes_allowed()?;
        Ok(change)
    }

    /// Rejects an authenticated storage peer after a committed split excludes it.
    pub(in crate::volumes::replicated) fn require_storage_peer_in_active_view(
        &self,
        peer: Uuid,
    ) -> Result<()> {
        if !self.cluster_view.includes_node(&peer) {
            anyhow::bail!("storage peer {peer} is outside the current cluster view");
        }
        Ok(())
    }

    /// Runs one narrow application RPC through the runtime-owned transport.
    pub(in crate::volumes::replicated) async fn call_storage_application<T, F>(
        &self,
        node_id: Uuid,
        size: usize,
        call: F,
    ) -> Result<T, TransportError>
    where
        T: Send + 'static,
        F: FnOnce(
                mantissa_protocol::raft::raft_transport::Client,
            ) -> futures::future::LocalBoxFuture<'static, Result<T, capnp::Error>>
            + Send
            + 'static,
    {
        self.transport.call_application(node_id, size, call).await
    }

    /// Reads current locally applied state, starting its saved group if needed.
    pub(crate) async fn read_applied_state(&self, key: ReplicaKey) -> Result<VolumeControlState> {
        self.require_current_generation(key)?;
        let state = self.groups.activate(&key).await?.state();
        if state.descriptor().is_some()
            && let Some(record) = self.replicas.replica(key)?
        {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(state)
    }

    /// Reads already-applied durable control state without starting the Raft group.
    pub(crate) fn applied_state(&self, key: ReplicaKey) -> Result<Option<VolumeControlState>> {
        self.require_current_generation(key)?;
        self.applied_volume_states
            .cell(key)
            .map(|cell| cell.load().control_state().map_err(anyhow::Error::from))
            .transpose()
    }

    /// Returns quorum-confirmed control state only when this member is the leader.
    pub(crate) async fn local_quorum_state(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<VolumeControlState>> {
        self.require_current_generation(key)?;
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        if leader != self.node_id {
            return Ok(None);
        }
        let state = group.leader_state(self.operation_timeout).await?;
        if let Some(record) = self.replicas.replica(key)? {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(Some(state))
    }

    /// Wakes saved voters and makes one bounded quorum-read attempt for reconciliation.
    pub(crate) async fn poll_local_quorum_state(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<VolumeControlState>> {
        self.require_current_generation(key)?;
        let group = self.activate_with_voters(key).await?;
        if group.metrics().leader != Some(self.node_id) {
            return Ok(None);
        }
        let control_state = match tokio::time::timeout(
            RECONCILE_RAFT_ATTEMPT_TIMEOUT,
            group.leader_state(self.operation_timeout),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Ok(None),
        };
        if let Some(record) = self.replicas.replica(key)? {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(Some(control_state))
    }

    /// Reads one running leader's applied state without requiring a live quorum.
    pub(crate) async fn poll_running_local_leader_observation(
        &self,
        key: ReplicaKey,
    ) -> Result<Option<LeaderVolumeGroupState>> {
        self.require_current_generation(key)?;
        let group = match self.groups.group(&key).await {
            Ok(group) => group,
            Err(RuntimeError::GroupNotRunning) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if group.metrics().leader != Some(self.node_id) {
            return Ok(None);
        }
        // Group status is an advisory CRDT observation and never authorizes a
        // transition. Reading the already-applied state here lets every stable
        // voter, including the last leader, become idle. Actionable desired or
        // health facts take the separate quorum path above, which explicitly
        // wakes the saved voters before reading state or proposing a command.
        let control_state = group.state();
        if group.metrics().leader != Some(self.node_id) {
            return Ok(None);
        }
        if let Some(record) = self.replicas.replica(key)? {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(Some(LeaderVolumeGroupState {
            control_state,
            membership: RaftMembershipSnapshot {
                voters: group.voter_node_ids(),
                members: group.member_node_ids(),
                is_joint: group.membership_is_joint(),
            },
        }))
    }

    /// Returns quorum-confirmed control state and membership on this leader.
    pub(crate) async fn inspect_quorum_state_as_leader(
        &self,
        key: ReplicaKey,
    ) -> Result<LeaderVolumeGroupState> {
        // The exclusive side returns a snapshot that cannot overlap a local
        // membership change. During split validation, the Proposed row has
        // already converged: this waits for older calls, while later calls
        // fail their durable split check.
        let _membership_changes = self.volume_membership_lock(key).write_owned().await;
        self.require_current_generation(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let control_state = group.leader_state(self.operation_timeout).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("volume leadership changed during quorum-state inspection");
        }
        Ok(LeaderVolumeGroupState {
            control_state,
            membership: RaftMembershipSnapshot {
                voters: group.voter_node_ids(),
                members: group.member_node_ids(),
                is_joint: group.membership_is_joint(),
            },
        })
    }

    /// Proposes through this member only while it remains the elected leader.
    pub(crate) async fn propose_as_leader(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        let _membership_change = if split_blocks_command(&command) {
            Some(self.lock_raft_membership_change(key).await?)
        } else {
            None
        };
        self.require_current_generation(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let response = group.write(command).await?.response;
        if let Some(record) = self.replicas.replica(key)? {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(response)
    }

    /// Makes one bounded semantic proposal attempt for level reconciliation.
    ///
    /// Cancelling the response wait does not cancel ownership inside the Raft
    /// core. A command that commits after the deadline is recognized as current
    /// by the next pass, so the controller never waits on peer wakeup or stores
    /// an RPC completion phase.
    pub(crate) async fn propose_as_leader_for_reconcile(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        let _membership_change = if split_blocks_command(&command) {
            Some(self.lock_raft_membership_change(key).await?)
        } else {
            None
        };
        self.require_current_generation(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let response = tokio::time::timeout(RECONCILE_RAFT_ATTEMPT_TIMEOUT, group.write(command))
            .await
            .context("volume control state reconciliation attempt timed out")??
            .response;
        if let Some(record) = self.replicas.replica(key)? {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(response)
    }

    /// Proposes one semantic compare-and-set control-state transition.
    pub(crate) async fn propose_volume_command(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        let _membership_change = if split_blocks_command(&command) {
            Some(self.lock_raft_membership_change(key).await?)
        } else {
            None
        };
        self.require_current_generation(key)?;
        let descriptor = self
            .replicas
            .replica(key)?
            .context("volume control state proposal has no local replica")?
            .descriptor()
            .clone();
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        let response = if leader == self.node_id {
            group.write(command).await?.response
        } else {
            self.propose_volume_command_on(leader, descriptor, command)
                .await?
        };
        if let Some(record) = self.replicas.replica(key)? {
            self.reconcile_replica_io_gate(&record).await?;
        }
        Ok(response)
    }

    /// Makes one bounded control state proposal from local attachment reconciliation.
    pub(super) async fn propose_volume_command_for_reconcile(
        &self,
        key: ReplicaKey,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        tokio::time::timeout(
            ATTACHMENT_RECONCILE_ATTEMPT_TIMEOUT,
            self.propose_volume_command(key, command),
        )
        .await
        .context("attachment control-state reconciliation attempt timed out")?
    }

    /// Returns current committed voter membership from the local group view.
    pub(crate) async fn membership(&self, key: ReplicaKey) -> Result<BTreeSet<Uuid>> {
        self.require_current_generation(key)?;
        Ok(self.groups.activate(&key).await?.voter_node_ids())
    }

    /// Reads durable local membership without activating its Raft member.
    pub(crate) fn saved_membership(&self, key: ReplicaKey) -> Result<BTreeSet<Uuid>> {
        self.require_current_generation(key)?;
        Ok(self
            .groups
            .catalog()
            .group(&key)?
            .and_then(|record| record.membership().cloned())
            .map(|membership| membership.voter_ids().collect())
            .unwrap_or_default())
    }

    /// Suspends process-idle groups while preserving restart replay markers.
    pub(crate) async fn suspend_idle_groups(&self) -> Result<usize> {
        let suspended = tokio::time::timeout(
            RECONCILE_RAFT_ATTEMPT_TIMEOUT,
            self.groups.suspend_idle(self.raft_group_idle_timeout),
        )
        .await
        .context("idle volume group suspension timed out")?
        .map_err(anyhow::Error::from)?;
        if suspended > 0 {
            debug!(
                target: "mantissa::volumes::raft",
                local_node_id = %self.node_id,
                suspended_group_count = suspended,
                "stopped idle volume Raft members"
            );
        }
        Ok(suspended)
    }

    /// Reconciles the learner or final voters for one replacement on this leader.
    pub(crate) async fn reconcile_replacement_membership_as_leader(
        &self,
        key: ReplicaKey,
        replacement_id: mantissa_volume::ReplacementId,
        coordinator_node_id: Uuid,
        goal: ReplacementMembershipGoal,
    ) -> Result<BTreeSet<Uuid>> {
        let _membership_change = self.lock_raft_membership_change(key).await?;
        self.require_current_generation(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let state = group.leader_state(self.operation_timeout).await?;
        let replacement = state
            .replacement()
            .context("volume has no current replacement grant")?;
        if replacement.id != replacement_id
            || *replacement.coordinator_node_id.as_uuid() != coordinator_node_id
        {
            anyhow::bail!("replacement membership request does not match current control state");
        }
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let mut voters = group.voter_node_ids();
        let new_node_id = *replacement.new_node_id.as_uuid();
        match goal {
            ReplacementMembershipGoal::Learner => {
                let members = group.member_node_ids();
                for stale in
                    stale_replacement_learners(&members, &voters, &data.copies, new_node_id)
                {
                    group.remove_learner(stale).await?;
                }
                if !group.member_node_ids().contains(&new_node_id) {
                    group.add_learner(new_node_id).await?;
                }
                return Ok(group.voter_node_ids());
            }
            ReplacementMembershipGoal::Absent { rollback_voters } => {
                validate_replacement_rollback_voters(&data.copies, &rollback_voters)?;
                for voter in &rollback_voters {
                    if !group.member_node_ids().contains(voter) {
                        group.add_learner(*voter).await?;
                    }
                }
                if voters != rollback_voters {
                    group.set_voters(rollback_voters.clone()).await?;
                    voters = group.voter_node_ids();
                }
                if group.member_node_ids().contains(&new_node_id) {
                    group.remove_learner(new_node_id).await?;
                }
                if voters != rollback_voters || group.member_node_ids().contains(&new_node_id) {
                    anyhow::bail!("replacement membership rollback has not converged");
                }
                return Ok(voters);
            }
            ReplacementMembershipGoal::FinalVoters => {}
        }
        let mut final_set = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<BTreeSet<_>>();
        if let Some(old_node_id) = replacement.old_node_id {
            final_set.remove(old_node_id.as_uuid());
        }
        final_set.insert(new_node_id);
        if final_set.len() != 3 {
            anyhow::bail!("replacement does not produce exactly three final voters");
        }
        if voters != final_set {
            group.set_voters(final_set.clone()).await?;
            voters = group.voter_node_ids();
        }
        if voters != final_set {
            anyhow::bail!("replacement voter change has not converged");
        }
        Ok(voters)
    }

    /// Removes only extra voters after committed control state still names all three data copies.
    pub(crate) async fn reconcile_data_membership_as_leader(
        &self,
        key: ReplicaKey,
        expected_revision: u64,
        expected_voters: BTreeSet<Uuid>,
    ) -> Result<BTreeSet<Uuid>> {
        let _membership_change = self.lock_raft_membership_change(key).await?;
        self.require_current_generation(key)?;
        let group = self.groups.activate(&key).await?;
        if group.metrics().leader != Some(self.node_id) {
            anyhow::bail!("local volume member is not the current Raft leader");
        }
        let state = group.leader_state(self.operation_timeout).await?;
        let data = state
            .data()
            .context("initialized volume has no data control state")?;
        let current_copies = data
            .copies
            .iter()
            .map(|node_id| *node_id.as_uuid())
            .collect::<BTreeSet<_>>();
        if state.disposition() != VolumeDisposition::Live
            || state.revision() != expected_revision
            || state.replacement().is_some()
            || data.recovery.is_some()
            || current_copies != expected_voters
            || expected_voters.len() != 3
        {
            anyhow::bail!("control-state changed before data membership reconciliation");
        }
        let mut voters = group.voter_node_ids();
        if !expected_voters.is_subset(&voters) {
            anyhow::bail!("current membership does not contain every active data copy");
        }
        if voters != expected_voters {
            group.set_voters(expected_voters.clone()).await?;
            voters = group.voter_node_ids();
        }
        if voters != expected_voters {
            anyhow::bail!("data membership reconciliation has not converged");
        }
        Ok(voters)
    }

    /// Routes replacement membership reconciliation without changing Raft leadership.
    pub(crate) async fn reconcile_replacement_membership(
        &self,
        descriptor: mantissa_volume::VolumeDescriptor,
        replacement_id: mantissa_volume::ReplacementId,
        goal: ReplacementMembershipGoal,
    ) -> Result<BTreeSet<Uuid>> {
        let key = ReplicaKey::from(&descriptor);
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        if leader == self.node_id {
            self.reconcile_replacement_membership_as_leader(key, replacement_id, self.node_id, goal)
                .await
        } else {
            self.reconcile_replacement_membership_on(leader, descriptor, replacement_id, goal)
                .await
        }
    }

    /// Reads control state after obtaining a live quorum and applying its current state.
    pub(super) async fn read_quorum_state(&self, key: ReplicaKey) -> Result<VolumeControlState> {
        self.require_current_generation(key)?;
        let record = self
            .replicas
            .replica(key)?
            .context("volume has no local replica")?;
        let group = self.activate_with_voters(key).await?;
        let leader = group.wait_for_leader(self.operation_timeout).await?;
        if leader != self.node_id {
            let observed = self
                .inspect_replica_on(leader, record.descriptor().clone())
                .await?;
            if observed.leader_node_id != Some(leader) {
                anyhow::bail!("remote volume leader changed during quorum-state read");
            }
            let applied = observed
                .applied_log_index
                .context("remote volume leader has no applied state entry")?;
            group
                .wait_for_applied(applied, self.operation_timeout)
                .await?;
            if group.metrics().leader != Some(leader) {
                anyhow::bail!("volume leader changed while applying current control state");
            }
        } else {
            group.leader_state(self.operation_timeout).await?;
        }
        let state = group.state();
        self.reconcile_replica_io_gate(&record).await?;
        Ok(state)
    }

    /// Wakes the saved voters before starting the local Raft member.
    pub(super) async fn activate_with_voters(
        &self,
        key: ReplicaKey,
    ) -> Result<Arc<group::RunningVolumeNode>> {
        self.require_current_generation(key)?;
        let saved = self
            .groups
            .catalog()
            .group(&key)?
            .context("volume has no saved Raft group")?;
        let voters = saved
            .membership()
            .into_iter()
            .flat_map(|membership| membership.voter_ids())
            .collect();
        // A saved leader starts replication as soon as OpenRaft opens. Starting
        // it first therefore races every idle follower and produces a failed
        // replication round before the explicit wake requests arrive. The wake
        // wait is bounded: an unavailable voter cannot block local activation.
        self.wake_voters(key, &voters).await;
        Ok(self.groups.activate(&key).await?)
    }

    /// Sends one bounded start request to every saved remote voter.
    pub(super) async fn wake_voters(&self, key: ReplicaKey, voters: &BTreeSet<Uuid>) {
        let remote_voters = voters
            .iter()
            .copied()
            .filter(|node| *node != self.node_id)
            .collect::<Vec<_>>();
        let wakeups = remote_voters
            .iter()
            .copied()
            .map(|voter| async move { (voter, self.transport.start_group_on(key, voter).await) });
        match tokio::time::timeout(
            PEER_WAKE_ATTEMPT_TIMEOUT,
            futures::future::join_all(wakeups),
        )
        .await
        {
            Ok(results) => {
                let mut ready_voters = 0_usize;
                let mut failed_voter_wakeups = Vec::new();
                for (voter, result) in results {
                    match result {
                        Ok(()) => ready_voters += 1,
                        Err(error) => {
                            failed_voter_wakeups.push((voter, error.to_string()));
                        }
                    }
                }
                debug!(
                    target: "mantissa::volumes::raft",
                    volume_id = %key.volume_id().as_uuid(),
                    generation = key.generation().get(),
                    local_node_id = %self.node_id,
                    remote_voter_count = remote_voters.len(),
                    ready_voter_count = ready_voters,
                    failed_voter_wakeups = ?failed_voter_wakeups,
                    "finished waking remote volume Raft members"
                );
            }
            Err(_) => {
                debug!(
                    target: "mantissa::volumes::raft",
                    volume_id = %key.volume_id().as_uuid(),
                    generation = key.generation().get(),
                    local_node_id = %self.node_id,
                    remote_voter_count = remote_voters.len(),
                    timeout_ms = PEER_WAKE_ATTEMPT_TIMEOUT.as_millis(),
                    "timed out waking remote volume Raft members"
                );
            }
        }
    }
}
