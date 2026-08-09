//! Narrow idempotent storage ensures and local-fact inspection.

use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::{Arc, OnceLock, Weak};

use anyhow::{Context, Result};
use mantissa_protocol::volumes::{
    LocalReplicaHealth as WireReplicaHealth, LocalReplicaState as WireReplicaState,
    ReplacementMembershipGoal as WireMembershipGoal, local_replica_status,
    replicated_volume_storage,
};
use mantissa_raft::transport::{
    AuthenticatedApplication, AuthenticatedStreamApplication, TransportError,
};
use mantissa_volume::catalog::{ReplicaHealth, ReplicaKey, ReplicaState};
use mantissa_volume::control_state::{VolumeCommand, VolumeCommandResponse};
use mantissa_volume::protocol::{
    descriptor_message_bytes, read_control_state, read_descriptor, read_volume_command,
    read_volume_command_response, write_control_state, write_descriptor, write_volume_command,
    write_volume_command_response,
};
use mantissa_volume::{OperationId, ReplacementId, VolumeDescriptor};
use uuid::Uuid;

use super::runtime::{
    LeaderVolumeGroupState, LocalReplicaStatus, ReplacementMembershipGoal, ReplicatedVolumeRuntime,
};

/// Creates authenticated storage services after the runtime is fully owned.
pub(super) struct StorageServiceFactory {
    runtime: OnceLock<Weak<ReplicatedVolumeRuntime>>,
}

impl StorageServiceFactory {
    /// Creates an empty one-time runtime binding.
    pub(super) const fn new() -> Self {
        Self {
            runtime: OnceLock::new(),
        }
    }

    /// Binds the completed runtime before the transport starts listening.
    pub(super) fn set_runtime(&self, runtime: &Arc<ReplicatedVolumeRuntime>) -> Result<()> {
        self.runtime
            .set(Arc::downgrade(runtime))
            .map_err(|_| anyhow::anyhow!("replicated-volume RPC runtime was already set"))
    }
}

impl AuthenticatedApplication<Uuid> for StorageServiceFactory {
    /// Creates one control service after Noise authenticates its peer node.
    fn client(&self, peer: Uuid) -> Result<capnp::capability::Client, capnp::Error> {
        let runtime = self.runtime.get().and_then(Weak::upgrade).ok_or_else(|| {
            capnp::Error::failed("replicated-volume runtime is unavailable".into())
        })?;
        let client: replicated_volume_storage::Client = capnp_rpc::new_client(StorageServer {
            peer,
            runtime: Arc::downgrade(&runtime),
        });
        Ok(client.client)
    }
}

impl AuthenticatedStreamApplication<Uuid> for StorageServiceFactory {
    /// Serves dynamically fenced data outside the Raft control queue.
    fn serve(
        &self,
        peer: Uuid,
        stream: mantissa_net::noise::NoiseStream,
    ) -> futures::future::BoxFuture<'static, Result<(), TransportError>> {
        let runtime = self.runtime.get().and_then(Weak::upgrade);
        Box::pin(async move {
            runtime
                .ok_or(TransportError::Stopped)?
                .serve_replica_data_stream(peer, stream)
                .await
                .map_err(|error| TransportError::Application {
                    message: format!("{error:#}"),
                })
        })
    }
}

/// Handles authenticated ensure and inspect calls without changing control state.
struct StorageServer {
    peer: Uuid,
    runtime: Weak<ReplicatedVolumeRuntime>,
}

impl StorageServer {
    /// Upgrades runtime ownership only for one bounded request.
    fn runtime(&self) -> Result<Arc<ReplicatedVolumeRuntime>, capnp::Error> {
        self.runtime
            .upgrade()
            .ok_or_else(|| capnp::Error::failed("replicated-volume runtime is unavailable".into()))
    }
}

impl replicated_volume_storage::Server for StorageServer {
    /// Ensures one immutable bootstrap replica and exact common voter set.
    async fn ensure_replica(
        self: Rc<Self>,
        params: replicated_volume_storage::EnsureReplicaParams,
        mut results: replicated_volume_storage::EnsureReplicaResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let descriptor = read_descriptor(request.get_descriptor()?).map_err(capnp_error)?;
        let bootstrap_id = read_operation_id(request.get_bootstrap_id()?)?;
        let runtime = self.runtime()?;
        let voters = read_node_ids(
            request.get_voter_node_ids()?,
            runtime.protocol_limits().max_membership_nodes() as usize,
        )?;
        if !voters.contains(&self.peer) {
            return Err(capnp_error("bootstrap caller is not in the voter set"));
        }
        let status = runtime
            .ensure_replica_local(descriptor, bootstrap_id, voters)
            .await
            .map_err(capnp_error)?;
        write_status(results.get().init_status(), &status);
        Ok(())
    }

    /// Inspects local facts without changing control or cleanup state.
    async fn inspect_replica(
        self: Rc<Self>,
        params: replicated_volume_storage::InspectReplicaParams,
        mut results: replicated_volume_storage::InspectReplicaResults,
    ) -> Result<(), capnp::Error> {
        let descriptor =
            read_descriptor(params.get()?.get_request()?.get_descriptor()?).map_err(capnp_error)?;
        let runtime = self.runtime()?;
        let key = ReplicaKey::from(&descriptor);
        if !runtime.generation_is_desired(key) {
            return Err(capnp_error(
                "replicated-volume generation is not current desired state",
            ));
        }
        let mut status = runtime.local_status(key).await.map_err(capnp_error)?;
        if status.leader_node_id == Some(runtime.node_id()) {
            if runtime
                .local_quorum_state(key)
                .await
                .map_err(capnp_error)?
                .is_none()
            {
                return Err(capnp_error("local replica is no longer the Raft leader"));
            }
            status = runtime.local_status(key).await.map_err(capnp_error)?;
        }
        write_status(results.get().init_status(), &status);
        Ok(())
    }

    /// Ensures only an inactive replacement named by current control state.
    async fn ensure_replacement_replica(
        self: Rc<Self>,
        params: replicated_volume_storage::EnsureReplacementReplicaParams,
        mut results: replicated_volume_storage::EnsureReplacementReplicaResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let descriptor = read_descriptor(request.get_descriptor()?).map_err(capnp_error)?;
        let replacement_id =
            ReplacementId::new(read_uuid(request.get_replacement_id()?, "replacement ID")?)
                .map_err(capnp_error)?;
        let runtime = self.runtime()?;
        let voters = read_node_ids(
            request.get_voter_node_ids()?,
            runtime.protocol_limits().max_membership_nodes() as usize,
        )?;
        if !voters.contains(&self.peer) {
            return Err(capnp_error("replacement caller is not a current voter"));
        }
        let status = runtime
            .ensure_replacement_local(descriptor, replacement_id, voters)
            .await
            .map_err(capnp_error)?;
        write_status(results.get().init_status(), &status);
        Ok(())
    }

    /// Evaluates one semantic command only while this node is the elected leader.
    async fn propose_volume_command(
        self: Rc<Self>,
        params: replicated_volume_storage::ProposeVolumeCommandParams,
        mut results: replicated_volume_storage::ProposeVolumeCommandResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let descriptor = read_descriptor(request.get_descriptor()?).map_err(capnp_error)?;
        let command = read_volume_command(request.get_command()?).map_err(capnp_error)?;
        let runtime = self.runtime()?;
        let key = ReplicaKey::from(&descriptor);
        if !runtime
            .membership(key)
            .await
            .map_err(capnp_error)?
            .contains(&self.peer)
        {
            return Err(capnp_error(
                "volume command proposer is not a current voter",
            ));
        }
        let response = runtime
            .propose_as_leader(key, command)
            .await
            .map_err(capnp_error)?;
        write_volume_command_response(results.get().init_response(), response);
        Ok(())
    }

    /// Ensures one current replacement's learner or final voter predicate.
    async fn ensure_replacement_membership(
        self: Rc<Self>,
        params: replicated_volume_storage::EnsureReplacementMembershipParams,
        mut results: replicated_volume_storage::EnsureReplacementMembershipResults,
    ) -> Result<(), capnp::Error> {
        let request = params.get()?.get_request()?;
        let descriptor = read_descriptor(request.get_descriptor()?).map_err(capnp_error)?;
        let replacement_id =
            ReplacementId::new(read_uuid(request.get_replacement_id()?, "replacement ID")?)
                .map_err(capnp_error)?;
        let runtime = self.runtime()?;
        let rollback_voters = read_node_ids(
            request.get_rollback_voter_node_ids()?,
            runtime.protocol_limits().max_membership_nodes() as usize,
        )?;
        let goal = match request.get_goal() {
            Ok(WireMembershipGoal::Learner) if rollback_voters.is_empty() => {
                ReplacementMembershipGoal::Learner
            }
            Ok(WireMembershipGoal::FinalVoters) if rollback_voters.is_empty() => {
                ReplacementMembershipGoal::FinalVoters
            }
            Ok(WireMembershipGoal::Absent) => ReplacementMembershipGoal::Absent { rollback_voters },
            Ok(_) => {
                return Err(capnp_error(
                    "only an absent replacement goal may carry rollback voters",
                ));
            }
            Err(capnp::NotInSchema(value)) => {
                return Err(capnp_error(format!(
                    "unknown replacement membership goal {value}"
                )));
            }
        };
        let voters = runtime
            .ensure_replacement_membership_as_leader(
                ReplicaKey::from(&descriptor),
                replacement_id,
                self.peer,
                goal,
            )
            .await
            .map_err(capnp_error)?;
        write_node_ids(
            results
                .get()
                .init_voter_node_ids(u32::try_from(voters.len()).map_err(capnp_error)?),
            &voters,
        );
        Ok(())
    }

    /// Returns linearizable control state and membership only from this elected leader.
    async fn inspect_quorum_state(
        self: Rc<Self>,
        params: replicated_volume_storage::InspectQuorumStateParams,
        mut results: replicated_volume_storage::InspectQuorumStateResults,
    ) -> Result<(), capnp::Error> {
        let descriptor =
            read_descriptor(params.get()?.get_request()?.get_descriptor()?).map_err(capnp_error)?;
        let runtime = self.runtime()?;
        let observation = runtime
            .inspect_quorum_state_as_leader(ReplicaKey::from(&descriptor))
            .await
            .map_err(capnp_error)?;
        if observation.control_state.descriptor() != Some(&descriptor) {
            return Err(capnp_error(
                "leader control-state descriptor differs from request",
            ));
        }
        write_control_state(results.get().init_state(), &observation.control_state);
        write_node_ids(
            results.get().init_voter_node_ids(
                u32::try_from(observation.voter_node_ids.len()).map_err(capnp_error)?,
            ),
            &observation.voter_node_ids,
        );
        Ok(())
    }
}

impl ReplicatedVolumeRuntime {
    /// Ensures one bootstrap replica directly or over authenticated storage RPC.
    pub(crate) async fn ensure_replica_on(
        &self,
        node_id: Uuid,
        descriptor: VolumeDescriptor,
        bootstrap_id: OperationId,
        voters: BTreeSet<Uuid>,
    ) -> Result<LocalReplicaStatus> {
        if node_id == self.node_id() {
            return self
                .ensure_replica_local(descriptor, bootstrap_id, voters)
                .await;
        }
        let maximum_nodes = self.protocol_limits().max_membership_nodes() as usize;
        let size = storage_request_size(&descriptor, voters.len());
        self.call_storage(node_id, size, move |transport| {
            Box::pin(async move {
                let client = storage_client(transport).await?;
                let mut call = client.ensure_replica_request();
                {
                    let mut request = call.get().init_request();
                    write_descriptor(request.reborrow().init_descriptor(), &descriptor);
                    request.set_bootstrap_id(bootstrap_id.as_bytes());
                    write_node_ids(
                        request.reborrow().init_voter_node_ids(voters.len() as u32),
                        &voters,
                    );
                }
                let response = call.send().promise.await?;
                read_status(response.get()?.get_status()?, maximum_nodes)
            })
        })
        .await
        .context("ensure remote bootstrap replica")
    }

    /// Inspects one local or remote replica without causing a transition.
    pub(crate) async fn inspect_replica_on(
        &self,
        node_id: Uuid,
        descriptor: VolumeDescriptor,
    ) -> Result<LocalReplicaStatus> {
        if node_id == self.node_id() {
            return self.local_status(ReplicaKey::from(&descriptor)).await;
        }
        let maximum_nodes = self.protocol_limits().max_membership_nodes() as usize;
        let size = storage_request_size(&descriptor, 0);
        self.call_storage(node_id, size, move |transport| {
            Box::pin(async move {
                let client = storage_client(transport).await?;
                let mut call = client.inspect_replica_request();
                write_descriptor(call.get().init_request().init_descriptor(), &descriptor);
                let response = call.send().promise.await?;
                read_status(response.get()?.get_status()?, maximum_nodes)
            })
        })
        .await
        .context("inspect remote replica")
    }

    /// Ensures one inactive replacement file directly or over authenticated storage RPC.
    pub(crate) async fn ensure_replacement_on(
        &self,
        node_id: Uuid,
        descriptor: VolumeDescriptor,
        replacement_id: ReplacementId,
        voters: BTreeSet<Uuid>,
    ) -> Result<LocalReplicaStatus> {
        if node_id == self.node_id() {
            return self
                .ensure_replacement_local(descriptor, replacement_id, voters)
                .await;
        }
        let maximum_nodes = self.protocol_limits().max_membership_nodes() as usize;
        let size = storage_request_size(&descriptor, voters.len());
        self.call_storage(node_id, size, move |transport| {
            Box::pin(async move {
                let client = storage_client(transport).await?;
                let mut call = client.ensure_replacement_replica_request();
                {
                    let mut request = call.get().init_request();
                    write_descriptor(request.reborrow().init_descriptor(), &descriptor);
                    request.set_replacement_id(replacement_id.as_bytes());
                    write_node_ids(
                        request.reborrow().init_voter_node_ids(voters.len() as u32),
                        &voters,
                    );
                }
                let response = call.send().promise.await?;
                read_status(response.get()?.get_status()?, maximum_nodes)
            })
        })
        .await
        .context("ensure remote replacement replica")
    }

    /// Proposes one bounded control state command to the already elected remote leader.
    pub(crate) async fn propose_volume_command_on(
        &self,
        node_id: Uuid,
        descriptor: VolumeDescriptor,
        command: VolumeCommand,
    ) -> Result<VolumeCommandResponse> {
        if node_id == self.node_id() {
            return self
                .propose_as_leader(ReplicaKey::from(&descriptor), command)
                .await;
        }
        let size = storage_request_size(&descriptor, 0).saturating_add(4096);
        self.call_storage(node_id, size, move |transport| {
            Box::pin(async move {
                let client = storage_client(transport).await?;
                let mut call = client.propose_volume_command_request();
                {
                    let mut request = call.get().init_request();
                    write_descriptor(request.reborrow().init_descriptor(), &descriptor);
                    write_volume_command(request.reborrow().init_command(), &command);
                }
                let response = call.send().promise.await?;
                read_volume_command_response(response.get()?.get_response()?).map_err(|error| {
                    capnp::Error::failed(format!("decode volume command response: {error}"))
                })
            })
        })
        .await
        .context("propose volume control state to elected leader")
    }

    /// Requests one idempotent replacement membership predicate from the elected leader.
    pub(crate) async fn ensure_replacement_membership_on(
        &self,
        leader_node_id: Uuid,
        descriptor: VolumeDescriptor,
        replacement_id: ReplacementId,
        goal: ReplacementMembershipGoal,
    ) -> Result<BTreeSet<Uuid>> {
        if leader_node_id == self.node_id() {
            return self
                .ensure_replacement_membership_as_leader(
                    ReplicaKey::from(&descriptor),
                    replacement_id,
                    self.node_id(),
                    goal,
                )
                .await;
        }
        let maximum_nodes = self.protocol_limits().max_membership_nodes() as usize;
        let size = storage_request_size(&descriptor, maximum_nodes);
        self.call_storage(leader_node_id, size, move |transport| {
            Box::pin(async move {
                let client = storage_client(transport).await?;
                let mut call = client.ensure_replacement_membership_request();
                {
                    let mut request = call.get().init_request();
                    write_descriptor(request.reborrow().init_descriptor(), &descriptor);
                    request.set_replacement_id(replacement_id.as_bytes());
                    let wire_goal = match &goal {
                        ReplacementMembershipGoal::Learner => WireMembershipGoal::Learner,
                        ReplacementMembershipGoal::FinalVoters => WireMembershipGoal::FinalVoters,
                        ReplacementMembershipGoal::Absent { rollback_voters } => {
                            write_node_ids(
                                request.reborrow().init_rollback_voter_node_ids(
                                    u32::try_from(rollback_voters.len()).map_err(capnp_error)?,
                                ),
                                rollback_voters,
                            );
                            WireMembershipGoal::Absent
                        }
                    };
                    request.set_goal(wire_goal);
                }
                let response = call.send().promise.await?;
                read_node_ids(response.get()?.get_voter_node_ids()?, maximum_nodes)
            })
        })
        .await
        .context("ensure replacement membership on elected leader")
    }

    /// Reads linearizable control state and membership from one elected leader.
    pub(crate) async fn inspect_quorum_state_on(
        &self,
        leader_node_id: Uuid,
        descriptor: VolumeDescriptor,
    ) -> Result<LeaderVolumeGroupState> {
        if leader_node_id == self.node_id() {
            return self
                .inspect_quorum_state_as_leader(ReplicaKey::from(&descriptor))
                .await;
        }
        let maximum_nodes = self.protocol_limits().max_membership_nodes() as usize;
        let size = storage_request_size(&descriptor, maximum_nodes).saturating_add(4096);
        self.call_storage(leader_node_id, size, move |transport| {
            Box::pin(async move {
                let client = storage_client(transport).await?;
                let mut call = client.inspect_quorum_state_request();
                write_descriptor(call.get().init_request().init_descriptor(), &descriptor);
                let response = call.send().promise.await?;
                let response = response.get()?;
                Ok(LeaderVolumeGroupState {
                    control_state: read_control_state(response.get_state()?).map_err(|error| {
                        capnp::Error::failed(format!("decode current control state: {error}"))
                    })?,
                    voter_node_ids: read_node_ids(response.get_voter_node_ids()?, maximum_nodes)?,
                })
            })
        })
        .await
        .context("inspect quorum state on elected leader")
    }

    /// Runs one typed storage call through the shared authenticated transport.
    async fn call_storage<T, F>(
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
        self.call_storage_application(node_id, size, call).await
    }
}

/// Resolves the application capability on one authenticated Raft transport.
async fn storage_client(
    transport: mantissa_protocol::raft::raft_transport::Client,
) -> Result<replicated_volume_storage::Client, capnp::Error> {
    let response = transport.get_application_request().send().promise.await?;
    response.get()?.get_service().get_as()
}

/// Writes all bounded local observation fields.
fn write_status(mut builder: local_replica_status::Builder<'_>, status: &LocalReplicaStatus) {
    builder.set_exists(status.exists);
    builder.set_state(match status.state {
        ReplicaState::Preparing => WireReplicaState::Preparing,
        ReplicaState::Ready => WireReplicaState::Ready,
        ReplicaState::Deleting => WireReplicaState::Deleting,
        ReplicaState::Retiring => WireReplicaState::Retiring,
        ReplicaState::Retained => WireReplicaState::Retained,
    });
    builder.set_health(match status.health {
        ReplicaHealth::Healthy => WireReplicaHealth::Healthy,
        ReplicaHealth::NeedsRecovery => WireReplicaHealth::NeedsRecovery,
    });
    builder.set_group_saved(status.group_saved);
    builder.set_group_running(status.group_running);
    builder.set_volume_state_committed(status.control_state_initialized);
    builder.set_has_applied_log(status.applied_log_index.is_some());
    builder.set_applied_log_index(status.applied_log_index.unwrap_or_default());
    if let Some(leader) = status.leader_node_id {
        builder.set_leader_node_id(leader.as_bytes());
    }
    write_node_ids(
        builder
            .reborrow()
            .init_voter_node_ids(status.voter_node_ids.len() as u32),
        &status.voter_node_ids,
    );
    builder.set_reserved_bytes(status.reserved_bytes);
}

/// Reads and validates all bounded local observation fields.
fn read_status(
    reader: local_replica_status::Reader<'_>,
    maximum_nodes: usize,
) -> Result<LocalReplicaStatus, capnp::Error> {
    let state = match reader.get_state()? {
        WireReplicaState::Preparing => ReplicaState::Preparing,
        WireReplicaState::Ready => ReplicaState::Ready,
        WireReplicaState::Deleting => ReplicaState::Deleting,
        WireReplicaState::Retiring => ReplicaState::Retiring,
        WireReplicaState::Retained => ReplicaState::Retained,
    };
    let health = match reader.get_health()? {
        WireReplicaHealth::Healthy => ReplicaHealth::Healthy,
        WireReplicaHealth::NeedsRecovery => ReplicaHealth::NeedsRecovery,
    };
    let leader = (!reader.get_leader_node_id()?.is_empty())
        .then(|| read_uuid(reader.get_leader_node_id()?, "leader node ID"))
        .transpose()?;
    Ok(LocalReplicaStatus {
        exists: reader.get_exists(),
        state,
        health,
        group_saved: reader.get_group_saved(),
        group_running: reader.get_group_running(),
        control_state_initialized: reader.get_volume_state_committed(),
        applied_log_index: reader
            .get_has_applied_log()
            .then(|| reader.get_applied_log_index()),
        leader_node_id: leader,
        voter_node_ids: read_node_ids(reader.get_voter_node_ids()?, maximum_nodes)?,
        reserved_bytes: reader.get_reserved_bytes(),
    })
}

/// Writes sorted node UUIDs into one bounded Cap'n Proto list.
fn write_node_ids(mut builder: capnp::data_list::Builder<'_>, nodes: &BTreeSet<Uuid>) {
    for (index, node) in nodes.iter().enumerate() {
        builder.set(index as u32, node.as_bytes());
    }
}

/// Reads unique non-zero node UUIDs under the caller's explicit bound.
fn read_node_ids(
    reader: capnp::data_list::Reader<'_>,
    maximum: usize,
) -> Result<BTreeSet<Uuid>, capnp::Error> {
    if reader.len() as usize > maximum {
        return Err(capnp_error("node ID list exceeds its bound"));
    }
    let mut nodes = BTreeSet::new();
    for value in reader.iter() {
        let node = read_uuid(value?, "node ID")?;
        if node.is_nil() || !nodes.insert(node) {
            return Err(capnp_error("node ID list contains zero or duplicate UUID"));
        }
    }
    Ok(nodes)
}

/// Reads one exact non-zero operation identity.
fn read_operation_id(bytes: &[u8]) -> Result<OperationId, capnp::Error> {
    OperationId::new(read_uuid(bytes, "bootstrap ID")?).map_err(capnp_error)
}

/// Reads one UUID from its exact sixteen-byte representation.
fn read_uuid(bytes: &[u8], field: &'static str) -> Result<Uuid, capnp::Error> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| capnp_error(format!("{field} must contain exactly 16 bytes")))?;
    Ok(Uuid::from_bytes(bytes))
}

/// Returns a conservative transport reservation for one storage request.
fn storage_request_size(descriptor: &VolumeDescriptor, node_count: usize) -> usize {
    descriptor_message_bytes(descriptor)
        .saturating_add(node_count.saturating_mul(32))
        .saturating_add(4096)
}

/// Converts one displayable internal failure into Cap'n Proto failure.
fn capnp_error(error: impl std::fmt::Display) -> capnp::Error {
    capnp::Error::failed(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Proves transient joint-consensus observations retain all four voters.
    #[test]
    fn status_reader_accepts_joint_replacement_membership() {
        let voters = (1..=4).map(Uuid::from_u128).collect::<BTreeSet<_>>();
        let mut message = capnp::message::Builder::new_default();
        {
            let mut root = message.init_root::<local_replica_status::Builder<'_>>();
            root.set_state(WireReplicaState::Ready);
            root.set_health(WireReplicaHealth::NeedsRecovery);
            write_node_ids(root.reborrow().init_voter_node_ids(4), &voters);
        }

        let reader = message
            .get_root_as_reader::<local_replica_status::Reader<'_>>()
            .expect("test status should be readable");
        let status = read_status(reader, 4).expect("four configured voters should fit");

        assert_eq!(status.voter_node_ids, voters);
        assert_eq!(status.health, ReplicaHealth::NeedsRecovery);
    }
}
