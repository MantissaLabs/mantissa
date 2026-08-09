use std::collections::BTreeMap;
use std::convert::Infallible;
use std::error::Error;
use std::future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use mantissa_raft::memory::{GroupState, InMemoryNode, InProcessNetwork};
use mantissa_raft::{
    ApplicationCommand, ApplicationResponse, ApplicationSnapshot, ApplicationStateMachine,
    ApplyContext, RaftApplication, SnapshotRead,
};
use openraft::{Config, EmptyNode, SnapshotPolicy};

const GROUP_WAIT: Duration = Duration::from_secs(5);
const NO_QUORUM_WAIT: Duration = Duration::from_millis(500);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Add {
    delta: u64,
}

impl ApplicationCommand for Add {}

#[derive(Debug, Eq, PartialEq)]
struct AddResult {
    value: u64,
    applied_index: u64,
}

impl ApplicationResponse for AddResult {}

#[derive(Debug)]
struct TestSnapshot;

impl ApplicationSnapshot for TestSnapshot {
    type Error = Infallible;

    /// Returns the complete empty snapshot used by this snapshot-disabled test.
    fn read_chunk(
        &mut self,
        _maximum_bytes: usize,
    ) -> impl Future<Output = Result<SnapshotRead, Self::Error>> + Send {
        future::ready(Ok(SnapshotRead::new(Vec::new(), true)))
    }

    /// Accepts no bytes because this runtime keeps snapshots disabled.
    fn write_chunk(
        &mut self,
        _bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        future::ready(Ok(()))
    }

    /// Completes the unused snapshot handle.
    fn finish_write(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        future::ready(Ok(()))
    }
}

struct TestApplication;

impl RaftApplication for TestApplication {
    type Command = Add;
    type Response = AddResult;
    type Snapshot = TestSnapshot;
    type Error = Infallible;
    type NodeId = u64;
    type Node = EmptyNode;
}

struct CounterStateMachine {
    value: Arc<AtomicU64>,
    _lifetime: Arc<()>,
}

impl ApplicationStateMachine<TestApplication> for CounterStateMachine {
    /// Applies one committed addition and reports its resulting value.
    fn apply(
        &mut self,
        context: ApplyContext,
        command: Add,
    ) -> impl Future<Output = Result<AddResult, Infallible>> + Send {
        let previous = self.value.fetch_add(command.delta, Ordering::SeqCst);
        future::ready(Ok(AddResult {
            value: previous + command.delta,
            applied_index: context.index(),
        }))
    }
}

struct TestCluster {
    network: InProcessNetwork<TestApplication>,
    nodes: BTreeMap<u64, InMemoryNode<TestApplication>>,
    values: BTreeMap<u64, Arc<AtomicU64>>,
    lifetimes: BTreeMap<u64, Weak<()>>,
}

impl TestCluster {
    /// Starts and initializes one test group with the requested voter IDs.
    async fn start(
        network: InProcessNetwork<TestApplication>,
        node_ids: &[u64],
        name: &str,
    ) -> Self {
        let mut nodes = BTreeMap::new();
        let mut values = BTreeMap::new();
        let mut lifetimes = BTreeMap::new();

        for node_id in node_ids {
            let value = Arc::new(AtomicU64::new(0));
            let lifetime = Arc::new(());
            let state_machine = CounterStateMachine {
                value: Arc::clone(&value),
                _lifetime: Arc::clone(&lifetime),
            };
            let node =
                InMemoryNode::start(*node_id, test_config(name), network.clone(), state_machine)
                    .await
                    .expect("test Raft node should start");

            nodes.insert(*node_id, node);
            values.insert(*node_id, value);
            lifetimes.insert(*node_id, Arc::downgrade(&lifetime));
        }

        let members = node_ids
            .iter()
            .map(|node_id| (*node_id, EmptyNode {}))
            .collect();
        nodes[node_ids.first().expect("test group needs one node")]
            .initialize(members)
            .await
            .expect("test Raft group should initialize");

        let leader = nodes[node_ids.first().expect("test group needs one node")]
            .wait_for_leader(GROUP_WAIT)
            .await
            .expect("test Raft group should elect a leader");
        for node in nodes.values() {
            let observed = node
                .wait_for_leader(GROUP_WAIT)
                .await
                .expect("every voter should observe a leader");
            assert_eq!(leader, observed);
        }

        Self {
            network,
            nodes,
            values,
            lifetimes,
        }
    }

    /// Returns one test node by its stable ID.
    fn node(&self, node_id: u64) -> &InMemoryNode<TestApplication> {
        &self.nodes[&node_id]
    }

    /// Returns the leader currently observed by the first test node.
    fn leader(&self) -> u64 {
        self.nodes
            .values()
            .next()
            .expect("test group should contain a node")
            .metrics()
            .leader
            .expect("initialized test group should have a leader")
    }

    /// Returns the application value currently held by one replica.
    fn value(&self, node_id: u64) -> u64 {
        self.values[&node_id].load(Ordering::SeqCst)
    }

    /// Waits until every replica has applied at least the supplied log index.
    async fn wait_all_applied(&self, log_index: u64) {
        for node in self.nodes.values() {
            node.wait_for_applied(log_index, GROUP_WAIT)
                .await
                .expect("every voter should apply the committed command");
        }
    }

    /// Shuts down every member and proves its worker-owned state was dropped.
    async fn shutdown(self) {
        for node in self.nodes.into_values() {
            node.shutdown()
                .await
                .expect("test Raft node should shut down");
        }

        assert_eq!(0, self.network.active_node_count());
        for lifetime in self.lifetimes.into_values() {
            assert!(
                lifetime.upgrade().is_none(),
                "state-machine worker survived shutdown"
            );
        }
    }
}

/// Builds a fast test-only timing configuration with snapshots disabled.
fn test_config(name: &str) -> Config {
    Config {
        cluster_name: name.to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        snapshot_policy: SnapshotPolicy::Never,
        ..Config::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_node_group_replicates_and_restarts_without_leaks() -> Result<(), Box<dyn Error>> {
    let network = InProcessNetwork::new();

    for generation in 0..3 {
        let cluster =
            TestCluster::start(network.clone(), &[1], &format!("one-node-{generation}")).await;
        let result = cluster
            .node(1)
            .write(Add {
                delta: generation + 1,
            })
            .await?;

        cluster.wait_all_applied(result.log_id.index).await;
        assert_eq!(generation + 1, result.response.value);
        assert_eq!(result.log_id.index, result.response.applied_index);
        assert_eq!(generation + 1, cluster.value(1));

        let metrics = cluster.node(1).metrics();
        assert_eq!(GroupState::Leader, metrics.state);
        assert_eq!(Some(1), metrics.leader);

        cluster.shutdown().await;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_group_elects_and_applies_on_every_replica() -> Result<(), Box<dyn Error>> {
    let cluster = TestCluster::start(InProcessNetwork::new(), &[1, 2, 3], "three-node").await;
    let leader = cluster.leader();
    let result = cluster.node(leader).write(Add { delta: 7 }).await?;

    cluster.wait_all_applied(result.log_id.index).await;
    assert_eq!(7, result.response.value);
    assert_eq!(result.log_id.index, result.response.applied_index);

    for node_id in [1, 2, 3] {
        assert_eq!(7, cluster.value(node_id));
        assert_eq!(Some(leader), cluster.node(node_id).metrics().leader);
    }
    assert_eq!(
        1,
        cluster
            .nodes
            .values()
            .filter(|node| node.metrics().state == GroupState::Leader)
            .count()
    );

    cluster.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leadership_moves_to_the_selected_voter() -> Result<(), Box<dyn Error>> {
    let cluster = TestCluster::start(InProcessNetwork::new(), &[1, 2, 3], "move-leader").await;
    let old_leader = cluster.leader();
    let new_leader = [1, 2, 3]
        .into_iter()
        .find(|node_id| *node_id != old_leader)
        .expect("three-node group must have another voter");

    cluster
        .node(old_leader)
        .move_leadership_to(new_leader, GROUP_WAIT)
        .await?;

    for node in cluster.nodes.values() {
        assert_eq!(Some(new_leader), node.metrics().leader);
    }
    assert_eq!(
        1,
        cluster
            .nodes
            .values()
            .filter(|node| node.metrics().state == GroupState::Leader)
            .count()
    );

    let result = cluster.node(new_leader).write(Add { delta: 9 }).await?;
    cluster.wait_all_applied(result.log_id.index).await;
    for node_id in [1, 2, 3] {
        assert_eq!(9, cluster.value(node_id));
    }

    cluster.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn isolated_minority_cannot_commit_and_is_repaired_by_the_majority()
-> Result<(), Box<dyn Error>> {
    let cluster =
        TestCluster::start(InProcessNetwork::new(), &[1, 2, 3], "minority-isolation").await;
    let original_leader = cluster.leader();
    let initial = cluster
        .node(original_leader)
        .write(Add { delta: 1 })
        .await?;
    cluster.wait_all_applied(initial.log_id.index).await;

    cluster.network.isolate(&original_leader);
    let stalled = tokio::time::timeout(
        NO_QUORUM_WAIT,
        cluster.node(original_leader).write(Add { delta: 100 }),
    )
    .await;
    assert!(
        stalled.is_err(),
        "an isolated one-node minority committed a command"
    );
    assert_eq!(1, cluster.value(original_leader));

    let connected_peer = [1, 2, 3]
        .into_iter()
        .find(|node_id| *node_id != original_leader)
        .expect("three-node group should have a connected peer");
    let majority_leader = cluster
        .node(connected_peer)
        .wait_for_leader_change(&original_leader, GROUP_WAIT)
        .await?;
    let committed = cluster
        .node(majority_leader)
        .write(Add { delta: 4 })
        .await?;

    for node_id in [1, 2, 3]
        .into_iter()
        .filter(|node_id| *node_id != original_leader)
    {
        cluster
            .node(node_id)
            .wait_for_applied(committed.log_id.index, GROUP_WAIT)
            .await?;
        assert_eq!(5, cluster.value(node_id));
    }
    assert_eq!(1, cluster.value(original_leader));

    cluster.network.restore(&original_leader);
    cluster.wait_all_applied(committed.log_id.index).await;
    for node_id in [1, 2, 3] {
        assert_eq!(5, cluster.value(node_id));
    }

    cluster.shutdown().await;
    Ok(())
}
