@0xb4a5acd2fc1e5d0b;

using Node = import "node.capnp";
using import "server.capnp".Server;
using import "info.capnp".Info;
using import "sync.capnp".Sync;
using import "health.capnp".NodeStatus;

interface Topology {
  # Topology defines operations to join or leave a
  # pool of servers.

  join @0 (request :JoinRequest) -> (resp :JoinResponse);
  # Join an existing pool of servers using an anchor address.
  # This method signals the intent to join. The next step is
  # to register the node on the Server interface which is
  # gating access to a ClusterSession handle.

  leave @1 () -> ();
  # Leave the pool.

  list @2 () -> (nodes :NodeList);
  # List machines in the cluster.

  showToken @3 () -> (token :Text);
  # Show the token for other nodes to use during join.

  rotateToken @4 () -> (token :Text);
  # Rotates the token for the node, invalidates existing token.

  getClusterView @5 () -> (view :ClusterViewId);
  # Returns the local node's currently active cluster view identifier.

  mergeClusters @6 (req :MergeRequest) -> (op :ClusterOperation);
  # Starts a merge operation between source and destination views.

  splitCluster @7 (req :SplitRequest) -> (op :ClusterOperation);
  # Starts a split operation from one source view into target views.

  getClusterOperation @8 (id :Data) -> (op :ClusterOperation);
  # Fetches the latest known state for a cluster operation id.

  submitClusterOperation @9 (id :Data, payload :Data) -> ();
  # Replicates a serialized cluster operation record to this node.

  listClusterViews @10 () -> (views :List(ClusterViewSummary));
  # Lists known cluster views and per-view node counts from this node's control-plane perspective.

  listSplitCandidates @11 (sourceView :ClusterViewId) -> (nodes :List(SplitCandidate));
  # Lists node candidates and host metadata used to prepare interactive split assignments.

  setClusterName @12 (clusterId :ClusterId, name :Text) -> ();
  # Sets or updates the friendly name for one cluster lineage identifier.

  submitClusterName @13 (
    clusterId :ClusterId,
    name :Text,
    updatedAtUnixMs :UInt64,
    actorNodeId :Node.NodeId
  ) -> ();
  # Replicates one cluster-name update payload to this node.

  drainNode @14 (
    nodeId :Node.NodeId,
    reason :Text,
    taskStopTimeoutSecs :UInt32
  ) -> ();
  # Marks one node unschedulable for maintenance and starts cluster-wide drain fencing.

  resumeNode @15 (nodeId :Node.NodeId) -> ();
  # Clears maintenance fencing so one node can receive placements again.

  getNodeDrainStatus @16 (nodeId :Node.NodeId) -> (status :NodeDrainStatus);
  # Returns the best-known drain progress and diagnostics for one node.

  setNodeLabels @17 (
    nodeId :Node.NodeId,
    labels :List(Text),
    removeKeys :List(Text),
    replace :Bool
  ) -> ();
  # Applies node labels to one peer entry and relays the converged update through topology gossip.

  evictNode @18 (nodeId :Node.NodeId) -> ();
  # Evicts a node or one stale peer identity from the cluster given a node ID.
}

enum NodeDrainState {
  open @0;
  # Node is schedulable and has no active drain request.

  fenced @1;
  # Node is unschedulable without an active drain request.

  draining @2;
  # Drain is in progress and work or reservations remain on the node.

  drained @3;
  # Drain is complete and the node is empty from the scheduler's perspective.

  blocked @4;
  # Drain cannot make progress with the current cluster state.
}

enum NodeReadinessState {
  ready @0;
  # Node has completed bootstrap sync and may participate in scheduling.

  syncing @1;
  # Node is reachable but still synchronizing cluster state after join or restart.
}

struct NodeDrainStatus {
  nodeId @0 :Node.NodeId;
  # Node identifier this status row describes.

  schedulable @1 :Bool;
  # True when the node is eligible for new placements.

  drainRequested @2 :Bool;
  # True when maintenance drain was requested for the node.

  state @3 :NodeDrainState;
  # Derived operator-facing drain state.

  remainingServiceTasks @4 :UInt32;
  # Non-terminal service-managed tasks still assigned to the node.

  blockingStandaloneTasks @5 :UInt32;
  # Non-terminal standalone tasks that prevent safe drain completion.

  remainingReservedSlots @6 :UInt32;
  # Scheduler slots still reserved on the node.

  remainingReservedGpus @7 :UInt32;
  # Scheduler GPU devices still reserved on the node.

  schedulerSummaryKnown @8 :Bool;
  # False when reservation counts could not be fetched from the node.

  reason @9 :Text;
  # Operator-supplied drain reason when one was recorded.

  message @10 :Text;
  # Human-readable progress or blocker summary.

  lastSchedulingError @11 :Text;
  # Best-known scheduling blocker if drain is waiting on placement capacity.

  taskStopTimeoutSecs @12 :UInt32;
  # Optional drain-only override for task stop timeout in seconds, 0 uses task defaults.
}

struct TopologyEvent {
  # A TopologyEvent to be performed on remote peers, it is
  # gossiped to other nodes to add/remove peers, update liveness, and
  # propagate cluster-level metadata updates.

  event @0 :EventType;
  # Type of event performed on the topology for a given node.

  node @1 :NodeInfo;
  # Node information linked to the action.

  clusterId @2 :ClusterId;
  # Cluster lineage id used by `clusterNameUpdated`.

  clusterName @3 :Text;
  # Friendly cluster lineage name carried by `clusterNameUpdated`.

  updatedAtUnixMs @4 :UInt64;
  # Last-writer timestamp for one `clusterNameUpdated` payload.

  actorNodeId @5 :Node.NodeId;
  # Actor node id used for deterministic name conflict resolution.

  enum EventType {
      # Enumerates actions possible on the topology.

      add @0;
      remove @1;
      suspect @2;
      alive @3;
      down @4;
      clusterNameUpdated @5;
      nodeSchedulingUpdated @6;
      nodeLabelsUpdated @7;
      nodeReadinessUpdated @8;
  }
}

struct JoinRequest {
  anchor @0 :Text;
  # IP address of the anchor node we'd like this node to join.
  # This node could be part of an existing cluster or not.

  joinToken @1 :Text;
  # Token used to authenticate the join request.
}

struct JoinResponse {
  error @0 :Text;
  # Error message when join fails; empty on success.
}

struct NodeInfo {
  # A Machine. Can be any process taking part
  # in the system throughout the cluster lifetime.

  id @0 :Node.NodeId;
  # ID of the node.

  handle @1 :Server;
  # Live interface to contact the node back on the current RPC connection.

  peer @2 :Peer;
  # Durable peer metadata advertised by the node.

  info @3 :Info;
  # Machine resource usage.

  rootHash @4 :Text;
  # The root hash of the tracking merkle search tree.
  # It is used for Anti-Antropy and syncing data between
  # nodes.

  health @5 :NodeStatus;
  # Health status of the node.

  activeClusterView @6 :ClusterViewId;
  # Active cluster view currently used by this node for control-plane operations.

  drainState @7 :NodeDrainState;
  # Derived maintenance progress state used by `Topology.list` output.

  readinessState @8 :NodeReadinessState;
  # Bootstrap/sync readiness state used by `Topology.list` output.
}

enum PeerMembershipState {
  active @0;
  # The peer row represents a member that should participate in the cluster.

  left @1;
  # The peer row represents a graceful leave marker for this node identity.
}

struct Peer {
  # Durable peer metadata shared by topology RPC and the peer store.

  address @0 :Text;
  # IP address and port advertised for direct node-to-node connections.

  hostname @1 :Text;
  # Hostname of the node.

  platformOs @2 :Text;
  # Canonical scheduler-visible operating-system identifier for placement selectors.

  platformArch @3 :Text;
  # Canonical scheduler-visible architecture identifier for placement selectors.

  noiseStaticPub @4 :Data;
  # The node's static public key used in secure communications.

  signingPub @5 :Data;
  # Ed25519 public key for signed cluster credentials.

  identitySig @6 :Data;
  # Ed25519 signature binding the node id, noise key, and signing key.

  wireguardPublicKey @7 :Data;
  # Optional WireGuard public key used to encrypt the VXLAN underlay.
  # Empty means the node is not advertising WireGuard capability yet.

  wireguardPort @8 :UInt16;
  # UDP listen port for WireGuard. 0 means "reuse the port from `address`".

  wireguardEnabled @9 :Bool;
  # True once the node has created and configured its WireGuard interface.

  schedulable @10 :Bool = true;
  # True when this node is allowed to receive new workload placements.

  drainRequested @11 :Bool;
  # True when maintenance drain has been requested for this node.

  schedulingUpdatedAtUnixMs @12 :UInt64;
  # Last-writer timestamp for scheduling state convergence.

  schedulingActorNodeId @13 :Data;
  # Actor node id used to resolve scheduling-state conflicts deterministically.

  schedulingReason @14 :Text;
  # Optional operator-supplied maintenance reason for diagnostics.

  drainTaskStopTimeoutSecs @15 :UInt32;
  # Optional drain-only override for task stop timeout in seconds, 0 uses task defaults.

  labels @16 :List(Text);
  # Operator-supplied node labels encoded as `key=value` assignments.

  labelsUpdatedAtUnixMs @17 :UInt64;
  # Last-writer timestamp for label-state convergence.

  labelsActorNodeId @18 :Data;
  # Actor node id used to resolve label-state conflicts deterministically.

  executionPlatforms @19 :List(Text);
  # Execution platforms this node can host, for example "oci" or "microvm".

  isolationModes @20 :List(Text);
  # Isolation contracts this node can satisfy, for example "standard" or "sandboxed".

  isolationProfiles @21 :List(Text);
  # Optional named isolation profiles this node can satisfy for workload placement.

  runtimeFeatureFlags @22 :List(Text);
  # Runtime-specific feature flags such as "exec" or "lifecycle_events".

  minimumRootSchemaVersion @23 :UInt32 = 1;
  # Lowest semantic root schema version this node binary still serves.

  supportedRootSchemaVersion @24 :UInt32 = 1;
  # Highest semantic root schema version this node binary can serve.

  rootSchemaUpdatedAtUnixMs @25 :UInt64;
  # Last publication time for this node's root-schema support range.

  rootSchemaPublicationGeneration @26 :UInt64;
  # Durable per-node publication order for root-schema support changes.

  membershipIncarnation @27 :UInt64;
  # SWIM-style incarnation number for membership conflict resolution.

  membershipState @28 :PeerMembershipState;
  # Durable membership state for the peer identity.

  readinessState @29 :NodeReadinessState;
  # Durable bootstrap/sync readiness state for the peer identity.

  readinessUpdatedAtUnixMs @30 :UInt64;
  # Last-writer timestamp for readiness state convergence.

  readinessActorNodeId @31 :Data;
  # Actor node id used to resolve readiness-state conflicts deterministically.
}

struct NodeList {
  nodes @0 :List(NodeInfo);
  # List of nodes currently known to the cluster.
}

struct ClusterId {
  value @0 :Data;
  # Stable 16-byte lineage identifier for a cluster.
}

struct ClusterViewId {
  clusterId @0 :ClusterId;
  # Stable lineage identifier for the cluster.

  epoch @1 :UInt64;
  # Monotonically increasing view epoch.
}

struct ClusterViewSummary {
  view @0 :ClusterViewId;
  # Concrete cluster view represented by this summary row.

  nodeCount @1 :UInt32;
  # Number of known nodes currently associated with this view.

  localActive @2 :Bool;
  # True when this row corresponds to the local node's active view.

  clusterName @3 :Text;
  # Friendly cluster lineage name when one has been defined.
}

struct ClusterNameRecord {
  name @0 :Text;
  # Friendly cluster lineage name.

  updatedAtUnixMs @1 :UInt64;
  # Wall-clock update time used as the primary conflict-resolution order.

  actorNodeId @2 :Data;
  # 16-byte node id that authored the update.
}

struct ClusterNodeCountRecord {
  nodeCount @0 :UInt32;
  # Last published member count for the cluster lineage.

  updatedAtUnixMs @1 :UInt64;
  # Wall-clock update time used as the primary conflict-resolution order.

  actorNodeId @2 :Data;
  # 16-byte node id that authored the update.
}

struct ClusterViewMetadataRecord {
  name @0 :ClusterNameRecord;
  # Optional friendly-name metadata for the cluster lineage.

  nodeCount @1 :ClusterNodeCountRecord;
  # Optional member-count metadata for the cluster lineage.
}

struct SplitCandidate {
  nodeId @0 :Node.NodeId;
  # Candidate node identifier.

  hostname @1 :Text;
  # Hostname reported by the candidate node.

  addr @2 :Text;
  # Advertised endpoint for the candidate node.

  health @3 :NodeStatus;
  # Most recent health state observed for the candidate node.

  activeClusterView @4 :ClusterViewId;
  # Best-known active cluster view for this candidate.

  cpuVendor @5 :Text;
  # CPU vendor string when available.

  cpuBrand @6 :Text;
  # CPU brand/model string when available.

  cpuLogical @7 :UInt64;
  # Logical CPU count.

  cpuCores @8 :UInt64;
  # Physical core count.

  memoryTotalKb @9 :UInt64;
  # Total memory in KiB.

  gpuVendor @10 :Text;
  # GPU vendor string when available.

  gpuCount @11 :UInt64;
  # Number of GPU devices detected.

  gpuModels @12 :List(Text);
  # GPU model names detected on the host.

  wireguardEnabled @13 :Bool;
  # Whether this node has WireGuard dataplane enabled.

  labels @14 :List(Text);
  # Operator-managed node labels encoded as `key=value` assignments.
}

enum ClusterOperationKind {
  merge @0;
  split @1;
}

enum SplitServicePolicy {
  partitioned @0;
  # Keep service control-plane scoped per split target by pruning out-of-scope task runtime state.

  preserve @1;
  # Preserve existing service/task runtime rows as-is after split.
}

enum SplitNetworkPolicy {
  isolate @0;
  # Isolate overlays per split target by pruning out-of-scope network peer/attachment state.

  preserve @1;
  # Preserve existing network peer/attachment rows as-is after split.
}

enum MergeServicePolicy {
  rebalance @0;
  # Trigger post-merge service reconciliation so replicas can rebalance across the merged cluster.

  preserve @1;
  # Preserve service runtime placement after merge without reconciliation hints.
}

enum ClusterOperationStage {
  proposed @0;
  prepared @1;
  committed @2;
  finalized @3;
  aborted @4;
}

struct ClusterOperation {
  id @0 :Data;
  # Operation id (UUID bytes).

  kind @1 :ClusterOperationKind;
  # Kind of operation being executed.

  stage @2 :ClusterOperationStage;
  # Current stage in the operation state machine.

  sourceViews @3 :List(ClusterViewId);
  # Source cluster views involved in the operation.

  targetViews @4 :List(ClusterViewId);
  # Target cluster views resulting from the operation.

  details @5 :Text;
  # Human-readable details, including conflict hints.

  dryRun @6 :Bool;
  # True when the operation validates intent without committing state changes.

  targetClusterNames @7 :List(Text);
  # Friendly lineage names assigned to split target views.

  splitAssignments @8 :List(SplitNodeAssignment);
  # Deterministic node-to-target assignments for split operations.

  splitServicePolicy @9 :SplitServicePolicy;
  # Service behavior policy applied when the split commits.

  splitNetworkPolicy @10 :SplitNetworkPolicy;
  # Network behavior policy applied when the split commits.

  mergeServicePolicy @11 :MergeServicePolicy;
  # Service behavior policy applied when the merge commits.

  updatedAtUnixMs @12 :UInt64;
  # Last mutation time used for retention ordering and stale-row eviction.
}

struct SplitNodeAssignment {
  nodeId @0 :Node.NodeId;
  # Node assigned to one split target.

  targetIndex @1 :UInt64;
  # Index into the operation's target view list.
}

struct MergeRequest {
  sourceView @0 :ClusterViewId;
  # Source view that will be merged.

  destinationView @1 :ClusterViewId;
  # Destination view that receives source state.

  dryRun @2 :Bool;
  # If true, perform validation only and do not commit state changes.

  servicePolicy @3 :MergeServicePolicy;
  # Service behavior policy applied when the merge commits.
}

struct SplitSelectorClause {
  key @0 :Text;
  # Selector key (for example label or hardware attribute).

  op @1 :Operator;
  # Comparison operation.

  value @2 :Text;
  # Selector value encoded as text.

  enum Operator {
    eq @0;
    ne @1;
    gt @2;
    gte @3;
    lt @4;
    lte @5;
  }
}

struct SplitSelector {
  clauses @0 :List(SplitSelectorClause);
  # Conjunction of selector clauses.

  explicitNodes @1 :List(Node.NodeId);
  # Explicit node ids selected into this target partition.
}

struct SplitTarget {
  name @0 :Text;
  # Friendly target name for this partition.

  selector @1 :SplitSelector;
  # Selector rules for placing nodes into this target.
}

struct SplitRequest {
  sourceView @0 :ClusterViewId;
  # Source view that will be partitioned.

  targets @1 :List(SplitTarget);
  # Target partitions to materialize.

  dryRun @2 :Bool;
  # If true, validate only and do not commit state changes.

  servicePolicy @3 :SplitServicePolicy;
  # Service behavior policy applied when the split commits.

  networkPolicy @4 :SplitNetworkPolicy;
  # Overlay/network behavior policy applied when the split commits.
}
