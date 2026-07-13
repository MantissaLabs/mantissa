@0xc6bf7606b8c44bc3;

using import "gossip.capnp".Gossip;
using import "topology.capnp".Topology;
using import "topology.capnp".NodeInfo;
using Node = import "node.capnp";
using import "sync.capnp".Sync;
using import "health.capnp".Health;
using import "task.capnp".Task;
using import "workload.capnp".Workload;
using import "jobs.capnp".Jobs;
using import "agents.capnp".Agents;
using import "services.capnp".Services;
using import "scheduling.capnp".Scheduler;
using import "secrets.capnp".Secrets;
using import "secrets.capnp".SecretMasterKeySyncRecord;
using import "network.capnp".Networks;
using import "ingress.capnp".Ingress;
using import "volumes.capnp".Volumes;
using import "rest.capnp".RestAdmin;
using import "topology.capnp".ClusterViewId;

interface Server {
  registerNode @0 (info :NodeInfo, token :Text) -> (response :RegisterNodeResponse);
  # First-time join. Adding the node to the trusted set of peers if the token
  # is valid. On failure, returns a capnp error.

  getSession @1 (ticket :Data) -> (result :SessionBootstrapResult);
  # Get a session given a ticket returned by registerNode. Expected
  # authentication and membership refusals are returned as typed rejection
  # codes; transport/decode failures still surface as Cap'n Proto errors.

  getWithCredential @2 (credential :Data) -> (result :CredentialBootstrapResult);
  # Bootstrap to (re)open a session on this node using a short-lived credential.
  # Used after join to contact other neighbors in the mesh/network.
}

enum SessionBootstrapRejectionCode {
  unknownSessionTicket @0;
  # The presented ticket is not known by this session authority.

  peerNotRegistered @1;
  # The subject is not an active peer on this node.

  localNodeInactive @2;
  # This node has left the cluster and will not mint peer sessions.

  credentialInvalid @3;
  # The credential could not be decoded, verified, or is expired.

  issuerMismatch @4;
  # The credential issuer does not match the subject's known signing key.

  issuerUnknown @5;
  # The subject is known, but its signing key has not converged locally yet.
}

struct SessionBootstrapRejection {
  code @0 :SessionBootstrapRejectionCode;
  # Stable machine-readable rejection code.

  detail @1 :Text;
  # Human-facing diagnostic detail for logs and debugging.
}

struct SessionBootstrapResult {
  union {
    accepted @0 :ClusterSession;
    # Session accepted and ready to use.

    rejected @1 :SessionBootstrapRejection;
    # Session bootstrap was refused for a typed, expected reason.
  }
}

struct CredentialBootstrapAccepted {
  session @0 :ClusterSession;
  # Cluster session capability granted to the credential subject.

  ticket @1 :Data;
  # Fresh session ticket for future ticket bootstrap.

  nodeInfo @2 :NodeInfo;
  # Server node metadata the caller should persist locally.

  ticketExpiresAtUnixSecs @3 :UInt64;
  # Absolute ticket expiry timestamp, 0 when no expiry is available.
}

struct CredentialBootstrapResult {
  union {
    accepted @0 :CredentialBootstrapAccepted;
    # Credential accepted and exchanged for a session plus fresh ticket.

    rejected @1 :SessionBootstrapRejection;
    # Credential bootstrap was refused for a typed, expected reason.
  }
}

struct RegisterNodeResponse {
  session @0 :ClusterSession;
  # Cluster session capability granted to the accepted node.

  ticket @1 :Data;
  # Short-lived session ticket for reconnecting to the anchor.

  nodeInfo @2 :NodeInfo;
  # Anchor node metadata the joiner should persist locally.

  credential @3 :Data;
  # Renewable cluster credential signed by the anchor.

  ticketExpiresAtUnixSecs @4 :UInt64;
  # Absolute ticket expiry timestamp, 0 when no expiry is available.

  masterKeyRecords @5 :List(SecretMasterKeySyncRecord);
  # Replicated master-key rows seeded into the joiner so join does not wait
  # for a separate anti-entropy round before it can adopt the cluster key.
}

struct ClusterCredential {
  issuer @0 :Data;
  # Ed25519 verifying key bytes for the node that signed the credential.

  subject @1 :Node.NodeId;
  # Node identity this credential authenticates.

  notAfterUnixSecs @2 :UInt64;
  # Absolute unix timestamp after which the credential is invalid.

  nonce @3 :Data;
  # Per-credential random bytes included in the signed payload.

  signature @4 :Data;
  # Ed25519 signature over the canonical credential message.
}

struct SessionTicketRecord {
  ticket @0 :Data;
  # Opaque ticket bytes returned by the remote server session authority.

  issuedAtUnixSecs @1 :UInt64;
  # Local unix timestamp when this ticket was cached.

  hasExpiresAt @2 :Bool;
  # True when `expiresAtUnixSecs` contains an absolute expiry timestamp.

  expiresAtUnixSecs @3 :UInt64;
  # Optional absolute expiry timestamp for this cached ticket.

  hasNote @4 :Bool;
  # True when `note` contains an operator-facing hint.

  note @5 :Text;
  # Optional human-readable hint associated with the cached ticket.
}

interface ClusterSession {
  # ClusterSession is the top level interface that gives access to a node's capabilities.
  # Access to a given service is granted only if a node has proper permission.

  ping @0 ();
  # Lightweight liveness check on the session.

  getCapabilities @1 () -> (caps :Capabilities);
  # One-call bootstrap to get all capabilities.

  getTopology @2 () -> (topology :Topology);
  # Access the topology management interface.

  getSync @3 () -> (sync :Sync);
  # Access the anti-entropy/sync interface.

  getNode @4 () -> (node :Node.Node);
  # Access the node info interface.

  getGossip @5 () -> (gossip :Gossip);
  # Access the gossip interface.

  getTask @6 () -> (task :Task);
  # Access the task control interface.

  getWorkload @15 () -> (workload :Workload);
  # Access the internal workload control interface.

  getScheduler @7 () -> (scheduler :Scheduler);
  # Access the scheduling interface.

  getServices @8 () -> (services :Services);
  # Access the services control interface.

  getJobs @13 () -> (jobs :Jobs);
  # Access the jobs control interface.

  getAgents @14 () -> (agents :Agents);
  # Access the agent session control interface.

  getSecrets @9 () -> (secrets :Secrets);
  # Access the secrets interface.

  getNetworks @10 () -> (networks :Networks);
  # Access the networks interface.

  getIngress @17 () -> (ingress :Ingress);
  # Access the public ingress interface.

  getVolumes @11 () -> (volumes :Volumes);
  # Access the volumes interface.

  getClusterView @12 () -> (view :ClusterViewId);
  # Returns the active cluster view associated with this session.

  getRestAdmin @16 () -> (restAdmin :RestAdmin);
  # Access the node-local REST facade administration interface.

  notifyWorkloadRowsAvailable @18 ();
  # Tell this node that the authenticated peer has workload rows available.
  # The receiver prioritizes pulling the workload domain from that peer through
  # the existing MST sync path.
}

struct Capabilities {
  gossip @0 :Gossip;
  # Gossip interface capability.

  topology @1 :Topology;
  # Topology interface capability.

  node @2 :Node.Node;
  # Node info interface capability.

  sync @3 :Sync;
  # Sync/anti-entropy interface capability.

  health @4 :Health;
  # Health interface capability.

  task @5 :Task;
  # Task interface capability.

  workload @14 :Workload;
  # Internal workload interface capability.

  restAdmin @15 :RestAdmin;
  # Node-local REST facade administration interface.

  scheduler @6 :Scheduler;
  # Scheduler interface capability.

  services @7 :Services;
  # Services interface capability.

  jobs @12 :Jobs;
  # Jobs interface capability.

  agents @13 :Agents;
  # Agents interface capability.

  secrets @8 :Secrets;
  # Secrets interface capability.

  networks @9 :Networks;
  # Networks interface capability.

  ingress @16 :Ingress;
  # Public ingress interface capability.

  volumes @10 :Volumes;
  # Volumes interface capability.

  activeView @11 :ClusterViewId;
  # Active cluster view for this capability bundle.
}
