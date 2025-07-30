use node::info::System;

/// This structure defines the delegate in charge of booking slots
/// running tasks on the machine.
pub struct Node {
    system_info: System,
    // engine: Rc<Engine>,
}

// NodeState contains all of the node transitions during its lifetime.
// Change in state could occur when receiving messages from other peers,
// or performing actions like joining or leaving the cluster.
pub enum NodeState {
    // Node is initializing hardware, setting up network interfaces, etc.
    Initializing,

    // Node is ready but has not joined any cluster yet
    Bootstrapped,

    // Node is attempting to join a cluster
    JoiningCluster,

    // Node has joined the cluster but has not synchronized its state
    PartiallySynchronized,

    // Node is fully synchronized and participating in the cluster
    Active,

    // Node is active but is currently running at its resource limits
    ResourceConstrained,

    // Node is in the process of leaving the cluster
    LeavingCluster,

    // Node has left the cluster but is still running
    LeftCluster,

    // Node is disconnecting from the network, releasing resources, etc.
    ShuttingDown,

    // Node is fully shut down
    Stopped,

    // Node is isolated from the rest of the cluster (network partition, etc.)
    NetworkIsolated,

    // Node is in a state of recovering from failures or inconsistencies
    Recovering,

    // Node is in maintenance mode, not participating in scheduling but part of cluster
    Maintenance,
}
