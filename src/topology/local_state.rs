use std::cell::OnceCell;
use std::net::SocketAddr;
use std::rc::Rc;

use ed25519_dalek::SigningKey;
use mantissa_protocol::server::ServerClient;
use parking_lot::Mutex;
use x25519_dalek::PublicKey;

use crate::cluster::{ClusterViewState, RootSchemaState};
use crate::node::Node;
use crate::runtime::types::RuntimeSupportProfile;

#[derive(Clone)]
pub(super) struct AdvertiseState {
    /// Address string as configured on startup. Used as last-resort advertise addr.
    configured_addr: String,

    /// Socket address we actually bound to. Filled once networking stack listens.
    bound_addr: std::sync::Arc<Mutex<Option<SocketAddr>>>,

    /// Optional manual override (tests, inproc transports) for advertise address.
    advertise_override: std::sync::Arc<Mutex<Option<String>>>,
}

impl AdvertiseState {
    /// Creates advertise-state tracking for one topology instance.
    pub(super) fn new(configured_addr: String) -> Self {
        Self {
            configured_addr,
            bound_addr: std::sync::Arc::new(Mutex::new(None)),
            advertise_override: std::sync::Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the configured address fallback used before the listener binds.
    pub(super) fn configured(&self) -> &str {
        &self.configured_addr
    }

    /// Records the socket address currently bound by the server listener.
    pub(super) fn set_bound(&self, addr: SocketAddr) {
        *self.bound_addr.lock() = Some(addr);
    }

    /// Replaces the optional advertise override used by tests and inproc transports.
    pub(super) fn set_override<S: Into<String>>(&self, addr: Option<S>) {
        *self.advertise_override.lock() = addr.map(Into::into);
    }

    /// Returns the current advertise override when one has been configured.
    pub(super) fn override_addr(&self) -> Option<String> {
        self.advertise_override.lock().clone()
    }

    /// Returns the bound listener address when networking has already started.
    pub(super) fn bound(&self) -> Option<SocketAddr> {
        *self.bound_addr.lock()
    }
}

/// Groups local node state that topology publishes and mutates at runtime.
#[derive(Clone)]
pub(super) struct LocalNodeState {
    /// Snapshot of the local node (id, host info, capabilities).
    pub(super) node: Node,

    /// Shared active cluster view identifier for control-plane observability.
    pub(super) cluster_view: ClusterViewState,

    /// Shared root schema support range advertised and used for sync negotiation.
    pub(super) root_schema: RootSchemaState,

    /// Addresses and advertise decision logic for the local node.
    pub(super) advertise: AdvertiseState,

    /// OnceCell holding the Cap'n Proto server capability exported to peers.
    pub(super) server_handle: Rc<OnceCell<ServerClient>>,

    /// Local node Noise static public key used during handshakes.
    pub(super) public_key: PublicKey,

    /// Ed25519 signing key used to mint cluster credentials.
    pub(super) signing_key: SigningKey,

    /// Cluster-visible runtime support metadata published for this node.
    pub(super) runtime_support: RuntimeSupportProfile,
}
