use crate::network::naming::host_access_peer_iface_name;
use crate::network::types::{BpfAttachPoint, BpfProgramSpec, NetworkSpecValue};
use anyhow::Result;
use uuid::Uuid;

/// Identifies the kernel interfaces Mantissa programs with eBPF for a specific overlay network.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Clone, Debug)]
pub struct NetworkInterfaceContext {
    network_id: Uuid,
    bridge_ifname: String,
    vxlan_ifname: String,
    host_peer_ifname: String,
    attachment_host_ifnames: Vec<String>,
}

impl NetworkInterfaceContext {
    /// Construct a context bundle so the runtime knows which interfaces to target for program
    /// attachment when bringing an overlay network online.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn new(
        network_id: Uuid,
        bridge_ifname: impl Into<String>,
        vxlan_ifname: impl Into<String>,
    ) -> Self {
        let bridge_ifname = bridge_ifname.into();
        let vxlan_ifname = vxlan_ifname.into();
        Self {
            network_id,
            bridge_ifname,
            vxlan_ifname,
            host_peer_ifname: host_access_peer_iface_name(network_id),
            attachment_host_ifnames: Vec::new(),
        }
    }

    /// Attach the deterministic host-side veth names for local task attachments on this network.
    ///
    /// Bridge tc programs must run on every bridge-facing port that can carry service VIP
    /// traffic. Local task attachments use `mnth-*` host veths, so callers provide them here
    /// when they exist.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn with_attachment_host_ifnames<I, S>(mut self, ifnames: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.attachment_host_ifnames = ifnames.into_iter().map(Into::into).collect();
        self
    }

    /// Return the stable network identifier so telemetry and logs can attribute actions properly.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn network_id(&self) -> Uuid {
        self.network_id
    }

    /// Provide the bridge interface name that hosts container-side veth devices.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn bridge_ifname(&self) -> &str {
        self.bridge_ifname.as_str()
    }

    /// Provide the VXLAN interface name that handles encapsulation on the host.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn vxlan_ifname(&self) -> &str {
        self.vxlan_ifname.as_str()
    }

    /// Provide the bridge-port interface name that connects host traffic into the overlay bridge.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn host_peer_ifname(&self) -> &str {
        self.host_peer_ifname.as_str()
    }

    /// Provide the host-side attachment veth names that enter the bridge for local tasks.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub fn attachment_host_ifnames(&self) -> &[String] {
        self.attachment_host_ifnames.as_slice()
    }
}

/// Return the canonical overlay-program set Mantissa expects on each managed network.
///
/// Both the main controller reconcile path and the discovery repair path need the same declared
/// program bundle. Keeping the list here avoids those callers drifting apart over time.
pub(crate) fn overlay_bpf_program_specs() -> Vec<BpfProgramSpec> {
    vec![
        BpfProgramSpec::with_attach_point("vxlan_xdp", BpfAttachPoint::VxlanXdp),
        BpfProgramSpec::with_attach_point("bridge_xdp", BpfAttachPoint::BridgeXdp),
        BpfProgramSpec::with_attach_point("bridge_tc_ingress", BpfAttachPoint::BridgeTcIngress),
        BpfProgramSpec::with_attach_point("bridge_tc_egress", BpfAttachPoint::BridgeTcEgress),
    ]
}

/// Coordinates loading and unloading eBPF programs that accelerate Mantissa overlay networks.
#[derive(Clone)]
pub struct NetworkBpfManager {
    platform: PlatformBpfManager,
}

impl NetworkBpfManager {
    /// Build a manager backed by the host platform implementation so reconciliation loops can
    /// request eBPF attachments.
    pub fn new() -> Result<Self> {
        let platform = PlatformBpfManager::new()?;
        Ok(Self { platform })
    }

    /// Construct a manager that never attempts to touch the kernel, used when initialization fails
    /// or the host platform does not support eBPF features.
    pub fn unavailable() -> Self {
        Self {
            platform: PlatformBpfManager::unavailable(),
        }
    }

    /// Ensure the declared programs for a network are loaded and attached to the relevant
    /// interfaces before workloads begin sending traffic.
    pub async fn ensure_network(
        &self,
        spec: &NetworkSpecValue,
        interfaces: &NetworkInterfaceContext,
    ) -> Result<()> {
        if spec.bpf_programs.is_empty() {
            return Ok(());
        }
        match self.platform.ensure_programs(spec, interfaces).await {
            Ok(()) => Ok(()),
            Err(error) => {
                crate::observability::metrics::record_network_bpf_failure("ensure", "failed");
                Err(error)
            }
        }
    }

    /// Report whether ensuring this network will detach and rebuild local eBPF state.
    ///
    /// The network controller uses this before reconciling a live dataplane so it can demote the
    /// local peer from `Ready` to `Configuring` before traffic is still routed to a node whose
    /// bridge and BPF state are about to be reloaded.
    pub async fn requires_reload(
        &self,
        spec: &NetworkSpecValue,
        interfaces: &NetworkInterfaceContext,
    ) -> Result<bool> {
        if spec.bpf_programs.is_empty() {
            return Ok(false);
        }
        match self.platform.requires_reload(spec, interfaces).await {
            Ok(requires_reload) => Ok(requires_reload),
            Err(error) => {
                crate::observability::metrics::record_network_bpf_failure(
                    "requires_reload",
                    "failed",
                );
                Err(error)
            }
        }
    }

    /// Tear down any previously attached programs for the network so the kernel datapath stays
    /// clean when the overlay is removed or reconfigured.
    pub async fn teardown_network(&self, interfaces: &NetworkInterfaceContext) -> Result<()> {
        match self.platform.teardown_programs(interfaces).await {
            Ok(()) => Ok(()),
            Err(error) => {
                crate::observability::metrics::record_network_bpf_failure("teardown", "failed");
                Err(error)
            }
        }
    }
}

mod platform;

use self::platform::PlatformBpfManager;
