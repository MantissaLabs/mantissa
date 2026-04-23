use crate::network::attachment::host_access_peer_iface_name;
use crate::network::types::NetworkSpecValue;
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
        self.platform.ensure_programs(spec, interfaces).await
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
        self.platform.requires_reload(spec, interfaces).await
    }

    /// Tear down any previously attached programs for the network so the kernel datapath stays
    /// clean when the overlay is removed or reconfigured.
    pub async fn teardown_network(&self, interfaces: &NetworkInterfaceContext) -> Result<()> {
        self.platform.teardown_programs(interfaces).await
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::NetworkInterfaceContext;
    use crate::config;
    use crate::network::allocator::{OverlayIpFamily, parse_overlay_cidr};
    use crate::network::types::{BpfAttachPoint, BpfProgramSpec, NetworkSpecValue};
    use anyhow::{Context, Result, anyhow};
    use aya::maps::MapData;
    use aya::pin::PinError;
    use aya::programs::tc::{
        SchedClassifierLinkId, TcAttachType, qdisc_add_clsact, qdisc_detach_program,
    };
    use aya::programs::xdp::XdpLinkId;
    use aya::programs::{SchedClassifier, Xdp, XdpFlags};
    use aya::{Ebpf, EbpfLoader};
    use libc::if_nametoindex;
    use nix::mount::{MsFlags, mount};
    use nix::sys::statfs::{BPF_FS_MAGIC, statfs};
    use std::collections::{HashMap, HashSet};
    use std::env;
    use std::ffi::CString;
    use std::fs;
    use std::io;
    use std::mem;
    use std::os::fd::{AsFd, AsRawFd};
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tracing::{debug, info, warn};
    use uuid::Uuid;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct BridgeRuntimeConfig {
        tcp_mss: u16,
        _pad: [u8; 6],
    }

    const IPV4_TCP_HEADER_BYTES: u32 = 40;
    const IPV6_TCP_HEADER_BYTES: u32 = 60;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum AttachTarget<'a> {
        Xdp {
            interface: &'a str,
        },
        Tc {
            interface: &'a str,
            attach_type: TcAttachType,
        },
    }

    type ProgramHandle = Box<dyn Detachable + Send>;

    trait Detachable {
        fn detach(&mut self) -> Result<()>;
    }

    trait ProgramLoader: Send + Sync {
        fn load_and_attach(
            &self,
            spec: &BpfProgramSpec,
            target: AttachTarget<'_>,
            artifact: &Path,
            map_pin_path: &Path,
            lb_family: Option<OverlayIpFamily>,
        ) -> Result<ProgramHandle>;
    }

    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
    struct DesiredAttachment {
        attach_point: BpfAttachPoint,
        name: String,
        interface: String,
    }

    #[derive(Clone)]
    pub struct PlatformBpfManager {
        resolver: ArtifactResolver,
        loader: Arc<dyn ProgramLoader>,
        loaded: Arc<AsyncMutex<HashMap<Uuid, LoadedNetwork>>>,
    }

    struct LoadedNetwork {
        programs: Vec<LoadedProgram>,
        lb_family: Option<OverlayIpFamily>,
    }

    impl LoadedNetwork {
        fn new(lb_family: Option<OverlayIpFamily>) -> Self {
            Self {
                programs: Vec::new(),
                lb_family,
            }
        }

        fn push(&mut self, program: LoadedProgram) {
            self.programs.push(program);
        }

        fn teardown(mut self) -> Result<()> {
            for program in self.programs.iter_mut() {
                program.handle.detach()?;
            }
            Ok(())
        }

        fn matches(
            &self,
            desired: &[DesiredAttachment],
            lb_family: Option<OverlayIpFamily>,
        ) -> bool {
            self.lb_family == lb_family && self.canonical_specs() == desired
        }

        fn canonical_specs(&self) -> Vec<DesiredAttachment> {
            let mut attachments: Vec<_> = self
                .programs
                .iter()
                .map(desired_attachment_for_loaded_program)
                .collect();
            attachments.sort();
            attachments
        }
    }

    struct LoadedProgram {
        spec: BpfProgramSpec,
        target: OwnedAttachTarget,
        _artifact: PathBuf,
        handle: ProgramHandle,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum OwnedAttachTarget {
        Xdp {
            interface: String,
        },
        Tc {
            interface: String,
            attach_type: TcAttachType,
        },
    }

    impl OwnedAttachTarget {
        fn as_ref(&self) -> AttachTarget<'_> {
            match self {
                OwnedAttachTarget::Xdp { interface } => AttachTarget::Xdp {
                    interface: interface.as_str(),
                },
                OwnedAttachTarget::Tc {
                    interface,
                    attach_type,
                } => AttachTarget::Tc {
                    interface: interface.as_str(),
                    attach_type: *attach_type,
                },
            }
        }
    }

    impl PlatformBpfManager {
        pub fn new() -> Result<Self> {
            Ok(Self {
                resolver: ArtifactResolver::new(),
                loader: default_loader(),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            })
        }

        pub fn unavailable() -> Self {
            Self {
                resolver: ArtifactResolver::new(),
                loader: Arc::new(NoopProgramLoader),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            }
        }

        /// Determine whether the next ensure pass will perform a destructive local reload.
        ///
        /// A `true` result means the currently loaded attachment set or pinned load-balancer maps
        /// do not match the desired state, so `ensure_programs()` will tear down and rebuild local
        /// eBPF state instead of reusing the current dataplane.
        pub async fn requires_reload(
            &self,
            network: &NetworkSpecValue,
            interfaces: &NetworkInterfaceContext,
        ) -> Result<bool> {
            let programs = &network.bpf_programs;
            if !config::bpf_attach_enabled() || programs.is_empty() {
                return Ok(false);
            }

            self.validate_programs(programs, interfaces)?;

            let network_id = interfaces.network_id();
            let lb_family = load_balancer_map_family(network)?;
            let mut canonical_desired = desired_attachments(programs, interfaces);
            canonical_desired.sort();

            let guard = self.loaded.lock().await;
            let Some(existing) = guard.get(&network_id) else {
                return Ok(true);
            };

            if !existing.matches(&canonical_desired, lb_family)
                && !can_incrementally_reconcile_loaded_network(
                    existing,
                    &canonical_desired,
                    lb_family,
                )
            {
                return Ok(true);
            }

            if let Some(family) = lb_family {
                let map_pin_path = Self::map_pin_path(network_id);
                if !lb_maps_present(&map_pin_path, family) {
                    return Ok(true);
                }
            }

            Ok(false)
        }

        pub async fn ensure_programs(
            &self,
            network: &NetworkSpecValue,
            interfaces: &NetworkInterfaceContext,
        ) -> Result<()> {
            let programs = &network.bpf_programs;
            if !config::bpf_attach_enabled() {
                tracing::debug!(
                    target: "network",
                    network = %interfaces.network_id(),
                    "skipping bpf ensure_programs because bpf attachment is disabled"
                );
                return Ok(());
            }
            if programs.is_empty() {
                return Ok(());
            }

            self.validate_programs(programs, interfaces)?;

            let network_id = interfaces.network_id();
            let lb_family = load_balancer_map_family(network)?;
            let desired = desired_attachments(programs, interfaces);
            let mut canonical_desired = desired.clone();
            canonical_desired.sort();
            let map_pin_path = Self::map_pin_path(network_id);
            {
                let guard = self.loaded.lock().await;
                if let Some(existing) = guard.get(&network_id)
                    && existing.matches(&canonical_desired, lb_family)
                {
                    if let Some(family) = lb_family
                        && !lb_maps_present(&map_pin_path, family)
                    {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "load balancer maps missing despite matching programs; forcing reload"
                        );
                    } else {
                        if let Some(family) = lb_family {
                            program_overlay_runtime_map(network, &map_pin_path, family)
                                .context("refresh overlay runtime config")?;
                        }
                        return Ok(());
                    }
                }
            }

            let mut guard = self.loaded.lock().await;
            if let Some(mut existing) = guard.remove(&network_id) {
                let can_reconcile_incrementally = can_incrementally_reconcile_loaded_network(
                    &existing,
                    &canonical_desired,
                    lb_family,
                );
                let maps_available = lb_family
                    .map(|family| lb_maps_present(&map_pin_path, family))
                    .unwrap_or(true);
                if can_reconcile_incrementally && maps_available {
                    drop(guard);
                    match self
                        .reconcile_incremental_tc_attachments(
                            network,
                            programs,
                            &desired,
                            &map_pin_path,
                            &mut existing,
                            lb_family,
                        )
                        .await
                    {
                        Ok(()) => {
                            let mut guard = self.loaded.lock().await;
                            guard.insert(network_id, existing);
                            return Ok(());
                        }
                        Err(err) => {
                            warn!(
                                target: "network",
                                network = %network_id,
                                "incremental task-attachment bpf reconcile failed; falling back to full reload: {err:#}"
                            );
                            if let Err(teardown_err) = existing.teardown() {
                                warn!(
                                    target: "network",
                                    network = %network_id,
                                    "failed to detach partially reconciled bpf programs before full reload: {teardown_err:#}"
                                );
                            }
                            if let Err(remove_err) = Self::remove_map_pin_dir(network_id) {
                                warn!(
                                    target: "network",
                                    network = %network_id,
                                    "failed to clear bpf map directory after incremental reconcile fallback: {remove_err:#}"
                                );
                            }
                        }
                    }
                } else {
                    drop(guard);
                    if let Some(family) = lb_family
                        && !maps_available
                    {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "load balancer maps missing before attachment reconcile; forcing full reload for {:?}",
                            family
                        );
                    }
                    if let Err(err) = existing.teardown() {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "failed to detach existing bpf programs before reattach: {err:#}"
                        );
                    }
                    if let Err(err) = Self::remove_map_pin_dir(network_id) {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "failed to clear stale bpf map directory before reattach: {err:#}"
                        );
                    }
                }
            } else {
                drop(guard);
            }

            let map_pin_path = Self::map_pin_dir(network_id)?;

            self.clear_stale_xdp_targets(programs, interfaces);
            clear_stale_bridge_tc_bridge_master_target(interfaces);

            let mut loaded_network = LoadedNetwork::new(lb_family);
            for desired_attachment in desired {
                let spec = programs
                    .iter()
                    .find(|program| {
                        program.attach_point() == desired_attachment.attach_point
                            && program.name == desired_attachment.name
                    })
                    .ok_or_else(|| {
                        anyhow!(
                            "missing bpf spec '{}' for attach point {:?}",
                            desired_attachment.name,
                            desired_attachment.attach_point
                        )
                    })?;
                let artifact = self
                    .resolver
                    .resolve(spec, network)
                    .with_context(|| format!("resolve artifact for program '{}'", spec))?;
                let attach_target = desired_target(&desired_attachment);
                let program_lb_family = program_load_balancer_family(spec, lb_family);

                info!(
                    target: "network",
                    network = %interfaces.network_id(),
                    program = %spec,
                    attach_point = %spec.attach_point(),
                    interface = %desired_attachment.interface,
                    artifact = %artifact.display(),
                    "attaching bpf program"
                );

                let handle = match load_with_retry(
                    &*self.loader,
                    spec,
                    attach_target,
                    &artifact,
                    &map_pin_path,
                    program_lb_family,
                ) {
                    Ok(handle) => handle,
                    Err(err) => {
                        if let Err(teardown_err) = loaded_network.teardown() {
                            warn!(
                                target: "network",
                                network = %network_id,
                                "failed to rollback partially attached bpf programs: {teardown_err:#}"
                            );
                        }
                        return Err(err);
                    }
                };

                loaded_network.push(LoadedProgram {
                    spec: spec.clone(),
                    target: own_target(attach_target),
                    _artifact: artifact,
                    handle,
                });
            }

            if let Some(family) = lb_family
                && let Err(err) = program_overlay_runtime_map(network, &map_pin_path, family)
                    .context("program overlay runtime config")
            {
                if let Err(teardown_err) = loaded_network.teardown() {
                    warn!(
                        target: "network",
                        network = %network_id,
                        "failed to rollback partially attached bpf programs after runtime-map error: {teardown_err:#}"
                    );
                }
                return Err(err);
            }

            let mut guard = self.loaded.lock().await;
            if let Some(replaced) = guard.insert(network_id, loaded_network)
                && let Err(err) = replaced.teardown()
            {
                warn!(
                    target: "network",
                    network = %network_id,
                    "failed to detach replaced bpf programs after ensure: {err:#}"
                );
            }
            Ok(())
        }

        pub async fn teardown_programs(&self, interfaces: &NetworkInterfaceContext) -> Result<()> {
            let mut guard = self.loaded.lock().await;
            let network = guard.remove(&interfaces.network_id());
            drop(guard);

            let network_id = interfaces.network_id();
            let detach_result = if let Some(network) = network {
                let result = network.teardown();
                if result.is_ok() {
                    info!(
                        target: "network",
                        network = %network_id,
                        "detached bpf programs"
                    );
                }
                result
            } else {
                debug!(
                    target: "network",
                    network = %network_id,
                    "no bpf programs recorded for teardown"
                );
                Ok(())
            };

            let cleanup_result = Self::remove_map_pin_dir(network_id);
            clear_stale_bridge_tc_host_peer_targets(interfaces);
            clear_stale_bridge_tc_bridge_master_target(interfaces);

            detach_result?;
            cleanup_result?;
            Ok(())
        }

        /// Remove the pinned bpffs directory for one network once no live dataplane references
        /// should remain.
        ///
        /// The pinned load-balancer maps are large, so leaving the directory behind after network
        /// deletion leaks kernel memory even when no userspace process is holding the maps open.
        fn remove_map_pin_dir(network_id: Uuid) -> Result<()> {
            let path = Self::map_pin_path(network_id);
            if !path.exists() {
                return Ok(());
            }
            match fs::remove_dir_all(&path) {
                Ok(()) => {
                    debug!(
                        target: "network",
                        network = %network_id,
                        path = %path.display(),
                        "removed pinned bpf map directory"
                    );
                    Ok(())
                }
                Err(err)
                    if err.kind() == io::ErrorKind::NotFound
                        || (err.kind() == io::ErrorKind::PermissionDenied && !path.exists()) =>
                {
                    Ok(())
                }
                Err(err) => Err(err)
                    .with_context(|| format!("remove pinned bpf map directory {}", path.display())),
            }
        }

        /// Return the stable bpffs pin directory for one overlay network.
        ///
        /// Keeping path construction separate from directory creation lets teardown remove stale
        /// pins without accidentally recreating the mount subtree during cleanup.
        fn map_pin_path(network_id: Uuid) -> PathBuf {
            PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string())
        }

        /// Ensure the per-network bpffs directory exists before Aya attempts to pin shared maps.
        ///
        /// This path is used during attach and reload only; teardown must use `map_pin_path`
        /// directly so cleanup never recreates an otherwise deleted pin directory.
        fn map_pin_dir(network_id: Uuid) -> Result<PathBuf> {
            ensure_bpffs().context("prepare bpffs mount")?;
            let path = Self::map_pin_path(network_id);
            fs::create_dir_all(&path)
                .with_context(|| format!("create map pin directory {}", path.display()))?;
            Ok(path)
        }

        fn validate_programs(
            &self,
            programs: &[BpfProgramSpec],
            interfaces: &NetworkInterfaceContext,
        ) -> Result<()> {
            let mut seen = HashSet::new();
            for desired in desired_attachments(programs, interfaces) {
                if !seen.insert((desired.attach_point, desired.interface.clone())) {
                    return Err(anyhow!(
                        "multiple programs declared for {} on {}",
                        desired.attach_point,
                        desired.interface
                    ));
                }
            }
            Ok(())
        }

        fn clear_stale_xdp_targets(
            &self,
            programs: &[BpfProgramSpec],
            interfaces: &NetworkInterfaceContext,
        ) {
            let mut seen = HashSet::new();
            let mut targets = Vec::new();
            for spec in programs {
                let point = spec.attach_point();
                if !matches!(point, BpfAttachPoint::BridgeXdp | BpfAttachPoint::VxlanXdp) {
                    continue;
                }
                let attach_target = match point {
                    BpfAttachPoint::VxlanXdp => AttachTarget::Xdp {
                        interface: interfaces.vxlan_ifname(),
                    },
                    BpfAttachPoint::BridgeXdp => AttachTarget::Xdp {
                        interface: interfaces.bridge_ifname(),
                    },
                    _ => continue,
                };
                let interface = target_interface(attach_target).to_string();
                if seen.insert(interface.clone()) {
                    targets.push((
                        detach_priority(point),
                        attach_target,
                        interface,
                        spec.to_string(),
                    ));
                }
            }

            targets.sort_by_key(|(priority, _, _, _)| *priority);
            for (_, target, interface, program) in targets {
                if let Err(err) = force_detach_target(target) {
                    warn!(
                        target: "network",
                        program = %program,
                        interface = %interface,
                        "failed to clear stale eBPF attachment: {err:#}"
                    );
                }
            }
        }

        /// Reconciles task-attachment bridge tc programs without tearing down the whole dataplane.
        ///
        /// Local `mnth-*` interfaces appear and disappear whenever service replicas move. Those
        /// changes should only add or drop the corresponding bridge tc attachments; they must not
        /// force a full bridge and map reload that interrupts unrelated VIP traffic on the node.
        async fn reconcile_incremental_tc_attachments(
            &self,
            network: &NetworkSpecValue,
            programs: &[BpfProgramSpec],
            desired: &[DesiredAttachment],
            map_pin_path: &Path,
            loaded_network: &mut LoadedNetwork,
            lb_family: Option<OverlayIpFamily>,
        ) -> Result<()> {
            let desired_set: HashSet<_> = desired.iter().cloned().collect();
            let current_set: HashSet<_> = loaded_network.canonical_specs().into_iter().collect();
            let additions: Vec<_> = desired_set.difference(&current_set).cloned().collect();
            let removals: HashSet<_> = current_set.difference(&desired_set).cloned().collect();

            if removals.is_empty() && additions.is_empty() {
                if let Some(family) = lb_family {
                    program_overlay_runtime_map(network, map_pin_path, family)
                        .context("refresh overlay runtime config")?;
                }
                return Ok(());
            }

            let mut retained = Vec::with_capacity(loaded_network.programs.len());
            for mut program in loaded_network.programs.drain(..) {
                let desired_program = desired_attachment_for_loaded_program(&program);
                if removals.contains(&desired_program) {
                    let interface = target_interface(program.target.as_ref()).to_string();
                    if interface_exists(&interface) {
                        program.handle.detach().with_context(|| {
                            format!(
                                "detach incremental tc program '{}' from {}",
                                program.spec.name, interface
                            )
                        })?;
                    }
                } else {
                    retained.push(program);
                }
            }
            loaded_network.programs = retained;

            let mut attached_programs = Vec::new();
            for desired_attachment in additions {
                let spec = programs
                    .iter()
                    .find(|program| {
                        program.attach_point() == desired_attachment.attach_point
                            && program.name == desired_attachment.name
                    })
                    .ok_or_else(|| {
                        anyhow!(
                            "missing bpf spec '{}' for incremental attach point {:?}",
                            desired_attachment.name,
                            desired_attachment.attach_point
                        )
                    })?;
                let artifact = self
                    .resolver
                    .resolve(spec, network)
                    .with_context(|| format!("resolve artifact for program '{}'", spec))?;
                let attach_target = desired_target(&desired_attachment);
                let program_lb_family = program_load_balancer_family(spec, lb_family);

                info!(
                    target: "network",
                    network = %network.id,
                    program = %spec,
                    attach_point = %spec.attach_point(),
                    interface = %desired_attachment.interface,
                    artifact = %artifact.display(),
                    "incrementally attaching bpf program"
                );

                let handle = load_with_retry(
                    &*self.loader,
                    spec,
                    attach_target,
                    &artifact,
                    map_pin_path,
                    program_lb_family,
                )?;

                attached_programs.push(LoadedProgram {
                    spec: spec.clone(),
                    target: own_target(attach_target),
                    _artifact: artifact,
                    handle,
                });
            }

            loaded_network.programs.extend(attached_programs);

            if let Some(family) = lb_family {
                program_overlay_runtime_map(network, map_pin_path, family)
                    .context("refresh overlay runtime config")?;
            }

            Ok(())
        }
    }

    fn desired_attachment_for_loaded_program(program: &LoadedProgram) -> DesiredAttachment {
        DesiredAttachment {
            attach_point: program.spec.attach_point(),
            name: program.spec.name.clone(),
            interface: target_interface(program.target.as_ref()).to_string(),
        }
    }

    fn can_incrementally_reconcile_loaded_network(
        existing: &LoadedNetwork,
        desired: &[DesiredAttachment],
        lb_family: Option<OverlayIpFamily>,
    ) -> bool {
        if existing.lb_family != lb_family {
            return false;
        }

        let current: HashSet<_> = existing.canonical_specs().into_iter().collect();
        let desired: HashSet<_> = desired.iter().cloned().collect();
        let removed = current.difference(&desired);
        let added = desired.difference(&current);

        removed
            .chain(added)
            .all(is_incremental_task_attachment_delta)
    }

    fn is_incremental_task_attachment_delta(attachment: &DesiredAttachment) -> bool {
        matches!(
            attachment.attach_point,
            BpfAttachPoint::BridgeTcIngress | BpfAttachPoint::BridgeTcEgress
        ) && attachment.interface.starts_with("mnth-")
    }

    #[derive(Clone)]
    struct ArtifactResolver {
        search_roots: Vec<PathBuf>,
    }

    impl ArtifactResolver {
        /// # Description:
        ///
        /// Build an artifact resolver using configured search roots so BPF bytecode can be found.
        fn new() -> Self {
            Self::new_with_config(&config::global_config())
        }

        /// # Description:
        ///
        /// Build an artifact resolver using the provided configuration snapshot.
        fn new_with_config(config: &crate::config::Config) -> Self {
            let mut roots = Vec::new();
            if let Some(dir) = config.network.bpf.artifact_dir.clone() {
                roots.push(PathBuf::from(dir));
            }
            if let Ok(pwd) = env::current_dir() {
                roots.push(pwd.join("target/bpf"));
                roots.push(pwd.join("assets/bpf"));
            }
            Self {
                search_roots: roots,
            }
        }

        fn resolve(&self, spec: &BpfProgramSpec, network: &NetworkSpecValue) -> Result<PathBuf> {
            let family_specific_name = load_balancer_map_family(network)?
                .and_then(|family| bridge_tc_artifact_name(spec, family));
            for candidate in self.candidates(family_specific_name.unwrap_or(spec.name.as_str())) {
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
            Err(anyhow!(
                "unable to locate bpf artifact '{}' (searched {:?})",
                spec.name,
                self.search_roots
            ))
        }

        fn candidates(&self, name: &str) -> Vec<PathBuf> {
            let mut out = Vec::new();
            let path = PathBuf::from(name);
            if path.is_absolute() || name.contains(std::path::MAIN_SEPARATOR) {
                out.push(path.clone());
                if path.extension().is_none() {
                    out.push(path.with_extension("bpf.o"));
                }
                return dedup(out);
            }

            for root in &self.search_roots {
                out.push(root.join(name));
                out.push(root.join(format!("{name}.bpf.o")));
                out.push(root.join(format!("{name}.o")));
            }

            dedup(out)
        }
    }

    /// # Description:
    ///
    /// Pick the program loader based on whether BPF attachment is enabled in config.
    fn default_loader() -> Arc<dyn ProgramLoader> {
        if !config::bpf_attach_enabled() {
            Arc::new(NoopProgramLoader)
        } else {
            Arc::new(AyaProgramLoader)
        }
    }

    fn dedup(paths: Vec<PathBuf>) -> Vec<PathBuf> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for path in paths {
            if seen.insert(path.clone()) {
                out.push(path);
            }
        }
        out
    }

    /// Expand one logical BPF program declaration into the concrete interface attachments it needs.
    ///
    /// Bridge tc programs must run on every local bridge-facing port that can carry overlay
    /// traffic: the VXLAN device, the host-access peer, and any currently present `mnth-*`
    /// attachment veths. This keeps task, remote, and host-originated service VIP traffic on the
    /// same dataplane instead of depending on one special interface.
    fn desired_attachments(
        programs: &[BpfProgramSpec],
        interfaces: &NetworkInterfaceContext,
    ) -> Vec<DesiredAttachment> {
        let mut desired = Vec::new();
        let mut ordered_programs: Vec<&BpfProgramSpec> = programs.iter().collect();
        ordered_programs.sort_by_key(|spec| attach_priority(spec.attach_point()));

        for spec in ordered_programs {
            let attach_point = spec.attach_point();
            match attach_point {
                BpfAttachPoint::VxlanXdp => desired.push(DesiredAttachment {
                    attach_point,
                    name: spec.name.clone(),
                    interface: interfaces.vxlan_ifname().to_string(),
                }),
                BpfAttachPoint::BridgeXdp => desired.push(DesiredAttachment {
                    attach_point,
                    name: spec.name.clone(),
                    interface: interfaces.bridge_ifname().to_string(),
                }),
                BpfAttachPoint::BridgeTcIngress | BpfAttachPoint::BridgeTcEgress => {
                    let mut ports = vec![
                        interfaces.vxlan_ifname().to_string(),
                        interfaces.host_peer_ifname().to_string(),
                    ];
                    ports.extend(interfaces.attachment_host_ifnames().iter().cloned());
                    ports.sort();
                    ports.dedup();
                    for interface in ports.into_iter().filter(|name| interface_exists(name)) {
                        desired.push(DesiredAttachment {
                            attach_point,
                            name: spec.name.clone(),
                            interface,
                        });
                    }
                }
            }
        }

        desired
    }

    fn desired_target(desired: &DesiredAttachment) -> AttachTarget<'_> {
        match desired.attach_point {
            BpfAttachPoint::VxlanXdp | BpfAttachPoint::BridgeXdp => AttachTarget::Xdp {
                interface: desired.interface.as_str(),
            },
            BpfAttachPoint::BridgeTcIngress => AttachTarget::Tc {
                interface: desired.interface.as_str(),
                attach_type: TcAttachType::Ingress,
            },
            BpfAttachPoint::BridgeTcEgress => AttachTarget::Tc {
                interface: desired.interface.as_str(),
                attach_type: TcAttachType::Egress,
            },
        }
    }

    fn own_target(target: AttachTarget<'_>) -> OwnedAttachTarget {
        match target {
            AttachTarget::Xdp { interface } => OwnedAttachTarget::Xdp {
                interface: interface.to_string(),
            },
            AttachTarget::Tc {
                interface,
                attach_type,
            } => OwnedAttachTarget::Tc {
                interface: interface.to_string(),
                attach_type,
            },
        }
    }

    fn target_interface(target: AttachTarget<'_>) -> &str {
        match target {
            AttachTarget::Xdp { interface } => interface,
            AttachTarget::Tc { interface, .. } => interface,
        }
    }

    /// # Description:
    ///
    /// Convert one configured map capacity into the `u32` value Aya expects before the loader
    /// creates the pinned kernel map.
    fn checked_map_capacity(name: &str, value: usize) -> Result<u32> {
        u32::try_from(value)
            .with_context(|| format!("configured {name} exceeds the kernel map size limit"))
    }

    fn interface_exists(name: &str) -> bool {
        match CString::new(name) {
            Ok(cstr) => unsafe { if_nametoindex(cstr.as_ptr()) != 0 },
            Err(_) => false,
        }
    }

    fn attach_priority(point: BpfAttachPoint) -> u8 {
        match point {
            BpfAttachPoint::VxlanXdp => 0,
            BpfAttachPoint::BridgeXdp => 1,
            _ => 2,
        }
    }

    fn detach_priority(point: BpfAttachPoint) -> u8 {
        match point {
            BpfAttachPoint::BridgeXdp => 0,
            BpfAttachPoint::VxlanXdp => 1,
            _ => 2,
        }
    }

    /// Return the pinned load-balancer map names used by one single-stack bridge dataplane family.
    fn lb_map_names(family: OverlayIpFamily) -> &'static [&'static str] {
        match family {
            OverlayIpFamily::Ipv4 => &[
                "LB_VIPS",
                "LB_BACKENDS",
                "LB_FWD",
                "LB_REV",
                "LB_RUNTIME_V4",
            ],
            OverlayIpFamily::Ipv6 => &[
                "LB_VIPS_V6",
                "LB_BACKENDS_V6",
                "LB_FWD_V6",
                "LB_REV_V6",
                "LB_RUNTIME_V6",
            ],
        }
    }

    /// # Description:
    ///
    /// Return the pinned flow-map names for one overlay address family.
    fn lb_flow_map_names(family: OverlayIpFamily) -> &'static [&'static str] {
        match family {
            OverlayIpFamily::Ipv4 => &["LB_FWD", "LB_REV"],
            OverlayIpFamily::Ipv6 => &["LB_FWD_V6", "LB_REV_V6"],
        }
    }

    /// Return the pinned runtime-config map name for one overlay load-balancer family.
    fn lb_runtime_map_name(family: OverlayIpFamily) -> &'static str {
        match family {
            OverlayIpFamily::Ipv4 => "LB_RUNTIME_V4",
            OverlayIpFamily::Ipv6 => "LB_RUNTIME_V6",
        }
    }

    /// Return the single-stack LB family required by one network's bridge TC programs.
    fn load_balancer_map_family(network: &NetworkSpecValue) -> Result<Option<OverlayIpFamily>> {
        let requires_lb = network.bpf_programs.iter().any(|spec| {
            matches!(
                spec.attach_point(),
                BpfAttachPoint::BridgeTcIngress | BpfAttachPoint::BridgeTcEgress
            )
        });
        if !requires_lb {
            return Ok(None);
        }

        Ok(Some(parse_overlay_cidr(&network.subnet_cidr)?.family))
    }

    /// Return the LB family required by one specific program being attached, if any.
    fn program_load_balancer_family(
        spec: &BpfProgramSpec,
        network_lb_family: Option<OverlayIpFamily>,
    ) -> Option<OverlayIpFamily> {
        if matches!(
            spec.attach_point(),
            BpfAttachPoint::BridgeTcIngress | BpfAttachPoint::BridgeTcEgress
        ) {
            return network_lb_family;
        }
        None
    }

    /// Remap the built-in bridge TC program names to their family-specific object artifacts.
    fn bridge_tc_artifact_name(
        spec: &BpfProgramSpec,
        family: OverlayIpFamily,
    ) -> Option<&'static str> {
        match (spec.attach_point(), spec.name.as_str(), family) {
            (BpfAttachPoint::BridgeTcIngress, "bridge_tc_ingress", OverlayIpFamily::Ipv4) => {
                Some("bridge_tc_ingress_v4")
            }
            (BpfAttachPoint::BridgeTcIngress, "bridge_tc_ingress", OverlayIpFamily::Ipv6) => {
                Some("bridge_tc_ingress_v6")
            }
            (BpfAttachPoint::BridgeTcEgress, "bridge_tc_egress", OverlayIpFamily::Ipv4) => {
                Some("bridge_tc_egress_v4")
            }
            (BpfAttachPoint::BridgeTcEgress, "bridge_tc_egress", OverlayIpFamily::Ipv6) => {
                Some("bridge_tc_egress_v6")
            }
            _ => None,
        }
    }

    /// Check whether the pinned load balancer maps for the requested family are reachable.
    fn lb_maps_present(base: &Path, family: OverlayIpFamily) -> bool {
        lb_map_names(family)
            .iter()
            .all(|name| map_is_pinned(base, name))
    }

    /// Attempt to locate a pinned map across the expected bpffs locations Aya may use.
    fn map_is_pinned(base: &Path, name: &str) -> bool {
        let candidates = [
            base.join(name),
            base.join("tc").join("globals").join(name),
            Path::new("/sys/fs/bpf/tc/globals").join(name),
        ];

        candidates
            .into_iter()
            .any(|candidate| MapData::from_pin(&candidate).is_ok())
    }

    fn force_detach_target(target: AttachTarget<'_>) -> Result<()> {
        match target {
            AttachTarget::Xdp { interface } => detach_xdp(interface),
            AttachTarget::Tc {
                interface,
                attach_type,
            } => detach_tc_filters(interface, attach_type, None),
        }
    }

    /// Detach stale `tc` filters so repeated daemon restarts do not stack multiple classifiers on
    /// the same hook (which can produce surprising behavior and wastes CPU cycles).
    fn detach_tc_filters(
        interface: &str,
        attach_type: TcAttachType,
        program_name: Option<&str>,
    ) -> Result<()> {
        let mut candidates = Vec::new();
        if let Some(name) = program_name {
            candidates.push(name.to_string());
            let truncated: String = name.chars().take(15).collect();
            if truncated != name {
                candidates.push(truncated);
            }
        } else {
            candidates.push("bridge_tc_ingress".chars().take(15).collect());
            candidates.push("bridge_tc_egress".chars().take(15).collect());
        }

        for name in candidates {
            match qdisc_detach_program(interface, attach_type, &name) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(())
    }

    /// Remove legacy host-peer tc attachments left behind from the earlier host-access-only bridge
    /// dataplane target selection.
    fn clear_stale_bridge_tc_host_peer_targets(interfaces: &NetworkInterfaceContext) {
        if !interface_exists(interfaces.host_peer_ifname()) {
            return;
        }
        for attach_type in [TcAttachType::Ingress, TcAttachType::Egress] {
            if let Err(err) = detach_tc_filters(interfaces.host_peer_ifname(), attach_type, None) {
                warn!(
                    target: "network",
                    network = %interfaces.network_id(),
                    interface = interfaces.host_peer_ifname(),
                    attach = ?attach_type,
                    "failed to clear stale host-peer bridge tc attachment: {err:#}"
                );
            }
        }
    }

    /// Remove stale bridge-master tc attachments left behind by the earlier single-interface
    /// bridge target selection.
    fn clear_stale_bridge_tc_bridge_master_target(interfaces: &NetworkInterfaceContext) {
        if !interface_exists(interfaces.bridge_ifname()) {
            return;
        }
        for attach_type in [TcAttachType::Ingress, TcAttachType::Egress] {
            if let Err(err) = detach_tc_filters(interfaces.bridge_ifname(), attach_type, None) {
                warn!(
                    target: "network",
                    network = %interfaces.network_id(),
                    interface = interfaces.bridge_ifname(),
                    attach = ?attach_type,
                    "failed to clear stale bridge-master tc attachment: {err:#}"
                );
            }
        }
    }

    /// Remove pinned LB maps from the family that is not required by the current bridge artifact.
    fn prune_unused_lb_maps(base: &Path, required_family: OverlayIpFamily) -> Result<()> {
        let stale_family = match required_family {
            OverlayIpFamily::Ipv4 => OverlayIpFamily::Ipv6,
            OverlayIpFamily::Ipv6 => OverlayIpFamily::Ipv4,
        };

        for name in lb_map_names(stale_family) {
            let path = base.join(name);
            if path.exists() {
                fs::remove_file(&path)
                    .with_context(|| format!("remove stale lb map {}", path.display()))?;
            }
        }

        Ok(())
    }

    /// Ensure only the required LB map family is pinned so unused bridge families do not reserve
    /// large backend maps for every single-stack network.
    fn ensure_lb_maps_pinned(
        bpf: &mut Ebpf,
        base: &Path,
        required_family: Option<OverlayIpFamily>,
    ) -> Result<()> {
        let Some(required_family) = required_family else {
            return Ok(());
        };
        if let Err(err) = fs::create_dir_all(base) {
            warn!(
                target: "network",
                path = %base.display(),
                "failed to prepare lb map pin directory: {err:#}"
            );
            return Ok(());
        }

        if let Err(err) = prune_unused_lb_maps(base, required_family) {
            warn!(
                target: "network",
                path = %base.display(),
                "failed to prune stale load balancer maps (continuing): {err:#}"
            );
        }

        for name in lb_map_names(required_family) {
            let Some(map) = bpf.map_mut(name) else {
                continue;
            };
            let path = base.join(name);
            if let Err(err) = map.pin(&path)
                && !is_already_pinned(&err)
            {
                warn!(
                    target: "network",
                    map = %name,
                    path = %path.display(),
                    "failed to pin lb map (continuing with in-kernel handle): {err:#}"
                );
            }
        }

        Ok(())
    }

    /// # Description:
    ///
    /// Apply the configured overlay flow capacity before Aya creates the pinned forward and
    /// reverse conntrack maps for one bridge tc object.
    fn configure_overlay_flow_map_capacities(
        loader: &mut EbpfLoader<'_>,
        family: Option<OverlayIpFamily>,
    ) -> Result<()> {
        let Some(family) = family else {
            return Ok(());
        };

        let flow_capacity = checked_map_capacity(
            "network.bpf.overlay_flow_capacity",
            config::bpf_overlay_flow_capacity(),
        )?;
        for map_name in lb_flow_map_names(family) {
            loader.set_max_entries(map_name, flow_capacity);
        }
        Ok(())
    }

    /// Compute the per-family overlay runtime config that tc ingress uses for SYN MSS clamping.
    ///
    /// The tc programs do not derive MTU math locally. Userspace converts the effective network
    /// MTU into one TCP MSS ceiling and writes it into a tiny pinned config map keyed by zero.
    fn overlay_runtime_config(
        network: &NetworkSpecValue,
        family: OverlayIpFamily,
    ) -> Result<BridgeRuntimeConfig> {
        let tcp_mss = match family {
            OverlayIpFamily::Ipv4 => ipv4_tcp_mss_from_mtu(network.mtu),
            OverlayIpFamily::Ipv6 => ipv6_tcp_mss_from_mtu(network.mtu),
        }
        .ok_or_else(|| {
            anyhow!(
                "overlay MTU {} is too small for {family:?} TCP",
                network.mtu
            )
        })?;

        Ok(BridgeRuntimeConfig {
            tcp_mss,
            _pad: [0u8; 6],
        })
    }

    /// Convert one IPv4 MTU into the largest TCP MSS that still fits on that link.
    fn ipv4_tcp_mss_from_mtu(mtu: u32) -> Option<u16> {
        mtu.checked_sub(IPV4_TCP_HEADER_BYTES)
            .and_then(|mss| u16::try_from(mss).ok())
    }

    /// Convert one IPv6 MTU into the largest TCP MSS that still fits on that link.
    fn ipv6_tcp_mss_from_mtu(mtu: u32) -> Option<u16> {
        mtu.checked_sub(IPV6_TCP_HEADER_BYTES)
            .and_then(|mss| u16::try_from(mss).ok())
    }

    /// Refresh the pinned overlay runtime-config map for one network after program load or MTU change.
    ///
    /// Keeping this separate from attachment logic lets Mantissa update the per-network MSS ceiling
    /// even when the attached program set itself did not change.
    fn program_overlay_runtime_map(
        network: &NetworkSpecValue,
        base: &Path,
        family: OverlayIpFamily,
    ) -> Result<()> {
        let map = open_pinned_map(base, lb_runtime_map_name(family))
            .with_context(|| format!("open {} runtime map", family_label(family)))?;
        let key = 0u32;
        let config = overlay_runtime_config(network, family)?;
        update_map_elem(map.fd().as_fd().as_raw_fd(), &key, &config)
            .with_context(|| format!("program {} runtime map", family_label(family)))?;
        Ok(())
    }

    /// Render one overlay address-family label for logs and operator-facing errors.
    fn family_label(family: OverlayIpFamily) -> &'static str {
        match family {
            OverlayIpFamily::Ipv4 => "IPv4",
            OverlayIpFamily::Ipv6 => "IPv6",
        }
    }

    /// Open one pinned overlay BPF map using the same fallback search order as the loader.
    fn open_pinned_map(base: &Path, name: &str) -> Result<MapData> {
        let candidates = [
            base.join(name),
            base.join("tc").join("globals").join(name),
            Path::new("/sys/fs/bpf/tc/globals").join(name),
        ];

        for candidate in candidates {
            if let Ok(map) = MapData::from_pin(&candidate) {
                return Ok(map);
            }
        }

        Err(anyhow!("map {name} not found in expected pin locations"))
    }

    /// Update one pinned BPF map entry through `bpf(BPF_MAP_UPDATE_ELEM)`.
    ///
    /// Aya sizes and pins the map, but the runtime config value is refreshed directly here so it
    /// can be rewritten without reconstructing the loaded tc object.
    fn update_map_elem<K, V>(fd: i32, key: &K, value: &V) -> Result<()> {
        const BPF_MAP_UPDATE_ELEM: libc::c_uint = 2;

        #[repr(C)]
        struct BpfAttrUpsert {
            map_fd: u32,
            _pad: u32,
            key: u64,
            value: u64,
            flags: u64,
        }

        let mut attr = BpfAttrUpsert {
            map_fd: fd as u32,
            _pad: 0,
            key: key as *const _ as u64,
            value: value as *const _ as u64,
            flags: 0,
        };

        let ret = unsafe {
            libc::syscall(
                libc::SYS_bpf,
                BPF_MAP_UPDATE_ELEM,
                &mut attr as *mut _,
                mem::size_of::<BpfAttrUpsert>(),
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    fn is_already_pinned(err: &PinError) -> bool {
        matches!(
            err,
            PinError::SyscallError(sys)
                if matches!(sys.io_error.raw_os_error(), Some(code) if code == libc::EEXIST)
        )
    }

    fn detach_xdp(interface: &str) -> Result<()> {
        let Some(if_index) = interface_index(interface)? else {
            debug!(
                target: "network",
                interface,
                "skipping xdp detach; interface missing"
            );
            return Ok(());
        };

        match unsafe { netlink::detach_xdp(if_index) } {
            Ok(()) => Ok(()),
            Err(err) => {
                warn!(
                    target: "network",
                    interface,
                    "failed to detach XDP program via netlink: {err}, trying iproute2 fallback"
                );
                detach_xdp_with_ip(interface)
            }
        }
    }

    fn interface_index(name: &str) -> Result<Option<i32>> {
        let cstr = CString::new(name).context("interface name contains null byte")?;
        let index = unsafe { if_nametoindex(cstr.as_ptr()) };
        if index == 0 {
            Ok(None)
        } else {
            Ok(Some(index as i32))
        }
    }

    fn detach_xdp_with_ip(interface: &str) -> Result<()> {
        let output = Command::new("ip")
            .args(["link", "set", "dev", interface, "xdp", "off"])
            .output()
            .with_context(|| format!("run ip link set xdp off for {interface}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!(
                "ip link set dev {interface} xdp off failed: {}",
                stderr.trim()
            ));
        }
        Ok(())
    }

    /// Ensure bpffs is mounted at /sys/fs/bpf so we have a stable pin location.
    fn ensure_bpffs() -> Result<()> {
        let mountpoint = Path::new("/sys/fs/bpf");
        if !mountpoint.exists() {
            fs::create_dir_all(mountpoint).context("create /sys/fs/bpf")?;
        }
        if is_bpffs(mountpoint) {
            return Ok(());
        }

        mount::<Path, Path, str, str>(
            None::<&Path>,
            mountpoint,
            Some("bpf"),
            MsFlags::empty(),
            None::<&str>,
        )
        .context("mount bpffs")?;
        Ok(())
    }

    /// Lightweight check to see if a path is a bpffs mount.
    fn is_bpffs(path: &Path) -> bool {
        matches!(statfs(path), Ok(stat) if stat.filesystem_type() == BPF_FS_MAGIC)
    }

    fn is_bpf_link_conflict(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            if let Some(sys) = cause.downcast_ref::<aya::sys::SyscallError>() {
                return is_stale_bpf_attach_errno(sys.call, sys.io_error.raw_os_error());
            }
            if let Some(aya::programs::ProgramError::SyscallError(sys)) =
                cause.downcast_ref::<aya::programs::ProgramError>()
            {
                return is_stale_bpf_attach_errno(sys.call, sys.io_error.raw_os_error());
            }
            false
        })
    }

    /// Classify errno values that indicate one stale eBPF attachment is still occupying the
    /// target hook and should be force-detached before retrying.
    fn is_stale_bpf_attach_errno(call: &str, errno: Option<i32>) -> bool {
        match errno {
            Some(code) if code == libc::EEXIST => call == "bpf_link_create",
            Some(code) if code == libc::EBUSY => true,
            _ => false,
        }
    }

    fn load_with_retry(
        loader: &dyn ProgramLoader,
        spec: &BpfProgramSpec,
        target: AttachTarget<'_>,
        artifact: &Path,
        map_pin_path: &Path,
        lb_family: Option<OverlayIpFamily>,
    ) -> Result<ProgramHandle> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match loader
                .load_and_attach(spec, target, artifact, map_pin_path, lb_family)
                .with_context(|| {
                    format!(
                        "load and attach program '{}' ({})",
                        spec,
                        artifact.display()
                    )
                }) {
                Ok(handle) => return Ok(handle),
                Err(err) => {
                    if attempt == 1
                        && matches!(target, AttachTarget::Xdp { .. })
                        && is_bpf_link_conflict(&err)
                    {
                        if let Err(detach_err) = force_detach_target(target) {
                            warn!(
                                target: "network",
                                program = %spec,
                                interface = %target_interface(target),
                                "failed to clear conflicting XDP link before retry: {detach_err:#}"
                            );
                            return Err(err);
                        }
                        warn!(
                            target: "network",
                            program = %spec,
                            interface = %target_interface(target),
                            "retrying XDP attachment after clearing stale link"
                        );
                        continue;
                    }
                    return Err(err);
                }
            }
        }
    }

    mod netlink {
        use libc::{
            AF_NETLINK, AF_UNSPEC, IFLA_XDP, NETLINK_CAP_ACK, NETLINK_EXT_ACK, NETLINK_ROUTE,
            NLA_F_NESTED, NLM_F_ACK, NLM_F_REQUEST, NLMSG_ERROR, RTM_SETLINK, SOCK_RAW,
            SOL_NETLINK, nlattr, nlmsgerr, nlmsghdr, recv, sa_family_t, send, setsockopt,
        };
        use std::io;
        use std::mem;
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        use std::ptr;
        use std::slice;

        const IFLA_XDP_FD_ATTR: u16 = 1;

        #[repr(C)]
        struct DetachRequest {
            header: nlmsghdr,
            ifinfo: IfInfoMsg,
            xdp_attr: nlattr,
            fd_attr: nlattr,
            fd_value: i32,
        }

        #[repr(C)]
        #[derive(Clone, Copy)]
        struct SockaddrNl {
            nl_family: sa_family_t,
            nl_pad: libc::c_ushort,
            nl_pid: u32,
            nl_groups: u32,
        }

        #[repr(C)]
        #[derive(Clone, Copy)]
        struct IfInfoMsg {
            ifi_family: u8,
            __ifi_pad: u8,
            ifi_type: u16,
            ifi_index: i32,
            ifi_flags: u32,
            ifi_change: u32,
        }

        pub(super) unsafe fn detach_xdp(if_index: i32) -> io::Result<()> {
            let sock = unsafe { NetlinkSocket::open()? };

            let mut req: DetachRequest = unsafe { mem::zeroed() };
            req.header.nlmsg_len = mem::size_of::<DetachRequest>() as u32;
            req.header.nlmsg_type = RTM_SETLINK;
            req.header.nlmsg_flags = (NLM_F_REQUEST | NLM_F_ACK) as u16;
            req.header.nlmsg_seq = 1;
            req.ifinfo.ifi_family = AF_UNSPEC as u8;
            req.ifinfo.ifi_index = if_index;
            #[allow(clippy::unnecessary_cast)]
            {
                req.xdp_attr.nla_type = (NLA_F_NESTED as u16) | (IFLA_XDP as u16);
            }
            req.xdp_attr.nla_len = (mem::size_of::<nlattr>() * 2 + mem::size_of::<i32>()) as u16;
            req.fd_attr.nla_type = IFLA_XDP_FD_ATTR;
            req.fd_attr.nla_len = (mem::size_of::<nlattr>() + mem::size_of::<i32>()) as u16;
            req.fd_value = -1;

            sock.send(bytes_of(&req))?;
            sock.recv_ack()
        }

        struct NetlinkSocket {
            fd: OwnedFd,
        }

        impl NetlinkSocket {
            unsafe fn open() -> io::Result<Self> {
                let fd = unsafe { libc::socket(AF_NETLINK, SOCK_RAW, NETLINK_ROUTE) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                let fd = unsafe { OwnedFd::from_raw_fd(fd) };

                let enable = 1i32;
                if unsafe {
                    setsockopt(
                        fd.as_raw_fd(),
                        SOL_NETLINK,
                        NETLINK_EXT_ACK,
                        &enable as *const _ as *const _,
                        mem::size_of::<i32>() as u32,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }

                if unsafe {
                    setsockopt(
                        fd.as_raw_fd(),
                        SOL_NETLINK,
                        NETLINK_CAP_ACK,
                        &enable as *const _ as *const _,
                        mem::size_of::<i32>() as u32,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }

                let local = SockaddrNl {
                    nl_family: AF_NETLINK as sa_family_t,
                    nl_pad: 0,
                    nl_pid: 0,
                    nl_groups: 0,
                };

                if unsafe {
                    libc::bind(
                        fd.as_raw_fd(),
                        &local as *const _ as *const _,
                        mem::size_of::<SockaddrNl>() as u32,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }

                let kernel = SockaddrNl {
                    nl_family: AF_NETLINK as sa_family_t,
                    nl_pad: 0,
                    nl_pid: 0,
                    nl_groups: 0,
                };

                if unsafe {
                    libc::connect(
                        fd.as_raw_fd(),
                        &kernel as *const _ as *const _,
                        mem::size_of::<SockaddrNl>() as u32,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }

                Ok(Self { fd })
            }

            fn send(&self, msg: &[u8]) -> io::Result<()> {
                if unsafe { send(self.fd.as_raw_fd(), msg.as_ptr() as *const _, msg.len(), 0) } < 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            }

            fn recv_ack(&self) -> io::Result<()> {
                let mut buf = [0u8; 256];
                let len = unsafe {
                    recv(
                        self.fd.as_raw_fd(),
                        buf.as_mut_ptr() as *mut _,
                        buf.len(),
                        0,
                    )
                };
                if len < 0 {
                    return Err(io::Error::last_os_error());
                }
                if (len as usize) < mem::size_of::<nlmsghdr>() + mem::size_of::<nlmsgerr>() {
                    return Err(io::Error::other("short netlink reply"));
                }

                let header = unsafe { ptr::read_unaligned(buf.as_ptr() as *const nlmsghdr) };
                if header.nlmsg_type as i32 != NLMSG_ERROR {
                    return Ok(());
                }

                let err = unsafe {
                    ptr::read_unaligned(
                        buf[mem::size_of::<nlmsghdr>()..].as_ptr() as *const nlmsgerr
                    )
                };
                if err.error == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(-err.error))
                }
            }
        }

        fn bytes_of<T>(val: &T) -> &[u8] {
            unsafe { slice::from_raw_parts((val as *const T).cast::<u8>(), mem::size_of::<T>()) }
        }
    }

    #[derive(Default)]
    struct NoopProgramLoader;

    impl ProgramLoader for NoopProgramLoader {
        fn load_and_attach(
            &self,
            _spec: &BpfProgramSpec,
            _target: AttachTarget<'_>,
            _artifact: &Path,
            _map_pin_path: &Path,
            _lb_family: Option<OverlayIpFamily>,
        ) -> Result<ProgramHandle> {
            Ok(Box::new(NoopHandle))
        }
    }

    struct NoopHandle;

    impl Detachable for NoopHandle {
        fn detach(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct AyaProgramLoader;

    impl ProgramLoader for AyaProgramLoader {
        fn load_and_attach(
            &self,
            spec: &BpfProgramSpec,
            target: AttachTarget<'_>,
            artifact: &Path,
            map_pin_path: &Path,
            lb_family: Option<OverlayIpFamily>,
        ) -> Result<ProgramHandle> {
            let mut loader = EbpfLoader::new();
            loader.map_pin_path(map_pin_path);
            configure_overlay_flow_map_capacities(&mut loader, lb_family)
                .context("configure overlay bpf map capacities")?;
            let mut bpf = loader
                .load_file(artifact)
                .with_context(|| format!("load bpf object {}", artifact.display()))?;
            ensure_lb_maps_pinned(&mut bpf, map_pin_path, lb_family)
                .context("pin load balancer maps")?;

            let program_name = spec.name.clone();

            match target {
                AttachTarget::Xdp { interface } => {
                    let program = bpf
                        .program_mut(program_name.as_str())
                        .with_context(|| format!("find xdp program '{}'", program_name))?;
                    let xdp: &mut Xdp = program
                        .try_into()
                        .with_context(|| format!("program '{}' is not XDP", program_name))?;
                    xdp.load()
                        .with_context(|| format!("load xdp program '{}'", program_name))?;
                    let link_id =
                        xdp.attach(interface, XdpFlags::default())
                            .with_context(|| {
                                format!("attach xdp program '{}' to {}", program_name, interface)
                            })?;
                    Ok(Box::new(XdpHandle {
                        bpf,
                        program_name,
                        link_id: Some(link_id),
                    }))
                }
                AttachTarget::Tc {
                    interface,
                    attach_type,
                } => {
                    match qdisc_add_clsact(interface) {
                        Ok(()) => {}
                        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(err) => {
                            return Err(anyhow!(
                                "ensure clsact qdisc on interface {}: {err}",
                                interface
                            ));
                        }
                    }

                    if let Err(err) = detach_tc_filters(interface, attach_type, Some(&program_name))
                    {
                        warn!(
                            target: "network",
                            program = %program_name,
                            interface,
                            attach = ?attach_type,
                            "failed to detach stale tc filter before attaching: {err:#}"
                        );
                    }

                    let program = bpf.program_mut(program_name.as_str()).with_context(|| {
                        format!("find tc classifier program '{}'", program_name)
                    })?;
                    let classifier: &mut SchedClassifier =
                        program.try_into().with_context(|| {
                            format!("program '{}' is not sched classifier", program_name)
                        })?;
                    classifier.load().with_context(|| {
                        format!("load tc classifier program '{}'", program_name)
                    })?;

                    let link_id = classifier.attach(interface, attach_type).with_context(|| {
                        format!(
                            "attach tc program '{}' to {} ({:?})",
                            program_name, interface, attach_type
                        )
                    })?;

                    Ok(Box::new(TcHandle {
                        bpf,
                        program_name,
                        link_id: Some(link_id),
                    }))
                }
            }
        }
    }

    struct XdpHandle {
        bpf: Ebpf,
        program_name: String,
        link_id: Option<XdpLinkId>,
    }

    impl Detachable for XdpHandle {
        fn detach(&mut self) -> Result<()> {
            if let Some(link_id) = self.link_id.take() {
                let program = self
                    .bpf
                    .program_mut(self.program_name.as_str())
                    .with_context(|| format!("lookup XDP program '{}'", self.program_name))?;
                let xdp: &mut Xdp = program
                    .try_into()
                    .with_context(|| format!("program '{}' is not XDP", self.program_name))?;
                xdp.detach(link_id)
                    .with_context(|| format!("detach XDP program '{}'", self.program_name))?;
            }
            Ok(())
        }
    }

    struct TcHandle {
        bpf: Ebpf,
        program_name: String,
        link_id: Option<SchedClassifierLinkId>,
    }

    impl Detachable for TcHandle {
        fn detach(&mut self) -> Result<()> {
            if let Some(link_id) = self.link_id.take() {
                let program = self
                    .bpf
                    .program_mut(self.program_name.as_str())
                    .with_context(|| format!("lookup TC program '{}'", self.program_name))?;
                let classifier: &mut SchedClassifier = program.try_into().with_context(|| {
                    format!("program '{}' is not sched classifier", self.program_name)
                })?;
                classifier
                    .detach(link_id)
                    .with_context(|| format!("detach TC program '{}'", self.program_name))?;
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::config::{
            Config, ConfigSource, global_config, global_config_source, load_config_with_source,
            set_global_config_with_source,
        };
        use crate::network::bpf::NetworkBpfManager;
        use crate::network::types::{NetworkDriver, NetworkSpecDraft, NetworkSpecValue};
        use parking_lot::{Mutex, MutexGuard};
        use std::ffi::OsString;
        use std::fs;
        use std::path::PathBuf;
        use std::sync::OnceLock;
        use tempfile::TempDir;
        use uuid::Uuid;

        /// Restores one process-global environment override after a unit test-scoped mutation.
        struct EnvOverrideGuard {
            key: &'static str,
            previous: Option<OsString>,
        }

        impl EnvOverrideGuard {
            /// Apply one temporary environment override and remember the prior value.
            fn set(key: &'static str, value: impl Into<OsString>) -> Self {
                let previous = std::env::var_os(key);
                unsafe {
                    std::env::set_var(key, value.into());
                }
                Self { key, previous }
            }
        }

        impl Drop for EnvOverrideGuard {
            /// Restore the previous environment value after the scoped override ends.
            fn drop(&mut self) {
                match &self.previous {
                    Some(value) => unsafe {
                        std::env::set_var(self.key, value);
                    },
                    None => unsafe {
                        std::env::remove_var(self.key);
                    },
                }
            }
        }

        /// Restores the global Mantissa config after a unit test-scoped override.
        struct ConfigOverrideGuard {
            previous: Config,
            source: ConfigSource,
            _lock: MutexGuard<'static, ()>,
        }

        /// Return the global mutex used to serialize config and env overrides in BPF unit tests.
        fn config_override_lock() -> &'static Mutex<()> {
            static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
            LOCK.get_or_init(|| Mutex::new(()))
        }

        impl ConfigOverrideGuard {
            /// Replace the global config for one test and restore it afterward.
            fn with_mutator(mutator: impl FnOnce(&mut Config)) -> Self {
                let lock = config_override_lock().lock();
                let previous = global_config();
                let source = global_config_source();

                let mut config = previous.clone();
                mutator(&mut config);

                let mut override_source = source.clone();
                override_source.env_overrides = true;
                set_global_config_with_source(config, override_source);

                Self {
                    previous,
                    source,
                    _lock: lock,
                }
            }
        }

        impl Drop for ConfigOverrideGuard {
            /// Restore the previous global config snapshot after the unit test completes.
            fn drop(&mut self) {
                set_global_config_with_source(self.previous.clone(), self.source.clone());
            }
        }

        #[test]
        fn resolves_artifact_from_config_directory() -> Result<()> {
            let dir = TempDir::new().context("create temp dir")?;
            let artifact_path = dir.path().join("resolver-example.bpf.o");
            fs::write(&artifact_path, b"test").context("write artifact stub")?;

            let mut config = crate::config::global_config();
            config.network.bpf.artifact_dir = Some(dir.path().to_string_lossy().to_string());
            let resolver = ArtifactResolver::new_with_config(&config);
            let spec = BpfProgramSpec::new("resolver-example");
            let network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "resolver-example".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 42,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec.clone()],
            });
            let resolved = resolver.resolve(&spec, &network)?;
            assert_eq!(resolved, artifact_path);

            Ok(())
        }

        #[test]
        fn resolves_artifact_from_env_directory() -> Result<()> {
            let _lock = config_override_lock().lock();
            let dir = TempDir::new().context("create temp dir")?;
            let artifact_path = dir.path().join("resolver-env-example.bpf.o");
            fs::write(&artifact_path, b"test").context("write artifact stub")?;
            let _env_guard = EnvOverrideGuard::set("MANTISSA_BPF_DIR", dir.path().as_os_str());

            let (config, _) = load_config_with_source(None)?;
            let resolver = ArtifactResolver::new_with_config(&config);
            let spec = BpfProgramSpec::new("resolver-env-example");
            let network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "resolver-env-example".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 42,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec.clone()],
            });
            let resolved = resolver.resolve(&spec, &network)?;
            assert_eq!(resolved, artifact_path);

            Ok(())
        }

        #[test]
        fn resolves_family_specific_bridge_artifact_from_network_subnet() -> Result<()> {
            let dir = TempDir::new().context("create temp dir")?;
            let v4_artifact = dir.path().join("bridge_tc_ingress_v4.bpf.o");
            let v6_artifact = dir.path().join("bridge_tc_ingress_v6.bpf.o");
            fs::write(&v4_artifact, b"v4").context("write IPv4 bridge artifact stub")?;
            fs::write(&v6_artifact, b"v6").context("write IPv6 bridge artifact stub")?;

            let mut config = crate::config::global_config();
            config.network.bpf.artifact_dir = Some(dir.path().to_string_lossy().to_string());
            let resolver = ArtifactResolver::new_with_config(&config);
            let spec = BpfProgramSpec::with_attach_point(
                "bridge_tc_ingress",
                BpfAttachPoint::BridgeTcIngress,
            );

            let ipv4_network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "resolver-bridge-v4".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 42,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec.clone()],
            });
            let ipv6_network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "resolver-bridge-v6".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "fd42::/64".to_string(),
                vni: 43,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec.clone()],
            });

            assert_eq!(resolver.resolve(&spec, &ipv4_network)?, v4_artifact);
            assert_eq!(resolver.resolve(&spec, &ipv6_network)?, v6_artifact);

            Ok(())
        }

        #[test]
        fn validate_programs_rejects_duplicate_attach() {
            let manager = PlatformBpfManager {
                resolver: ArtifactResolver::new(),
                loader: Arc::new(NoopProgramLoader),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            };
            let ctx = NetworkInterfaceContext::new(Uuid::new_v4(), "lo", "lo");
            let programs = vec![
                BpfProgramSpec::with_attach_point("dup-a", BpfAttachPoint::VxlanXdp),
                BpfProgramSpec::with_attach_point("dup-b", BpfAttachPoint::VxlanXdp),
            ];

            let error = manager.validate_programs(&programs, &ctx).unwrap_err();
            assert!(
                error.to_string().contains("multiple programs"),
                "expected duplicate attach error, got {error:#}"
            );
        }

        #[test]
        fn validate_programs_accepts_distinct_targets() -> Result<()> {
            let manager = PlatformBpfManager {
                resolver: ArtifactResolver::new(),
                loader: Arc::new(NoopProgramLoader),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            };
            let ctx = NetworkInterfaceContext::new(Uuid::new_v4(), "lo", "lo");
            let programs = vec![
                BpfProgramSpec::with_attach_point("ok-vxlan", BpfAttachPoint::VxlanXdp),
                BpfProgramSpec::with_attach_point("ok-bridge", BpfAttachPoint::BridgeXdp),
            ];

            manager.validate_programs(&programs, &ctx)?;
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn requires_reload_when_no_programs_are_loaded() -> Result<()> {
            let _guard = ConfigOverrideGuard::with_mutator(|config| {
                config.network.bpf.attach = true;
            });
            let manager = PlatformBpfManager {
                resolver: ArtifactResolver::new(),
                loader: Arc::new(NoopProgramLoader),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            };
            let network_id = Uuid::new_v4();
            let ctx = NetworkInterfaceContext::new(network_id, "br-test", "vxlan-test");
            let spec = BpfProgramSpec::with_attach_point("vxlan_xdp", BpfAttachPoint::VxlanXdp);
            let network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "reload-required".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 42,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec],
            });

            assert!(
                manager.requires_reload(&network, &ctx).await?,
                "an unloaded dataplane should be treated as requiring a local reload"
            );
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn requires_reload_is_false_when_loaded_programs_match() -> Result<()> {
            let _guard = ConfigOverrideGuard::with_mutator(|config| {
                config.network.bpf.attach = true;
            });
            let manager = PlatformBpfManager {
                resolver: ArtifactResolver::new(),
                loader: Arc::new(NoopProgramLoader),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            };
            let network_id = Uuid::new_v4();
            let ctx = NetworkInterfaceContext::new(network_id, "br-test", "vxlan-test");
            let spec = BpfProgramSpec::with_attach_point("vxlan_xdp", BpfAttachPoint::VxlanXdp);
            let network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "reload-not-required".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 43,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec.clone()],
            });

            let mut loaded_network = LoadedNetwork::new(None);
            loaded_network.push(LoadedProgram {
                spec,
                target: OwnedAttachTarget::Xdp {
                    interface: "vxlan-test".to_string(),
                },
                _artifact: PathBuf::new(),
                handle: Box::new(NoopHandle),
            });
            manager
                .loaded
                .lock()
                .await
                .insert(network_id, loaded_network);

            assert!(
                !manager.requires_reload(&network, &ctx).await?,
                "matching loaded programs should not trigger a destructive reload"
            );
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn requires_reload_is_false_when_bpf_attachment_disabled() -> Result<()> {
            let _guard = ConfigOverrideGuard::with_mutator(|config| {
                config.network.bpf.attach = false;
            });
            let manager = PlatformBpfManager {
                resolver: ArtifactResolver::new(),
                loader: Arc::new(NoopProgramLoader),
                loaded: Arc::new(AsyncMutex::new(HashMap::new())),
            };
            let network_id = Uuid::new_v4();
            let ctx = NetworkInterfaceContext::new(network_id, "br-test", "vxlan-test");
            let spec = BpfProgramSpec::with_attach_point("vxlan_xdp", BpfAttachPoint::VxlanXdp);
            let network = NetworkSpecValue::new(NetworkSpecDraft {
                name: "reload-disabled".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 44,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![spec],
            });

            assert!(
                !manager.requires_reload(&network, &ctx).await?,
                "disabled BPF attachment should report no local reload requirement"
            );
            Ok(())
        }

        #[tokio::test(flavor = "current_thread")]
        async fn ensure_network_skips_when_bpf_attachment_disabled() -> Result<()> {
            let _guard = ConfigOverrideGuard::with_mutator(|config| {
                config.network.bpf.attach = false;
                config.network.nodeport.enabled = false;
            });

            let manager = NetworkBpfManager::new()?;
            let spec = NetworkSpecValue::new(NetworkSpecDraft {
                name: "test-vxlan-xdp".to_string(),
                description: String::new(),
                driver: NetworkDriver::Vxlan,
                subnet_cidr: "10.200.0.0/24".to_string(),
                vni: 42,
                mtu: 1400,
                sealed: false,
                bpf_programs: vec![BpfProgramSpec::new("vxlan_xdp")],
            });
            let ctx = NetworkInterfaceContext::new(Uuid::new_v4(), "lo", "lo");

            manager.ensure_network(&spec, &ctx).await?;
            manager.teardown_network(&ctx).await?;
            Ok(())
        }

        #[test]
        fn task_attachment_delta_can_be_reconciled_incrementally() {
            let mut loaded = LoadedNetwork::new(Some(OverlayIpFamily::Ipv4));
            loaded.push(LoadedProgram {
                spec: BpfProgramSpec::with_attach_point(
                    "bridge_tc_ingress",
                    BpfAttachPoint::BridgeTcIngress,
                ),
                target: OwnedAttachTarget::Tc {
                    interface: "lo".to_string(),
                    attach_type: TcAttachType::Ingress,
                },
                _artifact: PathBuf::new(),
                handle: Box::new(NoopHandle),
            });
            loaded.push(LoadedProgram {
                spec: BpfProgramSpec::with_attach_point(
                    "bridge_tc_ingress",
                    BpfAttachPoint::BridgeTcIngress,
                ),
                target: OwnedAttachTarget::Tc {
                    interface: "mnth-stale".to_string(),
                    attach_type: TcAttachType::Ingress,
                },
                _artifact: PathBuf::new(),
                handle: Box::new(NoopHandle),
            });

            let desired = vec![DesiredAttachment {
                attach_point: BpfAttachPoint::BridgeTcIngress,
                name: "bridge_tc_ingress".to_string(),
                interface: "lo".to_string(),
            }];

            assert!(
                can_incrementally_reconcile_loaded_network(
                    &loaded,
                    &desired,
                    Some(OverlayIpFamily::Ipv4)
                ),
                "dropping one stale mnth-* tc attachment should stay incremental"
            );
        }

        #[test]
        fn non_task_attachment_delta_requires_full_reload() {
            let mut loaded = LoadedNetwork::new(Some(OverlayIpFamily::Ipv4));
            loaded.push(LoadedProgram {
                spec: BpfProgramSpec::with_attach_point(
                    "bridge_tc_ingress",
                    BpfAttachPoint::BridgeTcIngress,
                ),
                target: OwnedAttachTarget::Tc {
                    interface: "mnhp-demo".to_string(),
                    attach_type: TcAttachType::Ingress,
                },
                _artifact: PathBuf::new(),
                handle: Box::new(NoopHandle),
            });

            let desired = Vec::new();

            assert!(
                !can_incrementally_reconcile_loaded_network(
                    &loaded,
                    &desired,
                    Some(OverlayIpFamily::Ipv4)
                ),
                "fixed bridge-facing ports should still require a full reload when they change"
            );
        }

        #[test]
        fn stale_bpf_attach_errno_detects_link_create_eexist() {
            assert!(is_stale_bpf_attach_errno(
                "bpf_link_create",
                Some(libc::EEXIST)
            ));
        }

        #[test]
        fn stale_bpf_attach_errno_detects_attach_busy() {
            assert!(is_stale_bpf_attach_errno(
                "bpf_set_link_xdp_fd",
                Some(libc::EBUSY)
            ));
        }

        #[test]
        fn stale_bpf_attach_errno_rejects_unrelated_errno() {
            assert!(!is_stale_bpf_attach_errno(
                "bpf_link_create",
                Some(libc::ENOENT)
            ));
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{BpfProgramSpec, NetworkInterfaceContext};
    use crate::network::types::NetworkSpecValue;
    use anyhow::Result;

    /// Non-Linux stub that exposes the same API surface while intentionally doing nothing.
    #[derive(Clone, Default)]
    pub struct PlatformBpfManager;

    impl PlatformBpfManager {
        /// Create the stub backing manager. Non-Linux targets skip eBPF reconciliation entirely.
        pub fn new() -> Result<Self> {
            Ok(Self)
        }

        /// Produce the same stub manager for callers that request an unavailable variant.
        pub fn unavailable() -> Self {
            Self
        }

        /// Non-Linux targets never perform a destructive eBPF reload because the stub manager
        /// does not attach any programs.
        pub async fn requires_reload(
            &self,
            _network: &NetworkSpecValue,
            _interfaces: &NetworkInterfaceContext,
        ) -> Result<bool> {
            Ok(false)
        }

        /// No-op placeholder to satisfy the async interface on platforms without eBPF support.
        pub async fn ensure_programs(
            &self,
            _network: &NetworkSpecValue,
            _interfaces: &NetworkInterfaceContext,
        ) -> Result<()> {
            Ok(())
        }

        /// No-op placeholder matching the Linux teardown path so higher layers can call it
        /// unconditionally.
        pub async fn teardown_programs(&self, _interfaces: &NetworkInterfaceContext) -> Result<()> {
            Ok(())
        }
    }
}

use platform::PlatformBpfManager;
