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
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tracing::{debug, info, warn};
    use uuid::Uuid;

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

    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
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
                .map(|program| DesiredAttachment {
                    attach_point: program.spec.attach_point(),
                    name: program.spec.name.clone(),
                    interface: target_interface(program.target.as_ref()).to_string(),
                })
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
            {
                let guard = self.loaded.lock().await;
                if let Some(existing) = guard.get(&network_id)
                    && existing.matches(&canonical_desired, lb_family)
                {
                    let map_pin_path = Self::map_pin_path(network_id);
                    if let Some(family) = lb_family
                        && !lb_maps_present(&map_pin_path, family)
                    {
                        warn!(
                            target: "network",
                            network = %network_id,
                            "load balancer maps missing despite matching programs; forcing reload"
                        );
                    } else {
                        return Ok(());
                    }
                }
            }

            let mut guard = self.loaded.lock().await;
            let previous = guard.remove(&network_id);
            drop(guard);

            if let Some(existing) = previous
                && let Err(err) = existing.teardown()
            {
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
            OverlayIpFamily::Ipv4 => &["LB_VIPS", "LB_BACKENDS", "LB_FWD", "LB_REV"],
            OverlayIpFamily::Ipv6 => &["LB_VIPS_V6", "LB_BACKENDS_V6", "LB_FWD_V6", "LB_REV_V6"],
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
                return sys.call == "bpf_link_create"
                    && matches!(sys.io_error.raw_os_error(), Some(code) if code == libc::EEXIST);
            }
            if let Some(aya::programs::ProgramError::SyscallError(sys)) =
                cause.downcast_ref::<aya::programs::ProgramError>()
            {
                return sys.call == "bpf_link_create"
                    && matches!(sys.io_error.raw_os_error(), Some(code) if code == libc::EEXIST);
            }
            false
        })
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
