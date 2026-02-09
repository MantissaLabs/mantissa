use crate::network::attachment::host_access_peer_iface_name;
use crate::network::types::{BpfProgramSpec, NetworkSpecValue};
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
        Self {
            network_id,
            bridge_ifname: bridge_ifname.into(),
            vxlan_ifname: vxlan_ifname.into(),
            host_peer_ifname: host_access_peer_iface_name(network_id),
        }
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
        self.platform
            .ensure_programs(&spec.bpf_programs, interfaces)
            .await
    }

    /// Tear down any previously attached programs for the network so the kernel datapath stays
    /// clean when the overlay is removed or reconfigured.
    pub async fn teardown_network(&self, interfaces: &NetworkInterfaceContext) -> Result<()> {
        self.platform.teardown_programs(interfaces).await
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{BpfProgramSpec, NetworkInterfaceContext};
    use crate::config;
    use crate::network::types::BpfAttachPoint;
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

    #[derive(Clone, Copy)]
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
        ) -> Result<ProgramHandle>;
    }

    #[derive(Clone)]
    pub struct PlatformBpfManager {
        resolver: ArtifactResolver,
        loader: Arc<dyn ProgramLoader>,
        loaded: Arc<AsyncMutex<HashMap<Uuid, LoadedNetwork>>>,
    }

    struct LoadedNetwork {
        programs: Vec<LoadedProgram>,
    }

    impl LoadedNetwork {
        fn new() -> Self {
            Self {
                programs: Vec::new(),
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

        fn matches(&self, specs: &[BpfProgramSpec]) -> bool {
            self.canonical_specs() == canonical_specs(specs.iter())
        }

        fn canonical_specs(&self) -> Vec<(BpfAttachPoint, String)> {
            canonical_specs(self.programs.iter().map(|program| &program.spec))
        }
    }

    struct LoadedProgram {
        spec: BpfProgramSpec,
        _artifact: PathBuf,
        handle: ProgramHandle,
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
            programs: &[BpfProgramSpec],
            interfaces: &NetworkInterfaceContext,
        ) -> Result<()> {
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
            let map_pin_path = map_pin_dir(network_id)?;
            {
                let guard = self.loaded.lock().await;
                if let Some(existing) = guard.get(&network_id)
                    && existing.matches(programs)
                {
                    if lb_maps_required(programs) && !lb_maps_present(&map_pin_path) {
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

            self.clear_stale_xdp_targets(programs, interfaces);

            let mut ordered_programs: Vec<&BpfProgramSpec> = programs.iter().collect();
            ordered_programs.sort_by_key(|spec| attach_priority(spec.attach_point()));

            let mut network = LoadedNetwork::new();
            for spec in ordered_programs {
                let artifact = self
                    .resolver
                    .resolve(spec)
                    .with_context(|| format!("resolve artifact for program '{}'", spec))?;
                let attach_target = resolve_attach_target(spec.attach_point(), interfaces);
                let target_ifname = interface_name(attach_target);

                info!(
                    target: "network",
                    network = %interfaces.network_id(),
                    program = %spec,
                    attach_point = %spec.attach_point(),
                    interface = %target_ifname,
                    artifact = %artifact.display(),
                    "attaching bpf program"
                );

                let handle = match load_with_retry(
                    &*self.loader,
                    spec,
                    attach_target,
                    &artifact,
                    &map_pin_path,
                ) {
                    Ok(handle) => handle,
                    Err(err) => {
                        if let Err(teardown_err) = network.teardown() {
                            warn!(
                                target: "network",
                                network = %network_id,
                                "failed to rollback partially attached bpf programs: {teardown_err:#}"
                            );
                        }
                        return Err(err);
                    }
                };

                network.push(LoadedProgram {
                    spec: spec.clone(),
                    _artifact: artifact,
                    handle,
                });
            }

            let mut guard = self.loaded.lock().await;
            if let Some(replaced) = guard.insert(network_id, network)
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

            if let Some(network) = network {
                network.teardown()?;
                info!(
                    target: "network",
                    network = %interfaces.network_id(),
                    "detached bpf programs"
                );
            } else {
                debug!(
                    target: "network",
                    network = %interfaces.network_id(),
                    "no bpf programs recorded for teardown"
                );
            }

            Ok(())
        }

        fn validate_programs(
            &self,
            programs: &[BpfProgramSpec],
            interfaces: &NetworkInterfaceContext,
        ) -> Result<()> {
            let mut seen = HashSet::new();
            for spec in programs {
                let attach = spec.attach_point();
                let target = resolve_attach_target(attach, interfaces);
                let target_if = interface_name(target);

                if !interface_exists(target_if) {
                    return Err(anyhow!("interface '{}' missing for {}", target_if, attach));
                }

                if !seen.insert((attach, target_if.to_string())) {
                    return Err(anyhow!(
                        "multiple programs declared for {} on {}",
                        attach,
                        target_if
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
                let attach_target = resolve_attach_target(point, interfaces);
                let interface = interface_name(attach_target).to_string();
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

        fn resolve(&self, spec: &BpfProgramSpec) -> Result<PathBuf> {
            for candidate in self.candidates(&spec.name) {
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

    fn resolve_attach_target<'a>(
        attach_point: BpfAttachPoint,
        interfaces: &'a NetworkInterfaceContext,
    ) -> AttachTarget<'a> {
        match attach_point {
            BpfAttachPoint::VxlanXdp => AttachTarget::Xdp {
                interface: interfaces.vxlan_ifname(),
            },
            BpfAttachPoint::BridgeXdp => AttachTarget::Xdp {
                interface: interfaces.bridge_ifname(),
            },
            BpfAttachPoint::BridgeTcIngress => AttachTarget::Tc {
                interface: if interface_exists(interfaces.host_peer_ifname()) {
                    interfaces.host_peer_ifname()
                } else {
                    interfaces.bridge_ifname()
                },
                attach_type: TcAttachType::Ingress,
            },
            BpfAttachPoint::BridgeTcEgress => AttachTarget::Tc {
                interface: if interface_exists(interfaces.host_peer_ifname()) {
                    interfaces.host_peer_ifname()
                } else {
                    interfaces.bridge_ifname()
                },
                attach_type: TcAttachType::Egress,
            },
        }
    }

    fn interface_name(target: AttachTarget<'_>) -> &str {
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

    fn canonical_specs<'a, I>(specs: I) -> Vec<(BpfAttachPoint, String)>
    where
        I: IntoIterator<Item = &'a BpfProgramSpec>,
    {
        let mut out: Vec<_> = specs
            .into_iter()
            .map(|spec| (spec.attach_point(), spec.name.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        out
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

    /// Return whether the configured programs expect the load balancer maps to be present.
    fn lb_maps_required(programs: &[BpfProgramSpec]) -> bool {
        programs.iter().any(|spec| {
            matches!(
                spec.attach_point(),
                BpfAttachPoint::BridgeTcIngress | BpfAttachPoint::BridgeTcEgress
            )
        })
    }

    /// Check whether the pinned load balancer maps are reachable from userspace.
    fn lb_maps_present(base: &Path) -> bool {
        const MAPS: &[&str] = &["LB_VIPS", "LB_BACKENDS", "LB_FWD", "LB_REV"];
        MAPS.iter().all(|name| map_is_pinned(base, name))
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

    /// Ensure LB-related maps are pinned so subsequent program loads reuse the same instances and
    /// userspace can program them via predictable paths.
    fn ensure_lb_maps_pinned(bpf: &mut Ebpf, base: &Path) -> Result<()> {
        const MAPS: &[&str] = &["LB_VIPS", "LB_BACKENDS", "LB_FWD", "LB_REV"];
        if let Err(err) = fs::create_dir_all(base) {
            warn!(
                target: "network",
                path = %base.display(),
                "failed to prepare lb map pin directory: {err:#}"
            );
            return Ok(());
        }

        for name in MAPS {
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

    fn map_pin_dir(network_id: Uuid) -> Result<PathBuf> {
        ensure_bpffs().context("prepare bpffs mount")?;
        let path = PathBuf::from("/sys/fs/bpf/mantissa").join(network_id.to_string());
        fs::create_dir_all(&path)
            .with_context(|| format!("create map pin directory {}", path.display()))?;
        Ok(path)
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
    ) -> Result<ProgramHandle> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match loader
                .load_and_attach(spec, target, artifact, map_pin_path)
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
                                interface = %interface_name(target),
                                "failed to clear conflicting XDP link before retry: {detach_err:#}"
                            );
                            return Err(err);
                        }
                        warn!(
                            target: "network",
                            program = %spec,
                            interface = %interface_name(target),
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
        ) -> Result<ProgramHandle> {
            let mut loader = EbpfLoader::new();
            loader.map_pin_path(map_pin_path);
            let mut bpf = loader
                .load_file(artifact)
                .with_context(|| format!("load bpf object {}", artifact.display()))?;
            ensure_lb_maps_pinned(&mut bpf, map_pin_path).context("pin load balancer maps")?;

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
        use std::fs;
        use tempfile::TempDir;
        use uuid::Uuid;

        #[test]
        fn resolves_artifact_from_config_directory() -> Result<()> {
            let dir = TempDir::new().context("create temp dir")?;
            let artifact_path = dir.path().join("resolver-example.bpf.o");
            fs::write(&artifact_path, b"test").context("write artifact stub")?;

            let mut config = crate::config::global_config();
            config.network.bpf.artifact_dir = Some(dir.path().to_string_lossy().to_string());
            let resolver = ArtifactResolver::new_with_config(&config);
            let spec = BpfProgramSpec::new("resolver-example");
            let resolved = resolver.resolve(&spec)?;
            assert_eq!(resolved, artifact_path);

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
    }
}

#[cfg(not(target_os = "linux"))]
mod platform {
    use super::{BpfProgramSpec, NetworkInterfaceContext};
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
            _programs: &[BpfProgramSpec],
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
