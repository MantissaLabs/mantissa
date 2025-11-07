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
    use crate::network::types::BpfAttachPoint;
    use anyhow::{Context, Result, anyhow};
    use aya::programs::tc::{SchedClassifierLinkId, TcAttachType, qdisc_add_clsact};
    use aya::programs::xdp::XdpLinkId;
    use aya::programs::{SchedClassifier, Xdp, XdpFlags};
    use aya::{Ebpf, EbpfLoader};
    use libc::if_nametoindex;
    use std::collections::{HashMap, HashSet};
    use std::env;
    use std::ffi::CString;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;
    use tracing::{debug, info};
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
    }

    struct LoadedProgram {
        _spec: BpfProgramSpec,
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
            if programs.is_empty() {
                return Ok(());
            }

            self.validate_programs(programs, interfaces)?;

            let mut network = LoadedNetwork::new();
            for spec in programs {
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

                let handle = self
                    .loader
                    .load_and_attach(spec, attach_target, &artifact)
                    .with_context(|| {
                        format!(
                            "load and attach program '{}' ({})",
                            spec,
                            artifact.display()
                        )
                    })?;

                network.push(LoadedProgram {
                    _spec: spec.clone(),
                    _artifact: artifact,
                    handle,
                });
            }

            let mut guard = self.loaded.lock().await;
            guard.insert(interfaces.network_id(), network);
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
    }

    #[derive(Clone)]
    struct ArtifactResolver {
        search_roots: Vec<PathBuf>,
    }

    impl ArtifactResolver {
        fn new() -> Self {
            let mut roots = Vec::new();
            if let Some(dir) = env::var_os("MANTISSA_BPF_DIR") {
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

    fn default_loader() -> Arc<dyn ProgramLoader> {
        if env::var_os("MANTISSA_BPF_NO_ATTACH").is_some() {
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
                interface: interfaces.bridge_ifname(),
                attach_type: TcAttachType::Ingress,
            },
            BpfAttachPoint::BridgeTcEgress => AttachTarget::Tc {
                interface: interfaces.bridge_ifname(),
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

    #[derive(Default)]
    struct NoopProgramLoader;

    impl ProgramLoader for NoopProgramLoader {
        fn load_and_attach(
            &self,
            _spec: &BpfProgramSpec,
            _target: AttachTarget<'_>,
            _artifact: &Path,
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
        ) -> Result<ProgramHandle> {
            let mut loader = EbpfLoader::new();
            let mut bpf = loader
                .load_file(artifact)
                .with_context(|| format!("load bpf object {}", artifact.display()))?;

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

        fn reset_env(original: Option<std::ffi::OsString>) {
            if let Some(value) = original {
                env::set_var("MANTISSA_BPF_DIR", value);
            } else {
                env::remove_var("MANTISSA_BPF_DIR");
            }
        }

        #[test]
        fn resolves_artifact_from_env_directory() -> Result<()> {
            let dir = TempDir::new().context("create temp dir")?;
            let artifact_path = dir.path().join("resolver-example.bpf.o");
            fs::write(&artifact_path, b"test").context("write artifact stub")?;

            let original = env::var_os("MANTISSA_BPF_DIR");
            env::set_var("MANTISSA_BPF_DIR", dir.path());

            let resolver = ArtifactResolver::new();
            let spec = BpfProgramSpec::new("resolver-example");
            let resolved = resolver.resolve(&spec)?;
            assert_eq!(resolved, artifact_path);

            reset_env(original);
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
