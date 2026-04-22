#![allow(clippy::unwrap_used)]

use super::planner::{SchedulingError, StartIntent};
use super::reservation::{RemotePrepareRejection, RemotePrepareRejectionReason};
use super::*;

use crate::agents::types::{
    AGENT_ALLOW_NETWORK_ENV_VAR, AGENT_ALLOW_WRITE_ENV_VAR, AGENT_WORKDIR_ENV_VAR,
};
use crate::network::attachment::{AttachmentProvisionerApi, AttachmentProvisioningRequest};
use crate::network::events::ForwardingEvent;
use crate::network::registry::NetworkRegistry;
use crate::network::types::{
    NetworkAttachmentDraft, NetworkAttachmentState, NetworkAttachmentValue, NetworkDriver,
    NetworkPeerState, NetworkPeerStateValue, NetworkSpecDraft, NetworkSpecValue,
};
use crate::registry::Registry;
use crate::runtime::set::RuntimeSet;
use crate::runtime::types::{
    ResourceLimits, RuntimeAttachOptions, RuntimeAttachmentTarget, RuntimeBackend,
    RuntimeCapabilities, RuntimeConfigInfo, RuntimeCreateRequest, RuntimeError, RuntimeExecOptions,
    RuntimeExecResult, RuntimeInfo, RuntimeLogFrame, RuntimeLogStream, RuntimeLogsOptions,
    RuntimeResult, RuntimeSandboxAccessMode, RuntimeSandboxNetworkMode, RuntimeStateInfo,
    RuntimeSupportContract, RuntimeSupportProfile,
};
use crate::scheduler::digest::SchedulerDigestRegistry;
use crate::scheduler::{SlotCapacity, SlotReservationRequest, SlotSpec, SlotState};
use crate::secrets::crypto::SecretKeyring;
use crate::secrets::registry::SecretRegistry;
use crate::store::local::{LocalSessionStore, SecretMasterStore};
use crate::store::network_store::{
    open_network_attachment_store, open_network_peer_store, open_network_spec_store,
};
use crate::store::peer_store::open_peers_store;
use crate::store::scheduler_digest_store::open_scheduler_digest_store;
use crate::store::scheduler_store::open_scheduler_store;
use crate::store::secret_store::open_secret_store;
use crate::store::volume_store::{open_volume_node_store, open_volume_spec_store};
use crate::store::workload_store::open_workload_store;
use crate::task::types::{TaskStateFilter, TaskStateKind};
use crate::topology::peers::PeerSchedulingState;
use crate::volumes::VolumeRegistry;
use crate::volumes::local::managed_volume_data_path;
use crate::volumes::types::{
    LocalVolumeOwnership, LocalVolumeSource, LocalVolumeSpec, VolumeAccessMode, VolumeBindingMode,
    VolumeDriver, VolumeNodeState, VolumeReclaimPolicy, VolumeSpecDraft, VolumeSpecValue,
    VolumeStatus,
};
use crate::workload::model::select_best_workload_value;
use crate::workload::model::{
    ExecutionPlatform, WorkloadAgentRunMetadata, WorkloadOwner, WorkloadServiceMetadata,
    WorkloadStatus, WorkloadValue, WorkloadValueDraft,
};
use crate::workload::types::{
    ResolvedExecutionSpec, WorkloadLivenessProbe, WorkloadLivenessProbeKind,
    WorkloadRestartPolicyKind,
};
use ::health::HealthMonitor;
use anyhow::{Result, anyhow};
use async_channel::bounded;
use async_trait::async_trait;
use chrono::Utc;
use ed25519_dalek::SigningKey;
use net::noise::NoiseKeys;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::sync::{Notify, RwLock, mpsc};

type ExecCall = (String, Vec<String>, Option<std::time::Duration>);
type AttachCall = (String, RuntimeAttachOptions);
type ExecStreamCall = (String, RuntimeExecOptions);
type LogCall = (String, RuntimeLogsOptions);

#[derive(Clone, Default)]
struct MockRuntimeBackend {
    created: Arc<AsyncMutex<Vec<String>>>,
    create_requests: Arc<AsyncMutex<Vec<RuntimeCreateRequest>>>,
    create_errors: Arc<AsyncMutex<VecDeque<RuntimeError>>>,
    exec_calls: Arc<AsyncMutex<Vec<ExecCall>>>,
    exec_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
    exec_results: Arc<AsyncMutex<VecDeque<RuntimeResult<RuntimeExecResult>>>>,
    stopped: Arc<AsyncMutex<Vec<String>>>,
    stop_timeouts: Arc<AsyncMutex<Vec<Option<std::time::Duration>>>>,
    stop_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
    removed: Arc<AsyncMutex<Vec<String>>>,
    remove_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
    open_stdin: Arc<AsyncMutex<Vec<bool>>>,
    limits: Arc<AsyncMutex<Vec<ResourceLimits>>>,
    volume_mounts: Arc<AsyncMutex<Vec<Vec<String>>>>,
    inspect: Arc<AsyncMutex<HashMap<String, RuntimeInfo>>>,
    inspect_calls: Arc<AsyncMutex<Vec<String>>>,
    listed: Arc<AsyncMutex<Vec<RuntimeInfo>>>,
    attach_calls: Arc<AsyncMutex<Vec<AttachCall>>>,
    attach_frames: Arc<AsyncMutex<HashMap<String, Vec<RuntimeLogFrame>>>>,
    attach_inputs: Arc<AsyncMutex<HashMap<String, Vec<Vec<u8>>>>>,
    attach_errors: Arc<AsyncMutex<VecDeque<RuntimeError>>>,
    exec_stream_calls: Arc<AsyncMutex<Vec<ExecStreamCall>>>,
    exec_stream_frames: Arc<AsyncMutex<HashMap<String, Vec<RuntimeLogFrame>>>>,
    exec_stream_inputs: Arc<AsyncMutex<HashMap<String, Vec<Vec<u8>>>>>,
    exec_stream_results: Arc<AsyncMutex<VecDeque<RuntimeResult<RuntimeExecResult>>>>,
    log_calls: Arc<AsyncMutex<Vec<LogCall>>>,
    log_frames: Arc<AsyncMutex<HashMap<String, Vec<RuntimeLogFrame>>>>,
    log_errors: Arc<AsyncMutex<VecDeque<RuntimeError>>>,
    present_images: Arc<AsyncMutex<HashSet<String>>>,
    pull_errors: Arc<AsyncMutex<VecDeque<RuntimeError>>>,
    pull_calls: Arc<AsyncMutex<Vec<String>>>,
    pull_delay: Arc<AsyncMutex<Option<std::time::Duration>>>,
}

#[async_trait]
impl RuntimeBackend for MockRuntimeBackend {
    async fn create_instance(&self, request: RuntimeCreateRequest) -> RuntimeResult<String> {
        if let Some(err) = self.create_errors.lock().await.pop_front() {
            return Err(err);
        }

        self.create_requests.lock().await.push(request.clone());
        let resource_limits = request.resource_limits;
        let volumes = request.volumes.unwrap_or_default();
        self.open_stdin.lock().await.push(request.open_stdin);
        let mut guard = self.created.lock().await;
        let id = format!("container-{}", guard.len());
        guard.push(id.clone());
        self.limits.lock().await.push(resource_limits);
        self.volume_mounts.lock().await.push(volumes);

        let mut inspect = self.inspect.lock().await;
        let response = RuntimeInfo {
            id: id.clone(),
            name: request.name.clone(),
            image: request.image,
            status: "created".to_string(),
            state: RuntimeStateInfo {
                raw_status: Some("created".to_string()),
                running: Some(false),
                pid: Some(10_000 + inspect.len() as i64),
                ..Default::default()
            },
            ..Default::default()
        };
        inspect.insert(id.clone(), response.clone());
        inspect.insert(request.name, response);
        Ok(id)
    }

    async fn start_instance(&self, instance_id: &str) -> RuntimeResult<()> {
        let mut inspect = self.inspect.lock().await;
        let mut found = false;
        for info in inspect.values_mut() {
            if info.id == instance_id || info.name == instance_id {
                info.status = "Up".to_string();
                info.state.raw_status = Some("running".to_string());
                info.state.running = Some(true);
                if info.state.pid.unwrap_or_default() == 0 {
                    info.state.pid = Some(10_000);
                }
                info.attachment_target = Some(RuntimeAttachmentTarget::NetworkNamespacePid(
                    info.state.pid.unwrap_or(10_000) as i32,
                ));
                found = true;
            }
        }
        if !found {
            return Err(RuntimeError::NotFound(instance_id.to_string()));
        }
        Ok(())
    }

    async fn stop_instance(
        &self,
        instance_id: &str,
        timeout: Option<std::time::Duration>,
    ) -> RuntimeResult<()> {
        let delay = *self.stop_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.stopped.lock().await.push(instance_id.to_string());
        self.stop_timeouts.lock().await.push(timeout);
        let mut inspect = self.inspect.lock().await;
        for info in inspect.values_mut() {
            if info.id == instance_id || info.name == instance_id {
                info.status = "Exited".to_string();
                info.state.raw_status = Some("exited".to_string());
                info.state.running = Some(false);
                info.state.pid = Some(0);
                info.attachment_target = None;
            }
        }
        Ok(())
    }

    async fn exec_instance(
        &self,
        instance_id: &str,
        command: &[String],
        timeout: Option<std::time::Duration>,
    ) -> RuntimeResult<RuntimeExecResult> {
        let delay = *self.exec_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.exec_calls
            .lock()
            .await
            .push((instance_id.to_string(), command.to_vec(), timeout));
        if let Some(result) = self.exec_results.lock().await.pop_front() {
            return result;
        }
        Ok(RuntimeExecResult { exit_code: Some(0) })
    }

    async fn restart_instance(
        &self,
        _instance_id: &str,
        _timeout: Option<std::time::Duration>,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    async fn remove_instance(
        &self,
        instance_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> RuntimeResult<()> {
        let delay = *self.remove_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        self.removed.lock().await.push(instance_id.to_string());
        let mut inspect = self.inspect.lock().await;
        inspect.retain(|key, response| key != instance_id && response.id != instance_id);
        Ok(())
    }

    async fn list_instances(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> RuntimeResult<Vec<RuntimeInfo>> {
        Ok(self.listed.lock().await.clone())
    }

    async fn inspect_instance(&self, instance_id: &str) -> RuntimeResult<RuntimeInfo> {
        self.inspect_calls
            .lock()
            .await
            .push(instance_id.to_string());
        let guard = self.inspect.lock().await;
        guard
            .get(instance_id)
            .cloned()
            .ok_or_else(|| RuntimeError::NotFound(instance_id.into()))
    }

    async fn image_present(&self, image: &str) -> RuntimeResult<bool> {
        Ok(self.present_images.lock().await.contains(image))
    }

    async fn pull_image(&self, image: &str) -> RuntimeResult<()> {
        self.pull_calls.lock().await.push(image.to_string());
        let delay = *self.pull_delay.lock().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        if let Some(err) = self.pull_errors.lock().await.pop_front() {
            return Err(err);
        }
        Ok(())
    }

    async fn stream_instance_logs(
        &self,
        instance_id: &str,
        options: &RuntimeLogsOptions,
        logs_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
    ) -> RuntimeResult<()> {
        self.log_calls
            .lock()
            .await
            .push((instance_id.to_string(), options.clone()));
        if let Some(err) = self.log_errors.lock().await.pop_front() {
            return Err(err);
        }

        let frames = self
            .log_frames
            .lock()
            .await
            .get(instance_id)
            .cloned()
            .unwrap_or_default();
        for frame in frames {
            if logs_tx.send(frame).await.is_err() {
                return Ok(());
            }
        }

        Ok(())
    }

    async fn attach_instance(
        &self,
        instance_id: &str,
        options: &RuntimeAttachOptions,
        output_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
        mut input_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> RuntimeResult<()> {
        self.attach_calls
            .lock()
            .await
            .push((instance_id.to_string(), options.clone()));
        if let Some(err) = self.attach_errors.lock().await.pop_front() {
            return Err(err);
        }

        let frames = self
            .attach_frames
            .lock()
            .await
            .get(instance_id)
            .cloned()
            .unwrap_or_default();
        for frame in frames {
            if output_tx.send(frame).await.is_err() {
                return Ok(());
            }
        }

        let mut chunks = Vec::new();
        while let Some(chunk) = input_rx.recv().await {
            chunks.push(chunk);
        }
        self.attach_inputs
            .lock()
            .await
            .insert(instance_id.to_string(), chunks);
        Ok(())
    }

    async fn exec_instance_stream(
        &self,
        instance_id: &str,
        options: &RuntimeExecOptions,
        output_tx: tokio::sync::mpsc::Sender<RuntimeLogFrame>,
        mut input_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> RuntimeResult<RuntimeExecResult> {
        self.exec_stream_calls
            .lock()
            .await
            .push((instance_id.to_string(), options.clone()));

        let frames = self
            .exec_stream_frames
            .lock()
            .await
            .get(instance_id)
            .cloned()
            .unwrap_or_default();
        for frame in frames {
            if output_tx.send(frame).await.is_err() {
                return Ok(RuntimeExecResult { exit_code: None });
            }
        }

        let mut chunks = Vec::new();
        while let Some(chunk) = input_rx.recv().await {
            chunks.push(chunk);
        }
        self.exec_stream_inputs
            .lock()
            .await
            .insert(instance_id.to_string(), chunks);

        if let Some(result) = self.exec_stream_results.lock().await.pop_front() {
            return result;
        }

        Ok(RuntimeExecResult { exit_code: Some(0) })
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            exec: true,
            interactive_exec: true,
            logs: true,
            attach: true,
            lifecycle_events: false,
        }
    }

    fn advertised_support(&self) -> RuntimeSupportProfile {
        RuntimeSupportProfile::from_exact_contracts(
            [
                RuntimeSupportContract::new(ExecutionPlatform::Oci, IsolationMode::Standard, None),
                RuntimeSupportContract::new(ExecutionPlatform::Oci, IsolationMode::Sandboxed, None),
                RuntimeSupportContract::new(
                    ExecutionPlatform::Oci,
                    IsolationMode::Sandboxed,
                    Some("oci-default"),
                ),
                RuntimeSupportContract::new(
                    ExecutionPlatform::Oci,
                    IsolationMode::Sandboxed,
                    Some("nono-default"),
                ),
            ],
            self.capabilities().feature_flags(),
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn runtime_info_with_state(
    id: &str,
    name: &str,
    image: &str,
    status: &str,
    raw_status: &str,
    running: bool,
    pid: i64,
    exit_code: Option<i32>,
    error: Option<&str>,
) -> RuntimeInfo {
    let labels = name
        .strip_prefix("mantissa-")
        .and_then(|suffix| Uuid::parse_str(suffix).ok())
        .map(|workload_id| {
            HashMap::from([("mantissa.workload_id".to_string(), workload_id.to_string())])
        })
        .unwrap_or_default();
    RuntimeInfo {
        id: id.to_string(),
        name: name.to_string(),
        image: image.to_string(),
        labels,
        status: status.to_string(),
        state: RuntimeStateInfo {
            raw_status: Some(raw_status.to_string()),
            running: Some(running),
            pid: Some(pid),
            exit_code,
            error: error.map(str::to_string),
        },
        attachment_target: (running && pid > 0)
            .then_some(RuntimeAttachmentTarget::NetworkNamespacePid(pid as i32)),
        ..Default::default()
    }
}

fn running_runtime_info(id: &str, name: &str, image: &str) -> RuntimeInfo {
    runtime_info_with_state(id, name, image, "Up", "running", true, 1000, None, None)
}

#[derive(Default)]
struct FakeAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
}

#[async_trait]
impl AttachmentProvisionerApi for FakeAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct FlakyAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
    fail_next: AsyncMutex<bool>,
}

#[async_trait]
impl AttachmentProvisionerApi for FlakyAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut flag = self.fail_next.lock().await;
        if *flag {
            *flag = false;
            return Err(anyhow!("synthetic teardown failure"));
        }
        drop(flag);

        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

struct RetryingAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
    fail_remaining: AsyncMutex<usize>,
    ensure_calls: AsyncMutex<Vec<RuntimeAttachmentTarget>>,
}

impl RetryingAttachmentProvisioner {
    fn new(failures: usize) -> Self {
        Self {
            attachments: AsyncMutex::new(HashSet::new()),
            fail_remaining: AsyncMutex::new(failures),
            ensure_calls: AsyncMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl AttachmentProvisionerApi for RetryingAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        self.ensure_calls
            .lock()
            .await
            .push(request.attachment_target.clone());
        let mut remaining = self.fail_remaining.lock().await;
        if *remaining > 0 {
            *remaining -= 1;
            let attachment_target = format!("{:?}", request.attachment_target);
            return Err(anyhow!(
                "failed to move mntc-test to attachment target {attachment_target}\n\nCaused by:\n    Received a netlink error message No such process (os error 3)"
            ));
        }
        drop(remaining);

        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

struct BlockingAttachmentProvisioner {
    attachments: AsyncMutex<HashSet<Uuid>>,
    entered_count: AtomicUsize,
    release_ready: AtomicBool,
    entered: Notify,
    release: Notify,
}

impl Default for BlockingAttachmentProvisioner {
    fn default() -> Self {
        Self {
            attachments: AsyncMutex::new(HashSet::new()),
            entered_count: AtomicUsize::new(0),
            release_ready: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}

impl BlockingAttachmentProvisioner {
    async fn wait_for_first_attempt(&self) {
        while self.entered_count.load(Ordering::SeqCst) == 0 {
            self.entered.notified().await;
        }
    }

    fn release_first_attempt(&self) {
        self.release_ready.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

#[async_trait]
impl AttachmentProvisionerApi for BlockingAttachmentProvisioner {
    async fn attachment_exists(&self, attachment_id: Uuid) -> Result<bool> {
        let guard = self.attachments.lock().await;
        Ok(guard.contains(&attachment_id))
    }

    async fn ensure_attachment(&self, request: &AttachmentProvisioningRequest<'_>) -> Result<()> {
        if self.entered_count.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_waiters();
            while !self.release_ready.load(Ordering::SeqCst) {
                self.release.notified().await;
            }
        }

        let mut guard = self.attachments.lock().await;
        guard.insert(request.attachment_id);
        Ok(())
    }

    async fn teardown_attachment(&self, attachment_id: Uuid) -> Result<()> {
        let mut guard = self.attachments.lock().await;
        guard.remove(&attachment_id);
        Ok(())
    }

    async fn ensure_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn remove_remote_fdb(
        &self,
        _vxlan_name: &str,
        _mac: &str,
        _dst: std::net::IpAddr,
    ) -> Result<()> {
        Ok(())
    }

    async fn ensure_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<bool> {
        Ok(true)
    }

    async fn remove_flood_entry(&self, _vxlan_name: &str, _dst: std::net::IpAddr) -> Result<()> {
        Ok(())
    }

    async fn list_remote_fdb(&self, _vxlan_name: &str) -> Result<Vec<(String, std::net::IpAddr)>> {
        Ok(Vec::new())
    }
}

fn temp_db(prefix: &str) -> (Arc<redb::Database>, tempfile::TempDir) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join(format!("{prefix}-{}.redb", Uuid::new_v4()));
    let db = Arc::new(redb::Database::create(path).expect("create db"));
    (db, dir)
}

async fn setup_manager() -> (
    WorkloadManager,
    Rc<Scheduler>,
    Arc<MockRuntimeBackend>,
    NetworkRegistry,
) {
    setup_manager_with_forwarding(None, None).await
}

async fn setup_manager_with_forwarding(
    forwarding_events: Option<mpsc::UnboundedSender<ForwardingEvent>>,
    attachment_override: Option<Arc<dyn AttachmentProvisionerApi>>,
) -> (
    WorkloadManager,
    Rc<Scheduler>,
    Arc<MockRuntimeBackend>,
    NetworkRegistry,
) {
    let actor = Uuid::new_v4();
    let (scheduler_db, _dir) = temp_db("scheduler");
    let scheduler_store =
        open_scheduler_store(scheduler_db.clone(), actor).expect("open scheduler store");
    scheduler_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild scheduler store");
    let scheduler_digest_store =
        open_scheduler_digest_store(scheduler_db.clone(), actor).expect("open scheduler digests");
    scheduler_digest_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild scheduler digest store");

    let (registry_db, _reg_dir) = temp_db("registry");
    let peers_store = open_peers_store(registry_db.clone(), actor).expect("open peers store");
    peers_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild peers store");

    let noise_keys = Arc::new(NoiseKeys::from_private_bytes([0x11; 32]));
    let session_store =
        LocalSessionStore::open(registry_db.clone(), noise_keys.as_ref()).expect("open sessions");

    let (task_db, _task_dir) = temp_db("tasks");
    let workload_store = open_workload_store(task_db.clone(), actor).expect("open workload store");
    workload_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild workload store");

    let (network_db, _network_dir) = temp_db("networks");
    let network_spec_store =
        open_network_spec_store(network_db.clone(), actor).expect("open network spec store");
    network_spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network spec store");

    let network_peer_store =
        open_network_peer_store(network_db.clone(), actor).expect("open network peer store");
    network_peer_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network peer store");

    let network_attachment_store = open_network_attachment_store(network_db.clone(), actor)
        .expect("open network attachment store");
    network_attachment_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild network attachment store");

    let (secret_db, _secret_dir) = temp_db("secrets");
    let secret_store = open_secret_store(secret_db.clone(), actor).expect("open secret store");
    secret_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild secret store");
    let secret_registry = SecretRegistry::new(secret_store);
    let (volume_db, _volume_dir) = temp_db("volumes");
    let volume_spec_store =
        open_volume_spec_store(volume_db.clone(), actor).expect("open volume spec store");
    volume_spec_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume spec store");
    let volume_node_store =
        open_volume_node_store(volume_db.clone(), actor).expect("open volume node store");
    volume_node_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild volume node store");
    let volume_registry = VolumeRegistry::new(volume_spec_store, volume_node_store);
    let (master_db, _master_dir) = temp_db("master");
    let master_store = SecretMasterStore::new(master_db.clone()).expect("open master store");
    let master_record = master_store
        .ensure_current()
        .expect("ensure master key record");
    let secret_keyring = Arc::new(RwLock::new(SecretKeyring::new(
        master_store.clone(),
        master_record,
    )));

    let (tx, rx) = bounded(64);
    let mock_cm = Arc::new(MockRuntimeBackend::default());
    let signing_key = SigningKey::try_from(&[7u8; 32][..]).expect("signing key");
    let registry = Registry::new(
        peers_store.clone(),
        session_store,
        signing_key,
        noise_keys.clone(),
        actor,
        HealthMonitor::new(actor),
    );

    let scheduler = Rc::new(
        Scheduler::new(scheduler_store.clone(), registry.clone(), actor).expect("create scheduler"),
    );
    scheduler.set_digest_registry(SchedulerDigestRegistry::new(scheduler_digest_store));

    let network_registry = NetworkRegistry::new(
        network_spec_store,
        network_peer_store,
        network_attachment_store,
    );

    let attachment = attachment_override.unwrap_or_else(|| {
        Arc::new(FakeAttachmentProvisioner::default()) as Arc<dyn AttachmentProvisionerApi>
    });
    let local_volume_root =
        std::env::temp_dir().join(format!("mantissa-task-manager-volumes-{actor}"));
    std::fs::create_dir_all(&local_volume_root).expect("create local volume root");

    let manager = WorkloadManager::new(WorkloadManagerConfig {
        store: workload_store,
        tx,
        rx,
        local_node_id: actor,
        local_node_name: "local-node".to_string(),
        scheduler: scheduler.clone(),
        runtime_set: RuntimeSet::singleton("mock", mock_cm.clone()),
        registry,
        network_registry: network_registry.clone(),
        volume_registry,
        secret_registry,
        secret_keyring: secret_keyring.clone(),
        forwarding_events,
        attachment_override: Some(attachment),
        runtime_config: None,
        local_volume_root,
        enforce_local_volume_capacity: false,
    });

    (manager, scheduler, mock_cm, network_registry)
}

#[tokio::test]
async fn local_agent_launch_builds_runtime_sandbox_policy() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;
    let task_id = Uuid::new_v4();
    let command = vec!["sh".to_string(), "-lc".to_string(), "pwd".to_string()];
    let gpu_device_ids = Vec::new();
    let env = vec![
        crate::workload::model::WorkloadEnvironmentVariable {
            name: AGENT_ALLOW_NETWORK_ENV_VAR.to_string(),
            value: Some("false".to_string()),
            secret: None,
        },
        crate::workload::model::WorkloadEnvironmentVariable {
            name: AGENT_ALLOW_WRITE_ENV_VAR.to_string(),
            value: Some("false".to_string()),
            secret: None,
        },
        crate::workload::model::WorkloadEnvironmentVariable {
            name: AGENT_WORKDIR_ENV_VAR.to_string(),
            value: Some("/workspace".to_string()),
            secret: None,
        },
    ];
    let owner = WorkloadOwner::AgentRun(WorkloadAgentRunMetadata::new(
        Uuid::new_v4(),
        "demo-session",
        Uuid::new_v4(),
    ));

    manager
        .launch_task_instance(&super::launch::InstanceLaunchRequest {
            task_id,
            task_name: "demo-agent",
            instance_name: "mantissa-demo-agent",
            image: "ghcr.io/mantissa/demo-agent:latest",
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: IsolationMode::Sandboxed,
            isolation_profile: Some("nono-default"),
            command: &command,
            tty: false,
            cpu_millis: 250,
            memory_bytes: 128 * 1024 * 1024,
            gpu_count: 0,
            gpu_device_ids: &gpu_device_ids,
            truncate_gpu_device_ids: false,
            restart_policy: None,
            env: &env,
            secret_files: &[],
            volume_mounts: &[],
            networks: &[],
            owner: Some(&owner),
        })
        .await
        .expect("launch agent task");

    let create_request = mock_cm
        .create_requests
        .lock()
        .await
        .last()
        .cloned()
        .expect("captured runtime create request");
    let policy = create_request
        .sandbox_policy
        .expect("agent launch should carry a sandbox policy");

    assert_eq!(policy.network, RuntimeSandboxNetworkMode::Blocked);
    assert_eq!(
        policy.working_directory,
        Some(std::path::PathBuf::from("/workspace"))
    );
    assert!(policy.filesystem.iter().any(|rule| {
        rule.path == std::path::Path::new("/workspace")
            && rule.access == RuntimeSandboxAccessMode::Read
    }));
}

/// Ensures task-manager teardown always removes node-scoped secret staging directories.
#[tokio::test]
async fn workload_manager_drop_cleans_secret_runtime_root() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let runtime_root = manager.secrets.secret_runtime_root.clone();
    std::fs::create_dir_all(runtime_root.join("leftover")).expect("create staged marker");
    assert!(
        runtime_root.exists(),
        "expected secret runtime root to exist before drop"
    );

    drop(manager);

    assert!(
        !runtime_root.exists(),
        "task manager drop should remove secret runtime root {}",
        runtime_root.display()
    );
}

#[tokio::test]
async fn load_spec_cache_refreshes_after_store_change() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let mut spec = test_task_spec(&manager, "cache-refresh");

    manager
        .persist_spec(&spec)
        .await
        .expect("persist initial spec");

    let loaded = manager.load_spec(spec.id).await.expect("load initial spec");
    assert!(
        matches!(loaded.state, WorkloadPhase::Pending),
        "initial cached load should reflect the pending state"
    );

    let cached_clock = manager
        .local_state
        .workload_spec_cache
        .lock()
        .get(&spec.id)
        .map(|entry| entry.change_clock)
        .expect("cache entry after first load");
    assert_eq!(
        cached_clock,
        manager.core.store.change_clock(),
        "cache entry should be keyed to the current store clock"
    );

    spec.state = WorkloadPhase::Running;
    spec.phase_version = 1;
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist updated spec");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Running),
        "load_spec must not return a stale cached state after a write"
    );
    assert_eq!(
        refreshed.phase_version, 1,
        "load_spec should return the latest persisted phase version"
    );
}

#[tokio::test]
async fn task_value_index_cache_reuses_snapshot_until_store_changes() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let first_spec = test_task_spec(&manager, "cache-index-a");
    manager
        .persist_spec(&first_spec)
        .await
        .expect("persist first task");

    let first = manager
        .load_workload_value_index()
        .await
        .expect("load first cached index");
    let second = manager
        .load_workload_value_index()
        .await
        .expect("load second cached index");
    assert!(
        Arc::ptr_eq(&first, &second),
        "unchanged workload stores should reuse the same decoded snapshot"
    );

    let second_spec = test_task_spec(&manager, "cache-index-b");
    manager
        .persist_spec(&second_spec)
        .await
        .expect("persist second task");

    let refreshed = manager
        .load_workload_value_index()
        .await
        .expect("load refreshed cached index");
    assert!(
        !Arc::ptr_eq(&first, &refreshed),
        "a store write should invalidate the cached decoded snapshot"
    );
    assert_eq!(
        refreshed.len(),
        2,
        "refreshed decoded snapshot should include the new task"
    );
}

/// Writes the local peer scheduling row used by task-manager drain-aware reconciliation tests.
async fn set_local_drain_requested(
    manager: &WorkloadManager,
    drain_requested: bool,
    task_stop_timeout_secs: Option<u32>,
) {
    let local_id = manager.local_node_id;
    let scheduling = PeerSchedulingState {
        schedulable: !drain_requested,
        drain_requested,
        updated_at_unix_ms: 1,
        actor_node_id: local_id,
        reason: drain_requested.then(|| "test drain".to_string()),
        drain_task_stop_timeout_secs: if drain_requested {
            task_stop_timeout_secs
        } else {
            None
        },
    };

    manager
        .core
        .registry
        .upsert_self_scheduling(scheduling)
        .await
        .expect("upsert local drain state");
}

/// Stores one managed local volume spec in the registry so task tests can exercise locality.
async fn create_managed_local_volume(
    manager: &WorkloadManager,
    name: &str,
    binding_mode: VolumeBindingMode,
    bound_node_id: Option<Uuid>,
    bound_node_name: Option<&str>,
) -> VolumeSpecValue {
    let spec = VolumeSpecValue::new(VolumeSpecDraft {
        name: name.to_string(),
        driver: VolumeDriver::Local(LocalVolumeSpec {
            source: LocalVolumeSource::Managed,
            ownership: LocalVolumeOwnership::Daemon,
        }),
        access_mode: VolumeAccessMode::ReadWriteOnce,
        binding_mode,
        reclaim_policy: VolumeReclaimPolicy::Retain,
        requested_bytes: None,
        labels: Vec::new(),
        bound_node_id,
        bound_node_name: bound_node_name.map(str::to_string),
    });
    manager
        .volumes
        .volume_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert managed local volume");
    spec
}

/// Builds one minimal task spec used by cache and store-view tests.
fn test_task_spec(manager: &WorkloadManager, name: &str) -> WorkloadSpec {
    WorkloadSpec {
        id: Uuid::new_v4(),
        name: name.to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    }
}

/// Builds one default resolved execution spec so workload-start tests only
/// override relevant fields.
fn empty_resolved_execution(image: &str) -> ResolvedExecutionSpec {
    ResolvedExecutionSpec {
        image: image.to_string(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        placement: Default::default(),
    }
}

/// Builds one standalone task request that mounts a single resolved volume reference.
fn standalone_volume_task_request(volume: &VolumeSpecValue, target: &str) -> WorkloadStartRequest {
    WorkloadStartRequest {
        name: "volume-task".into(),
        execution: ResolvedExecutionSpec {
            volumes: vec![crate::task::types::TaskVolumeMount {
                volume_id: volume.id,
                volume_name: volume.name.clone(),
                target: target.to_string(),
                read_only: false,
            }],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    }
}

#[tokio::test]
async fn start_workload_reserves_slot_and_records_resources() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    let slot_id = slot_spec.slot_id;
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload(
            "svc",
            "img",
            vec!["/bin/echo".into()],
            200,
            64 * 1_024 * 1_024,
            None,
        )
        .await
        .expect("start container");

    assert_eq!(spec.cpu_millis, 200);
    assert_eq!(spec.memory_bytes, 64 * 1_024 * 1_024);
    assert_eq!(spec.slot_ids, vec![slot_id]);

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
    let limits = mock_cm.limits.lock().await.clone();
    assert_eq!(limits.len(), 1);
    let recorded = limits[0];
    assert_eq!(recorded.memory_bytes, Some((64 * 1_024 * 1_024) as i64));
    assert_eq!(recorded.nano_cpus, Some(200_000_000));
    assert_eq!(recorded.cpu_shares, Some(204));
    assert_eq!(mock_cm.open_stdin.lock().await.as_slice(), &[true]);
}

#[tokio::test]
async fn running_service_task_on_draining_node_marks_failed_instead_of_restart_pending() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;
    set_local_drain_requested(&manager, true, None).await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "svc-api-1".to_string(),
        image: "ghcr.io/demo/api:latest".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: Some(WorkloadRestartPolicy {
            name: WorkloadRestartPolicyKind::Always,
            max_retry_count: None,
        }),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "svc", "api",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager
        .persist_spec(&spec)
        .await
        .expect("persist service task");

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile draining running task");

    let latest = manager
        .inspect_workload(spec.id)
        .await
        .expect("inspect updated task");
    assert_eq!(
        latest.state,
        WorkloadPhase::Failed,
        "draining service task should fail instead of looping back to pending"
    );
    assert!(
        mock_cm.created.lock().await.is_empty(),
        "draining running task should not create a replacement container locally"
    );
}

#[tokio::test]
async fn pending_service_task_on_draining_node_does_not_launch_locally() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;
    set_local_drain_requested(&manager, true, None).await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "svc-api-1".to_string(),
        image: "ghcr.io/demo/api:latest".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: Some(WorkloadRestartPolicy {
            name: WorkloadRestartPolicyKind::Always,
            max_retry_count: None,
        }),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "svc", "api",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager
        .persist_spec(&spec)
        .await
        .expect("persist pending task");

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile draining pending task");

    let latest = manager
        .inspect_workload(spec.id)
        .await
        .expect("inspect updated task");
    assert_eq!(
        latest.state,
        WorkloadPhase::Failed,
        "draining service task should fail instead of launching locally"
    );
    assert!(
        mock_cm.created.lock().await.is_empty(),
        "draining pending task should not create a local instance"
    );
}

#[tokio::test]
async fn pull_image_for_task_retries_and_tracks_phase_progress() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "pull-retry".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    {
        let mut errors = mock_cm.pull_errors.lock().await;
        errors.push_back(RuntimeError::OperationFailed(
            "temporary pull failure #1".into(),
        ));
        errors.push_back(RuntimeError::OperationFailed(
            "temporary pull failure #2".into(),
        ));
    }

    manager
        .pull_image_for_task(
            spec.id,
            &spec.image,
            spec.execution_platform,
            spec.isolation_mode,
            spec.isolation_profile.as_deref(),
        )
        .await
        .expect("pull should succeed after retries");

    let pull_calls = mock_cm.pull_calls.lock().await.clone();
    assert_eq!(pull_calls.len(), 3, "pull should retry twice then succeed");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Pulling),
        "task should remain in pulling phase until create starts"
    );
    assert_eq!(refreshed.phase_progress.as_deref(), Some("3/3"));
    assert_eq!(refreshed.phase_reason.as_deref(), Some("pulling image"));
}

#[tokio::test]
async fn same_state_pulling_progress_stays_local_only() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "pull-local-only".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    manager
        .update_task_phase(
            spec.id,
            WorkloadPhase::Pulling,
            Some("pulling image".to_string()),
            Some("1/3".to_string()),
        )
        .await
        .expect("record initial pulling phase");
    manager
        .update_task_phase(
            spec.id,
            WorkloadPhase::Pulling,
            Some("pull retry backoff".to_string()),
            Some("2/3".to_string()),
        )
        .await
        .expect("record pulling progress locally");

    let first_round_pending = manager
        .flush_dirty_gossip_events()
        .await
        .expect("flush buffered gossip");
    assert!(
        first_round_pending,
        "initial pulling transition should stay dirty for wider fanout coverage"
    );

    let outbound = manager
        .core
        .rx
        .recv()
        .await
        .expect("receive initial pulling transition");
    match outbound {
        Message::Workload {
            event: WorkloadEvent::UpsertSpec(outbound_spec),
            ..
        } => {
            assert_eq!(outbound_spec.id, spec.id);
            assert_eq!(outbound_spec.state, WorkloadPhase::Pulling);
        }
        _ => panic!("unexpected outbound message for pulling transition"),
    }

    let next =
        tokio::time::timeout(std::time::Duration::from_millis(20), manager.core.rx.recv()).await;
    assert!(
        next.is_err(),
        "same-state pulling progress should not enqueue a second logical gossip event"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("reload task after local-only pulling progress");
    assert_eq!(refreshed.state, WorkloadPhase::Pulling);
    assert_eq!(
        refreshed.phase_reason.as_deref(),
        Some("pull retry backoff")
    );
    assert_eq!(refreshed.phase_progress.as_deref(), Some("2/3"));
}

#[tokio::test]
async fn dirty_gossip_flush_retries_latest_event_for_bounded_coverage_rounds() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let spec = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Running,
        2,
        4,
        now.to_rfc3339(),
    );
    manager
        .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(spec.clone())))
        .await
        .expect("buffer running task");

    for round in 0..WORKLOAD_GOSSIP_COVERAGE_ROUNDS {
        let has_pending = manager
            .flush_dirty_gossip_events()
            .await
            .expect("flush dirty gossip round");
        assert_eq!(
            has_pending,
            round + 1 < WORKLOAD_GOSSIP_COVERAGE_ROUNDS,
            "dirty workload retention should expire after the configured coverage rounds"
        );

        let outbound = manager
            .core
            .rx
            .recv()
            .await
            .expect("receive running workload gossip");
        match outbound {
            Message::Workload {
                event: WorkloadEvent::UpsertSpec(outbound_spec),
                ..
            } => {
                assert_eq!(outbound_spec.id, task_id);
                assert_eq!(outbound_spec.state, WorkloadPhase::Running);
            }
            _ => panic!("unexpected outbound message for running task"),
        }
    }

    let next =
        tokio::time::timeout(std::time::Duration::from_millis(20), manager.core.rx.recv()).await;
    assert!(
        next.is_err(),
        "coverage rounds should stop once the bounded dirty budget is exhausted"
    );
}

#[tokio::test]
async fn pull_image_for_task_skips_pull_when_image_exists_locally() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "pull-skip".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    mock_cm
        .present_images
        .lock()
        .await
        .insert(spec.image.clone());

    manager
        .pull_image_for_task(
            spec.id,
            &spec.image,
            spec.execution_platform,
            spec.isolation_mode,
            spec.isolation_profile.as_deref(),
        )
        .await
        .expect("pull should be skipped when image exists locally");

    let pull_calls = mock_cm.pull_calls.lock().await.clone();
    assert!(
        pull_calls.is_empty(),
        "existing local image should not trigger docker pull"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task");
    assert_eq!(refreshed.state, WorkloadPhase::Pending);
    assert!(refreshed.phase_reason.is_none());
    assert!(refreshed.phase_progress.is_none());
}

#[tokio::test]
async fn reconcile_rejects_missing_slot_assignments() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "orphan".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 0,
        memory_bytes: 0,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    let err = manager
        .reconcile_local_task(spec)
        .await
        .expect_err("reconcile should fail without slot assignments");
    assert!(
        err.to_string()
            .contains("missing scheduler slot assignments"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn reconcile_pending_task_reserves_assigned_slots_before_launch() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "slot-guard".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![slot_spec.slot_id],
        slot_id: Some(slot_spec.slot_id),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile pending task");

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == slot_spec.slot_id)
        .expect("slot present");
    match &slot.state {
        SlotState::Reserved(reservation) => {
            assert_eq!(reservation.owner, manager.local_node_id);
            assert_eq!(reservation.task_id, Some(spec.id));
        }
        SlotState::Leased(_) => panic!("slot should be committed before task reconcile completes"),
        SlotState::Free => panic!("slot should be reserved for reconciled task"),
    }

    assert_eq!(mock_cm.created.lock().await.len(), 1);
}

#[tokio::test]
async fn reconcile_uses_latest_persisted_slot_assignment() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let mut stale_argument = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "stale-assignment".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![slot_spec.slot_id],
        slot_id: Some(slot_spec.slot_id),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };

    // Persist a fresher snapshot with missing assignments to emulate a concurrent CRDT update
    // landing after this reconcile request was queued.
    stale_argument.updated_at = Utc::now().to_rfc3339();
    let mut persisted = stale_argument.clone();
    persisted.slot_ids.clear();
    persisted.slot_id = None;
    manager
        .persist_spec(&persisted)
        .await
        .expect("persist latest view");

    let err = manager
        .reconcile_local_task(stale_argument)
        .await
        .expect_err("reconcile should reject missing persisted assignments");
    assert!(
        err.to_string()
            .contains("missing scheduler slot assignments"),
        "unexpected error: {err}"
    );

    assert_eq!(mock_cm.created.lock().await.len(), 0);

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == slot_spec.slot_id)
        .expect("slot present");
    assert!(
        matches!(slot.state, SlotState::Free),
        "slot should remain free when reconcile aborts on stale assignments"
    );
}

#[tokio::test]
async fn update_task_phase_ignores_stale_regression_from_running() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("phase-guard", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));

    let updated = manager
        .update_task_phase(
            spec.id,
            WorkloadPhase::Pulling,
            Some("pulling image".to_string()),
            Some("1/3".to_string()),
        )
        .await
        .expect("update phase should not fail");
    assert!(
        matches!(updated.state, WorkloadPhase::Running),
        "running state should not regress to pulling"
    );
    assert_eq!(
        updated.phase_reason, spec.phase_reason,
        "stale pulling phase should be ignored"
    );
    assert_eq!(
        updated.phase_progress, spec.phase_progress,
        "stale pulling progress should be ignored"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task");
    assert!(matches!(refreshed.state, WorkloadPhase::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);
}

#[tokio::test]
async fn update_task_phase_ignores_stale_regression_from_creating_to_pulling() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let spec = WorkloadSpec {
        id: Uuid::new_v4(),
        name: "phase-order".into(),
        image: "img".into(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Creating,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".into(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 1,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    let updated = manager
        .update_task_phase(
            spec.id,
            WorkloadPhase::Pulling,
            Some("pull retry backoff".to_string()),
            Some("2/3".to_string()),
        )
        .await
        .expect("stale pulling update should not fail");

    assert_eq!(updated.state, WorkloadPhase::Creating);
    assert_eq!(updated.phase_reason, spec.phase_reason);
    assert_eq!(updated.phase_progress, spec.phase_progress);

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("reload task after stale pulling update");
    assert_eq!(refreshed.state, WorkloadPhase::Creating);
    assert!(refreshed.phase_reason.is_none());
    assert!(refreshed.phase_progress.is_none());
}

#[test]
fn compare_task_causality_prefers_epoch_then_phase_version() {
    let now = Utc::now();
    let id = Uuid::new_v4();
    let node_a = Uuid::new_v4();
    let node_b = Uuid::new_v4();

    let current = WorkloadValue::new(WorkloadValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: node_a,
        node_name: "node-a".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 2,
        phase_version: 7,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let lower_epoch = WorkloadValue::new(WorkloadValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(30)).to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: node_b,
        node_name: "node-b".to_string(),
        slot_ids: vec![2],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 1,
        phase_version: 99,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    assert!(
        !should_accept_incoming_workload_value_for_tests(&current, &lower_epoch),
        "lower epoch must not override current assignment"
    );

    let same_epoch_lower_phase = WorkloadValue::new(WorkloadValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(30)).to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: node_b,
        node_name: "node-b".to_string(),
        slot_ids: vec![2],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 2,
        phase_version: 6,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    assert!(
        !should_accept_incoming_workload_value_for_tests(&current, &same_epoch_lower_phase),
        "lower phase version must not override newer lifecycle state"
    );

    let higher_epoch = WorkloadValue::new(WorkloadValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: node_b,
        node_name: "node-b".to_string(),
        slot_ids: vec![2],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 3,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    assert!(
        should_accept_incoming_workload_value_for_tests(&current, &higher_epoch),
        "higher assignment epoch should win regardless of state rank"
    );
}

#[test]
fn select_best_workload_value_ignores_stale_timestamp_when_phase_is_older() {
    let now = Utc::now();
    let id = Uuid::new_v4();
    let node = Uuid::new_v4();

    let running_newer_phase = WorkloadValue::new(WorkloadValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: now.to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: node,
        node_name: "node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 4,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let stale_pending_later_timestamp = WorkloadValue::new(WorkloadValueDraft {
        id,
        name: "task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(45)).to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: node,
        node_name: "node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 3,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let chosen = select_best_workload_value(&[
        stale_pending_later_timestamp.clone(),
        running_newer_phase.clone(),
    ])
    .expect("best value");

    assert_eq!(chosen.state, WorkloadPhase::Running);
    assert_eq!(chosen.phase_version, 4);
}

#[tokio::test]
async fn reconcile_stale_pending_input_does_not_repull_running_task() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let running = manager
        .start_workload(
            "stale-reconcile",
            "img",
            vec![],
            200,
            64 * 1_024 * 1_024,
            None,
        )
        .await
        .expect("start container");
    assert!(matches!(running.state, WorkloadPhase::Running));

    let pulls_before = mock_cm.pull_calls.lock().await.len();

    // Emulate a delayed reconcile worker spawned from an older Pending snapshot.
    let mut stale = running.clone();
    stale.state = WorkloadPhase::Pending;
    stale.phase_reason = None;
    stale.phase_progress = None;

    manager
        .reconcile_local_task(stale)
        .await
        .expect("stale reconcile should short-circuit on running state");

    let pulls_after = mock_cm.pull_calls.lock().await.len();
    assert_eq!(
        pulls_after, pulls_before,
        "stale pending reconcile should not trigger another image pull"
    );

    let refreshed = manager
        .load_spec(running.id)
        .await
        .expect("load refreshed task");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Running),
        "task should remain running after stale reconcile input"
    );
}

#[tokio::test]
async fn reconcile_local_tasks_does_not_duplicate_batch_launch_in_progress() {
    let attachment = Arc::new(BlockingAttachmentProvisioner::default());
    let (manager, scheduler, mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(attachment.clone())).await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "race-net".to_string(),
        description: "batch launch race".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.47.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");
    network_registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            spec.id,
            manager.local_node_id,
            "local-node",
            NetworkPeerState::Ready,
            None,
        ))
        .await
        .expect("upsert local network peer state");

    let request = WorkloadStartRequest {
        name: "launch-race".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let reconcile_manager = manager.clone();
    let attachment_for_wait = attachment.clone();
    let (launch_result, ()) = tokio::join!(
        async move { manager.start_workloads_batch(vec![request]).await },
        async move {
            attachment_for_wait.wait_for_first_attempt().await;
            reconcile_manager
                .reconcile_local_tasks()
                .await
                .expect("reconcile during launch should succeed");
            attachment_for_wait.release_first_attempt();
        }
    );

    let mut specs = launch_result.expect("batch launch should complete");
    assert_eq!(specs.len(), 1, "batch launch should still return one task");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(
        created.len(),
        1,
        "reconcile must not create a duplicate container while the batch launch is in progress"
    );

    let task_spec = specs.remove(0);
    assert_eq!(task_spec.networks, vec![spec.id]);
}

#[tokio::test]
async fn reconcile_running_task_restarts_when_container_is_missing() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should restart missing runtime");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 2, "task runtime should be recreated");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Running),
        "task should converge back to running after restart"
    );
}

#[tokio::test]
async fn reconcile_running_task_marks_exited_when_container_exits_without_restart_policy() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));

    let instance_id = mock_cm
        .created
        .lock()
        .await
        .first()
        .cloned()
        .expect("instance id");

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.insert(
            instance_id.clone(),
            runtime_info_with_state(
                &instance_id,
                &format!("mantissa-{}", spec.id),
                "img",
                "Exited",
                "exited",
                false,
                0,
                Some(255),
                Some("exec format error"),
            ),
        );
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should mark terminal exit as exited");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Exited(255)),
        "task should transition to exited after terminal container exit"
    );
    assert_eq!(
        mock_cm.created.lock().await.len(),
        1,
        "task runtime should not be recreated for terminal exit without restart policy"
    );

    let slot_id = *spec.slot_ids.first().expect("task slot assignment");
    let snapshot = scheduler.snapshot().await.expect("scheduler snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|entry| entry.slot_id == slot_id)
        .expect("slot entry");
    assert!(
        matches!(slot.state, SlotState::Free),
        "exited task should release its reserved slot"
    );
}

#[tokio::test]
async fn reconcile_running_task_does_not_overwrite_newer_failed_state() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    let mut failed = manager
        .load_spec(spec.id)
        .await
        .expect("load running task before failure");
    failed.phase_version = failed.phase_version.saturating_add(1);
    failed.state = WorkloadPhase::Failed;
    failed.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&failed)
        .await
        .expect("persist newer failed task state");

    let mut stale_running = spec.clone();
    stale_running.state = WorkloadPhase::Running;

    let short_circuit = manager
        .reconcile_recorded_running_task(&mut stale_running)
        .await
        .expect("reconcile stale running task");
    assert!(
        short_circuit,
        "reconcile should short-circuit after detecting newer terminal state"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed task after stale reconcile");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Failed),
        "newer failed state should not be overwritten by stale running reconcile"
    );
    assert_eq!(
        mock_cm.created.lock().await.len(),
        1,
        "stale reconcile should not recreate runtime after terminal failure"
    );
}

#[tokio::test]
async fn reconcile_running_task_keeps_running_when_list_finds_container() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }
    {
        let mut listed = mock_cm.listed.lock().await;
        listed.clear();
        listed.push(running_runtime_info(
            "container-0",
            &format!("mantissa-{}", spec.id),
            "img",
        ));
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should keep running task");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1, "task runtime should not be recreated");

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Running),
        "task should remain running when runtime listing confirms container"
    );
}

#[tokio::test]
async fn reconcile_running_task_executes_liveness_probe_once_per_interval() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let mut probed = manager.load_spec(spec.id).await.expect("load running task");
    probed.liveness = Some(WorkloadLivenessProbe {
        kind: WorkloadLivenessProbeKind::Exec,
        command: vec!["/bin/check".to_string(), "--ready".to_string()],
        port: 0,
        path: None,
        interval_ms: 60_000,
        timeout_ms: 750,
        failure_threshold: 2,
        start_period_ms: 0,
    });
    manager
        .persist_spec(&probed)
        .await
        .expect("persist liveness probe");

    let mut first = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task for first probe");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut first)
        .await
        .expect("reconcile first liveness probe");
    assert!(
        short_circuit,
        "healthy liveness probe should keep task running"
    );

    let exec_calls = mock_cm.exec_calls.lock().await.clone();
    assert_eq!(
        exec_calls.len(),
        1,
        "first reconcile should execute probe once"
    );
    assert_eq!(exec_calls[0].0, "container-0");
    assert_eq!(
        exec_calls[0].1,
        vec!["/bin/check".to_string(), "--ready".to_string()]
    );
    assert_eq!(exec_calls[0].2, Some(std::time::Duration::from_millis(750)));

    let entry = manager
        .local_state
        .liveness_probes
        .lock()
        .await
        .get(&spec.id)
        .copied()
        .expect("cached liveness entry");
    assert_eq!(entry.launch_attempt, first.launch_attempt);
    assert_eq!(entry.consecutive_failures, 0);

    let mut second = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task for cached probe");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut second)
        .await
        .expect("reconcile cached liveness probe");
    assert!(
        short_circuit,
        "cached healthy probe should keep task running"
    );
    assert_eq!(
        mock_cm.exec_calls.lock().await.len(),
        1,
        "probe interval cache should suppress an immediate second exec"
    );
}

#[tokio::test]
async fn reconcile_running_task_executes_http_liveness_probe_without_container_exec() {
    let (manager, scheduler, mock_cm, network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http liveness listener");
    let port = listener.local_addr().expect("listener addr").port();
    let network_id = Uuid::new_v4();
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut stream, _) = listener.accept().await.expect("accept http probe");
        let mut buf = [0u8; 256];
        let _ = stream.read(&mut buf).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .expect("write probe response");
    });

    network_registry
        .upsert_attachment(NetworkAttachmentValue::new(NetworkAttachmentDraft {
            id: crate::network::types::compute_network_attachment_id(spec.id, network_id),
            task_id: spec.id,
            node_id: manager.local_node_id,
            instance_id: "container-0".to_string(),
            network_id,
            task_updated_at: Some(Utc::now().to_rfc3339()),
            requested_ip: Some("127.0.0.1".to_string()),
            assigned_ip: Some("127.0.0.1".to_string()),
            mac: Some("02:11:22:33:44:55".to_string()),
            state: NetworkAttachmentState::Ready,
            error: None,
            traffic_published: true,
            service_name: None,
            template_name: None,
        }))
        .await
        .expect("insert local attachment target");

    let mut probed = manager.load_spec(spec.id).await.expect("load running task");
    probed.liveness = Some(WorkloadLivenessProbe {
        kind: WorkloadLivenessProbeKind::Http,
        command: Vec::new(),
        port,
        path: Some("/".to_string()),
        interval_ms: 60_000,
        timeout_ms: 1_000,
        failure_threshold: 2,
        start_period_ms: 0,
    });
    manager
        .persist_spec(&probed)
        .await
        .expect("persist http liveness probe");

    let mut working = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut working)
        .await
        .expect("reconcile http liveness probe");
    assert!(
        short_circuit,
        "healthy http liveness probe should keep task running"
    );
    assert!(
        mock_cm.exec_calls.lock().await.is_empty(),
        "http liveness should not use container exec"
    );
    server.await.expect("join http probe server");
}

#[tokio::test]
async fn reconcile_running_task_executes_tcp_liveness_probe_without_container_exec() {
    let (manager, scheduler, mock_cm, network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp liveness listener");
    let port = listener.local_addr().expect("listener addr").port();
    let network_id = Uuid::new_v4();

    network_registry
        .upsert_attachment(NetworkAttachmentValue::new(NetworkAttachmentDraft {
            id: crate::network::types::compute_network_attachment_id(spec.id, network_id),
            task_id: spec.id,
            node_id: manager.local_node_id,
            instance_id: "container-0".to_string(),
            network_id,
            task_updated_at: Some(Utc::now().to_rfc3339()),
            requested_ip: Some("127.0.0.1".to_string()),
            assigned_ip: Some("127.0.0.1".to_string()),
            mac: Some("02:11:22:33:44:66".to_string()),
            state: NetworkAttachmentState::Ready,
            error: None,
            traffic_published: true,
            service_name: None,
            template_name: None,
        }))
        .await
        .expect("insert local attachment target");

    let mut probed = manager.load_spec(spec.id).await.expect("load running task");
    probed.liveness = Some(WorkloadLivenessProbe {
        kind: WorkloadLivenessProbeKind::Tcp,
        command: Vec::new(),
        port,
        path: None,
        interval_ms: 60_000,
        timeout_ms: 1_000,
        failure_threshold: 2,
        start_period_ms: 0,
    });
    manager
        .persist_spec(&probed)
        .await
        .expect("persist tcp liveness probe");

    let mut working = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut working)
        .await
        .expect("reconcile tcp liveness probe");
    assert!(
        short_circuit,
        "healthy tcp liveness probe should keep task running"
    );
    assert!(
        mock_cm.exec_calls.lock().await.is_empty(),
        "tcp liveness should not use container exec"
    );
    drop(listener);
}

#[tokio::test]
async fn reconcile_running_task_skips_liveness_probe_during_start_period() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let mut probed = manager.load_spec(spec.id).await.expect("load running task");
    probed.updated_at = Utc::now().to_rfc3339();
    probed.liveness = Some(WorkloadLivenessProbe {
        kind: WorkloadLivenessProbeKind::Exec,
        command: vec!["/bin/check".to_string()],
        port: 0,
        path: None,
        interval_ms: 0,
        timeout_ms: 500,
        failure_threshold: 1,
        start_period_ms: 60_000,
    });
    manager
        .persist_spec(&probed)
        .await
        .expect("persist liveness probe");

    let mut working = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut working)
        .await
        .expect("reconcile running task inside start period");
    assert!(short_circuit, "start period should keep task running");
    assert!(
        mock_cm.exec_calls.lock().await.is_empty(),
        "start period should suppress local exec probes"
    );
    assert!(
        manager
            .local_state
            .liveness_probes
            .lock()
            .await
            .get(&spec.id)
            .is_none(),
        "start period should not create a cached probe entry before the first probe runs"
    );
}

#[tokio::test]
async fn reconcile_running_task_restarts_after_liveness_threshold_failures() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let mut probed = manager.load_spec(spec.id).await.expect("load running task");
    probed.liveness = Some(WorkloadLivenessProbe {
        kind: WorkloadLivenessProbeKind::Exec,
        command: vec!["/bin/check".to_string()],
        port: 0,
        path: None,
        interval_ms: 0,
        timeout_ms: 500,
        failure_threshold: 2,
        start_period_ms: 0,
    });
    manager
        .persist_spec(&probed)
        .await
        .expect("persist liveness probe");

    {
        let mut results = mock_cm.exec_results.lock().await;
        results.push_back(Ok(RuntimeExecResult { exit_code: Some(7) }));
        results.push_back(Ok(RuntimeExecResult { exit_code: Some(7) }));
    }

    let mut first = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task for first failure");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut first)
        .await
        .expect("reconcile first liveness failure");
    assert!(
        short_circuit,
        "task should remain running until the failure threshold is reached"
    );

    let after_first = manager
        .load_spec(spec.id)
        .await
        .expect("load task after first liveness failure");
    assert_eq!(after_first.state, WorkloadPhase::Running);
    assert_eq!(
        manager
            .local_state
            .liveness_probes
            .lock()
            .await
            .get(&spec.id)
            .map(|entry| entry.consecutive_failures),
        Some(1),
        "first failure should be cached for threshold accounting"
    );

    let mut second = manager
        .load_spec(spec.id)
        .await
        .expect("reload running task for threshold failure");
    let short_circuit = manager
        .reconcile_recorded_running_task(&mut second)
        .await
        .expect("reconcile threshold liveness failure");
    assert!(
        !short_circuit,
        "threshold failure should hand control back to the pending restart path"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load task after threshold failure");
    assert_eq!(refreshed.state, WorkloadPhase::Pending);
    assert_eq!(
        refreshed.phase_reason.as_deref(),
        Some("liveness probe exited with status code 7")
    );
    assert_eq!(
        refreshed.last_terminal_observed_launch,
        Some(refreshed.launch_attempt),
        "threshold failure should record a terminal observation for the current launch"
    );
    assert!(
        manager
            .local_state
            .local_instances
            .lock()
            .await
            .get(&spec.id)
            .is_none(),
        "threshold failure should evict the cached local instance id"
    );
    assert!(
        manager
            .local_state
            .liveness_probes
            .lock()
            .await
            .get(&spec.id)
            .is_none(),
        "threshold failure should clear cached liveness accounting"
    );
    assert_eq!(
        mock_cm.stopped.lock().await.as_slice(),
        ["container-0"],
        "threshold failure should stop the unhealthy container before restart"
    );
}

#[tokio::test]
async fn reconcile_running_task_retries_create_after_stale_name_conflict() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));
    assert_eq!(mock_cm.created.lock().await.len(), 1);

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }
    {
        let mut errors = mock_cm.create_errors.lock().await;
        errors.push_back(RuntimeError::backend(Some(409), "name already in use"));
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should recover from stale name conflict");

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(
        created.len(),
        2,
        "task runtime should be recreated after retry"
    );

    let refreshed = manager
        .load_spec(spec.id)
        .await
        .expect("load refreshed spec");
    assert!(
        matches!(refreshed.state, WorkloadPhase::Running),
        "task should converge back to running after retry"
    );
}

#[tokio::test]
async fn reconcile_local_tasks_uses_runtime_inventory_for_running_tasks() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    assert!(matches!(spec.state, WorkloadPhase::Running));

    {
        let mut listed = mock_cm.listed.lock().await;
        listed.clear();
        listed.push(running_runtime_info(
            "runtime-container-1",
            &format!("mantissa-{}", spec.id),
            "img",
        ));
    }
    mock_cm.inspect_calls.lock().await.clear();

    manager
        .reconcile_local_tasks()
        .await
        .expect("reconcile local tasks");

    let inspect_calls = mock_cm.inspect_calls.lock().await.clone();
    assert!(
        inspect_calls.is_empty(),
        "running tasks present in runtime inventory should not trigger inspect"
    );
}

#[tokio::test]
async fn reconcile_local_slot_reservations_releases_stale_local_slots() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_a = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    let slot_b = SlotSpec::new(2, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_a.clone(), slot_b.clone()])
        .await
        .expect("init slots");

    let running = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    assert_eq!(
        running.slot_ids.len(),
        1,
        "expected one active slot reservation"
    );
    let active_slot = running.slot_ids[0];
    let stale_slot = if active_slot == slot_a.slot_id {
        slot_b.slot_id
    } else {
        slot_a.slot_id
    };

    let before = scheduler
        .snapshot()
        .await
        .expect("snapshot before stale reserve");
    scheduler
        .reserve_slots(
            before.version,
            vec![SlotReservationRequest {
                slot_id: stale_slot,
                owner: manager.local_node_id,
                task_id: Some(Uuid::new_v4()),
            }],
        )
        .await
        .expect("reserve stale slot");

    let snapshot_with_stale = scheduler
        .snapshot()
        .await
        .expect("snapshot with stale slot");
    let local_reserved_before = snapshot_with_stale
        .slots
        .iter()
        .filter(|slot| {
            matches!(
                slot.state,
                SlotState::Reserved(ref reservation) if reservation.owner == manager.local_node_id
            )
        })
        .count();
    assert_eq!(
        local_reserved_before, 2,
        "expected stale extra local reservation"
    );

    manager
        .reconcile_local_slot_reservations()
        .await
        .expect("reconcile local slot reservations");

    let after = scheduler
        .snapshot()
        .await
        .expect("snapshot after reconcile");
    let local_reserved_after = after
        .slots
        .iter()
        .filter(|slot| {
            matches!(
                slot.state,
                SlotState::Reserved(ref reservation) if reservation.owner == manager.local_node_id
            )
        })
        .count();
    assert_eq!(
        local_reserved_after, 1,
        "stale local reservation should be released"
    );

    let stale_state = after
        .slots
        .iter()
        .find(|slot| slot.slot_id == stale_slot)
        .map(|slot| slot.state.clone())
        .expect("stale slot present");
    assert!(matches!(stale_state, SlotState::Free));
}

#[tokio::test]
async fn reconcile_local_slot_reservations_demotes_conflicting_local_task_claims() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let winner = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start winner container");
    let contested_slot = winner.slot_ids[0];

    let now = Utc::now().to_rfc3339();
    let loser_id = Uuid::new_v4();
    let loser = WorkloadSpec {
        id: loser_id,
        name: "svc-loser".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "node".to_string(),
        slot_ids: vec![contested_slot],
        slot_id: Some(contested_slot),
        cpu_millis: 200,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 3,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&loser).await.expect("persist loser");

    manager
        .reconcile_local_slot_reservations()
        .await
        .expect("reconcile local slot reservations");

    if let Ok(updated_loser) = manager.load_spec(loser_id).await {
        assert!(
            matches!(
                updated_loser.state,
                WorkloadPhase::Stopping | WorkloadPhase::Stopped
            ),
            "conflicting local task should be demoted for draining"
        );
        assert!(
            updated_loser.slot_ids.is_empty(),
            "demoted conflicting task should no longer claim scheduler slots"
        );
        assert!(
            updated_loser.gpu_device_ids.is_empty(),
            "demoted conflicting task should no longer claim scheduler gpus"
        );
    }

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let state = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == contested_slot)
        .map(|slot| slot.state.clone())
        .expect("contested slot present");
    match state {
        SlotState::Reserved(reservation) => {
            assert_eq!(reservation.owner, manager.local_node_id);
            assert_eq!(
                reservation.task_id,
                Some(winner.id),
                "slot reservation should remain attached to deterministic winner"
            );
        }
        SlotState::Leased(_) => {
            panic!("conflicted slot should not remain in a lease-only state after reconcile")
        }
        SlotState::Free => panic!("conflicted slot should remain reserved by winner"),
    }
}

#[tokio::test]
async fn start_workload_reserves_multiple_slots_when_needed() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_a = SlotSpec::new(1, SlotCapacity::new(200, 64 * 1_024 * 1_024, 0));
    let slot_b = SlotSpec::new(2, SlotCapacity::new(200, 64 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_a.clone(), slot_b.clone()])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 400, 128 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    assert_eq!(spec.slot_ids.len(), 2);
    assert!(spec.slot_ids.contains(&slot_a.slot_id));
    assert!(spec.slot_ids.contains(&slot_b.slot_id));

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
}

#[tokio::test]
async fn request_task_stop_releases_slot_and_clears_resources() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop");
    assert!(matches!(requested.state, WorkloadPhase::Stopping));

    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    assert!(manager.inspect_workload(spec.id).await.is_err());

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
    let stopped_list = mock_cm.stopped.lock().await.clone();
    assert_eq!(stopped_list.len(), 1);
}

#[tokio::test]
async fn request_task_stop_uses_task_termination_grace_period() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.termination_grace_period_secs = Some(42);
    manager.persist_spec(&spec).await.expect("persist update");

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    let stop_timeout = stop_timeouts[0].expect("stop timeout");
    assert!(stop_timeout <= std::time::Duration::from_secs(42));
    assert!(stop_timeout > std::time::Duration::from_secs(41));
}

#[tokio::test]
async fn request_task_stop_uses_drain_task_stop_timeout_override() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.termination_grace_period_secs = Some(42);
    manager.persist_spec(&spec).await.expect("persist update");
    set_local_drain_requested(&manager, true, Some(3)).await;

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    let stop_timeout = stop_timeouts[0].expect("stop timeout");
    assert!(stop_timeout <= std::time::Duration::from_secs(3));
    assert!(stop_timeout > std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn request_task_stop_runs_pre_stop_hook_with_shared_shutdown_budget() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.termination_grace_period_secs = Some(5);
    spec.pre_stop_command = Some(vec!["/bin/sh".into(), "-c".into(), "sleep 1".into()]);
    manager.persist_spec(&spec).await.expect("persist update");

    *mock_cm.exec_delay.lock().await = Some(std::time::Duration::from_secs(2));

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let exec_calls = mock_cm.exec_calls.lock().await.clone();
    assert_eq!(exec_calls.len(), 1);
    assert_eq!(
        exec_calls[0].1,
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 1".to_string()
        ]
    );
    let exec_timeout = exec_calls[0].2.expect("pre-stop timeout");
    assert!(exec_timeout <= std::time::Duration::from_secs(5));
    assert!(exec_timeout > std::time::Duration::from_secs(4));

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    let stop_timeout = stop_timeouts[0].expect("stop timeout");
    assert!(stop_timeout < std::time::Duration::from_secs(5));
    assert!(stop_timeout >= std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn request_task_stop_continues_after_pre_stop_hook_failure() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    spec.pre_stop_command = Some(vec!["/bin/false".into()]);
    manager.persist_spec(&spec).await.expect("persist update");

    mock_cm
        .exec_results
        .lock()
        .await
        .push_back(Err(RuntimeError::OperationFailed("boom".into())));

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");

    let exec_calls = mock_cm.exec_calls.lock().await.clone();
    assert_eq!(exec_calls.len(), 1);
    let stopped = mock_cm.stopped.lock().await.clone();
    assert_eq!(stopped.len(), 1);
}

#[tokio::test]
async fn reconcile_inventory_uses_drain_stop_timeout_for_unowned_runtime() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    set_local_drain_requested(&manager, true, Some(4)).await;

    {
        let mut listed = mock_cm.listed.lock().await;
        listed.clear();
        listed.push(running_runtime_info(
            &format!("runtime-{}", spec.id),
            &format!("mantissa-{}", spec.id),
            "img",
        ));
    }

    let mut remote_spec = spec.clone();
    remote_spec.node_id = Uuid::new_v4();
    remote_spec.node_name = "other-node".to_string();
    remote_spec.updated_at = (Utc::now() - chrono::Duration::seconds(10)).to_rfc3339();
    manager
        .persist_spec(&remote_spec)
        .await
        .expect("persist remote ownership");

    manager
        .reconcile_local_runtime_inventory()
        .await
        .expect("reconcile inventory");

    let stop_timeouts = mock_cm.stop_timeouts.lock().await.clone();
    assert_eq!(stop_timeouts.len(), 1);
    assert_eq!(stop_timeouts[0], Some(std::time::Duration::from_secs(4)));
}

#[tokio::test]
async fn request_task_stop_uses_container_name_when_cache_missing() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .local_state
        .local_instances
        .lock()
        .await
        .remove(&spec.id);

    spec.state = WorkloadPhase::Running;
    manager.persist_spec(&spec).await.expect("persist update");

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop");
    assert!(matches!(requested.state, WorkloadPhase::Stopping));
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile requested stop");
}

#[tokio::test]
async fn request_task_stop_is_idempotent_while_stopping() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.phase_version = spec.phase_version.saturating_add(1);
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    mock_cm.stopped.lock().await.clear();

    let current = manager
        .request_workload_stop(spec.id)
        .await
        .expect("idempotent stop");
    assert!(matches!(current.state, WorkloadPhase::Stopping));
    assert!(
        mock_cm.stopped.lock().await.is_empty(),
        "stop should not invoke runtime stop again when task is already stopping"
    );
}

#[tokio::test]
async fn reconcile_requested_stop_removes_instance_less_stopping_task() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    let slot_id = slot_spec.slot_id;
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");
    let persisted = manager
        .load_spec(spec.id)
        .await
        .expect("load persisted stopping task");
    assert!(
        matches!(persisted.state, WorkloadPhase::Stopping),
        "manual test setup should persist a stopping task before explicit cleanup"
    );

    manager
        .local_state
        .local_instances
        .lock()
        .await
        .remove(&spec.id);
    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    manager
        .reconcile_requested_stop(spec.id)
        .await
        .expect("finish stop cleanup");

    assert!(
        manager
            .core
            .store
            .get_snapshot(&UuidKey::from(spec.id))
            .expect("raw task snapshot after explicit stop cleanup")
            .is_none(),
        "instance-less stopping task should be removed from the workload store by explicit stop cleanup"
    );
    assert!(
        manager.load_spec(spec.id).await.is_err(),
        "instance-less stopping task should be removed by explicit stop cleanup"
    );
    let snapshot = scheduler.snapshot().await.expect("scheduler snapshot");
    let slot = snapshot
        .slots
        .iter()
        .find(|slot| slot.slot_id == slot_id)
        .expect("slot present after cleanup");
    assert!(
        matches!(slot.state, SlotState::Free),
        "instance-less stopping cleanup should also release the reserved slot"
    );
}

#[tokio::test]
async fn reconcile_requested_stop_treats_non_running_instance_as_instance_less() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.phase_version = spec.phase_version.saturating_add(1);
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    manager
        .local_state
        .local_instances
        .lock()
        .await
        .remove(&spec.id);

    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    manager
        .reconcile_requested_stop(spec.id)
        .await
        .expect("finish stop cleanup");

    assert!(
        manager.load_spec(spec.id).await.is_err(),
        "non-running inspected containers should not block task-row removal"
    );
}

#[tokio::test]
async fn request_task_stop_only_updates_replicated_state() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    mock_cm.stopped.lock().await.clear();

    let requested = manager
        .request_workload_stop(spec.id)
        .await
        .expect("request stop transition");
    assert!(matches!(requested.state, WorkloadPhase::Stopping));
    assert!(
        mock_cm.stopped.lock().await.is_empty(),
        "request_task_stop should not invoke runtime stop directly"
    );

    let persisted = manager.load_spec(spec.id).await.expect("load spec");
    assert!(matches!(persisted.state, WorkloadPhase::Stopping));
}

#[tokio::test]
async fn reconcile_stopping_task_stops_immediately() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    mock_cm.stopped.lock().await.clear();

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile stopping task");
    assert_eq!(
        mock_cm.stopped.lock().await.len(),
        1,
        "reconcile should execute stop immediately for stopping tasks"
    );
}

#[tokio::test]
async fn reconcile_stopping_task_retries_after_grace_window() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.updated_at = (Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stale stopping state");

    mock_cm.stopped.lock().await.clear();

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile stale stopping task");
    assert_eq!(
        mock_cm.stopped.lock().await.len(),
        1,
        "reconcile should retry stop once stopping state is stale"
    );
}

#[tokio::test]
async fn reconcile_stopping_task_serializes_duplicate_stop_attempts() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.updated_at = (Utc::now() - chrono::Duration::seconds(30)).to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    {
        let mut delay = mock_cm.stop_delay.lock().await;
        *delay = Some(std::time::Duration::from_millis(200));
    }
    mock_cm.stopped.lock().await.clear();

    let (first, second) = tokio::join!(
        manager.reconcile_local_task(spec.clone()),
        manager.reconcile_local_task(spec.clone())
    );
    first.expect("first reconcile stop attempt");
    second.expect("second reconcile stop attempt");

    assert_eq!(
        mock_cm.stopped.lock().await.len(),
        1,
        "duplicate stop attempts should collapse into a single runtime stop call"
    );
}

#[tokio::test]
async fn reconcile_stopping_task_retries_after_stop_timeout() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.termination_grace_period_secs = Some(1);
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    *mock_cm.stop_delay.lock().await = Some(std::time::Duration::from_secs(2));

    let err = manager
        .reconcile_local_task(spec.clone())
        .await
        .expect_err("slow runtime stop should time out");
    assert!(
        err.to_string().contains("runtime stop timed out"),
        "expected bounded stop timeout, got: {err:#}"
    );

    let persisted = manager
        .load_spec(spec.id)
        .await
        .expect("load stopping task");
    assert!(matches!(persisted.state, WorkloadPhase::Stopping));

    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    *mock_cm.stop_delay.lock().await = None;
    manager
        .reconcile_local_task(persisted)
        .await
        .expect("retry stop after timeout");

    if let Ok(remaining) = manager.load_spec(spec.id).await {
        panic!(
            "task should be removed after stop retry succeeds, but remained in state {:?}",
            remaining.state
        );
    }
}

#[tokio::test]
async fn reconcile_stopping_task_retries_after_remove_timeout() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut spec = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    spec.state = WorkloadPhase::Stopping;
    spec.termination_grace_period_secs = Some(1);
    spec.updated_at = Utc::now().to_rfc3339();
    manager
        .persist_spec(&spec)
        .await
        .expect("persist stopping state");

    *mock_cm.remove_delay.lock().await = Some(std::time::Duration::from_secs(2));

    let err = manager
        .reconcile_local_task(spec.clone())
        .await
        .expect_err("slow runtime remove should time out");
    assert!(
        err.to_string().contains("runtime remove timed out"),
        "expected bounded remove timeout, got: {err:#}"
    );

    let persisted = manager
        .load_spec(spec.id)
        .await
        .expect("load stopping task");
    assert!(matches!(persisted.state, WorkloadPhase::Stopping));

    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    *mock_cm.remove_delay.lock().await = None;
    manager
        .reconcile_local_task(persisted)
        .await
        .expect("retry remove after timeout");

    if let Ok(remaining) = manager.load_spec(spec.id).await {
        panic!(
            "task should be removed after remove retry succeeds, but remained in state {:?}",
            remaining.state
        );
    }
}

#[tokio::test]
async fn list_tasks_respects_filters() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let running = manager
        .start_workload("running", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start running");

    let requested = manager
        .request_workload_stop(running.id)
        .await
        .expect("request stop running");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile stop running");

    let filter_running = TaskStateFilter::new([TaskStateKind::Running]);
    let running_tasks = manager
        .list_workloads(&filter_running)
        .await
        .expect("list running");
    assert!(running_tasks.is_empty());

    let filter_stopped = TaskStateFilter::new([TaskStateKind::Stopped]);
    let stopped_tasks = manager
        .list_workloads(&filter_stopped)
        .await
        .expect("list stopped");
    assert!(stopped_tasks.is_empty());

    let all_tasks = manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list all");
    assert!(all_tasks.is_empty());
}

#[tokio::test]
async fn resolve_task_id_accepts_unique_short_prefix() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let id = Uuid::parse_str("956bc5ba-0f2c-4d3f-8a07-fd9f1f72b8c1").expect("uuid");
    let spec = build_remote_task_spec(
        id,
        Uuid::new_v4(),
        WorkloadPhase::Running,
        1,
        1,
        Utc::now().to_rfc3339(),
    );
    manager.persist_spec(&spec).await.expect("persist task");

    let resolved = manager
        .resolve_workload_id("956bc5ba")
        .await
        .expect("resolve prefix");

    assert_eq!(resolved, id);
}

#[tokio::test]
async fn resolve_task_id_accepts_compact_prefix_across_hyphen_boundary() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let id = Uuid::parse_str("956bc5ba-0f2c-4d3f-8a07-fd9f1f72b8c1").expect("uuid");
    let spec = build_remote_task_spec(
        id,
        Uuid::new_v4(),
        WorkloadPhase::Running,
        1,
        1,
        Utc::now().to_rfc3339(),
    );
    manager.persist_spec(&spec).await.expect("persist task");

    let resolved = manager
        .resolve_workload_id("956bc5ba0f2c")
        .await
        .expect("resolve prefix");

    assert_eq!(resolved, id);
}

#[tokio::test]
async fn resolve_task_id_rejects_ambiguous_prefix() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let a = Uuid::parse_str("956bc5ba-0f2c-4d3f-8a07-fd9f1f72b8c1").expect("uuid");
    let b = Uuid::parse_str("956bc5ba-11aa-4d3f-8a07-fd9f1f72b8c2").expect("uuid");
    let now = Utc::now().to_rfc3339();
    let first =
        build_remote_task_spec(a, Uuid::new_v4(), WorkloadPhase::Running, 1, 1, now.clone());
    let second = build_remote_task_spec(b, Uuid::new_v4(), WorkloadPhase::Running, 1, 1, now);
    manager.persist_spec(&first).await.expect("persist first");
    manager.persist_spec(&second).await.expect("persist second");

    let error = manager
        .resolve_workload_id("956bc5ba")
        .await
        .expect_err("ambiguous prefix should fail");

    assert!(error.to_string().contains("ambiguous"));
}

#[tokio::test]
async fn start_workload_fails_when_no_matching_slot() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    // Seed an initialized scheduler snapshot whose slot capacity cannot satisfy the request.
    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(100, 32 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let result = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn start_tasks_batch_reserves_every_slot() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slots: Vec<_> = (1..=3)
        .map(|id| SlotSpec::new(id, SlotCapacity::new(200, 64 * 1_024 * 1_024, 0)))
        .collect();
    scheduler.init_slots(slots).await.expect("init slots");

    let specs = manager
        .start_workloads_batch(vec![
            WorkloadStartRequest {
                name: "svc-a".into(),
                execution: empty_resolved_execution("img"),
                execution_platform: ExecutionPlatform::Oci,
                isolation_mode: crate::workload::model::IsolationMode::Standard,
                isolation_profile: None,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                owner: None,
                target_node: None,
            },
            WorkloadStartRequest {
                name: "svc-b".into(),
                execution: empty_resolved_execution("img"),
                execution_platform: ExecutionPlatform::Oci,
                isolation_mode: crate::workload::model::IsolationMode::Standard,
                isolation_profile: None,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                owner: None,
                target_node: None,
            },
        ])
        .await
        .expect("start batch");

    assert_eq!(specs.len(), 2);
    assert!(specs.iter().all(|spec| spec.slot_ids.len() == 1));

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 2);
}

#[tokio::test]
async fn start_tasks_batch_respects_existing_reservations() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(400, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec.clone()])
        .await
        .expect("init slots");

    let task_id = Uuid::new_v4();
    scheduler
        .reserve_slots(
            scheduler.snapshot().await.expect("snapshot").version,
            vec![SlotReservationRequest {
                slot_id: slot_spec.slot_id,
                owner: manager.local_node_id,
                task_id: Some(task_id),
            }],
        )
        .await
        .expect("reserve slot");

    let specs = manager
        .start_workloads_batch(vec![WorkloadStartRequest {
            name: "svc-a".into(),
            execution: empty_resolved_execution("img"),
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: Some(task_id),
            slot_ids: vec![slot_spec.slot_id],
            owner: None,
            target_node: None,
        }])
        .await
        .expect("start with pre-reserved slot");

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].slot_ids, vec![slot_spec.slot_id]);

    let created = mock_cm.created.lock().await.clone();
    assert_eq!(created.len(), 1);
}

#[tokio::test]
async fn task_owned_locally_detects_remote_entries() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let local_spec = manager
        .start_workload("local", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start local task");

    assert!(
        manager
            .workload_owned_locally(local_spec.id)
            .await
            .expect("local ownership check")
    );

    let remote_id = Uuid::new_v4();
    let remote_value = WorkloadValue::new(WorkloadValueDraft {
        id: remote_id,
        name: "remote".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec![],
        tty: false,
        node_id: Uuid::new_v4(),
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let store = manager.core.store.clone();
    store
        .upsert(&UuidKey::from(remote_id), remote_value)
        .await
        .expect("insert remote task value");

    assert!(
        !manager
            .workload_owned_locally(remote_id)
            .await
            .expect("remote ownership check")
    );
}

#[tokio::test]
async fn stream_local_task_logs_forwards_frames_and_options() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let spec = WorkloadSpec {
        id: task_id,
        name: "loggable".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Failed,
        phase_reason: Some("crashed".to_string()),
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    let instance_name = format!("mantissa-{task_id}");
    mock_cm.log_frames.lock().await.insert(
        instance_name.clone(),
        vec![
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdOut,
                message: b"hello stdout\n".to_vec(),
            },
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdErr,
                message: b"hello stderr\n".to_vec(),
            },
        ],
    );
    mock_cm.inspect.lock().await.insert(
        instance_name.clone(),
        RuntimeInfo {
            id: instance_name.clone(),
            name: instance_name.clone(),
            status: "Up".to_string(),
            state: RuntimeStateInfo {
                raw_status: Some("running".to_string()),
                running: Some(true),
                pid: Some(1000),
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let options = RuntimeLogsOptions {
        follow: true,
        stdout: true,
        stderr: true,
        timestamps: true,
        tail: "25".to_string(),
    };
    let (logs_tx, mut logs_rx) = tokio::sync::mpsc::channel(8);
    manager
        .stream_local_workload_logs(task_id, &options, logs_tx)
        .await
        .expect("stream local logs");

    let mut frames = Vec::new();
    while let Some(frame) = logs_rx.recv().await {
        frames.push(frame);
    }

    let log_calls = mock_cm.log_calls.lock().await.clone();
    assert_eq!(log_calls, vec![(instance_name, options.clone())]);
    assert_eq!(
        frames,
        vec![
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdOut,
                message: b"hello stdout\n".to_vec(),
            },
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdErr,
                message: b"hello stderr\n".to_vec(),
            },
        ]
    );
}

#[tokio::test]
async fn attach_local_task_forwards_input_output_and_options() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let spec = WorkloadSpec {
        id: task_id,
        name: "attachable".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    let instance_name = format!("mantissa-{task_id}");
    mock_cm.attach_frames.lock().await.insert(
        instance_name.clone(),
        vec![
            RuntimeLogFrame {
                stream: RuntimeLogStream::Console,
                message: b"ready\n".to_vec(),
            },
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdErr,
                message: b"warn\n".to_vec(),
            },
        ],
    );
    mock_cm.inspect.lock().await.insert(
        instance_name.clone(),
        RuntimeInfo {
            id: instance_name.clone(),
            name: instance_name.clone(),
            status: "Up".to_string(),
            state: RuntimeStateInfo {
                raw_status: Some("running".to_string()),
                running: Some(true),
                pid: Some(1000),
                ..Default::default()
            },
            config: RuntimeConfigInfo { tty: Some(false) },
            ..Default::default()
        },
    );

    let options = RuntimeAttachOptions {
        logs: true,
        stream: true,
        stdin: true,
        stdout: true,
        stderr: true,
        detach_keys: Some("ctrl-p,ctrl-q".to_string()),
        tty: false,
        tty_width: None,
        tty_height: None,
    };
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(8);
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(8);
    input_tx
        .send(b"echo attached\n".to_vec())
        .await
        .expect("send attach input");
    drop(input_tx);

    manager
        .attach_local_workload(task_id, &options, output_tx, input_rx)
        .await
        .expect("attach local task");

    let mut frames = Vec::new();
    while let Some(frame) = output_rx.recv().await {
        frames.push(frame);
    }

    assert_eq!(
        mock_cm.attach_calls.lock().await.clone(),
        vec![(instance_name.clone(), options.clone())]
    );
    assert_eq!(
        mock_cm
            .attach_inputs
            .lock()
            .await
            .get(&instance_name)
            .cloned()
            .unwrap_or_default(),
        vec![b"echo attached\n".to_vec()]
    );
    assert_eq!(
        frames,
        vec![
            RuntimeLogFrame {
                stream: RuntimeLogStream::Console,
                message: b"ready\n".to_vec(),
            },
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdErr,
                message: b"warn\n".to_vec(),
            },
        ]
    );
}

#[tokio::test]
async fn attach_local_task_uses_runtime_tty_when_persisted_spec_is_stale() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;
    let task_id = Uuid::new_v4();
    let spec = WorkloadSpec {
        id: task_id,
        name: "demo-task".to_string(),
        image: "demo:latest".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    let instance_name = format!("mantissa-{task_id}");
    mock_cm.inspect.lock().await.insert(
        instance_name.clone(),
        RuntimeInfo {
            id: instance_name.clone(),
            name: instance_name.clone(),
            status: "Up".to_string(),
            state: RuntimeStateInfo {
                raw_status: Some("running".to_string()),
                running: Some(true),
                pid: Some(1000),
                ..Default::default()
            },
            config: RuntimeConfigInfo { tty: Some(true) },
            ..Default::default()
        },
    );

    let options = RuntimeAttachOptions {
        logs: false,
        stream: true,
        stdin: true,
        stdout: true,
        stderr: true,
        detach_keys: None,
        tty: false,
        tty_width: Some(80),
        tty_height: Some(24),
    };
    let (output_tx, _output_rx) = tokio::sync::mpsc::channel(1);
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);
    drop(input_tx);

    manager
        .attach_local_workload(task_id, &options, output_tx, input_rx)
        .await
        .expect("attach local task");

    assert_eq!(
        mock_cm.attach_calls.lock().await.clone(),
        vec![(
            instance_name,
            RuntimeAttachOptions {
                tty: true,
                ..options
            }
        )]
    );
}

#[tokio::test]
async fn exec_local_task_forwards_input_output_and_options() {
    let (manager, _scheduler, mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let spec = WorkloadSpec {
        id: task_id,
        name: "execable".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: manager.local_node_id,
        node_name: manager.local_node_name.clone(),
        slot_ids: Vec::new(),
        slot_id: None,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    };
    manager.persist_spec(&spec).await.expect("persist task");

    let instance_name = format!("mantissa-{task_id}");
    mock_cm.exec_stream_frames.lock().await.insert(
        instance_name.clone(),
        vec![
            RuntimeLogFrame {
                stream: RuntimeLogStream::Console,
                message: b"/ # ".to_vec(),
            },
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdOut,
                message: b"done\n".to_vec(),
            },
        ],
    );
    mock_cm
        .exec_stream_results
        .lock()
        .await
        .push_back(Ok(RuntimeExecResult { exit_code: Some(0) }));
    mock_cm.inspect.lock().await.insert(
        instance_name.clone(),
        RuntimeInfo {
            id: instance_name.clone(),
            name: instance_name.clone(),
            status: "Up".to_string(),
            state: RuntimeStateInfo {
                raw_status: Some("running".to_string()),
                running: Some(true),
                pid: Some(1000),
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let options = RuntimeExecOptions {
        command: vec!["sh".to_string(), "-c".to_string(), "echo done".to_string()],
        stdin: true,
        stdout: true,
        stderr: true,
        tty: true,
        detach_keys: Some("ctrl-p,ctrl-q".to_string()),
        tty_width: Some(80),
        tty_height: Some(24),
    };
    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(8);
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(8);
    input_tx
        .send(b"echo exec\n".to_vec())
        .await
        .expect("send exec input");
    drop(input_tx);

    let result = manager
        .exec_local_workload(task_id, &options, output_tx, input_rx)
        .await
        .expect("exec local task");

    let mut frames = Vec::new();
    while let Some(frame) = output_rx.recv().await {
        frames.push(frame);
    }

    assert_eq!(result.exit_code, Some(0));
    assert_eq!(
        mock_cm.exec_stream_calls.lock().await.clone(),
        vec![(instance_name.clone(), options.clone())]
    );
    assert_eq!(
        mock_cm
            .exec_stream_inputs
            .lock()
            .await
            .get(&instance_name)
            .cloned()
            .unwrap_or_default(),
        vec![b"echo exec\n".to_vec()]
    );
    assert_eq!(
        frames,
        vec![
            RuntimeLogFrame {
                stream: RuntimeLogStream::Console,
                message: b"/ # ".to_vec(),
            },
            RuntimeLogFrame {
                stream: RuntimeLogStream::StdOut,
                message: b"done\n".to_vec(),
            },
        ]
    );
}

#[tokio::test]
async fn start_tasks_batch_is_atomic_on_capacity_failure() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![
            SlotSpec::new(1, SlotCapacity::new(400, 128 * 1_024 * 1_024, 0)),
            SlotSpec::new(2, SlotCapacity::new(400, 128 * 1_024 * 1_024, 0)),
        ])
        .await
        .expect("init slots");

    manager
        .start_workload("baseline", "img", vec![], 400, 128 * 1_024 * 1_024, None)
        .await
        .expect("pre-existing container");

    let created_before = mock_cm.created.lock().await.len();

    let err = manager
        .start_workloads_batch(vec![
            WorkloadStartRequest {
                name: "svc-c".into(),
                execution: empty_resolved_execution("img"),
                execution_platform: ExecutionPlatform::Oci,
                isolation_mode: crate::workload::model::IsolationMode::Standard,
                isolation_profile: None,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                owner: None,
                target_node: None,
            },
            WorkloadStartRequest {
                name: "svc-d".into(),
                execution: empty_resolved_execution("img"),
                execution_platform: ExecutionPlatform::Oci,
                isolation_mode: crate::workload::model::IsolationMode::Standard,
                isolation_profile: None,
                gpu_device_ids: Vec::new(),
                id: None,
                slot_ids: Vec::new(),
                owner: None,
                target_node: None,
            },
        ])
        .await
        .expect_err("batch should fail when capacity is insufficient");

    assert!(
        err.chain()
            .any(|cause| cause.to_string().contains("scheduler reservation failed"))
    );

    let created_after = mock_cm.created.lock().await.len();
    assert_eq!(created_before, created_after);

    let snapshot = scheduler.snapshot().await.expect("snapshot");
    let reserved = snapshot
        .slots
        .iter()
        .filter(|slot| matches!(slot.state, SlotState::Reserved(_)))
        .count();
    assert_eq!(reserved, 1);
}

#[tokio::test]
async fn runtime_attachments_created_and_removed_on_stop() {
    let (manager, scheduler, mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "test-net".to_string(),
        description: "test network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.42.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "with-net".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let mut specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start batch with network");
    assert_eq!(specs.len(), 1);

    let task_spec = specs.remove(0);
    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    let attachment = &attachments[0];
    assert_eq!(attachment.network_id, spec.id);
    assert_eq!(attachment.state, NetworkAttachmentState::Ready);
    assert_eq!(attachment.node_id, manager.local_node_id);
    assert!(attachment.assigned_ip.is_some());
    assert!(attachment.mac.is_some());

    assert_eq!(mock_cm.created.lock().await.len(), 1);

    let requested = manager
        .request_workload_stop(task_spec.id)
        .await
        .expect("request stop networked task");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile stop networked task");

    let attachments_after = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments after stop");
    assert!(attachments_after.is_empty());

    assert!(manager.inspect_workload(task_spec.id).await.is_err());
}

#[tokio::test]
async fn service_runtime_attachments_start_unpublished_until_controller_publishes() {
    let (manager, scheduler, _mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "service-net".to_string(),
        description: "service network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.52.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "service-backend".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "svc", "backend",
        ))),
        target_node: None,
    };

    let mut specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start service task");
    let task_spec = specs.pop().expect("service task created");

    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    assert!(
        !attachments[0].traffic_published,
        "service attachments should start unpublished until the service controller cuts traffic over"
    );

    manager
        .set_task_traffic_published(task_spec.id, true)
        .await
        .expect("publish task traffic");
    let published = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments after publish");
    assert!(published[0].traffic_published);
}

#[tokio::test]
async fn set_task_traffic_published_reports_missing_attachments() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let network_id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    let value = WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: "service-task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now,
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network_id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "svc", "backend",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .core
        .store
        .upsert(&UuidKey::from(task_id), value)
        .await
        .expect("persist service task");

    let update = manager
        .set_task_traffic_published(task_id, true)
        .await
        .expect("set task traffic publication");
    assert_eq!(update, WorkloadTrafficPublicationUpdate::NoAttachments);
}

#[tokio::test]
async fn publish_task_traffic_when_attachment_rows_exist_publishes_late_attachment() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: "late-attachment-net".to_string(),
        description: "late attachment network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.54.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        network.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let task_id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    let value = WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: "service-task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network.id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "svc", "backend",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .core
        .store
        .upsert(&UuidKey::from(task_id), value)
        .await
        .expect("persist service task");

    let manager_for_wait = manager.clone();
    let wait_publish = async move {
        manager_for_wait
            .publish_task_traffic_when_attachment_rows_exist(
                task_id,
                std::time::Duration::from_secs(2),
            )
            .await
    };
    let insert_attachment = async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        network_registry
            .upsert_attachment(NetworkAttachmentValue::new(NetworkAttachmentDraft {
                id: crate::network::types::compute_network_attachment_id(task_id, network.id),
                task_id,
                node_id: manager.local_node_id,
                instance_id: format!("mantissa-{task_id}"),
                network_id: network.id,
                task_updated_at: Some(now),
                requested_ip: Some("10.54.0.2".to_string()),
                assigned_ip: Some("10.54.0.2".to_string()),
                mac: Some("02:11:22:33:44:aa".to_string()),
                state: NetworkAttachmentState::Ready,
                error: None,
                traffic_published: false,
                service_name: Some("svc".to_string()),
                template_name: Some("backend".to_string()),
            }))
            .await
            .expect("insert attachment");
    };

    let (wait_result, _) = tokio::join!(wait_publish, insert_attachment);
    wait_result.expect("wait for late attachment publication");

    let attachments = network_registry
        .list_attachments_for_task(task_id)
        .expect("list attachments after publish");
    assert_eq!(attachments.len(), 1);
    assert!(attachments[0].traffic_published);
}

#[tokio::test]
async fn ensure_task_service_traffic_ready_requires_local_network_readiness() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let network = NetworkSpecValue::new(NetworkSpecDraft {
        name: "service-traffic-ready-net".to_string(),
        description: "service traffic readiness network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.55.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(network.clone())
        .await
        .expect("upsert network spec");

    network_registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            network.id,
            manager.local_node_id,
            "local-node",
            NetworkPeerState::Configuring,
            None,
        ))
        .await
        .expect("upsert configuring peer state");

    let task_id = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();
    let value = WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: "service-task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        command: Vec::new(),
        tty: false,
        node_id: manager.local_node_id,
        node_name: "local-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network.id],
        cpu_millis: 100,
        memory_bytes: 64 * 1024 * 1024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "svc", "backend",
        ))),
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .core
        .store
        .upsert(&UuidKey::from(task_id), value)
        .await
        .expect("persist service task");

    network_registry
        .upsert_attachment(NetworkAttachmentValue::new(NetworkAttachmentDraft {
            id: crate::network::types::compute_network_attachment_id(task_id, network.id),
            task_id,
            node_id: manager.local_node_id,
            instance_id: format!("mantissa-{task_id}"),
            network_id: network.id,
            task_updated_at: Some(now),
            requested_ip: Some("10.55.0.2".to_string()),
            assigned_ip: Some("10.55.0.2".to_string()),
            mac: Some("02:11:22:33:44:ab".to_string()),
            state: NetworkAttachmentState::Ready,
            error: None,
            traffic_published: true,
            service_name: Some("svc".to_string()),
            template_name: Some("backend".to_string()),
        }))
        .await
        .expect("insert published attachment");

    let ready = manager
        .ensure_task_service_traffic_ready(task_id)
        .await
        .expect("evaluate service traffic readiness while network configuring");
    assert!(
        !ready,
        "service traffic must stay withdrawn until the local network peer is ready"
    );
    let attachments = network_registry
        .list_attachments_for_task(task_id)
        .expect("list attachments while configuring");
    assert!(
        !attachments[0].traffic_published,
        "configuring networks must withdraw published service traffic"
    );

    network_registry
        .upsert_peer_state(NetworkPeerStateValue::new(
            network.id,
            manager.local_node_id,
            "local-node",
            NetworkPeerState::Ready,
            None,
        ))
        .await
        .expect("upsert ready peer state");

    let republished = manager
        .ensure_task_service_traffic_ready(task_id)
        .await
        .expect("republish service traffic after peer readiness");
    assert!(
        !republished,
        "the first successful publish pass should republish and ask callers to recheck"
    );
    let attachments = network_registry
        .list_attachments_for_task(task_id)
        .expect("list attachments after republish");
    assert!(
        attachments[0].traffic_published,
        "ready local networks should republish service traffic"
    );

    let ready = manager
        .ensure_task_service_traffic_ready(task_id)
        .await
        .expect("re-evaluate service traffic readiness after republish");
    assert!(
        ready,
        "published ready attachments on a ready network should be routable"
    );
}

#[tokio::test]
async fn withdraw_local_service_traffic_publication_only_touches_local_service_rows() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let local_network = Uuid::new_v4();
    let remote_network = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();

    let local_service_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, local_network),
        task_id,
        node_id: manager.local_node_id,
        instance_id: format!("mantissa-{task_id}"),
        network_id: local_network,
        task_updated_at: Some(now.clone()),
        requested_ip: Some("10.78.0.2".to_string()),
        assigned_ip: Some("10.78.0.2".to_string()),
        mac: Some("02:11:22:33:44:66".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });
    let remote_service_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, remote_network),
        task_id,
        node_id: remote_node,
        instance_id: format!("mantissa-{task_id}"),
        network_id: remote_network,
        task_updated_at: Some(now.clone()),
        requested_ip: Some("10.78.0.3".to_string()),
        assigned_ip: Some("10.78.0.3".to_string()),
        mac: Some("02:11:22:33:44:77".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });
    let local_non_service_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(Uuid::new_v4(), Uuid::new_v4()),
        task_id: Uuid::new_v4(),
        node_id: manager.local_node_id,
        instance_id: "mantissa-non-service".to_string(),
        network_id: Uuid::new_v4(),
        task_updated_at: Some(now),
        requested_ip: Some("10.78.0.4".to_string()),
        assigned_ip: Some("10.78.0.4".to_string()),
        mac: Some("02:11:22:33:44:88".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: None,
        template_name: None,
    });

    network_registry
        .upsert_attachment(local_service_attachment)
        .await
        .expect("insert local service attachment");
    network_registry
        .upsert_attachment(remote_service_attachment)
        .await
        .expect("insert remote service attachment");
    network_registry
        .upsert_attachment(local_non_service_attachment)
        .await
        .expect("insert local non-service attachment");

    let updated = manager
        .withdraw_local_service_traffic_publication()
        .await
        .expect("withdraw startup service traffic");
    assert_eq!(updated, 1);

    let attachments = network_registry
        .list_attachments(None)
        .expect("list attachments after startup withdrawal");
    let local_service = attachments
        .iter()
        .find(|attachment| attachment.network_id == local_network)
        .expect("local service attachment");
    assert!(
        !local_service.traffic_published,
        "local service rows must be withdrawn during startup recovery"
    );
    let remote_service = attachments
        .iter()
        .find(|attachment| attachment.network_id == remote_network)
        .expect("remote service attachment");
    assert!(
        remote_service.traffic_published,
        "remote rows must not be touched by local startup withdrawal"
    );
    let local_non_service = attachments
        .iter()
        .find(|attachment| attachment.instance_id == "mantissa-non-service")
        .expect("local non-service attachment");
    assert!(
        local_non_service.traffic_published,
        "non-service rows must not be withdrawn by the service startup path"
    );
}

#[tokio::test]
async fn stop_withdraws_attachment_traffic_before_runtime_stop() {
    let (manager, scheduler, mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "stop-net".to_string(),
        description: "stop network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.53.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "standalone-net".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let mut specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start standalone task");
    let task_spec = specs.pop().expect("standalone task created");

    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list initial attachments");
    assert_eq!(attachments.len(), 1);
    assert!(
        attachments[0].traffic_published,
        "standalone attachment should begin published"
    );

    *mock_cm.stop_delay.lock().await = Some(std::time::Duration::from_millis(300));

    let requested = manager
        .request_workload_stop(task_spec.id)
        .await
        .expect("request stop");
    let manager_for_stop = manager.clone();
    let inspect_during_stop = async {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let attachments = network_registry
                .list_attachments_for_task(task_spec.id)
                .expect("list attachments during stop");
            if attachments.len() == 1 && !attachments[0].traffic_published {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "attachment traffic should be withdrawn before runtime stop completes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert!(
            mock_cm.stopped.lock().await.is_empty(),
            "runtime stop should still be pending while publication is already withdrawn"
        );
    };

    let stop_task = async move { manager_for_stop.reconcile_local_task(requested).await };
    let (_, stop_result) = tokio::join!(inspect_during_stop, stop_task);
    stop_result.expect("reconcile stop");
}

#[tokio::test]
async fn request_task_stop_cleans_up_after_teardown_failure() {
    let attachment: Arc<dyn AttachmentProvisionerApi> =
        Arc::new(FlakyAttachmentProvisioner::default());
    let (manager, scheduler, _mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(attachment)).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "flaky-net".to_string(),
        description: "flaky network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.99.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "flaky-task".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let mut specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start batch with network");
    let task_spec = specs.remove(0);

    manager
        .request_workload_stop(task_spec.id)
        .await
        .expect("request stop flaky networked task");
    let stopping = manager
        .load_spec(task_spec.id)
        .await
        .expect("load stopping task");
    manager
        .reconcile_local_task(stopping)
        .await
        .expect("reconcile stop flaky networked task");

    let attachments_after = network_registry
        .list_attachments(Some(spec.id))
        .expect("list attachments after flaky stop");
    assert!(
        attachments_after.is_empty(),
        "expected attachments to be purged after teardown failure"
    );
}

#[tokio::test]
async fn remove_event_purges_remote_attachment_without_local_spec() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let network_id = Uuid::new_v4();
    let attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id: Uuid::new_v4(),
        instance_id: format!("mantissa-{task_id}"),
        network_id,
        task_updated_at: Some(Utc::now().to_rfc3339()),
        requested_ip: Some("10.77.0.2".to_string()),
        assigned_ip: Some("10.77.0.2".to_string()),
        mac: Some("02:11:22:33:44:55".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });

    network_registry
        .upsert_attachment(attachment)
        .await
        .expect("insert remote-owned attachment");
    assert_eq!(
        network_registry
            .list_attachments_for_task(task_id)
            .expect("list attachment before remove")
            .len(),
        1
    );

    manager
        .teardown_runtime_attachments(task_id, HashSet::new(), true)
        .await
        .expect("force teardown for removed task");

    assert!(
        network_registry
            .list_attachments_for_task(task_id)
            .expect("list attachment after remove")
            .is_empty(),
        "remove event should purge stale replicated attachment rows even without a local spec"
    );
}

#[tokio::test]
async fn duplicate_remove_event_does_not_poison_future_epoch_upsert() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let original = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");
    manager
        .handle_event(WorkloadEvent::Remove { id: original.id })
        .await
        .expect("apply duplicate remove event");

    let mut replacement = original.clone();
    replacement.node_id = Uuid::new_v4();
    replacement.node_name = "remote-node".to_string();
    replacement.state = WorkloadPhase::Running;
    replacement.updated_at = (Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    replacement.task_epoch = replacement.task_epoch.saturating_add(1);
    replacement.phase_version = replacement.phase_version.saturating_add(1);

    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(replacement.clone())))
        .await
        .expect("apply replacement upsert");

    let tasks = manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list tasks after replacement");
    assert_eq!(
        tasks.len(),
        1,
        "replacement should be accepted after duplicate remove"
    );
    assert_eq!(tasks[0].id, replacement.id);
    assert_eq!(tasks[0].task_epoch, replacement.task_epoch);
}

#[tokio::test]
async fn next_epoch_after_remove_uses_watermark_increment() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let started = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    manager
        .remove_spec(started.id)
        .await
        .expect("remove task spec");

    let next = manager
        .next_task_epoch_for_assignment(started.id, manager.local_node_id, &[1])
        .await
        .expect("next epoch");
    assert_eq!(
        next,
        started.task_epoch.saturating_add(1),
        "removed task should restart on a newer epoch"
    );
}

#[tokio::test]
async fn next_epoch_after_remove_without_watermark_uses_tombstone_floor() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let started = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");
    manager
        .remove_spec(started.id)
        .await
        .expect("remove task spec");
    manager.clear_remove_watermark(started.id).await;

    let next = manager
        .next_task_epoch_for_assignment(started.id, manager.local_node_id, &[1])
        .await
        .expect("next epoch");
    assert_eq!(
        next, 1,
        "durable tombstone should force a non-zero restart epoch"
    );
}

#[tokio::test]
async fn next_epoch_after_conflicting_concurrent_assignment_uses_snapshot_max() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let mut local = test_task_spec(&manager, "svc");
    local.id = task_id;
    local.state = WorkloadPhase::Running;
    local.slot_ids = vec![11];
    local.slot_id = Some(11);
    local.task_epoch = 7;
    local.phase_version = 1;
    local.updated_at = (Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    manager
        .persist_spec(&local)
        .await
        .expect("persist local task");

    let mut remote = build_remote_task_spec(
        task_id,
        Uuid::nil(),
        WorkloadPhase::Running,
        7,
        1,
        Utc::now().to_rfc3339(),
    );
    remote.slot_ids = vec![22];
    remote.slot_id = Some(22);

    let (remote_db, _remote_dir) = temp_db("tasks-concurrent-epoch");
    let remote_store =
        open_workload_store(remote_db, Uuid::new_v4()).expect("open remote workload store");
    remote_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild remote workload store");
    remote_store
        .upsert(&UuidKey::from(task_id), spec_to_value(&remote))
        .await
        .expect("persist remote concurrent task");

    let remote_ranges = remote_store
        .page_range_summary()
        .await
        .expect("remote page ranges");
    let (regs, tombs) = remote_store
        .export_page_ranges_delta(&remote_ranges)
        .expect("export remote delta");
    manager
        .core
        .store
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .expect("apply remote concurrent delta");

    let snapshot = manager
        .core
        .store
        .get_snapshot(&UuidKey::from(task_id))
        .expect("load concurrent snapshot")
        .expect("task snapshot should exist");
    assert!(
        snapshot.as_slice().len() >= 2,
        "split-style concurrent assignments should retain both values before cutover"
    );

    let next = manager
        .next_task_epoch_for_assignment(task_id, manager.local_node_id, &[11])
        .await
        .expect("next epoch");
    assert_eq!(
        next, 8,
        "conflicting concurrent assignments should force a fresh cutover epoch"
    );
}

#[tokio::test]
async fn stale_remove_event_does_not_delete_active_local_task() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let running = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .handle_event(WorkloadEvent::Remove { id: running.id })
        .await
        .expect("handle stale remove");

    let persisted = manager
        .load_spec(running.id)
        .await
        .expect("running task should remain persisted");
    assert_eq!(persisted.id, running.id);
    assert_eq!(persisted.node_id, manager.local_node_id);
    assert_eq!(persisted.state, WorkloadPhase::Running);
}

#[tokio::test]
async fn stale_upsert_after_remove_watermark_is_ignored_until_newer_epoch() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut original = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");

    original.node_id = Uuid::new_v4();
    original.node_name = "remote-node".to_string();
    original.state = WorkloadPhase::Stopping;

    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(original.clone())))
        .await
        .expect("stale upsert should be handled");
    let after_stale = manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list after stale upsert");
    assert!(
        after_stale.is_empty(),
        "stale upsert should not recreate removed task rows"
    );

    let mut fresh = original.clone();
    fresh.state = WorkloadPhase::Running;
    fresh.updated_at = (Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
    fresh.task_epoch = fresh.task_epoch.saturating_add(1);

    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(fresh.clone())))
        .await
        .expect("fresh upsert should be accepted");
    let after_fresh = manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list after fresh upsert");
    assert_eq!(
        after_fresh.len(),
        1,
        "newer-epoch upsert should recreate the row"
    );
    assert_eq!(after_fresh[0].id, fresh.id);
}

#[tokio::test]
async fn upsert_after_remove_without_watermark_is_accepted_for_reconvergence() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let mut original = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");
    manager.clear_remove_watermark(original.id).await;

    original.node_id = Uuid::new_v4();
    original.node_name = "remote-node".to_string();
    original.state = WorkloadPhase::Stopping;

    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(original.clone())))
        .await
        .expect("upsert should be handled");
    let after_upsert = manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list after upsert");
    assert!(
        !after_upsert.is_empty(),
        "upsert should be accepted once remove watermark is no longer present"
    );
    assert_eq!(after_upsert[0].id, original.id);
    assert_eq!(after_upsert[0].node_id, original.node_id);
}

#[tokio::test]
async fn stale_delta_after_remove_without_watermark_does_not_recreate_row() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let slot_spec = SlotSpec::new(1, SlotCapacity::new(500, 128 * 1_024 * 1_024, 0));
    scheduler
        .init_slots(vec![slot_spec])
        .await
        .expect("init slots");

    let original = manager
        .start_workload("svc", "img", vec![], 200, 64 * 1_024 * 1_024, None)
        .await
        .expect("start container");

    manager
        .remove_spec(original.id)
        .await
        .expect("remove task spec");
    manager.clear_remove_watermark(original.id).await;

    let remote_node = Uuid::new_v4();
    let stale_delta = WorkloadValue::new(WorkloadValueDraft {
        id: original.id,
        name: original.name.clone(),
        image: original.image.clone(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Stopping,
        phase_reason: None,
        phase_progress: None,
        created_at: original.created_at.clone(),
        updated_at: (Utc::now() + chrono::Duration::seconds(30)).to_rfc3339(),
        command: original.command.clone(),
        tty: false,
        node_id: remote_node,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: original.task_epoch,
        phase_version: original.phase_version,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });

    let (remote_db, _remote_dir) = temp_db("tasks-sync-tomb");
    let remote_store =
        open_workload_store(remote_db, Uuid::new_v4()).expect("open remote workload store");
    remote_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild remote workload store");
    remote_store
        .upsert(&UuidKey::from(original.id), stale_delta)
        .await
        .expect("write stale value to remote store");

    let remote_ranges = remote_store
        .page_range_summary()
        .await
        .expect("remote page ranges");
    let (regs, tombs) = remote_store
        .export_page_ranges_delta(&remote_ranges)
        .expect("export remote delta");
    manager
        .core
        .store
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .expect("apply stale delta to local store");

    let after_stale = manager
        .list_workloads(&TaskStateFilter::all())
        .await
        .expect("list after stale delta");
    assert!(
        after_stale.is_empty(),
        "local tombstone should block stale delta replay for removed task rows"
    );
}

/// Builds a remote-owned task specification for ordering and sync/gossip conflict tests.
fn build_remote_task_spec(
    id: Uuid,
    node_id: Uuid,
    state: WorkloadPhase,
    task_epoch: u64,
    phase_version: u64,
    updated_at: String,
) -> WorkloadSpec {
    WorkloadSpec {
        id,
        name: "remote-task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state,
        phase_reason: None,
        phase_progress: None,
        created_at: updated_at.clone(),
        updated_at,
        command: Vec::new(),
        tty: false,
        node_id,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        slot_id: Some(1),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch,
        phase_version,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    }
}

/// Builds a compact remote-owned task status payload for lifecycle gossip tests.
fn build_remote_task_status(spec: &WorkloadSpec) -> WorkloadStatus {
    WorkloadStatus::from_spec(spec)
}

#[tokio::test]
async fn out_of_order_task_upsert_keeps_newer_running_state() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let running = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Running,
        3,
        9,
        now.to_rfc3339(),
    );
    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(running.clone())))
        .await
        .expect("apply running upsert");

    let delayed_pending = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Pending,
        running.task_epoch,
        running.phase_version.saturating_sub(1),
        (now + chrono::Duration::seconds(60)).to_rfc3339(),
    );
    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(delayed_pending)))
        .await
        .expect("apply delayed stale pending upsert");

    let resolved = manager
        .load_spec(task_id)
        .await
        .expect("load causally resolved task");
    assert_eq!(resolved.state, WorkloadPhase::Running);
    assert_eq!(resolved.task_epoch, running.task_epoch);
    assert_eq!(resolved.phase_version, running.phase_version);
}

#[tokio::test]
async fn compact_status_upsert_updates_existing_task_without_dropping_definition() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let mut pulling = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Pulling,
        2,
        5,
        now.to_rfc3339(),
    );
    pulling.command = vec!["sleep".to_string(), "30".to_string()];
    pulling.cpu_millis = 250;
    pulling.memory_bytes = 128 * 1_024 * 1_024;

    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(pulling.clone())))
        .await
        .expect("apply pulling upsert");

    let mut status = build_remote_task_status(&pulling);
    status.phase_reason = Some("backing off".to_string());
    status.phase_progress = Some("retry 2/3".to_string());
    status.updated_at = (now + chrono::Duration::seconds(10)).to_rfc3339();

    manager
        .handle_event(WorkloadEvent::UpsertStatus(Box::new(status)))
        .await
        .expect("apply compact status update");

    let resolved = manager
        .load_spec(task_id)
        .await
        .expect("load merged pulling task");
    assert_eq!(resolved.state, WorkloadPhase::Pulling);
    assert_eq!(resolved.phase_reason.as_deref(), Some("backing off"));
    assert_eq!(resolved.phase_progress.as_deref(), Some("retry 2/3"));
    assert_eq!(
        resolved.command,
        vec!["sleep".to_string(), "30".to_string()]
    );
    assert_eq!(resolved.cpu_millis, 250);
    assert_eq!(resolved.memory_bytes, 128 * 1_024 * 1_024);
}

#[tokio::test]
async fn late_full_spec_fills_definition_after_status_placeholder() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let mut running = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Running,
        4,
        7,
        now.to_rfc3339(),
    );
    running.command = vec!["server".to_string(), "--foreground".to_string()];
    running.cpu_millis = 700;
    running.memory_bytes = 512 * 1_024 * 1_024;

    let status = build_remote_task_status(&running);
    manager
        .handle_event(WorkloadEvent::UpsertStatus(Box::new(status)))
        .await
        .expect("apply compact running status first");

    let placeholder = manager
        .load_spec(task_id)
        .await
        .expect("load placeholder task");
    assert_eq!(placeholder.state, WorkloadPhase::Running);
    assert!(placeholder.command.is_empty());
    assert_eq!(placeholder.cpu_millis, 0);

    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(running.clone())))
        .await
        .expect("apply late full task definition");

    let resolved = manager
        .load_spec(task_id)
        .await
        .expect("load resolved task");
    assert_eq!(resolved.state, WorkloadPhase::Running);
    assert_eq!(
        resolved.command,
        vec!["server".to_string(), "--foreground".to_string()]
    );
    assert_eq!(resolved.cpu_millis, 700);
    assert_eq!(resolved.memory_bytes, 512 * 1_024 * 1_024);
}

#[tokio::test]
async fn dirty_gossip_flush_keeps_definition_and_latest_status() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let mut pulling = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Pulling,
        6,
        3,
        now.to_rfc3339(),
    );
    pulling.command = vec!["pull".to_string(), "image".to_string()];

    let mut status = build_remote_task_status(&pulling);
    status.phase_progress = Some("retry 1/2".to_string());
    status.updated_at = (now + chrono::Duration::seconds(10)).to_rfc3339();

    let mut newer_status = status.clone();
    newer_status.phase_progress = Some("retry 2/2".to_string());
    newer_status.updated_at = (now + chrono::Duration::seconds(20)).to_rfc3339();

    manager
        .enqueue_gossip_best_effort(WorkloadEvent::UpsertSpec(Box::new(pulling.clone())))
        .await
        .expect("buffer full task definition");
    manager
        .enqueue_gossip_best_effort(WorkloadEvent::UpsertStatus(Box::new(status)))
        .await
        .expect("buffer intermediate task status");
    manager
        .enqueue_gossip_best_effort(WorkloadEvent::UpsertStatus(Box::new(newer_status.clone())))
        .await
        .expect("buffer latest task status");

    let _ = manager
        .flush_dirty_gossip_events()
        .await
        .expect("flush dirty gossip");

    let first = manager
        .core
        .rx
        .recv()
        .await
        .expect("receive buffered definition");
    let second = manager
        .core
        .rx
        .recv()
        .await
        .expect("receive buffered latest status");

    match first {
        Message::Workload {
            event: WorkloadEvent::UpsertSpec(spec),
            ..
        } => {
            assert_eq!(spec.id, task_id);
            assert_eq!(spec.command, vec!["pull".to_string(), "image".to_string()]);
        }
        _ => panic!("unexpected first flushed message"),
    }

    match second {
        Message::Workload {
            event: WorkloadEvent::UpsertStatus(status),
            ..
        } => {
            assert_eq!(status.id, task_id);
            assert_eq!(status.phase_progress.as_deref(), Some("retry 2/2"));
        }
        _ => panic!("unexpected second flushed message"),
    }

    let third =
        tokio::time::timeout(std::time::Duration::from_millis(20), manager.core.rx.recv()).await;
    assert!(
        third.is_err(),
        "dirty gossip flush should collapse intermediate workload updates"
    );
}

#[tokio::test]
async fn stale_delta_write_does_not_override_newer_gossip_upsert() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now();

    let gossip_running = build_remote_task_spec(
        task_id,
        remote_node,
        WorkloadPhase::Running,
        5,
        11,
        now.to_rfc3339(),
    );
    manager
        .handle_event(WorkloadEvent::UpsertSpec(Box::new(gossip_running.clone())))
        .await
        .expect("apply newer gossip upsert");

    let stale_delta = WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: "remote-task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Pending,
        phase_reason: None,
        phase_progress: None,
        created_at: now.to_rfc3339(),
        updated_at: (now + chrono::Duration::seconds(120)).to_rfc3339(),
        command: Vec::new(),
        tty: false,
        node_id: remote_node,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: Vec::new(),
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: gossip_running.task_epoch,
        phase_version: gossip_running.phase_version.saturating_sub(1),
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    let (remote_db, _remote_dir) = temp_db("tasks-sync-delta");
    let remote_store =
        open_workload_store(remote_db, Uuid::new_v4()).expect("open remote workload store");
    remote_store
        .rebuild_mst_from_disk()
        .await
        .expect("rebuild remote workload store");
    remote_store
        .upsert(&UuidKey::from(task_id), stale_delta)
        .await
        .expect("write stale value to remote store");

    let remote_ranges = remote_store
        .page_range_summary()
        .await
        .expect("remote page ranges");
    let (regs, tombs) = remote_store
        .export_page_ranges_delta(&remote_ranges)
        .expect("export remote delta");
    manager
        .core
        .store
        .apply_delta_chunk_update_mst(regs, tombs)
        .await
        .expect("apply stale delta to local store");

    let resolved = manager
        .load_spec(task_id)
        .await
        .expect("load causally resolved task");
    assert_eq!(resolved.state, WorkloadPhase::Running);
    assert_eq!(resolved.task_epoch, gossip_running.task_epoch);
    assert_eq!(resolved.phase_version, gossip_running.phase_version);
}

#[tokio::test]
async fn teardown_local_attachment_records_preserves_remote_rows() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let local_network = Uuid::new_v4();
    let remote_network = Uuid::new_v4();
    let remote_node = Uuid::new_v4();

    let local_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, local_network),
        task_id,
        node_id: manager.local_node_id,
        instance_id: format!("mantissa-{task_id}"),
        network_id: local_network,
        task_updated_at: Some(Utc::now().to_rfc3339()),
        requested_ip: Some("10.78.0.2".to_string()),
        assigned_ip: Some("10.78.0.2".to_string()),
        mac: Some("02:11:22:33:44:66".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });
    let remote_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, remote_network),
        task_id,
        node_id: remote_node,
        instance_id: format!("mantissa-{task_id}"),
        network_id: remote_network,
        task_updated_at: Some(Utc::now().to_rfc3339()),
        requested_ip: Some("10.78.0.3".to_string()),
        assigned_ip: Some("10.78.0.3".to_string()),
        mac: Some("02:11:22:33:44:77".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });

    network_registry
        .upsert_attachment(local_attachment)
        .await
        .expect("insert local attachment");
    network_registry
        .upsert_attachment(remote_attachment)
        .await
        .expect("insert remote attachment");

    manager
        .teardown_local_attachment_records(task_id)
        .await
        .expect("teardown local attachment records");

    let remaining = network_registry
        .list_attachments_for_task(task_id)
        .expect("list remaining attachments");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].node_id, remote_node);
}

#[tokio::test]
async fn repair_runtime_attachments_purges_unowned_local_rows() {
    let (manager, _scheduler, _mock_cm, network_registry) = setup_manager().await;

    let task_id = Uuid::new_v4();
    let network_id = Uuid::new_v4();
    let remote_node = Uuid::new_v4();
    let now = Utc::now().to_rfc3339();

    let remote_value = WorkloadValue::new(WorkloadValueDraft {
        id: task_id,
        name: "remote-task".to_string(),
        image: "img".to_string(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        state: WorkloadPhase::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: now.clone(),
        updated_at: now.clone(),
        command: vec![],
        tty: false,
        node_id: remote_node,
        node_name: "remote-node".to_string(),
        slot_ids: vec![1],
        networks: vec![network_id],
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        owner: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 0,
        last_terminal_observed_launch: None,
    });
    manager
        .core
        .store
        .upsert(&UuidKey::from(task_id), remote_value)
        .await
        .expect("insert remote task value");

    let stale_local_attachment = NetworkAttachmentValue::new(NetworkAttachmentDraft {
        id: crate::network::types::compute_network_attachment_id(task_id, network_id),
        task_id,
        node_id: manager.local_node_id,
        instance_id: format!("mantissa-{task_id}"),
        network_id,
        task_updated_at: Some(now),
        requested_ip: Some("10.79.0.2".to_string()),
        assigned_ip: Some("10.79.0.2".to_string()),
        mac: Some("02:11:22:33:44:88".to_string()),
        state: NetworkAttachmentState::Ready,
        error: None,
        traffic_published: true,
        service_name: Some("svc".to_string()),
        template_name: Some("backend".to_string()),
    });
    network_registry
        .upsert_attachment(stale_local_attachment)
        .await
        .expect("insert stale local attachment");

    manager
        .repair_runtime_attachments()
        .await
        .expect("repair runtime attachments");

    assert!(
        network_registry
            .list_attachments_for_task(task_id)
            .expect("list attachments after repair")
            .is_empty(),
        "repair should remove local attachment rows for tasks now owned by remote nodes"
    );
}

#[tokio::test]
async fn attachment_ready_triggers_forwarding_event() {
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (manager, scheduler, _mock_cm, network_registry) =
        setup_manager_with_forwarding(Some(event_tx), None).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "forwarding-net".to_string(),
        description: "forwarding network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.55.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "with-forwarding".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let _specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start networked task");

    let event = event_rx
        .recv()
        .await
        .expect("forwarding event should be emitted");
    match event {
        ForwardingEvent::AttachmentReady { network_id } => assert_eq!(network_id, spec.id),
    }
}

#[tokio::test]
async fn runtime_attachments_reconcile_removes_stale_entries() {
    let (manager, scheduler, _mock_cm, network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec_a = NetworkSpecValue::new(NetworkSpecDraft {
        name: "net-a".to_string(),
        description: "network a".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.43.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    let spec_b = NetworkSpecValue::new(NetworkSpecDraft {
        name: "net-b".to_string(),
        description: "network b".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.44.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });

    network_registry
        .upsert_spec(spec_a.clone())
        .await
        .expect("upsert network a");
    network_registry
        .upsert_spec(spec_b.clone())
        .await
        .expect("upsert network b");

    let peer_state_a = NetworkPeerStateValue::new(
        spec_a.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    let peer_state_b = NetworkPeerStateValue::new(
        spec_b.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state_a)
        .await
        .expect("upsert peer a");
    network_registry
        .upsert_peer_state(peer_state_b)
        .await
        .expect("upsert peer b");

    let request = WorkloadStartRequest {
        name: "two-nets".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec_a.id, spec_b.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let mut specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("start batch with two networks");
    let task_spec = specs
        .pop()
        .expect("task created with two network attachments");

    let instance_id = {
        let guard = manager.local_state.local_instances.lock().await;
        guard
            .get(&task_spec.id)
            .cloned()
            .expect("instance id recorded")
    };

    let initial = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list initial attachments");
    assert_eq!(initial.len(), 2);

    manager
        .ensure_runtime_attachments(task_spec.id, &instance_id, &[spec_a.id], None)
        .await
        .expect("reconcile attachments");

    let reconciled = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list reconciled attachments");
    assert_eq!(reconciled.len(), 1);
    assert_eq!(reconciled[0].network_id, spec_a.id);
    assert_eq!(reconciled[0].state, NetworkAttachmentState::Ready);
}

#[tokio::test]
async fn runtime_attachments_retry_transient_provision_errors() {
    let provisioner = Arc::new(RetryingAttachmentProvisioner::new(1));
    let attachment_override: Arc<dyn AttachmentProvisionerApi> = provisioner.clone();
    let (manager, scheduler, mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(attachment_override)).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "retry-net".to_string(),
        description: "retry network".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.47.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "retry-net-task".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let specs = manager
        .start_workloads_batch(vec![request])
        .await
        .expect("task should converge after transient attachment race");
    assert_eq!(specs.len(), 1);

    let task_spec = &specs[0];
    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].state, NetworkAttachmentState::Ready);

    let ensure_calls = provisioner.ensure_calls.lock().await.clone();
    assert!(
        ensure_calls.len() >= 2,
        "expected one retry after transient attachment failure"
    );

    let inspect_calls = mock_cm.inspect_calls.lock().await.clone();
    assert!(
        inspect_calls.len() >= 2,
        "expected inspect refresh while retrying transient attachment provisioning"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn runtime_attachments_real_provisioning_runs_when_enabled() {
    if std::env::var("MANTISSA_NETWORK_TESTS").ok().as_deref() != Some("1") {
        eprintln!("skipping real networking test; set MANTISSA_NETWORK_TESTS=1 to enable");
        return;
    }

    let provisioner = match AttachmentProvisioner::new() {
        Ok(provisioner) => Arc::new(provisioner) as Arc<dyn AttachmentProvisionerApi>,
        Err(err) => {
            eprintln!("skipping real networking test; failed to initialize rtnetlink: {err}");
            return;
        }
    };

    let (manager, scheduler, _mock_cm, network_registry) =
        setup_manager_with_forwarding(None, Some(provisioner)).await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let spec = NetworkSpecValue::new(NetworkSpecDraft {
        name: "real-net".to_string(),
        description: "real networking".to_string(),
        driver: NetworkDriver::Vxlan,
        subnet_cidr: "10.46.0.0/24".to_string(),
        vni: 0,
        mtu: 0,
        sealed: false,
        bpf_programs: vec![],
    });
    network_registry
        .upsert_spec(spec.clone())
        .await
        .expect("upsert network spec");

    let peer_state = NetworkPeerStateValue::new(
        spec.id,
        manager.local_node_id,
        "local-node",
        NetworkPeerState::Ready,
        None,
    );
    network_registry
        .upsert_peer_state(peer_state)
        .await
        .expect("upsert peer state");

    let request = WorkloadStartRequest {
        name: "real-net-task".into(),
        execution: ResolvedExecutionSpec {
            networks: vec![spec.id],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: None,
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let mut specs = match manager.start_workloads_batch(vec![request]).await {
        Ok(specs) => specs,
        Err(err) => {
            if err
                .chain()
                .any(|cause| cause.to_string().contains("Operation not permitted"))
            {
                eprintln!("skipping real networking test; missing CAP_NET_ADMIN privileges: {err}");
                return;
            }
            panic!("failed to start networked task: {err:#}");
        }
    };

    let task_spec = specs.remove(0);
    let attachments = network_registry
        .list_attachments_for_task(task_spec.id)
        .expect("list attachments");
    assert_eq!(attachments.len(), 1);
    let attachment = &attachments[0];
    assert_eq!(attachment.network_id, spec.id);
    assert_eq!(attachment.state, NetworkAttachmentState::Ready);

    let requested = manager
        .request_workload_stop(task_spec.id)
        .await
        .expect("request stop real networked task");
    manager
        .reconcile_local_task(requested)
        .await
        .expect("reconcile stop real networked task");
}

#[test]
fn scheduling_retry_budget_stays_wide_for_untargeted_starts() {
    let intents = vec![StartIntent {
        index: 0,
        id: Uuid::new_v4(),
        name: "untargeted".into(),
        image: "img".into(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        required_runtime_features: Vec::new(),
        preassigned_slots: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        placement: Default::default(),
        owner: None,
        target_node: None,
    }];

    assert_eq!(scheduling_retry_max_attempts_for_intents(&intents), 30);
}

#[test]
fn scheduling_retry_budget_is_shorter_for_targeted_starts() {
    let intents = vec![StartIntent {
        index: 0,
        id: Uuid::new_v4(),
        name: "targeted".into(),
        image: "img".into(),
        command: Vec::new(),
        tty: false,
        cpu_millis: 100,
        memory_bytes: 64 * 1_024 * 1_024,
        gpu_count: 0,
        gpu_device_ids: Vec::new(),
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        required_runtime_features: Vec::new(),
        preassigned_slots: Vec::new(),
        restart_policy: None,
        termination_grace_period_secs: None,
        pre_stop_command: None,
        liveness: None,
        env: Vec::new(),
        secret_files: Vec::new(),
        volumes: Vec::new(),
        networks: Vec::new(),
        placement: Default::default(),
        owner: None,
        target_node: Some(Uuid::new_v4()),
    }];

    assert_eq!(scheduling_retry_max_attempts_for_intents(&intents), 8);
}

#[tokio::test]
async fn scheduling_retry_limit_override_fast_fails_retryable_errors() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let request = WorkloadStartRequest {
        name: "network-blocked".into(),
        execution: ResolvedExecutionSpec {
            cpu_millis: 100,
            networks: vec![Uuid::new_v4()],
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: Some(Uuid::new_v4()),
        slot_ids: Vec::new(),
        owner: Some(WorkloadOwner::ServiceReplica(WorkloadServiceMetadata::new(
            "demo-service",
            "api",
        ))),
        target_node: None,
    };

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        manager.start_workloads_batch_with_scheduling_retry_limit(vec![request], Some(1)),
    )
    .await
    .expect("override should fail without waiting for the default retry window")
    .expect_err("network-blocked start should fail");

    assert!(workload_start_error_is_retryable(&result));
}

#[tokio::test]
async fn workload_start_retryable_includes_capacity_shortage_for_controllers() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;
    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 256 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");
    let snapshot = scheduler.snapshot().await.expect("scheduler snapshot");
    scheduler
        .reserve_slots(
            snapshot.version,
            vec![SlotReservationRequest {
                slot_id: 1,
                owner: manager.local_node_id,
                task_id: Some(Uuid::new_v4()),
            }],
        )
        .await
        .expect("reserve the only slot");
    let request = WorkloadStartRequest {
        name: "capacity-blocked".into(),
        execution: ResolvedExecutionSpec {
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: Some(Uuid::new_v4()),
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let result = manager
        .start_workloads_batch(vec![request])
        .await
        .expect_err("capacity-blocked start should fail without slots");

    assert!(
        workload_start_error_is_retryable(&result),
        "higher-level controllers should keep workloads pending while capacity drains"
    );

    let cause = result
        .chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .expect("capacity scheduling error");
    assert!(
        matches!(
            cause,
            SchedulingError::NoCapacityAcrossCluster
                | SchedulingError::InsufficientCapacityForBatch
        ),
        "fully reserved local capacity should surface a concrete capacity scheduling error"
    );
}

#[tokio::test]
async fn workload_start_service_requeue_excludes_capacity_shortage() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;
    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 256 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");
    let snapshot = scheduler.snapshot().await.expect("scheduler snapshot");
    scheduler
        .reserve_slots(
            snapshot.version,
            vec![SlotReservationRequest {
                slot_id: 1,
                owner: manager.local_node_id,
                task_id: Some(Uuid::new_v4()),
            }],
        )
        .await
        .expect("reserve the only slot");
    let request = WorkloadStartRequest {
        name: "capacity-blocked".into(),
        execution: ResolvedExecutionSpec {
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::Oci,
        isolation_mode: crate::workload::model::IsolationMode::Standard,
        isolation_profile: None,
        gpu_device_ids: Vec::new(),
        id: Some(Uuid::new_v4()),
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let result = manager
        .start_workloads_batch(vec![request])
        .await
        .expect_err("capacity-blocked start should fail without slots");

    assert!(
        !workload_start_error_requires_service_requeue(&result),
        "services should consume rollout failure budget for pure capacity starvation"
    );
}

#[tokio::test]
async fn start_tasks_batch_rejects_unsupported_local_execution_platform() {
    let (manager, scheduler, _mock_cm, _network_registry) = setup_manager().await;
    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");
    let request = WorkloadStartRequest {
        name: "microvm-task".into(),
        execution: ResolvedExecutionSpec {
            cpu_millis: 100,
            memory_bytes: 64 * 1_024 * 1_024,
            ..empty_resolved_execution("img")
        },
        execution_platform: ExecutionPlatform::MicroVm,
        isolation_mode: crate::workload::model::IsolationMode::Sandboxed,
        isolation_profile: Some("vm-default".into()),
        gpu_device_ids: Vec::new(),
        id: Some(Uuid::new_v4()),
        slot_ids: Vec::new(),
        owner: None,
        target_node: None,
    };

    let result = manager
        .start_workloads_batch(vec![request])
        .await
        .expect_err("unsupported local runtime class should fail");

    assert!(
        !workload_start_error_is_retryable(&result),
        "runtime requirement failures should not enter the retry loop"
    );

    let cause = result
        .chain()
        .find_map(|cause| cause.downcast_ref::<SchedulingError>())
        .expect("runtime scheduling error");
    match cause {
        SchedulingError::RuntimeRequirementsBlocked {
            task,
            execution_platform,
            isolation_mode,
            isolation_profile,
            feature_flags,
        } => {
            assert_eq!(task, "microvm-task");
            assert_eq!(*execution_platform, "microvm");
            assert_eq!(*isolation_mode, "sandboxed");
            assert_eq!(isolation_profile.as_deref(), Some("vm-default"));
            assert!(
                feature_flags.is_empty(),
                "plain runtime-class mismatch should not invent extra feature requirements"
            );
        }
        other => panic!("unexpected scheduling error: {other:?}"),
    }
}

#[tokio::test]
async fn remote_prepare_feedback_records_and_clears_retryable_peer_backoff() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let peer_id = Uuid::new_v4();

    manager
        .local_state
        .remote_prepare_feedback
        .record_retryable_failure(peer_id);
    let first = manager.local_state.remote_prepare_feedback.snapshot();
    let first_feedback = first
        .get(&peer_id)
        .expect("peer feedback after first failure");
    let first_reject_until = manager
        .local_state
        .remote_prepare_feedback
        .reject_until(peer_id)
        .expect("peer backoff deadline after first failure");
    assert_eq!(first_feedback.consecutive_failures, 1);

    manager
        .local_state
        .remote_prepare_feedback
        .record_retryable_failure(peer_id);
    let second = manager.local_state.remote_prepare_feedback.snapshot();
    let second_feedback = second
        .get(&peer_id)
        .expect("peer feedback after second failure");
    let second_reject_until = manager
        .local_state
        .remote_prepare_feedback
        .reject_until(peer_id)
        .expect("peer backoff deadline after second failure");
    assert_eq!(second_feedback.consecutive_failures, 2);
    assert!(
        second_reject_until > first_reject_until,
        "later retryable failures should extend peer backoff"
    );

    manager
        .local_state
        .remote_prepare_feedback
        .clear_success(peer_id);
    assert!(
        !manager
            .local_state
            .remote_prepare_feedback
            .snapshot()
            .contains_key(&peer_id),
        "successful prepare should clear peer backoff immediately"
    );
}

#[tokio::test]
async fn remote_prepare_rejection_updates_digest_cache_and_backoff() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let peer_id = Uuid::new_v4();
    let rejection = RemotePrepareRejection {
        reason: RemotePrepareRejectionReason::InsufficientResources,
        digest: crate::scheduler::digest::SchedulerDigestValue {
            node_id: peer_id,
            snapshot_version: 7,
            updated_at_unix_ms: 123_456,
            free_slot_count: 1,
            free_cpu_millis: 500,
            free_memory_bytes: 512 * 1024 * 1024,
            largest_free_slot_cpu_millis: 500,
            largest_free_slot_memory_bytes: 512 * 1024 * 1024,
            free_gpu_count: 0,
            gpu_runtime_ready: true,
        },
    };

    manager
        .apply_remote_prepare_rejection(peer_id, rejection.clone())
        .await
        .expect("apply remote prepare rejection");

    let feedback = manager.local_state.remote_prepare_feedback.snapshot();
    let peer_feedback = feedback
        .get(&peer_id)
        .expect("peer feedback after rejection");
    assert_eq!(peer_feedback.consecutive_failures, 1);

    let digest = manager
        .core
        .scheduler
        .scheduler_digests()
        .expect("load scheduler digests")
        .into_iter()
        .find(|digest| digest.node_id == peer_id)
        .expect("rejection digest cached locally");
    assert_eq!(digest, rejection.digest);
}

#[tokio::test]
async fn local_volume_wait_for_first_consumer_binds_on_first_start() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let volume = create_managed_local_volume(
        &manager,
        "local-data",
        VolumeBindingMode::WaitForFirstConsumer,
        None,
        None,
    )
    .await;

    let mut started = manager
        .start_workloads_batch(vec![standalone_volume_task_request(
            &volume,
            "/var/lib/data",
        )])
        .await
        .expect("start volume-backed task");
    let spec = started.pop().expect("started task");

    let bound = manager
        .volumes
        .volume_registry
        .get_spec(volume.id)
        .expect("load bound volume")
        .expect("volume should exist");
    assert_eq!(bound.bound_node_id, Some(manager.local_node_id));
    assert_eq!(
        bound.bound_node_name.as_deref(),
        Some(manager.local_node_name.as_str())
    );
    assert!(
        matches!(
            bound.status,
            VolumeStatus::Bound | VolumeStatus::Ready | VolumeStatus::InUse
        ),
        "volume should be durably bound before publication, got {:?}",
        bound.status
    );

    let node_state = manager
        .volumes
        .volume_registry
        .get_node_state(volume.id, manager.local_node_id)
        .expect("load local volume node state")
        .expect("node-local volume state should exist");
    let expected_path = managed_volume_data_path(&manager.volumes.local_volume_root, volume.id);
    assert_eq!(
        node_state.local_path.as_deref(),
        Some(expected_path.to_string_lossy().as_ref())
    );
    assert_eq!(node_state.published_task_ids, vec![spec.id]);
    assert!(
        matches!(node_state.state, VolumeNodeState::Published),
        "volume node state should be published while the task is running, got {:?}",
        node_state.state
    );
    assert!(
        expected_path.is_dir(),
        "managed local volume path should exist at {}",
        expected_path.display()
    );

    let volume_mounts = mock_cm.volume_mounts.lock().await.clone();
    assert_eq!(volume_mounts.len(), 1, "expected one container create call");
    assert_eq!(
        volume_mounts[0],
        vec![format!("{}:/var/lib/data:rw", expected_path.display())]
    );
}

#[tokio::test]
async fn task_restart_preserves_local_volume_mount() {
    let (manager, scheduler, mock_cm, _network_registry) = setup_manager().await;

    scheduler
        .init_slots(vec![SlotSpec::new(
            1,
            SlotCapacity::new(500, 128 * 1_024 * 1_024, 0),
        )])
        .await
        .expect("init slots");

    let volume = create_managed_local_volume(
        &manager,
        "restart-data",
        VolumeBindingMode::WaitForFirstConsumer,
        None,
        None,
    )
    .await;

    let mut started = manager
        .start_workloads_batch(vec![standalone_volume_task_request(&volume, "/srv/data")])
        .await
        .expect("start initial volume-backed task");
    let spec = started.pop().expect("started task");

    let initial_mounts = mock_cm.volume_mounts.lock().await.clone();
    assert_eq!(
        initial_mounts.len(),
        1,
        "expected first launch to record mounts"
    );

    {
        let mut inspect = mock_cm.inspect.lock().await;
        inspect.clear();
    }

    manager
        .reconcile_local_task(spec.clone())
        .await
        .expect("reconcile should recreate the missing runtime");

    let volume_mounts = mock_cm.volume_mounts.lock().await.clone();
    assert_eq!(
        volume_mounts.len(),
        2,
        "expected the task runtime to relaunch"
    );
    assert_eq!(
        volume_mounts[0], volume_mounts[1],
        "restarted task should reuse the same local volume mount"
    );

    let node_state = manager
        .volumes
        .volume_registry
        .get_node_state(volume.id, manager.local_node_id)
        .expect("load volume node state after restart")
        .expect("volume node state should exist after restart");
    assert_eq!(
        node_state.local_path.as_deref(),
        Some(
            managed_volume_data_path(&manager.volumes.local_volume_root, volume.id)
                .to_string_lossy()
                .as_ref()
        )
    );
    assert_eq!(
        node_state.published_task_ids,
        vec![spec.id],
        "restart should preserve the same published task consumer"
    );
}

#[tokio::test]
async fn multi_volume_bound_node_conflict_rejected() {
    let (manager, _scheduler, _mock_cm, _network_registry) = setup_manager().await;
    let other_node = Uuid::new_v4();

    let left = create_managed_local_volume(
        &manager,
        "left-volume",
        VolumeBindingMode::Immediate,
        Some(manager.local_node_id),
        Some("local-node"),
    )
    .await;
    let right = create_managed_local_volume(
        &manager,
        "right-volume",
        VolumeBindingMode::Immediate,
        Some(other_node),
        Some("remote-node"),
    )
    .await;

    let err = manager
        .start_workloads_batch(vec![WorkloadStartRequest {
            name: "conflict".into(),
            execution: ResolvedExecutionSpec {
                cpu_millis: 100,
                volumes: vec![
                    crate::task::types::TaskVolumeMount {
                        volume_id: left.id,
                        volume_name: left.name.clone(),
                        target: "/left".into(),
                        read_only: false,
                    },
                    crate::task::types::TaskVolumeMount {
                        volume_id: right.id,
                        volume_name: right.name.clone(),
                        target: "/right".into(),
                        read_only: false,
                    },
                ],
                ..empty_resolved_execution("img")
            },
            execution_platform: ExecutionPlatform::Oci,
            isolation_mode: crate::workload::model::IsolationMode::Standard,
            isolation_profile: None,
            gpu_device_ids: Vec::new(),
            id: None,
            slot_ids: Vec::new(),
            owner: None,
            target_node: None,
        }])
        .await
        .expect_err("scheduler should reject tasks whose mounted volumes disagree on locality");

    assert!(
        err.to_string()
            .contains("references volumes bound to different nodes"),
        "unexpected error text: {err:#}"
    );
}
