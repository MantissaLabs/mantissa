#[macro_use]
mod common;

use async_trait::async_trait;
use capnp_rpc::new_client as capnp_new_client;
use chrono::Utc;
use common::testkit::{ContainerManagerOverrideGuard, TestNode};
use crdt_store::uuid_key::UuidKey;
use mantissa::task::container::ContainerState;
use mantissa::task::docker::{
    ContainerAttachOptions, ContainerCreateRequest, ContainerError, ContainerInfo,
    ContainerLogFrame, ContainerLogStream, ContainerManager,
};
use mantissa::task::types::{TaskValue, TaskValueDraft};
use protocol::task::task_log_sink;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use uuid::Uuid;

type AttachCall = (String, ContainerAttachOptions);
type CapturedTaskFrames = Arc<AsyncMutex<Vec<(String, Vec<u8>)>>>;

#[derive(Clone, Default)]
struct StaticAttachContainerManager {
    frames: Arc<AsyncMutex<HashMap<String, Vec<ContainerLogFrame>>>>,
    calls: Arc<AsyncMutex<Vec<AttachCall>>>,
    inputs: Arc<AsyncMutex<HashMap<String, Vec<Vec<u8>>>>>,
}

#[async_trait]
impl ContainerManager for StaticAttachContainerManager {
    async fn create_container(
        &self,
        _request: ContainerCreateRequest,
    ) -> Result<String, ContainerError> {
        Ok(Uuid::new_v4().to_string())
    }

    async fn start_container(&self, _container_id: &str) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn stop_container(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn restart_container(
        &self,
        _container_id: &str,
        _timeout: Option<Duration>,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn remove_container(
        &self,
        _container_id: &str,
        _force: bool,
        _remove_volumes: bool,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn list_containers(
        &self,
        _filters: Option<HashMap<String, Vec<String>>>,
    ) -> Result<Vec<ContainerInfo>, ContainerError> {
        Ok(Vec::new())
    }

    async fn inspect_container(
        &self,
        container_id: &str,
    ) -> Result<bollard::service::ContainerInspectResponse, ContainerError> {
        let exists = self.frames.lock().await.contains_key(container_id);
        if !exists {
            return Err(ContainerError::NotFound(container_id.to_string()));
        }

        Ok(bollard::service::ContainerInspectResponse {
            state: Some(bollard::models::ContainerState {
                running: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn pull_image(&self, _image: &str) -> Result<(), ContainerError> {
        Ok(())
    }

    async fn attach_container(
        &self,
        container_id: &str,
        options: &ContainerAttachOptions,
        output_tx: tokio::sync::mpsc::Sender<ContainerLogFrame>,
        mut input_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Result<(), ContainerError> {
        self.calls
            .lock()
            .await
            .push((container_id.to_string(), options.clone()));

        let frames = self
            .frames
            .lock()
            .await
            .get(container_id)
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
        self.inputs
            .lock()
            .await
            .insert(container_id.to_string(), chunks);
        Ok(())
    }
}

#[derive(Clone)]
struct CollectingTaskAttachSink {
    frames: CapturedTaskFrames,
    ended: Arc<Notify>,
}

impl Default for CollectingTaskAttachSink {
    fn default() -> Self {
        Self {
            frames: Arc::new(AsyncMutex::new(Vec::new())),
            ended: Arc::new(Notify::new()),
        }
    }
}

impl task_log_sink::Server for CollectingTaskAttachSink {
    async fn push_frame(
        self: std::rc::Rc<Self>,
        params: task_log_sink::PushFrameParams,
    ) -> Result<(), capnp::Error> {
        let frame = params.get()?.get_frame()?;
        let stream = frame
            .get_stream()
            .map_err(|_| capnp::Error::failed("unknown task log stream".into()))?;
        let bytes = frame.get_data()?.to_owned();
        let label = match stream {
            protocol::task::TaskLogStream::Stdout => "stdout",
            protocol::task::TaskLogStream::Stderr => "stderr",
            protocol::task::TaskLogStream::Console => "console",
        };
        self.frames
            .lock()
            .await
            .push((label.to_string(), bytes.as_slice().to_vec()));
        Ok(())
    }

    async fn end(
        self: std::rc::Rc<Self>,
        _params: task_log_sink::EndParams,
        _results: task_log_sink::EndResults,
    ) -> Result<(), capnp::Error> {
        self.ended.notify_one();
        Ok(())
    }
}

/// Builds a stable replicated task value owned by `owner_id` for RPC relay tests.
fn replicated_task_value(task_id: Uuid, owner_id: Uuid, owner_name: &str) -> TaskValue {
    TaskValue::new(TaskValueDraft {
        id: task_id,
        name: "demo-task".to_string(),
        image: "img".to_string(),
        state: ContainerState::Running,
        phase_reason: None,
        phase_progress: None,
        created_at: Utc::now().to_rfc3339(),
        updated_at: Utc::now().to_rfc3339(),
        command: vec!["/bin/demo".to_string()],
        tty: false,
        node_id: owner_id,
        node_name: owner_name.to_string(),
        slot_ids: Vec::new(),
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
        service_metadata: None,
        lease_id: None,
        lease_coordinator_node_id: None,
        task_epoch: 0,
        phase_version: 0,
        launch_attempt: 1,
        last_terminal_observed_launch: None,
    })
}

local_test!(task_attach_relay_over_tcp_sessions, {
    let owner_manager = Arc::new(StaticAttachContainerManager::default());
    let requester_manager = Arc::new(StaticAttachContainerManager::default());
    let install_index = Arc::new(AtomicUsize::new(0));
    let owner_for_factory = owner_manager.clone();
    let requester_for_factory = requester_manager.clone();
    let index_for_factory = install_index.clone();
    let _guard = ContainerManagerOverrideGuard::install_factory(Arc::new(move || {
        match index_for_factory.fetch_add(1, Ordering::SeqCst) {
            0 => owner_for_factory.clone() as Arc<dyn ContainerManager + Send + Sync>,
            1 => requester_for_factory.clone() as Arc<dyn ContainerManager + Send + Sync>,
            _ => Arc::new(StaticAttachContainerManager::default()),
        }
    }));

    let owner = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping task_attach_relay_over_tcp_sessions: {msg}");
                return;
            }
            panic!("failed to start owner node: {msg}");
        }
    };
    let requester = match TestNode::try_new_tcp_with_tick_ms(100).await {
        Ok(node) => node,
        Err(err) => {
            let msg = err.to_string();
            if msg.contains("Operation not permitted") {
                eprintln!("skipping task_attach_relay_over_tcp_sessions: {msg}");
                return;
            }
            panic!("failed to start requester node: {msg}");
        }
    };

    requester
        .join(&owner)
        .await
        .expect("join requester to owner");
    owner
        .assert_cluster_size(2, "owner should see requester")
        .await;
    requester
        .assert_cluster_size(2, "requester should see owner")
        .await;

    let task_id = Uuid::new_v4();
    let owner_value = replicated_task_value(task_id, owner.id(), "owner-node");
    owner
        .node
        .tasks
        .upsert(&UuidKey::from(task_id), owner_value.clone())
        .await
        .expect("store task on owner");
    requester
        .node
        .tasks
        .upsert(&UuidKey::from(task_id), owner_value)
        .await
        .expect("store task on requester");

    owner_manager.frames.lock().await.insert(
        format!("mantissa-{task_id}"),
        vec![
            ContainerLogFrame {
                stream: ContainerLogStream::Console,
                message: b"welcome\n".to_vec(),
            },
            ContainerLogFrame {
                stream: ContainerLogStream::StdErr,
                message: b"attached\n".to_vec(),
            },
        ],
    );

    let sink = CollectingTaskAttachSink::default();
    let sink_frames = sink.frames.clone();
    let sink_done = sink.ended.clone();
    let sink_client = capnp_new_client(sink);
    let selector = task_id
        .to_string()
        .split('-')
        .next()
        .expect("uuid prefix")
        .to_string();
    let mut request = requester.node.task_client.attach_request();
    {
        let mut builder = request.get().init_request();
        builder.set_selector(&selector);
        let mut options = builder.reborrow().init_options();
        options.set_logs(true);
        options.set_stream(true);
        options.set_stdin(true);
        options.set_stdout(true);
        options.set_stderr(true);
        options.set_detach_keys("ctrl-p,ctrl-q");
        builder.set_sink(sink_client);
    }
    let response = request
        .send()
        .promise
        .await
        .expect("attach relay request should succeed");
    let session = response
        .get()
        .expect("attach response")
        .get_session()
        .expect("attach session");

    let mut input = session.push_input_request();
    input.get().set_data(b"echo cluster attach\n");
    input.send().await.expect("send attach input");
    session
        .close_input_request()
        .send()
        .promise
        .await
        .expect("close attach input");

    if tokio::time::timeout(Duration::from_secs(5), sink_done.notified())
        .await
        .is_err()
    {
        panic!(
            "attach stream should end: calls={:?} inputs={:?} frames={:?}",
            owner_manager.calls.lock().await.clone(),
            owner_manager.inputs.lock().await.clone(),
            sink_frames.lock().await.clone(),
        );
    }

    assert_eq!(
        owner_manager.calls.lock().await.clone(),
        vec![(
            format!("mantissa-{task_id}"),
            ContainerAttachOptions {
                logs: true,
                stream: true,
                stdin: true,
                stdout: true,
                stderr: true,
                detach_keys: Some("ctrl-p,ctrl-q".to_string()),
                tty: false,
                tty_width: None,
                tty_height: None,
            }
        )]
    );
    assert_eq!(
        owner_manager
            .inputs
            .lock()
            .await
            .get(&format!("mantissa-{task_id}"))
            .cloned()
            .unwrap_or_default(),
        vec![b"echo cluster attach\n".to_vec()]
    );
    assert_eq!(
        sink_frames.lock().await.clone(),
        vec![
            ("console".to_string(), b"welcome\n".to_vec()),
            ("stderr".to_string(), b"attached\n".to_vec()),
        ]
    );
});
