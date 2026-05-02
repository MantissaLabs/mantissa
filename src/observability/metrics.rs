use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mantissa_protocol::sync::Domain;
use metrics::{Unit, counter, describe_counter, describe_gauge, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

use crate::cluster::{ClusterViewState, RootSchemaState};
use crate::config::RuntimeMetricsConfig;
use crate::network::nodeport::{NodePortManager, NodePortRuntimeState, NodePortStatus};
use crate::registry::Registry;
use crate::runtime::types::RuntimeError;
use crate::scheduler::{GpuDeviceState, Scheduler, SchedulerError, SchedulerSnapshot, SlotState};
use crate::store::registry::{REPLICATED_DOMAINS, domain_label};
use crate::sync::SyncGcProgress;

/// Runtime handles used by the low-cost metrics sampler.
pub struct MetricsSamplerInputs {
    pub scheduler: Rc<Scheduler>,
    pub registry: Registry,
    pub nodeport: NodePortManager,
    pub progress: SyncGcProgress,
    pub cluster_view: ClusterViewState,
    pub root_schema: RootSchemaState,
    pub state_db_path: PathBuf,
}

/// Installs the Prometheus exporter and spawns the cheap metrics sampler.
pub fn spawn_metrics(
    config: RuntimeMetricsConfig,
    inputs: MetricsSamplerInputs,
) -> Option<JoinHandle<()>> {
    if !config.enabled {
        return None;
    }

    if let Err(error) = PrometheusBuilder::new()
        .with_http_listener(config.listen_addr)
        .install()
    {
        warn!(
            target: "metrics",
            listen_addr = %config.listen_addr,
            "failed to start Prometheus metrics exporter: {error}"
        );
        return None;
    }

    describe_metrics();
    record_build_info();
    info!(
        target: "metrics",
        listen_addr = %config.listen_addr,
        "Prometheus metrics exporter listening"
    );

    Some(tokio::task::spawn_local(async move {
        MetricsSampler::new(config, inputs).run().await;
    }))
}

/// Records one inbound gossip drop using a bounded reason label.
pub fn record_gossip_drop(reason: &'static str) {
    counter!("mantissa_gossip_dropped_total", "reason" => reason).increment(1);
}

/// Records one outbound gossip send failure using a bounded reason label.
pub fn record_gossip_send_failure(reason: &'static str) {
    counter!("mantissa_gossip_send_failures_total", "reason" => reason).increment(1);
}

/// Updates aggregate outbound gossip backlog gauges.
pub fn set_gossip_backlog(pending: usize, oldest_age: Duration) {
    gauge!("mantissa_gossip_outbound_pending").set(pending as f64);
    gauge!("mantissa_gossip_outbound_oldest_age_seconds").set(oldest_age.as_secs_f64());
}

/// Records one sync attempt outcome.
pub fn record_sync_attempt(scope: &'static str, result: &'static str, reason: &'static str) {
    counter!(
        "mantissa_sync_attempts_total",
        "scope" => scope,
        "result" => result,
        "reason" => reason
    )
    .increment(1);
}

/// Updates the selected-peer gauge for one sync scope.
pub fn set_sync_selected_peers(scope: &'static str, count: usize) {
    gauge!("mantissa_sync_selected_peers", "scope" => scope).set(count as f64);
}

/// Records one scheduler lease prepare outcome.
pub fn record_scheduler_prepare(result: &'static str, reason: &'static str) {
    counter!(
        "mantissa_scheduler_lease_prepare_total",
        "scope" => "local",
        "result" => result,
        "reason" => reason
    )
    .increment(1);
}

/// Records how many expired prepared leases were reaped.
pub fn record_scheduler_expired_leases_reaped(count: usize) {
    if count > 0 {
        counter!("mantissa_scheduler_expired_leases_reaped_total").increment(count as u64);
    }
}

/// Maps a scheduler error into one bounded metrics reason.
pub fn scheduler_error_reason(error: &SchedulerError) -> &'static str {
    match error {
        SchedulerError::Store(_) => "store_error",
        SchedulerError::AlreadyInitialized { .. } => "already_initialized",
        SchedulerError::Uninitialized => "uninitialized",
        SchedulerError::SnapshotMismatch { .. } => "snapshot_mismatch",
        SchedulerError::DuplicateSlots { .. } => "duplicate_slots",
        SchedulerError::DuplicateGpuDevices { .. } => "duplicate_gpus",
        SchedulerError::UnknownSlots { .. } => "unknown_slots",
        SchedulerError::UnknownGpuDevices { .. } => "unknown_gpus",
        SchedulerError::SlotsUnavailable { .. } => "slots_unavailable",
        SchedulerError::GpuDevicesUnavailable { .. } => "gpus_unavailable",
        SchedulerError::InsufficientResources { .. } => "insufficient_resources",
        SchedulerError::UnknownLeases { .. } => "unknown_leases",
        SchedulerError::ExpiredLeases { .. } => "expired_leases",
        SchedulerError::LeaseMismatch { .. } => "lease_mismatch",
        SchedulerError::SlotsNotReserved { .. } => "slots_not_reserved",
        SchedulerError::GpuDevicesNotReserved { .. } => "gpus_not_reserved",
        SchedulerError::SnapshotVersionOverflow { .. } => "snapshot_version_overflow",
    }
}

/// Records one store GC pass outcome.
pub fn record_store_gc_run(result: &'static str) {
    counter!("mantissa_store_gc_runs_total", "result" => result).increment(1);
}

/// Updates the duration of the most recent store GC pass.
pub fn set_store_gc_last_duration(duration: Duration) {
    gauge!("mantissa_store_gc_last_duration_seconds").set(duration.as_secs_f64());
}

/// Records safe tombstone pruning progress for one replicated domain.
pub fn record_store_gc_tombstones_pruned(domain: Domain, count: usize) {
    if count > 0 {
        counter!(
            "mantissa_store_gc_tombstones_pruned_total",
            "domain" => metrics_domain_label(domain)
        )
        .increment(count as u64);
    }
}

/// Records MVReg compaction progress for one replicated domain.
pub fn record_store_gc_registers_compacted(domain: Domain, count: usize) {
    if count > 0 {
        counter!(
            "mantissa_store_gc_registers_compacted_total",
            "domain" => metrics_domain_label(domain)
        )
        .increment(count as u64);
    }
}

/// Records one store GC domain skip.
pub fn record_store_gc_skipped_domain(domain: Domain, reason: &'static str) {
    counter!(
        "mantissa_store_gc_skipped_domains_total",
        "domain" => metrics_domain_label(domain),
        "reason" => reason
    )
    .increment(1);
}

/// Records one runtime backend failure.
pub fn record_runtime_failure(operation: &'static str, error: &RuntimeError) {
    counter!(
        "mantissa_runtime_failures_total",
        "operation" => operation,
        "reason" => runtime_error_reason(error)
    )
    .increment(1);
}

/// Maps one runtime backend error into a bounded metrics reason.
pub fn runtime_error_reason(error: &RuntimeError) -> &'static str {
    match error {
        RuntimeError::Backend {
            status_code: Some(code),
            ..
        } if *code == 404 => "not_found",
        RuntimeError::Backend {
            status_code: Some(code),
            ..
        } if *code >= 500 => "backend_5xx",
        RuntimeError::Backend {
            status_code: Some(_),
            ..
        } => "backend_error",
        RuntimeError::Backend {
            status_code: None, ..
        } => "backend_error",
        RuntimeError::NotFound(_) => "not_found",
        RuntimeError::Timeout => "timeout",
        RuntimeError::OperationFailed(_) => "operation_failed",
    }
}

/// Records one runtime-observed task exit.
pub fn record_runtime_task_exit(exit_code: i32, restartable: bool) {
    let exit_class = if exit_code == 0 { "success" } else { "nonzero" };
    let restartable = if restartable { "true" } else { "false" };
    counter!(
        "mantissa_runtime_task_exits_total",
        "exit_class" => exit_class,
        "restartable" => restartable
    )
    .increment(1);
}

/// Records one Mantissa-driven runtime restart decision.
pub fn record_runtime_restart(reason: &'static str) {
    counter!("mantissa_runtime_restarts_total", "reason" => reason).increment(1);
}

/// Records one failed liveness probe.
pub fn record_liveness_probe_failure(kind: &'static str, reason: &'static str) {
    counter!(
        "mantissa_liveness_probe_failures_total",
        "kind" => kind,
        "reason" => reason
    )
    .increment(1);
}

/// Records one network reconciliation failure.
pub fn record_network_reconcile_failure(reason: &'static str) {
    counter!("mantissa_network_reconcile_failures_total", "reason" => reason).increment(1);
}

/// Records one eBPF dataplane operation failure.
pub fn record_network_bpf_failure(operation: &'static str, reason: &'static str) {
    counter!(
        "mantissa_network_bpf_failures_total",
        "operation" => operation,
        "reason" => reason
    )
    .increment(1);
}

/// Updates the WireGuard underlay state gauges.
pub fn set_wireguard_underlay(active: bool, required_peers: usize, configured_peers: usize) {
    gauge!("mantissa_wireguard_underlay_active").set(if active { 1.0 } else { 0.0 });
    gauge!("mantissa_wireguard_underlay_peers", "state" => "required").set(required_peers as f64);
    gauge!("mantissa_wireguard_underlay_peers", "state" => "configured")
        .set(configured_peers as f64);
}

/// Records one authentication or session failure.
pub fn record_auth_failure(stage: &'static str, reason: &'static str) {
    counter!(
        "mantissa_auth_failures_total",
        "stage" => stage,
        "reason" => reason
    )
    .increment(1);
}

/// Records one server-side session ticket lifecycle event.
pub fn record_session_ticket_event(event: &'static str) {
    counter!("mantissa_auth_session_ticket_events_total", "event" => event).increment(1);
}

/// Records multiple server-side session ticket lifecycle events.
pub fn record_session_ticket_events(event: &'static str, count: usize) {
    if count > 0 {
        counter!("mantissa_auth_session_ticket_events_total", "event" => event)
            .increment(count as u64);
    }
}

/// Renders one Cap'n Proto transport error into a bounded reason.
pub fn capnp_error_reason(error: &capnp::Error) -> &'static str {
    let text = error.to_string();
    if text.contains("Disconnected") || text.contains("disconnected") {
        "disconnected"
    } else if text.contains("timeout") || text.contains("timed out") {
        "timeout"
    } else {
        "rpc_error"
    }
}

struct MetricsSampler {
    config: RuntimeMetricsConfig,
    inputs: MetricsSamplerInputs,
}

impl MetricsSampler {
    /// Builds a low-impact sampler from runtime handles.
    fn new(config: RuntimeMetricsConfig, inputs: MetricsSamplerInputs) -> Self {
        Self { config, inputs }
    }

    /// Runs cheap runtime sampling until the task is aborted during shutdown.
    async fn run(self) {
        self.sample_runtime().await;
        self.sample_state_db();

        let mut runtime_interval = time::interval(self.config.sample_interval);
        runtime_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut state_db_interval = time::interval(self.config.state_db_sample_interval);
        state_db_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = runtime_interval.tick() => {
                    self.sample_runtime().await;
                }
                _ = state_db_interval.tick() => {
                    self.sample_state_db();
                }
            }
        }
    }

    /// Samples cheap gauges from in-memory runtime state.
    async fn sample_runtime(&self) {
        if let Some(snapshot) = self.inputs.scheduler.snapshot().await {
            record_scheduler_snapshot(&snapshot);
        } else {
            record_scheduler_empty();
        }

        record_nodeport_status(self.inputs.nodeport.status().await);
        self.sample_sync_progress();
    }

    /// Samples the durable Redb file size using filesystem metadata only.
    fn sample_state_db(&self) {
        match std::fs::metadata(&self.inputs.state_db_path) {
            Ok(metadata) => {
                gauge!("mantissa_state_db_size_bytes").set(metadata.len() as f64);
            }
            Err(error) => {
                warn!(
                    target: "metrics",
                    path = %self.inputs.state_db_path.display(),
                    "failed to sample state database size: {error}"
                );
            }
        }
    }

    /// Samples aggregate sync GC barrier gauges.
    fn sample_sync_progress(&self) {
        let cluster_view = self.inputs.cluster_view.active_view();
        let root_schema_version = self.inputs.root_schema.supported_version();
        let now_unix_ms = now_unix_ms();
        let active_remote_peers = match self.inputs.registry.known_peers() {
            Ok(peers) => peers,
            Err(error) => {
                warn!(target: "metrics", "failed to sample active peers for sync metrics: {error}");
                return;
            }
        };

        for domain in REPLICATED_DOMAINS {
            let label = metrics_domain_label(domain);
            match self.inputs.progress.barrier_for_domain(
                active_remote_peers.iter().copied(),
                domain,
                cluster_view,
                root_schema_version,
                now_unix_ms,
            ) {
                Some(barrier) => {
                    let barrier_age_ms =
                        now_unix_ms.saturating_sub(barrier.safe_observed_before_unix_ms);
                    gauge!("mantissa_sync_gc_barrier_available", "domain" => label).set(1.0);
                    gauge!("mantissa_sync_gc_barrier_age_seconds", "domain" => label)
                        .set(Duration::from_millis(barrier_age_ms).as_secs_f64());
                }
                None => {
                    gauge!("mantissa_sync_gc_barrier_available", "domain" => label).set(0.0);
                    gauge!("mantissa_sync_gc_barrier_age_seconds", "domain" => label).set(0.0);
                }
            }
        }
    }
}

/// Registers descriptions for the low-impact production metrics.
fn describe_metrics() {
    describe_gauge!(
        "mantissa_info",
        Unit::Count,
        "Static Mantissa binary information."
    );
    describe_counter!(
        "mantissa_gossip_dropped_total",
        Unit::Count,
        "Inbound gossip messages dropped before domain handling."
    );
    describe_counter!(
        "mantissa_gossip_send_failures_total",
        Unit::Count,
        "Outbound gossip send failures."
    );
    describe_gauge!(
        "mantissa_gossip_outbound_pending",
        Unit::Count,
        "Pending outbound gossip messages at the latest gossip tick."
    );
    describe_gauge!(
        "mantissa_gossip_outbound_oldest_age_seconds",
        Unit::Seconds,
        "Age of the oldest outbound gossip message at the latest gossip tick."
    );
    describe_counter!(
        "mantissa_sync_attempts_total",
        Unit::Count,
        "Periodic anti-entropy sync attempts."
    );
    describe_gauge!(
        "mantissa_sync_selected_peers",
        Unit::Count,
        "Peers selected for the latest sync tick."
    );
    describe_gauge!(
        "mantissa_sync_gc_barrier_age_seconds",
        Unit::Seconds,
        "Age of the oldest equal-root observation used by the current sync GC barrier."
    );
    describe_gauge!(
        "mantissa_sync_gc_barrier_available",
        Unit::Count,
        "Whether sync has enough progress to build a GC barrier by domain."
    );
    describe_gauge!(
        "mantissa_scheduler_slots",
        Unit::Count,
        "Local scheduler slot count by state."
    );
    describe_gauge!(
        "mantissa_scheduler_gpus",
        Unit::Count,
        "Local scheduler GPU device count by state."
    );
    describe_counter!(
        "mantissa_scheduler_lease_prepare_total",
        Unit::Count,
        "Local scheduler lease prepare outcomes."
    );
    describe_counter!(
        "mantissa_scheduler_expired_leases_reaped_total",
        Unit::Count,
        "Expired prepared scheduler leases reclaimed."
    );
    describe_counter!(
        "mantissa_store_gc_runs_total",
        Unit::Count,
        "Store GC pass outcomes."
    );
    describe_gauge!(
        "mantissa_store_gc_last_duration_seconds",
        Unit::Seconds,
        "Duration of the latest store GC pass."
    );
    describe_counter!(
        "mantissa_store_gc_tombstones_pruned_total",
        Unit::Count,
        "Tombstone rows pruned by store GC."
    );
    describe_counter!(
        "mantissa_store_gc_registers_compacted_total",
        Unit::Count,
        "MVReg rows compacted by store GC."
    );
    describe_counter!(
        "mantissa_store_gc_skipped_domains_total",
        Unit::Count,
        "Store GC domain skips by reason."
    );
    describe_counter!(
        "mantissa_runtime_failures_total",
        Unit::Count,
        "Runtime backend operation failures."
    );
    describe_counter!(
        "mantissa_runtime_task_exits_total",
        Unit::Count,
        "Runtime-observed task exits."
    );
    describe_counter!(
        "mantissa_runtime_restarts_total",
        Unit::Count,
        "Mantissa-driven task runtime restart decisions."
    );
    describe_counter!(
        "mantissa_liveness_probe_failures_total",
        Unit::Count,
        "Failed task liveness probes."
    );
    describe_counter!(
        "mantissa_network_reconcile_failures_total",
        Unit::Count,
        "Network reconciliation failures."
    );
    describe_counter!(
        "mantissa_network_bpf_failures_total",
        Unit::Count,
        "eBPF dataplane operation failures."
    );
    describe_gauge!(
        "mantissa_nodeport_state",
        Unit::Count,
        "Current NodePort runtime state."
    );
    describe_gauge!(
        "mantissa_wireguard_underlay_active",
        Unit::Count,
        "Whether WireGuard underlay is active."
    );
    describe_gauge!(
        "mantissa_wireguard_underlay_peers",
        Unit::Count,
        "WireGuard peer counts by state."
    );
    describe_counter!(
        "mantissa_auth_failures_total",
        Unit::Count,
        "Authentication and session failures."
    );
    describe_counter!(
        "mantissa_auth_session_ticket_events_total",
        Unit::Count,
        "Session ticket lifecycle events."
    );
    describe_gauge!(
        "mantissa_state_db_size_bytes",
        Unit::Bytes,
        "Local Redb state database file size."
    );
}

/// Records static binary information for scrape discovery.
fn record_build_info() {
    let git_sha = option_env!("MANTISSA_GIT_SHA").unwrap_or("unknown");
    gauge!(
        "mantissa_info",
        "version" => env!("CARGO_PKG_VERSION"),
        "git_sha" => git_sha
    )
    .set(1.0);
}

/// Records local scheduler capacity gauges from one snapshot.
fn record_scheduler_snapshot(snapshot: &SchedulerSnapshot) {
    let mut free_slots = 0usize;
    let mut leased_slots = 0usize;
    let mut reserved_slots = 0usize;
    for slot in &snapshot.slots {
        match slot.state {
            SlotState::Free => free_slots = free_slots.saturating_add(1),
            SlotState::Leased(_) => leased_slots = leased_slots.saturating_add(1),
            SlotState::Reserved(_) => reserved_slots = reserved_slots.saturating_add(1),
        }
    }

    let mut free_gpus = 0usize;
    let mut leased_gpus = 0usize;
    let mut reserved_gpus = 0usize;
    for gpu in &snapshot.gpu_devices {
        match gpu.state {
            GpuDeviceState::Free => free_gpus = free_gpus.saturating_add(1),
            GpuDeviceState::Leased(_) => leased_gpus = leased_gpus.saturating_add(1),
            GpuDeviceState::Reserved(_) => reserved_gpus = reserved_gpus.saturating_add(1),
        }
    }

    set_scheduler_capacity(
        "mantissa_scheduler_slots",
        snapshot.slots.len(),
        free_slots,
        leased_slots,
        reserved_slots,
    );
    set_scheduler_capacity(
        "mantissa_scheduler_gpus",
        snapshot.gpu_devices.len(),
        free_gpus,
        leased_gpus,
        reserved_gpus,
    );
}

/// Clears scheduler capacity gauges when the scheduler has no local snapshot.
fn record_scheduler_empty() {
    set_scheduler_capacity("mantissa_scheduler_slots", 0, 0, 0, 0);
    set_scheduler_capacity("mantissa_scheduler_gpus", 0, 0, 0, 0);
}

/// Writes one scheduler capacity metric family with stable state labels.
fn set_scheduler_capacity(
    name: &'static str,
    total: usize,
    free: usize,
    leased: usize,
    reserved: usize,
) {
    gauge!(name, "state" => "total").set(total as f64);
    gauge!(name, "state" => "free").set(free as f64);
    gauge!(name, "state" => "leased").set(leased as f64);
    gauge!(name, "state" => "reserved").set(reserved as f64);
}

/// Records the current NodePort runtime state using one-hot gauges.
fn record_nodeport_status(status: NodePortStatus) {
    for state in [
        NodePortRuntimeState::Disabled,
        NodePortRuntimeState::Pending,
        NodePortRuntimeState::Ready,
        NodePortRuntimeState::Degraded,
    ] {
        let value = if status.state == state { 1.0 } else { 0.0 };
        gauge!("mantissa_nodeport_state", "state" => nodeport_state_label(state)).set(value);
    }
}

/// Returns one stable metrics label for a NodePort runtime state.
fn nodeport_state_label(state: NodePortRuntimeState) -> &'static str {
    match state {
        NodePortRuntimeState::Disabled => "disabled",
        NodePortRuntimeState::Pending => "pending",
        NodePortRuntimeState::Ready => "ready",
        NodePortRuntimeState::Degraded => "degraded",
    }
}

/// Returns one stable metrics label for a sync domain.
fn metrics_domain_label(domain: Domain) -> &'static str {
    match domain {
        Domain::NetworkPeers => "network_peers",
        Domain::NetworkAttachments => "network_attachments",
        Domain::ClusterViews => "cluster_views",
        Domain::VolumeNodes => "volume_nodes",
        Domain::SchedulerDigests => "scheduler_digests",
        _ => domain_label(domain),
    }
}

/// Returns the current local wall-clock time as Unix milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_errors_map_to_bounded_reasons() {
        let err = SchedulerError::Uninitialized;
        assert_eq!(scheduler_error_reason(&err), "uninitialized");

        let err = SchedulerError::SnapshotVersionOverflow {
            snapshot: SchedulerSnapshot {
                version: 0,
                slots: Vec::new(),
                gpu_devices: Vec::new(),
            },
        };
        assert_eq!(scheduler_error_reason(&err), "snapshot_version_overflow");
    }

    #[test]
    fn runtime_errors_map_to_bounded_reasons() {
        assert_eq!(runtime_error_reason(&RuntimeError::Timeout), "timeout");
        assert_eq!(
            runtime_error_reason(&RuntimeError::backend(Some(500), "boom")),
            "backend_5xx"
        );
        assert_eq!(
            runtime_error_reason(&RuntimeError::NotFound("missing".to_string())),
            "not_found"
        );
    }

    #[test]
    fn metrics_render_with_local_recorder() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            describe_metrics();
            record_build_info();
            record_gossip_drop("duplicate");
            set_gossip_backlog(2, Duration::from_secs(3));
        });

        let rendered = handle.render();
        assert!(rendered.contains("mantissa_info"));
        assert!(rendered.contains("mantissa_gossip_dropped_total"));
        assert!(rendered.contains("mantissa_gossip_outbound_pending"));
    }
}
