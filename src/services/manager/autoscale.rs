use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use uuid::Uuid;

use crate::runtime::types::RuntimeUsageSample;
use crate::services::manager::ServiceController;
use crate::services::ownership::select_autoscale_owner;
use crate::services::types::{
    ServiceStatus, TaskTemplateAutoscaleMetricKindValue, TaskTemplateAutoscalePolicyValue,
};
use crate::workload::manager::LocalServiceRuntimeUsageSample;

/// Default time after which owner-side autoscale signals are discarded.
const AUTOSCALE_SIGNAL_TTL_SECS: u64 = 180;
/// Local runtime usage sampling interval for autoscale-enabled service replicas.
pub(super) const AUTOSCALE_LOCAL_SAMPLE_TICK_SECS: u64 = 10;
/// Minimum interval for quiet summary signals used by later downscale decisions.
const AUTOSCALE_SUMMARY_SIGNAL_MIN_SECS: u64 = 60;
/// Retention window for local runtime deltas and per-template signal state.
const AUTOSCALE_LOCAL_SAMPLE_TTL_SECS: u64 = 10 * 60;

/// Owner-directed autoscale signal emitted from one node's local service slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServiceAutoscaleSignal {
    pub service_id: Uuid,
    pub service_epoch: u64,
    pub template_name: String,
    pub node_id: Uuid,
    pub kind: ServiceAutoscaleSignalKind,
    pub reason: ServiceAutoscaleSignalReason,
    pub running_replicas: u32,
    pub ready_replicas: u32,
    pub hot_replicas: u32,
    pub cpu_requested_millis_total: u64,
    pub cpu_observed_millis_ewma: u64,
    pub memory_requested_bytes_total: u64,
    pub memory_observed_bytes_ewma: u64,
    pub observed_at_unix_ms: u64,
}

/// Cadence class for one autoscale signal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum ServiceAutoscaleSignalKind {
    Hot,
    Summary,
}

/// Primary reason one autoscale signal was emitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ServiceAutoscaleSignalReason {
    CpuHigh,
    MemoryHigh,
    Quiet,
}

/// Receiver-side outcome for one autoscale signal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServiceAutoscaleSignalReport {
    pub accepted: bool,
    pub detail: String,
}

impl ServiceAutoscaleSignalReport {
    /// Builds one accepted signal report.
    fn accepted() -> Self {
        Self {
            accepted: true,
            detail: String::new(),
        }
    }

    /// Builds one rejected signal report with a bounded human-readable reason.
    fn rejected(detail: impl Into<String>) -> Self {
        Self {
            accepted: false,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct AutoscaleSignalKey {
    service_id: Uuid,
    service_epoch: u64,
    template_name: String,
    node_id: Uuid,
    kind: ServiceAutoscaleSignalKind,
}

impl AutoscaleSignalKey {
    /// Builds the owner-side dedupe key for one autoscale signal.
    fn from_signal(signal: &ServiceAutoscaleSignal) -> Self {
        Self {
            service_id: signal.service_id,
            service_epoch: signal.service_epoch,
            template_name: signal.template_name.clone(),
            node_id: signal.node_id,
            kind: signal.kind,
        }
    }
}

/// In-memory owner-side autoscale signal cache.
///
/// Signals are soft control input. The durable service spec is updated only
/// after a later decision stage accepts a new desired replica count.
#[derive(Clone, Debug)]
pub(super) struct AutoscaleSignalStore {
    ttl: Duration,
    signals: HashMap<AutoscaleSignalKey, (ServiceAutoscaleSignal, Instant)>,
}

impl Default for AutoscaleSignalStore {
    /// Builds the production in-memory signal store.
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(AUTOSCALE_SIGNAL_TTL_SECS),
            signals: HashMap::new(),
        }
    }
}

impl AutoscaleSignalStore {
    /// Records the latest signal for one service/template/node/kind tuple.
    fn record(&mut self, signal: ServiceAutoscaleSignal, now: Instant) {
        self.prune(now);
        self.signals
            .insert(AutoscaleSignalKey::from_signal(&signal), (signal, now));
    }

    /// Removes expired soft-state signals from the owner-side cache.
    fn prune(&mut self, now: Instant) {
        self.signals
            .retain(|_, (_, received_at)| now.duration_since(*received_at) <= self.ttl);
    }

    /// Returns the number of currently retained signals after pruning.
    #[cfg(test)]
    fn len_after_prune(&mut self, now: Instant) -> usize {
        self.prune(now);
        self.signals.len()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct AutoscaleRuntimeSampleKey {
    backend_kind: String,
    runtime_id: String,
}

impl AutoscaleRuntimeSampleKey {
    /// Builds a low-cardinality local key for computing cumulative CPU deltas.
    fn from_sample(sample: &LocalServiceRuntimeUsageSample) -> Self {
        let runtime_id = if sample.usage.runtime_id.is_empty() {
            sample.replica.runtime.handle.clone()
        } else {
            sample.usage.runtime_id.clone()
        };
        Self {
            backend_kind: sample.replica.runtime.backend_kind.clone(),
            runtime_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct AutoscaleGroupKey {
    service_id: Uuid,
    service_epoch: u64,
    template_name: String,
}

impl AutoscaleGroupKey {
    /// Builds the local aggregate key for one service/template generation.
    fn new(service_id: Uuid, service_epoch: u64, template_name: impl Into<String>) -> Self {
        Self {
            service_id,
            service_epoch,
            template_name: template_name.into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AutoscaleLocalGroupState {
    cpu_observed_millis_ewma: u64,
    memory_observed_bytes_ewma: u64,
    hot_windows: u32,
    last_hot_signal_at: Option<Instant>,
    last_summary_signal_at: Option<Instant>,
    last_seen_at: Option<Instant>,
}

#[derive(Clone, Debug)]
struct AutoscaleSignalAccumulator {
    key: AutoscaleGroupKey,
    policy: TaskTemplateAutoscalePolicyValue,
    running_replicas: u32,
    hot_replicas: u32,
    cpu_hot_replicas: u32,
    memory_hot_replicas: u32,
    cpu_requested_millis_total: u64,
    cpu_observed_millis_total: u64,
    memory_requested_bytes_total: u64,
    memory_observed_bytes_total: u64,
    observed_at_unix_ms: u64,
}

impl AutoscaleSignalAccumulator {
    /// Starts a local signal aggregate for one service/template generation.
    fn new(
        service_id: Uuid,
        service_epoch: u64,
        template_name: String,
        policy: TaskTemplateAutoscalePolicyValue,
    ) -> Self {
        Self {
            key: AutoscaleGroupKey::new(service_id, service_epoch, template_name),
            policy,
            running_replicas: 0,
            hot_replicas: 0,
            cpu_hot_replicas: 0,
            memory_hot_replicas: 0,
            cpu_requested_millis_total: 0,
            cpu_observed_millis_total: 0,
            memory_requested_bytes_total: 0,
            memory_observed_bytes_total: 0,
            observed_at_unix_ms: 0,
        }
    }

    /// Adds one local replica sample into the aggregate signal input.
    fn push(&mut self, sample: &LocalServiceRuntimeUsageSample, cpu_observed_millis: u64) {
        let cpu_hot =
            policy_target_percent(&self.policy, TaskTemplateAutoscaleMetricKindValue::Cpu)
                .is_some_and(|target| {
                    usage_exceeds_target(
                        cpu_observed_millis,
                        sample.replica.cpu_requested_millis,
                        target,
                    )
                });
        let memory_hot =
            policy_target_percent(&self.policy, TaskTemplateAutoscaleMetricKindValue::Memory)
                .is_some_and(|target| {
                    usage_exceeds_target(
                        sample.usage.memory_current_bytes,
                        sample.replica.memory_requested_bytes,
                        target,
                    )
                });

        self.running_replicas = self.running_replicas.saturating_add(1);
        self.cpu_requested_millis_total = self
            .cpu_requested_millis_total
            .saturating_add(sample.replica.cpu_requested_millis);
        self.cpu_observed_millis_total = self
            .cpu_observed_millis_total
            .saturating_add(cpu_observed_millis);
        self.memory_requested_bytes_total = self
            .memory_requested_bytes_total
            .saturating_add(sample.replica.memory_requested_bytes);
        self.memory_observed_bytes_total = self
            .memory_observed_bytes_total
            .saturating_add(sample.usage.memory_current_bytes);
        self.observed_at_unix_ms = self
            .observed_at_unix_ms
            .max(sample.usage.sampled_at_unix_ms);

        if cpu_hot {
            self.cpu_hot_replicas = self.cpu_hot_replicas.saturating_add(1);
        }
        if memory_hot {
            self.memory_hot_replicas = self.memory_hot_replicas.saturating_add(1);
        }
        if cpu_hot || memory_hot {
            self.hot_replicas = self.hot_replicas.saturating_add(1);
        }
    }

    /// Returns the dominant hot reason for this aggregate, if any threshold is crossed.
    fn hot_reason(&self) -> Option<ServiceAutoscaleSignalReason> {
        let cpu_hot = self.cpu_hot_replicas > 0
            || policy_target_percent(&self.policy, TaskTemplateAutoscaleMetricKindValue::Cpu)
                .is_some_and(|target| {
                    usage_exceeds_target(
                        self.cpu_observed_millis_total,
                        self.cpu_requested_millis_total,
                        target,
                    )
                });
        if cpu_hot {
            return Some(ServiceAutoscaleSignalReason::CpuHigh);
        }

        let memory_hot = self.memory_hot_replicas > 0
            || policy_target_percent(&self.policy, TaskTemplateAutoscaleMetricKindValue::Memory)
                .is_some_and(|target| {
                    usage_exceeds_target(
                        self.memory_observed_bytes_total,
                        self.memory_requested_bytes_total,
                        target,
                    )
                });
        memory_hot.then_some(ServiceAutoscaleSignalReason::MemoryHigh)
    }
}

/// Local runtime sample state used to convert point samples into sparse autoscale signals.
#[derive(Clone, Debug)]
pub(super) struct AutoscaleLocalSampleStore {
    ttl: Duration,
    runtime_samples: HashMap<AutoscaleRuntimeSampleKey, (RuntimeUsageSample, Instant)>,
    group_states: HashMap<AutoscaleGroupKey, AutoscaleLocalGroupState>,
}

impl Default for AutoscaleLocalSampleStore {
    /// Builds the production local autoscale sample store.
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(AUTOSCALE_LOCAL_SAMPLE_TTL_SECS),
            runtime_samples: HashMap::new(),
            group_states: HashMap::new(),
        }
    }
}

impl AutoscaleLocalSampleStore {
    /// Computes CPU milli-core usage from a cumulative runtime CPU accounting sample.
    fn cpu_millis_for_sample(
        &mut self,
        sample: &LocalServiceRuntimeUsageSample,
        now: Instant,
    ) -> u64 {
        let key = AutoscaleRuntimeSampleKey::from_sample(sample);
        let current = sample.usage.clone();
        let previous = self.runtime_samples.insert(key, (current.clone(), now));
        let Some((previous, _)) = previous else {
            return 0;
        };
        if current.sampled_at_unix_ms <= previous.sampled_at_unix_ms
            || current.cpu_usage_nanos < previous.cpu_usage_nanos
        {
            return 0;
        }

        let elapsed_ms = current
            .sampled_at_unix_ms
            .saturating_sub(previous.sampled_at_unix_ms);
        let elapsed_nanos = elapsed_ms.saturating_mul(1_000_000);
        if elapsed_nanos == 0 {
            return 0;
        }

        let delta_cpu = current
            .cpu_usage_nanos
            .saturating_sub(previous.cpu_usage_nanos);
        let cpu_millis = u128::from(delta_cpu).saturating_mul(1_000) / u128::from(elapsed_nanos);
        cpu_millis.min(u128::from(u64::MAX)) as u64
    }

    /// Builds a sparse autoscale signal from one local aggregate when thresholds require it.
    fn signal_for_accumulator(
        &mut self,
        local_node_id: Uuid,
        accumulator: AutoscaleSignalAccumulator,
        now: Instant,
    ) -> Option<ServiceAutoscaleSignal> {
        let state = self
            .group_states
            .entry(accumulator.key.clone())
            .or_default();
        state.cpu_observed_millis_ewma = smooth_usage(
            state.cpu_observed_millis_ewma,
            accumulator.cpu_observed_millis_total,
        );
        state.memory_observed_bytes_ewma = smooth_usage(
            state.memory_observed_bytes_ewma,
            accumulator.memory_observed_bytes_total,
        );
        state.last_seen_at = Some(now);

        if let Some(reason) = accumulator.hot_reason() {
            state.hot_windows = state.hot_windows.saturating_add(1);
            let min_interval = Duration::from_secs(accumulator.policy.sample_window_secs.max(1));
            if state.hot_windows >= accumulator.policy.trigger_windows.max(1)
                && interval_elapsed(state.last_hot_signal_at, min_interval, now)
            {
                state.last_hot_signal_at = Some(now);
                return Some(build_signal(
                    local_node_id,
                    &accumulator,
                    state,
                    ServiceAutoscaleSignalKind::Hot,
                    reason,
                ));
            }
            return None;
        }

        state.hot_windows = 0;
        let summary_interval = Duration::from_secs(
            accumulator
                .policy
                .scale_down_stabilization_secs
                .max(accumulator.policy.sample_window_secs)
                .max(AUTOSCALE_SUMMARY_SIGNAL_MIN_SECS),
        );
        if state.last_summary_signal_at.is_none() {
            state.last_summary_signal_at = Some(now);
            return None;
        }
        if interval_elapsed(state.last_summary_signal_at, summary_interval, now) {
            state.last_summary_signal_at = Some(now);
            return Some(build_signal(
                local_node_id,
                &accumulator,
                state,
                ServiceAutoscaleSignalKind::Summary,
                ServiceAutoscaleSignalReason::Quiet,
            ));
        }

        None
    }

    /// Removes local autoscale sample state for runtimes and groups no longer observed.
    fn prune(&mut self, now: Instant) {
        self.runtime_samples
            .retain(|_, (_, seen_at)| now.saturating_duration_since(*seen_at) <= self.ttl);
        self.group_states.retain(|_, state| {
            state
                .last_seen_at
                .is_some_and(|seen_at| now.saturating_duration_since(seen_at) <= self.ttl)
        });
    }
}

/// Builds the wire-independent signal value sent to the deterministic autoscale owner.
fn build_signal(
    local_node_id: Uuid,
    accumulator: &AutoscaleSignalAccumulator,
    state: &AutoscaleLocalGroupState,
    kind: ServiceAutoscaleSignalKind,
    reason: ServiceAutoscaleSignalReason,
) -> ServiceAutoscaleSignal {
    ServiceAutoscaleSignal {
        service_id: accumulator.key.service_id,
        service_epoch: accumulator.key.service_epoch,
        template_name: accumulator.key.template_name.clone(),
        node_id: local_node_id,
        kind,
        reason,
        running_replicas: accumulator.running_replicas,
        ready_replicas: accumulator.running_replicas,
        hot_replicas: accumulator.hot_replicas,
        cpu_requested_millis_total: accumulator.cpu_requested_millis_total,
        cpu_observed_millis_ewma: state.cpu_observed_millis_ewma,
        memory_requested_bytes_total: accumulator.memory_requested_bytes_total,
        memory_observed_bytes_ewma: state.memory_observed_bytes_ewma,
        observed_at_unix_ms: accumulator.observed_at_unix_ms,
    }
}

/// Applies a fixed integer EWMA so signal payloads are stable without extra knobs.
fn smooth_usage(previous: u64, current: u64) -> u64 {
    if previous == 0 {
        return current;
    }
    previous.saturating_mul(3).saturating_add(current) / 4
}

/// Returns true when enough time has passed since the previous signal.
fn interval_elapsed(previous: Option<Instant>, interval: Duration, now: Instant) -> bool {
    previous
        .map(|previous| now.saturating_duration_since(previous) >= interval)
        .unwrap_or(true)
}

/// Returns the configured target percentage for one metric kind.
fn policy_target_percent(
    policy: &TaskTemplateAutoscalePolicyValue,
    kind: TaskTemplateAutoscaleMetricKindValue,
) -> Option<u16> {
    policy
        .metrics
        .iter()
        .filter(|metric| metric.kind == kind)
        .map(|metric| metric.target_percent)
        .min()
}

/// Compares observed usage against a percent target without overflowing counters.
fn usage_exceeds_target(observed: u64, requested: u64, target_percent: u16) -> bool {
    requested > 0
        && u128::from(observed).saturating_mul(100)
            >= u128::from(requested).saturating_mul(u128::from(target_percent))
}

impl ServiceController {
    /// Samples local service replicas and emits sparse autoscale signals to the owner.
    pub(super) async fn emit_local_autoscale_signals(&self) -> anyhow::Result<()> {
        let samples = self
            .workload_manager
            .sample_local_service_runtime_usage()
            .await?;
        if samples.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        let mut signals = Vec::new();
        {
            let mut store = self.autoscale_local_samples.lock().await;
            store.prune(now);
            let mut accumulators: HashMap<AutoscaleGroupKey, AutoscaleSignalAccumulator> =
                HashMap::new();

            for sample in samples {
                let Some(policy) = self.autoscale_policy_for_sample(&sample) else {
                    continue;
                };
                let cpu_observed_millis = store.cpu_millis_for_sample(&sample, now);
                let key = AutoscaleGroupKey::new(
                    sample.replica.service_id,
                    sample.replica.service_epoch,
                    sample.replica.template_name.clone(),
                );
                let accumulator = accumulators.entry(key).or_insert_with(|| {
                    AutoscaleSignalAccumulator::new(
                        sample.replica.service_id,
                        sample.replica.service_epoch,
                        sample.replica.template_name.clone(),
                        policy,
                    )
                });
                accumulator.push(&sample, cpu_observed_millis);
            }

            for accumulator in accumulators.into_values() {
                if let Some(signal) =
                    store.signal_for_accumulator(self.local_node_id, accumulator, now)
                {
                    signals.push(signal);
                }
            }
        }

        for signal in signals {
            let report = self.send_autoscale_signal(signal).await?;
            if !report.accepted {
                tracing::debug!(
                    target: "services",
                    detail = %report.detail,
                    "autoscale owner rejected local signal"
                );
            }
        }

        Ok(())
    }

    /// Returns the active autoscale policy for one sampled local service replica.
    fn autoscale_policy_for_sample(
        &self,
        sample: &LocalServiceRuntimeUsageSample,
    ) -> Option<TaskTemplateAutoscalePolicyValue> {
        let spec = self
            .registry
            .get(sample.replica.service_id)
            .ok()
            .flatten()?;
        if spec.status() != ServiceStatus::Running
            || spec.service_epoch != sample.replica.service_epoch
            || spec.service_name != sample.replica.service_name
        {
            return None;
        }

        spec.task_templates
            .iter()
            .find(|template| template.name == sample.replica.template_name)
            .and_then(|template| template.autoscale.clone())
    }

    /// Records one autoscale signal when this node is the deterministic service owner.
    pub(crate) async fn report_autoscale_signal(
        &self,
        signal: ServiceAutoscaleSignal,
    ) -> ServiceAutoscaleSignalReport {
        let Some(spec) = self.registry.get(signal.service_id).ok().flatten() else {
            return ServiceAutoscaleSignalReport::rejected("service not found");
        };
        if spec.status() != ServiceStatus::Running {
            return ServiceAutoscaleSignalReport::rejected("service is not running");
        }
        if signal.service_epoch != spec.service_epoch {
            return ServiceAutoscaleSignalReport::rejected("stale service epoch");
        }
        let Some(template) = spec
            .task_templates
            .iter()
            .find(|template| template.name == signal.template_name)
        else {
            return ServiceAutoscaleSignalReport::rejected("template not found");
        };
        if template.autoscale.is_none() {
            return ServiceAutoscaleSignalReport::rejected("template autoscale disabled");
        }

        let known_nodes = self.known_autoscale_signal_nodes();
        if !known_nodes.contains(&signal.node_id) {
            return ServiceAutoscaleSignalReport::rejected("signal node is unknown");
        }

        let eligible_nodes = self.collect_eligible_nodes();
        let Some(owner) = select_autoscale_owner(spec.id, &eligible_nodes) else {
            return ServiceAutoscaleSignalReport::rejected("no autoscale owner available");
        };
        if owner != self.local_node_id {
            return ServiceAutoscaleSignalReport::rejected("receiver is not autoscale owner");
        }

        self.autoscale_signals
            .lock()
            .await
            .record(signal, Instant::now());
        ServiceAutoscaleSignalReport::accepted()
    }

    /// Sends one autoscale signal to the current owner, or records it locally when owned here.
    pub(crate) async fn send_autoscale_signal(
        &self,
        signal: ServiceAutoscaleSignal,
    ) -> anyhow::Result<ServiceAutoscaleSignalReport> {
        let eligible_nodes = self.collect_eligible_nodes();
        let owner = select_autoscale_owner(signal.service_id, &eligible_nodes)
            .ok_or_else(|| anyhow!("no autoscale owner available"))?;
        if owner == self.local_node_id {
            return Ok(self.report_autoscale_signal(signal).await);
        }

        let session = self
            .cluster_registry
            .session_for_peer(owner)
            .await
            .ok_or_else(|| anyhow!("no active session for autoscale owner {owner}"))?;
        let services = session
            .get_services_request()
            .send()
            .promise
            .await
            .with_context(|| format!("failed to open services session with {owner}"))?
            .get()
            .with_context(|| format!("invalid services response from {owner}"))?
            .get_services()
            .with_context(|| format!("missing services capability from {owner}"))?;
        let mut request = services.report_autoscale_signal_request();
        {
            let builder = request.get().init_signal();
            crate::services::service::write_autoscale_signal(builder, &signal);
        }
        let response = request
            .send()
            .promise
            .await
            .with_context(|| format!("autoscale signal RPC failed for {owner}"))?;
        let response = response
            .get()
            .with_context(|| format!("invalid autoscale signal response from {owner}"))?;
        Ok(ServiceAutoscaleSignalReport {
            accepted: response.get_accepted(),
            detail: response
                .get_detail()
                .ok()
                .and_then(|detail| detail.to_str().ok())
                .unwrap_or("")
                .to_string(),
        })
    }

    /// Returns nodes allowed to send local autoscale observations to this controller.
    fn known_autoscale_signal_nodes(&self) -> HashSet<Uuid> {
        let mut nodes: HashSet<Uuid> = self
            .cluster_registry
            .known_peers()
            .unwrap_or_default()
            .into_iter()
            .collect();
        nodes.insert(self.local_node_id);
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::types::RuntimeInstanceRef;

    /// Signal storage should keep only the latest unexpired soft-state entries.
    #[test]
    fn autoscale_signal_store_prunes_expired_entries() {
        let now = Instant::now();
        let mut store = AutoscaleSignalStore {
            ttl: Duration::from_secs(10),
            signals: HashMap::new(),
        };
        store.record(test_signal(ServiceAutoscaleSignalKind::Hot), now);
        store.record(
            test_signal(ServiceAutoscaleSignalKind::Summary),
            now + Duration::from_secs(11),
        );

        assert_eq!(store.len_after_prune(now + Duration::from_secs(11)), 1);
    }

    /// Local sampling should convert cumulative CPU time into milli-core usage.
    #[test]
    fn local_sample_store_computes_cpu_millis_from_cumulative_usage() {
        let now = Instant::now();
        let mut store = AutoscaleLocalSampleStore::default();
        let first = test_usage_sample(0, 64, 1_000);
        let second = test_usage_sample(500_000_000, 64, 2_000);

        assert_eq!(store.cpu_millis_for_sample(&first, now), 0);
        assert_eq!(
            store.cpu_millis_for_sample(&second, now + Duration::from_secs(1)),
            500
        );
    }

    /// Quiet summaries should wait for the slow summary interval instead of firing immediately.
    #[test]
    fn local_sample_store_delays_initial_quiet_summary() {
        let now = Instant::now();
        let service_id = Uuid::new_v4();
        let mut store = AutoscaleLocalSampleStore::default();
        let mut accumulator = test_accumulator(service_id);
        accumulator.push(&test_usage_sample(0, 64, 1_000), 0);

        assert!(
            store
                .signal_for_accumulator(Uuid::new_v4(), accumulator, now)
                .is_none()
        );

        let mut accumulator = test_accumulator(service_id);
        accumulator.push(&test_usage_sample(0, 64, 61_000), 0);
        let signal = store
            .signal_for_accumulator(
                Uuid::new_v4(),
                accumulator,
                now + Duration::from_secs(AUTOSCALE_SUMMARY_SIGNAL_MIN_SECS),
            )
            .expect("quiet summary after interval");

        assert_eq!(signal.kind, ServiceAutoscaleSignalKind::Summary);
        assert_eq!(signal.reason, ServiceAutoscaleSignalReason::Quiet);
    }

    /// Builds one minimal autoscale signal for owner-side store tests.
    fn test_signal(kind: ServiceAutoscaleSignalKind) -> ServiceAutoscaleSignal {
        ServiceAutoscaleSignal {
            service_id: Uuid::new_v4(),
            service_epoch: 1,
            template_name: "api".to_string(),
            node_id: Uuid::new_v4(),
            kind,
            reason: ServiceAutoscaleSignalReason::Quiet,
            running_replicas: 1,
            ready_replicas: 1,
            hot_replicas: 0,
            cpu_requested_millis_total: 500,
            cpu_observed_millis_ewma: 100,
            memory_requested_bytes_total: 128 * 1024 * 1024,
            memory_observed_bytes_ewma: 32 * 1024 * 1024,
            observed_at_unix_ms: 1,
        }
    }

    /// Builds one local autoscale accumulator for sample-store tests.
    fn test_accumulator(service_id: Uuid) -> AutoscaleSignalAccumulator {
        AutoscaleSignalAccumulator::new(
            service_id,
            1,
            "api".to_string(),
            TaskTemplateAutoscalePolicyValue {
                min_replicas: 1,
                max_replicas: 4,
                cooldown_secs: 30,
                scale_down_stabilization_secs: 60,
                sample_window_secs: 10,
                trigger_windows: 1,
                metrics: vec![crate::services::types::TaskTemplateAutoscaleMetricValue {
                    kind: TaskTemplateAutoscaleMetricKindValue::Memory,
                    target_percent: 80,
                }],
            },
        )
    }

    /// Builds one local runtime usage sample for sample-store tests.
    fn test_usage_sample(
        cpu_usage_nanos: u64,
        memory_current_bytes: u64,
        sampled_at_unix_ms: u64,
    ) -> LocalServiceRuntimeUsageSample {
        LocalServiceRuntimeUsageSample {
            replica: crate::workload::manager::LocalServiceRuntimeReplica {
                service_id: Uuid::new_v4(),
                service_name: "web".to_string(),
                service_epoch: 1,
                template_name: "api".to_string(),
                task_id: Uuid::new_v4(),
                runtime: RuntimeInstanceRef::new("mock", "container-1"),
                cpu_requested_millis: 1_000,
                memory_requested_bytes: 128,
            },
            usage: RuntimeUsageSample {
                runtime_id: "container-1".to_string(),
                sampled_at_unix_ms,
                cpu_usage_nanos,
                memory_current_bytes,
            },
        }
    }
}
