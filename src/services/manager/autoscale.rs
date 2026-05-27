use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use mantissa_health::Status as HealthStatus;
use uuid::Uuid;

use crate::observability::metrics;
use crate::runtime::types::RuntimeUsageSample;
use crate::services::manager::ServiceController;
use crate::services::ownership::select_autoscale_owner;
use crate::services::types::{
    ServiceEvent, ServicePreviousGeneration, ServiceSpecValue, ServiceStatus,
    TaskTemplateAutoscaleMetricKindValue, TaskTemplateAutoscalePolicyValue,
};
use crate::workload::manager::{LocalServiceRuntimeReplica, LocalServiceRuntimeUsageSample};

/// Default time after which owner-side autoscale signals are discarded.
const AUTOSCALE_SIGNAL_TTL_SECS: u64 = 180;
/// Internal cap for one autoscale scale-up write relative to the current replica count.
const AUTOSCALE_MAX_SCALE_UP_FACTOR: u16 = 2;
/// Local runtime usage sampling interval for autoscale-enabled service replicas.
pub(super) const AUTOSCALE_LOCAL_SAMPLE_TICK_SECS: u64 = 10;
/// Quiet summary heartbeat interval used to keep owner-side downscale coverage alive.
const AUTOSCALE_QUIET_SUMMARY_INTERVAL_SECS: u64 = 60;
/// Retention window for local runtime deltas and per-template signal state.
const AUTOSCALE_LOCAL_SAMPLE_TTL_SECS: u64 = 10 * 60;

/// Owner-directed autoscale signal emitted from one node's local service slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ServiceAutoscaleSignal {
    pub service_id: Uuid,
    pub service_epoch: u64,
    pub service_phase_version: u64,
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
    service_phase_version: u64,
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
            service_phase_version: signal.service_phase_version,
            template_name: signal.template_name.clone(),
            node_id: signal.node_id,
            kind: signal.kind,
        }
    }
}

/// Returns true when two signal keys describe the same node's view of one template.
fn same_autoscale_signal_source(left: &AutoscaleSignalKey, right: &AutoscaleSignalKey) -> bool {
    left.service_id == right.service_id
        && left.service_epoch == right.service_epoch
        && left.service_phase_version == right.service_phase_version
        && left.template_name == right.template_name
        && left.node_id == right.node_id
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct AutoscaleDecisionKey {
    service_id: Uuid,
    template_name: String,
}

impl AutoscaleDecisionKey {
    /// Builds the owner-side cooldown key for one autoscaled service template.
    fn new(service_id: Uuid, template_name: impl Into<String>) -> Self {
        Self {
            service_id,
            template_name: template_name.into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AutoscaleDecisionState {
    service_epoch: u64,
    service_phase_version: u64,
    last_scale_at: Option<Instant>,
    quiet_since: Option<Instant>,
}

/// Direction of one accepted autoscale replica-count mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AutoscaleScaleDirection {
    Up,
    Down,
}

/// Bounded autoscale decision returned by the owner-side signal evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
struct AutoscaleScaleDecision {
    service_id: Uuid,
    service_epoch: u64,
    observed_phase_version: u64,
    template_name: String,
    policy: TaskTemplateAutoscalePolicyValue,
    current_replicas: u16,
    desired_replicas: u16,
    direction: AutoscaleScaleDirection,
}

/// In-memory owner-side autoscale signal cache.
///
/// Signals are soft control input. The durable service spec is updated only
/// after a later decision stage accepts a new desired replica count.
#[derive(Clone, Debug)]
pub(super) struct AutoscaleSignalStore {
    ttl: Duration,
    signals: HashMap<AutoscaleSignalKey, (ServiceAutoscaleSignal, Instant)>,
    decisions: HashMap<AutoscaleDecisionKey, AutoscaleDecisionState>,
}

impl Default for AutoscaleSignalStore {
    /// Builds the production in-memory signal store.
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(AUTOSCALE_SIGNAL_TTL_SECS),
            signals: HashMap::new(),
            decisions: HashMap::new(),
        }
    }
}

impl AutoscaleSignalStore {
    /// Records the latest signal for one service/template/node/kind tuple.
    fn record(&mut self, signal: ServiceAutoscaleSignal, now: Instant) {
        self.prune(now);
        let key = AutoscaleSignalKey::from_signal(&signal);
        if self.signals.iter().any(|(existing, (current, _))| {
            same_autoscale_signal_source(existing, &key)
                && current.observed_at_unix_ms > signal.observed_at_unix_ms
        }) {
            return;
        }
        self.signals
            .retain(|existing, _| !same_autoscale_signal_source(existing, &key));
        self.signals.insert(key, (signal, now));
    }

    /// Removes expired soft-state signals from the owner-side cache.
    fn prune(&mut self, now: Instant) {
        self.signals
            .retain(|_, (_, received_at)| now.duration_since(*received_at) <= self.ttl);
    }

    /// Computes bounded replica-count decisions for the current service generation.
    fn decisions_for_service(
        &mut self,
        spec: &ServiceSpecValue,
        now: Instant,
    ) -> Vec<AutoscaleScaleDecision> {
        self.prune(now);
        let mut decisions = Vec::new();
        for template in &spec.task_templates {
            let Some(policy) = template.autoscale.as_ref() else {
                continue;
            };
            let key = AutoscaleDecisionKey::new(spec.id, template.name.clone());
            let state = self.decisions.entry(key).or_default();
            if state.service_epoch != spec.service_epoch {
                state.service_epoch = spec.service_epoch;
                state.service_phase_version = spec.phase_version;
                state.quiet_since = None;
            } else if state.service_phase_version != spec.phase_version {
                state.service_phase_version = spec.phase_version;
                state.quiet_since = None;
            }
            if !interval_elapsed(
                state.last_scale_at,
                Duration::from_secs(policy.cooldown_secs),
                now,
            ) {
                continue;
            }

            let signals = current_template_signals(
                &self.signals,
                spec.id,
                spec.service_epoch,
                spec.phase_version,
                &template.name,
            );
            if let Some(decision) =
                scale_up_decision(spec, &template.name, template.replicas, policy, &signals)
            {
                state.quiet_since = None;
                decisions.push(decision);
                continue;
            }

            if quiet_signals_cover_replicas(&signals, template.replicas) {
                let quiet_since = state.quiet_since.get_or_insert(now);
                if now.saturating_duration_since(*quiet_since)
                    >= Duration::from_secs(policy.scale_down_stabilization_secs)
                    && let Some(decision) =
                        scale_down_decision(spec, &template.name, template.replicas, policy)
                {
                    decisions.push(decision);
                }
            } else {
                state.quiet_since = None;
            }
        }
        decisions
    }

    /// Marks successful decisions so stale signals cannot drive repeated scaling.
    fn mark_decisions_applied(&mut self, decisions: &[AutoscaleScaleDecision], now: Instant) {
        for decision in decisions {
            let key =
                AutoscaleDecisionKey::new(decision.service_id, decision.template_name.clone());
            let state = self.decisions.entry(key).or_default();
            state.service_epoch = decision.service_epoch;
            state.service_phase_version = decision.observed_phase_version;
            state.last_scale_at = Some(now);
            state.quiet_since = None;
            self.clear_template_signals(
                decision.service_id,
                decision.service_epoch,
                &decision.template_name,
            );
        }
    }

    /// Drops soft signals for one template after their decision has been persisted.
    fn clear_template_signals(
        &mut self,
        service_id: Uuid,
        service_epoch: u64,
        template_name: &str,
    ) {
        self.signals.retain(|key, _| {
            key.service_id != service_id
                || key.service_epoch != service_epoch
                || key.template_name != template_name
        });
    }

    /// Returns the number of currently retained signals after pruning.
    #[cfg(test)]
    fn len_after_prune(&mut self, now: Instant) -> usize {
        self.prune(now);
        self.signals.len()
    }
}

/// Returns non-expired owner-side signals for one service template generation.
fn current_template_signals(
    signals: &HashMap<AutoscaleSignalKey, (ServiceAutoscaleSignal, Instant)>,
    service_id: Uuid,
    service_epoch: u64,
    service_phase_version: u64,
    template_name: &str,
) -> Vec<ServiceAutoscaleSignal> {
    signals
        .iter()
        .filter(|(key, _)| {
            key.service_id == service_id
                && key.service_epoch == service_epoch
                && key.service_phase_version == service_phase_version
                && key.template_name == template_name
        })
        .map(|(_, (signal, _))| signal.clone())
        .collect()
}

/// Builds a utilization-based scale-up decision from hot signals when policy bounds allow it.
fn scale_up_decision(
    spec: &ServiceSpecValue,
    template_name: &str,
    current_replicas: u16,
    policy: &TaskTemplateAutoscalePolicyValue,
    signals: &[ServiceAutoscaleSignal],
) -> Option<AutoscaleScaleDecision> {
    let mut saw_hot_signal = false;
    let mut extra_replicas = 0u16;
    for signal in signals
        .iter()
        .filter(|signal| signal.kind == ServiceAutoscaleSignalKind::Hot && signal.hot_replicas > 0)
    {
        saw_hot_signal = true;
        extra_replicas = extra_replicas.saturating_add(scale_up_extra_for_signal(policy, signal));
    }
    if !saw_hot_signal {
        return None;
    }
    let desired_replicas = current_replicas
        .saturating_add(extra_replicas.max(1))
        .min(max_scale_up_replicas(current_replicas))
        .min(policy.max_replicas);
    (desired_replicas > current_replicas).then(|| AutoscaleScaleDecision {
        service_id: spec.id,
        service_epoch: spec.service_epoch,
        observed_phase_version: spec.phase_version,
        template_name: template_name.to_string(),
        policy: policy.clone(),
        current_replicas,
        desired_replicas,
        direction: AutoscaleScaleDirection::Up,
    })
}

/// Builds a one-step scale-down decision when policy bounds allow it.
fn scale_down_decision(
    spec: &ServiceSpecValue,
    template_name: &str,
    current_replicas: u16,
    policy: &TaskTemplateAutoscalePolicyValue,
) -> Option<AutoscaleScaleDecision> {
    let desired_replicas = current_replicas.saturating_sub(1).max(policy.min_replicas);
    (desired_replicas < current_replicas).then(|| AutoscaleScaleDecision {
        service_id: spec.id,
        service_epoch: spec.service_epoch,
        observed_phase_version: spec.phase_version,
        template_name: template_name.to_string(),
        policy: policy.clone(),
        current_replicas,
        desired_replicas,
        direction: AutoscaleScaleDirection::Down,
    })
}

/// Returns the extra replicas implied by one hot signal's strongest metric.
fn scale_up_extra_for_signal(
    policy: &TaskTemplateAutoscalePolicyValue,
    signal: &ServiceAutoscaleSignal,
) -> u16 {
    let accounted_replicas = signal
        .ready_replicas
        .max(signal.running_replicas)
        .min(u32::from(u16::MAX)) as u16;
    if accounted_replicas == 0 {
        return 0;
    }

    let recommended_replicas = policy
        .metrics
        .iter()
        .filter_map(|metric| match metric.kind {
            TaskTemplateAutoscaleMetricKindValue::Cpu => recommended_replicas_for_usage(
                accounted_replicas,
                signal.cpu_observed_millis_ewma,
                signal.cpu_requested_millis_total,
                metric.target_percent,
            ),
            TaskTemplateAutoscaleMetricKindValue::Memory => recommended_replicas_for_usage(
                accounted_replicas,
                signal.memory_observed_bytes_ewma,
                signal.memory_requested_bytes_total,
                metric.target_percent,
            ),
        })
        .max()
        .unwrap_or(accounted_replicas);

    recommended_replicas.saturating_sub(accounted_replicas)
}

/// Computes a replica recommendation from one aggregate utilization sample.
fn recommended_replicas_for_usage(
    accounted_replicas: u16,
    observed: u64,
    requested: u64,
    target_percent: u16,
) -> Option<u16> {
    if accounted_replicas == 0 || requested == 0 || target_percent == 0 {
        return None;
    }

    let numerator = u128::from(accounted_replicas)
        .saturating_mul(u128::from(observed))
        .saturating_mul(100);
    let denominator = u128::from(requested).saturating_mul(u128::from(target_percent));
    let recommendation = ceil_div_u128(numerator, denominator)?;
    Some(recommendation.clamp(1, u128::from(u16::MAX)) as u16)
}

/// Returns the maximum target replicas allowed for one scale-up decision.
fn max_scale_up_replicas(current_replicas: u16) -> u16 {
    current_replicas
        .saturating_mul(AUTOSCALE_MAX_SCALE_UP_FACTOR)
        .max(current_replicas.saturating_add(1))
}

/// Divides two unsigned integers and rounds up, returning none for zero denominators.
fn ceil_div_u128(numerator: u128, denominator: u128) -> Option<u128> {
    if denominator == 0 {
        return None;
    }
    Some(numerator.saturating_add(denominator - 1) / denominator)
}

/// Returns true once quiet summaries account for every desired replica in the template.
fn quiet_signals_cover_replicas(signals: &[ServiceAutoscaleSignal], desired_replicas: u16) -> bool {
    if desired_replicas == 0
        || signals
            .iter()
            .any(|signal| signal.kind == ServiceAutoscaleSignalKind::Hot)
    {
        return false;
    }

    let mut ready_replicas = 0u32;
    for signal in signals {
        if signal.kind != ServiceAutoscaleSignalKind::Summary
            || signal.reason != ServiceAutoscaleSignalReason::Quiet
            || signal.hot_replicas != 0
            || signal.ready_replicas > signal.running_replicas
        {
            return false;
        }
        ready_replicas = ready_replicas.saturating_add(signal.ready_replicas);
    }
    ready_replicas >= u32::from(desired_replicas)
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
    service_phase_version: u64,
    template_name: String,
}

impl AutoscaleGroupKey {
    /// Builds the local aggregate key for one service/template generation.
    fn new(
        service_id: Uuid,
        service_epoch: u64,
        service_phase_version: u64,
        template_name: impl Into<String>,
    ) -> Self {
        Self {
            service_id,
            service_epoch,
            service_phase_version,
            template_name: template_name.into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AutoscaleLocalGroupState {
    cpu_observed_millis_ewma: u64,
    memory_observed_bytes_ewma: u64,
    hot_windows: u32,
    last_hot_window_at: Option<Instant>,
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

#[derive(Clone, Debug)]
struct AutoscaleSamplePolicy {
    service_epoch: u64,
    service_phase_version: u64,
    policy: TaskTemplateAutoscalePolicyValue,
}

#[derive(Clone, Debug)]
struct ActiveAutoscaleService {
    service_name: String,
    service_epoch: u64,
    service_phase_version: u64,
    templates: HashMap<String, ActiveAutoscaleTemplate>,
}

#[derive(Clone, Debug)]
struct ActiveAutoscaleTemplate {
    policy: TaskTemplateAutoscalePolicyValue,
    assigned_task_ids: HashSet<Uuid>,
}

impl ActiveAutoscaleService {
    /// Builds a template-aware local sampling index from one running service spec.
    fn from_spec(spec: &ServiceSpecValue) -> Option<Self> {
        if spec.status() != ServiceStatus::Running {
            return None;
        }

        let mut templates = HashMap::new();
        let mut slot_index = 0usize;
        for template in &spec.task_templates {
            let Some(policy) = template.autoscale.clone() else {
                slot_index = slot_index.saturating_add(usize::from(template.replicas));
                continue;
            };

            let mut assigned_task_ids = HashSet::with_capacity(template.replicas as usize);
            for _ in 0..template.replicas {
                if let Some(task_id) = spec.assigned_replica_id(slot_index) {
                    assigned_task_ids.insert(task_id);
                }
                slot_index = slot_index.saturating_add(1);
            }

            templates.insert(
                template.name.clone(),
                ActiveAutoscaleTemplate {
                    policy,
                    assigned_task_ids,
                },
            );
        }

        (!templates.is_empty()).then(|| Self {
            service_name: spec.service_name.clone(),
            service_epoch: spec.service_epoch,
            service_phase_version: spec.phase_version,
            templates,
        })
    }
}

impl AutoscaleSignalAccumulator {
    /// Starts a local signal aggregate for one service/template generation.
    fn new(
        service_id: Uuid,
        service_epoch: u64,
        service_phase_version: u64,
        template_name: String,
        policy: TaskTemplateAutoscalePolicyValue,
    ) -> Self {
        Self {
            key: AutoscaleGroupKey::new(
                service_id,
                service_epoch,
                service_phase_version,
                template_name,
            ),
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
            let min_interval = Duration::from_secs(accumulator.policy.sample_window_secs.max(1));
            if interval_elapsed(state.last_hot_window_at, min_interval, now) {
                state.hot_windows = state.hot_windows.saturating_add(1);
                state.last_hot_window_at = Some(now);
            }
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
        state.last_hot_window_at = None;
        let summary_interval = quiet_summary_interval();
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

    /// Clears local delivery throttle for one signal so failed owner sends retry on the next tick.
    fn mark_signal_delivery_failed(&mut self, signal: &ServiceAutoscaleSignal) {
        let key = AutoscaleGroupKey::new(
            signal.service_id,
            signal.service_epoch,
            signal.service_phase_version,
            signal.template_name.clone(),
        );
        let Some(state) = self.group_states.get_mut(&key) else {
            return;
        };

        match signal.kind {
            ServiceAutoscaleSignalKind::Hot => state.last_hot_signal_at = None,
            ServiceAutoscaleSignalKind::Summary => state.last_summary_signal_at = None,
        }
    }
}

/// Returns the quiet summary heartbeat interval, capped below the owner signal TTL.
fn quiet_summary_interval() -> Duration {
    let max_interval_secs = AUTOSCALE_SIGNAL_TTL_SECS.saturating_sub(1).max(1);
    Duration::from_secs(AUTOSCALE_QUIET_SUMMARY_INTERVAL_SECS.min(max_interval_secs))
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
        service_phase_version: accumulator.key.service_phase_version,
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
            > u128::from(requested).saturating_mul(u128::from(target_percent))
}

/// Validates one owner-side signal against the current template policy before caching it.
fn validate_autoscale_signal(
    signal: &ServiceAutoscaleSignal,
    desired_replicas: u16,
    policy: &TaskTemplateAutoscalePolicyValue,
) -> Result<(), &'static str> {
    if signal.running_replicas == 0 {
        return Err("signal has no running replicas");
    }
    if signal.running_replicas > u32::from(desired_replicas) {
        return Err("signal running replicas exceed template replicas");
    }
    if signal.ready_replicas > signal.running_replicas {
        return Err("signal ready replicas exceed running replicas");
    }
    if signal.hot_replicas > signal.running_replicas {
        return Err("signal hot replicas exceed running replicas");
    }

    match signal.kind {
        ServiceAutoscaleSignalKind::Hot => validate_hot_autoscale_signal(signal, policy),
        ServiceAutoscaleSignalKind::Summary => {
            if signal.reason != ServiceAutoscaleSignalReason::Quiet {
                return Err("summary signal must be quiet");
            }
            if signal.hot_replicas != 0 {
                return Err("summary signal reports hot replicas");
            }
            Ok(())
        }
    }
}

/// Validates hot-signal specific fields against the autoscale policy metrics.
fn validate_hot_autoscale_signal(
    signal: &ServiceAutoscaleSignal,
    policy: &TaskTemplateAutoscalePolicyValue,
) -> Result<(), &'static str> {
    if signal.hot_replicas == 0 {
        return Err("hot signal has no hot replicas");
    }

    match signal.reason {
        ServiceAutoscaleSignalReason::CpuHigh => {
            if policy_target_percent(policy, TaskTemplateAutoscaleMetricKindValue::Cpu).is_none() {
                return Err("hot CPU signal is not enabled by policy");
            }
            if signal.cpu_requested_millis_total == 0 {
                return Err("hot CPU signal is missing requested CPU");
            }
        }
        ServiceAutoscaleSignalReason::MemoryHigh => {
            if policy_target_percent(policy, TaskTemplateAutoscaleMetricKindValue::Memory).is_none()
            {
                return Err("hot memory signal is not enabled by policy");
            }
            if signal.memory_requested_bytes_total == 0 {
                return Err("hot memory signal is missing requested memory");
            }
        }
        ServiceAutoscaleSignalReason::Quiet => {
            return Err("hot signal must report a hot metric");
        }
    }

    Ok(())
}

/// Builds the live sender set for owner-directed autoscale signals.
fn live_autoscale_signal_node_set(
    local_node_id: Uuid,
    known_peers: impl IntoIterator<Item = Uuid>,
    health_snapshot: &HashMap<Uuid, HealthStatus>,
) -> HashSet<Uuid> {
    let mut nodes: HashSet<Uuid> = known_peers
        .into_iter()
        .filter(|peer_id| !matches!(health_snapshot.get(peer_id), Some(HealthStatus::Down)))
        .collect();
    if !matches!(
        health_snapshot.get(&local_node_id),
        Some(HealthStatus::Down)
    ) {
        nodes.insert(local_node_id);
    }
    nodes
}

/// Builds the next deploying service generation for accepted autoscale decisions.
fn build_autoscale_pending_spec(
    current: &ServiceSpecValue,
    decisions: &[AutoscaleScaleDecision],
) -> Option<ServiceSpecValue> {
    if decisions.is_empty() {
        return None;
    }

    let mut templates = current.task_templates.clone();
    let mut changed = false;
    for decision in decisions {
        if decision.service_id != current.id
            || decision.service_epoch != current.service_epoch
            || decision.observed_phase_version != current.phase_version
        {
            continue;
        }
        let Some(template) = templates
            .iter_mut()
            .find(|template| template.name == decision.template_name)
        else {
            continue;
        };
        if template.replicas != decision.current_replicas
            || template.autoscale.as_ref() != Some(&decision.policy)
            || decision.desired_replicas < decision.policy.min_replicas.max(1)
            || decision.desired_replicas > decision.policy.max_replicas
            || template.replicas == decision.desired_replicas
        {
            continue;
        }

        template.replicas = decision.desired_replicas;
        changed = true;
    }

    if !changed {
        return None;
    }

    let mut pending = current.clone();
    pending.task_templates = templates;
    pending.start_new_generation();
    pending.clear_replica_assignments();
    pending.previous_generation = Some(ServicePreviousGeneration::from_service(current));
    pending.set_status(ServiceStatus::Deploying);
    Some(pending)
}

/// Returns the active autoscale policy for one local service replica from a service snapshot.
fn autoscale_policy_for_replica(
    specs: &HashMap<Uuid, ActiveAutoscaleService>,
    replica: &LocalServiceRuntimeReplica,
) -> Option<AutoscaleSamplePolicy> {
    let spec = specs.get(&replica.service_id)?;
    if spec.service_name != replica.service_name {
        return None;
    }
    let template = spec.templates.get(&replica.template_name)?;
    if !template.assigned_task_ids.contains(&replica.task_id) {
        return None;
    }

    Some(AutoscaleSamplePolicy {
        service_epoch: spec.service_epoch,
        service_phase_version: spec.service_phase_version,
        policy: template.policy.clone(),
    })
}

/// Returns the bounded metrics label for an autoscale signal kind.
fn autoscale_signal_kind_label(kind: ServiceAutoscaleSignalKind) -> &'static str {
    match kind {
        ServiceAutoscaleSignalKind::Hot => "hot",
        ServiceAutoscaleSignalKind::Summary => "summary",
    }
}

/// Returns the bounded metrics label for an autoscale scale direction.
fn autoscale_direction_label(direction: AutoscaleScaleDirection) -> &'static str {
    match direction {
        AutoscaleScaleDirection::Up => "up",
        AutoscaleScaleDirection::Down => "down",
    }
}

/// Records and returns an accepted autoscale signal response.
fn accepted_signal_report(kind: &'static str) -> ServiceAutoscaleSignalReport {
    metrics::record_autoscale_signal(kind, "accepted");
    ServiceAutoscaleSignalReport::accepted()
}

/// Records and returns a rejected autoscale signal response.
fn rejected_signal_report(
    kind: &'static str,
    detail: impl Into<String>,
) -> ServiceAutoscaleSignalReport {
    metrics::record_autoscale_signal(kind, "rejected");
    ServiceAutoscaleSignalReport::rejected(detail)
}

impl ServiceController {
    /// Samples local service replicas and emits sparse autoscale signals to the owner.
    pub(super) async fn emit_local_autoscale_signals(&self) -> anyhow::Result<()> {
        let replicas = self
            .workload_manager
            .list_local_running_service_replicas()
            .await?;
        if replicas.is_empty() {
            return Ok(());
        }

        let candidate_service_ids = replicas
            .iter()
            .map(|replica| replica.service_id)
            .collect::<HashSet<_>>();
        let active_specs = self.active_autoscale_specs(&candidate_service_ids)?;
        if active_specs.is_empty() {
            return Ok(());
        }

        let autoscale_replicas = replicas
            .into_iter()
            .filter(|replica| autoscale_policy_for_replica(&active_specs, replica).is_some())
            .collect::<Vec<_>>();
        if autoscale_replicas.is_empty() {
            return Ok(());
        }

        let samples = self
            .workload_manager
            .sample_local_service_runtime_replicas_usage(autoscale_replicas)
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
                let Some(active_policy) =
                    autoscale_policy_for_replica(&active_specs, &sample.replica)
                else {
                    continue;
                };
                let cpu_observed_millis = store.cpu_millis_for_sample(&sample, now);
                let key = AutoscaleGroupKey::new(
                    sample.replica.service_id,
                    active_policy.service_epoch,
                    active_policy.service_phase_version,
                    sample.replica.template_name.clone(),
                );
                let accumulator = accumulators.entry(key).or_insert_with(|| {
                    AutoscaleSignalAccumulator::new(
                        sample.replica.service_id,
                        active_policy.service_epoch,
                        active_policy.service_phase_version,
                        sample.replica.template_name.clone(),
                        active_policy.policy,
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
            let kind = autoscale_signal_kind_label(signal.kind);
            match self.send_autoscale_signal(signal.clone()).await {
                Ok(report) if report.accepted => {}
                Ok(report) => {
                    self.autoscale_local_samples
                        .lock()
                        .await
                        .mark_signal_delivery_failed(&signal);
                    tracing::debug!(
                        target: "services",
                        detail = %report.detail,
                        "autoscale owner rejected local signal"
                    );
                }
                Err(err) => {
                    self.autoscale_local_samples
                        .lock()
                        .await
                        .mark_signal_delivery_failed(&signal);
                    metrics::record_autoscale_signal(kind, "error");
                    tracing::debug!(
                        target: "services",
                        "failed to deliver local autoscale signal: {err:#}"
                    );
                }
            }
        }

        Ok(())
    }

    /// Evaluates owner-side autoscale signals and persists service scale decisions.
    pub(super) async fn reconcile_autoscale_decisions(&self) -> anyhow::Result<()> {
        let eligible_nodes = self.collect_eligible_nodes();
        if eligible_nodes.is_empty() {
            return Ok(());
        }

        for spec in self.registry.list()? {
            if spec.status() != ServiceStatus::Running
                || select_autoscale_owner(spec.id, &eligible_nodes) != Some(self.local_node_id)
            {
                continue;
            }

            let now = Instant::now();
            let decisions = {
                self.autoscale_signals
                    .lock()
                    .await
                    .decisions_for_service(&spec, now)
            };
            if decisions.is_empty() {
                continue;
            }

            match self.apply_autoscale_decisions(&spec, &decisions).await {
                Ok(true) => {
                    self.autoscale_signals
                        .lock()
                        .await
                        .mark_decisions_applied(&decisions, now);
                    for decision in &decisions {
                        metrics::record_autoscale_decision(
                            autoscale_direction_label(decision.direction),
                            "applied",
                        );
                    }
                }
                Ok(false) => {}
                Err(err) => {
                    for decision in &decisions {
                        metrics::record_autoscale_decision(
                            autoscale_direction_label(decision.direction),
                            "failed",
                        );
                    }
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    /// Applies template autoscale decisions through the normal service generation path.
    async fn apply_autoscale_decisions(
        &self,
        observed: &ServiceSpecValue,
        decisions: &[AutoscaleScaleDecision],
    ) -> anyhow::Result<bool> {
        let Some(current) = self.registry.get(observed.id)? else {
            return Ok(false);
        };
        if current.service_epoch != observed.service_epoch
            || current.status() != ServiceStatus::Running
        {
            return Ok(false);
        }

        let Some(pending) = build_autoscale_pending_spec(&current, decisions) else {
            return Ok(false);
        };
        let service_name = pending.service_name.clone();
        let service_epoch = pending.service_epoch;
        self.apply_upsert(pending.clone()).await?;
        self.broadcast(ServiceEvent::Upsert(pending.clone()))
            .await?;
        self.maybe_spawn_generation_execution_for_service(pending.id)
            .await;

        tracing::info!(
            target: "services",
            service = %service_name,
            epoch = service_epoch,
            "autoscale decision started service generation"
        );
        Ok(true)
    }

    /// Returns a template-aware index for locally observed service specs with autoscale policies.
    fn active_autoscale_specs(
        &self,
        service_ids: &HashSet<Uuid>,
    ) -> anyhow::Result<HashMap<Uuid, ActiveAutoscaleService>> {
        let mut specs = HashMap::new();
        for service_id in service_ids {
            let Some(spec) = self.registry.get(*service_id)? else {
                continue;
            };
            if let Some(active) = ActiveAutoscaleService::from_spec(&spec) {
                specs.insert(*service_id, active);
            }
        }
        Ok(specs)
    }

    /// Records one autoscale signal when this node is the deterministic service owner.
    pub(crate) async fn report_autoscale_signal(
        &self,
        signal: ServiceAutoscaleSignal,
    ) -> ServiceAutoscaleSignalReport {
        let kind = autoscale_signal_kind_label(signal.kind);
        let Some(spec) = self.registry.get(signal.service_id).ok().flatten() else {
            return rejected_signal_report(kind, "service not found");
        };
        if spec.status() != ServiceStatus::Running {
            return rejected_signal_report(kind, "service is not running");
        }
        if signal.service_epoch != spec.service_epoch {
            return rejected_signal_report(kind, "stale service epoch");
        }
        if signal.service_phase_version != spec.phase_version {
            return rejected_signal_report(kind, "stale service phase");
        }
        let Some(template) = spec
            .task_templates
            .iter()
            .find(|template| template.name == signal.template_name)
        else {
            return rejected_signal_report(kind, "template not found");
        };
        let Some(policy) = template.autoscale.as_ref() else {
            return rejected_signal_report(kind, "template autoscale disabled");
        };
        if let Err(detail) = validate_autoscale_signal(&signal, template.replicas, policy) {
            return rejected_signal_report(kind, detail);
        }

        let live_nodes = self.live_autoscale_signal_nodes();
        if !live_nodes.contains(&signal.node_id) {
            return rejected_signal_report(kind, "signal node is not live");
        }

        let eligible_nodes = self.collect_eligible_nodes();
        let Some(owner) = select_autoscale_owner(spec.id, &eligible_nodes) else {
            return rejected_signal_report(kind, "no autoscale owner available");
        };
        if owner != self.local_node_id {
            return rejected_signal_report(kind, "receiver is not autoscale owner");
        }

        self.autoscale_signals
            .lock()
            .await
            .record(signal, Instant::now());
        accepted_signal_report(kind)
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

    /// Returns live nodes allowed to send local autoscale observations to this controller.
    fn live_autoscale_signal_nodes(&self) -> HashSet<Uuid> {
        let health_snapshot = self.health_monitor.snapshot();
        live_autoscale_signal_node_set(
            self.local_node_id,
            self.cluster_registry.known_peers().unwrap_or_default(),
            &health_snapshot,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::types::RuntimeInstanceRef;
    use crate::workload::types::ExecutionSpec;

    /// Signal storage should keep only the latest unexpired soft-state entries.
    #[test]
    fn autoscale_signal_store_prunes_expired_entries() {
        let now = Instant::now();
        let mut store = AutoscaleSignalStore {
            ttl: Duration::from_secs(10),
            signals: HashMap::new(),
            decisions: HashMap::new(),
        };
        store.record(test_signal(ServiceAutoscaleSignalKind::Hot), now);
        store.record(
            test_signal(ServiceAutoscaleSignalKind::Summary),
            now + Duration::from_secs(11),
        );

        assert_eq!(store.len_after_prune(now + Duration::from_secs(11)), 1);
    }

    /// A node's latest signal should replace its previous hot/quiet state.
    #[test]
    fn autoscale_signal_store_keeps_latest_signal_per_node() {
        let now = Instant::now();
        let spec = test_service_spec(3);
        let node_id = Uuid::new_v4();
        let mut hot = test_hot_signal(&spec);
        hot.node_id = node_id;
        let mut quiet = test_quiet_signal(&spec, 3);
        quiet.node_id = node_id;
        let mut store = AutoscaleSignalStore::default();

        store.record(hot, now);
        store.record(quiet, now + Duration::from_secs(1));

        let signals = current_template_signals(
            &store.signals,
            spec.id,
            spec.service_epoch,
            spec.phase_version,
            "api",
        );
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].kind, ServiceAutoscaleSignalKind::Summary);
        assert!(
            store
                .decisions_for_service(&spec, now + Duration::from_secs(1))
                .is_empty()
        );
        assert_eq!(
            store.decisions_for_service(&spec, now + Duration::from_secs(61)),
            vec![AutoscaleScaleDecision {
                service_id: spec.id,
                service_epoch: spec.service_epoch,
                observed_phase_version: spec.phase_version,
                template_name: "api".to_string(),
                policy: test_policy(),
                current_replicas: 3,
                desired_replicas: 2,
                direction: AutoscaleScaleDirection::Down,
            }]
        );
    }

    /// Older observations that arrive late must not replace newer same-node signals.
    #[test]
    fn autoscale_signal_store_ignores_late_older_same_node_signal() {
        let now = Instant::now();
        let spec = test_service_spec(3);
        let node_id = Uuid::new_v4();
        let mut quiet = test_quiet_signal(&spec, 3);
        quiet.node_id = node_id;
        quiet.observed_at_unix_ms = 2_000;
        let mut hot = test_hot_signal(&spec);
        hot.node_id = node_id;
        hot.observed_at_unix_ms = 1_000;
        let mut store = AutoscaleSignalStore::default();

        store.record(quiet, now);
        store.record(hot, now + Duration::from_secs(1));

        let signals = current_template_signals(
            &store.signals,
            spec.id,
            spec.service_epoch,
            spec.phase_version,
            "api",
        );
        assert_eq!(signals.len(), 1);
        assert_eq!(signals[0].kind, ServiceAutoscaleSignalKind::Summary);
    }

    /// Hot owner signals should produce one bounded scale-up decision.
    #[test]
    fn autoscale_signal_store_scales_up_from_hot_signal() {
        let now = Instant::now();
        let spec = test_service_spec(2);
        let mut store = AutoscaleSignalStore::default();
        store.record(test_hot_signal(&spec), now);

        let decisions = store.decisions_for_service(&spec, now);

        assert_eq!(
            decisions,
            vec![AutoscaleScaleDecision {
                service_id: spec.id,
                service_epoch: spec.service_epoch,
                observed_phase_version: spec.phase_version,
                template_name: "api".to_string(),
                policy: test_policy(),
                current_replicas: 2,
                desired_replicas: 3,
                direction: AutoscaleScaleDirection::Up,
            }]
        );
    }

    /// Hot owner signals should use utilization severity while keeping a hard scale-up cap.
    #[test]
    fn autoscale_signal_store_scales_up_from_hot_utilization() {
        let now = Instant::now();
        let mut spec = test_service_spec(4);
        let mut policy = test_policy();
        policy.max_replicas = 10;
        spec.task_templates[0].autoscale = Some(policy.clone());
        let mut signal = test_hot_signal(&spec);
        signal.cpu_requested_millis_total = 1_000;
        signal.cpu_observed_millis_ewma = 2_000;
        let mut store = AutoscaleSignalStore::default();
        store.record(signal, now);

        let decisions = store.decisions_for_service(&spec, now);

        assert_eq!(
            decisions,
            vec![AutoscaleScaleDecision {
                service_id: spec.id,
                service_epoch: spec.service_epoch,
                observed_phase_version: spec.phase_version,
                template_name: "api".to_string(),
                policy,
                current_replicas: 4,
                desired_replicas: 8,
                direction: AutoscaleScaleDirection::Up,
            }]
        );
    }

    /// Applied decisions should clear stale signals and enforce policy cooldown.
    #[test]
    fn autoscale_signal_store_clears_signals_and_honors_cooldown() {
        let now = Instant::now();
        let spec = test_service_spec(2);
        let mut store = AutoscaleSignalStore::default();
        store.record(test_hot_signal(&spec), now);
        let decisions = store.decisions_for_service(&spec, now);

        store.mark_decisions_applied(&decisions, now);
        store.record(test_hot_signal(&spec), now + Duration::from_secs(1));

        assert_eq!(store.len_after_prune(now + Duration::from_secs(1)), 1);
        assert!(
            store
                .decisions_for_service(&spec, now + Duration::from_secs(1))
                .is_empty()
        );
    }

    /// Quiet summaries should scale down only after covering all current replicas.
    #[test]
    fn autoscale_signal_store_scales_down_after_stabilized_quiet_summary() {
        let now = Instant::now();
        let spec = test_service_spec(3);
        let mut store = AutoscaleSignalStore::default();
        store.record(test_quiet_signal(&spec, 3), now);

        assert!(store.decisions_for_service(&spec, now).is_empty());
        let decisions = store.decisions_for_service(&spec, now + Duration::from_secs(60));

        assert_eq!(
            decisions,
            vec![AutoscaleScaleDecision {
                service_id: spec.id,
                service_epoch: spec.service_epoch,
                observed_phase_version: spec.phase_version,
                template_name: "api".to_string(),
                policy: test_policy(),
                current_replicas: 3,
                desired_replicas: 2,
                direction: AutoscaleScaleDirection::Down,
            }]
        );
    }

    /// Quiet summaries should only cover replicas that the sender reports ready.
    #[test]
    fn autoscale_signal_store_scales_down_from_ready_quiet_coverage() {
        let now = Instant::now();
        let spec = test_service_spec(3);
        let node_id = Uuid::new_v4();
        let mut quiet = test_quiet_signal(&spec, 3);
        quiet.node_id = node_id;
        quiet.ready_replicas = 2;
        let mut store = AutoscaleSignalStore::default();
        store.record(quiet, now);

        assert!(
            store
                .decisions_for_service(&spec, now + Duration::from_secs(60))
                .is_empty()
        );

        let mut quiet = test_quiet_signal(&spec, 3);
        quiet.node_id = node_id;
        quiet.ready_replicas = 3;
        store.record(quiet, now + Duration::from_secs(61));
        assert!(
            store
                .decisions_for_service(&spec, now + Duration::from_secs(61))
                .is_empty()
        );
        assert_eq!(
            store.decisions_for_service(&spec, now + Duration::from_secs(121)),
            vec![AutoscaleScaleDecision {
                service_id: spec.id,
                service_epoch: spec.service_epoch,
                observed_phase_version: spec.phase_version,
                template_name: "api".to_string(),
                policy: test_policy(),
                current_replicas: 3,
                desired_replicas: 2,
                direction: AutoscaleScaleDirection::Down,
            }]
        );
    }

    /// Quiet stabilization should restart when same-generation phase metadata changes.
    #[test]
    fn autoscale_signal_store_resets_quiet_window_on_phase_change() {
        let now = Instant::now();
        let spec = test_service_spec(3);
        let mut store = AutoscaleSignalStore::default();
        store.record(test_quiet_signal(&spec, 3), now);

        assert!(store.decisions_for_service(&spec, now).is_empty());
        let mut changed_phase = spec.clone();
        changed_phase.set_status_detail(Some("slot replaced".to_string()));
        store.record(
            test_quiet_signal(&changed_phase, 3),
            now + Duration::from_secs(60),
        );

        assert!(
            store
                .decisions_for_service(&changed_phase, now + Duration::from_secs(60))
                .is_empty()
        );
        assert_eq!(
            store.decisions_for_service(&changed_phase, now + Duration::from_secs(120)),
            vec![AutoscaleScaleDecision {
                service_id: changed_phase.id,
                service_epoch: changed_phase.service_epoch,
                observed_phase_version: changed_phase.phase_version,
                template_name: "api".to_string(),
                policy: test_policy(),
                current_replicas: 3,
                desired_replicas: 2,
                direction: AutoscaleScaleDirection::Down,
            }]
        );
    }

    /// Owner-side signal reads should ignore stale same-generation phase signals.
    #[test]
    fn autoscale_signal_store_ignores_stale_phase_signals() {
        let now = Instant::now();
        let spec = test_service_spec(2);
        let mut changed_phase = spec.clone();
        changed_phase.set_status_detail(Some("slot replaced".to_string()));
        let mut store = AutoscaleSignalStore::default();
        store.record(test_hot_signal(&spec), now);

        assert!(
            store
                .decisions_for_service(&changed_phase, now + Duration::from_secs(1))
                .is_empty()
        );
    }

    /// Refreshed quiet summaries should keep coverage alive across stabilization windows over TTL.
    #[test]
    fn autoscale_signal_store_scales_down_with_refreshed_long_stabilization_summary() {
        let now = Instant::now();
        let mut spec = test_service_spec(3);
        let mut policy = test_policy();
        policy.scale_down_stabilization_secs = AUTOSCALE_SIGNAL_TTL_SECS + 120;
        spec.task_templates[0].autoscale = Some(policy.clone());
        let mut store = AutoscaleSignalStore::default();
        let node_id = Uuid::new_v4();

        for elapsed in [0, 60, 120, 180, 240] {
            let mut signal = test_quiet_signal(&spec, 3);
            signal.node_id = node_id;
            store.record(signal, now + Duration::from_secs(elapsed));
            assert!(
                store
                    .decisions_for_service(&spec, now + Duration::from_secs(elapsed))
                    .is_empty()
            );
        }

        let mut signal = test_quiet_signal(&spec, 3);
        signal.node_id = node_id;
        store.record(
            signal,
            now + Duration::from_secs(policy.scale_down_stabilization_secs),
        );
        let decisions = store.decisions_for_service(
            &spec,
            now + Duration::from_secs(policy.scale_down_stabilization_secs),
        );

        assert_eq!(
            decisions,
            vec![AutoscaleScaleDecision {
                service_id: spec.id,
                service_epoch: spec.service_epoch,
                observed_phase_version: spec.phase_version,
                template_name: "api".to_string(),
                policy,
                current_replicas: 3,
                desired_replicas: 2,
                direction: AutoscaleScaleDirection::Down,
            }]
        );
    }

    /// Autoscale scale mutations should use the existing deploying generation path.
    #[test]
    fn autoscale_pending_spec_starts_deploying_generation() {
        let spec = test_service_spec(2);
        let decision = AutoscaleScaleDecision {
            service_id: spec.id,
            service_epoch: spec.service_epoch,
            observed_phase_version: spec.phase_version,
            template_name: "api".to_string(),
            policy: test_policy(),
            current_replicas: 2,
            desired_replicas: 3,
            direction: AutoscaleScaleDirection::Up,
        };

        let pending = build_autoscale_pending_spec(&spec, &[decision]).expect("pending spec");

        assert_eq!(pending.service_epoch, spec.service_epoch + 1);
        assert_eq!(pending.status(), ServiceStatus::Deploying);
        assert_eq!(pending.task_templates[0].replicas, 3);
        assert!(!pending.has_assigned_replicas());
        assert!(pending.previous_generation.is_some());
    }

    /// Autoscale writes should drop decisions made against an older service phase or policy.
    #[test]
    fn autoscale_pending_spec_rejects_stale_decision_fence() {
        let spec = test_service_spec(2);
        let decision = AutoscaleScaleDecision {
            service_id: spec.id,
            service_epoch: spec.service_epoch,
            observed_phase_version: spec.phase_version,
            template_name: "api".to_string(),
            policy: test_policy(),
            current_replicas: 2,
            desired_replicas: 3,
            direction: AutoscaleScaleDirection::Up,
        };

        let mut changed_phase = spec.clone();
        changed_phase.set_status_detail(Some("rollout settled".to_string()));
        assert!(
            build_autoscale_pending_spec(&changed_phase, std::slice::from_ref(&decision)).is_none()
        );

        let mut changed_policy = spec.clone();
        changed_policy.task_templates[0]
            .autoscale
            .as_mut()
            .expect("autoscale policy")
            .max_replicas = 8;
        assert!(build_autoscale_pending_spec(&changed_policy, &[decision]).is_none());
    }

    /// Local sampling policy lookup should ignore replicas no longer assigned to the spec.
    #[test]
    fn autoscale_policy_for_replica_requires_current_assignment() {
        let mut spec = test_service_spec(1);
        let assigned = Uuid::new_v4();
        spec.set_replica_ids(vec![assigned]);
        let specs = HashMap::from([(
            spec.id,
            ActiveAutoscaleService::from_spec(&spec).expect("active autoscale service"),
        )]);
        let mut sample = test_usage_sample(0, 64, 1_000);
        sample.replica.service_id = spec.id;
        sample.replica.service_name = spec.service_name.clone();
        sample.replica.template_name = "api".to_string();
        sample.replica.task_id = Uuid::new_v4();

        assert!(autoscale_policy_for_replica(&specs, &sample.replica).is_none());

        sample.replica.task_id = assigned;
        let policy =
            autoscale_policy_for_replica(&specs, &sample.replica).expect("assigned replica");
        assert_eq!(policy.service_epoch, spec.service_epoch);
        assert_eq!(policy.service_phase_version, spec.phase_version);
        assert_eq!(policy.policy, test_policy());
    }

    /// Local sampling policy lookup should require assignment in the sampled template.
    #[test]
    fn autoscale_policy_for_replica_requires_template_assignment() {
        let mut api = test_template(1);
        api.name = "api".to_string();
        let mut worker = test_template(1);
        worker.name = "worker".to_string();
        let mut spec =
            ServiceSpecValue::new(Uuid::new_v4(), "demo", "web", vec![api, worker], Vec::new());
        spec.service_epoch = 7;
        spec.set_status(ServiceStatus::Running);
        let api_id = Uuid::new_v4();
        let worker_id = Uuid::new_v4();
        spec.set_replica_ids(vec![api_id, worker_id]);
        let specs = HashMap::from([(
            spec.id,
            ActiveAutoscaleService::from_spec(&spec).expect("active autoscale service"),
        )]);

        let mut sample = test_usage_sample(0, 64, 1_000);
        sample.replica.service_id = spec.id;
        sample.replica.service_name = spec.service_name.clone();
        sample.replica.template_name = "worker".to_string();
        sample.replica.task_id = api_id;

        assert!(autoscale_policy_for_replica(&specs, &sample.replica).is_none());

        sample.replica.task_id = worker_id;
        let policy = autoscale_policy_for_replica(&specs, &sample.replica).expect("worker replica");
        assert_eq!(policy.service_epoch, spec.service_epoch);
        assert_eq!(policy.service_phase_version, spec.phase_version);
    }

    /// Owner-side validation should reject malformed or policy-incompatible signals.
    #[test]
    fn autoscale_signal_validation_rejects_malformed_signals() {
        let spec = test_service_spec(2);
        let policy = spec.task_templates[0]
            .autoscale
            .as_ref()
            .expect("autoscale policy");
        assert!(validate_autoscale_signal(&test_hot_signal(&spec), 2, policy).is_ok());
        assert!(validate_autoscale_signal(&test_quiet_signal(&spec, 2), 2, policy).is_ok());

        let mut hot_without_running = test_hot_signal(&spec);
        hot_without_running.running_replicas = 0;
        assert_eq!(
            validate_autoscale_signal(&hot_without_running, 2, policy),
            Err("signal has no running replicas")
        );

        let mut hot_with_bad_count = test_hot_signal(&spec);
        hot_with_bad_count.hot_replicas = 3;
        assert_eq!(
            validate_autoscale_signal(&hot_with_bad_count, 2, policy),
            Err("signal hot replicas exceed running replicas")
        );

        let mut summary_with_bad_reason = test_quiet_signal(&spec, 2);
        summary_with_bad_reason.reason = ServiceAutoscaleSignalReason::CpuHigh;
        assert_eq!(
            validate_autoscale_signal(&summary_with_bad_reason, 2, policy),
            Err("summary signal must be quiet")
        );

        let mut memory_policy = test_policy();
        memory_policy.metrics[0].kind = TaskTemplateAutoscaleMetricKindValue::Memory;
        assert_eq!(
            validate_autoscale_signal(&test_hot_signal(&spec), 2, &memory_policy),
            Err("hot CPU signal is not enabled by policy")
        );
    }

    /// Autoscale signal senders should exclude members currently marked down.
    #[test]
    fn autoscale_signal_live_nodes_exclude_down_members() {
        let local = Uuid::from_bytes([1u8; 16]);
        let down_peer = Uuid::from_bytes([2u8; 16]);
        let live_peer = Uuid::from_bytes([3u8; 16]);
        let health = HashMap::from([
            (local, HealthStatus::Alive),
            (down_peer, HealthStatus::Down),
            (live_peer, HealthStatus::Alive),
        ]);

        let nodes = live_autoscale_signal_node_set(local, [down_peer, live_peer], &health);

        assert!(nodes.contains(&local));
        assert!(nodes.contains(&live_peer));
        assert!(!nodes.contains(&down_peer));
    }

    /// Autoscale thresholds should require usage above the target, not equal to it.
    #[test]
    fn autoscale_usage_threshold_is_strictly_greater_than_target() {
        assert!(!usage_exceeds_target(800, 1_000, 80));
        assert!(usage_exceeds_target(801, 1_000, 80));
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

    /// Quiet summaries should fire immediately and then refresh before owner TTL expires.
    #[test]
    fn local_sample_store_emits_initial_and_periodic_quiet_summary() {
        let now = Instant::now();
        let service_id = Uuid::new_v4();
        let local_node_id = Uuid::new_v4();
        let mut store = AutoscaleLocalSampleStore::default();
        let mut accumulator = test_accumulator(service_id);
        accumulator.policy.scale_down_stabilization_secs = AUTOSCALE_SIGNAL_TTL_SECS + 120;
        accumulator.push(&test_usage_sample(0, 64, 1_000), 0);

        let signal = store
            .signal_for_accumulator(local_node_id, accumulator, now)
            .expect("initial quiet summary");
        assert_eq!(signal.kind, ServiceAutoscaleSignalKind::Summary);
        assert_eq!(signal.reason, ServiceAutoscaleSignalReason::Quiet);

        let interval = quiet_summary_interval();
        assert!(interval < Duration::from_secs(AUTOSCALE_SIGNAL_TTL_SECS));

        let mut accumulator = test_accumulator(service_id);
        accumulator.policy.scale_down_stabilization_secs = AUTOSCALE_SIGNAL_TTL_SECS + 120;
        accumulator.push(&test_usage_sample(0, 64, 2_000), 0);
        assert!(
            store
                .signal_for_accumulator(local_node_id, accumulator, now + interval / 2)
                .is_none()
        );

        let mut accumulator = test_accumulator(service_id);
        accumulator.policy.scale_down_stabilization_secs = AUTOSCALE_SIGNAL_TTL_SECS + 120;
        accumulator.push(&test_usage_sample(0, 64, 61_000), 0);
        let signal = store
            .signal_for_accumulator(local_node_id, accumulator, now + interval)
            .expect("periodic quiet summary");

        assert_eq!(signal.kind, ServiceAutoscaleSignalKind::Summary);
        assert_eq!(signal.reason, ServiceAutoscaleSignalReason::Quiet);
    }

    /// Failed quiet-summary delivery should retry without waiting for the heartbeat interval.
    #[test]
    fn local_sample_store_retries_quiet_summary_after_delivery_failure() {
        let now = Instant::now();
        let service_id = Uuid::new_v4();
        let local_node_id = Uuid::new_v4();
        let mut store = AutoscaleLocalSampleStore::default();
        let mut accumulator = test_accumulator(service_id);
        accumulator.push(&test_usage_sample(0, 64, 1_000), 0);

        let signal = store
            .signal_for_accumulator(local_node_id, accumulator, now)
            .expect("initial quiet summary");
        store.mark_signal_delivery_failed(&signal);

        let mut accumulator = test_accumulator(service_id);
        accumulator.push(&test_usage_sample(0, 64, 2_000), 0);
        let retry = store
            .signal_for_accumulator(local_node_id, accumulator, now + Duration::from_secs(1))
            .expect("quiet summary retry");

        assert_eq!(retry.kind, ServiceAutoscaleSignalKind::Summary);
        assert_eq!(retry.reason, ServiceAutoscaleSignalReason::Quiet);
    }

    /// Hot windows should advance only once per configured sample window.
    #[test]
    fn local_sample_store_counts_hot_windows_by_sample_window() {
        let now = Instant::now();
        let service_id = Uuid::new_v4();
        let local_node_id = Uuid::new_v4();
        let mut store = AutoscaleLocalSampleStore::default();

        let mut accumulator = test_accumulator(service_id);
        accumulator.policy.trigger_windows = 2;
        accumulator.push(&test_usage_sample(0, 128, 1_000), 0);
        assert!(
            store
                .signal_for_accumulator(local_node_id, accumulator, now)
                .is_none()
        );

        let mut accumulator = test_accumulator(service_id);
        accumulator.policy.trigger_windows = 2;
        accumulator.push(&test_usage_sample(0, 128, 2_000), 0);
        assert!(
            store
                .signal_for_accumulator(local_node_id, accumulator, now + Duration::from_secs(5))
                .is_none()
        );

        let mut accumulator = test_accumulator(service_id);
        accumulator.policy.trigger_windows = 2;
        accumulator.push(&test_usage_sample(0, 128, 11_000), 0);
        let signal = store
            .signal_for_accumulator(local_node_id, accumulator, now + Duration::from_secs(10))
            .expect("second hot sample window should emit");

        assert_eq!(signal.kind, ServiceAutoscaleSignalKind::Hot);
        assert_eq!(signal.reason, ServiceAutoscaleSignalReason::MemoryHigh);
    }

    /// Builds one minimal autoscale signal for owner-side store tests.
    fn test_signal(kind: ServiceAutoscaleSignalKind) -> ServiceAutoscaleSignal {
        ServiceAutoscaleSignal {
            service_id: Uuid::new_v4(),
            service_epoch: 1,
            service_phase_version: 0,
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

    /// Builds a hot signal that points at the provided service spec.
    fn test_hot_signal(spec: &ServiceSpecValue) -> ServiceAutoscaleSignal {
        ServiceAutoscaleSignal {
            service_id: spec.id,
            service_epoch: spec.service_epoch,
            service_phase_version: spec.phase_version,
            template_name: "api".to_string(),
            node_id: Uuid::new_v4(),
            kind: ServiceAutoscaleSignalKind::Hot,
            reason: ServiceAutoscaleSignalReason::CpuHigh,
            running_replicas: spec.task_templates[0].replicas as u32,
            ready_replicas: spec.task_templates[0].replicas as u32,
            hot_replicas: 1,
            cpu_requested_millis_total: 1_000,
            cpu_observed_millis_ewma: 900,
            memory_requested_bytes_total: 128,
            memory_observed_bytes_ewma: 64,
            observed_at_unix_ms: 1,
        }
    }

    /// Builds a quiet summary signal that points at the provided service spec.
    fn test_quiet_signal(spec: &ServiceSpecValue, running_replicas: u32) -> ServiceAutoscaleSignal {
        ServiceAutoscaleSignal {
            service_id: spec.id,
            service_epoch: spec.service_epoch,
            service_phase_version: spec.phase_version,
            template_name: "api".to_string(),
            node_id: Uuid::new_v4(),
            kind: ServiceAutoscaleSignalKind::Summary,
            reason: ServiceAutoscaleSignalReason::Quiet,
            running_replicas,
            ready_replicas: running_replicas,
            hot_replicas: 0,
            cpu_requested_millis_total: 1_000,
            cpu_observed_millis_ewma: 100,
            memory_requested_bytes_total: 128,
            memory_observed_bytes_ewma: 64,
            observed_at_unix_ms: 1,
        }
    }

    /// Builds one minimal running service spec with autoscale enabled.
    fn test_service_spec(replicas: u16) -> ServiceSpecValue {
        let mut spec = ServiceSpecValue::new(
            Uuid::new_v4(),
            "demo",
            "web",
            vec![test_template(replicas)],
            Vec::new(),
        );
        spec.service_epoch = 7;
        spec.set_status(ServiceStatus::Running);
        spec
    }

    /// Builds one autoscale-enabled task template for owner decision tests.
    fn test_template(replicas: u16) -> crate::services::types::TaskTemplateSpecValue {
        crate::services::types::TaskTemplateSpecValue {
            name: "api".to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/api:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 1_000,
                memory_bytes: 128,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
                ports: Vec::new(),
                placement: Default::default(),
            },
            depends_on: Vec::new(),
            replicas,
            readiness: None,
            public_port: None,
            public_protocol: None,
            placement_preferences: Vec::new(),
            autoscale: Some(test_policy()),
        }
    }

    /// Builds one autoscale policy for owner decision tests.
    fn test_policy() -> TaskTemplateAutoscalePolicyValue {
        TaskTemplateAutoscalePolicyValue {
            min_replicas: 1,
            max_replicas: 4,
            cooldown_secs: 30,
            scale_down_stabilization_secs: 60,
            sample_window_secs: 10,
            trigger_windows: 1,
            metrics: vec![crate::services::types::TaskTemplateAutoscaleMetricValue {
                kind: TaskTemplateAutoscaleMetricKindValue::Cpu,
                target_percent: 80,
            }],
        }
    }

    /// Builds one local autoscale accumulator for sample-store tests.
    fn test_accumulator(service_id: Uuid) -> AutoscaleSignalAccumulator {
        AutoscaleSignalAccumulator::new(
            service_id,
            1,
            0,
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
