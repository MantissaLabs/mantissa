use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use uuid::Uuid;

use crate::observability::metrics;
use crate::runtime::types::RuntimeUsageSample;
use crate::services::manager::ServiceController;
use crate::services::ownership::select_autoscale_owner;
use crate::services::types::{
    ServiceEvent, ServicePreviousGeneration, ServiceSpecValue, ServiceStatus,
    TaskTemplateAutoscaleMetricKindValue, TaskTemplateAutoscalePolicyValue,
};
use crate::workload::manager::LocalServiceRuntimeUsageSample;

/// Default time after which owner-side autoscale signals are discarded.
const AUTOSCALE_SIGNAL_TTL_SECS: u64 = 180;
/// Internal cap for one autoscale scale-up write relative to the current replica count.
const AUTOSCALE_MAX_SCALE_UP_FACTOR: u16 = 2;
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
        self.signals
            .insert(AutoscaleSignalKey::from_signal(&signal), (signal, now));
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
    template_name: &str,
) -> Vec<ServiceAutoscaleSignal> {
    signals
        .iter()
        .filter(|(key, _)| {
            key.service_id == service_id
                && key.service_epoch == service_epoch
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

    let mut running_replicas = 0u32;
    for signal in signals {
        if signal.kind != ServiceAutoscaleSignalKind::Summary
            || signal.reason != ServiceAutoscaleSignalReason::Quiet
            || signal.hot_replicas != 0
        {
            return false;
        }
        running_replicas = running_replicas.saturating_add(signal.running_replicas);
    }
    running_replicas >= u32::from(desired_replicas)
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
            let kind = autoscale_signal_kind_label(signal.kind);
            match self.send_autoscale_signal(signal).await {
                Ok(report) if report.accepted => {}
                Ok(report) => {
                    tracing::debug!(
                        target: "services",
                        detail = %report.detail,
                        "autoscale owner rejected local signal"
                    );
                }
                Err(err) => {
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
        let Some(template) = spec
            .task_templates
            .iter()
            .find(|template| template.name == signal.template_name)
        else {
            return rejected_signal_report(kind, "template not found");
        };
        if template.autoscale.is_none() {
            return rejected_signal_report(kind, "template autoscale disabled");
        }

        let known_nodes = self.known_autoscale_signal_nodes();
        if !known_nodes.contains(&signal.node_id) {
            return rejected_signal_report(kind, "signal node is unknown");
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

    /// Builds a hot signal that points at the provided service spec.
    fn test_hot_signal(spec: &ServiceSpecValue) -> ServiceAutoscaleSignal {
        ServiceAutoscaleSignal {
            service_id: spec.id,
            service_epoch: spec.service_epoch,
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
