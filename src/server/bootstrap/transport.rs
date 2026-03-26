use super::runtime::BootstrapOptions;
use std::time::Duration;

/// Parses one positive `u64` value from the environment.
///
/// The daemon bootstrap path uses this for lightweight runtime tuning without
/// introducing a separate configuration parser for startup-only knobs.
fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
}

/// Parses one positive duration in milliseconds from the environment.
///
/// This keeps transport-specific timing overrides close to the daemon entry
/// path instead of scattering environment access across runtime assembly.
fn env_duration_ms(name: &str) -> Option<Duration> {
    env_u64(name).map(Duration::from_millis)
}

/// Resolves the daemon gossip fanout from environment overrides.
///
/// Production startup uses this to tune outbound gossip pressure without
/// changing the shared headless boot pipeline.
fn gossip_fanout_from_env(default: usize) -> usize {
    std::env::var("MANTISSA_GOSSIP_FANOUT")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Resolves the daemon gossip queue capacity from environment overrides.
///
/// This lets operators raise or lower queue depth while leaving the rest of
/// the runtime assembly identical to the headless path.
fn gossip_channel_capacity_from_env(default: usize) -> usize {
    env_u64("MANTISSA_GOSSIP_CHANNEL_CAPACITY")
        .map(|value| value as usize)
        .unwrap_or(default)
        .max(1)
}

/// Resolves the daemon gossip tick override from environment overrides.
///
/// Returning `None` preserves the topology default so only explicit overrides
/// change runtime timing behavior.
fn gossip_tick_from_env() -> Option<Duration> {
    env_duration_ms("MANTISSA_GOSSIP_TICK_MS")
}

/// Builds the runtime options used by the production daemon entrypoint.
///
/// Headless startup constructs the same option type directly so both code paths
/// converge on one shared bootstrap pipeline.
pub(crate) fn daemon_bootstrap_options() -> BootstrapOptions {
    let mut options = BootstrapOptions::default();
    options.gossip_channel_capacity =
        gossip_channel_capacity_from_env(options.gossip_channel_capacity);
    options.gossip_fanout = gossip_fanout_from_env(options.gossip_fanout);
    options.gossip_tick = gossip_tick_from_env();
    options
}
