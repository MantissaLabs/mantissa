#![allow(dead_code)]

use once_cell::sync::OnceCell;
use std::env;
use std::io;
use std::io::IsTerminal;
use time::{UtcOffset, format_description::FormatItem, macros::format_description};
use tracing_log::LogTracer;
use tracing_subscriber::{
    EnvFilter, fmt,
    fmt::{TestWriter, time::OffsetTime},
    prelude::*,
};

static INIT: OnceCell<()> = OnceCell::new();

fn local_timer() -> OffsetTime<&'static [FormatItem<'static>]> {
    // Produces: "[Sep 04 12:35:46]"
    let fmt =
        format_description!("[[[month repr:short] [day padding:zero] [hour]:[minute]:[second]]");
    let offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    OffsetTime::new(offset, fmt)
}

/// Adds one built-in dependency directive without making logging initialization fatal.
///
/// Built-in directives are static, so parse failure would indicate a coding error.
fn add_default_directive(filter: EnvFilter, directive: &'static str) -> EnvFilter {
    match directive.parse() {
        Ok(directive) => filter.add_directive(directive),
        Err(_) => filter,
    }
}

/// Builds the requested filter and applies quiet dependency defaults.
///
/// An invalid `RUST_LOG` falls back to the requested level and cannot bypass
/// the dependency defaults merely by containing a dependency name.
fn configured_filter(default_level: &'static str, rust_log: Option<&str>) -> EnvFilter {
    let (mut filter, valid_rust_log) = match rust_log {
        Some(raw) => match EnvFilter::try_new(raw) {
            Ok(filter) => (filter, Some(raw)),
            Err(_) => (EnvFilter::new(default_level), None),
        },
        None => (EnvFilter::new(default_level), None),
    };

    if !has_target_directive(valid_rust_log, "bollard", true) {
        filter = add_default_directive(filter, "bollard::docker=warn");
    }
    if !has_target_directive(valid_rust_log, "openraft", false) {
        // Keep OpenRaft's expected retries quiet by default. A specific child
        // target remains usable because it is more specific than this rule.
        filter = add_default_directive(filter, "openraft=off");
    }
    filter
}

/// Returns whether a valid filter configures one target directly.
///
/// Child matching is useful for Bollard's existing module-specific default.
/// OpenRaft deliberately requires an exact top-level directive so one selected
/// child module can be enabled without enabling every other OpenRaft target.
fn has_target_directive(rust_log: Option<&str>, target: &str, include_children: bool) -> bool {
    rust_log.is_some_and(|raw| {
        raw.split(',').any(|directive| {
            let selected = directive
                .split_once('=')
                .map_or(directive, |(selected, _level)| selected)
                .trim();
            selected == target
                || (include_children
                    && selected
                        .strip_prefix(target)
                        .is_some_and(|suffix| suffix.starts_with("::")))
        })
    })
}

/// Initialize pretty logs for binaries. Idempotent.
/// Respects `RUST_LOG`, defaults to `info`.
pub fn init() -> io::Result<()> {
    if INIT.get().is_some() {
        return Ok(());
    }

    // Route `log` crate records into `tracing` (idempotent: ignore error).
    let _ = LogTracer::init();

    let rust_log = env::var("RUST_LOG").ok();
    let filter = configured_filter("info", rust_log.as_deref());
    let ansi = std::io::stderr().is_terminal();
    let timer = local_timer();
    let layer = fmt::layer()
        .compact()
        .with_timer(timer)
        .with_ansi(ansi)
        .with_level(true)
        .with_target(true)
        .with_thread_names(false)
        .with_thread_ids(false);

    // If a subscriber is already set, just ignore the error.
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .try_init();

    let _ = INIT.set(());
    Ok(())
}

/// Initialize logging for tests.
/// Quiet by default; set `TEST_LOG=1` (and optionally `RUST_LOG`) to see logs.
pub fn init_for_tests() {
    if INIT.get().is_some() {
        return;
    }

    if std::env::var_os("TEST_LOG").is_none() {
        // Explicitly silence logs during tests unless opted in.
        let _ = tracing_subscriber::registry()
            .with(EnvFilter::new("off"))
            .try_init();

        let _ = INIT.set(());

        return;
    }

    let _ = LogTracer::init(); // idempotent

    // Default to debug in tests unless overridden.
    let rust_log = env::var("RUST_LOG").ok();
    let filter = configured_filter("debug", rust_log.as_deref());

    let timer = local_timer();
    let layer = fmt::layer()
        .compact()
        .with_timer(timer)
        .with_ansi(false) // keep test output clean
        .with_level(true)
        .with_target(true)
        .with_writer(TestWriter::default());

    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .try_init();

    let _ = INIT.set(());
}

#[cfg(test)]
mod tests {
    use tracing::Level;
    use tracing_subscriber::prelude::*;

    use super::configured_filter;

    /// OpenRaft stays quiet when no filter explicitly enables it.
    #[test]
    fn openraft_is_disabled_by_default() {
        let subscriber = tracing_subscriber::registry().with(configured_filter("info", None));
        tracing::subscriber::with_default(subscriber, || {
            assert!(!tracing::enabled!(
                target: "openraft::raft",
                Level::ERROR
            ));
            assert!(tracing::enabled!(
                target: "mantissa::volumes::raft",
                Level::INFO
            ));
        });
    }

    /// The documented Mantissa filter includes application-level Raft events.
    #[test]
    fn mantissa_filter_includes_volume_raft_logs() {
        let subscriber =
            tracing_subscriber::registry().with(configured_filter("info", Some("mantissa=info")));
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(
                target: "mantissa::volumes::raft",
                Level::INFO
            ));
            assert!(!tracing::enabled!(
                target: "mantissa::volumes::raft",
                Level::DEBUG
            ));
        });
    }

    /// A top-level OpenRaft directive replaces the quiet default.
    #[test]
    fn openraft_can_be_enabled_explicitly() {
        let subscriber = tracing_subscriber::registry()
            .with(configured_filter("info", Some("info,openraft=debug")));
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(
                target: "openraft::raft",
                Level::DEBUG
            ));
        });
    }

    /// An explicit off directive keeps every OpenRaft target disabled.
    #[test]
    fn openraft_can_be_disabled_explicitly() {
        let subscriber = tracing_subscriber::registry()
            .with(configured_filter("info", Some("info,openraft=off")));
        tracing::subscriber::with_default(subscriber, || {
            assert!(!tracing::enabled!(
                target: "openraft::raft",
                Level::ERROR
            ));
        });
    }

    /// One OpenRaft module can be inspected without enabling its siblings.
    #[test]
    fn one_openraft_module_can_be_enabled() {
        let subscriber = tracing_subscriber::registry()
            .with(configured_filter("info", Some("info,openraft::raft=debug")));
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(
                target: "openraft::raft",
                Level::DEBUG
            ));
            assert!(!tracing::enabled!(
                target: "openraft::replication",
                Level::ERROR
            ));
        });
    }

    /// Invalid input cannot accidentally restore noisy OpenRaft logs.
    #[test]
    fn invalid_filter_keeps_openraft_disabled() {
        let subscriber = tracing_subscriber::registry()
            .with(configured_filter("info", Some("info,openraft=not-a-level")));
        tracing::subscriber::with_default(subscriber, || {
            assert!(!tracing::enabled!(
                target: "openraft::raft",
                Level::ERROR
            ));
            assert!(tracing::enabled!(target: "mantissa", Level::INFO));
        });
    }
}
