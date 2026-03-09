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

/// Adds the default bollard directive when the user did not configure it.
///
/// The directive string is static, so parse failure would indicate a coding
/// error. Falling back to the original filter keeps logger initialization
/// non-fatal.
fn add_default_bollard_directive(filter: EnvFilter) -> EnvFilter {
    match "bollard::docker=warn".parse() {
        Ok(directive) => filter.add_directive(directive),
        Err(_) => filter,
    }
}

/// Initialize pretty logs for binaries. Idempotent.
/// Respects `RUST_LOG`, defaults to `info`.
pub fn init() -> io::Result<()> {
    if INIT.get().is_some() {
        return Ok(());
    }

    // Route `log` crate records into `tracing` (idempotent: ignore error).
    let _ = LogTracer::init();

    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if !rust_log_mentions("bollard") {
        filter = add_default_bollard_directive(filter);
    }
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
    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    if !rust_log_mentions("bollard") {
        filter = add_default_bollard_directive(filter);
    }

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

/// Return true when `RUST_LOG` explicitly references the provided target substring.
fn rust_log_mentions(target: &str) -> bool {
    env::var("RUST_LOG")
        .map(|raw| raw.contains(target))
        .unwrap_or(false)
}
