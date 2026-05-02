use std::io;

use tracing::{debug, warn};

/// Returns true when one I/O error kind maps to an expected disconnect path.
pub(super) fn is_expected_disconnect(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
    )
}

/// Emit one transport diagnostic with consistent fields for log correlation.
///
/// The transport code uses this helper instead of logging ad hoc warnings so
/// profiling and cluster-wide disconnect analysis can filter on stable targets
/// and stage names.
pub(super) fn log_transport_io(stage: &'static str, direction: &'static str, err: &io::Error) {
    if is_expected_disconnect(err.kind()) {
        debug!(
            target: "diag.transport",
            direction = direction,
            stage = stage,
            error_kind = ?err.kind(),
            error = %err,
            "noise transport disconnected"
        );
    } else {
        warn!(
            target: "diag.transport",
            direction = direction,
            stage = stage,
            error_kind = ?err.kind(),
            error = %err,
            "noise transport I/O error"
        );
    }
}
