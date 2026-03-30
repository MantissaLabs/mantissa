//! Image pull helpers for the Docker runtime backend.
//!
//! Keeping the pull stream formatting and dedupe logic separate makes the main
//! backend implementation easier to scan and keeps image concerns away from
//! attach and lifecycle code.

use std::collections::HashMap;
use std::time::Duration;

use bollard::models::CreateImageInfo;

use super::DockerRuntimeBackend;

/// Snapshot of one pull-stream update used to suppress duplicate log spam.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct PullProgressLogState {
    status: Option<String>,
    current: Option<i64>,
    total: Option<i64>,
}

impl DockerRuntimeBackend {
    /// Builds one stable dedupe key for a Docker image-pull stream update.
    pub(super) fn pull_progress_log_state(update: &CreateImageInfo) -> PullProgressLogState {
        let (current, total) = update
            .progress_detail
            .as_ref()
            .map(|detail| (detail.current, detail.total))
            .unwrap_or((None, None));
        PullProgressLogState {
            status: update.status.clone(),
            current,
            total,
        }
    }

    /// Formats one image-pull update for logs without repeating Docker's full
    /// JSON payload.
    pub(super) fn format_pull_status(update: &CreateImageInfo) -> Option<String> {
        let status = update.status.as_deref()?;
        let id = update.id.as_deref();
        let (current, total) = update
            .progress_detail
            .as_ref()
            .map(|detail| (detail.current, detail.total))
            .unwrap_or((None, None));

        match (id, current, total) {
            (Some(id), Some(current), Some(total)) => {
                Some(format!("{status} {id} ({current}/{total})"))
            }
            (Some(id), _, _) => Some(format!("{status} {id}")),
            (None, Some(current), Some(total)) => Some(format!("{status} ({current}/{total})")),
            (None, _, _) => Some(status.to_string()),
        }
    }

    /// Decides whether the next pull-stream update is new enough to log.
    pub(super) fn should_log_pull_update(
        last_updates: &mut HashMap<Option<String>, PullProgressLogState>,
        update: &CreateImageInfo,
    ) -> bool {
        let key = update.id.clone();
        let state = Self::pull_progress_log_state(update);
        match last_updates.get(&key) {
            Some(previous) if previous == &state => false,
            _ => {
                last_updates.insert(key, state);
                true
            }
        }
    }

    /// Converts an optional duration to Docker's timeout seconds format with a
    /// default.
    pub(super) fn timeout_seconds_or_default(timeout: Option<Duration>, default_secs: i32) -> i32 {
        timeout
            .map(|value| value.as_secs().min(i32::MAX as u64) as i32)
            .unwrap_or(default_secs)
    }
}
