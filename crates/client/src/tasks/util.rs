use crate::config::ClientConfig;
use crate::connection;
use anyhow::{Result, anyhow};
use capnp::Error as CapnpError;
use protocol::task::TaskStateFilter;
use uuid::Uuid;

pub fn uuid_from_data(data: capnp::data::Reader) -> Result<Uuid, CapnpError> {
    let bytes = data.to_owned();
    let slice: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| CapnpError::failed("invalid uuid".to_string()))?;
    Ok(Uuid::from_bytes(slice))
}

pub fn uuid_to_string(data: capnp::data::Reader) -> Result<String, CapnpError> {
    Ok(uuid_from_data(data)?.to_string())
}

pub fn uuid_short(data: capnp::data::Reader) -> Result<String, CapnpError> {
    let uuid = uuid_from_data(data)?;
    Ok(uuid
        .to_string()
        .split('-')
        .next()
        .unwrap_or_default()
        .to_string())
}

/// Resolves one operator-provided task identifier as a full UUID or unique prefix.
pub async fn resolve_task_id(cfg: &ClientConfig, raw: &str) -> Result<Uuid> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("task id must not be empty"));
    }

    if let Ok(id) = Uuid::parse_str(trimmed) {
        return Ok(id);
    }

    let client = connection::get_local_session(cfg).await?;
    let request = client.get_task_request();
    let task = request.send().pipeline.get_task();
    let mut request = task.list_request();
    {
        let mut builder = request.get().init_request();
        let states = [
            TaskStateFilter::Pending,
            TaskStateFilter::Creating,
            TaskStateFilter::VolumeUnavailable,
            TaskStateFilter::Running,
            TaskStateFilter::Stopping,
            TaskStateFilter::Paused,
            TaskStateFilter::Stopped,
            TaskStateFilter::Failed,
            TaskStateFilter::Exited,
            TaskStateFilter::Unknown,
        ];
        let mut state_builder = builder.reborrow().init_states(states.len() as u32);
        for (idx, state) in states.iter().enumerate() {
            state_builder.set(idx as u32, *state);
        }
    }

    let response = request.send().promise.await?;
    let tasks = response.get()?.get_tasks()?;
    let mut ids = Vec::with_capacity(tasks.len() as usize);
    for spec in tasks.iter() {
        ids.push(uuid_from_data(spec.get_id()?)?);
    }

    match_task_id_prefix(trimmed, &ids)
}

/// Matches one task id or prefix against the current visible task identifiers.
fn match_task_id_prefix(raw: &str, ids: &[Uuid]) -> Result<Uuid> {
    let canonical_prefix = raw.trim().to_ascii_lowercase();
    let compact_prefix = canonical_prefix.replace('-', "");
    if compact_prefix.is_empty() {
        return Err(anyhow!("task id must not be empty"));
    }

    let mut matches = Vec::new();
    for id in ids {
        let full = id.to_string();
        let compact = full.replace('-', "");
        if full.starts_with(&canonical_prefix) || compact.starts_with(&compact_prefix) {
            matches.push(*id);
        }
    }

    matches.sort_unstable();
    matches.dedup();

    match matches.len() {
        0 => Err(anyhow!(
            "unknown task id or prefix '{raw}'; use `mantissa tasks list --no-trunc` to inspect full ids"
        )),
        1 => Ok(matches[0]),
        _ => {
            let candidates = matches
                .iter()
                .map(Uuid::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            Err(anyhow!(
                "task id prefix '{raw}' is ambiguous; matches: {candidates}"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::match_task_id_prefix;
    use uuid::Uuid;

    #[test]
    fn match_task_id_prefix_accepts_unique_short_prefix() {
        let id = Uuid::parse_str("956bc5ba-0f2c-4d3f-8a07-fd9f1f72b8c1").expect("uuid");
        let resolved = match_task_id_prefix("956bc5ba", &[id]).expect("resolve prefix");
        assert_eq!(resolved, id);
    }

    #[test]
    fn match_task_id_prefix_accepts_compact_prefix_across_hyphen_boundary() {
        let id = Uuid::parse_str("956bc5ba-0f2c-4d3f-8a07-fd9f1f72b8c1").expect("uuid");
        let resolved = match_task_id_prefix("956bc5ba0f2c", &[id]).expect("resolve prefix");
        assert_eq!(resolved, id);
    }

    #[test]
    fn match_task_id_prefix_rejects_ambiguous_prefix() {
        let a = Uuid::parse_str("956bc5ba-0f2c-4d3f-8a07-fd9f1f72b8c1").expect("uuid");
        let b = Uuid::parse_str("956bc5ba-11aa-4d3f-8a07-fd9f1f72b8c2").expect("uuid");
        let error = match_task_id_prefix("956bc5ba", &[a, b]).expect_err("ambiguous prefix");
        assert!(error.to_string().contains("ambiguous"));
    }
}
