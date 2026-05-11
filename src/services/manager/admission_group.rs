use super::*;

/// Internal admission group scope derived from one service controller operation.
pub(super) enum ServiceAdmissionGroupScope<'a> {
    ServiceGeneration,
    DependencyStage {
        stage_index: usize,
        template_indices: &'a [usize],
        task_templates: &'a [TaskTemplateSpecValue],
    },
    RolloutChunk {
        stage_index: usize,
        chunk_indices: &'a [usize],
        replacements: &'a [ReplicaReplacement],
    },
}

/// Computes a stable admission group id for one service admission boundary.
pub(super) fn compute_service_admission_group_id(
    service_id: Uuid,
    manifest_id: Uuid,
    service_epoch: u64,
    scope: ServiceAdmissionGroupScope<'_>,
) -> anyhow::Result<Uuid> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mantissa-service-admission-group-v1");
    hasher.update(service_id.as_bytes());
    hasher.update(manifest_id.as_bytes());
    hasher.update(&service_epoch.to_le_bytes());

    match scope {
        ServiceAdmissionGroupScope::ServiceGeneration => {
            hasher.update(b"service-generation");
        }
        ServiceAdmissionGroupScope::DependencyStage {
            stage_index,
            template_indices,
            task_templates,
        } => {
            hash_dependency_stage_scope(
                &mut hasher,
                stage_index,
                template_indices,
                task_templates,
            )?;
        }
        ServiceAdmissionGroupScope::RolloutChunk {
            stage_index,
            chunk_indices,
            replacements,
        } => {
            hash_rollout_chunk_scope(&mut hasher, stage_index, chunk_indices, replacements)?;
        }
    }

    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(Uuid::from_bytes(bytes))
}

/// Converts a zero-based dependency stage index into an operator-facing stage number.
pub(super) fn service_admission_stage_number(stage_index: usize) -> anyhow::Result<u64> {
    let zero_based = u64::try_from(stage_index)
        .map_err(|_| anyhow!("dependency stage index does not fit in u64"))?;
    zero_based
        .checked_add(1)
        .ok_or_else(|| anyhow!("dependency stage number overflowed u64"))
}

/// Hashes one dependency stage as a service admission-group scope.
fn hash_dependency_stage_scope(
    hasher: &mut blake3::Hasher,
    stage_index: usize,
    template_indices: &[usize],
    task_templates: &[TaskTemplateSpecValue],
) -> anyhow::Result<()> {
    let stage_index = u64::try_from(stage_index)
        .map_err(|_| anyhow!("dependency stage index does not fit in u64"))?;
    hasher.update(b"dependency-stage");
    hasher.update(&stage_index.to_le_bytes());
    for template_index in template_indices {
        let template = task_templates.get(*template_index).ok_or_else(|| {
            anyhow!(
                "dependency admission group references missing template index {}",
                template_index
            )
        })?;
        let template_index = u64::try_from(*template_index)
            .map_err(|_| anyhow!("template index does not fit in u64"))?;
        hasher.update(&template_index.to_le_bytes());
        hash_variable_bytes(hasher, template.name.as_bytes())?;
    }
    Ok(())
}

/// Hashes one rollout replacement chunk as a service admission-group scope.
fn hash_rollout_chunk_scope(
    hasher: &mut blake3::Hasher,
    stage_index: usize,
    chunk_indices: &[usize],
    replacements: &[ReplicaReplacement],
) -> anyhow::Result<()> {
    let stage_index = u64::try_from(stage_index)
        .map_err(|_| anyhow!("rollout stage index does not fit in u64"))?;
    hasher.update(b"rollout-chunk");
    hasher.update(&stage_index.to_le_bytes());
    for replacement_index in chunk_indices {
        let replacement = replacements.get(*replacement_index).ok_or_else(|| {
            anyhow!(
                "rollout admission group references missing replacement index {}",
                replacement_index
            )
        })?;
        let replacement_index = u64::try_from(*replacement_index)
            .map_err(|_| anyhow!("replacement index does not fit in u64"))?;
        hasher.update(&replacement_index.to_le_bytes());
        hash_variable_bytes(hasher, replacement.template.name.as_bytes())?;
        hasher.update(&replacement.replica.to_le_bytes());
        hasher.update(replacement.desired_id.as_bytes());
        match &replacement.previous {
            Some(previous) => {
                hasher.update(&[1]);
                hasher.update(previous.task_id.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    Ok(())
}

/// Adds length-delimited bytes to a stable hash input so variable fields cannot overlap.
fn hash_variable_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) -> anyhow::Result<()> {
    let len = u64::try_from(bytes.len())
        .map_err(|_| anyhow!("admission group hash input is too large"))?;
    hasher.update(&len.to_le_bytes());
    hasher.update(bytes);
    Ok(())
}
