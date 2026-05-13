use crate::services::types::TaskTemplateSpecValue;
use std::collections::BTreeMap;
use uuid::Uuid;

/// Describes a running task replica and the template replica that spawned it.
#[derive(Clone, Debug)]
pub struct ServiceReplicaAssignment {
    pub task_id: Uuid,
    pub template: String,
    pub replica: u16,
}

/// Represents a desired replica that must be (re)created to match the latest manifest.
#[derive(Clone, Debug)]
pub struct ReplicaReplacement {
    pub template: TaskTemplateSpecValue,
    pub replica: u16,
    pub previous: Option<ServiceReplicaAssignment>,
    pub desired_id: Uuid,
}

/// Change-plan returned by the reconciler describing how to move from current to desired state.
#[derive(Clone, Debug, Default)]
pub struct ServiceChangePlan {
    pub retain: Vec<ServiceReplicaAssignment>,
    pub replace: Vec<ReplicaReplacement>,
    pub remove: Vec<ServiceReplicaAssignment>,
}

impl ServiceChangePlan {
    /// True when no action is required because desired and current states already match.
    pub fn is_noop(&self) -> bool {
        self.retain.is_empty() && self.replace.is_empty() && self.remove.is_empty()
    }
}

/// Computes the precise set of replica mutations needed to honour a new manifest.
pub fn compute_change_plan(
    current_templates: &[TaskTemplateSpecValue],
    desired_templates: &[TaskTemplateSpecValue],
    assignments: Vec<ServiceReplicaAssignment>,
) -> ServiceChangePlan {
    let mut plan = ServiceChangePlan::default();

    let mut current_by_name: BTreeMap<String, &TaskTemplateSpecValue> = BTreeMap::new();
    for template in current_templates {
        current_by_name.insert(template.name.clone(), template);
    }

    let mut by_template: BTreeMap<String, BTreeMap<u16, ServiceReplicaAssignment>> =
        BTreeMap::new();
    for assignment in assignments {
        by_template
            .entry(assignment.template.clone())
            .or_default()
            .insert(assignment.replica, assignment);
    }

    for desired in desired_templates {
        let mut existing = by_template.remove(&desired.name).unwrap_or_default();

        let attributes_changed = current_by_name
            .get(&desired.name)
            .map(|current| template_attributes_changed(current, desired))
            .unwrap_or(true);

        for replica in 1..=desired.replicas {
            let prior = existing.remove(&replica);
            if attributes_changed || prior.is_none() {
                plan.replace.push(ReplicaReplacement {
                    template: desired.clone(),
                    replica,
                    previous: prior,
                    desired_id: Uuid::new_v4(),
                });
            } else if let Some(assignment) = prior {
                plan.retain.push(assignment);
            }
        }

        for leftover in existing.into_values() {
            plan.remove.push(leftover);
        }
    }

    for leftover in by_template.into_values() {
        for assignment in leftover.into_values() {
            plan.remove.push(assignment);
        }
    }

    plan
}

/// Extracts the template name and replica index from a managed task name.
pub fn parse_template_and_replica(service_name: &str, task_name: &str) -> Option<(String, u16)> {
    let prefix = format!("{service_name}-");
    let suffix = task_name.strip_prefix(&prefix)?;
    if suffix.is_empty() {
        return None;
    }

    let segments: Vec<&str> = suffix.split('-').collect();
    if segments.is_empty() {
        return None;
    }

    for idx in (0..segments.len()).rev() {
        if let Ok(replica) = segments[idx].parse::<u16>() {
            let template = segments[..idx].join("-");
            if template.is_empty() {
                return None;
            }
            return Some((template, replica));
        }
    }

    Some((suffix.to_string(), 1))
}

fn template_attributes_changed(
    current: &TaskTemplateSpecValue,
    desired: &TaskTemplateSpecValue,
) -> bool {
    current.image != desired.image
        || current.command != desired.command
        || current.cpu_millis != desired.cpu_millis
        || current.memory_bytes != desired.memory_bytes
        || current.gpu_count != desired.gpu_count
        || current.restart_policy != desired.restart_policy
        || current.termination_grace_period_secs != desired.termination_grace_period_secs
        || current.pre_stop_command != desired.pre_stop_command
        || current.env != desired.env
        || current.secret_files != desired.secret_files
        || current.volumes != desired.volumes
        || current.networks != desired.networks
        || current.ports != desired.ports
        || current.readiness != desired.readiness
        || current.liveness != desired.liveness
        || current.public_port != desired.public_port
        || current.public_protocol != desired.public_protocol
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::types::TaskTemplateNetworkRequirement;
    use crate::workload::types::{ExecutionSpec, WorkloadPortBinding, WorkloadPortProtocol};

    /// Builds one minimal task template for service reconciliation tests.
    fn template(name: &str) -> TaskTemplateSpecValue {
        TaskTemplateSpecValue {
            name: name.to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/api:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 100,
                memory_bytes: 64 * 1_024 * 1_024,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: vec![TaskTemplateNetworkRequirement::new(
                    "default",
                    Uuid::new_v4(),
                )],
                ports: Vec::new(),
                placement: Default::default(),
            },
            depends_on: Vec::new(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
            placement_preferences: Vec::new(),
        }
    }

    /// Changing static host ports should replace existing service replicas.
    #[test]
    fn host_port_changes_replace_service_replica() {
        let current = template("api");
        let mut desired = current.clone();
        desired.execution.ports = vec![WorkloadPortBinding {
            name: "http".to_string(),
            target_port: 8080,
            host_port: 18080,
            host_ip: "0.0.0.0".to_string(),
            protocol: WorkloadPortProtocol::Tcp,
        }];
        let previous_task = Uuid::new_v4();

        let plan = compute_change_plan(
            &[current],
            &[desired],
            vec![ServiceReplicaAssignment {
                task_id: previous_task,
                template: "api".to_string(),
                replica: 1,
            }],
        );

        assert_eq!(plan.retain.len(), 0);
        assert_eq!(plan.replace.len(), 1);
        assert_eq!(
            plan.replace[0].previous.as_ref().map(|prior| prior.task_id),
            Some(previous_task)
        );
    }
}
