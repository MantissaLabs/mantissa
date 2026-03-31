use crate::services::types::TaskTemplateSpecValue;
use anyhow::{Result, anyhow};
use std::collections::HashMap;

/// One topological dependency stage for service task templates.
///
/// Every template in one stage may launch concurrently because all of its
/// declared upstream template dependencies have already been satisfied by
/// earlier stages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TemplateDependencyStage {
    pub(super) template_indices: Vec<usize>,
}

/// Validates task-template dependency metadata and returns deterministic
/// topological stages for deployment ordering.
///
/// Dependencies are scoped to template names within one service manifest. This
/// enforces unique template names, rejects unknown/self/duplicate dependencies,
/// and groups task templates by dependency depth so deployment can launch them stage
/// by stage.
pub(super) fn build_template_dependency_stages(
    task_templates: &[TaskTemplateSpecValue],
) -> Result<Vec<TemplateDependencyStage>> {
    let mut name_to_index = HashMap::with_capacity(task_templates.len());
    for (index, template) in task_templates.iter().enumerate() {
        if template.name.trim().is_empty() {
            return Err(anyhow!("task template name cannot be empty"));
        }
        if name_to_index.insert(template.name.clone(), index).is_some() {
            return Err(anyhow!(
                "task template '{}' is declared multiple times",
                template.name
            ));
        }
    }

    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); task_templates.len()];
    let mut indegree = vec![0usize; task_templates.len()];

    for (index, template) in task_templates.iter().enumerate() {
        let mut seen_dependencies: HashMap<&str, ()> = HashMap::new();
        for dependency in &template.depends_on {
            let dependency_name = dependency.trim();
            if dependency_name.is_empty() {
                return Err(anyhow!(
                    "task template '{}' contains an empty depends_on entry",
                    template.name
                ));
            }
            if dependency_name == template.name {
                return Err(anyhow!(
                    "task template '{}' cannot depend on itself",
                    template.name
                ));
            }
            if seen_dependencies.insert(dependency_name, ()).is_some() {
                return Err(anyhow!(
                    "task template '{}' depends on '{}' more than once",
                    template.name,
                    dependency_name
                ));
            }

            let Some(&dependency_index) = name_to_index.get(dependency_name) else {
                return Err(anyhow!(
                    "task template '{}' depends on unknown template '{}'",
                    template.name,
                    dependency_name
                ));
            };

            adjacency[dependency_index].push(index);
            indegree[index] = indegree[index].saturating_add(1);
        }
    }

    let mut current_stage: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, degree)| (*degree == 0).then_some(index))
        .collect();
    let mut visited = 0usize;
    let mut stages = Vec::new();

    while !current_stage.is_empty() {
        visited = visited.saturating_add(current_stage.len());
        stages.push(TemplateDependencyStage {
            template_indices: current_stage.clone(),
        });

        let mut next_ready = vec![false; task_templates.len()];
        for index in &current_stage {
            for dependent in &adjacency[*index] {
                indegree[*dependent] = indegree[*dependent].saturating_sub(1);
                if indegree[*dependent] == 0 {
                    next_ready[*dependent] = true;
                }
            }
        }

        current_stage = next_ready
            .into_iter()
            .enumerate()
            .filter_map(|(index, ready)| ready.then_some(index))
            .collect();
    }

    if visited != task_templates.len() {
        return Err(anyhow!(
            "task template dependencies contain a cycle and cannot be ordered"
        ));
    }

    Ok(stages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workload::types::ExecutionSpec;

    /// Builds a minimal task-template value for dependency graph tests.
    fn template(name: &str, depends_on: &[&str]) -> TaskTemplateSpecValue {
        TaskTemplateSpecValue {
            name: name.to_string(),
            execution: ExecutionSpec {
                image: "ghcr.io/demo/image:latest".to_string(),
                command: Vec::new(),
                tty: false,
                cpu_millis: 0,
                memory_bytes: 0,
                gpu_count: 0,
                restart_policy: None,
                termination_grace_period_secs: None,
                pre_stop_command: None,
                liveness: None,
                env: Vec::new(),
                secret_files: Vec::new(),
                volumes: Vec::new(),
                networks: Vec::new(),
            },
            depends_on: depends_on.iter().map(|entry| entry.to_string()).collect(),
            replicas: 1,
            readiness: None,
            public_port: None,
            public_protocol: None,
        }
    }

    /// Ensures dependency stages preserve manifest order within each topological layer.
    #[test]
    fn build_template_dependency_stages_orders_layers_deterministically() {
        let stages = build_template_dependency_stages(&[
            template("backend", &[]),
            template("frontend", &["backend"]),
            template("metrics", &["backend"]),
        ])
        .expect("dependency stages");

        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].template_indices, vec![0]);
        assert_eq!(stages[1].template_indices, vec![1, 2]);
    }

    /// Ensures cyclic dependencies are rejected before deployment proceeds.
    #[test]
    fn build_template_dependency_stages_rejects_cycles() {
        let error = build_template_dependency_stages(&[
            template("backend", &["frontend"]),
            template("frontend", &["backend"]),
        ])
        .expect_err("cyclic dependency graph must fail");

        assert!(error.to_string().contains("contain a cycle"));
    }
}
