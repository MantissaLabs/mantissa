use crate::cluster::ClusterViewId;
use crate::topology::operation::SplitNodeAssignment;
use capnp::Error;
use protocol::topology::split_selector_clause::Operator as SplitOperator;
use std::collections::HashSet;
use uuid::Uuid;

/// Parsed split selector clause used to evaluate one node attribute predicate.
#[derive(Clone, Debug)]
pub(super) struct SplitSelectorClauseSpec {
    pub(super) key: String,
    pub(super) op: SplitOperator,
    pub(super) value: String,
}

/// Parsed split target with selector clauses and explicit node overrides.
#[derive(Clone, Debug)]
pub(super) struct SplitTargetSpec {
    pub(super) name: String,
    pub(super) clauses: Vec<SplitSelectorClauseSpec>,
    pub(super) explicit_nodes: HashSet<Uuid>,
}

/// Candidate node attributes used during split target selection and assignment.
#[derive(Clone, Debug)]
pub(super) struct SplitNodeCandidate {
    pub(super) node_id: Uuid,
    pub(super) hostname: String,
    pub(super) address: String,
    pub(super) wireguard_enabled: bool,
    pub(super) cpu_vendor: Option<String>,
    pub(super) cpu_brand: Option<String>,
    pub(super) cpu_logical: Option<u64>,
    pub(super) cpu_cores: Option<u64>,
    pub(super) memory_total_kb: Option<u64>,
    pub(super) gpu_vendor: Option<String>,
    pub(super) gpu_count: Option<u64>,
    pub(super) gpu_models: Vec<String>,
}

/// Computes deterministic split assignments and validates selector coverage for all nodes.
pub(super) fn build_split_assignments_for_nodes(
    source_view: ClusterViewId,
    targets: &[SplitTargetSpec],
    nodes: &[SplitNodeCandidate],
) -> Result<Vec<SplitNodeAssignment>, Error> {
    if targets.is_empty() {
        return Err(Error::failed(
            "split assignment requires at least one target".to_string(),
        ));
    }
    if nodes.is_empty() {
        return Err(Error::failed(
            "split assignment requires at least one node candidate".to_string(),
        ));
    }

    let selectorless = targets
        .iter()
        .all(|target| target.clauses.is_empty() && target.explicit_nodes.is_empty());
    if selectorless {
        return Ok(assign_split_targets_by_order(
            source_view,
            nodes,
            targets.len(),
        ));
    }

    let fallback_targets = targets
        .iter()
        .enumerate()
        .filter_map(|(idx, target)| {
            if target.clauses.is_empty() && target.explicit_nodes.is_empty() {
                Some(idx)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if fallback_targets.len() > 1 {
        return Err(Error::failed(
            "split supports at most one fallback target without selectors".to_string(),
        ));
    }
    let fallback_target = fallback_targets.first().copied();

    let mut assignments = Vec::with_capacity(nodes.len());
    let mut per_target_count = vec![0usize; targets.len()];

    for node in nodes {
        let explicit_matches = targets
            .iter()
            .enumerate()
            .filter(|(_, target)| target.explicit_nodes.contains(&node.node_id))
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        if explicit_matches.len() > 1 {
            return Err(Error::failed(format!(
                "node {} is explicitly assigned to multiple split targets",
                node.node_id
            )));
        }

        let chosen = if let Some(index) = explicit_matches.first().copied() {
            index
        } else {
            let mut selector_matches = Vec::new();
            for (idx, target) in targets.iter().enumerate() {
                if Some(idx) == fallback_target {
                    continue;
                }
                if split_target_matches_node(target, node)? {
                    selector_matches.push(idx);
                }
            }

            match selector_matches.as_slice() {
                [] => fallback_target.ok_or_else(|| {
                    Error::failed(format!(
                        "node {} did not match any split target selectors",
                        node.node_id
                    ))
                })?,
                [only] => *only,
                _ => {
                    return Err(Error::failed(format!(
                        "node {} matched multiple split target selectors",
                        node.node_id
                    )));
                }
            }
        };

        per_target_count[chosen] = per_target_count[chosen].saturating_add(1);
        assignments.push(SplitNodeAssignment {
            node_id: node.node_id,
            target_index: chosen,
        });
    }

    for (index, count) in per_target_count.into_iter().enumerate() {
        if Some(index) == fallback_target {
            continue;
        }
        if count == 0 {
            return Err(Error::failed(format!(
                "split target '{}' has no matched nodes",
                targets[index].name
            )));
        }
    }

    assignments.sort_by_key(|assignment| assignment.node_id);
    Ok(assignments)
}

/// Assigns nodes to split targets deterministically when no explicit selectors are provided.
fn assign_split_targets_by_order(
    source_view: ClusterViewId,
    nodes: &[SplitNodeCandidate],
    target_count: usize,
) -> Vec<SplitNodeAssignment> {
    let offset = source_view.epoch as usize % target_count;
    let mut assignments = Vec::with_capacity(nodes.len());
    for (index, node) in nodes.iter().enumerate() {
        assignments.push(SplitNodeAssignment {
            node_id: node.node_id,
            target_index: (index + offset) % target_count,
        });
    }
    assignments.sort_by_key(|assignment| assignment.node_id);
    assignments
}

/// Evaluates whether one split target selector matches the provided node candidate.
fn split_target_matches_node(
    target: &SplitTargetSpec,
    node: &SplitNodeCandidate,
) -> Result<bool, Error> {
    if target.clauses.is_empty() {
        return Ok(true);
    }

    for clause in &target.clauses {
        if !evaluate_split_clause(node, clause)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluates one selector clause against one node candidate in split assignment planning.
fn evaluate_split_clause(
    node: &SplitNodeCandidate,
    clause: &SplitSelectorClauseSpec,
) -> Result<bool, Error> {
    match clause.key.as_str() {
        "node.id" => match clause.op {
            SplitOperator::Eq => Ok(node.node_id.to_string() == clause.value),
            SplitOperator::Ne => Ok(node.node_id.to_string() != clause.value),
            _ => Err(Error::failed(
                "node.id supports only eq/ne operators".to_string(),
            )),
        },
        "node.hostname" => match clause.op {
            SplitOperator::Eq => Ok(node.hostname == clause.value),
            SplitOperator::Ne => Ok(node.hostname != clause.value),
            _ => Err(Error::failed(
                "node.hostname supports only eq/ne operators".to_string(),
            )),
        },
        "node.address" => match clause.op {
            SplitOperator::Eq => Ok(node.address == clause.value),
            SplitOperator::Ne => Ok(node.address != clause.value),
            _ => Err(Error::failed(
                "node.address supports only eq/ne operators".to_string(),
            )),
        },
        "wireguard.enabled" => {
            let expected = parse_split_boolean(&clause.value).ok_or_else(|| {
                Error::failed(format!(
                    "wireguard.enabled expects a boolean value, got '{}'",
                    clause.value
                ))
            })?;
            match clause.op {
                SplitOperator::Eq => Ok(node.wireguard_enabled == expected),
                SplitOperator::Ne => Ok(node.wireguard_enabled != expected),
                _ => Err(Error::failed(
                    "wireguard.enabled supports only eq/ne operators".to_string(),
                )),
            }
        }
        "resources.cpu.logical" => evaluate_u64_clause(
            node,
            &clause.key,
            clause.op,
            &clause.value,
            node.cpu_logical,
        ),
        "resources.cpu.cores" => {
            evaluate_u64_clause(node, &clause.key, clause.op, &clause.value, node.cpu_cores)
        }
        "resources.memory.total_kb" => evaluate_u64_clause(
            node,
            &clause.key,
            clause.op,
            &clause.value,
            node.memory_total_kb,
        ),
        "resources.memory.total_bytes" => evaluate_u64_clause(
            node,
            &clause.key,
            clause.op,
            &clause.value,
            node.memory_total_kb.map(|kb| kb.saturating_mul(1024)),
        ),
        "resources.gpu.count" => {
            evaluate_u64_clause(node, &clause.key, clause.op, &clause.value, node.gpu_count)
        }
        "resources.cpu.vendor" => match clause.op {
            SplitOperator::Eq => Ok(node.cpu_vendor.as_deref() == Some(clause.value.as_str())),
            SplitOperator::Ne => Ok(node.cpu_vendor.as_deref() != Some(clause.value.as_str())),
            _ => Err(Error::failed(
                "resources.cpu.vendor supports only eq/ne operators".to_string(),
            )),
        },
        "resources.cpu.brand" => match clause.op {
            SplitOperator::Eq => Ok(node.cpu_brand.as_deref() == Some(clause.value.as_str())),
            SplitOperator::Ne => Ok(node.cpu_brand.as_deref() != Some(clause.value.as_str())),
            _ => Err(Error::failed(
                "resources.cpu.brand supports only eq/ne operators".to_string(),
            )),
        },
        "resources.gpu.vendor" => match clause.op {
            SplitOperator::Eq => Ok(node.gpu_vendor.as_deref() == Some(clause.value.as_str())),
            SplitOperator::Ne => Ok(node.gpu_vendor.as_deref() != Some(clause.value.as_str())),
            _ => Err(Error::failed(
                "resources.gpu.vendor supports only eq/ne operators".to_string(),
            )),
        },
        "resources.gpu.model" => match clause.op {
            SplitOperator::Eq => Ok(node.gpu_models.iter().any(|model| model == &clause.value)),
            SplitOperator::Ne => Ok(node.gpu_models.iter().all(|model| model != &clause.value)),
            _ => Err(Error::failed(
                "resources.gpu.model supports only eq/ne operators".to_string(),
            )),
        },
        _ => Err(Error::failed(format!(
            "unsupported split selector key '{}'",
            clause.key
        ))),
    }
}

/// Evaluates one numeric selector clause against an optional node metric.
fn evaluate_u64_clause(
    node: &SplitNodeCandidate,
    key: &str,
    op: SplitOperator,
    expected_raw: &str,
    actual: Option<u64>,
) -> Result<bool, Error> {
    let expected = parse_split_u64(expected_raw, key)?;
    let actual = actual.ok_or_else(|| {
        Error::failed(format!(
            "node {} has no metric for selector key '{}'",
            node.node_id, key
        ))
    })?;
    match op {
        SplitOperator::Eq => Ok(actual == expected),
        SplitOperator::Ne => Ok(actual != expected),
        SplitOperator::Gt => Ok(actual > expected),
        SplitOperator::Gte => Ok(actual >= expected),
        SplitOperator::Lt => Ok(actual < expected),
        SplitOperator::Lte => Ok(actual <= expected),
    }
}

/// Parses a split selector numeric operand as an unsigned integer.
fn parse_split_u64(value: &str, key: &str) -> Result<u64, Error> {
    value.parse::<u64>().map_err(|_| {
        Error::failed(format!(
            "selector key '{key}' expects an unsigned integer value, got '{value}'"
        ))
    })
}

/// Parses a textual boolean selector value accepted by split selector clauses.
fn parse_split_boolean(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}
