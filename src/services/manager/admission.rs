use super::*;

/// Identifies one externally visible public endpoint by port and transport protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) struct PublicPortSelector {
    port: u16,
    protocol: ServicePortProtocol,
}

impl std::fmt::Display for PublicPortSelector {
    /// Formats one selector as `port/protocol` for operator-facing conflict errors.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.port,
            public_port_protocol_label(self.protocol)
        )
    }
}

/// Captures one template-level public endpoint claim extracted from a service manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PublicPortClaim {
    pub(super) selector: PublicPortSelector,
    pub(super) template_name: String,
}

/// Expands one deployment manifest into its concrete public endpoint claims.
///
/// Validation happens here so deploy-time admission and runtime store scans use the same
/// definition of a legal public endpoint declaration.
pub(super) fn collect_public_port_claims(
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
) -> anyhow::Result<Vec<PublicPortClaim>> {
    let mut seen = HashMap::new();
    let mut claims = Vec::new();

    for template in task_templates {
        let Some(port) = template.public_port() else {
            continue;
        };
        if template.required_network_ids().len() != 1 {
            return Err(anyhow!(
                "service '{}' template '{}' must attach to exactly one network when public_port is set",
                service_name,
                template.name
            ));
        }

        for protocol in template.public_protocols() {
            let selector = PublicPortSelector { port, protocol };
            if let Some(existing_template) = seen.insert(selector, template.name.clone()) {
                return Err(anyhow!(
                    "service '{}' declares duplicate public port {} on templates '{}' and '{}'",
                    service_name,
                    selector,
                    existing_template,
                    template.name
                ));
            }
            claims.push(PublicPortClaim {
                selector,
                template_name: template.name.clone(),
            });
        }
    }

    Ok(claims)
}

/// Validates that one service does not declare overlapping public and static host ports.
pub(super) fn ensure_public_ports_do_not_overlap_template_host_ports(
    service_name: &str,
    public_claims: &[PublicPortClaim],
    task_templates: &[TaskTemplateSpecValue],
) -> anyhow::Result<()> {
    if public_claims.is_empty() {
        return Ok(());
    }

    for template in task_templates {
        for port in &template.execution.ports {
            if let Some(public_claim) = public_claims
                .iter()
                .find(|claim| public_claim_conflicts_host_port(claim, port))
            {
                return Err(anyhow!(
                    "service '{service_name}' template '{}' cannot reserve host port {}/{} because template '{}' already claims public port {}",
                    template.name,
                    port.host_port,
                    workload_port_protocol_label(port.protocol),
                    public_claim.template_name,
                    public_claim.selector
                ));
            }
        }
    }

    Ok(())
}

/// Returns true when one public endpoint and one workload host port share a socket namespace.
pub(super) fn public_claim_conflicts_host_port(
    claim: &PublicPortClaim,
    port: &WorkloadPortBinding,
) -> bool {
    claim.selector.port == port.host_port
        && public_protocol_conflicts_workload(claim.selector.protocol, port.protocol)
}

/// Returns true when one service public protocol includes one workload transport protocol.
pub(super) fn public_protocol_conflicts_workload(
    public: ServicePortProtocol,
    workload: WorkloadPortProtocol,
) -> bool {
    match public {
        ServicePortProtocol::Tcp => workload == WorkloadPortProtocol::Tcp,
        ServicePortProtocol::Udp => workload == WorkloadPortProtocol::Udp,
        ServicePortProtocol::TcpUdp => true,
    }
}

/// Returns true when two workload host port bindings contend for the same local socket.
pub(super) fn workload_host_ports_conflict(
    left: &WorkloadPortBinding,
    right: &WorkloadPortBinding,
) -> bool {
    if left.host_port != right.host_port || left.protocol != right.protocol {
        return false;
    }

    let Ok(left_ip) = left.host_ip.trim().parse::<IpAddr>() else {
        return true;
    };
    let Ok(right_ip) = right.host_ip.trim().parse::<IpAddr>() else {
        return true;
    };

    same_ip_family(left_ip, right_ip)
        && (left_ip == right_ip || left_ip.is_unspecified() || right_ip.is_unspecified())
}

/// Returns true when any host port in both sets would contend on one node.
pub(super) fn workload_host_port_sets_conflict(
    left: &[WorkloadPortBinding],
    right: &[WorkloadPortBinding],
) -> bool {
    left.iter().any(|left_port| {
        right
            .iter()
            .any(|right_port| workload_host_ports_conflict(left_port, right_port))
    })
}

/// Returns true when two IP addresses belong to the same family.
fn same_ip_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

/// Returns whether a service state still reserves its declared public endpoint claims.
pub(super) fn service_reserves_public_ports(status: ServiceStatus) -> bool {
    !matches!(status, ServiceStatus::Stopping | ServiceStatus::Stopped)
}

/// Renders one public endpoint protocol label used in validation and conflict messages.
pub(super) fn public_port_protocol_label(protocol: ServicePortProtocol) -> &'static str {
    match protocol {
        ServicePortProtocol::Tcp => "tcp",
        ServicePortProtocol::Udp => "udp",
        ServicePortProtocol::TcpUdp => "tcp+udp",
    }
}

/// Renders one workload host-port protocol label for conflict messages.
pub(super) fn workload_port_protocol_label(protocol: WorkloadPortProtocol) -> &'static str {
    match protocol {
        WorkloadPortProtocol::Tcp => "tcp",
        WorkloadPortProtocol::Udp => "udp",
    }
}

/// Validates service declarations whose behavior depends on referenced network drivers.
pub(super) fn validate_network_contracts(
    service_name: &str,
    task_templates: &[TaskTemplateSpecValue],
    network_registry: &NetworkRegistry,
) -> anyhow::Result<()> {
    for template in task_templates {
        if template.public_port().is_none() {
            continue;
        }

        for network_id in template.required_network_ids() {
            let Some(network) = network_registry.get_spec(network_id)? else {
                continue;
            };
            if network.driver.is_node_local() {
                return Err(anyhow!(
                    "service '{}' template '{}' cannot set public_port on bridge network '{}' ({})",
                    service_name,
                    template.name,
                    network.name,
                    network.id
                ));
            }
        }
    }

    Ok(())
}
