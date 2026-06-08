#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mantissa::scheduler::placement::{
    PlacementConstraint, PlacementConstraintOperator, PlacementConstraintSelector, PlacementNode,
    PlacementPolicy, PlacementStrategy,
};
use mantissa::topology::peers::PeerLabel;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

const MAX_CONSTRAINTS: usize = 32;
const MAX_LABELS: usize = 16;
const MAX_RAW_EXPRESSION_CHUNKS: usize = 64;
const MAX_TEXT_BYTES: usize = 128;

#[derive(Arbitrary, Debug)]
struct PlacementInput {
    node: GeneratedNode,
    constraints: Vec<GeneratedConstraint>,
    strategy: GeneratedStrategy,
}

#[derive(Arbitrary, Debug)]
struct GeneratedNode {
    node_id: [u8; 16],
    hostname: Vec<u8>,
    address: GeneratedAddress,
    platform_os: Vec<u8>,
    platform_arch: Vec<u8>,
    labels: Vec<GeneratedLabel>,
}

#[derive(Arbitrary, Debug)]
enum GeneratedAddress {
    Raw(Vec<u8>),
    Ipv4 {
        octets: [u8; 4],
        port: u16,
        socket: bool,
    },
    Ipv6 {
        segments: [u16; 8],
        port: u16,
        socket: bool,
    },
}

#[derive(Arbitrary, Debug)]
struct GeneratedLabel {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct GeneratedConstraint {
    selector: GeneratedSelector,
    value: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
enum GeneratedSelector {
    Id,
    Hostname,
    Ip,
    Address,
    PlatformOs,
    PlatformArch,
    Label { key: Vec<u8> },
}

#[derive(Arbitrary, Debug)]
enum GeneratedStrategy {
    Spread,
    Binpack,
}

fuzz_target!(|data: &[u8]| {
    assert_raw_expression_parsing_is_stable(data, &PlacementNode::default());

    let mut unstructured = Unstructured::new(data);
    let Ok(input) = PlacementInput::arbitrary(&mut unstructured) else {
        return;
    };

    let node = build_node(&input.node);

    assert_raw_expression_parsing_is_stable(data, &node);
    assert_known_matching_constraints(&node);
    assert_generated_constraints_are_stable(&input.constraints, &input.strategy, &node);
});

/// Exercises arbitrary expression text through parse, render, and match paths.
fn assert_raw_expression_parsing_is_stable(data: &[u8], node: &PlacementNode) {
    for raw in data.chunks(MAX_TEXT_BYTES).take(MAX_RAW_EXPRESSION_CHUNKS) {
        let expression = bounded_lossy(raw);
        let Ok(parsed) = PlacementConstraint::parse_expression(&expression) else {
            continue;
        };

        assert_parse_render_roundtrips(&parsed);
        let matched = parsed.matches(node);
        let policy = PlacementPolicy {
            constraints: vec![parsed],
            strategy: PlacementStrategy::Spread,
        };
        assert_eq!(policy.matches(node), matched);
    }
}

/// Verifies constraints built from actual node fields match the node.
fn assert_known_matching_constraints(node: &PlacementNode) {
    assert_constraint_matches(
        PlacementConstraintSelector::NodeId,
        node.node_id.to_string(),
        node,
    );
    assert_constraint_matches(
        PlacementConstraintSelector::NodeHostname,
        node.hostname.clone(),
        node,
    );
    assert_constraint_matches(
        PlacementConstraintSelector::NodeAddress,
        node.address.clone(),
        node,
    );
    assert_constraint_matches(
        PlacementConstraintSelector::NodePlatformOs,
        node.platform_os.clone(),
        node,
    );
    assert_constraint_matches(
        PlacementConstraintSelector::NodePlatformArch,
        node.platform_arch.clone(),
        node,
    );

    if let Some(ip) = node.ip_addr() {
        assert_constraint_matches(PlacementConstraintSelector::NodeIp, ip.to_string(), node);
        assert_constraint_matches(PlacementConstraintSelector::NodeIp, matching_cidr(ip), node);
    }

    for label in &node.labels {
        assert_constraint_matches(
            PlacementConstraintSelector::node_label(label.key.clone()),
            label.value.clone(),
            node,
        );
    }
}

/// Exercises typed constraints and policy conjunction semantics.
fn assert_generated_constraints_are_stable(
    generated: &[GeneratedConstraint],
    strategy: &GeneratedStrategy,
    node: &PlacementNode,
) {
    let mut constraints = Vec::new();
    for constraint in generated.iter().take(MAX_CONSTRAINTS) {
        let selector = selector_from_generated(&constraint.selector);
        let value = bounded_token(&constraint.value);

        let Ok(eq) = PlacementConstraint::eq(selector.clone(), value.clone()) else {
            continue;
        };
        let ne = PlacementConstraint::ne(selector, value)
            .expect("validated equality constraint should also validate inequality");

        assert_parse_render_roundtrips(&eq);
        assert_parse_render_roundtrips(&ne);
        assert_eq!(eq.operator(), PlacementConstraintOperator::Eq);
        assert_eq!(ne.operator(), PlacementConstraintOperator::Ne);
        assert_eq!(eq.matches(node), !ne.matches(node));

        constraints.push(eq);
    }

    let policy = PlacementPolicy {
        constraints,
        strategy: strategy_from_generated(strategy),
    };
    assert_eq!(
        policy.matches(node),
        policy
            .constraints
            .iter()
            .all(|constraint| constraint.matches(node))
    );
    assert_eq!(
        policy.rendered_constraints().len(),
        policy.constraints.len()
    );
    assert_eq!(policy.is_unconstrained(), policy.constraints.is_empty());
}

/// Verifies one matching equality constraint and its negation stay complementary.
fn assert_constraint_matches(
    selector: PlacementConstraintSelector,
    value: impl Into<String>,
    node: &PlacementNode,
) {
    let value = value.into();
    let eq = PlacementConstraint::eq(selector.clone(), value.clone())
        .expect("known node value should build a valid equality constraint");
    let ne = PlacementConstraint::ne(selector, value)
        .expect("known node value should build a valid inequality constraint");

    assert!(eq.matches(node));
    assert!(!ne.matches(node));
    assert_parse_render_roundtrips(&eq);
    assert_parse_render_roundtrips(&ne);
}

/// Verifies accepted constraints survive render and parse exactly.
fn assert_parse_render_roundtrips(constraint: &PlacementConstraint) {
    let rendered = constraint.render_expression();
    let reparsed = PlacementConstraint::parse_expression(&rendered)
        .expect("rendered placement constraint should parse");
    assert_eq!(&reparsed, constraint);
    assert_eq!(reparsed.render_expression(), rendered);
}

/// Builds one scheduler-visible placement node from generated data.
fn build_node(input: &GeneratedNode) -> PlacementNode {
    PlacementNode::new(
        Uuid::from_bytes(input.node_id),
        bounded_token(&input.hostname),
        address_from_generated(&input.address),
        bounded_token(&input.platform_os),
        bounded_token(&input.platform_arch),
        labels_from_generated(&input.labels),
    )
}

/// Builds one public placement selector from generated selector data.
fn selector_from_generated(input: &GeneratedSelector) -> PlacementConstraintSelector {
    match input {
        GeneratedSelector::Id => PlacementConstraintSelector::NodeId,
        GeneratedSelector::Hostname => PlacementConstraintSelector::NodeHostname,
        GeneratedSelector::Ip => PlacementConstraintSelector::NodeIp,
        GeneratedSelector::Address => PlacementConstraintSelector::NodeAddress,
        GeneratedSelector::PlatformOs => PlacementConstraintSelector::NodePlatformOs,
        GeneratedSelector::PlatformArch => PlacementConstraintSelector::NodePlatformArch,
        GeneratedSelector::Label { key } => {
            let key = bounded_token(key);
            PlacementConstraintSelector::node_label(key)
        }
    }
}

/// Maps generated placement strategy data to the public strategy enum.
fn strategy_from_generated(input: &GeneratedStrategy) -> PlacementStrategy {
    match input {
        GeneratedStrategy::Spread => PlacementStrategy::Spread,
        GeneratedStrategy::Binpack => PlacementStrategy::Binpack,
    }
}

/// Builds one address string that may or may not be IP parseable.
fn address_from_generated(input: &GeneratedAddress) -> String {
    match input {
        GeneratedAddress::Raw(bytes) => bounded_token(bytes),
        GeneratedAddress::Ipv4 {
            octets,
            port,
            socket,
        } => {
            let ip = Ipv4Addr::from(*octets);
            if *socket {
                format!("{ip}:{port}")
            } else {
                ip.to_string()
            }
        }
        GeneratedAddress::Ipv6 {
            segments,
            port,
            socket,
        } => {
            let ip = Ipv6Addr::new(
                segments[0],
                segments[1],
                segments[2],
                segments[3],
                segments[4],
                segments[5],
                segments[6],
                segments[7],
            );
            if *socket {
                format!("[{ip}]:{port}")
            } else {
                ip.to_string()
            }
        }
    }
}

/// Builds bounded peer labels while preserving the first value for duplicate keys.
fn labels_from_generated(labels: &[GeneratedLabel]) -> Vec<PeerLabel> {
    let mut out = Vec::new();
    for label in labels.iter().take(MAX_LABELS) {
        let key = bounded_token(&label.key);
        let value = bounded_token(&label.value);
        if out.iter().any(|known: &PeerLabel| known.key == key) {
            continue;
        }
        out.push(PeerLabel { key, value });
    }
    out
}

/// Returns a CIDR string guaranteed to contain the provided IP.
fn matching_cidr(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => format!("{ip}/32"),
        IpAddr::V6(ip) => format!("{ip}/128"),
    }
}

/// Converts arbitrary bytes into bounded lossy text for raw expression parsing.
fn bounded_lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_TEXT_BYTES)]).to_string()
}

/// Converts arbitrary bytes into a bounded non-empty ASCII token.
fn bounded_token(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_.:-";
    let mut out = String::with_capacity(bytes.len().min(MAX_TEXT_BYTES));
    for byte in bytes.iter().take(MAX_TEXT_BYTES) {
        out.push(ALPHABET[usize::from(*byte) % ALPHABET.len()] as char);
    }
    if out.is_empty() { "v".to_string() } else { out }
}
