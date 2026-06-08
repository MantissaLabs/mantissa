#![no_main]

use std::cmp::Ordering;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use mantissa_store::mvreg::{MvReg, VectorClock};
use uuid::Uuid;

const MAX_OPS: usize = 64;
const MAX_CLOCK_ENTRIES: usize = 16;
const MAX_VALUE_BYTES: usize = 128;

#[derive(Arbitrary, Debug)]
struct MvRegInput {
    left: Vec<Operation>,
    right: Vec<Operation>,
    third: Vec<Operation>,
    max_compacted_values: u8,
}

#[derive(Arbitrary, Debug)]
enum Operation {
    Write {
        actor: [u8; 16],
        value: Vec<u8>,
    },
    Put {
        clock: Vec<ClockEntry>,
        value: Vec<u8>,
    },
}

#[derive(Arbitrary, Debug)]
struct ClockEntry {
    actor: [u8; 16],
    counter: u64,
}

type Register = MvReg<Vec<u8>, Uuid>;
type CanonicalRegister = Vec<(Vec<([u8; 16], u64)>, Vec<u8>)>;

fuzz_target!(|input: MvRegInput| {
    let left = build_register(&input.left);
    let right = build_register(&input.right);
    let third = build_register(&input.third);

    assert_register_is_normalized(&left);
    assert_register_is_normalized(&right);
    assert_register_is_normalized(&third);
    assert_snapshot_is_canonical(&left);
    assert_snapshot_is_canonical(&right);
    assert_snapshot_is_canonical(&third);
    assert_merge_is_idempotent(&left);
    assert_merge_is_commutative(&left, &right);
    assert_merge_is_associative(&left, &right, &third);
    assert_compaction_preserves_clock_frontier(&left, input.max_compacted_values);
});

/// Builds one register by applying a bounded sequence of generated operations.
fn build_register(ops: &[Operation]) -> Register {
    let mut reg = MvReg::new();
    for op in ops.iter().take(MAX_OPS) {
        match op {
            Operation::Write { actor, value } => {
                reg.write(Uuid::from_bytes(*actor), bounded_bytes(value));
            }
            Operation::Put { clock, value } => {
                let mut vector = VectorClock::new();
                for entry in clock.iter().take(MAX_CLOCK_ENTRIES) {
                    vector.apply(Uuid::from_bytes(entry.actor), entry.counter);
                }
                reg.apply_put(vector, bounded_bytes(value));
            }
        }
    }
    reg
}

/// Verifies no visible entry is causally dominated by another visible entry.
fn assert_register_is_normalized(reg: &Register) {
    for (left_index, left) in reg.entries().iter().enumerate() {
        assert!(!left.clock().is_empty());
        for (right_index, right) in reg.entries().iter().enumerate() {
            if left_index == right_index {
                continue;
            }
            assert!(
                !matches!(
                    left.clock().partial_cmp(right.clock()),
                    Some(Ordering::Less)
                ),
                "MVReg retained a dominated entry"
            );
            assert_ne!(
                canonical_clock(left.clock()),
                canonical_clock(right.clock()),
                "MVReg retained duplicate clocks"
            );
        }
    }
}

/// Verifies snapshots are sorted and deduplicated for MST hashing.
fn assert_snapshot_is_canonical(reg: &Register) {
    let snapshot = reg.snapshot();
    let mut expected = reg.read_values();
    expected.sort();
    expected.dedup();
    assert_eq!(snapshot.as_slice(), expected.as_slice());
}

/// Verifies merging a register into itself does not change visible state.
fn assert_merge_is_idempotent(reg: &Register) {
    let mut merged = reg.clone();
    merged.merge(reg.clone());
    assert_eq!(canonical_register(&merged), canonical_register(reg));
}

/// Verifies merge order does not affect the converged register state.
fn assert_merge_is_commutative(left: &Register, right: &Register) {
    let mut left_then_right = left.clone();
    left_then_right.merge(right.clone());

    let mut right_then_left = right.clone();
    right_then_left.merge(left.clone());

    assert_eq!(
        canonical_register(&left_then_right),
        canonical_register(&right_then_left)
    );
}

/// Verifies three-way merges converge independent of grouping.
fn assert_merge_is_associative(left: &Register, right: &Register, third: &Register) {
    let mut left_group = left.clone();
    left_group.merge(right.clone());
    left_group.merge(third.clone());

    let mut right_group = right.clone();
    right_group.merge(third.clone());
    let mut regrouped = left.clone();
    regrouped.merge(right_group);

    assert_eq!(
        canonical_register(&left_group),
        canonical_register(&regrouped)
    );
}

/// Verifies compaction bounds entries and preserves the merged causal frontier.
fn assert_compaction_preserves_clock_frontier(reg: &Register, max_compacted_values: u8) {
    let max_values = usize::from(max_compacted_values % 16);
    if max_values == 0 {
        return;
    }

    let original_clock = reg.clock();
    let mut compacted = reg.clone();
    compacted.compact_with(max_values, |entry| entry.value().clone());

    assert!(compacted.entries().len() <= max_values);
    assert!(
        matches!(
            compacted.clock().partial_cmp(&original_clock),
            Some(Ordering::Equal | Ordering::Greater)
        ),
        "MVReg compaction lost causal clock frontier"
    );
    assert_register_is_normalized(&compacted);
}

/// Returns one canonical order-insensitive view of a register.
fn canonical_register(reg: &Register) -> CanonicalRegister {
    let mut entries = reg
        .entries()
        .iter()
        .map(|entry| (canonical_clock(entry.clock()), entry.value().clone()))
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

/// Returns one canonical byte-level view of a vector clock.
fn canonical_clock(clock: &VectorClock<Uuid>) -> Vec<([u8; 16], u64)> {
    clock
        .iter()
        .map(|(actor, counter)| (*actor.as_bytes(), counter))
        .collect()
}

/// Returns a bounded value payload for generated register values.
fn bounded_bytes(bytes: &[u8]) -> Vec<u8> {
    bytes.iter().copied().take(MAX_VALUE_BYTES).collect()
}
