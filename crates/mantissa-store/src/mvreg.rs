use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

/// Stable, hashable snapshot of the active values stored in one MVReg.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MvRegSnapshot<T> {
    vals: Vec<T>,
}

impl<T> MvRegSnapshot<T> {
    /// Builds a canonical snapshot from unsorted register values.
    pub fn from_unsorted(mut vals: Vec<T>) -> Self
    where
        T: Ord,
    {
        vals.sort();
        vals.dedup();
        Self { vals }
    }

    /// Builds a canonical snapshot from values that may still contain duplicates.
    pub fn new_sorted(mut vals: Vec<T>) -> Self
    where
        T: Ord,
    {
        vals.sort();
        vals.dedup();
        Self { vals }
    }

    /// Returns the canonical active values represented by this snapshot.
    pub fn as_slice(&self) -> &[T] {
        &self.vals
    }
}

impl<T: Hash> Hash for MvRegSnapshot<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vals.hash(state);
    }
}

/// Vector clock used by Mantissa-owned MVReg storage.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct VectorClock<A>
where
    A: Ord,
{
    dots: BTreeMap<A, u64>,
}

impl<A> VectorClock<A>
where
    A: Ord,
{
    /// Builds an empty vector clock.
    pub fn new() -> Self {
        Self {
            dots: BTreeMap::new(),
        }
    }

    /// Returns whether this clock contains no actor counters.
    pub fn is_empty(&self) -> bool {
        self.dots.is_empty()
    }

    /// Returns the number of actor counters carried by this clock.
    pub fn len(&self) -> usize {
        self.dots.len()
    }

    /// Returns the counter for one actor, treating missing actors as zero.
    pub fn get(&self, actor: &A) -> u64 {
        self.dots.get(actor).copied().unwrap_or(0)
    }

    /// Returns all actor counters in canonical actor order.
    pub fn iter(&self) -> impl Iterator<Item = (&A, u64)> {
        self.dots.iter().map(|(actor, counter)| (actor, *counter))
    }

    /// Applies one actor counter by keeping the maximum observed value.
    pub fn apply(&mut self, actor: A, counter: u64) {
        if self.get(&actor) < counter {
            self.dots.insert(actor, counter);
        }
    }

    /// Increments one actor counter in place while preserving monotonic ordering.
    pub fn increment(&mut self, actor: A)
    where
        A: Clone,
    {
        let next = self.get(&actor).saturating_add(1);
        self.apply(actor, next);
    }

    /// Merges another vector clock into this one.
    pub fn merge(&mut self, other: &Self)
    where
        A: Clone,
    {
        for (actor, counter) in other.iter() {
            self.apply(actor.clone(), counter);
        }
    }
}

impl<A> Default for VectorClock<A>
where
    A: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> PartialOrd for VectorClock<A>
where
    A: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else if other
            .dots
            .iter()
            .all(|(actor, counter)| self.get(actor) >= *counter)
        {
            Some(Ordering::Greater)
        } else if self
            .dots
            .iter()
            .all(|(actor, counter)| other.get(actor) >= *counter)
        {
            Some(Ordering::Less)
        } else {
            None
        }
    }
}

/// One MVReg value together with the vector clock that introduced it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MvRegEntry<V, A>
where
    A: Ord,
{
    clock: VectorClock<A>,
    value: V,
}

impl<V, A> MvRegEntry<V, A>
where
    A: Ord,
{
    /// Builds one register entry from an explicit vector clock and value.
    pub fn new(clock: VectorClock<A>, value: V) -> Self {
        Self { clock, value }
    }

    /// Returns the vector clock associated with this register entry.
    pub fn clock(&self) -> &VectorClock<A> {
        &self.clock
    }

    /// Returns the value associated with this register entry.
    pub fn value(&self) -> &V {
        &self.value
    }
}

/// Mantissa-owned multi-value register used for stable storage encoding.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MvReg<V, A>
where
    A: Ord,
{
    entries: Vec<MvRegEntry<V, A>>,
}

impl<V, A> MvReg<V, A>
where
    A: Ord,
{
    /// Builds an empty multi-value register.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Builds a register from explicit entries and normalizes dominated values.
    pub fn from_entries(entries: Vec<MvRegEntry<V, A>>) -> Self
    where
        A: Clone,
    {
        let mut reg = Self::new();
        for entry in entries {
            reg.apply_put(entry.clock, entry.value);
        }
        reg
    }

    /// Returns the active entries currently stored in the register.
    pub fn entries(&self) -> &[MvRegEntry<V, A>] {
        &self.entries
    }

    /// Returns the currently visible concurrent values.
    pub fn read_values(&self) -> Vec<V>
    where
        V: Clone,
    {
        self.entries
            .iter()
            .map(|entry| entry.value.clone())
            .collect()
    }

    /// Returns one canonical snapshot of the currently visible values.
    pub fn snapshot(&self) -> MvRegSnapshot<V>
    where
        V: Clone + Ord,
    {
        MvRegSnapshot::from_unsorted(self.read_values())
    }

    /// Returns the merged vector clock for all active entries.
    pub fn clock(&self) -> VectorClock<A>
    where
        A: Clone,
    {
        let mut clock = VectorClock::new();
        for entry in &self.entries {
            clock.merge(&entry.clock);
        }
        clock
    }

    /// Writes one value by incrementing the provided actor in the current read context.
    pub fn write(&mut self, actor: A, value: V)
    where
        A: Clone,
    {
        let mut clock = self.clock();
        clock.increment(actor);
        self.apply_put(clock, value);
    }

    /// Applies one explicit put operation to the register.
    pub fn apply_put(&mut self, clock: VectorClock<A>, value: V) {
        if clock.is_empty() {
            return;
        }

        self.entries.retain(|entry| {
            matches!(
                entry.clock.partial_cmp(&clock),
                None | Some(Ordering::Greater)
            )
        });

        let should_add = self
            .entries
            .iter()
            .all(|entry| !matches!(entry.clock.partial_cmp(&clock), Some(Ordering::Greater)));

        if should_add {
            self.entries.push(MvRegEntry { clock, value });
        }
    }

    /// Merges another register into this one and preserves only non-dominated entries.
    pub fn merge(&mut self, other: Self) {
        self.entries.retain(|entry| {
            !other.entries.iter().any(|other_entry| {
                matches!(
                    entry.clock.partial_cmp(&other_entry.clock),
                    Some(Ordering::Less)
                )
            })
        });

        let incoming = other
            .entries
            .into_iter()
            .filter(|entry| {
                !self.entries.iter().any(|current| {
                    matches!(
                        entry.clock.partial_cmp(&current.clock),
                        Some(Ordering::Less)
                    )
                })
            })
            .filter(|entry| {
                self.entries
                    .iter()
                    .all(|current| entry.clock != current.clock)
            })
            .collect::<Vec<_>>();

        self.entries.extend(incoming);
    }

    /// Compacts this register by keeping the highest-ranked active entries.
    ///
    /// The rank function must be deterministic from durable register state.
    /// Dropped entries are not simply removed: their vector clocks are merged
    /// into the highest-ranked retained entry so stale peers cannot later
    /// reintroduce those dropped values as concurrent register entries.
    pub fn compact_with<F, R>(&mut self, max_values: usize, mut rank: F) -> bool
    where
        A: Clone,
        V: Clone + Ord,
        F: FnMut(&MvRegEntry<V, A>) -> R,
        R: Ord,
    {
        if max_values == 0 || self.entries.len() <= max_values {
            return false;
        }

        let mut ranked = self
            .entries
            .iter()
            .enumerate()
            .map(|(index, entry)| RankedEntry {
                index,
                rank: rank(entry),
                value: entry.value.clone(),
                clock: entry.clock.clone(),
            })
            .collect::<Vec<_>>();
        ranked.sort_by(compare_ranked_entries_desc);

        let absorb_index = ranked[0].index;
        let retained = ranked
            .iter()
            .take(max_values)
            .map(|entry| entry.index)
            .collect::<BTreeSet<_>>();
        let mut absorbed_clock = VectorClock::new();

        let mut compacted_entries = Vec::with_capacity(max_values);
        for (index, entry) in self.entries.drain(..).enumerate() {
            if retained.contains(&index) {
                compacted_entries.push((index, entry));
            } else {
                absorbed_clock.merge(&entry.clock);
            }
        }

        for (index, entry) in &mut compacted_entries {
            if *index == absorb_index {
                entry.clock.merge(&absorbed_clock);
                break;
            }
        }

        for (_, entry) in compacted_entries {
            self.apply_put(entry.clock, entry.value);
        }

        true
    }
}

/// Cached ranking data used to choose MVReg compaction winners deterministically.
struct RankedEntry<R, V, A>
where
    A: Ord,
{
    index: usize,
    rank: R,
    value: V,
    clock: VectorClock<A>,
}

/// Sorts higher-ranked entries first, with deterministic durable-state tie breaks.
fn compare_ranked_entries_desc<R, V, A>(
    left: &RankedEntry<R, V, A>,
    right: &RankedEntry<R, V, A>,
) -> Ordering
where
    R: Ord,
    V: Ord,
    A: Ord,
{
    right
        .rank
        .cmp(&left.rank)
        .then_with(|| right.value.cmp(&left.value))
        .then_with(|| total_clock_cmp_desc(&left.clock, &right.clock))
        .then_with(|| left.index.cmp(&right.index))
}

/// Compares vector clocks as sorted maps instead of using causal partial order.
fn total_clock_cmp_desc<A>(left: &VectorClock<A>, right: &VectorClock<A>) -> Ordering
where
    A: Ord,
{
    right.dots.cmp(&left.dots)
}

impl<V, A> Default for MvReg<V, A>
where
    A: Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{MvReg, MvRegEntry, MvRegSnapshot, VectorClock};
    use crdts::{CmRDT, CvRDT, MVReg};
    use uuid::Uuid;

    /// Builds a deterministic test actor id from a small integer.
    fn actor(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    /// Builds a one-actor vector clock for deterministic compaction fixtures.
    fn clock(actor: Uuid, counter: u64) -> VectorClock<Uuid> {
        let mut clock = VectorClock::new();
        clock.apply(actor, counter);
        clock
    }

    /// Builds one explicit MVReg entry for deterministic compaction fixtures.
    fn entry(actor: Uuid, counter: u64, value: &str) -> MvRegEntry<String, Uuid> {
        MvRegEntry::new(clock(actor, counter), value.to_string())
    }

    /// Applies one write to the oracle MVReg from the `crdts` crate.
    fn oracle_write(reg: &mut MVReg<String, Uuid>, actor: Uuid, value: &str) {
        let read_ctx = reg.read();
        let add_ctx = read_ctx.derive_add_ctx(actor);
        let op = reg.write(value.to_string(), add_ctx);
        reg.apply(op);
    }

    /// Returns sorted visible values from the oracle register.
    fn oracle_values(reg: &MVReg<String, Uuid>) -> Vec<String> {
        let mut values = reg.read().val;
        values.sort();
        values
    }

    /// Returns sorted visible values from the Mantissa register.
    fn mantissa_values(reg: &MvReg<String, Uuid>) -> Vec<String> {
        let mut values = reg.read_values();
        values.sort();
        values
    }

    /// A single write should create one visible value with matching oracle behavior.
    #[test]
    fn mantissa_mvreg_matches_oracle_for_single_write() {
        let actor_a = actor(1);
        let mut oracle = MVReg::new();
        let mut mantissa = MvReg::new();

        oracle_write(&mut oracle, actor_a, "one");
        mantissa.write(actor_a, "one".to_string());

        assert_eq!(mantissa_values(&mantissa), oracle_values(&oracle));
    }

    /// Repeated writes by the same actor should replace the dominated older value.
    #[test]
    fn mantissa_mvreg_matches_oracle_for_repeated_actor_write() {
        let actor_a = actor(1);
        let mut oracle = MVReg::new();
        let mut mantissa = MvReg::new();

        oracle_write(&mut oracle, actor_a, "old");
        oracle_write(&mut oracle, actor_a, "new");
        mantissa.write(actor_a, "old".to_string());
        mantissa.write(actor_a, "new".to_string());

        assert_eq!(mantissa_values(&mantissa), oracle_values(&oracle));
        assert_eq!(mantissa_values(&mantissa), vec!["new".to_string()]);
    }

    /// Concurrent writes by different actors should both remain visible after merge.
    #[test]
    fn mantissa_mvreg_matches_oracle_for_concurrent_writes() {
        let actor_a = actor(1);
        let actor_b = actor(2);
        let mut oracle_left = MVReg::new();
        let mut oracle_right = MVReg::new();
        let mut mantissa_left = MvReg::new();
        let mut mantissa_right = MvReg::new();

        oracle_write(&mut oracle_left, actor_a, "left");
        oracle_write(&mut oracle_right, actor_b, "right");
        mantissa_left.write(actor_a, "left".to_string());
        mantissa_right.write(actor_b, "right".to_string());

        oracle_left.merge(oracle_right);
        mantissa_left.merge(mantissa_right);

        assert_eq!(mantissa_values(&mantissa_left), oracle_values(&oracle_left));
        assert_eq!(
            mantissa_values(&mantissa_left),
            vec!["left".to_string(), "right".to_string()]
        );
    }

    /// Merging the same concurrent registers in either direction should converge.
    #[test]
    fn mantissa_mvreg_merge_converges() {
        let actor_a = actor(1);
        let actor_b = actor(2);
        let mut left = MvReg::new();
        let mut right = MvReg::new();

        left.write(actor_a, "left".to_string());
        right.write(actor_b, "right".to_string());

        let mut left_then_right = left.clone();
        left_then_right.merge(right.clone());
        let mut right_then_left = right;
        right_then_left.merge(left);

        assert_eq!(
            mantissa_values(&left_then_right),
            mantissa_values(&right_then_left)
        );
    }

    /// Writes based on a prior value should dominate and remove that prior value.
    #[test]
    fn mantissa_mvreg_matches_oracle_for_dominated_entry_removal() {
        let actor_a = actor(1);
        let actor_b = actor(2);
        let mut oracle_old = MVReg::new();
        let mut mantissa_old = MvReg::new();

        oracle_write(&mut oracle_old, actor_a, "old");
        mantissa_old.write(actor_a, "old".to_string());

        let mut oracle_new = oracle_old.clone();
        let mut mantissa_new = mantissa_old.clone();
        oracle_write(&mut oracle_new, actor_b, "new");
        mantissa_new.write(actor_b, "new".to_string());

        oracle_old.merge(oracle_new);
        mantissa_old.merge(mantissa_new);

        assert_eq!(mantissa_values(&mantissa_old), oracle_values(&oracle_old));
        assert_eq!(mantissa_values(&mantissa_old), vec!["new".to_string()]);
    }

    /// Writing after a malformed maxed actor counter must not panic or wrap.
    #[test]
    fn mantissa_mvreg_write_saturates_max_counter() {
        let actor_a = actor(1);
        let actor_b = actor(2);
        let mut reg = MvReg::from_entries(vec![entry(actor_a, u64::MAX, "max")]);

        reg.write(actor_a, "same-actor".to_string());
        reg.write(actor_b, "other-actor".to_string());

        let merged_clock = reg.clock();
        assert_eq!(merged_clock.get(&actor_a), u64::MAX);
        assert_eq!(merged_clock.get(&actor_b), 1);
        assert_eq!(mantissa_values(&reg), vec!["other-actor".to_string()]);
    }

    /// Snapshots must remain sorted and deduplicated for stable MST hashing.
    #[test]
    fn mantissa_mvreg_snapshot_is_canonical() {
        let actor_a = actor(1);
        let actor_b = actor(2);
        let mut reg = MvReg::new();

        reg.write(actor_a, "z".to_string());
        reg.merge({
            let mut other = MvReg::new();
            other.write(actor_b, "a".to_string());
            other
        });

        assert_eq!(
            reg.snapshot(),
            MvRegSnapshot::from_unsorted(vec!["z".to_string(), "a".to_string()])
        );
        assert_eq!(
            reg.snapshot().as_slice(),
            &["a".to_string(), "z".to_string()]
        );
    }

    /// Compaction should be a no-op when the register is already within policy.
    #[test]
    fn mvreg_compaction_noops_below_limit() {
        let mut reg = MvReg::from_entries(vec![entry(actor(1), 1, "a"), entry(actor(2), 1, "b")]);
        let before = reg.clone();

        assert!(!reg.compact_with(2, |entry| entry.value().clone()));
        assert_eq!(reg, before);
    }

    /// Compaction should keep the highest-ranked values and drop older winners.
    #[test]
    fn mvreg_compaction_keeps_highest_ranked_values() {
        let mut reg = MvReg::from_entries(vec![
            entry(actor(1), 1, "a"),
            entry(actor(2), 1, "b"),
            entry(actor(3), 1, "c"),
        ]);

        assert!(reg.compact_with(2, |entry| entry.value().clone()));
        assert_eq!(
            mantissa_values(&reg),
            vec!["b".to_string(), "c".to_string()]
        );
    }

    /// Dropped clocks must be absorbed so stale peers cannot revive dropped values.
    #[test]
    fn mvreg_compaction_absorbs_dropped_clocks() {
        let dropped_actor = actor(1);
        let winner_actor = actor(2);
        let mut reg = MvReg::from_entries(vec![
            entry(dropped_actor, 1, "old"),
            entry(winner_actor, 1, "winner"),
        ]);

        assert!(reg.compact_with(1, |entry| entry.value().clone()));

        let winner = reg
            .entries()
            .iter()
            .find(|entry| entry.value() == "winner")
            .unwrap();
        assert_eq!(winner.clock().get(&dropped_actor), 1);
        assert_eq!(winner.clock().get(&winner_actor), 1);
    }

    /// A stale peer sending a dropped entry should be dominated after compaction.
    #[test]
    fn mvreg_compaction_prevents_stale_value_reintroduction() {
        let dropped_actor = actor(1);
        let winner_actor = actor(2);
        let mut compacted = MvReg::from_entries(vec![
            entry(dropped_actor, 1, "old"),
            entry(winner_actor, 1, "winner"),
        ]);
        let stale = MvReg::from_entries(vec![entry(dropped_actor, 1, "old")]);

        compacted.compact_with(1, |entry| entry.value().clone());
        compacted.merge(stale);

        assert_eq!(mantissa_values(&compacted), vec!["winner".to_string()]);
    }

    /// Equal ranks should still compact deterministically from durable state.
    #[test]
    fn mvreg_compaction_breaks_rank_ties_deterministically() {
        let entry_a = entry(actor(1), 1, "a");
        let entry_b = entry(actor(2), 1, "b");
        let mut left = MvReg::from_entries(vec![entry_a.clone(), entry_b.clone()]);
        let mut right = MvReg::from_entries(vec![entry_b, entry_a]);

        left.compact_with(1, |_| 0u8);
        right.compact_with(1, |_| 0u8);

        assert_eq!(left, right);
        assert_eq!(mantissa_values(&left), vec!["b".to_string()]);
    }
}
