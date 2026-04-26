use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
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

    /// Increments one actor counter in place.
    pub fn increment(&mut self, actor: A)
    where
        A: Clone,
    {
        let next = self.get(&actor) + 1;
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
    use super::{MvReg, MvRegSnapshot};
    use crdts::{CmRDT, CvRDT, MVReg};
    use uuid::Uuid;

    /// Builds a deterministic test actor id from a small integer.
    fn actor(n: u128) -> Uuid {
        Uuid::from_u128(n)
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
}
