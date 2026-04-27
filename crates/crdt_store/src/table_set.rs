use redb::TableDefinition;

pub trait TableSet {
    const VALUES: &'static str;
    const TOMBS: &'static str;
    const TOMBS_BY_OBSERVED: &'static str;
    const META: &'static str;

    #[inline]
    fn values() -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        TableDefinition::new(Self::VALUES)
    }

    #[inline]
    fn tombs() -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        TableDefinition::new(Self::TOMBS)
    }

    #[inline]
    fn tombs_by_observed() -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        TableDefinition::new(Self::TOMBS_BY_OBSERVED)
    }

    #[inline]
    fn meta() -> TableDefinition<'static, &'static str, u64> {
        TableDefinition::new(Self::META)
    }
}
