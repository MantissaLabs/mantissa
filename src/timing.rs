use std::time::Duration;

/// Maximum jitter window applied to one periodic background loop.
const PERIODIC_JITTER_MAX: Duration = Duration::from_secs(1);
/// Divisor used to derive the default jitter window from the base interval.
const PERIODIC_JITTER_DIVISOR: u32 = 10;

/// # Description:
///
/// Computes one bounded symmetric jitter around a base interval so large
/// fleets do not align periodic background work on the same wall-clock
/// boundaries.
pub(crate) fn jittered_interval(base: Duration) -> Duration {
    use ::rand::Rng as _;

    let jitter_window = (base / PERIODIC_JITTER_DIVISOR).min(PERIODIC_JITTER_MAX);
    let jitter_window_nanos = jitter_window.as_nanos().min(u128::from(u64::MAX)) as u64;
    if jitter_window_nanos == 0 {
        return base;
    }

    let base_nanos = base.as_nanos().min(u128::from(u64::MAX)) as u64;
    let mut rng = ::rand::rng();
    let offset_nanos = rng.random_range(0..=jitter_window_nanos.saturating_mul(2));
    Duration::from_nanos(
        base_nanos
            .saturating_sub(jitter_window_nanos)
            .saturating_add(offset_nanos),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jittered_interval_stays_within_expected_window() {
        let base = Duration::from_secs(5);
        let min = Duration::from_millis(4500);
        let max = Duration::from_millis(5500);

        for _ in 0..256 {
            let actual = jittered_interval(base);
            assert!(actual >= min, "expected {actual:?} to be >= {min:?}");
            assert!(actual <= max, "expected {actual:?} to be <= {max:?}");
        }
    }

    #[test]
    fn jittered_interval_caps_large_windows() {
        let base = Duration::from_secs(30);
        let min = Duration::from_secs(29);
        let max = Duration::from_secs(31);

        for _ in 0..256 {
            let actual = jittered_interval(base);
            assert!(actual >= min, "expected {actual:?} to be >= {min:?}");
            assert!(actual <= max, "expected {actual:?} to be <= {max:?}");
        }
    }
}
