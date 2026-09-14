/// Converts CPU quantities to the exact millicore count sent to the daemon.
pub(crate) fn parse_cpu(raw: &str) -> Result<u64, String> {
    let value = raw.trim();
    let (number, scale) = value
        .strip_suffix('m')
        .map_or((value, 1000), |number| (number, 1));

    parse_scaled_quantity(number, scale).map_err(|reason| {
        format!("invalid CPU quantity '{raw}': {reason}; use cores (0.5) or millicores (500m)")
    })
}

/// Converts memory and capacity quantities to bytes before submitting a request.
pub(crate) fn parse_bytes(raw: &str) -> Result<u64, String> {
    let value = raw.trim();
    let unit_start = value
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(unit_start);
    let scale = match unit.trim() {
        "" | "B" => 1,
        "kB" | "KB" => 1000,
        "MB" => 1000_u64.pow(2),
        "GB" => 1000_u64.pow(3),
        "TB" => 1000_u64.pow(4),
        "PB" => 1000_u64.pow(5),
        "EB" => 1000_u64.pow(6),
        "Ki" | "KiB" => 1 << 10,
        "Mi" | "MiB" => 1 << 20,
        "Gi" | "GiB" => 1 << 30,
        "Ti" | "TiB" => 1 << 40,
        "Pi" | "PiB" => 1 << 50,
        "Ei" | "EiB" => 1 << 60,
        _ => {
            return Err(format!(
                "unknown byte unit in '{raw}'; use B, MB, MiB, GiB, or larger units"
            ));
        }
    };

    parse_scaled_quantity(number, scale)
        .map_err(|reason| format!("invalid byte quantity '{raw}': {reason}"))
}

/// Uses integer arithmetic so requests are never rounded up or silently truncated.
fn parse_scaled_quantity(number: &str, scale: u64) -> Result<u64, String> {
    let number = number.trim();
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err("expected a positive decimal number".to_string());
    }

    let whole = whole.parse::<u64>().map_err(|_| "quantity is too large")?;
    let whole = whole.checked_mul(scale).ok_or("quantity is too large")?;

    let fraction = fraction.trim_end_matches('0');
    let fractional_units = if fraction.is_empty() {
        0
    } else {
        let digits = u32::try_from(fraction.len()).map_err(|_| "too many decimal places")?;
        let divisor = 10_u128
            .checked_pow(digits)
            .ok_or("too many decimal places")?;
        let numerator = fraction
            .parse::<u128>()
            .ok()
            .and_then(|value| value.checked_mul(u128::from(scale)))
            .ok_or("too many decimal places")?;
        if numerator % divisor != 0 {
            return Err("quantity must resolve to whole bytes or millicores".to_string());
        }

        u64::try_from(numerator / divisor).map_err(|_| "quantity is too large")?
    };

    let quantity = whole
        .checked_add(fractional_units)
        .ok_or("quantity is too large")?;
    if quantity == 0 {
        return Err("quantity must be greater than zero".to_string());
    }

    Ok(quantity)
}

/// Shows fractional CPUs below one core in millicores and larger requests in cores.
pub(crate) fn format_cpu(millicores: u64) -> String {
    if millicores < 1000 {
        return format!("{millicores}m");
    }

    let cores = millicores / 1000;
    let remainder = millicores % 1000;
    if remainder == 0 {
        return cores.to_string();
    }

    let fraction = format!("{remainder:03}");
    format!("{cores}.{}", fraction.trim_end_matches('0'))
}

/// Uses binary units consistently for memory and storage in terminal output.
pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    let formatted = format!("{value:.1}");
    format!("{} {}", formatted.trim_end_matches(".0"), UNITS[unit])
}

/// Preserves the distinction between missing capacity information and zero bytes.
pub(crate) fn format_optional_bytes(bytes: Option<u64>) -> String {
    bytes.map(format_bytes).unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Equivalent CPU units must produce identical admission requests without floating-point loss.
    #[test]
    fn cpu_quantities_preserve_millicore_precision() {
        assert_eq!(parse_cpu("500m").unwrap(), 500);
        assert_eq!(parse_cpu("0.5").unwrap(), 500);
        assert_eq!(parse_cpu("1.001").unwrap(), 1001);
        assert_eq!(parse_cpu("2").unwrap(), 2000);
        assert_eq!(parse_cpu("18446744073709551.615").unwrap(), u64::MAX);
    }

    /// Invalid requests must fail before admission rather than round into a different CPU limit.
    #[test]
    fn cpu_quantities_reject_zero_rounding_and_overflow() {
        for input in [
            "0",
            "-1",
            "NaN",
            "1e3",
            "0.0001",
            "0.5m",
            "18446744073709551.616",
        ] {
            assert!(parse_cpu(input).is_err(), "accepted {input}");
        }
    }

    /// Decimal and binary suffixes have different scales, and fractional sizes must remain exact.
    #[test]
    fn byte_quantities_distinguish_decimal_and_binary_units() {
        assert_eq!(parse_bytes("512").unwrap(), 512);
        assert_eq!(parse_bytes("512B").unwrap(), 512);
        assert_eq!(parse_bytes("500MB").unwrap(), 500_000_000);
        assert_eq!(parse_bytes("512MiB").unwrap(), 512 << 20);
        assert_eq!(parse_bytes("512Mi").unwrap(), 512 << 20);
        assert_eq!(parse_bytes("1.5GiB").unwrap(), 1536 << 20);
        assert_eq!(parse_bytes(" 1 KiB ").unwrap(), 1024);
        assert_eq!(parse_bytes("0.125KiB").unwrap(), 128);
        assert_eq!(parse_bytes("18446744073709551615B").unwrap(), u64::MAX);
    }

    /// Unsupported units and non-integral byte counts must never silently change a memory request.
    #[test]
    fn byte_quantities_reject_invalid_values_and_overflow() {
        for input in [
            "",
            "0",
            "-1",
            "1.2.3MiB",
            "1mb",
            "1e3",
            "0.1B",
            "0.1KiB",
            "16EiB",
            "18446744073709551616",
        ] {
            assert!(parse_bytes(input).is_err(), "accepted {input}");
        }
    }

    /// Displayed CPU values remain exact even for quantities beyond floating-point integer precision.
    #[test]
    fn cpu_display_round_trips_exact_requests() {
        for quantity in [1, 250, 1000, 1001, 1250, u64::MAX] {
            assert_eq!(parse_cpu(&format_cpu(quantity)).unwrap(), quantity);
        }
    }

    /// Missing capacities and small allocations must remain distinguishable in human-readable output.
    #[test]
    fn byte_display_preserves_zero_small_and_missing_values() {
        assert_eq!(format_optional_bytes(None), "-");
        assert_eq!(format_optional_bytes(Some(0)), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(512 << 10), "512 KiB");
        assert_eq!(format_bytes(1536 << 20), "1.5 GiB");
        assert_eq!(format_bytes(1 << 60), "1 EiB");
    }
}
