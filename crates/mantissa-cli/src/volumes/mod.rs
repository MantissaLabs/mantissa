mod create;
mod delete;
mod import;
mod inspect;
mod list;
mod status;

pub use create::{VolumeCreateRequest, create};
pub use delete::delete;
pub use import::import;
pub use inspect::inspect;
pub use list::list;
pub use status::status;

/// Formats optional byte counts for terminal volume output.
pub(super) fn format_bytes(bytes: Option<u64>) -> String {
    let Some(bytes) = bytes else {
        return "-".to_string();
    };
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit_idx = 0usize;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{value:.1} {}", UNITS[unit_idx])
    }
}
