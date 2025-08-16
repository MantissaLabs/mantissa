use std::path::PathBuf;

pub fn default_db_path() -> PathBuf {
    if let Ok(dir) = std::env::var("MANTISSA_DATA_DIR") {
        return PathBuf::from(dir).join("state.redb");
    }
    #[cfg(target_os = "macos")]
    if let Some(home) = dirs::home_dir() {
        return home.join("Library/Application Support/Mantissa/state.redb");
    }
    if let Ok(xdg) = std::env::var("XDG_STATE_HOME") {
        return PathBuf::from(xdg).join("mantissa/state.redb");
    }
    if let Some(home) = dirs::home_dir() {
        return home.join(".local/state/mantissa/state.redb");
    }
    // last resort
    std::env::temp_dir().join("mantissa-state.redb")
}
