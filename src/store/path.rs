use std::{fs, io, path::PathBuf};

pub fn default_db_path() -> io::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME not set"))?;

    let dir = home.join(".mantissa");
    fs::create_dir_all(&dir)?;

    Ok(dir.join("state.redb"))
}
