use anyhow::Result;
use crossterm::style::Stylize;
use mantissa_protocol::{volumes::filesystem_ownership, workload};
use std::{
    fmt::{Display, Write as _},
    io::IsTerminal,
};
use textwrap::{Options, WordSplitter, WrapAlgorithm};
use uuid::Uuid;

/// Keeps field alignment and wrapping consistent across task and service inspection sections.
pub(crate) struct InspectOutput {
    pub(crate) text: String,
    pub(crate) details: bool,
    width: usize,
    color: bool,
}

impl InspectOutput {
    /// Uses terminal width interactively and stable plain text when output is redirected.
    pub(crate) fn new(details: bool) -> Self {
        let terminal = std::io::stdout().is_terminal();
        let width = if terminal {
            crossterm::terminal::size()
                .ok()
                .map(|(width, _)| usize::from(width))
        } else {
            None
        };

        Self {
            text: String::new(),
            details,
            width: width.unwrap_or(100).clamp(40, 120),
            color: terminal
                && std::env::var_os("NO_COLOR").is_none()
                && std::env::var("TERM").as_deref() != Ok("dumb"),
        }
    }

    /// Separates major sections without adding borders that compete with the content.
    pub(crate) fn section(&mut self, title: impl Display) -> Result<()> {
        if !self.text.is_empty() {
            self.text.push('\n');
        }

        let title = title.to_string();
        if self.color {
            writeln!(self.text, "{}", title.bold())?;
        } else {
            writeln!(self.text, "{title}")?;
        }

        Ok(())
    }

    /// Aligns continuation lines while keeping identifiers and long words intact for copying.
    pub(crate) fn field(&mut self, label: &str, value: impl Display) -> Result<()> {
        let prefix = format!("  {label:<16}  ");
        let options = Options::new(self.width)
            .initial_indent(&prefix)
            .subsequent_indent("                    ")
            .word_splitter(WordSplitter::NoHyphenation)
            .wrap_algorithm(WrapAlgorithm::FirstFit)
            .break_words(false);

        writeln!(self.text, "{}", textwrap::fill(&value.to_string(), options))?;
        Ok(())
    }

    /// Omits unused optional lists normally and shows their absence inline in the expanded view.
    pub(crate) fn list(&mut self, label: &str, values: &[String]) -> Result<()> {
        if values.is_empty() {
            if self.details {
                self.field(label, "none")?;
            }
        } else {
            for (index, value) in values.iter().enumerate() {
                self.field(if index == 0 { label } else { "" }, value)?;
            }
        }

        Ok(())
    }
}

/// Distinguishes a fixed secret version from a reference that follows the latest version.
fn secret_reference(secret: workload::secret_ref::Reader<'_>) -> Result<String> {
    let name = secret.get_name()?.to_str()?;
    let version = secret.get_version_id()?;
    if version.is_empty() {
        Ok(format!("secret {name} (latest)"))
    } else {
        Ok(format!(
            "secret {name} (version {})",
            Uuid::from_slice(version)?
        ))
    }
}

/// Keeps empty arguments, quoting, and control characters visible when command lines wrap.
pub(crate) fn argument(value: &str) -> String {
    if value.is_empty()
        || value
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control() || matches!(ch, '\'' | '"' | '\\'))
    {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

/// Uses the largest exact unit so shorter timing labels do not lose precision.
pub(crate) fn duration(milliseconds: impl Into<u128>) -> String {
    let milliseconds = milliseconds.into();
    if milliseconds == 0 {
        return "0s".to_string();
    }

    for (unit, size) in [("h", 3_600_000), ("m", 60_000), ("s", 1000)] {
        if milliseconds.is_multiple_of(size) {
            return format!("{}{unit}", milliseconds / size);
        }
    }

    format!("{milliseconds}ms")
}

/// Preserves literal values and secret references without fetching decrypted secret contents.
pub(crate) fn render_mounts_and_environment(
    out: &mut InspectOutput,
    volumes: capnp::struct_list::Reader<workload::volume_mount::Owned>,
    env: capnp::struct_list::Reader<workload::environment_var::Owned>,
    files: capnp::struct_list::Reader<workload::secret_file::Owned>,
) -> Result<()> {
    let volumes = volumes
        .iter()
        .map(|mount| {
            let access = if mount.get_read_only() {
                "read-only"
            } else {
                "read-write"
            };

            Ok(format!(
                "{} ({}) -> {:?}, {access}",
                mount.get_volume_name()?.to_str()?,
                Uuid::from_slice(mount.get_volume_id()?)?,
                mount.get_target()?.to_str()?
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Volumes", &volumes)?;

    let env = env
        .iter()
        .map(|variable| {
            let name = variable.get_name()?.to_str()?;
            if variable.has_secret() {
                Ok(format!(
                    "{name} <- {}",
                    secret_reference(variable.get_secret()?)?
                ))
            } else {
                Ok(format!("{name}={:?}", variable.get_value()?.to_str()?))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    out.list("Environment", &env)?;

    if files.is_empty() && out.details {
        out.field("Secret files", "none")?;
    }
    for (index, file) in files.iter().enumerate() {
        out.field(
            if index == 0 { "Secret files" } else { "" },
            format!(
                "{:?} <- {}",
                file.get_path()?.to_str()?,
                secret_reference(file.get_secret()?)?
            ),
        )?;

        let mode = if file.get_mode() == 0 {
            "policy default".to_string()
        } else {
            format!("{:04o}", file.get_mode())
        };
        let ownership = match file.get_ownership()?.which()? {
            filesystem_ownership::Which::Daemon(()) => "daemon".to_string(),
            filesystem_ownership::Which::User(user) => {
                let user = user?;
                format!("uid {}, gid {}", user.get_uid(), user.get_gid())
            }
            filesystem_ownership::Which::FsGroup(group) => {
                format!("filesystem group {}", group?.get_gid())
            }
        };

        out.field("", format!("mode {mode}, ownership {ownership}"))?;

        let path_env = file.get_path_env_name()?.to_str()?;
        if !path_env.is_empty() {
            out.field("", format!("path environment variable {path_env}"))?;
        }
    }

    Ok(())
}
