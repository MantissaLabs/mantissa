use std::io::{self, Write};

/// Emit CLI output that may span multiple lines, ensuring the final payload terminates with a single
/// newline so downstream shells and pipelines observe canonical POSIX behavior.
pub fn emit_block(text: impl Into<String>) {
    let mut text = text.into();
    if !text.ends_with('\n') {
        text.push('\n');
    }
    print!("{text}");
    let _ = io::stdout().flush();
}

/// Emit a single logical line of CLI output, delegating to `emit_block` so the user always receives
/// a newline-terminated line without duplicated line endings.
pub fn emit_line(line: impl Into<String>) {
    emit_block(line.into());
}
