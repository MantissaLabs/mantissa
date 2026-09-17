use anyhow::{Result, bail};
use mantissa_client::{
    config::ClientConfig,
    services::{ServiceLogEvent, ServiceLogSource},
    tasks::TaskLogsOptions,
};
use mantissa_protocol::task::TaskLogStream;
use std::{
    collections::HashMap,
    io::{self, Write},
};

// Bound partial lines from programs that write indefinitely without a newline.
const MAX_LINE_BYTES: usize = 64 * 1024;

/// Combines the initially selected tasks' logs while keeping each line's source visible.
pub async fn logs(
    cfg: &ClientConfig,
    selector: &str,
    template: Option<&str>,
    options: &TaskLogsOptions<'_>,
) -> Result<()> {
    let mut events = mantissa_client::services::logs(cfg, selector, template, options).await?;
    let mut buffers: HashMap<String, [Vec<u8>; 3]> = HashMap::new();
    let mut failures = 0;
    let mut saw_task = false;

    while let Some(event) = events.recv().await {
        saw_task = true;
        match event {
            ServiceLogEvent::Frame {
                source,
                stream,
                data,
            } => {
                let buffers = buffers.entry(source.task_id.clone()).or_default();
                let index = stream_index(stream);
                let prefix = source_prefix(&source);

                if stream == TaskLogStream::Stderr {
                    write_lines(
                        &mut buffers[index],
                        &data,
                        &prefix,
                        &mut io::stderr().lock(),
                    )?;
                } else {
                    write_lines(
                        &mut buffers[index],
                        &data,
                        &prefix,
                        &mut io::stdout().lock(),
                    )?;
                }
            }
            ServiceLogEvent::End { source } => flush_task(&mut buffers, &source)?,
            ServiceLogEvent::Error { source, message } => {
                flush_task(&mut buffers, &source)?;
                let prefix = source_prefix(&source);
                writeln!(io::stderr().lock(), "{prefix}error: {message}")?;
                failures += 1;
            }
        }
    }

    if !saw_task {
        crate::output::emit_line("no tasks found for service with the selected filters");
    }
    if failures != 0 {
        bail!("could not read logs from {failures} task(s)");
    }

    Ok(())
}

/// Distinguishes replacement tasks even when their template and replica numbers are the same.
fn source_prefix(source: &ServiceLogSource) -> String {
    let id = crate::tasks::list::render_task_id(&source.task_id, false);
    format!("{}/{} [{id}] | ", source.template, source.replica)
}

/// Keeps stdout, stderr, and terminal fragments separate until each line is complete.
fn stream_index(stream: TaskLogStream) -> usize {
    match stream {
        TaskLogStream::Stdout => 0,
        TaskLogStream::Stderr => 1,
        TaskLogStream::Console => 2,
    }
}

/// Reassembles split frames and emits whole prefixed lines without unbounded buffering.
fn write_lines(
    buffer: &mut Vec<u8>,
    data: &[u8],
    prefix: &str,
    out: &mut impl Write,
) -> io::Result<()> {
    for mut part in data.split_inclusive(|byte| *byte == b'\n') {
        while !part.is_empty() {
            let count = part.len().min(MAX_LINE_BYTES - buffer.len());
            buffer.extend_from_slice(&part[..count]);
            part = &part[count..];

            if buffer.ends_with(b"\n") || buffer.len() == MAX_LINE_BYTES {
                write_buffer(buffer, prefix, out)?;
            }
        }
    }

    Ok(())
}

/// Finishes an unterminated line before another task's output can appear on that line.
fn write_buffer(buffer: &mut Vec<u8>, prefix: &str, out: &mut impl Write) -> io::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    out.write_all(prefix.as_bytes())?;
    out.write_all(buffer)?;
    if !buffer.ends_with(b"\n") {
        out.write_all(b"\n")?;
    }
    out.flush()?;
    buffer.clear();

    Ok(())
}

/// Flushes final fragments on both successful completion and task-specific failure.
fn flush_task(
    buffers: &mut HashMap<String, [Vec<u8>; 3]>,
    source: &ServiceLogSource,
) -> io::Result<()> {
    let Some(mut pending) = buffers.remove(&source.task_id) else {
        return Ok(());
    };
    let prefix = source_prefix(source);

    write_buffer(&mut pending[0], &prefix, &mut io::stdout().lock())?;
    write_buffer(&mut pending[1], &prefix, &mut io::stderr().lock())?;
    write_buffer(&mut pending[2], &prefix, &mut io::stdout().lock())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frame boundaries must not add prefixes inside lines or corrupt arbitrary runtime bytes.
    #[test]
    fn fragmented_lines_preserve_bytes_and_prefixes() {
        let mut buffer = Vec::new();
        let mut output = Vec::new();

        write_lines(&mut buffer, b"first \xc3", "task | ", &mut output).unwrap();
        assert!(output.is_empty());
        write_lines(
            &mut buffer,
            b"\xa9\nsecond\nlast\xff",
            "task | ",
            &mut output,
        )
        .unwrap();
        write_buffer(&mut buffer, "task | ", &mut output).unwrap();

        assert_eq!(
            output,
            b"task | first \xc3\xa9\ntask | second\ntask | last\xff\n"
        );
        assert!(buffer.is_empty());
    }

    /// A task without newlines must still produce output and retain only a bounded fragment.
    #[test]
    fn unterminated_output_does_not_grow_without_limit() {
        let mut buffer = Vec::new();
        let mut output = Vec::new();
        let data = vec![b'x'; MAX_LINE_BYTES * 2 + 7];

        write_lines(&mut buffer, &data, "task | ", &mut output).unwrap();

        assert_eq!(buffer.len(), 7);
        assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 2);
        assert_eq!(
            output.iter().filter(|byte| **byte == b'x').count(),
            MAX_LINE_BYTES * 2
        );
    }
}
