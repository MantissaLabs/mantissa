# mantissa-ui

Terminal UI helpers for Mantissa.

This crate contains reusable terminal interaction components used by the
Mantissa CLI. It currently focuses on interactive cluster split workflows built
with `ratatui` and `crossterm`.

## When To Use It

Use this crate from Mantissa command-line code when an operation needs a
stateful terminal interface rather than line-oriented output. Non-interactive
automation should use `mantissa-client` directly.

## Modules

- `split_interactive`: interactive cluster split planning and selection UI.

## Example

The crate is intended to be called from CLI command handlers after they have
loaded cluster state through `mantissa-client`:

```rust,no_run
use mantissa_ui::split_interactive;

fn wire_split_ui() {
    // Build the split view model in the CLI, then pass it to the interactive UI.
    // See the CLI cluster split command for the concrete integration.
    let _runner = split_interactive::run_split_planner;
}
```

## Consumer Guidance

This crate is not a full UI framework. It keeps Mantissa-specific terminal
components out of the CLI dispatcher so they can be tested and evolved in
isolation.
