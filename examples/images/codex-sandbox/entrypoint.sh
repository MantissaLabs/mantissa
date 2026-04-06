#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

workdir="${MANTISSA_AGENT_WORKDIR:-/workspace}"
model="${CODEX_MODEL:-gpt-5.4-nano}"
mkdir -p "$workdir" "$HOME" "$XDG_CONFIG_HOME" "$XDG_CACHE_HOME" "$XDG_STATE_HOME"

if [ -z "${MANTISSA_AGENT_INPUT:-}" ]; then
    exec codex --help
fi

exec codex exec \
    -m "$model" \
    --skip-git-repo-check \
    --dangerously-bypass-approvals-and-sandbox \
    -C "$workdir" \
    "$MANTISSA_AGENT_INPUT" </dev/null
