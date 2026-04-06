FROM node:22-bookworm-slim

ARG CODEX_VERSION=0.118.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        openssh-client \
        procps \
        ripgrep \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global "@openai/codex@${CODEX_VERSION}" \
    && npm cache clean --force \
    && mkdir -p \
        /workspace \
        /workspace/.home \
        /workspace/.config \
        /workspace/.cache \
        /workspace/.local/state \
    && chown -R node:node \
        /workspace \
        /workspace/.home \
        /workspace/.config \
        /workspace/.cache \
        /workspace/.local/state

COPY entrypoint.sh /usr/local/bin/mantissa-codex-entrypoint
RUN chmod 755 /usr/local/bin/mantissa-codex-entrypoint

USER node
WORKDIR /workspace

ENV CODEX_UNSAFE_ALLOW_NO_SANDBOX=1 \
    HOME=/workspace/.home \
    XDG_CONFIG_HOME=/workspace/.config \
    XDG_CACHE_HOME=/workspace/.cache \
    XDG_STATE_HOME=/workspace/.local/state

ENTRYPOINT ["/usr/local/bin/mantissa-codex-entrypoint"]
