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
    && mkdir -p /workspace /run/secrets \
    && chown -R node:node /workspace

COPY entrypoint.sh /usr/local/bin/mantissa-codex-entrypoint
RUN chmod 755 /usr/local/bin/mantissa-codex-entrypoint

USER node
WORKDIR /workspace

ENV CODEX_UNSAFE_ALLOW_NO_SANDBOX=1 \
    CODEX_MODEL=gpt-5.4-nano \
    HOME=/var/tmp/codex-home \
    XDG_CONFIG_HOME=/var/tmp/codex-config \
    XDG_CACHE_HOME=/var/tmp/codex-cache \
    XDG_STATE_HOME=/var/tmp/codex-state

ENTRYPOINT ["/usr/local/bin/mantissa-codex-entrypoint"]
