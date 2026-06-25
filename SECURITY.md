# Security Policy

## Reporting a Vulnerability

Do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.

Email reports to `security@mantissa.io`. Do not include exploit details,
secrets, crash payloads, logs, or proof-of-concept code in public issues,
discussions, pull requests, or social channels.

If email is unavailable, use GitHub private vulnerability reporting for this
repository. If both channels are unavailable, open a public issue that asks for
a private security contact without including sensitive details.

Please include:

- affected versions, commits, or deployment configuration;
- a concise description of the impact;
- reproduction steps or a minimal proof of concept;
- whether the issue is actively exploited or publicly disclosed;
- any temporary mitigations you already validated.

## Scope

Mantissa is experimental software. Security reports are still welcome for the
control plane, node authentication, join-token handling, secret storage and
replication, REST authentication, container runtime integration, eBPF dataplane,
release artifacts, and CI/CD workflows.

Out of scope:

- denial-of-service reports that only require unbounded public traffic against
  an intentionally exposed test node.
- vulnerabilities in third-party services, runners, registries, or hosted
  infrastructure unless Mantissa configuration makes them exploitable.
- social engineering, spam, or physical attacks.

## Disclosure

Give maintainers a reasonable window to investigate and publish a fix before
public disclosure. If you believe users are under active risk, state that clearly
in the report so triage can prioritize mitigation and advisory publication.
