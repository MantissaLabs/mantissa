# mantissa-rest

Typed REST facade for Mantissa's Cap'n Proto control plane.

This crate implements the HTTP API served by `mantissa init --rest`. It is not
an independent daemon. The REST listener runs inside the main Mantissa process
and talks to the local node through `mantissa-client`, which in turn uses the
same Cap'n Proto admin protocol as the CLI.

## Purpose

`mantissa-rest` exists for integrations that are awkward to build directly on
Cap'n Proto, especially local tools, JavaScript or TypeScript programs, UI
prototypes, and automation that expects JSON over HTTP.

The Cap'n Proto protocol remains the native control-plane API. REST is a typed
facade over that protocol, not a second source of truth. Route handlers should
stay thin: parse JSON, validate the REST shape, call `mantissa-client`, and map
the result into stable JSON response types.

## API Mapping Model

The REST API uses explicit Rust request and response types under `src/types`.
Those types are intentionally close to Mantissa's public client concepts, but
they are not generated from the Cap'n Proto schemas. This keeps the HTTP surface
small, documented, and shaped for JSON clients while still reusing the same
domain operations as the CLI.

The request flow is:

1. Axum extracts JSON, path, query, and authorization data.
2. The route handler converts the REST type into a `mantissa-client` call.
3. `mantissa-client` talks to the local daemon over Cap'n Proto.
4. The route handler maps the client result back into a REST response type.

Manifest endpoints reuse the same Rust manifest types that RON parsing uses.
There is no JSON-to-RON translation step: RON files and REST JSON bodies both
deserialize into the same typed structures.

## Security Model

The REST API is an admin API. Protected `/v1/*` routes require the REST bearer
token stored by the daemon. The token is distinct from the join token.

The default listener binds to loopback. Binding to a non-loopback address
requires TLS and client certificate authentication. Some endpoints, such as the
secrets API, can return plaintext secret material to an authenticated caller, so
the REST token and any client certificates must be treated as administrative
credentials.

## OpenAPI

The OpenAPI v3 specification is generated from the Axum route declarations and
the REST request and response types. The generated file is checked in at:

```text
openapi/mantissa-rest.openapi.json
```

Regenerate it after changing REST routes, request types, response types, or
endpoint documentation:

```sh
cargo run -p mantissa-rest --bin generate-openapi
```

The OpenAPI tests verify that the checked-in file is current and that every
registered route has a documented path and method:

```sh
cargo test -p mantissa-rest --test openapi
```

## Development Notes

Keep route handlers focused on transport concerns. Shared behavior should live
in `mantissa-client` or the daemon domain modules, not in REST-only logic.

When adding an endpoint:

1. Add or extend the REST type under `src/types`.
2. Add the route handler under `src/routes`.
3. Register the handler in `src/server.rs`.
4. Document the handler with `#[utoipa::path]`.
5. Regenerate `openapi/mantissa-rest.openapi.json`.
6. Add integration coverage under `tests/rest` when the endpoint exercises real
   daemon behavior.

## Stability

This crate tracks Mantissa's control-plane protocol and client API. It is meant
to be versioned with the rest of the repository, not as an independently stable
public API.
