use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use super::*;
use crate::runtime::registry_auth::DOCKER_HUB_ADDRESS;

/// Writes an isolated Docker configuration so tests never change the daemon's environment.
async fn write_config(path: &Path, config: Value) {
    tokio::fs::write(path, serde_json::to_vec(&config).unwrap())
        .await
        .unwrap();
}

/// Covers image naming rules that otherwise send private credentials to the wrong registry.
#[test]
fn image_registry_handles_hub_names_hosts_ports_and_digests() {
    for (image, expected) in [
        ("busybox:1.36", DOCKER_HUB_ADDRESS),
        ("team/api:latest", DOCKER_HUB_ADDRESS),
        ("docker.io/team/api:latest", DOCKER_HUB_ADDRESS),
        ("index.docker.io/team/api:latest", DOCKER_HUB_ADDRESS),
        ("registry-1.docker.io/team/api:latest", DOCKER_HUB_ADDRESS),
        ("ghcr.io/team/api:latest", "ghcr.io"),
        ("localhost/api:latest", "localhost"),
        ("registry:5000/api:latest", "registry:5000"),
        ("[::1]:5000/api:latest", "[::1]:5000"),
        ("REGISTRY/team/api:latest", "registry"),
        ("ghcr.io/team/api@sha256:abcd", "ghcr.io"),
    ] {
        assert_eq!(image_registry(image), expected, "image: {image}");
    }
}

/// Docker login encodes the username and full password together, including password colons.
#[tokio::test]
async fn resolve_decodes_login_credentials_only_for_matching_registry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    write_config(
        &path,
        json!({
            "auths": {
                "registry.example:5443": {
                    "auth": STANDARD.encode("reader:password:with:colons"),
                },
                "unrelated.example": {
                    "auth": "malformed-unrelated-credential",
                },
            },
        }),
    )
    .await;

    let credentials = resolve(Some(&path), "registry.example:5443/team/api:1")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(credentials.username.as_deref(), Some("reader"));
    assert_eq!(
        credentials.password.as_deref(),
        Some("password:with:colons")
    );
    assert_eq!(
        Some(credentials.registry.as_str()),
        Some("registry.example:5443")
    );
    assert!(
        resolve(Some(&path), "another.example/api:1")
            .await
            .unwrap()
            .is_none()
    );
}

/// Docker Hub uses a full index URL while other registries may store URL-shaped keys.
#[tokio::test]
async fn resolve_matches_hub_and_registry_url_keys() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    write_config(
        &path,
        json!({
            "auths": {
                DOCKER_HUB_ADDRESS: {
                    "auth": STANDARD.encode("hub-reader:hub-password"),
                },
                "https://registry.example:5443/v1/": {
                    "identitytoken": "registry-token",
                },
            },
        }),
    )
    .await;

    let hub = resolve(Some(&path), "team/api:1").await.unwrap().unwrap();
    let private = resolve(Some(&path), "registry.example:5443/api:1")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(hub.username.as_deref(), Some("hub-reader"));
    assert_eq!(Some(hub.registry.as_str()), Some(DOCKER_HUB_ADDRESS));
    assert_eq!(private.identity_token.as_deref(), Some("registry-token"));
    assert_eq!(
        Some(private.registry.as_str()),
        Some("registry.example:5443")
    );
}

/// Anonymous public pulls keep working without configuration or with empty auth entries.
#[tokio::test]
async fn resolve_allows_missing_and_empty_configuration() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");

    assert!(resolve(None, "busybox:1.36").await.unwrap().is_none());
    assert!(
        resolve(Some(&path), "busybox:1.36")
            .await
            .unwrap()
            .is_none()
    );

    tokio::fs::write(&path, []).await.unwrap();
    assert!(
        resolve(Some(&path), "busybox:1.36")
            .await
            .unwrap()
            .is_none()
    );

    write_config(&path, json!({ "auths": { DOCKER_HUB_ADDRESS: {} } })).await;
    assert!(
        resolve(Some(&path), "busybox:1.36")
            .await
            .unwrap()
            .is_none()
    );
}

/// A subsequent pull must observe login changes instead of retaining old passwords in memory.
#[tokio::test]
async fn resolve_reloads_rotated_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    for password in ["first-password", "rotated-password"] {
        write_config(
            &path,
            json!({
                "auths": {
                    "registry.example": {
                        "auth": STANDARD.encode(format!("reader:{password}")),
                    },
                },
            }),
        )
        .await;

        let credentials = resolve(Some(&path), "registry.example/api:1")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(credentials.password.as_deref(), Some(password));
    }
}

/// Configuration and authentication parsing errors must never quote stored credential values.
#[tokio::test]
async fn resolve_rejects_malformed_credentials_without_disclosing_them() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    for config in [
        json!({
            "auths": {
                "registry.example": {
                    "auth": "SENSITIVE-TOKEN",
                },
            },
        }),
        json!({
            "auths": {
                "registry.example": {
                    "username": ["SENSITIVE-TOKEN"],
                },
            },
        }),
        json!({
            "auths": {
                "registry.example": {
                    "auth": STANDARD.encode("SENSITIVE-TOKEN"),
                },
            },
        }),
    ] {
        write_config(&path, config).await;

        let error = resolve(Some(&path), "registry.example/api:1")
            .await
            .unwrap_err();

        assert!(!error.to_string().contains("SENSITIVE-TOKEN"));
    }
}

/// Helper selection follows the registry override before consulting the default store.
#[test]
fn registry_helper_overrides_default_store() {
    let config: RegistryAuthConfig = serde_json::from_value(json!({
        "credsStore": "default-store",
        "credHelpers": {
            "registry.example": "registry-store",
            DOCKER_HUB_ADDRESS: "hub-store",
        },
    }))
    .unwrap();

    assert_eq!(config.helper("registry.example"), "registry-store");
    assert_eq!(config.helper(DOCKER_HUB_ADDRESS), "hub-store");
    assert_eq!(config.helper("other.example"), "default-store");
}

/// A configured but unavailable helper must fail rather than using stale file credentials.
#[tokio::test]
async fn resolve_does_not_fall_back_when_configured_helper_fails() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("config.json");
    write_config(
        &path,
        json!({
            "auths": {
                "registry.example": {
                    "auth": STANDARD.encode("reader:stale-password"),
                },
            },
            "credsStore": "mantissa-test-unavailable-helper",
        }),
    )
    .await;

    let error = resolve(Some(&path), "registry.example/api:1")
        .await
        .unwrap_err();

    assert!(error.to_string().contains("cannot start"));
    assert!(!error.to_string().contains("stale-password"));
}

/// Runs isolated helper fixtures without modifying the process-global PATH used by other tests.
#[cfg(unix)]
fn helper_command(script: &str) -> Command {
    let mut command = Command::new("sh");
    command.args(["-c", script, "docker-credential-test"]);
    command
}

/// Helpers receive the registry through stdin and the `get` action as an argument.
#[cfg(unix)]
#[tokio::test]
async fn helper_protocol_returns_password_credentials() {
    let command = helper_command(
        r#"test "$1" = get && test "$(cat)" = registry.example:5443 || exit 2
printf '%s' '{"Username":"reader","Secret":"helper-password"}'"#,
    );

    let credentials = credentials_from_helper(command, "registry.example:5443", HELPER_TIMEOUT)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(credentials.username.as_deref(), Some("reader"));
    assert_eq!(credentials.password.as_deref(), Some("helper-password"));
    assert_eq!(
        Some(credentials.registry.as_str()),
        Some("registry.example:5443")
    );
}

/// Docker represents identity tokens with a special username in helper responses.
#[cfg(unix)]
#[tokio::test]
async fn helper_protocol_preserves_identity_tokens() {
    let command = helper_command(
        r#"cat >/dev/null; printf '%s' '{"Username":"<token>","Secret":"identity-token"}'"#,
    );

    let credentials = credentials_from_helper(command, DOCKER_HUB_ADDRESS, HELPER_TIMEOUT)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        credentials.identity_token.as_deref(),
        Some("identity-token")
    );
    assert!(credentials.username.is_none());
    assert!(credentials.password.is_none());
}

/// A credential store may have no login for a public registry without being broken.
#[cfg(unix)]
#[tokio::test]
async fn helper_not_found_allows_anonymous_pull() {
    let command = helper_command(
        "cat >/dev/null; printf '%s' 'credentials not found in native keychain'; exit 1",
    );

    let credentials = credentials_from_helper(command, DOCKER_HUB_ADDRESS, HELPER_TIMEOUT)
        .await
        .unwrap();

    assert!(credentials.is_none());
}

/// Helpers may print credentials on failure, so neither output stream belongs in an error.
#[cfg(unix)]
#[tokio::test]
async fn helper_failures_do_not_disclose_output() {
    let command = helper_command(
        "cat >/dev/null; printf '%s' 'SENSITIVE-TOKEN'; printf '%s' 'SENSITIVE-TOKEN' >&2; exit 1",
    );

    let error = credentials_from_helper(command, DOCKER_HUB_ADDRESS, HELPER_TIMEOUT)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("credential lookup failed"));
    assert!(!error.to_string().contains("SENSITIVE-TOKEN"));
}

/// A stalled helper must not occupy one of the node's limited image-pull slots indefinitely.
#[cfg(unix)]
#[tokio::test]
async fn helper_timeout_stops_waiting_for_credentials() {
    let command = helper_command("cat >/dev/null; exec sleep 60");

    let error = credentials_from_helper(command, DOCKER_HUB_ADDRESS, Duration::from_millis(100))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("timed out"));
}

/// Limit helper output so a faulty program cannot allocate unbounded credential buffers.
#[cfg(unix)]
#[tokio::test]
async fn helper_rejects_oversized_output() {
    let command = helper_command("cat >/dev/null; head -c 65537 /dev/zero");

    let error = credentials_from_helper(command, DOCKER_HUB_ADDRESS, HELPER_TIMEOUT)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("exceeds 64 KiB"));
}
