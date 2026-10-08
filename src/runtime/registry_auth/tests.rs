use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;

use super::RegistryAuth;

/// Portable secrets select the image's registry without disclosing another registry's login.
#[test]
fn registry_auth_selects_matching_credentials() {
    let config = serde_json::to_vec(&json!({
        "auths": {
            "registry.example:5443": {
                "auth": STANDARD.encode("reader:password:with:colons"),
            },
            "other.example": {
                "auth": STANDARD.encode("other:other-password"),
            },
        },
    }))
    .unwrap();

    let auth = RegistryAuth::from_config(&config, "registry.example:5443/team/app:1").unwrap();

    assert_eq!(auth.registry, "registry.example:5443");
    assert_eq!(auth.username.as_deref(), Some("reader"));
    assert_eq!(auth.password.as_deref(), Some("password:with:colons"));
    assert!(auth.validate_image("other.example/team/app:1").is_err());
    assert!(RegistryAuth::from_config(&config, "missing.example/app:1").is_err());
}

/// Secret payloads cannot depend on helper programs installed on the submitting user's machine.
#[test]
fn registry_auth_rejects_helper_settings_in_secrets() {
    for config in [
        json!({"credsStore": "secretservice"}),
        json!({"credHelpers": {"registry.example": "ecr-login"}}),
    ] {
        let config = serde_json::to_vec(&config).unwrap();

        let error = RegistryAuth::from_config(&config, "registry.example/app:1").unwrap_err();

        assert!(error.to_string().contains("inline auths"));
    }
}

/// Errors and Debug output must not disclose parsed tokens or malformed credential values.
#[test]
fn registry_auth_redacts_credentials() {
    let config = serde_json::to_vec(&json!({
        "auths": {
            "registry.example": {
                "identitytoken": "SENSITIVE-TOKEN",
            },
        },
    }))
    .unwrap();
    let auth = RegistryAuth::from_config(&config, "registry.example/app:1").unwrap();

    assert_eq!(auth.identity_token.as_deref(), Some("SENSITIVE-TOKEN"));
    assert!(!format!("{auth:?}").contains("SENSITIVE-TOKEN"));

    let malformed = serde_json::to_vec(&json!({
        "auths": {
            "registry.example": {
                "auth": "SENSITIVE-TOKEN",
            },
        },
    }))
    .unwrap();
    let error = RegistryAuth::from_config(&malformed, "registry.example/app:1").unwrap_err();

    assert!(!error.to_string().contains("SENSITIVE-TOKEN"));
}
