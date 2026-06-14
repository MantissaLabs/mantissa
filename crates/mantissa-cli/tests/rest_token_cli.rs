use mantissa::rest::is_valid_rest_token_format;
use mantissa::server::headless::HeadlessNode;
use mantissa_client::config::ClientConfig;
use std::path::Path;
use tokio::{process::Command, task::LocalSet};

/// Runs one `mantissa rest token` subcommand against the provided state directory.
async fn run_rest_token_command(binary: &str, state_dir: &Path, subcommand: &str) -> String {
    let output = Command::new(binary)
        .args(["rest", "token", subcommand])
        .env("MANTISSA_STATE_DIR", state_dir)
        .output()
        .await
        .expect("run mantissa rest token command");

    assert!(
        output.status.success(),
        "command failed: status={:?}; stdout={}; stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout)
        .expect("token output is utf8")
        .trim()
        .to_string()
}

#[tokio::test(flavor = "current_thread")]
async fn rest_token_cli_show_and_rotate_use_running_local_daemon() {
    mantissa::logger::init_for_tests();
    LocalSet::new()
        .run_until(Box::pin(async {
            let binary = env!("CARGO_BIN_EXE_mantissa");
            let state_dir = tempfile::tempdir().expect("create CLI REST state dir");
            let socket_path = state_dir.path().join("mantissa.sock");
            let node = HeadlessNode::new_inproc()
                .await
                .expect("start headless daemon");
            node.start_local_admin_socket_at(socket_path.clone())
                .await
                .expect("start local admin socket");
            let client_config = ClientConfig {
                socket: Some(socket_path),
                ..ClientConfig::default()
            };

            let first = run_rest_token_command(binary, state_dir.path(), "show").await;
            assert!(is_valid_rest_token_format(&first));
            assert!(
                mantissa_client::rest::validate_token(&client_config, &first)
                    .await
                    .expect("validate first REST token")
            );

            let rotated = run_rest_token_command(binary, state_dir.path(), "rotate").await;
            assert!(is_valid_rest_token_format(&rotated));
            assert_ne!(rotated, first);
            assert!(
                !mantissa_client::rest::validate_token(&client_config, &first)
                    .await
                    .expect("old REST token should be invalid")
            );
            assert!(
                mantissa_client::rest::validate_token(&client_config, &rotated)
                    .await
                    .expect("new REST token should be valid")
            );

            drop(node);
        }))
        .await;
}
