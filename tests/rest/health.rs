use axum::http::{Method, StatusCode};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::common;
use crate::harness::RestTestHarness;

local_test!(rest_liveness_probe_is_public, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/healthz", false, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["status"], "ok");
});

local_test!(rest_daemon_health_requires_bearer_token, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/health", false, None)
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(value["code"], "unauthorized");
});

local_test!(rest_daemon_health_reports_local_session_reachable, {
    let harness = RestTestHarness::new().await;

    let (status, value) = harness
        .json_request(Method::GET, "/v1/health", true, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(value["daemon"]["reachable"], true);
});

local_test!(rest_listener_serves_http_requests_over_tcp, {
    let harness = RestTestHarness::new().await;
    let listener = harness.start_listener().await;
    let mut stream = TcpStream::connect(listener.local_addr())
        .await
        .expect("connect REST listener");
    let request = format!(
        "GET /healthz HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        listener.local_addr()
    );

    stream
        .write_all(request.as_bytes())
        .await
        .expect("write REST request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read REST response");

    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    assert!(response.contains(r#""status":"ok""#), "{response}");
    listener.shutdown().await;
});

local_test!(rest_token_rotation_invalidates_old_token, {
    let harness = RestTestHarness::new().await;
    let old_token = mantissa_client::rest::show_token(&harness.client_config)
        .await
        .expect("show REST token");
    assert_eq!(old_token, harness.rest_token);

    let new_token = mantissa_client::rest::rotate_token(&harness.client_config)
        .await
        .expect("rotate REST token");
    assert_ne!(new_token, old_token);

    let old_response = harness
        .request_with_token(Method::GET, "/v1/health", Some(&old_token), None)
        .await;
    assert_eq!(old_response.status(), StatusCode::UNAUTHORIZED);

    let new_response = harness
        .request_with_token(Method::GET, "/v1/health", Some(&new_token), None)
        .await;
    assert_eq!(new_response.status(), StatusCode::OK);
});
