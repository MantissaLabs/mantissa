use net::noise::{client_handshake, server_handshake};
use net::noise::{read_framed_len, write_framed};
use std::io::ErrorKind;
use tokio::io;
use tokio::time::{Duration, timeout};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod common;
use common::fixed_noise_keys;

#[tokio::test(flavor = "current_thread")]
async fn noise_xx_handshake_and_echo_both_directions() {
    // Deterministic keys, different for server/client
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);

    // Listener on ephemeral port
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(err) if err.kind() == ErrorKind::PermissionDenied => {
            eprintln!("skipping noise handshake test due to permission error: {err}");
            return;
        }
        Err(err) => panic!("failed to bind noise listener: {err}"),
    };
    let addr = listener.local_addr().unwrap();

    // Accept & server-handshake
    let server_task = async move {
        let (sock, _) = listener.accept().await.unwrap();
        server_handshake(sock, &server_keys).await.unwrap()
    };

    // Connect & client-handshake
    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        client_handshake(sock, &client_keys).await.unwrap()
    };

    // Run both, collect the application ends
    let (mut server_app, mut client_app) = tokio::join!(server_task, client_task);

    // Client -> Server
    client_app.write_all(b"ping").await.unwrap();
    client_app.flush().await.unwrap();
    let mut buf = [0u8; 4];
    timeout(Duration::from_secs(2), server_app.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"ping");

    // Server -> Client
    server_app.write_all(b"pong").await.unwrap();
    server_app.flush().await.unwrap();
    let mut buf2 = [0u8; 4];
    timeout(Duration::from_secs(2), client_app.read_exact(&mut buf2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf2, b"pong");
}

#[tokio::test(flavor = "current_thread")]
async fn write_framed_rejects_too_large() {
    // Make a payload > u16::MAX so write_framed must reject it.
    let big = vec![0u8; (u16::MAX as usize) + 1];

    // Use a sink writer, nothing will be written due to early error.
    let mut sink = io::sink();

    let err = write_framed(&mut sink, &big)
        .await
        .expect_err("should be too large");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test(flavor = "current_thread")]
async fn write_then_read_framed_roundtrip_small() {
    // Duplex in-memory pipe
    let (mut a, mut b) = tokio::io::duplex(1024);

    // Writer task
    let writer = async {
        let payload = b"hello framed";
        write_framed(&mut b, payload).await.unwrap();
        b.shutdown().await.unwrap();
    };

    // Reader task
    let reader = async {
        let mut buf = Vec::new();
        let n = read_framed_len(&mut a, &mut buf).await.unwrap();
        assert_eq!(n, b"hello framed".len());
        assert_eq!(&buf[..n], b"hello framed");
    };

    tokio::join!(writer, reader);
}
