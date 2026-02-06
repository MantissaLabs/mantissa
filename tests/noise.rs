#![allow(clippy::unwrap_used)]

use async_trait::async_trait;
use net::noise::{
    NoisePeerVerifier, client_handshake_join, client_handshake_peer, read_framed_len,
    server_handshake_join, server_handshake_peer_with_first_frame, write_framed,
};
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::io;
use tokio::time::{Duration, timeout};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod common;
use common::fixed_noise_keys;

struct AllowPeer([u8; 32]);

#[async_trait(?Send)]
impl NoisePeerVerifier for AllowPeer {
    async fn is_allowed(&self, remote_static: &[u8]) -> std::io::Result<bool> {
        Ok(remote_static == self.0)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn noise_xx_handshake_and_echo_both_directions() {
    // Deterministic keys, different for server/client
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);
    let psk = net::noise::derive_psk_from_token("MNTISA-1-test-token").expect("derive psk");

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
        server_handshake_join(sock, &server_keys, &psk)
            .await
            .unwrap()
    };

    // Connect & client-handshake
    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        client_handshake_join(sock, &client_keys, &psk)
            .await
            .unwrap()
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
async fn noise_xx_psk_rejects_wrong_token() {
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);
    let server_psk =
        net::noise::derive_psk_from_token("MNTISA-1-server-token").expect("derive server psk");
    let client_psk =
        net::noise::derive_psk_from_token("MNTISA-1-client-token").expect("derive client psk");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().unwrap();

    let server_task = async move {
        let (sock, _) = listener.accept().await.unwrap();
        server_handshake_join(sock, &server_keys, &server_psk).await
    };

    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        client_handshake_join(sock, &client_keys, &client_psk).await
    };

    let (server_res, client_res) = timeout(Duration::from_secs(2), async {
        tokio::join!(server_task, client_task)
    })
    .await
    .expect("handshake timed out");

    if server_res.is_err() || client_res.is_err() {
        return;
    }

    let (mut server_app, mut client_app) = (server_res.unwrap(), client_res.unwrap());

    client_app.write_all(b"ping").await.unwrap();
    client_app.flush().await.unwrap();
    let mut buf = [0u8; 4];
    let read = timeout(Duration::from_secs(1), server_app.read_exact(&mut buf)).await;
    assert!(
        read.is_err() || read.unwrap().is_err(),
        "mismatched PSK should not decrypt transport data"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn noise_ik_peer_handshake_and_echo() {
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);
    let server_pk = server_keys.public_bytes();
    let client_pk = client_keys.public_bytes();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().unwrap();

    let server_task = async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (mut rd, wr) = sock.into_split();
        let mut first = vec![0u8; 65535];
        let nread = read_framed_len(&mut rd, &mut first).await.unwrap();
        server_handshake_peer_with_first_frame(
            rd,
            wr,
            &server_keys,
            &first[..nread],
            Arc::new(AllowPeer(client_pk)),
        )
        .await
        .unwrap()
    };

    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        client_handshake_peer(sock, &client_keys, &server_pk)
            .await
            .unwrap()
    };

    let (mut server_app, mut client_app) = tokio::join!(server_task, client_task);

    client_app.write_all(b"ping").await.unwrap();
    client_app.flush().await.unwrap();
    let mut buf = [0u8; 4];
    timeout(Duration::from_secs(2), server_app.read_exact(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf, b"ping");
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
