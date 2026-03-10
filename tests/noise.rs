#![allow(clippy::unwrap_used)]

use async_trait::async_trait;
use net::noise::{
    ClientJoinHandshake, HandshakeKind, NoisePeerVerifier, client_handshake_join,
    client_handshake_join_with_probe, client_handshake_peer, join_probe_client, join_probe_server,
    read_framed_len, server_handshake_join, server_handshake_peer_with_first_frame,
    server_handshake_select, write_framed,
};
use snow::params::NoiseParams;
use std::io::ErrorKind;
use std::rc::Rc;
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

struct DenyPeer;

#[async_trait(?Send)]
impl NoisePeerVerifier for DenyPeer {
    async fn is_allowed(&self, _remote_static: &[u8]) -> std::io::Result<bool> {
        Ok(false)
    }
}

const PROLOGUE: &[u8] = b"MANTISSA|v1";
const NOISE_PARAMS_JOIN: &str = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s";

async fn legacy_client_handshake_no_hello(
    mut tcp: TcpStream,
    keys: &net::noise::NoiseKeys,
    psk: &[u8; 32],
) -> std::io::Result<()> {
    let pk_bytes = keys.private.to_bytes();
    let params: NoiseParams = NOISE_PARAMS_JOIN.parse().unwrap();
    let builder = snow::Builder::new(params)
        .prologue(PROLOGUE)
        .local_private_key(&pk_bytes)
        .psk(3, psk);
    let mut hs = builder
        .build_initiator()
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let (mut rd, mut wr) = tcp.split();
    let mut out = vec![0u8; 65535];

    // -> e (legacy: empty payload)
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // <- e, ee, s, es (ignore payload)
    let mut inb = vec![0u8; 65535];
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // -> s, se (empty payload)
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    Ok(())
}

async fn legacy_server_handshake_no_probe(
    tcp: TcpStream,
    keys: &net::noise::NoiseKeys,
    psk: &[u8; 32],
) -> std::io::Result<()> {
    let pk_bytes = keys.private.to_bytes();
    let params: NoiseParams = NOISE_PARAMS_JOIN.parse().unwrap();
    let builder = snow::Builder::new(params)
        .prologue(PROLOGUE)
        .local_private_key(&pk_bytes)
        .psk(3, psk);
    let mut hs = builder
        .build_responder()
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    let (mut rd, mut wr) = tcp.into_split();
    let mut out = vec![0u8; 65535];
    let mut inb = vec![0u8; 65535];

    // <- e (ignore payload)
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // -> e, ee, s, es (legacy: no probe ack)
    let n = hs
        .write_message(&[], &mut out)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    write_framed(&mut wr, &out[..n]).await?;

    // <- s, se
    let nread = read_framed_len(&mut rd, &mut inb).await?;
    hs.read_message(&inb[..nread], &mut out)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    Ok(())
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
            Rc::new(AllowPeer(client_pk)),
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
async fn noise_join_probe_negotiation_enabled() {
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);
    let psk = net::noise::derive_psk_from_token("MNTISA-1-probe-token").expect("derive psk");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().unwrap();

    let server_task = async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (mut rd, wr) = sock.into_split();
        let mut first = vec![0u8; 65535];
        let nread = read_framed_len(&mut rd, &mut first).await.unwrap();
        let mut handshake = server_handshake_select(
            rd,
            wr,
            &server_keys,
            &psk,
            &first[..nread],
            Rc::new(DenyPeer),
        )
        .await
        .unwrap();

        assert_eq!(handshake.kind, HandshakeKind::Join);
        assert!(handshake.join_probe);

        join_probe_server(&mut handshake.stream).await.unwrap();
        handshake.stream.write_all(b"ping").await.unwrap();
        handshake.stream.flush().await.unwrap();
    };

    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        let mut handshake: ClientJoinHandshake =
            client_handshake_join_with_probe(sock, &client_keys, &psk)
                .await
                .unwrap();
        assert!(handshake.probe_enabled);
        join_probe_client(&mut handshake.stream).await.unwrap();
        let mut buf = [0u8; 4];
        handshake.stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    };

    tokio::join!(server_task, client_task);
}

#[tokio::test(flavor = "current_thread")]
async fn noise_join_probe_legacy_server_ignored() {
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);
    let psk = net::noise::derive_psk_from_token("MNTISA-1-legacy-server").expect("derive psk");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().unwrap();

    let server_task = async move {
        let (sock, _) = listener.accept().await.unwrap();
        legacy_server_handshake_no_probe(sock, &server_keys, &psk)
            .await
            .unwrap();
    };

    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        let handshake: ClientJoinHandshake =
            client_handshake_join_with_probe(sock, &client_keys, &psk)
                .await
                .unwrap();
        assert!(!handshake.probe_enabled);
    };

    tokio::join!(server_task, client_task);
}

#[tokio::test(flavor = "current_thread")]
async fn noise_join_probe_legacy_client_no_hello() {
    let server_keys = fixed_noise_keys(11);
    let client_keys = fixed_noise_keys(22);
    let psk = net::noise::derive_psk_from_token("MNTISA-1-legacy-client").expect("derive psk");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().unwrap();

    let server_task = async move {
        let (sock, _) = listener.accept().await.unwrap();
        let (mut rd, wr) = sock.into_split();
        let mut first = vec![0u8; 65535];
        let nread = read_framed_len(&mut rd, &mut first).await.unwrap();
        let handshake = server_handshake_select(
            rd,
            wr,
            &server_keys,
            &psk,
            &first[..nread],
            Rc::new(DenyPeer),
        )
        .await
        .unwrap();

        assert_eq!(handshake.kind, HandshakeKind::Join);
        assert!(!handshake.join_probe);
    };

    let client_task = async move {
        let sock = TcpStream::connect(addr).await.unwrap();
        legacy_client_handshake_no_hello(sock, &client_keys, &psk)
            .await
            .unwrap();
    };

    tokio::join!(server_task, client_task);
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
