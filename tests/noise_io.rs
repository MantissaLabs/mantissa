use mantissa::noise::{client_handshake, server_handshake};
use tokio::time::{timeout, Duration};
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
