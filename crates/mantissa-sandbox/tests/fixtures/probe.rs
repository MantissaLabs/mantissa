use std::fs;
use std::io::{ErrorKind, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

const LINUX_O_TRUNC: i32 = 0o1000;

/// Runs filesystem and socket assertions inside the helper's restricted process.
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let root = Path::new(&args[1]);
    let mode = args.get(3).map(String::as_str).unwrap_or_default();
    if mode != "child"
        && let Ok(expected) = std::env::var("EXPECTED_ARG0")
    {
        assert_eq!(args[0], expected);
    }
    if mode == "mark" {
        fs::write(root.join("workspace/check-executed"), "ran").unwrap();
        return;
    }

    assert_eq!(
        fs::read_to_string(root.join("readonly/input")).unwrap(),
        "readable"
    );
    fs::write(root.join("workspace/output"), "writable").unwrap();
    assert_eq!(
        fs::write(root.join("readonly/input"), "changed")
            .unwrap_err()
            .kind(),
        ErrorKind::PermissionDenied
    );
    // Linux can truncate a file while opening it read-only, without requesting write access.
    assert_eq!(
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(LINUX_O_TRUNC)
            .open(root.join("readonly/input"))
            .unwrap_err()
            .kind(),
        ErrorKind::PermissionDenied
    );
    for path in ["private/secret", "app/neighbor", "workspace/link"] {
        assert_eq!(
            fs::read(root.join(path)).unwrap_err().kind(),
            ErrorKind::PermissionDenied,
            "read {path}"
        );
    }
    assert_eq!(
        fs::write(root.join("private/output"), "changed")
            .unwrap_err()
            .kind(),
        ErrorKind::PermissionDenied
    );
    assert_eq!(
        fs::read("/proc/self/status").unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    if root == Path::new("/") {
        assert_eq!(
            fs::read_to_string("/etc/ssl/openssl.cnf").unwrap(),
            "test OpenSSL configuration"
        );
        assert_eq!(
            fs::write("/etc/ssl/openssl.cnf", "changed")
                .unwrap_err()
                .kind(),
            ErrorKind::PermissionDenied
        );

        for path in [
            "/etc/mantissa-test-secret",
            "/etc/ssl/mantissa-test-secret",
            "/usr/share/mantissa-test-secret",
        ] {
            assert_eq!(
                fs::read(path).unwrap_err().kind(),
                ErrorKind::PermissionDenied,
                "read {path}"
            );
        }
    }

    if args[2] == "blocked" {
        assert_eq!(
            TcpListener::bind("127.0.0.1:0").unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            TcpStream::connect_timeout(&"127.0.0.1:9".parse().unwrap(), Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            ErrorKind::PermissionDenied
        );
        assert_eq!(
            UdpSocket::bind("127.0.0.1:0").unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
    } else {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        TcpStream::connect_timeout(&listener.local_addr().unwrap(), Duration::from_secs(1))
            .unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").unwrap();
        udp.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        udp.send_to(b"ok", udp.local_addr().unwrap()).unwrap();
        assert_eq!(udp.recv(&mut [0; 2]).unwrap(), 2);
    }

    if mode != "child" {
        assert!(
            Command::new(std::env::current_exe().unwrap())
                .args([&args[1], &args[2], "child"])
                .status()
                .unwrap()
                .success()
        );
    }

    println!("SANDBOX_OK");
    std::io::stdout().flush().unwrap();
    if mode == "hold" {
        loop {
            std::thread::park();
        }
    }
}
