use std::process::{Command, Output};

/// Run the real binary, with no secret in its environment unless one is given.
fn sensapp(args: &[&str], secret: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sensapp"));
    command.args(args).env_remove("SENSAPP_JWT_SECRET");
    if let Some(secret) = secret {
        command.env("SENSAPP_JWT_SECRET", secret);
    }
    command.output().expect("the sensapp binary runs")
}

#[test]
fn generate_token_help_prints_the_usage_and_no_token() {
    for flag in ["--help", "-h"] {
        // Without the secret: the help does not need it
        let output = sensapp(&["generate-token", flag], None);
        assert!(output.status.success(), "{flag}: {output:?}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("Usage: sensapp generate-token"), "{stdout}");
        assert!(!stdout.contains("eyJ"), "{flag} printed a token: {stdout}");
    }
}

#[test]
fn generate_token_help_wins_over_a_subject() {
    let secret = "a-secret-that-is-long-enough-for-the-tests-0123456789";
    let output = sensapp(&["generate-token", "alice", "--help"], Some(secret));
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("eyJ"));
}

#[test]
fn generate_token_refuses_an_option_as_subject() {
    let secret = "a-secret-that-is-long-enough-for-the-tests-0123456789";
    let output = sensapp(&["generate-token", "--scope", "read"], Some(secret));
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stderr).contains("required"));
}

#[test]
fn generate_token_still_makes_a_token() {
    let secret = "a-secret-that-is-long-enough-for-the-tests-0123456789";
    let output = sensapp(&["generate-token", "alice"], Some(secret));
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("eyJ"));
}

#[test]
fn generate_token_sensor_options_make_an_allow_list() {
    let secret = "a-secret-that-is-long-enough-for-the-tests-0123456789";
    for args in [
        &["generate-token", "alice", "--sensors", "a,b"][..],
        &[
            "generate-token",
            "alice",
            "--sensor",
            "a,b",
            "--sensor",
            "c",
        ][..],
        &[
            "generate-token",
            "alice",
            "--scope",
            "read",
            "--duration",
            "60",
        ][..],
    ] {
        let output = sensapp(args, Some(secret));
        assert!(output.status.success(), "{args:?}: {output:?}");
    }
    // A wrong value is refused, with a message and no token
    let output = sensapp(
        &["generate-token", "alice", "--duration", "soon"],
        Some(secret),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
}

#[test]
fn sensapp_help_lists_the_subcommands() {
    let output = sensapp(&["--help"], None);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("generate-token") && stdout.contains("generate-secret"));
}

#[test]
fn serve_is_a_subcommand_with_its_own_help() {
    let output = sensapp(&["serve", "--help"], None);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Run the server"));
    let output = sensapp(&["--help"], None);
    assert!(String::from_utf8_lossy(&output.stdout).contains("serve"));
}

/// `docker stop` and Kubernetes stop a container with SIGTERM. SensApp runs as PID 1 in its
/// image, where an unhandled SIGTERM is ignored: the container was killed at the end of the grace
/// period, in the middle of its writes. Now SIGTERM stops the server like Ctrl+C: no new
/// connection, the requests in flight finish, then the process exits with 0.
///
/// The signals do not depend on the storage: the test runs on a file of its own, in the builds with
/// SQLite.
#[cfg(all(unix, feature = "sqlite"))]
#[test]
fn sigterm_lets_the_requests_in_flight_finish_and_exits() {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let directory = std::env::temp_dir().join(format!("sensapp-sigterm-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut server = Command::new(env!("CARGO_BIN_EXE_sensapp"))
        .env_remove("SENSAPP_JWT_SECRET")
        .env("SENSAPP_AUTH_DISABLED", "true")
        .env("SENSAPP_UI_ENABLED", "false")
        .env("SENSAPP_ENDPOINT", "127.0.0.1")
        .env("SENSAPP_PORT", port.to_string())
        .env(
            "SENSAPP_STORAGE_CONNECTION_STRING",
            format!("sqlite://{}", directory.join("sensapp.db").display()),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the sensapp binary runs");

    let started = Instant::now();
    let mut stream = loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => break stream,
            Err(_) if started.elapsed() < Duration::from_secs(30) => {
                std::thread::sleep(Duration::from_millis(50))
            }
            Err(error) => {
                let _ = server.kill();
                panic!("the server does not listen: {error}");
            }
        }
    };

    // A write whose body is half sent when the signal comes
    let line = b"sigterm value=1.5 1700000000000000000\n";
    write!(
        stream,
        "POST /api/v2/write?bucket=b&org=o HTTP/1.1\r\nHost: localhost\r\n\
         Content-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        line.len()
    )
    .unwrap();
    stream.write_all(&line[..10]).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let killed = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    std::thread::sleep(Duration::from_millis(200));
    stream.write_all(&line[10..]).unwrap();
    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);
    assert!(response.starts_with("HTTP/1.1 204"), "{response:?}");

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = server.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = server.kill();
            panic!("the server did not stop within 10 s of SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "{status:?}");
    std::fs::remove_dir_all(directory).unwrap();
}
