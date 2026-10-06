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
