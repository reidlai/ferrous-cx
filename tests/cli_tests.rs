use std::process::Command;

#[test]
fn test_setup_topics_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_setup-topics"))
        .arg("--help")
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Cross-Platform Apache Iggy Topic & Consumer Group Provisioner"));
    assert!(stdout.contains("--dry-run"));
}

#[test]
fn test_setup_topics_dry_run_success() {
    let output = Command::new(env!("CARGO_BIN_EXE_setup-topics"))
        .arg("--dry-run")
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("TOPIC SETUP CONFORMANCE AUDIT COMPLETED CLEANLY"));
    assert!(stdout.contains("Validated stream 'orders'"));
    assert!(stdout.contains("Validated stream 'olist'"));
    assert!(stdout.contains("Validated stream 'benchmark'"));
}

#[test]
fn test_setup_topics_missing_registry_fails() {
    let output = Command::new(env!("CARGO_BIN_EXE_setup-topics"))
        .args(["--dry-run", "--registry", "nonexistent-file.yml"])
        .output()
        .expect("Failed to execute binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Missing topic registry"));
}
