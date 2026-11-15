use std::process::Command;

#[test]
fn test_setup_iam_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .arg("--help")
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Cross-Platform Kanidm IAM Dev TLS Cert & Persona Provisioner"));
    assert!(stdout.contains("--generate-certs"));
    assert!(stdout.contains("--dry-run"));
}

#[test]
fn test_setup_iam_dry_run_success() {
    let output = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .arg("--dry-run")
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("IAM CONFORMANCE AUDIT COMPLETED CLEANLY"));
    assert!(stdout.contains("customer_specialist"));
    assert!(stdout.contains("operations_manager"));
    assert!(stdout.contains("system_admin"));
    assert!(stdout.contains("svc_ferrous_api"));
    assert!(stdout.contains("svc_ferrous_agent"));
}

#[test]
fn test_setup_iam_isolated_flags() {
    // --seed-rbac only
    let rbac_out = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--seed-rbac", "--dry-run"])
        .output()
        .expect("Failed to execute binary");
    assert!(rbac_out.status.success());
    let stdout = String::from_utf8_lossy(&rbac_out.stdout);
    assert!(stdout.contains("Auditing / Seeding Human RBAC Personas"));
    assert!(!stdout.contains("Auditing / Seeding M2M Service Accounts"));

    // --seed-m2m only
    let m2m_out = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--seed-m2m", "--dry-run"])
        .output()
        .expect("Failed to execute binary");
    assert!(m2m_out.status.success());
    let stdout = String::from_utf8_lossy(&m2m_out.stdout);
    assert!(stdout.contains("Auditing / Seeding M2M Service Accounts"));
    assert!(!stdout.contains("Auditing / Seeding Human RBAC Personas"));

    // --all
    let all_out = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--all", "--dry-run"])
        .output()
        .expect("Failed to execute binary");
    assert!(all_out.status.success());
    let stdout = String::from_utf8_lossy(&all_out.stdout);
    assert!(stdout.contains("Generating / Auditing Dev TLS Certificates"));
    assert!(stdout.contains("Auditing / Seeding Human RBAC Personas"));
    assert!(stdout.contains("Auditing / Seeding M2M Service Accounts"));
}

#[test]
fn test_setup_iam_cert_generation_and_idempotency() {
    let temp_dir = std::env::temp_dir().join(format!("test_iam_certs_{}", std::process::id()));
    let output1 = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--generate-certs", "--certs-dir", temp_dir.to_str().unwrap()])
        .output()
        .expect("Failed to execute binary");

    assert!(output1.status.success());
    assert!(temp_dir.join("chain.pem").exists());
    assert!(temp_dir.join("key.pem").exists());

    let chain = std::fs::read_to_string(temp_dir.join("chain.pem")).unwrap();
    let key = std::fs::read_to_string(temp_dir.join("key.pem")).unwrap();
    assert!(chain.contains("BEGIN CERTIFICATE"));
    assert!(key.contains("BEGIN PRIVATE KEY"));

    // Idempotent re-run without --force should succeed without error
    let output2 = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--generate-certs", "--certs-dir", temp_dir.to_str().unwrap()])
        .output()
        .expect("Failed to execute binary");
    assert!(output2.status.success());
    let stdout = String::from_utf8_lossy(&output2.stdout);
    assert!(stdout.contains("Valid TLS certificates already exist on disk"));

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[test]
fn test_setup_iam_relative_certs_dir() {
    let rel_dir_name = format!("target/test_rel_certs_{}", std::process::id());
    let rel_path = std::path::Path::new(&rel_dir_name);
    let _ = std::fs::remove_dir_all(rel_path);

    let output = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--generate-certs", "--certs-dir", &rel_dir_name])
        .output()
        .expect("Failed to execute binary");

    assert!(output.status.success());
    assert!(rel_path.join("chain.pem").exists());
    assert!(rel_path.join("key.pem").exists());

    let _ = std::fs::remove_dir_all(rel_path);
}

#[test]
fn test_setup_iam_missing_required_roles_fails() {
    let temp_dir = std::env::temp_dir().join(format!("test_iam_bad_config_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);

    // Empty RBAC groups
    let bad_rbac = r#"{
        "domain": "test.local",
        "groups": [],
        "users": []
    }"#;
    std::fs::write(temp_dir.join("seed-rbac.json"), bad_rbac).unwrap();
    std::fs::write(temp_dir.join("seed-m2m.json"), r#"{"domain": "test.local", "service_accounts": []}"#).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_setup-iam"))
        .args(["--dry-run", "--config-dir", temp_dir.to_str().unwrap()])
        .output()
        .expect("Failed to execute binary");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("missing required role group"));

    let _ = std::fs::remove_dir_all(temp_dir);
}
