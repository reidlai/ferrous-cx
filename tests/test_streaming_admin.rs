//! Integration tests for cross-platform streaming administrative tools (Story 13.10, Story 16.14)

use std::process::Command;

#[test]
fn test_streaming_admin_provision_topics_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["provision-topics", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin provision-topics");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Provision Ingestion Topics & Partition Durability"));
    assert!(stdout.contains("Validated stream 'orders' (id: 1) specification."));
    assert!(stdout.contains("Validated topic 'intake' (id: 1, partitions: 4, fsync: immediate)"));
    assert!(stdout.contains("Validated topic 'consolidated' (id: 2, partitions: 4, fsync: immediate)"));
    assert!(stdout.contains("Validated stream 'olist' (id: 3) specification."));
    assert!(stdout.contains("Validated topic 'orders_stream' (id: 1, partitions: 4, fsync: immediate)"));
}

#[test]
fn test_streaming_admin_provision_consumer_groups_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["provision-consumer-groups", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin provision-consumer-groups");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Provision Zero-Copy Consumer Groups"));
    assert!(stdout.contains("Validated consumer group 'cg-arroyo-raw' (id: 1)"));
    assert!(stdout.contains("Validated consumer group 'cg-cmd-handler' (id: 2)"));
    assert!(stdout.contains("Validated consumer group 'cg-arroyo-state' (id: 1)"));
    assert!(stdout.contains("Validated consumer group 'cg-arroyo-olist' (id: 1)"));
}

#[test]
fn test_streaming_admin_verify_broker_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["verify-broker", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin verify-broker");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Verify Apache Iggy Broker & Consumer Group Health"));
    assert!(stdout.contains("Validated Apache Iggy health probe (port 3000/ping)"));
    assert!(stdout.contains("cg-arroyo-olist"));
}

#[test]
fn test_streaming_admin_init_delta_storage_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["init-delta-storage", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin init-delta-storage");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Initialize Delta Lake Storage Sinks"));
    assert!(stdout.contains("orders_raw_events"));
    assert!(stdout.contains("orders_current_state"));
    assert!(stdout.contains("hpc_benchmarks"));
    assert!(stdout.contains("olist_orders"));
}

#[test]
fn test_streaming_admin_validate_connectors_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["validate-connectors", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin validate-connectors");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Validate Apache Iggy Delta Lake Sink Connector Config"));
    assert!(stdout.contains("orders_raw_events_sink"));
    assert!(stdout.contains("orders_current_state_sink"));
    assert!(stdout.contains("olist_orders_sink"));
    assert!(stdout.contains("All connector sinks validated with schema = 'json'"));
}

#[test]
fn test_streaming_admin_all_dry_run() {
    let output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["all", "--dry-run"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin all");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[Step 2] Provision Ingestion Topics & Partition Durability"));
    assert!(stdout.contains("[Step 3] Provision Zero-Copy Consumer Groups"));
    assert!(stdout.contains("[Step 4] Verify Apache Iggy Broker & Consumer Group Health"));
    assert!(stdout.contains("[Step 5] Initialize Delta Lake Storage Sinks"));
    assert!(stdout.contains("[Step 6] Validate Apache Iggy Delta Lake Sink Connector Config"));
    assert!(stdout.contains("[SUCCESS] All platform streaming steps completed successfully!"));
}

#[test]
fn test_streaming_admin_validate_connectors_real_storage_success() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let delta_dir = tmp.path().join("delta");

    // Initialize delta storage for real
    let init_output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args(["init-delta-storage", "--delta-dir", delta_dir.to_str().unwrap()])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin init-delta-storage");
    assert!(init_output.status.success());

    // Validate connectors against real pre-created directory
    let val_output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args([
            "validate-connectors",
            "--delta-dir",
            delta_dir.to_str().unwrap(),
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin validate-connectors");
    assert!(val_output.status.success());
    let stdout = String::from_utf8_lossy(&val_output.stdout);
    assert!(stdout.contains("All connector sinks validated with schema = 'json' and pre-created tables!"));
}

#[test]
fn test_streaming_admin_validate_connectors_missing_table_fails() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let uninitialized_dir = tmp.path().join("delta_empty");

    // Validate connectors against empty directory without init
    let val_output = Command::new(env!("CARGO_BIN_EXE_streaming-admin"))
        .args([
            "validate-connectors",
            "--delta-dir",
            uninitialized_dir.to_str().unwrap(),
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute streaming-admin validate-connectors");
    assert!(!val_output.status.success());
    let stderr = String::from_utf8_lossy(&val_output.stderr);
    assert!(stderr.contains("missing at"));
    assert!(stderr.contains("streaming-admin init-delta-storage"));
}
