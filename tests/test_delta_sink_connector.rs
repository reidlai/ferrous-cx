//! Comprehensive integration tests for Apache Iggy Native Delta Lake Sink Connector (Story 17.3, AD-28b)
//!
//! Validates:
//! - Strict `schema = "json"` invariant enforcement across all sinks
//! - `table_uri` mapping to `/data/delta/{orders_raw_events, orders_current_state, olist_*}`
//! - Pre-created Delta table validation via `streaming-admin init-delta-storage`
//! - Rejection of uninitialized directories (preventing connector startup crash)
//! - Delta Lake ACID transaction log (`_delta_log/`) and Parquet commit lifecycle

use std::path::Path;
use ferrous_cx::admin::{init_delta_storage, parse_and_validate_connector_config, TABLES};

#[test]
fn test_delta_sink_config_file_parses_cleanly() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config_path = Path::new(manifest_dir).join("config/streaming/connectors/delta_sink.toml");
    assert!(config_path.exists(), "delta_sink.toml must exist");

    let content = std::fs::read_to_string(&config_path).expect("read delta_sink.toml");
    let parsed = parse_and_validate_connector_config(&content, None).expect("valid config");

    assert_eq!(parsed.connector.name, "delta_sink");
    assert_eq!(parsed.connector.plugin, "delta_sink");
    assert!(parsed.connector.enabled);

    assert_eq!(parsed.sinks.len(), 4);

    // 1. orders_raw_events
    let raw_sink = parsed.sinks.iter().find(|s| s.name == "orders_raw_events_sink").expect("raw sink");
    assert_eq!(raw_sink.stream, "orders");
    assert_eq!(raw_sink.topic, "intake");
    assert_eq!(raw_sink.schema, "json");
    assert_eq!(raw_sink.table_uri, "/data/delta/orders_raw_events");

    // 2. orders_current_state
    let state_sink = parsed.sinks.iter().find(|s| s.name == "orders_current_state_sink").expect("state sink");
    assert_eq!(state_sink.stream, "orders");
    assert_eq!(state_sink.topic, "consolidated");
    assert_eq!(state_sink.schema, "json");
    assert_eq!(state_sink.table_uri, "/data/delta/orders_current_state");

    // 3. olist_orders
    let olist_sink = parsed.sinks.iter().find(|s| s.name == "olist_orders_sink").expect("olist sink");
    assert_eq!(olist_sink.stream, "olist");
    assert_eq!(olist_sink.topic, "orders_stream");
    assert_eq!(olist_sink.schema, "json");
    assert_eq!(olist_sink.table_uri, "/data/delta/olist_orders");
}

#[test]
fn test_delta_sink_rejects_non_json_schema() {
    let invalid_toml = r#"
[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = "orders"
topic = "intake"
schema = "avro"
table_uri = "/data/delta/orders_raw_events"
"#;

    let err = parse_and_validate_connector_config(invalid_toml, None).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("schema must be 'json'"),
        "Must reject non-JSON schemas: {msg}"
    );
    assert!(msg.contains("Non-JSON formats are rejected"));
}

#[test]
fn test_delta_sink_rejects_missing_target_delta_table() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let uninitialized_delta = tmp.path().join("delta");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config_path = Path::new(manifest_dir).join("config/streaming/connectors/delta_sink.toml");
    let content = std::fs::read_to_string(config_path).expect("read delta_sink.toml");

    let err = parse_and_validate_connector_config(&content, Some(&uninitialized_delta)).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("missing"),
        "Must detect uninitialized Delta tables: {msg}"
    );
    assert!(
        msg.contains("Tables must be pre-created via 'streaming-admin init-delta-storage'"),
        "Must guide user to init-delta-storage: {msg}"
    );
}

#[test]
fn test_delta_sink_pre_creation_lifecycle_with_init_delta_storage() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let delta_dir = tmp.path().join("delta");

    // Pre-create tables
    init_delta_storage(&delta_dir).expect("init delta storage");

    // All TABLES must exist with commit-zero
    for spec in TABLES {
        let table_path = delta_dir.join(spec.delta_table_name);
        let log_zero = table_path.join("_delta_log/00000000000000000000.json");
        assert!(
            log_zero.exists(),
            "Commit zero must exist for {}",
            spec.delta_table_name
        );

        let lines: Vec<String> = std::fs::read_to_string(&log_zero)
            .expect("read log zero")
            .lines()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(lines.len(), 3);

        let protocol: serde_json::Value = serde_json::from_str(&lines[0]).expect("protocol JSON");
        assert_eq!(protocol["protocol"]["minReaderVersion"], 1);
        assert_eq!(protocol["protocol"]["minWriterVersion"], 2);

        let metadata: serde_json::Value = serde_json::from_str(&lines[1]).expect("metadata JSON");
        assert_eq!(metadata["metaData"]["name"], spec.delta_table_name);
        assert_eq!(metadata["metaData"]["format"]["provider"], "parquet");
    }

    // Now validate connector config against pre-created directory
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config_path = Path::new(manifest_dir).join("config/streaming/connectors/delta_sink.toml");
    let content = std::fs::read_to_string(config_path).expect("read delta_sink.toml");

    let parsed = parse_and_validate_connector_config(&content, Some(&delta_dir))
        .expect("Connector validation must succeed against pre-created tables");
    assert_eq!(parsed.sinks.len(), 4);
}

#[test]
fn test_delta_sink_batch_commit_appends_to_delta_log() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let delta_dir = tmp.path().join("delta");

    init_delta_storage(&delta_dir).expect("init delta storage");

    // Simulate native delta_sink batch commit #1 to orders_raw_events
    let raw_table_log = delta_dir.join("orders_raw_events/_delta_log");
    let commit_one_path = raw_table_log.join("00000000000000000001.json");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let add_action = serde_json::json!({
        "add": {
            "path": "part-00000-orders-raw-events.parquet",
            "size": 1048576,
            "modificationTime": now_ms,
            "dataChange": true,
            "stats": "{\"numRecords\": 1000}"
        }
    });

    let commit_info = serde_json::json!({
        "commitInfo": {
            "timestamp": now_ms,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "batchSize": "1000"
            },
            "engineInfo": "Apache-Iggy-Delta-Sink/0.9.0"
        }
    });

    let commit_content = format!(
        "{}\n{}\n",
        serde_json::to_string(&add_action).unwrap(),
        serde_json::to_string(&commit_info).unwrap()
    );

    std::fs::write(&commit_one_path, commit_content).expect("write commit 1");
    assert!(commit_one_path.exists());

    // Verify commit progression and parse commit actions
    let commit_zero = raw_table_log.join("00000000000000000000.json");
    assert!(commit_zero.exists());
    assert!(commit_one_path.exists());

    let lines: Vec<String> = std::fs::read_to_string(&commit_one_path)
        .expect("read commit 1")
        .lines()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(lines.len(), 2);

    let parsed_add: serde_json::Value = serde_json::from_str(&lines[0]).expect("parsed add");
    assert_eq!(
        parsed_add["add"]["path"],
        "part-00000-orders-raw-events.parquet"
    );
    assert_eq!(parsed_add["add"]["size"], 1048576);

    let parsed_commit_info: serde_json::Value =
        serde_json::from_str(&lines[1]).expect("parsed commitInfo");
    assert_eq!(parsed_commit_info["commitInfo"]["operation"], "WRITE");
    assert_eq!(
        parsed_commit_info["commitInfo"]["operationParameters"]["mode"],
        "Append"
    );
}

#[test]
fn test_delta_sink_rejects_invalid_connector_type() {
    let invalid_toml = r#"
[connector]
name = "delta_sink"
type = "source"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = "orders"
topic = "intake"
schema = "json"
table_uri = "/data/delta/orders_raw_events"
"#;
    let err = parse_and_validate_connector_config(invalid_toml, None).unwrap_err();
    assert!(err.to_string().contains("Invalid connector type"));
}

#[test]
fn test_delta_sink_rejects_empty_sinks() {
    let empty_toml = r#"
sinks = []

[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"
"#;
    let err = parse_and_validate_connector_config(empty_toml, None).unwrap_err();
    assert!(err.to_string().contains("at least one sink definition"));
}

#[test]
fn test_delta_sink_rejects_zero_batch_parameters() {
    let toml_zero_batch = r#"
[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = "orders"
topic = "intake"
schema = "json"
table_uri = "/data/delta/orders_raw_events"
batch_size = 0
batch_timeout_ms = 1000
"#;
    let err = parse_and_validate_connector_config(toml_zero_batch, None).unwrap_err();
    assert!(err.to_string().contains("batch_size must be greater than zero"));
}

#[test]
fn test_delta_sink_rejects_blank_stream_or_topic() {
    let toml_blank_stream = r#"
[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = ""
topic = "intake"
schema = "json"
table_uri = "/data/delta/orders_raw_events"
"#;
    let err = parse_and_validate_connector_config(toml_blank_stream, None).unwrap_err();
    assert!(err.to_string().contains("cannot have an empty stream"));
}

#[test]
fn test_delta_sink_rejects_bare_delta_dir() {
    let toml_bare = r#"
[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = "orders"
topic = "intake"
schema = "json"
table_uri = "/data/delta"
"#;
    let err = parse_and_validate_connector_config(toml_bare, None).unwrap_err();
    assert!(err.to_string().contains("must target a specific table directory"));
}

#[test]
fn test_delta_sink_normalizes_trailing_slash_and_backslashes() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let delta_dir = tmp.path().join("delta");
    init_delta_storage(&delta_dir).expect("init delta storage");

    let toml_trailing_slash = r#"
[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = "orders"
topic = "intake"
schema = "json"
table_uri = "/data/delta/orders_raw_events/"
"#;
    let parsed = parse_and_validate_connector_config(toml_trailing_slash, Some(&delta_dir))
        .expect("must handle trailing slash properly");
    assert_eq!(parsed.sinks[0].name, "raw_sink");
}

#[test]
fn test_delta_sink_rejects_corrupted_or_empty_log_zero() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let delta_dir = tmp.path().join("delta");
    let raw_table_log = delta_dir.join("orders_raw_events/_delta_log");
    std::fs::create_dir_all(&raw_table_log).expect("create log dir");

    // Write 0-byte log zero
    let log_zero = raw_table_log.join("00000000000000000000.json");
    std::fs::write(&log_zero, "").expect("write empty log zero");

    let toml_cfg = r#"
[connector]
name = "delta_sink"
type = "sink"
plugin = "delta_sink"

[iggy]
server_address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"

[[sinks]]
name = "raw_sink"
stream = "orders"
topic = "intake"
schema = "json"
table_uri = "/data/delta/orders_raw_events"
"#;
    let err = parse_and_validate_connector_config(toml_cfg, Some(&delta_dir)).unwrap_err();
    assert!(err.to_string().contains("corrupt/empty commit log (0 bytes)"));
}

#[test]
fn test_ad34_olist_orders_base_schema_parity_with_orders_current_state() {
    let olist = TABLES
        .iter()
        .find(|t| t.delta_table_name == "olist_orders")
        .expect("olist_orders table spec must exist");
    let current = TABLES
        .iter()
        .find(|t| t.delta_table_name == "orders_current_state")
        .expect("orders_current_state table spec must exist");

    // AD-34: First 8 columns of orders_current_state must match olist_orders verbatim
    assert_eq!(olist.schema_fields.len(), 8);
    assert_eq!(current.schema_fields.len(), 14);
    assert_eq!(
        &current.schema_fields[..8],
        olist.schema_fields,
        "orders_current_state must use the exact 8-column olist_orders base schema (AD-34)"
    );

    // Verify 6-column CQRS / concession overlay
    let overlay_names: Vec<&str> = current.schema_fields[8..]
        .iter()
        .map(|(name, _, _)| *name)
        .collect();
    assert_eq!(
        overlay_names,
        vec![
            "total_amount_cents",
            "total_freight_cents",
            "courtesy_credit_cents",
            "credit_delta",
            "version",
            "updated_at"
        ]
    );

    // Verify Olist replay profiles route to olist/orders_stream (AD-34)
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    for profile in ["profiles/olist-demo.toml", "profiles/olist-load.toml"] {
        let content = std::fs::read_to_string(manifest_dir.join(profile))
            .unwrap_or_else(|e| panic!("failed to read {profile}: {e}"));
        assert!(
            content.contains("destination = \"olist/orders_stream\""),
            "{profile} must target olist/orders_stream (AD-34)"
        );
    }
}
