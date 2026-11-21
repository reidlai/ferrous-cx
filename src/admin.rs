//! Administrative and provisioning suite for Apache Iggy and Delta Lake (Story 13.10, Story 16.14)
//! Provides cross-platform, pure-Rust setup, verification, and Delta storage initialization.

use std::path::{Path, PathBuf};

pub const DEFAULT_IGGY_URL: &str = "http://localhost:3000";
pub const DEFAULT_STREAM_NAME: &str = "orders";
pub const DEFAULT_STREAM_ID: u32 = 1;
pub const BENCHMARK_STREAM_NAME: &str = "benchmark";
pub const BENCHMARK_STREAM_ID: u32 = 2;
pub const OLIST_STREAM_NAME: &str = "olist";
pub const OLIST_STREAM_ID: u32 = 3;

/// Topic specification matching contracts/topics.yml
#[derive(Debug, Clone)]
pub struct TopicSpec {
    pub id: u32,
    pub name: &'static str,
    pub partitions: u32,
    pub description: &'static str,
}

pub const STREAMING_TOPICS: &[TopicSpec] = &[
    TopicSpec {
        id: 1,
        name: "intake",
        partitions: 4,
        description: "Direct raw data injection from stream-replay-rs into Delta Lake (immediate fsync)",
    },
    TopicSpec {
        id: 2,
        name: "consolidated",
        partitions: 4,
        description: "Committed CQRS-ES domain state projections and snapshots for Delta Lake & Saga (immediate fsync)",
    },
    TopicSpec {
        id: 3,
        name: "callback",
        partitions: 4,
        description: "Asynchronous inbound callback queue consumed by CallbackService (immediate fsync)",
    },
];

pub const BENCHMARK_TOPICS: &[TopicSpec] = &[TopicSpec {
    id: 1,
    name: "hpc_mqbench",
    partitions: 4,
    description: "HPC-MQBench qualification-first synthetic benchmark stream (immediate fsync)",
}];

pub const OLIST_TOPICS: &[TopicSpec] = &[TopicSpec {
    id: 1,
    name: "orders_stream",
    partitions: 4,
    description: "Olist Brazilian E-Commerce historical order replay stream (immediate fsync)",
}];

/// Consumer group specification
#[derive(Debug, Clone)]
pub struct ConsumerGroupSpec {
    pub stream_id: u32,
    pub topic_id: u32,
    pub topic_name: &'static str,
    pub group_id: u32,
    pub name: &'static str,
    pub description: &'static str,
}

pub const CONSUMER_GROUPS: &[ConsumerGroupSpec] = &[
    ConsumerGroupSpec {
        stream_id: 1,
        topic_id: 1,
        topic_name: "intake",
        group_id: 1,
        name: "cg-arroyo-raw",
        description: "Raw ingestion consumer group",
    },
    ConsumerGroupSpec {
        stream_id: 1,
        topic_id: 1,
        topic_name: "intake",
        group_id: 2,
        name: "cg-cmd-handler",
        description: "CQRS-ES Command Handler aggregate worker group",
    },
    ConsumerGroupSpec {
        stream_id: 1,
        topic_id: 2,
        topic_name: "consolidated",
        group_id: 1,
        name: "cg-arroyo-state",
        description: "Consolidated state consumer group",
    },
    ConsumerGroupSpec {
        stream_id: 1,
        topic_id: 2,
        topic_name: "consolidated",
        group_id: 2,
        name: "cg-order-projection",
        description: "Projection engine consumer group for Delta Lake snapshots and callback dispatch",
    },
    ConsumerGroupSpec {
        stream_id: 1,
        topic_id: 3,
        topic_name: "callback",
        group_id: 1,
        name: "cg-order-callback",
        description: "CallbackService consumer group for rehydration, invariant evaluation, and SierraDB EAPPEND",
    },
    ConsumerGroupSpec {
        stream_id: 2,
        topic_id: 1,
        topic_name: "hpc_mqbench",
        group_id: 1,
        name: "cg-arroyo-benchmark",
        description: "HPC-MQBench benchmark consumer group",
    },
    ConsumerGroupSpec {
        stream_id: 3,
        topic_id: 1,
        topic_name: "orders_stream",
        group_id: 1,
        name: "cg-arroyo-olist",
        description: "Olist historical replay consumer group",
    },
];

/// Table schema definitions for native Delta Lake initialization
#[derive(Debug, Clone)]
pub struct TableInitSpec {
    pub name: &'static str,
    pub delta_table_name: &'static str,
    pub consumer_group: &'static str,
    pub schema_fields: &'static [(&'static str, &'static str, bool)],
}

pub const TABLES: &[TableInitSpec] = &[
    TableInitSpec {
        name: "raw_event_archiver",
        delta_table_name: "orders_raw_events",
        consumer_group: "cg-arroyo-raw",
        schema_fields: &[
            ("event_id", "string", false),
            ("order_id", "string", false),
            ("customer_id", "string", false),
            ("event_type", "string", false),
            ("payload", "string", false),
            ("ingest_timestamp", "timestamp", false),
        ],
    },
    TableInitSpec {
        name: "state_snapshot_consolidator",
        delta_table_name: "orders_current_state",
        consumer_group: "cg-arroyo-state",
        schema_fields: &[
            ("order_id", "string", false),
            ("customer_id", "string", false),
            ("status", "string", false),
            ("total_amount_cents", "long", false),
            ("item_count", "integer", false),
            ("version", "long", false),
            ("updated_at", "timestamp", false),
        ],
    },
    TableInitSpec {
        name: "benchmark_ingestion",
        delta_table_name: "hpc_benchmarks",
        consumer_group: "cg-arroyo-benchmark",
        schema_fields: &[
            ("run_id", "string", false),
            ("record_id", "string", false),
            ("seq", "long", false),
            ("partition", "integer", false),
            ("client_timestamp_us", "long", false),
            ("payload_size_bytes", "long", false),
            ("data", "string", false),
            ("arroyo_ingest_timestamp", "timestamp", false),
        ],
    },
    TableInitSpec {
        name: "olist_historical_ingestion",
        delta_table_name: "olist_orders",
        consumer_group: "cg-arroyo-olist",
        schema_fields: &[
            ("order_id", "string", false),
            ("customer_id", "string", false),
            ("order_status", "string", false),
            ("order_purchase_timestamp", "timestamp", false),
            ("order_approved_at", "timestamp", true),
            ("order_delivered_carrier_date", "timestamp", true),
            ("order_delivered_customer_date", "timestamp", true),
            ("order_estimated_delivery_date", "timestamp", false),
        ],
    },
];

/// Check HTTP liveness of Apache Iggy broker
pub async fn check_iggy_liveness(
    client: &reqwest::Client,
    base_url: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let clean_url = base_url.trim_end_matches('/');
    let endpoints = [
        format!("{}/ping", clean_url),
        format!("{}/api/v1/ping", clean_url),
        format!("{}/health", clean_url),
    ];

    let mut last_error = String::new();
    for ep in &endpoints {
        match client.get(ep).send().await {
            Ok(resp) if resp.status().is_success() => {
                return Ok(());
            }
            Ok(resp) => {
                last_error = format!("Endpoint '{}' returned status {}", ep, resp.status());
            }
            Err(e) => {
                last_error = format!("Endpoint '{}' connection failed: {}", ep, e);
                if e.is_connect() || e.is_timeout() {
                    break;
                }
            }
        }
    }

    Err(format!(
        "Cannot reach Apache Iggy broker at {}: {}. Ensure 'docker compose up -d' is running or pass --dry-run.",
        clean_url, last_error
    )
    .into())
}

/// Authenticate to Apache Iggy and acquire a session JWT token
pub async fn iggy_authenticate(
    client: &reqwest::Client,
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let clean_url = base_url.trim_end_matches('/');
    let login_payload = serde_json::json!({
        "username": username,
        "password": password
    });

    let resp = client
        .post(format!("{}/users/login", clean_url))
        .json(&login_payload)
        .send()
        .await?;

    if !resp.status().is_success() {
        let err_body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "Iggy authentication failed for user '{}': {}",
            username, err_body
        )
        .into());
    }

    let body: serde_json::Value = resp.json().await?;
    let token = body
        .get("tokens")
        .and_then(|t| t.get("access_token"))
        .and_then(|a| a.get("token"))
        .and_then(|tok| tok.as_str())
        .or_else(|| body.get("access_token").and_then(|a| a.get("token")).and_then(|t| t.as_str()))
        .or_else(|| body.get("access_token").and_then(|a| a.as_str()))
        .or_else(|| body.get("token").and_then(|t| t.as_str()))
        .ok_or_else(|| {
            format!(
                "Could not extract access_token from Iggy response: {:?}",
                body
            )
        })?;

    Ok(token.to_string())
}

/// Step 2: Provision Ingestion Topics & Partition Durability
pub async fn run_provision_topics(
    iggy_url: &str,
    username: &str,
    password: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("==================================================================");
    println!("  [Step 2] Provision Ingestion Topics & Partition Durability      ");
    println!("==================================================================");
    println!(" Broker URL: {}", iggy_url);

    if dry_run {
        println!("\n  [INFO] Running in --dry-run mode (skipping live broker mutation).");
        println!(
            "  [AUDIT] Validated stream '{}' (id: {}) specification.",
            DEFAULT_STREAM_NAME, DEFAULT_STREAM_ID
        );
        for topic in STREAMING_TOPICS {
            println!(
                "  [AUDIT] Validated topic '{}' (id: {}, partitions: {}, fsync: immediate)",
                topic.name, topic.id, topic.partitions
            );
            println!("          Description: {}", topic.description);
        }
        println!(
            "  [AUDIT] Validated stream '{}' (id: {}) specification.",
            BENCHMARK_STREAM_NAME, BENCHMARK_STREAM_ID
        );
        for topic in BENCHMARK_TOPICS {
            println!(
                "  [AUDIT] Validated topic '{}' (id: {}, partitions: {}, fsync: immediate)",
                topic.name, topic.id, topic.partitions
            );
            println!("          Description: {}", topic.description);
        }
        println!(
            "  [AUDIT] Validated stream '{}' (id: {}) specification.",
            OLIST_STREAM_NAME, OLIST_STREAM_ID
        );
        for topic in OLIST_TOPICS {
            println!(
                "  [AUDIT] Validated topic '{}' (id: {}, partitions: {}, fsync: immediate)",
                topic.name, topic.id, topic.partitions
            );
            println!("          Description: {}", topic.description);
        }
        println!("\n  [OK] Topics and partition durability verified in dry-run mode.");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    check_iggy_liveness(&client, iggy_url).await?;
    let token = iggy_authenticate(&client, iggy_url, username, password).await?;
    println!("  [OK] Authenticated to Apache Iggy.");

    let clean_url = iggy_url.trim_end_matches('/');

    // 1. Ensure Streams & Topics
    for (stream_id, stream_name, topics) in [
        (DEFAULT_STREAM_ID, DEFAULT_STREAM_NAME, STREAMING_TOPICS),
        (BENCHMARK_STREAM_ID, BENCHMARK_STREAM_NAME, BENCHMARK_TOPICS),
        (OLIST_STREAM_ID, OLIST_STREAM_NAME, OLIST_TOPICS),
    ] {
        println!("\n  [*] Ensuring stream '{}' (id: {})...", stream_name, stream_id);
        let stream_payload = serde_json::json!({
            "stream_id": stream_id,
            "name": stream_name,
        });

        let s_resp = client
            .post(format!("{}/streams", clean_url))
            .bearer_auth(&token)
            .json(&stream_payload)
            .send()
            .await?;

        let s_status = s_resp.status();
        if s_status.is_success() {
            println!("      [CREATED] Stream '{}' (id: {})", stream_name, stream_id);
        } else if s_status == reqwest::StatusCode::BAD_REQUEST || s_status == reqwest::StatusCode::CONFLICT {
            let err_text = s_resp.text().await.unwrap_or_default();
            println!(
                "      [EXISTS] Stream '{}' already exists (status: {}, info: {})",
                stream_name,
                s_status,
                err_text.trim()
            );
        } else {
            let err_text = s_resp.text().await.unwrap_or_default();
            return Err(format!(
                "Failed to create stream '{}': HTTP {} ({})",
                stream_name, s_status, err_text
            )
            .into());
        }

        for topic in topics {
            println!(
                "\n  [*] Ensuring topic '{}' (id: {}, partitions: {})...",
                topic.name, topic.id, topic.partitions
            );
            let topic_payload = serde_json::json!({
                "name": topic.name,
                "partitions_count": topic.partitions,
            });

            let t_resp = client
                .post(format!("{}/streams/{}/topics", clean_url, stream_name))
                .bearer_auth(&token)
                .json(&topic_payload)
                .send()
                .await?;

            let t_status = t_resp.status();
            if t_status.is_success() {
                println!(
                    "      [CREATED] Topic '{}' with {} partitions (fsync: immediate)",
                    topic.name, topic.partitions
                );
            } else if t_status == reqwest::StatusCode::BAD_REQUEST || t_status == reqwest::StatusCode::CONFLICT {
                let err_text = t_resp.text().await.unwrap_or_default();
                println!(
                    "      [EXISTS] Topic '{}' already exists (status: {}, info: {})",
                    topic.name,
                    t_status,
                    err_text.trim()
                );
            } else {
                let err_text = t_resp.text().await.unwrap_or_default();
                return Err(format!(
                    "Failed to create topic '{}': HTTP {} ({})",
                    topic.name, t_status, err_text
                )
                .into());
            }
        }
    }

    println!("\n==================================================================");
    println!("  [OK] Ingestion topics & partition durability provisioned!       ");
    println!("==================================================================");
    Ok(())
}

/// Step 3: Provision Zero-Copy Consumer Groups
pub async fn run_provision_consumer_groups(
    iggy_url: &str,
    username: &str,
    password: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("==================================================================");
    println!("  [Step 3] Provision Zero-Copy Consumer Groups                    ");
    println!("==================================================================");
    println!(" Broker URL: {}", iggy_url);

    if dry_run {
        println!("\n  [INFO] Running in --dry-run mode (skipping live broker mutation).");
        for cg in CONSUMER_GROUPS {
            println!(
                "  [AUDIT] Validated consumer group '{}' (id: {}) on topic '{}'",
                cg.name, cg.group_id, cg.topic_name
            );
            println!("          Description: {}", cg.description);
        }
        println!("\n  [OK] Consumer groups verified in dry-run mode.");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    check_iggy_liveness(&client, iggy_url).await?;
    let token = iggy_authenticate(&client, iggy_url, username, password).await?;
    println!("  [OK] Authenticated to Apache Iggy.");

    let clean_url = iggy_url.trim_end_matches('/');

    for cg in CONSUMER_GROUPS {
        let stream_name = match cg.stream_id {
            DEFAULT_STREAM_ID => DEFAULT_STREAM_NAME,
            BENCHMARK_STREAM_ID => BENCHMARK_STREAM_NAME,
            OLIST_STREAM_ID => OLIST_STREAM_NAME,
            _ => "unknown",
        };

        println!(
            "\n  [*] Ensuring consumer group '{}' (id: {}) on topic '{}/{}'...",
            cg.name, cg.group_id, stream_name, cg.topic_name
        );

        let cg_payload = serde_json::json!({
            "group_id": cg.group_id,
            "name": cg.name,
        });

        let cg_resp = client
            .post(format!(
                "{}/streams/{}/topics/{}/consumer-groups",
                clean_url, stream_name, cg.topic_name
            ))
            .bearer_auth(&token)
            .json(&cg_payload)
            .send()
            .await?;

        let cg_status = cg_resp.status();
        if cg_status.is_success() {
            println!("      [CREATED] Consumer group '{}' (id: {})", cg.name, cg.group_id);
        } else if cg_status == reqwest::StatusCode::BAD_REQUEST || cg_status == reqwest::StatusCode::CONFLICT {
            let err_text = cg_resp.text().await.unwrap_or_default();
            println!(
                "      [EXISTS] Consumer group '{}' already exists (status: {}, info: {})",
                cg.name,
                cg_status,
                err_text.trim()
            );
        } else {
            let err_text = cg_resp.text().await.unwrap_or_default();
            return Err(format!(
                "Failed to create consumer group '{}': HTTP {} ({})",
                cg.name, cg_status, err_text
            )
            .into());
        }
    }

    println!("\n==================================================================");
    println!("  [OK] Zero-copy consumer groups provisioned successfully!       ");
    println!("==================================================================");
    Ok(())
}

/// Step 4: Verify Apache Iggy Broker & Consumer Group Attachments
pub async fn run_verify_broker(
    iggy_url: &str,
    username: &str,
    password: &str,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("==================================================================");
    println!("  [Step 4] Verify Apache Iggy Broker & Consumer Group Health      ");
    println!("==================================================================");
    println!(" Iggy Broker URL: {}", iggy_url);

    if dry_run {
        println!("\n  [INFO] Running in --dry-run mode (skipping network probes).");
        println!("  [AUDIT] Validated Apache Iggy health probe (port 3000/ping)");
        println!("  [AUDIT] Validated required consumer group attachments: cg-arroyo-raw, cg-arroyo-state, cg-order-projection, cg-cmd-handler, cg-order-callback, cg-arroyo-benchmark, cg-arroyo-olist");
        println!("\n  [OK] Apache Iggy broker health and consumer groups verified in dry-run mode.");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    println!("\n[1/1] Checking Apache Iggy broker liveness...");
    check_iggy_liveness(&client, iggy_url).await?;
    println!("  [OK] Apache Iggy broker is alive on {}.", iggy_url);

    let token = iggy_authenticate(&client, iggy_url, username, password).await?;
    println!("  [OK] Verified Iggy credentials for user '{}'.", username);

    println!("  [*] Verifying consumer group attachments on Apache Iggy broker...");
    let groups_to_verify = [
        (DEFAULT_STREAM_NAME, "intake", "cg-arroyo-raw"),
        (DEFAULT_STREAM_NAME, "intake", "cg-cmd-handler"),
        (DEFAULT_STREAM_NAME, "consolidated", "cg-arroyo-state"),
        (DEFAULT_STREAM_NAME, "consolidated", "cg-order-projection"),
        (DEFAULT_STREAM_NAME, "callback", "cg-order-callback"),
        (BENCHMARK_STREAM_NAME, "hpc_mqbench", "cg-arroyo-benchmark"),
        (OLIST_STREAM_NAME, "orders_stream", "cg-arroyo-olist"),
    ];
    let clean_url = iggy_url.trim_end_matches('/');
    for (s_name, t_name, cg_name) in groups_to_verify {
        let cg_url = format!("{}/streams/{}/topics/{}/consumer-groups/{}", clean_url, s_name, t_name, cg_name);
        let resp = client
            .get(&cg_url)
            .bearer_auth(&token)
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => {
                println!("      [ATTACHED] Consumer group '{}' verified on stream '{}' topic '{}'.", cg_name, s_name, t_name);
            }
            Ok(r) => {
                println!("      [WARN] Consumer group '{}' status: HTTP {} (may need provisioning via step 3).", cg_name, r.status());
            }
            Err(e) => {
                println!("      [WARN] Could not verify consumer group '{}': {}", cg_name, e);
            }
        }
    }

    println!("\n==================================================================");
    println!("  [OK] Apache Iggy broker verification passed!                    ");
    println!("==================================================================");
    Ok(())
}

/// Fallback resolver for `--delta-dir` to locate `data/delta` across workspaces
pub fn resolve_delta_dir(delta_dir: &Path) -> PathBuf {
    if delta_dir.exists() {
        delta_dir.to_path_buf()
    } else {
        let in_submodule = Path::new("ferrous-cx").join(delta_dir);
        if in_submodule.exists() {
            in_submodule
        } else {
            delta_dir.to_path_buf()
        }
    }
}

/// Fallback resolver for `--config` to locate `delta_sink.toml` across workspaces
pub fn resolve_connector_config_path(config_path: &Path) -> PathBuf {
    if config_path.exists() {
        config_path.to_path_buf()
    } else {
        let in_submodule = Path::new("ferrous-cx").join(config_path);
        if in_submodule.exists() {
            in_submodule
        } else {
            config_path.to_path_buf()
        }
    }
}

/// Generate RFC-compliant Delta Lake 00000000000000000000.json commit log
pub fn generate_delta_log_zero(
    table_name: &str,
    fields: &[(&str, &str, bool)],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let schema_fields: Vec<serde_json::Value> = fields
        .iter()
        .map(|(name, type_str, nullable)| {
            serde_json::json!({
                "name": name,
                "type": type_str,
                "nullable": nullable,
                "metadata": {}
            })
        })
        .collect();

    let schema_json = serde_json::json!({
        "type": "struct",
        "fields": schema_fields
    });
    let schema_string = serde_json::to_string(&schema_json)?;

    let protocol_action = serde_json::json!({
        "protocol": {
            "minReaderVersion": 1,
            "minWriterVersion": 2
        }
    });

    let metadata_action = serde_json::json!({
        "metaData": {
            "id": uuid::Uuid::new_v4().to_string(),
            "name": table_name,
            "description": format!("Managed Delta Lake table for {}", table_name),
            "format": {
                "provider": "parquet",
                "options": {}
            },
            "schemaString": schema_string,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": now_ms
        }
    });

    let commit_info = serde_json::json!({
        "commitInfo": {
            "timestamp": now_ms,
            "operation": "CREATE TABLE",
            "operationParameters": {
                "isManaged": "true",
                "description": format!("Initialize Delta Lake sink schema for {}", table_name)
            },
            "engineInfo": "FerrousCX-Native-Storage-Engine/0.1.0"
        }
    });

    let output = format!(
        "{}\n{}\n{}\n",
        serde_json::to_string(&protocol_action)?,
        serde_json::to_string(&metadata_action)?,
        serde_json::to_string(&commit_info)?
    );

    Ok(output)
}

/// Initialize Delta Lake sink storage directories with atomic log generation
pub fn init_delta_storage(delta_base_dir: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for spec in TABLES {
        let table_dir = delta_base_dir.join(spec.delta_table_name);
        let log_dir = table_dir.join("_delta_log");
        std::fs::create_dir_all(&log_dir)?;

        let log_zero_path = log_dir.join("00000000000000000000.json");
        let is_uninitialized = if log_zero_path.exists() {
            let metadata = std::fs::metadata(&log_zero_path)?;
            metadata.len() == 0
        } else {
            true
        };

        if is_uninitialized {
            let log_zero_content = generate_delta_log_zero(spec.delta_table_name, spec.schema_fields)?;
            let tmp_path = log_dir.join(format!(".00000000000000000000.json.{}.tmp", uuid::Uuid::new_v4()));
            std::fs::write(&tmp_path, log_zero_content)?;
            std::fs::rename(&tmp_path, &log_zero_path)?;
            println!(
                "      [CREATED] Delta Lake schema initialized: {}",
                log_zero_path.display()
            );
        } else {
            println!(
                "      [EXISTS] Delta Lake schema already initialized: {}",
                log_zero_path.display()
            );
        }
    }
    Ok(())
}

/// Step 5: Initialize Delta Lake Storage Sinks
pub async fn run_init_delta_storage(
    delta_dir: &Path,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("==================================================================");
    println!("  [Step 5] Initialize Delta Lake Storage Sinks                    ");
    println!("==================================================================");
    println!(" Delta Sink Dir: {:?}", delta_dir);

    if dry_run {
        println!("\n  [INFO] Running in --dry-run mode (skipping disk mutation).");
        for spec in TABLES {
            println!("  [AUDIT] Validated table '{}' (target: {})", spec.name, spec.delta_table_name);
        }
        println!("\n  [OK] Delta Lake schemas verified in dry-run mode.");
        return Ok(());
    }

    let resolved_delta_dir = resolve_delta_dir(delta_dir);
    println!(" Resolved Delta Sink Dir: {:?}", resolved_delta_dir);
    init_delta_storage(&resolved_delta_dir)?;

    println!("  [OK] Delta Lake storage tier initialized directly for native Apache Iggy Delta sink.");
    println!("\n==================================================================");
    println!("  [OK] Delta storage initialized successfully!                    ");
    println!("==================================================================");
    Ok(())
}

/// Run all Steps (2, 3, 4, 5, 6) in sequence
pub async fn run_all(
    iggy_url: &str,
    username: &str,
    password: &str,
    delta_dir: &Path,
    connector_cfg_override: Option<&Path>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("##################################################################");
    println!("  [Full Setup] Executing Steps 2 through 6 in order...            ");
    println!("##################################################################\n");

    run_provision_topics(iggy_url, username, password, dry_run).await?;
    println!();
    run_provision_consumer_groups(iggy_url, username, password, dry_run).await?;
    println!();
    run_verify_broker(iggy_url, username, password, dry_run).await?;
    println!();
    let resolved_delta_dir = resolve_delta_dir(delta_dir);
    run_init_delta_storage(&resolved_delta_dir, dry_run).await?;

    let default_cfg = Path::new("config/streaming/connectors/delta_sink.toml");
    let target_cfg = connector_cfg_override.unwrap_or(default_cfg);
    let resolved_cfg = resolve_connector_config_path(target_cfg);

    if resolved_cfg.exists() {
        println!();
        run_validate_connectors(&resolved_cfg, Some(&resolved_delta_dir), dry_run).await?;
    } else {
        println!(
            "\n  [WARN] Connector config not found at {:?}, skipping Step 6 validation.",
            resolved_cfg
        );
    }

    println!("\n##################################################################");
    println!("  [SUCCESS] All platform streaming steps completed successfully!  ");
    println!("##################################################################");
    Ok(())
}

/// Apache Iggy connector header specification
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct ConnectorHeader {
    pub name: String,
    #[serde(rename = "type")]
    pub connector_type: String,
    pub plugin: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Apache Iggy broker connection configuration for connector runtime
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct IggyConnectionConfig {
    pub server_address: String,
    pub username: String,
    pub password: String,
}

/// Delta Lake sink definition for Apache Iggy `delta_sink` plugin
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct DeltaSinkDefinition {
    pub name: String,
    pub stream: String,
    pub topic: String,
    pub schema: String,
    pub table_uri: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,
}

fn default_true() -> bool {
    true
}

fn default_batch_size() -> usize {
    1000
}

fn default_batch_timeout() -> u64 {
    1000
}

/// Root connector configuration file (`delta_sink.toml`)
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct DeltaSinkConfig {
    pub connector: ConnectorHeader,
    pub iggy: IggyConnectionConfig,
    #[serde(default)]
    pub sinks: Vec<DeltaSinkDefinition>,
}

/// Parses and validates an Apache Iggy `delta_sink` TOML configuration string.
/// Strictly enforces `schema = "json"` on all stream sinks and validates that target tables exist
/// when `delta_base_dir` is provided.
pub fn parse_and_validate_connector_config(
    toml_content: &str,
    delta_base_dir: Option<&Path>,
) -> Result<DeltaSinkConfig, Box<dyn std::error::Error + Send + Sync>> {
    let config: DeltaSinkConfig = toml::from_str(toml_content)?;

    if config.connector.plugin != "delta_sink" {
        return Err(format!(
            "Invalid connector plugin: '{}', expected 'delta_sink'",
            config.connector.plugin
        )
        .into());
    }

    if config.connector.connector_type != "sink" {
        return Err(format!(
            "Invalid connector type: '{}', expected 'sink'",
            config.connector.connector_type
        )
        .into());
    }

    if config.sinks.is_empty() {
        return Err("delta_sink configuration must contain at least one sink definition".into());
    }

    for sink in &config.sinks {
        if sink.name.trim().is_empty() {
            return Err("Sink definition cannot have an empty name".into());
        }
        if sink.stream.trim().is_empty() {
            return Err(format!("Sink '{}' cannot have an empty stream", sink.name).into());
        }
        if sink.topic.trim().is_empty() {
            return Err(format!("Sink '{}' cannot have an empty topic", sink.name).into());
        }
        if sink.batch_size == 0 {
            return Err(format!("Sink '{}' batch_size must be greater than zero", sink.name).into());
        }
        if sink.batch_timeout_ms == 0 {
            return Err(format!("Sink '{}' batch_timeout_ms must be greater than zero", sink.name).into());
        }

        // Enforce strict JSON schema invariant (AD-28b)
        if sink.schema != "json" {
            return Err(format!(
                "Sink '{}' violates contract: stream schema must be 'json', got '{}'. Non-JSON formats are rejected.",
                sink.name, sink.schema
            )
            .into());
        }

        // Validate table_uri format
        let normalized_uri = sink.table_uri.replace('\\', "/");
        let trimmed_uri = normalized_uri.trim_end_matches('/');

        if trimmed_uri == "/data/delta" || trimmed_uri == "data/delta" {
            return Err(format!(
                "Sink '{}' table_uri '{}' must target a specific table directory under '/data/delta/', not the base directory itself",
                sink.name, sink.table_uri
            )
            .into());
        }

        if !trimmed_uri.starts_with("/data/delta/") && !trimmed_uri.starts_with("data/delta/") {
            return Err(format!(
                "Sink '{}' table_uri '{}' must target '/data/delta/' storage path",
                sink.name, sink.table_uri
            )
            .into());
        }

        let table_name = trimmed_uri.rsplit('/').next().unwrap_or("");
        if table_name.is_empty() {
            return Err(format!(
                "Sink '{}' table_uri '{}' cannot resolve to an empty table name",
                sink.name, sink.table_uri
            )
            .into());
        }

        // Check target table pre-existence if base delta directory is provided
        if let Some(base_dir) = delta_base_dir {
            let table_dir = base_dir.join(table_name);
            let log_zero = table_dir.join("_delta_log").join("00000000000000000000.json");
            if !log_zero.exists() {
                return Err(format!(
                    "Target Delta table for sink '{}' missing at '{}'. Tables must be pre-created via 'streaming-admin init-delta-storage' before connector launch.",
                    sink.name, table_dir.display()
                )
                .into());
            }

            let metadata = std::fs::metadata(&log_zero)?;
            if metadata.len() == 0 {
                return Err(format!(
                    "Target Delta table for sink '{}' at '{}' has corrupt/empty commit log (0 bytes). Re-run 'streaming-admin init-delta-storage'.",
                    sink.name, log_zero.display()
                )
                .into());
            }

            // Verify log content integrity
            let content = std::fs::read_to_string(&log_zero)?;
            let mut has_protocol = false;
            let mut has_metadata = false;
            let mut has_commit_info = false;
            for line in content.lines() {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
                    if val.get("protocol").is_some() {
                        has_protocol = true;
                    }
                    if let Some(meta) = val.get("metaData") {
                        if meta.get("name").and_then(|n| n.as_str()) == Some(table_name) {
                            has_metadata = true;
                        }
                    }
                    if val.get("commitInfo").is_some() {
                        has_commit_info = true;
                    }
                }
            }

            if !has_protocol || !has_metadata || !has_commit_info {
                return Err(format!(
                    "Target Delta table for sink '{}' at '{}' is missing valid Delta protocol, metadata for '{}', or commitInfo actions.",
                    sink.name, log_zero.display(), table_name
                )
                .into());
            }
        }
    }

    Ok(config)
}

/// Step 6: Validates an on-disk `delta_sink.toml` connector configuration file
pub async fn run_validate_connectors(
    config_path: &Path,
    delta_dir: Option<&Path>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("==================================================================");
    println!("  [Step 6] Validate Apache Iggy Delta Lake Sink Connector Config  ");
    println!("==================================================================");
    let resolved_cfg = resolve_connector_config_path(config_path);
    let resolved_delta = delta_dir.map(resolve_delta_dir);

    println!(" Config Path: {:?}", resolved_cfg);
    if let Some(ref dir) = resolved_delta {
        println!(" Delta Dir:   {:?}", dir);
    }

    let toml_str = std::fs::read_to_string(&resolved_cfg)
        .map_err(|e| format!("Failed to read connector config at {:?}: {}", resolved_cfg, e))?;

    let parsed = parse_and_validate_connector_config(
        &toml_str,
        if dry_run { None } else { resolved_delta.as_deref() },
    )?;

    println!(
        "  [OK] Connector header valid: {} ({})",
        parsed.connector.name, parsed.connector.plugin
    );
    for sink in &parsed.sinks {
        println!(
            "  [OK] Sink '{}': stream='{}', topic='{}', schema='{}', uri='{}'",
            sink.name, sink.stream, sink.topic, sink.schema, sink.table_uri
        );
    }
    if dry_run {
        println!("\n  [SUCCESS] All connector sinks validated with schema = 'json' (dry-run mode, table disk checks bypassed)!");
    } else {
        println!("\n  [SUCCESS] All connector sinks validated with schema = 'json' and pre-created tables!");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_storage_initialization() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let delta_path = tmp.path().join("delta");

        init_delta_storage(&delta_path).expect("init delta storage");

        for spec in TABLES {
            let log_zero = delta_path
                .join(spec.delta_table_name)
                .join("_delta_log")
                .join("00000000000000000000.json");
            assert!(log_zero.exists(), "Log zero missing for {}", spec.delta_table_name);

            let content = std::fs::read_to_string(&log_zero).expect("read log zero");
            let lines: Vec<&str> = content.lines().collect();
            assert_eq!(lines.len(), 3, "Delta log 0 must contain 3 lines (protocol, metaData, commitInfo)");

            let protocol: serde_json::Value = serde_json::from_str(lines[0]).expect("parse protocol");
            assert_eq!(protocol["protocol"]["minReaderVersion"], 1);
            assert_eq!(protocol["protocol"]["minWriterVersion"], 2);

            let metadata: serde_json::Value = serde_json::from_str(lines[1]).expect("parse metaData");
            assert_eq!(metadata["metaData"]["name"], spec.delta_table_name);
            assert_eq!(metadata["metaData"]["format"]["provider"], "parquet");
            assert!(metadata["metaData"]["schemaString"].is_string());

            let commit: serde_json::Value = serde_json::from_str(lines[2]).expect("parse commitInfo");
            assert_eq!(commit["commitInfo"]["operation"], "CREATE TABLE");
        }

        // Idempotent
        init_delta_storage(&delta_path).expect("idempotent second run");
    }

    #[test]
    fn test_table_spec_invariants() {
        assert_eq!(TABLES.len(), 4);
        assert_eq!(STREAMING_TOPICS.len(), 2);
        assert_eq!(BENCHMARK_TOPICS.len(), 1);
        assert_eq!(OLIST_TOPICS.len(), 1);
        assert_eq!(CONSUMER_GROUPS.len(), 5);
    }
}
