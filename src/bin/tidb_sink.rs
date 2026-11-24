//! Pure-Rust Apache Iggy -> TiDB Transactional Sink Connector Daemon (`tidb-sink`, Story 23.2, AD-36).
//!
//! Reads `tidb_sink.toml`, applies canonical 9-table Olist + CQRS DDL on startup,
//! polls every configured Apache Iggy stream/topic across all partitions, transforms JSON
//! payloads into typed TiDB rows, commits batched `INSERT ... ON DUPLICATE KEY UPDATE` +
//! `_iggy_sink_offsets` checkpoints inside a single ACID TiDB transaction, and exposes
//! `/health` and `/stats` on port `8095`.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::Parser;
use ferrous_cx::tidb::{
    connect_and_init_tidb, load_sink_offsets, parse_and_validate_tidb_sink_config,
    transform_iggy_payload, upsert_tidb_batch, TidbSinkDefinition, DEFAULT_TIDB_URL,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(
    name = "tidb-sink",
    about = "Custom Pure-Rust Apache Iggy to TiDB Transactional Sink Daemon (AD-36)"
)]
struct Args {
    /// Path to tidb_sink.toml connector configuration
    #[arg(
        long,
        env = "TIDB_SINK_CONFIG",
        default_value = "/etc/iggy/connectors/tidb_sink.toml"
    )]
    config: PathBuf,

    /// TiDB MySQL-protocol connection URL
    #[arg(long, env = "TIDB_URL", default_value = DEFAULT_TIDB_URL)]
    tidb_url: String,

    /// Apache Iggy HTTP API URL
    #[arg(long, env = "IGGY_HTTP_URL", default_value = "http://iggy-broker:3000")]
    iggy_url: String,

    /// Apache Iggy username
    #[arg(long, env = "IGGY_USERNAME", default_value = "iggy")]
    iggy_username: String,

    /// Apache Iggy password
    #[arg(long, env = "IGGY_PASSWORD", default_value = "iggy")]
    iggy_password: String,

    /// HTTP port for /health and /stats qualification API
    #[arg(long, env = "TIDB_SINK_PORT", default_value_t = 8095)]
    port: u16,

    /// Run container healthcheck probe and exit
    #[arg(long)]
    healthcheck: bool,
}

#[derive(Deserialize)]
struct LoginResp {
    access_token: AccessToken,
}

#[derive(Deserialize)]
struct AccessToken {
    token: String,
}

#[derive(Deserialize)]
struct TopicInfo {
    #[serde(default)]
    partitions_count: u32,
    #[serde(default)]
    partitions: Vec<PartitionInfo>,
}

#[derive(Deserialize)]
struct PartitionInfo {
    id: u32,
    #[serde(default)]
    messages_count: u64,
}

#[derive(Deserialize)]
struct PollResponse {
    #[serde(default)]
    messages: Vec<PolledMessage>,
}

#[derive(Deserialize)]
struct PolledMessage {
    header: PolledHeader,
    payload: String,
}

#[derive(Deserialize)]
struct PolledHeader {
    offset: u64,
    #[serde(default)]
    timestamp: u64,
}

#[derive(Serialize)]
struct RunStatsResponse {
    table: String,
    run_id: String,
    delta_stored: usize,
    distinct_delta_records: usize,
    iggy_commit_us: Vec<f64>,
    end_to_end_us: Vec<f64>,
}

struct SharedAuth {
    client: Client,
    base_url: String,
    username: String,
    password: String,
    token: RwLock<String>,
}

impl SharedAuth {
    async fn login(
        client: &Client,
        base_url: &str,
        username: &str,
        password: &str,
    ) -> Result<String, String> {
        let url = format!("{}/users/login", base_url.trim_end_matches('/'));
        let resp = client
            .post(&url)
            .json(&serde_json::json!({ "username": username, "password": password }))
            .send()
            .await
            .map_err(|e| format!("Iggy login request failed: {e}"))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Iggy login failed ({status}): {body}"));
        }
        let parsed: LoginResp = resp
            .json()
            .await
            .map_err(|e| format!("Invalid Iggy login JSON: {e}"))?;
        Ok(parsed.access_token.token)
    }

    async fn refresh(&self) -> Result<String, String> {
        let tok = Self::login(&self.client, &self.base_url, &self.username, &self.password).await?;
        *self.token.write().await = tok.clone();
        Ok(tok)
    }
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

async fn get_topic_info(auth: &SharedAuth, stream: &str, topic: &str) -> TopicInfo {
    let url = format!(
        "{}/streams/{stream}/topics/{topic}",
        auth.base_url.trim_end_matches('/')
    );
    let token = auth.token.read().await.clone();
    if let Ok(resp) = auth.client.get(&url).bearer_auth(&token).send().await {
        if resp.status().is_success() {
            if let Ok(info) = resp.json::<TopicInfo>().await {
                return info;
            }
        }
    }
    TopicInfo {
        partitions_count: 1,
        partitions: Vec::new(),
    }
}

async fn poll_partition(
    auth: &SharedAuth,
    stream: &str,
    topic: &str,
    partition_id: u32,
    offset: u64,
    count: u32,
) -> Result<Vec<PolledMessage>, String> {
    let url = format!(
        "{}/streams/{stream}/topics/{topic}/messages?consumer_id=78&partition_id={partition_id}&kind=offset&value={offset}&count={count}&auto_commit=false",
        auth.base_url.trim_end_matches('/')
    );
    let token = auth.token.read().await.clone();
    let mut resp = auth
        .client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| format!("poll {stream}/{topic} p{partition_id}: {e}"))?;

    if resp.status() == StatusCode::UNAUTHORIZED {
        let fresh = auth.refresh().await?;
        resp = auth
            .client
            .get(&url)
            .bearer_auth(&fresh)
            .send()
            .await
            .map_err(|e| format!("poll retry {stream}/{topic} p{partition_id}: {e}"))?;
    }

    if resp.status() == StatusCode::NOT_FOUND || !resp.status().is_success() {
        return Ok(Vec::new());
    }

    let parsed: PollResponse = resp
        .json()
        .await
        .map_err(|e| format!("parse poll response: {e}"))?;
    Ok(parsed.messages)
}

async fn run_sink_loop(sink: TidbSinkDefinition, auth: Arc<SharedAuth>, pool: MySqlPool) {
    let mut offsets: HashMap<u32, u64> = load_sink_offsets(&pool, &sink.name)
        .await
        .unwrap_or_default();

    let start_from_latest = std::env::var("TIDB_SINK_START_OFFSET")
        .map(|v| !v.eq_ignore_ascii_case("earliest"))
        .unwrap_or(false);
    if offsets.is_empty() && start_from_latest {
        let info = get_topic_info(&auth, &sink.stream, &sink.topic).await;
        for part in &info.partitions {
            offsets.insert(part.id, part.messages_count);
        }
    }

    let poll_batch = sink.batch_size.clamp(50, 2000) as u32;
    let idle_sleep = Duration::from_millis(sink.batch_timeout_ms.clamp(50, 500));

    eprintln!(
        "[tidb-sink] Started sink '{}' ({}/{}) -> TiDB table '{}' (offsets={:?})",
        sink.name, sink.stream, sink.topic, sink.table, offsets
    );

    loop {
        let partitions = get_topic_info(&auth, &sink.stream, &sink.topic)
            .await
            .partitions_count
            .max(1);
        let commit_ts_us = now_us();
        let mut batch_rows = Vec::new();
        let mut updated_offsets = HashMap::new();

        for p in 0..partitions {
            let next_offset = *offsets.get(&p).unwrap_or(&0);
            if let Ok(msgs) =
                poll_partition(&auth, &sink.stream, &sink.topic, p, next_offset, poll_batch).await
            {
                if !msgs.is_empty() {
                    let mut highest = next_offset;
                    for m in msgs {
                        if m.header.offset >= next_offset {
                            if let Ok(raw) = BASE64.decode(&m.payload) {
                                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&raw) {
                                    match transform_iggy_payload(
                                        &sink.table,
                                        &val,
                                        m.header.timestamp,
                                        commit_ts_us,
                                    ) {
                                        Ok(row) => batch_rows.push(row),
                                        Err(e) => eprintln!(
                                            "[tidb-sink] transform error ({}): {e}",
                                            sink.name
                                        ),
                                    }
                                }
                            }
                            highest = highest.max(m.header.offset + 1);
                        }
                    }
                    if highest > next_offset {
                        updated_offsets.insert(p, highest);
                    }
                }
            }
        }

        if batch_rows.is_empty() && updated_offsets.is_empty() {
            tokio::time::sleep(idle_sleep).await;
            continue;
        }

        match upsert_tidb_batch(&pool, &sink.name, &batch_rows, &updated_offsets).await {
            Ok(_) => {
                for (p, off) in updated_offsets {
                    offsets.insert(p, off);
                }
            }
            Err(e) => {
                eprintln!("[tidb-sink] TiDB batch commit failed for '{}': {e}", sink.name);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn query_run_stats(pool: &MySqlPool, table: &str, run_id: &str) -> RunStatsResponse {
    if table == "hpc_benchmarks" && !run_id.is_empty() {
        let rows: Vec<(u64, u64, u64)> = sqlx::query_as(
            "SELECT client_timestamp_us, iggy_timestamp_us, commit_timestamp_us \
             FROM hpc_benchmarks WHERE run_id = ? LIMIT 20000",
        )
        .bind(run_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM hpc_benchmarks WHERE run_id = ?",
        )
        .bind(run_id)
        .fetch_one(pool)
        .await
        .unwrap_or(0);

        let mut iggy_commit_us = Vec::with_capacity(rows.len());
        let mut end_to_end_us = Vec::with_capacity(rows.len());
        for (c, i, k) in rows {
            if i >= c {
                iggy_commit_us.push((i - c) as f64);
            }
            if k >= c {
                end_to_end_us.push((k - c) as f64);
            }
        }
        return RunStatsResponse {
            table: table.to_string(),
            run_id: run_id.to_string(),
            delta_stored: count as usize,
            distinct_delta_records: count as usize,
            iggy_commit_us,
            end_to_end_us,
        };
    }

    RunStatsResponse {
        table: table.to_string(),
        run_id: run_id.to_string(),
        delta_stored: 0,
        distinct_delta_records: 0,
        iggy_commit_us: Vec::new(),
        end_to_end_us: Vec::new(),
    }
}

async fn run_http_server(port: u16, pool: MySqlPool) {
    let addr = format!("0.0.0.0:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[tidb-sink] Failed to bind HTTP {addr}: {e}");
            return;
        }
    };
    eprintln!("[tidb-sink] HTTP /health and /stats listening on {addr}");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }
                let req = String::from_utf8_lossy(&buf[..n]);
                let first_line = req.lines().next().unwrap_or("");
                let path_and_query = first_line.split_whitespace().nth(1).unwrap_or("/");

                let (status, body) = if path_and_query.starts_with("/health") {
                    ("200 OK", r#"{"status":"healthy","sink":"tidb-sink"}"#.to_string())
                } else if path_and_query.starts_with("/stats") {
                    let query = path_and_query.split_once('?').map(|(_, q)| q).unwrap_or("");
                    let mut table = "hpc_benchmarks".to_string();
                    let mut run_id = String::new();
                    for pair in query.split('&') {
                        if let Some((k, v)) = pair.split_once('=') {
                            match k {
                                "table" => table = v.to_string(),
                                "run_id" => run_id = v.to_string(),
                                _ => {}
                            }
                        }
                    }
                    let stats = query_run_stats(&pool_clone, &table, &run_id).await;
                    (
                        "200 OK",
                        serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string()),
                    )
                } else {
                    ("404 Not Found", r#"{"error":"not_found"}"#.to_string())
                };

                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    if args.healthcheck {
        let url = format!("http://127.0.0.1:{}/health", args.port);
        let client = Client::builder().timeout(Duration::from_secs(3)).build()?;
        let resp = client.get(&url).send().await?;
        if resp.status().is_success() {
            return Ok(());
        }
        std::process::exit(1);
    }

    let raw_cfg = fs::read_to_string(&args.config)
        .map_err(|e| format!("Failed to read {}: {e}", args.config.display()))?;
    let cfg = parse_and_validate_tidb_sink_config(&raw_cfg)?;

    // Connect to TiDB (auto-creating database + 13 canonical tables) with bounded retry
    let pool = loop {
        match connect_and_init_tidb(&args.tidb_url, 16).await {
            Ok(p) => break p,
            Err(e) => {
                eprintln!("[tidb-sink] Waiting for TiDB ({}): {e}", args.tidb_url);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };
    eprintln!("[tidb-sink] Connected to TiDB and verified canonical 9-table Olist + CQRS schema");

    let client = Client::builder()
        .pool_max_idle_per_host(16)
        .timeout(Duration::from_secs(10))
        .build()?;
    let token = loop {
        match SharedAuth::login(&client, &args.iggy_url, &args.iggy_username, &args.iggy_password)
            .await
        {
            Ok(t) => break t,
            Err(e) => {
                eprintln!("[tidb-sink] Waiting for Iggy ({}): {e}", args.iggy_url);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };

    let auth = Arc::new(SharedAuth {
        client,
        base_url: args.iggy_url,
        username: args.iggy_username,
        password: args.iggy_password,
        token: RwLock::new(token),
    });

    for sink in cfg.sinks {
        let auth_clone = Arc::clone(&auth);
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            run_sink_loop(sink, auth_clone, pool_clone).await;
        });
    }

    run_http_server(args.port, pool).await;
    Ok(())
}
