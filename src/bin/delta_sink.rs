//! Pure-Rust Apache Iggy → Delta Lake 2PC Sink Connector Daemon (`delta-sink`).
//!
//! Reads `delta_sink.toml`, initializes Delta Lake tables (`_delta_log/00000000000000000000.json`),
//! polls every configured Iggy stream/topic across all partitions, writes NDJSON data parts,
//! atomically commits `_delta_log/{version:020}.json`, and exposes a lightweight HTTP `/health`
//! and `/stats` endpoint (default port `8095`) for real-time E2E benchmark qualification.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::Parser;
use ferrous_cx::admin::{
    init_delta_storage, parse_and_validate_connector_config, DeltaSinkDefinition,
};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[command(
    name = "delta-sink",
    about = "Native Apache Iggy to Delta Lake 2PC Sink Connector Daemon"
)]
struct Args {
    /// Path to delta_sink.toml connector configuration
    #[arg(
        long,
        env = "DELTA_SINK_CONFIG",
        default_value = "/etc/iggy/connectors/delta_sink.toml"
    )]
    config: PathBuf,

    /// Base Delta Lake storage directory
    #[arg(long, env = "DELTA_DATA_DIR", default_value = "/data/delta")]
    delta_dir: PathBuf,

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
    #[arg(long, env = "DELTA_SINK_PORT", default_value_t = 8095)]
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

#[derive(Serialize, Deserialize, Default)]
struct OffsetCheckpoint {
    partitions: HashMap<u32, u64>,
    next_delta_version: u64,
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn load_checkpoint(table_dir: &Path) -> OffsetCheckpoint {
    let cp_path = table_dir.join("_delta_log").join("_iggy_offsets.json");
    if let Ok(raw) = fs::read_to_string(&cp_path) {
        if let Ok(cp) = serde_json::from_str::<OffsetCheckpoint>(&raw) {
            return cp;
        }
    }
    // Discover highest existing _delta_log/*.json version
    let mut max_ver = 0u64;
    if let Ok(entries) = fs::read_dir(table_dir.join("_delta_log")) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.len() == 25 && name.ends_with(".json") {
                if let Ok(v) = name[..20].parse::<u64>() {
                    max_ver = max_ver.max(v);
                }
            }
        }
    }
    OffsetCheckpoint {
        partitions: HashMap::new(),
        next_delta_version: max_ver + 1,
    }
}

fn save_checkpoint(table_dir: &Path, cp: &OffsetCheckpoint) -> Result<(), String> {
    let cp_path = table_dir.join("_delta_log").join("_iggy_offsets.json");
    let tmp_path = table_dir.join("_delta_log").join("_iggy_offsets.json.tmp");
    let data = serde_json::to_vec(cp).map_err(|e| format!("serialize checkpoint: {e}"))?;
    fs::write(&tmp_path, data).map_err(|e| format!("write checkpoint tmp: {e}"))?;
    fs::rename(&tmp_path, &cp_path).map_err(|e| format!("rename checkpoint: {e}"))?;
    Ok(())
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
        "{}/streams/{stream}/topics/{topic}/messages?consumer_id=77&partition_id={partition_id}&kind=offset&value={offset}&count={count}&auto_commit=false",
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

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(Vec::new());
    }
    if !resp.status().is_success() {
        return Ok(Vec::new());
    }

    let parsed: PollResponse = resp
        .json()
        .await
        .map_err(|e| format!("parse poll response: {e}"))?;
    Ok(parsed.messages)
}

async fn run_sink_loop(sink: DeltaSinkDefinition, auth: Arc<SharedAuth>, delta_dir: PathBuf) {
    let table_name = Path::new(&sink.table_uri)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(&sink.name)
        .to_string();
    let table_dir = delta_dir.join(&table_name);
    let _ = fs::create_dir_all(table_dir.join("_delta_log"));

    let mut cp = load_checkpoint(&table_dir);
    let start_from_latest = std::env::var("DELTA_SINK_START_OFFSET")
        .map(|v| !v.eq_ignore_ascii_case("earliest"))
        .unwrap_or(true);
    if cp.partitions.is_empty() && start_from_latest {
        let info = get_topic_info(&auth, &sink.stream, &sink.topic).await;
        for part in &info.partitions {
            cp.partitions.insert(part.id, part.messages_count);
        }
        let _ = save_checkpoint(&table_dir, &cp);
    }
    let poll_batch = sink.batch_size.clamp(100, 5000) as u32;
    let idle_sleep = Duration::from_millis(sink.batch_timeout_ms.clamp(50, 500));

    eprintln!(
        "[delta-sink] Started sink '{}' ({}/{}) -> {} (next_ver={}, offsets={:?})",
        sink.name,
        sink.stream,
        sink.topic,
        table_dir.display(),
        cp.next_delta_version,
        cp.partitions
    );

    loop {
        let partitions = get_topic_info(&auth, &sink.stream, &sink.topic)
            .await
            .partitions_count
            .max(1);
        let mut collected: Vec<(u32, u64, u64, Vec<u8>)> = Vec::new();

        for p in 0..partitions {
            let next_offset = *cp.partitions.get(&p).unwrap_or(&0);
            match poll_partition(&auth, &sink.stream, &sink.topic, p, next_offset, poll_batch).await
            {
                Ok(msgs) if !msgs.is_empty() => {
                    let mut highest = next_offset;
                    for m in msgs {
                        if m.header.offset >= next_offset {
                            if let Ok(raw) = BASE64.decode(&m.payload) {
                                collected.push((p, m.header.offset, m.header.timestamp, raw));
                            }
                            highest = highest.max(m.header.offset + 1);
                        }
                    }
                    cp.partitions.insert(p, highest);
                }
                _ => {}
            }
        }

        if collected.is_empty() {
            tokio::time::sleep(idle_sleep).await;
            continue;
        }

        let commit_ts_us = now_us();
        let commit_ts_ms = now_ms();
        let version = cp.next_delta_version;
        let part_name = format!("part-{version:020}.ndjson");
        let part_path = table_dir.join(&part_name);

        let write_res: Result<u64, String> = (|| {
            let f = File::create(&part_path)
                .map_err(|e| format!("create part {}: {e}", part_path.display()))?;
            let mut w = BufWriter::new(f);
            for (part_id, offset, iggy_ts_us, raw) in &collected {
                if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(raw) {
                    if let Some(obj) = val.as_object_mut() {
                        obj.insert("_iggy_partition".into(), serde_json::json!(part_id));
                        obj.insert("_iggy_offset".into(), serde_json::json!(offset));
                        obj.insert("_iggy_timestamp_us".into(), serde_json::json!(iggy_ts_us));
                        obj.insert("_delta_commit_ts_us".into(), serde_json::json!(commit_ts_us));
                    }
                    if let Ok(line) = serde_json::to_string(&val) {
                        let _ = writeln!(w, "{line}");
                    }
                }
            }
            w.flush().map_err(|e| format!("flush part: {e}"))?;
            let size = fs::metadata(&part_path).map(|m| m.len()).unwrap_or(0);
            Ok(size)
        })();

        match write_res {
            Ok(size) => {
                let commit_path = table_dir
                    .join("_delta_log")
                    .join(format!("{version:020}.json"));
                let add_action = serde_json::json!({
                    "add": {
                        "path": part_name,
                        "partitionValues": {},
                        "size": size,
                        "modificationTime": commit_ts_ms,
                        "dataChange": true,
                        "stats": serde_json::json!({ "numRecords": collected.len() }).to_string()
                    }
                });
                let commit_info = serde_json::json!({
                    "commitInfo": {
                        "timestamp": commit_ts_ms,
                        "operation": "WRITE",
                        "operationParameters": {
                            "mode": "Append",
                            "source": format!("{}/{}", sink.stream, sink.topic)
                        },
                        "numRecords": collected.len()
                    }
                });
                let log_body = format!("{add_action}\n{commit_info}\n");
                if let Err(e) = fs::write(&commit_path, log_body) {
                    eprintln!("[delta-sink] Failed to write commit {version}: {e}");
                } else {
                    cp.next_delta_version = version + 1;
                    let _ = save_checkpoint(&table_dir, &cp);
                }
            }
            Err(e) => {
                eprintln!("[delta-sink] Failed to write batch for {}: {e}", sink.name);
            }
        }
    }
}

fn compute_run_stats(delta_dir: &Path, table: &str, run_id: &str) -> RunStatsResponse {
    let table_dir = delta_dir.join(table);
    let mut delta_stored = 0usize;
    let mut distinct_ids: HashSet<String> = HashSet::new();
    let mut iggy_commit_us: Vec<f64> = Vec::new();
    let mut end_to_end_us: Vec<f64> = Vec::new();

    if let Ok(entries) = fs::read_dir(&table_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
                continue;
            }
            if let Ok(file) = File::open(&path) {
                for line in BufReader::new(file).lines().map_while(Result::ok) {
                    if !run_id.is_empty() && !line.contains(run_id) {
                        continue;
                    }
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) {
                        if !run_id.is_empty()
                            && val.get("run_id").and_then(|v| v.as_str()) != Some(run_id)
                        {
                            continue;
                        }
                        delta_stored += 1;
                        if let Some(rec_id) = val.get("record_id").and_then(|v| v.as_str()) {
                            distinct_ids.insert(rec_id.to_string());
                        }
                        let client_ts = val.get("client_timestamp_us").and_then(|v| v.as_u64());
                        let iggy_ts = val.get("_iggy_timestamp_us").and_then(|v| v.as_u64());
                        let commit_ts = val.get("_delta_commit_ts_us").and_then(|v| v.as_u64());

                        // Sample up to 20,000 latencies to keep response payloads fast
                        if iggy_commit_us.len() < 20_000 {
                            if let (Some(c), Some(i)) = (client_ts, iggy_ts) {
                                if i >= c {
                                    iggy_commit_us.push((i - c) as f64);
                                }
                            }
                            if let (Some(c), Some(d)) = (client_ts, commit_ts) {
                                if d >= c {
                                    end_to_end_us.push((d - c) as f64);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    RunStatsResponse {
        table: table.to_string(),
        run_id: run_id.to_string(),
        delta_stored,
        distinct_delta_records: distinct_ids.len(),
        iggy_commit_us,
        end_to_end_us,
    }
}

async fn run_http_server(port: u16, delta_dir: PathBuf) {
    let listener = match TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[delta-sink] Failed to bind HTTP status port {port}: {e}");
            return;
        }
    };
    eprintln!("[delta-sink] HTTP status/stats API listening on 0.0.0.0:{port}");

    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let dir = delta_dir.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let n = socket.read(&mut buf).await.unwrap_or(0);
            if n == 0 {
                return;
            }
            let req = String::from_utf8_lossy(&buf[..n]);
            let first = req.lines().next().unwrap_or("");
            let path = first.split_whitespace().nth(1).unwrap_or("/");

            let (status, body) = if path == "/health" {
                ("200 OK", r#"{"status":"ok"}"#.to_string())
            } else if path.starts_with("/stats") {
                let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
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
                let stats = tokio::task::spawn_blocking(move || {
                    compute_run_stats(&dir, &table, &run_id)
                })
                .await
                .unwrap_or(RunStatsResponse {
                    table: String::new(),
                    run_id: String::new(),
                    delta_stored: 0,
                    distinct_delta_records: 0,
                    iggy_commit_us: Vec::new(),
                    end_to_end_us: Vec::new(),
                });
                (
                    "200 OK",
                    serde_json::to_string(&stats).unwrap_or_else(|_| "{}".into()),
                )
            } else {
                ("404 Not Found", r#"{"error":"not_found"}"#.to_string())
            };

            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.healthcheck {
        let hb = Path::new("/tmp/delta_sink_heartbeat");
        match fs::metadata(hb).and_then(|m| m.modified()) {
            Ok(modified) => match modified.elapsed() {
                Ok(e) if e <= Duration::from_secs(60) => std::process::exit(0),
                _ => {
                    eprintln!("Healthcheck failed: stale heartbeat");
                    std::process::exit(1);
                }
            },
            Err(e) => {
                eprintln!("Healthcheck failed: {e}");
                std::process::exit(1);
            }
        }
    }

    // Initialize Delta Lake table schemas idempotently
    if let Err(e) = init_delta_storage(&args.delta_dir) {
        eprintln!("[delta-sink] Failed to initialize Delta Lake storage: {e}");
        std::process::exit(1);
    }

    let raw_cfg = match fs::read_to_string(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "[delta-sink] Failed to read connector config '{}': {e}",
                args.config.display()
            );
            std::process::exit(1);
        }
    };

    let cfg = match parse_and_validate_connector_config(&raw_cfg, Some(&args.delta_dir)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[delta-sink] Invalid connector config: {e}");
            std::process::exit(1);
        }
    };

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build reqwest client");

    let token = loop {
        match SharedAuth::login(&client, &args.iggy_url, &args.iggy_username, &args.iggy_password)
            .await
        {
            Ok(t) => break t,
            Err(e) => {
                eprintln!("[delta-sink] Waiting for Apache Iggy login ({e})...");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    };

    let auth = Arc::new(SharedAuth {
        client,
        base_url: args.iggy_url.clone(),
        username: args.iggy_username.clone(),
        password: args.iggy_password.clone(),
        token: RwLock::new(token),
    });

    // Heartbeat writer for AD-24 healthcheck
    let _ = fs::write("/tmp/delta_sink_heartbeat", b"healthy");
    tokio::spawn(async {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let _ = fs::write("/tmp/delta_sink_heartbeat", b"healthy");
        }
    });

    // HTTP /health & /stats server
    let http_dir = args.delta_dir.clone();
    let port = args.port;
    tokio::spawn(async move {
        run_http_server(port, http_dir).await;
    });

    // Spawn 1 poller/committer task per configured sink in delta_sink.toml
    for sink in cfg.sinks {
        let a = auth.clone();
        let d = args.delta_dir.clone();
        tokio::spawn(async move {
            run_sink_loop(sink, a, d).await;
        });
    }

    // Wait for Ctrl+C / SIGTERM
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("[delta-sink] Shutting down cleanly.");
}
