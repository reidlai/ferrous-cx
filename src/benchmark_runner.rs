//! Pure-Rust HPC-MQBench qualification-first benchmark runner and replay profile loader (Story 15.1, Story 16.14, FR-32, FR-33, SM-8).
//!
//! Accounts for every record across the live 3-stage FerrousCX ingestion pipeline:
//!   1. Client Replayer Ingest (`stream-replay-rs` / Sent)
//!   2. Apache Iggy Broker WAL (`--iggy-url`, default `http://127.0.0.1:3000`, destination `benchmark/hpc_mqbench`)
//!   3. Delta Lake / `ferrous-api` DataFusion (`--query-url`, default `http://127.0.0.1:8080`)

use crate::{generate_workload_stream, WorkloadRecord};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

pub const DEFAULT_SINK_TYPE: &str = "iggy";
pub const DEFAULT_SINK_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_IGGY_HTTP_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_QUERY_SERVICE_URL: &str = "http://127.0.0.1:8080";
pub const DEFAULT_DELTA_SINK_URL: &str = "http://127.0.0.1:8095";
pub const DEFAULT_BENCHMARK_TOPIC: &str = "benchmark/hpc_mqbench";
pub const DEFAULT_REPLAY_PROFILE_PATH: &str = "benchmark/replay-profile.toml";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkEndpoints {
    pub sink: String,
    pub sink_url: String,
    pub iggy_url: String,
    pub query_url: String,
}

impl Default for BenchmarkEndpoints {
    fn default() -> Self {
        Self {
            sink: DEFAULT_SINK_TYPE.to_string(),
            sink_url: DEFAULT_SINK_URL.to_string(),
            iggy_url: DEFAULT_IGGY_HTTP_URL.to_string(),
            query_url: DEFAULT_QUERY_SERVICE_URL.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub run_id: String,
    /// Target ingestion rate in msg/sec (`0` = unthrottled `--speed max`).
    pub rate_msg_sec: u64,
    pub payload_size_bytes: usize,
    pub total_records: u64,
    pub partitions: u32,
    pub topic: String,
    pub simulate_loss_rate: f64,
    pub simulate_dup_rate: f64,
    pub input_file_path: Option<PathBuf>,
    pub config_path: PathBuf,
    pub offline: bool,
    pub endpoints: BenchmarkEndpoints,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        let short_id = &uuid::Uuid::new_v4().simple().to_string()[..12];
        Self {
            run_id: format!("hpc-{short_id}"),
            rate_msg_sec: 25,
            payload_size_bytes: 1024,
            total_records: 1000,
            partitions: 4,
            topic: DEFAULT_BENCHMARK_TOPIC.to_string(),
            simulate_loss_rate: 0.0,
            simulate_dup_rate: 0.0,
            input_file_path: None,
            config_path: PathBuf::from(DEFAULT_REPLAY_PROFILE_PATH),
            offline: true,
            endpoints: BenchmarkEndpoints::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub min: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadSummary {
    pub rate_msg_sec: u64,
    pub payload_size_bytes: usize,
    pub total_records: u64,
    pub partitions: u32,
    pub elapsed_seconds: f64,
    pub achieved_tps: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageAccounting {
    pub sent_records: usize,
    pub iggy_stored: usize,
    pub delta_stored: usize,
    pub distinct_delta_records: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StageLatencies {
    pub iggy_commit: LatencyPercentiles,
    pub end_to_end: LatencyPercentiles,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub run_id: String,
    pub topic: String,
    pub mode: String,
    pub status: String,
    pub disqualification_reason: Option<String>,
    pub endpoints: BenchmarkEndpoints,
    pub replayer_command: String,
    pub workload: WorkloadSummary,
    pub accounting: StageAccounting,
    pub latencies_us: StageLatencies,
}

/// Round a floating-point value to 2 decimal places.
fn round2(val: f64) -> f64 {
    (val * 100.0).round() / 100.0
}

/// Calculate min, p50, p90, p95, p99, max percentiles in microseconds.
pub fn calculate_percentiles(values: &[f64]) -> LatencyPercentiles {
    if values.is_empty() {
        return LatencyPercentiles {
            min: 0.0,
            p50: 0.0,
            p90: 0.0,
            p95: 0.0,
            p99: 0.0,
            max: 0.0,
        };
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();

    let pct = |p: f64| -> f64 {
        let k = (n.saturating_sub(1) as f64) * (p / 100.0);
        let f = k.floor() as usize;
        let c = k.ceil() as usize;
        if f == c {
            sorted[f]
        } else {
            let d0 = sorted[f] * (c as f64 - k);
            let d1 = sorted[c] * (k - f as f64);
            d0 + d1
        }
    };

    LatencyPercentiles {
        min: round2(sorted[0]),
        p50: round2(pct(50.0)),
        p90: round2(pct(90.0)),
        p95: round2(pct(95.0)),
        p99: round2(pct(99.0)),
        max: round2(sorted[n - 1]),
    }
}

/// Normalize `<stream>.<topic>` or `<stream>/<topic>` into `(stream, topic)`.
pub fn parse_iggy_destination(topic: &str) -> (String, String) {
    let trimmed = topic.trim();
    if let Some((s, t)) = trimmed.split_once('/') {
        return (s.trim().to_string(), t.trim().to_string());
    }
    if let Some((s, t)) = trimmed.split_once('.') {
        return (s.trim().to_string(), t.trim().to_string());
    }
    ("default".to_string(), trimmed.to_string())
}

/// Build the canonical `stream-replay-rs` invocation string for the configured endpoints and dataset.
pub fn build_replayer_command(config: &BenchmarkConfig) -> String {
    let config_display = config.config_path.to_string_lossy().replace('\\', "/");
    let input_flag = match &config.input_file_path {
        Some(p) => format!(" --input-file-path {}", p.to_string_lossy().replace('\\', "/")),
        None => String::new(),
    };
    let pacing = if config.rate_msg_sec == 0 {
        "--speed max".to_string()
    } else {
        format!("--rate {}", config.rate_msg_sec)
    };
    format!(
        "stream-replay-rs --config {}{} --sink {} --sink-url {} --topic {} {} --no-loop",
        config_display,
        input_flag,
        config.endpoints.sink,
        config.endpoints.sink_url,
        config.topic,
        pacing
    )
}

/// Load pre-generated NDJSON `WorkloadRecord` lines from disk.
pub fn load_workload_jsonl(path: &Path) -> Result<Vec<WorkloadRecord>, String> {
    if !path.is_file() {
        return Err(format!(
            "Workload input file not found: '{}'",
            path.display()
        ));
    }
    let file = File::open(path)
        .map_err(|e| format!("Failed to open workload file '{}': {e}", path.display()))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for (idx, line_res) in reader.lines().enumerate() {
        let line = line_res.map_err(|e| {
            format!(
                "Failed to read line {} of '{}': {e}",
                idx + 1,
                path.display()
            )
        })?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let rec: WorkloadRecord = serde_json::from_str(trimmed).map_err(|e| {
            format!(
                "Malformed WorkloadRecord JSON on line {} of '{}': {e}",
                idx + 1,
                path.display()
            )
        })?;
        records.push(rec);
    }
    if records.is_empty() {
        return Err(format!(
            "Workload input file '{}' contained 0 records",
            path.display()
        ));
    }
    Ok(records)
}

fn check_tcp_endpoint(label: &str, raw_addr: &str) -> Result<(), String> {
    let clean = raw_addr
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("nats://")
        .split('/')
        .next()
        .unwrap_or("");
    let addrs: Vec<SocketAddr> = clean
        .to_socket_addrs()
        .map_err(|e| format!("Cannot resolve {label} endpoint '{raw_addr}': {e}"))?
        .collect();
    if addrs.is_empty() {
        return Err(format!(
            "No socket addresses resolved for {label} endpoint '{raw_addr}'"
        ));
    }
    let mut last_err = String::new();
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(1500)) {
            Ok(_) => return Ok(()),
            Err(e) => last_err = e.to_string(),
        }
    }
    Err(format!(
        "Cannot connect to {label} at '{raw_addr}': {last_err}. Start the stack with 'docker compose up -d' or pass '--offline'."
    ))
}

/// Probe configured live server endpoints before running a live benchmark.
pub async fn verify_live_endpoints(endpoints: &BenchmarkEndpoints) -> Result<(), String> {
    if endpoints.sink != "stdout" && endpoints.sink != "iceoryx2" {
        check_tcp_endpoint("sink_url (Broker)", &endpoints.sink_url)?;
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(2000))
        .build()
        .map_err(|e| format!("Failed to construct HTTP client: {e}"))?;

    crate::admin::check_iggy_liveness(&client, &endpoints.iggy_url)
        .await
        .map_err(|e| e.to_string())?;

    check_tcp_endpoint("query_url (ferrous-api)", &endpoints.query_url)?;

    Ok(())
}

#[derive(Deserialize)]
struct IggyTopicStats {
    #[serde(default)]
    messages_count: u64,
}

#[derive(Deserialize)]
struct IggyLoginResp {
    access_token: IggyAccessToken,
}

#[derive(Deserialize)]
struct IggyAccessToken {
    token: String,
}

async fn iggy_login(
    client: &reqwest::Client,
    iggy_url: &str,
    username: &str,
    password: &str,
) -> Result<String, String> {
    let base = iggy_url.trim_end_matches('/');
    let url = format!("{base}/users/login");
    let resp = client
        .post(&url)
        .json(&serde_json::json!({ "username": username, "password": password }))
        .send()
        .await
        .map_err(|e| format!("Failed to reach Apache Iggy login at {url}: {e}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Apache Iggy login failed ({status}): {body}"));
    }
    let parsed: IggyLoginResp = resp
        .json()
        .await
        .map_err(|e| format!("Invalid Apache Iggy login response: {e}"))?;
    Ok(parsed.access_token.token)
}

async fn iggy_topic_message_count(
    client: &reqwest::Client,
    iggy_url: &str,
    token: &str,
    stream: &str,
    topic: &str,
) -> Result<u64, String> {
    let base = iggy_url.trim_end_matches('/');
    let url = format!("{base}/streams/{stream}/topics/{topic}");
    let resp = client
        .get(&url)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|e| format!("Failed to query Iggy topic stats at {url}: {e}"))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(0);
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "Failed to query Iggy topic {stream}/{topic} ({status}): {body}"
        ));
    }
    let stats: IggyTopicStats = resp
        .json()
        .await
        .map_err(|e| format!("Invalid Iggy topic stats JSON: {e}"))?;
    Ok(stats.messages_count)
}

fn find_stream_replay_binary() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("../stream-replay-rs/target/release/stream-replay-rs"),
        PathBuf::from("./stream-replay-rs/target/release/stream-replay-rs"),
        PathBuf::from("../stream-replay-rs/target/debug/stream-replay-rs"),
        PathBuf::from("./stream-replay-rs/target/debug/stream-replay-rs"),
    ];
    for c in candidates {
        if c.is_file() {
            return Some(c);
        }
    }
    // Fall back to PATH
    Command::new("stream-replay-rs")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| PathBuf::from("stream-replay-rs"))
}

/// Execute the HPC-MQBench benchmark run and return structured stage-by-stage qualification metrics.
pub async fn run_benchmark(config: &BenchmarkConfig) -> Result<BenchmarkResult, String> {
    if config.offline {
        return run_offline_simulation(config);
    }

    verify_live_endpoints(&config.endpoints).await?;

    // Inspect workload metadata (or generate a temporary JSONL file if none supplied)
    let mut temp_path: Option<PathBuf> = None;
    let (input_path, run_id, payload_size_bytes, n_sent) = match &config.input_file_path {
        Some(path) => {
            let file = File::open(path)
                .map_err(|e| format!("Failed to open workload file '{}': {e}", path.display()))?;
            let reader = BufReader::new(file);
            let mut count = 0usize;
            let mut first_run_id = config.run_id.clone();
            let mut first_payload = config.payload_size_bytes;
            for line_res in reader.lines() {
                let line = line_res.map_err(|e| format!("Error reading workload file: {e}"))?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if count == 0 {
                    if let Ok(rec) = serde_json::from_str::<WorkloadRecord>(trimmed) {
                        first_run_id = rec.run_id;
                        first_payload = rec.payload_size_bytes;
                    }
                }
                count += 1;
            }
            if count == 0 {
                return Err(format!(
                    "Workload input file '{}' contained 0 records",
                    path.display()
                ));
            }
            (path.clone(), first_run_id, first_payload, count)
        }
        None => {
            let tmp = std::env::temp_dir().join(format!("ferrous_hpc_{}.jsonl", config.run_id));
            let file = File::create(&tmp)
                .map_err(|e| format!("Failed to create temp workload file '{}': {e}", tmp.display()))?;
            let mut writer = BufWriter::new(file);
            for rec in generate_workload_stream(
                &config.run_id,
                config.total_records,
                config.payload_size_bytes,
                config.partitions,
            ) {
                let line = serde_json::to_string(&rec)
                    .map_err(|e| format!("Failed to serialize workload record: {e}"))?;
                writeln!(writer, "{line}")
                    .map_err(|e| format!("Failed to write temp workload file: {e}"))?;
            }
            writer
                .flush()
                .map_err(|e| format!("Failed to flush temp workload file: {e}"))?;
            temp_path = Some(tmp.clone());
            (
                tmp,
                config.run_id.clone(),
                config.payload_size_bytes,
                config.total_records as usize,
            )
        }
    };

    let binary = find_stream_replay_binary().ok_or_else(|| {
        "Cannot locate 'stream-replay-rs' binary. Build it first with: cargo build --release --manifest-path ../stream-replay-rs/Cargo.toml".to_string()
    })?;

    let (stream, topic) = parse_iggy_destination(&config.topic);
    let destination = format!("{stream}/{topic}");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("Failed to construct HTTP client: {e}"))?;

    let username = std::env::var("IGGY_USERNAME").unwrap_or_else(|_| "iggy".to_string());
    let password = std::env::var("IGGY_PASSWORD").unwrap_or_else(|_| "iggy".to_string());
    let token = iggy_login(&client, &config.endpoints.iggy_url, &username, &password).await?;

    let before_count =
        iggy_topic_message_count(&client, &config.endpoints.iggy_url, &token, &stream, &topic)
            .await?;

    let mut cmd = Command::new(&binary);
    cmd.args([
        "--input-file-path",
        input_path.to_str().unwrap_or(""),
        "--sink",
        &config.endpoints.sink,
        "--sink-url",
        &config.endpoints.sink_url,
        "--topic",
        &destination,
        "--no-loop",
        "--no-progress",
    ]);
    if config.rate_msg_sec == 0 {
        cmd.args(["--speed", "max"]);
    } else {
        cmd.args(["--rate", &config.rate_msg_sec.to_string()]);
    }

    let start = Instant::now();
    let output = cmd
        .output()
        .map_err(|e| format!("Failed to execute '{}': {e}", binary.display()))?;
    let elapsed_sec = start.elapsed().as_secs_f64();

    if let Some(tmp) = temp_path {
        let _ = std::fs::remove_file(tmp);
    }

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(format!(
            "stream-replay-rs exited with {}: {}\n{}",
            output.status,
            stderr.trim(),
            stdout.trim()
        ));
    }

    let after_count =
        iggy_topic_message_count(&client, &config.endpoints.iggy_url, &token, &stream, &topic)
            .await?;
    let iggy_stored = after_count.saturating_sub(before_count) as usize;

    // Query live delta-sink (/stats) if reachable to verify Stage 3 (Delta Lake 2PC)
    #[derive(Deserialize)]
    struct DeltaSinkStats {
        delta_stored: usize,
        distinct_delta_records: usize,
        iggy_commit_us: Vec<f64>,
        end_to_end_us: Vec<f64>,
    }

    let delta_sink_url =
        std::env::var("DELTA_SINK_URL").unwrap_or_else(|_| DEFAULT_DELTA_SINK_URL.to_string());
    let delta_health_ok = client
        .get(format!("{}/health", delta_sink_url.trim_end_matches('/')))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false);

    let (delta_stored, distinct_delta_records, iggy_lat, e2e_lat, mode_label) = if delta_health_ok {
        let stats_url = format!(
            "{}/stats?table=hpc_benchmarks&run_id={}",
            delta_sink_url.trim_end_matches('/'),
            run_id
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut last = DeltaSinkStats {
            delta_stored: 0,
            distinct_delta_records: 0,
            iggy_commit_us: Vec::new(),
            end_to_end_us: Vec::new(),
        };
        while Instant::now() < deadline {
            if let Ok(resp) = client.get(&stats_url).send().await {
                if let Ok(s) = resp.json::<DeltaSinkStats>().await {
                    last = s;
                    if last.delta_stored >= n_sent {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let avg_us = round2((elapsed_sec * 1_000_000.0) / (n_sent.max(1) as f64));
        let fallback = LatencyPercentiles {
            min: avg_us,
            p50: avg_us,
            p90: avg_us,
            p95: avg_us,
            p99: avg_us,
            max: avg_us,
        };
        let iggy_pct = if last.iggy_commit_us.is_empty() {
            fallback.clone()
        } else {
            calculate_percentiles(&last.iggy_commit_us)
        };
        let e2e_pct = if last.end_to_end_us.is_empty() {
            fallback
        } else {
            calculate_percentiles(&last.end_to_end_us)
        };
        (
            last.delta_stored,
            last.distinct_delta_records,
            iggy_pct,
            e2e_pct,
            "live (end-to-end verified)".to_string(),
        )
    } else {
        let avg_us = round2((elapsed_sec * 1_000_000.0) / (n_sent.max(1) as f64));
        let measured = LatencyPercentiles {
            min: avg_us,
            p50: avg_us,
            p90: avg_us,
            p95: avg_us,
            p99: avg_us,
            max: avg_us,
        };
        (
            0,
            0,
            measured.clone(),
            measured,
            "live (broker-verified)".to_string(),
        )
    };

    let (status, disqualification_reason) = if iggy_stored != n_sent {
        if iggy_stored < n_sent {
            (
                "DISQUALIFIED".to_string(),
                Some(format!(
                    "Data loss detected at Apache Iggy: {} sent, {} stored ({} lost)",
                    n_sent,
                    iggy_stored,
                    n_sent.saturating_sub(iggy_stored)
                )),
            )
        } else {
            (
                "DISQUALIFIED".to_string(),
                Some(format!(
                    "Extra messages observed on {destination}: {} sent, {} stored (concurrent producer?)",
                    n_sent, iggy_stored
                )),
            )
        }
    } else if delta_health_ok && (delta_stored != n_sent || distinct_delta_records != n_sent) {
        (
            "DISQUALIFIED".to_string(),
            Some(format!(
                "Delta Lake 2PC mismatch: sent={}, iggy={}, delta={}, distinct={}",
                n_sent, iggy_stored, delta_stored, distinct_delta_records
            )),
        )
    } else {
        ("QUALIFIED".to_string(), None)
    };

    Ok(BenchmarkResult {
        run_id,
        topic: destination,
        mode: mode_label,
        status,
        disqualification_reason,
        endpoints: config.endpoints.clone(),
        replayer_command: build_replayer_command(config),
        workload: WorkloadSummary {
            rate_msg_sec: config.rate_msg_sec,
            payload_size_bytes,
            total_records: n_sent as u64,
            partitions: config.partitions,
            elapsed_seconds: (elapsed_sec * 1000.0).round() / 1000.0,
            achieved_tps: round2(n_sent as f64 / elapsed_sec.max(0.001)),
        },
        accounting: StageAccounting {
            sent_records: n_sent,
            iggy_stored,
            delta_stored,
            distinct_delta_records,
        },
        latencies_us: StageLatencies {
            iggy_commit: iggy_lat,
            end_to_end: e2e_lat,
        },
    })
}

fn run_offline_simulation(config: &BenchmarkConfig) -> Result<BenchmarkResult, String> {
    let start = Instant::now();

    let client_records: Vec<WorkloadRecord> = match &config.input_file_path {
        Some(path) => load_workload_jsonl(path)?,
        None => generate_workload_stream(
            &config.run_id,
            config.total_records,
            config.payload_size_bytes,
            config.partitions,
        )
        .collect(),
    };

    let run_id = client_records
        .first()
        .map(|r| r.run_id.clone())
        .unwrap_or_else(|| config.run_id.clone());
    let payload_size_bytes = client_records
        .first()
        .map(|r| r.payload_size_bytes)
        .unwrap_or(config.payload_size_bytes);
    let total_records = client_records.len() as u64;

    let mut iggy_stored = 0usize;
    let mut delta_records: Vec<String> = Vec::with_capacity(client_records.len());
    let mut iggy_latencies_us = Vec::with_capacity(client_records.len());
    let mut e2e_latencies_us = Vec::with_capacity(client_records.len());

    let loss_modulus = if config.simulate_loss_rate > 0.0 && config.simulate_loss_rate.is_finite() {
        Some(((1.0 / config.simulate_loss_rate) as usize).max(1))
    } else {
        None
    };
    let dup_modulus = if config.simulate_dup_rate > 0.0 && config.simulate_dup_rate.is_finite() {
        Some(((1.0 / config.simulate_dup_rate) as usize).max(1))
    } else {
        None
    };

    for (idx, rec) in client_records.iter().enumerate() {
        if let Some(m) = loss_modulus {
            if idx % m == 0 {
                continue;
            }
        }

        let client_ts = rec.client_timestamp_us;

        // Stage 2: Apache Iggy Broker WAL append (offline simulation)
        let iggy_delay_us = 500.0 + ((idx * 31) % 300) as f64;
        let iggy_ts = client_ts + iggy_delay_us as u64;
        iggy_latencies_us.push(iggy_delay_us);
        iggy_stored += 1;

        // Stage 3: Delta Lake 2PC micro-batch commit (offline simulation)
        let delta_delay_us = 12000.0 + ((idx * 43) % 4000) as f64;
        let delta_ts = iggy_ts + delta_delay_us as u64;
        let total_e2e_us = (delta_ts.saturating_sub(client_ts)) as f64;
        e2e_latencies_us.push(total_e2e_us);
        delta_records.push(rec.record_id.clone());

        if let Some(m) = dup_modulus {
            if idx % m == 0 {
                delta_records.push(rec.record_id.clone());
            }
        }
    }

    let elapsed_sec = start.elapsed().as_secs_f64();
    let n_sent = client_records.len();
    let n_delta = delta_records.len();
    let distinct_delta_ids: HashSet<&str> = delta_records.iter().map(|s| s.as_str()).collect();
    let n_distinct = distinct_delta_ids.len();

    let has_duplicates = n_distinct < n_delta;
    let has_data_loss = n_distinct < n_sent;

    let is_qualified =
        n_sent == iggy_stored && iggy_stored == n_delta && !has_duplicates && !has_data_loss;

    let (status, disqualification_reason) = if is_qualified {
        ("QUALIFIED".to_string(), None)
    } else if has_data_loss {
        (
            "DISQUALIFIED".to_string(),
            Some(format!(
                "Data loss detected: {} sent, {} stored ({} lost)",
                n_sent,
                n_distinct,
                n_sent.saturating_sub(n_distinct)
            )),
        )
    } else if has_duplicates {
        (
            "DISQUALIFIED".to_string(),
            Some(format!(
                "Duplicates detected: {} records stored for {} distinct IDs",
                n_delta, n_distinct
            )),
        )
    } else {
        (
            "OVERDRIVEN".to_string(),
            Some(format!(
                "Stage record count mismatch: sent={}, iggy={}, delta={}",
                n_sent, iggy_stored, n_delta
            )),
        )
    };

    Ok(BenchmarkResult {
        run_id,
        topic: config.topic.clone(),
        mode: "offline (simulated)".to_string(),
        status,
        disqualification_reason,
        endpoints: config.endpoints.clone(),
        replayer_command: build_replayer_command(config),
        workload: WorkloadSummary {
            rate_msg_sec: config.rate_msg_sec,
            payload_size_bytes,
            total_records,
            partitions: config.partitions,
            elapsed_seconds: (elapsed_sec * 1000.0).round() / 1000.0,
            achieved_tps: round2(n_sent as f64 / elapsed_sec.max(0.001)),
        },
        accounting: StageAccounting {
            sent_records: n_sent,
            iggy_stored,
            delta_stored: n_delta,
            distinct_delta_records: n_distinct,
        },
        latencies_us: StageLatencies {
            iggy_commit: calculate_percentiles(&iggy_latencies_us),
            end_to_end: calculate_percentiles(&e2e_latencies_us),
        },
    })
}

/// Format benchmark results into a qualification-first human-readable report.
pub fn format_report(result: &BenchmarkResult) -> String {
    let mut lines = Vec::new();
    let sep = "=".repeat(72);
    lines.push(sep.clone());
    lines.push(" FerrousCX Public Platform Benchmark Report (HPC-MQBench Model)".to_string());
    lines.push(format!(" Run ID:   {}", result.run_id));
    lines.push(format!(" Topic:    {}", result.topic));
    lines.push(format!(" Mode:     {}", result.mode));
    lines.push(format!(" Status:   [{}]", result.status));
    if let Some(reason) = &result.disqualification_reason {
        lines.push(format!(" Reason:   {reason}"));
    }
    lines.push(sep.clone());

    lines.push("Server Endpoints & stream-replay-rs Command:".to_string());
    lines.push(format!("  - Sink Type:           {}", result.endpoints.sink));
    lines.push(format!("  - Broker / Sink URL:   {}", result.endpoints.sink_url));
    lines.push(format!("  - Apache Iggy URL:     {}", result.endpoints.iggy_url));
    lines.push(format!("  - Ferrous API URL:     {}", result.endpoints.query_url));
    lines.push(format!("  - Replay Command:      {}", result.replayer_command));
    lines.push(String::new());

    let wk = &result.workload;
    let rate_display = if wk.rate_msg_sec == 0 {
        "max (unthrottled)".to_string()
    } else {
        format!("{} msg/sec", wk.rate_msg_sec)
    };
    lines.push("Workload Parameters:".to_string());
    lines.push(format!("  - Configured Rate:     {rate_display}"));
    lines.push(format!("  - Payload Size:        {} bytes", wk.payload_size_bytes));
    lines.push(format!("  - Partitions:          {}", wk.partitions));
    lines.push(format!("  - Total Ingest Count:  {}", wk.total_records));
    lines.push(format!("  - Elapsed Seconds:     {} s", wk.elapsed_seconds));
    lines.push(format!("  - Achieved Rate:       {} msg/sec", wk.achieved_tps));
    lines.push(String::new());

    let ac = &result.accounting;
    lines.push("Qualification Accounting Across Stages (FR-32):".to_string());
    lines.push(format!("  1. Ingest Sent:        {}", ac.sent_records));
    lines.push(format!("  2. Apache Iggy WAL:    {}", ac.iggy_stored));
    lines.push(format!(
        "  3. Delta Lake 2PC:     {} (Distinct: {})",
        ac.delta_stored, ac.distinct_delta_records
    ));
    lines.push(String::new());

    if result.status == "QUALIFIED" {
        lines.push("Stage Latencies (microseconds):".to_string());
        let i = &result.latencies_us.iggy_commit;
        lines.push(format!(
            "  - Iggy WAL Commit:     p50={}us, p95={}us, p99={}us, max={}us",
            i.p50, i.p95, i.p99, i.max
        ));
        let e = &result.latencies_us.end_to_end;
        lines.push(format!(
            "  - Total End-to-End:    p50={}us, p95={}us, p99={}us, max={}us",
            e.p50, e.p95, e.p99, e.max
        ));
    } else {
        lines.push(
            "  [NOTICE] Latencies are not ranked or certified for disqualified/overdriven runs."
                .to_string(),
        );
    }

    lines.push(sep);
    lines.join("\n")
}
