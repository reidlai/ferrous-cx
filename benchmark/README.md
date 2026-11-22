# HPC-MQBench Benchmark Suite (`benchmark/`)

[← Back to ferrous-cx](../README.md)

This directory owns the workload parameters (`workload.toml`), the `stream-replay-rs` replay profile (`replay-profile.toml`), the synthetic workload generator (`workload-generator`), and the qualification-first benchmark runner (`run-hpc-benchmark`) for the **HPC-MQBench** platform ingestion benchmark (Story 3.11, Story 13.8, Story 15.1, Story 15.2, Story 16.14, FR-32, FR-33, OQ-9, SM-8).

The benchmark measures end-to-end platform ingestion latency under **qualification-first** reporting:
$$N_{\text{sent}} = N_{\text{iggy}} = N_{\text{delta}}$$

---

## 🌐 Server Endpoint Discovery & Configuration

| Ingestion Stage | CLI Flag | Environment Variable | Default Endpoint | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Stage 1: Replayer Sink** | `--sink` | — | `iggy` | `stream-replay-rs` sink type (`iggy`, `kafka`, `nats`, `fluvio`, `iceoryx2`, `stdout`) |
| **Stage 2: Apache Iggy Broker** | `--iggy-url` | `IGGY_HTTP_URL` | `http://127.0.0.1:3000` | Apache Iggy HTTP API for `/ping` liveness and topic/offset verification |
| **Stage 3: Delta Lake / Query** | `--query-url` | `QUERY_SERVICE_URL` | `http://127.0.0.1:8080` | Ferrous API DataFusion Query Service HTTP API over `/data/delta/hpc_benchmarks` |
| **Target Topic** | `--topic` | `BENCHMARK_TOPIC` | `benchmark/hpc_mqbench` | Target Iggy destination (`<stream>/<topic>`, 4 partitions) |

---

## 🚀 Step 1: Generate Synthetic HPC Workload (`workload-generator`)

Synthetic datasets are generated on-the-fly without bundling proprietary or static data files.

```bash
# Generate 1,000 records of 1,024 bytes each across 4 partitions into ./hpc_workload.jsonl
cargo run --bin workload-generator -- \
  --count 1000 \
  --payload-size 1024 \
  --partitions 4 \
  --output ./hpc_workload.jsonl

# Generate 5,000 records of 256 bytes each (Small Control profile)
cargo run --bin workload-generator -- --count 5000 --payload-size 256 --partitions 4 --output ./hpc_workload.jsonl

# Generate with a custom run ID
cargo run --bin workload-generator -- --run-id custom-bench-1 --count 2500 -o ./hpc_workload.jsonl
```

### Generator Options

| Flag | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--count` | `-c` | `1000` | Number of records to generate |
| `--payload-size` | `-s` | `1024` | Target payload size in bytes (exact byte-padded) |
| `--partitions` | `-p` | `4` | Number of topic partitions for round-robin distribution |
| `--run-id` | `-r` | `run-<uuid>` | Run identifier prefix used in `run_id` and `record_id` |
| `--output` | `-o` | `-` | Output file path or `-` for standard output |

### Workload Record Schema

Each record is serialized as a single-line JSON object (NDJSON):

```json
{
  "run_id": "run-42a1b2c3d4e5",
  "record_id": "run-42a1b2c3d4e5:00000000",
  "seq": 0,
  "partition": 0,
  "client_timestamp_us": 1731640000000000,
  "payload_size_bytes": 1024,
  "data": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX..."
}
```

The `data` field is dynamically padded with `'X'` characters so that the entire serialized JSON string matches `--payload-size` byte-for-byte.

---

## 🔁 Step 2: Replay Dataset with `stream-replay-rs` (`replay-profile.toml`)

`benchmark/replay-profile.toml` is a turnkey `stream-replay-rs` configuration file that maps `client_timestamp_us` (`us`) and `record_id` to `benchmark/hpc_mqbench`:

```bash
# Live replay into the ferrous-cx stack directly via Apache Iggy HTTP API (http://127.0.0.1:3000)
stream-replay-rs --config benchmark/replay-profile.toml

# Maximum unthrottled throughput (135k–155k+ msg/s across 4 batched partition workers)
stream-replay-rs --config benchmark/replay-profile.toml --speed max --no-loop
```

---

## 📊 Step 3: Run End-to-End Qualification Benchmark (`run-hpc-benchmark`)

```bash
# Run deterministic offline qualification verification (simulated)
cargo run --release --bin run-hpc-benchmark -- --offline --count 1000 --rate 25

# Run live broker-verified benchmark against running Apache Iggy stack (--rate 0 = unthrottled max speed)
cargo run --release --bin run-hpc-benchmark -- \
  --sink iggy --sink-url http://127.0.0.1:3000 \
  --topic benchmark/hpc_mqbench \
  --count 100000 --rate 0 --payload-size 256 --partitions 4
```
