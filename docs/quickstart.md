# FerrousCX Evaluator Quickstart Guide (Story 12.4)

Welcome to the **FerrousCX High-Performance Customer Experience Platform**. This guide provides a rapid, end-to-end walkthrough for technical evaluators to boot the complete 10-component containerized platform, provision streaming topics, and replay real-world Brazilian e-commerce (Olist) event streams.

---

## 1. Architecture & Ecosystem Overview

FerrousCX unites ten decoupled services into a deterministic, Day-0 hardened container topology:
- **Perimeter & Security:** `ferrous-reverseproxy` (Zentinel / Hyper TLS termination on ports 80/443 with Day-0 OWASP WAF), `ferrous-iam` (Kanidm pure-Rust IdP on port 8443).
- **Event Streaming & WAL:** `iggy-broker` (Apache Iggy memory/disk WAL on port 3000/8090), `bridge` / `bridge-host` (Iggy-Kafka bridge on 9092), `arroyo` (distributed streaming engine on 5115), and `delta-store` (Delta Lake 2PC ACID engine).
- **Core Domain & CQRS:** `command-handler` (CQRS-ES aggregate root), `ferrous-api` (Axum REST & SSE API gateway on 8080), `vectordb` (Qdrant vector engine on 6333), and `ferrous-agent` (Rig-core AI engine on 8081).
- **Observability & Studio:** `jaeger` (Ephemeral in-memory Jaeger v2 on 4317/4318/16686) and `ferrous-frontend` (Svelte 5 Operator Studio on 5173).

---

## 2. Step 1: Booting the Master Topology

Ensure Docker Compose v2.20+ is installed and Docker Desktop is running.

```bash
# Clone the repository and initialize submodules
git clone https://github.com/reidlai/ferrous-cx.git
cd ferrous-cx

# Copy environment configuration
cp .env.example .env

# Boot all services in background
docker compose up -d
```

Verify service status and healthcheck readiness:
```bash
# Linux/macOS
./scripts/smoke-test.sh

# Windows PowerShell
.\scripts\smoke-test.ps1
```

All 8 service probes should return `[OK]` within 15 seconds.

---

## 3. Step 2: Provisioning Streams, Topics & Consumer Groups

Run the automated topic provisioner to initialize the Apache Iggy streams and consumer groups declared in `contracts/topics.yml`:

```bash
# Cross-Platform (Cargo)
cargo run --bin setup-topics -- --dry-run   # Conformance audit
cargo run --bin setup-topics               # Live provisioning

# Optional shell wrappers:
# Linux/macOS:        ./scripts/setup-topics.sh
# Windows PowerShell: .\scripts\setup-topics.ps1
```

The provisioner idempotently creates:
- **Stream `orders`:** Topics `orders/intake` and `orders/consolidated` (4 partitions, replicated durability).
- **Stream `olist`:** Topic `olist/orders_stream` (4 partitions, persisted durability).
- **Stream `benchmark`:** Topic `benchmark/hpc_mqbench` (4 partitions, persisted durability).
- **Consumer Groups:** Bridge consumer groups declared in the contract: `cg-arroyo-raw`, `cg-arroyo-state`, `cg-arroyo-olist`, and `cg-arroyo-benchmark`.

---

## 4. Step 3: Replaying Olist Traffic

Event streaming traffic is produced into the platform using `stream-replay-rs`.

> **Note on Security:** No replayer command or profile requires or consumes a Kanidm user account. The replayer publishes strictly to the unauthenticated bridge loopback endpoint (`127.0.0.1:9092`).

### 4.1 Invoking the Replayer
You can run `stream-replay-rs` directly from the sibling submodule or install it:

```bash
# Option A: Run directly via Cargo
cargo run --release --manifest-path ../stream-replay-rs/Cargo.toml -- --config profiles/olist-demo.toml

# Option B: Run installed binary
stream-replay-rs --config profiles/olist-demo.toml
```

### 4.2 Demo Profile (Human-Paced Demonstration)

To observe order lifecycles and copilot recommendations at a comfortable human pace:

```bash
stream-replay-rs --config profiles/olist-demo.toml
```

#### The "Why 10x" Arithmetic (FR-1, FR-1b)
- The raw Olist Brazilian E-Commerce dataset contains ~100,000 orders spanning approximately 2 full years (730 days, or ~1,051,200 minutes).
- In real-time reality, customer purchases occurred at an average frequency of:
  $$\frac{100,000 \text{ orders}}{1,051,200 \text{ minutes}} \approx 0.095 \text{ orders/minute} \quad (\approx 1 \text{ order every } 10.5 \text{ minutes})$$
- Waiting 10.5 minutes between successive orders is impractical for interactive demonstrations and evaluation.
- Running at `--speed 10.0` (10x compression) divides the inter-arrival delta by 10, producing:
  $$0.095 \times 10 \approx 0.95 \text{ to } 1.0 \text{ orders/minute} \quad (\approx 1 \text{ order every } 60 \text{ seconds})$$
- This produces a steady, authentic stream of new orders appearing on the Operator Studio canvas in real time without overwhelming human attention.

---

### 4.2 Load Profile (Sustained Stress Testing)

To evaluate backpressure, Arroyo windowing, and Delta Lake ACID commit durability under load:

```bash
stream-replay-rs --config profiles/olist-load.toml
```

- Sustains a continuous throughput of **25 orders per second** (`--rate 25`).
- Maintains bounded watermark latency (< 500ms) with zero dropped packets.

---

## 5. Step 4: Operator Web Studio Inspection

Open your browser to the Web Operator Studio:
- **Perimeter HTTPS:** `https://localhost:443/` (or `http://localhost:80/`)
- **Direct Frontend:** `http://localhost:5173/`

### Important Architectural Invariant: Read-Only UI
The Operator Studio is **strictly read-only**. Operators investigate live orders, inspect SLA radar indicators, review carrier analytics, and inspect AI copilot HITL proposals. The UI never executes direct database writes or mutations; all state transitions originate exclusively as event streams ingested through Apache Iggy.

---

## 6. Step 5: Distributed Tracing & Observability

To inspect end-to-end distributed traces across the platform:
- Open the ephemeral **Jaeger Web UI** at `http://localhost:16686/`.
- Select service `ferrous-agent` or `ferrous-api` to inspect trace waterfalls and GenAI inference latencies.
