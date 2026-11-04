# FerrousCX

### Enterprise Conversational AI & Customer Experience Platform

[![Public Architecture Hub](https://img.shields.io/badge/hub-Public%20Root%20Architecture-blue.svg)](https://github.com/reidlai/ferrous-cx)
[![Architecture Standard: Pure Rust + Svelte 5](https://img.shields.io/badge/stack-Pure%20Rust%20%2B%20Svelte%205-orange.svg)](https://github.com/reidlai/ferrous-cx)
[![Components: 9 Repositories](https://img.shields.io/badge/components-9%20Git%20Repositories-purple.svg)](#2-component-repositories-nine-autonomous-repositories)
[![Runtime: Docker Compose](https://img.shields.io/badge/orchestration-Docker%20Compose%20v2.29+-blue.svg)](https://docs.docker.com/compose/)
[![Telemetry: OpenTelemetry](https://img.shields.io/badge/telemetry-OpenTelemetry%20%2B%20Jaeger-orange.svg)](https://opentelemetry.io/)
[![License: MIT / Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-green.svg)](LICENSE)

**FerrousCX** is an enterprise-grade, event-driven conversational AI and customer experience platform built on a high-performance **Rust-native stack**, integrating modern web copilot interfaces with an ultra-low-latency lakehouse architecture.

---

## 🚀 Release 1 MVP: Svelte Copilot & Rust Agentic Core

**Release 1** focuses on delivering an end-to-end, high-performance web copilot experience grounded on real-world e-commerce data:

1. **Frontend:** **Svelte 5** + **shadcn-svelte** in a 3-column layout featuring a **Transparent Agentic Workbench** and **Human-in-the-Loop (HITL)** approval workflows.
2. **Reactive Bridge:** **RxJS** unidirectional data stream bridging SSE events to Svelte 5 Runes.
3. **UI Architecture:** Strict **Atomic Design** hierarchy ensuring clean separation of presentation and business logic.
4. **Backend API Service Layer:** **Rust / Axum** API Service driven by **Goa Design v3** contracts and `rust-axum` OpenAPI code generation, leveraging native **Axum SSE** for real-time browser synchronization and embedding in-process **Apache DataFusion** (<3ms SQL).
5. **In-Memory Analytical Querying:** **Apache DataFusion** scanning **Delta Lake** Parquet tables in-process (<3ms query latency).
6. **Traffic Simulation:** **`stream-replay-rs`** (Git submodule at project root), replaying historical Parquet/CSV datasets into **Apache Iggy** via its Kafka gateway with real-time timestamp shifting.
7. **Flagship Scenario:** Brazilian E-Commerce dataset (**Olist**) for order tracking, shipment delay resolution, review analysis, and courtesy credits.

---

## 🏛️ System Architecture Topology

```text
                               ┌────────────────────────────────────────────────────────┐
                               │                    FRONTEND CLIENT                     │
                               │  Svelte 5 + shadcn-svelte (3-Column Atomic Layout)     │
                               │  [ Left Menu ]  [ Middle Canvas ]  [ Right Sidecar ]   │
                               └───────────────────────▲────────────────────────────────┘
                                                       │  ▲
                            (1) POST /v1/chat/completions  │ (SSE Stream: Tokens, Tool Events)
                                                       │  │
                               ┌───────────────────────┴──┴─────────────────────────────┐
                               │          FerrousCX API Service (Rust / Axum)           │
                               │                                                        │
                               │  • OpenAI-Compatible SSE Streamer                      │
                               │  • LLM Orchestration via `rig` (Pure Rust)             │
                               │  • Native Hugging Face (`rig::providers::huggingface`) │
                               │  • Provider Adapter (HF Qwen2.5, OpenRouter, Gemini)   │
                               └──────────────┬───────────────────────────┬─────────────┘
                                              │                           │
                   Query Historical Lakehouse │                           │ (A) Issue Live Commands
                   (Only for Historical       │                           │     POST /api/v1/commands
                    Streaming Datasets)       │                           │
                                              ▼                           ▼
┌─────────────────┐                                                       ┌─────────────────┐
│ Apache          │                                                       │ ferrous-cqrses  │
│ DataFusion      │                                                       │ Command Handler │
│ Query Engine    │                                                       │ & Aggregate Root│
└────────▲────────┘                                                       └────────┬────────┘
         │                                                                         │
         │ Vectorized SQL (<3ms)                                                   │ Direct TCP Connection:
         │ scans Parquet tables                                                    │ Event Store WAL (<1ms)
         │ (Historical Only)                                                       │ & Event Bus Pub/Sub
         ▼                                                                         ▼
┌─────────────────┐  (3) 2PC Micro-Batch  ┌─────────────────┐  (2) Consume Topic  ┌─────────────────┐
│ Delta Lake      │ ◄──────────────────── │ Arroyo Engine   │ ◄────────────────── │ Apache Iggy     │
│ (Historical     │      Parquet Sinks    │ (Stream State & │     Historical      │ Message Broker  │
│  Parquet Store) │                       │  Windowing SQL) │     Stream Topics   │ (WAL / Queues)  │
└─────────────────┘                       └─────────────────┘                     └────────▲────────┘
                                                                                           │
                                                                                           │ (1) Ingest Payload Stream
                                                                                           │     (Kafka Gateway 9092)
                                                                                  ┌────────┴────────┐
                                                                                  │ stream-replay-rs│
                                                                                  │ (Traffic Replay │
                                                                                  │  CLI & Library) │
                                                                                  └─────────────────┘
```

```mermaid
flowchart TD
    Client["Frontend Client<br/>(Svelte 5 + shadcn-svelte)"] -->|POST /v1/chat/completions| ApiService["FerrousCX API Service<br/>(Rust / Axum + Axum SSE)"]
    ApiService -->|SSE Event Stream| Client

    subgraph Historical_Path ["Historical Streaming & Analytics Pipeline (Olist)"]
        Replayer["Traffic Replayer<br/>(stream-replay-rs)"] -->|1. Ingest Payload (Kafka 9092)| IggyStream[("Apache Iggy<br/>Topic: orders/intake")]
        IggyStream -->|2. Consume Stream Topics| Arroyo["Arroyo Engine<br/>(Stream Windowing & SQL DDL)"]
        Arroyo -->|3. 2PC Parquet Sinks| DeltaHist[("Delta Lake<br/>(Historical Parquet Sinks)")]
        DeltaHist -->|4. Vectorized SQL Scan &lt;3ms| DataFusion["Apache DataFusion<br/>(Analytical Query Engine)"]
        ApiService -.->|Query Delta Lake (Historical Streaming Only)| DataFusion
    end

    subgraph CQRS_Path ["Transactional CQRS-ES Pipeline (Live Orders)"]
        ApiService -->|5. Issue Command Mutation| CQRS["Command Handler & Aggregate<br/>(ferrous-cqrses)"]
        CQRS <-->|6. Direct TCP Connection:<br/>Event Store WAL & Event Bus| IggyWAL[("Apache Iggy Broker<br/>(Event Store & Event Bus)")]
        IggyWAL -->|7. Event Bus Projections| Projector["State Projector<br/>(Live Read Model)"]
        Projector -.->|Read Active Aggregate State| ApiService
    end

    subgraph IAM_Layer ["Authentication & Access Control (Pure Rust)"]
        IAM[("Kanidm IdP Engine<br/>(ferrous-iam:8443)")]
        Client -.->|OIDC PKCE Login / Bearer JWT| IAM
        ApiService -.->|Cached JWKS Signature Check| IAM
    end
```

---

## 🎨 Frontend Architecture (Svelte 5 + Atomic Design)

The frontend adopts a responsive 3-column layout designed specifically for enterprise agentic workflows:

```text
┌──────────────┬──────────────────────────────────────────┬─────────────────────────────┐
│  Left Menu   │          Middle Content Panel            │       Right AI Sidecar      │
│  (Sidebar)   │           (Operational Canvas)           │   (Transparent Workbench)   │
├──────────────┼──────────────────────────────────────────┼─────────────────────────────┤
│ • Dashboard  │  Order #7a12b - Delayed in Transit       │  ▶ Thought Process (1.4s)   │
│ • Orders     │  ┌────────────────────────────────────┐  │                             │
│ • Customers  │  │ Status: [ In Transit - Alert ]     │  │  ⚡ Tool: query_lakehouse   │
│ • Settings   │  │ Est. Delivery: Oct 28 (2 days late)│  │     Latency: 2.4ms (DataFusion)
│   (API Keys) │  └────────────────────────────────────┘  │                             │
│              │  Items & Timeline:                       │  "The carrier reported a    │
│              │  • Wireless Earbuds ($49.99)             │   delay. Courtesy credit?"  │
│              │  Customer Sentiment: 😠 Frustrated       │                             │
│              │                                          │  ┌───────────────────────┐  │
│              │  [Contact Carrier]                       │  │ [Approve $15 Credit]  │  │
│              │                                          │  └───────────────────────┘  │
└──────────────┴──────────────────────────────────────────┴─────────────────────────────┘
```

### 1. RxJS Unidirectional Reactive Bridge
To guarantee predictable state management and handle high-throughput streaming events, an **RxJS observable pipeline** acts as the bridge between Axum's SSE stream and SvelteKit Runes:

$$\text{Axum SSE Stream} \xrightarrow{\text{EventSource / Fetch}} \textbf{RxJS Observable Pipeline} \xrightarrow{\text{Operators (map/filter/buffer)}} \textbf{Svelte 5 Runes } (\$state)$$

* **Unidirectional Flow:** SSE events enter RxJS observables, pass through pipeline operators (buffering tokens, handling backpressure, and routing tool-execution payloads), and update Svelte 5 `$state` runes.
* **Component Reactivity:** UI components purely read from `$state` and `$derived` runes; they never mutate streaming state directly.

### 2. Atomic Design Hierarchy
* **Atoms (Pre-built shadcn-svelte):** Primitive building blocks (`Button`, `Card`, `Badge`, `Accordion`, `Input`, `Separator`, `ScrollArea`).
* **Molecules (Pure Visual Presentation):** Stateless composite components maintaining zero business logic:
  * `ChatMessageBubble`: Displays message text and formatting.
  * `LatencyPill`: Formats query execution time (`2.4ms`) and engine badges.
  * `ToolCallHeader`: Renders tool name, status spinner, and arguments summary.
  * `ApprovalCardFrame`: Visual frame and action buttons for HITL prompts.
* **Organisms (Business Logic & State):** Domain-aware components connected to RxJS streams and Svelte runes:
  * `CopilotSidecarWorkbench`: Orchestrates message dispatch, thought accordions, and active tool calls.
  * `OrderResolutionCanvas`: Subscribes to order selection state, rendering live Olist items, timelines, and customer metrics.
  * `HitlApprovalWidget`: Handles approval/rejection button clicks and dispatches state-change mutations to the API gateway (`ferrous-api`).
  * `ProviderSettingsModal`: Manages user-supplied API keys (OpenAI, Anthropic, Google AI Studio, OpenRouter) stored locally in the browser.

---

## ⚡ High-Performance Rust Stack

* **API Service (`ferrous-api`):** **Rust / Axum** driven by **Goa Design v3** declarative contracts, compiling OpenAPI v3 specs into strongly typed Rust server traits via `rust-axum` and synchronizing live browser state via native **Axum SSE** (`axum::response::sse::Sse`).
* **LLM Orchestration:** **`rig` (`rig-core`)** for pure-Rust agent pipelines, prompt construction, native tool execution, and direct Hugging Face integration via `rig::providers::huggingface` (`huggingface::QWEN2_5`).
* **Analytical Query Engine:** **Apache DataFusion** executing vectorized SQL scans in <3ms directly against Delta Lake Parquet tables, used **strictly for historical streaming datasets** (e.g. Olist historical transactions, reviews, delivery logs).
* **Historical Lakehouse Storage:** **Delta Lake (`delta-lake-rs`)** receiving streaming 2PC Parquet commits from Arroyo for historical analytics.
* **CQRS-ES Engine (`ferrous-cqrses`):** Pure-Rust domain command handlers enforcing business invariants ($20 credit cap), managing aggregate rehydration, and publishing domain events to Apache Iggy Event Store WAL & Event Bus, cleanly decoupled from the historical lakehouse.
* **Message Broker:** **Apache Iggy** for ultra-high throughput event and conversation telemetry streaming.
* **Stream Processing:** **Arroyo** for stateful stream transformations, windowing computations, and session metrics.

### Direct `rig` + `DataFusion` Integration
Tools implement `rig::tool::Tool` and directly hold an `Arc<SessionContext>`:
```rust
pub struct LakehouseOrderQueryTool {
    pub df_ctx: Arc<SessionContext>,
}

impl Tool for LakehouseOrderQueryTool {
    const NAME: &'static str = "query_order_lakehouse";
    type Error = ToolError;
    type Args = OrderQueryArgs;
    type Output = OrderSummary;

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let sql = format!("SELECT * FROM orders WHERE order_id = '{}'", args.order_id);
        let df = self.df_ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        // Scanned in-memory in ~2ms with zero IPC overhead
        Ok(parse_order_summary(batches)?)
    }
}
```

---

## 🔄 Real-Time Traffic Simulation (`stream-replay-rs`)

`stream-replay-rs` ([reidlai/stream-replay-rs](https://github.com/reidlai/stream-replay-rs)) simulates realistic real-time traffic for the streaming pipeline as an autonomous, high-throughput CLI utility and library crate.

### Key Capabilities
* **Dual Format:** Usable as a standalone CLI tool (`cargo install`) and as an embeddable library crate dependency in other Rust services.
* **Dataset Agnostic:** Ingests CSV or Parquet files (`--input-file-format parquet --input-file-path <path>`) across arbitrary domains.
* **Pluggable Multi-Sink Architecture:** Dispatches events to Apache Iggy / Kafka gateway (`--sink kafka`), NATS JetStream (`--sink nats`), Fluvio (`--sink fluvio`), Eclipse iceoryx2 zero-copy IPC (`--sink iceoryx2`), or terminal inspection (`--sink stdout`).
* **Timestamp Time-Shifting:** Anchors historical event timestamps dynamically to the live wall-clock time (`chrono::Utc::now()`) maintaining precise inter-arrival pacing.
* **Interactive Micro-Burst Testing:** Supports `--burst <N>` to instantly inject batches (e.g. `--burst 500` orders) without pacing delays in under 1 second (>500 msgs/s), validating queue backpressure and buffer stability in Apache Iggy and Arroyo.

---

## 📊 Flagship Scenario & Playback Calibration

### 1. Olist Brazilian E-Commerce Dataset (Flagship for Release 1)
* **Purpose:** Retail customer support, order fulfillment, shipping delay mitigation, and sentiment analysis.
* **Schema:** Relational Parquet tables including `orders`, `order_items`, `customers`, `payments`, `reviews`, and `sellers`.
* **Execution:** Queried in real-time by `rig` agents via `DataFusion`, with order events replayed live through `stream-replay-rs` into `Iggy` $\rightarrow$ `Arroyo` $\rightarrow$ `Delta Lake`.

### 2. Calibrated 10x Default Playback Rationale
* The flagship Brazilian Olist e-commerce dataset contains **100,000 orders** spanning **730 days** (1,051,200 minutes).
* **Historical Baseline Arrival Rate**:
  $$\text{Rate}_{\text{historical}} = \frac{100,000 \text{ orders}}{1,051,200 \text{ minutes}} \approx 0.095 \text{ orders/min}$$
* **Calibrated 10x Acceleration**:
  $$\text{Rate}_{\text{accelerated}} = 0.095 \times 10 \approx 0.95 \text{ orders/min} \approx \mathbf{1\text{ order/minute}}$$
  Accelerated by **10x Speed**, the steady-state arrival rate is **~1 order/minute**, keeping platform intake queues actively populated during live human inspection and interactive operator workflows without overwhelming development machines.

### 3. Monotonic Timestamp Advancement & Primary Key Salting
* **Monotonic Time Shifting**:
  $$\text{Timestamp}_{\text{emitted}} = \text{Timestamp}_{\text{record}} + \Delta_{\text{now}} + (\text{loop\_count} \times \Delta_{\text{dataset}})$$
  Guarantees strictly increasing Arroyo watermarks with zero late-data drops or backward clock regressions across infinite looping simulations.
* **Entity ID Salting**: Appends loop iteration markers (`_L1`, `_L2`) to entity primary keys (`order_id`) when `--loop-id-salt` is enabled, eliminating primary key deduplication collisions in downstream Delta Lake tables (`orders_raw_events`, `orders_current_state`).

### 4. MultiWOZ (Multi-Domain Wizard-of-Oz)
* **Purpose:** Multi-turn dialogue state tracking and cross-domain hotel, restaurant, and train bookings.

### 5. Google Schema-Guided Dialogue (SGD)
* **Purpose:** API-driven state transitions and strict microservice schema validation across 20+ domains.

---

## 📁 Repository Architecture & Modular Submodule Ecosystem

The **FerrousCX** public hub (`ferrous-cx`) maintains the authoritative architectural specifications, C4 diagrams, and ecosystem index.

### 1. `ferrous-cx` Hub Repository Structure

```text
ferrous-cx/ (This Repository - Core Public Repository: demo config, runtime stack, docs, component submodules)
│
├── docker/                      # Compose stack, OTel tiers, smoke tests (absorbed from the former ferrous-docker)
│
├── docs/                        # Architecture deep-dives, C4 diagrams, and specifications
│   ├── c4/                      # C4 model architecture diagrams (Context, Container, Component)
│   ├── topologies/              # System topologies, streaming pipelines & IAM flows
│   ├── datasets/                # Reference dataset specifications (Olist, MultiWOZ, SGD)
│   └── security/                # Day-0 attack defense matrix, threat models & WAF policies
│
├── scripts/                     # Ecosystem validation & documentation helper scripts
│
├── LICENSE                      # Open-source license (MIT / Apache-2.0)
└── README.md                    # Public ecosystem index, architecture overview & quickstart
```

---

### 2. Component Repositories (Nine Autonomous Repositories)

`ferrous-cx` aggregates nine single-responsibility Git repositories as submodules. Each component is decoupled, contains its own documentation, pre-commit validation, and container builds. Dependencies point one way: a component must never include `ferrous-cx`, or a repository that contains it, as a submodule (components may link to `ferrous-cx` in their README).

| # | Component | Repository | Architectural Role | Tech Stack |
| :-: | :--- | :--- | :--- | :--- |
| **1** | **`ferrous-streaming`** | [`reidlai/ferrous-streaming`](https://github.com/reidlai/ferrous-streaming) | Consolidated data platform in one compose service: Apache Iggy (<1ms fsync, Kafka gateway), Arroyo 2PC Delta sinks, the Delta store (`orders_raw_events`, `orders_current_state`, `olist_*`, `llm_benchmarks`) and a DataFusion query service; also carries the public benchmark | Apache Iggy / Arroyo / Delta Lake (`delta-rs`) / DataFusion |
| **2** | **`stream-replay-rs`** | [`reidlai/stream-replay-rs`](https://github.com/reidlai/stream-replay-rs) | Independent, dataset-agnostic multi-sink traffic replayer with timestamp rebasing and speed control | Pure-Rust CLI & Library Crate |
| **3** | **`ferrous-iam`** | [`reidlai/ferrous-iam`](https://github.com/reidlai/ferrous-iam) | Identity & Access Management for human RBAC and the API (Kanidm IdP, OIDC/OAuth2, residual M2M accounts `svc_ferrous_api` and `svc_ferrous_agent`) | Pure-Rust Kanidm / Docker |
| **4** | **`ferrous-vectordb`** | [`reidlai/ferrous-vectordb`](https://github.com/reidlai/ferrous-vectordb) | Qdrant vector database indexing enterprise SOPs & support transcripts | Qdrant Vector DB / FastEmbed |
| **5** | **`ferrous-cqrses`** | [`reidlai/ferrous-cqrses`](https://github.com/reidlai/ferrous-cqrses) | Dual-Role CQRS Library (`lib.rs`) & Command Handler Service ($20 Credit Cap) | Pure-Rust Library Crate & Service |
| **6** | **`ferrous-api`** | [`reidlai/ferrous-api`](https://github.com/reidlai/ferrous-api) | Contract-First Axum API Service (Goa DSL, rust-axum, in-process DataFusion <3ms SQL, Axum SSE) | Goa v3 / Axum / DataFusion |
| **7** | **`ferrous-agent`** | [`reidlai/ferrous-agent`](https://github.com/reidlai/ferrous-agent) | Pure-Rust Multi-Provider AI Engine (`rig-core`), Context Sanitizer & In-Process Benchmarks | `rig-core` / Context Sanitizer |
| **8** | **`ferrous-frontend`** | [`reidlai/ferrous-frontend`](https://github.com/reidlai/ferrous-frontend) | Svelte 5 + shadcn-svelte 3-column UI workbench, including the read-only Replay Monitor | Svelte 5 / shadcn-svelte / RxJS / Tailwind |
| **9** | **`ferrous-reverseproxy`** | [`reidlai/ferrous-reverseproxy`](https://github.com/reidlai/ferrous-reverseproxy) | Zentinel (Cloudflare Pingora) Edge Reverse Proxy, TLS 1.3 Termination & OWASP WAF | Pure-Rust Zentinel / Pingora / CRS |

The former `ferrous-lakehouse` and `ferrous-query-engine` repositories are consolidated into `ferrous-streaming`, and the former `ferrous-docker` repository is merged into this repository (see below).

---

## 🐳 Runtime Orchestration & OpenTelemetry (formerly `ferrous-docker`)

`ferrous-cx` provides the unified integration and developer runtime harness for the platform. This section carries over the former `ferrous-docker` README, updated for the consolidated topology.

1. **Zero-Daemon Local Development:** Launch the platform with a single `docker compose up -d`, requiring zero external host dependencies.
2. **Explicit Healthcheck Dependency Graph:** Startup is gated with `condition: service_healthy`. Iggy (`ferrous-streaming`) and Qdrant boot first, then storage consumers, `ferrous-cqrses`, the API gateway, reverse proxy and frontend. `ferrous-iam` gates only `ferrous-api`, `ferrous-agent` and `ferrous-frontend`; the data platform does not depend on it.
3. **3-Tier OpenTelemetry Developer Runtime** (unrelated to the Tier 1 / Tier 2 CI repository tiers):
   - **Tier 1 (Disabled):** `OTEL_SDK_DISABLED=true` (zero span overhead, clean stdout).
   - **Tier 2 (Console):** `OTEL_TRACES_EXPORTER=console` (human-readable trace logs in the terminal).
   - **Tier 3 (OTLP Egress):** Ephemeral in-memory Jaeger All-In-One (`jaegertracing/all-in-one:latest`) on ports 4317/4318 with the Web UI on 16686.
4. **Pre-Commit DevSecOps Automation:** Hooks verify secret leakage (`gitleaks`), Dockerfile hygiene (`hadolint`) and vulnerability scanning (`trivy`) across the component repositories.
5. **Platform Smoke Validation:** Scripted end-to-end checks (`smoke-test.sh` / `smoke-test.ps1`) over the multi-service lifecycle.

### Runtime Topology

Ports for `ferrous-cqrses`, `ferrous-agent` and `ferrous-frontend` are carried over from the former `ferrous-docker` README and differ from the PRD addendum (agent `8081`, frontend `5173`); the architecture update fixes the final assignment.

| Component | Container service | Primary ports | Healthcheck probe |
| :--- | :--- | :---: | :--- |
| `ferrous-iam` | `kanidmd` | `8443`, `3636` | `kanidmd healthcheck` |
| `ferrous-streaming` | one service: Iggy, Arroyo, Delta store, DataFusion | Iggy `3000` (HTTP), `8090` (TCP), `9092` (Kafka gateway); Arroyo `5115`; DataFusion `50051` (Flight SQL), `8085` (HTTP) | Iggy ping, Arroyo health, `SELECT 1` |
| `stream-replay-rs` | on-demand CLI job (not a long-running service) | n/a | n/a |
| `ferrous-vectordb` | `qdrant` | `6333`, `6334` | `GET /readyz` HTTP 200 |
| `ferrous-cqrses` | `command-handler` | `8081` | Socket / partition liveness |
| `ferrous-api` | `axum-gateway` | `8080` | `GET /health/ready` HTTP 200 |
| `ferrous-agent` | `agent-service` | `8082` | `GET /health` HTTP 200 |
| `ferrous-frontend` | `web-studio` | `3000` | `GET /healthz` HTTP 200 |
| `ferrous-reverseproxy` | `zentinel` | `80`, `443` | TLS port 443 handshake |
| `ferrous-cx` | `jaeger` (ephemeral) | `16686`, `4317`, `4318` | `GET /` HTTP 200 |

> [!NOTE]
> Port `3000` is claimed by both the Iggy HTTP API (`ferrous-streaming`) and `web-studio` in this carried-over table. Resolve the collision in the architecture update before the compose file is written.

### DevSecOps & Smoke Testing

- Pre-commit hooks run `hadolint` and `trivy` on the Dockerfiles of every component repository.
- All containers run as non-root users with read-only root filesystems where applicable.

---

## 🛠️ Developer Quickstart

### Prerequisites
* **Rust:** `1.80+` (`rustup update stable`)
* **Node.js:** `20+` and `pnpm`
* **Docker & Docker Compose:** For running the local containerized stack

### Workflow Options

#### Option A: Develop on a Standalone Submodule
Each repository can be cloned and tested independently with zero monorepo coupling:
```bash
git clone https://github.com/reidlai/ferrous-api.git
cd ferrous-api
cargo test
```

#### Option B: Boot the Full Containerized Platform from `ferrous-cx`
To run the complete platform locally, clone this repository with its component submodules and start the compose stack:
```bash
git clone --recurse-submodules https://github.com/reidlai/ferrous-cx.git
cd ferrous-cx
docker compose up -d
# Target: all services green through healthcheck gating in <180s
docker compose ps                      # real-time status and health
open http://localhost:16686            # distributed traces in Jaeger (Tier 3)
./scripts/smoke-test.sh                # end-to-end platform smoke test
```

> [!WARNING]
> The compose file, configuration and smoke-test scripts are **not yet in this repository**. The former `ferrous-docker` repository held only its README, so these artifacts are still to be built here (compose stack under `docker/`, scripts under `scripts/`). The commands above are the target interface.

#### Option C: Full Multi-Component Development Workspace (`ferrouscx-bmad`)
For contributors working on the BMAD planning artifacts and the components together:
```bash
git clone --recurse-submodules https://github.com/reidlai/ferrouscx-bmad.git
cd ferrouscx-bmad
git submodule update --init --recursive
```

---

## 🚀 Local Demo Stack: Provision Services & Run `stream-replay-rs`

This walk-through is carried over from the former `ferrous-docker` README and adapted to the consolidated platform. It describes how to bring the stack up and drive it with the `stream-replay-rs` CLI for a local demo or manual testing. Paths assume the compose stack lives under `docker/` in this repository; run every command from the `ferrous-cx` checkout root.

### Prerequisites

| Tool | Version | Used for |
| :--- | :--- | :--- |
| Docker + Compose | `v2.29+` | Building images and running the stack |
| Rust | `1.80+` | Installing `stream-replay-rs` (`cargo install stream-replay-rs`, or `cargo install --git https://github.com/reidlai/stream-replay-rs` until the crate is published) |
| `openssl` | any | Generating Kanidm dev TLS certificates (bundled with Git for Windows) |
| Python | `3.9+` | `ferrous-streaming` provisioning scripts |

Make sure the submodules are checked out (`git submodule update --init --recursive`).

> [!IMPORTANT]
> **Shells.** On macOS / Linux use the `bash` blocks as written. On Windows use **Git Bash** for the `bash` blocks, or **PowerShell** for the `powershell` blocks. `cmd.exe` can only run `.bat` launchers, not the Bash or PowerShell blocks.
>
> Stop any other FerrousCX stack first (`docker compose down` in `ferrous-streaming`, or `stream-replay-rs/docker-compose.dev.yml`). They reuse ports `3000`, `8090` and `9092` and the container names `ferrous-iggy-broker` / `ferrous-arroyo-streaming`.

### 1. Build the Local Images

The compose file references images that are **not published to a registry**, so build them locally:

```bash
docker build -t ferrous-iam:latest ferrous-iam
(cd ferrous-streaming && ./scripts/build-and-scan.sh --skip-scan)   # PowerShell: .\scripts\build-and-scan.ps1 -SkipScan
```

`build-and-scan` builds the Iggy and Arroyo images of `ferrous-streaming`. Drop `--skip-scan` to also run the Trivy vulnerability gate.

### 2. Generate Dev TLS Certificates

Kanidm will not boot without TLS material. Generate self-signed certs and place them where the compose file mounts them (`docker/config/certs/chain.pem` and `key.pem`):

```bash
./ferrous-iam/scripts/generate-dev-certs.sh      # Windows: powershell -ExecutionPolicy Bypass -File ferrous-iam/scripts/generate-dev-certs.ps1
mkdir -p docker/config/certs
cp ferrous-iam/config/certs/chain.pem ferrous-iam/config/certs/key.pem docker/config/certs/
```

### 3. Create `docker/.env`

Iggy signs its admin JWTs with a shared secret that defaults to **empty** when unset, and the provisioning scripts in step 5 must use the same secret as the broker. Generate a random secret and set `IGGY_HTTP_JWT_ENCODING_SECRET` and `IGGY_HTTP_JWT_DECODING_SECRET` to the same value (HS256 requires identical secrets). `.env` holds secrets, so keep it out of Git. The data platform authenticates with Iggy's native credentials, not Kanidm.

### 4. Start the Stack

```bash
cd docker
docker compose up -d
docker compose ps        # wait until ferrous-iam, iggy and arroyo report "healthy"
cd ..
```

| Service | Endpoint |
| :--- | :--- |
| Kanidm (IAM) | `https://localhost:8443` (self-signed cert), LDAPS `3636` |
| Apache Iggy HTTP | `http://localhost:3000/ping` |
| Apache Iggy native TCP | `localhost:8090` |
| Kafka gateway | `localhost:9092` (**not served** by Iggy 0.9.0, see the warning below) |
| Arroyo console | `http://localhost:5115` |
| DataFusion (Flight SQL / HTTP) | `localhost:50051` / `localhost:8085` |

### 5. Provision Topics and Pipelines

`ferrous-cx` owns the FerrousCX topic setup. Load the same Iggy secret that is in `docker/.env`, then run the `ferrous-streaming` provisioners to create stream `orders` with topics `intake` and `consolidated` and the consumer groups, then deploy the Arroyo pipelines:

```bash
export IGGY_HTTP_JWT_ENCODING_SECRET=$(grep '^IGGY_HTTP_JWT_ENCODING_SECRET=' docker/.env | cut -d= -f2-)
cd ferrous-streaming
./scripts/provision-topics.sh
./scripts/provision-consumer-groups.sh
./scripts/provision-arroyo.sh
./scripts/run-migrations.sh
cd ..
```

PowerShell users run the `.ps1` twin of each script after setting `$env:IGGY_HTTP_JWT_ENCODING_SECRET`. The scripts are idempotent. The consolidation adds the replay topic `olist/orders_stream` and Arroyo Job 3 (`cg-arroyo-olist`) to this set; those scripts are still to be written.

> [!WARNING]
> **Known state against the Iggy 0.9.0 image the stack builds** (`Dockerfile.iggy` pulls `apache/iggy:latest`, which is not pinned):
> - `provision-topics` and `provision-consumer-groups` work only with the `ferrous-streaming` fixes that change the admin token's user id from `1` to `0` (Iggy 0.9.0 treats `1` as an unknown user and answers `403`), add the now-required `compression_algorithm` / `max_topic_size` topic fields, base64-encode the partition id, and address streams and topics by name. Without those fixes they stop at `403 unauthorized`.
> - `provision-arroyo` and `run-migrations` do **not** work: Arroyo reads from Iggy through a Kafka connector on port `9092`, and Iggy 0.9.0 does not serve one.
> - The `ferrous-streaming` unit tests use mock brokers written for the older API, so they report additional failures after those fixes.

### 6. Run `stream-replay-rs`

```bash
cargo install stream-replay-rs   # or: cargo install --git https://github.com/reidlai/stream-replay-rs

# Smoke test with no broker involved
stream-replay-rs --input-file-path data/olist_orders.parquet --sink stdout --speed 10x --progress

# Point at the Iggy Kafka gateway from the stack (demo profile: 10x)
stream-replay-rs --input-file-path data/olist_orders.parquet \
  --sink kafka --sink-url localhost:9092 --topic olist/orders_stream --speed 10x --progress
```

Micro-burst and looping variants are in the [`stream-replay-rs` README](https://github.com/reidlai/stream-replay-rs).

> [!WARNING]
> **No Kafka endpoint exists, and the `kafka` sink is a stub.** Port `9092` is published by Docker but nothing in the Iggy 0.9.0 container listens on it. In the `stream-replay-rs` revision last checked, the Kafka sink only opens a TCP connection and counts offsets locally; it does not write records to the wire, so **no events will appear in Iggy, Arroyo or Delta Lake**. See `stream-replay-rs/src/sink/kafka.rs` before treating the demo as end-to-end.

### 7. Check the Stack

```bash
curl http://localhost:3000/ping            # Iggy: "pong"  (PowerShell: use curl.exe)
curl http://localhost:5115/api/v1/ping     # Arroyo health
# Arroyo pipelines UI: http://localhost:5115/pipelines
```

> [!NOTE]
> In the earlier four-container stack, Arroyo wrote Delta output to a host bind mount while the lakehouse initializer used a different named volume, so the initializer's tables were not the files Arroyo wrote to. With the Delta store inside `ferrous-streaming` the stack should use one shared location; confirm this when the compose file is written.

### 8. Stop and Reset

```bash
cd docker
docker compose down            # stop, keep data
docker compose down -v         # also delete the named volumes
# Iggy, Arroyo and Delta state live under docker/data/{iggy,arroyo,delta}; delete those folders for a clean slate
```
