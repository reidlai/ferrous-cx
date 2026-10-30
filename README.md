# FerrousCX

### Enterprise Conversational AI & Customer Experience Platform

[![Public Architecture Hub](https://img.shields.io/badge/hub-Public%20Root%20Architecture-blue.svg)](https://github.com/reidlai/ferrous-cx)
[![Architecture Standard: Pure Rust + Svelte 5](https://img.shields.io/badge/stack-Pure%20Rust%20%2B%20Svelte%205-orange.svg)](https://github.com/reidlai/ferrous-cx)
[![Submodules: 13 Repositories](https://img.shields.io/badge/submodules-13%20Git%20Repositories-purple.svg)](#-repository-architecture--modular-submodules)
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

`stream-replay-rs` simulates realistic real-time traffic for the streaming pipeline:

```text
ferrous-cx/
├── stream-replay-rs/          # [Git Submodule] Rust crate & CLI binary
```

### Key Capabilities
* **Dual Format:** Usable as a standalone CLI tool (`cargo install`) and as a library crate dependency in other Rust services.
* **Input Formats:** Ingests CSV or Parquet files (`--input-file-format parquet --input-file-path <path>`).
* **Kafka Gateway Sink:** Streams simulated traffic directly into Apache Iggy's Kafka-compatible gateway (`--sink kafka --sink-url <iggy_kafka_url>`).
* **Timestamp Time-Shifting:** `--shift-timestamps-to-now` recalculates historical timestamps relative to `chrono::Utc::now()` so streaming engines treat events as live.
* **Rate & Speed Multipliers:** `--speed 1.0` (real-time replay), `--speed 10.0` (10x fast-forward), or `--rate 100` (fixed 100 events/sec).

---

## 📊 Reference Datasets

### 1. Olist Brazilian E-Commerce Dataset (Flagship for Release 1)
* **Purpose:** Retail customer support, order fulfillment, shipping delay mitigation, and sentiment analysis.
* **Schema:** Relational Parquet tables including `orders`, `order_items`, `customers`, `payments`, `reviews`, and `sellers`.
* **Execution:** Queried in real-time by `rig` agents via `DataFusion`, with order events replayed live through `stream-replay-rs` into `Iggy` $\rightarrow$ `Arroyo` $\rightarrow$ `Delta Lake`.

### 2. MultiWOZ (Multi-Domain Wizard-of-Oz)
* **Purpose:** Multi-turn dialogue state tracking and cross-domain hotel, restaurant, and train bookings.

### 3. Google Schema-Guided Dialogue (SGD)
* **Purpose:** API-driven state transitions and strict microservice schema validation across 20+ domains.

---

## 📁 Repository Architecture & Modular Submodules

The **FerrousCX** platform is organized as a modular, single-responsibility Git submodule ecosystem:

```text
ferrous-cx/ (This Repository - Public Root Architecture & Ecosystem Index)
│
├── ferrous-iam/             # [Submodule 1] Pure-Rust Identity & Access Management (Kanidm IdP, OIDC/OAuth2, 6 M2M Svc Accounts)
│
├── ferrous-lakehouse/       # [Submodule 2] Open Delta Lake ACID Storage Tier (orders_raw_events, orders_current_state, llm_benchmarks)
│
├── ferrous-streaming/       # [Submodule 3] Unified Streaming Stack (Apache Iggy Broker <1ms fsync + Arroyo 2PC Delta Lake Sinks)
│
├── stream-replay-rs/        # [Submodule 4] Standalone multi-sink traffic simulator with monotonic time shifting & 10x multiplier
│
├── ferrous-query-engine/    # [Submodule 5] Apache DataFusion Vectorized Analytical Query Engine & TableProvider Catalog
│
├── ferrous-vectordb/        # [Submodule 6] Qdrant vector database indexing enterprise SOPs & support transcripts
│
├── ferrous-cqrses/          # [Submodule 7] Dual-Role CQRS Library (lib.rs) & Command Handler Service ($20 Courtesy Credit Cap)
│
├── ferrous-api/             # [Submodule 8] Contract-First Axum API Service (Goa DSL, rust-axum, in-process DataFusion <3ms SQL, Axum SSE)
│
├── ferrous-agent/           # [Submodule 9] Pure-Rust Multi-Provider AI Engine (rig-core), Context Sanitizer & In-Process Benchmarks
│
├── ferrous-frontend/        # [Submodule 10] Svelte 5 + shadcn-svelte 3-column UI workbench (Screens 0–6)
│
├── ferrous-reverseproxy/    # [Submodule 11] Zentinel (Cloudflare Pingora) Edge Reverse Proxy, TLS 1.3 Termination & OWASP WAF
│
└── ferrous-docker/          # [Submodule 12] Unified 12-submodule Docker Compose runtime, 3-tier OTel & Ephemeral Jaeger UI
```

---

## 🛠️ Developer Quickstart

### Prerequisites
* **Rust:** `1.80+` (`rustup update stable`)
* **Node.js:** `20+` and `pnpm`
* **Docker & Docker Compose:** For running the local containerized stack

### 1. Clone the Ecosystem
```bash
git clone --recurse-submodules https://github.com/reidlai/ferrous-cx.git
cd ferrous-cx
```

### 2. Launch Local Infrastructure & Containers
```bash
# Start all services via the docker orchestration submodule
cd ferrous-docker
docker compose up -d
# Spins up: ferrous-iam (Kanidm), ferrous-lakehouse (Delta Lake), ferrous-streaming (Iggy + Arroyo), ferrous-vectordb (Qdrant), ferrous-api, etc.
```

### 3. Run the Traffic Replayer
```bash
# Install and run stream-replay-rs to inject continuous Olist traffic into Iggy
cargo install --path ./stream-replay-rs
stream-replay-rs \
  --input-file-format parquet \
  --input-file-path ./data/olist_orders.parquet \
  --sink kafka \
  --sink-url localhost:9092 \
  --advance-loop-timestamps \
  --loop-id-salt \
  --rate 25
```

### 4. Launch the Svelte 5 Frontend
```bash
cd ../ferrous-frontend
pnpm install
pnpm dev
# Open http://localhost:5173 in your browser
```
