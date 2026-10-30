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
ferrous-cx/ (This Repository - Public Root Architecture Hub & Ecosystem Index)
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

### 2. Modular Submodule Ecosystem (12 Autonomous Repositories)

The FerrousCX platform is partitioned into 12 single-responsibility Git repositories. Each submodule is completely decoupled, contains its own documentation, pre-commit validation, and container builds:

| # | Submodule | Repository | Architectural Role | Tech Stack |
| :-: | :--- | :--- | :--- | :--- |
| **1** | **`ferrous-iam`** | [`reidlai/ferrous-iam`](https://github.com/reidlai/ferrous-iam) | Identity & Access Management (Kanidm IdP, OIDC/OAuth2, 6 M2M Svc Accounts) | Pure-Rust Kanidm / Docker |
| **2** | **`ferrous-lakehouse`** | [`reidlai/ferrous-lakehouse`](https://github.com/reidlai/ferrous-lakehouse) | Open Delta Lake ACID Storage Tier (`orders_raw_events`, `orders_current_state`) | Delta Lake (`delta-rs`) / Arrow / Parquet |
| **3** | **`ferrous-streaming`** | [`reidlai/ferrous-streaming`](https://github.com/reidlai/ferrous-streaming) | Streaming Stack (Apache Iggy Broker <1ms fsync + Arroyo 2PC Delta Lake Sinks) | Apache Iggy / Arroyo Engine / Rust |
| **4** | **`stream-replay-rs`** | [`reidlai/stream-replay-rs`](https://github.com/reidlai/stream-replay-rs) | Standalone multi-sink traffic simulator with monotonic time shifting & 10x multiplier | Pure-Rust CLI & Library Crate |
| **5** | **`ferrous-query-engine`** | [`reidlai/ferrous-query-engine`](https://github.com/reidlai/ferrous-query-engine) | Vectorized Analytical Query Engine & TableProvider Catalog | Apache DataFusion / Arrow Flight SQL |
| **6** | **`ferrous-vectordb`** | [`reidlai/ferrous-vectordb`](https://github.com/reidlai/ferrous-vectordb) | Qdrant vector database indexing enterprise SOPs & support transcripts | Qdrant Vector DB / FastEmbed |
| **7** | **`ferrous-cqrses`** | [`reidlai/ferrous-cqrses`](https://github.com/reidlai/ferrous-cqrses) | Dual-Role CQRS Library (`lib.rs`) & Command Handler Service ($20 Credit Cap) | Pure-Rust Library Crate & Service |
| **8** | **`ferrous-api`** | [`reidlai/ferrous-api`](https://github.com/reidlai/ferrous-api) | Contract-First Axum API Service (Goa DSL, rust-axum, in-process DataFusion <3ms SQL, Axum SSE) | Goa v3 / Axum / DataFusion |
| **9** | **`ferrous-agent`** | [`reidlai/ferrous-agent`](https://github.com/reidlai/ferrous-agent) | Pure-Rust Multi-Provider AI Engine (`rig-core`), Context Sanitizer & In-Process Benchmarks | `rig-core` / Context Sanitizer / PyO3 |
| **10** | **`ferrous-frontend`** | [`reidlai/ferrous-frontend`](https://github.com/reidlai/ferrous-frontend) | Svelte 5 + shadcn-svelte 3-column UI workbench (Screens 0–6) | Svelte 5 / shadcn-svelte / RxJS / Tailwind |
| **11** | **`ferrous-reverseproxy`** | [`reidlai/ferrous-reverseproxy`](https://github.com/reidlai/ferrous-reverseproxy) | Zentinel (Cloudflare Pingora) Edge Reverse Proxy, TLS 1.3 Termination & OWASP WAF | Pure-Rust Zentinel / Pingora / CRS |
| **12** | **`ferrous-docker`** | [`reidlai/ferrous-docker`](https://github.com/reidlai/ferrous-docker) | Unified 12-submodule Docker Compose runtime, 3-tier OTel & Ephemeral Jaeger UI | Docker Compose / OTel / Jaeger |

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

#### Option B: Boot the Full Containerized Platform via `ferrous-docker`
To run the complete 12-submodule platform locally:
```bash
git clone https://github.com/reidlai/ferrous-docker.git
cd ferrous-docker
docker compose up -d
# Boots all 12 services with healthcheck graphs in <180s
```

#### Option C: Full Multi-Submodule Development Workspace (`ferrouscx-bmad`)
For contributors working across multiple crates or submodules simultaneously:
```bash
git clone --recurse-submodules https://github.com/reidlai/ferrouscx-bmad.git
cd ferrouscx-bmad
git submodule update --init --recursive
```
