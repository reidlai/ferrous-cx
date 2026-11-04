---
title: Bridge Contract Sheet (DRAFT)
story: 3-13-iggy-kafka-bridge-protocol-spike-contract-sheet-consume-side
status: draft-desk-only
created: 2026-11-04
implements: AD-28 rule 8, FR-33
source-of-draft: _bmad-output/implementation-artifacts/spike-3-13/bridge-contract-sheet-DRAFT.md (root repository)
---

# Bridge Contract Sheet (DRAFT)

> **Not yet run against a live stack.** Every row comes from reading source code and documentation on 2026-11-04. The Docker daemon was not available, so nothing here has been observed on a running system. Rows marked **Desk** are sourced and unverified; rows marked **L#** name the live check in the table at the end that must pass before the sheet is final. Evidence and URLs: [evidence-upstream-gateway.md](../../_bmad-output/implementation-artifacts/spike-3-13/evidence-upstream-gateway.md), [evidence-arroyo-kafka-source.md](../../_bmad-output/implementation-artifacts/spike-3-13/evidence-arroyo-kafka-source.md), [evidence-replayer-sink.md](../../_bmad-output/implementation-artifacts/spike-3-13/evidence-replayer-sink.md).

**Implementation assumed:** Apache Iggy's upstream `iggy-gateway-kafka` image, pinned by digest and configured, not built here (decision note: [decision.md](../../_bmad-output/implementation-artifacts/spike-3-13/decision.md)). If a live check forces the build-here option, the rows still hold. They describe the contract, and the implementation must meet them.

## 1. Topic coordinates

Kafka topic names cannot contain `/`, so each topic carries a generated `kafka_name` in `topics.yml` (AD-27). The bridge's topic map file is generated from the same file, never edited by hand.

| Iggy `{stream, topic}` | `kafka_name` (proposed) | Kafka clients that use it | Direction through the bridge |
|---|---|---|---|
| `orders/intake` | `orders.intake` | Arroyo `cg-arroyo-raw` | consume only |
| `orders/consolidated` | `orders.consolidated` | Arroyo `cg-arroyo-state` | consume only |
| `olist/orders_stream` | `olist.orders_stream` | Arroyo `cg-arroyo-olist`; replayer | consume and produce |
| benchmark topics | set by Story 3.11 | replayer; Arroyo | produce (and consume) |

- Rule for generating `kafka_name`: replace `/` with `.`. Allowed characters are `[A-Za-z0-9._-]`, at most 249 bytes (Kafka's rule, also enforced by the gateway). **Desk**
- Generated map file (TOML) for the gateway, pointed to by `IGGY_KAFKA_TOPIC_MAP_PATH`. Dotted keys must be quoted. **Desk** (L4, L8)

  ```toml
  default_stream = "kafka"           # required; must hold NO topics (see section 8)
  [topics."orders.intake"]
  stream = "orders"
  topic  = "intake"
  [topics."orders.consolidated"]
  stream = "orders"
  topic  = "consolidated"
  [topics."olist.orders_stream"]
  stream = "olist"
  topic  = "orders_stream"
  ```

- Overrides must be one-to-one. The gateway rejects a map that points two Kafka names at one Iggy topic. The map is read at start, so a change needs a restart. **Desk**
- Components never concatenate or hardcode these names; a conformance job (AD-27) compares the generated map with `topics.yml`.

## 2. Coordinate table

| Question | Contract | Source |
|---|---|---|
| Partition index base | Zero-based on both sides. Kafka partition `n` is Iggy partition id `n`. No conversion. | Desk (L1) |
| Offset | Kafka offset equals the Iggy offset of the same record. One Kafka record is one Iggy message. A Kafka batch is split into messages, so batch boundaries are not preserved. | Desk (L1, L10) |
| Next-to-read versus last-consumed | Arroyo's checkpoint stores **last consumed + 1** (next to read) and resumes with an absolute assign. Arroyo's best-effort offset commit sends **last consumed** (one lower). Nothing reads the committed value to resume, so the difference is informational. Anyone using `source.offset = group` on a fresh deploy would re-read one record per partition. | Desk (Arroyo source, L2) |
| Latest (log end) | The high watermark, one past the last committed offset. An empty partition answers 0. While a partition is still loading, ListOffsets answers a retriable error, never a guessed 0. | Desk (L1) |
| Earliest | Always answers 0. Iggy exposes no log-start offset. This is correct only while no segment is ever trimmed. **All platform topics are created without expiry**; enabling retention on any bridged topic changes this row and needs a contract revision. | Desk, upstream issue #4525 |
| Fetch below the oldest kept record | Not an error. The gateway reads from the oldest kept record (silent skip). Kafka would answer out-of-range. Not reachable while no retention is set. | Desk |
| Fetch at the high watermark | Empty success. | Desk (L1) |
| Fetch past the high watermark | See section 7. | Desk (L11) |
| Time lookups (ListOffsets by timestamp) | Use Iggy append time, not the producer's CreateTime. Not used by Arroyo or the replayer. | Desk |
| `log_start_offset` in Fetch responses | Always `-1` (unknown). Arroyo does not use it. | Desk |

## 3. Kafka API and version matrix

Clients negotiate down to the highest version both sides support. Ranges below are what each side advertises or can speak.

| API | Arroyo 0.15.0 (librdkafka 2.3.0, bundled via rdkafka 0.37) | Upstream gateway | Negotiated | Used by Arroyo? | Status |
|---|---|---|---|---|---|
| ApiVersions | sends v3, retries lower on rejection | answers (range not documented) | v0 to v3 | yes, every connection, must work | Desk (L1) |
| Metadata | v0 to v12 | v0 to v9 | v9 | yes: bootstrap, one-topic lookup, 5-minute refresh | Desk (L1, L6) |
| ListOffsets | v0 to v7 | v1 to v7 | v7 | yes: `earliest` and `latest` starts only | Desk (L1) |
| Fetch | v0 to v11 | v4 to v12 | v11 | yes, continuous | Desk (L1, L6) |
| FindCoordinator | v0 to v2 | v0 to v4 | v2 | yes, in the background because `group.id` is set | Desk (L5) |
| OffsetCommit | v0 to v7 | v2 to v9 | v7 | yes, async and best-effort at each checkpoint | Desk (L5, L7) |
| OffsetFetch | v0 to v7 | v1 to v9 | v7 | only when `source.offset = group` (not used) | Desk |
| JoinGroup, SyncGroup, Heartbeat, LeaveGroup | not used (`assign()`, never `subscribe()`) | v0 to v9 (classic protocol only) | none | no | Desk |
| Produce | not used by Arroyo. Replayer (`rskafka` 0.6.0): v3 to v7 | v3 to v9 | v7 | no | Desk (L10) |
| InitProducerId | not used by Arroyo; never sent by `rskafka` | v0 to v5 | none | no | Desk |
| CreateTopics, DeleteTopics, AlterConfigs | not used at run time | implemented | none wanted | no | rejected by Iggy permissions (L4) |

Notes that bind the implementation:
- Arroyo uses librdkafka's default `isolation.level = read_committed` even though its option says `read_uncommitted` (that setting sets nothing). Fetch v4 and later then depends on a valid `last_stable_offset`. The gateway ignores isolation level, so the pipelines set `isolation.level=read_uncommitted` explicitly (section 6) and L6 checks the response anyway.
- Metadata must list partitions in ascending order. Arroyo gives subtask `k` the partitions at positions `i` where `i mod parallelism = k`, by position, not by partition id. The MVP runs parallelism 1, which makes order irrelevant, and L6 checks order before raising it.
- KIP-848 (`group.protocol=consumer`), transactions, compaction and SCRAM are not supported and not needed.

## 4. Replayer sink: client profile

Today's `stream-replay-rs` Kafka sink is a simulation: it opens one TCP connection, ignores failure and counts records, with no Kafka client crate at all ([evidence-replayer-sink.md](../../_bmad-output/implementation-artifacts/spike-3-13/evidence-replayer-sink.md), section 1). The profile below is therefore what Story 4.6 builds, not a description of existing code. **Story 4.6 must not rely on it until L10 has passed.**

Fixed by the gateway, whichever client is chosen:

| Setting | Contract | Source |
|---|---|---|
| Produce versions | v3 to v9 only. The gateway closes the connection on v0 to v2. | Desk |
| `acks` | `0`, `1` and `-1` all behave the same: the gateway waits for Iggy to confirm the append. Any other value is rejected. The replayer uses `acks=all` anyway, so the profile does not depend on this. `acks=0` closes the connection on any failed partition and must not be used. | Desk |
| Compression in | gzip, snappy, lz4, zstd (zstd from Produce v7). Fetch always answers uncompressed. | Desk |
| Idempotence | **Off** on the gateway by default (`IGGY_KAFKA_IDEMPOTENT_PRODUCERS=false`), and the contract keeps it off: turning it on needs every Iggy node, gateway and consumer upgraded first because released builds cannot read a batch that carries producer information. A client may still send InitProducerId; it gets at-least-once behaviour. | Desk |
| Transactions | Unsupported for good. `transactional.id` is rejected. | Desk |
| Topic auto-creation | Off. Produce never creates a topic. A Metadata request for a missing topic does not create one either (`IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED=false`). The client must set `allow.auto.create.topics=false`. | Desk (L4) |
| Batches | One batch per partition per request (a second batch for the same partition is rejected). | Desk |
| Order across retries | Without server-side dedup, a retried batch can land after later ones. The client keeps `max.in.flight.requests.per.connection=1`, or accepts reordering. | Desk |
| Request size | The gateway frame limit defaults to 8 MiB (Kafka's is 100 MiB). Oversize answers `MESSAGE_TOO_LARGE`. | Desk |
| Client minimum | librdkafka 1.8 or newer. The Rust client `rskafka` is **not** in the upstream test matrix. | Desk, [upstream client matrix issue #4531](https://github.com/apache/iggy/issues/4531) |

Chosen client profile (proposed in the evidence file, section 3; **Desk**, L10):

| Item | Profile |
|---|---|
| Client | `rskafka` 0.6.0 (pure Rust, no C toolchain, no CMake; gzip and snappy features only), behind a small producer trait so `rdkafka` can replace it. The fallback is `rdkafka` with `cmake-build` on Windows. Equivalent `rdkafka` settings: `acks=all`, `enable.idempotence=false`, `max.in.flight=1`, bounded retries, `allow.auto.create.topics=false`, `compression.codec=none`, `partitioner=murmur2_random`. |
| Calls the bridge serves for it | ApiVersions, Metadata, Produce. Nothing else. |
| Versions negotiated | ApiVersions v3 (falls back to v0), Metadata v4, Produce v7. |
| Metadata request | At start, an **all-topics** request (`topics = null`); later per-topic requests. Partitions must be dense `0..N-1`, the leader the one advertised broker, and the advertised address reachable from the client (`rskafka` reconnects to the advertised address). |
| Acks, timeout | `acks=-1` and `timeout_ms=30000`, both fixed in `rskafka`. The gateway honours `timeout_ms` only up to 20 s. L10 checks what it does with 30 s. |
| Idempotence, transactions | Off. `producer_id=-1`; InitProducerId never sent. |
| Compression | None in v1. |
| Batching | Linger 5 ms, up to 256 KiB per request, one produce in flight per partition (keeps order under retries). |
| Timeouts and retries | `rskafka` 0.6.0 has no request timeout and retries forever by default. The sink wraps each call in a 30 s timeout, sets a 10 s backoff deadline and uses `UnknownTopicHandling::Error`. A dead bridge fails the run with a non-zero exit. A timeout is logged as "possibly written". |
| Topic creation | The replayer never creates topics. `rskafka` still sends `allow_auto_topic_creation=true` in Metadata v4 and cannot turn it off, so **the bridge must keep auto-creation disabled** (`IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED=false`, the default). Contract, not client behaviour. |
| Key | The value of `--key-column` as UTF-8 bytes (`order_id` for the order-related Olist tables). With the loop salt the key carries `_L{n}`, so an entity may change partition between loops. |
| Partition | Chosen by the replayer, not by Kafka: `(murmur2(key) & 0x7fffffff) mod N`, with `N` read from Metadata once at start (the Java and `murmur2_random` mapping). If `N` changes during the run, the run stops. |
| Headers | `replay.run_id`, `replay.seq`, `replay.loop`: three headers. The replayer is generic and does not emit `traceparent`. The gateway's limit is documented as "3 headers per slot", which three headers meet exactly, so **L10 must confirm three are accepted**. `rskafka` headers are a sorted map, so a repeated name (upstream issue #4517) cannot happen. |
| Timestamp | `CreateTime` = the rebased event time in ms. |
| Security | Plaintext on the compose network and host loopback; no TLS, no SASL (`[ASSUMPTION: Bridge Auth]`). |

**Duplicates.** With idempotence off, a response lost to a timeout or bridge restart makes a retry store the record twice under new offsets. Offsets cannot reveal it. The headers `replay.run_id` and `replay.seq` give each record a stable identity, so FR-32 accounting counts **distinct** identities, and "N sent equals N stored" means N distinct identities. This only works if the headers reach the Delta tables, which Arroyo does not do today (it ignores Kafka headers). Who projects them is open (`[OPEN: Duplicate Aggregation]`). The alternative is for the replayer to treat any ambiguous outcome as terminal and rerun into a fresh topic.

## 5. Record mapping

| Kafka field | Iggy representation | Notes | Status |
|---|---|---|---|
| value | message payload | Passed through unchanged (JSON envelope, AD-27). | Desk (L10) |
| key | user header `kafka.key` (raw bytes) | A native Iggy reader sees the key as a header, not as an Iggy key. | Desk (L10) |
| header `name: value` | user header `kafka.h.<name>` (raw bytes) | Includes `replay.run_id`, `replay.seq`, `replay.loop` (the replayer does not set `traceparent`; a header another producer sets passes through). A repeated header name rejects the whole partition batch (`INVALID_RECORD`, upstream issue #4517). Header order is not preserved on fetch (sorted by name). Header names and values are limited to 255 bytes; the documented cap of "3 headers per slot" is met exactly by the three replay headers (L10). | Desk (L10) |
| record timestamp (CreateTime, ms) | message `origin_timestamp` (microseconds, x1000); fetch divides by 1000 | Arroyo uses the Kafka record timestamp as the default event time and fails the task if it is missing. A batch whose timestamps span more than about 71.6 minutes is clamped and the real value goes into header `kafka.ts`. | Desk (L6, L10) |
| mapping marker | header `kafka.v` (1 byte) on every message the gateway writes | | Desk |
| null or empty value | payload `0x00` plus header `kafka.value = null or empty` | Tombstones are stored as ordinary records. | Desk |
| any other unusual shape (empty-present key, oversize names, headers over 100 KB) | "envelope" fallback (`kafka.envelope` header, opaque payload) | A native Iggy reader cannot read these. The platform never produces such records. | Desk |

Messages written by native Iggy clients (for example `orders/intake` written by `ferrous-api`) carry no `kafka.v` header. The gateway serves them as Kafka records with a **null key**, the payload as value, and every Iggy user header as a Kafka header under its own name. Arroyo needs only the value and the timestamp. **Whether such messages carry a usable timestamp is the first thing L6 checks.**

Reserved names: no native writer may set a header whose name starts with `kafka.` (see poison records, section 7).

## 6. Arroyo pipeline settings the contract requires

Each of the three Arroyo pipelines declares its Kafka source like this (`bridge:9092` is the compose-network listener, section 9):

```sql
CREATE TABLE <name> ( ... ) WITH (
  connector       = 'kafka',
  bootstrap_servers = 'bridge:9092',
  topic           = '<kafka_name>',              -- from topics.yml
  type            = 'source',
  format          = 'json',
  'source.offset'   = 'earliest',                -- first deploy reads the whole log
  'source.group_id' = 'cg-arroyo-<raw|state|olist>',
  client_configs  = 'auto.offset.reset=error,isolation.level=read_uncommitted'
);
```

| Setting | Why |
|---|---|
| `source.group_id` set per pipeline | Arroyo's default is `arroyo-{job}-{operator}-consumer`, which would not match the reserved group names (AD-3). |
| `source.offset = earliest` | Arroyo's default is `latest`, which would silently skip the history on the first deploy. After the first checkpoint, Arroyo restores absolute offsets from its own state and never asks the bridge for them. |
| `auto.offset.reset=error` | Arroyo leaves librdkafka's default `latest`, so an out-of-range offset would jump silently to the log end and lose data. With `error` the partition stalls and Arroyo logs an error instead. See section 7. |
| `isolation.level=read_uncommitted` | Removes the dependency on `last_stable_offset` (section 3). |
| `parallelism = 1` | Until L6 shows ascending partition order. |

## 7. Out-of-range checkpoint, unknown topic, poison records

**Out-of-range.** The gateway answers `OFFSET_OUT_OF_RANGE` only for a negative offset, or after a fetch offset has stayed past the high watermark for 30 seconds of six consecutive attempts on one connection. Before that it answers `NOT_LEADER_OR_FOLLOWER` and the client retries. Net effect for an Arroyo checkpoint ahead of the log (for example after a topic was deleted and recreated, which restarts at offset 0):
- With `auto.offset.reset=error` the partition stalls after about 30 seconds and Arroyo logs the error. The operator stops the job, drops its checkpoint and restarts from `earliest`. **Desk (L11)**
- A recreated topic whose new high watermark is already above the old checkpoint offset is **not detectable** from the Kafka side: Arroyo would read the wrong data silently. Contract: a bridged topic is never deleted and recreated without also dropping the Arroyo checkpoints that read it.

**Unknown topic or zero partitions.** Arroyo does not check the topic error; the pipeline sits idle forever without an error. The conformance job (AD-27) checks that Metadata for each `kafka_name` returns the expected partition count, and the bridge's compose `depends_on` waits for a healthy bridge before Arroyo starts. **Desk (L1, L9)**

**Poison records.** A stored message the gateway cannot map stalls its partition for Kafka consumers (Fetch answers an error at that offset). The only way to create one is a native writer setting `kafka.v` to an unknown version, so the contract reserves the `kafka.` header prefix. Skip or quarantine of an unmappable record is undecided upstream; the platform treats a stall as an incident, not a feature.

**Tombstones.** Arroyo silently skips null payloads.

## 8. Iggy user, permissions and allowlist

One Iggy user serves every Kafka client (AD-27, `[ASSUMPTION: Bridge Permissions]`). The gateway runs every Produce, Fetch and offset operation as this user, whatever the Kafka client. There is no per-client authorization upstream (issue #4518), and SASL is off.

So the allowlist of AD-28 rule 6 is built from three layers, none of them a gateway feature:

| Layer | Effect | Status |
|---|---|---|
| Generated topic map plus `default_stream` pointing at a stream that holds **no topics** | A Kafka name that is not in the map resolves to a topic that does not exist: `UNKNOWN_TOPIC_OR_PARTITION`. Produce and Fetch never create topics. | Desk (L4) |
| Iggy permissions of the bridge user | Read and poll on the three consumed topics; send on `olist/orders_stream` and the benchmark topics; **no** `manage_topics` or `manage_streams`. Iggy then rejects Kafka CreateTopics, DeleteTopics and AlterConfigs from any client. | Desk (L4, L13) |
| Network | Compose network and host loopback only; never published on all interfaces. | Desk (L8) |

**Not enforced upstream, and not claimed by this draft:** a group-name allowlist. The gateway accepts an OffsetCommit for any group id. Its offsets are stored under an Iggy consumer group named `kafka.cg.<group>` on the mapped topic, so a Kafka commit under `cg-arroyo-raw` can never touch a native Iggy group of that name. The cost of the gap is limited to junk consumer groups on the three topics. Closing it is a contribution candidate (decision note, section 5). **Desk (L5)**

Reserved by convention: no native client may use `cg-arroyo-*` or any group name starting with `kafka.cg.`.

Open: the exact Iggy permission set. The gateway also keeps a cluster id in stream `<default stream>.meta` and creates consumer groups on first commit, which may need more than read and send. L13 finds the minimum.

## 9. Listeners, advertised addresses and readiness

The gateway supports one listener per protocol, and a client is sent back only to the advertised address of the listener it connected through. Compose-network clients and a host client therefore need different advertised addresses, and two PLAINTEXT listeners are not possible. The contract uses **two gateway instances** of the same pinned image against the same Iggy server:

| Instance | Reached by | Bind (in container) | Advertised | Published |
|---|---|---|---|---|
| `bridge` | Arroyo (compose network) | `0.0.0.0:9092` | `bridge:9092` | not published |
| `bridge-host` | host-run replayer | `0.0.0.0:9092` | `127.0.0.1:9092` | `127.0.0.1:9092:9092` (loopback only) |

Both instances read the same generated topic map. Neither uses the coordinator log (`IGGY_KAFKA_BROKERS` unset), so they share no state: every client talks to one instance, and offsets live in Iggy. **Desk, untested (L8).**

Image run settings: `user: "65532:65532"` (the upstream image sets no `USER` and runs as root; the binary needs no privileges above port 1024), read-only root filesystem, all capabilities dropped, no password in the compose file (the Iggy password comes from a secret, `IGGY_KAFKA_IGGY_PASSWORD`). **Desk (L9)**

Readiness: the gateway has no HTTP health endpoint. The image ships `netcat`, and the upstream compose file probes `nc -z 127.0.0.1 <port>`. The contract uses that probe as the compose healthcheck. It proves the listener is up, not that Iggy is reachable, so the conformance job adds a real Kafka Metadata call for each `kafka_name` and its partition count. The gateway retries its connection to Iggy for 60 seconds at start and then exits, and a wrong password exits at once; compose `restart: on-failure` plus `depends_on: condition: service_healthy` covers both. **Desk (L9)**

## 10. Delivery semantics (summary)

| Direction | Guarantee |
|---|---|
| Consume | At-least-once. Arroyo resumes from its own checkpoint; with two-phase commit into Delta (NFR-5) the visible result is exactly once. The bridge keeps no delivery state. |
| Produce | A produce response is sent only after Iggy confirms the append. With a `persisted` topic that confirmation includes a stable-storage write on a quorum; with the Iggy default (`replicated`) it does not, and a crash of the Iggy server can lose acknowledged records. A bridge restart never loses an acknowledged record. A timeout after the write (`REQUEST_TIMED_OUT`) can duplicate on retry. |
| Durability of `olist/orders_stream` | **Proposed: create with `persisted`**, so "acknowledged" means durable. The throughput cost is measured in Story 3.11 and is the owner of the benchmark-topic choice (`[OPEN: Benchmark Scope]`). |

## 11. Live verification (all rows open)

A row closes only when the check was run against the pinned images and the result is recorded in the Story 3.13 record. The go and no-go thresholds are in [decision.md](../../_bmad-output/implementation-artifacts/spike-3-13/decision.md) (section 4).

| Check | What it proves | Result |
|---|---|---|
| L1 | Arroyo 0.15.0 reads `orders.intake`, `orders.consolidated` and `olist.orders_stream` from the first offset: count equals the Iggy count, offsets 1:1, partition base 0. | not run |
| L2 | A fresh pipeline starts at a given offset; a restarted pipeline resumes from its checkpoint. | not run |
| L3 | Kill the bridge and Arroyo mid-stream, repeatedly: no loss and no duplicate in Delta. | not run |
| L4 | Produce and Fetch on an unmapped name fail with `UNKNOWN_TOPIC_OR_PARTITION` and create nothing, including when the client sends `allow_auto_topic_creation=true` (as `rskafka` does); CreateTopics, DeleteTopics and AlterConfigs from a Kafka client are rejected. An all-topics Metadata request (as `rskafka` sends) lists only the mapped topics, or its listing is at least harmless. | not run |
| L5 | Group behaviour: FindCoordinator, OffsetCommit and OffsetFetch under `cg-arroyo-*`; a native group of the same name is untouched; confirms that any group name is accepted. | not run |
| L6 | Fetch fields: valid `last_stable_offset`, a usable timestamp on messages written by a native client, partitions in ascending order. | not run |
| L7 | Version fit: gateway image against the Iggy server pin (0.9) and against a post-0.9 build; native SDK 0.11 clients against the server chosen. | not run |
| L8 | Two instances: Arroyo gets `bridge:9092`, the host client gets `127.0.0.1:9092`, and neither is redirected to the wrong address. | not run |
| L9 | Non-root start, read-only filesystem, healthcheck, and a `trivy` scan of the pinned digest. | not run |
| L10 | Produce with `rskafka` 0.6.0 (and `rdkafka` as the oracle): N sent equals N in Iggy; key and all three headers survive; `timeout_ms=30000` is accepted; a kill during produce shows what a retry does and that `replay.run_id` and `replay.seq` identify the duplicate. | not run |
| L11 | Out-of-range: a checkpoint ahead of the log stalls loudly with `auto.offset.reset=error`. | not run |
| L12 | Baseline latency and throughput at the demonstration rate (above 25 msg/s) and at ten times that rate, for OQ-9. | not run |
| L13 | The minimum Iggy permission set for consume, commit and produce without any global `manage_*`. | not run |
| L14 | A Rust crate on the `iggy` SDK 0.11 builds with Rust 1.95 (edition 2024). Needed only for anything built here. | not run |
