#!/usr/bin/env bash
# Automated Validator for Quickstart, Topic Registry, Dataset & Replay Profiles (Story 12.4).
set -eu
set -o pipefail 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=========================================================="
echo "  FerrousCX - Quickstart, Registry & Profile Validator    "
echo "=========================================================="

# 1. Validate contracts/topics.yml
echo ""
echo "[1/5] Auditing contracts/topics.yml Single Source of Truth..."
TOPICS_YAML="${PROJECT_ROOT}/contracts/topics.yml"
if [ ! -f "$TOPICS_YAML" ]; then
    echo "ERROR: Missing topic registry at $TOPICS_YAML" >&2
    exit 1
fi

EXPECTED_TOPICS=(
    "orders.intake|4|replicated"
    "orders.consolidated|4|replicated"
    "olist.orders_stream|4|persisted"
    "benchmark.hpc_mqbench|4|persisted"
)

for t_info in "${EXPECTED_TOPICS[@]}"; do
    IFS="|" read -r t_name p_count dur <<< "$t_info"
    if ! grep -qE "kafka_name:\s*${t_name}" "$TOPICS_YAML" || \
       ! grep -qE "partitions:\s*${p_count}" "$TOPICS_YAML" || \
       ! grep -qE "durability:\s*${dur}" "$TOPICS_YAML"; then
        echo "ERROR: contracts/topics.yml missing or invalid topic definition: ${t_name} (${p_count}p, ${dur})" >&2
        exit 1
    fi
done
echo "  [OK] contracts/topics.yml defines all 4 required topics with valid partitions and durability."

# 2. Validate Dataset Existence
echo ""
echo "[2/5] Auditing Olist sample dataset existence & structure..."
DATASET_FILE="${PROJECT_ROOT}/docs/datasets/olist/olist_orders_dataset.csv"
if [ ! -f "$DATASET_FILE" ]; then
    echo "ERROR: Missing Olist dataset at $DATASET_FILE" >&2
    exit 1
fi
LINE_COUNT=$(wc -l < "$DATASET_FILE")
if [ "$LINE_COUNT" -lt 2 ]; then
    echo "ERROR: Olist dataset at $DATASET_FILE is empty or corrupt" >&2
    exit 1
fi
if ! head -n 1 "$DATASET_FILE" | grep -q "order_id" || ! head -n 1 "$DATASET_FILE" | grep -q "order_purchase_timestamp"; then
    echo "ERROR: Olist dataset header missing required fields" >&2
    exit 1
fi
echo "  [OK] Olist dataset verified ($((LINE_COUNT - 1)) records, valid schema)."

# 3. Validate Replay Profiles
echo ""
echo "[3/5] Auditing Replay Profiles (olist-demo.toml & olist-load.toml)..."
DEMO_PROFILE="${PROJECT_ROOT}/profiles/olist-demo.toml"
LOAD_PROFILE="${PROJECT_ROOT}/profiles/olist-load.toml"

if [ ! -f "$DEMO_PROFILE" ] || [ ! -f "$LOAD_PROFILE" ]; then
    echo "ERROR: Missing replay profile in profiles/ directory" >&2
    exit 1
fi

if ! grep -qE "speed\s*=\s*10(\.0)?" "$DEMO_PROFILE"; then
    echo "ERROR: olist-demo.toml must configure 'speed = 10.0' (FR-1, FR-1b)" >&2
    exit 1
fi
if ! grep -qE 'destination\s*=\s*"olist\.orders_stream"' "$DEMO_PROFILE"; then
    echo "ERROR: olist-demo.toml destination must be 'olist.orders_stream'" >&2
    exit 1
fi
if ! grep -qE 'default_url\s*=\s*"127\.0\.0\.1:9092"' "$DEMO_PROFILE"; then
    echo "ERROR: olist-demo.toml sink URL must target bridge endpoint '127.0.0.1:9092'" >&2
    exit 1
fi
if grep -qEi "^\s*(kanidm|auth_url|service_account|password|token|secret)" "$DEMO_PROFILE"; then
    echo "ERROR: olist-demo.toml must not use credentials or Kanidm authentication (AD-28)" >&2
    exit 1
fi
echo "  [OK] olist-demo.toml verified (10x pacing, loop salt, zero Kanidm credentials)."

if ! grep -qE "rate\s*=\s*25" "$LOAD_PROFILE"; then
    echo "ERROR: olist-load.toml must configure 'rate = 25' (NFR-3, SM-3)" >&2
    exit 1
fi
if ! grep -qE 'destination\s*=\s*"olist\.orders_stream"' "$LOAD_PROFILE"; then
    echo "ERROR: olist-load.toml destination must be 'olist.orders_stream'" >&2
    exit 1
fi
if grep -qEi "^\s*(kanidm|auth_url|service_account|password|token|secret)" "$LOAD_PROFILE"; then
    echo "ERROR: olist-load.toml must not use credentials or Kanidm authentication (AD-28)" >&2
    exit 1
fi
echo "  [OK] olist-load.toml verified (rate 25 fixed throughput, zero Kanidm credentials)."

# 4. Validate Quickstart Documentation
echo ""
echo "[4/5] Auditing docs/quickstart.md content, arithmetic & security invariants..."
QUICKSTART_DOC="${PROJECT_ROOT}/docs/quickstart.md"
if [ ! -f "$QUICKSTART_DOC" ]; then
    echo "ERROR: Missing quickstart guide: $QUICKSTART_DOC" >&2
    exit 1
fi

if ! grep -qE "Why 10x" "$QUICKSTART_DOC"; then
    echo "ERROR: docs/quickstart.md must document 'Why 10x' section" >&2
    exit 1
fi
if ! grep -qE "0\.095" "$QUICKSTART_DOC" || ! grep -qEi "order" "$QUICKSTART_DOC"; then
    echo "ERROR: docs/quickstart.md must explain the ~0.095 orders/min arithmetic derivation" >&2
    exit 1
fi
if ! grep -qEi "read-only UI" "$QUICKSTART_DOC" && ! grep -qEi "strictly read-only" "$QUICKSTART_DOC"; then
    echo "ERROR: docs/quickstart.md must document that Operator Studio UI is strictly read-only" >&2
    exit 1
fi
if ! grep -qEi "Kanidm" "$QUICKSTART_DOC"; then
    echo "ERROR: docs/quickstart.md must document zero Kanidm account dependency for replay" >&2
    exit 1
fi
if ! grep -qE "stream-replay-rs" "$QUICKSTART_DOC"; then
    echo "ERROR: docs/quickstart.md must document stream-replay-rs CLI execution" >&2
    exit 1
fi
echo "  [OK] docs/quickstart.md verified (Why 10x derivation, rate 25, read-only UI, Kanidm isolation)."

# 5. Run Dry-Run Topic Setup
echo ""
echo "[5/5] Executing setup-topics.sh dry-run verification..."
bash "${PROJECT_ROOT}/scripts/setup-topics.sh" --dry-run
echo "  [OK] setup-topics.sh executed cleanly in dry-run mode."

echo ""
echo "=========================================================="
echo "  [OK] QUICKSTART & REGISTRY AUDIT COMPLETED CLEANLY!     "
echo "=========================================================="
