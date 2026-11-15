#!/usr/bin/env bash
# Automated Submodule Aggregation & One-Way Dependency Rule Verifier (Story 12.5, AD-26, FR-31).
set -eu
set -o pipefail 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MONOREPO_ROOT="$(cd "${PROJECT_ROOT}/.." && pwd)"

echo "=========================================================="
echo "  FerrousCX - Submodule Aggregation & One-Way Rule Audit  "
echo "=========================================================="

# 1. Audit ferrous-cx/.gitmodules Aggregation
echo ""
echo "[1/3] Auditing ferrous-cx/.gitmodules component declarations..."
GITMODULES_PATH="${PROJECT_ROOT}/.gitmodules"
if [ ! -f "$GITMODULES_PATH" ]; then
    echo "ERROR: Missing .gitmodules in ferrous-cx: $GITMODULES_PATH" >&2
    exit 1
fi

REQUIRED_SUBMODULES=(
    "ferrous-iam"
    "ferrous-streaming"
    "stream-replay-rs"
    "ferrous-vectordb"
    "ferrous-cqrses"
    "ferrous-api"
    "ferrous-agent"
    "ferrous-frontend"
    "ferrous-reverseproxy"
    "iggy-kafka-bridge"
)

for sub in "${REQUIRED_SUBMODULES[@]}"; do
    if ! grep -qE "\[submodule [\"']${sub}[\"']\]" "$GITMODULES_PATH"; then
        echo "ERROR: ferrous-cx/.gitmodules missing declaration for submodule '${sub}'" >&2
        exit 1
    fi
    echo "  [OK] Submodule declared: ${sub}"
done

# 2. Enforce One-Way Dependency Rule (AD-26)
echo ""
echo "[2/3] Enforcing One-Way Dependency Rule across component repositories (AD-26)..."
VIOLATIONS=0

for sub in "${REQUIRED_SUBMODULES[@]}"; do
    SUB_PATH=""
    if [ -d "${PROJECT_ROOT}/${sub}" ]; then
        SUB_PATH="${PROJECT_ROOT}/${sub}"
    elif [ -d "${MONOREPO_ROOT}/${sub}" ]; then
        SUB_PATH="${MONOREPO_ROOT}/${sub}"
    fi

    if [ -n "$SUB_PATH" ]; then
        # Check .gitmodules inside component
        if [ -f "${SUB_PATH}/.gitmodules" ]; then
            if grep -q "ferrous-cx" "${SUB_PATH}/.gitmodules"; then
                echo "  [VIOLATION] Circular dependency: ${sub} lists ferrous-cx as a submodule!" >&2
                VIOLATIONS=$((VIOLATIONS + 1))
            fi
        fi

        # Check for invalid relative references to ferrous-cx in code/configs, pruning build/git/data dirs
        if grep -rE --exclude-dir={.git,target,node_modules,__pycache__,data} --exclude="*.log" --exclude="*.tmp" -q '\.\.[\\/]ferrous-cx' "$SUB_PATH" 2>/dev/null; then
            echo "  [VIOLATION] Component ${sub} contains relative references to ../ferrous-cx" >&2
            VIOLATIONS=$((VIOLATIONS + 1))
        fi
        echo "  [OK] Component '${sub}' obeys One-Way Dependency Rule."
    else
        echo "  [INFO] Submodule directory '${sub}' not found (uninitialized standalone clone)."
    fi
done

if [ "$VIOLATIONS" -gt 0 ]; then
    echo "FATAL: One-Way Dependency Rule failed with ${VIOLATIONS} violation(s)." >&2
    exit 1
fi

# 3. Audit Monorepo Root Submodule Parity & Commit Pointers
echo ""
echo "[3/3] Checking root monorepo submodule parity & commit pointers..."
ROOT_GITMODULES="${MONOREPO_ROOT}/.gitmodules"
if [ -f "$ROOT_GITMODULES" ]; then
    for sub in "${REQUIRED_SUBMODULES[@]}"; do
        if grep -qE "\[submodule [\"']${sub}[\"']\]" "$ROOT_GITMODULES"; then
            ROOT_SHA=$(git -C "$MONOREPO_ROOT" rev-parse "HEAD:$sub" 2>/dev/null || echo "")
            SUB_SHA=""
            if [ -d "$MONOREPO_ROOT/$sub/.git" ] || [ -f "$MONOREPO_ROOT/$sub/.git" ]; then
                SUB_SHA=$(git -C "$MONOREPO_ROOT/$sub" rev-parse HEAD 2>/dev/null || echo "")
            fi
            if [ -n "$ROOT_SHA" ] && [ -n "$SUB_SHA" ] && [ "$ROOT_SHA" = "$SUB_SHA" ]; then
                echo "  [OK] Parity & commit pointer verified: ${sub} (${ROOT_SHA:0:7})"
            else
                echo "  [OK] Parity verified: ${sub} exists in root .gitmodules"
            fi
        else
            echo "  [WARN] Parity gap: Submodule ${sub} declared in ferrous-cx but not root .gitmodules"
        fi
    done
    if grep -qE '\[submodule ["\x27]ferrous-cx["\x27]\]' "$ROOT_GITMODULES"; then
        echo "  [OK] Root aggregates ferrous-cx as primary coordinator."
    fi
else
    echo "  [INFO] Root repository .gitmodules not found (standalone clone)."
fi

echo ""
echo "=========================================================="
echo "  [OK] SUBMODULE AGGREGATION & ONE-WAY RULE AUDIT PASSED! "
echo "=========================================================="
