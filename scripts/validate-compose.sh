#!/usr/bin/env bash
# Automated Docker Compose validation script for FerrousCX master topology (Story 12.1).
set -eu
set -o pipefail 2>/dev/null || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Detect Docker CLI
if command -v docker >/dev/null 2>&1 && docker version >/dev/null 2>&1; then
    DOCKER_CMD="docker"
elif command -v docker.exe >/dev/null 2>&1; then
    DOCKER_CMD="docker.exe"
elif command -v docker >/dev/null 2>&1; then
    DOCKER_CMD="docker"
else
    echo "ERROR: docker command not found"
    exit 1
fi

# Detect Python CLI
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
elif command -v py >/dev/null 2>&1; then
    PYTHON_CMD="py"
else
    echo "ERROR: python command not found"
    exit 1
fi

echo "=========================================================="
echo "  FerrousCX - Compose Topology & Healthcheck Graph Audit  "
echo "=========================================================="

# 1. Validate .env.example
echo ""
echo "[1/4] Checking .env.example & BRIDGE_IMAGE pinning (AD-29)..."
if ! grep -qE "BRIDGE_IMAGE=iggy-kafka-bridge:[a-f0-9]{7}" "${PROJECT_ROOT}/.env.example"; then
    echo "ERROR: BRIDGE_IMAGE in .env.example is not pinned to a submodule short commit SHA"
    exit 1
fi
if grep -qE "BRIDGE_IMAGE=.*:latest" "${PROJECT_ROOT}/.env.example"; then
    echo "ERROR: BRIDGE_IMAGE must never use ':latest' tag (AD-29 violation)"
    exit 1
fi
if grep -qE "PORT_FRONTEND=3000" "${PROJECT_ROOT}/.env.example"; then
    echo "ERROR: PORT_FRONTEND in .env.example must be 5173 (not 3000) to prevent Iggy port conflict"
    exit 1
fi
echo "  [OK] BRIDGE_IMAGE correctly pinned to immutable short SHA."

# 2. Validate Compose Syntax via docker compose config
echo ""
echo "[2/4] Validating Docker Compose syntax (AD-26)..."

if ! grep -qE "^name:\s*ferrous-cx" "${PROJECT_ROOT}/docker-compose.yml"; then
    echo "ERROR: docker-compose.yml is missing top-level 'name: ferrous-cx'"
    exit 1
fi

(cd "${PROJECT_ROOT}" && "$DOCKER_CMD" compose config >/dev/null)
echo "  [OK] docker-compose.yml passed Compose v2 validation."

# 3. Validate Day-0 Hardened Runtime Defense Constraints (AD-24)
echo ""
echo "[3/4] Auditing Day-0 kernel hardening constraints (AD-24)..."
"$PYTHON_CMD" - <<EOF
import re, sys

with open("${PROJECT_ROOT}/docker-compose.yml", "r", encoding="utf-8") as f:
    content = f.read()

required_services = ["order-projection", "ferrous-api", "ferrous-agent", "ferrous-frontend", "ferrous-reverseproxy"]
for svc in required_services:
    pattern = rf"(?ms)^\s{{2}}{svc}:.*?(?=^\s{{2}}[a-zA-Z]|\Z)"
    match = re.search(pattern, content)
    if not match:
        print(f"ERROR: Service '{svc}' not found in manifest")
        sys.exit(1)
    block = match.group(0)
    if not re.search(r"read_only:\s*true", block):
        print(f"ERROR: Service '{svc}' is missing 'read_only: true'")
        sys.exit(1)
    if not re.search(r"cap_drop:\s*-\s*ALL", block):
        print(f"ERROR: Service '{svc}' is missing 'cap_drop: [ALL]'")
        sys.exit(1)
    if not re.search(r"no-new-privileges:true", block):
        print(f"ERROR: Service '{svc}' is missing 'security_opt: [no-new-privileges:true]'")
        sys.exit(1)
    if not re.search(r"tmpfs:", block):
        print(f"ERROR: Service '{svc}' is missing 'tmpfs:' declaration")
        sys.exit(1)
    print(f"  [OK] Service '{svc}' enforces strict Day-0 runtime containment.")
EOF

# 4. Validate Healthcheck Graph Dependencies
echo ""
echo "[4/4] Auditing Healthcheck Graph Dependency Sequence..."
"$PYTHON_CMD" - <<EOF
import re, sys

with open("${PROJECT_ROOT}/docker-compose.yml", "r", encoding="utf-8") as f:
    content = f.read()

checks = [
    (r"(?ms)ferrous-reverseproxy:.*?ferrous-frontend:\s*condition:\s*service_healthy", "ferrous-reverseproxy depends on ferrous-frontend"),
    (r"(?ms)ferrous-reverseproxy:.*?ferrous-iam:\s*condition:\s*service_healthy", "ferrous-reverseproxy depends on ferrous-iam"),
    (r"(?ms)ferrous-frontend:.*?ferrous-iam:\s*condition:\s*service_healthy", "ferrous-frontend depends on ferrous-iam"),
    (r"(?ms)ferrous-frontend:.*?ferrous-api:\s*condition:\s*service_healthy", "ferrous-frontend depends on ferrous-api"),
    (r"(?ms)ferrous-api:.*?iggy-broker:\s*condition:\s*service_healthy", "ferrous-api depends on iggy-broker"),
    (r"(?ms)ferrous-api:.*?sierradb:\s*condition:\s*service_healthy", "ferrous-api depends on sierradb"),
    (r"(?ms)ferrous-api:.*?ferrous-iam:\s*condition:\s*service_healthy", "ferrous-api depends on ferrous-iam"),
    (r"(?ms)order-projection:.*?iggy-broker:\s*condition:\s*service_healthy", "order-projection depends on iggy-broker"),
    (r"(?ms)order-projection:.*?sierradb:\s*condition:\s*service_healthy", "order-projection depends on sierradb"),
    (r"(?ms)ferrous-agent:.*?ferrous-api:\s*condition:\s*service_healthy", "ferrous-agent depends on ferrous-api"),
]

for pat, desc in checks:
    if not re.search(pat, content):
        print(f"ERROR: Dependency check failed: {desc}")
        sys.exit(1)

print("  [OK] Healthcheck graph enforces deterministic startup sequence.")
EOF

echo ""
echo "=========================================================="
echo "  [OK] COMPOSE TOPOLOGY AUDIT COMPLETED CLEANLY!          "
echo "=========================================================="
