#!/usr/bin/env bash
# Automated OpenTelemetry 3-Tier developer runtime & Jaeger validator (Story 12.2).
set -euo pipefail

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
echo "  FerrousCX - 3-Tier OpenTelemetry & Jaeger Topology Audit "
echo "=========================================================="

# 1. Validate .env.example templates (both root and docker/)
echo ""
echo "[1/5] Checking .env.example Tier 1/2/3 configuration (AD-23)..."
for env_file in "${PROJECT_ROOT}/.env.example" "${PROJECT_ROOT}/docker/.env.example"; do
    if [ ! -f "$env_file" ]; then
        echo "ERROR: Missing environment template: $env_file"
        exit 1
    fi
    if ! grep -qE "^OTEL_SDK_DISABLED=true" "$env_file"; then
        echo "ERROR: Missing Tier 1 switch 'OTEL_SDK_DISABLED=true' in $env_file"
        exit 1
    fi
    if ! grep -qE "^OTEL_TRACES_EXPORTER=console" "$env_file"; then
        echo "ERROR: Missing Tier 2 switch 'OTEL_TRACES_EXPORTER=console' in $env_file"
        exit 1
    fi
    if ! grep -qE "^OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317" "$env_file"; then
        echo "ERROR: Missing Tier 3 switch 'OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317' in $env_file"
        exit 1
    fi
    if ! grep -qE "^PORT_OTLP_GRPC=4317" "$env_file"; then
        echo "ERROR: Missing PORT_OTLP_GRPC in $env_file"
        exit 1
    fi
    if ! grep -qE "^PORT_OTLP_HTTP=4318" "$env_file"; then
        echo "ERROR: Missing PORT_OTLP_HTTP in $env_file"
        exit 1
    fi
    if ! grep -qE "^PORT_JAEGER_UI=16686" "$env_file"; then
        echo "ERROR: Missing PORT_JAEGER_UI in $env_file"
        exit 1
    fi
    echo "  [OK] Validated 3-Tier telemetry settings and ports in $env_file"
done

# 2. Validate Jaeger Service & Security Constraints in Compose
echo ""
echo "[2/5] Validating Jaeger container definition & Day-0 hardening..."
"$PYTHON_CMD" - <<EOF
import re, sys

with open("${PROJECT_ROOT}/docker/docker-compose.yml", "r", encoding="utf-8") as f:
    content = f.read()

pattern = r"(?ms)^\s{2}jaeger:.*?(?=^[a-zA-Z]|^\s{2}[a-zA-Z]|\Z)"
match = re.search(pattern, content)
if not match:
    print("ERROR: Service 'jaeger' not found in manifest")
    sys.exit(1)

block = match.group(0)

checks = [
    (r"image:\s*jaegertracing/jaeger:2\.3\.0", "Jaeger image jaegertracing/jaeger:2.3.0"),
    (r":4317", "OTLP gRPC port 4317"),
    (r":4318", "OTLP HTTP port 4318"),
    (r":16686", "Jaeger Web UI port 16686"),
    (r"METRICS_STORAGE_TYPE=memory", "METRICS_STORAGE_TYPE=memory"),
    (r"SPAN_STORAGE_TYPE=memory", "SPAN_STORAGE_TYPE=memory"),
    (r"memory:\s*512M", "memory limit 512M"),
    (r"read_only:\s*true", "read_only: true"),
    (r"cap_drop:\s*-\s*ALL", "cap_drop: [ALL]"),
    (r"no-new-privileges:true", "security_opt: [no-new-privileges:true]"),
    (r"(?s)tmpfs:.*?- /tmp:noexec,nosuid,nodev,size=64M", "tmpfs declaration with strict flags"),
    (r"(?s)healthcheck:.*?http://localhost:16686/", "healthcheck probing http://localhost:16686/"),
]

for pat, desc in checks:
    if not re.search(pat, block):
        print(f"ERROR: Jaeger missing {desc}")
        sys.exit(1)

if re.search(r"^\s+volumes:", block):
    print("ERROR: Jaeger container must have ZERO persistent volumes mounted")
    sys.exit(1)

print("  [OK] Jaeger container configured with OTLP gRPC/HTTP, Web UI, RAM storage, memory limits, and Day-0 defense.")
EOF

# 3. Validate Application Service Telemetry Propagation
echo ""
echo "[3/5] Validating OTel environment propagation across application services..."
"$PYTHON_CMD" - <<EOF
import re, sys

with open("${PROJECT_ROOT}/docker/docker-compose.yml", "r", encoding="utf-8") as f:
    content = f.read()

services = ["ferrous-api", "ferrous-agent", "ferrous-reverseproxy"]
for svc in services:
    pattern = rf"(?ms)^\s{{2}}{svc}:.*?(?=^\s{{2}}[a-zA-Z]|\Z)"
    match = re.search(pattern, content)
    if not match:
        print(f"ERROR: Service '{svc}' not found in manifest")
        sys.exit(1)
    block = match.group(0)
    for var in ["OTEL_SDK_DISABLED=", "OTEL_TRACES_EXPORTER=", "OTEL_EXPORTER_OTLP_ENDPOINT="]:
        if var not in block:
            print(f"ERROR: Service '{svc}' missing {var}")
            sys.exit(1)
    print(f"  [OK] Service '{svc}' propagates 3-tier OpenTelemetry environment parameters.")
EOF

# 4. Check Compose Syntax
echo ""
echo "[4/5] Validating Docker Compose syntax with Jaeger..."
(cd "${PROJECT_ROOT}/docker" && "$DOCKER_CMD" compose config >/dev/null)
echo "  [OK] Compose topology docker/docker-compose.yml passes validation cleanly."

(cd "${PROJECT_ROOT}" && "$DOCKER_CMD" compose config >/dev/null)
echo "  [OK] Root docker-compose.yml wrapper passes validation cleanly."

echo ""
echo "=========================================================="
echo "  [OK] OPENTELEMETRY & JAEGER AUDIT COMPLETED CLEANLY!    "
echo "=========================================================="
