#!/usr/bin/env bash
# ==============================================================================
# Automated Apache Iggy Stream, Topic & Consumer Group Provisioner (Story 13.3)
# ==============================================================================
set -eu
set -o pipefail 2>/dev/null || true
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Ensure cargo is in PATH if in standard user/cargo paths
export PATH="${USERPROFILE:-$HOME}/.cargo/bin:${USERPROFILE:-$HOME}/scoop/apps/rustup/current/.cargo/bin:${HOME}/.cargo/bin:$PATH"

if ! command -v cargo >/dev/null 2>&1; then
    echo "[ERROR] Cargo toolchain is required to run setup-topics." >&2
    exit 1
fi

cd "${PROJECT_ROOT}"

# Normalize legacy shell/PowerShell flags for clap
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -DryRun|--dry-run|-d)
            ARGS+=("--dry-run")
            shift
            ;;
        -IggyUrl|-url|--iggy-url)
            ARGS+=("--url" "$2")
            shift 2
            ;;
        http://*|https://*)
            ARGS+=("--url" "$1")
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

exec cargo run --bin setup-topics -- "${ARGS[@]}"
