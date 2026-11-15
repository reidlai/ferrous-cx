#!/usr/bin/env bash
# ==============================================================================
# Cross-Platform Kanidm IAM Dev TLS Cert & Persona Provisioner (Story 13.5)
# ==============================================================================
set -eu
set -o pipefail 2>/dev/null || true
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PATH="${USERPROFILE:-$HOME}/.cargo/bin:${USERPROFILE:-$HOME}/scoop/apps/rustup/current/.cargo/bin:${HOME}/.cargo/bin:$PATH"

if ! command -v cargo >/dev/null 2>&1; then
    echo "[ERROR] Cargo toolchain is required to run setup-iam." >&2
    exit 1
fi

cd "${PROJECT_ROOT}"

ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -DryRun|--dry-run|-d)
            ARGS+=("--dry-run")
            shift
            ;;
        *)
            ARGS+=("$1")
            shift
            ;;
    esac
done

exec cargo run --bin setup-iam -- "${ARGS[@]}"
