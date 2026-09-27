#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${DEBUG:-}" ]]; then
  set -x
fi

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 2
  fi
}

require_var "EVEQ_APP_DIR"
require_var "EVEQ_OUTPUT_ROOT"

PYTHON_BIN="${EVEQ_PYTHON_BIN:-python3}"
OPTIONAL_FLAGS=()

if [[ -n "${EVEQ_LOG_DIR:-}" ]]; then
  OPTIONAL_FLAGS+=(--log-dir "$EVEQ_LOG_DIR")
fi
if [[ -n "${EVEQ_STAGING_DIR:-}" ]]; then
  OPTIONAL_FLAGS+=(--staging-dir "$EVEQ_STAGING_DIR")
fi
if [[ -n "${EVEQ_DB_PATH:-}" ]]; then
  OPTIONAL_FLAGS+=(--db-path "$EVEQ_DB_PATH")
fi
if [[ -n "${EVEQ_RETAIN_RUN_COUNT:-}" ]]; then
  OPTIONAL_FLAGS+=(--retain-run-count "$EVEQ_RETAIN_RUN_COUNT")
fi
if [[ -n "${EVEQ_RETAIN_LOG_COUNT:-}" ]]; then
  OPTIONAL_FLAGS+=(--retain-log-count "$EVEQ_RETAIN_LOG_COUNT")
fi
if [[ -n "${EVEQ_RETAIN_SYNC_REPORT_COUNT:-}" ]]; then
  OPTIONAL_FLAGS+=(--retain-sync-report-count "$EVEQ_RETAIN_SYNC_REPORT_COUNT")
fi
if [[ -n "${EVEQ_STAGING_MAX_AGE_HOURS:-}" ]]; then
  OPTIONAL_FLAGS+=(--staging-max-age-hours "$EVEQ_STAGING_MAX_AGE_HOURS")
fi
if [[ -n "${EVEQ_PREV_DB_MAX_AGE_HOURS:-}" ]]; then
  OPTIONAL_FLAGS+=(--prev-db-max-age-hours "$EVEQ_PREV_DB_MAX_AGE_HOURS")
fi
if [[ "${EVEQ_DRY_RUN:-0}" == "1" ]]; then
  OPTIONAL_FLAGS+=(--dry-run)
fi

RUN_LOG_DIR="${EVEQ_LOG_DIR:-/srv/eveqtrading/logs}"
mkdir -p "$RUN_LOG_DIR"
mkdir -p "$EVEQ_OUTPUT_ROOT"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_path="${RUN_LOG_DIR}/eveq-retention-cleanup-${timestamp}.log"

cd "$EVEQ_APP_DIR"

if [[ -n "${EVEQ_VENV_DIR:-}" && -f "${EVEQ_VENV_DIR}/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${EVEQ_VENV_DIR}/bin/activate"
fi

{
  echo "[eveq] starting retention cleanup at $(date -u --iso-8601=seconds)"
  echo "[eveq] output_root=${EVEQ_OUTPUT_ROOT}"
  echo "[eveq] log_dir=${EVEQ_LOG_DIR:-}"
  echo "[eveq] staging_dir=${EVEQ_STAGING_DIR:-}"
  echo "[eveq] db_path=${EVEQ_DB_PATH:-}"

  "$PYTHON_BIN" -m eveqtrading run-retention-cleanup \
    --output-root "$EVEQ_OUTPUT_ROOT" \
    "${OPTIONAL_FLAGS[@]}"
} | tee -a "$log_path"
