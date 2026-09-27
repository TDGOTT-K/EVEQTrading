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
require_var "EVEQ_DB_PATH"
require_var "EVEQ_OUTPUT_ROOT"
require_var "EVEQ_LOCK_PATH"
require_var "EVEQ_STATE_PATH"
require_var "EVEQ_REGION_ID"

PYTHON_BIN="${EVEQ_PYTHON_BIN:-python3}"
OPTIONAL_FLAGS=()

if [[ -n "${EVEQ_TARGET_UNIVERSE:-}" ]]; then
  OPTIONAL_FLAGS+=(--target-universe "$EVEQ_TARGET_UNIVERSE")
fi
if [[ -n "${EVEQ_REFRESH_INTERVAL_MINUTES:-}" ]]; then
  OPTIONAL_FLAGS+=(--refresh-interval-minutes "$EVEQ_REFRESH_INTERVAL_MINUTES")
fi
if [[ -n "${EVEQ_LIMIT:-}" ]]; then
  OPTIONAL_FLAGS+=(--limit "$EVEQ_LIMIT")
fi
if [[ -n "${EVEQ_DATASOURCE:-}" ]]; then
  OPTIONAL_FLAGS+=(--datasource "$EVEQ_DATASOURCE")
fi
if [[ -n "${EVEQ_ESI_BASE_URL:-}" ]]; then
  OPTIONAL_FLAGS+=(--esi-base-url "$EVEQ_ESI_BASE_URL")
fi
if [[ -n "${EVEQ_ORDERS_USER_AGENT:-}" ]]; then
  OPTIONAL_FLAGS+=(--user-agent "$EVEQ_ORDERS_USER_AGENT")
fi
if [[ -n "${EVEQ_REQUEST_TIMEOUT_SECONDS:-}" ]]; then
  OPTIONAL_FLAGS+=(--request-timeout-seconds "$EVEQ_REQUEST_TIMEOUT_SECONDS")
fi
if [[ -n "${EVEQ_CONCURRENT_REQUESTS:-}" ]]; then
  OPTIONAL_FLAGS+=(--concurrent-requests "$EVEQ_CONCURRENT_REQUESTS")
fi
if [[ -n "${EVEQ_MAX_RETRIES:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-retries "$EVEQ_MAX_RETRIES")
fi
if [[ -n "${EVEQ_PAUSE_SECONDS:-}" ]]; then
  OPTIONAL_FLAGS+=(--pause-seconds "$EVEQ_PAUSE_SECONDS")
fi
if [[ -n "${EVEQ_ORDER_TYPE:-}" ]]; then
  OPTIONAL_FLAGS+=(--order-type "$EVEQ_ORDER_TYPE")
fi

RUN_LOG_DIR="${EVEQ_LOG_DIR:-/srv/eveqtrading/logs}"
mkdir -p "$RUN_LOG_DIR"
mkdir -p "$(dirname "$EVEQ_DB_PATH")"
mkdir -p "$EVEQ_OUTPUT_ROOT"
mkdir -p "$(dirname "$EVEQ_LOCK_PATH")"
mkdir -p "$(dirname "$EVEQ_STATE_PATH")"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_path="${RUN_LOG_DIR}/eveq-region-order-mirror-worker-${timestamp}.log"

cd "$EVEQ_APP_DIR"

if [[ -n "${EVEQ_VENV_DIR:-}" && -f "${EVEQ_VENV_DIR}/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${EVEQ_VENV_DIR}/bin/activate"
fi

{
  echo "[eveq] starting region order mirror worker at $(date -u --iso-8601=seconds)"
  echo "[eveq] app_dir=${EVEQ_APP_DIR}"
  echo "[eveq] db_path=${EVEQ_DB_PATH}"
  echo "[eveq] output_root=${EVEQ_OUTPUT_ROOT}"
  echo "[eveq] region_id=${EVEQ_REGION_ID}"
  echo "[eveq] target_universe=${EVEQ_TARGET_UNIVERSE:-history_ready}"
  echo "[eveq] refresh_interval_minutes=${EVEQ_REFRESH_INTERVAL_MINUTES:-30}"
  echo "[eveq] lock_path=${EVEQ_LOCK_PATH}"
  echo "[eveq] state_path=${EVEQ_STATE_PATH}"

  "$PYTHON_BIN" -m eveqtrading run-region-order-mirror-worker-once \
    --db-path "$EVEQ_DB_PATH" \
    --output-root "$EVEQ_OUTPUT_ROOT" \
    --lock-path "$EVEQ_LOCK_PATH" \
    --state-path "$EVEQ_STATE_PATH" \
    --region-id "$EVEQ_REGION_ID" \
    "${OPTIONAL_FLAGS[@]}"
} | tee -a "$log_path"
