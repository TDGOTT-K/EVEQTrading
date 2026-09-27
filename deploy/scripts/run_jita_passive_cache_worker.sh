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

if [[ -n "${EVEQ_ORDERS_CHUNK_SIZE:-}" ]]; then
  OPTIONAL_FLAGS+=(--orders-chunk-size "$EVEQ_ORDERS_CHUNK_SIZE")
fi
if [[ -n "${EVEQ_MAX_ORDERS_CHUNKS:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-orders-chunks "$EVEQ_MAX_ORDERS_CHUNKS")
fi
if [[ -n "${EVEQ_LIMIT:-}" ]]; then
  OPTIONAL_FLAGS+=(--limit "$EVEQ_LIMIT")
fi
if [[ -n "${EVEQ_ORDERS_CACHE_PROFILE:-}" ]]; then
  OPTIONAL_FLAGS+=(--orders-cache-profile "$EVEQ_ORDERS_CACHE_PROFILE")
fi
if [[ -n "${EVEQ_DATASOURCE:-}" ]]; then
  OPTIONAL_FLAGS+=(--datasource "$EVEQ_DATASOURCE")
fi
if [[ -n "${EVEQ_ESI_BASE_URL:-}" ]]; then
  OPTIONAL_FLAGS+=(--esi-base-url "$EVEQ_ESI_BASE_URL")
fi
if [[ -n "${EVEQ_ORDERS_USER_AGENT:-}" ]]; then
  OPTIONAL_FLAGS+=(--orders-user-agent "$EVEQ_ORDERS_USER_AGENT")
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
if [[ -n "${EVEQ_ORDERS_FETCH_MODE:-}" ]]; then
  OPTIONAL_FLAGS+=(--orders-fetch-mode "$EVEQ_ORDERS_FETCH_MODE")
fi
if [[ -n "${EVEQ_CAPITAL_BUDGET_ISK:-}" ]]; then
  OPTIONAL_FLAGS+=(--capital-budget-isk "$EVEQ_CAPITAL_BUDGET_ISK")
fi
if [[ -n "${EVEQ_TOP_N:-}" ]]; then
  OPTIONAL_FLAGS+=(--top-n "$EVEQ_TOP_N")
fi
if [[ -n "${EVEQ_MAX_FLIP_UNITS_CV_30D:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-flip-units-cv-30d "$EVEQ_MAX_FLIP_UNITS_CV_30D")
fi
if [[ -n "${EVEQ_MAX_FLIP_UNITS_CV_7D:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-flip-units-cv-7d "$EVEQ_MAX_FLIP_UNITS_CV_7D")
fi
if [[ -n "${EVEQ_MIN_DAILY_CAPACITY_ISK_30D:-}" ]]; then
  OPTIONAL_FLAGS+=(--min-daily-capacity-isk-30d "$EVEQ_MIN_DAILY_CAPACITY_ISK_30D")
fi
if [[ -n "${EVEQ_STRATEGY_SET:-}" ]]; then
  OPTIONAL_FLAGS+=(--strategy-set "$EVEQ_STRATEGY_SET")
fi
if [[ -n "${EVEQ_PARAMETER_VERSION:-}" ]]; then
  OPTIONAL_FLAGS+=(--parameter-version "$EVEQ_PARAMETER_VERSION")
fi
if [[ -n "${EVEQ_FAILURE_BACKOFF_SECONDS:-}" ]]; then
  OPTIONAL_FLAGS+=(--failure-backoff-seconds "$EVEQ_FAILURE_BACKOFF_SECONDS")
fi
if [[ -n "${EVEQ_MAX_RUNTIME_SECONDS:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-runtime-seconds "$EVEQ_MAX_RUNTIME_SECONDS")
fi
if [[ -n "${EVEQ_MAX_CYCLES_PER_RUN:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-cycles-per-run "$EVEQ_MAX_CYCLES_PER_RUN")
fi

RUN_LOG_DIR="${EVEQ_LOG_DIR:-/srv/eveqtrading/logs}"
mkdir -p "$RUN_LOG_DIR"
mkdir -p "$(dirname "$EVEQ_DB_PATH")"
mkdir -p "$EVEQ_OUTPUT_ROOT"
mkdir -p "$(dirname "$EVEQ_LOCK_PATH")"
mkdir -p "$(dirname "$EVEQ_STATE_PATH")"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_path="${RUN_LOG_DIR}/eveq-jita-passive-cache-worker-${timestamp}.log"

cd "$EVEQ_APP_DIR"

if [[ -n "${EVEQ_VENV_DIR:-}" && -f "${EVEQ_VENV_DIR}/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${EVEQ_VENV_DIR}/bin/activate"
fi

{
  echo "[eveq] starting jita passive cache worker at $(date -u --iso-8601=seconds)"
  echo "[eveq] app_dir=${EVEQ_APP_DIR}"
  echo "[eveq] db_path=${EVEQ_DB_PATH}"
  echo "[eveq] output_root=${EVEQ_OUTPUT_ROOT}"
  echo "[eveq] region_id=${EVEQ_REGION_ID}"
  echo "[eveq] lock_path=${EVEQ_LOCK_PATH}"
  echo "[eveq] state_path=${EVEQ_STATE_PATH}"

  "$PYTHON_BIN" -m eveqtrading run-jita-passive-cache-worker-once \
    --db-path "$EVEQ_DB_PATH" \
    --output-root "$EVEQ_OUTPUT_ROOT" \
    --lock-path "$EVEQ_LOCK_PATH" \
    --state-path "$EVEQ_STATE_PATH" \
    --region-id "$EVEQ_REGION_ID" \
    "${OPTIONAL_FLAGS[@]}"
} | tee -a "$log_path"
