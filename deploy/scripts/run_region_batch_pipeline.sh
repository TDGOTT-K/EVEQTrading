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
require_var "EVEQ_REGION_ID"

PYTHON_BIN="${EVEQ_PYTHON_BIN:-python3}"
TYPE_FLAGS=()

if [[ -n "${EVEQ_TYPE_ID_FILE:-}" ]]; then
  TYPE_FLAGS+=(--type-id-file "$EVEQ_TYPE_ID_FILE")
fi

if [[ -n "${EVEQ_TYPE_IDS:-}" ]]; then
  IFS=',' read -r -a parsed_type_ids <<< "$EVEQ_TYPE_IDS"
  for raw_id in "${parsed_type_ids[@]}"; do
    trimmed_id="$(echo "$raw_id" | xargs)"
    if [[ -n "$trimmed_id" ]]; then
      TYPE_FLAGS+=(--type-id "$trimmed_id")
    fi
  done
fi

if [[ "${EVEQ_ALL_PUBLISHED_MARKET_TYPES:-0}" == "1" ]]; then
  TYPE_FLAGS+=(--all-published-market-types)
fi

if [[ ${#TYPE_FLAGS[@]} -eq 0 ]]; then
  echo "No target selector configured. Set EVEQ_TYPE_ID_FILE, EVEQ_TYPE_IDS, or EVEQ_ALL_PUBLISHED_MARKET_TYPES=1." >&2
  exit 2
fi

OPTIONAL_FLAGS=()

if [[ -n "${EVEQ_LIMIT:-}" ]]; then
  OPTIONAL_FLAGS+=(--limit "$EVEQ_LIMIT")
fi
if [[ -n "${EVEQ_CHUNK_SIZE:-}" ]]; then
  OPTIONAL_FLAGS+=(--chunk-size "$EVEQ_CHUNK_SIZE")
fi
if [[ -n "${EVEQ_MAX_CHUNKS:-}" ]]; then
  OPTIONAL_FLAGS+=(--max-chunks "$EVEQ_MAX_CHUNKS")
fi
if [[ -n "${EVEQ_DATASOURCE:-}" ]]; then
  OPTIONAL_FLAGS+=(--datasource "$EVEQ_DATASOURCE")
fi
if [[ -n "${EVEQ_ESI_BASE_URL:-}" ]]; then
  OPTIONAL_FLAGS+=(--esi-base-url "$EVEQ_ESI_BASE_URL")
fi
if [[ -n "${EVEQ_HISTORY_USER_AGENT:-}" ]]; then
  OPTIONAL_FLAGS+=(--history-user-agent "$EVEQ_HISTORY_USER_AGENT")
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
if [[ "${EVEQ_SKIP_HISTORY_FETCH:-0}" == "1" ]]; then
  OPTIONAL_FLAGS+=(--skip-history-fetch)
fi
if [[ "${EVEQ_SKIP_ORDERS_FETCH:-0}" == "1" ]]; then
  OPTIONAL_FLAGS+=(--skip-orders-fetch)
fi
if [[ -n "${EVEQ_CAPITAL_BUDGET_ISK:-}" ]]; then
  OPTIONAL_FLAGS+=(--capital-budget-isk "$EVEQ_CAPITAL_BUDGET_ISK")
fi
if [[ -n "${EVEQ_TEST_POSITION_ISK:-}" ]]; then
  OPTIONAL_FLAGS+=(--test-position-isk "$EVEQ_TEST_POSITION_ISK")
fi
if [[ -n "${EVEQ_TOP_N:-}" ]]; then
  OPTIONAL_FLAGS+=(--top-n "$EVEQ_TOP_N")
fi
if [[ -n "${EVEQ_STRATEGY_SET:-}" ]]; then
  OPTIONAL_FLAGS+=(--strategy-set "$EVEQ_STRATEGY_SET")
fi
if [[ -n "${EVEQ_PARAMETER_VERSION:-}" ]]; then
  OPTIONAL_FLAGS+=(--parameter-version "$EVEQ_PARAMETER_VERSION")
fi

RUN_LOG_DIR="${EVEQ_LOG_DIR:-/srv/eveqtrading/logs}"
mkdir -p "$RUN_LOG_DIR"
mkdir -p "$(dirname "$EVEQ_DB_PATH")"
mkdir -p "$EVEQ_OUTPUT_ROOT"

LOCK_FILE="${EVEQ_LOCK_FILE:-/srv/eveqtrading/tmp/eveq-region-batch.lock}"
mkdir -p "$(dirname "$LOCK_FILE")"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_path="${RUN_LOG_DIR}/eveq-region-batch-${timestamp}.log"

cd "$EVEQ_APP_DIR"

if [[ -n "${EVEQ_VENV_DIR:-}" && -f "${EVEQ_VENV_DIR}/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${EVEQ_VENV_DIR}/bin/activate"
fi

{
  echo "[eveq] starting region batch pipeline at $(date -u --iso-8601=seconds)"
  echo "[eveq] app_dir=${EVEQ_APP_DIR}"
  echo "[eveq] db_path=${EVEQ_DB_PATH}"
  echo "[eveq] output_root=${EVEQ_OUTPUT_ROOT}"
  echo "[eveq] region_id=${EVEQ_REGION_ID}"

  flock -n 9 || {
    echo "[eveq] another batch pipeline run is already active"
    exit 3
  }

  "$PYTHON_BIN" -m eveqtrading run-region-batch-pipeline \
    --db-path "$EVEQ_DB_PATH" \
    --output-root "$EVEQ_OUTPUT_ROOT" \
    --region-id "$EVEQ_REGION_ID" \
    "${TYPE_FLAGS[@]}" \
    "${OPTIONAL_FLAGS[@]}"
} 9>"$LOCK_FILE" | tee -a "$log_path"
