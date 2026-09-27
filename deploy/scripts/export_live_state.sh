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

EVEQ_DB_PATH="${EVEQ_DB_PATH:-/srv/eveqtrading/data/eveq.db}"
EVEQ_EXPORT_ROOT="${EVEQ_EXPORT_ROOT:-/srv/eveqtrading/export}"
EVEQ_EXPORT_DB_PATH="${EVEQ_EXPORT_DB_PATH:-${EVEQ_EXPORT_ROOT}/eveq.db}"
EVEQ_EXPORT_MANIFEST_PATH="${EVEQ_EXPORT_MANIFEST_PATH:-${EVEQ_EXPORT_ROOT}/eveq-db-snapshot.json}"
EVEQ_RUNS_DIR="${EVEQ_RUNS_DIR:-/srv/eveqtrading/runs}"
DEFAULT_HISTORY_SYNC_LOCK_FILE="/srv/eveqtrading/tmp/eveq-history-sync-worker.lock"
DEFAULT_REGION_BATCH_LOCK_FILE="/srv/eveqtrading/tmp/eveq-region-batch.lock"
DEFAULT_JITA_PASSIVE_CACHE_WORKER_LOCK_FILE="/srv/eveqtrading/tmp/eveq-jita-passive-cache-worker.lock"
DEFAULT_REGION_ORDER_MIRROR_WORKER_LOCK_FILE="/srv/eveqtrading/tmp/eveq-region-order-mirror-worker.lock"
DEFAULT_REGION_ORDER_MIRROR_WORKER_STATE_PATH="/srv/eveqtrading/data/region-order-mirror-worker-state.json"
DEFAULT_JITA_PASSIVE_CACHE_WORKER_STATE_PATH="/srv/eveqtrading/data/jita-passive-cache-worker-state.json"
DEFAULT_HISTORY_SYNC_WORKER_STATE_PATH="/srv/eveqtrading/data/history-sync-worker-state.json"
DEFAULT_COVERAGE_WORKER_STATE_PATH="/srv/eveqtrading/data/coverage-worker-state.json"

if [[ -z "${EVEQ_LOCK_FILE:-}" ]]; then
  if [[ -e "$DEFAULT_REGION_ORDER_MIRROR_WORKER_LOCK_FILE" || -e "$DEFAULT_REGION_ORDER_MIRROR_WORKER_STATE_PATH" ]]; then
    EVEQ_LOCK_FILE="$DEFAULT_REGION_ORDER_MIRROR_WORKER_LOCK_FILE"
  elif [[ -e "$DEFAULT_JITA_PASSIVE_CACHE_WORKER_LOCK_FILE" || -e "$DEFAULT_JITA_PASSIVE_CACHE_WORKER_STATE_PATH" ]]; then
    EVEQ_LOCK_FILE="$DEFAULT_JITA_PASSIVE_CACHE_WORKER_LOCK_FILE"
  elif [[ -e "$DEFAULT_HISTORY_SYNC_LOCK_FILE" || -e "$DEFAULT_HISTORY_SYNC_WORKER_STATE_PATH" ]]; then
    EVEQ_LOCK_FILE="$DEFAULT_HISTORY_SYNC_LOCK_FILE"
  else
    EVEQ_LOCK_FILE="$DEFAULT_REGION_BATCH_LOCK_FILE"
  fi
fi
EVEQ_LOCK_WAIT_SECONDS="${EVEQ_LOCK_WAIT_SECONDS:-300}"
if [[ -z "${EVEQ_WORKER_STATE_PATH:-}" ]]; then
  if [[ -e "$DEFAULT_REGION_ORDER_MIRROR_WORKER_STATE_PATH" ]]; then
    EVEQ_WORKER_STATE_PATH="$DEFAULT_REGION_ORDER_MIRROR_WORKER_STATE_PATH"
  elif [[ -e "$DEFAULT_JITA_PASSIVE_CACHE_WORKER_STATE_PATH" ]]; then
    EVEQ_WORKER_STATE_PATH="$DEFAULT_JITA_PASSIVE_CACHE_WORKER_STATE_PATH"
  elif [[ -e "$DEFAULT_HISTORY_SYNC_WORKER_STATE_PATH" ]]; then
    EVEQ_WORKER_STATE_PATH="$DEFAULT_HISTORY_SYNC_WORKER_STATE_PATH"
  else
    EVEQ_WORKER_STATE_PATH="$DEFAULT_COVERAGE_WORKER_STATE_PATH"
  fi
fi
PYTHON_BIN="${EVEQ_PYTHON_BIN:-python3}"

require_var "EVEQ_DB_PATH"
require_var "EVEQ_EXPORT_ROOT"
require_var "EVEQ_EXPORT_DB_PATH"
require_var "EVEQ_EXPORT_MANIFEST_PATH"
require_var "EVEQ_RUNS_DIR"
require_var "EVEQ_LOCK_FILE"
require_var "EVEQ_WORKER_STATE_PATH"

mkdir -p "$EVEQ_EXPORT_ROOT"
mkdir -p "$(dirname "$EVEQ_EXPORT_DB_PATH")"
mkdir -p "$(dirname "$EVEQ_EXPORT_MANIFEST_PATH")"
mkdir -p "$(dirname "$EVEQ_LOCK_FILE")"

{
  flock -w "$EVEQ_LOCK_WAIT_SECONDS" 9 || {
    echo "[eveq] could not acquire export lock within ${EVEQ_LOCK_WAIT_SECONDS}s" >&2
    exit 3
  }

  export EVEQ_DB_PATH
  export EVEQ_EXPORT_DB_PATH
  export EVEQ_EXPORT_MANIFEST_PATH
  export EVEQ_RUNS_DIR
  export EVEQ_WORKER_STATE_PATH

  "$PYTHON_BIN" - <<'PY'
import json
import os
import socket
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


db_path = Path(os.environ["EVEQ_DB_PATH"])
snapshot_path = Path(os.environ["EVEQ_EXPORT_DB_PATH"])
manifest_path = Path(os.environ["EVEQ_EXPORT_MANIFEST_PATH"])
runs_dir = Path(os.environ["EVEQ_RUNS_DIR"])
worker_state_path = Path(os.environ["EVEQ_WORKER_STATE_PATH"])

tmp_snapshot_path = snapshot_path.with_name(f".{snapshot_path.name}.tmp")
tmp_manifest_path = manifest_path.with_name(f".{manifest_path.name}.tmp")

if not db_path.exists():
    raise SystemExit(f"Source database does not exist: {db_path}")

tmp_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
tmp_manifest_path.parent.mkdir(parents=True, exist_ok=True)

table_row_counts = {}
recent_batch_run_ids = []
worker_state = None
worker_state_exists = False
worker_state_error = None

src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)
try:
    dst = sqlite3.connect(tmp_snapshot_path)
    try:
        src.backup(dst)
        quick_check = dst.execute("PRAGMA quick_check").fetchone()[0]
        for table_name in (
            "market_history_daily",
            "market_order_snapshot_head",
            "market_order_snapshot_line",
            "scan_run",
            "opportunity_snapshot",
        ):
            try:
                table_row_counts[table_name] = dst.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
            except sqlite3.DatabaseError:
                table_row_counts[table_name] = None
    finally:
        dst.close()
finally:
    src.close()

if quick_check != "ok":
    raise SystemExit(f"SQLite quick_check failed: {quick_check}")

for path in sorted(runs_dir.glob("region-batch-pipeline-*"), reverse=True)[:5]:
    recent_batch_run_ids.append(path.name)

if worker_state_path.exists():
    worker_state_exists = True
    try:
        worker_state = json.loads(worker_state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        worker_state_error = f"invalid worker state json: {exc}"

tmp_snapshot_path.replace(snapshot_path)

manifest = {
    "exported_at_utc": utc_now(),
    "export_host": socket.gethostname(),
    "source_db_path": str(db_path),
    "snapshot_db_path": str(snapshot_path),
    "runs_dir": str(runs_dir),
    "snapshot_size_bytes": snapshot_path.stat().st_size,
    "source_db_mtime_utc": datetime.fromtimestamp(
        db_path.stat().st_mtime, timezone.utc
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "snapshot_db_mtime_utc": datetime.fromtimestamp(
        snapshot_path.stat().st_mtime, timezone.utc
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "db_quick_check": quick_check,
    "table_row_counts": table_row_counts,
    "recent_batch_run_ids": recent_batch_run_ids,
    "worker_state_path": str(worker_state_path),
    "worker_state_exists": worker_state_exists,
    "worker_state_error": worker_state_error,
    "worker_state": worker_state,
}

tmp_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
tmp_manifest_path.replace(manifest_path)

print(json.dumps(manifest, indent=2, sort_keys=True))
PY
} 9>"$EVEQ_LOCK_FILE"
