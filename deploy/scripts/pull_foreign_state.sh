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

quote_join() {
  local quoted=()
  local arg
  for arg in "$@"; do
    quoted+=("$(printf '%q' "$arg")")
  done
  printf '%s ' "${quoted[@]}"
}

count_run_dirs() {
  local target="$1"
  if [[ ! -d "$target" ]]; then
    echo 0
    return
  fi

  find "$target" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' '
}

require_var "EVEQ_REMOTE_HOST"
require_var "EVEQ_SSH_KEY_PATH"

SSH_BIN="${EVEQ_SSH_BIN:-ssh}"
RSYNC_BIN="${EVEQ_RSYNC_BIN:-rsync}"
PYTHON_BIN="${EVEQ_PYTHON_BIN:-python3}"
EVEQ_REMOTE_DB_PATH="${EVEQ_REMOTE_DB_PATH:-/srv/eveqtrading/data/eveq.db}"
EVEQ_REMOTE_EXPORT_ROOT="${EVEQ_REMOTE_EXPORT_ROOT:-/srv/eveqtrading/export}"
EVEQ_REMOTE_RUNS_DIR="${EVEQ_REMOTE_RUNS_DIR:-/srv/eveqtrading/runs}"
EVEQ_REMOTE_EXPORT_SCRIPT="${EVEQ_REMOTE_EXPORT_SCRIPT:-/srv/eveqtrading/app/EVEQTrading/deploy/scripts/export_live_state.sh}"
EVEQ_REMOTE_WORKER_STATE_PATH="${EVEQ_REMOTE_WORKER_STATE_PATH:-}"
EVEQ_LOCAL_DB_PATH="${EVEQ_LOCAL_DB_PATH:-/srv/eveqtrading/data/eveq.db}"
EVEQ_LOCAL_RUNS_DIR="${EVEQ_LOCAL_RUNS_DIR:-/srv/eveqtrading/runs}"
EVEQ_STAGING_DIR="${EVEQ_STAGING_DIR:-/srv/eveqtrading/tmp/foreign-sync}"
EVEQ_LOG_DIR="${EVEQ_LOG_DIR:-/srv/eveqtrading/logs}"
EVEQ_SYNC_LOCK_FILE="${EVEQ_SYNC_LOCK_FILE:-/srv/eveqtrading/tmp/eveq-foreign-sync.lock}"
EVEQ_SYNC_LOCK_WAIT_SECONDS="${EVEQ_SYNC_LOCK_WAIT_SECONDS:-300}"
EVEQ_PULL_RUNS="${EVEQ_PULL_RUNS:-1}"

mkdir -p "$EVEQ_STAGING_DIR"
mkdir -p "$EVEQ_LOG_DIR"
mkdir -p "$(dirname "$EVEQ_LOCAL_DB_PATH")"
mkdir -p "$EVEQ_LOCAL_RUNS_DIR"
mkdir -p "$(dirname "$EVEQ_SYNC_LOCK_FILE")"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
download_db_path="${EVEQ_STAGING_DIR}/eveq.db"
download_manifest_path="${EVEQ_STAGING_DIR}/eveq-db-snapshot.json"
install_db_path="${EVEQ_LOCAL_DB_PATH}.new"
local_manifest_copy_path="${EVEQ_LOCAL_DB_PATH}.snapshot.json"
report_path="${EVEQ_LOG_DIR}/eveq-foreign-sync-${timestamp}.json"
runs_before_sync="0"
runs_after_sync="0"
sync_stage="bootstrap"

ssh_opts=(
  "-i" "$EVEQ_SSH_KEY_PATH"
  "-o" "IdentitiesOnly=yes"
  "-o" "StrictHostKeyChecking=accept-new"
  "-o" "ServerAliveInterval=30"
)

printf -v rsync_rsh '%s' "$(quote_join "$SSH_BIN" "${ssh_opts[@]}")"

remote_cmd=""
remote_cmd+="EVEQ_DB_PATH=$(printf '%q' "$EVEQ_REMOTE_DB_PATH") "
remote_cmd+="EVEQ_EXPORT_ROOT=$(printf '%q' "$EVEQ_REMOTE_EXPORT_ROOT") "
if [[ -n "$EVEQ_REMOTE_WORKER_STATE_PATH" ]]; then
  remote_cmd+="EVEQ_WORKER_STATE_PATH=$(printf '%q' "$EVEQ_REMOTE_WORKER_STATE_PATH") "
fi
remote_cmd+="bash $(printf '%q' "$EVEQ_REMOTE_EXPORT_SCRIPT")"

write_sync_report() {
  export EVEQ_REPORT_PATH="$report_path"
  export EVEQ_LOCAL_DB_PATH
  export EVEQ_LOCAL_MANIFEST_COPY_PATH="$local_manifest_copy_path"
  export EVEQ_DOWNLOADED_MANIFEST_PATH="$download_manifest_path"
  export EVEQ_RUNS_BEFORE_SYNC="$runs_before_sync"
  export EVEQ_RUNS_AFTER_SYNC="$runs_after_sync"
  export EVEQ_REMOTE_HOST
  export EVEQ_PULL_RUNS
  export EVEQ_SYNC_COMPLETED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  "$PYTHON_BIN" - <<'PY'
import json
import os
from pathlib import Path


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _to_int(raw_value: str | None, default: int = 0) -> int:
    try:
        return int(raw_value or default)
    except ValueError:
        return default


report_path = Path(os.environ["EVEQ_REPORT_PATH"])
local_db_path = Path(os.environ["EVEQ_LOCAL_DB_PATH"])
manifest_copy_path = Path(os.environ["EVEQ_LOCAL_MANIFEST_COPY_PATH"])
downloaded_manifest_path = Path(os.environ["EVEQ_DOWNLOADED_MANIFEST_PATH"])

manifest = _read_json(manifest_copy_path)
if manifest is None:
    manifest = _read_json(downloaded_manifest_path)

report = {
    "sync_completed_at_utc": os.environ["EVEQ_SYNC_COMPLETED_AT_UTC"],
    "sync_status": os.environ.get("EVEQ_REPORT_STATUS"),
    "sync_stage": os.environ.get("EVEQ_REPORT_STAGE"),
    "error_exit_code": _to_int(os.environ.get("EVEQ_REPORT_ERROR_EXIT_CODE"), default=0) or None,
    "error_command": os.environ.get("EVEQ_REPORT_ERROR_COMMAND"),
    "error_message": os.environ.get("EVEQ_REPORT_ERROR_MESSAGE"),
    "remote_host": os.environ["EVEQ_REMOTE_HOST"],
    "local_db_path": str(local_db_path),
    "local_db_exists": local_db_path.exists(),
    "local_db_size_bytes": local_db_path.stat().st_size if local_db_path.exists() else None,
    "local_manifest_copy_path": str(manifest_copy_path),
    "local_manifest_copy_exists": manifest_copy_path.exists(),
    "pull_runs_enabled": os.environ["EVEQ_PULL_RUNS"] == "1",
    "runs_before_sync": _to_int(os.environ["EVEQ_RUNS_BEFORE_SYNC"]),
    "runs_after_sync": _to_int(os.environ["EVEQ_RUNS_AFTER_SYNC"]),
    "foreign_worker_status": ((manifest.get("worker_state") or {}).get("status")) if isinstance(manifest, dict) else None,
    "foreign_worker_run_id": ((manifest.get("worker_state") or {}).get("worker_run_id")) if isinstance(manifest, dict) else None,
    "remote_manifest": manifest,
}

report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(report, indent=2, sort_keys=True))
PY
}

on_sync_error() {
  local exit_code="$1"
  local failed_command="$2"
  trap - ERR
  export EVEQ_REPORT_STATUS="failed"
  export EVEQ_REPORT_STAGE="$sync_stage"
  export EVEQ_REPORT_ERROR_EXIT_CODE="$exit_code"
  export EVEQ_REPORT_ERROR_COMMAND="$failed_command"
  export EVEQ_REPORT_ERROR_MESSAGE="foreign sync failed during ${sync_stage}"
  write_sync_report || true
  exit "$exit_code"
}

trap 'on_sync_error $? "$BASH_COMMAND"' ERR

{
  sync_stage="acquire_lock"
  flock -w "$EVEQ_SYNC_LOCK_WAIT_SECONDS" 9 || {
    runs_before_sync="$(count_run_dirs "$EVEQ_LOCAL_RUNS_DIR")"
    runs_after_sync="$runs_before_sync"
    export EVEQ_REPORT_STATUS="skipped_locked"
    export EVEQ_REPORT_STAGE="$sync_stage"
    export EVEQ_REPORT_ERROR_EXIT_CODE="3"
    export EVEQ_REPORT_ERROR_COMMAND="flock -w $EVEQ_SYNC_LOCK_WAIT_SECONDS"
    export EVEQ_REPORT_ERROR_MESSAGE="could not acquire sync lock within ${EVEQ_SYNC_LOCK_WAIT_SECONDS}s"
    write_sync_report
    echo "[eveq] could not acquire sync lock within ${EVEQ_SYNC_LOCK_WAIT_SECONDS}s" >&2
    exit 3
  }

  runs_before_sync="$(count_run_dirs "$EVEQ_LOCAL_RUNS_DIR")"
  runs_after_sync="$runs_before_sync"

  sync_stage="remote_export"
  "$SSH_BIN" "${ssh_opts[@]}" "$EVEQ_REMOTE_HOST" "$remote_cmd"

  sync_stage="download_snapshot"
  "$RSYNC_BIN" -az \
    -e "$rsync_rsh" \
    "$EVEQ_REMOTE_HOST:${EVEQ_REMOTE_EXPORT_ROOT}/eveq.db" \
    "$EVEQ_REMOTE_HOST:${EVEQ_REMOTE_EXPORT_ROOT}/eveq-db-snapshot.json" \
    "$EVEQ_STAGING_DIR/"

  export EVEQ_DOWNLOADED_DB_PATH="$download_db_path"
  export EVEQ_DOWNLOADED_MANIFEST_PATH="$download_manifest_path"

  sync_stage="validate_snapshot"
  "$PYTHON_BIN" - <<'PY'
import json
import os
import sqlite3
from pathlib import Path

db_path = Path(os.environ["EVEQ_DOWNLOADED_DB_PATH"])
manifest_path = Path(os.environ["EVEQ_DOWNLOADED_MANIFEST_PATH"])

if not db_path.exists():
    raise SystemExit(f"Downloaded database does not exist: {db_path}")
if not manifest_path.exists():
    raise SystemExit(f"Downloaded manifest does not exist: {manifest_path}")

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
with sqlite3.connect(db_path) as conn:
    quick_check = conn.execute("PRAGMA quick_check").fetchone()[0]

if quick_check != "ok":
    raise SystemExit(f"Downloaded SQLite quick_check failed: {quick_check}")

print(json.dumps(
    {
        "validated_snapshot_size_bytes": db_path.stat().st_size,
        "validated_quick_check": quick_check,
        "remote_exported_at_utc": manifest.get("exported_at_utc"),
    },
    indent=2,
    sort_keys=True,
))
PY

  runs_after_sync="$runs_before_sync"
  if [[ "$EVEQ_PULL_RUNS" == "1" ]]; then
    sync_stage="download_runs"
    "$RSYNC_BIN" -az \
      -e "$rsync_rsh" \
      "$EVEQ_REMOTE_HOST:${EVEQ_REMOTE_RUNS_DIR}/" \
      "$EVEQ_LOCAL_RUNS_DIR/"
    runs_after_sync="$(count_run_dirs "$EVEQ_LOCAL_RUNS_DIR")"
  fi

  sync_stage="install_snapshot"
  if [[ -f "$EVEQ_LOCAL_DB_PATH" ]]; then
    cp -f "$EVEQ_LOCAL_DB_PATH" "${EVEQ_LOCAL_DB_PATH}.prev"
  fi

  mv -f "$download_db_path" "$install_db_path"
  mv -f "$install_db_path" "$EVEQ_LOCAL_DB_PATH"
  cp -f "$download_manifest_path" "$local_manifest_copy_path"

  sync_stage="write_report"
  trap - ERR
  export EVEQ_REPORT_STATUS="completed"
  export EVEQ_REPORT_STAGE="$sync_stage"
  unset EVEQ_REPORT_ERROR_EXIT_CODE || true
  unset EVEQ_REPORT_ERROR_COMMAND || true
  unset EVEQ_REPORT_ERROR_MESSAGE || true
  write_sync_report
} 9>"$EVEQ_SYNC_LOCK_FILE"
