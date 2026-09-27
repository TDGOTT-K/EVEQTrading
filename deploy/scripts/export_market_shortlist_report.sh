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
require_var "EVEQ_TOTAL_BUDGET_ISK"

PYTHON_BIN="${EVEQ_PYTHON_BIN:-python3}"
EVEQ_VENV_DIR="${EVEQ_VENV_DIR:-}"
EVEQ_RUNS_ROOT="${EVEQ_RUNS_ROOT:-/srv/eveqtrading/runs}"
EVEQ_LOG_DIR="${EVEQ_LOG_DIR:-/srv/eveqtrading/logs}"
EVEQ_EXPORT_ROOT="${EVEQ_EXPORT_ROOT:-/srv/eveqtrading/export}"
EVEQ_REPORT_EXPORT_DIR="${EVEQ_REPORT_EXPORT_DIR:-${EVEQ_EXPORT_ROOT}/market-shortlist}"
EVEQ_LOCK_PATH="${EVEQ_LOCK_PATH:-/srv/eveqtrading/tmp/eveq-market-shortlist-export.lock}"
EVEQ_RISK_PROFILE="${EVEQ_RISK_PROFILE:-standard}"
EVEQ_TOP_OPPORTUNITIES="${EVEQ_TOP_OPPORTUNITIES:-50}"
EVEQ_TOP_NEAR_MISSES="${EVEQ_TOP_NEAR_MISSES:-10}"
EVEQ_MAX_ROWS="${EVEQ_MAX_ROWS:-20}"
EVEQ_PREFER_LATEST_SELECTED="${EVEQ_PREFER_LATEST_SELECTED:-1}"
EVEQ_SCAN_RUN_ID="${EVEQ_SCAN_RUN_ID:-}"
EVEQ_EXPORT_LATEST_BASENAME="${EVEQ_EXPORT_LATEST_BASENAME:-market_shortlist}"

mkdir -p "$EVEQ_LOG_DIR"
mkdir -p "$EVEQ_RUNS_ROOT"
mkdir -p "$EVEQ_REPORT_EXPORT_DIR"
mkdir -p "$(dirname "$EVEQ_LOCK_PATH")"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
export_dir="${EVEQ_REPORT_EXPORT_DIR}/${EVEQ_EXPORT_LATEST_BASENAME}-${timestamp}"
latest_manifest_path="${EVEQ_REPORT_EXPORT_DIR}/${EVEQ_EXPORT_LATEST_BASENAME}_manifest.json"
latest_markdown_path="${EVEQ_REPORT_EXPORT_DIR}/${EVEQ_EXPORT_LATEST_BASENAME}.md"
latest_json_path="${EVEQ_REPORT_EXPORT_DIR}/${EVEQ_EXPORT_LATEST_BASENAME}.json"
log_path="${EVEQ_LOG_DIR}/eveq-market-shortlist-export-${timestamp}.log"

cd "$EVEQ_APP_DIR"

if [[ -n "$EVEQ_VENV_DIR" && -f "${EVEQ_VENV_DIR}/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${EVEQ_VENV_DIR}/bin/activate"
fi

{
  flock -w "${EVEQ_LOCK_WAIT_SECONDS:-300}" 9

  export EVEQ_EXPORT_DIR="$export_dir"
  export EVEQ_LATEST_MANIFEST_PATH="$latest_manifest_path"
  export EVEQ_LATEST_MARKDOWN_PATH="$latest_markdown_path"
  export EVEQ_LATEST_JSON_PATH="$latest_json_path"
  export EVEQ_HOSTNAME="$(hostname)"

  {
    echo "[eveq] exporting market shortlist report at $(date -u --iso-8601=seconds)"
    echo "[eveq] db_path=${EVEQ_DB_PATH}"
    echo "[eveq] runs_root=${EVEQ_RUNS_ROOT}"
    echo "[eveq] report_export_dir=${EVEQ_REPORT_EXPORT_DIR}"
    echo "[eveq] export_dir=${EVEQ_EXPORT_DIR}"
    echo "[eveq] total_budget_isk=${EVEQ_TOTAL_BUDGET_ISK}"
    echo "[eveq] risk_profile=${EVEQ_RISK_PROFILE}"

    "$PYTHON_BIN" - <<'PY'
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from eveqtrading.db import connect
from eveqtrading.market_shortlist import build_market_shortlist, render_market_shortlist_report


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def env_flag(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() not in {"", "0", "false", "no"}


db_path = Path(os.environ["EVEQ_DB_PATH"])
runs_root = Path(os.environ["EVEQ_RUNS_ROOT"])
log_dir = Path(os.environ["EVEQ_LOG_DIR"])
export_dir = Path(os.environ["EVEQ_EXPORT_DIR"])
latest_manifest_path = Path(os.environ["EVEQ_LATEST_MANIFEST_PATH"])
latest_markdown_path = Path(os.environ["EVEQ_LATEST_MARKDOWN_PATH"])
latest_json_path = Path(os.environ["EVEQ_LATEST_JSON_PATH"])
scan_run_id = os.environ.get("EVEQ_SCAN_RUN_ID") or None
prefer_latest_selected = env_flag("EVEQ_PREFER_LATEST_SELECTED", default=True)
total_budget_isk = float(os.environ["EVEQ_TOTAL_BUDGET_ISK"])
risk_profile = os.environ.get("EVEQ_RISK_PROFILE", "standard")
top_opportunities = int(os.environ.get("EVEQ_TOP_OPPORTUNITIES", "50"))
top_near_misses = int(os.environ.get("EVEQ_TOP_NEAR_MISSES", "10"))
max_rows = int(os.environ.get("EVEQ_MAX_ROWS", "20"))
hostname = os.environ.get("EVEQ_HOSTNAME") or ""

export_dir.mkdir(parents=True, exist_ok=True)
with connect(db_path) as connection:
    shortlist = build_market_shortlist(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=prefer_latest_selected,
        total_budget_isk=total_budget_isk,
        risk_profile=risk_profile,
        top_opportunities=top_opportunities,
        top_near_misses=top_near_misses,
        max_rows=max_rows,
        use_latest_account_budget=False,
        account_snapshot_run_id=None,
    )

archive_markdown_path = export_dir / "market_shortlist.md"
archive_json_path = export_dir / "market_shortlist.json"
archive_manifest_path = export_dir / "market_shortlist_manifest.json"
archive_markdown_path.write_text("\n".join(render_market_shortlist_report(shortlist)) + "\n", encoding="utf-8")
archive_json_path.write_text(json.dumps(shortlist, ensure_ascii=False, indent=2), encoding="utf-8")

manifest = {
    "export_generated_at_utc": utc_now_iso(),
    "server_hostname": hostname,
    "source": {
        "db_path": str(db_path),
        "runs_root": str(runs_root),
        "log_dir": str(log_dir),
        "scan_run_id": shortlist.get("scan_summary", {}).get("scan_run_id"),
        "prefer_latest_selected": prefer_latest_selected,
        "risk_profile": risk_profile,
        "total_budget_isk": total_budget_isk,
        "top_opportunities": top_opportunities,
        "top_near_misses": top_near_misses,
        "max_rows": max_rows,
        "use_latest_account_budget": False,
    },
    "report": {
        "headline": shortlist.get("headline"),
        "displayed_row_count": shortlist.get("displayed_row_count"),
        "selected_row_count_considered": shortlist.get("selected_row_count_considered"),
        "shortlist_generated_at_utc": shortlist.get("shortlist_generated_at_utc"),
        "data_freshness": shortlist.get("data_freshness"),
        "scan_summary": shortlist.get("scan_summary"),
        "budget_summary": shortlist.get("budget_summary"),
    },
    "paths": {
        "archive_dir": str(export_dir),
        "archive_markdown_path": str(archive_markdown_path),
        "archive_json_path": str(archive_json_path),
        "archive_manifest_path": str(archive_manifest_path),
        "latest_markdown_path": str(latest_markdown_path),
        "latest_json_path": str(latest_json_path),
        "latest_manifest_path": str(latest_manifest_path),
    },
}

archive_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
shutil.copy2(archive_markdown_path, latest_markdown_path)
shutil.copy2(archive_json_path, latest_json_path)
shutil.copy2(archive_manifest_path, latest_manifest_path)

print(json.dumps(manifest, ensure_ascii=False, indent=2))
PY
  } | tee -a "$log_path"
} 9>"$EVEQ_LOCK_PATH"
