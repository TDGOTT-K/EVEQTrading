from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .jita_passive_cache_report import resolve_jita_passive_cache_cycle_context
from .report import build_scan_diagnosis


def build_jita_passive_cache_diagnosis(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    cycle_run_id: str | None,
    prefer_latest_selected: bool,
    top_rejection_reasons: int,
    top_near_passes: int,
    top_skipped_reasons: int,
) -> dict[str, Any]:
    cycle_context = resolve_jita_passive_cache_cycle_context(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        cycle_run_id=cycle_run_id,
        prefer_latest_selected=prefer_latest_selected,
        top_candidates=10,
    )
    scan_run_id = cycle_context["cycle_summary"].get("scan_run_id")
    if not isinstance(scan_run_id, str) or not scan_run_id:
        raise SystemExit("The selected jita passive cache cycle does not contain a scan_run_id.")

    diagnosis = build_scan_diagnosis(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=False,
        top_rejection_reasons=top_rejection_reasons,
        top_near_passes=top_near_passes,
        top_skipped_reasons=top_skipped_reasons,
    )
    return {
        **diagnosis,
        "cache_cycle_summary": cycle_context["cycle_summary"],
        "orders_fetch_mode_summary": cycle_context.get("orders_fetch_mode_summary"),
        "cache_cycle_traceability": cycle_context["traceability"],
        "cache_cycle_artifact_health": cycle_context["artifact_health"],
    }
