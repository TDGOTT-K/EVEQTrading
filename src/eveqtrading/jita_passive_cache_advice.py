from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .advisor import build_market_advice
from .jita_passive_cache_report import resolve_jita_passive_cache_cycle_context


def build_jita_passive_cache_advice(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    cycle_run_id: str | None,
    prefer_latest_selected: bool,
    top_opportunities: int,
    top_near_misses: int,
    risk_profile: str = "standard",
    use_latest_account_budget: bool = False,
    account_snapshot_run_id: str | None = None,
) -> dict[str, Any]:
    cycle_context = resolve_jita_passive_cache_cycle_context(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        cycle_run_id=cycle_run_id,
        prefer_latest_selected=prefer_latest_selected,
        top_candidates=max(top_opportunities, 10),
    )
    scan_run_id = cycle_context["cycle_summary"].get("scan_run_id")
    if not isinstance(scan_run_id, str) or not scan_run_id:
        raise SystemExit("The selected jita passive cache cycle does not contain a scan_run_id.")

    advice = build_market_advice(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=False,
        top_opportunities=top_opportunities,
        top_near_misses=top_near_misses,
        risk_profile=risk_profile,
        use_latest_account_budget=use_latest_account_budget,
        account_snapshot_run_id=account_snapshot_run_id,
    )
    return {
        **advice,
        "cache_cycle_summary": cycle_context["cycle_summary"],
        "cache_cycle_traceability": cycle_context["traceability"],
        "cache_cycle_artifact_health": cycle_context["artifact_health"],
    }
