from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .report import resolve_runtime_roots


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _coerce_path(raw_value: Any) -> Path | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    return Path(raw_value)


def _count_rows(values: list[str], *, key_name: str, label_name: str) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return [
        {key_name: key, label_name: key, "count": count}
        for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _describe_fetch_mode_reason(
    *,
    requested_fetch_mode: str | None,
    effective_fetch_mode: str | None,
    reason: str | None,
) -> str:
    reason_code = str(reason or "")
    if reason_code == "target_set_small_or_predicted_pages_low":
        return "remote target set was still small/light, so per-type fetch stayed cheaper"
    if reason_code == "region_total_pages_not_better_than_predicted_per_type_pages":
        return "region-wide page probe was not cheaper than pulling targets one by one"
    if reason_code == "region_total_pages_below_predicted_per_type_pages":
        return "region-wide page sweep was cheaper than the predicted per-type page cost"
    if reason_code == "region_probe_failed_fallback_to_per_type":
        return "region-wide probe failed, so the cycle fell back to per-type fetch"
    if reason_code == "no_remote_targets":
        return "this cycle had no remote-due order targets"
    if requested_fetch_mode == "per_type" and effective_fetch_mode == "per_type":
        return "fetch mode was explicitly pinned to per_type"
    if requested_fetch_mode == "region_pages" and effective_fetch_mode == "region_pages":
        return "fetch mode was explicitly pinned to region_pages"
    if requested_fetch_mode == "auto" and effective_fetch_mode == "per_type":
        return "auto mode kept per_type"
    if requested_fetch_mode == "auto" and effective_fetch_mode == "region_pages":
        return "auto mode promoted the cycle to region_pages"
    if effective_fetch_mode:
        return f"effective mode settled on {effective_fetch_mode}"
    return "fetch mode reason was not recorded"


def _summarize_fetch_mode_narrative(summary: dict[str, Any]) -> str:
    if not summary.get("available"):
        return str(summary.get("message") or "order fetch mode trace is unavailable")

    if int(summary.get("selected_remote_target_count") or 0) == 0:
        return "This cycle had no remote-due order targets, so no remote fetch mode was exercised."

    dominant_mode = str(summary.get("dominant_effective_fetch_mode") or "unknown")
    requested_mode = str(summary.get("requested_fetch_mode") or "unknown")
    reason_rows = list(summary.get("decision_reason_counts") or [])
    leading_reason = dict(reason_rows[0] or {}) if reason_rows else {}
    reason_text = str(leading_reason.get("description") or "")
    orders_run_count = int(summary.get("orders_run_count") or 0)
    if orders_run_count <= 1:
        return (
            f"This cycle used `{dominant_mode}`"
            + (f" under requested mode `{requested_mode}`" if requested_mode else "")
            + (f" because {reason_text}." if reason_text else ".")
        )
    effective_rows = list(summary.get("effective_fetch_mode_counts") or [])
    if len(effective_rows) == 1:
        return (
            f"All {orders_run_count} order fetch run(s) used `{dominant_mode}`"
            + (f" because {reason_text}." if reason_text else ".")
        )
    effective_bits = ", ".join(
        f"{row.get('fetch_mode')} x{row.get('count')}" for row in effective_rows[:3] if row.get("fetch_mode")
    )
    return (
        f"This cycle mixed fetch modes across chunks; dominant mode was `{dominant_mode}` "
        f"({effective_bits})."
    )


def _build_orders_fetch_mode_summary(
    payload: dict[str, Any],
) -> dict[str, Any]:
    order_refresh_summary = payload.get("order_refresh_summary")
    if not isinstance(order_refresh_summary, dict):
        return {
            "available": False,
            "message": "cache cycle does not contain order refresh summary",
        }

    requested_fetch_mode = str(
        (((order_refresh_summary.get("config") or {}).get("orders_fetch") or {}).get("fetch_mode") or "")
    ) or None
    batch_run_id = order_refresh_summary.get("orders_batch_run_id")
    batch_run_dir = _coerce_path(order_refresh_summary.get("orders_batch_run_dir"))
    if batch_run_dir is None:
        return {
            "available": False,
            "requested_fetch_mode": requested_fetch_mode,
            "batch_run_id": batch_run_id,
            "selected_remote_target_count": int(order_refresh_summary.get("selected_remote_target_count") or 0),
            "executed_remote_target_count": int(order_refresh_summary.get("executed_remote_target_count") or 0),
            "message": "no orders batch run was produced in this cache cycle",
        }

    batch_json_path = batch_run_dir / "orders_batch_fetch.json"
    batch_payload = _read_json(batch_json_path)
    if batch_payload is None:
        return {
            "available": False,
            "requested_fetch_mode": requested_fetch_mode,
            "batch_run_id": batch_run_id,
            "batch_run_dir": str(batch_run_dir),
            "batch_json_path": str(batch_json_path),
            "message": "orders batch artifact is missing",
        }

    effective_modes: list[str] = []
    decision_reasons: list[str] = []
    sample_orders_runs: list[dict[str, Any]] = []
    orders_fetch_run_paths_sample: list[str] = []
    orders_fetch_trace_paths_sample: list[str] = []
    region_probe_used_count = 0
    region_pages_shared_request_count = 0
    total_pages_fetched = 0
    orders_run_count = 0

    for chunk in list(batch_payload.get("chunks") or []):
        if not isinstance(chunk, dict):
            continue
        orders_run_dir = _coerce_path(chunk.get("orders_run_dir"))
        if orders_run_dir is None:
            continue
        fetch_run_path = orders_run_dir / "orders_fetch_run.json"
        fetch_trace_path = orders_run_dir / "orders_fetch_trace.json"
        fetch_run = _read_json(fetch_run_path)
        fetch_trace = _read_json(fetch_trace_path)
        if fetch_run is None and fetch_trace is None:
            continue

        orders_run_count += 1
        if len(orders_fetch_run_paths_sample) < 10:
            orders_fetch_run_paths_sample.append(str(fetch_run_path))
        if len(orders_fetch_trace_paths_sample) < 10:
            orders_fetch_trace_paths_sample.append(str(fetch_trace_path))

        effective_fetch_mode = str(
            (fetch_run or {}).get("effective_fetch_mode")
            or (fetch_trace or {}).get("effective_fetch_mode")
            or "unknown"
        )
        effective_modes.append(effective_fetch_mode)
        decision = dict(
            (fetch_run or {}).get("fetch_mode_decision")
            or (fetch_trace or {}).get("fetch_mode_decision")
            or {}
        )
        reason = str(decision.get("reason") or "")
        if reason:
            decision_reasons.append(reason)
        if bool(decision.get("region_first_page_probe_used")):
            region_probe_used_count += 1

        shared_remote_trace = dict((fetch_trace or {}).get("shared_remote_trace") or {})
        if str(shared_remote_trace.get("request_scope") or "") == "region_pages":
            region_pages_shared_request_count += 1

        pages_fetched = int(
            (fetch_run or {}).get("pages_fetched")
            or ((chunk.get("fetch_counts") or {}).get("pages_fetched") or 0)
        )
        total_pages_fetched += pages_fetched

        if len(sample_orders_runs) < 5:
            sample_orders_runs.append(
                {
                    "orders_run_id": (fetch_run or {}).get("run_id") or chunk.get("orders_run_id"),
                    "orders_run_dir": str(orders_run_dir),
                    "requested_fetch_mode": requested_fetch_mode,
                    "effective_fetch_mode": effective_fetch_mode,
                    "decision_reason": reason or None,
                    "decision_reason_description": _describe_fetch_mode_reason(
                        requested_fetch_mode=requested_fetch_mode,
                        effective_fetch_mode=effective_fetch_mode,
                        reason=reason or None,
                    ),
                    "target_count_requested": (fetch_run or {}).get("type_count_requested"),
                    "target_count_success": (fetch_run or {}).get("type_count_success"),
                    "pages_fetched": pages_fetched,
                    "region_first_page_probe_used": bool(decision.get("region_first_page_probe_used")),
                    "predicted_per_type_pages": decision.get("predicted_per_type_pages"),
                    "region_total_pages": decision.get("region_total_pages"),
                    "shared_request_scope": shared_remote_trace.get("request_scope"),
                }
            )

    effective_fetch_mode_counts = _count_rows(
        effective_modes,
        key_name="fetch_mode",
        label_name="fetch_mode_label",
    )
    decision_reason_counts = _count_rows(
        decision_reasons,
        key_name="reason",
        label_name="reason_label",
    )
    for row in decision_reason_counts:
        row["description"] = _describe_fetch_mode_reason(
            requested_fetch_mode=requested_fetch_mode,
            effective_fetch_mode=str(
                effective_fetch_mode_counts[0]["fetch_mode"] if effective_fetch_mode_counts else ""
            )
            or None,
            reason=str(row.get("reason") or "") or None,
        )

    summary = {
        "available": True,
        "requested_fetch_mode": requested_fetch_mode,
        "batch_run_id": batch_payload.get("batch_run_id") or batch_run_id,
        "batch_run_dir": str(batch_run_dir),
        "batch_chunk_count": len(list(batch_payload.get("chunks") or [])),
        "orders_run_count": orders_run_count,
        "selected_remote_target_count": int(order_refresh_summary.get("selected_remote_target_count") or 0),
        "executed_remote_target_count": int(order_refresh_summary.get("executed_remote_target_count") or 0),
        "pages_fetched": total_pages_fetched,
        "dominant_effective_fetch_mode": (
            str(effective_fetch_mode_counts[0]["fetch_mode"]) if effective_fetch_mode_counts else None
        ),
        "effective_fetch_mode_counts": effective_fetch_mode_counts,
        "decision_reason_counts": decision_reason_counts,
        "region_probe_used_count": region_probe_used_count,
        "region_pages_shared_request_count": region_pages_shared_request_count,
        "sample_orders_runs": sample_orders_runs,
        "traceability": {
            "orders_batch_fetch_json_path": str(batch_json_path),
            "orders_fetch_run_paths_sample": orders_fetch_run_paths_sample,
            "orders_fetch_trace_paths_sample": orders_fetch_trace_paths_sample,
        },
    }
    summary["narrative"] = _summarize_fetch_mode_narrative(summary)
    return summary


def _fetch_cycle_rows(
    connection: sqlite3.Connection,
    *,
    cycle_run_id: str | None,
    limit: int = 20,
) -> list[sqlite3.Row]:
    sql = (
        "SELECT fetch_run_id, started_at, finished_at, output_dir, details_json, scope_json, status "
        "FROM data_fetch_run WHERE fetch_kind = 'jita_passive_cache_cycle' AND status = 'completed'"
    )
    params: list[Any] = []
    if cycle_run_id is not None:
        sql += " AND fetch_run_id = ?"
        params.append(cycle_run_id)
    sql += " ORDER BY started_at DESC"
    if cycle_run_id is None:
        sql += f" LIMIT {int(limit)}"
    return list(connection.execute(sql, params).fetchall())


def _candidate_count_selected(row: sqlite3.Row) -> int:
    try:
        details = json.loads(str(row["details_json"]))
    except json.JSONDecodeError:
        return 0
    if not isinstance(details, dict):
        return 0
    scan_summary = details.get("scan_summary")
    if not isinstance(scan_summary, dict):
        return 0
    return int(scan_summary.get("candidate_count_selected") or 0)


def _resolve_cycle_row(
    rows: list[sqlite3.Row],
    *,
    cycle_run_id: str | None,
    prefer_latest_selected: bool,
) -> sqlite3.Row:
    if not rows:
        raise SystemExit("No completed jita passive cache cycle was found.")
    if cycle_run_id is not None:
        return rows[0]
    if prefer_latest_selected:
        for row in rows:
            if _candidate_count_selected(row) > 0:
                return row
    return rows[0]


def _resolve_cycle_output_dir(row: sqlite3.Row, runs_root: Path) -> Path:
    raw_output_dir = _coerce_path(row["output_dir"])
    if raw_output_dir is not None and raw_output_dir.exists():
        return raw_output_dir
    fallback = runs_root / str(row["fetch_run_id"])
    if fallback.exists():
        return fallback
    return raw_output_dir or fallback


def _artifact_health(paths: dict[str, str]) -> dict[str, Any]:
    health: dict[str, Any] = {}
    missing_required: list[str] = []
    for key, raw_path in paths.items():
        exists = bool(raw_path) and Path(raw_path).exists()
        health[f"{key}_exists"] = exists
        if key in {
            "cycle_json_path",
            "cycle_summary_path",
            "scan_summary_path",
            "scan_candidate_table_path",
            "scan_calc_trace_path",
        } and not exists:
            missing_required.append(key)
    health["missing_required"] = missing_required
    health["report_ready"] = not missing_required
    return health


def resolve_jita_passive_cache_cycle_context(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    cycle_run_id: str | None,
    prefer_latest_selected: bool,
    top_candidates: int,
) -> dict[str, Any]:
    effective_runs_root, _ = resolve_runtime_roots(
        db_path=db_path,
        runs_root=runs_root,
        log_dir=None,
    )
    selected_row = _resolve_cycle_row(
        _fetch_cycle_rows(connection, cycle_run_id=cycle_run_id),
        cycle_run_id=cycle_run_id,
        prefer_latest_selected=prefer_latest_selected,
    )
    output_dir = _resolve_cycle_output_dir(selected_row, effective_runs_root)
    cycle_json_path = output_dir / "jita_passive_cache_cycle.json"
    cycle_summary_path = output_dir / "summary.md"
    payload = _read_json(cycle_json_path)
    if payload is None:
        raise SystemExit("The selected jita passive cache cycle is missing jita_passive_cache_cycle.json.")

    files = payload.get("files")
    if not isinstance(files, dict):
        files = {}
    traceability = {
        "cycle_output_dir": str(output_dir),
        "cycle_json_path": str(cycle_json_path),
        "cycle_summary_path": str(cycle_summary_path),
        "order_refresh_cycle_json_path": str(
            ((_coerce_path(payload.get("order_refresh_cycle_run_dir")) or Path()) / "order_refresh_cycle.json")
            if payload.get("order_refresh_cycle_run_dir")
            else ""
        ),
        "order_refresh_cycle_summary_path": str(files.get("order_refresh_cycle_summary_path") or ""),
        "scan_summary_path": str(files.get("scan_summary_path") or ""),
        "scan_analysis_run_path": str(files.get("scan_analysis_run_path") or ""),
        "scan_calc_trace_path": str(files.get("scan_calc_trace_path") or ""),
        "scan_candidate_table_path": str(files.get("scan_candidate_table_path") or ""),
        "scan_skipped_table_path": str(files.get("scan_skipped_table_path") or ""),
    }
    top_candidates_preview = list(payload.get("top_candidates_preview") or [])[:top_candidates]
    scan_summary = dict(payload.get("scan_summary") or {})
    after_order_cache_counts = dict(payload.get("after_order_cache_counts") or {})
    config = dict(payload.get("config") or {})
    orders_fetch_mode_summary = _build_orders_fetch_mode_summary(payload)
    cycle_summary = {
        "cycle_run_id": payload.get("cycle_run_id") or selected_row["fetch_run_id"],
        "started_at_utc": payload.get("started_at_utc") or selected_row["started_at"],
        "finished_at_utc": payload.get("finished_at_utc") or selected_row["finished_at"],
        "duration_seconds": payload.get("duration_seconds"),
        "region_id": payload.get("region_id"),
        "orders_cache_profile": config.get("orders_cache_profile"),
        "order_refresh_cycle_run_id": payload.get("order_refresh_cycle_run_id"),
        "scan_run_id": payload.get("scan_run_id"),
        "cache_hit_count_after_refresh": after_order_cache_counts.get("cache_hit_count"),
        "candidate_count_built": scan_summary.get("candidate_count_built"),
        "candidate_count_selected": scan_summary.get("candidate_count_selected"),
        "orders_requested_fetch_mode": orders_fetch_mode_summary.get("requested_fetch_mode"),
        "orders_effective_fetch_mode": orders_fetch_mode_summary.get("dominant_effective_fetch_mode"),
    }
    artifact_health = _artifact_health(traceability)

    return {
        "report_generated_at_utc": _utc_now_iso(),
        "cycle_summary": cycle_summary,
        "scan_summary": scan_summary,
        "order_refresh_summary": payload.get("order_refresh_summary"),
        "orders_fetch_mode_summary": orders_fetch_mode_summary,
        "top_candidates_preview": top_candidates_preview,
        "traceability": traceability,
        "artifact_health": artifact_health,
    }


def build_jita_passive_cache_report(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    cycle_run_id: str | None,
    prefer_latest_selected: bool,
    top_candidates: int,
) -> dict[str, Any]:
    return resolve_jita_passive_cache_cycle_context(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        cycle_run_id=cycle_run_id,
        prefer_latest_selected=prefer_latest_selected,
        top_candidates=top_candidates,
    )
