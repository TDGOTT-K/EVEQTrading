from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .coverage import collect_market_coverage_snapshot


_COVERAGE_PHASE_GROUPS = {
    "history_batch_seconds": "remote_fetch",
    "orders_batch_seconds": "remote_fetch",
    "benchmark_seconds": "local_compute",
    "scan_seconds": "local_compute",
    "diagnosis_seconds": "local_compute",
}
_COVERAGE_TARGET_CYCLE_SECONDS = 600.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_path(raw_value: str | None) -> Path | None:
    if not raw_value:
        return None
    return Path(raw_value)


def resolve_runtime_roots(
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
) -> tuple[Path, Path]:
    if db_path.parent.name in {"data", "state"}:
        base_dir = db_path.parent.parent
    else:
        base_dir = db_path.parent
    return runs_root or (base_dir / "runs"), log_dir or (base_dir / "logs")


def _read_json(path: Path | None) -> dict[str, Any] | list[Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_scan_output_dir(scan_row: sqlite3.Row, runs_root: Path) -> Path:
    raw_output_dir = _coerce_path(scan_row["output_dir"])
    if raw_output_dir is not None and raw_output_dir.exists():
        return raw_output_dir
    fallback = runs_root / str(scan_row["scan_run_id"])
    if fallback.exists():
        return fallback
    return raw_output_dir or fallback


def _artifact_paths(output_dir: Path) -> dict[str, str]:
    return {
        "output_dir": str(output_dir),
        "analysis_run_path": str(output_dir / "analysis_run.json"),
        "calc_trace_path": str(output_dir / "calc_trace.json"),
        "candidate_table_path": str(output_dir / "candidate_table.json"),
        "summary_path": str(output_dir / "summary.md"),
        "feature_table_path": str(output_dir / "feature_table.json"),
        "skipped_table_path": str(output_dir / "skipped_table.json"),
    }


def _artifact_health(paths: dict[str, str]) -> dict[str, Any]:
    output_dir = Path(paths["output_dir"])
    analysis_run_path = Path(paths["analysis_run_path"])
    calc_trace_path = Path(paths["calc_trace_path"])
    candidate_table_path = Path(paths["candidate_table_path"])
    summary_path = Path(paths["summary_path"])
    feature_table_path = Path(paths["feature_table_path"])
    skipped_table_path = Path(paths["skipped_table_path"])

    output_dir_exists = output_dir.exists()
    analysis_run_exists = analysis_run_path.exists()
    calc_trace_exists = calc_trace_path.exists()
    candidate_table_exists = candidate_table_path.exists()
    summary_exists = summary_path.exists()
    feature_table_exists = feature_table_path.exists()
    skipped_table_exists = skipped_table_path.exists()

    required_trace_complete = analysis_run_exists and calc_trace_exists and candidate_table_exists
    advisory_ready = required_trace_complete and output_dir_exists

    missing_required = []
    if not output_dir_exists:
        missing_required.append("output_dir")
    if not analysis_run_exists:
        missing_required.append("analysis_run.json")
    if not calc_trace_exists:
        missing_required.append("calc_trace.json")
    if not candidate_table_exists:
        missing_required.append("candidate_table.json")

    warnings: list[str] = []
    if not advisory_ready:
        warnings.append("trace artifacts are incomplete; advisory output may be degraded")
    if output_dir_exists and not summary_exists:
        warnings.append("summary.md is missing")

    return {
        "output_dir_exists": output_dir_exists,
        "analysis_run_exists": analysis_run_exists,
        "calc_trace_exists": calc_trace_exists,
        "candidate_table_exists": candidate_table_exists,
        "summary_exists": summary_exists,
        "feature_table_exists": feature_table_exists,
        "skipped_table_exists": skipped_table_exists,
        "required_trace_complete": required_trace_complete,
        "advisory_ready": advisory_ready,
        "missing_required": missing_required,
        "warnings": warnings,
    }


def _fetch_scan_row(
    connection: sqlite3.Connection,
    *,
    scan_run_id: str | None,
    require_selected: bool,
) -> sqlite3.Row | None:
    sql = (
        "SELECT scan_run_id, started_at, finished_at, market_scope, strategy_set, "
        "capital_budget_isk, parameter_version, input_path, output_dir, selected_candidate_count, status "
        "FROM scan_run WHERE status = 'completed'"
    )
    params: list[Any] = []
    if scan_run_id is not None:
        sql += " AND scan_run_id = ?"
        params.append(scan_run_id)
    if require_selected:
        sql += " AND selected_candidate_count > 0"
    sql += " ORDER BY started_at DESC LIMIT 1"
    return connection.execute(sql, params).fetchone()


def _fetch_opportunity_rows(connection: sqlite3.Connection, *, scan_run_id: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT scan_run_id, opportunity_id, strategy_type, type_id, type_name, region_id,
               buy_location_id, sell_location_id, entry_mode, exit_mode, unit_profit,
               total_profit, profit_margin, capital_required, tradable_units,
               expected_turnover_days, risk_adjusted_return, decision_reason
        FROM opportunity_snapshot
        WHERE scan_run_id = ?
        ORDER BY risk_adjusted_return DESC, profit_margin DESC, total_profit DESC, opportunity_id
        """,
        (scan_run_id,),
    ).fetchall()


def _parse_region_id_from_market_scope(raw_scope: Any) -> int | None:
    if not isinstance(raw_scope, str):
        return None
    if not raw_scope.startswith("region:"):
        return None
    try:
        return int(raw_scope.split(":", 1)[1])
    except ValueError:
        return None


def _worker_state_kind(worker_state_payload: dict[str, Any] | None) -> str | None:
    if not isinstance(worker_state_payload, dict):
        return None
    if any(key in worker_state_payload for key in ("mirror_run_id", "target_universe", "refresh_interval_minutes")):
        return "region_order_mirror_worker"
    if "orders_cache_profile" in worker_state_payload or "last_scan_run_id" in worker_state_payload:
        return "jita_passive_cache_worker"

    for counts_key in ("initial_counts", "final_counts"):
        counts = worker_state_payload.get(counts_key)
        if not isinstance(counts, dict):
            continue
        if any(key in counts for key in ("backfill_due_count", "refresh_due_count", "history_ready_30d_count")):
            return "history_sync_worker"
        if any(key in counts for key in ("missing_orders_count", "refresh_due_remote_count", "cache_hit_count")):
            return "jita_passive_cache_worker"

    if "cycle_run_id" in worker_state_payload or "cycle_summary" in worker_state_payload:
        return "coverage_worker"
    return "unknown"


def _build_worker_state_summary(worker_state_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(worker_state_payload, dict):
        return None

    aggregate = worker_state_payload.get("aggregate")
    aggregate_summary = None
    if isinstance(aggregate, dict):
        aggregate_summary = {
            "executed_remote_target_count": aggregate.get("executed_remote_target_count"),
            "cache_hit_gain": aggregate.get("cache_hit_gain"),
            "candidate_count_built": aggregate.get("candidate_count_built"),
            "candidate_count_selected": aggregate.get("candidate_count_selected"),
        }

    cycle_run_id = worker_state_payload.get("cycle_run_id") or worker_state_payload.get("last_cycle_run_id")
    last_cycle_run_id = worker_state_payload.get("last_cycle_run_id") or cycle_run_id
    last_scan_run_id = worker_state_payload.get("last_scan_run_id")
    if last_scan_run_id == "":
        last_scan_run_id = None

    return {
        "worker_kind": _worker_state_kind(worker_state_payload),
        "status": worker_state_payload.get("status"),
        "worker_run_id": worker_state_payload.get("worker_run_id"),
        "cycle_run_id": cycle_run_id,
        "cycle_count": worker_state_payload.get("cycle_count"),
        "last_cycle_run_id": last_cycle_run_id,
        "last_scan_run_id": last_scan_run_id,
        "orders_cache_profile": worker_state_payload.get("orders_cache_profile"),
        "stop_reason": worker_state_payload.get("stop_reason"),
        "started_at_utc": worker_state_payload.get("started_at_utc"),
        "finished_at_utc": worker_state_payload.get("finished_at_utc"),
        "next_allowed_at_utc": worker_state_payload.get("next_allowed_at_utc"),
        "message": worker_state_payload.get("message"),
        "aggregate": aggregate_summary,
    }


def _latest_sync_report(log_dir: Path, db_path: Path) -> dict[str, Any] | None:
    latest_path: Path | None = None
    if log_dir.exists():
        candidates = sorted(log_dir.glob("eveq-foreign-sync-*.json"))
        if candidates:
            latest_path = candidates[-1]

    manifest_path = Path(str(db_path) + ".snapshot.json")
    manifest_payload = _read_json(manifest_path)
    report_payload = _read_json(latest_path)
    if report_payload is None and manifest_payload is None:
        return None

    worker_state_payload = None
    if isinstance(report_payload, dict):
        remote_manifest = report_payload.get("remote_manifest")
        if isinstance(remote_manifest, dict) and isinstance(remote_manifest.get("worker_state"), dict):
            worker_state_payload = remote_manifest.get("worker_state")
    if worker_state_payload is None and isinstance(manifest_payload, dict):
        if isinstance(manifest_payload.get("worker_state"), dict):
            worker_state_payload = manifest_payload.get("worker_state")

    return {
        "latest_sync_report_path": str(latest_path) if latest_path else None,
        "snapshot_manifest_path": str(manifest_path) if manifest_payload is not None else None,
        "latest_sync_report": report_payload,
        "snapshot_manifest": manifest_payload,
        "worker_state_summary": _build_worker_state_summary(worker_state_payload),
        "sync_health_summary": _sync_health_summary(report_payload, manifest_payload),
        "worker_health_rollup": _worker_health_rollup(log_dir),
        "sync_problem_rollup": _sync_problem_rollup(log_dir, manifest_payload),
    }


def _latest_retention_cleanup(runs_root: Path) -> dict[str, Any] | None:
    if not runs_root.exists():
        return None

    candidates = sorted(
        [
            path
            for path in runs_root.glob("retention-cleanup-*")
            if path.is_dir() and (path / "retention_cleanup.json").exists()
        ]
    )
    if not candidates:
        return None

    latest_dir = candidates[-1]
    payload = _read_json(latest_dir / "retention_cleanup.json")
    if not isinstance(payload, dict):
        return None

    summary = payload.get("summary")
    if not isinstance(summary, dict):
        summary = {}

    return {
        "cleanup_run_id": payload.get("cleanup_run_id"),
        "cleanup_run_dir": str(latest_dir),
        "cleaned_at_utc": payload.get("cleaned_at_utc"),
        "dry_run": payload.get("dry_run"),
        "summary": {
            "candidate_count": summary.get("candidate_count"),
            "deleted_count": summary.get("deleted_count"),
            "dry_run_candidate_count": summary.get("dry_run_candidate_count"),
            "category_counts": summary.get("category_counts"),
        },
        "health_summary": _cleanup_health_summary(payload),
        "health_rollup": _cleanup_health_rollup(runs_root),
    }


def _coverage_cycle_dirs(runs_root: Path) -> list[Path]:
    if not runs_root.exists():
        return []
    return sorted(
        path
        for path in runs_root.glob("coverage-cycle-*")
        if path.is_dir() and (path / "coverage_cycle.json").exists()
    )


def _coverage_cycle_summary(cycle_dir: Path) -> dict[str, Any] | None:
    payload = _read_json(cycle_dir / "coverage_cycle.json")
    if not isinstance(payload, dict):
        return None

    before_counts = payload.get("before_counts")
    after_orders_counts = payload.get("after_orders_counts")
    if not isinstance(before_counts, dict):
        before_counts = {}
    if not isinstance(after_orders_counts, dict):
        after_orders_counts = {}

    before_ready = int(before_counts.get("ready_count") or 0)
    after_ready = int(after_orders_counts.get("ready_count") or 0)
    history_target_count = int(payload.get("history_target_count") or 0)
    orders_target_count = int(payload.get("orders_target_count") or 0)
    phase_durations = payload.get("phase_durations")
    if not isinstance(phase_durations, dict):
        phase_durations = None
    return {
        "cycle_run_id": payload.get("cycle_run_id"),
        "cycle_run_dir": str(cycle_dir),
        "started_at_utc": payload.get("started_at_utc"),
        "finished_at_utc": payload.get("finished_at_utc"),
        "duration_seconds": payload.get("duration_seconds"),
        "phase_durations": phase_durations,
        "region_id": payload.get("region_id"),
        "before_ready_count": before_ready,
        "after_ready_count": after_ready,
        "ready_gain": after_ready - before_ready,
        "history_target_count": history_target_count,
        "orders_target_count": orders_target_count,
        "total_target_count": history_target_count + orders_target_count,
        "scan_run_id": payload.get("scan_run_id"),
        "diagnosis_selected_count": ((payload.get("diagnosis_summary") or {}).get("selected_count")),
    }


def _recent_coverage_cycles(
    runs_root: Path,
    *,
    region_id: int,
    limit: int = 5,
) -> list[dict[str, Any]]:
    recent_cycles: list[dict[str, Any]] = []
    for cycle_dir in reversed(_coverage_cycle_dirs(runs_root)):
        cycle_summary = _coverage_cycle_summary(cycle_dir)
        if cycle_summary is None:
            continue
        if int(cycle_summary.get("region_id") or 0) != region_id:
            continue
        recent_cycles.append(cycle_summary)
        if len(recent_cycles) >= limit:
            break
    return recent_cycles


def _coverage_cycle_trend_summary(recent_cycles: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not recent_cycles:
        return None

    gains = [int(cycle.get("ready_gain") or 0) for cycle in recent_cycles]
    total_target_counts = [int(cycle.get("total_target_count") or 0) for cycle in recent_cycles]
    latest_phase_durations = recent_cycles[0].get("phase_durations")
    if not isinstance(latest_phase_durations, dict):
        latest_phase_durations = None
    latest_duration = recent_cycles[0].get("duration_seconds")
    if not isinstance(latest_duration, (int, float)):
        latest_duration = None
    durations = [
        float(cycle["duration_seconds"])
        for cycle in recent_cycles
        if isinstance(cycle.get("duration_seconds"), (int, float))
    ]
    no_progress_streak = 0
    for gain in gains:
        if gain > 0:
            break
        no_progress_streak += 1

    latest_gain = gains[0]
    if latest_gain < 0:
        latest_status = "regressed"
    elif latest_gain > 0:
        latest_status = "progressing"
    elif no_progress_streak >= 2:
        latest_status = "stalled"
    else:
        latest_status = "flat"

    return {
        "cycle_count": len(recent_cycles),
        "net_ready_gain": sum(gains),
        "average_ready_gain": (sum(gains) / len(gains)) if gains else 0.0,
        "positive_gain_cycles": sum(1 for gain in gains if gain > 0),
        "zero_gain_cycles": sum(1 for gain in gains if gain == 0),
        "negative_gain_cycles": sum(1 for gain in gains if gain < 0),
        "no_progress_streak": no_progress_streak,
        "latest_status": latest_status,
        "latest_duration_seconds": float(latest_duration) if latest_duration is not None else None,
        "latest_phase_durations": latest_phase_durations,
        "average_duration_seconds": (sum(durations) / len(durations)) if durations else None,
        "max_duration_seconds": max(durations) if durations else None,
        "latest_total_target_count": total_target_counts[0] if total_target_counts else 0,
        "average_total_target_count": (
            sum(total_target_counts) / len(total_target_counts) if total_target_counts else 0.0
        ),
    }


def _coverage_bottleneck_summary(recent_cycles: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not recent_cycles:
        return None

    latest_cycle = recent_cycles[0]
    latest_duration = latest_cycle.get("duration_seconds")
    latest_phase_durations = latest_cycle.get("phase_durations")
    if not isinstance(latest_duration, (int, float)) or not isinstance(latest_phase_durations, dict):
        return None

    normalized_latest_phases = {
        phase_name: float(value)
        for phase_name, value in latest_phase_durations.items()
        if isinstance(value, (int, float))
    }
    if not normalized_latest_phases:
        return None

    average_phase_seconds: dict[str, float] = {}
    for phase_name in normalized_latest_phases:
        values = [
            float((cycle.get("phase_durations") or {}).get(phase_name))
            for cycle in recent_cycles
            if isinstance((cycle.get("phase_durations") or {}).get(phase_name), (int, float))
        ]
        if values:
            average_phase_seconds[phase_name] = sum(values) / len(values)

    dominant_phase_name, dominant_phase_seconds = max(
        normalized_latest_phases.items(),
        key=lambda item: item[1],
    )
    dominant_group = _COVERAGE_PHASE_GROUPS.get(dominant_phase_name, "unknown")
    dominant_share = (dominant_phase_seconds / float(latest_duration)) if float(latest_duration) > 0 else None
    if dominant_share is None:
        dominance_level = "unknown"
    elif dominant_share >= 0.5:
        dominance_level = "high"
    elif dominant_share >= 0.35:
        dominance_level = "medium"
    else:
        dominance_level = "low"

    return {
        "latest_cycle_run_id": latest_cycle.get("cycle_run_id"),
        "latest_duration_seconds": float(latest_duration),
        "dominant_phase": {
            "phase_name": dominant_phase_name,
            "seconds": dominant_phase_seconds,
            "share_of_cycle": dominant_share,
            "group": dominant_group,
            "dominance_level": dominance_level,
        },
        "average_phase_seconds": average_phase_seconds,
        "suspected_bottleneck_group": dominant_group,
    }


def _coverage_runtime_target_summary(recent_cycles: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not recent_cycles:
        return None

    latest_cycle = recent_cycles[0]
    latest_duration = latest_cycle.get("duration_seconds")
    if not isinstance(latest_duration, (int, float)):
        return None

    durations = [
        float(cycle["duration_seconds"])
        for cycle in recent_cycles
        if isinstance(cycle.get("duration_seconds"), (int, float))
    ]
    latest_duration_seconds = float(latest_duration)
    latest_gap_seconds = latest_duration_seconds - _COVERAGE_TARGET_CYCLE_SECONDS
    average_duration_seconds = (sum(durations) / len(durations)) if durations else latest_duration_seconds
    average_gap_seconds = average_duration_seconds - _COVERAGE_TARGET_CYCLE_SECONDS

    return {
        "target_cycle_seconds": _COVERAGE_TARGET_CYCLE_SECONDS,
        "latest_duration_seconds": latest_duration_seconds,
        "average_duration_seconds": average_duration_seconds,
        "latest_gap_seconds": latest_gap_seconds,
        "average_gap_seconds": average_gap_seconds,
        "latest_status": "within_target" if latest_gap_seconds <= 0 else "over_target",
        "average_status": "within_target" if average_gap_seconds <= 0 else "over_target",
    }


def _coverage_primary_backlog_bucket(backlog_summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(backlog_summary, dict):
        return None

    bucket_rows = [
        {"bucket": "missing_history_only", "count": int(backlog_summary.get("missing_history_only_count") or 0)},
        {"bucket": "missing_orders_only", "count": int(backlog_summary.get("missing_orders_only_count") or 0)},
        {"bucket": "missing_both", "count": int(backlog_summary.get("missing_both_count") or 0)},
    ]
    bucket_rows.sort(key=lambda row: (-int(row["count"]), str(row["bucket"])))
    if not bucket_rows or int(bucket_rows[0]["count"]) <= 0:
        return None
    return bucket_rows[0]


def _coverage_recommended_focus(
    *,
    backlog_count: int,
    primary_backlog_bucket: dict[str, Any] | None,
    latest_cycle_status: str | None,
    runtime_target_status: str | None,
    suspected_bottleneck_group: str | None,
    sync_health_summary: dict[str, Any] | None,
) -> str:
    if isinstance(sync_health_summary, dict) and sync_health_summary.get("has_problems"):
        return "fix_sync_visibility"
    if latest_cycle_status == "regressed":
        return "investigate_ready_universe_regression"
    if runtime_target_status == "over_target":
        if suspected_bottleneck_group == "remote_fetch":
            return "optimize_remote_fetch"
        if suspected_bottleneck_group == "local_compute":
            return "optimize_local_compute"
        if latest_cycle_status == "stalled":
            return "reduce_cycle_runtime"
    if latest_cycle_status == "stalled":
        if primary_backlog_bucket is not None:
            bucket = str(primary_backlog_bucket.get("bucket"))
            if bucket in {"missing_history_only", "missing_both"}:
                return "expand_history_coverage"
            if bucket == "missing_orders_only":
                return "expand_orders_coverage"
        return "investigate_cycle_stall"
    if backlog_count > 0 and primary_backlog_bucket is not None:
        bucket = str(primary_backlog_bucket.get("bucket"))
        if bucket in {"missing_history_only", "missing_both"}:
            return "expand_history_coverage"
        if bucket == "missing_orders_only":
            return "expand_orders_coverage"
    return "maintain_ready_universe"


def _coverage_headline(
    *,
    backlog_count: int,
    latest_cycle_status: str | None,
    runtime_target_status: str | None,
    sync_health_summary: dict[str, Any] | None,
) -> str:
    if isinstance(sync_health_summary, dict) and sync_health_summary.get("has_problems"):
        return "coverage visibility is degraded because sync health is not clean"
    if backlog_count <= 0:
        return "the ready market universe is fully covered for this scan scope"
    if latest_cycle_status in {"stalled", "regressed"} and runtime_target_status == "over_target":
        return "coverage expansion is behind the runtime target and not progressing cleanly"
    if latest_cycle_status in {"stalled", "regressed"}:
        return "coverage expansion is not progressing cleanly"
    if runtime_target_status == "over_target":
        return "coverage expansion is progressing but still slower than the 10-minute target"
    return "coverage expansion is progressing within the current runtime target"


def _latest_coverage_cycle(runs_root: Path, *, region_id: int) -> dict[str, Any] | None:
    recent_cycles = _recent_coverage_cycles(runs_root, region_id=region_id, limit=1)
    if not recent_cycles:
        return None
    return recent_cycles[0]


def _coverage_progress_summary(
    connection: sqlite3.Connection,
    *,
    runs_root: Path,
    scan_row: sqlite3.Row,
) -> dict[str, Any] | None:
    region_id = _parse_region_id_from_market_scope(scan_row["market_scope"])
    if region_id is None:
        return None

    snapshot = collect_market_coverage_snapshot(connection, region_id=region_id)
    target_counts = snapshot["target_counts"]
    market_type_count = int(target_counts.get("market_type_count") or 0)
    ready_count = int(target_counts.get("ready_count") or 0)
    missing_both_count = int(target_counts.get("missing_both_count") or 0)
    missing_history_count = int(target_counts.get("missing_history_count") or 0)
    missing_orders_count = int(target_counts.get("missing_orders_count") or 0)
    backlog_count = max(market_type_count - ready_count, 0)
    recent_cycles = _recent_coverage_cycles(runs_root, region_id=region_id)

    return {
        "region_id": region_id,
        "target_counts": target_counts,
        "backlog_summary": {
            "backlog_count": backlog_count,
            "missing_history_only_count": max(missing_history_count - missing_both_count, 0),
            "missing_orders_only_count": missing_orders_count,
            "missing_both_count": missing_both_count,
        },
        "ready_ratio": (ready_count / market_type_count) if market_type_count else 0.0,
        "top_market_groups": snapshot.get("top_market_groups"),
        "sample_rows": snapshot.get("sample_rows"),
        "latest_cycle": _latest_coverage_cycle(runs_root, region_id=region_id),
        "recent_cycle_summary": _coverage_cycle_trend_summary(recent_cycles),
        "bottleneck_summary": _coverage_bottleneck_summary(recent_cycles),
        "runtime_target_summary": _coverage_runtime_target_summary(recent_cycles),
        "recent_cycles": recent_cycles,
    }


def _sync_problem_flags(
    report_payload: dict[str, Any] | None,
    manifest_payload: dict[str, Any] | None,
) -> list[str]:
    flags: list[str] = []
    if report_payload is None and manifest_payload is None:
        return ["sync_visibility_missing"]

    sync_status = report_payload.get("sync_status") if isinstance(report_payload, dict) else None
    if sync_status == "failed":
        flags.append("sync_failed")
    elif sync_status == "skipped_locked":
        flags.append("sync_locked")

    manifest = manifest_payload
    if isinstance(report_payload, dict):
        remote_manifest = report_payload.get("remote_manifest")
        if isinstance(remote_manifest, dict):
            manifest = remote_manifest

    if not isinstance(manifest, dict):
        if sync_status != "failed":
            flags.append("remote_manifest_missing")
        return flags

    worker_state = manifest.get("worker_state")
    worker_status = worker_state.get("status") if isinstance(worker_state, dict) else None
    worker_state_error = manifest.get("worker_state_error")
    db_quick_check = manifest.get("db_quick_check")

    if worker_status is None:
        flags.append("worker_status_missing")
    elif worker_status == "failed":
        flags.append("worker_failed")
    elif worker_status == "skipped_backoff":
        flags.append("worker_backoff")

    if worker_state_error:
        flags.append("worker_state_error")
    if db_quick_check not in {None, "ok"}:
        flags.append("snapshot_db_quick_check_failed")
    return flags


def _sync_health_summary(
    report_payload: dict[str, Any] | list[Any] | None,
    manifest_payload: dict[str, Any] | list[Any] | None,
) -> dict[str, Any]:
    effective_report = report_payload if isinstance(report_payload, dict) else None
    effective_manifest = manifest_payload if isinstance(manifest_payload, dict) else None
    flags = _sync_problem_flags(effective_report, effective_manifest)

    manifest = effective_manifest
    if isinstance(effective_report, dict):
        remote_manifest = effective_report.get("remote_manifest")
        if isinstance(remote_manifest, dict):
            manifest = remote_manifest

    worker_state = manifest.get("worker_state") if isinstance(manifest, dict) else None
    return {
        "problem_flags": flags,
        "has_problems": bool(flags),
        "sync_status": effective_report.get("sync_status") if isinstance(effective_report, dict) else None,
        "sync_stage": effective_report.get("sync_stage") if isinstance(effective_report, dict) else None,
        "error_message": effective_report.get("error_message") if isinstance(effective_report, dict) else None,
        "error_exit_code": effective_report.get("error_exit_code") if isinstance(effective_report, dict) else None,
        "worker_status": worker_state.get("status") if isinstance(worker_state, dict) else None,
        "worker_state_error": manifest.get("worker_state_error") if isinstance(manifest, dict) else None,
        "db_quick_check": manifest.get("db_quick_check") if isinstance(manifest, dict) else None,
        "remote_exported_at_utc": manifest.get("exported_at_utc") if isinstance(manifest, dict) else None,
    }


def _sync_problem_rollup(log_dir: Path, manifest_payload: dict[str, Any] | list[Any] | None, *, limit: int = 5) -> dict[str, Any] | None:
    recent_rows: list[dict[str, Any]] = []
    if log_dir.exists():
        recent_paths = sorted(log_dir.glob("eveq-foreign-sync-*.json"), reverse=True)[:limit]
        for path in recent_paths:
            payload = _read_json(path)
            report_payload = payload if isinstance(payload, dict) else None
            flags = _sync_problem_flags(report_payload, None)
            recent_rows.append(
                {
                    "sync_report_path": str(path),
                    "sync_completed_at_utc": report_payload.get("sync_completed_at_utc") if isinstance(report_payload, dict) else None,
                    "sync_status": report_payload.get("sync_status") if isinstance(report_payload, dict) else None,
                    "sync_stage": report_payload.get("sync_stage") if isinstance(report_payload, dict) else None,
                    "problem_flags": flags,
                }
            )

    if not recent_rows and isinstance(manifest_payload, dict):
        flags = _sync_problem_flags(None, manifest_payload)
        recent_rows.append(
            {
                "sync_report_path": None,
                "sync_completed_at_utc": None,
                "problem_flags": flags,
            }
        )

    if not recent_rows:
        return None

    flag_counts: dict[str, int] = {}
    consecutive_problem_count = 0
    for row in recent_rows:
        problem_flags = list(row["problem_flags"])
        if problem_flags:
            consecutive_problem_count += 1
        else:
            break
        for flag in problem_flags:
            flag_counts[flag] = flag_counts.get(flag, 0) + 1

    flag_rows = [{"flag": flag, "count": count} for flag, count in flag_counts.items()]
    flag_rows.sort(key=lambda row: (-int(row["count"]), str(row["flag"])))

    return {
        "window_size": len(recent_rows),
        "recent_problem_count": sum(1 for row in recent_rows if row["problem_flags"]),
        "consecutive_problem_count": consecutive_problem_count,
        "problem_flag_counts": flag_rows,
        "recent_reports": recent_rows,
    }


def _cleanup_problem_flags(payload: dict[str, Any] | None) -> list[str]:
    if not isinstance(payload, dict):
        return ["cleanup_visibility_missing"]

    flags: list[str] = []
    if payload.get("dry_run"):
        flags.append("dry_run")

    summary = payload.get("summary")
    if isinstance(summary, dict):
        candidate_count = int(summary.get("candidate_count") or 0)
        deleted_count = int(summary.get("deleted_count") or 0)
        if not payload.get("dry_run") and candidate_count > 0 and deleted_count == 0:
            flags.append("cleanup_deleted_nothing")
    return flags


def _cleanup_health_summary(payload: dict[str, Any] | None) -> dict[str, Any]:
    flags = _cleanup_problem_flags(payload)
    summary = payload.get("summary") if isinstance(payload, dict) else None
    if not isinstance(summary, dict):
        summary = {}
    return {
        "problem_flags": flags,
        "has_problems": bool(flags),
        "candidate_count": summary.get("candidate_count"),
        "deleted_count": summary.get("deleted_count"),
        "dry_run": payload.get("dry_run") if isinstance(payload, dict) else None,
    }


def _cleanup_health_rollup(runs_root: Path, *, limit: int = 5) -> dict[str, Any] | None:
    if not runs_root.exists():
        return None

    recent_paths = sorted(
        [
            path
            for path in runs_root.glob("retention-cleanup-*")
            if path.is_dir() and (path / "retention_cleanup.json").exists()
        ],
        reverse=True,
    )[:limit]
    if not recent_paths:
        return None

    recent_rows: list[dict[str, Any]] = []
    flag_counts: dict[str, int] = {}
    consecutive_problem_count = 0
    for path in recent_paths:
        payload = _read_json(path / "retention_cleanup.json")
        payload_dict = payload if isinstance(payload, dict) else None
        flags = _cleanup_problem_flags(payload_dict)
        recent_rows.append(
            {
                "cleanup_run_id": payload_dict.get("cleanup_run_id") if isinstance(payload_dict, dict) else None,
                "cleaned_at_utc": payload_dict.get("cleaned_at_utc") if isinstance(payload_dict, dict) else None,
                "problem_flags": flags,
                "dry_run": payload_dict.get("dry_run") if isinstance(payload_dict, dict) else None,
            }
        )

    for row in recent_rows:
        problem_flags = list(row["problem_flags"])
        if problem_flags:
            consecutive_problem_count += 1
        else:
            break
        for flag in problem_flags:
            flag_counts[flag] = flag_counts.get(flag, 0) + 1

    flag_rows = [{"flag": flag, "count": count} for flag, count in flag_counts.items()]
    flag_rows.sort(key=lambda row: (-int(row["count"]), str(row["flag"])))

    return {
        "window_size": len(recent_rows),
        "recent_problem_count": sum(1 for row in recent_rows if row["problem_flags"]),
        "consecutive_problem_count": consecutive_problem_count,
        "problem_flag_counts": flag_rows,
        "recent_runs": recent_rows,
    }


def _worker_health_rollup(log_dir: Path, *, limit: int = 5) -> dict[str, Any] | None:
    if not log_dir.exists():
        return None

    recent_paths = sorted(log_dir.glob("eveq-foreign-sync-*.json"), reverse=True)[:limit]
    if not recent_paths:
        return None

    recent_statuses: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    consecutive_problem_count = 0
    problem_statuses = {"failed", "skipped_backoff"}

    for path in recent_paths:
        payload = _read_json(path)
        if not isinstance(payload, dict):
            continue
        remote_manifest = payload.get("remote_manifest")
        worker_state = remote_manifest.get("worker_state") if isinstance(remote_manifest, dict) else None
        worker_state_summary = _build_worker_state_summary(worker_state)
        status = worker_state_summary.get("status") if isinstance(worker_state_summary, dict) else None
        status_key = str(status or "missing")
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        if worker_state_summary is None:
            worker_state_summary = {
                "worker_kind": None,
                "status": None,
                "worker_run_id": None,
                "cycle_run_id": None,
                "cycle_count": None,
                "last_cycle_run_id": None,
                "last_scan_run_id": None,
                "orders_cache_profile": None,
                "stop_reason": None,
                "started_at_utc": None,
                "finished_at_utc": None,
                "next_allowed_at_utc": None,
                "message": None,
                "aggregate": None,
            }
        recent_statuses.append(
            {
                "sync_report_path": str(path),
                "sync_completed_at_utc": payload.get("sync_completed_at_utc"),
                **worker_state_summary,
            }
        )

    for row in recent_statuses:
        if row.get("status") in problem_statuses:
            consecutive_problem_count += 1
        else:
            break

    count_rows = [{"status": status, "count": count} for status, count in status_counts.items()]
    count_rows.sort(key=lambda row: (-int(row["count"]), str(row["status"])))

    latest_status = recent_statuses[0]["status"] if recent_statuses else None
    return {
        "window_size": len(recent_statuses),
        "latest_status": latest_status,
        "consecutive_problem_count": consecutive_problem_count,
        "status_counts": count_rows,
        "recent_statuses": recent_statuses,
    }


def _candidate_trace_map(calc_trace_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(calc_trace_payload, dict):
        return {}
    trace_map: dict[str, dict[str, Any]] = {}
    for trace in calc_trace_payload.get("candidate_traces", []):
        source_record = trace.get("source_record", {})
        opportunity_id = source_record.get("opportunity_id")
        if isinstance(opportunity_id, str) and opportunity_id:
            trace_map[opportunity_id] = trace
    return trace_map


def _selected_candidate_map(candidate_table_payload: list[Any] | dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(candidate_table_payload, list):
        return {}
    candidate_map: dict[str, dict[str, Any]] = {}
    for row in candidate_table_payload:
        if not isinstance(row, dict):
            continue
        opportunity_id = row.get("opportunity_id")
        if isinstance(opportunity_id, str) and opportunity_id:
            candidate_map[opportunity_id] = row
    return candidate_map


def _find_candidate_trace(
    calc_trace_payload: dict[str, Any] | None,
    *,
    opportunity_id: str | None,
    type_id: int | None,
    strategy_type: str | None,
) -> dict[str, Any] | None:
    if not isinstance(calc_trace_payload, dict):
        return None

    fallback_match: dict[str, Any] | None = None
    for trace in calc_trace_payload.get("candidate_traces", []):
        source_record = trace.get("source_record", {})
        trace_opportunity_id = source_record.get("opportunity_id")
        if opportunity_id and isinstance(trace_opportunity_id, str) and trace_opportunity_id == opportunity_id:
            return trace
        if type_id is not None and int(trace.get("type_id", -1)) == type_id:
            if strategy_type is None or str(trace.get("strategy_pool")) == strategy_type:
                return trace
            if fallback_match is None:
                fallback_match = trace
    return fallback_match


def _build_selected_entry(
    row: sqlite3.Row,
    *,
    candidate_row: dict[str, Any] | None,
    trace_row: dict[str, Any] | None,
) -> dict[str, Any]:
    derived = trace_row.get("derived") if isinstance(trace_row, dict) else {}
    source_record = trace_row.get("source_record") if isinstance(trace_row, dict) else {}
    return {
        "opportunity_id": row["opportunity_id"],
        "strategy_type": row["strategy_type"],
        "type_id": row["type_id"],
        "type_name": row["type_name"],
        "category_id": source_record.get("category_id"),
        "market_group": source_record.get("market_group"),
        "region_id": row["region_id"],
        "buy_location_id": row["buy_location_id"],
        "sell_location_id": row["sell_location_id"],
        "buy_location_name": candidate_row.get("buy_location_name") if isinstance(candidate_row, dict) else None,
        "sell_location_name": candidate_row.get("sell_location_name") if isinstance(candidate_row, dict) else None,
        "entry_mode": row["entry_mode"],
        "exit_mode": row["exit_mode"],
        "unit_profit": row["unit_profit"],
        "total_profit": row["total_profit"],
        "profit_margin": row["profit_margin"],
        "capital_required": row["capital_required"],
        "tradable_units": row["tradable_units"],
        "expected_turnover_days": row["expected_turnover_days"],
        "risk_adjusted_return": row["risk_adjusted_return"],
        "decision_reason": row["decision_reason"],
        "source_order_snapshot_id": source_record.get("source_order_snapshot_id"),
        "source_order_captured_at": source_record.get("source_order_captured_at"),
        "history_latest_market_date": source_record.get("history_latest_market_date"),
        "history_latest_fetched_at": source_record.get("history_latest_fetched_at"),
        "window_start_30d": source_record.get("window_start_30d"),
        "window_end_30d": source_record.get("window_end_30d"),
        "window_start_7d": source_record.get("window_start_7d"),
        "window_end_7d": source_record.get("window_end_7d"),
        "buy_reference_price": candidate_row.get("buy_reference_price") if isinstance(candidate_row, dict) else None,
        "sell_reference_price": candidate_row.get("sell_reference_price") if isinstance(candidate_row, dict) else None,
        "gross_spread_pct": candidate_row.get("gross_spread_pct") if isinstance(candidate_row, dict) else None,
        "net_spread_pct": derived.get("net_spread_pct") if isinstance(derived, dict) else None,
        "screened_profit_margin": derived.get("screened_profit_margin") if isinstance(derived, dict) else None,
        "price_zscore_30d": derived.get("price_zscore_30d") if isinstance(derived, dict) else None,
        "estimated_exit_days_small_size": derived.get("estimated_exit_days_small_size")
        if isinstance(derived, dict)
        else None,
        "avg_daily_isk_volume_30d": source_record.get("avg_daily_isk_volume_30d"),
        "avg_daily_units_7d": source_record.get("avg_daily_units_7d"),
        "active_days_ratio_30d": source_record.get("active_days_ratio_30d"),
        "cross_location_net_spread_pct": source_record.get("cross_location_net_spread_pct"),
        "volatility_30d": source_record.get("volatility_30d"),
        "max_position_isk": source_record.get("max_position_isk"),
        "recommended_deployable_isk": source_record.get("recommended_deployable_isk"),
        "competition_adjusted_deployable_isk": source_record.get("competition_adjusted_deployable_isk"),
        "theoretical_market_capacity_isk": source_record.get("theoretical_market_capacity_isk"),
        "safe_exit_capacity_isk": source_record.get("safe_exit_capacity_isk"),
        "safe_inventory_cap_isk": source_record.get("safe_inventory_cap_isk"),
        "captured_sell_capacity_isk": source_record.get("captured_sell_capacity_isk"),
        "estimated_capture_share": source_record.get("estimated_capture_share"),
        "competition_intensity": source_record.get("competition_intensity"),
        "buy_capture_share": source_record.get("buy_capture_share"),
        "sell_capture_share": source_record.get("sell_capture_share"),
        "buy_competition_intensity": source_record.get("buy_competition_intensity"),
        "sell_competition_intensity": source_record.get("sell_competition_intensity"),
        "competition_safety_discount": source_record.get("competition_safety_discount"),
        "daily_capacity_isk_30d": source_record.get("daily_capacity_isk_30d"),
        "avg_price_weighted_30d": source_record.get("avg_price_weighted_30d"),
        "buy_price_vs_avg_30d": source_record.get("buy_price_vs_avg_30d"),
        "sell_price_vs_avg_30d": source_record.get("sell_price_vs_avg_30d"),
        "recent_high_trade_price_7d": source_record.get("recent_high_trade_price_7d"),
        "sell_price_overhang_ratio": source_record.get("sell_price_overhang_ratio"),
        "sell_price_overhang_steps": source_record.get("sell_price_overhang_steps"),
        "sell_fill_penalty_multiplier": source_record.get("sell_fill_penalty_multiplier"),
        "flip_units_mean_30d": source_record.get("flip_units_mean_30d"),
        "flip_units_mean_7d": source_record.get("flip_units_mean_7d"),
        "flip_units_cv_30d": source_record.get("flip_units_cv_30d"),
        "flip_units_cv_7d": source_record.get("flip_units_cv_7d"),
        "estimated_daily_buy_units_30d": source_record.get("estimated_daily_buy_units_30d"),
        "estimated_daily_sell_units_30d": source_record.get("estimated_daily_sell_units_30d"),
        "estimated_daily_buy_units_7d": source_record.get("estimated_daily_buy_units_7d"),
        "estimated_daily_sell_units_7d": source_record.get("estimated_daily_sell_units_7d"),
        "estimated_daily_buy_isk_30d": source_record.get("estimated_daily_buy_isk_30d"),
        "estimated_daily_sell_isk_30d": source_record.get("estimated_daily_sell_isk_30d"),
        "estimated_daily_buy_isk_7d": source_record.get("estimated_daily_buy_isk_7d"),
        "estimated_daily_sell_isk_7d": source_record.get("estimated_daily_sell_isk_7d"),
        "sell_liquidity_confidence_score": source_record.get("sell_liquidity_confidence_score"),
        "sell_liquidity_confidence_label": source_record.get("sell_liquidity_confidence_label"),
        "sell_liquidity_confidence_discount": source_record.get("sell_liquidity_confidence_discount"),
        "dominant_attribution_mode_30d": source_record.get("dominant_attribution_mode_30d"),
        "dominant_attribution_mode_7d": source_record.get("dominant_attribution_mode_7d"),
        "single_side_extreme_days_30d": source_record.get("single_side_extreme_days_30d"),
        "single_side_extreme_days_7d": source_record.get("single_side_extreme_days_7d"),
    }


def _build_near_miss_entries(
    calc_trace_payload: dict[str, Any] | None,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not isinstance(calc_trace_payload, dict):
        return []

    rejected: list[dict[str, Any]] = []
    for trace in calc_trace_payload.get("candidate_traces", []):
        if trace.get("filter_evaluation", {}).get("passed"):
            continue
        source_record = trace.get("source_record", {})
        derived = trace.get("derived", {})
        rejected.append(
            {
                "opportunity_id": source_record.get("opportunity_id"),
                "type_id": trace.get("type_id"),
                "type_name": trace.get("type_name"),
                "strategy_pool": trace.get("strategy_pool"),
                "buy_location_name": source_record.get("buy_location_name"),
                "sell_location_name": source_record.get("sell_location_name"),
                "screened_profit_margin": derived.get("screened_profit_margin"),
                "net_spread_pct": derived.get("net_spread_pct"),
                "price_zscore_30d": derived.get("price_zscore_30d"),
                "estimated_exit_days_small_size": derived.get("estimated_exit_days_small_size"),
                "avg_daily_isk_volume_30d": source_record.get("avg_daily_isk_volume_30d"),
                "avg_daily_units_7d": source_record.get("avg_daily_units_7d"),
                "active_days_ratio_30d": source_record.get("active_days_ratio_30d"),
                "cross_location_net_spread_pct": source_record.get("cross_location_net_spread_pct"),
                "category_id": source_record.get("category_id"),
                "market_group": source_record.get("market_group"),
                "rejection_reasons": trace.get("filter_evaluation", {}).get("rejection_reasons", []),
                "scan_filter_rejection_reasons": trace.get("filter_evaluation", {}).get("rejection_reasons", []),
                "source_order_snapshot_id": source_record.get("source_order_snapshot_id"),
                "source_order_captured_at": source_record.get("source_order_captured_at"),
                "history_latest_market_date": source_record.get("history_latest_market_date"),
                "history_latest_fetched_at": source_record.get("history_latest_fetched_at"),
                "window_end_30d": source_record.get("window_end_30d"),
            }
        )

    rejected.sort(
        key=lambda row: (
            float(row.get("screened_profit_margin") or 0.0),
            float(row.get("avg_daily_isk_volume_30d") or 0.0),
            -len(row.get("rejection_reasons") or []),
        ),
        reverse=True,
    )
    return rejected[:limit]


def _build_scan_summary(scan_row: sqlite3.Row, output_dir: Path) -> dict[str, Any]:
    return {
        "scan_run_id": scan_row["scan_run_id"],
        "started_at": scan_row["started_at"],
        "finished_at": scan_row["finished_at"],
        "market_scope": scan_row["market_scope"],
        "strategy_set": scan_row["strategy_set"],
        "capital_budget_isk": scan_row["capital_budget_isk"],
        "parameter_version": scan_row["parameter_version"],
        "input_path": scan_row["input_path"],
        "output_dir": str(output_dir),
        "selected_candidate_count": scan_row["selected_candidate_count"],
        "status": scan_row["status"],
    }


def _parse_iso_text(raw_value: Any) -> datetime | None:
    if not raw_value or not isinstance(raw_value, str):
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _min_max_text(values: list[str]) -> tuple[str | None, str | None]:
    clean = sorted({str(value) for value in values if str(value)})
    if not clean:
        return None, None
    return clean[0], clean[-1]


def _build_data_freshness(
    *,
    analysis_run_payload: dict[str, Any] | None,
    calc_trace_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    as_of_time = analysis_run_payload.get("as_of_time") if isinstance(analysis_run_payload, dict) else None
    candidate_traces = list(calc_trace_payload.get("candidate_traces", [])) if isinstance(calc_trace_payload, dict) else []
    source_records = [
        dict(trace.get("source_record") or {})
        for trace in candidate_traces
        if isinstance(trace, dict)
    ]
    order_times = [str(record.get("source_order_captured_at")) for record in source_records if record.get("source_order_captured_at")]
    history_market_dates = [
        str(record.get("history_latest_market_date") or record.get("window_end_30d"))
        for record in source_records
        if record.get("history_latest_market_date") or record.get("window_end_30d")
    ]
    history_fetch_times = [
        str(record.get("history_latest_fetched_at"))
        for record in source_records
        if record.get("history_latest_fetched_at")
    ]
    order_min, order_max = _min_max_text(order_times)
    history_date_min, history_date_max = _min_max_text(history_market_dates)
    history_fetch_min, history_fetch_max = _min_max_text(history_fetch_times)

    warnings: list[str] = []
    if not order_max:
        warnings.append("order snapshot time is unavailable in calc_trace")
    if not history_date_max:
        warnings.append("history market-date basis is unavailable in calc_trace")
    if history_date_max and order_max:
        order_dt = _parse_iso_text(order_max)
        if order_dt is not None and order_dt.date().isoformat() < history_date_max:
            warnings.append("latest order snapshot is older than the history window end date")

    if order_max and history_date_max:
        summary = (
            f"订单大致基于 {order_min or order_max} 到 {order_max} 的快照，"
            f"history 30天窗口截至 {history_date_max}"
        )
        if history_fetch_max:
            summary += f"，最近一次 history 抓取时间约为 {history_fetch_max}"
        summary += "。"
    elif order_max:
        summary = f"订单大致基于 {order_min or order_max} 到 {order_max} 的快照；history 时间基准未知。"
    elif history_date_max:
        summary = f"history 30天窗口截至 {history_date_max}"
        if history_fetch_max:
            summary += f"，最近一次 history 抓取时间约为 {history_fetch_max}"
        summary += "；订单快照时间未知。"
    elif as_of_time:
        summary = f"仅找到分析批次 as_of_time={as_of_time}，未能从 calc_trace 提取订单和 history 的明确时间基准。"
    else:
        summary = "未能识别这份报告所基于的订单和 history 时间基准。"

    return {
        "summary_zh": summary,
        "analysis_as_of_time": as_of_time,
        "order_data_time_basis": {
            "available": bool(order_max),
            "snapshot_time_min": order_min,
            "snapshot_time_max": order_max,
            "snapshot_time_count": len({value for value in order_times if value}),
        },
        "history_data_time_basis": {
            "available": bool(history_date_max or history_fetch_max),
            "window_end_date_min": history_date_min,
            "window_end_date_max": history_date_max,
            "latest_fetched_at_min": history_fetch_min,
            "latest_fetched_at_max": history_fetch_max,
            "history_time_count": len({value for value in history_market_dates if value}),
        },
        "warnings": warnings,
    }


def _load_scan_context(
    connection: sqlite3.Connection,
    *,
    scan_row: sqlite3.Row,
    runs_root: Path,
) -> dict[str, Any]:
    output_dir = _resolve_scan_output_dir(scan_row, runs_root)
    paths = _artifact_paths(output_dir)
    calc_trace_payload = _read_json(Path(paths["calc_trace_path"]))
    candidate_table_payload = _read_json(Path(paths["candidate_table_path"]))
    feature_table_payload = _read_json(Path(paths["feature_table_path"]))
    skipped_table_payload = _read_json(Path(paths["skipped_table_path"]))
    return {
        "output_dir": output_dir,
        "paths": paths,
        "artifact_health": _artifact_health(paths),
        "analysis_run_payload": _read_json(Path(paths["analysis_run_path"])),
        "calc_trace_payload": calc_trace_payload if isinstance(calc_trace_payload, dict) else None,
        "feature_table_payload": feature_table_payload if isinstance(feature_table_payload, list) else None,
        "skipped_table_payload": skipped_table_payload if isinstance(skipped_table_payload, list) else None,
        "selected_candidate_map": _selected_candidate_map(candidate_table_payload),
        "selected_rows": _fetch_opportunity_rows(connection, scan_run_id=str(scan_row["scan_run_id"])),
    }


def _summarize_reasons(traces: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    examples: dict[str, list[str]] = {}
    for trace in traces:
        source_record = trace.get("source_record", {})
        opportunity_id = source_record.get("opportunity_id")
        for reason in trace.get("filter_evaluation", {}).get("rejection_reasons", []):
            key = str(reason)
            counts[key] = counts.get(key, 0) + 1
            if opportunity_id:
                examples.setdefault(key, [])
                if len(examples[key]) < 3:
                    examples[key].append(str(opportunity_id))

    rows = [
        {
            "reason": reason,
            "count": count,
            "example_opportunity_ids": examples.get(reason, []),
        }
        for reason, count in counts.items()
    ]
    rows.sort(key=lambda row: (-int(row["count"]), str(row["reason"])))
    return rows[:limit]


def _build_near_pass_entries(traces: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    near_passes: list[dict[str, Any]] = []
    for trace in traces:
        filter_evaluation = trace.get("filter_evaluation", {})
        if filter_evaluation.get("passed"):
            continue
        source_record = trace.get("source_record", {})
        derived = trace.get("derived", {})
        rejection_reasons = list(filter_evaluation.get("rejection_reasons", []))
        near_passes.append(
            {
                "opportunity_id": source_record.get("opportunity_id"),
                "type_id": trace.get("type_id"),
                "type_name": trace.get("type_name"),
                "strategy_pool": trace.get("strategy_pool"),
                "rejection_reason_count": len(rejection_reasons),
                "rejection_reasons": rejection_reasons,
                "screened_profit_margin": derived.get("screened_profit_margin"),
                "estimated_exit_days_small_size": derived.get("estimated_exit_days_small_size"),
                "price_zscore_30d": derived.get("price_zscore_30d"),
                "avg_daily_isk_volume_30d": source_record.get("avg_daily_isk_volume_30d"),
                "buy_location_name": source_record.get("buy_location_name"),
                "sell_location_name": source_record.get("sell_location_name"),
                "source_order_snapshot_id": source_record.get("source_order_snapshot_id"),
            }
        )

    near_passes.sort(
        key=lambda row: (
            int(row["rejection_reason_count"]),
            -float(row.get("screened_profit_margin") or -999999.0),
            -float(row.get("avg_daily_isk_volume_30d") or 0.0),
            float(row.get("estimated_exit_days_small_size") or 999999.0),
            str(row.get("opportunity_id") or ""),
        )
    )
    return near_passes[:limit]


def _build_skipped_summary(skipped_rows: list[dict[str, Any]] | None, *, limit: int) -> dict[str, Any]:
    skipped_rows = skipped_rows or []
    counts: dict[str, int] = {}
    samples: list[dict[str, Any]] = []
    for row in skipped_rows:
        reason = str(row.get("reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
        if len(samples) < limit:
            samples.append(
                {
                    "type_id": row.get("type_id"),
                    "type_name": row.get("type_name"),
                    "reason": reason,
                }
            )

    reason_rows = [{"reason": reason, "count": count} for reason, count in counts.items()]
    reason_rows.sort(key=lambda row: (-int(row["count"]), str(row["reason"])))
    return {
        "skipped_count": len(skipped_rows),
        "reason_counts": reason_rows[:limit],
        "sample_rows": samples,
    }


def _build_strategy_summary(
    traces: list[dict[str, Any]],
    *,
    selected_rows: list[sqlite3.Row],
    rejection_reason_limit: int,
) -> list[dict[str, Any]]:
    selected_by_strategy: dict[str, int] = {}
    for row in selected_rows:
        strategy_type = str(row["strategy_type"])
        selected_by_strategy[strategy_type] = selected_by_strategy.get(strategy_type, 0) + 1

    grouped: dict[str, list[dict[str, Any]]] = {}
    for trace in traces:
        strategy_pool = str(trace.get("strategy_pool") or "unknown")
        grouped.setdefault(strategy_pool, []).append(trace)

    rows: list[dict[str, Any]] = []
    for strategy_pool, strategy_traces in grouped.items():
        passed_count = sum(1 for trace in strategy_traces if trace.get("filter_evaluation", {}).get("passed"))
        rejected_count = len(strategy_traces) - passed_count
        rows.append(
            {
                "strategy_pool": strategy_pool,
                "candidate_count": len(strategy_traces),
                "passed_hard_filters_count": passed_count,
                "selected_count": selected_by_strategy.get(strategy_pool, 0),
                "rejected_count": rejected_count,
                "top_rejection_reasons": _summarize_reasons(strategy_traces, limit=rejection_reason_limit),
            }
        )

    rows.sort(
        key=lambda row: (
            -int(row["candidate_count"]),
            -int(row["passed_hard_filters_count"]),
            str(row["strategy_pool"]),
        )
    )
    return rows


def build_market_report(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    top_opportunities: int,
    top_near_misses: int,
) -> dict[str, Any]:
    effective_runs_root, effective_log_dir = resolve_runtime_roots(
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
    )

    primary_scan_row = _fetch_scan_row(
        connection,
        scan_run_id=scan_run_id,
        require_selected=prefer_latest_selected and scan_run_id is None,
    )
    if primary_scan_row is None:
        raise SystemExit("No completed scan run was found for the requested scope.")

    latest_selected_row = _fetch_scan_row(connection, scan_run_id=None, require_selected=True)
    output_dir = _resolve_scan_output_dir(primary_scan_row, effective_runs_root)
    paths = _artifact_paths(output_dir)
    artifact_health = _artifact_health(paths)
    analysis_run_payload = _read_json(Path(paths["analysis_run_path"]))
    calc_trace_payload = _read_json(Path(paths["calc_trace_path"]))
    candidate_table_payload = _read_json(Path(paths["candidate_table_path"]))
    selected_candidate_map = _selected_candidate_map(candidate_table_payload)
    opportunity_rows = _fetch_opportunity_rows(connection, scan_run_id=str(primary_scan_row["scan_run_id"]))

    selected_entries = []
    for row in opportunity_rows[:top_opportunities]:
        trace_row = _find_candidate_trace(
            calc_trace_payload if isinstance(calc_trace_payload, dict) else None,
            opportunity_id=str(row["opportunity_id"]),
            type_id=int(row["type_id"]),
            strategy_type=str(row["strategy_type"]),
        )
        selected_entries.append(
            _build_selected_entry(
                row,
                candidate_row=selected_candidate_map.get(str(row["opportunity_id"])),
                trace_row=trace_row,
            )
        )
    near_miss_entries = _build_near_miss_entries(
        calc_trace_payload if isinstance(calc_trace_payload, dict) else None,
        limit=top_near_misses,
    )

    report: dict[str, Any] = {
        "report_generated_at_utc": _utc_now_iso(),
        "db_path": str(db_path),
        "runs_root": str(effective_runs_root),
        "log_dir": str(effective_log_dir),
        "primary_scan": {
            "scan_summary": _build_scan_summary(primary_scan_row, output_dir),
            "analysis_run": analysis_run_payload,
            "traceability": paths,
            "artifact_health": artifact_health,
            "data_freshness": _build_data_freshness(
                analysis_run_payload=analysis_run_payload if isinstance(analysis_run_payload, dict) else None,
                calc_trace_payload=calc_trace_payload if isinstance(calc_trace_payload, dict) else None,
            ),
            "selected_opportunities": selected_entries,
            "near_misses": near_miss_entries,
        },
        "coverage_progress": _coverage_progress_summary(
            connection,
            runs_root=effective_runs_root,
            scan_row=primary_scan_row,
        ),
        "latest_sync": _latest_sync_report(effective_log_dir, db_path),
        "latest_cleanup": _latest_retention_cleanup(effective_runs_root),
    }

    if latest_selected_row is not None and str(latest_selected_row["scan_run_id"]) != str(primary_scan_row["scan_run_id"]):
        report["latest_scan_with_selected_candidates"] = _build_scan_summary(
            latest_selected_row,
            _resolve_scan_output_dir(latest_selected_row, effective_runs_root),
        )
    return report


def build_opportunity_explanation(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    opportunity_id: str | None,
    type_id: int | None,
) -> dict[str, Any]:
    effective_runs_root, effective_log_dir = resolve_runtime_roots(
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
    )
    scan_row = _fetch_scan_row(
        connection,
        scan_run_id=scan_run_id,
        require_selected=prefer_latest_selected and scan_run_id is None,
    )
    if scan_row is None:
        raise SystemExit("No completed scan run was found for explanation lookup.")

    context = _load_scan_context(connection, scan_row=scan_row, runs_root=effective_runs_root)
    output_dir = context["output_dir"]
    paths = context["paths"]
    analysis_run_payload = context["analysis_run_payload"]
    calc_trace_payload = context["calc_trace_payload"]
    selected_candidate_map = context["selected_candidate_map"]
    selected_rows = context["selected_rows"]
    artifact_health = context["artifact_health"]

    selected_row: sqlite3.Row | None = None
    selected_candidate_row: dict[str, Any] | None = None
    candidate_trace: dict[str, Any] | None = None
    matched_status = "not_found"

    def try_match() -> tuple[sqlite3.Row | None, dict[str, Any] | None, dict[str, Any] | None]:
        local_selected_row: sqlite3.Row | None = None
        local_selected_candidate_row: dict[str, Any] | None = None
        local_candidate_trace: dict[str, Any] | None = None

        if opportunity_id:
            local_selected_row = next((row for row in selected_rows if str(row["opportunity_id"]) == opportunity_id), None)
            local_selected_candidate_row = selected_candidate_map.get(opportunity_id)
            local_candidate_trace = _find_candidate_trace(
                calc_trace_payload,
                opportunity_id=opportunity_id,
                type_id=int(local_selected_row["type_id"]) if local_selected_row is not None else None,
                strategy_type=str(local_selected_row["strategy_type"]) if local_selected_row is not None else None,
            )
            if local_selected_row is None:
                try:
                    fallback_type_id = int(opportunity_id.split("-", 1)[0])
                except (TypeError, ValueError):
                    fallback_type_id = None
                if fallback_type_id is not None:
                    local_selected_row = next(
                        (row for row in selected_rows if int(row["type_id"]) == fallback_type_id),
                        None,
                    )
                    if local_selected_row is not None and local_selected_candidate_row is None:
                        local_selected_candidate_row = selected_candidate_map.get(str(local_selected_row["opportunity_id"]))
                    if local_candidate_trace is None:
                        local_candidate_trace = _find_candidate_trace(
                            calc_trace_payload,
                            opportunity_id=None,
                            type_id=fallback_type_id,
                            strategy_type=str(local_selected_row["strategy_type"]) if local_selected_row is not None else None,
                        )
        elif type_id is not None:
            local_selected_row = next((row for row in selected_rows if int(row["type_id"]) == type_id), None)
            if local_selected_row is not None:
                local_selected_candidate_row = selected_candidate_map.get(str(local_selected_row["opportunity_id"]))
                local_candidate_trace = _find_candidate_trace(
                    calc_trace_payload,
                    opportunity_id=str(local_selected_row["opportunity_id"]),
                    type_id=type_id,
                    strategy_type=str(local_selected_row["strategy_type"]),
                )
            if local_candidate_trace is None:
                local_candidate_trace = _find_candidate_trace(
                    calc_trace_payload,
                    opportunity_id=None,
                    type_id=type_id,
                    strategy_type=None,
                )

        return local_selected_row, local_selected_candidate_row, local_candidate_trace

    selected_row, selected_candidate_row, candidate_trace = try_match()

    if selected_row is None and candidate_trace is None and scan_run_id is None:
        fallback_scan_row = _fetch_scan_row(connection, scan_run_id=None, require_selected=True)
        if fallback_scan_row is not None and str(fallback_scan_row["scan_run_id"]) != str(scan_row["scan_run_id"]):
            scan_row = fallback_scan_row
            context = _load_scan_context(connection, scan_row=scan_row, runs_root=effective_runs_root)
            output_dir = context["output_dir"]
            paths = context["paths"]
            analysis_run_payload = context["analysis_run_payload"]
            calc_trace_payload = context["calc_trace_payload"]
            selected_candidate_map = context["selected_candidate_map"]
            selected_rows = context["selected_rows"]
            artifact_health = context["artifact_health"]
            selected_row, selected_candidate_row, candidate_trace = try_match()

    if selected_row is not None:
        matched_status = "selected"
    elif candidate_trace is not None:
        matched_status = "rejected"
    else:
        raise SystemExit("The requested opportunity or type was not found in the selected scan run.")

    source_record = candidate_trace.get("source_record") if isinstance(candidate_trace, dict) else {}
    derived = candidate_trace.get("derived") if isinstance(candidate_trace, dict) else {}
    filter_evaluation = candidate_trace.get("filter_evaluation") if isinstance(candidate_trace, dict) else {}

    selected_snapshot = None
    if selected_row is not None:
        selected_snapshot = _build_selected_entry(
            selected_row,
            candidate_row=selected_candidate_row,
            trace_row=candidate_trace,
        )

    return {
        "report_generated_at_utc": _utc_now_iso(),
        "scan_summary": _build_scan_summary(scan_row, output_dir),
        "analysis_run": analysis_run_payload,
        "traceability": {
            **paths,
            "latest_sync": _latest_sync_report(effective_log_dir, db_path),
        },
        "artifact_health": artifact_health,
        "matched_status": matched_status,
        "requested_opportunity_id": opportunity_id,
        "requested_type_id": type_id,
        "matched_opportunity_id": source_record.get("opportunity_id")
        or (selected_row["opportunity_id"] if selected_row is not None else None),
        "matched_type_id": source_record.get("type_id")
        or (selected_row["type_id"] if selected_row is not None else None),
        "selected_snapshot": selected_snapshot,
        "source_record": source_record if source_record else None,
        "derived": derived if derived else None,
        "filter_evaluation": filter_evaluation if filter_evaluation else None,
    }


def build_scan_diagnosis(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    top_rejection_reasons: int,
    top_near_passes: int,
    top_skipped_reasons: int,
) -> dict[str, Any]:
    effective_runs_root, effective_log_dir = resolve_runtime_roots(
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
    )
    scan_row = _fetch_scan_row(
        connection,
        scan_run_id=scan_run_id,
        require_selected=prefer_latest_selected and scan_run_id is None,
    )
    if scan_row is None:
        raise SystemExit("No completed scan run was found for diagnosis lookup.")

    context = _load_scan_context(connection, scan_row=scan_row, runs_root=effective_runs_root)
    output_dir = context["output_dir"]
    paths = context["paths"]
    artifact_health = context["artifact_health"]
    analysis_run_payload = context["analysis_run_payload"]
    calc_trace_payload = context["calc_trace_payload"] or {}
    feature_table_payload = context["feature_table_payload"] or []
    skipped_table_payload = context["skipped_table_payload"] or []
    selected_rows = context["selected_rows"]
    candidate_traces = list(calc_trace_payload.get("candidate_traces", []))
    rejected_traces = [trace for trace in candidate_traces if not trace.get("filter_evaluation", {}).get("passed")]
    passed_traces = [trace for trace in candidate_traces if trace.get("filter_evaluation", {}).get("passed")]
    selected_count = len(selected_rows)
    passed_hard_filters_count = len(passed_traces)

    top_reasons = _summarize_reasons(rejected_traces, limit=top_rejection_reasons)
    strategy_summary = _build_strategy_summary(
        candidate_traces,
        selected_rows=selected_rows,
        rejection_reason_limit=top_rejection_reasons,
    )
    near_passes = _build_near_pass_entries(rejected_traces, limit=top_near_passes)
    skipped_summary = _build_skipped_summary(skipped_table_payload, limit=top_skipped_reasons)

    if selected_count == 0:
        if candidate_traces:
            headline = "no candidates survived into the persisted selected set"
        else:
            headline = "no coarse-scan candidates were built for this scan"
    else:
        headline = "the scan produced at least one selected candidate"

    diagnosis = {
        "report_generated_at_utc": _utc_now_iso(),
        "scan_summary": _build_scan_summary(scan_row, output_dir),
        "analysis_run": analysis_run_payload,
        "traceability": {
            **paths,
            "latest_sync": _latest_sync_report(effective_log_dir, db_path),
        },
        "artifact_health": artifact_health,
        "diagnosis_summary": {
            "headline": headline,
            "candidate_count_total": len(candidate_traces),
            "passed_hard_filters_count": passed_hard_filters_count,
            "selected_count": selected_count,
            "rejected_count": len(rejected_traces),
            "feature_row_count": len(feature_table_payload),
            "skipped_count": skipped_summary["skipped_count"],
        },
        "strategy_summary": strategy_summary,
        "top_rejection_reasons": top_reasons,
        "closest_near_passes": near_passes,
        "skipped_summary": skipped_summary,
    }
    return diagnosis


def build_coverage_diagnosis(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
) -> dict[str, Any]:
    effective_runs_root, effective_log_dir = resolve_runtime_roots(
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
    )
    scan_row = _fetch_scan_row(
        connection,
        scan_run_id=scan_run_id,
        require_selected=prefer_latest_selected and scan_run_id is None,
    )
    if scan_row is None:
        raise SystemExit("No completed scan run was found for coverage diagnosis lookup.")

    output_dir = _resolve_scan_output_dir(scan_row, effective_runs_root)
    coverage_progress = _coverage_progress_summary(
        connection,
        runs_root=effective_runs_root,
        scan_row=scan_row,
    )
    latest_sync = _latest_sync_report(effective_log_dir, db_path)
    latest_cleanup = _latest_retention_cleanup(effective_runs_root)

    backlog_summary = (coverage_progress or {}).get("backlog_summary") if isinstance(coverage_progress, dict) else {}
    target_counts = (coverage_progress or {}).get("target_counts") if isinstance(coverage_progress, dict) else {}
    recent_cycle_summary = (
        (coverage_progress or {}).get("recent_cycle_summary") if isinstance(coverage_progress, dict) else {}
    )
    bottleneck_summary = (
        (coverage_progress or {}).get("bottleneck_summary") if isinstance(coverage_progress, dict) else {}
    )
    runtime_target_summary = (
        (coverage_progress or {}).get("runtime_target_summary") if isinstance(coverage_progress, dict) else {}
    )

    backlog_count = int((backlog_summary or {}).get("backlog_count") or 0)
    primary_backlog_bucket = _coverage_primary_backlog_bucket(backlog_summary)
    sync_health_summary = latest_sync.get("sync_health_summary") if isinstance(latest_sync, dict) else None
    cleanup_health_summary = latest_cleanup.get("health_summary") if isinstance(latest_cleanup, dict) else None
    latest_cycle_status = (
        str((recent_cycle_summary or {}).get("latest_status"))
        if isinstance(recent_cycle_summary, dict) and recent_cycle_summary.get("latest_status") is not None
        else None
    )
    runtime_target_status = (
        str((runtime_target_summary or {}).get("latest_status"))
        if isinstance(runtime_target_summary, dict) and runtime_target_summary.get("latest_status") is not None
        else None
    )
    suspected_bottleneck_group = (
        str((bottleneck_summary or {}).get("suspected_bottleneck_group"))
        if isinstance(bottleneck_summary, dict) and bottleneck_summary.get("suspected_bottleneck_group") is not None
        else None
    )

    attention_flags: list[str] = []
    if isinstance(sync_health_summary, dict) and sync_health_summary.get("has_problems"):
        attention_flags.append("sync_problems")
    if latest_cycle_status == "stalled":
        attention_flags.append("coverage_stalled")
    elif latest_cycle_status == "regressed":
        attention_flags.append("coverage_regressed")
    if runtime_target_status == "over_target":
        attention_flags.append("runtime_over_target")
    if isinstance(cleanup_health_summary, dict) and cleanup_health_summary.get("has_problems"):
        attention_flags.append("cleanup_problems")
    if backlog_count > 0 and primary_backlog_bucket is not None:
        attention_flags.append(f"backlog_{primary_backlog_bucket['bucket']}")

    diagnosis_summary = {
        "headline": _coverage_headline(
            backlog_count=backlog_count,
            latest_cycle_status=latest_cycle_status,
            runtime_target_status=runtime_target_status,
            sync_health_summary=sync_health_summary,
        ),
        "attention_flags": attention_flags,
        "market_type_count": int((target_counts or {}).get("market_type_count") or 0),
        "ready_count": int((target_counts or {}).get("ready_count") or 0),
        "backlog_count": backlog_count,
        "ready_ratio": (coverage_progress or {}).get("ready_ratio") if isinstance(coverage_progress, dict) else None,
        "latest_cycle_status": latest_cycle_status,
        "runtime_target_status": runtime_target_status,
        "suspected_bottleneck_group": suspected_bottleneck_group,
        "primary_backlog_bucket": primary_backlog_bucket,
        "recommended_focus": _coverage_recommended_focus(
            backlog_count=backlog_count,
            primary_backlog_bucket=primary_backlog_bucket,
            latest_cycle_status=latest_cycle_status,
            runtime_target_status=runtime_target_status,
            suspected_bottleneck_group=suspected_bottleneck_group,
            sync_health_summary=sync_health_summary,
        ),
    }

    return {
        "report_generated_at_utc": _utc_now_iso(),
        "scan_summary": _build_scan_summary(scan_row, output_dir),
        "coverage_progress": coverage_progress,
        "latest_sync": {
            "sync_health_summary": sync_health_summary,
            "sync_problem_rollup": latest_sync.get("sync_problem_rollup") if isinstance(latest_sync, dict) else None,
            "worker_health_rollup": latest_sync.get("worker_health_rollup") if isinstance(latest_sync, dict) else None,
        },
        "latest_cleanup": {
            "health_summary": cleanup_health_summary,
            "health_rollup": latest_cleanup.get("health_rollup") if isinstance(latest_cleanup, dict) else None,
        },
        "diagnosis_summary": diagnosis_summary,
    }
