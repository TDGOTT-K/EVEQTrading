from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from .analysis import analyze_market_scan
from .batch_pipeline import chunked
from .db_scan import resolve_db_scan_targets
from .db_scan import build_db_scan_candidates
from .models import FeeConfig, FilterConfig, ScanConfig
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


SQLITE_IN_LIMIT = 900


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _chunked_type_ids(type_ids: list[int]) -> list[list[int]]:
    return chunked(type_ids, SQLITE_IN_LIMIT) if type_ids else []


def _count_published_market_types(connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) FROM item_catalog WHERE is_published = 1 AND market_group IS NOT NULL"
    ).fetchone()
    return int(row[0]) if row is not None else 0


def _count_order_snapshot_ok(connection, *, region_id: int, type_ids: list[int]) -> int:
    if not type_ids:
        return 0
    total = 0
    for type_id_chunk in _chunked_type_ids(type_ids):
        placeholders = ",".join("?" for _ in type_id_chunk)
        row = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM sync_scope_state
            WHERE scope_type = 'market_orders'
              AND region_id = ?
              AND coverage_marker LIKE 'ok:%'
              AND type_id IN ({placeholders})
            """,
            [region_id, *type_id_chunk],
        ).fetchone()
        total += int(row[0]) if row is not None else 0
    return total


def _count_history_30d(connection, *, region_id: int, type_ids: list[int]) -> int:
    if not type_ids:
        return 0
    total = 0
    for type_id_chunk in _chunked_type_ids(type_ids):
        placeholders = ",".join("?" for _ in type_id_chunk)
        row = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT type_id
                FROM market_history_daily
                WHERE region_id = ?
                  AND type_id IN ({placeholders})
                GROUP BY type_id
                HAVING COUNT(*) >= 30
            )
            """,
            [region_id, *type_id_chunk],
        ).fetchone()
        total += int(row[0]) if row is not None else 0
    return total


def _count_both_order_and_history(connection, *, region_id: int, type_ids: list[int]) -> int:
    if not type_ids:
        return 0
    total = 0
    for type_id_chunk in _chunked_type_ids(type_ids):
        placeholders = ",".join("?" for _ in type_id_chunk)
        row = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT s.type_id
                FROM sync_scope_state s
                JOIN (
                    SELECT type_id
                    FROM market_history_daily
                    WHERE region_id = ?
                      AND type_id IN ({placeholders})
                    GROUP BY type_id
                    HAVING COUNT(*) >= 30
                ) h ON h.type_id = s.type_id
                WHERE s.scope_type = 'market_orders'
                  AND s.region_id = ?
                  AND s.coverage_marker LIKE 'ok:%'
                  AND s.type_id IN ({placeholders})
                GROUP BY s.type_id
            )
            """,
            [region_id, *type_id_chunk, region_id, *type_id_chunk],
        ).fetchone()
        total += int(row[0]) if row is not None else 0
    return total


def run_scan_benchmark(
    connection,
    *,
    db_path: Path,
    output_root: Path,
    region_id: int,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool,
    all_published_market_types: bool,
    limit: int | None,
    chunk_size: int,
    max_chunks: int | None,
    capital_budget_isk: float,
    test_position_isk: float,
    top_n: int,
    strategy_set: str,
    parameter_version: str,
) -> dict[str, Any]:
    benchmark_run_id = create_run_id(prefix="benchmark-scan")
    run_dir = ensure_run_dir(output_root, benchmark_run_id)

    fee_config = FeeConfig()
    filter_config = FilterConfig()
    scan_config = ScanConfig(
        strategy_set=strategy_set,
        parameter_version=parameter_version,
        test_position_isk=test_position_isk,
        capital_budget_isk=capital_budget_isk,
        top_n=top_n,
    )

    total_started = perf_counter()

    resolve_started = perf_counter()
    selected_targets = resolve_db_scan_targets(
        connection,
        region_id=region_id,
        explicit_type_ids=explicit_type_ids,
        type_id_file=type_id_file,
        ready_market_scan_types=ready_market_scan_types,
        all_published_market_types=all_published_market_types,
        limit=limit,
    ) or []
    resolve_seconds = perf_counter() - resolve_started

    selected_type_ids = [int(row["type_id"]) for row in selected_targets]
    selection_mode = selected_targets[0]["selection_mode"] if selected_targets else "none"

    coverage_started = perf_counter()
    coverage_snapshot = {
        "published_market_type_count": _count_published_market_types(connection),
        "selected_target_count": len(selected_targets),
        "types_with_order_snapshot_ok": _count_order_snapshot_ok(
            connection,
            region_id=region_id,
            type_ids=selected_type_ids,
        ),
        "types_with_history_30d": _count_history_30d(
            connection,
            region_id=region_id,
            type_ids=selected_type_ids,
        ),
        "types_with_both_order_and_history_30d": _count_both_order_and_history(
            connection,
            region_id=region_id,
            type_ids=selected_type_ids,
        ),
    }
    coverage_seconds = perf_counter() - coverage_started

    target_chunks = chunked(selected_targets, chunk_size)
    if max_chunks is not None:
        target_chunks = target_chunks[:max_chunks]

    totals = {
        "chunk_count": len(target_chunks),
        "candidate_count_built": 0,
        "candidate_count_selected": 0,
        "skipped_count": 0,
    }
    skipped_reason_counter: Counter[str] = Counter()
    strategy_pool_counter: Counter[str] = Counter()
    chunk_results: list[dict[str, Any]] = []
    build_total_seconds = 0.0
    analyze_total_seconds = 0.0

    for chunk_index, chunk_targets in enumerate(target_chunks, start=1):
        chunk_build_started = perf_counter()
        candidate_build = build_db_scan_candidates(
            connection,
            region_id=region_id,
            fee_config=fee_config,
            test_position_isk=test_position_isk,
            selected_targets=chunk_targets,
        )
        chunk_build_seconds = perf_counter() - chunk_build_started
        build_total_seconds += chunk_build_seconds

        chunk_analyze_started = perf_counter()
        analysis_run, calc_trace, selected_candidates, _ = analyze_market_scan(
            payload=candidate_build.payload,
            input_path=db_path.resolve(),
            fee_config=fee_config,
            filter_config=filter_config,
            scan_config=scan_config,
        )
        chunk_analyze_seconds = perf_counter() - chunk_analyze_started
        analyze_total_seconds += chunk_analyze_seconds

        totals["candidate_count_built"] += len(candidate_build.payload["candidates"])
        totals["candidate_count_selected"] += len(selected_candidates)
        totals["skipped_count"] += len(candidate_build.skipped_rows)
        skipped_reason_counter.update(str(row.get("reason") or "unknown") for row in candidate_build.skipped_rows)
        strategy_pool_counter.update(
            str(row.get("strategy_hint") or "unknown") for row in candidate_build.payload["candidates"]
        )

        chunk_results.append(
            {
                "chunk_index": chunk_index,
                "target_count": len(chunk_targets),
                "selection_mode": chunk_targets[0]["selection_mode"] if chunk_targets else selection_mode,
                "timing_seconds": {
                    "candidate_build": round(chunk_build_seconds, 6),
                    "analyze_scan": round(chunk_analyze_seconds, 6),
                    "total": round(chunk_build_seconds + chunk_analyze_seconds, 6),
                },
                "candidate_count_built": len(candidate_build.payload["candidates"]),
                "candidate_count_selected": len(selected_candidates),
                "skipped_count": len(candidate_build.skipped_rows),
                "passed_hard_filters_count": sum(
                    1 for trace in calc_trace.get("candidate_traces", []) if trace.get("filter_evaluation", {}).get("passed")
                ),
                "top_skipped_reasons": [
                    {"reason": reason, "count": count}
                    for reason, count in Counter(
                        str(row.get("reason") or "unknown") for row in candidate_build.skipped_rows
                    ).most_common(5)
                ],
            }
        )

    total_seconds = perf_counter() - total_started
    throughput = {
        "selected_targets_per_second": (
            round(len(selected_targets) / total_seconds, 2) if total_seconds > 0 else None
        ),
        "covered_types_per_second": (
            round(coverage_snapshot["types_with_both_order_and_history_30d"] / total_seconds, 2)
            if total_seconds > 0
            else None
        ),
        "candidate_builds_per_second": (
            round(totals["candidate_count_built"] / total_seconds, 2) if total_seconds > 0 else None
        ),
    }

    benchmark_payload = {
        "benchmark_run_id": benchmark_run_id,
        "benchmark_generated_at_utc": _utc_now_iso(),
        "db_path": str(db_path),
        "run_dir": str(run_dir),
        "region_id": region_id,
        "target_selection": {
            "selection_mode": selection_mode,
            "selected_target_count": len(selected_targets),
            "chunk_size": chunk_size,
            "executed_chunk_count": len(target_chunks),
            "max_chunks": max_chunks,
            "limit": limit,
        },
        "coverage_snapshot": coverage_snapshot,
        "timing_seconds": {
            "resolve_targets": round(resolve_seconds, 6),
            "coverage_snapshot": round(coverage_seconds, 6),
            "candidate_build_total": round(build_total_seconds, 6),
            "analyze_scan_total": round(analyze_total_seconds, 6),
            "total": round(total_seconds, 6),
        },
        "throughput": throughput,
        "aggregate_results": {
            **totals,
            "skipped_reason_counts": [
                {"reason": reason, "count": count} for reason, count in skipped_reason_counter.most_common(20)
            ],
            "strategy_pool_counts": [
                {"strategy_pool": pool, "count": count} for pool, count in strategy_pool_counter.most_common()
            ],
        },
        "chunk_results": chunk_results,
    }

    summary_lines = [
        "# EVEQTrading Scan Benchmark",
        "",
        f"- Benchmark Run ID: `{benchmark_run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Selection Mode: `{selection_mode}`",
        f"- Selected Targets: `{len(selected_targets)}`",
        f"- Executed Chunks: `{len(target_chunks)}`",
        f"- Types With Orders: `{coverage_snapshot['types_with_order_snapshot_ok']}`",
        f"- Types With 30d History: `{coverage_snapshot['types_with_history_30d']}`",
        f"- Types With Both: `{coverage_snapshot['types_with_both_order_and_history_30d']}`",
        f"- Candidates Built: `{totals['candidate_count_built']}`",
        f"- Candidates Selected: `{totals['candidate_count_selected']}`",
        f"- Total Seconds: `{total_seconds:.6f}`",
        "",
        "## Top Skipped Reasons",
    ]
    top_skipped = benchmark_payload["aggregate_results"]["skipped_reason_counts"]
    if not top_skipped:
        summary_lines.extend(["", "No skipped reasons were recorded."])
    else:
        for row in top_skipped[:10]:
            summary_lines.append(f"- `{row['reason']}`: `{row['count']}`")

    write_json(run_dir / "benchmark_scan.json", benchmark_payload)
    write_summary(run_dir / "summary.md", summary_lines)
    return benchmark_payload
