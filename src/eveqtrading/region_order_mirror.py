from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .coverage import collect_market_coverage_snapshot
from .db import record_data_fetch_run
from .order_fetch import OrderFetchConfig, fetch_market_orders
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_market_type_rows(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT type_id, type_name, market_group
        FROM item_catalog
        WHERE is_published = 1
          AND market_group IS NOT NULL
        ORDER BY type_id
        """
    ).fetchall()
    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "market_group": str(row["market_group"]),
        }
        for row in rows
    ]


def build_region_order_mirror_targets(
    connection,
    *,
    region_id: int,
    target_universe: str,
    limit: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    coverage_snapshot = collect_market_coverage_snapshot(connection, region_id=region_id)
    market_rows = _load_market_type_rows(connection)

    if target_universe == "history_ready":
        selected_type_ids = set(int(type_id) for type_id in coverage_snapshot["type_ids"]["history_ready"])
    elif target_universe == "all_published":
        selected_type_ids = {int(row["type_id"]) for row in market_rows}
    else:
        raise ValueError(f"unsupported target universe: {target_universe!r}")

    selected_rows = [row for row in market_rows if int(row["type_id"]) in selected_type_ids]
    if limit is not None:
        selected_rows = selected_rows[:limit]

    targets = [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "selection_mode": f"region-order-mirror-{target_universe}",
        }
        for row in selected_rows
    ]
    return coverage_snapshot, targets


def run_region_order_mirror_once(
    connection,
    *,
    output_root: Path,
    region_id: int,
    target_universe: str,
    orders_config: OrderFetchConfig,
    limit: int | None = None,
) -> dict[str, Any]:
    mirror_run_id = create_run_id(prefix="region-order-mirror")
    mirror_run_dir = ensure_run_dir(output_root, mirror_run_id)
    started_at = _utc_now()

    coverage_snapshot, targets = build_region_order_mirror_targets(
        connection,
        region_id=region_id,
        target_universe=target_universe,
        limit=limit,
    )
    executed_type_id_set = {int(row["type_id"]) for row in targets}
    before_scope = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        selected_type_ids=executed_type_id_set if executed_type_id_set else set(),
    )
    before_global = collect_market_coverage_snapshot(connection, region_id=region_id)

    effective_orders_config = replace(
        orders_config,
        fetch_mode="region_pages",
        ignore_existing_freshness=True,
    )
    orders_result = fetch_market_orders(
        connection,
        output_root=output_root,
        type_targets=targets,
        config=effective_orders_config,
    )

    after_scope = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        selected_type_ids=executed_type_id_set if executed_type_id_set else set(),
    )
    after_global = collect_market_coverage_snapshot(connection, region_id=region_id)
    finished_at = _utc_now()

    payload = {
        "mirror_run_id": mirror_run_id,
        "started_at_utc": started_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "region_id": region_id,
        "target_universe": target_universe,
        "limit": limit,
        "selected_target_count": len(targets),
        "history_ready_count_before": int(coverage_snapshot["target_counts"]["history_ready_count"] or 0),
        "orders_fetch_run_id": orders_result["run_id"],
        "orders_fetch_run_dir": str(orders_result["run_dir"]),
        "orders_fetch": orders_result["fetch_run"],
        "before_executed_scope_counts": before_scope["target_counts"],
        "after_executed_scope_counts": after_scope["target_counts"],
        "before_global_counts": before_global["target_counts"],
        "after_global_counts": after_global["target_counts"],
        "orders_ready_gain_in_executed_scope": (
            int(after_scope["target_counts"]["orders_ready_count"] or 0)
            - int(before_scope["target_counts"]["orders_ready_count"] or 0)
        ),
        "ready_gain_in_executed_scope": (
            int(after_scope["target_counts"]["ready_count"] or 0)
            - int(before_scope["target_counts"]["ready_count"] or 0)
        ),
    }

    summary_lines = [
        "# EVEQTrading Region Order Mirror",
        "",
        f"- Mirror Run ID: `{mirror_run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Target Universe: `{target_universe}`",
        f"- Selected Target Count: `{len(targets)}`",
        f"- Orders Fetch Run ID: `{orders_result['run_id']}`",
        f"- Effective Fetch Mode: `{orders_result['fetch_run']['effective_fetch_mode']}`",
        f"- Pages Fetched: `{orders_result['fetch_run']['pages_fetched']}`",
        f"- Success: `{orders_result['fetch_run']['type_count_success']}`",
        f"- Orders Ready Gain In Executed Scope: `{payload['orders_ready_gain_in_executed_scope']}`",
        f"- Ready Gain In Executed Scope: `{payload['ready_gain_in_executed_scope']}`",
    ]

    write_json(mirror_run_dir / "region_order_mirror.json", payload)
    write_summary(mirror_run_dir / "summary.md", summary_lines)
    record_data_fetch_run(
        connection,
        fetch_run_id=mirror_run_id,
        fetch_kind="market_orders_region_mirror",
        source="esi-public-orders-region-mirror",
        output_dir=mirror_run_dir.resolve(),
        scope={
            "region_id": region_id,
            "target_universe": target_universe,
            "limit": limit,
        },
        details={
            "selected_target_count": len(targets),
            "orders_fetch_run_id": orders_result["run_id"],
            "orders_fetch_effective_mode": orders_result["fetch_run"]["effective_fetch_mode"],
            "orders_fetch_pages_fetched": orders_result["fetch_run"]["pages_fetched"],
        },
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        status="completed" if int(orders_result["fetch_run"]["type_count_failed"] or 0) == 0 else "completed_with_failures",
    )
    connection.commit()

    return {
        "mirror_run_id": mirror_run_id,
        "run_dir": mirror_run_dir.resolve(),
        "payload": payload,
        "orders_result": orders_result,
    }
