from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


@dataclass(frozen=True)
class OrderCacheProfile:
    name: str
    target_max_age_minutes: int
    description: str


ORDER_CACHE_PROFILES: dict[str, OrderCacheProfile] = {
    "hot": OrderCacheProfile(
        name="hot",
        target_max_age_minutes=5,
        description="for high-sensitivity strategies that need near-real-time Jita order state",
    ),
    "warm": OrderCacheProfile(
        name="warm",
        target_max_age_minutes=30,
        description="for most station-flip and medium-latency market scans",
    ),
    "cold": OrderCacheProfile(
        name="cold",
        target_max_age_minutes=120,
        description="for low-sensitivity research, ranking, and universe maintenance",
    ),
}

REMOTE_REFRESH_TARGET_STATES = frozenset({"missing_orders", "refresh_due_remote"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(raw_value: Any) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    normalized = raw_value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _hours_since(raw_value: Any, *, now: datetime) -> float | None:
    parsed = _parse_utc_iso(raw_value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds() / 3600.0, 0.0)


def resolve_order_cache_profile(profile_name: str) -> OrderCacheProfile:
    profile = ORDER_CACHE_PROFILES.get(str(profile_name or "").strip().lower())
    if profile is None:
        known = ", ".join(sorted(ORDER_CACHE_PROFILES))
        raise ValueError(f"unknown order cache profile: {profile_name!r}; expected one of {known}")
    return profile


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


def _load_history_counts(connection, *, region_id: int) -> dict[int, int]:
    rows = connection.execute(
        """
        SELECT type_id, COUNT(*) AS row_count
        FROM market_history_daily
        WHERE region_id = ?
        GROUP BY type_id
        """,
        (region_id,),
    ).fetchall()
    return {int(row["type_id"]): int(row["row_count"]) for row in rows}


def _load_order_state_rows(
    connection,
    *,
    region_id: int,
    order_type: str,
) -> dict[int, dict[str, Any]]:
    scope_suffix = f":{order_type}"
    rows = connection.execute(
        """
        SELECT scope_key, type_id, last_success_at, last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
        FROM sync_scope_state
        WHERE scope_type = 'market_orders'
          AND region_id = ?
        """,
        (region_id,),
    ).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        raw_scope_key = str(row["scope_key"] or "")
        if not raw_scope_key.endswith(scope_suffix):
            continue
        result[int(row["type_id"])] = {
            "last_success_at": row["last_success_at"],
            "last_attempt_at": row["last_attempt_at"],
            "next_scheduled_at": row["next_scheduled_at"],
            "failure_count": int(row["failure_count"] or 0),
            "last_error": row["last_error"],
            "coverage_marker": row["coverage_marker"],
        }
    return result


def _order_priority_bucket(row: dict[str, Any]) -> int:
    target_state = str(row["target_state"])
    if target_state == "missing_orders":
        return 0
    if target_state == "refresh_due_remote":
        return 1
    if target_state == "cache_hit":
        return 2
    if target_state == "refresh_due_wait":
        return 3
    return 4


def _order_priority_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    bucket = _order_priority_bucket(row)
    age_minutes = row.get("order_age_minutes")
    if bucket in {1, 3}:
        age_key = -(float(age_minutes) if age_minutes is not None else 9999999.0)
    elif bucket == 2:
        age_key = float(age_minutes) if age_minutes is not None else 9999999.0
    else:
        age_key = 0.0
    return (bucket, age_key, int(row["type_id"]))


def _cache_hit_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    age_minutes = row.get("order_age_minutes")
    return (
        float(age_minutes) if age_minutes is not None else 9999999.0,
        int(row["type_id"]),
    )


def _rows_to_targets(
    rows: list[dict[str, Any]],
    *,
    selection_mode: str,
) -> list[dict[str, Any]]:
    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "selection_mode": selection_mode,
        }
        for row in rows
    ]


def collect_order_refresh_snapshot(
    connection,
    *,
    region_id: int,
    profile_name: str,
    order_type: str = "all",
    limit: int | None = None,
    selected_type_ids: set[int] | None = None,
) -> dict[str, Any]:
    profile = resolve_order_cache_profile(profile_name)
    now = _utc_now()
    market_rows = _load_market_type_rows(connection)
    if selected_type_ids is not None:
        market_rows = [row for row in market_rows if int(row["type_id"]) in selected_type_ids]
    if limit is not None:
        market_rows = market_rows[:limit]

    history_counts = _load_history_counts(connection, region_id=region_id)
    order_state_rows = _load_order_state_rows(connection, region_id=region_id, order_type=order_type)

    annotated_rows: list[dict[str, Any]] = []
    for row in market_rows:
        type_id = int(row["type_id"])
        history_row_count = int(history_counts.get(type_id) or 0)
        has_history_30d = history_row_count >= 30
        order_state = order_state_rows.get(type_id)
        coverage_marker = str((order_state or {}).get("coverage_marker") or "")
        has_snapshot_ok = coverage_marker.startswith("ok:")
        last_success_at = (order_state or {}).get("last_success_at")
        next_scheduled_at = (order_state or {}).get("next_scheduled_at")
        next_due_time = _parse_utc_iso(next_scheduled_at)
        order_age_hours = _hours_since(last_success_at, now=now)
        order_age_minutes = order_age_hours * 60.0 if order_age_hours is not None else None
        local_fresh = (
            has_snapshot_ok
            and order_age_minutes is not None
            and order_age_minutes <= float(profile.target_max_age_minutes)
        )
        remote_allowed = next_due_time is None or next_due_time <= now

        if not has_history_30d:
            target_state = "missing_history"
        elif not has_snapshot_ok:
            target_state = "missing_orders"
        elif local_fresh:
            target_state = "cache_hit"
        elif remote_allowed:
            target_state = "refresh_due_remote"
        else:
            target_state = "refresh_due_wait"

        annotated_rows.append(
            {
                **row,
                "history_row_count": history_row_count,
                "has_history_30d": has_history_30d,
                "has_order_snapshot_ok": has_snapshot_ok,
                "order_last_success_at": last_success_at,
                "order_last_attempt_at": (order_state or {}).get("last_attempt_at"),
                "order_next_scheduled_at": next_scheduled_at,
                "order_failure_count": int((order_state or {}).get("failure_count") or 0),
                "order_last_error": (order_state or {}).get("last_error"),
                "order_coverage_marker": (order_state or {}).get("coverage_marker"),
                "order_age_minutes": order_age_minutes,
                "profile_name": profile.name,
                "target_max_age_minutes": profile.target_max_age_minutes,
                "local_fresh": local_fresh,
                "remote_refresh_allowed": remote_allowed,
                "target_state": target_state,
            }
        )

    history_ready_rows = [row for row in annotated_rows if bool(row["has_history_30d"])]
    missing_orders_rows = [row for row in history_ready_rows if str(row["target_state"]) == "missing_orders"]
    refresh_due_remote_rows = [row for row in history_ready_rows if str(row["target_state"]) == "refresh_due_remote"]
    refresh_due_wait_rows = [row for row in history_ready_rows if str(row["target_state"]) == "refresh_due_wait"]
    cache_hit_rows = [row for row in history_ready_rows if str(row["target_state"]) == "cache_hit"]
    priority_rows = sorted(
        [row for row in history_ready_rows if str(row["target_state"]) != "missing_history"],
        key=_order_priority_sort_key,
    )

    return {
        "generated_at_utc": _utc_now_iso(),
        "region_id": region_id,
        "order_type": order_type,
        "profile": {
            "name": profile.name,
            "target_max_age_minutes": profile.target_max_age_minutes,
            "description": profile.description,
        },
        "target_counts": {
            "market_type_count": len(market_rows),
            "history_ready_count": len(history_ready_rows),
            "missing_orders_count": len(missing_orders_rows),
            "refresh_due_remote_count": len(refresh_due_remote_rows),
            "refresh_due_wait_count": len(refresh_due_wait_rows),
            "cache_hit_count": len(cache_hit_rows),
            "priority_target_count": len(priority_rows),
        },
        "type_ids": {
            "missing_orders": [int(row["type_id"]) for row in missing_orders_rows],
            "refresh_due_remote": [int(row["type_id"]) for row in refresh_due_remote_rows],
            "refresh_due_wait": [int(row["type_id"]) for row in refresh_due_wait_rows],
            "cache_hit": [int(row["type_id"]) for row in cache_hit_rows],
            "priority": [int(row["type_id"]) for row in priority_rows],
        },
        "rows": {
            "priority": priority_rows,
            "missing_orders": missing_orders_rows,
            "refresh_due_remote": refresh_due_remote_rows,
            "refresh_due_wait": refresh_due_wait_rows,
            "cache_hit": cache_hit_rows,
        },
        "sample_rows": {
            "priority": priority_rows[:20],
            "missing_orders": missing_orders_rows[:10],
            "refresh_due_remote": refresh_due_remote_rows[:10],
            "refresh_due_wait": refresh_due_wait_rows[:10],
            "cache_hit": cache_hit_rows[:10],
        },
    }


def build_order_refresh_targets(
    connection,
    *,
    region_id: int,
    profile_name: str,
    order_type: str = "all",
    limit: int | None = None,
    selected_type_ids: set[int] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=order_type,
        limit=None,
        selected_type_ids=selected_type_ids,
    )
    rows = list(snapshot["rows"]["priority"])
    if limit is not None:
        rows = rows[:limit]
    targets = _rows_to_targets(rows, selection_mode=f"order-refresh-{profile_name}")
    return snapshot, targets


def build_order_remote_refresh_targets(
    connection,
    *,
    region_id: int,
    profile_name: str,
    order_type: str = "all",
    limit: int | None = None,
    selected_type_ids: set[int] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=order_type,
        limit=None,
        selected_type_ids=selected_type_ids,
    )
    rows = [
        row
        for row in snapshot["rows"]["priority"]
        if str(row["target_state"]) in REMOTE_REFRESH_TARGET_STATES
    ]
    if limit is not None:
        rows = rows[:limit]
    targets = _rows_to_targets(rows, selection_mode=f"order-refresh-remote-{profile_name}")
    return snapshot, targets


def build_order_cache_hit_targets(
    connection,
    *,
    region_id: int,
    profile_name: str,
    order_type: str = "all",
    limit: int | None = None,
    selected_type_ids: set[int] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=order_type,
        limit=None,
        selected_type_ids=selected_type_ids,
    )
    rows = sorted(list(snapshot["rows"]["cache_hit"]), key=_cache_hit_sort_key)
    if limit is not None:
        rows = rows[:limit]
    targets = _rows_to_targets(rows, selection_mode=f"order-cache-hit-{profile_name}")
    return snapshot, targets


def plan_order_refresh(
    connection,
    *,
    output_root: Path,
    region_id: int,
    profile_name: str,
    order_type: str = "all",
    limit: int | None = None,
) -> dict[str, Any]:
    run_id = create_run_id(prefix="order-refresh-plan")
    run_dir = ensure_run_dir(output_root, run_id)
    snapshot, targets = build_order_refresh_targets(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=order_type,
        limit=limit,
    )

    type_ids_path = run_dir / "priority_type_ids.txt"
    type_ids_path.write_text("".join(f"{row['type_id']}\n" for row in targets), encoding="utf-8")

    payload = {
        "order_refresh_plan_run_id": run_id,
        "generated_at_utc": snapshot["generated_at_utc"],
        "region_id": region_id,
        "order_type": order_type,
        "profile": snapshot["profile"],
        "target_counts": snapshot["target_counts"],
        "selected_target_count": len(targets),
        "files": {
            "priority_type_ids_path": str(type_ids_path),
        },
        "sample_rows": snapshot["sample_rows"],
    }

    summary_lines = [
        "# EVEQTrading Order Refresh Plan",
        "",
        f"- Plan Run ID: `{run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Order Type: `{order_type}`",
        f"- Profile: `{snapshot['profile']['name']}`",
        f"- Target Max Age Minutes: `{snapshot['profile']['target_max_age_minutes']}`",
        f"- History Ready Count: `{snapshot['target_counts']['history_ready_count']}`",
        f"- Missing Orders Count: `{snapshot['target_counts']['missing_orders_count']}`",
        f"- Refresh Due Remote Count: `{snapshot['target_counts']['refresh_due_remote_count']}`",
        f"- Refresh Due Wait Count: `{snapshot['target_counts']['refresh_due_wait_count']}`",
        f"- Cache Hit Count: `{snapshot['target_counts']['cache_hit_count']}`",
        f"- Selected Target Count: `{len(targets)}`",
    ]

    write_json(run_dir / "order_refresh_plan.json", payload)
    write_json(run_dir / "order_refresh_snapshot.json", snapshot)
    write_summary(run_dir / "summary.md", summary_lines)
    return payload
