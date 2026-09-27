from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

PLEX_TYPE_ID = 44992
GLOBAL_PLEX_REGION_ID = 19000001
GLOBAL_PLEX_SWITCH_DATE = "2025-07-07"


@dataclass(frozen=True)
class HistorySourceSegment:
    source_region_id: int
    market_scope: str
    start_date_inclusive: str | None = None
    end_date_exclusive: str | None = None


def is_global_plex_type(type_id: int) -> bool:
    return int(type_id) == PLEX_TYPE_ID


def resolve_order_source_region_id(*, region_id: int, type_id: int) -> int:
    if is_global_plex_type(type_id) and int(region_id) != GLOBAL_PLEX_REGION_ID:
        return GLOBAL_PLEX_REGION_ID
    return int(region_id)


def resolve_history_source_segments(*, region_id: int, type_id: int) -> list[HistorySourceSegment]:
    logical_region_id = int(region_id)
    if not is_global_plex_type(type_id) or logical_region_id == GLOBAL_PLEX_REGION_ID:
        market_scope = "global_plex" if logical_region_id == GLOBAL_PLEX_REGION_ID and is_global_plex_type(type_id) else "regional"
        return [HistorySourceSegment(source_region_id=logical_region_id, market_scope=market_scope)]
    return [
        HistorySourceSegment(
            source_region_id=logical_region_id,
            market_scope="regional",
            end_date_exclusive=GLOBAL_PLEX_SWITCH_DATE,
        ),
        HistorySourceSegment(
            source_region_id=GLOBAL_PLEX_REGION_ID,
            market_scope="global_plex",
            start_date_inclusive=GLOBAL_PLEX_SWITCH_DATE,
        ),
    ]


def describe_history_source(*, region_id: int, type_id: int) -> dict[str, Any]:
    segments = resolve_history_source_segments(region_id=region_id, type_id=type_id)
    return {
        "is_stitched": len(segments) > 1,
        "segments": [
            {
                "source_region_id": int(segment.source_region_id),
                "market_scope": str(segment.market_scope),
                "start_date_inclusive": segment.start_date_inclusive,
                "end_date_exclusive": segment.end_date_exclusive,
            }
            for segment in segments
        ],
    }


def _build_history_sql(*, region_id: int, type_ids: set[int], start_date_inclusive: str | None, end_date_exclusive: str | None) -> tuple[str, list[Any]]:
    placeholders = ",".join("?" for _ in sorted(type_ids))
    sql = (
        "SELECT type_id, region_id, market_date, average_price, highest_price, lowest_price, "
        "volume, order_count, fetched_at "
        "FROM market_history_daily "
        f"WHERE region_id = ? AND type_id IN ({placeholders})"
    )
    params: list[Any] = [int(region_id), *sorted(type_ids)]
    if start_date_inclusive is not None:
        sql += " AND market_date >= ?"
        params.append(start_date_inclusive)
    if end_date_exclusive is not None:
        sql += " AND market_date < ?"
        params.append(end_date_exclusive)
    sql += " ORDER BY type_id, market_date"
    return sql, params


def load_canonical_history_series_map(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_ids: set[int] | None,
) -> dict[int, list[dict[str, Any]]]:
    if type_ids is not None and not type_ids:
        return {}

    requested_type_ids = set(type_ids or [])
    series: dict[int, list[dict[str, Any]]] = defaultdict(list)

    regional_type_ids = requested_type_ids or {
        int(row["type_id"])
        for row in connection.execute(
            "SELECT DISTINCT type_id FROM market_history_daily WHERE region_id = ?",
            (int(region_id),),
        ).fetchall()
    }
    special_type_ids = {
        type_id
        for type_id in regional_type_ids
        if len(resolve_history_source_segments(region_id=region_id, type_id=type_id)) > 1
    }

    if regional_type_ids:
        sql, params = _build_history_sql(
            region_id=int(region_id),
            type_ids=regional_type_ids,
            start_date_inclusive=None,
            end_date_exclusive=None,
        )
        for row in connection.execute(sql, params).fetchall():
            type_id = int(row["type_id"])
            market_date = str(row["market_date"])
            if type_id in special_type_ids and market_date >= GLOBAL_PLEX_SWITCH_DATE:
                continue
            series[type_id].append(
                {
                    "market_date": market_date,
                    "average_price": float(row["average_price"]),
                    "highest_price": float(row["highest_price"]),
                    "lowest_price": float(row["lowest_price"]),
                    "volume": float(row["volume"]),
                    "order_count": int(row["order_count"]),
                    "fetched_at": str(row["fetched_at"]),
                    "source_region_id": int(row["region_id"]),
                    "market_scope": "regional",
                }
            )

    if special_type_ids:
        sql, params = _build_history_sql(
            region_id=GLOBAL_PLEX_REGION_ID,
            type_ids=special_type_ids,
            start_date_inclusive=GLOBAL_PLEX_SWITCH_DATE,
            end_date_exclusive=None,
        )
        for row in connection.execute(sql, params).fetchall():
            type_id = int(row["type_id"])
            series[type_id].append(
                {
                    "market_date": str(row["market_date"]),
                    "average_price": float(row["average_price"]),
                    "highest_price": float(row["highest_price"]),
                    "lowest_price": float(row["lowest_price"]),
                    "volume": float(row["volume"]),
                    "order_count": int(row["order_count"]),
                    "fetched_at": str(row["fetched_at"]),
                    "source_region_id": int(row["region_id"]),
                    "market_scope": "global_plex",
                }
            )

    for type_id in list(series):
        series[type_id].sort(key=lambda row: str(row["market_date"]))

    if type_ids is None:
        return dict(series)
    return {int(type_id): list(series.get(int(type_id), [])) for type_id in sorted(type_ids)}


def load_canonical_history_series(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_id: int,
) -> list[dict[str, Any]]:
    return load_canonical_history_series_map(
        connection,
        region_id=region_id,
        type_ids={int(type_id)},
    ).get(int(type_id), [])
