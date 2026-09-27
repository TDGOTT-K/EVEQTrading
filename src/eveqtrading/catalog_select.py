from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


def _read_type_ids_from_file(path: Path) -> list[int]:
    type_ids: list[int] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        type_ids.append(int(line))
    return type_ids


def resolve_type_targets(
    connection: sqlite3.Connection,
    *,
    region_id: int | None = None,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool = False,
    all_published_market_types: bool,
    limit: int | None,
) -> list[dict[str, Any]]:
    selected_type_ids: list[int] = []
    selection_mode = "explicit"
    if explicit_type_ids:
        selected_type_ids.extend(explicit_type_ids)
    if type_id_file is not None:
        selected_type_ids.extend(_read_type_ids_from_file(type_id_file))
        selection_mode = "explicit+file"
    if selected_type_ids:
        unique_type_ids = sorted(set(selected_type_ids))
        placeholders = ",".join("?" for _ in unique_type_ids)
        sql = (
            "SELECT type_id, type_name FROM item_catalog "
            f"WHERE type_id IN ({placeholders}) "
            "ORDER BY type_id"
        )
        rows = [dict(row) for row in connection.execute(sql, unique_type_ids).fetchall()]
    elif ready_market_scan_types:
        if region_id is None:
            raise ValueError("region_id is required when ready_market_scan_types is enabled.")
        selection_mode = "ready-market-scan-types"
        sql = """
            SELECT c.type_id, c.type_name
            FROM item_catalog c
            JOIN sync_scope_state s
              ON s.type_id = c.type_id
             AND s.scope_type = 'market_orders'
             AND s.region_id = ?
             AND s.coverage_marker LIKE 'ok:%'
            JOIN (
                SELECT type_id
                FROM market_history_daily
                WHERE region_id = ?
                GROUP BY type_id
                HAVING COUNT(*) >= 30
            ) h ON h.type_id = c.type_id
            WHERE c.is_published = 1
              AND c.market_group IS NOT NULL
            ORDER BY c.type_id
        """
        params: list[Any] = [region_id, region_id]
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = [dict(row) for row in connection.execute(sql, params).fetchall()]
    elif all_published_market_types:
        selection_mode = "all-published-market-types"
        sql = (
            "SELECT type_id, type_name FROM item_catalog "
            "WHERE is_published = 1 AND market_group IS NOT NULL "
            "ORDER BY type_id"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = [dict(row) for row in connection.execute(sql).fetchall()]
    else:
        raise ValueError("No type selection specified. Use --type-id, --type-id-file, or --all-published-market-types.")

    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": row["type_name"],
            "selection_mode": selection_mode,
        }
        for row in rows
    ]
