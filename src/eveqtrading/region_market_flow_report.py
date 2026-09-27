from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .market_scope import PLEX_TYPE_ID, resolve_history_source_segments
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary

UNKNOWN_MARKET_GROUP_ID = -1
SPECIAL_CANONICAL_TYPE_IDS = {PLEX_TYPE_ID}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_isk_human(amount: Any) -> str:
    try:
        numeric = float(amount or 0.0)
    except (TypeError, ValueError):
        return "-"
    abs_numeric = abs(numeric)
    if abs_numeric >= 1_000_000_000_000:
        return f"{numeric / 1_000_000_000_000:.2f}万亿"
    if abs_numeric >= 100_000_000:
        return f"{numeric / 100_000_000:.2f}亿"
    if abs_numeric >= 10_000:
        return f"{numeric / 10_000:.2f}万"
    return f"{numeric:.2f}"


def _format_percent(value: Any) -> str:
    try:
        return f"{float(value or 0.0) * 100.0:.2f}%"
    except (TypeError, ValueError):
        return "-"


def _render_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        return ["- 无"]
    divider = ["---" for _ in headers]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(divider) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _load_region_name(connection: sqlite3.Connection, region_id: int) -> str:
    row = connection.execute(
        """
        SELECT location_name
        FROM market_location_catalog
        WHERE location_id = ? AND location_kind = 'region'
        """,
        (region_id,),
    ).fetchone()
    if row is not None and row["location_name"]:
        return str(row["location_name"])
    return f"region:{region_id}"


def _load_market_groups(connection: sqlite3.Connection) -> dict[int, dict[str, Any]]:
    try:
        rows = connection.execute(
            """
            SELECT market_group_id, parent_group_id, market_group_name, market_group_name_en, market_group_name_zh
            FROM market_group_catalog
            ORDER BY market_group_id
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise RuntimeError("market_group_catalog 不存在，请先重新执行 import-sde。") from exc

    groups: dict[int, dict[str, Any]] = {}
    for row in rows:
        market_group_id = int(row["market_group_id"])
        groups[market_group_id] = {
            "market_group_id": market_group_id,
            "parent_group_id": int(row["parent_group_id"]) if row["parent_group_id"] is not None else None,
            "market_group_name": str(row["market_group_name"]),
            "market_group_name_en": row["market_group_name_en"],
            "market_group_name_zh": row["market_group_name_zh"],
            "children": [],
            "direct_items": [],
        }

    groups[UNKNOWN_MARKET_GROUP_ID] = {
        "market_group_id": UNKNOWN_MARKET_GROUP_ID,
        "parent_group_id": None,
        "market_group_name": "未分类",
        "market_group_name_en": "Uncategorized",
        "market_group_name_zh": "未分类",
        "children": [],
        "direct_items": [],
    }

    for group in list(groups.values()):
        parent_group_id = group.get("parent_group_id")
        if parent_group_id is None:
            continue
        parent = groups.get(int(parent_group_id))
        if parent is None:
            groups[UNKNOWN_MARKET_GROUP_ID]["children"].append(group["market_group_id"])
            group["parent_group_id"] = UNKNOWN_MARKET_GROUP_ID
            continue
        parent["children"].append(group["market_group_id"])

    return groups


def _resolve_logical_market_date(connection: sqlite3.Connection, *, region_id: int) -> str | None:
    row = connection.execute(
        "SELECT MAX(market_date) AS market_date FROM market_history_daily WHERE region_id = ?",
        (region_id,),
    ).fetchone()
    if row is not None and row["market_date"]:
        return str(row["market_date"])

    candidate_dates: list[str] = []
    for type_id in sorted(SPECIAL_CANONICAL_TYPE_IDS):
        segments = resolve_history_source_segments(region_id=region_id, type_id=type_id)
        if len(segments) <= 1:
            continue
        for segment in segments:
            sql = "SELECT MAX(market_date) AS market_date FROM market_history_daily WHERE region_id = ? AND type_id = ?"
            params: list[Any] = [int(segment.source_region_id), int(type_id)]
            if segment.start_date_inclusive is not None:
                sql += " AND market_date >= ?"
                params.append(segment.start_date_inclusive)
            if segment.end_date_exclusive is not None:
                sql += " AND market_date < ?"
                params.append(segment.end_date_exclusive)
            row = connection.execute(sql, params).fetchone()
            if row is not None and row["market_date"]:
                candidate_dates.append(str(row["market_date"]))

    if not candidate_dates:
        return None
    return max(candidate_dates)


def _resolve_history_segment_for_date(*, region_id: int, type_id: int, market_date: str) -> Any | None:
    for segment in resolve_history_source_segments(region_id=region_id, type_id=type_id):
        if segment.start_date_inclusive is not None and market_date < segment.start_date_inclusive:
            continue
        if segment.end_date_exclusive is not None and market_date >= segment.end_date_exclusive:
            continue
        return segment
    return None


def _load_history_rows_for_date(
    connection: sqlite3.Connection,
    *,
    source_region_id: int,
    market_date: str,
    type_ids: set[int] | None = None,
    exclude_type_ids: set[int] | None = None,
) -> list[sqlite3.Row]:
    sql = """
        SELECT
            h.type_id,
            h.region_id,
            h.market_date,
            h.average_price,
            h.volume,
            h.order_count,
            h.fetched_at,
            i.type_name,
            i.type_name_en,
            i.type_name_zh,
            i.market_group_id,
            i.market_group
        FROM market_history_daily h
        LEFT JOIN item_catalog i ON i.type_id = h.type_id
        WHERE h.region_id = ? AND h.market_date = ?
    """
    params: list[Any] = [int(source_region_id), str(market_date)]
    if type_ids:
        placeholders = ",".join("?" for _ in sorted(type_ids))
        sql += f" AND h.type_id IN ({placeholders})"
        params.extend(sorted(int(type_id) for type_id in type_ids))
    if exclude_type_ids:
        placeholders = ",".join("?" for _ in sorted(exclude_type_ids))
        sql += f" AND h.type_id NOT IN ({placeholders})"
        params.extend(sorted(int(type_id) for type_id in exclude_type_ids))
    return connection.execute(sql, params).fetchall()


def _normalize_item_row(row: sqlite3.Row) -> dict[str, Any]:
    type_id = int(row["type_id"])
    turnover_isk = float(row["average_price"]) * float(row["volume"])
    market_group_id = int(row["market_group_id"]) if row["market_group_id"] is not None else UNKNOWN_MARKET_GROUP_ID
    return {
        "type_id": type_id,
        "source_region_id": int(row["region_id"]),
        "market_date": str(row["market_date"]),
        "type_name": str(row["type_name"] or row["type_name_zh"] or row["type_name_en"] or f"type:{type_id}"),
        "average_price": float(row["average_price"]),
        "volume": float(row["volume"]),
        "order_count": int(row["order_count"]),
        "fetched_at": str(row["fetched_at"]),
        "market_group_id": market_group_id,
        "market_group_name": str(row["market_group"] or "未分类"),
        "turnover_isk": turnover_isk,
    }


def _load_daily_item_flows(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    market_date: str,
) -> list[dict[str, Any]]:
    override_type_ids: set[int] = set()
    override_rows: list[sqlite3.Row] = []
    for type_id in sorted(SPECIAL_CANONICAL_TYPE_IDS):
        segment = _resolve_history_segment_for_date(region_id=region_id, type_id=type_id, market_date=str(market_date))
        if segment is None or int(segment.source_region_id) == int(region_id):
            continue
        override_type_ids.add(int(type_id))
        override_rows.extend(
            _load_history_rows_for_date(
                connection,
                source_region_id=int(segment.source_region_id),
                market_date=str(market_date),
                type_ids={int(type_id)},
            )
        )

    rows = _load_history_rows_for_date(
        connection,
        source_region_id=int(region_id),
        market_date=str(market_date),
        exclude_type_ids=override_type_ids,
    )
    merged_rows = [*rows, *override_rows]

    items: list[dict[str, Any]] = []
    for row in merged_rows:
        items.append(_normalize_item_row(row))
    items.sort(key=lambda item: (-float(item["turnover_isk"]), int(item["type_id"])))
    return items


def _build_group_path_map(groups: dict[int, dict[str, Any]]) -> dict[int, list[str]]:
    path_map: dict[int, list[str]] = {}

    def resolve(group_id: int) -> list[str]:
        if group_id in path_map:
            return path_map[group_id]
        node = groups[group_id]
        parent_group_id = node.get("parent_group_id")
        if parent_group_id is None or parent_group_id not in groups:
            path = [str(node["market_group_name"])]
        else:
            path = [*resolve(int(parent_group_id)), str(node["market_group_name"])]
        path_map[group_id] = path
        return path

    for group_id in groups:
        resolve(int(group_id))
    return path_map


def _aggregate_group_tree(
    groups: dict[int, dict[str, Any]],
    *,
    top_items_per_group: int,
) -> dict[int, dict[str, Any]]:
    aggregates: dict[int, dict[str, Any]] = {}

    def walk(group_id: int) -> dict[str, Any]:
        group = groups[group_id]
        direct_items = sorted(
            list(group.get("direct_items") or []),
            key=lambda item: (-float(item["turnover_isk"]), str(item["type_name"]), int(item["type_id"])),
        )
        child_aggregates = [walk(int(child_id)) for child_id in group.get("children", [])]
        child_aggregates.sort(
            key=lambda child: (-float(child["total_turnover_isk"]), str(child["market_group_name"]), int(child["market_group_id"]))
        )

        direct_turnover_isk = sum(float(item["turnover_isk"]) for item in direct_items)
        total_turnover_isk = direct_turnover_isk + sum(float(child["total_turnover_isk"]) for child in child_aggregates)
        top_item_candidates = direct_items[:top_items_per_group]
        for child in child_aggregates:
            top_item_candidates.extend(child["top_items"])
        top_items = sorted(
            top_item_candidates,
            key=lambda item: (-float(item["turnover_isk"]), str(item["type_name"]), int(item["type_id"])),
        )[:top_items_per_group]

        aggregate = {
            "market_group_id": group_id,
            "market_group_name": group["market_group_name"],
            "parent_group_id": group.get("parent_group_id"),
            "direct_turnover_isk": direct_turnover_isk,
            "total_turnover_isk": total_turnover_isk,
            "direct_item_count": len(direct_items),
            "total_item_count": len(direct_items) + sum(int(child["total_item_count"]) for child in child_aggregates),
            "child_groups": child_aggregates,
            "top_items": top_items,
        }
        aggregates[group_id] = aggregate
        return aggregate

    for group_id in list(groups):
        if group_id not in aggregates:
            walk(int(group_id))
    return aggregates


def _attach_rankings(payloads: list[dict[str, Any]], *, key: str) -> list[dict[str, Any]]:
    for index, payload in enumerate(payloads, start=1):
        payload[key] = index
    return payloads


def _render_group_section(
    lines: list[str],
    aggregate: dict[str, Any],
    *,
    share_of_total: float,
    heading_level: int,
) -> None:
    heading = "#" * max(heading_level, 2)
    rank_label = aggregate.get("market_group_rank") or aggregate.get("child_rank") or "-"
    lines.extend(
        [
            f"{heading} {rank_label}. {aggregate['market_group_name']}",
            "",
            f"- 本组总流量: {_format_isk_human(aggregate['total_turnover_isk'])} ISK",
            f"- 本组直属物品流量: {_format_isk_human(aggregate['direct_turnover_isk'])} ISK",
            f"- 占全市场比例: {_format_percent(share_of_total)}",
            f"- 本组覆盖物品数: {aggregate['total_item_count']}",
            "",
        ]
    )

    child_groups = _attach_rankings(list(aggregate.get("child_groups") or []), key="child_rank")
    lines.append("子市场组排行")
    lines.append("")
    child_rows = [
        [
            str(child["child_rank"]),
            str(child["market_group_name"]),
            _format_isk_human(child["total_turnover_isk"]),
            _format_percent(_safe_div(float(child["total_turnover_isk"]), float(aggregate["total_turnover_isk"]))),
            str(child["total_item_count"]),
        ]
        for child in child_groups
    ]
    lines.extend(_render_table(["排名", "子市场组", "总流量", "占本组比例", "物品数"], child_rows))
    lines.append("")

    lines.append("本组 Top 5 物品")
    lines.append("")
    top_item_rows = [
        [
            str(index),
            str(item["type_name"]),
            _format_isk_human(item["turnover_isk"]),
            _format_isk_human(item["average_price"]),
            f"{float(item['volume']):,.0f}",
        ]
        for index, item in enumerate(aggregate.get("top_items") or [], start=1)
    ]
    lines.extend(_render_table(["排名", "物品", "日流量", "均价", "成交量"], top_item_rows))
    lines.append("")

    for child in child_groups:
        _render_group_section(
            lines,
            child,
            share_of_total=_safe_div(float(child["total_turnover_isk"]), float(aggregate["total_turnover_isk"])) * share_of_total,
            heading_level=heading_level + 1,
        )


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _write_items_csv(path: Path, items: list[dict[str, Any]]) -> None:
    fieldnames = [
        "global_rank",
        "type_id",
        "type_name",
        "market_group_path",
        "turnover_isk",
        "average_price",
        "volume",
        "order_count",
        "market_date",
        "fetched_at",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in items:
            writer.writerow({field: row.get(field) for field in fieldnames})


def build_region_market_flow_report(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    region_id: int,
    market_date: str | None = None,
    top_items_per_group: int = 5,
) -> dict[str, Any]:
    resolved_market_date = market_date or _resolve_logical_market_date(connection, region_id=region_id)
    if not resolved_market_date:
        raise RuntimeError(f"region_id={region_id} 没有可用 history 数据。")

    groups = _load_market_groups(connection)
    items = _load_daily_item_flows(connection, region_id=region_id, market_date=str(resolved_market_date))
    if not items:
        raise RuntimeError(f"region_id={region_id} 在 {resolved_market_date} 没有可用 history 数据。")

    path_map = _build_group_path_map(groups)
    fetched_times = sorted({str(item["fetched_at"]) for item in items if item.get("fetched_at")})
    for global_rank, item in enumerate(items, start=1):
        market_group_id = int(item["market_group_id"])
        item["global_rank"] = global_rank
        item["market_group_path"] = " > ".join(path_map.get(market_group_id, ["未分类"]))
        if market_group_id not in groups:
            groups[market_group_id] = {
                "market_group_id": market_group_id,
                "parent_group_id": UNKNOWN_MARKET_GROUP_ID,
                "market_group_name": str(item.get("market_group_name") or "未分类"),
                "children": [],
                "direct_items": [],
            }
            groups[UNKNOWN_MARKET_GROUP_ID]["children"].append(market_group_id)
        groups[market_group_id]["direct_items"].append(item)

    aggregates = _aggregate_group_tree(groups, top_items_per_group=top_items_per_group)
    top_level_groups = [
        aggregates[group_id]
        for group_id, group in groups.items()
        if group.get("parent_group_id") is None
    ]
    top_level_groups.sort(
        key=lambda group: (-float(group["total_turnover_isk"]), str(group["market_group_name"]), int(group["market_group_id"]))
    )
    _attach_rankings(top_level_groups, key="market_group_rank")

    total_turnover_isk = sum(float(item["turnover_isk"]) for item in items)
    run_id = create_run_id("region-market-flow")
    run_dir = ensure_run_dir(output_root, run_id)
    markdown_path = run_dir / "region_market_flow_report.md"
    json_path = run_dir / "region_market_flow_report.json"
    items_csv_path = run_dir / "region_market_flow_items_full.csv"

    for aggregate in top_level_groups:
        aggregate["share_of_total"] = _safe_div(float(aggregate["total_turnover_isk"]), total_turnover_isk)

    report = {
        "report_generated_at_utc": _utc_now_iso(),
        "report_run_id": run_id,
        "report_run_dir": str(run_dir),
        "region_id": region_id,
        "region_name": _load_region_name(connection, region_id),
        "market_date": str(resolved_market_date),
        "top_items_per_group": top_items_per_group,
        "history_fetched_at_min": fetched_times[0] if fetched_times else None,
        "history_fetched_at_max": fetched_times[-1] if fetched_times else None,
        "total_turnover_isk": total_turnover_isk,
        "total_item_count": len(items),
        "top_level_group_count": len(top_level_groups),
        "top_level_groups": top_level_groups,
        "global_top_items": items[:100],
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "items_csv_path": str(items_csv_path),
        },
    }

    lines: list[str] = [
        f"# {report['region_name']} 日市场流量报告",
        "",
        f"- 星域: {report['region_name']} ({region_id})",
        f"- 统计交易日: {resolved_market_date}",
        f"- 总日流量: {_format_isk_human(total_turnover_isk)} ISK",
        f"- 覆盖物品数: {len(items)}",
        f"- History 最早抓取时间: {report['history_fetched_at_min'] or '-'}",
        f"- History 最晚抓取时间: {report['history_fetched_at_max'] or '-'}",
        f"- 每个市场组展示物品数: Top {top_items_per_group}",
        "",
        "## 一级市场组排行",
        "",
    ]
    top_level_rows = [
        [
            str(index),
            str(group["market_group_name"]),
            _format_isk_human(group["total_turnover_isk"]),
            _format_isk_human(group["direct_turnover_isk"]),
            _format_percent(group["share_of_total"]),
            str(group["total_item_count"]),
        ]
        for index, group in enumerate(top_level_groups, start=1)
    ]
    lines.extend(_render_table(["排名", "市场组", "总流量", "直属流量", "占全市场比例", "物品数"], top_level_rows))
    lines.extend(
        [
            "",
            "## 全市场 Top 100 物品",
            "",
        ]
    )
    global_item_rows = [
        [
            str(item["global_rank"]),
            str(item["type_name"]),
            str(item["market_group_path"]),
            _format_isk_human(item["turnover_isk"]),
            _format_isk_human(item["average_price"]),
            f"{float(item['volume']):,.0f}",
        ]
        for item in items[:100]
    ]
    lines.extend(_render_table(["排名", "物品", "市场组路径", "日流量", "均价", "成交量"], global_item_rows))
    lines.append("")
    lines.append("## 市场组分层明细")
    lines.append("")

    for group in top_level_groups:
        _render_group_section(
            lines,
            group,
            share_of_total=float(group["share_of_total"]),
            heading_level=3,
        )

    write_summary(markdown_path, lines)
    write_json(json_path, report)
    _write_items_csv(items_csv_path, items)
    return report
