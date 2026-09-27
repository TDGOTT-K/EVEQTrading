from __future__ import annotations

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .history_pair_relationship_report import (
    _align_series,
    _format_number,
    _load_region_name,
    _load_type_name,
    _ols_regression,
    _pearson,
    _render_table,
)
from .market_scope import describe_history_source, load_canonical_history_series, load_canonical_history_series_map
from .region_market_flow_report import UNKNOWN_MARKET_GROUP_ID, _build_group_path_map, _load_market_groups
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


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


def _format_history_source(payload: dict[str, Any]) -> str:
    segments = list(payload.get("segments") or [])
    if not segments:
        return "unknown"
    rendered: list[str] = []
    for segment in segments:
        window_parts: list[str] = []
        if segment.get("start_date_inclusive") is not None:
            window_parts.append(f">={segment['start_date_inclusive']}")
        if segment.get("end_date_exclusive") is not None:
            window_parts.append(f"<{segment['end_date_exclusive']}")
        window_label = " ".join(window_parts) if window_parts else "all"
        rendered.append(
            f"{int(segment['source_region_id'])}:{segment['market_scope']}:{window_label}"
        )
    return " + ".join(rendered)


def _load_target_rows(
    connection: sqlite3.Connection,
    *,
    anchor_type_id: int,
    selected_targets: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if selected_targets is not None:
        return [row for row in selected_targets if int(row["type_id"]) != int(anchor_type_id)]
    rows = connection.execute(
        """
        SELECT type_id, type_name, market_group, market_group_id
        FROM item_catalog
        WHERE is_published = 1
          AND market_group IS NOT NULL
          AND type_id != ?
        ORDER BY type_id
        """,
        (anchor_type_id,),
    ).fetchall()
    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "market_group": str(row["market_group"] or "未分类"),
            "market_group_id": int(row["market_group_id"]) if row["market_group_id"] is not None else None,
            "selection_mode": "all-published-market-types",
        }
        for row in rows
    ]


def _load_item_meta_map(connection: sqlite3.Connection, *, type_ids: set[int]) -> dict[int, dict[str, Any]]:
    if not type_ids:
        return {}
    placeholders = ",".join("?" for _ in sorted(type_ids))
    rows = connection.execute(
        f"""
        SELECT type_id, type_name, type_name_zh, type_name_en, market_group, market_group_id
        FROM item_catalog
        WHERE type_id IN ({placeholders})
        """,
        sorted(type_ids),
    ).fetchall()
    return {
        int(row["type_id"]): {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"] or row["type_name_zh"] or row["type_name_en"] or f"type:{int(row['type_id'])}"),
            "market_group": str(row["market_group"] or "未分类"),
            "market_group_id": int(row["market_group_id"]) if row["market_group_id"] is not None else None,
        }
        for row in rows
    }


def _return_pairs(aligned_rows: list[dict[str, Any]]) -> list[tuple[float, float]]:
    pairs: list[tuple[float, float]] = []
    for row in aligned_rows:
        left_return = row.get("left_return")
        right_return = row.get("right_return")
        if left_return is None or right_return is None:
            continue
        pairs.append((float(left_return), float(right_return)))
    return pairs


def _safe_turnover(row: dict[str, Any]) -> float:
    if row.get("turnover_isk") is not None:
        return float(row["turnover_isk"])
    return float(row["average_price"]) * float(row["volume"])


def _summarize_target_relationship(
    *,
    region_id: int,
    anchor_type_id: int,
    anchor_label: str,
    anchor_source: dict[str, Any],
    anchor_series: list[dict[str, Any]],
    target_meta: dict[str, Any],
    target_series: list[dict[str, Any]],
) -> dict[str, Any] | None:
    aligned = _align_series(anchor_series, target_series)
    if len(aligned) < 2:
        return None

    price_pairs = [(float(row["left_price"]), float(row["right_price"])) for row in aligned]
    volume_pairs = [(float(row["left_volume"]), float(row["right_volume"])) for row in aligned]
    turnover_pairs = [(float(row["left_turnover_isk"]), float(row["right_turnover_isk"])) for row in aligned]
    return_pairs = _return_pairs(aligned)
    return_regression = _ols_regression(
        [pair[0] for pair in return_pairs],
        [pair[1] for pair in return_pairs],
    ) if len(return_pairs) >= 2 else {"alpha": 0.0, "beta": 0.0, "r_squared": 0.0}

    return {
        "anchor_type_id": int(anchor_type_id),
        "anchor_label": str(anchor_label),
        "anchor_history_source": anchor_source,
        "target_type_id": int(target_meta["type_id"]),
        "target_label": str(target_meta["type_name"]),
        "target_market_group": str(target_meta["market_group"]),
        "target_market_group_id": target_meta.get("market_group_id"),
        "target_history_source": describe_history_source(region_id=region_id, type_id=int(target_meta["type_id"])),
        "overlap_day_count": len(aligned),
        "overlap_market_date_min": aligned[0]["market_date"],
        "overlap_market_date_max": aligned[-1]["market_date"],
        "anchor_history_day_count": len(anchor_series),
        "target_history_day_count": len(target_series),
        "anchor_latest_fetched_at": max(str(row["fetched_at"]) for row in anchor_series),
        "target_latest_fetched_at": max(str(row["fetched_at"]) for row in target_series),
        "latest_anchor_price": float(aligned[-1]["left_price"]),
        "latest_target_price": float(aligned[-1]["right_price"]),
        "latest_anchor_volume": float(aligned[-1]["left_volume"]),
        "latest_target_volume": float(aligned[-1]["right_volume"]),
        "latest_anchor_turnover_isk": float(aligned[-1]["left_turnover_isk"]),
        "latest_target_turnover_isk": float(aligned[-1]["right_turnover_isk"]),
        "price_pearson_corr": _pearson([pair[0] for pair in price_pairs], [pair[1] for pair in price_pairs]),
        "return_pearson_corr": _pearson([pair[0] for pair in return_pairs], [pair[1] for pair in return_pairs]) if return_pairs else 0.0,
        "volume_pearson_corr": _pearson([pair[0] for pair in volume_pairs], [pair[1] for pair in volume_pairs]),
        "turnover_pearson_corr": _pearson([pair[0] for pair in turnover_pairs], [pair[1] for pair in turnover_pairs]),
        "target_return_beta_on_anchor": float(return_regression["beta"]),
        "target_return_alpha_on_anchor": float(return_regression["alpha"]),
        "target_return_r_squared_on_anchor": float(return_regression["r_squared"]),
    }


def _ensure_market_group_node(
    groups: dict[int, dict[str, Any]],
    *,
    market_group_id: int,
    market_group_name: str,
) -> None:
    if market_group_id in groups:
        return
    groups[market_group_id] = {
        "market_group_id": int(market_group_id),
        "parent_group_id": UNKNOWN_MARKET_GROUP_ID,
        "market_group_name": str(market_group_name or "未分类"),
        "market_group_name_en": None,
        "market_group_name_zh": str(market_group_name or "未分类"),
        "children": [],
        "direct_items": [],
    }
    groups[UNKNOWN_MARKET_GROUP_ID]["children"].append(int(market_group_id))


def _build_group_daily_series_map(
    *,
    groups: dict[int, dict[str, Any]],
    target_meta_map: dict[int, dict[str, Any]],
    target_series_map: dict[int, list[dict[str, Any]]],
) -> tuple[dict[int, list[dict[str, Any]]], dict[int, dict[str, Any]]]:
    group_day_map: dict[int, dict[str, dict[str, Any]]] = {}
    group_meta_map: dict[int, dict[str, Any]] = {}

    for type_id, series in target_series_map.items():
        target_meta = target_meta_map.get(type_id)
        if target_meta is None:
            continue
        raw_group_id = target_meta.get("market_group_id")
        market_group_id = int(raw_group_id) if raw_group_id is not None else UNKNOWN_MARKET_GROUP_ID
        market_group_name = str(target_meta.get("market_group") or "未分类")
        _ensure_market_group_node(groups, market_group_id=market_group_id, market_group_name=market_group_name)
        group_meta_map.setdefault(
            market_group_id,
            {
                "market_group_id": market_group_id,
                "market_group_name": market_group_name,
                "target_item_count": 0,
            },
        )
        group_meta_map[market_group_id]["target_item_count"] = int(group_meta_map[market_group_id]["target_item_count"]) + 1

        for row in series:
            market_date = str(row["market_date"])
            day_bucket = group_day_map.setdefault(market_group_id, {}).setdefault(
                market_date,
                {
                    "market_date": market_date,
                    "turnover_isk": 0.0,
                    "active_item_count": 0,
                    "fetched_at": str(row["fetched_at"]),
                },
            )
            day_bucket["turnover_isk"] = float(day_bucket["turnover_isk"]) + _safe_turnover(row)
            day_bucket["active_item_count"] = int(day_bucket["active_item_count"]) + 1
            if str(row["fetched_at"]) > str(day_bucket["fetched_at"]):
                day_bucket["fetched_at"] = str(row["fetched_at"])

    path_map = _build_group_path_map(groups)
    group_series_map: dict[int, list[dict[str, Any]]] = {}
    for market_group_id, day_map in group_day_map.items():
        group_meta = group_meta_map.setdefault(
            market_group_id,
            {
                "market_group_id": market_group_id,
                "market_group_name": str(groups.get(market_group_id, {}).get("market_group_name") or "未分类"),
                "target_item_count": 0,
            },
        )
        group_meta["market_group_path"] = " > ".join(path_map.get(market_group_id, [str(group_meta["market_group_name"])]))
        rows = [day_map[market_date] for market_date in sorted(day_map)]
        group_series_map[market_group_id] = rows
    return group_series_map, group_meta_map


def _align_anchor_with_group_turnover(
    anchor_series: list[dict[str, Any]],
    group_series: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    anchor_by_date = {str(row["market_date"]): row for row in anchor_series}
    group_by_date = {str(row["market_date"]): row for row in group_series}
    shared_dates = sorted(set(anchor_by_date) & set(group_by_date))
    aligned: list[dict[str, Any]] = []
    previous_anchor_price: float | None = None
    previous_group_turnover: float | None = None
    for market_date in shared_dates:
        anchor_row = anchor_by_date[market_date]
        group_row = group_by_date[market_date]
        anchor_price = float(anchor_row["average_price"])
        group_turnover = float(group_row["turnover_isk"])
        anchor_return = None
        group_turnover_return = None
        if previous_anchor_price not in (None, 0.0):
            anchor_return = (anchor_price / previous_anchor_price) - 1.0
        if previous_group_turnover not in (None, 0.0):
            group_turnover_return = (group_turnover / previous_group_turnover) - 1.0
        aligned.append(
            {
                "market_date": market_date,
                "anchor_price": anchor_price,
                "anchor_volume": float(anchor_row["volume"]),
                "anchor_turnover_isk": _safe_turnover(anchor_row),
                "anchor_return": anchor_return,
                "group_turnover_isk": group_turnover,
                "group_active_item_count": int(group_row["active_item_count"]),
                "group_turnover_return": group_turnover_return,
                "anchor_fetched_at": str(anchor_row["fetched_at"]),
                "group_fetched_at": str(group_row["fetched_at"]),
            }
        )
        previous_anchor_price = anchor_price
        previous_group_turnover = group_turnover
    return aligned


def _summarize_group_relationship(
    *,
    anchor_type_id: int,
    anchor_label: str,
    anchor_source: dict[str, Any],
    anchor_series: list[dict[str, Any]],
    group_meta: dict[str, Any],
    group_series: list[dict[str, Any]],
) -> dict[str, Any] | None:
    aligned = _align_anchor_with_group_turnover(anchor_series, group_series)
    if len(aligned) < 2:
        return None

    price_to_turnover_pairs = [(float(row["anchor_price"]), float(row["group_turnover_isk"])) for row in aligned]
    volume_to_turnover_pairs = [(float(row["anchor_volume"]), float(row["group_turnover_isk"])) for row in aligned]
    anchor_turnover_pairs = [(float(row["anchor_turnover_isk"]), float(row["group_turnover_isk"])) for row in aligned]
    turnover_return_pairs = [
        (float(row["anchor_return"]), float(row["group_turnover_return"]))
        for row in aligned
        if row.get("anchor_return") is not None and row.get("group_turnover_return") is not None
    ]
    return_regression = _ols_regression(
        [pair[0] for pair in turnover_return_pairs],
        [pair[1] for pair in turnover_return_pairs],
    ) if len(turnover_return_pairs) >= 2 else {"alpha": 0.0, "beta": 0.0, "r_squared": 0.0}

    return {
        "anchor_type_id": int(anchor_type_id),
        "anchor_label": str(anchor_label),
        "anchor_history_source": anchor_source,
        "market_group_id": int(group_meta["market_group_id"]),
        "market_group_name": str(group_meta["market_group_name"]),
        "market_group_path": str(group_meta["market_group_path"]),
        "group_target_item_count": int(group_meta["target_item_count"]),
        "overlap_day_count": len(aligned),
        "overlap_market_date_min": aligned[0]["market_date"],
        "overlap_market_date_max": aligned[-1]["market_date"],
        "anchor_history_day_count": len(anchor_series),
        "group_history_day_count": len(group_series),
        "anchor_latest_fetched_at": max(str(row["anchor_fetched_at"]) for row in aligned),
        "group_latest_fetched_at": max(str(row["group_fetched_at"]) for row in aligned),
        "latest_anchor_price": float(aligned[-1]["anchor_price"]),
        "latest_anchor_volume": float(aligned[-1]["anchor_volume"]),
        "latest_anchor_turnover_isk": float(aligned[-1]["anchor_turnover_isk"]),
        "latest_group_turnover_isk": float(aligned[-1]["group_turnover_isk"]),
        "latest_group_active_item_count": int(aligned[-1]["group_active_item_count"]),
        "anchor_price_to_group_turnover_corr": _pearson(
            [pair[0] for pair in price_to_turnover_pairs],
            [pair[1] for pair in price_to_turnover_pairs],
        ),
        "anchor_return_to_group_turnover_return_corr": _pearson(
            [pair[0] for pair in turnover_return_pairs],
            [pair[1] for pair in turnover_return_pairs],
        ) if turnover_return_pairs else 0.0,
        "anchor_volume_to_group_turnover_corr": _pearson(
            [pair[0] for pair in volume_to_turnover_pairs],
            [pair[1] for pair in volume_to_turnover_pairs],
        ),
        "anchor_turnover_to_group_turnover_corr": _pearson(
            [pair[0] for pair in anchor_turnover_pairs],
            [pair[1] for pair in anchor_turnover_pairs],
        ),
        "group_turnover_return_beta_on_anchor_return": float(return_regression["beta"]),
        "group_turnover_return_alpha_on_anchor_return": float(return_regression["alpha"]),
        "group_turnover_return_r_squared_on_anchor_return": float(return_regression["r_squared"]),
    }


def _rank_item_rows(
    rows: list[dict[str, Any]],
    *,
    field_name: str,
    top_n: int,
    descending: bool,
) -> list[dict[str, Any]]:
    filtered = [row for row in rows if row.get(field_name) is not None]
    ranked = sorted(
        filtered,
        key=lambda row: (
            float(row[field_name]),
            int(row["overlap_day_count"]),
            str(row["target_label"]),
            int(row["target_type_id"]),
        ),
        reverse=descending,
    )
    return ranked[:top_n]


def _rank_group_rows(
    rows: list[dict[str, Any]],
    *,
    field_name: str,
    top_n: int,
    descending: bool,
) -> list[dict[str, Any]]:
    filtered = [row for row in rows if row.get(field_name) is not None]
    ranked = sorted(
        filtered,
        key=lambda row: (
            float(row[field_name]),
            int(row["overlap_day_count"]),
            float(row["latest_group_turnover_isk"]),
            str(row["market_group_path"]),
            int(row["market_group_id"]),
        ),
        reverse=descending,
    )
    return ranked[:top_n]


def _item_table_rows(rows: list[dict[str, Any]], *, metric_name: str) -> list[list[str]]:
    rendered: list[list[str]] = []
    for index, row in enumerate(rows, start=1):
        rendered.append(
            [
                str(index),
                str(row["target_label"]),
                str(row["target_market_group"]),
                str(int(row["overlap_day_count"])),
                _format_number(row[metric_name], 4),
                _format_number(row["return_pearson_corr"], 4),
                _format_number(row["volume_pearson_corr"], 4),
                _format_number(row["turnover_pearson_corr"], 4),
                str(row["overlap_market_date_max"]),
            ]
        )
    return rendered


def _group_table_rows(rows: list[dict[str, Any]], *, metric_name: str) -> list[list[str]]:
    rendered: list[list[str]] = []
    for index, row in enumerate(rows, start=1):
        rendered.append(
            [
                str(index),
                str(row["market_group_path"]),
                str(int(row["group_target_item_count"])),
                str(int(row["overlap_day_count"])),
                _format_number(row[metric_name], 4),
                _format_number(row["anchor_return_to_group_turnover_return_corr"], 4),
                _format_number(row["anchor_volume_to_group_turnover_corr"], 4),
                _format_number(row["anchor_turnover_to_group_turnover_corr"], 4),
                _format_isk_human(row["latest_group_turnover_isk"]),
                str(row["overlap_market_date_max"]),
            ]
        )
    return rendered


def _write_items_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "target_type_id",
        "target_label",
        "target_market_group",
        "overlap_day_count",
        "overlap_market_date_min",
        "overlap_market_date_max",
        "anchor_history_day_count",
        "target_history_day_count",
        "anchor_latest_fetched_at",
        "target_latest_fetched_at",
        "price_pearson_corr",
        "return_pearson_corr",
        "volume_pearson_corr",
        "turnover_pearson_corr",
        "target_return_beta_on_anchor",
        "target_return_alpha_on_anchor",
        "target_return_r_squared_on_anchor",
        "latest_anchor_price",
        "latest_target_price",
        "latest_anchor_volume",
        "latest_target_volume",
        "latest_anchor_turnover_isk",
        "latest_target_turnover_isk",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def _write_group_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "market_group_id",
        "market_group_name",
        "market_group_path",
        "group_target_item_count",
        "overlap_day_count",
        "overlap_market_date_min",
        "overlap_market_date_max",
        "anchor_history_day_count",
        "group_history_day_count",
        "anchor_latest_fetched_at",
        "group_latest_fetched_at",
        "anchor_price_to_group_turnover_corr",
        "anchor_return_to_group_turnover_return_corr",
        "anchor_volume_to_group_turnover_corr",
        "anchor_turnover_to_group_turnover_corr",
        "group_turnover_return_beta_on_anchor_return",
        "group_turnover_return_alpha_on_anchor_return",
        "group_turnover_return_r_squared_on_anchor_return",
        "latest_anchor_price",
        "latest_anchor_volume",
        "latest_anchor_turnover_isk",
        "latest_group_turnover_isk",
        "latest_group_active_item_count",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def build_history_anchor_relationship_report(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    region_id: int,
    anchor_type_id: int,
    anchor_label: str | None = None,
    selected_targets: list[dict[str, Any]] | None = None,
    min_overlap_days: int = 180,
    top_n: int = 20,
) -> dict[str, Any]:
    anchor_name = anchor_label or _load_type_name(connection, anchor_type_id)
    anchor_series = load_canonical_history_series(connection, region_id=region_id, type_id=anchor_type_id)
    if len(anchor_series) < 30:
        raise RuntimeError(f"锚点物品 {anchor_name} ({anchor_type_id}) history 不足 30 天。")

    target_rows = _load_target_rows(connection, anchor_type_id=anchor_type_id, selected_targets=selected_targets)
    target_ids = {int(row["type_id"]) for row in target_rows}
    target_meta_map = _load_item_meta_map(connection, type_ids=target_ids)
    target_series_map = load_canonical_history_series_map(connection, region_id=region_id, type_ids=target_ids)
    anchor_source = describe_history_source(region_id=region_id, type_id=anchor_type_id)

    relationship_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    for target_row in target_rows:
        target_type_id = int(target_row["type_id"])
        target_series = target_series_map.get(target_type_id, [])
        if not target_series:
            skipped_rows.append({"type_id": target_type_id, "type_name": target_row["type_name"], "reason": "missing_history"})
            continue
        relationship = _summarize_target_relationship(
            region_id=region_id,
            anchor_type_id=anchor_type_id,
            anchor_label=anchor_name,
            anchor_source=anchor_source,
            anchor_series=anchor_series,
            target_meta=target_meta_map.get(
                target_type_id,
                {
                    "type_id": target_type_id,
                    "type_name": target_row["type_name"],
                    "market_group": target_row.get("market_group") or "未分类",
                    "market_group_id": target_row.get("market_group_id"),
                },
            ),
            target_series=target_series,
        )
        if relationship is None:
            skipped_rows.append({"type_id": target_type_id, "type_name": target_row["type_name"], "reason": "overlap_lt_2"})
            continue
        if int(relationship["overlap_day_count"]) < int(min_overlap_days):
            skipped_rows.append({"type_id": target_type_id, "type_name": target_row["type_name"], "reason": "overlap_below_threshold"})
            continue
        relationship_rows.append(relationship)

    if not relationship_rows:
        raise RuntimeError("没有满足最小重叠天数要求的目标物品，无法生成锚点关系报告。")

    groups = _load_market_groups(connection)
    group_series_map, group_meta_map = _build_group_daily_series_map(
        groups=groups,
        target_meta_map=target_meta_map,
        target_series_map=target_series_map,
    )

    group_relationship_rows: list[dict[str, Any]] = []
    for market_group_id, group_series in group_series_map.items():
        group_relationship = _summarize_group_relationship(
            anchor_type_id=anchor_type_id,
            anchor_label=anchor_name,
            anchor_source=anchor_source,
            anchor_series=anchor_series,
            group_meta=group_meta_map[market_group_id],
            group_series=group_series,
        )
        if group_relationship is None:
            continue
        if int(group_relationship["overlap_day_count"]) < int(min_overlap_days):
            continue
        group_relationship_rows.append(group_relationship)

    run_id = create_run_id("history-anchor")
    run_dir = ensure_run_dir(output_root, run_id)
    markdown_path = run_dir / "history_anchor_relationship_report.md"
    json_path = run_dir / "history_anchor_relationship_report.json"
    csv_path = run_dir / "history_anchor_relationship_screen.csv"
    group_csv_path = run_dir / "history_anchor_relationship_groups.csv"

    rankings = {
        "price_positive": _rank_item_rows(relationship_rows, field_name="price_pearson_corr", top_n=top_n, descending=True),
        "price_negative": _rank_item_rows(relationship_rows, field_name="price_pearson_corr", top_n=top_n, descending=False),
        "return_positive": _rank_item_rows(relationship_rows, field_name="return_pearson_corr", top_n=top_n, descending=True),
        "return_negative": _rank_item_rows(relationship_rows, field_name="return_pearson_corr", top_n=top_n, descending=False),
        "volume_positive": _rank_item_rows(relationship_rows, field_name="volume_pearson_corr", top_n=top_n, descending=True),
        "volume_negative": _rank_item_rows(relationship_rows, field_name="volume_pearson_corr", top_n=top_n, descending=False),
        "turnover_positive": _rank_item_rows(relationship_rows, field_name="turnover_pearson_corr", top_n=top_n, descending=True),
        "turnover_negative": _rank_item_rows(relationship_rows, field_name="turnover_pearson_corr", top_n=top_n, descending=False),
    }
    group_rankings = {
        "price_to_turnover_positive": _rank_group_rows(group_relationship_rows, field_name="anchor_price_to_group_turnover_corr", top_n=top_n, descending=True),
        "price_to_turnover_negative": _rank_group_rows(group_relationship_rows, field_name="anchor_price_to_group_turnover_corr", top_n=top_n, descending=False),
        "return_to_turnover_positive": _rank_group_rows(group_relationship_rows, field_name="anchor_return_to_group_turnover_return_corr", top_n=top_n, descending=True),
        "return_to_turnover_negative": _rank_group_rows(group_relationship_rows, field_name="anchor_return_to_group_turnover_return_corr", top_n=top_n, descending=False),
        "volume_to_turnover_positive": _rank_group_rows(group_relationship_rows, field_name="anchor_volume_to_group_turnover_corr", top_n=top_n, descending=True),
        "volume_to_turnover_negative": _rank_group_rows(group_relationship_rows, field_name="anchor_volume_to_group_turnover_corr", top_n=top_n, descending=False),
        "turnover_to_turnover_positive": _rank_group_rows(group_relationship_rows, field_name="anchor_turnover_to_group_turnover_corr", top_n=top_n, descending=True),
        "turnover_to_turnover_negative": _rank_group_rows(group_relationship_rows, field_name="anchor_turnover_to_group_turnover_corr", top_n=top_n, descending=False),
    }

    report = {
        "report_generated_at_utc": _utc_now_iso(),
        "report_run_id": run_id,
        "report_run_dir": str(run_dir),
        "region_id": int(region_id),
        "region_name": _load_region_name(connection, region_id),
        "anchor_type_id": int(anchor_type_id),
        "anchor_label": str(anchor_name),
        "anchor_history_source": anchor_source,
        "selection_mode": (selected_targets[0]["selection_mode"] if selected_targets else "all-published-market-types"),
        "universe_summary": {
            "selected_target_count": len(target_rows),
            "qualified_target_count": len(relationship_rows),
            "qualified_market_group_count": len(group_relationship_rows),
            "skipped_target_count": len(skipped_rows),
            "min_overlap_days": int(min_overlap_days),
            "top_n": int(top_n),
            "anchor_history_day_count": len(anchor_series),
            "anchor_market_date_min": anchor_series[0]["market_date"],
            "anchor_market_date_max": anchor_series[-1]["market_date"],
            "anchor_latest_fetched_at": max(str(row["fetched_at"]) for row in anchor_series),
        },
        "rankings": rankings,
        "group_rankings": group_rankings,
        "relationship_rows": relationship_rows,
        "group_relationship_rows": group_relationship_rows,
        "skipped_rows_sample": skipped_rows[:50],
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
            "group_csv_path": str(group_csv_path),
        },
    }

    lines: list[str] = [
        f"# {anchor_name} 锚点市场关系扫描报告",
        "",
        f"- 星域: {report['region_name']} ({region_id})",
        f"- 锚点物品: {anchor_name} ({anchor_type_id})",
        f"- 锚点 history 口径: {_format_history_source(anchor_source)}",
        f"- 锚点样本区间: {report['universe_summary']['anchor_market_date_min']} 到 {report['universe_summary']['anchor_market_date_max']}",
        f"- 锚点 history 天数: {report['universe_summary']['anchor_history_day_count']}",
        f"- 锚点最新抓取: {report['universe_summary']['anchor_latest_fetched_at']}",
        f"- 扫描目标数: {report['universe_summary']['selected_target_count']}",
        f"- 合格物品数: {report['universe_summary']['qualified_target_count']}",
        f"- 合格市场组数: {report['universe_summary']['qualified_market_group_count']}",
        f"- 跳过目标数: {report['universe_summary']['skipped_target_count']}",
        f"- 最小重叠天数: {report['universe_summary']['min_overlap_days']}",
        "",
        "## 说明",
        "",
        "- `物品价格相关`：锚点均价与目标均价的 Pearson 相关。",
        "- `物品收益率相关`：锚点日收益率与目标日收益率的 Pearson 相关。",
        "- `物品成交量相关`：锚点日成交量与目标日成交量的 Pearson 相关。",
        "- `物品日成交额相关`：锚点日成交额与目标日成交额的 Pearson 相关。",
        "- `市场组日成交额相关`：把同一直接市场组下所有物品的日成交额相加，再看它和锚点的关系。",
        "- 这里只是关系扫描，不等于因果结论。",
        "",
        "## 单物品关系榜单",
        "",
    ]

    item_section_specs = [
        ("价格正相关 Top", "price_positive", "price_pearson_corr"),
        ("价格负相关 Top", "price_negative", "price_pearson_corr"),
        ("收益率正相关 Top", "return_positive", "return_pearson_corr"),
        ("收益率负相关 Top", "return_negative", "return_pearson_corr"),
        ("成交量正相关 Top", "volume_positive", "volume_pearson_corr"),
        ("成交量负相关 Top", "volume_negative", "volume_pearson_corr"),
        ("日成交额正相关 Top", "turnover_positive", "turnover_pearson_corr"),
        ("日成交额负相关 Top", "turnover_negative", "turnover_pearson_corr"),
    ]
    for title, key, metric_name in item_section_specs:
        lines.extend([f"## {title}", ""])
        lines.extend(
            _render_table(
                ["排名", "商品", "市场组", "重叠天数", "主指标", "收益率相关", "成交量相关", "日成交额相关", "样本截止"],
                _item_table_rows(rankings[key], metric_name=metric_name),
            )
        )
        lines.append("")

    lines.extend(
        [
            "## 市场组关系榜单",
            "",
            "- 市场组口径是 `直接 market_group_id` 聚合，不把父级市场组再次重复并入榜单。",
            "",
        ]
    )
    group_section_specs = [
        ("锚点价格 vs 市场组日成交额 正相关 Top", "price_to_turnover_positive", "anchor_price_to_group_turnover_corr"),
        ("锚点价格 vs 市场组日成交额 负相关 Top", "price_to_turnover_negative", "anchor_price_to_group_turnover_corr"),
        ("锚点收益率 vs 市场组日成交额变化 正相关 Top", "return_to_turnover_positive", "anchor_return_to_group_turnover_return_corr"),
        ("锚点收益率 vs 市场组日成交额变化 负相关 Top", "return_to_turnover_negative", "anchor_return_to_group_turnover_return_corr"),
        ("锚点成交量 vs 市场组日成交额 正相关 Top", "volume_to_turnover_positive", "anchor_volume_to_group_turnover_corr"),
        ("锚点成交量 vs 市场组日成交额 负相关 Top", "volume_to_turnover_negative", "anchor_volume_to_group_turnover_corr"),
        ("锚点日成交额 vs 市场组日成交额 正相关 Top", "turnover_to_turnover_positive", "anchor_turnover_to_group_turnover_corr"),
        ("锚点日成交额 vs 市场组日成交额 负相关 Top", "turnover_to_turnover_negative", "anchor_turnover_to_group_turnover_corr"),
    ]
    for title, key, metric_name in group_section_specs:
        lines.extend([f"## {title}", ""])
        lines.extend(
            _render_table(
                ["排名", "市场组路径", "组内物品数", "重叠天数", "主指标", "收益率相关", "成交量相关", "日成交额相关", "最新组日成交额", "样本截止"],
                _group_table_rows(group_rankings[key], metric_name=metric_name),
            )
        )
        lines.append("")

    write_json(json_path, report)
    _write_items_csv(csv_path, relationship_rows)
    _write_group_csv(group_csv_path, group_relationship_rows)
    write_summary(markdown_path, lines)
    return report
