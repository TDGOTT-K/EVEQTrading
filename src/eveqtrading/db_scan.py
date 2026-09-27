from __future__ import annotations

import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .catalog_select import resolve_type_targets
from .market_scope import load_canonical_history_series_map
from .models import FeeConfig, ScanConfig

RESERVOIR_TRIGGER_ZSCORE = -1.0
RESERVOIR_TARGET_DISCOUNT_RATE = 0.01
RESERVOIR_RECENT_MEAN_FLOOR_RATIO = 0.75
RESERVOIR_ENTRY_TO_RECENT_MEAN_FLOOR_RATIO = 0.70
RESERVOIR_RECENT_HIGH_TARGET_CAP_RATIO = 1.15
RESERVOIR_EXIT_REBOUND_PENALTY_POWER = 2.0
JITA_PASSIVE_POSITION_FRACTION = 1.0 / 12.0


@dataclass
class DbScanCandidateBuild:
    payload: dict[str, Any]
    feature_rows: list[dict[str, Any]]
    skipped_rows: list[dict[str, Any]]


def resolve_db_scan_targets(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool,
    all_published_market_types: bool,
    limit: int | None,
) -> list[dict[str, Any]] | None:
    if explicit_type_ids or type_id_file is not None or ready_market_scan_types or all_published_market_types:
        return resolve_type_targets(
            connection,
            region_id=region_id,
            explicit_type_ids=explicit_type_ids,
            type_id_file=type_id_file,
            ready_market_scan_types=ready_market_scan_types,
            all_published_market_types=all_published_market_types,
            limit=limit,
        )
    return None


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _percentile_rank(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    count = sum(1 for item in values if item <= value)
    return count / len(values)


def _load_successful_order_snapshot_refs(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_ids: set[int] | None,
) -> dict[int, str]:
    if type_ids is not None and not type_ids:
        return {}
    sql = (
        "SELECT type_id, coverage_marker FROM sync_scope_state "
        "WHERE scope_type = 'market_orders' AND region_id = ? AND coverage_marker LIKE 'ok:%'"
    )
    params: list[Any] = [region_id]
    if type_ids is not None:
        placeholders = ",".join("?" for _ in type_ids)
        sql += f" AND type_id IN ({placeholders})"
        params.extend(sorted(type_ids))
    rows = connection.execute(sql, params).fetchall()
    snapshot_refs: dict[int, str] = {}
    for row in rows:
        coverage_marker = str(row["coverage_marker"])
        snapshot_id = coverage_marker[3:] if coverage_marker.startswith("ok:") else ""
        if snapshot_id:
            snapshot_refs[int(row["type_id"])] = snapshot_id
    return snapshot_refs


def _load_order_books(
    connection: sqlite3.Connection,
    *,
    snapshot_refs: dict[int, str],
) -> dict[int, dict[str, Any]]:
    if not snapshot_refs:
        return {}
    snapshot_ids = list(snapshot_refs.values())
    placeholders = ",".join("?" for _ in snapshot_ids)
    sql = (
        "SELECT h.snapshot_id, h.captured_at, l.order_id, l.type_id, l.location_id, l.is_buy_order, "
        "l.price, l.volume_remain, l.issued_at, l.duration_days "
        "FROM market_order_snapshot_head h "
        "JOIN market_order_snapshot_line l ON l.snapshot_id = h.snapshot_id "
        f"WHERE h.snapshot_id IN ({placeholders}) "
        "ORDER BY l.type_id, h.captured_at DESC, l.location_id, l.is_buy_order"
    )
    rows = connection.execute(sql, snapshot_ids).fetchall()

    per_type_lines: dict[int, list[sqlite3.Row]] = defaultdict(list)
    per_type_meta: dict[int, dict[str, Any]] = {}
    for row in rows:
        type_id = int(row["type_id"])
        per_type_lines[type_id].append(row)
        if type_id not in per_type_meta:
            per_type_meta[type_id] = {
                "snapshot_id": row["snapshot_id"],
                "captured_at": row["captured_at"],
            }

    order_books: dict[int, dict[str, Any]] = {}
    for type_id, line_rows in per_type_lines.items():
        global_best_bid: float | None = None
        global_best_bid_location_id: int | None = None
        global_best_ask: float | None = None
        global_best_ask_location_id: int | None = None
        per_location: dict[int, dict[str, Any]] = defaultdict(
            lambda: {
                "best_bid": None,
                "best_ask": None,
                "buy_orders": [],
                "sell_orders": [],
            }
        )

        for row in line_rows:
            location_id = int(row["location_id"])
            price = float(row["price"])
            order_entry = {
                "order_id": int(row["order_id"]),
                "price": price,
                "volume_remain": float(row["volume_remain"]),
                "issued_at": row["issued_at"],
                "duration_days": int(row["duration_days"]) if row["duration_days"] is not None else None,
            }
            if int(row["is_buy_order"]) == 1:
                if global_best_bid is None or price > global_best_bid:
                    global_best_bid = price
                    global_best_bid_location_id = location_id
                current = per_location[location_id]["best_bid"]
                if current is None or price > float(current):
                    per_location[location_id]["best_bid"] = price
                per_location[location_id]["buy_orders"].append(order_entry)
            else:
                if global_best_ask is None or price < global_best_ask:
                    global_best_ask = price
                    global_best_ask_location_id = location_id
                current = per_location[location_id]["best_ask"]
                if current is None or price < float(current):
                    per_location[location_id]["best_ask"] = price
                per_location[location_id]["sell_orders"].append(order_entry)

        same_location_pairs: list[dict[str, Any]] = []
        for location_id, location_book in per_location.items():
            location_book["buy_orders"].sort(key=lambda item: (-float(item["price"]), int(item["order_id"])))
            location_book["sell_orders"].sort(key=lambda item: (float(item["price"]), int(item["order_id"])))
            if location_book["best_bid"] is None or location_book["best_ask"] is None:
                continue
            same_location_pairs.append(
                {
                    "location_id": location_id,
                    "best_bid": float(location_book["best_bid"]),
                    "best_ask": float(location_book["best_ask"]),
                }
            )

        order_books[type_id] = {
            **per_type_meta[type_id],
            "global_best_bid": global_best_bid,
            "global_best_bid_location_id": global_best_bid_location_id,
            "global_best_ask": global_best_ask,
            "global_best_ask_location_id": global_best_ask_location_id,
            "same_location_pairs": same_location_pairs,
            "location_books": dict(per_location),
            "order_count": len(line_rows),
        }
    return order_books


def _load_history_series(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_ids: set[int] | None,
) -> dict[int, list[dict[str, Any]]]:
    return load_canonical_history_series_map(
        connection,
        region_id=region_id,
        type_ids=type_ids,
    )


def _load_location_names(
    connection: sqlite3.Connection,
    *,
    location_ids: set[int],
) -> dict[int, str]:
    if not location_ids:
        return {}
    placeholders = ",".join("?" for _ in location_ids)
    rows = connection.execute(
        f"SELECT location_id, location_name FROM market_location_catalog WHERE location_id IN ({placeholders})",
        sorted(location_ids),
    ).fetchall()
    return {int(row["location_id"]): str(row["location_name"]) for row in rows}


def _is_jita_location_name(location_name: str | None) -> bool:
    if not location_name:
        return False
    return "jita" in location_name.lower()


def _compute_same_location_choice(
    pairs: list[dict[str, Any]],
    fee_config: FeeConfig,
) -> dict[str, Any] | None:
    best_choice: dict[str, Any] | None = None
    for pair in pairs:
        buy_cost = float(pair["best_ask"]) * (1.0 + fee_config.buy_broker_fee_rate)
        sell_net = float(pair["best_bid"]) * (
            1.0 - fee_config.sell_broker_fee_rate - fee_config.sales_tax_rate - fee_config.relist_fee_rate
        )
        net_margin = _safe_div(sell_net - buy_cost, buy_cost)
        candidate = {
            **pair,
            "net_margin": net_margin,
        }
        if best_choice is None or candidate["net_margin"] > best_choice["net_margin"]:
            best_choice = candidate
    return best_choice


def _estimated_exit_days_bid(test_position_isk: float, avg_daily_isk_volume_30d: float) -> float:
    return _safe_div(test_position_isk, avg_daily_isk_volume_30d)


def _estimated_exit_days_ask(
    *,
    test_position_isk: float,
    buy_reference_price: float,
    avg_daily_units_7d: float,
) -> float:
    test_position_units = _safe_div(test_position_isk, buy_reference_price)
    return _safe_div(test_position_units, avg_daily_units_7d)


def _reservoir_target_sell_price(history_mean_30d: float) -> float:
    return history_mean_30d * (1.0 - RESERVOIR_TARGET_DISCOUNT_RATE)


def _reservoir_capped_target_sell_price(
    *,
    history_mean_30d: float,
    history_high_max_7d: float,
) -> tuple[float, float, float]:
    base_target_price = _reservoir_target_sell_price(history_mean_30d)
    recent_high_cap_price = history_high_max_7d * RESERVOIR_RECENT_HIGH_TARGET_CAP_RATIO
    target_price = min(base_target_price, recent_high_cap_price) if recent_high_cap_price > 0 else base_target_price
    return target_price, base_target_price, recent_high_cap_price


def _reservoir_exit_penalty_multiplier(
    *,
    target_sell_price: float,
    buy_reference_price: float,
) -> float:
    rebound_ratio = max(1.0, _safe_div(target_sell_price, buy_reference_price))
    return rebound_ratio**RESERVOIR_EXIT_REBOUND_PENALTY_POWER


def build_db_scan_candidates(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    fee_config: FeeConfig,
    test_position_isk: float,
    selected_targets: list[dict[str, Any]] | None,
) -> DbScanCandidateBuild:
    selected_type_ids = {int(row["type_id"]) for row in selected_targets} if selected_targets else None
    snapshot_refs = _load_successful_order_snapshot_refs(connection, region_id=region_id, type_ids=selected_type_ids)
    effective_type_ids = set(snapshot_refs)
    if selected_type_ids is not None:
        effective_type_ids &= selected_type_ids
    history_series = _load_history_series(connection, region_id=region_id, type_ids=effective_type_ids or None)

    if selected_targets is None:
        discovered = sorted(effective_type_ids & set(history_series))
        type_name_rows = connection.execute(
            f"SELECT type_id, type_name FROM item_catalog WHERE type_id IN ({','.join('?' for _ in discovered)}) ORDER BY type_id",
            discovered,
        ).fetchall() if discovered else []
        selected_targets = [{"type_id": int(r["type_id"]), "type_name": r["type_name"], "selection_mode": "available-data"} for r in type_name_rows]

    selected_by_type = {int(row["type_id"]): row for row in selected_targets}
    order_books = _load_order_books(connection, snapshot_refs=snapshot_refs)
    location_ids_to_load: set[int] = set()
    for order_book in order_books.values():
        if order_book.get("global_best_bid_location_id") is not None:
            location_ids_to_load.add(int(order_book["global_best_bid_location_id"]))
        if order_book.get("global_best_ask_location_id") is not None:
            location_ids_to_load.add(int(order_book["global_best_ask_location_id"]))
        for pair in order_book.get("same_location_pairs", []):
            location_ids_to_load.add(int(pair["location_id"]))
    location_names = _load_location_names(connection, location_ids=location_ids_to_load)
    feature_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    latest_as_of_time: str | None = None

    for type_id in sorted(selected_by_type):
        type_name = str(selected_by_type[type_id]["type_name"])
        history = history_series.get(type_id)
        order_book = order_books.get(type_id)
        if not history:
            skipped_rows.append({"type_id": type_id, "type_name": type_name, "reason": "missing_history"})
            continue
        if not order_book:
            skipped_rows.append({"type_id": type_id, "type_name": type_name, "reason": "missing_order_snapshot"})
            continue
        if len(history) < 30:
            skipped_rows.append({"type_id": type_id, "type_name": type_name, "reason": "history_lt_30_days"})
            continue

        same_location_choice = _compute_same_location_choice(order_book["same_location_pairs"], fee_config)
        if same_location_choice is None:
            skipped_rows.append({"type_id": type_id, "type_name": type_name, "reason": "no_same_location_book"})
            continue

        latest_30 = history[-30:]
        latest_7 = history[-7:]
        history_latest_market_date = latest_30[-1]["market_date"]
        history_latest_fetched_at = max(str(row.get("fetched_at") or "") for row in latest_30)
        history_prices_30d = [row["average_price"] for row in latest_30]
        history_prices_7d = [row["average_price"] for row in latest_7]
        history_mean_30d = statistics.fmean(history_prices_30d)
        history_mean_7d = statistics.fmean(history_prices_7d)
        history_std_30d = statistics.pstdev(history_prices_30d) if len(history_prices_30d) > 1 else 0.0
        history_high_max_7d = max(float(row["highest_price"]) for row in latest_7)
        avg_daily_isk_volume_30d = statistics.fmean(row["average_price"] * row["volume"] for row in latest_30)
        active_days_ratio_30d = _safe_div(sum(1 for row in latest_30 if row["volume"] > 0), 30.0)
        avg_daily_units_7d = statistics.fmean(row["volume"] for row in latest_7)
        mid_price = (same_location_choice["best_bid"] + same_location_choice["best_ask"]) / 2.0
        price_zscore_30d = _safe_div(mid_price - history_mean_30d, history_std_30d)
        price_percentile_30d = _percentile_rank(history_prices_30d, mid_price)
        volatility_30d = _safe_div(history_std_30d, history_mean_30d)
        exit_days_bid = _estimated_exit_days_bid(test_position_isk, avg_daily_isk_volume_30d)
        exit_days_ask = _estimated_exit_days_ask(
            test_position_isk=test_position_isk,
            buy_reference_price=float(same_location_choice["best_ask"]),
            avg_daily_units_7d=avg_daily_units_7d,
        )

        cross_location_net_spread_pct = 0.0
        cross_buy_location_id = order_book["global_best_ask_location_id"]
        cross_sell_location_id = order_book["global_best_bid_location_id"]
        if (
            order_book["global_best_bid"] is not None
            and order_book["global_best_ask"] is not None
            and cross_sell_location_id is not None
            and cross_buy_location_id is not None
            and cross_sell_location_id != cross_buy_location_id
        ):
            cross_buy_cost = float(order_book["global_best_ask"]) * (1.0 + fee_config.buy_broker_fee_rate)
            cross_sell_net = float(order_book["global_best_bid"]) * (
                1.0 - fee_config.sell_broker_fee_rate - fee_config.sales_tax_rate - fee_config.relist_fee_rate
            )
            cross_location_net_spread_pct = _safe_div(cross_sell_net - cross_buy_cost, cross_buy_cost)

        as_of_time = str(order_book["captured_at"])
        if latest_as_of_time is None or as_of_time > latest_as_of_time:
            latest_as_of_time = as_of_time

        feature_row = {
            "type_id": type_id,
            "type_name": type_name,
            "region_id": region_id,
            "location_id": int(same_location_choice["location_id"]),
            "as_of_time": as_of_time,
            "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
            "active_days_ratio_30d": active_days_ratio_30d,
            "avg_daily_units_7d": avg_daily_units_7d,
            "best_bid": float(same_location_choice["best_bid"]),
            "best_ask": float(same_location_choice["best_ask"]),
            "gross_spread_pct": _safe_div(
                float(same_location_choice["best_ask"]) - float(same_location_choice["best_bid"]),
                float(same_location_choice["best_bid"]),
            ),
            "net_spread_pct": float(same_location_choice["net_margin"]),
            "cross_location_net_spread_pct": cross_location_net_spread_pct,
            "price_zscore_30d": price_zscore_30d,
            "price_percentile_30d": price_percentile_30d,
            "volatility_30d": volatility_30d,
            "estimated_exit_days_small_size": exit_days_bid,
            "estimated_exit_days_small_size_bid": exit_days_bid,
            "estimated_exit_days_small_size_ask": exit_days_ask,
            "history_mean_30d": history_mean_30d,
            "history_mean_7d": history_mean_7d,
            "history_std_30d": history_std_30d,
            "history_high_max_7d": history_high_max_7d,
            "history_days_30d": len(latest_30),
            "history_latest_market_date": history_latest_market_date,
            "history_latest_fetched_at": history_latest_fetched_at,
            "window_start_30d": latest_30[0]["market_date"],
            "window_end_30d": history_latest_market_date,
            "window_start_7d": latest_7[0]["market_date"],
            "window_end_7d": latest_7[-1]["market_date"],
            "order_snapshot_id": order_book["snapshot_id"],
            "global_best_bid": order_book["global_best_bid"],
            "global_best_bid_location_id": order_book["global_best_bid_location_id"],
            "global_best_ask": order_book["global_best_ask"],
            "global_best_ask_location_id": order_book["global_best_ask_location_id"],
        }
        feature_rows.append(feature_row)
        candidates.append(
            {
                "opportunity_id": f"{type_id}-{region_id}-station-flip",
                "strategy_hint": "station-flip",
                "type_id": type_id,
                "type_name": type_name,
                "region_id": region_id,
                "buy_location_id": int(same_location_choice["location_id"]),
                "sell_location_id": int(same_location_choice["location_id"]),
                "buy_location_name": location_names.get(int(same_location_choice["location_id"])),
                "sell_location_name": location_names.get(int(same_location_choice["location_id"])),
                "best_bid": float(same_location_choice["best_bid"]),
                "best_ask": float(same_location_choice["best_ask"]),
                "buy_reference_price": float(same_location_choice["best_ask"]),
                "sell_reference_price": float(same_location_choice["best_bid"]),
                "gross_spread_pct": _safe_div(
                    float(same_location_choice["best_ask"]) - float(same_location_choice["best_bid"]),
                    float(same_location_choice["best_bid"]),
                ),
                "screened_profit_margin_override": float(same_location_choice["net_margin"]),
                "entry_mode_override": "buy_best_ask",
                "exit_mode_override": "sell_best_bid",
                "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
                "active_days_ratio_30d": active_days_ratio_30d,
                "avg_daily_units_7d": avg_daily_units_7d,
                "history_mean_30d": history_mean_30d,
                "history_mean_7d": history_mean_7d,
                "history_std_30d": history_std_30d,
                "history_high_max_7d": history_high_max_7d,
                "history_latest_market_date": history_latest_market_date,
                "history_latest_fetched_at": history_latest_fetched_at,
                "window_start_30d": latest_30[0]["market_date"],
                "window_end_30d": history_latest_market_date,
                "window_start_7d": latest_7[0]["market_date"],
                "window_end_7d": latest_7[-1]["market_date"],
                "volatility_30d": volatility_30d,
                "price_percentile_30d": price_percentile_30d,
                "cross_location_net_spread_pct": cross_location_net_spread_pct,
                "estimated_exit_days_override": exit_days_bid,
                "estimated_exit_days_small_size_bid": exit_days_bid,
                "estimated_exit_days_small_size_ask": exit_days_ask,
                "source_order_snapshot_id": order_book["snapshot_id"],
                "source_order_captured_at": as_of_time,
                "source_selection_mode": selected_by_type[type_id]["selection_mode"],
            }
        )

        same_location_name = location_names.get(int(same_location_choice["location_id"]))
        if _is_jita_location_name(same_location_name):
            passive_buy_price = float(same_location_choice["best_bid"])
            passive_sell_price = float(same_location_choice["best_ask"])
            passive_buy_cost = passive_buy_price * (1.0 + fee_config.buy_broker_fee_rate)
            passive_sell_net = passive_sell_price * (
                1.0 - fee_config.sell_broker_fee_rate - fee_config.sales_tax_rate - fee_config.relist_fee_rate
            )
            passive_margin = _safe_div(passive_sell_net - passive_buy_cost, passive_buy_cost)
            passive_max_position_isk = avg_daily_isk_volume_30d * JITA_PASSIVE_POSITION_FRACTION
            passive_test_position_isk = min(test_position_isk, passive_max_position_isk)
            passive_exit_days = _estimated_exit_days_ask(
                test_position_isk=passive_test_position_isk,
                buy_reference_price=passive_buy_price,
                avg_daily_units_7d=avg_daily_units_7d,
            )
            candidates.append(
                {
                    "opportunity_id": f"{type_id}-{region_id}-jita-passive-flip",
                    "strategy_hint": "jita-passive-flip",
                    "type_id": type_id,
                    "type_name": type_name,
                    "region_id": region_id,
                    "buy_location_id": int(same_location_choice["location_id"]),
                    "sell_location_id": int(same_location_choice["location_id"]),
                    "buy_location_name": same_location_name,
                    "sell_location_name": same_location_name,
                    "best_bid": passive_buy_price,
                    "best_ask": passive_sell_price,
                    "buy_reference_price": passive_buy_price,
                    "sell_reference_price": passive_sell_price,
                    "gross_spread_pct": _safe_div(passive_sell_price - passive_buy_price, passive_buy_price),
                    "screened_profit_margin_override": passive_margin,
                    "entry_mode_override": "place_buy_order_best_bid",
                    "exit_mode_override": "place_sell_order_best_ask",
                    "capital_required_override": passive_test_position_isk,
                    "recommended_test_position_isk": passive_test_position_isk,
                    "max_position_isk": passive_max_position_isk,
                    "inventory_limit_fraction": JITA_PASSIVE_POSITION_FRACTION,
                    "inventory_limit_basis": "avg_daily_isk_volume_30d_div_12",
                    "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
                    "active_days_ratio_30d": active_days_ratio_30d,
                    "avg_daily_units_7d": avg_daily_units_7d,
                    "history_mean_30d": history_mean_30d,
                    "history_mean_7d": history_mean_7d,
                    "history_std_30d": history_std_30d,
                    "history_high_max_7d": history_high_max_7d,
                    "history_latest_market_date": history_latest_market_date,
                    "history_latest_fetched_at": history_latest_fetched_at,
                    "window_start_30d": latest_30[0]["market_date"],
                    "window_end_30d": history_latest_market_date,
                    "window_start_7d": latest_7[0]["market_date"],
                    "window_end_7d": latest_7[-1]["market_date"],
                    "volatility_30d": volatility_30d,
                    "price_percentile_30d": price_percentile_30d,
                    "cross_location_net_spread_pct": cross_location_net_spread_pct,
                    "estimated_exit_days_override": passive_exit_days,
                    "estimated_exit_days_small_size_bid": exit_days_bid,
                    "estimated_exit_days_small_size_ask": exit_days_ask,
                    "source_order_snapshot_id": order_book["snapshot_id"],
                    "source_order_captured_at": as_of_time,
                    "source_selection_mode": selected_by_type[type_id]["selection_mode"],
                }
            )

        if (
            order_book["global_best_bid"] is not None
            and order_book["global_best_ask"] is not None
            and cross_sell_location_id is not None
            and cross_buy_location_id is not None
            and cross_sell_location_id != cross_buy_location_id
        ):
            cross_buy_price = float(order_book["global_best_ask"])
            cross_sell_price = float(order_book["global_best_bid"])
            candidates.append(
                {
                    "opportunity_id": f"{type_id}-{region_id}-cross-location",
                    "strategy_hint": "cross-location",
                    "type_id": type_id,
                    "type_name": type_name,
                    "region_id": region_id,
                    "buy_location_id": int(cross_buy_location_id),
                    "sell_location_id": int(cross_sell_location_id),
                    "buy_location_name": location_names.get(int(cross_buy_location_id)),
                    "sell_location_name": location_names.get(int(cross_sell_location_id)),
                    "best_bid": cross_sell_price,
                    "best_ask": cross_buy_price,
                    "buy_reference_price": cross_buy_price,
                    "sell_reference_price": cross_sell_price,
                    "gross_spread_pct": _safe_div(cross_sell_price - cross_buy_price, cross_buy_price),
                    "screened_profit_margin_override": cross_location_net_spread_pct,
                    "entry_mode_override": "buy_remote_best_ask",
                    "exit_mode_override": "sell_remote_best_bid",
                    "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
                    "active_days_ratio_30d": active_days_ratio_30d,
                    "avg_daily_units_7d": avg_daily_units_7d,
                    "history_mean_30d": history_mean_30d,
                    "history_mean_7d": history_mean_7d,
                    "history_std_30d": history_std_30d,
                    "history_high_max_7d": history_high_max_7d,
                    "history_latest_market_date": history_latest_market_date,
                    "history_latest_fetched_at": history_latest_fetched_at,
                    "window_start_30d": latest_30[0]["market_date"],
                    "window_end_30d": history_latest_market_date,
                    "window_start_7d": latest_7[0]["market_date"],
                    "window_end_7d": latest_7[-1]["market_date"],
                    "volatility_30d": volatility_30d,
                    "price_percentile_30d": price_percentile_30d,
                    "cross_location_net_spread_pct": cross_location_net_spread_pct,
                    "estimated_exit_days_override": exit_days_bid,
                    "estimated_exit_days_small_size_bid": exit_days_bid,
                    "estimated_exit_days_small_size_ask": exit_days_ask,
                    "source_order_snapshot_id": order_book["snapshot_id"],
                    "source_order_captured_at": as_of_time,
                    "source_selection_mode": selected_by_type[type_id]["selection_mode"],
                }
            )

        if price_zscore_30d <= RESERVOIR_TRIGGER_ZSCORE:
            reservoir_target_price, reservoir_base_target_price, reservoir_recent_high_cap_price = _reservoir_capped_target_sell_price(
                history_mean_30d=history_mean_30d,
                history_high_max_7d=history_high_max_7d,
            )
            buy_reference_price = float(same_location_choice["best_ask"])
            buy_cost = buy_reference_price * (1.0 + fee_config.buy_broker_fee_rate)
            reservoir_sell_net = reservoir_target_price * (
                1.0 - fee_config.sell_broker_fee_rate - fee_config.sales_tax_rate - fee_config.relist_fee_rate
            )
            reservoir_margin = _safe_div(reservoir_sell_net - buy_cost, buy_cost)
            reservoir_recent_mean_ratio = _safe_div(history_mean_7d, history_mean_30d)
            reservoir_exit_penalty_multiplier = _reservoir_exit_penalty_multiplier(
                target_sell_price=reservoir_target_price,
                buy_reference_price=buy_reference_price,
            )
            reservoir_max_position_isk = avg_daily_isk_volume_30d * JITA_PASSIVE_POSITION_FRACTION
            reservoir_test_position_isk = min(test_position_isk, reservoir_max_position_isk)
            reservoir_base_exit_days = _estimated_exit_days_ask(
                test_position_isk=reservoir_test_position_isk,
                buy_reference_price=buy_reference_price,
                avg_daily_units_7d=avg_daily_units_7d,
            )
            reservoir_exit_days = reservoir_base_exit_days * reservoir_exit_penalty_multiplier
            candidates.append(
                {
                    "opportunity_id": f"{type_id}-{region_id}-reservoir",
                    "strategy_hint": "reservoir",
                    "type_id": type_id,
                    "type_name": type_name,
                    "region_id": region_id,
                    "buy_location_id": int(same_location_choice["location_id"]),
                    "sell_location_id": int(same_location_choice["location_id"]),
                    "buy_location_name": location_names.get(int(same_location_choice["location_id"])),
                    "sell_location_name": location_names.get(int(same_location_choice["location_id"])),
                    "best_bid": float(same_location_choice["best_bid"]),
                    "best_ask": buy_reference_price,
                    "buy_reference_price": buy_reference_price,
                    "sell_reference_price": reservoir_target_price,
                    "gross_spread_pct": _safe_div(reservoir_target_price - buy_reference_price, buy_reference_price),
                    "screened_profit_margin_override": reservoir_margin,
                    "entry_mode_override": "buy_best_ask",
                    "exit_mode_override": "sell_mean_reversion_target",
                    "capital_required_override": reservoir_test_position_isk,
                    "recommended_test_position_isk": reservoir_test_position_isk,
                    "max_position_isk": reservoir_max_position_isk,
                    "inventory_limit_fraction": JITA_PASSIVE_POSITION_FRACTION,
                    "inventory_limit_basis": "avg_daily_isk_volume_30d_div_12",
                    "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
                    "active_days_ratio_30d": active_days_ratio_30d,
                    "avg_daily_units_7d": avg_daily_units_7d,
                    "history_mean_30d": history_mean_30d,
                    "history_mean_7d": history_mean_7d,
                    "history_std_30d": history_std_30d,
                    "history_high_max_7d": history_high_max_7d,
                    "history_latest_market_date": history_latest_market_date,
                    "history_latest_fetched_at": history_latest_fetched_at,
                    "window_start_30d": latest_30[0]["market_date"],
                    "window_end_30d": history_latest_market_date,
                    "window_start_7d": latest_7[0]["market_date"],
                    "window_end_7d": latest_7[-1]["market_date"],
                    "volatility_30d": volatility_30d,
                    "price_percentile_30d": price_percentile_30d,
                    "cross_location_net_spread_pct": cross_location_net_spread_pct,
                    "estimated_exit_days_override": reservoir_exit_days,
                    "estimated_exit_days_small_size_bid": exit_days_bid,
                    "estimated_exit_days_small_size_ask": exit_days_ask,
                    "reservoir_target_price": reservoir_target_price,
                    "reservoir_base_target_price": reservoir_base_target_price,
                    "reservoir_recent_high_cap_price": reservoir_recent_high_cap_price,
                    "reservoir_recent_mean_ratio_7d_30d": reservoir_recent_mean_ratio,
                    "reservoir_entry_to_recent_mean_ratio": _safe_div(buy_reference_price, history_mean_7d),
                    "reservoir_target_to_recent_high_ratio": _safe_div(reservoir_target_price, history_high_max_7d),
                    "reservoir_exit_days_base": reservoir_base_exit_days,
                    "reservoir_exit_penalty_multiplier": reservoir_exit_penalty_multiplier,
                    "reservoir_target_discount_rate": RESERVOIR_TARGET_DISCOUNT_RATE,
                    "source_order_snapshot_id": order_book["snapshot_id"],
                    "source_order_captured_at": as_of_time,
                    "source_selection_mode": selected_by_type[type_id]["selection_mode"],
                }
            )

    payload = {
        "market_scope": f"db-region:{region_id}",
        "as_of_time": latest_as_of_time,
        "candidates": candidates,
    }
    return DbScanCandidateBuild(payload=payload, feature_rows=feature_rows, skipped_rows=skipped_rows)


def persist_feature_rows(
    connection: sqlite3.Connection,
    *,
    feature_run_id: str,
    feature_rows: list[dict[str, Any]],
) -> None:
    connection.executemany(
        """
        INSERT INTO item_market_feature_snapshot (
            feature_run_id,
            type_id,
            region_id,
            location_id,
            as_of_time,
            avg_daily_isk_volume_30d,
            active_days_ratio_30d,
            avg_daily_units_7d,
            best_bid,
            best_ask,
            gross_spread_pct,
            net_spread_pct,
            cross_location_net_spread_pct,
            price_zscore_30d,
            price_percentile_30d,
            volatility_30d,
            estimated_exit_days_small_size
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                feature_run_id,
                row["type_id"],
                row["region_id"],
                row["location_id"],
                row["as_of_time"],
                row["avg_daily_isk_volume_30d"],
                row["active_days_ratio_30d"],
                row["avg_daily_units_7d"],
                row["best_bid"],
                row["best_ask"],
                row["gross_spread_pct"],
                row["net_spread_pct"],
                row["cross_location_net_spread_pct"],
                row["price_zscore_30d"],
                row["price_percentile_30d"],
                row["volatility_30d"],
                row["estimated_exit_days_small_size"],
            )
            for row in feature_rows
        ],
    )
    connection.commit()
