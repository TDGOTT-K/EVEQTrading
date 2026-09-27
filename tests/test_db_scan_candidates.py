from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from eveqtrading.analysis import analyze_market_scan
from eveqtrading.db import connect, init_db
from eveqtrading.db_scan import build_db_scan_candidates
from eveqtrading.models import FeeConfig, FilterConfig, ScanConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class DbScanCandidateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_data()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_data(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, is_published) VALUES (?, ?, ?)",
                (999001, "Reservoir Test Item", 1),
            )
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (60000001, "Test Station", "station", 10000002, 30000001, 1, 1),
            )

            for day_index in range(30):
                average_price = 90.0 if day_index < 15 else 110.0
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        999001,
                        10000002,
                        f"2026-03-{day_index + 1:02d}",
                        average_price,
                        average_price * 1.02,
                        average_price * 0.98,
                        2_000_000.0,
                        1000,
                        "2026-04-06T00:00:00Z",
                        "test",
                        "ok",
                    ),
                )

            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("snapshot-1", "2026-04-06T00:01:00Z", "region-type", 10000002, None, None, "test", "full"),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("snapshot-1", 1, 999001, 1, 80.0, 500000.0, 500000.0, None, "station", "2026-04-06T00:00:00Z", 90, 60000001, 10000002, None),
                    ("snapshot-1", 2, 999001, 0, 82.0, 500000.0, 500000.0, None, "station", "2026-04-06T00:00:00Z", 90, 60000001, 10000002, None),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, location_id, type_id, last_success_at,
                    last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market-orders-999001",
                    "market_orders",
                    10000002,
                    None,
                    999001,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    None,
                    0,
                    None,
                    "ok:snapshot-1",
                ),
            )
            connection.commit()

    def _seed_jita_passive_data(self, *, type_id: int, type_name: str) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, is_published) VALUES (?, ?, ?)",
                (type_id, type_name, 1),
            )
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (60003760, "Jita Trade Hub", "station", 10000002, 30000142, 1, 1),
            )
            for day_index in range(30):
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        type_id,
                        10000002,
                        f"2026-03-{day_index + 1:02d}",
                        100.0,
                        102.0,
                        98.0,
                        2_000_000.0,
                        1000,
                        "2026-04-06T00:00:00Z",
                        "test",
                        "ok",
                    ),
                )
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("snapshot-jita-passive", "2026-04-06T00:01:00Z", "region-type", 10000002, None, None, "test", "full"),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("snapshot-jita-passive", 101, type_id, 1, 98.0, 500000.0, 500000.0, None, "station", "2026-04-06T00:00:00Z", 90, 60003760, 10000002, None),
                    ("snapshot-jita-passive", 102, type_id, 0, 106.0, 500000.0, 500000.0, None, "station", "2026-04-06T00:00:00Z", 90, 60003760, 10000002, None),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, location_id, type_id, last_success_at,
                    last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"market-orders-{type_id}",
                    "market_orders",
                    10000002,
                    None,
                    type_id,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    None,
                    0,
                    None,
                    "ok:snapshot-jita-passive",
                ),
            )
            connection.commit()

    def _seed_reservoir_case(
        self,
        *,
        type_id: int,
        type_name: str,
        average_prices: list[float],
        best_bid: float,
        best_ask: float,
        location_id: int = 60000011,
        location_name: str = "Reservoir Test Station",
        snapshot_id: str | None = None,
    ) -> None:
        effective_snapshot_id = snapshot_id or f"snapshot-{type_id}"
        with connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, is_published) VALUES (?, ?, ?)",
                (type_id, type_name, 1),
            )
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (location_id, location_name, "station", 10000002, 30000001, 1, 1),
            )
            for day_index, average_price in enumerate(average_prices, start=1):
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        type_id,
                        10000002,
                        f"2026-03-{day_index:02d}",
                        average_price,
                        average_price * 1.02,
                        average_price * 0.98,
                        2_000_000.0,
                        1000,
                        "2026-04-06T00:00:00Z",
                        "test",
                        "ok",
                    ),
                )
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (effective_snapshot_id, "2026-04-06T00:01:00Z", "region-type", 10000002, None, None, "test", "full"),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (effective_snapshot_id, type_id * 10 + 1, type_id, 1, best_bid, 500000.0, 500000.0, None, "station", "2026-04-06T00:00:00Z", 90, location_id, 10000002, None),
                    (effective_snapshot_id, type_id * 10 + 2, type_id, 0, best_ask, 500000.0, 500000.0, None, "station", "2026-04-06T00:00:00Z", 90, location_id, 10000002, None),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, location_id, type_id, last_success_at,
                    last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"market-orders-{type_id}",
                    "market_orders",
                    10000002,
                    None,
                    type_id,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    None,
                    0,
                    None,
                    f"ok:{effective_snapshot_id}",
                ),
            )
            connection.commit()

    def test_db_scan_builds_reservoir_candidate_and_real_exit_days(self) -> None:
        connection = connect(self.db_path)
        try:
            candidate_build = build_db_scan_candidates(
                connection,
                region_id=10000002,
                fee_config=FeeConfig(),
                test_position_isk=250_000_000.0,
                selected_targets=[
                    {
                        "type_id": 999001,
                        "type_name": "Reservoir Test Item",
                        "selection_mode": "explicit",
                    }
                ],
            )
        finally:
            connection.close()

        self.assertEqual(1, len(candidate_build.feature_rows))
        feature_row = candidate_build.feature_rows[0]
        self.assertGreater(feature_row["estimated_exit_days_small_size"], 0.0)
        self.assertGreater(feature_row["estimated_exit_days_small_size_ask"], 0.0)

        reservoir_candidates = [
            row for row in candidate_build.payload["candidates"] if row["strategy_hint"] == "reservoir"
        ]
        self.assertEqual(1, len(reservoir_candidates))
        reservoir_candidate = reservoir_candidates[0]
        self.assertEqual("sell_mean_reversion_target", reservoir_candidate["exit_mode_override"])
        self.assertGreater(reservoir_candidate["screened_profit_margin_override"], 0.0)
        self.assertGreater(reservoir_candidate["estimated_exit_days_override"], 0.0)
        self.assertIsNotNone(reservoir_candidate["max_position_isk"])
        self.assertIsNotNone(reservoir_candidate["recommended_test_position_isk"])
        self.assertLessEqual(reservoir_candidate["capital_required_override"], 250_000_000.0)
        self.assertEqual("avg_daily_isk_volume_30d_div_12", reservoir_candidate["inventory_limit_basis"])
        self.assertLessEqual(
            reservoir_candidate["reservoir_target_price"],
            reservoir_candidate["reservoir_recent_high_cap_price"],
        )
        self.assertGreaterEqual(reservoir_candidate["reservoir_exit_penalty_multiplier"], 1.0)

    def test_reservoir_candidate_can_pass_analysis_filters(self) -> None:
        connection = connect(self.db_path)
        try:
            candidate_build = build_db_scan_candidates(
                connection,
                region_id=10000002,
                fee_config=FeeConfig(),
                test_position_isk=250_000_000.0,
                selected_targets=[
                    {
                        "type_id": 999001,
                        "type_name": "Reservoir Test Item",
                        "selection_mode": "explicit",
                    }
                ],
            )
        finally:
            connection.close()

        analysis_run, calc_trace, selected_candidates, _ = analyze_market_scan(
            payload=candidate_build.payload,
            input_path=self.db_path,
            fee_config=FeeConfig(),
            filter_config=FilterConfig(),
            scan_config=ScanConfig(
                strategy_set="v1-db-region",
                parameter_version="v1-db",
                test_position_isk=250_000_000.0,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
            ),
        )

        self.assertEqual(2, analysis_run["candidate_count_total"])
        self.assertTrue(any(row["strategy_type"] == "reservoir" for row in selected_candidates))
        reservoir_trace = next(
            trace for trace in calc_trace["candidate_traces"] if trace["strategy_pool"] == "reservoir"
        )
        self.assertTrue(reservoir_trace["filter_evaluation"]["passed"])
        self.assertEqual(
            "candidate_override",
            reservoir_trace["derived"]["estimated_exit_days_source"],
        )
        self.assertLessEqual(reservoir_trace["derived"]["capital_required_isk"], 250_000_000.0)

    def test_reservoir_candidate_rejects_broken_recent_price_regime(self) -> None:
        self._seed_reservoir_case(
            type_id=999004,
            type_name="Broken Reservoir Item",
            average_prices=[100.0] * 23 + [40.0] * 7,
            best_bid=38.0,
            best_ask=42.0,
        )
        connection = connect(self.db_path)
        try:
            candidate_build = build_db_scan_candidates(
                connection,
                region_id=10000002,
                fee_config=FeeConfig(),
                test_position_isk=250_000_000.0,
                selected_targets=[
                    {
                        "type_id": 999004,
                        "type_name": "Broken Reservoir Item",
                        "selection_mode": "explicit",
                    }
                ],
            )
        finally:
            connection.close()

        reservoir_candidate = next(
            row for row in candidate_build.payload["candidates"] if row["strategy_hint"] == "reservoir"
        )
        self.assertLess(reservoir_candidate["reservoir_recent_mean_ratio_7d_30d"], 0.75)

        _, calc_trace, selected_candidates, _ = analyze_market_scan(
            payload=candidate_build.payload,
            input_path=self.db_path,
            fee_config=FeeConfig(),
            filter_config=FilterConfig(),
            scan_config=ScanConfig(
                strategy_set="v1-db-region",
                parameter_version="v1-db",
                test_position_isk=250_000_000.0,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
            ),
        )

        self.assertFalse(any(row["strategy_type"] == "reservoir" for row in selected_candidates))
        reservoir_trace = next(
            trace
            for trace in calc_trace["candidate_traces"]
            if trace["strategy_pool"] == "reservoir" and trace["type_id"] == 999004
        )
        self.assertIn(
            "reservoir_recent_mean_ratio_7d_30d below floor",
            reservoir_trace["filter_evaluation"]["rejection_reasons"],
        )

    def test_reservoir_candidate_rejects_flash_crash_below_recent_mean(self) -> None:
        self._seed_reservoir_case(
            type_id=999005,
            type_name="Flash Crash Reservoir Item",
            average_prices=[100.0] * 29 + [95.0],
            best_bid=38.0,
            best_ask=42.0,
        )
        connection = connect(self.db_path)
        try:
            candidate_build = build_db_scan_candidates(
                connection,
                region_id=10000002,
                fee_config=FeeConfig(),
                test_position_isk=250_000_000.0,
                selected_targets=[
                    {
                        "type_id": 999005,
                        "type_name": "Flash Crash Reservoir Item",
                        "selection_mode": "explicit",
                    }
                ],
            )
        finally:
            connection.close()

        reservoir_candidate = next(
            row for row in candidate_build.payload["candidates"] if row["strategy_hint"] == "reservoir"
        )
        self.assertLess(reservoir_candidate["reservoir_entry_to_recent_mean_ratio"], 0.70)
        self.assertGreater(reservoir_candidate["reservoir_exit_penalty_multiplier"], 1.0)

        _, calc_trace, selected_candidates, _ = analyze_market_scan(
            payload=candidate_build.payload,
            input_path=self.db_path,
            fee_config=FeeConfig(),
            filter_config=FilterConfig(),
            scan_config=ScanConfig(
                strategy_set="v1-db-region",
                parameter_version="v1-db",
                test_position_isk=250_000_000.0,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
            ),
        )

        self.assertFalse(any(row["strategy_type"] == "reservoir" for row in selected_candidates))
        reservoir_trace = next(
            trace
            for trace in calc_trace["candidate_traces"]
            if trace["strategy_pool"] == "reservoir" and trace["type_id"] == 999005
        )
        self.assertIn(
            "reservoir_entry_to_recent_mean_ratio below floor",
            reservoir_trace["filter_evaluation"]["rejection_reasons"],
        )

    def test_db_scan_builds_jita_passive_flip_candidate_with_inventory_cap(self) -> None:
        self._seed_jita_passive_data(type_id=999002, type_name="Jita Passive Test Item")
        connection = connect(self.db_path)
        try:
            candidate_build = build_db_scan_candidates(
                connection,
                region_id=10000002,
                fee_config=FeeConfig(),
                test_position_isk=250_000_000.0,
                selected_targets=[
                    {
                        "type_id": 999002,
                        "type_name": "Jita Passive Test Item",
                        "selection_mode": "explicit",
                    }
                ],
            )
        finally:
            connection.close()

        passive_candidates = [
            row for row in candidate_build.payload["candidates"] if row["strategy_hint"] == "jita-passive-flip"
        ]
        self.assertEqual(1, len(passive_candidates))
        passive_candidate = passive_candidates[0]
        self.assertEqual("place_buy_order_best_bid", passive_candidate["entry_mode_override"])
        self.assertEqual("place_sell_order_best_ask", passive_candidate["exit_mode_override"])
        self.assertGreater(passive_candidate["screened_profit_margin_override"], 0.0)
        self.assertLessEqual(passive_candidate["capital_required_override"], 250_000_000.0)
        self.assertEqual("avg_daily_isk_volume_30d_div_12", passive_candidate["inventory_limit_basis"])

    def test_jita_passive_candidate_can_pass_analysis_filters(self) -> None:
        self._seed_jita_passive_data(type_id=999003, type_name="Jita Passive Analysis Item")
        connection = connect(self.db_path)
        try:
            candidate_build = build_db_scan_candidates(
                connection,
                region_id=10000002,
                fee_config=FeeConfig(),
                test_position_isk=250_000_000.0,
                selected_targets=[
                    {
                        "type_id": 999003,
                        "type_name": "Jita Passive Analysis Item",
                        "selection_mode": "explicit",
                    }
                ],
            )
        finally:
            connection.close()

        _, calc_trace, selected_candidates, _ = analyze_market_scan(
            payload=candidate_build.payload,
            input_path=self.db_path,
            fee_config=FeeConfig(),
            filter_config=FilterConfig(),
            scan_config=ScanConfig(
                strategy_set="v1-db-region",
                parameter_version="v1-db",
                test_position_isk=250_000_000.0,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
            ),
        )

        self.assertTrue(any(row["strategy_type"] == "jita-passive-flip" for row in selected_candidates))
        passive_candidate = next(row for row in selected_candidates if row["strategy_type"] == "jita-passive-flip")
        self.assertLessEqual(passive_candidate["capital_required"], 250_000_000.0)
        self.assertIsNotNone(passive_candidate["max_position_isk"])
        passive_trace = next(
            trace for trace in calc_trace["candidate_traces"] if trace["strategy_pool"] == "jita-passive-flip"
        )
        self.assertEqual("candidate_override", passive_trace["derived"]["estimated_exit_days_source"])
        self.assertLessEqual(passive_trace["derived"]["capital_required_isk"], 250_000_000.0)


if __name__ == "__main__":
    unittest.main()
