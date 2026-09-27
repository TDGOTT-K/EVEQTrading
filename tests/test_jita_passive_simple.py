from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.jita_passive_simple import (
    JitaPassiveSimpleFilterConfig,
    _estimate_daily_two_sided_flow,
    run_jita_passive_simple_scan,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveSimpleScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_data()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_type(
        self,
        *,
        connection,
        type_id: int,
        type_name: str,
        daily_volumes: list[float],
        best_bid: float = 100.0,
        best_ask: float = 115.0,
        last_success_at: str = "2026-04-06T00:01:00Z",
        next_scheduled_at: str | None = None,
        category_id: int = 7,
        market_group: str = "Ships",
        extra_buy_orders: list[tuple[float, float, str]] | None = None,
        extra_sell_orders: list[tuple[float, float, str]] | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO item_catalog (type_id, type_name, market_group, category_id, is_published) VALUES (?, ?, ?, ?, ?)",
            (type_id, type_name, market_group, category_id, 1),
        )
        for day_index, volume in enumerate(daily_volumes, start=1):
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
                    107.5,
                    115.0,
                    100.0,
                    volume,
                    1000,
                    "2026-04-06T00:00:00Z",
                    "test",
                    "ok",
                ),
            )

        order_rows = [
            (
                "snapshot-jita-simple",
                type_id * 10 + 1,
                type_id,
                1,
                best_bid,
                500000.0,
                500000.0,
                None,
                "station",
                "2026-04-06T00:00:00Z",
                90,
                60003760,
                10000002,
                None,
            ),
            (
                "snapshot-jita-simple",
                type_id * 10 + 2,
                type_id,
                0,
                best_ask,
                500000.0,
                500000.0,
                None,
                "station",
                "2026-04-06T00:00:00Z",
                90,
                60003760,
                10000002,
                None,
            ),
        ]
        for offset, (price, volume_remain, issued_at) in enumerate(extra_buy_orders or [], start=3):
            order_rows.append(
                (
                    "snapshot-jita-simple",
                    type_id * 10 + offset,
                    type_id,
                    1,
                    price,
                    volume_remain,
                    volume_remain,
                    None,
                    "station",
                    issued_at,
                    90,
                    60003760,
                    10000002,
                    None,
                )
            )
        base_offset = 3 + len(extra_buy_orders or [])
        for extra_index, (price, volume_remain, issued_at) in enumerate(extra_sell_orders or [], start=base_offset):
            order_rows.append(
                (
                    "snapshot-jita-simple",
                    type_id * 10 + extra_index,
                    type_id,
                    0,
                    price,
                    volume_remain,
                    volume_remain,
                    None,
                    "station",
                    issued_at,
                    90,
                    60003760,
                    10000002,
                    None,
                )
            )
        connection.executemany(
            """
            INSERT INTO market_order_snapshot_line (
                snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                order_range, issued_at, duration_days, location_id, region_id, account_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            order_rows,
        )
        connection.execute(
            """
            INSERT INTO sync_scope_state (
                scope_key, scope_type, region_id, location_id, type_id, last_success_at,
                last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"market_orders:10000002:{type_id}:all",
                "market_orders",
                10000002,
                None,
                type_id,
                last_success_at,
                last_success_at,
                next_scheduled_at,
                0,
                None,
                "ok:snapshot-jita-simple",
            ),
        )

    def _seed_data(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (60003760, "Jita IV - Moon 4 - Caldari Navy Assembly Plant", "station", 10000002, 30000142, 1, 1),
            )
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("snapshot-jita-simple", "2026-04-06T00:01:00Z", "region-type", 10000002, None, None, "test", "full"),
            )

            self._seed_type(
                connection=connection,
                type_id=101,
                type_name="Stable Winner",
                daily_volumes=[2_000_000.0] * 30,
                last_success_at="2026-04-07T11:50:00Z",
                next_scheduled_at="2026-04-07T12:20:00Z",
            )
            self._seed_type(
                connection=connection,
                type_id=102,
                type_name="Volatile Quantity",
                daily_volumes=[1_000_000.0 if day_index % 2 == 0 else 4_000_000.0 for day_index in range(30)],
                last_success_at="2026-04-07T10:00:00Z",
                next_scheduled_at="2026-04-07T11:00:00Z",
            )
            self._seed_type(
                connection=connection,
                type_id=103,
                type_name="Too Small",
                daily_volumes=[1_000_000.0] * 30,
                last_success_at="2026-04-07T09:00:00Z",
                next_scheduled_at="2026-04-07T12:30:00Z",
            )
            connection.commit()

    def test_simple_scan_filters_by_stability_and_capacity(self) -> None:
        with connect(self.db_path) as connection:
            result = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[101, 102, 103],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
            )

        self.assertEqual(3, result["analysis_run"]["candidate_count_total"])
        self.assertEqual(1, len(result["selected_candidates"]))
        self.assertEqual(3, result["universe_summary"]["selection_scope_target_count"])
        self.assertEqual(1, result["universe_summary"]["selected_candidate_count"])
        winner = result["selected_candidates"][0]
        self.assertEqual(101, winner["type_id"])
        self.assertGreater(winner["profit_margin"], 0.0)
        self.assertGreaterEqual(winner["daily_capacity_isk_30d"], 100_000_000.0)
        self.assertIn("safe_inventory_cap_isk", winner)
        self.assertIn("sell_liquidity_confidence_label", winner)
        self.assertLessEqual(float(winner["recommended_deployable_isk"]), float(winner["safe_inventory_cap_isk"]))

        trace_map = {
            int(trace["type_id"]): trace
            for trace in result["calc_trace"]["candidate_traces"]
        }
        self.assertTrue(trace_map[101]["filter_evaluation"]["passed"])
        self.assertIn("flip_units_cv_30d above threshold", trace_map[102]["filter_evaluation"]["rejection_reasons"])
        self.assertIn("daily_capacity_isk_30d below threshold", trace_map[103]["filter_evaluation"]["rejection_reasons"])
        self.assertEqual(
            {
                "flip_units_cv_30d above threshold": 1,
                "flip_units_cv_7d above threshold": 1,
                "daily_capacity_isk_30d below threshold": 1,
            },
            {row["reason"]: row["count"] for row in result["filter_reason_counts"]},
        )

        self.assertTrue((result["run_dir"] / "analysis_run.json").exists())
        self.assertTrue((result["run_dir"] / "calc_trace.json").exists())
        self.assertTrue((result["run_dir"] / "candidate_table.json").exists())

    def test_simple_scan_can_use_only_warm_cache_hits(self) -> None:
        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                result = run_jita_passive_simple_scan(
                    connection=connection,
                    db_path=self.db_path,
                    output_root=self.runs_root,
                    region_id=10000002,
                    explicit_type_ids=[],
                    type_id_file=None,
                    ready_market_scan_types=False,
                    all_published_market_types=False,
                    limit=None,
                    capital_budget_isk=1_000_000_000.0,
                    top_n=10,
                    strategy_set="jita-passive-simple",
                    parameter_version="v1-jita-passive-simple",
                    orders_cache_profile="warm",
                    order_type="all",
                    cache_only=True,
                )

        self.assertEqual(1, result["analysis_run"]["candidate_count_total"])
        self.assertEqual([101], [row["type_id"] for row in result["selected_targets"]])
        self.assertIsNotNone(result["cache_selection"])
        self.assertEqual("warm", result["cache_selection"]["profile_name"])
        self.assertEqual(1, result["cache_selection"]["cache_hit_count"])

    def test_simple_scan_reduces_recommended_deployable_when_top_book_is_crowded_and_fresh(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_type(
                connection=connection,
                type_id=104,
                type_name="Crowded Top Book",
                daily_volumes=[3_000_000.0] * 30,
                last_success_at="2026-04-07T11:58:00Z",
                next_scheduled_at="2026-04-07T12:28:00Z",
                extra_buy_orders=[
                    (100.0, 700_000.0, "2026-04-07T11:57:00Z"),
                    (99.9, 600_000.0, "2026-04-07T11:56:00Z"),
                    (99.8, 650_000.0, "2026-04-07T11:55:00Z"),
                ],
                extra_sell_orders=[
                    (115.0, 700_000.0, "2026-04-07T11:57:00Z"),
                    (115.1, 600_000.0, "2026-04-07T11:56:00Z"),
                    (115.2, 650_000.0, "2026-04-07T11:55:00Z"),
                ],
            )
            connection.commit()

        with connect(self.db_path) as connection:
            result = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[104],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=10_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
            )

        self.assertEqual(1, len(result["selected_candidates"]))
        crowded = result["selected_candidates"][0]
        self.assertLess(crowded["recommended_deployable_isk"], crowded["theoretical_market_capacity_isk"])
        self.assertLess(crowded["estimated_capture_share"], 0.12)
        self.assertGreater(crowded["competition_intensity"], 0.5)

    def test_daily_attribution_flags_buy_side_dominant_when_trade_high_sticks_near_bid(self) -> None:
        day_flow = _estimate_daily_two_sided_flow(
            day={
                "market_date": "2026-04-06",
                "volume": 1000.0,
                "average_price": 101.0,
                "highest_price": 102.0,
                "lowest_price": 100.0,
            },
            best_bid=100.0,
            best_ask=120.0,
        )

        self.assertEqual("buy_side_dominant", day_flow["attribution_mode"])
        self.assertLess(float(day_flow["estimated_sell_fill_units"]), float(day_flow["estimated_buy_fill_units"]))
        self.assertEqual("medium", day_flow["attribution_confidence_label"])

    def test_simple_scan_uses_sell_side_liquidity_to_cap_inventory(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_type(
                connection=connection,
                type_id=109,
                type_name="Buy Heavy Item",
                daily_volumes=[20_000_000.0] * 30,
                best_bid=100.0,
                best_ask=120.0,
            )
            connection.execute(
                """
                UPDATE market_history_daily
                SET average_price = ?, highest_price = ?, lowest_price = ?
                WHERE type_id = ?
                """,
                (101.0, 102.0, 100.0, 109),
            )
            connection.commit()

        with connect(self.db_path) as connection:
            result = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[109],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=10_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
            )

        self.assertEqual(1, len(result["selected_candidates"]))
        row = result["selected_candidates"][0]
        self.assertEqual("buy_side_dominant", row["dominant_attribution_mode_30d"])
        self.assertLess(float(row["estimated_daily_sell_isk_30d"]), float(row["estimated_daily_buy_isk_30d"]))
        self.assertLess(float(row["safe_inventory_cap_isk"]), float(row["theoretical_market_capacity_isk"]))
        self.assertLessEqual(float(row["recommended_deployable_isk"]), float(row["safe_inventory_cap_isk"]))

    def test_simple_scan_rejects_blueprint_category(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_type(
                connection=connection,
                type_id=105,
                type_name="Blueprint Candidate",
                daily_volumes=[2_000_000.0] * 30,
                category_id=9,
                market_group="Control Towers",
            )
            connection.commit()

        with connect(self.db_path) as connection:
            result = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[105],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
            )

        self.assertEqual([], result["selected_candidates"])
        trace = result["calc_trace"]["candidate_traces"][0]
        self.assertIn("blueprint category excluded", trace["filter_evaluation"]["rejection_reasons"])

    def test_simple_scan_rejects_extreme_price_sanity_gap(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_type(
                connection=connection,
                type_id=106,
                type_name="Extreme Spread Candidate",
                daily_volumes=[2_000_000.0] * 30,
                best_bid=1.0,
                best_ask=500.0,
            )
            connection.commit()

        with connect(self.db_path) as connection:
            result = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[106],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=1_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
            )

        self.assertEqual([], result["selected_candidates"])
        trace = result["calc_trace"]["candidate_traces"][0]
        self.assertIn("buy_price_vs_avg_30d below sanity threshold", trace["filter_evaluation"]["rejection_reasons"])
        self.assertIn("sell_price_vs_avg_30d above sanity threshold", trace["filter_evaluation"]["rejection_reasons"])

    def test_simple_scan_optional_unit_buy_cost_ceiling_is_disabled_by_default_and_filters_when_enabled(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_type(
                connection=connection,
                type_id=108,
                type_name="Expensive Winner",
                daily_volumes=[2_000_000.0] * 30,
                best_bid=100.0,
                best_ask=115.0,
            )
            connection.commit()

        with connect(self.db_path) as connection:
            baseline = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[108],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=10_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
            )

        self.assertEqual([108], [row["type_id"] for row in baseline["selected_candidates"]])

        with connect(self.db_path) as connection:
            capped = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[108],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=10_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
                filter_config=JitaPassiveSimpleFilterConfig(
                    max_unit_buy_cost_isk=90.0,
                ),
            )

        self.assertEqual([], capped["selected_candidates"])
        trace = capped["calc_trace"]["candidate_traces"][0]
        self.assertIn("buy_cost_per_unit above unit price ceiling", trace["filter_evaluation"]["rejection_reasons"])

    def test_simple_scan_halves_sell_side_flow_when_ask_is_far_above_recent_trade_high(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_type(
                connection=connection,
                type_id=107,
                type_name="Overhang Candidate",
                daily_volumes=[60_000_000.0] * 30,
                best_bid=100.0,
                best_ask=155.0,
            )
            connection.commit()

        no_penalty_filter = JitaPassiveSimpleFilterConfig(
            max_flip_units_cv_30d=0.30,
            max_flip_units_cv_7d=0.50,
            min_daily_capacity_isk_30d=100_000_000.0,
            max_sell_price_vs_avg_30d=2.0,
            sell_price_overhang_step_pct=10.0,
            sell_price_overhang_penalty_factor=0.5,
        )
        with_penalty_filter = JitaPassiveSimpleFilterConfig(
            max_flip_units_cv_30d=0.30,
            max_flip_units_cv_7d=0.50,
            min_daily_capacity_isk_30d=100_000_000.0,
            max_sell_price_vs_avg_30d=2.0,
            sell_price_overhang_step_pct=0.20,
            sell_price_overhang_penalty_factor=0.5,
        )

        with connect(self.db_path) as connection:
            baseline = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[107],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=10_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
                filter_config=no_penalty_filter,
            )
        with connect(self.db_path) as connection:
            penalized = run_jita_passive_simple_scan(
                connection=connection,
                db_path=self.db_path,
                output_root=self.runs_root,
                region_id=10000002,
                explicit_type_ids=[107],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=10_000_000_000.0,
                top_n=10,
                strategy_set="jita-passive-simple",
                parameter_version="v1-jita-passive-simple",
                filter_config=with_penalty_filter,
            )

        baseline_row = baseline["selected_candidates"][0]
        penalized_row = penalized["selected_candidates"][0]
        self.assertEqual(0, baseline_row["sell_price_overhang_steps"])
        self.assertEqual(1.0, baseline_row["sell_fill_penalty_multiplier"])
        self.assertEqual(1, penalized_row["sell_price_overhang_steps"])
        self.assertEqual(0.5, penalized_row["sell_fill_penalty_multiplier"])
        self.assertLess(penalized_row["theoretical_market_capacity_isk"], baseline_row["theoretical_market_capacity_isk"])
        self.assertLess(penalized_row["recommended_deployable_isk"], baseline_row["recommended_deployable_isk"])

    def test_cli_scan_jita_passive_simple_prints_structured_summary(self) -> None:
        stdout = io.StringIO()
        argv = [
            "eveq",
            "scan-jita-passive-simple",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.runs_root),
            "--region-id",
            "10000002",
            "--type-id",
            "101",
            "--type-id",
            "102",
            "--type-id",
            "103",
            "--top-n",
            "10",
        ]
        with patch.object(sys, "argv", argv):
            with redirect_stdout(stdout):
                exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive simple scan completed:"))
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual(3, payload["candidate_count_built"])
        self.assertEqual(1, payload["candidate_count_selected"])
        self.assertEqual(0, payload["skipped_count"])
        self.assertIsNone(payload["cache_selection"])

    def test_cli_scan_jita_passive_simple_can_report_cache_only_selection(self) -> None:
        stdout = io.StringIO()
        argv = [
            "eveq",
            "scan-jita-passive-simple",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.runs_root),
            "--region-id",
            "10000002",
            "--cache-only",
            "--orders-cache-profile",
            "warm",
        ]
        with patch(
            "eveqtrading.order_refresh._utc_now",
            return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
        ):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual(1, payload["candidate_count_built"])
        self.assertEqual("warm", payload["cache_selection"]["profile_name"])

    def test_cli_scan_jita_passive_full_prints_universe_summary(self) -> None:
        stdout = io.StringIO()
        argv = [
            "eveq",
            "scan-jita-passive-full",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.runs_root),
            "--region-id",
            "10000002",
            "--type-id",
            "101",
            "--type-id",
            "102",
            "--type-id",
            "103",
        ]
        with patch.object(sys, "argv", argv):
            with redirect_stdout(stdout):
                exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive full scan completed:"))
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual(3, payload["candidate_count_built"])
        self.assertEqual(1, payload["candidate_count_selected"])
        self.assertEqual(3, payload["universe_summary"]["selection_scope_target_count"])
        self.assertEqual(1, payload["universe_summary"]["selected_candidate_count"])
        self.assertEqual(
            1,
            {row["reason"]: row["count"] for row in payload["filter_reason_counts"]}["flip_units_cv_30d above threshold"],
        )


if __name__ == "__main__":
    unittest.main()
