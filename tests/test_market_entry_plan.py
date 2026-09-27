from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.market_entry_plan import build_market_entry_plan


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class MarketEntryPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.log_dir = self.base_dir / "logs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_scan(self, *, scan_run_id: str) -> None:
        run_dir = self.runs_root / scan_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO scan_run (
                    scan_run_id, started_at, finished_at, market_scope, strategy_set,
                    capital_budget_isk, parameter_version, input_path, output_dir,
                    selected_candidate_count, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scan_run_id,
                    "2026-04-09T10:00:00Z",
                    "2026-04-09T10:00:20Z",
                    "region:10000002",
                    "jita-passive-simple",
                    1_000_000_000.0,
                    "v1-jita-passive-simple",
                    str(self.base_dir / "inputs" / f"{scan_run_id}.json"),
                    str(run_dir),
                    1,
                    "completed",
                ),
            )
            connection.execute(
                """
                INSERT INTO opportunity_snapshot (
                    scan_run_id, opportunity_id, strategy_type, type_id, type_name, region_id,
                    buy_location_id, sell_location_id, entry_mode, exit_mode, unit_profit,
                    total_profit, profit_margin, capital_required, tradable_units,
                    expected_turnover_days, risk_adjusted_return, decision_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scan_run_id,
                    "3001-10000002-jita-passive-simple",
                    "jita-passive-simple",
                    3001,
                    "Alpha Item",
                    10000002,
                    60003760,
                    60003760,
                    "place_buy_order_best_bid",
                    "place_sell_order_best_ask",
                    4_500_000.0,
                    32_000_000.0,
                    0.032,
                    280_000_000.0,
                    5_000.0,
                    0.5,
                    0.18,
                    "healthy passive spread and daily capacity",
                ),
            )
            connection.commit()

        (run_dir / "analysis_run.json").write_text(
            json.dumps({"scan_run_id": scan_run_id, "strategy_set": "jita-passive-simple"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "candidate_table.json").write_text(
            json.dumps(
                [
                    {
                        "opportunity_id": "3001-10000002-jita-passive-simple",
                        "buy_location_name": "Jita Trade Hub",
                        "sell_location_name": "Jita Trade Hub",
                        "buy_reference_price": 140_000_000.0,
                        "sell_reference_price": 149_000_000.0,
                        "gross_spread_pct": 0.06428571428571428,
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "calc_trace.json").write_text(
            json.dumps(
                {
                    "candidate_traces": [
                        {
                            "type_id": 3001,
                            "type_name": "Alpha Item",
                            "strategy_pool": "jita-passive-simple",
                            "source_record": {
                                "opportunity_id": "3001-10000002-jita-passive-simple",
                                "type_id": 3001,
                                "avg_daily_isk_volume_30d": 12_000_000_000.0,
                                "avg_daily_units_7d": 9.0,
                                "active_days_ratio_30d": 0.98,
                                "volatility_30d": 0.08,
                                "buy_reference_price": 140_000_000.0,
                                "sell_reference_price": 149_000_000.0,
                                "max_position_isk": 4_200_000_000.0,
                                "recommended_deployable_isk": 920_000_000.0,
                                "theoretical_market_capacity_isk": 4_200_000_000.0,
                                "estimated_capture_share": 0.11,
                                "competition_intensity": 0.46,
                                "recent_high_trade_price_7d": 146_000_000.0,
                                "sell_price_overhang_ratio": 0.020547945205479535,
                                "sell_price_overhang_steps": 0,
                                "sell_fill_penalty_multiplier": 1.0,
                                "daily_capacity_isk_30d": 4_200_000_000.0,
                                "avg_price_weighted_30d": 145_000_000.0,
                                "flip_units_mean_30d": 28.0,
                                "flip_units_mean_7d": 9.0,
                                "flip_units_cv_30d": 0.35,
                                "flip_units_cv_7d": 0.20,
                                "source_order_snapshot_id": "orders-alpha",
                                "source_order_captured_at": "2026-04-09T10:00:00Z",
                            },
                            "derived": {
                                "net_spread_pct": 0.032,
                                "screened_profit_margin": 0.032,
                                "estimated_exit_days_small_size": 0.5,
                            },
                            "filter_evaluation": {
                                "passed": True,
                                "rejection_reasons": [],
                            },
                        },
                        {
                            "type_id": 3002,
                            "type_name": "Beta Item",
                            "strategy_pool": "jita-passive-simple",
                            "source_record": {
                                "opportunity_id": "3002-10000002-jita-passive-simple",
                                "type_id": 3002,
                                "avg_daily_isk_volume_30d": 180_000_000.0,
                                "active_days_ratio_30d": 0.92,
                                "volatility_30d": 0.16,
                                "flip_units_cv_30d": 0.95,
                                "flip_units_cv_7d": 0.55,
                            },
                            "derived": {
                                "screened_profit_margin": 0.011,
                                "estimated_exit_days_small_size": 1.4,
                            },
                            "filter_evaluation": {
                                "passed": False,
                                "rejection_reasons": ["profit margin below threshold"],
                            },
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
        (run_dir / "feature_table.json").write_text(
            json.dumps([{"type_id": 3001, "type_name": "Alpha Item"}], ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "skipped_table.json").write_text(
            json.dumps([], ensure_ascii=False),
            encoding="utf-8",
        )

    def test_market_entry_plan_uses_total_budget_override_and_real_position_cap(self) -> None:
        scan_run_id = "scan-entry-plan"
        self._seed_scan(scan_run_id=scan_run_id)

        with connect(self.db_path) as connection:
            plan = build_market_entry_plan(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                total_budget_isk=10_000_000_000.0,
                risk_profile="standard",
                max_candidates=20,
                max_positions=5,
                near_miss_limit=5,
            )

        self.assertEqual(scan_run_id, plan["scan_summary"]["scan_run_id"])
        self.assertEqual(10_000_000_000.0, plan["portfolio_summary"]["total_budget_isk"])
        self.assertEqual(1, plan["portfolio_summary"]["execute_now_count"])
        self.assertEqual(1, len(plan["first_wave_execute_now"]))
        first_row = plan["first_wave_execute_now"][0]
        self.assertEqual("Alpha Item", first_row["type_name"])
        self.assertGreater(float(first_row["suggested_capital_isk"] or 0.0), 280_000_000.0)
        self.assertEqual("stable", first_row["flow_quality"])
        self.assertEqual(1_840_000_000.0, first_row["recommended_deployable_isk"])
        self.assertEqual(4_200_000_000.0, first_row["theoretical_market_capacity_isk"])
        self.assertAlmostEqual(0.11, first_row["estimated_capture_share"])
        self.assertEqual(1.0, first_row["sell_fill_penalty_multiplier"])
        self.assertAlmostEqual(0.020547945205479535, first_row["sell_price_overhang_ratio"])
        self.assertEqual("no penalty", first_row["sell_overhang_summary"])
        self.assertIn(
            "recommended_deployable_isk_scaled",
            [str(item) for item in (first_row.get("binding_constraints") or [])],
        )
        self.assertEqual("build_first_wave_orders", plan["recommended_focus"])
        self.assertEqual("manual_budget_override", plan["analysis_context"]["context_mode"])

    def test_cli_plan_market_entry_prints_payload(self) -> None:
        fake_plan = {
            "scan_summary": {"scan_run_id": "scan-entry-plan"},
            "portfolio_summary": {"execute_now_count": 3},
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "plan-market-entry",
            "--db-path",
            str(self.db_path),
            "--total-budget-isk",
            "5000000000",
        ]
        with patch("eveqtrading.cli.build_market_entry_plan", return_value=fake_plan) as mock_build:
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual(5_000_000_000.0, mock_build.call_args.kwargs["total_budget_isk"])
        output_lines = stdout.getvalue().strip().splitlines()
        self.assertTrue(output_lines[0].startswith("Market entry plan ready:"))
        payload = json.loads("\n".join(output_lines[1:]))
        self.assertEqual("scan-entry-plan", payload["scan_summary"]["scan_run_id"])


if __name__ == "__main__":
    unittest.main()
