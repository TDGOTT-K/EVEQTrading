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
from eveqtrading.jita_reservoir_report import build_jita_reservoir_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaReservoirReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.output_root = self.base_dir / "reports"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_scan(self, *, scan_run_id: str, with_selected_reservoir: bool) -> None:
        run_dir = self.runs_root / scan_run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        calc_trace_payload = {
            "formula_versions": {
                "reservoir_target_price": "v2-min(history_mean_30d*0.99, history_high_max_7d*1.15)",
                "reservoir_regime_guard": "v2-history_mean_7d-over-history_mean_30d-floor-0.75-and-buy-over-history_mean_7d-floor-0.70",
                "reservoir_exit_penalty": "v2-estimated_exit_days_small_size_ask-times-max(1,target/buy)^2",
            },
            "candidate_traces": [
                {
                    "type_id": 3001,
                    "type_name": "低估稳健甲",
                    "strategy_pool": "reservoir",
                    "source_record": {
                        "opportunity_id": "3001-10000002-reservoir",
                        "type_id": 3001,
                        "type_name": "低估稳健甲",
                        "buy_location_name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                        "sell_location_name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                        "buy_reference_price": 90_000_000.0,
                        "sell_reference_price": 99_000_000.0,
                        "reservoir_target_price": 99_000_000.0,
                        "reservoir_target_discount_rate": 0.01,
                        "history_mean_30d": 100_000_000.0,
                        "avg_daily_isk_volume_30d": 900_000_000.0,
                        "active_days_ratio_30d": 0.93,
                        "avg_daily_units_7d": 25.0,
                        "estimated_exit_days_small_size_ask": 1.8,
                        "recommended_test_position_isk": 75_000_000.0,
                        "max_position_isk": 300_000_000.0,
                        "inventory_limit_basis": "avg_daily_isk_volume_30d_div_12",
                        "source_order_snapshot_id": "orders-3001",
                        "source_order_captured_at": "2026-04-12T10:00:00Z",
                        "history_latest_market_date": "2026-04-11",
                        "history_latest_fetched_at": "2026-04-12T10:03:00Z",
                        "volatility_30d": 0.12,
                    },
                    "derived": {
                        "price_zscore_30d": -1.65,
                        "screened_profit_margin": 0.064,
                        "estimated_exit_days_small_size": 1.8,
                    },
                    "filter_evaluation": {
                        "passed": with_selected_reservoir,
                        "rejection_reasons": [] if with_selected_reservoir else ["screened_profit_margin below threshold"],
                    },
                },
                {
                    "type_id": 3002,
                    "type_name": "低估但利润不够",
                    "strategy_pool": "reservoir",
                    "source_record": {
                        "opportunity_id": "3002-10000002-reservoir",
                        "type_id": 3002,
                        "type_name": "低估但利润不够",
                        "buy_location_name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                        "sell_location_name": "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
                        "buy_reference_price": 120_000_000.0,
                        "sell_reference_price": 123_750_000.0,
                        "reservoir_target_price": 123_750_000.0,
                        "reservoir_target_discount_rate": 0.01,
                        "history_mean_30d": 125_000_000.0,
                        "avg_daily_isk_volume_30d": 600_000_000.0,
                        "active_days_ratio_30d": 0.88,
                        "avg_daily_units_7d": 10.0,
                        "estimated_exit_days_small_size_ask": 3.5,
                        "recommended_test_position_isk": 60_000_000.0,
                        "max_position_isk": 180_000_000.0,
                        "inventory_limit_basis": "avg_daily_isk_volume_30d_div_12",
                        "source_order_snapshot_id": "orders-3002",
                        "source_order_captured_at": "2026-04-12T10:01:00Z",
                        "history_latest_market_date": "2026-04-11",
                        "history_latest_fetched_at": "2026-04-12T10:04:00Z",
                        "volatility_30d": 0.14,
                    },
                    "derived": {
                        "price_zscore_30d": -1.10,
                        "screened_profit_margin": 0.018,
                        "estimated_exit_days_small_size": 3.5,
                    },
                    "filter_evaluation": {
                        "passed": False,
                        "rejection_reasons": ["screened_profit_margin below threshold"],
                    },
                },
                {
                    "type_id": 4001,
                    "type_name": "普通同站倒卖件",
                    "strategy_pool": "station-flip",
                    "source_record": {
                        "opportunity_id": "4001-10000002-station-flip",
                        "type_id": 4001,
                    },
                    "derived": {
                        "price_zscore_30d": 0.10,
                        "screened_profit_margin": 0.03,
                        "estimated_exit_days_small_size": 0.8,
                    },
                    "filter_evaluation": {
                        "passed": True,
                        "rejection_reasons": [],
                    },
                },
            ],
        }

        (run_dir / "calc_trace.json").write_text(
            json.dumps(calc_trace_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (run_dir / "analysis_run.json").write_text(
            json.dumps({"scan_run_id": scan_run_id, "strategy_set": "all-market"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "candidate_table.json").write_text("[]", encoding="utf-8")
        (run_dir / "feature_table.json").write_text("[]", encoding="utf-8")
        (run_dir / "skipped_table.json").write_text("[]", encoding="utf-8")
        (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

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
                    "2026-04-12T10:00:00Z",
                    "2026-04-12T10:05:00Z",
                    "region:10000002",
                    "all-market",
                    1_000_000_000.0,
                    "v1",
                    str(self.base_dir / "inputs" / f"{scan_run_id}.json"),
                    str(run_dir),
                    2 if with_selected_reservoir else 1,
                    "completed",
                ),
            )
            if with_selected_reservoir:
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
                        "3001-10000002-reservoir",
                        "reservoir",
                        3001,
                        "低估稳健甲",
                        10000002,
                        60003760,
                        60003760,
                        "buy_best_ask",
                        "sell_mean_reversion_target",
                        5_760_000.0,
                        5_760_000.0,
                        0.064,
                        90_000_000.0,
                        1.0,
                        1.8,
                        0.056,
                        "passed hard filters; screened_profit_margin=0.0640; estimated_exit_days_small_size=1.80; price_zscore_30d=-1.65",
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
                    "4001-10000002-station-flip",
                    "station-flip",
                    4001,
                    "普通同站倒卖件",
                    10000002,
                    60003760,
                    60003760,
                    "buy_best_ask",
                    "sell_best_bid",
                    3_000_000.0,
                    3_000_000.0,
                    0.03,
                    100_000_000.0,
                    1.0,
                    0.8,
                    0.028,
                    "station flip",
                ),
            )
            connection.commit()

    def test_report_uses_formal_reservoir_definition(self) -> None:
        self._seed_scan(scan_run_id="scan-reservoir-a", with_selected_reservoir=True)

        with connect(self.db_path) as connection:
            report = build_jita_reservoir_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=None,
                output_root=self.output_root,
                scan_run_id="scan-reservoir-a",
                prefer_latest_selected=False,
                top_selected=10,
                top_near_misses=10,
            )

        self.assertEqual("scan-reservoir-a", report["scan_summary"]["scan_run_id"])
        self.assertEqual(2, report["strategy_summary"]["reservoir_candidate_count"])
        self.assertEqual(1, report["strategy_summary"]["reservoir_selected_count"])
        self.assertEqual("低估稳健甲", report["selected_entries"][0]["type_name"])
        self.assertEqual(100_000_000.0, report["selected_entries"][0]["history_mean_30d"])
        self.assertEqual(99_000_000.0, report["selected_entries"][0]["target_sell_price"])
        self.assertEqual(-1.65, report["selected_entries"][0]["price_zscore_30d"])
        self.assertEqual("低估但利润不够", report["near_miss_entries"][0]["type_name"])
        markdown = Path(report["files"]["markdown_path"]).read_text(encoding="utf-8")
        self.assertIn("min(history_mean_30d * 0.99, history_high_max_7d * 1.15)", markdown)
        self.assertIn("buy-over-history_mean_7d-floor-0.70", markdown)
        self.assertIn("低估稳健甲", markdown)
        self.assertIn("低估但利润不够", markdown)
        self.assertNotIn("普通同站倒卖件", markdown)

    def test_cli_prints_markdown_path(self) -> None:
        self._seed_scan(scan_run_id="scan-reservoir-b", with_selected_reservoir=True)

        stdout = io.StringIO()
        with patch.object(
            sys,
            "argv",
            [
                "eveq",
                "report-jita-reservoir",
                "--db-path",
                str(self.db_path),
                "--runs-root",
                str(self.runs_root),
                "--output-root",
                str(self.output_root),
                "--scan-run-id",
                "scan-reservoir-b",
            ],
        ):
            with redirect_stdout(stdout):
                exit_code = cli_main()

        self.assertEqual(0, exit_code)
        output = stdout.getvalue()
        self.assertIn("Jita reservoir report ready", output)
        self.assertIn("Markdown:", output)


if __name__ == "__main__":
    unittest.main()
