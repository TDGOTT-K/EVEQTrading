from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.account_budget import build_analysis_context, derive_account_budget_context, load_account_snapshot
from eveqtrading.advisor import build_market_advice
from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class AccountBudgetTests(unittest.TestCase):
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

    def _write_account_snapshot(self, *, run_id: str, wallet_balance_isk: float) -> None:
        run_dir = self.runs_root / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_id": run_id,
            "character": {
                "character_id": 12345,
                "character_name": "Budget Pilot",
                "owner_hash": "owner-hash-1",
            },
            "selection_context": {
                "mode": "default_linked_character",
                "used_explicit_selection": False,
                "linked_character_count": 2,
                "requested_character_id": None,
                "requested_character_name": None,
                "requested_owner_hash": None,
            },
            "wallet": {
                "balance": wallet_balance_isk,
                "trace": {"url": "https://esi.evetech.net/latest/characters/12345/wallet?datasource=tranquility"},
            },
            "assets": {
                "summary": {"count": 2, "type_count": 2, "total_quantity": 50},
                "trace": {"pages_total": 1, "requests": [{"page": 1}]},
            },
            "orders": {
                "summary": {
                    "count": 3,
                    "buy_count": 1,
                    "sell_count": 2,
                    "remaining_buy_value": 2_000_000_000.0,
                    "remaining_sell_value": 3_500_000_000.0,
                },
                "trace": {"url": "https://esi.evetech.net/latest/characters/12345/orders?datasource=tranquility"},
            },
            "order_history": {
                "summary": {"count": 4, "buy_count": 1, "sell_count": 3, "status_counts": {"expired": 4}},
                "trace": {"pages_total": 1, "requests": [{"page": 1}]},
            },
        }
        (run_dir / "account_snapshot.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

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
                    "2026-04-10T10:00:00Z",
                    "2026-04-10T10:00:20Z",
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
            json.dumps([{"opportunity_id": "3001-10000002-jita-passive-simple"}], ensure_ascii=False),
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
                                "active_days_ratio_30d": 0.98,
                                "volatility_30d": 0.08,
                                "buy_reference_price": 140_000_000.0,
                                "sell_reference_price": 149_000_000.0,
                                "max_position_isk": 4_200_000_000.0,
                                "recommended_deployable_isk": 920_000_000.0,
                                "theoretical_market_capacity_isk": 4_200_000_000.0,
                                "estimated_capture_share": 0.11,
                                "competition_intensity": 0.46,
                                "flip_units_cv_30d": 0.35,
                                "flip_units_cv_7d": 0.20,
                                "source_order_snapshot_id": "orders-alpha",
                                "source_order_captured_at": "2026-04-10T10:00:00Z",
                            },
                            "derived": {
                                "screened_profit_margin": 0.032,
                                "estimated_exit_days_small_size": 0.5,
                            },
                            "filter_evaluation": {"passed": True, "rejection_reasons": []},
                        }
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
        (run_dir / "skipped_table.json").write_text(json.dumps([], ensure_ascii=False), encoding="utf-8")

    def test_load_and_derive_account_budget_context(self) -> None:
        self._write_account_snapshot(run_id="character-account-20260410T120000Z-abcdef12", wallet_balance_isk=5_500_000_000.0)
        snapshot = load_account_snapshot(
            db_path=self.db_path,
            runs_root=self.runs_root,
            account_snapshot_run_id=None,
        )
        context = derive_account_budget_context(snapshot)
        self.assertEqual(5_500_000_000.0, context["wallet_balance_isk"])
        self.assertEqual(2_000_000_000.0, context["open_buy_commitment_isk"])
        self.assertEqual(5_500_000_000.0, context["available_budget_isk"])
        self.assertEqual("account_wallet_balance", context["budget_source"])
        self.assertEqual("default_linked_character", context["selection_mode"])

    def test_build_analysis_context_for_linked_and_market_only_modes(self) -> None:
        linked_context = build_analysis_context(
            budget_source="account_wallet_balance",
            total_budget_isk=5_500_000_000.0,
            account_budget_context={
                "account_snapshot_run_id": "character-account-1",
                "character_id": 12345,
                "character_name": "Budget Pilot",
                "selection_mode": "default_linked_character",
                "linked_character_count": 2,
            },
            manual_budget_isk=None,
            scan_budget_isk=1_000_000_000.0,
        )
        self.assertEqual("linked_character_account", linked_context["context_mode"])
        self.assertTrue(linked_context["uses_character_account"])
        self.assertIn("Budget Pilot", linked_context["budget_source_summary_zh"])

        market_only_context = build_analysis_context(
            budget_source="scan_or_manual_budget",
            total_budget_isk=2_000_000_000.0,
            account_budget_context=None,
            manual_budget_isk=2_000_000_000.0,
            scan_budget_isk=1_000_000_000.0,
        )
        self.assertEqual("manual_budget_override", market_only_context["context_mode"])
        self.assertFalse(market_only_context["uses_character_account"])
        self.assertIn("未使用角色绑定数据", market_only_context["budget_source_summary_zh"])

    def test_market_advice_can_use_latest_account_budget(self) -> None:
        self._seed_scan(scan_run_id="scan-account-budget")
        self._write_account_snapshot(run_id="character-account-20260410T120000Z-abcdef12", wallet_balance_isk=5_500_000_000.0)
        with connect(self.db_path) as connection:
            advice = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id="scan-account-budget",
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
                use_latest_account_budget=True,
            )
        self.assertEqual(5_500_000_000.0, advice["budget_summary"]["total_budget_isk"])
        self.assertEqual("account_wallet_balance", advice["budget_summary"]["budget_source"])
        self.assertEqual("Budget Pilot", advice["account_budget_context"]["character_name"])
        self.assertEqual("linked_character_account", advice["analysis_context"]["context_mode"])
        self.assertEqual("角色绑定模式", advice["analysis_context"]["analysis_mode_label_zh"])

    def test_cli_plan_market_entry_accepts_latest_account_budget_flag(self) -> None:
        fake_plan = {"scan_summary": {"scan_run_id": "scan-entry-plan"}}
        stdout = io.StringIO()
        argv = [
            "eveq",
            "plan-market-entry",
            "--db-path",
            str(self.db_path),
            "--use-latest-account-budget",
            "--account-run-id",
            "character-account-20260410T120000Z-abcdef12",
        ]
        with patch("eveqtrading.cli.build_market_entry_plan", return_value=fake_plan) as mock_build:
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()
        self.assertEqual(0, exit_code)
        self.assertTrue(mock_build.call_args.kwargs["use_latest_account_budget"])
        self.assertEqual("character-account-20260410T120000Z-abcdef12", mock_build.call_args.kwargs["account_snapshot_run_id"])

    def test_cli_plan_market_entry_requires_a_budget_source(self) -> None:
        argv = [
            "eveq",
            "plan-market-entry",
            "--db-path",
            str(self.db_path),
        ]
        with patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as raised:
                cli_main()
        self.assertIn("requires --total-budget-isk or --use-latest-account-budget", str(raised.exception))
