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
from eveqtrading.db import connect, init_db, record_data_fetch_run
from eveqtrading.jita_passive_cache_brief import _build_operator_action_plan
from eveqtrading.jita_passive_cache_brief import build_jita_passive_cache_brief


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveCacheBriefTests(unittest.TestCase):
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

    def _seed_scan(self, *, connection, scan_run_id: str, selected_candidate_count: int) -> None:
        run_dir = self.runs_root / scan_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
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
                "2026-04-07T12:00:00Z",
                "2026-04-07T12:05:00Z",
                "region:10000002",
                "jita-passive-simple",
                1_000_000_000.0,
                "v1-jita-passive-simple",
                str(self.base_dir / "inputs" / f"{scan_run_id}.json"),
                str(run_dir),
                selected_candidate_count,
                "completed",
            ),
        )
        if selected_candidate_count > 0:
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
                    "buy",
                    "sell",
                    1500.0,
                    15_000_000.0,
                    0.022,
                    280_000_000.0,
                    10_000.0,
                    0.7,
                    0.11,
                    "stable two-sided turnover and margin",
                ),
            )

        (run_dir / "analysis_run.json").write_text(
            json.dumps({"scan_run_id": scan_run_id, "strategy_set": "jita-passive-simple"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "candidate_table.json").write_text(
            json.dumps(
                [
                    {
                        "opportunity_id": "3001-10000002-jita-passive-simple",
                        "buy_location_name": "Jita IV - Moon 4",
                        "sell_location_name": "Jita IV - Moon 4",
                    }
                ]
                if selected_candidate_count > 0
                else [],
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
                                "avg_daily_isk_volume_30d": 220_000_000.0,
                                "active_days_ratio_30d": 0.97,
                                "volatility_30d": 0.11,
                            "source_order_snapshot_id": "orders-alpha",
                            "source_order_captured_at": "2026-04-07T12:04:00Z",
                            "history_latest_market_date": "2026-04-06",
                            "history_latest_fetched_at": "2026-04-07T12:06:00Z",
                            "window_end_30d": "2026-04-06",
                        },
                            "derived": {
                                "screened_profit_margin": 0.022,
                                "estimated_exit_days_small_size": 0.7,
                            },
                            "filter_evaluation": {
                                "passed": selected_candidate_count > 0,
                                "rejection_reasons": [] if selected_candidate_count > 0 else ["profit margin below threshold"],
                            },
                        },
                        {
                            "type_id": 3002,
                            "type_name": "Beta Item",
                            "strategy_pool": "jita-passive-simple",
                            "source_record": {
                                "opportunity_id": "3002-10000002-jita-passive-simple",
                                "type_id": 3002,
                                "avg_daily_isk_volume_30d": 150_000_000.0,
                                "active_days_ratio_30d": 0.91,
                                "volatility_30d": 0.18,
                            },
                            "derived": {
                                "screened_profit_margin": 0.009,
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
            json.dumps([{"type_id": 3999, "type_name": "Skipped Item", "reason": "missing_history"}], ensure_ascii=False),
            encoding="utf-8",
        )

    def _seed_orders_fetch_mode_artifacts(
        self,
        *,
        cycle_run_id: str,
        requested_fetch_mode: str = "auto",
        effective_fetch_mode: str = "per_type",
        decision_reason: str = "target_set_small_or_predicted_pages_low",
    ) -> None:
        refresh_run_dir = self.runs_root / f"refresh-{cycle_run_id}"
        batch_run_dir = self.runs_root / f"orders-batch-{cycle_run_id}"
        orders_run_dir = self.runs_root / f"orders-fetch-{cycle_run_id}"
        refresh_run_dir.mkdir(parents=True, exist_ok=True)
        batch_run_dir.mkdir(parents=True, exist_ok=True)
        orders_run_dir.mkdir(parents=True, exist_ok=True)
        (refresh_run_dir / "order_refresh_cycle.json").write_text(
            json.dumps(
                {
                    "cycle_run_id": f"refresh-{cycle_run_id}",
                    "orders_batch_run_id": f"orders-batch-{cycle_run_id}",
                    "orders_batch_run_dir": str(batch_run_dir),
                    "config": {"orders_fetch": {"fetch_mode": requested_fetch_mode}},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (batch_run_dir / "orders_batch_fetch.json").write_text(
            json.dumps(
                {
                    "batch_run_id": f"orders-batch-{cycle_run_id}",
                    "chunks": [
                        {
                            "chunk_index": 1,
                            "target_count": 5,
                            "type_ids": [3001, 3002, 3003, 3004, 3005],
                            "orders_run_id": f"orders-fetch-{cycle_run_id}",
                            "orders_run_dir": str(orders_run_dir),
                            "fetch_counts": {"success": 5, "pages_fetched": 5},
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (orders_run_dir / "orders_fetch_run.json").write_text(
            json.dumps(
                {
                    "run_id": f"orders-fetch-{cycle_run_id}",
                    "config": {"fetch_mode": requested_fetch_mode},
                    "effective_fetch_mode": effective_fetch_mode,
                    "fetch_mode_decision": {
                        "requested_fetch_mode": requested_fetch_mode,
                        "effective_fetch_mode": effective_fetch_mode,
                        "reason": decision_reason,
                        "region_first_page_probe_used": requested_fetch_mode == "auto",
                        "predicted_per_type_pages": 5,
                        "region_total_pages": 12 if effective_fetch_mode == "per_type" else 3,
                    },
                    "type_count_requested": 5,
                    "type_count_success": 5,
                    "pages_fetched": 5,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (orders_run_dir / "orders_fetch_trace.json").write_text(
            json.dumps(
                {
                    "run_id": f"orders-fetch-{cycle_run_id}",
                    "effective_fetch_mode": effective_fetch_mode,
                    "fetch_mode_decision": {
                        "requested_fetch_mode": requested_fetch_mode,
                        "effective_fetch_mode": effective_fetch_mode,
                        "reason": decision_reason,
                        "region_first_page_probe_used": requested_fetch_mode == "auto",
                        "predicted_per_type_pages": 5,
                        "region_total_pages": 12 if effective_fetch_mode == "per_type" else 3,
                    },
                    "shared_remote_trace": (
                        {"request_scope": "region_pages"}
                        if effective_fetch_mode == "region_pages"
                        else None
                    ),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _seed_cycle(
        self,
        *,
        connection,
        cycle_run_id: str,
        started_at: str,
        scan_run_id: str,
        candidate_count_selected: int,
    ) -> None:
        run_dir = self.runs_root / cycle_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        refresh_run_dir = self.runs_root / f"refresh-{cycle_run_id}"
        batch_run_dir = self.runs_root / f"orders-batch-{cycle_run_id}"
        (run_dir / "summary.md").write_text("# cache cycle\n", encoding="utf-8")
        payload = {
            "cycle_run_id": cycle_run_id,
            "started_at_utc": started_at,
            "finished_at_utc": "2026-04-07T12:10:00Z",
            "duration_seconds": 600.0,
            "region_id": 10000002,
            "config": {"orders_cache_profile": "warm"},
            "after_order_cache_counts": {"cache_hit_count": 42},
            "order_refresh_cycle_run_id": f"refresh-{cycle_run_id}",
            "order_refresh_cycle_run_dir": str(refresh_run_dir),
            "scan_run_id": scan_run_id,
            "scan_summary": {
                "candidate_count_built": 20,
                "candidate_count_selected": candidate_count_selected,
                "skipped_count": 1,
            },
            "order_refresh_summary": {
                "selected_remote_target_count": 5,
                "executed_remote_target_count": 5,
                "orders_batch_run_id": f"orders-batch-{cycle_run_id}",
                "orders_batch_run_dir": str(batch_run_dir),
                "config": {"orders_fetch": {"fetch_mode": "auto"}},
            },
            "top_candidates_preview": [{"type_id": 3001, "type_name": "Alpha Item"}],
            "files": {
                "order_refresh_cycle_summary_path": str(run_dir / "order-refresh-summary.md"),
                "scan_summary_path": str(self.runs_root / scan_run_id / "summary.md"),
                "scan_analysis_run_path": str(self.runs_root / scan_run_id / "analysis_run.json"),
                "scan_calc_trace_path": str(self.runs_root / scan_run_id / "calc_trace.json"),
                "scan_candidate_table_path": str(self.runs_root / scan_run_id / "candidate_table.json"),
                "scan_skipped_table_path": str(self.runs_root / scan_run_id / "skipped_table.json"),
            },
        }
        (run_dir / "jita_passive_cache_cycle.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        record_data_fetch_run(
            connection,
            fetch_run_id=cycle_run_id,
            fetch_kind="jita_passive_cache_cycle",
            source="jita-passive-cache-cycle",
            output_dir=run_dir,
            scope={"region_id": 10000002},
            details={"scan_summary": payload["scan_summary"]},
            started_at=started_at,
            finished_at="2026-04-07T12:10:00Z",
            status="completed",
        )

    def test_cache_brief_combines_report_diagnosis_and_advice(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_scan(connection=connection, scan_run_id="scan-alpha", selected_candidate_count=1)
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-a")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-a",
                started_at="2026-04-07T12:00:00Z",
                scan_run_id="scan-alpha",
                candidate_count_selected=1,
            )
            connection.commit()
            brief = build_jita_passive_cache_brief(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                cycle_run_id=None,
                prefer_latest_selected=False,
                top_candidates=5,
                top_opportunities=5,
                top_near_misses=5,
                top_rejection_reasons=5,
                top_near_passes=5,
                top_skipped_reasons=5,
            )

        self.assertEqual("jita-passive-cache-cycle-a", brief["cache_cycle_summary"]["cycle_run_id"])
        self.assertEqual("scan-alpha", brief["scan_summary"]["scan_run_id"])
        self.assertEqual("standard", brief["operator_summary"]["risk_profile"])
        self.assertEqual("Standard", brief["operator_summary"]["risk_profile_label"])
        self.assertEqual("review_deploy_small_candidates", brief["operator_summary"]["recommended_focus"])
        self.assertEqual("纯市场模式", brief["operator_summary"]["analysis_mode_label_zh"])
        self.assertEqual("扫描预算", brief["operator_summary"]["budget_source_label_zh"])
        self.assertIn("订单", brief["operator_summary"]["data_time_basis_summary_zh"])
        self.assertEqual("auto", brief["operator_summary"]["orders_requested_fetch_mode"])
        self.assertEqual("per_type", brief["operator_summary"]["orders_effective_fetch_mode"])
        self.assertIn("small/light", brief["operator_summary"]["orders_fetch_mode_narrative"])
        self.assertEqual("per_type", brief["orders_fetch_mode_summary"]["dominant_effective_fetch_mode"])
        self.assertEqual("deploy-small", brief["selected_actions_preview"][0]["action"])
        self.assertIsNotNone(brief["selected_actions_preview"][0]["position_plan"])
        self.assertEqual("allocate", brief["selected_actions_preview"][0]["position_plan"]["status"])
        self.assertEqual("Alpha Item", brief["top_candidates_preview"][0]["type_name"])
        self.assertGreater(brief["budget_summary"]["allocated_capital_isk"], 0.0)
        self.assertGreaterEqual(brief["operator_summary"]["allocated_capital_isk"], 0.0)
        self.assertEqual("scan_budget", brief["analysis_context"]["context_mode"])
        self.assertEqual("2026-04-07T12:04:00Z", brief["data_freshness"]["order_data_time_basis"]["snapshot_time_max"])
        self.assertIn("Alpha Item", brief["operator_narrative"]["executive_summary"])
        self.assertIn("Deploy-now shortlist", brief["operator_narrative"]["deployment_summary"])
        self.assertIn("liquidity_cap", brief["operator_narrative"]["constraint_summary"])
        self.assertIn("not requested", brief["operator_narrative"]["risk_comparison_summary"])
        self.assertEqual(1, len(brief["operator_action_plan"]["execute_now"]))
        self.assertEqual("Alpha Item", brief["operator_action_plan"]["execute_now"][0]["type_name"])
        self.assertEqual("prepare_entry_order", brief["operator_action_plan"]["execute_now"][0]["recommended_step"])
        self.assertEqual("execute_now", brief["execution_plan_preview"][0]["execution_status"])
        self.assertIn("Alpha Item", brief["operator_summary"]["next_action_summary"])
        self.assertEqual("profit margin below threshold", brief["top_rejection_reasons"][0]["reason"])
        self.assertEqual("missing_history", brief["skipped_summary"]["reason_counts"][0]["reason"])

    def test_cache_brief_can_prefer_latest_cycle_with_selected_candidates(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_scan(connection=connection, scan_run_id="scan-old", selected_candidate_count=1)
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-old")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-old",
                started_at="2026-04-07T11:00:00Z",
                scan_run_id="scan-old",
                candidate_count_selected=1,
            )
            self._seed_scan(connection=connection, scan_run_id="scan-new", selected_candidate_count=0)
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-new")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-new",
                started_at="2026-04-07T12:00:00Z",
                scan_run_id="scan-new",
                candidate_count_selected=0,
            )
            connection.commit()
            brief = build_jita_passive_cache_brief(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                cycle_run_id=None,
                prefer_latest_selected=True,
                top_candidates=5,
                top_opportunities=5,
                top_near_misses=5,
                top_rejection_reasons=5,
                top_near_passes=5,
                top_skipped_reasons=5,
            )

        self.assertEqual("jita-passive-cache-cycle-old", brief["cache_cycle_summary"]["cycle_run_id"])
        self.assertEqual("scan-old", brief["scan_summary"]["scan_run_id"])

    def test_cache_brief_can_compare_risk_profiles(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_scan(connection=connection, scan_run_id="scan-compare", selected_candidate_count=1)
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-compare")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-compare",
                started_at="2026-04-07T13:00:00Z",
                scan_run_id="scan-compare",
                candidate_count_selected=1,
            )
            connection.commit()
            brief = build_jita_passive_cache_brief(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                cycle_run_id=None,
                prefer_latest_selected=False,
                top_candidates=5,
                top_opportunities=5,
                top_near_misses=5,
                top_rejection_reasons=5,
                top_near_passes=5,
                top_skipped_reasons=5,
                risk_profile="standard",
                compare_risk_profiles=True,
            )

        self.assertTrue(brief["operator_summary"]["risk_profile_comparison_available"])
        comparison = brief["risk_profile_comparison"]
        self.assertEqual(["conservative", "standard", "aggressive"], [row["risk_profile"] for row in comparison])
        conservative_allocated = comparison[0]["allocated_capital_isk"]
        standard_allocated = comparison[1]["allocated_capital_isk"]
        aggressive_allocated = comparison[2]["allocated_capital_isk"]
        self.assertLess(conservative_allocated, standard_allocated)
        self.assertLess(standard_allocated, aggressive_allocated)
        allocation_preview = brief["allocation_comparison_preview"][0]
        self.assertEqual("Alpha Item", allocation_preview["type_name"])
        self.assertLess(
            allocation_preview["per_profile"]["conservative"]["suggested_capital_isk"],
            allocation_preview["per_profile"]["standard"]["suggested_capital_isk"],
        )
        self.assertLess(
            allocation_preview["per_profile"]["standard"]["suggested_capital_isk"],
            allocation_preview["per_profile"]["aggressive"]["suggested_capital_isk"],
        )
        self.assertIn("conservative", brief["operator_narrative"]["risk_comparison_summary"])
        self.assertIn("aggressive", brief["operator_narrative"]["risk_comparison_summary"])
        self.assertIn("Alpha Item", brief["operator_narrative"]["risk_comparison_summary"])
        self.assertIn("liquidity cap", brief["operator_narrative"]["risk_comparison_summary"])

    def test_build_operator_action_plan_categorizes_execute_defer_watch_and_near_pass(self) -> None:
        action_plan = _build_operator_action_plan(
            execution_plan=[
                {
                    "priority_rank": 1,
                    "execution_status": "execute_now",
                    "position_status": "allocate",
                    "type_id": 3001,
                    "type_name": "Alpha Item",
                    "strategy_type": "jita-passive-simple",
                    "suggested_capital_isk": 36_000_000.0,
                    "suggested_units": 1200,
                    "binding_constraints": ["liquidity_cap"],
                },
                {
                    "priority_rank": 2,
                    "execution_status": "defer",
                    "position_status": "budget_exhausted",
                    "type_id": 3002,
                    "type_name": "Beta Item",
                    "strategy_type": "jita-passive-simple",
                    "suggested_capital_isk": 0.0,
                    "suggested_units": 0,
                    "binding_constraints": ["remaining_budget"],
                },
            ],
            selected_actions_preview=[
                {
                    "type_id": 3003,
                    "type_name": "Gamma Item",
                    "strategy_type": "jita-passive-simple",
                    "action": "watch",
                    "action_rationale": "passes the hard floor but still has model caveats",
                    "position_plan": {"status": "watch_only"},
                }
            ],
            near_miss_watchlist=[
                {
                    "type_id": 3004,
                    "type_name": "Delta Item",
                    "strategy_type": "jita-passive-simple",
                    "hard_blockers": ["profit margin below threshold"],
                    "soft_blockers": ["turnover slightly slow"],
                }
            ],
            execute_limit=5,
            deferred_limit=5,
            watch_limit=5,
            near_pass_limit=5,
        )

        self.assertIn("1 execute now, 1 deferred", action_plan["summary"])
        self.assertEqual("Alpha Item", action_plan["execute_now"][0]["type_name"])
        self.assertEqual("Beta Item", action_plan["deferred"][0]["type_name"])
        self.assertEqual("hold_for_next_cycle", action_plan["deferred"][0]["recommended_step"])
        self.assertIn("budget is already exhausted", action_plan["deferred"][0]["reason"])
        self.assertEqual("Gamma Item", action_plan["watchlist"][0]["type_name"])
        self.assertEqual("monitor_only", action_plan["watchlist"][0]["recommended_step"])
        self.assertEqual("Delta Item", action_plan["near_pass_watchlist"][0]["type_name"])
        self.assertEqual("recheck_next_refresh", action_plan["near_pass_watchlist"][0]["recommended_step"])

    def test_cli_brief_jita_passive_cache_prints_payload(self) -> None:
        fake_brief = {
            "scan_summary": {"scan_run_id": "scan-alpha"},
            "operator_summary": {"headline": "1 selected from 20 built; 1 deploy-small and 0 watch."},
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "brief-jita-passive-cache",
            "--db-path",
            str(self.db_path),
        ]
        with patch("eveqtrading.cli.build_jita_passive_cache_brief", return_value=fake_brief):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive cache brief ready:"))
        payload = json.loads("\n".join(output_lines[1:]))
        self.assertEqual("scan-alpha", payload["scan_summary"]["scan_run_id"])
        self.assertIn("1 deploy-small", payload["operator_summary"]["headline"])

    def test_cli_brief_jita_passive_cache_passes_risk_profile(self) -> None:
        stdout = io.StringIO()
        argv = [
            "eveq",
            "brief-jita-passive-cache",
            "--db-path",
            str(self.db_path),
            "--risk-profile",
            "conservative",
        ]
        with patch("eveqtrading.cli.build_jita_passive_cache_brief", return_value={"scan_summary": {"scan_run_id": "scan-alpha"}, "operator_summary": {"headline": "ok"}}) as mock_build:
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual("conservative", mock_build.call_args.kwargs["risk_profile"])

    def test_cli_brief_jita_passive_cache_passes_compare_flag(self) -> None:
        stdout = io.StringIO()
        argv = [
            "eveq",
            "brief-jita-passive-cache",
            "--db-path",
            str(self.db_path),
            "--compare-risk-profiles",
        ]
        with patch("eveqtrading.cli.build_jita_passive_cache_brief", return_value={"scan_summary": {"scan_run_id": "scan-alpha"}, "operator_summary": {"headline": "ok"}}) as mock_build:
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertTrue(mock_build.call_args.kwargs["compare_risk_profiles"])


if __name__ == "__main__":
    unittest.main()
