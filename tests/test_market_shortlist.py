from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.advisor import _position_plan_row
from eveqtrading.advisor import _resolve_risk_profile_config
from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.market_shortlist import build_market_shortlist


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class MarketShortlistTests(unittest.TestCase):
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
                    "阿尔法商品",
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
                            "type_name": "阿尔法商品",
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
                                "source_order_captured_at": "2026-04-10T10:00:00Z",
                                "history_latest_market_date": "2026-04-09",
                                "history_latest_fetched_at": "2026-04-10T10:02:00Z",
                                "window_end_30d": "2026-04-09",
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
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
        (run_dir / "feature_table.json").write_text(
            json.dumps([{"type_id": 3001, "type_name": "阿尔法商品"}], ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "skipped_table.json").write_text(
            json.dumps([], ensure_ascii=False),
            encoding="utf-8",
        )

    def test_build_market_shortlist_returns_chinese_rows_and_table(self) -> None:
        scan_run_id = "scan-shortlist"
        self._seed_scan(scan_run_id=scan_run_id)

        with connect(self.db_path) as connection:
            shortlist = build_market_shortlist(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                total_budget_isk=100_000_000_000.0,
                risk_profile="standard",
                top_opportunities=20,
                top_near_misses=5,
                max_rows=10,
                report_profile="jita-passive",
            )

        self.assertEqual(scan_run_id, shortlist["scan_summary"]["scan_run_id"])
        self.assertIn("本次短名单展示 1 个商品", shortlist["headline"])
        self.assertEqual(1, shortlist["displayed_row_count"])
        self.assertEqual(1, shortlist["selected_row_count_considered"])
        self.assertEqual(1, len(shortlist["shortlist_rows"]))
        self.assertEqual("阿尔法商品", shortlist["shortlist_rows"][0]["type_name"])
        self.assertEqual("立即执行", shortlist["shortlist_rows_zh"][0]["状态"])
        self.assertEqual("9.20亿", shortlist["shortlist_rows_zh"][0]["基础建议上限"])
        self.assertEqual("18.40亿", shortlist["shortlist_rows_zh"][0]["当前仓位上限"])
        self.assertEqual("无", shortlist["shortlist_rows_zh"][0]["卖价高悬"])
        self.assertIn("| 商品 |", shortlist["rendered_table_md"])
        self.assertIn("| 阿尔法商品 |", shortlist["rendered_table_md"])
        self.assertIn("商品", shortlist["rendered_table_tsv"])
        self.assertIn("阿尔法商品", shortlist["rendered_table_tsv"])
        comparison = shortlist["position_sizing_comparison"]
        self.assertEqual(["conservative", "standard", "aggressive"], [row["risk_profile"] for row in comparison])
        self.assertLess(float(comparison[0]["allocated_capital_isk"] or 0.0), float(comparison[1]["allocated_capital_isk"] or 0.0))
        self.assertLess(float(comparison[1]["allocated_capital_isk"] or 0.0), float(comparison[2]["allocated_capital_isk"] or 0.0))
        self.assertIn("| 保守 |", shortlist["position_sizing_comparison_md"])
        self.assertIn("| 标准 |", shortlist["position_sizing_comparison_md"])
        self.assertIn("| 激进 |", shortlist["position_sizing_comparison_md"])
        self.assertEqual("manual_budget_override", shortlist["analysis_context"]["context_mode"])
        self.assertEqual("2026-04-10T10:00:00Z", shortlist["data_freshness"]["order_data_time_basis"]["snapshot_time_max"])
        self.assertEqual("2026-04-09", shortlist["data_freshness"]["history_data_time_basis"]["window_end_date_max"])
        self.assertEqual("enforced", shortlist["semantic_guard"]["semantic_guard_status"])
        self.assertIn("吉他站内被动挂单扫描结果", "\n".join(shortlist["semantic_guard"]["guard_notes_zh"]))

    def test_position_plan_marks_item_unaffordable_for_one_unit_as_watch_only(self) -> None:
        plan, remaining_budget = _position_plan_row(
            advice_row={
                "action": "deploy-small",
                "capital_required": 920_000_000.0,
                "tradable_units": 0.1,
                "recommended_deployable_isk": 920_000_000.0,
                "theoretical_market_capacity_isk": 4_200_000_000.0,
                "confidence_score": 85,
                "metrics": {
                    "avg_daily_isk_volume_30d": 12_000_000_000.0,
                },
            },
            total_budget_isk=50_000_000_000.0,
            remaining_budget_isk=50_000_000_000.0,
            priority_rank=1,
            risk_profile=_resolve_risk_profile_config("standard"),
        )

        self.assertEqual("watch_only", plan["status"])
        self.assertEqual(0, plan["suggested_units"])
        self.assertEqual(0.0, plan["suggested_capital_isk"])
        self.assertIn("cannot_afford_one_unit", plan["binding_constraints"])
        self.assertEqual(50_000_000_000.0, remaining_budget)

    def test_build_market_shortlist_rejects_non_jita_scan_for_jita_profile(self) -> None:
        scan_run_id = "scan-shortlist"
        self._seed_scan(scan_run_id=scan_run_id)
        with connect(self.db_path) as connection:
            connection.execute(
                "UPDATE scan_run SET strategy_set = ? WHERE scan_run_id = ?",
                ("v1-default", scan_run_id),
            )
            connection.commit()

        with connect(self.db_path) as connection:
            with self.assertRaisesRegex(ValueError, "requires scan strategy_set"):
                build_market_shortlist(
                    connection,
                    db_path=self.db_path,
                    runs_root=self.runs_root,
                    log_dir=self.log_dir,
                    scan_run_id=scan_run_id,
                    prefer_latest_selected=False,
                    total_budget_isk=100_000_000_000.0,
                    risk_profile="standard",
                    top_opportunities=20,
                    top_near_misses=5,
                    max_rows=10,
                    report_profile="jita-passive",
                )

    def test_cli_list_market_shortlist_writes_markdown_artifact(self) -> None:
        fake_shortlist = {
            "scan_summary": {"scan_run_id": "scan-shortlist"},
            "semantic_guard": {
                "report_profile_label_zh": "通用市场短名单",
                "strategy_set_label_zh": "jita-passive-simple",
                "semantic_guard_status": "not_enforced",
                "guard_notes_zh": ["本报告未启用吉他站内专用语义护栏。"],
            },
            "headline": "本次短名单展示 1 个商品，来自本轮评估的 1 个候选；其中 1 个可立即执行，0 个先观察或延后。",
            "shortlist_generated_at_utc": "2026-04-10T12:00:00Z",
            "budget_summary": {
                "risk_profile": "standard",
                "deployable_cap_multiplier": 2.0,
                "total_budget_isk": 5_000_000_000.0,
                "allocated_capital_isk": 100_000_000.0,
                "remaining_budget_isk": 4_900_000_000.0,
                "execute_now_count": 1,
                "deferred_count": 0,
            },
            "analysis_context": {
                "analysis_mode_label_zh": "纯市场模式",
                "budget_source_label_zh": "手动预算",
                "budget_source_summary_zh": "本轮未使用角色绑定数据，预算采用手动输入 5000000000.00 ISK。",
                "character_name": None,
                "selection_mode_label_zh": None,
            },
            "data_freshness": {
                "summary_zh": "订单大致基于 2026-04-10T10:00:00Z 到 2026-04-10T10:00:00Z 的快照，history 30天窗口截至 2026-04-09，最近一次 history 抓取时间约为 2026-04-10T10:02:00Z。",
                "analysis_as_of_time": "2026-04-10T10:00:00Z",
                "order_data_time_basis": {
                    "snapshot_time_min": "2026-04-10T10:00:00Z",
                    "snapshot_time_max": "2026-04-10T10:00:00Z",
                },
                "history_data_time_basis": {
                    "window_end_date_max": "2026-04-09",
                    "latest_fetched_at_max": "2026-04-10T10:02:00Z",
                },
            },
            "advice_basis": {
                "credibility_tier": "trace-complete",
                "basis": "analysis_run + calc_trace + candidate_table",
            },
            "shortlist_rows_zh": [{"商品": "阿尔法商品"}],
            "position_sizing_comparison_md": "| 档位 |\n| --- |\n| 标准 |",
            "rendered_table_md": "| 商品 |\n| --- |\n| 阿尔法商品 |",
            "rendered_table_tsv": "商品\n阿尔法商品",
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "list-market-shortlist",
            "--db-path",
            str(self.db_path),
            "--total-budget-isk",
            "5000000000",
        ]
        with patch("eveqtrading.cli.build_market_shortlist", return_value=fake_shortlist) as mock_build:
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual(5_000_000_000.0, mock_build.call_args.kwargs["total_budget_isk"])
        output_lines = stdout.getvalue().strip().splitlines()
        self.assertTrue(output_lines[0].startswith("Market shortlist ready:"))
        self.assertTrue(output_lines[1].startswith("Output dir: "))
        self.assertTrue(output_lines[2].startswith("Markdown: "))
        output_dir = Path(output_lines[1].split(": ", 1)[1])
        markdown_path = Path(output_lines[2].split(": ", 1)[1])
        self.assertTrue(output_dir.exists())
        self.assertTrue(markdown_path.exists())
        self.assertEqual(output_dir, markdown_path.parent)
        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 市场短名单", markdown_text)
        self.assertIn("## 数据时间基准", markdown_text)
        self.assertIn("History 窗口截止日期: 2026-04-09", markdown_text)
        self.assertIn("## 报告语义", markdown_text)
        self.assertIn("## 分析上下文", markdown_text)
        self.assertIn("预算来源: 手动预算", markdown_text)
        self.assertIn("| 商品 |", markdown_text)
        self.assertIn("| 档位 |", markdown_text)
        self.assertNotIn('"scan_summary"', stdout.getvalue())

    def test_cli_list_jita_passive_shortlist_writes_guarded_artifact(self) -> None:
        fake_shortlist = {
            "scan_summary": {"scan_run_id": "jita-passive-simple-1"},
            "semantic_guard": {
                "report_profile_label_zh": "吉他站内挂买挂卖差价报告",
                "strategy_set_label_zh": "吉他站内被动挂单简版扫描",
                "semantic_guard_status": "enforced",
                "guard_notes_zh": [
                    "这份报告只接受吉他站内被动挂单扫描结果。",
                    "每一行都要求买卖地点同站，且入场方式必须是挂买单收货、出场方式必须是挂卖单出货。",
                ],
            },
            "headline": "本次短名单展示 1 个商品，来自本轮评估的 1 个候选；其中 1 个可立即执行，0 个先观察或延后。",
            "shortlist_generated_at_utc": "2026-04-10T12:00:00Z",
            "budget_summary": {
                "risk_profile": "standard",
                "deployable_cap_multiplier": 2.0,
                "total_budget_isk": 5_000_000_000.0,
                "allocated_capital_isk": 100_000_000.0,
                "remaining_budget_isk": 4_900_000_000.0,
                "execute_now_count": 1,
                "deferred_count": 0,
            },
            "analysis_context": {
                "analysis_mode_label_zh": "纯市场模式",
                "budget_source_label_zh": "手动预算",
                "budget_source_summary_zh": "本轮未使用角色绑定数据，预算采用手动输入 5000000000.00 ISK。",
                "character_name": None,
                "selection_mode_label_zh": None,
            },
            "data_freshness": {
                "summary_zh": "订单大致基于 2026-04-10T10:00:00Z 到 2026-04-10T10:00:00Z 的快照，history 30天窗口截至 2026-04-09，最近一次 history 抓取时间约为 2026-04-10T10:02:00Z。",
                "analysis_as_of_time": "2026-04-10T10:00:00Z",
                "order_data_time_basis": {
                    "snapshot_time_min": "2026-04-10T10:00:00Z",
                    "snapshot_time_max": "2026-04-10T10:00:00Z",
                },
                "history_data_time_basis": {
                    "window_end_date_max": "2026-04-09",
                    "latest_fetched_at_max": "2026-04-10T10:02:00Z",
                },
            },
            "advice_basis": {
                "credibility_tier": "trace-complete",
                "basis": "analysis_run + calc_trace + candidate_table",
            },
            "shortlist_rows_zh": [{"商品": "阿尔法商品"}],
            "position_sizing_comparison_md": "| 档位 |\n| --- |\n| 标准 |",
            "rendered_table_md": "| 商品 |\n| --- |\n| 阿尔法商品 |",
            "rendered_table_tsv": "商品\n阿尔法商品",
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "list-jita-passive-shortlist",
            "--db-path",
            str(self.db_path),
            "--total-budget-isk",
            "5000000000",
        ]
        with patch("eveqtrading.cli.build_market_shortlist", return_value=fake_shortlist) as mock_build:
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual("jita-passive", mock_build.call_args.kwargs["report_profile"])
        output_lines = stdout.getvalue().strip().splitlines()
        self.assertTrue(output_lines[0].startswith("Jita passive shortlist ready:"))
        markdown_path = Path(output_lines[2].split(": ", 1)[1])
        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("报告类型: 吉他站内挂买挂卖差价报告", markdown_text)
        self.assertIn("语义护栏状态: enforced", markdown_text)


if __name__ == "__main__":
    unittest.main()
