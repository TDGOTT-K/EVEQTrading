from __future__ import annotations

import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.history_anchor_shock_backtest_report import build_history_anchor_shock_backtest_report
from eveqtrading.models import FeeConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class HistoryAnchorShockBacktestReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_backtest_finds_profitable_target_after_anchor_up_shock(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (10000002, "The Forge", "region", 10000002, None, 0, 0),
            )
            connection.executemany(
                """
                INSERT INTO item_catalog (
                    type_id, type_name, type_name_en, type_name_zh, group_id, category_id, market_group_id,
                    volume, packaged_volume, meta_level, market_group, is_published
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (60100, "锚矿", "Anchor Ore", "锚矿", None, None, None, None, None, None, "矿石", 1),
                    (50101, "受益工业品", "Beneficiary Item", "受益工业品", None, None, None, None, None, None, "模块", 1),
                    (50102, "稳定工业品", "Stable Item", "稳定工业品", None, None, None, None, None, None, "模块", 1),
                ],
            )

            anchor_price = 1_000.0
            beneficiary_price = 500.0
            stable_price = 700.0
            history_rows: list[tuple[object, ...]] = []
            start_day = date(2025, 5, 1)
            shock_offsets = {100, 120}
            beneficiary_jump_offsets = {102, 122}
            for offset in range(150):
                if offset in shock_offsets:
                    anchor_price *= 1.10
                if offset in beneficiary_jump_offsets:
                    beneficiary_price *= 1.10
                market_date = (start_day + timedelta(days=offset)).isoformat()
                history_rows.append(
                    (
                        60100,
                        10000002,
                        market_date,
                        anchor_price,
                        anchor_price,
                        anchor_price,
                        1000.0,
                        10,
                        "2026-04-12T00:00:00Z",
                        "test",
                        "ok",
                    )
                )
                history_rows.append(
                    (
                        50101,
                        10000002,
                        market_date,
                        beneficiary_price,
                        beneficiary_price,
                        beneficiary_price,
                        500_000.0,
                        10,
                        "2026-04-12T00:00:00Z",
                        "test",
                        "ok",
                    )
                )
                history_rows.append(
                    (
                        50102,
                        10000002,
                        market_date,
                        stable_price,
                        stable_price,
                        stable_price,
                        500_000.0,
                        10,
                        "2026-04-12T00:00:00Z",
                        "test",
                        "ok",
                    )
                )
            connection.executemany(
                """
                INSERT INTO market_history_daily (
                    type_id, region_id, market_date, average_price, highest_price, lowest_price,
                    volume, order_count, fetched_at, source, coverage_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                history_rows,
            )
            connection.commit()

            report = build_history_anchor_shock_backtest_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                anchor_type_id=60100,
                selected_targets=[
                    {"type_id": 50101, "type_name": "受益工业品", "selection_mode": "explicit"},
                    {"type_id": 50102, "type_name": "稳定工业品", "selection_mode": "explicit"},
                ],
                min_overlap_days=100,
                top_n=5,
                daily_shock_threshold=0.05,
                cumulative_3d_threshold=0.05,
                return_zscore_threshold=1.0,
                cooldown_days=5,
                entry_delay_days=1,
                holding_days_list=[1, 3],
                fee_config=FeeConfig(
                    buy_broker_fee_rate=0.0,
                    sell_broker_fee_rate=0.0,
                    sales_tax_rate=0.0,
                    relist_fee_rate=0.0,
                ),
            )

        self.assertEqual("锚矿", report["anchor_label"])
        self.assertEqual(2, report["universe_summary"]["selected_target_count"])
        self.assertEqual(2, report["universe_summary"]["qualified_target_count"])
        self.assertEqual(1, report["universe_summary"]["profitable_target_count"])
        self.assertEqual("受益工业品", report["profitable_rankings"][0]["target_label"])
        self.assertEqual(1, report["profitable_rankings"][0]["best_profitable_plan"]["holding_days"])
        self.assertGreater(report["profitable_rankings"][0]["best_profitable_plan"]["event_mean_net_return"], 0.09)
        self.assertAlmostEqual(
            0.0,
            report["response_rows"][1]["best_plan"]["event_mean_net_return"],
            places=6,
        )

        markdown_path = Path(report["files"]["markdown_path"])
        json_path = Path(report["files"]["json_path"])
        csv_path = Path(report["files"]["csv_path"])
        self.assertTrue(markdown_path.exists())
        self.assertTrue(json_path.exists())
        self.assertTrue(csv_path.exists())

        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 锚矿 冲击后屯货回测报告", markdown_text)
        self.assertIn("## 扣掉成本后仍有正收益的商品 Top", markdown_text)


if __name__ == "__main__":
    unittest.main()
