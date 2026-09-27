from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.history_anchor_relationship_report import build_history_anchor_relationship_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class HistoryAnchorRelationshipReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_build_history_anchor_relationship_report_ranks_positive_and_negative_relationships(self) -> None:
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
                INSERT INTO market_group_catalog (
                    market_group_id, parent_group_id, market_group_name, market_group_name_en, market_group_name_zh
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (10, None, "装备", "Equipment", "装备"),
                    (11, 10, "模块", "Modules", "模块"),
                    (20, None, "舰船", "Ships", "舰船"),
                    (21, 20, "战舰", "Warships", "战舰"),
                    (30, None, "矿物", "Materials", "矿物"),
                    (31, 30, "基础矿", "Ore", "基础矿"),
                ],
            )
            connection.executemany(
                """
                INSERT INTO item_catalog (
                    type_id, type_name, type_name_en, type_name_zh, group_id, category_id, market_group_id,
                    volume, packaged_volume, meta_level, market_group, is_published
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (44992, "伊甸币", "PLEX", "伊甸币", None, None, None, None, None, None, "PLEX", 1),
                    (50101, "正相关物品", "Positive Item", "正相关物品", None, None, 11, None, None, None, "模块", 1),
                    (50102, "负相关物品", "Negative Item", "负相关物品", None, None, 21, None, None, None, "战舰", 1),
                    (50103, "样本不足物品", "Short Sample Item", "样本不足物品", None, None, 31, None, None, None, "基础矿", 1),
                ],
            )

            history_rows: list[tuple[object, ...]] = []
            start_day = date(2025, 6, 15)
            for offset in range(40):
                market_date = (start_day + timedelta(days=offset)).isoformat()
                anchor_price = 5_000_000.0 + offset * 10_000.0
                anchor_volume = 1000.0 + offset * 5.0
                anchor_region_id = 10000002 if market_date < "2025-07-07" else 19000001
                history_rows.append(
                    (
                        44992,
                        anchor_region_id,
                        market_date,
                        anchor_price,
                        anchor_price * 1.01,
                        anchor_price * 0.99,
                        anchor_volume,
                        100 + offset,
                        "2026-04-11T00:00:00Z",
                        "test",
                        "ok",
                    )
                )
                positive_price = anchor_price * 2.0
                positive_volume = anchor_volume * 3.0
                history_rows.append(
                    (
                        50101,
                        10000002,
                        market_date,
                        positive_price,
                        positive_price * 1.01,
                        positive_price * 0.99,
                        positive_volume,
                        200 + offset,
                        "2026-04-11T00:00:00Z",
                        "test",
                        "ok",
                    )
                )
                negative_price = 20_000_000.0 - offset * 20_000.0
                negative_volume = 10_000.0 - offset * 7.0
                history_rows.append(
                    (
                        50102,
                        10000002,
                        market_date,
                        negative_price,
                        negative_price * 1.01,
                        negative_price * 0.99,
                        negative_volume,
                        300 + offset,
                        "2026-04-11T00:00:00Z",
                        "test",
                        "ok",
                    )
                )
                if offset < 10:
                    short_price = 7_000_000.0 + offset * 1_000.0
                    short_volume = 500.0 + offset
                    history_rows.append(
                        (
                            50103,
                            10000002,
                            market_date,
                            short_price,
                            short_price * 1.01,
                            short_price * 0.99,
                            short_volume,
                            50 + offset,
                            "2026-04-11T00:00:00Z",
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

            report = build_history_anchor_relationship_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                anchor_type_id=44992,
                selected_targets=[
                    {"type_id": 50101, "type_name": "正相关物品", "selection_mode": "explicit"},
                    {"type_id": 50102, "type_name": "负相关物品", "selection_mode": "explicit"},
                    {"type_id": 50103, "type_name": "样本不足物品", "selection_mode": "explicit"},
                ],
                min_overlap_days=30,
                top_n=2,
            )

        self.assertEqual("伊甸币", report["anchor_label"])
        self.assertTrue(report["anchor_history_source"]["is_stitched"])
        self.assertEqual(3, report["universe_summary"]["selected_target_count"])
        self.assertEqual(2, report["universe_summary"]["qualified_target_count"])
        self.assertEqual(2, report["universe_summary"]["qualified_market_group_count"])
        self.assertEqual("正相关物品", report["rankings"]["price_positive"][0]["target_label"])
        self.assertEqual("负相关物品", report["rankings"]["price_negative"][0]["target_label"])
        self.assertGreater(report["rankings"]["price_positive"][0]["price_pearson_corr"], 0.99)
        self.assertLess(report["rankings"]["price_negative"][0]["price_pearson_corr"], -0.99)
        self.assertEqual("装备 > 模块", report["group_rankings"]["price_to_turnover_positive"][0]["market_group_path"])
        self.assertEqual("舰船 > 战舰", report["group_rankings"]["price_to_turnover_negative"][0]["market_group_path"])

        markdown_path = Path(report["files"]["markdown_path"])
        json_path = Path(report["files"]["json_path"])
        csv_path = Path(report["files"]["csv_path"])
        group_csv_path = Path(report["files"]["group_csv_path"])
        self.assertTrue(markdown_path.exists())
        self.assertTrue(json_path.exists())
        self.assertTrue(csv_path.exists())
        self.assertTrue(group_csv_path.exists())

        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 伊甸币 锚点市场关系扫描报告", markdown_text)
        self.assertIn("## 价格正相关 Top", markdown_text)
        self.assertIn("## 成交量负相关 Top", markdown_text)
        self.assertIn("## 市场组关系榜单", markdown_text)
        self.assertIn("## 锚点价格 vs 市场组日成交额 正相关 Top", markdown_text)

        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(2, len(rows))
        self.assertEqual("50101", rows[0]["target_type_id"])

        with group_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            group_rows = list(csv.DictReader(handle))
        self.assertEqual(2, len(group_rows))
        self.assertEqual("11", group_rows[0]["market_group_id"])


if __name__ == "__main__":
    unittest.main()
