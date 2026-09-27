from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.history_pair_relationship_report import build_history_pair_relationship_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class HistoryPairRelationshipReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_build_history_pair_relationship_report_outputs_expected_statistics(self) -> None:
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
                    (40519, "技能提取器", "Skill Extractor", "技能提取器", None, None, None, None, None, None, "飞行员服务", 1),
                    (40520, "大型技能注入器", "Large Skill Injector", "大型技能注入器", None, None, None, None, None, None, "飞行员服务", 1),
                ],
            )
            start_day = date(2026, 1, 1)
            history_rows: list[tuple[object, ...]] = []
            for offset in range(40):
                market_date = (start_day + timedelta(days=offset)).isoformat()
                left_price = 100_000_000.0 + offset * 1_000_000.0
                right_price = left_price * 2.0
                history_rows.append(
                    (40519, 10000002, market_date, left_price, left_price, left_price, 100.0 + offset, 10, "2026-04-10T00:00:00Z", "test", "ok")
                )
                history_rows.append(
                    (40520, 10000002, market_date, right_price, right_price, right_price, 200.0 + offset, 10, "2026-04-10T00:00:00Z", "test", "ok")
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

            report = build_history_pair_relationship_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                left_type_id=40519,
                right_type_id=40520,
            )

        self.assertEqual("技能提取器", report["left_label"])
        self.assertEqual("大型技能注入器", report["right_label"])
        self.assertEqual(40, report["data_window"]["overlap_day_count"])
        self.assertAlmostEqual(1.0, report["statistics"]["summary"]["price_pearson_corr"], places=6)
        self.assertAlmostEqual(1.0, report["statistics"]["summary"]["return_pearson_corr"], places=6)
        self.assertAlmostEqual(0.0, report["statistics"]["price_regression"]["alpha"], places=4)
        self.assertAlmostEqual(2.0, report["statistics"]["price_regression"]["beta"], places=6)
        self.assertAlmostEqual(1.0, report["statistics"]["price_regression"]["r_squared"], places=6)
        self.assertAlmostEqual(2.0, report["statistics"]["summary"]["latest_price_ratio"], places=6)

        markdown_path = Path(report["files"]["markdown_path"])
        csv_path = Path(report["files"]["csv_path"])
        self.assertTrue(markdown_path.exists())
        self.assertTrue(csv_path.exists())

        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 技能提取器 / 大型技能注入器 历史关系报告", markdown_text)
        self.assertIn("## 核心结论", markdown_text)
        self.assertIn("## 滞后相关（收益率）", markdown_text)

        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(40, len(rows))
        self.assertEqual("2026-01-01", rows[0]["market_date"])
        self.assertEqual("2.0", rows[-1]["price_ratio"])


if __name__ == "__main__":
    unittest.main()
