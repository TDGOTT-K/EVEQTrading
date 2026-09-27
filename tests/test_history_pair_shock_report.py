from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.history_pair_shock_report import build_history_pair_shock_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class HistoryPairShockReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_build_history_pair_shock_report_detects_up_and_down_events(self) -> None:
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
            left_price = 100.0
            right_price = 200.0
            history_rows: list[tuple[object, ...]] = []
            for offset in range(120):
                if offset == 60:
                    left_price *= 1.10
                elif offset == 61:
                    right_price *= 1.08
                elif offset == 90:
                    left_price *= 0.92
                elif offset == 91:
                    right_price *= 0.96
                market_date = (start_day + timedelta(days=offset)).isoformat()
                history_rows.append(
                    (40519, 10000002, market_date, left_price, left_price, left_price, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok")
                )
                history_rows.append(
                    (40520, 10000002, market_date, right_price, right_price, right_price, 2000.0, 10, "2026-04-10T00:00:00Z", "test", "ok")
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

            report = build_history_pair_shock_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                left_type_id=40519,
                right_type_id=40520,
                daily_shock_threshold=0.05,
                cumulative_3d_threshold=0.05,
                return_zscore_threshold=1.0,
                cooldown_days=5,
            )

        self.assertEqual(1, report["event_counts"]["up_event_count"])
        self.assertEqual(1, report["event_counts"]["down_event_count"])
        self.assertEqual(2, report["event_counts"]["all_event_count"])
        self.assertAlmostEqual(0.10, report["up_events"][0]["left_return"], places=6)
        self.assertAlmostEqual(0.08, report["up_events"][0]["right_cum_return_t1"], places=6)
        self.assertAlmostEqual(-0.08, report["down_events"][0]["left_return"], places=6)
        self.assertAlmostEqual(-0.04, report["down_events"][0]["right_cum_return_t1"], places=6)

        all_summary = report["statistics"]["all_event_summary"]
        t1_row = next(row for row in all_summary if row["horizon_days"] == 1)
        self.assertEqual(2, t1_row["sample_count"])

        markdown_path = Path(report["files"]["markdown_path"])
        events_csv_path = Path(report["files"]["events_csv_path"])
        self.assertTrue(markdown_path.exists())
        self.assertTrue(events_csv_path.exists())

        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 技能提取器 -> 大型技能注入器 冲击传导报告", markdown_text)
        self.assertIn("## 事件数量", markdown_text)
        self.assertIn("## 上涨事件后的平均反应", markdown_text)
        self.assertIn("## 全事件逐条结果", markdown_text)
        self.assertIn("## 下跌事件逐条结果", markdown_text)

        with events_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(2, len(rows))
        self.assertEqual("up", rows[0]["direction"])
        self.assertEqual("down", rows[1]["direction"])


if __name__ == "__main__":
    unittest.main()
