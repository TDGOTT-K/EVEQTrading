from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.history_anchor_shock_propagation_report import build_history_anchor_shock_propagation_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class HistoryAnchorShockPropagationReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_build_history_anchor_shock_propagation_report_detects_delayed_response(self) -> None:
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
                    (44992, "伊甸币", "PLEX", "伊甸币", None, None, None, None, None, None, "PLEX", 1),
                    (50101, "传导目标A", "Target A", "传导目标A", None, None, None, None, None, None, "模块", 1),
                    (50102, "传导目标B", "Target B", "传导目标B", None, None, None, None, None, None, "舰船", 1),
                ],
            )

            anchor_price = 1_000.0
            target_a_price = 500.0
            target_b_price = 700.0
            history_rows: list[tuple[object, ...]] = []
            start_day = date(2025, 5, 1)
            for offset in range(150):
                if offset == 100:
                    anchor_price *= 1.10
                elif offset == 101:
                    target_a_price *= 1.06
                elif offset == 120:
                    anchor_price *= 0.92
                elif offset == 121:
                    target_a_price *= 0.96
                market_date = (start_day + timedelta(days=offset)).isoformat()
                anchor_region_id = 10000002 if market_date < "2025-07-07" else 19000001
                history_rows.append(
                    (
                        44992,
                        anchor_region_id,
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
                        target_a_price,
                        target_a_price,
                        target_a_price,
                        200.0,
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
                        target_b_price,
                        target_b_price,
                        target_b_price,
                        300.0,
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

            report = build_history_anchor_shock_propagation_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                anchor_type_id=44992,
                selected_targets=[
                    {"type_id": 50101, "type_name": "传导目标A", "selection_mode": "explicit"},
                    {"type_id": 50102, "type_name": "传导目标B", "selection_mode": "explicit"},
                ],
                min_overlap_days=100,
                top_n=5,
                daily_shock_threshold=0.05,
                cumulative_3d_threshold=0.05,
                return_zscore_threshold=1.0,
                cooldown_days=5,
            )

        self.assertEqual("伊甸币", report["anchor_label"])
        self.assertEqual(2, report["universe_summary"]["selected_target_count"])
        self.assertEqual(1, report["universe_summary"]["qualified_target_count"])
        self.assertEqual(1, report["universe_summary"]["up_event_count"])
        self.assertEqual(1, report["universe_summary"]["down_event_count"])
        self.assertEqual("传导目标A", report["up_rankings"][0]["target_label"])
        self.assertEqual(1, report["up_rankings"][0]["up_best_horizon"]["horizon_days"])
        self.assertGreater(report["up_rankings"][0]["up_best_horizon"]["abnormal_mean_return"], 0.05)
        self.assertEqual("传导目标A", report["down_rankings"][0]["target_label"])
        self.assertLess(report["down_rankings"][0]["down_best_horizon"]["event_mean_return"], 0.0)
        self.assertLess(report["down_rankings"][0]["down_best_horizon"]["abnormal_mean_return"], -0.03)

        markdown_path = Path(report["files"]["markdown_path"])
        json_path = Path(report["files"]["json_path"])
        csv_path = Path(report["files"]["csv_path"])
        self.assertTrue(markdown_path.exists())
        self.assertTrue(json_path.exists())
        self.assertTrue(csv_path.exists())

        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 伊甸币 冲击传导筛查报告", markdown_text)
        self.assertIn("## PLEX 上涨后最容易被带动的商品 Top", markdown_text)
        self.assertIn("## PLEX 下跌后最容易被带崩的商品 Top", markdown_text)

        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(1, len(rows))
        self.assertEqual("50101", rows[0]["target_type_id"])

    def test_build_history_anchor_shock_propagation_report_practical_filter_excludes_trash_targets(self) -> None:
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
                    (44992, "伊甸币", "PLEX", "伊甸币", None, None, None, None, None, None, "PLEX", 1),
                    (60101, "实用目标", "Useful Target", "实用目标", None, None, None, None, None, None, "模块", 1),
                    (60102, "垃圾涂装", "Trash SKIN", "垃圾涂装", None, None, None, None, None, None, "模块", 1),
                ],
            )

            anchor_price = 1_000.0
            useful_price = 500.0
            trash_price = 800.0
            history_rows: list[tuple[object, ...]] = []
            start_day = date(2025, 5, 1)
            for offset in range(150):
                if offset == 100:
                    anchor_price *= 1.10
                elif offset == 101:
                    useful_price *= 1.08
                    trash_price *= 1.08
                elif offset == 120:
                    anchor_price *= 0.92
                elif offset == 121:
                    useful_price *= 0.96
                    trash_price *= 0.96
                market_date = (start_day + timedelta(days=offset)).isoformat()
                anchor_region_id = 10000002 if market_date < "2025-07-07" else 19000001
                history_rows.append((44992, anchor_region_id, market_date, anchor_price, anchor_price, anchor_price, 1000.0, 10, "2026-04-12T00:00:00Z", "test", "ok"))
                history_rows.append((60101, 10000002, market_date, useful_price, useful_price, useful_price, 500_000.0, 10, "2026-04-12T00:00:00Z", "test", "ok"))
                history_rows.append((60102, 10000002, market_date, trash_price, trash_price, trash_price, 500_000.0, 10, "2026-04-12T00:00:00Z", "test", "ok"))
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

            report = build_history_anchor_shock_propagation_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                anchor_type_id=44992,
                selected_targets=[
                    {"type_id": 60101, "type_name": "实用目标", "selection_mode": "explicit"},
                    {"type_id": 60102, "type_name": "垃圾涂装", "selection_mode": "explicit"},
                ],
                min_overlap_days=100,
                top_n=5,
                daily_shock_threshold=0.05,
                cumulative_3d_threshold=0.05,
                return_zscore_threshold=1.0,
                cooldown_days=5,
                filter_profile="practical",
                min_event_sample_count=1,
            )

        self.assertEqual(2, report["universe_summary"]["selected_target_count"])
        self.assertEqual(1, report["universe_summary"]["qualified_target_count"])
        self.assertEqual("实用目标", report["up_rankings"][0]["target_label"])
        skipped_reasons = {row["type_name"]: row["reason"] for row in report["skipped_rows_sample"]}
        self.assertEqual("excluded_by_name_keyword", skipped_reasons["垃圾涂装"])


if __name__ == "__main__":
    unittest.main()
