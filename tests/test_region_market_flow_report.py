from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.region_market_flow_report import build_region_market_flow_report
from eveqtrading.sde_import import import_sde_reference_data


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class RegionMarketFlowReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.sde_root = self.base_dir / "sde"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.sde_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._write_sde_fixture()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _write_jsonl(self, name: str, rows: list[dict[str, object]]) -> None:
        path = self.sde_root / name
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _write_sde_fixture(self) -> None:
        self._write_jsonl("_sde.jsonl", [{"buildNumber": 3248221, "releaseDate": "2026-04-01"}])
        self._write_jsonl("categories.jsonl", [{"_key": 6, "name": {"en": "Ship", "zh": "舰船"}}])
        self._write_jsonl(
            "groups.jsonl",
            [
                {"_key": 25, "categoryID": 6, "name": {"en": "Frigate", "zh": "护卫舰"}},
                {"_key": 26, "categoryID": 6, "name": {"en": "Module", "zh": "装备"}},
            ],
        )
        self._write_jsonl(
            "marketGroups.jsonl",
            [
                {"_key": 9, "name": {"en": "Ships", "zh": "舰船"}},
                {"_key": 10, "parentGroupID": 9, "name": {"en": "Frigates", "zh": "护卫舰"}},
                {"_key": 20, "name": {"en": "Modules", "zh": "装备"}},
                {"_key": 21, "parentGroupID": 20, "name": {"en": "Shield Extenders", "zh": "护盾扩展"}},
                {"_key": 30, "name": {"en": "Pilot Services", "zh": "飞行员服务"}},
                {"_key": 31, "parentGroupID": 30, "name": {"en": "Pilot's License", "zh": "飞行员执照"}},
            ],
        )
        self._write_jsonl(
            "mapRegions.jsonl",
            [{"_key": 10000002, "name": {"en": "The Forge", "zh": "铸造厂星域"}}],
        )
        self._write_jsonl(
            "mapSolarSystems.jsonl",
            [{"_key": 30000142, "regionID": 10000002, "name": {"en": "Jita", "zh": "吉他"}}],
        )
        self._write_jsonl(
            "types.jsonl",
            [
                {"_key": 101, "groupID": 25, "marketGroupID": 10, "published": True, "name": {"zh": "护卫舰一号"}},
                {"_key": 102, "groupID": 25, "marketGroupID": 10, "published": True, "name": {"zh": "护卫舰二号"}},
                {"_key": 103, "groupID": 25, "marketGroupID": 10, "published": True, "name": {"zh": "护卫舰三号"}},
                {"_key": 104, "groupID": 25, "marketGroupID": 10, "published": True, "name": {"zh": "护卫舰四号"}},
                {"_key": 105, "groupID": 25, "marketGroupID": 10, "published": True, "name": {"zh": "护卫舰五号"}},
                {"_key": 106, "groupID": 25, "marketGroupID": 10, "published": True, "name": {"zh": "护卫舰六号"}},
                {"_key": 201, "groupID": 26, "marketGroupID": 21, "published": True, "name": {"zh": "护盾扩展一号"}},
                {"_key": 202, "groupID": 26, "marketGroupID": 21, "published": True, "name": {"zh": "护盾扩展二号"}},
                {"_key": 44992, "groupID": 26, "marketGroupID": 31, "published": True, "name": {"zh": "伊甸币"}},
            ],
        )
        self._write_jsonl("npcStations.jsonl", [])

    def test_build_region_market_flow_report_writes_ranked_markdown_and_full_csv(self) -> None:
        with connect(self.db_path) as connection:
            import_sde_reference_data(
                connection,
                self.sde_root,
                language="zh",
                import_run_id="test-import-region-flow",
            )
            connection.executemany(
                """
                INSERT INTO market_history_daily (
                    type_id, region_id, market_date, average_price, highest_price, lowest_price,
                    volume, order_count, fetched_at, source, coverage_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (101, 10000002, "2026-04-09", 100.0, 110.0, 90.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (102, 10000002, "2026-04-09", 90.0, 100.0, 80.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (103, 10000002, "2026-04-09", 80.0, 90.0, 70.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (104, 10000002, "2026-04-09", 70.0, 80.0, 60.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (105, 10000002, "2026-04-09", 60.0, 70.0, 50.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (106, 10000002, "2026-04-09", 50.0, 60.0, 40.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (201, 10000002, "2026-04-09", 300.0, 320.0, 280.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (202, 10000002, "2026-04-09", 250.0, 270.0, 230.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                ],
            )
            connection.commit()

            report = build_region_market_flow_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                top_items_per_group=5,
            )

        markdown_path = Path(report["files"]["markdown_path"])
        items_csv_path = Path(report["files"]["items_csv_path"])
        json_path = Path(report["files"]["json_path"])

        self.assertTrue(markdown_path.exists())
        self.assertTrue(items_csv_path.exists())
        self.assertTrue(json_path.exists())
        self.assertEqual("铸造厂星域", report["region_name"])
        self.assertEqual("2026-04-09", report["market_date"])
        self.assertEqual(1_000_000.0, report["total_turnover_isk"])
        self.assertEqual("装备", report["top_level_groups"][0]["market_group_name"])
        self.assertEqual("舰船", report["top_level_groups"][1]["market_group_name"])

        ship_group = report["top_level_groups"][1]
        frigate_group = ship_group["child_groups"][0]
        self.assertEqual("护卫舰", frigate_group["market_group_name"])
        self.assertEqual(5, len(frigate_group["top_items"]))
        self.assertEqual(
            ["护卫舰一号", "护卫舰二号", "护卫舰三号", "护卫舰四号", "护卫舰五号"],
            [item["type_name"] for item in frigate_group["top_items"]],
        )

        markdown_text = markdown_path.read_text(encoding="utf-8")
        self.assertIn("# 铸造厂星域 日市场流量报告", markdown_text)
        self.assertIn("## 一级市场组排行", markdown_text)
        self.assertIn("### 1. 装备", markdown_text)
        self.assertIn("#### 1. 护盾扩展", markdown_text)
        self.assertIn("#### 1. 护卫舰", markdown_text)

        with items_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(8, len(rows))
        self.assertEqual("1", rows[0]["global_rank"])
        self.assertEqual("护盾扩展一号", rows[0]["type_name"])
        self.assertEqual("装备 > 护盾扩展", rows[0]["market_group_path"])

    def test_build_region_market_flow_report_uses_global_plex_rows_after_transition(self) -> None:
        with connect(self.db_path) as connection:
            import_sde_reference_data(
                connection,
                self.sde_root,
                language="zh",
                import_run_id="test-import-region-flow-plex",
            )
            connection.executemany(
                """
                INSERT INTO market_history_daily (
                    type_id, region_id, market_date, average_price, highest_price, lowest_price,
                    volume, order_count, fetched_at, source, coverage_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (101, 10000002, "2026-04-09", 100.0, 110.0, 90.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (201, 10000002, "2026-04-09", 300.0, 320.0, 280.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (44992, 10000002, "2025-07-07", 5_975_000.0, 6_001_000.0, 5_970_000.0, 324704.0, 646, "2026-04-10T00:00:00Z", "test", "ok"),
                    (44992, 19000001, "2026-04-09", 4_296_000.0, 4_310_000.0, 4_280_000.0, 1_197_126.0, 2500, "2026-04-11T13:52:50Z", "test", "ok"),
                ],
            )
            connection.commit()

            report = build_region_market_flow_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                top_items_per_group=5,
            )

        self.assertEqual("2026-04-09", report["market_date"])
        self.assertTrue(any(item["type_id"] == 44992 for item in report["global_top_items"]))
        plex_item = next(item for item in report["global_top_items"] if item["type_id"] == 44992)
        self.assertEqual("伊甸币", plex_item["type_name"])
        self.assertEqual(19000001, plex_item["source_region_id"])
        self.assertEqual(4_296_000.0 * 1_197_126.0, plex_item["turnover_isk"])

        markdown_text = Path(report["files"]["markdown_path"]).read_text(encoding="utf-8")
        self.assertIn("飞行员服务", markdown_text)
        self.assertIn("飞行员执照", markdown_text)
        self.assertIn("伊甸币", markdown_text)

    def test_build_region_market_flow_report_keeps_regional_market_date_when_plex_global_is_ahead(self) -> None:
        with connect(self.db_path) as connection:
            import_sde_reference_data(
                connection,
                self.sde_root,
                language="zh",
                import_run_id="test-import-region-flow-plex-date",
            )
            connection.executemany(
                """
                INSERT INTO market_history_daily (
                    type_id, region_id, market_date, average_price, highest_price, lowest_price,
                    volume, order_count, fetched_at, source, coverage_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (101, 10000002, "2026-04-09", 100.0, 110.0, 90.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (201, 10000002, "2026-04-09", 300.0, 320.0, 280.0, 1000.0, 10, "2026-04-10T00:00:00Z", "test", "ok"),
                    (44992, 19000001, "2026-04-10", 4_285_000.0, 4_300_000.0, 4_270_000.0, 1_645_024.0, 3703, "2026-04-11T13:52:50Z", "test", "ok"),
                    (44992, 19000001, "2026-04-09", 4_296_000.0, 4_310_000.0, 4_280_000.0, 1_197_126.0, 2500, "2026-04-11T13:52:50Z", "test", "ok"),
                ],
            )
            connection.commit()

            report = build_region_market_flow_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                top_items_per_group=5,
            )

        self.assertEqual("2026-04-09", report["market_date"])
        plex_item = next(item for item in report["global_top_items"] if item["type_id"] == 44992)
        self.assertEqual("2026-04-09", plex_item["market_date"])
        self.assertEqual(19000001, plex_item["source_region_id"])


if __name__ == "__main__":
    unittest.main()
