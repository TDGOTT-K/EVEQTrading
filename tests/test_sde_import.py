from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.sde_import import import_sde_reference_data


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class SdeImportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.sde_root = self.base_dir / "sde"
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
        self._write_jsonl(
            "_sde.jsonl",
            [{"buildNumber": 3248221, "releaseDate": "2026-04-01"}],
        )
        self._write_jsonl(
            "categories.jsonl",
            [{"_key": 6, "name": {"en": "Ship", "zh": "舰船"}}],
        )
        self._write_jsonl(
            "groups.jsonl",
            [{"_key": 25, "categoryID": 6, "name": {"en": "Frigate", "zh": "护卫舰"}}],
        )
        self._write_jsonl(
            "marketGroups.jsonl",
            [
                {"_key": 9, "name": {"en": "Ships", "zh": "舰船"}},
                {"_key": 10, "parentGroupID": 9, "name": {"en": "Frigates", "zh": "护卫舰"}},
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
                {
                    "_key": 34,
                    "groupID": 25,
                    "marketGroupID": 10,
                    "published": True,
                    "volume": 0.01,
                    "packagedVolume": 0.01,
                    "metaLevel": 0,
                    "name": {"en": "Tritanium", "zh": "三钛合金"},
                }
            ],
        )
        self._write_jsonl("npcStations.jsonl", [])

    def test_import_sde_stores_bilingual_item_names_and_defaults_to_selected_language(self) -> None:
        with connect(self.db_path) as connection:
            summary = import_sde_reference_data(
                connection,
                self.sde_root,
                language="zh",
                import_run_id="test-import-zh",
            )
            row = connection.execute(
                "SELECT type_name, type_name_en, type_name_zh, market_group, market_group_id FROM item_catalog WHERE type_id = 34"
            ).fetchone()
            market_group_row = connection.execute(
                "SELECT market_group_id, parent_group_id, market_group_name FROM market_group_catalog WHERE market_group_id = 10"
            ).fetchone()

        self.assertEqual("zh", summary["display_language"])
        self.assertEqual(2, summary["market_group_catalog"]["total_rows"])
        self.assertEqual("三钛合金", row["type_name"])
        self.assertEqual("Tritanium", row["type_name_en"])
        self.assertEqual("三钛合金", row["type_name_zh"])
        self.assertEqual("护卫舰", row["market_group"])
        self.assertEqual(10, row["market_group_id"])
        self.assertEqual(10, market_group_row["market_group_id"])
        self.assertEqual(9, market_group_row["parent_group_id"])
        self.assertEqual("护卫舰", market_group_row["market_group_name"])


if __name__ == "__main__":
    unittest.main()
