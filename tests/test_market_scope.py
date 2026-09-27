from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from eveqtrading.db import connect, init_db
from eveqtrading.market_scope import (
    GLOBAL_PLEX_REGION_ID,
    PLEX_TYPE_ID,
    describe_history_source,
    load_canonical_history_series,
    resolve_history_source_segments,
    resolve_order_source_region_id,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class MarketScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_resolve_order_source_region_routes_plex_to_global_market(self) -> None:
        self.assertEqual(GLOBAL_PLEX_REGION_ID, resolve_order_source_region_id(region_id=10000002, type_id=PLEX_TYPE_ID))
        self.assertEqual(10000002, resolve_order_source_region_id(region_id=10000002, type_id=34))

    def test_load_canonical_history_series_stitches_plex_across_market_transition(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (PLEX_TYPE_ID, "伊甸币", "PLEX", 1),
            )
            connection.executemany(
                """
                INSERT INTO market_history_daily (
                    type_id, region_id, market_date, average_price, highest_price, lowest_price,
                    volume, order_count, fetched_at, source, coverage_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (PLEX_TYPE_ID, 10000002, "2025-07-05", 5_100_000.0, 5_200_000.0, 5_000_000.0, 100.0, 10, "2026-04-11T00:00:00Z", "test", "ok"),
                    (PLEX_TYPE_ID, 10000002, "2025-07-06", 5_200_000.0, 5_300_000.0, 5_100_000.0, 110.0, 11, "2026-04-11T00:00:00Z", "test", "ok"),
                    (PLEX_TYPE_ID, 10000002, "2025-07-07", 5_300_000.0, 5_400_000.0, 5_200_000.0, 120.0, 12, "2026-04-11T00:00:00Z", "test", "ok"),
                    (PLEX_TYPE_ID, GLOBAL_PLEX_REGION_ID, "2025-07-07", 5_350_000.0, 5_450_000.0, 5_250_000.0, 130.0, 13, "2026-04-11T00:00:00Z", "test", "ok"),
                    (PLEX_TYPE_ID, GLOBAL_PLEX_REGION_ID, "2025-07-08", 5_360_000.0, 5_460_000.0, 5_260_000.0, 140.0, 14, "2026-04-11T00:00:00Z", "test", "ok"),
                ],
            )
            connection.commit()

            series = load_canonical_history_series(connection, region_id=10000002, type_id=PLEX_TYPE_ID)

        self.assertEqual(["2025-07-05", "2025-07-06", "2025-07-07", "2025-07-08"], [row["market_date"] for row in series])
        self.assertEqual([10000002, 10000002, GLOBAL_PLEX_REGION_ID, GLOBAL_PLEX_REGION_ID], [row["source_region_id"] for row in series])
        self.assertEqual(["regional", "regional", "global_plex", "global_plex"], [row["market_scope"] for row in series])
        self.assertEqual(5_350_000.0, series[2]["average_price"])

    def test_describe_history_source_marks_plex_as_stitched(self) -> None:
        payload = describe_history_source(region_id=10000002, type_id=PLEX_TYPE_ID)
        self.assertTrue(payload["is_stitched"])
        self.assertEqual(2, len(payload["segments"]))
        self.assertEqual(10000002, payload["segments"][0]["source_region_id"])
        self.assertEqual(GLOBAL_PLEX_REGION_ID, payload["segments"][1]["source_region_id"])

    def test_resolve_history_source_segments_keeps_normal_items_regional(self) -> None:
        segments = resolve_history_source_segments(region_id=10000002, type_id=34)
        self.assertEqual(1, len(segments))
        self.assertEqual(10000002, segments[0].source_region_id)
        self.assertEqual("regional", segments[0].market_scope)


if __name__ == "__main__":
    unittest.main()
