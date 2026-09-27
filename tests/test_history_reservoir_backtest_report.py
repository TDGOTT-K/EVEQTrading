from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.history_reservoir_backtest_report import build_history_reservoir_backtest_report
from eveqtrading.models import FeeConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class HistoryReservoirBacktestReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "reports"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_item(
            type_id=700001,
            type_name="摆动蓄水样本",
            market_group="测试材料",
            prices=self._swing_prices(days=150),
        )
        self._seed_item(
            type_id=700002,
            type_name="下跌陷阱样本",
            market_group="测试材料",
            prices=self._downtrend_prices(days=150),
        )
        self._seed_item(
            type_id=700003,
            type_name="极端错价样本",
            market_group="测试材料",
            prices=self._glitch_prices(days=150),
        )

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    @staticmethod
    def _swing_prices(*, days: int) -> list[float]:
        prices: list[float] = []
        for day in range(days):
            cycle_day = day % 10
            if cycle_day == 9:
                prices.append(80.0)
            elif cycle_day in {0, 1}:
                prices.append(110.0)
            else:
                prices.append(100.0)
        return prices

    @staticmethod
    def _downtrend_prices(*, days: int) -> list[float]:
        return [120.0 - day * 0.45 for day in range(days)]

    @staticmethod
    def _glitch_prices(*, days: int) -> list[float]:
        prices: list[float] = []
        for day in range(days):
            if day % 20 == 19:
                prices.append(0.01)
            else:
                prices.append(100.0)
        return prices

    def _seed_item(
        self,
        *,
        type_id: int,
        type_name: str,
        market_group: str,
        prices: list[float],
    ) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO item_catalog (type_id, type_name, market_group, market_group_id, is_published)
                VALUES (?, ?, ?, ?, ?)
                """,
                (type_id, type_name, market_group, 990001, 1),
            )
            for index, average_price in enumerate(prices, start=1):
                market_date = f"2025-{((index - 1) // 28) + 1:02d}-{((index - 1) % 28) + 1:02d}"
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        type_id,
                        10000002,
                        market_date,
                        average_price,
                        average_price * 1.05,
                        average_price * 0.95,
                        50_000.0,
                        1000,
                        "2026-04-12T00:00:00Z",
                        "test",
                        "ok",
                    ),
                )
            connection.commit()

    def test_build_history_reservoir_backtest_report_prefers_oscillating_item(self) -> None:
        with connect(self.db_path) as connection:
            report = build_history_reservoir_backtest_report(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                selected_targets=None,
                lookback_days=30,
                max_hold_days=7,
                entry_zscore_threshold=-1.0,
                min_entry_price_vs_mean_ratio=0.25,
                target_net_return=0.0,
                min_history_days=120,
                min_signal_count=4,
                min_win_rate=0.5,
                min_mean_first_exit_net_return=0.05,
                min_median_daily_turnover_isk=1_000_000.0,
                min_latest_daily_turnover_isk=1_000_000.0,
                min_median_daily_volume=1000.0,
                top_n=10,
                top_current_n=10,
                fee_config=FeeConfig(),
            )

        historical_names = [row["type_name"] for row in report["historical_top_rows"]]
        current_names = [row["type_name"] for row in report["current_top_rows"]]

        self.assertIn("摆动蓄水样本", historical_names)
        self.assertIn("摆动蓄水样本", current_names)
        self.assertNotIn("下跌陷阱样本", historical_names)
        self.assertNotIn("极端错价样本", historical_names)

        markdown = Path(report["files"]["markdown_path"]).read_text(encoding="utf-8")
        self.assertIn("只使用 `history`", markdown)
        self.assertIn("摆动蓄水样本", markdown)
        self.assertIn("价格带异常保护", markdown)

    def test_cli_prints_markdown_path(self) -> None:
        stdout = io.StringIO()
        with patch.object(
            sys,
            "argv",
            [
                "eveq",
                "report-history-reservoir-backtest",
                "--db-path",
                str(self.db_path),
                "--output-root",
                str(self.output_root),
                "--region-id",
                "10000002",
                "--type-id",
                "700001",
                "--type-id",
                "700002",
                "--lookback-days",
                "30",
                "--max-hold-days",
                "7",
                "--min-entry-price-vs-mean-ratio",
                "0.25",
                "--top-n",
                "5",
                "--top-current-n",
                "5",
                "--min-history-days",
                "120",
                "--min-signal-count",
                "4",
                "--min-win-rate",
                "0.5",
                "--min-mean-first-exit-net-return",
                "0.05",
                "--min-median-daily-turnover-isk",
                "1000000",
                "--min-latest-daily-turnover-isk",
                "1000000",
                "--min-median-daily-volume",
                "1000",
            ],
        ):
            with redirect_stdout(stdout):
                exit_code = cli_main()

        self.assertEqual(0, exit_code)
        rendered = stdout.getvalue()
        self.assertIn("History reservoir backtest report ready", rendered)
        self.assertIn("Markdown:", rendered)


if __name__ == "__main__":
    unittest.main()
