from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .account_snapshot import capture_character_account_snapshot
from .character_account_report import build_character_account_report
from .analysis import analyze_market_scan, persist_scan_run
from .advisor import build_market_advice
from .batch_pipeline import chunked
from .benchmark import run_scan_benchmark
from .coverage import plan_market_coverage
from .coverage_cycle import run_coverage_cycle
from .coverage_worker import run_coverage_worker_once
from .db import connect, init_db
from .db_scan import build_db_scan_candidates, persist_feature_rows, resolve_db_scan_targets
from .history_fetch import HistoryFetchConfig, fetch_market_history, resolve_history_fetch_targets
from .history_anchor_relationship_report import build_history_anchor_relationship_report
from .history_anchor_shock_backtest_report import build_history_anchor_shock_backtest_report
from .history_anchor_shock_propagation_report import build_history_anchor_shock_propagation_report
from .history_pair_relationship_report import build_history_pair_relationship_report
from .history_pair_shock_report import build_history_pair_shock_report
from .history_reservoir_backtest_report import build_history_reservoir_backtest_report
from .history_batch import run_history_batch_fetch
from .history_sync import collect_history_sync_snapshot, run_history_sync_cycle
from .history_sync_worker import run_history_sync_worker_once
from .jita_passive_cache_cycle import run_jita_passive_cache_cycle
from .jita_passive_cache_advice import build_jita_passive_cache_advice
from .jita_passive_cache_brief import build_jita_passive_cache_brief
from .jita_passive_cache_diagnose import build_jita_passive_cache_diagnosis
from .jita_passive_cache_explain import build_jita_passive_cache_opportunity_explanation
from .jita_passive_cache_report import build_jita_passive_cache_report
from .jita_passive_cache_worker import run_jita_passive_cache_worker_once
from .jita_passive_cycle import run_jita_passive_cycle
from .jita_reservoir_report import build_jita_reservoir_report
from .models import FeeConfig, FilterConfig, ScanConfig
from .jita_passive_simple import JitaPassiveSimpleFilterConfig, run_jita_passive_simple_scan
from .market_entry_plan import build_market_entry_plan
from .market_shortlist import build_market_shortlist, render_market_shortlist_report
from .order_batch import run_order_batch_fetch
from .order_fetch import OrderFetchConfig, fetch_market_orders, resolve_order_fetch_targets
from .order_refresh import plan_order_refresh
from .order_refresh_cycle import run_order_refresh_cycle
from .order_refresh_worker import run_order_refresh_worker_once
from .region_order_mirror import run_region_order_mirror_once
from .region_order_mirror_worker import run_region_order_mirror_worker_once
from .report import (
    build_coverage_diagnosis,
    build_market_report,
    build_opportunity_explanation,
    build_scan_diagnosis,
    resolve_runtime_roots,
)
from .retention import run_retention_cleanup
from .region_market_flow_report import build_region_market_flow_report
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary
from .scan_runtime import execute_db_scan
from .sde_import import import_sde_reference_data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eveq", description="EVEQTrading traced analysis tools.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db_parser = subparsers.add_parser("init-db", help="Initialize the SQLite database schema.")
    init_db_parser.add_argument("--db-path", type=Path, required=True)
    init_db_parser.add_argument(
        "--schema-path",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "sql" / "schema.sql",
    )

    import_sde_parser = subparsers.add_parser("import-sde", help="Import local SDE reference data into SQLite.")
    import_sde_parser.add_argument("--db-path", type=Path, required=True)
    import_sde_parser.add_argument("--sde-root", type=Path, required=True)
    import_sde_parser.add_argument("--language", default="zh")

    region_market_flow_parser = subparsers.add_parser(
        "report-region-market-flow",
        help="Build a markdown report of one region's daily ISK turnover by market-group hierarchy.",
    )
    region_market_flow_parser.add_argument("--db-path", type=Path, required=True)
    region_market_flow_parser.add_argument("--output-root", type=Path, required=True)
    region_market_flow_parser.add_argument("--region-id", type=int, required=True)
    region_market_flow_parser.add_argument("--market-date")
    region_market_flow_parser.add_argument("--top-items-per-group", type=int, default=5)

    history_pair_parser = subparsers.add_parser(
        "report-history-pair-relationship",
        help="Build a markdown report describing the historical relationship between two market items.",
    )
    history_pair_parser.add_argument("--db-path", type=Path, required=True)
    history_pair_parser.add_argument("--output-root", type=Path, required=True)
    history_pair_parser.add_argument("--region-id", type=int, required=True)
    history_pair_parser.add_argument("--left-type-id", type=int, default=40519)
    history_pair_parser.add_argument("--right-type-id", type=int, default=40520)
    history_pair_parser.add_argument("--left-label")
    history_pair_parser.add_argument("--right-label")
    history_pair_parser.add_argument("--max-lag-days", type=int, default=14)

    history_anchor_parser = subparsers.add_parser(
        "report-history-anchor-relationship",
        help="Screen one anchor item against many market items and report price/volume/turnover relationships.",
    )
    history_anchor_parser.add_argument("--db-path", type=Path, required=True)
    history_anchor_parser.add_argument("--output-root", type=Path, required=True)
    history_anchor_parser.add_argument("--region-id", type=int, required=True)
    history_anchor_parser.add_argument("--anchor-type-id", type=int, default=44992)
    history_anchor_parser.add_argument("--anchor-label")
    history_anchor_parser.add_argument("--type-id", type=int, action="append", default=[])
    history_anchor_parser.add_argument("--type-id-file", type=Path)
    history_anchor_parser.add_argument("--all-published-market-types", action="store_true")
    history_anchor_parser.add_argument("--limit", type=int)
    history_anchor_parser.add_argument("--min-overlap-days", type=int, default=180)
    history_anchor_parser.add_argument("--top-n", type=int, default=20)

    history_anchor_shock_parser = subparsers.add_parser(
        "report-history-anchor-shock-propagation",
        help="Screen one anchor item's shock events against many market items and report delayed response windows.",
    )
    history_anchor_shock_parser.add_argument("--db-path", type=Path, required=True)
    history_anchor_shock_parser.add_argument("--output-root", type=Path, required=True)
    history_anchor_shock_parser.add_argument("--region-id", type=int, required=True)
    history_anchor_shock_parser.add_argument("--anchor-type-id", type=int, default=44992)
    history_anchor_shock_parser.add_argument("--anchor-label")
    history_anchor_shock_parser.add_argument("--type-id", type=int, action="append", default=[])
    history_anchor_shock_parser.add_argument("--type-id-file", type=Path)
    history_anchor_shock_parser.add_argument("--all-published-market-types", action="store_true")
    history_anchor_shock_parser.add_argument("--limit", type=int)
    history_anchor_shock_parser.add_argument("--min-overlap-days", type=int, default=180)
    history_anchor_shock_parser.add_argument("--top-n", type=int, default=20)
    history_anchor_shock_parser.add_argument("--daily-shock-threshold", type=float, default=0.05)
    history_anchor_shock_parser.add_argument("--cumulative-3d-threshold", type=float, default=0.08)
    history_anchor_shock_parser.add_argument("--return-zscore-threshold", type=float, default=2.0)
    history_anchor_shock_parser.add_argument("--cooldown-days", type=int, default=7)
    history_anchor_shock_parser.add_argument("--filter-profile", choices=["off", "practical"], default="off")
    history_anchor_shock_parser.add_argument("--min-median-daily-turnover-isk", type=float)
    history_anchor_shock_parser.add_argument("--min-latest-daily-turnover-isk", type=float)
    history_anchor_shock_parser.add_argument("--min-median-daily-volume", type=float)
    history_anchor_shock_parser.add_argument("--min-event-sample-count", type=int)
    history_anchor_shock_parser.add_argument("--exclude-name-keyword", action="append", default=[])
    history_anchor_shock_parser.add_argument("--exclude-group-keyword", action="append", default=[])

    history_anchor_shock_backtest_parser = subparsers.add_parser(
        "report-history-anchor-shock-backtest",
        help="Backtest whether buying downstream items after one anchor item's up-shock would still make net profit after costs.",
    )
    history_anchor_shock_backtest_parser.add_argument("--db-path", type=Path, required=True)
    history_anchor_shock_backtest_parser.add_argument("--output-root", type=Path, required=True)
    history_anchor_shock_backtest_parser.add_argument("--region-id", type=int, required=True)
    history_anchor_shock_backtest_parser.add_argument("--anchor-type-id", type=int, default=44992)
    history_anchor_shock_backtest_parser.add_argument("--anchor-label")
    history_anchor_shock_backtest_parser.add_argument("--type-id", type=int, action="append", default=[])
    history_anchor_shock_backtest_parser.add_argument("--type-id-file", type=Path)
    history_anchor_shock_backtest_parser.add_argument("--all-published-market-types", action="store_true")
    history_anchor_shock_backtest_parser.add_argument("--limit", type=int)
    history_anchor_shock_backtest_parser.add_argument("--min-overlap-days", type=int, default=180)
    history_anchor_shock_backtest_parser.add_argument("--top-n", type=int, default=20)
    history_anchor_shock_backtest_parser.add_argument("--daily-shock-threshold", type=float, default=0.05)
    history_anchor_shock_backtest_parser.add_argument("--cumulative-3d-threshold", type=float, default=0.08)
    history_anchor_shock_backtest_parser.add_argument("--return-zscore-threshold", type=float, default=2.0)
    history_anchor_shock_backtest_parser.add_argument("--cooldown-days", type=int, default=7)
    history_anchor_shock_backtest_parser.add_argument("--filter-profile", choices=["off", "practical"], default="off")
    history_anchor_shock_backtest_parser.add_argument("--min-median-daily-turnover-isk", type=float)
    history_anchor_shock_backtest_parser.add_argument("--min-latest-daily-turnover-isk", type=float)
    history_anchor_shock_backtest_parser.add_argument("--min-median-daily-volume", type=float)
    history_anchor_shock_backtest_parser.add_argument("--min-event-sample-count", type=int)
    history_anchor_shock_backtest_parser.add_argument("--exclude-name-keyword", action="append", default=[])
    history_anchor_shock_backtest_parser.add_argument("--exclude-group-keyword", action="append", default=[])
    history_anchor_shock_backtest_parser.add_argument("--entry-delay-days", type=int, default=1)
    history_anchor_shock_backtest_parser.add_argument("--holding-days", type=int, action="append", default=[])
    history_anchor_shock_backtest_parser.add_argument("--buy-broker-fee-rate", type=float, default=0.006)
    history_anchor_shock_backtest_parser.add_argument("--sell-broker-fee-rate", type=float, default=0.006)
    history_anchor_shock_backtest_parser.add_argument("--sales-tax-rate", type=float, default=0.03375)
    history_anchor_shock_backtest_parser.add_argument("--relist-fee-rate", type=float, default=0.0015)
    history_anchor_shock_backtest_parser.add_argument("--entry-slippage-rate", type=float, default=0.0)
    history_anchor_shock_backtest_parser.add_argument("--exit-slippage-rate", type=float, default=0.0)

    history_reservoir_backtest_parser = subparsers.add_parser(
        "report-history-reservoir-backtest",
        help="Backtest history-only reservoir suitability using only daily history windows, not orders.",
    )
    history_reservoir_backtest_parser.add_argument("--db-path", type=Path, required=True)
    history_reservoir_backtest_parser.add_argument("--output-root", type=Path, required=True)
    history_reservoir_backtest_parser.add_argument("--region-id", type=int, required=True)
    history_reservoir_backtest_parser.add_argument("--type-id", type=int, action="append", default=[])
    history_reservoir_backtest_parser.add_argument("--type-id-file", type=Path)
    history_reservoir_backtest_parser.add_argument("--all-published-market-types", action="store_true")
    history_reservoir_backtest_parser.add_argument("--limit", type=int)
    history_reservoir_backtest_parser.add_argument("--lookback-days", type=int, default=30)
    history_reservoir_backtest_parser.add_argument("--max-hold-days", type=int, default=7)
    history_reservoir_backtest_parser.add_argument("--entry-zscore-threshold", type=float, default=-1.0)
    history_reservoir_backtest_parser.add_argument("--target-net-return", type=float, default=0.0)
    history_reservoir_backtest_parser.add_argument("--min-entry-price-vs-mean-ratio", type=float, default=0.25)
    history_reservoir_backtest_parser.add_argument("--min-history-days", type=int, default=120)
    history_reservoir_backtest_parser.add_argument("--min-signal-count", type=int, default=4)
    history_reservoir_backtest_parser.add_argument("--min-win-rate", type=float, default=0.5)
    history_reservoir_backtest_parser.add_argument(
        "--min-mean-first-exit-net-return",
        "--min-mean-best-net-return",
        dest="min_mean_first_exit_net_return",
        type=float,
        default=0.02,
    )
    history_reservoir_backtest_parser.add_argument("--min-median-daily-turnover-isk", type=float, default=20_000_000.0)
    history_reservoir_backtest_parser.add_argument("--min-latest-daily-turnover-isk", type=float, default=20_000_000.0)
    history_reservoir_backtest_parser.add_argument("--min-median-daily-volume", type=float, default=100.0)
    history_reservoir_backtest_parser.add_argument("--top-n", type=int, default=30)
    history_reservoir_backtest_parser.add_argument("--top-current-n", type=int, default=20)
    history_reservoir_backtest_parser.add_argument("--buy-broker-fee-rate", type=float, default=0.006)
    history_reservoir_backtest_parser.add_argument("--sell-broker-fee-rate", type=float, default=0.006)
    history_reservoir_backtest_parser.add_argument("--sales-tax-rate", type=float, default=0.03375)
    history_reservoir_backtest_parser.add_argument("--relist-fee-rate", type=float, default=0.0015)

    history_pair_shock_parser = subparsers.add_parser(
        "report-history-pair-shock",
        help="Build an event-study markdown report for shock transmission between two market items.",
    )
    history_pair_shock_parser.add_argument("--db-path", type=Path, required=True)
    history_pair_shock_parser.add_argument("--output-root", type=Path, required=True)
    history_pair_shock_parser.add_argument("--region-id", type=int, required=True)
    history_pair_shock_parser.add_argument("--left-type-id", type=int, default=40519)
    history_pair_shock_parser.add_argument("--right-type-id", type=int, default=40520)
    history_pair_shock_parser.add_argument("--left-label")
    history_pair_shock_parser.add_argument("--right-label")
    history_pair_shock_parser.add_argument("--daily-shock-threshold", type=float, default=0.05)
    history_pair_shock_parser.add_argument("--cumulative-3d-threshold", type=float, default=0.08)
    history_pair_shock_parser.add_argument("--return-zscore-threshold", type=float, default=2.0)
    history_pair_shock_parser.add_argument("--cooldown-days", type=int, default=7)

    fetch_history_parser = subparsers.add_parser(
        "fetch-market-history",
        help="Fetch public ESI market history into SQLite and traced run artifacts.",
    )
    fetch_history_parser.add_argument("--db-path", type=Path, required=True)
    fetch_history_parser.add_argument("--output-root", type=Path, required=True)
    fetch_history_parser.add_argument("--region-id", type=int, required=True)
    fetch_history_parser.add_argument("--type-id", type=int, action="append", default=[])
    fetch_history_parser.add_argument("--type-id-file", type=Path)
    fetch_history_parser.add_argument("--all-published-market-types", action="store_true")
    fetch_history_parser.add_argument("--limit", type=int)
    fetch_history_parser.add_argument("--datasource", default="tranquility")
    fetch_history_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    fetch_history_parser.add_argument("--user-agent", default="EVEQTrading/0.1 market-history-fetch")
    fetch_history_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    fetch_history_parser.add_argument("--concurrent-requests", type=int, default=1)
    fetch_history_parser.add_argument("--request-wall-timeout-seconds", type=float)
    fetch_history_parser.add_argument("--max-retries", type=int, default=3)
    fetch_history_parser.add_argument("--pause-seconds", type=float, default=0.0)

    history_batch_parser = subparsers.add_parser(
        "run-history-batch-fetch",
        help="Run market history fetch in sequential chunks and write batch-level trace artifacts.",
    )
    history_batch_parser.add_argument("--db-path", type=Path, required=True)
    history_batch_parser.add_argument("--output-root", type=Path, required=True)
    history_batch_parser.add_argument("--region-id", type=int, required=True)
    history_batch_parser.add_argument("--type-id", type=int, action="append", default=[])
    history_batch_parser.add_argument("--type-id-file", type=Path)
    history_batch_parser.add_argument("--all-published-market-types", action="store_true")
    history_batch_parser.add_argument("--limit", type=int)
    history_batch_parser.add_argument("--chunk-size", type=int, default=100)
    history_batch_parser.add_argument("--max-chunks", type=int)
    history_batch_parser.add_argument("--datasource", default="tranquility")
    history_batch_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    history_batch_parser.add_argument("--user-agent", default="EVEQTrading/0.1 market-history-batch-fetch")
    history_batch_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    history_batch_parser.add_argument("--concurrent-requests", type=int, default=1)
    history_batch_parser.add_argument("--request-wall-timeout-seconds", type=float)
    history_batch_parser.add_argument("--max-retries", type=int, default=3)
    history_batch_parser.add_argument("--pause-seconds", type=float, default=0.0)

    history_sync_plan_parser = subparsers.add_parser(
        "plan-history-sync",
        help="Inspect history bootstrap backlog and latest-day refresh backlog for server scheduling.",
    )
    history_sync_plan_parser.add_argument("--db-path", type=Path, required=True)
    history_sync_plan_parser.add_argument("--output-root", type=Path, required=True)
    history_sync_plan_parser.add_argument("--region-id", type=int, required=True)
    history_sync_plan_parser.add_argument("--refresh-interval-hours", type=float, default=24.0)
    history_sync_plan_parser.add_argument("--not-found-recheck-hours", type=float, default=168.0)
    history_sync_plan_parser.add_argument("--limit", type=int)

    history_sync_cycle_parser = subparsers.add_parser(
        "run-history-sync-cycle",
        help="Run one server-side history cycle: bootstrap missing types, then refresh only the latest day for due types.",
    )
    history_sync_cycle_parser.add_argument("--db-path", type=Path, required=True)
    history_sync_cycle_parser.add_argument("--output-root", type=Path, required=True)
    history_sync_cycle_parser.add_argument("--region-id", type=int, required=True)
    history_sync_cycle_parser.add_argument("--backfill-chunk-size", type=int, default=100)
    history_sync_cycle_parser.add_argument("--max-backfill-chunks", type=int)
    history_sync_cycle_parser.add_argument("--refresh-chunk-size", type=int, default=200)
    history_sync_cycle_parser.add_argument("--max-refresh-chunks", type=int)
    history_sync_cycle_parser.add_argument("--refresh-interval-hours", type=float, default=24.0)
    history_sync_cycle_parser.add_argument("--not-found-recheck-hours", type=float, default=168.0)
    history_sync_cycle_parser.add_argument("--limit", type=int)
    history_sync_cycle_parser.add_argument("--datasource", default="tranquility")
    history_sync_cycle_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    history_sync_cycle_parser.add_argument("--user-agent", default="EVEQTrading/0.1 history-sync")
    history_sync_cycle_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    history_sync_cycle_parser.add_argument("--concurrent-requests", type=int, default=1)
    history_sync_cycle_parser.add_argument("--request-wall-timeout-seconds", type=float)
    history_sync_cycle_parser.add_argument("--max-retries", type=int, default=3)
    history_sync_cycle_parser.add_argument("--pause-seconds", type=float, default=0.0)
    history_sync_cycle_parser.add_argument("--failure-backoff-seconds", type=int, default=0)

    history_sync_worker_parser = subparsers.add_parser(
        "run-history-sync-worker-once",
        help="Run one guarded history sync worker cycle with lock and state files for server timers.",
    )
    history_sync_worker_parser.add_argument("--db-path", type=Path, required=True)
    history_sync_worker_parser.add_argument("--output-root", type=Path, required=True)
    history_sync_worker_parser.add_argument("--lock-path", type=Path, required=True)
    history_sync_worker_parser.add_argument("--state-path", type=Path, required=True)
    history_sync_worker_parser.add_argument("--region-id", type=int, required=True)
    history_sync_worker_parser.add_argument("--backfill-chunk-size", type=int, default=100)
    history_sync_worker_parser.add_argument("--max-backfill-chunks", type=int)
    history_sync_worker_parser.add_argument("--refresh-chunk-size", type=int, default=200)
    history_sync_worker_parser.add_argument("--max-refresh-chunks", type=int)
    history_sync_worker_parser.add_argument("--refresh-interval-hours", type=float, default=24.0)
    history_sync_worker_parser.add_argument("--not-found-recheck-hours", type=float, default=168.0)
    history_sync_worker_parser.add_argument("--limit", type=int)
    history_sync_worker_parser.add_argument("--datasource", default="tranquility")
    history_sync_worker_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    history_sync_worker_parser.add_argument("--user-agent", default="EVEQTrading/0.1 history-sync-worker")
    history_sync_worker_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    history_sync_worker_parser.add_argument("--concurrent-requests", type=int, default=1)
    history_sync_worker_parser.add_argument("--request-wall-timeout-seconds", type=float)
    history_sync_worker_parser.add_argument("--max-retries", type=int, default=3)
    history_sync_worker_parser.add_argument("--pause-seconds", type=float, default=0.0)
    history_sync_worker_parser.add_argument("--failure-backoff-seconds", type=int, default=0)
    history_sync_worker_parser.add_argument("--max-runtime-seconds", type=float, default=540.0)
    history_sync_worker_parser.add_argument("--max-cycles-per-run", type=int)

    fetch_orders_parser = subparsers.add_parser(
        "fetch-market-orders",
        help="Fetch public ESI region orders into snapshot tables and traced run artifacts.",
    )
    fetch_orders_parser.add_argument("--db-path", type=Path, required=True)
    fetch_orders_parser.add_argument("--output-root", type=Path, required=True)
    fetch_orders_parser.add_argument("--region-id", type=int, required=True)
    fetch_orders_parser.add_argument("--type-id", type=int, action="append", default=[])
    fetch_orders_parser.add_argument("--type-id-file", type=Path)
    fetch_orders_parser.add_argument("--all-published-market-types", action="store_true")
    fetch_orders_parser.add_argument("--limit", type=int)
    fetch_orders_parser.add_argument("--datasource", default="tranquility")
    fetch_orders_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    fetch_orders_parser.add_argument("--user-agent", default="EVEQTrading/0.1 market-orders-fetch")
    fetch_orders_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    fetch_orders_parser.add_argument("--concurrent-requests", type=int, default=1)
    fetch_orders_parser.add_argument("--max-retries", type=int, default=3)
    fetch_orders_parser.add_argument("--pause-seconds", type=float, default=0.0)
    fetch_orders_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    fetch_orders_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")

    orders_batch_parser = subparsers.add_parser(
        "run-orders-batch-fetch",
        help="Run market orders fetch in sequential chunks and write batch-level trace artifacts.",
    )
    orders_batch_parser.add_argument("--db-path", type=Path, required=True)
    orders_batch_parser.add_argument("--output-root", type=Path, required=True)
    orders_batch_parser.add_argument("--region-id", type=int, required=True)
    orders_batch_parser.add_argument("--type-id", type=int, action="append", default=[])
    orders_batch_parser.add_argument("--type-id-file", type=Path)
    orders_batch_parser.add_argument("--all-published-market-types", action="store_true")
    orders_batch_parser.add_argument("--limit", type=int)
    orders_batch_parser.add_argument("--chunk-size", type=int, default=100)
    orders_batch_parser.add_argument("--max-chunks", type=int)
    orders_batch_parser.add_argument("--datasource", default="tranquility")
    orders_batch_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    orders_batch_parser.add_argument("--user-agent", default="EVEQTrading/0.1 market-orders-batch-fetch")
    orders_batch_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    orders_batch_parser.add_argument("--concurrent-requests", type=int, default=1)
    orders_batch_parser.add_argument("--max-retries", type=int, default=3)
    orders_batch_parser.add_argument("--pause-seconds", type=float, default=0.0)
    orders_batch_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    orders_batch_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")

    order_refresh_plan_parser = subparsers.add_parser(
        "plan-order-refresh",
        help="Inspect the server-side order cache backlog by freshness tier and selection priority.",
    )
    order_refresh_plan_parser.add_argument("--db-path", type=Path, required=True)
    order_refresh_plan_parser.add_argument("--output-root", type=Path, required=True)
    order_refresh_plan_parser.add_argument("--region-id", type=int, required=True)
    order_refresh_plan_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"], default="warm")
    order_refresh_plan_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    order_refresh_plan_parser.add_argument("--limit", type=int)

    region_order_mirror_parser = subparsers.add_parser(
        "run-region-order-mirror",
        help="Mirror one whole region order book sweep, then split it back into per-type snapshots locally.",
    )
    region_order_mirror_parser.add_argument("--db-path", type=Path, required=True)
    region_order_mirror_parser.add_argument("--output-root", type=Path, required=True)
    region_order_mirror_parser.add_argument("--region-id", type=int, required=True)
    region_order_mirror_parser.add_argument(
        "--target-universe",
        choices=["history_ready", "all_published"],
        default="history_ready",
    )
    region_order_mirror_parser.add_argument("--limit", type=int)
    region_order_mirror_parser.add_argument("--datasource", default="tranquility")
    region_order_mirror_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    region_order_mirror_parser.add_argument("--user-agent", default="EVEQTrading/0.1 region-order-mirror")
    region_order_mirror_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    region_order_mirror_parser.add_argument("--concurrent-requests", type=int, default=4)
    region_order_mirror_parser.add_argument("--max-retries", type=int, default=3)
    region_order_mirror_parser.add_argument("--pause-seconds", type=float, default=0.0)
    region_order_mirror_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")

    region_order_mirror_worker_parser = subparsers.add_parser(
        "run-region-order-mirror-worker-once",
        help="Run one guarded whole-region order mirror worker for always-on foreign ingest.",
    )
    region_order_mirror_worker_parser.add_argument("--db-path", type=Path, required=True)
    region_order_mirror_worker_parser.add_argument("--output-root", type=Path, required=True)
    region_order_mirror_worker_parser.add_argument("--lock-path", type=Path, required=True)
    region_order_mirror_worker_parser.add_argument("--state-path", type=Path, required=True)
    region_order_mirror_worker_parser.add_argument("--region-id", type=int, required=True)
    region_order_mirror_worker_parser.add_argument(
        "--target-universe",
        choices=["history_ready", "all_published"],
        default="history_ready",
    )
    region_order_mirror_worker_parser.add_argument("--refresh-interval-minutes", type=int, default=30)
    region_order_mirror_worker_parser.add_argument("--limit", type=int)
    region_order_mirror_worker_parser.add_argument("--datasource", default="tranquility")
    region_order_mirror_worker_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    region_order_mirror_worker_parser.add_argument("--user-agent", default="EVEQTrading/0.1 region-order-mirror-worker")
    region_order_mirror_worker_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    region_order_mirror_worker_parser.add_argument("--concurrent-requests", type=int, default=8)
    region_order_mirror_worker_parser.add_argument("--max-retries", type=int, default=3)
    region_order_mirror_worker_parser.add_argument("--pause-seconds", type=float, default=0.0)
    region_order_mirror_worker_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")

    run_db_scan_parser = subparsers.add_parser(
        "run-db-scan",
        help="Run a traced coarse scan directly from SQLite history and order snapshots.",
    )
    run_db_scan_parser.add_argument("--db-path", type=Path, required=True)
    run_db_scan_parser.add_argument("--output-root", type=Path, required=True)
    run_db_scan_parser.add_argument("--region-id", type=int, required=True)
    run_db_scan_parser.add_argument("--type-id", type=int, action="append", default=[])
    run_db_scan_parser.add_argument("--type-id-file", type=Path)
    run_db_scan_parser.add_argument("--ready-market-scan-types", action="store_true")
    run_db_scan_parser.add_argument("--all-published-market-types", action="store_true")
    run_db_scan_parser.add_argument("--limit", type=int)
    run_db_scan_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    run_db_scan_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    run_db_scan_parser.add_argument("--top-n", type=int, default=10)
    run_db_scan_parser.add_argument("--strategy-set", default="v1-db-region")
    run_db_scan_parser.add_argument("--parameter-version", default="v1-db")

    jita_passive_simple_parser = subparsers.add_parser(
        "scan-jita-passive-simple",
        help="Run the temporary Jita passive flip scan using stable two-sided turnover filters.",
    )
    jita_passive_simple_parser.add_argument("--db-path", type=Path, required=True)
    jita_passive_simple_parser.add_argument("--output-root", type=Path, required=True)
    jita_passive_simple_parser.add_argument("--region-id", type=int, required=True)
    jita_passive_simple_parser.add_argument("--type-id", type=int, action="append", default=[])
    jita_passive_simple_parser.add_argument("--type-id-file", type=Path)
    jita_passive_simple_parser.add_argument("--ready-market-scan-types", action="store_true")
    jita_passive_simple_parser.add_argument("--all-published-market-types", action="store_true")
    jita_passive_simple_parser.add_argument("--limit", type=int)
    jita_passive_simple_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    jita_passive_simple_parser.add_argument("--top-n", type=int, default=50)
    jita_passive_simple_parser.add_argument("--max-flip-units-cv-30d", type=float, default=0.30)
    jita_passive_simple_parser.add_argument("--max-flip-units-cv-7d", type=float, default=0.50)
    jita_passive_simple_parser.add_argument("--min-daily-capacity-isk-30d", type=float, default=100_000_000.0)
    jita_passive_simple_parser.add_argument("--max-unit-buy-cost-isk", type=float)
    jita_passive_simple_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"])
    jita_passive_simple_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    jita_passive_simple_parser.add_argument("--cache-only", action="store_true")
    jita_passive_simple_parser.add_argument("--strategy-set", default="jita-passive-simple")
    jita_passive_simple_parser.add_argument("--parameter-version", default="v1-jita-passive-simple")

    jita_passive_full_parser = subparsers.add_parser(
        "scan-jita-passive-full",
        help="Run the current full cached-universe Jita passive scan across all ready order snapshots.",
    )
    jita_passive_full_parser.add_argument("--db-path", type=Path, required=True)
    jita_passive_full_parser.add_argument("--output-root", type=Path, required=True)
    jita_passive_full_parser.add_argument("--region-id", type=int, required=True)
    jita_passive_full_parser.add_argument("--type-id", type=int, action="append", default=[])
    jita_passive_full_parser.add_argument("--type-id-file", type=Path)
    jita_passive_full_parser.add_argument("--ready-market-scan-types", action="store_true")
    jita_passive_full_parser.add_argument("--all-published-market-types", action="store_true")
    jita_passive_full_parser.add_argument("--limit", type=int)
    jita_passive_full_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    jita_passive_full_parser.add_argument("--top-n", type=int, default=100)
    jita_passive_full_parser.add_argument("--max-flip-units-cv-30d", type=float, default=0.30)
    jita_passive_full_parser.add_argument("--max-flip-units-cv-7d", type=float, default=0.50)
    jita_passive_full_parser.add_argument("--min-daily-capacity-isk-30d", type=float, default=100_000_000.0)
    jita_passive_full_parser.add_argument("--max-unit-buy-cost-isk", type=float)
    jita_passive_full_parser.add_argument("--strategy-set", default="jita-passive-full")
    jita_passive_full_parser.add_argument("--parameter-version", default="v1-jita-passive-full")

    jita_passive_cycle_parser = subparsers.add_parser(
        "run-jita-passive-cycle",
        help="Refresh orders for history-ready types, then run the temporary Jita passive simple scan.",
    )
    jita_passive_cycle_parser.add_argument("--db-path", type=Path, required=True)
    jita_passive_cycle_parser.add_argument("--output-root", type=Path, required=True)
    jita_passive_cycle_parser.add_argument("--region-id", type=int, required=True)
    jita_passive_cycle_parser.add_argument("--orders-chunk-size", type=int, default=100)
    jita_passive_cycle_parser.add_argument("--max-orders-chunks", type=int)
    jita_passive_cycle_parser.add_argument("--limit", type=int)
    jita_passive_cycle_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"], default="warm")
    jita_passive_cycle_parser.add_argument("--datasource", default="tranquility")
    jita_passive_cycle_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    jita_passive_cycle_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 jita-passive-cycle-orders")
    jita_passive_cycle_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    jita_passive_cycle_parser.add_argument("--concurrent-requests", type=int, default=1)
    jita_passive_cycle_parser.add_argument("--max-retries", type=int, default=3)
    jita_passive_cycle_parser.add_argument("--pause-seconds", type=float, default=0.0)
    jita_passive_cycle_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    jita_passive_cycle_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    jita_passive_cycle_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    jita_passive_cycle_parser.add_argument("--top-n", type=int, default=50)
    jita_passive_cycle_parser.add_argument("--max-flip-units-cv-30d", type=float, default=0.30)
    jita_passive_cycle_parser.add_argument("--max-flip-units-cv-7d", type=float, default=0.50)
    jita_passive_cycle_parser.add_argument("--min-daily-capacity-isk-30d", type=float, default=100_000_000.0)
    jita_passive_cycle_parser.add_argument("--max-unit-buy-cost-isk", type=float)
    jita_passive_cycle_parser.add_argument("--strategy-set", default="jita-passive-simple")
    jita_passive_cycle_parser.add_argument("--parameter-version", default="v1-jita-passive-simple")

    jita_passive_cache_cycle_parser = subparsers.add_parser(
        "run-jita-passive-cache-cycle",
        help="Refresh one cache profile, then scan the whole local cache-hit pool for Jita passive opportunities.",
    )
    jita_passive_cache_cycle_parser.add_argument("--db-path", type=Path, required=True)
    jita_passive_cache_cycle_parser.add_argument("--output-root", type=Path, required=True)
    jita_passive_cache_cycle_parser.add_argument("--region-id", type=int, required=True)
    jita_passive_cache_cycle_parser.add_argument("--orders-chunk-size", type=int, default=50)
    jita_passive_cache_cycle_parser.add_argument("--max-orders-chunks", type=int)
    jita_passive_cache_cycle_parser.add_argument("--limit", type=int)
    jita_passive_cache_cycle_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"], default="warm")
    jita_passive_cache_cycle_parser.add_argument("--datasource", default="tranquility")
    jita_passive_cache_cycle_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    jita_passive_cache_cycle_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 jita-passive-cache-cycle")
    jita_passive_cache_cycle_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    jita_passive_cache_cycle_parser.add_argument("--concurrent-requests", type=int, default=1)
    jita_passive_cache_cycle_parser.add_argument("--max-retries", type=int, default=3)
    jita_passive_cache_cycle_parser.add_argument("--pause-seconds", type=float, default=0.0)
    jita_passive_cache_cycle_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    jita_passive_cache_cycle_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    jita_passive_cache_cycle_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    jita_passive_cache_cycle_parser.add_argument("--top-n", type=int, default=50)
    jita_passive_cache_cycle_parser.add_argument("--max-flip-units-cv-30d", type=float, default=0.30)
    jita_passive_cache_cycle_parser.add_argument("--max-flip-units-cv-7d", type=float, default=0.50)
    jita_passive_cache_cycle_parser.add_argument("--min-daily-capacity-isk-30d", type=float, default=100_000_000.0)
    jita_passive_cache_cycle_parser.add_argument("--max-unit-buy-cost-isk", type=float)
    jita_passive_cache_cycle_parser.add_argument("--strategy-set", default="jita-passive-simple")
    jita_passive_cache_cycle_parser.add_argument("--parameter-version", default="v1-jita-passive-simple")

    jita_passive_cache_worker_parser = subparsers.add_parser(
        "run-jita-passive-cache-worker-once",
        help="Run one guarded jita passive cache worker with lock and state files for server timers.",
    )
    jita_passive_cache_worker_parser.add_argument("--db-path", type=Path, required=True)
    jita_passive_cache_worker_parser.add_argument("--output-root", type=Path, required=True)
    jita_passive_cache_worker_parser.add_argument("--lock-path", type=Path, required=True)
    jita_passive_cache_worker_parser.add_argument("--state-path", type=Path, required=True)
    jita_passive_cache_worker_parser.add_argument("--region-id", type=int, required=True)
    jita_passive_cache_worker_parser.add_argument("--orders-chunk-size", type=int, default=50)
    jita_passive_cache_worker_parser.add_argument("--max-orders-chunks", type=int)
    jita_passive_cache_worker_parser.add_argument("--limit", type=int)
    jita_passive_cache_worker_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"], default="warm")
    jita_passive_cache_worker_parser.add_argument("--datasource", default="tranquility")
    jita_passive_cache_worker_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    jita_passive_cache_worker_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 jita-passive-cache-worker")
    jita_passive_cache_worker_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    jita_passive_cache_worker_parser.add_argument("--concurrent-requests", type=int, default=1)
    jita_passive_cache_worker_parser.add_argument("--max-retries", type=int, default=3)
    jita_passive_cache_worker_parser.add_argument("--pause-seconds", type=float, default=0.0)
    jita_passive_cache_worker_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    jita_passive_cache_worker_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    jita_passive_cache_worker_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    jita_passive_cache_worker_parser.add_argument("--top-n", type=int, default=50)
    jita_passive_cache_worker_parser.add_argument("--max-flip-units-cv-30d", type=float, default=0.30)
    jita_passive_cache_worker_parser.add_argument("--max-flip-units-cv-7d", type=float, default=0.50)
    jita_passive_cache_worker_parser.add_argument("--min-daily-capacity-isk-30d", type=float, default=100_000_000.0)
    jita_passive_cache_worker_parser.add_argument("--max-unit-buy-cost-isk", type=float)
    jita_passive_cache_worker_parser.add_argument("--strategy-set", default="jita-passive-simple")
    jita_passive_cache_worker_parser.add_argument("--parameter-version", default="v1-jita-passive-simple")
    jita_passive_cache_worker_parser.add_argument("--failure-backoff-seconds", type=int, default=0)
    jita_passive_cache_worker_parser.add_argument("--max-runtime-seconds", type=float, default=540.0)
    jita_passive_cache_worker_parser.add_argument("--max-cycles-per-run", type=int)

    order_refresh_cycle_parser = subparsers.add_parser(
        "run-order-refresh-cycle",
        help="Refresh only missing or remotely due order snapshots for one cache profile.",
    )
    order_refresh_cycle_parser.add_argument("--db-path", type=Path, required=True)
    order_refresh_cycle_parser.add_argument("--output-root", type=Path, required=True)
    order_refresh_cycle_parser.add_argument("--region-id", type=int, required=True)
    order_refresh_cycle_parser.add_argument("--orders-chunk-size", type=int, default=50)
    order_refresh_cycle_parser.add_argument("--max-orders-chunks", type=int)
    order_refresh_cycle_parser.add_argument("--limit", type=int)
    order_refresh_cycle_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"], default="warm")
    order_refresh_cycle_parser.add_argument("--datasource", default="tranquility")
    order_refresh_cycle_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    order_refresh_cycle_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 order-refresh-cycle")
    order_refresh_cycle_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    order_refresh_cycle_parser.add_argument("--concurrent-requests", type=int, default=1)
    order_refresh_cycle_parser.add_argument("--max-retries", type=int, default=3)
    order_refresh_cycle_parser.add_argument("--pause-seconds", type=float, default=0.0)
    order_refresh_cycle_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    order_refresh_cycle_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")

    order_refresh_worker_parser = subparsers.add_parser(
        "run-order-refresh-worker-once",
        help="Run one guarded order refresh worker that keeps warming the selected cache profile.",
    )
    order_refresh_worker_parser.add_argument("--db-path", type=Path, required=True)
    order_refresh_worker_parser.add_argument("--output-root", type=Path, required=True)
    order_refresh_worker_parser.add_argument("--lock-path", type=Path, required=True)
    order_refresh_worker_parser.add_argument("--state-path", type=Path, required=True)
    order_refresh_worker_parser.add_argument("--region-id", type=int, required=True)
    order_refresh_worker_parser.add_argument("--orders-chunk-size", type=int, default=50)
    order_refresh_worker_parser.add_argument("--max-orders-chunks", type=int)
    order_refresh_worker_parser.add_argument("--limit", type=int)
    order_refresh_worker_parser.add_argument("--orders-cache-profile", choices=["hot", "warm", "cold"], default="warm")
    order_refresh_worker_parser.add_argument("--datasource", default="tranquility")
    order_refresh_worker_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    order_refresh_worker_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 order-refresh-worker")
    order_refresh_worker_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    order_refresh_worker_parser.add_argument("--concurrent-requests", type=int, default=1)
    order_refresh_worker_parser.add_argument("--max-retries", type=int, default=3)
    order_refresh_worker_parser.add_argument("--pause-seconds", type=float, default=0.0)
    order_refresh_worker_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    order_refresh_worker_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    order_refresh_worker_parser.add_argument("--failure-backoff-seconds", type=int, default=0)
    order_refresh_worker_parser.add_argument("--max-runtime-seconds", type=float, default=540.0)
    order_refresh_worker_parser.add_argument("--max-cycles-per-run", type=int)

    region_pipeline_parser = subparsers.add_parser(
        "run-region-pipeline",
        help="Run history fetch, order fetch, and DB scan sequentially for one region scope.",
    )
    region_pipeline_parser.add_argument("--db-path", type=Path, required=True)
    region_pipeline_parser.add_argument("--output-root", type=Path, required=True)
    region_pipeline_parser.add_argument("--region-id", type=int, required=True)
    region_pipeline_parser.add_argument("--type-id", type=int, action="append", default=[])
    region_pipeline_parser.add_argument("--type-id-file", type=Path)
    region_pipeline_parser.add_argument("--all-published-market-types", action="store_true")
    region_pipeline_parser.add_argument("--limit", type=int)
    region_pipeline_parser.add_argument("--datasource", default="tranquility")
    region_pipeline_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    region_pipeline_parser.add_argument("--history-user-agent", default="EVEQTrading/0.1 market-history-fetch")
    region_pipeline_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 market-orders-fetch")
    region_pipeline_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    region_pipeline_parser.add_argument("--concurrent-requests", type=int, default=1)
    region_pipeline_parser.add_argument("--request-wall-timeout-seconds", type=float)
    region_pipeline_parser.add_argument("--max-retries", type=int, default=3)
    region_pipeline_parser.add_argument("--pause-seconds", type=float, default=0.0)
    region_pipeline_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    region_pipeline_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    region_pipeline_parser.add_argument("--skip-history-fetch", action="store_true")
    region_pipeline_parser.add_argument("--skip-orders-fetch", action="store_true")
    region_pipeline_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    region_pipeline_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    region_pipeline_parser.add_argument("--top-n", type=int, default=10)
    region_pipeline_parser.add_argument("--strategy-set", default="v1-db-region")
    region_pipeline_parser.add_argument("--parameter-version", default="v1-db")

    batch_pipeline_parser = subparsers.add_parser(
        "run-region-batch-pipeline",
        help="Run the region pipeline across many type targets in sequential chunks.",
    )
    batch_pipeline_parser.add_argument("--db-path", type=Path, required=True)
    batch_pipeline_parser.add_argument("--output-root", type=Path, required=True)
    batch_pipeline_parser.add_argument("--region-id", type=int, required=True)
    batch_pipeline_parser.add_argument("--type-id", type=int, action="append", default=[])
    batch_pipeline_parser.add_argument("--type-id-file", type=Path)
    batch_pipeline_parser.add_argument("--all-published-market-types", action="store_true")
    batch_pipeline_parser.add_argument("--limit", type=int)
    batch_pipeline_parser.add_argument("--chunk-size", type=int, default=100)
    batch_pipeline_parser.add_argument("--max-chunks", type=int)
    batch_pipeline_parser.add_argument("--datasource", default="tranquility")
    batch_pipeline_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    batch_pipeline_parser.add_argument("--history-user-agent", default="EVEQTrading/0.1 market-history-fetch")
    batch_pipeline_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 market-orders-fetch")
    batch_pipeline_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    batch_pipeline_parser.add_argument("--concurrent-requests", type=int, default=1)
    batch_pipeline_parser.add_argument("--request-wall-timeout-seconds", type=float)
    batch_pipeline_parser.add_argument("--max-retries", type=int, default=3)
    batch_pipeline_parser.add_argument("--pause-seconds", type=float, default=0.0)
    batch_pipeline_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    batch_pipeline_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    batch_pipeline_parser.add_argument("--skip-history-fetch", action="store_true")
    batch_pipeline_parser.add_argument("--skip-orders-fetch", action="store_true")
    batch_pipeline_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    batch_pipeline_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    batch_pipeline_parser.add_argument("--top-n", type=int, default=10)
    batch_pipeline_parser.add_argument("--strategy-set", default="v1-db-region")
    batch_pipeline_parser.add_argument("--parameter-version", default="v1-db")

    benchmark_scan_parser = subparsers.add_parser(
        "benchmark-scan",
        help="Profile coarse-scan throughput and data coverage on the current SQLite market dataset.",
    )
    benchmark_scan_parser.add_argument("--db-path", type=Path, required=True)
    benchmark_scan_parser.add_argument("--output-root", type=Path, required=True)
    benchmark_scan_parser.add_argument("--region-id", type=int, required=True)
    benchmark_scan_parser.add_argument("--type-id", type=int, action="append", default=[])
    benchmark_scan_parser.add_argument("--type-id-file", type=Path)
    benchmark_scan_parser.add_argument("--ready-market-scan-types", action="store_true")
    benchmark_scan_parser.add_argument("--all-published-market-types", action="store_true")
    benchmark_scan_parser.add_argument("--limit", type=int)
    benchmark_scan_parser.add_argument("--chunk-size", type=int, default=1000)
    benchmark_scan_parser.add_argument("--max-chunks", type=int)
    benchmark_scan_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    benchmark_scan_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    benchmark_scan_parser.add_argument("--top-n", type=int, default=10)
    benchmark_scan_parser.add_argument("--strategy-set", default="v1-db-region")
    benchmark_scan_parser.add_argument("--parameter-version", default="v1-db")

    coverage_plan_parser = subparsers.add_parser(
        "plan-market-coverage",
        help="Plan the next history/orders expansion batches for the current market universe.",
    )
    coverage_plan_parser.add_argument("--db-path", type=Path, required=True)
    coverage_plan_parser.add_argument("--output-root", type=Path, required=True)
    coverage_plan_parser.add_argument("--region-id", type=int, required=True)
    coverage_plan_parser.add_argument("--chunk-size", type=int, default=1000)
    coverage_plan_parser.add_argument("--limit", type=int)

    coverage_cycle_parser = subparsers.add_parser(
        "run-coverage-cycle",
        help="Run one traced coverage expansion cycle: plan, history batch, orders batch, then ready benchmark.",
    )
    coverage_cycle_parser.add_argument("--db-path", type=Path, required=True)
    coverage_cycle_parser.add_argument("--output-root", type=Path, required=True)
    coverage_cycle_parser.add_argument("--region-id", type=int, required=True)
    coverage_cycle_parser.add_argument("--history-chunk-size", type=int, default=100)
    coverage_cycle_parser.add_argument("--max-history-chunks", type=int)
    coverage_cycle_parser.add_argument("--orders-chunk-size", type=int, default=100)
    coverage_cycle_parser.add_argument("--max-orders-chunks", type=int)
    coverage_cycle_parser.add_argument("--benchmark-chunk-size", type=int, default=1000)
    coverage_cycle_parser.add_argument("--benchmark-max-chunks", type=int)
    coverage_cycle_parser.add_argument("--datasource", default="tranquility")
    coverage_cycle_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    coverage_cycle_parser.add_argument("--history-user-agent", default="EVEQTrading/0.1 coverage-cycle-history")
    coverage_cycle_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 coverage-cycle-orders")
    coverage_cycle_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    coverage_cycle_parser.add_argument("--concurrent-requests", type=int, default=1)
    coverage_cycle_parser.add_argument("--request-wall-timeout-seconds", type=float)
    coverage_cycle_parser.add_argument("--max-retries", type=int, default=3)
    coverage_cycle_parser.add_argument("--pause-seconds", type=float, default=0.0)
    coverage_cycle_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    coverage_cycle_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    coverage_cycle_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    coverage_cycle_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    coverage_cycle_parser.add_argument("--top-n", type=int, default=10)
    coverage_cycle_parser.add_argument("--strategy-set", default="v1-db-region")
    coverage_cycle_parser.add_argument("--parameter-version", default="v1-db")

    coverage_worker_parser = subparsers.add_parser(
        "run-coverage-worker-once",
        help="Run one guarded coverage worker cycle with lock and state files for server timers.",
    )
    coverage_worker_parser.add_argument("--db-path", type=Path, required=True)
    coverage_worker_parser.add_argument("--output-root", type=Path, required=True)
    coverage_worker_parser.add_argument("--lock-path", type=Path, required=True)
    coverage_worker_parser.add_argument("--state-path", type=Path, required=True)
    coverage_worker_parser.add_argument("--region-id", type=int, required=True)
    coverage_worker_parser.add_argument("--history-chunk-size", type=int, default=100)
    coverage_worker_parser.add_argument("--max-history-chunks", type=int)
    coverage_worker_parser.add_argument("--orders-chunk-size", type=int, default=100)
    coverage_worker_parser.add_argument("--max-orders-chunks", type=int)
    coverage_worker_parser.add_argument("--benchmark-chunk-size", type=int, default=1000)
    coverage_worker_parser.add_argument("--benchmark-max-chunks", type=int)
    coverage_worker_parser.add_argument("--datasource", default="tranquility")
    coverage_worker_parser.add_argument("--esi-base-url", default="https://esi.evetech.net/latest")
    coverage_worker_parser.add_argument("--history-user-agent", default="EVEQTrading/0.1 coverage-worker-history")
    coverage_worker_parser.add_argument("--orders-user-agent", default="EVEQTrading/0.1 coverage-worker-orders")
    coverage_worker_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    coverage_worker_parser.add_argument("--concurrent-requests", type=int, default=1)
    coverage_worker_parser.add_argument("--request-wall-timeout-seconds", type=float)
    coverage_worker_parser.add_argument("--max-retries", type=int, default=3)
    coverage_worker_parser.add_argument("--pause-seconds", type=float, default=0.0)
    coverage_worker_parser.add_argument("--order-type", choices=["all", "buy", "sell"], default="all")
    coverage_worker_parser.add_argument("--orders-fetch-mode", choices=["per_type", "region_pages", "auto"], default="per_type")
    coverage_worker_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    coverage_worker_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    coverage_worker_parser.add_argument("--top-n", type=int, default=10)
    coverage_worker_parser.add_argument("--strategy-set", default="v1-db-region")
    coverage_worker_parser.add_argument("--parameter-version", default="v1-db")
    coverage_worker_parser.add_argument("--failure-backoff-seconds", type=int, default=0)

    retention_cleanup_parser = subparsers.add_parser(
        "run-retention-cleanup",
        help="Delete old runs, logs, staging files, and previous DB backups with a traced cleanup artifact.",
    )
    retention_cleanup_parser.add_argument("--output-root", type=Path, required=True)
    retention_cleanup_parser.add_argument("--log-dir", type=Path)
    retention_cleanup_parser.add_argument("--staging-dir", type=Path)
    retention_cleanup_parser.add_argument("--db-path", type=Path)
    retention_cleanup_parser.add_argument("--retain-run-count", type=int, default=200)
    retention_cleanup_parser.add_argument("--retain-log-count", type=int, default=100)
    retention_cleanup_parser.add_argument("--retain-sync-report-count", type=int, default=100)
    retention_cleanup_parser.add_argument("--staging-max-age-hours", type=int)
    retention_cleanup_parser.add_argument("--prev-db-max-age-hours", type=int)
    retention_cleanup_parser.add_argument("--dry-run", action="store_true")

    report_market_parser = subparsers.add_parser(
        "report-market",
        help="Read the latest traced market scan state from SQLite and saved run artifacts.",
    )
    report_market_parser.add_argument("--db-path", type=Path, required=True)
    report_market_parser.add_argument("--runs-root", type=Path)
    report_market_parser.add_argument("--log-dir", type=Path)
    report_market_parser.add_argument("--scan-run-id")
    report_market_parser.add_argument("--prefer-latest-selected", action="store_true")
    report_market_parser.add_argument("--top-opportunities", type=int, default=10)
    report_market_parser.add_argument("--top-near-misses", type=int, default=5)

    report_jita_passive_cache_parser = subparsers.add_parser(
        "report-jita-passive-cache",
        help="Read the latest jita passive cache cycle and return the current warm-pool opportunity summary.",
    )
    report_jita_passive_cache_parser.add_argument("--db-path", type=Path, required=True)
    report_jita_passive_cache_parser.add_argument("--runs-root", type=Path)
    report_jita_passive_cache_parser.add_argument("--cycle-run-id")
    report_jita_passive_cache_parser.add_argument("--prefer-latest-selected", action="store_true")
    report_jita_passive_cache_parser.add_argument("--top-candidates", type=int, default=10)

    explain_jita_passive_cache_parser = subparsers.add_parser(
        "explain-jita-passive-cache-opportunity",
        help="Explain one cached Jita passive opportunity by resolving the latest cache cycle automatically.",
    )
    explain_jita_passive_cache_parser.add_argument("--db-path", type=Path, required=True)
    explain_jita_passive_cache_parser.add_argument("--runs-root", type=Path)
    explain_jita_passive_cache_parser.add_argument("--log-dir", type=Path)
    explain_jita_passive_cache_parser.add_argument("--cycle-run-id")
    explain_jita_passive_cache_parser.add_argument("--prefer-latest-selected", action="store_true")
    explain_jita_passive_cache_parser.add_argument("--opportunity-id")
    explain_jita_passive_cache_parser.add_argument("--type-id", type=int)

    diagnose_jita_passive_cache_parser = subparsers.add_parser(
        "diagnose-jita-passive-cache",
        help="Diagnose the latest cached Jita passive scan by resolving the cache cycle automatically.",
    )
    diagnose_jita_passive_cache_parser.add_argument("--db-path", type=Path, required=True)
    diagnose_jita_passive_cache_parser.add_argument("--runs-root", type=Path)
    diagnose_jita_passive_cache_parser.add_argument("--log-dir", type=Path)
    diagnose_jita_passive_cache_parser.add_argument("--cycle-run-id")
    diagnose_jita_passive_cache_parser.add_argument("--prefer-latest-selected", action="store_true")
    diagnose_jita_passive_cache_parser.add_argument("--top-rejection-reasons", type=int, default=10)
    diagnose_jita_passive_cache_parser.add_argument("--top-near-passes", type=int, default=10)
    diagnose_jita_passive_cache_parser.add_argument("--top-skipped-reasons", type=int, default=10)

    explain_opportunity_parser = subparsers.add_parser(
        "explain-opportunity",
        help="Explain one selected or rejected opportunity using saved calc traces.",
    )
    explain_opportunity_parser.add_argument("--db-path", type=Path, required=True)
    explain_opportunity_parser.add_argument("--runs-root", type=Path)
    explain_opportunity_parser.add_argument("--log-dir", type=Path)
    explain_opportunity_parser.add_argument("--scan-run-id")
    explain_opportunity_parser.add_argument("--prefer-latest-selected", action="store_true")
    explain_opportunity_parser.add_argument("--opportunity-id")
    explain_opportunity_parser.add_argument("--type-id", type=int)

    advise_market_parser = subparsers.add_parser(
        "advise-market",
        help="Apply the first fine-screen decision layer to selected opportunities and near misses.",
    )
    advise_market_parser.add_argument("--db-path", type=Path, required=True)
    advise_market_parser.add_argument("--runs-root", type=Path)
    advise_market_parser.add_argument("--log-dir", type=Path)
    advise_market_parser.add_argument("--scan-run-id")
    advise_market_parser.add_argument("--prefer-latest-selected", action="store_true")
    advise_market_parser.add_argument("--top-opportunities", type=int, default=10)
    advise_market_parser.add_argument("--top-near-misses", type=int, default=5)
    advise_market_parser.add_argument("--use-latest-account-budget", action="store_true")
    advise_market_parser.add_argument("--account-run-id")
    advise_market_parser.add_argument(
        "--risk-profile",
        choices=["conservative", "standard", "aggressive"],
        default="standard",
    )

    plan_market_entry_parser = subparsers.add_parser(
        "plan-market-entry",
        help="Build a first-wave entry plan for a selected scan using an operator-supplied total budget.",
    )
    plan_market_entry_parser.add_argument("--db-path", type=Path, required=True)
    plan_market_entry_parser.add_argument("--runs-root", type=Path)
    plan_market_entry_parser.add_argument("--log-dir", type=Path)
    plan_market_entry_parser.add_argument("--scan-run-id")
    plan_market_entry_parser.add_argument("--prefer-latest-selected", action="store_true")
    plan_market_entry_parser.add_argument("--total-budget-isk", type=float)
    plan_market_entry_parser.add_argument("--use-latest-account-budget", action="store_true")
    plan_market_entry_parser.add_argument("--account-run-id")
    plan_market_entry_parser.add_argument("--max-candidates", type=int, default=50)
    plan_market_entry_parser.add_argument("--max-positions", type=int, default=12)
    plan_market_entry_parser.add_argument("--near-miss-limit", type=int, default=10)
    plan_market_entry_parser.add_argument(
        "--risk-profile",
        choices=["conservative", "standard", "aggressive"],
        default="standard",
    )

    shortlist_market_parser = subparsers.add_parser(
        "list-market-shortlist",
        help="Build one Chinese shortlist table from selected candidates for operator review.",
    )
    shortlist_market_parser.add_argument("--db-path", type=Path, required=True)
    shortlist_market_parser.add_argument("--runs-root", type=Path)
    shortlist_market_parser.add_argument("--log-dir", type=Path)
    shortlist_market_parser.add_argument("--scan-run-id")
    shortlist_market_parser.add_argument("--prefer-latest-selected", action="store_true")
    shortlist_market_parser.add_argument("--total-budget-isk", type=float)
    shortlist_market_parser.add_argument("--use-latest-account-budget", action="store_true")
    shortlist_market_parser.add_argument("--account-run-id")
    shortlist_market_parser.add_argument("--top-opportunities", type=int, default=50)
    shortlist_market_parser.add_argument("--top-near-misses", type=int, default=10)
    shortlist_market_parser.add_argument("--max-rows", type=int, default=20)
    shortlist_market_parser.add_argument(
        "--risk-profile",
        choices=["conservative", "standard", "aggressive"],
        default="standard",
    )
    shortlist_market_parser.add_argument(
        "--report-profile",
        choices=["generic", "jita-passive"],
        default="generic",
    )

    shortlist_jita_passive_parser = subparsers.add_parser(
        "list-jita-passive-shortlist",
        help="Build one guarded Chinese shortlist table only from Jita passive scan results.",
    )
    shortlist_jita_passive_parser.add_argument("--db-path", type=Path, required=True)
    shortlist_jita_passive_parser.add_argument("--runs-root", type=Path)
    shortlist_jita_passive_parser.add_argument("--log-dir", type=Path)
    shortlist_jita_passive_parser.add_argument("--scan-run-id")
    shortlist_jita_passive_parser.add_argument("--prefer-latest-selected", action="store_true")
    shortlist_jita_passive_parser.add_argument("--total-budget-isk", type=float)
    shortlist_jita_passive_parser.add_argument("--use-latest-account-budget", action="store_true")
    shortlist_jita_passive_parser.add_argument("--account-run-id")
    shortlist_jita_passive_parser.add_argument("--top-opportunities", type=int, default=50)
    shortlist_jita_passive_parser.add_argument("--top-near-misses", type=int, default=10)
    shortlist_jita_passive_parser.add_argument("--max-rows", type=int, default=20)
    shortlist_jita_passive_parser.add_argument(
        "--risk-profile",
        choices=["conservative", "standard", "aggressive"],
        default="standard",
    )

    advise_jita_passive_cache_parser = subparsers.add_parser(
        "advise-jita-passive-cache",
        help="Apply the advice layer to the latest cached Jita passive scan by resolving the cache cycle automatically.",
    )
    advise_jita_passive_cache_parser.add_argument("--db-path", type=Path, required=True)
    advise_jita_passive_cache_parser.add_argument("--runs-root", type=Path)
    advise_jita_passive_cache_parser.add_argument("--log-dir", type=Path)
    advise_jita_passive_cache_parser.add_argument("--cycle-run-id")
    advise_jita_passive_cache_parser.add_argument("--prefer-latest-selected", action="store_true")
    advise_jita_passive_cache_parser.add_argument("--top-opportunities", type=int, default=10)
    advise_jita_passive_cache_parser.add_argument("--top-near-misses", type=int, default=5)
    advise_jita_passive_cache_parser.add_argument("--use-latest-account-budget", action="store_true")
    advise_jita_passive_cache_parser.add_argument("--account-run-id")
    advise_jita_passive_cache_parser.add_argument(
        "--risk-profile",
        choices=["conservative", "standard", "aggressive"],
        default="standard",
    )

    brief_jita_passive_cache_parser = subparsers.add_parser(
        "brief-jita-passive-cache",
        help="Build one operator-facing brief for the latest cached Jita passive scan.",
    )
    brief_jita_passive_cache_parser.add_argument("--db-path", type=Path, required=True)
    brief_jita_passive_cache_parser.add_argument("--runs-root", type=Path)
    brief_jita_passive_cache_parser.add_argument("--log-dir", type=Path)
    brief_jita_passive_cache_parser.add_argument("--cycle-run-id")
    brief_jita_passive_cache_parser.add_argument("--prefer-latest-selected", action="store_true")
    brief_jita_passive_cache_parser.add_argument("--top-candidates", type=int, default=10)
    brief_jita_passive_cache_parser.add_argument("--top-opportunities", type=int, default=5)
    brief_jita_passive_cache_parser.add_argument("--top-near-misses", type=int, default=5)
    brief_jita_passive_cache_parser.add_argument("--top-rejection-reasons", type=int, default=5)
    brief_jita_passive_cache_parser.add_argument("--top-near-passes", type=int, default=5)
    brief_jita_passive_cache_parser.add_argument("--top-skipped-reasons", type=int, default=5)
    brief_jita_passive_cache_parser.add_argument("--use-latest-account-budget", action="store_true")
    brief_jita_passive_cache_parser.add_argument("--account-run-id")
    brief_jita_passive_cache_parser.add_argument(
        "--risk-profile",
        choices=["conservative", "standard", "aggressive"],
        default="standard",
    )
    brief_jita_passive_cache_parser.add_argument("--compare-risk-profiles", action="store_true")

    report_jita_reservoir_parser = subparsers.add_parser(
        "report-jita-reservoir",
        help="Build a markdown report for the formal reservoir strategy defined by the scan pipeline.",
    )
    report_jita_reservoir_parser.add_argument("--db-path", type=Path, required=True)
    report_jita_reservoir_parser.add_argument("--runs-root", type=Path)
    report_jita_reservoir_parser.add_argument("--log-dir", type=Path)
    report_jita_reservoir_parser.add_argument("--output-root", type=Path)
    report_jita_reservoir_parser.add_argument("--scan-run-id")
    report_jita_reservoir_parser.add_argument("--prefer-latest-selected", action="store_true")
    report_jita_reservoir_parser.add_argument("--top-selected", type=int, default=20)
    report_jita_reservoir_parser.add_argument("--top-near-misses", type=int, default=20)

    diagnose_scan_parser = subparsers.add_parser(
        "diagnose-scan",
        help="Diagnose why a traced scan produced few or no selected opportunities.",
    )
    diagnose_scan_parser.add_argument("--db-path", type=Path, required=True)
    diagnose_scan_parser.add_argument("--runs-root", type=Path)
    diagnose_scan_parser.add_argument("--log-dir", type=Path)
    diagnose_scan_parser.add_argument("--scan-run-id")
    diagnose_scan_parser.add_argument("--prefer-latest-selected", action="store_true")
    diagnose_scan_parser.add_argument("--top-rejection-reasons", type=int, default=5)
    diagnose_scan_parser.add_argument("--top-near-passes", type=int, default=5)
    diagnose_scan_parser.add_argument("--top-skipped-reasons", type=int, default=5)

    diagnose_coverage_parser = subparsers.add_parser(
        "diagnose-coverage",
        help="Diagnose coverage expansion progress, runtime target gap, and likely bottlenecks.",
    )
    diagnose_coverage_parser.add_argument("--db-path", type=Path, required=True)
    diagnose_coverage_parser.add_argument("--runs-root", type=Path)
    diagnose_coverage_parser.add_argument("--log-dir", type=Path)
    diagnose_coverage_parser.add_argument("--scan-run-id")
    diagnose_coverage_parser.add_argument("--prefer-latest-selected", action="store_true")

    account_snapshot_parser = subparsers.add_parser(
        "capture-character-account",
        help="Capture one traced authenticated character account snapshot using the standalone SSO and ESI layers.",
    )
    account_snapshot_parser.add_argument("--output-root", type=Path, required=True)
    account_snapshot_parser.add_argument("--client-id", default=os.getenv("EVE_SSO_CLIENT_ID"))
    account_snapshot_parser.add_argument(
        "--callback-url",
        default=os.getenv("EVE_SSO_CALLBACK_URL", "http://127.0.0.1:8080/callback"),
    )
    account_snapshot_parser.add_argument(
        "--store",
        type=Path,
        default=Path(os.getenv("EVE_SSO_STORE_PATH", str(Path.home() / ".eve-sso" / "tokens.json"))),
    )
    account_snapshot_parser.add_argument(
        "--sso-base-url",
        default=os.getenv("EVE_SSO_BASE_URL", "https://login.eveonline.com"),
    )
    account_snapshot_parser.add_argument(
        "--esi-base-url",
        default=os.getenv("EVE_ESI_BASE_URL", "https://esi.evetech.net/latest"),
    )
    account_snapshot_parser.add_argument("--datasource", default=os.getenv("EVE_ESI_DATASOURCE", "tranquility"))
    account_snapshot_parser.add_argument(
        "--user-agent",
        default=os.getenv("EVE_ESI_USER_AGENT", "EVEQTrading/0.1 character-account"),
    )
    account_snapshot_parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    account_snapshot_parser.add_argument("--compatibility-date", default=os.getenv("EVE_ESI_COMPATIBILITY_DATE"))
    account_snapshot_parser.add_argument("--tenant", default=os.getenv("EVE_ESI_TENANT"))
    account_snapshot_parser.add_argument("--accept-language", default=os.getenv("EVE_ESI_ACCEPT_LANGUAGE"))
    account_snapshot_parser.add_argument("--character-id", type=int)
    account_snapshot_parser.add_argument("--character-name")
    account_snapshot_parser.add_argument("--owner-hash")

    account_report_parser = subparsers.add_parser(
        "report-character-account",
        help="Read the latest or a specified character account snapshot and build a human-facing funding report.",
    )
    account_report_parser.add_argument("--db-path", type=Path, required=True)
    account_report_parser.add_argument("--runs-root", type=Path)
    account_report_parser.add_argument("--output-root", type=Path, required=True)
    account_report_parser.add_argument("--account-run-id")

    run_scan_parser = subparsers.add_parser("run-scan", help="Run a traced coarse-screen scan from JSON input.")
    run_scan_parser.add_argument("--input-path", type=Path, required=True)
    run_scan_parser.add_argument("--output-root", type=Path, required=True)
    run_scan_parser.add_argument("--db-path", type=Path)
    run_scan_parser.add_argument("--capital-budget-isk", type=float, default=1_000_000_000.0)
    run_scan_parser.add_argument("--test-position-isk", type=float, default=250_000_000.0)
    run_scan_parser.add_argument("--top-n", type=int, default=3)
    run_scan_parser.add_argument("--strategy-set", default="v1-default")
    run_scan_parser.add_argument("--parameter-version", default="v1")

    return parser


def cmd_init_db(args: argparse.Namespace) -> int:
    init_db(args.db_path, args.schema_path)
    print(f"Initialized database at {args.db_path}")
    return 0


def cmd_import_sde(args: argparse.Namespace) -> int:
    import_run_id = create_run_id()
    with connect(args.db_path) as connection:
        summary = import_sde_reference_data(
            connection,
            args.sde_root,
            language=args.language,
            import_run_id=import_run_id,
        )
    print(f"SDE import completed: {import_run_id}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def cmd_capture_character_account(args: argparse.Namespace) -> int:
    snapshot = capture_character_account_snapshot(
        output_root=args.output_root,
        client_id=args.client_id,
        callback_url=args.callback_url,
        store_path=args.store,
        sso_base_url=args.sso_base_url,
        esi_base_url=args.esi_base_url,
        datasource=args.datasource,
        user_agent=args.user_agent,
        request_timeout_seconds=args.request_timeout_seconds,
        compatibility_date=args.compatibility_date,
        tenant=args.tenant,
        accept_language=args.accept_language,
        character_id=args.character_id,
        character_name=args.character_name,
        owner_hash=args.owner_hash,
    )
    print(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_report_character_account(args: argparse.Namespace) -> int:
    report = build_character_account_report(
        db_path=args.db_path,
        runs_root=args.runs_root,
        output_root=args.output_root,
        account_snapshot_run_id=args.account_run_id,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_fetch_market_history(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        type_targets = resolve_history_fetch_targets(
            connection,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
        )
        result = fetch_market_history(
            connection,
            output_root=args.output_root,
            type_targets=type_targets,
            config=HistoryFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                request_wall_timeout_seconds=args.request_wall_timeout_seconds,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
            ),
        )
    print(f"History fetch completed: {result['run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["fetch_run"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_history_batch_fetch(args: argparse.Namespace) -> int:
    if not (args.type_id or args.type_id_file or args.all_published_market_types):
        raise SystemExit(
            "run-history-batch-fetch requires a target selector: use --type-id, --type-id-file, or --all-published-market-types."
        )

    with connect(args.db_path) as connection:
        type_targets = resolve_history_fetch_targets(
            connection,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
        )
        result = run_history_batch_fetch(
            connection,
            output_root=args.output_root,
            type_targets=type_targets,
            config=HistoryFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                request_wall_timeout_seconds=args.request_wall_timeout_seconds,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
            ),
            chunk_size=args.chunk_size,
            max_chunks=args.max_chunks,
        )
    print(f"History batch fetch completed: {result['batch_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_fetch_market_orders(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        type_targets = resolve_order_fetch_targets(
            connection,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
        )
        result = fetch_market_orders(
            connection,
            output_root=args.output_root,
            type_targets=type_targets,
            config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
        )
    print(f"Orders fetch completed: {result['run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["fetch_run"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_orders_batch_fetch(args: argparse.Namespace) -> int:
    if not (args.type_id or args.type_id_file or args.all_published_market_types):
        raise SystemExit(
            "run-orders-batch-fetch requires a target selector: use --type-id, --type-id-file, or --all-published-market-types."
        )

    with connect(args.db_path) as connection:
        type_targets = resolve_order_fetch_targets(
            connection,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
        )
        result = run_order_batch_fetch(
            connection,
            output_root=args.output_root,
            type_targets=type_targets,
            config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
            chunk_size=args.chunk_size,
            max_chunks=args.max_chunks,
        )
    print(f"Orders batch fetch completed: {result['batch_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_plan_order_refresh(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        plan = plan_order_refresh(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            profile_name=args.orders_cache_profile,
            order_type=args.order_type,
            limit=args.limit,
        )
    print(f"Order refresh plan completed: {plan['order_refresh_plan_run_id']}")
    print(f"Artifacts: {args.output_root / plan['order_refresh_plan_run_id']}")
    print(json.dumps(plan, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_region_order_mirror(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_region_order_mirror_once(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            target_universe=args.target_universe,
            orders_config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode="region_pages",
                ignore_existing_freshness=True,
            ),
            limit=args.limit,
        )
    print(f"Region order mirror completed: {result['mirror_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_order_refresh_cycle(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_order_refresh_cycle(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            orders_chunk_size=args.orders_chunk_size,
            max_orders_chunks=args.max_orders_chunks,
            limit=args.limit,
            profile_name=args.orders_cache_profile,
            orders_config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.orders_user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
        )
    print(f"Order refresh cycle completed: {result['cycle_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2))
    return 0


def cmd_run_region_order_mirror_worker_once(args: argparse.Namespace) -> int:
    result = run_region_order_mirror_worker_once(
        db_path=args.db_path,
        output_root=args.output_root,
        lock_path=args.lock_path,
        state_path=args.state_path,
        region_id=args.region_id,
        target_universe=args.target_universe,
        refresh_interval_minutes=args.refresh_interval_minutes,
        orders_config=OrderFetchConfig(
            region_id=args.region_id,
            datasource=args.datasource,
            esi_base_url=args.esi_base_url,
            user_agent=args.user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            concurrent_requests=args.concurrent_requests,
            max_retries=args.max_retries,
            pause_seconds=args.pause_seconds,
            order_type=args.order_type,
            fetch_mode="region_pages",
            ignore_existing_freshness=True,
        ),
        limit=args.limit,
    )
    print(f"Region order mirror worker result: {result['worker_run_id']}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] not in {"skipped_locked", "skipped_fresh"} else 3


def cmd_run_order_refresh_worker_once(args: argparse.Namespace) -> int:
    result = run_order_refresh_worker_once(
        db_path=args.db_path,
        output_root=args.output_root,
        lock_path=args.lock_path,
        state_path=args.state_path,
        region_id=args.region_id,
        orders_chunk_size=args.orders_chunk_size,
        max_orders_chunks=args.max_orders_chunks,
        profile_name=args.orders_cache_profile,
        orders_config=OrderFetchConfig(
            region_id=args.region_id,
            datasource=args.datasource,
            esi_base_url=args.esi_base_url,
            user_agent=args.orders_user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            concurrent_requests=args.concurrent_requests,
            max_retries=args.max_retries,
            pause_seconds=args.pause_seconds,
            order_type=args.order_type,
            fetch_mode=args.orders_fetch_mode,
        ),
        limit=args.limit,
        failure_backoff_seconds=args.failure_backoff_seconds,
        max_runtime_seconds=args.max_runtime_seconds,
        max_cycles_per_run=args.max_cycles_per_run,
    )
    print(f"Order refresh worker result: {result['worker_run_id']}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] not in {"skipped_locked", "skipped_backoff"} else 3


def cmd_plan_history_sync(args: argparse.Namespace) -> int:
    run_id = create_run_id(prefix="history-sync-plan")
    run_dir = ensure_run_dir(args.output_root, run_id)
    with connect(args.db_path) as connection:
        snapshot = collect_history_sync_snapshot(
            connection,
            region_id=args.region_id,
            refresh_interval_hours=args.refresh_interval_hours,
            not_found_recheck_hours=args.not_found_recheck_hours,
            limit=args.limit,
        )
    payload = {
        "history_sync_plan_run_id": run_id,
        **snapshot,
        "files": {
            "backfill_due_type_ids_path": str(run_dir / "backfill_due_type_ids.txt"),
            "refresh_due_type_ids_path": str(run_dir / "refresh_due_type_ids.txt"),
        },
    }
    stdout_payload = {
        "history_sync_plan_run_id": run_id,
        "generated_at_utc": snapshot["generated_at_utc"],
        "region_id": snapshot["region_id"],
        "refresh_interval_hours": snapshot["refresh_interval_hours"],
        "not_found_recheck_hours": snapshot["not_found_recheck_hours"],
        "target_counts": snapshot["target_counts"],
        "files": payload["files"],
    }
    write_json(run_dir / "history_sync_plan.json", payload)
    write_summary(
        run_dir / "summary.md",
        [
            "# EVEQTrading History Sync Plan",
            "",
            f"- Plan Run ID: `{run_id}`",
            f"- Region ID: `{args.region_id}`",
            f"- Market Types Considered: `{snapshot['target_counts']['market_type_count']}`",
            f"- Backfill Due: `{snapshot['target_counts']['backfill_due_count']}`",
            f"- Backfill Wait: `{snapshot['target_counts']['backfill_wait_count']}`",
            f"- Refresh Due: `{snapshot['target_counts']['refresh_due_count']}`",
            f"- Refresh Wait: `{snapshot['target_counts']['refresh_wait_count']}`",
            f"- Healthy: `{snapshot['target_counts']['healthy_count']}`",
        ],
    )
    Path(payload["files"]["backfill_due_type_ids_path"]).write_text(
        "".join(f"{type_id}\n" for type_id in snapshot["type_ids"]["backfill_due"]),
        encoding="utf-8",
    )
    Path(payload["files"]["refresh_due_type_ids_path"]).write_text(
        "".join(f"{type_id}\n" for type_id in snapshot["type_ids"]["refresh_due"]),
        encoding="utf-8",
    )
    print(f"History sync plan completed: {run_id}")
    print(f"Artifacts: {run_dir}")
    print(json.dumps(stdout_payload, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_history_sync_cycle(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_history_sync_cycle(
            connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            backfill_chunk_size=args.backfill_chunk_size,
            max_backfill_chunks=args.max_backfill_chunks,
            refresh_chunk_size=args.refresh_chunk_size,
            max_refresh_chunks=args.max_refresh_chunks,
            history_config=HistoryFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                request_wall_timeout_seconds=args.request_wall_timeout_seconds,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                failure_backoff_seconds=args.failure_backoff_seconds,
            ),
            refresh_interval_hours=args.refresh_interval_hours,
            not_found_recheck_hours=args.not_found_recheck_hours,
            limit=args.limit,
        )
    print(f"History sync cycle completed: {result['cycle_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_history_sync_worker_once(args: argparse.Namespace) -> int:
    result = run_history_sync_worker_once(
        db_path=args.db_path,
        output_root=args.output_root,
        lock_path=args.lock_path,
        state_path=args.state_path,
        region_id=args.region_id,
        backfill_chunk_size=args.backfill_chunk_size,
        max_backfill_chunks=args.max_backfill_chunks,
        refresh_chunk_size=args.refresh_chunk_size,
        max_refresh_chunks=args.max_refresh_chunks,
        history_config=HistoryFetchConfig(
            region_id=args.region_id,
            datasource=args.datasource,
            esi_base_url=args.esi_base_url,
            user_agent=args.user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            concurrent_requests=args.concurrent_requests,
            request_wall_timeout_seconds=args.request_wall_timeout_seconds,
            max_retries=args.max_retries,
            pause_seconds=args.pause_seconds,
        ),
        refresh_interval_hours=args.refresh_interval_hours,
        not_found_recheck_hours=args.not_found_recheck_hours,
        limit=args.limit,
        failure_backoff_seconds=args.failure_backoff_seconds,
        max_runtime_seconds=args.max_runtime_seconds,
        max_cycles_per_run=args.max_cycles_per_run,
    )
    print(f"History sync worker completed: {result['worker_run_id']}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] not in {"skipped_locked", "skipped_backoff"} else 3


def cmd_run_db_scan(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = execute_db_scan(
            connection=connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=args.ready_market_scan_types,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
            capital_budget_isk=args.capital_budget_isk,
            test_position_isk=args.test_position_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
        )
    print(f"DB scan completed: {result['run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(
        json.dumps(
            {
                "candidate_count_built": len(result["candidate_build"].payload["candidates"]),
                "candidate_count_selected": len(result["selected_candidates"]),
                "skipped_count": len(result["candidate_build"].skipped_rows),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_scan_jita_passive_simple(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_jita_passive_simple_scan(
            connection=connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=args.ready_market_scan_types,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
            capital_budget_isk=args.capital_budget_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
            fee_config=FeeConfig(),
            filter_config=JitaPassiveSimpleFilterConfig(
                max_flip_units_cv_30d=args.max_flip_units_cv_30d,
                max_flip_units_cv_7d=args.max_flip_units_cv_7d,
                min_daily_capacity_isk_30d=args.min_daily_capacity_isk_30d,
                max_unit_buy_cost_isk=args.max_unit_buy_cost_isk,
            ),
            orders_cache_profile=args.orders_cache_profile,
            order_type=args.order_type,
            cache_only=args.cache_only,
        )
    print(f"Jita passive simple scan completed: {result['run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(
        json.dumps(
            {
                "candidate_count_built": result["analysis_run"]["candidate_count_total"],
                "candidate_count_selected": len(result["selected_candidates"]),
                "skipped_count": len(result["skipped_rows"]),
                "cache_selection": result["cache_selection"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_scan_jita_passive_full(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_jita_passive_simple_scan(
            connection=connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=args.ready_market_scan_types,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
            capital_budget_isk=args.capital_budget_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
            fee_config=FeeConfig(),
            filter_config=JitaPassiveSimpleFilterConfig(
                max_flip_units_cv_30d=args.max_flip_units_cv_30d,
                max_flip_units_cv_7d=args.max_flip_units_cv_7d,
                min_daily_capacity_isk_30d=args.min_daily_capacity_isk_30d,
                max_unit_buy_cost_isk=args.max_unit_buy_cost_isk,
            ),
            orders_cache_profile=None,
            order_type="all",
            cache_only=False,
        )
    print(f"Jita passive full scan completed: {result['run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(
        json.dumps(
            {
                "candidate_count_built": result["analysis_run"]["candidate_count_total"],
                "candidate_count_selected": len(result["selected_candidates"]),
                "skipped_count": len(result["skipped_rows"]),
                "universe_summary": result["universe_summary"],
                "filter_reason_counts": result["filter_reason_counts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_run_jita_passive_cycle(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_jita_passive_cycle(
            connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            orders_chunk_size=args.orders_chunk_size,
            max_orders_chunks=args.max_orders_chunks,
            limit=args.limit,
            orders_cache_profile=args.orders_cache_profile,
            orders_config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.orders_user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
            capital_budget_isk=args.capital_budget_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
            fee_config=FeeConfig(),
            filter_config=JitaPassiveSimpleFilterConfig(
                max_flip_units_cv_30d=args.max_flip_units_cv_30d,
                max_flip_units_cv_7d=args.max_flip_units_cv_7d,
                min_daily_capacity_isk_30d=args.min_daily_capacity_isk_30d,
                max_unit_buy_cost_isk=args.max_unit_buy_cost_isk,
            ),
        )
    print(f"Jita passive cycle completed: {result['cycle_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2))
    return 0


def cmd_run_jita_passive_cache_cycle(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_jita_passive_cache_cycle(
            connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            orders_chunk_size=args.orders_chunk_size,
            max_orders_chunks=args.max_orders_chunks,
            limit=args.limit,
            orders_cache_profile=args.orders_cache_profile,
            orders_config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.orders_user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
            capital_budget_isk=args.capital_budget_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
            fee_config=FeeConfig(),
            filter_config=JitaPassiveSimpleFilterConfig(
                max_flip_units_cv_30d=args.max_flip_units_cv_30d,
                max_flip_units_cv_7d=args.max_flip_units_cv_7d,
                min_daily_capacity_isk_30d=args.min_daily_capacity_isk_30d,
                max_unit_buy_cost_isk=args.max_unit_buy_cost_isk,
            ),
        )
    print(f"Jita passive cache cycle completed: {result['cycle_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2))
    return 0


def cmd_run_jita_passive_cache_worker_once(args: argparse.Namespace) -> int:
    result = run_jita_passive_cache_worker_once(
        db_path=args.db_path,
        output_root=args.output_root,
        lock_path=args.lock_path,
        state_path=args.state_path,
        region_id=args.region_id,
        orders_chunk_size=args.orders_chunk_size,
        max_orders_chunks=args.max_orders_chunks,
        orders_cache_profile=args.orders_cache_profile,
        orders_config=OrderFetchConfig(
            region_id=args.region_id,
            datasource=args.datasource,
            esi_base_url=args.esi_base_url,
            user_agent=args.orders_user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            concurrent_requests=args.concurrent_requests,
            max_retries=args.max_retries,
            pause_seconds=args.pause_seconds,
            order_type=args.order_type,
            fetch_mode=args.orders_fetch_mode,
        ),
        capital_budget_isk=args.capital_budget_isk,
        top_n=args.top_n,
        strategy_set=args.strategy_set,
        parameter_version=args.parameter_version,
        limit=args.limit,
        fee_config=FeeConfig(),
        filter_config=JitaPassiveSimpleFilterConfig(
            max_flip_units_cv_30d=args.max_flip_units_cv_30d,
            max_flip_units_cv_7d=args.max_flip_units_cv_7d,
            min_daily_capacity_isk_30d=args.min_daily_capacity_isk_30d,
            max_unit_buy_cost_isk=args.max_unit_buy_cost_isk,
        ),
        failure_backoff_seconds=args.failure_backoff_seconds,
        max_runtime_seconds=args.max_runtime_seconds,
        max_cycles_per_run=args.max_cycles_per_run,
    )
    print(f"Jita passive cache worker result: {result['worker_run_id']}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] not in {"skipped_locked", "skipped_backoff"} else 3


def cmd_run_region_pipeline(args: argparse.Namespace) -> int:
    if not (args.type_id or args.type_id_file or args.all_published_market_types):
        raise SystemExit(
            "run-region-pipeline requires a target selector: use --type-id, --type-id-file, or --all-published-market-types."
        )

    pipeline_run_id = create_run_id(prefix="region-pipeline")
    pipeline_run_dir = ensure_run_dir(args.output_root, pipeline_run_id)

    with connect(args.db_path) as connection:
        selected_targets = resolve_db_scan_targets(
            connection,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=False,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
        ) or []

        history_result = None
        if not args.skip_history_fetch:
            history_result = fetch_market_history(
                connection,
                output_root=args.output_root,
                type_targets=selected_targets,
                config=HistoryFetchConfig(
                    region_id=args.region_id,
                    datasource=args.datasource,
                    esi_base_url=args.esi_base_url,
                    user_agent=args.history_user_agent,
                    request_timeout_seconds=args.request_timeout_seconds,
                    concurrent_requests=args.concurrent_requests,
                    request_wall_timeout_seconds=args.request_wall_timeout_seconds,
                    max_retries=args.max_retries,
                    pause_seconds=args.pause_seconds,
                ),
            )

        orders_result = None
        if not args.skip_orders_fetch:
            orders_result = fetch_market_orders(
                connection,
                output_root=args.output_root,
                type_targets=selected_targets,
                config=OrderFetchConfig(
                    region_id=args.region_id,
                    datasource=args.datasource,
                    esi_base_url=args.esi_base_url,
                    user_agent=args.orders_user_agent,
                    request_timeout_seconds=args.request_timeout_seconds,
                    concurrent_requests=args.concurrent_requests,
                    max_retries=args.max_retries,
                    pause_seconds=args.pause_seconds,
                    order_type=args.order_type,
                    fetch_mode=args.orders_fetch_mode,
                ),
            )

        scan_result = execute_db_scan(
            connection=connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=False,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
            capital_budget_isk=args.capital_budget_isk,
            test_position_isk=args.test_position_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
        )

    pipeline_payload = {
        "pipeline_run_id": pipeline_run_id,
        "region_id": args.region_id,
        "target_count": len(selected_targets),
        "history_run_id": history_result["run_id"] if history_result else None,
        "orders_run_id": orders_result["run_id"] if orders_result else None,
        "scan_run_id": scan_result["run_id"],
        "history_run_dir": str(history_result["run_dir"]) if history_result else None,
        "orders_run_dir": str(orders_result["run_dir"]) if orders_result else None,
        "scan_run_dir": str(scan_result["run_dir"]),
        "selection_mode": selected_targets[0]["selection_mode"] if selected_targets else "none",
        "config": {
            "datasource": args.datasource,
            "esi_base_url": args.esi_base_url,
            "order_type": args.order_type,
            "capital_budget_isk": args.capital_budget_isk,
            "test_position_isk": args.test_position_isk,
            "top_n": args.top_n,
            "strategy_set": args.strategy_set,
            "parameter_version": args.parameter_version,
        },
    }
    summary_lines = [
        "# EVEQTrading Region Pipeline",
        "",
        f"- Pipeline Run ID: `{pipeline_run_id}`",
        f"- Region ID: `{args.region_id}`",
        f"- Target Count: `{len(selected_targets)}`",
        f"- History Run ID: `{pipeline_payload['history_run_id']}`",
        f"- Orders Run ID: `{pipeline_payload['orders_run_id']}`",
        f"- Scan Run ID: `{pipeline_payload['scan_run_id']}`",
        f"- Scan Candidates Built: `{len(scan_result['candidate_build'].payload['candidates'])}`",
        f"- Scan Candidates Selected: `{len(scan_result['selected_candidates'])}`",
    ]
    write_json(pipeline_run_dir / "pipeline_run.json", pipeline_payload)
    write_summary(pipeline_run_dir / "summary.md", summary_lines)

    print(f"Region pipeline completed: {pipeline_run_id}")
    print(f"Artifacts: {pipeline_run_dir}")
    print(json.dumps(pipeline_payload, ensure_ascii=False, indent=2))
    return 0


def cmd_run_region_batch_pipeline(args: argparse.Namespace) -> int:
    if not (args.type_id or args.type_id_file or args.all_published_market_types):
        raise SystemExit(
            "run-region-batch-pipeline requires a target selector: use --type-id, --type-id-file, or --all-published-market-types."
        )

    batch_run_id = create_run_id(prefix="region-batch-pipeline")
    batch_run_dir = ensure_run_dir(args.output_root, batch_run_id)

    with connect(args.db_path) as connection:
        selected_targets = resolve_db_scan_targets(
            connection,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=False,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
        ) or []

        chunks = chunked(selected_targets, args.chunk_size)
        if args.max_chunks is not None:
            chunks = chunks[: args.max_chunks]

        chunk_results: list[dict[str, object]] = []
        totals = {
            "chunk_count": len(chunks),
            "target_count": sum(len(chunk) for chunk in chunks),
            "history_runs": 0,
            "orders_runs": 0,
            "scan_runs": 0,
            "candidate_count_built": 0,
            "candidate_count_selected": 0,
            "skipped_count": 0,
        }

        for chunk_index, chunk_targets in enumerate(chunks, start=1):
            explicit_type_ids = [int(row["type_id"]) for row in chunk_targets]

            history_result = None
            if not args.skip_history_fetch:
                history_result = fetch_market_history(
                    connection,
                    output_root=args.output_root,
                    type_targets=chunk_targets,
                    config=HistoryFetchConfig(
                        region_id=args.region_id,
                        datasource=args.datasource,
                        esi_base_url=args.esi_base_url,
                        user_agent=args.history_user_agent,
                        request_timeout_seconds=args.request_timeout_seconds,
                        concurrent_requests=args.concurrent_requests,
                        request_wall_timeout_seconds=args.request_wall_timeout_seconds,
                        max_retries=args.max_retries,
                        pause_seconds=args.pause_seconds,
                    ),
                )
                totals["history_runs"] += 1

            orders_result = None
            if not args.skip_orders_fetch:
                orders_result = fetch_market_orders(
                    connection,
                    output_root=args.output_root,
                    type_targets=chunk_targets,
                config=OrderFetchConfig(
                    region_id=args.region_id,
                    datasource=args.datasource,
                    esi_base_url=args.esi_base_url,
                    user_agent=args.orders_user_agent,
                    request_timeout_seconds=args.request_timeout_seconds,
                    concurrent_requests=args.concurrent_requests,
                    max_retries=args.max_retries,
                    pause_seconds=args.pause_seconds,
                    order_type=args.order_type,
                    fetch_mode=args.orders_fetch_mode,
                ),
                )
                totals["orders_runs"] += 1

            scan_result = execute_db_scan(
                connection=connection,
                db_path=args.db_path,
                output_root=args.output_root,
                region_id=args.region_id,
                explicit_type_ids=explicit_type_ids,
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=False,
                limit=None,
                capital_budget_isk=args.capital_budget_isk,
                test_position_isk=args.test_position_isk,
                top_n=args.top_n,
                strategy_set=args.strategy_set,
                parameter_version=args.parameter_version,
            )
            totals["scan_runs"] += 1
            totals["candidate_count_built"] += len(scan_result["candidate_build"].payload["candidates"])
            totals["candidate_count_selected"] += len(scan_result["selected_candidates"])
            totals["skipped_count"] += len(scan_result["candidate_build"].skipped_rows)

            chunk_results.append(
                {
                    "chunk_index": chunk_index,
                    "target_count": len(chunk_targets),
                    "type_ids": explicit_type_ids,
                    "history_run_id": history_result["run_id"] if history_result else None,
                    "orders_run_id": orders_result["run_id"] if orders_result else None,
                    "scan_run_id": scan_result["run_id"],
                    "history_run_dir": str(history_result["run_dir"]) if history_result else None,
                    "orders_run_dir": str(orders_result["run_dir"]) if orders_result else None,
                    "scan_run_dir": str(scan_result["run_dir"]),
                    "candidate_count_built": len(scan_result["candidate_build"].payload["candidates"]),
                    "candidate_count_selected": len(scan_result["selected_candidates"]),
                    "skipped_count": len(scan_result["candidate_build"].skipped_rows),
                }
            )

    batch_payload = {
        "batch_run_id": batch_run_id,
        "region_id": args.region_id,
        "selection_mode": selected_targets[0]["selection_mode"] if selected_targets else "none",
        "chunk_size": args.chunk_size,
        "max_chunks": args.max_chunks,
        "config": {
            "datasource": args.datasource,
            "esi_base_url": args.esi_base_url,
            "order_type": args.order_type,
            "capital_budget_isk": args.capital_budget_isk,
            "test_position_isk": args.test_position_isk,
            "top_n": args.top_n,
            "strategy_set": args.strategy_set,
            "parameter_version": args.parameter_version,
        },
        "totals": totals,
        "chunks": chunk_results,
    }
    summary_lines = [
        "# EVEQTrading Region Batch Pipeline",
        "",
        f"- Batch Run ID: `{batch_run_id}`",
        f"- Region ID: `{args.region_id}`",
        f"- Chunk Size: `{args.chunk_size}`",
        f"- Chunk Count: `{totals['chunk_count']}`",
        f"- Target Count: `{totals['target_count']}`",
        f"- Scan Runs: `{totals['scan_runs']}`",
        f"- Candidates Built: `{totals['candidate_count_built']}`",
        f"- Candidates Selected: `{totals['candidate_count_selected']}`",
        f"- Skipped Count: `{totals['skipped_count']}`",
        "",
        "## Chunk Runs",
    ]
    if not chunk_results:
        summary_lines.extend(["", "No chunks were executed."])
    else:
        for row in chunk_results[:20]:
            summary_lines.extend(
                [
                    "",
                    f"- Chunk `{row['chunk_index']}`",
                    f"  - targets: `{row['target_count']}`",
                    f"  - history_run_id: `{row['history_run_id']}`",
                    f"  - orders_run_id: `{row['orders_run_id']}`",
                    f"  - scan_run_id: `{row['scan_run_id']}`",
                    f"  - candidates_built: `{row['candidate_count_built']}`",
                    f"  - candidates_selected: `{row['candidate_count_selected']}`",
                ]
            )

    write_json(batch_run_dir / "batch_pipeline_run.json", batch_payload)
    write_summary(batch_run_dir / "summary.md", summary_lines)

    print(f"Region batch pipeline completed: {batch_run_id}")
    print(f"Artifacts: {batch_run_dir}")
    print(json.dumps(batch_payload, ensure_ascii=False, indent=2))
    return 0


def cmd_benchmark_scan(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        benchmark = run_scan_benchmark(
            connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            explicit_type_ids=args.type_id,
            type_id_file=args.type_id_file,
            ready_market_scan_types=args.ready_market_scan_types,
            all_published_market_types=args.all_published_market_types,
            limit=args.limit,
            chunk_size=args.chunk_size,
            max_chunks=args.max_chunks,
            capital_budget_isk=args.capital_budget_isk,
            test_position_isk=args.test_position_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
        )
    print(f"Scan benchmark completed: {benchmark['benchmark_run_id']}")
    print(f"Artifacts: {benchmark['run_dir']}")
    print(json.dumps(benchmark, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_plan_market_coverage(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        coverage_plan = plan_market_coverage(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            chunk_size=args.chunk_size,
            limit=args.limit,
        )
    print(f"Coverage plan completed: {coverage_plan['coverage_plan_run_id']}")
    print(f"Artifacts: {args.output_root / coverage_plan['coverage_plan_run_id']}")
    print(json.dumps(coverage_plan, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_coverage_cycle(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        result = run_coverage_cycle(
            connection,
            db_path=args.db_path,
            output_root=args.output_root,
            region_id=args.region_id,
            history_chunk_size=args.history_chunk_size,
            max_history_chunks=args.max_history_chunks,
            orders_chunk_size=args.orders_chunk_size,
            max_orders_chunks=args.max_orders_chunks,
            history_config=HistoryFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.history_user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                request_wall_timeout_seconds=args.request_wall_timeout_seconds,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
            ),
            orders_config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.orders_user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
            benchmark_chunk_size=args.benchmark_chunk_size,
            benchmark_max_chunks=args.benchmark_max_chunks,
            capital_budget_isk=args.capital_budget_isk,
            test_position_isk=args.test_position_isk,
            top_n=args.top_n,
            strategy_set=args.strategy_set,
            parameter_version=args.parameter_version,
        )
    print(f"Coverage cycle completed: {result['cycle_run_id']}")
    print(f"Artifacts: {result['run_dir']}")
    print(json.dumps(result["payload"], ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_coverage_worker_once(args: argparse.Namespace) -> int:
    result = run_coverage_worker_once(
        db_path=args.db_path,
        output_root=args.output_root,
        lock_path=args.lock_path,
        state_path=args.state_path,
        region_id=args.region_id,
        history_chunk_size=args.history_chunk_size,
        max_history_chunks=args.max_history_chunks,
        orders_chunk_size=args.orders_chunk_size,
        max_orders_chunks=args.max_orders_chunks,
        benchmark_chunk_size=args.benchmark_chunk_size,
        benchmark_max_chunks=args.benchmark_max_chunks,
        history_config=HistoryFetchConfig(
            region_id=args.region_id,
            datasource=args.datasource,
            esi_base_url=args.esi_base_url,
            user_agent=args.history_user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            concurrent_requests=args.concurrent_requests,
            request_wall_timeout_seconds=args.request_wall_timeout_seconds,
            max_retries=args.max_retries,
            pause_seconds=args.pause_seconds,
        ),
            orders_config=OrderFetchConfig(
                region_id=args.region_id,
                datasource=args.datasource,
                esi_base_url=args.esi_base_url,
                user_agent=args.orders_user_agent,
                request_timeout_seconds=args.request_timeout_seconds,
                concurrent_requests=args.concurrent_requests,
                max_retries=args.max_retries,
                pause_seconds=args.pause_seconds,
                order_type=args.order_type,
                fetch_mode=args.orders_fetch_mode,
            ),
        capital_budget_isk=args.capital_budget_isk,
        test_position_isk=args.test_position_isk,
        top_n=args.top_n,
        strategy_set=args.strategy_set,
        parameter_version=args.parameter_version,
        failure_backoff_seconds=args.failure_backoff_seconds,
    )
    print(f"Coverage worker result: {result['worker_run_id']}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result["status"] not in {"skipped_locked", "skipped_backoff"} else 3


def cmd_run_retention_cleanup(args: argparse.Namespace) -> int:
    result = run_retention_cleanup(
        output_root=args.output_root,
        log_dir=args.log_dir,
        staging_dir=args.staging_dir,
        db_path=args.db_path,
        retain_run_count=args.retain_run_count,
        retain_log_count=args.retain_log_count,
        retain_sync_report_count=args.retain_sync_report_count,
        staging_max_age_hours=args.staging_max_age_hours,
        prev_db_max_age_hours=args.prev_db_max_age_hours,
        dry_run=args.dry_run,
    )
    print(f"Retention cleanup completed: {result['cleanup_run_id']}")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_run_scan(args: argparse.Namespace) -> int:
    payload = json.loads(args.input_path.read_text(encoding="utf-8"))
    run_id = create_run_id()
    run_dir = ensure_run_dir(args.output_root, run_id)

    fee_config = FeeConfig()
    filter_config = FilterConfig()
    scan_config = ScanConfig(
        strategy_set=args.strategy_set,
        parameter_version=args.parameter_version,
        test_position_isk=args.test_position_isk,
        capital_budget_isk=args.capital_budget_isk,
        top_n=args.top_n,
    )

    analysis_run, calc_trace, selected_candidates, summary_lines = analyze_market_scan(
        payload=payload,
        input_path=args.input_path.resolve(),
        fee_config=fee_config,
        filter_config=filter_config,
        scan_config=scan_config,
    )
    analysis_run["run_id"] = run_id
    analysis_run["output_dir"] = str(run_dir.resolve())
    calc_trace["run_id"] = run_id

    write_json(run_dir / "analysis_run.json", analysis_run)
    write_json(run_dir / "calc_trace.json", calc_trace)
    write_json(run_dir / "candidate_table.json", selected_candidates)
    write_summary(run_dir / "summary.md", summary_lines)

    if args.db_path:
        with connect(args.db_path) as connection:
            persist_scan_run(connection, run_id, analysis_run, selected_candidates, run_dir.resolve())

    print(f"Run completed: {run_id}")
    print(f"Artifacts: {run_dir}")
    return 0


def cmd_report_market(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        report = build_market_report(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_opportunities=args.top_opportunities,
            top_near_misses=args.top_near_misses,
        )
    primary_scan_id = report["primary_scan"]["scan_summary"]["scan_run_id"]
    print(f"Market report ready: {primary_scan_id}")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_report_region_market_flow(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        report = build_region_market_flow_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            market_date=args.market_date,
            top_items_per_group=args.top_items_per_group,
        )
    print(f"Region market flow report ready: {report['region_name']} {report['market_date']}")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"Items CSV: {report['files']['items_csv_path']}")
    return 0


def cmd_report_history_pair_relationship(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        report = build_history_pair_relationship_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            left_type_id=args.left_type_id,
            right_type_id=args.right_type_id,
            left_label=args.left_label,
            right_label=args.right_label,
            max_lag_days=args.max_lag_days,
        )
    print(f"History pair report ready: {report['left_label']} / {report['right_label']}")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"CSV: {report['files']['csv_path']}")
    return 0


def cmd_report_history_anchor_relationship(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        selected_targets = None
        if args.type_id or args.type_id_file is not None or args.all_published_market_types or args.limit is not None:
            selected_targets = resolve_db_scan_targets(
                connection,
                region_id=args.region_id,
                explicit_type_ids=args.type_id,
                type_id_file=args.type_id_file,
                ready_market_scan_types=False,
                all_published_market_types=(args.all_published_market_types or not args.type_id and args.type_id_file is None),
                limit=args.limit,
            )
        report = build_history_anchor_relationship_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            anchor_type_id=args.anchor_type_id,
            anchor_label=args.anchor_label,
            selected_targets=selected_targets,
            min_overlap_days=args.min_overlap_days,
            top_n=args.top_n,
        )
    print(f"History anchor report ready: {report['anchor_label']}")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"CSV: {report['files']['csv_path']}")
    return 0


def cmd_report_history_anchor_shock_propagation(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        selected_targets = None
        if args.type_id or args.type_id_file is not None or args.all_published_market_types or args.limit is not None:
            selected_targets = resolve_db_scan_targets(
                connection,
                region_id=args.region_id,
                explicit_type_ids=args.type_id,
                type_id_file=args.type_id_file,
                ready_market_scan_types=False,
                all_published_market_types=(args.all_published_market_types or not args.type_id and args.type_id_file is None),
                limit=args.limit,
            )
        report = build_history_anchor_shock_propagation_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            anchor_type_id=args.anchor_type_id,
            anchor_label=args.anchor_label,
            selected_targets=selected_targets,
            min_overlap_days=args.min_overlap_days,
            top_n=args.top_n,
            daily_shock_threshold=args.daily_shock_threshold,
            cumulative_3d_threshold=args.cumulative_3d_threshold,
            return_zscore_threshold=args.return_zscore_threshold,
            cooldown_days=args.cooldown_days,
            filter_profile=args.filter_profile,
            min_median_daily_turnover_isk=args.min_median_daily_turnover_isk,
            min_latest_daily_turnover_isk=args.min_latest_daily_turnover_isk,
            min_median_daily_volume=args.min_median_daily_volume,
            min_event_sample_count=args.min_event_sample_count,
            exclude_name_keywords=args.exclude_name_keyword,
            exclude_group_keywords=args.exclude_group_keyword,
        )
    print(f"History anchor shock report ready: {report['anchor_label']}")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"CSV: {report['files']['csv_path']}")
    return 0


def cmd_report_history_anchor_shock_backtest(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        selected_targets = None
        if args.type_id or args.type_id_file is not None or args.all_published_market_types or args.limit is not None:
            selected_targets = resolve_db_scan_targets(
                connection,
                region_id=args.region_id,
                explicit_type_ids=args.type_id,
                type_id_file=args.type_id_file,
                ready_market_scan_types=False,
                all_published_market_types=(args.all_published_market_types or not args.type_id and args.type_id_file is None),
                limit=args.limit,
            )
        report = build_history_anchor_shock_backtest_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            anchor_type_id=args.anchor_type_id,
            anchor_label=args.anchor_label,
            selected_targets=selected_targets,
            min_overlap_days=args.min_overlap_days,
            top_n=args.top_n,
            daily_shock_threshold=args.daily_shock_threshold,
            cumulative_3d_threshold=args.cumulative_3d_threshold,
            return_zscore_threshold=args.return_zscore_threshold,
            cooldown_days=args.cooldown_days,
            filter_profile=args.filter_profile,
            min_median_daily_turnover_isk=args.min_median_daily_turnover_isk,
            min_latest_daily_turnover_isk=args.min_latest_daily_turnover_isk,
            min_median_daily_volume=args.min_median_daily_volume,
            min_event_sample_count=args.min_event_sample_count,
            exclude_name_keywords=args.exclude_name_keyword,
            exclude_group_keywords=args.exclude_group_keyword,
            entry_delay_days=args.entry_delay_days,
            holding_days_list=args.holding_days,
            fee_config=FeeConfig(
                buy_broker_fee_rate=args.buy_broker_fee_rate,
                sell_broker_fee_rate=args.sell_broker_fee_rate,
                sales_tax_rate=args.sales_tax_rate,
                relist_fee_rate=args.relist_fee_rate,
            ),
            entry_slippage_rate=args.entry_slippage_rate,
            exit_slippage_rate=args.exit_slippage_rate,
        )
    print(f"History anchor shock backtest report ready: {report['anchor_label']}")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"CSV: {report['files']['csv_path']}")
    return 0


def cmd_report_history_reservoir_backtest(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        selected_targets = None
        if args.type_id or args.type_id_file is not None or args.all_published_market_types or args.limit is not None:
            selected_targets = resolve_db_scan_targets(
                connection,
                region_id=args.region_id,
                explicit_type_ids=args.type_id,
                type_id_file=args.type_id_file,
                ready_market_scan_types=False,
                all_published_market_types=(args.all_published_market_types or not args.type_id and args.type_id_file is None),
                limit=args.limit,
            )
        report = build_history_reservoir_backtest_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            selected_targets=selected_targets,
            lookback_days=args.lookback_days,
            max_hold_days=args.max_hold_days,
            entry_zscore_threshold=args.entry_zscore_threshold,
            min_entry_price_vs_mean_ratio=args.min_entry_price_vs_mean_ratio,
            target_net_return=args.target_net_return,
            min_history_days=args.min_history_days,
            min_signal_count=args.min_signal_count,
            min_win_rate=args.min_win_rate,
            min_mean_first_exit_net_return=args.min_mean_first_exit_net_return,
            min_median_daily_turnover_isk=args.min_median_daily_turnover_isk,
            min_latest_daily_turnover_isk=args.min_latest_daily_turnover_isk,
            min_median_daily_volume=args.min_median_daily_volume,
            top_n=args.top_n,
            top_current_n=args.top_current_n,
            fee_config=FeeConfig(
                buy_broker_fee_rate=args.buy_broker_fee_rate,
                sell_broker_fee_rate=args.sell_broker_fee_rate,
                sales_tax_rate=args.sales_tax_rate,
                relist_fee_rate=args.relist_fee_rate,
            ),
        )
    print("History reservoir backtest report ready")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"CSV: {report['files']['csv_path']}")
    return 0


def cmd_report_history_pair_shock(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        report = build_history_pair_shock_report(
            connection,
            output_root=args.output_root,
            region_id=args.region_id,
            left_type_id=args.left_type_id,
            right_type_id=args.right_type_id,
            left_label=args.left_label,
            right_label=args.right_label,
            daily_shock_threshold=args.daily_shock_threshold,
            cumulative_3d_threshold=args.cumulative_3d_threshold,
            return_zscore_threshold=args.return_zscore_threshold,
            cooldown_days=args.cooldown_days,
        )
    print(f"History pair shock report ready: {report['left_label']} -> {report['right_label']}")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    print(f"Events CSV: {report['files']['events_csv_path']}")
    return 0


def cmd_report_jita_passive_cache(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        report = build_jita_passive_cache_report(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            cycle_run_id=args.cycle_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_candidates=args.top_candidates,
        )
    cycle_run_id = report["cycle_summary"]["cycle_run_id"]
    print(f"Jita passive cache report ready: {cycle_run_id}")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_explain_opportunity(args: argparse.Namespace) -> int:
    if args.opportunity_id is None and args.type_id is None:
        raise SystemExit("explain-opportunity requires --opportunity-id or --type-id.")

    with connect(args.db_path) as connection:
        explanation = build_opportunity_explanation(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            opportunity_id=args.opportunity_id,
            type_id=args.type_id,
        )
    print(f"Opportunity explanation ready: {explanation['matched_opportunity_id']}")
    print(json.dumps(explanation, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_explain_jita_passive_cache_opportunity(args: argparse.Namespace) -> int:
    if args.opportunity_id is None and args.type_id is None:
        raise SystemExit("explain-jita-passive-cache-opportunity requires --opportunity-id or --type-id.")

    with connect(args.db_path) as connection:
        explanation = build_jita_passive_cache_opportunity_explanation(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            cycle_run_id=args.cycle_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            opportunity_id=args.opportunity_id,
            type_id=args.type_id,
        )
    print(f"Jita passive cache opportunity explanation ready: {explanation['matched_opportunity_id']}")
    print(json.dumps(explanation, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_diagnose_jita_passive_cache(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        diagnosis = build_jita_passive_cache_diagnosis(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            cycle_run_id=args.cycle_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_rejection_reasons=args.top_rejection_reasons,
            top_near_passes=args.top_near_passes,
            top_skipped_reasons=args.top_skipped_reasons,
        )
    print(f"Jita passive cache diagnosis ready: {diagnosis['scan_summary']['scan_run_id']}")
    print(json.dumps(diagnosis, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_advise_market(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        advice = build_market_advice(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_opportunities=args.top_opportunities,
            top_near_misses=args.top_near_misses,
            risk_profile=args.risk_profile,
            use_latest_account_budget=args.use_latest_account_budget,
            account_snapshot_run_id=args.account_run_id,
        )
    print(f"Market advice ready: {advice['scan_summary']['scan_run_id']}")
    print(json.dumps(advice, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_plan_market_entry(args: argparse.Namespace) -> int:
    if args.total_budget_isk is None and not args.use_latest_account_budget:
        raise SystemExit("plan-market-entry requires --total-budget-isk or --use-latest-account-budget.")
    with connect(args.db_path) as connection:
        plan = build_market_entry_plan(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            total_budget_isk=args.total_budget_isk,
            risk_profile=args.risk_profile,
            max_candidates=args.max_candidates,
            max_positions=args.max_positions,
            near_miss_limit=args.near_miss_limit,
            use_latest_account_budget=args.use_latest_account_budget,
            account_snapshot_run_id=args.account_run_id,
        )
    print(f"Market entry plan ready: {plan['scan_summary']['scan_run_id']}")
    print(json.dumps(plan, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_list_market_shortlist(args: argparse.Namespace) -> int:
    if args.total_budget_isk is None and not args.use_latest_account_budget:
        raise SystemExit("list-market-shortlist requires --total-budget-isk or --use-latest-account-budget.")
    with connect(args.db_path) as connection:
        shortlist = build_market_shortlist(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            total_budget_isk=args.total_budget_isk,
            risk_profile=args.risk_profile,
            top_opportunities=args.top_opportunities,
            top_near_misses=args.top_near_misses,
            max_rows=args.max_rows,
            use_latest_account_budget=args.use_latest_account_budget,
            account_snapshot_run_id=args.account_run_id,
            report_profile=args.report_profile,
        )
    runtime_runs_root, _ = resolve_runtime_roots(
        db_path=args.db_path,
        runs_root=args.runs_root,
        log_dir=args.log_dir,
    )
    run_id = create_run_id("market-shortlist")
    run_dir = ensure_run_dir(runtime_runs_root, run_id)
    markdown_path = run_dir / "market_shortlist.md"
    json_path = run_dir / "market_shortlist.json"
    write_summary(markdown_path, render_market_shortlist_report(shortlist))
    write_json(json_path, shortlist)
    print(f"Market shortlist ready: {shortlist['scan_summary']['scan_run_id']}")
    print(f"Output dir: {run_dir}")
    print(f"Markdown: {markdown_path}")
    return 0


def cmd_list_jita_passive_shortlist(args: argparse.Namespace) -> int:
    if args.total_budget_isk is None and not args.use_latest_account_budget:
        raise SystemExit("list-jita-passive-shortlist requires --total-budget-isk or --use-latest-account-budget.")
    with connect(args.db_path) as connection:
        shortlist = build_market_shortlist(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            total_budget_isk=args.total_budget_isk,
            risk_profile=args.risk_profile,
            top_opportunities=args.top_opportunities,
            top_near_misses=args.top_near_misses,
            max_rows=args.max_rows,
            use_latest_account_budget=args.use_latest_account_budget,
            account_snapshot_run_id=args.account_run_id,
            report_profile="jita-passive",
        )
    runtime_runs_root, _ = resolve_runtime_roots(
        db_path=args.db_path,
        runs_root=args.runs_root,
        log_dir=args.log_dir,
    )
    run_id = create_run_id("jita-passive-shortlist")
    run_dir = ensure_run_dir(runtime_runs_root, run_id)
    markdown_path = run_dir / "market_shortlist.md"
    json_path = run_dir / "market_shortlist.json"
    write_summary(markdown_path, render_market_shortlist_report(shortlist))
    write_json(json_path, shortlist)
    print(f"Jita passive shortlist ready: {shortlist['scan_summary']['scan_run_id']}")
    print(f"Output dir: {run_dir}")
    print(f"Markdown: {markdown_path}")
    return 0


def cmd_advise_jita_passive_cache(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        advice = build_jita_passive_cache_advice(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            cycle_run_id=args.cycle_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_opportunities=args.top_opportunities,
            top_near_misses=args.top_near_misses,
            risk_profile=args.risk_profile,
            use_latest_account_budget=args.use_latest_account_budget,
            account_snapshot_run_id=args.account_run_id,
        )
    print(f"Jita passive cache advice ready: {advice['scan_summary']['scan_run_id']}")
    print(json.dumps(advice, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_brief_jita_passive_cache(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        brief = build_jita_passive_cache_brief(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            cycle_run_id=args.cycle_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_candidates=args.top_candidates,
            top_opportunities=args.top_opportunities,
            top_near_misses=args.top_near_misses,
            top_rejection_reasons=args.top_rejection_reasons,
            top_near_passes=args.top_near_passes,
            top_skipped_reasons=args.top_skipped_reasons,
            risk_profile=args.risk_profile,
            compare_risk_profiles=args.compare_risk_profiles,
            use_latest_account_budget=args.use_latest_account_budget,
            account_snapshot_run_id=args.account_run_id,
        )
    print(f"Jita passive cache brief ready: {brief['scan_summary']['scan_run_id']}")
    print(json.dumps(brief, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_report_jita_reservoir(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        report = build_jita_reservoir_report(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            output_root=args.output_root,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_selected=args.top_selected,
            top_near_misses=args.top_near_misses,
        )

    print("Jita reservoir report ready")
    print(f"Output dir: {report['report_run_dir']}")
    print(f"Markdown: {report['files']['markdown_path']}")
    return 0


def cmd_diagnose_scan(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        diagnosis = build_scan_diagnosis(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
            top_rejection_reasons=args.top_rejection_reasons,
            top_near_passes=args.top_near_passes,
            top_skipped_reasons=args.top_skipped_reasons,
        )
    print(f"Scan diagnosis ready: {diagnosis['scan_summary']['scan_run_id']}")
    print(json.dumps(diagnosis, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_diagnose_coverage(args: argparse.Namespace) -> int:
    with connect(args.db_path) as connection:
        diagnosis = build_coverage_diagnosis(
            connection,
            db_path=args.db_path,
            runs_root=args.runs_root,
            log_dir=args.log_dir,
            scan_run_id=args.scan_run_id,
            prefer_latest_selected=args.prefer_latest_selected,
        )
    print(f"Coverage diagnosis ready: {diagnosis['coverage_progress']['region_id']}")
    print(json.dumps(diagnosis, ensure_ascii=False, indent=2, default=str))
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "init-db":
        return cmd_init_db(args)
    if args.command == "import-sde":
        return cmd_import_sde(args)
    if args.command == "fetch-market-history":
        return cmd_fetch_market_history(args)
    if args.command == "run-history-batch-fetch":
        return cmd_run_history_batch_fetch(args)
    if args.command == "plan-history-sync":
        return cmd_plan_history_sync(args)
    if args.command == "run-history-sync-cycle":
        return cmd_run_history_sync_cycle(args)
    if args.command == "run-history-sync-worker-once":
        return cmd_run_history_sync_worker_once(args)
    if args.command == "fetch-market-orders":
        return cmd_fetch_market_orders(args)
    if args.command == "run-orders-batch-fetch":
        return cmd_run_orders_batch_fetch(args)
    if args.command == "plan-order-refresh":
        return cmd_plan_order_refresh(args)
    if args.command == "run-region-order-mirror":
        return cmd_run_region_order_mirror(args)
    if args.command == "run-order-refresh-cycle":
        return cmd_run_order_refresh_cycle(args)
    if args.command == "run-region-order-mirror-worker-once":
        return cmd_run_region_order_mirror_worker_once(args)
    if args.command == "run-order-refresh-worker-once":
        return cmd_run_order_refresh_worker_once(args)
    if args.command == "run-db-scan":
        return cmd_run_db_scan(args)
    if args.command == "scan-jita-passive-simple":
        return cmd_scan_jita_passive_simple(args)
    if args.command == "scan-jita-passive-full":
        return cmd_scan_jita_passive_full(args)
    if args.command == "run-jita-passive-cycle":
        return cmd_run_jita_passive_cycle(args)
    if args.command == "run-jita-passive-cache-cycle":
        return cmd_run_jita_passive_cache_cycle(args)
    if args.command == "run-jita-passive-cache-worker-once":
        return cmd_run_jita_passive_cache_worker_once(args)
    if args.command == "run-region-pipeline":
        return cmd_run_region_pipeline(args)
    if args.command == "run-region-batch-pipeline":
        return cmd_run_region_batch_pipeline(args)
    if args.command == "benchmark-scan":
        return cmd_benchmark_scan(args)
    if args.command == "plan-market-coverage":
        return cmd_plan_market_coverage(args)
    if args.command == "run-coverage-cycle":
        return cmd_run_coverage_cycle(args)
    if args.command == "run-coverage-worker-once":
        return cmd_run_coverage_worker_once(args)
    if args.command == "run-retention-cleanup":
        return cmd_run_retention_cleanup(args)
    if args.command == "report-market":
        return cmd_report_market(args)
    if args.command == "report-region-market-flow":
        return cmd_report_region_market_flow(args)
    if args.command == "report-history-pair-relationship":
        return cmd_report_history_pair_relationship(args)
    if args.command == "report-history-anchor-relationship":
        return cmd_report_history_anchor_relationship(args)
    if args.command == "report-history-anchor-shock-propagation":
        return cmd_report_history_anchor_shock_propagation(args)
    if args.command == "report-history-anchor-shock-backtest":
        return cmd_report_history_anchor_shock_backtest(args)
    if args.command == "report-history-reservoir-backtest":
        return cmd_report_history_reservoir_backtest(args)
    if args.command == "report-history-pair-shock":
        return cmd_report_history_pair_shock(args)
    if args.command == "report-jita-passive-cache":
        return cmd_report_jita_passive_cache(args)
    if args.command == "explain-jita-passive-cache-opportunity":
        return cmd_explain_jita_passive_cache_opportunity(args)
    if args.command == "diagnose-jita-passive-cache":
        return cmd_diagnose_jita_passive_cache(args)
    if args.command == "explain-opportunity":
        return cmd_explain_opportunity(args)
    if args.command == "advise-market":
        return cmd_advise_market(args)
    if args.command == "plan-market-entry":
        return cmd_plan_market_entry(args)
    if args.command == "list-market-shortlist":
        return cmd_list_market_shortlist(args)
    if args.command == "list-jita-passive-shortlist":
        return cmd_list_jita_passive_shortlist(args)
    if args.command == "advise-jita-passive-cache":
        return cmd_advise_jita_passive_cache(args)
    if args.command == "brief-jita-passive-cache":
        return cmd_brief_jita_passive_cache(args)
    if args.command == "report-jita-reservoir":
        return cmd_report_jita_reservoir(args)
    if args.command == "diagnose-scan":
        return cmd_diagnose_scan(args)
    if args.command == "diagnose-coverage":
        return cmd_diagnose_coverage(args)
    if args.command == "capture-character-account":
        return cmd_capture_character_account(args)
    if args.command == "report-character-account":
        return cmd_report_character_account(args)
    if args.command == "run-scan":
        return cmd_run_scan(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
