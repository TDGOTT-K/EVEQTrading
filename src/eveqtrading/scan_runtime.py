from __future__ import annotations

from pathlib import Path
from typing import Any

from .analysis import analyze_market_scan, persist_scan_run
from .db_scan import build_db_scan_candidates, persist_feature_rows, resolve_db_scan_targets
from .models import FeeConfig, FilterConfig, ScanConfig
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def execute_db_scan(
    *,
    connection,
    db_path: Path,
    output_root: Path,
    region_id: int,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool,
    all_published_market_types: bool,
    limit: int | None,
    capital_budget_isk: float,
    test_position_isk: float,
    top_n: int,
    strategy_set: str,
    parameter_version: str,
) -> dict[str, Any]:
    run_id = create_run_id()
    run_dir = ensure_run_dir(output_root, run_id)
    fee_config = FeeConfig()
    filter_config = FilterConfig()
    scan_config = ScanConfig(
        strategy_set=strategy_set,
        parameter_version=parameter_version,
        test_position_isk=test_position_isk,
        capital_budget_isk=capital_budget_isk,
        top_n=top_n,
    )

    selected_targets = resolve_db_scan_targets(
        connection,
        region_id=region_id,
        explicit_type_ids=explicit_type_ids,
        type_id_file=type_id_file,
        ready_market_scan_types=ready_market_scan_types,
        all_published_market_types=all_published_market_types,
        limit=limit,
    )
    candidate_build = build_db_scan_candidates(
        connection,
        region_id=region_id,
        fee_config=fee_config,
        test_position_isk=test_position_isk,
        selected_targets=selected_targets,
    )
    analysis_run, calc_trace, selected_candidates, summary_lines = analyze_market_scan(
        payload=candidate_build.payload,
        input_path=db_path.resolve(),
        fee_config=fee_config,
        filter_config=filter_config,
        scan_config=scan_config,
    )
    analysis_run["run_id"] = run_id
    analysis_run["output_dir"] = str(run_dir.resolve())
    analysis_run["input_mode"] = "sqlite-db-scan"
    calc_trace["run_id"] = run_id
    calc_trace["source_mode"] = "sqlite-db-scan"
    calc_trace["skipped_rows"] = candidate_build.skipped_rows
    write_json(run_dir / "db_scan_input.json", candidate_build.payload)
    write_json(run_dir / "feature_table.json", candidate_build.feature_rows)
    write_json(run_dir / "skipped_table.json", candidate_build.skipped_rows)
    write_json(run_dir / "analysis_run.json", analysis_run)
    write_json(run_dir / "calc_trace.json", calc_trace)
    write_json(run_dir / "candidate_table.json", selected_candidates)
    write_summary(run_dir / "summary.md", summary_lines)
    persist_feature_rows(connection, feature_run_id=run_id, feature_rows=candidate_build.feature_rows)
    persist_scan_run(connection, run_id, analysis_run, selected_candidates, run_dir.resolve())

    return {
        "run_id": run_id,
        "run_dir": run_dir,
        "candidate_build": candidate_build,
        "selected_candidates": selected_candidates,
        "selected_targets": selected_targets or [],
        "analysis_run": analysis_run,
        "calc_trace": calc_trace,
    }
