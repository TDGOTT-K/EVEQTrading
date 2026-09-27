from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .account_budget import derive_account_budget_context, load_account_snapshot
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_isk_human(amount: Any) -> str:
    try:
        numeric = float(amount or 0.0)
    except (TypeError, ValueError):
        return "-"
    abs_numeric = abs(numeric)
    if abs_numeric >= 100_000_000.0:
        return f"{numeric / 100_000_000.0:.2f}亿"
    if abs_numeric >= 10_000.0:
        return f"{numeric / 10_000.0:.2f}万"
    return f"{numeric:.2f}"


def build_character_account_report(
    *,
    db_path: Path,
    runs_root: Path | None,
    output_root: Path,
    account_snapshot_run_id: str | None,
) -> dict[str, Any]:
    snapshot = load_account_snapshot(
        db_path=db_path,
        runs_root=runs_root,
        account_snapshot_run_id=account_snapshot_run_id,
    )
    budget_context = derive_account_budget_context(snapshot)
    run_id = create_run_id("character-account-report")
    run_dir = ensure_run_dir(output_root, run_id)

    wallet_trace = dict((snapshot.get("wallet") or {}).get("trace") or {})
    orders_trace = dict((snapshot.get("orders") or {}).get("trace") or {})
    assets_trace = dict((snapshot.get("assets") or {}).get("trace") or {})
    order_history_trace = dict((snapshot.get("order_history") or {}).get("trace") or {})

    report = {
        "report_generated_at_utc": _utc_now_iso(),
        "report_run_id": run_id,
        "report_run_dir": str(run_dir),
        "account_snapshot_run_id": snapshot.get("run_id"),
        "account_snapshot_path": snapshot.get("_snapshot_path"),
        "character": snapshot.get("character"),
        "selection_context": snapshot.get("selection_context"),
        "account_budget_context": budget_context,
        "freshness": {
            "wallet_fetched_at": wallet_trace.get("fetched_at"),
            "orders_fetched_at": orders_trace.get("fetched_at"),
            "assets_pages_total": assets_trace.get("pages_total"),
            "order_history_pages_total": order_history_trace.get("pages_total"),
        },
        "operator_summary": {
            "headline": _build_headline(budget_context),
            "liquid_isk": budget_context.get("wallet_balance_isk"),
            "buy_commitment_isk": budget_context.get("open_buy_commitment_isk"),
            "sell_gross_isk": budget_context.get("open_sell_gross_isk"),
            "managed_capital_proxy_isk": budget_context.get("managed_capital_proxy_isk"),
            "open_order_count": budget_context.get("open_order_count"),
            "asset_type_count": budget_context.get("asset_type_count"),
            "asset_row_count": budget_context.get("asset_row_count"),
        },
        "traceability": {
            "wallet_trace": wallet_trace,
            "orders_trace": orders_trace,
            "assets_trace": assets_trace,
            "order_history_trace": order_history_trace,
        },
        "source_files": snapshot.get("files"),
        "report_files": {
            "json_path": str(run_dir / "character_account_report.json"),
            "markdown_path": str(run_dir / "character_account_report.md"),
        },
    }
    write_json(run_dir / "character_account_report.json", report)
    write_summary(run_dir / "character_account_report.md", _render_report_markdown(report))
    return report


def _build_headline(context: dict[str, Any]) -> str:
    liquid = float(context.get("wallet_balance_isk") or 0.0)
    buy_commitment = float(context.get("open_buy_commitment_isk") or 0.0)
    sell_gross = float(context.get("open_sell_gross_isk") or 0.0)
    return (
        f"当前液态资金 {_format_isk_human(liquid)} ISK，"
        f"买单占用 {_format_isk_human(buy_commitment)} ISK，"
        f"卖单名义总额 {_format_isk_human(sell_gross)} ISK。"
    )


def _render_report_markdown(report: dict[str, Any]) -> list[str]:
    context = dict(report.get("account_budget_context") or {})
    character = dict(report.get("character") or {})
    selection_context = dict(report.get("selection_context") or {})
    operator_summary = dict(report.get("operator_summary") or {})
    freshness = dict(report.get("freshness") or {})
    source_files = dict(report.get("source_files") or {})
    report_files = dict(report.get("report_files") or {})
    return [
        "# 角色资金面报告",
        "",
        operator_summary.get("headline") or "",
        "",
        "## 角色",
        f"- 角色名: {character.get('character_name') or '-'}",
        f"- 角色ID: {character.get('character_id') or '-'}",
        f"- 所有者哈希: {character.get('owner_hash') or '-'}",
        "",
        "## 角色选择上下文",
        f"- 选择模式: {selection_context.get('mode') or '-'}",
        f"- 请求角色ID: {selection_context.get('requested_character_id') or '-'}",
        f"- 请求角色名: {selection_context.get('requested_character_name') or '-'}",
        f"- 请求所有者哈希: {selection_context.get('requested_owner_hash') or '-'}",
        f"- 实际使用角色名: {selection_context.get('resolved_character_name') or character.get('character_name') or '-'}",
        f"- 已绑定角色数量: {selection_context.get('linked_character_count') or 0}",
        "",
        "## 资金面",
        f"- 液态资金: {_format_isk_human(context.get('wallet_balance_isk'))} ISK",
        f"- 买单占用: {_format_isk_human(context.get('open_buy_commitment_isk'))} ISK",
        f"- 卖单名义总额: {_format_isk_human(context.get('open_sell_gross_isk'))} ISK",
        f"- 已管理资金代理值: {_format_isk_human(context.get('managed_capital_proxy_isk'))} ISK",
        f"- 当前可用于顾问预算: {_format_isk_human(context.get('available_budget_isk'))} ISK",
        "",
        "## 库存与订单",
        f"- 资产行数: {context.get('asset_row_count') or 0}",
        f"- 资产类型数: {context.get('asset_type_count') or 0}",
        f"- 总数量: {context.get('asset_total_quantity') or 0}",
        f"- 当前挂单数: {context.get('open_order_count') or 0}",
        f"- 历史订单行数: {context.get('order_history_count') or 0}",
        "",
        "## 新鲜度",
        f"- 钱包抓取时间: {freshness.get('wallet_fetched_at') or '-'}",
        f"- 订单抓取时间: {freshness.get('orders_fetched_at') or '-'}",
        f"- 资产分页数: {freshness.get('assets_pages_total') or 0}",
        f"- 历史订单分页数: {freshness.get('order_history_pages_total') or 0}",
        "",
        "## 文件",
        f"- 账户快照: {report.get('account_snapshot_path') or '-'}",
        f"- wallet.json: {source_files.get('wallet_path') or '-'}",
        f"- assets.json: {source_files.get('assets_path') or '-'}",
        f"- orders.json: {source_files.get('orders_path') or '-'}",
        f"- order_history.json: {source_files.get('order_history_path') or '-'}",
        f"- report json: {report_files.get('json_path') or '-'}",
        f"- report markdown: {report_files.get('markdown_path') or '-'}",
    ]
