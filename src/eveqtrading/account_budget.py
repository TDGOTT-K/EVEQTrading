from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .report import resolve_runtime_roots


def load_account_snapshot(
    *,
    db_path: Path,
    runs_root: Path | None,
    account_snapshot_run_id: str | None,
) -> dict[str, Any]:
    resolved_runs_root, _ = resolve_runtime_roots(
        db_path=db_path,
        runs_root=runs_root,
        log_dir=None,
    )
    run_dir: Path | None = None
    if account_snapshot_run_id:
        candidate = resolved_runs_root / account_snapshot_run_id
        if candidate.exists():
            run_dir = candidate
    else:
        candidates = sorted(
            [
                path
                for path in resolved_runs_root.glob("character-account-*")
                if path.is_dir() and (path / "account_snapshot.json").exists()
            ]
        )
        if candidates:
            run_dir = candidates[-1]
    if run_dir is None:
        raise SystemExit("No character account snapshot artifact was found. Run eveq capture-character-account first.")

    snapshot_path = run_dir / "account_snapshot.json"
    if not snapshot_path.exists():
        raise SystemExit(f"Character account snapshot artifact is missing: {snapshot_path}")
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["_run_dir"] = str(run_dir)
    payload["_snapshot_path"] = str(snapshot_path)
    return payload


def derive_account_budget_context(snapshot: dict[str, Any]) -> dict[str, Any]:
    wallet = dict(snapshot.get("wallet") or {})
    orders = dict(snapshot.get("orders") or {})
    assets = dict(snapshot.get("assets") or {})
    order_history = dict(snapshot.get("order_history") or {})
    character = dict(snapshot.get("character") or {})
    selection_context = dict(snapshot.get("selection_context") or {})
    orders_summary = dict(orders.get("summary") or {})
    assets_summary = dict(assets.get("summary") or {})
    order_history_summary = dict(order_history.get("summary") or {})

    wallet_balance_isk = float(wallet.get("balance") or 0.0)
    open_buy_commitment_isk = float(orders_summary.get("remaining_buy_value") or 0.0)
    open_sell_gross_isk = float(orders_summary.get("remaining_sell_value") or 0.0)

    return {
        "budget_source": "account_wallet_balance",
        "account_snapshot_run_id": snapshot.get("run_id"),
        "account_snapshot_path": snapshot.get("_snapshot_path"),
        "account_snapshot_dir": snapshot.get("_run_dir"),
        "character_id": character.get("character_id"),
        "character_name": character.get("character_name"),
        "owner_hash": character.get("owner_hash"),
        "selection_mode": selection_context.get("mode"),
        "used_explicit_selection": bool(selection_context.get("used_explicit_selection")),
        "requested_character_id": selection_context.get("requested_character_id"),
        "requested_character_name": selection_context.get("requested_character_name"),
        "requested_owner_hash": selection_context.get("requested_owner_hash"),
        "linked_character_count": int(selection_context.get("linked_character_count") or 0),
        "wallet_balance_isk": wallet_balance_isk,
        "open_buy_commitment_isk": open_buy_commitment_isk,
        "open_sell_gross_isk": open_sell_gross_isk,
        "managed_capital_proxy_isk": wallet_balance_isk + open_buy_commitment_isk,
        "asset_row_count": int(assets_summary.get("count") or 0),
        "asset_type_count": int(assets_summary.get("type_count") or 0),
        "asset_total_quantity": int(assets_summary.get("total_quantity") or 0),
        "open_order_count": int(orders_summary.get("count") or 0),
        "order_history_count": int(order_history_summary.get("count") or 0),
        "available_budget_isk": max(wallet_balance_isk, 0.0),
        "budget_summary_text": (
            f"wallet {wallet_balance_isk:.2f} ISK liquid, "
            f"buy orders consuming {open_buy_commitment_isk:.2f} ISK notional, "
            f"sell orders listing {open_sell_gross_isk:.2f} ISK gross"
        ),
    }


def build_analysis_context(
    *,
    budget_source: str,
    total_budget_isk: float,
    account_budget_context: dict[str, Any] | None,
    manual_budget_isk: float | None,
    scan_budget_isk: float | None,
) -> dict[str, Any]:
    account_context = dict(account_budget_context or {})
    if budget_source == "account_wallet_balance":
        character_name = account_context.get("character_name")
        character_id = account_context.get("character_id")
        selection_mode = str(account_context.get("selection_mode") or "")
        return {
            "context_mode": "linked_character_account",
            "analysis_mode": "account_linked",
            "analysis_mode_label_zh": "角色绑定模式",
            "uses_character_account": True,
            "budget_source": budget_source,
            "budget_source_label_zh": "角色钱包余额",
            "budget_source_summary_zh": (
                f"本轮使用绑定角色 {character_name or '-'}"
                + (f"（{character_id}）" if character_id is not None else "")
                + " 的钱包余额作为预算。"
            ),
            "total_budget_isk": total_budget_isk,
            "account_snapshot_run_id": account_context.get("account_snapshot_run_id"),
            "character_id": character_id,
            "character_name": character_name,
            "selection_mode": selection_mode or None,
            "selection_mode_label_zh": _selection_mode_label_zh(selection_mode),
            "linked_character_count": account_context.get("linked_character_count"),
        }
    if manual_budget_isk is not None:
        return {
            "context_mode": "manual_budget_override",
            "analysis_mode": "market_only",
            "analysis_mode_label_zh": "纯市场模式",
            "uses_character_account": False,
            "budget_source": budget_source,
            "budget_source_label_zh": "手动预算",
            "budget_source_summary_zh": f"本轮未使用角色绑定数据，预算采用手动输入 {manual_budget_isk:.2f} ISK。",
            "total_budget_isk": total_budget_isk,
            "account_snapshot_run_id": None,
            "character_id": None,
            "character_name": None,
            "selection_mode": None,
            "selection_mode_label_zh": None,
            "linked_character_count": 0,
        }
    return {
        "context_mode": "scan_budget",
        "analysis_mode": "market_only",
        "analysis_mode_label_zh": "纯市场模式",
        "uses_character_account": False,
        "budget_source": budget_source,
        "budget_source_label_zh": "扫描预算",
        "budget_source_summary_zh": f"本轮未使用角色绑定数据，预算采用扫描内置预算 {float(scan_budget_isk or 0.0):.2f} ISK。",
        "total_budget_isk": total_budget_isk,
        "account_snapshot_run_id": None,
        "character_id": None,
        "character_name": None,
        "selection_mode": None,
        "selection_mode_label_zh": None,
        "linked_character_count": 0,
    }


def _selection_mode_label_zh(mode: str | None) -> str | None:
    return {
        "explicit_character_id": "显式指定角色ID",
        "explicit_character_name": "显式指定角色名",
        "explicit_owner_hash": "显式指定所有者哈希",
        "default_linked_character": "当前默认绑定角色",
        "single_linked_character": "唯一已绑定角色",
        "implicit_selected_character": "隐式解析角色",
    }.get(str(mode or ""), None)
