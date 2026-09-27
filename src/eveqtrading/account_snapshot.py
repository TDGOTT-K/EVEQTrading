from __future__ import annotations

from pathlib import Path
from typing import Any

from eve_esi_client import EveAuthenticatedEsiClient, EveEsiConfig
from eve_sso_core import EveSsoClient, EveSsoConfig, LinkedCharacterRecord, LinkedCharacterSummary, TokenRequest

from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def capture_character_account_snapshot(
    *,
    output_root: Path,
    client_id: str | None,
    callback_url: str,
    store_path: Path,
    sso_base_url: str,
    esi_base_url: str,
    datasource: str,
    user_agent: str,
    request_timeout_seconds: float,
    compatibility_date: str | None,
    tenant: str | None,
    accept_language: str | None,
    character_id: int | None,
    character_name: str | None,
    owner_hash: str | None,
) -> dict[str, Any]:
    run_id = create_run_id("character-account")
    run_dir = ensure_run_dir(output_root, run_id)

    sso_client = EveSsoClient(
        EveSsoConfig(
            client_id=client_id,
            callback_url=callback_url,
            sso_base_url=sso_base_url,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
            store_path=store_path,
        )
    )
    esi_client = EveAuthenticatedEsiClient(
        sso_client,
        EveEsiConfig(
            base_url=esi_base_url,
            datasource=datasource,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
            compatibility_date=compatibility_date,
            tenant=tenant,
            accept_language=accept_language,
        ),
    )

    selection = {
        "character_id": character_id,
        "character_name": character_name,
        "owner_hash": owner_hash,
    }
    selected_character = sso_client.get_character_record(
        TokenRequest(
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
    )
    selection_context = _build_selection_context(
        selection=selection,
        selected_character=selected_character,
        linked_characters=sso_client.list_characters(),
    )
    wallet_result = esi_client.get_wallet(**selection)
    assets_result = esi_client.get_assets(**selection)
    orders_result = esi_client.get_orders(**selection)
    order_history_result = esi_client.get_order_history(**selection)

    wallet_path = run_dir / "wallet.json"
    assets_path = run_dir / "assets.json"
    orders_path = run_dir / "orders.json"
    order_history_path = run_dir / "order_history.json"

    write_json(wallet_path, wallet_result)
    write_json(assets_path, assets_result)
    write_json(orders_path, orders_result)
    write_json(order_history_path, order_history_result)

    snapshot = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "character": wallet_result.character,
        "config": {
            "datasource": datasource,
            "esi_base_url": esi_base_url,
            "store_path": str(store_path),
            "user_agent": user_agent,
        },
        "selection": selection,
        "selection_context": selection_context,
        "wallet": {
            "balance": wallet_result.balance,
            "trace": wallet_result.trace,
        },
        "assets": {
            "summary": assets_result.summary,
            "trace": assets_result.trace,
        },
        "orders": {
            "summary": orders_result.summary,
            "trace": orders_result.trace,
        },
        "order_history": {
            "summary": order_history_result.summary,
            "trace": order_history_result.trace,
        },
        "files": {
            "wallet_path": str(wallet_path),
            "assets_path": str(assets_path),
            "orders_path": str(orders_path),
            "order_history_path": str(order_history_path),
            "summary_path": str(run_dir / "summary.md"),
        },
    }
    write_json(run_dir / "account_snapshot.json", snapshot)
    write_summary(
        run_dir / "summary.md",
        _build_summary_lines(snapshot),
    )
    return snapshot


def _build_selection_context(
    *,
    selection: dict[str, Any],
    selected_character: LinkedCharacterRecord,
    linked_characters: list[LinkedCharacterSummary],
) -> dict[str, Any]:
    if selection.get("character_id") is not None:
        mode = "explicit_character_id"
    elif selection.get("character_name"):
        mode = "explicit_character_name"
    elif selection.get("owner_hash"):
        mode = "explicit_owner_hash"
    elif len(linked_characters) == 1:
        mode = "single_linked_character"
    elif any(
        summary.is_default and summary.character_id == selected_character.character_id for summary in linked_characters
    ):
        mode = "default_linked_character"
    else:
        mode = "implicit_selected_character"
    return {
        "mode": mode,
        "used_explicit_selection": mode.startswith("explicit_"),
        "linked_character_count": len(linked_characters),
        "requested_character_id": selection.get("character_id"),
        "requested_character_name": selection.get("character_name"),
        "requested_owner_hash": selection.get("owner_hash"),
        "resolved_character_id": selected_character.character_id,
        "resolved_character_name": selected_character.character_name,
        "resolved_owner_hash": selected_character.owner_hash,
    }


def _build_summary_lines(snapshot: dict[str, Any]) -> list[str]:
    character = snapshot["character"]
    selection_context = snapshot.get("selection_context") or {}
    wallet = snapshot["wallet"]
    assets = snapshot["assets"]["summary"]
    orders = snapshot["orders"]["summary"]
    order_history = snapshot["order_history"]["summary"]
    files = snapshot["files"]
    return [
        f"# Character Account Snapshot {snapshot['run_id']}",
        "",
        "## Character",
        f"- Name: {character.character_name}",
        f"- Character ID: {character.character_id}",
        f"- Owner Hash: {character.owner_hash}",
        "",
        "## Selection",
        f"- Selection Mode: {selection_context.get('mode') or '-'}",
        f"- Requested Character ID: {selection_context.get('requested_character_id') or '-'}",
        f"- Requested Character Name: {selection_context.get('requested_character_name') or '-'}",
        f"- Requested Owner Hash: {selection_context.get('requested_owner_hash') or '-'}",
        f"- Resolved Character Name: {selection_context.get('resolved_character_name') or '-'}",
        f"- Linked Character Count: {selection_context.get('linked_character_count') or 0}",
        "",
        "## Wallet",
        f"- Wallet Balance ISK: {wallet['balance']:.2f}",
        f"- Wallet Fetch URL: {wallet['trace'].url}",
        f"- Wallet Fetch Status: {wallet['trace'].status_code}",
        "",
        "## Assets",
        f"- Asset Rows: {assets['count']}",
        f"- Asset Types: {assets['type_count']}",
        f"- Total Quantity: {assets['total_quantity']}",
        "",
        "## Current Orders",
        f"- Open Orders: {orders['count']}",
        f"- Buy Orders: {orders['buy_count']}",
        f"- Sell Orders: {orders['sell_count']}",
        f"- Remaining Buy Value ISK: {orders['remaining_buy_value']:.2f}",
        f"- Remaining Sell Gross ISK: {orders['remaining_sell_value']:.2f}",
        "",
        "## Order History",
        f"- Historical Orders: {order_history['count']}",
        f"- Buy History Rows: {order_history['buy_count']}",
        f"- Sell History Rows: {order_history['sell_count']}",
        f"- Status Counts: {order_history['status_counts']}",
        "",
        "## Files",
        f"- wallet: {files['wallet_path']}",
        f"- assets: {files['assets_path']}",
        f"- orders: {files['orders_path']}",
        f"- order_history: {files['order_history_path']}",
        f"- snapshot: {snapshot['run_dir']}\\account_snapshot.json",
    ]
