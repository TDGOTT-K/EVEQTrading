from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from eve_sso_core import EveSsoClient, EveSsoConfig

from .client import EveAuthenticatedEsiClient
from .models import EveEsiConfig, dataclass_to_jsonable


def _read_env(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eve-esi", description="Authenticated EVE ESI character-read CLI.")
    parser.add_argument("--client-id", default=_read_env("EVE_SSO_CLIENT_ID"))
    parser.add_argument("--callback-url", default=_read_env("EVE_SSO_CALLBACK_URL") or "http://127.0.0.1:8080/callback")
    parser.add_argument("--store", type=Path, default=Path(_read_env("EVE_SSO_STORE_PATH") or (Path.home() / ".eve-sso" / "tokens.json")))
    parser.add_argument("--sso-base-url", default=_read_env("EVE_SSO_BASE_URL") or "https://login.eveonline.com")
    parser.add_argument("--esi-base-url", default=_read_env("EVE_ESI_BASE_URL") or "https://esi.evetech.net/latest")
    parser.add_argument("--datasource", default=_read_env("EVE_ESI_DATASOURCE") or "tranquility")
    parser.add_argument("--user-agent", default=_read_env("EVE_ESI_USER_AGENT") or "eve-esi-client/0.1")
    parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--compatibility-date", default=_read_env("EVE_ESI_COMPATIBILITY_DATE"))
    parser.add_argument("--tenant", default=_read_env("EVE_ESI_TENANT"))
    parser.add_argument("--accept-language", default=_read_env("EVE_ESI_ACCEPT_LANGUAGE"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command_name in (
        "wallet",
        "wallet-journal",
        "wallet-transactions",
        "assets",
        "orders",
        "order-history",
    ):
        command_parser = subparsers.add_parser(command_name)
        command_parser.add_argument("--character-id", type=int)
        command_parser.add_argument("--character-name")
        command_parser.add_argument("--owner-hash")
        command_parser.add_argument("--output", type=Path)

    return parser


def _build_client(args: argparse.Namespace) -> EveAuthenticatedEsiClient:
    sso_client = EveSsoClient(
        EveSsoConfig(
            client_id=args.client_id,
            callback_url=args.callback_url,
            sso_base_url=args.sso_base_url,
            user_agent=args.user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            store_path=args.store,
        )
    )
    return EveAuthenticatedEsiClient(
        sso_client,
        EveEsiConfig(
            base_url=args.esi_base_url,
            datasource=args.datasource,
            user_agent=args.user_agent,
            request_timeout_seconds=args.request_timeout_seconds,
            compatibility_date=args.compatibility_date,
            tenant=args.tenant,
            accept_language=args.accept_language,
        ),
    )


def _emit(payload: object, output_path: Path | None) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + "\n", encoding="utf-8")
    print(text)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = _build_client(args)
    try:
        common_kwargs = {
            "character_id": getattr(args, "character_id", None),
            "character_name": getattr(args, "character_name", None),
            "owner_hash": getattr(args, "owner_hash", None),
        }
        if args.command == "wallet":
            result = client.get_wallet(**common_kwargs)
            _emit(dataclass_to_jsonable(result), getattr(args, "output", None))
            return 0
        if args.command == "wallet-journal":
            result = client.get_wallet_journal(**common_kwargs)
            _emit(dataclass_to_jsonable(result), getattr(args, "output", None))
            return 0
        if args.command == "wallet-transactions":
            result = client.get_wallet_transactions(**common_kwargs)
            _emit(dataclass_to_jsonable(result), getattr(args, "output", None))
            return 0
        if args.command == "assets":
            result = client.get_assets(**common_kwargs)
            _emit(dataclass_to_jsonable(result), getattr(args, "output", None))
            return 0
        if args.command == "orders":
            result = client.get_orders(**common_kwargs)
            _emit(dataclass_to_jsonable(result), getattr(args, "output", None))
            return 0
        if args.command == "order-history":
            result = client.get_order_history(**common_kwargs)
            _emit(dataclass_to_jsonable(result), getattr(args, "output", None))
            return 0
        raise SystemExit(2)
    except Exception as error:
        print(str(error), file=sys.stderr)
        return 1
