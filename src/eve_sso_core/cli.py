from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .client import EveSsoClient
from .errors import EveSsoError
from .models import EveSsoConfig, TokenRequest


def _read_env(name: str) -> str | None:
    value = os.getenv(name)
    return value if value else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eve-sso", description="Reusable EVE Online SSO core CLI.")
    parser.add_argument("--client-id", default=_read_env("EVE_SSO_CLIENT_ID"))
    parser.add_argument("--callback-url", default=_read_env("EVE_SSO_CALLBACK_URL") or "http://127.0.0.1:8080/callback")
    parser.add_argument("--store", type=Path, default=Path(_read_env("EVE_SSO_STORE_PATH") or (Path.home() / ".eve-sso" / "tokens.json")))
    parser.add_argument("--sso-base-url", default=_read_env("EVE_SSO_BASE_URL") or "https://login.eveonline.com")
    parser.add_argument("--user-agent", default=_read_env("EVE_SSO_USER_AGENT") or "eve-sso-core/0.1")
    parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    subparsers = parser.add_subparsers(dest="command", required=True)

    login_parser = subparsers.add_parser("login", help="Run a local PKCE login flow.")
    login_parser.add_argument("--scope", action="append", default=[])
    login_parser.add_argument("--set-default", action="store_true")
    login_parser.add_argument("--timeout-seconds", type=float, default=180.0)
    login_parser.add_argument("--no-open-browser", action="store_true")
    login_parser.add_argument("--json", action="store_true")

    list_parser = subparsers.add_parser("list", help="List linked characters.")
    list_parser.add_argument("--json", action="store_true")

    token_parser = subparsers.add_parser("token", help="Resolve an access token for a linked character.")
    token_parser.add_argument("--character-id", type=int)
    token_parser.add_argument("--character-name")
    token_parser.add_argument("--owner-hash")
    token_parser.add_argument("--scope", action="append", default=[])
    token_parser.add_argument("--min-validity-seconds", type=int, default=300)
    token_parser.add_argument("--json", action="store_true")

    whoami_parser = subparsers.add_parser("whoami", help="Show the identity of the selected linked character.")
    whoami_parser.add_argument("--character-id", type=int)
    whoami_parser.add_argument("--character-name")
    whoami_parser.add_argument("--owner-hash")
    whoami_parser.add_argument("--scope", action="append", default=[])
    whoami_parser.add_argument("--min-validity-seconds", type=int, default=300)
    whoami_parser.add_argument("--json", action="store_true")

    set_default_parser = subparsers.add_parser("set-default", help="Set the default linked character.")
    set_default_parser.add_argument("--character-id", type=int, required=True)

    remove_parser = subparsers.add_parser("remove", help="Remove a linked character.")
    remove_parser.add_argument("--character-id", type=int, required=True)

    return parser


def _build_client(args: argparse.Namespace) -> EveSsoClient:
    config = EveSsoConfig(
        client_id=args.client_id,
        callback_url=args.callback_url,
        sso_base_url=args.sso_base_url,
        user_agent=args.user_agent,
        request_timeout_seconds=args.request_timeout_seconds,
        store_path=args.store,
    )
    return EveSsoClient(config)


def _build_token_request(args: argparse.Namespace) -> TokenRequest:
    return TokenRequest(
        character_id=getattr(args, "character_id", None),
        character_name=getattr(args, "character_name", None),
        owner_hash=getattr(args, "owner_hash", None),
        scopes=list(getattr(args, "scope", []) or []),
        min_validity_seconds=getattr(args, "min_validity_seconds", 300),
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    client = _build_client(args)

    try:
        if args.command == "login":
            record = client.login_with_local_callback(
                list(args.scope),
                set_default=args.set_default,
                open_browser=not args.no_open_browser,
                timeout_seconds=args.timeout_seconds,
            )
            if args.json:
                print(json.dumps(record.__dict__, ensure_ascii=False, indent=2))
            else:
                print(
                    f"Linked {record.character_name} ({record.character_id}) with scopes: {', '.join(record.scopes)}"
                )
                print(f"Token store: {client.config.store_path}")
            return 0

        if args.command == "list":
            records = client.list_characters()
            if args.json:
                print(json.dumps([record.__dict__ for record in records], ensure_ascii=False, indent=2))
            elif not records:
                print(f"No linked EVE characters. Token store: {client.config.store_path}")
            else:
                for record in records:
                    prefix = "*" if record.is_default else " "
                    print(
                        f"{prefix} {record.character_name} ({record.character_id}) | scopes={', '.join(record.scopes)} | expires={record.access_token_expires_at}"
                    )
            return 0

        if args.command == "token":
            resolved = client.get_access_token(_build_token_request(args))
            if args.json:
                print(json.dumps(resolved.__dict__, ensure_ascii=False, indent=2))
            else:
                print(resolved.access_token)
            return 0

        if args.command == "whoami":
            resolved = client.get_access_token(_build_token_request(args))
            payload = {
                "character_id": resolved.character_id,
                "character_name": resolved.character_name,
                "owner_hash": resolved.owner_hash,
                "scopes": resolved.scopes,
                "expires_at": resolved.expires_at,
                "issuer": resolved.issuer,
                "subject": resolved.subject,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        if args.command == "set-default":
            client.set_default_character(args.character_id)
            print(f"Default character set to {args.character_id}.")
            return 0

        if args.command == "remove":
            removed = client.remove_character(args.character_id)
            if removed:
                print(f"Removed character {args.character_id}.")
            else:
                print(f"Character {args.character_id} was not found.")
            return 0

        parser.error(f"Unknown command: {args.command}")
        return 2
    except EveSsoError as error:
        print(str(error), file=sys.stderr)
        return 1
