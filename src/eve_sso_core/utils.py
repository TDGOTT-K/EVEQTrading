from __future__ import annotations

import base64
import re
from datetime import datetime, timezone
from typing import Iterable, Optional

from .errors import EveSsoTokenValidationError


def normalize_scopes(scopes: Iterable[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for scope in scopes or []:
        for part in str(scope).replace(",", " ").split():
            item = part.strip()
            if not item or item in seen:
                continue
            seen.add(item)
            normalized.append(item)
    normalized.sort()
    return normalized


def scopes_to_string(scopes: Iterable[str] | None) -> str:
    return " ".join(normalize_scopes(scopes))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def now_utc_iso() -> str:
    return to_utc_iso(utc_now())


def parse_utc_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def expires_within(expires_at: str, min_validity_seconds: int) -> bool:
    try:
        expires_at_dt = parse_utc_datetime(expires_at)
    except ValueError:
        return True
    threshold = utc_now().timestamp() + max(min_validity_seconds, 0)
    return expires_at_dt.timestamp() <= threshold


def normalize_issuer(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.rstrip("/")


def parse_character_id(subject: Optional[str]) -> int:
    matched = re.fullmatch(r"CHARACTER:EVE:(\d+)", subject or "")
    if not matched:
        raise EveSsoTokenValidationError("EVE SSO token subject is missing CHARACTER:EVE:<id>.")
    return int(matched.group(1))


def decode_base64url(value: str) -> bytes:
    padding = "=" * ((4 - (len(value) % 4)) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))
