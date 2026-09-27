from __future__ import annotations

import json
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .errors import EveSsoError


def _normalize_headers(headers: Any) -> dict[str, str]:
    normalized: dict[str, str] = {}
    items = headers.items() if hasattr(headers, "items") else []
    for key, value in items:
        normalized[str(key).lower()] = str(value)
    return normalized


def request_json(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    form: Optional[dict[str, str]] = None,
    timeout_seconds: float = 30.0,
) -> tuple[Any, int, dict[str, str]]:
    body: bytes | None = None
    request_headers = dict(headers or {})
    if form is not None:
        body = urlencode(form).encode("utf-8")
        request_headers.setdefault("content-type", "application/x-www-form-urlencoded")
    request = Request(url, data=body, headers=request_headers, method=method.upper())
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload), int(response.status), _normalize_headers(response.headers)
    except HTTPError as error:
        payload = error.read().decode("utf-8", errors="replace")
        raise EveSsoError(f"SSO request failed for {url} with status {error.code}: {payload}") from error
    except URLError as error:
        raise EveSsoError(f"SSO request failed for {url}: {error.reason}") from error
