from __future__ import annotations

from urllib.parse import urlencode

from eve_sso_core import EveSsoClient
from eve_sso_core.http import request_json
from eve_sso_core.models import TokenRequest
from eve_sso_core.utils import now_utc_iso

from .models import (
    EsiCharacterAssetsResult,
    EsiCharacterIdentity,
    EsiCharacterOrderHistoryResult,
    EsiCharacterOrdersResult,
    EsiRequestTrace,
    EsiWalletBalanceResult,
    EsiWalletJournalResult,
    EsiWalletTransactionsResult,
    EveEsiConfig,
)

WALLET_SCOPE = "esi-wallet.read_character_wallet.v1"
ASSETS_SCOPE = "esi-assets.read_assets.v1"
ORDERS_SCOPE = "esi-markets.read_character_orders.v1"


class EveAuthenticatedEsiClient:
    def __init__(self, sso_client: EveSsoClient, config: EveEsiConfig = EveEsiConfig()) -> None:
        self.sso_client = sso_client
        self.config = config

    def get_wallet(
        self,
        *,
        character_id: int | None = None,
        character_name: str | None = None,
        owner_hash: str | None = None,
    ) -> EsiWalletBalanceResult:
        selection, headers = self._resolve_auth_headers(
            required_scope=WALLET_SCOPE,
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
        payload, status_code, response_headers = self._request_json(
            f"/characters/{selection.character_id}/wallet",
            headers=headers,
        )
        return EsiWalletBalanceResult(
            character=selection,
            balance=float(payload),
            trace=EsiRequestTrace(
                method="GET",
                url=self._build_url(f"/characters/{selection.character_id}/wallet", {"datasource": self.config.datasource}),
                status_code=status_code,
                fetched_at=now_utc_iso(),
                response_headers=response_headers,
            ),
        )

    def get_wallet_journal(
        self,
        *,
        character_id: int | None = None,
        character_name: str | None = None,
        owner_hash: str | None = None,
    ) -> EsiWalletJournalResult:
        selection, headers = self._resolve_auth_headers(
            required_scope=WALLET_SCOPE,
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
        base_path = f"/characters/{selection.character_id}/wallet/journal"
        first_payload, status_code, response_headers = self._request_json(base_path, headers=headers, page=1)
        entries = list(first_payload)
        pages_total = max(int(response_headers.get("x-pages", "1") or "1"), 1)
        page_traces: list[EsiRequestTrace] = [
            EsiRequestTrace(
                method="GET",
                url=self._build_url(base_path, {"datasource": self.config.datasource, "page": 1}),
                status_code=status_code,
                fetched_at=now_utc_iso(),
                response_headers=response_headers,
                page=1,
                pages_total=pages_total,
            )
        ]
        for page in range(2, pages_total + 1):
            payload, page_status_code, page_headers = self._request_json(base_path, headers=headers, page=page)
            entries.extend(list(payload))
            page_traces.append(
                EsiRequestTrace(
                    method="GET",
                    url=self._build_url(base_path, {"datasource": self.config.datasource, "page": page}),
                    status_code=page_status_code,
                    fetched_at=now_utc_iso(),
                    response_headers=page_headers,
                    page=page,
                    pages_total=pages_total,
                )
            )
        return EsiWalletJournalResult(
            character=selection,
            entries=entries,
            trace={
                "pages_total": pages_total,
                "requests": [trace.__dict__ for trace in page_traces],
            },
        )

    def get_wallet_transactions(
        self,
        *,
        character_id: int | None = None,
        character_name: str | None = None,
        owner_hash: str | None = None,
    ) -> EsiWalletTransactionsResult:
        selection, headers = self._resolve_auth_headers(
            required_scope=WALLET_SCOPE,
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
        base_path = f"/characters/{selection.character_id}/wallet/transactions"
        payload, status_code, response_headers = self._request_json(base_path, headers=headers)
        entries = list(payload)
        summary = {
            "count": len(entries),
            "buy_count": sum(1 for entry in entries if entry.get("is_buy")),
            "sell_count": sum(1 for entry in entries if not entry.get("is_buy")),
        }
        return EsiWalletTransactionsResult(
            character=selection,
            entries=entries,
            summary=summary,
            trace=EsiRequestTrace(
                method="GET",
                url=self._build_url(base_path, {"datasource": self.config.datasource}),
                status_code=status_code,
                fetched_at=now_utc_iso(),
                response_headers=response_headers,
            ),
        )

    def get_assets(
        self,
        *,
        character_id: int | None = None,
        character_name: str | None = None,
        owner_hash: str | None = None,
    ) -> EsiCharacterAssetsResult:
        selection, headers = self._resolve_auth_headers(
            required_scope=ASSETS_SCOPE,
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
        base_path = f"/characters/{selection.character_id}/assets"
        first_payload, status_code, response_headers = self._request_json(base_path, headers=headers, page=1)
        entries = list(first_payload)
        pages_total = max(int(response_headers.get("x-pages", "1") or "1"), 1)
        page_traces: list[EsiRequestTrace] = [
            EsiRequestTrace(
                method="GET",
                url=self._build_url(base_path, {"datasource": self.config.datasource, "page": 1}),
                status_code=status_code,
                fetched_at=now_utc_iso(),
                response_headers=response_headers,
                page=1,
                pages_total=pages_total,
            )
        ]
        for page in range(2, pages_total + 1):
            payload, page_status_code, page_headers = self._request_json(base_path, headers=headers, page=page)
            entries.extend(list(payload))
            page_traces.append(
                EsiRequestTrace(
                    method="GET",
                    url=self._build_url(base_path, {"datasource": self.config.datasource, "page": page}),
                    status_code=page_status_code,
                    fetched_at=now_utc_iso(),
                    response_headers=page_headers,
                    page=page,
                    pages_total=pages_total,
                )
            )
        type_ids = {entry.get("type_id") for entry in entries if entry.get("type_id") is not None}
        total_quantity = 0
        for entry in entries:
            quantity = entry.get("quantity")
            if isinstance(quantity, (int, float)):
                total_quantity += int(quantity)
        return EsiCharacterAssetsResult(
            character=selection,
            entries=entries,
            summary={
                "count": len(entries),
                "type_count": len(type_ids),
                "total_quantity": total_quantity,
            },
            trace={
                "pages_total": pages_total,
                "requests": [trace.__dict__ for trace in page_traces],
            },
        )

    def get_orders(
        self,
        *,
        character_id: int | None = None,
        character_name: str | None = None,
        owner_hash: str | None = None,
    ) -> EsiCharacterOrdersResult:
        selection, headers = self._resolve_auth_headers(
            required_scope=ORDERS_SCOPE,
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
        base_path = f"/characters/{selection.character_id}/orders"
        payload, status_code, response_headers = self._request_json(base_path, headers=headers)
        entries = list(payload)
        buy_orders = [entry for entry in entries if entry.get("is_buy_order")]
        sell_orders = [entry for entry in entries if not entry.get("is_buy_order")]
        return EsiCharacterOrdersResult(
            character=selection,
            entries=entries,
            summary={
                "count": len(entries),
                "buy_count": len(buy_orders),
                "sell_count": len(sell_orders),
                "remaining_buy_value": float(
                    sum(float(entry.get("price", 0.0)) * float(entry.get("volume_remain", 0.0)) for entry in buy_orders)
                ),
                "remaining_sell_value": float(
                    sum(float(entry.get("price", 0.0)) * float(entry.get("volume_remain", 0.0)) for entry in sell_orders)
                ),
            },
            trace=EsiRequestTrace(
                method="GET",
                url=self._build_url(base_path, {"datasource": self.config.datasource}),
                status_code=status_code,
                fetched_at=now_utc_iso(),
                response_headers=response_headers,
            ),
        )

    def get_order_history(
        self,
        *,
        character_id: int | None = None,
        character_name: str | None = None,
        owner_hash: str | None = None,
    ) -> EsiCharacterOrderHistoryResult:
        selection, headers = self._resolve_auth_headers(
            required_scope=ORDERS_SCOPE,
            character_id=character_id,
            character_name=character_name,
            owner_hash=owner_hash,
        )
        base_path = f"/characters/{selection.character_id}/orders/history"
        first_payload, status_code, response_headers = self._request_json(base_path, headers=headers, page=1)
        entries = list(first_payload)
        pages_total = max(int(response_headers.get("x-pages", "1") or "1"), 1)
        page_traces: list[EsiRequestTrace] = [
            EsiRequestTrace(
                method="GET",
                url=self._build_url(base_path, {"datasource": self.config.datasource, "page": 1}),
                status_code=status_code,
                fetched_at=now_utc_iso(),
                response_headers=response_headers,
                page=1,
                pages_total=pages_total,
            )
        ]
        for page in range(2, pages_total + 1):
            payload, page_status_code, page_headers = self._request_json(base_path, headers=headers, page=page)
            entries.extend(list(payload))
            page_traces.append(
                EsiRequestTrace(
                    method="GET",
                    url=self._build_url(base_path, {"datasource": self.config.datasource, "page": page}),
                    status_code=page_status_code,
                    fetched_at=now_utc_iso(),
                    response_headers=page_headers,
                    page=page,
                    pages_total=pages_total,
                )
            )
        status_counts: dict[str, int] = {}
        buy_count = 0
        sell_count = 0
        for entry in entries:
            if entry.get("is_buy_order"):
                buy_count += 1
            else:
                sell_count += 1
            status = str(entry.get("state") or "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
        return EsiCharacterOrderHistoryResult(
            character=selection,
            entries=entries,
            summary={
                "count": len(entries),
                "buy_count": buy_count,
                "sell_count": sell_count,
                "status_counts": status_counts,
            },
            trace={
                "pages_total": pages_total,
                "requests": [trace.__dict__ for trace in page_traces],
            },
        )

    def _resolve_auth_headers(
        self,
        *,
        required_scope: str,
        character_id: int | None,
        character_name: str | None,
        owner_hash: str | None,
    ) -> tuple[EsiCharacterIdentity, dict[str, str]]:
        token = self.sso_client.get_access_token(
            TokenRequest(
                character_id=character_id,
                character_name=character_name,
                owner_hash=owner_hash,
                scopes=[required_scope],
            )
        )
        headers = {
            "authorization": f"Bearer {token.access_token}",
            "accept": "application/json",
            "user-agent": self.config.user_agent,
        }
        if self.config.compatibility_date:
            headers["x-compatibility-date"] = self.config.compatibility_date
        if self.config.tenant:
            headers["x-tenant"] = self.config.tenant
        if self.config.accept_language:
            headers["accept-language"] = self.config.accept_language
        return (
            EsiCharacterIdentity(
                character_id=token.character_id,
                character_name=token.character_name,
                owner_hash=token.owner_hash,
            ),
            headers,
        )

    def _build_url(self, path: str, params: dict[str, object]) -> str:
        query = urlencode({key: value for key, value in params.items() if value is not None})
        separator = "&" if "?" in path else "?"
        return f"{self.config.base_url.rstrip('/')}{path}{separator}{query}" if query else f"{self.config.base_url.rstrip('/')}{path}"

    def _request_json(
        self,
        path: str,
        *,
        headers: dict[str, str],
        page: int | None = None,
    ) -> tuple[object, int, dict[str, str]]:
        params: dict[str, object] = {"datasource": self.config.datasource}
        if page is not None:
            params["page"] = page
        return request_json(
            self._build_url(path, params),
            method="GET",
            headers=headers,
            timeout_seconds=self.config.request_timeout_seconds,
        )
