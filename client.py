from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

import httpx
from pydantic import TypeAdapter

from models import (
    ActivityKind,
    ActivityRecord,
    ApiCredentials,
    ClosedPositionSnapshot,
    EventMetadata,
    PositionSnapshot,
    PublicProfile,
    SearchProfile,
    SearchResults,
    TotalValueSnapshot,
)
from utils import PolymarketAPIError


class PolymarketClient:
    """Thin client over Polymarket's public Gamma and Data APIs.

    Endpoint assumptions are taken from the current public docs/OpenAPI specs:
    - Data API `/activity` is the preferred public source for trader history because
      it supports `start` and `end` timestamps.
    - Data API `/positions`, `/closed-positions`, and `/value` expose current/open
      and closed snapshots without authentication.
    - Gamma API `/public-profile`, `/public-search`, and `/events/slug/{slug}` are
      used for profile resolution and metadata enrichment.
    """

    DATA_API_BASE = "https://data-api.polymarket.com"
    GAMMA_API_BASE = "https://gamma-api.polymarket.com"
    RETRY_STATUSES = {429, 500, 502, 503, 504}
    DATA_PAGINATION_BATCH_SIZE = 10
    # Live historical activity requests currently reject offsets above 3000 even
    # though the broader OpenAPI spec documents 10000 for the endpoint.
    ACTIVITY_OFFSET_CAP = 3000

    def __init__(
        self,
        credentials: ApiCredentials | None = None,
        timeout_seconds: float = 20.0,
        max_retries: int = 4,
        backoff_seconds: float = 1.0,
        metadata_concurrency: int = 48,
    ) -> None:
        self.credentials = credentials or ApiCredentials()
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.metadata_concurrency = max(1, metadata_concurrency)
        self.logger = logging.getLogger(self.__class__.__name__)

        self._data_client = httpx.Client(
            base_url=self.DATA_API_BASE,
            timeout=httpx.Timeout(timeout_seconds, connect=10.0),
            headers={"User-Agent": "polymarket-trader-analyzer/1.0"},
        )
        self._gamma_client = httpx.Client(
            base_url=self.GAMMA_API_BASE,
            timeout=httpx.Timeout(timeout_seconds, connect=10.0),
            headers={"User-Agent": "polymarket-trader-analyzer/1.0"},
        )

        self._activity_adapter = TypeAdapter(list[ActivityRecord])
        self._positions_adapter = TypeAdapter(list[PositionSnapshot])
        self._closed_positions_adapter = TypeAdapter(list[ClosedPositionSnapshot])
        self._search_adapter = TypeAdapter(SearchResults)
        self._profile_adapter = TypeAdapter(PublicProfile)
        self._value_adapter = TypeAdapter(list[TotalValueSnapshot])
        self._event_adapter = TypeAdapter(EventMetadata)

        self._event_cache: dict[str, EventMetadata | None] = {}
        self._profile_cache: dict[str, PublicProfile | None] = {}

    def close(self) -> None:
        self._data_client.close()
        self._gamma_client.close()

    def __enter__(self) -> PolymarketClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def get_public_profile(self, wallet: str) -> PublicProfile | None:
        """Return the public profile for a wallet if one exists."""

        cache_key = wallet.casefold()
        if cache_key in self._profile_cache:
            return self._profile_cache[cache_key]

        response = self._request_json(
            self._gamma_client,
            "GET",
            "/public-profile",
            params={"address": wallet},
            allow_not_found=True,
        )
        if response is None:
            self._profile_cache[cache_key] = None
            return None

        profile = self._profile_adapter.validate_python(response)
        self._profile_cache[cache_key] = profile
        return profile

    def search_profiles(
        self, query: str, limit_per_type: int = 15
    ) -> list[SearchProfile]:
        """Search public profiles by account/profile name."""

        payload = self._request_json(
            self._gamma_client,
            "GET",
            "/public-search",
            params={
                "q": query,
                "search_profiles": True,
                "search_tags": False,
                "limit_per_type": limit_per_type,
                "page": 1,
            },
        )
        results = self._search_adapter.validate_python(payload)
        return [profile for profile in results.profiles if profile.proxy_wallet]

    def get_user_activity(
        self,
        wallet: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        activity_types: Sequence[ActivityKind | str] = (ActivityKind.TRADE,),
        limit: int = 500,
    ) -> list[ActivityRecord]:
        """Fetch all matching user activity, handling offset caps via time windows.

        The public docs list a higher generic offset limit, but live historical
        `/activity` requests currently reject offsets above 3000. This method pages
        within a descending time window and only moves the `end` cursor backward
        when it reaches that live cap.
        """

        if limit <= 0 or limit > 500:
            raise ValueError("limit must be between 1 and 500")

        type_param = ",".join(str(item) for item in activity_types)
        cursor_end = end_ts
        records: list[ActivityRecord] = []
        seen_keys: set[tuple[Any, ...]] = set()

        while True:
            offset = 0
            reached_offset_cap = False
            window_oldest_ts: int | None = None
            window_any_results = False

            while True:
                params: dict[str, Any] = {
                    "user": wallet,
                    "limit": limit,
                    "offset": offset,
                    "type": type_param,
                    "sortBy": "TIMESTAMP",
                    "sortDirection": "DESC",
                }
                if start_ts is not None:
                    params["start"] = start_ts
                if cursor_end is not None:
                    params["end"] = cursor_end

                payload = self._request_json(
                    self._data_client, "GET", "/activity", params=params
                )
                page = self._activity_adapter.validate_python(payload)
                if not page:
                    break

                window_any_results = True
                for record in page:
                    dedupe_key = (
                        record.transaction_hash,
                        record.asset,
                        record.side,
                        record.size,
                        record.price,
                        record.timestamp,
                        record.condition_id,
                        str(record.type),
                    )
                    if dedupe_key in seen_keys:
                        continue
                    seen_keys.add(dedupe_key)
                    records.append(record)

                oldest_in_page = min(record.timestamp for record in page)
                window_oldest_ts = (
                    oldest_in_page
                    if window_oldest_ts is None
                    else min(window_oldest_ts, oldest_in_page)
                )

                if len(page) < limit:
                    reached_offset_cap = False
                    break
                if offset + limit > self.ACTIVITY_OFFSET_CAP:
                    reached_offset_cap = True
                    break
                offset += limit

            if not window_any_results:
                break
            if not reached_offset_cap:
                break
            if window_oldest_ts is None:
                break
            if start_ts is not None and window_oldest_ts <= start_ts:
                break

            cursor_end = window_oldest_ts - 1
            if cursor_end <= 0:
                break

        records.sort(
            key=lambda item: (
                item.timestamp,
                item.transaction_hash or "",
                item.asset or "",
            )
        )
        return records

    def get_user_trades_snapshot(
        self, wallet: str, limit: int = 500
    ) -> list[ActivityRecord]:
        """Fetch recent user trades from `/trades`.

        This helper exists for completeness, but `/activity?type=TRADE` is used by
        the analytics flow because it supports time-window pagination.
        """

        payload = self._request_json(
            self._data_client,
            "GET",
            "/trades",
            params={"user": wallet, "limit": limit, "offset": 0, "takerOnly": False},
        )
        return self._activity_adapter.validate_python(payload)

    def get_current_positions(self, wallet: str) -> list[PositionSnapshot]:
        """Fetch all current positions for a wallet."""

        return asyncio.run(
            self._paginate_endpoint_async(
                wallet=wallet,
                endpoint="/positions",
                page_limit=500,
                adapter=self._positions_adapter,
            )
        )

    def get_closed_positions(self, wallet: str) -> list[ClosedPositionSnapshot]:
        """Fetch all closed positions for a wallet."""

        return asyncio.run(
            self._paginate_endpoint_async(
                wallet=wallet,
                endpoint="/closed-positions",
                page_limit=50,
                adapter=self._closed_positions_adapter,
            )
        )

    def get_total_value(self, wallet: str) -> float | None:
        """Fetch the public total value snapshot for a wallet."""

        payload = self._request_json(
            self._data_client,
            "GET",
            "/value",
            params={"user": wallet},
        )
        snapshots = self._value_adapter.validate_python(payload)
        if not snapshots:
            return None
        return snapshots[0].value

    def get_event_by_slug(self, slug: str) -> EventMetadata | None:
        """Fetch and cache event metadata by slug."""

        cache_key = slug.casefold()
        if cache_key in self._event_cache:
            return self._event_cache[cache_key]

        encoded_slug = quote(slug, safe="")
        payload = self._request_json(
            self._gamma_client,
            "GET",
            f"/events/slug/{encoded_slug}",
            allow_not_found=True,
        )
        if payload is None:
            self._event_cache[cache_key] = None
            return None

        event = self._event_adapter.validate_python(payload)
        self._event_cache[cache_key] = event
        return event

    def get_events_by_slug(self, slugs: Sequence[str]) -> dict[str, EventMetadata]:
        """Fetch unique event metadata records with per-run caching.

        Large trader histories can touch hundreds of events. The slow path in the
        original implementation was fetching Gamma metadata sequentially; this now
        batches uncached slugs concurrently while preserving the same cache and
        validation behavior.
        """

        unique_slugs = sorted({item for item in slugs if item})
        if not unique_slugs:
            return {}

        missing_slugs = [
            slug for slug in unique_slugs if slug.casefold() not in self._event_cache
        ]
        if missing_slugs:
            self.logger.debug(
                "fetching %d event metadata records with concurrency=%d",
                len(missing_slugs),
                self.metadata_concurrency,
            )
            fetched_events = asyncio.run(self._get_events_by_slug_async(missing_slugs))
            for slug, event in fetched_events.items():
                self._event_cache[slug.casefold()] = event

        output: dict[str, EventMetadata] = {}
        for slug in unique_slugs:
            event = self._event_cache.get(slug.casefold())
            if event is not None and event.slug is not None:
                output[event.slug] = event
            elif event is not None:
                output[slug] = event
        return output

    async def _get_events_by_slug_async(
        self, slugs: Sequence[str]
    ) -> dict[str, EventMetadata | None]:
        timeout = httpx.Timeout(self.timeout_seconds, connect=10.0)
        limits = httpx.Limits(
            max_connections=self.metadata_concurrency,
            max_keepalive_connections=self.metadata_concurrency,
        )
        semaphore = asyncio.Semaphore(self.metadata_concurrency)
        output: dict[str, EventMetadata | None] = {}

        async with httpx.AsyncClient(
            base_url=self.GAMMA_API_BASE,
            timeout=timeout,
            headers={"User-Agent": "polymarket-trader-analyzer/1.0"},
            limits=limits,
        ) as client:

            async def fetch_one(slug: str) -> None:
                async with semaphore:
                    encoded_slug = quote(slug, safe="")
                    payload = await self._request_json_async(
                        client,
                        "GET",
                        f"/events/slug/{encoded_slug}",
                        allow_not_found=True,
                    )
                    if payload is None:
                        output[slug] = None
                        return
                    output[slug] = self._event_adapter.validate_python(payload)

            await asyncio.gather(*(fetch_one(slug) for slug in slugs))

        return output

    async def _paginate_endpoint_async(
        self,
        *,
        wallet: str,
        endpoint: str,
        page_limit: int,
        adapter: TypeAdapter,
    ) -> list[Any]:
        batch_size = max(1, self.DATA_PAGINATION_BATCH_SIZE)
        timeout = httpx.Timeout(self.timeout_seconds, connect=10.0)
        limits = httpx.Limits(
            max_connections=batch_size,
            max_keepalive_connections=batch_size,
        )
        results: list[Any] = []
        next_offset = 0

        async with httpx.AsyncClient(
            base_url=self.DATA_API_BASE,
            timeout=timeout,
            headers={"User-Agent": "polymarket-trader-analyzer/1.0"},
            limits=limits,
        ) as client:
            while True:
                offsets = [
                    next_offset + (page_limit * index) for index in range(batch_size)
                ]
                pages = await asyncio.gather(
                    *(
                        self._fetch_paginated_page_async(
                            client=client,
                            endpoint=endpoint,
                            wallet=wallet,
                            page_limit=page_limit,
                            offset=offset,
                            adapter=adapter,
                        )
                        for offset in offsets
                    )
                )

                reached_end = False
                for page in pages:
                    results.extend(page)
                    if len(page) < page_limit:
                        reached_end = True
                        break

                if reached_end:
                    break
                next_offset += page_limit * batch_size

        return results

    async def _fetch_paginated_page_async(
        self,
        *,
        client: httpx.AsyncClient,
        endpoint: str,
        wallet: str,
        page_limit: int,
        offset: int,
        adapter: TypeAdapter,
    ) -> list[Any]:
        payload = await self._request_json_async(
            client,
            "GET",
            endpoint,
            params={"user": wallet, "limit": page_limit, "offset": offset},
        )
        return adapter.validate_python(payload)

    def _request_json(
        self,
        client: httpx.Client,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = client.request(method, path, params=params)
                if allow_not_found and response.status_code == 404:
                    return None
                if (
                    response.status_code in self.RETRY_STATUSES
                    and attempt < self.max_retries
                ):
                    self._sleep_for_retry(attempt, response.status_code, path)
                    continue
                response.raise_for_status()
                if not response.content:
                    return None
                return response.json()
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if (
                    exc.response.status_code in self.RETRY_STATUSES
                    and attempt < self.max_retries
                ):
                    self._sleep_for_retry(attempt, exc.response.status_code, path)
                    continue
                message = self._extract_error_message(exc.response)
                raise PolymarketAPIError(
                    f"HTTP {exc.response.status_code} for {path}: {message}"
                ) from exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self._sleep_for_retry(attempt, None, path)
                    continue
                raise PolymarketAPIError(f"Request failed for {path}: {exc}") from exc

        raise PolymarketAPIError(f"Request failed for {path}: {last_error}")

    async def _request_json_async(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = await client.request(method, path, params=params)
                if allow_not_found and response.status_code == 404:
                    return None
                if (
                    response.status_code in self.RETRY_STATUSES
                    and attempt < self.max_retries
                ):
                    await self._sleep_for_retry_async(
                        attempt, response.status_code, path
                    )
                    continue
                response.raise_for_status()
                if not response.content:
                    return None
                return response.json()
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if (
                    exc.response.status_code in self.RETRY_STATUSES
                    and attempt < self.max_retries
                ):
                    await self._sleep_for_retry_async(
                        attempt, exc.response.status_code, path
                    )
                    continue
                message = self._extract_error_message(exc.response)
                raise PolymarketAPIError(
                    f"HTTP {exc.response.status_code} for {path}: {message}"
                ) from exc
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    await self._sleep_for_retry_async(attempt, None, path)
                    continue
                raise PolymarketAPIError(f"Request failed for {path}: {exc}") from exc

        raise PolymarketAPIError(f"Request failed for {path}: {last_error}")

    def _sleep_for_retry(
        self, attempt: int, status_code: int | None, path: str
    ) -> None:
        delay = self.backoff_seconds * (2 ** (attempt - 1))
        self.logger.debug(
            "retrying %s after status=%s in %.2fs", path, status_code, delay
        )
        time.sleep(delay)

    async def _sleep_for_retry_async(
        self, attempt: int, status_code: int | None, path: str
    ) -> None:
        delay = self.backoff_seconds * (2 ** (attempt - 1))
        self.logger.debug(
            "retrying %s after status=%s in %.2fs", path, status_code, delay
        )
        await asyncio.sleep(delay)

    @staticmethod
    def _extract_error_message(response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return response.text.strip() or "unknown error"
        if isinstance(payload, dict):
            if "error" in payload:
                return str(payload["error"])
            if "message" in payload:
                return str(payload["message"])
        return str(payload)
