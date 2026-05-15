"""Async HTTP client for communicating with the SLH FastAPI backend.

All methods are coroutines and share a single aiohttp.ClientSession that is
created on first use and closed at shutdown.

Usage::

    from api_client import FastAPIClient

    client = FastAPIClient()
    await client.init()

    health = await client.health_check()
    user   = await client.register_user(telegram_id=123, username="alice")

    await client.close()
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import aiohttp

log = logging.getLogger("slh-master-bot.api_client")

# Default retry settings
_MAX_RETRIES = 3
_RETRY_DELAY = 1.0  # seconds (doubles on each retry)
_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)


class FastAPIClient:
    """Thin async wrapper around the SLH FastAPI backend."""

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (
            (base_url or os.getenv("RAILWAY_FASTAPI_URL", "")).rstrip("/")
        )
        if not self.base_url:
            log.warning(
                "RAILWAY_FASTAPI_URL is not set — API calls will fail"
            )
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Create the underlying aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_TIMEOUT,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "slh-master-bot/1.0",
                },
            )
            log.info("API client session created (base_url=%s)", self.base_url)

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            log.info("API client session closed")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        retries: int = _MAX_RETRIES,
    ) -> dict[str, Any]:
        """Execute an HTTP request with automatic retries on transient errors.

        Raises ``aiohttp.ClientError`` after all retries are exhausted.
        """
        if self._session is None or self._session.closed:
            await self.init()

        url = f"{self.base_url}{path}"
        delay = _RETRY_DELAY

        for attempt in range(1, retries + 1):
            try:
                log.debug(
                    "API %s %s (attempt %d/%d)", method.upper(), path, attempt, retries
                )
                async with self._session.request(  # type: ignore[union-attr]
                    method, url, json=json, params=params
                ) as resp:
                    body = await resp.json(content_type=None)
                    if resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message=str(body),
                        )
                    log.debug("API %s %s → %d", method.upper(), path, resp.status)
                    return body  # type: ignore[return-value]

            except (aiohttp.ClientConnectionError, aiohttp.ServerDisconnectedError) as exc:
                log.warning(
                    "API %s %s attempt %d failed: %s", method.upper(), path, attempt, exc
                )
                if attempt == retries:
                    raise
                await asyncio.sleep(delay)
                delay *= 2

            except aiohttp.ClientResponseError as exc:
                if exc.status >= 500 and attempt < retries:
                    log.warning(
                        "API %s %s → HTTP %d, retrying (%d/%d)…",
                        method.upper(), path, exc.status, attempt, retries,
                    )
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    raise

        # Should never reach here
        raise RuntimeError(f"Exhausted {retries} retries for {method.upper()} {path}")

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def health_check(self) -> dict[str, Any]:
        """GET /api/health — returns the FastAPI backend health payload."""
        try:
            return await self._request("GET", "/api/health")
        except Exception as exc:
            log.error("health_check failed: %s", exc)
            return {"status": "unreachable", "error": str(exc)}

    async def register_user(
        self,
        telegram_id: int,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        referrer_id: int | None = None,
    ) -> dict[str, Any]:
        """POST /api/auth/bot-sync — register or update a Telegram user."""
        payload: dict[str, Any] = {
            "telegram_id": telegram_id,
            "username": username or "",
            "first_name": first_name or "",
            "last_name": last_name or "",
        }
        if referrer_id is not None:
            payload["referrer_id"] = referrer_id

        try:
            result = await self._request("POST", "/api/auth/bot-sync", json=payload)
            log.info(
                "register_user telegram_id=%d → %s",
                telegram_id,
                result.get("status", "?"),
            )
            return result
        except Exception as exc:
            log.error("register_user failed for %d: %s", telegram_id, exc)
            return {"status": "error", "error": str(exc)}

    async def check_eligibility(self, telegram_id: int) -> dict[str, Any]:
        """GET /api/airdrop/eligibility/{telegram_id} — check airdrop eligibility."""
        try:
            return await self._request(
                "GET", f"/api/airdrop/eligibility/{telegram_id}"
            )
        except Exception as exc:
            log.error("check_eligibility failed for %d: %s", telegram_id, exc)
            return {"eligible": False, "error": str(exc)}

    async def submit_wallet(
        self,
        telegram_id: int,
        wallet_address: str,
        chain: str = "bsc",
    ) -> dict[str, Any]:
        """POST /api/wallet/submit — link a wallet address to a Telegram user."""
        payload = {
            "telegram_id": telegram_id,
            "wallet_address": wallet_address,
            "chain": chain,
        }
        try:
            result = await self._request("POST", "/api/wallet/submit", json=payload)
            log.info(
                "submit_wallet telegram_id=%d wallet=%s → %s",
                telegram_id,
                wallet_address[:10] + "…",
                result.get("status", "?"),
            )
            return result
        except Exception as exc:
            log.error("submit_wallet failed for %d: %s", telegram_id, exc)
            return {"status": "error", "error": str(exc)}

    async def get_user_balances(self, telegram_id: int) -> dict[str, Any]:
        """GET /api/wallet/{telegram_id}/balances — fetch token balances."""
        try:
            return await self._request(
                "GET", f"/api/wallet/{telegram_id}/balances"
            )
        except Exception as exc:
            log.error("get_user_balances failed for %d: %s", telegram_id, exc)
            return {"balances": {}, "error": str(exc)}

    async def get_prices(self) -> dict[str, Any]:
        """GET /api/prices — fetch current token prices."""
        try:
            return await self._request("GET", "/api/prices")
        except Exception as exc:
            log.error("get_prices failed: %s", exc)
            return {"prices": {}, "error": str(exc)}
