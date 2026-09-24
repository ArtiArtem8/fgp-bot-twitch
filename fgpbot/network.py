"""Make bounded HTTP requests with explicit retry behavior."""

from __future__ import annotations

import asyncio
import json
import logging
from http import HTTPStatus
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from .security import REDACT

LOG = logging.getLogger(__name__)


class NetworkError(Exception):
    """No usable HTTP response; a POST may already have reached the server."""


class ProtocolError(Exception):
    """A response violates the expected protocol."""


class RemoteError(Exception):
    """Carry an HTTP failure with a redacted response detail."""

    def __init__(self, status: int, detail: str, retry_after: float = 0) -> None:
        self.status = status
        self.detail = REDACT(detail)[:300]
        self.retry_after = retry_after
        super().__init__(f"HTTP {status}: {self.detail}")


class Http:
    """Apply proxy and retry policy to Twitch HTTP requests."""

    def __init__(self, session: aiohttp.ClientSession, proxy: str | None) -> None:
        self.session = session
        self.proxy = proxy

    @staticmethod
    async def _read_response(response: aiohttp.ClientResponse, safe_target: str) -> Any:
        # Consume the complete body with a hard cap; read(n) may return one chunk.
        raw = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            raw.extend(chunk)
            if len(raw) > 2 * 1024 * 1024:
                raise ProtocolError(f"Слишком большой ответ от {safe_target}")
        try:
            data = json.loads(raw) if raw else {}
        except ValueError, UnicodeDecodeError:
            data = None
        if not HTTPStatus.OK <= response.status < HTTPStatus.MULTIPLE_CHOICES:
            detail = (
                str(data.get("message", data.get("error", "")))
                if isinstance(data, dict)
                else "Ответ не JSON"
            )
            try:
                retry_after = min(30.0, max(0.0, float(response.headers.get("Retry-After", "0"))))
            except ValueError:
                retry_after = 0
            raise RemoteError(response.status, detail, retry_after)
        if data is None:
            raise ProtocolError(f"Некорректный JSON от {safe_target}")
        return data

    async def _request_once(self, method: str, url: str, safe_target: str, **kwargs: Any) -> Any:
        LOG.debug("HTTP %s %s", method, safe_target)
        async with self.session.request(
            method,
            url,
            proxy=self.proxy,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=12, connect=5, sock_read=8),
            **kwargs,
        ) as response:
            return await self._read_response(response, safe_target)

    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        """Retry safe GETs once. Never replay an ambiguous POST."""
        target = urlsplit(url)
        safe_target = f"{target.hostname}{target.path}"
        for attempt in range(2 if method == "GET" else 1):
            try:
                return await self._request_once(method, url, safe_target, **kwargs)
            except RemoteError as exc:
                if (
                    method != "GET"
                    or attempt
                    or (
                        exc.status != HTTPStatus.TOO_MANY_REQUESTS
                        and exc.status < HTTPStatus.INTERNAL_SERVER_ERROR
                    )
                ):
                    raise
                await asyncio.sleep(max(1.0, exc.retry_after))
            except (aiohttp.ClientError, TimeoutError) as exc:
                if method != "GET" or attempt:
                    raise NetworkError(f"{method} {safe_target}: {type(exc).__name__}") from None
                await asyncio.sleep(1)
        raise AssertionError("unreachable")
