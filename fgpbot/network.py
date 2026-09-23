from __future__ import annotations

import asyncio
import json
import logging
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
    def __init__(self, status: int, detail: str, retry_after: float = 0) -> None:
        self.status = status
        self.detail = REDACT(detail)[:300]
        self.retry_after = retry_after
        super().__init__(f"HTTP {status}: {self.detail}")


class Http:
    def __init__(self, session: aiohttp.ClientSession, proxy: str | None) -> None:
        self.session = session
        self.proxy = proxy

    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        """Retry safe GETs once. Never replay an ambiguous POST."""
        target = urlsplit(url)
        safe_target = f"{target.hostname}{target.path}"
        for attempt in range(2 if method == "GET" else 1):
            try:
                LOG.debug("HTTP %s %s", method, safe_target)
                async with self.session.request(
                    method, url, proxy=self.proxy, allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(total=12, connect=5, sock_read=8), **kwargs,
                ) as response:
                    # read(n) may return only the first HTTP chunk. Consume the
                    # complete body, with a hard cap rather than an unbounded read.
                    raw = bytearray()
                    async for chunk in response.content.iter_chunked(65536):
                        raw.extend(chunk)
                        if len(raw) > 2 * 1024 * 1024:
                            raise ProtocolError(f"Слишком большой ответ от {safe_target}")
                    try:
                        data = json.loads(raw) if raw else {}
                    except (ValueError, UnicodeDecodeError):
                        data = None
                    if not 200 <= response.status < 300:
                        detail = str(data.get("message", data.get("error", ""))) if isinstance(data, dict) else "Ответ не JSON"
                        try:
                            retry_after = min(30.0, max(0.0, float(response.headers.get("Retry-After", "0"))))
                        except ValueError:
                            retry_after = 0
                        raise RemoteError(response.status, detail, retry_after)
                    if data is None:
                        raise ProtocolError(f"Некорректный JSON от {safe_target}")
                    return data
            except RemoteError as exc:
                if method != "GET" or attempt or (exc.status != 429 and exc.status < 500):
                    raise
                await asyncio.sleep(max(1.0, exc.retry_after))
            except (aiohttp.ClientError, TimeoutError) as exc:
                if method != "GET" or attempt:
                    raise NetworkError(f"{method} {safe_target}: {type(exc).__name__}") from None
                await asyncio.sleep(1)
        raise AssertionError("unreachable")
