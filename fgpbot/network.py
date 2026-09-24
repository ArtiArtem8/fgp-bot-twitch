"""Make bounded HTTP requests with explicit retry behavior."""

import asyncio
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, overload
from urllib.parse import urlsplit

import aiohttp
import msgspec

from .security import REDACT

if TYPE_CHECKING:
    from collections.abc import Mapping

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


class _ErrorBody(msgspec.Struct):
    message: str = ""
    error: str = ""


class Http:
    """Apply proxy and retry policy to Twitch HTTP requests."""

    def __init__(self, session: aiohttp.ClientSession, proxy: str | None) -> None:
        self.session = session
        self.proxy = proxy

    @staticmethod
    async def _read_response[T](
        response: aiohttp.ClientResponse, safe_target: str, model: type[T]
    ) -> T:
        # Consume the complete body with a hard cap; read(n) may return one chunk.
        raw = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            raw.extend(chunk)
            if len(raw) > 2 * 1024 * 1024:
                raise ProtocolError(f"Слишком большой ответ от {safe_target}")
        if not HTTPStatus.OK <= response.status < HTTPStatus.MULTIPLE_CHOICES:
            try:
                error = msgspec.json.decode(raw, type=_ErrorBody)
                detail = error.message or error.error
            except msgspec.DecodeError:
                detail = "Ответ не JSON"
            try:
                retry_after = min(30.0, max(0.0, float(response.headers.get("Retry-After", "0"))))
            except ValueError:
                retry_after = 0
            raise RemoteError(response.status, detail, retry_after)
        try:
            return msgspec.json.decode(raw or b"{}", type=model)
        except msgspec.DecodeError:
            raise ProtocolError(f"Некорректный JSON или тип поля от {safe_target}") from None

    async def _request_once[T](  # ruff: ignore[too-many-arguments] - transport options mirror aiohttp
        self,
        method: str,
        url: str,
        safe_target: str,
        model: type[T],
        *,
        headers: Mapping[str, str] | None,
        params: Mapping[str, str] | list[tuple[str, str]] | None,
        json: object,
        data: Mapping[str, str] | None,
    ) -> T:
        LOG.debug("HTTP %s %s", method, safe_target)
        async with self.session.request(
            method,
            url,
            proxy=self.proxy,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=12, connect=5, sock_read=8),
            headers=headers,
            params=params,
            json=json,
            data=data,
        ) as response:
            return await self._read_response(response, safe_target, model)

    @overload
    async def request[T](
        self, method: str, url: str, *, model: type[T], **kwargs: object
    ) -> T: ...

    @overload
    async def request(self, method: str, url: str, **kwargs: object) -> object: ...

    async def request(  # ruff: ignore[too-many-arguments] - transport options mirror aiohttp
        self,
        method: str,
        url: str,
        *,
        model: type[object] = object,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | list[tuple[str, str]] | None = None,
        json: object = None,
        data: Mapping[str, str] | None = None,
    ) -> object:
        """Retry safe GETs once. Never replay an ambiguous POST."""
        target = urlsplit(url)
        safe_target = f"{target.hostname}{target.path}"
        for attempt in range(2 if method == "GET" else 1):
            try:
                return await self._request_once(
                    method,
                    url,
                    safe_target,
                    model,
                    headers=headers,
                    params=params,
                    json=json,
                    data=data,
                )
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
