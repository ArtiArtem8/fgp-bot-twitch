"""Receive and validate Twitch EventSub WebSocket messages."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import aiohttp

from .config import CHAT_SCOPES
from .network import NetworkError, ProtocolError, RemoteError
from .tokens import AuthRequiredError

if TYPE_CHECKING:
    from collections.abc import Callable

    from .config import Config
    from .health import Health
    from .twitch import Twitch

LOG = logging.getLogger(__name__)
WS_URL = "wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds=30"
MIN_KEEPALIVE_SECONDS = 1
MAX_KEEPALIVE_SECONDS = 600
FAILURE_RESET_SECONDS = 60


class TransportError(Exception):
    """Report an invalid EventSub transport or session frame."""


class RecentIDs:
    """Bound duplicate detection by time and capacity."""

    def __init__(self, limit: int = 4096, ttl: float = 600) -> None:
        self.limit, self.ttl = limit, ttl
        self.items: OrderedDict[str, float] = OrderedDict()

    def seen(self, key: str) -> bool:
        now = time.monotonic()
        while self.items and now - next(iter(self.items.values())) >= self.ttl:
            self.items.popitem(last=False)
        if key in self.items:
            return True
        self.items[key] = now
        while len(self.items) > self.limit:
            self.items.popitem(last=False)
        return False


@dataclass
class _Connection:
    ws: Any
    reset: asyncio.Task[bool]
    read: asyncio.Task[dict[str, Any]] | None = None
    handoff: asyncio.Task[tuple[Any, dict[str, Any]]] | None = None


def reconnect_url(value: object) -> str:
    # Only a Twitch-issued TLS URL is allowed; never pass tokens to an arbitrary URL.
    """Accept only a secure Twitch EventSub reconnect URL."""
    if not isinstance(value, str):
        raise ProtocolError("EventSub reconnect_url отсутствует")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ProtocolError("EventSub прислал недопустимый reconnect_url") from None
    if parsed.scheme != "wss" or parsed.hostname != "eventsub.wss.twitch.tv":
        raise ProtocolError("EventSub прислал недопустимый reconnect_url")
    if parsed.username or parsed.password or parsed.fragment or port not in {None, 443}:
        raise ProtocolError("EventSub прислал недопустимый reconnect_url")
    return value


class EventSub:
    """Maintain the EventSub connection and subscription lifecycle."""

    def __init__(
        self, config: Config, api: Twitch, state: Health, enqueue: Callable[[dict[str, Any]], None]
    ) -> None:
        self.config, self.api, self.state, self.enqueue = config, api, state, enqueue
        self._reset = asyncio.Event()
        self._recent = RecentIDs()

    def reset(self, reason: str) -> None:
        self.state.error(reason)
        self._reset.set()

    async def _open(self, url: str) -> tuple[Any, dict[str, Any]]:
        ws = None
        try:
            # Timeout covers CONNECT, TLS, WS handshake AND the welcome frame.
            async with asyncio.timeout(15):
                ws = await self.api.http.session.ws_connect(
                    url,
                    proxy=self.config.proxy,
                    autoping=True,
                    heartbeat=None,
                    max_msg_size=2 * 1024 * 1024,
                    timeout=aiohttp.ClientWSTimeout(ws_close=5),  # ty: ignore[unknown-argument] - aiohttp 3.14 accepts ws_close; ty omits it
                )
                frame = await self._receive(ws, timeout=10)
                session = self._validate_welcome(frame)
        except BaseException:
            if ws is not None:
                await ws.close()
            raise
        return ws, session

    @staticmethod
    def _validate_welcome(frame: dict[str, Any]) -> dict[str, Any]:
        if frame.get("metadata", {}).get("message_type") != "session_welcome":
            raise ProtocolError("Первый EventSub frame — не session_welcome")
        session = frame["payload"].get("session")
        if (
            not isinstance(session, dict)
            or not isinstance(session.get("id"), str)
            or not session["id"]
        ):
            raise ProtocolError("EventSub welcome без session_id")
        keepalive = session.get("keepalive_timeout_seconds")
        if keepalive is not None and (
            not isinstance(keepalive, int)
            or not MIN_KEEPALIVE_SECONDS <= keepalive <= MAX_KEEPALIVE_SECONDS
        ):
            raise ProtocolError("Некорректный EventSub keepalive timeout")
        return session

    @staticmethod
    async def _receive(ws: Any, timeout: float) -> dict[str, Any]:  # ruff: ignore[async-function-with-timeout] - uses asyncio.timeout
        async with asyncio.timeout(timeout):
            frame = await ws.receive()
        if frame.type != aiohttp.WSMsgType.TEXT:
            raise TransportError(
                f"EventSub WebSocket закрыт: type={frame.type.name}, code={ws.close_code}"
            )
        try:
            data = json.loads(frame.data)
            if (
                not isinstance(data, dict)
                or not isinstance(data.get("metadata"), dict)
                or not isinstance(data.get("payload"), dict)
            ):
                raise TypeError
        except ValueError, TypeError:
            raise ProtocolError("Некорректный JSON EventSub") from None
        return data

    def _welcome(self, session: dict[str, Any]) -> None:
        self.state.session_id = session["id"]
        # Twitch may send null on a graceful reconnect; retain the previous timeout.
        self.state.keepalive_timeout = (
            session.get("keepalive_timeout_seconds") or self.state.keepalive_timeout
        )
        self.state.ws_connected = True
        self.state.frame_received()

    async def _fresh(self, session_id: str) -> None:
        self.state.phase = "SUBSCRIBING"
        self.state.subscriptions.clear()
        # First subscription must be created in the welcome subscription window.
        async with asyncio.timeout(9):
            chat = await self.api.subscribe(session_id, "channel.chat.message")
        self.state.subscriptions["channel.chat.message"] = chat["id"]
        if self.config.greet_stream:
            try:
                online = await self.api.subscribe(session_id, "stream.online")
            except (RemoteError, NetworkError, ProtocolError, TimeoutError) as exc:
                self.state.features["greeting"] = "UNAVAILABLE"
                LOG.warning("Приветствия недоступны, чат продолжает работать: %s", exc)
            else:
                self.state.subscriptions["stream.online"] = online["id"]
                self.state.features["greeting"] = "READY"
        self.state.phase = "LISTENING"
        self.state.api_ok = True
        self.state.last_error = ""
        LOG.info(
            "CHAT SUBSCRIBED | channel=%s (%s) | bot=%s | session=%s",
            self.state.channel_login,
            self.config.channel_id,
            self.state.bot_login,
            session_id,
        )

    def _notification(self, frame: dict[str, Any]) -> None:
        payload, metadata = frame["payload"], frame["metadata"]
        kind = metadata.get("message_type")
        if kind == "session_keepalive":
            return
        if kind == "revocation":
            self._revocation(payload)
            return
        if kind != "notification":
            raise ProtocolError(f"Неожиданный тип EventSub: {kind}")
        self._event(frame)

    def _revocation(self, payload: dict[str, Any]) -> None:
        sub = payload.get("subscription", {})
        event_type = sub.get("type", "unknown")
        self.state.subscriptions.pop(event_type, None)
        LOG.error("EventSub revocation | type=%s | reason=%s", event_type, sub.get("status"))
        if event_type == "channel.chat.message":
            raise TransportError(f"Подписка на чат отозвана: {sub.get('status')}")
        self.state.features["greeting"] = "REVOKED"

    def _event(self, frame: dict[str, Any]) -> None:
        payload, metadata = frame["payload"], frame["metadata"]
        sub, event = payload.get("subscription", {}), payload.get("event", {})
        if not isinstance(sub, dict) or not isinstance(sub.get("condition"), dict):
            raise ProtocolError("EventSub notification без корректной subscription/condition")
        if (
            not isinstance(event, dict)
            or event.get("broadcaster_user_id") != self.config.channel_id
        ):
            self.state.filtered_events += 1
            return
        if sub.get("type") not in {"channel.chat.message", "stream.online"}:
            return
        if sub.get("condition", {}).get("broadcaster_user_id") != self.config.channel_id:
            self.state.filtered_events += 1
            return
        if (
            sub.get("type") == "channel.chat.message"
            and sub.get("condition", {}).get("user_id") != self.config.bot_id
        ):
            self.state.filtered_events += 1
            return
        event_id = metadata.get("message_id")
        if not isinstance(event_id, str) or not event_id:
            raise ProtocolError("EventSub notification без message_id")
        if self._recent.seen(event_id):
            self.state.duplicate_events += 1
            return
        self.enqueue(frame)

    async def _listen(self, ws: Any) -> None:
        connection = _Connection(ws=ws, reset=asyncio.create_task(self._reset.wait()))
        try:
            while True:
                await self._tick(connection)
        finally:
            await self._close(connection)

    async def _tick(self, connection: _Connection) -> None:
        if connection.read is None:
            connection.read = asyncio.create_task(
                self._receive(connection.ws, self.state.keepalive_timeout + 5)
            )
        tasks = {connection.read, connection.reset}
        if connection.handoff:
            tasks.add(connection.handoff)
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if connection.reset in done:
            raise TransportError(self.state.last_error or "Запрошено переподключение")
        # Drain an already-received old-socket frame before swapping sockets.
        if connection.read in done:
            await self._drain(connection, done)
        if connection.handoff and connection.handoff in done:
            await self._switch(connection)

    async def _drain(self, connection: _Connection, done: set[asyncio.Task[Any]]) -> None:
        read = connection.read
        if read is None:
            raise RuntimeError("EventSub read task отсутствует")
        try:
            frame = read.result()
        except TransportError:
            if connection.handoff is None:
                raise
            # The old edge can disappear while the new welcome is in flight.
            await connection.handoff
            done.add(connection.handoff)
            frame = None
        connection.read = None
        if frame is None:
            return
        self.state.frame_received()
        if frame["metadata"].get("message_type") != "session_reconnect":
            self._notification(frame)
        elif connection.handoff is None:
            url = reconnect_url(frame["payload"].get("session", {}).get("reconnect_url"))
            connection.handoff = asyncio.create_task(self._open(url), name="eventsub-handoff")
            LOG.info("EventSub handoff: старое соединение читается до welcome нового")

    async def _switch(self, connection: _Connection) -> None:
        handoff = connection.handoff
        if handoff is None:
            raise RuntimeError("EventSub handoff task отсутствует")
        new_ws, session = handoff.result()
        connection.handoff = None
        if connection.read:
            connection.read.cancel()
            await asyncio.gather(connection.read, return_exceptions=True)
            connection.read = None
        old_ws, connection.ws = connection.ws, new_ws
        self._welcome(session)
        await old_ws.close()
        # Subscriptions transfer automatically. DO NOT recreate them.
        LOG.info("EventSub handoff завершён | session=%s", session["id"])

    @staticmethod
    async def _close(connection: _Connection) -> None:
        for task in (connection.read, connection.reset, connection.handoff):
            if task and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (connection.read, connection.reset, connection.handoff) if task),
            return_exceptions=True,
        )
        # If handoff finished simultaneously with cancellation, close its socket too.
        if connection.handoff and connection.handoff.done() and not connection.handoff.cancelled():
            with contextlib.suppress(Exception):
                other_ws, _ = connection.handoff.result()
                await other_ws.close()
        await connection.ws.close()

    async def run(self) -> None:
        failures = 0
        while True:
            self._reset.clear()
            self.state.phase = "CONNECTING"
            self.state.e2e = "UNVERIFIED"
            started = time.monotonic()
            delay = 0.0
            try:
                await self._connect()
            except AuthRequiredError as exc:
                self.state.auth_ok = False
                self.state.phase = "AUTH_REQUIRED"
                self.state.error(exc)
                if failures == 0 or self.state.reconnects % 6 == 0:
                    LOG.error("AUTH REQUIRED | %s", exc)  # ruff: ignore[error-instead-of-exception] - expected authorization state
                delay = 10  # Re-read DB soon after a successful local `auth`.
            except (
                RemoteError,
                NetworkError,
                TransportError,
                ProtocolError,
                aiohttp.ClientError,
                TimeoutError,
                OSError,
            ) as exc:
                self.state.phase = "RECONNECTING"
                self.state.error(exc)
                if time.monotonic() - started >= FAILURE_RESET_SECONDS:
                    failures = 0
                cap = min(60.0, 2.0 ** min(failures + 1, 6))
                delay = random.uniform(cap / 2, cap)  # ruff: ignore[suspicious-non-cryptographic-random-usage] - retry jitter only
                LOG.warning("EventSub недоступен; повтор через %.1fs | %s", delay, exc)
            finally:
                self.state.ws_connected = False
                self.state.subscriptions.clear()
            failures += 1
            self.state.reconnects += 1
            await asyncio.sleep(delay)

    async def _connect(self) -> None:
        token = await self.api.tokens.get(self.config.bot_id, CHAT_SCOPES)
        self.state.bot_login, self.state.auth_ok = token.login, True
        users = await self.api.users(ids=[self.config.channel_id])
        if not users or users[0].get("id") != self.config.channel_id:
            raise AuthRequiredError("Целевой TWITCH_CHANNEL_ID не найден; проверь .env")
        self.state.channel_login = users[0]["login"]
        ws, session = await self._open(WS_URL)
        try:
            self._welcome(session)
            await self._fresh(session["id"])
        except BaseException:
            await ws.close()
            raise
        await self._listen(ws)
