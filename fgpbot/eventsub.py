from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from collections import OrderedDict
from typing import Any, Callable
from urllib.parse import urlsplit

import aiohttp

from .config import CHAT_SCOPES, Config
from .health import Health
from .network import NetworkError, ProtocolError, RemoteError
from .tokens import AuthRequired
from .twitch import Twitch

LOG = logging.getLogger(__name__)
WS_URL = "wss://eventsub.wss.twitch.tv/ws?keepalive_timeout_seconds=30"


class TransportError(Exception):
    pass


class RecentIDs:
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


def reconnect_url(value: object) -> str:
    # Only a Twitch-issued TLS URL is allowed; never pass tokens to an arbitrary URL.
    if not isinstance(value, str):
        raise ProtocolError("EventSub reconnect_url отсутствует")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ProtocolError("EventSub прислал недопустимый reconnect_url") from None
    if (parsed.scheme != "wss" or parsed.hostname != "eventsub.wss.twitch.tv"
            or parsed.username or parsed.password or parsed.fragment or port not in {None, 443}):
        raise ProtocolError("EventSub прислал недопустимый reconnect_url")
    return value


class EventSub:
    def __init__(self, config: Config, api: Twitch, state: Health,
                 enqueue: Callable[[dict], None]) -> None:
        self.config, self.api, self.state, self.enqueue = config, api, state, enqueue
        self._reset = asyncio.Event()
        self._recent = RecentIDs()

    def reset(self, reason: str) -> None:
        self.state.error(reason)
        self._reset.set()

    async def _open(self, url: str) -> tuple[Any, dict]:
        ws = None
        try:
            # Timeout covers CONNECT, TLS, WS handshake AND the welcome frame.
            async with asyncio.timeout(15):
                ws = await self.api.http.session.ws_connect(
                    url, proxy=self.config.proxy, autoping=True, heartbeat=None,
                    max_msg_size=2 * 1024 * 1024,
                    timeout=aiohttp.ClientWSTimeout(ws_close=5),
                )
                frame = await self._receive(ws, timeout=10)
                if frame.get("metadata", {}).get("message_type") != "session_welcome":
                    raise ProtocolError("Первый EventSub frame — не session_welcome")
                session = frame["payload"].get("session")
                if not isinstance(session, dict) or not isinstance(session.get("id"), str) or not session["id"]:
                    raise ProtocolError("EventSub welcome без session_id")
                keepalive = session.get("keepalive_timeout_seconds")
                if keepalive is not None and (not isinstance(keepalive, int) or not 1 <= keepalive <= 600):
                    raise ProtocolError("Некорректный EventSub keepalive timeout")
                return ws, session
        except BaseException:
            if ws is not None:
                await ws.close()
            raise

    async def _receive(self, ws: Any, timeout: float) -> dict:
        async with asyncio.timeout(timeout):
            frame = await ws.receive()
        if frame.type != aiohttp.WSMsgType.TEXT:
            raise TransportError(f"EventSub WebSocket закрыт: type={frame.type.name}, code={ws.close_code}")
        try:
            data = json.loads(frame.data)
            if not isinstance(data, dict) or not isinstance(data.get("metadata"), dict) or not isinstance(data.get("payload"), dict):
                raise ValueError
            return data
        except (ValueError, TypeError):
            raise ProtocolError("Некорректный JSON EventSub") from None

    def _welcome(self, session: dict) -> None:
        self.state.session_id = session["id"]
        # Twitch may send null on a graceful reconnect; retain the previous timeout.
        self.state.keepalive_timeout = session.get("keepalive_timeout_seconds") or self.state.keepalive_timeout
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
                self.state.subscriptions["stream.online"] = online["id"]
                self.state.features["greeting"] = "READY"
            except (RemoteError, NetworkError, ProtocolError, TimeoutError) as exc:
                self.state.features["greeting"] = "UNAVAILABLE"
                LOG.warning("Приветствия недоступны, чат продолжает работать: %s", exc)
        self.state.phase = "LISTENING"
        self.state.api_ok = True
        self.state.last_error = ""
        LOG.info("CHAT SUBSCRIBED | channel=%s (%s) | bot=%s | session=%s",
                 self.state.channel_login, self.config.channel_id, self.state.bot_login, session_id)

    def _notification(self, frame: dict) -> None:
        payload, metadata = frame["payload"], frame["metadata"]
        kind = metadata.get("message_type")
        if kind == "session_keepalive":
            return
        if kind == "revocation":
            sub = payload.get("subscription", {})
            event_type = sub.get("type", "unknown")
            self.state.subscriptions.pop(event_type, None)
            LOG.error("EventSub revocation | type=%s | reason=%s", event_type, sub.get("status"))
            if event_type == "channel.chat.message":
                raise TransportError(f"Подписка на чат отозвана: {sub.get('status')}")
            self.state.features["greeting"] = "REVOKED"
            return
        if kind != "notification":
            raise ProtocolError(f"Неожиданный тип EventSub: {kind}")
        sub, event = payload.get("subscription", {}), payload.get("event", {})
        if not isinstance(sub, dict) or not isinstance(sub.get("condition"), dict):
            raise ProtocolError("EventSub notification без корректной subscription/condition")
        if not isinstance(event, dict) or event.get("broadcaster_user_id") != self.config.channel_id:
            self.state.filtered_events += 1
            return
        if sub.get("type") not in {"channel.chat.message", "stream.online"}:
            return
        if sub.get("condition", {}).get("broadcaster_user_id") != self.config.channel_id:
            self.state.filtered_events += 1
            return
        if sub.get("type") == "channel.chat.message" and sub.get("condition", {}).get("user_id") != self.config.bot_id:
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
        read: asyncio.Task | None = None
        handoff: asyncio.Task | None = None
        reset = asyncio.create_task(self._reset.wait())
        try:
            while True:
                if read is None:
                    read = asyncio.create_task(self._receive(ws, self.state.keepalive_timeout + 5))
                tasks = {read, reset}
                if handoff:
                    tasks.add(handoff)
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if reset in done:
                    raise TransportError(self.state.last_error or "Запрошено переподключение")
                # Drain an already-received old-socket frame before swapping sockets.
                if read in done:
                    try:
                        frame = read.result()
                    except TransportError:
                        if handoff is None:
                            raise
                        # The old edge can disappear while the new welcome is in
                        # flight. Finish that bounded handshake, not a third socket.
                        await handoff
                        done.add(handoff)
                        frame = None
                    read = None
                    if frame is not None:
                        self.state.frame_received()
                        kind = frame["metadata"].get("message_type")
                        if kind == "session_reconnect":
                            if handoff is None:
                                url = reconnect_url(frame["payload"].get("session", {}).get("reconnect_url"))
                                handoff = asyncio.create_task(self._open(url), name="eventsub-handoff")
                                LOG.info("EventSub handoff: старое соединение читается до welcome нового")
                        else:
                            self._notification(frame)
                if handoff and handoff in done:
                    new_ws, session = handoff.result()
                    handoff = None
                    if read:
                        read.cancel()
                        await asyncio.gather(read, return_exceptions=True)
                        read = None
                    old_ws, ws = ws, new_ws
                    self._welcome(session)
                    await old_ws.close()
                    # Subscriptions transfer automatically. DO NOT recreate them.
                    LOG.info("EventSub handoff завершён | session=%s", session["id"])
        finally:
            for task in (read, reset, handoff):
                if task and not task.done():
                    task.cancel()
            await asyncio.gather(*(t for t in (read, reset, handoff) if t), return_exceptions=True)
            # If handoff finished simultaneously with cancellation, close its socket too.
            if handoff and handoff.done() and not handoff.cancelled():
                with contextlib.suppress(Exception):
                    other_ws, _ = handoff.result()
                    await other_ws.close()
            await ws.close()

    async def run(self) -> None:
        failures = 0
        while True:
            self._reset.clear()
            self.state.phase = "CONNECTING"
            self.state.e2e = "UNVERIFIED"
            started = time.monotonic()
            ws = None
            delay = 0.0
            try:
                token = await self.api.tokens.get(self.config.bot_id, CHAT_SCOPES)
                self.state.bot_login, self.state.auth_ok = token.login, True
                users = await self.api.users(ids=[self.config.channel_id])
                if not users or users[0].get("id") != self.config.channel_id:
                    raise AuthRequired("Целевой TWITCH_CHANNEL_ID не найден; проверь .env")
                self.state.channel_login = users[0]["login"]
                ws, session = await self._open(WS_URL)
                self._welcome(session)
                await self._fresh(session["id"])
                await self._listen(ws)
            except AuthRequired as exc:
                self.state.auth_ok = False
                self.state.phase = "AUTH_REQUIRED"
                self.state.error(exc)
                if failures == 0 or self.state.reconnects % 6 == 0:
                    LOG.error("AUTH REQUIRED | %s", exc)
                delay = 10  # Re-read DB soon after a successful local `auth`.
            except (RemoteError, NetworkError, TransportError, ProtocolError,
                    aiohttp.ClientError, TimeoutError, OSError) as exc:
                self.state.phase = "RECONNECTING"
                self.state.error(exc)
                if time.monotonic() - started >= 60:
                    failures = 0
                cap = min(60.0, 2.0 ** min(failures + 1, 6))
                delay = random.uniform(cap / 2, cap)
                LOG.warning("EventSub недоступен; повтор через %.1fs | %s", delay, exc)
            finally:
                self.state.ws_connected = False
                self.state.subscriptions.clear()
                if ws is not None:
                    await ws.close()
            failures += 1
            self.state.reconnects += 1
            await asyncio.sleep(delay)
