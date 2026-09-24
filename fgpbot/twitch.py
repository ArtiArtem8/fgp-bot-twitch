"""Call Twitch APIs for users, subscriptions, and chat delivery."""

import asyncio
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, overload

from .config import CHAT_SCOPES
from .network import ProtocolError, RemoteError
from .wire import SendChat, Subscriptions, Users

if TYPE_CHECKING:
    from .config import Config
    from .network import Http
    from .tokens import Tokens
    from .wire import Subscription, User

HELIX = "https://api.twitch.tv/helix"


class DeliveryError(Exception):
    """Twitch did not confirm delivery; must not be logged as a sent message."""


def chat_text(text: str, limit: int = 500) -> str:
    """Clamp outbound text to the Twitch chat size limit."""
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


class Twitch:
    """Wrap the Twitch API calls used by this bot."""

    def __init__(self, config: Config, http: Http, tokens: Tokens) -> None:
        self.config, self.http, self.tokens = config, http, tokens
        self._send_lock = asyncio.Lock()
        self._next_send = 0.0

    @overload
    async def request[T](
        self, method: str, path: str, *, model: type[T], **kwargs: object
    ) -> T: ...

    @overload
    async def request(self, method: str, path: str, **kwargs: object) -> object: ...

    async def request(
        self,
        method: str,
        path: str,
        *,
        user_id: str | None = None,
        scopes: frozenset[str] = frozenset(),
        model: type[object] = object,
        **kwargs: object,
    ) -> object:
        user_id = user_id or self.config.bot_id
        for attempt in range(2):
            token = await self.tokens.get(user_id, scopes)
            try:
                return await self.http.request(
                    method,
                    HELIX + path,
                    headers={
                        "Client-Id": self.config.client_id,
                        "Authorization": f"Bearer {token.access}",
                    },
                    model=model,
                    **kwargs,
                )
            except RemoteError as exc:
                if exc.status != HTTPStatus.UNAUTHORIZED or attempt:
                    raise
                # A received 401 means authentication rejected the operation.
                # Unlike a timeout, it is safe to retry once with a refreshed token.
                self.tokens.invalidate(user_id, token.access)
        raise AssertionError("unreachable")

    async def users(self, *, ids: list[str] | None = None, login: str = "") -> list[User]:
        params = [("id", value) for value in ids] if ids else [("login", login)]
        result = await self.request("GET", "/users", params=params, model=Users)
        return result.data

    async def subscriptions(self) -> list[Subscription]:
        result: list[Subscription] = []
        params: dict[str, str] = {}
        cursors: set[str] = set()
        for _ in range(100):
            page = await self.request(
                "GET", "/eventsub/subscriptions", params=params, model=Subscriptions
            )
            result.extend(page.data)
            cursor = page.pagination.cursor
            if not cursor:
                return result
            if cursor in cursors:
                raise ProtocolError("EventSub pagination повторяет cursor")
            cursors.add(cursor)
            params["after"] = cursor
        raise ProtocolError("Слишком много страниц EventSub subscriptions")

    def matches(self, sub: Subscription | None, session_id: str, kind: str) -> bool:
        if sub is None:
            return False
        return bool(
            sub.type == kind
            and sub.version == "1"
            and sub.status == "enabled"
            and sub.condition.broadcaster_user_id == self.config.channel_id
            and (kind != "channel.chat.message" or sub.condition.user_id == self.config.bot_id)
            and sub.transport.method == "websocket"
            and sub.transport.session_id == session_id
        )

    async def subscribe(self, session_id: str, kind: str) -> Subscription:
        condition = {"broadcaster_user_id": self.config.channel_id}
        if kind == "channel.chat.message":
            condition["user_id"] = self.config.bot_id
        try:
            result = await self.request(
                "POST",
                "/eventsub/subscriptions",
                scopes=CHAT_SCOPES,
                json={
                    "type": kind,
                    "version": "1",
                    "condition": condition,
                    "transport": {"method": "websocket", "session_id": session_id},
                },
                model=Subscriptions,
            )
            candidates = result.data
        except RemoteError as exc:
            if exc.status != HTTPStatus.CONFLICT:
                raise
            candidates = await self.subscriptions()
        for sub in candidates:
            if self.matches(sub, session_id, kind) and sub.id:
                return sub
        raise ProtocolError(f"Нет подтверждённой подписки {kind} для текущего WebSocket")

    async def send(self, text: str, *, reply_to: str | None = None) -> str:
        """All messages target ONE configured broadcaster. Check is_sent, not just HTTP 200.

        With user tokens Twitch itself may relay messages into Shared Chat.
        We do not claim for_source_only works here: it causes HTTP 400.
        """
        text = chat_text(text)
        if not text:
            raise ValueError("Пустое сообщение")
        async with self._send_lock:
            await asyncio.sleep(max(0.0, self._next_send - time.monotonic()))
            self._next_send = time.monotonic() + 1.6  # Below 20/30s and 1/s.
            body = {
                "broadcaster_id": self.config.channel_id,
                "sender_id": self.config.bot_id,
                "message": text,
            }
            if reply_to:
                body["reply_parent_message_id"] = reply_to
            result = await self.request(
                "POST",
                "/chat/messages",
                scopes=frozenset({"user:write:chat"}),
                json=body,
                model=SendChat,
            )
            if len(result.data) != 1:
                raise ProtocolError("Send Chat Message: неполный ответ")
            message = result.data[0]
            if not message.is_sent:
                reason = message.drop_reason
                raise DeliveryError(
                    "Twitch не отправил сообщение: "
                    f"{reason.code if reason else 'unknown'} {reason.message if reason else ''}"
                )
            message_id = message.message_id
            if not message_id:
                raise ProtocolError("Twitch не вернул message_id отправленного сообщения")
            return message_id
