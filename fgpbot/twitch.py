"""Call Twitch APIs for users, subscriptions, and chat delivery."""

from __future__ import annotations

import asyncio
import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from .config import CHAT_SCOPES
from .network import ProtocolError, RemoteError

if TYPE_CHECKING:
    from .config import Config
    from .network import Http
    from .tokens import Tokens

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

    async def request(
        self,
        method: str,
        path: str,
        *,
        user_id: str | None = None,
        scopes: frozenset[str] = frozenset(),
        **kwargs: Any,
    ) -> Any:
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
                    **kwargs,
                )
            except RemoteError as exc:
                if exc.status != HTTPStatus.UNAUTHORIZED or attempt:
                    raise
                # A received 401 means authentication rejected the operation.
                # Unlike a timeout, it is safe to retry once with a refreshed token.
                self.tokens.invalidate(user_id, token.access)
        raise AssertionError("unreachable")

    async def users(
        self, *, ids: list[str] | None = None, login: str = ""
    ) -> list[dict[str, Any]]:
        params = [("id", value) for value in ids] if ids else [("login", login)]
        result = await self.request("GET", "/users", params=params)
        if (
            not isinstance(result, dict)
            or not isinstance(result.get("data"), list)
            or not all(isinstance(user, dict) for user in result["data"])
        ):
            raise ProtocolError("Get Users: нет массива data")
        return result["data"]

    async def subscriptions(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        params: dict[str, str] = {}
        cursors: set[str] = set()
        for _ in range(100):
            page = await self.request("GET", "/eventsub/subscriptions", params=params)
            if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                raise ProtocolError("EventSub subscriptions: нет массива data")
            result.extend(page["data"])
            cursor = page.get("pagination", {}).get("cursor")
            if not cursor:
                return result
            if cursor in cursors:
                raise ProtocolError("EventSub pagination повторяет cursor")
            cursors.add(cursor)
            params["after"] = cursor
        raise ProtocolError("Слишком много страниц EventSub subscriptions")

    def matches(self, sub: dict[str, Any] | None, session_id: str, kind: str) -> bool:
        if not isinstance(sub, dict):
            return False
        condition, transport = sub.get("condition"), sub.get("transport")
        if not isinstance(condition, dict) or not isinstance(transport, dict):
            return False
        return bool(
            sub.get("type") == kind
            and sub.get("version") == "1"
            and sub.get("status") == "enabled"
            and condition.get("broadcaster_user_id") == self.config.channel_id
            and (kind != "channel.chat.message" or condition.get("user_id") == self.config.bot_id)
            and sub.get("transport", {}).get("method") == "websocket"
            and sub.get("transport", {}).get("session_id") == session_id
        )

    async def subscribe(self, session_id: str, kind: str) -> dict[str, Any]:
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
            )
            if not isinstance(result, dict) or not isinstance(result.get("data"), list):
                raise ProtocolError("Create EventSub Subscription: нет массива data")
            candidates = result["data"]
        except RemoteError as exc:
            if exc.status != HTTPStatus.CONFLICT:
                raise
            candidates = await self.subscriptions()
        for sub in candidates:
            if self.matches(sub, session_id, kind) and sub.get("id"):
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
                "POST", "/chat/messages", scopes=frozenset({"user:write:chat"}), json=body
            )
            data = result.get("data", []) if isinstance(result, dict) else None
            if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
                raise ProtocolError("Send Chat Message: неполный ответ")
            message = data[0]
            if message.get("is_sent") is not True:
                reason = message.get("drop_reason") or {}
                if not isinstance(reason, dict):
                    reason = {"code": "unknown", "message": str(reason)}
                raise DeliveryError(
                    "Twitch не отправил сообщение: "
                    f"{reason.get('code', 'unknown')} {reason.get('message', '')}"
                )
            message_id = message.get("message_id")
            if not isinstance(message_id, str) or not message_id:
                raise ProtocolError("Twitch не вернул message_id отправленного сообщения")
            return message_id
