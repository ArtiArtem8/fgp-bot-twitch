from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import msgspec

from fgpbot.config import CHAT_SCOPES, Config
from fgpbot.health import Health
from fgpbot.storage import Store
from fgpbot.tokens import Token
from fgpbot.wire import Subscription, eventsub

if TYPE_CHECKING:
    from collections.abc import Callable

    from fgpbot.wire import Frame

type Payload = dict[str, Any]


def config(root: Path, **kwargs: object) -> Config:
    return replace(
        Config(
            client_id="test-client-id",
            client_secret="synthetic-client-secret",  # ruff: ignore[hardcoded-password-func-arg] - synthetic fixture
            bot_id="100",
            channel_id="200",
            owner_id="200",
            root=root,
            proxy=None,
            greet_stream=False,
            command_cooldown=0,
        ),
        **kwargs,
    )


def identity(user_id: str = "100", **kwargs: object) -> Payload:
    return {
        "client_id": "test-client-id",
        "user_id": user_id,
        "login": "fgp_test_bot",
        "scopes": list(CHAT_SCOPES),
        "expires_in": 14400,
        **kwargs,
    }


def token() -> Token:
    now = time.monotonic()
    return Token(
        "100",
        "fgp_test_bot",
        CHAT_SCOPES,
        "synthetic-access",
        "synthetic-refresh",
        now,
        now + 14400,
    )


def subscription(
    session: str = "session-1", kind: str = "channel.chat.message", **kwargs: object
) -> Payload:
    condition = {"broadcaster_user_id": "200"}
    if kind == "channel.chat.message":
        condition["user_id"] = "100"
    return {
        "id": "subscription-" + kind,
        "type": kind,
        "version": "1",
        "status": "enabled",
        "condition": condition,
        "transport": {"method": "websocket", "session_id": session},
        **kwargs,
    }


def notification(
    text: str = "!ping",
    *,
    message_id: str = "viewer-message-1",
    user_id: str = "300",
    channel: str = "200",
    metadata_id: str | None = None,
    source: str | None = None,
    kind: str = "channel.chat.message",
) -> Payload:
    sub = subscription(kind=kind)
    sub["condition"]["broadcaster_user_id"] = channel
    event = {
        "broadcaster_user_id": channel,
        "broadcaster_user_name": "Streamer",
        "message_id": message_id,
        "chatter_user_id": user_id,
        "chatter_user_login": "viewer",
        "chatter_user_name": "Viewer",
        "message": {"text": text},
        "badges": [],
        "message_type": "text",
        "source_broadcaster_user_id": source,
    }
    if kind == "stream.online":
        event["id"] = message_id
    return {
        "metadata": {
            "message_type": "notification",
            "message_id": metadata_id or message_id,
            "message_timestamp": "2026-09-23T12:00:00Z",
            "subscription_type": kind,
        },
        "payload": {"subscription": sub, "event": event},
    }


def typed_notification(
    text: str = "!ping",
    *,
    message_id: str = "viewer-message-1",
    user_id: str = "300",
    channel: str = "200",
    metadata_id: str | None = None,
    source: str | None = None,
    kind: str = "channel.chat.message",
) -> Frame:
    """Pass a wire fixture through the same decoder as a live WebSocket frame."""
    return eventsub(
        msgspec.json.encode(
            notification(
                text,
                message_id=message_id,
                user_id=user_id,
                channel=channel,
                metadata_id=metadata_id,
                source=source,
                kind=kind,
            )
        )
    )


def welcome(session: str = "session-1", keepalive: int | None = 30) -> Payload:
    return {
        "metadata": {"message_type": "session_welcome"},
        "payload": {"session": {"id": session, "keepalive_timeout_seconds": keepalive}},
    }


def ready(state: Health) -> None:
    state.auth_ok = state.ws_connected = True
    state.api_ok = True
    state.phase = "LISTENING"
    state.session_id = "session-1"
    state.subscriptions["channel.chat.message"] = "sub-1"
    state.frame_received()


def fake_api() -> SimpleNamespace:
    return SimpleNamespace(
        http=SimpleNamespace(request=AsyncMock()),
        send=AsyncMock(return_value="sent-1"),
        users=AsyncMock(return_value=[]),
        request=AsyncMock(),
        tokens=SimpleNamespace(get=AsyncMock(return_value=token()), invalidate=lambda *_: None),
        subscribe=AsyncMock(
            side_effect=lambda session, kind: msgspec.convert(
                subscription(session, kind), type=Subscription
            )
        ),
    )


async def until(predicate: Callable[[], bool], timeout: float = 4) -> None:  # ruff: ignore[async-function-with-timeout] - uses asyncio.timeout
    async with asyncio.timeout(timeout):
        while not predicate():  # ruff: ignore[async-busy-wait] - arbitrary predicate has no event
            await asyncio.sleep(0.01)


async def cancel(task: asyncio.Task[object]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


class StoreCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="fgpbot-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = config(self.root)
        self.store = Store(self.config.database)
        await self.store.initialize()
        self.state = Health(self.config.bot_id, self.config.channel_id)

    async def token_row(self, user_id: str) -> Payload:
        row = await self.store.token(user_id)
        if row is None:
            raise AssertionError(f"Expected stored token for {user_id}")
        return row

    async def probe_row(self, nonce: str) -> Payload:
        row = await self.store.probe(nonce)
        if row is None:
            raise AssertionError(f"Expected stored probe for {nonce}")
        return row
