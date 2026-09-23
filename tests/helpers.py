from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fgpbot.config import CHAT_SCOPES, Config
from fgpbot.health import Health
from fgpbot.storage import Store
from fgpbot.tokens import Token


def config(root: Path, **kwargs) -> Config:
    return replace(Config(client_id="test-client-id", client_secret="synthetic-client-secret",
                          bot_id="100", channel_id="200", owner_id="200", root=root,
                          proxy=None, greet_stream=False, command_cooldown=0), **kwargs)


def identity(user_id="100", **kwargs) -> dict:
    return {"client_id": "test-client-id", "user_id": user_id, "login": "fgp_test_bot",
            "scopes": list(CHAT_SCOPES), "expires_in": 14400, **kwargs}


def token() -> Token:
    now = time.monotonic()
    return Token("100", "fgp_test_bot", CHAT_SCOPES, "synthetic-access", "synthetic-refresh", now, now+14400)


def subscription(session="session-1", kind="channel.chat.message", **kwargs) -> dict:
    condition = {"broadcaster_user_id": "200"}
    if kind == "channel.chat.message":
        condition["user_id"] = "100"
    return {"id": "subscription-" + kind, "type": kind, "version": "1", "status": "enabled",
            "condition": condition, "transport": {"method": "websocket", "session_id": session}, **kwargs}


def notification(text="!ping", *, message_id="viewer-message-1", user_id="300", channel="200",
                 metadata_id=None, source=None, kind="channel.chat.message") -> dict:
    sub = subscription(kind=kind)
    sub["condition"]["broadcaster_user_id"] = channel
    event = {"broadcaster_user_id": channel, "broadcaster_user_name": "Streamer",
             "message_id": message_id, "chatter_user_id": user_id, "chatter_user_login": "viewer",
             "chatter_user_name": "Viewer", "message": {"text": text}, "badges": [],
             "message_type": "text", "source_broadcaster_user_id": source}
    if kind == "stream.online":
        event["id"] = message_id
    return {"metadata": {"message_type": "notification", "message_id": metadata_id or message_id,
                         "message_timestamp": "2026-09-23T12:00:00Z", "subscription_type": kind},
            "payload": {"subscription": sub, "event": event}}


def welcome(session="session-1", keepalive=30) -> dict:
    return {"metadata": {"message_type": "session_welcome"},
            "payload": {"session": {"id": session, "keepalive_timeout_seconds": keepalive}}}


def ready(state: Health) -> None:
    state.auth_ok = state.ws_connected = True
    state.api_ok = True
    state.phase = "LISTENING"
    state.session_id = "session-1"
    state.subscriptions["channel.chat.message"] = "sub-1"
    state.frame_received()


def fake_api() -> SimpleNamespace:
    return SimpleNamespace(http=SimpleNamespace(request=AsyncMock()), send=AsyncMock(return_value="sent-1"),
                           users=AsyncMock(return_value=[]), request=AsyncMock(),
                           tokens=SimpleNamespace(get=AsyncMock(return_value=token()), invalidate=lambda *_: None),
                           subscribe=AsyncMock(side_effect=lambda sid, kind: subscription(sid, kind)))


async def until(predicate, timeout=4):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(.01)


async def cancel(task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


class StoreCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="fgpbot-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = config(self.root)
        self.store = Store(self.config.database)
        await self.store.initialize()
        self.state = Health(self.config.bot_id, self.config.channel_id)
