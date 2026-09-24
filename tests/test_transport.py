from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, Mock, patch

import aiohttp
import msgspec

from fgpbot.eventsub import EventSub, RecentIDs, TransportError, reconnect_url
from fgpbot.network import Http, NetworkError, ProtocolError, RemoteError
from fgpbot.twitch import DeliveryError, Twitch
from fgpbot.wire import SendChat, Session, Subscription, Subscriptions, User, eventsub
from tests.helpers import (
    StoreCase,
    cancel,
    fake_api,
    notification,
    ready,
    subscription,
    until,
    welcome,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from fgpbot.wire import Frame


class FakeSocket:
    def __init__(self) -> None:
        self.frames: asyncio.Queue[SimpleNamespace] = asyncio.Queue()
        self.closed = False
        self.close_code = 1000

    async def receive(self) -> SimpleNamespace:
        return await self.frames.get()

    def put(self, data: object) -> None:
        self.frames.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(data)))

    def end(self) -> None:
        self.frames.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSE, data=1000))

    async def close(self) -> None:
        self.closed = True

    def typed(self) -> aiohttp.ClientWebSocketResponse:
        return cast("aiohttp.ClientWebSocketResponse", self)


def wire_frame(value: object) -> Frame:
    return eventsub(msgspec.json.encode(value))


def subscription_page(value: object) -> Subscriptions:
    return msgspec.convert(value, type=Subscriptions)


class URLTests(unittest.TestCase):
    def test_reconnect_only_accepts_exact_twitch_tls_host(self) -> None:
        valid = "wss://eventsub.wss.twitch.tv/ws?reconnect=opaque"
        self.assertEqual(reconnect_url(valid), valid)
        for invalid in (
            None,
            "ws://eventsub.wss.twitch.tv/ws",
            "wss://evil.example/ws",
            "wss://eventsub.wss.twitch.tv.evil.example/ws",
            "wss://user@eventsub.wss.twitch.tv/ws",
            "wss://eventsub.wss.twitch.tv:123/ws",
            "wss://eventsub.wss.twitch.tv:bad/ws",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ProtocolError):
                reconnect_url(invalid)

    def test_recent_ids_are_bounded_and_duplicates_recognized(self) -> None:
        recent = RecentIDs(limit=2)
        self.assertFalse(recent.seen("one"))
        self.assertTrue(recent.seen("one"))
        recent.seen("two")
        recent.seen("three")
        self.assertEqual(len(recent.items), 2)
        self.assertFalse(recent.seen("one"))


class SubscriptionTests(StoreCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.fake = fake_api()
        self.api = Twitch(self.config, self.fake.http, self.fake.tokens)
        self.request = AsyncMock()
        self.request_patch = patch.object(self.api, "request", new=self.request)
        self.request_patch.start()
        self.addCleanup(self.request_patch.stop)

    async def test_subscription_explicitly_uses_bot_identity_and_channel(self) -> None:
        self.request.return_value = subscription_page({"data": [subscription()]})
        await self.api.subscribe("session-1", "channel.chat.message")
        body = self.request.call_args.kwargs["json"]
        self.assertEqual(body["condition"], {"broadcaster_user_id": "200", "user_id": "100"})
        self.assertNotIn("user_id", self.request.call_args.kwargs)  # Default token user = bot.

    async def test_409_is_not_success_when_other_session_owns_subscription(self) -> None:
        self.request.side_effect = [
            RemoteError(409, "duplicate"),
            subscription_page({"data": [subscription("wrong-session")]}),
        ]
        with self.assertRaises(ProtocolError):
            await self.api.subscribe("session-1", "channel.chat.message")

    async def test_409_same_active_session_is_idempotent_success(self) -> None:
        self.request.side_effect = [
            RemoteError(409, "duplicate"),
            subscription_page({"data": [subscription()]}),
        ]
        self.assertEqual(
            (await self.api.subscribe("session-1", "channel.chat.message")).id,
            subscription()["id"],
        )

    async def test_disabled_subscription_and_wrong_user_do_not_match(self) -> None:
        for sub in (
            subscription(status="authorization_revoked"),
            subscription(condition={"broadcaster_user_id": "200", "user_id": "999"}),
            None,
        ):
            typed = msgspec.convert(sub, type=Subscription) if sub else None
            self.assertFalse(self.api.matches(typed, "session-1", "channel.chat.message"))

    async def test_subscription_pagination_and_repeated_cursor_guard(self) -> None:
        self.request.side_effect = [
            subscription_page({"data": [subscription()], "pagination": {"cursor": "next"}}),
            subscription_page({"data": [], "pagination": {}}),
        ]
        self.assertEqual(len(await self.api.subscriptions()), 1)
        self.request.side_effect = [
            subscription_page({"data": [], "pagination": {"cursor": "loop"}}),
            subscription_page({"data": [], "pagination": {"cursor": "loop"}}),
        ]
        with self.assertRaises(ProtocolError):
            await self.api.subscriptions()

    async def test_send_requires_is_sent_and_message_id(self) -> None:
        self.request.return_value = msgspec.convert(
            {
                "data": [
                    {
                        "message_id": "",
                        "is_sent": False,
                        "drop_reason": {"code": "automod", "message": "held"},
                    }
                ]
            },
            type=SendChat,
        )
        with self.assertRaises(DeliveryError):
            await self.api.send("hi")
        self.api._next_send = 0
        self.request.return_value = msgspec.convert({"data": [{"is_sent": True}]}, type=SendChat)
        with self.assertRaises(ProtocolError):
            await self.api.send("hi")

    async def test_send_target_length_and_no_invalid_source_only_parameter(self) -> None:
        self.request.return_value = msgspec.convert(
            {"data": [{"is_sent": True, "message_id": "sent"}]}, type=SendChat
        )
        self.assertEqual(await self.api.send("x" * 700, reply_to="parent"), "sent")
        body = self.request.call_args.kwargs["json"]
        self.assertEqual((body["broadcaster_id"], body["sender_id"]), ("200", "100"))
        self.assertEqual(len(body["message"]), 500)
        self.assertEqual(body["reply_parent_message_id"], "parent")
        self.assertNotIn("for_source_only", body)

    async def test_401_may_retry_but_ambiguous_network_post_never_replays(self) -> None:
        self.request_patch.stop()
        self.fake.http.request.side_effect = [RemoteError(401, "expired"), {"data": []}]
        await self.api.request("POST", "/chat/messages", json={})
        self.assertEqual(self.fake.http.request.await_count, 2)
        self.fake.http.request.reset_mock(side_effect=True)
        self.fake.http.request.side_effect = NetworkError("unknown delivery")
        with self.assertRaises(NetworkError):
            await self.api.request("POST", "/chat/messages", json={})
        self.fake.http.request.assert_awaited_once()


class EventSubTests(StoreCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.api = fake_api()
        self.frames = []
        self.eventsub = EventSub(
            self.config, cast("Twitch", self.api), self.state, self.frames.append
        )

    async def test_fresh_connection_always_creates_chat_subscription(self) -> None:
        await self.eventsub._fresh("new-session")
        self.api.subscribe.assert_awaited_once_with("new-session", "channel.chat.message")
        self.assertIn("channel.chat.message", self.state.subscriptions)

    async def test_failed_initial_subscription_closes_open_socket(self) -> None:
        socket = FakeSocket()
        self.api.users.return_value = [User(self.config.channel_id, "channel")]
        with (
            patch.object(
                self.eventsub,
                "_open",
                new=AsyncMock(return_value=(socket.typed(), Session("session-1"))),
            ),
            patch.object(
                self.eventsub,
                "_fresh",
                new=AsyncMock(side_effect=ProtocolError("subscription failed")),
            ),
            self.assertRaises(ProtocolError),
        ):
            await self.eventsub._connect()
        self.assertTrue(socket.closed)

    async def test_welcome_requires_session_identity_and_bounded_keepalive(self) -> None:
        wrong_kind = welcome()
        wrong_kind["metadata"]["message_type"] = "session_reconnect"
        for frame in (
            wrong_kind,
            welcome(session=""),
            welcome(keepalive=0),
            welcome(keepalive=601),
        ):
            with self.subTest(frame=frame), self.assertRaises(ProtocolError):
                self.eventsub._validate_welcome(wire_frame(frame))

    async def test_notifications_are_deduplicated_and_wrong_channel_filtered(self) -> None:
        frame = notification()
        self.eventsub._notification(wire_frame(frame))
        self.eventsub._notification(wire_frame(frame))
        self.eventsub._notification(wire_frame(notification(channel="999")))
        self.assertEqual(len(self.frames), 1)
        self.assertEqual(self.state.duplicate_events, 1)
        self.assertEqual(self.state.filtered_events, 1)

    async def test_revocation_clears_readiness(self) -> None:
        ready(self.state)
        frame = {
            "metadata": {"message_type": "revocation"},
            "payload": {"subscription": subscription(status="authorization_revoked")},
        }
        with self.assertRaises(TransportError):
            self.eventsub._notification(wire_frame(frame))
        self.assertFalse(self.state.transport_ready)

    async def test_missing_keepalive_times_out_even_if_socket_not_closed(self) -> None:
        ws = FakeSocket()
        with self.assertRaises(TimeoutError):
            await self.eventsub._receive(ws.typed(), 0.02)

    async def test_invalid_frame_is_protocol_error(self) -> None:
        ws = FakeSocket()
        ws.put(["not", "an", "object"])
        with self.assertRaises(ProtocolError):
            await self.eventsub._receive(ws.typed(), 1)

    async def test_keepalive_updates_health_without_chat_messages(self) -> None:
        ready(self.state)
        ws = FakeSocket()
        self.state.last_frame_mono -= 100
        task = asyncio.create_task(self.eventsub._listen(ws.typed()))
        try:
            ws.put({"metadata": {"message_type": "session_keepalive"}, "payload": {}})
            await until(lambda: self.state.transport_ready)
            self.assertEqual(self.state.received, 0)
        finally:
            await cancel(task)
        self.assertTrue(ws.closed)

    async def test_handoff_keeps_old_socket_reading_until_new_welcome_and_never_resubscribes(
        self,
    ) -> None:
        ready(self.state)
        self.state.keepalive_timeout = 30
        old, new = FakeSocket(), FakeSocket()
        gate, opening = asyncio.Event(), asyncio.Event()

        async def open_new(_url: str) -> tuple[aiohttp.ClientWebSocketResponse, Session]:
            opening.set()
            await gate.wait()
            return new.typed(), Session("session-2")

        open_patch = patch.object(self.eventsub, "_open", new=open_new)
        open_patch.start()
        self.addCleanup(open_patch.stop)
        task = asyncio.create_task(self.eventsub._listen(old.typed()))
        try:
            old.put({
                "metadata": {"message_type": "session_reconnect"},
                "payload": {
                    "session": {"reconnect_url": "wss://eventsub.wss.twitch.tv/ws?opaque=one"}
                },
            })
            await asyncio.wait_for(opening.wait(), 1)
            self.assertFalse(old.closed)
            old.put(notification(message_id="during-handoff"))
            await until(lambda: len(self.frames) == 1)
            gate.set()
            await until(lambda: self.state.session_id == "session-2")
            self.assertTrue(old.closed)
            self.assertEqual(self.state.keepalive_timeout, 30)
            new.put(notification(message_id="during-handoff"))
            new.put(notification(message_id="after-handoff"))
            await until(lambda: len(self.frames) == 2)
            self.assertEqual(self.state.duplicate_events, 1)
            self.api.subscribe.assert_not_awaited()
        finally:
            await cancel(task)
        self.assertTrue(new.closed)

    async def test_handoff_can_complete_when_old_socket_closes_before_new_welcome(self) -> None:
        ready(self.state)
        old, new = FakeSocket(), FakeSocket()
        gate, opening = asyncio.Event(), asyncio.Event()

        async def open_new(_url: str) -> tuple[aiohttp.ClientWebSocketResponse, Session]:
            opening.set()
            await gate.wait()
            return new.typed(), Session("session-2", keepalive_timeout_seconds=30)

        open_patch = patch.object(self.eventsub, "_open", new=open_new)
        open_patch.start()
        self.addCleanup(open_patch.stop)
        task = asyncio.create_task(self.eventsub._listen(old.typed()))
        try:
            old.put({
                "metadata": {"message_type": "session_reconnect"},
                "payload": {"session": {"reconnect_url": "wss://eventsub.wss.twitch.tv/ws"}},
            })
            await asyncio.wait_for(opening.wait(), 1)
            old.end()
            await asyncio.sleep(0.02)
            gate.set()
            await until(lambda: task.done() or self.state.session_id == "session-2")
            self.assertFalse(task.done(), "handoff abandoned after old connection closed")
            self.assertEqual(self.state.session_id, "session-2")
        finally:
            await cancel(task)


class ProxyTests(StoreCase):
    async def test_eventsub_connect_uses_configured_proxy_and_no_client_heartbeat(self) -> None:
        api = fake_api()
        ws = FakeSocket()
        ws.put(welcome())
        api.http.session = SimpleNamespace(ws_connect=AsyncMock(return_value=ws))
        conf = replace(self.config, proxy="http://127.0.0.1:12334")
        eventsub = EventSub(conf, cast("Twitch", api), self.state, lambda _: None)
        await eventsub._open("wss://eventsub.wss.twitch.tv/ws")
        kwargs = api.http.session.ws_connect.call_args.kwargs
        self.assertEqual(kwargs["proxy"], conf.proxy)
        self.assertTrue(kwargs["autoping"])
        self.assertIsNone(kwargs["heartbeat"])

    async def test_http_uses_same_proxy_and_does_not_follow_redirects(self) -> None:
        async def chunks(_n: int) -> AsyncIterator[bytes]:  # ruff: ignore[unused-async] - async iterator test double
            yield b'{"ok":true}'

        response = SimpleNamespace(status=200, content=SimpleNamespace(iter_chunked=chunks))
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = SimpleNamespace(request=Mock(return_value=context))
        http = Http(cast("aiohttp.ClientSession", session), "http://127.0.0.1:12334")
        self.assertEqual(
            await http.request("GET", "https://api.twitch.tv/helix/users"), {"ok": True}
        )
        self.assertEqual(session.request.call_args.kwargs["proxy"], http.proxy)
        self.assertFalse(session.request.call_args.kwargs["allow_redirects"])
