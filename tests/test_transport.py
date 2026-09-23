from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp

from fgpbot.eventsub import EventSub, RecentIDs, TransportError, reconnect_url
from fgpbot.network import NetworkError, ProtocolError, RemoteError
from fgpbot.twitch import DeliveryError, Twitch
from tests.helpers import StoreCase, cancel, fake_api, notification, ready, subscription, until


class FakeSocket:
    def __init__(self):
        self.frames=asyncio.Queue()
        self.closed=False
        self.close_code=1000

    async def receive(self):
        return await self.frames.get()

    def put(self, data):
        self.frames.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT,data=json.dumps(data)))

    def end(self):
        self.frames.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSE,data=1000))

    async def close(self):
        self.closed=True


class URLTests(unittest.TestCase):
    def test_reconnect_only_accepts_exact_twitch_tls_host(self):
        valid="wss://eventsub.wss.twitch.tv/ws?reconnect=opaque"
        self.assertEqual(reconnect_url(valid),valid)
        for invalid in (None,"ws://eventsub.wss.twitch.tv/ws","wss://evil.example/ws",
                        "wss://eventsub.wss.twitch.tv.evil.example/ws", "wss://user@eventsub.wss.twitch.tv/ws",
                        "wss://eventsub.wss.twitch.tv:123/ws", "wss://eventsub.wss.twitch.tv:bad/ws"):
            with self.subTest(invalid=invalid), self.assertRaises(ProtocolError):
                reconnect_url(invalid)

    def test_recent_ids_are_bounded_and_duplicates_recognized(self):
        recent=RecentIDs(limit=2)
        self.assertFalse(recent.seen("one"))
        self.assertTrue(recent.seen("one"))
        recent.seen("two")
        recent.seen("three")
        self.assertEqual(len(recent.items),2)
        self.assertFalse(recent.seen("one"))


class SubscriptionTests(StoreCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.fake=fake_api()
        self.api=Twitch(self.config,self.fake.http,self.fake.tokens)
        self.api.request=AsyncMock()

    async def test_subscription_explicitly_uses_bot_identity_and_channel(self):
        self.api.request.return_value={"data":[subscription()]}
        await self.api.subscribe("session-1","channel.chat.message")
        body=self.api.request.call_args.kwargs["json"]
        self.assertEqual(body["condition"],{"broadcaster_user_id":"200","user_id":"100"})
        self.assertNotIn("user_id",self.api.request.call_args.kwargs) # Default token user = bot.

    async def test_409_is_not_success_when_other_session_owns_subscription(self):
        self.api.request.side_effect=[RemoteError(409,"duplicate"),{"data":[subscription("wrong-session")]}]
        with self.assertRaises(ProtocolError):
            await self.api.subscribe("session-1","channel.chat.message")

    async def test_409_same_active_session_is_idempotent_success(self):
        self.api.request.side_effect=[RemoteError(409,"duplicate"),{"data":[subscription()]}]
        self.assertEqual((await self.api.subscribe("session-1","channel.chat.message"))["id"],subscription()["id"])

    async def test_disabled_subscription_and_wrong_user_do_not_match(self):
        for sub in (subscription(status="authorization_revoked"),subscription(condition={"broadcaster_user_id":"200","user_id":"999"}),None):
            self.assertFalse(self.api.matches(sub,"session-1","channel.chat.message"))

    async def test_subscription_pagination_and_repeated_cursor_guard(self):
        self.api.request.side_effect=[{"data":[subscription()],"pagination":{"cursor":"next"}},
                                      {"data":[],"pagination":{}}]
        self.assertEqual(len(await self.api.subscriptions()),1)
        self.api.request.side_effect=[{"data":[],"pagination":{"cursor":"loop"}},
                                      {"data":[],"pagination":{"cursor":"loop"}}]
        with self.assertRaises(ProtocolError):
            await self.api.subscriptions()

    async def test_send_requires_is_sent_and_message_id(self):
        self.api.request.return_value={"data":[{"message_id":"","is_sent":False,"drop_reason":{"code":"automod","message":"held"}}]}
        with self.assertRaises(DeliveryError):
            await self.api.send("hi")
        self.api._next_send=0
        self.api.request.return_value={"data":[{"is_sent":True}]}
        with self.assertRaises(ProtocolError):
            await self.api.send("hi")

    async def test_send_target_length_and_no_invalid_source_only_parameter(self):
        self.api.request.return_value={"data":[{"is_sent":True,"message_id":"sent"}]}
        self.assertEqual(await self.api.send("x"*700,reply_to="parent"),"sent")
        body=self.api.request.call_args.kwargs["json"]
        self.assertEqual((body["broadcaster_id"],body["sender_id"]),("200","100"))
        self.assertEqual(len(body["message"]),500)
        self.assertEqual(body["reply_parent_message_id"],"parent")
        self.assertNotIn("for_source_only",body)

    async def test_401_may_retry_but_ambiguous_network_post_never_replays(self):
        self.api.request= Twitch.request.__get__(self.api,Twitch)
        self.fake.http.request.side_effect=[RemoteError(401,"expired"),{"data":[]}]
        await self.api.request("POST","/chat/messages",json={})
        self.assertEqual(self.fake.http.request.await_count,2)
        self.fake.http.request.reset_mock(side_effect=True)
        self.fake.http.request.side_effect=NetworkError("unknown delivery")
        with self.assertRaises(NetworkError):
            await self.api.request("POST","/chat/messages",json={})
        self.fake.http.request.assert_awaited_once()


class EventSubTests(StoreCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.api=fake_api()
        self.frames=[]
        self.eventsub=EventSub(self.config,self.api,self.state,self.frames.append)

    async def test_fresh_connection_always_creates_chat_subscription(self):
        await self.eventsub._fresh("new-session")
        self.api.subscribe.assert_awaited_once_with("new-session","channel.chat.message")
        self.assertIn("channel.chat.message",self.state.subscriptions)

    async def test_notifications_are_deduplicated_and_wrong_channel_filtered(self):
        frame=notification()
        self.eventsub._notification(frame)
        self.eventsub._notification(frame)
        self.eventsub._notification(notification(channel="999"))
        self.assertEqual(len(self.frames),1)
        self.assertEqual(self.state.duplicate_events,1)
        self.assertEqual(self.state.filtered_events,1)

    async def test_revocation_clears_readiness(self):
        ready(self.state)
        frame={"metadata":{"message_type":"revocation"},"payload":{"subscription":subscription(status="authorization_revoked")}}
        with self.assertRaises(TransportError):
            self.eventsub._notification(frame)
        self.assertFalse(self.state.transport_ready)

    async def test_missing_keepalive_times_out_even_if_socket_not_closed(self):
        ws=FakeSocket()
        with self.assertRaises(TimeoutError):
            await self.eventsub._receive(ws,.02)

    async def test_invalid_frame_is_protocol_error(self):
        ws=FakeSocket()
        ws.put(["not","an","object"])
        with self.assertRaises(ProtocolError):
            await self.eventsub._receive(ws,1)

    async def test_keepalive_updates_health_without_chat_messages(self):
        ready(self.state)
        ws=FakeSocket()
        self.state.last_frame_mono-=100
        task=asyncio.create_task(self.eventsub._listen(ws))
        try:
            ws.put({"metadata":{"message_type":"session_keepalive"},"payload":{}})
            await until(lambda:self.state.transport_ready)
            self.assertEqual(self.state.received,0)
        finally:
            await cancel(task)
        self.assertTrue(ws.closed)

    async def test_handoff_keeps_old_socket_reading_until_new_welcome_and_never_resubscribes(self):
        ready(self.state)
        self.state.keepalive_timeout=30
        old,new=FakeSocket(),FakeSocket()
        gate,opening=asyncio.Event(),asyncio.Event()
        async def open_new(url):
            opening.set()
            await gate.wait()
            return new,{"id":"session-2","keepalive_timeout_seconds":None}
        self.eventsub._open=open_new
        task=asyncio.create_task(self.eventsub._listen(old))
        try:
            old.put({"metadata":{"message_type":"session_reconnect"},"payload":{"session":{"reconnect_url":"wss://eventsub.wss.twitch.tv/ws?opaque=one"}}})
            await asyncio.wait_for(opening.wait(),1)
            self.assertFalse(old.closed)
            old.put(notification(message_id="during-handoff"))
            await until(lambda:len(self.frames)==1)
            gate.set()
            await until(lambda:self.state.session_id=="session-2")
            self.assertTrue(old.closed)
            self.assertEqual(self.state.keepalive_timeout,30)
            new.put(notification(message_id="during-handoff"))
            new.put(notification(message_id="after-handoff"))
            await until(lambda:len(self.frames)==2)
            self.assertEqual(self.state.duplicate_events,1)
            self.api.subscribe.assert_not_awaited()
        finally:
            await cancel(task)
        self.assertTrue(new.closed)

    async def test_handoff_can_complete_when_old_socket_closes_before_new_welcome(self):
        ready(self.state)
        old,new=FakeSocket(),FakeSocket()
        gate,opening=asyncio.Event(),asyncio.Event()
        async def open_new(url):
            opening.set()
            await gate.wait()
            return new,{"id":"session-2","keepalive_timeout_seconds":30}
        self.eventsub._open=open_new
        task=asyncio.create_task(self.eventsub._listen(old))
        try:
            old.put({"metadata":{"message_type":"session_reconnect"},"payload":{"session":{"reconnect_url":"wss://eventsub.wss.twitch.tv/ws"}}})
            await asyncio.wait_for(opening.wait(),1)
            old.end()
            await asyncio.sleep(.02)
            gate.set()
            await until(lambda:task.done() or self.state.session_id=="session-2")
            self.assertFalse(task.done(),"handoff abandoned after old connection closed")
            self.assertEqual(self.state.session_id,"session-2")
        finally:
            await cancel(task)


class ProxyTests(StoreCase):
    async def test_eventsub_connect_uses_configured_proxy_and_no_client_heartbeat(self):
        from dataclasses import replace
        from tests.helpers import welcome
        api=fake_api()
        ws=FakeSocket()
        ws.put(welcome())
        api.http.session=SimpleNamespace(ws_connect=AsyncMock(return_value=ws))
        conf=replace(self.config,proxy="http://127.0.0.1:12334")
        eventsub=EventSub(conf,api,self.state,lambda _:None)
        await eventsub._open("wss://eventsub.wss.twitch.tv/ws")
        kwargs=api.http.session.ws_connect.call_args.kwargs
        self.assertEqual(kwargs["proxy"],conf.proxy)
        self.assertTrue(kwargs["autoping"])
        self.assertIsNone(kwargs["heartbeat"])

    async def test_http_uses_same_proxy_and_does_not_follow_redirects(self):
        from unittest.mock import Mock
        from fgpbot.network import Http
        async def chunks(n):
            yield b'{"ok":true}'
        response=SimpleNamespace(status=200,content=SimpleNamespace(iter_chunked=chunks))
        context=AsyncMock()
        context.__aenter__.return_value=response
        session=SimpleNamespace(request=Mock(return_value=context))
        http=Http(session,"http://127.0.0.1:12334")
        self.assertEqual(await http.request("GET","https://api.twitch.tv/helix/users"),{"ok":True})
        self.assertEqual(session.request.call_args.kwargs["proxy"],http.proxy)
        self.assertFalse(session.request.call_args.kwargs["allow_redirects"])
