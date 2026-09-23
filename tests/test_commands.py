from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import replace
from unittest.mock import AsyncMock

from fgpbot.app import Application
from fgpbot.commands import Commands, Music
from fgpbot.network import NetworkError, ProtocolError
from fgpbot.tokens import AuthRequired
from fgpbot.twitch import DeliveryError
from tests.helpers import StoreCase, cancel, fake_api, notification, ready


class CommandTests(StoreCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.api = fake_api()
        self.commands = Commands(self.config,self.api,self.store,self.state)

    async def test_viewer_ping_reaches_send_with_reply_parent(self):
        await self.commands.handle(notification())
        self.api.send.assert_awaited_once_with("Pong! FGPbot работает.",reply_to="viewer-message-1")
        self.assertEqual((self.state.commands,self.state.sent),(1,1))

    async def test_ordinary_messages_are_logged_without_any_api_request(self):
        await self.commands.handle(notification("hello"))
        self.api.request.assert_not_awaited()
        self.api.users.assert_not_awaited()
        self.api.send.assert_not_awaited()
        self.assertEqual(await self.store.call(lambda db: db.execute("SELECT count(*) FROM messages").fetchone()[0]),1)

    async def test_foreign_channel_shared_source_and_self_commands_are_ignored(self):
        await self.commands.handle(notification(channel="999"))
        await self.commands.handle(notification(source="999"))
        await self.commands.handle(notification(user_id="100"))
        self.api.send.assert_not_awaited()
        self.assertEqual(self.state.filtered_events,2)

    async def test_duplicate_command_not_replayed_after_restart(self):
        await self.commands.handle(notification())
        restarted = Commands(self.config,self.api,self.store,self.state)
        await restarted.handle(notification())
        self.api.send.assert_awaited_once()
        self.assertEqual(self.state.duplicate_events,1)

    async def test_user_cooldown_is_bounded_and_separate_between_users(self):
        self.commands.config = replace(self.config,command_cooldown=30)
        await self.commands.handle(notification(message_id="one"))
        await self.commands.handle(notification(message_id="two"))
        await self.commands.handle(notification(message_id="three",user_id="301"))
        self.assertEqual(self.api.send.await_count,2)
        self.commands._cooldowns.update((str(i),time.monotonic()) for i in range(2100))
        await self.commands.handle(notification(message_id="four",user_id="9999"))
        self.assertLessEqual(len(self.commands._cooldowns),2048)

    async def test_message_log_sql_error_does_not_swallow_command(self):
        self.store.log_message=AsyncMock(side_effect=sqlite3.OperationalError("synthetic logging failure"))
        await self.commands.handle(notification())
        self.api.send.assert_awaited_once()
        self.assertEqual(self.state.features["message_log"],"ERROR")

    async def test_music_failure_gets_honest_reply_and_next_command_works(self):
        self.commands.music=Music(self.api.http,"synthetic-music-token")
        self.api.http.request.side_effect=NetworkError("synthetic unavailable")
        await self.commands.handle(notification("!трек", message_id="music"))
        self.assertIn("недоступен",self.api.send.call_args.args[0])
        self.assertEqual(self.state.features["music"],"UNAVAILABLE")
        await self.commands.handle(notification("!ping",message_id="ping"))
        self.assertIn("Pong",self.api.send.call_args.args[0])

    async def test_no_unwatched_song_does_not_invent_current_song(self):
        self.commands.music=Music(self.api.http,"synthetic-music-token")
        self.api.http.request.return_value=[{"title":"old","is_watched":True}]
        await self.commands.handle(notification("!трек"))
        self.assertIn("нет",self.api.send.call_args.args[0])
        self.assertNotIn("old",self.api.send.call_args.args[0])

    async def test_followage_missing_permissions_is_not_reported_as_not_following(self):
        self.api.request.side_effect=AuthRequired("not scoped")
        await self.commands.handle(notification("!followage"))
        text=self.api.send.call_args.args[0]
        self.assertIn("права",text)
        self.assertNotIn("не зафоловлен",text)
        self.assertEqual(self.state.features["followage"],"AUTH_REQUIRED")

    async def test_followage_bot_moderator_fallback_after_owner_failure(self):
        self.api.request.side_effect=[AuthRequired("owner revoked"),{"data":[]}]
        await self.commands.handle(notification("!followage"))
        self.assertIn("не зафоловлен",self.api.send.call_args.args[0])
        self.assertEqual([c.kwargs["user_id"] for c in self.api.request.call_args_list],["200","100"])

    async def test_followage_success_formats_real_date(self):
        self.api.request.return_value={"data":[{"followed_at":"2020-01-01T00:00:00Z"}]}
        await self.commands.handle(notification("!followage"))
        self.assertIn("следит за каналом",self.api.send.call_args.args[0])
        self.assertEqual(self.state.features["followage"],"READY")

    async def test_drop_response_does_not_increment_sent_or_send_recursive_error(self):
        ready(self.state)
        self.api.send.side_effect=DeliveryError("automod rejected")
        await self.commands.handle(notification())
        self.assertEqual(self.state.sent,0)
        self.assertEqual(self.state.send_errors,1)
        self.assertEqual(self.state.command_errors,1)
        self.assertEqual(self.state.snapshot()["status"],"DEGRADED")
        self.api.send.assert_awaited_once()

    async def test_social_aliases_and_joke_ban_no_moderation_api(self):
        for i,text in enumerate(("!ДС", "!тг", "!help")):
            await self.commands.handle(notification(text,message_id=f"alias-{i}"))
        self.api.users.return_value=[{"id":"400","login":"friend"}]
        await self.commands.handle(notification("!бан friend",message_id="ban"))
        self.assertIn("Шуточный",self.api.send.call_args.args[0])
        self.api.request.assert_not_awaited()
        self.assertEqual(self.api.send.await_count,4)

    async def test_stream_greeting_only_once_per_stream_id(self):
        self.commands.config=replace(self.config,greet_stream=True)
        await self.commands.handle(notification(kind="stream.online",message_id="stream-id"))
        await self.commands.handle(notification(kind="stream.online",message_id="stream-id"))
        self.api.send.assert_awaited_once()

    async def test_probe_from_viewer_or_wrong_message_id_cannot_forge_success(self):
        nonce="e"*32
        await self.store.new_probe(nonce,self.state.run_id)
        await self.store.claim_probe(self.state.run_id)
        await self.store.probe_sent(nonce,"expected")
        await self.commands.handle(notification(f"!fgpcheck {nonce}",message_id="expected",user_id="300"))
        await self.commands.handle(notification(f"!fgpcheck {nonce}",message_id="wrong",user_id="100"))
        self.assertEqual((await self.store.probe(nonce))["state"],"SENT")
        await self.commands.handle(notification(f"!fgpcheck {nonce}",message_id="expected",user_id="100"))
        self.assertEqual((await self.store.probe(nonce))["state"],"CONFIRMED")
        self.api.send.assert_not_awaited()


class MusicTests(StoreCase):
    async def test_boolean_string_handling_and_five_second_cache(self):
        api=fake_api()
        api.http.request.return_value=[{"title":"skip","is_watched":"true"},
                                       {"title":"keep","is_watched":"false"},
                                       {"title":"skip2","is_watched":1}]
        music=Music(api.http,"synthetic-music-token")
        self.assertEqual([x["title"] for x in await music.queue()],["keep"])
        await music.queue()
        api.http.request.assert_awaited_once()

    async def test_invalid_payload_is_not_silently_an_empty_queue(self):
        api=fake_api()
        for payload in ({"error":"unavailable"}, [None], [{"is_watched":"maybe"}]):
            api.http.request.return_value=payload
            with self.assertRaises(ProtocolError):
                await Music(api.http,"synthetic-music-token").queue()


class WorkerTests(StoreCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.api=fake_api()
        self.app=Application(replace(self.config,queue_size=2),self.store,self.api,self.state)

    async def test_queue_is_bounded_and_exposes_loss(self):
        ready(self.state)
        for i in range(3):
            self.app.enqueue(notification(message_id=str(i)))
        self.assertEqual(self.app.queue.qsize(),2)
        self.assertEqual(self.state.dropped_events,1)
        self.assertEqual(self.state.snapshot()["status"],"DEGRADED")

    async def test_worker_ignores_expired_command_but_processes_next_one(self):
        self.app.queue.put_nowait((time.monotonic()-70,notification(message_id="old")))
        self.app.enqueue(notification(message_id="new"))
        task=asyncio.create_task(self.app.worker())
        try:
            await asyncio.wait_for(self.app.queue.join(),2)
            self.assertEqual(self.state.dropped_events,1)
            self.api.send.assert_awaited_once()
            self.assertEqual(self.api.send.call_args.kwargs["reply_to"],"new")
        finally:
            await cancel(task)

    async def test_unexpected_worker_failure_stops_all_critical_tasks(self):
        stopped=[]
        async def idle():
            try:
                await asyncio.Future()
            finally:
                stopped.append(True)
        async def broken():
            await asyncio.sleep(.02)
            raise RuntimeError("synthetic worker death")
        self.app.eventsub.run=idle
        self.app.audit=idle
        self.app.probes=idle
        self.app.worker=broken
        with self.assertRaises(ExceptionGroup):
            await self.app.run()
        self.assertEqual(len(stopped),3)

    async def test_starting_probe_worker_alone_never_sends_unsolicited_message(self):
        task=asyncio.create_task(self.app.probes())
        try:
            await asyncio.sleep(.03)
            self.api.send.assert_not_awaited()
        finally:
            await cancel(task)
