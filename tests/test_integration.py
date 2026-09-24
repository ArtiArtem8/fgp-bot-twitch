"""Real aiohttp HTTP/WebSocket + SQLite, entirely on 127.0.0.1.

No test reads the user's .env/database. Every credential is synthetic. Twitch
URLs are replaced before any application task starts, and restored afterwards.
"""

from __future__ import annotations

import asyncio
import io
import socket
import time
from contextlib import ExitStack
from dataclasses import replace
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp
from aiohttp import web

from fgpbot import cli
from fgpbot.app import Application
from fgpbot.auth import authorize
from fgpbot.health import atomic_json
from fgpbot.network import Http, NetworkError, ProtocolError, RemoteError
from fgpbot.tokens import Tokens
from fgpbot.twitch import Twitch
from tests.helpers import StoreCase, cancel, identity, notification, subscription, until, welcome


class LocalTwitch:
    def __init__(self) -> None:
        self.web = web.Application()
        self.web.router.add_get("/oauth2/validate", self.validate)
        self.web.router.add_post("/oauth2/token", self.refresh)
        self.web.router.add_get("/helix/users", self.users)
        self.web.router.add_route("*", "/helix/eventsub/subscriptions", self.subscriptions)
        self.web.router.add_post("/helix/chat/messages", self.send)
        self.web.router.add_get("/ws", self.ws)
        self.web.router.add_get("/handoff", self.ws)
        self.web.router.add_route("*", "/http/{mode}", self.http_test)
        self.runner = web.AppRunner(self.web, access_log=None)
        self.sockets = {}
        self.subs = []
        self.sent = []
        self.validate_calls = []
        self.refresh_count = 0
        self.sub_creations = 0
        self.connections = 0
        self.http_counts = {}
        self.drop_send = False
        self.echo = True
        self.echo_before_response = False
        self.bot_valid = True
        self.rotated = False
        self.handoff_gate = asyncio.Event()
        self.handoff_gate.set()
        self.handoff_open = asyncio.Event()
        self.base = ""

    async def start(self) -> None:
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        if not self.runner.addresses:
            raise AssertionError("Local aiohttp server did not start")
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def close(self) -> None:
        self.handoff_gate.set()
        for ws in list(self.sockets.values()):
            await ws.close()
        await self.runner.cleanup()

    async def validate(self, request: web.Request) -> web.Response:
        access = request.headers.get("Authorization", "").removeprefix("OAuth ")
        self.validate_calls.append(access)
        if (
            access == "synthetic-bot-access" and self.bot_valid
        ) or access == "synthetic-rotated-access":
            return web.json_response(identity())
        return web.json_response({"message": "invalid token"}, status=401)

    async def refresh(self, request: web.Request) -> web.Response:
        form = await request.post()
        self.refresh_count += 1
        if form.get("grant_type") == "authorization_code":
            assert not request.query
            return web.json_response({
                "access_token": "synthetic-bot-access",
                "refresh_token": "synthetic-bot-refresh",
            })
        if form.get("refresh_token") != "synthetic-bot-refresh":
            return web.json_response({"message": "invalid refresh token"}, status=400)
        assert not request.query, "Secrets must not be placed in URL"
        self.rotated = True
        return web.json_response({
            "access_token": "synthetic-rotated-access",
            "refresh_token": "synthetic-rotated-refresh",
        })

    @staticmethod
    async def users(request: web.Request) -> web.Response:
        ids = request.query.getall("id", [])
        return web.json_response({
            "data": [{"id": i, "login": "streamer" if i == "200" else "fgp_test_bot"} for i in ids]
        })

    async def subscriptions(self, request: web.Request) -> web.Response:
        assert request.headers.get("Authorization") in {
            "Bearer synthetic-bot-access",
            "Bearer synthetic-rotated-access",
        }
        if request.method == "GET":
            return web.json_response({"data": self.subs, "pagination": {}})
        body = await request.json()
        assert body["condition"]["broadcaster_user_id"] == "200"
        if body["type"] == "channel.chat.message":
            assert body["condition"]["user_id"] == "100"
        sid = body["transport"]["session_id"]
        assert sid in self.sockets, "Subscribed before welcome"
        self.sub_creations += 1
        sub = subscription(sid, body["type"], id=f"sub-{self.sub_creations}")
        self.subs.append(sub)
        return web.json_response({"data": [sub]}, status=202)

    async def send(self, request: web.Request) -> web.Response:
        body = await request.json()
        assert body["broadcaster_id"] == "200"
        assert body["sender_id"] == "100"
        assert "for_source_only" not in body, "Invalid with user token"
        self.sent.append(body)
        message_id = f"sent-{len(self.sent)}"
        if self.drop_send:
            return web.json_response({
                "data": [
                    {
                        "message_id": "",
                        "is_sent": False,
                        "drop_reason": {"code": "automod_held", "message": "held"},
                    }
                ]
            })
        if self.echo:
            for sub in list(self.subs):
                if sub["type"] != "channel.chat.message":
                    continue
                ws = self.sockets.get(sub["transport"]["session_id"])
                if ws and not ws.closed:
                    await ws.send_json(
                        notification(body["message"], message_id=message_id, user_id="100")
                    )
        if self.echo_before_response:
            await asyncio.sleep(0.1)
        return web.json_response({
            "data": [{"message_id": message_id, "is_sent": True, "drop_reason": None}]
        })

    async def ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.connections += 1
        sid = f"session-{self.connections}"
        self.sockets[sid] = ws
        handoff = request.path == "/handoff"
        if handoff:
            self.handoff_open.set()
            await self.handoff_gate.wait()
            previous = request.query["from"]
            for sub in self.subs:
                if sub["transport"]["session_id"] == previous:
                    sub["transport"]["session_id"] = sid
        await ws.send_json(welcome(sid, None if handoff else 30))
        try:
            async for frame in ws:
                if frame.type == aiohttp.WSMsgType.TEXT:
                    raise AssertionError("EventSub client must not send application data")
        finally:
            self.sockets.pop(sid, None)
            self.subs = [s for s in self.subs if s["transport"]["session_id"] != sid]
        return ws

    async def emit(self, frame: dict[str, object], session: str | None = None) -> None:
        sid = session or max(self.sockets, key=lambda s: int(s.split("-")[-1]))
        await self.sockets[sid].send_json(frame)

    async def http_test(self, request: web.Request) -> web.StreamResponse:
        mode = request.match_info["mode"]
        self.http_counts[mode] = self.http_counts.get(mode, 0) + 1
        if mode == "chunked":
            return await self._chunked_response(request)
        if mode == "large":
            return web.Response(
                body=b'"' + b"x" * (2 * 1024 * 1024) + b'"', content_type="application/json"
            )
        if mode == "invalid":
            return web.Response(text="<html>not JSON</html>")
        if mode == "redirect":
            raise web.HTTPFound("http://127.0.0.1:9/must-not-follow")
        if mode == "retry" and self.http_counts[mode] == 1:
            return web.json_response({"message": "retry"}, status=503)
        if mode == "error":
            return web.json_response({"message": "permanent"}, status=400)
        if mode == "ambiguous":
            self._abort_transport(request)
            return web.Response()
        return web.json_response({"ok": True})

    @staticmethod
    def _abort_transport(request: web.Request) -> None:
        if request.transport is None:
            raise AssertionError("Expected an active local HTTP transport")
        request.transport.close()

    @staticmethod
    async def _chunked_response(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "application/json"})
        await response.prepare(request)
        await response.write(b'{"value":')
        await asyncio.sleep(0.025)
        await response.write(b"42}")
        await response.write_eof()
        return response


class IntegrationTests(StoreCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.server = LocalTwitch()
        await self.server.start()
        self.addAsyncCleanup(self.server.close)
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        for target, value in (
            ("fgpbot.tokens.VALIDATE", self.server.base + "/oauth2/validate"),
            ("fgpbot.tokens.TOKEN_URL", self.server.base + "/oauth2/token"),
            ("fgpbot.twitch.HELIX", self.server.base + "/helix"),
            ("fgpbot.cli.HELIX", self.server.base + "/helix"),
            ("fgpbot.eventsub.WS_URL", self.server.base.replace("http:", "ws:") + "/ws"),
        ):
            self.patches.enter_context(patch(target, value))
        self.session = aiohttp.ClientSession(trust_env=False)
        self.addAsyncCleanup(self.session.close)
        self.http = Http(self.session, None)
        self.api = Twitch(self.config, self.http, Tokens(self.config, self.store, self.http))
        self.app = Application(self.config, self.store, self.api, self.state)
        await self.store.save_token("100", "synthetic-bot-access", "synthetic-bot-refresh")
        # Reproduce the user's revoked owner credentials without reading any real credentials.
        await self.store.save_token("200", "synthetic-revoked-owner", "synthetic-revoked-refresh")

    async def start_bot(self) -> asyncio.Task[None]:
        task = asyncio.create_task(self.app.run())
        self.addAsyncCleanup(cancel, task)
        await until(lambda: self.state.phase == "LISTENING", timeout=5)
        return task

    async def test_original_incident_revoked_owner_but_bot_chat_works(self) -> None:
        task = await self.start_bot()
        self.assertTrue(self.state.transport_ready)
        self.assertNotIn("synthetic-revoked-owner", self.server.validate_calls)
        self.assertEqual(self.server.sub_creations, 1)
        await self.server.emit(notification("!ping"))
        await until(lambda: self.state.sent == 1)
        self.assertIn("Pong", self.server.sent[0]["message"])
        self.assertEqual(self.server.sent[0]["reply_parent_message_id"], "viewer-message-1")
        self.assertFalse(task.done())

    async def test_startup_refresh_persists_rotated_pair_and_receives_command(self) -> None:
        self.server.bot_valid = False
        await self.start_bot()
        self.assertEqual(self.server.refresh_count, 1)
        self.assertEqual((await self.token_row("100"))["refresh"], "synthetic-rotated-refresh")
        await self.server.emit(notification("!ping"))
        await until(lambda: self.state.sent == 1)

    async def test_http_200_drop_is_not_reported_as_sent(self) -> None:
        self.server.drop_send = True
        await self.start_bot()
        await self.server.emit(notification("!ping"))
        await until(lambda: self.state.send_errors == 1)
        self.assertEqual(self.state.sent, 0)
        self.assertEqual(self.state.snapshot()["status"], "DEGRADED")
        self.assertEqual(len(self.server.sent), 1)

    async def test_disconnect_creates_new_subscription_and_commands_resume(self) -> None:
        await self.start_bot()
        first = self.state.session_id
        await self.server.sockets[first].close()
        await until(
            lambda: self.state.phase == "LISTENING" and self.state.session_id != first, timeout=8
        )
        self.assertEqual(self.server.sub_creations, 2)
        await self.server.emit(notification("!ping", message_id="after-reconnect"))
        await until(lambda: self.state.sent == 1)
        self.assertGreaterEqual(self.state.reconnects, 1)

    async def test_real_websocket_handoff_does_not_recreate_subscriptions(self) -> None:
        await self.start_bot()
        first = self.state.session_id
        self.server.handoff_gate.clear()
        local = self.server.base.replace("http:", "ws:") + f"/handoff?from={first}"
        # Allow a LOOPBACK replacement of the otherwise strict Twitch TLS allowlist.
        # Exact production-host enforcement is covered independently in URLTests.
        self.patches.enter_context(patch("fgpbot.eventsub.reconnect_url", lambda _: local))
        await self.server.emit(
            {
                "metadata": {"message_type": "session_reconnect"},
                "payload": {"session": {"reconnect_url": "wss://eventsub.wss.twitch.tv/ws?test"}},
            },
            first,
        )
        await asyncio.wait_for(self.server.handoff_open.wait(), 2)
        await self.server.emit(notification("!ping", message_id="during-handoff"), first)
        await until(lambda: self.state.sent == 1)
        self.server.handoff_gate.set()
        await until(lambda: self.state.session_id != first)
        self.assertEqual(self.server.sub_creations, 1)
        self.assertTrue(self.state.transport_ready)
        self.api._next_send = 0
        await self.server.emit(notification("!ping", message_id="after-handoff"))
        await until(lambda: self.state.sent == 2)

    async def test_full_probe_cli_to_api_to_websocket_to_command_handler(self) -> None:
        self.server.echo_before_response = True
        await self.start_bot()
        atomic_json(self.config.status_file, self.state.snapshot())
        with patch("sys.stdout", io.StringIO()) as output:
            result = await cli.check_chat(self.config)
        self.assertEqual(result, 0, output.getvalue())
        self.assertIn("PASS", output.getvalue())
        self.assertEqual(len(self.server.sent), 1)
        self.assertTrue(self.server.sent[0]["message"].startswith("!fgpcheck "))
        await until(lambda: self.state.e2e == "CONFIRMED", timeout=3)
        self.assertEqual(self.state.sent, 1)  # No bot reply loop or second test message.

    async def test_probe_cannot_pass_when_twitch_accepts_send_but_no_echo_arrives(self) -> None:
        self.server.echo = False
        await self.start_bot()
        nonce = "f" * 32
        await self.store.new_probe(nonce, self.state.run_id)
        await until(lambda: self.state.e2e == "WAITING_FOR_ECHO", timeout=3)
        self.assertEqual((await self.probe_row(nonce))["state"], "SENT")
        # Fast-forward only this request's deadline, not production clock functions.
        await self.store.call(
            lambda db: db.execute(
                "UPDATE fgp_probes SET created=? WHERE nonce=?", (time.time() - 36, nonce)
            )
        )
        await until(lambda: self.state.e2e == "FAILED", timeout=3)
        self.assertEqual(self.state.snapshot()["status"], "DEGRADED")
        self.assertEqual(len(self.server.sent), 1)

    async def test_doctor_checks_current_subscription_without_refresh_or_messages(self) -> None:
        await self.start_bot()
        atomic_json(self.config.status_file, self.state.snapshot())
        report = await cli.doctor(self.config, offline=False)
        self.assertEqual(report["bot_authorization"], "VALID")
        self.assertEqual(report["chat_subscriptions_on_current_session"], 1)
        self.assertEqual(self.server.refresh_count, 0)
        self.assertEqual(len(self.server.sent), 0)

    async def test_cancellation_closes_websocket_and_no_workers_survive(self) -> None:
        task = await self.start_bot()
        await cancel(task)
        await until(lambda: not self.server.sockets)
        self.assertFalse(self.state.ws_connected)
        names = {task.get_name() for task in asyncio.all_tasks() if not task.done()}
        self.assertTrue(
            names.isdisjoint({"commands", "audit", "probes", "status-writer", "eventsub"}), names
        )

    async def test_http_chunked_json_is_read_to_eof(self) -> None:
        result = await self.http.request("GET", self.server.base + "/http/chunked")
        self.assertEqual(result, {"value": 42})

    async def test_http_invalid_and_oversized_responses_fail_explicitly(self) -> None:
        for mode in ("invalid", "large"):
            with self.assertRaises(ProtocolError):
                await self.http.request("GET", self.server.base + "/http/" + mode)

    async def test_http_safe_get_retries_503_once(self) -> None:
        self.assertEqual(
            await self.http.request("GET", self.server.base + "/http/retry"), {"ok": True}
        )
        self.assertEqual(self.server.http_counts["retry"], 2)

    async def test_http_4xx_not_retried_and_redirect_not_followed(self) -> None:
        for mode, status in (("error", 400), ("redirect", 302)):
            with self.assertRaises(RemoteError) as caught:
                await self.http.request("GET", self.server.base + "/http/" + mode)
            self.assertEqual(caught.exception.status, status)
            self.assertEqual(self.server.http_counts[mode], 1)

    async def test_http_post_connection_lost_after_receipt_never_replayed(self) -> None:
        with self.assertRaises(NetworkError):
            await self.http.request(
                "POST", self.server.base + "/http/ambiguous", json={"message": "test"}
            )
        self.assertEqual(self.server.http_counts["ambiguous"], 1)

    async def test_lost_remote_subscription_detected_and_repaired_while_api_stays_up(self) -> None:
        self.patches.enter_context(patch("fgpbot.app.AUDIT_INITIAL_DELAY", 0.05))
        self.patches.enter_context(patch("fgpbot.app.AUDIT_INTERVAL", 0.05))
        await self.start_bot()
        first = self.state.session_id
        self.server.subs.clear()  # API GET /users would still succeed here.
        await until(lambda: self.state.reconnects >= 1, timeout=3)
        self.assertFalse(self.state.transport_ready)
        await until(
            lambda: self.state.phase == "LISTENING" and self.state.session_id != first, timeout=8
        )
        self.assertEqual(self.server.sub_creations, 2)
        await self.server.emit(notification("!ping", message_id="repaired"))
        await until(lambda: self.state.sent == 1)

    async def test_complete_local_oauth_listener_callback_exchange_and_shutdown(self) -> None:
        # Reserve a free loopback port for the short-lived OAuth callback server.
        with socket.socket() as probe_socket:
            probe_socket.bind(("127.0.0.1", 0))
            port = probe_socket.getsockname()[1]
        conf = replace(self.config, redirect_uri=f"http://localhost:{port}/oauth/callback")
        await self.store.save_token("100", "synthetic-invalid", "synthetic-invalid-refresh")
        self.patches.enter_context(
            patch("fgpbot.auth.TOKEN_URL", self.server.base + "/oauth2/token")
        )
        opened = asyncio.Event()
        urls = []
        loop = asyncio.get_running_loop()

        def fake_browser(url: str) -> bool:
            urls.append(url)
            loop.call_soon_threadsafe(opened.set)
            return True

        with (
            patch("fgpbot.auth.webbrowser.open", fake_browser),
            patch("sys.stdout", io.StringIO()),
        ):
            task = asyncio.create_task(authorize(conf, "bot", followers=False, open_browser=True))
            try:
                await asyncio.wait_for(opened.wait(), 3)
                state = parse_qs(urlsplit(urls[0]).query)["state"][0]
                query = urlencode({"state": state, "code": "synthetic-auth-code"})
                async with self.session.get(conf.redirect_uri + "?" + query) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn("Готово", await response.text())
                self.assertTrue(await asyncio.wait_for(task, 3))
            finally:
                await cancel(task)
        self.assertEqual((await self.token_row("100"))["refresh"], "synthetic-bot-refresh")
        self.assertEqual(len(self.server.sent), 0)
