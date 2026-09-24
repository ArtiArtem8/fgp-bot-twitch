from __future__ import annotations

import asyncio
import io
import logging
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import msgspec

from fgpbot import cli
from fgpbot.commands import format_queue, format_time_russian, parse_command, russian_word
from fgpbot.config import ConfigError, load_config
from fgpbot.health import Health, SingleInstance, atomic_json, read_status, status_writer
from fgpbot.security import REDACT, Redactor, SafeFormatter
from fgpbot.storage import Store
from fgpbot.wire import Badge, ChatEvent
from tests.helpers import StoreCase, notification, ready, until


class ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {
            "TWITCH_BOT_APP_CLIENT_ID": "test-client-id",
            "TWITCH_BOT_APP_CLIENT_SECRET": "synthetic-secret",
            "TWITCH_BOT_ID": "100",
            "TWITCH_OWNER_ID": "200",
        }

    def test_legacy_owner_id_is_channel_fallback(self) -> None:
        conf = load_config(self.root, self.env)
        self.assertEqual(conf.channel_id, "200")
        self.assertEqual(conf.database, self.root / "data" / "tokens.db")
        self.assertEqual(conf.proxy, "http://127.0.0.1:12334")
        self.assertEqual(conf.chat_retention_days, 0)

    def test_channel_override_and_explicit_direct_connection(self) -> None:
        conf = load_config(self.root, self.env | {"TWITCH_CHANNEL_ID": "400", "FGP_PROXY_URL": ""})
        self.assertEqual(conf.channel_id, "400")
        self.assertIsNone(conf.proxy)

    def test_config_is_anchored_to_root_not_working_directory(self) -> None:
        (self.root / ".env").write_text("\n".join(f"{k}={v}" for k, v in self.env.items()))
        self.assertEqual(load_config(self.root, {}).bot_id, "100")

    def test_environment_overrides_dotenv_without_mutating_environment(self) -> None:
        (self.root / ".env").write_text("FGP_PREFIX=?\nHTTP_PROXY=should-not-be-exported\n")
        before = dict(os.environ)
        self.assertEqual(load_config(self.root, self.env | {"FGP_PREFIX": "!"}).prefix, "!")
        self.assertEqual(dict(os.environ), before)

    def test_rejects_invalid_inputs_without_echoing_secrets(self) -> None:
        for field, value in (
            ("TWITCH_BOT_ID", "nickname"),
            ("TWITCH_CHANNEL_ID", "-1"),
            ("FGP_PROXY_URL", "socks5://secret-user:secret-pass@host:10"),
            ("FGP_QUEUE_SIZE", "0"),
            ("FGP_LOG_CHAT", "maybe"),
            ("FGP_PREFIX", "two words"),
            ("TWITCH_REDIRECT_URI", "http://0.0.0.0:4343/oauth/callback"),
        ):
            with self.subTest(field=field), self.assertRaises(ConfigError) as caught:
                load_config(self.root, self.env | {field: value})
            self.assertNotIn("secret-pass", str(caught.exception))

    def test_missing_required_credential_is_config_error(self) -> None:
        with self.assertRaises(ConfigError):
            load_config(self.root, self.env | {"TWITCH_BOT_APP_CLIENT_SECRET": "CHANGE_ME"})

    def test_secret_not_in_configuration_repr(self) -> None:
        self.assertNotIn("synthetic-secret", repr(load_config(self.root, self.env)))


class PureCommandTests(unittest.TestCase):
    def test_case_insensitive_alias_and_arguments(self) -> None:
        self.assertEqual(parse_command(" !Дс  @SomeOne ", "!"), ("дс", "@SomeOne"))
        self.assertIsNone(parse_command("hi !ping", "!"))
        self.assertIsNone(parse_command("!", "!"))

    def test_russian_plural_rules(self) -> None:
        self.assertEqual(
            [russian_word(n, "год", "года", "лет") for n in (1, 2, 5, 11, 21, 22, 112)],
            ["год", "года", "лет", "лет", "год", "года", "лет"],
        )

    def test_durations_are_not_rounded_to_next_unit(self) -> None:
        for seconds, expected in (
            (59 * 60, "59 минут"),
            (23 * 3600, "23 часа"),
            (364 * 86400, "364 дня"),
            (0, "0 секунд"),
            (-1, "0 секунд"),
            (3661, "1 час и 1 минуту"),
        ):
            with self.subTest(seconds=seconds):
                self.assertEqual(format_time_russian(seconds), expected)
        self.assertEqual(format_time_russian(3661, 3), "1 час, 1 минуту и 1 секунду")

    def test_queue_output_bounded_and_empty(self) -> None:
        value = format_queue([{"title": "a" * 1000} for _ in range(100)])
        self.assertLessEqual(len(value), 500)
        self.assertIn("ещё", value)
        self.assertIn("пуста", format_queue([]))


class SecurityTests(unittest.TestCase):
    def test_redacts_urls_headers_dictionaries_and_encoded_values(self) -> None:
        r = Redactor()
        r.add("secret with/slashes")
        for text in (
            "?refresh_token=abcdef123456&client_secret=zyx987654",
            "Bearer abcdef123456",
            "{'access_token': 'abcdef123456'}",
            "http://user:password@localhost:12334",
            "code=abcdef123456",
            "secret%20with%2Fslashes",
        ):
            with self.subTest(text=text):
                rendered = r(text)
                self.assertIn("[REDACTED]", rendered)
                for forbidden in ("abcdef123456", "zyx987654", "password", "secret%20"):
                    self.assertNotIn(forbidden, rendered)

    def test_redacts_exception_traceback_after_formatting(self) -> None:
        REDACT.add("synthetic-trace-secret")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(SafeFormatter("%(message)s"))
        logger = logging.Logger("test-only")  # ruff: ignore[direct-logger-instantiation] - isolated test logger
        logger.addHandler(handler)
        try:
            raise ValueError("bad token synthetic-trace-secret")
        except ValueError:
            logger.exception("operation failed")
        self.assertIn("ValueError", stream.getvalue())
        self.assertNotIn("synthetic-trace-secret", stream.getvalue())

    def test_redaction_preserves_valid_status_json(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "status.json"
            REDACT.add("synthetic-json-secret")
            atomic_json(
                path,
                {
                    "schema": 1,
                    "written_at": time.time(),
                    "last_error": "token=synthetic-json-secret",
                    "status": "FAILED",
                },
            )
            self.assertEqual(read_status(path)["status"], "FAILED")
            self.assertNotIn("synthetic-json-secret", path.read_text())


class HealthTests(unittest.TestCase):
    def test_http_ok_alone_never_means_chat_ready(self) -> None:
        state = Health("100", "200", api_ok=True, auth_ok=True)
        self.assertFalse(state.snapshot()["transport_ready"])
        self.assertNotEqual(state.snapshot()["status"], "READY")

    def test_ready_does_not_claim_end_to_end_test(self) -> None:
        state = Health("100", "200")
        ready(state)
        self.assertEqual(state.snapshot()["status"], "READY")
        self.assertEqual(state.snapshot()["e2e"], "UNVERIFIED")
        self.assertIsNone(state.last_message_at)  # A quiet chat is not a failure.

    def test_stale_keepalive_or_worker_not_healthy(self) -> None:
        state = Health("100", "200")
        ready(state)
        state.last_frame_mono -= 100
        self.assertFalse(state.transport_ready)
        self.assertEqual(state.snapshot()["status"], "DEGRADED")
        state.frame_received()
        state.worker_mono -= 100
        self.assertEqual(state.snapshot()["status"], "DEGRADED")

    def test_delivery_failure_probe_failure_and_overload_not_green(self) -> None:
        state = Health("100", "200")
        ready(state)
        for field, value, cleared in (
            ("last_delivery_error", "rejected", ""),
            ("e2e", "FAILED", "UNVERIFIED"),
            ("last_drop_at", time.time(), None),
        ):
            setattr(state, field, value)
            self.assertEqual(state.snapshot()["status"], "DEGRADED")
            setattr(state, field, cleared)
        self.assertEqual(state.snapshot()["status"], "READY")

    def test_disk_status_missing_corrupt_stale_and_future(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "status.json"
            self.assertEqual(read_status(path)["status"], "NOT_RUNNING")
            path.write_text("incomplete {")
            self.assertEqual(read_status(path)["status"], "UNKNOWN")
            for date in (time.time() - 30, time.time() + 100):
                atomic_json(
                    path,
                    {"schema": 1, "written_at": date, "status": "READY", "transport_ready": True},
                )
                self.assertEqual(read_status(path)["status"], "STALE")
                self.assertFalse(read_status(path)["transport_ready"])

    def test_single_instance_lock_releases_without_deleting_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "lock"
            with SingleInstance(path), self.assertRaises(RuntimeError), SingleInstance(path):
                self.fail("duplicate lock acquired")
            self.assertTrue(path.exists())
            with SingleInstance(path):
                pass

    def test_cli_requires_consent_before_any_probe_configuration_or_network(self) -> None:
        with patch("sys.stdout", io.StringIO()), patch.object(cli, "load_config") as load:
            self.assertEqual(cli.main(["check-chat"]), 2)
            load.assert_not_called()


class StorageTests(StoreCase):
    async def test_migration_preserves_legacy_tokens_and_messages(self) -> None:
        # Existing original-schema database, not a newly invented migration fixture.
        old = self.root / "legacy.db"
        with closing(sqlite3.connect(old)) as db, db:
            db.execute(
                "CREATE TABLE tokens(user_id TEXT PRIMARY KEY, token TEXT NOT NULL, "
                "refresh TEXT NOT NULL)"
            )
            db.execute("INSERT INTO tokens VALUES('100','legacy-access','legacy-refresh')")
        store = Store(old)
        await store.initialize()
        legacy = await store.token("100")
        if legacy is None:
            raise AssertionError("Legacy token disappeared during migration")
        self.assertEqual(legacy["token"], "legacy-access")
        event = msgspec.convert(notification()["payload"]["event"], type=ChatEvent)
        await store.log_message(event, "2020-01-01T00:00:00Z")
        await store.initialize()
        await store.cleanup(0)
        self.assertEqual(
            await store.call(lambda db: db.execute("SELECT count(*) FROM messages").fetchone()[0]),
            1,
        )

    async def test_compare_and_swap_cannot_overwrite_a_new_authorization(self) -> None:
        await self.store.save_token("100", "original", "original-refresh")
        await self.store.save_token("100", "manual-new", "manual-new-refresh")
        saved = await self.store.save_token(
            "100",
            "stale-rotation",
            "stale-rotation-refresh",
            expected=("original", "original-refresh"),
        )
        self.assertFalse(saved)
        self.assertEqual((await self.token_row("100"))["token"], "manual-new")

    async def test_message_insert_is_idempotent_and_follower_unknown(self) -> None:
        event = msgspec.convert(notification()["payload"]["event"], type=ChatEvent)
        event = msgspec.structs.replace(event, badges=[Badge("subscriber", "1")])
        await self.store.log_message(event, "2026-01-01T00:00:00Z")
        await self.store.log_message(event, "2026-01-01T00:00:00Z")
        rows = await self.store.call(lambda db: db.execute("SELECT * FROM messages").fetchall())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["is_subscriber"], 1)
        self.assertIsNone(rows[0]["is_follower"])

    async def test_claims_are_atomic_across_simultaneous_workers_and_restart(self) -> None:
        claims = await asyncio.gather(*(self.store.claim_event("same-key") for _ in range(8)))
        self.assertEqual(sum(claims), 1)
        self.assertFalse(await Store(self.store.path).claim_event("same-key"))

    async def test_probe_confirmed_only_for_matching_message_id(self) -> None:
        nonce = "a" * 32
        await self.store.new_probe(nonce, "run")
        await self.store.claim_probe("run")
        await self.store.probe_sent(nonce, "actual")
        await self.store.probe_observed(nonce, "wrong", "run")
        self.assertEqual((await self.probe_row(nonce))["state"], "SENT")
        await self.store.probe_observed(nonce, "actual", "run")
        self.assertEqual((await self.probe_row(nonce))["state"], "CONFIRMED")
        await self.store.probe_failed(nonce, "late failure")
        self.assertEqual((await self.probe_row(nonce))["state"], "CONFIRMED")

    async def test_probe_echo_can_arrive_before_post_response(self) -> None:
        nonce = "b" * 32
        await self.store.new_probe(nonce, "run")
        await self.store.claim_probe("run")
        await self.store.probe_observed(nonce, "actual", "run")
        self.assertEqual((await self.probe_row(nonce))["state"], "SENDING")
        await self.store.probe_sent(nonce, "actual")
        self.assertEqual((await self.probe_row(nonce))["state"], "CONFIRMED")

    async def test_probe_forgery_wrong_run_unknown_nonce_and_throttle(self) -> None:
        nonce = "c" * 32
        self.assertFalse(await self.store.probe_observed(nonce, "wrong", "run"))
        await self.store.new_probe(nonce, "run")
        self.assertFalse(await self.store.probe_observed(nonce, "wrong", "run"))  # Not yet sent.
        await self.store.claim_probe("run")
        self.assertFalse(await self.store.probe_observed(nonce, "wrong", "other-run"))
        with self.assertRaises(ValueError):
            await self.store.new_probe("d" * 32, "run")

    async def test_restart_expires_pending_probe_instead_of_replaying(self) -> None:
        await self.store.new_probe("c" * 32, "old-run")
        self.assertIsNone(await self.store.claim_probe("new-run"))
        self.assertEqual((await self.probe_row("c" * 32))["state"], "FAILED")

    async def test_doctor_offline_missing_token_explicit(self) -> None:
        report = await cli.doctor(self.config, offline=True)
        self.assertFalse(report["bot_token_stored"])
        self.assertIn("MISSING", report["bot_authorization"])
        self.assertFalse(report["sends_chat_messages"])
        self.assertFalse(report["refreshes_tokens"])


class AtomicWriteTests(StoreCase):
    async def test_concurrent_json_writes_leave_complete_file_and_no_temporary_files(self) -> None:
        await asyncio.gather(
            *(
                asyncio.to_thread(
                    atomic_json,
                    self.config.status_file,
                    {"schema": 1, "written_at": time.time(), "status": "READY", "run_id": str(i)},
                )
                for i in range(8)
            )
        )
        self.assertEqual(read_status(self.config.status_file)["status"], "READY")
        self.assertEqual(list(self.config.data_dir.glob(".*.tmp")), [])

    async def test_cancelled_status_writer_finishes_inflight_write_before_shutdown(self) -> None:
        started, finish = threading.Event(), threading.Event()

        def slow_write(*_args: object) -> None:
            started.set()
            finish.wait(2)

        with patch("fgpbot.health.atomic_json", slow_write):
            task = asyncio.create_task(status_writer(self.state, self.config.status_file))
            try:
                await until(started.is_set)
                task.cancel()
                await asyncio.sleep(0.02)
                self.assertFalse(task.done())
            finally:
                finish.set()
                await asyncio.gather(task, return_exceptions=True)

    async def test_bad_nonfinite_timestamp_is_not_a_fresh_ready_status(self) -> None:
        self.config.status_file.write_text('{"schema":1,"written_at":NaN,"status":"READY"}')
        self.assertEqual(read_status(self.config.status_file)["status"], "UNKNOWN")
