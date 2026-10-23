"""Coordinate chat processing, health audits, and end-to-end probes."""

import asyncio
import logging
import sqlite3
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import aiohttp

from .commands import Commands
from .concurrency import run_blocking
from .config import CHAT_SCOPES
from .eventsub import EventSub
from .health import Health, atomic_json, status_writer
from .network import Http, NetworkError, ProtocolError, RemoteError
from .security import REDACT
from .storage import Store
from .tokens import AuthRequiredError, Tokens
from .twitch import DeliveryError, Twitch
from .wire import ChatEvent, NotificationPayload

if TYPE_CHECKING:
    from .config import Config
    from .wire import Frame

LOG = logging.getLogger(__name__)
AUDIT_INITIAL_DELAY = 15
AUDIT_INTERVAL = 60
MAX_QUEUE_AGE_SECONDS = 60
CLEANUP_INTERVAL_SECONDS = 3600
PROBE_ECHO_TIMEOUT_SECONDS = 35
COMMAND_WORKERS = 4
MAX_COOLDOWN_ENTRIES = 2048


@dataclass(frozen=True, slots=True)
class QueuedEvent:
    """Keep the arrival time and reserved command owner with an event."""

    queued_at: float
    frame: Frame
    user_id: str | None = None


class Application:
    """Own the live bot loops and shared runtime state."""

    def __init__(self, config: Config, store: Store, api: Twitch, state: Health) -> None:
        self.config, self.store, self.api, self.state = config, store, api, state
        self.queue: asyncio.Queue[QueuedEvent] = asyncio.Queue(maxsize=config.queue_size)
        self.commands = Commands(config, api, store, state)
        self.eventsub = EventSub(config, api, state, self.ingest)
        self._pending_users: set[str] = set()
        self._cooldowns: OrderedDict[str, float] = OrderedDict()
        self._worker_ticks = [time.monotonic()] * COMMAND_WORKERS

    async def ingest(self, frame: Frame) -> None:
        """Commit chat history before any command can be throttled or dropped."""
        payload = frame.payload
        if isinstance(payload, NotificationPayload) and isinstance(payload.event, ChatEvent):
            event = payload.event
            if not self.commands.is_channel_event(event):
                self.state.filtered_events += 1
                return
            self.state.received += 1
            self.state.last_message_at = time.time()
            if event.chatter_user_id != self.config.bot_id:
                await self._archive(frame, event)
                if self.commands.command_user(frame) is None:
                    return
        self.enqueue(frame)

    async def _archive(self, frame: Frame, event: ChatEvent) -> None:
        timestamp = frame.metadata.message_timestamp or datetime.now(UTC).isoformat()
        try:
            await self.store.log_message(event, timestamp)
        except sqlite3.Error:
            self.state.features["message_log"] = "ERROR"
            raise
        self.state.features["message_log"] = "READY"

    def enqueue(self, frame: Frame) -> None:
        now = time.monotonic()
        user_id = self.commands.command_user(frame)
        if user_id is not None and (
            user_id in self._pending_users
            or now - self._cooldowns.get(user_id, float("-inf")) < self.config.command_cooldown
        ):
            self.state.throttled_commands += 1
            return
        try:
            self.queue.put_nowait(QueuedEvent(now, frame, user_id))
        except asyncio.QueueFull:
            self.state.dropped_events += 1
            self.state.last_drop_at = time.time()
            self.state.error("Переполнена очередь событий; часть событий потеряна")
            if self.state.dropped_events == 1 or self.state.dropped_events % 100 == 0:
                LOG.error("EVENT QUEUE FULL | dropped=%s", self.state.dropped_events)  # ruff: ignore[error-instead-of-exception] - expected overload
        else:
            if user_id is not None:
                self._pending_users.add(user_id)
                self._cooldowns[user_id] = now
                self._cooldowns.move_to_end(user_id)
                while len(self._cooldowns) > MAX_COOLDOWN_ENTRIES:
                    self._cooldowns.popitem(last=False)
            self.state.queue_depth = self.queue.qsize()

    async def worker(self) -> None:
        """Own a fixed number of workers; a slow viewer cannot occupy all of them."""
        async with asyncio.TaskGroup() as group:
            for index in range(COMMAND_WORKERS):
                group.create_task(self._consume(index), name=f"commands-{index}")

    def _heartbeat(self, index: int) -> None:
        self._worker_ticks[index] = time.monotonic()
        self.state.worker_mono = min(self._worker_ticks)

    async def _consume(self, index: int) -> None:
        while True:
            self._heartbeat(index)
            try:
                async with asyncio.timeout(1):
                    item = await self.queue.get()
            except TimeoutError:
                continue
            try:
                await self._handle_frame(item.queued_at, item.frame)
            except (
                NetworkError,
                RemoteError,
                ProtocolError,
                DeliveryError,
                AuthRequiredError,
            ) as exc:
                self.state.error(exc)
                LOG.error("EVENT FAILED | %s", exc)  # ruff: ignore[error-instead-of-exception] - recoverable event
            except ValueError, KeyError, TypeError:
                self.state.error("Некорректные данные события; подробности в логе")
                LOG.exception("Некорректные данные события")
            finally:
                if item.user_id is not None:
                    self._pending_users.discard(item.user_id)
                self.queue.task_done()
                self.state.queue_depth = self.queue.qsize()
                self._heartbeat(index)

    async def _handle_frame(self, queued: float, frame: Frame) -> None:
        if time.monotonic() - queued > MAX_QUEUE_AGE_SECONDS:
            self.state.dropped_events += 1
            self.state.last_drop_at = time.time()
            LOG.warning("Пропущено устаревшее событие из очереди")
            return
        await self.commands.handle(frame)

    async def _audit_subscription(self) -> None:
        await self.api.tokens.get(self.config.bot_id, CHAT_SCOPES)
        self.state.auth_ok = True
        session = self.state.session_id
        if not (self.state.ws_connected and "channel.chat.message" in self.state.subscriptions):
            return
        subs = await self.api.subscriptions()
        if session != self.state.session_id or not self.state.ws_connected:
            return
        if any(self.api.matches(s, session, "channel.chat.message") for s in subs):
            self.state.api_ok = True
        else:
            self.eventsub.reset("Twitch не подтверждает подписку на чат текущей сессии")

    async def audit(self) -> None:
        """Audit the actual active chat subscription, not the existence of two users."""
        await asyncio.sleep(AUDIT_INITIAL_DELAY)
        last_cleanup = 0.0
        while True:
            try:
                await self._audit_subscription()
                if time.monotonic() - last_cleanup > CLEANUP_INTERVAL_SECONDS:
                    await self.store.cleanup()
                    last_cleanup = time.monotonic()
            except AuthRequiredError as exc:
                self.state.auth_ok = False
                self.state.error(exc)
                if self.state.ws_connected:
                    self.eventsub.reset(str(exc))
            except (RemoteError, NetworkError, ProtocolError, TimeoutError) as exc:
                self.state.api_ok = False
                self.state.error(exc)
                LOG.warning("AUDIT DEGRADED | %s", exc)
            snapshot = self.state.snapshot()
            LOG.info(
                "HEALTH | %s | chat=%s | rx=%s commands=%s tx=%s | e2e=%s | queue=%s",
                snapshot["status"],
                snapshot["transport_ready"],
                self.state.received,
                self.state.commands,
                self.state.sent,
                self.state.e2e,
                self.queue.qsize(),
            )
            await asyncio.sleep(AUDIT_INTERVAL)

    async def _observe_probe(self, active: str) -> str | None:
        row = await self.store.probe(active)
        if row is None:
            return active
        if row["state"] == "CONFIRMED":
            self.state.e2e = "CONFIRMED"
            self.state.last_probe_at = time.time()
            self.state.last_probe_detail = (
                "API send → Twitch → EventSub → diagnostic command handler"
            )
            LOG.info("CHAT END-TO-END CONFIRMED | message_id=%s", row["message_id"])
            return None
        if row["state"] == "FAILED":
            self.state.e2e = "FAILED"
            self.state.last_probe_at = time.time()
            self.state.last_probe_detail = REDACT(row["detail"])
            return None
        if time.time() - row["created"] > PROBE_ECHO_TIMEOUT_SECONDS:
            await self.store.probe_failed(active, "Нет подтверждённого EventSub echo за 35 секунд")
            self.state.e2e = "FAILED"
            self.state.last_probe_at = time.time()
            self.state.last_probe_detail = "Отправка не подтверждена входящим событием"
            return None
        return active

    async def _start_probe(self) -> str | None:
        row = await self.store.claim_probe(self.state.run_id)
        if row is None:
            return None
        active = row["nonce"]
        if not isinstance(active, str):
            raise ProtocolError("Corrupt probe identifier in database")
        try:
            await self._send_probe(active)
        except (
            RemoteError,
            NetworkError,
            ProtocolError,
            DeliveryError,
            AuthRequiredError,
            TimeoutError,
        ) as exc:
            await self.store.probe_failed(active, REDACT(exc))
            self.state.e2e = "FAILED"
            self.state.last_probe_at = time.time()
            self.state.last_probe_detail = REDACT(exc)
            return None
        return active

    async def _send_probe(self, active: str) -> None:
        if not self.state.transport_ready:
            raise DeliveryError("Чат не подключён; тестовое сообщение не отправлено")
        async with asyncio.timeout(20):
            message_id = await self.commands.send(f"{self.config.prefix}fgpcheck {active}")
        await self.store.probe_sent(active, message_id)
        self.state.e2e = "WAITING_FOR_ECHO"

    async def probes(self) -> None:
        active: str | None = None
        while True:
            if active:
                active = await self._observe_probe(active)
            if active is None:
                active = await self._start_probe()
            await asyncio.sleep(1)

    async def run(self) -> None:
        # If a critical worker dies unexpectedly, all others stop too. No zombie READY process.
        async with asyncio.TaskGroup() as group:
            group.create_task(self.eventsub.run(), name="eventsub")
            group.create_task(self.worker(), name="commands")
            group.create_task(self.audit(), name="audit")
            group.create_task(self.probes(), name="probes")
            group.create_task(
                status_writer(self.state, self.config.status_file), name="status-writer"
            )


async def _run_session(config: Config, store: Store, state: Health) -> None:
    async with aiohttp.ClientSession(trust_env=False) as session:
        http = Http(session, config.proxy)
        api = Twitch(config, http, Tokens(config, store, http))
        app = Application(config, store, api, state)
        LOG.info(
            "FGPbot | bot_id=%s | channel_id=%s | proxy=%s | data=%s",
            config.bot_id,
            config.channel_id,
            REDACT(config.proxy or "DIRECT"),
            config.data_dir,
        )
        await app.run()


async def run_bot(config: Config) -> None:
    """Run the application until shutdown and close its resources."""
    store = Store(config.database)
    state = Health(config.bot_id, config.channel_id)
    state.features.update(
        music="NOT_CHECKED" if config.music_token else "NOT_CONFIGURED",
        followage="NOT_CHECKED",
        message_log="ENABLED",
    )
    failed = False
    try:
        await store.initialize()
        await _run_session(config, store, state)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        failed = True
        state.error(exc)
        raise
    finally:
        state.ws_connected = False
        state.subscriptions.clear()
        state.phase = "FAILED" if failed else "STOPPED"
        await run_blocking(atomic_json, config.status_file, state.snapshot())
