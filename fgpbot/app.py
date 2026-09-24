"""Coordinate chat processing, health audits, and end-to-end probes."""

import asyncio
import logging
import time
from typing import TYPE_CHECKING

import aiohttp

from .commands import Commands
from .config import CHAT_SCOPES
from .eventsub import EventSub
from .health import Health, atomic_json, status_writer
from .network import Http, NetworkError, ProtocolError, RemoteError
from .security import REDACT
from .storage import Store
from .tokens import AuthRequiredError, Tokens
from .twitch import DeliveryError, Twitch

if TYPE_CHECKING:
    from .config import Config
    from .wire import Frame

LOG = logging.getLogger(__name__)
AUDIT_INITIAL_DELAY = 15
AUDIT_INTERVAL = 60
MAX_QUEUE_AGE_SECONDS = 60
CLEANUP_INTERVAL_SECONDS = 3600
PROBE_ECHO_TIMEOUT_SECONDS = 35


class Application:
    """Own the live bot loops and shared runtime state."""

    def __init__(self, config: Config, store: Store, api: Twitch, state: Health) -> None:
        self.config, self.store, self.api, self.state = config, store, api, state
        self.queue: asyncio.Queue[tuple[float, Frame]] = asyncio.Queue(maxsize=config.queue_size)
        self.commands = Commands(config, api, store, state)
        self.eventsub = EventSub(config, api, state, self.enqueue)

    def enqueue(self, frame: Frame) -> None:
        try:
            self.queue.put_nowait((time.monotonic(), frame))
            self.state.queue_depth = self.queue.qsize()
        except asyncio.QueueFull:
            self.state.dropped_events += 1
            self.state.last_drop_at = time.time()
            self.state.error("Переполнена очередь событий; часть событий потеряна")
            if self.state.dropped_events == 1 or self.state.dropped_events % 100 == 0:
                LOG.error("EVENT QUEUE FULL | dropped=%s", self.state.dropped_events)  # ruff: ignore[error-instead-of-exception] - expected overload

    async def worker(self) -> None:
        while True:
            self.state.worker_mono = time.monotonic()
            try:
                queued, frame = await asyncio.wait_for(self.queue.get(), timeout=1)
            except TimeoutError:
                continue
            try:
                await self._handle_frame(queued, frame)
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
                self.queue.task_done()
                self.state.queue_depth = self.queue.qsize()
                self.state.worker_mono = time.monotonic()

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
                    await self.store.cleanup(self.config.chat_retention_days)
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
    await store.initialize()
    state = Health(config.bot_id, config.channel_id)
    state.features.update(
        music="NOT_CHECKED" if config.music_token else "NOT_CONFIGURED",
        followage="NOT_CHECKED",
        message_log="ENABLED" if config.log_chat else "DISABLED",
    )
    failed = False
    try:
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
        await asyncio.to_thread(atomic_json, config.status_file, state.snapshot())
