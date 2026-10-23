"""Track readiness and persist a redacted local status snapshot."""

import asyncio
import json
import math
import os
import tempfile
import time
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from . import __version__
from .concurrency import run_blocking
from .security import REDACT

if TYPE_CHECKING:
    from collections.abc import Generator

if os.name == "nt":
    import msvcrt
else:
    import fcntl

WORKER_STALE_SECONDS = 50
RECENT_LOSS_SECONDS = 60
ATOMIC_WRITE_ATTEMPTS = 3
FUTURE_STATUS_TOLERANCE_SECONDS = 5


class _StatusSnapshot(msgspec.Struct):
    """Validate status fields used by the CLI while allowing additional metrics."""

    schema: int
    written_at: float
    status: str
    transport_ready: bool = False
    last_probe_detail: str = ""
    features: dict[str, str] = msgspec.field(default_factory=dict)


@dataclass(slots=True)
class Health:
    """Track signals needed to distinguish ready from degraded chat."""

    bot_id: str
    channel_id: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: float = field(default_factory=time.time)
    phase: str = "STARTING"
    bot_login: str = ""
    channel_login: str = ""
    auth_ok: bool = False
    api_ok: bool | None = None
    session_id: str = ""
    ws_connected: bool = False
    subscriptions: dict[str, str] = field(default_factory=dict)
    last_frame_at: float | None = None
    last_frame_mono: float = 0
    keepalive_timeout: float = 10
    worker_mono: float = field(default_factory=time.monotonic)
    last_message_at: float | None = None
    last_command_at: float | None = None
    last_send_at: float | None = None
    last_error: str = ""
    last_delivery_error: str = ""
    received: int = 0
    commands: int = 0
    command_errors: int = 0
    throttled_commands: int = 0
    sent: int = 0
    send_errors: int = 0
    dropped_events: int = 0
    last_drop_at: float | None = None
    duplicate_events: int = 0
    filtered_events: int = 0
    reconnects: int = 0
    queue_depth: int = 0
    e2e: str = "UNVERIFIED"
    last_probe_at: float | None = None
    last_probe_detail: str = ""
    features: dict[str, str] = field(default_factory=dict)

    def frame_received(self) -> None:
        self.last_frame_at, self.last_frame_mono = time.time(), time.monotonic()

    def error(self, exc: object) -> None:
        self.last_error = REDACT(exc)[:500]

    @property
    def transport_ready(self) -> bool:
        return (
            self.auth_ok
            and self.ws_connected
            and "channel.chat.message" in self.subscriptions
            and self.last_frame_at is not None
            and time.monotonic() - self.last_frame_mono <= self.keepalive_timeout + 5
        )

    def snapshot(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("last_frame_mono")
        data.pop("worker_mono")
        worker_ok = time.monotonic() - self.worker_mono < WORKER_STALE_SECONDS
        ready = self.transport_ready
        overall = self.phase
        if ready:
            recent_loss = (
                self.last_drop_at is not None
                and time.time() - self.last_drop_at < RECENT_LOSS_SECONDS
            )
            healthy = (
                worker_ok
                and self.api_ok is not False
                and not self.last_delivery_error
                and self.e2e != "FAILED"
                and not recent_loss
            )
            overall = "READY" if healthy else "DEGRADED"
        elif self.phase == "LISTENING":
            overall = "DEGRADED"
        data.update(
            schema=1,
            version=__version__,
            pid=os.getpid(),
            written_at=time.time(),
            transport_ready=ready,
            worker_ok=worker_ok,
            status=overall,
        )
        return data


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    """Replace a status file atomically with redacted JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Unique temp files also make simultaneous diagnostic/test writers safe.
    with ExitStack() as cleanup:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp = Path(file.name)
            cleanup.callback(temp.unlink, missing_ok=True)
            json.dump(_scrub(data), file, ensure_ascii=False, indent=2)
        for attempt in range(ATOMIC_WRITE_ATTEMPTS):
            try:
                temp.replace(path)
            except PermissionError:
                if attempt == ATOMIC_WRITE_ATTEMPTS - 1:
                    raise
                time.sleep(0.05)
            else:
                return


def _scrub(value: object) -> object:
    if isinstance(value, str):
        return REDACT(value)
    if isinstance(value, dict):
        return {key: _scrub(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    return value


def read_status(path: Path, max_age: float = 20) -> dict[str, Any]:
    """Read a status snapshot and mark stale or corrupt data."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        snapshot = msgspec.convert(data, type=_StatusSnapshot)
        age = time.time() - snapshot.written_at
        if snapshot.schema != 1 or not math.isfinite(age):
            raise ValueError("invalid status snapshot")
    except FileNotFoundError:
        return {
            "status": "NOT_RUNNING",
            "transport_ready": False,
            "last_error": "Файл состояния ещё не создан",
        }
    except OSError, ValueError, TypeError:
        return {
            "status": "UNKNOWN",
            "transport_ready": False,
            "last_error": "Файл состояния не читается",
        }
    if age > max_age or age < -FUTURE_STATUS_TOLERANCE_SECONDS:
        data.update(status="STALE", transport_ready=False)
    data["status_age_seconds"] = round(age, 1)
    return data


@contextmanager
def single_instance(path: Path) -> Generator[None]:
    """Hold an OS file lock for the process lifetime; keep the lock file on disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as file:
        file.seek(0, 2)
        if file.tell() == 0:
            file.write(b"0")
            file.flush()
        file.seek(0)
        try:
            if os.name == "nt":
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("Другой экземпляр FGPbot уже запущен в этой папке") from None
        try:
            yield
        finally:
            if os.name == "nt":
                file.seek(0)
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)


async def status_writer(state: Health, path: Path) -> None:
    """Persist health snapshots while the bot is running."""
    while True:
        await run_blocking(atomic_json, path, state.snapshot())
        await asyncio.sleep(5)
