"""
Health monitoring component for a TwitchIO bot.

The check is intentionally simple and reliable:
- It verifies HTTP API reachability through your proxy by calling `fetch_users`.
- It measures round-trip time (RTT) for that API call.
- Optionally, it can check both bot_id and owner_id (if present) to catch config issues.

This component is designed to be lint-friendly (ruff/flake8/black) and safe to unload.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from twitchio.ext import commands

if TYPE_CHECKING:
    from twitchio import User

    from main import Bot


INITIAL_DELAY: Final[float] = 5.0
CHECK_INTERVAL: Final[float] = 60.0
API_TIMEOUT: Final[float] = 10.0


@dataclass(frozen=True, slots=True)
class ApiProbeResult:
    ok: bool
    target: str
    rtt_ms: float
    detail: str


class Health(commands.Component):
    """
    Periodic health checker.

    Lifecycle:
    - `component_load()` starts the background loop. [page:1]
    - `component_teardown()` stops it and awaits cancellation. [page:1]
    """

    def __init__(self, bot: Bot) -> None:
        self.bot: Bot = bot
        self.logger: logging.Logger = logging.getLogger(__name__)
        self._task: asyncio.Task[None] | None = None
        self._stop: asyncio.Event = asyncio.Event()

    async def component_load(self) -> None:
        """
        Called by TwitchIO when the component is being loaded. [page:1]
        """
        if self._task is not None and not self._task.done():
            self.logger.warning("Health monitor task already running")
            return

        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="health-monitor")
        self.logger.info("Health component loaded; monitor started")

    async def component_teardown(self) -> None:
        """
        Called by TwitchIO when the component is being unloaded. [page:1]
        """
        self.logger.info("Stopping Health component...")
        self._stop.set()

        task = self._task
        self._task = None

        if task is None:
            self.logger.info("Health component shutdown complete")
            return

        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self.logger.debug("Health monitor task cancelled cleanly")

        self.logger.info("Health component shutdown complete")

    async def _run(self) -> None:
        try:
            await asyncio.sleep(INITIAL_DELAY)

            while not self._stop.is_set():
                await self._check_once()

                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=CHECK_INTERVAL)
                except asyncio.TimeoutError:
                    continue

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Uncaught error in health monitor loop")

    async def _check_once(self) -> None:
        """
        Performs one health check cycle and logs a single structured line.
        """
        results: list[ApiProbeResult] = []

        bot_id = self._coerce_id(self.bot.bot_id)
        owner_id = self._coerce_id(self.bot.owner_id)

        if bot_id is None and owner_id is None:
            self.logger.warning("HEALTH | API: SKIPPED | reason=no bot_id/owner_id")
            return

        if bot_id is not None:
            results.append(await self._probe_user_id(bot_id, label="bot_id"))

        if owner_id is not None and owner_id != bot_id:
            results.append(await self._probe_user_id(owner_id, label="owner_id"))

        ok_all = all(r.ok for r in results)
        worst_rtt = max((r.rtt_ms for r in results), default=0.0)

        if ok_all:
            details = " | ".join(f"{r.target}: OK ({r.rtt_ms:.0f}ms)" for r in results)
            self.logger.info(
                "HEALTH | API: OK | worst_rtt=%.0fms | %s", worst_rtt, details
            )
            return

        details = " | ".join(
            f"{r.target}: FAIL ({r.detail}, {r.rtt_ms:.0f}ms)"
            if not r.ok
            else f"{r.target}: OK ({r.rtt_ms:.0f}ms)"
            for r in results
        )
        self.logger.warning(
            "HEALTH | API: DEGRADED | worst_rtt=%.0fms | %s", worst_rtt, details
        )

    async def _probe_user_id(self, user_id: int, *, label: str) -> ApiProbeResult:
        start = time.monotonic()
        try:
            users: list[User] = await asyncio.wait_for(
                self.bot.fetch_users(ids=[user_id]),
                timeout=API_TIMEOUT,
            )
            rtt_ms = (time.monotonic() - start) * 1000.0

            if not users:
                return ApiProbeResult(
                    ok=False,
                    target=label,
                    rtt_ms=rtt_ms,
                    detail="empty response",
                )

            return ApiProbeResult(
                ok=True,
                target=label,
                rtt_ms=rtt_ms,
                detail="ok",
            )

        except asyncio.TimeoutError:
            rtt_ms = (time.monotonic() - start) * 1000.0
            return ApiProbeResult(
                ok=False,
                target=label,
                rtt_ms=rtt_ms,
                detail=f"timeout>{API_TIMEOUT:.0f}s",
            )
        except Exception as exc:
            rtt_ms = (time.monotonic() - start) * 1000.0
            return ApiProbeResult(
                ok=False, target=label, rtt_ms=rtt_ms, detail=type(exc).__name__
            )

    @staticmethod
    def _coerce_id(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)  # pyright: ignore[reportArgumentType]
        except (TypeError, ValueError):
            return None
