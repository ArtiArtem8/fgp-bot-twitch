"""Expose local bot setup, status, and diagnostic commands."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import logging
import platform
import sqlite3
import sys
import time
import unittest
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

import aiohttp

from .app import run_bot
from .auth import authorize
from .config import CHAT_SCOPES, ROOT, ConfigError, load_config
from .health import SingleInstance, read_status
from .network import Http
from .security import REDACT, setup_logging
from .storage import Store
from .tokens import Tokens
from .twitch import HELIX, Twitch

if TYPE_CHECKING:
    from .config import Config

LOG = logging.getLogger(__name__)


def show_status(data: dict[str, Any], *, as_json: bool = False) -> None:
    """Render the latest local status for a person or JSON consumer."""
    if as_json:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    print(f"FGPbot: {data.get('status', 'UNKNOWN')}")
    if data.get("status") in {"STALE", "NOT_RUNNING", "UNKNOWN"}:
        print(
            "Нет свежего подтверждения от процесса. Старые успешные проверки "
            "не означают, что бот сейчас работает."
        )
    print(f"Канал: {data.get('channel_login') or '?'} ({data.get('channel_id', '?')})")
    print(f"Приём чата: {'подключён' if data.get('transport_ready') else 'НЕ подтверждён'}")
    print(
        f"Сообщений: {data.get('received', 0)} | команд: {data.get('commands', 0)} | "
        f"отправлено API: {data.get('sent', 0)}"
    )
    print(
        f"Ошибки команд: {data.get('command_errors', 0)} | "
        f"отправки: {data.get('send_errors', 0)} | "
        f"потери очереди: {data.get('dropped_events', 0)}"
    )
    print(f"Сквозная проверка: {data.get('e2e', 'UNVERIFIED')}")
    for key, label in (
        ("last_message_at", "Последнее сообщение"),
        ("last_send_at", "Последняя отправка"),
        ("last_probe_at", "Время сквозной проверки"),
    ):
        if data.get(key) is not None:
            _show_timestamp(label, data[key])
    if data.get("last_probe_detail"):
        print("Последняя проверка: " + data["last_probe_detail"])
    _show_details(data)


def _show_details(data: dict[str, Any]) -> None:
    for key, label in (
        ("last_error", "Последняя ошибка"),
        ("last_delivery_error", "Ошибка отправки"),
    ):
        if data.get(key):
            print(label + ": " + REDACT(data[key]))
    for feature, value in data.get("features", {}).items():
        print(f"{feature}: {value}")


def _show_timestamp(label: str, value: object) -> None:
    try:
        if not isinstance(value, (str, int, float)):
            raise TypeError
        stamp = datetime.fromtimestamp(float(value)).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except ValueError, OverflowError, OSError, TypeError:
        print(f"{label}: неизвестно")
    else:
        print(f"{label}: {stamp}")


async def doctor(config: Config, *, offline: bool) -> dict[str, Any]:
    """Check configuration and optional live API readiness."""
    report = {
        "python": platform.python_version(),
        "platform": platform.system(),
        "aiohttp": importlib.metadata.version("aiohttp"),
        "configuration": "OK",
        "bot_id": config.bot_id,
        "channel_id": config.channel_id,
        "channel_source": "TWITCH_CHANNEL_ID, либо совместимый fallback TWITCH_OWNER_ID",
        "proxy": REDACT(config.proxy or "DIRECT"),
        "database_exists": config.database.exists(),
        "runtime": read_status(config.status_file),
        "sends_chat_messages": False,
        "refreshes_tokens": False,
    }
    if not config.database.exists():
        report["bot_authorization"] = "MISSING: запусти auth"
        return report
    store = Store(config.database)
    try:
        bot = await store.token(config.bot_id)
    except sqlite3.Error as exc:
        report["database_error"] = REDACT(exc)
        return report
    report["bot_token_stored"] = bool(bot)
    if not bot:
        report["bot_authorization"] = "MISSING: запусти auth"
        return report
    if offline:
        report["bot_authorization"] = "NOT_CHECKED_OFFLINE"
        return report
    async with aiohttp.ClientSession(trust_env=False) as session:
        try:
            await _doctor_online(config, store, bot["token"], session, report=report)
        except Exception as exc:  # ruff: ignore[blind-except] - report diagnostic failures
            report["online_error"] = REDACT(exc)
            report["note"] = (
                "Doctor не обновляет токены. Истёкший access token ещё может быть "
                "восстановлен ботом через refresh."
            )
    return report


async def _doctor_online(
    config: Config,
    store: Store,
    token: str,
    session: aiohttp.ClientSession,
    *,
    report: dict[str, Any],
) -> None:
    http = Http(session, config.proxy)
    tokens = Tokens(config, store, http)
    identity = await tokens.validate(token, config.bot_id)
    report["bot_login"] = identity.get("login")
    report["bot_scopes"] = identity["scopes"]
    report["missing_chat_scopes"] = sorted(CHAT_SCOPES - frozenset(identity["scopes"]))
    report["expires_in_seconds"] = identity["expires_in"]
    report["bot_authorization"] = (
        "VALID" if not report["missing_chat_scopes"] else "MISSING_SCOPES: запусти auth"
    )
    headers = {"Client-Id": config.client_id, "Authorization": f"Bearer {token}"}
    users = await http.request(
        "GET", HELIX + "/users", params={"id": config.channel_id}, headers=headers
    )
    report["channel"] = [{"id": u["id"], "login": u["login"]} for u in users.get("data", [])]
    subs = await _subscriptions(http, headers)
    runtime = read_status(config.status_file)
    report["runtime"] = runtime
    api = Twitch(config, http, tokens)
    report["chat_subscriptions_on_current_session"] = sum(
        api.matches(sub, runtime.get("session_id", ""), "channel.chat.message") for sub in subs
    )
    report["chat_subscription_check"] = "READ_ONLY; сквозная доставка здесь не проверяется"


async def _subscriptions(http: Http, headers: dict[str, str]) -> list[dict[str, Any]]:
    subs, params, cursors = [], {}, set()
    for _ in range(100):
        response = await http.request(
            "GET", HELIX + "/eventsub/subscriptions", params=params, headers=headers
        )
        subs.extend(response.get("data", []))
        cursor = response.get("pagination", {}).get("cursor")
        if not cursor:
            return subs
        if cursor in cursors:
            raise ValueError("EventSub pagination повторяет cursor")
        cursors.add(cursor)
        params["after"] = cursor
    raise ValueError("Слишком много страниц EventSub")


async def check_chat(config: Config) -> int:
    """Submit one diagnostic message and wait for its EventSub echo."""
    runtime = read_status(config.status_file)
    if not runtime.get("transport_ready") or not runtime.get("run_id"):
        show_status(runtime)
        print("Проверка отменена: нет подключённого процесса. В чат ничего не отправлено.")
        return 1
    if runtime.get("bot_id") != config.bot_id or runtime.get("channel_id") != config.channel_id:
        print("Конфигурация не совпадает с запущенным процессом. Сначала перезапусти бот.")
        return 2
    store = Store(config.database)
    nonce = uuid.uuid4().hex
    await store.new_probe(nonce, runtime["run_id"])
    print(
        "Запрошено ОДНО диагностическое сообщение в целевом чате; "
        "проверяется его обратная доставка."
    )
    return await _await_probe(config, store, nonce, runtime["run_id"])


async def _await_probe(config: Config, store: Store, nonce: str, run_id: str) -> int:
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        row = await store.probe(nonce)
        if row and row["state"] == "CONFIRMED":
            print("PASS: Send API → Twitch → EventSub → обработчик диагностической команды.")
            print(f"Совпали message_id: {row['message_id']} и {row['observed_id']}")
            return 0
        if row and row["state"] == "FAILED":
            print("FAIL: " + REDACT(row["detail"]))
            return 1
        current = read_status(config.status_file)
        if (
            current.get("status") in {"STALE", "NOT_RUNNING", "STOPPED", "FAILED"}
            or current.get("run_id") != run_id
        ):
            await store.probe_failed(nonce, "Процесс остановлен или перезапущен во время проверки")
            print("FAIL: процесс остановлен или перезапущен во время проверки.")
            return 1
        await asyncio.sleep(0.5)
    await store.probe_failed(nonce, "Тайм-аут локальной проверки")
    print(
        "FAIL: нет подтверждения за отведённое время. "
        "Автоматический повтор отправки НЕ выполняется."
    )
    return 1


def parser() -> argparse.ArgumentParser:
    """Build the command-line parser for local bot operations."""
    result = argparse.ArgumentParser(description="FGPbot: локальный Twitch-бот одного канала")
    sub = result.add_subparsers(dest="command")
    sub.add_parser("run", help="Запуск (по умолчанию)")
    auth = sub.add_parser("auth", help="Однократная локальная OAuth-авторизация")
    auth.add_argument("--account", choices=("bot", "broadcaster"), default="bot")
    auth.add_argument(
        "--followers", action="store_true", help="Запросить bot scope moderator:read:followers"
    )
    auth.add_argument("--no-browser", action="store_true")
    status = sub.add_parser("status", help="Состояние без Twitch и сетевых запросов")
    status.add_argument("--json", action="store_true")
    diagnose = sub.add_parser("doctor", help="Диагностика без отправки сообщений и refresh")
    diagnose.add_argument("--offline", action="store_true")
    check = sub.add_parser("check-chat", help="Сквозная проверка запущенного процесса")
    check.add_argument(
        "--send", action="store_true", help="Разрешить одно публичное диагностическое сообщение"
    )
    sub.add_parser("selftest", help="Все автоматические тесты; реальный Twitch не используется")
    return result


def main(argv: list[str] | None = None) -> int:
    """Dispatch a local command and return its process exit code."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
    arguments = parser().parse_args(argv)
    command = arguments.command or "run"
    early = _early_command(command, arguments)
    if early is not None:
        return early
    try:
        config = load_config()
        REDACT.add(config.client_secret, config.music_token)
        return _execute(command, config, arguments)
    except ConfigError as exc:
        print("CONFIG ERROR: " + REDACT(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("FGPbot остановлен пользователем.")
        return 0
    except Exception as exc:
        if command == "run":
            LOG.exception("Критическая ошибка; процесс завершён с ненулевым кодом")
        else:
            print("ERROR: " + REDACT(exc), file=sys.stderr)
        return 1


def _early_command(command: str, arguments: argparse.Namespace) -> int | None:
    if command == "selftest":
        return _selftest()
    if command == "status":
        status = read_status(ROOT / "data" / "status.json")
        show_status(status, as_json=arguments.json)
        return 0 if status.get("status") == "READY" else 1
    if command == "check-chat" and not arguments.send:
        print(
            "Нужен явный --send: проверка публикует одно сообщение в чате. "
            "Для тихой проверки используй status или doctor."
        )
        return 2
    return None


def _selftest() -> int:
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"), top_level_dir=str(ROOT))
    if suite.countTestCases() == 0:
        print("FAIL: тесты отсутствуют. Установите полный архив проекта.", file=sys.stderr)
        return 1
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


def _execute(command: str, config: Config, arguments: argparse.Namespace) -> int:
    if command == "run":
        return _run(config)
    if command == "auth":
        return (
            0
            if asyncio.run(
                authorize(
                    config,
                    arguments.account,
                    followers=arguments.followers,
                    open_browser=not arguments.no_browser,
                )
            )
            else 2
        )
    if command == "doctor":
        return _doctor_result(config, offline=arguments.offline)
    if command == "check-chat":
        return asyncio.run(check_chat(config))
    return 0


def _run(config: Config) -> int:
    # Only the run process writes the rotating log. Auth and doctor cannot
    # race its rotation on Windows.
    try:
        lock = SingleInstance(config.data_dir / "bot.lock")
        # Keep the lock through the async run and release it in finally.
        lock.__enter__()  # ruff: ignore[unnecessary-dunder-call]
    except RuntimeError as exc:
        print(str(exc))
        return 3
    try:
        setup_logging(config.root, config.log_level)
        asyncio.run(run_bot(config))
    finally:
        lock.__exit__()
    return 0


def _doctor_result(config: Config, *, offline: bool) -> int:
    report = asyncio.run(doctor(config, offline=offline))
    print(REDACT(json.dumps(report, ensure_ascii=False, indent=2)))
    failed = (
        report.get("online_error")
        or report.get("database_error")
        or not report.get("bot_token_stored")
        or report.get("missing_chat_scopes")
    )
    if not offline:
        failed = (
            failed
            or not report.get("channel")
            or not report.get("chat_subscriptions_on_current_session")
            or report.get("runtime", {}).get("status") != "READY"
        )
    return 1 if failed else 0
