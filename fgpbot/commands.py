"""Parse chat commands and produce bounded replies."""

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import msgspec

from .ban import BanResponder
from .config import FOLLOW_SCOPE
from .network import NetworkError, ProtocolError, RemoteError
from .security import REDACT
from .tokens import AuthRequiredError
from .twitch import DeliveryError, chat_text
from .wire import ChatEvent, Followers, NotificationPayload, StreamEvent, User

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .ban import BanKind
    from .config import Config
    from .health import Health
    from .network import Http
    from .storage import Store
    from .twitch import Twitch
    from .wire import Frame

LOG = logging.getLogger(__name__)
MAX_QUEUE_TEXT_LENGTH = 400


def russian_word(n: int, one: str, few: str, many: str) -> str:
    """Select the Russian noun form for the given count."""
    n = abs(n)
    if n % 10 == 1 and n % 100 != 11:  # ruff: ignore[magic-value-comparison] - Russian grammar
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:  # ruff: ignore[magic-value-comparison] - Russian grammar
        return few
    return many


def format_time_russian(seconds: int, depth: int = 2) -> str:
    """Render a duration with up to the requested number of units."""
    seconds = max(0, int(seconds))
    parts: list[str] = []
    for size, words in (
        (31536000, ("год", "года", "лет")),
        (86400, ("день", "дня", "дней")),
        (3600, ("час", "часа", "часов")),
        (60, ("минуту", "минуты", "минут")),
        (1, ("секунду", "секунды", "секунд")),
    ):
        value, seconds = divmod(seconds, size)
        if value:
            parts.append(f"{value} {russian_word(value, *words)}")
    parts = parts[: max(1, depth)] or ["0 секунд"]
    return ", ".join(parts[:-1]) + " и " + parts[-1] if len(parts) > 1 else parts[0]


def parse_command(text: str, prefix: str) -> tuple[str, str] | None:
    """Extract a prefixed command and its remaining argument."""
    text = text.strip()
    if not text.startswith(prefix):
        return None
    pieces = text[len(prefix) :].split(maxsplit=1)
    if not pieces:
        return None
    argument = pieces[1].strip() if len(pieces) > 1 else ""
    return pieces[0].casefold(), argument


@dataclass(frozen=True, slots=True)
class Chat:
    """Carry the validated fields of one inbound chat message."""

    id: str
    user_id: str
    login: str
    name: str


class MusicTrack(msgspec.Struct, frozen=True):
    """Fields the music commands use from a Trula queue item."""

    title: str | None = None
    duration: str | int | float | None = None
    is_watched: bool | int | str | None = False


class Music:
    """Read and briefly cache the optional music queue."""

    def __init__(self, http: Http, token: str) -> None:
        self.http, self.token = http, token
        self._cached: list[MusicTrack] = []
        self._until = 0.0
        self._lock = asyncio.Lock()

    async def queue(self) -> list[MusicTrack]:
        if not self.token:
            raise ValueError("Музыкальный сервис не настроен: отсутствует TRULA_MUSIC_TOKEN")
        async with self._lock:
            if time.monotonic() < self._until:
                return list(self._cached)
            data = await self.http.request(
                "GET",
                "https://trula-music.ru/obs/orders/",
                params={"token": self.token},
                model=list[MusicTrack],
            )
            if not isinstance(data, list) or any(not isinstance(row, MusicTrack) for row in data):
                raise ProtocolError("Музыкальный сервис вернул не список треков")
            result: list[MusicTrack] = []
            for track in data:
                watched = track.is_watched
                if watched in {True, "true", "True", "1"}:
                    continue
                if watched not in {False, "false", "False", "0", None}:
                    raise ProtocolError("Музыкальный сервис: непонятное значение is_watched")
                result.append(track)
            self._cached, self._until = result, time.monotonic() + 5
            return list(result)


def format_queue(tracks: list[MusicTrack]) -> str:
    """Summarize the music queue within chat message limits."""
    if not tracks:
        return "В очереди пока ничего нет."
    parts: list[str] = []
    for index, track in enumerate(tracks[:10], 1):
        title = chat_text(track.title or "Неизвестный трек", 90)
        part = f"{index}. {'▶' if index == 1 else '⏭'} {title}"
        if len(" | ".join([*parts, part])) > MAX_QUEUE_TEXT_LENGTH:
            break
        parts.append(part)
    remaining = len(tracks) - len(parts)
    return "Очередь: " + " | ".join(parts) + (f" | …ещё {remaining}" if remaining else "")


class Commands:
    """Explicit command registry and one handler pipeline, independent of EventSub I/O."""

    def __init__(self, config: Config, api: Twitch, store: Store, state: Health) -> None:
        self.config, self.api, self.store, self.state = config, api, store, state
        self.music = Music(api.http, config.music_token)
        self.ban_responder = BanResponder()
        self.handlers: frozendict[str, Callable[[Chat, str], Awaitable[None]]] = frozendict({
            name: handler
            for handler, names in (
                (self.ping, ("ping", "пинг")),
                (self.help, ("help", "commands", "команды")),
                (self.discord, ("discord", "ds", "дс", "дискорд")),
                (self.telegram, ("telegram", "tg", "тг", "телеграм")),
                (self.followage, ("followage",)),
                (self.currentsong, ("currentsong", "трек")),
                (self.queue, ("queue", "очередь", "q")),
                (self.ban, ("ban", "бан", "удалить", "забанить")),
            )
            for name in names
        })

    def is_channel_event(self, event: ChatEvent | StreamEvent) -> bool:
        """Match the configured channel and the originating Shared Chat channel."""
        if event.broadcaster_user_id != self.config.channel_id:
            return False
        return not isinstance(event, ChatEvent) or event.source_broadcaster_user_id in {
            None,
            "",
            self.config.channel_id,
        }

    def command_user(self, frame: Frame) -> str | None:
        """Identify viewer commands for admission before they enter the queue."""
        payload = frame.payload
        if not isinstance(payload, NotificationPayload) or not isinstance(
            payload.event, ChatEvent
        ):
            return None
        event = payload.event
        if not self.is_channel_event(event) or event.chatter_user_id == self.config.bot_id:
            return None
        parsed = parse_command(event.message.text, self.config.prefix)
        return event.chatter_user_id if parsed and parsed[0] in self.handlers else None

    async def send(self, text: str, reply_to: str | None = None) -> str:
        try:
            message_id = await self.api.send(text, reply_to=reply_to)
        except (
            RemoteError,
            NetworkError,
            DeliveryError,
            ProtocolError,
            AuthRequiredError,
            TimeoutError,
        ) as exc:
            self.state.send_errors += 1
            self.state.last_delivery_error = REDACT(exc)[:300]
            LOG.error("SEND FAILED | %s", exc)  # ruff: ignore[error-instead-of-exception] - propagated failure
            raise
        self.state.sent += 1
        self.state.last_send_at = time.time()
        self.state.last_delivery_error = ""
        LOG.info(
            "SEND ACCEPTED | channel_id=%s | message_id=%s", self.config.channel_id, message_id
        )
        return message_id

    async def reply(self, chat: Chat, text: str) -> None:
        await self.send(text, reply_to=chat.id)

    async def handle(self, frame: Frame) -> None:
        payload = frame.payload
        if not isinstance(payload, NotificationPayload):
            raise ProtocolError("Команда получила не EventSub notification")
        event = payload.event
        kind = payload.subscription.type
        if not self.is_channel_event(event):
            self.state.filtered_events += 1
            return
        if kind == "stream.online":
            if not isinstance(event, StreamEvent):
                raise ProtocolError("Stream notification без stream event")
            if self.config.greet_stream and await self.store.claim_event(f"stream:{event.id}"):
                name = event.broadcaster_user_name or self.state.channel_login
                await self.send(f"Привет, {name}! yablok2Kiss")
            return
        if kind != "channel.chat.message":
            return
        if not isinstance(event, ChatEvent):
            raise ProtocolError("Chat notification без chat event")
        await self._chat_message(event)

    async def _chat_message(self, event: ChatEvent) -> None:
        chat = Chat(
            event.message_id,
            event.chatter_user_id,
            event.chatter_user_login,
            event.chatter_user_name,
        )
        parsed = parse_command(event.message.text, self.config.prefix)
        if chat.user_id == self.config.bot_id:
            # A single-use, locally requested diagnostic. Ordinary bot messages
            # never invoke commands, so the bot cannot get into a reply loop.
            if (
                parsed
                and parsed[0] == "fgpcheck"
                and re.fullmatch(r"[0-9a-f]{32}", parsed[1])
                and await self.store.probe_observed(parsed[1], chat.id, self.state.run_id)
            ):
                LOG.info("PROBE RECEIVED | diagnostic command reached handler")
            return
        await self._dispatch(chat, parsed)

    async def _dispatch(self, chat: Chat, parsed: tuple[str, str] | None) -> None:
        if not parsed or parsed[0] not in self.handlers:
            return
        if not await self.store.claim_event(f"chat:{self.config.channel_id}:{chat.id}"):
            self.state.duplicate_events += 1
            return
        self.state.commands += 1
        self.state.last_command_at = time.time()
        LOG.info(
            "COMMAND | name=%s | user_id=%s | message_id=%s", parsed[0], chat.user_id, chat.id
        )
        try:
            async with asyncio.timeout(40):
                await self.handlers[parsed[0]](chat, parsed[1])
        except (
            AuthRequiredError,
            RemoteError,
            NetworkError,
            ProtocolError,
            DeliveryError,
            TimeoutError,
        ) as exc:
            self.state.command_errors += 1
            self.state.error(exc)
            LOG.error("COMMAND FAILED | name=%s | %s", parsed[0], exc)  # ruff: ignore[error-instead-of-exception] - known command failure
            # No recursive error response: the send path itself may be unavailable.

    async def ping(self, chat: Chat, _: str) -> None:
        await self.reply(chat, "Pong! FGPbot работает.")

    async def help(self, chat: Chat, _: str) -> None:
        p = self.config.prefix
        await self.reply(
            chat,
            (
                f"Команды: {p}ping, {p}followage [ник], {p}трек, "
                f"{p}очередь, {p}дс, {p}тг, {p}бан [ник]."
            ),
        )

    async def discord(self, chat: Chat, _: str) -> None:
        await self.reply(chat, f"Дискорд: {self.config.discord_url}")

    async def telegram(self, chat: Chat, _: str) -> None:
        await self.reply(chat, f"Телеграм: {self.config.telegram_url}")

    async def target(self, chat: Chat, name: str) -> User | None:
        name = name.strip().lstrip("@").lower()
        if not name:
            return User(chat.user_id, chat.login, chat.name)
        if not re.fullmatch(r"[a-z0-9_]{1,25}", name):
            return None
        users = await self.api.users(login=name)
        return users[0] if users else None

    async def followage(self, chat: Chat, name: str) -> None:
        target = await self.target(chat, name)
        if target is None:
            await self.reply(
                chat, "Не нашёл такой ник. Укажи логин Twitch, как в ссылке на канал."
            )
            return
        result = await self._followers(target.id)
        if result is None:
            self.state.features["followage"] = "AUTH_REQUIRED"
            await self.reply(
                chat,
                "Не могу проверить фоллоу. Нужны права на просмотр фолловеров.",
            )
            return
        self.state.features["followage"] = "READY"
        rows = result.data
        if not rows:
            await self.reply(chat, f"@{target.login} пока не фолловит этот канал.")
            return
        try:
            followed = datetime.fromisoformat(rows[0].followed_at)
            if followed.tzinfo is None:
                raise ValueError
        except ValueError, TypeError:
            raise ProtocolError("Followage: некорректная дата") from None
        age = format_time_russian(int((datetime.now(UTC) - followed).total_seconds()))
        await self.reply(chat, f"@{target.login} следит за каналом уже {age}!")

    async def _followers(self, user_id: str) -> Followers | None:
        # Broadcaster token is OPTIONAL. A scoped bot moderator token also works.
        for token_user in dict.fromkeys((self.config.channel_id, self.config.bot_id)):
            try:
                return await self.api.request(
                    "GET",
                    "/channels/followers",
                    user_id=token_user,
                    scopes=FOLLOW_SCOPE,
                    params={
                        "broadcaster_id": self.config.channel_id,
                        "user_id": user_id,
                        "first": "1",
                    },
                    model=Followers,
                )
            except AuthRequiredError:
                continue
            except RemoteError as exc:
                if exc.status not in {401, 403}:
                    raise
        return None

    async def _music(self, chat: Chat) -> list[MusicTrack] | None:
        try:
            tracks = await self.music.queue()
            self.state.features["music"] = "READY"
        except (ValueError, RemoteError, NetworkError, ProtocolError, TimeoutError) as exc:
            self.state.features["music"] = "UNAVAILABLE"
            LOG.warning("Музыкальный сервис недоступен: %s", exc)
            await self.reply(
                chat,
                "Музыкальный сервис недоступен. Не получилось загрузить очередь треков.",
            )
            return None
        else:
            return tracks

    async def currentsong(self, chat: Chat, _: str) -> None:
        tracks = await self._music(chat)
        if tracks is None:
            return
        if not tracks:
            await self.reply(chat, "В очереди пока нет треков.")
            return
        track = tracks[0]
        await self.reply(
            chat,
            (f"Сейчас играет: {track.title or 'Неизвестный трек'} ({track.duration or '??:??'})"),
        )

    async def queue(self, chat: Chat, _: str) -> None:
        tracks = await self._music(chat)
        if tracks is not None:
            await self.reply(chat, format_queue(tracks))

    async def ban(self, chat: Chat, name: str) -> None:
        target = await self.target(chat, name)
        if target is None:
            await self.reply(chat, "Такого ника не нашёл. Ты уже и людей для бана выдумываешь?")
            return
        kind: BanKind
        if target.id == self.config.bot_id:
            kind = "bot"
        elif target.id == self.config.channel_id:
            kind = "streamer"
        elif target.id == chat.user_id:
            kind = "self"
        else:
            kind = "viewer"
        template = self.ban_responder.reply(
            target.id, chat.user_id, kind=kind, now=time.monotonic()
        )
        await self.reply(
            chat, template.format(target=f"@{target.login}", invoker=f"@{chat.login}")
        )
