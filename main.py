import asyncio
import functools
import importlib
import logging
import random
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
import asqlite
import twitchio
from twitchio import eventsub
from twitchio.authentication import ManagedHTTPClient, OAuth
from twitchio.ext import commands
from twitchio.utils import MISSING

from config import (
    BOT_ID,
    CLIENT_ID,
    CLIENT_SECRET,
    COMPONENTS_DIRECTORY,
    OWNER_ID,
    TOKENS_DATABASE_PATH,
)

LOGGER: logging.Logger = logging.getLogger("Bot")

if TYPE_CHECKING:
    from collections.abc import Callable


class Backoff:
    """An implementation of an Exponential Backoff.

    Parameters
    ----------
    base: int
        The base time to multiply exponentially. Defaults to 1.
    maximum_time: float
        The maximum wait time. Defaults to 30.0
    maximum_tries: Optional[int]
        The amount of times to backoff before resetting. Defaults to 5. If set to None, backoff will run indefinitely.
    """

    def __init__(
        self,
        *,
        base: int = 1,
        maximum_time: float = 30.0,
        maximum_tries: int | None = 5,
    ) -> None:
        self._base: int = base
        self._maximum_time: float = maximum_time
        self._maximum_tries: int | None = maximum_tries
        self._retries: int = 1

        rand = random.Random()
        rand.seed()

        self._rand: Callable[[float, float], float] = rand.uniform

        self._last_wait: float = 0

    def calculate(self) -> float:
        exponent = min((self._retries**2), self._maximum_time)
        wait = self._rand(0, (self._base * 2) * exponent)

        if wait <= self._last_wait:
            wait = self._last_wait * 2

        self._last_wait = wait

        if wait > self._maximum_time:
            wait = self._maximum_time
            self._retries = 0
            self._last_wait = 0

        if self._maximum_tries and self._retries >= self._maximum_tries:
            self._retries = 0
            self._last_wait = 0

        self._retries += 1

        return wait


class ProxiedOAuth(OAuth):
    """OAuth клиент с поддержкой прокси."""

    PROXY_URL = "http://127.0.0.1:12334"
    USE_PROXY = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = MISSING
        self._proxy_patched = False

    async def _init_session(self) -> None:
        """Инициализация сессии с прокси."""
        if self._session is not MISSING:
            return

        LOGGER.debug("Creating ProxiedOAuth session")

        connector = aiohttp.TCPConnector(
            limit=100, ttl_dns_cache=300, use_dns_cache=True
        )

        timeout = aiohttp.ClientTimeout(total=120, connect=60)

        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        if self.USE_PROXY and not self._proxy_patched:
            original_request = self._session._request

            @functools.wraps(original_request)
            async def proxied_request(method, str_or_url, **kwargs):
                kwargs["proxy"] = self.PROXY_URL
                LOGGER.debug(f"→ OAuth {method} {str_or_url}")
                response = await original_request(method, str_or_url, **kwargs)
                LOGGER.debug(f"← {response.status}")
                return response

            self._session._request = proxied_request
            self._proxy_patched = True
            LOGGER.debug("✓ ProxiedOAuth session patched")


class ProxiedHTTPClient(ManagedHTTPClient):
    """HTTP клиент с поддержкой Hiddify прокси."""

    PROXY_URL = "http://127.0.0.1:12334"
    USE_PROXY = True

    def __init__(self, *args, **kwargs):
        # НЕ вызываем super().__init__() сразу
        # Сначала инициализируем OAuth базовый класс
        OAuth.__init__(
            self,
            client_id=kwargs.get("client_id"),
            client_secret=kwargs.get("client_secret"),
            redirect_uri=kwargs.get("redirect_uri"),
            scopes=kwargs.get("scopes"),
            session=MISSING,
        )

        # Создаем ProxiedOAuth вместо обычного OAuth
        self._ManagedHTTPClient__isolated = ProxiedOAuth(
            client_id=kwargs.get("client_id"),
            client_secret=kwargs.get("client_secret"),
            redirect_uri=kwargs.get("redirect_uri"),
            scopes=kwargs.get("scopes"),
            session=MISSING,
        )

        # Остальные атрибуты из ManagedHTTPClient.__init__
        self._tokens = {}
        self._app_token = None
        self._nested_key = None
        self._token_lock = asyncio.Lock()
        self._has_loaded = False

        self._backoff = Backoff(base=3, maximum_time=90)  # type: ignore

        self._validate_task = None
        self._client = kwargs.get("client")

        # Наши атрибуты
        self._session = MISSING
        self._proxy_patched = False

        if self.USE_PROXY:
            LOGGER.info(f"ProxiedHTTPClient will use proxy: {self.PROXY_URL}")

    async def _init_session(self) -> None:
        """Инициализация основной сессии с прокси."""
        if self._session is not MISSING:
            return

        LOGGER.debug("Creating main ProxiedHTTPClient session")

        connector = aiohttp.TCPConnector(
            limit=100, ttl_dns_cache=300, use_dns_cache=True
        )

        timeout = aiohttp.ClientTimeout(
            total=120, connect=60, sock_connect=30, sock_read=30
        )

        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)

        if self.USE_PROXY and not self._proxy_patched:
            original_request = self._session._request

            @functools.wraps(original_request)
            async def proxied_request(method, str_or_url, **kwargs):
                kwargs["proxy"] = self.PROXY_URL
                LOGGER.debug(f"→ Main {method} {str_or_url}")
                response = await original_request(method, str_or_url, **kwargs)
                LOGGER.debug(f"← {response.status}")
                return response

            self._session._request = proxied_request
            self._proxy_patched = True
            LOGGER.info("✓ Main session patched with Hiddify proxy")


class Bot(commands.Bot):
    def __init__(self, *, token_database: asqlite.Pool) -> None:
        self.token_database = token_database

        super().__init__(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            bot_id=BOT_ID,
            owner_id=OWNER_ID,
            prefix="!",
        )

        self._http = ProxiedHTTPClient(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

        LOGGER.info("Bot initialized with ProxiedHTTPClient")

    async def setup_hook(self) -> None:
        """Настройка бота перед запуском."""
        LOGGER.info("Running setup_hook...")

        # Загружаем компоненты
        await self.add_components_from_directory(COMPONENTS_DIRECTORY)

        # Добавляем встроенный компонент
        await self.add_component(StartUp(self))

        # Подписки на EventSub будут создаваться после авторизации токенов
        # Здесь не создаем подписки, так как нужен user token

        LOGGER.info("✓ Setup hook completed")

    async def event_token_added(
        self, payload: twitchio.authentication.ValidateTokenPayload
    ) -> None:
        """Вызывается когда токен успешно добавлен."""
        if not payload.user_id:
            return

        # Не подписываемся на события для самого бота
        if payload.user_id == self.bot_id:
            LOGGER.info(f"Bot token added for user: {payload.user_id}")
            return

        LOGGER.info(f"Creating EventSub subscriptions for user: {payload.user_id}")

        try:
            # ChatMessage subscription требует user token
            chat_sub = eventsub.ChatMessageSubscription(
                broadcaster_user_id=payload.user_id, user_id=self.bot_id
            )
            await self.subscribe_websocket(
                payload=chat_sub,
                token_for=payload.user_id,  # Используем user token
            )

            # Stream online subscription
            stream_sub = eventsub.StreamOnlineSubscription(
                broadcaster_user_id=payload.user_id
            )
            await self.subscribe_websocket(
                payload=stream_sub, token_for=payload.user_id
            )

            LOGGER.info(f"✓ Subscriptions created for user: {payload.user_id}")

        except Exception as e:
            LOGGER.error(f"Failed to create subscriptions for {payload.user_id}: {e}")

    async def add_components_from_directory(self, directory: Path) -> None:
        """Динамическая загрузка компонентов."""
        LOGGER.info(f"Loading components from: {directory}")

        for component_file in directory.glob("*.py"):
            if component_file.name == "__init__.py":
                continue

            module_name = f"components.{component_file.stem}"
            try:
                module = importlib.import_module(module_name)

                for attr_name in dir(module):
                    attr = getattr(module, attr_name)

                    if (
                        isinstance(attr, type)
                        and issubclass(attr, commands.Component)
                        and attr is not commands.Component
                    ):
                        await self.add_component(attr(self))
                        LOGGER.info(f"✓ Loaded component: {module_name}.{attr_name}")

            except Exception as e:
                LOGGER.error(
                    f"✗ Failed to load component {module_name}: {e}", exc_info=True
                )

    async def add_token(
        self, token: str, refresh: str
    ) -> twitchio.authentication.ValidateTokenPayload:
        """Добавляет токен и сохраняет в базу."""
        resp = await super().add_token(token, refresh)

        query = """
        INSERT INTO tokens (user_id, token, refresh)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id)
        DO UPDATE SET
            token = excluded.token,
            refresh = excluded.refresh;
        """

        async with self.token_database.acquire() as connection:
            await connection.execute(query, (resp.user_id, token, refresh))

        LOGGER.info(f"Token saved for user: {resp.user_id}")

        # Вызываем событие для создания подписок
        await self.event_token_added(resp)

        return resp

    async def load_tokens(self, path: str | None = None) -> None:
        """Загружает токены из базы данных."""
        LOGGER.info("Loading tokens from database...")

        async with self.token_database.acquire() as connection:
            rows: list[sqlite3.Row] = await connection.fetchall("SELECT * FROM tokens")

        loaded_count = 0
        for row in rows:
            try:
                await self.add_token(row["token"], row["refresh"])
                loaded_count += 1
            except Exception as e:
                LOGGER.error(f"Failed to load token for user {row['user_id']}: {e}")

        LOGGER.info(f"✓ Loaded {loaded_count} tokens")

    async def setup_database(self) -> None:
        """Создает таблицы в базе данных."""
        LOGGER.info("Setting up database...")

        create_tokens_table = """
        CREATE TABLE IF NOT EXISTS tokens(
            user_id TEXT PRIMARY KEY, 
            token TEXT NOT NULL, 
            refresh TEXT NOT NULL
        )
        """

        create_messages_table = """
        CREATE TABLE IF NOT EXISTS messages(
            message_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            username TEXT NOT NULL,
            display_name TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            message_text TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            badges TEXT,
            is_subscriber BOOLEAN,
            is_follower BOOLEAN,
            message_type TEXT NOT NULL
        )
        """

        queries = [create_tokens_table, create_messages_table]

        async with self.token_database.acquire() as connection:
            for query in queries:
                await connection.execute(query)

        LOGGER.info("✓ Database setup completed")

    async def event_ready(self) -> None:
        """Вызывается когда бот готов к работе."""
        LOGGER.info(f"✓ Bot ready! Logged in as: {self.bot_id}")


class StartUp(commands.Component):
    """Компонент для приветствия при запуске стрима."""

    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @commands.Component.listener()
    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        """Отправляет приветствие когда стрим начинается."""
        try:
            await payload.broadcaster.send_message(
                sender=self.bot.bot_id,
                message=f"Привет... {payload.broadcaster}! yablok2Kiss",
            )
            LOGGER.info(f"Sent startup message to {payload.broadcaster.name}")
        except Exception as e:
            LOGGER.error(f"Failed to send startup message: {e}")


async def start_with_retry(
    bot: Bot, max_retries: int = 3, base_delay: float = 5.0
) -> None:
    """Запуск бота с повторными попытками."""
    for attempt in range(1, max_retries + 1):
        try:
            LOGGER.info(f"Starting bot (attempt {attempt}/{max_retries})...")
            await bot.start()
            break

        except (asyncio.TimeoutError, aiohttp.ClientConnectionError) as e:
            if attempt == max_retries:
                LOGGER.error(f"Failed to start bot after {max_retries} attempts")
                raise

            delay = base_delay * (2 ** (attempt - 1))
            LOGGER.warning(f"Connection failed: {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)

    LOGGER.info("✓ Bot started successfully")


def main() -> None:
    """Точка входа в приложение."""
    twitchio.utils.setup_logging(level=logging.DEBUG)

    async def runner() -> None:
        LOGGER.info("Starting Twitch bot...")

        async with asqlite.create_pool(str(TOKENS_DATABASE_PATH)) as tdb:
            async with Bot(token_database=tdb) as bot:
                await bot.setup_database()
                await bot.load_tokens()
                await start_with_retry(bot, max_retries=3, base_delay=5.0)

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        LOGGER.warning("Shutdown requested by user")
    except Exception as e:
        LOGGER.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
