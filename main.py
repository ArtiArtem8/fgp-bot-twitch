import asyncio
import importlib
import logging
import sqlite3
from pathlib import Path

import asqlite
import twitchio
from twitchio import eventsub
from twitchio.ext import commands

from config import (
    BOT_ID,
    CLIENT_ID,
    CLIENT_SECRET,
    COMPONENTS_DIRECTORY,
    OWNER_ID,
    TOKENS_DATABASE_PATH,
)

LOGGER: logging.Logger = logging.getLogger("Bot")


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

    async def setup_hook(self) -> None:
        await self.add_components_from_directory(COMPONENTS_DIRECTORY)
        await self.add_component(StartUp(self))
        subscription = eventsub.ChatMessageSubscription(
            broadcaster_user_id=OWNER_ID, user_id=BOT_ID
        )
        await self.subscribe_websocket(payload=subscription)

        # For StartUp purpose
        subscription = eventsub.StreamOnlineSubscription(broadcaster_user_id=OWNER_ID)
        await self.subscribe_websocket(payload=subscription)

    async def add_components_from_directory(self, directory: Path) -> None:
        for component_file in directory.glob("*.py"):
            if component_file.name == "__init__.py":
                continue

            module_name = f"components.{component_file.stem}"
            try:
                module = importlib.import_module(module_name)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if isinstance(attr, type) and issubclass(attr, commands.Component):
                        await self.add_component(attr(self))
                        LOGGER.info(f"Loaded component: {module_name}.{attr_name}")
            except Exception as e:
                LOGGER.error(
                    f"Failed to load component {module_name}: {e}", exc_info=True
                )

    async def add_token(
        self, token: str, refresh: str
    ) -> twitchio.authentication.ValidateTokenPayload:
        resp: twitchio.authentication.ValidateTokenPayload = await super().add_token(
            token, refresh
        )

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

        LOGGER.info("Added token to the database for user: %s", resp.user_id)
        return resp

    async def load_tokens(self, path: str | None = None) -> None:
        async with self.token_database.acquire() as connection:
            rows: list[sqlite3.Row] = await connection.fetchall(
                """SELECT * from tokens"""
            )

        for row in rows:
            await self.add_token(row["token"], row["refresh"])

    async def setup_database(self) -> None:
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

    async def event_ready(self) -> None:
        LOGGER.info("Successfully logged in as: %s", self.bot_id)


class StartUp(commands.Component):
    def __init__(self, bot):
        self.bot = bot

    @commands.Component.listener()
    async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
        await payload.broadcaster.send_message(
            sender=self.bot.bot_id,
            message=f"Привет... {payload.broadcaster}! :yablok2Kiss:",
        )


def main() -> None:
    twitchio.utils.setup_logging(level=logging.INFO)

    async def runner() -> None:
        async with (
            asqlite.create_pool(TOKENS_DATABASE_PATH) as tdb,
            Bot(token_database=tdb) as bot,
        ):
            await bot.setup_database()
            await bot.start()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        LOGGER.warning("Shutting down due to KeyboardInterrupt...")


if __name__ == "__main__":
    main()
