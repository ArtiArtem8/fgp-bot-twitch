import asyncio
import logging
import sqlite3
import os
import asqlite
import twitchio
import json
import datetime
import importlib
import pkgutil
from pathlib import Path
from twitchio.ext import commands
from twitchio import eventsub
from dotenv import load_dotenv


LOGGER: logging.Logger = logging.getLogger("Bot")

load_dotenv()

CLIENT_ID: str = os.getenv("TWITCH_BOT_APP_CLIENT_ID")
CLIENT_SECRET: str = os.getenv("TWITCH_BOT_APP_CLIENT_SECRET")
BOT_ID: str = os.getenv("TWITCH_BOT_ID")
OWNER_ID: str = os.getenv("TWITCH_OWNER_ID")

COMPONENTS_DIR = Path(__file__).parent / "components"
DATABASE_PATH = Path(__file__).parent / "data" / "tokens.db"


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
        # Add our component which contains our commands...
        await self.add_components_from_directory(COMPONENTS_DIR)

        # Subscribe to read chat (event_message) from our channel as the bot...
        # This creates and opens a websocket to Twitch EventSub...
        subscription = eventsub.ChatMessageSubscription(
            broadcaster_user_id=OWNER_ID, user_id=BOT_ID
        )
        await self.subscribe_websocket(payload=subscription)

        # Subscribe and listen to when a stream goes live..
        # For this example listen to our own stream...
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
                LOGGER.error(f"Failed to load component {module_name}: {e}", exc_info=True)
        

    async def add_token(
        self, token: str, refresh: str
    ) -> twitchio.authentication.ValidateTokenPayload:
        # Make sure to call super() as it will add the tokens interally and return us some data...
        resp: twitchio.authentication.ValidateTokenPayload = await super().add_token(
            token, refresh
        )

        # Store our tokens in a simple SQLite Database when they are authorized...
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
        # We don't need to call this manually, it is called in .login() from .start() internally...

        async with self.token_database.acquire() as connection:
            rows: list[sqlite3.Row] = await connection.fetchall(
                """SELECT * from tokens"""
            )

        for row in rows:
            await self.add_token(row["token"], row["refresh"])

    async def setup_database(self) -> None:
        # Create our token table, if it doesn't exist..
        queries = [
            """CREATE TABLE IF NOT EXISTS tokens(
                user_id TEXT PRIMARY KEY, 
                token TEXT NOT NULL, 
                refresh TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS messages(
                message_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                username TEXT NOT NULL,
                display_name TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                message_text TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                badges TEXT,
                is_subscriber BOOLEAN,
                message_type TEXT NOT NULL
            )"""
        ]
        async with self.token_database.acquire() as connection:
            for query in queries:
                await connection.execute(query)

    async def event_ready(self) -> None:
        LOGGER.info("Successfully logged in as: %s", self.bot_id)


# class MyComponent(commands.Component):
#     def __init__(self, bot: Bot):
#         self.bot = bot


#     @commands.command(aliases=["hello", "howdy", "hey"])
#     async def hi(self, ctx: commands.Context) -> None:
#         """Simple command that says hello!

#         !hi, !hello, !howdy, !hey
#         """
        
#         await ctx.reply(f"Hello {ctx.chatter.mention}!")

    # @commands.group(invoke_fallback=True)
    # async def socials(self, ctx: commands.Context) -> None:
    #     """Group command for our social links.

    #     !socials
    #     """
    #     await ctx.send("discord.gg/..., youtube.com/..., twitch.tv/...")

    # @socials.command(name="discord")
    # async def socials_discord(self, ctx: commands.Context) -> None:
    #     """Sub command of socials that sends only our discord invite.

    #     !socials discord
    #     """
    #     await ctx.send("discord.gg/...")

    # @commands.command(aliases=["repeat"])
    # @commands.is_moderator()
    # async def say(self, ctx: commands.Context, *, content: str) -> None:
    #     """Moderator only command which repeats back what you say.

    #     !say hello world, !repeat I am cool LUL
    #     """
    #     await ctx.send(content)

    # @commands.Component.listener()
    # async def event_stream_online(self, payload: twitchio.StreamOnline) -> None:
    #     # Event dispatched when a user goes live from the subscription we made above...

    #     # Keep in mind we are assuming this is for ourselves
    #     # others may not want your bot randomly sending messages...
    #     await payload.broadcaster.send_message(
    #         sender=self.bot.bot_id,
    #         message=f"Hi... {payload.broadcaster}! You are live!",
    #     )


def main() -> None:
    twitchio.utils.setup_logging(level=logging.INFO)

    async def runner() -> None:
        async with (
            asqlite.create_pool(DATABASE_PATH) as tdb,
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
