import logging

import aiohttp
from twitchio.ext import commands

from config import MUSIC_TOKEN


class MusicQueue(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("MusicQueue")

    async def fetch_queue(self) -> list[dict]:
        """Helper function to fetch music queue from API"""
        url = f"https://trula-music.ru/obs/orders/?token={MUSIC_TOKEN}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if 200 <= response.status <= 210:
                        data = await response.json()
                        if isinstance(data, list):
                            return data
                        raise ValueError("Invalid API response format")
                    raise ConnectionError(f"API returned status {response.status}")

        except aiohttp.ClientError as e:
            self.logger.error(f"Connection error: {e}", exc_info=True)
            raise ConnectionError("Could not connect to music service") from e
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
            raise

    @commands.command(aliases=["трек"])
    async def currentsong(self, ctx: commands.Context) -> None:
        """Show currently playing track"""
        try:
            data = await self.fetch_queue()
            if not data:
                return await ctx.reply("Сейчас в очереди нет музыки")

            # Find current track (first unwatched)
            current_track = next(
                (t for t in data if not t.get("is_watched", False)), None
            )

            if current_track:
                title = current_track.get("title", "Неизвестный трек")
                duration = current_track.get("duration", "??:??")
                await ctx.reply(f"Сейчас играет: {title} ({duration})")
            else:
                # Show first track if all are watched
                next_track = data[0]
                title = next_track.get("title", "Неизвестный трек")
                await ctx.reply(f"Следующий трек: {title}")

        except ConnectionError:
            pass
            # await ctx.reply("Ошибка соединения с музыкальным сервисом")
        except Exception as e:
            self.logger.error(f"Error in currentsong: {e}")
            # await ctx.reply("Произошла ошибка при получении информации")

    @commands.command(aliases=["очередь", "q"])
    async def queue(self, ctx: commands.Context) -> None:
        """Show up to 10 tracks in the queue"""
        try:
            data = await self.fetch_queue()
            if not data:
                return await ctx.reply("Музыкальная очередь пуста")

            queue = []
            current_found = False
            for idx, track in enumerate(data[:10], 1):
                status = (
                    "▶"
                    if not track.get("is_watched", False) and not current_found
                    else "⏭"
                )
                if status == "▶":
                    current_found = True
                title = track.get("title", "Неизвестный трек")
                queue.append(f"{idx}. {status} {title}")
            overflow = ""
            if len(data) > 10:
                overflow = f"\n...и ещё {len(data) - 10} треков"
            await ctx.reply(f"Очередь треков:{overflow}\n" + " | ".join(queue))
        except ConnectionError:
            pass
            # await ctx.reply("⚠ Ошибка соединения с музыкальным сервисом")
        except Exception as e:
            self.logger.error(f"Error in queue: {e}")
            # await ctx.reply("⚠ Произошла ошибка при получении очереди")
