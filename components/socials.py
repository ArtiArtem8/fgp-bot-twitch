import logging

from twitchio.ext import commands

from config import BOT_ID


class Socials(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("Socials")

    @commands.command(aliases=["ds", "дс", "дискорд"])
    async def discord(self, ctx: commands.Context) -> None:
        await ctx.reply("Дискорд: https://discord.gg/qKV4BCCgZ5")

    @commands.command(aliases=["tg", "тг", "телеграм"])
    async def telegram(self, ctx: commands.Context) -> None:
        await ctx.reply("Телеграм: https://t.me/yabloko18twitch")

    @commands.command(aliases=["бан", "удалить", "забанить"])
    async def ban(self, ctx: commands.Context, *, username: str = "") -> None:
        username = username.strip().strip(" 󠀀").lstrip("@").lower()

        if username == "":
            username = ctx.author.display_name.lower()

        res = await ctx.broadcaster.fetch_chatters(moderator=BOT_ID)
        chatters = [user async for user in res.users]
        user = next(
            (user for user in chatters if str(user.display_name).lower() == username),
            None,
        )

        if user is None:
            self.logger.info(
                f"User not found, look for: {[user.display_name for user in chatters]}"
            )
            return await ctx.reply("Такого чаттерса тут нет! Проверьте написание имени")

        await ctx.reply(
            f"Внимание! Пользователь {user.mention} был удалён из чата! Это действие отменить нельзя!"
        )
