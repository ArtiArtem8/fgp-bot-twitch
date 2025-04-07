from twitchio.ext import commands

from config import BOT_ID


class Socials(commands.Component):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(aliases=["ds", "дс", "дискорд"])
    async def discord(self, ctx: commands.Context) -> None:
        await ctx.reply("Дискорд: https://discord.gg/qKV4BCCgZ5")

    @commands.command(aliases=["tg", "тг", "телеграм"])
    async def telegram(self, ctx: commands.Context) -> None:
        await ctx.reply("Телеграм: https://t.me/yabloko18twitch")

    @commands.command(aliases=["бан", "удалить", "забанить"])
    async def ban(self, ctx: commands.Context, *, username: str = "") -> None:
        username = username.strip().lstrip("@").lower()

        if username == "":
            username = ctx.author.display_name.lower()

        res = await ctx.broadcaster.fetch_chatters(moderator=BOT_ID)
        async for user in res.users:
            if str(user.display_name).lower() == username:
                break
        else:
            return await ctx.reply("Такого чаттерса тут нет! Проверьте написание имени")

        await ctx.reply(
            f"Внимание! Пользователь {user.mention} был удалён из чата! Обратитесь в администрацию для подачи апелляции!"
        )
