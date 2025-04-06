import twitchio
import json
import logging
from twitchio.ext import commands

class Socials(commands.Component):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(aliases=["ds", "дс", "дискорд"])
    async def discord(self, ctx: commands.Context) -> None:
        await ctx.reply("Дискорд: https://discord.gg/qKV4BCCgZ5")

    @commands.command(aliases=["tg", "тг", "телеграм"])
    async def telegram(self, ctx: commands.Context) -> None:
        await ctx.reply("Телеграм: https://t.me/yabloko18twitch")