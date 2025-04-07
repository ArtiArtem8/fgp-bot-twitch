import datetime
import logging

from twitchio import ChannelFollowerEvent
from twitchio.ext import commands

from utils import format_time_russian


class FollowAge(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("FollowAge")

    @commands.command(name="followage")
    async def follow_age(self, ctx: commands.Context, *, username: str = "") -> None:
        username = username.strip().strip("󠀀").lstrip("@").lower()
        if not username:
            follow_info: ChannelFollowerEvent = await ctx.chatter.follow_info()
            if follow_info is None:
                return await ctx.reply(f"Вы не зафоловились на {ctx.broadcaster}!")
            return await self._reply_with_follow_age(
                ctx, follow_info, "Вы следите за этим каналом уже {formatted_age}!"
            )

        followers = await ctx.broadcaster.fetch_followers()
        username = username.strip()
        async for follow_info in followers.followers:
            if follow_info.user.name.lower().strip() == username:
                break
        else:
            return await ctx.reply("Такого фолловера не существует!")

        await self._reply_with_follow_age(
            ctx,
            follow_info,
            f"Пользователь {follow_info.user.mention} "
            "следит за этим каналом уже {formatted_age}!",
        )

    async def _reply_with_follow_age(
        self, ctx: commands.Context, follow_info: ChannelFollowerEvent, message: str
    ):
        self.logger.debug(f"Follow info: {follow_info}")
        now = datetime.datetime.now(datetime.timezone.utc)
        follow_age = now - follow_info.followed_at
        formatted_age = format_time_russian(follow_age.total_seconds())
        await ctx.reply(message.format(formatted_age=formatted_age))
