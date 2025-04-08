import datetime
import logging

from twitchio import ChannelFollowerEvent, PartialUser, User
from twitchio.ext import commands

from utils import format_time_russian


class FollowAge(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("FollowAge")

    @commands.command(name="followage")
    async def follow_age(self, ctx: commands.Context, *, username: str = "") -> None:
        username = username.strip().strip("").lstrip("@").lower()
        broadcaster = ctx.broadcaster
        target_user = (
            ctx.author if not username else await self._resolve_user(ctx, username)
        )

        if target_user is None:
            return await ctx.reply("Пользователь не найден. Проверьте написание имени.")

        follow_info = await self._get_follow_info(broadcaster, target_user)

        if follow_info is None:
            not_followed_msg = (
                f"Вы не зафоловлены на {broadcaster.display_name}!"
                if not username
                else f"Пользователь {target_user.display_name} не зафоловлен на {broadcaster.display_name}!"
            )
            return await ctx.reply(not_followed_msg)

        followed_message = (
            "Вы следите за этим каналом уже {formatted_age}!"
            if not username
            else f"Пользователь {follow_info.user.mention}"
            " следит за этим каналом уже {formatted_age}!"
        )

        await self._reply_with_follow_age(ctx, follow_info, followed_message)

    async def _get_follow_info(
        self, broadcaster: PartialUser, user: User
    ) -> ChannelFollowerEvent | None:
        followers = await broadcaster.fetch_followers(user=user.id, max_results=1)
        return await anext(followers.followers, None)

    async def _reply_with_follow_age(
        self, ctx: commands.Context, follow_info: ChannelFollowerEvent, message: str
    ):
        self.logger.debug(f"Follow info: {follow_info}")
        now = datetime.datetime.now(datetime.timezone.utc)
        follow_age = now - follow_info.followed_at
        formatted_age = format_time_russian(follow_age.total_seconds())
        await ctx.reply(message.format(formatted_age=formatted_age))

    async def _resolve_user(self, ctx: commands.Context, username: str) -> User | None:
        try:
            users = await ctx.bot.fetch_users(logins=[username])
            return users[0] if users else None
        except Exception:
            self.logger.error("Failed to resolve user", exc_info=True)
            return None
