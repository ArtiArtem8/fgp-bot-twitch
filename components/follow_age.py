import twitchio
import logging
import datetime
from twitchio.ext import commands
from utils import format_time_russian

class FollowAge(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("FollowAge")

    @commands.command(name="followage")
    async def follow_age(self, ctx: commands.Context) -> None:
        follow_info: twitchio.ChannelFollowerEvent  = await ctx.chatter.follow_info()
        self.logger.debug(f"Follow info: {follow_info}")
        
        if follow_info is None:
            return await ctx.reply(f"Ты не зафоловился на {ctx.broadcaster}!")
            
        now = datetime.datetime.now(datetime.timezone.utc)
        follow_age = (now - follow_info.followed_at)
        
        await ctx.reply(f"Вы следите за этим каналом уже {format_time_russian(follow_age.total_seconds())}!")
