import twitchio
import json
import logging
import datetime
from twitchio.ext import commands

class FollowAge(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("FollowAge")

    @commands.command(aliases=["followage"])
    async def follow_age(self, ctx: commands.Context) -> None:
        follow_info: twitchio.ChannelFollowerEvent  = await ctx.chatter.follow_info()
        self.logger.debug(f"Follow info: {follow_info}")
        print(follow_info)
        if not follow_info:
            await ctx.reply("You are not following this channel!")
        else:
            follow_age = (datetime.datetime.now() - follow_info.followed_at).days
            await ctx.reply(f"You have been following this channel for {follow_age} days!")
        
    
    
        