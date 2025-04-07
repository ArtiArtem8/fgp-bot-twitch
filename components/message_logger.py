import json
import logging

import twitchio
from twitchio.ext import commands


class MessageLogger(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("MessageLogger")

    @commands.Component.listener()
    async def event_message(self, payload: twitchio.ChatMessage) -> None:
        try:
            is_subscriber = any(
                badge.set_id == "Subscriber" for badge in payload.badges
            )
            badges = [
                {"set_id": badge.set_id, "version": badge.id}
                for badge in payload.badges
            ]
            is_follower = await payload.chatter.follow_info() is not None
            data = (
                payload.id,
                str(payload.chatter.id),
                payload.chatter.name,
                payload.chatter.display_name,
                str(payload.broadcaster.id),
                payload.text,
                payload.timestamp,
                json.dumps(badges) if badges else None,
                is_subscriber,
                is_follower,
                payload.type,
            )

            query = """
            INSERT INTO messages (
                message_id, user_id, username, display_name, 
                channel_id, message_text, timestamp, 
                badges, is_subscriber, is_follower, message_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            async with self.bot.token_database.acquire() as conn:
                await conn.execute(query, data)

            self.logger.debug(f"Saved message: {data}")
            self.logger.info(f"Logged message from {payload.chatter.display_name}")

        except Exception as e:
            self.logger.error(f"Failed to log message: {e}", exc_info=True)
