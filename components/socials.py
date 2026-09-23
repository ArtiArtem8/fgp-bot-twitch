import logging
from typing import Hashable

from twitchio.ext import commands
from ultimate_responder import EngineConfig, PoolConfig, ResponseEngine

from config import BOT_ID

BAN_POOLS: dict[Hashable, PoolConfig] = {
    "ban_ack": {
        "sequence": [
            "Внимание! Пользователь {target} был удалён из чата! Это действие отменить нельзя!",
            "Пользователь {target} снова отправлен в небытие.",
            "Кажется, {target} не выдержал проверку временем.",
        ],
        "random": [
            "Модерация молниеносна: {target} больше с нами не общается.",
            "Чат очищен. До свидания, {target}.",
        ],
        "heated": [
            "Так, хватит банить всех подряд, {invoker}.",
            "Чат горит от банов, {invoker}, может, передохнём?",
        ],
        "sequence_ttl_seconds": 60,
        "heat_window_seconds": 10,
        "cooldown_seconds": 3,
        "allow_immediate_repeat": False,
        "sequence_resets_to_random": True,
    }
}


class Socials(commands.Component):
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("Socials")

        config = EngineConfig(pools=BAN_POOLS)
        self._ban_engine = ResponseEngine(config)

    @commands.command(aliases=["ds", "дс", "дискорд"])
    async def discord(self, ctx: commands.Context) -> None:
        await ctx.reply("Дискорд: https://discord.gg/qKV4BCCgZ5")

    @commands.command(aliases=["tg", "тг", "телеграм"])
    async def telegram(self, ctx: commands.Context) -> None:
        await ctx.reply("Телеграм: https://t.me/yabloko18twitch")

    @commands.command(
        aliases=["бан", "удалить", "забанить"]
    )  # TODO: Добавить больще фановых сообщений о бане, например с другой вариацией, или что если забанить самого бота или стримера
    async def ban(self, ctx: commands.Context, *, username: str = "") -> None:
        username = username.strip().strip(" 󠀀").lstrip("@").lower()

        if username == "":
            username = (ctx.author.display_name or "no_name").lower()

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
            await ctx.reply("Такого чаттерса тут нет! Проверьте написание имени")
            return

        if user.id == ctx.author.id:
            await ctx.reply(
                f"{ctx.author.mention} попытался забанить сам себя. "
                "Шизофрения не допускается. Обратитесь к психотерапевту."
            )
            return
        if user.id == BOT_ID:
            await ctx.reply(
                "Внимание! Бот забанен. Но он сразу же себя разбанил. "
                "Я вообще-то контролирую команды, и мне не нравятся твои действия."
            )
            return
        pool_id = "ban_ack"
        # Personalize by target user; you could also use ctx.author.id if you prefer
        user_identity = str(user.id)
        guild_identity = ctx.channel.name  # channel name as guild identifier

        template = self._ban_engine.get(
            pool_id,
            user_id=user_identity,
            guild_id=guild_identity,
        )
        message = template.format(
            target=user.mention,
            invoker=ctx.author.mention,
        )
        await ctx.reply(message)

    # TODO: Текущее вермя стрима
    # TODO: Попробовать получать текущую музыка с бота который установил
