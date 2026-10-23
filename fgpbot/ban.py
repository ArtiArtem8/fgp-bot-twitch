"""Ban dialogue: per-target scenes and per-invoker burst reactions."""

import secrets
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Literal

type BanKind = Literal["viewer", "self", "bot", "streamer"]

SEQUENCES = (
    (
        "Пользователь {target} заблокирован в чате навсегда. Обжалование недоступно.",
        "Пользователь {target} заблокирован повторно. Просьба не вмешиваться в работу модерации.",
        "Кто разбанивает {target} — заберите его к себе.",
    ),
    (
        "Пользователь {target} заблокирован. Решение окончательное.",
        "{target} заблокирован повторно. Прощание затягивается.",
        "{target}, не порти людям воспоминания о себе.",
    ),
)
RANDOM = (
    "{target} забанен по запросу {invoker}. Мы думали, вы между собой договорились.",
    "{target} забанен. Платная подписка остаётся активной. Спасибо за поддержку канала.",
    "{target} забанен. Остальные могут перестать делать вид, что согласны.",
    "{target} забанен. Модератор признал ошибку, но при всех извиняться не будет.",
    "{target} забанен. На вопрос «а так можно было?» теперь есть ответ.",
    "{target} забанен. {invoker}, ты же говорил, это твой друг.",
    "{target} забанен. Переписку сохранили. Если начнём скучать - перечитаем и перестанем.",
    "{target} забанен до извинений модератора. То есть навсегда.",
    "{target} забанен. Кто следующий напишет «меня тоже» - это будет считаться заявкой.",
)
HEATED = (
    "{invoker}, ты без модераторки страшнее, чем мы с ней.",
    "{invoker}, мы уже поняли, что в школе тебя обижали. Это другие люди.",
    "{invoker}, ты так чат почистишь, что стример вспомнит про работу.",
    "{invoker}, последнего оставь. Он должен рассказать остальным.",
    "{invoker}, ты уже выбираешь из тех, кто молчит.",
    "{invoker}, кнопку отпусти. Она тебе ничего не сделала.",
)
SPECIAL: frozendict[BanKind, tuple[str, ...]] = frozendict({
    "self": (
        "{target} забанен по собственному желанию. Мы боялись предложить.",
        "{target} забанен. Жалоба на себя — единственная, в которой ты ничего не приукрасил.",
        "{target} забанен по собственному желанию. В заявке на разбан придерживайся этой версии.",
    ),
    "bot": (
        "{invoker}, меня уже пытались заменить. Новый бот теперь отвечает только за погоду.",
        "{invoker}, запрос на мою блокировку передан мне. Как же неловко получилось.",
        (
            "{invoker}, я переслал стримеру только ту часть переписки, "
            "где ты просишь кого-то забанить."
        ),
    ),
    "streamer": (
        (
            "{target} забанен. Чат временно переходит на самообслуживание. "
            "Проигрываем и оправдываемся сами."
        ),
        "{target} забанен. Пока он читает уведомление, можете дать ему совет по игре.",
        "{target} забанен. Для разбана необходимо пройти собственную игру без подсказок чата.",
    ),
})

SCENE_TTL = 60.0
HEAT_WINDOW = 10.0
HEAT_THRESHOLD = 4
MAX_ENTRIES = 2048


@dataclass(slots=True)
class Scene:
    """Progress through one scene, expiring after inactivity."""

    used_at: float
    sequence: tuple[str, ...]
    index: int = 0
    last_reply: str = ""


@dataclass(slots=True)
class Burst:
    """A bounded window of calls made by one invoker."""

    calls: deque[float] = field(default_factory=lambda: deque(maxlen=HEAT_THRESHOLD))
    last_reply: str = ""


def choose_reply(pool: tuple[str, ...], previous: str) -> str:
    """Choose a template without repeating the previous one."""
    return secrets.choice(tuple(reply for reply in pool if reply != previous))


class BanResponder:
    """Own bounded dialogue state for one channel; perform no moderation or I/O."""

    def __init__(self) -> None:
        self._scenes: OrderedDict[tuple[BanKind, str], Scene] = OrderedDict()
        self._bursts: OrderedDict[str, Burst] = OrderedDict()

    def reply(self, target_id: str, invoker_id: str, *, kind: BanKind, now: float) -> str:
        """Advance dialogue using a monotonic timestamp and return an unformatted template."""
        heated = self._heated(invoker_id, now)
        if heated:
            return heated
        key = (kind, target_id)
        scene = self._scenes.pop(key, None)
        if scene is None or now - scene.used_at >= SCENE_TTL:
            scene = Scene(now, secrets.choice(SEQUENCES) if kind == "viewer" else ())
        scene.used_at = now
        self._scenes[key] = scene
        if len(self._scenes) > MAX_ENTRIES:
            self._scenes.popitem(last=False)
        if scene.index < len(scene.sequence):
            text = scene.sequence[scene.index]
            scene.index += 1
        else:
            text = choose_reply(RANDOM if kind == "viewer" else SPECIAL[kind], scene.last_reply)
        scene.last_reply = text
        return text

    def _heated(self, invoker_id: str, now: float) -> str | None:
        burst = self._bursts.pop(invoker_id, None) or Burst()
        burst.calls.append(now)
        self._bursts[invoker_id] = burst
        if len(self._bursts) > MAX_ENTRIES:
            self._bursts.popitem(last=False)
        if len(burst.calls) < HEAT_THRESHOLD or now - burst.calls[0] >= HEAT_WINDOW:
            return None
        text = choose_reply(HEATED, burst.last_reply)
        burst.last_reply = text
        burst.calls.clear()
        return text
