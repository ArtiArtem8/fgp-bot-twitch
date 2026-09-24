"""Load validated bot settings from the local environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
MAX_PREFIX_LENGTH = 5
CHAT_SCOPES = frozenset({"user:read:chat", "user:write:chat"})
FOLLOW_SCOPE = frozenset({"moderator:read:followers"})


class ConfigError(ValueError):
    """A configuration issue that a restart cannot fix."""


@dataclass(frozen=True, slots=True)
class Config:
    """Hold validated single-channel settings and local paths."""

    client_id: str
    client_secret: str = field(repr=False)
    bot_id: str
    channel_id: str
    root: Path = ROOT
    owner_id: str = ""
    proxy: str | None = field(default="http://127.0.0.1:12334", repr=False)
    music_token: str = field(default="", repr=False)
    prefix: str = "!"
    log_level: str = "INFO"
    log_chat: bool = True
    greet_stream: bool = True
    discord_url: str = "https://discord.gg/qKV4BCCgZ5"
    telegram_url: str = "https://t.me/yabloko18twitch"
    redirect_uri: str = "http://localhost:4343/oauth/callback"
    queue_size: int = 512
    command_cooldown: float = 3.0
    chat_retention_days: int = 0  # No automatic deletion of existing chat history.

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def database(self) -> Path:
        return self.data_dir / "tokens.db"  # Original schema/path is preserved.

    @property
    def status_file(self) -> Path:
        return self.data_dir / "status.json"


def _validate_twitch_id(name: str, value: str) -> None:
    if not value.isascii() or not value.isdecimal() or int(value) <= 0:
        raise ConfigError(f"{name}: нужен числовой Twitch ID, не имя канала")


def _validated_proxy(values: dict[str, str]) -> str | None:
    proxy = values.get("FGP_PROXY_URL", "http://127.0.0.1:12334").strip() or None
    if proxy is None:
        return None
    try:
        parsed = urlsplit(proxy)
        valid = parsed.scheme == "http" and parsed.hostname and parsed.port is not None
    except ValueError:
        valid = False
    if not valid:
        raise ConfigError(
            "FGP_PROXY_URL: нужен http://host:port; пустое значение отключает прокси"
        )
    return proxy


def _validated_redirect(values: dict[str, str]) -> str:
    redirect_uri = values.get(
        "TWITCH_REDIRECT_URI", "http://localhost:4343/oauth/callback"
    ).strip()
    try:
        callback = urlsplit(redirect_uri)
        valid = (
            callback.scheme == "http"
            and callback.hostname in {"localhost", "127.0.0.1"}
            and callback.port is not None
            and callback.path == "/oauth/callback"
            and not callback.query
            and not callback.fragment
            and not callback.username
        )
    except ValueError:
        valid = False
    if not valid:
        raise ConfigError(
            "TWITCH_REDIRECT_URI: разрешён только "
            "http://localhost:PORT/oauth/callback или 127.0.0.1"
        )
    return redirect_uri


def _required(values: dict[str, str], root: Path, key: str) -> str:
    result = values.get(key, "").strip()
    if not result or result in {"...", "CHANGE_ME"}:
        raise ConfigError(f"Не задан {key} в {root / '.env'}")
    return result


def _boolean(values: dict[str, str], key: str, *, default: bool) -> bool:
    text = values.get(key, str(default)).strip().lower()
    if text not in {"1", "0", "true", "false", "yes", "no", "on", "off"}:
        raise ConfigError(f"{key}: требуется true или false")
    return text in {"1", "true", "yes", "on"}


def _integer(values: dict[str, str], key: str, default: int, *, low: int, high: int) -> int:
    try:
        value = int(values.get(key, str(default)))
    except ValueError:
        raise ConfigError(f"{key}: требуется целое число") from None
    if not low <= value <= high:
        raise ConfigError(f"{key}: допустимо {low}..{high}")
    return value


def load_config(root: Path = ROOT, env: dict[str, str] | None = None) -> Config:
    # Deterministic .env resolution; no chdir, global load_dotenv or proxy mutation.
    """Merge local settings and validate required Twitch credentials."""
    values = {
        k: v
        for k, v in dotenv_values(root / ".env", encoding="utf-8-sig").items()
        if v is not None
    }
    values.update(os.environ if env is None else env)

    bot_id = _required(values, root, "TWITCH_BOT_ID")
    channel_id = values.get("TWITCH_CHANNEL_ID", "").strip() or _required(
        values, root, "TWITCH_OWNER_ID"
    )
    owner_id = values.get("TWITCH_OWNER_ID", channel_id).strip()
    for name, value in (("TWITCH_BOT_ID", bot_id), ("TWITCH_CHANNEL_ID", channel_id)):
        _validate_twitch_id(name, value)
    proxy = _validated_proxy(values)
    redirect_uri = _validated_redirect(values)
    prefix = values.get("FGP_PREFIX", "!").strip()
    if not prefix or len(prefix) > MAX_PREFIX_LENGTH or any(c.isspace() for c in prefix):
        raise ConfigError("FGP_PREFIX: требуется 1–5 символов без пробелов")
    level = values.get("FGP_LOG_LEVEL", "INFO").upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise ConfigError("FGP_LOG_LEVEL: DEBUG, INFO, WARNING или ERROR")
    return Config(
        client_id=_required(values, root, "TWITCH_BOT_APP_CLIENT_ID"),
        client_secret=_required(values, root, "TWITCH_BOT_APP_CLIENT_SECRET"),
        bot_id=bot_id,
        channel_id=channel_id,
        owner_id=owner_id,
        root=root.resolve(),
        proxy=proxy,
        music_token=values.get("TRULA_MUSIC_TOKEN", "").strip(),
        prefix=prefix,
        log_level=level,
        log_chat=_boolean(values, "FGP_LOG_CHAT", default=True),
        greet_stream=_boolean(values, "FGP_GREET_STREAM", default=True),
        discord_url=values.get("FGP_DISCORD_URL", "https://discord.gg/qKV4BCCgZ5"),
        telegram_url=values.get("FGP_TELEGRAM_URL", "https://t.me/yabloko18twitch"),
        redirect_uri=redirect_uri,
        queue_size=_integer(values, "FGP_QUEUE_SIZE", 512, low=16, high=4096),
        chat_retention_days=_integer(values, "FGP_CHAT_RETENTION_DAYS", 0, low=0, high=3650),
    )
