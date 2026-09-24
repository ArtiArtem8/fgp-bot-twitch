"""Validate and refresh OAuth tokens without losing rotations."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING

from .network import ProtocolError, RemoteError
from .security import REDACT
from .wire import OAuthTokens, OAuthValidate

if TYPE_CHECKING:
    from .config import Config
    from .network import Http
    from .storage import Store

LOG = logging.getLogger(__name__)
VALIDATE = "https://id.twitch.tv/oauth2/validate"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"  # ruff: ignore[hardcoded-password-string] - endpoint, not a token
VALIDATION_CACHE_SECONDS = 3000
REFRESH_MARGIN_SECONDS = 300


class AuthRequiredError(Exception):
    """Permanent until the local user reauthorizes or repairs the configuration."""


@dataclass(slots=True)
class Token:
    """Carry one authorized access and refresh token pair."""

    user_id: str
    login: str
    scopes: frozenset[str]
    access: str = field(repr=False)
    refresh: str = field(repr=False)
    validated_at: float
    expires_at: float


class Tokens:
    """Coordinate validation, refresh, and atomic token persistence."""

    def __init__(self, config: Config, store: Store, http: Http) -> None:
        self.config, self.store, self.http = config, store, http
        self._locks: dict[str, asyncio.Lock] = {}
        self._cache: dict[str, Token] = {}
        self._blocked: dict[str, tuple[tuple[str, str], str]] = {}

    async def validate(self, access: str, expected_user: str) -> OAuthValidate:
        REDACT.add(access)
        data = await self.http.request(
            "GET", VALIDATE, headers={"Authorization": f"OAuth {access}"}, model=OAuthValidate
        )
        if data.client_id != self.config.client_id:
            raise AuthRequiredError(
                "Токен выпущен для другого Client ID; нужна повторная авторизация"
            )
        if data.user_id != expected_user:
            raise AuthRequiredError(f"Токен не принадлежит ожидаемому Twitch ID {expected_user}")
        if data.expires_in < 0:
            raise ProtocolError("OAuth validate не вернул корректный expires_in")
        return data

    @staticmethod
    def require_scopes(token: Token, scopes: frozenset[str]) -> Token:
        missing = scopes - token.scopes
        if missing:
            raise AuthRequiredError(
                "Не хватает OAuth scopes: " + ", ".join(sorted(missing)) + "; запусти auth снова"
            )
        return token

    def invalidate(self, user_id: str, access: str | None = None) -> None:
        cached = self._cache.get(user_id)
        if cached and (access is None or cached.access == access):
            self._cache.pop(user_id, None)

    async def _current_pair(self, user_id: str) -> tuple[str, str]:
        row = await self.store.token(user_id)
        if not row:
            raise AuthRequiredError(
                f"Нет токена для Twitch ID {user_id}. Запусти: python main.py auth"
            )
        access, refresh = row["token"], row["refresh"]
        if not isinstance(access, str) or not isinstance(refresh, str):
            raise AuthRequiredError("Сохранённые Twitch токены повреждены; запусти auth")
        pair = (access, refresh)
        REDACT.add(*pair)
        blocked = self._blocked.get(user_id)
        if blocked and blocked[0] == pair:
            raise AuthRequiredError(blocked[1])
        return pair

    def _cached_token(
        self, user_id: str, pair: tuple[str, str], now: float, scopes: frozenset[str]
    ) -> Token | None:
        cached = self._cache.get(user_id)
        if (
            cached
            and (cached.access, cached.refresh) == pair
            and (
                now - cached.validated_at < VALIDATION_CACHE_SECONDS
                and cached.expires_at - now > REFRESH_MARGIN_SECONDS
            )
        ):
            return self.require_scopes(cached, scopes)
        return None

    async def _validated_token(
        self, user_id: str, pair: tuple[str, str], now: float, scopes: frozenset[str]
    ) -> Token | None:
        try:
            data = await self.validate(pair[0], user_id)
        except RemoteError as exc:
            if exc.status != HTTPStatus.UNAUTHORIZED:
                raise
            return None
        if data.expires_in <= REFRESH_MARGIN_SECONDS:
            return None
        token = Token(
            user_id,
            data.login,
            frozenset(data.scopes),
            *pair,
            now,
            now + data.expires_in,
        )
        self._cache[user_id] = token
        self._blocked.pop(user_id, None)
        return self.require_scopes(token, scopes)

    async def _refresh_pair(self, user_id: str, pair: tuple[str, str]) -> None:
        try:
            # OAuth credentials are in the POST body, never the URL.
            refreshed = await self.http.request(
                "POST",
                TOKEN_URL,
                data={
                    "client_id": self.config.client_id,
                    "client_secret": self.config.client_secret,
                    "grant_type": "refresh_token",
                    "refresh_token": pair[1],
                },
                model=OAuthTokens,
            )
        except RemoteError as exc:
            if exc.status not in {400, 401, 403}:
                raise
            latest = await self.store.token(user_id)
            if latest and (latest["token"], latest["refresh"]) != pair:
                return  # A simultaneous explicit auth won the race.
            reason = (
                f"Авторизация Twitch ID {user_id} не восстанавливается; "
                "проверь Client Secret и запусти auth"
            )
            self._blocked[user_id] = (pair, reason)
            raise AuthRequiredError(reason) from None
        if not refreshed.access_token or not refreshed.refresh_token:
            raise ProtocolError("OAuth refresh не вернул пару access_token/refresh_token")
        new = (refreshed.access_token, refreshed.refresh_token)
        REDACT.add(*new)
        # Persist rotation before another network request or process shutdown.
        saved = await self.store.save_token(user_id, *new, expected=pair)
        self._cache.pop(user_id, None)
        if saved:
            LOG.info("OAuth обновлён и сохранён: user_id=%s", user_id)

    async def get(self, user_id: str, scopes: frozenset[str] = frozenset()) -> Token:
        async with self._locks.setdefault(user_id, asyncio.Lock()):
            for _ in range(3):
                pair = await self._current_pair(user_id)
                now = time.monotonic()
                cached = self._cached_token(user_id, pair, now, scopes)
                if cached is not None:
                    return cached
                validated = await self._validated_token(user_id, pair, now, scopes)
                if validated is not None:
                    return validated
                await self._refresh_pair(user_id, pair)
            raise AuthRequiredError(
                f"Не удалось стабилизировать токен {user_id}; повтори авторизацию"
            )
