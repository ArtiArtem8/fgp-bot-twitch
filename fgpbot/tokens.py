from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from .config import Config
from .network import Http, ProtocolError, RemoteError
from .security import REDACT
from .storage import Store

LOG = logging.getLogger(__name__)
VALIDATE = "https://id.twitch.tv/oauth2/validate"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"


class AuthRequired(Exception):
    """Permanent until the local user reauthorizes or repairs the configuration."""


@dataclass(slots=True)
class Token:
    user_id: str
    login: str
    scopes: frozenset[str]
    access: str = field(repr=False)
    refresh: str = field(repr=False)
    validated_at: float
    expires_at: float


class Tokens:
    def __init__(self, config: Config, store: Store, http: Http) -> None:
        self.config, self.store, self.http = config, store, http
        self._locks: dict[str, asyncio.Lock] = {}
        self._cache: dict[str, Token] = {}
        self._blocked: dict[str, tuple[tuple[str, str], str]] = {}

    async def validate(self, access: str, expected_user: str) -> dict:
        REDACT.add(access)
        data = await self.http.request("GET", VALIDATE, headers={"Authorization": f"OAuth {access}"})
        if (not isinstance(data, dict) or not isinstance(data.get("scopes"), list)
                or not all(isinstance(scope, str) for scope in data["scopes"])):
            raise ProtocolError("Неполный ответ OAuth validate")
        if data.get("client_id") != self.config.client_id:
            raise AuthRequired("Токен выпущен для другого Client ID; нужна повторная авторизация")
        if data.get("user_id") != expected_user:
            raise AuthRequired(f"Токен не принадлежит ожидаемому Twitch ID {expected_user}")
        if not isinstance(data.get("expires_in"), int) or data["expires_in"] < 0:
            raise ProtocolError("OAuth validate не вернул корректный expires_in")
        return data

    @staticmethod
    def require_scopes(token: Token, scopes: frozenset[str]) -> Token:
        missing = scopes - token.scopes
        if missing:
            raise AuthRequired("Не хватает OAuth scopes: " + ", ".join(sorted(missing)) + "; запусти auth снова")
        return token

    def invalidate(self, user_id: str, access: str | None = None) -> None:
        cached = self._cache.get(user_id)
        if cached and (access is None or cached.access == access):
            self._cache.pop(user_id, None)

    async def get(self, user_id: str, scopes: frozenset[str] = frozenset()) -> Token:
        async with self._locks.setdefault(user_id, asyncio.Lock()):
            for _ in range(3):
                row = await self.store.token(user_id)
                if not row:
                    raise AuthRequired(f"Нет токена для Twitch ID {user_id}. Запусти: python main.py auth")
                pair = (row["token"], row["refresh"])
                REDACT.add(*pair)
                blocked = self._blocked.get(user_id)
                if blocked and blocked[0] == pair:
                    raise AuthRequired(blocked[1])
                cached = self._cache.get(user_id)
                now = time.monotonic()
                if cached and (cached.access, cached.refresh) == pair:
                    if now - cached.validated_at < 3000 and cached.expires_at - now > 300:
                        return self.require_scopes(cached, scopes)
                try:
                    data = await self.validate(pair[0], user_id)
                except RemoteError as exc:
                    if exc.status != 401:
                        raise
                    data = None
                if data is not None and data["expires_in"] > 300:
                    token = Token(user_id, data.get("login", ""), frozenset(data["scopes"]),
                                  *pair, now, now + data["expires_in"])
                    self._cache[user_id] = token
                    self._blocked.pop(user_id, None)
                    return self.require_scopes(token, scopes)
                try:
                    # OAuth credentials are in the POST body, never the URL.
                    refreshed = await self.http.request("POST", TOKEN_URL, data={
                        "client_id": self.config.client_id, "client_secret": self.config.client_secret,
                        "grant_type": "refresh_token", "refresh_token": pair[1],
                    })
                except RemoteError as exc:
                    if exc.status not in {400, 401, 403}:
                        raise
                    latest = await self.store.token(user_id)
                    if latest and (latest["token"], latest["refresh"]) != pair:
                        continue  # A simultaneous explicit auth won the race.
                    reason = f"Авторизация Twitch ID {user_id} не восстанавливается; проверь Client Secret и запусти auth"
                    self._blocked[user_id] = (pair, reason)
                    raise AuthRequired(reason) from None
                if (not isinstance(refreshed, dict)
                        or not all(isinstance(refreshed.get(k), str) and refreshed[k]
                                   for k in ("access_token", "refresh_token"))):
                    raise ProtocolError("OAuth refresh не вернул пару access_token/refresh_token")
                new = (refreshed["access_token"], refreshed["refresh_token"])
                REDACT.add(*new)
                # Persist rotation BEFORE another network request. The new pair will
                # still exist if validation times out or the computer reboots now.
                saved = await self.store.save_token(user_id, *new, expected=pair)
                self._cache.pop(user_id, None)
                if saved:
                    LOG.info("OAuth обновлён и сохранён: user_id=%s", user_id)
                # Reload and validate the actual stored pair; never return an unchecked token.
            raise AuthRequired(f"Не удалось стабилизировать токен {user_id}; повтори авторизацию")
