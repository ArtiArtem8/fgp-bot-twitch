"""Authorize the bot through a single-use loopback OAuth callback."""

from __future__ import annotations

import asyncio
import contextlib
import html
import secrets
import webbrowser
from typing import TYPE_CHECKING
from urllib.parse import urlencode, urlsplit

import aiohttp
from aiohttp import web

from .config import CHAT_SCOPES, FOLLOW_SCOPE
from .network import Http, ProtocolError
from .security import REDACT
from .storage import Store
from .tokens import TOKEN_URL, AuthRequiredError, Tokens
from .wire import OAuthTokens

if TYPE_CHECKING:
    from .config import Config


class Authorization:
    """Single-use, state-checked OAuth callback, bound only to loopback."""

    def __init__(
        self,
        config: Config,
        store: Store,
        http: Http,
        account: str = "bot",
        *,
        followers: bool = False,
    ) -> None:
        self.config, self.store, self.http = config, store, http
        self.expected_user = config.bot_id if account == "bot" else config.channel_id
        self.scopes = (
            CHAT_SCOPES | (FOLLOW_SCOPE if followers else frozenset())
            if account == "bot"
            else FOLLOW_SCOPE
        )
        self.state = secrets.token_urlsafe(32)
        self.used = False
        self.done = asyncio.Event()
        self.success = False
        self.result = ""

    @property
    def url(self) -> str:
        return "https://id.twitch.tv/oauth2/authorize?" + urlencode({
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "response_type": "code",
            "scope": " ".join(sorted(self.scopes)),
            "state": self.state,
            "force_verify": "true",
        })

    async def callback(self, request: web.Request) -> web.Response:
        supplied = request.query.get("state", "")
        if not secrets.compare_digest(supplied.encode("utf-8"), self.state.encode("utf-8")):
            return web.Response(
                status=400, text="OAuth state не совпадает. Откройте исходную ссылку авторизации."
            )
        if self.used:
            return web.Response(status=409, text="Этот OAuth callback уже использован.")
        self.used = True  # Before the first await, preventing simultaneous replay.
        if request.query.get("error"):
            self.result = "Пользователь отказал в доступе"
            self.done.set()
            return web.Response(status=403, text=self.result)
        code = request.query.get("code", "")
        if not code:
            self.result = "В OAuth callback отсутствует code"
            self.done.set()
            return web.Response(status=400, text=self.result)
        REDACT.add(code)
        try:
            login = await self._exchange(code)
            self.success = True
            self.result = (
                f"Авторизован {login} (ID {self.expected_user}). Токены сохранены локально."
            )
        except Exception as exc:  # ruff: ignore[blind-except] - redact callback failures
            self.result = REDACT(exc)
        self.done.set()
        return web.Response(
            status=200 if self.success else 400,
            content_type="text/html",
            text=(
                "<!doctype html><meta charset='utf-8'><title>FGPbot OAuth</title>"
                f"<h1>{'Готово' if self.success else 'Авторизация не завершена'}</h1>"
                f"<p>{html.escape(self.result)}</p>"
                "<p>Можно закрыть эту вкладку. При ошибке запустите auth ещё раз.</p>"
            ),
            headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
        )

    async def _exchange(self, code: str) -> str:
        response = await self.http.request(
            "POST",
            TOKEN_URL,
            data={
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": self.config.redirect_uri,
            },
            model=OAuthTokens,
        )
        access, refresh = response.access_token, response.refresh_token
        if not access or not refresh:
            raise ProtocolError("Twitch не вернул пару токенов")
        REDACT.add(access, refresh)
        identity = await Tokens(self.config, self.store, self.http).validate(
            access, self.expected_user
        )
        missing = self.scopes - frozenset(identity.scopes)
        if missing:
            message = "Не выданы запрошенные scopes: " + ", ".join(sorted(missing))
            raise AuthRequiredError(message)
        await self.store.save_token(self.expected_user, access, refresh)
        return identity.login


async def authorize(config: Config, account: str, *, followers: bool, open_browser: bool) -> bool:
    """Complete bot authorization after validating callback state and identity."""
    store = Store(config.database)
    await store.initialize()
    async with aiohttp.ClientSession(trust_env=False) as session:
        flow = Authorization(
            config, store, Http(session, config.proxy), account, followers=followers
        )
        app = web.Application()
        app.router.add_get("/oauth/callback", flow.callback)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        try:
            port = urlsplit(config.redirect_uri).port
            await web.TCPSite(runner, "127.0.0.1", port).start()
            with contextlib.suppress(OSError):
                await web.TCPSite(runner, "::1", port).start()
            print(
                "В Twitch войди в "
                f"{'аккаунт БОТА' if account == 'bot' else 'аккаунт СТРИМЕРА'}: "
                f"ожидается ID {flow.expected_user}."
            )
            print("Redirect URL в Twitch Developer Console должен точно совпадать с:")
            print(config.redirect_uri)
            print("Ссылка для входа (работает только пока эта команда запущена):")
            print(flow.url)
            if open_browser:
                await asyncio.to_thread(webbrowser.open, flow.url)
            try:
                await asyncio.wait_for(flow.done.wait(), timeout=300)
            except TimeoutError:
                print("Авторизация не завершена за 5 минут. Запусти auth снова.")
                return False
            print(flow.result)
            # Give aiohttp a chance to flush the callback response before cleanup.
            await asyncio.sleep(0.25)
            return flow.success
        finally:
            await runner.cleanup()
