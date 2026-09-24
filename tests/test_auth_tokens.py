import asyncio
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock

import msgspec

from fgpbot.auth import Authorization
from fgpbot.config import CHAT_SCOPES, FOLLOW_SCOPE
from fgpbot.network import NetworkError, ProtocolError, RemoteError
from fgpbot.tokens import TOKEN_URL, VALIDATE, AuthRequiredError, Tokens
from fgpbot.wire import OAuthTokens, OAuthValidate
from tests.helpers import StoreCase
from tests.helpers import identity as raw_identity

if TYPE_CHECKING:
    from aiohttp import web

    from fgpbot.network import Http


def identity(user_id: str = "100", **kwargs: object) -> OAuthValidate:
    return msgspec.convert(raw_identity(user_id, **kwargs), type=OAuthValidate)


class TokenTests(StoreCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.http = SimpleNamespace(request=AsyncMock())
        self.tokens = Tokens(self.config, self.store, cast("Http", self.http))
        await self.store.save_token("100", "synthetic-old-access", "synthetic-old-refresh")

    async def test_valid_cached_token_only_validated_once_and_not_refreshed(self) -> None:
        self.http.request.return_value = identity()
        results = await asyncio.gather(*(self.tokens.get("100", CHAT_SCOPES) for _ in range(8)))
        self.assertTrue(all(result.access == "synthetic-old-access" for result in results))
        self.http.request.assert_awaited_once()
        self.assertEqual(self.http.request.call_args.args[:2], ("GET", VALIDATE))

    async def test_refresh_saves_new_pair_not_original_input(self) -> None:
        self.http.request.side_effect = [
            RemoteError(401, "expired"),
            OAuthTokens("synthetic-new-access", "synthetic-new-refresh"),
            identity(),
        ]
        result = await self.tokens.get("100", CHAT_SCOPES)
        self.assertEqual(
            (result.access, result.refresh), ("synthetic-new-access", "synthetic-new-refresh")
        )
        row = await self.token_row("100")
        self.assertEqual((row["token"], row["refresh"]), (result.access, result.refresh))
        call = self.http.request.call_args_list[1]
        self.assertEqual(call.args, ("POST", TOKEN_URL))
        self.assertEqual(call.kwargs["data"]["refresh_token"], "synthetic-old-refresh")
        self.assertNotIn("?", call.args[1])

    async def test_parallel_expired_token_requests_refresh_once(self) -> None:
        self.http.request.side_effect = [
            RemoteError(401, "expired"),
            OAuthTokens("synthetic-new-access", "synthetic-new-refresh"),
            identity(),
        ]
        tokens = await asyncio.gather(*(self.tokens.get("100", CHAT_SCOPES) for _ in range(10)))
        self.assertTrue(all(t.access == "synthetic-new-access" for t in tokens))
        self.assertEqual(self.http.request.await_count, 3)

    async def test_rotation_persists_even_if_next_validation_has_network_error(self) -> None:
        self.http.request.side_effect = [
            RemoteError(401, "expired"),
            OAuthTokens("synthetic-new-access", "synthetic-new-refresh"),
            NetworkError("lost"),
        ]
        with self.assertRaises(NetworkError):
            await self.tokens.get("100")
        row = await self.token_row("100")
        self.assertEqual(row["refresh"], "synthetic-new-refresh")
        self.http.request.side_effect = None
        self.http.request.return_value = identity()
        restarted = Tokens(self.config, self.store, cast("Http", self.http))
        self.assertEqual((await restarted.get("100")).access, "synthetic-new-access")

    async def test_background_near_expiry_refresh_is_persisted(self) -> None:
        self.http.request.side_effect = [
            identity(expires_in=240),
            OAuthTokens("synthetic-new-access", "synthetic-new-refresh"),
            identity(),
        ]
        await self.tokens.get("100")
        self.assertEqual((await self.token_row("100"))["refresh"], "synthetic-new-refresh")

    async def test_rejected_refresh_is_not_hammered_until_authorization_changes(self) -> None:
        self.http.request.side_effect = [
            RemoteError(401, "expired"),
            RemoteError(400, "invalid refresh"),
        ]
        for _ in range(4):
            with self.assertRaises(AuthRequiredError):
                await self.tokens.get("100")
        self.assertEqual(self.http.request.await_count, 2)
        await self.store.save_token("100", "synthetic-manual-access", "synthetic-manual-refresh")
        self.http.request.side_effect = None
        self.http.request.return_value = identity()
        self.assertEqual((await self.tokens.get("100")).access, "synthetic-manual-access")

    async def test_refresh_does_not_clobber_simultaneous_explicit_authorization(self) -> None:
        async def respond(
            method: str, _url: str, **kwargs: dict[str, str]
        ) -> OAuthTokens | OAuthValidate:
            if method == "POST":
                await self.store.save_token(
                    "100", "synthetic-manual-access", "synthetic-manual-refresh"
                )
                return OAuthTokens("synthetic-rotation-access", "synthetic-rotation-refresh")
            if kwargs["headers"]["Authorization"].endswith("synthetic-old-access"):
                raise RemoteError(401, "expired")
            return identity()

        self.http.request.side_effect = respond
        result = await self.tokens.get("100")
        self.assertEqual(result.access, "synthetic-manual-access")
        self.assertEqual((await self.token_row("100"))["refresh"], "synthetic-manual-refresh")

    async def test_wrong_account_or_application_is_rejected(self) -> None:
        for bad in (identity(user_id="999"), identity(client_id="other-client")):
            self.http.request.return_value = bad
            with self.assertRaises(AuthRequiredError):
                await self.tokens.get("100")
        self.assertTrue(all(call.args[0] == "GET" for call in self.http.request.call_args_list))

    async def test_missing_scopes_do_not_silently_allow_chat(self) -> None:
        self.http.request.return_value = identity(scopes=["user:read:chat"])
        with self.assertRaisesRegex(AuthRequiredError, "user:write:chat"):
            await self.tokens.get("100", CHAT_SCOPES)

    async def test_malformed_rotation_not_persisted(self) -> None:
        self.http.request.side_effect = [
            RemoteError(401, "expired"),
            ProtocolError("OAuth refresh: неверный тип поля"),
        ]
        with self.assertRaises(ProtocolError):
            await self.tokens.get("100")
        self.assertEqual((await self.token_row("100"))["token"], "synthetic-old-access")

    async def test_hourly_requirement_has_margin(self) -> None:
        self.http.request.return_value = identity()
        current = await self.tokens.get("100")
        current.validated_at -= 3001
        await self.tokens.get("100")
        self.assertEqual(self.http.request.await_count, 2)

    async def test_missing_owner_token_not_needed_for_bot_token(self) -> None:
        self.http.request.return_value = identity()
        self.assertIsNone(await self.store.token("200"))
        self.assertEqual((await self.tokens.get("100", CHAT_SCOPES)).user_id, "100")
        with self.assertRaises(AuthRequiredError):
            await self.tokens.get("200", FOLLOW_SCOPE)

    async def test_corrupt_stored_token_is_rejected_without_network_request(self) -> None:
        await self.store.call(
            lambda db: db.execute("UPDATE tokens SET token=x'FF' WHERE user_id='100'")
        )
        with self.assertRaises(AuthRequiredError):
            await self.tokens.get("100")
        self.http.request.assert_not_awaited()


class AuthorizationTests(StoreCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.http = SimpleNamespace(
            request=AsyncMock(
                side_effect=[
                    OAuthTokens("synthetic-authorized-access", "synthetic-authorized-refresh"),
                    identity(),
                ]
            )
        )
        self.flow = Authorization(self.config, self.store, cast("Http", self.http))

    @staticmethod
    def request(**query: str) -> web.Request:
        return cast("web.Request", SimpleNamespace(query=query))

    async def test_valid_state_and_identity_save_credentials(self) -> None:
        result = await self.flow.callback(
            self.request(state=self.flow.state, code="synthetic-one-use-code")
        )
        self.assertEqual(result.status, 200)
        self.assertTrue(self.flow.success)
        self.assertTrue(self.flow.done.is_set())
        self.assertEqual((await self.token_row("100"))["token"], "synthetic-authorized-access")
        call = self.http.request.call_args_list[0]
        self.assertEqual(call.kwargs["data"]["grant_type"], "authorization_code")
        self.assertIsNotNone(result.text)
        self.assertNotIn("synthetic-one-use-code", result.text or "")

    async def test_invalid_or_non_ascii_state_does_not_exchange(self) -> None:
        for state in ("", "wrong", "случайное"):
            response = await self.flow.callback(self.request(state=state, code="synthetic-code"))
            self.assertEqual(response.status, 400)
        self.http.request.assert_not_awaited()
        self.assertFalse(self.flow.used)

    async def test_replayed_callback_is_rejected(self) -> None:
        request = self.request(state=self.flow.state, code="synthetic-one-use-code")
        responses = await asyncio.gather(self.flow.callback(request), self.flow.callback(request))
        self.assertEqual(sorted(response.status for response in responses), [200, 409])
        self.assertEqual(self.http.request.await_count, 2)  # One exchange + one validation.

    async def test_wrong_account_cannot_overwrite_existing_bot_token(self) -> None:
        await self.store.save_token("100", "existing-access", "existing-refresh")
        self.http.request.side_effect = [
            OAuthTokens("synthetic-new-access", "synthetic-new-refresh"),
            identity(user_id="999"),
        ]
        response = await self.flow.callback(
            self.request(state=self.flow.state, code="synthetic-code")
        )
        self.assertEqual(response.status, 400)
        self.assertFalse(self.flow.success)
        self.assertEqual((await self.token_row("100"))["token"], "existing-access")

    async def test_denied_consent_does_not_exchange_token(self) -> None:
        response = await self.flow.callback(
            self.request(state=self.flow.state, error="access_denied")
        )
        self.assertEqual(response.status, 403)
        self.assertTrue(self.flow.done.is_set())
        self.http.request.assert_not_awaited()

    async def test_granted_scopes_must_match_request(self) -> None:
        self.http.request.side_effect = [
            OAuthTokens("synthetic-new-access", "synthetic-new-refresh"),
            identity(scopes=[]),
        ]
        response = await self.flow.callback(
            self.request(state=self.flow.state, code="synthetic-code")
        )
        self.assertEqual(response.status, 400)
        self.assertIsNone(await self.store.token("100"))

    async def test_scope_roles_are_explicit(self) -> None:
        self.assertEqual(self.flow.scopes, CHAT_SCOPES)
        mod = Authorization(self.config, self.store, cast("Http", self.http), followers=True)
        self.assertEqual(mod.scopes, CHAT_SCOPES | FOLLOW_SCOPE)
        broadcaster = Authorization(
            self.config, self.store, cast("Http", self.http), account="broadcaster"
        )
        self.assertEqual(broadcaster.expected_user, "200")
        self.assertEqual(broadcaster.scopes, FOLLOW_SCOPE)
