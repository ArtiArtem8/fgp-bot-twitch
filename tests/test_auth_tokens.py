from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fgpbot.auth import Authorization
from fgpbot.config import CHAT_SCOPES, FOLLOW_SCOPE
from fgpbot.network import NetworkError, ProtocolError, RemoteError
from fgpbot.tokens import AuthRequired, TOKEN_URL, VALIDATE, Tokens
from tests.helpers import StoreCase, identity


class TokenTests(StoreCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.http = SimpleNamespace(request=AsyncMock())
        self.tokens = Tokens(self.config, self.store, self.http)
        await self.store.save_token("100", "synthetic-old-access", "synthetic-old-refresh")

    async def test_valid_cached_token_only_validated_once_and_not_refreshed(self):
        self.http.request.return_value = identity()
        results = await asyncio.gather(*(self.tokens.get("100",CHAT_SCOPES) for _ in range(8)))
        self.assertTrue(all(result.access=="synthetic-old-access" for result in results))
        self.http.request.assert_awaited_once()
        self.assertEqual(self.http.request.call_args.args[:2],("GET",VALIDATE))

    async def test_refresh_saves_new_pair_not_original_input(self):
        self.http.request.side_effect = [RemoteError(401,"expired"),
            {"access_token":"synthetic-new-access", "refresh_token":"synthetic-new-refresh"}, identity()]
        result = await self.tokens.get("100", CHAT_SCOPES)
        self.assertEqual((result.access,result.refresh),("synthetic-new-access","synthetic-new-refresh"))
        row = await self.store.token("100")
        self.assertEqual((row["token"],row["refresh"]),(result.access,result.refresh))
        call = self.http.request.call_args_list[1]
        self.assertEqual(call.args,("POST", TOKEN_URL))
        self.assertEqual(call.kwargs["data"]["refresh_token"],"synthetic-old-refresh")
        self.assertNotIn("?",call.args[1])

    async def test_parallel_expired_token_requests_refresh_once(self):
        self.http.request.side_effect = [RemoteError(401,"expired"),
            {"access_token":"synthetic-new-access", "refresh_token":"synthetic-new-refresh"}, identity()]
        tokens = await asyncio.gather(*(self.tokens.get("100",CHAT_SCOPES) for _ in range(10)))
        self.assertTrue(all(t.access=="synthetic-new-access" for t in tokens))
        self.assertEqual(self.http.request.await_count,3)

    async def test_rotation_persists_even_if_next_validation_has_network_error(self):
        self.http.request.side_effect = [RemoteError(401,"expired"),
            {"access_token":"synthetic-new-access", "refresh_token":"synthetic-new-refresh"}, NetworkError("lost")]
        with self.assertRaises(NetworkError):
            await self.tokens.get("100")
        row = await self.store.token("100")
        self.assertEqual(row["refresh"],"synthetic-new-refresh")
        self.http.request.side_effect = None
        self.http.request.return_value = identity()
        restarted=Tokens(self.config, self.store, self.http)
        self.assertEqual((await restarted.get("100")).access,"synthetic-new-access")

    async def test_background_near_expiry_refresh_is_persisted(self):
        self.http.request.side_effect = [identity(expires_in=240),
            {"access_token":"synthetic-new-access", "refresh_token":"synthetic-new-refresh"}, identity()]
        await self.tokens.get("100")
        self.assertEqual((await self.store.token("100"))["refresh"],"synthetic-new-refresh")

    async def test_rejected_refresh_is_not_hammered_until_authorization_changes(self):
        self.http.request.side_effect = [RemoteError(401,"expired"), RemoteError(400,"invalid refresh")]
        for _ in range(4):
            with self.assertRaises(AuthRequired):
                await self.tokens.get("100")
        self.assertEqual(self.http.request.await_count,2)
        await self.store.save_token("100","synthetic-manual-access","synthetic-manual-refresh")
        self.http.request.side_effect = None
        self.http.request.return_value = identity()
        self.assertEqual((await self.tokens.get("100")).access,"synthetic-manual-access")

    async def test_refresh_does_not_clobber_simultaneous_explicit_authorization(self):
        async def respond(method,url,**kwargs):
            if method=="POST":
                await self.store.save_token("100","synthetic-manual-access","synthetic-manual-refresh")
                return {"access_token":"synthetic-rotation-access","refresh_token":"synthetic-rotation-refresh"}
            if kwargs["headers"]["Authorization"].endswith("synthetic-old-access"):
                raise RemoteError(401,"expired")
            return identity()
        self.http.request.side_effect = respond
        result = await self.tokens.get("100")
        self.assertEqual(result.access,"synthetic-manual-access")
        self.assertEqual((await self.store.token("100"))["refresh"],"synthetic-manual-refresh")

    async def test_wrong_account_or_application_is_rejected(self):
        for bad in (identity(user_id="999"), identity(client_id="other-client")):
            self.http.request.return_value=bad
            with self.assertRaises(AuthRequired):
                await self.tokens.get("100")
        self.assertTrue(all(call.args[0]=="GET" for call in self.http.request.call_args_list))

    async def test_missing_scopes_do_not_silently_allow_chat(self):
        self.http.request.return_value=identity(scopes=["user:read:chat"])
        with self.assertRaisesRegex(AuthRequired,"user:write:chat"):
            await self.tokens.get("100", CHAT_SCOPES)

    async def test_malformed_rotation_not_persisted(self):
        self.http.request.side_effect=[RemoteError(401,"expired"),{"access_token":123,"refresh_token":{}}]
        with self.assertRaises(ProtocolError):
            await self.tokens.get("100")
        self.assertEqual((await self.store.token("100"))["token"],"synthetic-old-access")

    async def test_hourly_requirement_has_margin(self):
        self.http.request.return_value=identity()
        current=await self.tokens.get("100")
        current.validated_at-=3001
        await self.tokens.get("100")
        self.assertEqual(self.http.request.await_count,2)

    async def test_missing_owner_token_not_needed_for_bot_token(self):
        self.http.request.return_value=identity()
        self.assertIsNone(await self.store.token("200"))
        self.assertEqual((await self.tokens.get("100", CHAT_SCOPES)).user_id,"100")
        with self.assertRaises(AuthRequired):
            await self.tokens.get("200", FOLLOW_SCOPE)


class AuthorizationTests(StoreCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.http=SimpleNamespace(request=AsyncMock(side_effect=[
            {"access_token":"synthetic-authorized-access", "refresh_token":"synthetic-authorized-refresh"},
            identity()]))
        self.flow=Authorization(self.config,self.store,self.http)

    def request(self, **query):
        return SimpleNamespace(query=query)

    async def test_valid_state_and_identity_save_credentials(self):
        result=await self.flow.callback(self.request(state=self.flow.state,code="synthetic-one-use-code"))
        self.assertEqual(result.status,200)
        self.assertTrue(self.flow.success)
        self.assertTrue(self.flow.done.is_set())
        self.assertEqual((await self.store.token("100"))["token"],"synthetic-authorized-access")
        call=self.http.request.call_args_list[0]
        self.assertEqual(call.kwargs["data"]["grant_type"],"authorization_code")
        self.assertNotIn("synthetic-one-use-code",result.text)

    async def test_invalid_or_non_ascii_state_does_not_exchange(self):
        for state in ("", "wrong", "случайное"):
            response=await self.flow.callback(self.request(state=state,code="synthetic-code"))
            self.assertEqual(response.status,400)
        self.http.request.assert_not_awaited()
        self.assertFalse(self.flow.used)

    async def test_replayed_callback_is_rejected(self):
        request=self.request(state=self.flow.state,code="synthetic-one-use-code")
        responses=await asyncio.gather(self.flow.callback(request),self.flow.callback(request))
        self.assertEqual(sorted(response.status for response in responses),[200,409])
        self.assertEqual(self.http.request.await_count,2) # One exchange + one validation.

    async def test_wrong_account_cannot_overwrite_existing_bot_token(self):
        await self.store.save_token("100","existing-access","existing-refresh")
        self.http.request.side_effect=[{"access_token":"synthetic-new-access","refresh_token":"synthetic-new-refresh"},
                                       identity(user_id="999")]
        response=await self.flow.callback(self.request(state=self.flow.state,code="synthetic-code"))
        self.assertEqual(response.status,400)
        self.assertFalse(self.flow.success)
        self.assertEqual((await self.store.token("100"))["token"],"existing-access")

    async def test_denied_consent_does_not_exchange_token(self):
        response=await self.flow.callback(self.request(state=self.flow.state,error="access_denied"))
        self.assertEqual(response.status,403)
        self.assertTrue(self.flow.done.is_set())
        self.http.request.assert_not_awaited()

    async def test_granted_scopes_must_match_request(self):
        self.http.request.side_effect=[{"access_token":"synthetic-new-access","refresh_token":"synthetic-new-refresh"},
                                       identity(scopes=[])]
        response=await self.flow.callback(self.request(state=self.flow.state,code="synthetic-code"))
        self.assertEqual(response.status,400)
        self.assertIsNone(await self.store.token("100"))

    async def test_scope_roles_are_explicit(self):
        self.assertEqual(self.flow.scopes, CHAT_SCOPES)
        mod=Authorization(self.config,self.store,self.http,followers=True)
        self.assertEqual(mod.scopes,CHAT_SCOPES|FOLLOW_SCOPE)
        broadcaster=Authorization(self.config,self.store,self.http,account="broadcaster")
        self.assertEqual(broadcaster.expected_user,"200")
        self.assertEqual(broadcaster.scopes,FOLLOW_SCOPE)
