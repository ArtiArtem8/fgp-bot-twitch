"""Verify failures at the Twitch JSON boundary, including future fields."""

from __future__ import annotations

import unittest

import msgspec

from fgpbot.network import ProtocolError
from fgpbot.wire import (
    ChatEvent,
    NotificationPayload,
    OAuthTokens,
    OAuthValidate,
    SendChat,
    Subscriptions,
    decode,
    eventsub,
)
from tests.helpers import identity, notification, subscription


class WireTests(unittest.TestCase):
    def test_valid_chat_notification_has_typed_fields(self) -> None:
        frame = eventsub(msgspec.json.encode(notification()))
        assert isinstance(frame.payload, NotificationPayload)
        assert isinstance(frame.payload.event, ChatEvent)
        self.assertEqual(frame.payload.event.message.text, "!ping")

    def test_missing_required_event_field_is_protocol_error(self) -> None:
        raw = notification()
        del raw["payload"]["event"]["message"]
        with self.assertRaises(ProtocolError):
            eventsub(msgspec.json.encode(raw))

    def test_wrong_event_type_is_protocol_error(self) -> None:
        raw = notification()
        raw["payload"]["event"]["message"] = {"text": 123}
        with self.assertRaises(ProtocolError):
            eventsub(msgspec.json.encode(raw))

    def test_malformed_json_is_protocol_error_without_body(self) -> None:
        private_value = "synthetic-private-value"
        with self.assertRaises(ProtocolError) as caught:
            eventsub('{"metadata": "' + private_value)
        self.assertNotIn(private_value, str(caught.exception))

    def test_unknown_fields_are_ignored(self) -> None:
        raw = notification()
        raw["future_envelope_field"] = {"added": True}
        raw["metadata"]["future_metadata_field"] = 42
        raw["payload"]["event"]["future_event_field"] = "new"
        frame = eventsub(msgspec.json.encode(raw))
        assert isinstance(frame.payload, NotificationPayload)
        assert isinstance(frame.payload.event, ChatEvent)
        self.assertEqual(frame.payload.event.message.text, "!ping")

    def test_empty_notification_payload_is_protocol_error(self) -> None:
        with self.assertRaises(ProtocolError):
            eventsub(b'{"metadata":{"message_type":"notification"},"payload":{}}')

    def test_send_chat_requires_boolean_is_sent(self) -> None:
        with self.assertRaises(ProtocolError):
            decode(b'{"data":[{"is_sent":"true","message_id":"m"}]}', SendChat, "chat")

    def test_send_chat_rejects_wrong_message_id_type(self) -> None:
        with self.assertRaises(ProtocolError):
            decode(b'{"data":[{"is_sent":true,"message_id":23}]}', SendChat, "chat")

    def test_subscription_requires_array_and_nested_structure(self) -> None:
        for raw in (b'{"data":{}}', b'{"data":[{"id":"a"}]}'):
            with self.subTest(raw=raw), self.assertRaises(ProtocolError):
                decode(raw, Subscriptions, "subscriptions")
        good = decode(msgspec.json.encode({"data": [subscription()]}), Subscriptions, "subs")
        self.assertEqual(good.data[0].transport.session_id, "session-1")

    def test_oauth_requires_used_fields_and_correct_types(self) -> None:
        for raw in (
            b'{"client_id":"c","user_id":"u","expires_in":20}',
            b'{"client_id":"c","user_id":"u","scopes":[],"expires_in":"20"}',
        ):
            with self.subTest(raw=raw), self.assertRaises(ProtocolError):
                decode(raw, OAuthValidate, "OAuth")
        value = decode(msgspec.json.encode(identity()), OAuthValidate, "OAuth")
        self.assertEqual(value.user_id, "100")
        with self.assertRaises(ProtocolError):
            decode(b'{"access_token":12,"refresh_token":"r"}', OAuthTokens, "OAuth")

    def test_token_response_repr_does_not_expose_credentials(self) -> None:
        value = OAuthTokens("synthetic-access-value", "synthetic-refresh-value")
        self.assertNotIn("synthetic-access-value", repr(value))
        self.assertNotIn("synthetic-refresh-value", repr(value))
