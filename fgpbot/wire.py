"""The small subset of Twitch JSON that this bot actually consumes."""

from dataclasses import dataclass

import msgspec

from .network import ProtocolError


class OAuthValidate(msgspec.Struct, frozen=True):
    """Identity and scopes returned by OAuth validate."""

    client_id: str
    user_id: str
    scopes: list[str]
    expires_in: int
    login: str = ""


class OAuthTokens(msgspec.Struct, frozen=True):
    """Rotated or newly authorized credentials."""

    access_token: str
    refresh_token: str

    def __repr__(self) -> str:
        return "OAuthTokens(<redacted>)"


class User(msgspec.Struct, frozen=True):
    """Helix user identity."""

    id: str
    login: str
    display_name: str = ""


class Users(msgspec.Struct, frozen=True):
    """Helix Get Users response."""

    data: list[User]


class Follow(msgspec.Struct, frozen=True):
    """Timestamp needed for the followage command."""

    followed_at: str


class Followers(msgspec.Struct, frozen=True):
    """Helix Get Channel Followers response."""

    data: list[Follow]


class Condition(msgspec.Struct, frozen=True):
    """Fields needed to verify EventSub ownership."""

    broadcaster_user_id: str
    user_id: str = ""


class Transport(msgspec.Struct, frozen=True):
    """EventSub transport binding."""

    method: str
    session_id: str


class Subscription(msgspec.Struct, frozen=True):
    """A subscription returned by Helix."""

    id: str
    type: str
    version: str
    status: str
    condition: Condition
    transport: Transport


class Pagination(msgspec.Struct, frozen=True):
    """Optional Helix pagination cursor."""

    cursor: str = ""


class Subscriptions(msgspec.Struct, frozen=True):
    """Helix subscription list or create response."""

    data: list[Subscription]
    pagination: Pagination = msgspec.field(default_factory=Pagination)


class DropReason(msgspec.Struct, frozen=True):
    """Reason Twitch declined to send a chat message."""

    code: str = "unknown"
    message: str = ""


class SentMessage(msgspec.Struct, frozen=True):
    """One Send Chat Message result."""

    is_sent: bool
    message_id: str = ""
    drop_reason: DropReason | None = None


class SendChat(msgspec.Struct, frozen=True):
    """Helix Send Chat Message response."""

    data: list[SentMessage]


class Metadata(msgspec.Struct, frozen=True):
    """EventSub envelope metadata used by this bot."""

    message_type: str
    message_id: str = ""
    message_timestamp: str = ""


class Envelope(msgspec.Struct, frozen=True):
    """Decode metadata first, leaving the variant payload unparsed."""

    metadata: Metadata
    payload: msgspec.Raw


class Session(msgspec.Struct, frozen=True):
    """EventSub welcome or handoff session."""

    id: str = ""
    keepalive_timeout_seconds: int | None = None
    reconnect_url: str | None = None


class SessionPayload(msgspec.Struct, frozen=True):
    """Payload for welcome and reconnect messages."""

    session: Session


class EmptyPayload(msgspec.Struct, frozen=True):
    """The required object payload on a keepalive."""


class RevokedSubscription(msgspec.Struct, frozen=True):
    """Fields used when Twitch revokes a subscription."""

    type: str
    status: str


class RevocationPayload(msgspec.Struct, frozen=True):
    """EventSub revocation payload."""

    subscription: RevokedSubscription


class EventSubscription(msgspec.Struct, frozen=True):
    """Fields checked before an EventSub notification is queued."""

    type: str
    condition: Condition


class ChatText(msgspec.Struct, frozen=True):
    """Chat text nested in an EventSub chat event."""

    text: str


class Badge(msgspec.Struct, frozen=True):
    """Badge identity used by local message logging."""

    set_id: str
    id: str = ""
    info: str = ""


class ChatEvent(msgspec.Struct, frozen=True):
    """Fields used to dispatch and record a chat message."""

    broadcaster_user_id: str
    message_id: str
    chatter_user_id: str
    message: ChatText
    chatter_user_login: str = ""
    chatter_user_name: str = ""
    source_broadcaster_user_id: str | None = None
    badges: list[Badge] = msgspec.field(default_factory=list)
    message_type: str = "text"


class StreamEvent(msgspec.Struct, frozen=True):
    """Fields used for stream greetings."""

    id: str
    broadcaster_user_id: str
    broadcaster_user_name: str = ""


class NotificationPayload(msgspec.Struct, frozen=True):
    """One validated supported notification."""

    subscription: EventSubscription
    event: ChatEvent | StreamEvent


@dataclass(frozen=True, slots=True)
class Frame:
    """Decoded EventSub message with one validated payload variant."""

    metadata: Metadata
    payload: (
        SessionPayload
        | EmptyPayload
        | RevocationPayload
        | NotificationPayload
        | _UnknownNotification
    )


def decode[T](raw: bytes | str, model: type[T], context: str) -> T:
    """Decode without putting untrusted response bodies in exceptions or logs."""
    try:
        return msgspec.json.decode(raw, type=model)
    except msgspec.DecodeError:
        raise ProtocolError(f"{context}: некорректный JSON или тип поля") from None


def eventsub(raw: bytes | str) -> Frame:
    """Select an EventSub payload schema using validated metadata."""
    envelope = decode(raw, Envelope, "EventSub envelope")
    kind = envelope.metadata.message_type
    if kind in {"session_welcome", "session_reconnect"}:
        payload = decode(envelope.payload, SessionPayload, f"EventSub {kind}")
    elif kind == "session_keepalive":
        payload = decode(envelope.payload, EmptyPayload, "EventSub keepalive")
    elif kind == "revocation":
        payload = decode(envelope.payload, RevocationPayload, "EventSub revocation")
    elif kind == "notification":
        partial = decode(envelope.payload, _RawNotification, "EventSub notification")
        event_type = partial.subscription.type
        if event_type == "channel.chat.message":
            event = decode(partial.event, ChatEvent, "EventSub chat event")
        elif event_type == "stream.online":
            event = decode(partial.event, StreamEvent, "EventSub stream event")
        else:
            # Unknown subscriptions are ignored by the caller, but the payload
            # still has a valid object shape and subscription identity.
            event = decode(partial.event, _UnknownEvent, "EventSub event")
            return Frame(envelope.metadata, _UnknownNotification(partial.subscription, event))
        payload = NotificationPayload(partial.subscription, event)
    else:
        raise ProtocolError(f"Неожиданный тип EventSub: {kind}")
    return Frame(envelope.metadata, payload)


class _RawNotification(msgspec.Struct, frozen=True):
    subscription: EventSubscription
    event: msgspec.Raw


class _UnknownEvent(msgspec.Struct, frozen=True):
    broadcaster_user_id: str = ""


class _UnknownNotification(msgspec.Struct, frozen=True):
    subscription: EventSubscription
    event: _UnknownEvent
