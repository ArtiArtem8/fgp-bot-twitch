"""Persist tokens, chat events, and probes in local SQLite."""

import asyncio
import os
import sqlite3
import time
from contextlib import closing
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from .wire import ChatEvent

PROBE_COOLDOWN_SECONDS = 30
SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens(
    user_id TEXT PRIMARY KEY, token TEXT NOT NULL, refresh TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages(
    message_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, username TEXT NOT NULL,
    display_name TEXT NOT NULL, channel_id TEXT NOT NULL, message_text TEXT NOT NULL,
    timestamp DATETIME NOT NULL, badges TEXT, is_subscriber BOOLEAN, is_follower BOOLEAN,
    message_type TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fgp_seen(key TEXT PRIMARY KEY, created REAL NOT NULL);
CREATE INDEX IF NOT EXISTS fgp_seen_created ON fgp_seen(created);
CREATE TABLE IF NOT EXISTS fgp_probes(
    nonce TEXT PRIMARY KEY, created REAL NOT NULL, run_id TEXT NOT NULL,
    state TEXT NOT NULL, message_id TEXT, observed_id TEXT, detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS fgp_probes_created ON fgp_probes(created);
"""


class Store:
    """SQLite is the only persistent service; blocking work runs off the event loop.

    A connection is owned by one worker call, never shared between threads.
    SQLite transactions coordinate the bot, auth and status/probe CLI processes.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def _transaction[T](self, callback: Callable[[sqlite3.Connection], T]) -> T:
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=5000")
            with db:
                return callback(db)

    async def call[T](self, callback: Callable[[sqlite3.Connection], T]) -> T:
        return await asyncio.to_thread(lambda: self._transaction(callback))

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        def initialize(db: sqlite3.Connection) -> None:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript(SCHEMA)

        await self.call(initialize)
        # Best effort on POSIX. On Windows the user's directory ACL applies.
        if os.name != "nt":
            self.path.chmod(0o600)

    async def token(self, user_id: str) -> dict[str, Any] | None:
        row = await self.call(
            lambda db: db.execute(
                "SELECT user_id, token, refresh FROM tokens WHERE user_id=?", (user_id,)
            ).fetchone()
        )
        return dict(row) if row else None

    async def save_token(
        self, user_id: str, token: str, refresh: str, expected: tuple[str, str] | None = None
    ) -> bool:
        def save(db: sqlite3.Connection) -> bool:
            if expected is not None:
                cur = db.execute(
                    "UPDATE tokens SET token=?, refresh=? "
                    "WHERE user_id=? AND token=? AND refresh=?",
                    (token, refresh, user_id, *expected),
                )
            else:
                cur = db.execute(
                    "INSERT INTO tokens(user_id,token,refresh) VALUES(?,?,?) "
                    "ON CONFLICT(user_id) DO UPDATE "
                    "SET token=excluded.token, refresh=excluded.refresh",
                    (user_id, token, refresh),
                )
            return cur.rowcount == 1

        return await self.call(save)

    async def claim_event(self, key: str) -> bool:
        return await self.call(
            lambda db: (
                db.execute(
                    "INSERT OR IGNORE INTO fgp_seen VALUES(?,?)", (key, time.time())
                ).rowcount
                == 1
            )
        )

    async def log_message(self, event: ChatEvent, timestamp: str) -> None:
        badges = event.badges
        subscriber = any(b.set_id.lower() in {"subscriber", "founder"} for b in badges)
        values = (
            event.message_id,
            event.chatter_user_id,
            event.chatter_user_login,
            event.chatter_user_name,
            event.broadcaster_user_id,
            event.message.text,
            timestamp,
            msgspec.json.encode(badges).decode(),
            subscriber,
            None,
            event.message_type,
        )
        # Follower status is unknown, NOT false. No API request per message.
        await self.call(
            lambda db: (
                db.execute(
                    "INSERT OR IGNORE INTO messages("
                    "message_id,user_id,username,display_name,channel_id,"
                    "message_text,timestamp,badges,is_subscriber,is_follower,message_type) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    values,
                ).rowcount
            )
        )

    async def new_probe(self, nonce: str, run_id: str) -> None:
        def create(db: sqlite3.Connection) -> None:
            db.execute("BEGIN IMMEDIATE")
            latest = db.execute(
                "SELECT created FROM fgp_probes ORDER BY created DESC LIMIT 1"
            ).fetchone()
            if latest and time.time() - latest[0] < PROBE_COOLDOWN_SECONDS:
                raise ValueError("Проверка уже запрошена недавно. Интервал — не менее 30 секунд.")
            db.execute(
                "INSERT INTO fgp_probes(nonce,created,run_id,state) VALUES(?,?,?,'REQUESTED')",
                (nonce, time.time(), run_id),
            )

        await self.call(create)

    async def claim_probe(self, run_id: str) -> dict[str, Any] | None:
        def claim(db: sqlite3.Connection) -> dict[str, Any] | None:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE fgp_probes SET state='FAILED',detail='Бот перезапущен или запрос устарел' "
                "WHERE state IN ('REQUESTED','SENDING','SENT') AND (run_id!=? OR created<?)",
                (run_id, time.time() - 45),
            )
            row = db.execute(
                "SELECT * FROM fgp_probes WHERE state='REQUESTED' AND run_id=? "
                "ORDER BY created LIMIT 1",
                (run_id,),
            ).fetchone()
            if not row:
                return None
            db.execute("UPDATE fgp_probes SET state='SENDING' WHERE nonce=?", (row["nonce"],))
            return dict(row)

        return await self.call(claim)

    async def probe(self, nonce: str) -> dict[str, Any] | None:
        row = await self.call(
            lambda db: db.execute("SELECT * FROM fgp_probes WHERE nonce=?", (nonce,)).fetchone()
        )
        return dict(row) if row else None

    async def probe_sent(self, nonce: str, message_id: str) -> None:
        await self.call(
            lambda db: (
                db.execute(
                    "UPDATE fgp_probes SET message_id=?,"
                    "state=CASE WHEN observed_id=? THEN 'CONFIRMED' ELSE 'SENT' END "
                    "WHERE nonce=? AND state='SENDING'",
                    (message_id, message_id, nonce),
                ).rowcount
            )
        )

    async def probe_observed(self, nonce: str, message_id: str, run_id: str) -> bool:
        return await self.call(
            lambda db: (
                db.execute(
                    "UPDATE fgp_probes SET observed_id=?,"
                    "state=CASE WHEN message_id=? THEN 'CONFIRMED' ELSE state END "
                    "WHERE nonce=? AND run_id=? AND state IN ('SENDING','SENT') AND created>?",
                    (message_id, message_id, nonce, run_id, time.time() - 45),
                ).rowcount
                == 1
            )
        )

    async def probe_failed(self, nonce: str, detail: str) -> None:
        await self.call(
            lambda db: (
                db.execute(
                    "UPDATE fgp_probes SET state='FAILED',detail=? "
                    "WHERE nonce=? AND state!='CONFIRMED'",
                    (detail, nonce),
                ).rowcount
            )
        )

    async def cleanup(self, chat_retention_days: int) -> None:
        def cleanup(db: sqlite3.Connection) -> None:
            db.execute("DELETE FROM fgp_seen WHERE created<?", (time.time() - 86400,))
            db.execute("DELETE FROM fgp_probes WHERE created<?", (time.time() - 7 * 86400,))
            if chat_retention_days > 0:
                db.execute(
                    "DELETE FROM messages WHERE julianday(timestamp)<julianday('now',?)",
                    (f"-{chat_retention_days} days",),
                )

        await self.call(cleanup)
