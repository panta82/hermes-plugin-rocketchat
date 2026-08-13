"""Durable, profile-scoped idempotency ledger for Rocket.Chat inbound events."""

from __future__ import annotations

import logging
import sqlite3
import time
from contextlib import closing
from pathlib import Path
from typing import Literal

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

ClaimResult = Literal["claimed", "duplicate", "error"]
_PROCESSED_RETENTION_SECONDS = 90 * 24 * 60 * 60


class InboundEventLedger:
    """Atomically claim Rocket.Chat posts across TTL expiry and restarts."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path or (get_hermes_home() / "rocketchat-inbound.db")

    def _connect(self) -> sqlite3.Connection:
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS inbound_events (
                platform TEXT NOT NULL,
                room_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('processing', 'processed')),
                claimed_at REAL NOT NULL,
                completed_at REAL,
                PRIMARY KEY (platform, room_id, message_id)
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS inbound_events_retention_idx
            ON inbound_events (platform, status, completed_at)
            """
        )
        return connection

    def claim(self, room_id: str, message_id: str) -> ClaimResult:
        """Claim an event atomically; an existing claim is always a duplicate."""
        now = time.time()
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    """
                    DELETE FROM inbound_events
                    WHERE platform = 'rocketchat'
                      AND status = 'processed'
                      AND completed_at < ?
                    """,
                    (now - _PROCESSED_RETENTION_SECONDS,),
                )
                row = connection.execute(
                    """
                    SELECT status, claimed_at
                    FROM inbound_events
                    WHERE platform = 'rocketchat' AND room_id = ? AND message_id = ?
                    """,
                    (room_id, message_id),
                ).fetchone()
                if row is None:
                    connection.execute(
                        """
                        INSERT INTO inbound_events (
                            platform, room_id, message_id, status, claimed_at
                        ) VALUES ('rocketchat', ?, ?, 'processing', ?)
                        """,
                        (room_id, message_id, now),
                    )
                    return "claimed"
                return "duplicate"
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logger.exception(
                "Rocket.Chat durable inbound claim failed; dropping event fail-closed"
            )
            return "error"

    def complete(self, room_id: str, message_id: str) -> bool:
        """Mark a successfully handled event permanently processed."""
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute(
                    """
                    UPDATE inbound_events
                    SET status = 'processed', completed_at = ?
                    WHERE platform = 'rocketchat' AND room_id = ? AND message_id = ?
                    """,
                    (time.time(), room_id, message_id),
                )
            return True
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logger.exception("Rocket.Chat durable inbound completion failed")
            return False

    def release(self, room_id: str, message_id: str) -> bool:
        """Release a failed event so Rocket.Chat may redeliver it."""
        try:
            with closing(self._connect()) as connection, connection:
                connection.execute(
                    """
                    DELETE FROM inbound_events
                    WHERE platform = 'rocketchat' AND room_id = ? AND message_id = ?
                    """,
                    (room_id, message_id),
                )
            return True
        except (OSError, sqlite3.Error, TypeError, ValueError):
            logger.exception("Rocket.Chat durable inbound claim release failed")
            return False
