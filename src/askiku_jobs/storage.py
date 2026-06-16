from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import sqlite3
from typing import Iterator


class Storage:
    """Minimal SQLite state store required by the portable jobs monitor."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS topic_bindings (
                    kind TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    label TEXT,
                    created_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT ''
                );
                """
            )

    def set_state(self, key: str, value: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def get_state(self, key: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM state WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def get_topic_binding(self, kind: str) -> tuple[int, int, str | None] | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT chat_id, topic_id, label
                FROM topic_bindings
                WHERE kind = ?
                """,
                (kind,),
            ).fetchone()
        if row is None:
            return None
        label = row["label"]
        return int(row["chat_id"]), int(row["topic_id"]), str(label) if label else None

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()
