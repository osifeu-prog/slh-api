"""PostgreSQL database layer for slh-master-bot.

Provides:
- Async connection pool via asyncpg
- User upsert / lookup helpers
- Session management utilities
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg

log = logging.getLogger("slh-master-bot.database")

# ---------------------------------------------------------------------------
# Module-level pool (initialised once at startup)
# ---------------------------------------------------------------------------

_pool: asyncpg.Pool | None = None

# DDL executed once on first connection
_SCHEMA = """
CREATE TABLE IF NOT EXISTS master_bot_users (
    user_id        BIGINT PRIMARY KEY,
    username       TEXT,
    full_name      TEXT,
    language_code  TEXT,
    is_premium     BOOLEAN  DEFAULT FALSE,
    is_blocked     BOOLEAN  DEFAULT FALSE,
    first_seen     TIMESTAMPTZ DEFAULT NOW(),
    last_seen      TIMESTAMPTZ DEFAULT NOW(),
    message_count  INTEGER  DEFAULT 0
);

CREATE TABLE IF NOT EXISTS master_bot_events (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT,
    event_type TEXT NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mbe_user_id ON master_bot_events(user_id);
CREATE INDEX IF NOT EXISTS idx_mbe_event_type ON master_bot_events(event_type);
"""


async def init_pool(database_url: str | None = None) -> asyncpg.Pool:
    """Create (or return existing) asyncpg connection pool.

    Normalises ``postgres://`` → ``postgresql://`` for SQLAlchemy compat.
    """
    global _pool
    if _pool is not None:
        return _pool

    url = database_url or os.getenv("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL is not set — cannot connect to PostgreSQL")

    # asyncpg requires postgresql:// scheme
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    log.info("Connecting to PostgreSQL…")
    _pool = await asyncpg.create_pool(
        url,
        min_size=1,
        max_size=10,
        command_timeout=30,
    )
    log.info("PostgreSQL pool ready")

    # Bootstrap schema
    async with _pool.acquire() as conn:
        await conn.execute(_SCHEMA)
    log.info("Schema bootstrapped")

    return _pool


async def close_pool() -> None:
    """Gracefully close the connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("PostgreSQL pool closed")


def get_pool() -> asyncpg.Pool:
    """Return the active pool; raises if not initialised."""
    if _pool is None:
        raise RuntimeError("Database pool not initialised — call init_pool() first")
    return _pool


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------


async def upsert_user(
    user_id: int,
    username: str | None = None,
    full_name: str | None = None,
    language_code: str | None = None,
) -> dict[str, Any]:
    """Insert or update a Telegram user record.

    Returns the current row as a plain dict.
    """
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO master_bot_users
                (user_id, username, full_name, language_code, last_seen, message_count)
            VALUES ($1, $2, $3, $4, NOW(), 1)
            ON CONFLICT (user_id) DO UPDATE
                SET username       = COALESCE($2, master_bot_users.username),
                    full_name      = COALESCE($3, master_bot_users.full_name),
                    language_code  = COALESCE($4, master_bot_users.language_code),
                    last_seen      = NOW(),
                    message_count  = master_bot_users.message_count + 1
            RETURNING *
            """,
            user_id,
            username,
            full_name,
            language_code,
        )
    return dict(row)


async def get_user(user_id: int) -> dict[str, Any] | None:
    """Fetch a user row by Telegram user_id. Returns None if not found."""
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM master_bot_users WHERE user_id = $1",
            user_id,
        )
    return dict(row) if row else None


async def get_user_count() -> int:
    """Return total number of registered users."""
    pool = get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM master_bot_users")


# ---------------------------------------------------------------------------
# Event logging
# ---------------------------------------------------------------------------


async def log_event(
    user_id: int | None,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> None:
    """Persist a bot event for analytics / audit trail."""
    import json

    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO master_bot_events (user_id, event_type, payload)
            VALUES ($1, $2, $3::jsonb)
            """,
            user_id,
            event_type,
            json.dumps(payload or {}),
        )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


async def ping() -> bool:
    """Return True if the database is reachable."""
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as exc:
        log.warning("DB ping failed: %s", exc)
        return False
