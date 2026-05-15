"""slh-master-bot — Telegram bot with FastAPI health endpoint.

Architecture
------------
- FastAPI app exposes /api/health (used by Railway healthcheck)
- python-telegram-bot runs in the same process via webhook or polling
- Structured JSON logging via logging_config
- PostgreSQL user tracking via database module
- FastAPI backend communication via api_client module

Environment variables (see .env.example):
    TELEGRAM_BOT_TOKEN      Bot token from @BotFather
    TELEGRAM_WEBHOOK_URL    Public HTTPS URL for webhook mode (optional)
    RAILWAY_FASTAPI_URL     Base URL of the SLH FastAPI backend
    DATABASE_URL            PostgreSQL connection string
    REDIS_URL               Redis connection string (optional)
    PORT                    HTTP port (default 8000)
    ALLOWED_IDS             Comma-separated Telegram user IDs allowed to use bot
    MASTER_BOT_TOKEN        Alias for TELEGRAM_BOT_TOKEN (legacy compat)
    GROQ_API_KEY            Groq API key for AI features (optional)
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Bootstrap: load .env before importing project modules
# ---------------------------------------------------------------------------

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, ".env"), override=False)

# ---------------------------------------------------------------------------
# Project imports (after env is loaded)
# ---------------------------------------------------------------------------

from logging_config import setup_logging, get_logger  # noqa: E402
from api_client import FastAPIClient  # noqa: E402
import database  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
setup_logging(LOG_LEVEL)
log = get_logger("slh-master-bot")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TOKEN: str = (
    os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("MASTER_BOT_TOKEN")
    or ""
)
WEBHOOK_URL: str = os.getenv("TELEGRAM_WEBHOOK_URL", "")
PORT: int = int(os.getenv("PORT", "8000"))
DATABASE_URL: str = os.getenv("DATABASE_URL", "")
RAILWAY_FASTAPI_URL: str = os.getenv("RAILWAY_FASTAPI_URL", "")

# Allowlist: empty = all users allowed
_raw_ids = os.getenv("ALLOWED_IDS", "")
ALLOWED_IDS: set[int] = (
    {int(x.strip()) for x in _raw_ids.split(",") if x.strip().isdigit()}
    if _raw_ids.strip()
    else set()
)

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

_api_client = FastAPIClient(base_url=RAILWAY_FASTAPI_URL)
_startup_time = datetime.now(timezone.utc)
_bot_info: dict[str, Any] = {}

# ---------------------------------------------------------------------------
# Telegram bot setup (python-telegram-bot v20+)
# ---------------------------------------------------------------------------

_application: Any = None  # telegram.ext.Application


def _is_authorized(user_id: int) -> bool:
    """Return True if the user is allowed to interact with the bot."""
    if not ALLOWED_IDS:
        return True
    return user_id in ALLOWED_IDS


async def _setup_telegram() -> None:
    """Initialise the python-telegram-bot Application."""
    global _application, _bot_info

    if not TOKEN:
        log.warning(
            "TELEGRAM_BOT_TOKEN is not set — Telegram bot will not start"
        )
        return

    try:
        from telegram import Update
        from telegram.ext import (
            Application,
            CommandHandler,
            MessageHandler,
            filters,
        )

        app = Application.builder().token(TOKEN).build()

        # ---- Command handlers ----

        async def cmd_start(update: Update, context: Any) -> None:
            user = update.effective_user
            if not user or not _is_authorized(user.id):
                await update.message.reply_text(  # type: ignore[union-attr]
                    "⛔ Access denied."
                )
                return

            # Track user in DB (best-effort)
            try:
                await database.upsert_user(
                    user_id=user.id,
                    username=user.username,
                    full_name=user.full_name,
                    language_code=user.language_code,
                )
                await database.log_event(user.id, "start")
            except Exception as exc:
                log.warning("DB upsert failed for %d: %s", user.id, exc)

            # Sync to FastAPI backend (best-effort)
            try:
                await _api_client.register_user(
                    telegram_id=user.id,
                    username=user.username,
                    first_name=user.first_name,
                    last_name=user.last_name,
                )
            except Exception as exc:
                log.warning("API register_user failed: %s", exc)

            await update.message.reply_text(  # type: ignore[union-attr]
                f"👋 Welcome, {user.first_name}!\n\n"
                "I'm the SLH Master Bot.\n\n"
                "Commands:\n"
                "/health  — system health\n"
                "/prices  — token prices\n"
                "/wallet  — your balances\n"
                "/help    — this message"
            )

        async def cmd_help(update: Update, context: Any) -> None:
            user = update.effective_user
            if not user or not _is_authorized(user.id):
                await update.message.reply_text("⛔ Access denied.")  # type: ignore[union-attr]
                return
            await update.message.reply_text(  # type: ignore[union-attr]
                "📖 *SLH Master Bot — Commands*\n\n"
                "/start   — register & welcome\n"
                "/health  — check API + DB status\n"
                "/prices  — live token prices\n"
                "/wallet  — your token balances\n"
                "/help    — this message",
                parse_mode="Markdown",
            )

        async def cmd_health(update: Update, context: Any) -> None:
            user = update.effective_user
            if not user or not _is_authorized(user.id):
                await update.message.reply_text("⛔ Access denied.")  # type: ignore[union-attr]
                return

            api_health = await _api_client.health_check()
            db_ok = await database.ping()

            api_status = api_health.get("status", "unknown")
            db_status = "connected" if db_ok else "error"

            lines = [
                "🩺 *System Health*",
                f"• API: `{api_status}`",
                f"• DB:  `{db_status}`",
            ]
            if "version" in api_health:
                lines.append(f"• Version: `{api_health['version']}`")
            if "error" in api_health:
                lines.append(f"• Error: `{api_health['error']}`")

            await update.message.reply_text(  # type: ignore[union-attr]
                "\n".join(lines), parse_mode="Markdown"
            )

        async def cmd_prices(update: Update, context: Any) -> None:
            user = update.effective_user
            if not user or not _is_authorized(user.id):
                await update.message.reply_text("⛔ Access denied.")  # type: ignore[union-attr]
                return

            data = await _api_client.get_prices()
            prices = data.get("prices") or data
            if not isinstance(prices, dict) or not prices:
                await update.message.reply_text("No price data available.")  # type: ignore[union-attr]
                return

            lines = ["💰 *Token Prices*"]
            for token, value in prices.items():
                if isinstance(value, dict):
                    ils = value.get("ils") or value.get("price") or "?"
                    usd = value.get("usd") or "?"
                    lines.append(f"• *{token}*: ₪{ils} / ${usd}")
                else:
                    lines.append(f"• *{token}*: {value}")

            await update.message.reply_text(  # type: ignore[union-attr]
                "\n".join(lines), parse_mode="Markdown"
            )

        async def cmd_wallet(update: Update, context: Any) -> None:
            user = update.effective_user
            if not user or not _is_authorized(user.id):
                await update.message.reply_text("⛔ Access denied.")  # type: ignore[union-attr]
                return

            data = await _api_client.get_user_balances(user.id)
            balances = data.get("balances", {})
            if not balances:
                await update.message.reply_text(  # type: ignore[union-attr]
                    "No wallet data found. Use /start to register first."
                )
                return

            lines = ["💼 *Your Balances*"]
            for token, amount in balances.items():
                lines.append(f"• *{token}*: `{amount}`")

            await update.message.reply_text(  # type: ignore[union-attr]
                "\n".join(lines), parse_mode="Markdown"
            )

        async def on_text(update: Update, context: Any) -> None:
            """Handle plain text messages — track user activity."""
            user = update.effective_user
            if not user or not _is_authorized(user.id):
                return
            try:
                await database.upsert_user(
                    user_id=user.id,
                    username=user.username,
                    full_name=user.full_name,
                    language_code=user.language_code,
                )
            except Exception as exc:
                log.debug("DB upsert on text failed: %s", exc)

        async def on_error(update: object, context: Any) -> None:
            log.error(
                "Telegram error: %s | update: %s",
                context.error,
                update,
                exc_info=context.error,
            )

        # Register handlers
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("help", cmd_help))
        app.add_handler(CommandHandler("health", cmd_health))
        app.add_handler(CommandHandler("prices", cmd_prices))
        app.add_handler(CommandHandler("wallet", cmd_wallet))
        app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, on_text)
        )
        app.add_error_handler(on_error)

        _application = app

        # Fetch bot info for health endpoint
        await app.initialize()
        me = await app.bot.get_me()
        _bot_info = {
            "id": me.id,
            "username": me.username,
            "first_name": me.first_name,
        }
        log.info(
            "Telegram bot ready: @%s (id=%d)", me.username, me.id
        )

    except ImportError:
        log.error(
            "python-telegram-bot is not installed — "
            "add 'python-telegram-bot' to requirements.txt"
        )
    except Exception as exc:
        log.error("Failed to initialise Telegram bot: %s", exc, exc_info=True)


async def _start_polling() -> None:
    """Start the bot in long-polling mode (no webhook)."""
    if _application is None:
        return
    try:
        log.info("Starting Telegram bot in polling mode…")
        await _application.start()
        await _application.updater.start_polling(  # type: ignore[union-attr]
            drop_pending_updates=True
        )
        log.info("Telegram polling active")
    except Exception as exc:
        log.error("Polling start failed: %s", exc, exc_info=True)


async def _stop_polling() -> None:
    """Stop the polling loop gracefully."""
    if _application is None:
        return
    try:
        if _application.updater and _application.updater.running:  # type: ignore[union-attr]
            await _application.updater.stop()  # type: ignore[union-attr]
        await _application.stop()
        await _application.shutdown()
        log.info("Telegram bot stopped")
    except Exception as exc:
        log.warning("Error stopping Telegram bot: %s", exc)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Startup / shutdown lifecycle for FastAPI."""
    log.info("slh-master-bot starting up…")

    # Initialise API client
    await _api_client.init()

    # Initialise database (best-effort — don't crash if DB is unavailable)
    if DATABASE_URL:
        try:
            await database.init_pool(DATABASE_URL)
        except Exception as exc:
            log.error("Database init failed: %s", exc)
    else:
        log.warning("DATABASE_URL not set — database features disabled")

    # Initialise and start Telegram bot
    await _setup_telegram()
    if not WEBHOOK_URL:
        asyncio.create_task(_start_polling())

    log.info("slh-master-bot startup complete (port=%d)", PORT)
    yield

    # Shutdown
    log.info("slh-master-bot shutting down…")
    await _stop_polling()
    await _api_client.close()
    await database.close_pool()
    log.info("slh-master-bot shutdown complete")


fastapi_app = FastAPI(
    title="SLH Master Bot",
    version="1.0.0",
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Health endpoint (required by Railway healthcheck)
# ---------------------------------------------------------------------------


@fastapi_app.get("/api/health")
async def health() -> JSONResponse:
    """Railway healthcheck endpoint.

    Returns HTTP 200 with a JSON payload describing the service state.
    """
    db_ok = await database.ping() if DATABASE_URL else None
    api_health = await _api_client.health_check()

    uptime_seconds = (
        datetime.now(timezone.utc) - _startup_time
    ).total_seconds()

    payload: dict[str, Any] = {
        "status": "ok",
        "service": "slh-master-bot",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_seconds": round(uptime_seconds, 1),
        "checks": {
            "db": "connected" if db_ok else ("disabled" if db_ok is None else "error"),
            "fastapi_backend": api_health.get("status", "unknown"),
        },
        "bot": _bot_info if _bot_info else {"status": "not_configured"},
    }

    return JSONResponse(content=payload, status_code=200)


@fastapi_app.get("/health")
async def health_root() -> JSONResponse:
    """Alias for /api/health (some Railway configs probe the root path)."""
    return await health()


@fastapi_app.get("/")
async def root() -> JSONResponse:
    return JSONResponse(
        content={
            "service": "slh-master-bot",
            "status": "running",
            "docs": "/docs",
            "health": "/api/health",
        }
    )


# ---------------------------------------------------------------------------
# Webhook endpoint (used when TELEGRAM_WEBHOOK_URL is set)
# ---------------------------------------------------------------------------


@fastapi_app.post("/webhook")
async def telegram_webhook(request: Request) -> JSONResponse:
    """Receive Telegram updates via webhook."""
    if _application is None:
        return JSONResponse({"error": "bot not initialised"}, status_code=503)

    try:
        from telegram import Update

        data = await request.json()
        update = Update.de_json(data, _application.bot)
        await _application.process_update(update)
        return JSONResponse({"ok": True})
    except Exception as exc:
        log.error("Webhook processing error: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the FastAPI + Telegram bot server."""
    log.info(
        "Starting slh-master-bot on 0.0.0.0:%d (webhook=%s)",
        PORT,
        bool(WEBHOOK_URL),
    )
    uvicorn.run(
        "bot:fastapi_app",
        host="0.0.0.0",
        port=PORT,
        log_level=LOG_LEVEL.lower(),
        # Disable uvicorn's own access log — our JSON logger handles it
        access_log=False,
    )


if __name__ == "__main__":
    main()
