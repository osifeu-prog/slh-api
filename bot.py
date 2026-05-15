"""
SLH Master Bot — FastAPI + python-telegram-bot entry point.

Starts a FastAPI server on $PORT (default 8000) for Railway healthchecks,
and runs the Telegram bot in polling mode concurrently.

Commands: /start, /help, /health, /prices, /wallet
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
import uvicorn

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("slh-master-bot")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOT_TOKEN: str = os.environ["BOT_TOKEN"]
PORT: int = int(os.getenv("PORT", "8000"))
API_BASE: str = os.getenv("SLH_API_BASE", "https://slh-api-production.up.railway.app")

# ---------------------------------------------------------------------------
# Telegram handlers
# ---------------------------------------------------------------------------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    await update.message.reply_html(
        f"👋 Hello, <b>{user.full_name}</b>!\n\n"
        "I'm the <b>SLH Master Bot</b>.\n\n"
        "Use /help to see available commands."
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📋 <b>Available Commands</b>\n\n"
        "/start   — Welcome message\n"
        "/help    — This help text\n"
        "/health  — API & DB health status\n"
        "/prices  — SLH token prices\n"
        "/wallet  — Wallet info\n",
        parse_mode="HTML",
    )


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(f"{API_BASE}/api/health")
            resp.raise_for_status()
            data = resp.json()
        status = data.get("status", "unknown")
        db = data.get("db", "unknown")
        version = data.get("version", "?")
        await update.message.reply_text(
            f"✅ <b>API:</b> {status}\n"
            f"🗄 <b>DB:</b> {db}\n"
            f"🔖 <b>Version:</b> {version}",
            parse_mode="HTML",
        )
    except httpx.HTTPStatusError as exc:
        await update.message.reply_text(f"⚠️ API returned {exc.response.status_code}.")
    except Exception as exc:
        log.exception("/health failed")
        await update.message.reply_text(f"❌ Error: {type(exc).__name__}: {exc}")


async def cmd_prices(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(f"{API_BASE}/api/prices")
            resp.raise_for_status()
            data = resp.json()
        prices = data.get("prices") or data
        if not isinstance(prices, dict) or not prices:
            await update.message.reply_text("ℹ️ No price data available right now.")
            return
        lines = ["💰 <b>Token Prices</b>\n"]
        for token, value in prices.items():
            if isinstance(value, dict):
                ils = value.get("ils") or value.get("price") or value.get("value")
            else:
                ils = value
            try:
                fmt = f"{float(ils):,.4f}"
            except (TypeError, ValueError):
                fmt = str(ils)
            lines.append(f"• <b>{token}:</b> {fmt}")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
    except Exception as exc:
        log.exception("/prices failed")
        await update.message.reply_text(f"❌ Error: {type(exc).__name__}: {exc}")


async def cmd_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🔐 <b>Wallet</b>\n\nWallet integration coming soon.",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# FastAPI app (healthcheck endpoint for Railway)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("FastAPI startup — healthcheck endpoint ready")
    yield
    log.info("FastAPI shutdown")


fastapi_app = FastAPI(title="SLH Master Bot", lifespan=lifespan)


@fastapi_app.get("/api/health")
async def health():
    return {"status": "ok", "service": "slh-master-bot"}


# ---------------------------------------------------------------------------
# Main — run FastAPI + Telegram bot concurrently
# ---------------------------------------------------------------------------

async def run_bot(stop_event: asyncio.Event) -> None:
    """Build and run the Telegram bot until stop_event is set."""
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("health", cmd_health))
    application.add_handler(CommandHandler("prices", cmd_prices))
    application.add_handler(CommandHandler("wallet", cmd_wallet))

    async with application:
        await application.start()
        log.info("Telegram bot polling started")
        await application.updater.start_polling(drop_pending_updates=True)
        await stop_event.wait()
        await application.updater.stop()
        await application.stop()
    log.info("Telegram bot stopped")


async def main() -> None:
    stop_event = asyncio.Event()

    config = uvicorn.Config(
        app=fastapi_app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
    )
    server = uvicorn.Server(config)

    # Run FastAPI server and Telegram bot concurrently
    await asyncio.gather(
        server.serve(),
        run_bot(stop_event),
    )


if __name__ == "__main__":
    asyncio.run(main())
