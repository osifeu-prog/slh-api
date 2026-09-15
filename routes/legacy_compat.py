"""
Compatibility endpoints carried over from the minimal API (commit f457efc).

These three routes were added on top of the stub main.py while the full
application was missing, so they do not exist in the restored 0926111 code.
The public site and control-center.html both depend on them.

Self-contained: own imports and constants, no reliance on main.py globals.
"""

import os

import asyncpg
import httpx
from fastapi import APIRouter, Body

router = APIRouter(tags=["legacy-compat"])

DB_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("DATABASE_PUBLIC_URL")
    or os.getenv("DB_URL")
)
BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "")
OWNER_ID = 224223270
SLH_CONTRACT = "0xACb0A09414CEA1C879c67bB7A877E4e19480f022"
BSC_RPC = "https://bsc-dataseed.binance.org/"

CREATE_WALLETS_TABLE = """
CREATE TABLE IF NOT EXISTS connected_wallets (
    wallet_address    TEXT PRIMARY KEY,
    telegram_id       BIGINT,
    telegram_username TEXT,
    slh_balance       NUMERIC,
    tier              TEXT,
    connected_at      TIMESTAMP DEFAULT NOW(),
    last_seen         TIMESTAMP DEFAULT NOW()
)
"""

UPSERT_WALLET = """
INSERT INTO connected_wallets
    (wallet_address, telegram_id, telegram_username, slh_balance, tier)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (wallet_address) DO UPDATE SET
    slh_balance = $4,
    tier        = $5,
    last_seen   = NOW(),
    telegram_id = COALESCE($2, connected_wallets.telegram_id)
"""


def _tier_for(balance: float) -> str:
    if balance >= 1_000_000:
        return "whale"
    if balance >= 100_000:
        return "major"
    if balance >= 10_000:
        return "holder"
    if balance >= 1_000:
        return "investor"
    return "member"


async def _notify(chat_id, text: str) -> None:
    if not (BOT_TOKEN and chat_id):
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
            )
    except Exception:
        pass


@router.post("/api/connect/wallet")
async def connect_wallet(data: dict = Body(...)):
    wallet = str(data.get("wallet_address", "")).lower()
    tg_id = data.get("telegram_id")
    tg_user = data.get("telegram_username", "")

    if not wallet.startswith("0x") or len(wallet) != 42:
        return {"error": "invalid wallet"}

    balance = 0.0
    try:
        padded = wallet[2:].zfill(64)
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                BSC_RPC,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {"to": SLH_CONTRACT, "data": "0x70a08231" + padded},
                        "latest",
                    ],
                    "id": 1,
                },
                timeout=10,
            )
            balance = int(resp.json().get("result", "0x0"), 16) / (10 ** 15)
    except Exception:
        pass

    tier = _tier_for(balance)

    try:
        conn = await asyncpg.connect(DB_URL)
        try:
            await conn.execute(CREATE_WALLETS_TABLE)
            await conn.execute(
                UPSERT_WALLET, wallet, tg_id, tg_user, balance, tier
            )
        finally:
            await conn.close()
    except Exception as exc:
        return {"error": f"db: {exc}"}

    short = f"{wallet[:6]}...{wallet[-4:]}"
    await _notify(tg_id, f"Wallet connected\n{short}\n{balance:,.0f} SLH\n{tier.upper()}")
    await _notify(OWNER_ID, f"New wallet\n{wallet}\n{balance:,.0f} SLH - {tier}")

    return {
        "status": "connected",
        "wallet": wallet,
        "slh_balance": balance,
        "tier": tier,
    }


@router.get("/api/holders")
async def get_holders():
    try:
        conn = await asyncpg.connect(DB_URL)
        try:
            await conn.execute(CREATE_WALLETS_TABLE)
            rows = await conn.fetch(
                "SELECT wallet_address, telegram_username, slh_balance, tier,"
                " connected_at FROM connected_wallets ORDER BY slh_balance DESC"
            )
        finally:
            await conn.close()
        return {"holders": [dict(r) for r in rows], "total": len(rows)}
    except Exception as exc:
        return {"error": str(exc), "holders": [], "total": 0}


@router.get("/api/live-stats")
async def get_live_stats():
    stats = {"premium_users": 0, "investors": 0, "holders": 0, "bots_active": 0}
    try:
        conn = await asyncpg.connect(DB_URL)
        try:
            async def count(sql: str) -> int:
                try:
                    return int(await conn.fetchval(sql) or 0)
                except Exception:
                    return 0

            stats["premium_users"] = await count(
                "SELECT COUNT(*) FROM premium_users"
            )
            stats["investors"] = await count(
                "SELECT COUNT(*) FROM launch_contributions"
                " WHERE status='verified'"
            )
            stats["holders"] = await count(
                "SELECT COUNT(*) FROM connected_wallets"
            )
            stats["bots_active"] = await count(
                "SELECT COUNT(*) FROM bot_registry"
            )
        finally:
            await conn.close()
        return stats
    except Exception as exc:
        return {**stats, "error": str(exc)}
