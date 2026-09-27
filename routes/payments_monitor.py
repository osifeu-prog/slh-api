"""
SLH Payment Monitor — automatic BSC/TON ingestion.

Polls Genesis wallets every POLL_INTERVAL seconds. When a new incoming
transaction is found that is not yet recorded, tries to match it against
pending_payment_intents (by user_id + approximate amount + time window).
On match: grants premium, issues receipt, optionally notifies user via
Telegram (if bot token set). On no match: stores as unmatched_deposit
for manual review.

Started from main.py startup event via start_monitor(pool).
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Optional

import aiohttp
from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api/payment/monitor", tags=["payment-monitor"])

POLL_INTERVAL = int(os.getenv("PAYMENT_MONITOR_INTERVAL", "30"))
BSC_GENESIS = os.getenv("BSC_GENESIS_ADDRESS", "").strip().lower()
BSC_ABS_TOLERANCE = 0.00002
PAYMENT_MONITOR_ENABLED = os.getenv("PAYMENT_MONITOR_ENABLED", "0") == "1"
MATCH_WINDOW_SECONDS = 3600

_pool = None
_task: Optional[asyncio.Task] = None
_state = {
    "running": False,
    "last_block": None,
    "last_run_iso": None,
    "matches_total": 0,
    "unmatched_total": 0,
    "errors_last": None,
}


def set_pool(pool):
    global _pool
    _pool = pool


async def _ensure_tables(conn):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pending_payment_intents (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            chain TEXT NOT NULL,
            expected_amount DOUBLE PRECISION NOT NULL,
            plan_key TEXT NOT NULL DEFAULT 'premium',
            bot_name TEXT NOT NULL DEFAULT 'ecosystem',
            status TEXT NOT NULL DEFAULT 'open',
            tx_hash TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            matched_at TIMESTAMP,
            expires_at TIMESTAMP NOT NULL DEFAULT (now() + interval '2 hours')
        );
        CREATE INDEX IF NOT EXISTS idx_intents_open ON pending_payment_intents(status, chain, expires_at);
        CREATE TABLE IF NOT EXISTS unmatched_deposits (
            id SERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            tx_hash TEXT UNIQUE NOT NULL,
            from_addr TEXT,
            amount DOUBLE PRECISION NOT NULL,
            block_number BIGINT,
            detected_at TIMESTAMP NOT NULL DEFAULT now(),
            resolved_user_id BIGINT,
            resolved_at TIMESTAMP
        );
        """
    )


async def _rpc(session, url, method, params):
    payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
        return await r.json()


async def _bsc_latest_incoming(session) -> list:
    """Return list of {tx_hash, from, value_bnb, block} for recent BSC incoming txs."""
    rpcs = [
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.ninicoin.io",
        "https://bsc-dataseed2.defibit.io",
    ]
    for rpc in rpcs:
        try:
            latest = await _rpc(session, rpc, "eth_blockNumber", [])
            if not latest.get("result"):
                continue
            latest_block = int(latest["result"], 16)
            start = _state.get("last_block") or (latest_block - 10)
            out = []
            for bn in range(max(start, latest_block - 20), latest_block + 1):
                blk = await _rpc(session, rpc, "eth_getBlockByNumber", [hex(bn), True])
                if not blk.get("result"):
                    continue
                for tx in blk["result"].get("transactions", []) or []:
                    if (tx.get("to") or "").lower() != BSC_GENESIS:
                        continue
                    if tx.get("input") and tx["input"] != "0x":
                        continue
                    val_bnb = int(tx.get("value", "0x0"), 16) / 1e18
                    if val_bnb <= 0:
                        continue
                    out.append({
                        "tx_hash": tx["hash"],
                        "from": (tx.get("from") or "").lower(),
                        "value_bnb": val_bnb,
                        "block": bn,
                    })
            _state["last_block"] = latest_block
            return out
        except Exception as e:
            _state["errors_last"] = f"{rpc}: {e}"
            continue
    return []


async def _match_and_ingest(conn, chain: str, deposit: dict) -> Optional[dict]:
    tx_hash = deposit["tx_hash"]
    existing = await conn.fetchrow(
        "SELECT 1 FROM bsc_deposits WHERE tx_hash = $1" if chain == "bsc" else "SELECT 1 FROM ton_deposits WHERE tx_hash = $1",
        tx_hash,
    )
    if existing:
        return None

    amount = deposit["value_bnb"] if chain == "bsc" else deposit.get("value_ton", 0.0)
    intent = await conn.fetchrow(
        """
        SELECT * FROM pending_payment_intents
        WHERE status = 'open'
          AND chain = $1
          AND expires_at > now()
          AND expected_amount - $2 <= $3
        ORDER BY abs(expected_amount - $2) ASC, created_at ASC
        LIMIT 1
        """,
        chain,
        amount,
        BSC_ABS_TOLERANCE,
    )

    if intent:
        try:
            from routes.payments_auto import _grant_premium, _issue_receipt
        except Exception as e:
            _state["errors_last"] = f"import payments_auto: {e}"
            return None

        result = await _grant_premium(
            conn, intent["user_id"], intent["bot_name"], tx_hash, amount,
            "BNB" if chain == "bsc" else "TON", intent["plan_key"],
            chain=chain, source="payment_monitor",
        )
        rcpt = None
        if not result.get("already_processed"):
            rcpt = await _issue_receipt(
                conn, intent["user_id"], f"{chain}_deposit",
                result.get("deposit_id"), amount, "BNB" if chain == "bsc" else "TON",
            )
        await conn.execute(
            "UPDATE pending_payment_intents SET status='matched', tx_hash=$1, matched_at=now() WHERE id=$2",
            tx_hash, intent["id"],
        )
        _state["matches_total"] += 1
        return {"matched": True, "user_id": intent["user_id"], "receipt": rcpt}

    await conn.execute(
        """
        INSERT INTO unmatched_deposits (chain, tx_hash, from_addr, amount, block_number)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (tx_hash) DO NOTHING
        """,
        chain, tx_hash, deposit.get("from"), amount, deposit.get("block"),
    )
    _state["unmatched_total"] += 1
    return {"matched": False}


async def _loop():
    if not PAYMENT_MONITOR_ENABLED or not BSC_GENESIS:
        _state["running"] = False
        _state["errors_last"] = "payment monitor disabled or BSC settlement treasury not configured"
        return
    _state["running"] = True
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                deposits = await _bsc_latest_incoming(session)
                if deposits and _pool:
                    async with _pool.acquire() as conn:
                        await _ensure_tables(conn)
                        for d in deposits:
                            await _match_and_ingest(conn, "bsc", d)
                _state["last_run_iso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            except Exception as e:
                _state["errors_last"] = f"loop: {e}"
            await asyncio.sleep(POLL_INTERVAL)


def start_monitor():
    global _task
    if not PAYMENT_MONITOR_ENABLED or not BSC_GENESIS:
        _state["running"] = False
        _state["errors_last"] = "payment monitor disabled or BSC settlement treasury not configured"
        return
    if _task and not _task.done():
        return
    _task = asyncio.create_task(_loop())


@router.get("/status")
async def monitor_status():
    return {"ok": True, **_state, "poll_interval_s": POLL_INTERVAL, "genesis": BSC_GENESIS}


class IntentReq:
    user_id: int
    chain: str
    expected_amount: float
    plan_key: str = "premium"


@router.post("/intent")
async def register_intent(
    user_id: int,
    chain: str,
    expected_amount: float,
    plan_key: str = "premium",
    bot_name: str = "ecosystem",
    request: Request = None,
):
    if request is None:
        raise HTTPException(401, "Authentication required")
    from main import _require_owner
    _require_owner(
        user_id,
        request.headers.get("authorization"),
        request.headers.get("x-admin-key"),
        request.headers.get("x-slh-service-token"),
        allow_service=False,
    )
    if chain not in ("bsc", "ton"):
        raise HTTPException(400, "chain must be bsc or ton")
    if _pool is None:
        raise HTTPException(500, "db pool not initialized")
    async with _pool.acquire() as conn:
        await _ensure_tables(conn)
        row = await conn.fetchrow(
            """
            INSERT INTO pending_payment_intents (user_id, chain, expected_amount, plan_key, bot_name)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, expires_at
            """,
            user_id, chain, expected_amount, plan_key, bot_name,
        )
    return {"ok": True, "intent_id": row["id"], "expires_at": row["expires_at"].isoformat()}
