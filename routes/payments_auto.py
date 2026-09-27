"""
SLH Auto-verify Payment Endpoints
==================================
On-chain verification for TON + BSC transfers.
Replaces the manual 24h approve flow with sub-60s automatic verification.

Endpoints:
  POST /api/payment/ton/auto-verify     — verify TON tx, grant premium
  POST /api/payment/bsc/auto-verify     — verify BSC/BNB tx, grant premium
  GET  /api/payment/status/{user_id}    — check user premium + last deposit
  GET  /api/payment/config              — public config (addresses, min amounts)
  POST /api/payment/external/record     — record a 3rd-party fiat payment (Stripe/PayBox/Bit)
  POST /api/payment/receipt              — issue a SLH digital receipt
  GET  /api/payment/geography/summary   — dashboard: payments by country/currency

Added 2026-04-17 morning (ship-all session).
"""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Optional

import aiohttp
from fastapi import APIRouter, HTTPException, Header, Request
from pydantic import BaseModel

# Sibling import — revenue recording helper. Same module, no new deps.
from routes.treasury import record_revenue_internal as _record_revenue

router = APIRouter(prefix="/api/payment", tags=["Payments"])

TON_PAY_ADDRESS = os.getenv("TON_PAY_ADDRESS", "").strip()
BSC_GENESIS_ADDRESS = os.getenv("BSC_GENESIS_ADDRESS", "").strip().lower()
BSCSCAN_API_KEY = os.getenv("BSCSCAN_API_KEY", "").strip()
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "").strip()
PREMIUM_MIN_BNB = float(os.getenv("PREMIUM_MIN_BNB", "0.0005"))  # ~$0.30 at BNB=$633
PREMIUM_MIN_TON = float(os.getenv("PREMIUM_MIN_TON", "0.01"))    # ~$0.014 at TON=$1.41
PREMIUM_MIN_ILS = float(os.getenv("PREMIUM_MIN_ILS", "1"))       # for testing — 1 ILS = symbolic

# Testnet mode — set PAYMENT_MODE=testnet in Railway to switch to TON Testnet + BSC Chapel.
# Default is "mainnet" — real money. Testnet = fake money, safe for QA/tutorials.
PAYMENT_MODE = os.getenv("PAYMENT_MODE", "mainnet").lower().strip()
IS_TESTNET = PAYMENT_MODE == "testnet"

if IS_TESTNET:
    # BSC Testnet (Chapel) — public RPCs, free tBNB from https://testnet.binance.org/faucet-smart
    BSC_GENESIS_ADDRESS = os.getenv("BSC_TESTNET_ADDRESS", BSC_GENESIS_ADDRESS).lower()
    # TON Testnet — https://testnet.toncenter.com
    # (TON_PAY_ADDRESS on testnet is a separate wallet — user sets via env)


# Pool is injected by main.py via set_pool()
_pool = None

# Crypto auto-settlement stays OFF until the canonical on-chain settlement
# contract is explicitly enabled after end-to-end verification.
CRYPTO_AUTO_VERIFY_ENABLED = os.getenv("CRYPTO_AUTO_VERIFY_ENABLED", "0") == "1"
BSC_EXPECTED_CHAIN_ID = int(os.getenv("BSC_CHAIN_ID", "97" if IS_TESTNET else "56"))
BSC_MIN_CONFIRMATIONS = int(os.getenv("BSC_MIN_CONFIRMATIONS", "15"))


def _require_user_owner(request: Request, user_id: int) -> None:
    from main import _require_owner
    _require_owner(
        user_id,
        request.headers.get("authorization"),
        request.headers.get("x-admin-key"),
        request.headers.get("x-slh-service-token"),
        allow_service=False,
    )


def _require_trusted_payment_writer(request: Request) -> None:
    from main import _is_service_principal, _require_admin
    service_token = request.headers.get("x-slh-service-token")
    if _is_service_principal(service_token):
        return
    _require_admin(
        request.headers.get("authorization"),
        request.headers.get("x-admin-key"),
    )


def set_pool(pool):
    global _pool
    _pool = pool
    # Create payment tables eagerly so GET endpoints don't 500 on fresh DBs.
    if pool is not None:
        import asyncio as _asyncio
        async def _bootstrap():
            try:
                async with pool.acquire() as conn:
                    await _ensure_payment_tables(conn)
            except Exception as e:
                import logging as _logging
                _logging.getLogger("payments_auto").warning(f"payment tables bootstrap failed: {e}")
        try:
            loop = _asyncio.get_event_loop()
            if loop.is_running():
                _asyncio.ensure_future(_bootstrap())
            else:
                loop.run_until_complete(_bootstrap())
        except RuntimeError:
            pass


async def _ensure_payment_tables(conn):
    """Idempotent table setup for external payments + receipts + geo."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS external_payments (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            provider TEXT NOT NULL,
            provider_tx_id TEXT UNIQUE,
            amount NUMERIC(18,4) NOT NULL,
            currency TEXT NOT NULL,
            status TEXT DEFAULT 'approved',
            country_code TEXT,
            ip_address TEXT,
            plan_key TEXT DEFAULT 'premium',
            bot_name TEXT DEFAULT 'ecosystem',
            metadata JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_ext_pay_user ON external_payments(user_id);
        CREATE INDEX IF NOT EXISTS idx_ext_pay_provider ON external_payments(provider);
        CREATE INDEX IF NOT EXISTS idx_ext_pay_country ON external_payments(country_code);

        CREATE TABLE IF NOT EXISTS payment_receipts (
            id BIGSERIAL PRIMARY KEY,
            receipt_number TEXT UNIQUE NOT NULL,
            user_id BIGINT NOT NULL,
            source_type TEXT NOT NULL,
            source_id BIGINT,
            amount NUMERIC(18,4) NOT NULL,
            currency TEXT NOT NULL,
            tokens_granted NUMERIC(18,6) DEFAULT 0,
            tokens_currency TEXT DEFAULT 'SLH',
            issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pdf_url TEXT,
            metadata JSONB DEFAULT '{}'::jsonb
        );
        CREATE INDEX IF NOT EXISTS idx_receipt_user ON payment_receipts(user_id);
        """
    )


async def _grant_premium(conn, user_id: int, bot_name: str, tx_hash: str, amount: float, currency: str, plan_key: str) -> dict:
    """Record deposit + flip premium to approved. Idempotent by tx_hash."""
    existing = await conn.fetchval("SELECT id FROM deposits WHERE tx_hash = $1", tx_hash)
    if existing:
        return {"already_processed": True, "deposit_id": existing}

    dep_id = await conn.fetchval(
        """
        INSERT INTO deposits (user_id, amount, currency, tx_hash, status, plan_key)
        VALUES ($1, $2, $3, $4, 'approved', $5) RETURNING id
        """,
        user_id, amount, currency, tx_hash, plan_key,
    )
    await conn.execute(
        """
        INSERT INTO premium_users (user_id, bot_name, payment_status)
        VALUES ($1, $2, 'approved')
        ON CONFLICT (user_id, bot_name) DO UPDATE SET payment_status = 'approved'
        """,
        user_id, bot_name,
    )
    return {"deposit_id": dep_id, "already_processed": False}


async def _issue_receipt(conn, user_id: int, source_type: str, source_id: int, amount: float, currency: str, tokens_granted: float = 0, tokens_currency: str = "SLH") -> dict:
    """Emit a digital receipt. receipt_number format: SLH-YYYYMMDD-{id}."""
    today = datetime.utcnow().strftime("%Y%m%d")
    row = await conn.fetchrow(
        """
        INSERT INTO payment_receipts (receipt_number, user_id, source_type, source_id, amount, currency, tokens_granted, tokens_currency)
        VALUES ('SLH-' || $1 || '-TMP-' || FLOOR(RANDOM()*1000000)::TEXT, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, receipt_number, issued_at
        """,
        today, user_id, source_type, source_id, amount, currency, tokens_granted, tokens_currency,
    )
    final_number = f"SLH-{today}-{row['id']:06d}"
    await conn.execute("UPDATE payment_receipts SET receipt_number = $1 WHERE id = $2", final_number, row["id"])

    # Record this as treasury revenue so it lands on /api/treasury/health.
    # Safe no-op on failure; never blocks a receipt.
    await _record_revenue(
        conn,
        source_type="payment_receipt",
        amount_gross=float(amount),
        currency=currency,
        source_id=int(row["id"]),
        user_id=user_id,
        metadata={
            "receipt_number": final_number,
            "origin_source_type": source_type,
            "origin_source_id": source_id,
            "tokens_granted": float(tokens_granted or 0),
            "tokens_currency": tokens_currency,
        },
    )

    return {
        "id": row["id"],
        "receipt_number": final_number,
        "issued_at": row["issued_at"].isoformat(),
    }


def _client_country(request: Request) -> Optional[str]:
    """Extract country code from standard proxy headers (Cloudflare, Railway, etc)."""
    for h in ("cf-ipcountry", "x-vercel-ip-country", "x-country-code"):
        v = request.headers.get(h)
        if v and v != "XX":
            return v.upper()
    return None


def _client_ip(request: Request) -> str:
    for h in ("cf-connecting-ip", "x-forwarded-for", "x-real-ip"):
        v = request.headers.get(h)
        if v:
            return v.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# ============================================================
# TON auto-verify
# ============================================================

class TonVerifyReq(BaseModel):
    user_id: int
    tx_hash: str
    expected_amount_ton: Optional[float] = None
    plan_key: str = "premium"
    bot_name: str = "ecosystem"
    issue_receipt: bool = True


@router.post("/ton/auto-verify")
async def ton_auto_verify(req: TonVerifyReq, request: Request):
    _require_user_owner(request, req.user_id)
    if not CRYPTO_AUTO_VERIFY_ENABLED:
        raise HTTPException(503, "Crypto auto-settlement is disabled until canonical settlement is enabled")
    if not TON_PAY_ADDRESS:
        raise HTTPException(503, "TON_PAY_ADDRESS env var not configured")

    tx_hash = (req.tx_hash or "").strip()
    if not tx_hash or len(tx_hash) < 40:
        raise HTTPException(400, "invalid tx_hash")

    expected = req.expected_amount_ton or PREMIUM_MIN_TON
    key_q = f"&api_key={TONCENTER_API_KEY}" if TONCENTER_API_KEY else ""
    url = f"https://toncenter.com/api/v2/getTransactions?address={TON_PAY_ADDRESS}&limit=50{key_q}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    raise HTTPException(502, f"toncenter returned {resp.status}")
                data = await resp.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(504, f"toncenter fetch failed: {e}")

    match = None
    for tx in data.get("result", []) or []:
        tid = tx.get("transaction_id", {}) or {}
        if tid.get("hash") == tx_hash:
            match = tx
            break
        in_msg = tx.get("in_msg", {}) or {}
        if in_msg.get("body_hash") == tx_hash or in_msg.get("hash") == tx_hash:
            match = tx
            break

    if not match:
        raise HTTPException(404, "TX not found on TON yet. Wait 30 seconds and retry.")

    in_msg = match.get("in_msg", {}) or {}
    amount_nano = int(in_msg.get("value", 0) or 0)
    amount_ton = amount_nano / 1e9

    if amount_ton < expected * 0.98:
        raise HTTPException(400, f"amount too low: got {amount_ton:.4f} TON, expected {expected} TON")

    if _pool is None:
        raise HTTPException(500, "db pool not initialized")

    async with _pool.acquire() as conn:
        await _ensure_payment_tables(conn)
        result = await _grant_premium(conn, req.user_id, req.bot_name, tx_hash, amount_ton, "TON", req.plan_key)
        receipt = None
        if req.issue_receipt and not result.get("already_processed"):
            receipt = await _issue_receipt(conn, req.user_id, "ton_deposit", result.get("deposit_id"), amount_ton, "TON")

    return {
        "ok": True,
        "verified": True,
        "amount_ton": amount_ton,
        "tx_hash": tx_hash,
        "country_code": _client_country(request),
        "receipt": receipt,
        **result,
        "message": "תשלום אומת ב-TON. Premium הופעל.",
    }


# ============================================================
# BSC auto-verify
# ============================================================

class BscVerifyReq(BaseModel):
    user_id: int
    tx_hash: str
    expected_min_bnb: Optional[float] = None
    plan_key: str = "premium"
    bot_name: str = "ecosystem"
    issue_receipt: bool = True


@router.post("/bsc/auto-verify")
async def bsc_auto_verify(req: BscVerifyReq, request: Request):
    _require_user_owner(request, req.user_id)
    if not CRYPTO_AUTO_VERIFY_ENABLED:
        raise HTTPException(503, "Crypto auto-settlement is disabled until canonical settlement is enabled")
    if not BSC_GENESIS_ADDRESS:
        raise HTTPException(503, "BSC settlement treasury is not configured")
    tx_hash = (req.tx_hash or "").strip()
    if not tx_hash.startswith("0x") or len(tx_hash) != 66:
        raise HTTPException(400, "invalid BSC tx_hash (expected 0x + 64 hex)")

    expected_min = req.expected_min_bnb or PREMIUM_MIN_BNB
    # BSC public RPC (free, no key needed).
    if IS_TESTNET:
        # BSC Testnet (Chapel) — free tBNB at https://testnet.binance.org/faucet-smart
        bsc_rpcs = [
            "https://data-seed-prebsc-1-s1.binance.org:8545",
            "https://data-seed-prebsc-2-s1.binance.org:8545",
            "https://bsc-testnet.public.blastapi.io",
        ]
    else:
        # Mainnet — Binance dataseed + fallbacks
        bsc_rpcs = [
            "https://bsc-dataseed.binance.org",
            "https://bsc-dataseed1.ninicoin.io",
            "https://bsc-dataseed2.defibit.io",
        ]

    async def _rpc_call(session, url, method, params):
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as r:
            return await r.json()

    tx_data = rc_data = None
    last_err = None
    try:
        async with aiohttp.ClientSession() as session:
            for rpc in bsc_rpcs:
                try:
                    tx_resp = await _rpc_call(session, rpc, "eth_getTransactionByHash", [tx_hash])
                    rc_resp = await _rpc_call(session, rpc, "eth_getTransactionReceipt", [tx_hash])
                    chain_resp = await _rpc_call(session, rpc, "eth_chainId", [])
                    latest_resp = await _rpc_call(session, rpc, "eth_blockNumber", [])
                    tx_data = {
                        "result": tx_resp.get("result"),
                        "chain_id": chain_resp.get("result"),
                        "latest_block": latest_resp.get("result"),
                    }
                    rc_data = {"result": rc_resp.get("result")}
                    break
                except Exception as e:
                    last_err = f"{rpc}: {e}"
                    continue
    except Exception as e:
        raise HTTPException(504, f"all BSC RPCs failed: {e}")
    if tx_data is None:
        raise HTTPException(504, f"all BSC RPCs failed. last: {last_err}")

    tx = tx_data.get("result") if isinstance(tx_data, dict) else None
    receipt_raw = rc_data.get("result") if isinstance(rc_data, dict) else None

    if isinstance(tx_data, dict) and tx_data.get("status") == "0":
        raise HTTPException(502, f"bscscan rate-limited: {tx_data.get('result') or tx_data.get('message')}")
    if not tx or not isinstance(tx, dict):
        raise HTTPException(404, "TX not found on BSC yet. Wait ~15s and retry.")
    if not receipt_raw or not isinstance(receipt_raw, dict):
        raise HTTPException(400, "TX not yet confirmed (no receipt)")
    if receipt_raw.get("status") != "0x1":
        raise HTTPException(400, "TX failed on-chain")

    chain_raw = tx_data.get("chain_id")
    try:
        chain_id = int(chain_raw, 16) if isinstance(chain_raw, str) else int(chain_raw or 0)
    except (TypeError, ValueError):
        raise HTTPException(502, "BSC RPC returned malformed chain id")
    if chain_id != BSC_EXPECTED_CHAIN_ID:
        raise HTTPException(400, f"wrong BSC chain id: got {chain_id}, expected {BSC_EXPECTED_CHAIN_ID}")

    try:
        receipt_block = int(receipt_raw.get("blockNumber", "0x0"), 16)
        latest_block = int(tx_data.get("latest_block", "0x0"), 16)
    except (TypeError, ValueError):
        raise HTTPException(502, "BSC RPC returned malformed block number")
    confirmations = max(0, latest_block - receipt_block + 1)
    if confirmations < BSC_MIN_CONFIRMATIONS:
        raise HTTPException(400, f"TX not sufficiently confirmed: {confirmations}/{BSC_MIN_CONFIRMATIONS}")

    to_addr = (tx.get("to") or "").lower()
    try:
        value_bnb = int(tx.get("value", "0x0"), 16) / 1e18
    except (ValueError, TypeError):
        raise HTTPException(502, "bscscan returned malformed value")

    if to_addr != BSC_GENESIS_ADDRESS:
        raise HTTPException(400, "TX recipient does not match the configured BSC settlement treasury")

    from_addr = (tx.get("from") or "").lower()
    async with _pool.acquire() as conn:
        bound_wallet = await conn.fetchval(
            "SELECT eth_wallet FROM web_users WHERE telegram_id=$1",
            req.user_id,
        )
    if not bound_wallet:
        raise HTTPException(409, "No verified BSC/ETH wallet is bound to this user")
    if from_addr != str(bound_wallet).lower():
        raise HTTPException(403, "TX sender does not match the user's bound wallet")

    BSC_ABS_TOLERANCE = 0.00002
    min_acceptable = max(0.0, expected_min - BSC_ABS_TOLERANCE)
    if value_bnb < min_acceptable:
        raise HTTPException(400, f"amount too low: got {value_bnb:.6f} BNB, expected >= {expected_min} BNB")

    if _pool is None:
        raise HTTPException(500, "db pool not initialized")

    async with _pool.acquire() as conn:
        await _ensure_payment_tables(conn)
        result = await _grant_premium(conn, req.user_id, req.bot_name, tx_hash, value_bnb, "BNB", req.plan_key)
        rcpt = None
        if req.issue_receipt and not result.get("already_processed"):
            rcpt = await _issue_receipt(conn, req.user_id, "bsc_deposit", result.get("deposit_id"), value_bnb, "BNB")

    return {
        "ok": True,
        "verified": True,
        "amount_bnb": value_bnb,
        "tx_hash": tx_hash,
        "country_code": _client_country(request),
        "receipt": rcpt,
        **result,
        "message": "תשלום אומת ב-BSC. Premium הופעל.",
    }


# ============================================================
# External providers (Stripe, PayBox, Bit, PayPal, GrowClub, ICount)
# Record-only — the actual card processing happens on the provider side,
# we just ingest the confirmed webhook/return and grant premium + issue receipt.
# ============================================================

class ExternalPaymentRecord(BaseModel):
    user_id: int
    provider: str
    provider_tx_id: str
    amount: float
    currency: str = "ILS"
    plan_key: str = "premium"
    bot_name: str = "ecosystem"
    country_code: Optional[str] = None
    issue_receipt: bool = True
    grant_premium: bool = True
    metadata: Optional[dict] = None


SUPPORTED_PROVIDERS = {"stripe", "paypal", "growclub", "icount", "isracard", "cardcom", "meshulam", "manual_bank", "ton_direct", "bsc_direct"}


@router.post("/external/record")
async def external_payment_record(req: ExternalPaymentRecord, request: Request):
    _require_trusted_payment_writer(request)
    provider = req.provider.lower().strip()
    if provider not in SUPPORTED_PROVIDERS:
        raise HTTPException(400, f"unsupported provider '{provider}'. allowed: {sorted(SUPPORTED_PROVIDERS)}")
    if not req.provider_tx_id.strip():
        raise HTTPException(400, "provider_tx_id required")

    if _pool is None:
        raise HTTPException(500, "db pool not initialized")

    country = req.country_code or _client_country(request)
    ip = _client_ip(request)

    async with _pool.acquire() as conn:
        await _ensure_payment_tables(conn)

        existing = await conn.fetchval(
            "SELECT id FROM external_payments WHERE provider = $1 AND provider_tx_id = $2",
            provider, req.provider_tx_id,
        )
        if existing:
            return {"ok": True, "already_processed": True, "external_payment_id": existing}

        ext_id = await conn.fetchval(
            """
            INSERT INTO external_payments
                (user_id, provider, provider_tx_id, amount, currency, country_code, ip_address, plan_key, bot_name, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            RETURNING id
            """,
            req.user_id, provider, req.provider_tx_id, req.amount, req.currency.upper(),
            country, ip, req.plan_key, req.bot_name, json.dumps(req.metadata or {}),
        )

        if req.grant_premium:
            await conn.execute(
                """
                INSERT INTO premium_users (user_id, bot_name, payment_status)
                VALUES ($1, $2, 'approved')
                ON CONFLICT (user_id, bot_name) DO UPDATE SET payment_status = 'approved'
                """,
                req.user_id, req.bot_name,
            )

        rcpt = None
        if req.issue_receipt:
            rcpt = await _issue_receipt(conn, req.user_id, f"external_{provider}", ext_id, req.amount, req.currency.upper())

    return {
        "ok": True,
        "external_payment_id": ext_id,
        "provider": provider,
        "country_code": country,
        "receipt": rcpt,
        "message": f"תשלום {provider} נרשם" + (" ו-Premium הופעל" if req.grant_premium else ""),
    }


# ============================================================
# Status + config + geography dashboard
# ============================================================

@router.get("/status/{user_id}")
async def payment_status(user_id: int, bot_name: str = "ecosystem", request: Request = None):
    if request is None:
        raise HTTPException(401, "Authentication required")
    _require_user_owner(request, user_id)
    if _pool is None:
        raise HTTPException(500, "db pool not initialized")
    async with _pool.acquire() as conn:
        await _ensure_payment_tables(conn)
        prem = await conn.fetchrow(
            """
            SELECT payment_status, created_at FROM premium_users
            WHERE user_id = $1 AND bot_name = $2
            ORDER BY created_at DESC LIMIT 1
            """,
            user_id, bot_name,
        )
        # NOTE: deposits schema uses `token` (not currency) and `confirmed_at` (not created_at).
        # Aliased here so the response shape stays the same for bot clients
        # (academia-bot _check_status, etc.). Bug fix 2026-04-21: endpoint was 500ing on every
        # call because of column mismatch — which killed the ACAD payment verification flow
        # for user 8789977826 (4x ILS 49 paid, 0 licenses granted).
        last_dep = await conn.fetchrow(
            "SELECT id, amount, token AS currency, tx_hash, status, confirmed_at AS created_at "
            "FROM deposits WHERE user_id = $1 ORDER BY id DESC LIMIT 1",
            user_id,
        )
        last_ext = await conn.fetchrow(
            "SELECT id, provider, amount, currency, status, country_code, created_at FROM external_payments WHERE user_id = $1 ORDER BY id DESC LIMIT 1",
            user_id,
        )
        receipts = await conn.fetch(
            "SELECT id, receipt_number, amount, currency, issued_at FROM payment_receipts WHERE user_id = $1 ORDER BY id DESC LIMIT 10",
            user_id,
        )

    return {
        "user_id": user_id,
        "bot_name": bot_name,
        "has_premium": bool(prem and prem["payment_status"] == "approved"),
        "status": prem["payment_status"] if prem else "none",
        "approved_at": prem["created_at"].isoformat() if prem and prem["created_at"] else None,
        "last_deposit": dict(last_dep) if last_dep else None,
        "last_external": dict(last_ext) if last_ext else None,
        "receipts": [dict(r) for r in receipts],
    }


@router.get("/config")
async def payment_config():
    return {
        "payment_mode": PAYMENT_MODE,
        "is_testnet": IS_TESTNET,
        "ton_address": TON_PAY_ADDRESS or None,
        "bsc_genesis_address": BSC_GENESIS_ADDRESS,
        "premium_min_bnb": PREMIUM_MIN_BNB,
        "premium_min_ton": PREMIUM_MIN_TON,
        "premium_min_ils": PREMIUM_MIN_ILS,
        "bscscan_configured": bool(BSCSCAN_API_KEY),
        "toncenter_configured": bool(TON_PAY_ADDRESS),
        "supported_providers": sorted(SUPPORTED_PROVIDERS),
        # Estimated gas + micro-transaction info (for UI to show "total cost")
        "gas_estimate": {
            "ton_network_fee": 0.005,  # typical TON transfer fee
            "bsc_network_fee_bnb": 0.0003,  # ~21000 gas × 5 gwei
            "bsc_usd_equivalent": 0.20,  # rough
        },
        "test_tx_note": (
            "לבדיקת flow ראשונה מומלץ: 0.01 TON (כולל עמלה ~0.005 TON) "
            "או 0.001 BNB (~$0.63). אחרי שהtest עובד, פרמייד רגיל מ-0.5 TON / 0.05 BNB."
        ),
    }


@router.get("/geography/summary")
async def payment_geography(x_admin_key: Optional[str] = Header(None)):
    """Admin: breakdown of payments by country + currency + provider."""
    if _pool is None:
        raise HTTPException(500, "db pool not initialized")
    admin_keys = [k.strip() for k in os.getenv("ADMIN_API_KEYS", "").split(",") if k.strip()]
    if x_admin_key not in admin_keys:
        raise HTTPException(403, "admin key required")

    async with _pool.acquire() as conn:
        await _ensure_payment_tables(conn)
        by_country = await conn.fetch(
            """
            SELECT COALESCE(country_code, 'UNKNOWN') AS country,
                   COUNT(*) AS count,
                   SUM(amount) AS total_amount,
                   currency
            FROM external_payments GROUP BY country, currency ORDER BY total_amount DESC LIMIT 100
            """
        )
        by_provider = await conn.fetch(
            """
            SELECT provider, COUNT(*) AS count, SUM(amount) AS total_amount, currency
            FROM external_payments GROUP BY provider, currency ORDER BY total_amount DESC
            """
        )
        by_crypto = await conn.fetch(
            """
            SELECT currency, COUNT(*) AS count, SUM(amount) AS total_amount
            FROM deposits WHERE status = 'approved' AND tx_hash IS NOT NULL
            GROUP BY currency ORDER BY total_amount DESC
            """
        )
        totals = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM external_payments) AS external_count,
                (SELECT COUNT(*) FROM deposits WHERE status='approved' AND tx_hash IS NOT NULL) AS crypto_count,
                (SELECT COUNT(*) FROM payment_receipts) AS receipts_count,
                (SELECT COUNT(*) FROM premium_users WHERE payment_status='approved') AS premium_active
            """
        )

    return {
        "totals": dict(totals) if totals else {},
        "by_country": [dict(r) for r in by_country],
        "by_provider": [dict(r) for r in by_provider],
        "by_crypto": [dict(r) for r in by_crypto],
    }


@router.get("/receipts/{user_id}")
async def user_receipts(user_id: int, limit: int = 50, request: Request = None):
    if request is None:
        raise HTTPException(401, "Authentication required")
    _require_user_owner(request, user_id)
    if _pool is None:
        raise HTTPException(500, "db pool not initialized")
    async with _pool.acquire() as conn:
        await _ensure_payment_tables(conn)
        rows = await conn.fetch(
            """
            SELECT id, receipt_number, source_type, source_id, amount, currency,
                   tokens_granted, tokens_currency, issued_at, metadata
            FROM payment_receipts WHERE user_id = $1
            ORDER BY id DESC LIMIT $2
            """,
            user_id, max(1, min(limit, 500)),
        )
    return {"user_id": user_id, "receipts": [dict(r) for r in rows]}
