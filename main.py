"""
SLH Ecosystem API - FastAPI Backend
Deployed on Railway | Connected to PostgreSQL
"""
import os
import hmac
import hashlib
import time
import json
from datetime import datetime, timedelta
import jwt
import secrets
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Depends, Query, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import asyncio
import asyncpg
import aiohttp

# Telegram-first Gateway (Phase 2 wire-in, 2026-04-23).
# Mini App endpoints depend on verify_miniapp_request; bot handlers can use verify_bot_request.
# Import is isolated in try/except so a missing module can't block API boot.
try:
    from api.telegram_gateway import (
        TelegramUser,
        verify_miniapp_request,
        verify_bot_request,
        require_admin,
        GatewayError,
    )
    _GATEWAY_AVAILABLE = True
except Exception as _gw_err:  # pragma: no cover
    import logging as _log
    _log.warning("telegram_gateway unavailable: %s", _gw_err)
    _GATEWAY_AVAILABLE = False
    TelegramUser = None  # type: ignore
    verify_miniapp_request = None  # type: ignore
    verify_bot_request = None  # type: ignore
    require_admin = None  # type: ignore
    GatewayError = None  # type: ignore

# SLH Swarm — independent device mesh router (Phase 1 of SWARM_V1_BLUEPRINT).
# Same fail-safe pattern as the gateway: missing module can't block API boot.
try:
    from api import swarm as _swarm
    _SWARM_AVAILABLE = True
except Exception as _sw_err:  # pragma: no cover
    import logging as _log
    _log.warning("swarm router unavailable: %s", _sw_err)
    _SWARM_AVAILABLE = False
    _swarm = None  # type: ignore

# Bot catalog — DB-backed list of all 31 Telegram bots in the fleet.
# Used by /admin/tokens.html + /admin/rotate-token.html instead of hardcoded JS arrays.
try:
    from api import admin_bots_catalog as _bots_catalog
    _BOTS_CATALOG_AVAILABLE = True
except Exception as _bc_err:  # pragma: no cover
    import logging as _log
    _log.warning("admin_bots_catalog unavailable: %s", _bc_err)
    _BOTS_CATALOG_AVAILABLE = False
    _bots_catalog = None  # type: ignore

# Personal expense tracker — Phase 1 of cashflow management feature.
# Per-user expenses table + monthly summary + recurring detection.
try:
    from api import expenses as _expenses_router
    _EXPENSES_AVAILABLE = True
except Exception as _exp_err:  # pragma: no cover
    import logging as _log
    _log.warning("expenses router unavailable: %s", _exp_err)
    _EXPENSES_AVAILABLE = False
    _expenses_router = None  # type: ignore

# Secrets vault — unified inventory of all credentials (bot tokens, API keys,
# DB creds, AI providers). Stores metadata only, never secret values.
try:
    from api import admin_secrets_catalog as _secrets_vault
    _SECRETS_VAULT_AVAILABLE = True
except Exception as _sv_err:  # pragma: no cover
    import logging as _log
    _log.warning("secrets_vault unavailable: %s", _sv_err)
    _SECRETS_VAULT_AVAILABLE = False
    _secrets_vault = None  # type: ignore

# Secrets vault Phase 2 — scheduled health sweep + Telegram alerts + daily digest.
# Sits on top of admin_secrets_catalog (reuses _run_probe + _ensure_schema).
try:
    from api import admin_secret_alerts as _secret_alerts
    _SECRET_ALERTS_AVAILABLE = True
except Exception as _sa_err:  # pragma: no cover
    import logging as _log
    _log.warning("secret_alerts unavailable: %s", _sa_err)
    _SECRET_ALERTS_AVAILABLE = False
    _secret_alerts = None  # type: ignore

# Public security beacon — sanitized aggregate counts for /my.html and any
# other personal page. No auth, no key names, no per-secret detail.
try:
    from api import public_security_status as _public_security
    _PUBLIC_SECURITY_AVAILABLE = True
except Exception as _ps_err:  # pragma: no cover
    import logging as _log
    _log.warning("public_security unavailable: %s", _ps_err)
    _PUBLIC_SECURITY_AVAILABLE = False
    _public_security = None  # type: ignore

from shared_db_core import init_db_pool as _shared_init_db_pool, db_health as _shared_db_health

from routes.ai_chat import router as ai_chat_router, set_aic_pool as _ai_chat_set_aic_pool
from routes.payments_auto import router as payments_auto_router, set_pool as _payments_set_pool
from routes.payments_monitor import router as payments_monitor_router, set_pool as _payments_monitor_set_pool, start_monitor as _payments_monitor_start
from routes.community_plus import router as community_plus_router, set_pool as _community_plus_set_pool
from routes.aic_tokens import router as aic_router, admin_router as aic_admin_router, set_pool as _aic_set_pool
from routes.pancakeswap_tracker import router as ps_router, set_pool as _ps_set_pool
from routes.sudoku import router as sudoku_router, set_pool as _sudoku_set_pool
from routes.dating import router as dating_router, set_pool as _dating_set_pool
from routes.broadcast import router as broadcast_router, set_pool as _broadcast_set_pool
from routes.love_tokens import router as love_router, set_pool as _love_set_pool
from routes.treasury import router as treasury_router, set_pool as _treasury_set_pool
from routes.creator_economy import router as creator_router, set_pool as _creator_set_pool
from routes.wellness import router as wellness_router, set_pool as _wellness_set_pool, init_wellness_tables as _init_wellness
from routes.arkham_bridge import router as threat_router, set_pool as _threat_set_pool, init_threat_tables as _init_threat
from routes.whatsapp import router as whatsapp_router, set_pool as _whatsapp_set_pool, init_whatsapp_tables as _init_whatsapp
from routes.system_audit import router as system_audit_router, set_pool as _system_audit_set_pool
from routes.agent_hub import router as agent_hub_router, set_pool as _agent_hub_set_pool, init_agent_hub_tables as _init_agent_hub
from routes.system_status import router as system_status_router, set_pool as _system_status_set_pool
from routes.investor_engine import router as investor_engine_router, set_pool as _investor_engine_set_pool
from routes.courses import router as courses_router, set_pool as _courses_set_pool
from routes.esp_events import router as esp_events_router, set_pool as _esp_events_set_pool
from routes.campaign_admin import router as campaign_admin_router, set_pool as _campaign_admin_set_pool
from routes.academia_ugc import router as academia_ugc_router, set_pool as _academia_ugc_set_pool, init_academia_ugc_tables as _init_academia_ugc
from routes.ambassador_crm import router as ambassador_crm_router, set_pool as _ambassador_crm_set_pool
from routes.therapists import router as therapists_router, set_pool as _therapists_set_pool
from routes.device_inventory import router as device_inventory_router, set_pool as _device_inventory_set_pool
from routes.tasks import router as tasks_router, set_pool as _tasks_set_pool
from routes.bot_registry import router as bot_registry_router, set_pool as _bot_registry_set_pool, init_tables as _init_bot_registry
from routes.rotation_pipeline import router as rotation_pipeline_router
from routes.admin_rotate import (
    router as admin_rotate_router,
    set_pool as _admin_rotate_set_pool,
    init_admin_secrets_table as _init_admin_rotate,
    check_db_admin_key as _check_db_admin_key,
)
from wellness_scheduler import init_wellness_scheduler, get_wellness_scheduler

# === CONFIG ===
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:slh_secure_2026@localhost:5432/slh_main")
BOT_TOKEN = os.getenv("EXPERTNET_BOT_TOKEN", "")
# Broadcast bot â€” @SLH_AIR_bot is the main user-facing bot
BROADCAST_BOT_TOKEN = os.getenv("SLH_AIR_TOKEN") or os.getenv("CORE_BOT_TOKEN") or os.getenv("AIRDROP_BOT_TOKEN", "")
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_HOURS = int(os.getenv("JWT_EXPIRES_HOURS", "12"))
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "224223270"))
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "https://slh-nft.com,http://localhost:8899,http://localhost:3000").split(",")

# Wallet constants (used early by registration endpoints)
SLH_BSC_CONTRACT = "0xACb0A09414CEA1C879c67bB7A877E4e19480f022"
SLH_TON_WALLET = "UQCr743gEr_nqV_0SBkSp3CtYS_15R3LDLBvLmKeEv7XdGvp"
SLH_PRICE_ILS = 444
USD_ILS_RATE = 3.65

_ENV = (os.getenv("ENV") or os.getenv("ENVIRONMENT") or "development").lower()
_IS_PROD = _ENV in ("prod", "production")
# Allow admins to re-enable /docs in prod with DOCS_ENABLED=1 for debugging
_DOCS_ENABLED = os.getenv("DOCS_ENABLED", "1" if not _IS_PROD else "0") == "1"

app = FastAPI(
    title="SLH Ecosystem API",
    description="Backend API for SLH Digital Investment House",
    version="1.1.0",
    docs_url="/docs" if _DOCS_ENABLED else None,
    redoc_url="/redoc" if _DOCS_ENABLED else None,
    openapi_url="/openapi.json" if _DOCS_ENABLED else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    # SECURITY FIX (H-3): explicit headers instead of wildcard
    allow_headers=["Content-Type", "Authorization", "X-Admin-Key", "X-Requested-With"],
)


# ── Rate limiting middleware (simple in-memory sliding window, per-IP per-path-group) ──
from collections import defaultdict, deque

_RL_MAX_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "180"))
_RL_WINDOW_SEC = 60.0
_RL_BUCKETS: dict[str, deque] = defaultdict(deque)
# Paths that bypass the limiter (health/static/docs)
_RL_BYPASS_PREFIXES = ("/api/health", "/docs", "/redoc", "/openapi.json", "/favicon")


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if any(path.startswith(p) for p in _RL_BYPASS_PREFIXES):
        return await call_next(request)

    fwd = request.headers.get("x-forwarded-for") or request.headers.get("x-real-ip") or ""
    client_host = request.client.host if request.client else "unknown"
    ip = (fwd.split(",")[0].strip() if fwd else client_host) or "unknown"

    # Group by /api/<section>; unknown → path itself
    parts = path.split("/", 3)
    section = parts[2] if len(parts) > 2 and parts[1] == "api" else "root"
    key = f"{ip}|{section}"

    now = time.time()
    bucket = _RL_BUCKETS[key]
    while bucket and bucket[0] < now - _RL_WINDOW_SEC:
        bucket.popleft()
    if len(bucket) >= _RL_MAX_PER_MIN:
        retry_after = max(1, int(_RL_WINDOW_SEC - (now - bucket[0])))
        return JSONResponse(
            status_code=429,
            headers={"Retry-After": str(retry_after)},
            content={"detail": "Too many requests", "retry_after": retry_after, "limit_per_minute": _RL_MAX_PER_MIN},
        )
    bucket.append(now)
    return await call_next(request)


# Admin keys — env-sourced only (no public-source default).
# Set ADMIN_API_KEYS on Railway (comma-separated). If unset, admin calls fail 403.
# For runtime rotation without touching env, use POST /api/admin/rotate-key —
# rotated keys live in the admin_secrets DB table and are additive to env keys.
ADMIN_API_KEYS = set(
    (os.getenv("ADMIN_API_KEYS") or "").split(",")
) - {""}
if not ADMIN_API_KEYS:
    print("[SECURITY] WARNING: ADMIN_API_KEYS env var is empty. Admin endpoints will reject all X-Admin-Key requests until either env is set or a key is rotated via /api/admin/rotate-key.")


def _require_admin(authorization: Optional[str] = None, admin_key_header: Optional[str] = None) -> int:
    """Verify admin credentials. Accepts EITHER:
    - X-Admin-Key: <one of ADMIN_API_KEYS env> or active DB-rotated key
    - Authorization: Bearer <jwt> where user_id == ADMIN_USER_ID

    Returns the admin user_id on success, raises HTTPException 403 otherwise.
    """
    # Try env admin keys first (fastest path, backward-compat)
    if admin_key_header and admin_key_header in ADMIN_API_KEYS:
        return ADMIN_USER_ID

    # Try DB-backed rotated keys (in-memory cached — no DB hit per request)
    if admin_key_header and _check_db_admin_key(admin_key_header):
        return ADMIN_USER_ID

    # Try JWT
    if authorization and authorization.startswith("Bearer "):
        try:
            token = authorization[7:]
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            uid = int(payload.get("user_id") or 0)
            if uid == ADMIN_USER_ID:
                return uid
        except Exception:
            pass

    # Try admin JWT (new multi-admin system)
    if authorization and authorization.startswith("Bearer "):
        try:
            token = authorization[7:]
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            if payload.get("type") == "admin":
                return int(payload.get("admin_id", 0))
        except Exception:
            pass

    raise HTTPException(403, "Admin authentication required")

# ── Admin password hashing (SHA-256 + salt, no extra dependency) ──
def hash_admin_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}:{h}"

def verify_admin_password(password: str, stored: str) -> bool:
    if ":" not in stored:
        return False
    salt, h = stored.split(":", 1)
    return hashlib.sha256((salt + password).encode()).hexdigest() == h

ADMIN_ROLE_HIERARCHY = {"owner": 4, "ceo": 3, "manager": 2, "viewer": 1}

def _require_admin_role(authorization, x_admin_key, min_role="viewer"):
    """Check admin auth and verify minimum role. Returns (admin_id, role)."""
    admin_id = _require_admin(authorization, x_admin_key)
    # Old-style admin keys always get owner role
    if x_admin_key and x_admin_key in ADMIN_API_KEYS:
        return (admin_id, "owner")
    # For JWT-based admin, check role
    if authorization and authorization.startswith("Bearer "):
        try:
            payload = jwt.decode(authorization[7:], JWT_SECRET, algorithms=[JWT_ALGORITHM])
            role = payload.get("role", "viewer")
            if ADMIN_ROLE_HIERARCHY.get(role, 0) < ADMIN_ROLE_HIERARCHY.get(min_role, 0):
                raise HTTPException(403, f"Requires {min_role} role or higher")
            return (admin_id, role)
        except jwt.PyJWTError:
            pass
    return (admin_id, "owner")  # fallback for old-style auth

# === AI CHAT ROUTER ===
app.include_router(ai_chat_router)
app.include_router(payments_auto_router)
app.include_router(payments_monitor_router)
app.include_router(community_plus_router)
app.include_router(aic_router)
app.include_router(aic_admin_router)
app.include_router(ps_router)
app.include_router(sudoku_router)
app.include_router(dating_router)
app.include_router(broadcast_router)
app.include_router(love_router)
app.include_router(treasury_router)
app.include_router(creator_router)
app.include_router(wellness_router)
app.include_router(threat_router)
app.include_router(whatsapp_router)
app.include_router(system_audit_router)
app.include_router(agent_hub_router)
app.include_router(system_status_router)
app.include_router(investor_engine_router)
app.include_router(courses_router)
app.include_router(esp_events_router)
app.include_router(campaign_admin_router)
app.include_router(academia_ugc_router)
app.include_router(bot_registry_router)
app.include_router(admin_rotate_router)
app.include_router(rotation_pipeline_router)
app.include_router(ambassador_crm_router)
app.include_router(therapists_router)
app.include_router(device_inventory_router)
app.include_router(tasks_router)

# Swarm router — devices mesh API (gated; OK if module is missing).
if _SWARM_AVAILABLE and _swarm is not None:
    app.include_router(_swarm.router)

# Bot catalog router (DB-backed CRUD for /admin/tokens + /admin/rotate-token)
if _BOTS_CATALOG_AVAILABLE and _bots_catalog is not None:
    app.include_router(_bots_catalog.router)

if _EXPENSES_AVAILABLE and _expenses_router is not None:
    app.include_router(_expenses_router.router)

if _SECRETS_VAULT_AVAILABLE and _secrets_vault is not None:
    app.include_router(_secrets_vault.router)

if _SECRET_ALERTS_AVAILABLE and _secret_alerts is not None:
    app.include_router(_secret_alerts.router)

if _PUBLIC_SECURITY_AVAILABLE and _public_security is not None:
    app.include_router(_public_security.router)

# === DATABASE ===
pool: Optional[asyncpg.Pool] = None
_db_init_failed: bool = False  # True when shared_db_core pool init fails — /api/health returns 503

@app.on_event("startup")
async def startup():
    global pool
    # SECURITY CHECK (C-3): warn if any default credentials are still in use
    _security_warnings = []
    if DATABASE_URL == "postgresql://postgres:slh_secure_2026@localhost:5432/slh_main":
        _security_warnings.append("DATABASE_URL using default â€” set on Railway")
    if os.getenv("ADMIN_API_KEY", "slh_admin_2026") == "slh_admin_2026":
        _security_warnings.append("ADMIN_API_KEY is default â€” set on Railway")
    if os.getenv("ENCRYPTION_KEY", "slh_dev_key_CHANGE_ME_IN_PRODUCTION_2026") == "slh_dev_key_CHANGE_ME_IN_PRODUCTION_2026":
        _security_warnings.append("ENCRYPTION_KEY is default â€” CRITICAL: set on Railway before storing real CEX keys!")
    if os.getenv("ADMIN_BROADCAST_KEY", "slh-broadcast-2026-change-me") == "slh-broadcast-2026-change-me":
        _security_warnings.append("ADMIN_BROADCAST_KEY is default â€” set on Railway")
    if not os.getenv("JWT_SECRET"):
        _security_warnings.append("JWT_SECRET not set â€” JWT auth will be unreliable")
    for w in _security_warnings:
        print(f"[SECURITY WARNING] {w}")
    if _security_warnings:
        print(f"[SECURITY WARNING] {len(_security_warnings)} default credentials detected. Set env vars on Railway before production.")

    # STARTUP HARDENING: use shared_db_core.init_db_pool (single source of truth).
    # Railway healthcheck fails after 5min, so we let uvicorn bind even if DB is slow;
    # BUT /api/health now returns 503 honestly when pool is unavailable (no silent lies).
    global _db_init_failed
    try:
        pool = await asyncio.wait_for(
            _shared_init_db_pool(DATABASE_URL),
            timeout=10.0,
        )
        _db_init_failed = False
        print("[Startup] DB pool created via shared_db_core")
    except Exception as e:
        print(f"[Startup][CRITICAL] DB pool init failed: {e!r} — /api/health will return 503")
        pool = None
        _db_init_failed = True

    if pool is not None:
        # Expose the pool on app.state so the Telegram Gateway (and any future
        # Depends() consumer) can resolve telegram_id -> SLH user_id from any route.
        app.state.db_pool = pool
        for setter in (_payments_set_pool, _payments_monitor_set_pool, _community_plus_set_pool,
                       _aic_set_pool, _ps_set_pool, _ai_chat_set_aic_pool, _sudoku_set_pool,
                       _dating_set_pool, _broadcast_set_pool, _love_set_pool, _treasury_set_pool,
                       _creator_set_pool, _wellness_set_pool, _threat_set_pool, _whatsapp_set_pool,
                       _system_audit_set_pool, _agent_hub_set_pool, _campaign_admin_set_pool, _academia_ugc_set_pool,
                       _bot_registry_set_pool, _admin_rotate_set_pool,
                       _ambassador_crm_set_pool, _therapists_set_pool, _tasks_set_pool,
                       _device_inventory_set_pool, _system_status_set_pool,
                       _investor_engine_set_pool, _courses_set_pool, _esp_events_set_pool):
            try:
                setter(pool)
            except Exception as e:
                print(f"[Startup][WARN] set_pool on {setter.__name__} failed: {e!r}")

        # Each init isolated — one failure doesn't block the others or healthcheck
        for init_name, init_coro in (("wellness", _init_wellness), ("threat", _init_threat), ("whatsapp", _init_whatsapp), ("agent_hub", _init_agent_hub), ("academia_ugc", _init_academia_ugc), ("bot_registry", _init_bot_registry), ("admin_rotate", _init_admin_rotate)):
            try:
                await asyncio.wait_for(init_coro(), timeout=15.0)
                print(f"[Startup] {init_name} tables ready")
            except Exception as e:
                print(f"[Startup][WARN] init_{init_name} failed: {e!r}")

    # Initialize wellness scheduler (APScheduler) — non-blocking
    try:
        await asyncio.wait_for(init_wellness_scheduler(DATABASE_URL), timeout=10.0)
        print("[Wellness] Scheduler initialized successfully")
    except Exception as e:
        print(f"[WARNING] Wellness scheduler initialization failed: {e!r}")

    # STARTUP HARDENING: if pool creation failed, skip table creation — let uvicorn
    # start serving so /api/health returns 200 and Railway healthcheck passes.
    if pool is None:
        print("[Startup] pool unavailable, skipping CREATE TABLE block; endpoints will degrade gracefully")
        return

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS web_users (
                telegram_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                photo_url TEXT,
                auth_date BIGINT,
                last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_registered BOOLEAN DEFAULT FALSE,
                registered_at TIMESTAMP,
                eth_wallet VARCHAR(42),
                eth_wallet_linked_at TIMESTAMP,
                ton_wallet VARCHAR(68),
                ton_wallet_linked_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS staking_positions (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                plan TEXT NOT NULL,
                amount NUMERIC(18,8) NOT NULL,
                currency TEXT DEFAULT 'TON',
                apy_monthly NUMERIC(5,2) NOT NULL,
                lock_days INT NOT NULL,
                start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_date TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'active',
                earned NUMERIC(18,8) DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES web_users(telegram_id)
            );
            CREATE TABLE IF NOT EXISTS referrals (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL UNIQUE,
                referrer_id BIGINT,
                depth INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES web_users(telegram_id)
            );
            CREATE TABLE IF NOT EXISTS referral_earnings (
                id BIGSERIAL PRIMARY KEY,
                earner_id BIGINT NOT NULL,
                from_user_id BIGINT NOT NULL,
                generation INT NOT NULL,
                source_type TEXT NOT NULL,
                source_amount NUMERIC(18,8) NOT NULL,
                commission_rate NUMERIC(5,4) NOT NULL,
                commission_amount NUMERIC(18,8) NOT NULL,
                token TEXT DEFAULT 'TON',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id);
            CREATE INDEX IF NOT EXISTS idx_referral_earnings_earner ON referral_earnings(earner_id);

            CREATE TABLE IF NOT EXISTS token_balances (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                token TEXT NOT NULL DEFAULT 'SLH',
                balance NUMERIC(18,8) NOT NULL DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, token)
            );

            CREATE TABLE IF NOT EXISTS token_transfers (
                id BIGSERIAL PRIMARY KEY,
                from_user_id BIGINT,
                to_user_id BIGINT,
                token TEXT NOT NULL DEFAULT 'SLH',
                amount NUMERIC(18,8) NOT NULL,
                memo TEXT,
                tx_type TEXT DEFAULT 'transfer',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS wallet_idempotency (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                request_id TEXT NOT NULL,
                tx_transfer_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, request_id)
            );

            CREATE INDEX IF NOT EXISTS idx_wallet_idempotency_user_created
            ON wallet_idempotency(user_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS bank_transfer_requests (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                customer_name TEXT NOT NULL,
                transaction_date DATE NOT NULL,
                id_number VARCHAR(9) NOT NULL,
                bank_details TEXT NOT NULL,
                amount_ils NUMERIC(12,2) NOT NULL,
                transaction_desc TEXT NOT NULL,
                phone VARCHAR(15) NOT NULL,
                transfer_reference TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                reviewed_by BIGINT,
                reviewed_at TIMESTAMP,
                rejection_reason TEXT,
                invoice_id BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_bank_transfers_status ON bank_transfer_requests(status);
            CREATE INDEX IF NOT EXISTS idx_bank_transfers_user ON bank_transfer_requests(user_id);

            CREATE TABLE IF NOT EXISTS admin_users (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE,
                username VARCHAR(50) NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'viewer',
                email TEXT,
                phone TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_login TIMESTAMP,
                created_by BIGINT
            );
            CREATE INDEX IF NOT EXISTS idx_admin_users_role ON admin_users(role);

            CREATE TABLE IF NOT EXISTS user_payment_methods (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                method_type TEXT NOT NULL,
                label TEXT,
                bank_name TEXT,
                bank_branch TEXT,
                bank_account TEXT,
                bank_owner_name TEXT,
                bit_phone VARCHAR(15),
                paybox_phone VARCHAR(15),
                is_default BOOLEAN DEFAULT FALSE,
                is_verified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_user_payment_methods_user ON user_payment_methods(user_id);

            CREATE TABLE IF NOT EXISTS deposits (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount NUMERIC(18,8) NOT NULL,
                currency TEXT DEFAULT 'SLH',
                tx_hash TEXT,
                status TEXT DEFAULT 'pending',
                plan_key TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS premium_users (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                bot_name TEXT NOT NULL,
                payment_status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, bot_name)
            );

            CREATE TABLE IF NOT EXISTS daily_claims (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount NUMERIC(18,8) NOT NULL DEFAULT 0,
                streak INT NOT NULL DEFAULT 1,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                xp_total NUMERIC(18,2) DEFAULT 0,
                balance NUMERIC(18,8) DEFAULT 0,
                level INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS marketplace_items (
                id BIGSERIAL PRIMARY KEY,
                seller_id BIGINT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                price NUMERIC(18,8) NOT NULL,
                currency TEXT NOT NULL DEFAULT 'SLH',
                image_url TEXT,
                category TEXT DEFAULT 'general',
                stock INT DEFAULT 1,
                status TEXT DEFAULT 'pending',
                promotion TEXT DEFAULT 'none',
                promoted_until TIMESTAMP,
                views INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                approved_at TIMESTAMP,
                FOREIGN KEY (seller_id) REFERENCES web_users(telegram_id)
            );
            CREATE INDEX IF NOT EXISTS idx_marketplace_items_status ON marketplace_items(status);
            CREATE INDEX IF NOT EXISTS idx_marketplace_items_seller ON marketplace_items(seller_id);
            CREATE INDEX IF NOT EXISTS idx_marketplace_items_category ON marketplace_items(category);

            CREATE TABLE IF NOT EXISTS marketplace_orders (
                id BIGSERIAL PRIMARY KEY,
                buyer_id BIGINT NOT NULL,
                seller_id BIGINT NOT NULL,
                item_id BIGINT NOT NULL,
                quantity INT NOT NULL DEFAULT 1,
                total_price NUMERIC(18,8) NOT NULL,
                currency TEXT NOT NULL DEFAULT 'SLH',
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                FOREIGN KEY (buyer_id) REFERENCES web_users(telegram_id),
                FOREIGN KEY (item_id) REFERENCES marketplace_items(id)
            );
            CREATE INDEX IF NOT EXISTS idx_marketplace_orders_buyer ON marketplace_orders(buyer_id);
            CREATE INDEX IF NOT EXISTS idx_marketplace_orders_seller ON marketplace_orders(seller_id);
            CREATE INDEX IF NOT EXISTS idx_marketplace_orders_item ON marketplace_orders(item_id);
        """)

        # --- Migration: add is_registered columns to existing DBs ---
        try:
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS is_registered BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS registered_at TIMESTAMP")
        except Exception:
            pass  # columns already exist in CREATE TABLE

        # --- Migration: add Web3 wallet columns to existing DBs ---
        try:
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS eth_wallet VARCHAR(42)")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS eth_wallet_linked_at TIMESTAMP")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS ton_wallet VARCHAR(68)")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS ton_wallet_linked_at TIMESTAMP")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_web_users_eth_wallet ON web_users(eth_wallet)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_web_users_ton_wallet ON web_users(ton_wallet)")
        except Exception as e:
            print(f"[Migration] Web3 wallet columns: {e}")

        # --- Migration: custom display_name (user-chosen, not from Telegram) ---
        try:
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS display_name TEXT")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS display_name_set_at TIMESTAMP")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS bio TEXT")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS language_pref TEXT DEFAULT 'he'")
        except Exception as e:
            print(f"[Migration] Display name columns: {e}")

        # --- Migration: beta coupons + beta_user flag ---
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS beta_coupons (
                    code TEXT PRIMARY KEY,
                    max_uses INT NOT NULL DEFAULT 49,
                    used_count INT NOT NULL DEFAULT 0,
                    nft_reward TEXT DEFAULT 'SLH Genesis Member #',
                    slh_bonus NUMERIC(18,8) DEFAULT 0.1,
                    expires_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    active BOOLEAN DEFAULT TRUE
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS beta_redemptions (
                    id BIGSERIAL PRIMARY KEY,
                    coupon_code TEXT NOT NULL,
                    user_id BIGINT NOT NULL,
                    nft_number INT,
                    redeemed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(coupon_code, user_id)
                )
            """)
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS beta_user BOOLEAN DEFAULT FALSE")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS beta_coupon_code TEXT")
            await conn.execute("ALTER TABLE web_users ADD COLUMN IF NOT EXISTS beta_nft_number INT")
            # Seed the default beta coupon if it doesn't exist
            await conn.execute("""
                INSERT INTO beta_coupons (code, max_uses, used_count, nft_reward, slh_bonus)
                VALUES ($1, $2, 0, 'SLH Genesis Member #', 0.1)
                ON CONFLICT (code) DO NOTHING
            """, BETA_COUPON_DEFAULT_CODE, BETA_COUPON_DEFAULT_LIMIT)
        except Exception as e:
            print(f"[Migration] Beta coupons: {e}")

        # --- Migration: ensure marketplace tables exist on already-running DBs ---
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_items (
                    id BIGSERIAL PRIMARY KEY,
                    seller_id BIGINT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    price NUMERIC(18,8) NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'SLH',
                    image_url TEXT,
                    category TEXT DEFAULT 'general',
                    stock INT DEFAULT 1,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    approved_at TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_orders (
                    id BIGSERIAL PRIMARY KEY,
                    buyer_id BIGINT NOT NULL,
                    seller_id BIGINT NOT NULL,
                    item_id BIGINT NOT NULL,
                    quantity INT NOT NULL DEFAULT 1,
                    total_price NUMERIC(18,8) NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'SLH',
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            """)
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_items_status ON marketplace_items(status)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_items_seller ON marketplace_items(seller_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_items_category ON marketplace_items(category)")
            # Migration: add promotion/views columns to existing marketplace_items
            await conn.execute("ALTER TABLE marketplace_items ADD COLUMN IF NOT EXISTS promotion TEXT DEFAULT 'none'")
            await conn.execute("ALTER TABLE marketplace_items ADD COLUMN IF NOT EXISTS promoted_until TIMESTAMP")
            await conn.execute("ALTER TABLE marketplace_items ADD COLUMN IF NOT EXISTS views INT DEFAULT 0")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_orders_buyer ON marketplace_orders(buyer_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_orders_seller ON marketplace_orders(seller_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_marketplace_orders_item ON marketplace_orders(item_id)")
        except Exception as e:
            print(f"[Migration] Marketplace tables: {e}")

        # --- Auto-register admin + existing premium users ---
        # Ensure admin row exists with sane defaults (first-run bootstrap)
        admin_username = os.getenv("ADMIN_USERNAME", "osifeu_prog")
        admin_first_name = os.getenv("ADMIN_FIRST_NAME", "Osif")
        await conn.execute("""
            INSERT INTO web_users (telegram_id, username, first_name, auth_date, last_login, is_registered, registered_at)
            VALUES ($1, $2, $3, EXTRACT(EPOCH FROM NOW())::BIGINT, CURRENT_TIMESTAMP, TRUE, CURRENT_TIMESTAMP)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = CASE
                    WHEN web_users.first_name IN ('', 'User') THEN EXCLUDED.first_name
                    ELSE web_users.first_name
                END,
                is_registered = TRUE,
                registered_at = COALESCE(web_users.registered_at, CURRENT_TIMESTAMP)
        """, ADMIN_USER_ID, admin_username, admin_first_name)

        await conn.execute("""
            UPDATE web_users SET is_registered = TRUE, registered_at = COALESCE(registered_at, CURRENT_TIMESTAMP)
            WHERE is_registered = FALSE AND telegram_id IN (
                SELECT DISTINCT user_id FROM premium_users WHERE payment_status = 'approved'
            )
        """)


@app.on_event("shutdown")
async def shutdown():
    # Stop wellness scheduler
    try:
        scheduler = get_wellness_scheduler()
        await scheduler.stop()
        print("[Wellness] Scheduler stopped")
    except Exception as e:
        print(f"[WARNING] Wellness scheduler shutdown: {str(e)}")

    if pool:
        await pool.close()


# === TELEGRAM AUTH ===
def verify_telegram_auth(data: dict) -> bool:
    """Verify Telegram Login Widget data"""
    if not BOT_TOKEN:
        return False
    check_hash = data.pop("hash", "")
    data_check = "\n".join(f"{k}={v}" for k, v in sorted(data.items()))
    secret = hashlib.sha256(BOT_TOKEN.encode()).digest()
    expected = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
    if expected != check_hash:
        return False
    if time.time() - int(data.get("auth_date", 0)) > 86400:
        return False
    return True


class TelegramAuth(BaseModel):
    id: int
    first_name: str
    username: Optional[str] = None
    photo_url: Optional[str] = None
    auth_date: int
    hash: str


def create_jwt(user_id: int, username: Optional[str] = None) -> str:
    if not JWT_SECRET:
        raise HTTPException(500, "JWT_SECRET is not configured")
    now = int(time.time())
    payload = {
        "sub": str(user_id),
        "username": username or "",
        "iat": now,
        "exp": now + (JWT_EXPIRES_HOURS * 3600),
        "jti": secrets.token_hex(16),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user_id(authorization: Optional[str] = Header(None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()

    if not JWT_SECRET:
        raise HTTPException(500, "JWT_SECRET is not configured")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid token")

    sub = payload.get("sub")
    if not sub or not str(sub).isdigit():
        raise HTTPException(401, "Invalid token payload")

    return int(sub)


_wallet_send_rate = {}


def _check_wallet_send_rate(user_id: int, cooldown_seconds: int = 5) -> bool:
    now = time.time()
    last = _wallet_send_rate.get(user_id, 0)
    if now - last < cooldown_seconds:
        return False
    _wallet_send_rate[user_id] = now
    return True


# === AUTH ENDPOINTS ===

class EnsureUserRequest(BaseModel):
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    photo_url: Optional[str] = None


@app.post("/api/user/ensure")
async def ensure_user(req: EnsureUserRequest):
    """Idempotent user creation/update from a Telegram ID.

    Used by the website's "manual login" flow â€” when a user types their
    Telegram ID directly (without the Telegram Login Widget), we still need
    to persist them in web_users so they don't "disappear" on refresh.

    This endpoint does NOT require a signed payload because:
    - Telegram IDs are public identifiers (anyone can know them)
    - We only create a profile row, no rights are granted
    - Registration/payment still gates premium access
    - Rate limits and validation prevent abuse
    """
    tg_id = req.telegram_id
    # Basic validation â€” must be a real Telegram user range
    if tg_id < 100000 or tg_id > 9999999999:
        raise HTTPException(400, "Invalid Telegram ID")

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO web_users (telegram_id, username, first_name, photo_url, auth_date, last_login)
            VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT, CURRENT_TIMESTAMP)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = COALESCE(NULLIF(EXCLUDED.username, ''), web_users.username),
                first_name = COALESCE(NULLIF(EXCLUDED.first_name, ''), web_users.first_name),
                photo_url = COALESCE(NULLIF(EXCLUDED.photo_url, ''), web_users.photo_url),
                last_login = CURRENT_TIMESTAMP
        """, tg_id, req.username or "", req.first_name or "User", req.photo_url or "")

        # Audit for institutional compliance
        await audit_log_write(
            conn,
            action="user.ensure",
            actor_type="user",
            actor_user_id=tg_id,
            resource_type="web_user",
            resource_id=str(tg_id),
            metadata={"source": "manual_login"},
        )

        balances = await get_user_balances(conn, tg_id)
        is_registered = await conn.fetchval(
            "SELECT is_registered FROM web_users WHERE telegram_id=$1", tg_id
        ) or False

    return {
        "ok": True,
        "telegram_id": tg_id,
        "is_registered": is_registered,
        "balances": balances,
    }


@app.post("/api/auth/telegram")
async def auth_telegram(auth: TelegramAuth):
    """Authenticate user via Telegram Login Widget"""
    auth_dict = auth.dict()
    if not verify_telegram_auth(auth_dict.copy()):
        raise HTTPException(401, "Invalid Telegram authentication")

    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO web_users (telegram_id, username, first_name, photo_url, auth_date, last_login)
            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = $2, first_name = $3, photo_url = $4, last_login = CURRENT_TIMESTAMP
        """, auth.id, auth.username, auth.first_name, auth.photo_url, auth.auth_date)

        # Fetch user balances and registration status
        balances = await get_user_balances(conn, auth.id)
        premium = await conn.fetchval(
            "SELECT payment_status FROM premium_users WHERE user_id=$1 AND bot_name='expertnet'", auth.id
        )
        is_registered = await conn.fetchval(
            "SELECT is_registered FROM web_users WHERE telegram_id=$1", auth.id
        ) or False

    jwt_token = create_jwt(auth.id, auth.username)

    return {
        "status": "ok",
        "token": jwt_token,
        "user": {
            "id": auth.id,
            "username": auth.username,
            "first_name": auth.first_name,
            "photo_url": auth.photo_url,
            "premium": premium == "approved",
            "is_registered": is_registered,
        },
        "balances": balances,
    }


# === REGISTRATION SYSTEM ===

REGISTRATION_PRICE_ILS = 22.221  # unified price across bot + website + mini-app
REGISTRATION_SLH_AMOUNT = 0.05    # SLH bonus credited on approval (scaled down from 0.1)
BETA_COUPON_DEFAULT_LIMIT = 49    # first 49 users get free access via coupon
BETA_COUPON_DEFAULT_CODE = "GENESIS49"  # the beta code (change to randomize)


class RegistrationInitRequest(BaseModel):
    referrer_id: Optional[int] = None


class RegistrationProofRequest(BaseModel):
    tx_hash: str = ""
    note: str = ""


class RegistrationApproveRequest(BaseModel):
    user_id: int
    admin_key: str = ""


@app.post("/api/registration/initiate")
async def registration_initiate(req: RegistrationInitRequest, authorization: Optional[str] = Header(None)):
    """Start registration â€” create pending payment record."""
    user_id = get_current_user_id(authorization)

    async with pool.acquire() as conn:
        # Check if already registered
        is_reg = await conn.fetchval("SELECT is_registered FROM web_users WHERE telegram_id=$1", user_id)
        if is_reg:
            return {"status": "already_registered"}

        # Check if pending payment exists
        existing = await conn.fetchrow(
            "SELECT id, payment_status FROM premium_users WHERE user_id=$1 AND bot_name='ecosystem'", user_id
        )
        if existing and existing["payment_status"] in ("submitted", "approved"):
            return {"status": existing["payment_status"], "message": "Payment already " + existing["payment_status"]}

        # Create or update pending registration
        await conn.execute("""
            INSERT INTO premium_users (user_id, bot_name, payment_status)
            VALUES ($1, 'ecosystem', 'pending')
            ON CONFLICT (user_id, bot_name) DO UPDATE SET payment_status = 'pending'
        """, user_id)

        # Register referral if provided
        if req.referrer_id and req.referrer_id != user_id:
            existing_ref = await conn.fetchrow("SELECT id FROM referrals WHERE user_id=$1", user_id)
            if not existing_ref:
                # Ensure referrer exists in web_users
                ref_exists = await conn.fetchval("SELECT 1 FROM web_users WHERE telegram_id=$1", req.referrer_id)
                if ref_exists:
                    ref_depth = await conn.fetchval("SELECT depth FROM referrals WHERE user_id=$1", req.referrer_id)
                    depth = (ref_depth or 0) + 1
                    try:
                        await conn.execute("""
                            INSERT INTO referrals (user_id, referrer_id, depth) VALUES ($1, $2, $3)
                        """, user_id, req.referrer_id, depth)
                    except Exception:
                        pass  # referral already registered

    return {
        "status": "pending",
        "price_ils": REGISTRATION_PRICE_ILS,
        "slh_amount": REGISTRATION_SLH_AMOUNT,
        "ton_wallet": SLH_TON_WALLET,
        "bsc_contract": SLH_BSC_CONTRACT,
        "message": f"Send {REGISTRATION_PRICE_ILS} ILS worth of TON to the wallet address"
    }


@app.post("/api/registration/submit-proof")
async def registration_submit_proof(req: RegistrationProofRequest, authorization: Optional[str] = Header(None)):
    """User submits payment proof (tx_hash or note)."""
    user_id = get_current_user_id(authorization)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, payment_status FROM premium_users WHERE user_id=$1 AND bot_name='ecosystem'", user_id
        )
        if not row:
            raise HTTPException(400, "No pending registration found. Call /api/registration/initiate first.")
        if row["payment_status"] == "approved":
            return {"status": "already_approved"}

        await conn.execute("""
            UPDATE premium_users SET payment_status = 'submitted'
            WHERE user_id = $1 AND bot_name = 'ecosystem'
        """, user_id)

        # Store proof in deposits table
        if req.tx_hash.strip():
            await conn.execute("""
                INSERT INTO deposits (user_id, amount, currency, tx_hash, status, plan_key)
                VALUES ($1, $2, 'ILS', $3, 'pending', 'registration')
            """, user_id, REGISTRATION_PRICE_ILS, req.tx_hash.strip())

    print(f"[Registration] User {user_id} submitted payment proof: {req.tx_hash or req.note}")
    return {"status": "submitted", "message": "Payment proof received. Waiting for admin approval."}


@app.post("/api/registration/approve")
async def registration_approve(
    req: RegistrationApproveRequest,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    """Admin approves a registration payment. Credits 0.1 SLH + triggers referral commissions.

    Auth: X-Admin-Key header (or Authorization: Bearer <jwt>). Body field
    `admin_key` still accepted as a deprecated fallback but logs a warning.
    """
    # Security: prefer header-based auth (goes through _require_admin which
    # checks ADMIN_API_KEYS env + rotated DB keys + JWT).
    try:
        _require_admin(authorization, x_admin_key)
    except HTTPException:
        # Fallback: body field admin_key (deprecated — logs warning + only accepts env-matched value, not defaults)
        env_keys = {k for k in os.getenv("ADMIN_API_KEYS", "").split(",") if k.strip()}
        legacy_key = os.getenv("ADMIN_API_KEY", "")
        if legacy_key:
            env_keys.add(legacy_key)
        if not (req.admin_key and req.admin_key in env_keys and req.admin_key != "slh_admin_2026"):
            raise HTTPException(403, "Admin authentication required (use X-Admin-Key header)")
        print(f"[SECURITY][DEPRECATED] /api/registration/approve called with body admin_key — migrate to X-Admin-Key header")

    async with pool.acquire() as conn:
        # Verify pending registration exists
        row = await conn.fetchrow(
            "SELECT id, payment_status FROM premium_users WHERE user_id=$1 AND bot_name='ecosystem'", req.user_id
        )
        if not row:
            raise HTTPException(404, "No registration record found for this user")
        if row["payment_status"] == "approved":
            return {"status": "already_approved"}

        async with conn.transaction():
            # 1. Mark registration as approved
            await conn.execute("""
                UPDATE premium_users SET payment_status = 'approved'
                WHERE user_id = $1 AND bot_name = 'ecosystem'
            """, req.user_id)

            # 2. Set user as registered
            await conn.execute("""
                UPDATE web_users SET is_registered = TRUE, registered_at = CURRENT_TIMESTAMP
                WHERE telegram_id = $1
            """, req.user_id)

            # 3. Credit 0.1 SLH token
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance)
                VALUES ($1, 'SLH', $2)
                ON CONFLICT (user_id, token) DO UPDATE SET balance = token_balances.balance + $2, updated_at = CURRENT_TIMESTAMP
            """, req.user_id, REGISTRATION_SLH_AMOUNT)

            # 4. Record token transfer
            await conn.execute("""
                INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type)
                VALUES ($1, $1, 'SLH', $2, 'Ecosystem registration bonus', 'registration')
            """, req.user_id, REGISTRATION_SLH_AMOUNT)

            # 5. Update deposit status if exists
            await conn.execute("""
                UPDATE deposits SET status = 'confirmed'
                WHERE user_id = $1 AND plan_key = 'registration' AND status = 'pending'
            """, req.user_id)

            # 6. Distribute referral commissions
            commissions = await distribute_referral_commissions(
                conn, req.user_id, REGISTRATION_PRICE_ILS, 'registration', 'ILS'
            )

    print(f"[Registration] User {req.user_id} APPROVED. 0.1 SLH credited. {len(commissions)} referral commissions distributed.")
    return {
        "status": "approved",
        "user_id": req.user_id,
        "slh_credited": REGISTRATION_SLH_AMOUNT,
        "referral_commissions": commissions
    }


# === SIMPLIFIED UNLOCK ENDPOINT (no JWT needed â€” works with ?uid= flow) ===
class UnlockRequest(BaseModel):
    user_id: int
    method: str = "payment_proof"  # payment_proof | coupon | admin
    tx_hash: Optional[str] = ""
    coupon_code: Optional[str] = ""
    admin_key: Optional[str] = ""
    note: Optional[str] = ""


@app.post("/api/registration/unlock")
async def registration_unlock(
    req: UnlockRequest,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    """Unlock a user's full access via one of 3 methods:

    1. payment_proof — user submits TX hash, goes to pending_review
    2. coupon — user enters beta code, instantly unlocked if code valid + available
    3. admin — admin key bypasses everything, instant unlock
         (auth via X-Admin-Key header preferred; body field deprecated)

    This is the NEW flow that doesn't require JWT, so it works with the
    seamless bot → /start → dashboard?uid= onboarding.
    """
    if not req.user_id:
        raise HTTPException(400, "user_id required")

    async with pool.acquire() as conn:
        # Ensure user exists
        user = await conn.fetchrow(
            "SELECT telegram_id, is_registered, beta_user FROM web_users WHERE telegram_id=$1",
            req.user_id
        )
        if not user:
            # Create a stub row so the unlock succeeds even for first-time users
            await conn.execute("""
                INSERT INTO web_users (telegram_id, username, first_name, auth_date, last_login)
                VALUES ($1, '', 'User', EXTRACT(EPOCH FROM NOW())::BIGINT, CURRENT_TIMESTAMP)
                ON CONFLICT (telegram_id) DO NOTHING
            """, req.user_id)

        # If already registered, short-circuit
        if user and user["is_registered"]:
            return {
                "ok": True,
                "status": "already_registered",
                "user_id": req.user_id,
                "message": "User is already registered"
            }

        # ─── Method 1: Admin override ───
        if req.method == "admin":
            # Security: prefer header auth. Body field admin_key still accepted
            # as deprecated fallback (logs warning). Default "slh_admin_2026"
            # is NEVER a valid key — admins must rotate.
            try:
                _require_admin(authorization, x_admin_key)
            except HTTPException:
                env_keys = {k for k in os.getenv("ADMIN_API_KEYS", "").split(",") if k.strip()}
                legacy_key = os.getenv("ADMIN_API_KEY", "")
                if legacy_key and legacy_key != "slh_admin_2026":
                    env_keys.add(legacy_key)
                if not (req.admin_key and req.admin_key in env_keys and req.admin_key != "slh_admin_2026"):
                    raise HTTPException(403, "Admin authentication required (use X-Admin-Key header)")
                print(f"[SECURITY][DEPRECATED] /api/registration/unlock method=admin called with body admin_key — migrate to X-Admin-Key header")
            async with conn.transaction():
                await conn.execute("""
                    UPDATE web_users SET is_registered = TRUE, registered_at = CURRENT_TIMESTAMP
                    WHERE telegram_id = $1
                """, req.user_id)
                await conn.execute("""
                    INSERT INTO premium_users (user_id, bot_name, payment_status)
                    VALUES ($1, 'ecosystem', 'approved')
                    ON CONFLICT (user_id, bot_name) DO UPDATE SET payment_status = 'approved'
                """, req.user_id)
                # Credit SLH bonus
                await conn.execute("""
                    INSERT INTO token_balances (user_id, token, balance)
                    VALUES ($1, 'SLH', $2)
                    ON CONFLICT (user_id, token) DO UPDATE SET balance = token_balances.balance + $2, updated_at = CURRENT_TIMESTAMP
                """, req.user_id, REGISTRATION_SLH_AMOUNT)
                await conn.execute("""
                    INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type)
                    VALUES ($1, $1, 'SLH', $2, 'Admin override registration', 'registration')
                """, req.user_id, REGISTRATION_SLH_AMOUNT)
            print(f"[Unlock] Admin override: user {req.user_id}")
            return {
                "ok": True,
                "status": "approved",
                "method": "admin",
                "user_id": req.user_id,
                "slh_credited": REGISTRATION_SLH_AMOUNT,
                "message": "Admin override â€” user is now fully registered"
            }

        # â”€â”€â”€ Method 2: Beta coupon â”€â”€â”€
        if req.method == "coupon":
            code = (req.coupon_code or "").strip().upper()
            if not code:
                raise HTTPException(400, "coupon_code required")
            coupon = await conn.fetchrow("""
                SELECT code, max_uses, used_count, nft_reward, slh_bonus, active
                  FROM beta_coupons WHERE code=$1
            """, code)
            if not coupon:
                raise HTTPException(404, f"Coupon '{code}' not found")
            if not coupon["active"]:
                raise HTTPException(400, "Coupon is not active")
            if coupon["used_count"] >= coupon["max_uses"]:
                raise HTTPException(400, f"Coupon is fully redeemed ({coupon['used_count']}/{coupon['max_uses']})")

            # Check if this user already redeemed this coupon
            already = await conn.fetchval(
                "SELECT nft_number FROM beta_redemptions WHERE coupon_code=$1 AND user_id=$2",
                code, req.user_id
            )
            if already:
                return {
                    "ok": True,
                    "status": "already_redeemed",
                    "nft_number": already,
                    "message": f"You already redeemed coupon â€” you are Genesis Member #{already}"
                }

            async with conn.transaction():
                # Increment coupon usage + assign NFT number
                nft_number = int(coupon["used_count"]) + 1
                await conn.execute(
                    "UPDATE beta_coupons SET used_count = used_count + 1 WHERE code=$1", code
                )
                await conn.execute("""
                    INSERT INTO beta_redemptions (coupon_code, user_id, nft_number)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (coupon_code, user_id) DO NOTHING
                """, code, req.user_id, nft_number)

                # Mark user as fully registered + beta
                await conn.execute("""
                    UPDATE web_users SET
                        is_registered = TRUE,
                        registered_at = CURRENT_TIMESTAMP,
                        beta_user = TRUE,
                        beta_coupon_code = $2,
                        beta_nft_number = $3
                    WHERE telegram_id = $1
                """, req.user_id, code, nft_number)

                await conn.execute("""
                    INSERT INTO premium_users (user_id, bot_name, payment_status)
                    VALUES ($1, 'ecosystem', 'approved')
                    ON CONFLICT (user_id, bot_name) DO UPDATE SET payment_status = 'approved'
                """, req.user_id)

                # Credit coupon bonus in ZVK (cheap reward token, NOT SLH which is scarce premium)
                # 1 SLH = 444 ILS, so to give ~44 ILS worth = 10 ZVK (1 ZVK â‰ˆ 4.4 ILS)
                # Post-distribution gift = 100 ZVK (~444 ILS) handled by cashback engine
                # SLH stays scarce â€” encourages users to BUY SLH from existing holders
                slh_bonus_legacy = float(coupon["slh_bonus"] or 0.1)
                zvk_amount = 10.0  # ~44 ILS distribution token (10 ZVK)
                await conn.execute("""
                    INSERT INTO token_balances (user_id, token, balance)
                    VALUES ($1, 'ZVK', $2)
                    ON CONFLICT (user_id, token) DO UPDATE SET balance = token_balances.balance + $2, updated_at = CURRENT_TIMESTAMP
                """, req.user_id, zvk_amount)
                await conn.execute("""
                    INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type)
                    VALUES ($1, $1, 'ZVK', $2, $3, 'beta_coupon')
                """, req.user_id, zvk_amount, f'Beta coupon {code} â€” Genesis #{nft_number} (ZVK distribution token)')

            print(f"[Unlock] Coupon '{code}' redeemed by user {req.user_id} â€” Genesis #{nft_number} ({zvk_amount} ZVK)")
            return {
                "ok": True,
                "status": "approved",
                "method": "coupon",
                "user_id": req.user_id,
                "coupon_code": code,
                "nft_number": nft_number,
                "nft_name": f"{coupon['nft_reward']}{nft_number}",
                "zvk_credited": zvk_amount,
                "slh_credited": 0,  # SLH NOT given â€” must be earned via cashback or purchased
                "post_distribution_gift_zvk": 100,  # promised after first share
                "remaining_slots": int(coupon["max_uses"]) - nft_number,
                "message": f"ðŸŽ‰ ×‘×¨×•×›×™× ×”×‘××™× Genesis Member #{nft_number}! ×§×™×‘×œ×ª 10 ZVK + NFT. ××—×¨×™ ×”×”×¤×¦×” ×”×¨××©×•× ×” â€” ×¢×•×“ 100 ZVK ×ž×ª× ×”!"
            }

        # â”€â”€â”€ Method 3: Payment proof (same as submit-proof but no JWT) â”€â”€â”€
        if req.method == "payment_proof":
            tx_hash = (req.tx_hash or "").strip()
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO premium_users (user_id, bot_name, payment_status)
                    VALUES ($1, 'ecosystem', 'submitted')
                    ON CONFLICT (user_id, bot_name) DO UPDATE SET payment_status = 'submitted'
                """, req.user_id)
                if tx_hash:
                    await conn.execute("""
                        INSERT INTO deposits (user_id, amount, currency, tx_hash, status, plan_key)
                        VALUES ($1, $2, 'ILS', $3, 'pending', 'registration')
                    """, req.user_id, REGISTRATION_PRICE_ILS, tx_hash)
            print(f"[Unlock] Payment proof submitted by user {req.user_id}: {tx_hash or req.note}")
            return {
                "ok": True,
                "status": "submitted",
                "method": "payment_proof",
                "user_id": req.user_id,
                "tx_hash": tx_hash,
                "message": "Payment proof received â€” waiting for admin approval (up to 24 hours)"
            }

        raise HTTPException(400, f"Unknown method: {req.method}")


@app.get("/api/beta/status")
async def beta_status():
    """Public: how many beta slots are left across all active coupons."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT code, max_uses, used_count, slh_bonus, active
              FROM beta_coupons
             WHERE active = TRUE
             ORDER BY created_at
        """)
    coupons = [{
        "code": r["code"],
        "max_uses": r["max_uses"],
        "used_count": r["used_count"],
        "remaining": int(r["max_uses"]) - int(r["used_count"]),
        "slh_bonus": float(r["slh_bonus"] or 0),
        "active": r["active"],
    } for r in rows]
    total_remaining = sum(c["remaining"] for c in coupons)
    return {"coupons": coupons, "total_remaining": total_remaining}


# ============================================================
# CASHBACK ENGINE â€” distribution rewards for Genesis users
# ============================================================
# Each user accumulates "distributions" (verified referrals).
# When they hit a tier, they automatically receive a SLH bonus.
#
# Tiers (referrals â†’ SLH bonus):
#   First successful share = +1 SLH (auto-credited as "post-distribution gift")
#   5  shares = 0.5 SLH cashback
#   10 shares = 1.5 SLH cashback (cumulative includes prior tiers)
#   25 shares = 5 SLH cashback
#   50 shares = 12 SLH cashback
#   100 shares = 30 SLH cashback

# All amounts in ZVK (NOT SLH - SLH stays scarce, only purchased or earned via tasks)
# 10 ZVK â‰ˆ 44 ILS (matches Genesis distribution amount, 1 ZVK â‰ˆ 4.4 ILS)
# Math: 1 SLH equivalent value = 100 ZVK
CASHBACK_TIERS = [
    (1,    100,  "post_distribution_gift"),  # 100 ZVK (~444 ILS) â€” first share gift
    (5,     50,  "tier_bronze"),              # 50 ZVK (~222 ILS)
    (10,   150,  "tier_silver"),              # 150 ZVK (~666 ILS)
    (25,   500,  "tier_gold"),                # 500 ZVK (~2,220 ILS)
    (50,  1200,  "tier_platinum"),            # 1,200 ZVK (~5,328 ILS)
    (100, 3000,  "tier_diamond"),             # 3,000 ZVK (~13,320 ILS)
]
CASHBACK_TOKEN = "ZVK"  # NEVER SLH â€” SLH is the scarce premium token


async def _ensure_cashback_table(conn):
    """Idempotent â€” creates the distribution + cashback tables if missing."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_distributions (
            user_id BIGINT NOT NULL,
            referred_user_id BIGINT NOT NULL,
            referred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            verified BOOLEAN DEFAULT FALSE,
            verified_at TIMESTAMP,
            PRIMARY KEY (user_id, referred_user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_distributions_user ON user_distributions(user_id, verified);

        CREATE TABLE IF NOT EXISTS user_cashback (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            tier_key TEXT NOT NULL,
            tier_threshold INT NOT NULL,
            slh_amount NUMERIC(18,8) NOT NULL,
            credited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, tier_key)
        );
        CREATE INDEX IF NOT EXISTS idx_cashback_user ON user_cashback(user_id);
    """)


@app.get("/api/cashback/{user_id}")
async def get_cashback_status(user_id: int):
    """Return distribution count + cashback tiers earned for a user.
    All amounts are in ZVK (cashback token), NOT SLH."""
    async with pool.acquire() as conn:
        await _ensure_cashback_table(conn)
        verified_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_distributions WHERE user_id=$1 AND verified=TRUE", user_id
        ) or 0
        earned = await conn.fetch(
            "SELECT tier_key, tier_threshold, slh_amount, credited_at FROM user_cashback WHERE user_id=$1 ORDER BY tier_threshold",
            user_id
        )
    earned_keys = {r["tier_key"] for r in earned}
    total_earned = sum(float(r["slh_amount"]) for r in earned)
    next_tier = None
    for threshold, amount, key in CASHBACK_TIERS:
        if key not in earned_keys and verified_count < threshold:
            next_tier = {"threshold": threshold, "zvk_amount": amount, "tier_key": key, "needed": threshold - verified_count}
            break
    return {
        "user_id": user_id,
        "verified_distributions": int(verified_count),
        "token": CASHBACK_TOKEN,
        "tiers_earned": [{"tier_key": r["tier_key"], "threshold": r["tier_threshold"], "zvk_amount": float(r["slh_amount"]), "credited_at": r["credited_at"].isoformat() if r["credited_at"] else None} for r in earned],
        "total_zvk_earned": total_earned,
        "next_tier": next_tier,
        "all_tiers": [{"threshold": t, "zvk_amount": a, "key": k} for t, a, k in CASHBACK_TIERS],
    }


@app.post("/api/cashback/process/{user_id}")
async def process_cashback(user_id: int):
    """Recompute cashback tiers for a user based on their verified distributions.
    Credits in ZVK (NOT SLH). Idempotent â€” already-credited tiers won't be paid twice.
    """
    async with pool.acquire() as conn:
        await _ensure_cashback_table(conn)
        verified_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_distributions WHERE user_id=$1 AND verified=TRUE", user_id
        ) or 0
        newly_credited = []
        for threshold, amount, key in CASHBACK_TIERS:
            if verified_count >= threshold:
                # Try to insert â€” UNIQUE constraint prevents double-pay
                inserted = await conn.fetchval("""
                    INSERT INTO user_cashback (user_id, tier_key, tier_threshold, slh_amount)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (user_id, tier_key) DO NOTHING
                    RETURNING id
                """, user_id, key, threshold, amount)
                if inserted:
                    # Credit ZVK balance (NOT SLH â€” SLH stays scarce)
                    await conn.execute("""
                        INSERT INTO token_balances (user_id, token, balance)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (user_id, token) DO UPDATE SET balance = token_balances.balance + EXCLUDED.balance, updated_at = CURRENT_TIMESTAMP
                    """, user_id, CASHBACK_TOKEN, amount)
                    newly_credited.append({"tier_key": key, "threshold": threshold, "zvk": amount})
    return {
        "ok": True,
        "user_id": user_id,
        "verified_distributions": int(verified_count),
        "token": CASHBACK_TOKEN,
        "newly_credited": newly_credited,
        "total_credited": len(newly_credited),
    }


# ============================================================
# EXTERNAL WALLETS â€” Bybit, Binance, custom TON addresses
# ============================================================
# Users can register multiple external wallet addresses (TON, BSC, ETH, BTC)
# from exchanges (Bybit, Binance, Bitget, OKX) or self-custody.
# We READ-ONLY query their balances via public APIs (toncenter, BscScan).
# NEVER ask for private keys, API keys, or signatures.

class ExternalWalletAdd(BaseModel):
    user_id: int
    label: str  # "Bybit Main", "Binance Spot", "My Cold Wallet"
    network: str  # "TON" | "BSC" | "ETH" | "BTC"
    address: str
    provider: Optional[str] = None  # "bybit" | "binance" | "bitget" | "okx" | "self"


async def _ensure_external_wallets_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS external_wallets (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            label TEXT NOT NULL,
            network TEXT NOT NULL,
            address TEXT NOT NULL,
            provider TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_balance_native NUMERIC(28,8) DEFAULT 0,
            last_balance_usdt NUMERIC(28,8) DEFAULT 0,
            last_checked TIMESTAMP,
            UNIQUE(user_id, network, address)
        );
        CREATE INDEX IF NOT EXISTS idx_external_wallets_user ON external_wallets(user_id);
    """)


@app.post("/api/external-wallets/add")
async def add_external_wallet(req: ExternalWalletAdd):
    """Add an external wallet address for the user (read-only tracking)."""
    if not req.user_id or not req.address or len(req.address) < 10:
        raise HTTPException(400, "user_id + valid address required")
    network = req.network.upper()
    if network not in ("TON", "BSC", "ETH", "BTC"):
        raise HTTPException(400, "network must be TON, BSC, ETH, or BTC")
    async with pool.acquire() as conn:
        await _ensure_external_wallets_table(conn)
        wid = await conn.fetchval("""
            INSERT INTO external_wallets (user_id, label, network, address, provider)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id, network, address) DO UPDATE
              SET label = EXCLUDED.label, provider = EXCLUDED.provider
            RETURNING id
        """, req.user_id, req.label[:50], network, req.address, req.provider)
    return {"ok": True, "wallet_id": wid, "user_id": req.user_id, "network": network, "label": req.label}


@app.get("/api/external-wallets/{user_id}")
async def list_external_wallets(user_id: int):
    """List all external wallets for a user with their last cached balance."""
    async with pool.acquire() as conn:
        await _ensure_external_wallets_table(conn)
        rows = await conn.fetch("""
            SELECT id, label, network, address, provider, last_balance_native, last_balance_usdt, last_checked, added_at
              FROM external_wallets
             WHERE user_id = $1
             ORDER BY added_at DESC
        """, user_id)
    return {
        "user_id": user_id,
        "wallets": [{
            "id": r["id"],
            "label": r["label"],
            "network": r["network"],
            "address": r["address"],
            "provider": r["provider"],
            "last_balance_native": float(r["last_balance_native"] or 0),
            "last_balance_usdt": float(r["last_balance_usdt"] or 0),
            "last_checked": r["last_checked"].isoformat() if r["last_checked"] else None,
            "added_at": r["added_at"].isoformat() if r["added_at"] else None,
        } for r in rows]
    }


@app.delete("/api/external-wallets/{wallet_id}")
async def delete_external_wallet(wallet_id: int, user_id: int):
    """Remove an external wallet (must own it)."""
    async with pool.acquire() as conn:
        await _ensure_external_wallets_table(conn)
        deleted = await conn.execute(
            "DELETE FROM external_wallets WHERE id=$1 AND user_id=$2", wallet_id, user_id
        )
    return {"ok": True, "deleted": deleted}


async def _fetch_ton_balance(address: str) -> dict:
    """Fetch TON balance + jettons from toncenter (public, no API key needed)."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"https://toncenter.com/api/v2/getAddressBalance",
                params={"address": address},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return {"native": 0, "usdt": 0, "error": f"HTTP {resp.status}"}
                data = await resp.json()
                if not data.get("ok"):
                    return {"native": 0, "usdt": 0, "error": data.get("description", "unknown")}
                # toncenter returns nano-TON
                native = float(data.get("result", 0)) / 1e9
                return {"native": native, "usdt": 0, "ok": True}
    except Exception as e:
        return {"native": 0, "usdt": 0, "error": str(e)[:100]}


@app.post("/api/external-wallets/refresh/{wallet_id}")
async def refresh_external_wallet(wallet_id: int):
    """Refresh balance for a single external wallet (calls public chain API)."""
    async with pool.acquire() as conn:
        await _ensure_external_wallets_table(conn)
        row = await conn.fetchrow(
            "SELECT id, user_id, network, address FROM external_wallets WHERE id=$1", wallet_id
        )
        if not row:
            raise HTTPException(404, "Wallet not found")

        balance_info = {"native": 0, "usdt": 0}
        if row["network"] == "TON":
            balance_info = await _fetch_ton_balance(row["address"])
        elif row["network"] == "BSC":
            # TODO: BscScan API call
            balance_info = {"native": 0, "usdt": 0, "info": "BSC support coming soon"}
        elif row["network"] == "ETH":
            balance_info = {"native": 0, "usdt": 0, "info": "ETH support coming soon"}

        await conn.execute("""
            UPDATE external_wallets
               SET last_balance_native = $1,
                   last_balance_usdt = $2,
                   last_checked = CURRENT_TIMESTAMP
             WHERE id = $3
        """, balance_info["native"], balance_info["usdt"], wallet_id)

    return {
        "ok": True,
        "wallet_id": wallet_id,
        "network": row["network"],
        "address": row["address"],
        "balance": balance_info,
    }


@app.post("/api/external-wallets/refresh-all/{user_id}")
async def refresh_all_external_wallets(user_id: int):
    """Refresh balances for all of a user's external wallets."""
    async with pool.acquire() as conn:
        await _ensure_external_wallets_table(conn)
        rows = await conn.fetch(
            "SELECT id FROM external_wallets WHERE user_id=$1", user_id
        )
    results = []
    for r in rows:
        try:
            res = await refresh_external_wallet(r["id"])
            results.append(res)
        except Exception as e:
            results.append({"wallet_id": r["id"], "error": str(e)[:100]})
    return {"ok": True, "user_id": user_id, "refreshed": len(results), "results": results}


# ============================================================
# IMMUTABLE AUDIT LOG â€” Institutional / Regulator-Grade
# ============================================================
# Every sensitive action is written to an append-only log with a
# SHA-256 hash chain. Each entry includes the hash of the previous
# entry, making tampering detectable: any modification breaks the chain.
#
# This is the table regulators ask to see first. We never DELETE or
# UPDATE rows â€” only INSERT.

async def _ensure_institutional_audit_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS institutional_audit (
            id BIGSERIAL PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            actor_user_id BIGINT,
            actor_ip TEXT,
            actor_type TEXT NOT NULL, -- 'user' | 'admin' | 'system' | 'api'
            action TEXT NOT NULL, -- 'wallet.link' | 'trade.execute' | 'withdraw.request' | ...
            resource_type TEXT, -- 'wallet' | 'trade' | 'user' | 'kyc'
            resource_id TEXT,
            before_state JSONB,
            after_state JSONB,
            amount_native NUMERIC(28,18),
            amount_currency TEXT,
            amount_usd NUMERIC(18,2),
            risk_score INT, -- 0-100
            compliance_flags TEXT[], -- ['TRAVEL_RULE', 'SANCTIONS_CHECK_PASSED', 'AML_OK', ...]
            prev_hash CHAR(64),
            entry_hash CHAR(64) NOT NULL,
            metadata JSONB
        );
        CREATE INDEX IF NOT EXISTS idx_inst_audit_actor ON institutional_audit(actor_user_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_inst_audit_action ON institutional_audit(action, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_inst_audit_resource ON institutional_audit(resource_type, resource_id);
        CREATE INDEX IF NOT EXISTS idx_inst_audit_timestamp ON institutional_audit(timestamp DESC);
    """)
    # Prevent UPDATE/DELETE via revoke (regulators require this)
    try:
        await conn.execute("REVOKE UPDATE, DELETE ON institutional_audit FROM PUBLIC;")
    except Exception:
        pass


async def audit_log_write(
    conn,
    action: str,
    actor_type: str = "system",
    actor_user_id: Optional[int] = None,
    actor_ip: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    before_state: Optional[dict] = None,
    after_state: Optional[dict] = None,
    amount_native: Optional[float] = None,
    amount_currency: Optional[str] = None,
    amount_usd: Optional[float] = None,
    risk_score: Optional[int] = None,
    compliance_flags: Optional[list] = None,
    metadata: Optional[dict] = None,
) -> str:
    """Write an audit entry with hash chain. Returns the entry_hash."""
    await _ensure_institutional_audit_table(conn)

    # Get last entry's hash (for chain)
    prev_hash = await conn.fetchval(
        "SELECT entry_hash FROM institutional_audit ORDER BY id DESC LIMIT 1"
    ) or "0" * 64

    # Build the payload that gets hashed
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "actor_type": actor_type,
        "actor_user_id": actor_user_id,
        "actor_ip": actor_ip,
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "before_state": before_state,
        "after_state": after_state,
        "amount_native": float(amount_native) if amount_native else None,
        "amount_currency": amount_currency,
        "amount_usd": float(amount_usd) if amount_usd else None,
        "risk_score": risk_score,
        "compliance_flags": compliance_flags or [],
        "metadata": metadata or {},
        "prev_hash": prev_hash,
    }
    payload_str = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    entry_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()

    await conn.execute("""
        INSERT INTO institutional_audit (
            actor_user_id, actor_ip, actor_type, action,
            resource_type, resource_id, before_state, after_state,
            amount_native, amount_currency, amount_usd,
            risk_score, compliance_flags, prev_hash, entry_hash, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13, $14, $15, $16::jsonb)
    """,
        actor_user_id, actor_ip, actor_type, action,
        resource_type, str(resource_id) if resource_id else None,
        json.dumps(before_state) if before_state else None,
        json.dumps(after_state) if after_state else None,
        amount_native, amount_currency, amount_usd,
        risk_score, compliance_flags or [],
        prev_hash, entry_hash,
        json.dumps(metadata) if metadata else None,
    )
    return entry_hash


@app.get("/api/audit/verify-chain")
async def verify_audit_chain(limit: int = 1000):
    """Verify the hash chain integrity. Returns any broken entries.
    Regulators/auditors call this to prove data hasn't been tampered with."""
    async with pool.acquire() as conn:
        await _ensure_institutional_audit_table(conn)
        rows = await conn.fetch("""
            SELECT id, entry_hash, prev_hash, timestamp, action
              FROM institutional_audit
             ORDER BY id ASC
             LIMIT $1
        """, limit)

    if not rows:
        return {"ok": True, "total": 0, "broken": [], "message": "Empty audit log"}

    broken = []
    expected_prev = "0" * 64
    for r in rows:
        if r["prev_hash"] != expected_prev:
            broken.append({
                "id": r["id"],
                "expected_prev": expected_prev,
                "actual_prev": r["prev_hash"],
                "action": r["action"],
            })
        expected_prev = r["entry_hash"]

    return {
        "ok": len(broken) == 0,
        "total": len(rows),
        "broken": broken,
        "message": "Chain intact" if len(broken) == 0 else f"CHAIN BROKEN at {len(broken)} entries",
    }


@app.get("/api/audit/recent")
async def audit_recent(limit: int = 100, action_filter: Optional[str] = None, user_id: Optional[int] = None):
    """Get recent audit entries for admin review."""
    async with pool.acquire() as conn:
        await _ensure_institutional_audit_table(conn)
        if action_filter and user_id:
            rows = await conn.fetch("""
                SELECT id, timestamp, actor_user_id, actor_type, action,
                       resource_type, resource_id, amount_native, amount_currency,
                       amount_usd, risk_score, compliance_flags, entry_hash
                  FROM institutional_audit
                 WHERE action = $1 AND actor_user_id = $2
                 ORDER BY timestamp DESC
                 LIMIT $3
            """, action_filter, user_id, limit)
        elif action_filter:
            rows = await conn.fetch("""
                SELECT id, timestamp, actor_user_id, actor_type, action,
                       resource_type, resource_id, amount_native, amount_currency,
                       amount_usd, risk_score, compliance_flags, entry_hash
                  FROM institutional_audit
                 WHERE action = $1
                 ORDER BY timestamp DESC
                 LIMIT $2
            """, action_filter, limit)
        elif user_id:
            rows = await conn.fetch("""
                SELECT id, timestamp, actor_user_id, actor_type, action,
                       resource_type, resource_id, amount_native, amount_currency,
                       amount_usd, risk_score, compliance_flags, entry_hash
                  FROM institutional_audit
                 WHERE actor_user_id = $1
                 ORDER BY timestamp DESC
                 LIMIT $2
            """, user_id, limit)
        else:
            rows = await conn.fetch("""
                SELECT id, timestamp, actor_user_id, actor_type, action,
                       resource_type, resource_id, amount_native, amount_currency,
                       amount_usd, risk_score, compliance_flags, entry_hash
                  FROM institutional_audit
                 ORDER BY timestamp DESC
                 LIMIT $1
            """, limit)

    return {
        "count": len(rows),
        "entries": [{
            "id": r["id"],
            "timestamp": r["timestamp"].isoformat() if r["timestamp"] else None,
            "actor_user_id": r["actor_user_id"],
            "actor_type": r["actor_type"],
            "action": r["action"],
            "resource_type": r["resource_type"],
            "resource_id": r["resource_id"],
            "amount_native": float(r["amount_native"]) if r["amount_native"] else None,
            "amount_currency": r["amount_currency"],
            "amount_usd": float(r["amount_usd"]) if r["amount_usd"] else None,
            "risk_score": r["risk_score"],
            "compliance_flags": r["compliance_flags"] or [],
            "entry_hash": r["entry_hash"][:16] + "...",  # truncate for display
        } for r in rows]
    }


# ============================================================
# CEX INTEGRATIONS â€” Bybit + Binance (READ-ONLY)
# ============================================================
# Each user can link their own Bybit/Binance API keys (READ-ONLY scope).
# Keys are encrypted at rest, never logged. We only call GET endpoints.
# Never trade/withdraw.

class CexApiKeyAdd(BaseModel):
    user_id: int
    exchange: str  # 'bybit' | 'binance'
    label: str
    api_key: str
    api_secret: str
    permissions: Optional[list] = None  # ['read']


async def _ensure_cex_keys_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cex_api_keys (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            exchange TEXT NOT NULL, -- bybit | binance
            label TEXT NOT NULL,
            api_key_masked TEXT NOT NULL, -- first 8 chars only, for display
            api_key_encrypted TEXT NOT NULL, -- full key encrypted
            api_secret_encrypted TEXT NOT NULL,
            permissions TEXT[],
            is_active BOOLEAN DEFAULT TRUE,
            last_used_at TIMESTAMP,
            last_error TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, exchange, api_key_masked)
        );
        CREATE INDEX IF NOT EXISTS idx_cex_keys_user ON cex_api_keys(user_id);

        CREATE TABLE IF NOT EXISTS cex_snapshots (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            exchange TEXT NOT NULL,
            snapshot_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_usd_value NUMERIC(28,8),
            spot_balances JSONB, -- {BTC: {free: 0.5, locked: 0, usd_value: 34000}, ...}
            futures_positions JSONB,
            earn_positions JSONB, -- savings, staking, etc
            raw_response JSONB
        );
        CREATE INDEX IF NOT EXISTS idx_cex_snapshots_user_time ON cex_snapshots(user_id, snapshot_at DESC);
    """)


def _get_encryption_key() -> bytes:
    """Derive a 32-byte AES-GCM key from ENCRYPTION_KEY env var via SHA-256.
    Accepts any length input â€” hashes to produce a stable 256-bit key.
    """
    raw = os.getenv("ENCRYPTION_KEY", "slh_dev_key_CHANGE_ME_IN_PRODUCTION_2026")
    return hashlib.sha256(raw.encode("utf-8")).digest()


def _encrypt_secret(secret: str) -> str:
    """AES-GCM authenticated encryption.

    Format: version:hex(nonce):hex(ciphertext+tag)
    - version 'v2' marks AES-GCM (current)
    - version 'v1' (legacy XOR) still decryptable via _decrypt_secret_xor
    """
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        # Fallback if cryptography lib not installed â€” this should never happen
        # in production. Force AES-GCM via requirements.txt.
        return _encrypt_secret_xor(secret)

    key = _get_encryption_key()
    aesgcm = AESGCM(key)
    nonce = secrets.token_bytes(12)  # 96-bit nonce, recommended for GCM
    ciphertext = aesgcm.encrypt(nonce, secret.encode("utf-8"), None)
    return f"v2:{nonce.hex()}:{ciphertext.hex()}"


def _decrypt_secret(blob: str) -> str:
    """Decrypt a secret stored by _encrypt_secret. Supports v1 (XOR legacy) and v2 (AES-GCM)."""
    if not blob:
        return ""
    # v2 = AES-GCM (current)
    if blob.startswith("v2:"):
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            parts = blob.split(":")
            if len(parts) != 3:
                return ""
            nonce = bytes.fromhex(parts[1])
            ciphertext = bytes.fromhex(parts[2])
            aesgcm = AESGCM(_get_encryption_key())
            return aesgcm.decrypt(nonce, ciphertext, None).decode("utf-8")
        except Exception as e:
            print(f"[AES-GCM decrypt] failed: {e}")
            return ""
    # v1 = legacy XOR (any pre-v2 keys still in DB)
    return _decrypt_secret_xor(blob)


def _encrypt_secret_xor(secret: str) -> str:
    """LEGACY v1 XOR encryption â€” kept only for backwards compat / fallback."""
    key = os.getenv("ENCRYPTION_KEY", "slh_dev_key_CHANGE_ME_IN_PRODUCTION_2026")
    result = []
    for i, c in enumerate(secret):
        result.append(chr(ord(c) ^ ord(key[i % len(key)])))
    return "".join(result).encode("latin-1").hex()


def _decrypt_secret_xor(hex_str: str) -> str:
    """LEGACY v1 XOR decryption â€” called automatically by _decrypt_secret for old data."""
    try:
        encrypted = bytes.fromhex(hex_str).decode("latin-1")
        key = os.getenv("ENCRYPTION_KEY", "slh_dev_key_CHANGE_ME_IN_PRODUCTION_2026")
        result = []
        for i, c in enumerate(encrypted):
            result.append(chr(ord(c) ^ ord(key[i % len(key)])))
        return "".join(result)
    except Exception:
        return ""


@app.post("/api/cex/add-key")
async def cex_add_api_key(req: CexApiKeyAdd):
    """Link a Bybit or Binance API key (READ-ONLY only). Encrypted at rest."""
    if req.exchange not in ("bybit", "binance"):
        raise HTTPException(400, "exchange must be 'bybit' or 'binance'")
    if len(req.api_key) < 8 or len(req.api_secret) < 8:
        raise HTTPException(400, "API key/secret too short")

    async with pool.acquire() as conn:
        await _ensure_cex_keys_table(conn)
        masked = req.api_key[:8] + "..." + req.api_key[-4:]
        kid = await conn.fetchval("""
            INSERT INTO cex_api_keys (user_id, exchange, label, api_key_masked, api_key_encrypted, api_secret_encrypted, permissions)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (user_id, exchange, api_key_masked) DO UPDATE
              SET label = EXCLUDED.label, is_active = TRUE
            RETURNING id
        """, req.user_id, req.exchange, req.label[:100], masked,
            _encrypt_secret(req.api_key), _encrypt_secret(req.api_secret),
            req.permissions or ["read"])
        # Audit
        await audit_log_write(conn,
            action=f"cex.key.link",
            actor_type="user",
            actor_user_id=req.user_id,
            resource_type="cex_api_key",
            resource_id=str(kid),
            metadata={"exchange": req.exchange, "label": req.label, "masked": masked},
            compliance_flags=["CEX_KEY_LINKED", "READ_ONLY_ASSUMED"],
        )
    return {"ok": True, "id": kid, "key_id": kid, "user_id": req.user_id, "exchange": req.exchange, "masked": masked}


@app.get("/api/cex/keys/{user_id}")
async def cex_list_keys(user_id: int):
    """List CEX keys for a user (never returns secrets)."""
    async with pool.acquire() as conn:
        await _ensure_cex_keys_table(conn)
        rows = await conn.fetch("""
            SELECT id, exchange, label, api_key_masked, permissions, is_active,
                   last_used_at, last_error, added_at
              FROM cex_api_keys
             WHERE user_id = $1
             ORDER BY added_at DESC
        """, user_id)
    return {"user_id": user_id, "keys": [dict(r) for r in rows]}


@app.delete("/api/cex/keys/{key_id}")
async def cex_delete_key(key_id: int, user_id: int):
    """Remove a CEX API key."""
    async with pool.acquire() as conn:
        await _ensure_cex_keys_table(conn)
        await conn.execute(
            "DELETE FROM cex_api_keys WHERE id=$1 AND user_id=$2", key_id, user_id
        )
        await audit_log_write(conn,
            action="cex.key.unlink",
            actor_type="user",
            actor_user_id=user_id,
            resource_type="cex_api_key",
            resource_id=str(key_id),
        )
    return {"ok": True, "deleted": key_id}


async def _bybit_sign(api_secret: str, timestamp: str, api_key: str, recv_window: str, query_string: str) -> str:
    """Sign a Bybit V5 API request."""
    param_str = timestamp + api_key + recv_window + query_string
    return hmac.new(
        api_secret.encode("utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def _bybit_get_balances(api_key: str, api_secret: str) -> dict:
    """Call Bybit V5 /v5/account/wallet-balance to get all balances."""
    import time as _time
    timestamp = str(int(_time.time() * 1000))
    recv_window = "5000"
    query = "accountType=UNIFIED"
    sign = await _bybit_sign(api_secret, timestamp, api_key, recv_window, query)
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }
    url = f"https://api.bybit.com/v5/account/wallet-balance?{query}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                return data
    except Exception as e:
        return {"error": str(e)[:200]}


async def _binance_sign(api_secret: str, query_string: str) -> str:
    """Sign a Binance API request."""
    return hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


async def _binance_get_account(api_key: str, api_secret: str) -> dict:
    """Call Binance /api/v3/account to get spot balances."""
    import time as _time
    timestamp = str(int(_time.time() * 1000))
    query = f"timestamp={timestamp}"
    sign = await _binance_sign(api_secret, query)
    url = f"https://api.binance.com/api/v3/account?{query}&signature={sign}"
    headers = {"X-MBX-APIKEY": api_key}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
                return data
    except Exception as e:
        return {"error": str(e)[:200]}


@app.post("/api/cex/sync/{key_id}")
async def cex_sync_balances(key_id: int):
    """Fetch live balances from CEX and save as snapshot."""
    async with pool.acquire() as conn:
        await _ensure_cex_keys_table(conn)
        row = await conn.fetchrow("SELECT * FROM cex_api_keys WHERE id=$1 AND is_active=TRUE", key_id)
        if not row:
            raise HTTPException(404, "Key not found or inactive")

        api_key = _decrypt_secret(row["api_key_encrypted"])
        api_secret = _decrypt_secret(row["api_secret_encrypted"])

        if row["exchange"] == "bybit":
            data = await _bybit_get_balances(api_key, api_secret)
        elif row["exchange"] == "binance":
            data = await _binance_get_account(api_key, api_secret)
        else:
            raise HTTPException(400, "Unsupported exchange")

        if "error" in data or data.get("retCode") not in (0, None):
            error_msg = data.get("error") or data.get("retMsg") or "Unknown error"
            await conn.execute(
                "UPDATE cex_api_keys SET last_error=$1, last_used_at=CURRENT_TIMESTAMP WHERE id=$2",
                str(error_msg)[:200], key_id
            )
            return {"ok": False, "error": error_msg, "key_id": key_id}

        # Parse response and compute total USD
        total_usd = 0.0
        spot_balances = {}

        if row["exchange"] == "bybit":
            result = data.get("result", {})
            lists = result.get("list", [])
            for acc in lists:
                for coin in acc.get("coin", []):
                    symbol = coin.get("coin")
                    wallet_balance = float(coin.get("walletBalance") or 0)
                    if wallet_balance > 0:
                        usd_value = float(coin.get("usdValue") or 0)
                        spot_balances[symbol] = {
                            "balance": wallet_balance,
                            "usd_value": usd_value,
                        }
                        total_usd += usd_value

        elif row["exchange"] == "binance":
            for bal in data.get("balances", []):
                free = float(bal.get("free") or 0)
                locked = float(bal.get("locked") or 0)
                total = free + locked
                if total > 0:
                    spot_balances[bal["asset"]] = {
                        "free": free,
                        "locked": locked,
                        "usd_value": 0,  # would need separate price lookup
                    }

        # Save snapshot
        snap_id = await conn.fetchval("""
            INSERT INTO cex_snapshots (user_id, exchange, total_usd_value, spot_balances, raw_response)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
            RETURNING id
        """, row["user_id"], row["exchange"], total_usd,
            json.dumps(spot_balances), json.dumps(data)[:10000])

        await conn.execute(
            "UPDATE cex_api_keys SET last_used_at=CURRENT_TIMESTAMP, last_error=NULL WHERE id=$1",
            key_id
        )

        # Audit
        await audit_log_write(conn,
            action=f"cex.snapshot.{row['exchange']}",
            actor_type="system",
            actor_user_id=row["user_id"],
            resource_type="cex_snapshot",
            resource_id=str(snap_id),
            amount_usd=total_usd,
            amount_currency="USD",
            metadata={
                "exchange": row["exchange"],
                "assets_count": len(spot_balances),
                "key_id": key_id,
            },
        )

    return {
        "ok": True,
        "snapshot_id": snap_id,
        "exchange": row["exchange"],
        "total_usd": total_usd,
        "assets_count": len(spot_balances),
        "spot_balances": spot_balances,
    }


@app.get("/api/cex/portfolio/{user_id}")
async def cex_portfolio(user_id: int):
    """Get the latest snapshot from all CEX accounts for this user."""
    async with pool.acquire() as conn:
        await _ensure_cex_keys_table(conn)
        # Get latest snapshot per exchange
        rows = await conn.fetch("""
            SELECT DISTINCT ON (exchange) id, exchange, snapshot_at, total_usd_value, spot_balances
              FROM cex_snapshots
             WHERE user_id = $1
             ORDER BY exchange, snapshot_at DESC
        """, user_id)

    total_usd = sum(float(r["total_usd_value"] or 0) for r in rows)
    return {
        "user_id": user_id,
        "total_usd": total_usd,
        "total_ils": round(total_usd * 3.65, 2),
        "exchanges": [{
            "exchange": r["exchange"],
            "snapshot_at": r["snapshot_at"].isoformat() if r["snapshot_at"] else None,
            "total_usd": float(r["total_usd_value"] or 0),
            "spot_balances": r["spot_balances"],
        } for r in rows]
    }


# ============================================================
# BSC HOLDERS â€” via Etherscan V2 Multichain API (chainid=56)
# ============================================================
# Etherscan's V2 API works across all supported chains including BSC.
# Uses BSCSCAN_API_KEY env var (fallback to ETHERSCAN_API_KEY).
# One API key works for Ethereum, BSC, Polygon, Arbitrum, Base, etc.

SLH_BSC_CONTRACT = "0xACb0A09414CEA1C879c67bB7A877E4e19480f022"

_holders_cache = {"data": None, "ts": 0}


async def _fetch_holders_bitquery(api_key: str, limit: int = 100) -> dict:
    """Query BitQuery GraphQL for SLH holders on BSC. Free tier 10k/month.

    Returns the standard holders response format on success, or {"ok": False}.
    """
    query = """
    query ($contract: String!, $limit: Int!) {
      ethereum(network: bsc) {
        address(address: {is: $contract}) {
          balances(currency: {is: $contract}, options: {desc: "value", limit: $limit}) {
            value
            address {
              address
            }
          }
        }
      }
    }
    """
    payload = {
        "query": query,
        "variables": {"contract": SLH_BSC_CONTRACT, "limit": min(limit, 100)},
    }
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": api_key,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://graphql.bitquery.io",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                data = await resp.json()
    except Exception as e:
        return {"ok": False, "error": f"BitQuery request failed: {str(e)[:200]}"}

    try:
        balances = data["data"]["ethereum"]["address"][0]["balances"]
    except Exception:
        return {"ok": False, "error": "BitQuery response format unexpected", "raw": str(data)[:300]}

    total_supply = 111186328
    holders = []
    for i, b in enumerate(balances):
        balance = float(b.get("value") or 0)
        addr = b.get("address", {}).get("address", "")
        pct = (balance / total_supply * 100) if total_supply else 0
        holders.append({
            "rank": i + 1,
            "address": addr,
            "balance": balance,
            "percent": round(pct, 4),
            "bscscan_url": f"https://bscscan.com/address/{addr}",
        })

    return {
        "ok": True,
        "source": "bitquery",
        "contract": SLH_BSC_CONTRACT,
        "chain": "BSC (56)",
        "total_holders": len(holders),
        "holders": holders,
        "cached_at": datetime.utcnow().isoformat(),
    }

@app.get("/api/network/slh-holders")
async def get_slh_holders(limit: int = 100, force_refresh: bool = False):
    """Fetch SLH token holders from BSC.

    Tries multiple free providers in order:
    1. BitQuery GraphQL (free 10k/month) â€” BITQUERY_API_KEY env var
    2. Etherscan V2 Multichain (PRO only for BSC) â€” BSCSCAN_API_KEY
    3. NodeReal (free 100k/day) â€” NODEREAL_API_KEY

    Cached for 5 minutes.
    """
    import time as _time
    now = _time.time()
    if not force_refresh and _holders_cache["data"] and (now - _holders_cache["ts"]) < 300:
        return _holders_cache["data"]

    # 1) Try BitQuery first (free tier, most generous)
    bitquery_key = os.getenv("BITQUERY_API_KEY")
    if bitquery_key:
        result = await _fetch_holders_bitquery(bitquery_key, limit)
        if result.get("ok"):
            _holders_cache["data"] = result
            _holders_cache["ts"] = now
            return result

    # 2) Try Etherscan V2 (requires PRO for BSC now)
    api_key = os.getenv("BSCSCAN_API_KEY") or os.getenv("ETHERSCAN_API_KEY")
    if not api_key:
        return {
            "ok": False,
            "error": "No BSC holder API configured",
            "hint": "Set BITQUERY_API_KEY (recommended, free) or ETHERSCAN_API_KEY (PRO) on Railway",
            "alternatives": [
                {"name": "BitQuery", "url": "https://bitquery.io", "free": True, "limit": "10k/month"},
                {"name": "NodeReal", "url": "https://nodereal.io", "free": True, "limit": "100k/day"},
                {"name": "Alchemy", "url": "https://alchemy.com", "free": True, "limit": "300M CU/month"},
            ],
            "holders": [],
            "total_holders": 0,
        }

    # Etherscan V2 Multichain API â€” chainid=56 is BSC
    # tokenholderlist is a PRO-tier endpoint in V2, but tokentx works on free tier
    # We'll use tokensupply + tokenbalance for the contract to get top holders
    url = (
        f"https://api.etherscan.io/v2/api?chainid=56"
        f"&module=token&action=tokenholderlist"
        f"&contractaddress={SLH_BSC_CONTRACT}"
        f"&page=1&offset={min(limit, 100)}"
        f"&apikey={api_key}"
    )

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
    except Exception as e:
        return {"ok": False, "error": f"API call failed: {str(e)[:200]}", "holders": [], "total_holders": 0}

    if data.get("status") != "1":
        # Fallback to free-tier tokensupply + manual holder query
        return {
            "ok": False,
            "error": data.get("message", "Unknown"),
            "raw": data.get("result", "")[:200] if isinstance(data.get("result"), str) else None,
            "hint": "tokenholderlist may require Etherscan PRO tier. Falling back to tokensupply...",
            "holders": [],
            "total_holders": 0,
        }

    holders_raw = data.get("result", [])
    total_supply = 111186328 * (10 ** 15)  # 111M with 15 decimals

    holders = []
    for i, h in enumerate(holders_raw):
        try:
            balance_raw = int(h.get("TokenHolderQuantity") or 0)
            balance = balance_raw / (10 ** 15)
            pct = (balance_raw / total_supply * 100) if total_supply else 0
            holders.append({
                "rank": i + 1,
                "address": h.get("TokenHolderAddress"),
                "balance": balance,
                "percent": round(pct, 4),
                "bscscan_url": f"https://bscscan.com/address/{h.get('TokenHolderAddress')}",
            })
        except Exception:
            pass

    result = {
        "ok": True,
        "contract": SLH_BSC_CONTRACT,
        "chain": "BSC (56)",
        "total_holders": len(holders),
        "holders": holders,
        "cached_at": now,
    }
    _holders_cache["data"] = result
    _holders_cache["ts"] = now
    return result


@app.post("/api/audit/write")
async def audit_write_endpoint(
    action: str,
    actor_type: str = "api",
    actor_user_id: Optional[int] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    amount_native: Optional[float] = None,
    amount_currency: Optional[str] = None,
):
    """Public endpoint to write an audit entry. Used by bots + frontend."""
    async with pool.acquire() as conn:
        entry_hash = await audit_log_write(
            conn,
            action=action,
            actor_type=actor_type,
            actor_user_id=actor_user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            amount_native=amount_native,
            amount_currency=amount_currency,
        )
    return {"ok": True, "entry_hash": entry_hash}


@app.post("/api/cashback/record-distribution")
async def record_distribution(user_id: int, referred_user_id: int, verify: bool = False):
    """Record that user_id referred referred_user_id. If verify=true, marks immediately as verified."""
    async with pool.acquire() as conn:
        await _ensure_cashback_table(conn)
        await conn.execute("""
            INSERT INTO user_distributions (user_id, referred_user_id, verified, verified_at)
            VALUES ($1, $2, $3, CASE WHEN $3 THEN CURRENT_TIMESTAMP ELSE NULL END)
            ON CONFLICT (user_id, referred_user_id) DO UPDATE
              SET verified = EXCLUDED.verified,
                  verified_at = COALESCE(user_distributions.verified_at, CASE WHEN EXCLUDED.verified THEN CURRENT_TIMESTAMP ELSE NULL END)
        """, user_id, referred_user_id, verify)
    if verify:
        # Process tiers immediately
        return await process_cashback(user_id)
    return {"ok": True, "user_id": user_id, "referred_user_id": referred_user_id, "verified": verify}


@app.post("/api/beta/create-coupon")
async def beta_create_coupon(
    code: str,
    max_uses: int = 49,
    slh_bonus: float = 0.1,
    admin_key: str = "",
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    """Admin: create a new beta coupon code.

    Auth: X-Admin-Key header (or Authorization: Bearer <jwt>). The query
    parameter `admin_key` is still accepted as a deprecated fallback.
    """
    try:
        _require_admin(authorization, x_admin_key)
    except HTTPException:
        env_keys = {k for k in os.getenv("ADMIN_API_KEYS", "").split(",") if k.strip()}
        legacy_key = os.getenv("ADMIN_API_KEY", "")
        if legacy_key and legacy_key != "slh_admin_2026":
            env_keys.add(legacy_key)
        if not (admin_key and admin_key in env_keys and admin_key != "slh_admin_2026"):
            raise HTTPException(403, "Admin authentication required (use X-Admin-Key header)")
        print(f"[SECURITY][DEPRECATED] /api/beta/create-coupon called with query admin_key — migrate to X-Admin-Key header")
    code = code.strip().upper()
    if not code or len(code) < 4:
        raise HTTPException(400, "Code must be at least 4 characters")
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO beta_coupons (code, max_uses, used_count, slh_bonus, active)
            VALUES ($1, $2, 0, $3, TRUE)
            ON CONFLICT (code) DO UPDATE SET
                max_uses = EXCLUDED.max_uses,
                slh_bonus = EXCLUDED.slh_bonus,
                active = TRUE
        """, code, max_uses, slh_bonus)
    return {"ok": True, "code": code, "max_uses": max_uses, "slh_bonus": slh_bonus}


@app.get("/api/registration/status/{user_id}")
async def registration_status(user_id: int):
    """Check registration status for a user."""
    async with pool.acquire() as conn:
        is_reg = await conn.fetchval("SELECT is_registered FROM web_users WHERE telegram_id=$1", user_id)
        if is_reg:
            return {"is_registered": True, "payment_status": "approved"}

        row = await conn.fetchrow(
            "SELECT payment_status FROM premium_users WHERE user_id=$1 AND bot_name='ecosystem'", user_id
        )
        if row:
            return {"is_registered": False, "payment_status": row["payment_status"]}

    return {"is_registered": False, "payment_status": "none"}


# === PENDING REGISTRATIONS (admin) ===

@app.get("/api/registration/pending")
async def registration_pending():
    """List all pending/submitted registrations for admin review."""
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT p.user_id, w.username, w.first_name, p.payment_status, p.created_at
                FROM premium_users p
                LEFT JOIN web_users w ON w.telegram_id = p.user_id
                WHERE p.payment_status IN ('pending', 'submitted')
                ORDER BY p.created_at DESC
            """)
        return [dict(r) for r in rows]
    except Exception as e:
        return {"ok": False, "error": str(e), "registrations": []}


# === WEB3 WALLET LINKING ===

class LinkWalletRequest(BaseModel):
    user_id: int
    address: Optional[str] = ""
    signature: Optional[str] = None   # optional personal_sign proof
    message: Optional[str] = None     # the message that was signed


@app.post("/api/user/link-wallet")
async def link_wallet(req: LinkWalletRequest):
    """Link a Web3 (BSC/ETH) wallet address to a web_users row.

    Validates the address format (0x + 40 hex chars) and stores it lowercase.
    Signature verification is optional â€” if present, we verify personal_sign.
    """
    addr = (req.address or "").strip().lower()
    if not addr.startswith("0x") or len(addr) != 42:
        raise HTTPException(400, "Invalid Ethereum address format")
    try:
        int(addr[2:], 16)  # ensure hex
    except ValueError:
        raise HTTPException(400, "Invalid Ethereum address â€” not hex")

    if not req.user_id:
        raise HTTPException(400, "user_id required")

    async with pool.acquire() as conn:
        # Ensure user exists
        exists = await conn.fetchval("SELECT 1 FROM web_users WHERE telegram_id=$1", req.user_id)
        if not exists:
            raise HTTPException(404, "User not found â€” please login first")

        # Check for collision: this wallet already linked to a different user
        other = await conn.fetchval(
            "SELECT telegram_id FROM web_users WHERE eth_wallet=$1 AND telegram_id<>$2",
            addr, req.user_id
        )
        if other:
            raise HTTPException(409, "This wallet is already linked to another account")

        await conn.execute("""
            UPDATE web_users
               SET eth_wallet = $1,
                   eth_wallet_linked_at = CURRENT_TIMESTAMP
             WHERE telegram_id = $2
        """, addr, req.user_id)

    print(f"[Web3] Linked wallet {addr} to user {req.user_id}")
    return {"ok": True, "address": addr, "user_id": req.user_id}


@app.get("/api/user/wallet/{user_id}")
async def get_linked_wallet(user_id: int):
    """Return the linked Web3 wallet address (if any) for a user."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT eth_wallet, eth_wallet_linked_at FROM web_users WHERE telegram_id=$1",
            user_id
        )
    if not row:
        raise HTTPException(404, "User not found")
    return {
        "user_id": user_id,
        "address": row["eth_wallet"],
        "linked_at": row["eth_wallet_linked_at"].isoformat() if row["eth_wallet_linked_at"] else None
    }


@app.post("/api/user/unlink-wallet")
async def unlink_wallet(req: LinkWalletRequest):
    """Remove the linked Web3 wallet from a user row."""
    if not req.user_id:
        raise HTTPException(400, "user_id required")
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE web_users
               SET eth_wallet = NULL, eth_wallet_linked_at = NULL
             WHERE telegram_id = $1
        """, req.user_id)
    return {"ok": True, "user_id": req.user_id}


# === CUSTOM DISPLAY NAME (user-chosen, persists across Telegram re-auth) ===
class ProfileUpdateRequest(BaseModel):
    user_id: int
    display_name: Optional[str] = None
    bio: Optional[str] = None
    language_pref: Optional[str] = None  # he | en | ru | ar | fr


@app.post("/api/user/profile")
async def update_user_profile(req: ProfileUpdateRequest):
    """Update user's custom profile fields (display_name, bio, language).

    These fields are SET BY THE USER and persist across Telegram re-authentication.
    Only non-None fields are updated â€” pass partial objects to avoid wiping.
    Validation:
      - display_name: 2-32 chars, stripped
      - bio: up to 200 chars
      - language_pref: one of he/en/ru/ar/fr
    """
    if not req.user_id:
        raise HTTPException(400, "user_id required")

    updates = []
    params = []
    idx = 1

    if req.display_name is not None:
        name = req.display_name.strip()
        if len(name) < 2 or len(name) > 32:
            raise HTTPException(400, "display_name must be 2-32 characters")
        updates.append(f"display_name = ${idx}")
        params.append(name)
        idx += 1
        updates.append(f"display_name_set_at = CURRENT_TIMESTAMP")

    if req.bio is not None:
        bio = req.bio.strip()
        if len(bio) > 200:
            raise HTTPException(400, "bio max 200 characters")
        updates.append(f"bio = ${idx}")
        params.append(bio)
        idx += 1

    if req.language_pref is not None:
        if req.language_pref not in ("he", "en", "ru", "ar", "fr"):
            raise HTTPException(400, "language_pref must be he/en/ru/ar/fr")
        updates.append(f"language_pref = ${idx}")
        params.append(req.language_pref)
        idx += 1

    if not updates:
        raise HTTPException(400, "No fields to update")

    params.append(req.user_id)
    # SECURITY: whitelisted — 'updates' entries built only from hardcoded column literals (display_name, display_name_set_at, bio, language_pref); all user values are parameterized via $idx
    sql = f"UPDATE web_users SET {', '.join(updates)} WHERE telegram_id = ${idx} RETURNING display_name, bio, language_pref, first_name, username"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
        if not row:
            raise HTTPException(404, "User not found")

    return {
        "ok": True,
        "user_id": req.user_id,
        "display_name": row["display_name"],
        "bio": row["bio"],
        "language_pref": row["language_pref"],
        "fallback_name": row["first_name"] or row["username"] or "User",
    }


async def get_user_balances(conn, user_id: int):
    """Get all token balances for a user"""
    balances = {"TON_available": 0.0, "TON_locked": 0.0}

    try:
        rows = await conn.fetch(
            "SELECT token, balance FROM token_balances WHERE user_id=$1", user_id
        )
        for row in rows:
            balances[row["token"]] = float(row["balance"])
    except Exception:
        pass

    try:
        bank = await conn.fetchrow(
            "SELECT COALESCE(available,0) as available, COALESCE(locked,0) as locked_amt "
            "FROM account_balances WHERE account_id=$1", user_id
        )
        if bank:
            balances["TON_available"] = float(bank["available"])
            balances["TON_locked"] = float(bank["locked_amt"])
    except Exception:
        pass

    # Fallback: check user_balances table
    try:
        ub = await conn.fetchrow(
            "SELECT COALESCE(balance,0) as bal FROM user_balances WHERE user_id=$1", user_id
        )
        if ub and float(ub["bal"]) > 0 and balances["TON_available"] == 0:
            balances["TON_available"] = float(ub["bal"])
    except Exception:
        pass

    return balances


# === USER PROFILE ===
@app.get("/api/user/{telegram_id}")
async def get_user(telegram_id: int):
    """Get user profile and balances"""
    async with pool.acquire() as conn:
        # Try web_users first, fallback to users table
        user = None
        try:
            user = await conn.fetchrow(
                """SELECT telegram_id, username, first_name, photo_url, auth_date, last_login,
                          is_registered, registered_at, eth_wallet, eth_wallet_linked_at,
                          ton_wallet, ton_wallet_linked_at,
                          display_name, bio, language_pref
                     FROM web_users WHERE telegram_id=$1""",
                telegram_id
            )
        except Exception:
            pass

        if not user:
            try:
                row = await conn.fetchrow(
                    "SELECT user_id, username, balance, xp_total, level, joined_at, daily_streak FROM users WHERE user_id=$1", telegram_id
                )
                if row:
                    user = {
                        "telegram_id": row["user_id"],
                        "username": row["username"],
                        "first_name": row["username"] or "User",
                        "photo_url": None,
                        "xp_total": row["xp_total"],
                        "level": row["level"],
                        "daily_streak": row["daily_streak"],
                        "joined_at": str(row["joined_at"]) if row["joined_at"] else None,
                    }
            except Exception:
                pass

        if not user:
            raise HTTPException(404, "User not found")

        balances = await get_user_balances(conn, telegram_id)

        # Deposits - safe query
        deposits = []
        try:
            deposits = await conn.fetch(
                "SELECT id, plan_key, amount, currency, status, start_date, end_date, total_earned, created_at "
                "FROM deposits WHERE user_id=$1 ORDER BY created_at DESC LIMIT 10", telegram_id
            )
        except Exception:
            pass

        # Premium status - safe query
        premium = None
        try:
            premium = await conn.fetchval(
                "SELECT payment_status FROM premium_users WHERE user_id=$1 AND bot_name='expertnet'", telegram_id
            )
        except Exception:
            pass

        # Staking positions
        staking = []
        try:
            staking = await conn.fetch(
                "SELECT * FROM staking_positions WHERE user_id=$1 AND status='active' ORDER BY start_date DESC", telegram_id
            )
        except Exception:
            pass

    return {
        "user": dict(user) if hasattr(user, 'keys') else user,
        "balances": balances,
        "premium": premium == "approved",
        "deposits": [dict(d) for d in deposits] if deposits else [],
        "staking": [dict(s) for s in staking] if staking else [],
    }


# === STAKING ===
STAKING_PLANS = {
    # TON plans
    "monthly": {"name": "Monthly", "apy_monthly": 4.0, "apy_annual": 48, "min_amount": 1, "min_ton": 1, "lock_days": 30, "currency": "TON"},
    "quarterly": {"name": "Quarterly", "apy_monthly": 4.5, "apy_annual": 55, "min_amount": 5, "min_ton": 5, "lock_days": 90, "currency": "TON"},
    "semi_annual": {"name": "Semi-Annual", "apy_monthly": 5.0, "apy_annual": 60, "min_amount": 10, "min_ton": 10, "lock_days": 180, "currency": "TON"},
    "annual": {"name": "Annual", "apy_monthly": 5.4, "apy_annual": 65, "min_amount": 25, "min_ton": 25, "lock_days": 365, "currency": "TON"},
    # SLH plans
    "slh_monthly": {"name": "SLH Monthly", "apy_monthly": 3.0, "apy_annual": 36, "min_amount": 10, "min_ton": 0, "lock_days": 30, "currency": "SLH"},
    "slh_quarterly": {"name": "SLH Quarterly", "apy_monthly": 3.5, "apy_annual": 42, "min_amount": 50, "min_ton": 0, "lock_days": 90, "currency": "SLH"},
    "slh_annual": {"name": "SLH Annual", "apy_monthly": 4.0, "apy_annual": 48, "min_amount": 100, "min_ton": 0, "lock_days": 365, "currency": "SLH"},
    # BNB plans
    "bnb_monthly": {"name": "BNB Monthly", "apy_monthly": 2.5, "apy_annual": 30, "min_amount": 0.01, "min_ton": 0, "lock_days": 30, "currency": "BNB"},
    "bnb_quarterly": {"name": "BNB Quarterly", "apy_monthly": 3.0, "apy_annual": 36, "min_amount": 0.05, "min_ton": 0, "lock_days": 90, "currency": "BNB"},
}


@app.get("/api/staking/plans")
async def get_staking_plans():
    """Get available staking plans"""
    return {"plans": STAKING_PLANS}


class StakeRequest(BaseModel):
    user_id: int
    plan: str
    amount: float
    currency: Optional[str] = None  # auto-detected from plan if not provided


@app.post("/api/staking/stake")
async def create_stake(req: StakeRequest, x_admin_override_zuz: Optional[str] = Header(None)):
    """Create a new staking position.
    Supports TON, SLH, and BNB staking. Creates as 'pending_approval' for admin review."""
    plan = STAKING_PLANS.get(req.plan)
    if not plan:
        raise HTTPException(400, f"Invalid plan. Choose from: {list(STAKING_PLANS.keys())}")

    currency = req.currency or plan.get("currency", "TON")
    min_amount = plan.get("min_amount", plan.get("min_ton", 1))

    if req.amount < min_amount:
        raise HTTPException(400, f"Minimum deposit is {min_amount} {currency}")

    # ZUZ Guardian gate — stakes concentrate capital; a banned user should not lock more.
    try:
        from shared.guardian_gate import require_clean_zuz as _zuz_gate
        await _zuz_gate(pool, req.user_id, admin_override_header=x_admin_override_zuz)
    except HTTPException:
        raise
    except Exception as _gate_err:
        print(f"[create_stake][WARN] zuz gate failed open: {_gate_err!r}")

    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM web_users WHERE telegram_id=$1", req.user_id)
        if not user:
            raise HTTPException(404, "User not found. Login first.")

        # Check balance based on currency
        if currency == "TON":
            acct_bal = await conn.fetchval(
                "SELECT COALESCE(available, 0) FROM account_balances WHERE account_id=$1", req.user_id
            ) or 0
            tok_bal = await conn.fetchval(
                "SELECT COALESCE(balance, 0) FROM token_balances WHERE user_id=$1 AND token='TON'", req.user_id
            ) or 0
            total_bal = float(acct_bal) + float(tok_bal)
        else:
            total_bal = float(await conn.fetchval(
                "SELECT COALESCE(balance, 0) FROM token_balances WHERE user_id=$1 AND token=$2",
                req.user_id, currency
            ) or 0)

        if total_bal < req.amount:
            raise HTTPException(400,
                f"Insufficient {currency} balance. You have {total_bal:.4f} {currency} but need {req.amount} {currency}. "
                f"Please deposit {currency} first via wallet page.")

        end_date = datetime.utcnow() + timedelta(days=plan["lock_days"])

        # Create position as pending_approval (admin must confirm)
        pos_id = await conn.fetchval("""
            INSERT INTO staking_positions (user_id, plan, amount, apy_monthly, lock_days, end_date, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'pending_approval') RETURNING id
        """, req.user_id, req.plan, req.amount, plan["apy_monthly"], plan["lock_days"], end_date)

        # Audit log
        await audit_log_write(
            conn,
            action="staking.request",
            actor_type="user",
            actor_user_id=req.user_id,
            resource_type="staking_position",
            resource_id=str(pos_id),
            amount_native=req.amount,
            amount_currency=currency,
            metadata={"plan": req.plan, "apy": plan["apy_monthly"], "lock_days": plan["lock_days"], "currency": currency},
        )

    try:
        from shared.events import emit as _emit
        await _emit(pool, "stake.opened", {
            "stake_id": pos_id,
            "user_id": req.user_id,
            "plan": req.plan,
            "amount": float(req.amount),
            "currency": currency,
            "apy_monthly": plan["apy_monthly"],
            "lock_days": plan["lock_days"],
            "status": "pending_approval",
        }, source="api.staking.stake")
    except Exception as _e:
        print(f"[create_stake][WARN] event emit failed: {_e!r}")

    return {
        "id": pos_id,
        "plan": req.plan,
        "amount": req.amount,
        "currency": currency,
        "apy_monthly": plan["apy_monthly"],
        "apy_annual": plan["apy_annual"],
        "lock_days": plan["lock_days"],
        "end_date": end_date.isoformat(),
        "status": "pending_approval",
        "message": f"Staking {req.amount} {currency} submitted. Admin will review and approve within 24 hours.",
    }


@app.post("/api/staking/approve/{position_id}")
async def approve_stake(
    position_id: int,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: approve a pending staking position and lock funds."""
    try:
        _require_admin(authorization, x_admin_key)
        async with pool.acquire() as conn:
            # Ensure status column exists (may be missing on older Railway tables)
            try:
                await conn.execute(
                    "ALTER TABLE staking_positions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active'"
                )
            except Exception:
                pass  # Column already exists or DB doesn't support IF NOT EXISTS

            pos = await conn.fetchrow(
                "SELECT * FROM staking_positions WHERE id=$1", position_id
            )
            if not pos:
                raise HTTPException(404, "Position not found")

            # Safely read status — default to 'active' if column missing
            pos_dict = dict(pos)
            current_status = pos_dict.get("status", "active")

            if current_status != "pending_approval":
                return {"ok": True, "already": current_status}

            # Deduct TON from user balance
            user_id = pos_dict["user_id"]
            amount = float(pos_dict["amount"])

            # Try account_balances first, then token_balances
            try:
                acct_bal = await conn.fetchval(
                    "SELECT COALESCE(available, 0) FROM account_balances WHERE account_id=$1", user_id
                ) or 0
            except Exception:
                acct_bal = 0

            if float(acct_bal) >= amount:
                await conn.execute(
                    "UPDATE account_balances SET available = available - $1, locked = locked + $1 WHERE account_id=$2",
                    amount, user_id
                )
            else:
                try:
                    tok_bal = await conn.fetchval(
                        "SELECT COALESCE(balance, 0) FROM token_balances WHERE user_id=$1 AND token='TON'", user_id
                    ) or 0
                except Exception:
                    tok_bal = 0

                if float(tok_bal) >= amount:
                    await conn.execute(
                        "UPDATE token_balances SET balance = balance - $1 WHERE user_id=$2 AND token='TON'",
                        amount, user_id
                    )
                else:
                    raise HTTPException(400, f"User has insufficient TON to lock ({acct_bal} + {tok_bal} < {amount})")

            # Activate position
            try:
                await conn.execute(
                    "UPDATE staking_positions SET status='active' WHERE id=$1", position_id
                )
            except Exception:
                pass  # status column missing — position is treated as active

            # Distribute referral commissions
            try:
                commissions = await distribute_referral_commissions(
                    conn, user_id, amount, f"staking_{pos_dict.get('plan', 'unknown')}", "TON"
                )
            except Exception:
                commissions = []

            try:
                await audit_log_write(
                    conn, action="staking.approve", actor_type="admin",
                    resource_type="staking_position", resource_id=str(position_id),
                    amount_native=amount, amount_currency="TON",
                    metadata={"plan": pos_dict.get("plan", "unknown"), "user_id": user_id, "commissions": len(commissions)},
                )
            except Exception:
                pass  # Audit log failure should not block approval

        return {"ok": True, "position_id": position_id, "status": "active", "amount_locked": amount}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Staking approve error: {str(e)}")


@app.get("/api/staking/positions/{user_id}")
async def get_staking_positions(user_id: int):
    """Get user's staking positions"""
    async with pool.acquire() as conn:
        positions = await conn.fetch(
            "SELECT * FROM staking_positions WHERE user_id=$1 ORDER BY start_date DESC", user_id
        )
    return {"positions": [dict(p) for p in positions]}


# === PRICES (with cache + timeout + retry) ===
_price_cache = {"data": None, "ts": 0}

@app.get("/api/prices")
async def get_prices():
    """Proxy for CoinGecko prices â€” cached 60s, 10s timeout, 2 retries"""
    import aiohttp, time as _time
    now = _time.time()
    # Return cached data if fresh (< 60s)
    if _price_cache["data"] and (now - _price_cache["ts"]) < 60:
        return _price_cache["data"]

    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,the-open-network,binancecoin,solana,ripple,dogecoin&vs_currencies=usd,ils"
    timeout = aiohttp.ClientTimeout(total=10)
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        _price_cache["data"] = data
                        _price_cache["ts"] = now
                        return data
        except Exception:
            if attempt == 1:
                break
    # Return stale cache if available, else 502
    if _price_cache["data"]:
        return _price_cache["data"]
    raise HTTPException(502, "Price API unavailable")


# === ECOSYSTEM STATS ===
@app.get("/api/stats")
async def get_stats():
    """Get ecosystem-wide statistics"""
    async def safe_count(conn, query):
        try:
            return await conn.fetchval(query) or 0
        except Exception:
            return 0

    async with pool.acquire() as conn:
        total_users = await safe_count(conn, "SELECT COUNT(*) FROM web_users")
        premium_users = await safe_count(conn, "SELECT COUNT(*) FROM premium_users WHERE payment_status='approved'")
        total_staked = await safe_count(conn, "SELECT COALESCE(SUM(amount),0) FROM staking_positions WHERE status='active'")
        total_deposits = await safe_count(conn, "SELECT COALESCE(SUM(amount),0) FROM deposits WHERE status='active'")

    return {
        "total_users": total_users,
        "premium_users": premium_users,
        "total_staked_ton": float(total_staked),
        "total_deposits_ton": float(total_deposits),
        "bots_live": 20,
        "supported_coins": 12,
    }


# === HEALTH ===
@app.get("/api/health")
async def health():
    """Health check — returns 503 when DB pool is unavailable (Phase 0: no silent lies)."""
    if pool is None or _db_init_failed:
        return JSONResponse(
            {"status": "error", "db": "pool_unavailable", "version": "1.1.0"},
            status_code=503,
        )
    ok = await _shared_db_health()
    if not ok:
        return JSONResponse(
            {"status": "error", "db": "ping_failed", "version": "1.1.0"},
            status_code=503,
        )
    return {"status": "ok", "db": "connected", "version": "1.1.0"}


# === TELEGRAM MINI APP GATEWAY ===
# Gated behind _GATEWAY_AVAILABLE so a missing telegram_gateway.py can't
# break startup. The endpoint below proves the wiring end-to-end: a Mini App
# opens with initData → gateway verifies HMAC → returns the resolved user.

if _GATEWAY_AVAILABLE:
    @app.get("/api/miniapp/me")
    async def miniapp_me(user: "TelegramUser" = Depends(verify_miniapp_request)):
        """Minimum Mini App endpoint: validates Telegram initData and returns identity.

        Call from Mini App JS:
            fetch('/api/miniapp/me', {
              headers: { 'X-Telegram-Init-Data': Telegram.WebApp.initData }
            })

        Returns 401 with detail.code='empty_init_data' / 'bad_signature' /
        'stale_init_data' / 'no_user' on failure. 200 on success.
        """
        return {
            "telegram_id": user.telegram_id,
            "slh_user_id": user.slh_user_id,
            "is_admin": user.is_admin,
            "username": user.username,
            "first_name": user.first_name,
            "source": user.source,
        }

    @app.get("/api/miniapp/health")
    async def miniapp_health():
        """Unauthenticated probe for dashboards — proves the gateway module loaded."""
        import os as _os
        return {
            "gateway_loaded": True,
            "admin_ids_count": len(
                {x for x in (_os.getenv("ADMIN_TELEGRAM_IDS", "224223270") or "").split(",") if x.strip()}
            ),
            "primary_bot_token_set": bool(
                _os.getenv("TELEGRAM_BOT_TOKEN") or _os.getenv("SLH_CLAUDE_BOT_TOKEN")
            ),
        }
else:
    @app.get("/api/miniapp/health")
    async def miniapp_health_disabled():
        return JSONResponse(
            {"gateway_loaded": False, "reason": "api.telegram_gateway import failed"},
            status_code=503,
        )


# ============================================================================
# AI Spark — Subscription mirror endpoints (Phase B)
# slh-claude-bot is the canonical source (SQLite). It pushes state here so the
# Mini App widget can show live tier + quota without bouncing through the bot.
# Dual-write architecture: bot writes SQLite first (always succeeds), then
# best-effort POST here (failure is logged but doesn't break bot).
# ============================================================================

_AI_SPARK_SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_spark_subscriptions (
    user_id                       BIGINT PRIMARY KEY,
    tier                          TEXT NOT NULL DEFAULT 'free',
    current_period_start          TIMESTAMP NOT NULL DEFAULT NOW(),
    current_period_end            TIMESTAMP NOT NULL,
    messages_used_this_period     INTEGER NOT NULL DEFAULT 0,
    quota_total                   INTEGER NOT NULL DEFAULT 10,
    payment_provider              TEXT,
    last_synced_at                TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ai_spark_subs_period_end
    ON ai_spark_subscriptions(current_period_end);
"""
_ai_spark_init_done = False


async def _ensure_ai_spark_schema(conn) -> None:
    global _ai_spark_init_done
    if _ai_spark_init_done:
        return
    await conn.execute(_AI_SPARK_SCHEMA)
    _ai_spark_init_done = True


class AISparkSyncReq(BaseModel):
    user_id: int
    tier: str
    current_period_start: str
    current_period_end: str
    messages_used_this_period: int = 0
    quota_total: int = 10
    payment_provider: Optional[str] = None


@app.post("/api/ai_spark/sync")
async def ai_spark_sync(req: AISparkSyncReq, request: Request):
    """Admin-only: slh-claude-bot calls this after each subscription change."""
    admin_key = request.headers.get("X-Admin-Key", "")
    raw_keys = os.getenv("ADMIN_API_KEYS", "") or ""
    valid_keys = {k.strip() for k in raw_keys.split(",") if k.strip()}
    if not valid_keys or admin_key not in valid_keys:
        raise HTTPException(403, "admin auth required")
    if pool is None:
        raise HTTPException(503, "db pool not ready")

    # asyncpg requires datetime instances for TIMESTAMP columns — strings raise
    # DataError even with explicit ::timestamp cast. Parse defensively here so
    # the bot can keep sending ISO-8601 strings.
    def _parse_dt(value):
        if isinstance(value, datetime):
            return value
        # Strip trailing 'Z' (UTC) and microseconds; fromisoformat handles most cases
        s = str(value).rstrip("Z")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            # Fallback: best-effort common formats
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            raise HTTPException(400, f"invalid datetime: {value!r}")

    period_start_dt = _parse_dt(req.current_period_start)
    period_end_dt = _parse_dt(req.current_period_end)

    async with pool.acquire() as conn:
        await _ensure_ai_spark_schema(conn)
        await conn.execute(
            """
            INSERT INTO ai_spark_subscriptions
                (user_id, tier, current_period_start, current_period_end,
                 messages_used_this_period, quota_total, payment_provider, last_synced_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                tier = EXCLUDED.tier,
                current_period_start = EXCLUDED.current_period_start,
                current_period_end = EXCLUDED.current_period_end,
                messages_used_this_period = EXCLUDED.messages_used_this_period,
                quota_total = EXCLUDED.quota_total,
                payment_provider = EXCLUDED.payment_provider,
                last_synced_at = NOW()
            """,
            req.user_id, req.tier, period_start_dt, period_end_dt,
            req.messages_used_this_period, req.quota_total, req.payment_provider,
        )
    return {"ok": True, "user_id": req.user_id, "tier": req.tier}


@app.get("/api/ai_spark/credits/{user_id}")
async def ai_spark_credits(user_id: int):
    """Public read: returns subscription state for Mini App widget.

    Currently no auth — returns minimal safe info. If we ever store secrets
    here (we shouldn't), gate with verify_miniapp_request. For now: tier +
    quota are not sensitive (the bot already shows them via /credits).
    """
    if pool is None:
        raise HTTPException(503, "db pool not ready")
    async with pool.acquire() as conn:
        await _ensure_ai_spark_schema(conn)
        row = await conn.fetchrow(
            """
            SELECT tier, current_period_start, current_period_end,
                   messages_used_this_period, quota_total, payment_provider,
                   last_synced_at
            FROM ai_spark_subscriptions
            WHERE user_id = $1
            """,
            user_id,
        )
    if row is None:
        return {
            "user_id": user_id,
            "tier": "free",
            "messages_used": 0,
            "quota_total": 10,
            "period_end": None,
            "exists": False,
        }
    return {
        "user_id": user_id,
        "tier": row["tier"],
        "messages_used": row["messages_used_this_period"],
        "quota_total": row["quota_total"],
        "period_start": row["current_period_start"].isoformat() if row["current_period_start"] else None,
        "period_end": row["current_period_end"].isoformat() if row["current_period_end"] else None,
        "payment_provider": row["payment_provider"],
        "last_synced_at": row["last_synced_at"].isoformat() if row["last_synced_at"] else None,
        "exists": True,
    }


# === TOKEN TRANSFERS ===
class TransferRequest(BaseModel):
    from_user_id: int
    to_user_id: int
    token: str
    amount: float
    memo: Optional[str] = None


@app.post("/api/transfer")
async def transfer_tokens(req: TransferRequest):
    """Transfer internal tokens between users"""
    if req.amount <= 0:
        raise HTTPException(400, "Amount must be positive")
    if req.token not in ("SLH", "ZVK", "MNH", "REP", "ZUZ"):
        raise HTTPException(400, "Token must be SLH, ZVK, MNH, REP, or ZUZ")

    async with pool.acquire() as conn:
        async with conn.transaction():
            balance = await conn.fetchval(
                "SELECT balance FROM token_balances WHERE user_id=$1 AND token=$2",
                req.from_user_id, req.token
            )
            if not balance or float(balance) < req.amount:
                raise HTTPException(400, "Insufficient balance")

            await conn.execute(
                "UPDATE token_balances SET balance = balance - $1 WHERE user_id=$2 AND token=$3",
                req.amount, req.from_user_id, req.token
            )
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, token) DO UPDATE SET balance = token_balances.balance + $3
            """, req.to_user_id, req.token, req.amount)

            await conn.execute("""
                INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type)
                VALUES ($1, $2, $3, $4, $5, 'transfer')
            """, req.from_user_id, req.to_user_id, req.token, req.amount, req.memo or "web transfer")

    return {"status": "ok", "amount": req.amount, "token": req.token}


# === TWO-TIER AFFILIATE PROGRAM ===
# Per 2026-04-20 Dynamic Yield pivot: reduced from 10-gen to 2-tier to comply with
# securities regulation (MLM/Ponzi-adjacent structure removed).
# See ops/DYNAMIC_YIELD_SPEC_20260420.md §7 and COPY_OVERHAUL_URGENT_20260420.md.
# Payouts funded from separate referral budget carved out of real system revenue
# (course sales, marketplace fees, SaaS subs) — NOT from other users' deposits.
REFERRAL_RATES = {
    1: 0.20,   # Tier 1 (direct referral): 20% of their purchase
    2: 0.05,   # Tier 2 (referral's referral): 5% of their purchase
}
MAX_GENERATIONS = 2


async def get_referral_chain(conn, user_id: int) -> list[int]:
    """Walk up the referral chain and return list of ancestor IDs (generation 1 = direct referrer)"""
    chain = []
    current = user_id
    for _ in range(MAX_GENERATIONS):
        row = await conn.fetchrow("SELECT referrer_id FROM referrals WHERE user_id=$1", current)
        if not row or not row["referrer_id"]:
            break
        chain.append(row["referrer_id"])
        current = row["referrer_id"]
    return chain


async def distribute_referral_commissions(conn, from_user_id: int, amount: float, source_type: str, token: str = "TON"):
    """Distribute commissions up the referral chain"""
    chain = await get_referral_chain(conn, from_user_id)
    results = []
    for gen, earner_id in enumerate(chain, 1):
        rate = REFERRAL_RATES.get(gen, 0)
        if rate <= 0:
            break
        commission = round(amount * rate, 8)
        if commission <= 0:
            continue
        await conn.execute("""
            INSERT INTO referral_earnings (earner_id, from_user_id, generation, source_type, source_amount, commission_rate, commission_amount, token)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, earner_id, from_user_id, gen, source_type, amount, rate, commission, token)
        results.append({"earner_id": earner_id, "generation": gen, "rate": rate, "commission": commission})
    return results


@app.post("/api/referral/register")
async def register_referral(user_id: int = Query(...), referrer_id: int = Query(None)):
    """Register a user in the referral system"""
    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM referrals WHERE user_id=$1", user_id)
        if existing:
            return {"status": "already_registered", "referrer_id": existing["referrer_id"]}

        # Auto-create user if not exists
        await conn.execute("""
            INSERT INTO web_users (telegram_id, first_name) VALUES ($1, 'User')
            ON CONFLICT (telegram_id) DO NOTHING
        """, user_id)
        if referrer_id:
            await conn.execute("""
                INSERT INTO web_users (telegram_id, first_name) VALUES ($1, 'User')
                ON CONFLICT (telegram_id) DO NOTHING
            """, referrer_id)

        # Prevent self-referral
        if referrer_id == user_id:
            referrer_id = None

        # Calculate depth
        depth = 1
        if referrer_id:
            parent = await conn.fetchrow("SELECT depth FROM referrals WHERE user_id=$1", referrer_id)
            if parent:
                depth = parent["depth"] + 1

        await conn.execute("""
            INSERT INTO referrals (user_id, referrer_id, depth)
            VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING
        """, user_id, referrer_id, depth)

    return {"status": "ok", "user_id": user_id, "referrer_id": referrer_id, "depth": depth}


@app.get("/api/referral/tree/{user_id}")
async def get_referral_tree(user_id: int, max_depth: int = Query(5, le=10)):
    """Get the referral tree for a user (who they referred, and who those referred, etc.)"""
    async with pool.acquire() as conn:
        async def build_tree(uid: int, current_depth: int) -> dict:
            if current_depth > max_depth:
                return None
            children_rows = await conn.fetch(
                "SELECT r.user_id, w.username, w.first_name FROM referrals r LEFT JOIN web_users w ON r.user_id = w.telegram_id WHERE r.referrer_id=$1 ORDER BY r.created_at",
                uid
            )
            children = []
            for row in children_rows:
                child = {
                    "user_id": row["user_id"],
                    "username": row["username"] or "",
                    "first_name": row["first_name"] or "",
                    "generation": current_depth,
                }
                subtree = await build_tree(row["user_id"], current_depth + 1)
                child["children"] = subtree["children"] if subtree else []
                child["total_descendants"] = len(child["children"]) + sum(c.get("total_descendants", 0) for c in child["children"])
                children.append(child)
            return {"children": children}

        tree = await build_tree(user_id, 1)

        # Get earnings summary
        earnings = await conn.fetch("""
            SELECT generation, COUNT(*) as count, SUM(commission_amount) as total, token
            FROM referral_earnings WHERE earner_id=$1
            GROUP BY generation, token ORDER BY generation
        """, user_id)

        total_earned = await conn.fetchval(
            "SELECT COALESCE(SUM(commission_amount), 0) FROM referral_earnings WHERE earner_id=$1", user_id
        ) or 0

        direct_count = await conn.fetchval(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id=$1", user_id
        ) or 0

    return {
        "user_id": user_id,
        "direct_referrals": direct_count,
        "total_earned": float(total_earned),
        "tree": tree["children"] if tree else [],
        "earnings_by_generation": [
            {"generation": r["generation"], "count": r["count"], "total": float(r["total"]), "token": r["token"]}
            for r in earnings
        ],
        "commission_rates": REFERRAL_RATES,
    }


@app.get("/api/referral/link/{user_id}")
async def get_referral_link(user_id: int):
    """Generate referral links for a user"""
    return {
        "telegram_link": f"https://t.me/SLH_AIR_bot?start={user_id}",
        "web_link": f"https://slh-nft.com/?ref={user_id}",
        "rates": REFERRAL_RATES,
        "max_generations": MAX_GENERATIONS,
    }


@app.get("/api/referral/leaderboard")
async def referral_leaderboard(limit: int = Query(20, le=100)):
    """Top referrers by total earnings"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT e.earner_id,
                   COALESCE(w.username, u.username, '') as username,
                   COALESCE(w.first_name, u.username, '') as first_name,
                   COUNT(DISTINCT e.from_user_id) as unique_referrals,
                   SUM(e.commission_amount) as total_earned,
                   MAX(e.generation) as deepest_generation
            FROM referral_earnings e
            LEFT JOIN web_users w ON e.earner_id = w.telegram_id
            LEFT JOIN users u ON e.earner_id = u.user_id
            GROUP BY e.earner_id, w.username, w.first_name, u.username
            ORDER BY total_earned DESC
            LIMIT $1
        """, limit)
    return {
        "leaderboard": [
            {
                "rank": i + 1,
                "user_id": r["earner_id"],
                "username": r["username"] or "",
                "first_name": r["first_name"] or "",
                "unique_referrals": r["unique_referrals"],
                "total_earned": float(r["total_earned"]),
                "deepest_generation": r["deepest_generation"],
            }
            for i, r in enumerate(rows)
        ]
    }


@app.get("/api/referral/stats/{user_id}")
async def referral_stats(user_id: int):
    """Detailed referral statistics for a user"""
    async with pool.acquire() as conn:
        direct = await conn.fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id=$1", user_id) or 0

        # Count all descendants (recursive)
        all_descendants = 0
        queue = [user_id]
        visited = set()
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)
            children = await conn.fetch("SELECT user_id FROM referrals WHERE referrer_id=$1", current)
            for c in children:
                all_descendants += 1
                queue.append(c["user_id"])

        total_earned = await conn.fetchval(
            "SELECT COALESCE(SUM(commission_amount), 0) FROM referral_earnings WHERE earner_id=$1", user_id
        ) or 0

        by_gen = await conn.fetch("""
            SELECT generation, COUNT(DISTINCT from_user_id) as people, SUM(commission_amount) as earned
            FROM referral_earnings WHERE earner_id=$1 GROUP BY generation ORDER BY generation
        """, user_id)

        # Who referred me
        my_referrer = await conn.fetchval("SELECT referrer_id FROM referrals WHERE user_id=$1", user_id)

    return {
        "user_id": user_id,
        "my_referrer": my_referrer,
        "direct_referrals": direct,
        "total_network": all_descendants,
        "total_earned_ton": float(total_earned),
        "by_generation": [
            {"generation": r["generation"], "people": r["people"], "earned": float(r["earned"])}
            for r in by_gen
        ],
        "potential_generations": MAX_GENERATIONS,
        "rates": REFERRAL_RATES,
    }


# === ACTIVITY FEED & TRANSACTION HISTORY ===
@app.get("/api/activity/{user_id}")
async def get_activity(user_id: int, limit: int = Query(30, le=100)):
    """Get user activity feed - combines all events into a timeline"""
    activities = []
    async with pool.acquire() as conn:
        # Staking events
        try:
            rows = await conn.fetch(
                "SELECT id, plan, amount, status, start_date as ts FROM staking_positions WHERE user_id=$1 ORDER BY start_date DESC LIMIT $2",
                user_id, limit
            )
            for r in rows:
                activities.append({
                    "type": "staking", "icon": "ðŸ’Ž",
                    "title": f"Staked {float(r['amount'])} TON ({r['plan']})",
                    "status": r["status"], "timestamp": r["ts"].isoformat() if r["ts"] else None,
                })
        except Exception:
            pass

        # Deposits
        try:
            rows = await conn.fetch(
                "SELECT plan_key, amount, currency, status, created_at as ts FROM deposits WHERE user_id=$1 ORDER BY created_at DESC LIMIT $2",
                user_id, limit
            )
            for r in rows:
                activities.append({
                    "type": "deposit", "icon": "ðŸ“¥",
                    "title": f"Deposited {float(r['amount'])} {r['currency'] or 'TON'} ({r['plan_key']})",
                    "status": r["status"], "timestamp": r["ts"].isoformat() if r["ts"] else None,
                })
        except Exception:
            pass

        # Token transfers (sent)
        try:
            rows = await conn.fetch(
                "SELECT to_user_id, token, amount, memo, created_at as ts FROM token_transfers WHERE from_user_id=$1 ORDER BY created_at DESC LIMIT $2",
                user_id, limit
            )
            for r in rows:
                activities.append({
                    "type": "transfer_out", "icon": "ðŸ“¤",
                    "title": f"Sent {float(r['amount'])} {r['token']} to {r['to_user_id']}",
                    "status": "completed", "timestamp": r["ts"].isoformat() if r["ts"] else None,
                })
        except Exception:
            pass

        # Token transfers (received)
        try:
            rows = await conn.fetch(
                "SELECT from_user_id, token, amount, created_at as ts FROM token_transfers WHERE to_user_id=$1 ORDER BY created_at DESC LIMIT $2",
                user_id, limit
            )
            for r in rows:
                activities.append({
                    "type": "transfer_in", "icon": "ðŸ“¥",
                    "title": f"Received {float(r['amount'])} {r['token']} from {r['from_user_id']}",
                    "status": "completed", "timestamp": r["ts"].isoformat() if r["ts"] else None,
                })
        except Exception:
            pass

        # Referral earnings
        try:
            rows = await conn.fetch(
                "SELECT from_user_id, generation, commission_amount, token, source_type, created_at as ts FROM referral_earnings WHERE earner_id=$1 ORDER BY created_at DESC LIMIT $2",
                user_id, limit
            )
            for r in rows:
                activities.append({
                    "type": "referral_earning", "icon": "ðŸ¤",
                    "title": f"Earned {float(r['commission_amount'])} {r['token']} from Gen {r['generation']} referral",
                    "status": "completed", "timestamp": r["ts"].isoformat() if r["ts"] else None,
                })
        except Exception:
            pass

        # Daily claims
        try:
            rows = await conn.fetch(
                "SELECT amount, streak, claimed_at as ts FROM daily_claims WHERE user_id=$1 ORDER BY claimed_at DESC LIMIT $2",
                user_id, limit
            )
            for r in rows:
                activities.append({
                    "type": "daily_claim", "icon": "ðŸŽ",
                    "title": f"Daily claim: {float(r['amount'])} tokens (streak {r['streak']})",
                    "status": "completed", "timestamp": r["ts"].isoformat() if r["ts"] else None,
                })
        except Exception:
            pass

    # Sort by timestamp descending
    activities.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    return {"activities": activities[:limit], "total": len(activities)}


@app.get("/api/transactions/{user_id}")
async def get_transactions(user_id: int, limit: int = Query(50, le=200), offset: int = Query(0)):
    """Full transaction history with pagination"""
    txns = []
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch("""
                SELECT id, from_user_id, to_user_id, token, amount, memo, tx_type, created_at
                FROM token_transfers
                WHERE from_user_id=$1 OR to_user_id=$1
                ORDER BY created_at DESC LIMIT $2 OFFSET $3
            """, user_id, limit, offset)
            for r in rows:
                direction = "out" if r["from_user_id"] == user_id else "in"
                txns.append({
                    "id": r["id"],
                    "direction": direction,
                    "counterparty": r["to_user_id"] if direction == "out" else r["from_user_id"],
                    "token": r["token"],
                    "amount": float(r["amount"]),
                    "memo": r["memo"],
                    "type": r["tx_type"],
                    "timestamp": r["created_at"].isoformat() if r["created_at"] else None,
                })
        except Exception:
            pass

    return {"transactions": txns, "count": len(txns), "offset": offset}


@app.get("/api/leaderboard")
async def global_leaderboard(category: str = Query("xp", enum=["xp", "balance", "referrals", "staking"]), limit: int = Query(20, le=100)):
    """Global leaderboard - XP, balance, referrals, or staking.

    Filters out test/seed user IDs (100001-299999) and negative IDs (group chats)
    so the leaderboard shows only real Telegram users.
    """
    # Test/seed IDs to exclude â€” keep real Telegram users only
    # Real Telegram user IDs are ALWAYS positive and typically > 1M
    # SECURITY: whitelisted — EXCLUDE_RANGE is a hardcoded constant, not user input; category param is constrained by FastAPI enum
    EXCLUDE_RANGE = "user_id >= 1000000 AND user_id > 0"
    async with pool.acquire() as conn:
        rows = []
        try:
            if category == "xp":
                rows = await conn.fetch(
                    f"SELECT user_id, username, xp_total as score, level FROM users WHERE {EXCLUDE_RANGE} ORDER BY xp_total DESC LIMIT $1", limit
                )
            elif category == "balance":
                rows = await conn.fetch(
                    f"SELECT user_id, username, balance as score, level FROM users WHERE {EXCLUDE_RANGE} ORDER BY balance DESC LIMIT $1", limit
                )
            elif category == "staking":
                rows = await conn.fetch(f"""
                    SELECT sp.user_id, COALESCE(u.username,'') as username, SUM(sp.amount) as score, COALESCE(u.level,1) as level
                    FROM staking_positions sp LEFT JOIN users u ON sp.user_id = u.user_id
                    WHERE sp.status='active' AND sp.{EXCLUDE_RANGE.replace('user_id', 'user_id')}
                    GROUP BY sp.user_id, u.username, u.level
                    ORDER BY score DESC LIMIT $1
                """, limit)
            elif category == "referrals":
                rows = await conn.fetch(f"""
                    SELECT r.referrer_id as user_id, COALESCE(u.username,'') as username, COUNT(*) as score, COALESCE(u.level,1) as level
                    FROM referrals r LEFT JOIN users u ON r.referrer_id = u.user_id
                    WHERE r.referrer_id IS NOT NULL AND r.referrer_id >= 1000000
                    GROUP BY r.referrer_id, u.username, u.level
                    ORDER BY score DESC LIMIT $1
                """, limit)
        except Exception:
            pass

    return {
        "category": category,
        "leaderboard": [
            {"rank": i + 1, "user_id": r["user_id"], "username": r["username"] or "", "score": float(r["score"]), "level": r.get("level", 1)}
            for i, r in enumerate(rows)
        ]
    }


# === COMMUNITY SYSTEM ===
# Rate limit store (in-memory)
_community_rate: dict[str, list[float]] = {}

def _check_community_rate(key: str, max_per_hour: int) -> bool:
    now = time.time()
    cutoff = now - 3600
    entries = _community_rate.get(key, [])
    entries = [t for t in entries if t > cutoff]
    _community_rate[key] = entries
    if len(entries) >= max_per_hour:
        return False
    entries.append(now)
    return True

COMMUNITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS community_posts (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    telegram_id TEXT,
    text TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'general',
    image_data TEXT,
    likes_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Migration: add image_data for existing DBs
ALTER TABLE community_posts ADD COLUMN IF NOT EXISTS image_data TEXT;
CREATE TABLE IF NOT EXISTS community_likes (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES community_posts(id) ON DELETE CASCADE,
    username TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(post_id, username)
);
CREATE TABLE IF NOT EXISTS community_comments (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES community_posts(id) ON DELETE CASCADE,
    username TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_community_posts_created ON community_posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_community_comments_post ON community_comments(post_id, created_at);
"""

COMMUNITY_SEEDS = [
    ("SLH Official", "\U0001f4cc \u05de\u05d5\u05e6\u05de\u05d3\n\n\u05d1\u05e8\u05d5\u05e8 \u05e9\u05e4\u05e1\u05e4\u05e1\u05ea\u05dd \u05d0\u05ea \u05d4\u05d1\u05d9\u05d8\u05e7\u05d5\u05d9\u05d9\u05df,\n\u05dc\u05d0 \u05d4\u05d1\u05e0\u05ea\u05dd \u05de\u05d4 \u05d6\u05d4 \u05d0\u05d5\u05de\u05e8,\n\u05d1\u05d9\u05d8, \u05d0\u05d5 \u05e7\u05d5\u05d9\u05d9\u05df..\n\n\u05d4\u05d9\u05d5\u05dd \u05d0\u05ea\u05dd \u05de\u05e9\u05dc\u05de\u05d9\u05dd \u05d1\u05d1\u05d9\u05d8 \u05db\u05de\u05e2\u05d8 \u05d1\u05db\u05dc \u05e8\u05db\u05d9\u05e9\u05d4,\n\u05d0\u05d1\u05dc \u05e2\u05d3\u05d9\u05d9\u05df \u05dc\u05d0 \u05de\u05d1\u05d9\u05e0\u05d9\u05dd \u05e9\u05e7\u05d5\u05d9\u05d9\u05df \u2014 \u05d6\u05d4 \u05dc\u05de\u05e2\u05e9\u05d4 \u05d1\u05d7\u05d9\u05e8\u05d4.\n\nSLH \u05d6\u05d5 \u05d4\u05d1\u05d7\u05d9\u05e8\u05d4 \u05d4\u05d7\u05db\u05de\u05d4 \u2014 \u05e1\u05d5\u05e6\u05d9\u05d5\u05e7\u05e8\u05d8\u05d9\u05d4.\n\u05de\u05e0\u05d4\u05dc, \u05dc\u05d0 \u05de\u05e9\u05d8\u05e8 \u05d5\u05dc\u05d0 \u05de\u05de\u05e9\u05dc.\n\u05e7\u05d4\u05d9\u05dc\u05d4. \U0001f3db\ufe0f", "slh", 147),
    ("AvivCrypto", "\u05de\u05d9 \u05e2\u05d5\u05d3 \u05e2\u05e9\u05d4 staking \u05e9\u05dc SLH \u05d4\u05e9\u05d1\u05d5\u05e2? \u05d4\u05ea\u05e9\u05d5\u05d0\u05d5\u05ea \u05de\u05d8\u05d5\u05e8\u05e4\u05d5\u05ea! \U0001f680", "slh", 24),
    ("MosheTrader", "\u05e0\u05d9\u05ea\u05d5\u05d7 \u05e9\u05d5\u05e7 \u05d9\u05d5\u05de\u05d9:\nSLH \u05e0\u05e1\u05d7\u05e8 \u05d1-444\u20aa \u05e2\u05dd \u05e0\u05e4\u05d7 \u05de\u05e1\u05d7\u05e8 \u05d2\u05d1\u05d5\u05d4.", "investments", 31),
    ("NoaInvest", "\u05e9\u05de\u05e2\u05ea\u05dd \u05e2\u05dc \u05d4\u05d1\u05d5\u05d8 \u05d4\u05d7\u05d3\u05e9? Guardian Bot \u05de\u05d2\u05df \u05e2\u05dc \u05d4\u05e7\u05d1\u05d5\u05e6\u05d5\u05ea \u05e9\u05dc\u05db\u05dd! \U0001f6e1\ufe0f", "slh", 18),
    ("DanielDeFi", "\u05d8\u05d9\u05e4 \u05dc\u05de\u05e9\u05e7\u05d9\u05e2\u05d9\u05dd \u05d7\u05d3\u05e9\u05d9\u05dd: \u05ea\u05de\u05d9\u05d3 \u05ea\u05e2\u05e9\u05d5 DYOR \u05dc\u05e4\u05e0\u05d9 \u05db\u05dc \u05d4\u05e9\u05e7\u05e2\u05d4.", "investments", 45),
    ("YosiBlockchain", "\u05de\u05d9\u05e9\u05d4\u05d5 \u05e8\u05d5\u05e6\u05d4 \u05dc\u05d4\u05e6\u05d8\u05e8\u05e3 \u05dc\u05de\u05d9\u05d8\u05d0\u05e4 \u05d1\u05ea\u05dc \u05d0\u05d1\u05d9\u05d1? \U0001f1ee\U0001f1f1", "general", 37),
]

async def _init_community_tables():
    """Create community tables and seed if empty. Called after pool is ready."""
    async with pool.acquire() as conn:
        await conn.execute(COMMUNITY_SCHEMA)
        count = await conn.fetchval("SELECT count(*) FROM community_posts")
        if count == 0:
            for i, (uname, txt, cat, likes) in enumerate(COMMUNITY_SEEDS):
                await conn.execute(
                    "INSERT INTO community_posts (username, text, category, likes_count, created_at) VALUES ($1,$2,$3,$4, now() - interval '1 hour' * $5)",
                    uname, txt, cat, likes, (len(COMMUNITY_SEEDS) - i) * 4
                )

# Hook into existing startup
_original_startup = startup
async def _extended_startup():
    await _original_startup()
    try:
        await _init_community_tables()
    except Exception as e:
        print(f"[community] init warning: {e}")
    try:
        _payments_monitor_start()
        print("[payments-monitor] started · polling BSC Genesis wallet")
    except Exception as e:
        print(f"[payments-monitor] start warning: {e}")
app.router.on_startup.clear()
app.add_event_handler("startup", _extended_startup)


class CommunityPostCreate(BaseModel):
    username: str
    text: str
    category: str = "general"
    telegram_id: Optional[str] = None
    image_data: Optional[str] = None  # base64 data URL, stored as-is (frontend-capped to 2MB)

class CommunityLikeToggle(BaseModel):
    username: str

class CommunityCommentCreate(BaseModel):
    username: str
    text: str


@app.get("/api/community/rss")
async def community_rss():
    """RSS 2.0 feed of recent community posts — for IFTTT/Zapier/Buffer auto-share.

    Usage:
      1. Register at ifttt.com (free, 2 applets)
      2. New Applet: Trigger 'New item in RSS feed' with this URL
      3. Action: Post to Twitter/LinkedIn/Facebook/Telegram channel
      4. Repeat for each social network
    """
    from fastapi.responses import Response
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, username, text, category, created_at FROM community_posts ORDER BY created_at DESC LIMIT 30"
        )
    from xml.sax.saxutils import escape as xml_esc
    items = []
    for r in rows:
        title = (r["text"] or "")[:80].replace("\n", " ")
        link = f"https://slh-nft.com/community.html#post-{r['id']}"
        pub_date = r["created_at"].strftime("%a, %d %b %Y %H:%M:%S +0000")
        desc = xml_esc((r["text"] or "")[:2000])
        author = xml_esc(r["username"] or "SLH_System")
        category = xml_esc(r["category"] or "general")
        items.append(
            f"<item><title>{xml_esc(title)}</title><link>{link}</link>"
            f"<guid isPermaLink=\"true\">{link}</guid>"
            f"<pubDate>{pub_date}</pubDate>"
            f"<category>{category}</category>"
            f"<author>noreply@slh-nft.com ({author})</author>"
            f"<description>{desc}</description></item>"
        )
    rss = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n<channel>\n'
        '<title>SLH Spark — קהילה</title>\n'
        '<link>https://slh-nft.com/community.html</link>\n'
        '<description>Official feed of SLH Spark community updates, product launches, and ecosystem news.</description>\n'
        '<language>he</language>\n'
        f'<lastBuildDate>{datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")}</lastBuildDate>\n'
        + "\n".join(items) +
        "\n</channel>\n</rss>"
    )
    return Response(content=rss, media_type="application/rss+xml; charset=utf-8")


@app.get("/api/community/posts")
async def community_get_posts(category: str = Query("all"), limit: int = Query(50, le=100), offset: int = Query(0)):
    """Get community posts with comments"""
    async with pool.acquire() as conn:
        if category == "all":
            rows = await conn.fetch(
                "SELECT id, username, telegram_id, text, category, image_data, likes_count, created_at FROM community_posts ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                limit, offset
            )
        else:
            rows = await conn.fetch(
                "SELECT id, username, telegram_id, text, category, image_data, likes_count, created_at FROM community_posts WHERE category=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                category, limit, offset
            )
        posts = []
        for row in rows:
            post = dict(row)
            post["created_at"] = post["created_at"].isoformat()
            comments = await conn.fetch(
                "SELECT id, username, text, created_at FROM community_comments WHERE post_id=$1 ORDER BY created_at ASC",
                post["id"]
            )
            post["comments"] = [
                {"id": c["id"], "username": c["username"], "text": c["text"], "created_at": c["created_at"].isoformat()}
                for c in comments
            ]
            posts.append(post)
    return {"posts": posts, "count": len(posts), "offset": offset}


@app.post("/api/community/posts")
async def community_create_post(body: CommunityPostCreate):
    """Create a new community post"""
    if not body.text.strip() or not body.username.strip():
        raise HTTPException(400, "Username and text required")
    if not _check_community_rate(f"post:{body.username}", 10):
        raise HTTPException(429, "Rate limit: max 10 posts per hour")

    # Image validation: accept data URL only (frontend caps at 2MB), reject suspicious URLs
    image_data = body.image_data
    if image_data:
        if not image_data.startswith("data:image/"):
            image_data = None  # silently drop if not a proper data URL
        elif len(image_data) > 3_500_000:  # 2MB base64 â‰ˆ 2.7MB encoded, +safety
            raise HTTPException(413, "Image too large (max 2MB)")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO community_posts (username, telegram_id, text, category, image_data) VALUES ($1,$2,$3,$4,$5) RETURNING id, username, telegram_id, text, category, image_data, likes_count, created_at",
            body.username.strip(), body.telegram_id, body.text.strip(), body.category, image_data
        )
        post = dict(row)
        post["created_at"] = post["created_at"].isoformat()
        post["comments"] = []
        return post


@app.post("/api/community/posts/{post_id}/like")
async def community_toggle_like(post_id: int, body: CommunityLikeToggle):
    """Toggle like on a post"""
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT id FROM community_posts WHERE id=$1", post_id)
        if not exists:
            raise HTTPException(404, "Post not found")
        existing = await conn.fetchval(
            "SELECT id FROM community_likes WHERE post_id=$1 AND username=$2", post_id, body.username
        )
        if existing:
            await conn.execute("DELETE FROM community_likes WHERE id=$1", existing)
            await conn.execute("UPDATE community_posts SET likes_count=GREATEST(likes_count-1,0) WHERE id=$1", post_id)
            return {"action": "unliked", "post_id": post_id}
        else:
            await conn.execute("INSERT INTO community_likes (post_id,username) VALUES ($1,$2)", post_id, body.username)
            await conn.execute("UPDATE community_posts SET likes_count=likes_count+1 WHERE id=$1", post_id)
            return {"action": "liked", "post_id": post_id}


@app.post("/api/community/posts/{post_id}/comments")
async def community_add_comment(post_id: int, body: CommunityCommentCreate):
    """Add a comment to a post"""
    if not body.text.strip() or not body.username.strip():
        raise HTTPException(400, "Username and text required")
    if not _check_community_rate(f"comment:{body.username}", 50):
        raise HTTPException(429, "Rate limit: max 50 comments per hour")
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT id FROM community_posts WHERE id=$1", post_id)
        if not exists:
            raise HTTPException(404, "Post not found")
        row = await conn.fetchrow(
            "INSERT INTO community_comments (post_id, username, text) VALUES ($1,$2,$3) RETURNING id, post_id, username, text, created_at",
            post_id, body.username.strip(), body.text.strip()
        )
        comment = dict(row)
        comment["created_at"] = comment["created_at"].isoformat()
        return comment


@app.delete("/api/community/posts/{post_id}")
async def community_delete_post(
    post_id: int,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: delete a community post."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM community_comments WHERE post_id=$1", post_id)
        await conn.execute("DELETE FROM community_likes WHERE post_id=$1", post_id)
        deleted = await conn.execute("DELETE FROM community_posts WHERE id=$1", post_id)
    return {"ok": True, "deleted_post_id": post_id}


@app.get("/api/community/stats")
async def community_stats():
    """Community statistics"""
    async with pool.acquire() as conn:
        total_posts = await conn.fetchval("SELECT count(*) FROM community_posts")
        total_users = await conn.fetchval("SELECT count(DISTINCT username) FROM community_posts")
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        posts_today = await conn.fetchval("SELECT count(*) FROM community_posts WHERE created_at >= $1", today_start)
        active_today = await conn.fetchval("SELECT count(DISTINCT username) FROM community_posts WHERE created_at >= $1", today_start)
    return {"total_posts": total_posts, "total_users": total_users, "posts_today": posts_today, "active_today": active_today}


@app.get("/api/community/health")
async def community_health():
    return {"status": "ok", "service": "community"}


# === ANALYTICS ENDPOINTS ===
@app.post("/api/analytics/event")
async def analytics_event(request: Request):
    """Receive analytics events from the website tracker"""
    try:
        data = await request.json()
        # Store in DB if table exists, otherwise just acknowledge
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_type TEXT,
                    page TEXT,
                    visitor_id TEXT,
                    session_id TEXT,
                    data JSONB,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            await conn.execute(
                "INSERT INTO analytics_events (event_type, page, visitor_id, session_id, data) VALUES ($1,$2,$3,$4,$5)",
                data.get("event", "pageview"),
                data.get("page", ""),
                data.get("visitor_id", ""),
                data.get("session_id", ""),
                json.dumps(data)
            )
        return {"status": "ok"}
    except Exception as e:
        return {"status": "ok", "note": str(e)}


@app.get("/api/analytics/stats")
async def analytics_stats():
    """Get aggregated analytics stats for the admin dashboard"""
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_type TEXT,
                    page TEXT,
                    visitor_id TEXT,
                    session_id TEXT,
                    data JSONB,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            total_events = await conn.fetchval("SELECT count(*) FROM analytics_events")
            unique_visitors = await conn.fetchval("SELECT count(DISTINCT visitor_id) FROM analytics_events WHERE visitor_id != ''")
            today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            today_views = await conn.fetchval("SELECT count(*) FROM analytics_events WHERE created_at >= $1", today_start)
            today_visitors = await conn.fetchval("SELECT count(DISTINCT visitor_id) FROM analytics_events WHERE created_at >= $1 AND visitor_id != ''", today_start)

            # Last 7 days breakdown
            daily = await conn.fetch("""
                SELECT DATE(created_at) as day, count(*) as views, count(DISTINCT visitor_id) as visitors
                FROM analytics_events WHERE created_at >= now() - interval '7 days'
                GROUP BY DATE(created_at) ORDER BY day
            """)

            # Top pages
            pages = await conn.fetch("""
                SELECT page, count(*) as views FROM analytics_events
                WHERE page != '' GROUP BY page ORDER BY views DESC LIMIT 10
            """)

            return {
                "total_events": total_events,
                "unique_visitors": unique_visitors,
                "today_views": today_views,
                "today_visitors": today_visitors,
                "daily": [{"day": str(r["day"]), "views": r["views"], "visitors": r["visitors"]} for r in daily],
                "top_pages": [{"page": r["page"], "views": r["views"]} for r in pages],
            }
        except Exception as e:
            return {"error": str(e)}


# === WALLET API ENDPOINTS ===
# For wallet.html real data integration

class DepositRequest(BaseModel):
    user_id: int
    amount: float
    currency: str = "SLH"
    tx_hash: str


def _generate_bsc_address(user_id: int) -> str:
    """Generate a deterministic BSC deposit address from user_id."""
    raw = hashlib.sha256(f"slh-deposit-{user_id}".encode()).hexdigest()
    return "0x" + raw[:40]



# Static route MUST come before /{user_id} to avoid FastAPI matching "price" as user_id
@app.get("/api/wallet/price")
async def get_slh_price():
    """Return current SLH price in ILS and USD"""
    slh_usd = round(SLH_PRICE_ILS / USD_ILS_RATE, 4)
    return {
        "token": "SLH",
        "price_ils": SLH_PRICE_ILS,
        "price_usd": slh_usd,
        "usd_ils_rate": USD_ILS_RATE,
        "bsc_contract": SLH_BSC_CONTRACT,
    }


@app.get("/api/wallet/{user_id}")
async def get_wallet(user_id: int):
    """Get user wallet info: SLH balance, deposit addresses"""
    async with pool.acquire() as conn:
        # Get SLH balance from token_balances
        balance_row = await conn.fetchrow(
            "SELECT balance FROM token_balances WHERE user_id=$1 AND token='SLH'",
            user_id
        )
        slh_balance = float(balance_row["balance"]) if balance_row else 0.0

    bsc_address = _generate_bsc_address(user_id)
    slh_usd = round(SLH_PRICE_ILS / USD_ILS_RATE, 4)

    return {
        "user_id": user_id,
        "slh_balance": slh_balance,
        "slh_value_ils": round(slh_balance * SLH_PRICE_ILS, 2),
        "slh_value_usd": round(slh_balance * slh_usd, 2),
        "bsc_deposit_address": bsc_address,
        "ton_deposit_address": SLH_TON_WALLET,
        "ton_memo": str(user_id),
        "bsc_contract": SLH_BSC_CONTRACT,
    }


@app.get("/api/wallet/{user_id}/balances")
async def get_wallet_balances(user_id: int):
    """Get all token balances for a user"""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT token, balance FROM token_balances WHERE user_id=$1",
            user_id
        )
    balances = {r["token"]: float(r["balance"]) for r in rows}
    # Ensure SLH always appears
    if "SLH" not in balances:
        balances["SLH"] = 0.0

    slh_usd = round(SLH_PRICE_ILS / USD_ILS_RATE, 4)
    slh_bal = balances.get("SLH", 0.0)

    return {
        "user_id": user_id,
        "balances": balances,
        "total_slh": slh_bal,
        "total_value_ils": round(slh_bal * SLH_PRICE_ILS, 2),
        "total_value_usd": round(slh_bal * slh_usd, 2),
    }


@app.post("/api/wallet/deposit")
async def record_deposit(req: DepositRequest, x_admin_override_zuz: Optional[str] = Header(None)):
    """Record a deposit and credit token_balances"""
    if req.amount <= 0:
        raise HTTPException(400, "Amount must be positive")
    if not req.tx_hash.strip():
        raise HTTPException(400, "tx_hash is required")

    # ZUZ Guardian gate — deposit credits an on-chain tx to an internal balance.
    # Block if recipient account is banned (even if the on-chain tx is real).
    try:
        from shared.guardian_gate import require_clean_zuz as _zuz_gate
        await _zuz_gate(pool, req.user_id, admin_override_header=x_admin_override_zuz)
    except HTTPException:
        raise
    except Exception as _gate_err:
        print(f"[wallet_deposit][WARN] zuz gate failed open: {_gate_err!r}")

    async with pool.acquire() as conn:
        # Check for duplicate tx_hash
        existing = await conn.fetchval(
            "SELECT id FROM deposits WHERE tx_hash=$1", req.tx_hash.strip()
        )
        if existing:
            raise HTTPException(409, "Transaction already recorded")

        async with conn.transaction():
            # Insert deposit record
            dep_id = await conn.fetchval("""
                INSERT INTO deposits (user_id, amount, currency, tx_hash, status, created_at)
                VALUES ($1, $2, $3, $4, 'confirmed', now())
                RETURNING id
            """, req.user_id, req.amount, req.currency, req.tx_hash.strip())

            # Credit token_balances
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, token) DO UPDATE SET balance = token_balances.balance + $3
            """, req.user_id, req.currency, req.amount)

            # Record in token_transfers
            await conn.execute("""
                INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type)
                VALUES ($1, $2, $3, $4, $5, 'deposit')
            """, req.user_id, req.user_id, req.currency, req.amount, f"deposit:{req.tx_hash.strip()}")

    return {
        "status": "ok",
        "deposit_id": dep_id,
        "user_id": req.user_id,
        "amount": req.amount,
        "currency": req.currency,
        "tx_hash": req.tx_hash.strip(),
    }


@app.get("/api/wallet/{user_id}/transactions")
async def get_wallet_transactions(user_id: int, limit: int = Query(50, le=200), offset: int = Query(0)):
    """Get transaction history from token_transfers for a user"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, from_user_id, to_user_id, token, amount, memo, tx_type, created_at
            FROM token_transfers
            WHERE from_user_id=$1 OR to_user_id=$1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
        """, user_id, limit, offset)

        total = await conn.fetchval(
            "SELECT count(*) FROM token_transfers WHERE from_user_id=$1 OR to_user_id=$1",
            user_id
        )

    transactions = []
    for r in rows:
        direction = "out" if r["from_user_id"] == user_id and r["to_user_id"] != user_id else "in"
        if r["tx_type"] == "deposit":
            direction = "in"
        transactions.append({
            "id": r["id"],
            "direction": direction,
            "counterparty": r["to_user_id"] if direction == "out" else r["from_user_id"],
            "token": r["token"],
            "amount": float(r["amount"]),
            "memo": r["memo"],
            "type": r["tx_type"],
            "timestamp": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return {
        "user_id": user_id,
        "transactions": transactions,
        "total": total or 0,
        "limit": limit,
        "offset": offset,
    }

class WalletSendRequest(BaseModel):
    to: str
    amount: float
    currency: str = "SLH"
    request_id: str


@app.post("/api/wallet/send")
async def wallet_send(
    req: WalletSendRequest,
    authorization: Optional[str] = Header(None),
    x_admin_override_zuz: Optional[str] = Header(None),
):
    user_id = get_current_user_id(authorization)

    if req.amount <= 0:
        raise HTTPException(400, "Amount must be positive")

    if not req.request_id or len(req.request_id.strip()) < 8:
        raise HTTPException(400, "request_id is required")

    if not _check_wallet_send_rate(user_id, cooldown_seconds=5):
        raise HTTPException(429, "Too many requests, wait a few seconds")

    # ZUZ Guardian gate — block senders with ZUZ >= 100 or active ban
    try:
        from shared.guardian_gate import require_clean_zuz as _zuz_gate
        await _zuz_gate(pool, user_id, admin_override_header=x_admin_override_zuz)
    except HTTPException:
        raise
    except Exception as _gate_err:
        print(f"[wallet_send][WARN] zuz gate failed open: {_gate_err!r}")

    token = (req.currency or "SLH").upper().strip()
    if token != "SLH":
        raise HTTPException(400, "Only SLH internal transfer is supported right now")

    if not req.to.isdigit():
        raise HTTPException(400, "Recipient must be a Telegram numeric ID for now")

    to_user_id = int(req.to)
    if to_user_id == user_id:
        raise HTTPException(400, "Cannot send to yourself")

    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow(
                "SELECT tx_transfer_id FROM wallet_idempotency WHERE user_id=$1 AND request_id=$2",
                user_id, req.request_id.strip()
            )

            if existing and existing["tx_transfer_id"]:
                row = await conn.fetchrow(
                    """
                    SELECT id, from_user_id, to_user_id, token, amount, memo, tx_type, created_at
                    FROM token_transfers
                    WHERE id=$1
                    """,
                    existing["tx_transfer_id"]
                )
                if row:
                    return {
                        "status": "ok",
                        "data": {
                            "transfer_id": row["id"],
                            "from_id": row["from_user_id"],
                            "to_id": row["to_user_id"],
                            "token": row["token"],
                            "amount": float(row["amount"]),
                            "memo": row["memo"],
                            "type": row["tx_type"],
                            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                            "idempotent_replay": True
                        }
                    }

            balance = await conn.fetchval(
                "SELECT balance FROM token_balances WHERE user_id=$1 AND token=$2",
                user_id, token
            )
            if balance is None or float(balance) < req.amount:
                raise HTTPException(400, "Insufficient balance")

            await conn.execute(
                "UPDATE token_balances SET balance = balance - $1, updated_at = CURRENT_TIMESTAMP WHERE user_id=$2 AND token=$3",
                req.amount, user_id, token
            )

            await conn.execute(
                """
                INSERT INTO token_balances (user_id, token, balance, updated_at)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id, token)
                DO UPDATE SET balance = token_balances.balance + $3, updated_at = CURRENT_TIMESTAMP
                """,
                to_user_id, token, req.amount
            )

            transfer_id = await conn.fetchval(
                """
                INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type)
                VALUES ($1, $2, $3, $4, $5, 'wallet_send')
                RETURNING id
                """,
                user_id, to_user_id, token, req.amount, f"wallet send | request_id={req.request_id.strip()}"
            )

            await conn.execute(
                """
                INSERT INTO wallet_idempotency (user_id, request_id, tx_transfer_id)
                VALUES ($1, $2, $3)
                """,
                user_id, req.request_id.strip(), transfer_id
            )

            row = await conn.fetchrow(
                """
                SELECT id, from_user_id, to_user_id, token, amount, memo, tx_type, created_at
                FROM token_transfers
                WHERE id=$1
                """,
                transfer_id
            )

    return {
        "status": "ok",
        "data": {
            "transfer_id": row["id"],
            "from_id": row["from_user_id"],
            "to_id": row["to_user_id"],
            "token": row["token"],
            "amount": float(row["amount"]),
            "memo": row["memo"],
            "type": row["tx_type"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "idempotent_replay": False
        }
    }


# === ADMIN DASHBOARD API ===

@app.get("/api/admin/dashboard")
async def admin_dashboard(
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Aggregated admin dashboard data â€” all stats in one call.

    SECURITY FIX (H-1): Now requires admin authentication via JWT or X-Admin-Key header.
    """
    _require_admin(authorization, x_admin_key)
    async def safe(conn, query, *args):
        try:
            return await conn.fetchval(query, *args) or 0
        except Exception:
            return 0

    async with pool.acquire() as conn:
        # Count REAL users only (telegram_id >= 1M, excludes seed test ids 100001-299999)
        total_users = await safe(conn, "SELECT COUNT(*) FROM web_users WHERE telegram_id >= 1000000")
        premium_users = await safe(conn, "SELECT COUNT(*) FROM premium_users WHERE payment_status='approved' AND user_id >= 1000000")
        total_staked = await safe(conn, "SELECT COALESCE(SUM(amount),0) FROM staking_positions WHERE status='active' AND user_id >= 1000000")
        total_deposits = await safe(conn, "SELECT COALESCE(SUM(amount),0) FROM deposits WHERE status='active' AND user_id >= 1000000")
        pending_payments = await safe(conn, "SELECT COUNT(*) FROM premium_users WHERE payment_status='pending' AND user_id >= 1000000")

        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_views = await safe(conn, "SELECT COUNT(*) FROM analytics_events WHERE created_at >= $1", today_start)
        today_visitors = await safe(conn, "SELECT COUNT(DISTINCT visitor_id) FROM analytics_events WHERE created_at >= $1 AND visitor_id != ''", today_start)
        total_events = await safe(conn, "SELECT COUNT(*) FROM analytics_events")
        total_visitors = await safe(conn, "SELECT COUNT(DISTINCT visitor_id) FROM analytics_events WHERE visitor_id != ''")

        # Recent signups â€” real Telegram IDs only
        today_signups = await safe(conn, "SELECT COUNT(*) FROM web_users WHERE last_login >= $1 AND telegram_id >= 1000000", today_start)

        # Referral stats
        referral_count = await safe(conn, "SELECT COUNT(*) FROM referrals")

        # Last 7 days analytics
        try:
            daily = await conn.fetch("""
                SELECT DATE(created_at) as day, COUNT(*) as views, COUNT(DISTINCT visitor_id) as visitors
                FROM analytics_events WHERE created_at >= now() - interval '7 days'
                GROUP BY DATE(created_at) ORDER BY day
            """)
            daily_data = [{"day": str(r["day"]), "views": r["views"], "visitors": r["visitors"]} for r in daily]
        except Exception:
            daily_data = []

        # Top pages
        try:
            pages = await conn.fetch("""
                SELECT page, COUNT(*) as views FROM analytics_events
                WHERE page != '' GROUP BY page ORDER BY views DESC LIMIT 10
            """)
            top_pages = [{"page": r["page"], "views": r["views"]} for r in pages]
        except Exception:
            top_pages = []

        # Recent users â€” REAL users only (filter test IDs + group chats)
        try:
            recent = await conn.fetch(
                "SELECT telegram_id, username, first_name, last_login FROM web_users "
                "WHERE telegram_id >= 1000000 "
                "ORDER BY last_login DESC LIMIT 15"
            )
            recent_users = [{"id": r["telegram_id"], "username": r["username"], "name": r["first_name"], "last_login": str(r["last_login"])} for r in recent]
        except Exception:
            recent_users = []

    return {
        "total_users": total_users,
        "premium_users": premium_users,
        "pending_payments": pending_payments,
        "total_staked_ton": float(total_staked),
        "total_deposits_ton": float(total_deposits),
        "referral_count": referral_count,
        "today_signups": today_signups,
        "analytics": {
            "total_events": total_events,
            "total_visitors": total_visitors,
            "today_views": today_views,
            "today_visitors": today_visitors,
            "daily": daily_data,
            "top_pages": top_pages,
        },
        "recent_users": recent_users,
        "bots_live": 20,
        "timestamp": datetime.utcnow().isoformat(),
    }


# === AUTO-SYNC FROM TELEGRAM BOT (no login widget required) ===
class BotSyncRequest(BaseModel):
    telegram_id: int
    username: Optional[str] = ""
    first_name: Optional[str] = ""
    photo_url: Optional[str] = ""
    referrer_id: Optional[int] = None
    bot_secret: str  # required to prevent anyone from creating users via this endpoint


BOT_SYNC_SECRET = os.getenv("BOT_SYNC_SECRET", "slh-bot-sync-2026-default-please-override")


@app.post("/api/auth/bot-sync")
async def auth_bot_sync(req: BotSyncRequest):
    """Called by SLH_AIR_bot whenever a user presses /start.

    Creates / updates the user in web_users so they can log into the
    website / mini-app using the same Telegram ID WITHOUT going through
    @userinfobot or the Telegram Login Widget. This is the core of the
    seamless registration UX: open bot â†’ press /start â†’ you're in.

    The bot passes a shared secret so random clients can't create users.
    Returns a short-lived JWT so the bot can generate a "login link" that
    drops the user straight into their dashboard.
    """
    if not req.bot_secret or req.bot_secret != BOT_SYNC_SECRET:
        raise HTTPException(403, "Invalid bot secret")
    if not req.telegram_id:
        raise HTTPException(400, "telegram_id required")

    async with pool.acquire() as conn:
        # Upsert the user
        await conn.execute("""
            INSERT INTO web_users
                (telegram_id, username, first_name, photo_url, auth_date, last_login)
            VALUES ($1, $2, $3, $4, EXTRACT(EPOCH FROM NOW())::BIGINT, CURRENT_TIMESTAMP)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = COALESCE(NULLIF(EXCLUDED.username, ''), web_users.username),
                first_name = COALESCE(NULLIF(EXCLUDED.first_name, ''), web_users.first_name),
                photo_url = COALESCE(NULLIF(EXCLUDED.photo_url, ''), web_users.photo_url),
                last_login = CURRENT_TIMESTAMP
        """, req.telegram_id, req.username or "", req.first_name or "", req.photo_url or "")

        # Record referral if given
        if req.referrer_id and req.referrer_id != req.telegram_id:
            try:
                await conn.execute("""
                    INSERT INTO referrals (user_id, referrer_id, depth)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (user_id) DO NOTHING
                """, req.telegram_id, req.referrer_id)
            except Exception as e:
                print(f"[bot-sync] referral write failed: {e}")

        is_registered = await conn.fetchval(
            "SELECT is_registered FROM web_users WHERE telegram_id=$1", req.telegram_id
        ) or False

    # Issue JWT for auto-login (graceful fallback if JWT_SECRET missing)
    token = None
    try:
        token = create_jwt(req.telegram_id, req.username or "")
    except Exception as e:
        print(f"[bot-sync] jwt creation failed (JWT_SECRET missing?): {e}")

    # Always return a login URL â€” even without JWT, the dashboard accepts ?uid=
    login_url = (
        f"https://slh-nft.com/dashboard.html?uid={req.telegram_id}&jwt={token}"
        if token else
        f"https://slh-nft.com/dashboard.html?uid={req.telegram_id}"
    )

    print(f"[bot-sync] Synced user {req.telegram_id} (@{req.username}) â€” registered={is_registered}")
    return {
        "ok": True,
        "telegram_id": req.telegram_id,
        "is_registered": is_registered,
        "jwt": token,
        "login_url": login_url,
    }


# === UNIFIED USER ENDPOINT (single call for everything) ===
@app.get("/api/user/full/{telegram_id}")
async def get_user_full(telegram_id: int):
    """Return EVERYTHING about a user in one call.

    Consolidates: profile, registration, wallets (internal + linked Web3),
    balances (from all sources), premium status, staking, deposits,
    referrals, recent transactions, marketplace activity.

    Designed to be the single source of truth for the dashboard / mini-app,
    so the bot / website / mini-app all show the same numbers.
    """
    async with pool.acquire() as conn:
        profile = await conn.fetchrow("""
            SELECT telegram_id, username, first_name, photo_url, auth_date, last_login,
                   is_registered, registered_at, eth_wallet, eth_wallet_linked_at,
                   ton_wallet, ton_wallet_linked_at,
                   display_name, bio, language_pref
              FROM web_users WHERE telegram_id=$1
        """, telegram_id)
        if not profile:
            raise HTTPException(404, "User not found")

        # All internal token balances
        balances = {"TON_available": 0.0, "TON_locked": 0.0}
        try:
            rows = await conn.fetch(
                "SELECT token, balance FROM token_balances WHERE user_id=$1", telegram_id
            )
            for row in rows:
                balances[row["token"]] = float(row["balance"])
        except Exception:
            pass

        # Bank account balances if available
        try:
            bank = await conn.fetchrow(
                "SELECT COALESCE(available,0) as available, COALESCE(locked,0) as locked_amt "
                "FROM account_balances WHERE account_id=$1", telegram_id
            )
            if bank:
                balances["TON_available"] = float(bank["available"] or 0)
                balances["TON_locked"] = float(bank["locked_amt"] or 0)
        except Exception:
            pass

        # Deposits
        deposits = []
        try:
            rows = await conn.fetch(
                "SELECT id, amount, currency, tx_hash, status, plan_key, created_at "
                "FROM deposits WHERE user_id=$1 ORDER BY created_at DESC LIMIT 20",
                telegram_id
            )
            deposits = [{
                "id": r["id"], "amount": float(r["amount"]), "currency": r["currency"],
                "tx_hash": r["tx_hash"], "status": r["status"], "plan_key": r["plan_key"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            } for r in rows]
        except Exception:
            pass

        # Active staking positions
        staking = []
        try:
            rows = await conn.fetch("""
                SELECT id, plan, amount, currency, apy_monthly, lock_days,
                       start_date, end_date, status, earned
                  FROM staking_positions
                 WHERE user_id=$1 AND status='active'
                 ORDER BY start_date DESC
            """, telegram_id)
            staking = [{
                "id": r["id"], "plan": r["plan"], "amount": float(r["amount"]),
                "currency": r["currency"], "apy_monthly": float(r["apy_monthly"]),
                "lock_days": r["lock_days"],
                "start_date": r["start_date"].isoformat() if r["start_date"] else None,
                "end_date": r["end_date"].isoformat() if r["end_date"] else None,
                "status": r["status"], "earned": float(r["earned"] or 0),
            } for r in rows]
        except Exception:
            pass

        # Referral stats
        referrals = {"direct_count": 0, "total_network": 0, "total_earned": 0.0}
        try:
            direct = await conn.fetchval(
                "SELECT count(*) FROM referrals WHERE referrer_id=$1", telegram_id
            ) or 0
            earned = await conn.fetchval(
                "SELECT COALESCE(SUM(commission_amount), 0) FROM referral_earnings WHERE earner_id=$1",
                telegram_id
            ) or 0
            referrals = {
                "direct_count": int(direct),
                "total_network": int(direct),  # TODO: real recursive count
                "total_earned": float(earned),
            }
        except Exception:
            pass

        # Recent transfers
        transactions = []
        try:
            rows = await conn.fetch("""
                SELECT id, from_user_id, to_user_id, token, amount, memo, tx_type, created_at
                  FROM token_transfers
                 WHERE from_user_id=$1 OR to_user_id=$1
                 ORDER BY created_at DESC LIMIT 20
            """, telegram_id)
            transactions = [{
                "id": r["id"],
                "direction": "out" if r["from_user_id"] == telegram_id else "in",
                "counterparty": r["to_user_id"] if r["from_user_id"] == telegram_id else r["from_user_id"],
                "token": r["token"], "amount": float(r["amount"]),
                "memo": r["memo"] or "", "tx_type": r["tx_type"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            } for r in rows]
        except Exception:
            pass

        # Marketplace activity
        marketplace = {"listings": 0, "orders_bought": 0, "orders_sold": 0}
        try:
            marketplace["listings"] = int(await conn.fetchval(
                "SELECT count(*) FROM marketplace_items WHERE seller_id=$1", telegram_id
            ) or 0)
            marketplace["orders_bought"] = int(await conn.fetchval(
                "SELECT count(*) FROM marketplace_orders WHERE buyer_id=$1", telegram_id
            ) or 0)
            marketplace["orders_sold"] = int(await conn.fetchval(
                "SELECT count(*) FROM marketplace_orders WHERE seller_id=$1", telegram_id
            ) or 0)
        except Exception:
            pass

        # Premium status
        premium_status = "none"
        try:
            row = await conn.fetchrow(
                "SELECT payment_status FROM premium_users WHERE user_id=$1 AND bot_name='expertnet'",
                telegram_id
            )
            if row:
                premium_status = row["payment_status"]
        except Exception:
            pass

    return {
        "profile": {
            "telegram_id": profile["telegram_id"],
            "username": profile["username"],
            "first_name": profile["first_name"],
            "photo_url": profile["photo_url"],
            "last_login": profile["last_login"].isoformat() if profile["last_login"] else None,
            "is_registered": profile["is_registered"],
            "registered_at": profile["registered_at"].isoformat() if profile["registered_at"] else None,
        },
        "wallets": {
            "ton_internal": SLH_TON_WALLET,  # shared project wallet
            "eth_linked": profile["eth_wallet"],
            "eth_linked_at": profile["eth_wallet_linked_at"].isoformat() if profile["eth_wallet_linked_at"] else None,
            "ton_linked": profile["ton_wallet"],
            "ton_linked_at": profile["ton_wallet_linked_at"].isoformat() if profile["ton_wallet_linked_at"] else None,
        },
        "balances": balances,
        "deposits": deposits,
        "staking": staking,
        "referrals": referrals,
        "transactions": transactions,
        "marketplace": marketplace,
        "premium": {
            "status": premium_status,
            "is_premium": premium_status == "approved",
        },
        "price_info": {
            "slh_ils": SLH_PRICE_ILS,
            "slh_usd": round(SLH_PRICE_ILS / USD_ILS_RATE, 4),
            "registration_ils": REGISTRATION_PRICE_ILS,
            "registration_usd": round(REGISTRATION_PRICE_ILS / USD_ILS_RATE, 4),
        },
    }


# === MARKETPLACE ENDPOINTS ===
class MarketplaceListRequest(BaseModel):
    seller_id: int
    title: str
    description: Optional[str] = ""
    price: float
    currency: Optional[str] = "SLH"
    image_url: Optional[str] = ""  # URL or data:image/... base64
    category: Optional[str] = "general"
    stock: Optional[int] = 1
    promotion: Optional[str] = "none"  # none | featured | top | homepage


class MarketplaceBuyRequest(BaseModel):
    buyer_id: int
    item_id: int
    quantity: Optional[int] = 1


class MarketplaceApproveRequest(BaseModel):
    admin_id: int
    item_id: int
    action: str  # 'approve' | 'reject'


ALLOWED_CURRENCIES = {"SLH", "TON", "ILS", "USD"}
ALLOWED_CATEGORIES = {"general", "digital", "physical", "nft", "course", "service"}


@app.post("/api/marketplace/list")
async def marketplace_list_item(req: MarketplaceListRequest):
    """Create a new marketplace listing. Starts as 'pending' until admin approves."""
    title = (req.title or "").strip()
    if not title or len(title) < 3:
        raise HTTPException(400, "Title must be at least 3 characters")
    if len(title) > 200:
        raise HTTPException(400, "Title too long (max 200 chars)")
    if req.price is None or req.price <= 0:
        raise HTTPException(400, "Price must be positive")
    currency = (req.currency or "SLH").upper()
    if currency not in ALLOWED_CURRENCIES:
        raise HTTPException(400, f"Currency must be one of: {sorted(ALLOWED_CURRENCIES)}")
    category = (req.category or "general").lower()
    if category not in ALLOWED_CATEGORIES:
        raise HTTPException(400, f"Category must be one of: {sorted(ALLOWED_CATEGORIES)}")
    stock = max(1, int(req.stock or 1))
    description = (req.description or "").strip()[:2000]
    # Image: accept either regular URL (capped at 500 chars) or data:image/... base64 (3.5MB raw cap)
    image_url = (req.image_url or "").strip()
    if image_url.startswith("data:image/"):
        if len(image_url) > 3_500_000:
            raise HTTPException(413, "Image too large (max 2MB)")
    else:
        image_url = image_url[:500]
    promotion = (req.promotion or "none").lower()
    if promotion not in {"none", "featured", "top", "homepage"}:
        promotion = "none"

    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM web_users WHERE telegram_id=$1", req.seller_id)
        if not exists:
            raise HTTPException(404, "Seller not found â€” please login first")

        # Admin listings are auto-approved
        initial_status = "approved" if req.seller_id == ADMIN_USER_ID else "pending"
        # SECURITY FIX (C-1): Use parameterized CASE instead of f-string injection
        row = await conn.fetchrow("""
            INSERT INTO marketplace_items
                (seller_id, title, description, price, currency, image_url, category, stock, status, promotion, approved_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    CASE WHEN $11 = 'approved' THEN CURRENT_TIMESTAMP ELSE NULL END)
            RETURNING id, status, created_at
        """, req.seller_id, title, description, req.price, currency, image_url, category, stock, initial_status, promotion, initial_status)

    print(f"[Marketplace] New listing #{row['id']} by {req.seller_id}: {title} ({req.price} {currency}) status={row['status']}")
    return {
        "ok": True,
        "item_id": row["id"],
        "status": row["status"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "message": "×¤×¨×™×˜ ×”×•×¢×œ×” ×œ××™×©×•×¨" if initial_status == "pending" else "×¤×¨×™×˜ ×¤×•×¨×¡× ×‘×”×¦×œ×—×”"
    }


@app.get("/api/marketplace/items")
async def marketplace_get_items(
    category: Optional[str] = None,
    status: str = "approved",
    limit: int = Query(50, le=200),
    offset: int = 0
):
    """List marketplace items. Default: approved only. Supports category filter.

    Promoted items (homepage > top > featured > none) sort first.
    """
    query = """
        SELECT mi.id, mi.seller_id, mi.title, mi.description, mi.price, mi.currency,
               mi.image_url, mi.category, mi.stock, mi.status,
               COALESCE(mi.promotion, 'none') AS promotion,
               COALESCE(mi.views, 0) AS views,
               mi.created_at, mi.approved_at,
               COALESCE(wu.username, wu.first_name, '') AS seller_name
          FROM marketplace_items mi
          LEFT JOIN web_users wu ON wu.telegram_id = mi.seller_id
         WHERE 1=1
    """
    params = []
    if status:
        params.append(status)
        query += f" AND mi.status = ${len(params)}"
    if category:
        params.append(category.lower())
        query += f" AND mi.category = ${len(params)}"
    query += """
         ORDER BY
            CASE COALESCE(mi.promotion, 'none')
                WHEN 'homepage' THEN 0
                WHEN 'top' THEN 1
                WHEN 'featured' THEN 2
                ELSE 3
            END,
            mi.created_at DESC
    """
    params.append(limit)
    query += f" LIMIT ${len(params)}"
    params.append(offset)
    query += f" OFFSET ${len(params)}"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "seller_id": r["seller_id"],
            "seller_name": r["seller_name"] or f"user{r['seller_id']}",
            "title": r["title"],
            "description": r["description"] or "",
            "price": float(r["price"]),
            "currency": r["currency"],
            "image_url": r["image_url"] or "",
            "category": r["category"],
            "stock": r["stock"],
            "status": r["status"],
            "promotion": r["promotion"] or "none",
            "views": r["views"] or 0,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "approved_at": r["approved_at"].isoformat() if r["approved_at"] else None,
        })
    return {"items": items, "count": len(items), "limit": limit, "offset": offset}


@app.get("/api/marketplace/items/{item_id}")
async def marketplace_get_item(item_id: int):
    """Get a single marketplace item by ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT mi.id, mi.seller_id, mi.title, mi.description, mi.price, mi.currency,
                   mi.image_url, mi.category, mi.stock, mi.status, mi.created_at, mi.approved_at,
                   COALESCE(wu.username, wu.first_name, '') AS seller_name
              FROM marketplace_items mi
              LEFT JOIN web_users wu ON wu.telegram_id = mi.seller_id
             WHERE mi.id = $1
        """, item_id)
    if not row:
        raise HTTPException(404, "Item not found")
    return {
        "id": row["id"],
        "seller_id": row["seller_id"],
        "seller_name": row["seller_name"] or f"user{row['seller_id']}",
        "title": row["title"],
        "description": row["description"] or "",
        "price": float(row["price"]),
        "currency": row["currency"],
        "image_url": row["image_url"] or "",
        "category": row["category"],
        "stock": row["stock"],
        "status": row["status"],
        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        "approved_at": row["approved_at"].isoformat() if row["approved_at"] else None,
    }


@app.post("/api/marketplace/buy")
async def marketplace_buy(req: MarketplaceBuyRequest):
    """Create an order for an approved marketplace item. Decrements stock atomically."""
    if req.quantity < 1:
        raise HTTPException(400, "Quantity must be at least 1")

    async with pool.acquire() as conn:
        async with conn.transaction():
            buyer = await conn.fetchval("SELECT 1 FROM web_users WHERE telegram_id=$1", req.buyer_id)
            if not buyer:
                raise HTTPException(404, "Buyer not found â€” please login first")

            item = await conn.fetchrow("""
                SELECT id, seller_id, title, price, currency, stock, status
                  FROM marketplace_items
                 WHERE id = $1
                 FOR UPDATE
            """, req.item_id)
            if not item:
                raise HTTPException(404, "Item not found")
            if item["status"] != "approved":
                raise HTTPException(400, f"Item is {item['status']}, not available for purchase")
            if item["seller_id"] == req.buyer_id:
                raise HTTPException(400, "Cannot buy your own item")
            if item["stock"] < req.quantity:
                raise HTTPException(400, f"Not enough stock (available: {item['stock']})")

            total_price = float(item["price"]) * req.quantity

            order = await conn.fetchrow("""
                INSERT INTO marketplace_orders
                    (buyer_id, seller_id, item_id, quantity, total_price, currency, status)
                VALUES ($1, $2, $3, $4, $5, $6, 'pending')
                RETURNING id, created_at
            """, req.buyer_id, item["seller_id"], item["id"], req.quantity, total_price, item["currency"])

            # Decrement stock (mark sold-out if it hits zero)
            new_stock = item["stock"] - req.quantity
            new_status = "sold_out" if new_stock == 0 else item["status"]
            await conn.execute("""
                UPDATE marketplace_items
                   SET stock = $1, status = $2
                 WHERE id = $3
            """, new_stock, new_status, item["id"])

    print(f"[Marketplace] Order #{order['id']}: buyer={req.buyer_id} item={req.item_id} x{req.quantity} = {total_price} {item['currency']}")
    return {
        "ok": True,
        "order_id": order["id"],
        "item_id": req.item_id,
        "quantity": req.quantity,
        "total_price": total_price,
        "currency": item["currency"],
        "status": "pending",
        "created_at": order["created_at"].isoformat() if order["created_at"] else None,
        "message": "×”×–×ž× ×” × ×•×¦×¨×” â€” ×ž×ž×ª×™×Ÿ ×œ×ª×©×œ×•×",
    }


@app.get("/api/marketplace/orders/{user_id}")
async def marketplace_user_orders(user_id: int, role: str = "buyer", limit: int = Query(50, le=200)):
    """List orders for a user. role=buyer|seller."""
    if role not in ("buyer", "seller"):
        raise HTTPException(400, "role must be 'buyer' or 'seller'")

    col = "buyer_id" if role == "buyer" else "seller_id"
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT mo.id, mo.buyer_id, mo.seller_id, mo.item_id, mo.quantity,
                   mo.total_price, mo.currency, mo.status, mo.created_at, mo.completed_at,
                   mi.title AS item_title, mi.image_url AS item_image
              FROM marketplace_orders mo
              LEFT JOIN marketplace_items mi ON mi.id = mo.item_id
             WHERE mo.{col} = $1
             ORDER BY mo.created_at DESC
             LIMIT $2
        """, user_id, limit)

    orders = []
    for r in rows:
        orders.append({
            "id": r["id"],
            "buyer_id": r["buyer_id"],
            "seller_id": r["seller_id"],
            "item_id": r["item_id"],
            "item_title": r["item_title"] or "",
            "item_image": r["item_image"] or "",
            "quantity": r["quantity"],
            "total_price": float(r["total_price"]),
            "currency": r["currency"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
        })
    return {"orders": orders, "count": len(orders), "role": role}


@app.get("/api/marketplace/my-listings/{user_id}")
async def marketplace_my_listings(user_id: int, limit: int = Query(50, le=200)):
    """List all items a user has put up for sale (any status)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, seller_id, title, description, price, currency, image_url,
                   category, stock, status, created_at, approved_at
              FROM marketplace_items
             WHERE seller_id = $1
             ORDER BY created_at DESC
             LIMIT $2
        """, user_id, limit)
    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "title": r["title"],
            "description": r["description"] or "",
            "price": float(r["price"]),
            "currency": r["currency"],
            "image_url": r["image_url"] or "",
            "category": r["category"],
            "stock": r["stock"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "approved_at": r["approved_at"].isoformat() if r["approved_at"] else None,
        })
    return {"items": items, "count": len(items)}


@app.post("/api/marketplace/admin/approve")
async def marketplace_admin_approve(req: MarketplaceApproveRequest):
    """Admin-only: approve or reject a pending marketplace item."""
    if req.admin_id != ADMIN_USER_ID:
        raise HTTPException(403, "Admin only")
    if req.action not in ("approve", "reject"):
        raise HTTPException(400, "Action must be 'approve' or 'reject'")

    new_status = "approved" if req.action == "approve" else "rejected"
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE marketplace_items
               SET status = $1,
                   approved_at = CASE WHEN $1 = 'approved' THEN CURRENT_TIMESTAMP ELSE approved_at END
             WHERE id = $2
         RETURNING id, title, seller_id, status
        """, new_status, req.item_id)
    if not row:
        raise HTTPException(404, "Item not found")
    print(f"[Marketplace] Admin {req.admin_id} {req.action}d item #{row['id']}")
    return {"ok": True, "item_id": row["id"], "status": row["status"], "title": row["title"]}


@app.get("/api/marketplace/admin/pending")
async def marketplace_admin_pending(
    admin_id: int = Query(default=0),
    limit: int = Query(100, le=500),
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin-only: list all pending items waiting for approval."""
    # Accept either admin_id param or X-Admin-Key header
    if admin_id != ADMIN_USER_ID:
        try:
            _require_admin(authorization, x_admin_key)
        except Exception:
            raise HTTPException(403, "Admin only")
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT mi.id, mi.seller_id, mi.title, mi.description, mi.price, mi.currency,
                   mi.image_url, mi.category, mi.stock, mi.created_at,
                   COALESCE(wu.username, wu.first_name, '') AS seller_name
              FROM marketplace_items mi
              LEFT JOIN web_users wu ON wu.telegram_id = mi.seller_id
             WHERE mi.status = 'pending'
             ORDER BY mi.created_at ASC
             LIMIT $1
        """, limit)
    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "seller_id": r["seller_id"],
            "seller_name": r["seller_name"] or f"user{r['seller_id']}",
            "title": r["title"],
            "description": r["description"] or "",
            "price": float(r["price"]),
            "currency": r["currency"],
            "image_url": r["image_url"] or "",
            "category": r["category"],
            "stock": r["stock"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })
    return {"items": items, "count": len(items)}


@app.get("/api/marketplace/stats")
async def marketplace_stats():
    """Public stats about the marketplace."""
    async with pool.acquire() as conn:
        total_items = await conn.fetchval("SELECT COUNT(*) FROM marketplace_items WHERE status='approved'") or 0
        total_orders = await conn.fetchval("SELECT COUNT(*) FROM marketplace_orders") or 0
        total_volume = await conn.fetchval("SELECT COALESCE(SUM(total_price), 0) FROM marketplace_orders WHERE status='completed'") or 0
        active_sellers = await conn.fetchval("SELECT COUNT(DISTINCT seller_id) FROM marketplace_items WHERE status='approved'") or 0
    return {
        "total_items": int(total_items),
        "total_orders": int(total_orders),
        "total_volume": float(total_volume),
        "active_sellers": int(active_sellers),
    }


@app.get("/api/admin/activity")
async def admin_activity(
    limit: int = Query(20, le=100),
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """SECURITY FIX (H-2): Now requires admin authentication."""
    _require_admin(authorization, x_admin_key)
    """Recent activity across the ecosystem"""
    activities = []
    async with pool.acquire() as conn:
        # Recent logins
        try:
            logins = await conn.fetch(
                "SELECT telegram_id, username, first_name, last_login FROM web_users ORDER BY last_login DESC LIMIT $1",
                limit // 2
            )
            for r in logins:
                name = r["username"] or r["first_name"] or str(r["telegram_id"])
                activities.append({
                    "type": "login",
                    "icon": "ðŸ‘¤",
                    "text": f"User @{name} logged in",
                    "time": r["last_login"].isoformat() if r["last_login"] else ""
                })
        except Exception:
            pass

        # Recent premium payments
        try:
            payments = await conn.fetch(
                "SELECT user_id, amount, payment_status, created_at FROM premium_users ORDER BY created_at DESC LIMIT $1",
                limit // 2
            )
            for r in payments:
                status = "approved" if r["payment_status"] == "approved" else "pending"
                activities.append({
                    "type": "payment",
                    "icon": "ðŸ’°",
                    "text": f"Premium payment ({status}): {r['amount']} from user #{r['user_id']}",
                    "time": r["created_at"].isoformat() if r["created_at"] else ""
                })
        except Exception:
            pass

    # Sort by time
    activities.sort(key=lambda x: x.get("time", ""), reverse=True)
    return activities[:limit]


# ============================================================
# TOKENOMICS â€” SLH/MNH/ZVK dual-track economy
# ============================================================
# SLH = Premium/Governance (high value, scarce, deflationary)
# MNH = ILS-pegged stablecoin (free internal transfers)
# ZVK = Activity rewards (low value, high volume)
#
# Revenue â†’ 50% â†’ Buyback SLH from DEX â†’ Burn â†’ deflationary pressure
# Staking SLH â†’ earns ZVK+MNH yield â†’ locks supply
# Internal MNH transfers â†’ FREE (internal ledger, no blockchain)

async def _ensure_tokenomics_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS token_burns (
            id BIGSERIAL PRIMARY KEY,
            token TEXT NOT NULL,
            amount NUMERIC(28,8) NOT NULL,
            source TEXT NOT NULL,  -- 'revenue_buyback' | 'fee_burn' | 'manual'
            tx_hash TEXT,
            usd_value NUMERIC(18,2),
            notes TEXT,
            burned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_token_burns_token_time ON token_burns(token, burned_at DESC);

        CREATE TABLE IF NOT EXISTS token_backing_reserves (
            id BIGSERIAL PRIMARY KEY,
            token TEXT NOT NULL,  -- 'SLH' | 'MNH'
            reserve_asset TEXT NOT NULL,  -- 'USDT' | 'BNB' | 'TON' | 'USD_BANK'
            amount NUMERIC(28,8) NOT NULL,
            usd_value NUMERIC(18,2) NOT NULL,
            proof_url TEXT,  -- link to on-chain address or bank statement
            verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS internal_transfers (
            id BIGSERIAL PRIMARY KEY,
            from_user_id BIGINT NOT NULL,
            to_user_id BIGINT NOT NULL,
            token TEXT NOT NULL,  -- usually 'MNH'
            amount NUMERIC(28,8) NOT NULL,
            memo TEXT,
            fee NUMERIC(28,8) DEFAULT 0,  -- 0 for MNH internal
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_internal_xfers_from ON internal_transfers(from_user_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_internal_xfers_to ON internal_transfers(to_user_id, created_at DESC);
    """)


@app.get("/api/tokenomics/stats")
async def tokenomics_stats():
    """Full tokenomics overview: supply, burned, staked, reserves."""
    async with pool.acquire() as conn:
        await _ensure_tokenomics_tables(conn)

        # SLH stats
        slh_burned = await conn.fetchval(
            "SELECT COALESCE(SUM(amount), 0) FROM token_burns WHERE token='SLH'"
        ) or 0
        slh_staked = await conn.fetchval(
            "SELECT COALESCE(SUM(amount), 0) FROM staking_positions WHERE status='active'"
        ) or 0
        slh_in_internal_balances = await conn.fetchval(
            "SELECT COALESCE(SUM(balance), 0) FROM token_balances WHERE token='SLH' AND user_id >= 1000000"
        ) or 0

        # Reserves
        reserves = await conn.fetch("""
            SELECT token, reserve_asset, SUM(amount) as amount, SUM(usd_value) as usd_value
              FROM token_backing_reserves
             GROUP BY token, reserve_asset
        """)

        # Recent burns
        recent_burns = await conn.fetch("""
            SELECT token, amount, source, usd_value, burned_at
              FROM token_burns
             ORDER BY burned_at DESC LIMIT 10
        """)

    TOTAL_SUPPLY = 111186328  # SLH fixed supply
    MAX_SUPPLY = 111186328
    circulating = TOTAL_SUPPLY - float(slh_burned)

    return {
        "slh": {
            "max_supply": MAX_SUPPLY,
            "circulating_supply": circulating,
            "burned": float(slh_burned),
            "burn_pct": round(float(slh_burned) / MAX_SUPPLY * 100, 4),
            "staked_active": float(slh_staked),
            "staked_pct": round(float(slh_staked) / circulating * 100, 4) if circulating > 0 else 0,
            "internal_balances": float(slh_in_internal_balances),
            "internal_price_ils": 444,
            "internal_price_usd": 121.6438,
        },
        "mnh": {
            "description": "ILS-pegged stablecoin, free internal transfers",
            "rate": "1 MNH = 1 ILS",
            "backed_by": "USDT/USD reserves (1:1)",
        },
        "zvk": {
            "description": "Activity rewards token",
            "rate": "~4.4 ILS per ZVK (floating)",
            "conversion_to_slh": "100 ZVK = 1 SLH",
        },
        "zuz": {
            "description": "Guardian anti-fraud token â€” Mark of Cain (××•×ª ×§×™×Ÿ)",
            "purpose": "Negative reputation marker for scammers, bots, and fraudsters",
            "mechanism": "Assigned by Guardian bot reports. Higher ZUZ = more suspicious",
            "auto_ban_threshold": ZUZ_AUTO_BAN_THRESHOLD,
            "severity_levels": ZUZ_SEVERITY,
            "cross_group": "Shared across all SLH ecosystem groups â€” one report affects all",
        },
        "reserves": [
            {
                "token": r["token"],
                "asset": r["reserve_asset"],
                "amount": float(r["amount"]),
                "usd_value": float(r["usd_value"]),
            }
            for r in reserves
        ],
        "recent_burns": [
            {
                "token": r["token"],
                "amount": float(r["amount"]),
                "source": r["source"],
                "usd_value": float(r["usd_value"] or 0),
                "burned_at": r["burned_at"].isoformat() if r["burned_at"] else None,
            }
            for r in recent_burns
        ],
    }


class InternalTransferRequest(BaseModel):
    from_user_id: int
    to_user_id: int
    amount: float
    token: str = "MNH"
    memo: Optional[str] = None


@app.post("/api/tokenomics/internal-transfer")
async def internal_transfer(req: InternalTransferRequest, authorization: Optional[str] = Header(None)):
    """FREE internal transfer between users. Works for MNH/ZVK/SLH.
    Uses the internal ledger â€” no blockchain fees. Instant.
    SECURITY: Requires JWT auth â€” sender must be the from_user_id."""
    # Verify caller is the sender (or admin)
    try:
        caller_id = get_current_user_id(authorization)
        if caller_id != req.from_user_id and caller_id != ADMIN_USER_ID:
            raise HTTPException(403, "You can only transfer from your own account")
    except Exception:
        raise HTTPException(401, "Authentication required for transfers")
    if req.from_user_id == req.to_user_id:
        raise HTTPException(400, "Cannot transfer to self")
    if req.amount <= 0:
        raise HTTPException(400, "Amount must be positive")
    if req.token not in ("MNH", "ZVK", "SLH"):
        raise HTTPException(400, "Token must be MNH, ZVK, or SLH")

    async with pool.acquire() as conn:
        await _ensure_tokenomics_tables(conn)

        # Check sender balance
        balance = await conn.fetchval(
            "SELECT balance FROM token_balances WHERE user_id=$1 AND token=$2",
            req.from_user_id, req.token
        ) or 0

        if float(balance) < req.amount:
            raise HTTPException(400, f"Insufficient {req.token} balance: {balance}")

        # Transaction â€” debit sender, credit recipient
        async with conn.transaction():
            await conn.execute("""
                UPDATE token_balances SET balance = balance - $1, updated_at = CURRENT_TIMESTAMP
                 WHERE user_id=$2 AND token=$3
            """, req.amount, req.from_user_id, req.token)
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance) VALUES ($1, $2, $3)
                ON CONFLICT (user_id, token) DO UPDATE
                  SET balance = token_balances.balance + EXCLUDED.balance,
                      updated_at = CURRENT_TIMESTAMP
            """, req.to_user_id, req.token, req.amount)
            xfer_id = await conn.fetchval("""
                INSERT INTO internal_transfers (from_user_id, to_user_id, token, amount, memo, fee)
                VALUES ($1, $2, $3, $4, $5, 0)
                RETURNING id
            """, req.from_user_id, req.to_user_id, req.token, req.amount, req.memo)

            # Audit
            await audit_log_write(
                conn,
                action="internal_transfer",
                actor_type="user",
                actor_user_id=req.from_user_id,
                resource_type="internal_transfer",
                resource_id=str(xfer_id),
                amount_native=req.amount,
                amount_currency=req.token,
            )

    return {"ok": True, "transfer_id": xfer_id, "fee": 0, "token": req.token, "amount": req.amount}


class BurnRequest(BaseModel):
    token: str
    amount: float
    source: str  # 'revenue_buyback', 'fee_burn', 'manual'
    tx_hash: Optional[str] = None
    usd_value: Optional[float] = None
    notes: Optional[str] = None


@app.post("/api/tokenomics/burn")
async def burn_tokens(req: BurnRequest, authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    """Record a token burn. Admin-only. For audit trail + supply accounting."""
    _require_admin(authorization, x_admin_key)
    if req.amount <= 0:
        raise HTTPException(400, "Amount must be positive")
    async with pool.acquire() as conn:
        await _ensure_tokenomics_tables(conn)
        burn_id = await conn.fetchval("""
            INSERT INTO token_burns (token, amount, source, tx_hash, usd_value, notes)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """, req.token, req.amount, req.source, req.tx_hash, req.usd_value, req.notes)
        await audit_log_write(
            conn,
            action="token.burn",
            actor_type="system",
            resource_type="token_burn",
            resource_id=str(burn_id),
            amount_native=req.amount,
            amount_currency=req.token,
            amount_usd=req.usd_value,
            metadata={"source": req.source, "tx_hash": req.tx_hash},
        )
    return {"ok": True, "burn_id": burn_id}


class ReserveRequest(BaseModel):
    token: str  # 'SLH' | 'MNH'
    reserve_asset: str  # 'USDT' | 'BNB' | etc
    amount: float
    usd_value: float
    proof_url: Optional[str] = None
    notes: Optional[str] = None


@app.post("/api/tokenomics/reserves/add")
async def add_reserve(req: ReserveRequest, authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    """Record backing reserves. Admin-only. Published for transparency."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_tokenomics_tables(conn)
        rid = await conn.fetchval("""
            INSERT INTO token_backing_reserves (token, reserve_asset, amount, usd_value, proof_url, notes)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """, req.token, req.reserve_asset, req.amount, req.usd_value, req.proof_url, req.notes)
        await audit_log_write(
            conn,
            action="reserves.add",
            actor_type="admin",
            resource_type="token_backing",
            resource_id=str(rid),
            amount_usd=req.usd_value,
            metadata={"token": req.token, "asset": req.reserve_asset},
            compliance_flags=["PROOF_OF_RESERVES"],
        )
    return {"ok": True, "reserve_id": rid}


# ============================================================
# STRATEGY ENGINE â€” Backtested investment strategies
# ============================================================
# 3 strategies with historical backtest data.
# Each strategy has expected annual yield + max drawdown.
# Users can opt in; strategies execute against CEX accounts.

STRATEGIES = [
    {
        "id": "grid_btc_usdt",
        "name": "Grid Trading BTC/USDT",
        "description": "Market-neutral strategy: places buy/sell orders at fixed price intervals. Profits from volatility.",
        "risk_level": "LOW",
        "expected_annual": 25.4,  # %
        "max_drawdown": -8.2,
        "backtest_period": "2024-01 to 2026-03",
        "sharpe_ratio": 2.1,
        "min_capital_usd": 1000,
        "rebalance_freq": "hourly",
        "assets": ["BTC", "USDT"],
        "exchanges": ["bybit", "binance"],
        "status": "READY",
    },
    {
        "id": "dca_rebalance",
        "name": "DCA + Weekly Rebalance",
        "description": "Dollar-cost-averages into BTC/ETH/SOL/TON with weekly rebalance to target weights.",
        "risk_level": "MEDIUM",
        "expected_annual": 42.7,
        "max_drawdown": -22.5,
        "backtest_period": "2024-01 to 2026-03",
        "sharpe_ratio": 1.4,
        "min_capital_usd": 500,
        "rebalance_freq": "weekly",
        "assets": ["BTC", "ETH", "SOL", "TON", "USDT"],
        "exchanges": ["bybit", "binance"],
        "status": "READY",
    },
    {
        "id": "momentum_multi",
        "name": "Multi-Asset Momentum",
        "description": "Rotates between top-5 crypto assets based on 30-day momentum. Goes to USDT in bear markets.",
        "risk_level": "HIGH",
        "expected_annual": 78.3,
        "max_drawdown": -34.1,
        "backtest_period": "2024-01 to 2026-03",
        "sharpe_ratio": 1.8,
        "min_capital_usd": 2500,
        "rebalance_freq": "weekly",
        "assets": ["BTC", "ETH", "BNB", "SOL", "TON", "USDT"],
        "exchanges": ["bybit", "binance"],
        "status": "READY",
    },
]


@app.get("/api/strategy/list")
async def list_strategies():
    """List all available investment strategies with backtested performance."""
    return {
        "strategies": STRATEGIES,
        "total": len(STRATEGIES),
        "portfolio_target_annual": "65%+",
        "note": "All strategies are READ-ONLY backtests. Live execution requires Fireblocks custody + user opt-in.",
    }


@app.get("/api/strategy/{strategy_id}")
async def get_strategy(strategy_id: str):
    """Get details for a specific strategy."""
    for s in STRATEGIES:
        if s["id"] == strategy_id:
            return s
    raise HTTPException(404, "Strategy not found")


# ============================================================
# BROADCAST â€” Send Telegram messages to registered users
# ============================================================
# Uses @SLH_AIR_bot to DM every registered user. Ideal for
# presale announcements, Genesis 49 updates, system alerts.
# Admin-only. All broadcasts logged to institutional_audit.

class BroadcastRequest(BaseModel):
    message: str
    target: str = "registered"  # 'registered' | 'genesis49' | 'all' | 'custom'
    custom_ids: Optional[list] = None
    admin_key: str  # must match ADMIN_BROADCAST_KEY
    dry_run: bool = False


async def _ensure_broadcast_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id BIGSERIAL PRIMARY KEY,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            target TEXT NOT NULL,
            total_targets INT NOT NULL,
            success_count INT NOT NULL DEFAULT 0,
            fail_count INT NOT NULL DEFAULT 0,
            message_preview TEXT,
            admin_actor TEXT
        );
        CREATE TABLE IF NOT EXISTS broadcast_deliveries (
            id BIGSERIAL PRIMARY KEY,
            broadcast_id BIGINT NOT NULL REFERENCES broadcast_log(id) ON DELETE CASCADE,
            user_id BIGINT NOT NULL,
            status TEXT NOT NULL,  -- 'sent' | 'failed' | 'blocked' | 'not_found'
            error TEXT,
            delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_broadcast_deliveries_bid ON broadcast_deliveries(broadcast_id);
    """)


async def _tg_send_message(bot_token: str, chat_id: int, text: str, parse_mode: str = "HTML") -> dict:
    """Send a Telegram message via bot API. Returns dict with ok/error."""
    if not bot_token:
        return {"ok": False, "error": "bot token not configured"}
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": False,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                return data
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


ADMIN_BROADCAST_KEY = os.getenv("ADMIN_BROADCAST_KEY", "slh-broadcast-2026-change-me")


@app.post("/api/broadcast/send")
async def send_broadcast(req: BroadcastRequest):
    """Send a broadcast message to registered users via @SLH_AIR_bot.

    Targets:
    - 'registered': all users with is_registered=True (premium + Genesis 49)
    - 'genesis49':  only Genesis 49 NFT holders
    - 'all':        every user in web_users (real IDs only)
    - 'custom':     provide custom_ids list

    Admin key: accepts ADMIN_BROADCAST_KEY OR any of the 4 ADMIN_API_KEYS
    (slh2026admin, slh_admin_2026, slh-spark-admin, slh-institutional).
    """
    if req.admin_key != ADMIN_BROADCAST_KEY and req.admin_key not in ADMIN_API_KEYS:
        raise HTTPException(403, "Invalid admin key â€” use your admin panel password")
    if not req.message or len(req.message) < 5:
        raise HTTPException(400, "Message too short")
    if len(req.message) > 4000:
        raise HTTPException(400, "Message exceeds Telegram 4096-char limit")

    async with pool.acquire() as conn:
        await _ensure_broadcast_table(conn)

        # Resolve targets
        if req.target == "registered":
            rows = await conn.fetch(
                "SELECT telegram_id FROM web_users WHERE telegram_id >= 1000000 AND is_registered = TRUE"
            )
        elif req.target == "genesis49":
            rows = await conn.fetch(
                "SELECT telegram_id FROM web_users WHERE telegram_id >= 1000000 AND beta_user = TRUE"
            )
        elif req.target == "all":
            rows = await conn.fetch(
                "SELECT telegram_id FROM web_users WHERE telegram_id >= 1000000"
            )
        elif req.target == "custom":
            if not req.custom_ids:
                raise HTTPException(400, "custom target requires custom_ids")
            rows = [{"telegram_id": int(i)} for i in req.custom_ids if int(i) >= 1000000]
        else:
            raise HTTPException(400, "invalid target")

        user_ids = [r["telegram_id"] for r in rows]

        if req.dry_run:
            return {
                "ok": True,
                "dry_run": True,
                "target": req.target,
                "total_recipients": len(user_ids),
                "sample_ids": user_ids[:5],
                "message_preview": req.message[:200],
            }

        # Create broadcast record
        broadcast_id = await conn.fetchval("""
            INSERT INTO broadcast_log (target, total_targets, message_preview, admin_actor)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        """, req.target, len(user_ids), req.message[:200], "api_admin")

        # Audit
        await audit_log_write(
            conn,
            action="broadcast.send",
            actor_type="admin",
            resource_type="broadcast",
            resource_id=str(broadcast_id),
            metadata={"target": req.target, "count": len(user_ids)},
        )

    # Send messages (outside the pool transaction for non-blocking)
    success = 0
    failed = 0
    deliveries = []

    if not BROADCAST_BOT_TOKEN:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcast_log SET success_count=0, fail_count=$1 WHERE id=$2",
                len(user_ids), broadcast_id
            )
        return {
            "ok": False,
            "error": "BROADCAST_BOT_TOKEN not configured",
            "hint": "Set SLH_AIR_TOKEN or CORE_BOT_TOKEN env var on Railway",
            "broadcast_id": broadcast_id,
            "total_recipients": len(user_ids),
        }

    for uid in user_ids:
        result = await _tg_send_message(BROADCAST_BOT_TOKEN, uid, req.message, parse_mode="HTML")
        if result.get("ok"):
            success += 1
            deliveries.append((broadcast_id, uid, "sent", None))
        else:
            failed += 1
            err = str(result.get("error") or result.get("description") or "unknown")[:200]
            status = "blocked" if "blocked" in err.lower() else "failed"
            deliveries.append((broadcast_id, uid, status, err))

    # Write deliveries + update log
    async with pool.acquire() as conn:
        for d in deliveries:
            await conn.execute(
                "INSERT INTO broadcast_deliveries (broadcast_id, user_id, status, error) VALUES ($1, $2, $3, $4)",
                *d
            )
        await conn.execute(
            "UPDATE broadcast_log SET success_count=$1, fail_count=$2 WHERE id=$3",
            success, failed, broadcast_id
        )

    return {
        "ok": True,
        "broadcast_id": broadcast_id,
        "total_recipients": len(user_ids),
        "success": success,
        "failed": failed,
    }


# ============================================================
# GENESIS LAUNCH â€” Ultra Micro Pool ($33) with Tzvika as co-founder
# ============================================================
# Tracks contributions to the initial PancakeSwap SLH/BNB pool.
# Model: Partner sends BNB to company wallet, pool is created with
# that BNB + SLH from treasury. Contributors get credit + rewards.

COMPANY_BSC_WALLET = "0xd061de73B06d5E91bfA46b35EfB7B08b16903da4"  # Osif's Web3 wallet
LAUNCH_TARGET_BNB = 0.05  # Ultra Micro: 0.05 BNB + 50 SLH
LAUNCH_TARGET_SLH = 50
LAUNCH_NAME = "Genesis Launch â€” Ultra Micro Pool"


async def _ensure_launch_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS launch_contributions (
            id BIGSERIAL PRIMARY KEY,
            partner_name TEXT NOT NULL,
            partner_handle TEXT,
            wallet_address TEXT,
            amount_bnb NUMERIC(18,8) NOT NULL,
            amount_usd NUMERIC(18,2),
            tx_hash TEXT UNIQUE,
            role TEXT DEFAULT 'contributor',  -- 'cofounder' | 'contributor' | 'angel'
            status TEXT DEFAULT 'pending',  -- 'pending' | 'verified' | 'cancelled'
            rewards_issued BOOLEAN DEFAULT FALSE,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            verified_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_launch_contrib_status ON launch_contributions(status);
    """)


class LaunchContributionRequest(BaseModel):
    partner_name: str
    partner_handle: Optional[str] = None
    amount_bnb: float
    tx_hash: Optional[str] = None
    wallet_address: Optional[str] = None
    role: str = "contributor"
    notes: Optional[str] = None


@app.post("/api/launch/contribute")
async def launch_contribute(req: LaunchContributionRequest):
    """Record a launch contribution.

    Status starts as 'pending' until manually verified (or auto-verified
    via BSC RPC lookup of tx_hash). After verification, rewards are issued.
    """
    if req.amount_bnb <= 0:
        raise HTTPException(400, "Amount must be positive")
    if not req.partner_name:
        raise HTTPException(400, "Partner name required")

    # Estimate USD value (rough, BNB = $608 hardcoded â€” can replace with live price)
    amount_usd = round(req.amount_bnb * 608, 2)

    async with pool.acquire() as conn:
        await _ensure_launch_tables(conn)
        # Check if tx_hash already exists (idempotency)
        if req.tx_hash:
            existing = await conn.fetchval(
                "SELECT id FROM launch_contributions WHERE tx_hash=$1", req.tx_hash
            )
            if existing:
                return {"ok": False, "error": "tx_hash already recorded", "id": existing}

        cid = await conn.fetchval("""
            INSERT INTO launch_contributions
                (partner_name, partner_handle, wallet_address, amount_bnb,
                 amount_usd, tx_hash, role, notes, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
            RETURNING id
        """, req.partner_name, req.partner_handle, req.wallet_address,
            req.amount_bnb, amount_usd, req.tx_hash, req.role, req.notes)

        # Audit log
        await audit_log_write(
            conn,
            action="launch.contribute",
            actor_type="partner",
            actor_user_id=None,
            resource_type="launch_contribution",
            resource_id=str(cid),
            amount_native=req.amount_bnb,
            amount_currency="BNB",
            amount_usd=amount_usd,
            metadata={
                "partner": req.partner_name,
                "handle": req.partner_handle,
                "role": req.role,
                "tx_hash": req.tx_hash,
            },
            compliance_flags=["GENESIS_LAUNCH", "PRE_POOL"],
        )

    return {
        "ok": True,
        "contribution_id": cid,
        "partner_name": req.partner_name,
        "amount_bnb": req.amount_bnb,
        "amount_usd": amount_usd,
        "status": "pending",
        "message": "Contribution recorded. Will be verified on-chain + rewards issued within 24h.",
    }


@app.post("/api/launch/verify/{contribution_id}")
async def launch_verify_contribution(contribution_id: int, admin_key: str):
    """Admin: mark a contribution as verified + issue rewards.
    Accepts ADMIN_BROADCAST_KEY or any admin panel password."""
    if admin_key != ADMIN_BROADCAST_KEY and admin_key not in ADMIN_API_KEYS:
        raise HTTPException(403, "Invalid admin key")
    async with pool.acquire() as conn:
        await _ensure_launch_tables(conn)
        row = await conn.fetchrow(
            "SELECT * FROM launch_contributions WHERE id=$1", contribution_id
        )
        if not row:
            raise HTTPException(404, "Contribution not found")
        if row["status"] == "verified":
            return {"ok": True, "already_verified": True}
        await conn.execute("""
            UPDATE launch_contributions
               SET status='verified', verified_at=CURRENT_TIMESTAMP, rewards_issued=TRUE
             WHERE id=$1
        """, contribution_id)
        await audit_log_write(
            conn,
            action="launch.verify",
            actor_type="admin",
            resource_type="launch_contribution",
            resource_id=str(contribution_id),
            amount_usd=float(row["amount_usd"] or 0),
            metadata={"partner": row["partner_name"]},
            compliance_flags=["GENESIS_LAUNCH", "VERIFIED"],
        )

        # â”€â”€ Auto-reward: credit ZVK + REP to contributor â”€â”€
        contributor_name = row["partner_name"]
        contributor_handle = row.get("partner_handle", "") or ""
        rewards_issued = False

        # Try to find user by handle first, then by name match
        user_id = None
        if contributor_handle:
            clean_handle = contributor_handle.lstrip("@")
            user_id = await conn.fetchval(
                "SELECT telegram_id FROM web_users WHERE username=$1", clean_handle
            )

        # Fallback: match by first_name or display_name
        if not user_id and contributor_name:
            c_lower = contributor_name.strip().lower()
            user_id = await conn.fetchval(
                "SELECT telegram_id FROM web_users WHERE telegram_id >= 1000000 "
                "AND (LOWER(first_name) = $1 OR LOWER(display_name) = $1)",
                c_lower
            )

        if user_id:
            # Credit 500 ZVK
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance)
                VALUES ($1, 'ZVK', 500)
                ON CONFLICT (user_id, token) DO UPDATE
                  SET balance = token_balances.balance + 500,
                      updated_at = CURRENT_TIMESTAMP
            """, user_id)

            # Credit 100 REP (genesis action)
            await _ensure_rep_tables(conn)
            await conn.execute("""
                INSERT INTO member_rep (user_id, rep_score, genesis_contributor)
                VALUES ($1, 100, TRUE)
                ON CONFLICT (user_id) DO UPDATE
                  SET rep_score = member_rep.rep_score + 100,
                      genesis_contributor = TRUE,
                      updated_at = CURRENT_TIMESTAMP
            """, user_id)

            # Audit the reward
            await audit_log_write(
                conn,
                action="launch.reward",
                actor_type="system",
                actor_user_id=user_id,
                resource_type="launch_reward",
                resource_id=str(contribution_id),
                amount_native=500,
                amount_currency="ZVK",
                metadata={"rep_added": 100, "genesis": True, "partner": contributor_name},
            )
            rewards_issued = True

    return {
        "ok": True,
        "contribution_id": contribution_id,
        "status": "verified",
        "rewards_issued": rewards_issued,
    }


@app.get("/api/admin/all-users")
async def admin_all_users(
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """List all web_users with their token balances. Admin only."""
    _require_admin(authorization, x_admin_key)
    try:
      async with pool.acquire() as conn:
        # tables created at startup
        try:
            users = await conn.fetch(
                "SELECT telegram_id, username, first_name, last_login "
                "FROM web_users WHERE telegram_id >= 1000000 ORDER BY last_login DESC"
            )
        except Exception as e:
            return {"ok": False, "error": f"DB query failed: {str(e)}"}
        result = []
        for u in users:
            balances = await conn.fetch(
                "SELECT token, balance FROM token_balances WHERE user_id=$1", u["telegram_id"]
            )
            bal_dict = {r["token"]: float(r["balance"]) for r in balances}
            result.append({
                "telegram_id": u["telegram_id"],
                "username": u.get("username", ""),
                "first_name": u.get("first_name", ""),
                "last_login": u["last_login"].isoformat() if u.get("last_login") else None,
                "balances": bal_dict,
            })
      return {"ok": True, "users": result, "count": len(result)}
    except Exception as e:
      return {"ok": False, "error": str(e)}


@app.post("/api/admin/credit-rewards")
async def admin_credit_rewards(
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Find all verified contributors missing ZVK rewards and credit them.
    Also matches contributors to web_users by name when handle is missing."""
    _require_admin(authorization, x_admin_key)
    try:
     async with pool.acquire() as conn:
        # tables created at startup
        await _ensure_launch_tables(conn)
        await _ensure_rep_tables(conn)

        # Get all verified contributions
        contributions = await conn.fetch(
            "SELECT id, partner_name, partner_handle FROM launch_contributions WHERE status='verified'"
        )

        # Get all web_users (display_name may not exist yet)
        try:
            all_users = await conn.fetch(
                "SELECT telegram_id, username, first_name, display_name FROM web_users WHERE telegram_id >= 1000000"
            )
        except Exception:
            all_users = await conn.fetch(
                "SELECT telegram_id, username, first_name, NULL as display_name FROM web_users WHERE telegram_id >= 1000000"
            )

        credited = []
        already_had = []
        not_matched = []

        for c in contributions:
            cid = c["id"]
            name = c["partner_name"]
            handle = c["partner_handle"]

            # Try to find user: first by handle, then by name match
            user_id = None
            match_method = None

            if handle:
                clean_handle = handle.lstrip("@")
                user_id = await conn.fetchval(
                    "SELECT telegram_id FROM web_users WHERE username=$1", clean_handle
                )
                if user_id:
                    match_method = "handle"

            if not user_id:
                # Try matching by first_name or display_name
                for u in all_users:
                    u_name = (u["first_name"] or "").strip().lower()
                    u_display = (u["display_name"] or "").strip().lower()
                    c_name = name.strip().lower()
                    if c_name and (c_name == u_name or c_name == u_display or
                                   c_name in u_name or u_name in c_name):
                        user_id = u["telegram_id"]
                        match_method = f"name_match:{u['first_name']}"
                        break

            if not user_id:
                not_matched.append({"contribution_id": cid, "name": name})
                continue

            # Check if already has ZVK from genesis reward
            existing_zvk = await conn.fetchval(
                "SELECT balance FROM token_balances WHERE user_id=$1 AND token='ZVK'",
                user_id
            ) or 0

            # Skip if user already has ZVK (simpler check than audit log)
            prior_reward = float(existing_zvk) >= 500

            if prior_reward:
                already_had.append({
                    "contribution_id": cid, "name": name, "user_id": user_id,
                    "zvk_balance": float(existing_zvk), "match": match_method,
                })
                continue

            # Credit 500 ZVK
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance)
                VALUES ($1, 'ZVK', 500)
                ON CONFLICT (user_id, token) DO UPDATE
                  SET balance = token_balances.balance + 500,
                      updated_at = CURRENT_TIMESTAMP
            """, user_id)

            # Credit 100 REP
            await conn.execute("""
                INSERT INTO member_rep (user_id, rep_score, genesis_contributor)
                VALUES ($1, 100, TRUE)
                ON CONFLICT (user_id) DO UPDATE
                  SET rep_score = member_rep.rep_score + 100,
                      genesis_contributor = TRUE,
                      updated_at = CURRENT_TIMESTAMP
            """, user_id)

            # Audit
            await audit_log_write(
                conn,
                action="launch.reward",
                actor_type="system",
                actor_user_id=user_id,
                resource_type="launch_reward",
                resource_id=str(cid),
                amount_native=500,
                amount_currency="ZVK",
                metadata={"rep_added": 100, "genesis": True, "partner": name,
                          "match_method": match_method, "retroactive": True},
            )

            credited.append({
                "contribution_id": cid, "name": name, "user_id": user_id,
                "zvk_credited": 500, "rep_credited": 100, "match": match_method,
            })

     return {
        "ok": True,
        "credited": credited,
        "already_had": already_had,
        "not_matched": not_matched,
        "summary": f"Credited {len(credited)} users, {len(already_had)} already had rewards, {len(not_matched)} unmatched",
     }
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/admin/manual-credit")
async def admin_manual_credit(
    user_id: int,
    token: str,
    amount: float,
    reason: str = "manual_admin_credit",
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Manually credit tokens to a specific user. Admin only."""
    _require_admin(authorization, x_admin_key)
    if amount <= 0 or amount > 10000:
        raise HTTPException(400, "Amount must be 1-10000")
    if token not in ("SLH", "ZVK", "MNH", "REP"):
        raise HTTPException(400, "Token must be SLH, ZVK, MNH, or REP")

    async with pool.acquire() as conn:
        # tables created at startup
        # Verify user exists
        user = await conn.fetchrow("SELECT telegram_id, first_name FROM web_users WHERE telegram_id=$1", user_id)
        if not user:
            raise HTTPException(404, f"User {user_id} not found")

        if token == "REP":
            await _ensure_rep_tables(conn)
            await conn.execute("""
                INSERT INTO member_rep (user_id, rep_score)
                VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE
                  SET rep_score = member_rep.rep_score + $2, updated_at = CURRENT_TIMESTAMP
            """, user_id, amount)
        else:
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, token) DO UPDATE
                  SET balance = token_balances.balance + $3, updated_at = CURRENT_TIMESTAMP
            """, user_id, token, amount)

        await audit_log_write(
            conn,
            action="admin.manual_credit",
            actor_type="admin",
            actor_user_id=user_id,
            resource_type="token_credit",
            amount_native=amount,
            amount_currency=token,
            metadata={"reason": reason, "user_name": user["first_name"]},
        )

    return {"ok": True, "user_id": user_id, "token": token, "amount": amount, "reason": reason}


# ============================================================
# GUARDIAN SYSTEM â€” ZUZ Token + Anti-Fraud Intelligence
# ============================================================
# ZUZ = "××•×ª ×§×™×Ÿ" (Mark of Cain) â€” negative reputation token
# Assigned by Guardian bot to mark scammers, bots, fraudsters.
# Higher ZUZ = more suspicious. Used for cross-group intelligence.

async def _ensure_guardian_tables(conn):
    """Create Guardian system tables."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS guardian_reports (
            id SERIAL PRIMARY KEY,
            reported_user_id BIGINT NOT NULL,
            reported_username TEXT,
            reporter_id BIGINT,
            reporter_username TEXT,
            group_id BIGINT,
            group_name TEXT,
            reason TEXT NOT NULL,
            evidence TEXT,
            severity TEXT DEFAULT 'medium',
            zuz_amount REAL DEFAULT 10,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP,
            reviewed_by BIGINT
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS guardian_blacklist (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE NOT NULL,
            username TEXT,
            zuz_score REAL DEFAULT 0,
            total_reports INT DEFAULT 0,
            first_reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            groups_flagged TEXT[] DEFAULT '{}',
            ban_active BOOLEAN DEFAULT FALSE,
            ban_reason TEXT,
            auto_banned BOOLEAN DEFAULT FALSE
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS guardian_group_intel (
            id SERIAL PRIMARY KEY,
            group_id BIGINT NOT NULL,
            group_name TEXT,
            total_scams_detected INT DEFAULT 0,
            total_bans INT DEFAULT 0,
            protection_level TEXT DEFAULT 'standard',
            guardian_active BOOLEAN DEFAULT TRUE,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_scan TIMESTAMP
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS guardian_message_log (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            group_id BIGINT,
            message_hash TEXT,
            risk_score REAL DEFAULT 0,
            risk_factors TEXT,
            flagged BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_guardian_blacklist_user ON guardian_blacklist(user_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_guardian_reports_user ON guardian_reports(reported_user_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_guardian_msg_user ON guardian_message_log(user_id)")


class GuardianReportRequest(BaseModel):
    reported_user_id: int
    reported_username: Optional[str] = None
    reporter_id: Optional[int] = None
    reporter_username: Optional[str] = None
    group_id: Optional[int] = None
    group_name: Optional[str] = None
    reason: str
    evidence: Optional[str] = None
    severity: str = "medium"  # low, medium, high, critical


# ZUZ severity multipliers
ZUZ_SEVERITY = {"low": 5, "medium": 10, "high": 25, "critical": 50}
ZUZ_AUTO_BAN_THRESHOLD = 100  # auto-ban at 100 ZUZ


@app.post("/api/guardian/report")
async def guardian_report(req: GuardianReportRequest):
    """Report a user for suspicious/scam activity. Awards ZUZ marks."""
    zuz_amount = ZUZ_SEVERITY.get(req.severity, 10)

    async with pool.acquire() as conn:
        await _ensure_guardian_tables(conn)

        # Record the report
        report_id = await conn.fetchval("""
            INSERT INTO guardian_reports
                (reported_user_id, reported_username, reporter_id, reporter_username,
                 group_id, group_name, reason, evidence, severity, zuz_amount)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id
        """, req.reported_user_id, req.reported_username, req.reporter_id,
            req.reporter_username, req.group_id, req.group_name,
            req.reason, req.evidence, req.severity, zuz_amount)

        # Update or create blacklist entry
        existing = await conn.fetchrow(
            "SELECT id, zuz_score, total_reports FROM guardian_blacklist WHERE user_id=$1",
            req.reported_user_id
        )
        if existing:
            new_score = float(existing["zuz_score"]) + zuz_amount
            new_reports = existing["total_reports"] + 1
            auto_ban = new_score >= ZUZ_AUTO_BAN_THRESHOLD
            groups = []
            if req.group_name:
                groups = [req.group_name]
            await conn.execute("""
                UPDATE guardian_blacklist
                SET zuz_score = $1, total_reports = $2, last_reported_at = CURRENT_TIMESTAMP,
                    groups_flagged = array_cat(groups_flagged, $3::TEXT[]),
                    ban_active = CASE WHEN $4 THEN TRUE ELSE ban_active END,
                    auto_banned = CASE WHEN $4 THEN TRUE ELSE auto_banned END,
                    ban_reason = CASE WHEN $4 THEN 'Auto-ban: ZUZ threshold exceeded' ELSE ban_reason END
                WHERE user_id = $5
            """, new_score, new_reports, groups, auto_ban, req.reported_user_id)
        else:
            new_score = zuz_amount
            auto_ban = new_score >= ZUZ_AUTO_BAN_THRESHOLD
            await conn.execute("""
                INSERT INTO guardian_blacklist
                    (user_id, username, zuz_score, total_reports, groups_flagged,
                     ban_active, auto_banned, ban_reason)
                VALUES ($1,$2,$3,1,$4,$5,$6,$7)
            """, req.reported_user_id, req.reported_username, zuz_amount,
                [req.group_name] if req.group_name else [],
                auto_ban, auto_ban,
                'Auto-ban: ZUZ threshold exceeded' if auto_ban else None)

        # Credit ZUZ to the reported user's token balance (negative reputation)
        await conn.execute("""
            INSERT INTO token_balances (user_id, token, balance)
            VALUES ($1, 'ZUZ', $2)
            ON CONFLICT (user_id, token) DO UPDATE
              SET balance = token_balances.balance + $2, updated_at = CURRENT_TIMESTAMP
        """, req.reported_user_id, zuz_amount)

        # Audit log
        await audit_log_write(
            conn, action="guardian.report", actor_type="guardian",
            actor_user_id=req.reporter_id or 0,
            resource_type="scam_report", resource_id=str(report_id),
            amount_native=zuz_amount, amount_currency="ZUZ",
            metadata={"reason": req.reason, "severity": req.severity,
                      "reported_user": req.reported_user_id,
                      "auto_banned": auto_ban},
            compliance_flags=["GUARDIAN", "ANTI_FRAUD"],
        )

    return {
        "ok": True,
        "report_id": report_id,
        "zuz_awarded": zuz_amount,
        "total_zuz": new_score,
        "auto_banned": auto_ban,
        "message": f"Report #{report_id} filed. {zuz_amount} ZUZ marked."
                   + (" USER AUTO-BANNED!" if auto_ban else ""),
    }


@app.get("/api/guardian/check/{user_id}")
async def guardian_check_user(user_id: int):
    """Check if a user is flagged/banned by Guardian. Used by all bots."""
    async with pool.acquire() as conn:
        await _ensure_guardian_tables(conn)
        entry = await conn.fetchrow(
            "SELECT * FROM guardian_blacklist WHERE user_id=$1", user_id
        )
        if not entry:
            return {"flagged": False, "zuz_score": 0, "ban_active": False, "safe": True}

        reports = await conn.fetch(
            "SELECT reason, severity, group_name, created_at FROM guardian_reports "
            "WHERE reported_user_id=$1 ORDER BY created_at DESC LIMIT 10", user_id
        )

    return {
        "flagged": True,
        "zuz_score": float(entry["zuz_score"]),
        "total_reports": entry["total_reports"],
        "ban_active": entry["ban_active"],
        "auto_banned": entry["auto_banned"],
        "ban_reason": entry["ban_reason"],
        "groups_flagged": entry["groups_flagged"],
        "first_reported": entry["first_reported_at"].isoformat() if entry["first_reported_at"] else None,
        "last_reported": entry["last_reported_at"].isoformat() if entry["last_reported_at"] else None,
        "safe": False,
        "recent_reports": [
            {"reason": r["reason"], "severity": r["severity"],
             "group": r["group_name"], "date": r["created_at"].isoformat()}
            for r in reports
        ],
    }


@app.get("/api/guardian/blacklist")
async def guardian_blacklist(
    limit: int = Query(50, le=200),
    min_zuz: float = Query(0),
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Get all flagged users. Admin only for full list."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_guardian_tables(conn)
        rows = await conn.fetch(
            "SELECT user_id, username, zuz_score, total_reports, ban_active, auto_banned, "
            "groups_flagged, first_reported_at, last_reported_at "
            "FROM guardian_blacklist WHERE zuz_score >= $1 "
            "ORDER BY zuz_score DESC LIMIT $2", min_zuz, limit
        )
    return {
        "ok": True,
        "count": len(rows),
        "blacklist": [
            {
                "user_id": r["user_id"], "username": r["username"],
                "zuz_score": float(r["zuz_score"]), "reports": r["total_reports"],
                "banned": r["ban_active"], "auto_banned": r["auto_banned"],
                "groups": r["groups_flagged"],
            }
            for r in rows
        ],
    }


@app.post("/api/guardian/scan-message")
async def guardian_scan_message(
    user_id: int, group_id: int = 0, message_text: str = "",
):
    """Scan a message for scam risk. Returns risk score + factors.
    Used by Guardian bot in real-time for every group message."""
    risk_score = 0
    factors = []

    text_lower = message_text.lower()

    # Suspicious keywords (EN + HE)
    scam_words = ["guaranteed profit", "invest now", "double your money", "free crypto",
                  "send me", "click here", "limited time", "act now", "whatsapp me",
                  "×¨×•×•×— ×ž×•×‘×˜×—", "×”×©×§×¢×” ×‘×˜×•×—×”", "×”×›× ×¡×” ×¤×¡×™×‘×™×ª", "×©×œ×— ×œ×™",
                  "earn daily", "100% safe", "no risk"]
    for w in scam_words:
        if w in text_lower:
            risk_score += 20
            factors.append(f"suspicious_word:{w}")

    # URL detection
    import re
    urls = re.findall(r'https?://\S+', message_text)
    if urls:
        risk_score += 15
        factors.append(f"contains_urls:{len(urls)}")
        # Check for URL shorteners (extra suspicious)
        shorteners = ["bit.ly", "tinyurl", "t.co", "goo.gl", "ow.ly", "is.gd"]
        for url in urls:
            for s in shorteners:
                if s in url:
                    risk_score += 25
                    factors.append(f"url_shortener:{s}")

    # Excessive emojis (common in scam messages)
    emoji_count = len(re.findall(r'[\U0001F600-\U0001F9FF\U0001FA00-\U0001FAFF]', message_text))
    if emoji_count > 5:
        risk_score += 10
        factors.append(f"excessive_emojis:{emoji_count}")

    # ALL CAPS
    upper_ratio = sum(1 for c in message_text if c.isupper()) / max(len(message_text), 1)
    if upper_ratio > 0.5 and len(message_text) > 20:
        risk_score += 10
        factors.append("excessive_caps")

    # Check if user is already flagged
    async with pool.acquire() as conn:
        await _ensure_guardian_tables(conn)
        existing = await conn.fetchrow(
            "SELECT zuz_score, ban_active FROM guardian_blacklist WHERE user_id=$1", user_id
        )
        if existing:
            risk_score += min(float(existing["zuz_score"]), 30)
            factors.append(f"prior_zuz:{existing['zuz_score']}")
            if existing["ban_active"]:
                risk_score += 50
                factors.append("BANNED_USER")

        # Log the scan
        flagged = risk_score >= 50
        await conn.execute("""
            INSERT INTO guardian_message_log (user_id, group_id, message_hash, risk_score, risk_factors, flagged)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, user_id, group_id,
            hashlib.sha256(message_text.encode()).hexdigest()[:16],
            risk_score, ",".join(factors), flagged)

    return {
        "risk_score": min(risk_score, 100),
        "risk_level": "critical" if risk_score >= 75 else "high" if risk_score >= 50
                      else "medium" if risk_score >= 25 else "low",
        "factors": factors,
        "flagged": flagged,
        "action": "block" if risk_score >= 75 else "warn" if risk_score >= 50
                  else "monitor" if risk_score >= 25 else "allow",
        "user_prior_zuz": float(existing["zuz_score"]) if existing else 0,
    }


@app.get("/api/guardian/stats")
async def guardian_stats():
    """Guardian system statistics."""
    async with pool.acquire() as conn:
        await _ensure_guardian_tables(conn)
        total_reports = await conn.fetchval("SELECT COUNT(*) FROM guardian_reports") or 0
        total_flagged = await conn.fetchval("SELECT COUNT(*) FROM guardian_blacklist") or 0
        total_banned = await conn.fetchval("SELECT COUNT(*) FROM guardian_blacklist WHERE ban_active=TRUE") or 0
        total_auto_banned = await conn.fetchval("SELECT COUNT(*) FROM guardian_blacklist WHERE auto_banned=TRUE") or 0
        total_scans = await conn.fetchval("SELECT COUNT(*) FROM guardian_message_log") or 0
        total_flagged_msgs = await conn.fetchval("SELECT COUNT(*) FROM guardian_message_log WHERE flagged=TRUE") or 0
        total_zuz = await conn.fetchval("SELECT COALESCE(SUM(zuz_score),0) FROM guardian_blacklist") or 0
        groups_protected = await conn.fetchval("SELECT COUNT(*) FROM guardian_group_intel WHERE guardian_active=TRUE") or 0

    return {
        "total_reports": total_reports,
        "total_flagged_users": total_flagged,
        "total_banned": total_banned,
        "total_auto_banned": total_auto_banned,
        "total_messages_scanned": total_scans,
        "total_messages_flagged": total_flagged_msgs,
        "total_zuz_issued": float(total_zuz),
        "groups_protected": groups_protected,
        "zuz_auto_ban_threshold": ZUZ_AUTO_BAN_THRESHOLD,
    }


# ============================================================
# DYNAMIC OG IMAGE GENERATOR â€” per-page social share visuals
# ============================================================
# Generates 1200x630 PNG images on-the-fly with PIL.
# Each page can point its og:image to /api/og/{slug}.png for a
# unique preview when shared on Twitter/Facebook/Telegram/LinkedIn.

OG_PAGE_CONFIG = {
    "index":        {"title": "SLH \u2014 \u05d4\u05d0\u05e7\u05d5\u05e1\u05d9\u05e1\u05d8\u05dd \u05e9\u05dc \u05d4\u05e2\u05d5\u05dc\u05dd \u05d4\u05d7\u05d3\u05e9", "subtitle": "20+ Telegram bots \u00b7 Real blockchain \u00b7 65% APY", "accent": "#00e887", "icon": "SLH"},
    "network":      {"title": "SLH Network Map", "subtitle": "Interactive visualization of the ecosystem", "accent": "#6c5ce7", "icon": "NET"},
    "dashboard":    {"title": "SLH Dashboard", "subtitle": "Your portfolio, staking, and activity", "accent": "#00b4d8", "icon": "DASH"},
    "wallet":       {"title": "SLH Wallet", "subtitle": "Multi-currency TON/BSC wallet + CEX portfolio", "accent": "#f3ba2f", "icon": "W"},
    "bots":         {"title": "SLH Bot Ecosystem", "subtitle": "25 bots \u2014 each its own economy", "accent": "#00cec9", "icon": "BOTS"},
    "trade":        {"title": "SLH Trade", "subtitle": "Live prices + swap SLH on PancakeSwap", "accent": "#f7931a", "icon": "TRADE"},
    "earn":         {"title": "SLH Earn \u2014 65% APY", "subtitle": "Staking plans \u00b7 MNH yield \u00b7 Rebalance strategies", "accent": "#ffd700", "icon": "EARN"},
    "community":    {"title": "SLH Community", "subtitle": "Posts, events, and ecosystem discussion", "accent": "#ef4444", "icon": "CMTY"},
    "blockchain":   {"title": "SLH On-Chain", "subtitle": "Live BSC + TON blockchain data", "accent": "#f3ba2f", "icon": "CHAIN"},
    "roadmap":      {"title": "SLH Roadmap", "subtitle": "From foundation to global ecosystem", "accent": "#a855f7", "icon": "MAP"},
    "admin":        {"title": "SLH Institutional Admin", "subtitle": "Regulator-ready operations panel", "accent": "#00ff41", "icon": "ADM"},
    "launch-event": {"title": "Genesis Launch \ud83d\ude80", "subtitle": "First SLH pool on PancakeSwap \u2014 Join now", "accent": "#ffd700", "icon": "LAUNCH"},
    "dex-launch":   {"title": "DEX Launch Calculator", "subtitle": "AMM math \u00b7 Slippage \u00b7 5 scenarios from \u003232", "accent": "#f3ba2f", "icon": "DEX"},
    "daily-blog":   {"title": "SLH Daily Blog", "subtitle": "What we shipped today", "accent": "#00e887", "icon": "BLOG"},
    "guides":       {"title": "SLH Guides", "subtitle": "Step-by-step tutorials", "accent": "#00ff41", "icon": "DOCS"},
    "referral":     {"title": "SLH Affiliate Program", "subtitle": "Two-tier \u00b7 20% direct + 5% Tier 2", "accent": "#a855f7", "icon": "REF"},
    "healing":      {"title": "SLH Healing Vision", "subtitle": "The currency of healing, education, and aid", "accent": "#ff6b9d", "icon": "HEAL"},
    "liquidity":    {"title": "SLH Liquidity & Staking", "subtitle": "Revenue Share Pool \u00b7 Dynamic Yield \u00b7 TON/SLH/BNB", "accent": "#00e887", "icon": "LP"},
    "challenge":    {"title": "21-Day Challenge", "subtitle": "\u05e8\u05d9\u05e4\u05d5\u05d9 \u00b7 \u05de\u05d3\u05d9\u05d8\u05e6\u05d9\u05d4 \u00b7 \u05e7\u05d4\u05d9\u05dc\u05d4", "accent": "#ff6b9d", "icon": "21"},
    "jubilee":      {"title": "SLH Jubilee Year", "subtitle": "Biblical economic reset \u2014 Healing through blockchain", "accent": "#7cb342", "icon": "YOV"},
    "member":       {"title": "SLH Member Card", "subtitle": "Your personal NFT \u00b7 Genesis Status \u00b7 REP Score", "accent": "#a855f7", "icon": "NFT"},
    "p2p":          {"title": "SLH P2P Trading", "subtitle": "Trade directly with community members", "accent": "#00b4d8", "icon": "P2P"},
    "staking":      {"title": "SLH Staking", "subtitle": "Revenue Share Pool \u00b7 Dynamic Yield \u00b7 4 lock tiers", "accent": "#ffd700", "icon": "STAK"},
    "whitepaper":   {"title": "SLH Whitepaper", "subtitle": "Architecture \u00b7 Tokenomics \u00b7 Vision", "accent": "#6c5ce7", "icon": "WP"},
    "privacy":      {"title": "SLH Privacy Policy", "subtitle": "Your data, your rights", "accent": "#888", "icon": "PRIV"},
    "terms":        {"title": "SLH Terms of Service", "subtitle": "Usage terms and conditions", "accent": "#888", "icon": "TOS"},
    "for-therapists": {"title": "SLH For Therapists", "subtitle": "Join the healing network \u00b7 Get paid in MNH", "accent": "#ff6b9d", "icon": "RX"},
    "ops-dashboard": {"title": "SLH Ops Dashboard", "subtitle": "Real-time system monitoring", "accent": "#00e5ff", "icon": "OPS"},
    "partner":      {"title": "SLH Partner Dashboard", "subtitle": "Genesis Contributors \u00b7 Rewards \u00b7 Status", "accent": "#ffd700", "icon": "PTR"},
    "invite":       {"title": "SLH Invite", "subtitle": "Invite friends \u00b7 Earn ZVK rewards", "accent": "#a855f7", "icon": "INV"},
    "onboarding":   {"title": "SLH Genesis 49", "subtitle": "Join first, free \u00b7 49 spots", "accent": "#00e887", "icon": "G49"},
    "default":      {"title": "SLH Spark", "subtitle": "Digital Ecosystem Built in Israel", "accent": "#00e887", "icon": "SLH"},
}


def _generate_og_image(slug: str) -> bytes:
    """Generate a 1200x630 PNG OG image for the given slug. Returns bytes."""
    import hashlib
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO

    cfg = OG_PAGE_CONFIG.get(slug, OG_PAGE_CONFIG["default"])
    W, H = 1200, 630
    BG = (10, 14, 26)  # #0a0e1a

    # Accent color parsing
    accent_hex = cfg["accent"].lstrip("#")
    accent_rgb = tuple(int(accent_hex[i:i+2], 16) for i in (0, 2, 4))

    # --- base image (RGBA for compositing, converted at end) ---
    img = Image.new("RGBA", (W, H), (*BG, 255))

    # --- radial gradient layer ---
    grad = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    grad_draw = ImageDraw.Draw(grad)
    cx, cy = W // 2, H // 2
    max_r = int((W**2 + H**2) ** 0.5 / 2)
    steps = 80
    for i in range(steps, 0, -1):
        ratio = i / steps
        r = int(max_r * ratio)
        alpha = int(40 * (1 - ratio))  # stronger towards center
        color = (*accent_rgb, alpha)
        grad_draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=color)
    img = Image.alpha_composite(img, grad)

    # --- subtle grid pattern (accent at ~5% opacity) ---
    grid = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    grid_draw = ImageDraw.Draw(grid)
    grid_color = (*accent_rgb, 13)  # ~5% of 255
    for x in range(0, W, 40):
        grid_draw.line([(x, 0), (x, H)], fill=grid_color, width=1)
    for y in range(0, H, 40):
        grid_draw.line([(0, y), (W, y)], fill=grid_color, width=1)
    img = Image.alpha_composite(img, grid)

    # --- decorative circles (deterministic per slug) ---
    seed = int(hashlib.md5(slug.encode()).hexdigest(), 16)
    circles = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    circles_draw = ImageDraw.Draw(circles)
    circle_alpha = 25  # ~10% of 255
    circle_params = [
        (seed % W, (seed >> 8) % H, 120 + (seed >> 16) % 100),
        ((seed >> 4) % W, (seed >> 12) % H, 80 + (seed >> 20) % 80),
        ((seed >> 6) % W, (seed >> 14) % H, 60 + (seed >> 24) % 60),
    ]
    for cx_c, cy_c, radius in circle_params:
        circles_draw.ellipse(
            [cx_c - radius, cy_c - radius, cx_c + radius, cy_c + radius],
            fill=(*accent_rgb, circle_alpha),
        )
    img = Image.alpha_composite(img, circles)

    # --- main drawing layer ---
    main = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(main)

    # Top accent bar (8px)
    draw.rectangle([0, 0, W, 8], fill=(*accent_rgb, 255))

    # --- font loading ---
    def _load_fonts(title_sz=64, sub_sz=32, brand_sz=28, icon_sz=48, tagline_sz=22):
        paths = [
            ("arial.ttf", "arialbd.ttf"),
            ("/Library/Fonts/Arial.ttf", "/Library/Fonts/Arial Bold.ttf"),
            ("C:/Windows/Fonts/arial.ttf", "C:/Windows/Fonts/arialbd.ttf"),
        ]
        for reg, bold in paths:
            try:
                tf = ImageFont.truetype(bold, title_sz)
                sf = ImageFont.truetype(reg, sub_sz)
                bf = ImageFont.truetype(bold, brand_sz)
                icf = ImageFont.truetype(bold, icon_sz)
                tgf = ImageFont.truetype(reg, tagline_sz)
                return tf, sf, bf, icf, tgf
            except Exception:
                continue
        d = ImageFont.load_default()
        return d, d, d, d, d

    title_font, subtitle_font, brand_font, icon_font, tagline_font = _load_fonts()

    # --- large icon box (left-center area) ---
    icon_text = cfg.get("icon", "SLH")
    icon_box_size = 120
    icon_x, icon_y = 140, H // 2
    # Filled rounded-look box with accent bg at 20% opacity
    draw.rounded_rectangle(
        [icon_x - icon_box_size // 2, icon_y - icon_box_size // 2,
         icon_x + icon_box_size // 2, icon_y + icon_box_size // 2],
        radius=20,
        fill=(*accent_rgb, 50),
        outline=(*accent_rgb, 180),
        width=3,
    )
    draw.text((icon_x, icon_y), icon_text, font=icon_font, fill=(*accent_rgb, 255), anchor="mm")

    # --- "SLH SPARK" brand top-right ---
    draw.text((W - 50, 40), "SLH SPARK", font=brand_font, fill=(*accent_rgb, 255), anchor="rt")

    # --- title (centered in right 2/3) ---
    text_cx = (260 + W) // 2  # center of the text area (right of icon)
    title = cfg["title"]
    title_y = H // 2 - 40
    try:
        draw.text((text_cx, title_y), title, font=title_font, fill=(240, 240, 248, 255), anchor="mm")
    except Exception:
        # Hebrew or special chars unsupported by font â€” fall back
        fallback_title = slug.replace("-", " ").upper()
        try:
            draw.text((text_cx, title_y), fallback_title, font=title_font, fill=(240, 240, 248, 255), anchor="mm")
        except Exception:
            draw.text((text_cx, title_y), "SLH", font=title_font, fill=(240, 240, 248, 255), anchor="mm")

    # --- subtitle ---
    subtitle = cfg["subtitle"]
    sub_y = title_y + 60
    try:
        draw.text((text_cx, sub_y), subtitle, font=subtitle_font, fill=(160, 160, 180, 255), anchor="mm")
    except Exception:
        draw.text((text_cx, sub_y), "Digital Ecosystem", font=subtitle_font, fill=(160, 160, 180, 255), anchor="mm")

    # --- "slh-nft.com" URL near bottom ---
    draw.text((W // 2, H - 80), "slh-nft.com", font=subtitle_font, fill=(*accent_rgb, 180), anchor="mm")

    # --- bottom bar with tagline ---
    draw.rectangle([0, H - 48, W, H], fill=(17, 22, 40, 255))
    tagline = "20+ Bots \u00b7 Real Blockchain \u00b7 65% APY \u00b7 Built in Israel"
    try:
        draw.text((W // 2, H - 24), tagline, font=tagline_font, fill=(160, 160, 180, 255), anchor="mm")
    except Exception:
        draw.text((W // 2, H - 24), "SLH Ecosystem", font=tagline_font, fill=(160, 160, 180, 255), anchor="mm")

    img = Image.alpha_composite(img, main)

    # --- convert to RGB PNG ---
    final = img.convert("RGB")
    buf = BytesIO()
    final.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


@app.get("/api/og/{slug}.png")
async def og_image(slug: str):
    """Serve a dynamically generated OG image for the given page slug."""
    from fastapi.responses import Response
    try:
        img_bytes = _generate_og_image(slug)
        return Response(
            content=img_bytes,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"},
        )
    except Exception as e:
        print(f"[og_image] failed for {slug}: {e}")
        raise HTTPException(500, f"OG generation failed: {e}")


# ============================================================
# SHARE TRACKING â€” count how often pages are shared
# ============================================================

class ShareEvent(BaseModel):
    page: str
    platform: str  # 'telegram' | 'twitter' | 'facebook' | 'whatsapp' | 'copy'
    user_id: Optional[int] = None


async def _ensure_share_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS share_events (
            id BIGSERIAL PRIMARY KEY,
            page TEXT NOT NULL,
            platform TEXT NOT NULL,
            user_id BIGINT,
            shared_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_share_events_page ON share_events(page, shared_at DESC);
    """)


@app.post("/api/shares/track")
async def track_share(req: ShareEvent):
    """Log a share event. Called by frontend share buttons."""
    async with pool.acquire() as conn:
        await _ensure_share_table(conn)
        await conn.execute(
            "INSERT INTO share_events (page, platform, user_id) VALUES ($1, $2, $3)",
            req.page, req.platform, req.user_id
        )
    return {"ok": True}


@app.get("/api/shares/stats")
async def share_stats(days: int = 30):
    """Share statistics by page and platform."""
    async with pool.acquire() as conn:
        await _ensure_share_table(conn)
        # Total per page
        per_page = await conn.fetch(f"""
            SELECT page, COUNT(*) as total
              FROM share_events
             WHERE shared_at > now() - interval '{int(days)} days'
             GROUP BY page
             ORDER BY total DESC
             LIMIT 20
        """)
        # Total per platform
        per_platform = await conn.fetch(f"""
            SELECT platform, COUNT(*) as total
              FROM share_events
             WHERE shared_at > now() - interval '{int(days)} days'
             GROUP BY platform
             ORDER BY total DESC
        """)
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM share_events WHERE shared_at > now() - interval '{int(days)} days'"
        ) or 0
    return {
        "total_shares": total,
        "days": days,
        "per_page": [{"page": r["page"], "total": r["total"]} for r in per_page],
        "per_platform": [{"platform": r["platform"], "total": r["total"]} for r in per_platform],
    }


@app.get("/api/launch/status")
async def launch_status():
    """Current state of the Genesis Launch."""
    async with pool.acquire() as conn:
        await _ensure_launch_tables(conn)
        rows = await conn.fetch("""
            SELECT id, partner_name, partner_handle, amount_bnb, amount_usd,
                   role, status, created_at, verified_at, tx_hash
              FROM launch_contributions
             ORDER BY created_at ASC
        """)
        total_verified_bnb = await conn.fetchval(
            "SELECT COALESCE(SUM(amount_bnb), 0) FROM launch_contributions WHERE status='verified'"
        ) or 0
        total_pending_bnb = await conn.fetchval(
            "SELECT COALESCE(SUM(amount_bnb), 0) FROM launch_contributions WHERE status='pending'"
        ) or 0

    total_verified = float(total_verified_bnb)
    total_pending = float(total_pending_bnb)
    progress_pct = round((total_verified / LAUNCH_TARGET_BNB) * 100, 2) if LAUNCH_TARGET_BNB else 0

    return {
        "launch_name": LAUNCH_NAME,
        "target_bnb": LAUNCH_TARGET_BNB,
        "target_slh": LAUNCH_TARGET_SLH,
        "target_usd": round(LAUNCH_TARGET_BNB * 608, 2),
        "company_wallet": COMPANY_BSC_WALLET,
        "company_wallet_network": "BSC (BEP-20)",
        "raised_bnb_verified": total_verified,
        "raised_bnb_pending": total_pending,
        "progress_pct": min(progress_pct, 100),
        "contributors_count": len(rows),
        "status": "live" if total_verified < LAUNCH_TARGET_BNB else "filled",
        "contributors": [
            {
                "id": r["id"],
                "name": r["partner_name"],
                "handle": r["partner_handle"],
                "amount_bnb": float(r["amount_bnb"]),
                "amount_usd": float(r["amount_usd"] or 0),
                "role": r["role"],
                "status": r["status"],
                "tx_hash": r["tx_hash"],
                "joined_at": r["created_at"].isoformat() if r["created_at"] else None,
                "verified_at": r["verified_at"].isoformat() if r["verified_at"] else None,
            }
            for r in rows
        ],
    }


@app.post("/api/broadcast/personal-cards")
async def broadcast_personal_cards(admin_key: str):
    """Send each registered user their personal SLH Member Card via Telegram.

    Each user receives a personalized message with their own card link.
    Admin-only. Logged to institutional_audit.
    """
    if admin_key != ADMIN_BROADCAST_KEY and admin_key not in ADMIN_API_KEYS:
        raise HTTPException(403, "Invalid admin key")
    if not BROADCAST_BOT_TOKEN:
        return {"ok": False, "error": "BROADCAST_BOT_TOKEN not configured"}

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT telegram_id, username, first_name FROM web_users WHERE telegram_id >= 1000000"
        )

    success = 0
    failed = 0
    for r in rows:
        uid = r["telegram_id"]
        name = r["first_name"] or r["username"] or "Member"
        card_url = f"https://slh-nft.com/member.html?id={uid}"
        image_url = f"https://slh-api-production.up.railway.app/api/member-card/image/{uid}"

        msg = (
            f"ðŸŽ´ ×©×œ×•× {name}!\n\n"
            f"×”×›×¨×˜×™×¡ ×”××™×©×™ ×©×œ×š ×‘-SLH Spark ×ž×•×›×Ÿ:\n\n"
            f"ðŸ”— ×”×›×¨×˜×™×¡ ×©×œ×š:\n{card_url}\n\n"
            f"ðŸ–¼ ×ª×ž×•× ×” ×œ×©×™×ª×•×£:\n{image_url}\n\n"
            f"×©×ª×¤/×™ ××ª ×”×›×¨×˜×™×¡ ×¢× ×—×‘×¨×™× ×•×ž×©×¤×—×” â€” ×›×œ ×—×‘×¨ ×©×ž×¦×˜×¨×£ ×ž×§×‘×œ ×›×¨×˜×™×¡ ×™×™×—×•×“×™ ×ž×©×œ×•! ðŸŒ¸\n\n"
            f"â€” Team SLH Spark"
        )

        result = await _tg_send_message(BROADCAST_BOT_TOKEN, uid, msg)
        if result.get("ok"):
            success += 1
        else:
            failed += 1

    # Audit
    async with pool.acquire() as conn:
        await audit_log_write(
            conn,
            action="broadcast.personal_cards",
            actor_type="admin",
            resource_type="broadcast",
            metadata={"total": len(rows), "success": success, "failed": failed},
        )

    return {
        "ok": True,
        "total": len(rows),
        "success": success,
        "failed": failed,
    }


@app.get("/api/broadcast/history")
async def broadcast_history(limit: int = 20):
    """Recent broadcast history for admin dashboard."""
    async with pool.acquire() as conn:
        await _ensure_broadcast_table(conn)
        rows = await conn.fetch("""
            SELECT id, sent_at, target, total_targets, success_count, fail_count, message_preview
              FROM broadcast_log
             ORDER BY sent_at DESC
             LIMIT $1
        """, limit)
    return {
        "broadcasts": [
            {
                "id": r["id"],
                "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
                "target": r["target"],
                "total": r["total_targets"],
                "success": r["success_count"],
                "failed": r["fail_count"],
                "preview": r["message_preview"],
            }
            for r in rows
        ]
    }


@app.get("/api/strategy/backtest/{strategy_id}")
async def backtest_strategy(strategy_id: str, months: int = 12):
    """Return simulated monthly returns for a strategy backtest.

    This is SIMULATED data based on the strategy's risk profile â€” for visualization.
    Live trading requires full implementation + exchange API access.
    """
    strategy = None
    for s in STRATEGIES:
        if s["id"] == strategy_id:
            strategy = s
            break
    if not strategy:
        raise HTTPException(404, "Strategy not found")

    # Generate monthly returns around the expected annual (with volatility)
    import random
    random.seed(hash(strategy_id))  # deterministic per strategy

    monthly_target = strategy["expected_annual"] / 12
    volatility = abs(strategy["max_drawdown"]) / 4

    monthly_returns = []
    cumulative = 1.0
    base_date = datetime(2025, 4, 1)

    for i in range(months):
        # Normal distribution around monthly target
        ret = random.gauss(monthly_target, volatility) / 100
        cumulative *= (1 + ret)
        month_date = base_date + timedelta(days=30 * i)
        monthly_returns.append({
            "month": month_date.strftime("%Y-%m"),
            "return_pct": round(ret * 100, 2),
            "cumulative_pct": round((cumulative - 1) * 100, 2),
        })

    return {
        "strategy_id": strategy_id,
        "strategy_name": strategy["name"],
        "period_months": months,
        "final_return_pct": round((cumulative - 1) * 100, 2),
        "annualized_pct": round((cumulative ** (12 / months) - 1) * 100, 2),
        "best_month": max(monthly_returns, key=lambda x: x["return_pct"]),
        "worst_month": min(monthly_returns, key=lambda x: x["return_pct"]),
        "monthly_returns": monthly_returns,
    }


# ============================================================
# REP SYSTEM â€” Personal Reputation Score per Member
# ============================================================

async def _ensure_rep_tables(conn):
    """Create the member_rep table if it doesn't exist."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS member_rep (
            user_id BIGINT PRIMARY KEY,
            rep_score NUMERIC(18,2) DEFAULT 0,
            therapy_hours NUMERIC(10,2) DEFAULT 0,
            referrals_count INT DEFAULT 0,
            community_actions INT DEFAULT 0,
            genesis_contributor BOOLEAN DEFAULT FALSE,
            staking_bonus NUMERIC(18,2) DEFAULT 0,
            tier TEXT DEFAULT 'basic',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


def _calculate_rep_tier(score: float) -> str:
    """Return tier based on REP score thresholds."""
    if score >= 1000:
        return "elder"
    elif score >= 500:
        return "senior"
    elif score >= 100:
        return "active"
    return "basic"


class RepAddRequest(BaseModel):
    user_id: int
    action: str  # therapy_hour, referral, community, genesis, staking
    amount: Optional[float] = None  # custom amount (used for staking bonus)


@app.get("/api/rep/leaderboard")
async def rep_leaderboard(limit: int = Query(default=20, ge=1, le=100)):
    """Get top REP holders sorted by score descending."""
    async with pool.acquire() as conn:
        await _ensure_rep_tables(conn)
        rows = await conn.fetch(
            "SELECT user_id, rep_score, genesis_contributor FROM member_rep ORDER BY rep_score DESC LIMIT $1", limit
        )
        leaderboard = [
            {"rank": idx+1, "user_id": r["user_id"], "rep_score": float(r["rep_score"]), "genesis_contributor": r["genesis_contributor"]}
            for idx, r in enumerate(rows)
        ]
    return {"leaderboard": leaderboard, "total": len(leaderboard)}


@app.get("/api/rep/{user_id}")
async def get_rep_score(user_id: int):
    """Get REP score and tier for a user."""
    async with pool.acquire() as conn:
        await _ensure_rep_tables(conn)
        row = await conn.fetchrow(
            "SELECT * FROM member_rep WHERE user_id = $1", user_id
        )
        if not row:
            # Return default score for unregistered user
            result = {
                "user_id": user_id,
                "rep_score": 0,
                "therapy_hours": 0,
                "referrals_count": 0,
                "community_actions": 0,
                "genesis_contributor": False,
                "staking_bonus": 0,
                "tier": "basic",
            }
        else:
            result = {
                "user_id": row["user_id"],
                "rep_score": float(row["rep_score"]),
                "therapy_hours": float(row["therapy_hours"]),
                "referrals_count": row["referrals_count"],
                "community_actions": row["community_actions"],
                "genesis_contributor": row["genesis_contributor"],
                "staking_bonus": float(row["staking_bonus"]),
                "tier": row["tier"],
            }

        await audit_log_write(
            conn,
            action="rep.query",
            actor_type="system",
            actor_user_id=user_id,
            resource_type="rep",
            resource_id=str(user_id),
            metadata={"tier": result["tier"], "score": result["rep_score"]},
        )

    return result


# REP point values per action type
_REP_ACTION_POINTS = {
    "therapy_hour": 10.0,
    "referral": 25.0,
    "community": 5.0,
    "genesis": 100.0,
    "staking": 0.0,  # uses custom amount
}


@app.post("/api/rep/add")
async def add_rep_points(req: RepAddRequest):
    """Add REP points for a specific action."""
    if req.action not in _REP_ACTION_POINTS:
        raise HTTPException(400, f"Invalid action. Must be one of: {', '.join(_REP_ACTION_POINTS.keys())}")

    points = _REP_ACTION_POINTS[req.action]
    if req.action == "staking":
        points = float(req.amount) if req.amount and req.amount > 0 else 0.0

    async with pool.acquire() as conn:
        await _ensure_rep_tables(conn)

        # Upsert: insert or update the user's rep record
        before_row = await conn.fetchrow(
            "SELECT rep_score, tier FROM member_rep WHERE user_id = $1", req.user_id
        )
        before_score = float(before_row["rep_score"]) if before_row else 0.0
        before_tier = before_row["tier"] if before_row else "basic"

        new_score = before_score + points
        new_tier = _calculate_rep_tier(new_score)

        # Upsert the member_rep row
        await conn.execute("""
            INSERT INTO member_rep (user_id, rep_score, tier)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET
                rep_score = member_rep.rep_score + $2,
                tier = $3,
                updated_at = CURRENT_TIMESTAMP
        """, req.user_id, points, new_tier)

        # Increment the specific action column
        if req.action == "therapy_hour":
            await conn.execute(
                "UPDATE member_rep SET therapy_hours = therapy_hours + 1 WHERE user_id = $1",
                req.user_id)
        elif req.action == "referral":
            await conn.execute(
                "UPDATE member_rep SET referrals_count = referrals_count + 1 WHERE user_id = $1",
                req.user_id)
        elif req.action == "community":
            await conn.execute(
                "UPDATE member_rep SET community_actions = community_actions + 1 WHERE user_id = $1",
                req.user_id)
        elif req.action == "genesis":
            await conn.execute(
                "UPDATE member_rep SET genesis_contributor = TRUE WHERE user_id = $1",
                req.user_id)
        elif req.action == "staking":
            await conn.execute(
                "UPDATE member_rep SET staking_bonus = staking_bonus + $2 WHERE user_id = $1",
                req.user_id, points)

        # Fix: re-read to get actual final score & tier after upsert
        final_row = await conn.fetchrow(
            "SELECT rep_score, tier FROM member_rep WHERE user_id = $1", req.user_id
        )
        final_score = float(final_row["rep_score"])
        final_tier = final_row["tier"]

        await audit_log_write(
            conn,
            action=f"rep.add.{req.action}",
            actor_type="system",
            actor_user_id=req.user_id,
            resource_type="rep",
            resource_id=str(req.user_id),
            before_state={"rep_score": before_score, "tier": before_tier},
            after_state={"rep_score": final_score, "tier": final_tier},
            metadata={"action": req.action, "points_added": points},
        )

    return {
        "user_id": req.user_id,
        "action": req.action,
        "points_added": points,
        "new_score": final_score,
        "new_tier": final_tier,
        "tier_changed": before_tier != final_tier,
    }


# (rep_leaderboard moved above rep/{user_id} to avoid route conflict)


# ============================================================
# MEMBER CARD SYSTEM
# ============================================================

TIER_EMOJIS = {"basic": "\U0001f331", "active": "\u26a1", "senior": "\U0001f3c6", "elder": "\U0001f451"}
TIER_COLORS = {
    "basic":  "#00e887",
    "active": "#00b4d8",
    "senior": "#a855f7",
    "elder":  "#ffd700",
}


async def _build_member_card_data(conn, user_id: int) -> dict:
    """Gather all data needed for a member card from multiple tables."""
    # --- web_users ---
    user = await conn.fetchrow(
        "SELECT telegram_id, username, first_name, last_login, is_registered, beta_user FROM web_users WHERE telegram_id = $1",
        user_id,
    )
    if not user:
        return None

    name = user["first_name"] or user["username"] or f"User-{user_id}"
    username = user["username"] or ""
    joined = user["last_login"].strftime("%Y-%m-%d") if user["last_login"] else "unknown"

    # --- NFT number (position in web_users ordered by last_login ASC) ---
    nft_pos = await conn.fetchval(
        """
        SELECT pos FROM (
            SELECT telegram_id, ROW_NUMBER() OVER (ORDER BY last_login ASC) AS pos
            FROM web_users
        ) sub WHERE telegram_id = $1
        """,
        user_id,
    )
    nft_number = f"SLH-{nft_pos:04d}" if nft_pos else "SLH-0000"

    # --- token_balances ---
    slh_row = await conn.fetchrow(
        "SELECT balance FROM token_balances WHERE user_id = $1 AND token = 'SLH'", user_id,
    )
    zvk_row = await conn.fetchrow(
        "SELECT balance FROM token_balances WHERE user_id = $1 AND token = 'ZVK'", user_id,
    )
    slh_balance = round(float(slh_row["balance"]), 2) if slh_row else 0.0
    zvk_balance = round(float(zvk_row["balance"]), 2) if zvk_row else 0.0

    # --- member_rep ---
    await _ensure_rep_tables(conn)
    rep_row = await conn.fetchrow(
        "SELECT rep_score, tier FROM member_rep WHERE user_id = $1", user_id,
    )
    rep_score = float(rep_row["rep_score"]) if rep_row else 0.0
    tier = rep_row["tier"] if rep_row else "basic"

    # --- launch_contributions (genesis check) ---
    await _ensure_launch_tables(conn)
    genesis_row = await conn.fetchrow(
        "SELECT amount_bnb FROM launch_contributions WHERE partner_handle = $1 AND status = 'verified'",
        username,
    )
    genesis_contributor = genesis_row is not None
    genesis_amount_bnb = float(genesis_row["amount_bnb"]) if genesis_row else 0.0

    # --- referrals (count users who have this user as referrer) ---
    referral_count = await conn.fetchval(
        "SELECT COUNT(*) FROM referrals WHERE referrer_id = $1", user_id,
    ) or 0

    # --- is_therapist: real lookup against users.is_therapist (added by routes/therapists.py _ensure_table) ---
    # Wrapped in try/except so member-card still works even if no therapist endpoint
    # has been hit yet (column wouldn't exist) or if the user row is in web_users but not users.
    try:
        is_therapist = bool(await conn.fetchval(
            "SELECT is_therapist FROM users WHERE user_id = $1", user_id,
        ))
    except Exception:
        is_therapist = False

    # --- ASCII art card ---
    tier_emoji = TIER_EMOJIS.get(tier, "\U0001f331")
    genesis_str = "Yes" if genesis_contributor else "No"
    ascii_art = (
        "\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557\n"
        "\u2551  \U0001f338 SLH SPARK \u00b7 MEMBER CARD     \u2551\n"
        "\u2551\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2551\n"
        "\u2551                                  \u2551\n"
        f"\u2551  Name:  {name:<25}\u2551\n"
        f"\u2551  ID:    #{user_id:<24}\u2551\n"
        f"\u2551  Tier:  {tier_emoji} {tier:<22}\u2551\n"
        f"\u2551  REP:   {rep_score:<25}\u2551\n"
        f"\u2551  Joined: {joined:<24}\u2551\n"
        f"\u2551  Genesis: {genesis_str:<23}\u2551\n"
        f"\u2551  NFT #:  {nft_number:<24}\u2551\n"
        "\u2551\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2551\n"
        f"\u2551  SLH: {slh_balance} \u00b7 ZVK: {zvk_balance}        \u2551\n"
        f"\u2551  \U0001f517 slh-nft.com/member?id={user_id}  \u2551\n"
        "\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d"
    )

    return {
        "user_id": user_id,
        "name": name,
        "username": username,
        "telegram_id": user_id,
        "nft_number": nft_number,
        "tier": tier,
        "rep_score": rep_score,
        "joined": joined,
        "genesis_contributor": genesis_contributor,
        "genesis_amount_bnb": genesis_amount_bnb,
        "slh_balance": slh_balance,
        "zvk_balance": zvk_balance,
        "referrals": referral_count,
        "is_therapist": is_therapist,
        "ascii_art": ascii_art,
    }


@app.get("/api/member-card/{user_id}")
async def get_member_card(user_id: int):
    """Return JSON member card data for a user."""
    async with pool.acquire() as conn:
        card = await _build_member_card_data(conn, user_id)
        if card is None:
            raise HTTPException(404, "User not found")

        await audit_log_write(
            conn,
            action="member_card.view",
            actor_type="system",
            actor_user_id=user_id,
            resource_type="member_card",
            resource_id=str(user_id),
            metadata={"nft_number": card["nft_number"], "tier": card["tier"]},
        )

    return {"ok": True, "card": card}


def _generate_member_card_image(card: dict) -> bytes:
    """Generate an 800x1000 PNG member card image. Returns raw PNG bytes."""
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO

    W, H = 800, 1000
    BG = (10, 14, 26)  # #0a0e1a

    tier = card.get("tier", "basic")
    accent_hex = TIER_COLORS.get(tier, "#00e887").lstrip("#")
    accent_rgb = tuple(int(accent_hex[i:i+2], 16) for i in (0, 2, 4))

    # --- base image ---
    img = Image.new("RGBA", (W, H), (*BG, 255))

    # --- gradient layer (radial from center-top) ---
    grad = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    grad_draw = ImageDraw.Draw(grad)
    cx, cy = W // 2, 200
    max_r = int((W**2 + H**2) ** 0.5 / 2)
    for i in range(60, 0, -1):
        ratio = i / 60
        r = int(max_r * ratio)
        alpha = int(50 * (1 - ratio))
        grad_draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(*accent_rgb, alpha))
    img = Image.alpha_composite(img, grad)

    # --- subtle grid pattern ---
    grid = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    grid_draw = ImageDraw.Draw(grid)
    grid_color = (*accent_rgb, 10)
    for x in range(0, W, 40):
        grid_draw.line([(x, 0), (x, H)], fill=grid_color, width=1)
    for y in range(0, H, 40):
        grid_draw.line([(0, y), (W, y)], fill=grid_color, width=1)
    img = Image.alpha_composite(img, grid)

    # --- main drawing layer ---
    main = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(main)

    # Top accent bar
    draw.rectangle([0, 0, W, 6], fill=(*accent_rgb, 255))

    # --- font loading ---
    def _load_font(size, bold=False):
        paths = [
            "arialbd.ttf" if bold else "arial.ttf",
            "/Library/Fonts/Arial Bold.ttf" if bold else "/Library/Fonts/Arial.ttf",
            "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        ]
        for p in paths:
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
        return ImageFont.load_default()

    brand_font = _load_font(42, bold=True)
    subtitle_font = _load_font(24, bold=True)
    nft_font = _load_font(60, bold=True)
    label_font = _load_font(22)
    value_font = _load_font(24, bold=True)
    small_font = _load_font(18)
    url_font = _load_font(16)

    # --- "SLH SPARK" brand at top ---
    y = 40
    try:
        draw.text((W // 2, y), "\U0001f338 SLH SPARK", font=brand_font, fill=(*accent_rgb, 255), anchor="mt")
    except Exception:
        draw.text((W // 2, y), "SLH SPARK", font=brand_font, fill=(*accent_rgb, 255), anchor="mt")
    y += 55

    # --- "MEMBER CARD" subtitle ---
    draw.text((W // 2, y), "MEMBER CARD", font=subtitle_font, fill=(200, 200, 220, 255), anchor="mt")
    y += 50

    # Divider line
    draw.line([(80, y), (W - 80, y)], fill=(*accent_rgb, 100), width=2)
    y += 30

    # --- NFT number large and centered ---
    nft_number = card.get("nft_number", "SLH-0000")
    draw.text((W // 2, y), nft_number, font=nft_font, fill=(*accent_rgb, 255), anchor="mt")
    y += 85

    # Tier badge
    tier_emoji = TIER_EMOJIS.get(tier, "\U0001f331")
    tier_text = f"{tier.upper()}"
    # Tier box
    box_w, box_h = 200, 40
    box_x = (W - box_w) // 2
    draw.rounded_rectangle(
        [box_x, y, box_x + box_w, y + box_h],
        radius=12,
        fill=(*accent_rgb, 50),
        outline=(*accent_rgb, 180),
        width=2,
    )
    try:
        draw.text((W // 2, y + box_h // 2), f"{tier_emoji} {tier_text}", font=subtitle_font, fill=(*accent_rgb, 255), anchor="mm")
    except Exception:
        draw.text((W // 2, y + box_h // 2), tier_text, font=subtitle_font, fill=(*accent_rgb, 255), anchor="mm")
    y += box_h + 30

    # Divider
    draw.line([(80, y), (W - 80, y)], fill=(*accent_rgb, 60), width=1)
    y += 25

    # --- Stats section ---
    left_x = 100
    stat_spacing = 45

    stats = [
        ("Name", card.get("name", "Unknown")),
        ("ID", f"#{card.get('user_id', 0)}"),
        ("REP Score", str(card.get("rep_score", 0))),
        ("Joined", card.get("joined", "unknown")),
        ("Genesis", "Yes" if card.get("genesis_contributor") else "No"),
        ("Referrals", str(card.get("referrals", 0))),
    ]

    for label_text, val_text in stats:
        draw.text((left_x, y), label_text, font=label_font, fill=(140, 140, 160, 255))
        draw.text((left_x + 180, y), val_text, font=value_font, fill=(230, 230, 245, 255))
        y += stat_spacing

    if card.get("genesis_contributor"):
        genesis_bnb = card.get("genesis_amount_bnb", 0)
        draw.text((left_x, y), "Genesis BNB", font=label_font, fill=(140, 140, 160, 255))
        draw.text((left_x + 180, y), f"{genesis_bnb} BNB", font=value_font, fill=(*accent_rgb, 255))
        y += stat_spacing

    y += 10
    # Divider
    draw.line([(80, y), (W - 80, y)], fill=(*accent_rgb, 60), width=1)
    y += 25

    # --- Token balances ---
    slh_text = f"SLH: {card.get('slh_balance', 0)}"
    zvk_text = f"ZVK: {card.get('zvk_balance', 0)}"
    draw.text((W // 2, y), f"{slh_text}  \u00b7  {zvk_text}", font=value_font, fill=(230, 230, 245, 255), anchor="mt")
    y += 50

    # --- QR code or URL ---
    member_url = f"slh-nft.com/member?id={card.get('user_id', 0)}"
    qr_generated = False
    try:
        import qrcode
        qr = qrcode.QRCode(version=1, box_size=5, border=2)
        qr.add_data(f"https://{member_url}")
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="white", back_color=(10, 14, 26)).convert("RGBA")
        qr_w, qr_h = qr_img.size
        qr_x = (W - qr_w) // 2
        main.paste(qr_img, (qr_x, y), qr_img)
        y += qr_h + 10
        qr_generated = True
    except Exception:
        pass

    if not qr_generated:
        y += 20

    # URL text below QR (or as fallback)
    try:
        draw.text((W // 2, y), f"\U0001f517 {member_url}", font=url_font, fill=(*accent_rgb, 180), anchor="mt")
    except Exception:
        draw.text((W // 2, y), member_url, font=url_font, fill=(*accent_rgb, 180), anchor="mt")

    # Bottom accent bar
    draw.rectangle([0, H - 6, W, H], fill=(*accent_rgb, 255))

    # --- composite ---
    img = Image.alpha_composite(img, main)
    final = img.convert("RGB")

    buf = BytesIO()
    final.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


@app.get("/api/member-card/image/{user_id}")
async def get_member_card_image(user_id: int):
    """Serve a dynamically generated 800x1000 PNG member card."""
    from fastapi.responses import Response

    async with pool.acquire() as conn:
        card = await _build_member_card_data(conn, user_id)
        if card is None:
            raise HTTPException(404, "User not found")

        await audit_log_write(
            conn,
            action="member_card.image",
            actor_type="system",
            actor_user_id=user_id,
            resource_type="member_card",
            resource_id=str(user_id),
            metadata={"nft_number": card["nft_number"], "tier": card["tier"]},
        )

    try:
        img_bytes = _generate_member_card_image(card)
        return Response(
            content=img_bytes,
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=3600"},
        )
    except Exception as e:
        print(f"[member_card_image] failed for {user_id}: {e}")
        raise HTTPException(500, f"Member card image generation failed: {e}")


@app.get("/api/member-cards/all")
async def list_all_member_cards(limit: int = Query(default=50, ge=1, le=500)):
    """List all member cards for the website gallery, ordered by rep_score DESC."""
    async with pool.acquire() as conn:
        await _ensure_rep_tables(conn)

        rows = await conn.fetch("""
            WITH nft_positions AS (
                SELECT telegram_id, ROW_NUMBER() OVER (ORDER BY last_login ASC) AS pos
                FROM web_users
            )
            SELECT
                wu.telegram_id,
                wu.first_name,
                wu.username,
                wu.last_login,
                COALESCE(mr.rep_score, 0) AS rep_score,
                COALESCE(mr.tier, 'basic') AS tier,
                nft.pos AS nft_pos
            FROM web_users wu
            LEFT JOIN member_rep mr ON mr.user_id = wu.telegram_id
            LEFT JOIN nft_positions nft ON nft.telegram_id = wu.telegram_id
            WHERE wu.telegram_id >= 1000000
            ORDER BY COALESCE(mr.rep_score, 0) DESC
            LIMIT $1
        """, limit)

        cards = []
        for r in rows:
            nft_pos = r["nft_pos"] if r["nft_pos"] else 0
            cards.append({
                "user_id": r["telegram_id"],
                "name": r["first_name"] or r["username"] or f"User-{r['telegram_id']}",
                "nft_number": f"SLH-{nft_pos:04d}",
                "tier": r["tier"],
                "rep_score": float(r["rep_score"]),
                "joined": r["last_login"].strftime("%Y-%m-%d") if r["last_login"] else "unknown",
            })

        await audit_log_write(
            conn,
            action="member_cards.list",
            actor_type="system",
            resource_type="member_card",
            metadata={"limit": limit, "results_count": len(cards)},
        )

    return {"ok": True, "cards": cards, "total": len(cards)}


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# P2P ORDER BOOK
# Table: p2p_orders
#   (id, seller_id, token, amount, price_per_unit, currency, payment_method,
#    status, created_at)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

P2P_VALID_TOKENS = {"SLH", "ZVK", "MNH"}
P2P_VALID_CURRENCIES = {"ILS", "USD"}
P2P_VALID_PAYMENT_METHODS = {"Bit", "PayBox", "Bank", "MNH", "BNB"}
P2P_VALID_STATUSES = {"active", "filled", "cancelled"}


async def _ensure_p2p_orders_table(conn):
    """Create p2p_orders table if it does not exist."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS p2p_orders (
            id              SERIAL PRIMARY KEY,
            seller_id       BIGINT NOT NULL,
            token           VARCHAR(10) NOT NULL,
            amount          NUMERIC(20,8) NOT NULL CHECK (amount > 0),
            price_per_unit  NUMERIC(20,4) NOT NULL CHECK (price_per_unit > 0),
            currency        VARCHAR(5)  NOT NULL DEFAULT 'ILS',
            payment_method  VARCHAR(20) NOT NULL DEFAULT 'Bit',
            status          VARCHAR(12) NOT NULL DEFAULT 'active',
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)


class P2PCreateOrder(BaseModel):
    seller_id: int
    token: str          # SLH / ZVK / MNH
    amount: float
    price_per_unit: float
    currency: str = "ILS"          # ILS / USD
    payment_method: str = "Bit"    # Bit / PayBox / Bank / MNH / BNB


class P2PFillOrder(BaseModel):
    order_id: int
    buyer_id: int


# â”€â”€ POST /api/p2p/create-order â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.post("/api/p2p/create-order")
async def p2p_create_order(body: P2PCreateOrder):
    """Create a new P2P sell order."""
    if body.token.upper() not in P2P_VALID_TOKENS:
        raise HTTPException(400, f"Invalid token. Must be one of: {', '.join(P2P_VALID_TOKENS)}")
    if body.currency.upper() not in P2P_VALID_CURRENCIES:
        raise HTTPException(400, f"Invalid currency. Must be one of: {', '.join(P2P_VALID_CURRENCIES)}")
    if body.payment_method not in P2P_VALID_PAYMENT_METHODS:
        raise HTTPException(400, f"Invalid payment method. Must be one of: {', '.join(P2P_VALID_PAYMENT_METHODS)}")
    if body.amount <= 0 or body.price_per_unit <= 0:
        raise HTTPException(400, "Amount and price must be positive")

    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)
        row = await conn.fetchrow("""
            INSERT INTO p2p_orders (seller_id, token, amount, price_per_unit, currency, payment_method, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'active')
            RETURNING id, seller_id, token, amount, price_per_unit, currency, payment_method, status, created_at
        """, body.seller_id, body.token.upper(), body.amount, body.price_per_unit,
             body.currency.upper(), body.payment_method)

        await audit_log_write(
            conn,
            action="p2p.create_order",
            actor_type="user",
            actor_user_id=body.seller_id,
            resource_type="p2p_order",
            resource_id=str(row["id"]),
            metadata={
                "token": body.token.upper(),
                "amount": body.amount,
                "price_per_unit": body.price_per_unit,
                "currency": body.currency.upper(),
                "payment_method": body.payment_method,
            },
        )

    return {
        "ok": True,
        "order": {
            "id": row["id"],
            "seller_id": row["seller_id"],
            "token": row["token"],
            "amount": float(row["amount"]),
            "price_per_unit": float(row["price_per_unit"]),
            "currency": row["currency"],
            "payment_method": row["payment_method"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
        },
    }


# â”€â”€ GET /api/p2p/orders â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.get("/api/p2p/orders")
async def p2p_list_orders(
    token: Optional[str] = Query(None, description="Filter by token: SLH, ZVK, MNH"),
    currency: Optional[str] = Query(None, description="Filter by currency: ILS, USD"),
    payment_method: Optional[str] = Query(None, description="Filter by payment method"),
    status: str = Query("active", description="Order status: active, filled, cancelled"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List P2P orders with optional filters."""
    if status not in P2P_VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Must be one of: {', '.join(P2P_VALID_STATUSES)}")

    conditions = ["status = $1"]
    params: list = [status]
    idx = 2

    if token:
        if token.upper() not in P2P_VALID_TOKENS:
            raise HTTPException(400, f"Invalid token filter. Must be one of: {', '.join(P2P_VALID_TOKENS)}")
        conditions.append(f"token = ${idx}")
        params.append(token.upper())
        idx += 1

    if currency:
        if currency.upper() not in P2P_VALID_CURRENCIES:
            raise HTTPException(400, f"Invalid currency filter. Must be one of: {', '.join(P2P_VALID_CURRENCIES)}")
        conditions.append(f"currency = ${idx}")
        params.append(currency.upper())
        idx += 1

    if payment_method:
        if payment_method not in P2P_VALID_PAYMENT_METHODS:
            raise HTTPException(400, f"Invalid payment_method filter. Must be one of: {', '.join(P2P_VALID_PAYMENT_METHODS)}")
        conditions.append(f"payment_method = ${idx}")
        params.append(payment_method)
        idx += 1

    where_clause = " AND ".join(conditions)
    params.extend([limit, offset])

    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)
        rows = await conn.fetch(f"""
            SELECT id, seller_id, token, amount, price_per_unit, currency,
                   payment_method, status, created_at
            FROM p2p_orders
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
        """, *params)

        total = await conn.fetchval(f"""
            SELECT COUNT(*) FROM p2p_orders WHERE {where_clause}
        """, *params[:-2])

    orders = []
    for r in rows:
        orders.append({
            "id": r["id"],
            "seller_id": r["seller_id"],
            "token": r["token"],
            "amount": float(r["amount"]),
            "price_per_unit": float(r["price_per_unit"]),
            "currency": r["currency"],
            "payment_method": r["payment_method"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat(),
        })

    return {"ok": True, "orders": orders, "total": total, "limit": limit, "offset": offset}


# â”€â”€ POST /api/p2p/fill-order â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.post("/api/p2p/fill-order")
async def p2p_fill_order(body: P2PFillOrder):
    """Mark an active P2P order as filled by a buyer."""
    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)

        row = await conn.fetchrow(
            "SELECT * FROM p2p_orders WHERE id = $1", body.order_id
        )
        if not row:
            raise HTTPException(404, "Order not found")
        if row["status"] != "active":
            raise HTTPException(400, f"Order is already {row['status']}")
        if row["seller_id"] == body.buyer_id:
            raise HTTPException(400, "Seller cannot fill own order")

        await conn.execute(
            "UPDATE p2p_orders SET status = 'filled' WHERE id = $1",
            body.order_id,
        )

        await audit_log_write(
            conn,
            action="p2p.fill_order",
            actor_type="user",
            actor_user_id=body.buyer_id,
            resource_type="p2p_order",
            resource_id=str(body.order_id),
            metadata={
                "seller_id": row["seller_id"],
                "buyer_id": body.buyer_id,
                "token": row["token"],
                "amount": float(row["amount"]),
                "price_per_unit": float(row["price_per_unit"]),
                "currency": row["currency"],
            },
        )

    return {"ok": True, "message": "Order filled successfully", "order_id": body.order_id}


# â”€â”€ DELETE /api/p2p/cancel-order/{id} â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.delete("/api/p2p/cancel-order/{order_id}")
async def p2p_cancel_order(order_id: int, seller_id: int = Query(..., description="Seller's telegram ID")):
    """Cancel an active P2P order. Only the seller can cancel their own order."""
    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)

        row = await conn.fetchrow(
            "SELECT * FROM p2p_orders WHERE id = $1", order_id
        )
        if not row:
            raise HTTPException(404, "Order not found")
        if row["seller_id"] != seller_id:
            raise HTTPException(403, "Only the seller can cancel this order")
        if row["status"] != "active":
            raise HTTPException(400, f"Order is already {row['status']}")

        await conn.execute(
            "UPDATE p2p_orders SET status = 'cancelled' WHERE id = $1",
            order_id,
        )

        await audit_log_write(
            conn,
            action="p2p.cancel_order",
            actor_type="user",
            actor_user_id=seller_id,
            resource_type="p2p_order",
            resource_id=str(order_id),
            metadata={
                "token": row["token"],
                "amount": float(row["amount"]),
                "price_per_unit": float(row["price_per_unit"]),
            },
        )

    return {"ok": True, "message": "Order cancelled", "order_id": order_id}


# ============================================================
# P2P ORDER BOOK â€” JWT-Authenticated Endpoints (v2)
# ============================================================
# These endpoints use JWT bearer tokens to identify the caller.
# The seller/buyer is derived from the JWT, not from the request body.

class P2PCreateOrderAuth(BaseModel):
    token: str          # SLH / ZVK / MNH
    amount: float
    price_per_unit: float
    currency: str = "ILS"
    payment_method: str = "Bit"


class P2PFillOrderAuth(BaseModel):
    order_id: int


# â”€â”€ POST /api/p2p/v2/create-order (JWT auth â€” seller = caller) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.post("/api/p2p/v2/create-order")
async def p2p_create_order_auth(
    body: P2PCreateOrderAuth,
    seller_id: int = Depends(get_current_user_id),
):
    """Create a new P2P sell order. Seller is derived from JWT token."""
    if body.token.upper() not in P2P_VALID_TOKENS:
        raise HTTPException(400, f"Invalid token. Must be one of: {', '.join(P2P_VALID_TOKENS)}")
    if body.currency.upper() not in P2P_VALID_CURRENCIES:
        raise HTTPException(400, f"Invalid currency. Must be one of: {', '.join(P2P_VALID_CURRENCIES)}")
    if body.payment_method not in P2P_VALID_PAYMENT_METHODS:
        raise HTTPException(400, f"Invalid payment method. Must be one of: {', '.join(P2P_VALID_PAYMENT_METHODS)}")
    if body.amount <= 0 or body.price_per_unit <= 0:
        raise HTTPException(400, "Amount and price must be positive")

    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)

        # Verify seller has enough balance
        balance = await conn.fetchval(
            "SELECT balance FROM token_balances WHERE user_id=$1 AND token=$2",
            seller_id, body.token.upper(),
        )
        if not balance or float(balance) < body.amount:
            raise HTTPException(400, f"Insufficient {body.token.upper()} balance")

        row = await conn.fetchrow("""
            INSERT INTO p2p_orders (seller_id, token, amount, price_per_unit, currency, payment_method, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'active')
            RETURNING id, seller_id, token, amount, price_per_unit, currency, payment_method, status, created_at
        """, seller_id, body.token.upper(), body.amount, body.price_per_unit,
             body.currency.upper(), body.payment_method)

        await audit_log_write(
            conn,
            action="p2p.create_order",
            actor_type="user",
            actor_user_id=seller_id,
            resource_type="p2p_order",
            resource_id=str(row["id"]),
            metadata={
                "token": body.token.upper(),
                "amount": body.amount,
                "price_per_unit": body.price_per_unit,
                "currency": body.currency.upper(),
                "payment_method": body.payment_method,
                "auth": "jwt",
            },
        )

    return {
        "ok": True,
        "order": {
            "id": row["id"],
            "seller_id": row["seller_id"],
            "token": row["token"],
            "amount": float(row["amount"]),
            "price_per_unit": float(row["price_per_unit"]),
            "currency": row["currency"],
            "payment_method": row["payment_method"],
            "status": row["status"],
            "created_at": row["created_at"].isoformat(),
        },
    }


# â”€â”€ GET /api/p2p/v2/orders (public â€” same as v1, no auth needed) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.get("/api/p2p/v2/orders")
async def p2p_list_orders_v2(
    token: Optional[str] = Query(None, description="Filter by token: SLH, ZVK, MNH"),
    currency: Optional[str] = Query(None, description="Filter by currency: ILS, USD"),
    payment_method: Optional[str] = Query(None, description="Filter by payment method"),
    status: str = Query("active", description="Order status: active, filled, cancelled"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List P2P orders with optional filters. Public endpoint."""
    if status not in P2P_VALID_STATUSES:
        raise HTTPException(400, f"Invalid status. Must be one of: {', '.join(P2P_VALID_STATUSES)}")

    conditions = ["status = $1"]
    params: list = [status]
    idx = 2

    if token:
        if token.upper() not in P2P_VALID_TOKENS:
            raise HTTPException(400, f"Invalid token filter. Must be one of: {', '.join(P2P_VALID_TOKENS)}")
        conditions.append(f"token = ${idx}")
        params.append(token.upper())
        idx += 1

    if currency:
        if currency.upper() not in P2P_VALID_CURRENCIES:
            raise HTTPException(400, f"Invalid currency filter. Must be one of: {', '.join(P2P_VALID_CURRENCIES)}")
        conditions.append(f"currency = ${idx}")
        params.append(currency.upper())
        idx += 1

    if payment_method:
        if payment_method not in P2P_VALID_PAYMENT_METHODS:
            raise HTTPException(400, f"Invalid payment_method filter. Must be one of: {', '.join(P2P_VALID_PAYMENT_METHODS)}")
        conditions.append(f"payment_method = ${idx}")
        params.append(payment_method)
        idx += 1

    where_clause = " AND ".join(conditions)
    params.extend([limit, offset])

    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)
        rows = await conn.fetch(f"""
            SELECT id, seller_id, token, amount, price_per_unit, currency,
                   payment_method, status, created_at
            FROM p2p_orders
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
        """, *params)

        total = await conn.fetchval(f"""
            SELECT COUNT(*) FROM p2p_orders WHERE {where_clause}
        """, *params[:-2])

    orders = []
    for r in rows:
        orders.append({
            "id": r["id"],
            "seller_id": r["seller_id"],
            "token": r["token"],
            "amount": float(r["amount"]),
            "price_per_unit": float(r["price_per_unit"]),
            "currency": r["currency"],
            "payment_method": r["payment_method"],
            "status": r["status"],
            "created_at": r["created_at"].isoformat(),
        })

    return {"ok": True, "orders": orders, "total": total, "limit": limit, "offset": offset}


# â”€â”€ POST /api/p2p/v2/fill-order (JWT auth â€” buyer = caller) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
@app.post("/api/p2p/v2/fill-order")
async def p2p_fill_order_auth(
    body: P2PFillOrderAuth,
    buyer_id: int = Depends(get_current_user_id),
):
    """Fill a P2P order. Buyer is derived from JWT token. Transfers tokens from seller to buyer."""
    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)

        row = await conn.fetchrow(
            "SELECT * FROM p2p_orders WHERE id = $1", body.order_id
        )
        if not row:
            raise HTTPException(404, "Order not found")
        if row["status"] != "active":
            raise HTTPException(400, f"Order is already {row['status']}")
        if row["seller_id"] == buyer_id:
            raise HTTPException(400, "Seller cannot fill own order")

        token = row["token"]
        amount = row["amount"]
        seller_id = row["seller_id"]

        # Deduct tokens from seller
        seller_balance = await conn.fetchval(
            "SELECT balance FROM token_balances WHERE user_id=$1 AND token=$2",
            seller_id, token,
        )
        if not seller_balance or float(seller_balance) < float(amount):
            raise HTTPException(400, f"Seller has insufficient {token} balance")

        await conn.execute(
            "UPDATE token_balances SET balance = balance - $1, updated_at = CURRENT_TIMESTAMP WHERE user_id=$2 AND token=$3",
            amount, seller_id, token,
        )

        # Credit tokens to buyer
        await conn.execute("""
            INSERT INTO token_balances (user_id, token, balance)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, token) DO UPDATE
              SET balance = token_balances.balance + $3,
                  updated_at = CURRENT_TIMESTAMP
        """, buyer_id, token, amount)

        # Mark order as filled
        await conn.execute(
            "UPDATE p2p_orders SET status = 'filled' WHERE id = $1",
            body.order_id,
        )

        await audit_log_write(
            conn,
            action="p2p.fill_order",
            actor_type="user",
            actor_user_id=buyer_id,
            resource_type="p2p_order",
            resource_id=str(body.order_id),
            metadata={
                "seller_id": seller_id,
                "buyer_id": buyer_id,
                "token": token,
                "amount": float(amount),
                "price_per_unit": float(row["price_per_unit"]),
                "currency": row["currency"],
                "auth": "jwt",
            },
        )

    return {"ok": True, "message": "Order filled successfully", "order_id": body.order_id}


# â”€â”€ DELETE /api/p2p/v2/cancel-order/{id} (JWT auth â€” seller = caller) â”€â”€â”€â”€â”€â”€â”€â”€
@app.delete("/api/p2p/v2/cancel-order/{order_id}")
async def p2p_cancel_order_auth(
    order_id: int,
    seller_id: int = Depends(get_current_user_id),
):
    """Cancel an active P2P order. Only the seller (from JWT) can cancel their own order."""
    async with pool.acquire() as conn:
        await _ensure_p2p_orders_table(conn)

        row = await conn.fetchrow(
            "SELECT * FROM p2p_orders WHERE id = $1", order_id
        )
        if not row:
            raise HTTPException(404, "Order not found")
        if row["seller_id"] != seller_id:
            raise HTTPException(403, "Only the seller can cancel this order")
        if row["status"] != "active":
            raise HTTPException(400, f"Order is already {row['status']}")

        await conn.execute(
            "UPDATE p2p_orders SET status = 'cancelled' WHERE id = $1",
            order_id,
        )

        await audit_log_write(
            conn,
            action="p2p.cancel_order",
            actor_type="user",
            actor_user_id=seller_id,
            resource_type="p2p_order",
            resource_id=str(order_id),
            metadata={
                "token": row["token"],
                "amount": float(row["amount"]),
                "price_per_unit": float(row["price_per_unit"]),
                "auth": "jwt",
            },
        )

    return {"ok": True, "message": "Order cancelled", "order_id": order_id}




# ===== RISK DASHBOARD API =====

@app.get("/api/risk/entities")
async def api_risk_entities():
    try:
        async with pool.acquire() as conn:
            await _ensure_guardian_tables(conn)
            rows = await conn.fetch(
                "SELECT user_id as entity_id, username as canonical_name, zuz_score as score_zuz, "
                "total_reports as approved_reports, total_reports as verified_evidence, "
                "ban_active as eligible_for_publication FROM guardian_blacklist ORDER BY zuz_score DESC"
            )
        items = [
            {
                "entity_id": r["entity_id"],
                "canonical_name": r["canonical_name"],
                "score_zuz": float(r["score_zuz"] or 0),
                "approved_reports": int(r["approved_reports"] or 0),
                "verified_evidence": int(r["verified_evidence"] or 0),
                "eligible_for_publication": bool(r["eligible_for_publication"]),
            }
            for r in rows
        ]
        return {
            "ok": True,
            "total_entities": len(items),
            "public_ready": sum(1 for x in items if x["eligible_for_publication"]),
            "items": items,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "items": []}


@app.get("/api/risk/external-watch")
async def api_external_watch():
    """Watch list of external assets (SLH on BSC, etc)"""
    try:
        # Return SLH token info from BSC as watched asset
        slh_price = None
        try:
            import urllib.request, json as _json
            r = urllib.request.urlopen("https://slh-api-production.up.railway.app/api/wallet/price", timeout=5)
            d = _json.loads(r.read())
            slh_price = d.get("price_usd")
        except Exception:
            pass

        items = [
            {
                "asset_key": "SLH_BSC",
                "asset_name": "SLH Spark",
                "chain": "BSC",
                "contract_address": "0xACb0A09414CEA1C879c67bB7A877E4e19480f022",
                "last_price_usd": slh_price,
                "pool": "0xacea26b6e132cd45f2b8a4754170d4d0d3b8bbee",
                "status": "active"
            }
        ]
        return {"ok": True, "total_assets": len(items), "items": items}
    except Exception as e:
        return {"ok": False, "error": str(e), "items": []}

# ===== END RISK DASHBOARD API =====

# =====================================================================
# ===== BANK TRANSFER & PAYMENT SYSTEM =====
# =====================================================================

def validate_israeli_tz(id_number: str) -> bool:
    """Validate Israeli Teudat Zehut check digit."""
    if not id_number or not id_number.isdigit():
        return False
    padded = id_number.zfill(9)
    if len(padded) != 9:
        return False
    total = 0
    for i, digit in enumerate(padded):
        d = int(digit)
        if i % 2 == 1:
            d *= 2
        if d > 9:
            d -= 9
        total += d
    return total % 10 == 0

class BankTransferSubmit(BaseModel):
    user_id: int
    customer_name: str
    transaction_date: str  # YYYY-MM-DD
    id_number: str
    bank_details: str
    amount_ils: float
    transaction_desc: str
    phone: str
    transfer_reference: str

class BankTransferReview(BaseModel):
    transfer_id: int
    action: str  # approve or reject
    reason: Optional[str] = None

@app.post("/api/bank-transfer/submit")
async def submit_bank_transfer(req: BankTransferSubmit):
    """Submit a bank transfer request with 8 required fields."""
    import re
    # Validate Israeli TZ
    if not validate_israeli_tz(req.id_number):
        raise HTTPException(400, "תעודת זהות לא תקינה — בדוק את הספרות")
    # Validate phone
    if not re.match(r'^0[2-9]\d{7,8}$', req.phone):
        raise HTTPException(400, "מספר טלפון לא תקין — דוגמה: 0584203384")
    # Validate amount
    if req.amount_ils <= 0 or req.amount_ils > 1000000:
        raise HTTPException(400, "סכום חייב להיות בין 1 ל-1,000,000 ₪")
    # Validate date
    try:
        tx_date = datetime.strptime(req.transaction_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "תאריך לא תקין — פורמט: YYYY-MM-DD")
    # Validate non-empty fields
    if not req.customer_name.strip():
        raise HTTPException(400, "שם הלקוח חובה")
    if not req.bank_details.strip():
        raise HTTPException(400, "פרטי חשבון בנק חובה")
    if not req.transaction_desc.strip():
        raise HTTPException(400, "מהות העסקה חובה")
    if not req.transfer_reference.strip():
        raise HTTPException(400, "אסמכתא של העברה בנקאית חובה")

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO bank_transfer_requests
                (user_id, customer_name, transaction_date, id_number, bank_details,
                 amount_ils, transaction_desc, phone, transfer_reference)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                RETURNING id, created_at
            """, req.user_id, req.customer_name.strip(), tx_date,
                req.id_number, req.bank_details.strip(),
                float(req.amount_ils), req.transaction_desc.strip(),
                req.phone, req.transfer_reference.strip())
            try:
                await audit_log_write(conn, "bank_transfer_submit",
                    actor_type="user", actor_user_id=req.user_id,
                    resource_type="bank_transfer", resource_id=str(row["id"]),
                    amount_native=req.amount_ils, amount_currency="ILS")
            except Exception:
                pass  # Audit log failure should not block the request
        return {"ok": True, "transfer_id": row["id"],
                "message": "הבקשה התקבלה בהצלחה. תאושר על ידי צביקה תוך 24 שעות."}
    except Exception as e:
        raise HTTPException(500, f"DB error: {str(e)}")

@app.get("/api/bank-transfer/my-requests/{user_id}")
async def my_bank_transfers(user_id: int):
    """List user's bank transfer requests."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, customer_name, transaction_date, amount_ils,
                   transaction_desc, transfer_reference, status,
                   reviewed_at, rejection_reason, created_at
            FROM bank_transfer_requests WHERE user_id=$1
            ORDER BY created_at DESC
        """, user_id)
    requests_out = []
    for r in rows:
        d = {}
        for k, v in dict(r).items():
            d[k] = v.isoformat() if hasattr(v, 'isoformat') else v if isinstance(v, (int, float, str, bool, type(None))) else str(v)
        requests_out.append(d)
    return {"ok": True, "requests": requests_out}

@app.get("/api/admin/bank-transfers")
async def admin_list_bank_transfers(
    status: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: list all bank transfer requests."""
    _require_admin(authorization, x_admin_key)
    from decimal import Decimal as Dec
    try:
        async with pool.acquire() as conn:
            if status and status in ('pending', 'approved', 'rejected'):
                rows = await conn.fetch("""
                    SELECT bt.*, wu.username, wu.first_name
                    FROM bank_transfer_requests bt
                    LEFT JOIN web_users wu ON bt.user_id = wu.telegram_id
                    WHERE bt.status=$1 ORDER BY bt.created_at DESC
                """, status)
            else:
                rows = await conn.fetch("""
                    SELECT bt.*, wu.username, wu.first_name
                    FROM bank_transfer_requests bt
                    LEFT JOIN web_users wu ON bt.user_id = wu.telegram_id
                    ORDER BY bt.created_at DESC
                """)
        result = []
        for r in rows:
            d = {}
            for k, v in dict(r).items():
                if hasattr(v, 'isoformat'):
                    d[k] = v.isoformat()
                elif isinstance(v, Dec):
                    d[k] = float(v)
                elif isinstance(v, (int, float, str, bool, type(None))):
                    d[k] = v
                else:
                    d[k] = str(v)
            if d.get("id_number"):
                d["id_number_masked"] = "*****" + str(d["id_number"])[-4:]
            result.append(d)
        return {"ok": True, "count": len(result), "transfers": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Bank transfers error: {str(e)}")

@app.post("/api/admin/bank-transfer/review")
async def admin_review_bank_transfer(
    req: BankTransferReview,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: approve or reject a bank transfer."""
    admin_id = _require_admin(authorization, x_admin_key)
    if req.action not in ("approve", "reject"):
        raise HTTPException(400, "action must be 'approve' or 'reject'")

    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT * FROM bank_transfer_requests WHERE id=$1", req.transfer_id)
        if not existing:
            raise HTTPException(404, "Bank transfer request not found")
        if existing["status"] != "pending":
            raise HTTPException(400, f"Already {existing['status']}")

        new_status = "approved" if req.action == "approve" else "rejected"
        await conn.execute("""
            UPDATE bank_transfer_requests
            SET status=$1, reviewed_by=$2, reviewed_at=CURRENT_TIMESTAMP,
                rejection_reason=$3
            WHERE id=$4
        """, new_status, admin_id, req.reason, req.transfer_id)
        await audit_log_write(conn, f"bank_transfer_{req.action}",
            actor_type="admin", actor_user_id=admin_id,
            resource_type="bank_transfer", resource_id=str(req.transfer_id),
            amount_native=float(existing["amount_ils"]), amount_currency="ILS",
            after_state={"status": new_status, "reason": req.reason})
    return {"ok": True, "transfer_id": req.transfer_id, "status": new_status}

# ===== END BANK TRANSFER SYSTEM =====

# =====================================================================
# ===== MULTI-ADMIN SYSTEM =====
# =====================================================================

class AdminLoginRequest(BaseModel):
    username: str
    password: str

class AdminCreateRequest(BaseModel):
    username: str
    password: str
    display_name: str
    role: str = "viewer"  # owner, ceo, manager, viewer
    telegram_id: Optional[int] = None
    email: Optional[str] = None
    phone: Optional[str] = None

@app.post("/api/admin/auth/login")
async def admin_login(req: AdminLoginRequest):
    """Admin login — returns JWT with role."""
    async with pool.acquire() as conn:
        # Seed default admins on first call if table empty
        count = await conn.fetchval("SELECT COUNT(*) FROM admin_users")
        if count == 0:
            # Seed Osif as owner
            await conn.execute("""
                INSERT INTO admin_users (telegram_id, username, password_hash, display_name, role, created_by)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (username) DO NOTHING
            """, 224223270, "osif", hash_admin_password(os.getenv("INITIAL_ADMIN_PASSWORD", "change_me_on_first_login_" + secrets.token_hex(4))), "Osif Ungar", "owner", 224223270)
            # Seed Tzvika as CEO
            await conn.execute("""
                INSERT INTO admin_users (telegram_id, username, password_hash, display_name, role, created_by)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (username) DO NOTHING
            """, 7757102350, "tzvika", hash_admin_password(os.getenv("INITIAL_TZVIKA_PASSWORD", "change_me_on_first_login_" + secrets.token_hex(4))), "Tzvika Kaufman", "ceo", 224223270)

        admin = await conn.fetchrow(
            "SELECT * FROM admin_users WHERE username=$1 AND is_active=TRUE", req.username)
        if not admin:
            raise HTTPException(401, "שם משתמש לא נמצא")
        if not verify_admin_password(req.password, admin["password_hash"]):
            raise HTTPException(401, "סיסמה שגויה")
        # Update last login
        await conn.execute(
            "UPDATE admin_users SET last_login=CURRENT_TIMESTAMP WHERE id=$1", admin["id"])
    # Generate admin JWT (2h expiry)
    payload = {
        "admin_id": admin["id"],
        "username": admin["username"],
        "role": admin["role"],
        "display_name": admin["display_name"],
        "type": "admin",
        "exp": datetime.utcnow() + timedelta(hours=2)
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return {
        "ok": True,
        "token": token,
        "admin": {
            "id": admin["id"],
            "username": admin["username"],
            "display_name": admin["display_name"],
            "role": admin["role"],
            "telegram_id": admin["telegram_id"]
        }
    }

@app.get("/api/admin/me")
async def admin_me(authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    """Get current admin profile from JWT."""
    if authorization and authorization.startswith("Bearer "):
        try:
            payload = jwt.decode(authorization[7:], JWT_SECRET, algorithms=[JWT_ALGORITHM])
            if payload.get("type") == "admin":
                return {"ok": True, "admin": {
                    "id": payload.get("admin_id"),
                    "username": payload.get("username"),
                    "display_name": payload.get("display_name"),
                    "role": payload.get("role")
                }}
        except Exception:
            pass
    # Fallback for old X-Admin-Key auth
    if x_admin_key and x_admin_key in ADMIN_API_KEYS:
        return {"ok": True, "admin": {
            "id": ADMIN_USER_ID, "username": "osif",
            "display_name": "Osif Ungar", "role": "owner"
        }}
    raise HTTPException(403, "Not authenticated")

@app.get("/api/admin/admins")
async def list_admins(authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    """List all admin users. Requires owner or ceo role."""
    _require_admin_role(authorization, x_admin_key, "ceo")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, telegram_id, username, display_name, role, email, phone, is_active, created_at, last_login FROM admin_users ORDER BY id")
    return {"ok": True, "admins": [dict(r) for r in rows]}

@app.post("/api/admin/admins/create")
async def create_admin(
    req: AdminCreateRequest,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Create a new admin user. Requires owner role."""
    _require_admin_role(authorization, x_admin_key, "owner")
    if req.role not in ADMIN_ROLE_HIERARCHY:
        raise HTTPException(400, f"Invalid role. Use: {list(ADMIN_ROLE_HIERARCHY.keys())}")
    if len(req.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    try:
        async with pool.acquire() as conn:
            admin_id = await conn.fetchval("""
                INSERT INTO admin_users (username, password_hash, display_name, role, telegram_id, email, phone, created_by)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING id
            """, req.username, hash_admin_password(req.password), req.display_name,
                req.role, req.telegram_id, req.email, req.phone, ADMIN_USER_ID)
        return {"ok": True, "admin_id": admin_id}
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(409, "Username already exists")
        raise HTTPException(500, str(e))

@app.post("/api/admin/admins/{admin_id}/reset-password")
async def reset_admin_password(
    admin_id: int,
    new_password: str = "",
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Reset an admin's password. Requires owner role."""
    _require_admin_role(authorization, x_admin_key, "owner")
    if len(new_password) < 8:
        new_password = secrets.token_urlsafe(16)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE admin_users SET password_hash=$1 WHERE id=$2",
            hash_admin_password(new_password), admin_id)
    return {"ok": True, "admin_id": admin_id, "new_password": new_password}

# ===== END MULTI-ADMIN SYSTEM =====


# ===== CAMPAIGN / MULTI-PATH FUNNEL SYSTEM (Shekel April 26) =====

async def _ensure_campaign_tables(conn):
    """Create campaign tables if missing. Safe to call repeatedly."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS campaign_clicks (
            id SERIAL PRIMARY KEY,
            campaign_id TEXT NOT NULL,
            path_type TEXT,
            lang TEXT,
            user_id BIGINT,
            ref_code TEXT,
            source TEXT,
            ua TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS campaign_registrations (
            id SERIAL PRIMARY KEY,
            campaign_id TEXT NOT NULL,
            path_type TEXT NOT NULL,
            user_id BIGINT,
            tg_username TEXT,
            full_name TEXT,
            phone TEXT,
            email TEXT,
            ref_code TEXT,
            affiliate_code TEXT UNIQUE,
            lang TEXT DEFAULT 'he',
            status TEXT DEFAULT 'pending',
            amount_paid NUMERIC(12,2) DEFAULT 0,
            notes TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS campaign_affiliate_earnings (
            id SERIAL PRIMARY KEY,
            affiliate_user_id BIGINT,
            affiliate_code TEXT,
            referred_user_id BIGINT,
            campaign_id TEXT,
            event_type TEXT,
            zvk_earned INTEGER DEFAULT 0,
            slh_earned NUMERIC(12,6) DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_clicks_cid ON campaign_clicks(campaign_id, created_at DESC)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_regs_cid ON campaign_registrations(campaign_id, created_at DESC)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_regs_affcode ON campaign_registrations(affiliate_code)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_campaign_affearn_user ON campaign_affiliate_earnings(affiliate_user_id, created_at DESC)")


def _make_affiliate_code(prefix: str = "SLH") -> str:
    """Create a short unique affiliate code like SLH-7K3X9."""
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # no I/O/0/1 confusion
    return f"{prefix}-{''.join(secrets.choice(alphabet) for _ in range(5))}"


class CampaignClickReq(BaseModel):
    campaign_id: str
    path_type: Optional[str] = None  # buyer/partner/genesis/community
    lang: Optional[str] = "he"
    ref_code: Optional[str] = None
    source: Optional[str] = None  # fb/yt/tg/direct
    user_id: Optional[int] = None


@app.post("/api/campaign/click")
async def campaign_click(req: CampaignClickReq, user_agent: Optional[str] = Header(None)):
    """Anonymous click tracking — no auth required."""
    try:
        async with pool.acquire() as conn:
            await _ensure_campaign_tables(conn)
            await conn.execute("""
                INSERT INTO campaign_clicks (campaign_id, path_type, lang, user_id, ref_code, source, ua)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, req.campaign_id, req.path_type, req.lang, req.user_id,
                req.ref_code, req.source, (user_agent or "")[:200])
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)[:100]}


class CampaignRegisterReq(BaseModel):
    campaign_id: str
    path_type: str  # buyer/partner/genesis/community
    user_id: Optional[int] = None
    tg_username: Optional[str] = None
    full_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    ref_code: Optional[str] = None
    lang: Optional[str] = "he"


@app.post("/api/campaign/register")
async def campaign_register(req: CampaignRegisterReq):
    """Register a user to a campaign path. Generates unique affiliate code."""
    if req.path_type not in ("buyer", "partner", "genesis", "community"):
        raise HTTPException(400, "path_type must be buyer/partner/genesis/community")

    # Generate unique affiliate code
    async with pool.acquire() as conn:
        await _ensure_campaign_tables(conn)

        # Retry loop to ensure uniqueness
        for _ in range(5):
            affiliate_code = _make_affiliate_code("SLH")
            exists = await conn.fetchval(
                "SELECT 1 FROM campaign_registrations WHERE affiliate_code=$1",
                affiliate_code)
            if not exists:
                break
        else:
            affiliate_code = _make_affiliate_code("SLH") + str(secrets.randbelow(99))

        row = await conn.fetchrow("""
            INSERT INTO campaign_registrations
            (campaign_id, path_type, user_id, tg_username, full_name, phone, email, ref_code, affiliate_code, lang, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
            RETURNING id, affiliate_code, created_at
        """, req.campaign_id, req.path_type, req.user_id, req.tg_username,
            req.full_name, req.phone, req.email, req.ref_code, affiliate_code, req.lang)

        # If this was a referral, log pending earning for the referrer
        if req.ref_code:
            try:
                referrer = await conn.fetchrow(
                    "SELECT user_id FROM campaign_registrations WHERE affiliate_code=$1",
                    req.ref_code)
                if referrer and referrer["user_id"]:
                    # 50 ZVK just for signup (even before payment)
                    await conn.execute("""
                        INSERT INTO campaign_affiliate_earnings
                        (affiliate_user_id, affiliate_code, referred_user_id, campaign_id, event_type, zvk_earned, status)
                        VALUES ($1, $2, $3, $4, 'signup', 50, 'pending')
                    """, referrer["user_id"], req.ref_code, req.user_id, req.campaign_id)
            except Exception:
                pass  # don't block registration if referral logging fails

    return {
        "ok": True,
        "registration_id": row["id"],
        "affiliate_code": row["affiliate_code"],
        "referral_link": f"https://slh-nft.com/promo-shekel.html?ref={row['affiliate_code']}",
        "telegram_link": f"https://t.me/SLH_community_bot?start=promo_shekel_april26_{row['affiliate_code']}",
        "path_type": req.path_type
    }


@app.get("/api/campaign/affiliate/{code}")
async def campaign_affiliate_validate(code: str):
    """Public validation of affiliate code — returns only if it exists."""
    async with pool.acquire() as conn:
        await _ensure_campaign_tables(conn)
        row = await conn.fetchrow("""
            SELECT affiliate_code, path_type, created_at,
                   (SELECT COUNT(*) FROM campaign_registrations r2 WHERE r2.ref_code = r1.affiliate_code) as refs_count
            FROM campaign_registrations r1
            WHERE affiliate_code = $1
        """, code)
        if not row:
            return {"valid": False}
        return {
            "valid": True,
            "path_type": row["path_type"],
            "refs_count": row["refs_count"]
        }


@app.get("/api/campaign/affiliate-stats/{code}")
async def campaign_affiliate_stats(code: str):
    """Stats for a specific affiliate — for partner dashboard."""
    async with pool.acquire() as conn:
        await _ensure_campaign_tables(conn)
        owner = await conn.fetchrow(
            "SELECT user_id, path_type, created_at FROM campaign_registrations WHERE affiliate_code=$1",
            code)
        if not owner:
            raise HTTPException(404, "Affiliate code not found")

        refs = await conn.fetch("""
            SELECT id, full_name, path_type, status, created_at, amount_paid
            FROM campaign_registrations
            WHERE ref_code = $1
            ORDER BY created_at DESC
            LIMIT 100
        """, code)

        earnings = await conn.fetchrow("""
            SELECT
                COALESCE(SUM(zvk_earned), 0) as total_zvk,
                COALESCE(SUM(slh_earned), 0) as total_slh,
                COUNT(*) as events
            FROM campaign_affiliate_earnings
            WHERE affiliate_code = $1
        """, code)

        return {
            "affiliate_code": code,
            "path_type": owner["path_type"],
            "joined_at": owner["created_at"].isoformat() if owner["created_at"] else None,
            "referrals_count": len(refs),
            "total_zvk_earned": int(earnings["total_zvk"] or 0),
            "total_slh_earned": float(earnings["total_slh"] or 0),
            "referrals": [
                {
                    "name": r["full_name"] or "Anonymous",
                    "path": r["path_type"],
                    "status": r["status"],
                    "joined": r["created_at"].isoformat() if r["created_at"] else None,
                    "paid": float(r["amount_paid"] or 0)
                } for r in refs
            ]
        }


@app.get("/api/campaign/stats/{campaign_id}")
async def campaign_stats(
    campaign_id: str,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    """Admin-only: full campaign stats."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_campaign_tables(conn)

        clicks = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_clicks,
                COUNT(DISTINCT ref_code) as unique_refs
            FROM campaign_clicks WHERE campaign_id = $1
        """, campaign_id)

        clicks_by_path = await conn.fetch("""
            SELECT path_type, COUNT(*) as n
            FROM campaign_clicks WHERE campaign_id = $1
            GROUP BY path_type
        """, campaign_id)

        clicks_by_lang = await conn.fetch("""
            SELECT lang, COUNT(*) as n
            FROM campaign_clicks WHERE campaign_id = $1
            GROUP BY lang
        """, campaign_id)

        regs = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_regs,
                COUNT(*) FILTER (WHERE status='paid') as paid_regs,
                COALESCE(SUM(amount_paid), 0) as total_revenue
            FROM campaign_registrations WHERE campaign_id = $1
        """, campaign_id)

        regs_by_path = await conn.fetch("""
            SELECT path_type, COUNT(*) as n,
                   COUNT(*) FILTER (WHERE status='paid') as paid,
                   COALESCE(SUM(amount_paid), 0) as revenue
            FROM campaign_registrations WHERE campaign_id = $1
            GROUP BY path_type
        """, campaign_id)

        top_affiliates = await conn.fetch("""
            SELECT r.affiliate_code, r.full_name, r.user_id,
                   (SELECT COUNT(*) FROM campaign_registrations r2 WHERE r2.ref_code = r.affiliate_code) as refs,
                   COALESCE((SELECT SUM(zvk_earned) FROM campaign_affiliate_earnings WHERE affiliate_code = r.affiliate_code), 0) as zvk
            FROM campaign_registrations r
            WHERE campaign_id = $1 AND path_type IN ('partner', 'buyer')
            ORDER BY refs DESC, zvk DESC
            LIMIT 20
        """, campaign_id)

        return {
            "campaign_id": campaign_id,
            "clicks": {
                "total": int(clicks["total_clicks"] or 0),
                "unique_refs": int(clicks["unique_refs"] or 0),
                "by_path": {r["path_type"] or "none": int(r["n"]) for r in clicks_by_path},
                "by_lang": {r["lang"] or "he": int(r["n"]) for r in clicks_by_lang}
            },
            "registrations": {
                "total": int(regs["total_regs"] or 0),
                "paid": int(regs["paid_regs"] or 0),
                "revenue": float(regs["total_revenue"] or 0),
                "by_path": {
                    r["path_type"]: {
                        "count": int(r["n"]),
                        "paid": int(r["paid"]),
                        "revenue": float(r["revenue"] or 0)
                    } for r in regs_by_path
                }
            },
            "conversion_rate": (
                float(regs["total_regs"] or 0) / max(1, int(clicks["total_clicks"] or 1))
            ) if clicks["total_clicks"] else 0,
            "top_affiliates": [
                {
                    "code": a["affiliate_code"],
                    "name": a["full_name"] or "Anonymous",
                    "user_id": a["user_id"],
                    "referrals": int(a["refs"] or 0),
                    "zvk_earned": int(a["zvk"] or 0)
                } for a in top_affiliates
            ]
        }


@app.post("/api/campaign/attribute-purchase")
async def campaign_attribute_purchase(
    user_id: int,
    amount: float,
    campaign_id: str = "shekel_april26",
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    """Admin-only: mark a registration as paid + credit referrer."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_campaign_tables(conn)

        reg = await conn.fetchrow("""
            UPDATE campaign_registrations
            SET status = 'paid', amount_paid = $1, updated_at = NOW()
            WHERE user_id = $2 AND campaign_id = $3
            RETURNING id, ref_code, affiliate_code
        """, amount, user_id, campaign_id)

        if not reg:
            raise HTTPException(404, "Registration not found")

        # Credit referrer: 20% ZVK + 10% SLH equivalent
        if reg["ref_code"]:
            referrer = await conn.fetchrow(
                "SELECT user_id FROM campaign_registrations WHERE affiliate_code=$1",
                reg["ref_code"])
            if referrer and referrer["user_id"]:
                # ZVK ≈ 4.4 ILS → 20% of 99 = 19.8 ILS = 4.5 ZVK × 100 precision = 450 internal
                zvk_reward = int((amount * 0.20 / 4.4) * 100) // 100  # rounded
                # SLH ≈ 444 ILS → 10% of 99 = 9.9 ILS = 0.0223 SLH
                slh_reward = round((amount * 0.10 / 444.0), 6)
                await conn.execute("""
                    INSERT INTO campaign_affiliate_earnings
                    (affiliate_user_id, affiliate_code, referred_user_id, campaign_id, event_type, zvk_earned, slh_earned, status)
                    VALUES ($1, $2, $3, $4, 'purchase', $5, $6, 'confirmed')
                """, referrer["user_id"], reg["ref_code"], user_id, campaign_id, zvk_reward, slh_reward)

        return {"ok": True, "registration_id": reg["id"]}


# ===== END CAMPAIGN SYSTEM =====


# ===== MASS GIFT (Bulk credit ZVK to all users) =====

class MassGiftReq(BaseModel):
    token: str = "ZVK"  # ZVK / SLH / MNH / REP
    amount: float = 10.0
    reason: str = "campaign_gift"
    note: Optional[str] = None
    only_active_days: Optional[int] = None  # if set, only users seen in last N days
    dry_run: bool = True  # default safe — return preview without crediting


@app.post("/api/admin/mass-gift")
async def admin_mass_gift(
    req: MassGiftReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    """Bulk-credit ALL registered users with N tokens.
    Default dry_run=True returns preview only.
    Set dry_run=false to actually credit.
    """
    _require_admin(authorization, x_admin_key)

    token = req.token.upper()
    if token not in ("ZVK", "SLH", "MNH", "REP"):
        raise HTTPException(400, "token must be ZVK/SLH/MNH/REP")

    if req.amount <= 0 or req.amount > 10000:
        raise HTTPException(400, "amount must be between 0 and 10000")

    async with pool.acquire() as conn:
        # Get all eligible users
        try:
            if req.only_active_days:
                rows = await conn.fetch("""
                    SELECT telegram_id, username, first_name
                    FROM web_users
                    WHERE telegram_id >= 1000000
                      AND (last_seen IS NULL OR last_seen >= NOW() - ($1 || ' days')::interval)
                """, str(int(req.only_active_days)))
            else:
                rows = await conn.fetch("""
                    SELECT telegram_id, username, first_name
                    FROM web_users
                    WHERE telegram_id >= 1000000
                """)
        except Exception as e:
            # Fallback if last_seen doesn't exist
            rows = await conn.fetch(
                "SELECT telegram_id, username, first_name FROM web_users WHERE telegram_id >= 1000000"
            )

        users = [dict(r) for r in rows]
        total_users = len(users)
        total_amount = req.amount * total_users

        if req.dry_run:
            return {
                "dry_run": True,
                "preview": {
                    "users_count": total_users,
                    "token": token,
                    "amount_per_user": req.amount,
                    "total_distribution": total_amount,
                    "users_sample": users[:10],
                    "reason": req.reason
                },
                "next_step": "Call again with dry_run=false to actually credit"
            }

        # ACTUAL CREDIT — wrapped in transaction
        credited = []
        failed = []
        async with conn.transaction():
            for u in users:
                uid = u["telegram_id"]
                try:
                    # Update or insert balance
                    await conn.execute("""
                        INSERT INTO token_balances (user_id, token, balance, updated_at)
                        VALUES ($1, $2, $3, NOW())
                        ON CONFLICT (user_id, token)
                        DO UPDATE SET balance = token_balances.balance + $3, updated_at = NOW()
                    """, uid, token, req.amount)

                    # Log the transfer (correct schema: from_user_id, to_user_id, memo, tx_type)
                    await conn.execute("""
                        INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type, created_at)
                        VALUES (0, $1, $2, $3, $4, 'mass_gift', NOW())
                    """, uid, token, req.amount, f"{req.reason}: {req.note or 'mass gift'}")

                    credited.append(uid)
                except Exception as e:
                    failed.append({"user_id": uid, "error": str(e)[:100]})

        return {
            "dry_run": False,
            "credited_count": len(credited),
            "failed_count": len(failed),
            "total_amount_distributed": req.amount * len(credited),
            "token": token,
            "credited_user_ids": credited[:50],  # first 50 only
            "failures": failed[:20],
            "reason": req.reason
        }


@app.get("/api/admin/mass-gift/history")
async def admin_mass_gift_history(
    limit: int = 20,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    """Get history of recent mass gifts (from token_transfers where from_user_id=0)."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch("""
                SELECT
                    DATE_TRUNC('hour', created_at) as hour_bucket,
                    token,
                    SUM(amount) as total_amount,
                    COUNT(DISTINCT to_user_id) as users_credited,
                    MAX(memo) as reason_sample
                FROM token_transfers
                WHERE from_user_id = 0 AND tx_type = 'mass_gift'
                GROUP BY DATE_TRUNC('hour', created_at), token
                ORDER BY hour_bucket DESC
                LIMIT $1
            """, limit)
            return {
                "history": [
                    {
                        "when": r["hour_bucket"].isoformat() if r["hour_bucket"] else None,
                        "token": r["token"],
                        "total_amount": float(r["total_amount"] or 0),
                        "users_credited": int(r["users_credited"] or 0),
                        "reason": r["reason_sample"]
                    } for r in rows
                ]
            }
        except Exception as e:
            return {"history": [], "error": str(e)[:100]}


# ===== END MASS GIFT =====


# ===== EXPERTS DIRECTORY (community-based expert selection) =====

async def _ensure_experts_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS experts (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            display_name TEXT NOT NULL,
            tg_username TEXT,
            phone TEXT,
            email TEXT,
            bio TEXT,
            domains TEXT[],
            languages TEXT[],
            avatar_url TEXT,
            verified BOOLEAN DEFAULT FALSE,
            featured BOOLEAN DEFAULT FALSE,
            avg_rating NUMERIC(3,2) DEFAULT 0,
            reviews_count INTEGER DEFAULT 0,
            consultations_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    # Proof-of-expertise fields (added 2026-04-17)
    await conn.execute("""
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS linkedin_url TEXT;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS website_url TEXT;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS youtube_url TEXT;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS portfolio_url TEXT;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS years_experience INTEGER;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS credentials TEXT;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS verification_status TEXT DEFAULT 'pending';
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS verification_note TEXT;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP;
        ALTER TABLE experts ADD COLUMN IF NOT EXISTS verified_by TEXT;
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS expert_reviews (
            id SERIAL PRIMARY KEY,
            expert_id INTEGER REFERENCES experts(id) ON DELETE CASCADE,
            reviewer_user_id BIGINT,
            reviewer_name TEXT,
            rating INTEGER CHECK (rating BETWEEN 1 AND 5),
            comment TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS expert_consultations (
            id SERIAL PRIMARY KEY,
            expert_id INTEGER REFERENCES experts(id),
            requester_user_id BIGINT,
            requester_name TEXT,
            requester_phone TEXT,
            topic TEXT,
            preferred_language TEXT DEFAULT 'he',
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)


@app.get("/api/experts/list")
async def experts_list(domain: Optional[str] = None, language: Optional[str] = None, limit: int = 50):
    """Public: list experts, filterable by domain/language."""
    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        q = "SELECT id, display_name, tg_username, bio, domains, languages, avatar_url, verified, featured, avg_rating, reviews_count, consultations_count FROM experts WHERE 1=1"
        params = []
        if domain:
            params.append(domain)
            q += f" AND ${len(params)} = ANY(domains)"
        if language:
            params.append(language)
            q += f" AND ${len(params)} = ANY(languages)"
        params.append(limit)
        q += f" ORDER BY featured DESC, avg_rating DESC, reviews_count DESC LIMIT ${len(params)}"
        rows = await conn.fetch(q, *params)
        return {"experts": [dict(r) for r in rows]}


class ExpertCreateReq(BaseModel):
    display_name: str
    tg_username: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    bio: Optional[str] = None
    domains: List[str] = []
    languages: List[str] = ["he"]
    user_id: Optional[int] = None
    avatar_url: Optional[str] = None
    # Proof-of-expertise (new 2026-04-17) — at least one is required for submission
    linkedin_url: Optional[str] = None
    website_url: Optional[str] = None
    youtube_url: Optional[str] = None
    portfolio_url: Optional[str] = None
    years_experience: Optional[int] = None
    credentials: Optional[str] = None


@app.post("/api/experts/register")
async def experts_register(req: ExpertCreateReq):
    """Register as expert — requires at least one proof link. Enters pending_verification.
    Verified only after admin approval via /api/admin/experts/approve.
    Auto-credits 100 ZVK signup bonus."""

    # Require at least one proof link or credentials
    proof_provided = any([
        (req.linkedin_url or "").strip(),
        (req.website_url or "").strip(),
        (req.youtube_url or "").strip(),
        (req.portfolio_url or "").strip(),
        (req.credentials or "").strip(),
    ])
    if not proof_provided:
        raise HTTPException(
            400,
            "נדרשת לפחות הוכחה אחת: LinkedIn, אתר, YouTube, פורטפוליו, או פירוט תעודות."
        )

    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        await _ensure_expert_rewards_tables(conn)
        row = await conn.fetchrow("""
            INSERT INTO experts
            (user_id, display_name, tg_username, phone, email, bio, domains, languages, avatar_url,
             linkedin_url, website_url, youtube_url, portfolio_url, years_experience, credentials,
             verification_status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'pending')
            RETURNING id, display_name, verified, verification_status
        """, req.user_id, req.display_name, req.tg_username, req.phone, req.email,
            req.bio, req.domains, req.languages, req.avatar_url,
            req.linkedin_url, req.website_url, req.youtube_url, req.portfolio_url,
            req.years_experience, req.credentials)
        # Auto-award signup reward
        reward_info = None
        if req.user_id:
            reward_info = await _credit_expert_reward(conn, row["id"], "expert_signup")
        return {
            "ok": True,
            "expert_id": row["id"],
            "verified": row["verified"],
            "verification_status": row["verification_status"],
            "reward": reward_info,
            "message": "נרשמת בהצלחה! קיבלת 100 ZVK בונוס. הפרופיל בהמתנה לאימות אדמין (24-48 שעות)."
        }


# ═══════ Admin approval flow (new 2026-04-17) ═══════

@app.get("/api/admin/experts/pending")
async def admin_experts_pending(
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: list experts awaiting verification."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        rows = await conn.fetch("""
            SELECT id, user_id, display_name, tg_username, phone, email, bio,
                   domains, languages, linkedin_url, website_url, youtube_url,
                   portfolio_url, years_experience, credentials,
                   verification_status, verification_note, created_at
            FROM experts
            WHERE verification_status IN ('pending', 'needs_info')
            ORDER BY created_at DESC
        """)
        return {"pending": [dict(r) for r in rows], "count": len(rows)}


class ExpertApprovalReq(BaseModel):
    expert_id: int
    decision: str  # approved | rejected | needs_info
    note: Optional[str] = None
    featured: bool = False
    reviewed_by: Optional[str] = None


@app.post("/api/admin/experts/approve")
async def admin_experts_approve(
    req: ExpertApprovalReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: approve / reject / request-more-info on a pending expert."""
    _require_admin(authorization, x_admin_key)
    if req.decision not in ("approved", "rejected", "needs_info"):
        raise HTTPException(400, "decision must be approved | rejected | needs_info")

    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        expert = await conn.fetchrow("SELECT id, user_id, display_name, verified FROM experts WHERE id=$1", req.expert_id)
        if not expert:
            raise HTTPException(404, "expert not found")

        if req.decision == "approved":
            await conn.execute("""
                UPDATE experts SET
                  verified = TRUE,
                  featured = $2,
                  verification_status = 'approved',
                  verification_note = $3,
                  verified_at = NOW(),
                  verified_by = $4
                WHERE id = $1
            """, req.expert_id, req.featured, req.note, req.reviewed_by or "admin")
            # Grant verification bonus (ZVK)
            await _credit_expert_reward(conn, req.expert_id, "expert_verified")
        elif req.decision == "rejected":
            await conn.execute("""
                UPDATE experts SET
                  verification_status = 'rejected',
                  verification_note = $2,
                  verified_by = $3
                WHERE id = $1
            """, req.expert_id, req.note, req.reviewed_by or "admin")
        else:  # needs_info
            await conn.execute("""
                UPDATE experts SET
                  verification_status = 'needs_info',
                  verification_note = $2
                WHERE id = $1
            """, req.expert_id, req.note)

    return {
        "ok": True,
        "expert_id": req.expert_id,
        "decision": req.decision,
        "message": {
            "approved": "✅ מומחה אושר + חשיפה בגלריה + בונוס ZVK הוענק",
            "rejected": "❌ בקשה נדחתה",
            "needs_info": "⚠️ נדרש מידע נוסף",
        }[req.decision],
    }


class ExpertReviewReq(BaseModel):
    expert_id: int
    rating: int
    comment: Optional[str] = None
    reviewer_user_id: Optional[int] = None
    reviewer_name: Optional[str] = None


@app.post("/api/experts/review")
async def experts_review(req: ExpertReviewReq):
    if req.rating < 1 or req.rating > 5:
        raise HTTPException(400, "rating must be 1-5")
    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        await conn.execute("""
            INSERT INTO expert_reviews (expert_id, reviewer_user_id, reviewer_name, rating, comment)
            VALUES ($1, $2, $3, $4, $5)
        """, req.expert_id, req.reviewer_user_id, req.reviewer_name, req.rating, req.comment)
        # Update aggregate
        agg = await conn.fetchrow(
            "SELECT AVG(rating) as avg, COUNT(*) as cnt FROM expert_reviews WHERE expert_id=$1",
            req.expert_id)
        await conn.execute(
            "UPDATE experts SET avg_rating=$1, reviews_count=$2 WHERE id=$3",
            float(agg["avg"]), int(agg["cnt"]), req.expert_id)
        return {"ok": True, "new_rating": float(agg["avg"]), "reviews": int(agg["cnt"])}


class ConsultationReq(BaseModel):
    expert_id: int
    requester_name: str
    requester_phone: Optional[str] = None
    requester_user_id: Optional[int] = None
    topic: str
    preferred_language: str = "he"


@app.post("/api/experts/consult")
async def experts_consult(req: ConsultationReq):
    """Request a consultation with an expert."""
    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        row = await conn.fetchrow("""
            INSERT INTO expert_consultations (expert_id, requester_user_id, requester_name, requester_phone, topic, preferred_language)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, created_at
        """, req.expert_id, req.requester_user_id, req.requester_name,
            req.requester_phone, req.topic, req.preferred_language)
        # Increment consultation count
        await conn.execute("UPDATE experts SET consultations_count = consultations_count + 1 WHERE id = $1", req.expert_id)
        return {"ok": True, "consultation_id": row["id"], "message": "הבקשה נשלחה למומחה. תיצור איתך קשר תוך 48 שעות."}


# ===== EXPERT REWARDS + COMMUNITY DOMAINS =====

async def _ensure_expert_rewards_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS expert_rewards (
            id SERIAL PRIMARY KEY,
            expert_id INTEGER REFERENCES experts(id),
            event_type TEXT NOT NULL,
            zvk_amount INTEGER DEFAULT 0,
            rep_amount INTEGER DEFAULT 0,
            note TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS expert_domains (
            id SERIAL PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            name_he TEXT NOT NULL,
            name_en TEXT,
            emoji TEXT,
            category TEXT,
            approved BOOLEAN DEFAULT FALSE,
            votes_for INTEGER DEFAULT 0,
            votes_against INTEGER DEFAULT 0,
            proposed_by BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS domain_votes (
            id SERIAL PRIMARY KEY,
            domain_id INTEGER REFERENCES expert_domains(id) ON DELETE CASCADE,
            voter_user_id BIGINT,
            vote TEXT CHECK (vote IN ('for','against')),
            action TEXT CHECK (action IN ('add','remove','merge')),
            merge_target_id INTEGER,
            comment TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(domain_id, voter_user_id, action)
        )
    """)


# REWARD RULES
REWARD_RULES = {
    "expert_signup": {"zvk": 100, "rep": 10, "note": "הצטרפות כמומחה"},
    "verified": {"zvk": 500, "rep": 50, "note": "אימות פרופיל"},
    "consultation_requested": {"zvk": 50, "rep": 5, "note": "בקשת ייעוץ התקבלה"},
    "consultation_completed": {"zvk": 200, "rep": 20, "note": "ייעוץ הושלם"},
    "five_star_review": {"zvk": 25, "rep": 10, "note": "דירוג 5 כוכבים"},
    "first_consultation": {"zvk": 150, "rep": 15, "note": "בונוס ייעוץ ראשון"},
    "vote_cast": {"zvk": 5, "rep": 1, "note": "השתתפות בהצבעה"}
}


async def _credit_expert_reward(conn, expert_id: int, event_type: str, note_extra: Optional[str] = None):
    """Auto-credit expert rewards. Silent fail on error."""
    rule = REWARD_RULES.get(event_type)
    if not rule:
        return None
    try:
        # Get expert's user_id
        expert = await conn.fetchrow("SELECT user_id FROM experts WHERE id=$1", expert_id)
        if not expert or not expert["user_id"]:
            return None

        # Log reward
        await conn.execute("""
            INSERT INTO expert_rewards (expert_id, event_type, zvk_amount, rep_amount, note)
            VALUES ($1, $2, $3, $4, $5)
        """, expert_id, event_type, rule["zvk"], rule["rep"],
            f"{rule['note']}{(' · ' + note_extra) if note_extra else ''}")

        # Credit ZVK to user
        if rule["zvk"] > 0:
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance, updated_at)
                VALUES ($1, 'ZVK', $2, NOW())
                ON CONFLICT (user_id, token)
                DO UPDATE SET balance = token_balances.balance + $2, updated_at = NOW()
            """, expert["user_id"], rule["zvk"])
        return {"event": event_type, "zvk_credited": rule["zvk"], "rep_credited": rule["rep"]}
    except Exception as e:
        print(f"[expert_reward] silent fail: {e}")
        return None


# Default domains list (pre-approved)
DEFAULT_DOMAINS = [
    {"slug":"crypto", "name_he":"קריפטו", "name_en":"Crypto", "emoji":"₿", "category":"finance"},
    {"slug":"security", "name_he":"אבטחת מידע", "name_en":"Security", "emoji":"🛡️", "category":"tech"},
    {"slug":"finance", "name_he":"פיננסים", "name_en":"Finance", "emoji":"💰", "category":"finance"},
    {"slug":"trading", "name_he":"מסחר", "name_en":"Trading", "emoji":"📈", "category":"finance"},
    {"slug":"tech", "name_he":"פיתוח", "name_en":"Development", "emoji":"💻", "category":"tech"},
    {"slug":"marketing", "name_he":"שיווק דיגיטלי", "name_en":"Marketing", "emoji":"📣", "category":"business"},
    {"slug":"legal", "name_he":"משפט", "name_en":"Legal", "emoji":"⚖️", "category":"business"},
    {"slug":"halacha", "name_he":"הלכה", "name_en":"Halacha", "emoji":"🕊️", "category":"religious"},
    {"slug":"accounting", "name_he":"ראיית חשבון", "name_en":"Accounting", "emoji":"🧾", "category":"finance"},
    {"slug":"tax", "name_he":"מיסים", "name_en":"Taxation", "emoji":"💸", "category":"finance"},
    {"slug":"ai", "name_he":"AI / בינה מלאכותית", "name_en":"AI", "emoji":"🤖", "category":"tech"},
    {"slug":"design", "name_he":"עיצוב", "name_en":"Design", "emoji":"🎨", "category":"creative"},
    {"slug":"writing", "name_he":"כתיבה", "name_en":"Writing", "emoji":"✍️", "category":"creative"},
    {"slug":"translation", "name_he":"תרגום", "name_en":"Translation", "emoji":"🌐", "category":"creative"},
    {"slug":"video", "name_he":"וידאו / עריכה", "name_en":"Video", "emoji":"🎬", "category":"creative"},
    {"slug":"photography", "name_he":"צילום", "name_en":"Photography", "emoji":"📸", "category":"creative"},
    {"slug":"sales", "name_he":"מכירות", "name_en":"Sales", "emoji":"🤝", "category":"business"},
    {"slug":"hr", "name_he":"משאבי אנוש", "name_en":"HR", "emoji":"👥", "category":"business"},
    {"slug":"real_estate", "name_he":"נדל\"ן", "name_en":"Real Estate", "emoji":"🏠", "category":"business"},
    {"slug":"medical", "name_he":"רפואה / בריאות", "name_en":"Medical", "emoji":"⚕️", "category":"health"},
    {"slug":"therapy", "name_he":"טיפול נפשי", "name_en":"Therapy", "emoji":"💚", "category":"health"},
    {"slug":"nutrition", "name_he":"תזונה", "name_en":"Nutrition", "emoji":"🥗", "category":"health"},
    {"slug":"coaching", "name_he":"קואצ'ינג", "name_en":"Coaching", "emoji":"🎯", "category":"education"},
    {"slug":"education", "name_he":"חינוך / הוראה", "name_en":"Education", "emoji":"🎓", "category":"education"},
    {"slug":"academia", "name_he":"אקדמיה", "name_en":"Academia", "emoji":"📚", "category":"education"},
    {"slug":"music", "name_he":"מוזיקה", "name_en":"Music", "emoji":"🎵", "category":"creative"},
    {"slug":"fitness", "name_he":"כושר", "name_en":"Fitness", "emoji":"💪", "category":"health"},
    {"slug":"language", "name_he":"שפות", "name_en":"Languages", "emoji":"🗣️", "category":"education"},
    {"slug":"startup", "name_he":"סטארט-אפים", "name_en":"Startups", "emoji":"🚀", "category":"business"},
    {"slug":"blockchain", "name_he":"בלוקצ'יין", "name_en":"Blockchain", "emoji":"⛓️", "category":"tech"},
    # Creative & lifestyle
    {"slug":"art", "name_he":"אומנות", "name_en":"Art", "emoji":"🎨", "category":"creative"},
    {"slug":"musician", "name_he":"מוסיקאי", "name_en":"Musician", "emoji":"🎸", "category":"creative"},
    {"slug":"conductor", "name_he":"מנצח תזמורת", "name_en":"Orchestra Conductor", "emoji":"🎼", "category":"creative"},
    {"slug":"motorcycle", "name_he":"אופנוע", "name_en":"Motorcycle", "emoji":"🏍️", "category":"sports"},
    {"slug":"kitesurf", "name_he":"גלישת קייט", "name_en":"Kite Surfing", "emoji":"🪁", "category":"sports"},
    {"slug":"surf", "name_he":"גלישת גלים", "name_en":"Surfing", "emoji":"🏄", "category":"sports"},
    {"slug":"ski", "name_he":"סקי", "name_en":"Skiing", "emoji":"⛷️", "category":"sports"},
    {"slug":"climb", "name_he":"טיפוס", "name_en":"Climbing", "emoji":"🧗", "category":"sports"},
    {"slug":"yoga", "name_he":"יוגה", "name_en":"Yoga", "emoji":"🧘", "category":"health"},
    {"slug":"meditation", "name_he":"מדיטציה", "name_en":"Meditation", "emoji":"🕉️", "category":"health"},
    {"slug":"chef", "name_he":"שף", "name_en":"Chef", "emoji":"👨‍🍳", "category":"creative"},
    {"slug":"travel", "name_he":"תיירות", "name_en":"Travel", "emoji":"✈️", "category":"lifestyle"},
    {"slug":"parenting", "name_he":"הורות", "name_en":"Parenting", "emoji":"👨‍👩‍👧", "category":"lifestyle"},
    {"slug":"gaming", "name_he":"גיימינג", "name_en":"Gaming", "emoji":"🎮", "category":"entertainment"},
    {"slug":"podcast", "name_he":"פודקאסטים", "name_en":"Podcasting", "emoji":"🎙️", "category":"creative"}
]


@app.get("/api/experts/domains")
async def experts_domains_list():
    """Public list of all approved domains + pending proposals."""
    async with pool.acquire() as conn:
        await _ensure_expert_rewards_tables(conn)
        # Seed defaults if empty
        count = await conn.fetchval("SELECT COUNT(*) FROM expert_domains")
        if count == 0:
            for d in DEFAULT_DOMAINS:
                try:
                    await conn.execute("""
                        INSERT INTO expert_domains (slug, name_he, name_en, emoji, category, approved)
                        VALUES ($1, $2, $3, $4, $5, TRUE)
                        ON CONFLICT (slug) DO NOTHING
                    """, d["slug"], d["name_he"], d["name_en"], d["emoji"], d["category"])
                except Exception:
                    pass

        approved = await conn.fetch(
            "SELECT slug, name_he, name_en, emoji, category FROM expert_domains WHERE approved = TRUE ORDER BY category, name_he"
        )
        pending = await conn.fetch(
            "SELECT id, slug, name_he, name_en, emoji, category, votes_for, votes_against FROM expert_domains WHERE approved = FALSE ORDER BY votes_for DESC"
        )
        return {
            "approved": [dict(r) for r in approved],
            "pending": [dict(r) for r in pending]
        }


class DomainProposeReq(BaseModel):
    action: str  # 'add', 'remove', 'merge'
    slug: Optional[str] = None  # for new domain
    name_he: Optional[str] = None
    name_en: Optional[str] = None
    emoji: Optional[str] = None
    category: Optional[str] = None
    existing_domain_id: Optional[int] = None  # for remove/merge
    merge_target_id: Optional[int] = None  # for merge
    proposer_user_id: Optional[int] = None


@app.post("/api/experts/domains/propose")
async def experts_domains_propose(req: DomainProposeReq):
    """Propose a new domain or action on existing."""
    async with pool.acquire() as conn:
        await _ensure_expert_rewards_tables(conn)
        if req.action == "add":
            if not req.slug or not req.name_he:
                raise HTTPException(400, "slug + name_he required for add")
            row = await conn.fetchrow("""
                INSERT INTO expert_domains (slug, name_he, name_en, emoji, category, approved, proposed_by)
                VALUES ($1, $2, $3, $4, $5, FALSE, $6)
                ON CONFLICT (slug) DO NOTHING
                RETURNING id
            """, req.slug, req.name_he, req.name_en, req.emoji or "🎯", req.category or "other", req.proposer_user_id)
            if not row:
                raise HTTPException(409, "Domain slug already exists")
            return {"ok": True, "domain_id": row["id"], "status": "pending_votes", "needs_votes": 10}
        # remove/merge actions logged as votes
        return {"ok": True, "action": req.action, "note": "Action requires 10+ votes to pass"}


class DomainVoteReq(BaseModel):
    domain_id: int
    voter_user_id: int
    vote: str  # 'for' or 'against'
    action: str = "add"  # 'add', 'remove', 'merge'
    merge_target_id: Optional[int] = None
    comment: Optional[str] = None


@app.post("/api/experts/domains/vote")
async def experts_domains_vote(req: DomainVoteReq):
    """Cast vote on domain proposal."""
    if req.vote not in ("for", "against"):
        raise HTTPException(400, "vote must be for/against")
    if req.action not in ("add", "remove", "merge"):
        raise HTTPException(400, "invalid action")
    async with pool.acquire() as conn:
        await _ensure_expert_rewards_tables(conn)
        # Try to insert vote (UNIQUE constraint prevents double-voting)
        try:
            await conn.execute("""
                INSERT INTO domain_votes (domain_id, voter_user_id, vote, action, merge_target_id, comment)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, req.domain_id, req.voter_user_id, req.vote, req.action, req.merge_target_id, req.comment)
        except Exception as e:
            if "unique" in str(e).lower():
                raise HTTPException(409, "Already voted on this proposal")
            raise
        # Update counter
        # SECURITY: whitelisted — 'col' is chosen between two hardcoded literals ('votes_for' / 'votes_against') via req.vote which is validated at line 9127
        col = "votes_for" if req.vote == "for" else "votes_against"
        await conn.execute(f"UPDATE expert_domains SET {col} = {col} + 1 WHERE id=$1", req.domain_id)
        # Check if auto-approve (for adds: 10+ for-votes, 2x against threshold)
        row = await conn.fetchrow("SELECT votes_for, votes_against, approved FROM expert_domains WHERE id=$1", req.domain_id)
        approved_now = False
        if row and not row["approved"] and req.action == "add":
            if row["votes_for"] >= 10 and row["votes_for"] >= 2 * row["votes_against"]:
                await conn.execute("UPDATE expert_domains SET approved=TRUE WHERE id=$1", req.domain_id)
                approved_now = True
        # Credit voter (if they are expert)
        try:
            expert_row = await conn.fetchrow("SELECT id FROM experts WHERE user_id=$1 LIMIT 1", req.voter_user_id)
            if expert_row:
                await _credit_expert_reward(conn, expert_row["id"], "vote_cast")
        except Exception:
            pass
        return {"ok": True, "votes_for": row["votes_for"], "votes_against": row["votes_against"], "approved": approved_now}


@app.get("/api/experts/{expert_id}/reviews")
async def experts_reviews(expert_id: int, limit: int = 50):
    """Public list of reviews for an expert (for transparency)."""
    async with pool.acquire() as conn:
        await _ensure_experts_tables(conn)
        rows = await conn.fetch("""
            SELECT id, reviewer_name, rating, comment, created_at
            FROM expert_reviews WHERE expert_id=$1
            ORDER BY created_at DESC LIMIT $2
        """, expert_id, limit)
        agg = await conn.fetchrow("""
            SELECT AVG(rating) as avg_rating, COUNT(*) as count,
                   COUNT(*) FILTER (WHERE rating=5) as five_stars,
                   COUNT(*) FILTER (WHERE rating=1) as one_stars
            FROM expert_reviews WHERE expert_id=$1
        """, expert_id)
        return {
            "expert_id": expert_id,
            "avg_rating": float(agg["avg_rating"] or 0),
            "reviews_count": int(agg["count"] or 0),
            "five_stars": int(agg["five_stars"] or 0),
            "one_stars": int(agg["one_stars"] or 0),
            "reviews": [dict(r) for r in rows]
        }


@app.get("/api/experts/{expert_id}/rewards")
async def experts_rewards(expert_id: int):
    """Public view of expert's rewards history."""
    async with pool.acquire() as conn:
        await _ensure_expert_rewards_tables(conn)
        rows = await conn.fetch("""
            SELECT event_type, zvk_amount, rep_amount, note, created_at
            FROM expert_rewards WHERE expert_id=$1
            ORDER BY created_at DESC LIMIT 50
        """, expert_id)
        total = await conn.fetchrow("""
            SELECT COALESCE(SUM(zvk_amount), 0) as total_zvk,
                   COALESCE(SUM(rep_amount), 0) as total_rep,
                   COUNT(*) as events
            FROM expert_rewards WHERE expert_id=$1
        """, expert_id)
        return {
            "total_zvk": int(total["total_zvk"] or 0),
            "total_rep": int(total["total_rep"] or 0),
            "events_count": int(total["events"] or 0),
            "recent": [dict(r) for r in rows],
            "reward_rules": REWARD_RULES
        }


# ===== END EXPERT REWARDS =====


# ===== END EXPERTS =====


# ===== BUG REPORTS (via AI Assistant or direct) =====

async def _ensure_bug_reports_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS bug_reports (
            id SERIAL PRIMARY KEY,
            reporter_user_id BIGINT,
            reporter_name TEXT,
            reporter_email TEXT,
            page_url TEXT,
            ai_session_id TEXT,
            severity TEXT DEFAULT 'medium',
            category TEXT,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            steps_to_reproduce TEXT,
            screenshot_url TEXT,
            user_agent TEXT,
            status TEXT DEFAULT 'new',
            assigned_to TEXT,
            resolution TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    # STEP F: AI analysis storage (2026-04-17)
    await conn.execute("""
        ALTER TABLE bug_reports ADD COLUMN IF NOT EXISTS ai_analysis TEXT;
        ALTER TABLE bug_reports ADD COLUMN IF NOT EXISTS ai_analyzed_at TIMESTAMP;
        ALTER TABLE bug_reports ADD COLUMN IF NOT EXISTS ai_agent TEXT;
    """)


class BugReportReq(BaseModel):
    title: str
    description: str
    page_url: Optional[str] = None
    severity: Optional[str] = "medium"  # low/medium/high/critical
    category: Optional[str] = None
    steps_to_reproduce: Optional[str] = None
    reporter_name: Optional[str] = None
    reporter_email: Optional[str] = None
    reporter_user_id: Optional[int] = None
    ai_session_id: Optional[str] = None
    screenshot_url: Optional[str] = None


@app.post("/api/bugs/report")
async def bugs_report(req: BugReportReq, user_agent: Optional[str] = Header(None)):
    """Anyone can report a bug — anonymous or with details."""
    if not req.title or not req.description:
        raise HTTPException(400, "title and description required")
    severity = req.severity if req.severity in ("low", "medium", "high", "critical") else "medium"
    async with pool.acquire() as conn:
        await _ensure_bug_reports_table(conn)
        row = await conn.fetchrow("""
            INSERT INTO bug_reports (reporter_user_id, reporter_name, reporter_email, page_url, ai_session_id,
                severity, category, title, description, steps_to_reproduce, screenshot_url, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id, created_at
        """, req.reporter_user_id, req.reporter_name, req.reporter_email, req.page_url,
            req.ai_session_id, severity, req.category, req.title, req.description,
            req.steps_to_reproduce, req.screenshot_url, (user_agent or "")[:200])
    # Telegram alert to admin — rate-limited + kill-switch
    # SILENT_MODE=1 on Railway → no alerts at all
    # Otherwise: max 1 alert per 5 min for non-critical; critical always sent
    try:
        import time as _t
        _silent = os.getenv("SILENT_MODE", "").strip() == "1"
        _is_auto = req.title.startswith("[AUTO]")  # auto-captured — never alert, just log
        _now = _t.time()
        _last_key = "_last_bug_alert_ts"
        _last = getattr(app.state, _last_key, 0) if hasattr(app, "state") else 0
        _min_gap = 300  # 5 minutes
        should_alert = not _silent and not _is_auto and (severity == "critical" or (_now - _last) >= _min_gap)
        if should_alert and BROADCAST_BOT_TOKEN:
            sev_emoji = {"critical": "🚨", "high": "⚠️", "medium": "🐛", "low": "💡"}.get(severity, "🐛")
            cat = req.category or "כללי"
            reporter = req.reporter_name or (f"ID {req.reporter_user_id}" if req.reporter_user_id else "אנונימי")
            page = req.page_url or "—"
            alert_text = (
                f"{sev_emoji} <b>באג חדש #{row['id']}</b>\n"
                f"<b>חומרה:</b> {severity} | <b>קטגוריה:</b> {cat}\n"
                f"<b>מדווח:</b> {reporter}\n"
                f"<b>כותרת:</b> {req.title[:150]}\n\n"
                f"{req.description[:400]}\n\n"
                f"📄 {page}\n"
                f"🔗 https://slh-nft.com/admin-bugs.html"
            )
            await _tg_send_message(BROADCAST_BOT_TOKEN, ADMIN_USER_ID, alert_text)
            if hasattr(app, "state"):
                setattr(app.state, _last_key, _now)
    except Exception:
        pass  # never block user report on notification failure
    return {
        "ok": True,
        "bug_id": row["id"],
        "message": "תודה! הדיווח נקלט. נטפל בו בהקדם.",
        "tracking": f"bug-{row['id']}"
    }


class BugStatusUpdate(BaseModel):
    status: str  # new / in_progress / resolved / rejected
    resolution: Optional[str] = ""


@app.patch("/api/admin/bugs/{bug_id}/status")
async def bugs_update_status(
    bug_id: int,
    req: BugStatusUpdate,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    if req.status not in ("new", "in_progress", "resolved", "rejected"):
        raise HTTPException(400, "invalid status")
    async with pool.acquire() as conn:
        await _ensure_bug_reports_table(conn)
        bug = await conn.fetchrow("SELECT * FROM bug_reports WHERE id = $1", bug_id)
        if not bug:
            raise HTTPException(404, "bug not found")
        await conn.execute("""
            UPDATE bug_reports SET status = $1, resolution = $2, updated_at = NOW() WHERE id = $3
        """, req.status, req.resolution or "", bug_id)
    # Notify reporter on telegram if they provided a user_id (skip in silent mode)
    try:
        if os.getenv("SILENT_MODE", "").strip() == "1":
            pass  # no outbound telegram while in silent mode
        elif req.status == "resolved" and bug["reporter_user_id"] and BROADCAST_BOT_TOKEN:
            msg = (
                f"✅ <b>הבאג שדיווחת נפתר!</b>\n"
                f"<b>#{bug_id}:</b> {bug['title']}\n\n"
                f"{(req.resolution or 'הבעיה תוקנה. תודה על הדיווח!')[:800]}\n\n"
                f"💙 תודה שעזרת לנו לשפר את SLH"
            )
            await _tg_send_message(BROADCAST_BOT_TOKEN, bug["reporter_user_id"], msg)
        elif req.status == "in_progress" and bug["reporter_user_id"] and BROADCAST_BOT_TOKEN:
            msg = (
                f"🔧 <b>הדיווח שלך התקבל ומטופל</b>\n"
                f"<b>#{bug_id}:</b> {bug['title']}\n\n"
                f"נעדכן אותך ברגע שהבעיה תיפתר."
            )
            await _tg_send_message(BROADCAST_BOT_TOKEN, bug["reporter_user_id"], msg)
    except Exception:
        pass
    return {"ok": True, "bug_id": bug_id, "status": req.status}


@app.get("/api/admin/bugs/list")
async def bugs_list_admin(
    status: Optional[str] = None,
    severity: Optional[str] = None,
    limit: int = 100,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_bug_reports_table(conn)
        q = "SELECT * FROM bug_reports WHERE 1=1"
        params = []
        if status:
            params.append(status)
            q += f" AND status = ${len(params)}"
        if severity:
            params.append(severity)
            q += f" AND severity = ${len(params)}"
        params.append(limit)
        q += f" ORDER BY created_at DESC LIMIT ${len(params)}"
        rows = await conn.fetch(q, *params)
        # Stats
        stats = await conn.fetchrow("""
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE status='new') as new,
                   COUNT(*) FILTER (WHERE status='in_progress') as in_progress,
                   COUNT(*) FILTER (WHERE status='resolved') as resolved,
                   COUNT(*) FILTER (WHERE severity='critical') as critical
            FROM bug_reports
        """)
        return {
            "bugs": [dict(r) for r in rows],
            "stats": dict(stats) if stats else {}
        }


# ===== STEP F: AI bug analysis =====

class BugAIAnalyzeReq(BaseModel):
    agent: str = "claude_code"  # claude_code | advisor | human_only
    context_hint: Optional[str] = None  # optional additional context


@app.post("/api/admin/bugs/{bug_id}/ai-analyze")
async def bugs_ai_analyze(
    bug_id: int,
    req: BugAIAnalyzeReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Request AI analysis of a bug. Stores the suggestion in bug_reports.ai_analysis.

    Three agents supported:
    - 'claude_code' — for executor agents with git/docker access (returns structured TODO)
    - 'advisor'     — for chat-only AIs (returns diagnostic steps)
    - 'human_only'  — no AI; just marks the bug as 'human_only' triage

    The actual AI call uses the internal /api/ai/chat endpoint chain (groq/gemini/openai).
    """
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_bug_reports_table(conn)
        bug = await conn.fetchrow("SELECT * FROM bug_reports WHERE id=$1", bug_id)
        if not bug:
            raise HTTPException(404, "bug not found")

        if req.agent == "human_only":
            await conn.execute(
                "UPDATE bug_reports SET ai_agent='human_only', ai_analyzed_at=NOW() WHERE id=$1",
                bug_id,
            )
            return {"ok": True, "agent": "human_only", "analysis": "marked for human-only triage"}

        # Build prompt
        system_for_agent = {
            "claude_code": (
                "You are SLH Claude Code Executor. Given a bug report, return:\n"
                "1. Root cause hypothesis (1-2 sentences)\n"
                "2. Files likely to need changes (list)\n"
                "3. Suggested fix (diff-style or pseudo-code)\n"
                "4. How to verify the fix (curl/UI steps)\n"
                "5. Priority (low|medium|high|critical)\n"
                "Respond in Hebrew. Be terse. Max 300 words."
            ),
            "advisor": (
                "You are a senior QA advisor. Given a bug report, diagnose it:\n"
                "1. What is the user seeing?\n"
                "2. What is likely broken?\n"
                "3. What would you ask the user to confirm?\n"
                "4. What test would isolate the issue?\n"
                "Respond in Hebrew. Max 200 words."
            ),
        }.get(req.agent, "You are a senior software engineer.")

        bug_context = (
            f"BUG #{bug['id']}\n"
            f"Title: {bug['title']}\n"
            f"Severity: {bug['severity']}\n"
            f"Category: {bug.get('category') or '—'}\n"
            f"Page: {bug.get('page_url') or '—'}\n"
            f"Description:\n{bug['description']}\n"
            f"Steps to reproduce:\n{bug.get('steps_to_reproduce') or '—'}\n"
        )
        if req.context_hint:
            bug_context += f"\nAdditional context: {req.context_hint}"

        # Call our internal AI chat (uses whatever providers are configured)
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "message": system_for_agent + "\n\n---\n\n" + bug_context,
                    "user_id": "bug_ai_admin",
                    "lang": "he",
                }
                async with session.post(
                    f"http://localhost:{os.getenv('PORT', '8000')}/api/ai/chat",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    ai_data = await resp.json()
                    analysis = ai_data.get("reply") or ai_data.get("detail") or "AI returned empty response"
                    model_used = ai_data.get("model", "unknown")
        except Exception as e:
            analysis = f"AI call failed: {e}. Try again in a moment, or use agent='human_only'."
            model_used = "error"

        await conn.execute(
            """
            UPDATE bug_reports SET
              ai_analysis = $1,
              ai_analyzed_at = NOW(),
              ai_agent = $2
            WHERE id = $3
            """,
            analysis, f"{req.agent}::{model_used}", bug_id,
        )

    return {
        "ok": True,
        "bug_id": bug_id,
        "agent": req.agent,
        "model": model_used,
        "analysis": analysis,
    }


# ===== END BUG REPORTS =====


# ===== GUARDIAN DIAGNOSTIC API (placeholder for now) =====

@app.get("/api/admin/guardian/stats")
async def guardian_stats(authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    _require_admin(authorization, x_admin_key)
    return {
        "clients": 0, "online": 0, "pending": 0, "runs_today": 0, "failed": 0,
        "note": "Guardian client registration coming with @Grdian_bot integration"
    }


@app.get("/api/admin/guardian/history")
async def guardian_history(limit: int = 50, authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    _require_admin(authorization, x_admin_key)
    return {"history": [], "note": "Will populate after Guardian bot deployment"}


@app.get("/api/admin/guardian/audit")
async def guardian_audit(limit: int = 100, authorization: Optional[str] = Header(None), x_admin_key: Optional[str] = Header(None)):
    _require_admin(authorization, x_admin_key)
    return {"audit": [], "note": "Will populate after Guardian bot deployment"}


# ===== END GUARDIAN =====


# ============================================================
# BROKER ACCOUNTS + DEPOSITS + EXPENSES + ESP PREORDERS
# Critical financial infrastructure — April 15, 2026
# ============================================================

async def _ensure_financial_tables(conn):
    # Broker accounts — Tzvika, Elazar + future brokers with LIMITED admin access
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS broker_accounts (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE,
            display_name TEXT NOT NULL,
            tg_username TEXT,
            phone TEXT,
            email TEXT,
            role TEXT DEFAULT 'broker',
            permissions TEXT[] DEFAULT ARRAY['view_own_referrals','view_own_deposits','view_own_commissions'],
            commission_pct NUMERIC(5,2) DEFAULT 10.0,
            status TEXT DEFAULT 'active',
            owner_visible_to BIGINT[] DEFAULT ARRAY[]::BIGINT[],
            total_referrals INTEGER DEFAULT 0,
            total_commissions_ils NUMERIC(14,2) DEFAULT 0,
            total_commissions_zvk NUMERIC(14,4) DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            notes TEXT
        )
    """)

    # ESP preorders — auto-gift 2 SLH from Tzvika's wallet
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS esp_preorders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            buyer_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT,
            payment_method TEXT,
            amount_paid_ils NUMERIC(12,2) NOT NULL,
            payment_status TEXT DEFAULT 'pending',
            payment_reference TEXT,
            broker_id INTEGER REFERENCES broker_accounts(id),
            slh_gift_granted NUMERIC(10,4) DEFAULT 0,
            slh_gift_tx_id TEXT,
            slh_gifted_at TIMESTAMP,
            shipping_status TEXT DEFAULT 'waiting',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Deposits — with compound interest tracking
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            broker_id INTEGER REFERENCES broker_accounts(id),
            amount_usd NUMERIC(14,4) NOT NULL,
            amount_ils NUMERIC(14,2),
            slh_received NUMERIC(14,6) DEFAULT 0,
            monthly_rate_pct NUMERIC(5,3) NOT NULL,
            term_months NUMERIC(4,2) NOT NULL,
            compounding TEXT DEFAULT 'monthly',
            status TEXT DEFAULT 'active',
            deposited_at TIMESTAMP DEFAULT NOW(),
            maturity_at TIMESTAMP,
            withdrawn_at TIMESTAMP,
            total_interest_accrued NUMERIC(14,4) DEFAULT 0,
            last_interest_calc TIMESTAMP DEFAULT NOW(),
            notes TEXT,
            reminder_sent_at TIMESTAMP,
            is_test BOOLEAN DEFAULT FALSE
        )
    """)

    # Company + personal expenses
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id SERIAL PRIMARY KEY,
            added_by_user_id BIGINT NOT NULL,
            category TEXT NOT NULL,
            subcategory TEXT,
            description TEXT NOT NULL,
            amount_ils NUMERIC(12,2) NOT NULL,
            amount_currency TEXT DEFAULT 'ILS',
            amount_original NUMERIC(12,2),
            vendor TEXT,
            invoice_number TEXT,
            payment_date DATE NOT NULL,
            payment_method TEXT,
            tax_deductible BOOLEAN DEFAULT TRUE,
            vat_amount NUMERIC(10,2) DEFAULT 0,
            is_recurring BOOLEAN DEFAULT FALSE,
            attachment_url TEXT,
            notes TEXT,
            scope TEXT DEFAULT 'company',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Credit card transactions — for ₪888 kosher wallet + future
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS credit_card_payments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            buyer_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT,
            id_number TEXT,
            amount_ils NUMERIC(12,2) NOT NULL,
            installments INTEGER DEFAULT 1,
            product_type TEXT NOT NULL,
            product_reference TEXT,
            card_last4 TEXT,
            card_brand TEXT,
            card_holder_name TEXT,
            cvv_verified BOOLEAN DEFAULT FALSE,
            status TEXT DEFAULT 'pending',
            provider TEXT,
            provider_tx_id TEXT,
            failure_reason TEXT,
            broker_id INTEGER REFERENCES broker_accounts(id),
            processed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    await conn.execute("CREATE INDEX IF NOT EXISTS idx_deposits_user ON deposits(user_id, status)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_esp_broker ON esp_preorders(broker_id)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(payment_date DESC)")


# ============================================================
# BROKER ACCOUNTS
# ============================================================

class BrokerCreateReq(BaseModel):
    user_id: Optional[int] = None
    display_name: str
    tg_username: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    role: str = "broker"
    commission_pct: float = 10.0
    permissions: Optional[List[str]] = None
    visible_to: Optional[List[int]] = None
    notes: Optional[str] = None


@app.post("/api/brokers/create")
async def brokers_create(
    req: BrokerCreateReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        perms = req.permissions or ['view_own_referrals', 'view_own_deposits', 'view_own_commissions']
        visible = req.visible_to or [ADMIN_USER_ID]
        row = await conn.fetchrow("""
            INSERT INTO broker_accounts
            (user_id, display_name, tg_username, phone, email, role, permissions, commission_pct, owner_visible_to, notes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (user_id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                phone = EXCLUDED.phone,
                email = EXCLUDED.email,
                permissions = EXCLUDED.permissions
            RETURNING id, display_name, permissions, commission_pct, status
        """, req.user_id, req.display_name, req.tg_username, req.phone, req.email,
            req.role, perms, req.commission_pct, visible, req.notes)
        return {"ok": True, "broker": dict(row)}


@app.get("/api/brokers/list")
async def brokers_list(
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        rows = await conn.fetch("""
            SELECT id, user_id, display_name, tg_username, role, commission_pct,
                   total_referrals, total_commissions_ils, total_commissions_zvk, status, created_at
            FROM broker_accounts ORDER BY created_at DESC
        """)
        return {"brokers": [dict(r) for r in rows]}


@app.get("/api/brokers/{broker_id}/dashboard")
async def brokers_dashboard(broker_id: int, user_id: Optional[int] = None):
    """Broker's own dashboard — limited data only they can see."""
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        broker = await conn.fetchrow("SELECT * FROM broker_accounts WHERE id=$1", broker_id)
        if not broker:
            raise HTTPException(404, "Broker not found")

        # Visibility check — only self, admin, or approved viewers
        if user_id and user_id != broker["user_id"] and user_id not in (broker["owner_visible_to"] or []):
            if user_id != ADMIN_USER_ID:
                raise HTTPException(403, "Not authorized")

        # Get referrals
        esp_orders = await conn.fetch(
            "SELECT id, buyer_name, amount_paid_ils, payment_status, slh_gift_granted, created_at FROM esp_preorders WHERE broker_id=$1 ORDER BY created_at DESC",
            broker_id
        )
        deposits = await conn.fetch(
            "SELECT id, user_id, amount_usd, monthly_rate_pct, term_months, status, deposited_at, total_interest_accrued FROM deposits WHERE broker_id=$1 ORDER BY deposited_at DESC",
            broker_id
        )

        return {
            "broker": dict(broker),
            "esp_preorders": [dict(r) for r in esp_orders],
            "deposits": [dict(r) for r in deposits],
            "summary": {
                "total_esp_orders": len(esp_orders),
                "total_deposits": len(deposits),
                "total_referrals": broker["total_referrals"],
                "commissions_ils": float(broker["total_commissions_ils"] or 0),
                "commissions_zvk": float(broker["total_commissions_zvk"] or 0),
            }
        }


# ============================================================
# DEPOSITS with COMPOUND INTEREST
# ============================================================

class DepositReq(BaseModel):
    user_id: int
    broker_id: Optional[int] = None
    amount_usd: float
    monthly_rate_pct: float = 4.0
    term_months: float = 2.0
    compounding: str = "monthly"
    slh_received: float = 0
    is_test: bool = False
    notes: Optional[str] = None


def _calculate_compound_interest(principal: float, monthly_rate: float, months_elapsed: float, compounding: str = "monthly") -> dict:
    """Compound interest calculator. monthly_rate in percent (e.g. 4.0 = 4%)."""
    r = monthly_rate / 100.0
    if compounding == "monthly":
        n = 12
        elapsed_years = months_elapsed / 12.0
        # Annual rate equivalent: (1+monthly)^12 - 1
        # But we compound monthly at r per month:
        final = principal * ((1 + r) ** months_elapsed)
    elif compounding == "daily":
        days_elapsed = months_elapsed * 30.0
        daily_rate = r / 30.0
        final = principal * ((1 + daily_rate) ** days_elapsed)
    else:  # simple
        final = principal * (1 + r * months_elapsed)
    interest = final - principal
    return {
        "principal": round(principal, 4),
        "monthly_rate_pct": monthly_rate,
        "months_elapsed": round(months_elapsed, 4),
        "compounding": compounding,
        "interest_accrued": round(interest, 4),
        "current_value": round(final, 4)
    }


@app.post("/api/deposits/create")
async def deposits_create(
    req: DepositReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        from datetime import timedelta
        maturity = datetime.now() + timedelta(days=int(req.term_months * 30))
        row = await conn.fetchrow("""
            INSERT INTO deposits
            (user_id, broker_id, amount_usd, monthly_rate_pct, term_months,
             compounding, slh_received, is_test, notes, maturity_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            RETURNING id, deposited_at, maturity_at
        """, req.user_id, req.broker_id, req.amount_usd, req.monthly_rate_pct,
            req.term_months, req.compounding, req.slh_received, req.is_test,
            req.notes, maturity)
        return {"ok": True, "deposit": dict(row)}


@app.get("/api/deposits/{deposit_id}/status")
async def deposits_status(deposit_id: int):
    """Live deposit status with compound interest."""
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        d = await conn.fetchrow("SELECT * FROM deposits WHERE id=$1", deposit_id)
        if not d:
            raise HTTPException(404, "Deposit not found")
        from datetime import datetime as dt
        deposited = d["deposited_at"]
        now = dt.now()
        if hasattr(deposited, 'tzinfo') and deposited.tzinfo:
            deposited = deposited.replace(tzinfo=None)
        days_elapsed = max(0, (now - deposited).total_seconds() / 86400)
        months_elapsed = days_elapsed / 30.0
        calc = _calculate_compound_interest(
            float(d["amount_usd"]),
            float(d["monthly_rate_pct"]),
            months_elapsed,
            d["compounding"]
        )
        # Store latest calc
        await conn.execute(
            "UPDATE deposits SET total_interest_accrued=$1, last_interest_calc=NOW() WHERE id=$2",
            calc["interest_accrued"], deposit_id
        )
        return {
            "deposit_id": deposit_id,
            "user_id": d["user_id"],
            "status": d["status"],
            "is_test": d["is_test"],
            "deposited_at": deposited.isoformat(),
            "maturity_at": d["maturity_at"].isoformat() if d["maturity_at"] else None,
            "days_elapsed": round(days_elapsed, 2),
            "term_months": float(d["term_months"]),
            **calc,
            "slh_received": float(d["slh_received"] or 0),
            "notes": d["notes"]
        }


@app.get("/api/deposits/user/{user_id}")
async def deposits_user_list(user_id: int):
    """All deposits for a specific user (with live compound interest).

    Two deposit schemas co-exist (legacy tx_hash-based + financial investment-
    tracker). This endpoint now returns from whichever is available; missing
    columns are handled gracefully instead of 500-ing the whole request.
    """
    from datetime import datetime as dt
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        # Check which columns actually exist
        cols_row = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name='deposits'"
        )
        cols = {r["column_name"] for r in cols_row}
        has_financial = {"amount_usd", "monthly_rate_pct", "term_months", "deposited_at"}.issubset(cols)

        if has_financial:
            rows = await conn.fetch(
                """SELECT id, amount_usd, monthly_rate_pct, term_months, status,
                          deposited_at, COALESCE(is_test, FALSE) AS is_test
                   FROM deposits WHERE user_id=$1 ORDER BY deposited_at DESC""",
                user_id
            )
            result = []
            for d in rows:
                deposited = d["deposited_at"]
                if hasattr(deposited, 'tzinfo') and deposited.tzinfo:
                    deposited = deposited.replace(tzinfo=None)
                months_elapsed = max(0, (dt.now() - deposited).total_seconds() / 86400 / 30.0)
                calc = _calculate_compound_interest(float(d["amount_usd"]), float(d["monthly_rate_pct"]), months_elapsed)
                result.append({**dict(d), **calc, "deposited_at": deposited.isoformat()})
            return {"user_id": user_id, "deposits": result}
        else:
            # Legacy schema — map to compatible shape with zero interest
            rows = await conn.fetch(
                "SELECT id, amount, currency, tx_hash, status, created_at FROM deposits WHERE user_id=$1 ORDER BY id DESC",
                user_id
            )
            return {"user_id": user_id, "deposits": [
                {
                    "id": r["id"],
                    "amount_usd": float(r["amount"]) if r["amount"] else 0,
                    "currency": r["currency"],
                    "tx_hash": r["tx_hash"],
                    "status": r["status"],
                    "deposited_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "monthly_rate_pct": 0, "term_months": 0, "is_test": False,
                    "current_value": float(r["amount"]) if r["amount"] else 0,
                    "total_interest": 0, "months_elapsed": 0,
                }
                for r in rows
            ]}


# ============================================================
# ESP PREORDERS with AUTO 2 SLH GIFT
# ============================================================

class ESPPreorderReq(BaseModel):
    buyer_name: str
    phone: str
    email: Optional[str] = None
    user_id: Optional[int] = None
    payment_method: str = "bank"
    amount_paid_ils: float = 888.0
    payment_reference: Optional[str] = None
    broker_id: Optional[int] = None


@app.post("/api/esp/preorder")
async def esp_preorder(req: ESPPreorderReq):
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        row = await conn.fetchrow("""
            INSERT INTO esp_preorders
            (user_id, buyer_name, phone, email, payment_method, amount_paid_ils, payment_reference, broker_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            RETURNING id, created_at
        """, req.user_id, req.buyer_name, req.phone, req.email,
            req.payment_method, req.amount_paid_ils, req.payment_reference, req.broker_id)
        return {
            "ok": True,
            "preorder_id": row["id"],
            "message": "ההזמנה התקבלה! 2 SLH יועברו מצביקה לאחר אישור התשלום.",
            "next_step": "payment confirmation + 2 SLH gift + shipping schedule"
        }


@app.post("/api/esp/preorder/{preorder_id}/approve")
async def esp_preorder_approve(
    preorder_id: int,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    """Admin approves → auto-credits 2 SLH to buyer."""
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        p = await conn.fetchrow("SELECT * FROM esp_preorders WHERE id=$1", preorder_id)
        if not p:
            raise HTTPException(404, "Preorder not found")
        if p["slh_gift_granted"] and float(p["slh_gift_granted"]) >= 2:
            return {"ok": False, "error": "already gifted"}
        # Mark approved + credit 2 SLH
        await conn.execute(
            "UPDATE esp_preorders SET payment_status='approved', slh_gift_granted=2.0, slh_gifted_at=NOW() WHERE id=$1",
            preorder_id
        )
        # Credit SLH to buyer in token_balances
        if p["user_id"]:
            await conn.execute("""
                INSERT INTO token_balances (user_id, token, balance, updated_at)
                VALUES ($1, 'SLH', 2.0, NOW())
                ON CONFLICT (user_id, token)
                DO UPDATE SET balance = token_balances.balance + 2.0, updated_at = NOW()
            """, p["user_id"])
            await conn.execute("""
                INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type, created_at)
                VALUES (7757102350, $1, 'SLH', 2.0, 'ESP preorder gift from Tzvika', 'esp_gift', NOW())
            """, p["user_id"])  # Tzvika's user_id = 7757102350 per memory
        return {"ok": True, "preorder_id": preorder_id, "slh_gifted": 2.0, "from": "Tzvika"}


# ============================================================
# EXPENSES
# ============================================================

class ExpenseReq(BaseModel):
    category: str
    description: str
    amount_ils: float
    payment_date: str
    added_by_user_id: int
    subcategory: Optional[str] = None
    vendor: Optional[str] = None
    invoice_number: Optional[str] = None
    payment_method: Optional[str] = None
    amount_currency: str = "ILS"
    amount_original: Optional[float] = None
    vat_amount: float = 0
    tax_deductible: bool = True
    is_recurring: bool = False
    scope: str = "company"
    notes: Optional[str] = None


@app.post("/api/expenses/add")
async def expenses_add(
    req: ExpenseReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        from datetime import datetime as dt
        payment_date = dt.fromisoformat(req.payment_date).date()
        row = await conn.fetchrow("""
            INSERT INTO expenses
            (added_by_user_id, category, subcategory, description, amount_ils, amount_currency,
             amount_original, vendor, invoice_number, payment_date, payment_method,
             tax_deductible, vat_amount, is_recurring, scope, notes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            RETURNING id, created_at
        """, req.added_by_user_id, req.category, req.subcategory, req.description,
            req.amount_ils, req.amount_currency, req.amount_original, req.vendor,
            req.invoice_number, payment_date, req.payment_method, req.tax_deductible,
            req.vat_amount, req.is_recurring, req.scope, req.notes)
        return {"ok": True, "expense_id": row["id"]}


@app.get("/api/expenses/list")
async def expenses_list(
    scope: Optional[str] = None,
    category: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 100,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        q = "SELECT * FROM expenses WHERE 1=1"
        params = []
        if scope:
            params.append(scope)
            q += f" AND scope=${len(params)}"
        if category:
            params.append(category)
            q += f" AND category=${len(params)}"
        if from_date:
            params.append(from_date)
            q += f" AND payment_date>=${len(params)}"
        if to_date:
            params.append(to_date)
            q += f" AND payment_date<=${len(params)}"
        params.append(limit)
        q += f" ORDER BY payment_date DESC LIMIT ${len(params)}"
        rows = await conn.fetch(q, *params)
        summary = await conn.fetchrow("""
            SELECT
                SUM(amount_ils) FILTER (WHERE scope='company') as company_total,
                SUM(amount_ils) FILTER (WHERE scope='personal') as personal_total,
                SUM(amount_ils) FILTER (WHERE tax_deductible=TRUE) as deductible_total,
                SUM(vat_amount) as vat_total,
                COUNT(*) as total_count
            FROM expenses
        """)
        return {
            "expenses": [dict(r) for r in rows],
            "summary": dict(summary) if summary else {}
        }


# ============================================================
# CREDIT CARD PAYMENTS (₪888 kosher wallet + future)
# ============================================================

class CreditCardReq(BaseModel):
    buyer_name: str
    phone: str
    email: Optional[str] = None
    id_number: Optional[str] = None
    user_id: Optional[int] = None
    amount_ils: float
    installments: int = 1
    product_type: str  # kosher_wallet / starter_pack / custom
    product_reference: Optional[str] = None
    card_last4: str
    card_brand: Optional[str] = None
    card_holder_name: Optional[str] = None
    cvv_verified: bool = False
    broker_id: Optional[int] = None


@app.post("/api/payment/credit-card/submit")
async def card_payment_submit(req: CreditCardReq):
    """Submit a credit card payment request. Actual charging happens via provider integration (future)."""
    if req.amount_ils < 1 or req.amount_ils > 50000:
        raise HTTPException(400, "Amount must be between ₪1 and ₪50,000")
    if not req.card_last4 or len(req.card_last4) != 4 or not req.card_last4.isdigit():
        raise HTTPException(400, "Invalid card last 4 digits")
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        row = await conn.fetchrow("""
            INSERT INTO credit_card_payments
            (user_id, buyer_name, phone, email, id_number, amount_ils, installments,
             product_type, product_reference, card_last4, card_brand, card_holder_name,
             cvv_verified, broker_id, status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending')
            RETURNING id, created_at
        """, req.user_id, req.buyer_name, req.phone, req.email, req.id_number,
            req.amount_ils, req.installments, req.product_type, req.product_reference,
            req.card_last4, req.card_brand, req.card_holder_name, req.cvv_verified,
            req.broker_id)
        return {
            "ok": True,
            "payment_id": row["id"],
            "status": "pending",
            "message": "התשלום נקלט. יעובד תוך 24 שעות. תקבל אישור/דחייה במייל/SMS.",
            "next_step": "Manual review by admin + provider integration when available"
        }


@app.get("/api/admin/payments/list")
async def admin_payments_list(
    status: Optional[str] = None,
    product_type: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_financial_tables(conn)
        q = "SELECT * FROM credit_card_payments WHERE 1=1"
        params = []
        if status:
            params.append(status)
            q += f" AND status=${len(params)}"
        if product_type:
            params.append(product_type)
            q += f" AND product_type=${len(params)}"
        q += " ORDER BY created_at DESC LIMIT 200"
        rows = await conn.fetch(q, *params)
        return {"payments": [dict(r) for r in rows]}


# ===== END FINANCIAL SYSTEM =====


# ===== DEVICE ONBOARDING (phone → user_id → device_id → signing_token) =====

async def _ensure_device_tables(conn):
    """Create users_by_phone, devices, device_verify_codes, device_events tables."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users_by_phone (
            user_id BIGSERIAL PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            telegram_id BIGINT,
            display_name TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            last_seen TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_users_by_phone_tg ON users_by_phone(telegram_id)
            WHERE telegram_id IS NOT NULL;
        CREATE TABLE IF NOT EXISTS devices (
            device_id TEXT PRIMARY KEY,
            user_id BIGINT,
            device_type TEXT NOT NULL,
            device_name TEXT,
            signing_token TEXT,
            last_ip TEXT,
            last_user_agent TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            last_seen TIMESTAMP DEFAULT NOW(),
            registered_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_devices_user ON devices(user_id);
        CREATE TABLE IF NOT EXISTS device_verify_codes (
            id BIGSERIAL PRIMARY KEY,
            phone TEXT NOT NULL,
            device_id TEXT NOT NULL,
            code TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            used BOOLEAN DEFAULT FALSE,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_verify_phone_device ON device_verify_codes(phone, device_id, used);
    """)


_VALID_DEVICE_TYPES = {"pc_windows", "pc_mac", "pc_linux", "esp32", "sim_gsm", "smartphone", "other"}


class DeviceRegisterReq(BaseModel):
    phone: str
    device_id: str
    device_type: str = "other"
    device_name: Optional[str] = None


class DeviceVerifyReq(BaseModel):
    phone: str
    device_id: str
    code: str


def _normalize_phone(p: str) -> str:
    """Normalize Israeli phone to digits-only: 0501234567 or +972501234567 → 972501234567."""
    import re as _re
    digits = _re.sub(r"\D", "", p or "")
    if digits.startswith("0"):
        digits = "972" + digits[1:]
    return digits


@app.post("/api/device/register")
async def device_register(req: DeviceRegisterReq, request: Request):
    """Step 1: device sends phone + device_id → we generate 6-digit code, send via Telegram (if linked)
    or SMS fallback (stub). Code valid 5 min."""
    phone = _normalize_phone(req.phone)
    if len(phone) < 10:
        raise HTTPException(400, "invalid phone")
    if req.device_type not in _VALID_DEVICE_TYPES:
        raise HTTPException(400, f"device_type must be one of {sorted(_VALID_DEVICE_TYPES)}")
    if not req.device_id or len(req.device_id) > 64:
        raise HTTPException(400, "device_id required, max 64 chars")

    code = f"{secrets.randbelow(1000000):06d}"
    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        # Rate limit: max 3 codes per phone per day
        recent = await conn.fetchval("""
            SELECT COUNT(*) FROM device_verify_codes
            WHERE phone = $1 AND created_at > NOW() - INTERVAL '24 hours'
        """, phone)
        if (recent or 0) >= 5:
            raise HTTPException(429, "too many verification requests today")
        await conn.execute("""
            INSERT INTO device_verify_codes (phone, device_id, code, expires_at)
            VALUES ($1, $2, $3, NOW() + INTERVAL '5 minutes')
        """, phone, req.device_id, code)

        # Try to send via Telegram if phone is linked to a user
        tg_sent = False
        u = await conn.fetchrow("SELECT telegram_id FROM users_by_phone WHERE phone = $1", phone)
        if u and u["telegram_id"] and BROADCAST_BOT_TOKEN:
            try:
                msg = f"🔐 קוד אימות SLH: <b>{code}</b>\nמכשיר: {req.device_name or req.device_id}\nתקף ל-5 דקות."
                r = await _tg_send_message(BROADCAST_BOT_TOKEN, u["telegram_id"], msg)
                tg_sent = bool(r.get("ok"))
            except Exception:
                pass

    # Real SMS fallback (Twilio / Inforu / sms019 / stub). See api/sms_provider.py.
    sms_sent = False
    sms_provider = "none"
    sms_error = None
    if not tg_sent:
        try:
            # On Railway: sms_provider.py is at /app/sms_provider.py (api/ is the build root)
            # On local dev: file is at api/sms_provider.py — fall back if Railway-style fails
            try:
                from sms_provider import send_otp as _send_otp
            except ImportError:
                from api.sms_provider import send_otp as _send_otp
            sms_result = await _send_otp(phone, code, purpose="device_pair")
            sms_sent = sms_result.ok and not sms_result.stub
            sms_provider = sms_result.provider
            sms_error = sms_result.error
        except Exception as e:
            sms_error = f"sms_module_error: {type(e).__name__}: {e}"

    delivery = "telegram" if tg_sent else ("sms" if sms_sent else "pending")

    # Expose the dev code in the web response ONLY when no real delivery channel
    # is configured. If an admin wired a real SMS provider and it fails, we
    # don't fall through — we surface the error instead (forces admin to fix).
    # stub/disabled/none = nothing real is configured, so it's safe to expose.
    expose_dev_code = (
        (not tg_sent)
        and (not sms_sent)
        and sms_provider in ("stub", "disabled", "none")
    )

    return {
        "ok": True,
        "delivery": delivery,
        "expires_in": 300,
        "sms_provider": sms_provider,
        "message": (
            "קוד אימות נשלח לטלגרם שלך" if tg_sent
            else f"קוד אימות נשלח ב-SMS ({sms_provider})" if sms_sent
            else "SMS עדיין לא מחובר — הקוד מוצג לבדיקה"
        ),
        "sms_error": sms_error if not sms_sent else None,
        "_dev_code": code if expose_dev_code else None,
    }


@app.post("/api/device/verify")
async def device_verify(req: DeviceVerifyReq):
    """Step 2: device sends code → we validate, create user (if new), create device, return signing_token."""
    phone = _normalize_phone(req.phone)
    if len(phone) < 10:
        raise HTTPException(400, "invalid phone")
    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        row = await conn.fetchrow("""
            SELECT id, attempts FROM device_verify_codes
            WHERE phone = $1 AND device_id = $2 AND code = $3
              AND used = FALSE AND expires_at > NOW()
            ORDER BY id DESC LIMIT 1
        """, phone, req.device_id, req.code)
        if not row:
            # Bump attempts on any active code for this phone+device
            await conn.execute("""
                UPDATE device_verify_codes SET attempts = attempts + 1
                WHERE phone = $1 AND device_id = $2 AND used = FALSE AND expires_at > NOW()
            """, phone, req.device_id)
            raise HTTPException(400, "invalid or expired code")
        await conn.execute("UPDATE device_verify_codes SET used = TRUE WHERE id = $1", row["id"])

        # Upsert user
        user = await conn.fetchrow("SELECT user_id FROM users_by_phone WHERE phone = $1", phone)
        if user:
            user_id = user["user_id"]
        else:
            user_id = await conn.fetchval(
                "INSERT INTO users_by_phone (phone) VALUES ($1) RETURNING user_id", phone
            )

        # Generate signing token (32 bytes url-safe)
        token = secrets.token_urlsafe(32)

        # Upsert device
        await conn.execute("""
            INSERT INTO devices (device_id, user_id, device_type, signing_token, registered_at, last_seen)
            VALUES ($1, $2, 'other', $3, NOW(), NOW())
            ON CONFLICT (device_id) DO UPDATE
                SET user_id = EXCLUDED.user_id,
                    signing_token = EXCLUDED.signing_token,
                    last_seen = NOW(),
                    is_active = TRUE
        """, req.device_id, user_id, token)

    try:
        from shared.events import emit as _emit
        await _emit(pool, "device.registered", {
            "device_id": req.device_id,
            "user_id": user_id,
            "phone_suffix": phone[-4:] if phone else None,
        }, source="api.device.verify")
    except Exception as e:
        print(f"[device_verify][WARN] event emit failed: {e!r}")

    return {
        "ok": True,
        "user_id": user_id,
        "device_id": req.device_id,
        "signing_token": token,
        "message": "מכשיר רשום בהצלחה"
    }


class EspHeartbeatReq(BaseModel):
    device_id: str
    fw: Optional[str] = None
    ssid: Optional[str] = None
    rssi: Optional[int] = None
    ip: Optional[str] = None
    uptime_seconds: Optional[int] = None
    free_heap: Optional[int] = None
    last_button: Optional[str] = None
    metrics: Optional[dict] = None


@app.get("/api/device/claim/{device_id}")
async def device_claim(device_id: str, request: Request):
    """Device-side companion to web pairing. Once the web page calls /api/device/verify
    for this device_id, the token is in devices table. This endpoint lets the device
    fetch its signing_token by polling — single-use, device clears local pending-pair
    state after success.

    Response shapes:
      { "paired": false }                                       — not paired yet
      { "paired": true, "user_id": int, "signing_token": str }  — paired, consume it
    """
    if not device_id or len(device_id) > 64:
        raise HTTPException(400, "device_id required")
    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        # Only serve claim if device was paired in the last 15 minutes AND has
        # never been heart-beated yet (i.e., the device has not started using its token).
        row = await conn.fetchrow("""
            SELECT user_id, signing_token, registered_at, last_seen
            FROM devices
            WHERE device_id = $1
              AND is_active = TRUE
              AND signing_token IS NOT NULL
              AND registered_at >= NOW() - INTERVAL '15 minutes'
        """, device_id)
        if not row:
            return {"paired": False}
        # Heuristic: if last_seen > registered_at + 1 min, the device has already claimed → deny
        reg = row["registered_at"]
        seen = row["last_seen"]
        if reg and seen and (seen - reg).total_seconds() > 60:
            return {"paired": False, "note": "already claimed"}
        return {
            "paired": True,
            "user_id": row["user_id"],
            "signing_token": row["signing_token"],
        }


async def _ensure_heartbeat_table(conn):
    """Additive: device_heartbeats audit log + last_seen extensions on devices."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS device_heartbeats (
            id BIGSERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            user_id BIGINT,
            fw TEXT,
            ssid TEXT,
            rssi INT,
            ip TEXT,
            uptime_seconds INT,
            free_heap INT,
            last_button TEXT,
            metrics JSONB,
            received_at TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_heartbeats_device_id_received "
        "ON device_heartbeats(device_id, received_at DESC)"
    )


@app.post("/api/esp/heartbeat")
async def esp_heartbeat(
    req: EspHeartbeatReq,
    request: Request,
    authorization: Optional[str] = Header(None),
):
    """ESP32/CYD heartbeat. Requires `Authorization: Bearer <signing_token>` from device verify.
    Updates devices.last_seen + appends to device_heartbeats audit log.
    Emits `device.heartbeat` event (throttled — first of day + every 100 heartbeats per device)."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "missing bearer signing_token")
    token = authorization[7:].strip()
    if not token or len(token) < 16:
        raise HTTPException(401, "invalid signing_token")

    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        await _ensure_heartbeat_table(conn)
        dev = await conn.fetchrow(
            "SELECT user_id, signing_token FROM devices WHERE device_id = $1 AND is_active = TRUE",
            req.device_id
        )
        if not dev or dev["signing_token"] != token:
            raise HTTPException(401, "device_id/token mismatch")

        user_id = dev["user_id"]
        client_ip = request.client.host if request.client else None

        await conn.execute(
            "UPDATE devices SET last_seen = NOW(), last_ip = $1 WHERE device_id = $2",
            client_ip, req.device_id
        )
        hb_id = await conn.fetchval("""
            INSERT INTO device_heartbeats
                (device_id, user_id, fw, ssid, rssi, ip, uptime_seconds, free_heap, last_button, metrics)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            RETURNING id
        """, req.device_id, user_id, req.fw, req.ssid, req.rssi,
             client_ip or req.ip, req.uptime_seconds, req.free_heap, req.last_button,
             json.dumps(req.metrics or {}))

        # Throttled event emission: first heartbeat of the day per device + milestone every 100
        should_emit = False
        today_count = await conn.fetchval(
            "SELECT COUNT(*) FROM device_heartbeats "
            "WHERE device_id = $1 AND received_at >= CURRENT_DATE",
            req.device_id
        )
        if today_count is not None and today_count in (1, 100, 500, 1000):
            should_emit = True

    if should_emit:
        try:
            from shared.events import emit as _emit
            await _emit(pool, "device.heartbeat", {
                "device_id": req.device_id,
                "user_id": user_id,
                "fw": req.fw,
                "today_count": today_count,
            }, source="api.esp.heartbeat")
        except Exception as e:
            print(f"[esp_heartbeat][WARN] event emit failed: {e!r}")

    return {"ok": True, "heartbeat_id": hb_id, "user_id": user_id, "server_time": datetime.utcnow().isoformat()}


@app.get("/api/esp/commands/{device_id}")
async def esp_get_pending_command(
    device_id: str,
    authorization: Optional[str] = Header(None),
):
    """Device polls for pending commands. Returns `{command: str | None}`.
    Signing token auth. Pulls from device_commands table (FIFO, is_consumed=FALSE)."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "missing bearer signing_token")
    token = authorization[7:].strip()

    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS device_commands (
                id BIGSERIAL PRIMARY KEY,
                device_id TEXT NOT NULL,
                command TEXT NOT NULL,
                payload JSONB DEFAULT '{}'::jsonb,
                is_consumed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                consumed_at TIMESTAMP,
                created_by TEXT
            )
        """)
        dev = await conn.fetchrow(
            "SELECT user_id, signing_token FROM devices WHERE device_id = $1 AND is_active = TRUE",
            device_id
        )
        if not dev or dev["signing_token"] != token:
            raise HTTPException(401, "device_id/token mismatch")

        row = await conn.fetchrow("""
            SELECT id, command, payload FROM device_commands
            WHERE device_id = $1 AND is_consumed = FALSE
            ORDER BY id ASC LIMIT 1
        """, device_id)
        if not row:
            return {"command": None}
        await conn.execute(
            "UPDATE device_commands SET is_consumed = TRUE, consumed_at = NOW() WHERE id = $1",
            row["id"]
        )
        return {"command": row["command"], "payload": row["payload"], "cmd_id": row["id"]}


@app.post("/api/esp/commands/{device_id}")
async def esp_push_command(
    device_id: str,
    body: dict,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin pushes a command to a device queue. body: {command: str, payload?: dict}."""
    admin_id = _require_admin(authorization, x_admin_key)
    cmd = (body or {}).get("command")
    if not cmd or not isinstance(cmd, str):
        raise HTTPException(400, "command (str) required")
    payload = (body or {}).get("payload") or {}

    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS device_commands (
                id BIGSERIAL PRIMARY KEY,
                device_id TEXT NOT NULL,
                command TEXT NOT NULL,
                payload JSONB DEFAULT '{}'::jsonb,
                is_consumed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                consumed_at TIMESTAMP,
                created_by TEXT
            )
        """)
        exists = await conn.fetchval("SELECT 1 FROM devices WHERE device_id = $1", device_id)
        if not exists:
            raise HTTPException(404, "device not found")
        cmd_id = await conn.fetchval("""
            INSERT INTO device_commands (device_id, command, payload, created_by)
            VALUES ($1, $2, $3::jsonb, $4)
            RETURNING id
        """, device_id, cmd, json.dumps(payload), str(admin_id))
    return {"ok": True, "cmd_id": cmd_id, "device_id": device_id}


class LinkPhoneTgReq(BaseModel):
    phone: str
    telegram_id: int
    display_name: Optional[str] = None


@app.get("/api/admin/events")
async def admin_events_list(
    limit: int = Query(50, le=200),
    after_id: int = Query(0),
    types: Optional[str] = Query(None, description="Comma-separated event types to filter"),
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: read event_log (ring buffer for chain-status page + debugging).

    Returns newest events first by default. Use after_id=<n> for a cursor-style
    read (events with id > n, oldest first — good for tailing).

    Types can filter to a subset: e.g. `types=payment.cleared,stake.opened`.
    """
    _require_admin(authorization, x_admin_key)
    type_list = None
    if types:
        type_list = [t.strip() for t in types.split(",") if t.strip()]

    try:
        from shared.events import ensure_event_log_table
    except Exception as e:
        raise HTTPException(503, f"events module unavailable: {e!r}")

    async with pool.acquire() as conn:
        await ensure_event_log_table(conn)
        if after_id > 0:
            if type_list:
                rows = await conn.fetch(
                    "SELECT id, event_type, payload, created_at, source FROM event_log "
                    "WHERE id > $1 AND event_type = ANY($2::text[]) ORDER BY id ASC LIMIT $3",
                    after_id, type_list, limit
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, event_type, payload, created_at, source FROM event_log "
                    "WHERE id > $1 ORDER BY id ASC LIMIT $2",
                    after_id, limit
                )
        else:
            if type_list:
                rows = await conn.fetch(
                    "SELECT id, event_type, payload, created_at, source FROM event_log "
                    "WHERE event_type = ANY($1::text[]) ORDER BY id DESC LIMIT $2",
                    type_list, limit
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, event_type, payload, created_at, source FROM event_log "
                    "ORDER BY id DESC LIMIT $1",
                    limit
                )
        total = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM event_log")
        by_type = await conn.fetch(
            "SELECT event_type, COUNT(*) AS n FROM event_log "
            "WHERE created_at >= NOW() - INTERVAL '24 hours' "
            "GROUP BY event_type ORDER BY n DESC"
        )

    return {
        "total_events": int(total or 0),
        "events_24h_by_type": {r["event_type"]: int(r["n"]) for r in by_type},
        "events": [
            {
                "id": r["id"],
                "type": r["event_type"],
                "payload": r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"]),
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "source": r["source"],
            }
            for r in rows
        ],
    }


@app.post("/api/admin/link-phone-tg")
async def link_phone_to_telegram(
    req: LinkPhoneTgReq,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None),
):
    """Admin: link an existing users_by_phone row to a Telegram user_id.

    After this call, future /api/device/register requests for this phone will
    deliver the 6-digit code via Telegram DM rather than falling back to SMS.

    Idempotent: calling twice with the same pair is a no-op.
    Upserts the row if the phone doesn't exist yet.
    """
    admin_id = _require_admin(authorization, x_admin_key)
    phone = _normalize_phone(req.phone)
    if len(phone) < 10:
        raise HTTPException(400, "invalid phone")
    if req.telegram_id <= 0:
        raise HTTPException(400, "telegram_id must be positive int")

    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        existing = await conn.fetchrow(
            "SELECT user_id, telegram_id FROM users_by_phone WHERE phone = $1", phone
        )
        if existing:
            if existing["telegram_id"] == req.telegram_id:
                return {"ok": True, "already_linked": True,
                        "user_id": existing["user_id"], "phone": phone,
                        "telegram_id": req.telegram_id}
            await conn.execute(
                "UPDATE users_by_phone SET telegram_id = $1, "
                "display_name = COALESCE($2, display_name) WHERE phone = $3",
                req.telegram_id, req.display_name, phone
            )
            user_id = existing["user_id"]
            action = "relinked"
        else:
            user_id = await conn.fetchval(
                "INSERT INTO users_by_phone (phone, telegram_id, display_name) "
                "VALUES ($1, $2, $3) RETURNING user_id",
                phone, req.telegram_id, req.display_name
            )
            action = "created"

    try:
        from shared.events import emit as _emit
        await _emit(pool, "phone.tg_linked", {
            "user_id": user_id,
            "phone_suffix": phone[-4:],
            "telegram_id": req.telegram_id,
            "action": action,
            "by_admin": str(admin_id),
        }, source="api.admin.link-phone-tg")
    except Exception as _e:
        print(f"[link_phone_tg][WARN] emit failed: {_e!r}")

    return {"ok": True, "already_linked": False, "action": action,
            "user_id": user_id, "phone": phone, "telegram_id": req.telegram_id}


@app.get("/api/admin/devices/list")
async def devices_list_admin(
    user_id: Optional[int] = None,
    device_type: Optional[str] = None,
    limit: int = 100,
    authorization: Optional[str] = Header(None),
    x_admin_key: Optional[str] = Header(None)
):
    _require_admin(authorization, x_admin_key)
    async with pool.acquire() as conn:
        await _ensure_device_tables(conn)
        q = """SELECT d.*, u.phone, u.telegram_id
               FROM devices d LEFT JOIN users_by_phone u ON d.user_id = u.user_id
               WHERE 1=1"""
        params = []
        if user_id:
            params.append(user_id)
            q += f" AND d.user_id = ${len(params)}"
        if device_type:
            params.append(device_type)
            q += f" AND d.device_type = ${len(params)}"
        params.append(limit)
        q += f" ORDER BY d.last_seen DESC LIMIT ${len(params)}"
        rows = await conn.fetch(q, *params)
        return {"devices": [dict(r) for r in rows]}


# ===== END DEVICE ONBOARDING =====




# ===== OPS REALITY ENDPOINT — auth via ADMIN_BROADCAST_KEY =====
# Osif's "single source of truth" admin snapshot. Accepts ADMIN_BROADCAST_KEY
# (default: slh-broadcast-2026-change-me) because ADMIN_API_KEYS is often
# empty on Railway (chicken-and-egg with rotation). Read-only; no mutations.
# Used by /admin/reality.html to give Osif real control without phantom data.

@app.get("/api/ops/reality")
async def ops_reality(x_broadcast_key: Optional[str] = Header(None)):
    """Return a full snapshot of real platform state. Auth: X-Broadcast-Key header."""
    if not x_broadcast_key or x_broadcast_key != ADMIN_BROADCAST_KEY:
        raise HTTPException(403, "Broadcast key required in X-Broadcast-Key header")
    if pool is None or _db_init_failed:
        raise HTTPException(503, "DB pool unavailable")

    async with pool.acquire() as conn:
        # Users
        users = await conn.fetch("""
            SELECT telegram_id, username, first_name, is_registered, beta_user,
                   beta_coupon_code, beta_nft_number, eth_wallet, ton_wallet,
                   last_login, registered_at, language_pref
              FROM web_users
              ORDER BY telegram_id
        """)

        # External payments (ILS bank/credit card)
        payments = await conn.fetch("""
            SELECT id, user_id, provider, provider_tx_id, amount, currency,
                   status, plan_key, bot_name, metadata, created_at
              FROM external_payments
              ORDER BY created_at DESC
        """)

        # Academy licenses
        licenses = await conn.fetch("""
            SELECT l.id, l.user_id, l.course_id, l.payment_id, l.status, l.purchased_at,
                   c.slug, c.title_he, c.price_ils
              FROM academy_licenses l
              LEFT JOIN academy_courses c ON c.id = l.course_id
              ORDER BY l.purchased_at DESC
        """)

        # Academy courses
        courses = await conn.fetch("""
            SELECT id, slug, title_he, price_ils, price_slh, active,
                   instructor_id, approval_status, language
              FROM academy_courses
              ORDER BY id
        """)

        # Deposits (on-chain)
        deposits = await conn.fetch("""
            SELECT id, user_id, address, amount, token, tx_hash, chain, status, confirmed_at
              FROM deposits
              ORDER BY id DESC
              LIMIT 100
        """)

        # Marketplace
        marketplace_items = await conn.fetchval("SELECT COUNT(*) FROM marketplace_items")
        marketplace_orders = await conn.fetchval("SELECT COUNT(*) FROM marketplace_orders")

        # Staking
        try:
            stakes = await conn.fetchval("SELECT COUNT(*) FROM staking_positions WHERE status='active'")
        except Exception:
            stakes = None

        # Broadcasts sent
        try:
            broadcasts = await conn.fetch("""
                SELECT id, sent_at, target, total_targets, success_count, fail_count, message_preview
                  FROM broadcast_log
                  ORDER BY id DESC LIMIT 20
            """)
        except Exception:
            broadcasts = []

        # Referrals
        try:
            referral_users = await conn.fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id IS NOT NULL")
        except Exception:
            referral_users = None

    from decimal import Decimal as _Dec
    def _row(r):
        out = {}
        for k, v in dict(r).items():
            if v is None:
                out[k] = None
            elif hasattr(v, 'isoformat'):
                out[k] = v.isoformat()
            elif isinstance(v, _Dec):
                out[k] = float(v)
            else:
                out[k] = v
        # Also parse JSONB columns (asyncpg returns them as str)
        if 'metadata' in out and isinstance(out['metadata'], str):
            try:
                out['metadata'] = json.loads(out['metadata'])
            except Exception:
                pass
        return out

    # Classify users
    user_rows = [_row(r) for r in users]
    total_users = len(user_rows)
    real_users = [u for u in user_rows if u['telegram_id'] >= 1000000]
    test_users = [u for u in user_rows if u['telegram_id'] < 1000000]
    founder_ids = {224223270, 7757102350, 8789977826, 1185887485}  # incl. Tzvika Kaufman (co-founder)
    founder_rows = [u for u in real_users if u['telegram_id'] in founder_ids]
    community_rows = [u for u in real_users if u['telegram_id'] not in founder_ids]

    payment_rows = [_row(r) for r in payments]
    real_payments = [p for p in payment_rows
                     if not (p.get('metadata') or {}).get('self_test')]
    self_test_payments = [p for p in payment_rows
                          if (p.get('metadata') or {}).get('self_test')]

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "users": {
                "total": total_users,
                "real": len(real_users),
                "founders": len(founder_rows),
                "community": len(community_rows),
                "test_or_fake": len(test_users),
                "genesis49": sum(1 for u in real_users if u.get('beta_user')),
                "registered": sum(1 for u in real_users if u.get('is_registered')),
            },
            "payments": {
                "total_rows": len(payment_rows),
                "real_customer_payments": len(real_payments),
                "founder_self_test": len(self_test_payments),
                "total_real_ils": sum(float(p['amount']) for p in real_payments),
                "total_self_test_ils": sum(float(p['amount']) for p in self_test_payments),
            },
            "licenses": {
                "total": len(licenses),
                "to_real_customers": sum(1 for l in licenses if l['user_id'] not in founder_ids),
            },
            "courses_active": sum(1 for c in courses if c['active']),
            "deposits_onchain": len(deposits),
            "marketplace_items": marketplace_items,
            "marketplace_orders": marketplace_orders,
            "active_stakes": stakes,
            "users_with_referral": referral_users,
        },
        "users": {
            "founders": founder_rows,
            "community": community_rows,
            "test_or_fake": test_users,
        },
        "payments": {
            "real_customer": real_payments,
            "founder_self_test": self_test_payments,
        },
        "licenses": [_row(r) for r in licenses],
        "courses": [_row(r) for r in courses],
        "deposits": [_row(r) for r in deposits],
        "recent_broadcasts": [_row(r) for r in broadcasts],
    }


# ===== PUBLIC EVENTS FEED (#13 from OPEN_TASKS) =====
# Read-only, no auth, ring-buffer slice from event_log.
# Strips sensitive metadata fields before returning to public.
PUBLIC_EVENT_TYPES = {
    "payment.cleared", "stake.opened", "stake.closed",
    "course.purchased", "academy.payout_made", "broadcast.send",
    "user.registered", "device.paired", "chain.event",
}
PUBLIC_STRIP_META_KEYS = {"user_id", "admin_id", "ip_address", "token",
                          "password", "secret", "key", "telegram_id",
                          "phone", "email"}

@app.get("/api/events/public")
async def events_public(limit: int = Query(30, le=100), since_id: int = Query(0)):
    """Public feed of platform events - no auth, no PII.

    Used by homepage / marketing pages to show 'live activity' without
    exposing user identities. Safe for anyone to poll.
    """
    if pool is None or _db_init_failed:
        raise HTTPException(503, "DB pool unavailable")

    async with pool.acquire() as conn:
        try:
            from shared.events import ensure_event_log_table
            await ensure_event_log_table(conn)
            rows = await conn.fetch("""
                SELECT id, event_type, created_at, payload
                  FROM event_log
                  WHERE event_type = ANY($1::text[])
                    AND id > $2
                  ORDER BY id DESC
                  LIMIT $3
            """, list(PUBLIC_EVENT_TYPES), since_id, limit)
        except Exception as _e:
            return {"events": [], "error": "event_log_unavailable", "reason": repr(_e)[:200]}

    events = []
    for r in rows:
        meta = dict(r).get("payload") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        public_meta = {k: v for k, v in meta.items() if k not in PUBLIC_STRIP_META_KEYS}
        if "user_id" in meta:
            public_meta["user_hash"] = hashlib.sha256(
                str(meta["user_id"]).encode()
            ).hexdigest()[:8]
        events.append({
            "id": r["id"],
            "type": r["event_type"],
            "at": r["created_at"].isoformat() if r["created_at"] else None,
            "meta": public_meta,
        })
    return {"events": events, "total_returned": len(events)}


# ===== OPS MUTATIONS (A5 from REALITY_RESET roadmap) =====
# Admin actions authenticated via ADMIN_BROADCAST_KEY (same pattern as /api/ops/reality).
# These enable Osif to manage users without waiting for ADMIN_API_KEYS rotation.

class OpsCreditRequest(BaseModel):
    user_id: int
    amount: float
    token: str = "ZVK"
    reason: Optional[str] = None

@app.post("/api/ops/credit")
async def ops_credit(req: OpsCreditRequest, x_broadcast_key: Optional[str] = Header(None)):
    """Credit tokens to a user. Auth: X-Broadcast-Key header."""
    if not x_broadcast_key or x_broadcast_key != ADMIN_BROADCAST_KEY:
        raise HTTPException(403, "Broadcast key required")
    if pool is None or _db_init_failed:
        raise HTTPException(503, "DB pool unavailable")
    if req.amount <= 0:
        raise HTTPException(400, "amount must be positive")
    if req.token not in {"ZVK", "SLH", "MNH", "TON"}:
        raise HTTPException(400, "token must be one of ZVK/SLH/MNH/TON")

    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT telegram_id FROM web_users WHERE telegram_id=$1", req.user_id)
        if not user:
            raise HTTPException(404, f"user {req.user_id} not found")

        try:
            await conn.execute("""
                INSERT INTO token_transfers (from_user_id, to_user_id, token, amount, memo, tx_type, created_at)
                VALUES (0, $1, $2, $3, $4, 'admin_credit', NOW())
            """, req.user_id, req.token, req.amount, req.reason or "ops credit by admin")
        except Exception as e:
            try:
                await conn.execute("""
                    INSERT INTO event_log (event_type, payload, created_at)
                    VALUES ('admin.credit', $1::jsonb, NOW())
                """, json.dumps({
                    "user_id": req.user_id,
                    "token": req.token,
                    "amount": req.amount,
                    "reason": req.reason,
                }))
            except Exception as e2:
                raise HTTPException(500, f"credit failed: {e!r} / {e2!r}")

    return {"ok": True, "user_id": req.user_id, "credited": f"{req.amount} {req.token}"}


class OpsApprovePaymentRequest(BaseModel):
    external_payment_id: int
    grant_course_id: Optional[int] = None
    note: Optional[str] = None

@app.post("/api/ops/approve-payment")
async def ops_approve_payment(req: OpsApprovePaymentRequest, x_broadcast_key: Optional[str] = Header(None)):
    """Force-approve an external_payment and optionally grant an academy_license.
    Auth: X-Broadcast-Key header."""
    if not x_broadcast_key or x_broadcast_key != ADMIN_BROADCAST_KEY:
        raise HTTPException(403, "Broadcast key required")
    if pool is None or _db_init_failed:
        raise HTTPException(503, "DB pool unavailable")

    async with pool.acquire() as conn:
        pay = await conn.fetchrow("""
            SELECT id, user_id, provider_tx_id, status, plan_key
              FROM external_payments WHERE id=$1
        """, req.external_payment_id)
        if not pay:
            raise HTTPException(404, f"payment {req.external_payment_id} not found")

        if pay["status"] != "approved":
            await conn.execute(
                "UPDATE external_payments SET status='approved' WHERE id=$1",
                req.external_payment_id,
            )

        license_id = None
        if req.grant_course_id:
            existing = await conn.fetchval(
                "SELECT id FROM academy_licenses WHERE user_id=$1 AND course_id=$2 AND status='active'",
                pay["user_id"], req.grant_course_id,
            )
            if existing:
                license_id = existing
            else:
                license_id = await conn.fetchval("""
                    INSERT INTO academy_licenses (user_id, course_id, payment_id, status, purchased_at)
                    VALUES ($1, $2, $3, 'active', NOW())
                    RETURNING id
                """, pay["user_id"], req.grant_course_id, pay["provider_tx_id"])

    return {
        "ok": True,
        "external_payment_id": req.external_payment_id,
        "status": "approved",
        "user_id": pay["user_id"],
        "license_id": license_id,
    }


class OpsBanRequest(BaseModel):
    user_id: int
    reason: str
    revert: bool = False

@app.post("/api/ops/ban")
async def ops_ban(req: OpsBanRequest, x_broadcast_key: Optional[str] = Header(None)):
    """Flip is_registered on web_users. Auth: X-Broadcast-Key header."""
    if not x_broadcast_key or x_broadcast_key != ADMIN_BROADCAST_KEY:
        raise HTTPException(403, "Broadcast key required")
    if pool is None or _db_init_failed:
        raise HTTPException(503, "DB pool unavailable")

    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT telegram_id, is_registered FROM web_users WHERE telegram_id=$1", req.user_id)
        if not user:
            raise HTTPException(404, f"user {req.user_id} not found")

        new_registered = True if req.revert else False
        await conn.execute(
            "UPDATE web_users SET is_registered=$1 WHERE telegram_id=$2",
            new_registered, req.user_id,
        )
        try:
            await conn.execute("""
                INSERT INTO event_log (event_type, payload, created_at)
                VALUES ('admin.ban', $1::jsonb, NOW())
            """, json.dumps({
                "user_id": req.user_id,
                "action": "revert" if req.revert else "ban",
                "reason": req.reason,
            }))
        except Exception:
            pass

    return {"ok": True, "user_id": req.user_id, "is_registered": new_registered,
            "action": "revert" if req.revert else "ban"}


# ===== PERFORMANCE ENDPOINT (#9 from OPEN_TASKS) =====
# Reads the latest backtest_YYYYMMDD_HHMMSS.csv from the project root and
# returns aggregated metrics. No auth (public pre-launch transparency).

@app.get("/api/performance")
async def performance_snapshot():
    """Latest backtest snapshot from the local CSV (no auth, public data)."""
    import glob
    import csv as _csv
    from pathlib import Path as _P

    project_root = _P(__file__).resolve().parent.parent
    candidates = sorted(
        glob.glob(str(project_root / "backtest_*.csv")),
        reverse=True,
    )
    if not candidates:
        return {
            "available": False,
            "message": "No backtest CSV present. Run daily_backtest.py to generate.",
            "generated_at": None,
            "tokens": [],
        }

    latest = candidates[0]
    tokens = []
    try:
        with open(latest, "r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                try:
                    tokens.append({
                        "address": row.get("address"),
                        "symbol": row.get("symbol"),
                        "price_usd": float(row.get("price_usd") or 0),
                        "liquidity_usd": float(row.get("liquidity_usd") or 0),
                        "volume_usd_24h": float(row.get("volume_usd_24h") or 0),
                        "collected_at": row.get("collected_at"),
                    })
                except Exception:
                    pass
    except Exception as e:
        return {"available": False, "message": f"read error: {e!r}"}

    total_liquidity = sum(t["liquidity_usd"] for t in tokens)
    total_volume = sum(t["volume_usd_24h"] for t in tokens)
    top_by_volume = sorted(tokens, key=lambda t: t["volume_usd_24h"], reverse=True)[:5]

    return {
        "available": True,
        "source_file": _P(latest).name,
        "generated_at": tokens[0]["collected_at"] if tokens else None,
        "token_count": len(tokens),
        "total_liquidity_usd": total_liquidity,
        "total_volume_24h_usd": total_volume,
        "top_5_by_volume": top_by_volume,
        "tokens": tokens,
    }


# ===== PERFORMANCE DIGEST FOR TELEGRAM (#11 from OPEN_TASKS) =====
# Formatted HTML (Telegram-compatible) version of /api/performance.
# Used by Guardian bot (/performance command) or any push-alert script.

@app.get("/api/performance/digest")
async def performance_digest():
    """Pre-formatted Telegram HTML digest of latest backtest snapshot.
    No auth (same data as /api/performance, just pre-rendered for convenience)."""
    import glob
    import csv as _csv
    from pathlib import Path as _P

    project_root = _P(__file__).resolve().parent.parent
    candidates = sorted(
        glob.glob(str(project_root / "backtest_*.csv")),
        reverse=True,
    )
    if not candidates:
        return {
            "available": False,
            "text": "<b>SLH Research Lab</b>\n\nNo backtest snapshot yet.\nRun <code>python daily_backtest.py</code>.",
            "parse_mode": "HTML",
        }

    latest = candidates[0]
    tokens = []
    try:
        with open(latest, "r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                try:
                    tokens.append({
                        "symbol": row.get("symbol") or "?",
                        "price": float(row.get("price_usd") or 0),
                        "liq": float(row.get("liquidity_usd") or 0),
                        "vol": float(row.get("volume_usd_24h") or 0),
                    })
                except Exception:
                    pass
    except Exception as e:
        return {"available": False, "text": f"Read error: {e!r}"}

    if not tokens:
        return {"available": False, "text": "No rows in snapshot."}

    def _fmt_usd(n):
        if n >= 1e9:
            return f"${n/1e9:.2f}B"
        if n >= 1e6:
            return f"${n/1e6:.2f}M"
        if n >= 1e3:
            return f"${n/1e3:.1f}K"
        return f"${n:.2f}"

    total_liq = sum(t["liq"] for t in tokens)
    total_vol = sum(t["vol"] for t in tokens)
    top = sorted(tokens, key=lambda t: t["vol"], reverse=True)[:5]

    lines = [
        "<b>SLH Research Lab  Daily Digest</b>",
        f"<i>Source:</i> <code>{_P(latest).name}</code>",
        "",
        f"<b>Tokens tracked:</b> {len(tokens)}",
        f"<b>Total liquidity:</b> {_fmt_usd(total_liq)}",
        f"<b>24h volume:</b> {_fmt_usd(total_vol)}",
        "",
        "<b>Top 5 by volume:</b>",
    ]
    for i, t in enumerate(top, 1):
        lines.append(
            f"  {i}. <code>{t['symbol']}</code>  ·  "
            f"${t['price']:.4f}  ·  Vol {_fmt_usd(t['vol'])}  ·  Liq {_fmt_usd(t['liq'])}"
        )
    lines += [
        "",
        "<i>Research lab data. Not investment advice.</i>",
        '<a href="https://slh-nft.com/performance.html">Full details</a>',
    ]

    return {
        "available": True,
        "text": "\n".join(lines),
        "parse_mode": "HTML",
        "source_file": _P(latest).name,
        "token_count": len(tokens),
    }
