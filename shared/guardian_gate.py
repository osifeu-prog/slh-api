"""
Guardian Gate — single source of truth for "is this user allowed to transact?".

Every money-moving endpoint MUST call `require_clean_zuz(pool, user_id)` before
writing balances, opening stakes, or processing payments.

Reads `guardian_blacklist` directly from the slh_main DB.
In-process: 60s TTL cache to avoid hammering the table.

Usage (Railway side):
    from shared.guardian_gate import require_clean_zuz

    @app.post("/api/wallet/send")
    async def wallet_send(req: SendReq):
        await require_clean_zuz(pool, req.user_id)
        ...

Usage (bot side):
    from shared.guardian_gate import check_zuz
    status = await check_zuz(pool, user_id)
    if status["blocked"]:
        await m.answer(status["reason_he"])
        return
"""
from __future__ import annotations

import time
from typing import Optional
from fastapi import HTTPException

ZUZ_BLOCK_THRESHOLD = 100.0
ZUZ_WARN_THRESHOLD = 50.0

_cache: dict = {}
_cache_ttl = 60.0


async def check_zuz(pool, user_id: int, *, bypass_cache: bool = False) -> dict:
    """
    Returns: {
        "blocked": bool,           # True if ZUZ >= 100 or ban_active
        "warn": bool,              # True if 50 <= ZUZ < 100
        "zuz_score": float,
        "reason": str,             # English
        "reason_he": str,          # Hebrew for bot DMs
    }
    """
    now = time.time()
    key = int(user_id)

    if not bypass_cache:
        cached = _cache.get(key)
        if cached and cached["_t"] + _cache_ttl > now:
            return {k: v for k, v in cached.items() if not k.startswith("_")}

    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT zuz_score, ban_active, auto_banned, ban_reason "
                "FROM guardian_blacklist WHERE user_id = $1",
                user_id
            )
    except Exception as e:
        # Transaction authorization is fail-closed: an unavailable Guardian
        # must never be treated as a clean account on a money-moving path.
        print(f"[guardian_gate][ERROR] DB check failed for {user_id}: {e!r}")
        raise HTTPException(
            status_code=503,
            detail={"error": "guardian_unavailable", "reason": "Guardian check unavailable"},
        )

    if not row:
        result = {"blocked": False, "warn": False, "zuz_score": 0.0,
                  "reason": "clean", "reason_he": ""}
    else:
        score = float(row["zuz_score"] or 0)
        banned = bool(row["ban_active"] or row["auto_banned"])
        blocked = banned or score >= ZUZ_BLOCK_THRESHOLD
        warn = (not blocked) and score >= ZUZ_WARN_THRESHOLD
        reason = row["ban_reason"] or f"ZUZ score {score:.0f}"
        reason_he = (
            f"🛡 החשבון שלך סומן ב-Guardian (ZUZ {score:.0f}). "
            f"הפעולה חסומה. לערעור פנה/י ל-@osifeu_prog."
            if blocked else
            f"⚠️ שים/י לב: ציון ZUZ {score:.0f}. פעולה מותרת אבל בחודש הזה פעולות נוספות עלולות להיחסם."
            if warn else ""
        )
        result = {"blocked": blocked, "warn": warn, "zuz_score": score,
                  "reason": reason, "reason_he": reason_he}

    _cache[key] = {**result, "_t": now}
    return result


async def require_clean_zuz(pool, user_id: int, *, admin_override_header: Optional[str] = None) -> None:
    """FastAPI-friendly: raises 403 HTTPException if blocked."""
    # Caller-supplied override headers are never trusted here. Administrative
    # authorization belongs at the endpoint boundary; Guardian remains a
    # mandatory safety check for every transaction path.
    status = await check_zuz(pool, user_id)
    if status["blocked"]:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "zuz_blocked",
                "zuz_score": status["zuz_score"],
                "reason": status["reason"],
                "reason_he": status["reason_he"],
            }
        )


def invalidate_cache(user_id: Optional[int] = None) -> None:
    """Drop cache entry (call after Guardian report to force re-check)."""
    if user_id is None:
        _cache.clear()
    else:
        _cache.pop(int(user_id), None)
