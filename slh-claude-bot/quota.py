"""
SLH AI Spark — Quota middleware (FREE UNLIMITED MODE).

All users get unlimited messages. No Anthropic, no paid tiers.
AI provider is always free (Groq/Gemini via free_ai_client).

Usage in bot.py on_text:

    decision = await quota.check(user_id)
    if not decision.allowed:
        await msg.answer(decision.refusal_he)
        return
    # ... call AI client (decision.use_anthropic is always False) ...
    await quota.record(user_id, chat_id, ...)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import subscriptions


@dataclass
class QuotaDecision:
    allowed: bool
    tier: str                       # always 'free'
    use_anthropic: bool             # always False
    refusal_he: Optional[str]
    quota_remaining: int
    quota_total: int


async def check(user_id: int) -> QuotaDecision:
    """Pre-flight check — always allows, always free."""
    # Still create/fetch a subscription row for usage tracking
    try:
        sub = await subscriptions.get_or_create(user_id)
        used = sub.messages_used_this_period
    except Exception:
        used = 0

    return QuotaDecision(
        allowed=True,
        tier="free",
        use_anthropic=False,
        refusal_he=None,
        quota_remaining=999999,
        quota_total=999999,
    )


async def record(user_id: int, chat_id: int, tier: str, provider: str,
                 model: Optional[str], tokens_in: int, tokens_out: int,
                 cost_usd_cents: int) -> None:
    """Post-flight: log usage + bump counter (for analytics only, no enforcement)."""
    try:
        await subscriptions.record_usage(
            user_id=user_id,
            chat_id=chat_id,
            tier="free",
            provider=provider,
            model=model,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd_cents=0,  # always free
        )
        await subscriptions.increment_usage_counter(user_id)
    except Exception:
        pass  # analytics failure should never block the user


async def quota_status_he(user_id: int) -> str:
    """For /credits command — show usage stats."""
    try:
        sub = await subscriptions.get_or_create(user_id)
        used = sub.messages_used_this_period
    except Exception:
        used = 0

    return (
        f"📊 *Tier: Free Unlimited*\n\n"
        f"השתמשת ב-{used} הודעות החודש\n"
        f"ללא הגבלה ✅\n\n"
        f"AI: Groq / Gemini (חינם)"
    )
