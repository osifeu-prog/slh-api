"""Free AI client — 100% free, zero credit card, zero Anthropic.

Provider chain (first available key wins):
  1. Groq  → llama-3.3-70b-versatile  (30 RPM / 14,400 RPD free tier)
  2. Gemini → gemini-2.0-flash          (15 RPM / 1 M TPD free tier)
  3. Railway API → /api/ai/chat         (internal fallback, uses Groq/Gemini on server)
  4. Built-in local response             (always works, no key needed)

Get free keys (2 min each, no credit card):
  Groq:   https://console.groq.com  → "Create API Key"
  Gemini: https://aistudio.google.com → "Get API key"

Set in slh-claude-bot/.env:
  GROQ_API_KEY=gsk_...
  GEMINI_API_KEY=AIza...
"""
from __future__ import annotations

import logging
import os
from typing import List, Tuple

import httpx

log = logging.getLogger("slh-claude-bot.free-ai")

# ── Config ──────────────────────────────────────────────────────────────────
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
RAILWAY_BASE   = os.getenv("SLH_API_BASE", "https://slh-api-production.up.railway.app")
TIMEOUT        = float(os.getenv("SLH_AI_TIMEOUT", "30"))

GROQ_MODEL_PRIMARY  = "llama-3.3-70b-versatile"
GROQ_MODEL_FALLBACK = "llama-3.1-8b-instant"
GEMINI_MODEL        = "gemini-2.0-flash"

SYSTEM_PROMPT = (
    "אתה SLH Spark AI — עוזר אישי חכם של אוסיף ומשתמשי SLH Spark.\n"
    "ענה תמיד בעברית, קצר ופרקטי. אל תציג את עצמך בכל תשובה.\n"
    "רקע: SLH Spark = פלטפורמת קריפטו ישראלית עם 26 בוטים בטלגרם, "
    "אתר slh-nft.com, API על Railway, טוקן SLH על BSC, ו-TON chain.\n"
    "אם המשתמש שואל על docker/git/deploy — הצע פקודות: /ps /logs /git /health /control\n"
    "אם לא יודע — אמור זאת ישירות במקום להמציא."
)

# ── Provider 1: Groq ─────────────────────────────────────────────────────────
async def _call_groq(messages: list[dict], model: str = GROQ_MODEL_PRIMARY) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": 1024,
        "temperature": 0.7,
    }
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        reply = data["choices"][0]["message"]["content"].strip()
        used_model = data.get("model", model)
        return f"{reply}\n\n_[Groq · {used_model}]_"


# ── Provider 2: Gemini ───────────────────────────────────────────────────────
async def _call_gemini(messages: list[dict]) -> str:
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    # Convert OpenAI-style messages to Gemini format
    parts = []
    for m in messages:
        role = "user" if m["role"] == "user" else "model"
        parts.append({"role": role, "parts": [{"text": m["content"]}]})

    payload = {"contents": parts}
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        reply = data["candidates"][0]["content"]["parts"][0]["text"].strip()
        return f"{reply}\n\n_[Gemini · {GEMINI_MODEL}]_"


# ── Provider 3: Railway API (internal Groq/Gemini) ───────────────────────────
async def _call_railway(user_text: str, context: str) -> str:
    url = RAILWAY_BASE.rstrip("/") + "/api/ai/chat"
    full_msg = (SYSTEM_PROMPT + "\n\n" + context + "\n\n" + user_text) if context else (SYSTEM_PROMPT + "\n\n" + user_text)
    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(url, json={"message": full_msg, "user_id": "slh_bot", "lang": "he"})
        resp.raise_for_status()
        data = resp.json()
        reply = data.get("reply") or data.get("message") or "(ריק)"
        model = data.get("model", "railway")
        return f"{reply}\n\n_[{model}]_"


# ── Provider 4: Local fallback ───────────────────────────────────────────────
_LOCAL_REPLY = (
    "⚙️ אין חיבור ל-AI כרגע.\n\n"
    "הוסף מפתח חינמי ל-slh-claude-bot/.env:\n"
    "`GROQ_API_KEY=gsk_...` ← console.groq.com (חינם)\n"
    "`GEMINI_API_KEY=AIza...` ← aistudio.google.com (חינם)\n\n"
    "פקודות שעובדות ללא AI:\n"
    "/ps /logs /git /health /control /price /devices"
)


# ── Public API ───────────────────────────────────────────────────────────────
def _build_messages(history: List[dict], user_text: str) -> list[dict]:
    """Build OpenAI-compatible message list with system prompt."""
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
    for m in history[-10:]:  # last 5 turns
        role = m.get("role", "user")
        content = m.get("content", "")
        if isinstance(content, list):
            content = " ".join(
                b.get("text", "") if isinstance(b, dict) else str(b) for b in content
            )
        if role in ("user", "assistant") and content.strip():
            msgs.append({"role": role, "content": content})
    msgs.append({"role": "user", "content": user_text})
    return msgs


async def converse(
    history: List[dict],
    user_text: str,
    tier_mode: str = "free",  # kept for API compat, ignored
) -> Tuple[str, List[dict]]:
    """Main entry point. Tries Groq → Gemini → Railway → local fallback."""
    msgs = _build_messages(history, user_text)
    reply = None

    # 1. Groq
    if GROQ_API_KEY and GROQ_API_KEY.startswith("gsk_"):
        try:
            reply = await _call_groq(msgs, GROQ_MODEL_PRIMARY)
            log.info("Groq answered (%d chars)", len(reply))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                log.warning("Groq rate-limited, trying fallback model")
                try:
                    reply = await _call_groq(msgs, GROQ_MODEL_FALLBACK)
                except Exception as e2:
                    log.warning("Groq fallback failed: %s", e2)
            else:
                log.warning("Groq error %s: %s", e.response.status_code, e.response.text[:200])
        except Exception as e:
            log.warning("Groq failed: %s", e)

    # 2. Gemini
    if reply is None and GEMINI_API_KEY and GEMINI_API_KEY.startswith("AIza"):
        try:
            reply = await _call_gemini(msgs)
            log.info("Gemini answered (%d chars)", len(reply))
        except Exception as e:
            log.warning("Gemini failed: %s", e)

    # 3. Railway internal API
    if reply is None:
        try:
            context = "\n".join(
                f"[{m['role']}] {m['content']}"
                for m in msgs[1:-1]  # skip system + last user
                if isinstance(m.get("content"), str)
            )
            reply = await _call_railway(user_text, context)
            log.info("Railway API answered (%d chars)", len(reply))
        except Exception as e:
            log.warning("Railway API failed: %s", e)

    # 4. Local fallback
    if reply is None:
        reply = _LOCAL_REPLY

    new_msgs = [
        {"role": "user",      "content": user_text},
        {"role": "assistant", "content": reply},
    ]
    return reply, new_msgs


# Backwards-compat alias
async def chat(
    history: List[dict],
    user_text: str,
    tier_mode: str = "free",
) -> Tuple[str, List[dict]]:
    return await converse(history, user_text, tier_mode=tier_mode)
