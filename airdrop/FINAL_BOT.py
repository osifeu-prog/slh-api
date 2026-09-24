#!/usr/bin/env python3
"""
SLH_AIR bot (@SLH_AIR_bot) — companion bot for slh-nft.com

2026-09-23 fixes:
- /buy no longer depends on the dead legacy airdrop API. Payment is verified
  on-chain (TON) by a per-user comment code; /paid checks the chain.
- No hardcoded secrets: TELEGRAM_TOKEN and ADMIN_API_KEY come from env only.
- ADMIN_ID is deterministic (ADMIN_ID env first) — before, it was picked from
  an unordered set and could change between restarts.
- Login link (JWT) is sent only on /login, not in every /start message.
- Removed the legacy username/tx-hash state machine that swallowed any text.
"""

import io
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone

import requests

# ====================
# CONFIGURATION
# ====================
TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("AIRDROP_BOT_TOKEN")
if not TOKEN:
    sys.exit("TELEGRAM_TOKEN is not set")

SLH_API_BASE = os.getenv("SLH_API_URL", "https://slh-api-production.up.railway.app")
BOT_SYNC_SECRET = os.getenv("BOT_SYNC_SECRET", "")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")
TELEGRAM_LINK_SECRET = os.getenv("TELEGRAM_LINK_SECRET", "")
SLH_THERAPISTS_API = os.getenv(
    "SLH_THERAPISTS_API", f"{SLH_API_BASE}/api/therapists/telegram/link"
)

# Admins: ADMIN_ID (primary, receives notifications) + optional ADMIN_IDS csv.
_admins = []
for raw in [os.getenv("ADMIN_ID", "")] + (os.getenv("ADMIN_IDS", "")).split(","):
    raw = raw.strip()
    if raw and raw not in _admins:
        _admins.append(raw)
if not _admins:
    sys.exit("ADMIN_ID is not set")
ADMIN_IDS = set(_admins)
ADMIN_ID = _admins[0]

SUPPORT_CONTACT = os.getenv("SUPPORT_CONTACT", "@osifeu_prog")

# Genesis Pack payment
TON_WALLET = os.getenv("TON_WALLET", "UQCr743gEr_nqV_0SBkSp3CtYS_15R3LDLBvLmKeEv7XdGvp")
PACK_PRICE_TON = float(os.getenv("PACK_PRICE_TON", "44.4"))
PACK_SLH = int(os.getenv("PACK_SLH", "1000"))
PACK_PRICE_NANO = int(round(PACK_PRICE_TON * 1_000_000_000))
TONCENTER_URL = os.getenv("TONCENTER_URL", "https://toncenter.com/api/v2")
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "")  # optional, raises rate limit

# ====================
# LOGGING
# ====================
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
logging.basicConfig(
    format="%(asctime)s - SLH BOT - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ====================
# PROCESSED-TX STORE (prevents one payment being counted twice)
# Uses Redis if available, otherwise memory (lost on restart).
# ====================
_processed_mem = set()
_redis = None
try:
    import redis  # type: ignore

    if os.getenv("REDIS_URL"):
        _redis = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        _redis.ping()
        logger.info("Redis connected — processed payments are persistent")
except Exception as e:  # noqa: BLE001
    _redis = None
    logger.warning(f"Redis unavailable ({e!r}) — using in-memory store")


def tx_already_processed(tx_hash: str) -> bool:
    if _redis:
        return bool(_redis.sismember("slh:paid_tx", tx_hash))
    return tx_hash in _processed_mem


def mark_tx_processed(tx_hash: str) -> None:
    if _redis:
        _redis.sadd("slh:paid_tx", tx_hash)
    _processed_mem.add(tx_hash)


# ====================
# TELEGRAM HELPERS
# ====================
def send_message(chat_id, text, parse_mode="HTML"):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if r.status_code != 200:
            logger.error(f"sendMessage {r.status_code}: {r.text[:200]}")
        return r.status_code == 200
    except Exception as e:  # noqa: BLE001
        logger.error(f"Telegram error: {e!r}")
        return False


def notify_admin(text):
    send_message(ADMIN_ID, text)


# ====================
# SLH API
# ====================
def slh_bot_sync(chat_id, name, username, referrer_id=None):
    """Upsert the user in SLH DB (single source of truth). Returns dict or None."""
    if not BOT_SYNC_SECRET:
        logger.warning("BOT_SYNC_SECRET unset — skipping bot-sync")
        return None
    try:
        r = requests.post(
            f"{SLH_API_BASE}/api/auth/bot-sync",
            json={
                "telegram_id": int(chat_id),
                "username": (username or "").lstrip("@"),
                "first_name": name or "",
                "photo_url": "",
                "referrer_id": referrer_id,
                "bot_secret": BOT_SYNC_SECRET,
            },
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
        logger.error(f"bot-sync HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:  # noqa: BLE001
        logger.error(f"bot-sync failed: {e!r}")
    return None


def get_member_card(chat_id):
    try:
        r = requests.get(f"{SLH_API_BASE}/api/member-card/{chat_id}", timeout=10)
        if r.status_code == 200:
            return r.json()
        if r.status_code != 404:
            logger.error(f"member-card {chat_id} HTTP {r.status_code}")
    except Exception as e:  # noqa: BLE001
        logger.error(f"member-card failed: {e!r}")
    return None


# ====================
# TON PAYMENT VERIFICATION
# ====================
def payment_code(chat_id) -> str:
    return f"SLH{chat_id}"


def find_payment(chat_id):
    """
    Look for an incoming transfer to TON_WALLET with comment SLH<chat_id>
    and value >= pack price, not processed before.
    Returns (tx_hash, amount_ton, utime) or None. Raises on network error.
    """
    headers = {"X-API-Key": TONCENTER_API_KEY} if TONCENTER_API_KEY else {}
    r = requests.get(
        f"{TONCENTER_URL}/getTransactions",
        params={"address": TON_WALLET, "limit": 100, "archival": "true"},
        headers=headers,
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"toncenter: {str(data)[:200]}")

    code = payment_code(chat_id).lower()
    for tx in data.get("result", []):
        in_msg = tx.get("in_msg") or {}
        comment = (in_msg.get("message") or "").strip().lower()
        if comment != code:
            continue
        try:
            value = int(in_msg.get("value") or 0)
        except ValueError:
            continue
        if value < PACK_PRICE_NANO:
            continue
        tx_hash = (tx.get("transaction_id") or {}).get("hash", "")
        if not tx_hash or tx_already_processed(tx_hash):
            continue
        return tx_hash, value / 1_000_000_000, tx.get("utime", 0)
    return None


# ====================
# MESSAGES
# ====================
def welcome_message(name, username=""):
    uname = f" (@{username})" if username else ""
    return f"""
🌟 <b>ברוך הבא ל-SLH Spark!</b>

👤 {name}{uname}

🌐 <b>אתר הקהילה:</b>
https://slh-nft.com

📋 <b>פקודות זמינות:</b>
/me — הפרופיל שלך + יתרות
/dashboard — לוח בקרה אישי
/login — קישור התחברות לאתר
/therapists — ספריית מטפלים
/buy — רכישת SLH (Genesis Pack)
/help — רשימת פקודות מלאה

⚠️ זהו פרויקט בשלב Pre-Launch. גילוי סיכון מלא: https://slh-nft.com/risk.html
"""


def help_message():
    return f"""
📖 <b>פקודות הבוט</b>

<b>חשבון:</b>
/me — פרופיל + טוקנים + סטטוס מטפל
/dashboard — לוח בקרה אישי
/login — קישור התחברות לאתר

<b>קהילה:</b>
/therapists — ספריית מטפלים מאושרים
/courses — קורסים
/blog — בלוג יומי

<b>מערכת:</b>
/bots — בוטי SLH
/swarm — מצב Swarm + Brain

<b>תשלומים:</b>
/buy — רכישת Genesis Pack ({PACK_SLH:,} SLH ב-{PACK_PRICE_TON:g} TON)
/paid — אימות תשלום אחרי ששלחת
/wallet — הארנק שלך

<b>תמיכה:</b>
/support — צור קשר
/admin — פאנל ניהול (לאדמין בלבד)
"""


def buy_message(chat_id):
    return f"""
💸 <b>רכישת Genesis Pack</b>
{PACK_SLH:,} SLH תמורת <b>{PACK_PRICE_TON:g} TON</b>

🏦 <b>שלח לכתובת:</b>
<code>{TON_WALLET}</code>

📝 <b>בשדה ההערה (Comment/Memo) כתוב בדיוק:</b>
<code>{payment_code(chat_id)}</code>

⚠️ בלי ההערה הזו לא נוכל לזהות שהתשלום שלך.

✅ אחרי שהעסקה נשלחה, חכה כדקה ושלח /paid — הבוט יבדוק אותה ישירות בבלוקצ'יין.

⚠️ גילוי סיכון: https://slh-nft.com/risk.html
שאלות? {SUPPORT_CONTACT} (לעולם לא נפנה אליך ראשונים ב-DM)
"""


# ====================
# THERAPISTS DEEP-LINK
# ====================
def link_therapist(application_id, telegram_id):
    if not TELEGRAM_LINK_SECRET:
        return False, "⚠️ <b>חיבור הטלגרם זמנית לא זמין.</b> נסה שוב מאוחר יותר."
    try:
        resp = requests.post(
            SLH_THERAPISTS_API,
            headers={"X-Bot-Secret": TELEGRAM_LINK_SECRET},
            json={
                "telegram_id": int(telegram_id),
                "application_id": int(application_id),
                "kind": "therapist",
            },
            timeout=10,
        )
        if resp.status_code == 200:
            if resp.json().get("idempotent"):
                return True, f"✅ <b>כבר חובר</b>\n\nהחשבון שלך כבר מקושר לאפליקציה #{application_id}."
            return True, (
                f"✅ <b>חוברת בהצלחה!</b>\n\n"
                f"החשבון שלך מקושר לאפליקציית מטפל #{application_id}. "
                f"מעכשיו תקבל כאן התראות על פגישות, אישורים ותשלומים."
            )
        if resp.status_code == 404:
            return False, f"❌ <b>אפליקציה #{application_id} לא נמצאה.</b> פנה ל-{SUPPORT_CONTACT}"
        if resp.status_code == 400:
            return False, "⏳ <b>האפליקציה עדיין לא אושרה.</b> אחרי האישור תוכל לחבר שוב."
        logger.error(f"therapist link HTTP {resp.status_code}: {resp.text[:200]}")
        return False, f"❌ שגיאה (HTTP {resp.status_code})"
    except Exception as e:  # noqa: BLE001
        logger.error(f"therapist link failed: {e!r}")
        return False, "❌ שגיאת רשת, נסה שוב."


# ====================
# HEARTBEAT
# ====================
def heartbeat_loop():
    if not BOT_SYNC_SECRET:
        logger.info("[heartbeat] BOT_SYNC_SECRET not set — skipping")
        return
    while True:
        try:
            requests.post(
                f"{SLH_API_BASE}/api/bots/heartbeat",
                json={
                    "bot_name": "slh-air-bot",
                    "display_name": "SLH Companion (@SLH_AIR_bot)",
                    "username": "SLH_AIR_bot",
                    "version": "2026.09.23",
                    "metadata": {"container": "slh-airdrop", "polling": True},
                },
                headers={"X-Bot-Secret": BOT_SYNC_SECRET},
                timeout=8,
            )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[heartbeat] failed: {e!r}")
        time.sleep(60)


# ====================
# COMMAND HANDLERS
# ====================
def cmd_start(chat_id, name, username, arg):
    referrer_id = None
    if arg.startswith("therapist_"):
        try:
            app_id = int(arg.split("_", 1)[1])
        except (ValueError, IndexError):
            send_message(chat_id, "❌ קישור לא תקין. ודא שלחצת על הקישור המלא מהאתר.")
            return
        slh_bot_sync(chat_id, name, username)
        ok, msg = link_therapist(app_id, chat_id)
        send_message(chat_id, msg)
        if ok and str(chat_id) not in ADMIN_IDS:
            notify_admin(f"🩺 מטפל חיבר טלגרם: app #{app_id} ↔ tg {chat_id} ({name})")
        return
    if arg.startswith("ref_"):
        try:
            referrer_id = int(arg.split("_", 1)[1])
        except (ValueError, IndexError):
            referrer_id = None

    sync = slh_bot_sync(chat_id, name, username, referrer_id)
    send_message(chat_id, welcome_message(name, username))
    if str(chat_id) not in ADMIN_IDS:
        status = "✅ סונכרן" if sync else "⚠️ sync נכשל"
        notify_admin(f"👤 משתמש פתח את הבוט ({status}):\n{name} (@{username or '—'})\nID: {chat_id}")


def cmd_login(chat_id, name, username):
    sync = slh_bot_sync(chat_id, name, username)
    if sync and sync.get("login_url"):
        send_message(
            chat_id,
            "🔐 <b>קישור התחברות אישי</b>\n\n"
            f"{sync['login_url']}\n\n"
            "⚠️ הקישור מחבר ישירות לחשבון שלך — אל תעביר אותו לאף אחד.",
        )
    else:
        send_message(chat_id, "⚠️ לא הצלחתי ליצור קישור התחברות כרגע. נסה שוב בעוד דקה.")


def cmd_me(chat_id):
    resp = get_member_card(chat_id)
    card = resp.get("card") if isinstance(resp, dict) else None
    if not card:
        send_message(
            chat_id,
            "❓ <b>לא נמצא חשבון</b>\n\nשלח /start כדי לפתוח חשבון, או /login להתחברות לאתר.",
        )
        return
    is_th = "✅ כן" if card.get("is_therapist") else "—"
    genesis = "✅" if card.get("genesis_contributor") else "—"
    send_message(
        chat_id,
        f"""
👤 <b>{card.get('name') or 'משתמש'}</b> · #{card.get('nft_number', '—')}

🆔 <code>{chat_id}</code>
🏆 רמה: <b>{card.get('tier', '—')}</b>
⭐ REP: {card.get('rep_score', 0)}
💎 SLH: <b>{card.get('slh_balance', 0)}</b>
🪙 ZVK: <b>{card.get('zvk_balance', 0)}</b>
👥 הפניות: {card.get('referrals', 0)}
🩺 מטפל מאושר: {is_th}
🎟️ Genesis: {genesis}
📅 הצטרף: {card.get('joined') or '—'}

🔗 /dashboard — לוח בקרה
""",
    )


def cmd_paid(chat_id, name, username):
    send_message(chat_id, "🔎 בודק את הבלוקצ'יין...")
    try:
        found = find_payment(chat_id)
    except Exception as e:  # noqa: BLE001
        logger.error(f"toncenter check failed: {e!r}")
        send_message(chat_id, "⚠️ לא הצלחתי לבדוק את הרשת כרגע. נסה שוב בעוד דקה.")
        return
    if not found:
        send_message(
            chat_id,
            "⏳ <b>עוד לא מצאתי את התשלום.</b>\n\n"
            f"ודא שכתבת בהערה בדיוק: <code>{payment_code(chat_id)}</code>\n"
            f"ושהסכום הוא לפחות {PACK_PRICE_TON:g} TON.\n"
            "עסקאות לפעמים לוקחות דקה-שתיים — נסה /paid שוב.",
        )
        return
    tx_hash, amount, utime = found
    mark_tx_processed(tx_hash)
    when = datetime.fromtimestamp(utime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    send_message(
        chat_id,
        f"✅ <b>התשלום אומת בבלוקצ'יין!</b>\n\n"
        f"💰 {amount:g} TON · {when}\n"
        f"🎁 {PACK_SLH:,} SLH ייזקפו לחשבונך תוך 24 שעות.\n\n"
        f"שאלות? {SUPPORT_CONTACT}",
    )
    notify_admin(
        f"💰 <b>תשלום Genesis אומת on-chain</b>\n\n"
        f"👤 {name} (@{username or '—'})\n🆔 <code>{chat_id}</code>\n"
        f"💰 {amount:g} TON · {when}\n"
        f"📝 hash: <code>{tx_hash}</code>\n\n"
        f"👉 לזקוף {PACK_SLH:,} SLH לחשבון."
    )


def cmd_bots(chat_id):
    live = []
    if ADMIN_API_KEY:
        try:
            r = requests.get(
                f"{SLH_API_BASE}/api/bots/list",
                headers={"X-Admin-Key": ADMIN_API_KEY},
                timeout=5,
            )
            if r.status_code == 200:
                live = r.json().get("bots", [])
            else:
                logger.error(f"bots/list HTTP {r.status_code}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"bots/list failed: {e!r}")
    if live:
        lines = []
        for b in live:
            uname = b.get("username") or b.get("bot_name") or "?"
            label = b.get("display_name") or ""
            lines.append(f"🟢 @{uname} — {label}")
        body = "\n".join(lines)
    else:
        body = "🟢 @SLH_AIR_bot — הבוט הראשי (זה)"
    send_message(chat_id, f"🤖 <b>SLH Bots</b>\n\n{body}\n\n📊 https://slh-nft.com/admin/bot-registry.html")


def cmd_swarm(chat_id):
    total = online = events = "?"
    try:
        r = requests.get(f"{SLH_API_BASE}/api/swarm/stats", timeout=5)
        if r.status_code == 200:
            s = r.json()
            total = s.get("total_devices", "?")
            online = s.get("online", "?")
            events = s.get("events_24h", "?")
    except Exception:  # noqa: BLE001
        pass
    brain_line = ""
    try:
        r = requests.get(f"{SLH_API_BASE}/api/brain/state", timeout=5)
        if r.status_code == 200:
            b = r.json()
            st = b.get("system_state", "?")
            emoji = {"HEALTHY": "🟢", "DEGRADED": "🟠"}.get(st, "🔴")
            brain_line = f"\n🧠 <b>Brain:</b> {emoji} {st} · {b.get('health_score', '?')}/100"
    except Exception:  # noqa: BLE001
        pass
    send_message(
        chat_id,
        f"🌐 <b>SLH Swarm</b>\n\n📡 <b>Devices:</b> {online}/{total} online · {events} events (24h)"
        f"{brain_line}\n\n📊 https://slh-nft.com/swarm.html",
    )


def cmd_admin(chat_id):
    if str(chat_id) not in ADMIN_IDS:
        send_message(chat_id, "⛔ <b>אין הרשאה</b>\n\nפקודה זו זמינה רק למנהלים.")
        return
    health = "🟡"
    try:
        h = requests.get(f"{SLH_API_BASE}/api/health", timeout=5).json()
        health = "🟢" if (h.get("db_connected") is True or h.get("db") == "connected" or h.get("status") == "ok") else "🔴"
    except Exception:  # noqa: BLE001
        pass
    pending = approved = "?"
    if ADMIN_API_KEY:
        try:
            for status in ("pending", "approved"):
                r = requests.get(
                    f"{SLH_API_BASE}/api/therapists/applications",
                    params={"status": status, "limit": 1},
                    headers={"X-Admin-Key": ADMIN_API_KEY},
                    timeout=5,
                )
                if r.status_code == 200:
                    if status == "pending":
                        pending = r.json().get("total", "?")
                    else:
                        approved = r.json().get("total", "?")
        except Exception:  # noqa: BLE001
            pass
    send_message(
        chat_id,
        f"""
👑 <b>פאנל ניהול</b>

{health} SLH API
🩺 מטפלים: {pending} ממתינים · {approved} מאושרים
🔑 ADMIN_API_KEY: {'✅' if ADMIN_API_KEY else '❌ חסר ב-Railway'}
🗄️ Redis לתשלומים: {'✅' if _redis else '❌ זיכרון בלבד'}

📊 https://slh-nft.com/admin/mission-control.html
🩺 https://slh-nft.com/admin/therapists.html
🤖 https://slh-nft.com/admin/bot-registry.html
""",
    )


STATIC_REPLIES = {
    "/dashboard": "📊 <b>לוח בקרה אישי</b>\n\nhttps://slh-nft.com/dashboard.html\n\nאם אתה לא מחובר באתר — שלח /login.",
    "/therapists": (
        "🩺 <b>רשת המטפלים של SLH</b>\n\n"
        "📚 ספריית מטפלים: https://slh-nft.com/therapists.html\n"
        "➕ הצטרף כמטפל: https://slh-nft.com/for-therapists.html\n"
        "📋 לוח בקרה למטפל: https://slh-nft.com/dashboard-therapist.html"
    ),
    "/courses": "🎓 <b>קורסים</b>\n\nhttps://slh-nft.com/academy/course-1-dynamic-yield.html",
    "/blog": "📰 <b>בלוג יומי</b>\n\nhttps://slh-nft.com/blog.html",
    "/wallet": "💼 <b>הארנק שלך</b>\n\nhttps://slh-nft.com/wallet.html",
    "/support": (
        f"💬 <b>תמיכה</b>\n\n🔵 צוות SLH: {SUPPORT_CONTACT}\n"
        "🐛 דיווח באג: https://slh-nft.com/bug-report.html\n\n"
        "⚠️ לעולם לא נפנה אליך ראשונים ב-DM ולא נבקש seed phrase."
    ),
}


def handle_message(msg):
    chat_id = msg["chat"]["id"]
    raw = (msg.get("text") or "").strip()
    name = msg["chat"].get("first_name", "משתמש")
    username = msg["chat"].get("username", "")
    logger.info(f"📨 {chat_id}: {raw[:80]!r}")

    # If several lines were pasted, act on the first command line only.
    text = raw
    if "\n" in raw:
        text = next((l.strip() for l in raw.split("\n") if l.strip().startswith("/")), raw)

    if not text.startswith("/"):
        send_message(chat_id, "💡 שלח /help לרשימת פקודות, או /me לפרופיל שלך.")
        return

    parts = text.split(maxsplit=1)
    cmd = parts[0].split("@")[0].lower()  # handles /buy@SLH_AIR_bot in groups
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd == "/start":
        cmd_start(chat_id, name, username, arg)
    elif cmd == "/help":
        send_message(chat_id, help_message())
    elif cmd == "/me":
        cmd_me(chat_id)
    elif cmd == "/login":
        cmd_login(chat_id, name, username)
    elif cmd == "/buy":
        send_message(chat_id, buy_message(chat_id))
    elif cmd == "/paid":
        cmd_paid(chat_id, name, username)
    elif cmd == "/bots":
        cmd_bots(chat_id)
    elif cmd == "/swarm":
        cmd_swarm(chat_id)
    elif cmd == "/admin":
        cmd_admin(chat_id)
    elif cmd in STATIC_REPLIES:
        send_message(chat_id, STATIC_REPLIES[cmd])
    else:
        send_message(chat_id, "❓ <b>פקודה לא מוכרת</b>\n\nשלח /help לרשימת פקודות.")


# ====================
# MAIN LOOP
# ====================
def main():
    logger.info("=" * 50)
    logger.info("🤖 SLH_AIR bot starting")
    logger.info(f"👤 ADMIN_ID: {ADMIN_ID} (admins: {len(ADMIN_IDS)})")
    logger.info(f"🌐 SLH API: {SLH_API_BASE}")
    logger.info(f"🔑 ADMIN_API_KEY set: {bool(ADMIN_API_KEY)}")
    logger.info(f"💎 Pack: {PACK_SLH} SLH / {PACK_PRICE_TON} TON → {TON_WALLET}")
    logger.info("=" * 50)

    threading.Thread(target=heartbeat_loop, daemon=True, name="heartbeat").start()

    offset = 0
    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 30, "allowed_updates": '["message"]'},
                timeout=35,
            )
            data = r.json()
            if r.status_code == 409:
                # Another instance is polling (e.g. during a redeploy). Back off.
                logger.warning("409 conflict — another instance is polling, waiting")
                time.sleep(10)
                continue
            for update in data.get("result", []) if data.get("ok") else []:
                offset = update["update_id"] + 1
                if "message" in update:
                    try:
                        handle_message(update["message"])
                    except Exception as e:  # noqa: BLE001
                        logger.exception(f"handler error: {e!r}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"main loop error: {e!r}")
            time.sleep(5)


if __name__ == "__main__":
    main()
