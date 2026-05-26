"""@SLH_Claude_bot — aiogram entrypoint.

100% FREE AI — Groq + Gemini, zero Anthropic, zero cost.
Guards with Telegram ID allowlist. Persists conversation per chat.
"""
import asyncio
import logging
import os
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, ".env"))

import httpx
from aiogram import types, Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, PreCheckoutQuery

import auth
import handlers
from admin_handlers import cmd_status, cmd_system, cmd_logs, cmd_balance
import session
import quota
import subscriptions
import admin_panel

# Payment flow kept for backwards compat but no longer promoted
try:
    import payment_flow
except Exception:
    payment_flow = None

import free_ai_client as ai_client
_AI_MODE = "Free Unlimited (Groq/Gemini)"


def _pick_ai_client(use_anthropic: bool = False):
    """Always returns the free client."""
    return ai_client, "free", "groq/gemini"

API_BASE = os.getenv("SLH_API_BASE", "https://slh-api-production.up.railway.app")
ADMIN_KEY = os.getenv("ADMIN_API_KEY", "")
TASK_BOARD_PATH = os.getenv(
    "TASK_BOARD_PATH",
    os.path.join(HERE, "..", "ops", "TASK_BOARD.md"),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("slh-claude-bot")

TOKEN = os.getenv("SLH_CLAUDE_BOT_TOKEN")
if not TOKEN:
    raise SystemExit("SLH_CLAUDE_BOT_TOKEN not set. See .env.example.")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
dp = Dispatcher()


# ---------- Cross-bot coordination (shared agents group) ----------
# Registers BEFORE the @dp.message handlers below so coord group messages
# don't get swallowed by the F.text catch-all on_text() handler. No-op when
# COORDINATION_GROUP_CHAT_ID is unset. See shared/coordination.py.
import sys as _sys
_WORKSPACE_ROOT = "/workspace" if os.path.isdir("/workspace/shared") else os.path.abspath(os.path.join(HERE, ".."))
if _WORKSPACE_ROOT not in _sys.path:
    _sys.path.insert(0, _WORKSPACE_ROOT)
try:
    from shared import coordination as _coord
    log.info("shared.coordination loaded; enabled=%s", _coord.is_enabled())
except Exception as _coord_err:
    _coord = None
    log.warning("shared.coordination not loadable: %s", _coord_err)


_BOT_USERNAME_FOR_COORD = os.getenv("SLH_CLAUDE_BOT_USERNAME", "SLH_Claude_bot").lstrip("@")


async def _coord_ping(msg) -> None:
    await msg.reply("pong")


async def _coord_health_handler(msg) -> None:
    try:
        await msg.answer("⚠️ Health check only on Railway")
        return
        await msg.reply(
            f"[OK] API: {h.get('status','?')} · DB: {h.get('db','?')} · v{h.get('version','?')}"
        )
    except Exception as e:
        await msg.reply(f"[X!] {type(e).__name__}: {e}")


async def _coord_who_handler(msg) -> None:
    me = await bot.get_me()
    await msg.reply(f"[i] @{me.username} (claude-bot) · AI={_AI_MODE}")


if _coord is not None:
    _coord.register_inbound(
        dp,
        _BOT_USERNAME_FOR_COORD,
        handlers={
            "ping": _coord_ping,
            "health": _coord_health_handler,
            "who": _coord_who_handler,
        },
    )


# Telegram messages are capped at 4096 chars; split long replies
def _chunks(text: str, size: int = 4000) -> list[str]:
    return [text[i : i + size] for i in range(0, len(text), size)] or [text]


@dp.message(Command("start"))
async def cmd_start(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer(
        f"שלום 👋 אני SLH Spark AI\n"
        f"מצב: {_AI_MODE} — ללא הגבלה ✅\n\n"
        f"━━━ שיחה חופשית ━━━\n"
        f"כל טקסט → AI חינם (Groq/Gemini)\n\n"
        f"━━━ מערכת ━━━\n"
        f"/control  — סיכום מצב בשורה אחת\n"
        f"/health   — בריאות API + DB\n"
        f"/price    — מחירי SLH/MNH/ZVK\n"
        f"/devices  — מכשירים מחוברים\n"
        f"/credits  — סטטיסטיקת שימוש\n\n"
        f"━━━ Ops ━━━\n"
        f"/ps  /bots  /logs <name>  /git\n"
        f"/task <desc>  /clear  /ai_mode\n\n"
        f"━━━ עורך אתר ━━━\n"
        f"/ls  /cat  /grep  /find\n"
        f"/append  /replace  /newpage\n"
        f"/commit  /push  /sync\n\n"
        f"עזרה מלאה: /help",
        parse_mode=None,
    )


@dp.message(Command("help"))
async def cmd_help(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer(
        "*🤖 פקודות Ops \\(מיידי, ללא AI\\):*\n"
        "`/ps` `/bots` `/logs <name>` `/git` `/health` `/price` `/devices` `/task` `/ai_mode`\n\n"
        "*🛠 פקודות עורך \\(שליטה באתר\\):*\n"
        "`/cat` `/ls` `/grep` `/find`\n"
        "`/append` `/replace` `/newpage`\n"
        "`/commit` `/push` `/sync`\n"
        "`/draft` `/apply` `/reject`\n"
        "פירוט מלא: `/editor`\n\n"
        f"*🧠 שיחה חופשית \\(AI: {_AI_MODE}\\):*\n"
        "כל טקסט אחר נענה דרך Groq חינם\\.\n\n"
        "*דוגמאות:*\n"
        "• `/ls website`\n"
        "• `/cat website/voice\\.html`\n"
        "• `/draft website/index\\.html שנה את הכותרת`\n"
        "• `/sync \"feat: my edit\"`"
    )


# ---------- Phase 1: direct API handlers (no Claude, no $ cost) ----------

async def _http_get_json(path: str, headers: dict | None = None) -> dict:
    """Thin httpx wrapper. Raises on non-2xx."""
    url = path if path.startswith("http") else API_BASE + path
    async with httpx.AsyncClient(timeout=8.0) as client:
        resp = await client.get(url, headers=headers or {})
        resp.raise_for_status()
        return resp.json()


def _escape_md(text: str) -> str:
    """Escape MarkdownV1 special chars inside values."""
    if not isinstance(text, str):
        text = str(text)
    # aiogram default is MARKDOWN (v1) — escape only `_*`[
    return (
        text.replace("_", "\\_")
        .replace("*", "\\*")
        .replace("`", "\\`")
        .replace("[", "\\[")
    )


@dp.message(Command("health"))
async def cmd_health(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    try:
        await msg.answer("⚠️ Health check only on Railway")
        return
        api_ok = h.get("status") == "ok" or h.get("api") == "ok"
        db = h.get("db") or (h.get("checks") or {}).get("db") or "unknown"
        lines = [
            f"*API:* {'חי ✓' if api_ok else 'כבוי ✗'}",
            f"*DB:* `{_escape_md(db)}`",
        ]
        if "version" in h:
            lines.append(f"*גרסה:* `{_escape_md(h['version'])}`")
        if "timestamp" in h:
            lines.append(f"*בדוק ב:* `{_escape_md(h['timestamp'])}`")
        await msg.answer("\n".join(lines))
    except httpx.HTTPStatusError as e:
        await msg.answer(f"ה-API החזיר {e.response.status_code}. כנראה down.")
    except Exception as e:
        log.exception("/health failed")
        await msg.answer(f"שגיאה: `{_escape_md(type(e).__name__)}: {_escape_md(str(e))}`")


@dp.message(Command("price"))
async def cmd_price(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("?? Price service only available on Railway")
    return
async def cmd_price(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    try:
        p = await _http_get_json("/api/prices")
        prices = p.get("prices") or p
        if not isinstance(prices, dict) or not prices:
            await msg.answer("אין נתוני מחיר כרגע.")
            return
        lines = ["*מחירים \\(₪\\):*"]
        for token, value in prices.items():
            # /api/prices returns {ils, usd} objects; fall back to scalar if not
            if isinstance(value, dict):
                ils = value.get("ils") or value.get("price") or value.get("value")
            else:
                ils = value
            try:
                fmt = f"{float(ils):,.2f}"
            except (TypeError, ValueError):
                fmt = str(ils)
            lines.append(f"• *{_escape_md(token)}:* `{fmt}`")
        await msg.answer("\n".join(lines))
    except Exception as e:
        log.exception("/price failed")
        await msg.answer(f"שגיאה: `{_escape_md(str(e))}`")


@dp.message(Command("devices"))
async def cmd_devices(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("?? Devices info only available on Railway")
    return
async def cmd_devices(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    if not ADMIN_KEY:
        await msg.answer("חסר `ADMIN_API_KEY` ב-.env של הבוט.")
        return
    try:
        d = await _http_get_json(
            "/api/admin/devices/list", headers={"X-Admin-Key": ADMIN_KEY}
        )
        devices = d.get("devices") or d if isinstance(d, (list, dict)) else []
        if not devices:
            await msg.answer("אין מכשירים רשומים.")
            return
        lines = [f"*מכשירים \\({len(devices)}\\):*"]
        for dev in devices[:10]:
            dev_id = dev.get("device_id") or dev.get("id") or "?"
            last_seen = dev.get("last_seen_at") or dev.get("last_heartbeat") or "--"
            online = dev.get("online") or dev.get("is_online")
            mark = "🟢" if online else "⚫"
            lines.append(
                f"{mark} `{_escape_md(str(dev_id))}` · {_escape_md(str(last_seen))}"
            )
        if len(devices) > 10:
            lines.append(f"_\\+ {len(devices) - 10} נוספים_")
        await msg.answer("\n".join(lines))
    except httpx.HTTPStatusError as e:
        await msg.answer(f"admin API החזיר {e.response.status_code}.")
    except Exception as e:
        log.exception("/devices failed")
        await msg.answer(f"שגיאה: `{_escape_md(str(e))}`")


@dp.message(Command("control"))
async def cmd_control(msg: Message) -> None:
    """One-shot ops summary: API + DB + Gateway + Swarm + Marketplace + Events.

    This is THE 'where do I stand right now' command Osif's advisor asked for.
    No AI, no token cost, ~500ms response. Plain text, no markdown traps.
    """
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return

    sections = []
    timestamp = __import__("datetime").datetime.now().strftime("%H:%M:%S %d/%m")

    # 1. API health
    try:
        await msg.answer("⚠️ Health check only on Railway")
        return
        api_ok = h.get("status") == "ok"
        db_ok = h.get("db") == "connected"
        sections.append(
            f"🟢 API: ok · DB: {h.get('db','?')} · v{h.get('version','?')}"
            if api_ok and db_ok
            else f"🔴 API: {h}"
        )
    except Exception as e:
        sections.append(f"🔴 API: unreachable ({type(e).__name__})")

    # 2. Gateway
    try:
        g = await _http_get_json("/api/miniapp/health")
        if g.get("gateway_loaded"):
            tok = "✓" if g.get("primary_bot_token_set") else "⚠ TELEGRAM_BOT_TOKEN חסר"
            sections.append(f"🟢 Gateway: loaded · admins:{g.get('admin_ids_count')} · bot_token:{tok}")
        else:
            sections.append(f"🔴 Gateway: not loaded")
    except Exception as e:
        sections.append(f"⚪ Gateway: skip ({type(e).__name__})")

    # 3. Swarm
    try:
        s = await _http_get_json("/api/swarm/stats")
        sections.append(
            f"🐝 Swarm: {s.get('online',0)}/{s.get('total_devices',0)} online · "
            f"{s.get('events_24h',0)} events 24h · {s.get('pending_commands',0)} cmds pending"
        )
    except Exception as e:
        sections.append(f"⚪ Swarm: skip ({type(e).__name__})")

    # 4. Marketplace
    try:
        m = await _http_get_json("/api/marketplace/items?limit=100")
        items = [i for i in (m.get("items") or []) if i.get("status") == "approved"]
        sections.append(f"🛒 Marketplace: {len(items)} פריטים approved")
    except Exception as e:
        sections.append(f"⚪ Marketplace: skip")

    # 5. Recent events
    try:
        e = await _http_get_json("/api/events/public?limit=5")
        evts = e.get("events") or []
        if evts:
            recent = ", ".join(set(ev.get("type") or ev.get("event_type", "?") for ev in evts[:5]))
            sections.append(f"📡 Events 5 last: {recent}")
        else:
            sections.append("📡 Events: 0 (פיד פעילות ריק)")
    except Exception:
        sections.append(f"⚪ Events: skip")

    # 6. Your queue (4 user-action blockers)
    queue_items = []
    if 'g' in locals() and not g.get("primary_bot_token_set"):
        queue_items.append("• הגדר TELEGRAM_BOT_TOKEN ב-Railway")
    queue_items.append("• פייר ESP — שלח /devices לבדיקה")
    queue_items.append("• הגדר SMS_PROVIDER ב-Railway (Inforu)")
    queue_items.append("• BotFather: הגדר Mini App URL")

    sections.append("")
    sections.append("📋 התור שלך:")
    sections.extend(queue_items)
    sections.append("")
    sections.append(f"🏠 הבית: https://slh-nft.com/my.html")
    sections.append(f"⏱ נבדק: {timestamp}")

    await msg.answer("\n".join(sections), parse_mode=None)


@dp.message(Command("swarm"))
async def cmd_swarm(msg: Message) -> None:
    """Show SLH Swarm mesh status — total/online/events/pending + per-device list."""
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    try:
        stats = await _http_get_json("/api/swarm/stats")
        devices_resp = await _http_get_json("/api/swarm/devices?limit=20")
        devices = devices_resp.get("devices", [])

        lines = [
            "*🐝 רשת Swarm:*",
            f"• *סה״כ:* `{stats.get('total_devices', 0)}` · "
            f"*online:* `{stats.get('online', 0)}`",
            f"• *events 24h:* `{stats.get('events_24h', 0)}` · "
            f"*commands ממתינות:* `{stats.get('pending_commands', 0)}`",
        ]

        if devices:
            lines.append("\n*מכשירים:*")
            for d in devices[:10]:
                dev_id = d.get("device_id", "?")
                online = d.get("online", False)
                mark = "🟢" if online else "⚫"
                rssi = d.get("last_rssi")
                bat = d.get("last_battery_pct")
                tail_bits = []
                if rssi is not None:
                    tail_bits.append(f"RSSI {rssi}dBm")
                if bat is not None:
                    tail_bits.append(f"{bat}%")
                tail = " · ".join(tail_bits)
                lines.append(
                    f"{mark} `{_escape_md(str(dev_id))}`"
                    + (f" · {_escape_md(tail)}" if tail else "")
                )
            if len(devices) > 10:
                lines.append(f"_\\+ {len(devices) - 10} נוספים_")
        else:
            lines.append(
                "\n_אין מכשירים רשומים עדיין\\. כשתבעיר את ה-firmware עם תמיכת ESP-NOW, המכשירים יירשמו אוטומטית\\._"
            )

        await msg.answer("\n".join(lines))
    except Exception as e:
        log.exception("/swarm failed")
        await msg.answer(f"שגיאה: `{_escape_md(str(e))}`")


@dp.message(Command("task"))
async def cmd_task(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    # Everything after the /task command word
    text = (msg.text or "").split(maxsplit=1)
    if len(text) < 2 or not text[1].strip():
        await msg.answer("שימוש: `/task \\<תיאור המשימה\\>`")
        return
    task_text = text[1].strip()
    try:
        import datetime

        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        line = f"- [ ] {task_text}  _(added {ts} via bot)_\n"
        os.makedirs(os.path.dirname(TASK_BOARD_PATH), exist_ok=True)
        with open(TASK_BOARD_PATH, "a", encoding="utf-8") as f:
            f.write(line)
        await msg.answer(
            f"נוסף ל\\-TASK\\_BOARD\\.md:\n`{_escape_md(task_text)}`"
        )
    except Exception as e:
        log.exception("/task failed")
        await msg.answer(f"שגיאה: `{_escape_md(str(e))}`")


@dp.message(Command("status"))
async def cmd_status(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("מבצע בדיקת מצב מהירה...")
    try:
        reply, new_msgs = await ai_client.converse(
            history=[],
            user_text="בצע בדיקה מהירה: 1) curl ל-/api/health של Railway, 2) git status בשני ה-repos (D:\\SLH_ECOSYSTEM ו-D:\\SLH_ECOSYSTEM\\website), 3) docker ps. תן סיכום של 3-5 שורות בעברית.",
        )
        for msg_part in new_msgs:
            await session.append(msg.chat.id, msg_part["role"], msg_part["content"])
        for chunk in _chunks(reply):
            await msg.answer(chunk)
    except Exception as e:
        log.exception("status failed")
        await msg.answer(f"שגיאה: `{type(e).__name__}: {e}`")


@dp.message(Command("clear"))
async def cmd_clear(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    n = await session.clear(msg.chat.id)
    await msg.answer(f"נוקה. נמחקו {n} הודעות.")


# ---------- Direct executor commands (no AI, no cost) ----------
import subprocess

_SAFE_EXEC_ALLOWLIST = {
    "docker ps", "docker compose ps", "docker stats --no-stream",
    "git status", "git log --oneline -10", "git diff --stat",
    "df -h", "uptime", "uname -a",
}


def _resolve_cwd() -> str:
    """Pick a valid working dir. /workspace works in container, falls back
    to repo root on local Windows installs."""
    candidates = ["/workspace", os.path.join(HERE, ".."), HERE]
    for c in candidates:
        if c and os.path.isdir(c):
            return c
    return os.getcwd()


_CMD_CWD = _resolve_cwd()


def _run_cmd(cmd: str, timeout: int = 15) -> str:
    """Run a shell command and return stdout+stderr, capped."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=_CMD_CWD,
        )
        out = (result.stdout or "") + (result.stderr or "")
        return out[:3500] or "(no output)"
    except subprocess.TimeoutExpired:
        return f"⏱ command timed out after {timeout}s"
    except FileNotFoundError as e:
        # Docker / git not in PATH — friendly message
        return f"⚠️ command not found: {e}"
    except Exception as e:
        return f"⚠️ {type(e).__name__}: {e}"


def _has_binary(name: str) -> bool:
    import shutil
    return shutil.which(name) is not None


@dp.message(Command("ps"))
async def cmd_ps(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("?? Docker not available  use Railway Dashboard")
    return
async def cmd_ps(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    if not _has_binary("docker"):
        # Fallback: list services from docker-compose.yml so the user sees
        # the configured fleet even when the bot has no docker socket.
        compose_path = os.path.join(_CMD_CWD, "docker-compose.yml")
        if os.path.isfile(compose_path):
            try:
                with open(compose_path, "r", encoding="utf-8") as f:
                    raw = f.read()
                services = []
                in_services = False
                for line in raw.splitlines():
                    if line.startswith("services:"):
                        in_services = True
                        continue
                    if in_services:
                        if line and not line.startswith((" ", "\t")):
                            break
                        # Service names are 2-space indented and end with ':'
                        if line.startswith("  ") and not line.startswith("    ") and line.rstrip().endswith(":"):
                            services.append(line.strip().rstrip(":"))
                services_str = "\n".join(f"• {s}" for s in services[:40])
                await msg.answer(
                    "🐳 docker לא מותקן בסביבה הזו של הבוט.\n\n"
                    f"שירותים שמוגדרים ב-docker-compose.yml ({len(services)}):\n"
                    f"{services_str}\n\n"
                    "להפעלת הסטטוס בפועל הרץ במחשב המארח: `docker compose ps`",
                    parse_mode=None,
                )
                return
            except Exception as e:
                await msg.answer(f"docker חסר ולא הצלחתי לקרוא compose: {e}")
                return
        await msg.answer("🐳 docker לא מותקן + docker-compose.yml לא נמצא.", parse_mode=None)
        return
    out = _run_cmd("docker ps --format 'table {{.Names}}\\t{{.Status}}'")
    await msg.answer(f"```\n{out}\n```")


@dp.message(Command("logs"))
async def cmd_logs(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("?? Logs not available  check Railway Dashboard")
    return
async def cmd_logs(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await msg.answer("שימוש: `/logs \\<container\\-name\\>`  \nלמשל: `/logs slh\\-claude\\-bot`")
        return
    name = parts[1].strip().replace(";", "").replace("&", "").replace("|", "")
    # Allowlist prefix check
    if not name.startswith(("slh-", "slh_")):
        await msg.answer("רק containers עם prefix `slh-` מותרים.")
        return
    await msg.answer("⚠️ Logs not available  check Railway Dashboard")
    return
    await msg.answer(f"*logs {name}:*\n```\n{out[-3500:]}\n```")


@dp.message(Command("git"))
async def cmd_git(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    parts = (msg.text or "").split(maxsplit=1)
    subcmd = (parts[1].strip() if len(parts) > 1 else "status").split()[0]
    safe_subs = {"status", "log", "diff", "branch"}
    if subcmd not in safe_subs:
        await msg.answer(f"פקודת git מותרות בלבד: {', '.join(safe_subs)}")
        return
    repo_hint = (parts[1].strip() if len(parts) > 1 else "")
    # Default = website (small repo); switch to main only if user says api/main
    if "api" in repo_hint or "main" in repo_hint:
        repo = "/workspace"
    else:
        repo = "/workspace/website"
    if subcmd == "log":
        out = _run_cmd(f"cd {repo} && git log --oneline -10", timeout=10)
    elif subcmd == "diff":
        out = _run_cmd(f"cd {repo} && git diff --stat HEAD", timeout=10)
    elif subcmd == "branch":
        out = _run_cmd(f"cd {repo} && git branch --show-current", timeout=5)
    else:
        # -uno = no untracked (workspace has 100s of untracked backup files)
        await msg.answer("⚠️ Git status not available locally")
        return
    repo_short = "website" if "website" in repo else "main"
    await msg.answer(f"*git {subcmd} @ `{repo_short}`:*\n```\n{out[:3500]}\n```")


@dp.message(Command("bots"))
async def cmd_bots(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("?? Bot fleet info only available on Railway")
    return
async def cmd_bots(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    out = _run_cmd("docker ps --format '{{.Names}}' | grep ^slh- | sort | wc -l")
    running = out.strip()
    out_list = _run_cmd("docker ps --format '{{.Names}}: {{.Status}}' | grep ^slh- | sort")
    await msg.answer(
        f"*Bot fleet: {running} רצים*\n```\n{out_list[:3500]}\n```"
    )


@dp.message(Command("ai_mode"))
async def cmd_ai_mode(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer(
        f"*AI mode:* `{_AI_MODE}`\n\n"
        f"{'✅ Anthropic Claude עם tool use (עולה כסף)' if _AI_MODE == 'anthropic-tools' else '✅ SLH multi-provider (Groq/Gemini חינם)'}"
    )


# Photo/screenshot handler — saves incoming images to /workspace/incoming_screenshots/
# so the human operator can read them via Read tool from outside the container.
@dp.message(F.photo)
async def on_photo(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    try:
        from datetime import datetime
        photo = msg.photo[-1]  # largest size
        file = await bot.get_file(photo.file_id)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = "/workspace/incoming_screenshots"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/screenshot_{ts}_{msg.from_user.id}.jpg"
        await bot.download_file(file.file_path, out_path)
        # Optional caption
        cap = (msg.caption or "").strip()
        log.info(f"saved screenshot from {msg.from_user.id} to {out_path} (caption='{cap[:60]}')")
        await msg.answer(
            f"✅ קיבלתי תמונה ושמרתי\\.\n"
            f"📂 `screenshot_{ts}`\n"
            f"{'📝 ' + _escape_md(cap[:200]) if cap else ''}\n\n"
            f"Claude ניגש לקובץ הזה ויקרא אותו\\."
        )
    except Exception as e:
        log.exception("photo save failed")
        await msg.answer(f"שגיאה בשמירת התמונה: `{type(e).__name__}: {e}`")


# Filter excludes slash-commands so they fall through to Command-filtered
# handlers registered LATER (payment_flow, admin_panel, editor_commands).
@dp.message(F.text & ~F.text.startswith("/"))
async def on_text(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    text = msg.text or ""
    if not text.strip():
        return

    # ==== QUOTA GATE ====
    decision = await quota.check(msg.from_user.id)
    if not decision.allowed:
        await msg.answer(decision.refusal_he, parse_mode="Markdown")
        return

    await bot.send_chat_action(msg.chat.id, "typing")

    try:
        hist = await session.history(msg.chat.id)
        reply, new_msgs = await ai_client.converse(hist, text)

        for m in new_msgs:
            await session.append(msg.chat.id, m["role"], m["content"])

        # Usage log (analytics only, cost is always 0)
        tokens_in = max(1, sum(len(str(m.get("content", ""))) for m in hist) // 4)
        tokens_out = max(1, len(reply) // 4)
        await quota.record(
            user_id=msg.from_user.id,
            chat_id=msg.chat.id,
            tier="free",
            provider="free",
            model="groq/gemini",
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd_cents=0,
        )

        for chunk in _chunks(reply):
            await msg.answer(chunk)

    except Exception as e:
        log.exception("converse failed")
        await msg.answer(f"שגיאה: `{type(e).__name__}: {e}`")



@dp.message(Command("status"))
async def status_handler(msg: Message):
    await cmd_status(m)

@dp.message(Command("system"))
async def system_handler(m: types.Message):
    await cmd_system(m)

@dp.message(Command("logs"))
async def cmd_logs(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await msg.answer("?? Logs not available  check Railway Dashboard")
    return
async def logs_handler(m: types.Message):
    await cmd_logs(m)

@dp.message(Command("balance"))
async def balance_handler(m: types.Message):
    await cmd_balance(m)


@dp.pre_checkout_query()
async def checkout_handler(query: PreCheckoutQuery) -> None:
    await bot.answer_pre_checkout_query(query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(msg: Message) -> None:
    await msg.answer("? ???? ?? ??????! ??? ????? Premium.")
    # ????? ????? Premium ?-DB

async def main() -> None:
    await session.init_db()
    await subscriptions.init_db()
    # Wire optional panels (non-critical — won't block startup)
    if payment_flow is not None:
        try:
            payment_flow.register(dp, auth)
        except Exception as e:
            log.warning(f"payment_flow not loaded: {e}")
    try:
        admin_panel.register(dp, auth)
    except Exception as e:
        log.warning(f"admin_panel not loaded: {e}")
    # Rotation panel must register BEFORE the F.text handler in bot.py runs
    # so its token-input filter gets first crack at user messages when a
    # rotation flow is pending. Order matters: aiogram dispatches in
    # registration order.
    try:
        import rotation_panel
        rotation_panel.register(dp, auth)
        log.info("rotation_panel wired in")
    except Exception as e:
        log.warning(f"rotation_panel not loaded: {e}")
    try:
        import railway_ops
        railway_ops.register(dp, auth)
        log.info("payment_flow + admin_panel + railway_ops wired in")
    except Exception as e:
        log.warning(f"railway_ops not loaded: {e}")
        log.info("payment_flow + admin_panel wired in")
    # Wire up editor commands (cat/ls/grep/append/replace/newpage/commit/push/sync/draft/apply/reject)
    try:
        import editor_commands
        editor_commands.register(dp, auth, _chunks)
        log.info("editor_commands wired in")
    except Exception as e:
        log.warning(f"editor_commands not loaded: {e}")
    log.info(f"starting @SLH_Claude_bot · AI mode: {_AI_MODE}")
    me = await bot.get_me()
    log.info(f"connected as @{me.username} (id={me.id})")
    # Announce startup to the coordination group (no-op if disabled)
    if _coord is not None:
        await _coord.post_event(
            bot, "claude-bot", "ready",
            f"@{me.username} polling · AI={_AI_MODE}"
        )
    handlers.register(dp)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())






