"""
SLH Master Bot
FastAPI + python-telegram-bot
"""

import os
import logging
import asyncio
import datetime
import threading

from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
import uvicorn

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 8000))

# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="SLH Master Bot")

START_TIME = datetime.datetime.utcnow()


@app.get("/api/health")
async def health():
    uptime = (datetime.datetime.utcnow() - START_TIME).total_seconds()
    return {
        "status": "ok",
        "uptime_seconds": uptime,
        "bot": "slh-master-bot",
        "version": "53.2",
    }


@app.get("/")
async def root():
    return {"status": "ok", "service": "slh-master-bot"}


# ── Telegram handlers ─────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    banner = (
        "╔══════════════════════════════════════════════════════════╗\n"
        "║                🔐  SLH CORE SYSTEM v53.2  🔐            ║\n"
        "║               SLH Spark — Intelligence Layer            ║\n"
        "╚══════════════════════════════════════════════════════════╝\n\n"
        f"👤 User: {user.full_name}\n"
        f"🆔 ID: {user.id}\n"
        f"🎭 Role: viewer\n"
        f"🕒 {now}\n"
        "🔒 Status: AUTHORIZED"
    )
    await update.message.reply_text(banner)
    await update.message.reply_text("✅ SLH v53.2 ready. Send /help for commands.")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "<b>SLH Commands</b>\n\n"
        "/tasks — כל המשימות\n"
        "/task_new כותרת — יצירת משימה\n"
        "/task_info ID — פרטי משימה\n"
        "/task_pick ID — לקיחת משימה\n"
        "/task_done ID — סימון כהושלמה\n"
        "/task_comment ID טקסט — הוספת תגובה\n\n"
        "/status — System status\n"
        "/my_access — ההרשאות שלי"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = (datetime.datetime.utcnow() - START_TIME).total_seconds()
    import platform
    text = (
        "<b>📡 SYSTEM STATUS</b>\n"
        "🟢 Bot: Online\n"
        f"⏱ Uptime: {uptime:.0f}s\n"
        f"🐍 Python: {platform.python_version()}\n"
        f"💻 OS: {platform.system()} {platform.release()}"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_my_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👤 Role: <b>viewer</b>\nTeam: general", parse_mode="HTML")


# ── Task Board ────────────────────────────────────────────────────────────────
class TaskBoard:
    def __init__(self):
        self.tasks: dict = {}
        self.next_id = 1

    def add_task(self, title: str, creator: str) -> int:
        tid = self.next_id
        self.tasks[tid] = {
            "id": tid,
            "title": title,
            "status": "open",
            "creator": creator,
            "assignee": None,
            "created_at": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "comments": [],
        }
        self.next_id += 1
        return tid

    def get_all(self) -> str:
        if not self.tasks:
            return "📋 אין משימות פתוחות."
        lines = ["<b>📋 SLH Task Board</b>\n"]
        for t in sorted(self.tasks.values(), key=lambda x: x["id"]):
            emoji = "🟢" if t["status"] == "open" else "🔵" if t["status"] == "in_progress" else "✅"
            assignee = f" → @{t['assignee']}" if t["assignee"] else ""
            lines.append(f"{emoji} <b>#{t['id']}</b> {t['title']}{assignee}")
        return "\n".join(lines)

    def info(self, tid: int) -> str:
        t = self.tasks.get(tid)
        if not t:
            return "❌ משימה לא נמצאה."
        comments = "\n".join(
            [f"▪ {c['time']} @{c['user']}: {c['text']}" for c in t["comments"]]
        ) or "אין תגובות"
        return (
            f"<b>📋 משימה #{t['id']}</b>\n"
            f"כותרת: {t['title']}\n"
            f"סטטוס: {t['status']}\n"
            f"יוצר: @{t['creator']}\n"
            f"אחראי: @{t.get('assignee') or '—'}\n"
            f"נוצרה: {t['created_at']}\n\n"
            f"💬 <b>תגובות:</b>\n{comments}"
        )

    def pick(self, tid: int, user: str) -> str:
        t = self.tasks.get(tid)
        if not t:
            return "❌ משימה לא נמצאה"
        if t["status"] == "done":
            return "❌ המשימה כבר הושלמה"
        t["status"] = "in_progress"
        t["assignee"] = user
        return f"✅ משימה #{tid} נלקחה על ידי @{user}"

    def done(self, tid: int) -> str:
        t = self.tasks.get(tid)
        if not t:
            return "❌ משימה לא נמצאה"
        t["status"] = "done"
        return f"🎉 משימה #{tid} הושלמה!"

    def comment(self, tid: int, user: str, text: str) -> str:
        t = self.tasks.get(tid)
        if not t:
            return "❌ משימה לא נמצאה"
        t["comments"].append({
            "user": user,
            "text": text,
            "time": datetime.datetime.utcnow().strftime("%H:%M"),
        })
        return f"💬 תגובה נוספה למשימה #{tid}"


task_board = TaskBoard()


async def cmd_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(task_board.get_all(), parse_mode="HTML")


async def cmd_task_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = update.message.text.replace("/task_new", "").strip()
    if not title:
        return await update.message.reply_text("❌ דוגמה: /task_new לתקן את ה-Docker networking")
    tid = task_board.add_task(title, update.effective_user.username or "user")
    await update.message.reply_text(f"✅ משימה נוצרה!\nID: <code>#{tid}</code>", parse_mode="HTML")


async def cmd_task_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        tid = int(update.message.text.replace("/task_info", "").strip())
        await update.message.reply_text(task_board.info(tid), parse_mode="HTML")
    except Exception:
        await update.message.reply_text("❌ שימוש: /task_info 5")


async def cmd_task_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        tid = int(update.message.text.replace("/task_pick", "").strip())
        await update.message.reply_text(task_board.pick(tid, update.effective_user.username or "user"))
    except Exception:
        await update.message.reply_text("❌ שימוש: /task_pick 5")


async def cmd_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        tid = int(update.message.text.replace("/task_done", "").strip())
        await update.message.reply_text(task_board.done(tid))
    except Exception:
        await update.message.reply_text("❌ שימוש: /task_done 5")


async def cmd_task_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        parts = update.message.text.replace("/task_comment", "").strip().split(maxsplit=1)
        tid = int(parts[0])
        comment_text = parts[1] if len(parts) > 1 else ""
        if not comment_text:
            return await update.message.reply_text("❌ שימוש: /task_comment 5 כאן כותבים תגובה")
        await update.message.reply_text(
            task_board.comment(tid, update.effective_user.username or "user", comment_text)
        )
    except Exception:
        await update.message.reply_text("❌ שימוש: /task_comment 5 הטקסט")


# ── Bot runner ────────────────────────────────────────────────────────────────
def run_bot():
    if not TOKEN:
        logger.warning("BOT_TOKEN not set — Telegram bot will not start.")
        return

    async def _run():
        bot_app = (
            ApplicationBuilder()
            .token(TOKEN)
            .build()
        )
        bot_app.add_handler(CommandHandler("start", cmd_start))
        bot_app.add_handler(CommandHandler("help", cmd_help))
        bot_app.add_handler(CommandHandler("status", cmd_status))
        bot_app.add_handler(CommandHandler("my_access", cmd_my_access))
        bot_app.add_handler(CommandHandler("tasks", cmd_tasks))
        bot_app.add_handler(CommandHandler("task_new", cmd_task_new))
        bot_app.add_handler(CommandHandler("task_info", cmd_task_info))
        bot_app.add_handler(CommandHandler("task_pick", cmd_task_pick))
        bot_app.add_handler(CommandHandler("task_done", cmd_task_done))
        bot_app.add_handler(CommandHandler("task_comment", cmd_task_comment))

        logger.info("🚀 SLH Telegram bot polling started")
        await bot_app.run_polling(drop_pending_updates=True)

    asyncio.run(_run())


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Run Telegram bot in a background thread so uvicorn can own the main thread
    if TOKEN:
        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()

    logger.info(f"🌐 Starting FastAPI on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
