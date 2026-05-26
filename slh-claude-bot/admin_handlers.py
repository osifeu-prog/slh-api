import platform, subprocess, time, os
from aiogram import types
from aiogram.filters import Command

start_time = time.time()

async def cmd_status(m: types.Message):
    uptime = time.time() - start_time
    await m.answer(
        "<b>📡 SYSTEM STATUS</b>\n"
        f"🟢 Bot: Online\n"
        f"⏱ Uptime: {uptime:.0f}s\n"
        f"🐍 Python: {platform.python_version()}\n"
        f"💻 OS: {platform.system()} {platform.release()}"
    )

async def cmd_system(m: types.Message):
    try:
        cpu = subprocess.check_output("wmic cpu get loadpercentage", shell=True).decode().split('\n')[1].strip()
        mem = subprocess.check_output("wmic OS get FreePhysicalMemory,TotalVisibleMemorySize /Value", shell=True).decode()
        await m.answer(f"<b>🖥 SYSTEM</b>\nCPU: {cpu}%\n{mem}")
    except:
        await m.answer("❌ לא ניתן לקבל מידע מערכת.")

async def cmd_logs(m: types.Message):
    try:
        with open("logs/real_bot.log", "r", encoding="utf-8") as f:
            lines = f.readlines()[-20:]
        await m.answer("<b>📄 LAST 20 LOGS</b>\n" + "".join(lines))
    except:
        await m.answer("❌ קובץ לוג לא נמצא.")

async def cmd_balance(m: types.Message):
    await m.answer("⛓ Web3 balance – ABI בתיקון. זמין בקרוב.")
