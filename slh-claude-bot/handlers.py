from aiogram import types
from aiogram.filters import Command
import auth
import httpx
from bot import bot

# ========= פקודות קיימות =========
async def pay_cmd(msg: types.Message):
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    await bot.send_invoice(
        chat_id=msg.chat.id,
        title="SLH Premium",
        description="Gisha Hodshit LeTokhen Premium",
        payload="premium_monthly",
        provider_token="",
        currency="XTR",
        prices=[{"label": "SLH Premium", "amount": 5000}],
        start_parameter="premium"
    )

async def premium_cmd(msg: types.Message):
    await msg.answer("Tukhn Premium - Hishtamesh b-/pay Lerkisha")

async def contact_cmd(msg: types.Message):
    await msg.answer(
        "Yetzirat Kesher:\n"
        "Telegram: @OsifUngar\n"
        "Email: osif.e.u@gmail.com\n"
        "Website: https://slh-nft.com"
    )

async def menu_cmd(msg: types.Message):
    await msg.answer(
        "AI Khofshi - Pashut Tishlekh Hodaah\n"
        "Premium - /pay (Tashlum), /premium (Tokhen)\n"
        "Tzor Kesher - /contact\n"
        "Status - /status\n"
        "Pekudot Ops - /ps, /logs, /git, /health\n"
        "Ezra - /help"
    )

# ========= פקודות חדשות (עובדות) =========
async def donate_cmd(msg: types.Message):
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    text = (
        "🤝 *תרומה ל‑SLH Ecosystem*\n\n"
        "אתה יכול לתמוך בפרויקט דרך הקריפטו:\n"
        "`USDT (TRC-20):` TYoB3sXqH3kL9xQZqR5nL8wJqVkL3wYxZ\n"
        "`Bitcoin:` bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh\n\n"
        "כל תרומה תעזור להמשיך לפתח כלים חופשיים למי שאין לו.\n"
        "תודה 🙏"
    )
    await msg.answer(text, parse_mode="Markdown")

async def crypto_cmd(msg: types.Message):
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            btc = await client.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd")
            eth = await client.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd")
            data = f"💰 *BTC:* ${btc.json()['bitcoin']['usd']}  |  *ETH:* ${eth.json()['ethereum']['usd']}"
            await msg.answer(data, parse_mode="Markdown")
        except Exception as e:
            await msg.answer(f"⚠️ שגיאה: {e}")

async def guide_cmd(msg: types.Message):
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    guide = (
        "*📘 מדריך הישרדות כלכלית*\n\n"
        "🔐 *ארנק לא מפוקח:* Trust Wallet / Exodus. שמור מילות גיבוי.\n\n"
        "💵 *קניית stablecoin ללא בנק:* בורסה מבוזרת (Bisq) או P2P.\n\n"
        "📉 *הגנה מאינפלציה:* המר חלק ל-USDT או USDC.\n\n"
        "⚠️ *CBDC הוא כלי שליטה.*\n\n"
        "🤝 *תרומה:* /donate"
    )
    await msg.answer(guide, parse_mode="Markdown")

# ========= רישום כל הפקודות =========
def register(dp):
    dp.message(Command("pay"))(pay_cmd)
    dp.message(Command("premium"))(premium_cmd)
    dp.message(Command("contact"))(contact_cmd)
    dp.message(Command("menu"))(menu_cmd)
    dp.message(Command("donate"))(donate_cmd)
    dp.message(Command("crypto"))(crypto_cmd)
    dp.message(Command("guide"))(guide_cmd)