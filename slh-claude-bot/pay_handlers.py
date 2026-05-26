@dp.message(Command("pay"))
async def cmd_pay(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    # Telegram Stars invoice  50 Stars for Premium
    await bot.send_invoice(
        chat_id=msg.chat.id,
        title="SLH Premium",
        description="גישה חודשית לתוכן פרימיום: ניתוחים, סיגנלים, דירוגים.",
        payload="premium_monthly",
        provider_token="",  # Stars payments don't need provider_token
        currency="XTR",
        prices=[{"label": "SLH Premium", "amount": 5000}],  # 50 XTR = 50 Stars
        start_parameter="premium"
    )
