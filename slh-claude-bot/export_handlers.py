@dp.message(Command("export_users"))
async def export_users_cmd(msg: Message) -> None:
    if not auth.is_authorized(msg.from_user.id):
        await msg.answer(auth.unauthorized_reply_he(msg.from_user.id))
        return
    try:
        conn = await asyncpg.connect(DB_URL)
        rows = await conn.fetch("SELECT * FROM users")
        await conn.close()
        csv = "user_id,username,full_name,first_seen,last_seen,is_premium\n"
        for r in rows:
            csv += f"{r['user_id']},{r['username']},{r['full_name']},{r['first_seen']},{r['last_seen']},{r['is_premium']}\n"
        with open("users_export.csv", "w", encoding="utf-8") as f:
            f.write(csv)
        await msg.answer("✅ ייצוא הושלם. הקובץ נשמר בשרת.")
    except Exception as e:
        await msg.answer(f"❌ שגיאה: {e}")
