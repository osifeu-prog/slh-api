# -*- coding: utf-8 -*-
import os
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import asyncpg
import httpx

load_dotenv()

DB_URL = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DB_URL")
BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "")
OWNER_ID = 224223270
SLH_CONTRACT = "0xACb0A09414CEA1C879c67bB7A877E4e19480f022"
BSC_RPC = "https://bsc-dataseed.binance.org/"

app = FastAPI(title="SLH Core API", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def _check_db():
    try:
        conn = await asyncpg.connect(DB_URL)
        await conn.execute("SELECT 1")
        await conn.close()
        return "ok"
    except Exception as e:
        return f"error: {e}"

@app.get("/health")
async def health():
    return {"status": "ok", "db": await _check_db()}

@app.get("/api/health")
async def health_api():
    return {"status": "ok", "db": await _check_db()}

@app.post("/api/connect/wallet")
async def connect_wallet(data: dict = Body(...)):
    wallet = data.get("wallet_address", "").lower()
    tg_id = data.get("telegram_id")
    tg_user = data.get("telegram_username", "")
    if not wallet or not wallet.startswith("0x"):
        return {"error": "invalid wallet"}
    padded = wallet.replace("0x", "").zfill(64)
    balance = 0.0
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(BSC_RPC, json={"jsonrpc":"2.0","method":"eth_call","params":[{"to":SLH_CONTRACT,"data":"0x70a08231"+padded},"latest"],"id":1}, timeout=10)
            balance = int(r.json().get("result","0x0"),16) / (10**15)
    except Exception:
        pass
    tier = "whale" if balance>=1_000_000 else "major" if balance>=100_000 else "holder" if balance>=10_000 else "investor" if balance>=1_000 else "member"
    try:
        conn = await asyncpg.connect(DB_URL)
        await conn.execute("""CREATE TABLE IF NOT EXISTS connected_wallets (wallet_address TEXT PRIMARY KEY, telegram_id BIGINT, telegram_username TEXT, slh_balance NUMERIC, tier TEXT, connected_at TIMESTAMP DEFAULT NOW(), last_seen TIMESTAMP DEFAULT NOW())""")
        await conn.execute("""INSERT INTO connected_wallets (wallet_address,telegram_id,telegram_username,slh_balance,tier) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (wallet_address) DO UPDATE SET slh_balance=$4,tier=$5,last_seen=NOW(),telegram_id=COALESCE($2,connected_wallets.telegram_id)""", wallet,tg_id,tg_user,balance,tier)
        await conn.close()
    except Exception as e:
        return {"error": f"db: {e}"}
    if BOT_TOKEN and tg_id:
        try:
            async with httpx.AsyncClient() as c:
                await c.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id":tg_id,"text":f"✅ ארנק חובר!\n📍 {wallet[:6]}...{wallet[-4:]}\n💰 {balance:,.0f} SLH\n🏅 {tier.upper()}","parse_mode":"HTML"}, timeout=10)
        except Exception:
            pass
    if BOT_TOKEN:
        try:
            async with httpx.AsyncClient() as c:
                await c.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id":OWNER_ID,"text":f"🔔 ארנק חדש!\n{wallet}\n{balance:,.0f} SLH — {tier}"}, timeout=10)
        except Exception:
            pass
    return {"status":"connected","wallet":wallet,"slh_balance":balance,"tier":tier}

@app.get("/api/holders")
async def get_holders():
    try:
        conn = await asyncpg.connect(DB_URL)
        rows = await conn.fetch("SELECT wallet_address,telegram_username,slh_balance,tier,connected_at FROM connected_wallets ORDER BY slh_balance DESC")
        await conn.close()
        return {"holders":[dict(r) for r in rows],"total":len(rows)}
    except Exception as e:
        return {"error": str(e)}