"""Telegram WebApp Init-Data verification."""
import hashlib, hmac, json, os, time
from urllib.parse import parse_qsl

BOT_TOKEN = os.getenv("COMMUNITY_BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
MAX_AGE_SECONDS = 24 * 3600


def verify_init_data(init_data):
    if not BOT_TOKEN or not init_data:
        return None
    try:
        pairs = dict(parse_qsl(init_data, strict_parsing=True))
    except Exception:
        return None
    received = pairs.pop("hash", None)
    if not received:
        return None
    dcs = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    sk = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    calc = hmac.new(sk, dcs.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, received):
        return None
    try:
        ad = int(pairs.get("auth_date", 0))
    except (TypeError, ValueError):
        return None
    if ad and (time.time() - ad) > MAX_AGE_SECONDS:
        return None
    try:
        user = json.loads(pairs.get("user", "{}"))
    except Exception:
        return None
    if not isinstance(user, dict) or not user.get("id"):
        return None
    return {"id": int(user["id"]), "username": user.get("username"), "first_name": user.get("first_name")}
