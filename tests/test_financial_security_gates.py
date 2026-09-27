import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_owner_auth_is_fail_closed():
    src = _read("main.py")
    assert 'AUTH_SHADOW = os.getenv("AUTH_SHADOW", "0") == "1"' in src
    assert 'raise HTTPException(403, "Not authorized for this user")' in src


def test_direct_wallet_deposit_is_disabled():
    src = _read("main.py")
    assert 'Direct deposit credit is disabled; use the verified chain settlement flow.' in src


def test_guardian_is_fail_closed():
    src = _read("shared/guardian_gate.py")
    assert 'status_code=503' in src
    assert 'fail OPEN' not in src


def test_crypto_auto_settlement_defaults_closed():
    src = _read("routes/payments_auto.py")
    assert 'CRYPTO_AUTO_VERIFY_ENABLED = os.getenv("CRYPTO_AUTO_VERIFY_ENABLED", "0") == "1"' in src
    assert 'BSC_GENESIS_ADDRESS = os.getenv("BSC_GENESIS_ADDRESS", "").strip().lower()' in src
    assert 'BSC_EXPECTED_CHAIN_ID' in src
    assert 'BSC_MIN_CONFIRMATIONS' in src
    assert 'TX sender does not match the user' in src

def test_bsc_minimum_is_server_authoritative():
    src = _read("routes/payments_auto.py")
    assert 'expected_min = max(PREMIUM_MIN_BNB, float(req.expected_min_bnb or 0.0))' in src


def test_bsc_binding_checks_pool_before_query():
    src = _read("routes/payments_auto.py")
    assert 'if _pool is None:' in src
    assert 'SELECT eth_wallet FROM web_users WHERE telegram_id=$1' in src


def test_external_payment_requires_trusted_writer():
    src = _read("routes/payments_auto.py")
    assert '_require_trusted_payment_writer(request)' in src


def test_payment_monitor_defaults_disabled():
    src = _read("routes/payments_monitor.py")
    assert 'PAYMENT_MONITOR_ENABLED = os.getenv("PAYMENT_MONITOR_ENABLED", "0") == "1"' in src
    assert 'BSC_GENESIS = os.getenv("BSC_GENESIS_ADDRESS", "").strip().lower()' in src


def test_registration_unlock_user_methods_require_owner():
    src = _read("main.py")
    assert 'if req.method != "admin":' in src
    assert '_require_owner(req.user_id, authorization, x_admin_key, allow_service=False)' in src


def test_bank_transfer_submit_requires_owner():
    src = _read("main.py")
    assert 'async def submit_bank_transfer(' in src
    assert '_require_owner(req.user_id, authorization, x_admin_key, allow_service=False)' in src


def test_bank_transfer_history_requires_owner():
    src = _read("main.py")
    assert 'async def my_bank_transfers(' in src
    assert '_require_owner(user_id, authorization, x_admin_key, allow_service=False)' in src


def test_legacy_deposit_reads_require_owner():
    src = _read("main.py")
    assert 'async def deposits_status(' in src
    assert '_require_owner(int(d["user_id"]), authorization, x_admin_key, allow_service=False)' in src
    assert 'async def deposits_user_list(' in src


def test_python_parses():
    for rel in ("main.py", "shared/guardian_gate.py", "routes/payments_auto.py", "routes/payments_monitor.py"):
        ast.parse(_read(rel), filename=rel)
