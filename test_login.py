"""
test_login.py -- Telegram session headless login test
"""

import asyncio
import sys
import os

# ===== load config from .env =====
def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        print(f"[ERROR] .env not found: {env_path}")
        print(f"        Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)
    cfg = {}
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg

_env = _load_env()

API_ID = int(_env.get("API_ID", "0"))
API_HASH = _env.get("API_HASH", "")
SESSION_DIR = _env.get("SESSION_DIR", "./data")
ACCOUNTS = [a.strip() for a in _env.get("ACCOUNTS", "").split(",") if a.strip()]

if not API_ID or not API_HASH:
    print("[ERROR] API_ID / API_HASH not set in .env")
    sys.exit(1)
if not ACCOUNTS:
    print("[ERROR] ACCOUNTS not set in .env")
    sys.exit(1)


async def test_login(account_id: str):
    from telethon import TelegramClient
    from proxy_utils import get_proxy_for_telethon

    session_path = os.path.join(SESSION_DIR, account_id, "session")
    session_file = session_path + ".session"

    if not os.path.exists(session_file):
        print(f"  [FAIL] session file not found: {session_file}")
        return False

    print(f"  session: {session_file} ({os.path.getsize(session_file)} bytes)")

    proxy_dict, proxy_status = get_proxy_for_telethon(
        mode="auto",
        fallback_to_direct=True,
        logger_func=lambda msg: print(f"  {msg}")
    )
    print(f"  proxy: {proxy_status}")

    client = TelegramClient(
        session_path, API_ID, API_HASH,
        proxy=proxy_dict,
        timeout=15,
        connection_retries=3,
    )

    try:
        print("  connecting...")
        await client.connect()

        if not await client.is_user_authorized():
            print(f"  [FAIL] session expired, need re-login")
            return False

        me = await client.get_me()
        print(f"  [OK] login success!")
        print(f"       name:    {me.first_name or ''} {me.last_name or ''}")
        print(f"       user_id: {me.id}")
        print(f"       dc:      DC{client.session.dc_id}")
        print(f"       premium: {me.premium or False}")

        dialogs = await client.get_dialogs(limit=3)
        print(f"       dialogs: {len(dialogs)}")
        for d in dialogs[:3]:
            print(f"         - {d.name} (id={d.id})")

        return True

    except Exception as e:
        print(f"  [FAIL] error: {e}")
        return False

    finally:
        await client.disconnect()


async def main():
    print("=" * 50)
    print("TGDownloader SDK - headless login test")
    print("=" * 50)
    print()

    results = {}
    for account_id in ACCOUNTS:
        print(f"[{account_id}]")
        ok = await test_login(account_id)
        results[account_id] = ok
        print()

    print("=" * 50)
    for acc, ok in results.items():
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {acc}")
    print("=" * 50)

    passed = sum(1 for v in results.values() if v)
    print(f"result: {passed}/{len(results)} accounts OK")


if __name__ == "__main__":
    asyncio.run(main())
