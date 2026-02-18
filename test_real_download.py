"""
test_real_download.py -- WorkerPool download test with visual progress
"""

import asyncio
import logging
import os
import sys
import time

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
ACCOUNT_ID = _env.get("DOWNLOAD_ACCOUNT", "")
CHANNEL = _env.get("DOWNLOAD_CHANNEL", "")
MSG_ID = int(_env.get("DOWNLOAD_MSG_ID", "0"))
SAVE_DIR = _env.get("SAVE_DIR", "./test_output")
LOG_FILE = os.path.join(SAVE_DIR, "download.log")

if not API_ID or not API_HASH:
    print("[ERROR] API_ID / API_HASH not set in .env")
    sys.exit(1)
if not ACCOUNT_ID or not CHANNEL or not MSG_ID:
    print("[ERROR] DOWNLOAD_ACCOUNT / DOWNLOAD_CHANNEL / DOWNLOAD_MSG_ID not set in .env")
    sys.exit(1)

# ===== logging setup (file only, no console) =====
os.makedirs(SAVE_DIR, exist_ok=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
fmt = logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

fh = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="w")
fh.setLevel(logging.DEBUG)
fh.setFormatter(fmt)
root_logger.addHandler(fh)

logger = logging.getLogger("test")

# ===== terminal helpers =====
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

if sys.platform == "win32":
    os.system("")  # enable ANSI escape codes on Windows 10+

LINE = "=" * 62
THIN = "-" * 62


def P(text=""):
    """Print to terminal only (not to log)."""
    print(text, flush=True)


def bar(pct, width=30):
    filled = int(width * pct / 100)
    return "\u2588" * filled + "\u2591" * (width - filled)


def sz(b):
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KB"
    if b < 1024 * 1024 * 1024:
        return f"{b / 1024 / 1024:.1f} MB"
    return f"{b / 1024 / 1024 / 1024:.2f} GB"


async def main():
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import InputDocumentFileLocation
    from proxy_utils import get_proxy_for_telethon
    from download_pool import WorkerPool, ClientPool, Part
    from download_config import get_config_value
    from sparse_file_utils import create_sparse_file

    P(LINE)
    P("  TG Download Test  --  WorkerPool Engine")
    P(LINE)

    # ===== 1. connect =====
    session_path = os.path.join(SESSION_DIR, ACCOUNT_ID, "session")
    proxy_dict, proxy_status = get_proxy_for_telethon(
        mode="auto", fallback_to_direct=True,
        logger_func=lambda msg: logger.info(msg),
    )
    logger.info(f"proxy: {proxy_status}")
    P(f"  Proxy:       {proxy_status}")
    P(f"  Connecting...")

    client = TelegramClient(
        session_path, API_ID, API_HASH,
        proxy=proxy_dict, timeout=30,
        flood_sleep_threshold=0,
    )
    await client.connect()
    if not await client.is_user_authorized():
        P("  [FAIL] Session expired!")
        logger.error("[FAIL] session expired")
        return

    me = await client.get_me()
    account_dc = client.session.dc_id
    logger.info(f"account: {me.first_name} (id={me.id}), DC{account_dc}, premium={me.premium or False}")

    P(f"  Account:     {me.first_name} (id={me.id})")
    P(f"  Account DC:  DC{account_dc}")
    P(f"  Premium:     {me.premium or False}")
    P(THIN)

    # ===== 2. resolve channel + message =====
    P(f"  Resolving    {CHANNEL}/{MSG_ID} ...")
    logger.info(f"resolving: {CHANNEL}/{MSG_ID}")
    entity = await client.get_entity(CHANNEL)
    msg = await client.get_messages(entity, ids=MSG_ID)
    if not msg or not msg.document:
        P("  [FAIL] Message has no document!")
        logger.error("[FAIL] message has no document")
        await client.disconnect()
        return

    doc = msg.document
    file_name = "unknown"
    for attr in doc.attributes:
        if hasattr(attr, "file_name") and attr.file_name:
            file_name = attr.file_name
            break
    file_size = doc.size or 0
    file_dc = doc.dc_id

    if account_dc == file_dc:
        routing = "SAME DC"
        use_dc_pool = False
    else:
        routing = f"CROSS DC  (DC{account_dc} -> DC{file_dc})"
        use_dc_pool = True

    P(f"  File:        {file_name}")
    P(f"  Size:        {sz(file_size)} ({file_size:,} bytes)")
    P(f"  File DC:     DC{file_dc}")
    P(f"  Routing:     {routing}")
    logger.info(f"file={file_name}, size={file_size}, file_dc=DC{file_dc}, routing={routing}")

    # ===== 3. file location =====
    file_location = InputDocumentFileLocation(
        id=doc.id,
        access_hash=doc.access_hash,
        file_reference=doc.file_reference,
        thumb_size="",
    )

    # ===== 4. create client pool =====
    pool_size = get_config_value("connection_pool_size", 5)
    string_session = StringSession.save(client.session)

    P(THIN)
    P(f"  Creating {pool_size} connections ...")
    clients = []

    async def create_conn(idx):
        try:
            c = TelegramClient(
                StringSession(string_session), API_ID, API_HASH,
                proxy=proxy_dict, flood_sleep_threshold=0,
            )
            await c.connect()
            if await c.is_user_authorized():
                logger.info(f"connection #{idx+1} OK")
                return c
            else:
                logger.warning(f"connection #{idx+1} not authorized")
                await c.disconnect()
                return None
        except Exception as e:
            logger.warning(f"connection #{idx+1} failed: {e}")
            return None

    for batch_start in range(0, pool_size, 4):
        batch_end = min(batch_start + 4, pool_size)
        tasks = [create_conn(i) for i in range(batch_start, batch_end)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if r and not isinstance(r, Exception):
                clients.append(r)

    if not clients:
        logger.error("[FAIL] no pool connections, fallback to single")
        P("  [WARN] Pool failed, using single connection")
        clients = [client]

    client_pool = ClientPool(clients)
    P(f"  Connections: {len(clients)} ready")

    # ===== 5. calculate parts =====
    part_size = get_config_value("worker_pool_part_size", 1024 * 1024)
    parts = []
    offset = 0
    idx = 0
    while offset < file_size:
        length = min(part_size, file_size - offset)
        parts.append(Part(index=idx, offset=offset, length=length,
                          is_last=(offset + length >= file_size)))
        offset += length
        idx += 1

    num_workers = min(get_config_value("download_threads", 5), len(parts))
    P(f"  Parts:       {len(parts)} x {part_size // 1024}KB")
    P(f"  Workers:     {num_workers}")

    # ===== 6. sparse file =====
    save_path = os.path.join(SAVE_DIR, file_name)
    temp_path = save_path + ".part"
    if os.path.exists(temp_path):
        os.remove(temp_path)
    create_sparse_file(temp_path, file_size)

    # ===== 7. download =====
    worker_pool = WorkerPool(
        client_pool=client_pool,
        file_path=temp_path,
        file_location=file_location,
        num_workers=num_workers,
        max_retries=3,
        main_client_for_export=client,
        chat_id=entity.id,
        msg_id=MSG_ID,
        file_dc=file_dc,
        account_dc=account_dc,
        use_dc_pool=use_dc_pool,
    )

    P(LINE)
    P()
    logger.info(
        f"DOWNLOAD START: workers={num_workers}, clients={client_pool.size}, "
        f"parts={len(parts)}, cross_dc={use_dc_pool}"
    )

    start_time = time.time()
    prog = {"dl": 0, "prev_dl": 0, "prev_t": start_time, "speed": 0.0}

    def on_progress(bytes_delta):
        prog["dl"] += bytes_delta
        now = time.time()
        dt = now - prog["prev_t"]
        if dt >= 0.4:
            prog["speed"] = (prog["dl"] - prog["prev_dl"]) / dt
            prog["prev_dl"] = prog["dl"]
            prog["prev_t"] = now

        pct = prog["dl"] / file_size * 100 if file_size else 0
        spd = prog["speed"]
        elapsed = now - start_time
        if spd > 0:
            eta = f"ETA {(file_size - prog['dl']) / spd:.0f}s"
        else:
            eta = "ETA --"

        line = (
            f"\r  {bar(pct)}  {pct:5.1f}%  "
            f"{sz(prog['dl'])}/{sz(file_size)}  "
            f"{spd / 1024 / 1024:.2f} MB/s  {eta}  [{elapsed:.1f}s]   "
        )
        sys.stdout.write(line)
        sys.stdout.flush()

    dl_results = await worker_pool.download_all(parts, progress_callback=on_progress)
    elapsed = time.time() - start_time

    # clear progress line
    sys.stdout.write("\r" + " " * 100 + "\r")
    sys.stdout.flush()

    # ===== 8. results =====
    ok_count = sum(1 for r in dl_results if r)
    fail_count = len(dl_results) - ok_count
    avg_speed = file_size / elapsed / 1024 / 1024 if elapsed > 0 else 0

    if fail_count == 0:
        if os.path.exists(save_path):
            os.remove(save_path)
        os.rename(temp_path, save_path)
        final_size = os.path.getsize(save_path)
        status = "SUCCESS"
    else:
        final_size = 0
        status = "FAILED"
        save_path = temp_path

    done_pct = ok_count / len(dl_results) * 100 if dl_results else 0
    P(f"  {bar(done_pct)}  {status}")
    P()
    P(LINE)
    P(f"  Result:      {status}  ({ok_count}/{len(dl_results)} parts)")
    P(f"  Time:        {elapsed:.1f}s")
    P(f"  Avg Speed:   {avg_speed:.2f} MB/s")
    P(THIN)
    P(f"  File:        {file_name}")
    if final_size:
        P(f"  File Size:   {final_size:,} bytes")
    P(f"  Save Path:   {save_path}")
    P(THIN)
    P(f"  Account DC:  DC{account_dc}")
    P(f"  File DC:     DC{file_dc}")
    P(f"  Cross DC:    {use_dc_pool}")
    P(f"  Workers:     {num_workers}")
    P(f"  Connections: {client_pool.size}")
    P(THIN)
    P(f"  Log:         {LOG_FILE}")
    P(LINE)

    logger.info(
        f"DOWNLOAD {status}: {ok_count}/{len(dl_results)} parts, "
        f"{elapsed:.1f}s, {avg_speed:.2f} MB/s"
    )

    if fail_count > 0:
        P(f"\n  [!] {fail_count} parts failed, .part file kept")

    # ===== 9. cleanup =====
    for c in clients:
        if c != client:
            try:
                await c.disconnect()
            except Exception:
                pass
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
