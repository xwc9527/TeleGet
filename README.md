# TeleGet — Telegram Downloader SDK

<p align="center">
  <strong>Multi-connection parallel file downloading for Telegram</strong><br>
  Built on <a href="https://github.com/LonamiWebs/Telethon">Telethon</a> · Python 3.9+ · Windows / Linux / macOS
</p>

---

## Why TeleGet?

If you've tried downloading large files from Telegram using Telethon's built-in `download_media()`, you've likely hit these walls:

**Speed ceiling.** Telethon downloads through a single MTProto connection. On a free account this tops out around 0.3–0.5 MB/s. A 2 GB video takes over an hour — if the connection doesn't drop first.

**Silent throttling.** Telegram's server silently imposes bandwidth limits per account. You won't see an error; your download just stalls repeatedly, tanking average throughput to unusable levels.

**FloodWait punishment.** Send requests too fast and Telegram locks you out with a `FloodWaitError` — anywhere from 30 seconds to 24 hours. Most wrappers either ignore this or leave you to handle it manually.

**No resume.** Connection drops at 95%? Start over. Telethon has no built-in checkpoint mechanism for partial downloads.

**Cross-DC pain.** Files hosted on a different datacenter than your account require special handling. Telethon doesn't handle this transparently for concurrent downloads.

TeleGet solves all of these with multi-connection parallel downloading, intelligent rate limiting, checkpoint resume, and automatic cross-DC routing.

---

## Download Performance

![download result](docs/8628832b-9b8f-41ca-86e8-ba2130e2e9ac.png)

> 2.16 GB file, free account, same DC — **7.55 MB/s**, 2214/2214 parts, zero failures.

---

## Core Features

- **Multi-connection parallel download** — splits files into chunks, multiple workers download simultaneously
- **Smart rate limiting** — avoids FloodWait and server-side throttling penalties
- **Cross-DC support** — automatically detects file datacenter and routes accordingly
- **Checkpoint resume** — interrupted downloads pick up where they left off, not from zero
- **Efficient disk I/O** — no temp files, no merge step
- **Account isolation** — accounts are fully isolated, crash-safe
- **Proxy auto-detection** — tries system proxy, common local ports, falls back to direct
- **Multi-account management** — switch between accounts without restarting

---

## Requirements

| Requirement | Details |
|-------------|---------|
| Python | >= 3.9 |
| OS | Windows 10+, Linux, macOS |
| Network | Telegram API accessible (direct or via proxy) |

### Telegram API Credentials

You need a Telegram API ID and API Hash from [my.telegram.org](https://my.telegram.org), and at least one Telethon `.session` file.

### Dependencies

| Package | Purpose | Install |
|---------|---------|---------|
| [Telethon](https://github.com/LonamiWebs/Telethon) >= 1.38.0 | Telegram MTProto client | Required |
| [psutil](https://github.com/giampaolo/psutil) >= 5.9.0 | Process monitoring | Required |
| [cryptg](https://github.com/cher-nov/cryptg) >= 0.4.0 | Encryption acceleration (~10x faster) | Recommended |
| [PySocks](https://github.com/Anorov/PySocks) >= 1.7.0 | SOCKS proxy support | Optional |

---

## Installation

```bash
# From PyPI
pip install teleget9527

# Recommended: with encryption acceleration
pip install teleget9527[fast]

# Full install (encryption + proxy)
pip install teleget9527[all]

# From source
git clone https://github.com/xwc9527/TeleGet.git
cd TeleGet
pip install -e ".[all]"
```

---

## Quick Start

### 1. Configure

```bash
cp .env.example .env
```

Edit `.env` with your API credentials and session path. See `.env.example` for all available options.

### 2. Session Setup

Place your Telethon `.session` file in the session directory:

```
data/
└── your_account_id/
    └── session.session
```

### 3. Test Login

```bash
python test_login.py
```

### 4. Test Download

```bash
python test_real_download.py
```

### 5. SDK Usage

```python
import asyncio
from tg_downloader import TGDownloader

async def main():
    downloader = TGDownloader(
        api_id=12345678,
        api_hash="your_api_hash",
        session_dir="./data",
    )

    await downloader.start("my_account")

    request_id = await downloader.download(
        chat_id=-1001234567890,
        msg_id=42,
        save_path="./downloads/video.mp4",
        progress_callback=lambda dl, total, pct: print(f"{pct:.1f}%"),
    )

    # Wait for completion...

    await downloader.shutdown()

asyncio.run(main())
```

The SDK provides 4 async methods: `start()`, `download()`, `cancel()`, `shutdown()`.

---

## What TeleGet Handles

If you're building a Telegram downloader and hitting these errors, TeleGet already handles them:

### Telegram API Errors — All Handled Automatically

- **`FloodWaitError: A wait of X seconds is required`** — Telegram locks you out for sending requests too fast. TeleGet auto-detects and decelerates before this happens.
- **`FloodPremiumWaitError`** — Free accounts get throttled 7–11 seconds per hit. TeleGet handles the backoff transparently.
- **`FILE_REFERENCE_EXPIRED` / `FILE_REFERENCE_INVALID`** — File metadata goes stale after ~1 hour. TeleGet auto-refreshes without restarting the download.
- **`AUTH_KEY_UNREGISTERED`** — DC auth key invalidated server-side. TeleGet rebuilds authorization automatically.
- **`AuthBytesInvalidError`** — Cross-DC auth race condition that crashes most implementations. TeleGet prevents it entirely.
- **`Server closed the connection` / `ConnectionError`** — Telegram drops sockets under load. TeleGet recovers without losing progress.
- **`0 bytes read on a total of 8 expected bytes`** — Silent connection death. TeleGet detects and reconnects.
- **`asyncio.TimeoutError`** — Requests hang indefinitely. TeleGet enforces per-request timeouts and rotates to healthy connections.
- **`WinError 32: The process cannot access the file`** — Windows file locking during rename. TeleGet retries automatically.

### Telegram Download Challenges — All Solved

- **Single-connection speed ceiling** (0.3–0.5 MB/s) → Multi-connection parallel download, 7+ MB/s on free accounts
- **Undocumented rate limits** → Multi-layer adaptive rate limiting that auto-tunes to your account's limits
- **FloodWait cascading failures** → Circuit breaker hierarchy isolates failures per-connection
- **Cross-DC file download** → Automatic DC detection, dedicated connection pool per datacenter
- **Cross-DC `AuthBytesInvalidError`** → Race condition prevention for concurrent auth exports
- **No download resume** → Part-level checkpoint persistence, survives crashes and restarts
- **Cross-session resume** → Switch accounts or restart app, download continues from where it left off
- **Connection storms after "Server closed"** → Disconnect detection with coordinated recovery
- **Download stalls at 99%** → Watchdog detects stuck workers and force-restarts them
- **Disk I/O bottleneck** → Sparse file pre-allocation, direct offset writes, no temp files or merge step
- **Multi-account auth collision** → Process-level account isolation, zero shared state

---

## Troubleshooting

**`FloodWaitError`** — TeleGet handles this automatically. If frequent, increase `rate_limiter_interval`.

**`FloodPremiumWaitError`** — Free account throttling. Lower `connection_pool_size` to reduce frequency.

**`FILE_REFERENCE_EXPIRED`** — Auto-refreshed up to 3 times. If persistent, the message may have been edited or deleted.

**`AUTH_KEY_UNREGISTERED`** — Auto-recovered. If persistent, your `.session` file may be corrupted — re-login.

**Download stalls** — Watchdog auto-recovers after 30s. Check logs for `[WATCHDOG]` entries.

**`WinError 32`** — Auto-retried. If persistent, another process may be holding the file open.

---

## License

AGPL-3.0 — see [LICENSE](LICENSE) for details.
