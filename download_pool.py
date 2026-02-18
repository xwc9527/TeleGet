"""
Worker Pool 并发下载模块

实现固定数量 Worker 从 Part 队列消费的生产者-消费者模式。
参考 iyear/tdl 的并发架构设计。

核心优势：
- 固定 8 个 Worker（而非 N 个分片 = N 个协程）
- 固定 512KB Parts（而非动态分片）
- Client Pool 轮询（而非静态分配）
- asyncio.Queue 生产者-消费者模式
"""

import asyncio
import os
import logging
from typing import Dict, List, Tuple, Optional, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 导入 Telegram API
from telethon.tl.functions.upload import GetFileRequest
from telethon.errors import FloodWaitError
from telethon.errors import FloodPremiumWaitError, AuthBytesInvalidError
import time
import random
import re

# ========== 全局熔断器 ==========
_CIRCUIT_BREAKER_STATE = {
    'is_open': False,           # True=熔断中，所有请求暂停
    'until_timestamp': 0,       # 熔断结束时间
    'trigger_count': 0,         # 累计触发次数（用于日志和监控）
}
_CIRCUIT_BREAKER_LOCK: Optional[asyncio.Lock] = None  # 延迟初始化

def _get_circuit_breaker_lock():
    """获取熔断器锁（延迟初始化，避免在导入时创建）"""
    global _CIRCUIT_BREAKER_LOCK
    if _CIRCUIT_BREAKER_LOCK is None:
        _CIRCUIT_BREAKER_LOCK = asyncio.Lock()
    return _CIRCUIT_BREAKER_LOCK

async def trigger_global_circuit_breaker(wait_seconds: int):
    """
    触发全局熔断 - 所有 Worker 立即停止请求

    Args:
        wait_seconds: Telegram 要求的等待时间（秒）
    """
    lock = _get_circuit_breaker_lock()
    async with lock:
        _CIRCUIT_BREAKER_STATE['is_open'] = True
        _CIRCUIT_BREAKER_STATE['until_timestamp'] = time.time() + wait_seconds
        _CIRCUIT_BREAKER_STATE['trigger_count'] += 1
        logger.warning(f"[CIRCUIT-BREAKER] Triggered global breaker! wait {wait_seconds}s, total triggers: {_CIRCUIT_BREAKER_STATE['trigger_count']}")

async def check_circuit_breaker():
    """
    检查熔断器状态 - 每个下载请求前调用

    如果熔断器打开，阻塞等待直到熔断结束。
    """
    lock = _get_circuit_breaker_lock()
    async with lock:
        if not _CIRCUIT_BREAKER_STATE['is_open']:
            return
        remaining = _CIRCUIT_BREAKER_STATE['until_timestamp'] - time.time()

    # 在锁外等待（避免长时间持锁阻塞其他 Worker）
    if remaining > 0:
        logger.info(f"[CIRCUIT-BREAKER] Breaker active, waiting {remaining:.1f}s...")
        await asyncio.sleep(remaining)

    # 熔断结束，重置状态
    async with lock:
        if _CIRCUIT_BREAKER_STATE['is_open']:
            now = time.time()
            if now >= _CIRCUIT_BREAKER_STATE['until_timestamp']:
                _CIRCUIT_BREAKER_STATE['is_open'] = False
                logger.info("[CIRCUIT-BREAKER] Breaker ended, resuming requests")

# ========== 连接级熔断器 ==========
_CONNECTION_CIRCUIT_BREAKER_STATE: Dict[int, Dict[str, object]] = {}
_CONNECTION_CIRCUIT_BREAKER_LOCK: Optional[asyncio.Lock] = None

def _get_connection_circuit_breaker_lock():
    """Lazy init lock for per-connection circuit breaker state."""
    global _CONNECTION_CIRCUIT_BREAKER_LOCK
    if _CONNECTION_CIRCUIT_BREAKER_LOCK is None:
        _CONNECTION_CIRCUIT_BREAKER_LOCK = asyncio.Lock()
    return _CONNECTION_CIRCUIT_BREAKER_LOCK

def _is_connection_circuit_breaker_enabled() -> bool:
    try:
        from download_config import get_config_value
        return bool(get_config_value("connection_circuit_breaker_enabled", True))
    except Exception:
        return True

def _get_connection_circuit_breaker_config() -> Tuple[float, int, float]:
    """Return (repeat_window_seconds, threshold, hard_seconds)."""
    try:
        from download_config import get_config_value
        repeat_window = float(get_config_value("connection_circuit_breaker_repeat_window_seconds", 1.0))
        threshold = int(get_config_value("connection_circuit_breaker_threshold", 2))
        hard_seconds = float(get_config_value("connection_circuit_breaker_hard_seconds", 2.0))
        return max(0.0, repeat_window), max(1, threshold), max(0.0, hard_seconds)
    except Exception:
        return 1.0, 2, 2.0

def _get_circuit_breaker_scope(default: str = "global") -> str:
    try:
        from download_config import get_config_value
        return str(get_config_value("circuit_breaker_scope", default))
    except Exception:
        return default

async def trigger_connection_circuit_breaker(client, wait_seconds: float, reason: str = "flood_wait", extra_tag: str = ""):
    """
    Per-connection circuit breaker: blocks only the affected connection.
    """
    if not _is_connection_circuit_breaker_enabled():
        return
    if _get_circuit_breaker_scope("global") != "connection":
        return
    if client is None:
        return

    now = time.time()
    safe_wait = float(wait_seconds or 0)
    if safe_wait <= 0:
        safe_wait = 1.0

    repeat_window, threshold, hard_seconds = _get_connection_circuit_breaker_config()
    client_id = id(client)
    dc_id = getattr(getattr(client, "session", None), "dc_id", None)

    lock = _get_connection_circuit_breaker_lock()
    async with lock:
        state = _CONNECTION_CIRCUIT_BREAKER_STATE.get(client_id)
        if state is None:
            state = {
                "is_open": False,
                "until_timestamp": 0.0,
                "trigger_count": 0,
                "repeat_count": 0,
                "last_trigger_ts": 0.0,
                "last_reason": "",
            }

        if now - state["last_trigger_ts"] <= repeat_window:
            state["repeat_count"] += 1
        else:
            state["repeat_count"] = 1

        state["last_trigger_ts"] = now
        state["trigger_count"] += 1
        state["last_reason"] = reason or "flood_wait"

        block_seconds = safe_wait
        if state["repeat_count"] >= threshold:
            block_seconds = max(block_seconds, hard_seconds)

        state["is_open"] = True
        state["until_timestamp"] = now + block_seconds
        _CONNECTION_CIRCUIT_BREAKER_STATE[client_id] = state

    tag = extra_tag or ""
    dc_info = f"dc={dc_id}" if dc_id is not None else "dc=?"
    logger.warning(f"[CONNECTION-CB] open client#{client_id % 1000} {dc_info} wait={block_seconds:.1f}s "
          f"(reason={reason}, repeats={state['repeat_count']}/{threshold}) {tag}")

async def check_connection_circuit_breaker(client, reason: str = "pre_request", extra_tag: str = ""):
    """
    Wait for per-connection breaker if open.
    """
    if not _is_connection_circuit_breaker_enabled():
        return
    if _get_circuit_breaker_scope("global") != "connection":
        return
    if client is None:
        return

    client_id = id(client)
    lock = _get_connection_circuit_breaker_lock()
    async with lock:
        state = _CONNECTION_CIRCUIT_BREAKER_STATE.get(client_id)
        if not state or not state.get("is_open"):
            return
        remaining = state.get("until_timestamp", 0.0) - time.time()

    if remaining > 0:
        tag = extra_tag or ""
        dc_id = getattr(getattr(client, "session", None), "dc_id", None)
        dc_info = f"dc={dc_id}" if dc_id is not None else "dc=?"
        logger.info(f"[CONNECTION-CB] wait client#{client_id % 1000} {dc_info} {remaining:.1f}s "
              f"(reason={reason}) {tag}")
        await asyncio.sleep(remaining)

    async with lock:
        state = _CONNECTION_CIRCUIT_BREAKER_STATE.get(client_id)
        if state and state.get("is_open") and time.time() >= state.get("until_timestamp", 0.0):
            state["is_open"] = False

# ========== 会话级断连熔断器 ==========
_SESSION_DISCONNECT_BREAKER_STATE = {
    'event_timestamps': [],       # 滑动窗口内的断连时间戳列表
    'is_paused': False,           # True=暂停中，所有 Worker 应等待
    'pause_until': 0.0,           # 暂停结束时间
    'cooldown_until': 0.0,        # 冷却期结束时间（防止连续触发）
    'trigger_count': 0,           # 累计触发次数（统计用）
    'total_disconnect_count': 0,  # 累计断连事件总数（统计用）
}
_SESSION_DISCONNECT_BREAKER_LOCK: Optional[asyncio.Lock] = None

def _get_session_disconnect_breaker_lock():
    """获取会话级断连熔断器锁（延迟初始化）"""
    global _SESSION_DISCONNECT_BREAKER_LOCK
    if _SESSION_DISCONNECT_BREAKER_LOCK is None:
        _SESSION_DISCONNECT_BREAKER_LOCK = asyncio.Lock()
    return _SESSION_DISCONNECT_BREAKER_LOCK

def _is_session_disconnect_breaker_enabled() -> bool:
    """检查会话级断连熔断器是否启用"""
    try:
        from download_config import get_config_value
        return bool(get_config_value("session_disconnect_breaker_enabled", True))
    except Exception:
        return True

def _get_session_disconnect_breaker_config() -> Tuple[float, int, float, float]:
    """返回 (window_seconds, threshold, pause_seconds, cooldown_seconds)"""
    try:
        from download_config import get_config_value
        window = float(get_config_value("session_disconnect_breaker_window_seconds", 5.0))
        threshold = int(get_config_value("session_disconnect_breaker_threshold", 3))
        pause = float(get_config_value("session_disconnect_breaker_pause_seconds", 3.0))
        cooldown = float(get_config_value("session_disconnect_breaker_cooldown_seconds", 10.0))
        return max(1.0, window), max(1, threshold), max(0.5, pause), max(1.0, cooldown)
    except Exception:
        return 5.0, 3, 3.0, 10.0

async def record_session_disconnect_event(extra_tag: str = ""):
    """
    记录一次 "Server closed" 事件

    由 Worker 的 connection-close 错误处理分支调用。
    在滑动窗口内累积事件，超过阈值时触发会话级暂停。

    Args:
        extra_tag: 日志标签（用于追踪来源 Worker）

    Returns:
        True: 触发了会话级暂停
        False: 未触发（在阈值内，或冷却期中，或功能禁用）
    """
    if not _is_session_disconnect_breaker_enabled():
        return False

    now = time.time()
    window_seconds, threshold, pause_seconds, cooldown_seconds = _get_session_disconnect_breaker_config()

    lock = _get_session_disconnect_breaker_lock()
    triggered = False

    async with lock:
        state = _SESSION_DISCONNECT_BREAKER_STATE
        state['total_disconnect_count'] += 1

        # 冷却期检查：防止连续触发
        if now < state['cooldown_until']:
            return False

        # 添加当前事件时间戳
        state['event_timestamps'].append(now)

        # 清理窗口外的旧事件
        cutoff = now - window_seconds
        state['event_timestamps'] = [ts for ts in state['event_timestamps'] if ts >= cutoff]

        # 检查是否达到阈值
        event_count = len(state['event_timestamps'])
        if event_count >= threshold:
            # 触发会话级暂停
            state['is_paused'] = True
            state['pause_until'] = now + pause_seconds
            state['cooldown_until'] = now + pause_seconds + cooldown_seconds
            state['trigger_count'] += 1
            state['event_timestamps'] = []  # 清空窗口，防止重复触发
            triggered = True

            logger.warning(
                f"[SESSION-DISCONNECT-BREAKER] Session-level pause triggered! "
                f"{event_count} disconnects in window >= threshold {threshold}, "
                f"pause {pause_seconds:.1f}s, cooldown {cooldown_seconds:.1f}s "
                f"(total triggers {state['trigger_count']}) {extra_tag}"
            )

    return triggered

async def check_session_disconnect_breaker(extra_tag: str = ""):
    """
    检查会话级断连熔断器

    每次下载请求前调用。如果会话级暂停中，阻塞等待直到暂停结束。

    Args:
        extra_tag: 日志标签
    """
    if not _is_session_disconnect_breaker_enabled():
        return

    lock = _get_session_disconnect_breaker_lock()
    remaining = 0.0

    async with lock:
        state = _SESSION_DISCONNECT_BREAKER_STATE
        if not state['is_paused']:
            return
        remaining = state['pause_until'] - time.time()

    # 在锁外等待（避免长时间持锁阻塞其他 Worker）
    if remaining > 0:
        logger.info(
            f"[SESSION-DISCONNECT-BREAKER] Session-level pause active, waiting {remaining:.1f}s... {extra_tag}"
        )
        await asyncio.sleep(remaining)

    # 暂停结束，重置状态
    async with lock:
        state = _SESSION_DISCONNECT_BREAKER_STATE
        if state['is_paused']:
            now = time.time()
            if now >= state['pause_until']:
                state['is_paused'] = False
                logger.info(f"[SESSION-DISCONNECT-BREAKER] Session-level pause ended, resuming requests {extra_tag}")

def get_session_disconnect_breaker_stats() -> dict:
    """
    获取会话级断连熔断器统计

    Returns:
        统计字典
    """
    state = _SESSION_DISCONNECT_BREAKER_STATE
    return {
        'trigger_count': state.get('trigger_count', 0),
        'total_disconnect_count': state.get('total_disconnect_count', 0),
        'is_paused': state.get('is_paused', False),
    }

# ========== TDL 策略模式开关缓存 ==========
_TDL_STRATEGY_CACHE = None

def _is_tdl_strategy() -> bool:
    """检查是否启用 TDL 策略模式（缓存结果）"""
    global _TDL_STRATEGY_CACHE
    if _TDL_STRATEGY_CACHE is not None:
        return _TDL_STRATEGY_CACHE
    try:
        from download_config import get_config_value
        _TDL_STRATEGY_CACHE = bool(get_config_value("tdl_strategy_enabled", False))
        if _TDL_STRATEGY_CACHE:
            logger.info("[TDL-STRATEGY] TDL strategy mode enabled: skip RateLimiter / CB / Session Breaker")
    except Exception:
        _TDL_STRATEGY_CACHE = False
    return _TDL_STRATEGY_CACHE

# ========== 带宽限速器 ==========
_BANDWIDTH_LIMITER: Optional['BandwidthLimiter'] = None
_BANDWIDTH_LIMITER_LOCK: Optional[asyncio.Lock] = None

class BandwidthLimiter:
    """令牌桶带宽限速器"""

    def __init__(self, limit_mbps: float, burst_mb: float = 2.0):
        """
        Args:
            limit_mbps: 带宽上限（MB/s）
            burst_mb: 允许的突发量（MB）
        """
        self.limit_mbps = limit_mbps
        self.limit_bps = limit_mbps * 1024 * 1024  # 字节/秒
        self.burst_bytes = burst_mb * 1024 * 1024  # 突发容量（字节）
        self.tokens = self.burst_bytes  # 当前令牌（字节），初始满桶
        self.last_update = time.time()
        self._lock = asyncio.Lock()

        # 统计
        self.total_acquired = 0
        self.total_waited = 0.0
        self.wait_count = 0

        logger.info(f"[BANDWIDTH-LIMITER] init: limit={limit_mbps:.2f} MB/s, burst={burst_mb:.1f} MB")

    async def acquire(self, bytes_needed: int = 1024 * 1024) -> float:
        """
        获取带宽令牌，不足时等待

        Args:
            bytes_needed: 需要的字节数（默认 1MB = 1 Part）

        Returns:
            等待时间（秒），0 表示无需等待
        """
        async with self._lock:
            now = time.time()

            # 补充令牌
            elapsed = now - self.last_update
            self.tokens = min(self.burst_bytes, self.tokens + elapsed * self.limit_bps)
            self.last_update = now

            # 检查是否需要等待
            wait_time = 0.0
            if bytes_needed > self.tokens:
                # 计算需要等待的时间
                deficit = bytes_needed - self.tokens
                wait_time = deficit / self.limit_bps

            # 统计
            self.total_acquired += bytes_needed
            if wait_time > 0:
                self.total_waited += wait_time
                self.wait_count += 1

            # 扣减令牌（允许负值，通过等待补偿）
            self.tokens -= bytes_needed

        # 在锁外等待（不阻塞其他 Worker 的令牌计算）
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        return wait_time

    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            'limit_mbps': self.limit_mbps,
            'total_acquired_mb': self.total_acquired / (1024 * 1024),
            'total_waited_seconds': self.total_waited,
            'wait_count': self.wait_count,
            'current_tokens_mb': self.tokens / (1024 * 1024),
        }

def _get_bandwidth_limiter_lock() -> asyncio.Lock:
    """获取带宽限速器初始化锁"""
    global _BANDWIDTH_LIMITER_LOCK
    if _BANDWIDTH_LIMITER_LOCK is None:
        _BANDWIDTH_LIMITER_LOCK = asyncio.Lock()
    return _BANDWIDTH_LIMITER_LOCK

async def _get_bandwidth_limiter() -> Optional[BandwidthLimiter]:
    """获取或创建带宽限速器（懒初始化）"""
    global _BANDWIDTH_LIMITER

    if _BANDWIDTH_LIMITER is not None:
        return _BANDWIDTH_LIMITER

    lock = _get_bandwidth_limiter_lock()
    async with lock:
        # 双重检查
        if _BANDWIDTH_LIMITER is not None:
            return _BANDWIDTH_LIMITER

        try:
            from download_config import get_config_value
            enabled = bool(get_config_value("bandwidth_limit_enabled", False))
            if not enabled:
                logger.info("[BANDWIDTH-LIMITER] disabled (bandwidth_limit_enabled=False)")
                return None

            limit_mbps = float(get_config_value("bandwidth_limit_mbps", 6.15))
            burst_mb = float(get_config_value("bandwidth_limit_burst_mb", 2.0))

            _BANDWIDTH_LIMITER = BandwidthLimiter(limit_mbps, burst_mb)
            return _BANDWIDTH_LIMITER

        except Exception as e:
            logger.error(f"[BANDWIDTH-LIMITER] init failed: {e}")
            return None

_ORIGINAL_TRIGGER_GLOBAL_CIRCUIT_BREAKER = trigger_global_circuit_breaker
_ORIGINAL_CHECK_CIRCUIT_BREAKER = check_circuit_breaker

async def trigger_global_circuit_breaker(wait_seconds: int):
    """Scoped wrapper: allow per-connection mode to bypass global breaker."""
    try:
        from download_config import get_config_value
        scope = get_config_value("circuit_breaker_scope", "global")
        if scope != "global":
            logger.info(f"[CIRCUIT-BREAKER] Global breaker skipped (scope={scope}, wait={wait_seconds}s)")
            return
    except Exception:
        pass
    return await _ORIGINAL_TRIGGER_GLOBAL_CIRCUIT_BREAKER(wait_seconds)

async def check_circuit_breaker():
    """Scoped wrapper: skip global breaker when scope != global."""
    try:
        from download_config import get_config_value
        scope = get_config_value("circuit_breaker_scope", "global")
        if scope != "global":
            return
    except Exception:
        pass
    return await _ORIGINAL_CHECK_CIRCUIT_BREAKER()

# ========== 不可恢复错误识别 ==========
UNRECOVERABLE_ERRORS = [
    "The provided authorization is invalid",
    "AUTH_KEY_DUPLICATED",
]

# Auth Key 失效错误（可通过池重建恢复）
AUTH_KEY_POOL_RESET_ERRORS = [
    "The key is not registered in the system",
    "AUTH_KEY_UNREGISTERED",
]

def _is_auth_key_error(error: Exception) -> bool:
    """判断是否为 Auth Key 失效错误"""
    error_msg = str(error).lower()
    for auth_err in AUTH_KEY_POOL_RESET_ERRORS:
        if auth_err.lower() in error_msg:
            return True
    return False

# File Reference 可恢复错误列表
RECOVERABLE_FILE_REFERENCE_ERRORS = [
    "FILE_REFERENCE_EXPIRED",
    "FILE_REFERENCE_INVALID",
    "file reference expired",
    "file reference invalid",
]

def _is_file_reference_error(error: Exception) -> bool:
    """判断是否为 File Reference 错误"""
    error_msg = str(error).lower()
    for ref_error in RECOVERABLE_FILE_REFERENCE_ERRORS:
        if ref_error.lower() in error_msg:
            return True
    return False

def _is_unrecoverable_error(error: Exception) -> bool:
    """判断是否为不可恢复错误"""
    error_msg = str(error).lower()
    for unrecoverable in UNRECOVERABLE_ERRORS:
        if unrecoverable.lower() in error_msg:
            return True
    return False

# ========== DC 专用连接池 ==========
_GLOBAL_DC_CLIENT_POOLS: dict = {}
_DC_POOL_TOTALS: Dict[int, int] = {}
_DC_POOL_READY: Dict[int, bool] = {}
_DC_POOL_SELF_HEAL_TASKS: Dict[int, asyncio.Task] = {}

_DC_POOL_FAIL_COUNT: Dict[int, int] = {}
_DC_POOL_COOLDOWN_UNTIL: Dict[int, float] = {}
_DC_POOL_MAX_INIT_RETRIES = 3
_DC_POOL_BACKOFF_BASE = 10.0
_DC_POOL_BACKOFF_MAX = 300.0

def _dc_pool_total_get(dc_id: int) -> int:
    return int(_DC_POOL_TOTALS.get(dc_id, 0))

def _dc_pool_total_set(dc_id: int, value: int) -> None:
    _DC_POOL_TOTALS[dc_id] = max(0, int(value or 0))

def _dc_pool_total_inc(dc_id: int, delta: int = 1) -> None:
    _DC_POOL_TOTALS[dc_id] = max(0, int(_DC_POOL_TOTALS.get(dc_id, 0)) + int(delta or 0))

def _dc_pool_total_dec(dc_id: int, delta: int = 1) -> None:
    _DC_POOL_TOTALS[dc_id] = max(0, int(_DC_POOL_TOTALS.get(dc_id, 0)) - int(delta or 0))
_DC_POOL_SIZE = 4

def _get_dc_pool_size() -> int:
    """Get DC pool size from config/env with safe fallback."""
    # Env override takes priority
    env_val = os.getenv("DOWNLOAD_DC_POOL_SIZE") or os.getenv("DC_POOL_SIZE")
    if env_val:
        try:
            size = int(env_val)
            if size > 0:
                return size
        except Exception:
            pass

    # Config fallback
    try:
        from download_config import get_config_value
        size = int(get_config_value("dc_pool_size", _DC_POOL_SIZE))
        if size > 0:
            return size
    except Exception:
        pass

    return _DC_POOL_SIZE
_DC_POOL_LOCK: Optional[asyncio.Lock] = None  # 全局连接池操作锁（延迟初始化）
_DC_POOL_CREATING: dict = {}  # Key: dc_id, Value: asyncio.Event（防止重复创建）

_BACKGROUND_EXPAND_TASKS: Dict[int, asyncio.Task] = {}

_EXPORT_AUTH_LOCK_V2: Optional[asyncio.Lock] = None

def _get_export_auth_lock_v2():
    """获取导出授权锁（延迟初始化，防止并发授权冲突）"""
    global _EXPORT_AUTH_LOCK_V2
    if _EXPORT_AUTH_LOCK_V2 is None:
        _EXPORT_AUTH_LOCK_V2 = asyncio.Lock()
    return _EXPORT_AUTH_LOCK_V2

async def _export_auth_with_lock(main_client, target_dc: int, tag: str = ""):
    """
    统一的导出授权入口（串行化）
    - 背景：后台扩容与自愈任务并发导出，偶发 AuthBytesInvalidError
    - 方案：全局锁串行导出，避免会话状态冲突
    """
    if main_client is None:
        raise RuntimeError("[DC-POOL] main_client_for_export is None")
    lock = _get_export_auth_lock_v2()
    async with lock:
        if tag:
            logger.info(f"[DC-POOL] [AUTH-LOCK] {tag} exporting auth...")
        from telethon.tl.functions.auth import ExportAuthorizationRequest
        exported_auth = await main_client(ExportAuthorizationRequest(dc_id=target_dc))
        if tag:
            logger.info(f"[DC-POOL] [AUTH-LOCK] {tag} auth export succeeded")
        return exported_auth

async def _invalidate_dc_pool(target_dc: int):
    """
    销毁指定DC的连接池，强制下次访问时重建

    当 AuthKeyUnregisteredError 发生时，池内所有连接的 auth key 均已失效，
    必须销毁整个池并重新创建（重新 ExportAuth + ImportAuth）。

    Args:
        target_dc: 目标 DC 编号
    """
    dc_lock = _get_dc_pool_lock()
    async with dc_lock:
        pool = _GLOBAL_DC_CLIENT_POOLS.pop(target_dc, None)
        if pool:
            _drained = 0
            while not pool.empty():
                try:
                    c = pool.get_nowait()
                    if c.is_connected():
                        await c.disconnect()
                    _drained += 1
                except Exception:
                    pass
            logger.info(f"[DC-POOL] [INVALIDATE] DC{target_dc} pool destroyed (cleaned {_drained} connections), will rebuild on next access")
        else:
            logger.info(f"[DC-POOL] [INVALIDATE] DC{target_dc} pool does not exist (already cleaned or not created)")
        _DC_POOL_TOTALS.pop(target_dc, None)
        _DC_POOL_READY.pop(target_dc, None)
        # 重置失败计数，允许重建
        _DC_POOL_FAIL_COUNT.pop(target_dc, None)
        _DC_POOL_COOLDOWN_UNTIL.pop(target_dc, None)
        # 清理创建中标记
        if target_dc in _DC_POOL_CREATING:
            del _DC_POOL_CREATING[target_dc]

def _get_dc_pool_lock():
    """获取全局 DC 连接池锁（延迟初始化）"""
    global _DC_POOL_LOCK
    if _DC_POOL_LOCK is None:
        _DC_POOL_LOCK = asyncio.Lock()
    return _DC_POOL_LOCK

@dataclass
class Part:
    """下载分块"""
    index: int           # 分块序号
    offset: int          # 文件偏移
    length: int          # 分块长度
    is_last: bool        # 是否为最后一块（可能需要特殊处理）

# ========== 请求速率限制器 ==========
class RateLimiter:
    """批次速率限制器（滑动时间窗口 + AIMD 自适应）"""

    def __init__(self, batch_size: int = 4, interval: float = 0.2, worker_count: int = 0):
        self.batch_size = batch_size
        self.interval = interval
        self._lock = asyncio.Lock()
        self._count = 0                    # 当前窗口已发送的请求数
        self._window_start = 0.0           # 当前窗口开始时间
        self._total_acquired = 0           # 统计：总获取次数
        self._total_waited = 0             # 统计：总等待次数

        # 自适应速率调节（AIMD）
        try:
            from download_config import get_config_value
            self.adaptive = get_config_value("rate_limiter_adaptive", True)
            self.min_interval = get_config_value("rate_limiter_min_interval", 0.1)
            self.max_interval = get_config_value("rate_limiter_max_interval", 1.0)
            self.success_threshold = get_config_value("rate_limiter_success_threshold", 50)
            self.decrease_ratio = get_config_value("rate_limiter_decrease_ratio", 0.9)
            self.increase_step = get_config_value("rate_limiter_increase_step", 0.1)
        except Exception:
            # 配置加载失败，使用默认值
            self.adaptive = True
            self.min_interval = 0.1
            self.max_interval = 1.0
            self.success_threshold = 50
            self.decrease_ratio = 0.9
            self.increase_step = 0.1

        # 动态间隔状态
        self.current_interval = interval  # 初始值=配置的静态间隔
        self.success_streak = 0           # 连续成功计数
        self.flood_wait_count = 0         # FloodWait总次数
        self.last_adjust_time = 0.0       # 最后一次调整时间（用于日志节流）

        if self.adaptive:
            logger.info(f"[RATE-LIMITER] 初始化（自适应模式）: batch_size={batch_size}, 初始间隔={interval}s, 范围=[{self.min_interval}, {self.max_interval}]s, 成功阈值={self.success_threshold}")
        else:
            logger.info(f"[RATE-LIMITER] 初始化（固定模式）: batch_size={batch_size}, interval={interval}s, QPS={batch_size/interval:.1f}")

    async def acquire(self):
        """获取一个请求配额（阻塞直到获取成功）"""
        while True:
            wait_time = 0.0

            async with self._lock:
                now = time.time()

                effective_interval = self.current_interval if self.adaptive else self.interval

                # 检查是否需要开始新窗口
                if self._window_start == 0.0 or (now - self._window_start) >= effective_interval:
                    self._window_start = now
                    self._count = 0

                # 当前窗口还有配额
                if self._count < self.batch_size:
                    self._count += 1
                    self._total_acquired += 1
                    return  # 获取成功，立即返回

                # 配额用完，计算等待时间
                wait_time = effective_interval - (now - self._window_start)
                if wait_time > 0:
                    self._total_waited += 1

            # 在锁外等待（不阻塞其他 Worker）
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            # 循环重试

    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            "total_acquired": self._total_acquired,
            "total_waited": self._total_waited,
            "wait_ratio": self._total_waited / max(1, self._total_acquired),
            "current_interval": self.current_interval,
            "flood_wait_count": self.flood_wait_count,
            "success_streak": self.success_streak,
            "adaptive": self.adaptive,
        }

    def on_success(self):
        """Part下载成功时调用（连续成功→加速）"""
        if not self.adaptive:
            return

        self.success_streak += 1

        if self.success_streak >= self.success_threshold:
            old_interval = self.current_interval
            self.current_interval = max(
                self.min_interval,
                self.current_interval * self.decrease_ratio
            )
            self.success_streak = 0

            # 终端输出：每次调整都打印（方便观察收敛过程）
            logger.info(f"[RATE-LIMITER] [ADAPTIVE] [OK] 连续 {self.success_threshold} 次成功，加速: {old_interval:.3f}s -> {self.current_interval:.3f}s")

            # 日志节流5秒
            now = time.time()
            if now - self.last_adjust_time >= 5.0:
                self.last_adjust_time = now

    def on_flood_wait(self, wait_seconds: float):
        """触发FloodWait时调用（减速 + 重置连续成功计数）"""
        if not self.adaptive:
            return

        self.flood_wait_count += 1
        self.success_streak = 0  # 重置连续成功计数

        old_interval = self.current_interval
        self.current_interval = min(
            self.max_interval,
            self.current_interval + self.increase_step
        )

        # 终端输出：每次FloodWait都打印（关键事件）
        logger.warning(f"[RATE-LIMITER] [ADAPTIVE] [WARN] FloodWait#{self.flood_wait_count}（{wait_seconds}s），减速: {old_interval:.3f}s -> {self.current_interval:.3f}s")

    def get_adaptive_summary(self) -> str:
        """获取自适应状态摘要"""
        if not self.adaptive:
            return f"[RATE-LIMITER] 固定模式: interval={self.interval}s"

        return (
            f"[RATE-LIMITER] [ADAPTIVE] 任务摘要:\n"
            f"  初始间隔: {self.interval:.3f}s\n"
            f"  最终间隔: {self.current_interval:.3f}s\n"
            f"  FloodWait总次数: {self.flood_wait_count}\n"
            f"  总请求: {self._total_acquired}, 总等待: {self._total_waited}\n"
            f"  间隔变化: {'加速' if self.current_interval < self.interval else '减速' if self.current_interval > self.interval else '不变'} "
            f"({self.interval:.3f}s -> {self.current_interval:.3f}s)"
        )

_BaseRateLimiter = RateLimiter

class RateLimiter(_BaseRateLimiter):
    def __init__(self, batch_size: int = 4, interval: float = 0.2, worker_count: int = 0):
        super().__init__(batch_size=batch_size, interval=interval, worker_count=worker_count)

        # Only-backoff (no speed-up) settings
        self._only_backoff_enabled = False
        self._only_backoff_base_interval = interval
        self._only_backoff_cooldown_seconds = 1.0
        self._only_backoff_force_min_interval = True
        self._only_backoff_log_interval_seconds = 5.0
        self._only_backoff_cooldown_until = 0.0
        self._only_backoff_last_log_time = 0.0

        # Smooth scheduling settings
        self._smooth_enabled = False
        self._smooth_gap_override = 0.0
        self._smooth_lock = asyncio.Lock()
        self._smooth_next_time = 0.0
        self._legacy_disabled_logged = False

        # Cross-DC backoff isolation
        self._cross_dc_backoff_enabled = False
        self._cross_dc_disable_global_rate_limiter = False
        self._context_is_cross_dc = False

        try:
            from download_config import get_config_value
            self._only_backoff_enabled = bool(get_config_value("rate_limiter_only_backoff", False))
            self._only_backoff_base_interval = float(get_config_value("rate_limiter_only_backoff_base_interval", interval))
            self._only_backoff_cooldown_seconds = float(get_config_value("rate_limiter_only_backoff_cooldown_seconds", 1.0))
            self._only_backoff_force_min_interval = bool(get_config_value("rate_limiter_only_backoff_force_min_interval", True))
            self._only_backoff_log_interval_seconds = float(get_config_value("rate_limiter_only_backoff_log_interval_seconds", 5.0))
            self._smooth_enabled = bool(get_config_value("rate_limiter_smooth", False))
            self._smooth_gap_override = float(get_config_value("rate_limiter_smooth_gap_override", 0.0))
            self._cross_dc_backoff_enabled = bool(get_config_value("cross_dc_backoff_enabled", False))
            self._cross_dc_disable_global_rate_limiter = bool(get_config_value("cross_dc_disable_global_rate_limiter", False))
            if bool(get_config_value("dc_rate_limiter_skip_global", False)):
                self._cross_dc_disable_global_rate_limiter = True
        except Exception:
            pass

        if self._only_backoff_enabled:
            base_interval = self._only_backoff_base_interval
            if not isinstance(base_interval, (int, float)) or base_interval <= 0:
                base_interval = interval
            if base_interval < interval:
                base_interval = interval
            self._only_backoff_base_interval = base_interval

            if self._only_backoff_force_min_interval:
                try:
                    self.min_interval = max(self.min_interval, base_interval)
                except Exception:
                    self.min_interval = base_interval

            if not getattr(self, "adaptive", True):
                # Keep adaptive path for FloodWait handling
                self.adaptive = True
                logger.info("[RATE-LIMITER] [ONLY-BACKOFF] adaptive forced on for flood handling")

            if getattr(self, "current_interval", interval) < base_interval:
                self.current_interval = base_interval

            logger.info(
                f"[RATE-LIMITER] [ONLY-BACKOFF] enabled: base_interval={self._only_backoff_base_interval:.3f}s "
                f"cooldown={self._only_backoff_cooldown_seconds:.1f}s"
            )
            self._log_legacy_disabled("init")

        if self._smooth_enabled:
            gap = self._get_smooth_gap(self._effective_interval())
            logger.info(f"[RATE-LIMITER] [SMOOTH] enabled: gap~{gap:.3f}s (batch_size={self.batch_size})")

        if self._cross_dc_backoff_enabled and self._cross_dc_disable_global_rate_limiter:
            logger.info("[RATE-LIMITER] [CROSS-DC] global backoff disabled; cross-DC FloodWait won't raise global interval")

    def _effective_interval(self) -> float:
        try:
            return self.current_interval if getattr(self, "adaptive", False) else self.interval
        except Exception:
            return self.interval

    def _get_smooth_gap(self, effective_interval: float) -> float:
        if self._smooth_gap_override and self._smooth_gap_override > 0:
            return self._smooth_gap_override
        return max(0.0, effective_interval / max(1, self.batch_size))

    def set_context_is_cross_dc(self, is_cross_dc: bool) -> None:
        self._context_is_cross_dc = bool(is_cross_dc)

    def _log_legacy_disabled(self, context: str = "") -> None:
        if not self._only_backoff_enabled:
            return
        if self._legacy_disabled_logged:
            return
        self._legacy_disabled_logged = True
        suffix = f" ({context})" if context else ""
        logger.info(
            "[RATE-LIMITER] [DISABLED-LEGACY-AIMD] only_backoff enabled: speed-up disabled; "
            "ignored keys: rate_limiter_success_threshold, rate_limiter_decrease_ratio; "
            "rate_limiter_adaptive forced True for FloodWait; min_interval forced >= base_interval"
            + suffix
        )

    def _only_backoff_refresh(self) -> None:
        if not self._only_backoff_enabled:
            return
        now = time.time()
        if self._only_backoff_cooldown_until and now >= self._only_backoff_cooldown_until:
            if self.current_interval != self._only_backoff_base_interval:
                old_interval = self.current_interval
                self.current_interval = self._only_backoff_base_interval
                self._only_backoff_cooldown_until = 0.0
                if now - self._only_backoff_last_log_time >= self._only_backoff_log_interval_seconds:
                    logger.info(f"[RATE-LIMITER] [ONLY-BACKOFF] restore {old_interval:.3f}s -> {self.current_interval:.3f}s")
                    self._only_backoff_last_log_time = now

    async def _acquire_smooth(self):
        while True:
            wait_time = 0.0
            async with self._smooth_lock:
                self._only_backoff_refresh()
                now = time.time()
                effective_interval = self._effective_interval()
                gap = self._get_smooth_gap(effective_interval)

                if self._smooth_next_time == 0.0:
                    self._smooth_next_time = now

                if now >= self._smooth_next_time:
                    self._smooth_next_time = now + gap
                    self._total_acquired += 1
                    return

                wait_time = self._smooth_next_time - now
                if wait_time > 0:
                    self._total_waited += 1

            if wait_time > 0:
                await asyncio.sleep(wait_time)

    async def acquire(self):
        try:
            if self._context_is_cross_dc and (self._cross_dc_disable_global_rate_limiter or _should_skip_global_rate_limiter_for_cross_dc()):
                self._context_is_cross_dc = False
                if self._only_backoff_enabled:
                    self._only_backoff_refresh()
                return
        except Exception:
            self._context_is_cross_dc = False
        if self._smooth_enabled:
            await self._acquire_smooth()
            return
        self._only_backoff_refresh()
        return await super().acquire()

    def on_success(self):
        if self._only_backoff_enabled:
            try:
                self.success_streak += 1
            except Exception:
                pass
            self._log_legacy_disabled("on_success")
            return
        return super().on_success()

    def on_flood_wait(self, wait_seconds: float):
        if self._cross_dc_backoff_enabled and self._cross_dc_disable_global_rate_limiter and self._context_is_cross_dc:
            self.flood_wait_count += 1
            self.success_streak = 0
            logger.warning(
                f"[DISABLED-CROSS-DC-GLOBAL-BACKOFF] FloodWait#{self.flood_wait_count} "
                f"wait={wait_seconds}s (cross-dc); global interval unchanged"
            )
            self._context_is_cross_dc = False
            return

        if self._only_backoff_enabled:
            self.flood_wait_count += 1
            self.success_streak = 0
            old_interval = self.current_interval

            base_interval = self._only_backoff_base_interval or self.interval
            if base_interval < self.interval:
                base_interval = self.interval

            self.current_interval = min(
                self.max_interval,
                max(base_interval, self.current_interval + self.increase_step)
            )

            now = time.time()
            cooldown = max(float(wait_seconds or 0), float(self._only_backoff_cooldown_seconds))
            if cooldown <= 0:
                cooldown = self._only_backoff_cooldown_seconds or 1.0
            self._only_backoff_cooldown_until = now + cooldown

            logger.warning(
                f"[RATE-LIMITER] [ONLY-BACKOFF] FloodWait#{self.flood_wait_count} wait={wait_seconds}s "
                f"interval {old_interval:.3f}s -> {self.current_interval:.3f}s cooldown={cooldown:.1f}s"
            )
            self._log_legacy_disabled("on_flood_wait")
            self._context_is_cross_dc = False
            return
        return super().on_flood_wait(wait_seconds)

    def on_connection_closed(self):
        """断连时降速（步长比 FloodWait 更小）"""
        try:
            from download_config import get_config_value
            if not bool(get_config_value("rate_limiter_backoff_on_disconnect", True)):
                return
            disconnect_step = float(get_config_value("rate_limiter_disconnect_increase_step", 0.05))
        except Exception:
            disconnect_step = 0.05

        if disconnect_step <= 0:
            return

        self.success_streak = 0  # 重置连续成功计数

        old_interval = self.current_interval
        self.current_interval = min(
            self.max_interval,
            self.current_interval + disconnect_step
        )

        # 仅在间隔确实改变时打印日志（避免触顶后刷屏）
        if self.current_interval != old_interval:
            logger.warning(
                f"[RATE-LIMITER] [DISCONNECT-BACKOFF] interval "
                f"{old_interval:.3f}s -> {self.current_interval:.3f}s "
                f"(+{disconnect_step:.3f}s)"
            )

    def get_stats(self) -> dict:
        stats = super().get_stats()
        stats.update({
            "only_backoff": self._only_backoff_enabled,
            "only_backoff_base_interval": self._only_backoff_base_interval,
            "only_backoff_cooldown_until": self._only_backoff_cooldown_until,
            "smooth_enabled": self._smooth_enabled,
            "smooth_gap": self._get_smooth_gap(self._effective_interval()),
        })
        return stats

    def get_extended_summary(self) -> str:
        lines = []
        if self._only_backoff_enabled:
            lines.append(
                f"[RATE-LIMITER] [ONLY-BACKOFF] base={self._only_backoff_base_interval:.3f}s "
                f"cooldown={self._only_backoff_cooldown_seconds:.1f}s"
            )
            lines.append(
                "[RATE-LIMITER] [DISABLED-LEGACY-AIMD] speed-up disabled; "
                "ignored keys: rate_limiter_success_threshold, rate_limiter_decrease_ratio"
            )
        if self._smooth_enabled:
            lines.append(
                f"[RATE-LIMITER] [SMOOTH] gap~{self._get_smooth_gap(self._effective_interval()):.3f}s "
                f"batch_size={self.batch_size}"
            )
        return "\n".join(lines)

    def get_adaptive_summary(self) -> str:
        base = super().get_adaptive_summary()
        if self._only_backoff_enabled:
            base += (
                "\n[RATE-LIMITER] [DISABLED-LEGACY-AIMD] only_backoff enabled; speed-up disabled; "
                "ignored keys: rate_limiter_success_threshold, rate_limiter_decrease_ratio"
            )
        return base

# ========== per-DC rate limiter (only-backoff) ==========
_DC_RATE_LIMITERS: Dict[int, "DcRateLimiter"] = {}
_DC_RATE_LIMITER_LOCK: Optional[asyncio.Lock] = None

def _get_dc_rate_limiter_lock():
    global _DC_RATE_LIMITER_LOCK
    if _DC_RATE_LIMITER_LOCK is None:
        _DC_RATE_LIMITER_LOCK = asyncio.Lock()
    return _DC_RATE_LIMITER_LOCK

class DcRateLimiter:
    """
    Per-DC rate limiter (only-backoff).
    - Normal path: batched window (batch_size/interval)
    - FloodWait: interval += step, cooldown to base after N seconds
    """
    def __init__(self, dc_id: int, worker_count: int = 0):
        self.dc_id = dc_id
        self._lock = asyncio.Lock()
        self._count = 0
        self._window_start = 0.0
        self._total_acquired = 0
        self._total_waited = 0
        self._last_adjust_time = 0.0

        # Config
        try:
            from download_config import get_config_value
            self.batch_size = int(get_config_value("dc_rate_limiter_batch_size", 4))
            self.interval = float(get_config_value("dc_rate_limiter_interval", 0.2))
            self.min_interval = float(get_config_value("dc_rate_limiter_min_interval", 0.2))
            self.max_interval = float(get_config_value("dc_rate_limiter_max_interval", 1.0))
            self.increase_step = float(get_config_value("dc_rate_limiter_increase_step", 0.1))
            self.cooldown_seconds = float(get_config_value("dc_rate_limiter_cooldown_seconds", 1.0))
            self.only_backoff = bool(get_config_value("dc_rate_limiter_only_backoff", True))
            self.log_interval = float(get_config_value("dc_rate_limiter_log_interval_seconds", 5.0))
        except Exception:
            self.batch_size = 4
            self.interval = 0.2
            self.min_interval = 0.2
            self.max_interval = 1.0
            self.increase_step = 0.1
            self.cooldown_seconds = 1.0
            self.only_backoff = True
            self.log_interval = 5.0

        if self.batch_size <= 0:
            self.batch_size = 1
        if self.interval <= 0:
            self.interval = 0.2

        self.current_interval = max(self.interval, self.min_interval)
        self._cooldown_until = 0.0
        self._restore_at = 0.0
        self.flood_wait_count = 0

        logger.info(f"[DC-RATE-LIMITER] init dc={dc_id} batch_size={self.batch_size} interval={self.interval:.3f}s")

    def _refresh_restore(self):
        if not self.only_backoff:
            return
        now = time.time()
        if self._restore_at and now >= self._restore_at:
            if self.current_interval != self.interval:
                old_interval = self.current_interval
                self.current_interval = max(self.interval, self.min_interval)
                self._restore_at = 0.0
                if now - self._last_adjust_time >= self.log_interval:
                    logger.info(f"[DC-RATE-LIMITER] restore dc={self.dc_id} {old_interval:.3f}s -> {self.current_interval:.3f}s")
                    self._last_adjust_time = now

    async def acquire(self):
        while True:
            wait_time = 0.0
            async with self._lock:
                self._refresh_restore()
                now = time.time()
                effective_interval = self.current_interval if self.only_backoff else self.interval

                # honor cooldown window
                if self._cooldown_until and now < self._cooldown_until:
                    wait_time = self._cooldown_until - now
                else:
                    if self._window_start == 0.0 or (now - self._window_start) >= effective_interval:
                        self._window_start = now
                        self._count = 0

                    if self._count < self.batch_size:
                        self._count += 1
                        self._total_acquired += 1
                        return

                    wait_time = effective_interval - (now - self._window_start)
                    if wait_time > 0:
                        self._total_waited += 1

            if wait_time > 0:
                await asyncio.sleep(wait_time)

    def on_flood_wait(self, wait_seconds: float):
        self.flood_wait_count += 1
        now = time.time()
        old_interval = self.current_interval

        if self.only_backoff:
            self.current_interval = min(
                self.max_interval,
                max(self.min_interval, self.current_interval + self.increase_step)
            )

        wait_seconds = float(wait_seconds or 0)
        if wait_seconds > 0:
            self._cooldown_until = max(self._cooldown_until, now + wait_seconds)

        cooldown = max(float(self.cooldown_seconds or 0), 0.0)
        if cooldown > 0:
            self._restore_at = now + cooldown

        if now - self._last_adjust_time >= self.log_interval:
            logger.warning(
                f"[DC-RATE-LIMITER] FloodWait#{self.flood_wait_count} dc={self.dc_id} "
                f"wait={wait_seconds}s interval {old_interval:.3f}s -> {self.current_interval:.3f}s"
            )
            self._last_adjust_time = now

    def get_stats(self) -> dict:
        return {
            "total_acquired": self._total_acquired,
            "total_waited": self._total_waited,
            "wait_ratio": self._total_waited / max(1, self._total_acquired),
            "current_interval": self.current_interval,
            "flood_wait_count": self.flood_wait_count,
            "batch_size": self.batch_size,
        }

async def _get_dc_rate_limiter(dc_id: int, worker_count: int = 0) -> DcRateLimiter:
    lock = _get_dc_rate_limiter_lock()
    async with lock:
        limiter = _DC_RATE_LIMITERS.get(dc_id)
        if limiter is None:
            limiter = DcRateLimiter(dc_id=dc_id, worker_count=worker_count)
            _DC_RATE_LIMITERS[dc_id] = limiter
        return limiter

def _is_dc_rate_limiter_enabled() -> bool:
    try:
        from download_config import get_config_value
        return bool(get_config_value("dc_rate_limiter_enabled", False))
    except Exception:
        return False

def _is_floodwait_soft_retry_enabled() -> bool:
    try:
        from download_config import get_config_value
        return bool(get_config_value("floodwait_soft_retry_enabled", False))
    except Exception:
        return False

def _get_floodwait_soft_retry_margin() -> float:
    try:
        from download_config import get_config_value
        return float(get_config_value("floodwait_soft_retry_margin", 0.2))
    except Exception:
        return 0.2

def _should_skip_global_rate_limiter_for_cross_dc() -> bool:
    try:
        from download_config import get_config_value
        return bool(get_config_value("dc_rate_limiter_skip_global", False))
    except Exception:
        return False

_NON_PREMIUM_WAIT_PATTERN = re.compile(r"wait of (\d+) seconds", re.IGNORECASE)

def _extract_wait_seconds_from_error(err: Exception) -> int:
    """Best-effort extraction of wait seconds from Telethon errors/messages."""
    try:
        seconds = int(getattr(err, "seconds", 0) or 0)
        if seconds > 0:
            return seconds
    except Exception as e:
        logger.warning(f"[POOL-WARN] FloodWait seconds attribute parsing failed: {e}")
    try:
        msg = str(err)
        match = _NON_PREMIUM_WAIT_PATTERN.search(msg)
        if match:
            return int(match.group(1))
    except Exception as e:
        logger.warning(f"[POOL-WARN] FloodWait message pattern parsing failed: {e}")
    return 0

def _is_non_premium_wait_error(err: Exception) -> bool:
    """Detect non-premium wait errors (FloodPremiumWaitError or message pattern)."""
    try:
        if isinstance(err, FloodPremiumWaitError):
            return True
    except Exception as e:
        logger.warning(f"[POOL-WARN] FloodPremiumWaitError isinstance check failed: {e}")
    try:
        msg = str(err).lower()
        if "non-premium" in msg and "wait of" in msg:
            return True
    except Exception as e:
        logger.warning(f"[POOL-WARN] Non-premium wait message detection failed: {e}")
    return False

def _is_connection_closed_error(err: Exception) -> bool:
    msg = str(err).lower()
    if "server closed the connection" in msg:
        return True
    if "connection closed" in msg:
        return True
    if "connection reset" in msg:
        return True
    if "0 bytes read on a total of 8 expected bytes" in msg:
        return True
    if "connection lost" in msg:
        return True
    return False

class ClientPool:
    """
    Telegram Client 连接池（Daemon 进程内）

    【架构说明】
    此连接池是 Daemon 隔离架构的核心组件（正确设计）。

    [OK] 正确用法：
    - 每个 Daemon 进程创建一个 ClientPool 实例
    - ClientPool 管理该 Daemon 的 4-8 个连接
    - 连接池生命周期 = Daemon 进程生命周期
    - 账号切换 = 整个 Daemon 进程切换

    [FAIL] 错误用法（已废弃）：
    - 全局单例 ClientPool 管理 19 个连接
    - 跨账号共享 ClientPool - 触发 AUTH_KEY_DUPLICATED
    - 尝试动态切换 session - 无法原子操作

    【功能】
    - 支持 Round-Robin 轮询获取连接，提高连接利用率
    - 增加DC切换全局锁和状态缓存，防止并发切换竞态
    - 自动过滤无效连接，确保连接池健康

    【参考】
    - download_daemon_core.py - Daemon 内 ClientPool 创建
    - ARCHITECTURE.md 第 290-364 行 - Daemon 架构
    """

    def __init__(self, clients: List):
        """
        初始化连接池

        Args:
            clients: Telegram Client 列表

        Raises:
            RuntimeError: 如果没有可用的已连接 Client
        """
        # 过滤有效连接：确保所有 Client 都已连接且已授权
        self._clients = [
            c for c in clients
            if c.is_connected() and (hasattr(c, '_authorized') and c._authorized)
        ]

        if not self._clients:
            # 如果严格过滤后没有可用连接，尝试使用所有已连接的
            self._clients = [c for c in clients if c.is_connected()]

        if not self._clients:
            raise RuntimeError("没有可用的已连接 Telegram Client")

        if len(self._clients) < len(clients):
            logger.info(f"[CLIENT_POOL] 过滤后可用连接: {len(self._clients)}/{len(clients)}")
        else:
            logger.info(f"[CLIENT_POOL] 初始化成功，共 {len(self._clients)} 个连接")

        self._index = 0
        self._lock = asyncio.Lock()

        self._dc_switch_lock = asyncio.Lock()

        # Key: id(client), Value: dc_id (int)
        self._client_dc_cache = {}

    async def acquire(self):
        """
        获取下一个可用连接（Round-Robin）

        Returns:
            TelegramClient 实例
        """
        async with self._lock:
            try:
                total = len(self._clients)
                if total > 0:
                    selected = None
                    for _ in range(total):
                        client = self._clients[self._index]
                        self._index = (self._index + 1) % total
                        until = getattr(client, "_dl_quarantine_until", 0) or 0
                        if until and time.time() < float(until):
                            continue
                        selected = client
                        break
                    if selected is not None:
                        return selected
                    logger.warning("[CLIENT_POOL] [QUARANTINE] all clients quarantined, fallback to normal selection")
            except Exception as e:
                logger.warning(f"[CLIENT_POOL] [QUARANTINE] skip failed: {e}")

            client = self._clients[self._index]
            self._index = (self._index + 1) % len(self._clients)
            return client

    async def mark_client_quarantine(self, client, seconds: float, reason: str = ""):
        """Mark a client as temporarily quarantined to avoid immediate reuse."""
        if client is None:
            return
        try:
            wait_seconds = float(seconds or 0)
        except Exception:
            wait_seconds = 0.0
        if wait_seconds <= 0:
            wait_seconds = 1.0
        until_ts = time.time() + wait_seconds
        try:
            setattr(client, "_dl_quarantine_until", until_ts)
            setattr(client, "_dl_quarantine_reason", reason or "connection_issue")
        except Exception:
            pass
        try:
            logger.info(
                f"[CLIENT_POOL] [QUARANTINE] client#{id(client) % 1000} "
                f"wait={wait_seconds:.1f}s reason={reason}"
            )
        except Exception:
            pass

    @property
    def size(self) -> int:
        """可用连接数量"""
        return len(self._clients)

class StickyPartQueue:
    """Sticky part assignment queue for per-worker affinity."""
    def __init__(self, num_workers: int, total_parts: int, strategy: str = "contiguous", enable_log: bool = False):
        self._num_workers = max(1, int(num_workers or 1))
        self._total_parts = max(1, int(total_parts or 1))
        self._strategy = (strategy or "contiguous").lower()
        if self._strategy not in ("contiguous", "strided"):
            self._strategy = "contiguous"
        self._queues = [asyncio.Queue() for _ in range(self._num_workers)]
        self._sentinel_next = 0
        self._task_worker_map: Dict[int, int] = {}
        self._chunk_size = max(1, (self._total_parts + self._num_workers - 1) // self._num_workers)
        self._enable_log = bool(enable_log)
        if self._enable_log:
            logger.info(
                f"[WORKER_POOL] [STICKY] enabled strategy={self._strategy} "
                f"workers={self._num_workers} parts={self._total_parts} chunk={self._chunk_size}"
            )

    def register_worker_task(self, task, worker_id: int) -> None:
        try:
            setattr(task, "_sticky_worker_id", int(worker_id))
        except Exception:
            pass
        try:
            self._task_worker_map[id(task)] = int(worker_id)
        except Exception:
            pass

    def _resolve_worker_id(self, part) -> int:
        try:
            idx = int(getattr(part, "index", 0))
        except Exception:
            idx = 0
        if self._strategy == "strided":
            return idx % self._num_workers
        worker_id = idx // self._chunk_size
        if worker_id >= self._num_workers:
            worker_id = self._num_workers - 1
        if worker_id < 0:
            worker_id = 0
        return worker_id

    async def put(self, item) -> None:
        if item is None:
            worker_id = self._sentinel_next % self._num_workers
            self._sentinel_next += 1
            await self._queues[worker_id].put(None)
            return
        worker_id = self._resolve_worker_id(item)
        await self._queues[worker_id].put(item)

    async def get(self):
        worker_id = None
        try:
            task = asyncio.current_task()
        except Exception:
            task = None
        if task is not None:
            try:
                worker_id = getattr(task, "_sticky_worker_id", None)
            except Exception:
                worker_id = None
            if worker_id is None:
                try:
                    worker_id = self._task_worker_map.get(id(task))
                except Exception:
                    worker_id = None
        if worker_id is None or not isinstance(worker_id, int) or worker_id < 0 or worker_id >= self._num_workers:
            worker_id = 0
        return await self._queues[worker_id].get()

class WorkerPool:
    """
    固定数量 Worker 的下载池

    Worker 从 Part 队列消费，使用 ClientPool 获取连接下载。
    """

    def __init__(
        self,
        client_pool: ClientPool,
        file_path: str,
        file_location,
        num_workers: int = 8,
        max_retries: int = 3,
        main_client_for_export=None,
        chat_id=None,
        msg_id=None,
        file_dc: int = None,
        account_dc: int = None,
        use_dc_pool: bool = False,
    ):
        """
        初始化 Worker Pool

        Args:
            client_pool: Telegram 连接池
            file_path: 目标文件路径
            file_location: Telegram 文件位置对象
            num_workers: Worker 数量
            max_retries: 每个 Part 的最大重试次数
            main_client_for_export: 主客户端（用于跨 DC 时导出授权）
            chat_id: 聊天ID（用于刷新file_reference）
            msg_id: 消息ID（用于刷新file_reference）
            file_dc: 文件所在 DC 编号
            account_dc: 账号所在 DC 编号
            use_dc_pool: 是否使用 DC 专用连接池
        """
        self.client_pool = client_pool
        self.file_path = file_path
        self.file_location = file_location
        self.num_workers = num_workers
        self.max_retries = max_retries
        self.main_client_for_export = main_client_for_export

        self.chat_id = chat_id
        self.msg_id = msg_id
        self._file_reference_refresh_count = 0

        self.file_dc = file_dc
        self.account_dc = account_dc
        self.use_dc_pool = use_dc_pool

        self._part_queue: asyncio.Queue[Optional[Part]] = asyncio.Queue()
        self._sticky_queue_enabled = False
        self._sticky_queue_strategy = "contiguous"
        self._sticky_queue_log = False
        try:
            from download_config import get_config_value
            self._sticky_queue_enabled = bool(get_config_value("worker_pool_sticky_assignment", False))
            self._sticky_queue_strategy = str(get_config_value("worker_pool_sticky_strategy", "contiguous")).lower()
            self._sticky_queue_log = bool(get_config_value("worker_pool_sticky_log", False))
        except Exception:
            pass
        self._dc_rate_limiter_dc_id = None
        self._dc_rate_limiter_seen = False

        self._results: List[bool] = []
        self._lock = asyncio.Lock()
        self._progress_callback: Optional[Callable[[int], None]] = None
        self._total_downloaded = 0
        self._total_file_size = 0
        self._active_workers = 0  # 正在工作的Worker数
        self._parts_done = 0      # 已完成的Part数
        self._total_parts = 0     # 总Part数
        self._last_speed_log_time = 0   # 上次打印速度的时间
        self._last_speed_log_bytes = 0  # 上次打印速度时的字节数

        # 轮询替补：活跃 Worker 信号量
        self._rotation_enabled = False
        self._active_semaphore = None
        try:
            from download_config import get_config_value as _rot_gcv
            self._rotation_enabled = bool(_rot_gcv("rotation_enabled", False))
            if self._rotation_enabled:
                _active_slots = int(_rot_gcv("rotation_active_slots", 12))
                self._active_semaphore = asyncio.Semaphore(_active_slots)
                _backup_count = num_workers - _active_slots
                logger.info(f"[WORKER_POOL] [ROTATION] 轮询替补已启用: {_active_slots} 个活跃槽位, {num_workers} 个 Worker ({_backup_count}个备用)")
        except Exception:
            pass

        logger.info(f"[WORKER_POOL] ========================================")
        logger.info(f"[WORKER_POOL] 初始化 WorkerPool")
        logger.info(f"[WORKER_POOL]   Workers: {num_workers}")
        logger.info(f"[WORKER_POOL]   Clients: {client_pool.size}")
        logger.info(f"[WORKER_POOL]   重试次数: {max_retries}")
        logger.info(f"[WORKER_POOL]   文件: {file_path}")
        logger.info(f"[WORKER_POOL] ========================================")

        # 性能追踪
        self._start_time = None
        self._last_progress_log_time = 0
        self._last_progress_bytes = 0
        self._non_premium_wait_count = 0
        self._non_premium_wait_total_seconds = 0.0
        self._connection_closed_count = 0

        self._session_disconnect_breaker_triggered = 0
        self._reconnect_jitter_enabled = True
        self._reconnect_jitter_max_ms = 500
        try:
            from download_config import get_config_value
            self._reconnect_jitter_enabled = bool(get_config_value("reconnect_jitter_enabled", True))
            self._reconnect_jitter_max_ms = int(get_config_value("reconnect_jitter_max_ms", 500))
        except Exception:
            pass

        # 单次请求超时保护 + 看门狗检测卡死
        self._per_request_timeout = 15
        self._watchdog_stall_seconds = 30
        self._watchdog_check_interval = 5
        self._request_timeout_count = 0
        self._watchdog_cancelled = False
        self._cancel_event: asyncio.Event = asyncio.Event()
        self._cancelled = False
        try:
            from download_config import get_config_value
            self._per_request_timeout = get_config_value("worker_pool_per_request_timeout", 15)
            self._watchdog_stall_seconds = get_config_value("worker_pool_watchdog_stall_seconds", 30)
            self._watchdog_check_interval = get_config_value("worker_pool_watchdog_check_interval", 5)
        except Exception:
            pass
        logger.info(
            f"[WORKER_POOL] per_request_timeout={self._per_request_timeout}s, "
            f"watchdog_stall={self._watchdog_stall_seconds}s, "
            f"watchdog_interval={self._watchdog_check_interval}s"
        )

        # 初始化速率限制器
        try:
            from download_config import get_config_value
            rate_limiter_enabled = get_config_value("rate_limiter_enabled", True)

            if rate_limiter_enabled:
                batch_size = get_config_value("rate_limiter_batch_size", 4)
                interval = get_config_value("rate_limiter_interval", 0.2)
                self._rate_limiter = RateLimiter(batch_size=batch_size, interval=interval, worker_count=num_workers)
            else:
                self._rate_limiter = None
                logger.info("[WORKER_POOL] [RATE-LIMITER] 速率限制已禁用")
        except Exception as e:
            logger.warning(f"[WORKER_POOL] [RATE-LIMITER] 初始化失败，禁用速率限制: {e}")
            self._rate_limiter = None

        if self.use_dc_pool:
            logger.info(f"[WORKER_POOL] [DC-ROUTING] 跨 DC 模式：账号 DC{self.account_dc} -> 文件 DC{self.file_dc}")
        else:
            logger.info(f"[WORKER_POOL] [DC-ROUTING] 同 DC 模式：DC{self.account_dc}，直接使用主连接池")

    async def _maybe_cross_dc_jitter(self, tag: str = ""):
        """
        Apply small jitter for cross-DC requests
        """
        if not self.use_dc_pool:
            return
        try:
            from download_config import get_config_value
            jitter_ms = float(get_config_value("cross_dc_request_jitter_ms", 0.0))
            if jitter_ms <= 0:
                return
            delay = random.uniform(0, jitter_ms) / 1000.0
            if delay > 0:
                await asyncio.sleep(delay)
                if get_config_value("debug_mode", False):
                    logger.debug(f"{tag} [CROSS-DC-JITTER] delay={delay*1000:.1f}ms")
        except Exception:
            return

    async def _maybe_reconnect_jitter(self, tag: str = ""):
        """
        重连后随机抖动

        在 "Server closed" 错误处理后调用，打散 Worker 恢复的同步性。

        Args:
            tag: 日志标签
        """
        if not self._reconnect_jitter_enabled:
            return
        if self._reconnect_jitter_max_ms <= 0:
            return

        try:
            delay_ms = random.uniform(0, self._reconnect_jitter_max_ms)
            delay_s = delay_ms / 1000.0
            if delay_s > 0:
                await asyncio.sleep(delay_s)
                # 仅在 debug 模式打印详细日志
                try:
                    from download_config import get_config_value
                    if get_config_value("debug_mode", False):
                        logger.debug(f"{tag} [RECONNECT-JITTER] delay={delay_ms:.0f}ms")
                except Exception:
                    pass
        except Exception:
            return

    async def _maybe_phase_spread_on_start(self, worker_id: int) -> None:
        """
        Optional startup phase spread to reduce burst collisions.
        """
        if not self.use_dc_pool:
            return
        try:
            from download_config import get_config_value
            delay_ms = 0.0
            if bool(get_config_value("worker_pool_phase_spread_enabled", False)):
                delay_ms = max(delay_ms, float(get_config_value("worker_pool_phase_spread_max_ms", 0.0)))
            if _is_dc_rate_limiter_enabled() and bool(get_config_value("dc_rate_limiter_phase_spread_enabled", False)):
                delay_ms = max(delay_ms, float(get_config_value("dc_rate_limiter_phase_spread_max_ms", 0.0)))
            if delay_ms <= 0:
                return
            divisor = max(1, int(self.num_workers or 1))
            slot = int(worker_id or 0) % divisor
            delay = (delay_ms * slot) / float(divisor)
            if delay <= 0:
                return
            await asyncio.sleep(delay / 1000.0)
            if get_config_value("debug_mode", False):
                logger.debug(f"[WORKER_POOL] [PHASE-SPREAD] worker={worker_id} delay={delay:.1f}ms")
        except Exception:
            return

    async def _maybe_wait_dc_pool_ready(self, worker_id: int) -> None:
        """
        跨 DC 冷启动等待

        只在 use_dc_pool=True 时生效。
        Worker 启动时，如果 DC 连接池当前连接数不足以覆盖该 Worker，
        则等待后台扩容直到有足够连接或超时。

        Args:
            worker_id: Worker 编号（0-based）
        """
        if not self.use_dc_pool:
            return
        if self.file_dc is None:
            return

        # 配置：最大等待时间（默认 15 秒，覆盖 1-2 个连接的创建时间）
        max_wait = 15.0
        check_interval = 0.5
        try:
            from download_config import get_config_value
            if not bool(get_config_value("worker_pool_cold_start_wait_enabled", True)):
                return
            max_wait = float(get_config_value("worker_pool_cold_start_max_wait_seconds", 15.0))
        except Exception:
            pass

        if max_wait <= 0:
            return

        pool = _GLOBAL_DC_CLIENT_POOLS.get(self.file_dc)
        if pool is None:
            # 连接池尚未创建，由 _download_part 中的 _ensure_dc_pool_initialized 处理
            return

        # 核心逻辑：worker_id < 当前连接总数 -> 无需等待
        # worker_id >= 当前连接总数 -> 等待扩容
        current_total = _dc_pool_total_get(self.file_dc)
        if worker_id < current_total:
            return

        # 需要等待
        start_wait = time.time()
        deadline = start_wait + max_wait
        logger.info(
            f"[WORKER-{worker_id}] [COLD-START] 等待 DC{self.file_dc} 连接池扩容 "
            f"(当前={current_total}, 需要>={worker_id + 1}, 超时={max_wait:.1f}s)"
        )

        while time.time() < deadline:
            current_total = _dc_pool_total_get(self.file_dc)
            if worker_id < current_total:
                waited = time.time() - start_wait
                logger.info(
                    f"[WORKER-{worker_id}] [COLD-START] DC{self.file_dc} 连接就绪 "
                    f"(当前={current_total}, 等待了 {waited:.1f}s)"
                )
                return
            await asyncio.sleep(check_interval)

        # 超时：仍然启动 Worker（退化为原来的阻塞行为）
        waited = time.time() - start_wait
        current_total = _dc_pool_total_get(self.file_dc)
        logger.info(
            f"[WORKER-{worker_id}] [COLD-START] 等待超时，继续启动 "
            f"(当前={current_total}, 等待了 {waited:.1f}s)"
        )

    def get_completed_indices(self) -> List[int]:
        """返回已成功下载的 Part 索引列表（GIL 保证 bool 读取原子性）"""
        if not hasattr(self, '_results') or not self._results:
            return []
        return [i for i, ok in enumerate(self._results) if ok]

    def cancel(self):
        """外部取消下载任务

        设置取消标志，Worker 在获取下一个 Part 时检测到标志后退出。
        同时向队列注入终止信号，唤醒阻塞在 queue.get() 的 Worker。
        """
        self._cancelled = True
        self._cancel_event.set()
        logger.info(f"[WORKER_POOL] [CANCEL] 收到取消信号，通知所有 Worker 退出")
        # 向队列注入 None 终止信号，唤醒阻塞的 Worker
        for _ in range(self.num_workers):
            try:
                self._part_queue.put_nowait(None)
            except Exception:
                pass

    def is_cancelled(self) -> bool:
        """检查是否已被取消"""
        return self._cancelled

    async def download_all(self, parts: List[Part], progress_callback: Optional[Callable[[int], None]] = None) -> bool:
        """
        下载所有分块

        Args:
            parts: 分块列表
            progress_callback: 进度回调函数，接收增量字节数

        Returns:
            True: 全部成功
            False: 有失败
        """
        self._progress_callback = progress_callback
        self._results = [False] * len(parts)
        self._total_downloaded = 0
        self._parts_done = 0
        self._total_parts = len(parts)
        self._start_time = time.time()
        self._last_speed_log_time = time.time()
        self._last_speed_log_bytes = 0

        # Enable sticky queue per strategy
        if getattr(self, "_sticky_queue_enabled", False):
            try:
                self._part_queue = StickyPartQueue(
                    num_workers=self.num_workers,
                    total_parts=len(parts),
                    strategy=self._sticky_queue_strategy,
                    enable_log=self._sticky_queue_log
                )
            except Exception as sticky_err:
                logger.warning(f"[WORKER_POOL] [STICKY] init failed: {sticky_err}")

        # 计算总大小
        total_size = sum(p.length for p in parts)
        self._total_file_size = total_size
        logger.info(f"[WORKER_POOL] ========================================")
        logger.info(f"[WORKER_POOL] 开始下载任务")
        logger.info(f"[WORKER_POOL]   Parts数量: {len(parts)}")
        logger.info(f"[WORKER_POOL]   总大小: {total_size:,} bytes ({total_size / (1024**3):.3f} GB)")
        logger.info(f"[WORKER_POOL]   Part大小: {parts[0].length if parts else 0:,} bytes")
        logger.info(f"[WORKER_POOL] ========================================")

        # 填充队列
        for part in parts:
            await self._part_queue.put(part)

        # 添加终止信号（None）
        for _ in range(self.num_workers):
            await self._part_queue.put(None)

        # 启动 Workers
        workers = [
            asyncio.create_task(self._worker(i))
            for i in range(self.num_workers)
        ]

        # Register worker tasks for sticky queue
        try:
            if hasattr(self._part_queue, "register_worker_task"):
                for idx, task in enumerate(workers):
                    self._part_queue.register_worker_task(task, idx)
        except Exception:
            pass

        # 看门狗保护的 Worker 等待（替代裸 asyncio.gather）
        # 看门狗监控下载进度，卡死时 cancel 所有 Worker
        watchdog_task = asyncio.create_task(self._progress_watchdog(workers))

        try:
            # 等待所有 Worker 完成（return_exceptions=True 防止单个 Worker CancelledError 打断其他）
            await asyncio.gather(*workers, return_exceptions=True)
        except Exception as gather_err:
            logger.error(f"[WORKER_POOL] [BUG-FIX-WATCHDOG] gather 异常: {gather_err}")
        finally:
            # 无论正常还是异常退出，都取消看门狗
            if not watchdog_task.done():
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass

        # 打印看门狗结果
        if self._watchdog_cancelled:
            logger.warning(
                f"[WORKER_POOL] [BUG-FIX-WATCHDOG] [WARN] 下载被看门狗终止 "
                f"(已完成 {self._parts_done}/{self._total_parts} Parts)"
            )

        # 打印取消结果
        if self._cancelled:
            logger.warning(
                f"[WORKER_POOL] [CANCEL] [WARN] 下载被用户取消 "
                f"(已完成 {self._parts_done}/{self._total_parts} Parts)"
            )

        # 下载完成统计
        elapsed = time.time() - self._start_time
        success_count = sum(1 for r in self._results if r)
        fail_count = len(self._results) - success_count
        total_size = sum(p.length for p in parts)
        speed_mbps = (total_size / (1024**2)) / elapsed if elapsed > 0 else 0

        logger.info(f"[WORKER_POOL] ========================================")
        logger.info(f"[WORKER_POOL] 下载完成")
        logger.info(f"[WORKER_POOL]   成功: {success_count}/{len(parts)} Parts")
        logger.info(f"[WORKER_POOL]   失败: {fail_count}")
        logger.info(f"[WORKER_POOL]   总字节数: {total_size:,} bytes ({total_size / (1024**3):.3f} GB)")
        logger.info(f"[WORKER_POOL]   耗时: {elapsed:.2f} 秒")
        logger.info(f"[WORKER_POOL]   平均速度: {speed_mbps:.2f} MB/s")
        logger.info(f"[WORKER_POOL]   成功率: {success_count / len(parts) * 100:.1f}%")

        # 打印速率限制统计
        if self._rate_limiter:
            stats = self._rate_limiter.get_stats()
            logger.info(f"[WORKER_POOL]   [RATE-LIMITER] 请求总数: {stats['total_acquired']}")
            logger.info(f"[WORKER_POOL]   [RATE-LIMITER] 等待次数: {stats['total_waited']}")
            logger.info(f"[WORKER_POOL]   [RATE-LIMITER] 等待比例: {stats['wait_ratio']:.1%}")

        if self._non_premium_wait_count > 0:
            logger.info(
                f"[WORKER_POOL]   [NON-PREMIUM-WAIT] 次数: {self._non_premium_wait_count}, "
                f"等待总计: {self._non_premium_wait_total_seconds:.1f}s"
            )
        if self._connection_closed_count > 0:
            logger.info(f"[WORKER_POOL]   [CONNECTION-CLOSE] 次数: {self._connection_closed_count}")

        # 会话级断连熔断器统计
        try:
            sdb_stats = get_session_disconnect_breaker_stats()
            if sdb_stats.get('total_disconnect_count', 0) > 0:
                logger.info(
                    f"[WORKER_POOL]   [SESSION-DISCONNECT-BREAKER] "
                    f"断连总数: {sdb_stats['total_disconnect_count']}, "
                    f"熔断触发: {sdb_stats['trigger_count']}, "
                    f"本任务触发: {self._session_disconnect_breaker_triggered}"
                )
        except Exception:
            pass

        # 超时统计
        if self._request_timeout_count > 0:
            logger.info(f"[WORKER_POOL]   [BUG-FIX-TIMEOUT] GetFileRequest 超时次数: {self._request_timeout_count}")
        if self._watchdog_cancelled:
            logger.warning(f"[WORKER_POOL]   [BUG-FIX-WATCHDOG] [WARN] 下载被看门狗强制终止")

        logger.info(f"[WORKER_POOL] ========================================")

        failed_indices = [i for i, success in enumerate(self._results) if not success]
        if failed_indices:
            logger.warning(f"[WORKER_POOL] [CHECKPOINT] [WARN] 有 {len(failed_indices)} 个 Part 失败")
            logger.warning(f"[WORKER_POOL] [CHECKPOINT] 失败 Part 索引: {failed_indices[:20]}{'...' if len(failed_indices) > 20 else ''}")
        else:
            logger.info(f"[WORKER_POOL] [CHECKPOINT] [OK] 全部 {len(self._results)} 个 Part 下载成功")

        # 打印 AIMD 自适应摘要
        if self._rate_limiter:
            # 终端输出：详细摘要
            logger.info(self._rate_limiter.get_adaptive_summary())

            # Optional extended summary
            try:
                if hasattr(self._rate_limiter, "get_extended_summary"):
                    extra_summary = self._rate_limiter.get_extended_summary()
                    if extra_summary:
                        logger.info(extra_summary)
            except Exception:
                pass

        # Per-DC limiter stats (optional)
        try:
            from download_config import get_config_value
            if bool(get_config_value("worker_pool_stats_detailed", False)) and self.use_dc_pool and _is_dc_rate_limiter_enabled():
                dc_id = self.file_dc if self.file_dc is not None else self._dc_rate_limiter_dc_id
                if dc_id is not None:
                    limiter = _DC_RATE_LIMITERS.get(dc_id)
                    if limiter:
                        dc_stats = limiter.get_stats()
                        logger.info(
                            f"[WORKER_POOL] [DC-RATE-LIMITER] dc={dc_id} "
                            f"acquired={dc_stats['total_acquired']} waited={dc_stats['total_waited']} "
                            f"wait_ratio={dc_stats['wait_ratio']:.1%} interval={dc_stats['current_interval']:.3f}s "
                            f"floodwait={dc_stats['flood_wait_count']}"
                        )
        except Exception:
            pass

            # 日志输出：简洁结论（仅自适应模式且间隔有变化时记录）
            try:
                if self._rate_limiter.adaptive:
                    interval_change = self._rate_limiter.current_interval - self._rate_limiter.interval
                    direction = "加速" if interval_change < 0 else ("减速" if interval_change > 0 else "不变")
                    logger.info(
                        f"[ADAPTIVE] 任务完成: 初始{self._rate_limiter.interval:.3f}s -> "
                        f"最终{self._rate_limiter.current_interval:.3f}s ({direction}), "
                        f"FloodWait {self._rate_limiter.flood_wait_count}次"
                    )
            except Exception:
                pass

        return self._results  # 返回 List[bool] 而非 bool

    # 下载进度看门狗
    # 后台协程：连续 _watchdog_stall_seconds 秒无进展则判定卡死，cancel 所有 Worker
    async def _progress_watchdog(self, worker_tasks: list):
        """
        下载进度看门狗

        监控下载进度，连续 N 秒无进展则取消所有 Worker。

        Args:
            worker_tasks: Worker asyncio.Task 列表
        """
        stall_start_time = None
        # 初始化为 -1 避免首次检查误判
        # 首次循环时 current_downloaded(>=0) > last_downloaded(-1) 恒成立，不会误报卡死
        last_downloaded = -1

        logger.info(
            f"[WORKER_POOL] [BUG-FIX-WATCHDOG] 看门狗已启动 "
            f"(stall_timeout={self._watchdog_stall_seconds}s, "
            f"check_interval={self._watchdog_check_interval}s)"
        )

        try:
            while True:
                await asyncio.sleep(self._watchdog_check_interval)

                current_downloaded = self._total_downloaded

                # 检查是否所有 Worker 都已结束（正常退出场景）
                all_done = all(t.done() for t in worker_tasks)
                if all_done:
                    logger.info("[WORKER_POOL] [BUG-FIX-WATCHDOG] 所有 Worker 已结束，看门狗退出")
                    break

                if current_downloaded > last_downloaded:
                    # 有进展，重置卡死计时
                    stall_start_time = None
                    last_downloaded = current_downloaded
                else:
                    # 无进展
                    if stall_start_time is None:
                        stall_start_time = time.time()
                        logger.warning(
                            f"[WORKER_POOL] [BUG-FIX-WATCHDOG] [WARN] 检测到无进展 "
                            f"(downloaded={current_downloaded:,} bytes, "
                            f"parts_done={self._parts_done}/{self._total_parts})"
                        )

                    stall_duration = time.time() - stall_start_time

                    # 每 30 秒打印一次卡死警告
                    if int(stall_duration) % 30 < self._watchdog_check_interval:
                        active_workers = sum(1 for t in worker_tasks if not t.done())
                        logger.warning(
                            f"[WORKER_POOL] [BUG-FIX-WATCHDOG] [WARN] 持续无进展 "
                            f"{stall_duration:.0f}s/{self._watchdog_stall_seconds}s "
                            f"(活跃Workers={active_workers}, "
                            f"parts_done={self._parts_done}/{self._total_parts})"
                        )

                    if stall_duration >= self._watchdog_stall_seconds:
                        # 超时！强制取消所有 Worker
                        active_workers = sum(1 for t in worker_tasks if not t.done())
                        logger.error(
                            f"[WORKER_POOL] [BUG-FIX-WATCHDOG] [FAIL] 下载卡死超时！"
                            f"连续 {stall_duration:.0f}s 无进展，"
                            f"强制取消 {active_workers} 个活跃 Worker"
                        )
                        # 计算进度百分比（防止除零）
                        _wd_pct = (current_downloaded / self._total_file_size * 100) if self._total_file_size > 0 else 0.0
                        logger.warning(
                            f"[WORKER_POOL] [BUG-FIX-WATCHDOG] 当前进度: "
                            f"{current_downloaded:,}/{self._total_file_size:,} bytes "
                            f"({_wd_pct:.1f}%), "
                            f"parts_done={self._parts_done}/{self._total_parts}"
                        )

                        self._watchdog_cancelled = True

                        for i, task in enumerate(worker_tasks):
                            if not task.done():
                                task.cancel()
                                logger.info(f"[WORKER_POOL] [BUG-FIX-WATCHDOG] 已取消 Worker-{i}")

                        break

        except asyncio.CancelledError:
            logger.info("[WORKER_POOL] [BUG-FIX-WATCHDOG] 看门狗被取消（正常退出）")
        except Exception as e:
            logger.error(f"[WORKER_POOL] [BUG-FIX-WATCHDOG] 看门狗异常: {e}")

    async def _worker(self, worker_id: int):
        """
        Worker 协程：从队列消费 Part 并下载

        Args:
            worker_id: Worker 编号
        """
        # Worker启动
        worker_start_time = time.time()
        parts_completed = 0

        # Optional startup spread
        await self._maybe_phase_spread_on_start(worker_id)

        # 跨DC冷启动：等待DC连接池有足够连接
        await self._maybe_wait_dc_pool_ready(worker_id)

        logger.info(f"[WORKER-{worker_id}] Worker 启动")

        while True:
            # 检查取消信号
            if self._cancelled:
                worker_elapsed = time.time() - worker_start_time
                logger.info(f"[WORKER-{worker_id}] [CANCEL] Worker 收到取消信号，退出 (已完成 {parts_completed} parts, 运行 {worker_elapsed:.1f}s)")
                break

            part = await self._part_queue.get()

            if part is None:
                # 终止信号
                worker_elapsed = time.time() - worker_start_time
                logger.info(
                    f"[WORKER-{worker_id}] ========== Worker 退出 ========== "
                    f"处理 Parts: {parts_completed}, "
                    f"运行时间: {worker_elapsed:.2f} 秒"
                )
                break

            # 日志格式：[WORKER-ID][PART-ID] 便于追踪特定 Part 的生命周期
            tag = f"[WORKER-{worker_id}][PART-{part.index}]"
            part_start_time = time.time()
            logger.info(f"{tag} 开始下载 | offset={part.offset:,} | length={part.length:,}")

            try:
                success = await self._download_part(worker_id, part)
                async with self._lock:
                    self._results[part.index] = success
                    parts_completed += 1

                # Part完成统计
                part_elapsed = time.time() - part_start_time
                part_speed = (part.length / (1024**2)) / part_elapsed if part_elapsed > 0 else 0
                if success:
                    logger.info(
                        f"{tag} [OK] 成功 | "
                        f"耗时={part_elapsed:.2f}s | "
                        f"速度={part_speed:.2f} MB/s"
                    )
                else:
                    logger.error(
                        f"{tag} [FAIL] 失败 | "
                        f"耗时={part_elapsed:.2f}s | "
                        f"速度={part_speed:.2f} MB/s"
                    )
            except Exception as e:
                logger.error(f"{tag} [FAIL] 异常失败: {e}")
                async with self._lock:
                    self._results[part.index] = False
                    parts_completed += 1
            # CancelledError 处理（Python 3.9+ 是 BaseException，不被 except Exception 捕获）
            except asyncio.CancelledError:
                worker_elapsed = time.time() - worker_start_time
                logger.info(
                    f"[WORKER-{worker_id}] [BUG-FIX-WATCHDOG] Worker 被看门狗取消 "
                    f"(正在处理 PART-{part.index})"
                )
                logger.info(
                    f"[WORKER-{worker_id}] [BUG-FIX-WATCHDOG]   已处理 Parts: {parts_completed}, "
                    f"运行时间: {worker_elapsed:.2f}s"
                )
                async with self._lock:
                    self._results[part.index] = False
                break

    async def _download_part(self, worker_id: int, part):
        """下载单个 Part"""
        from sparse_file_utils import ShardWriter

        if not _is_tdl_strategy():
            await check_circuit_breaker()

        if self._rate_limiter and self.use_dc_pool:
            try:
                if _should_skip_global_rate_limiter_for_cross_dc() and hasattr(self._rate_limiter, "set_context_is_cross_dc"):
                    self._rate_limiter.set_context_is_cross_dc(True)
            except Exception:
                pass

        if self._rate_limiter:
            await self._rate_limiter.acquire()

        tag = f"[WORKER-{worker_id}][PART-{part.index}]"

        _bw_limiter = await _get_bandwidth_limiter()
        if _bw_limiter:
            _bw_wait = await _bw_limiter.acquire(1024 * 1024)
            if _bw_wait > 0.01:
                logger.debug(f"{tag} [BANDWIDTH-LIMITER] 等待 {_bw_wait:.3f}s")

        if not _is_tdl_strategy():
            await check_session_disconnect_breaker(extra_tag=tag)

        MB = 1024 * 1024
        KB = 1024

        _has_active_slot = False
        if self._active_semaphore is not None:
            await self._active_semaphore.acquire()
            _has_active_slot = True

        # Part 级别的重试循环
        for attempt in range(self.max_retries):
            client = None
            try:
                # 获取连接
                client = await self.client_pool.acquire()

                with ShardWriter(self.file_path, part.offset, part.length, part.index) as writer:
                    downloaded = 0

                    while downloaded < part.length:
                        remaining = part.length - downloaded
                        current_offset = part.offset + downloaded

                        # 固定请求 1MB，让 Telegram 服务器自动处理剩余字节
                        # Telegram 会智能返回实际可用数据，减少握手次数
                        request_limit = 1024 * KB

                        # 1MB 边界保护
                        offset_mb = current_offset // MB
                        end_mb = (current_offset + request_limit - 1) // MB

                        if offset_mb != end_mb:
                            next_mb_boundary = (offset_mb + 1) * MB
                            request_limit = next_mb_boundary - current_offset
                            logger.debug(f"{tag} 1MB边界调整: offset={current_offset}, 调整后limit={request_limit}")

                        if not self.use_dc_pool:
                            # ========== 路径 1: 同 DC 下载（使用主连接池） ==========
                            try:
                                if not _is_tdl_strategy():
                                    await check_connection_circuit_breaker(client, reason="pre_request", extra_tag=tag)
                                result = await asyncio.wait_for(
                                    client(GetFileRequest(
                                        location=self.file_location,
                                        offset=current_offset,
                                        limit=request_limit
                                    )),
                                    timeout=self._per_request_timeout
                                )
                                data = result.bytes
                            except FloodPremiumWaitError as premium_err:
                                wait_seconds = _extract_wait_seconds_from_error(premium_err) or 3
                                if wait_seconds <= 0:
                                    wait_seconds = 3
                                logger.warning(f"{tag} [SAME-DC][NON-PREMIUM-WAIT] wait={wait_seconds}s err={premium_err}")
                                try:
                                    self._non_premium_wait_count += 1
                                    self._non_premium_wait_total_seconds += float(wait_seconds)
                                except Exception as e:
                                    logger.warning(f"[POOL-WARN] Same-DC NPW count tracking failed: {e}")
                                if _is_tdl_strategy():
                                    logger.info(f"{tag} [SAME-DC][DESTROY-REBUILD] NPW触发->销毁重建连接 (不等待{wait_seconds}s)")
                                    await client.disconnect()
                                    await client.connect()
                                    logger.info(f"{tag} [SAME-DC][DESTROY-REBUILD] 连接重建完成，新Session")
                                    continue
                            except FloodWaitError as flood_err:
                                wait_seconds = flood_err.seconds or 30
                                logger.warning(f"{tag} [同DC] 触发 FloodWait wait={wait_seconds}s")
                                try:
                                    self._non_premium_wait_count += 1
                                    self._non_premium_wait_total_seconds += float(wait_seconds)
                                except Exception as e:
                                    logger.warning(f"[POOL-WARN] Same-DC FloodWait count tracking failed: {e}")
                                if _is_tdl_strategy():
                                    logger.info(f"{tag} [SAME-DC][DESTROY-REBUILD] FloodWait触发->销毁重建连接 (不等待{wait_seconds}s)")
                                    await client.disconnect()
                                    await client.connect()
                                    logger.info(f"{tag} [SAME-DC][DESTROY-REBUILD] 连接重建完成，新Session")
                                    continue

                            # 空数据校验
                            if not data or len(data) == 0:
                                raise RuntimeError(f"[同DC] 服务器返回空数据 (offset={current_offset})")

                        else:
                            # ========== 路径 2: 跨 DC 下载 ==========
                            _dc_rate_limiter = None
                            if _is_dc_rate_limiter_enabled():
                                try:
                                    _dc_rate_limiter = await _get_dc_rate_limiter(self.file_dc, worker_count=self.num_workers)
                                    self._dc_rate_limiter_dc_id = self.file_dc
                                    self._dc_rate_limiter_seen = True
                                except Exception as dc_rl_err:
                                    logger.warning(f"{tag} [DC-RATE-LIMITER] init failed: {dc_rl_err}")
                            if _dc_rate_limiter and not _is_tdl_strategy():
                                await _dc_rate_limiter.acquire()

                            dc_client = None
                            _dc_pool_fallback = False
                            try:
                                dc_client = await self._get_dc_client_from_pool(self.file_dc)
                            except Exception as _pool_err:
                                logger.warning(f"{tag} [DC-POOL-FALLBACK] DC{self.file_dc} 连接池不可用: {_pool_err}")
                                logger.warning(f"{tag} [DC-POOL-FALLBACK] 回退到主连接（DC{self.account_dc}中继）")
                                dc_client = client
                                _dc_pool_fallback = True
                            try:
                                try:
                                    if not _is_tdl_strategy():
                                        await check_connection_circuit_breaker(dc_client, reason="pre_request", extra_tag=tag)
                                    await self._maybe_cross_dc_jitter(tag=tag)
                                    result = await asyncio.wait_for(
                                        dc_client(GetFileRequest(
                                            location=self.file_location,
                                            offset=current_offset,
                                            limit=request_limit
                                        )),
                                        timeout=self._per_request_timeout
                                    )
                                    data = result.bytes
                                except FloodPremiumWaitError as premium_err:
                                    wait_seconds = _extract_wait_seconds_from_error(premium_err) or 3
                                    if wait_seconds <= 0:
                                        wait_seconds = 3
                                    logger.warning(f"{tag} [CROSS-DC][NON-PREMIUM-WAIT] wait={wait_seconds}s err={premium_err}")
                                    try:
                                        self._non_premium_wait_count += 1
                                        self._non_premium_wait_total_seconds += float(wait_seconds)
                                    except Exception as e:
                                        logger.warning(f"[POOL-WARN] Cross-DC NPW count tracking failed: {e}")
                                    if _is_tdl_strategy():
                                        logger.info(f"{tag} [CROSS-DC][DESTROY-REBUILD] NPW触发->销毁重建连接 (不等待{wait_seconds}s)")
                                        await dc_client.disconnect()
                                        await dc_client.connect()
                                        logger.info(f"{tag} [CROSS-DC][DESTROY-REBUILD] 连接重建完成，新Session")
                                        continue
                                except FloodWaitError as flood_err:
                                    wait_seconds = flood_err.seconds or 30
                                    logger.warning(f"{tag} [跨DC-DC{self.file_dc}] 触发 FloodWait wait={wait_seconds}s")
                                    try:
                                        self._non_premium_wait_count += 1
                                        self._non_premium_wait_total_seconds += float(wait_seconds)
                                    except Exception as e:
                                        logger.warning(f"[POOL-WARN] Cross-DC FloodWait count tracking failed: {e}")
                                    if _is_tdl_strategy():
                                        logger.info(f"{tag} [CROSS-DC][DESTROY-REBUILD] FloodWait触发->销毁重建连接 (不等待{wait_seconds}s)")
                                        await dc_client.disconnect()
                                        await dc_client.connect()
                                        logger.info(f"{tag} [CROSS-DC][DESTROY-REBUILD] 连接重建完成，新Session")
                                        continue

                                # 空数据校验
                                if not data or len(data) == 0:
                                    raise RuntimeError(f"[跨DC-DC{self.file_dc}] 返回空数据 (offset={current_offset})")

                            finally:
                                if not _dc_pool_fallback:
                                    try:
                                        import sys
                                        exc = sys.exc_info()[1]
                                        if exc and _is_auth_key_error(exc):
                                            # Auth Key 失效 -> 销毁整个池（所有连接均无效）
                                            # 不归还连接，直接断开；池将在下次重试时重建
                                            logger.warning(f"{tag} [CROSS-DC] [REAUTH] Auth Key 失效，销毁 DC{self.file_dc} 连接池")
                                            try:
                                                if dc_client.is_connected():
                                                    await dc_client.disconnect()
                                            except Exception as e:
                                                logger.warning(f"[POOL-WARN] Cross-DC client disconnect during auth key error failed: {e}")
                                            try:
                                                await _invalidate_dc_pool(self.file_dc)
                                            except Exception as _inv_err:
                                                logger.error(f"{tag} [CROSS-DC] [REAUTH] 池销毁异常: {_inv_err}")
                                        elif exc and _is_connection_closed_error(exc):
                                            try:
                                                setattr(dc_client, "_dl_connection_failed", True)
                                            except Exception as e:
                                                logger.warning(f"[POOL-WARN] Cross-DC client quarantine mark failed: {e}")
                                            logger.warning(f"{tag} [CROSS-DC] mark connection failed: {exc}")
                                            # 归还标记为失败的连接
                                            await self._return_dc_client_to_pool(self.file_dc, dc_client)
                                        else:
                                            # 正常归还连接到池中
                                            await self._return_dc_client_to_pool(self.file_dc, dc_client)
                                    except Exception:
                                        # 安全兜底：异常时仍尝试归还
                                        try:
                                            await self._return_dc_client_to_pool(self.file_dc, dc_client)
                                        except Exception:
                                            pass

                        # ========== 通用数据处理逻辑（两条路径共享） ==========

                        # 步骤1: 计算实际要写入的字节数
                        bytes_to_write = min(len(data), remaining)

                        # 步骤2: 立即更新全局进度（不等待磁盘IO）
                        async with self._lock:
                            self._total_downloaded += bytes_to_write

                        # 步骤3: 立即触发进度回调（UI实时更新）
                        if self._progress_callback:
                            self._progress_callback(bytes_to_write)

                        # 步骤4: 写入磁盘（可能阻塞，但不影响UI进度）
                        writer.write(data[:bytes_to_write])
                        downloaded += bytes_to_write

                        now = time.time()
                        if now - self._last_speed_log_time >= 0.5:  # 每0.5秒刷新
                            elapsed = now - self._start_time
                            bytes_delta = self._total_downloaded - self._last_speed_log_bytes
                            time_delta = now - self._last_speed_log_time

                            # 瞬时速度和平均速度
                            current_speed_mbps = bytes_delta / time_delta / (1024*1024) if time_delta > 0 else 0
                            avg_speed_mbps = self._total_downloaded / elapsed / (1024*1024) if elapsed > 0 else 0
                            progress_pct = self._total_downloaded / self._total_file_size * 100 if self._total_file_size > 0 else 0

                            # ETA计算
                            remaining_bytes = self._total_file_size - self._total_downloaded
                            if avg_speed_mbps > 0:
                                eta_seconds = remaining_bytes / (avg_speed_mbps * 1024 * 1024)
                                eta_min = int(eta_seconds // 60)
                                eta_sec = int(eta_seconds % 60)
                                eta_str = f"{eta_min}m{eta_sec}s"
                            else:
                                eta_str = "计算中..."

                            # 终端实时输出（用\r覆盖上一行）
                            print(
                                f"\r[下载] {progress_pct:5.1f}% | "
                                f"{self._total_downloaded/(1024**3):6.2f}/{self._total_file_size/(1024**3):6.2f} GB | "
                                f"瞬时: {current_speed_mbps:7.2f} MB/s | "
                                f"平均: {avg_speed_mbps:7.2f} MB/s | "
                                f"连接数: {self.client_pool.size:2d} | "
                                f"Parts: {self._parts_done}/{self._total_parts} | "
                                f"ETA: {eta_str:>9s}",
                                end='', flush=True
                            )

                            self._last_speed_log_time = now
                            self._last_speed_log_bytes = self._total_downloaded

                # 成功完成整个 Part
                async with self._lock:
                    self._parts_done += 1

                if self._rate_limiter:
                    self._rate_limiter.on_success()

                if _has_active_slot:
                    self._active_semaphore.release()
                    _has_active_slot = False

                return True

            except Exception as e:
                if isinstance(e, asyncio.TimeoutError):
                    self._request_timeout_count += 1
                    logger.warning(
                        f"{tag} [BUG-FIX-TIMEOUT] [WARN] GetFileRequest 超时 "
                        f"({self._per_request_timeout}s)，"
                        f"尝试 {attempt + 1}/{self.max_retries}，"
                        f"累计超时 {self._request_timeout_count} 次"
                    )
                    # 标记 client 为不健康，促使连接池隔离
                    try:
                        if client is not None and hasattr(self.client_pool, "mark_client_quarantine"):
                            await self.client_pool.mark_client_quarantine(
                                client, 5.0, reason="request_timeout"
                            )
                            logger.info(f"{tag} [BUG-FIX-TIMEOUT] client 已隔离 5s")
                    except Exception as q_err:
                        logger.warning(f"{tag} [BUG-FIX-TIMEOUT] client 隔离失败: {q_err}")
                    # 不消耗重试次数，直接 continue 重试（换 client）
                    await asyncio.sleep(1)
                    continue

                if _is_non_premium_wait_error(e):
                    wait_seconds = _extract_wait_seconds_from_error(e) or 3
                    if wait_seconds <= 0:
                        wait_seconds = 3
                    logger.warning(f"{tag} [NON-PREMIUM-WAIT] wait={wait_seconds}s err={e}")
                    try:
                        self._non_premium_wait_count += 1
                        self._non_premium_wait_total_seconds += float(wait_seconds)
                    except Exception:
                        pass
                    try:
                        if _is_tdl_strategy():
                            logger.info(f"{tag} [FALLBACK][DESTROY-REBUILD] NPW触发->销毁重建连接 (不等待{wait_seconds}s)")
                            if client is not None:
                                await client.disconnect()
                                await client.connect()
                                logger.info(f"{tag} [FALLBACK][DESTROY-REBUILD] 连接重建完成，新Session")
                    except Exception:
                        pass
                    continue

                if _is_connection_closed_error(e):
                    try:
                        self._connection_closed_count += 1
                    except Exception as e:
                        logger.warning(f"[POOL-WARN] Connection closed count tracking failed: {e}")
                    try:
                        if client is not None and hasattr(self.client_pool, "mark_client_quarantine"):
                            await self.client_pool.mark_client_quarantine(client, 1.5, reason="connection_closed")
                            await trigger_connection_circuit_breaker(client, 1.5, reason="connection_closed", extra_tag=tag)
                            try:
                                setattr(client, "_dl_connection_failed", True)
                            except Exception:
                                pass
                    except Exception:
                        pass

                    if not _is_tdl_strategy():
                        try:
                            breaker_triggered = await record_session_disconnect_event(extra_tag=tag)
                            if breaker_triggered:
                                self._session_disconnect_breaker_triggered += 1
                            # 检查是否需要暂停（如果刚触发或已在暂停中）
                            await check_session_disconnect_breaker(extra_tag=tag)
                        except Exception as sdb_err:
                            logger.warning(f"{tag} [SESSION-DISCONNECT-BREAKER] error: {sdb_err}")

                    if not _is_tdl_strategy():
                        try:
                            if self._rate_limiter and hasattr(self._rate_limiter, "on_connection_closed"):
                                self._rate_limiter.on_connection_closed()
                        except Exception as e:
                            logger.warning(f"[POOL-WARN] RateLimiter on_connection_closed failed: {e}")

                    try:
                        await self._maybe_reconnect_jitter(tag=tag)
                    except Exception:
                        pass

                    # 不消耗重试次数，continue 重试（换 client）
                    continue

                if _is_file_reference_error(e):
                    logger.warning(f"{tag} [WARN] File Reference 错误，尝试刷新: {e}")

                    # 调用刷新方法
                    refresh_success = await self._refresh_file_reference()

                    if refresh_success:
                        logger.info(f"{tag} [OK] File Reference 已刷新，重试下载...")
                        # 刷新成功，不计入 attempt，立即重试
                        continue
                    else:
                        logger.warning(f"{tag} [FAIL] File Reference 刷新失败，按正常重试逻辑处理")
                        # 刷新失败，按正常重试逻辑处理（下方代码）

                if _is_auth_key_error(e):
                    logger.warning(f"{tag} [WARN] [REAUTH] Auth Key 失效（DC池已销毁），将在下次重试时重建池: {e}")
                    if attempt < self.max_retries - 1:
                        backoff = 2 ** attempt
                        logger.info(f"{tag} [REAUTH] {backoff}秒后重试（attempt {attempt + 1}/{self.max_retries}）...")
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        logger.error(f"{tag} [FAIL] [REAUTH] Auth Key 失效且重试耗尽（{self.max_retries}次）")
                        if _has_active_slot:
                            self._active_semaphore.release()
                            _has_active_slot = False
                        raise

                if _is_unrecoverable_error(e):
                    logger.error(f"{tag} [FAIL] 不可恢复错误，立即失败（无需重试）: {e}", exc_info=True)
                    if _has_active_slot:
                        self._active_semaphore.release()
                        _has_active_slot = False
                    raise  # 立即失败，不重试

                if attempt == self.max_retries - 1:
                    # 最后一次尝试失败，放弃
                    logger.error(f"{tag} 失败 (尝试 {attempt + 1}/{self.max_retries}): {e}")
                    if _has_active_slot:
                        self._active_semaphore.release()
                        _has_active_slot = False
                    raise
                else:
                    # 指数退避后重试
                    backoff = 2 ** attempt  # 1s, 2s, 4s
                    logger.warning(f"{tag} 失败 (尝试 {attempt + 1}/{self.max_retries})，{backoff}秒后重试: {e}")
                    await asyncio.sleep(backoff)

        if _has_active_slot:
            self._active_semaphore.release()
            _has_active_slot = False
        return False  # 理论上不会到这里

    async def _refresh_file_reference(self) -> bool:
        """
        刷新 File Reference

        当检测到 FILE_REFERENCE_EXPIRED/INVALID 时调用，
        重新获取消息并更新 file_location 的 file_reference。

        Returns:
            True: 刷新成功
            False: 刷新失败（chat_id/msg_id 不可用，或获取消息失败）
        """
        from telethon.tl.types import InputDocumentFileLocation

        # 检查是否有必要的参数
        if not self.main_client_for_export or not self.chat_id or not self.msg_id:
            logger.error(f"[WORKER_POOL] [FAIL] 无法刷新 file_reference：缺少 chat_id/msg_id")
            return False

        # 防止无限循环刷新
        if self._file_reference_refresh_count >= 3:
            logger.warning(f"[WORKER_POOL] [FAIL] file_reference 刷新次数达到上限（3次）")
            return False

        self._file_reference_refresh_count += 1
        logger.info(f"[WORKER_POOL] 刷新 file_reference（第 {self._file_reference_refresh_count} 次）...")

        try:
            # 重新获取消息
            msg = await self.main_client_for_export.get_messages(
                self.chat_id,
                ids=self.msg_id
            )

            if isinstance(msg, list):
                msg = msg[0] if msg else None

            if not msg or not getattr(msg, "document", None):
                logger.error(f"[WORKER_POOL] [FAIL] 消息不存在或无文档")
                return False

            # 更新 file_location
            document = msg.document
            new_file_location = InputDocumentFileLocation(
                id=document.id,
                access_hash=document.access_hash,
                file_reference=document.file_reference,
                thumb_size=''
            )

            self.file_location = new_file_location
            logger.info(f"[WORKER_POOL] [OK] file_reference 刷新成功（第 {self._file_reference_refresh_count} 次）")

            return True

        except Exception as e:
            logger.error(f"[WORKER_POOL] [FAIL] file_reference 刷新失败: {e}", exc_info=True)

            return False

    async def _ensure_dc_pool_initialized(self, target_dc: int):
        """
        确保 DC 连接池已初始化

        为目标 DC 创建 4 个专用连接，避免 8 Worker 抢 1 连接的竞态。

        Args:
            target_dc: 目标 DC 编号

        Raises:
            RuntimeError: 连接池创建失败
        """
        global _GLOBAL_DC_CLIENT_POOLS, _DC_POOL_CREATING
        try:
            new_size = _get_dc_pool_size()
            if new_size != _DC_POOL_SIZE:
                # NOTE: Keep this in-memory override so existing code paths use it.
                globals()["_DC_POOL_SIZE"] = new_size
                logger.info(f"[DC-POOL] override size: {_DC_POOL_SIZE}")
        except Exception as e:
            logger.warning(f"[DC-POOL] override size failed: {e}")

        # 快速检查：如果连接池已存在，直接返回
        if target_dc in _GLOBAL_DC_CLIENT_POOLS and target_dc not in _DC_POOL_READY:
            _DC_POOL_READY[target_dc] = True
        if target_dc in _GLOBAL_DC_CLIENT_POOLS:
            return

        _now = time.time()
        _cooldown_until = _DC_POOL_COOLDOWN_UNTIL.get(target_dc, 0)
        if _now < _cooldown_until:
            _fail_count = _DC_POOL_FAIL_COUNT.get(target_dc, 0)
            _remaining = _cooldown_until - _now
            raise RuntimeError(
                f"DC{target_dc} 连接池冷却中（{_remaining:.0f}s后重试，已连续失败{_fail_count}次）"
            )

        dc_lock = _get_dc_pool_lock()

        async with dc_lock:
            # 双重检查：可能在等待锁时已创建
            if target_dc in _GLOBAL_DC_CLIENT_POOLS:
                return

            while target_dc in _DC_POOL_CREATING:
                waited_event = _DC_POOL_CREATING[target_dc]
                logger.info(f"[DC-POOL] DC{target_dc} 连接池正在创建中，等待完成...")

                # 释放锁等待（不阻塞其他操作）
                dc_lock.release()
                try:
                    await asyncio.wait_for(waited_event.wait(), timeout=60.0)
                    logger.info(f"[DC-POOL] DC{target_dc} 连接池创建完成")
                except asyncio.TimeoutError:
                    logger.warning(f"[DC-POOL] DC{target_dc} 等待超时（60秒）")
                finally:
                    # 关键：重新获取锁后再做任何决策
                    await dc_lock.acquire()

                # 池已创建 -> 直接返回
                if target_dc in _GLOBAL_DC_CLIENT_POOLS:
                    logger.info(f"[DC-POOL] DC{target_dc} 连接池已由其他Worker创建，直接返回")
                    return

                # event身份比对：只有等待的那个event还在（创建者失败且无人接手），才清理并跳出
                current_event = _DC_POOL_CREATING.get(target_dc)
                if current_event is waited_event:
                    # 同一个event = 创建者已失败，清理后本Worker接手创建
                    del _DC_POOL_CREATING[target_dc]
                    logger.info(f"[DC-POOL] 已清理 DC{target_dc} 的过期标记，本Worker接手创建")
                    break
                # else: event已被另一个Worker替换 -> 回到while循环继续等待新的创建者

            # while循环结束（或从未进入），本Worker负责创建
            event = asyncio.Event()
            _DC_POOL_CREATING[target_dc] = event

        # 释放锁，开始创建（避免长时间占用全局锁）
        try:
            logger.info(f"[DC-POOL] 启动流水线模式：首连接优先，后台扩容")

            from telethon.tl.functions.auth import ExportAuthorizationRequest
            logger.info(f"[DC-POOL] 为 DC{target_dc} 创建 {_DC_POOL_SIZE} 个专用连接（每连接独立授权）...")

            # 2. 创建连接池队列
            pool = asyncio.Queue(maxsize=_DC_POOL_SIZE)

            # ========== 阶段1：创建第1个连接（阻塞，必须成功） ==========
            try:
                logger.info(f"[DC-POOL] [PIPELINE] DC{target_dc} 连接 #1/{_DC_POOL_SIZE}: 导出授权...")
                exported_auth_1 = await _export_auth_with_lock(
                    self.main_client_for_export,
                    target_dc,
                    tag=f"[PIPELINE] DC{target_dc} #1"
                )

                logger.info(f"[DC-POOL] [PIPELINE] DC{target_dc} 连接 #1: 创建连接...")
                client_1 = await self._create_single_dc_client(target_dc, exported_auth_1, 0)

                await pool.put(client_1)
                _dc_pool_total_inc(target_dc, 1)

                logger.info(f"[DC-POOL] [PIPELINE] DC{target_dc} 连接 #1/{_DC_POOL_SIZE} [OK] 就绪（Worker可开始下载）")

            except Exception as first_conn_err:
                # 第1个连接创建失败 = 致命错误，拒绝初始化
                logger.error(f"[DC-POOL] [PIPELINE] DC{target_dc} [FAIL] 首连接创建失败（致命）: {first_conn_err}")
                _fc = _DC_POOL_FAIL_COUNT.get(target_dc, 0) + 1
                _DC_POOL_FAIL_COUNT[target_dc] = _fc
                if _fc >= _DC_POOL_MAX_INIT_RETRIES:
                    _cooldown = min(
                        _DC_POOL_BACKOFF_BASE * (2 ** (_fc - _DC_POOL_MAX_INIT_RETRIES)),
                        _DC_POOL_BACKOFF_MAX
                    )
                    _DC_POOL_COOLDOWN_UNTIL[target_dc] = time.time() + _cooldown
                    logger.warning(f"[DC-POOL] DC{target_dc} 连续失败{_fc}次，进入冷却期{_cooldown:.0f}s")
                raise RuntimeError(f"无法创建 DC{target_dc} 连接池: 首连接创建失败: {first_conn_err}")

            # ========== 阶段2：注册连接池到全局，立即返回 ==========
            async with dc_lock:
                _GLOBAL_DC_CLIENT_POOLS[target_dc] = pool
                _dc_pool_total_set(target_dc, pool.qsize())
                _DC_POOL_READY[target_dc] = False

            _DC_POOL_FAIL_COUNT[target_dc] = 0
            _DC_POOL_COOLDOWN_UNTIL[target_dc] = 0

            logger.info(f"[DC-POOL] [PIPELINE] DC{target_dc} 连接池已注册（1/{_DC_POOL_SIZE} 连接可用，后台扩容中...）")

            # ========== 阶段3：同步创建剩余连接（TDL: 先建后下） ==========
            _expand_start = time.time()
            _pool_size = _DC_POOL_SIZE
            logger.info(f"[DC-POOL] DC{target_dc} 同步创建剩余 {_pool_size - 1} 个连接（复用阶段1 auth）...")

            _conn_idx = 1
            for i in range(1, _pool_size):
                try:
                    _client = await self._create_single_dc_client(target_dc, exported_auth_1, _conn_idx)
                    await pool.put(_client)
                    _dc_pool_total_inc(target_dc, 1)
                    _total = _dc_pool_total_get(target_dc)
                    logger.info(f"[DC-POOL] DC{target_dc} #{i+1}/{_pool_size} [OK] 就绪（池内={_total}）")
                    _conn_idx += 1
                except Exception as conn_err:
                    logger.warning(f"[DC-POOL] DC{target_dc} #{_conn_idx+1} [FAIL] 失败: {conn_err}, 立即重建")
                    _conn_idx += 1
                    # 立即重建一个新连接补位
                    try:
                        _client = await self._create_single_dc_client(target_dc, exported_auth_1, _conn_idx)
                        await pool.put(_client)
                        _dc_pool_total_inc(target_dc, 1)
                        _total = _dc_pool_total_get(target_dc)
                        logger.info(f"[DC-POOL] DC{target_dc} #{_conn_idx+1} [OK] 重建成功（池内={_total}）")
                        _conn_idx += 1
                    except Exception as rebuild_err:
                        logger.error(f"[DC-POOL] DC{target_dc} #{_conn_idx+1} [FAIL] 重建也失败: {rebuild_err}, 跳过")
                        _conn_idx += 1

            _expand_elapsed = time.time() - _expand_start
            final_total = _dc_pool_total_get(target_dc)
            _DC_POOL_READY[target_dc] = True
            logger.info(f"[DC-POOL] DC{target_dc} 全部就绪: {final_total}/{_pool_size} 耗时={_expand_elapsed:.1f}s")

        except Exception as e:
            logger.error(f"[DC-POOL] DC{target_dc} 连接池创建失败: {e}")
            raise RuntimeError(f"无法创建 DC{target_dc} 连接池: {e}")

        finally:
            try:
                event.set()
                logger.debug(f"[DC-POOL] DC{target_dc} Event 已 set()")
            except Exception as set_err:
                logger.warning(f"[DC-POOL] Event.set() 失败: {set_err}")

            # 清理标记（即使 event.set() 失败）
            try:
                async with dc_lock:
                    if target_dc in _DC_POOL_CREATING:
                        del _DC_POOL_CREATING[target_dc]
                        logger.debug(f"[DC-POOL] DC{target_dc} 已从 _DC_POOL_CREATING 移除")
            except Exception as del_err:
                logger.warning(f"[DC-POOL] 清理 _DC_POOL_CREATING 失败: {del_err}")

    async def _maybe_self_heal_dc_pool(self, target_dc: int, reason: str = "") -> None:
        """
        Background self-heal when pool total drops below threshold.
        """
        try:
            from download_config import get_config_value
            if not bool(get_config_value("dc_pool_self_heal_enabled", False)):
                return
            min_size = int(get_config_value("dc_pool_self_heal_min_size", 0))
            retry_delay = float(get_config_value("dc_pool_self_heal_retry_delay_seconds", 1.0))
        except Exception:
            return

        if min_size <= 0:
            return

        if target_dc in _BACKGROUND_EXPAND_TASKS and not _BACKGROUND_EXPAND_TASKS[target_dc].done():
            return
        if not _DC_POOL_READY.get(target_dc, False):
            return

        target_min = min(int(min_size), int(_DC_POOL_SIZE))
        if _dc_pool_total_get(target_dc) >= target_min:
            return
        if target_dc in _DC_POOL_SELF_HEAL_TASKS and not _DC_POOL_SELF_HEAL_TASKS[target_dc].done():
            return
        if self.main_client_for_export is None:
            return

        async def _heal():
            try:
                pool = _GLOBAL_DC_CLIENT_POOLS.get(target_dc)
                if pool is None:
                    return
                while _dc_pool_total_get(target_dc) < target_min:
                    await asyncio.sleep(max(0.0, retry_delay))
                    try:
                        exported_auth = await _export_auth_with_lock(
                            self.main_client_for_export,
                            target_dc,
                            tag=f"[SELF-HEAL] DC{target_dc} add"
                        )
                        client = await self._create_single_dc_client(target_dc, exported_auth, _dc_pool_total_get(target_dc))
                        await pool.put(client)
                        _dc_pool_total_inc(target_dc, 1)
                        logger.info(
                            f"[DC-POOL] [SELF-HEAL] DC{target_dc} add connection "
                            f"total={_dc_pool_total_get(target_dc)}/{_DC_POOL_SIZE} reason={reason}"
                        )
                    except Exception as heal_err:
                        logger.warning(f"[DC-POOL] [SELF-HEAL] DC{target_dc} create failed: {heal_err}", exc_info=True)
                        continue
            finally:
                try:
                    if target_dc in _DC_POOL_SELF_HEAL_TASKS:
                        del _DC_POOL_SELF_HEAL_TASKS[target_dc]
                except Exception:
                    pass

        try:
            _DC_POOL_SELF_HEAL_TASKS[target_dc] = asyncio.create_task(_heal())
            logger.info(f"[DC-POOL] [SELF-HEAL] DC{target_dc} task started (reason={reason})")
        except Exception:
            pass

    async def _create_single_dc_client(self, target_dc: int, exported_auth, index: int):
        """
        创建单个 DC 专用连接

        Args:
            target_dc: 目标 DC 编号
            exported_auth: 导出的授权信息
            index: 连接编号（用于日志）

        Returns:
            TelegramClient 实例（已授权）

        Raises:
            RuntimeError: 创建失败
        """
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from telethon.tl.functions.auth import ImportAuthorizationRequest
        from telethon.tl.functions.help import GetConfigRequest

        try:
            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: 创建Session...")

            # 1. 创建空 Session 客户端
            temp_client = TelegramClient(
                StringSession(),
                api_id=self.main_client_for_export.api_id,
                api_hash=self.main_client_for_export.api_hash,
                proxy=self.main_client_for_export._proxy,
                flood_sleep_threshold=0,
            )

            # 2. 在connect前设置目标DC，跳过默认DC连接
            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: 设置目标DC={target_dc}...")

            # 使用 Telegram 官方 DC 地址表
            TELEGRAM_DC_ADDRESSES = {
                1: ('149.154.175.53', 443),
                2: ('149.154.167.51', 443),
                3: ('149.154.175.100', 443),
                4: ('149.154.167.91', 443),
                5: ('91.108.56.130', 443),
            }

            if target_dc not in TELEGRAM_DC_ADDRESSES:
                raise RuntimeError(f"[DC-POOL] 未知的DC ID: {target_dc}")

            # 使用官方地址
            dc_address, dc_port = TELEGRAM_DC_ADDRESSES[target_dc]
            temp_client.session.set_dc(target_dc, dc_address, dc_port)
            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: 使用官方地址 {dc_address}:{dc_port}")

            # 3. 连接到目标DC（已设置dc_id，直接连，不走默认DC）
            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: 正在连接目标DC...")
            await temp_client.connect()

            # 4. 验证是否在目标DC（理论上应该直接在目标DC）
            current_dc = temp_client.session.dc_id
            if current_dc != target_dc:
                # 不应发生！如果发生，说明set_dc()没生效
                raise RuntimeError(
                    f"[DC-POOL] DC{target_dc} 连接 #{index+1}: "
                    f"set_dc()失败！当前DC={current_dc}，目标DC={target_dc}"
                )

            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: [OK] 已直接连接到目标DC")

            # 5. 触发 InitConnection（设备登记，Telethon协议要求）
            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: 初始化连接...")
            await temp_client(GetConfigRequest())

            # 6. 导入授权（使用主客户端导出的授权令牌）
            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1}: 导入授权...")
            try:
                await temp_client(ImportAuthorizationRequest(
                    id=exported_auth.id,
                    bytes=exported_auth.bytes
                ))
            except AuthBytesInvalidError as auth_err:
                logger.warning(f"[DC-POOL] DC{target_dc} 连接 #{index+1} AuthBytesInvalid，重试导出授权: {auth_err}")
                if self.main_client_for_export is None:
                    raise
                retry_auth = await _export_auth_with_lock(
                    self.main_client_for_export,
                    target_dc,
                    tag=f"[RETRY-IMPORT] DC{target_dc} #{index+1}"
                )
                await temp_client(ImportAuthorizationRequest(
                    id=retry_auth.id,
                    bytes=retry_auth.bytes
                ))
                logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1} [OK] 重试导入授权成功")

            logger.info(f"[DC-POOL] DC{target_dc} 连接 #{index+1} [OK] 已认证")
            return temp_client

        except Exception as e:
            logger.error(f"[DC-POOL] DC{target_dc} 连接 #{index+1} [FAIL] 创建失败: {e}")
            raise

    async def _clone_dc_client(self, session_str: str, target_dc: int, index: int):
        """
        克隆 DC 连接（复用 auth key）

        Args:
            session_str: 首连接的 StringSession.save() 结果
            target_dc: 目标 DC 编号
            index: 连接编号（用于日志）

        Returns:
            已连接的 TelegramClient 实例（复用首连接的 auth key）
        """
        from telethon import TelegramClient
        from telethon.sessions import StringSession

        tag = f"clone-DC{target_dc}-#{index+1}"

        proxy_to_use = None
        if self.main_client_for_export and hasattr(self.main_client_for_export, '_proxy'):
            proxy_to_use = self.main_client_for_export._proxy

        client = TelegramClient(
            StringSession(session_str),
            api_id=self.main_client_for_export.api_id,
            api_hash=self.main_client_for_export.api_hash,
            proxy=proxy_to_use,
            flood_sleep_threshold=0,
        )
        await client.connect()

        if not client.is_connected():
            raise RuntimeError(f"[DC-POOL] {tag} 连接失败")

        logger.info(f"[DC-POOL] {tag} 克隆连接成功")
        return client

    async def _get_dc_client_from_pool(self, target_dc: int):
        """
        从连接池借用一个连接

        Args:
            target_dc: 目标 DC 编号

        Returns:
            TelegramClient 实例

        Raises:
            RuntimeError: 连接池未初始化或无可用连接
        """
        # 确保连接池已初始化
        await self._ensure_dc_pool_initialized(target_dc)

        try:
            await self._maybe_self_heal_dc_pool(target_dc, reason="borrow")
        except Exception:
            pass

        # 从队列中获取（阻塞等待）
        pool = _GLOBAL_DC_CLIENT_POOLS[target_dc]

        # 最大重试次数（健康检查失败时重试）
        max_health_retries = 3
        borrow_timeout = 30.0  # 等待连接的超时时间（秒）

        for health_attempt in range(max_health_retries):
            try:
                pool_size = pool.qsize()
                if pool_size == 0:
                    logger.info(f"[DC-POOL] DC{target_dc} 等待可用连接（超时={borrow_timeout}s，池当前为空）...")

                client = await asyncio.wait_for(pool.get(), timeout=borrow_timeout)

            except asyncio.TimeoutError:
                # 超时：所有连接可能都卡住了
                logger.error(f"[DC-POOL] DC{target_dc} 等待连接超时（{borrow_timeout}秒），队列大小={pool.qsize()}")
                raise RuntimeError(f"DC{target_dc} 连接池等待超时（{borrow_timeout}秒），可能所有连接均被占用或卡死")

            # 健康检查：验证连接可用
            try:
                if not client.is_connected():
                    logger.warning(f"[DC-POOL] DC{target_dc} 连接已断开，尝试重连...")

                    await client.connect()
                    logger.info(f"[DC-POOL] DC{target_dc} 连接重连成功")

                # 连接健康，返回
                return client

            except Exception as health_err:
                # 连接损坏，销毁而非放回
                logger.error(f"[DC-POOL] DC{target_dc} 连接健康检查失败（尝试 {health_attempt+1}/{max_health_retries}）: {health_err}")

                # 尝试断开损坏的连接
                try:
                    if client.is_connected():
                        await client.disconnect()
                        logger.info(f"[DC-POOL] DC{target_dc} 损坏连接已断开")
                except Exception as disconnect_err:
                    logger.warning(f"[DC-POOL] DC{target_dc} 断开连接失败: {disconnect_err}")

                # 销毁连接，不放回池中
                del client
                _dc_pool_total_dec(target_dc, 1)

                # 继续等待下一个连接
                if health_attempt < max_health_retries - 1:
                    logger.info(f"[DC-POOL] 继续等待 DC{target_dc} 下一个连接...")
                    continue
                else:
                    raise RuntimeError(f"DC{target_dc} 连接池健康检查连续失败 {max_health_retries} 次")

        # 理论上不会到这里
        raise RuntimeError(f"DC{target_dc} 连接池中无可用连接")

    async def _return_dc_client_to_pool(self, target_dc: int, client):
        """
        归还连接到池中

        Args:
            target_dc: 目标 DC 编号
            client: TelegramClient 实例
        """
        if target_dc not in _GLOBAL_DC_CLIENT_POOLS:
            logger.warning(f"[DC-POOL] 警告：DC{target_dc} 连接池不存在，无法归还连接")
            return

        try:
            from download_config import get_config_value
            drop_on_return = bool(get_config_value("dc_pool_drop_disconnected_on_return", False))
        except Exception:
            drop_on_return = False

        if drop_on_return and client is not None:
            should_drop = False
            try:
                if getattr(client, "_dl_connection_failed", False):
                    should_drop = True
                elif hasattr(client, "is_connected") and (not client.is_connected()):
                    should_drop = True
            except Exception:
                pass

            if should_drop:
                try:
                    logger.info(f"[DC-POOL] [DROP] DC{target_dc} drop bad connection on return")
                except Exception:
                    pass
                try:
                    if hasattr(client, "disconnect"):
                        await client.disconnect()
                except Exception:
                    pass
                _dc_pool_total_dec(target_dc, 1)
                await self._maybe_self_heal_dc_pool(target_dc, reason="drop_on_return")
                return

        pool = _GLOBAL_DC_CLIENT_POOLS[target_dc]
        await pool.put(client)

# ========== 进程级全局清理函数 ==========

async def cleanup_global_dc_pools():
    """
    清理所有 DC 连接池

    仅在 Worker 进程关闭时调用。
    断开并销毁所有连接池中的连接。

    调用位置：download_worker.py 的 Worker 关闭方法
    """
    global _GLOBAL_DC_CLIENT_POOLS, _BACKGROUND_EXPAND_TASKS

    if _BACKGROUND_EXPAND_TASKS:
        logger.info(f"[DC-POOL] 取消 {len(_BACKGROUND_EXPAND_TASKS)} 个后台扩容任务...")
        for dc_id, task in list(_BACKGROUND_EXPAND_TASKS.items()):
            if not task.done():
                task.cancel()
                logger.info(f"[DC-POOL] DC{dc_id} 后台扩容任务已取消")
            else:
                logger.info(f"[DC-POOL] DC{dc_id} 后台扩容任务已完成（无需取消）")
        _BACKGROUND_EXPAND_TASKS.clear()
        logger.info(f"[DC-POOL] 后台扩容任务引用已全部清理")

    if not _GLOBAL_DC_CLIENT_POOLS:
        logger.info(f"[DC-POOL] 无需清理（连接池为空）")
        return

    logger.info(f"[DC-POOL] 开始清理 {len(_GLOBAL_DC_CLIENT_POOLS)} 个 DC 连接池...")

    dc_lock = _get_dc_pool_lock()
    async with dc_lock:
        for dc_id, pool in list(_GLOBAL_DC_CLIENT_POOLS.items()):
            logger.info(f"[DC-POOL] 清理 DC{dc_id} 连接池...")

            # 取出所有连接并断开
            disconnected_count = 0
            while not pool.empty():
                try:
                    client = pool.get_nowait()
                    if client.is_connected():
                        await client.disconnect()
                        disconnected_count += 1
                except Exception as e:
                    logger.warning(f"[DC-POOL] DC{dc_id} 连接断开失败: {e}")

            logger.info(f"[DC-POOL] DC{dc_id} 连接池已清理（断开 {disconnected_count} 个连接）")

        # 清空全局字典
        _GLOBAL_DC_CLIENT_POOLS.clear()
        try:
            _DC_POOL_TOTALS.clear()
            _DC_POOL_READY.clear()
            _DC_POOL_SELF_HEAL_TASKS.clear()
            _DC_POOL_FAIL_COUNT.clear()
            _DC_POOL_COOLDOWN_UNTIL.clear()
        except Exception:
            pass
        logger.info(f"[DC-POOL] 所有 DC 连接池已清理完成")
