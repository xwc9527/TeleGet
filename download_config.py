"""
下载子进程配置管理

提供配置读写和模式切换功能。
支持运行时切换新旧模式，无需重启应用。
"""

import os
import json
import logging
from typing import Literal

logger = logging.getLogger(__name__)

# 配置文件路径（可通过 init_config_dir() 覆盖）
_CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
CONFIG_FILE = os.path.join(_CONFIG_DIR, 'download_config.json')


def init_config_dir(base_dir: str):
    """设置配置文件根目录（库初始化时调用）"""
    global _CONFIG_DIR, CONFIG_FILE
    _CONFIG_DIR = base_dir
    CONFIG_FILE = os.path.join(_CONFIG_DIR, 'download_config.json')
    os.makedirs(_CONFIG_DIR, exist_ok=True)

# Default configuration
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │                        ⚠️  SAFETY WARNING  ⚠️                              │
# │                                                                             │
# │  The rate-limiting and connection parameters below have been carefully       │
# │  tuned through extensive testing against Telegram's undocumented server-    │
# │  side limits.  Lowering intervals or raising connection counts beyond the   │
# │  defaults WILL result in:                                                   │
# │                                                                             │
# │    1. FloodWait penalties  – server-imposed cooldowns (30s – 24h)           │
# │    2. NPW (No Permit Wait) – bandwidth throttling (7-11s stalls per hit)   │
# │    3. Temporary or permanent account restrictions                           │
# │    4. Session revocation requiring re-login                                 │
# │                                                                             │
# │  Telegram's limits are per-account, not per-connection.  More connections   │
# │  or faster request rates do NOT increase throughput — they only increase    │
# │  the probability of server-side punishment.                                 │
# │                                                                             │
# │  Key parameters and their safe ranges:                                      │
# │                                                                             │
# │    rate_limiter_interval    >= 0.2s   (default 0.2s)                        │
# │    connection_pool_size     <= 8      (default 4)                           │
# │    download_threads         <= 8      (default 4)                           │
# │    dc_rate_limiter_interval >= 0.2s   (default 0.2s)                        │
# │                                                                             │
# │  These defaults yield 4-7 MB/s with zero FloodWait on free accounts.       │
# │  Modify at your own risk.                                                   │
# └─────────────────────────────────────────────────────────────────────────────┘
DEFAULT_CONFIG = {
    # ========== 下载模式 ==========
    "download_mode": "concurrent",

    # ========== Worker 超时配置（秒） ==========
    "worker_init_timeout": 60,
    "worker_download_timeout": 300,
    "worker_idle_timeout": 600,

    # ========== 动态超时配置 ==========
    "download_timeout_mode": "adaptive",
    "download_timeout_fixed": 300,
    "download_timeout_min_speed_mbps": 1.0,
    "download_timeout_min": 60,
    "download_timeout_max": 7200,

    # ========== 自动重启配置 ==========
    "worker_auto_restart": True,
    "max_restart_attempts": 3,

    # ========== 队列配置 ==========
    "request_queue_size": 300,
    "response_queue_size": 500,

    # ========== 进度回调配置 ==========
    "progress_throttle_interval": 0.5,

    # ========== 代理配置 ==========
    "proxy_mode": "auto",
    "custom_proxy": {
        "enabled": False,
        "type": "http",
        "host": "127.0.0.1",
        "port": 7897
    },
    "proxy_fallback_to_direct": True,
    "proxy_test_timeout": 3.0,

    # ========== 调试配置 ==========
    "debug_mode": False,

    # ========== 并发下载配置 ==========
    "connection_pool_size": 4,
    "connection_pool_size_turbo": 4,

    # ========== 轮询替补配置 ==========
    "rotation_enabled": False,
    "rotation_active_slots": 4,
    "rotation_total_workers": 4,

    # ========== 连接池懒加载配置 ==========
    "connection_pool_lazy_enabled": True,
    "connection_pool_prewarm_size": 4,
    "connection_pool_max_size": 4,
    "connection_pool_lazy_replace_dead": True,
    "connection_pool_lazy_log": True,

    # ========== DC 连接池配置 ==========
    "dc_pool_size": 4,
    "max_concurrent_tasks": 1,
    "max_connections_per_task": 4,
    "download_threads": 4,
    "shard_retry_count": 3,
    "connection_health_check": True,

    # ========== DC 连接池分批并行创建 ==========
    "dc_pool_batch_parallel_enabled": True,
    "dc_pool_batch_size": 4,
    "dc_pool_batch_gap_seconds": 3.0,

    # ========== DC 连接池自愈 ==========
    "dc_pool_min_ready": 4,
    "dc_pool_min_ready_wait_seconds": 5.0,
    "dc_pool_self_heal_enabled": True,
    "dc_pool_self_heal_min_size": 4,
    "dc_pool_self_heal_retry_delay_seconds": 1.0,
    "dc_pool_drop_disconnected_on_return": True,

    # ========== 持久化连接池配置 ==========
    "persistent_pool_enabled": True,
    "persistent_pool_idle_timeout": 300,
    "persistent_pool_health_check": True,
    "persistent_pool_auth_check": False,

    # ========== Worker Pool 并发下载配置 ==========
    "worker_pool_size": 10,
    "worker_pool_size_turbo": 12,
    "worker_pool_part_size": 1024 * 1024,
    "worker_pool_max_retries": 2,
    "worker_pool_per_request_timeout": 15,
    "worker_pool_watchdog_stall_seconds": 30,
    "worker_pool_watchdog_check_interval": 5,

    # ========== TDL 策略模式 ==========
    "tdl_strategy_enabled": False,

    # ========== 带宽限速器 ==========
    "bandwidth_limit_enabled": False,
    "bandwidth_limit_mbps": 6.15,
    "bandwidth_limit_burst_mb": 2.0,

    # ========== 会话级断连防御 ==========
    "session_disconnect_breaker_enabled": True,
    "session_disconnect_breaker_window_seconds": 8.0,
    "session_disconnect_breaker_threshold": 2,
    "session_disconnect_breaker_pause_seconds": 3.0,
    "session_disconnect_breaker_cooldown_seconds": 10.0,
    "rate_limiter_backoff_on_disconnect": True,
    "rate_limiter_disconnect_increase_step": 0.05,
    "reconnect_jitter_enabled": True,
    "reconnect_jitter_max_ms": 500,

    # ========== NPW 处理策略 ==========
    "npw_skip_circuit_breaker": False,
    "npw_sleep_multiplier": 1.0,
    "npw_jitter_max_ms": 0,

    # ========== 跨DC冷启动等待 ==========
    "worker_pool_cold_start_wait_enabled": True,
    "worker_pool_cold_start_max_wait_seconds": 15.0,

    # ========== Part 分配策略 ==========
    "worker_pool_sticky_assignment": False,
    "worker_pool_sticky_strategy": "contiguous",
    "worker_pool_sticky_log": True,

    # ========== Worker 启动错峰 ==========
    "worker_pool_phase_spread_enabled": True,
    "worker_pool_phase_spread_max_ms": 80,

    # ========== FloodWait 软重试 ==========
    "floodwait_soft_retry_enabled": True,
    "floodwait_soft_retry_margin": 0.20,

    # ========== 统计输出 ==========
    "worker_pool_stats_detailed": True,

    # ========== fsync 节流配置 ==========
    "fsync_mode": "periodic",
    "fsync_interval_mb": 50,
    "fsync_interval_seconds": 30,

    # ========== 请求速率限制配置 ==========
    "rate_limiter_enabled": True,
    "rate_limiter_batch_size": 1,
    "rate_limiter_interval": 0.2,

    # ========== 自适应速率限制配置 ==========
    "rate_limiter_adaptive": False,
    "rate_limiter_min_interval": 0.1,
    "rate_limiter_max_interval": 1.0,
    "rate_limiter_success_threshold": 50,
    "rate_limiter_decrease_ratio": 0.9,
    "rate_limiter_increase_step": 0.1,

    # ========== 单向退避模式 ==========
    "rate_limiter_only_backoff": False,
    "rate_limiter_only_backoff_base_interval": 0.125,
    "rate_limiter_only_backoff_cooldown_seconds": 1.0,
    "rate_limiter_only_backoff_force_min_interval": True,
    "rate_limiter_only_backoff_log_interval_seconds": 5.0,

    # ========== 平滑调度 ==========
    "rate_limiter_smooth": False,
    "rate_limiter_smooth_gap_override": 0.0,

    # ========== 跨DC退避隔离 ==========
    "cross_dc_backoff_enabled": True,
    "cross_dc_disable_global_rate_limiter": True,
    "cross_dc_request_jitter_ms": 10.0,

    # ========== 连接级熔断器 ==========
    "circuit_breaker_scope": "connection",
    "connection_circuit_breaker_enabled": True,
    "connection_circuit_breaker_repeat_window_seconds": 1.0,
    "connection_circuit_breaker_threshold": 2,
    "connection_circuit_breaker_hard_seconds": 2.0,

    # ========== Daemon 独立进程配置 ==========
    "use_daemon_downloader": True,
    "daemon_socket_type": "unix",
    "daemon_socket_dir": "",
    "daemon_tcp_port": 9999,
    "daemon_ipc_timeout": 5.0,
    "daemon_heartbeat_interval": 10,
    "daemon_max_idle_time": 300,
    "daemon_log_level": "INFO",
    "daemon_memory_limit_mb": 4000,
    "daemon_auto_restart": True,
    "daemon_checkpoint_dir": "data/checkpoints",
    "daemon_checkpoint_interval": 30,
    "account_scheduler_max_queue_size": 1000,
    "daemon_ipc_connect_max_retries": 10,
    "daemon_ipc_connect_retry_delay": 0.5,

    # ========== 小文件批量下载配置 ==========
    "small_file_threshold_bytes": 5242880,
    "small_file_pool_size": 5,

    # ========== 跨DC专用限速配置 ==========
    "dc_rate_limiter_enabled": True,
    "dc_rate_limiter_batch_size": 4,
    "dc_rate_limiter_interval": 0.20,
    "dc_rate_limiter_min_interval": 0.20,
    "dc_rate_limiter_max_interval": 1.00,
    "dc_rate_limiter_increase_step": 0.10,
    "dc_rate_limiter_cooldown_seconds": 1.0,
    "dc_rate_limiter_only_backoff": True,
    "dc_rate_limiter_skip_global": True,
    "dc_rate_limiter_log_interval_seconds": 5.0,

    # ========== 跨DC错峰启动 ==========
    "dc_rate_limiter_phase_spread_enabled": True,
    "dc_rate_limiter_phase_spread_max_ms": 50,
}


def _ensure_config_dir():
    """确保配置目录存在"""
    if not os.path.exists(_CONFIG_DIR):
        os.makedirs(_CONFIG_DIR, exist_ok=True)


def load_config() -> dict:
    """
    加载配置

    Returns:
        完整的配置字典（合并默认值和用户配置）
    """
    if not os.path.exists(CONFIG_FILE):
        return DEFAULT_CONFIG.copy()

    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            user_config = json.load(f)
            config = DEFAULT_CONFIG.copy()
            config.update(user_config)
            return config
    except Exception as e:
        logger.error("[DOWNLOAD-CONFIG] 加载配置失败: %s", e)
        return DEFAULT_CONFIG.copy()


def save_config(config: dict):
    """
    保存配置

    Args:
        config: 配置字典
    """
    _ensure_config_dir()
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        logger.info("[DOWNLOAD-CONFIG] 配置已保存")
    except Exception as e:
        logger.error("[DOWNLOAD-CONFIG] 保存配置失败: %s", e)


def get_download_mode() -> Literal["concurrent", "subprocess", "legacy"]:
    """获取当前下载模式"""
    config = load_config()
    mode = config.get("download_mode", "subprocess")
    if mode not in ("concurrent", "subprocess", "legacy"):
        return "subprocess"
    return mode


def set_download_mode(mode: Literal["concurrent", "subprocess", "legacy"]):
    """设置下载模式"""
    if mode not in ("concurrent", "subprocess", "legacy"):
        raise ValueError(f"无效的下载模式: {mode}")

    config = load_config()
    old_mode = config.get("download_mode", "legacy")
    config["download_mode"] = mode
    save_config(config)
    logger.info("[DOWNLOAD-CONFIG] 下载模式已切换: %s -> %s", old_mode, mode)


def get_config_value(key: str, default=None):
    """获取指定配置项"""
    config = load_config()
    return config.get(key, default)


def set_config_value(key: str, value):
    """设置指定配置项"""
    config = load_config()
    config[key] = value
    save_config(config)


def is_subprocess_mode() -> bool:
    """快速检查是否为子进程模式（包含 concurrent 模式）"""
    return get_download_mode() in ("subprocess", "concurrent")


def is_concurrent_mode() -> bool:
    """快速检查是否为并发模式"""
    return get_download_mode() == "concurrent"


def get_connections_for_file_size(file_size: int) -> int:
    """根据文件大小计算需要的连接数"""
    size_mb = file_size / (1024 * 1024)

    if size_mb < 10:
        num_connections = 4
    elif size_mb < 100:
        num_connections = 6
    else:
        max_connections = get_config_value("connection_pool_size", 10)
        num_connections = max_connections

    logger.debug("[DOWNLOAD-CONFIG] 文件大小: %.2f MB -> 分配连接数: %d", size_mb, num_connections)
    return num_connections


def get_optimal_worker_count(num_connections: int) -> int:
    """根据连接数返回最优 Worker 数量"""
    return num_connections


def calculate_download_timeout(file_size: int) -> int:
    """根据文件大小动态计算下载超时"""
    config = load_config()
    timeout_mode = config.get("download_timeout_mode", "adaptive")

    if timeout_mode == "fixed":
        timeout = config.get("download_timeout_fixed", 300)
        logger.debug("[DOWNLOAD-CONFIG] 使用固定超时: %d 秒", timeout)
        return timeout

    min_speed_mbps = config.get("download_timeout_min_speed_mbps", 1.0)
    min_timeout = config.get("download_timeout_min", 60)
    max_timeout = config.get("download_timeout_max", 7200)

    min_speed_bps = min_speed_mbps * 1024 * 1024 / 8
    base_timeout = (file_size / min_speed_bps) * 1.3
    timeout = max(min_timeout, min(int(base_timeout), max_timeout))

    size_mb = file_size / (1024 * 1024)
    logger.debug("[DOWNLOAD-CONFIG] 动态超时计算: 文件 %.2f MB, 最小速度 %s Mbps -> 超时 %d 秒", size_mb, min_speed_mbps, timeout)

    return timeout
