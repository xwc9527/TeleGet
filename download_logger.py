"""
下载器详细日志系统 - 用于测试和诊断

日志级别：
- DEBUG: 详细的执行流程
- INFO: 关键操作和状态变化
- WARNING: 异常情况但不影响功能
- ERROR: 错误情况

日志类别：
- DAEMON: Daemon进程生命周期
- POOL: 连接池和Worker池操作
- DOWNLOAD: 下载任务执行
- IPC: 进程间通信
- PROGRESS: 下载进度
- PERFORMANCE: 性能指标
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

# ==================== Terminal-only logging ====================
def _env_truthy(name: str) -> bool:
    val = os.getenv(name, "")
    return val.strip().lower() in ("1", "true", "yes", "on")


def _disable_file_logging_if_requested() -> None:
    """
    Disable file logging by monkey-patching logging.FileHandler when requested.
    This prevents log file creation and keeps logs in terminal only.
    """
    # Preserve original FileHandler for recovery.
    if not hasattr(logging, "_ORIGINAL_FILE_HANDLER"):
        logging._ORIGINAL_FILE_HANDLER = logging.FileHandler

    # If explicitly requested to keep both, restore original and skip disable.
    if _env_truthy("DOWNLOAD_LOG_BOTH") or _env_truthy("LOG_BOTH"):
        try:
            logging.FileHandler = logging._ORIGINAL_FILE_HANDLER
        except Exception:
            pass
        return

    if not (
        _env_truthy("DOWNLOAD_LOG_DISABLE_FILES")
        or _env_truthy("DOWNLOAD_LOG_TERMINAL_ONLY")
    ):
        return

    class _NoFileHandler(logging.Handler):
        def __init__(self, *args, **kwargs):
            super().__init__()

        def emit(self, record):
            # Terminal-only mode: drop file output.
            return

    logging.FileHandler = _NoFileHandler  # type: ignore[assignment]
    try:
        sys.stdout.write("[DownloadLogger] File logging disabled by env; terminal-only mode enabled\n")
        sys.stdout.flush()
    except Exception:
        pass


_disable_file_logging_if_requested()

# 日志目录
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs', 'download_tests')
os.makedirs(LOG_DIR, exist_ok=True)


class DownloadLogger:
    """下载器详细日志系统"""

    def __init__(self, test_name: str = "default"):
        """
        初始化日志系统

        Args:
            test_name: 测试名称（用于区分不同测试会话）
        """
        self.test_name = test_name
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 日志文件路径
        self.main_log = os.path.join(LOG_DIR, f"{test_name}_{self.session_id}_main.log")
        self.daemon_log = os.path.join(LOG_DIR, f"{test_name}_{self.session_id}_daemon.log")
        self.performance_log = os.path.join(LOG_DIR, f"{test_name}_{self.session_id}_perf.log")

        # 初始化日��记录器
        self._setup_loggers()

    def _setup_loggers(self):
        """配置日志记录器"""
        # 主日志记录器（所有日志）
        self.main_logger = self._create_logger(
            'download_main',
            self.main_log,
            level=logging.DEBUG
        )

        # Daemon专用日志
        self.daemon_logger = self._create_logger(
            'download_daemon',
            self.daemon_log,
            level=logging.DEBUG
        )

        # 性能日志（简化格式）
        self.perf_logger = self._create_logger(
            'download_perf',
            self.performance_log,
            level=logging.INFO,
            format_str='%(asctime)s | %(message)s'
        )

    def _create_logger(
        self,
        name: str,
        file_path: str,
        level=logging.INFO,
        format_str: Optional[str] = None
    ) -> logging.Logger:
        """
        创建日志记录器

        Args:
            name: 日志器名称
            file_path: 日志文件路径
            level: 日志级别
            format_str: 自定义格式
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.handlers.clear()  # 清除已有的handlers

        # 文件handler
        file_handler = logging.FileHandler(
            file_path,
            encoding='utf-8',
            mode='w'  # 覆盖模式
        )
        file_handler.setLevel(level)

        # 控制台handler（ERROR及以上）
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.WARNING)
        # [TERMINAL-LOG-2026-02-01] Allow console level override via env (default INFO).
        console_level_name = os.getenv("DOWNLOAD_LOG_CONSOLE_LEVEL", "INFO").upper()
        console_level = logging._nameToLevel.get(console_level_name, logging.INFO)
        console_handler.setLevel(console_level)

        # 格式化
        if format_str is None:
            format_str = (
                '%(asctime)s.%(msecs)03d | '
                '%(levelname)-8s | '
                '%(name)-20s | '
                '%(funcName)-25s | '
                '%(message)s'
            )

        formatter = logging.Formatter(
            format_str,
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # [TERMINAL-LOG-2026-02-01] Optional: disable file logs, keep console only.
        if os.getenv("DOWNLOAD_LOG_DISABLE_FILES", "0") == "1":
            logger.removeHandler(file_handler)
            try:
                file_handler.close()
            except Exception:
                pass

        return logger

    def log_test_start(self, test_type: str, description: str):
        """记录测试开始"""
        self.main_logger.info("=" * 80)
        self.main_logger.info(f"测试开始: {test_type}")
        self.main_logger.info(f"描述: {description}")
        self.main_logger.info(f"会话ID: {self.session_id}")
        self.main_logger.info("=" * 80)

    def log_test_end(self, success: bool, summary: str):
        """记录测试结束"""
        self.main_logger.info("=" * 80)
        self.main_logger.info(f"测试结束: {'成功' if success else '失败'}")
        self.main_logger.info(f"总结: {summary}")
        self.main_logger.info("=" * 80)

    def log_daemon_lifecycle(self, event: str, account_id: str, details: str = ""):
        """记录Daemon生命周期事件"""
        self.daemon_logger.info(
            f"[DAEMON-LIFECYCLE] {event} | account={account_id} | {details}"
        )

    def log_pool_operation(self, pool_type: str, operation: str, details: str):
        """记录连接池/Worker池操作"""
        self.main_logger.debug(
            f"[POOL-{pool_type}] {operation} | {details}"
        )

    def log_download_start(self, request_id: str, file_name: str, file_size: int):
        """记录下载开始"""
        self.main_logger.info(
            f"[DOWNLOAD-START] {request_id} | file={file_name} | "
            f"size={file_size:,} bytes ({file_size / (1024**3):.2f} GB)"
        )
        self.perf_logger.info(
            f"START | {request_id} | {file_name} | {file_size}"
        )

    def log_download_progress(
        self,
        request_id: str,
        downloaded: int,
        total: int,
        speed_mbps: float = 0
    ):
        """记录下载进度"""
        percentage = (downloaded / total * 100) if total > 0 else 0
        self.main_logger.debug(
            f"[DOWNLOAD-PROGRESS] {request_id} | "
            f"{downloaded:,}/{total:,} ({percentage:.1f}%) | "
            f"速度={speed_mbps:.2f} MB/s"
        )

    def log_download_complete(
        self,
        request_id: str,
        success: bool,
        elapsed_seconds: float,
        file_size: int
    ):
        """记录下载完成"""
        speed_mbps = (file_size / (1024**2)) / elapsed_seconds if elapsed_seconds > 0 else 0

        self.main_logger.info(
            f"[DOWNLOAD-COMPLETE] {request_id} | "
            f"success={success} | "
            f"耗时={elapsed_seconds:.2f}s | "
            f"速度={speed_mbps:.2f} MB/s"
        )

        self.perf_logger.info(
            f"COMPLETE | {request_id} | {success} | "
            f"{elapsed_seconds:.2f}s | {speed_mbps:.2f} MB/s"
        )

    def log_ipc_event(self, event_type: str, direction: str, details: str):
        """记录IPC事件"""
        self.main_logger.debug(
            f"[IPC-{direction}] {event_type} | {details}"
        )

    def log_worker_activity(
        self,
        worker_id: int,
        action: str,
        part_index: int = -1,
        details: str = ""
    ):
        """记录Worker活动"""
        part_info = f"part={part_index}" if part_index >= 0 else ""
        self.main_logger.debug(
            f"[WORKER-{worker_id}] {action} | {part_info} {details}"
        )

    def log_performance_metric(self, metric_name: str, value: float, unit: str = ""):
        """记录性能指标"""
        self.perf_logger.info(
            f"METRIC | {metric_name} | {value:.4f} {unit}"
        )

    def log_error(self, component: str, error_msg: str, exception: Exception = None):
        """记录错误"""
        self.main_logger.error(
            f"[ERROR-{component}] {error_msg}",
            exc_info=exception
        )

    def get_log_files(self):
        """获取所有日志文件路径"""
        return {
            'main': self.main_log,
            'daemon': self.daemon_log,
            'performance': self.performance_log
        }


# 全局日志实例
_global_logger: Optional[DownloadLogger] = None


def init_download_logger(test_name: str = "default") -> DownloadLogger:
    """
    初始化全局日志系统

    Args:
        test_name: 测试名称

    Returns:
        DownloadLogger实例
    """
    global _global_logger
    _global_logger = DownloadLogger(test_name)
    return _global_logger


def ensure_terminal_logging(test_name: str = "terminal") -> Optional[DownloadLogger]:
    """
    Ensure download logger is initialized for terminal output.

    - Forces console logging to INFO by default
    - Disables file logs unless explicitly enabled
    """
    os.environ.setdefault("DOWNLOAD_LOG_CONSOLE_LEVEL", "INFO")
    os.environ.setdefault("DOWNLOAD_LOG_DISABLE_FILES", "1")
    if _global_logger is None:
        return init_download_logger(test_name)
    return _global_logger


def get_download_logger() -> Optional[DownloadLogger]:
    """获取全局日志实例"""
    return _global_logger


# 便捷函数
def log_daemon_start(account_id: str, details: str = ""):
    """记录Daemon启动"""
    if _global_logger:
        _global_logger.log_daemon_lifecycle("START", account_id, details)


def log_daemon_shutdown(account_id: str, details: str = ""):
    """记录Daemon关闭"""
    if _global_logger:
        _global_logger.log_daemon_lifecycle("SHUTDOWN", account_id, details)


def log_download_task_start(request_id: str, file_name: str, file_size: int):
    """记录下载任务开始"""
    if _global_logger:
        _global_logger.log_download_start(request_id, file_name, file_size)


def log_download_task_complete(
    request_id: str,
    success: bool,
    elapsed: float,
    file_size: int
):
    """记录下载任务完成"""
    if _global_logger:
        _global_logger.log_download_complete(request_id, success, elapsed, file_size)


def log_pool_info(pool_type: str, operation: str, details: str):
    """记录池操作信息"""
    if _global_logger:
        _global_logger.log_pool_operation(pool_type, operation, details)


if __name__ == '__main__':
    # 测试日志系统
    logger = init_download_logger("test_logger")

    logger.log_test_start(
        "大文件下载测试",
        "测试单个5GB文件的下载性能"
    )

    logger.log_daemon_lifecycle("START", "acc_123", "PID=1234")
    logger.log_download_start("req_001", "large_file.zip", 5 * 1024**3)
    logger.log_download_progress("req_001", 1024**3, 5 * 1024**3, 25.5)
    logger.log_download_complete("req_001", True, 180.5, 5 * 1024**3)
    logger.log_daemon_lifecycle("SHUTDOWN", "acc_123", "graceful")

    logger.log_test_end(True, "所有下载成功完成")

    print("\n日志文件位置:")
    for name, path in logger.get_log_files().items():
        print(f"  {name}: {path}")
