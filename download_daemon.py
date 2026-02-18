"""
Daemon进程入口 - 独立下载守护进程

功能：
1. 命令行参数解析
2. 日志初始化
3. IPC通道建立
4. Daemon核心启动
5. Watchdog看门狗监控

用法：
  python download_daemon.py \\
    --session <path> \\
    --account-id <id> \\
    --ipc-socket <path> \\
    --log-level INFO \\
    --watchdog-timeout 60
"""

import asyncio
import argparse
import logging
import os
import signal
import sys
import time
from typing import Optional

logger = logging.getLogger(__name__)


# ==================== Terminal-only logging ====================
def _env_truthy(name: str) -> bool:
    val = os.getenv(name, "")
    return val.strip().lower() in ("1", "true", "yes", "on")


def _disable_file_logging_if_requested() -> None:
    """
    Disable file logging by monkey-patching logging.FileHandler when requested.
    This prevents any log file creation and keeps logs in terminal only.
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
        _env_truthy("DAEMON_LOG_DISABLE_FILES")
        or _env_truthy("DOWNLOAD_LOG_DISABLE_FILES")
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
        sys.stdout.write("[Init] File logging disabled by env; terminal-only mode enabled\n")
        sys.stdout.flush()
    except Exception:
        pass


_disable_file_logging_if_requested()


# ==================== Console Encoding Fix ====================
def _ensure_utf8_stdio():
    """Best-effort reconfigure stdout/stderr to UTF-8 to avoid encode errors."""
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception as exc:
        # Avoid raising; keep daemon running even if reconfigure fails.
        try:
            logging.getLogger(__name__).warning(
                "[Logging] Failed to reconfigure stdio encoding: %s", exc
            )
        except Exception:
            pass


# ==================== Watchdog看门狗 ====================
class DaemonWatchdog:
    """
    看门狗：防止Daemon成为僵尸进程

    机制：
    - 监控与主进程的IPC连接
    - 连接断开超过N秒则自杀
    - 自杀前优雅清理资源
    """

    def __init__(
        self,
        ipc_channel: 'IPCChannel',
        daemon_core: Optional['DaemonCore'] = None,
        timeout: int = 60
    ):
        """
        初始化看门狗

        Args:
            ipc_channel: IPC通道对象
            daemon_core: Daemon核心对象（用于优雅关闭）
            timeout: 连接断开超过此秒数则自杀，默认60秒
        """
        self.ipc_channel = ipc_channel
        self.daemon_core = daemon_core
        self.timeout = timeout
        self.last_ping = time.time()
        self.dead = False
        self.check_interval = 5  # 每5秒检查一次

    async def monitor(self):
        """
        持续监控IPC连接状态

        流程：
        1. 每5秒检查一次IPC连接
        2. 如果连接正常，更新时间戳
        3. 如果连接断开超过timeout秒，执行自杀
        """
        logger.info(
            f"[Watchdog] 启动监控 (timeout={self.timeout}s, check_interval={self.check_interval}s)"
        )

        while not self.dead:
            try:
                # 检查IPC连接状态
                is_connected = self.ipc_channel.is_connected()

                if is_connected:
                    # 连接正常，更新时间戳
                    self.last_ping = time.time()
                    logger.debug("[Watchdog] IPC连接正常")

                else:
                    # 连接断开，计算断开时长
                    disconnected_time = time.time() - self.last_ping

                    logger.warning(
                        f"[Watchdog] IPC断连 ({disconnected_time:.1f}s)"
                    )

                    if disconnected_time > self.timeout:
                        logger.critical(
                            f"[Watchdog] IPC断连超过{self.timeout}秒，"
                            f"判断主进程已崩溃，执行自杀"
                        )
                        await self._graceful_suicide()
                        return

                # 每N秒检查一次
                await asyncio.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"[Watchdog] 监控异常: {e}")
                await asyncio.sleep(self.check_interval)

    async def _graceful_suicide(self):
        """
        优雅自杀：清理资源后退出

        步骤：
        1. 保存所有检查点（如果支持）
        2. 断开Daemon的所有连接
        3. 清理临时文件（session副本）
        4. 退出进程
        """
        logger.info("[Watchdog] 开始优雅自杀...")

        try:
            # 步骤1：保存检查点（如果daemon_core支持）
            if self.daemon_core and hasattr(self.daemon_core, 'save_checkpoints'):
                try:
                    logger.info("[Watchdog] 保存所有检查点...")
                    await self.daemon_core.save_checkpoints()
                except Exception as e:
                    logger.error(f"[Watchdog] 保存检查点失败: {e}")

            # 步骤2：关闭Daemon核心
            if self.daemon_core and hasattr(self.daemon_core, 'shutdown'):
                try:
                    logger.info("[Watchdog] 关闭Daemon核心...")
                    await self.daemon_core.shutdown()
                except Exception as e:
                    logger.error(f"[Watchdog] 关闭Daemon失败: {e}")

            # 步骤3：关闭IPC连接
            try:
                logger.info("[Watchdog] 关闭IPC连接...")
                await self.ipc_channel.close()
            except Exception as e:
                logger.error(f"[Watchdog] 关闭IPC失败: {e}")

        except Exception as e:
            logger.error(f"[Watchdog] 自杀前清理异常: {e}")

        finally:
            # 步骤4：退出进程（无论如何）
            logger.critical("[Watchdog] Daemon进程即将退出")
            os.kill(os.getpid(), signal.SIGKILL)

    def stop(self):
        """停止监控"""
        self.dead = True


# ==================== 信号处理 ====================
class SignalHandler:
    """处理系统信号"""

    def __init__(self, daemon_core: Optional['DaemonCore'] = None):
        self.daemon_core = daemon_core
        self.shutdown_requested = False

    def handle_sigterm(self, signum, frame):
        """处理SIGTERM：优雅关闭"""
        logger.info("[Signal] 收到SIGTERM，准备优雅关闭...")
        self.shutdown_requested = True
        if self.daemon_core:
            self.daemon_core.request_shutdown()

    def handle_sigint(self, signum, frame):
        """处理SIGINT(Ctrl+C)：立即关闭"""
        logger.warning("[Signal] 收到SIGINT(Ctrl+C)，立即关闭...")
        self.shutdown_requested = True
        if self.daemon_core:
            self.daemon_core.request_shutdown()


# ==================== 日志配置 ====================
def setup_logging(account_id: str, log_level: str, log_file_override: str = None):
    """
    设置daemon日志

    Args:
        account_id: 账号ID（用于日志文件名）
        log_level: 日志级别（DEBUG/INFO/WARNING/ERROR）
        log_file_override: 统一日志文件路径（与主进程共享），为 None 时使用独立文件
    """
    # 创建logs目录
    os.makedirs("logs", exist_ok=True)

    # 日志文件名
    if log_file_override:
        log_file = log_file_override
    else:
        log_file = f"logs/daemon_{account_id}_{os.getpid()}.log"

    # 日志格式（含 [DAEMON] 标签，便于与主进程日志区分）
    log_format = (
        "%(asctime)s.%(msecs)03d [DAEMON] %(name)s - %(levelname)s - %(message)s"
    )
    date_format = "%Y-%m-%d %H:%M:%S"

    # 配置root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))

    # 文件处理器（追加模式，与主进程共享）
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, log_level))
    file_handler.setFormatter(
        logging.Formatter(log_format, datefmt=date_format)
    )
    root_logger.addHandler(file_handler)

    # 控制台处理器
    # [FIX-2026-02-01] Ensure stdio can encode unicode logs on Windows GBK consoles.
    # This prevents UnicodeEncodeError when log lines contain non-ASCII symbols.
    _ensure_utf8_stdio()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(
        logging.Formatter(log_format, datefmt=date_format)
    )
    root_logger.addHandler(console_handler)

    logger.info(f"[Init] 日志已初始化 -> {log_file}")


# ==================== 命令行参数解析 ====================
def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Telegram下载器Daemon进程",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python download_daemon.py \\
    --session /path/to/session_daemon \\
    --account-id acc_123_456 \\
    --ipc-socket /tmp/tg_daemon.sock \\
    --log-level INFO \\
    --watchdog-timeout 60
        """
    )

    parser.add_argument(
        '--session',
        required=True,
        help='Session文件路径（主进程副本）'
    )

    parser.add_argument(
        '--account-id',
        required=True,
        help='Telegram账号ID'
    )

    parser.add_argument(
        '--ipc-socket',
        required=True,
        help='IPC socket路径（或TCP端口）'
    )

    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='日志级别（默认：INFO）'
    )

    parser.add_argument(
        '--watchdog-timeout',
        type=int,
        default=60,
        help='看门狗超时秒数（默认：60秒）'
    )

    parser.add_argument(
        '--log-file',
        default=None,
        help='统一日志文件路径（与主进程共享）'
    )

    parser.add_argument(
        '--api-id',
        type=int,
        required=True,
        help='Telegram API ID'
    )

    parser.add_argument(
        '--api-hash',
        required=True,
        help='Telegram API Hash'
    )

    return parser.parse_args()


# ==================== 主函数 ====================
async def daemon_main():
    """
    Daemon主函数

    流程：
    1. 解析命令行参数
    2. 初始化日志
    3. 导入依赖模块
    4. 建立IPC连接
    5. 启动Daemon核心
    6. 启动Watchdog监控
    7. 处理信号
    8. 等待关闭
    """
    # 步骤1：解析参数
    args = parse_arguments()

    # 步骤2：初始化日志
    setup_logging(args.account_id, args.log_level, log_file_override=args.log_file)

    logger.info("=" * 60)
    logger.info("Daemon进程启动")
    logger.info("=" * 60)
    logger.info(f"[Init] 账号ID: {args.account_id}")
    logger.info(f"[Init] Session路径: {args.session}")
    logger.info(f"[Init] IPC路径: {args.ipc_socket}")
    logger.info(f"[Init] Watchdog超时: {args.watchdog_timeout}秒")

    try:
        # 步骤3：导入依赖模块
        logger.info("[Init] 导入依赖模块...")
        from download_ipc import IPCChannel
        from download_event_bus import EventBus
        # download_daemon_core会在下个任务创建
        try:
            from download_daemon_core import DaemonCore
        except ImportError:
            logger.error("[Init] download_daemon_core.py尚未实现，使用stub")
            # 使用stub类进行测试
            class DaemonCore:
                def __init__(self, *args, **kwargs):
                    self.shutdown_requested = False

                async def run(self):
                    logger.info("[Stub] DaemonCore运行（stub模式）")
                    while not self.shutdown_requested:
                        await asyncio.sleep(1)

                async def shutdown(self):
                    logger.info("[Stub] DaemonCore关闭")

                async def save_checkpoints(self):
                    logger.info("[Stub] 保存检查点")

                def request_shutdown(self):
                    self.shutdown_requested = True

        # 步骤4：建立IPC连接
        logger.info("[Init] 建立IPC连接...")
        # 判断是否为socket路径或TCP端口
        if args.ipc_socket.isdigit():
            # TCP端口
            ipc = IPCChannel(tcp_port=int(args.ipc_socket))
        else:
            # Unix socket路径
            ipc = IPCChannel(socket_path=args.ipc_socket)

        # 连接为服务端（等待主进程客户端连接）
        await ipc.connect(is_server=True)
        logger.info("[Init] IPC服务端已启动，等待主进程连接...")

        # 步骤5：初始化EventBus
        event_bus = EventBus()
        logger.info("[Init] 事件总线已初始化")

        # 步骤6：初始化Daemon核心
        logger.info("[Init] 初始化Daemon核心...")
        daemon_core = DaemonCore(
            session_path=args.session,
            account_id=args.account_id,
            ipc_channel=ipc,
            event_bus=event_bus,
            api_id=args.api_id,
            api_hash=args.api_hash
        )

        # 步骤7：初始化看门狗
        watchdog = DaemonWatchdog(
            ipc_channel=ipc,
            daemon_core=daemon_core,
            timeout=args.watchdog_timeout
        )

        # 步骤8：设置信号处理器
        signal_handler = SignalHandler(daemon_core)
        signal.signal(signal.SIGTERM, signal_handler.handle_sigterm)
        signal.signal(signal.SIGINT, signal_handler.handle_sigint)
        logger.info("[Init] 信号处理器已注册")

        logger.info("=" * 60)
        logger.info("Daemon初始化完成，启动主循环")
        logger.info("=" * 60)

        # 步骤9：并发运行daemon核心和watchdog监控
        try:
            await asyncio.gather(
                daemon_core.run(),
                watchdog.monitor(),
                return_exceptions=True
            )
        except Exception as e:
            logger.error(f"[Error] 主循环异常: {e}", exc_info=True)

        # 步骤10：优雅关闭
        logger.info("[Shutdown] 关闭Daemon...")
        await daemon_core.shutdown()
        await ipc.close()

        logger.info("=" * 60)
        logger.info("Daemon进程已关闭")
        logger.info("=" * 60)

    except Exception as e:
        logger.critical(f"[Fatal] Daemon崩溃: {e}", exc_info=True)
        raise

    finally:
        logger.info("[Cleanup] 清理完毕")


# ==================== 崩溃保护 ====================
def _install_crash_logger():
    """
    安装全局异常钩子，确保 Daemon 进程崩溃时将完整 traceback
    写入日志文件，而非默默消失（导致只能看到 IPC 断连的 WinError 64）。
    """
    _original_excepthook = sys.excepthook

    def _crash_hook(exc_type, exc_value, exc_tb):
        if exc_type is KeyboardInterrupt:
            _original_excepthook(exc_type, exc_value, exc_tb)
            return
        try:
            import traceback as _tb
            crash_msg = "".join(_tb.format_exception(exc_type, exc_value, exc_tb))
            logger.critical(f"[CRASH] Daemon进程未捕获异常:\n{crash_msg}")
            # 双保险：直接写 stderr（即使 logger 已失效）
            sys.stderr.write(f"[DAEMON-CRASH] {crash_msg}\n")
            sys.stderr.flush()
        except Exception:
            pass
        _original_excepthook(exc_type, exc_value, exc_tb)

    sys.excepthook = _crash_hook


# ==================== 进程入口 ====================
def main():
    """进程入口点"""
    _install_crash_logger()
    try:
        # 运行async main函数
        asyncio.run(daemon_main())
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("[Shutdown] 用户中断")
        sys.exit(0)

    except BaseException as e:
        logger.critical(f"[Fatal] 未处理的异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
