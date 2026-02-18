"""
下载管理器v2 - 支持Daemon和事件驱动

组件：
1. AccountScheduler - 账号调度器（单账号活跃规则）
2. DownloadEventListener - 事件监听器（异步事件驱动）
3. TaskQueueManager - 任务队列管理器

设计原则：
- 严格单账号活跃：只有当前选中账号的任务立即执行
- 非活跃账号进队列：切换后逐个恢复
- 事件驱动而非回调：完全解耦daemon和UI
- 异步优先：所有操作都是非阻塞的
"""

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List, Callable, Any, Tuple

logger = logging.getLogger(__name__)


# ==================== Session copy helpers (Phase 1.5) ====================
# NOTE: Only add logic; do not delete existing code. Legacy single-file copy is
# explicitly bypassed in _start_daemon with an explanation.
def _safe_copy_file_with_retries(
    src: str,
    dst: str,
    max_retries: int = 5,
    base_delay: float = 0.2,
) -> None:
    """Copy file with retries to tolerate transient file locks (Windows)."""
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            shutil.copy2(src, dst)
            logger.info(
                f"[Daemon-SessionCopy] Copied: {src} -> {dst} (attempt {attempt})"
            )
            return
        except FileNotFoundError as e:
            # Source missing is fatal for .session, non-fatal for wal/shm (handled by caller)
            last_err = e
            break
        except PermissionError as e:
            last_err = e
        except OSError as e:
            # Windows file-lock: winerror 32/33, treat as retryable
            if getattr(e, "winerror", None) in (32, 33):
                last_err = e
            else:
                raise

        delay = base_delay * attempt
        logger.warning(
            f"[Daemon-SessionCopy] File locked, retrying in {delay:.2f}s: {src}"
        )
        time.sleep(delay)

    if last_err:
        raise last_err


def _copy_session_triple_with_retries(
    session_path_base: str,
    daemon_path_base: str,
    max_retries: int = 5,
    base_delay: float = 0.2,
) -> None:
    """Copy .session/.session-wal/.session-shm with lock-tolerant retries."""
    session_file = f"{session_path_base}.session"
    daemon_session_file = f"{daemon_path_base}.session"

    # Always require main .session to exist
    _safe_copy_file_with_retries(
        session_file,
        daemon_session_file,
        max_retries=max_retries,
        base_delay=base_delay,
    )

    # WAL/SHM are optional but should be copied if present
    for suffix in (".session-wal", ".session-shm"):
        src = f"{session_path_base}{suffix}"
        dst = f"{daemon_path_base}{suffix}"
        if os.path.exists(src):
            try:
                _safe_copy_file_with_retries(
                    src,
                    dst,
                    max_retries=max_retries,
                    base_delay=base_delay,
                )
            except Exception as e:
                # Non-fatal: continue but log for diagnostics
                logger.warning(
                    f"[Daemon-SessionCopy] Optional copy failed: {src} -> {dst}, err={e}"
                )
        else:
            logger.info(f"[Daemon-SessionCopy] Optional file not found, skip: {src}")


def _start_daemon_pipe_logger(process: subprocess.Popen, tag: str):
    """Start background threads to drain daemon stdout/stderr and log lines.

    [FIX-2026-02-15] 日志去重：Daemon 的 logger 已直接写统一日志文件，
    pipe 转发会造成每条日志出现 2~3 次。过滤规则：
    - 含 '[DAEMON]' 前缀的行 = logger 输出 → 已直接写文件，跳过
    - 不含 '[DAEMON]' 的行 = print() 输出 → 仅通过 pipe 可见，保留
    """
    def _reader(pipe, level: str):
        try:
            for line in iter(pipe.readline, ''):
                line = line.rstrip()
                if not line:
                    continue
                if level == "stderr":
                    logger.warning(f"[Daemon-STDERR] {tag} {line}")
                else:
                    # 跳过 daemon logger 已直接写入日志文件的行（去重）
                    if '[DAEMON]' in line:
                        continue
                    logger.info(f"[Daemon-STDOUT] {tag} {line}")
        except Exception as e:
            logger.warning(f"[Daemon-Pipe] {tag} read failed: {e}")
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    if process.stdout:
        t_out = threading.Thread(target=_reader, args=(process.stdout, "stdout"), daemon=True)
        t_out.start()
    if process.stderr:
        t_err = threading.Thread(target=_reader, args=(process.stderr, "stderr"), daemon=True)
        t_err.start()


# ==================== 数据模型 ====================
class DownloadTaskState(Enum):
    """下载任务状态"""
    PENDING = "pending"        # 待处理
    QUEUED = "queued"          # 在队列中
    DOWNLOADING = "downloading"  # 下载中
    PAUSED = "paused"          # 暂停
    COMPLETED = "completed"    # 完成
    FAILED = "failed"          # 失败
    CANCELLED = "cancelled"    # 取消


@dataclass
class DownloadTask:
    """下载任务"""
    request_id: str
    account_id: str
    chat_id: int
    msg_id: int
    save_path: str
    media_type: Optional[str] = None
    state: DownloadTaskState = DownloadTaskState.PENDING

    # 回调函数
    progress_callback: Optional[Callable] = None
    complete_callback: Optional[Callable] = None
    error_callback: Optional[Callable] = None

    # 进度信息
    file_name: str = ""
    file_size: int = 0
    downloaded: int = 0
    percentage: float = 0.0
    error_message: Optional[str] = None

    # 时间戳
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


# ==================== 任务队列管理器 ====================
class TaskQueueManager:
    """
    任务队列管理器

    为每个账号维护一个独立的任务队列
    """

    def __init__(self, max_queue_size: int = 1000):
        """
        初始化队列管理器

        Args:
            max_queue_size: 队列最大大小
        """
        self.max_queue_size = max_queue_size
        self.queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def get_queue(self, account_id: str) -> asyncio.Queue:
        """获取或创建账号的任务队列"""
        async with self._lock:
            if account_id not in self.queues:
                self.queues[account_id] = asyncio.Queue(
                    maxsize=self.max_queue_size
                )
                logger.debug(f"[Queue] 创建账号队列: {account_id}")
            return self.queues[account_id]

    async def enqueue_task(self, account_id: str, task: DownloadTask) -> bool:
        """
        将任务加入队列

        Returns:
            True表示加入成功，False表示队列已满
        """
        queue = await self.get_queue(account_id)

        try:
            queue.put_nowait(task)
            logger.info(
                f"[Queue] 任务入队: {task.request_id} -> {account_id}"
            )
            return True
        except asyncio.QueueFull:
            logger.error(f"[Queue] 队列已满: {account_id}")
            return False

    async def dequeue_task(self, account_id: str) -> Optional[DownloadTask]:
        """从队列中取出一个任务"""
        queue = await self.get_queue(account_id)

        try:
            task = queue.get_nowait()
            logger.debug(f"[Queue] 任务出队: {task.request_id}")
            return task
        except asyncio.QueueEmpty:
            return None

    async def get_queue_size(self, account_id: str) -> int:
        """获取队列大小"""
        queue = await self.get_queue(account_id)
        return queue.qsize()

    async def clear_queue(self, account_id: str):
        """清空队列"""
        queue = await self.get_queue(account_id)
        while not queue.empty():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        logger.info(f"[Queue] 清空队列: {account_id}")


# ==================== 账号调度器 ====================
class AccountScheduler:
    """
    账号调度器

    核心规则：
    - 只有活跃账号的任务立即执行
    - 非活跃账号的任务进队列
    - 账号切换时暂停当前并启动新Daemon
    - 完全单账号活跃（不允许多个账号并行）
    """

    def __init__(self, download_config: Optional[Dict[str, Any]] = None):
        """
        初始化调度器

        Args:
            download_config: 下载配置字典
        """
        # 配置
        self.config = download_config or {}
        # [TERMINAL-ONLY-LOGS-2026-02-01] Force terminal-only logs by default.
        # Users can override by setting DOWNLOAD_LOG_DISABLE_FILES=0 before launch.
        os.environ.setdefault("DOWNLOAD_LOG_DISABLE_FILES", "1")
        os.environ.setdefault("DOWNLOAD_LOG_CONSOLE_LEVEL", "INFO")
        # [LOG-BOTH-2026-02-01] Force both terminal and file logs to be enabled.
        os.environ["DOWNLOAD_LOG_DISABLE_FILES"] = "0"
        os.environ["DAEMON_LOG_DISABLE_FILES"] = "0"
        os.environ["DOWNLOAD_LOG_BOTH"] = "1"
        os.environ["DOWNLOAD_LOG_CONSOLE_LEVEL"] = os.environ.get("DOWNLOAD_LOG_CONSOLE_LEVEL", "INFO")
        # [LOG-BOTH-2026-02-01] Ensure root logger prints to terminal if not configured.
        try:
            root_logger = logging.getLogger()
            if not root_logger.handlers:
                logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
        except Exception as log_setup_err:
            logger.warning(f"[Scheduler] Root logger setup failed: {log_setup_err}")
        # [TERMINAL-LOG-2026-02-01] Ensure download logs are visible in terminal.
        try:
            from download_logger import ensure_terminal_logging
            self._terminal_logger = ensure_terminal_logging("terminal")
        except Exception as log_err:
            logger.warning(f"[Scheduler] Terminal logger init failed: {log_err}")
            self._terminal_logger = None
        # 统一日志文件路径（主进程和 daemon 共享）
        self._unified_log_path: Optional[str] = None
        self._main_file_handler: Optional[logging.FileHandler] = None
        # [FIX-SESSION-PATH] ??Session??????????? data/accounts ???????
        # [REASON] ???????????Daemon ?????????? session_path
        self.session_path_resolver: Optional[Callable[[str], Optional[str]]] = None

        # 账号状态
        self.active_account_id: Optional[str] = None
        self.account_daemons: Dict[str, 'DaemonProcess'] = {}
        self.ipc_channels: Dict[str, 'IPCChannel'] = {}

        # ✅ [FIX-2026-02-01] IPC 接收循环任务字典
        # 【问题】主进程缺少 IPC 接收循环，导致 ACK 和事件无法被处理
        # 【修复】为每个 IPC 连接创建后台接收任务
        # 字典格式：{account_id: asyncio.Task}
        self.ipc_receive_tasks: Dict[str, asyncio.Task] = {}

        # 任务管理
        self.queue_manager = TaskQueueManager(
            max_queue_size=self.config.get('account_scheduler_max_queue_size', 1000)
        )
        self.active_downloads: Dict[str, DownloadTask] = {}  # request_id -> task

        # 事件总线
        self.event_bus: Optional['EventBus'] = None
        self.event_listener: Optional['DownloadEventListener'] = None

        # [REFRESH-DIALOGS-FIX-2026-02-02] 刷新对话列表回调函数
        # 由main.py在初始化时设置，用于处理Daemon发送的REFRESH_DIALOGS_REQUEST
        # 回调签名：def refresh_dialogs_callback(account_id: str, reason: str) -> None
        self.refresh_dialogs_callback: Optional[Callable[[str, str], None]] = None

        # 锁和标志
        self._lock = asyncio.Lock()
        self.shutdown_requested = False

        logger.info("[Scheduler] 初始化完成")

    def _setup_unified_logging(self, account_id: str):
        """
        设置统一日志文件（主进程 + daemon 共享）

        日志路径: logs/chatdex_{account_id}_{启动时间戳}.log
        每次启动生成独立日志文件，避免单文件无限增长。
        主进程标记: [MAIN]
        Daemon标记: [DAEMON]

        同时将主进程的 print() 输出也转发到日志文件，确保 [CHAIN-*] 等
        诊断信息也会被记录。
        """
        if self._unified_log_path:
            return  # 已设置

        import datetime
        log_dir = self.config.get('log_dir', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self._unified_log_path = os.path.abspath(os.path.join(log_dir, f"tgdownloader_{account_id}_{ts}.log"))

        # 为主进程 root logger 添加 FileHandler
        log_format = "%(asctime)s.%(msecs)03d [MAIN] %(name)s - %(levelname)s - %(message)s"
        date_format = "%Y-%m-%d %H:%M:%S"

        root_logger = logging.getLogger()
        file_handler = logging.FileHandler(self._unified_log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=date_format))
        root_logger.addHandler(file_handler)
        self._main_file_handler = file_handler

        # 将 sys.stdout 的 print() 也转发到日志文件
        # 这样 main.py 中的 print("[CHAIN-*]") 等诊断信息也会被记录
        try:
            self._install_stdout_tee(self._unified_log_path)
        except Exception as tee_err:
            logger.warning(f"[Scheduler] stdout 转发安装失败（非致命）: {tee_err}")

        logger.info(f"[Scheduler] 统一日志已启用 -> {self._unified_log_path}")

    @staticmethod
    def _install_stdout_tee(log_path: str):
        """
        将 sys.stdout 包装为 TeeStream，使 print() 同时输出到终端和日志文件。
        """
        import io

        import datetime as _dt_module

        class _TeeStream:
            """写入时同时输出到原始 stdout 和日志文件"""

            def __init__(self, original_stdout, log_file_path):
                self._original = original_stdout
                self._log_fp = open(log_file_path, 'a', encoding='utf-8', errors='replace')
                self._dt = _dt_module

            def write(self, msg):
                try:
                    self._original.write(msg)
                except Exception as e:
                    logger.debug(f"[TeeStream] stdout write failed: {e}")
                if msg and msg.strip():
                    try:
                        ts = self._dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                        self._log_fp.write(f"{ts} [MAIN] [print] - INFO - {msg}")
                        if not msg.endswith('\n'):
                            self._log_fp.write('\n')
                        self._log_fp.flush()
                    except Exception as e:
                        logger.debug(f"[TeeStream] log write failed: {e}")

            def flush(self):
                try:
                    self._original.flush()
                except Exception:
                    pass
                try:
                    self._log_fp.flush()
                except Exception:
                    pass

            def close(self):
                try:
                    if self._log_fp and not self._log_fp.closed:
                        self._log_fp.flush()
                        self._log_fp.close()
                except Exception:
                    pass

            def __del__(self):
                self.close()

            def fileno(self):
                return self._original.fileno()

            def isatty(self):
                return hasattr(self._original, 'isatty') and self._original.isatty()

            @property
            def encoding(self):
                return getattr(self._original, 'encoding', 'utf-8')

        # 安装 TeeStream（关闭旧的再创建新的）
        if isinstance(sys.stdout, _TeeStream):
            old_original = sys.stdout._original
            sys.stdout.close()
            sys.stdout = _TeeStream(old_original, log_path)
        else:
            sys.stdout = _TeeStream(sys.stdout, log_path)

    async def set_event_bus(self, event_bus: 'EventBus'):
        """设置事件总线"""
        self.event_bus = event_bus
        logger.info("[Scheduler] 事件总线已设置")

    def set_session_path_resolver(self, resolver: Callable[[str], Optional[str]]):
        """?? Session ??????????/????????"""
        self.session_path_resolver = resolver
        logger.info("[Scheduler] Session ????????")

    # [REFRESH-DIALOGS-FIX-2026-02-02] 新增：设置刷新对话列表回调函数
    def set_refresh_dialogs_callback(self, callback: Callable[[str, str], None]):
        """
        设置刷新对话列表回调函数

        用于处理Daemon发送的REFRESH_DIALOGS_REQUEST事件。
        该回调会在接收到Daemon的刷新对话列表请求时被调用。

        Args:
            callback: 回调函数，签名为 callback(account_id: str, reason: str) -> None
                - account_id: 需要刷新对话的账号ID
                - reason: 刷新原因（"sync_completed", "account_switched", "manual"等）

        使用示例：
            scheduler.set_refresh_dialogs_callback(
                lambda account_id, reason: refresh_dialogs_list()
            )
        """
        self.refresh_dialogs_callback = callback
        logger.info(f"[Scheduler] 刷新对话列表回调已设置")

    async def set_active_account(self, account_id: str) -> bool:  # [BUG-FIX] 改为返回bool而非void
        """
        切换活跃账号

        规则：
        1. 如果已有活跃账号，关闭其Daemon
        2. 启动新账号的Daemon
        3. 处理新账号的任务队列

        返回值：
        - True: 切换成功
        - False: 切换失败（Session文件缺失或其他错误）
        """
        async with self._lock:
            # [NEW-DEBUG-LOG] 添加set_active_account入口日志
            logger.info(f"[Scheduler] [DEBUG] set_active_account入口: account_id={account_id}, active_account_id={self.active_account_id}")

            if self.active_account_id == account_id:
                logger.info(f"[Scheduler] 账号已是活跃: {account_id}")
                return True  # [BUG-FIX] 返回True表示成功（已经是活跃账号）

            logger.info(
                f"[Scheduler] [DEBUG] 开始切换账号: "
                f"{self.active_account_id} → {account_id}"
            )
            logger.info(
                f"[Scheduler] 切换账号: "
                f"{self.active_account_id} → {account_id}"
            )

            # 步骤1：关闭旧Daemon
            if self.active_account_id:
                old_account = self.active_account_id
                logger.info(f"[Scheduler] [DEBUG] 关闭旧Daemon: {old_account}")
                try:
                    await self._shutdown_daemon(old_account, save_state=True)
                    logger.info(f"[Scheduler] 旧Daemon已关闭: {old_account}")
                except Exception as e:
                    # [BUG-FIX] 关闭旧Daemon失败不应该阻止切换，继续执行
                    logger.warning(f"[Scheduler] 关闭旧Daemon失败（继续执行）: {e}")

            # 步骤2：更新活跃账号
            logger.info(f"[Scheduler] [DEBUG] 更新active_account_id: {account_id}")
            self.active_account_id = account_id

            # 步骤3：启动新Daemon（关键操作，需要捕获异常）
            logger.info(f"[Scheduler] [DEBUG] 步骤3：启动新Daemon")
            try:
                await self._ensure_daemon_running(account_id)
                logger.info(f"[Scheduler] [DEBUG] Daemon启动成功")
            except FileNotFoundError as e:
                # [BUG-FIX-CRITICAL] Session文件不存在时，不抛异常，返回False
                logger.error(
                    f"[Scheduler] ❌ 无法切换到账号 {account_id}\n"
                    f"原因: Session文件不存在\n"
                    f"路径: {str(e)}\n"
                    f"结果: 账号切换失败，active_account_id已重置为None"
                )
                # [NEW-DEBUG-LOG] 添加状态重置前的日志
                logger.info(f"[Scheduler] [DEBUG] 由于FileNotFoundError，重置active_account_id为None")
                # 重置状态，避免Ghost State（幽灵状态）
                self.active_account_id = None
                return False  # [BUG-FIX] 返回False表示失败

            except Exception as e:
                # [BUG-FIX-CRITICAL] 其他异常也捕获，防止传播导致崩溃
                logger.error(
                    f"[Scheduler] ❌ 启动Daemon失败: {account_id}\n"
                    f"错误类型: {type(e).__name__}\n"
                    f"错误信息: {str(e)}\n"
                    f"结果: 账号切换失败，active_account_id已重置为None"
                )
                # [NEW-DEBUG-LOG] 添加状态重置前的日志
                logger.info(f"[Scheduler] [DEBUG] 由于异常，重置active_account_id为None")
                # 重置状态，避免Ghost State（幽灵状态）
                self.active_account_id = None
                return False  # [BUG-FIX] 返回False表示失败

            # 步骤4：处理队列中的任务（只在成功启动Daemon后执行）
            logger.info(f"[Scheduler] [DEBUG] 步骤4：处理队列中的任务")
            try:
                await self._process_queued_tasks(account_id)
                logger.info(f"[Scheduler] [DEBUG] 队列处理完成")
            except Exception as e:
                # [BUG-FIX] 处理队列异常也不要崩溃，只记录警告
                logger.warning(f"[Scheduler] 处理队列异常（不影响切换结果）: {e}")

            # [BUG-FIX] 成功执行完所有步骤，返回True
            logger.info(f"[Scheduler] [DEBUG] set_active_account成功完成，返回True")
            logger.info(f"[Scheduler] ✅ 账号切换成功: {account_id}")
            return True

    async def submit_download(
        self,
        account_id: str,
        chat_id: int,
        msg_id: int,
        save_path: str,
        media_type: Optional[str] = None,
        file_size: int = 0,
        progress_callback: Optional[Callable] = None,
        complete_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None
    ) -> str:
        """
        提交下载任务

        关键规则：
        - 如果是活跃账号，立即提交给Daemon
        - 如果不是活跃账号，加入队列，立即返回

        Returns:
            request_id (字符串)
        """
        # [ENHANCED-LOG-2026-02-01] 记录任务提交详情
        import os
        file_name = os.path.basename(save_path)
        logger.info(
            f"[Scheduler] ========== 提交下载任务 =========="
        )
        logger.info(
            f"[Scheduler] 文件: {file_name}"
        )
        logger.info(
            f"[Scheduler] 账号: {account_id}"
        )
        logger.info(
            f"[Scheduler] 当前活跃账号: {self.active_account_id}"
        )

        async with self._lock:
            # [FIX-CANCEL-2026-02-16] 去重检测：拒绝相同 (chat_id, msg_id) 的重复下载
            for _existing_rid, _existing_task in self.active_downloads.items():
                _e_chat = getattr(_existing_task, 'chat_id', None)
                _e_msg = getattr(_existing_task, 'msg_id', None)
                if _e_chat == chat_id and _e_msg == msg_id:
                    logger.warning(
                        f"[Scheduler] [DEDUP] 拒绝重复提交: chat_id={chat_id}, msg_id={msg_id}, "
                        f"existing_req={_existing_rid[:8]}..."
                    )
                    return _existing_rid  # 返回已有的 request_id

            # 生成request_id
            request_id = str(uuid.uuid4())
            logger.info(f"[Scheduler] Request ID: {request_id}")

            # 创建下载任务
            task = DownloadTask(
                request_id=request_id,
                account_id=account_id,
                chat_id=chat_id,
                msg_id=msg_id,
                save_path=save_path,
                media_type=media_type,
                file_size=file_size,
                progress_callback=progress_callback,
                complete_callback=complete_callback,
                error_callback=error_callback
            )

            # ✅ [FIX-2026-02-01] 添加活跃账号判断诊断日志
            # 【目的】确认 submit_download 时 active_account_id 是否已正确设置
            # 【证据链】此前因异步竞态，active_account_id=None 导致所有任务进队列
            logger.info(
                f"[Scheduler-Submit] 活跃账号判断: "
                f"account_id={account_id}, "
                f"active_account_id={self.active_account_id}, "
                f"match={account_id == self.active_account_id}"
            )

            # 判断是否为活跃账号
            if account_id == self.active_account_id:
                # 活跃账号：立即提交给Daemon
                logger.info(
                    f"[Scheduler] ✅ 活跃账号任务 - 立即执行"
                )
                logger.info(
                    f"[Scheduler] Request: {request_id} | Chat: {chat_id} | Msg: {msg_id}"
                )

                # [ENHANCED-LOG] 记录Daemon启动时机
                daemon_exists = account_id in self.account_daemons
                daemon_running = daemon_exists and self.account_daemons[account_id].is_running()
                logger.info(
                    f"[Scheduler] Daemon状态: exists={daemon_exists}, running={daemon_running}"
                )

                # 确保Daemon运行
                await self._ensure_daemon_running(account_id)

                logger.info(f"[Scheduler] 正在提交任务到 Daemon...")
                # 提交给Daemon
                await self._submit_to_daemon(account_id, task)

                # 记录任务
                # [FIX-DEADLOCK-2026-02-16] 移除嵌套 async with self._lock（已在外层持锁）
                self.active_downloads[request_id] = task
                logger.info(f"[Scheduler] 任务提交完成，活跃下载数: {len(self.active_downloads)}")

            else:
                # 非活跃账号：加入队列
                logger.info(
                    f"[Scheduler] ⏳ 非活跃账号任务 - 加入队列"
                )
                logger.info(
                    f"[Scheduler] Request: {request_id} | Account: {account_id} | Chat: {chat_id}"
                )

                # [ENHANCED-LOG] 记录队列状态
                queue_size = await self.queue_manager.get_queue_size(account_id)
                logger.info(f"[Scheduler] 目标账号队列深度: {queue_size}")

                # 加入队列（不阻塞）
                success = await self.queue_manager.enqueue_task(
                    account_id, task
                )

                if not success:
                    logger.error(f"[Scheduler] ❌ 队列已满: {account_id}")
                    if task.error_callback:
                        await self._safe_call_callback(
                            task.error_callback,
                            "队列已满"
                        )
                    return request_id

                new_queue_size = await self.queue_manager.get_queue_size(account_id)
                logger.info(
                    f"[Scheduler] 任务已入队，队列深度: {queue_size} -> {new_queue_size}"
                )

            return request_id

    # [FIX-CANCEL-2026-02-16] 取消下载任务
    async def cancel_download(self, request_id: str):
        """取消下载任务

        流程：
        1. 从 active_downloads 中移除
        2. 发送 CancelDownloadRequest 给 Daemon 进程
        3. Daemon 收到后停止 WorkerPool，保存 checkpoint

        Args:
            request_id: 要取消的任务 ID
        """
        from download_protocol_v2 import CancelDownloadRequest
        from download_ipc import MessageType, IPCMessage
        import uuid as _uuid

        logger.info(f"[Scheduler] [CANCEL] 取消下载: request_id={request_id}")

        # 从 active_downloads 移除
        task = self.active_downloads.pop(request_id, None)
        if task:
            account_id = getattr(task, 'account_id', self.active_account_id)
            logger.info(f"[Scheduler] [CANCEL] 已从 active_downloads 移除: {request_id[:8]}...")
        else:
            account_id = self.active_account_id
            logger.warning(f"[Scheduler] [CANCEL] 任务不在 active_downloads 中: {request_id[:8]}...")

        # 发送取消请求给 Daemon
        if account_id and account_id in self.account_daemons:
            daemon = self.account_daemons[account_id]
            if daemon.is_running() and daemon.ipc:
                try:
                    cancel_req = CancelDownloadRequest(
                        request_id=str(_uuid.uuid4()),
                        target_request_id=request_id,
                        save_checkpoint=True,
                    )
                    ipc_msg = IPCMessage(
                        message_id=cancel_req.request_id,
                        message_type=MessageType.CANCEL_DOWNLOAD_REQUEST,
                        timestamp=cancel_req.timestamp,
                        payload=cancel_req.to_dict()
                    )
                    await daemon.ipc.send_with_ack(ipc_msg, timeout=5.0)
                    logger.info(f"[Scheduler] [CANCEL] ✅ CancelDownloadRequest 已发送给 Daemon: {request_id[:8]}...")
                except Exception as send_err:
                    logger.error(f"[Scheduler] [CANCEL] 发送取消请求失败（Daemon可能正忙）: {send_err}")
            else:
                logger.warning(f"[Scheduler] [CANCEL] Daemon未运行，跳过IPC取消")
        else:
            logger.warning(f"[Scheduler] [CANCEL] 无活跃Daemon，无法发送取消请求")

    async def _ensure_daemon_running(self, account_id: str):
        """确保账号的Daemon正在运行

        [FIX-DEADLOCK-2026-02-16] 移除 async with self._lock
        此方法仅在 set_active_account() 和 submit_download() 中调用，
        调用方已持有 self._lock。asyncio.Lock 不可重入，嵌套获取会死锁。
        """
        # [NEW-DEBUG-LOG] 添加调试日志，跟踪Daemon启动流程
        logger.info(f"[Scheduler] [DEBUG] _ensure_daemon_running 被调用: account_id={account_id}")

        # [FIX-DEADLOCK-2026-02-16] 不再获取锁（调用方已持锁）
        if account_id in self.account_daemons:
            daemon = self.account_daemons[account_id]
            logger.info(f"[Scheduler] [DEBUG] 发现已有Daemon: account_id={account_id}, is_running={daemon.is_running()}")
            if daemon.is_running():
                logger.debug(f"[Scheduler] Daemon已运行: {account_id}")
                return

        # 启动新Daemon
        logger.info(f"[Scheduler] [DEBUG] 启动新Daemon: account_id={account_id}")
        logger.info(f"[Scheduler] 启动Daemon: {account_id}")
        await self._start_daemon(account_id)

    async def _start_daemon(self, account_id: str):
        """
        启动账号的Daemon进程

        步骤：
        1. 复制session文件副本
        2. 启动daemon subprocess
        3. 建立IPC连接
        4. 等待daemon就绪
        """
        try:
            logger.info(f"[Daemon] 启动进程: {account_id}")

            # 步骤1：获取session路径（从config中）
            # 注意：实际应该从账号管理器获取session路径
            session_dir = self.config.get('session_dir', 'data/accounts')
            session_path = f"{session_dir}/{account_id}/session"
            session_daemon_path = f"{session_path}_daemon_{account_id}"

            # [NEW-DEBUG-LOG] 添加session路径的调试日志
            logger.info(f"[Daemon] [DEBUG] Session目录: {os.path.basename(session_dir)}")
            logger.info(f"[Daemon] [DEBUG] Session: {os.path.basename(session_path)}")

            # [FIX-SESSION-PATH] ????????? session_path???????? Session ??????
            resolved_session_path = None
            try:
                if callable(getattr(self, "session_path_resolver", None)):
                    resolved_session_path = self.session_path_resolver(account_id)
            except Exception as e:
                logger.error(f"[Daemon] Session ???????: {e}")
            if resolved_session_path:
                session_path = resolved_session_path
                session_daemon_path = f"{session_path}_daemon_{account_id}"
                logger.info(f"[Daemon] [DEBUG] Session路径已解析: {os.path.basename(session_path)}")

            # 确保session文件存在
            session_file = f"{session_path}.session"
            logger.info(f"[Daemon] [DEBUG] 检查Session文件: {os.path.basename(session_file)}")
            if not os.path.exists(session_file):
                # Session文件不存在时，打印有限调试信息
                logger.error(f"[Daemon] Session文件不存在: {os.path.basename(session_file)}")
                logger.error(f"[Daemon] 当前工作目录: {os.getcwd()}")
                raise FileNotFoundError(f"Session文件不存在: {os.path.basename(session_file)}")

            logger.info(f"[Daemon] [DEBUG] Session文件存在，准备复制")

            # [PHASE1.5] Robust session copy: .session + .session-wal + .session-shm
            # NOTE: This replaces legacy single-file copy to avoid SQLite WAL inconsistency.
            try:
                _copy_session_triple_with_retries(
                    session_path_base=session_path,
                    daemon_path_base=session_daemon_path,
                    max_retries=5,
                    base_delay=0.2,
                )
                logger.info(
                    f"[Daemon] [PHASE1.5] Session triple copy done: {session_path} -> {session_daemon_path}"
                )
            except Exception as copy_err:
                logger.error(
                    f"[Daemon] [PHASE1.5] Session triple copy failed: {copy_err}",
                    exc_info=True,
                )
                raise

            # [DISABLED-LEGACY] Single-file copy is unsafe with WAL; kept for reference only.
            # logger.info(f"[Daemon] 复制session文件: {session_file} → {session_daemon_path}.session")
            # shutil.copy2(session_file, f"{session_daemon_path}.session")

            # 步骤2：确定IPC socket路径
            import tempfile
            daemon_socket_dir = self.config.get('daemon_socket_dir', '') or tempfile.gettempdir()
            socket_type = self.config.get('daemon_socket_type', 'unix')

            if socket_type == 'unix' and os.name != 'nt':
                # Unix socket (Linux/macOS)
                ipc_socket = f"{daemon_socket_dir}/daemon_{account_id}.sock"
            else:
                # TCP端口 (Windows或其他)
                tcp_port = self.config.get('daemon_tcp_port', 9999)
                ipc_socket = str(tcp_port)

            logger.info(f"[Daemon] IPC路径: {ipc_socket}")

            # 步骤3：启动daemon subprocess
            log_level = self.config.get('daemon_log_level', 'INFO')
            watchdog_timeout = self.config.get('daemon_watchdog_timeout', 60)

            # 设置统一日志（主进程 + daemon 共享同一个日志文件）
            self._setup_unified_logging(account_id)

            daemon_script = os.path.join(os.path.dirname(__file__), 'download_daemon.py')
            cmd = [
                sys.executable,
                daemon_script,
                '--session', session_daemon_path,
                '--account-id', account_id,
                '--ipc-socket', ipc_socket,
                '--log-level', log_level,
                '--watchdog-timeout', str(watchdog_timeout),
                '--api-id', str(self.config.get('api_id', 0)),
                '--api-hash', self.config.get('api_hash', ''),
            ]
            # 传递统一日志路径给 daemon
            if self._unified_log_path:
                cmd.extend(['--log-file', self._unified_log_path])

            logger.info(f"[Daemon] 启动命令: {' '.join(cmd)}")
            # [FIX-2026-02-01] Force UTF-8 for daemon stdio to avoid GBK decode errors/garbled logs.
            popen_env = os.environ.copy()
            popen_env.setdefault("PYTHONUTF8", "1")
            popen_env.setdefault("PYTHONIOENCODING", "utf-8")
            # [TERMINAL-ONLY-LOGS-2026-02-01] Ensure daemon uses terminal-only logging.
            popen_env.setdefault("DAEMON_LOG_DISABLE_FILES", "1")
            popen_env.setdefault("DOWNLOAD_LOG_DISABLE_FILES", "1")
            popen_env.setdefault("DOWNLOAD_LOG_CONSOLE_LEVEL", "INFO")
            # [LOG-BOTH-2026-02-01] Force daemon to keep both terminal and file logs.
            popen_env["DAEMON_LOG_DISABLE_FILES"] = "0"
            popen_env["DOWNLOAD_LOG_DISABLE_FILES"] = "0"
            popen_env["DOWNLOAD_LOG_BOTH"] = "1"

            # [DISABLED-2026-02-01] Legacy Popen without explicit encoding/env caused GBK decode failures.
            # process = subprocess.Popen(
            #     cmd,
            #     stdout=subprocess.PIPE,
            #     stderr=subprocess.PIPE,
            #     text=True
            # )

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=popen_env,
            )

            logger.info(f"[Daemon] 进程已启动 PID={process.pid}")
            # [LOG] Drain daemon stdout/stderr for diagnostics (ACK timeouts, IPC errors)
            _start_daemon_pipe_logger(process, f"(PID={process.pid}, account={account_id})")

            # [LOG] Early exit detection
            try:
                exit_code = process.poll()
                if exit_code is not None:
                    logger.error(f"[Daemon] 进程异常退出: PID={process.pid}, code={exit_code}")
            except Exception as poll_err:
                logger.warning(f"[Daemon] 进程状态检查失败: {poll_err}")

            # 步骤4：建立IPC连接（等待daemon启动）
            from download_ipc import IPCChannel

            # ❌ [CRITICAL BUG - 已屏蔽] UnboundLocalError: cannot access local variable 'os'
            # ❌ 原代码：import os  (第485行)
            # ❌
            # ❌ 【BUG 根因】
            # ❌ 1. 文件顶部第18行已全局导入：import os
            # ❌ 2. 此处（第485行）在函数内部重复导入：import os
            # ❌ 3. Python编译器发现函数内有 `import os` 语句
            # ❌ 4. 编译器将 `os` 标记为**局部变量**（而非全局变量）
            # ❌ 5. 第431行 `os.path.exists()` 在第485行 `import os` **之前**执行
            # ❌ 6. 访问局部变量 `os` 时，该变量尚未被赋值
            # ❌ 7. 触发：UnboundLocalError: cannot access local variable 'os' where it is not associated with a value
            # ❌
            # ❌ 【完整错误栈】
            # ❌ File "download_manager_v2.py", line 431, in _start_daemon
            # ❌     if not os.path.exists(session_file):
            # ❌            ^^
            # ❌ UnboundLocalError: cannot access local variable 'os' where it is not associated with a value
            # ❌
            # ❌ 【影响范围】
            # ❌ - Daemon 启动失败（第431行异常退出）
            # ❌ - active_account_id 被重置为 None
            # ❌ - 所有下载请求失败（无活跃账号）
            # ❌
            # ❌ 【修复方案】
            # ❌ 删除此行重复导入（os 已在文件顶部第18行全局导入）
            # ❌
            # ❌ 已屏蔽的代码：
            # ❌ import os
            # ✅ [FIX] 使用全局导入的 os 模块（第18行），不再重复导入

            logger.info(f"[Daemon-IPC-Fix] 使用全局导入的 os 模块（download_manager_v2.py:18），避免UnboundLocalError")

            # [FIX-2026-02-01] Windows IPC 连接失败修复 (WinError 10049)
            #
            # 【BUG 位置】lines 486-488 (原代码)
            # 【BUG 现象】WinError 10049: 在其上下文中，该请求的地址无效
            # 【BUG 原因】Windows 上 socket_type='unix' 导致参数错误配置
            #
            # 【完整证据链】
            # 1. socket_type='unix' 是默认配置（download_config.py:81）
            # 2. 在 Windows(os.name='nt') 上仍使用 socket_type='unix'
            # 3. 原逻辑：socket_type == 'unix' → socket_path="9999" ✗
            # 4. 原逻辑：socket_type != 'unix' → False → tcp_port=None ✗
            # 5. IPCChannel.__init__ 中：tcp_port=None → self.tcp_port=0 ✗
            # 6. connect() 尝试连接到 127.0.0.1:0 → WinError 10049 ✗
            #
            # 【修复方案】检查 os.name，在 Windows 上强制使用 TCP 模式
            # - Windows (os.name='nt'): 必须使用 TCP loopback (127.0.0.1:port)
            # - Linux/macOS (os.name='posix'): 使用 Unix Domain Socket

            # ❌ [DEPRECATED CODE - BUG代码已屏蔽]
            # ❌ 原代码在Windows上错误配置IPC参数：
            # ❌ ipc = IPCChannel(
            # ❌     socket_path=ipc_socket if socket_type == 'unix' else None,  # Windows上错误：设置为"9999"
            # ❌     tcp_port=int(ipc_socket) if socket_type != 'unix' else None   # Windows上错误：设置为None→0
            # ❌ )

            # ✅ [FIX CODE - 修复后的代码]
            # 判断当前OS类型
            is_windows = os.name == 'nt'
            is_unix_like = os.name == 'posix'

            # 根据 OS 和 socket_type 选择正确的 IPC 参数
            if is_windows or socket_type != 'unix':
                # Windows 或显式要求TCP：使用 TCP loopback
                ipc_socket_path = None
                ipc_tcp_port = int(ipc_socket)
                logger.info(f"[Daemon-IPC] 配置TCP模式 (OS={os.name}, socket_type={socket_type}, port={ipc_tcp_port})")
            else:
                # Linux/macOS 且 socket_type='unix'：使用 Unix Domain Socket
                ipc_socket_path = ipc_socket
                ipc_tcp_port = None
                logger.info(f"[Daemon-IPC] 配置Unix Domain Socket模式 (OS={os.name}, socket_type={socket_type}, path={ipc_socket_path})")

            ipc = IPCChannel(
                socket_path=ipc_socket_path,
                tcp_port=ipc_tcp_port
            )

            logger.info(f"[Daemon-IPC] IPCChannel 已初始化: socket_path={ipc_socket_path}, tcp_port={ipc_tcp_port}")

            # 等待daemon就绪（重试机制）
            max_retries = int(self.config.get('daemon_ipc_connect_max_retries', 10))
            retry_delay = float(self.config.get('daemon_ipc_connect_retry_delay', 0.5))

            for attempt in range(max_retries):
                try:
                    # [FIX-2026-02-01] 移除 timeout 参数
                    # [ROOT-CAUSE] IPCChannel.connect() 不接受 timeout 参数
                    # [SIGNATURE] async def connect(self, is_server: bool = False) - 第 download_ipc.py:166 行
                    # [BUG] 原代码传递了 timeout=2.0，导致 TypeError
                    # [SOLUTION] 移除多余的 timeout 参数
                    await ipc.connect(is_server=False)  # ← 移除 timeout=2.0
                    logger.info(f"[Daemon] IPC连接成功 (尝试 {attempt + 1}/{max_retries})")
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.debug(f"[Daemon] 连接失败，重试 ({attempt + 1}/{max_retries}): {e}")
                        await asyncio.sleep(retry_delay)
                    else:
                        raise RuntimeError(f"无法建立IPC连接到daemon: {e}")

            # [DISABLED-2026-02-01] 旧代码：传递了多余的 timeout 参数
            # [BUG] await ipc.connect(is_server=False, timeout=2.0)
            # [ERROR] TypeError: IPCChannel.connect() got an unexpected keyword argument 'timeout'
            # [REASON] IPCChannel.connect() 的实际签名中没有 timeout 参数
            # [IMPACT] 导致 Daemon 启动失败，active_account_id 被重置为 None
            # [RESULT] 所有下载任务进入队列，永远不会被处理
            # for attempt in range(max_retries):
            #     try:
            #         await ipc.connect(is_server=False, timeout=2.0)  # ❌ 多余的参数
            #         logger.info(f"[Daemon] IPC连接成功 (尝试 {attempt + 1}/{max_retries})")
            #         break
            #     except Exception as e:
            #         if attempt < max_retries - 1:
            #             logger.debug(f"[Daemon] 连接失败，重试 ({attempt + 1}/{max_retries}): {e}")
            #             await asyncio.sleep(retry_delay)
            #         else:
            #             raise RuntimeError(f"无法建立IPC连接到daemon: {e}")

            # 步骤5：存储daemon信息
            # [FIX-DEADLOCK-2026-02-16] 移除嵌套锁（调用方 set_active_account/submit_download 已持锁）
            daemon_process = DaemonProcess(account_id, process, ipc)
            self.account_daemons[account_id] = daemon_process
            self.ipc_channels[account_id] = ipc

            logger.info(f"[Daemon] Daemon初始化完成: {account_id} (PID={process.pid})")

            # ✅ [FIX-2026-02-01] 步骤6：启动 IPC 接收循环
            #
            # 【问题】主进程缺少 IPC 接收循环，导致 ACK 和事件无法被处理
            # 【根因】send_with_ack() 等待 Future，但从未调用 receive() 处理 ACK
            # 【修复】启动后台任务不断调用 receive() 处理消息
            logger.info(f"[Daemon] 启动 IPC 接收循环: {account_id}")

            # 创建后台接收任务
            receive_task = asyncio.create_task(
                self._start_ipc_receive_loop(account_id),
                name=f"ipc_receiver_{account_id}"
            )

            # 保存任务引用（用于关闭时取消）
            self.ipc_receive_tasks[account_id] = receive_task

            logger.info(f"[Daemon] IPC 接收循环已启动 (task_name={receive_task.get_name()})")

        except Exception as e:
            logger.error(f"[Daemon] 启动失败 {account_id}: {e}", exc_info=True)
            # 清理已分配的资源
            # [FIX-DEADLOCK-2026-02-16] 移除嵌套锁（调用方已持锁）
            if account_id in self.ipc_channels:
                try:
                    await self.ipc_channels[account_id].close()
                except Exception:
                    pass
                del self.ipc_channels[account_id]
            if account_id in self.account_daemons:
                del self.account_daemons[account_id]
            raise

    async def _start_ipc_receive_loop(self, account_id: str):
        """
        启动 IPC 接收循环（后台任务）

        [FIX-2026-02-01] 主进程缺少 IPC 接收循环，导致 ACK 和事件无法被处理

        【问题分析】
        原代码中：
        1. 主进程调用 send_with_ack() 发送消息并等待 ACK
        2. Daemon 接收消息后立即发送 ACK
        3. 但主进程从未调用 receive() 来读取 ACK
        4. ACK 在 IPC 缓冲区中无人处理
        5. 5秒超时后报告 "ACK超时"

        【根本原因】
        - send_with_ack() 创建 Future 等待 ACK
        - receive() 中有处理 ACK 的逻辑（设置 Future 结果）
        - 但主进程从不调用 receive()
        - Future 永远不会被设置，导致超时

        【解决方案】
        启动后台任务持续调用 receive() 来处理：
        1. ACK 消息（设置 Future 结果）
        2. 事件消息（进度、完成、错误）

        Args:
            account_id: 账号ID

        日志追踪：
        - [IPC-Receiver] 启动接收循环
        - [IPC-Receiver] 接收到消息
        - [IPC-Receiver] ACK 已处理
        - [IPC-Receiver] 事件已处理
        - [IPC-Receiver] 接收循环停止
        """
        ipc = self.ipc_channels.get(account_id)
        if not ipc:
            logger.error(f"[IPC-Receiver] IPC连接不存在: {account_id}")
            return

        logger.info(f"[IPC-Receiver] 启动接收循环: {account_id}")

        try:
            idle_ticks = 0
            while account_id in self.ipc_channels:
                try:
                    # 接收消息（1秒超时）
                    message = await ipc.receive(timeout=1.0)

                    if message is None:
                        # 超时，继续循环
                        idle_ticks += 1
                        if idle_ticks % 30 == 0:
                            logger.info(
                                f"[IPC-Receiver] 无消息({idle_ticks}s) account_id={account_id}, "
                                f"ipc_connected={ipc.is_connected()}"
                            )
                        continue
                    idle_ticks = 0

                    # 日志输出
                    logger.info(
                        f"[IPC-Receiver] 接收到消息: "
                        f"type={message.message_type.name}, "
                        f"message_id={message.message_id}"
                    )

                    # ACK 消息已在 receive() 中自动处理
                    # 参见 download_ipc.py:457-461

                    # 处理其他事件消息
                    from download_ipc import MessageType

                    if message.message_type == MessageType.TASK_PROGRESS_EVENT:
                        await self._handle_progress_event(message, account_id)

                    elif message.message_type == MessageType.TASK_COMPLETED_EVENT:
                        await self._handle_completed_event(message, account_id)

                    elif message.message_type == MessageType.TASK_STARTED_EVENT:
                        await self._handle_started_event(message, account_id)

                    elif message.message_type == MessageType.DAEMON_STATUS_EVENT:
                        await self._handle_daemon_status_event(message, account_id)

                    # [REFRESH-DIALOGS-FIX-2026-02-02] 处理刷新对话列表请求
                    elif message.message_type == MessageType.REFRESH_DIALOGS_REQUEST:
                        await self._handle_refresh_dialogs_request(message, account_id)

                    elif message.message_type == MessageType.INIT_COMPLETED:
                        logger.info(f"[IPC-Receiver] Daemon 初始化完成: {account_id}")

                    elif message.message_type == MessageType.ACK_MESSAGE:
                        # ACK 已在 receive() 中处理
                        logger.info(f"[IPC-Receiver] ACK 已处理: {message.message_id}")

                    else:
                        logger.info(
                            f"[IPC-Receiver] 未处理的消息类型: "
                            f"{message.message_type.name}"
                        )

                except asyncio.CancelledError:
                    logger.info(f"[IPC-Receiver] 接收任务被取消: {account_id}")
                    break

                except Exception as e:
                    logger.error(
                        f"[IPC-Receiver] 接收消息异常: {e}",
                        exc_info=True
                    )
                    # 短暂等待后继续
                    await asyncio.sleep(0.5)

        finally:
            logger.info(f"[IPC-Receiver] 接收循环已停止: {account_id}")

    async def _handle_progress_event(self, message: 'IPCMessage', account_id: str):
        """处理进度事件"""
        from download_protocol_v2 import TaskProgressEvent

        payload = message.payload
        event = TaskProgressEvent.from_dict(payload)

        logger.debug(
            f"[Event-Progress] "
            f"request_id={event.request_id}, "
            f"progress={event.percentage:.1f}%"
        )

        # 更新任务进度
        async with self._lock:
            task = self.active_downloads.get(event.request_id)
            if task:
                task.downloaded = event.current
                task.file_size = event.total
                task.percentage = event.percentage
                progress_cb = task.progress_callback
            else:
                progress_cb = None

        # 回调在锁外执行，避免长持有
        if progress_cb:
            await self._safe_call_callback(
                progress_cb,
                event.current,
                event.total,
                event.percentage
            )

    async def _handle_completed_event(self, message: 'IPCMessage', account_id: str):
        """处理完成事件"""
        from download_protocol_v2 import TaskCompletedEvent

        payload = message.payload
        event = TaskCompletedEvent.from_dict(payload)

        logger.info(
            f"[Event-Completed] "
            f"request_id={event.request_id}, "
            f"success={event.success}"
        )

        # 更新任务状态
        async with self._lock:
            task = self.active_downloads.get(event.request_id)
            if task:
                if event.success:
                    task.state = DownloadTaskState.COMPLETED
                    task.percentage = 100.0
                else:
                    task.state = DownloadTaskState.FAILED
                    task.error_message = event.error_message
                complete_cb = task.complete_callback
                # 从活跃下载中移除
                self.active_downloads.pop(event.request_id, None)
            else:
                complete_cb = None

        # 回调在锁外执行
        if complete_cb:
            await self._safe_call_callback(
                complete_cb,
                event.success,
                event.file_path,
                event.error_message
            )

    async def _handle_started_event(self, message: 'IPCMessage', account_id: str):
        """处理开始事件"""
        from download_protocol_v2 import TaskStartedEvent

        payload = message.payload
        event = TaskStartedEvent.from_dict(payload)

        logger.info(
            f"[Event-Started] "
            f"request_id={event.request_id}, "
            f"file_name={event.file_name}, "
            f"file_size={event.file_size}"
        )

        # 更新任务信息
        async with self._lock:
            task = self.active_downloads.get(event.request_id)
            if task:
                task.file_name = event.file_name
                task.file_size = event.file_size
                task.state = DownloadTaskState.DOWNLOADING

    async def _handle_daemon_status_event(self, message: 'IPCMessage', account_id: str):
        """处理 Daemon 状态事件"""
        payload = message.payload
        status = payload.get('status', 'unknown')

        logger.info(f"[Event-DaemonStatus] account_id={account_id}, status={status}")

    # [REFRESH-DIALOGS-FIX-2026-02-02] 处理刷新对话列表请求
    async def _handle_refresh_dialogs_request(self, message: 'IPCMessage', account_id: str):
        """
        处理 Daemon 发送的刷新对话列表请求

        [ROOT-CAUSE] Daemon进程无法访问主进程的account_manager和db_service_ref全局引用
        [SOLUTION] Daemon通过IPC发送REFRESH_DIALOGS_REQUEST，主进程处理后调用已注册的回调函数

        Args:
            message: IPC消息
            account_id: 账号ID（用于验证）
        """
        from download_protocol_v2 import RefreshDialogsRequest

        try:
            payload = message.payload
            request = RefreshDialogsRequest.from_dict(payload)

            logger.info(
                f"[Event-RefreshDialogs] 收到Daemon刷新对话列表请求 "
                f"(account_id={account_id}, reason={request.reason})"
            )

            # 验证账号ID匹配
            if request.account_id != account_id:
                logger.warning(
                    f"[Event-RefreshDialogs] ⚠️ 账号ID不匹配 "
                    f"(expected={account_id}, got={request.account_id})"
                )
                return

            # 调用已注册的refresh_dialogs回调函数
            if hasattr(self, 'refresh_dialogs_callback') and self.refresh_dialogs_callback:
                logger.debug(f"[Event-RefreshDialogs] 调用refresh_dialogs_callback")
                try:
                    # 调用回调函数（必须在主进程UI线程执行）
                    self.refresh_dialogs_callback(account_id, request.reason)
                    logger.info(f"[Event-RefreshDialogs] ✅ 刷新对话列表请求已处理")
                except Exception as callback_err:
                    logger.error(
                        f"[Event-RefreshDialogs] 回调函数执行异常: {callback_err}",
                        exc_info=True
                    )
            else:
                logger.warning(
                    f"[Event-RefreshDialogs] ⚠️ refresh_dialogs_callback未注册，无法刷新对话列表"
                )

        except Exception as e:
            logger.error(
                f"[Event-RefreshDialogs] 处理刷新对话列表请求异常: {e}",
                exc_info=True
            )

    async def _shutdown_daemon(self, account_id: str, save_state: bool = True):
        """
        关闭账号的Daemon进程

        步骤：
        1. 发送ShutdownRequest
        2. 等待响应（timeout=5秒）
        3. 如果超时则强制kill
        4. 清理IPC连接
        5. 清理临时文件
        """
        try:
            logger.info(
                f"[Daemon] 关闭进程: {account_id} (save_state={save_state})"
            )

            # ✅ [FIX-2026-02-01] 步骤0：停止 IPC 接收循环
            # 【原因】Daemon 关闭前，应先停止后台接收任务
            # 【处理】取消 IPC 接收任务，防止异常
            if hasattr(self, 'ipc_receive_tasks'):
                receive_task = self.ipc_receive_tasks.pop(account_id, None)
                if receive_task and not receive_task.done():
                    logger.info(f"[Daemon] 取消 IPC 接收任务: {account_id}")
                    receive_task.cancel()
                    try:
                        await receive_task
                    except asyncio.CancelledError:
                        logger.info(f"[Daemon] IPC 接收任务已取消: {account_id}")
                    except Exception as e:
                        logger.error(f"[Daemon] 取消接收任务异常: {e}")

            # 步骤1：获取daemon对象
            daemon = self.account_daemons.get(account_id)
            ipc = self.ipc_channels.get(account_id)

            if not daemon:
                logger.warning(f"[Daemon] Daemon不存在: {account_id}")
                return

            # 步骤2：如果daemon仍在运行，发送关闭请求
            if daemon.is_running():
                try:
                    from download_protocol_v2 import ShutdownRequest

                    request = ShutdownRequest(
                        request_id=str(uuid.uuid4()),
                        save_state=save_state
                    )

                    logger.info(f"[Daemon] 发送ShutdownRequest到: {account_id}")

                    if ipc:
                        # 发送关闭请求并等待ACK (timeout=5秒)
                        try:
                            await ipc.send_with_ack(request, timeout=5.0)
                            logger.info(f"[Daemon] ShutdownRequest已发送")

                            # 等待daemon优雅关闭（最多10秒）
                            for i in range(10):
                                if not daemon.is_running():
                                    logger.info(f"[Daemon] Daemon已优雅关闭")
                                    break
                                await asyncio.sleep(1)

                        except asyncio.TimeoutError:
                            logger.warning(f"[Daemon] ShutdownRequest超时，准备强制关闭")

                except Exception as e:
                    logger.error(f"[Daemon] 发送关闭请求失败: {e}")

            # 步骤3：强制kill（如果进程仍在运行）
            if daemon.is_running():
                logger.warning(f"[Daemon] 强制kill进程: {account_id} (PID={daemon.get_pid()})")
                try:
                    daemon.process.kill()
                    daemon.process.wait(timeout=5)
                except Exception as e:
                    logger.error(f"[Daemon] kill进程失败: {e}")

            # 步骤4：关闭IPC连接
            if ipc:
                try:
                    await ipc.close()
                    logger.info(f"[Daemon] IPC连接已关闭: {account_id}")
                except Exception as e:
                    logger.error(f"[Daemon] 关闭IPC失败: {e}")

            # 步骤5：清理session副本文件
            session_dir = self.config.get('session_dir', 'data/accounts')
            session_path = f"{session_dir}/{account_id}/session"
            session_daemon_path = f"{session_path}_daemon_{account_id}"

            try:
                session_file = f"{session_daemon_path}.session"
                if os.path.exists(session_file):
                    os.remove(session_file)
                    logger.info(f"[Daemon] 清理session副本: {session_file}")
            except Exception as e:
                logger.error(f"[Daemon] 清理session副本失败: {e}")

            # 步骤6：清理daemon记录
            if account_id in self.account_daemons:
                del self.account_daemons[account_id]
            if account_id in self.ipc_channels:
                del self.ipc_channels[account_id]

            logger.info(f"[Daemon] 进程关闭完成: {account_id}")

        except Exception as e:
            logger.error(f"[Daemon] 关闭失败 {account_id}: {e}", exc_info=True)

    async def _submit_to_daemon(self, account_id: str, task: DownloadTask):
        """将任务提交给Daemon"""
        try:
            # 获取IPC连接
            ipc = self.ipc_channels.get(account_id)
            if not ipc:
                raise RuntimeError(f"IPC连接不存在: {account_id}")

            # ❌ [CRITICAL BUG - 已屏蔽] AttributeError: 'DownloadRequestV2' object has no attribute 'message_id'
            # ❌ 原代码直接传入 DownloadRequestV2 对象到 send_with_ack()
            # ❌
            # ❌ 【BUG 根因】
            # ❌ 1. DownloadRequestV2 是业务协议对象（Protocol Object），只有 request_id
            # ❌ 2. IPCMessage 是传输层对象（Transport Object），要求有 message_id
            # ❌ 3. send_with_ack(message: IPCMessage) 期望 IPCMessage 类型
            # ❌ 4. 第743行直接传入 DownloadRequestV2 对象
            # ❌ 5. send_with_ack() 内部第273行访问 message.message_id
            # ❌ 6. DownloadRequestV2 没有此属性，触发 AttributeError
            # ❌
            # ❌ 【完整错误栈】
            # ❌ File "download_manager_v2.py", line 743, in _submit_to_daemon
            # ❌     await ipc.send_with_ack(request, timeout=ipc_timeout)
            # ❌ File "download_ipc.py", line 273, in send_with_ack
            # ❌     self._pending_acks[message.message_id] = ack_future
            # ❌                        ^^^^^^^^^^^^^^^^^^
            # ❌ AttributeError: 'DownloadRequestV2' object has no attribute 'message_id'
            # ❌
            # ❌ 【影响范围】
            # ❌ - 所有下载任务提交到 Daemon 时失败
            # ❌ - 任务状态被标记为 FAILED
            # ❌ - 下载流程中断
            # ❌
            # ❌ 【修复方案】
            # ❌ 需要将 DownloadRequestV2（业务对象）转换为 IPCMessage（传输对象）
            # ❌ 转换时：
            # ❌ - message_id = request.request_id（使用业务ID作为消息ID）
            # ❌ - message_type = MessageType.DOWNLOAD_REQUEST
            # ❌ - payload = request.to_dict()（序列化业务数据）
            # ❌
            # ❌ 已屏蔽的代码：
            # ❌ from download_protocol_v2 import DownloadRequestV2
            # ❌ request = DownloadRequestV2(...)
            # ❌ await ipc.send_with_ack(request, timeout=ipc_timeout)

            logger.info(f"[Task-Submit] 开始提交任务到Daemon: request_id={task.request_id}")

            # ✅ [FIX CODE - 修复后的代码]
            # 步骤1：构造业务请求对象（Protocol Layer）
            from download_protocol_v2 import DownloadRequestV2

            request = DownloadRequestV2(
                request_id=task.request_id,
                chat_id=task.chat_id,
                msg_id=task.msg_id,
                save_path=task.save_path,
                account_id=account_id,
                media_type=task.media_type,
                file_size=task.file_size
            )

            logger.info(
                f"[Task-Submit] 创建业务请求对象: "
                f"request_id={request.request_id}, "
                f"chat_id={request.chat_id}, "
                f"msg_id={request.msg_id}"
            )

            # 步骤2：转换为IPC传输层消息（Transport Layer）
            from download_ipc import IPCMessage, MessageType

            ipc_message = IPCMessage(
                message_id=request.request_id,  # ✅ 使用 request_id 作为 message_id
                message_type=MessageType.DOWNLOAD_REQUEST,  # ✅ 设置消息类型
                timestamp=request.timestamp,  # ✅ 保留时间戳
                payload=request.to_dict()  # ✅ 序列化业务数据到 payload
            )

            logger.info(
                f"[Task-Submit] 转换为IPC消息: "
                f"message_id={ipc_message.message_id}, "
                f"message_type={ipc_message.message_type.name}, "
                f"payload_keys={list(ipc_message.payload.keys())}"
            )

            # 步骤3：更新任务状态
            task.state = DownloadTaskState.DOWNLOADING
            logger.info(f"[Task-Submit] 任务状态已更新: {task.state.name}")

            # 步骤4：发送IPC消息到Daemon（带ACK确认）
            ipc_timeout = self.config.get('daemon_ipc_timeout', 5.0)

            logger.info(
                f"[Task-Submit] 准备发送IPC消息: "
                f"message_id={ipc_message.message_id}, "
                f"timeout={ipc_timeout}s"
            )

            await ipc.send_with_ack(ipc_message, timeout=ipc_timeout)  # ✅ 传入 IPCMessage 对象

            logger.info(
                f"[Task] 任务已提交到Daemon: "
                f"{task.request_id} (chat={task.chat_id}, msg={task.msg_id})"
            )

        except Exception as e:
            logger.error(f"[Task] 提交失败: {task.request_id}: {e}", exc_info=True)
            task.state = DownloadTaskState.FAILED
            task.error_message = str(e)

            # 调用错误回调
            if task.error_callback:
                await self._safe_call_callback(task.error_callback, str(e))

    async def _process_queued_tasks(self, account_id: str):
        """处理新账号队列中的任务"""
        logger.info(f"[Scheduler] 处理队列任务: {account_id}")

        while True:
            # 检查是否仍是活跃账号
            if self.active_account_id != account_id:
                logger.info(f"[Scheduler] 账号已不活跃，停止处理队列: {account_id}")
                break

            # 从队列取出任务
            task = await self.queue_manager.dequeue_task(account_id)
            if task is None:
                # 队列为空
                break

            logger.info(f"[Scheduler] 处理队列任务: {task.request_id}")

            # 提交给Daemon
            try:
                await self._submit_to_daemon(account_id, task)
                self.active_downloads[task.request_id] = task
            except Exception as e:
                logger.error(f"[Scheduler] 处理队列任务失败: {e}")
                if task.error_callback:
                    await self._safe_call_callback(task.error_callback, str(e))

    async def _safe_call_callback(self, callback: Callable, *args, **kwargs):
        """安全地调用回调函数（捕获异常）"""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args, **kwargs)
            else:
                callback(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[Callback] 回调异常: {e}")

    async def shutdown_all_daemons(self):
        """关闭所有Daemon"""
        logger.info("[Scheduler] 关闭所有Daemon...")

        for account_id in list(self.account_daemons.keys()):
            await self._shutdown_daemon(account_id, save_state=True)

        logger.info("[Scheduler] 所有Daemon已关闭")


# ==================== 下载事件监听器 ====================
class DownloadEventListener:
    """
    下载事件监听器

    职责：
    - 订阅Daemon发送的事件
    - 查找对应的UI回调
    - 调用回调函数（安全地，不影响下载）
    - 错误处理和重试
    """

    def __init__(
        self,
        scheduler: AccountScheduler,
        event_bus: 'EventBus'
    ):
        """
        初始化事件监听器

        Args:
            scheduler: 账号调度器
            event_bus: 事件总线
        """
        self.scheduler = scheduler
        self.event_bus = event_bus

    async def subscribe_all(self):
        """订阅所有事件"""
        logger.info("[Listener] 订阅所有事件...")

        # 订阅任务开始事件
        self.event_bus.subscribe(
            'TaskStartedEvent',
            self.on_task_started,
            priority=100
        )

        # 订阅进度事件
        self.event_bus.subscribe(
            'TaskProgressEvent',
            self.on_task_progress,
            priority=50
        )

        # 订阅任务完成事件
        self.event_bus.subscribe(
            'TaskCompletedEvent',
            self.on_task_completed,
            priority=100
        )

        # 订阅Daemon状态事件
        self.event_bus.subscribe(
            'DaemonStatusEvent',
            self.on_daemon_status,
            priority=10
        )

        logger.info("[Listener] 事件订阅完成")

    async def on_task_started(self, event: 'TaskStartedEvent'):
        """处理任务开始事件"""
        request_id = event.request_id
        task = self.scheduler.active_downloads.get(request_id)

        if not task:
            logger.warning(f"[Listener] 任务未找到: {request_id}")
            return

        logger.info(
            f"[Listener] 任务开始: {request_id} "
            f"({event.file_name}, {event.file_size} bytes)"
        )

        task.file_name = event.file_name
        task.file_size = event.file_size
        task.state = DownloadTaskState.DOWNLOADING
        task.started_at = time.time()

        # 不调用回调（应用可监听事件总线直接更新UI）

    async def on_task_progress(self, event: 'TaskProgressEvent'):
        """处理进度事件"""
        request_id = event.request_id
        task = self.scheduler.active_downloads.get(request_id)

        if not task:
            return

        # 更新进度
        task.downloaded = event.current
        task.percentage = event.percentage

        # 调用进度回调（如果存在）
        if task.progress_callback:
            try:
                await self.scheduler._safe_call_callback(
                    task.progress_callback,
                    event.current,
                    event.total
                )
            except Exception as e:
                logger.warning(f"[Listener] 进度回调异常: {e}")
                # 继续，不影响下载

        logger.debug(f"[Listener] 进度更新: {request_id} {event.percentage:.1f}%")

    async def on_task_completed(self, event: 'TaskCompletedEvent'):
        """处理任务完成事件"""
        request_id = event.request_id
        task = self.scheduler.active_downloads.get(request_id)

        if not task:
            logger.warning(f"[Listener] 任务未找到: {request_id}")
            return

        logger.info(
            f"[Listener] 任务完成: {request_id} "
            f"(success={event.success})"
        )

        # 更新任务状态
        if event.success:
            task.state = DownloadTaskState.COMPLETED
            task.file_name = event.file_name or task.file_name
            task.file_size = event.file_size
        else:
            task.state = DownloadTaskState.FAILED
            task.error_message = event.error_message

        task.completed_at = time.time()

        # 调用完成回调
        if task.complete_callback and event.success:
            try:
                await self.scheduler._safe_call_callback(
                    task.complete_callback,
                    event.file_path,
                    event.file_size
                )
            except Exception as e:
                logger.warning(f"[Listener] 完成回调异常: {e}")

        # 调用错误回调
        if task.error_callback and not event.success:
            try:
                await self.scheduler._safe_call_callback(
                    task.error_callback,
                    event.error_message
                )
            except Exception as e:
                logger.warning(f"[Listener] 错误回调异常: {e}")

        # 从活跃下载中移除
        del self.scheduler.active_downloads[request_id]

    async def on_daemon_status(self, event: 'DaemonStatusEvent'):
        """处理Daemon状态事件"""
        logger.debug(
            f"[Listener] Daemon状态: {event.status} "
            f"(memory={event.memory_mb:.1f}MB)"
        )

        # 可选：监控内存，超过阈值时触发告警
        # if event.memory_mb > 4000:
        #     logger.warning(f"[Listener] Daemon内存过高: {event.memory_mb}MB")


# ==================== Daemon进程包装 ====================
class DaemonProcess:
    """Daemon进程包装类"""

    def __init__(
        self,
        account_id: str,
        process: subprocess.Popen,
        ipc_channel: 'IPCChannel'
    ):
        self.account_id = account_id
        self.process = process
        self.ipc_channel = ipc_channel
        self.created_at = time.time()

    def is_running(self) -> bool:
        """检查进程是否运行中"""
        return self.process.poll() is None

    @property
    def ipc(self):
        """[FIX-CANCEL-2026-02-16] IPC通道的便捷访问"""
        return self.ipc_channel

    def get_pid(self) -> Optional[int]:
        """获取进程ID"""
        return self.process.pid

    def get_uptime(self) -> float:
        """获取运行时间（秒）"""
        return time.time() - self.created_at


# ==================== Completion Handling Patch (2026-02-02) ====================
# [BUG-EXPLAIN] The original completion handlers call callbacks before removing
# active_downloads, so UI refreshes still see the task as "active" and disable
# the "open" button. The event-bus listener also calls complete_callback with a
# mismatched signature. We only add code and override handlers to bypass the
# buggy paths without deleting them.
# [GOAL] 1) Pop active_downloads BEFORE callbacks, 2) Deduplicate completion
# across IPC + EventBus, 3) Keep callback signature consistent:
# complete_callback(success, file_path, error_message).

def _dmv2_init_completion_guard(scheduler: 'AccountScheduler') -> None:
    """Initialize completion guard containers lazily."""
    if not hasattr(scheduler, "_dmv2_completion_lock"):
        scheduler._dmv2_completion_lock = asyncio.Lock()
    if not hasattr(scheduler, "_dmv2_completed_request_ids"):
        scheduler._dmv2_completed_request_ids = {}
    if not hasattr(scheduler, "_dmv2_completed_request_ids_max"):
        scheduler._dmv2_completed_request_ids_max = 5000
    if not hasattr(scheduler, "_dmv2_completed_request_ids_ttl"):
        scheduler._dmv2_completed_request_ids_ttl = 3600.0


def _dmv2_cleanup_completion_guard(scheduler: 'AccountScheduler') -> None:
    """Best-effort cleanup to avoid unbounded growth."""
    completed = getattr(scheduler, "_dmv2_completed_request_ids", None)
    if not completed:
        return
    max_size = getattr(scheduler, "_dmv2_completed_request_ids_max", 5000)
    ttl = getattr(scheduler, "_dmv2_completed_request_ids_ttl", 3600.0)
    if len(completed) <= max_size:
        return
    now = time.time()
    expired = [rid for rid, ts in completed.items() if now - ts > ttl]
    for rid in expired:
        completed.pop(rid, None)
    if len(completed) > max_size:
        # Drop oldest 10% if still too large.
        drop_count = max(1, len(completed) // 10)
        for rid, _ in sorted(completed.items(), key=lambda kv: kv[1])[:drop_count]:
            completed.pop(rid, None)


async def _dmv2_process_completed_event(
    scheduler: 'AccountScheduler',
    event: 'TaskCompletedEvent',
    source: str = "unknown"
) -> None:
    """Unified completion handling with ordering + dedup."""
    _dmv2_init_completion_guard(scheduler)
    request_id = getattr(event, "request_id", None)
    if not request_id:
        logger.error("[Completion-Patch] Missing request_id in completion event")
        return

    async with scheduler._dmv2_completion_lock:
        completed = scheduler._dmv2_completed_request_ids
        if request_id in completed:
            logger.warning(
                f"[Completion-Patch] Duplicate completion ignored "
                f"(request_id={request_id}, source={source})"
            )
            return

        task = scheduler.active_downloads.pop(request_id, None)
        if not task:
            # [FIX-CANCEL-2026-02-16] 任务可能已被用户取消（正常行为）
            logger.info(
                f"[Completion-Patch] 任务 {request_id[:8]}... 不在 active_downloads 中（可能已取消）"
            )
            logger.warning(
                f"[Completion-Patch] Task not found in active_downloads "
                f"(request_id={request_id}, source={source})"
            )
            return

        completed[request_id] = time.time()
        _dmv2_cleanup_completion_guard(scheduler)

    # Update task state outside the lock and invoke callbacks.
    try:
        if event.success:
            task.state = DownloadTaskState.COMPLETED
            task.file_name = event.file_name or task.file_name
            task.file_size = event.file_size
        else:
            task.state = DownloadTaskState.FAILED
            task.error_message = event.error_message
        task.completed_at = time.time()

        if task.complete_callback:
            await scheduler._safe_call_callback(
                task.complete_callback,
                event.success,
                event.file_path,
                event.error_message
            )

        if task.error_callback and not event.success:
            await scheduler._safe_call_callback(
                task.error_callback,
                event.error_message
            )

        logger.info(
            f"[Completion-Patch] Processed completion "
            f"(request_id={request_id}, success={event.success}, source={source})"
        )
    except Exception as patch_err:
        logger.error(
            f"[Completion-Patch] Completion handling failed "
            f"(request_id={request_id}, source={source}): {patch_err}",
            exc_info=True
        )


async def _dmv2_handle_completed_event_patched(
    self: 'AccountScheduler',
    message: 'IPCMessage',
    account_id: str
):
    """Patched IPC completion handler (pop before callbacks + dedup)."""
    from download_protocol_v2 import TaskCompletedEvent
    payload = message.payload
    event = TaskCompletedEvent.from_dict(payload)
    logger.info(
        f"[Event-Completed][Patched] request_id={event.request_id}, "
        f"success={event.success}, account_id={account_id}"
    )
    await _dmv2_process_completed_event(self, event, source="ipc")


async def _dmv2_listener_on_task_completed_patched(
    self: 'DownloadEventListener',
    event: 'TaskCompletedEvent'
):
    """Patched EventBus handler to reuse the unified completion path."""
    await _dmv2_process_completed_event(self.scheduler, event, source="event_bus")


if not getattr(AccountScheduler, "_dmv2_completion_patch_applied", False):
    AccountScheduler._dmv2_completion_patch_applied = True
    AccountScheduler._handle_completed_event = _dmv2_handle_completed_event_patched
    DownloadEventListener.on_task_completed = _dmv2_listener_on_task_completed_patched
    logger.info("[Completion-Patch] Applied completion handler override")


if __name__ == '__main__':
    # 测试代码
    async def test():
        import sys
        logging.basicConfig(level=logging.DEBUG)

        # 创建调度器
        scheduler = AccountScheduler()

        # 创建任务
        request_id = await scheduler.submit_download(
            account_id='test_account',
            chat_id=123,
            msg_id=456,
            save_path='/tmp/test.zip',
            media_type='document'
        )

        print(f"任务已提交: {request_id}")

    asyncio.run(test())
