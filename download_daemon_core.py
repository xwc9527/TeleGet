"""
Daemon核心逻辑 - 独立下载守护进程的核心

功能：
1. Session加载和TelegramClient初始化
2. 下载请求处理
3. Worker池管理
4. 事件发送
5. 账号切换
6. 优雅关闭

状态机：
INITIALIZING → IDLE → DOWNLOADING → SHUTTING_DOWN
"""

import asyncio
import logging
import os

import time
from enum import Enum
from typing import Optional, Dict, Any
from typing import Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ==================== 状态枚举 ====================
class DaemonState(Enum):
    """Daemon状态"""
    INITIALIZING = "initializing"  # 初始化中
    IDLE = "idle"                  # 空闲
    DOWNLOADING = "downloading"    # 下载中
    PAUSED = "paused"              # 暂停
    SHUTTING_DOWN = "shutting_down"  # 关闭中


# ==================== 小文件批量下载配置 ====================
def _get_small_file_threshold():
    try:
        from download_config import get_config_value
        return int(get_config_value("small_file_threshold_bytes", 5242880))
    except Exception:
        return 5 * 1024 * 1024

def _get_small_file_pool_size():
    try:
        from download_config import get_config_value
        return int(get_config_value("small_file_pool_size", 5))
    except Exception:
        return 5


def _validate_save_path(save_path: str) -> str:
    """验证并规范化保存路径，防止路径穿越"""
    import pathlib
    p = pathlib.Path(save_path)
    if '..' in p.parts:
        raise ValueError(f"路径包含非法遍历: {save_path}")
    return str(p.resolve())


# ==================== Daemon核心 ====================
class DaemonCore:
    """
    Daemon核心逻辑

    职责：
    - 管理TelegramClient和连接池
    - 处理来自主进程的下载请求
    - 发送进度和完成事件
    - 处理账号切换和优雅关闭
    """

    def __init__(
        self,
        session_path: str,
        account_id: str,
        ipc_channel: 'IPCChannel',
        event_bus: 'EventBus',
        api_id: int = 0,
        api_hash: str = ''
    ):
        """
        初始化Daemon核心

        Args:
            session_path: Session文件路径（副本）
            account_id: 账号ID
            ipc_channel: IPC通道
            event_bus: 事件总线
            api_id: Telegram API ID
            api_hash: Telegram API Hash
        """
        self.session_path = session_path
        self.account_id = account_id
        self.ipc = ipc_channel
        self.event_bus = event_bus
        self.api_id = api_id
        self.api_hash = api_hash

        # 状态
        self.state = DaemonState.INITIALIZING
        self.shutdown_requested = False

        # TelegramClient和连接池
        self.client: Optional[Any] = None  # TelegramClient实例
        self.download_pool: Optional[Any] = None  # DownloadPool实例

        # 活跃任务
        self.active_tasks: Dict[str, Any] = {}  # request_id -> task
        self._tasks_lock = asyncio.Lock()
        # [FIX-CANCEL-2026-02-16] 每个任务的取消事件
        self._cancel_events: Dict[str, asyncio.Event] = {}  # request_id -> cancel_event

        # 启动时间
        self.start_time = time.time()

        # [NEW-CHECKPOINT-MANAGER-2026-02-05] Part 级断点续传
        self.checkpoint_manager: Optional[Any] = None

        # 跟踪所有创建的 TelegramClient，确保 shutdown 时全部断开
        self._tracked_clients: list = []

        logger.info(
            f"[DaemonCore] 初始化完成 "
            f"(account={account_id}, session={os.path.basename(session_path)})"
        )

    async def run(self):
        """
        主循环

        流程：
        1. 初始化session和连接池
        2. 等待客户端连接
        3. 通知主进程初始化完成
        4. 循环接收和处理请求
        5. 关闭时清理资源（异常时也保证执行）
        """
        try:
            # 步骤1：初始化
            logger.info("[DaemonCore] 开始初始化...")
            await self._initialize()

            logger.info("[DaemonCore] 初始化完成，等待IPC通道就绪...")

            try:
                await self.ipc.wait_ready(timeout=10.0)
                logger.info("[DaemonCore] IPC通道已就绪，客户端已连接")

            except TimeoutError as e:
                logger.error(f"[DaemonCore] IPC就绪超时: {e}")
                logger.error("[DaemonCore] 客户端未在规定时间内连接，Daemon无法继续")
                raise

            # 步骤2：通知主进程初始化完成
            logger.info("[DaemonCore] 发送初始化完成通知...")
            await self._send_init_completed()
            logger.info("[DaemonCore] 初始化完成通知已发送")

            # 步骤3：主循环
            logger.info("[DaemonCore] 进入主循环...")
            self.state = DaemonState.IDLE

            while not self.shutdown_requested:
                message = await self.ipc.receive(timeout=1.0)

                if message is None:
                    continue

                await self._process_message(message)

            # 步骤4：关闭
            logger.info("[DaemonCore] 退出主循环，开始关闭...")
            await self.shutdown()

        except Exception as e:
            logger.error(f"[DaemonCore] 运行异常: {e}", exc_info=True)

            # 异常时强制清空 active_tasks
            if self.active_tasks:
                logger.warning(
                    f"[DaemonCore] 异常发生时有 {len(self.active_tasks)} 个任务残留，强制清空"
                )
                for request_id, task_info in list(self.active_tasks.items()):
                    try:
                        await self._emit_task_completed(
                            request_id,
                            success=False,
                            file_path=task_info.get("save_path"),
                            file_size=0,
                            error_message=f"Daemon异常退出: {str(e)}"
                        )
                    except Exception as emit_err:
                        logger.warning(f"[Run] 发送失败事件失败 {request_id}: {emit_err}")

                self.active_tasks.clear()

            # 确保异常时也执行 shutdown 清理
            try:
                self.shutdown_requested = True
                await self.shutdown()
            except Exception as se:
                logger.error(f"[DaemonCore] shutdown error after run exception: {se}", exc_info=True)

    async def _initialize(self):
        """
        初始化session和连接池

        步骤：
        1. 验证session文件存在
        2. 导入必要模块
        3. 初始化TelegramClient
        4. 初始化DownloadPool
        5. 状态转为IDLE
        """
        logger.info("[Init] 开始初始化...")

        # 步骤1：验证session文件
        session_file = f"{self.session_path}.session"
        if not os.path.exists(session_file):
            raise FileNotFoundError(
                f"Session文件不存在: {session_file}"
            )
        logger.info(f"[Init] Session文件已找到: {os.path.basename(session_file)}")

        # [WORKERPOOL-PATH-2026-02-01] 使用 WorkerPool 并发下载（已启用）
        # NOTE: 弃用 Phase1.5 单连接模式，启用真正的并发下载
        try:
            # 初始化并发客户端池（12个客户端）
            logger.info("[Init] 初始化 WorkerPool 并发客户端池...")
            self.client = await self._init_telethon_client_single()  # 主客户端（用于消息获取）
            self.download_pool = None  # WorkerPool 由 worker 创建（动态）

            # [NEW-CHECKPOINT-MANAGER-2026-02-05] 初始化 CheckpointManager
            try:
                from download_checkpoint import CheckpointManager
                self.checkpoint_manager = CheckpointManager()
                await self.checkpoint_manager.init()
                logger.info("[Init] [CHECKPOINT-2026-02-05] CheckpointManager 已初始化")
            except Exception as checkpoint_init_err:
                logger.error(
                    f"[Init] [CHECKPOINT-2026-02-05] CheckpointManager 初始化失败: {checkpoint_init_err}",
                    exc_info=True
                )
                logger.warning("[Init] [CHECKPOINT-2026-02-05] 将继续运行但不支持断点续传")
                self.checkpoint_manager = None

            self.state = DaemonState.IDLE
            from download_config import get_config_value
            pool_sz = get_config_value("connection_pool_size", 4)
            logger.info(f"[Init] WorkerPool 路径已启用（{pool_sz}个并发连接）")
            return
        except Exception as init_err:
            logger.error(
                f"[Init] WorkerPool 初始化失败: {init_err}",
                exc_info=True,
            )
            raise

    async def _send_init_completed(self):
        """发送初始化完成通知"""
        from download_protocol_v2 import InitializationCompleted
        from download_ipc import IPCMessage, MessageType
        import time
        import uuid

        message = IPCMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.INIT_COMPLETED,
            timestamp=time.time(),
            payload=InitializationCompleted().to_dict()
        )

        await self.ipc.send(message)
        logger.info("[DaemonCore] 已发送初始化完成通知")

    # ==================== Phase 1.5 helpers ====================
    async def _init_telethon_client_single(self):
        """Initialize a single TelegramClient for download_media (Phase 1.5)."""
        from telethon import TelegramClient
        from download_config import get_config_value

        API_ID = self.api_id
        API_HASH = self.api_hash

        # Optional proxy config
        proxy_to_use = None
        try:
            from proxy_utils import get_proxy_for_telethon, format_proxy_status

            proxy_mode = get_config_value("proxy_mode", "auto")
            custom_proxy = get_config_value("custom_proxy", None)
            fallback_to_direct = get_config_value("proxy_fallback_to_direct", True)
            test_timeout = get_config_value("proxy_test_timeout", 3.0)

            proxy_to_use, proxy_status = get_proxy_for_telethon(
                mode=proxy_mode,
                custom_proxy=custom_proxy,
                fallback_to_direct=fallback_to_direct,
                test_timeout=test_timeout,
                logger_func=lambda msg: logger.info(f"[DaemonCore][Proxy] {msg}"),
            )
            logger.info(f"[DaemonCore][Proxy] {format_proxy_status(proxy_status)}")
        except Exception as proxy_err:
            logger.warning(f"[DaemonCore][Proxy] Proxy init failed, fallback direct: {proxy_err}")
            proxy_to_use = None

        client = TelegramClient(
            self.session_path,
            API_ID,
            API_HASH,
            system_version="4.16.30-vxCUSTOM",
            proxy=proxy_to_use,
            flood_sleep_threshold=0,  # [FIX-FLOOD-2026-02-04] 禁用自动 sleep，让 FloodWait 抛异常触发全局熔断
        )

        await client.connect()
        is_auth = await client.is_user_authorized()
        if not is_auth:
            raise RuntimeError("Session not authorized")

        logger.info("[DaemonCore] TelegramClient connected and authorized")
        self._tracked_clients.append(client)
        return client

    async def _emit_task_started(self, request_id: str, file_name: str, file_size: int):
        from download_protocol_v2 import TaskStartedEvent
        from download_ipc import MessageType

        event = TaskStartedEvent(
            request_id=request_id,
            file_name=file_name,
            file_size=file_size or 0,
        )
        await self._send_event(MessageType.TASK_STARTED_EVENT, event.to_dict())

    async def _emit_task_progress(self, request_id: str, current: int, total: int, percentage: float):
        from download_protocol_v2 import TaskProgressEvent
        from download_ipc import MessageType

        event = TaskProgressEvent(
            request_id=request_id,
            current=int(current),
            total=int(total) if total is not None else 0,
            percentage=float(percentage),
        )
        await self._send_event(MessageType.TASK_PROGRESS_EVENT, event.to_dict())

    async def _emit_task_completed(
        self,
        request_id: str,
        success: bool,
        file_path: Optional[str],
        file_size: int,
        error_message: Optional[str],
    ):
        from download_protocol_v2 import TaskCompletedEvent
        from download_ipc import MessageType

        event = TaskCompletedEvent(
            request_id=request_id,
            success=bool(success),
            file_path=file_path,
            file_size=int(file_size or 0),
            error_message=error_message,
        )
        await self._send_event(MessageType.TASK_COMPLETED_EVENT, event.to_dict())

    def _validate_downloaded_file(
        self,
        file_path: Optional[str],
        expected_size: Optional[int] = None,
    ) -> Tuple[bool, int, Optional[str]]:
        """Validate downloaded file exists and size is reasonable."""
        if not file_path:
            return False, 0, "file_path is empty"
        if not os.path.exists(file_path):
            return False, 0, "file not found"
        try:
            actual_size = os.path.getsize(file_path)
        except OSError as e:
            return False, 0, f"stat failed: {e}"
        if actual_size <= 0:
            return False, actual_size, "file size is 0"
        if expected_size and expected_size > 0 and actual_size < expected_size:
            return False, actual_size, f"size mismatch (expected {expected_size}, got {actual_size})"
        return True, actual_size, None

    async def _process_message(self, message: 'IPCMessage'):
        """
        处理接收到的消息

        消息类型：
        - DownloadRequestV2: 下载请求
        - CancelDownloadRequest: 取消下载请求 [FIX-CANCEL-2026-02-16]
        - SwitchAccountRequest: 账号切换
        - HeartbeatRequest: 心跳请求
        - ShutdownRequest: 关闭请求
        - ReloadSessionRequest: 重新加载session
        """
        from download_ipc import MessageType

        message_type = message.message_type

        logger.debug(f"[Message] 收到消息: {message_type.name}")

        try:
            # 发送ACK确认
            await self.ipc.send_ack(message.message_id)

            # 根据类型分发
            if message_type == MessageType.DOWNLOAD_REQUEST:
                # [FIX-CANCEL-2026-02-16] 用 create_task 替代 await，避免阻塞主循环
                # 阻塞主循环会导致无法接收后续的取消/心跳/关闭消息
                asyncio.create_task(self._handle_download_request(message))

            elif message_type == MessageType.CANCEL_DOWNLOAD_REQUEST:
                # [FIX-CANCEL-2026-02-16] 处理取消请求
                await self._handle_cancel_request(message)

            elif message_type == MessageType.HEARTBEAT_REQUEST:
                await self._handle_heartbeat_request(message)

            elif message_type == MessageType.SHUTDOWN_REQUEST:
                await self._handle_shutdown_request(message)

            else:
                logger.warning(f"[Message] 未知消息类型: {message_type}")

        except Exception as e:
            logger.error(f"[Message] 处理消息异常: {e}", exc_info=True)

    async def _handle_download_request(self, message: 'IPCMessage'):
        """
        处理下载请求

        分流逻辑：
        - file_size > 0 且 < 50MB: 小文件批量并行下载（download_media）
        - file_size >= 50MB 或 未知(0): WorkerPool 并发分片下载
        """
        from download_protocol_v2 import DownloadRequestV2

        payload = message.payload
        request = DownloadRequestV2.from_dict(payload)

        # [FIX-CANCEL-2026-02-16] 去重检测：拒绝相同 (chat_id, msg_id) 的重复下载
        _dup_rid = None
        for _at_rid, _at_info in self.active_tasks.items():
            if _at_info.get('chat_id') == request.chat_id and _at_info.get('msg_id') == request.msg_id:
                _dup_rid = _at_rid
                break
        if _dup_rid:
            logger.warning(f"[DEDUP-DAEMON] 拒绝重复请求: chat_id={request.chat_id}, msg_id={request.msg_id}")
            logger.warning(f"[DEDUP-DAEMON]   已存在任务: {_dup_rid[:8]}... 新请求: {request.request_id[:8]}...")
            logger.warning(
                f"[Download] 拒绝重复请求: chat={request.chat_id}, msg={request.msg_id}, "
                f"existing={_dup_rid}, new={request.request_id}"
            )
            await self._emit_task_completed(
                request.request_id, False, None, 0,
                error_message=f"重复请求：相同文件已在下载中 (existing={_dup_rid[:8]}...)"
            )
            return

        logger.info(f"[DEDUP-DAEMON] 无重复: chat_id={request.chat_id}, msg_id={request.msg_id}, active={len(self.active_tasks)}")

        logger.info(
            f"[Download] 接收下载请求: "
            f"chat_id={request.chat_id}, msg_id={request.msg_id}, "
            f"file_size={request.file_size:,} bytes"
        )

        if 0 < request.file_size < _get_small_file_threshold():
            logger.info(f"[Download] 小文件路径: {request.file_size / (1024*1024):.1f}MB < {_get_small_file_threshold() / (1024*1024):.0f}MB 阈值")
            await self._handle_small_file_batch(request)
        else:
            if request.file_size == 0:
                logger.info(f"[Download] 大文件路径（file_size 未知，保守走 WorkerPool）")
            else:
                logger.info(f"[Download] 大文件路径: {request.file_size / (1024*1024):.1f}MB >= {_get_small_file_threshold() / (1024*1024):.0f}MB 阈值")
            await self._handle_download_request_workerpool(request)

    async def _handle_small_file_batch(self, first_request):
        """
        小文件批量并行下载

        使用主客户端 self.client 发起并发 download_media() 调用。
        不创建池连接，避免多个克隆连接并发 ExportAuthorization 导致
        AuthBytesInvalidError（跨 DC 授权冲突）。

        单个 TelegramClient 通过 MTProto 消息复用支持并发请求，
        且 DC 授权缓存共享，跨 DC 下载不会冲突。
        """
        from download_protocol_v2 import DownloadRequestV2
        from download_ipc import MessageType

        small_requests = [first_request]
        deferred_messages = []  # 非小文件消息暂存

        # 非阻塞 drain：尽量收集更多小文件请求
        while True:
            msg = await self.ipc.receive(timeout=0.1)
            if msg is None:
                break
            # 发 ACK
            try:
                await self.ipc.send_ack(msg.message_id)
            except Exception as ack_err:
                logger.warning(f"[SmallBatch] ACK发送失败: {ack_err}")

            if msg.message_type == MessageType.DOWNLOAD_REQUEST:
                req = DownloadRequestV2.from_dict(msg.payload)
                if 0 < req.file_size < _get_small_file_threshold():
                    small_requests.append(req)
                else:
                    deferred_messages.append(msg)
            else:
                deferred_messages.append(msg)

        total_size = sum(r.file_size for r in small_requests)
        concurrency = min(_get_small_file_pool_size(), len(small_requests))
        logger.info(f"[SmallBatch] ========== 小文件批量下载 ==========")
        logger.info(f"[SmallBatch] 文件数: {len(small_requests)}, 总大小: {total_size / (1024*1024):.1f}MB, 并发度: {concurrency}")
        logger.info(f"[SmallBatch] 暂存消息: {len(deferred_messages)}")
        logger.info(
            f"[SmallBatch] 收集到 {len(small_requests)} 个小文件 "
            f"({total_size / (1024*1024):.1f}MB), 并发度={concurrency}, "
            f"{len(deferred_messages)} 个暂存消息"
        )

        # 使用主客户端 + asyncio.Queue 并发下载（无需额外连接池）
        try:
            queue = asyncio.Queue()
            for req in small_requests:
                queue.put_nowait(req)

            # N 个 worker 共享 self.client 并行消费
            workers = [
                asyncio.create_task(
                    self._small_file_worker(queue, worker_id=i)
                )
                for i in range(concurrency)
            ]
            await asyncio.gather(*workers)
        except Exception as e:
            logger.error(f"[SmallBatch] 批量下载异常: {e}", exc_info=True)

        logger.info(f"[SmallBatch] ========== 批量下载完成 ==========")

        # 处理暂存的消息
        for msg in deferred_messages:
            try:
                await self._process_message(msg)
            except Exception as e:
                logger.error(f"[SmallBatch] 处理暂存消息异常: {e}", exc_info=True)

    async def _small_file_worker(self, queue: asyncio.Queue, worker_id: int):
        """
        小文件 worker：从队列取任务，用 self.client.download_media() 下载。

        所有 worker 共享 self.client（主客户端），通过 MTProto 消息复用
        支持并发请求。DC 授权缓存在 client 内部共享，跨 DC 下载安全。
        """
        while not queue.empty():
            try:
                request = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            request_id = request.request_id
            self.active_tasks[request_id] = {
                "chat_id": request.chat_id,
                "msg_id": request.msg_id,
                "save_path": request.save_path,
            }
            self.state = DaemonState.DOWNLOADING
            download_start = time.time()

            try:
                # 1. 获取消息
                msg = await self.client.get_messages(request.chat_id, ids=request.msg_id)
                if isinstance(msg, list):
                    msg = msg[0] if msg else None

                if not msg or not getattr(msg, 'media', None):
                    err = "Message not found or has no media"
                    logger.warning(f"[SmallBatch][W{worker_id}] {err}: request_id={request_id}")
                    await self._emit_task_completed(request_id, False, None, 0, err)
                    self.active_tasks.pop(request_id, None)
                    continue

                # 获取文件信息
                file_name = None
                file_size = 0
                try:
                    if getattr(msg, 'file', None):
                        file_size = msg.file.size or 0
                        file_name = msg.file.name
                except Exception:
                    pass
                if not file_name:
                    file_name = os.path.basename(request.save_path or "") or "unknown"

                logger.info(f"[SmallBatch][W{worker_id}] 开始: {file_name} ({file_size / (1024*1024):.1f}MB)")
                await self._emit_task_started(request_id, file_name, file_size)

                # 2. 进度回调（带节流）
                loop = asyncio.get_running_loop()
                last_emit = [0.0]

                def on_progress(current, total, _rid=request_id, _le=last_emit):
                    now = time.time()
                    if now - _le[0] < 0.3:
                        return
                    _le[0] = now
                    pct = (current / total * 100) if total > 0 else 0
                    try:
                        loop.create_task(self._emit_task_progress(_rid, current, total, pct))
                    except Exception:
                        pass

                # 3. download_media 直接下载完整文件（使用主客户端）
                result_path = await self.client.download_media(
                    msg,
                    file=request.save_path,
                    progress_callback=on_progress
                )

                elapsed = time.time() - download_start

                if result_path and os.path.exists(result_path):
                    actual_size = os.path.getsize(result_path)
                    speed = (actual_size / (1024 ** 2)) / elapsed if elapsed > 0 else 0
                    logger.info(f"[SmallBatch][W{worker_id}] {file_name} | {actual_size/(1024*1024):.1f}MB | {speed:.1f}MB/s | {elapsed:.1f}s")
                    logger.info(f"[SmallBatch][W{worker_id}] {file_name} ({actual_size:,} bytes, {speed:.1f}MB/s)")
                    await self._emit_task_completed(request_id, True, result_path, actual_size, None)
                else:
                    err = "download_media returned None"
                    logger.error(f"[SmallBatch][W{worker_id}] {file_name}: {err}")
                    logger.error(f"[SmallBatch][W{worker_id}] {err}: request_id={request_id}")
                    await self._emit_task_completed(request_id, False, None, 0, err)

            except BaseException as e:
                logger.error(f"[SmallBatch][W{worker_id}] {request_id}: {e}", exc_info=True)
                logger.error(f"[SmallBatch][W{worker_id}] 异常: {e}")
                try:
                    await self._emit_task_completed(request_id, False, None, 0, str(e))
                except Exception as emit_err:
                    logger.warning(f"[SmallBatch][W{worker_id}] 完成事件发送失败: {emit_err}")
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
            finally:
                self.active_tasks.pop(request_id, None)

        if not self.active_tasks:
            self.state = DaemonState.IDLE

    async def _handle_download_request_workerpool(self, request: 'DownloadRequestV2'):
        """
        WorkerPool 并发下载路径

        流程：
        1. 获取消息和文件信息
        2. 初始化连接池（复用主客户端的session创建多个客户端）
        3. 创建 WorkerPool，用 12 个 Worker 并发下载
        4. 实时打印进度到终端
        """
        from download_pool import WorkerPool, ClientPool, Part
        from download_config import get_config_value
        from sparse_file_utils import create_sparse_file, calculate_parts, verify_file_integrity
        from telethon.tl.types import InputDocumentFileLocation
        from telethon import TelegramClient

        request_id = request.request_id

        # [FIX-PATH-SECURITY] 验证保存路径，防止路径穿越
        try:
            request.save_path = _validate_save_path(request.save_path)
        except ValueError as path_err:
            logger.error(f"[Download][WorkerPool] 路径验证失败: {path_err}")
            await self._emit_task_completed(request_id, False, None, 0, str(path_err))
            return

        logger.info(f"[Download][WorkerPool] ========== 开始 ==========")
        logger.info(f"[Download][WorkerPool] request_id={request_id}")
        logger.info(f"[Download][WorkerPool] chat_id={request.chat_id}, msg_id={request.msg_id}")
        logger.info(f"[Download][WorkerPool] save_path=...{os.path.basename(request.save_path)}")

        self.active_tasks[request_id] = {
            "chat_id": request.chat_id,
            "msg_id": request.msg_id,
            "save_path": request.save_path,
            "worker_pool": None,   # [CROSS-SESSION-RESUME] 供关闭存盘读取
            "file_size": 0,
            "file_name": "",
            "part_size": 0,
            "total_parts": 0,
            "temp_path": "",
        }
        # [FIX-CANCEL-2026-02-16] 创建取消事件
        self._cancel_events[request_id] = asyncio.Event()

        if not self.client:
            err = "TelegramClient not initialized"
            logger.error(f"[Download][WorkerPool] {err}")
            await self._emit_task_completed(request_id, False, None, 0, err)
            self.active_tasks.pop(request_id, None)
            return

        if self.state != DaemonState.DOWNLOADING:
            self.state = DaemonState.DOWNLOADING

        # Step 1: 获取消息
        try:
            msg = await self.client.get_messages(request.chat_id, ids=request.msg_id)
            if isinstance(msg, list):
                msg = msg[0] if msg else None
        except Exception as e:
            err = f"get_messages failed: {e}"
            logger.error(f"[Download][WorkerPool] {err}", exc_info=True)
            await self._emit_task_completed(request_id, False, None, 0, err)
            self.active_tasks.pop(request_id, None)
            if not self.active_tasks:
                self.state = DaemonState.IDLE
            return

        if not msg or not getattr(msg, "media", None):
            err = "Message not found or has no media"
            logger.warning(f"[Download][WorkerPool] {err}")
            await self._emit_task_completed(request_id, False, None, 0, err)
            self.active_tasks.pop(request_id, None)
            if not self.active_tasks:
                self.state = DaemonState.IDLE
            return

        # 获取文件信息
        expected_size = 0
        file_name = None
        try:
            if getattr(msg, "file", None):
                expected_size = msg.file.size or 0
                file_name = msg.file.name
        except Exception:
            expected_size = 0
            file_name = None

        if not file_name:
            file_name = os.path.basename(request.save_path or "") or "unknown"

        logger.info(f"[Download][WorkerPool] 文件: {file_name}")
        logger.info(f"[Download][WorkerPool] 大小: {expected_size:,} bytes ({expected_size / (1024**3):.2f} GB)")

        # 发送 TaskStartedEvent
        try:
            await self._emit_task_started(request_id, file_name, expected_size)
        except Exception as e:
            logger.warning(f"[Download][WorkerPool] TaskStartedEvent failed: {e}")

        download_start_time = time.time()

        # Step 2: 获取文件位置 + DC 分流判断
        try:
            document = msg.document
            if not document:
                raise ValueError("消息不包含文档")

            file_location = InputDocumentFileLocation(
                id=document.id,
                access_hash=document.access_hash,
                file_reference=document.file_reference,
                thumb_size=''
            )

            # [NEW-AUTO-DC-ROUTING-2026-02-06] 自动 DC 分流：同DC用主池，跨DC用DC专用池
            file_dc = document.dc_id
            account_dc = self.client.session.dc_id

            # 分流判断
            if account_dc == file_dc:
                use_dc_pool = False
                logger.info(f"[Download][WorkerPool] [DC-ROUTING] 同 DC 下载（DC{account_dc}），使用主连接池")
                logger.info(f"[Download][WorkerPool] 同 DC 下载: 账号 DC{account_dc} = 文件 DC{file_dc}")
            else:
                use_dc_pool = True
                logger.warning(f"[Download][WorkerPool] [DC-ROUTING] 跨 DC 下载（账号 DC{account_dc} -> 文件 DC{file_dc}），使用 DC 专用连接池")
                logger.info(f"[Download][WorkerPool] 跨 DC 下载: 账号 DC{account_dc} → 文件 DC{file_dc}")

        except Exception as e:
            err = f"获取文件位置失败: {e}"
            logger.error(f"[Download][WorkerPool] {err}", exc_info=True)
            await self._emit_task_completed(request_id, False, None, 0, err)
            self.active_tasks.pop(request_id, None)
            if not self.active_tasks:
                self.state = DaemonState.IDLE
            return

        # Step 3: 续传检测 + 创建稀疏文件
        file_path = request.save_path
        # [TEMP-PART-2026-02-01] Use .part temp file to prevent ghost files.
        final_path = file_path
        temp_path = f"{file_path}.part"
        file_path = temp_path
        logger.info(f"[Download][WorkerPool] temp_path={temp_path}")

        # [CROSS-SESSION-RESUME-2026-02-16] 续传检测
        resume_info = None  # 非 None 时表示续传
        if self.checkpoint_manager and expected_size > 0:
            try:
                # ====== [DIAG-RESUME-2026-02-16] 续传检测诊断 START ======
                logger.debug(f"[RESUME] ============================================")
                logger.debug(f"[RESUME] 开始续传检测:")
                logger.debug(f"[RESUME]   account_id={self.account_id}")
                logger.debug(f"[RESUME]   chat_id={request.chat_id}, msg_id={request.msg_id}")
                logger.debug(f"[RESUME]   request_id={request_id}")
                logger.debug(f"[RESUME]   expected_size={expected_size:,} bytes")
                logger.debug(f"[RESUME]   temp_path={temp_path}")
                # ====== [DIAG-RESUME-2026-02-16] END ======
                checkpoint = await self.checkpoint_manager.find_by_message(
                    account_id=self.account_id,
                    chat_id=request.chat_id,
                    msg_id=request.msg_id
                )
                part_file_exists = os.path.exists(temp_path)

                # ====== [DIAG-RESUME-2026-02-16] checkpoint查询结果 ======
                logger.debug(f"[RESUME] checkpoint查询结果: {'找到' if checkpoint else '未找到'}")
                if checkpoint:
                    logger.debug(f"[RESUME]   checkpoint.request_id={checkpoint.request_id}")
                    logger.debug(f"[RESUME]   checkpoint.file_size={checkpoint.file_size:,}")
                    logger.debug(f"[RESUME]   checkpoint.state={checkpoint.state}")
                    logger.debug(f"[RESUME]   checkpoint.downloaded={checkpoint.downloaded:,}")
                    logger.debug(f"[RESUME]   checkpoint.percentage={checkpoint.percentage:.1f}%")
                    logger.debug(f"[RESUME]   checkpoint.extra_data长度={len(str(checkpoint.extra_data)) if checkpoint.extra_data else 0}")
                else:
                    logger.debug(f"[RESUME]   数据库中无此(account_id, chat_id, msg_id)的checkpoint记录")
                    logger.debug(f"[RESUME]   可能原因: 上次取消时未保存checkpoint / checkpoint已过期(7天) / 从未下载过")
                logger.debug(f"[RESUME] .part文件存在: {part_file_exists}")
                if part_file_exists:
                    _diag_part_actual_size = os.path.getsize(temp_path)
                    logger.debug(f"[RESUME]   .part文件大小: {_diag_part_actual_size:,} bytes")
                    logger.debug(f"[RESUME]   期望大小: {expected_size:,} bytes")
                    logger.debug(f"[RESUME]   大小匹配: {_diag_part_actual_size == expected_size}")
                # ====== [DIAG-RESUME-2026-02-16] END ======

                if checkpoint and part_file_exists:
                    parsed = self.checkpoint_manager.parse_completed_parts(checkpoint)
                    # ====== [DIAG-RESUME-2026-02-16] 解析结果诊断 ======
                    logger.debug(f"[RESUME] parse_completed_parts 结果: {'成功' if parsed else '失败(None)'}")
                    if parsed:
                        logger.debug(f"[RESUME]   parsed.part_size={parsed.get('part_size', 0)}")
                        logger.debug(f"[RESUME]   parsed.total_parts={parsed.get('total_parts', 0)}")
                        logger.debug(f"[RESUME]   parsed.completed_count={len(parsed.get('completed_part_indices', []))}")
                        logger.debug(f"[RESUME]   parsed.checkpoint_type={parsed.get('checkpoint_type', 'unknown')}")
                    # ====== [DIAG-RESUME-2026-02-16] END ======
                    if parsed:
                        saved_part_size = parsed.get("part_size", 0)
                        current_part_size = get_config_value("worker_pool_part_size", 1024 * 1024)
                        actual_part_file_size = os.path.getsize(temp_path)

                        # ====== [DIAG-RESUME-2026-02-16] 续传条件验证 ======
                        _diag_cond1 = checkpoint.file_size == expected_size
                        _diag_cond2 = saved_part_size == current_part_size
                        _diag_cond3 = actual_part_file_size == expected_size
                        logger.debug(f"[RESUME] 续传条件验证:")
                        logger.debug(f"[RESUME]   条件1 file_size匹配: {_diag_cond1} (checkpoint={checkpoint.file_size:,} vs expected={expected_size:,})")
                        logger.debug(f"[RESUME]   条件2 part_size匹配: {_diag_cond2} (saved={saved_part_size:,} vs current={current_part_size:,})")
                        logger.debug(f"[RESUME]   条件3 .part文件大小==expected: {_diag_cond3} (actual={actual_part_file_size:,} vs expected={expected_size:,})")
                        if not _diag_cond3:
                            logger.debug(f"[RESUME]   条件3失败! .part文件大小与预期不符")
                            logger.debug(f"[RESUME]   可能原因：上次取消时.part文件尚未创建为完整的稀疏文件")
                        _diag_all_pass = _diag_cond1 and _diag_cond2 and _diag_cond3
                        logger.debug(f"[RESUME] 全部通过: {_diag_all_pass}")
                        if _diag_all_pass:
                            logger.debug(f"[RESUME] 将启用续传模式")
                        else:
                            logger.debug(f"[RESUME] 续传条件不满足，将从头下载!")
                        logger.debug(f"[RESUME] ============================================")
                        # ====== [DIAG-RESUME-2026-02-16] END ======

                        if (checkpoint.file_size == expected_size
                                and saved_part_size == current_part_size
                                and actual_part_file_size == expected_size):
                            completed_indices = set(parsed["completed_part_indices"])
                            completed_bytes = min(len(completed_indices) * saved_part_size, expected_size)
                            resume_info = {
                                "completed_indices": completed_indices,
                                "completed_bytes": completed_bytes,
                                "total_parts": parsed.get("total_parts", 0),
                                "part_size": saved_part_size,
                                "old_request_id": checkpoint.request_id,
                            }
                            pct = (completed_bytes / expected_size * 100)
                            logger.info(f"[Download][WorkerPool] [RESUME] 检测到续传: {pct:.1f}% ({len(completed_indices)}/{resume_info['total_parts']} parts)")
                            logger.info(f"[RESUME] 跨会话续传: {len(completed_indices)}/{resume_info['total_parts']} parts ({pct:.1f}%)")
                        else:
                            logger.info(f"[Download][WorkerPool] [RESUME] 参数不匹配，从头开始")
                            try:
                                os.remove(temp_path)
                            except OSError:
                                pass
                            await self.checkpoint_manager.clear_failed_parts(checkpoint.request_id)
                    else:
                        logger.info(f"[Download][WorkerPool] [RESUME] checkpoint 无法解析，从头开始")
                        try:
                            os.remove(temp_path)
                        except OSError:
                            pass
                        await self.checkpoint_manager.clear_failed_parts(checkpoint.request_id)
                elif checkpoint and not part_file_exists:
                    logger.info(f"[Download][WorkerPool] [RESUME] .part 文件已删除，清理 checkpoint")
                    await self.checkpoint_manager.clear_failed_parts(checkpoint.request_id)
                elif not checkpoint and part_file_exists:
                    logger.info(f"[Download][WorkerPool] [RESUME] 孤儿 .part 文件，删除")
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass
            except Exception as resume_err:
                logger.warning(f"[Download][WorkerPool] [RESUME] 检测异常: {resume_err}，从头开始")
                logger.error(f"[RESUME] Detection error: {resume_err}", exc_info=True)
                resume_info = None

        # 创建稀疏文件（续传时跳过）
        if resume_info is None:
            try:
                create_sparse_file(file_path, expected_size)
            except Exception as e:
                err = f"创建稀疏文件失败: {e}"
                logger.error(f"[Download][WorkerPool] {err}", exc_info=True)

                # [FIX-SCENE5-2026-02-03] 删除失败的 .part 文件
                try:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        logger.info(f"[Download][WorkerPool] 已删除失败的 .part 文件: {temp_path}")
                except Exception as remove_err:
                    logger.error(f"[Download][WorkerPool] 删除 .part 文件失败: {remove_err}")

                await self._emit_task_completed(request_id, False, None, 0, err)
                self.active_tasks.pop(request_id, None)
                if not self.active_tasks:
                    self.state = DaemonState.IDLE
                return
        else:
            logger.info(f"[Download][WorkerPool] [RESUME] 复用已有 .part 文件")

        # Step 4: 初始化并发客户端池
        pool_size = get_config_value("connection_pool_size", 4)

        try:
            clients = await self._create_client_pool(pool_size)
            client_pool = ClientPool(clients)
            logger.info(f"[Download][WorkerPool] 连接池就绪: {client_pool.size} 个活跃连接")
        except Exception as e:
            err = f"连接池初始化失败: {e}"
            logger.error(f"[Download][WorkerPool] {err}", exc_info=True)
            # 降级到单连接
            logger.warning(f"[Download][WorkerPool] 降级到单连接模式")
            clients = [self.client]
            client_pool = ClientPool(clients)

        # Step 5: 计算 Parts 并创建 WorkerPool
        part_size = get_config_value("worker_pool_part_size", 1024 * 1024)  # 1MB
        part_tuples = calculate_parts(expected_size, part_size)
        all_parts = [
            Part(index=i, offset=o, length=l, is_last=(i == len(part_tuples) - 1))
            for i, (o, l) in enumerate(part_tuples)
        ]
        total_parts_count = len(all_parts)

        # [CROSS-SESSION-RESUME-2026-02-16] 过滤已完成 Part
        # [FIX-RESUME-INDEX-2026-02-16] 索引重映射：WorkerPool 用 part.index 作为
        # results 数组下标，results 长度 = len(parts)。如果 parts 保留原始索引
        # (如 103-176)，会导致 results[103] 越界。必须重映射为 0-based 连续索引，
        # 同时保持 offset/length 不变（写入位置由 offset 决定，与 index 无关）。
        if resume_info is not None:
            remaining = [p for p in all_parts if p.index not in resume_info["completed_indices"]]
            parts = [
                Part(index=new_i, offset=p.offset, length=p.length, is_last=p.is_last)
                for new_i, p in enumerate(remaining)
            ]
            # [FIX-RESUME-INDEX-MAP-2026-02-16] 构建 remapped→original 索引映射表
            # get_completed_indices() 返回 remapped 索引 (0-73)，保存 checkpoint 时需要
            # 转换回原始索引 (103-176)，否则与 resume_info["completed_indices"] (0-102) 合并时
            # 发生索引碰撞，导致新下载的 Part 永远无法正确记录到 checkpoint。
            resume_index_map = {new_i: remaining[new_i].index for new_i in range(len(remaining))}
            resume_info["index_map"] = resume_index_map

            logger.info(f"[Download][WorkerPool] [RESUME] 过滤: 总 {total_parts_count}, 已完成 {total_parts_count - len(parts)}, 剩余 {len(parts)}")
            logger.info(f"[Download][WorkerPool] [RESUME] [INDEX-REMAP] 索引已重映射: 原始[{remaining[0].index}..{remaining[-1].index}] -> 新[0..{len(parts)-1}]" if parts else "")

            if len(parts) == 0:
                # 所有 Part 已完成 → 直接验证 + rename
                logger.info(f"[Download][WorkerPool] [RESUME] 所有 Part 已完成，跳过下载，直接验证")
                # 清理旧 checkpoint
                try:
                    if self.checkpoint_manager:
                        await self.checkpoint_manager.clear_failed_parts(resume_info["old_request_id"])
                except Exception:
                    pass
                # 跳到 Step 7
                elapsed = time.time() - download_start_time
                ok, actual_size, err = self._validate_downloaded_file(file_path, expected_size)
                if ok:
                    try:
                        os.replace(file_path, final_path)
                        file_path = final_path
                    except OSError as promote_err:
                        err = f"promote failed: {promote_err}"
                        await self._emit_task_completed(request_id, False, final_path, actual_size, err)
                        self.active_tasks.pop(request_id, None)
                        if not self.active_tasks:
                            self.state = DaemonState.IDLE
                        return
                    await self._emit_task_completed(request_id, True, file_path, actual_size, None)
                else:
                    await self._emit_task_completed(request_id, False, file_path, actual_size, err)
                self.active_tasks.pop(request_id, None)
                if not self.active_tasks:
                    self.state = DaemonState.IDLE
                return
        else:
            parts = all_parts

        # [TDL-4-WORKERS-2026-02-10] Worker 数量独立于连接数（对齐 TDL threads=4）
        num_workers = min(get_config_value("download_threads", 4), len(parts))
        logger.info(f"[Download][WorkerPool] Workers={num_workers} (clients={client_pool.size})")
        max_retries = get_config_value("worker_pool_max_retries", 3)

        worker_pool = WorkerPool(
            client_pool=client_pool,
            file_path=file_path,
            file_location=file_location,
            num_workers=num_workers,
            max_retries=max_retries,
            main_client_for_export=self.client,
            chat_id=request.chat_id,  # [P2-FILE-REF-2026-02-03] 用于刷新 file_reference
            msg_id=request.msg_id,    # [P2-FILE-REF-2026-02-03] 用于刷新 file_reference
            file_dc=file_dc,          # [NEW-AUTO-DC-ROUTING-2026-02-06] 文件所在 DC
            account_dc=account_dc,    # [NEW-AUTO-DC-ROUTING-2026-02-06] 账号所在 DC
            use_dc_pool=use_dc_pool,  # [NEW-AUTO-DC-ROUTING-2026-02-06] 是否使用 DC 专用连接池
        )

        # [CROSS-SESSION-RESUME-2026-02-16] 更新 active_tasks 供关闭存盘使用
        if request_id in self.active_tasks:
            self.active_tasks[request_id].update({
                "worker_pool": worker_pool,
                "file_size": expected_size,
                "file_name": file_name,
                "part_size": part_size,
                "total_parts": total_parts_count,
                "temp_path": temp_path,
                # [FIX-RESUME-INDEX-MAP-2026-02-16] 存储索引映射供 cancel/shutdown 使用
                "resume_index_map": resume_info.get("index_map") if resume_info else None,
                "resume_completed_indices": list(resume_info["completed_indices"]) if resume_info else None,
            })

        # Step 6: 开始并发下载（带进度回调 -> IPC事件）
        loop = asyncio.get_running_loop()
        # [CROSS-SESSION-RESUME-2026-02-16] 续传时从已完成字节开始计算进度
        resume_bytes = resume_info["completed_bytes"] if resume_info else 0
        progress_state = {"last_emit": 0.0, "downloaded": resume_bytes}
        progress_throttle = 0.3  # 事件节流

        def on_pool_progress(bytes_delta):
            progress_state["downloaded"] += bytes_delta
            now = time.time()
            if now - progress_state["last_emit"] < progress_throttle:
                return
            progress_state["last_emit"] = now
            current = progress_state["downloaded"]
            pct = (current / expected_size * 100) if expected_size > 0 else 0
            try:
                loop.create_task(self._emit_task_progress(request_id, current, expected_size, pct))
            except Exception:
                pass

        # [CROSS-SESSION-RESUME-2026-02-16] 续传时立即 emit 一次进度，让 UI 显示正确起始百分比
        if resume_info:
            initial_pct = (resume_bytes / expected_size * 100) if expected_size > 0 else 0
            try:
                loop.create_task(self._emit_task_progress(request_id, resume_bytes, expected_size, initial_pct))
            except Exception:
                pass

        # [CROSS-SESSION-RESUME-2026-02-16] 周期存盘任务（每 30 秒保存已完成 Part 索引）
        _periodic_ckpt_task = None
        if self.checkpoint_manager:
            async def _periodic_checkpoint_saver():
                try:
                    while True:
                        await asyncio.sleep(30)
                        try:
                            completed = worker_pool.get_completed_indices()
                            # [FIX-RESUME-INDEX-MAP-2026-02-16] 续传时先将 remapped 索引转换回原始索引
                            if resume_info:
                                idx_map = resume_info.get("index_map")
                                if idx_map:
                                    completed = [idx_map.get(i, i) for i in completed]
                                completed_set = set(completed) | resume_info["completed_indices"]
                                completed = sorted(completed_set)
                            if completed:
                                await self.checkpoint_manager.save_download_state(
                                    request_id=request_id,
                                    account_id=self.account_id,
                                    chat_id=request.chat_id,
                                    msg_id=request.msg_id,
                                    file_path=temp_path,
                                    file_name=file_name,
                                    file_size=expected_size,
                                    completed_part_indices=completed,
                                    part_size=part_size,
                                    total_parts=total_parts_count,
                                )
                        except Exception as save_err:
                            logger.warning(f"[PERIODIC-CHECKPOINT] 保存失败: {save_err}")
                except asyncio.CancelledError:
                    # 取消时做最后一次保存
                    try:
                        completed = worker_pool.get_completed_indices()
                        # [FIX-RESUME-INDEX-MAP-2026-02-16] 同上，remapped→original
                        if resume_info:
                            idx_map = resume_info.get("index_map")
                            if idx_map:
                                completed = [idx_map.get(i, i) for i in completed]
                            completed_set = set(completed) | resume_info["completed_indices"]
                            completed = sorted(completed_set)
                        if completed:
                            await self.checkpoint_manager.save_download_state(
                                request_id=request_id,
                                account_id=self.account_id,
                                chat_id=request.chat_id,
                                msg_id=request.msg_id,
                                file_path=temp_path,
                                file_name=file_name,
                                file_size=expected_size,
                                completed_part_indices=completed,
                                part_size=part_size,
                                total_parts=total_parts_count,
                            )
                    except Exception:
                        pass

            _periodic_ckpt_task = asyncio.create_task(_periodic_checkpoint_saver())
            # 清理续传前的旧 checkpoint（新 request_id 会覆盖）
            if resume_info:
                try:
                    await self.checkpoint_manager.clear_failed_parts(resume_info["old_request_id"])
                except Exception:
                    pass

        try:
            # 接收 Part 结果列表
            results = await worker_pool.download_all(parts, progress_callback=on_pool_progress)

            # [FIX-CANCEL-2026-02-16] 检查是否被用户取消
            if worker_pool.is_cancelled():
                logger.info(f"[Download][WorkerPool] [CANCEL] 下载被用户取消，跳过后续处理")
                logger.info(f"[Download][WorkerPool] [CANCEL] request_id={request_id} 被用户取消")
                # 发送取消完成事件
                await self._emit_task_completed(
                    request_id, False, file_path, 0,
                    error_message="用户取消"
                )
                self.active_tasks.pop(request_id, None)
                self._cancel_events.pop(request_id, None)
                if not self.active_tasks:
                    self.state = DaemonState.IDLE
                await self._cleanup_client_pool(clients)
                return

            # 检查是否全部成功
            success = all(results)

            if not success:
                # 有失败 Part，识别失败索引
                failed_indices = [i for i, r in enumerate(results) if not r]
                logger.warning(f"[Download][WorkerPool] [CHECKPOINT] 首次下载有 {len(failed_indices)} 个 Part 失败")
                logger.warning(f"[Download][WorkerPool] [CHECKPOINT] 失败 Part 索引: {failed_indices[:50]}{'...' if len(failed_indices) > 50 else ''}")
                logger.warning(f"[CHECKPOINT] 首次下载 {len(failed_indices)}/{len(parts)} Part 失败（{len(failed_indices)/len(parts)*100:.1f}%），触发 Part 级重试")

                # 保存失败 Part 索引到 checkpoint
                try:
                    # [FIX-CHECKPOINT-SAVE-2026-02-05] 持久化失败 Part 信息到 SQLite
                    if self.checkpoint_manager:
                        await self.checkpoint_manager.save_failed_parts(
                            request_id=request_id,
                            account_id=self.account_id,
                            chat_id=request.chat_id,
                            msg_id=request.msg_id,
                            file_path=file_path,
                            file_name=file_name,
                            file_size=expected_size,
                            failed_part_indices=failed_indices,
                            part_size=part_size,
                            total_parts=len(parts),
                            retry_count=0
                        )
                        logger.info(f"[Download][WorkerPool] [CHECKPOINT] 失败 Part 信息已持久化到数据库")
                        logger.info(f"[CHECKPOINT] 已持久化 {len(failed_indices)} 个失败 Part 到 SQLite")
                    else:
                        logger.warning(f"[Download][WorkerPool] [CHECKPOINT] CheckpointManager 未初始化，跳过持久化")
                        logger.warning(f"[CHECKPOINT] CheckpointManager 未初始化，跳过持久化")
                except Exception as checkpoint_err:
                    logger.warning(f"[Download][WorkerPool] [CHECKPOINT] 保存 checkpoint 失败: {checkpoint_err}")
                    logger.error(f"[Download][WorkerPool] [CHECKPOINT] 保存失败: {checkpoint_err}", exc_info=True)

                # Part 级重试逻辑（最多2次重试）
                max_retries = 2
                logger.info(f"[CHECKPOINT] 开始 Part 级重试（最多 {max_retries} 次）")
                for retry_attempt in range(max_retries):
                    logger.info(f"[Download][WorkerPool] [CHECKPOINT] 开始第 {retry_attempt + 1}/{max_retries} 次 Part 重试...")
                    logger.info(f"[Download][WorkerPool] [CHECKPOINT] 待重试 Part 数量: {len(failed_indices)}")

                    # [FIX-CHECKPOINT-INCREMENT-2026-02-05] 更新重试计数
                    try:
                        if self.checkpoint_manager and retry_attempt > 0:
                            new_retry_count = await self.checkpoint_manager.increment_retry_count(request_id)
                            logger.info(f"[Download][WorkerPool] [CHECKPOINT] 重试计数已更新: {new_retry_count}")
                    except Exception as inc_err:
                        logger.warning(f"[Download][WorkerPool] [CHECKPOINT] 更新重试计数失败: {inc_err}")

                    # [FIX-INDEX-REMAP-2026-02-05] 索引重映射：避免 retry_parts 保持原始索引导致越界
                    from dataclasses import dataclass
                    retry_parts = []
                    index_mapping = {}  # new_index -> original_index

                    for new_idx, orig_idx in enumerate(failed_indices):
                        orig_part = parts[orig_idx]
                        # 创建新 Part，index 重映射为连续值
                        remapped_part = Part(
                            index=new_idx,           # 重映射索引：0, 1, 2, ...
                            offset=orig_part.offset, # 保持原偏移（写入位置不变）
                            length=orig_part.length, # 保持原长度
                            is_last=orig_part.is_last
                        )
                        retry_parts.append(remapped_part)
                        index_mapping[new_idx] = orig_idx

                    logger.info(f"[Download][WorkerPool] [CHECKPOINT] [INDEX-REMAP] 索引映射: {len(index_mapping)} 个 Part")
                    logger.info(f"[Download][WorkerPool] [CHECKPOINT] [INDEX-REMAP] 映射示例: {list(index_mapping.items())[:5]}...")
                    logger.info(f"[CHECKPOINT][INDEX-REMAP] 重试 {len(index_mapping)} 个失败 Part（索引重映射已修复越界Bug）")

                    try:
                        # 重试下载（使用重映射后的 Part 列表）
                        retry_results = await worker_pool.download_all(retry_parts, progress_callback=on_pool_progress)

                        # [FIX-INDEX-REMAP-2026-02-05] 使用映射表更新总结果
                        # [DISABLED-BUG-INDEX-2026-02-05] 原代码：for i, retry_result in enumerate(retry_results):
                        # [DISABLED-BUG-INDEX-2026-02-05]            original_index = failed_indices[i]
                        # [DISABLED-BUG-INDEX-2026-02-05]            results[original_index] = retry_result
                        success_remap_count = 0
                        fail_remap_count = 0
                        for new_idx, retry_result in enumerate(retry_results):
                            original_index = index_mapping[new_idx]
                            results[original_index] = retry_result
                            if retry_result:
                                logger.info(f"[Download][WorkerPool] [CHECKPOINT] [INDEX-REMAP] Part[new={new_idx}->orig={original_index}] 恢复成功")
                                success_remap_count += 1
                            else:
                                logger.warning(f"[Download][WorkerPool] [CHECKPOINT] [INDEX-REMAP] Part[new={new_idx}->orig={original_index}] 仍失败")
                                fail_remap_count += 1

                        # [LOG-OPTIMIZE-2026-02-05] 仅输出汇总结论到日志文件
                        if fail_remap_count > 0:
                            logger.warning(f"[CHECKPOINT][INDEX-REMAP] 重试结果: 成功 {success_remap_count}，失败 {fail_remap_count}")
                        else:
                            logger.info(f"[CHECKPOINT][INDEX-REMAP] 重试结果: 全部 {success_remap_count} 个 Part 恢复成功")

                        # 重新检查失败 Part
                        new_failed_indices = [i for i, r in enumerate(results) if not r]

                        if not new_failed_indices:
                            # 全部成功！
                            logger.info(f"[Download][WorkerPool] [CHECKPOINT] 重试成功！全部 Part 已下载")
                            success = True
                            logger.info(f"[CHECKPOINT] Part 级重试成功（第 {retry_attempt + 1} 次），全部 Part 已恢复")

                            # [FIX-CHECKPOINT-CLEAR-RETRY-2026-02-05] 重试成功后清除 checkpoint
                            try:
                                if self.checkpoint_manager:
                                    await self.checkpoint_manager.clear_failed_parts(request_id)
                                    logger.info(f"[Download][WorkerPool] [CHECKPOINT] 已清除 checkpoint（重试成功）")
                            except Exception as clear_err:
                                logger.warning(f"[Download][WorkerPool] [CHECKPOINT] 清除 checkpoint 失败: {clear_err}")

                            break
                        else:
                            # 仍有失败
                            recovered = len(failed_indices) - len(new_failed_indices)
                            logger.info(f"[Download][WorkerPool] [CHECKPOINT] 第 {retry_attempt + 1} 次重试: 恢复 {recovered} 个，仍有 {len(new_failed_indices)} 个失败")
                            failed_indices = new_failed_indices

                            if retry_attempt == max_retries - 1:
                                # 最后一次重试仍有失败
                                logger.error(f"[Download][WorkerPool] [CHECKPOINT] 重试 {max_retries} 次后仍有 {len(failed_indices)} 个 Part 失败")
                                success = False
                                logger.error(f"[CHECKPOINT] Part 级重试失败（{max_retries} 次），仍有 {len(failed_indices)} Part 失败")

                    except Exception as retry_err:
                        logger.error(f"[Download][WorkerPool] [CHECKPOINT] 第 {retry_attempt + 1} 次重试异常: {retry_err}")
                        logger.error(f"[CHECKPOINT] Part 级重试异常（第 {retry_attempt + 1} 次）: {retry_err}", exc_info=True)
                        # 继续下一次重试
                        continue

        except Exception as e:
            err = f"WorkerPool 下载失败: {e}"
            logger.error(f"[Download][WorkerPool] {err}", exc_info=True)
            await self._emit_task_completed(request_id, False, file_path, 0, err)
            self.active_tasks.pop(request_id, None)
            self._cancel_events.pop(request_id, None)  # [FIX-CANCEL-2026-02-16]
            if not self.active_tasks:
                self.state = DaemonState.IDLE
            await self._cleanup_client_pool(clients)
            return
        finally:
            # [CROSS-SESSION-RESUME-2026-02-16] 取消周期存盘任务（cancel 触发最后一次保存）
            if _periodic_ckpt_task and not _periodic_ckpt_task.done():
                _periodic_ckpt_task.cancel()
                try:
                    await _periodic_ckpt_task
                except asyncio.CancelledError:
                    pass

        # Step 7: 验证文件
        elapsed = time.time() - download_start_time
        ok, actual_size, err = self._validate_downloaded_file(file_path, expected_size)

        if not ok:
            logger.error(f"[Download][WorkerPool] 验证失败: {err}")
            await self._emit_task_completed(request_id, False, file_path, actual_size, err)
        else:
            # [FIX-WINERROR32-2026-02-05] Windows 文件句柄释放 + 带重试的 promote
            import gc
            import platform

            # [FIX-WINERROR32-2026-02-05] Windows 特殊处理
            if platform.system() == 'Windows':
                # 强制 GC 回收 ShardWriter 等对象释放文件句柄
                gc.collect()
                logger.info(f"[Download][WorkerPool] [FIX-WINERROR32] Windows 检测: 执行 GC 释放文件句柄")

                # 短暂等待让 Windows 释放句柄
                await asyncio.sleep(0.5)
                logger.info(f"[Download][WorkerPool] [FIX-WINERROR32] 等待 0.5 秒后尝试 promote")
                logger.debug(f"[WINERROR32] Windows GC + 0.5s wait before promote")

            # [FIX-WINERROR32-2026-02-05] 带重试的 promote 逻辑
            promote_max_retries = 3
            promote_success = False
            promote_last_err = None

            for promote_attempt in range(promote_max_retries):
                try:
                    os.replace(file_path, final_path)
                    file_path = final_path
                    if promote_attempt == 0:
                        logger.info(f"[Download] promote ok: {file_path}")
                    else:
                        logger.info(f"[Download] promote ok（第 {promote_attempt + 1} 次重试）: {file_path}")
                    promote_success = True
                    if promote_attempt > 0:
                        logger.info(f"[Download][WorkerPool] [FIX-WINERROR32] 第 {promote_attempt + 1} 次重试 promote 成功")
                    break
                except OSError as promote_err:
                    promote_last_err = promote_err
                    # 检查是否为 WinError 32（文件被占用）
                    if hasattr(promote_err, 'winerror') and promote_err.winerror == 32:
                        logger.warning(f"[Download][WorkerPool] [FIX-WINERROR32] 第 {promote_attempt + 1}/{promote_max_retries} 次 promote 失败: 文件被占用")
                        logger.warning(f"[WINERROR32] promote 第 {promote_attempt + 1}/{promote_max_retries} 次失败（文件被占用）")
                        if promote_attempt < promote_max_retries - 1:
                            # 再次 GC 并等待更长时间
                            gc.collect()
                            wait_time = 1.0 * (promote_attempt + 1)  # 递增等待：1秒、2秒
                            logger.info(f"[Download][WorkerPool] [FIX-WINERROR32] 等待 {wait_time} 秒后重试...")
                            await asyncio.sleep(wait_time)
                    else:
                        # 非 WinError 32，直接退出重试循环
                        logger.error(f"[Download][WorkerPool] [FIX-WINERROR32] 非文件占用错误，停止重试: {promote_err}")
                        logger.error(f"[WINERROR32] promote 非文件占用错误: {promote_err}")
                        break

            # [FIX-WINERROR32-2026-02-05] 检查 promote 最终结果
            if not promote_success:
                err = f"promote failed after {promote_max_retries} retries: {promote_last_err}"
                logger.error(f"[Download] {err}")
                logger.error(f"[Download][WorkerPool] [FIX-WINERROR32] promote 最终失败: {err}")
                await self._emit_task_completed(request_id, False, final_path, actual_size, err)
                await self._cleanup_client_pool(clients)
                self.active_tasks.pop(request_id, None)
                self._cancel_events.pop(request_id, None)  # [FIX-CANCEL-2026-02-16]
                if not self.active_tasks:
                    self.state = DaemonState.IDLE
                return

            speed = (actual_size / (1024**2)) / elapsed if elapsed > 0 else 0
            logger.info(f"[Download][WorkerPool] 下载成功")
            logger.info(f"[Download][WorkerPool]   文件: {file_name}")
            logger.info(f"[Download][WorkerPool]   大小: {actual_size:,} bytes ({actual_size/(1024**3):.2f} GB)")
            logger.info(f"[Download][WorkerPool]   耗时: {elapsed:.2f}s")
            logger.info(f"[Download][WorkerPool]   平均速度: {speed:.2f} MB/s")
            logger.info(f"[Download][WorkerPool]   连接数: {client_pool.size}")
            logger.info(f"[Download] 完成: {file_name} | {actual_size/(1024**3):.2f}GB | {speed:.2f}MB/s | {elapsed:.1f}s")
            await self._emit_task_completed(request_id, True, file_path, actual_size, None)

            # [FIX-CHECKPOINT-CLEAR-2026-02-05] 下载成功时清除 checkpoint
            try:
                if self.checkpoint_manager:
                    await self.checkpoint_manager.clear_failed_parts(request_id)
                    logger.info(f"[Download][WorkerPool] [CHECKPOINT] 已清除 checkpoint（任务完成）")
            except Exception as clear_err:
                logger.warning(f"[CHECKPOINT] 清除失败: {clear_err}")

        # 清理连接池
        await self._cleanup_client_pool(clients)

        self.active_tasks.pop(request_id, None)
        self._cancel_events.pop(request_id, None)  # [FIX-CANCEL-2026-02-16] 清理取消事件
        if not self.active_tasks:
            self.state = DaemonState.IDLE

    async def _create_client_pool(self, pool_size: int) -> list:
        """创建并发客户端池"""
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from download_config import get_config_value
        

        API_ID = self.api_id
        API_HASH = self.api_hash

        # 获取主客户端的 StringSession
        string_session = StringSession.save(self.client.session)

        # 代理配置
        proxy_to_use = None
        try:
            from proxy_utils import get_proxy_for_telethon
            proxy_mode = get_config_value("proxy_mode", "auto")
            custom_proxy = get_config_value("custom_proxy", None)
            proxy_to_use, _ = get_proxy_for_telethon(
                mode=proxy_mode, custom_proxy=custom_proxy,
                fallback_to_direct=True, test_timeout=3.0,
                logger_func=lambda msg: logger.debug(f"[Pool][Proxy] {msg}"),
            )
        except Exception:
            pass

        clients = []
        logger.info(f"[Pool] 创建 {pool_size} 个并发连接（分批：每批4个并发）...")

        # [BATCH-CONNECT-2026-02-01] 分批创建，每批4个并发
        batch_size = 4
        for batch_start in range(0, pool_size, batch_size):
            batch_end = min(batch_start + batch_size, pool_size)
            batch_num = batch_start // batch_size + 1
            total_batches = (pool_size + batch_size - 1) // batch_size

            logger.info(f"[Pool] 批次 {batch_num}/{total_batches}: 并发创建连接 {batch_start+1}-{batch_end}...")

            # 创建当前批次的任务
            async def create_and_connect(index):
                try:
                    client = TelegramClient(
                        StringSession(string_session),
                        API_ID, API_HASH,
                        proxy=proxy_to_use,
                        flood_sleep_threshold=0,  # [FIX-FLOOD-2026-02-04] 禁用自动 sleep，让 FloodWait 抛异常
                    )
                    await client.connect()

                    if await client.is_user_authorized():
                        return client
                    else:
                        logger.warning(f"[Pool] 客户端 {index} 未授权")
                        await client.disconnect()
                        return None
                except Exception as e:
                    logger.warning(f"[Pool] 客户端 {index} 创建失败: {e}")
                    return None

            # [BATCH-CONNECT] 并发创建当前批次的4个连接（而不是串行）
            tasks = [create_and_connect(i) for i in range(batch_start, batch_end)]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # 收集成功的连接
            batch_success = 0
            for result in batch_results:
                if result and not isinstance(result, Exception):
                    clients.append(result)
                    batch_success += 1

            logger.info(f"[Pool] 批次 {batch_num}: {batch_success}/{batch_end - batch_start} 连接成功")

        if not clients:
            raise RuntimeError("无法创建任何并发连接")

        logger.info(f"[Pool] 连接池创建完成: {len(clients)}/{pool_size} 个活跃连接")
        for c in clients:
            if c not in self._tracked_clients:
                self._tracked_clients.append(c)
        return clients

    async def _cleanup_client_pool(self, clients: list):
        """清理连接池（包括主池连接 + 全局 DC 专用连接池）"""
        # 1. 断开主池连接
        for client in clients:
            if client != self.client:  # 不要关闭主客户端
                try:
                    await client.disconnect()
                except Exception:
                    pass

        # 2. [FIX-2026-02-14-ZOMBIE-DC] 清理全局 DC 专用连接池，防止僵尸连接
        #    销毁重建策略下，下载完成后不应有任何残留 DC 连接
        try:
            from download_pool import cleanup_global_dc_pools
            await cleanup_global_dc_pools()
        except Exception as dc_err:
            logger.warning(f"[Cleanup] DC 连接池清理失败（非致命）: {dc_err}")

    async def _send_event(self, message_type: 'MessageType', payload: Dict[str, Any]):
        """发送事件到主进程"""
        from download_ipc import IPCMessage
        import uuid
        import time

        message = IPCMessage(
            message_id=str(uuid.uuid4()),
            message_type=message_type,
            timestamp=time.time(),
            payload=payload
        )

        await self.ipc.send(message)

    # [REFRESH-DIALOGS-FIX-2026-02-02] 新增：发送刷新对话列表请求到主进程
    async def _send_refresh_dialogs_request(self, reason: str = ""):
        """
        发送刷新对话列表请求到主进程

        [ROOT-CAUSE] Daemon进程无法访问主进程的account_manager和db_service_ref
        [SOLUTION] 通过IPC通知主进程在UI线程执行refresh_dialogs_list()

        Args:
            reason: 刷新原因（用于日志追踪）
                - "sync_completed": 同步完成
                - "account_switched": 账号切换
                - "manual": 手动刷新
        """
        from download_protocol_v2 import RefreshDialogsRequest
        from download_ipc import MessageType
        import uuid

        request = RefreshDialogsRequest(
            request_id=str(uuid.uuid4()),
            account_id=self.account_id,
            reason=reason,
        )

        logger.info(
            f"[RefreshDialogs] 发送刷新对话列表请求到主进程 "
            f"(account_id={self.account_id}, reason={reason})"
        )

        await self._send_event(MessageType.REFRESH_DIALOGS_REQUEST, request.to_dict())

    async def _handle_heartbeat_request(self, message: 'IPCMessage'):
        """处理心跳请求"""
        from download_protocol_v2 import HeartbeatResponse
        from download_ipc import MessageType
        import psutil

        # 获取内存占用
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024

        # 计算运行时间
        uptime_seconds = int(time.time() - self.start_time)

        # 构造响应
        response = HeartbeatResponse(
            request_id=message.payload.get('request_id', ''),
            memory_mb=memory_mb,
            uptime_seconds=uptime_seconds
        )

        await self._send_event(MessageType.HEARTBEAT_RESPONSE, response.to_dict())

        logger.debug(
            f"[Heartbeat] 响应心跳 "
            f"(memory={memory_mb:.1f}MB, uptime={uptime_seconds}s)"
        )

    # [FIX-CANCEL-2026-02-16] 处理取消下载请求
    async def _handle_cancel_request(self, message: 'IPCMessage'):
        """处理取消下载请求

        流程：
        1. 解析 target_request_id
        2. 检查任务是否存在于 active_tasks
        3. 设置取消事件（通知 WorkerPool 停止）
        4. 保存 checkpoint（断点续传数据）
        5. 发送 TaskCompletedEvent(success=False, error="用户取消")
        """
        from download_protocol_v2 import CancelDownloadRequest

        payload = message.payload
        request = CancelDownloadRequest.from_dict(payload)
        target_rid = request.target_request_id

        logger.info(f"[Cancel] 收到取消请求: target_request_id={target_rid}")
        logger.info(f"[CANCEL-DAEMON] 收到取消请求: target={target_rid[:8]}...")

        # 查找目标任务
        task_info = self.active_tasks.get(target_rid)
        if not task_info:
            logger.warning(f"[Cancel] 任务不存在: {target_rid}")
            logger.warning(f"[CANCEL-DAEMON] 任务 {target_rid[:8]}... 不在 active_tasks 中（可能已完成）")
            return

        logger.info(f"[CANCEL-DAEMON] 找到任务: chat={task_info.get('chat_id')}, msg={task_info.get('msg_id')}")

        # 设置取消事件 → WorkerPool 的 Worker 检测到后会退出
        cancel_event = self._cancel_events.get(target_rid)
        if cancel_event:
            cancel_event.set()
            logger.info(f"[CANCEL-DAEMON] cancel_event 已设置")

        # 直接调用 WorkerPool.cancel() 停止所有 Worker
        wp = task_info.get("worker_pool")
        if wp and hasattr(wp, 'cancel'):
            wp.cancel()
            logger.info(f"[CANCEL-DAEMON] WorkerPool.cancel() 已调用")

        # 保存 checkpoint（断点续传数据）
        if request.save_checkpoint and self.checkpoint_manager and wp:
            try:
                completed = wp.get_completed_indices() if hasattr(wp, 'get_completed_indices') else []
                # [FIX-RESUME-INDEX-MAP-2026-02-16] 如果是续传任务，将 remapped 索引转换回原始索引
                # 并合并之前已完成的 Part 索引
                idx_map = task_info.get("resume_index_map")
                if idx_map:
                    completed = [idx_map.get(i, i) for i in completed]
                resume_completed = task_info.get("resume_completed_indices")
                if resume_completed:
                    completed_set = set(completed) | set(resume_completed)
                    completed = sorted(completed_set)
                if completed:
                    await self.checkpoint_manager.save_download_state(
                        request_id=target_rid,
                        account_id=self.account_id,
                        chat_id=task_info.get("chat_id", 0),
                        msg_id=task_info.get("msg_id", 0),
                        file_path=task_info.get("temp_path", task_info.get("save_path", "")),
                        file_name=task_info.get("file_name", ""),
                        file_size=task_info.get("file_size", 0),
                        completed_part_indices=completed,
                        part_size=task_info.get("part_size", 1048576),
                        total_parts=task_info.get("total_parts", 0),
                    )
                    logger.info(f"[CANCEL-DAEMON] checkpoint 已保存: {len(completed)} parts completed")
                    logger.info(f"[Cancel] checkpoint saved: {len(completed)} parts for {target_rid}")
                else:
                    logger.info(f"[CANCEL-DAEMON] 无已完成 parts，跳过 checkpoint 保存")
            except Exception as ckpt_err:
                logger.error(f"[Cancel] checkpoint save failed: {ckpt_err}", exc_info=True)
                logger.warning(f"[CANCEL-DAEMON] checkpoint 保存失败: {ckpt_err}")

        # 注意：不在此处清理 active_tasks 和发送 TaskCompletedEvent
        # WorkerPool 取消后 download_all 会返回，_handle_download_request_workerpool 会继续执行
        # 在那里统一处理清理和事件发送
        logger.info(f"[Cancel] 取消处理完成: {target_rid}")

    async def _handle_shutdown_request(self, message: 'IPCMessage'):
        """处理关闭请求"""
        from download_protocol_v2 import ShutdownRequest

        payload = message.payload
        request = ShutdownRequest.from_dict(payload)

        # ====== [DIAG-SHUTDOWN-2026-02-16] 关闭请求诊断 ======
        logger.debug(f"[SHUTDOWN-STATE] ============================================")
        logger.debug(f"[SHUTDOWN-STATE] Daemon收到关闭请求")
        logger.debug(f"[SHUTDOWN-STATE]   save_state={request.save_state}")
        logger.debug(f"[SHUTDOWN-STATE]   当前 active_tasks 数量: {len(self.active_tasks)}")
        for _st_rid, _st_info in self.active_tasks.items():
            _st_wp = _st_info.get('worker_pool')
            _st_wp_has_indices = _st_wp and hasattr(_st_wp, 'get_completed_indices')
            _st_completed = _st_wp.get_completed_indices() if _st_wp_has_indices else []
            logger.debug(f"[SHUTDOWN-STATE]   任务: req={_st_rid[:8]}... chat={_st_info.get('chat_id','?')} msg={_st_info.get('msg_id','?')}")
            logger.debug(f"[SHUTDOWN-STATE]     worker_pool存在: {_st_wp is not None}")
            logger.debug(f"[SHUTDOWN-STATE]     已完成parts数: {len(_st_completed)}")
            logger.debug(f"[SHUTDOWN-STATE]     total_parts: {_st_info.get('total_parts', 0)}")
        logger.debug(f"[SHUTDOWN-STATE] 注意: 关闭时将保存checkpoint，但用户取消不会触发关闭!")
        logger.debug(f"[SHUTDOWN-STATE] ============================================")
        # ====== [DIAG-SHUTDOWN-2026-02-16] END ======

        logger.info(
            f"[Shutdown] 收到关闭请求 (save_state={request.save_state})"
        )

        # 标记关闭
        self.shutdown_requested = True

        # 如果需要保存状态
        if request.save_state:
            await self.save_checkpoints()

    async def save_checkpoints(self):
        """[CROSS-SESSION-RESUME-2026-02-16] 保存所有检查点（Part 级断点续传）"""
        logger.info("[Checkpoint] 保存所有检查点（Part 级）...")

        if not self.checkpoint_manager:
            logger.warning("[Checkpoint] CheckpointManager 未初始化，跳过保存")
            return

        saved_count = 0
        for request_id, task_info in list(self.active_tasks.items()):
            try:
                wp = task_info.get("worker_pool")
                if wp and hasattr(wp, 'get_completed_indices'):
                    # 有 WorkerPool 引用 → 保存 Part 级完成状态
                    completed = wp.get_completed_indices()
                    # [FIX-RESUME-INDEX-MAP-2026-02-16] 续传任务：remapped→original 索引转换 + 合并
                    idx_map = task_info.get("resume_index_map")
                    if idx_map:
                        completed = [idx_map.get(i, i) for i in completed]
                    resume_completed = task_info.get("resume_completed_indices")
                    if resume_completed:
                        completed_set = set(completed) | set(resume_completed)
                        completed = sorted(completed_set)
                    await self.checkpoint_manager.save_download_state(
                        request_id=request_id,
                        account_id=self.account_id,
                        chat_id=task_info.get("chat_id", 0),
                        msg_id=task_info.get("msg_id", 0),
                        file_path=task_info.get("temp_path", task_info.get("save_path", "")),
                        file_name=task_info.get("file_name", os.path.basename(task_info.get("save_path", ""))),
                        file_size=task_info.get("file_size", 0),
                        completed_part_indices=completed,
                        part_size=task_info.get("part_size", 1048576),
                        total_parts=task_info.get("total_parts", 0),
                    )
                    logger.info(f"[Checkpoint] Part 级: {request_id} ({len(completed)} parts 已完成)")
                else:
                    # 无 WorkerPool → 降级为基本存盘
                    await self.checkpoint_manager.save_progress(
                        request_id=request_id,
                        account_id=self.account_id,
                        chat_id=task_info.get("chat_id", 0),
                        msg_id=task_info.get("msg_id", 0),
                        file_path=task_info.get("save_path", ""),
                        file_name=os.path.basename(task_info.get("save_path", "")),
                        file_size=task_info.get("file_size", 0),
                        downloaded=task_info.get("downloaded", 0),
                        percentage=task_info.get("percentage", 0.0),
                        state='PAUSED',
                        error_message="Daemon shutdown - task interrupted"
                    )
                    logger.info(f"[Checkpoint] 基本: {request_id}")
                saved_count += 1
            except Exception as save_err:
                logger.error(f"[Checkpoint] 保存任务 {request_id} 失败: {save_err}")

        logger.info(f"[Checkpoint] 已保存 {saved_count}/{len(self.active_tasks)} 个检查点")

    async def shutdown(self):
        """
        优雅关闭

        步骤：
        1. 标记关闭状态
        2. 等待所有任务完成（或超时）
        3. 关闭DownloadPool
        4. 断开TelegramClient
        5. 断开所有跟踪的客户端连接
        6. 清理资源
        """
        self.state = DaemonState.SHUTTING_DOWN
        self.shutdown_requested = True
        logger.info("[Shutdown] 开始关闭...")

        try:
            # 步骤1：等待任务完成（最多10秒）
            if self.active_tasks:
                logger.info(
                    f"[Shutdown] 等待{len(self.active_tasks)}个任务完成..."
                )
                tasks_to_clear = list(self.active_tasks.items())
                for request_id, task_info in tasks_to_clear:
                    try:
                        logger.warning(
                            f"[Shutdown] 任务 {request_id} 未完成，发送失败事件"
                        )
                        await self._emit_task_completed(
                            request_id,
                            success=False,
                            file_path=task_info.get("save_path"),
                            file_size=0,
                            error_message="Daemon关闭，任务中止"
                        )
                    except Exception as emit_err:
                        logger.error(
                            f"[Shutdown] 发送失败事件异常: {emit_err}",
                            exc_info=True
                        )

                self.active_tasks.clear()
                logger.info("[Shutdown] active_tasks 已清空")

            # 步骤2：断开主 TelegramClient
            if self.client:
                logger.info("[Shutdown] 断开TelegramClient...")
                try:
                    await self.client.disconnect()
                    logger.info("[Shutdown] TelegramClient已断开")
                except Exception as dc_err:
                    logger.warning(f"[Shutdown] TelegramClient断开异常: {dc_err}")

            # 步骤3：断开所有跟踪的客户端连接（连接池等）
            for c in self._tracked_clients:
                if c is None:
                    continue
                try:
                    await c.disconnect()
                except Exception as e:
                    logger.debug(f"[Shutdown] tracked client disconnect failed: {e}")
            self._tracked_clients.clear()

            # 步骤4：关闭 CheckpointManager
            if self.checkpoint_manager:
                try:
                    logger.info("[Shutdown] [CHECKPOINT] 关闭 CheckpointManager...")
                    await self.checkpoint_manager.close()
                    logger.info("[Shutdown] [CHECKPOINT] CheckpointManager 已关闭")
                except Exception as checkpoint_close_err:
                    logger.error(f"[Shutdown] [CHECKPOINT] 关闭 CheckpointManager 失败: {checkpoint_close_err}")

            logger.info("[Shutdown] 关闭完成")

        except Exception as e:
            logger.error(f"[Shutdown] 关闭异常: {e}", exc_info=True)
            try:
                self.active_tasks.clear()
                logger.info("[Shutdown] 异常恢复：active_tasks 已清空")
            except Exception as clear_err:
                logger.error(f"[Shutdown] 清空 active_tasks 失败: {clear_err}")

    def request_shutdown(self):
        """请求关闭（信号处理器调用）"""
        logger.info("[Shutdown] 收到关闭请求...")
        self.shutdown_requested = True


# ==================== 测试工具 ====================
async def test_daemon_core():
    """测试DaemonCore（独立运行）"""
    print("=== DaemonCore测试 ===\n")

    # 创建mock对象
    class MockIPC:
        async def send(self, message):
            print(f"[MockIPC] 发送: {message.message_type.name}")

        async def send_ack(self, message_id):
            print(f"[MockIPC] 发送ACK: {message_id}")

        async def receive(self, timeout):
            await asyncio.sleep(0.5)
            return None

        async def close(self):
            pass

    class MockEventBus:
        async def publish(self, event):
            print(f"[MockEventBus] 发布事件: {event}")

    # 创建DaemonCore
    core = DaemonCore(
        session_path="./test_session",
        account_id="test_account",
        ipc_channel=MockIPC(),
        event_bus=MockEventBus()
    )

    # 运行一小段时间
    try:
        await asyncio.wait_for(core.run(), timeout=3.0)
    except asyncio.TimeoutError:
        print("\n测试完成（超时）")
        await core.shutdown()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(test_daemon_core())
