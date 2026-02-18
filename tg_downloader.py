"""
TGDownloader SDK - Telegram 文件下载器的单一入口

用法:
    from tg_downloader import TGDownloader

    downloader = TGDownloader(
        api_id=12345,
        api_hash="your_api_hash",
        session_dir="data/accounts",
    )

    async def main():
        # 激活账号（启动 Daemon 子进程）
        await downloader.start("my_account")

        # 提交下载
        request_id = await downloader.download(
            chat_id=-1001234567890,
            msg_id=42,
            save_path="/tmp/video.mp4",
        )

        # 取消下载（可选）
        await downloader.cancel(request_id)

        # 关闭（自动清理 Daemon 子进程）
        await downloader.shutdown()
"""

import asyncio
import atexit
import logging
import os
import signal
import sys
from typing import Optional, Callable, Dict, Any

logger = logging.getLogger(__name__)


class TGDownloader:
    """Telegram 文件下载 SDK 入口。

    封装 AccountScheduler + EventBus + DownloadEventListener，
    对外仅暴露 start / download / cancel / shutdown 四个方法。
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session_dir: str = "data/accounts",
        log_dir: str = "logs",
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Args:
            api_id:      Telegram API ID
            api_hash:    Telegram API Hash
            session_dir: 存放 .session 文件的目录
            log_dir:     日志文件输出目录
            config:      额外配置（会合并到 AccountScheduler 的 download_config）
        """
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_dir = os.path.abspath(session_dir)
        self._log_dir = os.path.abspath(log_dir)

        # 合并用户配置
        self._config: Dict[str, Any] = {
            "api_id": api_id,
            "api_hash": api_hash,
            "session_dir": self._session_dir,
            "log_dir": self._log_dir,
        }
        if config:
            self._config.update(config)

        # 懒初始化
        self._scheduler = None  # AccountScheduler
        self._event_bus = None  # EventBus
        self._listener = None   # DownloadEventListener
        self._event_loop_task = None  # 事件循环后台任务
        self._started = False
        self._shutting_down = False

        # 注册退出清理
        atexit.register(self._atexit_cleanup)
        self._install_signal_handlers()

    # ------------------------------------------------------------------ #
    #  公开 API
    # ------------------------------------------------------------------ #

    async def start(self, account_id: str) -> bool:
        """激活账号并启动 Daemon 子进程。

        Args:
            account_id: 账号标识（对应 session_dir 下的子目录名或文件名前缀）

        Returns:
            True 表示启动成功，False 表示失败（如 session 文件缺失）
        """
        self._ensure_init()
        await self._setup_event_bus()

        ok = await self._scheduler.set_active_account(account_id)
        if ok:
            self._started = True
            logger.info(f"[TGDownloader] 账号 {account_id} 已激活")
        else:
            logger.error(f"[TGDownloader] 账号 {account_id} 激活失败")
        return ok

    async def download(
        self,
        chat_id: int,
        msg_id: int,
        save_path: str,
        media_type: Optional[str] = None,
        file_size: int = 0,
        progress_callback: Optional[Callable] = None,
        complete_callback: Optional[Callable] = None,
        error_callback: Optional[Callable] = None,
    ) -> str:
        """提交一个下载任务。

        Args:
            chat_id:            Telegram 会话 ID
            msg_id:             消息 ID
            save_path:          文件保存路径
            media_type:         媒体类型（'document', 'photo' 等，可选）
            file_size:          预知文件大小（字节，可选，用于进度计算）
            progress_callback:  进度回调 (downloaded_bytes, total_bytes, percentage)
            complete_callback:  完成回调 (success, file_path, error_message)
            error_callback:     错误回调 (error_message)

        Returns:
            request_id（UUID 字符串），可用于 cancel()

        Raises:
            RuntimeError: 未调用 start() 就直接 download
        """
        if not self._started or self._scheduler is None:
            raise RuntimeError(
                "请先调用 start(account_id) 激活一个账号"
            )

        account_id = self._scheduler.active_account_id
        if account_id is None:
            raise RuntimeError("没有活跃账号，请先调用 start()")

        request_id = await self._scheduler.submit_download(
            account_id=account_id,
            chat_id=chat_id,
            msg_id=msg_id,
            save_path=save_path,
            media_type=media_type,
            file_size=file_size,
            progress_callback=progress_callback,
            complete_callback=complete_callback,
            error_callback=error_callback,
        )
        logger.info(
            f"[TGDownloader] 下载已提交: request_id={request_id}, "
            f"chat={chat_id}, msg={msg_id}"
        )
        return request_id

    async def cancel(self, request_id: str) -> None:
        """取消一个下载任务。

        Args:
            request_id: download() 返回的任务 ID
        """
        if self._scheduler is None:
            return
        await self._scheduler.cancel_download(request_id)
        logger.info(f"[TGDownloader] 已请求取消: {request_id}")

    async def shutdown(self) -> None:
        """优雅关闭所有 Daemon 子进程并释放资源。"""
        if self._shutting_down:
            return
        self._shutting_down = True

        logger.info("[TGDownloader] 正在关闭...")

        # 停止事件循环后台任务
        if self._event_loop_task and not self._event_loop_task.done():
            self._event_loop_task.cancel()
            try:
                await self._event_loop_task
            except asyncio.CancelledError:
                pass

        # 关闭所有 Daemon
        if self._scheduler:
            try:
                await self._scheduler.shutdown_all_daemons()
            except Exception as e:
                logger.warning(f"[TGDownloader] 关闭 Daemon 时出错: {e}")

        self._started = False
        logger.info("[TGDownloader] 已关闭")

    # ------------------------------------------------------------------ #
    #  属性
    # ------------------------------------------------------------------ #

    @property
    def active_account(self) -> Optional[str]:
        """当前活跃账号 ID，未启动时为 None。"""
        if self._scheduler:
            return self._scheduler.active_account_id
        return None

    @property
    def active_downloads(self) -> Dict:
        """当前活跃的下载任务字典 {request_id: DownloadTask}。"""
        if self._scheduler:
            return dict(self._scheduler.active_downloads)
        return {}

    # ------------------------------------------------------------------ #
    #  内部方法
    # ------------------------------------------------------------------ #

    def _ensure_init(self) -> None:
        """懒初始化 Scheduler / EventBus / Listener。"""
        if self._scheduler is not None:
            return

        from download_manager_v2 import AccountScheduler, DownloadEventListener
        from download_event_bus import EventBus

        self._scheduler = AccountScheduler(download_config=self._config)

        # session 路径解析器
        session_dir = self._session_dir

        def _resolve(account_id: str) -> Optional[str]:
            # 约定：session_dir/<account_id>  (无扩展名，Telethon 自动加 .session)
            candidate = os.path.join(session_dir, account_id)
            if os.path.exists(f"{candidate}.session"):
                return candidate
            # 兼容子目录模式：session_dir/<account_id>/session
            candidate2 = os.path.join(session_dir, account_id, "session")
            if os.path.exists(f"{candidate2}.session"):
                return candidate2
            return None

        self._scheduler.set_session_path_resolver(_resolve)

        # 事件总线（可选，用于事件驱动的进度回调）
        self._event_bus = EventBus(throttle_interval=0.5)
        # set_event_bus 是 async 的，需要在 event loop 里调用
        # 但 _ensure_init 是 sync 的，所以推迟到 start() 中处理
        self._listener = DownloadEventListener(self._scheduler, self._event_bus)

        logger.info("[TGDownloader] 初始化完成")

    async def _setup_event_bus(self) -> None:
        """设置事件总线并启动事件处理循环（仅执行一次）。"""
        if self._event_loop_task is not None:
            return
        if self._event_bus and self._scheduler:
            await self._scheduler.set_event_bus(self._event_bus)
        if self._listener:
            await self._listener.subscribe_all()

        # 后台事件处理循环
        self._event_loop_task = asyncio.create_task(self._event_loop())

    async def _event_loop(self) -> None:
        """后台事件分发循环。"""
        while True:
            try:
                if self._event_bus:
                    await self._event_bus.process_events(timeout=0.5)
                else:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[TGDownloader] 事件循环异常: {e}")
                await asyncio.sleep(1)

    # ------------------------------------------------------------------ #
    #  退出保护
    # ------------------------------------------------------------------ #

    def _atexit_cleanup(self) -> None:
        """atexit 回调：尽力关闭 Daemon 子进程，防止僵尸进程。"""
        if self._scheduler is None:
            return
        # 同步强杀仍在运行的子进程
        for account_id, daemon in list(
            self._scheduler.account_daemons.items()
        ):
            try:
                if daemon.is_running():
                    logger.info(
                        f"[TGDownloader] atexit: 强制终止 Daemon {account_id} "
                        f"(pid={daemon.get_pid()})"
                    )
                    daemon.process.kill()
            except Exception:
                pass

    def _install_signal_handlers(self) -> None:
        """注册 SIGINT/SIGTERM 处理，确保 Daemon 被清理。"""
        def _handler(signum, frame):
            logger.info(f"[TGDownloader] 收到信号 {signum}，正在清理...")
            self._atexit_cleanup()
            sys.exit(128 + signum)

        try:
            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
        except (OSError, ValueError):
            # 非主线程或平台不支持
            pass
