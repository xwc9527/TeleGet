"""
Daemon监控管理器

功能：
1. DaemonHealthMonitor - 监控Daemon进程存活
2. MemoryMonitor - 监控内存占用
3. HeartbeatDetector - 监控心跳响应
4. SessionMonitor - 监控会话文件变化

每个组件都运行在后台，定期检查和告警
"""

import asyncio
import logging
import os
import psutil
import time
from pathlib import Path
from typing import Optional, Callable, Dict, Any

logger = logging.getLogger(__name__)


# ==================== DaemonHealthMonitor - 进程监控 ====================
class DaemonHealthMonitor:
    """
    Daemon进程健康监控

    监控内容：
    - 进程是否存活
    - 进程是否崩溃
    - 自动重启失败的Daemon（如果是活跃账号）
    """

    def __init__(self, scheduler: 'AccountScheduler', check_interval: int = 10):
        """
        初始化进程监控

        Args:
            scheduler: AccountScheduler实例
            check_interval: 检查间隔（秒）
        """
        self.scheduler = scheduler
        self.check_interval = check_interval
        self._monitor_task = None
        self._crashed_accounts = {}  # account_id -> crash_count

        logger.info(
            f"[DaemonHealthMonitor] 初始化 (interval={check_interval}s)"
        )

    async def start(self):
        """启动监控"""
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("[DaemonHealthMonitor] 监控已启动")

    async def stop(self):
        """停止监控"""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("[DaemonHealthMonitor] 监控已停止")

    async def _monitor_loop(self):
        """监控主循环"""
        while True:
            try:
                await asyncio.sleep(self.check_interval)

                # 检查每个Daemon
                for account_id, daemon in list(
                    self.scheduler.account_daemons.items()
                ):
                    await self._check_daemon(account_id, daemon)

            except asyncio.CancelledError:
                logger.info("[DaemonHealthMonitor] 监控循环已停止")
                break

            except Exception as e:
                logger.error(f"[DaemonHealthMonitor] 监控异常: {e}")

    async def _check_daemon(self, account_id: str, daemon: 'DaemonProcess'):
        """
        检查单个Daemon

        Args:
            account_id: 账号ID
            daemon: Daemon进程对象
        """
        try:
            # 检查进程是否存活
            if not daemon.is_running():
                await self._on_daemon_crashed(account_id, daemon)

        except Exception as e:
            logger.error(f"[DaemonHealthMonitor] 检查异常 {account_id}: {e}")

    async def _on_daemon_crashed(self, account_id: str, daemon: 'DaemonProcess'):
        """
        处理Daemon崩溃

        Args:
            account_id: 账号ID
            daemon: Daemon进程对象
        """
        logger.critical(f"[DaemonHealthMonitor] Daemon崩溃: {account_id}")

        # 记录崩溃次数
        crash_count = self._crashed_accounts.get(account_id, 0) + 1
        self._crashed_accounts[account_id] = crash_count

        logger.warning(
            f"[DaemonHealthMonitor] 崩溃次数: {crash_count}"
        )

        # 获取进程退出码和输出
        try:
            if daemon.process:
                returncode = daemon.process.returncode
                stdout, stderr = daemon.process.communicate(timeout=1)
                logger.error(f"[DaemonHealthMonitor] 退出码: {returncode}")
                if stderr:
                    logger.error(f"[DaemonHealthMonitor] 错误输出: {stderr[:500]}")
        except Exception as e:
            logger.debug(f"[DaemonHealthMonitor] 无法获取进程信息: {e}")

        # 如果这是活跃账号，自动重启
        if account_id == self.scheduler.active_account_id:
            logger.warning(
                f"[DaemonHealthMonitor] 重启活跃账号Daemon: {account_id}"
            )

            try:
                # 清理旧daemon记录
                if account_id in self.scheduler.account_daemons:
                    del self.scheduler.account_daemons[account_id]
                if account_id in self.scheduler.ipc_channels:
                    del self.scheduler.ipc_channels[account_id]

                # 重启Daemon
                await self.scheduler._ensure_daemon_running(account_id)

                # 重置崩溃计数
                self._crashed_accounts[account_id] = 0

                logger.info(f"[DaemonHealthMonitor] Daemon已重启: {account_id}")

            except Exception as e:
                logger.error(
                    f"[DaemonHealthMonitor] 重启失败: {account_id}: {e}"
                )

        # 如果连续崩溃超过5次，上报告警
        if crash_count >= 5:
            logger.critical(
                f"[DaemonHealthMonitor] 频繁崩溃告警: "
                f"{account_id} (崩溃{crash_count}次)"
            )


# ==================== MemoryMonitor - 内存监控 ====================
class MemoryMonitor:
    """
    Daemon内存占用监控

    监控内容：
    - 内存占用量
    - 内存泄漏趋势
    - 超限告警和重启
    """

    def __init__(self,
                 scheduler: 'AccountScheduler',
                 memory_limit_mb: int = 4000,
                 check_interval: int = 30):
        """
        初始化内存监控

        Args:
            scheduler: AccountScheduler实例
            memory_limit_mb: 内存限制（MB）
            check_interval: 检查间隔（秒）
        """
        self.scheduler = scheduler
        self.memory_limit_mb = memory_limit_mb
        self.check_interval = check_interval
        self._monitor_task = None
        self._memory_history: Dict[str, list] = {}  # account_id -> [memory, time]

        logger.info(
            f"[MemoryMonitor] 初始化 "
            f"(limit={memory_limit_mb}MB, interval={check_interval}s)"
        )

    async def start(self):
        """启动监控"""
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("[MemoryMonitor] 监控已启动")

    async def stop(self):
        """停止监控"""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("[MemoryMonitor] 监控已停止")

    async def _monitor_loop(self):
        """监控主循环"""
        while True:
            try:
                await asyncio.sleep(self.check_interval)

                # 检查每个Daemon的内存
                for account_id, daemon in list(
                    self.scheduler.account_daemons.items()
                ):
                    await self._check_memory(account_id, daemon)

            except asyncio.CancelledError:
                logger.info("[MemoryMonitor] 监控循环已停止")
                break

            except Exception as e:
                logger.error(f"[MemoryMonitor] 监控异常: {e}")

    async def _check_memory(self, account_id: str, daemon: 'DaemonProcess'):
        """
        检查单个Daemon的内存

        Args:
            account_id: 账号ID
            daemon: Daemon进程对象
        """
        try:
            if not daemon.is_running():
                return

            # 获取进程内存
            process = psutil.Process(daemon.process.pid)
            memory_mb = process.memory_info().rss / 1024 / 1024

            # 记录历史
            if account_id not in self._memory_history:
                self._memory_history[account_id] = []

            self._memory_history[account_id].append(
                (memory_mb, time.time())
            )

            # 保持最近100条记录
            if len(self._memory_history[account_id]) > 100:
                self._memory_history[account_id] = self._memory_history[
                    account_id
                ][-100:]

            # 检查是否超限
            if memory_mb > self.memory_limit_mb:
                logger.warning(
                    f"[MemoryMonitor] 内存超限告警: {account_id} "
                    f"({memory_mb:.1f}MB > {self.memory_limit_mb}MB)"
                )

                # 如果连续超限，重启Daemon
                if await self._is_continuous_overflow(account_id):
                    await self._restart_daemon(account_id)

            elif memory_mb > self.memory_limit_mb * 0.8:
                logger.warning(
                    f"[MemoryMonitor] 内存告警: {account_id} "
                    f"({memory_mb:.1f}MB)"
                )

            else:
                logger.debug(
                    f"[MemoryMonitor] 内存正常: {account_id} "
                    f"({memory_mb:.1f}MB)"
                )

        except Exception as e:
            logger.error(
                f"[MemoryMonitor] 检查异常 {account_id}: {e}"
            )

    async def _is_continuous_overflow(self, account_id: str) -> bool:
        """
        检查是否连续超限（最近5条都超过）

        Args:
            account_id: 账号ID

        Returns:
            True表示连续超限
        """
        history = self._memory_history.get(account_id, [])
        if len(history) < 5:
            return False

        # 检查最近5条
        recent = history[-5:]
        overflow_count = sum(
            1 for mem, _ in recent
            if mem > self.memory_limit_mb
        )

        return overflow_count >= 3  # 至少3条超限

    async def _restart_daemon(self, account_id: str):
        """
        由于内存超限而重启Daemon

        Args:
            account_id: 账号ID
        """
        logger.critical(
            f"[MemoryMonitor] 由于内存超限重启Daemon: {account_id}"
        )

        try:
            await self.scheduler._shutdown_daemon(
                account_id,
                save_state=True
            )

            # 清除历史记录
            if account_id in self._memory_history:
                del self._memory_history[account_id]

            # 如果是活跃账号，立即重启
            if account_id == self.scheduler.active_account_id:
                await self.scheduler._ensure_daemon_running(account_id)

        except Exception as e:
            logger.error(f"[MemoryMonitor] 重启失败: {account_id}: {e}")


# ==================== HeartbeatDetector - 心跳监控 ====================
class HeartbeatDetector:
    """
    Daemon心跳响应监控

    监控内容：
    - 定期发送心跳请求
    - 检测Daemon是否响应
    - 超时重试机制
    """

    def __init__(self,
                 scheduler: 'AccountScheduler',
                 heartbeat_interval: int = 10,
                 heartbeat_timeout: int = 5):
        """
        初始化心跳监控

        Args:
            scheduler: AccountScheduler实例
            heartbeat_interval: 心跳间隔（秒）
            heartbeat_timeout: 心跳超时（秒）
        """
        self.scheduler = scheduler
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self._monitor_task = None
        self._last_heartbeat: Dict[str, float] = {}  # account_id -> timestamp

        logger.info(
            f"[HeartbeatDetector] 初始化 "
            f"(interval={heartbeat_interval}s, timeout={heartbeat_timeout}s)"
        )

    async def start(self):
        """启动监控"""
        self._monitor_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("[HeartbeatDetector] 监控已启动")

    async def stop(self):
        """停止监控"""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("[HeartbeatDetector] 监控已停止")

    async def _heartbeat_loop(self):
        """心跳监控循环"""
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)

                # 发送心跳到每个Daemon
                for account_id, daemon in list(
                    self.scheduler.account_daemons.items()
                ):
                    await self._send_heartbeat(account_id, daemon)

            except asyncio.CancelledError:
                logger.info("[HeartbeatDetector] 监控循环已停止")
                break

            except Exception as e:
                logger.error(f"[HeartbeatDetector] 监控异常: {e}")

    async def _send_heartbeat(self, account_id: str, daemon: 'DaemonProcess'):
        """
        发送心跳请求

        Args:
            account_id: 账号ID
            daemon: Daemon进程对象
        """
        try:
            if not daemon.is_running():
                return

            ipc = self.scheduler.ipc_channels.get(account_id)
            if not ipc:
                return

            self._last_heartbeat[account_id] = time.time()
            logger.debug(f"[HeartbeatDetector] 心跳检测: {account_id}")

        except asyncio.TimeoutError:
            logger.warning(
                f"[HeartbeatDetector] 心跳超时: {account_id}"
            )

        except Exception as e:
            logger.error(
                f"[HeartbeatDetector] 发送心跳失败 {account_id}: {e}"
            )


# ==================== SessionMonitor - 会话文件监控 ====================
class SessionMonitor:
    """
    Session会话文件变化监控

    监控内容：
    - 会话文件是否被修改
    - 自动同步到Daemon
    - 权限变化检测

    注意：这是为了处理多设备登录的情况
    """

    def __init__(self,
                 scheduler: 'AccountScheduler',
                 check_interval: int = 60):
        """
        初始化会话监控

        Args:
            scheduler: AccountScheduler实例
            check_interval: 检查间隔（秒）
        """
        self.scheduler = scheduler
        self.check_interval = check_interval
        self._monitor_task = None
        self._session_mtime: Dict[str, float] = {}  # account_id -> mtime

        logger.info(
            f"[SessionMonitor] 初始化 (interval={check_interval}s)"
        )

    async def start(self):
        """启动监控"""
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("[SessionMonitor] 监控已启动")

    async def stop(self):
        """停止监控"""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("[SessionMonitor] 监控已停止")

    async def _monitor_loop(self):
        """监控主循环"""
        while True:
            try:
                await asyncio.sleep(self.check_interval)

                # 检查每个活跃账号的session文件
                for account_id in list(self.scheduler.account_daemons.keys()):
                    await self._check_session(account_id)

            except asyncio.CancelledError:
                logger.info("[SessionMonitor] 监控循环已停止")
                break

            except Exception as e:
                logger.error(f"[SessionMonitor] 监控异常: {e}")

    async def _check_session(self, account_id: str):
        """
        检查单个账号的session文件

        Args:
            account_id: 账号ID
        """
        try:
            # 构造session文件路径
            session_dir = self.scheduler.config.get('session_dir', 'data/accounts')
            session_path = f"{session_dir}/{account_id}/session.session"

            if not os.path.exists(session_path):
                logger.warning(f"[SessionMonitor] Session文件不存在: {os.path.basename(session_path)}")
                return

            # 获取修改时间
            current_mtime = os.path.getmtime(session_path)
            last_mtime = self._session_mtime.get(account_id, 0)

            # 检查是否被修改
            if current_mtime > last_mtime:
                logger.warning(
                    f"[SessionMonitor] Session文件被修改: {account_id}"
                )

                # TODO: 通知Daemon重新加载session
                # await self.scheduler._notify_daemon_reload_session(account_id)

                # 更新记录
                self._session_mtime[account_id] = current_mtime

        except Exception as e:
            logger.error(
                f"[SessionMonitor] 检查异常 {account_id}: {e}"
            )


# ==================== 监控协调器 ====================
class DaemonMonitor:
    """
    Daemon监控协调器

    集中管理所有监控器
    """

    def __init__(self, scheduler: 'AccountScheduler', config: Dict[str, Any]):
        """
        初始化监控协调器

        Args:
            scheduler: AccountScheduler实例
            config: 配置字典
        """
        self.scheduler = scheduler
        self.config = config

        # 创建各个监控器
        self.health_monitor = DaemonHealthMonitor(
            scheduler,
            check_interval=config.get('daemon_health_check_interval', 10)
        )

        self.memory_monitor = MemoryMonitor(
            scheduler,
            memory_limit_mb=config.get('daemon_memory_limit_mb', 4000),
            check_interval=config.get('daemon_memory_check_interval', 30)
        )

        self.heartbeat_detector = HeartbeatDetector(
            scheduler,
            heartbeat_interval=config.get('daemon_heartbeat_interval', 10),
            heartbeat_timeout=config.get('daemon_heartbeat_timeout', 5)
        )

        self.session_monitor = SessionMonitor(
            scheduler,
            check_interval=config.get('daemon_session_check_interval', 60)
        )

        logger.info("[DaemonMonitor] 初始化完成")

    async def start(self):
        """启动所有监控器"""
        await self.health_monitor.start()
        await self.memory_monitor.start()
        await self.heartbeat_detector.start()
        await self.session_monitor.start()

        logger.info("[DaemonMonitor] 所有监控器已启动")

    async def stop(self):
        """停止所有监控器"""
        await self.health_monitor.stop()
        await self.memory_monitor.stop()
        await self.heartbeat_detector.stop()
        await self.session_monitor.stop()

        logger.info("[DaemonMonitor] 所有监控器已停止")

    def get_status(self) -> Dict[str, Any]:
        """
        获取监控状态

        Returns:
            状态字典
        """
        return {
            'health_monitor': {
                'running': self.health_monitor._monitor_task is not None,
                'crashed_accounts': dict(self.health_monitor._crashed_accounts)
            },
            'memory_monitor': {
                'running': self.memory_monitor._monitor_task is not None,
                'limit_mb': self.memory_monitor.memory_limit_mb,
                'history': self.memory_monitor._memory_history
            },
            'heartbeat_detector': {
                'running': self.heartbeat_detector._monitor_task is not None,
                'last_heartbeat': self.heartbeat_detector._last_heartbeat
            },
            'session_monitor': {
                'running': self.session_monitor._monitor_task is not None,
                'watched_sessions': self.session_monitor._session_mtime
            }
        }
