"""
异步事件总线 - 支持事件发布订阅、节流和优先级

关键特性：
1. 异步事件发布订阅机制
2. 进度事件自动节流（防止IPC通道堵塞）
3. 优先级队列（重要事件优先）
4. 事件处理超时检测
5. 重入检测（防止死锁）
"""

import asyncio
import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Any, Set
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


# ==================== 事件优先级 ====================
class EventPriority(Enum):
    """事件优先级"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


# ==================== 基础事件类 ====================
@dataclass
class Event(ABC):
    """事件基类"""
    request_id: str
    timestamp: float = field(default_factory=time.time)
    priority: EventPriority = EventPriority.NORMAL

    @property
    def event_type(self) -> str:
        """事件类型标识"""
        return self.__class__.__name__


# ==================== 具体事件类 ====================
@dataclass
class TaskStartedEvent(Event):
    """任务开始事件"""
    file_size: int = 0
    file_name: str = ""


@dataclass
class TaskProgressEvent(Event):
    """任务进度事件（受节流控制）"""
    current: int = 0
    total: int = 0
    percentage: float = 0.0
    priority: EventPriority = field(default_factory=lambda: EventPriority.NORMAL)


@dataclass
class TaskCompletedEvent(Event):
    """任务完成事件"""
    file_path: Optional[str] = None
    success: bool = False
    error_message: Optional[str] = None
    priority: EventPriority = field(default_factory=lambda: EventPriority.HIGH)


@dataclass
class DaemonStatusEvent(Event):
    """Daemon状态事件"""
    status: str = "idle"
    memory_mb: float = 0.0
    cpu_percent: float = 0.0


@dataclass
class AccountSwitchEvent(Event):
    """账号切换事件"""
    from_account_id: str = ""
    to_account_id: str = ""
    priority: EventPriority = field(default_factory=lambda: EventPriority.HIGH)


# ==================== 进度节流器 ====================
class ProgressThrottler:
    """
    进度事件节流器 - 防止IPC通道堵塞

    策略：
    1. 限制发送频率：最多每500ms发一次
    2. 过滤小变化：进度变化<1%不发送
    3. 强制发送完成：100%必须发送
    4. 防止洪泛：队列满时丢弃低优先级
    """

    def __init__(self, interval: float = 0.5, delta_threshold: float = 1.0):
        """
        初始化节流器

        Args:
            interval: 最小发送间隔（秒），默认0.5s
            delta_threshold: 最小变化阈值（百分比），默认1.0%
        """
        self.interval = interval
        self.delta_threshold = delta_threshold

        # 记录每个request_id的最后发送时间和百分比
        self._last_progress: Dict[str, tuple] = {}
        self._lock = asyncio.Lock()

    async def should_send(self, request_id: str, current: int, total: int) -> bool:
        """
        判断是否应该发送进度事件

        Args:
            request_id: 任务ID
            current: 当前字节数
            total: 总字节数

        Returns:
            True表示应该发送，False表示应该丢弃
        """
        async with self._lock:
            # 计算百分比
            percentage = (current / total * 100) if total > 0 else 0
            now = time.time()

            # 首次进度，必须发送
            if request_id not in self._last_progress:
                self._last_progress[request_id] = (now, percentage)
                logger.debug(f"[ProgressThrottler] 首次进度: {request_id} {percentage:.1f}%")
                return True

            last_time, last_percentage = self._last_progress[request_id]

            # 检查条件
            time_elapsed = now - last_time > self.interval
            progress_changed = abs(percentage - last_percentage) > self.delta_threshold
            is_complete = percentage >= 100

            # 满足任一条件则发送
            should_send = time_elapsed or progress_changed or is_complete

            if should_send:
                self._last_progress[request_id] = (now, percentage)
                logger.debug(
                    f"[ProgressThrottler] 发送进度: {request_id} {percentage:.1f}% "
                    f"(时间:{time_elapsed}, 变化:{progress_changed}, 完成:{is_complete})"
                )
                return True

            # 调试：记录被丢弃的事件（概率性）
            if int(now * 10) % 20 == 0:  # 每2秒记录一次
                logger.debug(
                    f"[ProgressThrottler] 丢弃进度: {request_id} {percentage:.1f}% "
                    f"(最后:{last_percentage:.1f}%)"
                )

            return False

    async def cleanup(self, request_id: str):
        """清理已完成任务的记录"""
        async with self._lock:
            self._last_progress.pop(request_id, None)


# ==================== 事件总线 ====================
class EventBus:
    """
    异步事件总线

    支持：
    - 异步发布/订阅
    - 多优先级队列
    - 进度事件节流
    - 处理超时检测
    - 重入检测
    """

    def __init__(self, throttle_interval: float = 0.5):
        """初始化事件总线"""
        # 按优先级的事件队列
        self._queues = {
            EventPriority.CRITICAL: asyncio.Queue(maxsize=100),
            EventPriority.HIGH: asyncio.Queue(maxsize=200),
            EventPriority.NORMAL: asyncio.Queue(maxsize=500),
            EventPriority.LOW: asyncio.Queue(maxsize=100),
        }

        # 事件订阅者
        self._subscribers: Dict[str, List[tuple]] = {}  # event_type -> [(handler, priority)]

        # 进度节流器
        self.throttler = ProgressThrottler(interval=throttle_interval)

        # 线程本地存储（重入检测）
        self._processing = None

        logger.info(f"[EventBus] 已初始化 (节流间隔:{throttle_interval}s)")

    def subscribe(self, event_type: str, handler: Callable,
                  priority: int = 0) -> Callable:
        """
        订阅事件

        Args:
            event_type: 事件类型（类名）
            handler: 事件处理器（异步函数）
            priority: 处理优先级（0=最低，100=最高）

        Returns:
            取消订阅的函数
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        self._subscribers[event_type].append((handler, priority))

        # 按优先级排序
        self._subscribers[event_type].sort(key=lambda x: x[1], reverse=True)

        logger.debug(f"[EventBus] 订阅事件: {event_type}")

        # 返回取消订阅函数
        def unsubscribe():
            self._subscribers[event_type].remove((handler, priority))

        return unsubscribe

    async def publish(self, event: Event):
        """
        发布事件（异步，不阻塞）

        Args:
            event: 事件对象
        """
        # 特殊处理：进度事件节流
        if isinstance(event, TaskProgressEvent):
            if not await self.throttler.should_send(event.request_id, event.current, event.total):
                # 节流：丢弃此事件
                return

        # 重入检测
        if self._processing:
            logger.warning("[EventBus] 检测到事件处理器中发布事件，转为后台任务")
            asyncio.create_task(self._publish_async(event))
            return

        await self._publish_async(event)

    async def _publish_async(self, event: Event):
        """异步发布事件到队列"""
        try:
            # 选择队列
            queue = self._queues[event.priority]

            # 尝试放入队列（队列满时丢弃低优先级）
            try:
                queue.put_nowait(event)
                logger.debug(f"[EventBus] 发布事件: {event.event_type} (优先级:{event.priority.name})")

            except asyncio.QueueFull:
                # 队列满时的处理
                if event.priority in [EventPriority.HIGH, EventPriority.CRITICAL]:
                    # 重要事件：尝试从队列取出一个低优先级事件
                    try:
                        discarded = queue.get_nowait()
                        logger.warning(f"[EventBus] 队列满，丢弃事件: {discarded.event_type}")
                    except asyncio.QueueEmpty:
                        pass

                    queue.put_nowait(event)

                else:
                    # 低优先级事件：丢弃
                    logger.warning(f"[EventBus] 队列满，丢弃事件: {event.event_type}")

        except Exception as e:
            logger.error(f"[EventBus] 发布事件异常: {e}")

    async def consume(self, timeout: float = 1.0) -> Optional[Event]:
        """
        消费事件（带超时）

        优先级顺序：CRITICAL → HIGH → NORMAL → LOW

        Returns:
            Event或None（超时）
        """
        try:
            # 按优先级检查队列
            for priority in [EventPriority.CRITICAL, EventPriority.HIGH,
                           EventPriority.NORMAL, EventPriority.LOW]:
                queue = self._queues[priority]

                try:
                    event = queue.get_nowait()

                    # 清理进度节流记录
                    if isinstance(event, TaskCompletedEvent):
                        await self.throttler.cleanup(event.request_id)

                    return event

                except asyncio.QueueEmpty:
                    continue

            # 如果所有队列都空，阻塞等待
            # 这里使用gather + wait_for实现优先级等待
            for priority in [EventPriority.CRITICAL, EventPriority.HIGH,
                           EventPriority.NORMAL, EventPriority.LOW]:
                queue = self._queues[priority]

                try:
                    event = await asyncio.wait_for(
                        queue.get(),
                        timeout=timeout / 4  # 均分超时时间
                    )

                    return event

                except asyncio.TimeoutError:
                    continue

            return None

        except Exception as e:
            logger.error(f"[EventBus] 消费事件异常: {e}")
            return None

    async def dispatch_event(self, event: Event, handler: Callable,
                           timeout: float = 5.0):
        """
        分发事件给处理器

        Args:
            event: 事件对象
            handler: 处理器函数
            timeout: 处理超时（秒）
        """
        handler_name = getattr(handler, '__name__', str(handler))

        try:
            self._processing = handler_name

            await asyncio.wait_for(
                handler(event),
                timeout=timeout
            )

        except asyncio.TimeoutError:
            logger.error(f"[EventBus] 事件处理器超时: {handler_name}")

        except Exception as e:
            logger.error(f"[EventBus] 事件处理器异常 ({handler_name}): {e}")

        finally:
            self._processing = None

    async def process_events(self, timeout: float = 1.0):
        """
        处理所有待处理事件（实时处理）

        Args:
            timeout: 单个事件处理超时
        """
        while True:
            event = await self.consume(timeout=0.1)

            if event is None:
                break

            # 查找订阅者
            subscribers = self._subscribers.get(event.event_type, [])

            for handler, _priority in subscribers:
                await self.dispatch_event(event, handler, timeout=timeout)


# ==================== 测试工具 ====================
async def test_event_bus():
    """事件总线测试"""
    print("=== 事件总线测试 ===\n")

    bus = EventBus(throttle_interval=0.5)

    # 测试1：基本订阅
    print("测试1：事件订阅和发布")

    async def on_task_started(event: TaskStartedEvent):
        print(f"  [Handler] 任务开始: {event.file_name}")

    async def on_task_progress(event: TaskProgressEvent):
        print(f"  [Handler] 进度: {event.percentage:.1f}%")

    async def on_task_completed(event: TaskCompletedEvent):
        print(f"  [Handler] 任务完成: {event.success}")

    bus.subscribe(TaskStartedEvent.__name__, on_task_started)
    bus.subscribe(TaskProgressEvent.__name__, on_task_progress)
    bus.subscribe(TaskCompletedEvent.__name__, on_task_completed)

    # 发布事件
    await bus.publish(TaskStartedEvent(
        request_id="task_001",
        file_size=1000000,
        file_name="test.zip"
    ))

    # 发布多个进度事件（测试节流）
    print("\n测试2：进度事件节流")
    for i in range(1, 11):
        await bus.publish(TaskProgressEvent(
            request_id="task_001",
            current=i * 100000,
            total=1000000,
            percentage=i * 10
        ))

        # 短暂延迟，但不足以触发时间条件
        await asyncio.sleep(0.05)

    # 发布完成事件
    await bus.publish(TaskCompletedEvent(
        request_id="task_001",
        file_path="/path/to/file",
        success=True
    ))

    # 处理队列中的事件
    print("\n处理事件队列...")
    await bus.process_events(timeout=5.0)

    print("\n=== 测试完成 ===")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(test_event_bus())
