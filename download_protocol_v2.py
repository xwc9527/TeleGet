"""
IPC消息协议v2 - Daemon和主进程间的消息定义

包含：
1. Request消息（主进程 → Daemon）
2. Response消息（Daemon → 主进程）
3. Event消息（Daemon → 主进程）
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
import time


# ==================== Request消息 ====================
@dataclass
class DownloadRequestV2:
    """下载请求 (主进程 → Daemon)"""
    request_id: str
    chat_id: int
    msg_id: int
    save_path: str
    account_id: str
    media_type: Optional[str] = None
    file_size: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'chat_id': self.chat_id,
            'msg_id': self.msg_id,
            'save_path': self.save_path,
            'account_id': self.account_id,
            'media_type': self.media_type,
            'file_size': self.file_size,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DownloadRequestV2':
        known_fields = {f for f in cls.__dataclass_fields__}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


@dataclass
class SwitchAccountRequest:
    """切换账号请求 (主进程 → Daemon)"""
    request_id: str
    account_id: str
    session_path: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'account_id': self.account_id,
            'session_path': self.session_path,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SwitchAccountRequest':
        return cls(**data)


@dataclass
class ShutdownRequest:
    """关闭请求 (主进程 → Daemon)"""
    request_id: str
    save_state: bool = True
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'save_state': self.save_state,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ShutdownRequest':
        return cls(**data)


# [FIX-CANCEL-2026-02-16] 取消下载请求（主进程 → Daemon）
@dataclass
class CancelDownloadRequest:
    """取消下载请求 (主进程 → Daemon)

    用途：
    - 通知Daemon停止指定request_id的下载任务
    - Daemon收到后：设置取消标志 → WorkerPool停止分片下载 → 保存checkpoint → 发送TaskCompletedEvent(cancelled)
    """
    request_id: str
    target_request_id: str  # 要取消的下载任务的 request_id
    save_checkpoint: bool = True  # 是否保存断点续传信息
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'target_request_id': self.target_request_id,
            'save_checkpoint': self.save_checkpoint,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CancelDownloadRequest':
        return cls(**data)


@dataclass
class HeartbeatRequest:
    """心跳请求 (主进程 → Daemon)"""
    request_id: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HeartbeatRequest':
        return cls(**data)


@dataclass
class ReloadSessionRequest:
    """重新加载Session请求 (主进程 → Daemon)"""
    request_id: str
    session_path: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'session_path': self.session_path,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReloadSessionRequest':
        return cls(**data)


@dataclass
class RequestEntityRequest:
    """请求实体信息 (Daemon → 主进程)"""
    request_id: str
    chat_id: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'chat_id': self.chat_id,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RequestEntityRequest':
        return cls(**data)


@dataclass
class SendEntityCacheRequest:
    """发送实体缓存 (主进程 → Daemon)"""
    request_id: str
    entities: List[Dict[str, Any]] = field(default_factory=list)
    version: float = field(default_factory=time.time)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'entities': self.entities,
            'version': self.version,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SendEntityCacheRequest':
        return cls(**data)


# ==================== Response消息 ====================
@dataclass
class DownloadResponseV2:
    """下载完成响应 (Daemon → 主进程)"""
    request_id: str
    status: str  # "success" / "failed" / "cancelled"
    file_path: Optional[str] = None
    file_size: int = 0
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'status': self.status,
            'file_path': self.file_path,
            'file_size': self.file_size,
            'error_message': self.error_message,
            'error_type': self.error_type,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DownloadResponseV2':
        return cls(**data)


@dataclass
class AckMessage:
    """确认消息 (双向)"""
    request_id: str
    original_message_id: str
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'original_message_id': self.original_message_id,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AckMessage':
        return cls(**data)


@dataclass
class HeartbeatResponse:
    """心跳响应 (Daemon → 主进程)"""
    request_id: str
    memory_mb: float = 0.0
    uptime_seconds: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'memory_mb': self.memory_mb,
            'uptime_seconds': self.uptime_seconds,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HeartbeatResponse':
        return cls(**data)


@dataclass
class GetEntityResponse:
    """实体信息响应 (主进程 → Daemon)"""
    request_id: str
    chat_id: int
    found: bool = False
    entity: Optional[Dict[str, Any]] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'chat_id': self.chat_id,
            'found': self.found,
            'entity': self.entity,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GetEntityResponse':
        return cls(**data)


# ==================== Event消息 ====================
@dataclass
class TaskProgressEvent:
    """任务进度事件 (Daemon → 主进程)"""
    request_id: str
    current: int
    total: int
    percentage: float = 0.0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'current': self.current,
            'total': self.total,
            'percentage': self.percentage,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskProgressEvent':
        return cls(**data)


@dataclass
class TaskCompletedEvent:
    """任务完成事件 (Daemon → 主进程)"""
    request_id: str
    success: bool
    file_path: Optional[str] = None
    file_size: int = 0
    error_message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    # [FIX-FILE-NAME-2026-02-04] 新增file_name字段
    # 修复 AttributeError: 'TaskCompletedEvent' object has no attribute 'file_name'
    # 原因：download_manager_v2.py:1565等多处访问event.file_name，但原定义缺失此字段
    file_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'success': self.success,
            'file_path': self.file_path,
            'file_size': self.file_size,
            'error_message': self.error_message,
            'timestamp': self.timestamp,
            'file_name': self.file_name,  # [FIX-FILE-NAME-2026-02-04] 序列化新增字段
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskCompletedEvent':
        return cls(**data)


@dataclass
class TaskStartedEvent:
    """任务开始事件 (Daemon → 主进程)"""
    request_id: str
    file_name: str
    file_size: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'file_name': self.file_name,
            'file_size': self.file_size,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskStartedEvent':
        return cls(**data)


@dataclass
class DaemonStatusEvent:
    """Daemon状态事件 (Daemon → 主进程)"""
    status: str  # "initializing" / "idle" / "downloading" / "shutting_down"
    memory_mb: float = 0.0
    cpu_percent: float = 0.0
    active_tasks: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status,
            'memory_mb': self.memory_mb,
            'cpu_percent': self.cpu_percent,
            'active_tasks': self.active_tasks,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DaemonStatusEvent':
        return cls(**data)


@dataclass
class InitializationCompleted:
    """初始化完成通知 (Daemon → 主进程)"""
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'InitializationCompleted':
        return cls(**data)


# [REFRESH-DIALOGS-FIX-2026-02-02] UI刷新事件（Daemon → 主进程）
# [ROOT-CAUSE] Daemon进程无法访问主进程的account_manager和db_service_ref全局引用
# [SOLUTION] Daemon通过IPC通知主进程刷新对话列表，而非直接调用refresh_dialogs_list()
@dataclass
class RefreshDialogsRequest:
    """刷新对话列表请求 (Daemon → 主进程)

    用途：
    - Daemon进程完成同步对话后，通知主进程刷新UI
    - 账号切换时，通知主进程清空旧对话列表

    规则：
    - 必须在主进程UI线程执行refresh_dialogs_list()
    - Daemon进程禁止直接调用refresh_dialogs_list()（会导致account_manager.get_current_account()=None）
    """
    request_id: str
    account_id: str  # 需要刷新的账号ID（用于验证）
    reason: str = ""  # 刷新原因（用于日志追踪）：sync_completed / account_switched / manual
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'account_id': self.account_id,
            'reason': self.reason,
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RefreshDialogsRequest':
        return cls(**data)


# ==================== 消息类型映射 ====================
MESSAGE_TYPES = {
    # Requests
    'DownloadRequestV2': DownloadRequestV2,
    'SwitchAccountRequest': SwitchAccountRequest,
    'ShutdownRequest': ShutdownRequest,
    'CancelDownloadRequest': CancelDownloadRequest,  # [FIX-CANCEL-2026-02-16]
    'HeartbeatRequest': HeartbeatRequest,
    'ReloadSessionRequest': ReloadSessionRequest,
    'RequestEntityRequest': RequestEntityRequest,
    'SendEntityCacheRequest': SendEntityCacheRequest,

    # Responses
    'DownloadResponseV2': DownloadResponseV2,
    'AckMessage': AckMessage,
    'HeartbeatResponse': HeartbeatResponse,
    'GetEntityResponse': GetEntityResponse,

    # Events
    'TaskProgressEvent': TaskProgressEvent,
    'TaskCompletedEvent': TaskCompletedEvent,
    'TaskStartedEvent': TaskStartedEvent,
    'DaemonStatusEvent': DaemonStatusEvent,
    'InitializationCompleted': InitializationCompleted,
    # [REFRESH-DIALOGS-FIX-2026-02-02] UI刷新事件
    'RefreshDialogsRequest': RefreshDialogsRequest,
}


def deserialize_message(message_type: str, data: Dict[str, Any]):
    """根据类型反序列化消息"""
    if message_type not in MESSAGE_TYPES:
        raise ValueError(f"未知消息类型: {message_type}")

    message_class = MESSAGE_TYPES[message_type]
    return message_class.from_dict(data)
