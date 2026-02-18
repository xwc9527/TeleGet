"""
IPC通信层 - 跨平台进程间通信
支持Unix Domain Socket (Linux/macOS/Windows 10+) 和 TCP Loopback (Windows fallback)

关键特性：
1. 自动检测平台，选择最优IPC方式
2. TLV消息帧格式，处理粘包/半包问题
3. ACK确认机制，保证可靠传输
4. 连接重试和断线重连
"""

import asyncio
import json
import os
import socket
import struct
import sys
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Callable, Dict, Any
import logging

logger = logging.getLogger(__name__)


# ==================== 消息类型枚举 ====================
class MessageType(Enum):
    """IPC消息类型"""
    # 请求类
    DOWNLOAD_REQUEST = 0x01
    SWITCH_ACCOUNT_REQUEST = 0x02
    SHUTDOWN_REQUEST = 0x03
    HEARTBEAT_REQUEST = 0x04
    RELOAD_SESSION_REQUEST = 0x05
    REQUEST_ENTITY_REQUEST = 0x06
    CANCEL_DOWNLOAD_REQUEST = 0x07  # [FIX-CANCEL-2026-02-16] 取消下载请求

    # 响应类
    DOWNLOAD_RESPONSE = 0x10
    ACK_MESSAGE = 0x11
    HEARTBEAT_RESPONSE = 0x12
    GET_ENTITY_RESPONSE = 0x13

    # 事件类
    TASK_PROGRESS_EVENT = 0x20
    TASK_COMPLETED_EVENT = 0x21
    TASK_STARTED_EVENT = 0x22
    DAEMON_STATUS_EVENT = 0x23

    # [REFRESH-DIALOGS-FIX-2026-02-02] UI刷新事件（Daemon → 主进程）
    REFRESH_DIALOGS_REQUEST = 0x24

    # 控制类
    INIT_COMPLETED = 0x30


# ==================== 基础消息类 ====================
@dataclass
class IPCMessage:
    """IPC消息基类"""
    message_id: str
    message_type: MessageType
    timestamp: float
    payload: Dict[str, Any]

    def to_bytes(self) -> bytes:
        """序列化为字节（TLV格式）"""
        # 构建payload的JSON
        payload_json = json.dumps({
            'message_id': self.message_id,
            'timestamp': self.timestamp,
            'payload': self.payload
        }, ensure_ascii=False).encode('utf-8')

        # TLV格式: [Type: 1 byte][Length: 4 bytes][Value: N bytes]
        type_byte = self.message_type.value.to_bytes(1, 'big')
        length_bytes = struct.pack('>I', len(payload_json))  # 大端序4字节

        return type_byte + length_bytes + payload_json

    @classmethod
    def from_bytes(cls, data: bytes) -> 'IPCMessage':
        """从字节反序列化（TLV格式）"""
        if len(data) < 5:
            raise ValueError(f"消息太短，无法解析: {len(data)} bytes")

        # 解析TLV
        type_value = int.from_bytes(data[0:1], 'big')
        length = struct.unpack('>I', data[1:5])[0]

        if len(data) < 5 + length:
            raise ValueError(f"消息长度不足: 期望{5+length}, 实际{len(data)}")

        payload_json = data[5:5+length].decode('utf-8')
        payload_data = json.loads(payload_json)

        # 查找对应的MessageType
        message_type = MessageType(type_value)

        return cls(
            message_id=payload_data['message_id'],
            message_type=message_type,
            timestamp=payload_data['timestamp'],
            payload=payload_data['payload']
        )


# ==================== Socket类型检测 ====================
def detect_socket_type() -> str:
    """根据操作系统自动选择IPC方式"""
    if sys.platform == 'win32':
        # Windows平台
        if sys.version_info >= (3, 7) and os.name == 'nt':
            # Python 3.7+ 且 Windows NT
            try:
                # 尝试创建Unix Domain Socket
                test_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                test_socket.close()
                logger.info("[IPC] Windows 10+ 检测到Unix Socket支持")
                return "unix"
            except (OSError, AttributeError):
                # 不支持，降级到TCP Loopback
                logger.info("[IPC] Windows降级到TCP Loopback模式")
                return "tcp_loopback"
        else:
            # 旧版本Windows
            logger.info("[IPC] 旧版Windows使用TCP Loopback")
            return "tcp_loopback"
    else:
        # Linux/macOS，优先Unix Domain Socket
        logger.info(f"[IPC] {sys.platform}平台使用Unix Socket")
        return "unix"


# ==================== IPC通道 ====================
class IPCChannel:
    """IPC通道 - 支持Unix Socket和TCP Loopback"""

    def __init__(self, socket_path: Optional[str] = None,
                 tcp_port: Optional[int] = None,
                 socket_type: Optional[str] = None):
        """
        初始化IPC通道

        Args:
            socket_path: Unix Socket路径（unix模式）
            tcp_port: TCP端口（tcp_loopback模式）
            socket_type: 强制指定类型（"unix"或"tcp_loopback"），None则自动检测
        """
        self.socket_type = socket_type or detect_socket_type()
        self.socket_path = socket_path
        self.tcp_port = tcp_port or 0  # 0表示自动分配

        self._socket: Optional[socket.socket] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._connected = False

        # ACK等待队列
        self._pending_acks: Dict[str, asyncio.Future] = {}

        # 消息处理器
        self._message_handlers: Dict[MessageType, Callable] = {}

        # 接收缓冲区（处理半包）
        self._recv_buffer = b''

    async def connect(self, is_server: bool = False):
        """
        建立连接

        Args:
            is_server: True表示作为服务端监听，False表示作为客户端连接
        """
        logger.info(
            f"[IPC] connect() start: is_server={is_server}, "
            f"socket_type={self.socket_type}, "
            f"socket_path={self.socket_path}, tcp_port={self.tcp_port}"
        )
        if is_server:
            await self._start_server()
        else:
            await self._start_client()

        self._connected = True
        logger.info(f"[IPC] 连接建立成功 ({self.socket_type})")

    def is_ready(self) -> bool:
        """
        检查 IPC 是否就绪（可发送/接收消息）

        [FIX-2026-02-01] 解决 Daemon 时序竞争问题

        【问题分析】
        原代码中，Daemon 启动流程存在时序竞争：
        1. Daemon 调用 ipc.connect(is_server=True) 启动服务端
        2. connect() 设置 _connected=True 后立即返回
        3. Daemon 立即尝试发送初始化完成消息
        4. 但客户端（主进程）的连接请求还在途中
        5. _writer 仍未被赋值（只有客户端连接时才赋值）
        6. send() 检查 if not self._writer 时失败

        【根本原因】
        _connected 标志有歧义：
        - 对客户端：_connected=True 且 _writer!=None → 连接完成
        - 对服务端：_connected=True，但 _writer 还是 None（等待客户端）

        【解决方案】
        添加 is_ready() 方法，同时检查：
        - _connected: 服务端已启动或客户端已连接
        - _writer: 有实际的流写入对象（客户端已连接）

        【适用场景】
        - 服务端（Daemon）：is_ready() 等于 "客户端已连接"
        - 客户端（主进程）：is_ready() 等于 "连接成功"

        Returns:
            True: IPC通道就绪，可以发送/接收消息
            False: IPC通道未就绪

        示例：
            # Daemon 端检查
            if ipc.is_ready():
                await ipc.send(message)

            # 主进程端使用（通常不需要，connect()后就就绪）
            if ipc.is_ready():
                await ipc.send_with_ack(message)
        """
        # 必须同时满足：
        # 1. 连接标志为True（服务启动或客户端已连接）
        # 2. _writer不为None（有实际的流对象）
        return self._connected and self._writer is not None

    async def wait_ready(self, timeout: float = 10.0):
        """
        等待 IPC 通道就绪

        [FIX-2026-02-01] Daemon 启动后需要等待客户端连接

        【应用场景】
        Daemon 核心 run() 方法：
        1. 初始化 session 和连接池
        2. 调用 await self.ipc.wait_ready()
        3. 等待 is_ready() 返回 True
        4. 然后发送初始化完成消息

        Args:
            timeout: 超时时间（秒），默认10秒

        Raises:
            TimeoutError: 等待超时，IPC 未就绪

        用法：
            # Daemon 端
            try:
                await self.ipc.wait_ready(timeout=10.0)
                logger.info("IPC 就绪，可以发送消息")
            except TimeoutError as e:
                logger.error(f"IPC 就绪超时: {e}")

            # 客户端通常不需要调用（connect 后就绪）
        """
        import asyncio

        logger.info(f"[IPC-WaitReady] 等待通道就绪（超时={timeout}s）...")

        start_time = asyncio.get_event_loop().time()
        check_count = 0

        while not self.is_ready():
            # 检查超时
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout:
                raise TimeoutError(
                    f"IPC通道未就绪超时（{timeout}秒）"
                    f" [_connected={self._connected}, "
                    f"_writer={'None' if self._writer is None else 'exists'}]"
                )

            # 短暂等待
            await asyncio.sleep(0.1)
            check_count += 1

            # 每10次检查（1秒）打印一次日志
            if check_count % 10 == 0:
                logger.debug(
                    f"[IPC-WaitReady] 等待中... "
                    f"({elapsed:.1f}s, "
                    f"_connected={self._connected}, "
                    f"_writer={'None' if self._writer is None else 'exists'})"
                )

        elapsed = asyncio.get_event_loop().time() - start_time
        logger.info(
            f"[IPC-WaitReady] IPC通道已就绪 "
            f"(耗时={elapsed:.2f}s, "
            f"_connected={self._connected}, "
            f"_writer={'exists'})"
        )

    async def _start_server(self):
        """启动服务端（Daemon侧）"""
        if self.socket_type == "unix":
            # Unix Socket服务端
            if os.path.exists(self.socket_path):
                os.remove(self.socket_path)

            server = await asyncio.start_unix_server(
                self._handle_client_connection,
                path=self.socket_path
            )
            logger.info(f"[IPC] Unix Server启动: {self.socket_path}")

        else:
            # TCP Loopback服务端
            server = await asyncio.start_server(
                self._handle_client_connection,
                host='127.0.0.1',
                port=self.tcp_port
            )

            # 获取实际端口（如果是自动分配）
            if self.tcp_port == 0:
                self.tcp_port = server.sockets[0].getsockname()[1]

            logger.info(f"[IPC] TCP Server启动: 127.0.0.1:{self.tcp_port}")

        self._server = server
        logger.info(
            f"[IPC] Server ready: socket_type={self.socket_type}, "
            f"socket_path={self.socket_path}, tcp_port={self.tcp_port}"
        )

    async def _start_client(self):
        """启动客户端（主进程侧）"""
        max_retries = 5
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                if self.socket_type == "unix":
                    # Unix Socket客户端
                    self._reader, self._writer = await asyncio.open_unix_connection(
                        path=self.socket_path
                    )
                else:
                    # TCP Loopback客户端
                    self._reader, self._writer = await asyncio.open_connection(
                        host='127.0.0.1',
                        port=self.tcp_port
                    )

                logger.info(f"[IPC] 客户端连接成功 (尝试{attempt+1}/{max_retries})")
                return

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"[IPC] 连接失败，{retry_delay}秒后重试: {e}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 1.5  # 指数退避
                else:
                    logger.error(f"[IPC] 连接失败，达到最大重试次数")
                    raise

    async def _handle_client_connection(self, reader: asyncio.StreamReader,
                                       writer: asyncio.StreamWriter):
        """处理客户端连接（服务端侧）"""
        self._reader = reader
        self._writer = writer
        logger.info("[IPC] 客户端已连接 (server accepted)")

    async def send(self, message: IPCMessage):
        """发送消息（不等待ACK）"""
        if not self._connected or not self._writer:
            logger.error(
                f"[IPC] send() failed: connected={self._connected}, writer={'set' if self._writer else 'None'}"
            )
            raise ConnectionError("IPC通道未连接")

        try:
            data = message.to_bytes()
            self._writer.write(data)
            await self._writer.drain()

            logger.info(
                f"[IPC] 发送消息: type={message.message_type.name}, id={message.message_id}"
            )
        except Exception as e:
            logger.error(f"[IPC] 发送消息失败: {e}")
            self._connected = False
            raise

    async def send_with_ack(self, message: IPCMessage, timeout: float = 5.0) -> bool:
        """
        发送消息并等待ACK确认

        Returns:
            True表示收到ACK，False表示超时
        """
        # 创建ACK等待Future
        ack_future = asyncio.Future()
        self._pending_acks[message.message_id] = ack_future
        logger.info(
            f"[IPC] send_with_ack start: id={message.message_id}, "
            f"type={message.message_type.name}, timeout={timeout}s, "
            f"pending={len(self._pending_acks)}"
        )

        try:
            # 发送消息
            await self.send(message)

            # 等待ACK（带超时）
            await asyncio.wait_for(ack_future, timeout=timeout)
            return True

        except asyncio.TimeoutError:
            logger.warning(
                f"[IPC] ACK超时: {message.message_id}, "
                f"pending={len(self._pending_acks)}, "
                f"connected={self._connected}, "
                f"writer={'set' if self._writer else 'None'}"
            )
            return False

        finally:
            # 清理Future
            self._pending_acks.pop(message.message_id, None)

    async def receive(self, timeout: float = 1.0) -> Optional[IPCMessage]:
        """
        接收消息（非阻塞，带超时）

        Returns:
            IPCMessage或None（超时）
        """
        if not self._connected or not self._reader:
            logger.error(
                f"[IPC] receive() failed: connected={self._connected}, reader={'set' if self._reader else 'None'}"
            )
            raise ConnectionError("IPC通道未连接")

        try:
            # 读取TLV头部（5字节）
            while len(self._recv_buffer) < 5:
                chunk = await asyncio.wait_for(
                    self._reader.read(4096),
                    timeout=timeout
                )

                if not chunk:
                    # 连接关闭
                    self._connected = False
                    return None

                self._recv_buffer += chunk

            # 解析长度
            type_byte = self._recv_buffer[0:1]
            length = struct.unpack('>I', self._recv_buffer[1:5])[0]

            # 读取完整消息体
            total_length = 5 + length
            while len(self._recv_buffer) < total_length:
                chunk = await asyncio.wait_for(
                    self._reader.read(4096),
                    timeout=timeout
                )

                if not chunk:
                    self._connected = False
                    return None

                self._recv_buffer += chunk

            # 提取完整消息
            message_data = self._recv_buffer[:total_length]
            self._recv_buffer = self._recv_buffer[total_length:]

            # 反序列化
            message = IPCMessage.from_bytes(message_data)

            logger.info(
                f"[IPC] 接收消息: type={message.message_type.name}, id={message.message_id}"
            )

            # 处理ACK消息
            if message.message_type == MessageType.ACK_MESSAGE:
                original_id = message.payload.get('original_message_id')
                if original_id in self._pending_acks:
                    self._pending_acks[original_id].set_result(True)
                    logger.info(
                        f"[IPC] ACK处理完成: original_id={original_id}, "
                        f"pending={len(self._pending_acks)}"
                    )

            return message

        except asyncio.TimeoutError:
            return None

        except Exception as e:
            logger.error(f"[IPC] 接收消息异常: {e}")
            self._connected = False
            raise

    async def send_ack(self, original_message_id: str):
        """发送ACK确认"""
        ack_message = IPCMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.ACK_MESSAGE,
            timestamp=time.time(),
            payload={'original_message_id': original_message_id}
        )
        logger.info(f"[IPC] 发送ACK: original_id={original_message_id}")
        await self.send(ack_message)

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected

    async def close(self):
        """关闭连接"""
        self._connected = False

        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception as e:
                logger.warning(f"[IPC] 关闭写入流异常: {e}")

        if hasattr(self, '_server') and self._server:
            self._server.close()
            await self._server.wait_closed()

        # 清理Unix Socket文件
        if self.socket_type == "unix" and self.socket_path:
            if os.path.exists(self.socket_path):
                try:
                    os.remove(self.socket_path)
                except Exception as e:
                    logger.warning(f"[IPC] 删除socket文件失败: {e}")

        logger.info("[IPC] 连接已关闭")


# ==================== 测试工具 ====================
async def test_ipc():
    """IPC通信层测试"""
    import tempfile

    print("=== IPC通信层测试 ===\n")

    socket_type = detect_socket_type()
    print(f"检测到IPC类型: {socket_type}\n")

    if socket_type == "unix":
        # Unix Socket测试
        socket_path = os.path.join(tempfile.gettempdir(), f"test_ipc_{os.getpid()}.sock")

        async def server_task():
            server = IPCChannel(socket_path=socket_path)
            await server.connect(is_server=True)

            # 接收消息
            msg = await server.receive(timeout=5.0)
            print(f"[Server] 收到消息: {msg.payload}")

            # 发送ACK
            await server.send_ack(msg.message_id)

            await server.close()

        async def client_task():
            await asyncio.sleep(0.5)  # 等待服务端启动

            client = IPCChannel(socket_path=socket_path)
            await client.connect(is_server=False)

            # 发送消息
            test_msg = IPCMessage(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.DOWNLOAD_REQUEST,
                timestamp=time.time(),
                payload={'chat_id': 123, 'msg_id': 456}
            )

            print(f"[Client] 发送消息: {test_msg.payload}")
            success = await client.send_with_ack(test_msg, timeout=3.0)
            print(f"[Client] ACK状态: {'成功' if success else '超时'}")

            await client.close()

        # 并发运行
        await asyncio.gather(server_task(), client_task())

    else:
        # TCP Loopback测试
        tcp_port = 0  # 自动分配

        async def server_task():
            server = IPCChannel(tcp_port=tcp_port, socket_type="tcp_loopback")
            await server.connect(is_server=True)

            print(f"[Server] 监听端口: {server.tcp_port}")

            # 接收消息
            msg = await server.receive(timeout=5.0)
            print(f"[Server] 收到消息: {msg.payload}")

            # 发送ACK
            await server.send_ack(msg.message_id)

            await server.close()

        async def client_task():
            await asyncio.sleep(0.5)

            # 注意：需要知道服务端的实际端口
            # 生产环境中端口会通过配置文件共享
            client = IPCChannel(tcp_port=9999, socket_type="tcp_loopback")
            await client.connect(is_server=False)

            test_msg = IPCMessage(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.DOWNLOAD_REQUEST,
                timestamp=time.time(),
                payload={'chat_id': 123, 'msg_id': 456}
            )

            print(f"[Client] 发送消息: {test_msg.payload}")
            success = await client.send_with_ack(test_msg, timeout=3.0)
            print(f"[Client] ACK状态: {'成功' if success else '超时'}")

            await client.close()

        await asyncio.gather(server_task(), client_task())

    print("\n=== 测试完成 ===")


if __name__ == '__main__':
    # 测试IPC通信
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_ipc())
