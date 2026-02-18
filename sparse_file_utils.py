"""
稀疏文件工具模块

提供 Windows NTFS 稀疏文件创建和分片写入功能。
用于并发下载时的文件预分配，避免临时文件合并。
"""

import os
import sys
import ctypes
import logging
from ctypes import wintypes
from typing import Optional

logger = logging.getLogger(__name__)


# Windows API 常量
FSCTL_SET_SPARSE = 0x000900C4
FILE_ATTRIBUTE_SPARSE_FILE = 0x200


def is_windows() -> bool:
    """检查是否为 Windows 系统"""
    return sys.platform == 'win32'


def create_sparse_file(file_path: str, size: int) -> bool:
    """
    创建稀疏文件并设置逻辑大小

    Args:
        file_path: 文件路径
        size: 文件逻辑大小（字节）

    Returns:
        True: 成功创建稀疏文件
        False: 失败（但仍会创建普通预分配文件）
    """
    logger.info(f"[SPARSE] 创建稀疏文件: {file_path}, 大小: {size} 字节")

    # 确保目录存在
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)

    # [NEW-DISK-CHECK-2026-01-28] 磁盘空间预检查
    # ┌────────────────────────────────────────────────────────────────────────────┐
    # │ [FIX-SPARSE-DISK-CHECK-2026-02-08] 稀疏文件空间检查修正                   │
    # │                                                                            │
    # │ 【Bug说明】                                                                │
    # │   原代码对所有文件统一要求 120% 空间（size * 1.2）                         │
    # │   但 Windows NTFS 稀疏文件不会预分配物理空间：                             │
    # │   - 逻辑大小：由 SetEndOfFile 设置（如 10GB）                             │
    # │   - 物理占用：仅已写入数据的实际大小（按需增长）                           │
    # │   - 元数据开销：约 0.1%（NTFS MFT + 稀疏映射表）                          │
    # │                                                                            │
    # │ 【原Bug影响】                                                              │
    # │   下载 10GB 文件要求 12GB 可用空间，实际只需要 10GB + 少量元数据           │
    # │   磁盘剩余 11GB 时误拒本可完成的下载                                       │
    # │                                                                            │
    # │ 【修复方案】                                                               │
    # │   Windows 平台（将创建稀疏文件）：要求 102% 空间                           │
    # │   非 Windows 平台（truncate 预分配）：保留 110% 空间                       │
    # │   两者都保留安全裕度，但稀疏模式大幅降低要求                               │
    # │                                                                            │
    # │ 【修复日期】2026-02-08                                                     │
    # └────────────────────────────────────────────────────────────────────────────┘
    import shutil
    stat = shutil.disk_usage(dir_path)

    # [DISABLED-BUG] required = int(size * 1.2)  # 原代码：统一 120% 空间，对稀疏文件过于保守
    if is_windows():
        # 稀疏文件：不预分配物理空间，只需文件实际大小 + 少量元数据开销
        required = int(size * 1.02)  # 102%（2% 覆盖 NTFS 元数据 + 稀疏映射表）
    else:
        # 非稀疏（truncate 预分配）：需要完整的物理空间 + 安全裕度
        required = int(size * 1.10)  # 110%（从 120% 降至 110%，原 120% 也偏保守）

    logger.info(f"[SPARSE] [FIX-DISK-CHECK] 磁盘检查: 文件={size/(1024*1024):.1f}MB, "
          f"要求={required/(1024*1024):.1f}MB, 可用={stat.free/(1024*1024):.1f}MB, "
          f"模式={'sparse' if is_windows() else 'preallocate'}")

    if stat.free < required:
        raise OSError(
            f"磁盘空间不足：需要 {required/1024/1024:.1f} MB，"
            f"可用 {stat.free/1024/1024:.1f} MB"
        )

    if is_windows():
        return _create_sparse_file_windows(file_path, size)
    else:
        # 非 Windows：使用 truncate 预分配
        return _create_preallocated_file(file_path, size)


def _create_sparse_file_windows(file_path: str, size: int) -> bool:
    """
    Windows 平台创建稀疏文件

    使用 DeviceIoControl + FSCTL_SET_SPARSE 设置稀疏属性
    """
    try:
        kernel32 = ctypes.windll.kernel32

        # 创建文件
        GENERIC_WRITE = 0x40000000
        GENERIC_READ = 0x80000000
        FILE_SHARE_READ = 0x00000001
        FILE_SHARE_WRITE = 0x00000002
        CREATE_ALWAYS = 2
        FILE_ATTRIBUTE_NORMAL = 0x80

        handle = kernel32.CreateFileW(
            file_path,
            GENERIC_WRITE | GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            None,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            None
        )

        if handle == -1:
            logger.error(f"[SPARSE] CreateFileW 失败")
            return _create_preallocated_file(file_path, size)

        try:
            # 设置稀疏属性
            bytes_returned = wintypes.DWORD()
            result = kernel32.DeviceIoControl(
                handle,
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                ctypes.byref(bytes_returned),
                None
            )

            if not result:
                logger.warning(f"[SPARSE] DeviceIoControl FSCTL_SET_SPARSE 失败，回退到预分配")
                kernel32.CloseHandle(handle)
                return _create_preallocated_file(file_path, size)

            # 设置文件大小（稀疏模式下不会实际分配磁盘空间）
            # 移动文件指针到目标位置
            kernel32.SetFilePointerEx(
                handle,
                ctypes.c_longlong(size),
                None,
                0  # FILE_BEGIN
            )

            # 设置文件结束位置
            kernel32.SetEndOfFile(handle)

            logger.info(f"[SPARSE] 稀疏文件创建成功")
            return True

        finally:
            kernel32.CloseHandle(handle)

    except Exception as e:
        logger.warning(f"[SPARSE] 创建稀疏文件异常: {e}，回退到预分配")
        return _create_preallocated_file(file_path, size)


def _create_preallocated_file(file_path: str, size: int) -> bool:
    """
    创建预分配文件（非稀疏，用于不支持稀疏文件的情况）

    使用 truncate 设置文件大小
    """
    try:
        with open(file_path, 'wb') as f:
            f.truncate(size)

        logger.info(f"[SPARSE] 预分配文件创建成功（非稀疏）")
        return False  # 返回 False 表示不是真正的稀疏文件

    except Exception as e:
        logger.error(f"[SPARSE] 预分配文件创建失败: {e}")
        raise


def is_sparse_file(file_path: str) -> bool:
    """
    检查文件是否为稀疏文件

    Args:
        file_path: 文件路径

    Returns:
        True: 是稀疏文件
        False: 不是稀疏文件
    """
    if not is_windows():
        return False

    if not os.path.exists(file_path):
        return False

    try:
        attrs = ctypes.windll.kernel32.GetFileAttributesW(file_path)
        if attrs == -1:
            return False

        return bool(attrs & FILE_ATTRIBUTE_SPARSE_FILE)

    except Exception:
        return False


class ShardWriter:
    """
    分片写入器

    每个实例维护一个独立的文件句柄，支持并发写入到同一文件的不同位置。
    """

    def __init__(self, file_path: str, offset: int, length: int, shard_id: int = 0):
        """
        初始化分片写入器

        Args:
            file_path: 文件路径
            offset: 此分片的起始偏移
            length: 此分片的长度
            shard_id: 分片编号（用于日志）
        """
        self.file_path = file_path
        self.offset = offset
        self.length = length
        self.shard_id = shard_id

        self._file_handle: Optional[object] = None
        self._bytes_written = 0

        # [NEW-FSYNC-THROTTLE-2026-02-01] fsync 节流状态追踪
        # [说明] 用于periodic模式，追踪上次fsync时间和字节数
        self._last_fsync_time = None  # 上次 fsync 的时间戳（延迟初始化到open）
        self._bytes_since_last_fsync = 0  # 自上次 fsync 以来写入的字节数
        self._fsync_mode = None  # fsync 模式（延迟加载配置）
        self._fsync_interval_mb = None  # MB 间隔
        self._fsync_interval_seconds = None  # 时间间隔

    def open(self):
        """打开文件句柄"""
        # 使用 r+b 模式打开已存在的文件（稀疏文件应该已经创建）
        self._file_handle = open(self.file_path, 'r+b')
        self._file_handle.seek(self.offset)
        logger.debug(f"[SHARD-{self.shard_id}] 已打开，偏移: {self.offset}, 长度: {self.length}")

        # [NEW-FSYNC-THROTTLE-2026-02-01] 初始化 fsync 节流配置
        # [说明] 延迟加载配置，避免在模块导入时触发配置读取
        import time
        from download_config import get_config_value
        self._last_fsync_time = time.time()
        self._bytes_since_last_fsync = 0
        self._fsync_mode = get_config_value("fsync_mode", "periodic")
        self._fsync_interval_mb = get_config_value("fsync_interval_mb", 50)
        self._fsync_interval_seconds = get_config_value("fsync_interval_seconds", 30)
        logger.debug(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] 模式: {self._fsync_mode}, 间隔: {self._fsync_interval_mb}MB 或 {self._fsync_interval_seconds}秒")

    def write(self, data: bytes) -> int:
        """
        写入数据

        Args:
            data: 要写入的数据

        Returns:
            实际写入的字节数
        """
        if not self._file_handle:
            raise RuntimeError("文件句柄未打开")

        written = self._file_handle.write(data)
        self._bytes_written += written

        # [NEW-FSYNC-THROTTLE-2026-02-01] Periodic 模式的 fsync 节流逻辑
        # [说明] 在写入过程中定期fsync，而不是每个Part关闭时才fsync
        # [性能优化] 1GB文件：2048次fsync → 约20次fsync（HDD节省20-100秒）
        if self._fsync_mode == "periodic":
            self._bytes_since_last_fsync += written
            import time
            current_time = time.time()

            # 检查是否达到 fsync 条件（MB间隔 或 时间间隔）
            bytes_threshold = self._fsync_interval_mb * 1024 * 1024
            time_threshold = self._fsync_interval_seconds

            should_fsync = (
                self._bytes_since_last_fsync >= bytes_threshold or
                (current_time - self._last_fsync_time) >= time_threshold
            )

            if should_fsync:
                try:
                    self._file_handle.flush()
                    os.fsync(self._file_handle.fileno())
                    # 重置计数器
                    self._bytes_since_last_fsync = 0
                    self._last_fsync_time = current_time
                    logger.debug(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] Periodic fsync 执行（已写入 {self._bytes_written / (1024*1024):.2f} MB）")
                except Exception as e:
                    # [安全保障] fsync失败不应中断下载流程，仅记录日志
                    logger.warning(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] 警告: fsync 失败 - {e}")

        return written

    def close(self):
        """关闭文件句柄"""
        if self._file_handle:
            self._file_handle.flush()

            # [NEW-FSYNC-THROTTLE-2026-02-01] 根据 fsync_mode 决定关闭时行为
            # - always: 每次关闭都执行 fsync（原有行为，保持兼容）
            # - periodic: 关闭时执行最终 fsync（保障数据完整性）
            # - end: 不在 Shard 级别 fsync（由调用方在文件完成后统一 fsync）
            #
            # [DEPRECATED-2026-02-01] 原有的无条件 fsync 代码已被节流逻辑替代
            # [原代码] os.fsync(self._file_handle.fileno())  # 每个Part都fsync
            # [问题] 1GB文件=2048个Part=2048次fsync → 严重IO瓶颈

            fsync_mode = self._fsync_mode if self._fsync_mode else "always"

            if fsync_mode == "always":
                # [原有行为] 保持兼容：每个Part关闭时都fsync
                os.fsync(self._file_handle.fileno())
                logger.debug(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] Always 模式: 关闭时 fsync")
            elif fsync_mode == "periodic":
                # ┌────────────────────────────────────────────────────────────────────────────┐
                # │ [DISABLED-PERIODIC-CLOSE-FSYNC-2026-02-08] 屏蔽 periodic 模式关闭时 fsync │
                # │                                                                            │
                # │ 【Bug说明】                                                                │
                # │   periodic 模式的 write() 方法已按 50MB/30秒 间隔执行 fsync               │
                # │   但 close() 又对每个 Part 关闭时额外执行一次 fsync                       │
                # │   1GB 文件 = 1024 个 Part = 1024 次额外 fsync，完全抵消 periodic 优化     │
                # │                                                                            │
                # │ 【性能影响】                                                               │
                # │   HDD: 每次 fsync 5-50ms × 1024 = 5-50 秒额外开销                        │
                # │   SSD: 每次 fsync 0.1-1ms × 1024 = 0.1-1 秒（影响较小）                  │
                # │                                                                            │
                # │ 【修复方案】                                                               │
                # │   periodic 模式 close() 跳过 fsync（与 end 模式行为一致）                 │
                # │   数据安全保障：                                                           │
                # │   1. write() 中已按间隔 fsync（50MB 或 30秒）                             │
                # │   2. 文件下载完成后由 final_fsync_file() 统一 fsync                       │
                # │   3. OS 文件缓存 + 断点续传提供兜底保障                                   │
                # │                                                                            │
                # │ 【屏蔽日期】2026-02-08                                                     │
                # └────────────────────────────────────────────────────────────────────────────┘
                # [DISABLED-BUG] os.fsync(self._file_handle.fileno())
                # [DISABLED-BUG] print(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] Periodic 模式: 关闭时最终 fsync")
                logger.debug(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] Periodic 模式: 关闭时跳过 fsync（由 write() 间隔 fsync + final_fsync_file() 保障）")
            elif fsync_mode == "end":
                # [最高性能] 不在Shard级别fsync，由调用方统一处理
                logger.debug(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] End 模式: 跳过 fsync（由调用方处理）")
            else:
                # [兜底] 未知模式按always处理
                os.fsync(self._file_handle.fileno())
                logger.warning(f"[SHARD-{self.shard_id}][FSYNC-THROTTLE] 未知模式({fsync_mode}): 兜底 fsync")

            self._file_handle.close()
            self._file_handle = None
            logger.debug(f"[SHARD-{self.shard_id}] 已关闭，共写入 {self._bytes_written} 字节")

    @property
    def bytes_written(self) -> int:
        """已写入的字节数"""
        return self._bytes_written

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def calculate_parts(total_size: int, part_size: int = 512 * 1024) -> list:
    """
    计算固定大小的分块列表（Worker Pool 模式使用）

    与 calculate_shards() 的区别：
    - calculate_shards(): 动态分片，分片数 = Worker 数，每个分片可能很大（如 17MB）
    - calculate_parts(): 固定分块，Part 数 = ceil(total_size / part_size)，每个 Part 大小由参数指定

    Worker Pool 模式的优势：
    - 固定大小的 Part 更容易管理和追踪
    - 1MB 的 Part 符合 Telegram 官方最佳实践（减少握手次数）
    - 固定数量的 Worker（12或19个）从队列消费 Part，内存占用恒定

    [NOTE-2026-01-30] Part 大小实际配置：
    - 默认参数 512KB 仅为保底值
    - 实际调用时传入 1MB (1024*1024)，见 download_config.py:76
    - 1MB 避免跨越 1MB 边界，配合 download_pool.py:381-388 边界保护逻辑

    Args:
        total_size: 文件总大小（字节）
        part_size: 每块大小（默认 512KB，实际使用 1MB，必须 1KB 对齐）

    Returns:
        分块列表 [(offset, length), ...]

    Raises:
        ValueError: 如果 part_size 不是 1KB 的倍数
    """
    if part_size % 1024 != 0:
        raise ValueError(f"part_size={part_size} 必须是 1KB 的倍数")

    KB = 1024
    parts = []
    offset = 0

    while offset < total_size:
        remaining = total_size - offset
        length = min(part_size, remaining)

        # 最后一块可能不对齐，记录原始长度（下载时由 Worker Pool 处理对齐）
        parts.append((offset, length))

        offset += length

    logger.info(f"[PARTS] 文件 {total_size} bytes 分成 {len(parts)} 个 parts (每个 {part_size // KB} KB)")
    return parts


def verify_file_integrity(file_path: str, expected_size: int) -> bool:
    """
    验证文件完整性

    Args:
        file_path: 文件路径
        expected_size: 期望的文件大小

    Returns:
        True: 文件完整
        False: 文件不完整或不存在
    """
    if not os.path.exists(file_path):
        return False

    actual_size = os.path.getsize(file_path)
    return actual_size == expected_size


# [NEW-FSYNC-THROTTLE-2026-02-01] End 模式全局 fsync 函数
def final_fsync_file(file_path: str) -> bool:
    """
    [P0优化] 文件下载完成后执行全局 fsync

    用于 fsync_mode="end" 时，在所有 Shard 写入完成后统一执行 fsync。
    这是性能最高的模式（仅1次fsync），适合SSD或配备UPS的环境。

    Args:
        file_path: 文件路径

    Returns:
        True: fsync 成功
        False: fsync 失败

    Example:
        >>> # 所有 Part 下载完成后
        >>> success = final_fsync_file("/path/to/downloaded/file.mp4")
        >>> if success:
        >>>     print("文件已安全写入磁盘")
    """
    if not os.path.exists(file_path):
        logger.error(f"[FSYNC-THROTTLE] 错误: 文件不存在 - {file_path}")
        return False

    try:
        # 打开文件并执行 fsync
        with open(file_path, 'r+b') as f:
            f.flush()
            os.fsync(f.fileno())

        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"[FSYNC-THROTTLE] 全局 fsync 成功: {file_path} ({file_size_mb:.2f} MB)")
        return True

    except Exception as e:
        logger.error(f"[FSYNC-THROTTLE] 全局 fsync 失败: {e}")
        return False
