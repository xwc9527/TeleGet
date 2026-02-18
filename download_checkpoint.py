"""
断点续传管理器 - 基于SQLite + WAL

功能：
1. 保存下载检查点到SQLite数据库
2. 支持从检查点恢复未完成的任务
3. 定期清理过期检查点
4. WAL模式保证高效和可靠性

数据库设计：
checkpoints 表
├─ request_id (PRIMARY KEY) - 任务ID
├─ account_id - 账号ID
├─ chat_id - 对话ID
├─ msg_id - 消息ID
├─ file_path - 保存路径
├─ file_name - 文件名
├─ file_size - 文件大小（字节）
├─ downloaded - 已下载大小
├─ percentage - 进度百分比
├─ state - 任务状态（DOWNLOADING/PAUSED/FAILED）
├─ error_message - 错误信息（如果失败）
├─ created_at - 创建时间
├─ updated_at - 最后更新时间
├─ expires_at - 过期时间（7天后自动删除）
└─ extra_data - JSON格式的扩展数据

性能优化：
- WAL模式：写入只追加，读写不互斥，高吞吐量
- 批量提交：每30秒一次事务，减少磁盘I/O
- 索引：account_id和expires_at索引加速查询
- 定期清理：每小时删除过期记录
"""

import asyncio
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


# ==================== 数据模型 ====================
@dataclass
class CheckpointRecord:
    """检查点记录"""
    request_id: str
    account_id: str
    chat_id: int
    msg_id: int
    file_path: str
    file_name: str
    file_size: int
    downloaded: int
    percentage: float
    state: str  # DOWNLOADING/PAUSED/FAILED
    error_message: Optional[str] = None
    created_at: Optional[float] = None
    updated_at: Optional[float] = None
    expires_at: Optional[float] = None
    extra_data: Optional[str] = None  # JSON格式


# ==================== SQLite检查点数据库 ====================
class CheckpointDB:
    """
    检查点数据库（SQLite + WAL）

    WAL (Write-Ahead Logging) 模式的优势：
    1. 读写并发：读不会被写阻塞
    2. 高吞吐量：多个写者可以排队而不是等待
    3. 可靠性：事务崩溃恢复更安全
    4. 性能：减少磁盘同步次数
    """

    def __init__(self, db_path: str = 'data/checkpoints.db'):
        """
        初始化数据库

        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        self.conn = None
        self._lock = asyncio.Lock()

        logger.info(f"[CheckpointDB] 初始化: {db_path}")

    async def init(self):
        """初始化数据库和表"""
        # 确保目录存在
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        # 打开数据库
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)

        # 启用WAL模式
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")  # 比FULL更快，仍然安全
        self.conn.execute("PRAGMA cache_size=10000")  # 增加缓存
        self.conn.execute("PRAGMA temp_store=MEMORY")  # 临时数据用内存

        # 创建表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                request_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                msg_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT,
                file_size INTEGER,
                downloaded INTEGER DEFAULT 0,
                percentage REAL DEFAULT 0.0,
                state TEXT DEFAULT 'DOWNLOADING',
                error_message TEXT,
                created_at REAL,
                updated_at REAL,
                expires_at REAL,
                extra_data TEXT
            )
        """)

        # 创建索引
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_account_id
            ON checkpoints(account_id)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_expires_at
            ON checkpoints(expires_at)
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_state
            ON checkpoints(state)
        """)
        # [CROSS-SESSION-RESUME-2026-02-16] 复合索引：按消息身份查找续传记录
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_account_chat_msg
            ON checkpoints(account_id, chat_id, msg_id)
        """)

        self.conn.commit()
        logger.info("[CheckpointDB] 初始化完成")

    async def close(self):
        """关闭数据库"""
        if self.conn:
            try:
                self.conn.execute("PRAGMA optimize")
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception as e:
                logger.warning(f"[CheckpointDB] 关闭失败: {e}")
            finally:
                self.conn = None
            logger.info("[CheckpointDB] 数据库已关闭")

    async def save_checkpoint(self, record: CheckpointRecord) -> bool:
        """
        保存或更新检查点

        Args:
            record: 检查点记录

        Returns:
            True表示成功，False表示失败
        """
        try:
            async with self._lock:
                now = time.time()
                expires_at = now + (7 * 24 * 60 * 60)  # 7天后过期

                self.conn.execute("""
                    INSERT OR REPLACE INTO checkpoints
                    (request_id, account_id, chat_id, msg_id, file_path,
                     file_name, file_size, downloaded, percentage, state,
                     error_message, created_at, updated_at, expires_at, extra_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.request_id,
                    record.account_id,
                    record.chat_id,
                    record.msg_id,
                    record.file_path,
                    record.file_name,
                    record.file_size,
                    record.downloaded,
                    record.percentage,
                    record.state,
                    record.error_message,
                    record.created_at or now,
                    now,
                    expires_at,
                    record.extra_data
                ))

                self.conn.commit()
                logger.debug(
                    f"[CheckpointDB] 保存检查点: {record.request_id} "
                    f"({record.percentage:.1f}%)"
                )
                return True

        except Exception as e:
            logger.error(f"[CheckpointDB] 保存失败: {e}", exc_info=True)
            return False

    async def load_checkpoint(self, request_id: str) -> Optional[CheckpointRecord]:
        """
        加载检查点

        Args:
            request_id: 任务ID

        Returns:
            检查点记录，如果不存在返回None
        """
        try:
            async with self._lock:
                cursor = self.conn.execute("""
                    SELECT request_id, account_id, chat_id, msg_id, file_path,
                           file_name, file_size, downloaded, percentage, state,
                           error_message, created_at, updated_at, expires_at, extra_data
                    FROM checkpoints
                    WHERE request_id = ?
                """, (request_id,))

                row = cursor.fetchone()
                if not row:
                    return None

                return CheckpointRecord(
                    request_id=row[0],
                    account_id=row[1],
                    chat_id=row[2],
                    msg_id=row[3],
                    file_path=row[4],
                    file_name=row[5],
                    file_size=row[6],
                    downloaded=row[7],
                    percentage=row[8],
                    state=row[9],
                    error_message=row[10],
                    created_at=row[11],
                    updated_at=row[12],
                    expires_at=row[13],
                    extra_data=row[14]
                )

        except Exception as e:
            logger.error(f"[CheckpointDB] 加载失败: {e}", exc_info=True)
            return None

    # [CROSS-SESSION-RESUME-2026-02-16] 按消息身份查找续传记录
    async def find_by_message(self, account_id: str, chat_id: int, msg_id: int) -> Optional[CheckpointRecord]:
        """按 (account_id, chat_id, msg_id) 查找最新的可续传记录"""
        try:
            async with self._lock:
                now = time.time()
                cursor = self.conn.execute("""
                    SELECT request_id, account_id, chat_id, msg_id, file_path,
                           file_name, file_size, downloaded, percentage, state,
                           error_message, created_at, updated_at, expires_at, extra_data
                    FROM checkpoints
                    WHERE account_id = ? AND chat_id = ? AND msg_id = ?
                      AND expires_at > ?
                      AND state IN ('DOWNLOADING', 'PAUSED')
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (account_id, chat_id, msg_id, now))

                row = cursor.fetchone()
                if not row:
                    return None

                return CheckpointRecord(
                    request_id=row[0], account_id=row[1],
                    chat_id=row[2], msg_id=row[3],
                    file_path=row[4], file_name=row[5],
                    file_size=row[6], downloaded=row[7],
                    percentage=row[8], state=row[9],
                    error_message=row[10], created_at=row[11],
                    updated_at=row[12], expires_at=row[13],
                    extra_data=row[14]
                )

        except Exception as e:
            logger.error(f"[CheckpointDB] find_by_message failed: {e}", exc_info=True)
            return None

    async def load_all_checkpoints(self) -> List[CheckpointRecord]:
        """
        加载所有未过期的检查点

        Returns:
            检查点列表
        """
        try:
            async with self._lock:
                now = time.time()
                cursor = self.conn.execute("""
                    SELECT request_id, account_id, chat_id, msg_id, file_path,
                           file_name, file_size, downloaded, percentage, state,
                           error_message, created_at, updated_at, expires_at, extra_data
                    FROM checkpoints
                    WHERE expires_at > ? AND state IN ('DOWNLOADING', 'PAUSED')
                    ORDER BY updated_at DESC
                """, (now,))

                records = []
                for row in cursor.fetchall():
                    records.append(CheckpointRecord(
                        request_id=row[0],
                        account_id=row[1],
                        chat_id=row[2],
                        msg_id=row[3],
                        file_path=row[4],
                        file_name=row[5],
                        file_size=row[6],
                        downloaded=row[7],
                        percentage=row[8],
                        state=row[9],
                        error_message=row[10],
                        created_at=row[11],
                        updated_at=row[12],
                        expires_at=row[13],
                        extra_data=row[14]
                    ))

                logger.info(f"[CheckpointDB] 加载了{len(records)}个检查点")
                return records

        except Exception as e:
            logger.error(f"[CheckpointDB] 加载所有检查点失败: {e}", exc_info=True)
            return []

    async def delete_checkpoint(self, request_id: str) -> bool:
        """
        删除检查点（任务完成后）

        Args:
            request_id: 任务ID

        Returns:
            True表示成功
        """
        try:
            async with self._lock:
                self.conn.execute("""
                    DELETE FROM checkpoints WHERE request_id = ?
                """, (request_id,))
                self.conn.commit()

                logger.debug(f"[CheckpointDB] 删除检查点: {request_id}")
                return True

        except Exception as e:
            logger.error(f"[CheckpointDB] 删除失败: {e}", exc_info=True)
            return False

    async def cleanup_expired(self) -> int:
        """
        清理过期检查点（7天未更新）

        Returns:
            删除的记录数
        """
        try:
            async with self._lock:
                now = time.time()
                cursor = self.conn.execute("""
                    SELECT COUNT(*) FROM checkpoints WHERE expires_at <= ?
                """, (now,))

                count = cursor.fetchone()[0]

                self.conn.execute("""
                    DELETE FROM checkpoints WHERE expires_at <= ?
                """, (now,))

                self.conn.commit()

                if count > 0:
                    logger.info(f"[CheckpointDB] 清理了{count}个过期检查点")

                return count

        except Exception as e:
            logger.error(f"[CheckpointDB] 清理失败: {e}", exc_info=True)
            return 0

    async def vacuum_database(self):
        """
        清理数据库和WAL日志

        在大量删除后调用以回收空间
        """
        try:
            async with self._lock:
                logger.info("[CheckpointDB] 执行VACUUM清理...")
                self.conn.execute("PRAGMA optimize")
                self.conn.execute("VACUUM")
                logger.info("[CheckpointDB] VACUUM完成")

        except Exception as e:
            logger.error(f"[CheckpointDB] VACUUM失败: {e}", exc_info=True)


# ==================== 检查点管理器 ====================
class CheckpointManager:
    """
    检查点管理器

    职责：
    1. 定期保存下载进度
    2. 应用启动时恢复未完成的任务
    3. 自动清理过期检查点
    """

    def __init__(self,
                 db_path: str = 'data/checkpoints.db',
                 checkpoint_interval: int = 30):
        """
        初始化检查点管理器

        Args:
            db_path: 数据库路径
            checkpoint_interval: 检查点保存间隔（秒）
        """
        self.db = CheckpointDB(db_path)
        self.checkpoint_interval = checkpoint_interval
        self.scheduler = None
        self._cleanup_task = None

        logger.info(
            f"[CheckpointManager] 初始化 "
            f"(interval={checkpoint_interval}s)"
        )

    async def init(self):
        """初始化"""
        await self.db.init()
        logger.info("[CheckpointManager] 初始化完成")

    async def close(self):
        """关闭"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
        await self.db.close()

    async def set_scheduler(self, scheduler: 'AccountScheduler'):
        """设置AccountScheduler引用"""
        self.scheduler = scheduler
        logger.info("[CheckpointManager] Scheduler已设置")

    async def start_background_tasks(self):
        """启动后台任务（定期清理）"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("[CheckpointManager] 后台任务已启动")

    async def _cleanup_loop(self):
        """后台清理循环"""
        while True:
            try:
                # 每小时清理一次
                await asyncio.sleep(3600)
                await self.db.cleanup_expired()
                await self.db.vacuum_database()

            except asyncio.CancelledError:
                logger.info("[CheckpointManager] 清理任务已停止")
                break

            except Exception as e:
                logger.error(f"[CheckpointManager] 清理循环异常: {e}")

    async def save_progress(self,
                           request_id: str,
                           account_id: str,
                           chat_id: int,
                           msg_id: int,
                           file_path: str,
                           file_name: str,
                           file_size: int,
                           downloaded: int,
                           percentage: float,
                           state: str = 'DOWNLOADING',
                           error_message: Optional[str] = None):
        """
        保存下载进度

        在下载过程中定期调用（推荐每30秒）
        """
        record = CheckpointRecord(
            request_id=request_id,
            account_id=account_id,
            chat_id=chat_id,
            msg_id=msg_id,
            file_path=file_path,
            file_name=file_name,
            file_size=file_size,
            downloaded=downloaded,
            percentage=percentage,
            state=state,
            error_message=error_message,
            created_at=time.time(),
            updated_at=time.time()
        )

        await self.db.save_checkpoint(record)

    async def recover_all_tasks(self) -> int:
        """
        恢复所有未完成的任务

        应用启动时调用（在创建scheduler后）

        Returns:
            恢复的任务数
        """
        if not self.scheduler:
            logger.warning("[CheckpointManager] Scheduler未设置，无法恢复任务")
            return 0

        logger.info("[CheckpointManager] 开始恢复未完成的任务...")

        checkpoints = await self.db.load_all_checkpoints()
        recovered = 0

        for checkpoint in checkpoints:
            try:
                logger.info(
                    f"[CheckpointManager] 恢复任务: {checkpoint.request_id} "
                    f"({checkpoint.percentage:.1f}%)"
                )

                # 向scheduler提交任务恢复请求
                # 注意：这里只是重新提交任务，实际的续传逻辑由Daemon处理
                await self.scheduler.submit_download(
                    account_id=checkpoint.account_id,
                    chat_id=checkpoint.chat_id,
                    msg_id=checkpoint.msg_id,
                    save_path=checkpoint.file_path,
                    media_type=None  # 可以从extra_data恢复
                )

                recovered += 1

            except Exception as e:
                logger.error(
                    f"[CheckpointManager] 恢复失败: {checkpoint.request_id}: {e}"
                )

        logger.info(f"[CheckpointManager] 恢复了{recovered}个任务")
        return recovered

    async def mark_completed(self, request_id: str):
        """
        标记任务已完成（删除检查点）

        Args:
            request_id: 任务ID
        """
        await self.db.delete_checkpoint(request_id)
        logger.info(f"[CheckpointManager] 任务已完成（检查点已删除）: {request_id}")

    async def mark_failed(self, request_id: str, error_message: str):
        """
        标记任务失败

        Args:
            request_id: 任务ID
            error_message: 错误信息
        """
        # 更新检查点状态为FAILED
        checkpoint = await self.db.load_checkpoint(request_id)
        if checkpoint:
            checkpoint.state = 'FAILED'
            checkpoint.error_message = error_message
            await self.db.save_checkpoint(checkpoint)
            logger.warning(
                f"[CheckpointManager] 任务标记为失败: "
                f"{request_id} ({error_message})"
            )

    # ┌────────────────────────────────────────────────────────────────────────────┐
    # │ [NEW-PART-CHECKPOINT-2026-02-05] Part 级别断点续传辅助方法                 │
    # │                                                                            │
    # │ 【功能说明】                                                               │
    # │   - save_failed_parts(): 保存失败 Part 索引列表到 extra_data              │
    # │   - load_failed_parts(): 加载失败 Part 索引列表                           │
    # │   - clear_failed_parts(): 清除失败 Part 信息（下载成功时调用）            │
    # │ 【数据格式】                                                               │
    # │   extra_data JSON: {                                                       │
    # │     "failed_part_indices": [1, 5, 23, ...],                               │
    # │     "retry_count": 0,                                                     │
    # │     "part_size": 1048576,                                                 │
    # │     "total_parts": 2593,                                                  │
    # │     "last_retry_at": 1738739200.0                                         │
    # │   }                                                                        │
    # │ 【添加日期】2026-02-05                                                     │
    # └────────────────────────────────────────────────────────────────────────────┘

    async def save_failed_parts(
        self,
        request_id: str,
        account_id: str,
        chat_id: int,
        msg_id: int,
        file_path: str,
        file_name: str,
        file_size: int,
        failed_part_indices: List[int],
        part_size: int = 1048576,
        total_parts: int = 0,
        retry_count: int = 0
    ):
        """
        [NEW-PART-CHECKPOINT-2026-02-05] 保存失败 Part 索引列表

        在下载过程中有 Part 失败时调用，用于断点续传。

        Args:
            request_id: 任务ID
            account_id: 账号ID
            chat_id: 对话ID
            msg_id: 消息ID
            file_path: 文件保存路径
            file_name: 文件名
            file_size: 文件总大小（字节）
            failed_part_indices: 失败 Part 索引列表
            part_size: Part 大小（默认 1MB）
            total_parts: 总 Part 数量
            retry_count: 已重试次数
        """
        extra_data = json.dumps({
            "failed_part_indices": failed_part_indices,
            "retry_count": retry_count,
            "part_size": part_size,
            "total_parts": total_parts,
            "last_retry_at": time.time()
        })

        # 计算已下载大小（非失败 Part 的总大小）
        success_parts = total_parts - len(failed_part_indices)
        downloaded = success_parts * part_size
        percentage = (downloaded / file_size * 100) if file_size > 0 else 0

        record = CheckpointRecord(
            request_id=request_id,
            account_id=account_id,
            chat_id=chat_id,
            msg_id=msg_id,
            file_path=file_path,
            file_name=file_name,
            file_size=file_size,
            downloaded=downloaded,
            percentage=percentage,
            state='PAUSED',  # Part 失败 = 任务暂停
            error_message=f"{len(failed_part_indices)} Parts failed",
            created_at=time.time(),
            updated_at=time.time(),
            extra_data=extra_data
        )

        await self.db.save_checkpoint(record)

        logger.info(
            f"[CheckpointManager] [PART] 保存失败 Part 信息: "
            f"{request_id} ({len(failed_part_indices)}/{total_parts} Parts 失败)"
        )

    # ┌────────────────────────────────────────────────────────────────────────────┐
    # │ [CROSS-SESSION-RESUME-2026-02-16] 跨会话断点续传方法                      │
    # └────────────────────────────────────────────────────────────────────────────┘

    async def find_by_message(self, account_id: str, chat_id: int, msg_id: int) -> Optional['CheckpointRecord']:
        """按 (account_id, chat_id, msg_id) 查找可续传记录"""
        return await self.db.find_by_message(account_id, chat_id, msg_id)

    async def save_download_state(
        self,
        request_id: str,
        account_id: str,
        chat_id: int,
        msg_id: int,
        file_path: str,
        file_name: str,
        file_size: int,
        completed_part_indices: List[int],
        part_size: int = 1048576,
        total_parts: int = 0
    ):
        """周期性保存已完成 Part 索引，用于跨会话续传"""
        extra_data = json.dumps({
            "completed_part_indices": completed_part_indices,
            "part_size": part_size,
            "total_parts": total_parts,
            "checkpoint_type": "periodic"
        })

        completed_count = len(completed_part_indices)
        downloaded = min(completed_count * part_size, file_size)
        percentage = (downloaded / file_size * 100) if file_size > 0 else 0

        record = CheckpointRecord(
            request_id=request_id,
            account_id=account_id,
            chat_id=chat_id,
            msg_id=msg_id,
            file_path=file_path,
            file_name=file_name,
            file_size=file_size,
            downloaded=downloaded,
            percentage=percentage,
            state='DOWNLOADING',
            created_at=time.time(),
            updated_at=time.time(),
            extra_data=extra_data
        )

        await self.db.save_checkpoint(record)
        logger.debug(
            f"[CheckpointManager] [PERIODIC] 保存下载状态: "
            f"{request_id} ({completed_count}/{total_parts} parts, {percentage:.1f}%)"
        )

    @staticmethod
    def parse_completed_parts(checkpoint: 'CheckpointRecord') -> Optional[Dict[str, Any]]:
        """从 extra_data 解析已完成 Part 索引，兼容 completed/failed 两种格式"""
        if not checkpoint or not checkpoint.extra_data:
            return None
        try:
            extra = json.loads(checkpoint.extra_data)
            if "completed_part_indices" in extra:
                return extra
            elif "failed_part_indices" in extra:
                total = extra.get("total_parts", 0)
                failed_set = set(extra["failed_part_indices"])
                completed = [i for i in range(total) if i not in failed_set]
                return {
                    "completed_part_indices": completed,
                    "part_size": extra.get("part_size", 1048576),
                    "total_parts": total,
                    "checkpoint_type": "from_failed"
                }
            return None
        except json.JSONDecodeError:
            return None

    async def load_failed_parts(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        [NEW-PART-CHECKPOINT-2026-02-05] 加载失败 Part 索引列表

        用于断点续传时恢复失败 Part 信息。

        Args:
            request_id: 任务ID

        Returns:
            包含失败 Part 信息的字典，如果不存在返回 None
            {
                "failed_part_indices": [1, 5, 23, ...],
                "retry_count": 0,
                "part_size": 1048576,
                "total_parts": 2593,
                "last_retry_at": 1738739200.0
            }
        """
        checkpoint = await self.db.load_checkpoint(request_id)

        if not checkpoint or not checkpoint.extra_data:
            return None

        try:
            extra_data = json.loads(checkpoint.extra_data)

            if "failed_part_indices" not in extra_data:
                return None

            logger.info(
                f"[CheckpointManager] [PART] 加载失败 Part 信息: "
                f"{request_id} ({len(extra_data.get('failed_part_indices', []))} Parts)"
            )

            return extra_data

        except json.JSONDecodeError as e:
            logger.error(f"[CheckpointManager] [PART] 解析 extra_data 失败: {e}")
            return None

    async def clear_failed_parts(self, request_id: str):
        """
        [NEW-PART-CHECKPOINT-2026-02-05] 清除失败 Part 信息（下载成功时调用）

        Args:
            request_id: 任务ID
        """
        # 直接删除整个 checkpoint 记录
        await self.db.delete_checkpoint(request_id)

        logger.info(
            f"[CheckpointManager] [PART] 已清除失败 Part 信息: {request_id}"
        )

    async def increment_retry_count(self, request_id: str) -> int:
        """
        [NEW-PART-CHECKPOINT-2026-02-05] 递增重试计数

        每次重试前调用，返回新的重试计数。

        Args:
            request_id: 任务ID

        Returns:
            新的重试计数，如果记录不存在返回 -1
        """
        checkpoint = await self.db.load_checkpoint(request_id)

        if not checkpoint or not checkpoint.extra_data:
            return -1

        try:
            extra_data = json.loads(checkpoint.extra_data)
            retry_count = extra_data.get("retry_count", 0) + 1
            extra_data["retry_count"] = retry_count
            extra_data["last_retry_at"] = time.time()

            checkpoint.extra_data = json.dumps(extra_data)
            checkpoint.updated_at = time.time()

            await self.db.save_checkpoint(checkpoint)

            logger.info(
                f"[CheckpointManager] [PART] 重试计数递增: "
                f"{request_id} (retry_count={retry_count})"
            )

            return retry_count

        except json.JSONDecodeError as e:
            logger.error(f"[CheckpointManager] [PART] 解析 extra_data 失败: {e}")
            return -1

    async def get_resumable_tasks(self, account_id: str) -> List[Dict[str, Any]]:
        """
        [NEW-PART-CHECKPOINT-2026-02-05] 获取指定账号的可恢复任务列表

        用于应用启动时显示可恢复的下载任务。

        Args:
            account_id: 账号ID

        Returns:
            可恢复任务列表，每个任务包含：
            {
                "request_id": "xxx",
                "file_name": "xxx.zip",
                "file_size": 1234567890,
                "downloaded": 1234500000,
                "percentage": 99.9,
                "failed_parts": 3,
                "total_parts": 2593
            }
        """
        all_checkpoints = await self.db.load_all_checkpoints()

        # 过滤指定账号的 PAUSED 状态任务
        resumable = []
        for checkpoint in all_checkpoints:
            if checkpoint.account_id != account_id:
                continue

            if checkpoint.state != 'PAUSED':
                continue

            # 解析 extra_data
            extra_data = {}
            if checkpoint.extra_data:
                try:
                    extra_data = json.loads(checkpoint.extra_data)
                except json.JSONDecodeError:
                    pass

            failed_indices = extra_data.get("failed_part_indices", [])
            total_parts = extra_data.get("total_parts", 0)

            resumable.append({
                "request_id": checkpoint.request_id,
                "chat_id": checkpoint.chat_id,
                "msg_id": checkpoint.msg_id,
                "file_name": checkpoint.file_name,
                "file_path": checkpoint.file_path,
                "file_size": checkpoint.file_size,
                "downloaded": checkpoint.downloaded,
                "percentage": checkpoint.percentage,
                "failed_parts": len(failed_indices),
                "total_parts": total_parts
            })

        logger.info(
            f"[CheckpointManager] [PART] 找到 {len(resumable)} 个可恢复任务 "
            f"(account_id={account_id})"
        )

        return resumable


# ==================== TaskRecovery - 任务恢复工具 ====================
class TaskRecovery:
    """
    任务恢复工具

    在应用启动时使用，恢复所有未完成的下载
    """

    def __init__(self, checkpoint_manager: CheckpointManager):
        """
        初始化恢复工具

        Args:
            checkpoint_manager: 检查点管理器
        """
        self.checkpoint_manager = checkpoint_manager

    async def recover_crashed_daemon(self, account_id: str) -> int:
        """
        恢复指定账号的Daemon崩溃后的任务

        当Daemon检测到崩溃时调用

        Args:
            account_id: 账号ID

        Returns:
            恢复的任务数
        """
        logger.warning(f"[TaskRecovery] 恢复Daemon崩溃任务: {account_id}")

        # TODO: 实现Daemon特定账号的任务恢复
        # 1. 加载该账号的所有DOWNLOADING检查点
        # 2. 重新提交给新启动的Daemon
        # 3. 返回恢复数量

        return 0

    async def check_and_recover(self) -> int:
        """
        检查并恢复所有未完成的任务

        应用启动时调用

        Returns:
            恢复的任务数
        """
        logger.info("[TaskRecovery] 检查未完成的任务...")

        return await self.checkpoint_manager.recover_all_tasks()


# ==================== 测试代码 ====================
if __name__ == '__main__':
    import sys

    async def test_checkpoint():
        """测试检查点功能"""
        print("=== CheckpointManager 测试 ===\n")

        # 初始化
        manager = CheckpointManager()
        await manager.init()

        try:
            # 测试1：保存检查点
            print("[测试1] 保存检查点")
            await manager.save_progress(
                request_id="test_001",
                account_id="acc_123",
                chat_id=456,
                msg_id=789,
                file_path="/tmp/test.zip",
                file_name="test.zip",
                file_size=1000000,
                downloaded=500000,
                percentage=50.0
            )
            print("✓ 检查点已保存\n")

            # 测试2：加载检查点
            print("[测试2] 加载检查点")
            checkpoint = await manager.db.load_checkpoint("test_001")
            if checkpoint:
                print(f"✓ 加载成功: {checkpoint.request_id} ({checkpoint.percentage}%)\n")
            else:
                print("✗ 加载失败\n")

            # 测试3：加载所有检查点
            print("[测试3] 加载所有检查点")
            all_checkpoints = await manager.db.load_all_checkpoints()
            print(f"✓ 加载了{len(all_checkpoints)}个检查点\n")

            # 测试4：删除检查点
            print("[测试4] 删除检查点")
            await manager.db.delete_checkpoint("test_001")
            checkpoint = await manager.db.load_checkpoint("test_001")
            if checkpoint is None:
                print("✓ 删除成功\n")
            else:
                print("✗ 删除失败\n")

        finally:
            await manager.close()
            print("[完成] 所有测试已执行")

    asyncio.run(test_checkpoint())
