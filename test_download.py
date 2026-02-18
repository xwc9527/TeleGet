"""
test_download.py — TGDownloader SDK 验证脚本

功能：
  1. 检查所有 SDK 模块是否可正常导入
  2. 验证 TGDownloader 实例化（无需网络）
  3. 验证配置传递和 session 解析逻辑
  4. （可选）使用真实账号执行一次完整下载

用法：
  # 仅检查导入和实例化（无需 Telegram 账号）
  python test_download.py

  # 执行真实下载测试
  python test_download.py --live \
      --api-id 12345 \
      --api-hash "your_api_hash" \
      --session-dir "data/accounts" \
      --account-id "my_account" \
      --chat-id -1001234567890 \
      --msg-id 42 \
      --save-path "./test_output.mp4"
"""

import argparse
import asyncio
import logging
import os
import sys
import tempfile
import traceback

# ---------- 日志配置 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("test_download")


# ================================================================
#  Phase 1：模块导入检查
# ================================================================

def check_imports() -> bool:
    """逐个导入 SDK 模块，报告结果。"""
    # (模块名, 描述, 是否必须)
    modules = [
        ("tg_downloader",        "TGDownloader SDK 入口",      True),
        ("download_manager_v2",  "AccountScheduler / 任务管理", True),
        ("download_daemon_core", "Daemon 核心逻辑",            True),
        ("download_daemon",      "Daemon 进程入口",            True),
        ("download_pool",        "WorkerPool / ClientPool",    True),
        ("download_ipc",         "IPC 通道",                   True),
        ("download_event_bus",   "事件总线",                   True),
        ("download_config",      "下载配置",                   True),
        ("download_protocol_v2", "IPC 协议定义",               True),
        ("download_checkpoint",  "断点续传",                   True),
        ("download_logger",      "日志工具",                   True),
        ("download_monitor",     "下载监控",                   False),  # 依赖 psutil（可选）
        ("sparse_file_utils",    "稀疏文件写入",               True),
        ("proxy_utils",          "代理工具",                   True),
    ]

    all_ok = True
    for mod_name, desc, required in modules:
        try:
            __import__(mod_name)
            logger.info(f"  [OK] {mod_name:<25s} — {desc}")
        except Exception as e:
            if required:
                logger.error(f"  [FAIL] {mod_name:<25s} — {e}")
                all_ok = False
            else:
                logger.warning(f"  [SKIP] {mod_name:<25s} — {e} (可选依赖)")

    return all_ok


# ================================================================
#  Phase 2：TGDownloader 实例化检查
# ================================================================

def check_instantiation() -> bool:
    """创建 TGDownloader 实例，验证属性和配置传递。"""
    from tg_downloader import TGDownloader

    # 使用临时目录作为 session_dir（不需要真实 session 文件）
    with tempfile.TemporaryDirectory() as tmp_dir:
        session_dir = os.path.join(tmp_dir, "accounts")
        log_dir = os.path.join(tmp_dir, "logs")
        os.makedirs(session_dir, exist_ok=True)

        extra_config = {
            "rate_limiter_enabled": False,
            "tdl_strategy_enabled": False,
        }

        downloader = TGDownloader(
            api_id=12345,
            api_hash="test_hash_not_real",
            session_dir=session_dir,
            log_dir=log_dir,
            config=extra_config,
        )

        # 检查属性
        assert downloader.active_account is None, "未启动时 active_account 应为 None"
        assert downloader.active_downloads == {}, "未启动时 active_downloads 应为空"
        assert downloader._api_id == 12345
        assert downloader._api_hash == "test_hash_not_real"
        assert downloader._session_dir == os.path.abspath(session_dir)
        assert downloader._config["rate_limiter_enabled"] is False

        # 检查 download() 在未 start 时抛出 RuntimeError
        try:
            asyncio.get_event_loop().run_until_complete(
                downloader.download(chat_id=1, msg_id=1, save_path="/tmp/x")
            )
            logger.error("  [FAIL] download() 未抛出 RuntimeError")
            return False
        except RuntimeError:
            logger.info("  [OK] download() 正确拒绝未启动的调用")

        # 检查 shutdown 幂等（未启动时也不报错）
        asyncio.get_event_loop().run_until_complete(downloader.shutdown())
        logger.info("  [OK] shutdown() 幂等调用正常")

    logger.info("  [OK] TGDownloader 实例化和属性检查通过")
    return True


# ================================================================
#  Phase 3：Session 路径解析检查
# ================================================================

def check_session_resolver() -> bool:
    """验证两种 session 文件查找约定。"""
    from tg_downloader import TGDownloader

    with tempfile.TemporaryDirectory() as tmp_dir:
        session_dir = os.path.join(tmp_dir, "accounts")
        os.makedirs(session_dir, exist_ok=True)

        downloader = TGDownloader(
            api_id=1, api_hash="x", session_dir=session_dir,
        )
        downloader._ensure_init()
        resolver = downloader._scheduler.session_path_resolver

        # 约定1：session_dir/<account_id>.session
        acct1 = "flat_account"
        open(os.path.join(session_dir, f"{acct1}.session"), "w").close()
        result1 = resolver(acct1)
        assert result1 is not None, f"约定1解析失败: {acct1}"
        logger.info(f"  [OK] 约定1 (平铺) 解析成功: {result1}")

        # 约定2：session_dir/<account_id>/session.session
        acct2 = "nested_account"
        nested_dir = os.path.join(session_dir, acct2)
        os.makedirs(nested_dir, exist_ok=True)
        open(os.path.join(nested_dir, "session.session"), "w").close()
        result2 = resolver(acct2)
        assert result2 is not None, f"约定2解析失败: {acct2}"
        logger.info(f"  [OK] 约定2 (子目录) 解析成功: {result2}")

        # 不存在的账号 → None
        result_none = resolver("nonexistent_account")
        assert result_none is None, "不存在的账号应返回 None"
        logger.info("  [OK] 不存在的账号正确返回 None")

        # 清理
        asyncio.get_event_loop().run_until_complete(downloader.shutdown())

    return True


# ================================================================
#  Phase 4：核心类导入验证
# ================================================================

def check_core_classes() -> bool:
    """验证关键类和枚举可正常导入使用。"""
    from download_manager_v2 import (
        AccountScheduler,
        DownloadEventListener,
        DownloadTask,
        DownloadTaskState,
    )
    from download_event_bus import EventBus
    from download_pool import WorkerPool, ClientPool, RateLimiter, Part

    # DownloadTaskState 枚举值
    assert DownloadTaskState.PENDING.value == "pending"
    assert DownloadTaskState.COMPLETED.value == "completed"
    assert DownloadTaskState.FAILED.value == "failed"
    assert DownloadTaskState.CANCELLED.value == "cancelled"
    logger.info("  [OK] DownloadTaskState 枚举值正确")

    # Part 数据类
    p = Part(index=0, offset=0, length=1024 * 1024, is_last=False)
    assert p.length == 1024 * 1024
    logger.info("  [OK] Part 数据类可正常实例化")

    # EventBus
    bus = EventBus(throttle_interval=0.5)
    logger.info("  [OK] EventBus 实例化正常")

    # RateLimiter
    rl = RateLimiter(batch_size=4, interval=0.2)
    rl.on_success()
    stats = rl.get_stats()
    assert "total_acquired" in stats
    logger.info(f"  [OK] RateLimiter 实例化和统计正常: {stats}")

    return True


# ================================================================
#  Phase 5（可选）：真实下载测试
# ================================================================

async def live_download_test(args) -> bool:
    """使用真实 Telegram 账号执行一次完整下载。"""
    from tg_downloader import TGDownloader

    progress_events = []
    complete_result = {}

    def on_progress(downloaded, total, pct):
        progress_events.append((downloaded, total, pct))
        if len(progress_events) % 20 == 0:
            logger.info(f"  [PROGRESS] {pct:.1f}% ({downloaded}/{total})")

    def on_complete(success, file_path, error_msg):
        complete_result["success"] = success
        complete_result["file_path"] = file_path
        complete_result["error"] = error_msg
        logger.info(
            f"  [COMPLETE] success={success}, path={file_path}, error={error_msg}"
        )

    def on_error(error_msg):
        logger.error(f"  [ERROR] {error_msg}")

    config = {}
    if args.config_json:
        import json
        with open(args.config_json, "r", encoding="utf-8") as f:
            config = json.load(f)

    downloader = TGDownloader(
        api_id=args.api_id,
        api_hash=args.api_hash,
        session_dir=args.session_dir,
        config=config,
    )

    try:
        # 1. 激活账号
        logger.info(f"[LIVE] 激活账号: {args.account_id}")
        ok = await downloader.start(args.account_id)
        if not ok:
            logger.error("[LIVE] 账号激活失败，请检查 session 文件")
            return False
        logger.info(f"[LIVE] 账号已激活: {downloader.active_account}")

        # 2. 提交下载
        logger.info(
            f"[LIVE] 提交下载: chat={args.chat_id}, msg={args.msg_id}, "
            f"save={args.save_path}"
        )
        request_id = await downloader.download(
            chat_id=args.chat_id,
            msg_id=args.msg_id,
            save_path=args.save_path,
            progress_callback=on_progress,
            complete_callback=on_complete,
            error_callback=on_error,
        )
        logger.info(f"[LIVE] 下载已提交: request_id={request_id}")

        # 3. 等待完成（轮询 active_downloads）
        timeout = args.timeout or 600
        elapsed = 0
        while elapsed < timeout:
            downloads = downloader.active_downloads
            if request_id not in downloads:
                break
            task = downloads[request_id]
            if task.state.value in ("completed", "failed", "cancelled"):
                break
            await asyncio.sleep(2)
            elapsed += 2

        # 4. 报告结果
        if complete_result.get("success"):
            file_size = os.path.getsize(args.save_path) if os.path.exists(args.save_path) else 0
            logger.info(
                f"[LIVE] 下载成功! 文件大小: {file_size:,} bytes, "
                f"进度回调次数: {len(progress_events)}"
            )
            return True
        else:
            logger.error(
                f"[LIVE] 下载失败: {complete_result.get('error', '超时或未知错误')}"
            )
            return False

    finally:
        await downloader.shutdown()
        logger.info("[LIVE] 已关闭")


# ================================================================
#  主入口
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description="TGDownloader SDK 验证脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--live", action="store_true",
        help="执行真实下载测试（需要 Telegram 账号）",
    )
    parser.add_argument("--api-id", type=int, default=0)
    parser.add_argument("--api-hash", default="")
    parser.add_argument("--session-dir", default="data/accounts")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--chat-id", type=int, default=0)
    parser.add_argument("--msg-id", type=int, default=0)
    parser.add_argument("--save-path", default="./test_download_output")
    parser.add_argument("--timeout", type=int, default=600,
                        help="下载超时（秒，默认600）")
    parser.add_argument("--config-json", default=None,
                        help="额外配置 JSON 文件路径")
    args = parser.parse_args()

    total = 0
    passed = 0

    # ---------- Phase 1: 导入检查 ----------
    logger.info("=" * 50)
    logger.info("Phase 1: 模块导入检查")
    logger.info("=" * 50)
    total += 1
    try:
        if check_imports():
            logger.info("[Phase 1] PASSED")
            passed += 1
        else:
            logger.error("[Phase 1] FAILED — 部分模块导入失败")
    except Exception:
        logger.error(f"[Phase 1] FAILED — 异常:\n{traceback.format_exc()}")

    # ---------- Phase 2: 实例化检查 ----------
    logger.info("")
    logger.info("=" * 50)
    logger.info("Phase 2: TGDownloader 实例化检查")
    logger.info("=" * 50)
    total += 1
    try:
        if check_instantiation():
            logger.info("[Phase 2] PASSED")
            passed += 1
        else:
            logger.error("[Phase 2] FAILED")
    except Exception:
        logger.error(f"[Phase 2] FAILED — 异常:\n{traceback.format_exc()}")

    # ---------- Phase 3: Session 解析检查 ----------
    logger.info("")
    logger.info("=" * 50)
    logger.info("Phase 3: Session 路径解析检查")
    logger.info("=" * 50)
    total += 1
    try:
        if check_session_resolver():
            logger.info("[Phase 3] PASSED")
            passed += 1
        else:
            logger.error("[Phase 3] FAILED")
    except Exception:
        logger.error(f"[Phase 3] FAILED — 异常:\n{traceback.format_exc()}")

    # ---------- Phase 4: 核心类检查 ----------
    logger.info("")
    logger.info("=" * 50)
    logger.info("Phase 4: 核心类导入验证")
    logger.info("=" * 50)
    total += 1
    try:
        if check_core_classes():
            logger.info("[Phase 4] PASSED")
            passed += 1
        else:
            logger.error("[Phase 4] FAILED")
    except Exception:
        logger.error(f"[Phase 4] FAILED — 异常:\n{traceback.format_exc()}")

    # ---------- Phase 5: 真实下载（可选） ----------
    if args.live:
        logger.info("")
        logger.info("=" * 50)
        logger.info("Phase 5: 真实下载测试")
        logger.info("=" * 50)

        if not args.api_id or not args.api_hash or not args.account_id:
            logger.error(
                "[Phase 5] SKIPPED — 缺少必要参数: "
                "--api-id, --api-hash, --account-id"
            )
        elif not args.chat_id or not args.msg_id:
            logger.error(
                "[Phase 5] SKIPPED — 缺少必要参数: --chat-id, --msg-id"
            )
        else:
            total += 1
            try:
                ok = asyncio.run(live_download_test(args))
                if ok:
                    logger.info("[Phase 5] PASSED")
                    passed += 1
                else:
                    logger.error("[Phase 5] FAILED")
            except Exception:
                logger.error(
                    f"[Phase 5] FAILED — 异常:\n{traceback.format_exc()}"
                )

    # ---------- 汇总 ----------
    logger.info("")
    logger.info("=" * 50)
    logger.info(f"结果: {passed}/{total} 项通过")
    logger.info("=" * 50)

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
