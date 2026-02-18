"""
代理工具模块

提供代理检测、测试、配置功能，完全解耦独立。
支持自动检测、手动配置、智能回退等特性。
"""

import socket
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass


@dataclass
class ProxyConfig:
    """代理配置"""
    proxy_type: str  # "http" | "socks5" | "socks4"
    addr: str
    port: int

    def to_telethon_dict(self) -> Dict[str, Any]:
        """转换为 Telethon 代理格式"""
        return {
            'proxy_type': self.proxy_type,
            'addr': self.addr,
            'port': self.port
        }

    def __str__(self):
        return f"{self.proxy_type}://{self.addr}:{self.port}"


def test_proxy_port(host: str, port: int, timeout: float = 3.0) -> bool:
    """
    测试代理端口是否可用

    Args:
        host: 主机地址
        port: 端口号
        timeout: 超时时间（秒）

    Returns:
        True: 端口可用，False: 端口不可用
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0  # 0 表示成功连接
    except Exception as e:
        return False


def detect_system_proxy() -> Optional[ProxyConfig]:
    """
    检测系统代理设置

    Returns:
        ProxyConfig 或 None
    """
    try:
        import urllib.request
        import urllib.parse

        proxies = urllib.request.getproxies()

        # 优先查找 https 代理，其次 http
        proxy_url = proxies.get('https') or proxies.get('http')

        if proxy_url:
            # 解析代理 URL
            parsed = urllib.parse.urlparse(proxy_url)
            host = parsed.hostname
            port = parsed.port

            if host and port:
                # 根据 scheme 确定代理类型
                proxy_type = "http"
                if parsed.scheme in ('socks5', 'socks5h'):
                    proxy_type = "socks5"
                elif parsed.scheme in ('socks4', 'socks4a'):
                    proxy_type = "socks4"

                return ProxyConfig(
                    proxy_type=proxy_type,
                    addr=host,
                    port=port
                )

        return None

    except Exception:
        return None


def get_proxy_for_telethon(
    mode: str = "auto",
    custom_proxy: Optional[Dict[str, Any]] = None,
    fallback_to_direct: bool = True,
    test_timeout: float = 3.0,
    logger_func=None
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    获取 Telethon 使用的代理配置

    Args:
        mode: 代理模式
            - "auto": 自动检测（系统代理 → 默认端口 → 直连）
            - "system": 仅使用系统代理
            - "custom": 使用自定义代理
            - "direct": 直连，不使用代理
        custom_proxy: 自定义代理配置 {"type": str, "host": str, "port": int}
        fallback_to_direct: 代理失败时是否回退到直连
        test_timeout: 代理端口测试超时（秒）
        logger_func: 日志函数，用于输出调试信息

    Returns:
        (proxy_dict, status_message)
        - proxy_dict: Telethon 代理字典或 None（直连）
        - status_message: 状态描述字符串
    """
    def log(msg: str):
        if logger_func:
            logger_func(msg)

    # 检查 python-socks 是否安装
    try:
        import python_socks
        has_socks = True
    except ImportError:
        has_socks = False
        log("[PROXY] 警告: python-socks 未安装，无法使用代理功能")
        log("[PROXY] 请运行: pip install python-socks[asyncio]")

    # 模式1: 直连模式
    if mode == "direct":
        log("[PROXY] 使用直连模式（无代理）")
        return None, "direct"

    # 模式2: 自定义代理
    if mode == "custom" and custom_proxy and custom_proxy.get("enabled"):
        if not has_socks:
            if fallback_to_direct:
                log("[PROXY] python-socks 未安装，回退到直连")
                return None, "direct_fallback"
            else:
                raise ImportError("需要 python-socks 才能使用代理")

        proxy_config = ProxyConfig(
            proxy_type=custom_proxy.get("type", "http"),
            addr=custom_proxy.get("host", "127.0.0.1"),
            port=custom_proxy.get("port", 7890)
        )

        log(f"[PROXY] 使用自定义代理: {proxy_config}")

        # 测试端口可用性
        if test_proxy_port(proxy_config.addr, proxy_config.port, test_timeout):
            log(f"[PROXY] 自定义代理端口测试成功: {proxy_config.addr}:{proxy_config.port}")
            return proxy_config.to_telethon_dict(), f"custom:{proxy_config}"
        else:
            log(f"[PROXY] 自定义代理端口不可用: {proxy_config.addr}:{proxy_config.port}")
            if fallback_to_direct:
                log("[PROXY] 回退到直连模式")
                return None, "direct_fallback"
            else:
                raise ConnectionError(f"自定义代理不可用: {proxy_config}")

    # 模式3: 仅系统代理
    if mode == "system":
        if not has_socks:
            if fallback_to_direct:
                log("[PROXY] python-socks 未安装，回退到直连")
                return None, "direct_fallback"
            else:
                raise ImportError("需要 python-socks 才能使用代理")

        system_proxy = detect_system_proxy()
        if system_proxy:
            log(f"[PROXY] 检测到系统代理: {system_proxy}")

            # 测试端口可用性
            if test_proxy_port(system_proxy.addr, system_proxy.port, test_timeout):
                log(f"[PROXY] 系统代理端口测试成功: {system_proxy.addr}:{system_proxy.port}")
                return system_proxy.to_telethon_dict(), f"system:{system_proxy}"
            else:
                log(f"[PROXY] 系统代理端口不可用: {system_proxy.addr}:{system_proxy.port}")
        else:
            log("[PROXY] 未检测到系统代理")

        if fallback_to_direct:
            log("[PROXY] 回退到直连模式")
            return None, "direct_fallback"
        else:
            raise ConnectionError("系统代理不可用且未启用直连回退")

    # 模式4: 自动模式（推荐）
    if mode == "auto":
        if not has_socks:
            log("[PROXY] python-socks 未安装，使用直连模式")
            return None, "direct"

        # 尝试1: 系统代理
        system_proxy = detect_system_proxy()
        if system_proxy:
            log(f"[PROXY] 检测到系统代理: {system_proxy}")
            if test_proxy_port(system_proxy.addr, system_proxy.port, test_timeout):
                log(f"[PROXY] 系统代理端口测试成功，使用系统代理")
                return system_proxy.to_telethon_dict(), f"auto:system:{system_proxy}"
            else:
                log(f"[PROXY] 系统代理端口 {system_proxy.addr}:{system_proxy.port} 不可用")
        else:
            log("[PROXY] 未检测到系统代理")

        # 尝试2: 默认端口 7890（Clash/V2Ray 常用）
        log("[PROXY] 尝试使用默认代理端口 7890")
        if test_proxy_port("127.0.0.1", 7890, test_timeout):
            log("[PROXY] 默认端口 7890 可用，使用默认代理")
            default_proxy = ProxyConfig(
                proxy_type="http",
                addr="127.0.0.1",
                port=7890
            )
            return default_proxy.to_telethon_dict(), f"auto:default:{default_proxy}"
        else:
            log("[PROXY] 默认端口 7890 不可用")

        # 尝试3: 回退到直连
        if fallback_to_direct:
            log("[PROXY] 所有代理尝试失败，回退到直连模式")
            return None, "auto:direct_fallback"
        else:
            raise ConnectionError("所有代理尝试失败且未启用直连回退")

    # 未知模式
    raise ValueError(f"未知的代理模式: {mode}")


def format_proxy_status(status: str) -> str:
    """
    格式化代理状态消息为用户友好的描述

    Args:
        status: 状态字符串（从 get_proxy_for_telethon 返回）

    Returns:
        用户友好的描述
    """
    if status == "direct":
        return "直连（无代理）"
    elif status == "direct_fallback":
        return "直连（代理不可用，已回退）"
    elif status.startswith("custom:"):
        return f"自定义代理: {status.split(':', 1)[1]}"
    elif status.startswith("system:"):
        return f"系统代理: {status.split(':', 1)[1]}"
    elif status.startswith("auto:system:"):
        return f"自动检测（系统代理）: {status.split(':', 2)[2]}"
    elif status.startswith("auto:default:"):
        return f"自动检测（默认端口）: {status.split(':', 2)[2]}"
    elif status == "auto:direct_fallback":
        return "自动检测（代理不可用，使用直连）"
    else:
        return status
