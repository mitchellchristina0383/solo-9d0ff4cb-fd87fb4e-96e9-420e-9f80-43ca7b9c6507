"""线路端点配置：两条本地回显线路（a/b），地址由环境变量注入。"""
from __future__ import annotations

import os
from dataclasses import dataclass

#: UDP 载荷理论上限（65535 - 20(IP) - 8(UDP)）
MAX_UDP_PAYLOAD = 65507


@dataclass(frozen=True)
class LineEndpoint:
    """一条回显线路的寻址信息。"""

    name: str
    udp_host: str
    udp_port: int
    ctrl_url: str  # 线路模拟器的 HTTP 控制口（配置丢包/延迟/断线）

    @property
    def udp_addr(self) -> str:
        return f"{self.udp_host}:{self.udp_port}"


def load_lines() -> dict[str, LineEndpoint]:
    """从环境变量加载两条线路。默认指向本机端口，便于裸机开发。"""
    defaults = {
        "a": ("127.0.0.1", 9001, "http://127.0.0.1:9101"),
        "b": ("127.0.0.1", 9002, "http://127.0.0.1:9102"),
    }
    lines: dict[str, LineEndpoint] = {}
    for name, (host, udp_port, ctrl_url) in defaults.items():
        prefix = f"LINE_{name.upper()}"
        lines[name] = LineEndpoint(
            name=name,
            udp_host=os.environ.get(f"{prefix}_HOST", host),
            udp_port=int(os.environ.get(f"{prefix}_UDP_PORT", str(udp_port))),
            ctrl_url=os.environ.get(f"{prefix}_CTRL_URL", ctrl_url).rstrip("/"),
        )
    return lines
