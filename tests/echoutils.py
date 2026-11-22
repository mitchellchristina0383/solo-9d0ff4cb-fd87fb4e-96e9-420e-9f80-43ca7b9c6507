"""测试辅助：在进程内拉起回显线路（UDP 回显 + HTTP 控制口）。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import uvicorn

from echo.server import EchoProtocol, LineConfig, LineStats, create_echo_app


@asynccontextmanager
async def raw_echo(max_size=65507, delay_ms=0, down=False):
    """只起 UDP 回显（无 HTTP 控制口），供引擎级测试使用。"""
    cfg = LineConfig(max_size=max_size, delay_ms=delay_ms, down=down)
    stats = LineStats()
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: EchoProtocol(cfg, stats), local_addr=("127.0.0.1", 0)
    )
    port = transport.get_extra_info("sockname")[1]
    try:
        yield SimpleNamespace(cfg=cfg, stats=stats, port=port)
    finally:
        transport.close()


@asynccontextmanager
async def full_echo(name, max_size=65507, delay_ms=0, down=False):
    """起完整回显线路（UDP + HTTP 控制口），供 API 级测试使用。"""
    echo_app = create_echo_app(
        line_name=name,
        udp_host="127.0.0.1",
        udp_port=0,
        max_size=max_size,
        delay_ms=delay_ms,
        down=down,
    )
    server = uvicorn.Server(
        uvicorn.Config(echo_app, host="127.0.0.1", port=0, log_level="error")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    http_port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield SimpleNamespace(
            app=echo_app,
            cfg=echo_app.state.cfg,
            stats=echo_app.state.stats,
            udp_port=echo_app.state.udp_port,
            http_port=http_port,
            ctrl_url=f"http://127.0.0.1:{http_port}",
        )
    finally:
        server.should_exit = True
        await task
