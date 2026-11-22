"""本地回显线路模拟器：UDP 回显服务 + HTTP 控制口。

可模拟三种线路行为：
- 超长丢包：载荷超过 ``max_size`` 字节的包直接丢弃；
- 回包延迟：收到包后延迟 ``delay_ms`` 毫秒再回显；
- 断线：``down`` 为真时丢弃所有包。

行为以**收到包那一刻**的配置为准（延迟期间改配置不影响已入队的包）。

另外提供**分段演练**（``POST /drill``）：对同一条线路按时间表依次执行
“断线 / 指定回包延迟”，最多 8 段。演练状态先落盘再生效，重启后按绝对时间
表续跑（停机时间照算、已过段直接跳过）。详见 :mod:`app.drill`。

运行方式：``python -m echo.server``（参数走环境变量，见文件底部 main）。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from typing import Callable

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.config import MAX_UDP_PAYLOAD
from app.drill import (
    DrillActive,
    DrillManager,
    DrillNotFound,
    DrillPersistError,
    DrillRequest,
)


@dataclass
class LineConfig:
    max_size: int = MAX_UDP_PAYLOAD  # 超过即丢（模拟超长丢包）
    delay_ms: int = 0                # 回包延迟
    down: bool = False               # 断线


@dataclass
class LineStats:
    received: int = 0
    echoed: int = 0
    dropped_oversize: int = 0
    dropped_down: int = 0


class EchoProtocol(asyncio.DatagramProtocol):
    """逐字节回显；按收到时刻的配置决定丢包/延迟。

    ``effective`` 为可选的同步钩子（演练用）：收到包时调用一次，返回那一刻
    应生效的 ``down``/``delay_ms``/``max_size``；返回 None 时沿用 ``cfg``。
    延迟回包用 ``call_later`` 独立排队，后续改配置或切段都不会取消它。
    """

    def __init__(
        self,
        cfg: LineConfig,
        stats: LineStats,
        effective: Callable[[], dict | None] | None = None,
    ) -> None:
        self.cfg = cfg
        self.stats = stats
        self.effective = effective
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport) -> None:  # noqa: ANN001
        self.transport = transport

    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        self.stats.received += 1
        view = self.effective() if self.effective is not None else None
        down = self.cfg.down if view is None else bool(view["down"])
        max_size = self.cfg.max_size if view is None else int(view["max_size"])
        delay_ms = self.cfg.delay_ms if view is None else int(view["delay_ms"])
        if down:
            self.stats.dropped_down += 1
            return
        if len(data) > max_size:
            self.stats.dropped_oversize += 1
            return
        delay_s = delay_ms / 1000
        asyncio.get_running_loop().call_later(delay_s, self._echo, data, addr)

    def _echo(self, data: bytes, addr) -> None:  # noqa: ANN001
        if self.transport is None:
            return
        try:
            self.transport.sendto(data, addr)
            self.stats.echoed += 1
        except Exception:  # noqa: BLE001 - 对端消失等，模拟器不因此崩溃
            pass


class ConfigPatch(BaseModel):
    max_size: int | None = Field(default=None, ge=0, le=MAX_UDP_PAYLOAD)
    delay_ms: int | None = Field(default=None, ge=0, le=60_000)
    down: bool | None = None


def create_echo_app(
    line_name: str,
    udp_host: str,
    udp_port: int,
    max_size: int = MAX_UDP_PAYLOAD,
    delay_ms: int = 0,
    down: bool = False,
    drill_state_path: str | os.PathLike[str] | None = None,
) -> FastAPI:
    """构造一条回显线路：控制口随 HTTP 服务启动，UDP 口在 lifespan 内绑定。

    ``drill_state_path`` 为演练状态文件；为 None 时默认放在系统临时目录下，
    按线路名区分（测试可显式传入临时路径）。
    """
    cfg = LineConfig(max_size=max_size, delay_ms=delay_ms, down=down)
    stats = LineStats()
    state_path = drill_state_path or (
        os.path.join(
            os.environ.get("DRILL_STATE_DIR", "/tmp/udp-line-drills"),
            f"drill-{line_name}.json",
        )
    )
    drills = DrillManager(line=line_name, cfg=cfg, state_path=state_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: EchoProtocol(cfg, stats, lambda: drills.effective_at(time.time())),
            local_addr=(udp_host, udp_port),
        )
        app.state.udp_port = transport.get_extra_info("sockname")[1]
        await drills.startup()  # 重启续跑 / 停机期间到期则恢复开场前设置
        try:
            yield
        finally:
            await drills.close()
            transport.close()

    app = FastAPI(title=f"回显线路 {line_name}", lifespan=lifespan)
    app.state.cfg = cfg
    app.state.stats = stats
    app.state.line_name = line_name
    app.state.drills = drills

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True, "line": line_name, "udp_port": app.state.udp_port}

    @app.get("/config")
    async def get_config() -> dict:
        return {"line": line_name, **asdict(cfg)}

    @app.post("/config")
    async def set_config(patch: ConfigPatch) -> dict:
        try:
            await drills.update_config(patch.model_dump(exclude_none=True))
        except DrillActive as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"line": line_name, **asdict(cfg)}

    @app.get("/stats")
    async def get_stats() -> dict:
        return {"line": line_name, **asdict(stats)}

    @app.post("/stats/reset")
    async def reset_stats() -> dict:
        stats.received = stats.echoed = 0
        stats.dropped_oversize = stats.dropped_down = 0
        return {"line": line_name, **asdict(stats)}

    @app.post("/drill", status_code=202)
    async def start_drill(req: DrillRequest) -> dict:
        """按时间表开始一轮演练；同线已有演练时 409，落盘失败则不开始。"""
        try:
            return await drills.start(req.segments)
        except DrillActive as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        except DrillPersistError as exc:
            raise HTTPException(status_code=503, detail=str(exc))

    @app.get("/drill")
    async def get_drill() -> dict:
        """当前段、整轮结束时间与各段明细；无演练时 status 为 none。"""
        return await drills.status()

    @app.delete("/drill")
    async def cancel_drill() -> dict:
        """提前结束演练并恢复开场前设置；没有进行中的演练时 404。"""
        try:
            return await drills.cancel()
        except DrillNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except DrillPersistError as exc:
            raise HTTPException(status_code=503, detail=str(exc))

    return app


def main() -> None:
    import uvicorn

    app = create_echo_app(
        line_name=os.environ.get("LINE_NAME", "line"),
        udp_host=os.environ.get("UDP_HOST", "0.0.0.0"),
        udp_port=int(os.environ.get("UDP_PORT", "9001")),
        max_size=int(os.environ.get("MAX_SIZE", str(MAX_UDP_PAYLOAD))),
        delay_ms=int(os.environ.get("DELAY_MS", "0")),
        down=os.environ.get("DOWN", "").lower() in {"1", "true", "yes"},
        drill_state_path=os.environ.get("DRILL_STATE_PATH") or None,
    )
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("CTRL_PORT", "9101")),
        log_level=os.environ.get("LOG_LEVEL", "info"),
    )


if __name__ == "__main__":
    main()
