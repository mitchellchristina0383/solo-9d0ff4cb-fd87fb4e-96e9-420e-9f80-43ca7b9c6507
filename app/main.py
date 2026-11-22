"""FastAPI 入口：测量、建议查询、切线、线路模拟参数代理。

- ``POST /api/measure``            发起一轮测量（后台执行，202 返回任务）
- ``GET  /api/measurements/{id}``  查询测量进度与逐包事件
- ``GET  /api/recommendation``     查看当前建议（无建议时为 null）
- ``POST /api/line/switch``        切线：立即撤下旧建议并推进 epoch
- ``GET  /api/lines``              两条回显线路的实时配置与统计
- ``POST /api/lines/{name}/config`` 调整线路模拟参数（超长丢包/延迟/断线）
- ``GET  /docs``                   Swagger 交互式演示
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from .config import MAX_UDP_PAYLOAD, LineEndpoint, load_lines
from .state import (
    LineManager,
    Measurement,
    MeasurementBusy,
    MeasurementService,
)


class MeasureRequest(BaseModel):
    lower: int = Field(ge=1, le=MAX_UDP_PAYLOAD, description="载荷字节数下限")
    upper: int = Field(ge=1, le=MAX_UDP_PAYLOAD, description="载荷字节数上限")
    timeout_ms: int = Field(default=800, ge=50, le=10_000, description="单次探测超时")
    attempts: int = Field(default=3, ge=1, le=10, description="单长度连试次数")

    @model_validator(mode="after")
    def _check_range(self) -> "MeasureRequest":
        if self.lower > self.upper:
            raise ValueError("lower 不能大于 upper")
        return self


class SwitchRequest(BaseModel):
    line: str = Field(description="目标线路名，如 a 或 b")


class LineConfigPatch(BaseModel):
    max_size: int | None = Field(
        default=None, ge=0, le=MAX_UDP_PAYLOAD, description="超过该字节数的包丢弃"
    )
    delay_ms: int | None = Field(default=None, ge=0, le=60_000, description="回包延迟")
    down: bool | None = Field(default=None, description="断线开关")


def measurement_view(rec: Measurement) -> dict:
    return {
        "id": rec.id,
        "line": rec.line,
        "epoch": rec.epoch,
        "range": {"lower": rec.lower, "upper": rec.upper},
        "timeout_ms": rec.timeout_ms,
        "attempts": rec.attempts,
        "status": rec.status,
        "max_payload": rec.max_payload,
        "probes_sent": rec.probes_sent,
        "published": rec.published,
        "detail": rec.detail,
        "started_at": rec.started_at,
        "finished_at": rec.finished_at,
        "events": rec.events,
    }


def create_app(lines: dict[str, LineEndpoint] | None = None) -> FastAPI:
    endpoints = lines or load_lines()
    manager = LineManager(endpoints)
    service = MeasurementService(manager)
    http = httpx.AsyncClient(timeout=3.0)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await http.aclose()

    app = FastAPI(
        title="UDP 线路载荷测量服务",
        version="1.0.0",
        description=(
            "采集线路改路后，值班员用本服务实测当前线路上可用的最大 UDP 载荷。"
            "二分探测，次数不随范围线性增长；支持两条本地回显线路切换，"
            "可模拟超长丢包、回包延迟与断线。"
        ),
        lifespan=lifespan,
    )
    app.state.manager = manager
    app.state.service = service

    @app.get("/", summary="服务概览")
    async def root() -> dict:
        return {
            "service": "UDP 线路载荷测量服务",
            "docs": "/docs",
            "state": manager.snapshot(),
            "measurement_running": service.running,
        }

    @app.post("/api/measure", status_code=202, summary="发起一轮测量")
    async def start_measure(req: MeasureRequest) -> dict:
        try:
            rec = await service.start(
                req.lower, req.upper, req.timeout_ms, req.attempts
            )
        except MeasurementBusy:
            raise HTTPException(status_code=409, detail="已有测量在进行中，请稍后再试")
        return measurement_view(rec)

    @app.get("/api/measurements", summary="最近测量列表")
    async def list_measurements() -> list[dict]:
        return [measurement_view(r) for r in service.list_recent()]

    @app.get("/api/measurements/{measurement_id}", summary="查询测量进度")
    async def get_measurement(measurement_id: str) -> dict:
        rec = service.get(measurement_id)
        if rec is None:
            raise HTTPException(status_code=404, detail="测量不存在")
        return measurement_view(rec)

    @app.get("/api/recommendation", summary="当前建议")
    async def get_recommendation() -> dict:
        return manager.snapshot()

    @app.get("/api/line", summary="当前线路状态")
    async def get_line() -> dict:
        return manager.snapshot()

    @app.post("/api/line/switch", summary="切线（立即撤下旧建议）")
    async def switch_line(req: SwitchRequest) -> dict:
        if req.line not in manager.lines:
            raise HTTPException(
                status_code=404,
                detail=f"未知线路 {req.line!r}，可选: {sorted(manager.lines)}",
            )
        return await manager.switch(req.line)

    async def _fetch_line(name: str, ep: LineEndpoint) -> dict:
        base = {
            "name": name,
            "udp": ep.udp_addr,
            "ctrl_url": ep.ctrl_url,
            "active": name == manager.active,
        }
        try:
            cfg_resp, stats_resp = await asyncio.gather(
                http.get(f"{ep.ctrl_url}/config"), http.get(f"{ep.ctrl_url}/stats")
            )
            cfg_resp.raise_for_status()
            stats_resp.raise_for_status()
            return {
                **base,
                "reachable": True,
                "config": cfg_resp.json(),
                "stats": stats_resp.json(),
            }
        except Exception as exc:  # noqa: BLE001 - 线路控制口不可达也要能展示
            return {**base, "reachable": False, "error": str(exc)}

    @app.get("/api/lines", summary="两条线路的实时配置与统计")
    async def list_lines() -> list[dict]:
        return await asyncio.gather(
            *(_fetch_line(name, ep) for name, ep in manager.lines.items())
        )

    @app.post("/api/lines/{name}/config", summary="调整线路模拟参数")
    async def set_line_config(name: str, patch: LineConfigPatch) -> dict:
        ep = manager.lines.get(name)
        if ep is None:
            raise HTTPException(
                status_code=404,
                detail=f"未知线路 {name!r}，可选: {sorted(manager.lines)}",
            )
        try:
            resp = await http.post(
                f"{ep.ctrl_url}/config",
                json=patch.model_dump(exclude_none=True),
            )
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc))
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"线路控制口不可达: {exc}")
        return resp.json()

    return app


app = create_app()
