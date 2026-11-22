"""API 级测试：完整走 FastAPI 路由 + 两条真实回显线路（进程内）。"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from app.config import LineEndpoint
from app.main import create_app
from tests.echoutils import full_echo

MEASURE_BODY = {"lower": 512, "upper": 9000, "timeout_ms": 200, "attempts": 3}


@asynccontextmanager
async def stack(a_delay_ms=10, b_delay_ms=10):
    """两条回显线路 + 被测应用，全部跑在同一事件循环里。"""
    async with full_echo("a", max_size=1500, delay_ms=a_delay_ms) as ea, full_echo(
        "b", max_size=4000, delay_ms=b_delay_ms
    ) as eb:
        lines = {
            "a": LineEndpoint("a", "127.0.0.1", ea.udp_port, ea.ctrl_url),
            "b": LineEndpoint("b", "127.0.0.1", eb.udp_port, eb.ctrl_url),
        }
        app = create_app(lines)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, ea, eb


async def wait_done(client, measurement_id, timeout_s=20.0):
    for _ in range(int(timeout_s / 0.05)):
        resp = await client.get(f"/api/measurements/{measurement_id}")
        assert resp.status_code == 200
        body = resp.json()
        if body["status"] != "running":
            return body
        await asyncio.sleep(0.05)
    raise TimeoutError(f"测量 {measurement_id} 超时未结束")


async def test_measure_publishes_recommendation():
    """超长丢包场景：测出线路 a 的阈值 1500 并发布建议。"""
    async with stack() as (client, ea, _):
        resp = await client.post("/api/measure", json=MEASURE_BODY)
        assert resp.status_code == 202
        rec = await wait_done(client, resp.json()["id"])
        assert rec["status"] == "completed"
        assert rec["max_payload"] == 1500
        assert rec["published"] is True
        assert rec["probes_sent"] < 200  # 对数级，不随 8488 的范围宽度线性增长

        snap = (await client.get("/api/recommendation")).json()
        assert snap["recommendation"]["max_payload"] == 1500
        assert snap["recommendation"]["line"] == "a"
        assert ea.stats.dropped_oversize >= 3


async def test_switch_clears_recommendation_immediately():
    """切线立即撤下旧建议；换线后测出线路 b 的阈值 4000。"""
    async with stack() as (client, _, __):
        rec = await wait_done(
            client, (await client.post("/api/measure", json=MEASURE_BODY)).json()["id"]
        )
        assert rec["status"] == "completed"

        resp = await client.post("/api/line/switch", json={"line": "b"})
        assert resp.status_code == 200
        assert resp.json()["changed"] is True
        assert resp.json()["recommendation"] is None  # 立即撤下
        snap = (await client.get("/api/recommendation")).json()
        assert snap["recommendation"] is None
        assert snap["active_line"] == "b"

        rec = await wait_done(
            client, (await client.post("/api/measure", json=MEASURE_BODY)).json()["id"]
        )
        assert rec["max_payload"] == 4000
        assert (await client.get("/api/recommendation")).json()["recommendation"][
            "max_payload"
        ] == 4000


async def test_undetermined_when_line_down_keeps_old_recommendation():
    """断线：最短包也超时 → undetermined，本轮不发布建议（旧建议保留）。"""
    async with stack() as (client, _, __):
        rec = await wait_done(
            client, (await client.post("/api/measure", json=MEASURE_BODY)).json()["id"]
        )
        assert rec["status"] == "completed"  # 线路 a 上已有 1500 的建议

        resp = await client.post("/api/lines/a/config", json={"down": True})
        assert resp.status_code == 200
        assert resp.json()["down"] is True

        rec = await wait_done(
            client, (await client.post("/api/measure", json=MEASURE_BODY)).json()["id"]
        )
        assert rec["status"] == "undetermined"
        assert rec["published"] is False
        assert "无法判定" in rec["detail"]
        # 本轮不发布建议：旧建议原样保留
        assert (await client.get("/api/recommendation")).json()["recommendation"][
            "max_payload"
        ] == 1500


async def test_switch_during_measurement_invalidates_result():
    """测量中切线（含切回原线路）：旧测量作废，建议被撤下。"""
    async with stack(a_delay_ms=60) as (client, _, __):
        body = {**MEASURE_BODY, "upper": 65507, "timeout_ms": 300}
        mid = (await client.post("/api/measure", json=body)).json()["id"]
        await asyncio.sleep(0.3)  # 让测量跑起来

        # a → b → a：即使切回原线路，epoch 已变，旧测量必须作废
        await client.post("/api/line/switch", json={"line": "b"})
        assert (await client.get("/api/recommendation")).json()["recommendation"] is None
        await client.post("/api/line/switch", json={"line": "a"})

        rec = await wait_done(client, mid)
        assert rec["status"] in ("aborted", "discarded")
        assert rec["published"] is False
        assert (await client.get("/api/recommendation")).json()["recommendation"] is None

        # 切换后重新开始测量才有效
        rec = await wait_done(
            client, (await client.post("/api/measure", json=MEASURE_BODY)).json()["id"]
        )
        assert rec["status"] == "completed"
        assert rec["max_payload"] == 1500
        assert (await client.get("/api/recommendation")).json()["recommendation"][
            "max_payload"
        ] == 1500


async def test_concurrent_measurement_rejected():
    """单并发：测量进行中再发起返回 409。"""
    async with stack(a_delay_ms=60) as (client, _, __):
        body = {**MEASURE_BODY, "upper": 65507}
        first = await client.post("/api/measure", json=body)
        assert first.status_code == 202
        second = await client.post("/api/measure", json=body)
        assert second.status_code == 409
        await wait_done(client, first.json()["id"])


async def test_line_config_proxy_and_listing():
    """通过主服务代理调整线路模拟参数，并能读到统计。"""
    async with stack() as (client, ea, _):
        resp = await client.post(
            "/api/lines/a/config", json={"max_size": 2000, "delay_ms": 5}
        )
        assert resp.status_code == 200
        assert resp.json()["max_size"] == 2000
        assert ea.cfg.max_size == 2000

        lines = (await client.get("/api/lines")).json()
        by_name = {l["name"]: l for l in lines}
        assert by_name["a"]["reachable"] is True
        assert by_name["a"]["config"]["max_size"] == 2000
        assert by_name["b"]["config"]["max_size"] == 4000

        resp = await client.post("/api/lines/nope/config", json={"down": True})
        assert resp.status_code == 404


async def test_validation():
    """入参校验：lower>upper、越界长度、未知线路。"""
    async with stack() as (client, _, __):
        assert (
            await client.post("/api/measure", json={"lower": 100, "upper": 50})
        ).status_code == 422
        assert (
            await client.post(
                "/api/measure", json={"lower": 1, "upper": 70000}
            )
        ).status_code == 422
        assert (
            await client.post("/api/line/switch", json={"line": "z"})
        ).status_code == 404
