"""回显线路分段演练：落盘、按段切换、跨段回包、互斥、重启续跑。"""
from __future__ import annotations

import asyncio
import json
import os
import socket
import time

import httpx
import pytest

from app.config import LineEndpoint
from app.drill import MAX_DRILL_SEGMENTS
from app.main import create_app
from tests.echoutils import full_echo


async def wait_status(client, path, status, timeout_s=5.0):
    deadline = time.monotonic() + timeout_s
    body = None
    while time.monotonic() < deadline:
        body = (await client.get(path)).json()
        if body.get("status") == status:
            return body
        await asyncio.sleep(0.02)
    raise TimeoutError(f"等待演练状态 {status} 超时: {body}")


async def wait_current_segment(client, path, index, timeout_s=5.0):
    deadline = time.monotonic() + timeout_s
    body = None
    while time.monotonic() < deadline:
        body = (await client.get(path)).json()
        if body.get("status") == "running" and body.get("current_segment") == index:
            return body
        await asyncio.sleep(0.02)
    raise TimeoutError(f"等待第 {index} 段超时: {body}")


def udp_socket() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    return sock


async def test_main_app_proxies_drill_endpoints(tmp_path):
    """主服务代理：/api/lines/{name}/drill 的开启、查询、结束与未知线路。"""
    async with full_echo(
        "a", max_size=1500, delay_ms=30, drill_state_path=tmp_path / "a.json"
    ) as ea:
        lines = {"a": LineEndpoint("a", "127.0.0.1", ea.udp_port, ea.ctrl_url)}
        app = create_app(lines)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            assert (await client.get("/api/lines/a/drill")).json()["status"] == "none"
            resp = await client.post(
                "/api/lines/a/drill",
                json={"segments": [{"duration_s": 2.0, "delay_ms": 150}]},
            )
            assert resp.status_code == 202, resp.text
            body = (await client.get("/api/lines/a/drill")).json()
            assert body["status"] == "running"
            assert body["current_segment"] == 1
            assert body["current"]["remaining_s"] > 0
            assert body["ends_at"] and body["ends_at_epoch"] > time.time()
            # 演练期间改同线参数由主服务透传 409
            assert (
                await client.post("/api/lines/a/config", json={"down": True})
            ).status_code == 409
            assert (await client.delete("/api/lines/a/drill")).status_code == 200
            assert ea.cfg.delay_ms == 30 and ea.cfg.down is False
            assert (
                await client.get("/api/lines/z/drill")
            ).status_code == 404


async def test_drill_persisted_before_start_and_visible(tmp_path):
    """落盘文件先出现，才允许线路进入第一段；查询可见当前段与结束时间。"""
    path = tmp_path / "drill.json"
    async with full_echo("a", max_size=1500, delay_ms=30, drill_state_path=path) as echo:
        assert not path.exists()
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            resp = await client.post(
                "/drill",
                json={"segments": [{"duration_s": 1.0, "delay_ms": 200}]},
            )
            assert resp.status_code == 202, resp.text
            body = resp.json()
            assert body["status"] == "running"
            assert body["current_segment"] == 1
            assert body["segments"][0]["delay_ms"] == 200
            assert body["ends_at_epoch"] - body["started_at_epoch"] == pytest.approx(1.0)
            assert 0 < body["remaining_s"] <= 1.0
            # 生效：cfg 已切到本段延迟，max_size 保持不变
            assert echo.cfg.delay_ms == 200
            assert echo.cfg.down is False
            assert echo.cfg.max_size == 1500
            # 落盘内容可复核
            on_disk = json.loads(path.read_text())
            assert on_disk["status"] == "running"
            assert on_disk["baseline"] == {
                "max_size": 1500,
                "delay_ms": 30,
                "down": False,
            }
            assert on_disk["segments"][0]["delay_ms"] == 200
            assert on_disk["ends_at"] == pytest.approx(body["ends_at_epoch"])


async def test_drill_rejects_config_and_second_round(tmp_path):
    """演练中：同线改参数 409，再开一轮 409；到期后恢复开场前设置。"""
    async with full_echo(
        "a", max_size=1500, delay_ms=30, drill_state_path=tmp_path / "a.json"
    ) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            resp = await client.post(
                "/drill", json={"segments": [{"duration_s": 0.5, "delay_ms": 500}]}
            )
            assert resp.status_code == 202
            assert (
                await client.post("/config", json={"delay_ms": 5})
            ).status_code == 409
            assert (
                await client.post(
                    "/drill", json={"segments": [{"duration_s": 0.5, "down": True}]}
                )
            ).status_code == 409
            # 等整轮结束，自动恢复开场前设置
            await wait_status(client, "/drill", "completed")
            assert echo.cfg.delay_ms == 30
            assert echo.cfg.down is False
            assert echo.cfg.max_size == 1500
            # 到期后可以改参数、可以再开一轮
            assert (
                await client.post("/config", json={"delay_ms": 5})
            ).status_code == 200
            assert (
                await client.post(
                    "/drill", json={"segments": [{"duration_s": 0.5, "down": True}]}
                )
            ).status_code == 202
            assert echo.cfg.down is True
            assert (await client.delete("/drill")).status_code == 200
            # cancel 恢复的是新轮次开场前的线路设置（上一步改成的 5ms/未断线）
            assert echo.cfg.down is False
            assert echo.cfg.delay_ms == 5
            assert echo.cfg.max_size == 1500


async def test_drill_cancel_restores_baseline(tmp_path):
    """提前结束：恢复开场前设置，无演练时再结束返回 404。"""
    async with full_echo(
        "a", max_size=1500, delay_ms=30, drill_state_path=tmp_path / "a.json"
    ) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            await client.post(
                "/drill", json={"segments": [{"duration_s": 5.0, "down": True}]}
            )
            assert echo.cfg.down is True
            resp = await client.delete("/drill")
            assert resp.status_code == 200
            assert resp.json()["status"] == "cancelled"
            assert echo.cfg.down is False
            assert echo.cfg.delay_ms == 30
            assert (await client.delete("/drill")).status_code == 404


async def test_drill_segment_validation(tmp_path):
    """段数 1~8、断线段不能带延迟、延时段必须给延迟。"""
    async with full_echo("a", drill_state_path=tmp_path / "a.json") as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            too_many = [{"duration_s": 0.1, "down": True}] * (
                MAX_DRILL_SEGMENTS + 1
            )
            assert (await client.post("/drill", json={"segments": too_many})).status_code == 422
            assert (await client.post("/drill", json={"segments": []})).status_code == 422
            assert (
                await client.post(
                    "/drill",
                    json={"segments": [{"duration_s": 0.1, "down": True, "delay_ms": 5}]},
                )
            ).status_code == 422
            assert (
                await client.post(
                    "/drill", json={"segments": [{"duration_s": 0.1}]}
                )
            ).status_code == 422
            assert (
                await client.post(
                    "/drill", json={"segments": [{"duration_s": 0, "down": True}]}
                )
            ).status_code == 422
            # 正好 8 段是允许的（先取消避免拖住测试）
            ok = await client.post(
                "/drill",
                json={"segments": [{"duration_s": 0.1, "delay_ms": 0}] * MAX_DRILL_SEGMENTS},
            )
            assert ok.status_code == 202
            await client.delete("/drill")


async def test_queued_reply_survives_transition_into_down_segment(tmp_path):
    """延迟段入队的回包，跨进断线段后仍照原决定发出；断线段新到的包被丢。"""
    async with full_echo(
        "a", max_size=1500, delay_ms=0, drill_state_path=tmp_path / "a.json"
    ) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            await client.post(
                "/drill",
                json={
                    "segments": [
                        {"duration_s": 0.2, "delay_ms": 500},
                        {"duration_s": 0.6, "down": True},
                    ]
                },
            )
            await wait_current_segment(client, "/drill", 1)
            loop = asyncio.get_running_loop()
            sock = udp_socket()
            try:
                payload = os.urandom(64)
                await loop.sock_sendto(sock, payload, ("127.0.0.1", echo.udp_port))

                body = await wait_current_segment(client, "/drill", 2)
                assert body["current"]["down"] is True

                # 断线段内到达的新包：不应有回包
                await loop.sock_sendto(sock, os.urandom(64), ("127.0.0.1", echo.udp_port))
                try:
                    data, _ = await asyncio.wait_for(loop.sock_recvfrom(sock, 65535), 0.1)
                except asyncio.TimeoutError:
                    data = None
                assert data is None, "断线段内到达的包不应有回包"

                # 但第一段入队的回包仍会按原延迟发出（此刻已处于断线段）
                received = None
                deadline = time.monotonic() + 1.0
                while time.monotonic() < deadline:
                    try:
                        data, _ = await asyncio.wait_for(
                            loop.sock_recvfrom(sock, 65535), 0.1
                        )
                    except asyncio.TimeoutError:
                        continue
                    if data == payload:
                        received = data
                        break
                assert received == payload, "跨段前排队的回包必须照原决定发出"
                assert echo.cfg.down is True
            finally:
                sock.close()


async def test_persist_failure_means_drill_never_starts(tmp_path):
    """落盘失败：整轮不开始，线路设置保持开场前，查询为 none。"""
    blocked = tmp_path / "blocked"
    blocked.mkdir()  # 让状态“文件”其实是个目录，open() 必然失败
    async with full_echo(
        "a", max_size=1500, delay_ms=30, drill_state_path=blocked
    ) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            resp = await client.post(
                "/drill", json={"segments": [{"duration_s": 1.0, "delay_ms": 500}]}
            )
            assert resp.status_code == 503
            assert "落盘失败" in resp.json()["detail"]
            assert echo.cfg.delay_ms == 30 and echo.cfg.down is False
            body = (await client.get("/drill")).json()
            assert body["status"] == "none"
            # 没有落盘、没有生效，因此允许改参数
            assert (
                await client.post("/config", json={"delay_ms": 7})
            ).status_code == 200


async def test_payload_drop_rule_preserved_during_drill(tmp_path):
    """演练只改 down/delay：开场前 max_size=1500，延迟段内超大包仍被丢。"""
    async with full_echo(
        "a", max_size=1500, delay_ms=0, drill_state_path=tmp_path / "a.json"
    ) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            await client.post(
                "/drill", json={"segments": [{"duration_s": 2.0, "delay_ms": 100}]}
            )
            loop = asyncio.get_running_loop()
            sock = udp_socket()
            try:
                # 1500B：延迟段内应收发成功
                ok = os.urandom(1500)
                await loop.sock_sendto(sock, ok, ("127.0.0.1", echo.udp_port))
                data, _ = await asyncio.wait_for(loop.sock_recvfrom(sock, 65535), 1.0)
                assert data == ok
                # 1501B：超过开场前 max_size，演练中仍按原规则丢弃
                echo.stats.dropped_oversize = 0
                await loop.sock_sendto(
                    sock, os.urandom(1501), ("127.0.0.1", echo.udp_port)
                )
                try:
                    data, _ = await asyncio.wait_for(
                        loop.sock_recvfrom(sock, 65535), 0.4
                    )
                except asyncio.TimeoutError:
                    data = None
                assert data is None
                assert echo.stats.dropped_oversize == 1
                assert echo.cfg.max_size == 1500
            finally:
                sock.close()
                await client.delete("/drill")


async def test_corrupt_state_file_is_ignored(tmp_path):
    """状态文件损坏：进程仍能启动，按无演练处理，不阻塞改参数。"""
    path = tmp_path / "drill.json"
    path.write_text("{ not valid json")
    async with full_echo("a", drill_state_path=path) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            body = (await client.get("/drill")).json()
            assert body["status"] == "none"
            assert (await client.post("/config", json={"delay_ms": 3})).status_code == 200


async def test_restart_continues_schedule_and_skips_elapsed(tmp_path):
    """重启：按绝对时间续跑，停机时间照算，已过段跳过；到期则恢复开场前设置。"""
    path = tmp_path / "drill.json"
    async with full_echo("a", max_size=1500, delay_ms=30, drill_state_path=path) as echo:
        async with httpx.AsyncClient(base_url=echo.ctrl_url) as client:
            resp = await client.post(
                "/drill",
                json={
                    "segments": [
                        {"duration_s": 0.2, "delay_ms": 300},
                        {"duration_s": 5.0, "down": True},
                    ]
                },
            )
            assert resp.status_code == 202
            ends_at = resp.json()["ends_at_epoch"]
            await wait_current_segment(client, "/drill", 2)
        # 退出上下文只关闭监控任务，不修改落盘状态（模拟进程停机）

    # 重启：同一状态文件、同一线路；停机时间照算，直接进入第 2 段（断线）
    async with full_echo(
        "a", max_size=1500, delay_ms=999, drill_state_path=path
    ) as echo2:
        assert echo2.cfg.down is True
        assert echo2.cfg.delay_ms == 0
        async with httpx.AsyncClient(base_url=echo2.ctrl_url) as client:
            body = (await client.get("/drill")).json()
            assert body["status"] == "running"
            assert body["current_segment"] == 2
            assert body["ends_at_epoch"] == pytest.approx(ends_at)

    # 再停机到整轮过期之后：重启时立即恢复开场前设置并标记 completed
    _shift_schedule_into_past(path, ends_at)
    async with full_echo(
        "a", max_size=1500, delay_ms=999, down=True, drill_state_path=path
    ) as echo3:
        # 开场前设置来自落盘 baseline（30ms、未断线），覆盖了本次进程的启动参数
        assert echo3.cfg.down is False
        assert echo3.cfg.delay_ms == 30
        async with httpx.AsyncClient(base_url=echo3.ctrl_url) as client:
            body = (await client.get("/drill")).json()
            assert body["status"] == "completed"
            assert "到期" in body["reason"]


def _shift_schedule_into_past(path, ends_at: float) -> None:
    """把落盘时间表整体平移到过去，模拟停机时间超过整轮剩余时长。"""
    data = json.loads(path.read_text())
    shift = ends_at + 10.0
    data["started_at"] -= shift
    data["ends_at"] -= shift
    path.write_text(json.dumps(data))
