"""引擎级测试：二分查找、三次确认、最短包探活、断线无法判定、切线中止。"""
from __future__ import annotations

import math

import pytest

from app.measurement import Aborted, Undetermined, find_max_payload
from app.prober import UdpProber
from tests.echoutils import raw_echo

TIMEOUT_S = 0.2


async def run_engine(port, lower, upper, timeout_s=TIMEOUT_S, attempts=3, abort_after=None):
    """跑一轮测量，返回 (结果, 事件, 探测包数)。abort_after: 第 N 次检查后中止。"""
    prober = UdpProber("127.0.0.1", port, timeout_s)
    await prober.open()
    events: list[str] = []
    calls = {"n": 0}

    def should_abort():
        calls["n"] += 1
        return abort_after is not None and calls["n"] > abort_after

    try:
        best = await find_max_payload(
            prober, lower, upper, should_abort, events, attempts=attempts
        )
    finally:
        prober.close()
    return best, events, prober.sent


async def test_binary_search_finds_exact_threshold():
    """超长丢包：max_size=1500 的线路上，[512, 9000] 应测出 1500。"""
    async with raw_echo(max_size=1500) as echo:
        best, events, sent = await run_engine(echo.port, 512, 9000)
    assert best == 1500
    # 探测次数必须是对数级：远小于范围宽度 8488
    width = 9000 - 512 + 1
    assert sent <= 6 * (math.ceil(math.log2(width)) + 2)
    assert sent < width // 10
    # 超长包确实被丢过，且触发过最短包探活
    assert echo.stats.dropped_oversize >= 3
    assert any("最短包" in e for e in events)
    assert echo.stats.echoed > 0


async def test_probe_count_scales_logarithmically_with_range():
    """范围扩到 [1, 65507]，探测次数仍是对数级，不随宽度线性增长。"""
    async with raw_echo(max_size=40000) as echo:
        best, events, sent = await run_engine(echo.port, 1, 65507)
    assert best == 40000
    assert sent < 200  # 线性扫描需要 65507 次
    assert echo.stats.dropped_oversize >= 3


async def test_lower_equals_upper():
    """区间退化为单点：直接测该点。"""
    async with raw_echo(max_size=1000) as echo:
        best, _, sent = await run_engine(echo.port, 1000, 1000)
    assert best == 1000
    assert sent == 1


async def test_undetermined_when_line_down():
    """断线：任何长度都超时，最短包探活也超时 → Undetermined。"""
    async with raw_echo(down=True) as echo:
        prober = UdpProber("127.0.0.1", echo.port, TIMEOUT_S)
        await prober.open()
        events: list[str] = []
        with pytest.raises(Undetermined):
            await find_max_payload(prober, 512, 9000, lambda: False, events)
        prober.close()
    assert echo.stats.dropped_down > 0
    assert echo.stats.echoed == 0
    assert any("最短包" in e for e in events)


async def test_delay_within_timeout_still_works():
    """回包延迟 120ms、超时 500ms：测量正常完成。"""
    async with raw_echo(max_size=2000, delay_ms=120) as echo:
        best, _, _ = await run_engine(echo.port, 512, 4000, timeout_s=0.5)
    assert best == 2000


async def test_delay_beyond_timeout_is_undetermined():
    """回包延迟 400ms、超时 100ms：等同于断线 → Undetermined。"""
    async with raw_echo(delay_ms=400) as echo:
        prober = UdpProber("127.0.0.1", echo.port, 0.1)
        await prober.open()
        with pytest.raises(Undetermined):
            await find_max_payload(prober, 512, 9000, lambda: False, [])
        prober.close()


async def test_abort_stops_measurement():
    """切线信号（should_abort）触发后立即中止。"""
    async with raw_echo(max_size=1500, delay_ms=20) as echo:
        prober = UdpProber("127.0.0.1", echo.port, TIMEOUT_S)
        await prober.open()
        with pytest.raises(Aborted):
            await find_max_payload(
                prober, 512, 9000, lambda: True, [], attempts=3
            )
        prober.close()
