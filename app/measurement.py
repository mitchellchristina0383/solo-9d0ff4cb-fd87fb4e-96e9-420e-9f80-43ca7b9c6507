"""测量引擎：在 [lower, upper] 内用二分查找可用的最大 UDP 载荷字节数。

判定规则（与运维约定一致）：
- 某长度连试 ``attempts`` 次都收不到匹配回包 → 不直接判死，
  改发范围内最短包（lower 字节）确认线路仍通；
  - 最短包收到匹配回包 → 该长度才判为不可用；
  - 最短包连试 ``attempts`` 次也超时 → 线路疑似中断，抛 :class:`Undetermined`，
    本轮不发布建议。
- 探测次数为 O(log(upper-lower))，不随范围宽度线性增长。
- ``should_abort()`` 为真（测量期间发生切线）时抛 :class:`Aborted`。
"""
from __future__ import annotations

from typing import Awaitable, Callable, MutableSequence

from .prober import UdpProber


class Undetermined(Exception):
    """线路疑似中断，本轮无法判定。"""


class Aborted(Exception):
    """测量期间发生切线，结果必须作废。"""


ShouldAbort = Callable[[], bool]
EventLog = MutableSequence[str]


async def try_length(
    prober: UdpProber,
    size: int,
    attempts: int,
    should_abort: ShouldAbort,
    events: EventLog,
) -> bool:
    """对单个长度连试 ``attempts`` 次，任意一次收到匹配回包即为可用。"""
    for i in range(1, attempts + 1):
        if should_abort():
            raise Aborted()
        rtt = await prober.roundtrip(size)
        if rtt is not None:
            events.append(f"len={size}B 第{i}次探测收到匹配回包 rtt={rtt * 1000:.1f}ms")
            return True
        events.append(f"len={size}B 第{i}次探测超时({prober.timeout_s * 1000:.0f}ms)")
    return False


async def find_max_payload(
    prober: UdpProber,
    lower: int,
    upper: int,
    should_abort: ShouldAbort,
    events: EventLog,
    attempts: int = 3,
) -> int:
    """二分查找范围内可用的最大载荷字节数。

    :raises Undetermined: 最短包连试多次仍无回包，线路疑似中断。
    :raises Aborted: 测量期间发生切线。
    """
    lo, hi = lower, upper
    best: int | None = None
    lower_ever_ok = False
    events.append(
        f"开始二分测量: 范围[{lower}, {upper}]B, 单长度连试{attempts}次, "
        f"单次超时{prober.timeout_s * 1000:.0f}ms"
    )
    while lo <= hi:
        if should_abort():
            raise Aborted()
        mid = (lo + hi) // 2
        events.append(f"二分探测 mid={mid}B (当前区间[{lo}, {hi}])")
        if await try_length(prober, mid, attempts, should_abort, events):
            best = mid
            if mid == lower:
                lower_ever_ok = True
            lo = mid + 1
            continue
        # mid 连试失败：先发范围内最短包确认线路仍通，再判 mid 不可用。
        events.append(
            f"len={mid}B 连续{attempts}次无匹配回包, 改发最短包 {lower}B 确认线路"
        )
        if await try_length(prober, lower, attempts, should_abort, events):
            lower_ever_ok = True
            events.append(f"最短包 {lower}B 有匹配回包 → 判定 {mid}B 不可用")
            hi = mid - 1
        else:
            raise Undetermined(
                f"最短包 {lower}B 连续{attempts}次超时, 线路疑似中断, 本轮无法判定"
            )
    if best is None:
        # 仅当线路确认还通、但连 lower 都偶发失败时才可能走到这里。
        if lower_ever_ok:
            best = lower
        else:
            raise Undetermined("范围内未找到可用长度且线路状态异常, 本轮无法判定")
    events.append(f"测量完成: 可用最大载荷 {best}B, 共发出 {prober.sent} 个探测包")
    return best
