"""运行状态：当前线路、纪元(epoch)、建议值、测量任务生命周期。

切线语义：
- 每次真实切线（目标线路 ≠ 当前线路）都会令 epoch +1，并**立即撤下旧建议**；
- 测量启动时记录当时的 epoch，测量过程中或发布前发现 epoch 变化即作废——
  即使又切回原线路，也只接受切换之后开始的测量结果。
"""
from __future__ import annotations

import asyncio
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque

from .config import LineEndpoint
from .measurement import Aborted, Undetermined, find_max_payload
from .prober import UdpProber


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Recommendation:
    """一条已发布的建议：某条线路上可用的最大载荷字节数。"""

    line: str
    epoch: int
    max_payload: int
    lower: int
    upper: int
    probes_sent: int
    measurement_id: str
    published_at: datetime


class LineManager:
    """线路台账：当前激活线路、epoch、现行建议。"""

    def __init__(self, lines: dict[str, LineEndpoint]) -> None:
        if not lines:
            raise ValueError("至少需要一条线路")
        self.lines = lines
        self.active: str = next(iter(lines))
        self.epoch = 0
        self.recommendation: Recommendation | None = None
        self._lock = asyncio.Lock()

    async def switch(self, name: str) -> dict:
        """切线：立即撤下旧建议并推进 epoch。切到当前线路视为无操作。"""
        async with self._lock:
            changed = name != self.active
            if changed:
                self.active = name
                self.epoch += 1
                self.recommendation = None  # 立即撤下旧建议
            return self.snapshot(changed=changed)

    async def publish_if_current(
        self, epoch: int, rec: Recommendation
    ) -> Recommendation | None:
        """仅当测量启动时的 epoch 仍然有效才发布建议（与切线互斥）。"""
        async with self._lock:
            if epoch != self.epoch:
                return None
            self.recommendation = rec
            return rec

    def snapshot(self, changed: bool | None = None) -> dict:
        snap = {
            "active_line": self.active,
            "epoch": self.epoch,
            "known_lines": sorted(self.lines),
            "recommendation": (
                None
                if self.recommendation is None
                else {
                    "line": self.recommendation.line,
                    "epoch": self.recommendation.epoch,
                    "max_payload": self.recommendation.max_payload,
                    "range": {
                        "lower": self.recommendation.lower,
                        "upper": self.recommendation.upper,
                    },
                    "probes_sent": self.recommendation.probes_sent,
                    "measurement_id": self.recommendation.measurement_id,
                    "published_at": self.recommendation.published_at,
                }
            ),
        }
        if changed is not None:
            snap["changed"] = changed
        return snap


class MeasurementBusy(Exception):
    """已有测量在进行中。"""


@dataclass
class Measurement:
    id: str
    line: str
    epoch: int
    lower: int
    upper: int
    timeout_ms: int
    attempts: int
    status: str = "running"  # running/completed/undetermined/aborted/discarded/error
    max_payload: int | None = None
    probes_sent: int = 0
    published: bool = False
    detail: str = ""
    events: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=_utcnow)
    finished_at: datetime | None = None


class MeasurementService:
    """测量任务调度：单并发（物理线路共享），后台执行，结果按 epoch 认领。"""

    def __init__(self, manager: LineManager, history: int = 50) -> None:
        self.manager = manager
        self.records: dict[str, Measurement] = {}
        self._order: Deque[str] = deque(maxlen=history)
        self._run_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return self._run_lock.locked()

    async def start(
        self, lower: int, upper: int, timeout_ms: int, attempts: int
    ) -> Measurement:
        if self._run_lock.locked():
            raise MeasurementBusy()
        await self._run_lock.acquire()
        rec = Measurement(
            id=uuid.uuid4().hex[:12],
            line=self.manager.active,
            epoch=self.manager.epoch,
            lower=lower,
            upper=upper,
            timeout_ms=timeout_ms,
            attempts=attempts,
        )
        self.records[rec.id] = rec
        self._order.append(rec.id)
        asyncio.create_task(self._run(rec))
        return rec

    def get(self, measurement_id: str) -> Measurement | None:
        return self.records.get(measurement_id)

    def list_recent(self) -> list[Measurement]:
        return [self.records[i] for i in reversed(self._order) if i in self.records]

    async def _run(self, rec: Measurement) -> None:
        ep = self.manager.lines[rec.line]
        prober = UdpProber(ep.udp_host, ep.udp_port, rec.timeout_ms / 1000)
        epoch_at_start = rec.epoch

        def should_abort() -> bool:
            return self.manager.epoch != epoch_at_start

        try:
            await prober.open()
            best = await find_max_payload(
                prober,
                rec.lower,
                rec.upper,
                should_abort,
                rec.events,
                attempts=rec.attempts,
            )
            rec.max_payload = best
            rec.probes_sent = prober.sent
            published = await self.manager.publish_if_current(
                epoch_at_start,
                Recommendation(
                    line=rec.line,
                    epoch=epoch_at_start,
                    max_payload=best,
                    lower=rec.lower,
                    upper=rec.upper,
                    probes_sent=prober.sent,
                    measurement_id=rec.id,
                    published_at=_utcnow(),
                ),
            )
            if published is not None:
                rec.status = "completed"
                rec.published = True
                rec.detail = f"测量完成，已发布建议: {best}B (线路 {rec.line})"
            else:
                rec.status = "discarded"
                rec.detail = "测量期间发生切线，结果作废，未发布建议"
        except Undetermined as exc:
            rec.status = "undetermined"
            rec.probes_sent = prober.sent
            rec.detail = str(exc)
        except Aborted:
            rec.status = "aborted"
            rec.probes_sent = prober.sent
            rec.detail = "测量期间发生切线，已主动中止，结果作废"
        except Exception as exc:  # noqa: BLE001 - 兜底，保证锁一定释放
            rec.status = "error"
            rec.probes_sent = prober.sent
            rec.detail = f"测量内部错误: {exc!r}"
        finally:
            rec.finished_at = _utcnow()
            prober.close()
            self._run_lock.release()
