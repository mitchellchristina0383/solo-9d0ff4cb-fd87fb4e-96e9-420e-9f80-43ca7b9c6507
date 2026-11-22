"""回显线路分段演练：最多 8 段，每段选择“断线”或“指定回包延迟”。

语义（与值班员约定一致）：

- **先落盘后生效**：整轮时间表、开场前的线路设置先原子写入磁盘，写成功后才
  把第一段应用到线路；落盘失败则整轮不开始，线路设置保持原样。
- **绝对时间表**：开始时记录 Unix 时间戳，每段按顺序累计时长。查询可见当前
  段号、当前段剩余时间与整轮结束时间。
- **到达时判定**：UDP 包按到达那一刻的段配置处理（断线则丢，否则按该段的
  延迟排队）。已排队的延迟回包由回显协议自己的定时器发送，后续切段——包括
  切到断线段——不会取消它们，照原决定发出。
- **载荷丢包规则不变**：演练只改 ``down``/``delay_ms``，``max_size`` 保持开场
  前的值；跨重启也会从落盘状态里恢复该值。
- **重启续跑**：进程重启后读取同一份状态文件，用墙钟计算当前段；停机时间照
  算，已经过去的段直接跳过。若重启时整轮已过期，立即恢复开场前设置并记为
  ``completed``。
- **互斥**：同一条线路演练进行中，拒绝改模拟参数，也拒绝再开一轮（均返回
  409，由回显控制口实现）。到期或提前取消后恢复开场前设置。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence

from pydantic import BaseModel, Field, model_validator

MAX_DRILL_SEGMENTS = 8
MIN_SEGMENT_DURATION_S = 0.1
MAX_SEGMENT_DURATION_S = 24 * 60 * 60
MAX_DRILL_DELAY_MS = 60_000

logger = logging.getLogger(__name__)


class _ConfigLike(Protocol):
    max_size: int
    delay_ms: int
    down: bool


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class DrillSegment(BaseModel):
    """演练的一段：要么断线，要么指定回包延迟。"""

    duration_s: float = Field(
        ge=MIN_SEGMENT_DURATION_S,
        le=MAX_SEGMENT_DURATION_S,
        description="本段时长（秒）",
    )
    down: bool = Field(default=False, description="true 表示本段断线（丢弃所有到达包）")
    delay_ms: int | None = Field(
        default=None,
        ge=0,
        le=MAX_DRILL_DELAY_MS,
        description="本段回包延迟（毫秒）；选择断线时不要填",
    )

    @model_validator(mode="after")
    def _either_down_or_delay(self) -> "DrillSegment":
        if self.down and self.delay_ms is not None:
            raise ValueError("down=true 表示断线，不能再指定 delay_ms")
        if not self.down and self.delay_ms is None:
            raise ValueError("非断线段必须指定 delay_ms")
        return self


class DrillRequest(BaseModel):
    segments: list[DrillSegment] = Field(
        min_length=1,
        max_length=MAX_DRILL_SEGMENTS,
        description=f"按顺序执行的 1~{MAX_DRILL_SEGMENTS} 段",
    )


class DrillActive(Exception):
    """同一条线路已有演练在进行中。"""


class DrillNotFound(Exception):
    """该线路没有进行中的演练。"""


class DrillPersistError(Exception):
    """演练状态无法落盘。"""


@dataclass
class DrillState:
    line: str
    status: str  # running / completed / cancelled
    segments: list[dict[str, Any]]
    baseline: dict[str, Any]
    started_at: float
    ends_at: float
    completed_at: float | None = None
    reason: str = ""


@dataclass
class DrillManager:
    """一条回显线路的演练调度器（与 :class:`EchoProtocol` 共享同一个 cfg 对象）。"""

    line: str
    cfg: _ConfigLike
    state_path: str | os.PathLike[str]
    tick_s: float = 0.05
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _state: DrillState | None = field(default=None, init=False, repr=False)
    _monitor_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _applied_index: int | None = field(default=None, init=False, repr=False)
    last_persist_error: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.state_path = Path(self.state_path)

    # ------------------------------------------------------------------ 生命周期

    async def startup(self) -> None:
        """进程启动时读取落盘状态，按墙钟续跑或补做“到期恢复”。"""
        async with self._lock:
            state = self._load()
            if state is None:
                return
            self._state = state
            if state.status != "running":
                # 已完成/已取消的轮次只作为查询记录保留，不覆盖本次进程的启动配置。
                return

            now = time.time()
            if now >= state.ends_at:
                self._restore(state.baseline)
                state.status = "completed"
                state.completed_at = state.ends_at
                state.reason = "重启时发现整轮已在停机期间到期，已恢复开场前设置"
                self._persist_or_warn(state)
                logger.info(
                    "线路 %s 演练在停机期间已到期，已恢复开场前设置", self.line
                )
                return

            # 停机时间照算：直接应用重启时刻所在的段，不重放之前的段。
            self.cfg.max_size = int(state.baseline["max_size"])
            index = self._index_at(state, now)
            self._apply_segment(state, index)
            self._applied_index = index
            logger.info(
                "线路 %s 演练重启续跑：当前第 %d/%d 段，整轮结束于 %s",
                self.line,
                index + 1,
                len(state.segments),
                _iso(state.ends_at),
            )
            self._ensure_monitor()

    async def close(self) -> None:
        """进程退出：停止监控任务，但保留落盘状态供下次启动续跑。"""
        task = self._monitor_task
        self._monitor_task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # ------------------------------------------------------------------ 对外操作

    async def start(self, segments: Sequence[DrillSegment]) -> dict:
        """校验互斥后先原子落盘，再应用第一段。"""
        if not 1 <= len(segments) <= MAX_DRILL_SEGMENTS:
            raise ValueError(f"演练段数必须在 1~{MAX_DRILL_SEGMENTS} 之间")

        async with self._lock:
            await self._finalize_if_due_locked()
            if self._state is not None and self._state.status == "running":
                raise DrillActive(
                    f"线路 {self.line} 已有演练进行中（结束于 {_iso(self._state.ends_at)}），"
                    "不能重复开启"
                )

            started_at = time.time()
            cursor = started_at
            stored_segments: list[dict[str, Any]] = []
            for index, segment in enumerate(segments, start=1):
                duration = float(segment.duration_s)
                down = bool(segment.down)
                stored_segments.append(
                    {
                        "index": index,
                        "duration_s": duration,
                        "down": down,
                        "delay_ms": 0 if down else int(segment.delay_ms or 0),
                        "starts_at": cursor,
                        "ends_at": cursor + duration,
                    }
                )
                cursor += duration

            state = DrillState(
                line=self.line,
                status="running",
                segments=stored_segments,
                baseline=self._baseline(),
                started_at=started_at,
                ends_at=cursor,
            )
            # 关键顺序：落盘成功之前绝不改线路设置。
            try:
                self._persist(state)
            except OSError as exc:
                self.last_persist_error = str(exc)
                raise DrillPersistError(
                    f"演练时间表落盘失败，未开始：{exc}"
                ) from exc
            self.last_persist_error = None
            self._state = state
            self._applied_index = 0
            self._apply_segment(state, 0)
            self._ensure_monitor()
            logger.info(
                "线路 %s 演练开始：%d 段，结束于 %s",
                self.line,
                len(stored_segments),
                _iso(state.ends_at),
            )
            return self._view(state)

    async def status(self) -> dict:
        async with self._lock:
            await self._finalize_if_due_locked()
            if self._state is None:
                return {
                    "line": self.line,
                    "status": "none",
                    "started_at": None,
                    "started_at_epoch": None,
                    "ends_at": None,
                    "ends_at_epoch": None,
                    "completed_at": None,
                    "reason": "",
                    "elapsed_s": None,
                    "remaining_s": None,
                    "current_segment": None,
                    "current": None,
                    "segment_count": 0,
                    "segments": [],
                    "baseline": None,
                    "config": self._config_view(),
                    "persist_error": self.last_persist_error,
                }
            return self._view(self._state)

    async def cancel(self) -> dict:
        """提前结束演练：先持久化取消状态，再恢复开场前设置。"""
        async with self._lock:
            await self._finalize_if_due_locked()
            state = self._state
            if state is None or state.status != "running":
                raise DrillNotFound(f"线路 {self.line} 当前没有进行中的演练")

            previous = (state.status, state.completed_at, state.reason)
            state.status = "cancelled"
            state.completed_at = time.time()
            state.reason = "值班员提前结束"
            try:
                self._persist(state)
            except OSError as exc:
                state.status, state.completed_at, state.reason = previous
                self.last_persist_error = str(exc)
                raise DrillPersistError(
                    f"取消状态落盘失败，演练仍继续：{exc}"
                ) from exc

            self.last_persist_error = None
            self._restore(state.baseline)
            self._applied_index = None
            logger.info("线路 %s 演练被提前结束，已恢复开场前设置", self.line)
            return self._view(state)

    async def update_config(self, patch: dict[str, Any]) -> None:
        """演练期间禁止改同线参数；到期瞬间先完成恢复再判断。"""
        async with self._lock:
            await self._finalize_if_due_locked()
            if self._state is not None and self._state.status == "running":
                raise DrillActive(
                    f"线路 {self.line} 演练进行中（结束于 {_iso(self._state.ends_at)}），"
                    "禁止修改模拟参数"
                )
            for key, value in patch.items():
                setattr(self.cfg, key, value)

    # ------------------------------------------------------------------ 时间/段

    def effective_at(self, now: float) -> dict[str, Any] | None:
        """给 UDP 收包路径用的同步判定：按**到达时刻**返回生效设置。

        监控任务只是把段设置同步进 ``cfg`` 便于查询；真正的收包判定走这里，
        因此即使轮询有几十毫秒误差，跨段那一刻的包也严格按墙钟所属段处理。
        没有进行中的演练时返回 None（沿用 ``cfg`` 当前值）。
        """
        state = self._state
        if state is None or state.status != "running":
            return None
        if now >= state.ends_at:
            # 已到期但监控任务尚未落恢复：包按开场前设置处理。
            return dict(state.baseline)
        segment = state.segments[self._index_at(state, now)]
        return {
            "max_size": int(state.baseline["max_size"]),
            "down": bool(segment["down"]),
            "delay_ms": 0 if segment["down"] else int(segment["delay_ms"]),
        }

    @staticmethod
    def _index_at(state: DrillState, now: float) -> int:
        offset = now - state.started_at
        accumulated = 0.0
        for index, segment in enumerate(state.segments):
            accumulated += float(segment["duration_s"])
            if offset < accumulated:
                return index
        return len(state.segments) - 1

    @staticmethod
    def _segment_start(state: DrillState, index: int) -> float:
        return state.started_at + sum(
            float(segment["duration_s"]) for segment in state.segments[:index]
        )

    def _apply_segment(self, state: DrillState, index: int) -> None:
        segment = state.segments[index]
        self.cfg.down = bool(segment["down"])
        self.cfg.delay_ms = 0 if segment["down"] else int(segment["delay_ms"])

    def _restore(self, baseline: dict[str, Any]) -> None:
        self.cfg.max_size = int(baseline["max_size"])
        self.cfg.delay_ms = int(baseline["delay_ms"])
        self.cfg.down = bool(baseline["down"])

    def _baseline(self) -> dict[str, Any]:
        return {
            "max_size": int(self.cfg.max_size),
            "delay_ms": int(self.cfg.delay_ms),
            "down": bool(self.cfg.down),
        }

    def _config_view(self) -> dict[str, Any]:
        return {
            "max_size": int(self.cfg.max_size),
            "delay_ms": int(self.cfg.delay_ms),
            "down": bool(self.cfg.down),
        }

    def _view(self, state: DrillState) -> dict:
        now = time.time()
        segments = []
        for index, segment in enumerate(state.segments):
            start = self._segment_start(state, index)
            segments.append(
                {
                    "index": index + 1,
                    "duration_s": segment["duration_s"],
                    "down": bool(segment["down"]),
                    "delay_ms": int(segment["delay_ms"]),
                    "starts_at": _iso(start),
                    "ends_at": _iso(start + float(segment["duration_s"])),
                }
            )

        running = state.status == "running" and now < state.ends_at
        if running:
            current_index = self._index_at(state, now)
            current = segments[current_index]
            current_segment = current_index + 1
            current["remaining_s"] = round(
                max(
                    0.0,
                    self._segment_start(state, current_index)
                    + float(state.segments[current_index]["duration_s"])
                    - now,
                ),
                3,
            )
            remaining = max(0.0, state.ends_at - now)
            elapsed = max(0.0, min(now, state.ends_at) - state.started_at)
        else:
            current = None
            current_segment = None
            remaining = 0.0
            elapsed = max(0.0, state.ends_at - state.started_at)

        # 演练进行中时，查询里的 config 给出按墙钟判定应生效的值，与 UDP
        # 收包路径一致（监控任务写入 cfg 有几十毫秒的轮询粒度）。
        config_view = self.effective_at(now) if running else None
        if config_view is None:
            config_view = self._config_view()

        return {
            "line": state.line,
            "status": state.status,
            "started_at": _iso(state.started_at),
            "started_at_epoch": state.started_at,
            "ends_at": _iso(state.ends_at),
            "ends_at_epoch": state.ends_at,
            "completed_at": _iso(state.completed_at),
            "reason": state.reason,
            "elapsed_s": round(elapsed, 3),
            "remaining_s": round(remaining, 3),
            "current_segment": current_segment,
            "current": current,
            "segment_count": len(segments),
            "segments": segments,
            "baseline": state.baseline,
            "config": config_view,
            "persist_error": self.last_persist_error,
        }

    # ------------------------------------------------------------------ 落盘

    def _persist(self, state: DrillState) -> None:
        """原子写：临时文件 + fsync + rename + 目录 fsync。"""
        data = {
            "version": 1,
            "line": state.line,
            "status": state.status,
            "started_at": state.started_at,
            "started_at_iso": _iso(state.started_at),
            "ends_at": state.ends_at,
            "ends_at_iso": _iso(state.ends_at),
            "completed_at": state.completed_at,
            "reason": state.reason,
            "baseline": state.baseline,
            "segments": [
                {
                    "duration_s": segment["duration_s"],
                    "down": bool(segment["down"]),
                    "delay_ms": int(segment["delay_ms"]),
                }
                for segment in state.segments
            ],
        }
        path = self.state_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
            try:
                dir_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                # 某些文件系统不支持目录 fsync；文件本身已 fsync + rename。
                pass
        except OSError:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _persist_or_warn(self, state: DrillState) -> None:
        try:
            self._persist(state)
            self.last_persist_error = None
        except OSError as exc:
            self.last_persist_error = str(exc)
            logger.error("线路 %s 演练状态落盘失败：%s", self.line, exc)

    def _load(self) -> DrillState | None:
        try:
            raw = self.state_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            logger.warning("线路 %s 读取演练状态失败：%s", self.line, exc)
            return None

        try:
            data = json.loads(raw)
            if int(data.get("version", 0)) != 1:
                raise ValueError("未知状态文件版本")
            if data.get("line") != self.line:
                raise ValueError(f"状态文件属于线路 {data.get('line')!r}")
            status = data["status"]
            if status not in {"running", "completed", "cancelled"}:
                raise ValueError(f"未知演练状态 {status!r}")
            baseline = data["baseline"]
            if not all(key in baseline for key in ("max_size", "delay_ms", "down")):
                raise ValueError("baseline 不完整")
            raw_segments = data["segments"]
            if not 1 <= len(raw_segments) <= MAX_DRILL_SEGMENTS:
                raise ValueError("段数越界")
            segments = []
            cursor = float(data["started_at"])
            for index, segment in enumerate(raw_segments, start=1):
                duration = float(segment["duration_s"])
                if duration <= 0:
                    raise ValueError("段时长必须为正")
                down = bool(segment["down"])
                delay_ms = int(segment["delay_ms"])
                segments.append(
                    {
                        "index": index,
                        "duration_s": duration,
                        "down": down,
                        "delay_ms": delay_ms,
                        "starts_at": cursor,
                        "ends_at": cursor + duration,
                    }
                )
                cursor += duration
            started_at = float(data["started_at"])
            ends_at = float(data.get("ends_at") or cursor)
            completed_at = data.get("completed_at")
            return DrillState(
                line=self.line,
                status=status,
                segments=segments,
                baseline={
                    "max_size": int(baseline["max_size"]),
                    "delay_ms": int(baseline["delay_ms"]),
                    "down": bool(baseline["down"]),
                },
                started_at=started_at,
                ends_at=ends_at,
                completed_at=float(completed_at) if completed_at is not None else None,
                reason=str(data.get("reason") or ""),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            logger.warning("线路 %s 演练状态文件无效，已忽略：%s", self.line, exc)
            return None

    # ------------------------------------------------------------------ 监控任务

    def _ensure_monitor(self) -> None:
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(self._monitor())

    async def _monitor(self) -> None:
        while True:
            async with self._lock:
                state = self._state
                if state is None or state.status != "running":
                    return
                now = time.time()
                if now >= state.ends_at:
                    self._restore(state.baseline)
                    state.status = "completed"
                    state.completed_at = state.ends_at
                    state.reason = "到期自动恢复开场前设置"
                    self._persist_or_warn(state)
                    self._applied_index = None
                    logger.info(
                        "线路 %s 演练到期，已恢复开场前设置 %s",
                        self.line,
                        self._config_view(),
                    )
                    return

                index = self._index_at(state, now)
                if index != self._applied_index:
                    self._apply_segment(state, index)
                    self._applied_index = index
                    logger.info(
                        "线路 %s 演练进入第 %d/%d 段（%s）",
                        self.line,
                        index + 1,
                        len(state.segments),
                        "断线"
                        if state.segments[index]["down"]
                        else f"延迟 {state.segments[index]['delay_ms']}ms",
                    )
                next_boundary = self._segment_start(state, index) + float(
                    state.segments[index]["duration_s"]
                )
                sleep_for = min(self.tick_s, max(0.001, next_boundary - now))
                if state.ends_at - now < sleep_for:
                    sleep_for = max(0.001, state.ends_at - now)
            await asyncio.sleep(sleep_for)

    async def _finalize_if_due_locked(self) -> None:
        state = self._state
        if state is None or state.status != "running":
            return
        if time.time() < state.ends_at:
            return
        self._restore(state.baseline)
        state.status = "completed"
        state.completed_at = state.ends_at
        state.reason = "到期自动恢复开场前设置"
        self._persist_or_warn(state)
        self._applied_index = None
