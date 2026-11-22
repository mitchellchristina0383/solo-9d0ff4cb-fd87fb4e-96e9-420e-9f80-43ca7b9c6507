#!/usr/bin/env python3
"""端到端自测脚本（仅用标准库）：对运行中的服务栈做完整场景核对。

覆盖：超长丢包二分测量、探测次数对数级、切线即撤建议、断线无法判定、
测量中切线作废、切回原线路也只认切换后的测量、/docs 可用。

用法：
    python3 scripts/e2e.py --base http://127.0.0.1:8000
退出码非 0 表示有场景未通过。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

RESULTS: list[tuple[bool, str, str]] = []


def api(base: str, method: str, path: str, body: dict | None = None):
    req = urllib.request.Request(
        base + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw.decode(errors="replace")


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((ok, name, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def wait_done(base: str, mid: str, timeout_s: float = 60.0) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        _, rec = api(base, "GET", f"/api/measurements/{mid}")
        if rec["status"] != "running":
            return rec
        time.sleep(0.2)
    raise TimeoutError(f"测量 {mid} 超时未结束")


def measure(base: str, lower: int, upper: int, timeout_ms: int = 300) -> dict:
    status, rec = api(
        base,
        "POST",
        "/api/measure",
        {"lower": lower, "upper": upper, "timeout_ms": timeout_ms, "attempts": 3},
    )
    assert status == 202, f"发起测量失败: {status} {rec}"
    return wait_done(base, rec["id"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    base = parser.parse_args().base.rstrip("/")
    print(f"目标服务: {base}\n")

    print("== 0. 服务与文档可用 ==")
    status, _ = api(base, "GET", "/")
    check("GET / 返回 200", status == 200)
    req = urllib.request.Request(base + "/docs")
    with urllib.request.urlopen(req, timeout=10) as resp:
        docs_ok = resp.status == 200 and b"swagger" in resp.read().lower()
    check("GET /docs 返回 Swagger UI", docs_ok)

    print("== 1. 初始化两条线路的模拟参数 ==")
    status, cfg_a = api(base, "POST", "/api/lines/a/config",
                        {"max_size": 1500, "delay_ms": 30, "down": False})
    check("线路 a 配置 max_size=1500", status == 200 and cfg_a["max_size"] == 1500)
    status, cfg_b = api(base, "POST", "/api/lines/b/config",
                        {"max_size": 4000, "delay_ms": 80, "down": False})
    check("线路 b 配置 max_size=4000", status == 200 and cfg_b["max_size"] == 4000)
    _, snap = api(base, "GET", "/api/line")
    check("默认激活线路为 a", snap["active_line"] == "a")

    print("== 2. 超长丢包：线路 a 上二分测量 [512, 9000] ==")
    rec = measure(base, 512, 9000)
    check("测量完成", rec["status"] == "completed", rec["detail"])
    check("测出阈值 1500", rec["max_payload"] == 1500,
          f"max_payload={rec['max_payload']}")
    check("探测次数为对数级 (<200，范围宽 8489)", rec["probes_sent"] < 200,
          f"probes_sent={rec['probes_sent']}")
    _, snap = api(base, "GET", "/api/recommendation")
    check("建议已发布 1500",
          snap["recommendation"] and snap["recommendation"]["max_payload"] == 1500)

    print("== 3. 切线：建议立即撤下，换线后重新测量 ==")
    _, snap = api(base, "POST", "/api/line/switch", {"line": "b"})
    check("切到 b 后建议为 null", snap["recommendation"] is None)
    _, snap = api(base, "GET", "/api/recommendation")
    check("撤下立即可见", snap["recommendation"] is None)
    rec = measure(base, 512, 9000)
    check("线路 b 测出阈值 4000", rec["max_payload"] == 4000,
          f"max_payload={rec['max_payload']}")

    print("== 4. 断线：最短包也超时 → 无法判定，本轮不发布建议 ==")
    api(base, "POST", "/api/lines/b/config", {"down": True})
    rec = measure(base, 512, 9000)
    check("状态为 undetermined", rec["status"] == "undetermined", rec["detail"])
    _, snap = api(base, "GET", "/api/recommendation")
    check("旧建议(4000)保留、未发布新建议",
          snap["recommendation"] and snap["recommendation"]["max_payload"] == 4000)
    api(base, "POST", "/api/lines/b/config", {"down": False})

    print("== 5. 测量中切线：旧测量作废，建议立即撤下 ==")
    status, rec = api(base, "POST", "/api/measure",
                      {"lower": 512, "upper": 65507, "timeout_ms": 300, "attempts": 3})
    mid = rec["id"]
    time.sleep(1.0)  # 让测量跑起来
    _, snap = api(base, "POST", "/api/line/switch", {"line": "a"})
    check("切线响应中建议已为 null", snap["recommendation"] is None)
    rec = wait_done(base, mid)
    check("旧测量作废(aborted/discarded)", rec["status"] in ("aborted", "discarded"),
          f"status={rec['status']}")
    _, snap = api(base, "GET", "/api/recommendation")
    check("建议未被旧测量发布", snap["recommendation"] is None)

    print("== 6. 切回原线路：也只接受切换后开始的测量 ==")
    status, rec = api(base, "POST", "/api/measure",
                      {"lower": 512, "upper": 65507, "timeout_ms": 300, "attempts": 3})
    mid = rec["id"]
    time.sleep(1.0)
    api(base, "POST", "/api/line/switch", {"line": "b"})
    api(base, "POST", "/api/line/switch", {"line": "a"})  # 立刻切回 a
    rec = wait_done(base, mid)
    check("切回后旧测量仍作废", rec["status"] in ("aborted", "discarded"),
          f"status={rec['status']}")
    _, snap = api(base, "GET", "/api/recommendation")
    check("建议仍为 null", snap["recommendation"] is None)
    rec = measure(base, 512, 9000)
    check("切回后重新测量有效 (1500)", rec["max_payload"] == 1500)

    print("== 7. 线路统计佐证超长丢包真实发生 ==")
    _, lines = api(base, "GET", "/api/lines")
    by_name = {l["name"]: l for l in lines}
    check("线路 a 有超长丢包计数",
          by_name["a"]["reachable"] and by_name["a"]["stats"]["dropped_oversize"] > 0,
          f"dropped_oversize={by_name['a']['stats']['dropped_oversize']}")
    check("线路 b 有断线丢包计数",
          by_name["b"]["reachable"] and by_name["b"]["stats"]["dropped_down"] > 0,
          f"dropped_down={by_name['b']['stats']['dropped_down']}")

    failed = [item for item in RESULTS if not item[0]]
    print(f"\n汇总: {len(RESULTS) - len(failed)}/{len(RESULTS)} 项通过")
    for _, name, detail in failed:
        print(f"  未通过: {name} {detail}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
