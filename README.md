# UDP 线路载荷测量服务

采集线路改路后，值班员需要知道当前线路上 UDP 包还能发多大。本服务提供测量接口：
给定载荷字节数的上下限，通过**实际 UDP 收发**二分探测出范围内可用的最大值，
探测次数为 **O(log 范围宽度)**，不随范围线性增长。

自带两条可切换的本地回显线路（模拟器），可模拟**超长丢包、回包延迟、断线**；
测量期间可切线，切线立即撤下旧建议，即使切回原线路，也只接受切换后开始的测量结果。

## 组成

| 组件 | 说明 | 端口（compose 内） |
|---|---|---|
| `app` | FastAPI 测量服务（本仓库 `app/`） | `8000`（映射到宿主机） |
| `echo-a` | 回显线路 a：默认超过 1500B 丢弃、延迟 30ms | UDP `9001` / 控制口 `9101` |
| `echo-b` | 回显线路 b：默认超过 4000B 丢弃、延迟 80ms | UDP `9002` / 控制口 `9102` |

回显线路逐字节回显；超过 `max_size` 的包丢弃（超长丢包），`delay_ms` 后回包
（回包延迟），`down=true` 时全部丢弃（断线）。行为以收到包那一刻的配置为准。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

无 Docker 时也可本地跑（Python 3.11+）：

```bash
pip install -r requirements.txt
LINE_NAME=a UDP_PORT=9001 CTRL_PORT=9101 MAX_SIZE=1500 DELAY_MS=30 python -m echo.server &
LINE_NAME=b UDP_PORT=9002 CTRL_PORT=9102 MAX_SIZE=4000 DELAY_MS=80 python -m echo.server &
uvicorn app.main:app --port 8000
```

## 在 /docs 上演示测量与切线

1. **测量**：`POST /api/measure`，Body `{"lower": 512, "upper": 9000}` → 202 返回测量
   任务；用返回的 `id` 调 `GET /api/measurements/{id}` 查看进度与逐包事件轨迹
   （二分过程、三次重试、最短包探活都记录在 `events` 里）。
2. **看建议**：`GET /api/recommendation` → `max_payload: 1500`（线路 a）。
3. **切线**：`POST /api/line/switch`，Body `{"line": "b"}` → 响应里
   `recommendation` 立即为 `null`；再测一次 → 线路 b 测出 `4000`。
4. **模拟断线**：`POST /api/lines/b/config`，Body `{"down": true}` → 再发起测量，
   结果 `status: undetermined`（最短包连试三次也超时），本轮不发布建议。
   `{"down": false}` 恢复。
5. **测量中切线**：先发一个宽范围测量（如 `{"lower": 512, "upper": 65507}`），
   趁 `status` 还是 `running` 时切线（可切走再切回）→ 该测量以
   `aborted`/`discarded` 结束，建议保持为 `null`，需重新测量。

## 测量算法与判定规则

- 在 `[lower, upper]` 上**二分**：中点可用则搜右半，不可用则搜左半。
- 某长度**连试 3 次**（次数可调）都收不到匹配回包时，不直接判死——改发范围内
  **最短包**（`lower` 字节）确认线路仍通：
  - 最短包有匹配回包 → 该长度才判为不可用；
  - 最短包连试 3 次也超时 → 返回**无法判定**（`undetermined`），本轮不发布建议。
- 回包按**内容逐字节匹配**（每包随机载荷），串台包、迟到包不计数。
- 探测次数：每轮二分最多 `3(试) + 3(探活)` 个包，总复杂度 O(log 宽度)。

## 切线语义

- 每次真实切线（目标 ≠ 当前线路）：`epoch + 1`，**立即撤下旧建议**。
- 测量启动时记录 `epoch`；测量中或发布前发现 `epoch` 变化即中止/作废。
- 因此**切回原线路也没用**：a→b→a 之后，只接受最后一次切换之后开始的测量。

## API 一览

| 方法/路径 | 说明 |
|---|---|
| `POST /api/measure` | 发起测量（`lower`/`upper`/`timeout_ms`/`attempts`），202 返回任务 |
| `GET /api/measurements/{id}` | 测量进度、结果、逐包事件 |
| `GET /api/measurements` | 最近测量列表 |
| `GET /api/recommendation` | 当前建议（无则 `null`） |
| `GET /api/line` / `POST /api/line/switch` | 当前线路 / 切线 |
| `GET /api/lines` | 两条线路的实时配置与收发统计 |
| `POST /api/lines/{name}/config` | 调模拟参数：`max_size`/`delay_ms`/`down` |
| `GET /docs` | Swagger 交互式文档 |

## 自测

```bash
pip install -r requirements-dev.txt
pytest                                  # 14 项单元/集成测试
# 对运行中的服务栈（compose 或本地）跑端到端场景：
python3 scripts/e2e.py --base http://127.0.0.1:8000
```

## 实测结果（2026-11-22，本机 Python 3.11 跑完整服务栈）

- `pytest`：**14/14 通过**（引擎级 7 项 + API 级 7 项，含断线、延迟、中止、校验）。
- `scripts/e2e.py`：**22/22 项通过**，关键数据：
  - 超长丢包：线路 a（阈值 1500）测 `[512, 9000]` → `max_payload=1500`，
    **仅发 31 个探测包**（范围宽 8489，线性需 8489 次）；
  - 宽范围 `[1, 65507]` → `1500`，**仅 40 个探测包**（线性需 65507 次），
    事件轨迹可见完整二分与最短包探活过程；
  - 断线（`down=true`）：测量返回 `undetermined`（“最短包 512B 连续3次超时，
    线路疑似中断”），旧建议保留、未发布新建议；
  - 测量中切线及 a→b→a 切回：旧测量均 `aborted`，建议立即为 `null`，
    切换后重新测量才发布；
  - 线路统计佐证：线路 a `dropped_oversize=64`，线路 b `dropped_down=6`。
- Docker 本环境不可用，`docker-compose.yml` 已做结构校验（3 个服务、命令与
  环境变量与代码一致），镜像内启动命令与本地实测命令相同。

## 余项 / 已知限制

- 测量为**单并发**（物理线路共享，避免互相干扰）；并发请求返回 409。
- 建议与测量历史只存内存，服务重启即清空；未做持久化与鉴权（内网值班工具定位）。
- 回显按全量字节比对，未做分片校验和等优化；单包最大 65507B（UDP 协议上限）。
- 大于 MTU 的 UDP 包依赖 IP 分片送达；跨真实广域网时路径可能丢分片，
  本工具面向改路后的本地回显线路验证，不替代 RFC 8899 之类的 PMTUD。
- 断线期间发起的测量返回“无法判定”后，旧建议按约定保留；如需更保守的
  “断线即撤建议”，可在 `LineManager` 增加线路健康钩子。
