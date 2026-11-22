# UDP 线路载荷测量服务

采集线路改路后，值班员需要知道当前线路上 UDP 包还能发多大。本服务提供测量接口：
给定载荷字节数的上下限，通过**实际 UDP 收发**二分探测出范围内可用的最大值，
探测次数为 **O(log 范围宽度)**，不随范围线性增长。

自带两条可切换的本地回显线路（模拟器），可模拟**超长丢包、回包延迟、断线**；
测量期间可切线，切线立即撤下旧建议，即使切回原线路，也只接受切换后开始的测量结果。

回显线路还支持**分段演练**：选一条线，按时间表最多 8 段，每段选择断线或指定
回包延迟，用来复现“先延迟、后断线”的过程并核对采集端告警。演练先落盘再生效，
查询可见当前段与整轮结束时间，重启后按原时间表续跑。

## 组成

| 组件 | 说明 | 端口（compose 内） |
|---|---|---|
| `app` | FastAPI 测量服务（本仓库 `app/`） | `8000`（映射到宿主机） |
| `echo-a` | 回显线路 a：默认超过 1500B 丢弃、延迟 30ms | UDP `9001` / 控制口 `9101` |
| `echo-b` | 回显线路 b：默认超过 4000B 丢弃、延迟 80ms | UDP `9002` / 控制口 `9102` |

回显线路逐字节回显；超过 `max_size` 的包丢弃（超长丢包），`delay_ms` 后回包
（回包延迟），`down=true` 时全部丢弃（断线）。行为以收到包那一刻的配置为准。

## 在 /docs 上演示分段演练（先延迟、后断线）

1. **开场前设置**：`POST /api/lines/a/config`，Body `{"max_size": 1500, "delay_ms": 30}`。
2. **开一轮演练**：`POST /api/lines/a/drill`，Body：

   ```json
   {"segments": [
     {"duration_s": 3, "delay_ms": 800},
     {"duration_s": 5, "down": true},
     {"duration_s": 2, "delay_ms": 100}
   ]}
   ```

   202 返回当前段、各段起止时间与整轮 `ends_at`；每轮 1~8 段，
   每段要么 `down: true`（断线），要么给 `delay_ms`（回包延迟），二选一。
3. **看进度**：`GET /api/lines/a/drill` → `current_segment`、`current.remaining_s`、
   `ends_at`、`elapsed_s`/`remaining_s`，以及开场前设置 `baseline`。
4. **核对跨段回包**：在第 1 段（延迟 800ms）内发一个 UDP 包到线路 a 的 9001 口，
   等它跨进第 2 段（断线）后仍会收到回包——已排队的回包照原决定发送；
   断线段内新到的包则全部丢弃。
5. **互斥**：演练进行中 `POST /api/lines/a/config` 与再开一轮 `POST .../drill`
   都返回 409（`detail` 里写明原因和结束时间）。
6. **提前收场**：`DELETE /api/lines/a/drill` → 立即恢复开场前设置；不删也会在
   到期时自动恢复。
7. **重启续跑**：`docker compose restart echo-a`（或重建容器）后
   `GET /api/lines/a/drill` 仍显示原时间表，按墙钟落在正确的段；停机期间已经
   过去的段直接跳过，停机时间照算。若整轮在停机期间结束，重启即恢复开场前设置。

演练只改 `down`/`delay_ms`，原有载荷丢包规则（`max_size`）照旧；超长包仍按
开场前的 `max_size` 被丢弃。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

无 Docker 时也可本地跑（Python 3.11+）：

```bash
pip install -r requirements.txt
LINE_NAME=a UDP_PORT=9001 CTRL_PORT=9101 MAX_SIZE=1500 DELAY_MS=30 \
  DRILL_STATE_PATH=/tmp/drills/a.json python -m echo.server &
LINE_NAME=b UDP_PORT=9002 CTRL_PORT=9102 MAX_SIZE=4000 DELAY_MS=80 \
  DRILL_STATE_PATH=/tmp/drills/b.json python -m echo.server &
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
| `GET /api/lines` | 两条线路的实时配置、收发统计与演练状态 |
| `POST /api/lines/{name}/config` | 调模拟参数：`max_size`/`delay_ms`/`down`（演练中 409） |
| `POST /api/lines/{name}/drill` | 开始分段演练（1~8 段），202；先落盘再生效 |
| `GET /api/lines/{name}/drill` | 当前段、剩余时间、整轮结束时间与各段明细 |
| `DELETE /api/lines/{name}/drill` | 提前结束演练并恢复开场前设置 |
| `GET /docs` | Swagger 交互式文档 |

回显线路控制口上也有一组同名接口（`/drill`、`/config`），主服务只做代理，
因此演练状态天然与线路进程同生共死。`GET /api/lines` 里每条线也带 `drill` 字段。

## 自测

```bash
pip install -r requirements-dev.txt
pytest                                  # 24 项单元/集成测试
# 对运行中的服务栈（compose 或本地）跑端到端场景：
python3 scripts/e2e.py --base http://127.0.0.1:8000
```

## 实测结果（2026-11-22，本机 Python 3.11 跑完整服务栈）

- `pytest`：**24/24 通过**（引擎级 7 项 + API 级 7 项 + 演练 10 项，覆盖落盘、
  互斥、跨段回包、载荷丢包规则不变、提前结束、重启续跑、停机期间到期、
  状态文件损坏、段数/参数校验）。
- `scripts/e2e.py`：**36/36 项通过**，关键数据：
  - 超长丢包：线路 a（阈值 1500）测 `[512, 9000]` → `max_payload=1500`，
    **仅发 31 个探测包**（范围宽 8489，线性需 8489 次）；
  - 宽范围 `[1, 65507]` → `1500`，**仅 40 个探测包**（线性需 65507 次），
    事件轨迹可见完整二分与最短包探活过程；
  - 断线（`down=true`）：测量返回 `undetermined`（“最短包 512B 连续3次超时，
    线路疑似中断”），旧建议保留、未发布新建议；
  - 测量中切线及 a→b→a 切回：旧测量均 `aborted`，建议立即为 `null`，
    切换后重新测量才发布；
  - 分段演练：3 段（延迟 800ms / 断线 / 延迟 50ms）启动后 `current_segment=1`，
    `ends_at - started_at = 5.5s`，`baseline={max_size:1500, delay_ms:30, down:false}`；
    延迟段入队的回包在断线段内仍收到，断线段新到的包被丢弃；演练中改参数/再开
    一轮均 409；到期后线路恢复 `1500B/30ms/未断线`；
  - 线路统计佐证：线路 a `dropped_oversize=40`，线路 b `dropped_down=6`。
- 手工重启实测（本地三进程栈）：演练进行到第 2 段（断线）时 `kill` echo-a，
  停机 4s 后重启 → 查询仍为 `running`、`current_segment=2`、`down=true`、
  `ends_at` 与停机前一致；停机跨过整轮结束再重启 → `completed`，原因
  “重启时发现整轮已在停机期间到期”，线路恢复到落盘的开场前设置（而不是本次
  进程启动参数）。跨段回包实测：第 1 段（0.5s，延迟 1500ms）内发包，越过段边界后
  在 1.506s（已处于断线段）收到回包，同一时刻在断线段新发的包无回包。
- Docker 本环境不可用，`docker-compose.yml` 已做结构校验（3 个服务、命令与
  环境变量与代码一致；演练状态文件挂在命名卷 `drill-a`/`drill-b` 上，容器重启
  后仍在），镜像内启动命令与本地实测命令相同。本地实测以等价环境变量启动
  （`DRILL_STATE_PATH=/tmp/drills/*.json`）。

## 余项 / 已知限制

- 测量为**单并发**（物理线路共享，避免互相干扰）；并发请求返回 409。
- 建议与测量历史只存内存，服务重启即清空；未做持久化与鉴权（内网值班工具定位）。
- 回显按全量字节比对，未做分片校验和等优化；单包最大 65507B（UDP 协议上限）。
- 大于 MTU 的 UDP 包依赖 IP 分片送达；跨真实广域网时路径可能丢分片，
  本工具面向改路后的本地回显线路验证，不替代 RFC 8899 之类的 PMTUD。
- 断线期间发起的测量返回“无法判定”后，旧建议按约定保留；如需更保守的
  “断线即撤建议”，可在 `LineManager` 增加线路健康钩子。
- 演练时间表按**墙钟**续跑：若宿主时钟大幅回拨/前跳，段边界会随之平移。
  重启续跑依赖同一份状态文件（compose 用命名卷）；删除卷等同于丢掉续跑依据。
- 演练状态只保留**最近一轮**（进行中或已结束）；历史轮次不归档。
- 已被监控任务取消的延迟回包在进程内是定时器，重启会丢失尚未发出的排队回包；
  重启后只保证**后续到达**的包按当前段处理，不补发停机前未发出的回包。
- 同一线路上的演练与测量互不影响（测量只读线路行为），但两者同时进行时
  测量结果会反映演练中的断线/延迟，需值班员自行判断。
