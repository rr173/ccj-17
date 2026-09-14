# append-only-log —— 可压缩、租约保护、可校验的追加日志

只追加的日志服务。旧段可以被压缩成 **快照 + 可校验清单（manifest）**，
但严格遵守读者租约：任何读者钉住的序号及其所在段之前，一律不回收。

* 零第三方依赖，仅 Python 3.11 标准库
* 哈希链记录（防篡改），快照/清单自校验摘要 + 逐条段凭证 + 只追加审计链
* 读者登记、心跳续租、过期自动失效
* 压缩双 Pass 校验 + 业务等价证明（完整重放 ≡ 快照 + 尾部重放）
* 唯一提交点（原子指针替换），压缩中途崩溃/校验失败自动回到上一个完整边界
* 压缩互斥（线程锁 + `flock`）：多个压缩任务只有一个能改变可见结果
* 提供 Docker / docker-compose 部署

---

## 1. 数据模型与磁盘布局

```
/data
├── segments/
│   ├── seg-000001.log          # 每行一个规范 JSON 记录，末尾 \n
│   ├── seg-000002.log
│   └── ...
├── checkpoints/
│   ├── current.json            # 可见指针（唯一提交点，tmp+rename 原子替换）
│   ├── gen-7/
│   │   ├── snapshot.json       # 某代快照（自校验 envelope）
│   │   └── manifest.json       # 该代压缩清单 + 每段凭证（自校验 envelope）
│   ├── gen-7.tmp.<rand>/       # 半成品目录（读者永远看不到）
│   └── audit.log               # 只追加审计链：每代 manifest 永久留痕
└── state/
    ├── readers.json            # 读者租约（原子写，重启有效）
    └── compact-result.json     # 最近一次压缩结果（成功/失败/跳过原因）
```

### 1.1 记录哈希链

每条记录：

```json
{"seq": 12, "ts": 1694600000000, "type": "put",
 "payload": {"key": "k0", "value": 9}, "prev": "<上条记录摘要>"}
```

* `digest(record) = sha256(canonical_json({seq,ts,type,payload,prev}))`
* `canonical_json` = 键排序、紧凑分隔、UTF-8（见 `app/common.py`）
* 段内、段间共用同一条链；首条记录锚点是 64 个 `0`（GENESIS）
* 活动段（最后一个）只追加不回收；写满 `SEGMENT_BYTES` 自动滚动

业务事件（可插拔于 `app/reducer.py`）：

| type | payload | 语义 |
|---|---|---|
| `put` | `{"key","value"}` | `state[key] = value` |
| `delete` | `{"key"}` | 删除 key（墓碑） |
| `data` | 任意 JSON | 不参与业务状态，仅入链 |

### 1.2 快照 / 清单 / 指针 / 审计链

* `snapshot.json`：`{gen, snapshot_seq, state(截至该点的完整业务状态),
  tail_anchor(尾部起始记录的 prev 摘要), prev_snapshot_digest, ..., digest}`
* `manifest.json`：`{gen, anchor, tail_anchor, covered_segs, range,
  segments:[每段凭证], state_digest, snapshot_digest,
  prev_manifest_digest, ..., digest}`
* 每段凭证：段内**每条记录的摘要列表**、其 Merkle 风格根
  `digest(list)`、首/尾摘要、计数、**原始段文件 SHA-256**。
  原始段删除后，仍能凭审计链中的 manifest 摘要与凭证逐条追溯核对。
* envelope 自校验：文件内 `digest = sha256(canonical(除 digest 外全部字段))`。
* 代际链：`snapshot.prev_snapshot_digest` / `manifest.prev_manifest_digest`
  指向父代；首代锚点为 GENESIS。`audit.log` 自身也是哈希链。

---

## 2. 读者租约与回收规则

* `POST /readers/{id}`：登记并声明 `pin_seq`（钉住的最老序号），获得 TTL 租约。
* `POST /readers/{id}/heartbeat`：续租；可把游标 `position` 向后推进，
  但不能退到 `pin_seq` 之前。
* 租约过期：钉位立即失效（可被回收）；过期后心跳返回 `410`，需重新登记。
* `DELETE /readers/{id}`：主动释放。
* **回收判定（段粒度）**：设所有未过期读者的最小 `pin_seq = oldest_pin`，
  它所在（或之后最近现存）的段为 `protected_segment`；
  只有「sealed 且段号 `< protected_segment`」的段可回收。
  没有任何读者时，所有 sealed 段可回收；活动段永远不回收。
* 租约落盘（fsync），服务重启后未过期租约继续保护。

查询：`GET /pins` 返回最老钉住点、钉住者、受保护段、可回收段列表、
可回收 seq 范围与字节数。

---

## 3. 压缩协议（核心不变量）

`POST /compact [{"force": true}]`。`force` 只绕过「可回收段数阈值」，
**绝不绕过租约**。

```
0. 获取线程锁 + flock（非阻塞；拿不到 -> 409 busy，不做任何改动）
1. 规划 candidates = 钉点之前的 sealed 连续段；
   检查它们与当前 checkpoint 锚点连续
2. Pass A（构建）
   - 从折叠起点（首代空状态/GENESIS；续代上一代快照状态/其 tail_anchor）
     逐段验证哈希链、折叠业务状态
   - 为每段生成凭证（逐条记录摘要、文件 SHA-256）
   - 在 gen-N.tmp.<rand>/ 写 snapshot.json + manifest.json，
     fsync 后原子 rename 为 gen-N/
3. Pass B（独立复核：重新从磁盘打开文档与原始段）
   - envelope digest、manifest↔snapshot 互链、锚点
   - 逐条记录摘要 == 凭证、文件 SHA-256 == 凭证、records_root
   - 独立重折叠摘要 == 快照 state_digest
   - 业务等价证明（此时原始段都还在）：
       完整重放（续代=上代快照+现存全部段）
           ≡ 新快照状态 + 保留尾部重放
4. 校验失败：删除临时/未提交代目录，current.json 不动、段不删，
   记录 failed 结果（可查询），返回 200 + status=failed
5. 提交（唯一改变可见结果的动作，顺序不可乱）：
   audit.log 追加 -> current.json 原子替换
6. 提交后：逐段删除原始段、清理旧代目录
```

### 崩溃恢复（启动时自动执行，幂等）

| 崩溃时机 | 磁盘状态 | 启动恢复动作 |
|---|---|---|
| Pass A/B（指针未切换） | `gen-N.tmp.*` 或未提交的 `gen-N/`，段齐全 | 清掉半成品，对外仍是上一完整边界 |
| `current.json` 切换后、删段中途 | 新指针 + 部分旧段残留 | 校验新边界后**幂等补删**已承诺段 |
| 活动段半行写入 | 段尾截断行 | 截断到最后一条完整记录后续写 |
| 已提交边界自身损坏 | current.json 与 gen-N 不一致 | **拒绝启动**（`boundary_corrupt`），不静默篡改历史 |

### 并发

线程内 RLock + 跨进程 `flock(LOCK_NB)`：同进程多任务、多进程副本同时
压缩时，只有一个能进入提交，其余立即得到 `409 {"error":"busy"}`，
审计链与 checkpoint 目录中只有一个新一代。

---

## 4. HTTP API

| 方法 路径 | 说明 |
|---|---|
| `POST /append` | `{type, payload}` → 返回 `{seq, prev, digest,...}` |
| `GET /read?from=&limit=` | 读原始事件（压缩掉的序号 → `410 compacted`，附 `readable_from`） |
| `GET /state` | **从头读取的业务含义**：快照状态+尾部重放，返回当前状态与摘要 |
| `GET /head` | 当前 checkpoint 与可读起点 `readable_from` |
| `POST /readers[/{id}]` | 登记读者：`{pin_seq, ttl_ms?}`（id 可省略自动生成） |
| `POST /readers/{id}/heartbeat` | `{ttl_ms?, position?}` 续租/推进游标 |
| `DELETE /readers/{id}` | 释放钉位 |
| `GET /readers` | 列出读者、剩余租约、是否存活 |
| `GET /pins` | **最老钉住点、受保护段、可回收范围/字节数** |
| `POST /compact` | 触发压缩；body `{"force":true}` 绕过段数阈值 |
| `GET /compact/result` | **最近一次压缩结果**（成功/失败阶段/等价证明/跳过原因） |
| `POST /compact/verify` | 重新复核当前边界，返回逐项 checks |
| `GET /audit` | 压缩审计链 |
| `GET /status` | 总览：段、租约、钉位、checkpoint、压缩状态 |
| `GET /health` | 健康检查 |

错误统一为 `{"error","message","details"}`，语义化状态码
（400 参数 / 404 / 409 冲突或压缩忙 / 410 租约过期或序号已压缩 / 413 / 500）。

### 快速试一下

```bash
python -m app                      # 默认 /data，:8080

curl -s -XPOST localhost:8080/append -H 'content-type: application/json' \
  -d '{"type":"put","payload":{"key":"k0","value":1}}'
# 多写一些、滚动段之后：
curl -s -XPOST localhost:8080/readers/alice -H 'content-type: application/json' \
  -d '{"pin_seq":100,"ttl_ms":60000}'
curl -s -XPOST localhost:8080/compact -d '{"force":true}' -H 'content-type: application/json'
curl -s localhost:8080/pins
curl -s localhost:8080/compact/result
curl -s -XPOST localhost:8080/compact/verify
curl -s localhost:8080/state
```

---

## 5. Docker 部署

```bash
docker build -t append-only-log .
docker run -d --name appendlog -p 8080:8080 -v appendlog-data:/data append-only-log

# 或
docker compose up -d --build
docker compose logs -f
```

镜像只含标准库，以非 root 用户运行，声明 `/data` 卷与 HEALTHCHECK。
生产请把 `/data` 放到可靠磁盘；所有元数据写入都带 `fsync`，
但文件系统层的写序/持久化保证（fsync 语义）仍取决于底层存储。

环境变量：

| 变量 | 默认 | 说明 |
|---|---|---|
| `APP_DATA_DIR` | `/data` | 数据目录 |
| `PORT` / `HOST` | `8080` / `0.0.0.0` | 监听地址 |
| `SEGMENT_BYTES` | `1048576` | 段滚动大小 |
| `DEFAULT_TTL_MS` | `30000` | 租约默认时长 |
| `MAX_TTL_MS` | `86400000` | 单次续租上限（1d~24h，最小 1000ms） |
| `COMPACTION_MIN_SEGMENTS` | `4` | janitor 自动压缩所需最少可回收 sealed 段 |
| `JANITOR_ENABLED` | `true` | 后台自动压缩线程 |
| `JANITOR_INTERVAL_MS` | `5000` | janitor 轮询间隔 |

> 压缩期间持有元数据锁，会短暂阻塞追加（默认小段配置下为毫秒~亚秒级）。
> 服务定位为单副本 + 持久卷；多副本共享卷时 `flock` 保证压缩互斥，
> 但读者租约仍以单一主副本写入为准。

---

## 6. 测试

```bash
python tests/run_tests.py          # 无 pytest 环境（标准库垫片运行器）
# 若已安装 pytest：
python -m pytest -q
```

覆盖：哈希链/滚动/半行截断、租约登记续租过期释放/重启有效、
钉位保护与多读者最小钉、首代/续代压缩业务等价、410 与从头读取、
凭证追溯原始段、篡改段/篡改快照导致失败并完全回滚、
同进程多任务与跨进程 flock 互斥、四个压缩阶段的断电恢复、
HTTP 端到端（含 janitor 在钉位保护下不误删、释放后自动压缩、重启边界）。

`CRASH_HOOK={after_fold|after_write|after_verify|after_switch}`
可让进程在压缩对应阶段 `os._exit(99)`，用于灾难演练。
