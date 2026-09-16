# append-only-log —— 可压缩、租约保护、可校验、可主备切换的追加日志

只追加的日志服务。旧段可以被压缩成 **快照 + 可校验清单（manifest）**，
但严格遵守读者租约：任何读者钉住的序号及其所在段之前，一律不回收。
写入支持 **带幂等键的原子批次**：分多次组装、整批提交、崩溃不留半批。
支持 **主备复制与故障切换**：备实例从主实例建立复制（一致边界 + 顺序增量），
提升使用单调递增任期与带有效期的授权，分叉历史被明确拒绝。
写入采用 **多数派提交水位（commit watermark）**：主先本地持久化，再等待当前
任期多数投票成员确认同一日志位置，只有越过提交水位的记录才对普通读取可见；
支持 `write_id` 提交幂等与 `consistency=linearizable` 的读取屏障。

* 零第三方依赖，仅 Python 3.11 标准库
* 哈希链记录（防篡改），快照/清单自校验摘要 + 逐条段凭证 + 只追加审计链
* 原子批次：提交前不可见、整批一次性可见、幂等键防重、崩溃只恢复成整批已提交/未提交
* **多数派提交水位**：`commit_index` 持久化；`write_id` 重试继续同一提交，
  超时返回 `commit_timeout`（任期/序号/write_id）；原子批次不被提交水位拆开
* **线性一致读**：主在当前任期完成多数派读屏障后才返回状态；失去多数派、
  授权过期或任期变化时明确拒绝；备实例拒绝并返回角色、任期、已知主
* 读者登记、心跳续租、过期自动失效
* 压缩双 Pass 校验 + 业务等价证明（完整重放 ≡ 快照 + 尾部重放）
* 唯一提交点（原子指针替换），压缩中途崩溃/校验失败自动回到上一个完整边界
* 压缩互斥（线程锁 + `flock`）：多个压缩任务只有一个能改变可见结果
* 主备复制：一致快照边界原子安装、增量逐条摘要链校验、重复段不重复应用、
  已确认位置分叉停在 conflict；主主动推送（故障切换后旧主无需改配即可追上）；
  故障切换任期单调、授权 TTL、每任期多数派单胜者、只保留多数派可证前缀
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
    ├── batches.json            # 原子批次生命周期 + 幂等提交登记（原子写，重启有效）
    ├── compact-result.json     # 最近一次压缩结果（成功/失败/跳过原因）
    ├── head.json               # 链头锚点 {seq, digest}：每次追加后原子更新
    ├── cluster.json            # 集群角色/单调任期/带 TTL 授权/每任期投票（重启有效）
    ├── replica.json            # 复制关系、已确认序号/摘要、来源边界、最近错误
    └── commit.json             # 多数派提交水位、待定提议、write_id 登记、成员确认位置
    └── schedules.json          # 未来生效变更预约（请求标识/生效时间/操作/版本/执行纪元/最终日志位置）
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
* **链头锚点**：链尖（最后一条记录的 `{seq, digest}`）在每次追加、
  记录 fsync 之后原子写入 `state/head.json`。哈希链只锚定每条记录的
  前驱，若不持久化链尖，改动活动段最后一条记录的内容后所有链接依然
  完好——重启恢复与 `POST /compact/verify` 都以 head.json 为准拒绝
  这种尾部篡改（拒绝启动 `tail_corrupt` / 校验 `failed`）。

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

## 3. 原子批次与幂等提交

调用方可以创建批次、分多次加入 `put`/`delete`/`data` 记录，再选择提交或放弃：

```
POST /batches                 {"idempotency_key"?, "ttl_ms"?}  -> 201 {batch_id, status:"open", ...}
POST /batches/{id}/ops        {"ops":[{type,payload},...]} 或单条 {type,payload}
POST /batches/{id}/commit     -> 200 {status:"committed", first_seq, last_seq, record_count, ...}
POST /batches/{id}/abort      -> 200 {status:"aborted"}
GET  /batches/{id}            -> 批次最终状态与日志序号范围（first_seq..last_seq）
GET  /batches                 -> 全部批次摘要
```

**可见性**：未提交的批次只存在于 `state/batches.json`，普通读取（`/read`）
与业务状态（`/state`）都看不到；提交时整批记录按加入顺序一次性入链
（全程持元数据锁），且提交水位只会在整批的 `last_seq` 推进——读者要么看到
整批、要么完全看不到，批次绝不会落在提交水位两侧。多数派确认超时的批次保持
`committing`（记录已在本地持久化），相同 `write_id` 重试继续同一序号区间。

**幂等**（提交结果登记在 `batches.json` 的 `commits` 表，重启不失）：

* 同一幂等键 + 相同内容（按加入顺序的操作列表摘要）重复提交
  -> 返回第一次的结果（`replay: true`，含首次的序号范围），不重复入链；
* 同一幂等键 + 不同内容 -> `409 idempotency_conflict`；
* 同一批次被并发提交 -> 元数据锁串行化，只有一个提交真正生效，
  其余拿到同一首次结果；未给幂等键时以批次 id 兜底，同批次重试仍幂等。

**有效期**：批次创建时带 TTL（默认 `DEFAULT_TTL_MS`，上限 `MAX_TTL_MS`）。
主动放弃（`abort`）或过期后：再提交返回 `409 batch_aborted` /
`410 batch_expired`，且从未占用任何日志序号。

**提交协议与崩溃恢复**（唯一提交点 = 幂等登记落盘）：

```
1. 批次置 committing 并落盘（记录 first_seq 与内容摘要）
2. 整批记录按序追加到段日志（逐条 fsync）
3. commits[key] = 提交结果并落盘        —— 唯一提交点
4. 批次置 committed 并落盘
```

| 崩溃时机 | 恢复动作 |
|---|---|
| 第 1 步后、记录入链前/中 | 整批未提交：批次记录必是链尾后缀，截断回滚（跨段则删段），批次回到 `open` 可重试，不留序号空洞 |
| 第 3 步（提交点）后 | 整批已提交：启动时补记批次状态；恢复后的重复提交仍返回原幂等结果 |

运行期追加失败（如磁盘错误）同样整批回滚，不留半批。

---

## 3.5 多数派提交水位与线性一致读

普通写入（`POST /append`）与原子批次提交（`POST /batches/{id}/commit`）
都遵循同一个多数派提交流程，且都接受调用方提供的 **`write_id`**：

```
1. 主先把记录持久化到本地日志（fsync + 链头锚点），登记 write_id 与待定提议；
2. 主等待当前任期的多数投票成员确认「同一日志位置」（摘要一致）；
3. 多数派确认后推进持久化的 commit_index，只有 <= commit_index 的记录可见。
```

* **`write_id` 幂等**：缺省自动生成。相同 `write_id` 的重试永远续用同一
  任期与序号区间——已提交返回首次结果（`replay:true`）、等待中继续等待、
  被新任期截断返回 `409 commit_superseded`。绝不追加重复记录或返回另一个序号。
* **`commit_timeout`**：超时返回 `504 commit_timeout`，错误详情含
  `term / write_id / first_seq / last_seq / commit_index`；记录仍是待定状态，
  相同 `write_id` 重试继续同一提交过程。`timeout_ms`（默认 `COMMIT_TIMEOUT_MS`，
  50..120000ms）可控制等待上限，`{"wait": false}` 只做本地持久化立即返回。
* **原子批次不被水位拆开**：提交水位只在提议边界（普通写 / 整批 / 任期标记）
  推进，一个批次永远整体可见或整体不可见。
* **待定区间可查询**：`GET /replica`、`GET /status` 返回 `commit_index`、
  `pending`（待定序号区间与提议明细）、`member_acks`（各成员确认位置）。
  `GET /read` 与 `GET /state` 默认只返回不超过提交水位的内容（更高的读取
  返回空页并标记 `ahead_of_commit`）。

### 线性一致读

`GET /state?consistency=linearizable`（或 `/read?...&consistency=linearizable`）
触发一次 **读屏障（ReadIndex）**：当前主在**当前任期**向多数投票成员发心跳
并得到确认后，才返回提交水位内的状态。

* 联系不到多数派 -> `504 read_barrier_timeout`；
* 等待期间发现更高任期/失去主身份 -> `409 term_changed`；
* 授权过期 -> `403 grant_expired`；
* 备实例 -> `403 not_primary`，详情给出当前 `role`、`term` 与已知主 `primary`。

### 故障切换与旧任期尾部

新主竞选时只保留**能由多数派证明的日志前缀**（投票响应携带 `attest_seq`），
超出此前缀的旧主半提交尾部会被裁掉，不会暴露；已经返回成功的记录（曾被
多数派确认）绝不回退、不重复。新主就任时先写入一条**当前任期标记**
（内部 `data` 记录）：旧任期的待定尾部不能仅凭新主自己的副本变成已提交，
必须等这条当前任期记录先被多数派确认后，才随提交水位一起确认（Raft 安全性）。
故障切换后旧主不会自动改拉新主，新主会通过 `POST /cluster/append_entries`
**主动推送**增量并回收确认；旧主本地若有分叉的未提交尾部，则停在 `conflict`
等待显式处理，绝不静默覆盖已确认历史。

---

## 3.6 未来生效的日志变更预约（scheduled changes）

调用方可以预约一组**未来到点才生效**的 put/delete 操作。每个预约包含唯一
请求标识 `request_id`、墙上生效时间 `effective_at`（epoch 毫秒）和一组按序
执行的操作。预约先持久化（`state/schedules.json`），到点后由**持有当前有效
任期授权的可写主实例**领取，并作为一个**原子批次**提交：整组记录作为一个
`kind=schedule` 提议入链，提交水位只会越过整组边界——到期操作**要么全部生效、
要么完全不生效**，绝不会落在提交水位两侧。

```
POST /schedules          {request_id, effective_at, ops:[put|delete...]}
GET  /schedules[?status=pending|executing|applied|cancelled|superseded]
GET  /schedules/{request_id}
POST /schedules/{request_id}/reschedule {effective_at, expected_version}
POST /schedules/{request_id}/cancel     {expected_version}
POST /schedules/tick      # 手动触发一轮调度（后台线程默认每 200ms 自动跑）
```

预约状态机与查询字段：

| 状态 | 含义 |
|---|---|
| `pending` | 未到生效时间，或已到点未被领取（含停机错过、待补跑） |
| `executing` | 已被主领取，整组记录已在本地链上，等待多数派确认（可重试） |
| `applied` | 整组越过提交水位、业务一次性可见（终态），含 `first_seq/last_seq` 最终日志位置 |
| `cancelled` | 生效开始前被携带当前版本的取消请求取消（终态，无业务变更） |
| `superseded` | 生效开始前被改期取代，或日志被更新任期截断（终态） |

**幂等与乐观版本**

* 相同 `request_id` + 相同内容（操作列表与生效时间）重复创建 → 返回原预约
  （`replay:true`），不重复安排；相同 `request_id` + 不同内容/时间 →
  `409 schedule_conflict`。
* 每次改期 `version += 1`。取消/改期必须携带 `expected_version`，版本过期
  → `409 version_conflict`，**旧版本不能覆盖较新的安排**。
* 只有 `pending` 可取消/改期；`executing/applied` 的取消得到
  `409 already_started`。取消与到期领取在同一把元数据锁上串行，因此竞争有
  **唯一结果**：要么取消成功且没有业务变更，要么执行成功且取消明确返回「已经开始」。

**领取、接管与任期（沿用原请求标识，绝不重复执行）**

* 执行批次/提交使用由 `request_id` 确定性派生的 `batch_id`/`write_id`（按领取
  纪元 epoch 区分）。执行开始后发生进程退出、授权失效或主切换：新主从复制
  镜像的预约元数据接管未完成预约，沿用原 `request_id` 在**同一日志位置**完成
  提交；日志被更新任期截断时纪元前滚、在新位置重做整组，旧 `write_id` 已
  superseded，**旧实例稍后返回的结果不可能被写成成功**。
* 到期执行暂时得不到足够成员确认时，预约停在 `executing`、**保留原日志位置**
  进入可重试状态；成员恢复后由后续 ack/主动推送推进提交水位，下一轮继续原过程。
* 同一生效时间的预约严格按**创建顺序**处理（`(effective_at, seq)` 排序），
  后一个预约必须能看到前一个预约完成后的状态。

**墙上时间、时钟回拨与停机补跑**

* 调度依据可持久化的墙上时间（`effective_at` 落盘）。`applied` 是终态，**系统
  时间回拨不会让已 applied 的预约再次执行**（批次幂等登记同样去重）。
* 停机期间错过的预约在恢复后按原顺序补跑；单轮处理受可配置的
  `CATCHUP_BATCH_LIMIT`（默认 100）约束，超出部分留到下一轮，**不阻塞即时写入**。

后台调度线程只在当前可写主上运行；备实例通过复制协议（boundary / records 页
携带 `schedules` 镜像）只读同步预约元数据，提升为主后即可接管。新增配置项：
`SCHEDULER_ENABLED`（默认 true）、`SCHEDULER_INTERVAL_MS`（默认 200）、
`CATCHUP_BATCH_LIMIT`（默认 100）。

---

## 4. 压缩协议（核心不变量）

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
| 活动段尾部被篡改/截断 | 链尖与 `head.json` 链头锚点不符 | **拒绝启动**（`tail_corrupt`），不对外提供被篡改状态 |
| 已提交边界自身损坏 | current.json 与 gen-N 不一致 | **拒绝启动**（`boundary_corrupt`），不静默篡改历史 |

### 并发

线程内 RLock + 跨进程 `flock(LOCK_NB)`：同进程多任务、多进程副本同时
压缩时，只有一个能进入提交，其余立即得到 `409 {"error":"busy"}`，
审计链与 checkpoint 目录中只有一个新一代。

---

## 5. HTTP API

| 方法 路径 | 说明 |
|---|---|
| `POST /append` | `{type, payload, write_id?, timeout_ms?, wait?}` → 多数派确认后返回 `{seq, write_id, committed, commit_index,...}`；超时 `504 commit_timeout` |
| `POST /batches` | 创建原子批次 `{idempotency_key?, ttl_ms?}` → `{batch_id, status:"open",...}` |
| `POST /batches/{id}/ops` | 分次加入记录 `{ops:[{type,payload},...]}`（或单条 `{type,payload}`） |
| `POST /batches/{id}/commit` | 提交批次（可带 `write_id?/timeout_ms?/wait?`）：整批越提交水位一次性可见；幂等重放返回首次结果 |
| `POST /batches/{id}/abort` | 放弃批次（之后不能再提交） |
| `GET /batches/{id}` | 批次最终状态与日志序号范围 `first_seq..last_seq` |
| `GET /batches` | 全部批次摘要 |
| `POST /schedules` | 创建未来生效预约 `{request_id,effective_at,ops}`；相同请求+内容返回原预约，内容不同 `409` |
| `GET /schedules[?status=]` | 预约列表（可按状态过滤）与各状态计数 |
| `GET /schedules/{request_id}` | 预约状态与最终日志位置 `first_seq..last_seq` |
| `POST /schedules/{request_id}/reschedule` | 携带 `expected_version` 改期（旧版本 `409 version_conflict`） |
| `POST /schedules/{request_id}/cancel` | 携带 `expected_version` 取消（执行已开始则 `409 already_started`） |
| `POST /schedules/tick` | 手动触发一轮到期领取（后台线程默认自动） |
| `GET /read?from=&limit=[&consistency=linearizable]` | 读原始事件；默认只返回 `<= commit_index` 的记录（待定位置返回空页 `ahead_of_commit`）；压缩掉的序号 → `410 compacted` |
| `GET /state[?consistency=linearizable]` | **从头读取的业务含义**：默认折叠到提交水位；线性一致读先做多数派读屏障 |
| `GET /head` | 当前 checkpoint 与可读起点 `readable_from` |
| `POST /readers[/{id}]` | 登记读者：`{pin_seq, ttl_ms?}`（id 可省略自动生成） |
| `POST /readers/{id}/heartbeat` | `{ttl_ms?, position?}` 续租/推进游标 |
| `DELETE /readers/{id}` | 释放钉位 |
| `GET /readers` | 列出读者、剩余租约、是否存活 |
| `GET /pins` | **最老钉住点、受保护段、可回收范围/字节数** |
| `POST /compact` | 触发压缩（只回收已越提交水位的段）；body `{"force":true}` 绕过段数阈值 |
| `GET /compact/result` | **最近一次压缩结果**（成功/失败阶段/等价证明/跳过原因） |
| `POST /compact/verify` | 重新复核当前边界，返回逐项 checks |
| `GET /audit` | 压缩审计链 |
| `GET /replica` | **复制/集群总览**：phase、角色、任期、授权、已同步序号、**`commit_index` 提交水位、`pending` 待定区间、`member_acks` 各成员确认位置**、来源边界、延迟、最近错误 |
| `POST /replica` | 备实例指定来源 `{peer_url}`（须为 standby） |
| `POST /replica/cycle` | 手动触发一次拉取周期（返回 applied/installed_snapshot/commit_index/status） |
| `POST /replica/stop` / `POST /replica/reset` | 停止跟随 / 冲突后人工重置（冲突不能自动恢复） |
| `GET /replica/boundary` | （复制协议）主导出任期、tip、提交水位与当前 checkpoint 边界 |
| `GET /replica/records?from=&limit=&term=` | （复制协议）导出增量记录（含提交水位、待定提议、任期标记） |
| `GET /replica/snapshot?gen=&term=` | （复制协议）导出完整边界（snapshot+manifest+pointer+提交水位） |
| `POST /cluster/promote` | 备实例竞选提升：`{term?, ttl_ms?, voters?, required_seq?}`；只保留多数派可证前缀并写入任期标记 |
| `POST /cluster/stepdown` | 当前主主动交接（授权立即失效、降为备） |
| `POST /cluster/grant` | 授权续租 `{ttl_ms?}`（过期后不能续，必须重新竞选） |
| `POST /cluster/request_vote` | （选举协议）候选拉票：每任期持久化最多一票，回执含 `attest_seq` 可证位置 |
| `POST /cluster/lease?term=` | （租约协议）主任期心跳；见更高任期的节点自动让位 |
| `POST /cluster/ack` | （提交协议）成员上报确认位置 / 主读屏障心跳（`barrier:true`） |
| `POST /cluster/append_entries` / `GET /cluster/progress` | （复制协议）主主动推送增量 / 备汇报同步进度（故障切换后旧主无需改配即可追上） |
| `GET /status` | 总览：段、租约、钉位、checkpoint、压缩状态、提交水位、待定区间、cluster/replication |
| `GET /health` | 健康检查 |

错误统一为 `{"error","message","details"}`，语义化状态码
（400 参数 / 403 非主或授权过期 / 404 / 409 冲突（幂等冲突、批次已放弃、压缩忙、
`commit_superseded`、`term_changed`）/ 410 租约过期、批次过期或序号已压缩 /
413 / 500 / **504 `commit_timeout`、`read_barrier_timeout`**）。

### 快速试一下

```bash
python -m app                      # 默认 /data，:8080

curl -s -XPOST localhost:8080/append -H 'content-type: application/json' \
  -d '{"type":"put","payload":{"key":"k0","value":1}}'
# 原子批次：创建 -> 分次加入 -> 提交（可带幂等键）
B=$(curl -s -XPOST localhost:8080/batches -H 'content-type: application/json' \
  -d '{"idempotency_key":"order-42","ttl_ms":60000}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["batch_id"])')
curl -s -XPOST localhost:8080/batches/$B/ops -H 'content-type: application/json' \
  -d '{"ops":[{"type":"put","payload":{"key":"a","value":1}},{"type":"delete","payload":{"key":"k0"}}]}'
curl -s -XPOST localhost:8080/batches/$B/commit     # 整批一次性可见；重复提交返回首次结果
curl -s localhost:8080/batches/$B                   # 批次最终状态与序号范围
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

## 6. 主备复制与故障切换

角色只有两种：**primary（主）** 与 **standby（备）**。备实例只接受读与
复制，写入（`/append`、原子批次、压缩）一律 `403 not_primary`。集群
状态持久化在 `state/cluster.json`（任期、授权、投票），复制进度持久化在
`state/replica.json`（已确认序号/摘要、来源边界、最近错误）。

### 6.1 建立复制：一致边界 + 顺序增量

```
# 备实例（BOOTSTRAP_ROLE=standby 启动后，或运行时）：
POST /replica   {"peer_url": "http://primary:8080"}
# 后台线程按 REPLICATION_INTERVAL_MS 自动拉取；也可手动：
POST /replica/cycle
```

每个拉取周期：

1. `GET /replica/boundary` 得到来源当前任期、tip 与 checkpoint 边界
   （**一致的起点**）；来源任期低于本地任期 -> 明确拒绝 `stale_term`。
2. 备库没有同一代边界、或已确认位置没有越过来源边界时，先
   `GET /replica/snapshot` 安装完整边界；否则直接增量。
3. `GET /replica/records?from=synced+1` 顺序应用增量。

增量应用的校验（`apply_records`）：

* **顺序**：每条记录必须是 `synced_seq+1`；
* **摘要链**：`prev` 必须等于已确认位置摘要，重算 `digest` 必须一致；
* **去重**：序号 `<= synced` 的重复段逐条与本地摘要比对，相同才跳过
  （**绝不重复应用**），不同即冲突。
* 每条记录 fsync 后才前滚 `synced_seq` 并原子落盘；中断重启从已确认
  位置继续（记录已 fsync、进度未落盘的窗口在启动时重放对账前滚）。
* 若本地历史与来源在已同步位置分叉（`prev` 不符、重复段摘要不同、
  快照凭证在已确认点不一致）：**停在 `conflict`**，不静默覆盖、
  不继续提供错误结果、禁止提升；只有显式 `POST /replica/reset`
  才能离开冲突。

### 6.2 全量边界安装的原子性

快照安装与压缩使用同一套「临时目录 + 唯一提交点」协议：

```
1. 复核 envelope 自校验、snapshot/manifest/pointer 互链、状态摘要、
   与本地已确认历史的分叉检查
2. 完整写入 gen-N.tmp.* 并 fsync，再原子 rename      [snapshot_after_verify / snapshot_after_write]
3. audit 留痕 + current.json 原子替换（唯一提交点）  [snapshot_after_switch]
4. 删除边界覆盖的本地旧前缀；崩溃则启动时凭 pending_prefix 幂等补做
```

| 崩溃时机 | 重启后 |
|---|---|
| 第 2 步完成前 | 临时目录被清除，仍是**安装前的完整状态** |
| 第 3 步（提交点）后、前缀清理中 | 新边界已完整可见，幂等补删前缀，**不会快照与增量各一半** |

备库落后太多（需要的序号已被主压缩）时，复制状态机自动重装当前边界后再追；
重启恢复后继续逐条校验顺序与摘要链。

### 6.3 任期、授权与故障切换

* **单调任期（term）**：本地见到任何更高任期立即前滚；旧任期的写入、
  复制数据、拉票请求一律 `stale_term` 拒绝。
* **带有效期的授权（grant）**：只有 `role=primary` 且
  `grant_expires_at > now` 的实例接受新写入；过期 -> `403 grant_expired`，
  且不能续租（`/cluster/grant`），必须以更高任期重新竞选成功才能再写。
  多节点主由后台 `LeaseRefresher` 向成员发任期心跳，**拿到多数派确认才
  续期**；联系不到多数派时授权到期自动不可写。
* **每个任期恰好/最多一个胜者**：投票（voted_for）持久化，重启不重置。
  候选**先持久化进入任期并投自己一票，再向其他成员拉票**；因此同一任期
  两个候选并发申请时会互相拿到 `already_voted`，决胜票只可能投给其中
  一个——可达的奇数选举集合中恰好一个成功，另一个拿到
  `already_voted`/`leader_valid` 拒绝并以 `election_lost` 失败
  （败者本任期票已投出，须以更高任期重试）；任何情况下都不会两个都
  成功。当前主授权有效时拒绝任何让位投票；优雅切换先
  `POST /cluster/stepdown`。
* **未追平不提升**：竞选时 `synced_seq` 必须达到来源 tip
  （或显式 `required_seq`）；从未联系过来源、处于冲突态也禁止提升。
  投票方还会比较日志新旧（tip seq/摘要），日志落后的候选拿不到票。
* **旧主不能复活**：提升成功后，旧主角色已是备（stepdown 或收到更高
  任期心跳），其旧任期写入被 `not_primary`/`grant_expired` 拒绝、
  旧任期复制数据被 `stale_term` 拒绝；授权过期或进程重启都不会让旧
  任期重新生效（cluster.json 落盘恢复）。
* **只保留多数派可证前缀**：投票响应携带投票方可证明的位置 `attest_seq`，
  新主就任时把日志裁到多数派共同持有前缀；旧主独占的半提交尾部被丢弃，
  曾被多数派确认（返回过成功）的记录绝不回退、不重复。新主先写入当前任期
  标记，旧任期待定记录必须等它随当前任期记录一起被多数派确认后才可见。
* **主主动推送**：故障切换后旧主不会自动改拉新主，新主通过
  `/cluster/progress` 探测成员位置并以 `/cluster/append_entries` 主动推送
  增量（与拉取走同一套逐记录校验），确认位置随响应回收、计入多数派提交。

### 6.4 状态接口

`GET /replica` 的 `phase`：

| phase | 含义 |
|---|---|
| `syncing` | 备实例正在安装边界或追赶增量（含可重试的传输错误） |
| `caught_up` | 备实例已追平来源 tip |
| `conflict` | 已同步位置分叉，冻结：不复制、不提升，等待人工 reset |
| `writable_primary` | 主且授权有效，可接受写入 |
| `grant_invalid` | 主角色但授权过期/缺失，不可写，需要重新竞选 |

同响应给出：角色、term、授权剩余毫秒、`synced_seq`/`synced_digest`、
来源边界（term/tip/checkpoint）、`lag_ms`、`last_error`，以及多数派
提交视图 `commit_index`、`pending`（待定序号区间与提议明细：普通写 /
原子批次 / 任期标记）与 `member_acks`（各成员当前确认位置）。

### 6.5 典型两节点切换（HTTP）

```bash
# 主 P 以 :8080 启动；备 S：
PORT=8081 BOOTSTRAP_ROLE=standby NODE_ID=S \
  PEERS="P=http://127.0.0.1:8080,S=http://127.0.0.1:8081" \
  REPLICA_SOURCE=http://127.0.0.1:8080 python -m app
curl -sXPOST localhost:8081/replica -H 'content-type: application/json' \
  -d '{"peer_url":"http://127.0.0.1:8080"}'
curl -s localhost:8081/replica       # 观察 phase: syncing -> caught_up
# 切换：主交接 -> 备竞选更高任期（voters 为多数派成员集合）
curl -s -XPOST localhost:8080/cluster/stepdown
curl -s -XPOST localhost:8081/cluster/promote -H 'content-type: application/json' \
  -d '{"term":2,"ttl_ms":30000,"voters":["http://127.0.0.1:8080","http://127.0.0.1:8081"]}'
```

---

## 7. Docker 部署

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
| `JANITOR_ENABLED` | `true` | 后台自动压缩线程（仅主实例实际压缩） |
| `JANITOR_INTERVAL_MS` | `5000` | janitor 轮询间隔 |
| `NODE_ID` | 自动生成 | 节点标识（写入 `state/cluster.json`，不可变更） |
| `BOOTSTRAP_ROLE` | `primary` | 数据目录首次初始化角色：`primary`/`standby`（重启后以落盘角色为准） |
| `PEERS` | 空 | `node_id=base_url,...` 选举/租约成员地址 |
| `BOOTSTRAP_VOTERS` | 空 | 首次自举时持久化的选举成员 node_id 列表 |
| `REPLICA_SOURCE` | 空 | 以 standby 首次引导时的来源主 URL（之后可 `POST /replica` 改） |
| `GRANT_TTL_MS` | `10000` | 竞选成功获得的授权有效期（上限 5 分钟） |
| `LEASE_INTERVAL_MS` | `2000` | 多节点主授权续租心跳间隔 |
| `REPLICATION_INTERVAL_MS` | `500` | 备库拉取 / 主主动推送 / 角色监督周期 |
| `REPLICATION_BATCH` | `500` | 单页增量记录上限 |
| `COMMIT_TIMEOUT_MS` | `3000` | 写入等待多数派提交的默认超时（可被请求体 `timeout_ms` 覆盖，50..120000） |
| `READ_BARRIER_TIMEOUT_MS` | `3000` | 线性一致读屏障的默认等待超时 |
| `SCHEDULER_ENABLED` | `true` | 到期预约领取后台线程（仅当前可写主实际领取） |
| `SCHEDULER_INTERVAL_MS` | `200` | 调度轮询间隔 |
| `CATCHUP_BATCH_LIMIT` | `100` | 单轮到期/补跑处理上限（超出留下一轮，不阻塞即时写入） |

> 压缩期间持有元数据锁，会短暂阻塞追加（默认小段配置下为毫秒~亚秒级）。
> 主备部署使用**各自独立的数据卷**；压缩互斥的 `flock` 针对同一数据目录，
> 主备之间的一致性由复制协议（快照边界 + 哈希链增量）保证。

---

## 8. 测试

```bash
python tests/run_tests.py          # 无 pytest 环境（标准库垫片运行器）
# 若已安装 pytest：
python -m pytest -q
```

覆盖：哈希链/滚动/半行截断、租约登记续租过期释放/重启有效、
钉位保护与多读者最小钉、首代/续代压缩业务等价、410 与从头读取、
凭证追溯原始段、篡改段/篡改快照导致失败并完全回滚、
活动段尾部篡改/截断被链头锚点拒绝（重启拒绝启动、校验失败）、
同进程多任务与跨进程 flock 互斥、四个压缩阶段的断电恢复、
原子批次（提交前不可见、整批有序可见、幂等重放/冲突、并发单胜者、
放弃与过期、跨段回滚、三个提交阶段的断电恢复、重启后幂等结果保持）、
HTTP 端到端（含 janitor 在钉位保护下不误删、释放后自动压缩、重启边界），
以及主备复制与故障切换：

* 初始一致边界 + 增量追平、角色/序号/来源边界/延迟/最近错误状态
* 中断后从已确认位置继续、重复段不重复应用
* 已同步位置分叉（增量/重复段/快照凭证）停在 conflict、冻结、禁止提升
* 全新备库装快照、落后自动重装边界、原子批次整批到达
* 快照安装三个阶段（复核后/写完/指针切换后）断电：只剩旧完整态或完整新态
* 记录已 fsync、进度未落盘的重启对账（不丢、不重）
* 授权 TTL 写门控、未追平不提升、有效主拒绝让位、stepdown 后提升、
  同任期多数派单胜者、旧主旧任期写入/拉票/复制数据被拒、
  授权过期不可写不可续、重启不复活旧任期、备见到更高任期前滚
* 真实 HTTP 双进程复制、交接、提升与备进程重启续传

多数派提交与线性一致读（`tests/test_consensus.py`）：

* 多数派正常确认推进 `commit_index`；少数派隔离返回 `commit_timeout`
  （含任期/序号/write_id），记录保持待定且不暴露
* 相同 `write_id` 重试继续同一提交：同一序号、不重复入链、忽略不同负载
* 并发写入保持日志顺序；原子批次不会在提交水位两侧拆开
* 主在本地持久化后崩溃：重启记录保持待定，相同 `write_id` 续用同一序号
* 新主只保留多数派可证前缀并写入任期标记；旧任期待定尾部不能仅凭新主
  副本提交；旧主半提交尾部冲突时冻结，已确认记录跨重启/切换不回退不重复
* 线性一致读：多数派读屏障成功、失去多数派/任期变化/授权过期被拒、
  备实例返回角色/任期/已知主；读与状态默认只暴露 `<= commit_index` 的内容

未来生效预约（`tests/test_schedules.py` 与 `tests/test_server.py` 的
`test_schedule_*`）：

* 正常到期 pending→executing→applied、整组原子可见、返回最终日志位置；
  同请求+内容重放返回原预约、内容不同 `schedule_conflict`；坏操作不入预约
* 同一生效时间严格按创建顺序、更早生效时间优先；后一个看到前一个完成后的状态
* 改期版本号自增、旧版本竞争被 `version_conflict` 拒绝；携带当前版本取消成功
  且无业务变更；取消与到期领取竞争唯一结果（取消成功 / 取消得到 already_started）
* 执行中进程退出后重启接管：沿用原 `request_id`/`write_id` 在原位置续等，
  不重复入链；成员不足保持 executing、保留原位置、恢复后续跑
* 执行期间主切换：半提交组被可证前缀截断，新主沿用请求标识在新位置重做一次，
  旧 `write_id` 纪元前滚、旧实例迟到结果无法写成成功
* 时钟回拨不重复执行 applied；停机积压按原顺序分批补跑、受单轮上限约束、
  不阻塞即时写入
* HTTP 端到端：创建/幂等/改期/取消竞争/手动 tick/后台线程自动到期

`CRASH_HOOK={after_fold|after_write|after_verify|after_switch}` 可让进程在压缩对应阶段 `os._exit(99)`；
`CRASH_HOOK={batch_after_mark|batch_after_append|batch_after_register}`
可让进程在批次提交对应阶段崩溃；
`CRASH_HOOK={snapshot_after_verify|snapshot_after_write|snapshot_after_switch}`
可让进程在备库安装快照边界的对应阶段崩溃，用于灾难演练。
