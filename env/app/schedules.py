"""未来生效的日志变更预约（scheduled change）。

调用方提交「唯一请求标识 request_id + 生效时间 effective_at（墙上毫秒）
+ 一组按序执行的 put/delete 操作」，服务端持久化预约；只有到期时由
**持有当前有效任期授权的可写主实例**领取并作为一个原子批次提交
（整组全部生效或完全不生效，复用多数派提交水位，绝不会落在水位两侧）。

预约状态机：
  pending     尚未到生效时间，或已到点但尚未被领取
  executing   已被某个主实例领取，整组记录在本地链上，等待多数派确认
              （成员不足时停在此状态、保留原日志位置，恢复后续跑）
  applied     整组记录越过提交水位，业务变更一次性可见（终态）
  cancelled   生效开始前被携带当前版本的取消请求取消（终态，无业务变更）
  superseded  生效开始前被携带当前版本的改期取代，或日志被更新任期截断
              （终态）

幂等与乐观版本：
  * 相同 request_id + 相同内容重复创建 -> 返回原预约（replay:true），
    绝不重复安排；相同 request_id + 不同内容 -> 409 schedule_conflict。
  * 每次改期 version += 1；取消/改期必须携带「我所知道的当前版本」expected_version，
    版本过期（已被别人改过）-> 409 version_conflict，不能覆盖较新的安排。
  * 只有 pending 允许取消/改期；executing/applied 取消返回 409 already_started
    （与到期领取竞争时有唯一结果：要么取消成功且无业务变更，要么执行成功
    且取消明确返回已经开始）。

崩溃与主切换：
  * 预约元数据与执行批次都原子落盘；执行批次使用由 request_id 确定性派生的
    batch_id / idempotency_key / write_id，因此任何主实例接管都沿用原请求
    标识完成同一提交，绝不重复执行，旧实例迟到的结果也不会被写成成功。
  * 调度依据可持久化的墙上时间（effective_at 落盘）；系统时间回拨不会让
    applied 预约再次执行（applied 是终态，且批次幂等登记去重）。
  * 停机错过的预约在恢复后按 (effective_at, seq) 原顺序补跑，单轮补跑有
    可配置上限 catchup_batch_limit，超出留到下一轮，不阻塞即时写入。

持久化在 data/state/schedules.json（tmp+rename+fsync 原子写）。
线程安全由 Kernel.meta_lock 保证。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

from .common import digest_json, now_ms, read_json, write_json

PENDING = "pending"
EXECUTING = "executing"
APPLIED = "applied"
CANCELLED = "cancelled"
SUPERSEDED = "superseded"

TERMINAL = (APPLIED, CANCELLED, SUPERSEDED)
ACTIVE = (PENDING, EXECUTING)

MIN_EFFECTIVE_AHEAD_MS = 0          # 允许立即/过去时间（用于补跑语义）
MAX_FUTURE_MS = 366 * 24 * 3600 * 1000
MAX_SCHEDULE_OPS = 10_000


def schedule_content_hash(ops: list[dict]) -> str:
    """预约内容摘要：按执行顺序的规范化操作列表（与批次 ops_hash 同形）。"""
    return digest_json([{"type": op["type"], "payload": op.get("payload")} for op in ops])


def exec_batch_id(request_id: str) -> str:
    """执行批次 id：由请求标识确定性派生，接管的主沿用同一批次。"""
    return f"sched:{request_id}"


def exec_write_id(request_id: str, epoch: int) -> str:
    """执行提交 write_id：请求标识 + 领取纪元，确定性且跨进程一致。

    正常执行/同一主重试都在同一 epoch 内复用同一 write_id（同一序号区间）；
    日志被更新任期截断后 epoch 前滚，派生出新的 write_id 在新位置重做，
    旧 write_id 已被标记 superseded，旧实例迟到结果无法覆盖。
    """
    return f"sched-w:{request_id}:{epoch}"


@dataclass
class Schedule:
    request_id: str
    effective_at: int
    ops: list[dict] = field(default_factory=list)
    content_hash: str = ""
    version: int = 1
    status: str = PENDING
    seq: int = 0                       # 同一生效时间内的创建顺序（单调）
    created_at: int = 0
    created_term: int = 0
    # ---- 执行期字段 ----
    epoch: int = 0                     # 领取纪元：每次（重新）领取 +1
    batch_id: Optional[str] = None
    write_id: Optional[str] = None
    first_seq: Optional[int] = None    # 最终日志位置
    last_seq: Optional[int] = None
    attempt_count: int = 0
    last_attempt_at: Optional[int] = None
    executed_term: Optional[int] = None
    applied_at: Optional[int] = None
    cancelled_at: Optional[int] = None
    supersede_reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "effective_at": self.effective_at,
            "ops": self.ops,
            "content_hash": self.content_hash,
            "version": self.version,
            "status": self.status,
            "seq": self.seq,
            "created_at": self.created_at,
            "created_term": self.created_term,
            "epoch": self.epoch,
            "batch_id": self.batch_id,
            "write_id": self.write_id,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
            "attempt_count": self.attempt_count,
            "last_attempt_at": self.last_attempt_at,
            "executed_term": self.executed_term,
            "applied_at": self.applied_at,
            "cancelled_at": self.cancelled_at,
            "supersede_reason": self.supersede_reason,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Schedule":
        return cls(
            request_id=d["request_id"],
            effective_at=int(d["effective_at"]),
            ops=list(d.get("ops", [])),
            content_hash=d.get("content_hash", ""),
            version=int(d.get("version", 1)),
            status=d.get("status", PENDING),
            seq=int(d.get("seq", 0)),
            created_at=int(d.get("created_at", 0)),
            created_term=int(d.get("created_term", 0)),
            epoch=int(d.get("epoch", 0)),
            batch_id=d.get("batch_id"),
            write_id=d.get("write_id"),
            first_seq=d.get("first_seq"),
            last_seq=d.get("last_seq"),
            attempt_count=int(d.get("attempt_count", 0)),
            last_attempt_at=d.get("last_attempt_at"),
            executed_term=d.get("executed_term"),
            applied_at=d.get("applied_at"),
            cancelled_at=d.get("cancelled_at"),
            supersede_reason=d.get("supersede_reason"),
        )


class ScheduleStore:
    """预约的持久化存储。线程安全由 Kernel.meta_lock 保证。"""

    def __init__(self, path: str):
        self.path = path
        self.schedules: dict[str, Schedule] = {}
        self._seq = 0
        if os.path.exists(path):
            data = read_json(path)
            for d in data.get("schedules", []):
                s = Schedule.from_dict(d)
                self.schedules[s.request_id] = s
                if s.seq > self._seq:
                    self._seq = s.seq

    def persist(self) -> None:
        write_json(self.path, {
            "seq": self._seq,
            "schedules": [s.to_dict() for s in self.schedules.values()],
        })

    # ---------- 创建 / 查询 ----------

    def create(self, request_id: str, effective_at: int, ops: list[dict],
               content_hash: str, term: int, now: int) -> Schedule:
        self._seq += 1
        s = Schedule(
            request_id=request_id, effective_at=effective_at, ops=ops,
            content_hash=content_hash, version=1, status=PENDING,
            seq=self._seq, created_at=now, created_term=term)
        self.schedules[request_id] = s
        self.persist()
        return s

    def get(self, request_id: str) -> Optional[Schedule]:
        return self.schedules.get(request_id)

    def list(self) -> list[Schedule]:
        return sorted(self.schedules.values(), key=lambda s: (s.effective_at, s.seq))

    # ---------- 生命周期变更（调用方持 meta_lock）----------

    def reschedule(self, s: Schedule, effective_at: int, now: int) -> None:
        s.effective_at = effective_at
        s.version += 1
        s.status = PENDING
        s.supersede_reason = None
        self.persist()

    def cancel(self, s: Schedule, now: int) -> None:
        s.status = CANCELLED
        s.cancelled_at = now
        self.persist()

    def supersede(self, s: Schedule, reason: str, now: int) -> None:
        s.status = SUPERSEDED
        s.supersede_reason = reason
        self.persist()

    def claim(self, s: Schedule, epoch: int, batch_id: str, write_id: str,
              term: int, now: int) -> None:
        """领取执行：记录纪元/批次/写入标识，状态转 executing。"""
        s.status = EXECUTING
        s.epoch = epoch
        s.batch_id = batch_id
        s.write_id = write_id
        s.executed_term = term
        s.attempt_count += 1
        s.last_attempt_at = now
        self.persist()

    def record_attempt_error(self, s: Schedule, now: int) -> None:
        s.attempt_count += 1
        s.last_attempt_at = now
        self.persist()

    def mark_applied(self, s: Schedule, first_seq: int, last_seq: int,
                     term: int, now: int) -> None:
        s.status = APPLIED
        s.first_seq = first_seq
        s.last_seq = last_seq
        s.executed_term = term
        s.applied_at = now
        self.persist()

    # ---------- 调度选择 ----------

    def due_order(self, now: int, limit: Optional[int] = None) -> list[Schedule]:
        """到期且仍 active 的预约，按 (effective_at, 创建 seq) 排序。

        生效时间 <= now 即到期（停机错过的也包含在内）；同一生效时间严格
        按创建顺序，保证后一个预约能看到前一个完成后的状态。
        """
        due = [s for s in self.schedules.values()
               if s.status in ACTIVE and s.effective_at <= now]
        due.sort(key=lambda s: (s.effective_at, s.seq))
        if limit is not None and limit >= 0:
            due = due[:limit]
        return due

    def counts(self) -> dict[str, int]:
        c = {PENDING: 0, EXECUTING: 0, APPLIED: 0, CANCELLED: 0, SUPERSEDED: 0}
        for s in self.schedules.values():
            c[s.status] = c.get(s.status, 0) + 1
        c["total"] = len(self.schedules)
        return c

    # ---------- 主 -> 备 元数据复制 ----------

    def snapshot(self) -> dict:
        """导出全部预约（主为准），随复制页/边界带给备实例。"""
        return {"seq": self._seq,
                "schedules": [s.to_dict() for s in self.schedules.values()]}

    def merge_primary(self, snap: dict, now: Optional[int] = None) -> bool:
        """备实例用主的预约表与本地对账（主为准）。

        - 主表中存在的预约以主版本覆盖（按 seq 单调，绝不退创建序）；
        - 备本地有、主表没有的预约保留（理论上不该出现：只有主能创建）；
        - 仅当内容实际变化时落盘。返回是否发生了变化。
        """
        if not isinstance(snap, dict) or "schedules" not in snap:
            return False
        changed = False
        seq = int(snap.get("seq", self._seq))
        if seq > self._seq:
            self._seq = seq
        for d in snap["schedules"]:
            try:
                incoming = Schedule.from_dict(d)
            except (KeyError, TypeError, ValueError):
                continue
            cur = self.schedules.get(incoming.request_id)
            if cur is None:
                self.schedules[incoming.request_id] = incoming
                changed = True
                continue
            # 主状态更新（seq/版本单调推进）才覆盖；不允许主把终态改回活态
            if cur.status in TERMINAL and incoming.status not in TERMINAL:
                continue
            if (incoming.seq, incoming.version) >= (cur.seq, cur.version) and \
                    (incoming.to_dict() != cur.to_dict()):
                self.schedules[incoming.request_id] = incoming
                changed = True
        if changed:
            self.persist()
        return changed
