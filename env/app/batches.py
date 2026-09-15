"""原子批次：幂等键、提交前不可见、崩溃只恢复成整批已提交/未提交。

持久化在 data/state/batches.json（tmp+rename+fsync 原子写）：
  batches: 批次生命周期  open -> committing -> committed
                         open -> aborted；open 超 expires_at 派生为 expired
  commits: 幂等提交登记表  key -> 首次提交结果（批次提交的唯一提交点）

提交协议（Kernel.meta_lock 内串行，见 Kernel.commit_batch）：
  1. 批次置 committing 并落盘（记录 first_seq 与内容摘要 content_hash）
  2. 整批记录按加入顺序追加到段日志（逐条 fsync）
  3. commits[key] = 提交结果并落盘 —— 唯一提交点
  4. 批次置 committed 并落盘

崩溃恢复（Kernel.startup -> Kernel._recover_batches）：
  仍处于 committing 的批次，若 commits 中已有同 key 同内容摘要的登记，
  说明提交点已过 -> 整批已提交（补记批次状态）；
  否则提交点未到 -> 整批未提交：批次记录必是链尾后缀（提交在锁内串行，
  不会与其他记录交错），截断链尾回滚，批次回到 open 可在有效期内重试，
  不占任何日志序号。
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from .common import digest_json, read_json, write_json

OPEN = "open"
COMMITTING = "committing"
COMMITTED = "committed"
ABORTED = "aborted"
EXPIRED = "expired"  # 派生状态：open 且已过 expires_at（不落盘）


def ops_hash(ops: list[dict]) -> str:
    """批次内容摘要：按加入顺序的规范化操作列表。"""
    return digest_json([{"type": op["type"], "payload": op.get("payload")} for op in ops])


@dataclass
class Batch:
    batch_id: str
    idempotency_key: Optional[str]
    status: str
    ops: list[dict] = field(default_factory=list)
    content_hash: Optional[str] = None   # 提交时确定：ops 列表摘要
    first_seq: Optional[int] = None      # 提交结果：日志序号范围
    last_seq: Optional[int] = None
    record_count: int = 0
    replay: bool = False                 # 本次提交是幂等重放（记录来自首次提交）
    created_at: int = 0
    expires_at: int = 0
    committed_ts: Optional[int] = None

    def registry_key(self) -> str:
        """幂等登记键：调用方给的幂等键；未给则以批次 id 兜底（同批次重试仍幂等）。"""
        return self.idempotency_key or f"batch:{self.batch_id}"

    def effective_status(self, now: int) -> str:
        if self.status == OPEN and self.expires_at <= now:
            return EXPIRED
        return self.status

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "idempotency_key": self.idempotency_key,
            "status": self.status,
            "ops": self.ops,
            "content_hash": self.content_hash,
            "first_seq": self.first_seq,
            "last_seq": self.last_seq,
            "record_count": self.record_count,
            "replay": self.replay,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "committed_ts": self.committed_ts,
        }


class BatchStore:
    """批次与幂等登记的持久化存储。线程安全由 Kernel 元数据锁保证。"""

    def __init__(self, path: str):
        self.path = path
        self.batches: dict[str, Batch] = {}
        self.commits: dict[str, dict] = {}
        if os.path.exists(path):
            data = read_json(path)
            for b in data.get("batches", []):
                bt = Batch(**b)
                self.batches[bt.batch_id] = bt
            self.commits = dict(data.get("commits", {}))

    def persist(self) -> None:
        write_json(self.path, {
            "batches": [b.to_dict() for b in self.batches.values()],
            "commits": self.commits,
        })

    # ---------- 生命周期 ----------

    def create(self, idempotency_key: Optional[str], ttl_ms: int, now: int) -> Batch:
        b = Batch(
            batch_id=f"b-{uuid.uuid4().hex[:16]}",
            idempotency_key=idempotency_key,
            status=OPEN,
            created_at=now,
            expires_at=now + ttl_ms,
        )
        self.batches[b.batch_id] = b
        self.persist()
        return b

    def get(self, batch_id: str) -> Optional[Batch]:
        return self.batches.get(batch_id)

    def add_ops(self, b: Batch, ops: list[dict]) -> None:
        b.ops.extend({"type": op["type"], "payload": op.get("payload")} for op in ops)
        self.persist()

    def mark_committing(self, b: Batch, first_seq: int, content_hash: str) -> None:
        """提交意图落盘：崩溃恢复据此识别未完成的提交并回滚。"""
        b.status = COMMITTING
        b.first_seq = first_seq
        b.content_hash = content_hash
        self.persist()

    def register_commit(self, key: str, entry: dict) -> None:
        """唯一提交点：登记落盘后，批次即视为整批已提交。"""
        self.commits[key] = entry
        self.persist()

    def mark_committed(self, b: Batch, entry: dict, replay: bool) -> None:
        b.status = COMMITTED
        b.content_hash = entry["content_hash"]
        b.first_seq = entry["first_seq"]
        b.last_seq = entry["last_seq"]
        b.record_count = entry["record_count"]
        b.committed_ts = entry["committed_ts"]
        b.replay = replay
        self.persist()

    def rollback_to_open(self, b: Batch) -> None:
        """整批未提交：清掉提交痕迹，批次回到 open（有效期内可重试）。"""
        b.status = OPEN
        b.first_seq = None
        b.content_hash = None
        self.persist()

    def mark_aborted(self, b: Batch) -> None:
        b.status = ABORTED
        self.persist()

    # ---------- 查询 ----------

    def commit_entry(self, key: str) -> Optional[dict]:
        return self.commits.get(key)

    def committing_batches(self) -> list[Batch]:
        return [b for b in self.batches.values() if b.status == COMMITTING]
