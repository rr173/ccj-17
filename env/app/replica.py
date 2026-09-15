"""备用实例的复制关系与复制进度（持久化 data/state/replica.json）。

状态机（status）：
  idle       尚未配置复制来源
  syncing    正在安装初始边界（快照）或追赶增量
  caught_up  已追到来源（tip 一致）；来源再写入后下次轮询回到 syncing
  conflict   本地历史与来源在已确认位置分叉 / 增量记录顺序或摘要链校验失败；
             停在此状态，不静默覆盖、不继续提供错误结果，禁止提升
  grant_invalid  仅角色层使用（见 cluster 状态接口），复制本身不产生此态

进度字段：
  synced_seq / synced_digest  已确认应用的链尖（每条记录应用成功并 fsync 后
                              才前滚；中断重启只会从这里继续）
  source_boundary             来源当前边界（checkpoint gen/seq/tail_anchor/tip）
  last_error                  最近一次错误（code/message/ts），成功后清空

全量安装（快照边界）的原子性见 Kernel.install_snapshot：
  staging 目录完整写完并独立复核 -> 原子切换 -> 安装完成前崩溃重启，
  本文件仍记录安装前的 synced_*，启动恢复丢弃 staging、继续使用旧完整状态。
"""
from __future__ import annotations

import os
from typing import Any, Optional

from .common import GENESIS, now_ms, read_json, write_json

IDLE = "idle"
SYNCING = "syncing"
CAUGHT_UP = "caught_up"
CONFLICT = "conflict"


class ReplicaStore:
    """线程安全由 Kernel.meta_lock 保证；变更立即原子落盘。"""

    def __init__(self, path: str):
        self.path = path
        self.peer_url: Optional[str] = None
        self.status = IDLE
        self.synced_seq = 0
        self.synced_digest = GENESIS
        self.source_boundary: Optional[dict] = None
        self.last_error: Optional[dict] = None
        self.last_attempt_ts = 0
        self.last_success_ts = 0
        if os.path.exists(path):
            d = read_json(path)
            self.peer_url = d.get("peer_url")
            self.status = d.get("status", IDLE)
            self.synced_seq = int(d.get("synced_seq", 0))
            self.synced_digest = d.get("synced_digest", GENESIS)
            self.source_boundary = d.get("source_boundary")
            self.last_error = d.get("last_error")
            self.last_attempt_ts = int(d.get("last_attempt_ts", 0))
            self.last_success_ts = int(d.get("last_success_ts", 0))
        # 重启不丢失任何进度；syncing/caught_up/conflict 的最终判定
        # 由 Kernel._reconcile_replica 在链对账后完成。

    def persist(self) -> None:
        write_json(self.path, {
            "peer_url": self.peer_url,
            "status": self.status,
            "synced_seq": self.synced_seq,
            "synced_digest": self.synced_digest,
            "source_boundary": self.source_boundary,
            "last_error": self.last_error,
            "last_attempt_ts": self.last_attempt_ts,
            "last_success_ts": self.last_success_ts,
        })

    # ---------- 生命周期 ----------

    def configure(self, peer_url: str) -> None:
        self.peer_url = peer_url
        if self.status == IDLE:
            self.status = SYNCING
        self.last_attempt_ts = now_ms()
        self.persist()

    def stop(self) -> None:
        self.peer_url = None
        if self.status != CONFLICT:
            self.status = IDLE
        self.persist()

    def set_status(self, status: str) -> None:
        self.status = status
        self.persist()

    def mark_attempt(self) -> None:
        self.last_attempt_ts = now_ms()
        if self.status != CONFLICT:
            self.status = SYNCING
        self.persist()

    def mark_error(self, code: str, message: str, **details: Any) -> dict:
        self.last_error = {"code": code, "message": message,
                           "details": details, "ts": now_ms()}
        if code == "replication_conflict":
            self.status = CONFLICT
        self.persist()
        return self.last_error

    def clear_error(self) -> None:
        if self.last_error is not None:
            self.last_error = None
            self.persist()

    def advance(self, seq: int, digest: str, status: Optional[str] = None) -> None:
        """确认进度前滚：只许前进、且必须与已确认锚点衔接（调用方已校验）。"""
        if seq < self.synced_seq:
            raise ValueError("replica progress cannot move backwards")
        self.synced_seq = seq
        self.synced_digest = digest
        self.last_success_ts = now_ms()
        self.last_error = None
        if status is not None:
            self.status = status
        self.persist()

    def set_boundary(self, boundary: dict, status: Optional[str] = None) -> None:
        self.source_boundary = boundary
        if status is not None:
            self.status = status
        self.persist()

    def reset_to_installed(self, seq: int, digest: str, boundary: dict) -> None:
        """快照边界完整安装后：进度一次性跳到新边界（唯一提交点之后调用）。"""
        self.synced_seq = seq
        self.synced_digest = digest
        self.source_boundary = boundary
        self.last_error = None
        self.last_success_ts = now_ms()
        self.status = SYNCING  # 快照已装好，继续追增量；追上后转 caught_up
        self.persist()

    def is_conflict(self) -> bool:
        return self.status == CONFLICT

    def view(self) -> dict:
        return {
            "role_status": self.status,
            "peer_url": self.peer_url,
            "synced_seq": self.synced_seq,
            "synced_digest": self.synced_digest,
            "source_boundary": self.source_boundary,
            "last_error": self.last_error,
            "last_attempt_ts": self.last_attempt_ts,
            "last_success_ts": self.last_success_ts,
            "lag_ms": (max(0, now_ms() - self.last_success_ts)
                       if self.last_success_ts else None),
        }
