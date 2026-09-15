"""多数派提交水位、待定提议与 write_id 幂等登记（持久化 state/commit.json）。

核心概念：
  commit_index  已确认提交水位：只有 <= commit_index 的记录对普通读取可见。
                主实例在当前任期内拿到同一日志位置的多数派确认才能推进；
                备实例由主实例在复制 ack 回执中告知，绝不自己越过。
  pending       待定提议：按日志顺序排列的提交单位（一次普通写或一个原子批次），
                每个提议记录 first_seq..last_seq、所属任期与 write_id。
                提交水位只能在「提议边界」上推进：原子批次永远不会在提交水位
                两侧被拆开；旧任期提议必须等当前任期先提交一条记录后，
                才能随提交水位一起被确认（Raft 安全规则）。
  writes        write_id 登记表：调用方提供或服务端生成，重启有效。
                同 write_id 重试必须续用同一序号：已提交 -> 原结果重放，
                待多数确认 -> 继续等待同一提交过程；被新任期截断 ->
                明确返回 commit_superseded，绝不追加重复记录。

线程安全由 Kernel.meta_lock 保证；所有变更立即原子落盘。
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from .common import GENESIS, now_ms, read_json, write_json


@dataclass
class Proposal:
    first_seq: int
    last_seq: int
    term: int = 0           # 记录入链时主实例的任期；0 表示来源未知
    kind: str = "write"     # write | batch | term_marker
    write_id: Optional[str] = None
    batch_id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "first_seq": self.first_seq, "last_seq": self.last_seq,
            "term": self.term, "kind": self.kind,
            "write_id": self.write_id, "batch_id": self.batch_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Proposal":
        return cls(first_seq=int(d["first_seq"]), last_seq=int(d["last_seq"]),
                   term=int(d.get("term", 0)), kind=d.get("kind", "write"),
                   write_id=d.get("write_id"), batch_id=d.get("batch_id"))


def new_write_id() -> str:
    return f"w-{uuid.uuid4().hex[:20]}"


class CommitStore:
    """提交水位 + 待定提议 + write_id 登记 + 各成员确认位置。"""

    def __init__(self, path: str):
        self.path = path
        self.commit_index = 0
        self.commit_term = 0
        self.commit_digest = GENESIS
        self.pending: list[Proposal] = []
        self.writes: dict[str, dict] = {}
        self.acks: dict[str, dict] = {}
        # 任期标记位置：{seq: term}，随记录持久化（即使标记进入提交水位、
        # 被快照压缩也不删除），供判定「待定记录所属任期」使用。
        self.term_markers: dict[int, int] = {}
        if os.path.exists(path):
            d = read_json(path)
            self.commit_index = int(d.get("commit_index", 0))
            self.commit_term = int(d.get("commit_term", 0))
            self.commit_digest = d.get("commit_digest", GENESIS)
            self.pending = [Proposal.from_dict(x) for x in d.get("pending", [])]
            self.writes = dict(d.get("writes", {}))
            self.acks = dict(d.get("acks", {}))
            self.term_markers = {int(k): int(v)
                                 for k, v in (d.get("term_markers") or {}).items()}

    def persist(self) -> None:
        write_json(self.path, {
            "commit_index": self.commit_index,
            "commit_term": self.commit_term,
            "commit_digest": self.commit_digest,
            "pending": [p.to_dict() for p in self.pending],
            "writes": self.writes,
            "acks": self.acks,
            "term_markers": self.term_markers,
        })

    def add_term_marker(self, seq: int, term: int) -> None:
        self.term_markers[seq] = term

    def term_at(self, seq: int) -> int:
        """seq 位置记录所属任期：最近一个 marker_seq <= seq 的任期，缺省 1。"""
        term = 1
        for marker_seq in sorted(self.term_markers):
            if marker_seq <= seq:
                term = self.term_markers[marker_seq]
            else:
                break
        return term

    def term_marker_view(self) -> list[dict]:
        return [{"seq": s, "term": t}
                for s, t in sorted(self.term_markers.items())]

    # ---------- write_id 登记 ----------

    def register_write(self, write_id: str, term: int, first_seq: int,
                       last_seq: int, kind: str = "write",
                       batch_id: Optional[str] = None) -> dict:
        entry = {
            "write_id": write_id, "term": term,
            "first_seq": first_seq, "last_seq": last_seq,
            "kind": kind, "batch_id": batch_id,
            "status": "pending",
            "created_ts": now_ms(),
        }
        self.writes[write_id] = entry
        return entry

    def get_write(self, write_id: str) -> Optional[dict]:
        return self.writes.get(write_id)

    def mark_write_committed(self, write_id: str, commit_ts: Optional[int] = None) -> None:
        e = self.writes.get(write_id)
        if e is not None:
            e["status"] = "committed"
            e["committed_ts"] = commit_ts or now_ms()

    def mark_write_superseded(self, write_id: str, new_term: int) -> None:
        e = self.writes.get(write_id)
        if e is not None and e["status"] == "pending":
            e["status"] = "superseded"
            e["superseded_by_term"] = new_term
            e["superseded_ts"] = now_ms()

    # ---------- 待定提议 ----------

    def add_proposal(self, proposal: Proposal, persist: bool = True) -> None:
        self.pending.append(proposal)
        self.pending.sort(key=lambda p: p.first_seq)
        if persist:
            self.persist()

    def replace_pending(self, proposals: list[Proposal]) -> None:
        """备实例：用主实例导出的待定提议表替换本地（主为准）。"""
        self.pending = sorted(proposals, key=lambda p: p.first_seq)

    def drop_committed(self) -> list[Proposal]:
        """剔除已完全进入提交水位的提议，返回被提交的提议（保持顺序）。"""
        committed: list[Proposal] = []
        keep: list[Proposal] = []
        for p in self.pending:
            if p.last_seq <= self.commit_index:
                committed.append(p)
            else:
                keep.append(p)
        self.pending = keep
        return committed

    def prune_pending_through(self, seq: int) -> None:
        """新任期截断后：丢弃 first_seq > seq 的待定提议。"""
        self.pending = [p for p in self.pending if p.first_seq <= seq]

    def pending_ranges(self) -> list[dict]:
        tip_known = self.pending[-1].last_seq if self.pending else self.commit_index
        return {
            "first_seq": (self.commit_index + 1 if self.pending else None),
            "last_seq": (tip_known if self.pending else None),
            "count": len(self.pending),
            "proposals": [p.to_dict() for p in self.pending],
        }

    # ---------- 成员确认 ----------

    def record_ack(self, node_id: str, term: int, match_seq: int,
                   commit_seq: int) -> dict:
        prev = self.acks.get(node_id) or {}
        if int(prev.get("match_seq", -1)) > match_seq:
            match_seq = int(prev["match_seq"])  # 确认位置不许倒退
        ack = {"node_id": node_id, "term": term, "match_seq": match_seq,
               "commit_seq": commit_seq, "ts": now_ms()}
        self.acks[node_id] = ack
        return ack

    def ack_view(self) -> dict:
        return {nid: {"match_seq": a["match_seq"], "commit_seq": a["commit_seq"],
                      "term": a.get("term"), "ts": a.get("ts")}
                for nid, a in self.acks.items()}

    # ---------- 提交水位 ----------

    def set_commit(self, seq: int, term: int, digest: str) -> bool:
        if seq < self.commit_index:
            raise ValueError("commit_index cannot move backwards")
        if seq == self.commit_index:
            return False
        self.commit_index = seq
        self.commit_term = term
        self.commit_digest = digest
        return True
