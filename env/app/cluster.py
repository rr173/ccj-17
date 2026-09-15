"""集群角色、单调任期、带有效期授权（领导租约）与投票记录。
持久化在 data/state/cluster.json（tmp+rename+fsync 原子写）。

核心规则：
1. term 单调递增：本地见到的任何更高任期都会立刻前滚；旧任期的写入、
   复制数据、投票请求一律拒绝（stale_term）。
2. 只有持有「当前任期 + 未过期」授权的实例才能接受写入；授权有 TTL，
   过期即不可写（renew 只能在仍有效、且仍为当前任期时续期）。
3. 每个任期只投一票（voted_for 落盘），重启不重置；两个实例并发竞选
   同一任期时，多数派互斥保证最多一个成功。
4. 授权过期或进程重启都不会让旧任期复活：过期后写入被拒，
   必须以更高任期重新竞选成功才能再次成为主。

角色：
  primary  主（须有有效授权才可写）
  standby  备（从某个来源拉取复制，永远不接受本地写）
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Optional

from .common import now_ms, read_json, write_json

PRIMARY = "primary"
STANDBY = "standby"

DEFAULT_GRANT_TTL_MS = 10_000
MAX_GRANT_TTL_MS = 5 * 60_000  # 单次授权上限 5 分钟（最小 1000ms）


def new_node_id() -> str:
    return f"n-{uuid.uuid4().hex[:12]}"


@dataclass
class ClusterState:
    node_id: str = ""
    role: str = PRIMARY
    term: int = 0
    grant_expires_at: int = 0        # 主授权有效期；role=primary 且 >now 才可写
    granted_at: int = 0
    voted_for: Optional[str] = None  # 当前 term 已投给的候选（None=未投）
    voters: tuple[str, ...] = ()     # 选举多数派成员（含自己）

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "role": self.role,
            "term": self.term,
            "grant_expires_at": self.grant_expires_at,
            "granted_at": self.granted_at,
            "voted_for": self.voted_for,
            "voters": list(self.voters),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ClusterState":
        return cls(
            node_id=d.get("node_id", ""),
            role=d.get("role", PRIMARY),
            term=int(d.get("term", 0)),
            grant_expires_at=int(d.get("grant_expires_at", 0)),
            granted_at=int(d.get("granted_at", 0)),
            voted_for=d.get("voted_for"),
            voters=tuple(d.get("voters", []) or ()),
        )


class ClusterStore:
    """线程安全由 Kernel.meta_lock 保证；所有变更立即原子落盘。"""

    def __init__(self, path: str, node_id: Optional[str], voters: list[str]):
        self.path = path
        self.fresh = not os.path.exists(path)
        if not self.fresh:
            self.s = ClusterState.from_dict(read_json(path))
            if node_id and self.s.node_id and node_id != self.s.node_id:
                raise ValueError(f"node_id mismatch: disk={self.s.node_id} given={node_id}")
            if node_id:
                self.s.node_id = node_id
        else:
            nid = node_id or new_node_id()
            self.s = ClusterState(node_id=nid, role=PRIMARY)
        # 配置的选举成员（持久化首次成功竞选使用的集合；未配置时默认仅自己）
        cfg_voters = [v for v in voters if v]
        if cfg_voters:
            if self.s.node_id not in cfg_voters:
                cfg_voters = [self.s.node_id, *cfg_voters]
            self.s.voters = tuple(cfg_voters)
        elif not self.s.voters:
            self.s.voters = (self.s.node_id,)
        self.persist()

    def persist(self) -> None:
        write_json(self.path, self.s.to_dict())

    # ---------- 读视图 ----------

    @property
    def node_id(self) -> str:
        return self.s.node_id

    @property
    def role(self) -> str:
        return self.s.role

    @property
    def term(self) -> int:
        return self.s.term

    def is_primary(self) -> bool:
        return self.s.role == PRIMARY

    def grant_valid(self, now: Optional[int] = None) -> bool:
        at = now if now is not None else now_ms()
        return self.s.role == PRIMARY and self.s.grant_expires_at > at

    def grant_remaining_ms(self, now: Optional[int] = None) -> int:
        at = now if now is not None else now_ms()
        if self.s.role != PRIMARY:
            return 0
        return max(0, self.s.grant_expires_at - at)

    def view(self, now: Optional[int] = None) -> dict:
        at = now if now is not None else now_ms()
        return {
            "node_id": self.s.node_id,
            "role": self.s.role,
            "term": self.s.term,
            "writable": self.s.role == PRIMARY and self.s.grant_expires_at > at,
            "grant_valid": self.s.role == PRIMARY and self.s.grant_expires_at > at,
            "grant_expires_at": self.s.grant_expires_at if self.s.role == PRIMARY else None,
            "grant_remaining_ms": self.grant_remaining_ms(at),
            "voters": list(self.s.voters),
        }

    # ---------- 任期推进 / 角色切换 ----------

    def bump_term(self, term: int) -> bool:
        """见到更高任期：前滚 term、清投票、主失去授权并降为备。

        返回是否发生了前滚。相等或更低的任期不改变状态（更低由调用方拒绝）。
        """
        if term <= self.s.term:
            return False
        self.s.term = term
        self.s.voted_for = None
        self.s.grant_expires_at = 0
        if self.s.role == PRIMARY:
            self.s.role = STANDBY
        self.persist()
        return True

    def become_standby(self) -> None:
        if self.s.role != STANDBY:
            self.s.role = STANDBY
        self.s.grant_expires_at = 0
        self.persist()

    def can_vote(self, term: int, candidate: str) -> tuple[bool, str]:
        """本任期是否可把票投给 candidate（不修改状态；投票由 cast_vote 落盘）。"""
        if term < self.s.term:
            return False, "stale_term"
        if self.s.role == PRIMARY and self.grant_valid():
            # 当前主授权仍有效：无论同任期还是更高任期都不让位
            # （优雅切换必须先 stepdown；授权过期后才可被更高任期接管）
            return False, "leader_valid"
        if term > self.s.term:
            return True, "ok"  # 更高任期且无有效主：尚未投票
        if self.s.voted_for in (None, candidate):
            return True, "ok"
        return False, "already_voted"

    def cast_vote(self, term: int, candidate: str) -> None:
        """落盘投票：进入该任期（更高任期先前滚，授权随之失效）并记 voted_for。"""
        if term > self.s.term:
            self.bump_term(term)  # 更高任期：主授权失效、角色降备
        self.s.voted_for = candidate
        self.persist()

    def assume_leadership(self, term: int, grant_ttl_ms: int,
                          now: Optional[int] = None) -> None:
        """竞选成功：以 term 就任主并颁发带有效期的授权。"""
        at = now if now is not None else now_ms()
        if term < self.s.term:
            raise ValueError("cannot assume leadership at a stale term")
        if term > self.s.term:
            self.s.term = term
            self.s.voted_for = None
        self.s.role = PRIMARY
        self.s.granted_at = at
        self.s.grant_expires_at = at + grant_ttl_ms
        self.persist()

    def renew_grant(self, ttl_ms: int, now: Optional[int] = None) -> dict:
        """授权续租：只有当前主、授权仍在有效期内才能续；过期须重新竞选。"""
        at = now if now is not None else now_ms()
        if self.s.role != PRIMARY:
            raise ValueError("not primary")
        if self.s.grant_expires_at <= at:
            raise TimeoutError("grant expired; a new election is required")
        self.s.grant_expires_at = at + ttl_ms
        self.persist()
        return self.view(at)

    def invalidate_grant(self) -> None:
        """主动放弃主身份（交接）：授权立即失效，降为备。"""
        self.s.role = STANDBY
        self.s.grant_expires_at = 0
        self.persist()

    def quorum(self) -> int:
        return len(self.s.voters) // 2 + 1
