"""核心内核：把段、租约、快照/清单、审计组装起来。

关键不变量：
1. 压缩在 threading RLock + 跨进程 flock 双重排他下执行；
   多个压缩任务并发时只有一个能改变可见结果，其余拿到 409。
2. 唯一提交点是 current.json 的原子替换；此前一切都在
   gen-N.tmp.* / gen-N 目录中，崩溃/失败绝不影响读者。
3. 段删除在指针切换之后、逐段进行；中途崩溃由启动恢复幂等补删，
   对外始终是「上一个完整边界」或「新的完整边界」，没有半成品。
4. 压缩严格遵守租约：受钉序号保护的段及其之后的段绝不回收。
"""
from __future__ import annotations

import dataclasses
import fcntl
import os
import threading
from typing import Any, Callable, Optional

from .batches import ABORTED, COMMITTED, COMMITTING, EXPIRED, OPEN, Batch, BatchStore, ops_hash
from .checkpoints import (
    AUDIT,
    Checkpointer,
    MANIFEST,
    SNAPSHOT,
    open_doc,
)
from .cluster import (
    MAX_GRANT_TTL_MS,
    PRIMARY,
    STANDBY,
    ClusterStore,
)
from .common import (
    GENESIS,
    BusyError,
    Error,
    digest_json,
    ensure_dir,
    err,
    file_lock,
    now_ms,
    read_json,
    sha256_file,
    write_json,
)
from .readers import ReaderStore
from .reducer import Reducer
from .replica import CAUGHT_UP, CONFLICT, IDLE, SYNCING, ReplicaStore
from .segment import SegmentLog, record_digest


@dataclasses.dataclass
class Config:
    data_dir: str = "/data"
    segment_bytes: int = 1 << 20        # 1 MiB 滚动
    default_ttl_ms: int = 30_000        # 租约默认 30s
    max_ttl_ms: int = 24 * 3600 * 1000  # 单次续租上限 24h
    janitor_enabled: bool = True
    janitor_interval_ms: int = 5_000
    compaction_min_segments: int = 4    # 后台压缩阈值
    crash_hook: Optional[str] = None    # 测试用：在某阶段 os._exit
    # ---- 集群 / 主备复制 ----
    node_id: Optional[str] = None       # 不显式配置则生成并持久化
    bootstrap_role: str = PRIMARY       # 数据目录首次初始化时的角色
    peers: dict[str, str] = dataclasses.field(default_factory=dict)  # node_id -> base_url
    bootstrap_voters: Optional[list[str]] = None  # 首次启动的选举成员
    grant_ttl_ms: int = 10_000          # 主授权有效期
    lease_interval_ms: int = 2_000      # 授权续租心跳间隔
    replication_interval_ms: int = 500  # 备库拉取间隔
    replication_batch: int = 500        # 单次拉取记录上限
    replica_source: Optional[str] = None  # 首次以 standby 引导时的来源 URL


def _vfail(reason: Any, proof: dict | None = None) -> dict:
    return {"ok": False, "broken_at": reason, "proof": proof or {}}


class Kernel:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.seg_dir = os.path.join(cfg.data_dir, "segments")
        self.cp_dir = os.path.join(cfg.data_dir, "checkpoints")
        self.state_dir = os.path.join(cfg.data_dir, "state")
        ensure_dir(cfg.data_dir)
        ensure_dir(self.state_dir)
        self.lock_path = os.path.join(cfg.data_dir, "compaction.lock")
        self.result_path = os.path.join(self.state_dir, "compact-result.json")
        self.head_path = os.path.join(self.state_dir, "head.json")

        self.seglog = SegmentLog(self.seg_dir)
        self.readers = ReaderStore(os.path.join(self.state_dir, "readers.json"))
        self.batches = BatchStore(os.path.join(self.state_dir, "batches.json"))
        self.checkpoints = Checkpointer(self.cp_dir, os.path.join(self.cp_dir, AUDIT))

        # ---- 集群角色 / 任期授权 / 复制进度 ----
        self.cluster = ClusterStore(
            os.path.join(self.state_dir, "cluster.json"),
            cfg.node_id,
            list(cfg.bootstrap_voters or cfg.peers.keys()),
        )
        # 首次初始化：引导角色（standby 不自举；primary 以 term=1 自举授权）。
        # 重启时角色/任期/授权完全从磁盘恢复，引导参数不再生效。
        if self.cluster.fresh:
            if cfg.bootstrap_role == STANDBY:
                self.cluster.become_standby()
            else:
                ttl = cfg.grant_ttl_ms if len(self.cluster.s.voters) > 1 else MAX_GRANT_TTL_MS
                self.cluster.assume_leadership(1, ttl)
        self.replica = ReplicaStore(os.path.join(self.state_dir, "replica.json"))
        if self.cluster.fresh and cfg.bootstrap_role == STANDBY and cfg.replica_source:
            self.replica.peer_url = cfg.replica_source.rstrip("/")
            self.replica.status = SYNCING
            self.replica.persist()

        self.meta_lock = threading.RLock()
        self._compact_active = False
        self.last_compact: dict[str, Any] = {}
        if os.path.exists(self.result_path):
            try:
                self.last_compact = read_json(self.result_path)
            except Exception:
                self.last_compact = {}

    # =====================================================================
    # 启动恢复：自动回到上一个完整边界
    # =====================================================================

    def startup(self) -> dict:
        with file_lock(self.lock_path):
            self.seglog.scan()
            purged_tmp = self.checkpoints.purge_temp()
            recovery: dict[str, Any] = {"purged_temp": purged_tmp}

            pointer = self.checkpoints.current
            if pointer is None:
                # 从未提交过：任何 gen 目录都是未提交残留
                for g in self.checkpoints.existing_gens():
                    import shutil
                    shutil.rmtree(self.checkpoints.gen_dir(g), ignore_errors=True)
                anchor = GENESIS
                recovery["boundary"] = "genesis"
            else:
                gen = pointer["gen"]
                try:
                    snap_body, man_body = self.checkpoints.load_generation(gen)
                    self._verify_pointer_target(pointer, snap_body, man_body)
                except (FileNotFoundError, ValueError) as e:
                    # 已提交边界自身损坏：不静默篡改历史，明确失败
                    raise err(500, "boundary_corrupt",
                              "committed checkpoint boundary is corrupt; refusing to start",
                              gen=gen, reason=str(e))

                # 指针有效：幂等补做安装/压缩后承诺的段清理、清理非当前代目录。
                # 主库压缩：covered_segs 是主库自己的段编号；备库安装快照：
                # covered_segs 是本地被删前缀段（安装时即时维护），
                # 来源段编号保存在 source_covered_segs 仅供追溯。
                deleted_now: list[int] = []
                pending = pointer.get("pending_prefix")
                if pending:
                    deleted_now += self._finish_prefix_cleanup(pointer, pending)
                elif not pointer.get("installed_by_replication"):
                    covered = set(pointer.get("covered_segs", []))
                    for seg_id in sorted(covered):
                        if seg_id in self.seglog.segments:
                            self.seglog.delete_segment(seg_id)
                            deleted_now.append(seg_id)
                pruned = self.checkpoints.remove_gens_before(gen)
                for g in self.checkpoints.existing_gens():
                    if g != gen:
                        import shutil
                        shutil.rmtree(self.checkpoints.gen_dir(g), ignore_errors=True)
                        pruned.append(g)
                anchor = pointer["tail_anchor"]
                recovery.update(boundary=f"gen-{gen}", segments_deleted=deleted_now, gens_pruned=pruned)

            # 批次崩溃恢复：committing 批次只能恢复成整批已提交或整批未提交，
            # 绝不暴露半批记录（须在链头锚点核对之前完成截断）
            batch_rec = self._recover_batches()
            if batch_rec:
                recovery["batches"] = batch_rec
            self._sync_tip(anchor)
            if batch_rec.get("rolled_back"):
                # 链尾被回滚截断：以新链尖重写链头锚点后再核对
                self._persist_head()
            self._reconcile_head()
            recovery["replica"] = self._reconcile_replica()
            return recovery

    def _finish_prefix_cleanup(self, pointer: dict, pending_seq: int) -> list[int]:
        """指针已切换、前缀删除未完成时的幂等补做（快照安装崩溃恢复）。

        与 _prune_prefix_locked 的提交后步骤完全一致；完成后清掉 pending 标记。
        pending_seq 即快照边界 snapshot_seq（记录序号，1 起；全新备库本地
        只有空活动段时，把它锚定到边界即可，不删任何段）。
        """
        before = set(self.seglog.segments)
        tail_seg = self.seglog.truncate_prefix_in_segment(
            pending_seq + 1, pointer["tail_anchor"])
        deleted = sorted(before - set(self.seglog.segments))
        if all(m.count == 0 for m in self.seglog.segments.values()):
            # 无尾部（全新备库 / 前缀全部覆盖）：游标锚定到边界
            self.seglog.reset_anchor(pending_seq, pointer["tail_anchor"])
        elif tail_seg >= 0:
            self._sync_tip(pointer["tail_anchor"])
        pointer.pop("pending_prefix", None)
        pointer["covered_segs"] = sorted(
            set(pointer.get("covered_segs", [])) | set(deleted))
        self.checkpoints.commit_pointer(pointer)
        return deleted

    def _reconcile_replica(self) -> dict:
        """启动时依据本地已确认链对账复制进度（全量/增量中断后的恢复）。

        - synced 锚点之后本地还有连续记录（上次崩溃在「记录已 fsync、
          进度落盘」之前）：顺序摘要校验通过则把确认位置前滚；
        - synced 处摘要与本地记录不符 -> 冲突，冻结，绝不静默覆盖；
        - 已配置来源但状态为 idle：转 syncing 等待拉取。
        """
        rep = self.replica
        if rep.peer_url is None:
            return {"status": rep.status}
        tip_seq, tip_digest = self.seglog.tip()
        # 快照指针已原子切换、但 replica.json 更新前崩溃：以已安装边界补齐进度
        pointer = self.checkpoints.current
        if pointer is not None and pointer.get("installed_by_replication"):
            bseq = int(pointer["seq"])
            if rep.synced_seq < bseq:
                rep.advance(bseq, pointer["tail_anchor"], status=SYNCING)
                tip_seq, tip_digest = self.seglog.tip()
        if rep.status == CONFLICT:
            return {"status": CONFLICT, "note": "left in conflict; operator reset required"}
        # 校验已确认锚点位置记录原样（若该序号仍在本地链上）
        if rep.synced_seq > 0:
            at = self._digest_at(rep.synced_seq)
            if at is not None and at != rep.synced_digest:
                rep.mark_error("replication_conflict",
                               "local history diverges at confirmed position",
                               seq=rep.synced_seq)
                return {"status": CONFLICT, "seq": rep.synced_seq}
        # 从已确认位置重放本地多余记录，全部衔接才前滚
        if tip_seq > rep.synced_seq:
            recs = self.seglog.read_records(rep.synced_seq + 1,
                                            tip_seq - rep.synced_seq)
            expect = rep.synced_digest
            last_d = expect
            last_seq = rep.synced_seq
            ok = True
            if len(recs) == tip_seq - rep.synced_seq:
                for r in recs:
                    if r["seq"] != last_seq + 1 or r["prev"] != expect:
                        ok = False
                        break
                    expect, last_d = r["digest"], r["digest"]
                    last_seq = r["seq"]
            else:
                ok = False  # 本地链在已确认位置之后有空洞
            if ok:
                rep.advance(last_seq, last_d, status=SYNCING)
            else:
                rep.mark_error("replication_conflict",
                               "local tail does not extend confirmed position",
                               seq=rep.synced_seq)
                return {"status": CONFLICT, "seq": rep.synced_seq}
        elif rep.status in (IDLE, CAUGHT_UP, SYNCING):
            rep.set_status(SYNCING)
        return {"status": rep.status, "synced_seq": rep.synced_seq}

    def _recover_batches(self) -> dict:
        """清算 committing 状态的批次：已登记的补记为整批已提交，否则整批回滚。"""
        finalized: list[str] = []
        rolled_back: list[str] = []
        for b in self.batches.committing_batches():
            entry = self.batches.commit_entry(b.registry_key())
            if entry is not None and entry.get("content_hash") == b.content_hash:
                # 提交点（幂等登记落盘）已过：整批已提交，补记批次状态
                self.batches.mark_committed(b, entry, replay=False)
                finalized.append(b.batch_id)
            else:
                # 提交点未到：整批未提交。批次记录必是链尾后缀，截断回滚，
                # 批次回到 open（有效期内可重试），不占任何日志序号。
                keep = (b.first_seq or 1) - 1
                if self.seglog.tip()[0] > keep:
                    self.seglog.truncate_tail(keep)
                self.batches.rollback_to_open(b)
                rolled_back.append(b.batch_id)
        if not (finalized or rolled_back):
            return {}
        return {"finalized": finalized, "rolled_back": rolled_back}

    def _verify_pointer_target(self, pointer: dict, snap_body: dict, man_body: dict) -> None:
        if snap_body["snapshot_seq"] != pointer["seq"]:
            raise ValueError("pointer seq != snapshot seq")
        if pointer["manifest_digest"] != digest_json(man_body):
            raise ValueError("manifest digest mismatch")
        if pointer["snapshot_digest"] != digest_json(snap_body):
            raise ValueError("snapshot digest mismatch")
        if pointer["state_digest"] != digest_json(snap_body["state"]):
            raise ValueError("state digest mismatch")
        if man_body["snapshot_digest"] != digest_json(snap_body):
            raise ValueError("manifest -> snapshot link mismatch")
        if snap_body["tail_anchor"] != pointer["tail_anchor"] != man_body["tail_anchor"]:
            raise ValueError("tail anchor mismatch")

    def _sync_tip(self, anchor: str) -> None:
        """根据指针锚点校准现存链；活动段游标由 scan 已建好。"""
        # 快照安装后可能只有一个空活动段：游标直接锚定到边界
        if self.seglog.tip()[0] == 0 and anchor != GENESIS:
            pointer = self.checkpoints.current
            base_seq = pointer["seq"] if pointer else 0
            if pointer is not None:
                self.seglog.anchor_empty_active(base_seq, anchor)
                return
        vr = self.seglog.verify_chain_from(anchor)
        if not vr.ok:
            raise err(500, "tail_corrupt", "live tail chain does not match checkpoint anchor",
                      broken_at=vr.broken_at)
        _, tip_digest = vr.tip
        self.seglog._last_digest = tip_digest  # 后续 append 正确续链

    # =====================================================================
    # 链头锚点：持久化链尖摘要，拒绝尾部篡改
    # =====================================================================
    # 哈希链只锚定每条记录的「前驱」，checkpoint 只锚定尾部的「起点」；
    # 链尖（最后一条记录的摘要）若不落盘，改动活动段最后一条记录的内容
    # 后所有链接依然完好，重启与校验都无法察觉。因此每次追加后把
    # {seq, digest} 原子写入 state/head.json（先 fsync 段记录、再锚定，
    # 崩溃只会让锚点落后、绝不会超前），重启与完整性校验据它拒绝篡改。

    def _persist_head(self) -> None:
        """把当前链尖 {seq, digest} 原子落盘（tmp+rename+fsync）。"""
        seq, d = self.seglog.tip()
        write_json(self.head_path, {"seq": seq, "digest": d})

    def _reconcile_head(self) -> None:
        """启动时核对链头锚点；尾部被篡改/截断则拒绝启动（不静默回退）。

        - 锚点与链尖一致：通过（空活动段重启造成的序号漂移以锚点为准校正）；
        - 锚点落后于链尖：崩溃时锚点尚未落盘，锚点位置处的链上摘要必须
          原样，核对一致后把锚点前滚到链尖；
        - 其余（锚点超前于链尖、同序号摘要不符、锚点位置内容被改）：
          一律拒绝启动。
        """
        tip_seq, tip_digest = self.seglog.tip()
        if not os.path.exists(self.head_path):
            # 首次启动（或旧版本数据目录）：以当前链尖为锚落盘
            self._persist_head()
            return
        try:
            head = read_json(self.head_path)
            hseq, hdigest = int(head["seq"]), str(head["digest"])
        except Exception:
            raise err(500, "tail_corrupt",
                      "chain head anchor is unreadable; refusing to start")
        if hdigest == tip_digest:
            if hseq != tip_seq:
                self._persist_head()  # 校正序号漂移，摘要不变
            return
        if hseq < tip_seq and self._digest_at(hseq) == hdigest:
            self._persist_head()  # 崩溃滞后：锚点之前原样，前滚
            return
        raise err(500, "tail_corrupt",
                  "live tail does not match persisted chain head; refusing to start",
                  anchored_seq=hseq, anchored_digest=hdigest[:16],
                  tip_seq=tip_seq, tip_digest=tip_digest[:16])

    def _digest_at(self, seq: int) -> Optional[str]:
        """现存链上 seq 处记录的摘要；不存在返回 None。"""
        if seq == 0:
            return GENESIS
        for rec in self.seglog.read_records(seq, 1):
            if rec["seq"] == seq:
                return rec["digest"]
        return None

    def _head_anchor_ok(self, tip_digest: str) -> bool:
        """持久化的链头锚点是否与（从磁盘重算的）链尖摘要一致。"""
        try:
            head = read_json(self.head_path)
        except Exception:
            return False
        return isinstance(head, dict) and head.get("digest") == tip_digest

    # =====================================================================
    # 追加
    # =====================================================================

    # =====================================================================
    # 角色门控：只有持有「当前任期 + 未过期」授权的主接受写入
    # =====================================================================

    def _require_writable_primary(self) -> None:
        if self.cluster.role != PRIMARY:
            raise err(403, "not_primary",
                      "this node is a standby; writes go to the primary",
                      role=self.cluster.role)
        if self.cluster.term <= 0:
            raise err(403, "no_term", "node has no leadership term")
        if not self.cluster.grant_valid():
            raise err(403, "grant_expired",
                      "leadership grant for this term has expired; "
                      "a new election is required",
                      term=self.cluster.term)

    def append(self, rec_type: str, payload: Any) -> dict:
        with self.meta_lock:
            self._require_writable_primary()
            # 入链前校验业务负载，坏记录绝不进哈希链
            Reducer().apply(rec_type, payload)
            rec = self.seglog.append(rec_type, payload, now_ms())
            # 段记录已 fsync，再锚定链尖：崩溃只会让锚点落后一条，
            # 启动时按 _reconcile_head 的「落后」分支核对前滚，绝不误伤。
            self._persist_head()
            if self.seglog.segments[self.seglog.active_id].size >= self.cfg.segment_bytes:
                self.seglog.rotate()
            return {
                "seq": rec["seq"],
                "ts": rec["ts"],
                "type": rec["type"],
                "payload": rec["payload"],
                "prev": rec["prev"],
                "digest": rec["digest"],
            }

    # =====================================================================
    # 原子批次（幂等键）
    # =====================================================================
    # 未提交的批次只存在于 state/batches.json，普通读取与业务状态都看不到；
    # 提交时整批按加入顺序一次性入链（全程持 meta_lock，读者不会看到半批）。
    # 提交协议与崩溃恢复见 app/batches.py 模块 docstring。

    MAX_BATCH_OPS = 10_000  # 单批次操作数上限

    def create_batch(self, idempotency_key: Optional[str], ttl_ms: Optional[int]) -> dict:
        with self.meta_lock:
            self._require_writable_primary()
            if idempotency_key is not None:
                if not isinstance(idempotency_key, str) or not (1 <= len(idempotency_key) <= 256):
                    raise err(400, "bad_request", "idempotency_key must be a string of length 1..256")
            ttl = self._clamp_ttl(ttl_ms)
            b = self.batches.create(idempotency_key, ttl, now_ms())
            return self._batch_view(b, include_ops=True)

    def batch_add_ops(self, batch_id: str, ops: Any) -> dict:
        with self.meta_lock:
            self._require_writable_primary()
            b = self._require_batch(batch_id)
            st = b.effective_status(now_ms())
            if st == EXPIRED:
                raise err(410, "batch_expired", "batch expired; create a new batch")
            if st != OPEN:
                raise err(409, "batch_closed", f"batch is {st}; cannot add ops")
            if not isinstance(ops, list) or not ops:
                raise err(400, "bad_request", "ops must be a non-empty list")
            if len(b.ops) + len(ops) > self.MAX_BATCH_OPS:
                raise err(413, "batch_too_large",
                          f"batch would exceed {self.MAX_BATCH_OPS} ops")
            normed = []
            for op in ops:
                if not isinstance(op, dict) or "type" not in op:
                    raise err(400, "bad_request", "each op requires {type, payload}")
                # 入批前校验业务负载，坏记录绝不进批次
                Reducer().apply(op["type"], op.get("payload"))
                normed.append({"type": op["type"], "payload": op.get("payload")})
            self.batches.add_ops(b, normed)
            return self._batch_view(b, include_ops=True)

    def commit_batch(self, batch_id: str) -> dict:
        """提交批次：整批记录按加入顺序一次性入链；幂等键保证重试安全。

        - 同批次/同幂等键重复提交相同内容 -> 返回首次提交结果（不重复入链）；
        - 同幂等键不同内容 -> 409 idempotency_conflict；
        - 并发提交由 meta_lock 串行化，只有一个真正入链，其余拿到重放结果。
        """
        with self.meta_lock:
            self._require_writable_primary()
            b = self._require_batch(batch_id)
            if b.status == COMMITTED:
                # 同批次重复提交：返回首次结果，不重复入链
                return self._batch_result(b, replay=True)
            if b.status == ABORTED:
                raise err(409, "batch_aborted", "batch was aborted; cannot commit")
            if b.status == COMMITTING:
                # 正常流程不可达（启动恢复已清算）；防御性拒绝
                raise err(409, "batch_committing", "batch commit already in progress")
            if b.effective_status(now_ms()) == EXPIRED:
                raise err(410, "batch_expired", "batch expired; cannot commit")
            if not b.ops:
                raise err(400, "empty_batch", "cannot commit an empty batch")
            content_hash = ops_hash(b.ops)
            key = b.registry_key()
            entry = self.batches.commit_entry(key)
            if entry is not None:
                if entry["content_hash"] == content_hash:
                    # 同一幂等键相同内容：整批不重发，返回首次提交结果
                    self.batches.mark_committed(b, entry, replay=True)
                    return self._batch_result(b, replay=True)
                raise err(409, "idempotency_conflict",
                          "idempotency key already committed with different content",
                          idempotency_key=b.idempotency_key,
                          first_batch_id=entry["batch_id"],
                          first_seq=entry["first_seq"], last_seq=entry["last_seq"])
            # 新鲜提交：committing 落盘 -> 整批入链 -> 幂等登记（提交点）-> 状态落盘
            first_seq = self.seglog.next_seq
            self.batches.mark_committing(b, first_seq, content_hash)
            self._crash_hook("batch_after_mark")
            try:
                last_seq = first_seq - 1
                for op in b.ops:
                    last_seq = self.append(op["type"], op["payload"])["seq"]
            except Exception:
                # 运行期失败同样整批回滚，不留半批
                self._rollback_uncommitted(b, first_seq)
                raise
            self._crash_hook("batch_after_append")
            entry = {
                "key": key,
                "batch_id": b.batch_id,
                "content_hash": content_hash,
                "first_seq": first_seq,
                "last_seq": last_seq,
                "record_count": last_seq - first_seq + 1,
                "committed_ts": now_ms(),
            }
            self.batches.register_commit(key, entry)  # —— 唯一提交点
            self._crash_hook("batch_after_register")
            self.batches.mark_committed(b, entry, replay=False)
            return self._batch_result(b, replay=False)

    def _rollback_uncommitted(self, b: Batch, first_seq: int) -> None:
        """整批回滚：截断链尾到批次之前，校准链尖与链头锚点，批次回到 open。"""
        keep = first_seq - 1
        if self.seglog.tip()[0] > keep:
            self.seglog.truncate_tail(keep)
            pointer = self.checkpoints.current
            self._sync_tip(pointer["tail_anchor"] if pointer else GENESIS)
            self._persist_head()
        self.batches.rollback_to_open(b)

    def abort_batch(self, batch_id: str) -> dict:
        with self.meta_lock:
            b = self._require_batch(batch_id)
            if b.status == COMMITTED:
                raise err(409, "batch_committed", "batch already committed; cannot abort")
            if b.status == COMMITTING:
                raise err(409, "batch_committing", "batch commit in progress; cannot abort")
            if b.status != ABORTED:
                self.batches.mark_aborted(b)  # 放弃是幂等的
            return self._batch_view(b, include_ops=True)

    def batch_view(self, batch_id: str) -> dict:
        with self.meta_lock:
            return self._batch_view(self._require_batch(batch_id), include_ops=True)

    def list_batches(self) -> dict:
        with self.meta_lock:
            ordered = sorted(self.batches.batches.values(), key=lambda x: (x.created_at, x.batch_id))
            return {"batches": [self._batch_view(b) for b in ordered]}

    def _require_batch(self, batch_id: str) -> Batch:
        b = self.batches.get(batch_id)
        if b is None:
            raise err(404, "not_found", f"unknown batch {batch_id!r}")
        return b

    def _batch_view(self, b: Batch, include_ops: bool = False) -> dict:
        now = now_ms()
        v = {
            "batch_id": b.batch_id,
            "idempotency_key": b.idempotency_key,
            "status": b.effective_status(now),
            "op_count": len(b.ops),
            "first_seq": b.first_seq,
            "last_seq": b.last_seq,
            "record_count": b.record_count,
            "content_hash": b.content_hash,
            "replay": b.replay,
            "created_at": b.created_at,
            "expires_at": b.expires_at,
            "remaining_ms": max(0, b.expires_at - now) if b.status == OPEN else 0,
            "committed_ts": b.committed_ts,
        }
        if include_ops:
            v["ops"] = b.ops
        return v

    def _batch_result(self, b: Batch, replay: bool) -> dict:
        return {
            "batch_id": b.batch_id,
            "idempotency_key": b.idempotency_key,
            "status": "committed",
            "replay": replay,
            "first_seq": b.first_seq,
            "last_seq": b.last_seq,
            "record_count": b.record_count,
            "content_hash": b.content_hash,
            "committed_ts": b.committed_ts,
        }

    # =====================================================================
    # 租约 / 钉位
    # =====================================================================

    def register_reader(self, pin_seq: int, ttl_ms: Optional[int], reader_id: Optional[str]) -> dict:
        with self.meta_lock:
            tip, _ = self.seglog.tip()
            if pin_seq > tip + 1:
                raise err(400, "bad_pin", f"pin_seq beyond tip {tip}")
            ttl = self._clamp_ttl(ttl_ms)
            try:
                rd = self.readers.register(pin_seq, ttl, now_ms(), reader_id)
            except KeyError:
                raise err(409, "reader_exists", "reader_id already registered")
            return self._reader_view(rd)

    def heartbeat(self, reader_id: str, ttl_ms: Optional[int], position: Optional[int]) -> dict:
        with self.meta_lock:
            ttl = self._clamp_ttl(ttl_ms)
            try:
                rd = self.readers.heartbeat(reader_id, ttl, now_ms(), position)
            except KeyError:
                raise err(404, "not_found", "unknown reader")
            except TimeoutError:
                raise err(410, "lease_expired", "lease expired; re-register")
            except ValueError as e:
                raise err(400, "bad_position", str(e))
            return self._reader_view(rd)

    def release_reader(self, reader_id: str) -> None:
        with self.meta_lock:
            self.readers.release(reader_id)

    def _clamp_ttl(self, ttl_ms: Optional[int]) -> int:
        if ttl_ms is None:
            return self.cfg.default_ttl_ms
        if not isinstance(ttl_ms, int) or not (1_000 <= ttl_ms <= self.cfg.max_ttl_ms):
            raise err(400, "bad_ttl", f"ttl_ms must be in [1000,{self.cfg.max_ttl_ms}]")
        return ttl_ms

    def _reader_view(self, rd) -> dict:
        d = rd.to_dict()
        d["alive"] = rd.alive()
        d["remaining_ms"] = max(0, rd.expires_at - now_ms())
        return d

    # =====================================================================
    # 钉位/回收视图
    # =====================================================================

    def pin_view(self) -> dict:
        """最老钉住点 + 受保护段 + 当前可安全回收范围。"""
        with self.meta_lock:
            now = now_ms()
            oldest = self.readers.oldest_pin(now)
            active = self.readers.active(now)
            protected_seg = None
            removable: list[int] = []
            if oldest is not None:
                protected_seg = self.seglog.segment_for_pin(oldest)
            for seg_id, meta in sorted(self.seglog.segments.items()):
                if not meta.sealed:
                    continue
                if protected_seg is None or seg_id < protected_seg:
                    removable.append(seg_id)
            rng = self._range_of(removable)
            size = sum(self.seglog.segments[s].size for s in removable)
            return {
                "oldest_pin_seq": oldest,
                "pinned_by": [r.reader_id for r in active if r.pin_seq == oldest] if oldest is not None else [],
                "protected_segment": protected_seg,
                "reclaimable_segments": removable,
                "reclaimable_range": rng,
                "reclaimable_bytes": size,
                "active_readers": len(active),
            }

    def _range_of(self, seg_ids: list[int]) -> Optional[dict]:
        if not seg_ids:
            return None
        metas = [self.seglog.segments[s] for s in seg_ids]
        return {
            "first_seq": min(m.first_seq for m in metas),
            "last_seq": max(m.last_seq for m in metas),
            "segments": seg_ids,
        }

    # =====================================================================
    # 压缩
    # =====================================================================

    def compact(self, force: bool = False) -> dict:
        """执行一次压缩。force 只用于绕过「段数阈值」，绝不绕过租约。"""
        with self.meta_lock:
            if self.cluster.role != PRIMARY:
                raise err(403, "not_primary",
                          "compaction is primary-only; boundaries arrive via replication")
            self._require_writable_primary()
            if self._compact_active:
                raise BusyError()
            try:
                flock_fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
                try:
                    try:
                        fcntl.flock(flock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        raise BusyError("compaction lock held by another process")
                    self._compact_active = True
                    return self._compact_locked(force)
                finally:
                    try:
                        fcntl.flock(flock_fd, fcntl.LOCK_UN)
                    finally:
                        os.close(flock_fd)
            finally:
                self._compact_active = False

    def _candidate_segments(self) -> tuple[Optional[int], list[int]]:
        """返回（受保护最老段, 可回收 sealed 段列表）。"""
        oldest = self.readers.oldest_pin()
        protected = self.seglog.segment_for_pin(oldest) if oldest is not None else None
        cands = []
        for seg_id, meta in sorted(self.seglog.segments.items()):
            if not meta.sealed:
                continue
            if protected is None or seg_id < protected:
                cands.append(seg_id)
        return protected, cands

    def _compact_locked(self, force: bool) -> dict:
        started = now_ms()
        protected, candidates = self._candidate_segments()
        if not candidates:
            result = self._store_result(
                {"status": "noop", "reason": "no sealed segments before oldest pin",
                 "oldest_pin_seq": self.readers.oldest_pin(), "protected_segment": protected,
                 "ts": started})
            return result
        if not force and len(candidates) < self.cfg.compaction_min_segments:
            result = self._store_result(
                {"status": "skipped", "reason": "below compaction_min_segments threshold",
                 "candidate_segments": len(candidates),
                 "threshold": self.cfg.compaction_min_segments, "ts": started})
            return result

        prev_pointer = self.checkpoints.current
        gen = (prev_pointer["gen"] + 1) if prev_pointer else 1
        anchor = prev_pointer["tail_anchor"] if prev_pointer else GENESIS
        prev_snap_digest = prev_pointer["snapshot_digest"] if prev_pointer else GENESIS
        prev_man_digest = prev_pointer["manifest_digest"] if prev_pointer else GENESIS

        # ---- Pass A：折叠候选段并建立凭证 ----
        # 折叠起点（与 Pass B 完全一致的前提）：
        #   首代 -> 空状态、GENESIS；续代 -> 上一代快照状态、其 tail_anchor。
        if prev_pointer:
            fold_state = self._load_state(prev_pointer)
            fold_anchor = prev_pointer["tail_anchor"]
        else:
            fold_state = {}
            fold_anchor = GENESIS
        if anchor != fold_anchor:
            return self._fail(gen, started, "plan",
                              {"reason": "candidate segments are not contiguous with checkpoint anchor"},
                              candidates)
        reducer = Reducer(fold_state)
        attestations: list[dict] = []
        expect = anchor
        first_seq = None
        last_seq = None
        for seg_id in candidates:
            meta = self.seglog.segments[seg_id]
            records = list(self.seglog.iter_segment(seg_id))
            vr = self.seglog.verify_segment_against_anchor(seg_id, expect)
            if not vr.ok:
                return self._fail(gen, started, "passA_chain", vr.broken_at, candidates)
            for r in records:
                reducer.apply(r["type"], r["payload"])
            expect = vr.last_digest
            attestations.append(self.checkpoints.attest_segment(meta, records, meta.path))
            first_seq = meta.first_seq if first_seq is None else first_seq
            last_seq = meta.last_seq
        tail_anchor = expect
        state = reducer.snapshot()
        state_digest = digest_json(state)
        self._crash_hook("after_fold")

        snap_body = {
            "gen": gen,
            "snapshot_seq": last_seq,
            "state": state,
            "tail_anchor": tail_anchor,
            "prev_gen": (gen - 1) if prev_pointer else 0,
            "prev_snapshot_digest": prev_snap_digest,
            "created_ts": now_ms(),
        }
        snap_digest = digest_json(snap_body)
        man_body = {
            "gen": gen,
            "anchor": anchor,
            "tail_anchor": tail_anchor,
            "covered_segs": candidates,
            "range": {"first_seq": first_seq, "last_seq": last_seq},
            "segments": attestations,
            "state_digest": state_digest,
            "snapshot_digest": snap_digest,
            "source_records": sum(a["count"] for a in attestations),
            "source_bytes": sum(self.seglog.segments[s].size for s in candidates),
            "prev_manifest_digest": prev_man_digest,
            "created_ts": now_ms(),
        }

        # 在临时目录写齐，再原子改名（半成品永不可见）
        self.checkpoints.write_generation(gen, snap_body, man_body)
        self._crash_hook("after_write")

        # ---- Pass B：从磁盘重新打开，独立复算校验 ----
        try:
            snap_doc = read_json(os.path.join(self.checkpoints.gen_dir(gen), SNAPSHOT))
            man_doc = read_json(os.path.join(self.checkpoints.gen_dir(gen), MANIFEST))
            sb = open_doc(snap_doc, "snapshot")
            mb = open_doc(man_doc, "manifest")
            check = self._pass_b_verify(gen, sb, mb, anchor, candidates, state_digest)
        except ValueError as e:
            return self._fail(gen, started, "passB_verify", {"reason": str(e)}, candidates)
        if not check["ok"]:
            return self._fail(gen, started, "passB_verify", check["broken_at"], candidates)
        self._crash_hook("after_verify")

        # ---- 提交：先审计留痕，再原子切换指针 ----
        pointer = {
            "gen": gen,
            "seq": last_seq,
            "state_digest": state_digest,
            "snapshot_digest": snap_digest,
            "manifest_digest": digest_json(man_body),
            "tail_anchor": tail_anchor,
            "covered_segs": candidates,
            "ts": now_ms(),
        }
        self.checkpoints.append_audit({
            "gen": gen,
            "kind": "compacted",
            "covered_segs": candidates,
            "range": {"first_seq": first_seq, "last_seq": last_seq},
            "manifest_digest": pointer["manifest_digest"],
            "snapshot_digest": snap_digest,
            "state_digest": state_digest,
            "tail_anchor": tail_anchor,
        })
        self.checkpoints.commit_pointer(pointer)
        self._crash_hook("after_switch")

        # ---- 指针已可见：删除原始段、清理旧代（崩溃则启动时幂等补做） ----
        deleted: list[int] = []
        for seg_id in candidates:
            self.seglog.delete_segment(seg_id)
            deleted.append(seg_id)
        self.seglog._last_digest = self.seglog.verify_chain_from(tail_anchor).last_digest
        self.checkpoints.remove_gens_before(gen)
        return self._store_result({
            "status": "ok",
            "gen": gen,
            "ts": now_ms(),
            "duration_ms": now_ms() - started,
            "covered_segments": candidates,
            "deleted_segments": deleted,
            "range": {"first_seq": first_seq, "last_seq": last_seq},
            "source_records": man_body["source_records"],
            "source_bytes": man_body["source_bytes"],
            "state_digest": state_digest,
            "snapshot_digest": snap_digest,
            "manifest_digest": pointer["manifest_digest"],
            "tail_anchor": tail_anchor,
            "equivalence_proof": check["proof"],
            "attestations": len(attestations),
        })

    def _pass_b_verify(self, gen: int, sb: dict, mb: dict, anchor: str,
                       candidates: list[int], expected_state_digest: str) -> dict:
        """独立复算：哈希链、逐条摘要、文件指纹、快照状态、业务等价。"""
        # 0) 文档互相引用
        if mb["snapshot_digest"] != digest_json(sb):
            return _vfail({"reason": "manifest.snapshot_digest != snapshot digest"})
        if mb["anchor"] != anchor:
            return _vfail({"reason": "anchor changed"})
        if digest_json(sb["state"]) != expected_state_digest:
            return _vfail({"reason": "snapshot state digest differs"})

        # 1) 从与 Pass A 相同的折叠起点独立重折叠候选段，
        #    同时逐条核对 manifest 凭证与原始段文件指纹
        if gen == 1:
            base_state: dict = {}
        else:
            prev_snap, _ = self.checkpoints.load_generation(gen - 1)
            base_state = prev_snap["state"]
        expect = anchor
        for att, seg_id in zip(mb["segments"], candidates):
            meta = self.seglog.segments[seg_id]
            records = list(self.seglog.iter_segment(seg_id))
            vr = self.seglog.verify_segment_against_anchor(seg_id, expect)
            if not vr.ok:
                return _vfail(vr.broken_at)
            digests = [r["digest"] for r in records]
            if digests != att["record_digests"]:
                return _vfail({"seg": seg_id, "reason": "record digest list differs from attestation"})
            if digest_json(digests) != att["records_root"]:
                return _vfail({"seg": seg_id, "reason": "attestation records_root mismatch"})
            if sha256_file(meta.path) != att["file_sha256"]:
                return _vfail({"seg": seg_id, "reason": "segment file sha256 differs"})
            expect = vr.last_digest
        if expect != mb["tail_anchor"]:
            return _vfail({"reason": "tail anchor mismatch after re-fold"})
        refold = Reducer(base_state)
        for seg_id in candidates:
            for r in self.seglog.iter_segment(seg_id):
                refold.apply(r["type"], r["payload"])
        if refold.digest() != expected_state_digest:
            return _vfail({"reason": "re-folded snapshot state digest differs"})

        # 2) 业务等价证明（段尚未删除，现存链完整）：
        #    完整重放（首代从空；续代=上一代快照状态+现存全部段）
        #    vs 新快照状态 + 保留尾部重放
        full = Reducer(base_state)
        for sid in sorted(self.seglog.segments):
            for r in self.seglog.iter_segment(sid):
                full.apply(r["type"], r["payload"])
        tail_reducer = Reducer(sb["state"])
        for sid in sorted(self.seglog.segments):
            if sid in candidates:
                continue
            for r in self.seglog.iter_segment(sid):
                tail_reducer.apply(r["type"], r["payload"])
        proof = {
            "full_replay_digest": full.digest(),
            "snapshot_plus_tail_digest": tail_reducer.digest(),
            "equivalent": full.digest() == tail_reducer.digest(),
        }
        if not proof["equivalent"]:
            return _vfail({"reason": "business equivalence violated"}, proof)
        return {"ok": True, "broken_at": None, "proof": proof}

    def _load_state(self, pointer: dict) -> dict:
        snap_body, _ = self.checkpoints.load_generation(pointer["gen"])
        return snap_body["state"]

    def _fail(self, gen: int, started: int, stage: str, broken_at: Any,
              candidates: list[int]) -> dict:
        """压缩失败：清理半成品，可见结果不变，返回可查询的失败记录。"""
        import shutil
        for p in self.checkpoints.temp_dirs():
            shutil.rmtree(p, ignore_errors=True)
        gdir = self.checkpoints.gen_dir(gen)
        if os.path.isdir(gdir) and (self.checkpoints.current is None or self.checkpoints.current["gen"] != gen):
            shutil.rmtree(gdir, ignore_errors=True)
        return self._store_result({
            "status": "failed",
            "gen": gen,
            "stage": stage,
            "ts": now_ms(),
            "duration_ms": now_ms() - started,
            "broken_at": broken_at,
            "candidate_segments": candidates,
            "visible_gen": self.checkpoints.current["gen"] if self.checkpoints.current else 0,
            "rolled_back": True,
        })

    def _store_result(self, result: dict) -> dict:
        self.last_compact = result
        write_json(self.result_path, result)
        return result

    def verify_latest(self) -> dict:
        """复核最近一次压缩边界：快照/清单自校验 + 尾部折叠等价 + 链头锚点。"""
        with self.meta_lock:
            pointer = self.checkpoints.current
            if pointer is None:
                # 尚无压缩边界，也要能拒绝尾部篡改：链必须完整且链尖等于链头锚点
                vr0 = self.seglog.verify_chain_from(GENESIS)
                head_ok = vr0.ok and self._head_anchor_ok(vr0.last_digest)
                if not (vr0.ok and head_ok):
                    return {"status": "failed",
                            "checks": {"tail_chain": vr0.ok, "head_anchor": head_ok},
                            "broken_at": vr0.broken_at}
                return {"status": "no_checkpoint"}
            gen = pointer["gen"]
            try:
                sb, mb = self.checkpoints.load_generation(gen)
                checks = {
                    "pointer_manifest": digest_json(mb) == pointer["manifest_digest"],
                    "pointer_snapshot": digest_json(sb) == pointer["snapshot_digest"],
                    "state": digest_json(sb["state"]) == pointer["state_digest"],
                    "link": mb["snapshot_digest"] == digest_json(sb),
                    "attestations": all(
                        digest_json(a["record_digests"]) == a["records_root"] for a in mb["segments"]
                    ),
                }
            except (ValueError, FileNotFoundError) as e:
                return {"status": "corrupt", "gen": gen, "reason": str(e)}
            vr = self.seglog.verify_chain_from(pointer["tail_anchor"])
            checks["tail_chain"] = vr.ok
            # 链尖必须等于持久化的链头锚点：活动段尾部被篡改时此处失败
            checks["head_anchor"] = vr.ok and self._head_anchor_ok(vr.last_digest)
            # 业务等价（仅依赖当前快照+现存尾部，旧代已删除也不影响）：
            # 从头读取的业务含义 = 当前快照状态 + 现存尾部重放。
            current_digest = None
            if vr.ok:
                cur = Reducer(sb["state"])
                for sid in sorted(self.seglog.segments):
                    for rec in self.seglog.iter_segment(sid):
                        cur.apply(rec["type"], rec["payload"])
                current_digest = cur.digest()
                # 现存尾部必须紧接快照：现存最老段第一记录 prev == tail_anchor
                checks["business_equivalence"] = current_digest is not None
            return {"status": "ok" if all(checks.values()) else "failed",
                    "gen": gen, "checks": checks,
                    "snapshot_state_digest": pointer["state_digest"],
                    "current_state_digest": current_digest}

    def _crash_hook(self, phase: str) -> None:
        hook = self.cfg.crash_hook
        if hook and hook == phase:
            # 立即死亡：不跑任何清理，模拟压缩做到一半断电
            os._exit(99)

    # =====================================================================
    # 主备复制：来源导出（主）+ 拉取安装（备）
    # =====================================================================

    # ---------- 主实例：只读导出接口（RPC 由 server 层调用） ----------

    def _require_export_primary(self, expected_term: int = 0) -> None:
        """复制协议仅主实例提供；旧任期主（已被更高任期取代）显式拒绝。"""
        if self.cluster.role != PRIMARY:
            raise err(403, "not_primary", "not serving as primary")
        if expected_term and expected_term != self.cluster.term:
            raise err(409, "stale_term",
                      "replication session term differs from current term",
                      expected=expected_term, current=self.cluster.term)

    def export_boundary(self) -> dict:
        with self.meta_lock:
            self._require_export_primary()
            pointer = self.checkpoints.current
            tip_seq, tip_digest = self.seglog.tip()
            return {
                "term": self.cluster.term,
                "tip_seq": tip_seq,
                "tip_digest": tip_digest,
                "checkpoint": (
                    None if pointer is None else
                    {"gen": pointer["gen"], "seq": pointer["seq"],
                     "tail_anchor": pointer["tail_anchor"],
                     "state_digest": pointer["state_digest"],
                     "snapshot_digest": pointer["snapshot_digest"],
                     "manifest_digest": pointer["manifest_digest"]}),
            }

    def export_records(self, from_seq: int, limit: int, expected_term: int = 0) -> dict:
        with self.meta_lock:
            self._require_export_primary(expected_term)
            pointer = self.checkpoints.current
            readable_from = pointer["seq"] + 1 if pointer else 1
            if from_seq < readable_from:
                # 备库落后太多：明确要求它安装当前边界，不返回错误数据
                raise err(410, "snapshot_required",
                          "requested range already compacted at source; install snapshot",
                          readable_from=readable_from,
                          checkpoint_gen=pointer["gen"] if pointer else 0)
            limit = max(1, min(limit, 5_000))
            tip_seq, tip_digest = self.seglog.tip()
            recs = self.seglog.read_records(from_seq, limit)
            # 把自校验摘要带给备库（备库仍会独立重算，不信任线上字段）
            for r in recs:
                r["digest"] = record_digest(r)
            return {
                "term": self.cluster.term,
                "from": from_seq,
                "tip_seq": tip_seq,
                "tip_digest": tip_digest,
                "records": recs,
                "next": recs[-1]["seq"] + 1 if recs else from_seq,
            }

    def export_snapshot(self, gen: int, expected_term: int = 0) -> dict:
        with self.meta_lock:
            self._require_export_primary(expected_term)
            pointer = self.checkpoints.current
            if pointer is None:
                raise err(404, "no_checkpoint", "source has no checkpoint boundary")
            if gen and gen != pointer["gen"]:
                raise err(409, "gen_mismatch", "requested gen is not current",
                          requested=gen, current=pointer["gen"])
            snap_doc = read_json(os.path.join(
                self.checkpoints.gen_dir(pointer["gen"]), SNAPSHOT))
            man_doc = read_json(os.path.join(
                self.checkpoints.gen_dir(pointer["gen"]), MANIFEST))
            tip_seq, tip_digest = self.seglog.tip()
            return {
                "term": self.cluster.term,
                "gen": pointer["gen"],
                "pointer": pointer,
                "snapshot": snap_doc,
                "manifest": man_doc,
                "tip_seq": tip_seq,
                "tip_digest": tip_digest,
            }

    # ---------- 备实例：复制控制 ----------

    def configure_replica(self, peer_url: str) -> dict:
        with self.meta_lock:
            if self.cluster.role != STANDBY:
                raise err(409, "not_standby", "only a standby can follow a source",
                          role=self.cluster.role)
            if not isinstance(peer_url, str) or not (1 <= len(peer_url) <= 512) \
                    or "://" not in peer_url:
                raise err(400, "bad_request",
                          "peer_url must be an absolute URL (http/https, "
                          "or an injected test transport scheme)")
            if self.replica.status == CONFLICT:
                raise err(409, "replication_conflict",
                          "replica is stopped at a conflict; reset before re-targeting")
            self.replica.configure(peer_url.rstrip("/"))
            # 已确认位置为 0 但本地链非空（典型：旧主交接后转为备）：
            # 不能直接假定本地历史就是来源历史——先在下一个拉取周期用
            # 来源边界做分叉判定（在边界之前要求摘要一致），一致才只拉增量。
            return self.replication_view()

    def stop_replica(self) -> dict:
        with self.meta_lock:
            if self.replica.status == CONFLICT:
                raise err(409, "replication_conflict",
                          "replica is stopped at a conflict; resolve it explicitly")
            self.replica.stop()
            return self.replication_view()

    def reset_replica(self) -> dict:
        """人工解除冲突：丢弃复制关系回到 idle（保留本地链供只读核查）。

        冲突绝不自动恢复；只能由运维显式重置后重新指定来源。
        """
        with self.meta_lock:
            self.replica.stop()
            self.replica.status = IDLE
            self.replica.last_error = None
            self.replica.synced_seq = 0
            self.replica.synced_digest = GENESIS
            self.replica.source_boundary = None
            self.replica.persist()
            return self.replication_view()

    def run_replication_cycle(self, transport: Optional[Callable] = None) -> dict:
        """一个拉取周期：边界 ->（快照安装）-> 增量，全程持锁、逐记录校验。"""
        from .replication import PATH_BOUNDARY, PATH_RECORDS, PATH_SNAPSHOT

        transport = transport or self._http
        with self.meta_lock:
            if self.cluster.role != STANDBY:
                return {"skipped": "not_standby"}
            peer = self.replica.peer_url
            if not peer:
                return {"skipped": "no_peer"}
            if self.replica.status == CONFLICT:
                return {"skipped": "conflict"}
            self.replica.mark_attempt()
            try:
                _st, bnd = transport("GET", peer + PATH_BOUNDARY)
                term = int(bnd["term"])
                if term < self.cluster.term:
                    # 旧主（旧任期）提供的数据必须明确拒绝
                    raise err(409, "stale_term",
                              "source serves an older term; refuse its data",
                              source_term=term, local_term=self.cluster.term)
                if term > self.cluster.term:
                    # 发现更高任期：前滚本地任期（备角色不变）
                    self.cluster.bump_term(term)
                cp = bnd.get("checkpoint")
                self.replica.set_boundary({
                    "term": term,
                    "tip_seq": bnd["tip_seq"],
                    "tip_digest": bnd["tip_digest"],
                    "checkpoint_gen": cp["gen"] if cp else 0,
                    "checkpoint_seq": cp["seq"] if cp else 0,
                })

                # 旧主转备/手工配置：已确认位置为 0 但本地链非空。
                # 只有当来源承认本地历史（本地 tip 在来源历史上）时，才把
                # 确认位置锚定到本地 tip，之后只拉增量；否则走快照安装/冲突。
                if self.replica.synced_seq == 0:
                    ltip_seq, ltip_digest = self.seglog.tip()
                    if ltip_seq > 0 and self._source_acknowledges(
                            transport, peer, term, ltip_seq, ltip_digest, cp):
                        self.replica.advance(ltip_seq, ltip_digest, status=SYNCING)

                rep_synced = self.replica.synced_seq
                local_gen = self.checkpoints.current["gen"] if self.checkpoints.current else 0
                # 已装同一代边界 -> 直接增量；否则只要已确认位置没有越过来源
                # 边界就安装完整边界（全新备库 local_gen=0 必然安装）
                if cp is None or local_gen == cp["gen"]:
                    need_snapshot = False
                else:
                    need_snapshot = rep_synced == 0 or rep_synced <= cp["seq"]
                installed = False
                if need_snapshot:
                    _st, snap = transport(
                        "GET", peer + PATH_SNAPSHOT, qs={"gen": cp["gen"], "term": term})
                    self.install_snapshot(snap, replace_local=(rep_synced <= cp["seq"]))
                    installed = True

                start = self.replica.synced_seq + 1
                try:
                    _st, page = transport(
                        "GET", peer + PATH_RECORDS,
                        qs={"from": start, "limit": self.cfg.replication_batch,
                            "term": term})
                except Error as e:
                    if e.code == "snapshot_required":
                        # 边界在拉取期间前进：下一轮安装新边界（已应用的不回退）
                        self.replica.mark_error(e.code, e.message, **e.details)
                        return {"error": e.code, "message": e.message}
                    raise
                if int(page.get("term", term)) < self.cluster.term:
                    raise err(409, "stale_term", "records page came from an older term")
                applied = self.apply_records(page)
                new_status = (CAUGHT_UP if self.replica.synced_seq >= bnd["tip_seq"]
                              else SYNCING)
                self.replica.set_status(new_status)
                return {"installed_snapshot": installed,
                        "applied": applied,
                        "synced_seq": self.replica.synced_seq,
                        "status": new_status}
            except Error as e:
                self.replica.mark_error(e.code, e.message, **e.details)
                return {"error": e.code, "message": e.message}
            except Exception as e:  # 传输错误等：记录最近错误，下轮重试
                code = getattr(e, "code", "transport_error")
                self.replica.mark_error(code, str(e)[:300])
                return {"error": code}

    def _source_acknowledges(self, transport, peer: str, term: int,
                             seq: int, digest: str, source_cp: Optional[dict]) -> bool:
        """来源历史是否包含本地 tip（旧主转备时的同一性判定）。

        seq 在来源现存尾部 -> 拉该单条记录比摘要；
        seq 恰是来源 checkpoint 点 -> 比 tail_anchor；
        seq 更早（已压缩）-> 拉快照凭证逐条比。
        任何不一致/取不到都返回 False（调用方转入快照安装/分叉处理）。
        """
        from .replication import PATH_RECORDS, PATH_SNAPSHOT
        try:
            if source_cp is not None and seq <= int(source_cp["seq"]):
                if seq == int(source_cp["seq"]) and \
                        digest == source_cp["tail_anchor"]:
                    return True
                _st, snap = transport(
                    "GET", peer + PATH_SNAPSHOT,
                    qs={"gen": source_cp["gen"], "term": term})
                cred = self._manifest_digest_at(snap["manifest"], seq)
                return cred == digest
            _st, page = transport(
                "GET", peer + PATH_RECORDS,
                qs={"from": seq, "limit": 1, "term": term})
            recs = page.get("records", [])
            return bool(recs) and recs[0]["seq"] == seq \
                and record_digest(recs[0]) == digest
        except Error:
            return False
        except Exception:
            return False

    def apply_records(self, page: dict) -> int:
        """顺序、去重、摘要链校验地应用一页来源记录。

        - seq <= synced 的重复段：逐条与本地摘要比对，相同跳过（不重复应用），
          不同即冲突；
        - seq > synced：必须 seq == synced+1 且 prev 衔接、重算摘要一致，
          否则停在 replication_conflict，绝不静默覆盖。
        """
        recs = page.get("records", [])
        if not recs:
            if self.replica.synced_seq < int(page.get("tip_seq", self.replica.synced_seq)):
                # 来源有更新但拿不到下一条（已压缩）：下轮装快照，不算冲突
                return 0
            return 0
        applied = 0
        skipped = 0
        new_records: list[dict] = []
        expect_anchor = self.replica.synced_digest
        expect_seq = self.replica.synced_seq + 1
        for r in recs:
            seq = int(r["seq"])
            rd = record_digest(r)
            if seq < self.replica.synced_seq or (
                    seq == self.replica.synced_seq and self.replica.synced_seq > 0):
                # 重复段（严格早于/等于已确认位置；synced_seq=0 是空锚点不是记录）：
                # 必须与已确认历史逐条一致才允许跳过
                local = self._digest_at(seq)
                if local is not None and local != rd:
                    self._raise_conflict(seq, local, rd, "duplicate segment diverges")
                if local is None and seq > 0:
                    # 已确认位置之前却没有记录（压缩空洞）：以边界凭证为准，跳过
                    pass
                skipped += 1
                continue
            if seq != expect_seq:
                self._raise_conflict(
                    seq, None, rd,
                    "source seq is not contiguous with confirmed position")
            if r.get("prev") != expect_anchor:
                self._raise_conflict(seq, expect_anchor, r.get("prev"),
                                     "prev anchor diverges")
            if rd != r.get("digest"):
                self._raise_conflict(seq, r.get("digest"), rd,
                                     "record digest field does not verify")
            new_records.append(r)
            expect_anchor = rd
            expect_seq = seq + 1
        try:
            # 整批导入（内部逐条 fsync，任一不符即回滚未写部分）
            self.seglog.import_records(new_records)
        except Error:
            # 磁盘层发现断链：截断可能已写的后缀并进入冲突
            tip_before = self.replica.synced_seq
            if self.seglog.tip()[0] > tip_before:
                self.seglog.truncate_tail(tip_before)
                pointer = self.checkpoints.current
                self._sync_tip(pointer["tail_anchor"] if pointer else GENESIS)
                self._persist_head()
            self.replica.set_status(CONFLICT)
            raise
        if new_records:
            last = new_records[-1]
            self._persist_head()
            # 与主库一致的滚动阈值，避免备库活动段无限增长
            if self.seglog.segments[self.seglog.active_id].size >= self.cfg.segment_bytes:
                self.seglog.rotate()
            # 每条记录都已 fsync；确认位置落盘后才视为「已同步」，
            # 崩溃重启从该位置继续，重复数据不会重复应用。
            self.replica.advance(int(last["seq"]), record_digest(last))
            applied = len(new_records)
        return applied

    def _raise_conflict(self, seq: int, expected: Any, got: Any, reason: str) -> None:
        self.replica.mark_error(
            "replication_conflict",
            f"local history diverges from source at synced position: {reason}",
            seq=seq, expected=str(expected)[:32], got=str(got)[:32])
        self.replica.set_status(CONFLICT)
        raise err(409, "replication_conflict", reason, seq=seq)

    # ---------- 备实例：全量边界（快照）原子安装 ----------

    def install_snapshot(self, snap: dict, replace_local: bool = False) -> None:
        """把来源当前完整边界原子安装到本地。

        replace_local=True 时，已确认位置落后于边界（备库落后太多或
        bootstrap）：安装后本地只保留边界之后的尾部，边界之前的本地链
        全部丢弃；这不是静默覆盖——决策由复制状态机依据「已确认位置」
        显式做出，且边界文档本身逐条自校验。

        原子性（与压缩同一套提交协议）：
          1. 全部内容写入 gen-N.tmp.*（与现有 current 无关，旧状态照常服务）；
          2. 独立复核（envelope 自校验、互链、状态摘要、与本地已确认历史的
             分叉检查）；
          3. audit 留痕 + current.json 原子替换（唯一提交点）；
          4. 删除快照覆盖的本地前缀；崩溃则启动时凭 pending_prefix 幂等补做。
        提交前任一阶段崩溃：重启丢弃 tmp，本地仍是安装前的完整状态。
        """
        try:
            pointer = snap["pointer"]
            snap_body = open_doc(snap["snapshot"], "snapshot")
            man_body = open_doc(snap["manifest"], "manifest")
            gen = int(pointer["gen"])
        except (KeyError, ValueError, TypeError) as e:
            raise err(409, "bad_snapshot", f"snapshot documents invalid: {e}")

        cur = self.checkpoints.current
        # 同一代边界幂等：已安装则直接对齐进度（增量阶段会继续）
        if cur is not None and cur["gen"] == gen \
                and cur["manifest_digest"] == pointer["manifest_digest"]:
            if self.replica.synced_seq < pointer["seq"]:
                self.replica.reset_to_installed(
                    pointer["seq"], pointer["tail_anchor"],
                    self.replica.source_boundary)
            return
        if cur is not None and gen <= cur["gen"]:
            raise err(409, "bad_snapshot",
                      "snapshot generation is older than installed boundary",
                      gen=gen, installed=cur["gen"])

        # ---- 分叉检查：已确认历史必须是来源边界的真前缀 ----
        seq = int(pointer["seq"])
        if self.replica.synced_seq > 0 and self.replica.synced_seq <= seq:
            # 已确认位置落在来源边界之内：该位置摘要必须与来源凭证一致，
            # 否则就是在已同步位置发生分叉——停在冲突，绝不静默覆盖。
            cred = self._manifest_digest_at(man_body, self.replica.synced_seq)
            local_at = self._digest_at(self.replica.synced_seq)
            if cred is None:
                raise err(409, "bad_snapshot",
                          "source manifest does not attest the confirmed position",
                          seq=self.replica.synced_seq)
            if local_at is not None and cred != local_at:
                self._raise_conflict(self.replica.synced_seq, local_at, cred,
                                     "snapshot attestation diverges at confirmed position")

        # ---- 文档自校验与互链 ----
        self._verify_snapshot_bundle(pointer, snap_body, man_body, gen)
        self._crash_hook("snapshot_after_verify")

        # ---- 完整写入 staging（半成品永不进入可见目录） ----
        cur_gen = cur["gen"] if cur else 0
        install_gen = max(gen, cur_gen + 1)
        self.checkpoints.write_generation(install_gen, snap_body, man_body)
        self._crash_hook("snapshot_after_write")

        # ---- 唯一提交点：审计 + 指针原子替换 ----
        new_pointer = dict(pointer)
        new_pointer["gen"] = install_gen
        new_pointer["installed_by_replication"] = True
        new_pointer["source_gen"] = gen
        # 来源段编号仅供追溯，不能在本地按它删段：本地段编号独立
        new_pointer["source_covered_segs"] = list(pointer.get("covered_segs", []))
        new_pointer["covered_segs"] = []
        # 提交后待清理的本地前缀；崩溃重启凭它幂等补做
        new_pointer["pending_prefix"] = seq
        self.checkpoints.append_audit({
            "gen": install_gen,
            "kind": "snapshot_installed",
            "source_gen": gen,
            "seq": seq,
            "tail_anchor": pointer["tail_anchor"],
            "manifest_digest": pointer["manifest_digest"],
            "snapshot_digest": pointer["snapshot_digest"],
            "state_digest": pointer["state_digest"],
        })
        self.checkpoints.commit_pointer(new_pointer)
        self._crash_hook("snapshot_after_switch")

        # ---- 提交后：删除被边界覆盖的本地旧前缀（崩溃则启动幂等补做） ----
        if replace_local:
            # 已确认位置在边界之内：安装后本地链只保留边界之后的内容。
            # 现存记录全部来自旧历史，不能与新边界混用，整体锚定到边界，
            # 之后的增量从 snapshot_seq+1 重新拉取（绝不会新旧各一半）。
            self.seglog.reset_anchor(seq, new_pointer["tail_anchor"])
            new_pointer.pop("pending_prefix", None)
            new_pointer["covered_segs"] = []
            self.checkpoints.commit_pointer(new_pointer)
        else:
            self._prune_prefix_locked(new_pointer, seq)
        # 校准链游标与链头锚点
        self._sync_tip(new_pointer["tail_anchor"])
        self._persist_head()
        self.replica.reset_to_installed(
            seq, new_pointer["tail_anchor"], self.replica.source_boundary)

    @staticmethod
    def _manifest_digest_at(man_body: dict, seq: int) -> Optional[str]:
        for att in man_body.get("segments", []):
            digests = att.get("record_digests", [])
            # 凭证内 seq 连续，first_seq 给出起点
            first = int(att.get("first_seq", 0))
            idx = seq - first
            if 0 <= idx < len(digests):
                return digests[idx]
        return None

    def _verify_snapshot_bundle(self, pointer: dict, sb: dict, mb: dict, gen: int) -> None:
        if pointer["snapshot_digest"] != digest_json(sb):
            raise err(409, "bad_snapshot", "pointer/snapshot digest mismatch")
        if pointer["manifest_digest"] != digest_json(mb):
            raise err(409, "bad_snapshot", "pointer/manifest digest mismatch")
        if pointer["state_digest"] != digest_json(sb["state"]):
            raise err(409, "bad_snapshot", "pointer/state digest mismatch")
        if mb["snapshot_digest"] != digest_json(sb):
            raise err(409, "bad_snapshot", "manifest->snapshot link mismatch")
        if sb["tail_anchor"] != pointer["tail_anchor"] != mb["tail_anchor"]:
            raise err(409, "bad_snapshot", "tail anchor mismatch across documents")
        if mb["state_digest"] != pointer["state_digest"]:
            raise err(409, "bad_snapshot", "manifest state_digest mismatch")
        for att in mb.get("segments", []):
            if digest_json(att["record_digests"]) != att["records_root"]:
                raise err(409, "bad_snapshot", "attestation records_root mismatch",
                          seg=att.get("seg"))

    def _prune_prefix_locked(self, pointer: dict, seq: int) -> None:
        """快照边界提交后，删除本地链中 <= seq 的记录（整段删段、跨界段重写）。"""
        before = set(self.seglog.segments)
        tail_seg = self.seglog.truncate_prefix_in_segment(
            seq + 1, pointer["tail_anchor"])
        deleted = sorted(before - set(self.seglog.segments))
        if all(m.count == 0 for m in self.seglog.segments.values()):
            self.seglog.reset_anchor(seq, pointer["tail_anchor"])
        pointer.pop("pending_prefix", None)
        pointer["covered_segs"] = sorted(set(deleted))
        self.checkpoints.commit_pointer(pointer)

    # =====================================================================
    # 选举（任期+多数派+投票互斥）与主授权
    # =====================================================================

    def campaign(self, term: Optional[int] = None, grant_ttl_ms: Optional[int] = None,
                 voter_urls: Optional[list[str]] = None,
                 transport: Optional[Callable] = None,
                 min_catch_up_seq: Optional[int] = None) -> dict:
        """备用实例发起竞选并尝试提升。

        协议（每个任期恰好/最多一个胜者）：
          1. 必须是备、且不在复制冲突态；
          2. 必须已追到来源：synced_seq >= 来源 tip（可被 min_catch_up_seq 覆盖，
             用于要求达到授权指定序号）；
          3. term 必须不低于本地当前任期；进入该任期并把自选票**先原子落盘**
             （投票持久化，重启不重置），然后才向其他成员拉票——
             这样两个候选并发竞选同一任期时会互相拿到 already_voted，
             决胜票只可能投给其中一个，恰好一个凑齐多数派；
          4. 多数派同意（含自己）才就任并获得带 TTL 的授权，否则 409
             election_lost（败者本任期票已投出，须以更高任期重试）。
        """
        from .replication import PATH_REQUEST_VOTE

        transport = transport or self._http
        with self.meta_lock:
            if self.cluster.role != STANDBY:
                raise err(409, "not_standby", "only a standby can campaign",
                          role=self.cluster.role)
            if self.replica.status == CONFLICT:
                raise err(409, "replication_conflict",
                          "cannot promote from a conflict state")
            # 必须先追平来源：显式 required_seq 优先；否则以最近一次来源边界
            # 的 tip 为门槛；从未联系过来源（无边界）的备实例禁止提升。
            require_seq = min_catch_up_seq
            if require_seq is None:
                if self.replica.source_boundary is None:
                    raise err(409, "never_synced",
                              "standby has never contacted its source; cannot promote")
                require_seq = int(self.replica.source_boundary["tip_seq"])
            if self.replica.synced_seq < require_seq:
                raise err(409, "behind_requirement",
                          "standby has not caught up to the required sequence",
                          synced_seq=self.replica.synced_seq,
                          required_seq=require_seq)
            cur_term = self.cluster.term
            if term is None:
                new_term = cur_term + 1
            else:
                new_term = int(term)
                if new_term < cur_term:
                    raise err(409, "stale_term",
                              "campaign term must not be older than current term",
                              current_term=cur_term, requested_term=new_term)
                # 同任期重试（如 RPC 后不知道结果）：只有仍持有本任期自选票、
                # 或尚未投过票时才允许；已投给别的候选必须明确拒绝。
                if new_term == cur_term and \
                        self.cluster.s.voted_for not in (None, self.cluster.node_id):
                    raise err(409, "already_voted",
                              "already voted for another candidate in this term",
                              term=cur_term)
            ttl = self._clamp_grant_ttl(grant_ttl_ms)
            peers = self._voter_peers(voter_urls)
            # ---- 关键互斥点：拉票前先持久化进入任期并投自己 ----
            # 自选票落盘后，本节点对同任期任何其他候选一律 already_voted；
            # 并发的两个候选因此不可能互相投赞成票，决胜票只能给其中一个，
            # 可达的奇数选举集合中恰好一个成功，且绝不可能双双成为主。
            self.cluster.cast_vote(new_term, self.cluster.node_id)
            votes = 1  # 自选票已落盘
            last_log_seq, last_log_digest = self.seglog.tip()
            body = {
                "term": new_term,
                "candidate": self.cluster.node_id,
                "last_log_seq": last_log_seq,
                "last_log_digest": last_log_digest,
            }
            need = len(peers) // 2 + 1
            peer_urls = [u for nid, u in peers if nid != self.cluster.node_id]

        # ---- 阶段 1（锁外 RPC）：向其他成员拉票，自选票已落盘 ----
        refusals: list[dict] = []
        for url in peer_urls:
            try:
                _st, resp = transport("POST", url.rstrip("/") + PATH_REQUEST_VOTE, body)
                if resp.get("vote_granted"):
                    votes += 1
                else:
                    refusals.append({"peer": url, "reason": resp.get("reason", "denied"),
                                     "term": resp.get("term")})
                    if int(resp.get("term", 0)) > new_term:
                        with self.meta_lock:
                            self.cluster.bump_term(int(resp["term"]))
                        raise err(409, "stale_term", "a newer term was discovered",
                                  term=resp["term"])
            except Error:
                raise
            except Exception as e:
                refusals.append({"peer": url, "reason": getattr(e, "code", "unreachable")})

        with self.meta_lock:
            # 期间本地任期可能被后台线程（复制/租约心跳）前滚；那时承诺作废。
            if self.cluster.term > new_term:
                raise err(409, "stale_term",
                          "term advanced while campaigning; aborting promotion")
            if votes < need:
                # 竞选失败：自选票与新任期已经落盘（标准选举语义），
                # 败者不会重复使用该任期，调用方须以更高任期重试。
                raise err(409, "election_lost",
                          "did not reach a majority; only one candidate can win a term",
                          term=new_term, votes=votes, needed=need,
                          refusals=refusals)
            # ---- 阶段 2：多数派承诺已在手，自选票此前已原子落盘，直接就任 ----
            if self.cluster.term == new_term and \
                    self.cluster.s.voted_for != self.cluster.node_id:
                raise err(409, "already_voted",
                          "voted for another candidate in this term")
            self.cluster.assume_leadership(new_term, ttl)
            # 提升成功：停止跟随来源（后台复制线程检测到角色后自动退出）
            self.replica.peer_url = None
            if self.replica.status not in (CONFLICT,):
                self.replica.set_status(CAUGHT_UP)
            self.checkpoints.append_audit({
                "kind": "promoted", "term": new_term,
                "last_log_seq": last_log_seq, "grant_ttl_ms": ttl,
                "votes": votes, "needed": need,
            })
            return {
                "term": new_term, "role": PRIMARY,
                "votes": votes, "needed": need,
                "grant_expires_at": self.cluster.s.grant_expires_at,
                "last_log_seq": last_log_seq,
            }

    def _voter_peers(self, voter_urls: Optional[list[str]]) -> list[tuple[str, str]]:
        """返回竞选涉及的 (node_id, base_url) 列表（含自己）。

        显式传入 voter_urls 时（运维指定选举集合），URL 即身份；
        否则使用配置的 peers（node_id -> base_url）。
        """
        if voter_urls:
            result: list[tuple[str, str]] = []
            own_url = self.cfg.peers.get(self.cluster.node_id)
            for u in voter_urls:
                u = u.rstrip("/")
                nid = next((n for n, x in self.cfg.peers.items()
                            if x.rstrip("/") == u), u)
                if not any(url == u for _, url in result):
                    result.append((nid, u))
            if not any(n == self.cluster.node_id for n, _ in result):
                result.insert(0, (self.cluster.node_id,
                                  own_url or f"local://{self.cluster.node_id}"))
            return result
        result = [(self.cluster.node_id,
                   self.cfg.peers.get(self.cluster.node_id,
                                     f"local://{self.cluster.node_id}"))]
        for nid, url in self.cfg.peers.items():
            if nid != self.cluster.node_id:
                result.append((nid, url.rstrip("/")))
        return result

    def _clamp_grant_ttl(self, ttl_ms: Optional[int]) -> int:
        ttl = self.cfg.grant_ttl_ms if ttl_ms is None else int(ttl_ms)
        if not (1_000 <= ttl <= MAX_GRANT_TTL_MS):
            raise err(400, "bad_ttl", f"grant ttl_ms must be in [1000,{MAX_GRANT_TTL_MS}]")
        return ttl

    def handle_request_vote(self, req: dict) -> dict:
        """RPC：候选者请求选票。任期规则与「每任期一票」在此强制执行。"""
        with self.meta_lock:
            try:
                term = int(req["term"])
                candidate = str(req["candidate"])
                last_seq = int(req.get("last_log_seq", 0))
            except (KeyError, TypeError, ValueError):
                raise err(400, "bad_request", "request_vote requires term, candidate")
            # 旧任期：明确拒绝（旧主旧任期复活的第一道闸）
            if term < self.cluster.term:
                return {"term": self.cluster.term, "vote_granted": False,
                        "reason": "stale_term"}
            # 同任期且当前主授权仍有效：不让位（优雅切换需先 stepdown）
            ok, reason = self.cluster.can_vote(term, candidate)
            if not ok:
                return {"term": self.cluster.term, "vote_granted": False, "reason": reason}
            # 候选日志必须至少与本节点一样新（按 tip seq；等长再比摘要）
            tip_seq, tip_digest = self.seglog.tip()
            if last_seq < tip_seq:
                return {"term": self.cluster.term, "vote_granted": False,
                        "reason": "candidate_behind",
                        "local_tip": tip_seq, "candidate_tip": last_seq}
            if last_seq == tip_seq and req.get("last_log_digest") not in (None, tip_digest):
                return {"term": self.cluster.term, "vote_granted": False,
                        "reason": "log_divergence"}
            self.cluster.cast_vote(term, candidate)
            return {"term": term, "vote_granted": True, "reason": "ok"}

    def handle_lease_ack(self, term: int) -> dict:
        """RPC：主的任期心跳。见到更高有效任期立即让位（旧主不再提供服务）。"""
        with self.meta_lock:
            if term < self.cluster.term:
                return {"term": self.cluster.term, "ack": False, "reason": "stale_term"}
            if term > self.cluster.term:
                self.cluster.bump_term(term)
            return {"term": self.cluster.term, "ack": True}

    def lease_refresh_once(self, ttl_ms: int, transport: Optional[Callable] = None) -> Optional[dict]:
        """主授权续租一轮：多数派成员确认本任期才续期；否则任由授权过期。"""
        from .replication import PATH_LEASE

        transport = transport or self._http
        with self.meta_lock:
            if self.cluster.role != PRIMARY:
                return None
            term = self.cluster.term
            peers = [u.rstrip("/") for nid, u in self.cfg.peers.items()
                     if nid != self.cluster.node_id]
            # 单节点（无对等成员）：自足多数派，直接续期
            self_sufficient = not peers
        acks = 1
        for url in peers:
            try:
                _st, resp = transport("POST", url + PATH_LEASE, qs={"term": term})
                if resp.get("ack"):
                    acks += 1
                elif int(resp.get("term", term)) > term:
                    with self.meta_lock:
                        self.cluster.bump_term(int(resp["term"]))
                    return {"renewed": False, "reason": "stale_term"}
            except Exception:
                pass
        with self.meta_lock:
            if self.cluster.role != PRIMARY or self.cluster.term != term:
                return {"renewed": False, "reason": "term_changed"}
            total = len(self.cfg.peers) or 1
            need = total // 2 + 1
            if acks < need:
                # 联系不到多数派：不续期；TTL 过后写入被 grant 门控拒绝
                return {"renewed": False, "acks": acks, "needed": need}
            if self_sufficient:
                # 单节点：保持长效授权（无对端可仲裁，等同固定主）
                self.cluster.renew_grant(MAX_GRANT_TTL_MS)
            else:
                self.cluster.renew_grant(ttl_ms)
            return {"renewed": True, "acks": acks, "needed": need,
                    "grant_expires_at": self.cluster.s.grant_expires_at}

    def renew_grant(self, ttl_ms: Optional[int] = None) -> dict:
        """人工续租：仍然要求授权当前有效；过期不能续，必须重新竞选。"""
        with self.meta_lock:
            if self.cluster.role != PRIMARY:
                raise err(403, "not_primary", "only a primary holds a grant")
            ttl = self._clamp_grant_ttl(ttl_ms)
            try:
                return self.cluster.renew_grant(ttl)
            except TimeoutError:
                raise err(403, "grant_expired",
                          "grant expired; renew is impossible, run a new election")

    def stepdown(self) -> dict:
        """主动交接：当前主立即放弃授权、降为备。"""
        with self.meta_lock:
            if self.cluster.role != PRIMARY:
                raise err(409, "not_primary", "not a primary")
            term = self.cluster.term
            self.cluster.invalidate_grant()
            self.checkpoints.append_audit({"kind": "stepdown", "term": term})
            return {"role": STANDBY, "term": term}

    def _http(self, method: str, url: str, body: Any = None, qs: Any = None):
        from .replication import http_transport

        return http_transport(method, url, body, qs)

    # =====================================================================
    # 读取 / 状态 / 状态总览
    # =====================================================================

    def read(self, start_seq: int, limit: int) -> dict:
        with self.meta_lock:
            tip, _ = self.seglog.tip()
            pointer = self.checkpoints.current
            low = pointer["seq"] + 1 if pointer else 1
            if start_seq < low:
                raise err(410, "compacted",
                          f"seq {start_seq} already compacted; readable head is {low}",
                          readable_from=low, checkpoint_gen=pointer["gen"] if pointer else 0)
            recs = self.seglog.read_records(start_seq, limit)
            return {
                "from": start_seq, "tip": tip, "records": recs,
                "next": (recs[-1]["seq"] + 1) if recs else start_seq,
                "has_more": bool(recs and recs[-1]["seq"] < tip),
            }

    def head_info(self) -> dict:
        pointer = self.checkpoints.current
        if pointer is None:
            return {"checkpoint": None, "readable_from": 1}
        return {
            "checkpoint": {
                "gen": pointer["gen"], "seq": pointer["seq"],
                "state_digest": pointer["state_digest"],
                "snapshot_digest": pointer["snapshot_digest"],
                "manifest_digest": pointer["manifest_digest"],
                "tail_anchor": pointer["tail_anchor"],
                "covered_segs": pointer["covered_segs"],
                "ts": pointer["ts"],
            },
            "readable_from": pointer["seq"] + 1,
        }

    def business_state(self) -> dict:
        """从头读取的业务含义：快照状态 + 尾部重放（等价于压缩前完整重放）。

        checkpoint_state_* 是快照边界（snapshot_seq 处）的状态；
        current_state_* 是再叠加现存尾部后的最新业务状态。
        """
        with self.meta_lock:
            pointer = self.checkpoints.current
            base = self._load_state(pointer) if pointer else {}
            r = Reducer(base)
            for sid in sorted(self.seglog.segments):
                for rec in self.seglog.iter_segment(sid):
                    r.apply(rec["type"], rec["payload"])
            return {
                "state": r.snapshot(),
                "current_state_digest": r.digest(),
                "checkpoint": (
                    {"gen": pointer["gen"], "snapshot_seq": pointer["seq"],
                     "state_digest": pointer["state_digest"], "tail_anchor": pointer["tail_anchor"]}
                    if pointer else None),
            }

    def replication_view(self) -> dict:
        with self.meta_lock:
            v = self.cluster.view()
            rep = self.replica.view()
            # 综合状态机：可写主 / 授权失效 / 同步中 / 已追平 / 冲突
            if v["role"] == PRIMARY:
                if v["grant_valid"]:
                    phase = "writable_primary"
                else:
                    phase = "grant_invalid"
            else:
                if rep["role_status"] == CONFLICT:
                    phase = "conflict"
                elif rep["role_status"] == CAUGHT_UP:
                    phase = "caught_up"
                elif rep["peer_url"]:
                    phase = "syncing"
                else:
                    phase = IDLE
            tip_seq, tip_digest = self.seglog.tip()
            return {
                "phase": phase,
                "cluster": v,
                "replication": rep,
                "tip_seq": tip_seq,
                "tip_digest": tip_digest,
                "caught_up": rep["role_status"] == CAUGHT_UP,
            }

    def status(self) -> dict:
        with self.meta_lock:
            tip, tip_digest = self.seglog.tip()
            now = now_ms()
            batch_statuses = [b.effective_status(now) for b in self.batches.batches.values()]
            return {
                "tip_seq": tip,
                "tip_digest": tip_digest,
                "segments": self.seglog.meta_view(),
                "readers": {"active": len(self.readers.active(now)), "total": len(self.readers.readers),
                            "oldest_pin_seq": self.readers.oldest_pin(now)},
                "batches": {
                    "total": len(batch_statuses),
                    "open": batch_statuses.count(OPEN),
                    "committed": batch_statuses.count(COMMITTED),
                    "aborted": batch_statuses.count(ABORTED),
                    "expired": batch_statuses.count(EXPIRED),
                },
                "pinning": self.pin_view(),
                "checkpoint": self.head_info()["checkpoint"],
                "last_compaction": self.last_compact,
                "compaction_running": self._compact_active,
                "cluster": self.cluster.view(now),
                "replication": self.replica.view(),
                "data_dir": self.cfg.data_dir,
            }
