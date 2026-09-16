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
from contextlib import contextmanager
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
from .consensus import CommitStore, Proposal, new_write_id
from .readers import ReaderStore
from .reducer import Reducer
from .replica import CAUGHT_UP, CONFLICT, IDLE, SYNCING, ReplicaStore
from .schedules import (
    ACTIVE,
    APPLIED,
    CANCELLED,
    EXECUTING,
    PENDING,
    SUPERSEDED,
    ScheduleStore,
    exec_batch_id,
    exec_write_id,
    schedule_content_hash,
)
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
    commit_timeout_ms: int = 3_000      # 多数派提交等待上限（超时返回 commit_timeout）
    read_barrier_timeout_ms: int = 3_000  # 线性一致读屏障等待上限
    # ---- 未来生效变更预约（scheduled changes）----
    scheduler_enabled: bool = True      # 到期领取后台线程（仅主实例实际领取）
    scheduler_interval_ms: int = 200    # 调度轮询间隔
    catchup_batch_limit: int = 100      # 单轮补跑/单轮到期处理上限（超出留下一轮）


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
        # ---- 多数派提交水位 / 待定提议 / write_id 登记 / 成员确认位置 ----
        self.commit = CommitStore(os.path.join(self.state_dir, "commit.json"))
        # ---- 未来生效变更预约 ----
        self.schedules = ScheduleStore(os.path.join(self.state_dir, "schedules.json"))
        # 可注入的墙上时钟（测试时钟回拨/停机补跑）；缺省走真实时间。
        self._clock: Optional[Callable[[], int]] = None
        # schedule 执行记录跨度：{(first_seq,last_seq): request_id}，
        # 供重启后重建待定提议时把整组识别为一个不可拆开的 schedule 提议。
        self._schedule_spans: dict[tuple[int, int], str] = {}
        for sc in self.schedules.schedules.values():
            if sc.status in ACTIVE and sc.first_seq is not None and sc.last_seq is not None:
                self._schedule_spans[(int(sc.first_seq), int(sc.last_seq))] = sc.request_id
        if self.cluster.fresh and cfg.bootstrap_role == STANDBY and cfg.replica_source:
            self.replica.peer_url = cfg.replica_source.rstrip("/")
            self.replica.status = SYNCING
            self.replica.persist()

        self.meta_lock = threading.RLock()
        # 调度循环在每个预约之间释放 meta_lock。已到达 append 入口的写请求
        # 登记在这里，调度器会等它们至少拿到一次 meta_lock 后再继续，避免
        # RLock 非公平导致调度线程刚释放又立刻抢到锁、写请求被整轮饿死。
        self._scheduler_lock = threading.Lock()
        self._append_admit_cv = threading.Condition()
        self._append_waiters: set[object] = set()
        # 提交事件由处理 ack 的线程持独立条件锁广播；等待提交的写线程
        # 先在 meta_lock 内检查状态、再在此条件上释放锁等待，被唤醒后
        # 重新进入 meta_lock 复核，避免「提交发生在检查与等待之间」的窗口。
        self._commit_cv = threading.Condition()
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

            # 批次崩溃恢复：committing 批次只能恢复成整批已提交、整批未提交，
            # 或「本地已整批持久化、等待多数确认」的待定状态（绝不暴露半批）；
            # 截断必须在链头锚点核对之前完成。
            batch_rec = self._recover_batches()
            if batch_rec:
                recovery["batches"] = batch_rec
            self._sync_tip(anchor)
            if batch_rec.get("rolled_back"):
                # 链尾被回滚截断：以新链尖重写链头锚点后再核对
                self._persist_head()
            self._reconcile_head()
            recovery["replica"] = self._reconcile_replica()
            recovery["commit"] = self._reconcile_commit()
            recovery["schedules"] = self._recover_schedules()
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
        """清算 committing 状态的批次。

        - 幂等登记已存在（提交点已过）：补记为整批已提交；
        - 批次记录完整留在链上、但提交点未到：多数派提交语义下仍可能在
          等待确认（主在本地持久化后崩溃），保留为 committing，提交水位
          重建后继续等待多数派（见 _reconcile_commit），不截断、不回退；
        - 批次记录没有完整落盘（崩溃在追加途中）：整批未提交，截断链尾，
          批次回到 open 可在有效期内重试，不留序号空洞。
        """
        finalized: list[str] = []
        rolled_back: list[str] = []
        pending: list[str] = []
        for b in self.batches.committing_batches():
            entry = self.batches.commit_entry(b.registry_key())
            first = b.first_seq or 1
            present = self._batch_records_present(b, first)
            if entry is not None and entry.get("content_hash") == b.content_hash \
                    and present and int(entry["last_seq"]) <= self.commit.commit_index:
                # 幂等登记已落盘且整批已进入提交水位：整批已提交，补记状态
                self.batches.mark_committed(b, entry, replay=False)
                finalized.append(b.batch_id)
                continue
            if present:
                # 整批已在本地链上 fsync，但尚未进入提交水位（崩溃在多数派
                # 确认之前）：保留 committing，由 _reconcile_commit 重建为
                # 待定提议；主重启后续等多数派，绝不静默当作已提交。
                b.last_seq = first + len(b.ops) - 1
                b.record_count = len(b.ops)
                pending.append(b.batch_id)
                continue
            # 整批未完成：批次记录必是链尾后缀，截断回滚，批次回到 open。
            keep = first - 1
            if self.seglog.tip()[0] > keep:
                self.seglog.truncate_tail(keep)
            if self.commit.commit_index > keep:
                self.commit.commit_index = keep
                self.commit.commit_digest = self._digest_or_anchor(keep) or GENESIS
            self.commit.prune_pending_through(keep)
            self.commit.persist()
            self.batches.rollback_to_open(b)
            rolled_back.append(b.batch_id)
        out: dict[str, Any] = {}
        if finalized:
            out["finalized"] = finalized
        if rolled_back:
            out["rolled_back"] = rolled_back
        if pending:
            out["pending"] = pending
        return out

    def _batch_records_present(self, b: Batch, first_seq: int) -> bool:
        """committing 批次的全部记录是否完整连续地留在链上（含摘要链衔接）。"""
        n = len(b.ops)
        if n <= 0:
            return False
        recs = self.seglog.read_records(first_seq, n)
        if len(recs) != n:
            return False
        anchor = self._digest_or_anchor(first_seq - 1)
        for i, r in enumerate(recs):
            if r["seq"] != first_seq + i or r["prev"] != anchor:
                return False
            anchor = r["digest"]
        return True

    def _reconcile_commit(self) -> dict:
        """启动时对账提交水位、待定提议与 write_id 登记。

        - commit_index 位置必须在本地链上（或恰为快照边界）且摘要一致；
        - 待定提议按现存链 + 写入登记 + committing 批次重建；
        - 本地链尖已到达的待多数确认记录（主在 fsync 后崩溃）保持待定，
          重启为当前主后继续等待多数派，绝不静默当作已提交；
        - 登记中 first_seq 已超出本地链尖（记录没落盘就崩溃）的写入标记
          为未开始：同 write_id 重试续用同一序号占位。
        """
        cs = self.commit
        tip_seq, _tip_digest = self.seglog.tip()
        if cs.commit_index > tip_seq:
            pointer = self.checkpoints.current
            if pointer is None or cs.commit_index != int(pointer["seq"]):
                raise err(500, "tail_corrupt",
                          "commit watermark is beyond the local log; refusing to start",
                          commit_index=cs.commit_index, tip_seq=tip_seq)
        elif cs.commit_index > 0:
            at = self._digest_or_anchor(cs.commit_index)
            if at is not None and at != cs.commit_digest and cs.commit_digest != GENESIS:
                raise err(500, "tail_corrupt",
                          "commit watermark digest does not match the log; refusing",
                          commit_index=cs.commit_index)
        if cs.commit_index > 0 and cs.commit_digest == GENESIS:
            d = self._digest_or_anchor(cs.commit_index)
            if d:
                cs.commit_digest = d
        # 未真正落盘的写入登记（记录不在链上）：转为未开始，等待同 write_id 重试
        dormant: list[str] = []
        for wid, e in list(cs.writes.items()):
            if e.get("status") == "pending" and int(e["first_seq"]) > tip_seq:
                dormant.append(wid)
        # 本地整批持久化但未提交水位的批次：以其 write_id（缺省以批次 id 兜底）
        # 重建 write 登记，相同 write_id 重试继续同一序号区间。
        for b in self.batches.committing_batches():
            if b.first_seq and self._batch_records_present(b, b.first_seq) \
                    and b.last_seq and b.last_seq > cs.commit_index:
                wid = b.write_id or f"batch:{b.batch_id}"
                b.write_id = wid
                if cs.get_write(wid) is None:
                    cs.register_write(wid, self.cluster.term, b.first_seq,
                                      b.last_seq, kind="batch",
                                      batch_id=b.batch_id)
        # 重建待定提议
        self._rebuild_pending_locked()
        if dormant:
            for wid in dormant:
                cs.writes[wid]["status"] = "unstarted"
            cs.persist()
        return {"commit_index": cs.commit_index,
                "pending": len(cs.pending),
                "dormant_writes": dormant}

    def _recover_schedules(self) -> dict:
        """启动时对账预约（在提交水位/待定提议重建之后）。

        - executing 且整组已越提交水位 -> applied（时钟回拨也不会重跑）；
        - executing 但整组记录已不在链上（被更新任期截断）-> 纪元前滚、
          回 pending，由当前主在新位置按原请求标识重做；
        - executing 记录仍在链但待定 -> 重建 write 登记/跨度，保持 executing
          可重试；pending 待补跑的不动，调度器按原顺序补跑。
        """
        applied: list[str] = []
        retried: list[str] = []
        waiting: list[str] = []
        for s in self.schedules.schedules.values():
            if s.status not in ACTIVE:
                continue
            if s.first_seq is not None and s.last_seq is not None:
                present = self._span_present(int(s.first_seq), int(s.last_seq))
                self._schedule_spans[(int(s.first_seq), int(s.last_seq))] = s.request_id
                if present and self.commit.commit_index >= int(s.last_seq):
                    if s.status != APPLIED:
                        self.schedules.mark_applied(
                            s, int(s.first_seq), int(s.last_seq),
                            s.executed_term or self.cluster.term, self._now())
                    applied.append(s.request_id)
                    continue
                if not present:
                    self._schedule_spans.pop((int(s.first_seq), int(s.last_seq)), None)
                    self._reset_schedule_for_retry(s, reason="truncated_by_new_term")
                    retried.append(s.request_id)
                    continue
                # 记录在链、水位未到：重建 write 登记，保持 executing
                if s.write_id and self.commit.get_write(s.write_id) is None:
                    self.commit.register_write(
                        s.write_id, s.executed_term or self.cluster.term,
                        int(s.first_seq), int(s.last_seq),
                        kind="schedule", batch_id=s.request_id)
                    self.commit.persist()
                waiting.append(s.request_id)
        return {"applied": applied, "retried": retried, "waiting": waiting,
                "pending": self.schedules.counts()[PENDING]}

    def _rebuild_pending_locked(self) -> None:
        """依据现存链、write_id 登记与 committing 批次重建 commit_index 之后的待定提议。

        日志位置按以下归属拆成提议（不允许跨提议边界推进水位）：
          term_marker（内部 data 记录）；
          整批已持久化的 committing 批次（整段属于该批次）；
          其余每条登记在 writes 中的记录一条普通写提议；
          未知记录（理论上不该出现的无登记项）以最保守的单条提议占位，
          term=0（永远不满足「当前任期先提交一条」的前提交条件）。
        """
        cs = self.commit
        commit_seq = cs.commit_index
        tip_seq, _ = self.seglog.tip()
        proposals: list[Proposal] = []
        seq = commit_seq + 1
        committing = [b for b in self.batches.committing_batches()
                      if (b.first_seq or 0) > commit_seq
                      and self._batch_records_present(b, b.first_seq)]
        while seq <= tip_seq:
            rec = self._record_at(seq)
            if rec is None:
                break  # 链在快照空洞：剩余部分未知，交快照边界处理
            if self._is_term_marker(rec):
                t = int(rec["payload"].get("term", 0))
                self.commit.add_term_marker(seq, t)
                proposals.append(Proposal(seq, seq, term=t, kind="term_marker"))
                seq += 1
                continue
            batch = next((b for b in committing if b.first_seq == seq), None)
            if batch is not None:
                last_seq = seq + len(batch.ops) - 1
                proposals.append(Proposal(
                    seq, last_seq, term=self.commit.term_at(seq), kind="batch",
                    write_id=batch.write_id, batch_id=batch.batch_id))
                seq = last_seq + 1
                continue
            # schedule 整组：以登记的跨度识别为一个不可拆开的提议
            sched_span = next(((f, l, rid) for (f, l), rid in self._schedule_spans.items()
                               if f == seq), None)
            if sched_span is not None:
                f0, l0, rid = sched_span
                sc = self.schedules.get(rid)
                proposals.append(Proposal(
                    f0, l0, term=self.commit.term_at(f0), kind="schedule",
                    write_id=(sc.write_id if sc else None), request_id=rid))
                seq = l0 + 1
                continue
            entry = None
            for e in cs.writes.values():
                if int(e["first_seq"]) == seq and int(e["last_seq"]) == seq:
                    entry = e
                    break
            if entry is not None:
                proposals.append(Proposal(
                    seq, seq, term=int(entry.get("term", self.commit.term_at(seq))),
                    kind="write", write_id=entry["write_id"]))
            else:
                # 无登记的尾部记录：最保守地按单条未知任期提议处理
                proposals.append(Proposal(seq, seq, term=0, kind="write"))
            seq += 1
        cs.pending = proposals
        cs.persist()

    @staticmethod
    def _is_term_marker(rec: dict) -> bool:
        return rec.get("type") == "data" and isinstance(rec.get("payload"), dict) \
            and rec["payload"].get("kind") == "term_marker"

    def _record_at(self, seq: int) -> Optional[dict]:
        recs = self.seglog.read_records(seq, 1)
        return recs[0] if recs else None

    def _digest_or_anchor(self, seq: int) -> Optional[str]:
        """链上 seq 处摘要；seq==0 -> GENESIS；落在快照边界 -> tail_anchor。"""
        if seq <= 0:
            return GENESIS
        d = self._digest_at(seq)
        if d is not None:
            return d
        pointer = self.checkpoints.current
        if pointer is not None and seq == int(pointer["seq"]):
            return pointer["tail_anchor"]
        return None

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

    # =====================================================================
    # 未来生效变更预约（scheduled changes）
    # =====================================================================

    MAX_SCHEDULE_OPS = 10_000

    def set_clock(self, fn: Optional[Callable[[], int]]) -> None:
        """注入墙上时钟（毫秒）；测试时钟回拨/停机补跑用。None 恢复真实时钟。"""
        self._clock = fn

    def _now(self) -> int:
        return int(self._clock()) if self._clock is not None else now_ms()

    def _validate_schedule_ops(self, ops: Any) -> list[dict]:
        if not isinstance(ops, list) or not ops:
            raise err(400, "bad_request", "ops must be a non-empty list of put/delete ops")
        if len(ops) > self.MAX_SCHEDULE_OPS:
            raise err(413, "schedule_too_large",
                      f"schedule would exceed {self.MAX_SCHEDULE_OPS} ops")
        normed: list[dict] = []
        for op in ops:
            if not isinstance(op, dict) or op.get("type") not in ("put", "delete"):
                raise err(400, "bad_request",
                          "each schedule op must be put or delete with a payload")
            # 入预约前校验业务负载，坏操作绝不进预约/日志
            Reducer().apply(op["type"], op.get("payload"))
            normed.append({"type": op["type"], "payload": op.get("payload")})
        return normed

    def create_schedule(self, request_id: Any, effective_at: Any, ops: Any,
                        expected_version: Optional[int] = None) -> dict:
        """创建（或幂等重放）一个未来生效的预约。

        - 相同 request_id + 相同内容 -> 返回原预约（replay:true），不重复安排；
        - 相同 request_id + 不同内容 -> 409 schedule_conflict；
        - 终态/执行中的相同请求也走原记录返回，绝不重建。
        """
        rid = self._validate_request_id(request_id)
        at = self._validate_effective_at(effective_at)
        normed = self._validate_schedule_ops(ops)
        content_hash = schedule_content_hash(normed)
        with self.meta_lock:
            self._require_writable_primary()
            existing = self.schedules.get(rid)
            if existing is not None:
                if existing.content_hash != content_hash or \
                        existing.effective_at != at:
                    # 相同请求不同内容/时间：明确拒绝（改期请走 reschedule）
                    raise err(409, "schedule_conflict",
                              "request_id already used with different content or "
                              "effective time",
                              request_id=rid, schedule_status=existing.status,
                              version=existing.version,
                              existing_effective_at=existing.effective_at)
                return self._schedule_view(existing, replay=True)
            s = self.schedules.create(rid, at, normed, content_hash,
                                      self.cluster.term, self._now())
            return self._schedule_view(s, replay=False, created=True)

    @staticmethod
    def _validate_request_id(request_id: Any) -> str:
        if not isinstance(request_id, str) or not (1 <= len(request_id) <= 256):
            raise err(400, "bad_request",
                      "request_id must be a string of length 1..256")
        if any(c in request_id for c in "/\\\n\r\t\0"):
            raise err(400, "bad_request", "request_id contains illegal characters")
        return request_id

    def _validate_effective_at(self, effective_at: Any) -> int:
        if isinstance(effective_at, bool) or not isinstance(effective_at, int):
            raise err(400, "bad_request",
                      "effective_at must be an integer epoch in milliseconds")
        if effective_at < 0:
            raise err(400, "bad_request", "effective_at must be >= 0")
        if effective_at - self._now() > 366 * 24 * 3600 * 1000:
            raise err(400, "bad_request", "effective_at too far in the future")
        return effective_at

    def reschedule(self, request_id: str, effective_at: Any,
                   expected_version: Any) -> dict:
        """携带当前版本改期：只有 pending 且版本匹配才能改到新时间。"""
        rid = self._validate_request_id(request_id)
        at = self._validate_effective_at(effective_at)
        ver = self._validate_version(expected_version)
        with self.meta_lock:
            self._require_writable_primary()
            s = self._require_schedule(rid)
            if s.status == EXECUTING or s.status == APPLIED:
                raise err(409, "already_started",
                          "schedule execution has already started; cannot reschedule",
                          request_id=rid, schedule_status=s.status, version=s.version)
            if s.status in (CANCELLED, SUPERSEDED):
                raise err(409, "schedule_closed",
                          f"schedule is {s.status}; cannot reschedule",
                          request_id=rid, schedule_status=s.status)
            if ver != s.version:
                raise err(409, "version_conflict",
                          "expected_version is stale; a newer arrangement exists",
                          request_id=rid, expected_version=ver,
                          current_version=s.version)
            self.schedules.reschedule(s, at, self._now())
            return self._schedule_view(s)

    def cancel_schedule(self, request_id: str, expected_version: Any) -> dict:
        """携带当前版本取消：pending 且版本匹配才成功。

        与到期领取竞争时由 meta_lock 串行化，结果唯一：
        领取先到 -> executing/applied，取消拿到 409 already_started（执行成功）；
        取消先到 -> cancelled，领取跳过它（取消成功、无业务变更）。
        """
        rid = self._validate_request_id(request_id)
        ver = self._validate_version(expected_version)
        with self.meta_lock:
            self._require_writable_primary()
            s = self._require_schedule(rid)
            if s.status == CANCELLED:
                # 取消幂等：同版本重复取消返回原结果
                return self._schedule_view(s, replay=True)
            if s.status in (EXECUTING, APPLIED):
                raise err(409, "already_started",
                          "schedule execution has already started; cancel rejected",
                          request_id=rid, schedule_status=s.status, version=s.version)
            if s.status == SUPERSEDED:
                raise err(409, "schedule_closed",
                          "schedule was superseded; cannot cancel",
                          request_id=rid, schedule_status=s.status, version=s.version)
            if ver != s.version:
                raise err(409, "version_conflict",
                          "expected_version is stale; a newer arrangement exists",
                          request_id=rid, expected_version=ver,
                          current_version=s.version)
            self.schedules.cancel(s, self._now())
            return self._schedule_view(s, cancelled=True)

    @staticmethod
    def _validate_version(version: Any) -> int:
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise err(400, "bad_request",
                      "expected_version must be a positive integer")
        return version

    def _require_schedule(self, rid: str):
        s = self.schedules.get(rid)
        if s is None:
            raise err(404, "not_found", f"unknown schedule request_id {rid!r}")
        return s

    def get_schedule(self, request_id: str) -> dict:
        with self.meta_lock:
            return self._schedule_view(self._require_schedule(request_id))

    def list_schedules(self, status: Optional[str] = None) -> dict:
        with self.meta_lock:
            items = self.schedules.list()
            if status:
                items = [s for s in items if s.status == status]
            return {"schedules": [self._schedule_view(s) for s in items],
                    "counts": self.schedules.counts()}

    def _schedule_view(self, s, replay: bool = False, created: bool = False,
                       cancelled: bool = False) -> dict:
        v = {
            "request_id": s.request_id,
            "effective_at": s.effective_at,
            "version": s.version,
            "status": s.status,
            "op_count": len(s.ops),
            "content_hash": s.content_hash,
            "seq": s.seq,
            "created_at": s.created_at,
            "created_term": s.created_term,
            "epoch": s.epoch,
            "write_id": s.write_id,
            "batch_id": s.batch_id,
            "attempt_count": s.attempt_count,
            "executed_term": s.executed_term,
            "first_seq": s.first_seq,
            "last_seq": s.last_seq,
            "applied_at": s.applied_at,
            "cancelled_at": s.cancelled_at,
            "supersede_reason": s.supersede_reason,
        }
        if replay:
            v["replay"] = True
        if created:
            v["created"] = True
        if cancelled:
            v["cancelled"] = True
        return v

    # ---------- 到期领取与执行（仅当前可写主）----------

    def scheduler_tick(self, transport: Optional[Callable] = None) -> dict:
        """一轮调度：对账执行中预约、按顺序领取到期预约（受单轮上限约束）。

        - 只处理到期 active 预约，按 (effective_at, 创建 seq) 排序，因此同一
          生效时间严格按创建顺序，后一个能看到前一个完成后的状态；
        - 单轮最多处理 catchup_batch_limit 个（停机积压的补跑同样受限），
          超出留到下一轮，不阻塞即时写入；
        - 每个预约处理完都释放 meta_lock，并等待已在入口排队的 /append 写请求
          获得一次执行机会；整轮不会连续占住写路径。预约本身仍由
          _scheduler_lock 串行选择，两个 tick 不会打乱先后关系；
        - 执行不等待多数派：记录本地持久化后即返回，成员确认由后续 ack /
          推送周期推进，下一轮 tick 再对账 applied；成员不足时停在 executing、
          保留原日志位置，恢复后续跑。
        """
        with self._scheduler_lock:
            with self.meta_lock:
                if self.cluster.role != PRIMARY or not self.cluster.grant_valid():
                    return {"ran": 0, "reason": "not_writable_primary"}
                self._reconcile_executing_locked()
                now = self._now()
                due = self.schedules.due_order(now, limit=self.cfg.catchup_batch_limit)
                chosen = [s.request_id for s in due]

            ran = 0
            results: list[dict] = []
            for rid in chosen:
                with self.meta_lock:
                    # 领取与取消/改期在同一把 meta_lock 上串行，竞争结果唯一。
                    # 选择快照后若该预约被取消/改期，这里跳过，后续预约顺序不变。
                    s = self.schedules.get(rid)
                    if s is None or s.status not in ACTIVE or s.effective_at > now:
                        continue
                    if s.status == EXECUTING:
                        out = self._continue_schedule_locked(s, transport)
                    else:
                        out = self._claim_and_append_locked(s)
                ran += 1
                results.append(out)
                self._yield_to_append_waiters()

            with self.meta_lock:
                # 仍到期且尚未领取（pending）的数量：受单轮上限约束留下的积压
                left = [s for s in self.schedules.due_order(now) if s.status == PENDING]
            return {"ran": ran, "now": now,
                    "results": results,
                    "remaining_due": len(left)}

    @contextmanager
    def _append_admission(self):
        """在排队 meta_lock 前登记写请求，供调度器在预约之间显式放行。"""
        token = object()
        with self._append_admit_cv:
            self._append_waiters.add(token)
        try:
            yield
        finally:
            with self._append_admit_cv:
                self._append_waiters.discard(token)
                self._append_admit_cv.notify_all()

    def _yield_to_append_waiters(self) -> None:
        """在两个预约之间放行所有已进入 append 入口的写请求。

        仅释放 meta_lock 仍依赖 RLock 的非公平调度，调度线程可能立刻再次获
        锁；因此 append 先在条件变量上登记，等其真正拿到 meta_lock 后撤销
        登记。此处等待的是边界时刻已存在的写请求，后续持续到达的新请求不会
        让调度器永久饥饿。
        """
        with self._append_admit_cv:
            waiting = set(self._append_waiters)
        for token in waiting:
            with self._append_admit_cv:
                self._append_admit_cv.wait_for(
                    lambda: token not in self._append_waiters)

    def _reconcile_executing_locked(self) -> None:
        """把「记录已越提交水位」的 executing 预约标记 applied（幂等）。"""
        ci = self.commit.commit_index
        for s in self.schedules.schedules.values():
            if s.status == EXECUTING and s.last_seq is not None and ci >= int(s.last_seq):
                self.schedules.mark_applied(
                    s, int(s.first_seq), int(s.last_seq),
                    s.executed_term or self.cluster.term, self._now())

    def _claim_and_append_locked(self, s) -> dict:
        """领取一个 pending 预约并把整组记录原子地追加到本地链。

        整组作为一个 kind=schedule 提议登记，提交水位只会越过整组边界，
        到期操作要么全部生效、要么完全不生效。复用确定性派生的
        batch/write 标识，接管的主沿用原请求标识完成同一提交。
        """
        term = self.cluster.term
        epoch = s.epoch + 1
        bid = exec_batch_id(s.request_id)
        wid = exec_write_id(s.request_id, epoch)
        first_seq = self.seglog.next_seq
        # 领取意图先落盘：崩溃/切换后新主据此判断是否已开始、沿用何纪元
        self.schedules.claim(s, epoch, bid, wid, term, self._now())
        try:
            for op in s.ops:
                self._append_record_locked(op["type"], op["payload"])
            last_seq = first_seq + len(s.ops) - 1
        except Exception:
            # 本地追加失败：整批回滚链尾，预约回到 pending 留待下轮重做
            self._rollback_schedule_append(s, first_seq)
            raise
        s.first_seq, s.last_seq = first_seq, last_seq
        self._schedule_spans[(first_seq, last_seq)] = s.request_id
        self.commit.register_write(wid, term, first_seq, last_seq,
                                   kind="schedule", batch_id=s.request_id)
        self.commit.add_proposal(Proposal(
            first_seq, last_seq, term=term, kind="schedule",
            write_id=wid, request_id=s.request_id))
        self.commit.persist()
        self.schedules.persist()
        self._advance_commit_locked()
        # 单节点（仅自己一个投票成员）通常当场越过提交水位
        if self.commit.commit_index >= last_seq:
            self.schedules.mark_applied(s, first_seq, last_seq, term, self._now())
            return {"request_id": s.request_id, "status": APPLIED,
                    "first_seq": first_seq, "last_seq": last_seq}
        return {"request_id": s.request_id, "status": EXECUTING,
                "first_seq": first_seq, "last_seq": last_seq, "epoch": epoch}

    def _continue_schedule_locked(self, s, transport: Optional[Callable]) -> dict:
        """继续一个 executing 预约：沿用原 write_id/位置，绝不重复执行。

        - 已越提交水位 -> applied；
        - write 登记缺失（截断后未重建等）/ 本地记录不在链上（被更新任期截断）
          -> 纪元前滚，回到 pending 在新位置重做（旧 write_id 已 superseded，
             旧实例迟到结果不可能被写成成功）；
        - 仍是本任期待定 -> 保持 executing、原位置，尝试推进水位
          （成员不足时停在此处可重试，恢复后续跑）。
        """
        first = s.first_seq
        last = s.last_seq
        wid = s.write_id
        term = s.executed_term or self.cluster.term
        entry = self.commit.get_write(wid) if wid else None
        records_present = first is not None and last is not None and \
            self._span_present(int(first), int(last))
        if last is not None and self.commit.commit_index >= int(last) and records_present:
            self.schedules.mark_applied(s, int(first), int(last), term, self._now())
            return {"request_id": s.request_id, "status": APPLIED,
                    "first_seq": int(first), "last_seq": int(last), "retry": True}
        # 被更新任期截断：旧位置记录已不在链上，或 write 登记被标 superseded
        superseded = entry is not None and entry.get("status") == "superseded"
        if not records_present or superseded:
            self._reset_schedule_for_retry(s, reason="truncated_by_new_term")
            return self._claim_and_append_locked(s)
        if entry is None:
            # 登记缺失但记录在链（理论兜底）：重建登记后继续等同一位置
            self.commit.register_write(wid, term, int(first), int(last),
                                       kind="schedule", batch_id=s.request_id)
            self.commit.persist()
        # 仍是当前主：尝试推进；不阻塞，下轮再来
        self._advance_commit_locked()
        self.schedules.record_attempt_error(s, self._now())
        if self.commit.commit_index >= int(last):
            self.schedules.mark_applied(s, int(first), int(last), term, self._now())
            return {"request_id": s.request_id, "status": APPLIED,
                    "first_seq": int(first), "last_seq": int(last), "retry": True}
        return {"request_id": s.request_id, "status": EXECUTING,
                "first_seq": int(first), "last_seq": int(last),
                "retry": True, "commit_index": self.commit.commit_index}

    def _span_present(self, first_seq: int, last_seq: int) -> bool:
        """整组记录是否完整连续地留在链上（含与前驱的摘要衔接）。"""
        if first_seq <= 0 or last_seq < first_seq:
            return False
        n = last_seq - first_seq + 1
        recs = self.seglog.read_records(first_seq, n)
        if len(recs) != n:
            return False
        anchor = self._digest_or_anchor(first_seq - 1)
        for i, r in enumerate(recs):
            if r["seq"] != first_seq + i or r["prev"] != anchor:
                return False
            anchor = r["digest"]
        return True

    def _rollback_schedule_append(self, s, first_seq: int) -> None:
        """本地追加失败：截断链尾、清登记，预约回到 pending（不留半组/空洞）。"""
        keep = first_seq - 1
        if self.seglog.tip()[0] > keep:
            self.seglog.truncate_tail(keep)
        if self.commit.commit_index > keep:
            self.commit.commit_index = keep
            self.commit.commit_digest = self._digest_or_anchor(keep) or GENESIS
        self.commit.prune_pending_through(keep)
        self.commit.persist()
        s.status = PENDING
        s.first_seq = s.last_seq = None
        self.schedules.persist()
        self._persist_head()

    def _reset_schedule_for_retry(self, s, reason: str) -> None:
        """日志被更新任期截断：纪元前滚、回 pending，以便在新位置重做整组。"""
        s.status = PENDING
        s.epoch += 1  # 派生新 write_id；旧 write_id 已 superseded
        s.first_seq = s.last_seq = None
        s.batch_id = s.write_id = None
        s.supersede_reason = None
        self.schedules.persist()

    def _validate_write_id(self, write_id: Any) -> str:
        if write_id is None:
            return new_write_id()
        if not isinstance(write_id, str) or not (1 <= len(write_id) <= 256):
            raise err(400, "bad_request", "write_id must be a string of length 1..256")
        return write_id

    def _clamp_commit_timeout(self, timeout_ms: Any, default_ms: Optional[int] = None) -> int:
        if timeout_ms is None:
            return default_ms if default_ms is not None else self.cfg.commit_timeout_ms
        try:
            t = int(timeout_ms)
        except (TypeError, ValueError):
            raise err(400, "bad_request", "timeout_ms must be an integer")
        if not (50 <= t <= 120_000):
            raise err(400, "bad_request", "timeout_ms must be in [50,120000]")
        return t

    def _append_record_locked(self, rec_type: str, payload: Any) -> dict:
        """在已持锁情况下把一条记录追加到本地链（fsync + 链头锚点 + 滚动）。"""
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

    def append(self, rec_type: str, payload: Any, write_id: Optional[str] = None,
               timeout_ms: Optional[int] = None, wait_commit: bool = True,
               transport: Optional[Callable] = None) -> dict:
        """追加一条记录并等待多数派提交（默认）。

        - write_id 由调用方提供（缺省自动生成）；相同 write_id 重试永远
          续用同一任期/序号：已提交 -> 原结果重放；等待中 -> 继续等；
          被新任期截断 -> 409 commit_superseded，绝不追加重复记录。
        - 主先把记录持久化到本地，再等当前任期多数投票成员确认同一日志
          位置；超时返回 504 commit_timeout（记录仍是待定状态）。
        """
        timeout = self._clamp_commit_timeout(timeout_ms)
        wid = self._validate_write_id(write_id)
        with self._append_admission(), self.meta_lock:
            self._require_writable_primary()
            term = self.cluster.term
            existing = self.commit.get_write(wid)
            if existing is not None:
                return self._resume_write_locked(existing, timeout, wait_commit, transport)
            # 入链前校验业务负载，坏记录绝不进哈希链
            Reducer().apply(rec_type, payload)
            rec = self._append_record_locked(rec_type, payload)
            seq = rec["seq"]
            self.commit.register_write(wid, term, seq, seq, kind="write")
            self.commit.add_proposal(Proposal(seq, seq, term=term,
                                              kind="write", write_id=wid))
            self.commit.persist()
            self._advance_commit_locked()
            result = self._write_result(rec, wid, term, seq, seq)
            if self.commit.commit_index >= seq:
                result["committed"] = True
                result["commit_index"] = self.commit.commit_index
                return result
        if not wait_commit:
            result["committed"] = False
            result["status"] = "pending"
            return result
        return self._await_commit(wid, term, seq, seq, result, timeout, transport)

    def _resume_write_locked(self, existing: dict, timeout: int, wait_commit: bool,
                             transport: Optional[Callable]) -> dict:
        """相同 write_id 重试：续用原提交过程，绝不追加重复记录。"""
        wid = existing["write_id"]
        first, last = int(existing["first_seq"]), int(existing["last_seq"])
        term = int(existing["term"])
        if existing.get("status") == "committed":
            return self._committed_write_view(existing, replay=True)
        if existing.get("status") == "superseded":
            raise err(409, "commit_superseded",
                      "write was truncated by a newer term's leader; "
                      "retry with a new write_id",
                      term=term, write_id=wid, first_seq=first, last_seq=last)
        if existing.get("status") == "unstarted":
            raise err(409, "write_unstarted",
                      "write was registered but its record never reached disk; "
                      "restart the append with this write_id",
                      term=term, write_id=wid)
        # pending：必须仍是当前任期主，记录仍在链上
        if self.cluster.role != PRIMARY or self.cluster.term != term \
                or not self.cluster.grant_valid():
            raise err(409, "commit_superseded",
                      "leadership changed before the write committed; "
                      "the record stays pending and is not duplicated",
                      term=term, current_term=self.cluster.term,
                      write_id=wid, first_seq=first, last_seq=last,
                      role=self.cluster.role)
        self._advance_commit_locked()
        result = self._committed_write_view(existing, replay=False) \
            if self.commit.commit_index >= last else None
        if result is not None:
            return result
        if not wait_commit:
            return {"write_id": wid, "term": term, "seq": first,
                    "first_seq": first, "last_seq": last,
                    "committed": False, "status": "pending"}
        rec = self._record_at(first) or {}
        base = self._write_result(rec, wid, term, first, last) if rec else \
            {"write_id": wid, "term": term, "seq": first,
             "first_seq": first, "last_seq": last}
        return self._await_commit(wid, term, first, last, base, timeout, transport)

    def _write_result(self, rec: dict, wid: str, term: int,
                      first: int, last: int) -> dict:
        out = {
            "write_id": wid,
            "term": term,
            "first_seq": first,
            "last_seq": last,
        }
        if "seq" in rec:
            out.update({
                "seq": rec["seq"], "ts": rec.get("ts"), "type": rec.get("type"),
                "payload": rec.get("payload"), "prev": rec.get("prev"),
                "digest": rec.get("digest"),
            })
        return out

    def _committed_write_view(self, entry: dict, replay: bool) -> dict:
        first, last = int(entry["first_seq"]), int(entry["last_seq"])
        rec = self._record_at(first) or {}
        out = self._write_result(rec, entry["write_id"], int(entry["term"]), first, last)
        out["committed"] = True
        out["replay"] = replay
        out["commit_index"] = self.commit.commit_index
        return out

    def _await_commit(self, wid: str, term: int, first: int, last: int,
                      base_result: dict, timeout_ms: int,
                      transport: Optional[Callable]) -> dict:
        """释放 meta_lock 等待 [first..last] 整体进入提交水位。

        被提交事件（成员 ack）广播唤醒后重新入锁复核；多数派恢复后由
        ack 处理按原顺序推进待定记录，等待线程无需自行重试。
        """
        deadline = now_ms() + timeout_ms
        while True:
            with self.meta_lock:
                self._advance_commit_locked()
                if self.commit.commit_index >= last:
                    out = dict(base_result)
                    out.update({"committed": True,
                                "commit_index": self.commit.commit_index})
                    return out
                if self.cluster.term != term or self.cluster.role != PRIMARY:
                    raise err(409, "commit_superseded",
                              "term changed while waiting for quorum commit",
                              term=term, current_term=self.cluster.term,
                              write_id=wid, first_seq=first, last_seq=last)
                if not self.cluster.grant_valid():
                    raise err(403, "grant_expired",
                              "leadership grant expired before quorum commit",
                              term=term, write_id=wid,
                              first_seq=first, last_seq=last)
                remaining = deadline - now_ms()
                if remaining <= 0:
                    raise err(504, "commit_timeout",
                              "record persisted locally but not confirmed by a "
                              "majority before the timeout; retry with the same "
                              "write_id to continue the same commit",
                              term=term, write_id=wid,
                              first_seq=first, last_seq=last,
                              commit_index=self.commit.commit_index)
            with self._commit_cv:
                self._commit_cv.wait(min(1.0, remaining / 1000))

    # =====================================================================
    # 多数派提交水位：成员确认推进、线性一致读屏障
    # =====================================================================
    #
    # 主实例把「本任期各成员已确认的最长相同日志位置」收集起来；只有当
    # 多数派确认的位置上是**当前任期**的记录时才推进 commit_index，并且
    # 只在提议边界（普通写/原子批次/任期标记）上推进——原子批次绝不会在
    # 提交水位两侧被拆开。旧任期的待定尾部因此必须等当前任期先提交一条
    # 记录（上任时写入的 term_marker）后才随水位确认。

    def _voter_ids(self) -> list[str]:
        return list(self.cluster.s.voters)

    def _quorum_size(self) -> int:
        n = len(self.cluster.s.voters) or 1
        return n // 2 + 1

    def _advance_commit_locked(self) -> Optional[int]:
        """依据当前多数派确认位置推进主实例提交水位，返回新水位（无推进 None）。

        仅在主实例调用；备实例的水位由来源主在 ack 回执中告知。
        """
        if self.cluster.role != PRIMARY:
            return None
        term = self.cluster.term
        n = len(self.cluster.s.voters) or 1
        need = n // 2 + 1
        tip_seq, _ = self.seglog.tip()
        # 自己在当前任期已把记录持久化到 tip
        matches = [tip_seq]
        for nid in self.cluster.s.voters:
            if nid == self.cluster.node_id:
                continue
            ack = self.commit.acks.get(nid)
            if ack is None or int(ack.get("term", -1)) != term:
                continue
            seq = int(ack["match_seq"])
            # 只承认「同一日志位置」：摘要必须与本地链一致（已压缩则信任边界）
            if seq > tip_seq:
                continue
            d = self._digest_or_anchor(seq)
            if d is None:
                continue
            if seq <= self.commit.commit_index or d == self._digest_or_anchor(seq):
                matches.append(seq)
        if len(matches) < need:
            return None
        matches.sort(reverse=True)
        qseq = matches[need - 1]
        if qseq <= self.commit.commit_index:
            return None
        # 当前任期规则：qseq 位置必须属于当前任期的提议
        boundary = self._committable_boundary(term, qseq)
        if boundary is None or boundary <= self.commit.commit_index:
            return None
        d = self._digest_or_anchor(boundary) or GENESIS
        self._apply_commit_locked(boundary, term, d)
        return boundary

    def _committable_boundary(self, term: int, qseq: int) -> Optional[int]:
        """返回 <= qseq 可提交的最大提议边界；须含一条当前任期提议。"""
        boundary = self.commit.commit_index
        saw_current_term = False
        for p in self.commit.pending:
            if p.first_seq > qseq:
                break
            if p.last_seq > qseq:
                break  # qseq 落在提议内部（原子批次中间）：水位不能越过
            boundary = p.last_seq
            if p.term == term:
                saw_current_term = True
        return boundary if saw_current_term else None

    def _apply_commit_locked(self, seq: int, term: int, digest: str) -> None:
        if not self.commit.set_commit(seq, term, digest):
            return
        committed = self.commit.drop_committed()
        for p in committed:
            if p.write_id:
                self.commit.mark_write_committed(p.write_id)
        self.commit.persist()
        # 唤醒等待提交的写线程
        with self._commit_cv:
            self._commit_cv.notify_all()

    def handle_ack(self, node_id: str, term: int, match_seq: int,
                   commit_seq: int, barrier: bool = False,
                   commit_digest: Optional[str] = None) -> dict:
        """RPC：投票成员向主实例报告确认位置；也承担读屏障心跳。

        - 备实例见到更高任期：前滚本地任期（但角色不变）；
        - 主实例收到更高任期：授权失效、降为备，明确拒绝；
        - barrier=True 时不更新复制匹配进度（仅作为读屏障心跳）。
        """
        with self.meta_lock:
            if term < self.cluster.term:
                return {"term": self.cluster.term, "ack": False,
                        "role": self.cluster.role,
                        "reason": "stale_term"}
            if term > self.cluster.term:
                if self.cluster.role == PRIMARY:
                    self.cluster.bump_term(term)
                    with self._commit_cv:
                        self._commit_cv.notify_all()
                    return {"term": self.cluster.term, "ack": False,
                            "role": STANDBY, "reason": "term_changed"}
                self.cluster.bump_term(term)
            if self.cluster.role != PRIMARY:
                # 读屏障心跳：备实例也可以确认「我见过该任期」，候选主据此
                # 证明自己仍是多数派认可的主（follower 不再支持更高任期者）。
                if barrier:
                    return {"term": self.cluster.term, "ack": True,
                            "role": STANDBY, "primary": self.replica.peer_url,
                            "commit_index": self.commit.commit_index}
                return {"term": self.cluster.term, "ack": False,
                        "role": STANDBY,
                        "primary": self.replica.peer_url,
                        "reason": "not_primary"}
            match_seq = int(match_seq)
            if not barrier:
                # 同一位置摘要必须一致才计入（防止不同历史被当作确认）
                tip_seq, _ = self.seglog.tip()
                d = self._digest_or_anchor(match_seq) if match_seq <= tip_seq else None
                if match_seq > 0 and d is None:
                    return {"term": self.cluster.term, "ack": False,
                            "role": PRIMARY, "reason": "unknown_position",
                            "commit_index": self.commit.commit_index}
                if match_seq > self.commit.commit_index and d is not None \
                        and commit_digest and d != commit_digest:
                    # 该位置历史与主不一致：绝不据此推进提交水位
                    return {"term": self.cluster.term, "ack": False,
                            "role": PRIMARY, "reason": "divergent_position",
                            "commit_index": self.commit.commit_index,
                            "match_seq": match_seq}
                self.commit.record_ack(node_id, term, match_seq, int(commit_seq))
                self.commit.persist()
                self._advance_commit_locked()
            return {"term": self.cluster.term, "ack": True, "role": PRIMARY,
                    "commit_index": self.commit.commit_index,
                    "tip_seq": self.seglog.tip()[0]}

    def _follower_ack_primary(self, transport: Optional[Callable] = None) -> None:
        """备实例：把本地已确认位置报告给主实例（复制推进后调用）。"""
        peer = self.replica.peer_url
        if not peer:
            return
        from .replication import PATH_ACK

        transport = transport or self._http
        try:
            _st, resp = transport("POST", peer + PATH_ACK, {
                "node_id": self.cluster.node_id,
                "term": self.cluster.term,
                "match_seq": self.replica.synced_seq,
                "commit_seq": self.replica.synced_seq,
                "commit_digest": self.replica.synced_digest,
            })
            if int(resp.get("term", self.cluster.term)) > self.cluster.term:
                self.cluster.bump_term(int(resp["term"]))
            # 主在回执里给出它的提交水位：备实例只跟随，绝不自己越过
            leader_commit = int(resp.get("commit_index", -1))
            if resp.get("ack") and leader_commit >= 0:
                self._follower_advance_commit(leader_commit)
        except Error:
            pass
        except Exception:
            pass

    # ---------- 主实例主动推送（新主对旧主/未拉取成员） ----------

    def leader_push_once(self, peer_url: str,
                         transport: Optional[Callable] = None) -> dict:
        """主实例把提交水位之后的记录主动推给一个跟随成员，并收集其确认。

        故障切换后旧主不会自动改去拉取新主，因此主必须能主动推送。
        推送是标准复制页的逆方向：备端走与拉取完全相同的校验/应用路径。
        """
        from .replication import PATH_APPEND, PATH_PROGRESS

        transport = transport or self._http
        with self.meta_lock:
            if self.cluster.role != PRIMARY:
                return {"skipped": "not_primary"}
            term = self.cluster.term
            tip_seq, _ = self.seglog.tip()
            nid = self._peer_node(peer_url)
            ack = self.commit.acks.get(nid)
            match = int(ack["match_seq"]) if ack else None
            pointer = self.checkpoints.current
        # 未知成员（典型：故障切换后的旧主）：先探测其同步进度/链尖，
        # 与本地历史一致则直接当作已确认位置，随后推送缺失尾部。
        probe_applied = 0
        if match is None:
            try:
                _st, prog = transport("GET", peer_url + PATH_PROGRESS)
                if int(prog.get("term", term)) > term:
                    with self.meta_lock:
                        self.cluster.bump_term(int(prog["term"]))
                    return {"pushed": False, "reason": "term_changed"}
                ftip = int(prog.get("tip_seq", 0))
                with self.meta_lock:
                    if ftip > 0 and self._digest_or_anchor(ftip) == prog.get("tip_digest"):
                        # 链尖与本地一致：把它登记到链尖（多数派可证）
                        match = ftip
                        self.commit.record_ack(
                            str(prog.get("node_id") or nid), term, match,
                            int(prog.get("commit_index", 0)))
                        self.commit.persist()
                        self._advance_commit_locked()
                    else:
                        match = min(int(prog.get("commit_index", 0)),
                                    self.commit.commit_index)
            except Error as e:
                return {"pushed": False, "error": e.code}
            except Exception as e:
                return {"pushed": False, "error": getattr(e, "code", "unreachable")}
        start = max(1, match + 1)
        readable_from = pointer["seq"] + 1 if pointer else 1
        try:
            if start < readable_from or (pointer is not None and match < pointer["seq"]):
                # 跟随者落后到需要快照：交给它自己的拉取周期/人工处理，
                # 主动推送只覆盖增量尾部。
                return {"skipped": "snapshot_required", "match_seq": match}
            if start > tip_seq:
                return {"pushed": True, "sent": 0, "match_seq": match,
                        "commit_index": self.commit.commit_index}
            page = self.export_records(start, self.cfg.replication_batch, term)
            _st, resp = transport("POST", peer_url + PATH_APPEND, page)
            if int(resp.get("term", term)) > term:
                with self.meta_lock:
                    self.cluster.bump_term(int(resp["term"]))
                return {"pushed": False, "reason": "term_changed"}
            with self.meta_lock:
                follower_match = int(resp.get("match_seq", tip_seq)) if resp.get("success") else match
            # 跟随者确认后，主可能推进了自己的提交水位；再发一条空心跳，
            # 把新的 commit_index 告知跟随者（否则它停在推送页的旧水位）。
            if resp.get("success"):
                with self.meta_lock:
                    nid = resp.get("node_id") or self._peer_node(peer_url)
                    self.commit.record_ack(
                        nid, term, follower_match,
                        int(resp.get("commit_seq", self.commit.commit_index)))
                    self.commit.persist()
                    self._advance_commit_locked()
                    heartbeat = {
                        "term": self.cluster.term,
                        "from": follower_match + 1,
                        "tip_seq": self.seglog.tip()[0],
                        "records": [],
                        "commit_index": self.commit.commit_index,
                        "commit_digest": self.commit.commit_digest,
                        "pending": [p.to_dict() for p in self.commit.pending],
                        "term_markers": self.commit.term_marker_view(),
                    }
                try:
                    transport("POST", peer_url + PATH_APPEND, heartbeat)
                except Exception:
                    pass
            with self.meta_lock:
                return {"pushed": bool(resp.get("success")),
                        "sent": len(page.get("records", [])),
                        "match_seq": follower_match,
                        "commit_index": self.commit.commit_index}
        except Error as e:
            return {"pushed": False, "error": e.code}
        except Exception as e:
            return {"pushed": False, "error": getattr(e, "code", "transport_error")}

    def _peer_node(self, peer_url: str) -> str:
        for nid, url in self.cfg.peers.items():
            if url.rstrip("/") == peer_url.rstrip("/"):
                return nid
        return peer_url

    def handle_progress(self) -> dict:
        """RPC（备端）：向主汇报当前同步进度/链尖（供主主动推送定位）。"""
        with self.meta_lock:
            tip_seq, tip_digest = self.seglog.tip()
            return {
                "term": self.cluster.term,
                "role": self.cluster.role,
                "node_id": self.cluster.node_id,
                "synced_seq": self.replica.synced_seq,
                "synced_digest": self.replica.synced_digest,
                "commit_index": self.commit.commit_index,
                "tip_seq": tip_seq,
                "tip_digest": tip_digest,
            }

    def handle_append_entries(self, page: dict) -> dict:
        """RPC（备端）：主主动推送的复制页，走与拉取相同的 apply_records。"""
        with self.meta_lock:
            term = int(page.get("term", 0))
            if term < self.cluster.term:
                return {"success": False, "term": self.cluster.term,
                        "reason": "stale_term"}
            if term > self.cluster.term:
                self.cluster.bump_term(term)
            if self.cluster.role == PRIMARY and self.cluster.grant_valid():
                return {"success": False, "term": self.cluster.term,
                        "reason": "leader_valid"}
            # 推送页与拉取页同构：顺序/摘要链/冲突逻辑完全一致
            try:
                before = self.replica.synced_seq
                # 先合并主的提议归属与提交水位（不回拉 ack：确认由响应返回）
                self._merge_primary_meta(page)
                self.apply_records(page, ack_back=False)
                leader_commit = int(page.get("commit_index", self.commit.commit_index))
                if leader_commit > self.commit.commit_index:
                    self._follower_advance_commit(leader_commit)
                return {"success": True, "term": self.cluster.term,
                        "node_id": self.cluster.node_id,
                        "match_seq": self.replica.synced_seq,
                        "commit_seq": self.commit.commit_index,
                        "applied": self.replica.synced_seq - before}
            except Error as e:
                return {"success": False, "term": self.cluster.term,
                        "reason": e.code, "details": e.details}

    def _follower_advance_commit(self, leader_commit: int) -> None:
        """备实例推进提交水位：只到本地已同步位置、不跨越已知批次边界。

        主实例的提交水位是权威的（它只会在提议边界推进）；备实例只在
        本地待定提议表**明确知道**某个原子批次跨越目标位置时才停在该
        批次之前。提议表为空/未覆盖目标（边界来自快照安装、归属信息已
        压缩）时信任主实例，直接推进到 min(leader_commit, synced)。
        """
        cs = self.commit
        target = min(leader_commit, self.replica.synced_seq)
        if target <= cs.commit_index:
            return
        boundary = target
        for p in cs.pending:
            # 目标严格落在已知原子批次内部才停在它之前；
            # target == last_seq 即整批边界，可直接提交。
            if p.kind == "batch" and p.first_seq <= target < p.last_seq:
                boundary = p.first_seq - 1
                break
        if boundary > cs.commit_index:
            d = self._digest_or_anchor(boundary) or GENESIS
            self._apply_commit_locked(boundary, self.cluster.term, d)

    def linearizable_read(self, timeout_ms: Optional[int] = None,
                          transport: Optional[Callable] = None) -> dict:
        """线性一致读：当前主在当前任期向多数成员完成一次读屏障后返回。

        读屏障 = 当前任期心跳被多数派确认（ReadIndex 语义）：
        - 失去多数派 / 授权过期 / 等待期间发生任期变化 -> 明确拒绝；
        - 备实例拒绝并返回当前角色、任期与已知主实例。
        返回的状态只包含不超过提交水位的内容。
        """
        with self.meta_lock:
            if self.cluster.role != PRIMARY:
                raise err(403, "not_primary",
                          "linearizable reads are served by the primary only",
                          role=self.cluster.role, term=self.cluster.term,
                          primary=self.replica.peer_url)
            if not self.cluster.grant_valid():
                raise err(403, "grant_expired",
                          "leadership grant expired; cannot prove leadership",
                          term=self.cluster.term)
            term = self.cluster.term
            timeout = self._clamp_commit_timeout(
                timeout_ms, self.cfg.read_barrier_timeout_ms)
        if not self._read_barrier(term, timeout, transport):
            with self.meta_lock:
                cur_term = self.cluster.term
                role = self.cluster.role
            if cur_term != term or role != PRIMARY:
                raise err(409, "term_changed",
                          "term or leadership changed during the read barrier",
                          term=term, current_term=cur_term)
            raise err(504, "read_barrier_timeout",
                      "majority did not acknowledge this term before timeout",
                      term=term)
        # 屏障成功后再次复核：任期未变化且仍是主，才返回提交水位内的状态
        with self.meta_lock:
            if self.cluster.term != term or self.cluster.role != PRIMARY:
                raise err(409, "term_changed",
                          "term changed after the read barrier",
                          term=term, current_term=self.cluster.term)
            if not self.cluster.grant_valid():
                raise err(403, "grant_expired", "grant expired during the read",
                          term=term)
            return self._committed_state_view(term, barrier=True)

    def _read_barrier(self, term: int, timeout_ms: int,
                      transport: Optional[Callable]) -> bool:
        """当前任期心跳拿到多数派确认（含自己一票）。"""
        from .replication import PATH_ACK

        transport = transport or self._http
        peers = [(nid, u.rstrip("/")) for nid, u in self.cfg.peers.items()
                 if nid != self.cluster.node_id]
        need = (len(self.cluster.s.voters) or 1) // 2 + 1
        deadline = now_ms() + timeout_ms
        # 单节点：自足多数派
        if not peers:
            with self.meta_lock:
                return self.cluster.term == term and self.cluster.role == PRIMARY
        acks = 1
        newer_term = False
        lock = threading.Lock()

        def probe(url: str) -> None:
            nonlocal acks, newer_term
            try:
                _st, resp = transport("POST", url + PATH_ACK, {
                    "node_id": self.cluster.node_id, "term": term,
                    "barrier": True})
                if resp.get("ack"):
                    with lock:
                        acks += 1
                elif int(resp.get("term", term)) > term:
                    newer_term = True
                    with self.meta_lock:
                        self.cluster.bump_term(int(resp["term"]))
            except Exception:
                pass

        threads = [threading.Thread(target=probe, args=(url,), daemon=True)
                   for _nid, url in peers]
        for t in threads:
            t.start()
        for t in threads:
            remaining = max(0.05, (deadline - now_ms()) / 1000)
            t.join(remaining)
        if newer_term:
            return False
        return acks >= need


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

    def commit_batch(self, batch_id: str, write_id: Optional[str] = None,
                     timeout_ms: Optional[int] = None, wait_commit: bool = True,
                     transport: Optional[Callable] = None) -> dict:
        """提交批次：整批记录按加入顺序一次性入链并等待多数派确认。

        - 同批次/同幂等键重复提交相同内容 -> 返回首次提交结果（不重复入链）；
        - 同幂等键不同内容 -> 409 idempotency_conflict；
        - 并发提交由 meta_lock 串行化，只有一个真正入链，其余拿到重放结果；
        - 整批共享一个 write_id：提交水位只会越过整批边界，批次绝不会在
          水位两侧被拆开；多数派确认超时返回 504 commit_timeout，相同
          write_id（或同批次 id）重试继续同一提交过程，不追加重复记录。
        """
        timeout = self._clamp_commit_timeout(timeout_ms)
        with self.meta_lock:
            self._require_writable_primary()
            term = self.cluster.term
            b = self._require_batch(batch_id)
            if b.status == COMMITTED:
                last_seq = b.last_seq or 0
                if self.commit.commit_index >= last_seq:
                    # 同批次重复提交且已越提交水位：返回首次结果，不重复入链
                    return self._batch_result(b, replay=True, committed=True)
                # 批次状态已落 committed 但水位未到（重启后续等多数派）：
                # 继续同一提交过程。
                wid = self._validate_write_id(write_id or b.write_id)
                return self._continue_batch_locked(
                    b, wid, term, b.first_seq, b.last_seq,
                    timeout, wait_commit, transport)
            if b.status == ABORTED:
                raise err(409, "batch_aborted", "batch was aborted; cannot commit")
            if b.effective_status(now_ms()) == EXPIRED:
                raise err(410, "batch_expired", "batch expired; cannot commit")
            if not b.ops:
                raise err(400, "empty_batch", "cannot commit an empty batch")
            content_hash = ops_hash(b.ops)
            key = b.registry_key()
            entry = self.batches.commit_entry(key)
            if entry is not None:
                if entry["content_hash"] != content_hash:
                    raise err(409, "idempotency_conflict",
                              "idempotency key already committed with different content",
                              idempotency_key=b.idempotency_key,
                              first_batch_id=entry["batch_id"],
                              first_seq=entry["first_seq"], last_seq=entry["last_seq"])
                wid = self._validate_write_id(write_id or b.write_id)
                last_seq = int(entry["last_seq"])
                # 是否「另一个批次」复用同一幂等键（真正的跨批次重放）：
                # 同一批次在多数确认超时后的重试不算 replay。
                cross_batch = entry.get("batch_id") not in (None, b.batch_id)
                replay = cross_batch
                if b.status != COMMITTED:
                    # 幂等登记已落盘但本批次对象尚未标记 committed（典型：
                    # 上一调用本地提交点已过但多数确认超时，或重启后补记）。
                    first_seq = int(entry["first_seq"])
                    if self.commit.commit_index >= last_seq:
                        self.batches.mark_committed(b, entry, replay=replay)
                        return self._batch_result(b, replay=replay, committed=True)
                    return self._continue_batch_locked(
                        b, wid, term, first_seq, last_seq,
                        timeout, wait_commit, transport, replay=replay)
                if self.commit.commit_index >= last_seq:
                    # 同一幂等键相同内容且已越提交水位：返回首次提交结果
                    return self._batch_result(b, replay=True, committed=True)
                # 已登记但未越水位：继续等待同一提交过程
                return self._continue_batch_locked(
                    b, wid, term, int(entry["first_seq"]), last_seq,
                    timeout, wait_commit, transport, replay=True)
            wid = self._validate_write_id(write_id or b.write_id)
            if b.status == COMMITTING:
                # 多数派确认期间客户端重试（含进程重启后本地已整批持久化）：
                # 继续等待同一 write_id 的同一提交过程，绝不追加记录。
                if b.write_id and b.write_id != wid:
                    raise err(409, "write_id_conflict",
                              "batch is already committing under another write_id",
                              batch_id=b.batch_id, write_id=b.write_id)
                first_seq = b.first_seq
                last_seq = b.last_seq
                return self._continue_batch_locked(
                    b, wid, term, first_seq, last_seq, timeout, wait_commit,
                    transport, replay=False)
            # 同 write_id 已用于另一提交：明确冲突，不允许混用
            existing = self.commit.get_write(wid)
            if existing is not None and existing.get("kind") == "write":
                raise err(409, "write_id_conflict",
                          "write_id already used by a different write",
                          write_id=wid)
            # 新鲜提交：committing 落盘 -> 整批入链 -> 幂等登记 -> 等多数派
            first_seq = self.seglog.next_seq
            b.write_id = wid
            self.batches.mark_committing(b, first_seq, content_hash, wid)
            self._crash_hook("batch_after_mark")
            try:
                for op in b.ops:
                    # 直接本地追加：批量提交在锁内串行，等待多数派在锁外
                    Reducer().apply(op["type"], op["payload"])
                    self._append_record_locked(op["type"], op["payload"])
                last_seq = first_seq + len(b.ops) - 1
            except Exception:
                # 运行期失败同样整批回滚，不留半批
                self._rollback_uncommitted(b, first_seq)
                raise
            self._crash_hook("batch_after_append")
            self.commit.register_write(wid, term, first_seq, last_seq,
                                       kind="batch", batch_id=b.batch_id)
            self.commit.add_proposal(Proposal(
                first_seq, last_seq, term=term, kind="batch",
                write_id=wid, batch_id=b.batch_id))
            self.commit.persist()
            self._advance_commit_locked()
            return self._continue_batch_locked(
                b, wid, term, first_seq, last_seq, timeout, wait_commit,
                transport, replay=False)

    def _continue_batch_locked(self, b: Batch, wid: str, term: int,
                               first_seq: int, last_seq: Optional[int],
                               timeout: int, wait_commit: bool,
                               transport: Optional[Callable],
                               replay: bool = False) -> dict:
        """本地记录已就位后：登记提交点（一旦多数确认即可见）并等待水位。

        replay=True 表示同一幂等键的另一次提交在复用首次序号区间，
        提交结果返回 replay=True，但首次提交结果不变。
        """
        last_seq = last_seq if last_seq is not None else b.last_seq
        content_hash = b.content_hash
        key = b.registry_key()
        if self.batches.commit_entry(key) is None:
            reg = {
                "key": key,
                "batch_id": b.batch_id,
                "content_hash": content_hash,
                "first_seq": first_seq,
                "last_seq": last_seq,
                "record_count": (last_seq - first_seq + 1) if last_seq else 0,
                "committed_ts": now_ms(),
            }
            self.batches.register_commit(key, reg)  # 幂等登记（本地提交点）
            self._crash_hook("batch_after_register")
        if self.commit.commit_index >= (last_seq or 0):
            entry = self.batches.commit_entry(key)
            self.batches.mark_committed(b, entry, replay=replay)
            return self._batch_result(b, replay=replay, committed=True)
        if not wait_commit:
            return self._batch_pending(b, wid)
        # 锁外等待多数派推进提交水位（批记录保持提交前不可见语义）
        deadline = now_ms() + timeout
        while True:
            with self.meta_lock:
                self._advance_commit_locked()
                if self.commit.commit_index >= (last_seq or 0):
                    entry = self.batches.commit_entry(key)
                    self.batches.mark_committed(b, entry, replay=replay)
                    return self._batch_result(b, replay=replay, committed=True)
                if self.cluster.term != term or self.cluster.role != PRIMARY:
                    raise err(409, "commit_superseded",
                              "term changed while waiting for the batch commit; "
                              "retry with the same write_id to continue",
                              term=term, current_term=self.cluster.term,
                              write_id=wid, first_seq=first_seq, last_seq=last_seq)
                if not self.cluster.grant_valid():
                    raise err(403, "grant_expired",
                              "grant expired before the batch reached quorum",
                              term=term, write_id=wid,
                              first_seq=first_seq, last_seq=last_seq)
                remaining = deadline - now_ms()
                if remaining <= 0:
                    raise err(504, "commit_timeout",
                              "batch persisted locally but not confirmed by a "
                              "majority before the timeout; retry with the same "
                              "write_id to continue the same commit",
                              term=term, write_id=wid,
                              first_seq=first_seq, last_seq=last_seq,
                              commit_index=self.commit.commit_index)
            with self._commit_cv:
                self._commit_cv.wait(min(1.0, remaining / 1000))

    def _batch_pending(self, b: Batch, wid: str) -> dict:
        return {
            "batch_id": b.batch_id, "idempotency_key": b.idempotency_key,
            "status": "committing", "replay": False,
            "write_id": wid, "first_seq": b.first_seq, "last_seq": b.last_seq,
            "record_count": b.record_count, "committed": False,
        }

    def _truncate_tail_chain(self, keep_seq: int, anchor: Optional[str] = None) -> None:
        """截断链尾到 keep_seq，并把提交水位/待定提议/write 登记一起回退。

        提交水位绝不可以指向已不存在的记录；被截断的本地未提交写入（批次
        回滚等）保持其 write_id 登记但序号区间不再有效，重试会追加新记录。
        """
        if self.seglog.tip()[0] > keep_seq:
            self.seglog.truncate_tail(keep_seq)
            self._sync_tip(anchor if anchor is not None else
                           (self.checkpoints.current["tail_anchor"]
                            if self.checkpoints.current else GENESIS))
            self._persist_head()
        if self.commit.commit_index > keep_seq:
            self.commit.commit_index = keep_seq
            self.commit.commit_digest = self._digest_or_anchor(keep_seq) or GENESIS
        for wid, e in self.commit.writes.items():
            if e.get("status") == "pending" and int(e["first_seq"]) > keep_seq:
                self.commit.mark_write_superseded(wid, self.cluster.term)
        self.commit.prune_pending_through(keep_seq)
        self.commit.persist()

    def _rollback_uncommitted(self, b: Batch, first_seq: int) -> None:
        """整批回滚：截断链尾到批次之前，校准链尖与链头锚点，批次回到 open。

        批次记录尚未取得多数确认（整批原子，水位不会落在批次内部），
        提交水位无需调整；write 登记标记为已取代。
        """
        keep = first_seq - 1
        if self.seglog.tip()[0] > keep:
            self.seglog.truncate_tail(keep)
            pointer = self.checkpoints.current
            self._sync_tip(pointer["tail_anchor"] if pointer else GENESIS)
            self._persist_head()
        if b.write_id:
            e = self.commit.get_write(b.write_id)
            if e is not None and e.get("status") == "pending":
                self.commit.mark_write_superseded(b.write_id, self.cluster.term)
        self.commit.prune_pending_through(keep)
        self.commit.persist()
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

    def _batch_result(self, b: Batch, replay: bool,
                      committed: Optional[bool] = None) -> dict:
        out = {
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
        if committed is not None:
            out["committed"] = committed
            if committed:
                out["commit_index"] = self.commit.commit_index
        return out

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
        """返回（受保护最老段, 可回收 sealed 段列表）。

        除读者钉位外，段的最后一条记录必须已进入提交水位：绝不压缩
        尚未取得多数派确认的待定记录（含旧主半提交尾部）。
        """
        oldest = self.readers.oldest_pin()
        protected = self.seglog.segment_for_pin(oldest) if oldest is not None else None
        commit_seq = self.commit.commit_index
        cands = []
        for seg_id, meta in sorted(self.seglog.segments.items()):
            if not meta.sealed:
                continue
            if meta.last_seq > commit_seq:
                continue  # 段内含待定记录：绝不回收
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
                "commit_index": self.commit.commit_index,
                "commit_digest": self.commit.commit_digest,
                "checkpoint": (
                    None if pointer is None else
                    {"gen": pointer["gen"], "seq": pointer["seq"],
                     "tail_anchor": pointer["tail_anchor"],
                     "state_digest": pointer["state_digest"],
                     "snapshot_digest": pointer["snapshot_digest"],
                     "manifest_digest": pointer["manifest_digest"]}),
                # 预约元数据随边界带给备实例，使新主提升后能接管未完成预约
                "schedules": self.schedules.snapshot(),
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
                "commit_index": self.commit.commit_index,
                "commit_digest": self.commit.commit_digest,
                # 主实例的待定提议表（含任期标记）：备实例据此知道提交水位
                # 只能在哪些边界推进，原子批次不会被水位拆成两半。
                "pending": [p.to_dict() for p in self.commit.pending],
                "term_markers": self.commit.term_marker_view(),
                "schedules": self.schedules.snapshot(),
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
                # 快照边界可能已经越过来源提交水位之前；备实例安装后从
                # min(边界, 来源水位) 起步，任期标记位置一并携带。
                "commit_index": self.commit.commit_index,
                "commit_digest": self.commit.commit_digest,
                "term_markers": self.commit.term_marker_view(),
                "pending": [p.to_dict() for p in self.commit.pending],
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
                # 预约元数据即使没有新记录也要随边界对账（备提升后能接管）
                if isinstance(bnd.get("schedules"), dict):
                    self._merge_primary_schedules(bnd["schedules"])

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
                    # 快照安装后立即把确认位置回报给主（推动多数派提交）
                    self._follower_ack_primary(transport)

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
                applied = self.apply_records(page, transport=transport)
                new_status = (CAUGHT_UP if self.replica.synced_seq >= bnd["tip_seq"]
                              else SYNCING)
                self.replica.set_status(new_status)
                return {"installed_snapshot": installed,
                        "applied": applied,
                        "commit_index": self.commit.commit_index,
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

    def apply_records(self, page: dict,
                      transport: Optional[Callable] = None,
                      ack_back: bool = True) -> int:
        """顺序、去重、摘要链校验地应用一页来源记录。

        - seq <= synced 的重复段：逐条与本地摘要比对，相同跳过（不重复应用），
          不同即冲突；
        - seq > synced：必须 seq == synced+1 且 prev 衔接、重算摘要一致，
          否则停在 replication_conflict，绝不静默覆盖。
        ack_back=False 用于主主动推送：确认位置通过 RPC 响应返回，不再回拉 ack。
        """
        recs = page.get("records", [])
        if not recs:
            if self.replica.synced_seq < int(page.get("tip_seq", self.replica.synced_seq)):
                # 来源有更新但拿不到下一条（已压缩）：下轮装快照，不算冲突
                return 0
            # 没有新记录：仍跟随来源提交水位（可能全部是旧页重放）
            self._follower_follow_page(page, applied=0,
                                       transport=transport)
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
                    # 已确认历史分叉：冲突冻结（绝不静默覆盖已确认位置）
                    self._raise_conflict(seq, local, rd, "duplicate segment diverges")
                if local is None and seq > 0:
                    # 已确认位置之前却没有记录（压缩空洞）：以边界凭证为准，跳过
                    pass
                skipped += 1
                continue
            if seq != expect_seq:
                # 空洞（本地有未提交分叉、来源从更早位置给页）：安全裁剪后重试
                if self._trim_uncommitted_prefix(r):
                    self._follower_follow_page(page, applied=0, imported=False,
                                               transport=transport)
                    return self.apply_records(page, transport=transport, ack_back=ack_back)
                self._raise_conflict(
                    seq, None, rd,
                    "source seq is not contiguous with confirmed position")
            if r.get("prev") != expect_anchor:
                # 本地未提交尾部与来源分叉：裁掉到提交水位之间的本地后缀，
                # 只要匹配点仍在提交水位之后即可安全续接，不冻结复制。
                if self._trim_uncommitted_prefix(r):
                    self._follower_follow_page(page, applied=0, imported=False,
                                               transport=transport)
                    return self.apply_records(page, transport=transport, ack_back=ack_back)
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
        self._follower_follow_page(page, applied=applied,
                                       transport=transport if ack_back else None)
        return applied

    def _trim_uncommitted_prefix(self, incoming: dict) -> bool:
        """备实例本地未提交尾部与来源分叉时，安全裁剪到共同前缀。

        只允许裁掉严格高于本地提交水位（commit_index）的本地后缀；
        共同前缀由来源记录的 prev 摘要在本地链上定位。任何裁剪触及
        已提交记录都返回 False（调用方进入冲突冻结）。
        """
        seq = int(incoming["seq"])
        want_prev = incoming.get("prev")
        commit_seq = self.commit.commit_index
        # 来源记录序号不能落在已提交水位之内（那种分叉必须冻结）
        if seq <= commit_seq:
            return False
        anchor_seq = seq - 1
        local_anchor = self._digest_or_anchor(anchor_seq)
        if local_anchor is None or want_prev != local_anchor:
            # 来源 prev 在本地链上找不到一致位置：裁到提交水位，交给外层重拉
            target = commit_seq
        else:
            target = anchor_seq
        if target < commit_seq:
            return False
        # 同步游标从未前滚（旧主刚交接，synced=0）但本地链与来源一致：
        # 不截断，只把确认位置前滚到共同前缀。
        if target == self.seglog.tip()[0] and self.replica.synced_seq == 0:
            d = self._digest_or_anchor(target) or GENESIS
            self.replica.advance(target, d, status=SYNCING)
            return True
        if target >= self.seglog.tip()[0]:
            return False
        self.seglog.truncate_tail(target)
        pointer = self.checkpoints.current
        self._sync_tip(pointer["tail_anchor"] if pointer else GENESIS)
        self._persist_head()
        # 复制进度回到裁剪点（仍 >= commit_index，绝不丢掉已确认位置）
        d = self._digest_or_anchor(target) or GENESIS
        self.replica.advance(target, d, status=SYNCING)
        return True

    def _follower_follow_page(self, page: dict, applied: int,
                              imported: bool = True,
                              transport: Optional[Callable] = None) -> None:
        """备实例应用一页后：合并主的待定提议/任期标记、跟随提交水位、回报。"""
        self._merge_primary_meta(page)
        leader_commit = int(page.get("commit_index", self.commit.commit_index))
        if leader_commit > self.commit.commit_index:
            self._follower_advance_commit(leader_commit)
        # 向主报告已确认位置（持久化 + 推动主的多数派提交）
        self._follower_ack_primary(transport)

    def _merge_primary_meta(self, page: dict) -> None:
        """合并来源主的待定提议表与任期标记位置（备实例不自行决定归属）。"""
        for m in page.get("term_markers", []) or []:
            self.commit.add_term_marker(int(m["seq"]), int(m["term"]))
        pending = page.get("pending")
        if isinstance(pending, list):
            self.commit.replace_pending([Proposal.from_dict(p) for p in pending])
        self.commit.persist()
        # 预约元数据随复制页对账（备只读镜像，提升后据此接管未完成预约）
        if isinstance(page.get("schedules"), dict):
            self._merge_primary_schedules(page["schedules"])

    def _merge_primary_schedules(self, snap: dict) -> None:
        changed = self.schedules.merge_primary(snap, now=self._now())
        if not changed:
            return
        # 重建执行中预约的跨度索引
        self._schedule_spans = {}
        for sc in self.schedules.schedules.values():
            if sc.status in ACTIVE and sc.first_seq is not None \
                    and sc.last_seq is not None:
                self._schedule_spans[(int(sc.first_seq), int(sc.last_seq))] = sc.request_id

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
            # 即使边界幂等，也跟随来源提交水位与任期标记（可能本轮才到达）
            source_commit = int(snap.get("commit_index", pointer["seq"]))
            target = min(source_commit, pointer["seq"])
            if target > self.commit.commit_index:
                self.commit.set_commit(
                    target, int(snap.get("term", self.cluster.term)),
                    pointer["tail_anchor"] if target >= pointer["seq"]
                    else (self._digest_or_anchor(target) or GENESIS))
            for m in snap.get("term_markers", []) or []:
                self.commit.add_term_marker(int(m["seq"]), int(m["term"]))
            self.commit.persist()
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
        # ---- 提交水位随快照边界安装：备实例只能采纳 min(边界, 来源水位) ----
        source_commit = int(snap.get("commit_index", seq))
        new_commit = min(seq, source_commit)
        # 重装边界可能从更小位置重来：以新边界为准（旧边界整段被来源覆盖）
        self.commit.commit_index = new_commit
        self.commit.commit_term = int(snap.get("term", self.cluster.term))
        self.commit.commit_digest = (
            new_pointer["tail_anchor"] if new_commit >= seq
            else (self._digest_or_anchor(new_commit) or GENESIS))
        # 任期标记位置随边界一并安装
        for m in snap.get("term_markers", []) or []:
            self.commit.add_term_marker(int(m["seq"]), int(m["term"]))
        # 边界之后的待定提议以来源导出为准（边界之内的提议已全部提交）
        self.commit.replace_pending([
            Proposal.from_dict(p) for p in (snap.get("pending") or [])
            if int(p["first_seq"]) > new_commit])
        self.commit.persist()

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
            # 投票方给出的「与候选同一条记录」证明：候选的 advertised
            # tip 在投票方链上原样存在 -> 可证到 tip；否则只可证到投票方
            # 自己的提交水位（多数派交集论证，见就任后的前缀裁剪）。
            attested = [last_log_seq]  # 自己证明自己的整条链

        # ---- 阶段 1（锁外 RPC）：向其他成员拉票，自选票已落盘 ----
        refusals: list[dict] = []
        for url in peer_urls:
            try:
                _st, resp = transport("POST", url.rstrip("/") + PATH_REQUEST_VOTE, body)
                if resp.get("vote_granted"):
                    votes += 1
                    attested.append(int(resp.get("attest_seq",
                                                 resp.get("commit_seq", -1))))
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
            # ---- 多数派可证前缀：只保留能由多数派证明的日志前缀 ----
            # attested 中第 need 大的位置即多数派共同持有的最靠后位置；
            # 其后的半提交尾部是旧主可能独占的记录，绝不暴露、必须裁掉。
            attested.sort(reverse=True)
            proven_seq = attested[need - 1] if attested else 0
            proven_seq = max(proven_seq, self.commit.commit_index)
            truncated = self._truncate_for_new_leadership(proven_seq, new_term)
            self.cluster.assume_leadership(new_term, ttl)
            # 新任期先提交一条任期标记：旧任期待定记录不能仅凭新主自己的
            # 副本变已提交，必须等它随当前任期记录一起越过提交水位。
            marker_seq = self._append_term_marker(new_term)
            # 提升成功：停止跟随来源（后台复制线程检测到角色后自动退出）
            self.replica.peer_url = None
            if self.replica.status not in (CONFLICT,):
                self.replica.set_status(CAUGHT_UP)
            self.checkpoints.append_audit({
                "kind": "promoted", "term": new_term,
                "last_log_seq": last_log_seq, "grant_ttl_ms": ttl,
                "votes": votes, "needed": need,
                "proven_seq": proven_seq, "truncated_to": truncated,
                "term_marker_seq": marker_seq,
            })
            return {
                "term": new_term, "role": PRIMARY,
                "votes": votes, "needed": need,
                "grant_expires_at": self.cluster.s.grant_expires_at,
                "last_log_seq": last_log_seq,
                "proven_seq": proven_seq,
                "truncated_from": last_log_seq,
                "truncated_to": truncated,
                "term_marker_seq": marker_seq,
            }

    def _truncate_for_new_leadership(self, proven_seq: int, new_term: int) -> int:
        """就任前把本地日志裁到「多数派可证前缀」，丢弃旧主半提交尾部。

        - 已进入提交水位的记录绝不回退（proven_seq 必 >= commit_index）；
        - 超出 proven_seq 的记录从链尾截断，关联的 write_id 标记为
          superseded（同 write_id 重试得到明确错误，不追加重复记录）；
        - 原子批次若部分位于可证前缀之外，整批回滚到 open；
        - 截断后重建待定提议，但不推进提交水位（旧任期尾部仍须等
          当前任期的 term_marker 先被多数派确认）。
        """
        tip_seq, _ = self.seglog.tip()
        target = min(proven_seq, tip_seq)
        if target < tip_seq:
            self.seglog.truncate_tail(target)
            pointer = self.checkpoints.current
            self._sync_tip(pointer["tail_anchor"] if pointer else GENESIS)
            self._persist_head()
        # 超出可证前缀的 write_id：标记被新任期取代
        for wid, e in list(self.commit.writes.items()):
            if e.get("status") == "pending" and int(e["first_seq"]) > target:
                self.commit.mark_write_superseded(wid, new_term)
        # 原子批次：任何记录超出可证前缀即整批回到 open（不拆开批次）
        for b in list(self.batches.batches.values()):
            if b.status == COMMITTING and (b.first_seq or 0) > 0 \
                    and (b.last_seq or b.first_seq) > target:
                wid = b.write_id
                if wid and self.commit.get_write(wid):
                    self.commit.mark_write_superseded(wid, new_term)
                self.batches.rollback_to_open(b)
        # schedule：执行中整组任何记录超出可证前缀 -> 纪元前滚回 pending，
        # 由新主沿用原请求标识在新位置重做（旧 write_id 已 superseded）。
        for sc in list(self.schedules.schedules.values()):
            if sc.status == EXECUTING and sc.first_seq is not None \
                    and int(sc.first_seq) > target:
                if sc.write_id and self.commit.get_write(sc.write_id):
                    self.commit.mark_write_superseded(sc.write_id, new_term)
                if sc.first_seq is not None and sc.last_seq is not None:
                    self._schedule_spans.pop(
                        (int(sc.first_seq), int(sc.last_seq)), None)
                self._reset_schedule_for_retry(sc, reason="truncated_by_new_term")
        self.commit.prune_pending_through(target)
        self.commit.commit_index = min(self.commit.commit_index, target)
        self.commit.commit_digest = self._digest_or_anchor(self.commit.commit_index) or GENESIS
        self._rebuild_pending_locked()
        return target

    def _append_term_marker(self, term: int) -> int:
        """新主任期开始：写入一条内部 term_marker 记录（当前任期首条记录）。"""
        seq = self.seglog.next_seq
        rec = self._append_record_locked(
            "data", {"kind": "term_marker", "term": term})
        self.commit.add_term_marker(rec["seq"], term)
        self.commit.add_proposal(Proposal(
            rec["seq"], rec["seq"], term=term, kind="term_marker"))
        self.commit.persist()
        return seq

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
            # 多数派可证前缀的证据：候选 advertised 位置的记录在本节点
            # 链上原样存在 -> 可证到候选 tip；否则只可证到本地提交水位。
            if last_seq > tip_seq:
                cand_digest = req.get("last_log_digest")
                if cand_digest and self._digest_or_anchor(last_seq) == cand_digest:
                    attest = last_seq
                else:
                    attest = self.commit.commit_index
            else:
                attest = last_seq  # last_seq == tip_seq（已等长比对摘要）
            return {"term": term, "vote_granted": True, "reason": "ok",
                    "attest_seq": attest, "commit_seq": self.commit.commit_index}

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
        """读原始事件：默认只展示不超过提交水位 commit_index 的内容。"""
        with self.meta_lock:
            tip, _ = self.seglog.tip()
            commit_seq = self.commit.commit_index
            pointer = self.checkpoints.current
            low = pointer["seq"] + 1 if pointer else 1
            if start_seq < low:
                raise err(410, "compacted",
                          f"seq {start_seq} already compacted; readable head is {low}",
                          readable_from=low, checkpoint_gen=pointer["gen"] if pointer else 0)
            if start_seq > commit_seq:
                # 请求位置尚在待定（本地已写但未取得多数确认）：不暴露，
                # 返回空页并附带提交水位（调用方可知何时再读）。
                return {
                    "from": start_seq, "tip": tip, "commit_index": commit_seq,
                    "records": [], "next": start_seq, "has_more": False,
                    "ahead_of_commit": True,
                }
            limit = min(limit, max(0, commit_seq - start_seq + 1))
            recs = self.seglog.read_records(start_seq, limit) if limit else []
            return {
                "from": start_seq, "tip": tip, "commit_index": commit_seq,
                "records": recs,
                "next": (recs[-1]["seq"] + 1) if recs else start_seq,
                "has_more": bool(recs and recs[-1]["seq"] < commit_seq),
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

        默认只折叠不超过提交水位 commit_index 的记录：本地已写但尚未取得
        多数确认的待定记录不会出现在业务状态里。
        checkpoint_state_* 是快照边界（snapshot_seq 处）的状态；
        current_state_* 是再叠加现存提交尾部后的最新业务状态。
        """
        with self.meta_lock:
            return self._committed_state_view()

    def _committed_state_view(self, term: Optional[int] = None,
                              barrier: bool = False) -> dict:
        pointer = self.checkpoints.current
        base = self._load_state(pointer) if pointer else {}
        commit_seq = self.commit.commit_index
        r = Reducer(base)
        for sid in sorted(self.seglog.segments):
            for rec in self.seglog.iter_segment(sid):
                if rec["seq"] > commit_seq:
                    break
                r.apply(rec["type"], rec["payload"])
        tip_seq, _ = self.seglog.tip()
        view = {
            "state": r.snapshot(),
            "current_state_digest": r.digest(),
            "commit_index": commit_seq,
            "tip_seq": tip_seq,
            "pending": self.commit.pending_ranges(),
            "role": self.cluster.role,
            "term": self.cluster.term,
            "checkpoint": (
                {"gen": pointer["gen"], "snapshot_seq": pointer["seq"],
                 "state_digest": pointer["state_digest"], "tail_anchor": pointer["tail_anchor"]}
                if pointer else None),
        }
        if barrier:
            view["read_barrier"] = {"term": term, "verified": True}
        return view

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
                # ---- 多数派提交水位 ----
                "commit_index": self.commit.commit_index,
                "commit_term": self.commit.commit_term,
                "commit_digest": self.commit.commit_digest,
                "pending": self.commit.pending_ranges(),
                "member_acks": self._member_ack_view(),
                "primary": (self.replica.peer_url if v["role"] != PRIMARY else None),
            }

    def _member_ack_view(self) -> dict:
        """各成员确认位置：本地 tip（主）/ 已同步位置（备）+ 收到的成员 ack。"""
        view: dict[str, Any] = {}
        tip_seq, tip_digest = self.seglog.tip()
        if self.cluster.role == PRIMARY:
            view[self.cluster.node_id] = {
                "match_seq": tip_seq, "commit_seq": self.commit.commit_index,
                "term": self.cluster.term, "self": True}
        else:
            view[self.cluster.node_id] = {
                "match_seq": self.replica.synced_seq,
                "commit_seq": self.commit.commit_index,
                "term": self.cluster.term, "self": True}
        for nid, a in self.commit.ack_view().items():
            view[nid] = a
        return view

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
                "commit_index": self.commit.commit_index,
                "commit_term": self.commit.commit_term,
                "pending": self.commit.pending_ranges(),
                "member_acks": self._member_ack_view(),
                "schedules": self.schedules.counts(),
                "data_dir": self.cfg.data_dir,
            }
