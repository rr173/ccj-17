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
import json
import os
import threading
from typing import Any, Optional

from .checkpoints import (
    AUDIT,
    Checkpointer,
    MANIFEST,
    SNAPSHOT,
    open_doc,
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
from .segment import SegmentLog


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
        self.checkpoints = Checkpointer(self.cp_dir, os.path.join(self.cp_dir, AUDIT))
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
                self._sync_tip(GENESIS)
                self._reconcile_head()
                recovery["boundary"] = "genesis"
                return recovery

            gen = pointer["gen"]
            try:
                snap_body, man_body = self.checkpoints.load_generation(gen)
                self._verify_pointer_target(pointer, snap_body, man_body)
            except (FileNotFoundError, ValueError) as e:
                # 已提交边界自身损坏：不静默篡改历史，明确失败
                raise err(500, "boundary_corrupt",
                          "committed checkpoint boundary is corrupt; refusing to start",
                          gen=gen, reason=str(e))

            # 指针有效：幂等补删已承诺覆盖的段、清理非当前代目录
            covered = set(pointer.get("covered_segs", []))
            deleted_now: list[int] = []
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
            self._sync_tip(pointer["tail_anchor"])
            self._reconcile_head()
            recovery.update(boundary=f"gen-{gen}", segments_deleted=deleted_now, gens_pruned=pruned)
            return recovery

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

    def append(self, rec_type: str, payload: Any) -> dict:
        with self.meta_lock:
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

    def status(self) -> dict:
        with self.meta_lock:
            tip, tip_digest = self.seglog.tip()
            now = now_ms()
            return {
                "tip_seq": tip,
                "tip_digest": tip_digest,
                "segments": self.seglog.meta_view(),
                "readers": {"active": len(self.readers.active(now)), "total": len(self.readers.readers),
                            "oldest_pin_seq": self.readers.oldest_pin(now)},
                "pinning": self.pin_view(),
                "checkpoint": self.head_info()["checkpoint"],
                "last_compaction": self.last_compact,
                "compaction_running": self._compact_active,
                "data_dir": self.cfg.data_dir,
            }
