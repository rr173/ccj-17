"""pytest 套件：覆盖追加、哈希链、租约钉位、压缩等价、
校验失败回滚、并发压缩、崩溃恢复、篡改检测与 HTTP 接口。

运行：python -m pytest -q
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode

import pytest

from app.checkpoints import MANIFEST, SNAPSHOT, open_doc
from app.common import BusyError, Error, canonical, digest_json, read_json, sha256_file, write_json
from app.kernel import Config, Kernel
from app.readers import ReaderStore
from app.reducer import Reducer
from app.segment import record_digest, seg_name


# --------------------------------------------------------------------- 基础


def make_kernel(tmp_path, **kw) -> Kernel:
    cfg = Config(
        data_dir=str(tmp_path),
        segment_bytes=kw.pop("segment_bytes", 10_000_000),
        janitor_enabled=False,
        compaction_min_segments=kw.pop("compaction_min_segments", 1),
        default_ttl_ms=kw.pop("default_ttl_ms", 30_000),
        crash_hook=kw.pop("crash_hook", None),
        **kw,
    )
    k = Kernel(cfg)
    k.startup()
    return k


def seed(k: Kernel, n: int, rotate_at=()) -> None:
    for i in range(n):
        k.append("put", {"key": f"k{i % 3}", "value": i})
        if i in rotate_at:
            k.seglog.rotate()


def expected_state(n: int) -> dict:
    s = {}
    for i in range(n):
        s[f"k{i % 3}"] = i
    return s


# --------------------------------------------------------------------- 追加/哈希链


class TestAppendChain:
    def test_append_returns_chain_digests(self, tmp_path):
        k = make_kernel(tmp_path)
        a = k.append("put", {"key": "x", "value": 1})
        b = k.append("put", {"key": "y", "value": 2})
        assert a["seq"] == 1 and a["prev"] == "0" * 64
        assert b["seq"] == 2 and b["prev"] == a["digest"]
        assert b["digest"] == record_digest({"seq": 2, "ts": b["ts"], "type": "put",
                                             "payload": {"key": "y", "value": 2},
                                             "prev": a["digest"]})

    def test_rotation_preserves_chain(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 9, rotate_at=(2, 5))
        vr = k.seglog.verify_chain()
        assert vr.ok and vr.records == 9
        assert [m.seg_id for m in k.seglog.segments.values()] == [1, 2, 3]
        assert k.seglog.segments[1].sealed and k.seglog.segments[2].sealed
        assert not k.seglog.segments[3].sealed

    def test_tail_torn_line_truncated_on_scan(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 3)
        with open(k.seglog.segments[k.seglog.active_id].path, "ab") as f:
            f.write(b'{"seq":4,"ts":1,"typ')  # 崩溃留下的半行
            f.flush()
            os.fsync(f.fileno())
        k2 = make_kernel(tmp_path)
        assert k2.seglog.tip()[0] == 3
        rec = k2.append("data", {"note": "ok"})
        assert rec["seq"] == 4  # 半行被丢弃后正常续写

    def test_append_validates_payload(self, tmp_path):
        k = make_kernel(tmp_path)
        with pytest.raises(Error) as e:
            k.append("put", {"key": "x"})
        assert e.value.status == 400


# --------------------------------------------------------------------- 租约钉位


class TestLeases:
    def test_register_heartbeat_release(self, tmp_path):
        k = make_kernel(tmp_path, default_ttl_ms=1100)
        seed(k, 2)
        rd = k.register_reader(1, 1100, None)
        assert rd["alive"] and rd["remaining_ms"] > 0
        time.sleep(1.25)
        with pytest.raises(Error) as e:
            k.heartbeat(rd["reader_id"], None, None)
        assert e.value.status == 410
        rd2 = k.register_reader(1, 5000, "bob")
        k.heartbeat("bob", 5000, 5)
        assert k.readers.get("bob", 0).position == 5
        k.release_reader("bob")
        assert k.pin_view()["oldest_pin_seq"] is None

    def test_position_cannot_move_before_pin(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 12)
        k.register_reader(10, 5000, "slow")
        with pytest.raises(Error) as e:
            k.heartbeat("slow", 5000, 3)
        assert e.value.status == 400

    def test_duplicate_reader_rejected(self, tmp_path):
        k = make_kernel(tmp_path)
        k.register_reader(1, 5000, "dup")
        with pytest.raises(Error) as e:
            k.register_reader(1, 5000, "dup")
        assert e.value.status == 409

    def test_pin_beyond_tip_rejected(self, tmp_path):
        k = make_kernel(tmp_path)
        with pytest.raises(Error) as e:
            k.register_reader(999, 5000, None)
        assert e.value.status == 400

    def test_ttl_bounds(self, tmp_path):
        k = make_kernel(tmp_path)
        with pytest.raises(Error):
            k.register_reader(1, 10, None)
        with pytest.raises(Error):
            k.register_reader(1, 99_999_999_999, None)

    def test_pin_protects_its_segment_and_after(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 12, rotate_at=(2, 5, 8))  # 段:1[1-3] 2[4-6] 3[7-9] 4[10-12]
        pin = k.seglog.segments[2].first_seq  # seq4 -> 保护 seg2 起
        k.register_reader(pin, 5000, "a")
        view = k.pin_view()
        assert view["protected_segment"] == 2
        assert view["reclaimable_segments"] == [1]
        assert view["reclaimable_range"]["first_seq"] == 1
        assert view["reclaimable_range"]["last_seq"] == 3

    def test_multiple_readers_minimum_pin_wins(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 12, rotate_at=(2, 5, 8))
        k.register_reader(7, 5000, "later")   # seg3
        k.register_reader(4, 5000, "earlier")  # seg2 -> 更老
        assert k.pin_view()["oldest_pin_seq"] == 4
        assert k.pin_view()["protected_segment"] == 2
        k.release_reader("earlier")
        assert k.pin_view()["protected_segment"] == 3

    def test_leases_survive_restart(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 4)
        k.register_reader(2, 60_000, "persist")
        k2 = make_kernel(tmp_path, default_ttl_ms=60_000)
        assert k2.pin_view()["oldest_pin_seq"] == 2


# --------------------------------------------------------------------- 压缩等价性


class TestCompaction:
    def test_compact_first_generation_equivalence(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 12, rotate_at=(3, 7))
        full = Reducer()
        for sid in sorted(k.seglog.segments):
            for r in k.seglog.iter_segment(sid):
                full.apply(r["type"], r["payload"])
        res = k.compact(force=True)
        assert res["status"] == "ok" and res["gen"] == 1
        assert res["equivalence_proof"]["equivalent"] is True
        assert res["equivalence_proof"]["full_replay_digest"] == full.digest()
        bs = k.business_state()
        assert bs["state"] == expected_state(12)
        assert bs["current_state_digest"] == full.digest()

    def test_compacted_returns_410_and_head_reads_tail(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 8, rotate_at=(3,))
        k.compact(force=True)
        with pytest.raises(Error) as e:
            k.read(1, 10)
        assert e.value.status == 410
        from_seq = k.head_info()["readable_from"]
        recs = k.read(from_seq, 100)["records"]
        assert [r["seq"] for r in recs] == list(range(from_seq, 9))

    def test_chained_generations(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 6, rotate_at=(1,))
        r1 = k.compact(force=True)
        k.seglog.rotate()
        for i in range(6, 10):
            k.append("delete", {"key": f"k{i % 3}"})
        k.seglog.rotate()
        k.append("put", {"key": "fresh", "value": "v"})
        r2 = k.compact(force=True)
        assert r1["status"] == r2["status"] == "ok" and r2["gen"] == 2
        assert r2["equivalence_proof"]["equivalent"]
        st = k.business_state()["state"]
        # 三个 key 依次被 delete 过，只剩 fresh
        assert st == {"fresh": "v"}
        v = k.verify_latest()
        assert v["status"] == "ok" and all(v["checks"].values())
        # 旧代目录已清理，只有当前代
        gens = [n for n in os.listdir(k.checkpoints.dir) if n.startswith("gen-")]
        assert gens == ["gen-2"]

    def test_threshold_skips_and_force_overrides(self, tmp_path):
        k = make_kernel(tmp_path, compaction_min_segments=4)
        seed(k, 4, rotate_at=(1,))
        r = k.compact()
        assert r["status"] == "skipped"
        assert k.compact(force=True)["status"] == "ok"

    def test_noop_when_no_sealed(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 3)
        assert k.compact(force=True)["status"] == "noop"

    def test_pin_blocks_compaction_before_it(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 12, rotate_at=(2, 5, 8))
        k.register_reader(k.seglog.segments[2].first_seq, 5000, "a")  # 保护 seg2+
        r = k.compact(force=True)
        assert r["status"] == "ok" and r["covered_segments"] == [1]
        # 再次：钉点之前没有更多 sealed
        assert k.compact(force=True)["status"] == "noop"

    def test_result_is_persisted_and_queryable(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 4, rotate_at=(1,))
        k.compact(force=True)
        assert read_json(k.result_path)["status"] == "ok"
        assert k.last_compact["gen"] == 1

    def test_attestations_trace_to_raw_segments(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 6, rotate_at=(2,))
        # 先记下原始段文件指纹
        files = {sid: sha256_file(m.path) for sid, m in k.seglog.segments.items() if m.sealed}
        k.compact(force=True)
        _, mb = k.checkpoints.load_generation(1)
        for att in mb["segments"]:
            assert att["file_sha256"] == files[att["seg"]]
            assert digest_json(att["record_digests"]) == att["records_root"]
            assert len(att["record_digests"]) == att["count"]
        # 审计链保留 manifest 摘要，段删了仍可追溯
        audit = k.checkpoints.read_audit()
        assert audit[-1]["manifest_digest"] == digest_json(mb)

    def test_active_segment_never_compacted(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 10, rotate_at=(4,))
        r = k.compact(force=True)
        assert k.seglog.active_id not in r["covered_segments"]

    def test_committed_boundary_tamper_is_detected_on_restart(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 6, rotate_at=(2,))
        assert k.compact(force=True)["status"] == "ok"
        # 篡改已提交快照
        p = os.path.join(k.checkpoints.gen_dir(1), "snapshot.json")
        doc = read_json(p)
        doc["state"]["HACK"] = 1
        write_json(p, doc)
        k2 = Kernel(Config(data_dir=str(tmp_path), segment_bytes=10_000_000,
                           janitor_enabled=False, compaction_min_segments=1))
        with pytest.raises(Error) as e:
            k2.startup()
        assert e.value.status == 500 and e.value.code == "boundary_corrupt"

    def test_live_tail_tamper_after_compaction_detected(self, tmp_path):
        from app.common import canonical as _canonical
        k = make_kernel(tmp_path)
        seed(k, 8, rotate_at=(3,))
        k.compact(force=True)
        # 语义篡改保留下来的尾部：改第一条记录的 value 后重写整段
        tail = sorted(k.seglog.segments)[0]
        meta = k.seglog.segments[tail]
        recs = list(k.seglog.iter_segment(tail))
        recs[0]["payload"]["value"] = 999999
        with open(meta.path, "wb") as f:
            for r in recs:
                body = {x: r[x] for x in ("seq", "ts", "type", "payload", "prev")}
                f.write(_canonical(body) + b"\n")
        v = k.verify_latest()
        assert v["status"] == "failed" and v["checks"]["tail_chain"] is False

    def test_audit_chain_exposes_digests(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 6, rotate_at=(2,))
        k.compact(force=True)
        audit = k.checkpoints.read_audit()
        assert len(audit) == 1 and "digest" in audit[0]


# --------------------------------------------------------------------- 链头锚点（尾部篡改）


def _rewrite_active_record(k: Kernel, index: int, mutate) -> None:
    """直接改动活动段中第 index 行记录的内容（保持 prev 链接完好）。"""
    meta = k.seglog.segments[k.seglog.active_id]
    lines = open(meta.path, "rb").read().splitlines()
    rec = json.loads(lines[index].decode())
    mutate(rec)
    lines[index] = canonical(rec)
    with open(meta.path, "wb") as f:
        f.write(b"\n".join(lines) + b"\n")


def _restart(tmp_path) -> Kernel:
    return Kernel(Config(data_dir=str(tmp_path), segment_bytes=10_000_000,
                         janitor_enabled=False, compaction_min_segments=1))


class TestChainHeadAnchor:
    def test_head_anchor_persisted_on_every_append(self, tmp_path):
        k = make_kernel(tmp_path)
        for i in range(3):
            k.append("put", {"key": f"k{i}", "value": i})
            head = read_json(k.head_path)
            assert head["seq"] == i + 1
            assert head["digest"] == k.seglog.tip()[1]

    def test_tampered_last_record_rejected_on_restart(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 5)
        _rewrite_active_record(k, -1, lambda r: r["payload"].__setitem__("value", 999999))
        with pytest.raises(Error) as e:
            _restart(tmp_path).startup()
        assert e.value.status == 500 and e.value.code == "tail_corrupt"

    def test_tampered_last_record_fails_verify_without_checkpoint(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 5)
        _rewrite_active_record(k, -1, lambda r: r["payload"].__setitem__("value", 999999))
        v = k.verify_latest()
        assert v["status"] == "failed" and v["checks"]["head_anchor"] is False

    def test_tampered_last_record_after_compaction_rejected(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 8, rotate_at=(3,))
        assert k.compact(force=True)["status"] == "ok"
        _rewrite_active_record(k, -1, lambda r: r["payload"].__setitem__("value", 424242))
        v = k.verify_latest()
        assert v["status"] == "failed" and v["checks"]["head_anchor"] is False
        with pytest.raises(Error) as e:
            _restart(tmp_path).startup()
        assert e.value.code == "tail_corrupt"

    def test_tampered_middle_record_rejected_on_restart(self, tmp_path):
        # 改中间一条：链在后续记录处断开，scan 截断后锚点超前 -> 拒绝启动
        k = make_kernel(tmp_path)
        seed(k, 6)
        _rewrite_active_record(k, 1, lambda r: r["payload"].__setitem__("value", 777))
        with pytest.raises(Error) as e:
            _restart(tmp_path).startup()
        assert e.value.code == "tail_corrupt"

    def test_truncated_last_record_rejected_on_restart(self, tmp_path):
        # 直接删掉最后一条完整记录：锚点超前于链尖 -> 拒绝启动
        k = make_kernel(tmp_path)
        seed(k, 4)
        meta = k.seglog.segments[k.seglog.active_id]
        lines = open(meta.path, "rb").read().splitlines()
        with open(meta.path, "wb") as f:
            f.write(b"\n".join(lines[:-1]) + b"\n")
        with pytest.raises(Error) as e:
            _restart(tmp_path).startup()
        assert e.value.code == "tail_corrupt"

    def test_crash_lagging_anchor_fast_forwards(self, tmp_path):
        # 段记录已 fsync、锚点尚未落盘（崩溃窗口）：重启核对后前滚锚点
        k = make_kernel(tmp_path)
        seed(k, 3)
        tip_seq, tip_d = k.seglog.tip()
        rec = {"seq": tip_seq + 1, "ts": 1, "type": "put",
               "payload": {"key": "kx", "value": 9}, "prev": tip_d}
        meta = k.seglog.segments[k.seglog.active_id]
        with open(meta.path, "ab") as f:
            f.write(canonical(rec) + b"\n")
            f.flush()
            os.fsync(f.fileno())
        k2 = make_kernel(tmp_path)
        assert k2.seglog.tip()[0] == tip_seq + 1
        assert k2.business_state()["state"]["kx"] == 9
        assert read_json(k2.head_path)["seq"] == tip_seq + 1

    def test_clean_restart_unaffected(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 4)
        k2 = make_kernel(tmp_path)
        assert k2.business_state()["state"] == expected_state(4)
        assert k2.append("put", {"key": "k9", "value": 9})["seq"] == 5


# --------------------------------------------------------------------- 校验失败 -> 回滚


class TestVerifyFailureRollback:
    def _corrupt_candidate_byte(self, k: Kernel):
        # 篡改最老 sealed 段中某个 value 字节，保持其余格式合法
        sid = sorted(s for s, m in k.seglog.segments.items() if m.sealed)[0]
        path = k.seglog.segments[sid].path
        data = open(path, "rb").read()
        # 找到第一个 "value":N 改数字；直接翻转中间一个字节更稳：
        idx = data.find(b'"value"')
        b = bytearray(data)
        b[idx + 8] = ord("9") if chr(b[idx + 8]) != "9" else ord("8")
        open(path, "wb").write(bytes(b))
        return sid

    def test_tampered_segment_aborts_without_visible_change(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 8, rotate_at=(3,))
        sid = self._corrupt_candidate_byte(k)
        r = k.compact(force=True)
        assert r["status"] == "failed" and r["rolled_back"] is True
        assert r["visible_gen"] == 0
        # 指针未创建、原始段未删、无半成品目录
        assert not os.path.exists(k.checkpoints._current_path())
        assert sid in k.seglog.segments
        assert not k.checkpoints.temp_dirs()
        # 完整性检查如实报告磁盘上的断链（而不是若无其事的 no_checkpoint）
        v = k.verify_latest()
        assert v["status"] == "failed" and v["checks"]["tail_chain"] is False

    def test_tampered_snapshot_after_write_aborts(self, tmp_path, monkeypatch):
        k = make_kernel(tmp_path)
        seed(k, 6, rotate_at=(2,))
        orig = k.checkpoints.write_generation

        def tampered_write(gen, snap, man):
            docs = orig(gen, snap, man)
            # 提交前破坏快照（模拟落盘损坏）
            p = os.path.join(k.checkpoints.gen_dir(gen), SNAPSHOT)
            doc = read_json(p)
            doc["state"]["INJECTED"] = True
            write_json(p, doc)
            return docs

        monkeypatch.setattr(k.checkpoints, "write_generation", tampered_write)
        r = k.compact(force=True)
        assert r["status"] == "failed" and r["stage"] == "passB_verify"
        assert r["visible_gen"] == 0


# --------------------------------------------------------------------- 并发压缩


class TestConcurrentCompaction:
    def test_only_one_changes_visible_result(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 8, rotate_at=(1, 3, 5))
        results = []

        def do():
            try:
                results.append(k.compact(force=True)["status"])
            except BusyError:
                results.append("busy")

        ts = [threading.Thread(target=do) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        assert results.count("ok") == 1
        assert all(r in ("busy", "noop") for r in results if r != "ok")
        gens = [n for n in os.listdir(k.checkpoints.dir) if n.startswith("gen-")]
        assert gens == ["gen-1"]

    def test_cross_process_lock(self, tmp_path):
        k = make_kernel(tmp_path)
        seed(k, 4, rotate_at=(1,))
        import fcntl
        fd = os.open(k.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            with pytest.raises(BusyError):
                k.compact(force=True)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


# --------------------------------------------------------------------- 崩溃恢复


CRASH_SCRIPT = r"""
import sys; sys.path.insert(0, {root!r})
from app.kernel import Kernel, Config
cfg = Config(data_dir={data!r}, segment_bytes=10_000_000, janitor_enabled=False,
             compaction_min_segments=1, crash_hook={hook!r})
k = Kernel(cfg); k.startup()
for i in range(9):
    k.append("put", {{"key": f"k{{i%3}}", "value": i}})
    if i in (2,5): k.seglog.rotate()
k.compact(force=True)
"""


def _crash_then_recover(tmp_path, hook, root):
    data = str(tmp_path)
    script = CRASH_SCRIPT.format(root=root, data=data, hook=hook)
    p = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert p.returncode == 99, (hook, p.returncode, p.stderr[-800:])
    k2 = Kernel(Config(data_dir=data, segment_bytes=10_000_000,
                       janitor_enabled=False, compaction_min_segments=1))
    rec = k2.startup()
    return k2, rec


class TestCrashRecovery:
    @pytest.mark.parametrize("hook", ["after_fold", "after_write", "after_verify"])
    def test_crash_before_switch_rolls_back_to_genesis(self, tmp_path, hook):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        k, rec = _crash_then_recover(tmp_path, hook, root)
        assert rec["boundary"] == "genesis"
        assert len(k.seglog.segments) == 3  # 原始段一个没少
        assert k.seglog.tip()[0] == 9
        # 没有半成品
        assert not k.checkpoints.temp_dirs()
        # 可以干净地重新压缩
        r = k.compact(force=True)
        assert r["status"] == "ok"
        assert k.business_state()["state"] == expected_state(9)

    def test_crash_after_switch_finishes_cleanup(self, tmp_path):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        k, rec = _crash_then_recover(tmp_path, "after_switch", root)
        assert rec["boundary"] == "gen-1"
        assert sorted(rec["segments_deleted"]) == [1, 2]  # 幂等补删
        # 只留活动尾部
        assert sorted(k.seglog.segments) == [3]
        v = k.verify_latest()
        assert v["status"] == "ok" and all(v["checks"].values())
        # 续写链不断
        rec_new = k.append("put", {"key": "after", "value": 1})
        assert rec_new["seq"] == 10
        assert k.business_state()["state"]["after"] == 1
        # 再次重启稳定（幂等）
        k2 = Kernel(Config(data_dir=str(tmp_path), segment_bytes=10_000_000,
                           janitor_enabled=False, compaction_min_segments=1))
        rec2 = k2.startup()
        assert rec2["boundary"] == "gen-1" and rec2["segments_deleted"] == []


# --------------------------------------------------------------------- 原子批次


class TestBatch:
    def test_commit_visibility_and_order(self, tmp_path):
        k = make_kernel(tmp_path)
        k.append("put", {"key": "base", "value": 0})  # seq 1
        b = k.create_batch("k-1", 60_000)
        bid = b["batch_id"]
        assert b["status"] == "open" and b["remaining_ms"] > 0
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        k.batch_add_ops(bid, [{"type": "delete", "payload": {"key": "base"}},
                              {"type": "data", "payload": {"n": 1}}])
        # 提交前：普通读取与业务状态都看不到批次记录
        assert k.seglog.tip()[0] == 1
        assert k.business_state()["state"] == {"base": 0}
        assert [r["seq"] for r in k.read(1, 100)["records"]] == [1]
        # 提交：整批按加入顺序一次性可见
        res = k.commit_batch(bid)
        assert res["status"] == "committed" and res["replay"] is False
        assert (res["first_seq"], res["last_seq"], res["record_count"]) == (2, 4, 3)
        recs = k.read(2, 10)["records"]
        assert [r["seq"] for r in recs] == [2, 3, 4]
        assert [r["type"] for r in recs] == ["put", "delete", "data"]
        assert k.business_state()["state"] == {"a": 1}
        # 批次最终状态与日志序号范围可查询
        v = k.batch_view(bid)
        assert v["status"] == "committed"
        assert (v["first_seq"], v["last_seq"], v["record_count"]) == (2, 4, 3)
        assert v["content_hash"] == res["content_hash"]

    def test_same_batch_double_commit_returns_first_result(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("k-dup", 60_000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        r1 = k.commit_batch(bid)
        r2 = k.commit_batch(bid)
        assert r1["first_seq"] == r2["first_seq"] == 1
        assert r2["replay"] is True
        assert k.seglog.tip()[0] == 1  # 没有重复入链

    def test_idempotent_key_replay_across_batches(self, tmp_path):
        k = make_kernel(tmp_path)
        ops = [{"type": "put", "payload": {"key": "a", "value": 1}}]
        b1 = k.create_batch("same-key", 60_000)["batch_id"]
        k.batch_add_ops(b1, ops)
        r1 = k.commit_batch(b1)
        # 新批次、同幂等键、同内容 -> 返回第一次结果，不重复入链
        b2 = k.create_batch("same-key", 60_000)["batch_id"]
        k.batch_add_ops(b2, ops)
        r2 = k.commit_batch(b2)
        assert r2["replay"] is True
        assert (r2["first_seq"], r2["last_seq"]) == (r1["first_seq"], r1["last_seq"])
        assert k.seglog.tip()[0] == 1
        # 第二个批次也落成 committed 且指向首次序号范围
        v2 = k.batch_view(b2)
        assert v2["status"] == "committed" and v2["replay"] is True
        assert v2["first_seq"] == r1["first_seq"]

    def test_idempotent_key_conflict(self, tmp_path):
        k = make_kernel(tmp_path)
        b1 = k.create_batch("c-key", 60_000)["batch_id"]
        k.batch_add_ops(b1, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        k.commit_batch(b1)
        b2 = k.create_batch("c-key", 60_000)["batch_id"]
        k.batch_add_ops(b2, [{"type": "put", "payload": {"key": "a", "value": 2}}])
        with pytest.raises(Error) as e:
            k.commit_batch(b2)
        assert e.value.status == 409 and e.value.code == "idempotency_conflict"
        assert k.seglog.tip()[0] == 1  # 冲突提交不入链

    def test_concurrent_commit_single_effective(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("cc", 60_000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": f"k{i}", "value": i}}
                              for i in range(5)])
        results = []

        def do():
            results.append(k.commit_batch(bid))

        ts = [threading.Thread(target=do) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        # 只有一个提交真正生效，其余拿到同一首次结果
        assert {r["first_seq"] for r in results} == {1}
        assert sum(1 for r in results if not r["replay"]) == 1
        assert k.seglog.tip()[0] == 5

    def test_abort_then_commit_rejected(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("ab", 60_000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        v = k.abort_batch(bid)
        assert v["status"] == "aborted"
        with pytest.raises(Error) as e:
            k.commit_batch(bid)
        assert e.value.status == 409 and e.value.code == "batch_aborted"
        with pytest.raises(Error) as e2:
            k.batch_add_ops(bid, [{"type": "data", "payload": {}}])
        assert e2.value.status == 409
        assert k.seglog.tip()[0] == 0  # 从未占用日志序号

    def test_expiry_blocks_commit_and_ops(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("ttl", 1000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        time.sleep(1.1)
        with pytest.raises(Error) as e:
            k.commit_batch(bid)
        assert e.value.status == 410 and e.value.code == "batch_expired"
        with pytest.raises(Error) as e2:
            k.batch_add_ops(bid, [{"type": "data", "payload": {}}])
        assert e2.value.status == 410
        assert k.batch_view(bid)["status"] == "expired"
        # 过期批次不占日志序号
        assert k.seglog.tip()[0] == 0
        assert k.append("put", {"key": "n", "value": 1})["seq"] == 1

    def test_empty_batch_rejected(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("e", 60_000)["batch_id"]
        with pytest.raises(Error) as e:
            k.commit_batch(bid)
        assert e.value.status == 400 and e.value.code == "empty_batch"

    def test_add_ops_validates_payload(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("v", 60_000)["batch_id"]
        with pytest.raises(Error) as e:
            k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "x"}}])
        assert e.value.status == 400
        with pytest.raises(Error):
            k.batch_add_ops(bid, [{"type": "nope", "payload": {}}])
        assert k.batch_view(bid)["op_count"] == 0  # 坏批次不残留

    def test_unknown_batch_404(self, tmp_path):
        k = make_kernel(tmp_path)
        with pytest.raises(Error) as e:
            k.commit_batch("b-nope")
        assert e.value.status == 404
        with pytest.raises(Error):
            k.batch_view("b-nope")

    def test_open_batch_survives_restart(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("persist", 60_000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        k2 = make_kernel(tmp_path)
        v = k2.batch_view(bid)
        assert v["status"] == "open" and v["op_count"] == 1
        r = k2.commit_batch(bid)
        assert r["first_seq"] == 1
        assert k2.business_state()["state"] == {"a": 1}

    def test_committed_batch_idempotent_after_restart(self, tmp_path):
        k = make_kernel(tmp_path)
        bid = k.create_batch("pq", 60_000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        k.commit_batch(bid)
        k2 = make_kernel(tmp_path)
        v = k2.batch_view(bid)
        assert v["status"] == "committed" and v["first_seq"] == 1
        # 重启后重复提交仍返回首次结果
        r = k2.commit_batch(bid)
        assert r["replay"] is True and r["first_seq"] == 1
        assert k2.seglog.tip()[0] == 1

    def test_commit_failure_rolls_back_whole_batch(self, tmp_path, monkeypatch):
        k = make_kernel(tmp_path)
        k.append("put", {"key": "base", "value": 0})  # seq 1
        bid = k.create_batch("f", 60_000)["batch_id"]
        k.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}},
                              {"type": "put", "payload": {"key": "b", "value": 2}}])
        orig = k.seglog.append
        calls = {"n": 0}

        def flaky(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError("disk on fire")
            return orig(*a, **kw)

        monkeypatch.setattr(k.seglog, "append", flaky)
        with pytest.raises(OSError):
            k.commit_batch(bid)
        # 半批已整批回滚：链尖、业务状态、批次状态都回到提交前
        assert k.seglog.tip()[0] == 1
        assert k.business_state()["state"] == {"base": 0}
        assert k.batch_view(bid)["status"] == "open"
        # 重试成功且序号无空洞
        monkeypatch.undo()
        r = k.commit_batch(bid)
        assert (r["first_seq"], r["last_seq"]) == (2, 3)
        assert k.business_state()["state"] == {"base": 0, "a": 1, "b": 2}

    def test_truncate_tail_across_segments(self, tmp_path):
        k = make_kernel(tmp_path, segment_bytes=150)  # 小段强制滚动
        for i in range(8):
            k.append("put", {"key": f"k{i}", "value": i})
        assert len(k.seglog.segments) >= 3
        k.seglog.truncate_tail(3)
        k._sync_tip("0" * 64)
        k._persist_head()
        assert k.seglog.tip()[0] == 3
        assert k.seglog.verify_chain().ok
        rec = k.append("put", {"key": "n", "value": 1})
        assert rec["seq"] == 4  # 回滚后序号无空洞
        assert k.business_state()["state"] == {"k0": 0, "k1": 1, "k2": 2, "n": 1}

    def test_truncate_tail_to_empty_log(self, tmp_path):
        k = make_kernel(tmp_path)
        for i in range(3):
            k.append("put", {"key": f"k{i}", "value": i})
        k.seglog.truncate_tail(0)  # 整批从 seq1 开始 -> 全部回滚
        k._sync_tip("0" * 64)
        k._persist_head()
        assert k.seglog.tip()[0] == 0
        assert k.business_state()["state"] == {}
        assert k.append("put", {"key": "n", "value": 1})["seq"] == 1
        # 重启后依旧稳定（链头锚点与空链尖一致）
        k2 = make_kernel(tmp_path)
        assert k2.seglog.tip()[0] == 1
        assert k2.business_state()["state"] == {"n": 1}


# --------------------------------------------------------------------- 批次崩溃恢复


BATCH_CRASH_SCRIPT = r"""
import sys; sys.path.insert(0, {root!r})
from app.kernel import Kernel, Config
cfg = Config(data_dir={data!r}, segment_bytes={seg_bytes}, janitor_enabled=False,
             compaction_min_segments=1, crash_hook={hook!r})
k = Kernel(cfg); k.startup()
for i in range(3):
    k.append("put", {{"key": f"base{{i}}", "value": i}})
bid = k.create_batch("idem-1", 600000)["batch_id"]
k.batch_add_ops(bid, [{{"type": "put", "payload": {{"key": "bx", "value": 1}}}},
                      {{"type": "put", "payload": {{"key": "by", "value": 2}}}}])
k.commit_batch(bid)
"""


class TestBatchCrashRecovery:
    def _crash_then_recover(self, tmp_path, hook, seg_bytes=10_000_000):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        script = BATCH_CRASH_SCRIPT.format(root=root, data=str(tmp_path),
                                           seg_bytes=seg_bytes, hook=hook)
        p = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
        assert p.returncode == 99, (hook, p.returncode, p.stderr[-800:])
        k2 = Kernel(Config(data_dir=str(tmp_path), segment_bytes=seg_bytes,
                           janitor_enabled=False, compaction_min_segments=1))
        rec = k2.startup()
        return k2, rec

    def test_crash_after_mark_rolls_back(self, tmp_path):
        # 崩溃于 committing 落盘后、任何记录入链前
        k, rec = self._crash_then_recover(tmp_path, "batch_after_mark")
        rb = rec["batches"]["rolled_back"]
        assert len(rb) == 1
        assert k.seglog.tip()[0] == 3  # 链尾原样
        assert k.batch_view(rb[0])["status"] == "open"
        r = k.commit_batch(rb[0])  # 重试成功
        assert r["first_seq"] == 4 and r["last_seq"] == 5

    def test_crash_after_append_rolls_back_whole_batch(self, tmp_path):
        # 崩溃于整批记录已入链、幂等登记（提交点）之前
        k, rec = self._crash_then_recover(tmp_path, "batch_after_append")
        rb = rec["batches"]["rolled_back"]
        assert len(rb) == 1
        # 整批未提交：链尾回到批次之前，业务状态无半批记录
        assert k.seglog.tip()[0] == 3
        assert k.business_state()["state"] == {"base0": 0, "base1": 1, "base2": 2}
        assert k.batch_view(rb[0])["status"] == "open"
        # 回滚不占序号：重试提交拿到同一 first_seq
        r = k.commit_batch(rb[0])
        assert (r["first_seq"], r["last_seq"]) == (4, 5)
        assert k.business_state()["state"]["bx"] == 1
        assert k.seglog.verify_chain().ok

    def test_crash_after_append_with_segment_rollover(self, tmp_path):
        # 小段：批次提交中途触发滚动，回滚要跨段截断并删除滚出的段
        k, rec = self._crash_then_recover(tmp_path, "batch_after_append", seg_bytes=150)
        rb = rec["batches"]["rolled_back"]
        assert len(rb) == 1
        assert k.seglog.tip()[0] == 3
        assert k.business_state()["state"] == {"base0": 0, "base1": 1, "base2": 2}
        r = k.commit_batch(rb[0])
        assert r["first_seq"] == 4
        assert k.seglog.verify_chain().ok
        st = k.business_state()["state"]
        assert st["bx"] == 1 and st["by"] == 2

    def test_crash_after_register_finalizes_whole_batch(self, tmp_path):
        # 崩溃于幂等登记（提交点）之后、批次状态落盘之前
        k, rec = self._crash_then_recover(tmp_path, "batch_after_register")
        fin = rec["batches"]["finalized"]
        assert len(fin) == 1
        # 整批已提交：两条记录都在
        assert k.seglog.tip()[0] == 5
        st = k.business_state()["state"]
        assert st["bx"] == 1 and st["by"] == 2
        v = k.batch_view(fin[0])
        assert v["status"] == "committed" and (v["first_seq"], v["last_seq"]) == (4, 5)
        # 恢复后重复提交遵守原幂等结果
        r = k.commit_batch(fin[0])
        assert r["replay"] is True and r["first_seq"] == 4
        # 换批次、同幂等键、同内容：仍返回首次结果，不重复入链
        b2 = k.create_batch("idem-1", 600000)["batch_id"]
        k.batch_add_ops(b2, [{"type": "put", "payload": {"key": "bx", "value": 1}},
                             {"type": "put", "payload": {"key": "by", "value": 2}}])
        r2 = k.commit_batch(b2)
        assert r2["replay"] is True and r2["first_seq"] == 4
        assert k.seglog.tip()[0] == 5
