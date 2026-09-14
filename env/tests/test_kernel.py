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
        assert k.verify_latest()["status"] == "no_checkpoint"

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
