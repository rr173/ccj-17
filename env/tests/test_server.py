"""HTTP 端到端测试：真实起服务，含 janitor 自动压缩。"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, tmp_path, **env):
        self.port = _free_port()
        self.tmp = str(tmp_path)
        self.proc = None
        self.env = env

    def __enter__(self):
        base = dict(
            os.environ,
            APP_DATA_DIR=self.tmp, PORT=str(self.port), HOST="127.0.0.1",
            SEGMENT_BYTES="300", COMPACTION_MIN_SEGMENTS="2",
            JANITOR_INTERVAL_MS="500", DEFAULT_TTL_MS="3000",
            MAX_TTL_MS="600000", QUIET="1",
        )
        base.update({k: str(v) for k, v in self.env.items()})
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app"], cwd=ROOT, env=base,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        for _ in range(50):
            try:
                self.call("GET", "/health")
                return self
            except Exception:
                time.sleep(0.1)
        out = self.proc.stdout.read() if self.proc.stdout else ""
        raise RuntimeError(f"server did not start: {out[-1000:]}")

    def __exit__(self, *exc):
        assert self.proc is not None
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def call(self, method, path, body=None, timeout=8):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data,
            headers={"Content-Type": "application/json"}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return r.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            return e.status, json.loads(e.read())

    def append_puts(self, n):
        for i in range(n):
            st, b = self.call("POST", "/append",
                              {"type": "put", "payload": {"key": f"k{i % 3}", "value": i}})
            assert st == 201, b


def test_full_flow(tmp_path):
    with Server(tmp_path) as s:
        s.append_puts(30)
        st, stb = s.call("GET", "/status")
        assert len(stb["segments"]) >= 5

        # 钉位查询
        pin_seq = stb["segments"][6]["first_seq"]
        st, rd = s.call("POST", "/readers/alice", {"pin_seq": pin_seq, "ttl_ms": 60000})
        assert st == 201
        st, pv = s.call("GET", "/pins")
        assert pv["protected_segment"] == pv["reclaimable_segments"][-1] + 1
        reclaimable_before = pv["reclaimable_segments"]

        # 压缩：只能回收钉点之前
        st, r = s.call("POST", "/compact", {"force": True})
        assert st == 200 and r["status"] == "ok"
        assert r["covered_segments"] == reclaimable_before
        assert r["equivalence_proof"]["equivalent"] is True

        # 校验结果查询
        st, v = s.call("POST", "/compact/verify", {})
        assert st == 200 and v["status"] == "ok" and all(v["checks"].values())
        st, last = s.call("GET", "/compact/result")
        assert last["gen"] == 1

        # 从头读取的业务含义
        st, bstate = s.call("GET", "/state")
        assert bstate["state"] == {"k0": 27, "k1": 28, "k2": 29}

        # 已压缩序号 -> 410 并告知可读起点
        st, err = s.call("GET", "/read?from=1&limit=5")
        assert st == 410 and err["details"]["readable_from"]
        st, head = s.call("GET", "/head")
        st, page = s.call("GET", f"/read?from={head['readable_from']}&limit=1000")
        assert len(page["records"]) == 30 - head["readable_from"] + 1

        # 审计链
        st, aud = s.call("GET", "/audit")
        assert len(aud["audit"]) == 1 and aud["audit"][0]["kind"] == "compacted"


def test_janitor_respects_pin_and_compresses_after_release(tmp_path):
    with Server(tmp_path) as s:
        s.append_puts(40)
        st, stb = s.call("GET", "/status")
        pin = stb["segments"][-3]["first_seq"]  # 钉在靠近尾部
        s.call("POST", "/readers/keep", {"pin_seq": pin, "ttl_ms": 60000})
        time.sleep(1.4)  # 等 janitor 跑两轮
        st, res = s.call("GET", "/compact/result")
        # 钉点之后段不能被误删
        st, pv = s.call("GET", "/pins")
        assert pv["protected_segment"] is not None
        st, stb2 = s.call("GET", "/status")
        assert all(m["seg"] >= pv["protected_segment"] - 1 for m in stb2["segments"])

        s.call("DELETE", "/readers/keep")
        deadline = time.time() + 6
        while time.time() < deadline:
            st, res = s.call("GET", "/compact/result")
            if res.get("status") == "ok" and res.get("covered_segments"):
                break
            time.sleep(0.3)
        assert res["status"] == "ok" and res["covered_segments"]
        st, v = s.call("POST", "/compact/verify", {})
        assert v["status"] == "ok"


def test_lease_expiry_then_reclaim(tmp_path):
    with Server(tmp_path) as s:
        s.append_puts(30)
        st, stb = s.call("GET", "/status")
        pin = stb["segments"][2]["first_seq"]
        s.call("POST", "/readers/temp", {"pin_seq": pin, "ttl_ms": 1000})
        st, pv = s.call("GET", "/pins")
        assert pv["active_readers"] == 1
        time.sleep(1.3)  # 租约过期
        st, pv2 = s.call("GET", "/pins")
        assert pv2["active_readers"] == 0 and pv2["oldest_pin_seq"] is None
        st, hb = s.call("POST", "/readers/temp/heartbeat", {})
        assert st == 410


def test_concurrent_compact_endpoint_single_winner(tmp_path):
    with Server(tmp_path, JANITOR_ENABLED="false") as s:
        s.append_puts(30)
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(4) as ex:
            futs = [ex.submit(s.call, "POST", "/compact", {"force": True}) for _ in range(4)]
            results = [f.result() for f in futs]
        oks = []
        for st, b in results:
            if b.get("status") == "ok" and "gen" in b:
                oks.append(b)
            else:
                assert b.get("status") in ("noop", "busy"), b
        assert len(oks) == 1, results
        st, aud = s.call("GET", "/audit")
        assert len(aud["audit"]) == 1


def test_restart_serves_last_boundary(tmp_path):
    with Server(tmp_path, JANITOR_ENABLED="false") as s:
        s.append_puts(30)
        s.call("POST", "/compact", {"force": True})
        st, head = s.call("GET", "/head")
        assert head["checkpoint"]["gen"] == 1
    # 重启
    with Server(tmp_path) as s:
        st, v = s.call("POST", "/compact/verify", {})
        assert v["status"] == "ok"
        st, bstate = s.call("GET", "/state")
        assert bstate["state"] == {"k0": 27, "k1": 28, "k2": 29}
        st, rec = s.call("POST", "/append", {"type": "put", "payload": {"key": "z", "value": 1}})
        assert st == 201


def test_batch_flow_http(tmp_path):
    with Server(tmp_path, JANITOR_ENABLED="false") as s:
        # 创建批次（带幂等键）
        st, b = s.call("POST", "/batches", {"idempotency_key": "http-1", "ttl_ms": 60000})
        assert st == 201 and b["status"] == "open"
        bid = b["batch_id"]
        # 分多次加入记录
        st, b2 = s.call("POST", f"/batches/{bid}/ops",
                        {"ops": [{"type": "put", "payload": {"key": "h1", "value": 1}}]})
        assert st == 200 and b2["op_count"] == 1
        st, b2 = s.call("POST", f"/batches/{bid}/ops",
                        {"type": "delete", "payload": {"key": "h1"}})
        assert st == 200 and b2["op_count"] == 2
        st, b2 = s.call("POST", f"/batches/{bid}/ops",
                        {"ops": [{"type": "put", "payload": {"key": "h2", "value": 2}}]})
        assert b2["op_count"] == 3
        # 提交前：业务状态与普通读取都看不到
        st, stb = s.call("GET", "/state")
        assert stb["state"] == {}
        st, page = s.call("GET", "/read?from=1&limit=10")
        assert page["records"] == []
        # 提交：整批按加入顺序一次性可见
        st, r = s.call("POST", f"/batches/{bid}/commit", {})
        assert st == 200 and r["status"] == "committed" and r["replay"] is False
        assert (r["first_seq"], r["last_seq"], r["record_count"]) == (1, 3, 3)
        st, page = s.call("GET", "/read?from=1&limit=10")
        assert [rec["type"] for rec in page["records"]] == ["put", "delete", "put"]
        st, stb = s.call("GET", "/state")
        assert stb["state"] == {"h2": 2}  # h1 先 put 后 delete
        # 批次最终状态与序号范围可查询
        st, v = s.call("GET", f"/batches/{bid}")
        assert v["status"] == "committed" and (v["first_seq"], v["last_seq"]) == (1, 3)
        # 同批次重复提交 -> 首次结果
        st, r2 = s.call("POST", f"/batches/{bid}/commit", {})
        assert r2["replay"] is True and r2["first_seq"] == 1
        # 同幂等键不同内容 -> 409 冲突
        st, b3 = s.call("POST", "/batches", {"idempotency_key": "http-1"})
        bid3 = b3["batch_id"]
        s.call("POST", f"/batches/{bid3}/ops",
               {"ops": [{"type": "put", "payload": {"key": "h1", "value": 99}}]})
        st, e = s.call("POST", f"/batches/{bid3}/commit", {})
        assert st == 409 and e["error"] == "idempotency_conflict"
        # 同幂等键相同内容 -> 重放首次结果
        st, b4 = s.call("POST", "/batches", {"idempotency_key": "http-1"})
        bid4 = b4["batch_id"]
        s.call("POST", f"/batches/{bid4}/ops",
               {"ops": [{"type": "put", "payload": {"key": "h1", "value": 1}},
                        {"type": "delete", "payload": {"key": "h1"}},
                        {"type": "put", "payload": {"key": "h2", "value": 2}}]})
        st, r4 = s.call("POST", f"/batches/{bid4}/commit", {})
        assert st == 200 and r4["replay"] is True and r4["first_seq"] == 1
        st, stb = s.call("GET", "/status")
        assert stb["tip_seq"] == 3  # 重放不占新序号
        # 放弃后不能再提交
        st, b5 = s.call("POST", "/batches", {})
        bid5 = b5["batch_id"]
        st, _ = s.call("POST", f"/batches/{bid5}/ops",
                       {"ops": [{"type": "data", "payload": {"x": 1}}]})
        st, ab = s.call("POST", f"/batches/{bid5}/abort", {})
        assert st == 200 and ab["status"] == "aborted"
        st, e = s.call("POST", f"/batches/{bid5}/commit", {})
        assert st == 409 and e["error"] == "batch_aborted"
        # 未知批次 404；空批次 400
        st, e = s.call("GET", "/batches/b-nope")
        assert st == 404
        st, b6 = s.call("POST", "/batches", {})
        st, e = s.call("POST", f"/batches/{b6['batch_id']}/commit", {})
        assert st == 400 and e["error"] == "empty_batch"


def test_batch_committed_survives_server_restart(tmp_path):
    with Server(tmp_path, JANITOR_ENABLED="false") as s:
        st, b = s.call("POST", "/batches", {"idempotency_key": "rst"})
        bid = b["batch_id"]
        s.call("POST", f"/batches/{bid}/ops",
               {"ops": [{"type": "put", "payload": {"key": "z", "value": 7}}]})
        st, r = s.call("POST", f"/batches/{bid}/commit", {})
        assert r["first_seq"] == 1
    # 重启：批次状态与幂等结果都还在
    with Server(tmp_path, JANITOR_ENABLED="false") as s:
        st, v = s.call("GET", f"/batches/{bid}")
        assert v["status"] == "committed" and v["first_seq"] == 1
        st, r = s.call("POST", f"/batches/{bid}/commit", {})
        assert r["replay"] is True and r["first_seq"] == 1
        st, page = s.call("GET", "/read?from=1&limit=10")
        assert len(page["records"]) == 1
        st, stb = s.call("GET", "/state")
        assert stb["state"] == {"z": 7}


# ---------------------------------------------------------------- schedules


def test_schedule_http_lifecycle_and_idempotency(tmp_path):
    future = int(time.time() * 1000) + 600_000
    with Server(tmp_path, JANITOR_ENABLED="false", SCHEDULER_ENABLED="false") as s:
        body = {"request_id": "http-1", "effective_at": future,
                "ops": [{"type": "put", "payload": {"key": "a", "value": 1}}]}
        st, r = s.call("POST", "/schedules", body)
        assert st == 201 and r["status"] == "pending" and r["version"] == 1
        # 重复创建相同内容 -> 原预约 replay
        st, r2 = s.call("POST", "/schedules", body)
        assert st == 201 and r2.get("replay") is True
        # 不同内容 -> 409
        bad = dict(body, ops=[{"type": "put", "payload": {"key": "a", "value": 2}}])
        st, err = s.call("POST", "/schedules", bad)
        assert st == 409 and err["error"] == "schedule_conflict"
        # 改期：旧版本被拒，新版本成功
        st, err = s.call("POST", "/schedules/http-1/reschedule",
                         {"effective_at": future + 1000, "expected_version": 99})
        assert st == 409 and err["error"] == "version_conflict"
        st, rr = s.call("POST", "/schedules/http-1/reschedule",
                        {"effective_at": future + 1000, "expected_version": 1})
        assert rr["version"] == 2
        # 列表过滤
        st, lst = s.call("GET", "/schedules?status=pending")
        assert [x["request_id"] for x in lst["schedules"]] == ["http-1"]


def test_schedule_http_due_then_applied_with_log_position(tmp_path):
    past = int(time.time() * 1000) - 1000
    with Server(tmp_path, JANITOR_ENABLED="false", SCHEDULER_ENABLED="false") as s:
        body = {"request_id": "http-due", "effective_at": past,
                "ops": [{"type": "put", "payload": {"key": "k", "value": 1}},
                        {"type": "delete", "payload": {"key": "k"}},
                        {"type": "put", "payload": {"key": "m", "value": 2}}]}
        st, r = s.call("POST", "/schedules", body)
        assert st == 201
        st, out = s.call("POST", "/schedules/tick", {})
        assert out["ran"] == 1
        st, v = s.call("GET", "/schedules/http-due")
        assert v["status"] == "applied" and v["first_seq"] == 1 and v["last_seq"] == 3
        st, state = s.call("GET", "/state")
        assert state["state"] == {"m": 2}


def test_schedule_http_cancel_competes_with_tick_unique_outcome(tmp_path):
    past = int(time.time() * 1000) - 1000
    with Server(tmp_path, JANITOR_ENABLED="false", SCHEDULER_ENABLED="false") as s:
        body = {"request_id": "http-cancel", "effective_at": past,
                "ops": [{"type": "put", "payload": {"key": "c", "value": 1}}]}
        s.call("POST", "/schedules", body)
        st, cr = s.call("POST", "/schedules/http-cancel/cancel",
                        {"expected_version": 1})
        assert st == 200 and cr["status"] == "cancelled"
        st, out = s.call("POST", "/schedules/tick", {})
        assert out["ran"] == 0
        st, state = s.call("GET", "/state")
        assert state["state"] == {}
        # 执行后再取消明确返回已经开始
        body["request_id"] = "http-cancel-2"
        s.call("POST", "/schedules", body)
        s.call("POST", "/schedules/tick", {})
        st, err = s.call("POST", "/schedules/http-cancel-2/cancel",
                         {"expected_version": 1})
        assert st == 409 and err["error"] == "already_started"


def test_schedule_background_scheduler_applies_due_automatically(tmp_path):
    past = int(time.time() * 1000) + 500
    with Server(tmp_path, JANITOR_ENABLED="false",
                SCHEDULER_INTERVAL_MS="50", GRANT_TTL_MS="600000") as s:
        body = {"request_id": "bg-1", "effective_at": past,
                "ops": [{"type": "put", "payload": {"key": "bg", "value": 1}}]}
        st, r = s.call("POST", "/schedules", body)
        assert st == 201
        deadline = time.time() + 5
        while time.time() < deadline:
            st, v = s.call("GET", "/schedules/bg-1")
            if v["status"] == "applied":
                break
            time.sleep(0.1)
        assert v["status"] == "applied" and v["first_seq"] == 1
        st, state = s.call("GET", "/state")
        assert state["state"] == {"bg": 1}
