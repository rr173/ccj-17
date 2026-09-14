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
