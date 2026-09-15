"""HTTP 端到端：两个真实服务进程间的主备复制与故障切换。"""
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
    def __init__(self, tmp_path, name, port, **env):
        self.port = port
        self.tmp = os.path.join(str(tmp_path), name)
        os.makedirs(self.tmp, exist_ok=True)
        self.proc = None
        self.env = env

    def __enter__(self):
        base = dict(
            os.environ,
            APP_DATA_DIR=self.tmp, PORT=str(self.port), HOST="127.0.0.1",
            SEGMENT_BYTES="10000000", JANITOR_ENABLED="false",
            DEFAULT_TTL_MS="60000", MAX_TTL_MS="600000", QUIET="1",
            GRANT_TTL_MS="60000", REPLICATION_INTERVAL_MS="100",
            LEASE_INTERVAL_MS="500",
        )
        base.update({k: str(v) for k, v in self.env.items()})
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app"], cwd=ROOT, env=base,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        for _ in range(60):
            try:
                self.call("GET", "/health")
                return self
            except Exception:
                time.sleep(0.1)
        out = self.proc.stdout.read() if self.proc.stdout else ""
        raise RuntimeError(f"server did not start: {out[-1200:]}")

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

    def append_puts(self, n, start=0):
        for i in range(start, start + n):
            st, b = self.call("POST", "/append",
                              {"type": "put", "payload": {"key": f"k{i % 3}", "value": i}})
            assert st == 201, b

    def wait_until(self, fn, timeout=5.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = fn()
            if last:
                return last
            time.sleep(0.1)
        raise AssertionError(f"condition not met; last={last}")


def test_replication_and_promotion_over_http(tmp_path):
    pp, sp = _free_port(), _free_port()
    primary = Server(tmp_path, "p", pp, NODE_ID="P",
                     PEERS=f"P=http://127.0.0.1:{pp},S=http://127.0.0.1:{sp}")
    standby = Server(tmp_path, "s", sp, NODE_ID="S", BOOTSTRAP_ROLE="standby",
                     PEERS=f"P=http://127.0.0.1:{pp},S=http://127.0.0.1:{sp}",
                     REPLICA_SOURCE=f"http://127.0.0.1:{pp}")
    with primary as p, standby as s:
        p.append_puts(10)
        # 后台复制自动追平
        def caught():
            st, v = s.call("GET", "/replica")
            return v if v.get("phase") == "caught_up" and v["tip_seq"] == 10 else None
        s.wait_until(caught)
        st, stb = s.call("GET", "/state")
        assert stb["state"] == {"k0": 9, "k1": 7, "k2": 8}
        # 备拒绝写
        st, err = s.call("POST", "/append",
                         {"type": "put", "payload": {"key": "x", "value": 1}})
        assert st == 403 and err["error"] == "not_primary"

        # 增量
        p.append_puts(5, start=10)
        s.wait_until(lambda: s.call("GET", "/replica")[1].get("tip_seq") == 15)

        # 未追平场景：提升需要多数派；先在主仍有效时尝试 -> 选举失败
        st, err = s.call("POST", "/cluster/promote",
                         {"term": 2, "voters": [
                             f"http://127.0.0.1:{pp}", f"http://127.0.0.1:{sp}"]})
        assert st == 409 and err["error"] == "election_lost", err

        # 交接后提升成功
        st, b = p.call("POST", "/cluster/stepdown", {})
        assert st == 200
        st, b = s.call("POST", "/cluster/promote",
                       {"term": 2, "ttl_ms": 30000, "voters": [
                           f"http://127.0.0.1:{pp}", f"http://127.0.0.1:{sp}"]})
        assert st == 200 and b["term"] == 2 and b["votes"] == 2, b

        # 新主可写；旧主拒绝写
        st, w = s.call("POST", "/append",
                       {"type": "put", "payload": {"key": "after", "value": 1}})
        assert st == 201 and w["seq"] == 16
        st, err = p.call("POST", "/append",
                         {"type": "put", "payload": {"key": "stale", "value": 1}})
        assert st == 403

        # 状态接口区分可写主与授权失效
        st, vs = s.call("GET", "/replica")
        assert vs["phase"] == "writable_primary"


def test_standby_restart_resumes_and_status_phases(tmp_path):
    pp, sp = _free_port(), _free_port()
    primary = Server(tmp_path, "p", pp, NODE_ID="P")
    standby = Server(tmp_path, "s", sp, NODE_ID="S", BOOTSTRAP_ROLE="standby",
                     REPLICA_SOURCE=f"http://127.0.0.1:{pp}")
    with primary as p, standby as s:
        p.append_puts(6)
        s.wait_until(lambda: s.call("GET", "/replica")[1].get("tip_seq") == 6)
        st, v = s.call("GET", "/replica")
        assert v["replication"]["synced_seq"] == 6
        assert v["replication"]["source_boundary"]["term"] == 1
        assert v["replication"]["last_error"] is None
    # 主继续写，备重启后从已确认位置继续追上
    with primary as p:
        p.append_puts(4, start=6)
        with standby:
            def caught10():
                st, v = standby.call("GET", "/replica")
                return v if v.get("tip_seq") == 10 else None
            standby.wait_until(caught10, timeout=8.0)
            st, stb = standby.call("GET", "/state")
            assert stb["state"] == {"k0": 9, "k1": 7, "k2": 8}
