"""HTTP 服务：路由 + 后台 janitor（定时自动压缩）。

仅依赖标准库 http.server；ThreadingHTTPServer 处理并发请求。
压缩由 meta_lock+flock 串行化，janitor 与人工触发不会重叠。
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .common import Error, canonical, err
from .kernel import Config, Kernel
from .replication import LeaseRefresher, Replicator

MAX_BODY = 4 * 1024 * 1024


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    return default if v is None else v.lower() in ("1", "true", "yes", "on")


def _env_peers() -> dict[str, str]:
    """PEERS=nid1=http://host:port,nid2=http://host:port"""
    raw = os.environ.get("PEERS", "").strip()
    peers: dict[str, str] = {}
    if raw:
        for item in raw.split(","):
            item = item.strip()
            if not item:
                continue
            if "=" not in item:
                raise ValueError("PEERS entries must be node_id=base_url")
            nid, url = item.split("=", 1)
            peers[nid.strip()] = url.strip().rstrip("/")
    return peers


def build_config() -> Config:
    return Config(
        data_dir=os.environ.get("APP_DATA_DIR", "/data"),
        segment_bytes=int(os.environ.get("SEGMENT_BYTES", 1 << 20)),
        default_ttl_ms=int(os.environ.get("DEFAULT_TTL_MS", 30_000)),
        max_ttl_ms=int(os.environ.get("MAX_TTL_MS", 24 * 3600 * 1000)),
        janitor_enabled=_env_bool("JANITOR_ENABLED", True),
        janitor_interval_ms=int(os.environ.get("JANITOR_INTERVAL_MS", 5_000)),
        compaction_min_segments=int(os.environ.get("COMPACTION_MIN_SEGMENTS", 4)),
        crash_hook=os.environ.get("CRASH_HOOK") or None,
        node_id=os.environ.get("NODE_ID") or None,
        bootstrap_role=os.environ.get("BOOTSTRAP_ROLE", "primary"),
        peers=_env_peers(),
        bootstrap_voters=(
            [v.strip() for v in os.environ["BOOTSTRAP_VOTERS"].split(",") if v.strip()]
            if os.environ.get("BOOTSTRAP_VOTERS") else None),
        grant_ttl_ms=int(os.environ.get("GRANT_TTL_MS", 10_000)),
        lease_interval_ms=int(os.environ.get("LEASE_INTERVAL_MS", 2_000)),
        replication_interval_ms=int(os.environ.get("REPLICATION_INTERVAL_MS", 500)),
        replication_batch=int(os.environ.get("REPLICATION_BATCH", 500)),
        replica_source=(os.environ.get("REPLICA_SOURCE", "").strip() or None),
    )


class Janitor(threading.Thread):
    daemon = True

    def __init__(self, kernel: Kernel):
        super().__init__(name="janitor")
        self.k = kernel
        self.stop_evt = threading.Event()

    def run(self) -> None:
        interval = max(0.5, self.k.cfg.janitor_interval_ms / 1000)
        while not self.stop_evt.wait(interval):
            try:
                # 备库不自行压缩：边界随复制从主库安装
                if self.k.cluster.role != "primary":
                    continue
                self.k.compact()
            except Error:
                pass  # 409 busy / 403 非主等；结果已可在 /compact/result 查询
            except Exception:
                pass

    def stop(self) -> None:
        self.stop_evt.set()


class Handler(BaseHTTPRequestHandler):
    server_version = "AppendLog/1.0"
    kernel: Kernel  # 由 main 注入到类上

    def log_message(self, fmt: str, *args) -> None:
        if os.environ.get("QUIET"):
            return
        super().log_message(fmt, *args)

    # ---------- 基础收发 ----------

    def _send(self, status: int, obj) -> None:
        body = canonical(obj)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length > MAX_BODY:
            raise err(413, "body_too_large", f"body exceeds {MAX_BODY} bytes")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            raise err(400, "bad_json", "request body must be valid JSON")
        if not isinstance(data, dict):
            raise err(400, "bad_json", "request body must be a JSON object")
        return data

    def _qs(self) -> dict:
        return parse_qs(urlsplit(self.path).query)

    def _qs_int(self, name: str, default: int, minimum: int = 0) -> int:
        qs = self._qs()
        if name not in qs:
            return default
        try:
            v = int(qs[name][0])
        except ValueError:
            raise err(400, "bad_query", f"{name} must be an integer")
        if v < minimum:
            raise err(400, "bad_query", f"{name} must be >= {minimum}")
        return v

    # ---------- 路由 ----------

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        try:
            if not parts:
                return self._send(200, {"service": "append-only-log", "endpoints": _ENDPOINTS})
            route = (parts[0], len(parts), method)
            if route == ("append", 1, "POST"):
                b = self._body()
                if "type" not in b:
                    raise err(400, "bad_request", "append requires type")
                return self._send(201, self.kernel.append(b["type"], b.get("payload")))
            if route == ("batches", 1, "POST"):
                b = self._body()
                return self._send(201, self.kernel.create_batch(b.get("idempotency_key"), b.get("ttl_ms")))
            if route == ("batches", 1, "GET"):
                return self._send(200, self.kernel.list_batches())
            if len(parts) == 2 and parts[0] == "batches" and method == "GET":
                return self._send(200, self.kernel.batch_view(parts[1]))
            if len(parts) == 3 and parts[0] == "batches" and parts[2] == "ops" and method == "POST":
                b = self._body()
                ops = b.get("ops")
                if ops is None:
                    if "type" not in b:
                        raise err(400, "bad_request", "ops requires {ops:[...]} or a single {type, payload}")
                    ops = [{"type": b["type"], "payload": b.get("payload")}]
                return self._send(200, self.kernel.batch_add_ops(parts[1], ops))
            if len(parts) == 3 and parts[0] == "batches" and parts[2] == "commit" and method == "POST":
                return self._send(200, self.kernel.commit_batch(parts[1]))
            if len(parts) == 3 and parts[0] == "batches" and parts[2] == "abort" and method == "POST":
                return self._send(200, self.kernel.abort_batch(parts[1]))
            if route == ("read", 1, "GET"):
                start = self._qs_int("from", 1, minimum=1)
                limit = min(self._qs_int("limit", 100, minimum=1), 1000)
                return self._send(200, self.kernel.read(start, limit))
            if route == ("readers", 1, "GET"):
                from .common import now_ms
                return self._send(200, {"readers": self.kernel.readers.list_view(now_ms())})
            if len(parts) == 2 and parts[0] == "readers" and method == "POST":
                return self._send(201, self._register(parts[1]))
            if len(parts) == 1 and parts[0] == "readers" and method == "POST":
                return self._send(201, self._register(None))
            if len(parts) == 3 and parts[0] == "readers" and parts[2] == "heartbeat" and method == "POST":
                b = self._body()
                return self._send(200, self.kernel.heartbeat(parts[1], b.get("ttl_ms"), b.get("position")))
            if len(parts) == 2 and parts[0] == "readers" and method == "DELETE":
                self.kernel.release_reader(parts[1])
                self.send_response(204)
                self.end_headers()
                return
            if route == ("pins", 1, "GET"):
                return self._send(200, self.kernel.pin_view())
            if route == ("state", 1, "GET"):
                return self._send(200, self.kernel.business_state())
            if route == ("head", 1, "GET"):
                return self._send(200, self.kernel.head_info())
            if route == ("compact", 1, "POST"):
                b = self._body()
                return self._send(200, self.kernel.compact(force=bool(b.get("force", False))))
            if len(parts) == 2 and parts[0] == "compact" and parts[1] == "result" and method == "GET":
                return self._send(200, self.kernel.last_compact or {"status": "never"})
            if len(parts) == 2 and parts[0] == "compact" and parts[1] == "verify" and method == "POST":
                return self._send(200, self.kernel.verify_latest())
            if route == ("audit", 1, "GET"):
                return self._send(200, {"audit": self.kernel.checkpoints.read_audit()})
            # ---------- 主备复制 / 故障切换 ----------
            if route == ("replica", 1, "POST"):
                b = self._body()
                return self._send(200, self.kernel.configure_replica(b["peer_url"]))
            if len(parts) == 2 and parts[0] == "replica" and parts[1] == "stop" and method == "POST":
                return self._send(200, self.kernel.stop_replica())
            if len(parts) == 2 and parts[0] == "replica" and parts[1] == "reset" and method == "POST":
                return self._send(200, self.kernel.reset_replica())
            if len(parts) == 2 and parts[0] == "replica" and parts[1] == "cycle" and method == "POST":
                return self._send(200, self.kernel.run_replication_cycle())
            if route == ("replica", 1, "GET"):
                return self._send(200, self.kernel.replication_view())
            if len(parts) == 2 and parts[0] == "replica" and parts[1] == "boundary" and method == "GET":
                return self._send(200, self.kernel.export_boundary())
            if len(parts) == 2 and parts[0] == "replica" and parts[1] == "records" and method == "GET":
                qs = self._qs()
                start = self._qs_int("from", 0, minimum=0)
                if start <= 0:
                    raise err(400, "bad_query", "from must be >= 1")
                limit = min(self._qs_int("limit", 500, minimum=1), 5000)
                term = int(qs.get("term", ["0"])[0] or 0)
                return self._send(200, self.kernel.export_records(start, limit, term))
            if len(parts) == 2 and parts[0] == "replica" and parts[1] == "snapshot" and method == "GET":
                gen = self._qs_int("gen", 0, minimum=0)
                term = int(self._qs().get("term", ["0"])[0] or 0)
                return self._send(200, self.kernel.export_snapshot(gen, term))
            if len(parts) == 2 and parts[0] == "cluster" and parts[1] == "request_vote" and method == "POST":
                return self._send(200, self.kernel.handle_request_vote(self._body()))
            if len(parts) == 2 and parts[0] == "cluster" and parts[1] == "lease" and method == "POST":
                term = int(self._qs().get("term", ["0"])[0] or 0)
                return self._send(200, self.kernel.handle_lease_ack(term))
            if len(parts) == 2 and parts[0] == "cluster" and parts[1] == "grant" and method == "POST":
                b = self._body()
                return self._send(200, self.kernel.renew_grant(b.get("ttl_ms")))
            if len(parts) == 2 and parts[0] == "cluster" and parts[1] == "stepdown" and method == "POST":
                return self._send(200, self.kernel.stepdown())
            if len(parts) == 2 and parts[0] == "cluster" and parts[1] == "promote" and method == "POST":
                b = self._body()
                return self._send(200, self.kernel.campaign(
                    term=b.get("term"), grant_ttl_ms=b.get("ttl_ms"),
                    voter_urls=b.get("voters"),
                    min_catch_up_seq=b.get("required_seq")))
            if route == ("status", 1, "GET"):
                return self._send(200, self.kernel.status())
            if route == ("health", 1, "GET"):
                return self._send(200, {"ok": True})
            raise err(404, "not_found", f"no route for {method} /{'/'.join(parts)}")
        except Error as e:
            self._send(e.status, e.to_dict())
        except Exception as e:  # 不把内部异常细节泄露出去
            self._send(500, {"error": "internal", "message": str(e)})

    def _register(self, reader_id: str | None) -> dict:
        b = self._body()
        pin = int(b.get("pin_seq", 0))
        return self.kernel.register_reader(pin, b.get("ttl_ms"), reader_id)


_ENDPOINTS = [
    "POST /append", "GET /read?from=1&limit=100",
    "POST /batches", "GET /batches", "GET /batches/{id}",
    "POST /batches/{id}/ops", "POST /batches/{id}/commit", "POST /batches/{id}/abort",
    "GET /readers", "POST /readers[/{id}]", "POST /readers/{id}/heartbeat", "DELETE /readers/{id}",
    "GET /pins", "GET /state", "GET /head",
    "POST /compact", "GET /compact/result", "POST /compact/verify",
    "GET /audit", "GET /status", "GET /health",
    "GET /replica", "POST /replica {peer_url}", "POST /replica/cycle",
    "POST /replica/stop", "POST /replica/reset",
    "GET /replica/boundary", "GET /replica/records?from=&term=",
    "GET /replica/snapshot?gen=&term=",
    "POST /cluster/promote {term?,ttl_ms?,voters?,required_seq?}",
    "POST /cluster/stepdown", "POST /cluster/grant {ttl_ms?}",
    "POST /cluster/request_vote", "POST /cluster/lease?term=",
]


def main() -> None:
    cfg = build_config()
    kernel = Kernel(cfg)
    recovery = kernel.startup()
    Handler.kernel = kernel
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    httpd = ThreadingHTTPServer((host, port), Handler)
    bg = []
    janitor = Janitor(kernel) if cfg.janitor_enabled else None
    if janitor:
        janitor.start()
        bg.append(janitor)
    # 备库后台拉取；多节点主库授权续租心跳（拿不到多数派则不续期，
    # TTL 过后写入被门控拒绝；发现更高任期则授权失效、线程退出）
    if kernel.cluster.role == "standby":
        rep = Replicator(kernel, cfg.replication_interval_ms)
        rep.start()
        bg.append(rep)
    elif cfg.peers:
        refresher = LeaseRefresher(kernel, cfg.lease_interval_ms, cfg.grant_ttl_ms)
        refresher.start()
        bg.append(refresher)
    print(f"append-only log listening on {host}:{port}; "
          f"role={kernel.cluster.role} term={kernel.cluster.term} "
          f"recovery={json.dumps(recovery, ensure_ascii=False)}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for t in bg:
            t.stop()
        httpd.server_close()
