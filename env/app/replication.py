"""主备复制传输与后台线程（仅标准库）。

两类后台活动：
* Replicator：备用实例周期性地从来源主实例拉取
  boundary ->（必要时安装快照边界）-> 增量记录，顺序与摘要链由
  Kernel.apply_records / Kernel.install_snapshot 逐条校验。
* LeaseRefresher：主实例周期性向选举成员做任期心跳（lease ack），
  拿到多数派确认才续授权；隔离到失去多数派时授权到期自动不可写。

传输抽象成 callable，生产用 http_transport（urllib），测试可注入
直连 transport（同进程直接调用对端 Kernel 的导出/RPC 方法）。
"""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any, Callable, Optional

from .common import Error

# 复制协议路径
PATH_BOUNDARY = "/replica/boundary"
PATH_RECORDS = "/replica/records"
PATH_SNAPSHOT = "/replica/snapshot"
PATH_REQUEST_VOTE = "/cluster/request_vote"
PATH_LEASE = "/cluster/lease"


class TransportError(Exception):
    """对端返回了非 2xx 或网络不可达。"""

    def __init__(self, status: int, code: str, body: Any, url: str):
        super().__init__(f"{code} ({status}) @ {url}")
        self.status = status
        self.code = code
        self.body = body or {}
        self.url = url


Transport = Callable[[str, str, Optional[dict], Optional[dict]], tuple[int, dict]]


def http_transport(method: str, url: str, body: Optional[dict] = None,
                   qs: Optional[dict] = None, timeout: float = 5.0) -> tuple[int, dict]:
    """极简 JSON HTTP 传输：返回 (status, json_body)。"""
    if qs:
        from urllib.parse import urlencode

        url = f"{url}?{urlencode({k: v for k, v in qs.items() if v is not None})}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, json.loads(raw.decode()) if raw else {}
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode())
        except Exception:
            payload = {}
        raise TransportError(e.code, str(payload.get("error", "http_error")), payload, url)
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise TransportError(0, "unreachable", {"message": str(e)}, url)


def direct_transport(peers: dict[str, Any]) -> Transport:
    """测试用：{base_url: Kernel} 直连，绕开 HTTP。"""
    from urllib.parse import urlsplit

    def call(method: str, url: str, body: Optional[dict] = None,
             qs: Optional[dict] = None) -> tuple[int, dict]:
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"
        path = parts.path
        k = peers[base]
        qs = qs or {}
        try:
            if path == PATH_BOUNDARY:
                return 200, k.export_boundary()
            if path == PATH_RECORDS:
                return 200, k.export_records(int(qs["from"]), int(qs.get("limit", 200)),
                                             int(qs.get("term", 0)))
            if path == PATH_SNAPSHOT:
                return 200, k.export_snapshot(int(qs.get("gen", 0)), int(qs.get("term", 0)))
            if path == PATH_REQUEST_VOTE:
                return 200, k.handle_request_vote(body or {})
            if path == PATH_LEASE:
                return 200, k.handle_lease_ack(int(qs.get("term", 0)))
            raise TransportError(404, "no_route", {"path": path}, url)
        except Error as e:
            raise TransportError(e.status, e.code, e.to_dict(), url)

    return call


class _StopLoop(threading.Thread):
    daemon = True

    def __init__(self, name: str, interval: float):
        super().__init__(name=name)
        self.stop_evt = threading.Event()
        self.interval = max(0.05, interval)

    def stop(self) -> None:
        self.stop_evt.set()


class Replicator(_StopLoop):
    """备用实例后台拉取线程。冲突态冻结，提升后停止。"""

    def __init__(self, kernel, interval_ms: int, transport: Optional[Transport] = None):
        super().__init__("replicator", interval_ms / 1000)
        self.k = kernel
        self.transport = transport or http_transport

    def run(self) -> None:
        while not self.stop_evt.wait(self.interval):
            try:
                self.k.run_replication_cycle(self.transport)
            except Exception:
                pass  # 错误已记录进 replica.last_error


class LeaseRefresher(_StopLoop):
    """主实例授权续租：联系到多数派成员才能续期，否则任由授权过期。"""

    def __init__(self, kernel, interval_ms: int, ttl_ms: int,
                 transport: Optional[Transport] = None):
        super().__init__("lease-refresher", interval_ms / 1000)
        self.k = kernel
        self.ttl_ms = ttl_ms
        self.transport = transport or http_transport

    def run(self) -> None:
        while not self.stop_evt.wait(self.interval):
            try:
                res = self.k.lease_refresh_once(self.ttl_ms, self.transport)
                if res is None or not res.get("renewed"):
                    # 授权已失效或无法确认多数派：续租线程退出，不再自我复活
                    if self.k.cluster.role != "primary" or not self.k.cluster.grant_valid():
                        return
            except Exception:
                # 网络抖动等：本轮不续期；连续失败到过期后写入即被拒
                if not self.k.cluster.grant_valid():
                    return
