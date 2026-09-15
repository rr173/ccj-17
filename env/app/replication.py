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
PATH_ACK = "/cluster/ack"
PATH_APPEND = "/cluster/append_entries"
PATH_PROGRESS = "/cluster/progress"


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
            if path == PATH_ACK:
                return 200, k.handle_ack(
                    str((body or {}).get("node_id", "?")),
                    int((body or {}).get("term", 0)),
                    int((body or {}).get("match_seq", 0)),
                    int((body or {}).get("commit_seq", 0)),
                    barrier=bool((body or {}).get("barrier", False)),
                    commit_digest=(body or {}).get("commit_digest"))
            if path == PATH_APPEND:
                return 200, k.handle_append_entries(body or {})
            if path == PATH_PROGRESS:
                return 200, k.handle_progress()
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


class LeaderPusher(_StopLoop):
    """主实例主动推送：故障切换后旧主不会自动改拉新主，由主推送增量。

    周期性把提交水位之后的记录推给已知投票成员；备端走与拉取相同的
    apply_records 校验路径，确认位置随 RPC 响应返回并计入主的多数派提交。
    """

    def __init__(self, kernel, interval_ms: int,
                 transport: Optional[Transport] = None):
        super().__init__("leader-pusher", max(0.05, interval_ms / 1000))
        self.k = kernel
        self.transport = transport or http_transport

    def run(self) -> None:
        while not self.stop_evt.wait(self.interval):
            try:
                if self.k.cluster.role != "primary":
                    return
                for nid, url in self.k.cfg.peers.items():
                    if nid == self.k.cluster.node_id:
                        continue
                    self.k.leader_push_once(url.rstrip("/"), self.transport)
            except Exception:
                pass


class ClusterSupervisor(_StopLoop):
    """角色驱动的后台循环：随当前角色启动/停止拉取、续租与主动推送。

    故障切换是运行时事件（不重启进程）：旧主被更高任期心跳降为备后必须
    开始跟随新主，备竞选成功后必须开始续租与主动推送。本线程按当前角色
    管理这些子循环的生命周期；子线程均为 daemon，监督线程退出时一起回收。
    """

    def __init__(self, kernel, replication_interval_ms: int,
                 lease_interval_ms: int, grant_ttl_ms: int,
                 transport: Optional[Transport] = None):
        super().__init__("cluster-supervisor",
                         max(0.05, min(replication_interval_ms,
                                       lease_interval_ms) / 1000 / 2))
        self.k = kernel
        self.replication_interval_ms = replication_interval_ms
        self.lease_interval_ms = lease_interval_ms
        self.grant_ttl_ms = grant_ttl_ms
        self.transport = transport or http_transport
        self._children: list[_StopLoop] = []

    def _stop_children(self) -> None:
        for c in self._children:
            c.stop()
        self._children = []

    def _reconcile(self) -> None:
        role = self.k.cluster.role
        # 备实例只要配置了复制来源就拉取（即使没有 PEERS 选举成员）；
        # 主实例的续租/主动推送只在多节点集群中进行。
        want = {
            "replicator": role == "standby" and (
                bool(self.k.cfg.peers) or bool(self.k.replica.peer_url)),
            "refresher": role == "primary" and bool(self.k.cfg.peers),
            "pusher": role == "primary" and bool(self.k.cfg.peers)}
        have = {("replicator" if isinstance(c, Replicator)
                 else "refresher" if isinstance(c, LeaseRefresher)
                 else "pusher" if isinstance(c, LeaderPusher) else "?"): c
                for c in self._children}
        for name, on in want.items():
            if bool(name in have) != on:
                if not on:
                    c = have.pop(name, None)
                    if c:
                        c.stop()
                        self._children = [x for x in self._children if x is not c]
                else:
                    if name == "replicator":
                        c = Replicator(self.k, self.replication_interval_ms,
                                       self.transport)
                    elif name == "refresher":
                        c = LeaseRefresher(self.k, self.lease_interval_ms,
                                           self.grant_ttl_ms, self.transport)
                    else:
                        c = LeaderPusher(self.k, self.replication_interval_ms,
                                         self.transport)
                    c.start()
                    self._children.append(c)

    def run(self) -> None:
        while not self.stop_evt.wait(self.interval):
            try:
                self._reconcile()
            except Exception:
                pass
        self._stop_children()
