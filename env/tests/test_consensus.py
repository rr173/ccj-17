"""多数派提交水位、write_id 幂等、线性一致读与故障切换语义测试。

覆盖需求中列举的场景：
- 多数派正常确认：写入在主本地持久化后等待当前任期多数确认同一日志位置
- 少数派隔离：写入超时返回 commit_timeout（含任期/序号/write_id）
- 同 write_id 重试：继续原提交过程，不追加重复记录、不返回另一个序号
- 并发写入保持日志顺序；原子批次不会在提交水位两侧拆开
- 主在本地持久化后中断：重启后记录保持待定，重试继续同一序号
- 新主任期处理旧待定尾部：只保留多数派可证前缀，旧待定不能仅凭新主副本提交
- 已确认记录跨重启与切换保持；读取默认只暴露 <= commit_index 的内容
- 线性一致读屏障：成功、失去多数派/任期变化拒绝、备实例拒绝
"""
from __future__ import annotations

import threading

import pytest

from app.common import Error
from app.kernel import Config, Kernel
from app.replication import direct_transport


def make_kernel(tmp_path, name, role="primary", node_id=None,
                peers=None, source=None, grant_ttl=300_000,
                segment_bytes=10_000_000, commit_timeout=3_000):
    cfg = Config(
        data_dir=f"{tmp_path}/{name}", segment_bytes=segment_bytes,
        janitor_enabled=False, compaction_min_segments=1,
        bootstrap_role=role, node_id=node_id or name.upper(),
        peers=peers or {}, grant_ttl_ms=grant_ttl, replica_source=source,
        commit_timeout_ms=commit_timeout)
    k = Kernel(cfg)
    k.startup()
    return k


def trio(tmp_path):
    peers = {"P": "local://P", "S": "local://S", "T": "local://T"}
    p = make_kernel(tmp_path, "p", node_id="P", peers=peers)
    s = make_kernel(tmp_path, "s", role="standby", node_id="S",
                    peers=peers, source="local://P")
    t = make_kernel(tmp_path, "t", role="standby", node_id="T",
                    peers=peers, source="local://P")
    tr = direct_transport({"local://P": p, "local://S": s, "local://T": t})
    return p, s, t, tr


def isolated_append(p, tr, key, value, write_id):
    """主本地持久化但不等确认（模拟成员尚未应答）。"""
    return p.append("put", {"key": key, "value": value},
                    write_id=write_id, wait_commit=False, transport=tr)


# ---------------------------------------------------------------- 基本提交


class TestQuorumCommit:
    def test_single_node_self_commits(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        r = k.append("put", {"key": "a", "value": 1}, write_id="w1")
        assert r["committed"] is True and r["seq"] == 1
        assert k.commit.commit_index == 1
        assert k.business_state()["state"] == {"a": 1}

    def test_majority_confirmation_advances_watermark(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        rec = isolated_append(p, tr, "a", 1, "w1")
        # 只有主本地：水位不动，默认读不到
        assert p.commit.commit_index == 0
        assert p.business_state()["state"] == {}
        # 一个备确认（P+S 多数）：提交
        s.run_replication_cycle(tr)
        r = p.append("put", {"key": "a", "value": 1}, write_id="w1",
                     timeout_ms=2000, transport=tr)
        assert r["committed"] is True and r["seq"] == 1
        assert p.commit.commit_index == 1
        assert s.commit.commit_index == 1
        assert s.business_state()["state"] == {"a": 1}
        # 第二个备稍后追上
        t.run_replication_cycle(tr)
        assert t.commit.commit_index == 1

    def test_minority_isolation_commit_timeout(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        rec = isolated_append(p, tr, "a", 1, "w-late")
        assert rec["seq"] == 1
        with pytest.raises(Error) as e:
            p.append("put", {"key": "a", "value": 1}, write_id="w-late",
                     timeout_ms=60, transport=tr)
        assert e.value.status == 504 and e.value.code == "commit_timeout"
        d = e.value.details
        assert d["write_id"] == "w-late" and d["term"] == 1
        assert d["first_seq"] == 1 and d["last_seq"] == 1
        # 超时记录保持待定，不暴露
        assert p.commit.commit_index == 0
        assert p.business_state()["state"] == {}

    def test_same_write_id_retry_continues_same_commit(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        isolated_append(p, tr, "a", 1, "w-r")
        with pytest.raises(Error) as e:
            p.append("put", {"key": "a", "value": 1}, write_id="w-r",
                     timeout_ms=60, transport=tr)
        assert e.value.code == "commit_timeout"
        # 备复制，相同 write_id 重试
        s.run_replication_cycle(tr)
        r = p.append("put", {"key": "a", "value": 1}, write_id="w-r",
                     timeout_ms=2000, transport=tr)
        assert r["seq"] == 1 and r["committed"] is True
        assert p.seglog.tip()[0] == 1  # 没有重复记录

    def test_retry_with_different_payload_returns_original(self, tmp_path):
        # write_id 标识的是提交过程，重试不追加新记录，忽略第二次的内容
        p, s, t, tr = trio(tmp_path)
        isolated_append(p, tr, "a", 1, "w-x")
        s.run_replication_cycle(tr)
        r = p.append("put", {"key": "totally", "value": 999},
                     write_id="w-x", timeout_ms=2000, transport=tr)
        assert r["seq"] == 1 and r["replay"] is True
        assert p.business_state()["state"] == {"a": 1}
        assert "totally" not in p.business_state()["state"]

    def test_concurrent_writes_preserve_order(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        # 预先把备跑起来：提交在后台由 ack 推进
        results = []

        def writer(i):
            try:
                # 不阻塞等待：本地持久化后立即返回，顺序由 meta_lock 保证
                r = p.append("put", {"key": f"k{i}", "value": i},
                             write_id=f"c{i}", wait_commit=False, transport=tr)
                results.append(r["seq"])
            except Error as ex:
                results.append(ex)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert sorted(results) == list(range(1, 9))
        # 链顺序与摘要链完好；两个备追上后全部提交
        for _ in range(5):
            s.run_replication_cycle(tr)
            t.run_replication_cycle(tr)
        for wid in (f"c{i}" for i in range(8)):
            rr = p.append("put", {"key": "x", "value": 0}, write_id=wid,
                          timeout_ms=2000, transport=tr)
            assert rr["committed"] is True and rr["replay"] is True
        assert p.commit.commit_index == 8
        assert p.seglog.tip()[0] == 8


class TestBatchAtomicity:
    def test_batch_never_split_across_watermark(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        b = p.create_batch("batch-1", 60_000)
        bid = b["batch_id"]
        p.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}},
                              {"type": "put", "payload": {"key": "b", "value": 2}},
                              {"type": "put", "payload": {"key": "c", "value": 3}}])
        with pytest.raises(Error) as e:
            p.commit_batch(bid, timeout_ms=60, transport=tr)
        assert e.value.code == "commit_timeout"
        # 三条都在本地但水位为 0：读/状态完全看不到整批
        assert p.commit.commit_index == 0
        assert p.business_state()["state"] == {}
        # 备复制到整批：水位只在整批边界（last_seq）推进，绝不停在 1 或 2
        cyc = s.run_replication_cycle(tr)
        assert cyc["synced_seq"] == 3 and s.commit.commit_index == 3
        assert s.business_state()["state"] == {"a": 1, "b": 2, "c": 3}
        # 主拿到多数确认后整批一次性可见（已是提交状态）
        wid = p.batches.get(bid).write_id
        res = p.commit_batch(bid, write_id=wid, timeout_ms=2000, transport=tr)
        assert res["committed"] is True
        assert (res["first_seq"], res["last_seq"]) == (1, 3)
        assert p.commit.commit_index == 3
        st = p.business_state()["state"]
        assert st == {"a": 1, "b": 2, "c": 3}

    def test_batch_retry_same_write_id_no_duplicate(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        b = p.create_batch("batch-r", 60_000)
        bid = b["batch_id"]
        p.batch_add_ops(bid, [{"type": "put", "payload": {"key": "a", "value": 1}}])
        with pytest.raises(Error):
            p.commit_batch(bid, timeout_ms=80, transport=tr)
        s.run_replication_cycle(tr)  # 备确认整条批次
        t.run_replication_cycle(tr)
        wid = p.batches.get(bid).write_id
        res = p.commit_batch(bid, write_id=wid, timeout_ms=2000, transport=tr)
        assert res["committed"] is True and res["first_seq"] == 1
        assert p.seglog.tip()[0] == 1


class TestFailover:
    def test_primary_crash_after_local_persist_keeps_pending(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        isolated_append(p, tr, "a", 1, "w-crash")
        assert p.seglog.tip()[0] == 1 and p.commit.commit_index == 0
        cfg = p.cfg
        del p
        p2 = Kernel(cfg)
        rec = p2.startup()
        # 重启后仍是待定：水位 0，记录在链，同 write_id 继续
        assert rec["commit"]["commit_index"] == 0
        assert rec["commit"]["pending"] == 1
        assert p2.seglog.tip()[0] == 1
        assert p2.business_state()["state"] == {}
        # 备此时连接新进程并确认（直连表换成 p2）
        tr2 = direct_transport({"local://P": p2, "local://S": s, "local://T": t})
        s.run_replication_cycle(tr2)
        r = p2.append("put", {"key": "a", "value": 1}, write_id="w-crash",
                      timeout_ms=2000, transport=tr2)
        assert r["seq"] == 1 and r["committed"] is True
        assert p2.seglog.tip()[0] == 1

    def test_new_leader_keeps_only_proven_prefix(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        # seq1 复制到两个备 -> 已确认
        isolated_append(p, tr, "c", 1, "w1")
        s.run_replication_cycle(tr)
        t.run_replication_cycle(tr)
        p.append("put", {"key": "c", "value": 1}, write_id="w1",
                 timeout_ms=2000, transport=tr)
        # seq2 只在主（少数派）-> 待定
        isolated_append(p, tr, "tail", 2, "w2")
        assert p.commit.commit_index == 1
        # 主失联；S 与 T（都只有 seq1）构成多数派竞选（P 不可达）
        p.cluster.invalidate_grant()
        # 让到 P 的拉票不可达（模拟主分区）
        def without_p(method, url, body=None, qs=None):
            if url.startswith("local://P"):
                raise RuntimeError("partitioned")
            return tr(method, url, body, qs)

        res = s.campaign(term=2,
                         voter_urls=["local://P", "local://S", "local://T"],
                         transport=without_p)
        # 可证前缀=1；新主写入任期标记 seq2，旧主的 seq2 被裁掉
        assert res["proven_seq"] == 1
        assert res["truncated_to"] == 1
        assert s.seglog.read_records(2, 1)[0]["type"] == "data"  # term marker
        assert s.commit.commit_index == 1  # 旧提交不回退
        # 旧待定尾部不能仅凭新主副本变已提交
        assert s.commit.pending_ranges()["proposals"][0]["kind"] == "term_marker"
        # 旧主的 w2 记录已被裁掉：新主上同 write_id 是一次新提交（seq3），
        # 但在当前任期标记确认前，它同样保持待定、不暴露。
        rec = s.append("put", {"key": "tail", "value": 2}, write_id="w2",
                       wait_commit=False, transport=tr)
        assert rec["seq"] == 3
        assert s.commit.commit_index == 1
        # T 追上来确认 marker；marker 与 seq3 都是当前任期，主一旦收到
        # T 对链尖的确认即可把水位一次推到 3（marker 保证旧提交不回退）。
        t.configure_replica("local://S")
        t.run_replication_cycle(tr)
        t.run_replication_cycle(tr)
        assert s.commit.commit_index >= 2
        r2 = s.append("put", {"key": "tail", "value": 2}, write_id="w2",
                      timeout_ms=2000, transport=tr)
        assert r2["committed"] is True and r2["seq"] == 3
        assert s.commit.commit_index == 3
        t.run_replication_cycle(tr)
        assert t.commit.commit_index == 3
        # 旧主重新跟随：它本地有一条半提交的 seq2（旧 w2），与新主历史
        # 不同 -> 明确进入冲突冻结，绝不静默覆盖已确认历史（冲突必须显式
        # 处理，不能自动混用两条历史）。
        p.configure_replica("local://S")
        first = p.run_replication_cycle(tr)
        assert first["error"] == "replication_conflict", first
        assert p.replica.status == "conflict"
        assert p.replication_view()["phase"] == "conflict"
        # 后续周期被冻结；旧 tail 记录没有被新历史污染
        assert p.run_replication_cycle(tr)["skipped"] == "conflict"

    def test_old_term_tail_needs_current_term_commit(self, tmp_path):
        # 旧任期待定尾部必须等当前任期先提交一条记录（任期标记）
        p, s, t, tr = trio(tmp_path)
        isolated_append(p, tr, "committed", 1, "w1")
        s.run_replication_cycle(tr)
        p.append("put", {"key": "committed", "value": 1}, write_id="w1",
                 timeout_ms=2000, transport=tr)
        isolated_append(p, tr, "pending", 2, "w2")
        # 主失联：S 与 T 构成多数派（S/T 都只有 seq1）
        p.cluster.invalidate_grant()

        def without_p(method, url, body=None, qs=None):
            if url.startswith("local://P"):
                raise RuntimeError("partitioned")
            return tr(method, url, body, qs)

        res = s.campaign(term=2,
                         voter_urls=["local://P", "local://S", "local://T"],
                         transport=without_p)
        # S 日志到 seq1，竞选 term2 -> proven=1，marker=seq2
        assert res["proven_seq"] == 1
        # marker 未确认前：commit 停在 1，旧 w2 不暴露
        assert s.commit.commit_index == 1
        assert s.business_state()["state"].get("pending") is None
        # T 确认 marker（当前任期第一条记录）-> 水位推进到 marker
        t.configure_replica("local://S")
        t.run_replication_cycle(tr)
        assert s.commit.commit_index == 2
        # 旧任期待定记录（旧主独占的 seq2）仍不在新主链上，永不凭新主副本提交
        assert p.seglog.tip()[0] == 2
        assert s.seglog.read_records(2, 1)[0]["type"] == "data"

    def test_committed_records_survive_restart_and_switchover(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        for i in range(3):
            isolated_append(p, tr, f"k{i % 2}", i, f"w{i}")
            s.run_replication_cycle(tr)
            p.append("put", {"key": f"k{i % 2}", "value": i},
                     write_id=f"w{i}", timeout_ms=2000, transport=tr)
        assert p.commit.commit_index == 3
        # 重启主：已确认记录与水位保持
        cfg = p.cfg
        del p
        p2 = Kernel(cfg)
        p2.startup()
        assert p2.commit.commit_index == 3
        assert p2.business_state()["state"] == {"k0": 2, "k1": 1}
        # 切换到 S：已确认记录不回退、不重复
        p2.cluster.invalidate_grant()
        tr2 = direct_transport({"local://P": p2, "local://S": s, "local://T": t})
        s.campaign(term=2, voter_urls=["local://P", "local://S", "local://T"],
                   transport=tr2)
        assert s.commit.commit_index == 3
        assert s.business_state()["state"] == {"k0": 2, "k1": 1}


class TestLinearizableRead:
    def test_barrier_succeeds_with_majority(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        view = p.linearizable_read(timeout_ms=2000, transport=tr)
        assert view["read_barrier"] == {"term": 1, "verified": True}
        assert view["state"] == {}
        isolated_append(p, tr, "a", 1, "w1")
        s.run_replication_cycle(tr)
        p.append("put", {"key": "a", "value": 1}, write_id="w1",
                 timeout_ms=2000, transport=tr)
        view = p.linearizable_read(timeout_ms=2000, transport=tr)
        assert view["state"]["a"] == 1

    def test_barrier_fails_without_majority(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        # 两个备都进入更高任期：主无法拿到当前任期多数屏障
        s.cluster.bump_term(9)
        t.cluster.bump_term(9)
        with pytest.raises(Error) as e:
            p.linearizable_read(timeout_ms=400, transport=tr)
        assert e.value.code == "term_changed"
        assert e.value.details["current_term"] == 9

    def test_barrier_rejected_when_grant_expired(self, tmp_path):
        p = make_kernel(tmp_path, "p", node_id="P", peers={})
        p.cluster.s.grant_expires_at = 1
        p.cluster.persist()
        with pytest.raises(Error) as e:
            p.linearizable_read()
        assert e.value.code == "grant_expired"

    def test_standby_rejects_linearizable_read(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        # 先拉一轮让备见到主任期（term 1）
        isolated_append(p, tr, "a", 1, "w0")
        s.run_replication_cycle(tr)
        with pytest.raises(Error) as e:
            s.linearizable_read(timeout_ms=500, transport=tr)
        assert e.value.status == 403 and e.value.code == "not_primary"
        d = e.value.details
        assert d["role"] == "standby" and d["term"] == 1
        assert d["primary"] == "local://P"

    def test_term_change_during_barrier_is_rejected(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        # 让 T 在屏障期间报告更高任期；P 自己可见的一个备跳变即失败
        orig = tr

        def flaky(method, url, body=None, qs=None):
            if body and body.get("barrier") and url.startswith("local://T"):
                # 模拟 T 已投票给更高任期主
                return 200, {"ack": False, "term": 5, "role": "standby"}
            return orig(method, url, body, qs)

        with pytest.raises(Error) as e:
            p.linearizable_read(timeout_ms=800, transport=flaky)
        assert e.value.code == "term_changed"
        assert p.cluster.role == "standby"


class TestStatusView:
    def test_replication_view_exposes_watermark_and_acks(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        isolated_append(p, tr, "a", 1, "w1")
        s.run_replication_cycle(tr)
        p.append("put", {"key": "a", "value": 1}, write_id="w1",
                 timeout_ms=2000, transport=tr)
        v = p.replication_view()
        assert v["commit_index"] == 1
        assert v["pending"]["count"] == 0
        assert v["member_acks"]["P"]["match_seq"] == 1
        assert v["member_acks"]["S"]["match_seq"] == 1
        # 待定区间视图
        isolated_append(p, tr, "b", 2, "w2")
        v = p.replication_view()
        assert v["pending"]["first_seq"] == 2 and v["pending"]["last_seq"] == 2
        assert v["commit_index"] == 1

    def test_read_clamped_to_watermark(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        isolated_append(p, tr, "a", 1, "w1")
        page = p.read(1, 10)
        assert page["records"] == [] and page["commit_index"] == 0
        assert page["ahead_of_commit"] is True
        st = p.business_state()
        assert st["state"] == {} and st["commit_index"] == 0
