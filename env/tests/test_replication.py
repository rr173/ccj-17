"""主备复制与故障切换测试（同进程直连 transport，无需起 HTTP）。

覆盖：
- 初始一致边界 + 增量追平、角色/序号/边界/延迟/最近错误状态接口
- 中断后从已确认位置继续；重复数据不重复应用
- 已确认位置分叉 -> conflict 冻结，不静默覆盖、禁止提升
- 全量快照安装：全新备库、落后重装、批次复制；安装三阶段崩溃原子性
- 单调任期 + 带 TTL 授权：写门控、同任期唯一胜者、未追平不提升、
  旧主旧任期写/复制数据被拒、授权过期不可写不可续、重启不复活旧任期
"""
from __future__ import annotations

import os
import threading

import pytest

from app.cluster import MAX_GRANT_TTL_MS
from app.common import Error
from app.kernel import Config, Kernel
from app.replication import direct_transport


def make_kernel(tmp_path, name, role="primary", node_id=None,
                peers=None, source=None, grant_ttl=300_000,
                segment_bytes=10_000_000, hook=None):
    cfg = Config(
        data_dir=os.path.join(str(tmp_path), name),
        segment_bytes=segment_bytes, janitor_enabled=False,
        compaction_min_segments=1, bootstrap_role=role,
        node_id=node_id or name.upper(), peers=peers or {},
        grant_ttl_ms=grant_ttl, replica_source=source, crash_hook=hook,
    )
    k = Kernel(cfg)
    k.startup()
    return k


def pair(tmp_path, grants=300_000, segment_bytes=10_000_000):
    p = make_kernel(tmp_path, "p", node_id="P",
                    peers={"S": "local://S"}, grant_ttl=grants,
                    segment_bytes=segment_bytes)
    s = make_kernel(tmp_path, "s", role="standby", node_id="S",
                    peers={"P": "local://P", "S": "local://S"},
                    source="local://P", grant_ttl=grants,
                    segment_bytes=segment_bytes)
    return p, s, direct_transport({"local://P": p, "local://S": s})


def seed(p, n, start=0):
    for i in range(start, start + n):
        p.append("put", {"key": f"k{i % 3}", "value": i})


class TestBasicReplication:
    def test_initial_sync_and_incremental(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 5)
        r = s.run_replication_cycle(t)
        assert r["applied"] == 5 and r["status"] == "caught_up"
        v = s.replication_view()
        assert v["phase"] == "caught_up"
        assert v["replication"]["synced_seq"] == 5
        assert v["replication"]["source_boundary"]["tip_seq"] == 5
        assert v["tip_digest"] == p.seglog.tip()[1]
        # 增量
        seed(p, 3, start=5)
        r = s.run_replication_cycle(t)
        assert r["applied"] == 3 and r["status"] == "caught_up"
        assert p.business_state()["state"] == s.business_state()["state"]

    def test_status_fields(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 2)
        s.run_replication_cycle(t)
        rep = s.replication_view()
        assert rep["cluster"]["role"] == "standby"
        assert rep["cluster"]["writable"] is False
        assert rep["replication"]["last_error"] is None
        assert rep["replication"]["lag_ms"] is not None
        pview = p.replication_view()
        assert pview["phase"] == "writable_primary"
        assert pview["cluster"]["grant_valid"] is True

    def test_standby_rejects_writes(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 1)
        s.run_replication_cycle(t)
        with pytest.raises(Error) as e:
            s.append("put", {"key": "x", "value": 1})
        assert e.value.status == 403 and e.value.code == "not_primary"
        with pytest.raises(Error):
            s.create_batch(None, 60000)
        with pytest.raises(Error):
            s.compact()

    def test_resume_from_confirmed_position_and_duplicate_apply(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 8)
        s.run_replication_cycle(t)
        # 再来一轮：整段重复 -> 全部跳过，不重复应用
        r = s.run_replication_cycle(t)
        assert r["applied"] == 0
        assert s.replica.synced_seq == 8
        # 模拟拉取页含重复段+新段（from=6）：6..8 跳过，新记录应用
        seed(p, 2, start=8)
        page = p.export_records(6, 10, p.cluster.term)
        assert [r["seq"] for r in page["records"]] == [6, 7, 8, 9, 10]
        applied = s.apply_records(page)
        assert applied == 2
        assert s.replica.synced_seq == 10
        assert p.business_state()["state"] == s.business_state()["state"]

    def test_restart_keeps_progress(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 6)
        s.run_replication_cycle(t)
        cfg = s.cfg
        del s
        s2 = Kernel(cfg)
        rec = s2.startup()
        assert rec["replica"]["synced_seq"] == 6
        seed(p, 2, start=6)
        r = s2.run_replication_cycle(t)
        assert r["status"] == "caught_up" and r["synced_seq"] == 8

    def test_recent_error_recorded_and_cleared(self, tmp_path):
        p, s, t = pair(tmp_path)
        # 指向不存在的来源 -> 记录最近错误，状态保持 syncing
        s.replica.peer_url = "local://NOPE"
        s.replica.persist()
        r = s.run_replication_cycle(t)
        assert "error" in r
        assert s.replica.last_error is not None
        assert s.replica.status == "syncing"
        # 恢复来源后成功一轮，错误清空
        s.replica.peer_url = "local://P"
        s.replica.persist()
        seed(p, 2)
        s.run_replication_cycle(t)
        assert s.replica.last_error is None


class TestDivergence:
    def test_divergent_record_at_confirmed_position_conflicts(self, tmp_path):
        p, s, t = pair(tmp_path)
        x = make_kernel(tmp_path, "x", node_id="X", peers={"S": "local://S"})
        tx = direct_transport({"local://X": x})
        for i in range(3):
            seed(p, 1, i)
            seed(x, 1, i)
        for i in range(3, 6):
            p.append("put", {"key": "from_p", "value": i})
            x.append("put", {"key": "from_x", "value": 100 + i})
        s.run_replication_cycle(t)
        assert s.replica.synced_seq == 6
        x.append("put", {"key": "more", "value": 1})
        s.replica.peer_url = "local://X"
        s.replica.persist()
        r = s.run_replication_cycle(tx)
        assert r["error"] == "replication_conflict"
        assert s.replica.status == "conflict"
        assert s.replication_view()["phase"] == "conflict"
        # 冲突冻结：周期跳过、提升拒绝
        assert s.run_replication_cycle(tx) == {"skipped": "conflict"}
        with pytest.raises(Error) as e:
            s.campaign(term=2, voter_urls=["local://X", "local://S"], transport=tx)
        assert e.value.code == "replication_conflict"
        # 本地已验证数据仍可读，且没有被分叉内容污染
        st = s.business_state()["state"]
        assert st.get("from_p") == 5 and "from_x" not in st
        # 只有显式重置才能离开冲突
        view = s.reset_replica()
        assert view["replication"]["role_status"] == "idle"

    def test_duplicate_segment_with_different_digest_conflicts(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 4)
        s.run_replication_cycle(t)
        # 伪造一页：seq 3 是不同内容（prev 照旧伪装），且 seq 已被确认
        good = p.export_records(3, 2, p.cluster.term)["records"]
        bogus = dict(good[0])
        bogus["payload"] = {"key": "k0", "value": 9999}
        from app.segment import record_digest
        page = {"records": [bogus] + good[1:], "tip_seq": 4}
        with pytest.raises(Error) as e:
            s.apply_records(page)
        assert e.value.code == "replication_conflict"
        assert s.replica.status == "conflict"


class TestSnapshotInstall:
    def test_fresh_standby_installs_boundary_then_follows(self, tmp_path):
        p = make_kernel(tmp_path, "p", node_id="P", peers={"S": "local://S"},
                        segment_bytes=300)
        seed(p, 30)
        r = p.compact(force=True)
        assert r["status"] == "ok" and r["range"]["last_seq"] >= 24
        s = make_kernel(tmp_path, "s", role="standby", node_id="S",
                        peers={"P": "local://P", "S": "local://S"},
                        source="local://P", segment_bytes=300)
        t = direct_transport({"local://P": p, "local://S": s})
        r = s.run_replication_cycle(t)
        assert r["installed_snapshot"] is True
        assert r["status"] == "caught_up"
        assert p.seglog.tip() == s.seglog.tip()
        assert p.business_state()["state"] == s.business_state()["state"]
        # 已压缩序号在备端也是 410
        with pytest.raises(Error) as e:
            s.read(1, 1)
        assert e.value.code == "compacted"

    def test_lagging_standby_reinstalls_boundary(self, tmp_path):
        p, s, t = pair(tmp_path, segment_bytes=300)
        seed(p, 20)
        s.run_replication_cycle(t)
        r = p.compact(force=True)
        assert r["status"] == "ok"
        seed(p, 4, start=20)
        # 把备库已确认位置人为回退到边界之内
        d5 = s.seglog.read_records(5, 1)[0]["digest"]
        s.replica.synced_seq = 5
        s.replica.synced_digest = d5
        s.replica.status = "syncing"
        s.replica.persist()
        r = s.run_replication_cycle(t)
        assert r["installed_snapshot"] is True
        assert r["status"] == "caught_up" and r["synced_seq"] == 24
        assert p.business_state()["state"] == s.business_state()["state"]

    def test_batch_records_replicate_as_one_tail(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 3)
        s.run_replication_cycle(t)
        b = p.create_batch("order-x", 60_000)
        p.batch_add_ops(b["batch_id"], [
            {"type": "put", "payload": {"key": "ba", "value": 1}},
            {"type": "put", "payload": {"key": "bb", "value": 2}},
        ])
        res = p.commit_batch(b["batch_id"])
        r = s.run_replication_cycle(t)
        assert r["synced_seq"] == res["last_seq"]
        st = s.business_state()["state"]
        assert st["ba"] == 1 and st["bb"] == 2

    @pytest.mark.parametrize("hook", [
        "snapshot_after_verify", "snapshot_after_write", "snapshot_after_switch"])
    def test_install_crash_leaves_one_complete_state(self, tmp_path, hook):
        # 主：小段位 + 压缩边界
        p = make_kernel(tmp_path, "p", node_id="P", peers={"S": "local://S"},
                        segment_bytes=300)
        seed(p, 24)
        p.compact(force=True)
        bnd = p.export_boundary()
        snap = p.export_snapshot(bnd["checkpoint"]["gen"], bnd["term"])

        sdir = os.path.join(str(tmp_path), "s")
        pid = os.fork()
        if pid == 0:  # 子进程在指定阶段断电
            cfg = Config(data_dir=sdir, segment_bytes=300, janitor_enabled=False,
                         compaction_min_segments=1, bootstrap_role="standby",
                         node_id="S", peers={"P": "local://P"},
                         grant_ttl_ms=60_000, replica_source="local://P",
                         crash_hook=hook)
            child = Kernel(cfg)
            try:
                child.startup()
                child.install_snapshot(snap)
            except BaseException:
                pass
            os._exit(0)
        os.waitpid(pid, 0)

        cfg2 = Config(data_dir=sdir, segment_bytes=300, janitor_enabled=False,
                      compaction_min_segments=1, bootstrap_role="standby",
                      node_id="S", peers={"P": "local://P"},
                      grant_ttl_ms=60_000, replica_source="local://P")
        s2 = Kernel(cfg2)
        s2.startup()
        t = direct_transport({"local://P": p})
        s2.run_replication_cycle(t)
        assert s2.seglog.tip()[0] == bnd["tip_seq"]
        assert p.business_state()["state"] == s2.business_state()["state"]
        # 再次重启，幂等补清理不产生半成品
        del s2
        s3 = Kernel(cfg2)
        s3.startup()
        assert s3.seglog.tip()[0] == bnd["tip_seq"]
        seed(p, 2, start=bnd["tip_seq"])
        r = s3.run_replication_cycle(t)
        assert r["status"] == "caught_up"
        assert p.business_state()["state"] == s3.business_state()["state"]

    def test_crash_after_records_fsync_resumes_without_dup(self, tmp_path):
        # 记录已入链、确认位置未推进：重启时对账前滚，不重复应用
        p, s, t = pair(tmp_path)
        seed(p, 5)
        s.run_replication_cycle(t)
        seed(p, 3, start=5)
        # 再导入 3 条但把进度文件钉在 5（模拟 advance 前崩溃）
        page = p.export_records(6, 3, p.cluster.term)
        assert [r["seq"] for r in page["records"]] == [6, 7, 8]
        s.seglog.import_records(page["records"])
        s._persist_head()
        cfg = s.cfg
        del s
        s2 = Kernel(cfg)
        rec = s2.startup()
        assert rec["replica"]["synced_seq"] == 8
        seed(p, 2, start=8)
        r = s2.run_replication_cycle(t)
        assert r["applied"] == 2 and r["synced_seq"] == 10
        assert p.business_state()["state"] == s2.business_state()["state"]


class TestElectionAndGrant:
    def test_grant_ttl_gates_writes(self, tmp_path):
        p = make_kernel(tmp_path, "p", node_id="P", grant_ttl=60_000)
        assert p.replication_view()["phase"] == "writable_primary"
        p.cluster.s.grant_expires_at = 1  # 模拟过期
        with pytest.raises(Error) as e:
            p.append("put", {"key": "x", "value": 1})
        assert e.value.code == "grant_expired"
        assert p.replication_view()["phase"] == "grant_invalid"
        # 过期不能续，必须重新竞选
        with pytest.raises(Error) as e:
            p.renew_grant(60_000)
        assert e.value.code == "grant_expired"

    def test_cannot_promote_before_caught_up(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 10)  # 备从未同步
        with pytest.raises(Error) as e:
            s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        assert e.value.code == "never_synced"
        s.run_replication_cycle(t)
        seed(p, 5, start=10)  # 同步过但落后：来源边界前进到 15，本地停在 10
        bnd = p.export_boundary()
        s.replica.set_boundary({
            "term": bnd["term"], "tip_seq": bnd["tip_seq"],
            "tip_digest": bnd["tip_digest"],
            "checkpoint_gen": 0, "checkpoint_seq": 0})
        # 未追到要求序号：本地门槛在拉票前就拒绝
        with pytest.raises(Error) as e:
            s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        assert e.value.code == "behind_requirement"
        assert e.value.details == {"synced_seq": 10, "required_seq": 15}
        # 显式指定更高的 required_seq（授权要求的序号）时同样被拒
        with pytest.raises(Error) as e:
            s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t,
                       min_catch_up_seq=14)
        assert e.value.code == "behind_requirement"
        # 追上后门槛消失：先 stepdown，再提升成功
        s.run_replication_cycle(t)
        assert s.replica.synced_seq == 15
        p.stepdown()
        res = s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        assert res["term"] == 2 and res["votes"] == 2

    def test_valid_primary_denies_votes_then_stepdown_allows(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 4)
        s.run_replication_cycle(t)
        # 当前主授权有效：拒绝更高/同任期投票
        resp = p.handle_request_vote(
            {"term": 2, "candidate": "S", "last_log_seq": 4,
             "last_log_digest": s.seglog.tip()[1]})
        assert resp["vote_granted"] is False and resp["reason"] == "leader_valid"
        with pytest.raises(Error) as e:
            s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        assert e.value.code == "election_lost"
        # 交接后竞选成功
        p.stepdown()
        res = s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        assert res["term"] == 2 and res["votes"] == 2 and res["needed"] == 2
        assert s.cluster.role == "primary" and s.cluster.grant_valid()

    def test_only_one_winner_per_term_concurrent(self, tmp_path):
        p, s, t = pair(tmp_path)
        third = make_kernel(tmp_path, "t", role="standby", node_id="T",
                            peers={"P": "local://P", "S": "local://S",
                                   "T": "local://T"},
                            source="local://P")
        t3 = direct_transport({"local://P": p, "local://S": s,
                               "local://T": third})
        seed(p, 4)
        s.run_replication_cycle(t)
        third.run_replication_cycle(t3)
        p.stepdown()
        # S 赢得 term 2（P 在投票时把本任期票投给了 S）
        res = s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        assert res["term"] == 2 and res["votes"] == 2
        # 第三个实例也想拿 term 2：P 已投 S、S 是有效主，拿不到多数
        with pytest.raises(Error) as e:
            third.campaign(term=2,
                           voter_urls=["local://P", "local://S", "local://T"],
                           transport=t3)
        assert e.value.code == "election_lost"
        reasons = {r["reason"] for r in e.value.details["refusals"]}
        assert {"already_voted", "leader_valid"} <= reasons
        # S 作为有效主也拒绝再来一个 term 2 候选
        resp = s.handle_request_vote(
            {"term": 2, "candidate": "P", "last_log_seq": 4})
        assert resp["vote_granted"] is False

    def test_concurrent_same_term_campaign_exactly_one_wins(self, tmp_path):
        # 两个已追平的备用实例并发竞选同一任期：恰好一个成功，
        # 另一个收到明确拒绝；不允许两个都失败，也不允许两个都成为主。
        p = make_kernel(tmp_path, "p", node_id="P",
                        peers={"P": "local://P", "S": "local://S",
                               "T": "local://T"}, grant_ttl=300_000)
        s = make_kernel(tmp_path, "s", role="standby", node_id="S",
                        peers={"P": "local://P", "S": "local://S",
                               "T": "local://T"},
                        source="local://P", grant_ttl=300_000)
        third = make_kernel(tmp_path, "t", role="standby", node_id="T",
                            peers={"P": "local://P", "S": "local://S",
                                   "T": "local://T"},
                            source="local://P", grant_ttl=300_000)
        t3 = direct_transport({"local://P": p, "local://S": s,
                               "local://T": third})
        seed(p, 4)
        s.run_replication_cycle(t3)
        third.run_replication_cycle(t3)
        p.stepdown()

        voters = ["local://P", "local://S", "local://T"]
        # 让两个候选的首个拉票 RPC 同步后再放行，强制两边在处理任何
        # 选票前都已完成（拉票前必须先落盘的）自选票。
        barrier = threading.Barrier(2)
        gate_lock = threading.Lock()
        arrived: set[int] = set()

        def synced_transport(method, url, body=None, qs=None):
            if method == "POST" and url.endswith("/cluster/request_vote"):
                tid = id(threading.current_thread())
                with gate_lock:
                    first = tid not in arrived
                    arrived.add(tid)
                if first:
                    barrier.wait(timeout=5)
            return t3(method, url, body, qs)

        outcomes: dict[str, tuple] = {}

        def run_campaign(who, kernel):
            try:
                outcomes[who] = ("won", kernel.campaign(
                    term=2, voter_urls=voters, transport=synced_transport))
            except Error as e:
                outcomes[who] = ("lost", e)

        threads = [threading.Thread(target=run_campaign, args=("S", s)),
                   threading.Thread(target=run_campaign, args=("T", third))]
        for th in threads:
            th.start()
        for th in threads:
            th.join(10)
            assert not th.is_alive(), "campaign thread hung"

        winners = [n for n, r in outcomes.items() if r[0] == "won"]
        losers = [n for n, r in outcomes.items() if r[0] == "lost"]
        assert len(winners) == 1 and len(losers) == 1, outcomes
        win_res = outcomes[winners[0]][1]
        assert win_res["term"] == 2 and win_res["votes"] == 2
        wk = s if winners[0] == "S" else third
        lk = third if winners[0] == "S" else s
        assert wk.cluster.role == "primary" and wk.cluster.term == 2
        assert wk.cluster.grant_valid()
        # 败者：明确的多数派失败，拒绝理由包含对手自选票的 already_voted
        loss = outcomes[losers[0]][1]
        assert loss.status == 409 and loss.code == "election_lost"
        reasons = {r["reason"] for r in loss.details["refusals"]}
        assert "already_voted" in reasons
        # 败者仍是备；本任期票已投出并持久化（须更高任期才能再竞选）
        assert lk.cluster.role == "standby"
        assert lk.cluster.term == 2 and lk.cluster.s.voted_for == losers[0]
        # 集群中恰好一个主
        assert [n for n, k in (("P", p), ("S", s), ("T", third))
                if k.cluster.role == "primary"] == [winners[0]]

    def test_old_leader_old_term_rejected(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 4)
        s.run_replication_cycle(t)
        p.stepdown()
        s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        # 旧主：角色已是备 -> 写入被拒
        with pytest.raises(Error) as e:
            p.append("put", {"key": "stale", "value": 1})
        assert e.value.code == "not_primary"
        # 旧任期 request_vote 明确拒绝
        resp = s.handle_request_vote(
            {"term": 1, "candidate": "P", "last_log_seq": 4})
        assert resp["vote_granted"] is False and resp["reason"] == "stale_term"
        # 新主写入生效
        w = s.append("put", {"key": "new", "value": 1})
        assert w["seq"] == 5

    def test_restart_does_not_revive_old_term(self, tmp_path):
        p = make_kernel(tmp_path, "p", node_id="P", grant_ttl=60_000)
        p.append("put", {"key": "a", "value": 1})
        # 授权过期后重启：角色仍为 primary，但不再可写（旧任期不复活）
        p.cluster.s.grant_expires_at = 1
        p.cluster.persist()
        cfg = p.cfg
        del p
        p2 = Kernel(cfg)
        p2.startup()
        assert p2.cluster.term == 1
        with pytest.raises(Error) as e:
            p2.append("put", {"key": "b", "value": 2})
        assert e.value.code == "grant_expired"

    def test_restart_keeps_valid_grant_within_ttl(self, tmp_path):
        p = make_kernel(tmp_path, "p", node_id="P", grant_ttl=300_000)
        p.append("put", {"key": "a", "value": 1})
        cfg = p.cfg
        del p
        p2 = Kernel(cfg)
        p2.startup()
        assert p2.cluster.grant_valid()
        assert p2.append("put", {"key": "b", "value": 2})["seq"] == 2

    def test_term_is_monotonic_and_standby_bumps(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 1)
        s.run_replication_cycle(t)
        # 备见到更高任期（来源主换届时）前滚本地任期
        p.cluster.assume_leadership(5, 60_000)
        s.run_replication_cycle(t)
        assert s.cluster.term == 5
        # 旧任期来源的数据被明确拒绝（记录 stale_term，不应用任何内容）
        old = make_kernel(tmp_path, "old", node_id="O", grant_ttl=60_000)
        assert old.cluster.term == 1  # 旧主停在 term 1
        old.append("put", {"key": "stale", "value": 1})
        told = direct_transport({"local://O": old})
        s.replica.peer_url = "local://O"
        s.replica.persist()
        r = s.run_replication_cycle(told)
        assert r["error"] == "stale_term"
        assert s.replica.last_error["code"] == "stale_term"
        # 本地链没有被旧任期数据污染
        assert "stale" not in s.business_state()["state"]

    def test_vote_denied_for_behind_candidate(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 10)
        s.run_replication_cycle(t)
        # 授权过期后才接受更高任期的投票请求；但候选日志落后仍拒绝
        p.cluster.s.grant_expires_at = 1
        p.cluster.persist()
        resp = p.handle_request_vote(
            {"term": 9, "candidate": "S", "last_log_seq": 3})
        assert resp["vote_granted"] is False and resp["reason"] == "candidate_behind"

    def test_old_primary_follows_new_primary_then_restarts(self, tmp_path):
        p, s, t = pair(tmp_path)
        seed(p, 6)
        s.run_replication_cycle(t)
        p.stepdown()
        s.campaign(term=2, voter_urls=["local://P", "local://S"], transport=t)
        # 新主写入；旧主转为备跟随，只拉增量（不重装、不重复）
        seed(s, 2, start=6)
        p.configure_replica("local://S")
        r = p.run_replication_cycle(t)
        assert r["applied"] == 2 and r["synced_seq"] == 8
        assert p.cluster.term == 2 and p.cluster.role == "standby"
        assert p.business_state()["state"] == s.business_state()["state"]
        seed(s, 2, start=8)
        r = p.run_replication_cycle(t)
        assert r["synced_seq"] == 10 and r["status"] == "caught_up"
        # 重启后角色/任期/复制进度都保持，继续增量
        cfg = p.cfg
        del p
        p2 = Kernel(cfg)
        p2.startup()
        assert p2.cluster.role == "standby" and p2.cluster.term == 2
        seed(s, 1, start=10)
        r = p2.run_replication_cycle(t)
        assert r["synced_seq"] == 11 and r["status"] == "caught_up"
        assert p2.business_state()["state"] == s.business_state()["state"]

    def test_old_primary_with_divergent_history_cannot_follow(self, tmp_path):
        s = make_kernel(tmp_path, "s", node_id="S",
                        peers={"O": "local://O", "S": "local://S"})
        o = make_kernel(tmp_path, "o", node_id="O",
                        peers={"O": "local://O", "S": "local://S"})
        # 同序号、不同内容的两条独立历史
        seed(s, 5)
        seed(o, 5)
        o.stepdown()
        t = direct_transport({"local://S": s, "local://O": o})
        o.configure_replica("local://S")
        r = o.run_replication_cycle(t)
        # 来源不承认 O 的链尖：不能假定一致，停在冲突，不静默覆盖
        assert r["error"] == "replication_conflict"
        assert o.replica.status == "conflict"

    def test_request_vote_persistence_and_replay(self, tmp_path):
        p = make_kernel(tmp_path, "p", node_id="P", grant_ttl=60_000)
        # 授权过期后允许更高任期投票
        p.cluster.s.grant_expires_at = 1
        p.cluster.persist()
        r1 = p.handle_request_vote(
            {"term": 3, "candidate": "S", "last_log_seq": 0})
        assert r1["vote_granted"] is True
        # 同任期另一候选被拒（投票已持久化）
        r2 = p.handle_request_vote(
            {"term": 3, "candidate": "X", "last_log_seq": 0})
        assert r2["vote_granted"] is False and r2["reason"] == "already_voted"
        # 同一候选重放允许（幂等）
        r3 = p.handle_request_vote(
            {"term": 3, "candidate": "S", "last_log_seq": 0})
        assert r3["vote_granted"] is True
