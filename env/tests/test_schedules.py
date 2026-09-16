"""未来生效变更预约（scheduled changes）语义测试。

覆盖需求列举的全部场景：
- 正常到期：pending -> executing -> applied，整组原子可见、给出最终日志位置
- 相同 request_id/内容重复创建返回原预约；内容不同被拒绝
- 同一生效时间严格按创建顺序处理，后一个看到前一个完成后的状态
- 改期携带版本：版本匹配才生效，旧版本不能覆盖较新安排
- 取消与到期领取竞争：唯一结果（取消成功无业务变更 / 执行成功取消明确已开始）
- 执行中进程退出后新实例接管，沿用原 request_id 提交，不重复执行
- 成员不足进入可重试 executing（保留原日志位置），恢复后继续原过程
- 时钟回拨不会让 applied 预约再次执行
- 停机积压按原顺序补跑，受单轮上限约束分批，不阻塞即时写入
- 处理期间主切换：截断半提交组并由新主沿用请求标识重做
"""
from __future__ import annotations

import threading

import pytest

from app.common import Error, now_ms
from app.kernel import Config, Kernel
from app.replication import direct_transport
from app.schedules import (
    APPLIED,
    CANCELLED,
    EXECUTING,
    PENDING,
    SUPERSEDED,
    exec_write_id,
)


def make_kernel(tmp_path, name, role="primary", node_id=None,
                peers=None, source=None, grant_ttl=300_000,
                segment_bytes=10_000_000, commit_timeout=3_000,
                catchup_limit=100):
    cfg = Config(
        data_dir=f"{tmp_path}/{name}", segment_bytes=segment_bytes,
        janitor_enabled=False, compaction_min_segments=1,
        bootstrap_role=role, node_id=node_id or name.upper(),
        peers=peers or {}, grant_ttl_ms=grant_ttl, replica_source=source,
        commit_timeout_ms=commit_timeout,
        scheduler_enabled=False, catchup_batch_limit=catchup_limit)
    k = Kernel(cfg)
    k.startup()
    return k


class FakeClock:
    """可控墙上时钟（毫秒）。"""

    def __init__(self, t: int):
        self.t = t

    def __call__(self) -> int:
        return self.t


def trio(tmp_path, catchup_limit=100):
    peers = {"P": "local://P", "S": "local://S", "T": "local://T"}
    p = make_kernel(tmp_path, "p", node_id="P", peers=peers,
                    catchup_limit=catchup_limit)
    s = make_kernel(tmp_path, "s", role="standby", node_id="S",
                    peers=peers, source="local://P", catchup_limit=catchup_limit)
    t = make_kernel(tmp_path, "t", role="standby", node_id="T",
                    peers=peers, source="local://P", catchup_limit=catchup_limit)
    tr = direct_transport({"local://P": p, "local://S": s, "local://T": t})
    return p, s, t, tr


def put(k, v):
    return {"type": "put", "payload": {"key": k, "value": v}}


def delete(k):
    return {"type": "delete", "payload": {"key": k}}


def refresh_boundary(standby, primary, tr):
    """备只拉一次来源边界（不应用记录），用于同步预约元数据/来源视图。"""
    from app.replication import PATH_BOUNDARY
    _st, bnd = tr("GET", primary + PATH_BOUNDARY)
    standby._merge_primary_schedules(bnd["schedules"])
    standby.replica.set_boundary({
        "term": bnd["term"], "tip_seq": bnd["tip_seq"],
        "tip_digest": bnd["tip_digest"],
        "checkpoint_gen": (bnd.get("checkpoint") or {}).get("gen", 0),
        "checkpoint_seq": (bnd.get("checkpoint") or {}).get("seq", 0),
    })


# ---------------------------------------------------------------- 基本生命周期


class TestScheduleLifecycle:
    def test_normal_due_applies_group_atomically_single_node(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        future = now_ms() + 60_000
        k.create_schedule("r1", future, [put("a", 1), put("b", 2), delete("a")])
        # 未到期不执行
        assert k.scheduler_tick()["ran"] == 0
        v = k.get_schedule("r1")
        assert v["status"] == PENDING and v["first_seq"] is None
        # 到期（这里用过去时间直接模拟）
        k.create_schedule("r2", now_ms() - 1, [put("x", 9)])
        out = k.scheduler_tick()
        assert out["ran"] == 1
        got = k.get_schedule("r2")
        assert got["status"] == APPLIED
        assert (got["first_seq"], got["last_seq"]) == (1, 1)
        assert k.business_state()["state"] == {"x": 9}
        # r1 仍是 pending（未到期）
        assert k.get_schedule("r1")["status"] == PENDING

    def test_multi_op_group_all_or_nothing_visible_only_after_watermark(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        p.create_schedule("g", now_ms() - 1, [put("a", 1), put("b", 2), put("c", 3)])
        out = p.scheduler_tick()
        # 本地三条但多数派未确认：executing、保留原位置、完全不可见
        assert out["results"][0]["status"] == EXECUTING
        assert p.commit.commit_index == 0
        assert p.business_state()["state"] == {}
        sv = p.get_schedule("g")
        assert sv["status"] == EXECUTING and (sv["first_seq"], sv["last_seq"]) == (1, 3)
        # 一个备确认整组：水位只能在整组边界 3 推进
        s.run_replication_cycle(tr)
        p.scheduler_tick()
        assert p.commit.commit_index == 3
        assert p.get_schedule("g")["status"] == APPLIED
        assert p.business_state()["state"] == {"a": 1, "b": 2, "c": 3}

    def test_duplicate_same_content_returns_original(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        at = now_ms() + 60_000
        ops = [put("a", 1)]
        first = k.create_schedule("dup", at, ops)
        again = k.create_schedule("dup", at, [put("a", 1)])
        assert again.get("replay") is True
        assert again["seq"] == first["seq"] and again["version"] == 1

    def test_same_request_different_content_rejected(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        at = now_ms() + 60_000
        k.create_schedule("dup", at, [put("a", 1)])
        with pytest.raises(Error) as e:
            k.create_schedule("dup", at, [put("a", 2)])
        assert e.value.code == "schedule_conflict"
        # 不同时间但相同内容也视为不同安排 -> 拒绝（改期请走 reschedule）
        with pytest.raises(Error) as e:
            k.create_schedule("dup", at + 1, [put("a", 1)])
        assert e.value.code == "schedule_conflict"

    def test_bad_ops_rejected_before_persisting(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        at = now_ms() + 60_000
        with pytest.raises(Error) as e:
            k.create_schedule("bad", at, [{"type": "put", "payload": {"key": "a"}}])
        assert e.value.status == 400
        with pytest.raises(Error):
            k.create_schedule("bad2", at, [{"type": "data", "payload": {}}])
        with pytest.raises(Error):
            k.create_schedule("bad3", at, [])
        assert k.list_schedules()["counts"]["total"] == 0

    def test_standby_cannot_create(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        with pytest.raises(Error) as e:
            s.create_schedule("x", now_ms() + 1000, [put("a", 1)])
        assert e.value.status == 403 and e.value.code == "not_primary"


# ---------------------------------------------------------------- 顺序


class TestOrdering:
    def test_same_effective_time_processed_in_creation_order(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        at = now_ms() - 1
        k.create_schedule("s1", at, [put("k", "first")])
        k.create_schedule("s2", at, [put("k", "second")])
        k.create_schedule("s3", at, [put("k", "third")])
        k.scheduler_tick()
        for rid, f in (("s1", 1), ("s2", 2), ("s3", 3)):
            sv = k.get_schedule(rid)
            assert sv["status"] == APPLIED and sv["first_seq"] == f
        assert k.business_state()["state"] == {"k": "third"}

    def test_earlier_effective_time_goes_first_regardless_of_creation(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        now = now_ms()
        k.create_schedule("later", now - 100, [put("k", "later")])
        k.create_schedule("earlier", now - 5000, [put("e", 1)])
        k.scheduler_tick()
        assert k.get_schedule("earlier")["first_seq"] == 1
        assert k.get_schedule("later")["first_seq"] == 2


# ---------------------------------------------------------------- 改期/取消/版本


class TestRescheduleCancel:
    def test_reschedule_bumps_version_and_moves_time(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        now = now_ms()
        s = k.create_schedule("r", now - 1, [put("a", 1)])  # 已到期但未领取
        assert s["version"] == 1
        moved = k.reschedule("r", now + 60_000, expected_version=1)
        assert moved["version"] == 2 and moved["status"] == PENDING
        # tick 不再执行它（时间被推后）
        assert k.scheduler_tick()["ran"] == 0
        assert k.get_schedule("r")["status"] == PENDING

    def test_stale_version_cannot_overwrite_newer_arrangement(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        now = now_ms()
        k.create_schedule("r", now + 60_000, [put("a", 1)])
        k.reschedule("r", now + 70_000, expected_version=1)  # version -> 2
        with pytest.raises(Error) as e:
            k.reschedule("r", now + 5, expected_version=1)  # 旧版本想抢到马上
        assert e.value.code == "version_conflict"
        assert k.get_schedule("r")["effective_at"] == now + 70_000
        with pytest.raises(Error) as e:
            k.cancel_schedule("r", expected_version=1)
        assert e.value.code == "version_conflict"
        assert k.get_schedule("r")["status"] == PENDING

    def test_cancel_with_current_version_succeeds_no_change(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        now = now_ms()
        k.create_schedule("r", now - 1, [put("a", 1)])
        res = k.cancel_schedule("r", expected_version=1)
        assert res["status"] == CANCELLED and res.get("cancelled")
        k.scheduler_tick()
        assert k.business_state()["state"] == {}
        assert k.seglog.tip()[0] == 0  # 没有任何记录入链

    def test_cancel_after_start_returns_already_started(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        k.create_schedule("r", now_ms() - 1, [put("a", 1)])
        k.scheduler_tick()  # 单节点当场 applied
        with pytest.raises(Error) as e:
            k.cancel_schedule("r", expected_version=1)
        assert e.value.code == "already_started"
        assert k.get_schedule("r")["status"] == APPLIED

    def test_cancel_and_claim_race_has_unique_outcome(self, tmp_path):
        # 并发：一个线程领取执行，一个线程取消；meta_lock 串行，结果唯一。
        k = make_kernel(tmp_path, "solo", peers={})
        k.create_schedule("race", now_ms() - 1, [put("a", 1)])
        outcomes = []

        def claim():
            try:
                k.scheduler_tick()
                outcomes.append(("claimed", k.get_schedule("race")["status"]))
            except Error as ex:
                outcomes.append(("claim_err", ex.code))

        def cancel():
            try:
                k.cancel_schedule("race", expected_version=1)
                outcomes.append(("cancelled", CANCELLED))
            except Error as ex:
                outcomes.append(("cancel_rejected", ex.code))

        threads = [threading.Thread(target=claim), threading.Thread(target=cancel)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        status = k.get_schedule("race")["status"]
        if status == CANCELLED:
            # 取消先到：无业务变更，领取那侧必然没执行
            assert k.business_state()["state"] == {}
            assert ("cancelled", CANCELLED) in outcomes
        else:
            # 领取先到：执行成功，取消明确返回 already_started
            assert status == APPLIED
            assert k.business_state()["state"] == {"a": 1}
            assert ("cancel_rejected", "already_started") in outcomes
        # 终态唯一且稳定
        assert status in (APPLIED, CANCELLED)

    def test_cancel_idempotent_and_reschedule_rejected_when_closed(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        now = now_ms()
        k.create_schedule("r", now + 60_000, [put("a", 1)])
        k.cancel_schedule("r", expected_version=1)
        again = k.cancel_schedule("r", expected_version=1)
        assert again.get("replay") is True
        with pytest.raises(Error):
            k.reschedule("r", now + 1, expected_version=1)


# ---------------------------------------------------------------- 接管 / 成员不足


class TestTakeoverAndQuorum:
    def test_restart_takes_over_executing_schedule_same_request_id(self, tmp_path):
        # 主本地持久化整组后崩溃（未取得多数确认）；重启后沿用原 request_id/
        # write_id 在原位置续等，绝不追加重复记录。
        p, s, t, tr = trio(tmp_path)
        p.create_schedule("take", now_ms() - 1, [put("a", 1), put("b", 2)])
        p.scheduler_tick()
        assert p.seglog.tip()[0] == 2 and p.commit.commit_index == 0
        old_wid = p.get_schedule("take")["write_id"]
        cfg = p.cfg
        del p
        p2 = Kernel(cfg)
        rec = p2.startup()
        assert "take" in rec["schedules"]["waiting"]
        tr2 = direct_transport({"local://P": p2, "local://S": s, "local://T": t})
        sv = p2.get_schedule("take")
        assert sv["status"] == EXECUTING and sv["write_id"] == old_wid
        assert (sv["first_seq"], sv["last_seq"]) == (1, 2)
        # 备确认原位置后完成
        s.run_replication_cycle(tr2)
        p2.scheduler_tick()
        assert p2.get_schedule("take")["status"] == APPLIED
        assert p2.seglog.tip()[0] == 2  # 没有重复记录
        assert p2.business_state()["state"] == {"a": 1, "b": 2}

    def test_insufficient_members_keeps_position_and_retries(self, tmp_path):
        p, s, t, tr = trio(tmp_path)
        p.create_schedule("iso", now_ms() - 1, [put("a", 1), put("b", 2)])
        p.scheduler_tick()
        assert p.get_schedule("iso")["status"] == EXECUTING
        first = p.get_schedule("iso")["first_seq"]
        # 多轮 tick：成员不足，保持原位置、不重复追加
        for _ in range(3):
            p.scheduler_tick()
        assert p.seglog.tip()[0] == 2
        assert p.get_schedule("iso")["first_seq"] == first
        assert p.commit.commit_index == 0
        # 恢复（一个备确认）-> 原过程继续
        s.run_replication_cycle(tr)
        p.scheduler_tick()
        assert p.get_schedule("iso")["status"] == APPLIED
        assert p.commit.commit_index == 2

    def test_executing_schedule_then_leader_switch_is_redone_once(self, tmp_path):
        # 半提交组被新任期截断：新主沿用 request_id 在新位置重做整组，
        # 旧 write_id 被标记 superseded，不重复执行业务。
        p, s, t, tr = trio(tmp_path)
        # 先建立一条多数派已确认记录，让备具备可提升的来源边界（seq1）
        p.append("put", {"key": "base", "value": 1}, write_id="base",
                 wait_commit=False, transport=tr)
        s.run_replication_cycle(tr)
        t.run_replication_cycle(tr)
        p.append("put", {"key": "base", "value": 1}, write_id="base",
                 timeout_ms=2000, transport=tr)
        assert p.commit.commit_index == 1
        # 到期组只在主本地（半提交，seq2..3），备尚未复制
        p.create_schedule("sw", now_ms() - 1, [put("a", 1), put("b", 2)])
        p.scheduler_tick()
        old_wid = p.get_schedule("sw")["write_id"]
        assert p.seglog.tip()[0] == 3
        # 旧主失联；S 只可证到 seq1，组在多数派可证前缀之外。
        # S 通过边界镜像拿到预约元数据（生产中由持续复制/主动推送保持最新）。
        refresh_boundary(s, "local://P", tr)
        p.cluster.invalidate_grant()

        def without_p(method, url, body=None, qs=None):
            if url.startswith("local://P"):
                raise RuntimeError("partitioned")
            return tr(method, url, body, qs)

        res = s.campaign(term=2,
                         voter_urls=["local://P", "local://S", "local://T"],
                         transport=without_p, min_catch_up_seq=1)
        assert res["proven_seq"] == 1  # 组未被多数派证明，被裁到 seq1
        # 新主 S 上跑 tick：原组记录不在 S 链上，应在 marker(seq2) 之后重做
        out = s.scheduler_tick(transport=tr)
        assert out["ran"] == 1
        sv = s.get_schedule("sw")
        # 新位置：marker seq2 之后，组占 seq3..4
        assert sv["status"] in (EXECUTING, APPLIED)
        assert (sv["first_seq"], sv["last_seq"]) == (3, 4)
        assert sv["write_id"] != old_wid  # 纪元前滚
        # T 确认 marker + 组
        t.configure_replica("local://S")
        t.run_replication_cycle(tr)
        t.run_replication_cycle(tr)
        s.scheduler_tick(transport=tr)
        assert s.get_schedule("sw")["status"] == APPLIED
        assert s.business_state()["state"] == {"base": 1, "a": 1, "b": 2}
        assert s.seglog.tip()[0] == 4

    def test_old_instance_late_result_not_written_as_success(self, tmp_path):
        # 旧 write_id 在新任期已 superseded：用它的任何重试都被明确拒绝，
        # 不会把旧实例迟到结果写成成功。
        p, s, t, tr = trio(tmp_path)
        p.append("put", {"key": "base", "value": 1}, write_id="base",
                 wait_commit=False, transport=tr)
        s.run_replication_cycle(tr)
        t.run_replication_cycle(tr)
        p.append("put", {"key": "base", "value": 1}, write_id="base",
                 timeout_ms=2000, transport=tr)
        p.create_schedule("late", now_ms() - 1, [put("a", 1)])
        p.scheduler_tick()
        old_wid = p.get_schedule("late")["write_id"]
        refresh_boundary(s, "local://P", tr)
        p.cluster.invalidate_grant()

        def without_p(method, url, body=None, qs=None):
            if url.startswith("local://P"):
                raise RuntimeError("x")
            return tr(method, url, body, qs)

        s.campaign(term=2, voter_urls=["local://P", "local://S", "local://T"],
                   transport=without_p, min_catch_up_seq=1)
        # 旧主侧 write 登记被新任期标记 superseded 的路径：直接验证写登记表
        # 在 S 重做后，旧 wid 与新 wid 不同
        s.scheduler_tick(transport=tr)
        new_wid = s.get_schedule("late")["write_id"]
        assert new_wid != old_wid  # 纪元前滚：旧实例的迟到结果无法冒名成功
        assert new_wid.startswith("sched-w:late:")
        # 旧主 P 已失去授权：它即便稍后返回也无法把旧 wid 的结果写成成功
        # （继续同一 write_id 提交被授权/主身份门控明确拒绝）。
        with pytest.raises(Error) as ex:
            p.append("put", {"key": "a", "value": 1}, write_id=old_wid,
                     timeout_ms=500, transport=tr)
        assert ex.value.code in ("not_primary", "grant_expired",
                                 "commit_superseded", "stale_term")


# ---------------------------------------------------------------- 时钟与补跑


class TestClockAndCatchup:
    def test_clock_rollback_does_not_reapply(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        clk = FakeClock(1_000_000)
        k.set_clock(clk)
        k.create_schedule("r", 900_000, [put("a", 1)])
        k.scheduler_tick()
        assert k.get_schedule("r")["status"] == APPLIED
        assert k.business_state()["state"] == {"a": 1}
        tip_before = k.seglog.tip()[0]
        # 系统时间大幅回拨
        clk.t = 100_000
        for _ in range(3):
            k.scheduler_tick()
        assert k.get_schedule("r")["status"] == APPLIED  # 终态不再执行
        assert k.seglog.tip()[0] == tip_before            # 没有重复入链
        assert k.business_state()["state"] == {"a": 1}

    def test_downtime_backlog_runs_in_original_order_batched(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={}, catchup_limit=2)
        clk = FakeClock(1_000_000)
        k.set_clock(clk)
        # 停机期间错过 5 个预约，生效时间交错；创建顺序 s1..s5
        for i in range(5):
            k.create_schedule(f"s{i}", 500_000 + i * 1000, [put("k", i)])
        # 恢复，时钟已越过全部生效时间
        clk.t = 2_000_000
        out1 = k.scheduler_tick()
        assert out1["ran"] == 2 and out1["remaining_due"] == 3
        # 前两个按生效时间（=创建顺序）执行
        assert k.get_schedule("s0")["status"] == APPLIED
        assert k.get_schedule("s1")["status"] == APPLIED
        assert k.get_schedule("s2")["status"] == PENDING
        out2 = k.scheduler_tick()
        assert out2["ran"] == 2
        out3 = k.scheduler_tick()
        assert out3["ran"] == 1
        assert k.business_state()["state"] == {"k": 4}
        # 最终日志位置严格按生效顺序分配
        pos = {k.get_schedule(f"s{i}")["first_seq"]: f"s{i}" for i in range(5)}
        assert [pos[s_] for s_ in sorted(pos)] == [f"s{i}" for i in range(5)]

    def test_backlog_does_not_block_immediate_write(self, tmp_path):
        p, s, t, tr = trio(tmp_path, catchup_limit=1)
        clk = FakeClock(1_000_000)
        p.set_clock(clk)
        for i in range(4):
            p.create_schedule(f"s{i}", 500_000 + i * 1000, [put("bk", i)])
        clk.t = 2_000_000
        p.scheduler_tick()  # 只补跑一个
        # 即时写入不受积压影响：用普通 append 立即写（wait=False 先本地）
        r = p.append("put", {"key": "imm", "value": 1}, write_id="imm-1",
                     wait_commit=False, transport=tr)
        assert r["seq"] == 2  # s0 占 seq1，即时写紧随其后，无需等积压清空
        s.run_replication_cycle(tr)
        p.append("put", {"key": "imm", "value": 1}, write_id="imm-1",
                 timeout_ms=2000, transport=tr)
        assert p.business_state()["state"].get("imm") == 1


# ---------------------------------------------------------------- 终态/查询


class TestViews:
    def test_list_and_filter_and_counts(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        now = now_ms()
        k.create_schedule("a", now - 1, [put("a", 1)])
        k.create_schedule("b", now + 60_000, [put("b", 1)])
        k.create_schedule("c", now + 60_000, [put("c", 1)])
        k.cancel_schedule("c", expected_version=1)
        k.scheduler_tick()
        lst = k.list_schedules()
        assert lst["counts"][APPLIED] == 1
        assert lst["counts"][PENDING] == 1
        assert lst["counts"][CANCELLED] == 1
        pend = k.list_schedules(status=PENDING)["schedules"]
        assert [x["request_id"] for x in pend] == ["b"]

    def test_unknown_request_404(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        with pytest.raises(Error) as e:
            k.get_schedule("nope")
        assert e.value.status == 404

    def test_status_exposes_schedule_counts(self, tmp_path):
        k = make_kernel(tmp_path, "solo", peers={})
        k.create_schedule("a", now_ms() + 60_000, [put("a", 1)])
        assert k.status()["schedules"]["pending"] == 1
