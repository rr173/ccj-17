"""追加日志段：哈希链记录、滚动、扫描、校验。

磁盘布局：
  data/segments/seg-000001.log   每行一个 JSON 记录（末尾 \\n）
  data/segments/seg-000002.log
  ...

记录是一条哈希链：
  record_n = {seq, ts, type, payload, prev: digest(record_{n-1})}
  digest(record) = sha256(canonical({seq,ts,type,payload,prev}))
段间锚点：段的第一个记录的 prev = 上一段最后一个记录的摘要；
第一段以 64 个 0（GENESIS）为锚点。任何字节被篡改，从头校验立刻断裂。

活动段是唯一只追加、不回收的段；其余 sealed 段可被压缩。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterator, Optional

from .common import (
    GENESIS,
    canonical,
    digest_json,
    ensure_dir,
    err,
)

PREFIX = "seg-"
SUFFIX = ".log"

VALID_TYPES = {"put", "delete", "data"}  # put/delete=KV 业务事件；data=不透明记录


def seg_name(seg_id: int) -> str:
    return f"{PREFIX}{seg_id:06d}{SUFFIX}"


def parse_seg_name(name: str) -> Optional[int]:
    if name.startswith(PREFIX) and name.endswith(SUFFIX):
        try:
            return int(name[len(PREFIX): -len(SUFFIX)])
        except ValueError:
            return None
    return None


def record_digest(rec: dict) -> str:
    return digest_json(
        {"seq": rec["seq"], "ts": rec["ts"], "type": rec["type"], "payload": rec["payload"], "prev": rec["prev"]}
    )


def record_line(rec: dict) -> bytes:
    return canonical(rec) + b"\n"


@dataclass(frozen=True)
class SegMeta:
    seg_id: int
    path: str
    first_seq: int
    last_seq: int
    count: int
    size: int
    sealed: bool  # False = 活动段

    def contains(self, seq: int) -> bool:
        return self.first_seq <= seq <= self.last_seq


@dataclass
class VerifyResult:
    ok: bool
    records: int
    last_seq: int
    last_digest: str
    broken_at: Optional[dict] = None  # {"seg":..,"line":..,"reason":..}

    @property
    def tip(self) -> tuple[int, str]:
        return self.last_seq, self.last_digest


class SegmentLog:
    """对段目录的读写与内存索引。线程安全由 Kernel 元数据锁保证。"""

    def __init__(self, dirpath: str):
        self.dir = dirpath
        ensure_dir(dirpath)
        self.segments: dict[int, SegMeta] = {}
        self.active_id: Optional[int] = None
        self.next_seq = 1
        self._last_digest = GENESIS
        self._fh: Optional[Any] = None  # 活动段文件句柄（append 模式）

    # ---------- 启动 ----------

    def scan(self) -> None:
        """读取磁盘重建索引；截断段尾不完整/损坏的最后一行。

        每段独立按段内哈希链校验（跨段锚点由 Kernel 依据快照指针复核）；
        段内中途断链处之后的内容视为未完成写入，一并截断。
        """
        self.segments.clear()
        ids = sorted(pid for pid in (parse_seg_name(n) for n in os.listdir(self.dir)) if pid is not None)
        for seg_id in ids:
            path = os.path.join(self.dir, seg_name(seg_id))
            good: list[tuple[dict, str, int]] = []  # (rec, digest, raw_len)
            anchor = None
            good_bytes = 0
            with open(path, "rb") as f:
                for raw in f:
                    if not raw.endswith(b"\n"):
                        break  # 截断的半行
                    try:
                        rec = json.loads(raw.decode("utf-8"))
                        d = record_digest(rec)
                        if anchor is None:
                            anchor = rec["prev"]
                        elif rec["prev"] != anchor:
                            break
                    except Exception:
                        break
                    good.append((rec, d, len(raw)))
                    anchor = d
                    good_bytes += len(raw)
            size = os.path.getsize(path)
            if good_bytes < size:
                with open(path, "r+b") as f:
                    f.truncate(good_bytes)
                size = good_bytes
            if not good:
                continue
            first_rec = good[0][0]
            last_rec = good[-1][0]
            self.segments[seg_id] = SegMeta(
                seg_id, path, first_rec["seq"], last_rec["seq"], len(good), size,
                sealed=seg_id != ids[-1],
            )
        if self.segments:
            active_id = ids[-1]
            # 最高编号文件为空（崩溃于滚动后）：补一个空活动段索引
            if active_id not in self.segments:
                path = os.path.join(self.dir, seg_name(active_id))
                prior = self.segments[max(self.segments)]
                self.segments[active_id] = SegMeta(
                    active_id, path, prior.last_seq + 1, prior.last_seq, 0,
                    os.path.getsize(path) if os.path.exists(path) else 0, sealed=False)
            self._open_active(active_id)
            active = self.segments[active_id]
            self.next_seq = active.last_seq + 1
            # _last_digest 由 Kernel 在校验跨段锚点后校准；这里先取活动段尾
            self._last_digest = self._tail_digest(active_id)
        else:
            self._open_active(1)

    def _tail_digest(self, seg_id: int) -> str:
        recs = list(self.iter_segment(seg_id))
        return recs[-1]["digest"] if recs else GENESIS

    def _open_active(self, seg_id: int) -> None:
        path = os.path.join(self.dir, seg_name(seg_id))
        self._fh = open(path, "ab")
        self.active_id = seg_id
        if seg_id not in self.segments:
            self.segments[seg_id] = SegMeta(seg_id, path, self.next_seq, self.next_seq - 1, 0, 0, sealed=False)

    # ---------- 追加 ----------

    def append(self, rec_type: str, payload: Any, ts: int) -> dict:
        if rec_type not in VALID_TYPES:
            raise err(400, "bad_request", f"unknown record type {rec_type!r}")
        if self.active_id is None:
            self._open_active(max(self.segments, default=0) + 1)
        rec = {
            "seq": self.next_seq,
            "ts": ts,
            "type": rec_type,
            "payload": payload,
            "prev": self._last_digest,
        }
        line = record_line(rec)
        assert self._fh is not None
        self._fh.write(line)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        d = record_digest(rec)
        meta = self.segments[self.active_id]  # type: ignore[index]
        self.segments[self.active_id] = SegMeta(  # type: ignore[index]
            meta.seg_id,
            meta.path,
            meta.first_seq,
            rec["seq"],
            meta.count + 1,
            meta.size + len(line),
            sealed=False,
        )
        self.next_seq += 1
        self._last_digest = d
        rec["digest"] = d
        return rec

    def rotate(self) -> int:
        """关闭活动段并开新段，返回新活动段 id。空段不滚动。"""
        if self.active_id is None or self.segments[self.active_id].count == 0:
            return self.active_id or self._open_active_id()
        old_id = self.active_id
        meta = self.segments[old_id]
        self.segments[old_id] = SegMeta(meta.seg_id, meta.path, meta.first_seq, meta.last_seq, meta.count, meta.size, True)
        assert self._fh is not None
        self._fh.close()
        self._fh = None
        new_id = old_id + 1
        self._open_active(new_id)
        return new_id

    def _open_active_id(self) -> int:
        self._open_active(max(self.segments, default=0) + 1)
        return self.active_id  # type: ignore[return-value]

    # ---------- 读取 ----------

    def read_records(self, start_seq: int, limit: int) -> list[dict]:
        out: list[dict] = []
        for seg_id, meta in sorted(self.segments.items()):
            if meta.last_seq < start_seq:
                continue
            with open(meta.path, "rb") as f:
                for raw in f:
                    if not raw.endswith(b"\n"):
                        break
                    rec = json.loads(raw.decode("utf-8"))
                    if rec["seq"] < start_seq:
                        continue
                    if len(out) >= limit:
                        return out
                    rec["digest"] = record_digest(rec)
                    out.append(rec)
            if len(out) >= limit:
                break
        return out

    def iter_segment(self, seg_id: int, *, stop_at_bad: bool = True) -> Iterator[dict]:
        meta = self.segments.get(seg_id)
        if meta is None:
            raise err(404, "not_found", f"segment {seg_id} not found")
        with open(meta.path, "rb") as f:
            for raw in f:
                if not raw.endswith(b"\n"):
                    break
                try:
                    rec = json.loads(raw.decode("utf-8"))
                    rec["digest"] = record_digest(rec)
                except Exception:
                    if stop_at_bad:
                        return  # 交给锚点比对报告断链
                    raise
                yield rec

    def locate(self, seq: int) -> Optional[int]:
        """返回包含 seq 的段 id；seq 落在空隙（已被压缩）时返回 None。"""
        for seg_id, meta in sorted(self.segments.items()):
            if meta.first_seq <= seq <= meta.last_seq:
                return seg_id
        return None

    def segment_for_pin(self, seq: int) -> Optional[int]:
        """钉住 seq 时，包含 seq 的段及其后都不能回收；返回受保护的最老段。

        seq <= first_seq_of_chain 时返回最老段（全保护）。
        seq 处于已压缩空洞时，保护链上现存最老段。
        """
        if not self.segments:
            return None
        oldest = min(self.segments)
        if seq <= self.segments[oldest].first_seq:
            return oldest
        sid = self.locate(seq)
        if sid is not None:
            return sid
        # 空洞：保护空洞之后的第一个现存段
        for cand, meta in sorted(self.segments.items()):
            if meta.first_seq > seq:
                return cand
        return None

    # ---------- 删除 ----------

    def delete_segment(self, seg_id: int) -> None:
        meta = self.segments.pop(seg_id, None)
        if meta:
            os.remove(meta.path)

    # ---------- 校验 ----------

    def verify_chain(self) -> VerifyResult:
        """校验现存所有段的哈希链（含跨段锚点）。"""
        return self.verify_chain_from(GENESIS)

    def verify_chain_from(self, anchor: str) -> VerifyResult:
        """从给定锚点摘要起，按段 id 顺序校验现存链。

        现存最老段第一条记录的 prev 必须等于 anchor；之后逐记录、逐段衔接。
        """
        prev = anchor
        total = 0
        last_seq = 0
        for seg_id, meta in sorted(self.segments.items()):
            for lineno, rec in enumerate(self.iter_segment(seg_id), 1):
                if rec["prev"] != prev:
                    return VerifyResult(False, total, last_seq, prev,
                                        {"seg": seg_id, "line": lineno,
                                         "reason": f"prev mismatch: expected {prev[:12]} got {rec['prev'][:12]}"})
                prev = rec["digest"]
                last_seq = rec["seq"]
                total += 1
        return VerifyResult(True, total, last_seq, prev)

    def verify_segment_against_anchor(self, seg_id: int, expected_anchor: str) -> VerifyResult:
        """校验单个段：第一个记录的 prev 必须等于 expected_anchor。"""
        prev = expected_anchor
        total = 0
        last_seq = 0
        for lineno, rec in enumerate(self.iter_segment(seg_id), 1):
            if rec["prev"] != prev:
                return VerifyResult(False, total, last_seq, prev,
                                    {"seg": seg_id, "line": lineno, "reason": "anchor/prev mismatch"})
            prev = rec["digest"]
            last_seq = rec["seq"]
            total += 1
        return VerifyResult(True, total, last_seq, prev)

    def tip(self) -> tuple[int, str]:
        return self.next_seq - 1, self._last_digest

    def meta_view(self) -> list[dict]:
        return [
            {"seg": m.seg_id, "first_seq": m.first_seq, "last_seq": m.last_seq,
             "records": m.count, "size": m.size, "sealed": m.sealed}
            for m in (self.segments[k] for k in sorted(self.segments))
        ]
