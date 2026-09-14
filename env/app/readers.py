"""读者登记与租约（钉位）。

读者注册时声明 pin_seq（钉住的最老记录位置），并获得一个 TTL 租约；
心跳续租。只要租约未过期，被钉序号之前的段一律不回收。
状态原子落盘（tmp+rename+fsync），重启后未过期租约继续有效。
"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Optional

from .common import now_ms, read_json, validate_name, write_json


@dataclass
class Reader:
    reader_id: str
    pin_seq: int
    position: int  # 最近读到的位置（游标）
    expires_at: int
    created_at: int
    last_beat: int

    def alive(self, at: Optional[int] = None) -> bool:
        return self.expires_at > (at or now_ms())

    def to_dict(self) -> dict:
        return {
            "reader_id": self.reader_id,
            "pin_seq": self.pin_seq,
            "position": self.position,
            "expires_at": self.expires_at,
            "created_at": self.created_at,
            "last_beat": self.last_beat,
        }


class ReaderStore:
    def __init__(self, path: str):
        self.path = path
        self.readers: dict[str, Reader] = {}
        if os.path.exists(path):
            data = read_json(path)
            for r in data.get("readers", []):
                rd = Reader(**r)
                self.readers[rd.reader_id] = rd

    def _persist(self) -> None:
        write_json(
            self.path,
            {"readers": [r.to_dict() for r in self.readers.values()]},
        )

    def register(self, pin_seq: int, ttl_ms: int, now: int, reader_id: Optional[str] = None) -> Reader:
        if pin_seq < 0:
            raise ValueError("pin_seq must be >= 0")
        rid = reader_id or f"r-{uuid.uuid4().hex[:16]}"
        validate_name(rid, "reader_id")
        if rid in self.readers:
            raise KeyError(rid)
        rd = Reader(
            reader_id=rid,
            pin_seq=int(pin_seq),
            position=int(pin_seq),
            expires_at=now + ttl_ms,
            created_at=now,
            last_beat=now,
        )
        self.readers[rid] = rd
        self._persist()
        return rd

    def heartbeat(self, reader_id: str, ttl_ms: int, now: int, position: Optional[int] = None) -> Reader:
        rd = self._get(reader_id)
        if not rd.alive(now):
            raise TimeoutError(reader_id)
        rd.expires_at = now + ttl_ms
        rd.last_beat = now
        if position is not None:
            if position < rd.pin_seq:
                raise ValueError("position cannot move before pin_seq")
            rd.position = int(position)
        self._persist()
        return rd

    def release(self, reader_id: str) -> None:
        if self.readers.pop(reader_id, None) is not None:
            self._persist()

    def _get(self, reader_id: str) -> Reader:
        rd = self.readers.get(reader_id)
        if rd is None:
            raise KeyError(reader_id)
        return rd

    def get(self, reader_id: str, now: int) -> Reader:
        rd = self._get(reader_id)
        if not rd.alive(now):
            raise TimeoutError(reader_id)
        return rd

    def active(self, now: Optional[int] = None) -> list[Reader]:
        at = now or now_ms()
        return [r for r in self.readers.values() if r.alive(at)]

    def oldest_pin(self, now: Optional[int] = None) -> Optional[int]:
        alive = self.active(now)
        return min((r.pin_seq for r in alive), default=None)

    def list_view(self, now: int) -> list[dict]:
        out = []
        for r in self.readers.values():
            d = r.to_dict()
            d["alive"] = r.alive(now)
            d["remaining_ms"] = max(0, r.expires_at - now)
            out.append(d)
        return sorted(out, key=lambda x: x["reader_id"])
