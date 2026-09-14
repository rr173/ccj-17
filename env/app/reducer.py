"""业务规约器：把事件流折叠成 KV 状态。

- put {key, value} -> state[key] = value
- delete {key}     -> 从 state 删除（墓碑语义）
- data {..}        -> 不参与业务状态（仅占位计数）

state_digest = sha256(canonical(state))，用于证明
「完整重放」与「快照 + 尾部重放」得到同一业务结果。
"""
from __future__ import annotations

from typing import Any

from .common import canonical, err, sha256_bytes


class Reducer:
    def __init__(self, state: dict | None = None):
        self.state: dict[str, Any] = dict(state or {})

    def apply(self, rec_type: str, payload: Any) -> None:
        if rec_type == "put":
            if not isinstance(payload, dict) or "key" not in payload or "value" not in payload:
                raise err(400, "bad_payload", "put requires {key, value}")
            key = payload["key"]
            if not isinstance(key, str) or not key:
                raise err(400, "bad_payload", "key must be a non-empty string")
            self.state[key] = payload["value"]
        elif rec_type == "delete":
            if not isinstance(payload, dict) or "key" not in payload:
                raise err(400, "bad_payload", "delete requires {key}")
            key = payload["key"]
            if not isinstance(key, str) or not key:
                raise err(400, "bad_payload", "key must be a non-empty string")
            self.state.pop(key, None)
        elif rec_type == "data":
            pass
        else:
            raise err(400, "bad_payload", f"unknown type {rec_type!r}")

    def digest(self) -> str:
        return sha256_bytes(canonical(self.state))

    def snapshot(self) -> dict:
        return dict(self.state)
