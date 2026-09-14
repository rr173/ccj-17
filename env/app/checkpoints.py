"""快照、压缩清单（manifest）、指针与审计链。

目录布局：
  data/checkpoints/current.json          可见指针（原子替换 = 唯一提交点）
  data/checkpoints/gen-N/snapshot.json   某代快照
  data/checkpoints/gen-N/manifest.json   某代压缩清单+校验凭证
  data/checkpoints/gen-N.tmp.<rand>/     半成品（崩溃后启动时直接清除）
  data/checkpoints/audit.log             只追加审计链：每代 manifest 永久留痕

文档自校验约定：snapshot.json / manifest.json 为
  {<业务字段...>, "digest": sha256(canonical(<除 digest 外全部字段>))}
读入时重算 digest 即可识别任何篡改。

代际链：
  snapshot.body.prev_snapshot_digest -> 上一代快照摘要
  manifest.body.prev_manifest_digest -> 上一代清单摘要（首代为 GENESIS）
  manifest.body.anchor               -> 本段折叠起始的链上摘要
                                        （首代 GENESIS，后续=上一代 tail_anchor）
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

from .common import (
    GENESIS,
    append_fsync,
    canonical,
    digest_json,
    ensure_dir,
    now_ms,
    read_json,
    sha256_file,
    write_json,
)

CURRENT = "current.json"
SNAPSHOT = "snapshot.json"
MANIFEST = "manifest.json"
AUDIT = "audit.log"
DIGEST = "digest"


def seal_doc(body: dict[str, Any]) -> dict[str, Any]:
    """给文档加盖自校验摘要。"""
    assert DIGEST not in body
    return {**body, DIGEST: digest_json(body)}


def open_doc(doc: dict[str, Any], kind: str) -> dict[str, Any]:
    """校验并剥离开盖摘要；失败抛 ValueError。"""
    if not isinstance(doc, dict) or DIGEST not in doc:
        raise ValueError(f"{kind}: missing digest")
    body = {k: v for k, v in doc.items() if k != DIGEST}
    if digest_json(body) != doc[DIGEST]:
        raise ValueError(f"{kind}: digest mismatch")
    return body


class Checkpointer:
    def __init__(self, cp_dir: str, audit_path: Optional[str] = None):
        self.dir = cp_dir
        ensure_dir(cp_dir)
        self.audit_path = audit_path or os.path.join(cp_dir, AUDIT)
        self.current: Optional[dict[str, Any]] = None
        if os.path.exists(self._current_path()):
            self.current = read_json(self._current_path())

    def _current_path(self) -> str:
        return os.path.join(self.dir, CURRENT)

    # ---------- 半成品/旧代目录 ----------

    def temp_dirs(self) -> list[str]:
        out = []
        for name in os.listdir(self.dir):
            if re.fullmatch(r"gen-\d+\.tmp\..+", name):
                out.append(os.path.join(self.dir, name))
        return out

    def purge_temp(self) -> list[str]:
        removed = []
        for p in self.temp_dirs():
            _rmtree(p)
            removed.append(os.path.basename(p))
        return removed

    def gen_dir(self, gen: int) -> str:
        return os.path.join(self.dir, f"gen-{gen}")

    def existing_gens(self) -> list[int]:
        out = []
        for name in os.listdir(self.dir):
            m = re.fullmatch(r"gen-(\d+)", name)
            if m:
                out.append(int(m.group(1)))
        return sorted(out)

    def write_generation(self, gen: int, snapshot_body: dict, manifest_body: dict) -> tuple[dict, dict]:
        """在临时目录中完整落盘并 fsync，再原子改名——读者绝不会看到半成品。"""
        tmp = os.path.join(self.dir, f"gen-{gen}.tmp.{os.urandom(6).hex()}")
        ensure_dir(tmp)
        snap_doc = seal_doc(snapshot_body)
        man_doc = seal_doc(manifest_body)
        write_json(os.path.join(tmp, SNAPSHOT), snap_doc)
        write_json(os.path.join(tmp, MANIFEST), man_doc)
        target = self.gen_dir(gen)
        os.rename(tmp, target)  # 同文件系统，目录 rename 原子
        _fsync_dir(self.dir)
        return snap_doc, man_doc

    def load_generation(self, gen: int) -> tuple[dict, dict]:
        return (
            open_doc(read_json(os.path.join(self.gen_dir(gen), SNAPSHOT)), f"gen-{gen} snapshot"),
            open_doc(read_json(os.path.join(self.gen_dir(gen), MANIFEST)), f"gen-{gen} manifest"),
        )

    def commit_pointer(self, pointer: dict) -> None:
        write_json(self._current_path(), pointer)
        self.current = pointer

    def remove_gens_before(self, gen: int) -> list[int]:
        removed = []
        for g in self.existing_gens():
            if g < gen:
                _rmtree(self.gen_dir(g))
                removed.append(g)
        if removed:
            _fsync_dir(self.dir)
        return removed

    # ---------- 审计链（只追加） ----------

    def append_audit(self, entry: dict) -> None:
        body = {**entry, "ts": entry.get("ts", now_ms())}
        prev = GENESIS
        line_prev = self.last_audit_digest()
        if line_prev is not None:
            prev = line_prev
        doc = seal_doc({**body, "prev": prev})
        append_fsync(self.audit_path, canonical(doc) + b"\n")

    def last_audit_digest(self) -> Optional[str]:
        if not os.path.exists(self.audit_path):
            return None
        last = None
        with open(self.audit_path, "rb") as f:
            for raw in f:
                if raw.endswith(b"\n"):
                    last = raw
        if last is None:
            return None
        return json.loads(last.decode())[DIGEST]

    def read_audit(self) -> list[dict]:
        if not os.path.exists(self.audit_path):
            return []
        out = []
        with open(self.audit_path, "rb") as f:
            for raw in f:
                if not raw.endswith(b"\n"):
                    break
                doc = json.loads(raw.decode())
                try:
                    open_doc(doc, "audit")  # 校验通过即可，保留 digest 字段以便展示/串联
                    entry = dict(doc)
                except ValueError:
                    entry = {**doc, "_broken": True}
                out.append(entry)
        return out

    # ---------- 凭证构建 ----------

    @staticmethod
    def attest_segment(meta, records: list[dict], file_path: str) -> dict:
        digests = [r["digest"] for r in records]
        return {
            "seg": meta.seg_id,
            "first_seq": meta.first_seq,
            "last_seq": meta.last_seq,
            "count": meta.count,
            "first_digest": digests[0],
            "last_digest": digests[-1],
            # 每个原始记录的摘要都可逐条核对；root 防止清单本身被重排/替换
            "record_digests": digests,
            "records_root": digest_json(digests),
            "file_sha256": sha256_file(file_path),
        }


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)
    except OSError:
        pass


def _rmtree(path: str) -> None:
    import shutil

    shutil.rmtree(path, ignore_errors=True)
