"""通用工具：规范编码、哈希、原子写、文件锁、时间。

全部 JSON 采用 sort_keys + 紧凑分隔符的规范形式（canonical JSON），
保证同一份逻辑对象在任何机器上算出的摘要都一致。
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
from typing import Any, Iterator

GENESIS = "0" * 64  # 创世前序摘要（全零）


def canonical(obj: Any) -> bytes:
    """规范 JSON 编码：键排序、无多余空白、不转义非 ASCII。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def digest_json(obj: Any) -> str:
    """对可 JSON 化对象做规范编码后取 SHA-256。"""
    return sha256_bytes(canonical(obj))


def now_ms() -> int:
    return int(time.time() * 1000)


def atomic_write(path: str, data: bytes, fsync_dir: bool = True) -> None:
    """同目录 tmp 文件 + fsync + 原子 rename，保证读者永远只看到完整文件。"""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{os.urandom(4).hex()}"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    if fsync_dir:
        fsync_dirfd(d)


def fsync_dirfd(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # 某些文件系统不支持目录 fsync
        pass
    finally:
        os.close(fd)


def append_fsync(path: str, line: bytes) -> None:
    """追加一行并 fsync（用于段文件、审计日志）。"""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    with open(path, "ab") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())
    fsync_dirfd(d)


@contextlib.contextmanager
def file_lock(path: str) -> Iterator[None]:
    """跨进程排他文件锁（flock），同进程线程另用 threading 锁。"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_json(path: str) -> Any:
    with open(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def write_json(path: str, obj: Any) -> None:
    atomic_write(path, canonical(obj))


def remove_quiet(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def bytes_on_disk(*paths: str) -> int:
    total = 0
    for p in paths:
        if os.path.isdir(p):
            for root, _dirs, files in os.walk(p):
                for name in files:
                    fp = os.path.join(root, name)
                    try:
                        total += os.path.getsize(fp)
                    except OSError:
                        pass
        elif os.path.isfile(p):
            total += os.path.getsize(p)
    return total


class Error(Exception):
    """带 HTTP 状态码的业务错误。"""

    def __init__(self, status: int, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message, "details": self.details}


def err(status: int, code: str, message: str, **details: Any) -> Error:
    return Error(status, code, message, details)


class BusyError(Error):
    def __init__(self, message: str = "compaction already running"):
        super().__init__(409, "busy", message)


def validate_name(name: Any, field: str) -> str:
    if not isinstance(name, str) or not (1 <= len(name) <= 128):
        raise err(400, "bad_request", f"{field} must be a string of length 1..128")
    if any(c in name for c in "/\\\n\r\t\0"):
        raise err(400, "bad_request", f"{field} contains illegal characters")
    return name
