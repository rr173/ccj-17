"""无 pytest 环境下的极简兼容垫片（仅覆盖本仓库测试用到的能力）。

优先使用真实 pytest；导入失败时本模块提供：
  - pytest.fixture / pytest.mark.parametrize（参数化用例收集）
  - pytest.raises
  - tmp_path fixture（每用例独立临时目录）
  - monkeypatch fixture（setattr/delenv/undo）
由 tests/run_tests.py 驱动。
"""
from __future__ import annotations

import contextlib
import os
import shutil
import sys
import tempfile
import types


class _Mark:
    def __init__(self, name, args):
        self.name = name
        self.args = args


class _Marks:
    @staticmethod
    def parametrize(argnames, argvalues):
        if isinstance(argnames, str):
            names = [a.strip() for a in argnames.split(",")]
        else:
            names = list(argnames)

        def deco(fn):
            cases = []
            for val in argvalues:
                if not isinstance(val, tuple):
                    val = (val,)
                cases.append(dict(zip(names, val)))
            fn.__param_cases__ = cases
            return fn
        return deco


def fixture(fn=None, **_kw):
    if fn is None:
        return lambda f: fixture(f)
    fn.__is_fixture__ = True
    return fn


class _Raises:
    def __init__(self, exc):
        self.exc = exc
        self.value = None

    def __enter__(self):
        return self

    def __exit__(self, etype, evalue, tb):
        if etype is None:
            raise AssertionError(f"DID NOT RAISE {self.exc}")
        if not issubclass(etype, self.exc):
            return False
        self.value = evalue
        return True


def raises(exc):
    return _Raises(exc)


mark = _Marks()


class MonkeyPatch:
    def __init__(self):
        self._undo = []

    def setattr(self, obj, name, value):
        old = getattr(obj, name)

        def restore():
            setattr(obj, name, old)

        self._undo.append(restore)
        setattr(obj, name, value)

    def delattr(self, obj, name, raising=True):
        if hasattr(obj, name):
            old = getattr(obj, name)
            self._undo.append(lambda: setattr(obj, name, old))
            delattr(obj, name)
        elif raising:
            raise AttributeError(name)

    def setenv(self, name, value):
        old = os.environ.get(name, None)
        self._undo.append(lambda: (os.environ.__setitem__(name, old) if old is not None
                                   else os.environ.pop(name, None)))
        os.environ[name] = value

    def delenv(self, name, raising=True):
        if name in os.environ:
            old = os.environ[name]
            self._undo.append(lambda: os.environ.__setitem__(name, old))
            del os.environ[name]
        elif raising:
            raise KeyError(name)

    def undo(self):
        for fn in reversed(self._undo):
            fn()
        self._undo.clear()


def make_tmp_path():
    return types.SimpleNamespace(  # 让 str(tmp_path) 可用
        **{"__class__": type("P", (str,), {})})


class TmpPath(str):
    """str 子类，直接就是路径字符串。"""


def new_tmp_path():
    d = tempfile.mkdtemp(prefix="applog-test-")
    return TmpPath(d)


def cleanup_tmp(p):
    shutil.rmtree(str(p), ignore_errors=True)
