#!/usr/bin/env python3
"""标准库测试运行器：把 tests/pytest_shim.py 注册为 `pytest`，
然后收集 tests/test_*.py 中的 Test* 类用例（支持 fixtures 与参数化）。

若环境中安装了真正的 pytest，则直接转交 pytest 运行。
"""
from __future__ import annotations

import importlib
import inspect
import os
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    import pytest  # noqa: F401
    sys.exit(importlib.import_module("pytest").main([str(ROOT / "tests"), "-q"]))
except ImportError:
    pass

# 注册垫片为 pytest
sys.path.insert(0, str(Path(__file__).resolve().parent))
import pytest_shim  # noqa: E402
sys.modules["pytest"] = pytest_shim

PASS = 0
FAIL = 0
FAILURES = []


def _resolve_fixture(name, cache, tmp_path):
    if name in cache:
        return cache[name]
    if name == "tmp_path":
        cache[name] = tmp_path
    elif name == "monkeypatch":
        mp = pytest_shim.MonkeyPatch()
        cache[name] = mp
    else:
        raise RuntimeError(f"unknown fixture {name}")
    return cache[name]


def _run_one(fn, self_obj, tmp_path, case_kwargs=None):
    sig = inspect.signature(fn)
    cache = {}
    kwargs = {}
    params = list(sig.parameters.items())
    if self_obj is not None and params and params[0][0] == "self":
        params = params[1:]
    for pname, _p in params:
        if case_kwargs and pname in case_kwargs:
            kwargs[pname] = case_kwargs[pname]
        else:
            kwargs[pname] = _resolve_fixture(pname, cache, tmp_path)
    if self_obj is not None:
        fn(self_obj, **kwargs)
    else:
        fn(**kwargs)
    for v in cache.values():
        if isinstance(v, pytest_shim.MonkeyPatch):
            v.undo()


def run_module(modname: str):
    global PASS, FAIL
    mod = importlib.import_module(modname)
    for cls_name, cls in inspect.getmembers(mod, inspect.isclass):
        if not cls_name.startswith("Test") or cls.__module__ != mod.__name__:
            continue
        for name, fn in inspect.getmembers(cls, inspect.isfunction):
            if not name.startswith("test_"):
                continue
            cases = getattr(fn, "__param_cases__", None) or [None]
            for i, case in enumerate(cases):
                label = f"{cls_name}.{name}" + (f"[{i}]" if len(cases) > 1 else "")
                tmp = pytest_shim.new_tmp_path()
                try:
                    obj = cls()
                    setup = getattr(obj, "setup_method", None)
                    teardown = getattr(obj, "teardown_method", None)
                    if setup:
                        setup(fn)
                    _run_one(fn, obj, tmp, case)
                    if teardown:
                        teardown(fn)
                    PASS += 1
                    print(".", end="", flush=True)
                except Exception:
                    FAIL += 1
                    FAILURES.append((label, traceback.format_exc()))
                    print("F", end="", flush=True)
                finally:
                    pytest_shim.cleanup_tmp(tmp)
    # 模块级函数（本仓库没有，保留通用）
    for name, fn in inspect.getmembers(mod, inspect.isfunction):
        if name.startswith("test_") and fn.__module__ == mod.__name__:
            for i, case in enumerate(getattr(fn, "__param_cases__", None) or [None]):
                label = f"{name}" + (f"[{i}]" if case else "")
                tmp = pytest_shim.new_tmp_path()
                try:
                    _run_one(fn, None, tmp, case)
                    PASS += 1
                    print(".", end="", flush=True)
                except Exception:
                    FAIL += 1
                    FAILURES.append((label, traceback.format_exc()))
                    print("F", end="", flush=True)
                finally:
                    pytest_shim.cleanup_tmp(tmp)


if __name__ == "__main__":
    run_module("tests.test_kernel")
    run_module("tests.test_server")
    print()
    for label, tb in FAILURES:
        print("\n" + "=" * 70)
        print("FAIL:", label)
        print(tb)
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)
