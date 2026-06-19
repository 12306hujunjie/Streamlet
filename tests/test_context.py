"""Tests for BaseFlowContext."""

import copy
import threading

import pytest

from src.streamlet import BaseFlowContext
from src.streamlet.context import ContextVarProvider


class TestBaseFlowContext:
    def test_context_lazy_init_and_consistent(self):
        """ContextVarProvider 首次 _provide() 创建 dict，后续返回同一实例。"""
        container = BaseFlowContext()
        ctx1 = container.context()
        ctx2 = container.context()
        assert isinstance(ctx1, dict)
        assert ctx1 is ctx2
        ctx1["x"] = 42
        assert ctx2["x"] == 42

    def test_context_isolation_between_containers(self):
        c1 = BaseFlowContext()
        c2 = BaseFlowContext()
        c1.context()["x"] = 1
        c2.context()["x"] = 2
        assert c1.context()["x"] == 1
        assert c2.context()["x"] == 2

    def test_context_thread_isolation(self):
        container = BaseFlowContext()
        container.context()["main"] = "main_value"
        results = []

        def thread_func():
            results.append(container.context().get("main"))

        t = threading.Thread(target=thread_func)
        t.start()
        t.join()
        assert results[0] is None

    def test_deepcopy_creates_independent_contextvar(self):
        """dependency-injector 实例化容器时会 deepcopy providers，
        确保复制出的 provider 拥有独立的 ContextVar，不会共享状态。"""
        p1 = BaseFlowContext.context  # 类属性上的 ContextVarProvider
        p2 = copy.deepcopy(p1)

        p1()["x"] = 1
        p2()["x"] = 2

        assert p1()["x"] == 1
        assert p2()["x"] == 2
        assert p1() is not p2()

    def test_context_provider_rejects_unknown_copy_policy(self):
        with pytest.raises(ValueError, match="copy_policy"):
            ContextVarProvider(dict, copy_policy="deep")
