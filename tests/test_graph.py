"""测试 graph.py —— Pipeline / Parallel / Conditional / Repeat / FanIn。"""

import asyncio
import inspect

import pytest

from src.streamlet.graph import (
    Conditional,
    FanIn,
    Parallel,
    Pipeline,
    Repeat,
)

# ============================================================
# Stub Node —— 实现 _execute / _execute_async / _is_async
# ============================================================


class StubNode:
    def __init__(self, name: str, func, is_async: bool = False):
        self.name = name
        self._func = func
        self._is_async = is_async

    def _execute(self, *args, **kwargs):
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
            return asyncio.run(result)
        return result

    async def _execute_async(self, *args, **kwargs):
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
            return await result
        return result


# ============================================================
# Pipeline 测试
# ============================================================


class TestPipeline:
    def test_two_sync_nodes(self):
        left = StubNode("double", lambda x: x * 2)
        right = StubNode("add_ten", lambda x: x + 10)
        p = Pipeline(left, right)

        assert p._is_async is False
        assert p(5) == 20

    def test_async_node_propagates(self):
        left = StubNode("double", lambda x: x * 2)
        right = StubNode("async_add", lambda x: x + 10, is_async=True)
        p = Pipeline(left, right)

        assert p._is_async is True
        assert p(5) == 20

    def test_error_propagation(self):
        def fail(_):
            raise ValueError("left failed")

        left = StubNode("failer", fail)
        right = StubNode("add_ten", lambda x: x + 10)
        p = Pipeline(left, right)

        with pytest.raises(ValueError, match="left failed"):
            p(1)

    def test_inspectable_attrs(self):
        left = StubNode("a", lambda x: x)
        right = StubNode("b", lambda x: x)
        p = Pipeline(left, right)

        assert p.left is left
        assert p.right is right

    @pytest.mark.asyncio
    async def test_async_run(self):
        async def afunc(x):
            await asyncio.sleep(0)
            return x * 3

        left = StubNode("triple", afunc, is_async=True)
        right = StubNode("add_one", lambda x: x + 1)
        p = Pipeline(left, right)

        result = await p(4)
        assert result == 13


# ============================================================
# Parallel 测试
# ============================================================


class TestParallel:
    def test_source_and_targets(self):
        source = StubNode("src", lambda x: x)
        t1 = StubNode("t1", lambda x: x * 2)
        t2 = StubNode("t2", lambda x: x + 10)
        p = Parallel(source, [t1, t2])

        results = p(5)
        assert results["t1"].success
        assert results["t1"].result == 10
        assert results["t2"].success
        assert results["t2"].result == 15

    def test_executor_type_stored(self):
        source = StubNode("src", lambda x: x)
        t = StubNode("t", lambda x: x)
        p = Parallel(source, [t], executor_type="async", max_workers=4)

        assert p.executor_type == "async"
        assert p.max_workers == 4

    def test_auto_all_sync_uses_thread(self):
        source = StubNode("src", lambda x: x)
        t1 = StubNode("t1", lambda x: x * 2)
        t2 = StubNode("t2", lambda x: x + 10)
        p = Parallel(source, [t1, t2], executor_type="auto")

        results = p(5)
        assert len(results) == 2

    def test_async_node_propagates(self):
        source = StubNode("src", lambda x: x)

        async def a_target(x):
            return x * 2

        t = StubNode("t", a_target, is_async=True)
        p = Parallel(source, [t])

        assert p._is_async is True

    def test_error_in_target(self):
        def fail(_):
            raise ValueError("target failed")

        source = StubNode("src", lambda x: x)
        t = StubNode("failer", fail)
        p = Parallel(source, [t])

        results = p(1)
        assert not results["failer"].success
        assert "target failed" in results["failer"].error

    def test_auto_mixed_sync_async_targets(self):
        """auto 模式 + 混合 sync/async 目标 → 走 async 路径。"""
        source = StubNode("src", lambda x: x)

        async def a_target(x):
            return x * 2

        t1 = StubNode("sync_t", lambda x: x + 10)
        t2 = StubNode("async_t", a_target, is_async=True)
        p = Parallel(source, [t1, t2], executor_type="auto")

        assert p._is_async is True
        results = p(5)
        assert results["sync_t"].result == 15
        assert results["async_t"].result == 10


# ============================================================
# Conditional 测试
# ============================================================


class TestConditional:
    def test_branch_selection(self):
        cond = StubNode("is_even", lambda x: x % 2 == 0)
        even = StubNode("even_branch", lambda: "even")
        odd = StubNode("odd_branch", lambda: "odd")
        c = Conditional(cond, {True: even, False: odd})

        assert c(2) == "even"
        assert c(3) == "odd"

    def test_no_branch_raises(self):
        cond = StubNode("always_3", lambda x: 3)
        c = Conditional(cond, {1: StubNode("one", lambda x: x)})

        with pytest.raises(ValueError, match="No branch defined"):
            c(0)

    def test_async_propagates(self):
        async def acond(x):
            return True

        cond = StubNode("cond", acond, is_async=True)
        branch = StubNode("b", lambda: "hit")
        c = Conditional(cond, {True: branch})

        assert c._is_async is True
        assert c(1) == "hit"

    def test_none_as_branch_key(self):
        """None 作为合法分支键值。"""
        cond = StubNode("maybe_none", lambda x: None if x == 0 else x)
        c = Conditional(
            cond,
            {
                None: StubNode("nil", lambda: "got_none"),
                1: StubNode("one", lambda: "got_1"),
            },
        )

        assert c(0) == "got_none"
        assert c(1) == "got_1"


# ============================================================
# Repeat 测试
# ============================================================


class TestRepeat:
    def test_iterations(self):
        node = StubNode("inc", lambda x: x + 1)
        r = Repeat(node, times=3)

        assert r(0) == 3

    def test_data_accumulation(self):
        node = StubNode("accum", lambda d: {"count": d.get("count", 0) + 1})
        r = Repeat(node, times=5)

        result = r({})
        assert result == {"count": 5}

    def test_times_validation(self):
        node = StubNode("x", lambda x: x)
        with pytest.raises(TypeError, match="times must be an integer"):
            Repeat(node, "bad")
        with pytest.raises(ValueError, match="greater than 0"):
            Repeat(node, 0)

    def test_stop_on_error(self):
        def fail(_):
            raise ValueError("fail")

        node = StubNode("failer", fail)
        r = Repeat(node, times=3, stop_on_error=True)

        with pytest.raises(ValueError, match="fail"):
            r(0)

    def test_continue_on_error(self):
        counter = [0]

        def succeed_after_two_failures(_):
            counter[0] += 1
            if counter[0] <= 2:
                raise ValueError("transient")
            return counter[0]

        node = StubNode("flaky", succeed_after_two_failures)
        r = Repeat(node, times=5, stop_on_error=False)

        result = r(None)
        assert result == 5  # 第 3/4/5 次成功，最后一次 counter=5


# ============================================================
# FanIn 测试
# ============================================================


class TestFanIn:
    def test_aggregation(self):
        upstream = StubNode("up", lambda x: {"count": x + 1})
        agg = StubNode("agg", lambda d: f"total:{d['count']}")
        fi = FanIn(upstream, agg)

        assert fi(5) == "total:6"

    def test_async_propagates(self):
        async def a_up(x):
            return x

        upstream = StubNode("up", a_up, is_async=True)
        agg = StubNode("agg", lambda x: x)
        fi = FanIn(upstream, agg)

        assert fi._is_async is True

    def test_error_from_upstream(self):
        def fail(_):
            raise ValueError("upstream failed")

        upstream = StubNode("failer", fail)
        agg = StubNode("agg", lambda x: x)
        fi = FanIn(upstream, agg)

        with pytest.raises(ValueError, match="upstream failed"):
            fi(0)

    def test_aggregator_error(self):
        """聚合器节点自身失败时异常正确传播。"""
        upstream = StubNode("up", lambda x: {"data": x})

        def fail_agg(_):
            raise ValueError("aggregator boom")

        agg = StubNode("fail_agg", fail_agg)
        fi = FanIn(upstream, agg)

        with pytest.raises(ValueError, match="aggregator boom"):
            fi(42)

    def test_async_path_with_sync_upstream(self):
        """上游 sync + 聚合器 async → _is_async=True 走 _async_run。"""
        upstream = StubNode("up", lambda x: {"v": x})

        async def async_agg(d):
            return f"async:{d['v']}"

        agg = StubNode("async_agg", async_agg, is_async=True)
        fi = FanIn(upstream, agg)

        assert fi._is_async is True
        result = fi(7)
        assert result == "async:7"
