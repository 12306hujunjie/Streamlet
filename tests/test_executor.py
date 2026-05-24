"""测试 executor.py —— SyncExecutor / AsyncExecutor / 并行 / 错误处理。"""

import asyncio
import time

import pytest

from src.streamlet.executor import AsyncExecutor, SyncExecutor

# ============================================================
# Stub Node —— 实现 _execute / _execute_async 接口
# ============================================================


class StubNode:
    """最小化 Node stub，供 executor 测试使用。"""

    def __init__(self, name: str, func, is_async: bool = False):
        self.name = name
        self._func = func
        self._is_async = is_async

    def _execute(self, *args, **kwargs):
        result = self._func(*args, **kwargs)
        if self._is_async:
            return asyncio.run(result)
        return result

    async def _execute_async(self, *args, **kwargs):
        result = self._func(*args, **kwargs)
        if self._is_async:
            return await result
        return result


# ============================================================
# SyncExecutor 测试
# ============================================================


class TestSyncExecutorRun:
    """SyncExecutor.run() 通过 node._execute() 调用节点。"""

    def test_run_sync_node(self):
        node = StubNode("double", lambda x: x * 2)
        ex = SyncExecutor()
        assert ex.run(node, 5) == 10

    def test_run_async_node_bridged(self):
        async def afunc(x):
            return x * 3

        node = StubNode("triple", afunc, is_async=True)
        ex = SyncExecutor()
        assert ex.run(node, 4) == 12

    def test_run_passes_args_and_kwargs(self):
        node = StubNode("concat", lambda a, b, suffix="": f"{a}{b}{suffix}")
        ex = SyncExecutor()
        assert ex.run(node, "x", "y", suffix="!") == "xy!"


class TestSyncExecutorGather:
    """SyncExecutor.gather() —— ThreadPoolExecutor 并行。"""

    def test_gather_multiple_different_nodes(self):
        """3 个不同节点并发 → 3 个结果。"""
        slow = StubNode("slow", lambda x: (time.sleep(0.05), x * 2)[1])
        fast = StubNode("fast", lambda x: x + 10)
        triple = StubNode("triple", lambda x: x * 3)
        ex = SyncExecutor(max_workers=4)

        start = time.time()
        results = ex.gather([(slow, 1), (fast, 2), (triple, 3)])
        elapsed = time.time() - start

        assert len(results) == 3
        assert results["slow"].result == 2
        assert results["fast"].result == 12
        assert results["triple"].result == 9
        assert elapsed < 0.15  # 并发而非串行

    def test_gather_same_node_auto_dedup(self):
        """同名节点自动追加后缀 [1], [2], ..."""
        node = StubNode("worker", lambda x: x * 2)
        ex = SyncExecutor()

        results = ex.gather([(node, 1), (node, 10), (node, 100)])

        assert len(results) == 3
        assert results["worker"].result == 2
        assert results["worker[1]"].result == 20
        assert results["worker[2]"].result == 200

    def test_gather_custom_key_func(self):
        """自定义 key_func 控制结果键名。"""
        a = StubNode("a", lambda x: x + 1)
        b = StubNode("b", lambda x: x * 10)
        ex = SyncExecutor()

        results = ex.gather(
            [(a, 1), (b, 2)],
            key_func=lambda n: f"result_{n.name}",
        )

        assert results["result_a"].result == 2
        assert results["result_b"].result == 20

    def test_gather_error_wrapping(self):
        def fail(_):
            raise ValueError("boom")

        node = StubNode("failer", fail)
        ex = SyncExecutor()

        results = ex.gather([(node, 1)])

        assert not results["failer"].success
        assert "boom" in results["failer"].error
        assert results["failer"].error_traceback is not None


# ============================================================
# AsyncExecutor 测试
# ============================================================


class TestAsyncExecutorARun:
    """AsyncExecutor.arun() —— await node._execute_async()。"""

    @pytest.mark.asyncio
    async def test_arun_sync_node(self):
        node = StubNode("double", lambda x: x * 2)
        ex = AsyncExecutor()
        result = await ex.arun(node, 5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_arun_async_node(self):
        async def afunc(x):
            await asyncio.sleep(0)
            return x * 3

        node = StubNode("triple", afunc, is_async=True)
        ex = AsyncExecutor()
        result = await ex.arun(node, 4)
        assert result == 12


class TestAsyncExecutorAGather:
    """AsyncExecutor.agather() —— asyncio.gather 并发。"""

    @pytest.mark.asyncio
    async def test_agather_multiple_different_nodes(self):
        """3 个不同异步节点并发 → 耗时 ~50ms，串行需 ~150ms。"""

        async def slow(x):
            await asyncio.sleep(0.05)
            return x * 2

        async def fast(x):
            await asyncio.sleep(0.05)
            return x + 10

        async def triple(x):
            await asyncio.sleep(0.05)
            return x * 3

        a = StubNode("slow", slow, is_async=True)
        b = StubNode("fast", fast, is_async=True)
        c = StubNode("triple", triple, is_async=True)
        ex = AsyncExecutor()

        start = time.time()
        results = await ex.agather([(a, 1), (b, 2), (c, 3)])
        elapsed = time.time() - start

        assert len(results) == 3
        assert results["slow"].result == 2
        assert results["fast"].result == 12
        assert results["triple"].result == 9
        assert elapsed < 0.10  # 并发 ~50ms，串行 ~150ms

    @pytest.mark.asyncio
    async def test_agather_same_node_auto_dedup(self):
        """同名节点自动追加后缀。"""
        node = StubNode("worker", lambda x: x * 2)
        ex = AsyncExecutor()

        results = await ex.agather([(node, 1), (node, 10)])

        assert len(results) == 2
        assert results["worker"].result == 2
        assert results["worker[1]"].result == 20

    @pytest.mark.asyncio
    async def test_agather_custom_key_func(self):
        """自定义 key_func。"""
        a = StubNode("a", lambda x: x + 1)
        b = StubNode("b", lambda x: x * 10)
        ex = AsyncExecutor()

        results = await ex.agather(
            [(a, 1), (b, 2)],
            key_func=lambda n: f"r_{n.name}",
        )

        assert results["r_a"].result == 2
        assert results["r_b"].result == 20

    @pytest.mark.asyncio
    async def test_agather_error_wrapping(self):
        def fail(_):
            raise ValueError("async boom")

        node = StubNode("failer", fail)
        ex = AsyncExecutor()

        results = await ex.agather([(node, 1)])

        assert not results["failer"].success
        assert "async boom" in results["failer"].error
        assert results["failer"].error_traceback is not None
