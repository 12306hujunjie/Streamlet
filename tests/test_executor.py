"""测试 executor.py —— SyncExecutor / AsyncExecutor / 并行 / 错误处理。"""

import asyncio
import inspect
import time

import pytest

from src.streamlet import BaseFlowContext
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
        if inspect.iscoroutine(result):
            return asyncio.run(result)
        return result

    async def _execute_async(self, *args, **kwargs):
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
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


# ============================================================
# ContextVar 传播测试
# ============================================================


class TestContextVarPropagation:
    """ContextVar 在线程池和 asyncio.gather 中的传播。"""

    def test_contextvar_propagated_to_thread_pool(self):
        """ContextVar 显式设置后在 SyncExecutor.gather 线程池中正确传播。"""
        import contextvars

        cv = contextvars.ContextVar("test_executor_cv", default=None)
        cv.set("parent_value")

        def read_ctx_var(x):
            return cv.get()

        node = StubNode("ctx_reader", read_ctx_var)
        ex = SyncExecutor()

        results = ex.gather([(node, 1)])
        assert results["ctx_reader"].result == "parent_value"

    @pytest.mark.asyncio
    async def test_contextvar_inherited_in_async_gather(self):
        """ContextVar 在 asyncio.gather 中自动继承（协程天然支持 ContextVar）。"""
        import contextvars

        cv = contextvars.ContextVar("test_async_cv", default=None)
        cv.set("parent_async_value")

        async def read_ctx_var(x):
            await asyncio.sleep(0)
            return cv.get()

        node = StubNode("async_ctx_reader", read_ctx_var, is_async=True)
        ex = AsyncExecutor()

        results = await ex.agather([(node, 1)])
        assert results["async_ctx_reader"].result == "parent_async_value"


# ============================================================
# ContextVarProvider 隔离测试——验证 capture/apply 机制
# ============================================================


class TestSyncExecutorContextIsolation:
    """SyncExecutor.gather 中 ContextVarProvider 的线程池 task 间隔离。

    关键点：
    - contextvars.copy() 是浅拷贝：ContextVar 的值（dict 对象）在 task 间共享引用
    - capture_context() + apply_context() 为每个 task 浅拷贝 dict，确保隔离
    """

    def test_pool_tasks_have_independent_context_dicts(self):
        """线程池中不同 task 修改 context dict 不互相污染。"""
        container = BaseFlowContext()
        container.context()["shared"] = "parent"

        def mutate_context(task_id: str) -> tuple[str, list[str]]:
            ctx = container.context()
            parent_val = ctx.get("shared", None)
            ctx[f"task_{task_id}"] = task_id
            return (parent_val, sorted(ctx.keys()))

        ex = SyncExecutor(max_workers=2)
        node_a = StubNode("a", lambda x: mutate_context("a"))
        node_b = StubNode("b", lambda x: mutate_context("b"))

        results = ex.gather([(node_a, 1), (node_b, 1)])

        parent_a, keys_a = results["a"].result
        parent_b, keys_b = results["b"].result

        # 两个 task 都应该看到 parent 预设的值
        assert parent_a == "parent"
        assert parent_b == "parent"
        # task_a 不应看到 task_b 写入的 key
        assert "task_b" not in keys_a
        # task_b 不应看到 task_a 写入的 key
        assert "task_a" not in keys_b
        # 各自看到自己的 key
        assert "task_a" in keys_a
        assert "task_b" in keys_b


class TestAsyncExecutorContextIsolation:
    """AsyncExecutor.agather 中 ContextVarProvider 的协程 task 间隔离。

    关键点：
    - asyncio.gather 创建的 Task 自动 copy_context()
    - 但 dict 值仍是共享引用，需要 capture/apply 做 dict 浅拷贝
    """

    @pytest.mark.asyncio
    async def test_async_tasks_have_independent_context_dicts(self):
        """asyncio.gather 中不同 task 修改 context dict 不互相污染。"""
        container = BaseFlowContext()
        container.context()["shared"] = "parent"

        async def mutate_context(task_id: str) -> tuple[str, list[str]]:
            await asyncio.sleep(0.01)
            ctx = container.context()
            parent_val = ctx.get("shared", None)
            ctx[f"task_{task_id}"] = task_id
            return (parent_val, sorted(ctx.keys()))

        ex = AsyncExecutor()
        node_a = StubNode("a", lambda x: mutate_context("a"), is_async=True)
        node_b = StubNode("b", lambda x: mutate_context("b"), is_async=True)

        results = await ex.agather([(node_a, 1), (node_b, 1)])

        parent_a, keys_a = results["a"].result
        parent_b, keys_b = results["b"].result

        assert parent_a == "parent"
        assert parent_b == "parent"
        assert "task_b" not in keys_a
        assert "task_a" not in keys_b
        assert "task_a" in keys_a
        assert "task_b" in keys_b
