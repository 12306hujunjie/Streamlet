"""显式执行图——内部实现细节，不对外暴露。

5 个内部类对应 5 种组合模式，用户通过 Node fluent 接口间接使用。
"""

import asyncio
from typing import Any

from .exceptions import LoopControlException
from .executor import AsyncExecutor, SyncExecutor


def _maybe_await(coro: Any) -> Any:
    """协程桥接：event loop 内返回协程供上层 await，否则 asyncio.run。"""
    try:
        asyncio.get_running_loop()
        return coro
    except RuntimeError:
        return asyncio.run(coro)


class Pipeline:
    """顺序组合：left → right"""

    def __init__(self, left: Any, right: Any) -> None:
        self.left = left
        self.right = right
        self._is_async = left._is_async or right._is_async

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return _maybe_await(self._async_run(*args, **kwargs))
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor()
        mid = ex.run(self.left, *args, **kwargs)
        return ex.run(self.right, mid)

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor()
        mid = await ex.arun(self.left, *args, **kwargs)
        return await ex.arun(self.right, mid)


class Parallel:
    """并行扇出：source → [target1, target2, ...]

    executor_type 控制并行调度策略：
    - "thread": ThreadPoolExecutor 调度
    - "async":  asyncio.gather 调度
    - "auto":   全 sync 走 thread，含 async 走 async
    """

    def __init__(
        self,
        source: Any,
        targets: list[Any],
        executor_type: str = "thread",
        max_workers: int | None = None,
    ) -> None:
        self.source = source
        self.targets = targets
        self.executor_type = executor_type
        self.max_workers = max_workers
        self._is_async = source._is_async or any(t._is_async for t in targets)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self.executor_type == "thread":
            return self._sync_run(*args, **kwargs)
        elif self.executor_type == "async":
            return _maybe_await(self._async_run(*args, **kwargs))
        else:  # "auto"
            if self._is_async:
                return _maybe_await(self._async_run(*args, **kwargs))
            else:
                return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor(max_workers=self.max_workers)
        source_result = ex.run(self.source, *args, **kwargs)
        return ex.gather([(t, source_result) for t in self.targets])

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor()
        source_result = await ex.arun(self.source, *args, **kwargs)
        return await ex.agather([(t, source_result) for t in self.targets])


class Conditional:
    """条件分支：condition_node 返回值作为分支选择键"""

    def __init__(self, condition_node: Any, branches: dict[Any, Any]) -> None:
        self.condition_node = condition_node
        self.branches = branches
        self._is_async = condition_node._is_async or any(
            b._is_async for b in branches.values()
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return _maybe_await(self._async_run(*args, **kwargs))
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor()
        condition_result = ex.run(self.condition_node, *args, **kwargs)
        if condition_result not in self.branches:
            raise ValueError(
                f"No branch defined for condition result: {condition_result}"
            )
        return ex.run(self.branches[condition_result])

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor()
        condition_result = await ex.arun(self.condition_node, *args, **kwargs)
        if condition_result not in self.branches:
            raise ValueError(
                f"No branch defined for condition result: {condition_result}"
            )
        return await ex.arun(self.branches[condition_result])


class Repeat:
    """循环组合：重复执行 node N 次"""

    def __init__(self, node: Any, times: int, stop_on_error: bool = False) -> None:
        if not isinstance(times, int):
            raise TypeError(f"times must be an integer, got {type(times).__name__}")
        if times <= 0:
            raise ValueError("Repeat times must be greater than 0")
        self.node = node
        self.times = times
        self.stop_on_error = stop_on_error
        self._is_async = node._is_async

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return _maybe_await(self._async_run(*args, **kwargs))
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor()
        last_result = None
        for i in range(self.times):
            try:
                last_result = ex.run(self.node, *args if i == 0 else (last_result,))
            except Exception as e:
                if self.stop_on_error:
                    raise LoopControlException(
                        message=f"repeat(stop_on_error=True) 在第 {i + 1} 次迭代失败: {e}",
                        node_name=getattr(self.node, "name", None),
                        iteration=i + 1,
                        times=self.times,
                    ) from e
        return last_result

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor()
        last_result = None
        for i in range(self.times):
            try:
                last_result = await ex.arun(
                    self.node, *args if i == 0 else (last_result,)
                )
            except Exception as e:
                if self.stop_on_error:
                    raise LoopControlException(
                        message=f"repeat(stop_on_error=True) 在第 {i + 1} 次迭代失败: {e}",
                        node_name=getattr(self.node, "name", None),
                        iteration=i + 1,
                        times=self.times,
                    ) from e
        return last_result


class FanIn:
    """聚合：接收上游 Node 的结果 → aggregator"""

    def __init__(self, upstream: Any, aggregator: Any) -> None:
        self.upstream = upstream
        self.aggregator = aggregator
        self._is_async = upstream._is_async or aggregator._is_async

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return _maybe_await(self._async_run(*args, **kwargs))
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor()
        upstream_result = ex.run(self.upstream, *args, **kwargs)
        return ex.run(self.aggregator, upstream_result)

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor()
        upstream_result = await ex.arun(self.upstream, *args, **kwargs)
        return await ex.arun(self.aggregator, upstream_result)
