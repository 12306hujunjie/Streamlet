"""显式执行图——内部实现细节，不对外暴露。

5 个内部类对应 5 种组合模式，用户通过 Node fluent 接口间接使用。
"""

import asyncio
import logging
from typing import Any

from .exceptions import LoopControlException
from .executor import AsyncExecutor, FanOutArgs, ParallelTask, SyncExecutor
from .types import CallArgs, RepeatInputMode, call_args

logger = logging.getLogger("streamlet")


def _parallel_tasks(source_result: Any, targets: list[Any]) -> list[ParallelTask]:
    if not isinstance(source_result, FanOutArgs):
        return [(target, source_result) for target in targets]

    expected = len(targets)
    actual = len(source_result.items)
    if actual != expected:
        raise ValueError(f"expected {expected} fan-out inputs, got {actual}")

    return [
        (target, (), kwargs)
        for target, kwargs in zip(targets, source_result.items, strict=True)
    ]


class Pipeline:
    """顺序组合：left → right"""

    def __init__(self, left: Any, right: Any) -> None:
        self.left = left
        self.right = right
        self._is_async = left._is_async or right._is_async

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return self._async_run(*args, **kwargs)
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
    - "auto":   全 sync 走 thread，混合 sync/async 走 hybrid
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
            if self._is_async:
                return self._async_thread_run(*args, **kwargs)
            return self._sync_run(*args, **kwargs)
        elif self.executor_type == "async":
            return self._async_run(*args, **kwargs)
        else:  # "auto"
            if self._is_async:
                return self._async_hybrid_run(*args, **kwargs)
            else:
                return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor(max_workers=self.max_workers)
        source_result = ex.run(self.source, *args, **kwargs)
        return ex.gather(_parallel_tasks(source_result, self.targets))

    async def _async_thread_run(self, *args: Any, **kwargs: Any) -> Any:
        source_ex = AsyncExecutor()
        source_result = await source_ex.arun(self.source, *args, **kwargs)
        target_ex = SyncExecutor(max_workers=self.max_workers)
        return await asyncio.to_thread(
            target_ex.gather, _parallel_tasks(source_result, self.targets)
        )

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor(max_workers=self.max_workers)
        source_result = await ex.arun(self.source, *args, **kwargs)
        return await ex.agather(_parallel_tasks(source_result, self.targets))

    async def _async_hybrid_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor(max_workers=self.max_workers)
        source_result = await ex.arun(self.source, *args, **kwargs)
        return await ex.ahybrid_gather(_parallel_tasks(source_result, self.targets))


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
            return self._async_run(*args, **kwargs)
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


def _validate_repeat_input_mode(input_mode: Any) -> RepeatInputMode:
    if not isinstance(input_mode, RepeatInputMode):
        raise TypeError(
            f"input_mode must be RepeatInputMode, got {type(input_mode).__name__}"
        )
    return input_mode


def _repeat_next_call(last_result: Any) -> CallArgs:
    if isinstance(last_result, CallArgs):
        return last_result
    return call_args(last_result)


class Repeat:
    """循环组合：重复执行 node N 次"""

    def __init__(
        self,
        node: Any,
        times: int,
        stop_on_error: bool = False,
        input_mode: RepeatInputMode = RepeatInputMode.PREVIOUS_RESULT,
    ) -> None:
        if isinstance(times, bool) or not isinstance(times, int):
            raise TypeError(f"times must be an integer, got {type(times).__name__}")
        if times <= 0:
            raise ValueError("Repeat times must be greater than 0")
        self.node = node
        self.times = times
        self.stop_on_error = stop_on_error
        self.input_mode = _validate_repeat_input_mode(input_mode)
        self._is_async = node._is_async

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return self._async_run(*args, **kwargs)
        else:
            return self._sync_run(*args, **kwargs)

    def _sync_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = SyncExecutor()
        last_result = None
        next_call = call_args(*args, **kwargs)
        for i in range(self.times):
            try:
                if i == 0 or self.input_mode is RepeatInputMode.SAME_INPUT:
                    candidate_result = ex.run(self.node, *args, **kwargs)
                else:
                    candidate_result = ex.run(
                        self.node, *next_call.args, **next_call.kwargs
                    )
                if self.input_mode is RepeatInputMode.PREVIOUS_RESULT:
                    next_call = _repeat_next_call(candidate_result)
                last_result = candidate_result
            except Exception as e:
                if self.stop_on_error:
                    raise LoopControlException(
                        message=f"repeat(stop_on_error=True) 在第 {i + 1} 次迭代失败: {e}",
                        node_name=getattr(self.node, "name", None),
                        iteration=i + 1,
                        times=self.times,
                    ) from e
                logger.warning(
                    "repeat 第 %d/%d 次迭代失败（stop_on_error=False，继续循环）: %s",
                    i + 1,
                    self.times,
                    e,
                )
        return last_result

    async def _async_run(self, *args: Any, **kwargs: Any) -> Any:
        ex = AsyncExecutor()
        last_result = None
        next_call = call_args(*args, **kwargs)
        for i in range(self.times):
            try:
                if i == 0 or self.input_mode is RepeatInputMode.SAME_INPUT:
                    candidate_result = await ex.arun(self.node, *args, **kwargs)
                else:
                    candidate_result = await ex.arun(
                        self.node, *next_call.args, **next_call.kwargs
                    )
                if self.input_mode is RepeatInputMode.PREVIOUS_RESULT:
                    next_call = _repeat_next_call(candidate_result)
                last_result = candidate_result
            except Exception as e:
                if self.stop_on_error:
                    raise LoopControlException(
                        message=f"repeat(stop_on_error=True) 在第 {i + 1} 次迭代失败: {e}",
                        node_name=getattr(self.node, "name", None),
                        iteration=i + 1,
                        times=self.times,
                    ) from e
                logger.warning(
                    "repeat 第 %d/%d 次迭代失败（stop_on_error=False，继续循环）: %s",
                    i + 1,
                    self.times,
                    e,
                )
        return last_result


class FanIn:
    """聚合：接收上游 Node 的结果 → aggregator"""

    def __init__(self, upstream: Any, aggregator: Any) -> None:
        self.upstream = upstream
        self.aggregator = aggregator
        self._is_async = upstream._is_async or aggregator._is_async

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            return self._async_run(*args, **kwargs)
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
