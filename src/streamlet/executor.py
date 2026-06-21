"""Executor 协议与实现——纯执行策略。

SyncExecutor: 同步执行，gather 使用 ThreadPoolExecutor
AsyncExecutor: 异步执行，gather 使用 asyncio.gather
"""

import asyncio
import contextvars
import time
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .context import apply_context, capture_context


@dataclass
class ParallelResult:
    """并行执行结果——记录成功/失败状态、错误信息、执行时间。"""

    node_name: str
    success: bool
    result: Any = None
    error: str | None = None
    error_traceback: str | None = None
    execution_time: float | None = None


@dataclass(frozen=True)
class FanOutArgs:
    """显式 fan-out 参数协议：每个 dict 对应一个 target 的 kwargs。"""

    items: tuple[dict[str, Any], ...]

    def __post_init__(self) -> None:
        if not all(isinstance(item, dict) for item in self.items):
            raise TypeError("fan_out_args items must be dict")
        object.__setattr__(self, "items", tuple(dict(item) for item in self.items))


def fan_out_args(*items: dict[str, Any]) -> FanOutArgs:
    """Create explicit per-target kwargs for fan-out execution."""
    return FanOutArgs(items)


ParallelTask = tuple[Any, Any] | tuple[Any, tuple[Any, ...], dict[str, Any]]


def _split_task(task: ParallelTask) -> tuple[Any, tuple[Any, ...], dict[str, Any]]:
    if len(task) == 2:
        node, inp = task
        return node, (inp,), {}
    if len(task) == 3:
        node, args, kwargs = task
        return node, tuple(args), dict(kwargs)
    raise ValueError(f"Parallel task must contain 2 or 3 items, got {len(task)}")


def _unique_key(base_name: str, existing: dict) -> str:
    """生成唯一结果键，同名自动追加后缀 [1], [2], ..."""
    if base_name not in existing:
        return base_name
    counter = 1
    while f"{base_name}[{counter}]" in existing:
        counter += 1
    return f"{base_name}[{counter}]"


@runtime_checkable
class Executor(Protocol):
    """同步执行器协议。"""

    def run(self, node: Any, *args: Any, **kwargs: Any) -> Any: ...
    def gather(
        self,
        tasks: list[ParallelTask],
        key_func: Callable[[Any], str] | None = None,
    ) -> dict[str, "ParallelResult"]: ...


class SyncExecutor:
    """同步执行器：gather 使用 ThreadPoolExecutor 并行。

    线程池内调用 node._execute()，内部有 asyncio.run() 桥接 async 节点。
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers

    def run(self, node: Any, *args: Any, **kwargs: Any) -> Any:
        return node._execute(*args, **kwargs)

    def _run_with_time(
        self, node: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Any:
        start = time.perf_counter()
        try:
            result = node._execute(*args, **kwargs)
            return ParallelResult(
                node_name=node.name,
                success=True,
                result=result,
                execution_time=time.perf_counter() - start,
            )
        except Exception as e:
            return ParallelResult(
                node_name=node.name,
                success=False,
                error=str(e),
                error_traceback=traceback.format_exc(),
                execution_time=time.perf_counter() - start,
            )

    def _run_isolated(
        self,
        node: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        snapshot: dict[int, Any],
    ) -> Any:
        """在隔离的 context 中执行——dict 浅拷贝避免线程池 task 间污染。"""
        apply_context(snapshot)
        return self._run_with_time(node, args, kwargs)

    def gather(
        self,
        node_inputs: list[ParallelTask],
        key_func: Callable[[Any], str] | None = None,
    ) -> dict[str, "ParallelResult"]:
        parent_ctx = contextvars.copy_context()
        parent_snapshot = capture_context()
        results: dict[str, ParallelResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = [
                (
                    ex.submit(
                        parent_ctx.copy().run,
                        self._run_isolated,
                        n,
                        args,
                        kwargs,
                        parent_snapshot,
                    ),
                    n,
                )
                for n, args, kwargs in (_split_task(task) for task in node_inputs)
            ]
            for future, node in futures:
                base = key_func(node) if key_func else node.name
                key = _unique_key(base, results)
                pr = future.result()
                results[key] = pr
        return results


class AsyncExecutor:
    """异步执行器：gather 使用 asyncio.gather 实现真正的异步并发。"""

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers

    async def arun(self, node: Any, *args: Any, **kwargs: Any) -> Any:
        return await node._execute_async(*args, **kwargs)

    async def agather(
        self,
        node_inputs: list[ParallelTask],
        key_func: Callable[[Any], str] | None = None,
    ) -> dict[str, "ParallelResult"]:
        parent_ctx_snapshot = capture_context()
        if self.max_workers is not None and self.max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        semaphore = (
            asyncio.Semaphore(self.max_workers)
            if self.max_workers is not None
            else None
        )

        async def _execute_one(
            node: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
        ) -> tuple[str, ParallelResult]:
            # fan-out 分支隔离：为每个 asyncio task 注入独立的 context 快照
            apply_context(parent_ctx_snapshot)
            base = key_func(node) if key_func else node.name
            start = time.perf_counter()
            try:
                result = await node._execute_async(*args, **kwargs)
                return base, ParallelResult(
                    node_name=node.name,
                    success=True,
                    result=result,
                    execution_time=time.perf_counter() - start,
                )
            except Exception as e:
                return base, ParallelResult(
                    node_name=node.name,
                    success=False,
                    error=str(e),
                    error_traceback=traceback.format_exc(),
                    execution_time=time.perf_counter() - start,
                )

        async def _execute_limited(
            node: Any, args: tuple[Any, ...], kwargs: dict[str, Any]
        ) -> tuple[str, ParallelResult]:
            if semaphore is None:
                return await _execute_one(node, args, kwargs)
            async with semaphore:
                return await _execute_one(node, args, kwargs)

        results_list = await asyncio.gather(
            *(_execute_limited(*_split_task(task)) for task in node_inputs)
        )
        # asyncio.gather 并发返回，存在同名键可能 → _unique_key 去重
        results: dict[str, ParallelResult] = {}
        for base, pr in results_list:
            key = _unique_key(base, results)
            results[key] = pr
        return results
