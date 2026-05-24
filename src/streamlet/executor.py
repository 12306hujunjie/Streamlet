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
from typing import Any, Protocol, runtime_checkable

from . import ParallelResult


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
        tasks: list[tuple[Any, Any]],
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

    def _run_with_time(self, node: Any, inp: Any) -> Any:
        start = time.time()
        try:
            result = node._execute(inp)
            return ParallelResult(
                node_name=node.name,
                success=True,
                result=result,
                execution_time=time.time() - start,
            )
        except Exception as e:
            return ParallelResult(
                node_name=node.name,
                success=False,
                error=str(e),
                error_traceback=traceback.format_exc(),
                execution_time=time.time() - start,
            )

    def gather(
        self,
        node_inputs: list[tuple[Any, Any]],
        key_func: Callable[[Any], str] | None = None,
    ) -> dict[str, "ParallelResult"]:
        parent_ctx = contextvars.copy_context()
        results: dict[str, ParallelResult] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = [
                (ex.submit(parent_ctx.copy().run, self._run_with_time, n, inp), n)
                for n, inp in node_inputs
            ]
            for future, node in futures:
                base = key_func(node) if key_func else node.name
                key = _unique_key(base, results)
                pr = future.result()
                results[key] = pr
        return results


class AsyncExecutor:
    """异步执行器：gather 使用 asyncio.gather 实现真正的异步并发。"""

    async def arun(self, node: Any, *args: Any, **kwargs: Any) -> Any:
        return await node._execute_async(*args, **kwargs)

    async def agather(
        self,
        node_inputs: list[tuple[Any, Any]],
        key_func: Callable[[Any], str] | None = None,
    ) -> dict[str, "ParallelResult"]:
        async def _execute_one(node: Any, inp: Any) -> tuple[str, ParallelResult]:
            base = key_func(node) if key_func else node.name
            start = time.time()
            try:
                result = await node._execute_async(inp)
                return base, ParallelResult(
                    node_name=node.name,
                    success=True,
                    result=result,
                    execution_time=time.time() - start,
                )
            except Exception as e:
                return base, ParallelResult(
                    node_name=node.name,
                    success=False,
                    error=str(e),
                    error_traceback=traceback.format_exc(),
                    execution_time=time.time() - start,
                )

        results_list = await asyncio.gather(
            *(_execute_one(n, inp) for n, inp in node_inputs)
        )
        # asyncio.gather 并发返回，存在同名键可能 → _unique_key 去重
        results: dict[str, ParallelResult] = {}
        for base, pr in results_list:
            key = _unique_key(base, results)
            results[key] = pr
        return results
