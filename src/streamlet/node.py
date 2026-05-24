"""Node —— 用户唯一接触的类型。_func 存储原始函数或 Graph 内部类。"""

import asyncio
import functools
import inspect
from collections.abc import Callable
from typing import Any

from pydantic import ConfigDict

from . import (
    RetryConfig,
    custom_validate_call,
    retry_decorator,
)
from .graph import Conditional, FanIn, Parallel, Pipeline, Repeat

logger = __import__("logging").getLogger("streamlet")


def _get_func_name(func: Any, fallback_name: str | None = None) -> str:
    if hasattr(func, "__name__"):
        return str(func.__name__)
    elif hasattr(func, "func") and hasattr(func.func, "__name__"):
        return str(func.func.__name__)
    elif hasattr(func, "name"):
        return str(func.name)
    elif fallback_name:
        return fallback_name
    else:
        return "unknown_function"


class Node:
    """用户唯一接触的类型。_func 可以是原始函数或 Graph 内部类。"""

    def __init__(
        self,
        func: Callable,
        name: str,
        is_async: bool | None = None,
    ) -> None:
        self._func = func
        self.name = name
        if is_async is not None:
            self._is_async = is_async
        elif hasattr(func, "_is_async"):
            self._is_async = func._is_async
        else:
            self._is_async = inspect.iscoroutinefunction(func)

    # === 公开入口 ===

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._is_async:
            try:
                asyncio.get_running_loop()
                return self._func(*args, **kwargs)
            except RuntimeError:
                return asyncio.run(self._func(*args, **kwargs))
        else:
            return self._func(*args, **kwargs)

    # === 内部接口：供 Executor 使用 ===

    def _execute(self, *args: Any, **kwargs: Any) -> Any:
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
            return asyncio.run(result)
        return result

    async def _execute_async(self, *args: Any, **kwargs: Any) -> Any:
        result = self._func(*args, **kwargs)
        if inspect.iscoroutine(result):
            return await result
        return result

    # === Fluent 接口 ===

    def then(self, other: "Node") -> "Node":
        pipeline = Pipeline(self, other)
        return Node(pipeline, name=f"{self.name}→{other.name}")

    def fan_out_to(
        self,
        nodes: list["Node"],
        executor: str = "thread",
        max_workers: int | None = None,
    ) -> "Node":
        executor_lower = executor.lower()
        if executor_lower not in ("thread", "async", "auto"):
            raise ValueError(
                f"Only 'thread', 'async', and 'auto' executors are "
                f"supported, got '{executor}'"
            )
        if not nodes:
            raise ValueError("Target nodes list cannot be empty")
        parallel = Parallel(
            self, nodes, executor_type=executor_lower, max_workers=max_workers
        )
        # executor="async" 强制 _is_async=True（即使全是 sync 节点），
        # 否则 FanIn 等下游走 sync 路径会导致 asyncio.run 在 event loop 内失败
        is_async = (
            parallel._is_async
            if executor_lower == "auto"
            else (executor_lower == "async")
        )
        return Node(parallel, name=f"{self.name}∥[...]", is_async=is_async)

    def fan_in(self, aggregator: "Node") -> "Node":
        fan_in = FanIn(self, aggregator)
        return Node(fan_in, name=f"...⤇{aggregator.name}")

    def branch_on(self, conditions: dict[Any, "Node"]) -> "Node":
        cond = Conditional(self, conditions)
        return Node(cond, name=f"{self.name}?")

    def repeat(self, times: int, stop_on_error: bool = False) -> "Node":
        rep = Repeat(self, times, stop_on_error)
        return Node(rep, name=f"{self.name}×{times}")

    def fan_out_in(
        self,
        targets: list["Node"],
        aggregator: "Node",
        executor: str = "thread",
        max_workers: int | None = None,
    ) -> "Node":
        return self.fan_out_to(targets, executor, max_workers).fan_in(aggregator)

    def __repr__(self) -> str:
        return f"Node(name='{self.name}')"


# ============================================================
# @node 装饰器
# ============================================================


def node_decorator(
    func: Callable | None = None,
    *,
    retry_count: int = 3,
    name: str | None = None,
    retry_delay: float = 1.0,
    exception_types: tuple = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
) -> Node | Callable:
    """保持现有签名不变。"""
    config = RetryConfig(
        retry_count, retry_delay, exception_types, backoff_factor, max_delay
    )

    @functools.wraps(Node)
    def decorator(f: Callable) -> Node:
        node_name = name or _get_func_name(f, "unnamed_node")
        is_original_async = inspect.iscoroutinefunction(f)

        decorators = [
            custom_validate_call(
                validate_return=True,
                config=ConfigDict(arbitrary_types_allowed=True),
                node_name=node_name,
            ),
        ]
        if enable_retry:
            decorators.append(retry_decorator(config=config, node_name=node_name))
        decorators.append(__import__("dependency_injector.wiring").wiring.inject)

        decorated_func = functools.reduce(lambda func, deco: deco(func), decorators, f)
        return Node(func=decorated_func, name=node_name, is_async=is_original_async)

    if func is None:
        return decorator
    else:
        return decorator(func)
