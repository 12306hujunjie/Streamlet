"""Node —— 用户唯一接触的类型。_func 存储原始函数或 Graph 内部类。"""

import asyncio
import functools
import inspect
from collections.abc import Callable
from typing import Annotated, Any, get_args, get_origin, overload

from dependency_injector.wiring import Provide as _DIProvide
from dependency_injector.wiring import Provider as _DIProvider
from dependency_injector.wiring import inject as _di_inject
from func_timeout import FunctionTimedOut, func_timeout  # type: ignore[import-untyped]
from pydantic import ConfigDict

from .context import _custom_validate_call, apply_context, capture_context
from .exceptions import NodeTimeoutException
from .graph import Conditional, FanIn, Parallel, Pipeline, Repeat
from .retry import RetryConfig, get_func_name, retry_decorator
from .types import RepeatInputMode

logger = __import__("logging").getLogger("streamlet")


def _validate_node(value: Any, param_name: str) -> "Node":
    if not isinstance(value, Node):
        raise TypeError(f"{param_name} must be a Node, got {type(value).__name__}")
    return value


def _validate_node_list(values: Any, param_name: str) -> list["Node"]:
    if not isinstance(values, list):
        raise TypeError(f"{param_name} must be a list of Node instances")
    for index, value in enumerate(values):
        _validate_node(value, f"{param_name}[{index}]")
    return values


def _validate_node_mapping(values: Any, param_name: str) -> dict[Any, "Node"]:
    if not isinstance(values, dict):
        raise TypeError(f"{param_name} must be a dict mapping branch keys to Nodes")
    for key, value in values.items():
        _validate_node(value, f"{param_name}[{key!r}]")
    return values


def _validate_max_workers(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            f"max_workers must be an int or None, got {type(value).__name__}"
        )
    if value <= 0:
        raise ValueError("max_workers must be greater than 0")
    return value


def _validate_timeout(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"timeout must be a number or None, got {type(value).__name__}")
    if value <= 0:
        raise ValueError("timeout must be greater than 0")
    return float(value)


def _timeout_decorator(
    timeout: float,
    node_name: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    return await asyncio.wait_for(func(*args, **kwargs), timeout)
                except asyncio.TimeoutError as exc:
                    raise NodeTimeoutException(
                        message=f"节点 {node_name} 执行超时",
                        node_name=node_name,
                        timeout_seconds=timeout,
                    ) from exc

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            context_snapshot = capture_context()

            def run_with_context() -> Any:
                apply_context(context_snapshot)
                return func(*args, **kwargs)

            try:
                return func_timeout(timeout, run_with_context)
            except FunctionTimedOut as exc:
                raise NodeTimeoutException(
                    message=f"节点 {node_name} 执行超时",
                    node_name=node_name,
                    timeout_seconds=timeout,
                ) from exc

        return sync_wrapper

    return decorator


def _reject_sync_awaitable(result: Any, node_name: str) -> None:
    if not inspect.isawaitable(result):
        return
    if inspect.iscoroutine(result):
        result.close()
    raise TypeError(
        f"sync node '{node_name}' returned an awaitable; "
        "define the node function with 'async def' instead"
    )


class Node:
    """用户唯一接触的类型。_func 可以是原始函数或 Graph 内部类。"""

    def __init__(
        self,
        func: Callable[..., Any],
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
            except RuntimeError:
                return asyncio.run(self._func(*args, **kwargs))
            else:
                return self._func(*args, **kwargs)
        else:
            result = self._func(*args, **kwargs)
            _reject_sync_awaitable(result, self.name)
            return result

    # === 内部接口：供 Executor 使用 ===

    def _execute(self, *args: Any, **kwargs: Any) -> Any:
        result = self._func(*args, **kwargs)
        if not self._is_async:
            _reject_sync_awaitable(result, self.name)
        elif inspect.iscoroutine(result):
            return asyncio.run(result)
        return result

    async def _execute_async(self, *args: Any, **kwargs: Any) -> Any:
        result = self._func(*args, **kwargs)
        if not self._is_async:
            _reject_sync_awaitable(result, self.name)
        elif inspect.isawaitable(result):
            return await result
        return result

    # === Fluent 接口 ===

    def then(self, other: "Node") -> "Node":
        other = _validate_node(other, "other")
        pipeline = Pipeline(self, other)
        return Node(pipeline, name=f"{self.name}→{other.name}")

    def fan_out_to(
        self,
        nodes: list["Node"],
        executor: str = "thread",
        max_workers: int | None = None,
    ) -> "Node":
        if not isinstance(executor, str):
            raise TypeError(f"executor must be a string, got {type(executor).__name__}")
        executor_lower = executor.lower()
        if executor_lower not in ("thread", "async", "auto"):
            raise ValueError(
                f"Only 'thread', 'async', and 'auto' executors are "
                f"supported, got '{executor}'"
            )
        nodes = _validate_node_list(nodes, "nodes")
        if not nodes:
            raise ValueError("Target nodes list cannot be empty")
        max_workers = _validate_max_workers(max_workers)
        parallel = Parallel(
            self, nodes, executor_type=executor_lower, max_workers=max_workers
        )
        # executor="async" 强制 _is_async=True（即使全是 sync 节点），
        # thread/auto 则按组合链真实异步性传播，避免 async source 被伪装成 sync。
        is_async = (
            parallel._is_async
            if executor_lower == "auto"
            else (executor_lower == "async" or parallel._is_async)
        )
        return Node(parallel, name=f"{self.name}∥[...]", is_async=is_async)

    def fan_in(self, aggregator: "Node") -> "Node":
        aggregator = _validate_node(aggregator, "aggregator")
        fan_in = FanIn(self, aggregator)
        return Node(fan_in, name=f"...⤇{aggregator.name}")

    def branch_on(self, conditions: dict[Any, "Node"]) -> "Node":
        conditions = _validate_node_mapping(conditions, "conditions")
        cond = Conditional(self, conditions)
        return Node(cond, name=f"{self.name}?")

    def repeat(
        self,
        times: int,
        stop_on_error: bool = False,
        *,
        input_mode: RepeatInputMode = RepeatInputMode.PREVIOUS_RESULT,
    ) -> "Node":
        rep = Repeat(self, times, stop_on_error, input_mode)
        return Node(rep, name=f"{self.name}×{times}")

    def fan_out_in(
        self,
        targets: list["Node"],
        aggregator: "Node",
        executor: str = "thread",
        max_workers: int | None = None,
    ) -> "Node":
        targets = _validate_node_list(targets, "targets")
        return self.fan_out_to(targets, executor, max_workers).fan_in(aggregator)

    def __repr__(self) -> str:
        return f"Node(name='{self.name}')"

    def __reduce__(self) -> Any:
        raise TypeError(
            "Streamlet Node instances are not pickle-serializable. "
            "Use thread or async executors instead of process-based execution."
        )


def _is_di_marker(value: Any) -> bool:
    return value.__class__ in {_DIProvide, _DIProvider}


def _annotation_has_di_marker(annotation: Any) -> bool:
    return get_origin(annotation) is Annotated and any(
        _is_di_marker(metadata) for metadata in get_args(annotation)[1:]
    )


def _has_di_marker(func: Callable[..., Any]) -> bool:
    """Return whether the function signature asks dependency-injector to resolve DI."""

    return any(
        _is_di_marker(param.default) or _annotation_has_di_marker(param.annotation)
        for param in inspect.signature(func).parameters.values()
    )


# ============================================================
# @node 装饰器
# ============================================================


@overload
def node_decorator(func: Callable[..., Any]) -> Node: ...


@overload
def node_decorator(
    func: Callable[..., Any],
    *,
    retry_count: int = 3,
    name: str | None = None,
    timeout: float | None = None,
    retry_delay: float = 1.0,
    exception_types: tuple[type[Exception], ...] = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
) -> Node: ...


@overload
def node_decorator(
    func: None = None,
    *,
    retry_count: int = 3,
    name: str | None = None,
    timeout: float | None = None,
    retry_delay: float = 1.0,
    exception_types: tuple[type[Exception], ...] = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
) -> Callable[[Callable[..., Any]], Node]: ...


def node_decorator(
    func: Callable[..., Any] | None = None,
    *,
    retry_count: int = 3,
    name: str | None = None,
    timeout: float | None = None,
    retry_delay: float = 1.0,
    exception_types: tuple[type[Exception], ...] = (Exception,),
    backoff_factor: float = 1.0,
    max_delay: float = 60.0,
    enable_retry: bool = False,
) -> Node | Callable[[Callable[..., Any]], Node]:
    """保持现有签名不变。"""
    config = (
        RetryConfig(
            retry_count, retry_delay, exception_types, backoff_factor, max_delay
        )
        if enable_retry
        else None
    )
    timeout = _validate_timeout(timeout)

    @functools.wraps(Node)
    def decorator(f: Callable[..., Any]) -> Node:
        node_name = name or get_func_name(f, "unnamed_node")
        is_original_async = inspect.iscoroutinefunction(f)

        decorators = [
            _custom_validate_call(
                validate_return=True,
                config=ConfigDict(arbitrary_types_allowed=True),
                node_name=node_name,
            ),
        ]
        if config is not None:
            decorators.append(retry_decorator(config=config, node_name=node_name))
        if _has_di_marker(f):
            decorators.append(_di_inject)
        if timeout is not None:
            decorators.append(_timeout_decorator(timeout, node_name))

        decorated_func = functools.reduce(lambda func, deco: deco(func), decorators, f)
        node_obj = Node(func=decorated_func, name=node_name, is_async=is_original_async)
        functools.update_wrapper(node_obj, f, updated=())
        node_obj.__annotations__ = inspect.get_annotations(f, eval_str=False)
        return node_obj

    if func is None:
        return decorator
    else:
        return decorator(func)
