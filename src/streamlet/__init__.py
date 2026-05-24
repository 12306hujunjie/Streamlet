import functools
import inspect
import logging
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from dependency_injector import containers, providers
from pydantic import ConfigDict, TypeAdapter, ValidationError, validate_call

logger = logging.getLogger("streamlet")


@dataclass
class ParallelResult:
    """并行执行结果——记录成功/失败状态、错误信息、执行时间。"""

    node_name: str
    success: bool
    result: Any = None
    error: str | None = None
    error_traceback: str | None = None
    execution_time: float | None = None


# ==================== 异常类型体系（已迁移到 exceptions.py） ====================

from .exceptions import (  # noqa: E402
    LoopControlException,
    NodeExecutionException,
    NodeRetryExhaustedException,
    NodeTimeoutException,
    StreamletException,
    UserBusinessException,
    ValidationInputException,
    ValidationOutputException,
)

# ==================== 重试装饰器（已迁移到 retry.py） ====================
from .retry import RetryConfig, _get_func_name  # noqa: E402

# 原 RetryConfig / retry_decorator / _get_func_name 已提取到 retry.py
# Context variables for asyncio coroutine safety
_context_state: ContextVar[dict | None] = ContextVar("streamlet_state", default=None)
_context_context: ContextVar[dict | None] = ContextVar(
    "streamlet_context", default=None
)


# ==================== 自定义ContextVar Provider ====================


class ContextVarProvider(providers.Provider):
    """自定义Provider类，支持ContextVar的协程安全依赖注入。

    这个Provider替代了直接调用ContextVar.get()的方式，
    提供了正确的dependency-injector集成。
    """

    def __init__(self, default_factory: Callable[[], Any] = dict):
        """初始化ContextVarProvider。

        Args:
            default_factory: 创建默认值的工厂函数，默认为dict
        """
        super().__init__()
        self._context_var = ContextVar(f"streamlet_{id(self)}", default=None)
        self._default_factory = default_factory

    def _provide(self, *args: Any, **kwargs: Any) -> Any:
        """提供协程安全的状态值。

        Returns:
            ContextVar中的值，如果未设置则返回默认值
        """
        try:
            value = self._context_var.get()
            if value is None:
                # 如果未设置，创建并设置默认值
                value = self._default_factory()
                self._context_var.set(value)
            return value
        except LookupError:
            # 如果ContextVar未初始化，创建默认值
            value = self._default_factory()
            self._context_var.set(value)
            return value


class BaseFlowContext(containers.DeclarativeContainer):
    """Base container for flow context with thread-safe and coroutine-safe dependency injection support."""

    # Use ThreadLocalSingleton for thread-local state isolation
    # Each thread gets its own state dictionary
    state: providers.Provider = providers.ThreadLocalSingleton(dict)
    context: providers.Provider = providers.ThreadLocalSingleton(dict)
    shared_data: providers.Provider = providers.Singleton(dict)

    # Coroutine-safe providers using ContextVar for asyncio
    async_state: providers.Provider = ContextVarProvider(dict)
    async_context: providers.Provider = ContextVarProvider(dict)


def custom_validate_call(
    validate_return: bool = True,
    config: ConfigDict | None = None,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    自定义validate_call包装器，使用Pydantic最佳实践区分输入验证和输出验证异常

    Args:
        validate_return: 是否验证返回值
        config: Pydantic配置
        node_name: 节点名称用于异常信息

    Returns:
        装饰器函数
    """

    def decorator(func: Callable) -> Callable:
        # 获取函数签名
        sig = inspect.signature(func)

        # 创建输入验证器 - 只验证参数，不验证返回值
        input_validator = validate_call(
            validate_return=False,
            config=config or ConfigDict(arbitrary_types_allowed=True),
        )(func)

        # 创建返回值验证器（如果需要且有返回值类型注解）
        return_type_adapter = None
        if validate_return and sig.return_annotation != inspect.Signature.empty:
            return_type_adapter = TypeAdapter(sig.return_annotation)

        # 提取公共逻辑
        func_name = node_name or _get_func_name(func)

        def create_input_exception(e: ValidationError) -> ValidationInputException:
            return ValidationInputException(
                f"输入参数验证失败: {e}",
                validation_error=e,
                node_name=func_name,
            )

        def create_output_exception(e: ValidationError) -> ValidationOutputException:
            return ValidationOutputException(
                f"返回值验证失败: {e}",
                validation_error=e,
                node_name=func_name,
            )

        def validate_result(result: Any) -> Any:
            if return_type_adapter:
                try:
                    return_type_adapter.validate_python(result)
                except ValidationError as e:
                    raise create_output_exception(e) from e
            return result

        # 根据函数类型提供对应的wrapper
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    result = await input_validator(*args, **kwargs)
                except ValidationError as e:
                    raise create_input_exception(e) from e
                return validate_result(result)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                try:
                    result = input_validator(*args, **kwargs)
                except ValidationError as e:
                    raise create_input_exception(e) from e
                return validate_result(result)

            return sync_wrapper

    return decorator


# ============================================================
# 重新导出：Node + @node（node.py）
# ============================================================

from .node import Node, _get_func_name  # noqa: E402
from .node import node_decorator as node  # noqa: E402

__all__ = [
    "node",
    "Node",
    "BaseFlowContext",
    "ParallelResult",
    "StreamletException",
    "ValidationInputException",
    "ValidationOutputException",
    "UserBusinessException",
    "NodeExecutionException",
    "NodeTimeoutException",
    "NodeRetryExhaustedException",
    "LoopControlException",
    "RetryConfig",
]
