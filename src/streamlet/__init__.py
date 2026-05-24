import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from dependency_injector import containers, providers
from pydantic import ConfigDict, TypeAdapter, ValidationError, validate_call

logger = logging.getLogger("streamlet")


@dataclass
class ParallelResult:
    """Pydantic model for recording parallel execution results and exception stacks."""

    node_name: str
    success: bool
    result: Any = None
    error: str | None = None
    error_traceback: str | None = None
    execution_time: float | None = None


# ==================== 异常类型体系 ====================
class StreamletException(Exception):
    """Streamlet框架基础异常类"""

    retryable = False  # 默认框架异常不重试

    def __init__(
        self, message: str, node_name: str | None = None, **kwargs: Any
    ) -> None:
        self.node_name = node_name
        self.context = kwargs
        super().__init__(message)


class ValidationInputException(StreamletException):
    """参数验证异常 - validate_call前置校验失败"""

    retryable = False  # 参数验证失败不应该重试

    def __init__(
        self,
        message: str,
        validation_error: Any = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.validation_error = validation_error
        super().__init__(message, node_name, **kwargs)


class ValidationOutputException(StreamletException):
    """返回值验证异常 - validate_call返回值校验失败"""

    retryable = False  # 返回值验证失败不应该重试

    def __init__(
        self,
        message: str,
        validation_error: Any = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.validation_error = validation_error
        super().__init__(message, node_name, **kwargs)


class UserBusinessException(StreamletException):
    """用户业务异常基类 - 用户可自定义重试策略"""

    retryable = True  # 默认用户业务异常可重试

    def __init__(
        self,
        message: str,
        retryable: bool | None = None,
        node_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        # 允许用户在实例化时覆盖重试策略
        if retryable is not None:
            self.retryable = retryable
        super().__init__(message, node_name, **kwargs)


class NodeExecutionException(StreamletException):
    """节点执行异常"""

    def __init__(
        self,
        message: str,
        node_name: str | None = None,
        original_exception: Exception | None = None,
        **kwargs: Any,
    ) -> None:
        self.original_exception = original_exception
        super().__init__(message, node_name, **kwargs)


class NodeTimeoutException(NodeExecutionException):
    """节点执行超时异常"""

    def __init__(
        self,
        message: str,
        node_name: str | None = None,
        timeout_seconds: float | None = None,
        **kwargs: Any,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(message, node_name, **kwargs)


class NodeRetryExhaustedException(NodeExecutionException):
    """节点重试次数耗尽异常"""

    def __init__(
        self,
        message: str,
        node_name: str | None = None,
        retry_count: int | None = None,
        last_exception: Exception | None = None,
        **kwargs: Any,
    ) -> None:
        self.retry_count = retry_count
        self.last_exception = last_exception
        super().__init__(
            message, node_name, original_exception=last_exception, **kwargs
        )


class LoopControlException(StreamletException):
    """循环控制异常基类"""

    pass


# ==================== 重试装饰器 ====================


class RetryConfig:
    """重试配置类"""

    def __init__(
        self,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        exception_types: tuple = (Exception,),
        backoff_factor: float = 1.0,
        max_delay: float = 60.0,
    ):
        if retry_count < 0:
            raise ValueError(f"retry_count must be >= 0, got {retry_count}")
        if retry_delay < 0:
            raise ValueError(f"retry_delay must be >= 0, got {retry_delay}")
        if max_delay < 0:
            raise ValueError(f"max_delay must be >= 0, got {max_delay}")
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.exception_types = exception_types
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay

    def should_retry(self, exception: Exception) -> bool:
        """判断是否应该重试 - 优先检查retryable属性，否则使用isinstance检查继承关系"""
        # 如果异常有retryable属性，优先使用
        if hasattr(exception, "retryable"):
            return bool(exception.retryable)

        # 否则使用isinstance检查异常是否属于指定类型（包括继承关系）
        return isinstance(exception, self.exception_types)

    def get_delay(self, attempt: int) -> float:
        """计算重试延迟时间（支持指数退避）"""
        delay = self.retry_delay * (self.backoff_factor**attempt)
        return min(delay, self.max_delay)


def _get_func_name(func: Any, fallback_name: str | None = None) -> str:
    """安全获取函数名称"""
    if hasattr(func, "__name__"):
        return str(func.__name__)
    elif hasattr(func, "func") and hasattr(func.func, "__name__"):  # partial对象
        return str(func.func.__name__)
    elif hasattr(func, "name"):  # Node对象
        return str(func.name)
    elif fallback_name:
        return fallback_name
    else:
        return "unknown_function"


def retry_decorator(
    config: RetryConfig,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """重试装饰器

    Args:
        config: RetryConfig 配置模型
        node_name: 节点名称，用于异常信息
    """

    def decorator(func: Callable) -> Callable:
        func_name = node_name or _get_func_name(func)

        if inspect.iscoroutinefunction(func):
            # 异步函数wrapper
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(config.retry_count + 1):
                    try:
                        logger.debug(
                            f"执行节点 {func_name}，尝试 {attempt + 1}/{config.retry_count + 1}"
                        )
                        result = await func(*args, **kwargs)  # 异步调用

                        if attempt > 0:
                            logger.info(
                                f"节点 {func_name} 在第 {attempt + 1} 次尝试后成功"
                            )
                        return result

                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as e:
                        if not config.should_retry(e):
                            # 记录不重试的原因
                            logger.debug(
                                f"节点 {func_name} 异常不支持重试: {type(e).__name__}: {e}"
                            )
                            raise  # 直接抛出，不封装

                        if attempt == config.retry_count:
                            # 记录重试耗尽
                            logger.error(
                                f"节点 {func_name} 重试 {config.retry_count} 次后仍失败: {type(e).__name__}: {e}"
                            )
                            raise  # 重试耗尽也直接抛出，不封装

                        delay = config.get_delay(attempt)
                        logger.warning(
                            f"节点 {func_name} 第 {attempt + 1} 次尝试失败: {e}，{delay:.2f}秒后重试"
                        )
                        await asyncio.sleep(delay)  # 异步延迟

            return async_wrapper
        else:
            # 同步函数wrapper
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                for attempt in range(config.retry_count + 1):
                    try:
                        logger.debug(
                            f"执行节点 {func_name}，尝试 {attempt + 1}/{config.retry_count + 1}"
                        )
                        result = func(*args, **kwargs)  # 同步调用

                        if attempt > 0:
                            logger.info(
                                f"节点 {func_name} 在第 {attempt + 1} 次尝试后成功"
                            )
                        return result

                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as e:
                        if not config.should_retry(e):
                            # 记录不重试的原因
                            logger.debug(
                                f"节点 {func_name} 异常不支持重试: {type(e).__name__}: {e}"
                            )
                            raise  # 直接抛出，不封装

                        if attempt == config.retry_count:
                            # 记录重试耗尽
                            logger.error(
                                f"节点 {func_name} 重试 {config.retry_count} 次后仍失败: {type(e).__name__}: {e}"
                            )
                            raise  # 重试耗尽也直接抛出，不封装

                        delay = config.get_delay(attempt)
                        logger.warning(
                            f"节点 {func_name} 第 {attempt + 1} 次尝试失败: {e}，{delay:.2f}秒后重试"
                        )
                        time.sleep(delay)  # 同步延迟

            return sync_wrapper

    return decorator


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
