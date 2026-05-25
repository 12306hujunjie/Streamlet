"""依赖注入与验证工具。

BaseFlowContext + ContextVarProvider + custom_validate_call。
"""

import functools
import inspect
import logging
from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

from dependency_injector import containers, providers
from pydantic import ConfigDict, TypeAdapter, ValidationError, validate_call

from .exceptions import ValidationInputException, ValidationOutputException
from .retry import get_func_name

logger = logging.getLogger("streamlet")


class ContextVarProvider(providers.Provider):
    """自定义 Provider 类，支持 ContextVar 的协程安全依赖注入。"""

    def __init__(self, default_factory: Callable[[], Any] = dict):
        super().__init__()
        self._context_var = ContextVar(f"streamlet_{id(self)}", default=None)
        self._default_factory = default_factory

    def _provide(self, *args: Any, **kwargs: Any) -> Any:
        value = self._context_var.get()
        if value is None:
            value = self._default_factory()
            self._context_var.set(value)
        return value


class BaseFlowContext(containers.DeclarativeContainer):
    """Base container for flow context with thread-safe and coroutine-safe DI."""

    state: providers.Provider = providers.ThreadLocalSingleton(dict)
    context: providers.Provider = providers.ThreadLocalSingleton(dict)
    shared_data: providers.Provider = providers.Singleton(dict)

    async_state: providers.Provider = ContextVarProvider(dict)
    async_context: providers.Provider = ContextVarProvider(dict)


def custom_validate_call(
    validate_return: bool = True,
    config: ConfigDict | None = None,
    node_name: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """自定义 validate_call 包装器，区分输入/输出验证异常。"""

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)

        input_validator = validate_call(
            validate_return=False,
            config=config or ConfigDict(arbitrary_types_allowed=True),
        )(func)

        return_type_adapter = None
        if validate_return and sig.return_annotation != inspect.Signature.empty:
            return_type_adapter = TypeAdapter(sig.return_annotation)

        func_name = node_name or get_func_name(func)

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
